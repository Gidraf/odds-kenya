"""
app/services/playwright_fetcher.py
====================================
Playwright Request Fetcher — intercepts ALL network requests from a launched
Chromium instance, stores curl + response bodies to MinIO, streams state to
frontend via polling REST API.

Architecture:
  • POST /api/fetcher/sessions              → launch Chromium
  • GET  /api/fetcher/sessions/<id>/state   → poll for captured requests
  • POST /api/fetcher/sessions/<id>/save    → save selected request(s) to MinIO
  • POST /api/fetcher/sessions/<id>/navigate → tell browser to go to a URL
  • POST /api/fetcher/sessions/<id>/label   → label a captured request
  • POST /api/fetcher/sessions/<id>/clear   → clear all captures
  • DELETE /api/fetcher/sessions/<id>       → kill session
  • GET  /api/fetcher/sessions/<id>/request/<req_id>/curl  → raw curl text
  • GET  /api/fetcher/sessions/<id>/request/<req_id>/body  → raw response body

MinIO:
  Stores under: fetcher/<session_id>/<label>/<ts>_curl.txt
                fetcher/<session_id>/<label>/<ts>_response.json|bin
                fetcher/<session_id>/<label>/<ts>_meta.json

Requirements:
  pip install playwright minio
  playwright install chromium
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import re
import shlex
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

# ─── Playwright ───────────────────────────────────────────────────────────────
try:
    from playwright.async_api import async_playwright
    _PW_OK = True
except ImportError:
    _PW_OK = False

# ─── MinIO ────────────────────────────────────────────────────────────────────
try:
    from minio import Minio
    _MINIO_OK = True
except ImportError:
    _MINIO_OK = False

from flask import Blueprint, request as flask_request, jsonify, make_response

# ─────────────────────────────────────────────────────────────────────────────
# Config  (override via env)
# ─────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.environ.get("STORAGE_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("STORAGE_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("STORAGE_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.environ.get("STORAGE_BUCKET",     "fetcher")
MINIO_SECURE     = os.environ.get("STORAGE_SECURE",     "false").lower() == "true"

_CDP_PORT_BASE = int(os.environ.get("CDP_PORT_BASE", "9222"))

# ── Noise filter — skip these entirely ────────────────────────────────────────
_NOISE = re.compile(
    r"\.(png|jpe?g|gif|svg|ico|webp|woff2?|ttf|eot|otf|css|js\.map)"
    r"|google-analytics|googletagmanager|doubleclick|facebook\.net"
    r"|hotjar|sentry\.io|beacon|cdn\.jsdelivr|cdnjs\.cloudflare"
    r"|snapchat\.com/cm|adservice\.google|adnxs\.com|connextra\.com"
    r"|bidr\.io|eskimi\.com|sc-static\.net|zdassets\.com",
    re.I,
)

# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CapturedRequest:
    req_id:       str
    url:          str
    method:       str
    status:       int
    req_headers:  dict
    resp_headers: dict
    post_data:    str | None
    body_raw:     bytes
    content_type: str
    size:         int
    ts:           float = field(default_factory=time.time)
    saved:        bool  = False
    minio_paths:  dict  = field(default_factory=dict)
    label:        str   = ""

    @property
    def body_text(self) -> str:
        try:
            return self.body_raw.decode("utf-8", errors="replace")
        except Exception:
            return ""

    @property
    def body_preview(self) -> str:
        return self.body_text[:2000]

    @property
    def is_json(self) -> bool:
        return "json" in self.content_type.lower()

    @property
    def is_binary(self) -> bool:
        ct = self.content_type.lower()
        return any(x in ct for x in ("image", "audio", "video", "octet-stream", "font"))

    @property
    def parsed_body(self) -> Any:
        if not self.is_json:
            return None
        try:
            return json.loads(self.body_raw)
        except Exception:
            return None

    def to_wire(self) -> dict:
        return {
            "req_id":       self.req_id,
            "url":          self.url,
            "method":       self.method,
            "status":       self.status,
            "content_type": self.content_type,
            "size":         self.size,
            "ts":           round(self.ts, 3),
            "saved":        self.saved,
            "minio_paths":  self.minio_paths,
            "label":        self.label,
            "is_json":      self.is_json,
            "is_binary":    self.is_binary,
            "body_preview": self.body_preview,
            "req_headers":  self.req_headers,
            "resp_headers": self.resp_headers,
            "post_data":    self.post_data,
        }

    def to_curl(self) -> str:
        parts = ["curl", "-X", self.method, shlex.quote(self.url)]
        skip  = {":method", ":path", ":authority", ":scheme", "content-length"}
        for k, v in self.req_headers.items():
            if k.lower() not in skip:
                parts += ["-H", shlex.quote(f"{k}: {v}")]
        if self.post_data:
            parts += ["--data-raw", shlex.quote(self.post_data)]
        parts += ["--compressed"]
        return " \\\n  ".join(parts)


@dataclass
class FetcherSession:
    session_id:   str
    domain:       str
    status:       str   = "launching"
    error:        str   = ""
    page_url:     str   = ""
    page_title:   str   = ""
    cdp_port:     int   = 0
    captures:     list  = field(default_factory=list)
    logs:         list  = field(default_factory=list)
    is_active:    bool  = True
    started_at:   float = field(default_factory=time.time)
    _seen:        set   = field(default_factory=set)    # dedup: method:url
    _pending_url: str   = ""                            # queued navigation

    def add_log(self, level: str, msg: str):
        entry = {"level": level, "msg": msg, "ts": time.time()}
        self.logs.append(entry)
        if len(self.logs) > 400:
            self.logs = self.logs[-400:]

    def get_state(self) -> dict:
        return {
            "session_id": self.session_id,
            "domain":     self.domain,
            "status":     self.status,
            "error":      self.error,
            "page_url":   self.page_url,
            "page_title": self.page_title,
            "cdp_port":   self.cdp_port,
            "captures":   [c.to_wire() for c in self.captures],
            "logs":       self.logs[-100:],
            "count":      len(self.captures),
        }


# ─────────────────────────────────────────────────────────────────────────────
# Session store
# ─────────────────────────────────────────────────────────────────────────────

_SESSIONS:      dict[str, FetcherSession] = {}
_PORT_COUNTER = [0]


# ─────────────────────────────────────────────────────────────────────────────
# MinIO
# ─────────────────────────────────────────────────────────────────────────────

def _minio_client():
    if not _MINIO_OK:
        return None
    try:
        c = Minio(MINIO_ENDPOINT,
                  access_key=MINIO_ACCESS_KEY,
                  secret_key=MINIO_SECRET_KEY,
                  secure=MINIO_SECURE)
        if not c.bucket_exists(MINIO_BUCKET):
            c.make_bucket(MINIO_BUCKET)
        return c
    except Exception as e:
        print(f"[Fetcher] MinIO init error: {e}")
        return None


def _save_to_minio(session_id: str, cap: CapturedRequest, label: str) -> dict:
    client = _minio_client()
    if client is None:
        return {}

    slug   = re.sub(r"[^a-z0-9_]", "_", label.lower())[:40] or "request"
    ts     = int(cap.ts)
    prefix = f"fetcher/{session_id}/{slug}/{ts}"
    paths: dict[str, str] = {}

    # curl
    curl_bytes = cap.to_curl().encode()
    curl_path  = f"{prefix}_curl.txt"
    try:
        client.put_object(MINIO_BUCKET, curl_path, io.BytesIO(curl_bytes), len(curl_bytes), content_type="text/plain")
        paths["curl"] = f"{MINIO_BUCKET}/{curl_path}"
    except Exception as e:
        print(f"[Fetcher] MinIO curl error: {e}")

    # response body
    if cap.body_raw:
        ext       = ".json" if cap.is_json else ".bin"
        resp_path = f"{prefix}_response{ext}"
        try:
            client.put_object(MINIO_BUCKET, resp_path, io.BytesIO(cap.body_raw), len(cap.body_raw), content_type=cap.content_type or "application/octet-stream")
            paths["response"] = f"{MINIO_BUCKET}/{resp_path}"
        except Exception as e:
            print(f"[Fetcher] MinIO body error: {e}")

    # meta
    meta       = {"session_id": session_id, "url": cap.url, "method": cap.method, "status": cap.status,
                  "content_type": cap.content_type, "size": cap.size, "ts": cap.ts,
                  "req_headers": cap.req_headers, "resp_headers": cap.resp_headers,
                  "post_data": cap.post_data, "label": label, "minio_paths": paths}
    meta_bytes = json.dumps(meta, indent=2).encode()
    meta_path  = f"{prefix}_meta.json"
    try:
        client.put_object(MINIO_BUCKET, meta_path, io.BytesIO(meta_bytes), len(meta_bytes), content_type="application/json")
        paths["meta"] = f"{MINIO_BUCKET}/{meta_path}"
    except Exception as e:
        print(f"[Fetcher] MinIO meta error: {e}")

    return paths


# ─────────────────────────────────────────────────────────────────────────────
# Manager
# ─────────────────────────────────────────────────────────────────────────────

class PlaywrightFetcherManager:

    # ── Public API ─────────────────────────────────────────────────────────────

    def start_session(self, session_id: str, domain: str) -> dict:
        if not _PW_OK:
            return {"ok": False, "error": "playwright not installed — run: pip install playwright && playwright install chromium"}
        if session_id in _SESSIONS:
            return {"ok": False, "error": "Session already exists"}

        _PORT_COUNTER[0] += 1
        cdp_port = _CDP_PORT_BASE + _PORT_COUNTER[0]

        session = FetcherSession(session_id=session_id, domain=domain, cdp_port=cdp_port)
        _SESSIONS[session_id] = session

        threading.Thread(
            target=self._run_thread,
            args=(session_id, cdp_port),
            daemon=True,
            name=f"fetcher-{session_id[:8]}"
        ).start()

        return {"ok": True, "session_id": session_id, "cdp_port": cdp_port}

    def get_state(self, session_id: str) -> dict | None:
        s = _SESSIONS.get(session_id)
        return s.get_state() if s else None

    def navigate(self, session_id: str, url: str) -> dict:
        s = _SESSIONS.get(session_id)
        if not s:
            return {"ok": False, "error": "session not found"}
        if s.status != "active":
            return {"ok": False, "error": "session not active"}
        nav_url = url if url.startswith("http") else f"https://{url}"
        s._pending_url = nav_url
        s.add_log("INFO", f"Navigation queued: {nav_url}")
        return {"ok": True}

    def save_requests(self, session_id: str, req_ids: list, label: str) -> dict:
        s = _SESSIONS.get(session_id)
        if not s:
            return {"ok": False, "error": "session not found"}
        saved, errors = [], []
        for req_id in req_ids:
            cap = next((c for c in s.captures if c.req_id == req_id), None)
            if not cap:
                errors.append(f"{req_id}: not found"); continue
            cap_label = label or cap.label or _url_slug(cap.url)
            paths = _save_to_minio(session_id, cap, cap_label)
            if paths:
                cap.saved = True; cap.minio_paths = paths
                saved.append({"req_id": req_id, "paths": paths})
            else:
                errors.append(f"{req_id}: minio unavailable")
        return {"ok": True, "saved": saved, "errors": errors}

    def label_request(self, session_id: str, req_id: str, label: str) -> dict:
        s = _SESSIONS.get(session_id)
        if not s:
            return {"ok": False, "error": "session not found"}
        cap = next((c for c in s.captures if c.req_id == req_id), None)
        if not cap:
            return {"ok": False, "error": "request not found"}
        cap.label = label
        return {"ok": True}

    def clear_captures(self, session_id: str) -> dict:
        s = _SESSIONS.get(session_id)
        if not s:
            return {"ok": False}
        s.captures.clear()
        s._seen.clear()
        s.add_log("INFO", "Captures cleared")
        return {"ok": True}

    def stop_session(self, session_id: str):
        s = _SESSIONS.get(session_id)
        if s:
            s.is_active = False
            s.status    = "stopped"
        _SESSIONS.pop(session_id, None)

    # ── Playwright thread ──────────────────────────────────────────────────────

    def _run_thread(self, session_id: str, cdp_port: int):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._async_session(session_id, cdp_port))
        except Exception as e:
            s = _SESSIONS.get(session_id)
            if s:
                s.status = "error"
                s.error  = str(e)
                s.add_log("ERROR", f"Fatal thread error: {str(e)}")
        finally:
            loop.close()

    async def _async_session(self, session_id: str, cdp_port: int):
        session = _SESSIONS.get(session_id)
        if not session:
            return

        session.add_log("INFO", f"Launching Chromium (headless) on CDP :{cdp_port}…")

        try:
            async with async_playwright() as pw:

                # ── Launch ─────────────────────────────────────────────────────
                try:
                    browser = await pw.chromium.launch(
                        headless=False,
                        args=[
                            "--no-sandbox",
                            "--disable-dev-shm-usage",
                            "--disable-gpu",
                            "--disable-blink-features=AutomationControlled",
                            "--disable-setuid-sandbox",
                            "--disable-background-networking",
                            f"--remote-debugging-port={cdp_port}",
                            "--r"
                            "emote-debugging-address=0.0.0.0",
                        ],
                         env={**os.environ, "DISPLAY": ":99"},
                    )
                except Exception as e:
                    session.status = "error"
                    session.error  = f"Failed to launch Chromium: {str(e)}"
                    session.add_log("ERROR", session.error)
                    return

                session.add_log("SUCCESS", "Chromium launched")

                context = await browser.new_context(
                    viewport={"width": 1440, "height": 900},
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                    locale="en-US",
                    ignore_https_errors=True,
                )

                await context.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
                )

                page = await context.new_page()

                # ── Response interceptor ───────────────────────────────────────
                async def on_response(response):
                    try:
                        url = response.url

                        # Skip noise
                        if _NOISE.search(url):
                            return

                        req    = response.request
                        method = req.method

                        # Dedup: same method+URL = skip (keep first only)
                        dedup_key = f"{method}:{url}"
                        if dedup_key in session._seen:
                            return
                        session._seen.add(dedup_key)

                        # Read body
                        try:
                            body = await response.body()
                        except Exception:
                            body = b""

                        ct = (response.headers.get("content-type") or "").lower()

                        # Skip tiny non-JSON responses
                        if len(body) < 20 and "json" not in ct:
                            return

                        req_id = hashlib.md5(f"{method}:{url}".encode()).hexdigest()[:12]

                        cap = CapturedRequest(
                            req_id       = req_id,
                            url          = url,
                            method       = method,
                            status       = response.status,
                            req_headers  = dict(req.headers),
                            resp_headers = dict(response.headers),
                            post_data    = req.post_data,
                            body_raw     = body,
                            content_type = ct,
                            size         = len(body),
                        )
                        session.captures.append(cap)

                        kb = len(body) // 1024
                        session.add_log(
                            "CAPTURE",
                            f"{method} {url[:85]} → {response.status} "
                            f"({'%dKB' % kb if kb else '%dB' % len(body)}) [{ct.split(';')[0].strip()}]"
                        )

                        # Keep page_url fresh
                        try:
                            session.page_url = page.url
                        except Exception:
                            pass

                    except Exception as e:
                        session.add_log("DEBUG", f"response handler err: {e}")

                page.on("response", on_response)

                # ── URL change tracker (sync callback) ─────────────────────────
                def on_url(url: str):
                    if not url.startswith("data:"):
                        session.page_url = url
                        session.add_log("INFO", f"URL: {url[:100]}")

                page.on("url", on_url)

                # ── Initial navigation ─────────────────────────────────────────
                domain    = session.domain
                start_url = domain if domain.startswith("http") else f"https://{domain}"
                session.add_log("INFO", f"Navigating → {start_url}")

                try:
                    await page.goto(start_url, wait_until="domcontentloaded", timeout=30_000)
                except Exception as e:
                    # SPA sites often trigger timeout on domcontentloaded — continue anyway
                    session.add_log("WARN", f"goto warning (continuing): {str(e)[:100]}")

                # Mark active regardless — page likely loaded enough
                try:
                    session.page_url   = page.url
                    session.page_title = await page.title()
                except Exception:
                    session.page_url   = start_url
                    session.page_title = ""

                session.status = "active"
                session.add_log("SUCCESS", f"Active — {session.page_url} · \"{session.page_title or 'no title'}\"")

                # ── Keep-alive loop ────────────────────────────────────────────
                while session.is_active:
                    await asyncio.sleep(0.8)

                    # Check if page is still alive
                    try:
                        session.page_url   = page.url
                        session.page_title = await page.title()
                    except Exception:
                        session.add_log("WARN", "Page unresponsive — session ended")
                        break

                    # Handle pending navigation from UI
                    pending = session._pending_url
                    if pending:
                        session._pending_url = ""
                        try:
                            session.add_log("INFO", f"Navigating → {pending}")
                            await page.goto(pending, wait_until="domcontentloaded", timeout=25_000)
                            session.page_url   = page.url
                            session.page_title = await page.title()
                            session.add_log("SUCCESS", f"Navigated → {session.page_url} · \"{session.page_title}\"")
                        except Exception as e:
                            session.add_log("WARN", f"Navigation warning: {str(e)[:100]}")
                            try:
                                session.page_url = page.url
                            except Exception:
                                pass

                # ── Cleanup ────────────────────────────────────────────────────
                try:
                    await browser.close()
                except Exception:
                    pass

                if session.status not in ("error", "stopped"):
                    session.status = "stopped"
                    session.add_log("INFO", "Session ended cleanly")

        except Exception as e:
            if session:
                session.status = "error"
                session.error  = str(e)
                session.add_log("ERROR", f"Unhandled: {str(e)}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _url_slug(url: str) -> str:
    try:
        p    = urllib.parse.urlparse(url)
        path = p.path.strip("/").replace("/", "_")[-40:]
        return path or p.netloc.replace(".", "_")[:30]
    except Exception:
        return "request"


# ─────────────────────────────────────────────────────────────────────────────
# Flask Blueprint
# ─────────────────────────────────────────────────────────────────────────────

bp_fetcher = Blueprint("playwright_fetcher", __name__, url_prefix="/api/fetcher")
_manager: PlaywrightFetcherManager | None = None


def init_fetcher_manager():
    global _manager
    _manager = PlaywrightFetcherManager()


def _mgr() -> PlaywrightFetcherManager:
    if _manager is None:
        raise RuntimeError("Call init_fetcher_manager() at app startup")
    return _manager


@bp_fetcher.route("/sessions", methods=["POST"])
def start_session():
    d      = flask_request.json or {}
    domain = (d.get("domain") or "").strip().lstrip("https://").lstrip("http://").rstrip("/")
    if not domain:
        return jsonify({"ok": False, "error": "domain required"}), 400
    sid    = f"fetch-{int(time.time())}"
    result = _mgr().start_session(sid, domain)
    return jsonify(result), (201 if result["ok"] else 400)


@bp_fetcher.route("/sessions/<sid>/state", methods=["GET"])
def get_state(sid: str):
    state = _mgr().get_state(sid)
    if not state:
        return jsonify({"ok": False, "error": "Session not found"}), 404
    return jsonify({"ok": True, **state})


@bp_fetcher.route("/sessions/<sid>/navigate", methods=["POST"])
def navigate(sid: str):
    d   = flask_request.json or {}
    url = (d.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "url required"}), 400
    return jsonify(_mgr().navigate(sid, url))


@bp_fetcher.route("/sessions/<sid>/save", methods=["POST"])
def save_requests(sid: str):
    d       = flask_request.json or {}
    req_ids = d.get("req_ids", [])
    label   = d.get("label", "")
    if not req_ids:
        return jsonify({"ok": False, "error": "req_ids required"}), 400
    return jsonify(_mgr().save_requests(sid, req_ids, label))


@bp_fetcher.route("/sessions/<sid>/label", methods=["POST"])
def label_request(sid: str):
    d      = flask_request.json or {}
    req_id = d.get("req_id", "")
    label  = d.get("label", "")
    return jsonify(_mgr().label_request(sid, req_id, label))


@bp_fetcher.route("/sessions/<sid>/clear", methods=["POST"])
def clear_captures(sid: str):
    return jsonify(_mgr().clear_captures(sid))


@bp_fetcher.route("/sessions/<sid>", methods=["DELETE"])
def stop_session(sid: str):
    _mgr().stop_session(sid)
    return jsonify({"ok": True})


@bp_fetcher.route("/sessions/<sid>/request/<req_id>/curl", methods=["GET"])
def get_curl(sid: str, req_id: str):
    s = _SESSIONS.get(sid)
    if not s:
        return jsonify({"ok": False, "error": "session not found"}), 404
    cap = next((c for c in s.captures if c.req_id == req_id), None)
    if not cap:
        return jsonify({"ok": False, "error": "request not found"}), 404
    resp = make_response(cap.to_curl())
    resp.headers["Content-Type"] = "text/plain"
    return resp


@bp_fetcher.route("/sessions/<sid>/request/<req_id>/body", methods=["GET"])
def get_body(sid: str, req_id: str):
    s = _SESSIONS.get(sid)
    if not s:
        return jsonify({"ok": False, "error": "session not found"}), 404
    cap = next((c for c in s.captures if c.req_id == req_id), None)
    if not cap:
        return jsonify({"ok": False, "error": "request not found"}), 404
    resp = make_response(cap.body_raw)
    resp.headers["Content-Type"] = cap.content_type or "application/octet-stream"
    return resp