"""
app/services/playwright_fetcher.py
====================================
Playwright Request Fetcher — intercepts ALL network requests from a launched
Chromium instance, stores curl + response bodies to MinIO, streams state to
frontend via polling REST API.

Architecture:
  • POST /fetcher/sessions          → launch Chromium (headless=False, VNC/CDP)
  • GET  /fetcher/sessions/<id>/state  → poll for captured requests
  • POST /fetcher/sessions/<id>/save   → save selected request(s) to MinIO
  • GET  /fetcher/sessions/<id>/stream → CDP WS url for iframe embedding
  • DELETE /fetcher/sessions/<id>   → kill session

MinIO:
  Stores under: fetcher/<session_id>/<phase_slug>/<ts>_curl.txt
                fetcher/<session_id>/<phase_slug>/<ts>_response.json|bin

Requirements:
  pip install playwright minio
  playwright install chromium
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import mimetypes
import re
import shlex
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

# ─── Playwright ───────────────────────────────────────────────────────────────
try:
    from playwright.async_api import async_playwright, Response as PwResponse, Request as PwRequest
    _PW_OK = True
except ImportError:
    _PW_OK = False

# ─── MinIO ────────────────────────────────────────────────────────────────────
try:
    from minio import Minio
    from minio.error import S3Error
    _MINIO_OK = True
except ImportError:
    _MINIO_OK = False

# ─── Flask ────────────────────────────────────────────────────────────────────
from flask import Blueprint, request as flask_request, jsonify

import os

# ─────────────────────────────────────────────────────────────────────────────
# Config  (override via env)
# ─────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "fetcher")
MINIO_SECURE     = os.environ.get("MINIO_SECURE",     "false").lower() == "true"

# CDP remote debugging port per session  (base + session index)
_CDP_PORT_BASE = int(os.environ.get("CDP_PORT_BASE", "9222"))

# Patterns to silently ignore (images, fonts, analytics…)
_NOISE = re.compile(
    r"\.(png|jpe?g|gif|svg|ico|webp|woff2?|ttf|eot|otf|css|js\.map)"
    r"|google-analytics|googletagmanager|doubleclick|facebook\.net"
    r"|hotjar|sentry|beacon|pixel|cdn\.jsdelivr|cdnjs\.cloudflare",
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
    minio_paths:  dict  = field(default_factory=dict)   # {curl, response}
    label:        str   = ""   # user-editable friendly name

    @property
    def body_text(self) -> str:
        try:
            return self.body_raw.decode("utf-8", errors="replace")
        except Exception:
            return ""

    @property
    def body_preview(self) -> str:
        return self.body_text[:1200]

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
        """Safe dict for JSON serialisation."""
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
        """Build a ready-to-paste curl command."""
        parts = ["curl", "-X", self.method, shlex.quote(self.url)]
        skip = {":method", ":path", ":authority", ":scheme", "content-length"}
        for k, v in self.req_headers.items():
            if k.lower() not in skip:
                parts += ["-H", shlex.quote(f"{k}: {v}")]
        if self.post_data:
            parts += ["--data-raw", shlex.quote(self.post_data)]
        parts += ["--compressed"]
        return " \\\n  ".join(parts)


@dataclass
class FetcherSession:
    session_id:  str
    domain:      str
    status:      str   = "launching"   # launching|active|error|stopped
    error:       str   = ""
    page_url:    str   = ""
    page_title:  str   = ""
    cdp_port:    int   = 0
    captures:    list  = field(default_factory=list)   # list[CapturedRequest]
    logs:        list  = field(default_factory=list)   # list[dict]
    is_active:   bool  = True
    started_at:  float = field(default_factory=time.time)

    def add_log(self, level: str, msg: str):
        self.logs.append({"level": level, "msg": msg, "ts": time.time()})
        if len(self.logs) > 300:
            self.logs = self.logs[-300:]

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
            "logs":       self.logs[-80:],
            "count":      len(self.captures),
        }


# ─────────────────────────────────────────────────────────────────────────────
# MinIO helper
# ─────────────────────────────────────────────────────────────────────────────

def _minio_client() -> "Minio | None":
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


def _save_to_minio(session_id: str, cap: CapturedRequest, label: str) -> dict[str, str]:
    """
    Save curl.txt and response body to MinIO.
    Returns {curl: <object_path>, response: <object_path>}
    """
    client = _minio_client()
    if client is None:
        return {}

    slug = re.sub(r"[^a-z0-9_]", "_", label.lower())[:40] or "request"
    ts   = int(cap.ts)
    prefix = f"fetcher/{session_id}/{slug}/{ts}"

    paths: dict[str, str] = {}

    # ── 1. Save curl command ──────────────────────────────────────────────────
    curl_txt  = cap.to_curl().encode()
    curl_path = f"{prefix}_curl.txt"
    try:
        import io
        client.put_object(
            MINIO_BUCKET, curl_path,
            io.BytesIO(curl_txt), len(curl_txt),
            content_type="text/plain",
        )
        paths["curl"] = f"{MINIO_BUCKET}/{curl_path}"
    except Exception as e:
        print(f"[Fetcher] MinIO curl upload error: {e}")

    # ── 2. Save response body ─────────────────────────────────────────────────
    if cap.body_raw:
        ext = ".json" if cap.is_json else ".bin"
        resp_path = f"{prefix}_response{ext}"
        ct        = cap.content_type or "application/octet-stream"
        try:
            import io
            client.put_object(
                MINIO_BUCKET, resp_path,
                io.BytesIO(cap.body_raw), len(cap.body_raw),
                content_type=ct,
            )
            paths["response"] = f"{MINIO_BUCKET}/{resp_path}"
        except Exception as e:
            print(f"[Fetcher] MinIO response upload error: {e}")

    # ── 3. Save metadata JSON ─────────────────────────────────────────────────
    meta = {
        "session_id":   session_id,
        "url":          cap.url,
        "method":       cap.method,
        "status":       cap.status,
        "content_type": cap.content_type,
        "size":         cap.size,
        "ts":           cap.ts,
        "req_headers":  cap.req_headers,
        "resp_headers": cap.resp_headers,
        "post_data":    cap.post_data,
        "label":        label,
        "minio_paths":  paths,
    }
    meta_bytes = json.dumps(meta, indent=2).encode()
    meta_path  = f"{prefix}_meta.json"
    try:
        import io
        client.put_object(
            MINIO_BUCKET, meta_path,
            io.BytesIO(meta_bytes), len(meta_bytes),
            content_type="application/json",
        )
        paths["meta"] = f"{MINIO_BUCKET}/{meta_path}"
    except Exception as e:
        print(f"[Fetcher] MinIO meta upload error: {e}")

    return paths


# ─────────────────────────────────────────────────────────────────────────────
# Session store
# ─────────────────────────────────────────────────────────────────────────────

_SESSIONS: dict[str, FetcherSession] = {}
_PORT_COUNTER = [0]  # simple atomic-ish counter


# ─────────────────────────────────────────────────────────────────────────────
# Manager
# ─────────────────────────────────────────────────────────────────────────────

class PlaywrightFetcherManager:

    def start_session(self, session_id: str, domain: str) -> dict:
        if not _PW_OK:
            return {"ok": False, "error": "playwright not installed — run: pip install playwright && playwright install chromium"}
        if session_id in _SESSIONS:
            return {"ok": False, "error": "Session already exists"}

        _PORT_COUNTER[0] += 1
        cdp_port = _CDP_PORT_BASE + _PORT_COUNTER[0]

        session          = FetcherSession(session_id=session_id, domain=domain, cdp_port=cdp_port)
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

    def save_requests(self, session_id: str, req_ids: list[str], label: str) -> dict:
        s = _SESSIONS.get(session_id)
        if not s:
            return {"ok": False, "error": "session not found"}

        saved = []
        errors = []
        for req_id in req_ids:
            cap = next((c for c in s.captures if c.req_id == req_id), None)
            if not cap:
                errors.append(f"{req_id}: not found")
                continue
            cap_label = label or cap.label or _url_slug(cap.url)
            paths = _save_to_minio(session_id, cap, cap_label)
            if paths:
                cap.saved        = True
                cap.minio_paths  = paths
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
        s.add_log("INFO", "Captures cleared")
        return {"ok": True}

    def stop_session(self, session_id: str):
        s = _SESSIONS.get(session_id)
        if s:
            s.is_active = False
            s.status    = "stopped"
        _SESSIONS.pop(session_id, None)

    # ── Playwright async ───────────────────────────────────────────────────────

    def _run_thread(self, session_id: str, cdp_port: int):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._async_session(session_id, cdp_port))
        except Exception as e:
            s = _SESSIONS.get(session_id)
            if s:
                s.status = "error"
                s.error  = str(e)
                s.add_log("ERROR", str(e))
        finally:
            loop.close()

    async def _async_session(self, session_id: str, cdp_port: int):
        session = _SESSIONS.get(session_id)
        if not session:
            return

        session.add_log("INFO", f"Launching Chromium on CDP port {cdp_port}…")

        try:
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        f"--remote-debugging-port={cdp_port}",
                        "--remote-debugging-address=0.0.0.0",
                    ],
                )

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

                # ── Response interceptor ────────────────────────────────────────
                async def on_response(response):
                    try:
                        url = response.url
                        if _NOISE.search(url):
                            return
                        req = response.request
                        try:
                            body = await response.body()
                        except Exception:
                            body = b""
                        ct = (response.headers.get("content-type") or "").lower()
                        if len(body) < 20 and "json" not in ct:
                            return
                        req_id = hashlib.md5(f"{url}:{req.method}:{time.time()}".encode()).hexdigest()[:12]
                        cap = CapturedRequest(
                            req_id=req_id, url=url, method=req.method,
                            status=response.status,
                            req_headers=dict(req.headers),
                            resp_headers=dict(response.headers),
                            post_data=req.post_data,
                            body_raw=body, content_type=ct, size=len(body),
                        )
                        session.captures.append(cap)
                        session.add_log(
                            "CAPTURE",
                            f"{req.method} {url[:90]} → {response.status} "
                            f"({len(body)//1024}KB) [{ct.split(';')[0]}]"
                        )
                        # update page url on every capture too
                        try:
                            session.page_url = page.url
                        except Exception:
                            pass
                    except Exception as e:
                        session.add_log("DEBUG", f"response handler: {e}")

                page.on("response", on_response)

                # ── Nav tracker ─────────────────────────────────────────────────
                def on_url(url: str):
                    if not url.startswith("data:"):
                        session.page_url = url
                        session.add_log("INFO", f"Navigated: {url[:80]}")

                page.on("url", on_url)

                # ── Load initial URL ─────────────────────────────────────────────
                domain = session.domain
                start_url = domain if domain.startswith("http") else f"https://{domain}"

                session.add_log("INFO", f"Navigating to {start_url}…")

                try:
                    await page.goto(start_url, wait_until="domcontentloaded", timeout=30_000)
                except Exception as e:
                    # domcontentloaded may fire but goto still throws on slow SPAs — 
                    # check if page actually loaded before giving up
                    session.add_log("WARN", f"goto warning (continuing): {str(e)[:120]}")

                # Always mark active after goto attempt — page may have partially loaded
                try:
                    session.page_url   = page.url
                    session.page_title = await page.title()
                except Exception:
                    session.page_url   = start_url
                    session.page_title = ""

                session.status = "active"
                session.add_log("SUCCESS", f"Active: {session.page_url} — {session.page_title or '(no title)'}")

                # ── Keep alive ──────────────────────────────────────────────────
                while session.is_active:
                    await asyncio.sleep(1)
                    try:
                        # refresh url + title periodically
                        session.page_url   = page.url
                        session.page_title = await page.title()
                    except Exception:
                        session.add_log("INFO", "Page closed or crashed")
                        break

                try:
                    await browser.close()
                except Exception:
                    pass

                if session.status not in ("error", "stopped"):
                    session.status = "stopped"
                    session.add_log("INFO", "Session ended")

        except Exception as e:
            session.status = "error"
            session.error  = str(e)
            session.add_log("ERROR", f"Fatal: {str(e)}")
# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _url_slug(url: str) -> str:
    try:
        p = urllib.parse.urlparse(url)
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
    d            = flask_request.json or {}
    domain       = (d.get("domain") or "").strip().lstrip("https://").lstrip("http://").rstrip("/")
    if not domain:
        return jsonify({"ok": False, "error": "domain required"}), 400
    session_id   = f"fetch-{int(time.time())}"
    result       = _mgr().start_session(session_id, domain)
    return jsonify(result), (201 if result["ok"] else 400)


@bp_fetcher.route("/sessions/<sid>/state", methods=["GET"])
def get_state(sid: str):
    state = _mgr().get_state(sid)
    if not state:
        return jsonify({"ok": False, "error": "Session not found"}), 404
    return jsonify({"ok": True, **state})


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
    """Return the raw curl command for a captured request."""
    from flask import make_response
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
    """Return raw response body."""
    from flask import make_response
    s = _SESSIONS.get(sid)
    if not s:
        return jsonify({"ok": False, "error": "session not found"}), 404
    cap = next((c for c in s.captures if c.req_id == req_id), None)
    if not cap:
        return jsonify({"ok": False, "error": "request not found"}), 404
    resp = make_response(cap.body_raw)
    resp.headers["Content-Type"] = cap.content_type or "application/octet-stream"
    return resp