"""
app/services/request_interceptor_webhook.py
=============================================
Webhook receiver for browser-injected request interceptor.

The browser JS snippet POSTs every intercepted request+response to:
  POST /api/interceptor/collect

Captured entries are stored in-memory (per session/tab) and optionally
saved to MinIO on demand.

Routes:
  POST /api/interceptor/collect          ← receives from browser snippet
  GET  /api/interceptor/sessions         ← list all active sessions
  GET  /api/interceptor/sessions/<sid>   ← poll session state + entries
  POST /api/interceptor/sessions/<sid>/save   ← save selected entries to MinIO
  POST /api/interceptor/sessions/<sid>/clear  ← clear entries
  DELETE /api/interceptor/sessions/<sid>      ← remove session
  GET  /api/interceptor/sessions/<sid>/entries/<eid>/body  ← raw response body
  GET  /api/interceptor/sessions/<sid>/entries/<eid>/curl  ← curl command
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shlex
import time
import urllib.parse
from dataclasses import dataclass, field
from typing import Any

try:
    from minio import Minio
    _MINIO_OK = True
except ImportError:
    _MINIO_OK = False

from flask import Blueprint, request as flask_request, jsonify, make_response

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = "interceptor"
MINIO_SECURE     = os.environ.get("MINIO_SECURE",     "false").lower() == "true"

# Max entries kept per session before oldest are dropped
MAX_ENTRIES = 2000

# ─────────────────────────────────────────────────────────────────────────────
# Noise filter — skip tracking/analytics/static assets
# ─────────────────────────────────────────────────────────────────────────────

_NOISE = re.compile(
    r"\.(png|jpe?g|gif|svg|ico|webp|woff2?|ttf|eot|otf|css)"
    r"|google-analytics|googletagmanager|doubleclick|facebook\.net"
    r"|hotjar|sentry\.io|beacon|pixel|clarity\.ms"
    r"|snapchat\.com|adservice\.google|adnxs\.com"
    r"|bidr\.io|eskimi\.com|sc-static\.net|zdassets\.com"
    r"|livechat|zendesk",
    re.I,
)

# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class InterceptedEntry:
    entry_id:        str
    session_id:      str
    url:             str
    method:          str
    status:          int
    req_headers:     dict
    resp_headers:    dict
    req_body:        str | None       # request payload (POST body etc)
    resp_body:       str              # response as text
    resp_body_bytes: bytes            # response as bytes for storage
    content_type:    str
    size:            int
    duration_ms:     int              # how long the request took
    ts:              float = field(default_factory=time.time)
    saved:           bool  = False
    minio_paths:     dict  = field(default_factory=dict)
    label:           str   = ""
    tab_url:         str   = ""       # the page URL where the request was made
    tab_title:       str   = ""

    @property
    def is_json(self) -> bool:
        return "json" in self.content_type.lower()

    @property
    def body_preview(self) -> str:
        if self.is_json:
            try:
                return json.dumps(json.loads(self.resp_body), indent=2)[:3000]
            except Exception:
                pass
        return self.resp_body[:3000]

    def to_curl(self) -> str:
        parts = ["curl", "-X", self.method, shlex.quote(self.url)]
        skip  = {":method", ":path", ":authority", ":scheme", "content-length"}
        for k, v in self.req_headers.items():
            if k.lower() not in skip:
                parts += ["-H", shlex.quote(f"{k}: {v}")]
        if self.req_body:
            parts += ["--data-raw", shlex.quote(self.req_body[:500])]
        parts += ["--compressed"]
        return " \\\n  ".join(parts)

    def to_wire(self) -> dict:
        return {
            "entry_id":     self.entry_id,
            "session_id":   self.session_id,
            "url":          self.url,
            "method":       self.method,
            "status":       self.status,
            "content_type": self.content_type,
            "size":         self.size,
            "duration_ms":  self.duration_ms,
            "ts":           round(self.ts, 3),
            "saved":        self.saved,
            "minio_paths":  self.minio_paths,
            "label":        self.label,
            "tab_url":      self.tab_url,
            "tab_title":    self.tab_title,
            "is_json":      self.is_json,
            "body_preview": self.body_preview,
            "req_headers":  self.req_headers,
            "resp_headers": self.resp_headers,
            "req_body":     self.req_body,
        }


@dataclass
class InterceptorSession:
    session_id:  str
    created_at:  float = field(default_factory=time.time)
    last_seen:   float = field(default_factory=time.time)
    entries:     list  = field(default_factory=list)   # list[InterceptedEntry]
    tab_url:     str   = ""
    tab_title:   str   = ""
    _seen:       set   = field(default_factory=set)    # dedup: method:url

    def touch(self):
        self.last_seen = time.time()

    def get_state(self) -> dict:
        return {
            "session_id": self.session_id,
            "created_at": self.created_at,
            "last_seen":  self.last_seen,
            "tab_url":    self.tab_url,
            "tab_title":  self.tab_title,
            "count":      len(self.entries),
            "saved":      sum(1 for e in self.entries if e.saved),
            "entries":    [e.to_wire() for e in self.entries],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Session store
# ─────────────────────────────────────────────────────────────────────────────

_SESSIONS: dict[str, InterceptorSession] = {}


def _get_or_create_session(session_id: str) -> InterceptorSession:
    if session_id not in _SESSIONS:
        _SESSIONS[session_id] = InterceptorSession(session_id=session_id)
    return _SESSIONS[session_id]


# ─────────────────────────────────────────────────────────────────────────────
# MinIO
# ─────────────────────────────────────────────────────────────────────────────

def _minio_client():
    if not _MINIO_OK:
        return None
    try:
        c = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                  secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)
        if not c.bucket_exists(MINIO_BUCKET):
            c.make_bucket(MINIO_BUCKET)
        return c
    except Exception as e:
        print(f"[Interceptor] MinIO init error: {e}")
        return None


def _save_entry_to_minio(session_id: str, entry: InterceptedEntry, label: str) -> dict:
    client = _minio_client()
    if not client:
        return {}

    slug   = re.sub(r"[^a-z0-9_]", "_", label.lower())[:40] or "entry"
    ts     = int(entry.ts)
    prefix = f"interceptor/{session_id}/{slug}/{ts}_{entry.entry_id}"
    paths: dict[str, str] = {}

    # 1. curl command
    curl_bytes = entry.to_curl().encode()
    try:
        client.put_object(MINIO_BUCKET, f"{prefix}_curl.txt",
                          io.BytesIO(curl_bytes), len(curl_bytes), content_type="text/plain")
        paths["curl"] = f"{MINIO_BUCKET}/{prefix}_curl.txt"
    except Exception as e:
        print(f"[Interceptor] MinIO curl error: {e}")

    # 2. response body
    body_bytes = entry.resp_body_bytes or entry.resp_body.encode("utf-8", errors="replace")
    if body_bytes:
        ext = ".json" if entry.is_json else ".txt"
        try:
            client.put_object(MINIO_BUCKET, f"{prefix}_response{ext}",
                              io.BytesIO(body_bytes), len(body_bytes),
                              content_type=entry.content_type or "text/plain")
            paths["response"] = f"{MINIO_BUCKET}/{prefix}_response{ext}"
        except Exception as e:
            print(f"[Interceptor] MinIO body error: {e}")

    # 3. full metadata
    meta = {
        "session_id":   session_id,
        "entry_id":     entry.entry_id,
        "url":          entry.url,
        "method":       entry.method,
        "status":       entry.status,
        "content_type": entry.content_type,
        "size":         entry.size,
        "duration_ms":  entry.duration_ms,
        "ts":           entry.ts,
        "tab_url":      entry.tab_url,
        "tab_title":    entry.tab_title,
        "req_headers":  entry.req_headers,
        "resp_headers": entry.resp_headers,
        "req_body":     entry.req_body,
        "label":        label,
        "minio_paths":  paths,
    }
    meta_bytes = json.dumps(meta, indent=2).encode()
    try:
        client.put_object(MINIO_BUCKET, f"{prefix}_meta.json",
                          io.BytesIO(meta_bytes), len(meta_bytes), content_type="application/json")
        paths["meta"] = f"{MINIO_BUCKET}/{prefix}_meta.json"
    except Exception as e:
        print(f"[Interceptor] MinIO meta error: {e}")

    return paths


# ─────────────────────────────────────────────────────────────────────────────
# Blueprint
# ─────────────────────────────────────────────────────────────────────────────

bp_interceptor = Blueprint("request_interceptor", __name__, url_prefix="/api/interceptor")


# ── Collect (called by browser snippet) ──────────────────────────────────────

@bp_interceptor.route("/collect", methods=["POST", "OPTIONS"])
def collect():
    # CORS — the snippet runs from any domain
    if flask_request.method == "OPTIONS":
        resp = make_response("", 204)
        resp.headers["Access-Control-Allow-Origin"]  = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Session-ID"
        return resp

    try:
        data = flask_request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    session_id = (
        flask_request.headers.get("X-Session-ID") or
        data.get("session_id") or
        "default"
    )

    url         = data.get("url", "")
    method      = data.get("method", "GET").upper()
    status      = int(data.get("status", 0))
    req_headers = data.get("req_headers") or {}
    resp_headers= data.get("resp_headers") or {}
    req_body    = data.get("req_body")
    resp_body   = data.get("resp_body") or ""
    content_type= data.get("content_type") or resp_headers.get("content-type", "")
    duration_ms = int(data.get("duration_ms") or 0)
    tab_url     = data.get("tab_url") or ""
    tab_title   = data.get("tab_title") or ""

    # Skip noise
    if _NOISE.search(url):
        resp = make_response(jsonify({"ok": True, "skipped": True}))
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    # Skip empty responses
    if not resp_body and status == 0:
        resp = make_response(jsonify({"ok": True, "skipped": True}))
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    session = _get_or_create_session(session_id)
    session.touch()
    if tab_url:   session.tab_url   = tab_url
    if tab_title: session.tab_title = tab_title

    # Dedup: same method+url within this session
    dedup_key = f"{method}:{url}"
    is_dupe = dedup_key in session._seen
    if not is_dupe:
        session._seen.add(dedup_key)

    # Always store (dedup just marks it)
    entry_id = hashlib.md5(f"{method}:{url}:{session_id}".encode()).hexdigest()[:14]

    # If it's a dupe, update the existing entry's body (fresher response)
    if is_dupe:
        existing = next((e for e in session.entries if e.entry_id == entry_id), None)
        if existing and not existing.saved:
            existing.resp_body       = resp_body
            existing.resp_body_bytes = resp_body.encode("utf-8", errors="replace")
            existing.status          = status
            existing.ts              = time.time()
        resp = make_response(jsonify({"ok": True, "updated": True, "entry_id": entry_id}))
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    resp_bytes = resp_body.encode("utf-8", errors="replace") if isinstance(resp_body, str) else resp_body

    entry = InterceptedEntry(
        entry_id        = entry_id,
        session_id      = session_id,
        url             = url,
        method          = method,
        status          = status,
        req_headers     = req_headers,
        resp_headers    = resp_headers,
        req_body        = req_body,
        resp_body       = resp_body if isinstance(resp_body, str) else json.dumps(resp_body),
        resp_body_bytes = resp_bytes,
        content_type    = content_type.split(";")[0].strip(),
        size            = len(resp_bytes),
        duration_ms     = duration_ms,
        tab_url         = tab_url,
        tab_title       = tab_title,
    )

    session.entries.append(entry)

    # Trim if too large
    if len(session.entries) > MAX_ENTRIES:
        session.entries = session.entries[-MAX_ENTRIES:]

    resp = make_response(jsonify({"ok": True, "entry_id": entry_id}))
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


# ── Session management ────────────────────────────────────────────────────────

@bp_interceptor.route("/sessions", methods=["GET"])
def list_sessions():
    sessions = [
        {
            "session_id": s.session_id,
            "tab_url":    s.tab_url,
            "tab_title":  s.tab_title,
            "count":      len(s.entries),
            "saved":      sum(1 for e in s.entries if e.saved),
            "last_seen":  s.last_seen,
            "created_at": s.created_at,
        }
        for s in sorted(_SESSIONS.values(), key=lambda x: x.last_seen, reverse=True)
    ]
    return jsonify({"ok": True, "sessions": sessions})


@bp_interceptor.route("/sessions/<sid>", methods=["GET"])
def get_session(sid: str):
    s = _SESSIONS.get(sid)
    if not s:
        return jsonify({"ok": False, "error": "session not found"}), 404
    return jsonify({"ok": True, **s.get_state()})


@bp_interceptor.route("/sessions/<sid>/save", methods=["POST"])
def save_entries(sid: str):
    s = _SESSIONS.get(sid)
    if not s:
        return jsonify({"ok": False, "error": "session not found"}), 404

    d        = flask_request.json or {}
    entry_ids= d.get("entry_ids", [])
    label    = d.get("label", "")

    saved, errors = [], []
    for eid in entry_ids:
        entry = next((e for e in s.entries if e.entry_id == eid), None)
        if not entry:
            errors.append(f"{eid}: not found"); continue
        use_label = label or entry.label or _url_slug(entry.url)
        paths = _save_entry_to_minio(sid, entry, use_label)
        if paths:
            entry.saved = True; entry.minio_paths = paths
            saved.append({"entry_id": eid, "paths": paths})
        else:
            errors.append(f"{eid}: minio unavailable")

    return jsonify({"ok": True, "saved": saved, "errors": errors})


@bp_interceptor.route("/sessions/<sid>/label", methods=["POST"])
def label_entry(sid: str):
    s  = _SESSIONS.get(sid)
    d  = flask_request.json or {}
    if not s:
        return jsonify({"ok": False, "error": "session not found"}), 404
    entry = next((e for e in s.entries if e.entry_id == d.get("entry_id")), None)
    if not entry:
        return jsonify({"ok": False, "error": "entry not found"}), 404
    entry.label = d.get("label", "")
    return jsonify({"ok": True})


@bp_interceptor.route("/sessions/<sid>/clear", methods=["POST"])
def clear_session(sid: str):
    s = _SESSIONS.get(sid)
    if not s:
        return jsonify({"ok": False, "error": "session not found"}), 404
    s.entries.clear()
    s._seen.clear()
    return jsonify({"ok": True})


@bp_interceptor.route("/sessions/<sid>", methods=["DELETE"])
def delete_session(sid: str):
    _SESSIONS.pop(sid, None)
    return jsonify({"ok": True})


@bp_interceptor.route("/sessions/<sid>/entries/<eid>/curl", methods=["GET"])
def get_curl(sid: str, eid: str):
    s = _SESSIONS.get(sid)
    if not s:
        return jsonify({"ok": False, "error": "not found"}), 404
    entry = next((e for e in s.entries if e.entry_id == eid), None)
    if not entry:
        return jsonify({"ok": False, "error": "not found"}), 404
    resp = make_response(entry.to_curl())
    resp.headers["Content-Type"] = "text/plain"
    return resp


@bp_interceptor.route("/sessions/<sid>/entries/<eid>/body", methods=["GET"])
def get_body(sid: str, eid: str):
    s = _SESSIONS.get(sid)
    if not s:
        return jsonify({"ok": False, "error": "not found"}), 404
    entry = next((e for e in s.entries if e.entry_id == eid), None)
    if not entry:
        return jsonify({"ok": False, "error": "not found"}), 404
    resp = make_response(entry.resp_body_bytes or entry.resp_body.encode())
    resp.headers["Content-Type"] = entry.content_type or "text/plain"
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _url_slug(url: str) -> str:
    try:
        p    = urllib.parse.urlparse(url)
        path = p.path.strip("/").replace("/", "_")[-40:]
        return path or p.netloc.replace(".", "_")[:30]
    except Exception:
        return "entry"