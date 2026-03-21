"""
app/services/request_interceptor_webhook.py
"""
from __future__ import annotations
import hashlib, io, json, os, re, shlex, time, urllib.parse
from dataclasses import dataclass, field
from typing import Any

try:
    from minio import Minio
    _MINIO_OK = True
except ImportError:
    _MINIO_OK = False
    print("[Interceptor] WARNING: pip install minio")

from flask import Blueprint, request as flask_request, jsonify, make_response

MINIO_ENDPOINT   = os.environ.get("STORAGE_ENDPOINT",   "5.78.137.59:6500")
MINIO_ACCESS_KEY = os.environ.get("STORAGE_ACCESS_KEY", "gidraf")
MINIO_SECRET_KEY = os.environ.get("STORAGE_SECRET_KEY", "Winners1127")
MINIO_BUCKET     = os.environ.get("STORAGE_BUCKET",     "default")
MINIO_SECURE     = os.environ.get("STORAGE_USE_SSL",    "false").lower() == "true"
MAX_ENTRIES      = 2000

_NOISE = re.compile(
    r"\.(png|jpe?g|gif|svg|ico|webp|woff2?|ttf|eot|otf|css)(\?|$)"
    r"|google-analytics|googletagmanager|doubleclick|facebook\.net"
    r"|hotjar|sentry\.io|beacon|pixel|clarity\.ms|snapchat\.com"
    r"|adservice\.google|adnxs\.com|bidr\.io|eskimi\.com"
    r"|sc-static\.net|zdassets\.com|livechat|zendesk",
    re.I,
)

@dataclass
class InterceptedEntry:
    entry_id: str; session_id: str; url: str; method: str; status: int
    req_headers: dict; resp_headers: dict; req_body: str | None
    resp_body: str; resp_body_bytes: bytes; content_type: str
    size: int; duration_ms: int
    ts: float = field(default_factory=time.time)
    saved: bool = False; minio_paths: dict = field(default_factory=dict)
    label: str = ""; tab_url: str = ""; tab_title: str = ""; save_error: str = ""

    @property
    def is_json(self): return "json" in self.content_type.lower()

    @property
    def body_preview(self):
        if self.is_json:
            try: return json.dumps(json.loads(self.resp_body), indent=2)[:3000]
            except: pass
        return self.resp_body[:3000]

    def to_curl(self):
        parts = ["curl", "-X", self.method, shlex.quote(self.url)]
        skip  = {":method", ":path", ":authority", ":scheme", "content-length"}
        for k, v in self.req_headers.items():
            if k.lower() not in skip: parts += ["-H", shlex.quote(f"{k}: {v}")]
        if self.req_body: parts += ["--data-raw", shlex.quote(self.req_body[:500])]
        parts += ["--compressed"]
        return " \\\n  ".join(parts)

    def to_wire(self):
        return {
            "entry_id": self.entry_id, "session_id": self.session_id,
            "url": self.url, "method": self.method, "status": self.status,
            "content_type": self.content_type, "size": self.size,
            "duration_ms": self.duration_ms, "ts": round(self.ts, 3),
            "saved": self.saved, "save_error": self.save_error,
            "minio_paths": self.minio_paths, "label": self.label,
            "tab_url": self.tab_url, "tab_title": self.tab_title,
            "is_json": self.is_json, "body_preview": self.body_preview,
            "req_headers": self.req_headers, "resp_headers": self.resp_headers,
            "req_body": self.req_body,
        }

@dataclass
class InterceptorSession:
    session_id: str
    created_at: float = field(default_factory=time.time)
    last_seen:  float = field(default_factory=time.time)
    entries:    list  = field(default_factory=list)
    tab_url:    str   = ""; tab_title: str = ""
    _seen:      set   = field(default_factory=set)

    def touch(self): self.last_seen = time.time()

    def get_state(self):
        return {
            "session_id": self.session_id, "created_at": self.created_at,
            "last_seen": self.last_seen, "tab_url": self.tab_url,
            "tab_title": self.tab_title, "count": len(self.entries),
            "saved": sum(1 for e in self.entries if e.saved),
            "entries": [e.to_wire() for e in self.entries],
        }

_SESSIONS: dict[str, InterceptorSession] = {}

def _get_or_create_session(sid):
    if sid not in _SESSIONS:
        _SESSIONS[sid] = InterceptorSession(session_id=sid)
    return _SESSIONS[sid]

def _minio_client():
    if not _MINIO_OK:
        return None, "minio not installed — pip install minio"
    try:
        c = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
            region=os.environ.get("STORAGE_REGION", "us-east-1"),
        )
        c.list_buckets()
        if not c.bucket_exists(MINIO_BUCKET):
            c.make_bucket(MINIO_BUCKET)
            print(f"[Interceptor] Created bucket: {MINIO_BUCKET}")
        return c, ""
    except Exception as e:
        import traceback
        msg = f"MinIO error ({MINIO_ENDPOINT}): {type(e).__name__}: {e}"
        print(f"[Interceptor] {msg}")
        print(traceback.format_exc())
        return None, msg
def _put(client, key, data_bytes, content_type):
    client.put_object(MINIO_BUCKET, key, io.BytesIO(data_bytes),
                      len(data_bytes), content_type=content_type)

def _save_entry_to_minio(session_id, entry, label):
    client, err = _minio_client()
    if not client:
        return {}, err

    slug   = re.sub(r"[^a-z0-9_]", "_", label.lower())[:40] or "entry"
    prefix = f"interceptor/{session_id}/{slug}/{int(entry.ts)}_{entry.entry_id}"
    paths  = {}; errors = []

    # curl
    try:
        b = entry.to_curl().encode("utf-8")
        _put(client, f"{prefix}_curl.txt", b, "text/plain")
        paths["curl"] = f"{MINIO_BUCKET}/{prefix}_curl.txt"
    except Exception as e: errors.append(f"curl: {e}")

    # response body
    body = entry.resp_body_bytes or entry.resp_body.encode("utf-8", errors="replace")
    if body:
        ext = ".json" if entry.is_json else ".txt"
        try:
            _put(client, f"{prefix}_response{ext}", body,
                 entry.content_type or "text/plain")
            paths["response"] = f"{MINIO_BUCKET}/{prefix}_response{ext}"
        except Exception as e: errors.append(f"body: {e}")

    # meta
    try:
        meta = {
            "session_id": session_id, "entry_id": entry.entry_id,
            "url": entry.url, "method": entry.method, "status": entry.status,
            "content_type": entry.content_type, "size": entry.size,
            "duration_ms": entry.duration_ms, "ts": entry.ts,
            "tab_url": entry.tab_url, "tab_title": entry.tab_title,
            "req_headers": entry.req_headers, "resp_headers": entry.resp_headers,
            "req_body": entry.req_body, "label": label, "minio_paths": paths,
        }
        b = json.dumps(meta, indent=2, ensure_ascii=False).encode("utf-8")
        _put(client, f"{prefix}_meta.json", b, "application/json")
        paths["meta"] = f"{MINIO_BUCKET}/{prefix}_meta.json"
    except Exception as e: errors.append(f"meta: {e}")

    err_str = "; ".join(errors)
    if err_str: print(f"[Interceptor] partial MinIO errors: {err_str}")
    return paths, err_str

bp_interceptor = Blueprint("request_interceptor", __name__, url_prefix="/api/interceptor")

# ── MinIO test ────────────────────────────────────────────────────────────────
@bp_interceptor.route("/minio/test", methods=["GET"])
def test_minio():
    client, err = _minio_client()
    if not client:
        return jsonify({"ok": False, "error": err,
                        "endpoint": MINIO_ENDPOINT, "bucket": MINIO_BUCKET,
                        "minio_installed": _MINIO_OK}), 500
    try:
        buckets = [b.name for b in client.list_buckets()]
    except Exception as e:
        buckets = [f"(error: {e})"]
    return jsonify({"ok": True, "endpoint": MINIO_ENDPOINT,
                    "bucket": MINIO_BUCKET, "buckets": buckets})

# ── Collect ───────────────────────────────────────────────────────────────────
@bp_interceptor.route("/collect", methods=["POST", "OPTIONS"])
def collect():
    def cors(r):
        r.headers["Access-Control-Allow-Origin"]  = "*"
        r.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        r.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Session-ID"
        return r

    if flask_request.method == "OPTIONS":
        return cors(make_response("", 204))

    data = flask_request.get_json(force=True, silent=True) or {}

    session_id   = flask_request.headers.get("X-Session-ID") or data.get("session_id") or "default"
    url          = data.get("url", "")
    method       = data.get("method", "GET").upper()
    status       = int(data.get("status", 0))
    req_headers  = data.get("req_headers") or {}
    resp_headers = data.get("resp_headers") or {}
    req_body     = data.get("req_body")
    resp_raw     = data.get("resp_body") or ""
    content_type = data.get("content_type") or resp_headers.get("content-type", "")
    duration_ms  = int(data.get("duration_ms") or 0)
    tab_url      = data.get("tab_url") or ""
    tab_title    = data.get("tab_title") or ""

    # normalise resp_body to str
    if isinstance(resp_raw, (dict, list)):
        resp_body = json.dumps(resp_raw)
    else:
        resp_body = str(resp_raw) if resp_raw else ""

    def ok(extra=None):
        return cors(make_response(jsonify({"ok": True, **(extra or {})})))

    if _NOISE.search(url):            return ok({"skipped": True})
    if not resp_body and status == 0: return ok({"skipped": True})

    session = _get_or_create_session(session_id)
    session.touch()
    if tab_url:   session.tab_url   = tab_url
    if tab_title: session.tab_title = tab_title

    dedup_key = f"{method}:{url}"
    is_dupe   = dedup_key in session._seen
    entry_id  = hashlib.md5(f"{method}:{url}:{session_id}".encode()).hexdigest()[:14]

    if not is_dupe:
        session._seen.add(dedup_key)
    else:
        ex = next((e for e in session.entries if e.entry_id == entry_id), None)
        if ex and not ex.saved:
            ex.resp_body       = resp_body
            ex.resp_body_bytes = resp_body.encode("utf-8", errors="replace")
            ex.status          = status
            ex.ts              = time.time()
        return ok({"updated": True, "entry_id": entry_id})

    resp_bytes = resp_body.encode("utf-8", errors="replace")
    entry = InterceptedEntry(
        entry_id=entry_id, session_id=session_id, url=url, method=method,
        status=status, req_headers=req_headers, resp_headers=resp_headers,
        req_body=req_body, resp_body=resp_body, resp_body_bytes=resp_bytes,
        content_type=content_type.split(";")[0].strip(),
        size=len(resp_bytes), duration_ms=duration_ms,
        tab_url=tab_url, tab_title=tab_title,
    )
    session.entries.append(entry)
    if len(session.entries) > MAX_ENTRIES:
        session.entries = session.entries[-MAX_ENTRIES:]

    return ok({"entry_id": entry_id})

# ── Sessions ──────────────────────────────────────────────────────────────────
@bp_interceptor.route("/sessions", methods=["GET"])
def list_sessions():
    rows = sorted(_SESSIONS.values(), key=lambda x: x.last_seen, reverse=True)
    return jsonify({"ok": True, "sessions": [
        {"session_id": s.session_id, "tab_url": s.tab_url, "tab_title": s.tab_title,
         "count": len(s.entries), "saved": sum(1 for e in s.entries if e.saved),
         "last_seen": s.last_seen, "created_at": s.created_at}
        for s in rows
    ]})

@bp_interceptor.route("/sessions/<sid>", methods=["GET"])
def get_session(sid):
    s = _SESSIONS.get(sid)
    if not s: return jsonify({"ok": False, "error": "not found"}), 404
    return jsonify({"ok": True, **s.get_state()})

@bp_interceptor.route("/sessions/<sid>/save", methods=["POST"])
def save_entries(sid):
    s = _SESSIONS.get(sid)
    if not s: return jsonify({"ok": False, "error": "not found"}), 404
    d         = flask_request.json or {}
    entry_ids = d.get("entry_ids", [])
    label     = d.get("label", "")
    if not entry_ids: return jsonify({"ok": False, "error": "entry_ids required"}), 400

    saved, errors = [], []
    for eid in entry_ids:
        entry = next((e for e in s.entries if e.entry_id == eid), None)
        if not entry:
            errors.append({"entry_id": eid, "error": "not found"}); continue
        use_label = label or entry.label or _url_slug(entry.url)
        paths, err = _save_entry_to_minio(sid, entry, use_label)
        if paths:
            entry.saved = True; entry.minio_paths = paths; entry.save_error = err
            saved.append({"entry_id": eid, "paths": paths, "partial_error": err})
        else:
            entry.save_error = err
            errors.append({"entry_id": eid, "error": err})

    return jsonify({"ok": True, "saved": saved, "errors": errors,
                    "minio": {"endpoint": MINIO_ENDPOINT, "bucket": MINIO_BUCKET,
                              "installed": _MINIO_OK}})

@bp_interceptor.route("/sessions/<sid>/label", methods=["POST"])
def label_entry(sid):
    s = _SESSIONS.get(sid)
    if not s: return jsonify({"ok": False, "error": "not found"}), 404
    d = flask_request.json or {}
    e = next((e for e in s.entries if e.entry_id == d.get("entry_id")), None)
    if not e: return jsonify({"ok": False, "error": "entry not found"}), 404
    e.label = d.get("label", "")
    return jsonify({"ok": True})

@bp_interceptor.route("/sessions/<sid>/clear", methods=["POST"])
def clear_session(sid):
    s = _SESSIONS.get(sid)
    if not s: return jsonify({"ok": False, "error": "not found"}), 404
    s.entries.clear(); s._seen.clear()
    return jsonify({"ok": True})

@bp_interceptor.route("/sessions/<sid>", methods=["DELETE"])
def delete_session(sid):
    _SESSIONS.pop(sid, None)
    return jsonify({"ok": True})

@bp_interceptor.route("/sessions/<sid>/entries/<eid>/curl", methods=["GET"])
def get_curl(sid, eid):
    s = _SESSIONS.get(sid)
    if not s: return jsonify({"ok": False}), 404
    e = next((e for e in s.entries if e.entry_id == eid), None)
    if not e: return jsonify({"ok": False}), 404
    r = make_response(e.to_curl()); r.headers["Content-Type"] = "text/plain"; return r

@bp_interceptor.route("/sessions/<sid>/entries/<eid>/body", methods=["GET"])
def get_body(sid, eid):
    s = _SESSIONS.get(sid)
    if not s: return jsonify({"ok": False}), 404
    e = next((e for e in s.entries if e.entry_id == eid), None)
    if not e: return jsonify({"ok": False}), 404
    body = e.resp_body_bytes or e.resp_body.encode("utf-8", errors="replace")
    r = make_response(body); r.headers["Content-Type"] = e.content_type or "text/plain"; return r

def _url_slug(url):
    try:
        p = urllib.parse.urlparse(url)
        path = p.path.strip("/").replace("/", "_")[-40:]
        return path or p.netloc.replace(".", "_")[:30]
    except: return "entry"