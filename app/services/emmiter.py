"""
app/services/emitter.py
========================
A standalone SocketIO client used by Celery workers to publish events
through the Redis message queue to the Flask server, which forwards
them to connected browsers.

The regular `socketio` instance from app.extensions only has message_queue
configured in the Flask server process. Workers need their own emitter
that connects directly to Redis.
"""
import os
from datetime import datetime, timezone
from flask_socketio import SocketIO

# Lazy singleton — created once per worker process
_emitter: SocketIO | None = None


def get_emitter() -> SocketIO:
    global _emitter
    if _emitter is None:
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        _emitter = SocketIO(
            message_queue=redis_url,
            channel="flask-socketio",   # must match server channel
        )
    return _emitter


def emit_log(bookmaker_id: int, level: str, msg: str, **extra):
    """
    Emit an agent_status event to all clients in the /admin namespace.
    Safe to call from any Celery task or background thread.
    """
    payload = {
        "bookmaker_id": bookmaker_id,
        "level": level,
        "msg": msg,
        "ts": datetime.now(timezone.utc).isoformat(),
        **extra,
    }
    icon = {
        "INFO": "🔵", "WARN": "🟡", "ERROR": "🔴",
        "SUCCESS": "✅", "AI": "🤖", "NET": "🌐", "CAPTURE": "📡",
    }.get(level, "⚪")
    print(f"[EMIT/{level}] {msg}")
    try:
        get_emitter().emit("agent_status", payload, namespace="/admin")
    except Exception as e:
        print(f"[EMIT/ERROR] Failed to emit via Redis: {e}")
        # Fallback: try direct socketio emit (works if called from Flask process)
        try:
            from app.extensions import socketio
            socketio.emit("agent_status", payload, namespace="/admin")
        except Exception:
            pass


def emit_harvest_log(bookmaker_id: int, level: str, msg: str):
    """Emit a harvest_log event (lighter, used by the harvest loop)."""
    payload = {
        "bookmaker_id": bookmaker_id,
        "level": level,
        "msg": msg,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    print(f"[HARVEST/{level}] {msg}")
    try:
        get_emitter().emit("harvest_log", payload, namespace="/admin")
    except Exception as e:
        print(f"[EMIT/ERROR] harvest_log failed: {e}")