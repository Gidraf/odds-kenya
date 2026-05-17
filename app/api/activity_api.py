"""
app/api/activity_api.py
========================
Anonymous activity tracking.

Fixes vs previous version
──────────────────────────
• /debug endpoint tells you exactly what's wrong (table name, columns, write test)
• /log surfaces errors in dev (FLASK_ENV != production) instead of silent 200
• properties stored as JSON string — works with both text and jsonb columns
• Summary queries detect column type at runtime and use the right accessor
• Table name read from __tablename__ so it never drifts from the model
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, request

log = logging.getLogger("kinetic.activity")

bp_activity = Blueprint("activity", __name__, url_prefix="/api/activity")

IS_DEV = os.environ.get("FLASK_ENV", "production") != "production"


def _hash_ip(ip: str) -> str:
    if not ip:
        return ""
    return hashlib.sha256(ip.encode()).hexdigest()[:16]


def _redis():
    try:
        from app.workers.match_window_service import _redis as _r
        return _r()
    except Exception:
        return None


def _get_model():
    """Return (Model class, table_name). Raises on import error."""
    from app.models.tracking_model import UserActivityLog
    return UserActivityLog, UserActivityLog.__tablename__


# ══════════════════════════════════════════════════════════════════════════════
# DEBUG  — hit this first to find the problem
# ══════════════════════════════════════════════════════════════════════════════

@bp_activity.route("/debug", methods=["GET"])
def activity_debug():
    """
    GET /api/activity/debug
    Shows: table name, columns, column types, row count, last 3 rows, write test.
    Remove or auth-gate in production.
    """
    info: dict = {}

    # Model
    try:
        Model, table_name = _get_model()
        info["table_name"]    = table_name
        info["model_columns"] = [c.name for c in Model.__table__.columns]
    except Exception as e:
        return jsonify({"model_error": str(e)}), 500

    # Row count
    try:
        from app.extensions import db
        info["row_count"] = db.session.execute(
            db.text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()
    except Exception as e:
        info["count_error"] = str(e)

    # Column types from information_schema
    try:
        from app.extensions import db
        cols = db.session.execute(db.text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = :t
            ORDER BY ordinal_position
        """), {"t": table_name}).fetchall()
        info["column_types"] = {r[0]: {"type": r[1], "nullable": r[2]} for r in cols}
    except Exception as e:
        info["column_types_error"] = str(e)

    # Last 3 rows
    try:
        from app.extensions import db
        rows = db.session.execute(db.text(f"""
            SELECT event_name, session_id, occurred_at, properties
            FROM {table_name}
            ORDER BY occurred_at DESC LIMIT 3
        """)).fetchall()
        info["last_rows"] = [
            {"event": r[0], "session": r[1],
             "at": str(r[2]), "props": r[3]}
            for r in rows
        ]
    except Exception as e:
        info["rows_error"] = str(e)

    # Write test
    try:
        from app.extensions import db
        test = Model(
            user_id     = None,
            session_id  = "debug_test",
            event_name  = "debug_ping",
            url         = "/debug",
            properties  = json.dumps({"test": True}),
            ip_address  = "debug",
            user_agent  = "debug",
            occurred_at = datetime.now(timezone.utc),
        )
        db.session.add(test)
        db.session.commit()
        info["write_test"] = "OK — row written successfully"
    except Exception as e:
        info["write_test"] = f"FAILED: {e}"
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass

    return jsonify(info)


# ══════════════════════════════════════════════════════════════════════════════
# LOG
# ══════════════════════════════════════════════════════════════════════════════

@bp_activity.route("/log", methods=["POST"])
def log_activity():
    body   = request.get_json(silent=True) or {}
    events = body.get("events", [])

    if not events or not isinstance(events, list):
        return jsonify({"ok": True, "written": 0, "reason": "empty"})

    raw_ip  = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.remote_addr or ""
    )
    ip_hash = _hash_ip(raw_ip)
    ua      = request.headers.get("User-Agent", "")[:256]

    written = 0
    error   = None

    try:
        Model, _ = _get_model()
        from app.extensions import db

        rows = []
        for ev in events[:50]:
            if not isinstance(ev, dict):
                continue

            props = dict(ev.get("properties") or {})
            for pii in ("user_id", "email", "phone", "name", "ip"):
                props.pop(pii, None)

            try:
                ts = datetime.fromtimestamp(int(ev.get("ts", 0)) / 1000, tz=timezone.utc)
            except Exception:
                ts = datetime.now(timezone.utc)

            rows.append(Model(
                user_id     = None,
                session_id  = str(ev.get("session_id") or "")[:128],
                event_name  = str(ev.get("event") or "unknown")[:64],
                url         = str(ev.get("url") or "")[:512],
                properties  = json.dumps(props, default=str)[:4096],
                ip_address  = ip_hash,
                user_agent  = ua,
                occurred_at = ts,
            ))

        db.session.bulk_save_objects(rows)
        db.session.commit()
        written = len(rows)

    except Exception as e:
        error = str(e)
        log.error("Activity log write failed: %s", e)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass

    if error and IS_DEV:
        return jsonify({"ok": False, "written": 0, "error": error}), 500

    return jsonify({"ok": True, "written": written})


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════

@bp_activity.route("/summary", methods=["GET"])
def activity_summary():
    hours     = min(max(1, int(request.args.get("hours", 24))), 168)
    force     = request.args.get("force", "false").lower() == "true"
    cache_key = f"kinetic:activity:summary:{hours}h"

    try:
        _, table_name = _get_model()
    except Exception as e:
        return jsonify({"error": f"Model import failed: {e}"}), 500

    # Cache
    r = _redis()
    if r and not force:
        try:
            cached = r.get(cache_key)
            if cached:
                data = json.loads(cached)
                data["cached"] = True
                return jsonify(data)
        except Exception:
            pass

    try:
        from app.extensions import db
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Detect column type so queries work on both text and jsonb
        col_type = db.session.execute(db.text("""
            SELECT data_type FROM information_schema.columns
            WHERE table_name = :t AND column_name = 'properties' LIMIT 1
        """), {"t": table_name}).scalar() or "text"

        is_jsonb = "json" in (col_type or "").lower()

        def prop(key: str) -> str:
            return f"properties->>'{key}'" if is_jsonb \
                else f"(properties::jsonb)->>'{key}'"

        totals = db.session.execute(db.text(f"""
            SELECT COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name} WHERE occurred_at >= :since
        """), {"since": since}).fetchone()

        active_now = db.session.execute(db.text(f"""
            SELECT COUNT(DISTINCT session_id) FROM {table_name}
            WHERE occurred_at >= NOW() - INTERVAL '15 minutes'
        """)).scalar() or 0

        top_events = db.session.execute(db.text(f"""
            SELECT event_name, COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name} WHERE occurred_at >= :since
            GROUP BY event_name ORDER BY 2 DESC LIMIT 30
        """), {"since": since}).fetchall()

        top_sports = db.session.execute(db.text(f"""
            SELECT {prop('to')}, COUNT(*) FROM {table_name}
            WHERE event_name = 'sport_switch' AND occurred_at >= :since
              AND {prop('to')} IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """), {"since": since}).fetchall()

        top_matches = db.session.execute(db.text(f"""
            SELECT {prop('home_team')}, {prop('away_team')},
                   {prop('sport')}, COUNT(*)
            FROM {table_name}
            WHERE event_name = 'match_click' AND occurred_at >= :since
              AND {prop('home_team')} IS NOT NULL
            GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 10
        """), {"since": since}).fetchall()

        sparkline = db.session.execute(db.text(f"""
            SELECT date_trunc('hour', occurred_at), COUNT(*)
            FROM {table_name}
            WHERE occurred_at >= NOW() - INTERVAL '24 hours'
            GROUP BY 1 ORDER BY 1 ASC
        """)).fetchall()

        result = {
            "period":         f"{hours}h",
            "generated_at":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "cached":         False,
            "table":          table_name,
            "properties_type": col_type,
            "summary": {
                "total_events":   int(totals[0]) if totals else 0,
                "total_sessions": int(totals[1]) if totals else 0,
                "active_now":     int(active_now),
            },
            "top_events": [
                {"event": r[0], "count": int(r[1]), "unique_sessions": int(r[2])}
                for r in top_events
            ],
            "top_sports": [
                {"sport": r[0], "count": int(r[1])}
                for r in top_sports if r[0]
            ],
            "top_matches": [
                {"match": f"{r[0]} v {r[1]}", "sport": r[2] or "", "clicks": int(r[3])}
                for r in top_matches if r[0] and r[1]
            ],
            "hourly_sparkline": [
                {"hour": r[0].strftime("%Y-%m-%dT%H:00Z") if r[0] else "",
                 "count": int(r[1])}
                for r in sparkline
            ],
        }

        if r:
            try:
                r.setex(cache_key, 60, json.dumps(result))
            except Exception:
                pass

        return jsonify(result)

    except Exception as e:
        log.error("activity_summary error: %s", e)
        return jsonify({"error": str(e), "table": table_name}), 500