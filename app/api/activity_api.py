"""
app/api/activity_api.py
========================
Fixed to match the actual UserActivityLog schema discovered via /debug:

  id, ip_address, user_id, event_type, event_name, session_id,
  resource, occurred_at, meta_data, user_agent, properties,
  utm_source, utm_medium, utm_campaign, utm_content, utm_term, created_at

Key fixes vs previous version
──────────────────────────────
  url        → resource         (column name was wrong)
  event_type is NOT NULL        (was never being set — caused write failure)
  properties is json type       (pass dict, not JSON string)
  meta_data  is json type       (extra metadata slot — used for page/sport)
  UTM fields extracted from     (properties or request headers if present)
  Summary queries use ->>       (json type supports this, no ::jsonb cast needed)
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
    from app.models.tracking_model import UserActivityLog
    return UserActivityLog, UserActivityLog.__tablename__


# ══════════════════════════════════════════════════════════════════════════════
# DEBUG
# ══════════════════════════════════════════════════════════════════════════════

@bp_activity.route("/debug", methods=["GET"])
def activity_debug():
    info: dict = {}
    try:
        Model, table_name = _get_model()
        info["table_name"]    = table_name
        info["model_columns"] = [c.name for c in Model.__table__.columns]
    except Exception as e:
        return jsonify({"model_error": str(e)}), 500

    try:
        from app.extensions import db
        info["row_count"] = db.session.execute(
            db.text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()
    except Exception as e:
        info["count_error"] = str(e)

    try:
        from app.extensions import db
        cols = db.session.execute(db.text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = :t ORDER BY ordinal_position
        """), {"t": table_name}).fetchall()
        info["column_types"] = {r[0]: {"type": r[1], "nullable": r[2]} for r in cols}
    except Exception as e:
        info["column_types_error"] = str(e)

    try:
        from app.extensions import db
        rows = db.session.execute(db.text(f"""
            SELECT event_name, event_type, session_id, occurred_at
            FROM {table_name} ORDER BY occurred_at DESC LIMIT 3
        """)).fetchall()
        info["last_rows"] = [
            {"event": r[0], "type": r[1], "session": r[2], "at": str(r[3])}
            for r in rows
        ]
    except Exception as e:
        info["rows_error"] = str(e)

    # Write test using correct column names
    try:
        from app.extensions import db
        test = Model(
            user_id    = None,
            session_id = "debug_test",
            event_name = "debug_ping",
            event_type = "debug",       # NOT NULL — must provide
            resource   = "/debug",      # was called "url" in old code
            properties = {"test": True},  # json column — pass dict
            meta_data  = {},
            ip_address = "debug",
            user_agent = "debug",
            occurred_at = datetime.now(timezone.utc),
        )
        db.session.add(test)
        db.session.commit()
        info["write_test"] = "OK"
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

# Map frontend event names to event_type categories
_EVENT_TYPE_MAP: dict[str, str] = {
    "page_view":      "navigation",
    "tab_switch":     "navigation",
    "sport_switch":   "navigation",
    "match_click":    "engagement",
    "market_view":    "engagement",
    "arb_view":       "engagement",
    "result_view":    "engagement",
    "live_view":      "engagement",
    "match_watch_add": "engagement",
    "search":         "search",
    "filter_change":  "filter",
    "load_more":      "pagination",
    "signup_click":   "conversion",
    "upgrade_view":   "conversion",
}


def _event_type(event_name: str) -> str:
    return _EVENT_TYPE_MAP.get(event_name, "custom")


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

    # UTM params — same for all events in a batch (from query string or body)
    utm = {
        "utm_source":   request.args.get("utm_source")   or body.get("utm_source")   or "",
        "utm_medium":   request.args.get("utm_medium")   or body.get("utm_medium")   or "",
        "utm_campaign": request.args.get("utm_campaign") or body.get("utm_campaign") or "",
        "utm_content":  request.args.get("utm_content")  or body.get("utm_content")  or "",
        "utm_term":     request.args.get("utm_term")     or body.get("utm_term")     or "",
    }

    written = 0
    error   = None

    try:
        Model, _ = _get_model()
        from app.extensions import db

        rows = []
        for ev in events[:50]:
            if not isinstance(ev, dict):
                continue

            event_name = str(ev.get("event") or "unknown")[:64]
            props      = dict(ev.get("properties") or {})
            resource   = str(ev.get("url") or props.get("page") or "")[:512]
            session_id = str(ev.get("session_id") or "")[:128]

            # Scrub PII
            for key in ("user_id", "email", "phone", "name", "ip"):
                props.pop(key, None)

            # meta_data holds page/sport context separately from event properties
            meta = {
                "page":  props.pop("page",  None),
                "sport": props.pop("sport", None),
            }
            meta = {k: v for k, v in meta.items() if v is not None}

            try:
                ts = datetime.fromtimestamp(int(ev.get("ts", 0)) / 1000, tz=timezone.utc)
            except Exception:
                ts = datetime.now(timezone.utc)

            rows.append(Model(
                user_id     = None,
                session_id  = session_id,
                event_name  = event_name,
                event_type  = _event_type(event_name),   # NOT NULL — derived
                resource    = resource,                   # was "url" — FIXED
                properties  = props,                      # json column — pass dict
                meta_data   = meta,                       # json column — pass dict
                ip_address  = ip_hash,
                user_agent  = ua,
                occurred_at = ts,
                **{k: v[:128] for k, v in utm.items() if v},  # UTM fields
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

    try:
        _, table_name = _get_model()
    except Exception as e:
        return jsonify({"error": f"Model import failed: {e}"}), 500

    cache_key = f"kinetic:activity:summary:{hours}h"

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

        # properties is `json` type — ->> operator works directly (no ::jsonb cast needed)

        totals = db.session.execute(db.text(f"""
            SELECT COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name} WHERE occurred_at >= :since
        """), {"since": since}).fetchone()

        active_now = db.session.execute(db.text(f"""
            SELECT COUNT(DISTINCT session_id)
            FROM {table_name}
            WHERE occurred_at >= NOW() - INTERVAL '15 minutes'
        """)).scalar() or 0

        top_events = db.session.execute(db.text(f"""
            SELECT event_name, COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name} WHERE occurred_at >= :since
            GROUP BY event_name ORDER BY 2 DESC LIMIT 30
        """), {"since": since}).fetchall()

        top_event_types = db.session.execute(db.text(f"""
            SELECT event_type, COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name} WHERE occurred_at >= :since
            GROUP BY event_type ORDER BY 2 DESC
        """), {"since": since}).fetchall()

        # meta_data->>sport works because meta_data is json type
        top_sports = db.session.execute(db.text(f"""
            SELECT meta_data->>'sport', COUNT(*)
            FROM {table_name}
            WHERE occurred_at >= :since
              AND meta_data->>'sport' IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """), {"since": since}).fetchall()

        # properties->>'home_team' works on json type
        top_matches = db.session.execute(db.text(f"""
            SELECT
                properties->>'home_team',
                properties->>'away_team',
                meta_data->>'sport',
                COUNT(*)
            FROM {table_name}
            WHERE event_name = 'match_click'
              AND occurred_at >= :since
              AND properties->>'home_team' IS NOT NULL
            GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 10
        """), {"since": since}).fetchall()

        # Top pages visited
        top_pages = db.session.execute(db.text(f"""
            SELECT resource, COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name}
            WHERE event_name = 'page_view'
              AND occurred_at >= :since
              AND resource IS NOT NULL AND resource != ''
            GROUP BY resource ORDER BY 2 DESC LIMIT 10
        """), {"since": since}).fetchall()

        sparkline = db.session.execute(db.text(f"""
            SELECT date_trunc('hour', occurred_at), COUNT(*)
            FROM {table_name}
            WHERE occurred_at >= NOW() - INTERVAL '24 hours'
            GROUP BY 1 ORDER BY 1 ASC
        """)).fetchall()

        result = {
            "period":       f"{hours}h",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "cached":       False,
            "table":        table_name,
            "summary": {
                "total_events":   int(totals[0]) if totals else 0,
                "total_sessions": int(totals[1]) if totals else 0,
                "active_now":     int(active_now),
            },
            "top_events": [
                {"event": r[0], "count": int(r[1]), "unique_sessions": int(r[2])}
                for r in top_events
            ],
            "top_event_types": [
                {"type": r[0], "count": int(r[1]), "unique_sessions": int(r[2])}
                for r in top_event_types
            ],
            "top_sports": [
                {"sport": r[0], "count": int(r[1])}
                for r in top_sports if r[0]
            ],
            "top_matches": [
                {
                    "match":  f"{r[0]} v {r[1]}",
                    "sport":  r[2] or "",
                    "clicks": int(r[3]),
                }
                for r in top_matches if r[0] and r[1]
            ],
            "top_pages": [
                {"page": r[0], "views": int(r[1]), "unique_sessions": int(r[2])}
                for r in top_pages if r[0]
            ],
            "hourly_sparkline": [
                {
                    "hour":  r[0].strftime("%Y-%m-%dT%H:00Z") if r[0] else "",
                    "count": int(r[1]),
                }
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
        return jsonify({"error": str(e)}), 500