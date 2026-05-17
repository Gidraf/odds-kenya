"""
app/api/activity_api.py
========================
Activity logging endpoint — receives batched client-side events.
Writes to UserActivityLog (model already exists in app/models/tracking_model.py).
 
Register in create_app():
    from app.api.activity_api import bp_activity
    flask_app.register_blueprint(bp_activity)
 
Events from useActivityTracker:
  page_view, sport_switch, match_click, market_view, arb_view,
  tab_switch, search, filter_change, load_more, signup_click, upgrade_view,
  match_watch_add, result_view, live_view
"""

from __future__ import annotations
from datetime import timedelta
import json
import logging
from datetime import datetime, timezone
 
from flask import Blueprint, jsonify, request
 
log = logging.getLogger("kinetic.activity")
 
bp_activity = Blueprint("activity", __name__, url_prefix="/api/activity")
 
 
def _auth_user_optional():
    """Returns user or None — never raises."""
    try:
        from app.utils.customer_jwt_helpers import _decode_token
        from app.models.customer import Customer
        auth  = request.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else request.args.get("token", "")
        if not token:
            return None
        payload = _decode_token(token)
        return Customer.query.get(int(payload["sub"]))
    except Exception:
        return None
 
 
@bp_activity.route("/log", methods=["POST"])
def log_activity():
    """
    Receive a batch of activity events from the frontend.
 
    Body:
        {
          "events": [
            {
              "event":      "match_click",
              "properties": {"join_key": "...", "has_arb": true},
              "ts":         1716000000000,
              "url":        "/odds",
              "session_id": "anon_xyz123",
              "user_id":    null
            }
          ]
        }
    """
    # Anonymous access — no auth required
    user = _auth_user_optional()
 
    body   = request.get_json(silent=True) or {}
    events = body.get("events", [])
 
    if not events or not isinstance(events, list):
        return jsonify({"ok": True, "written": 0})
 
    written = 0
    try:
        from app.models.tracking_model import UserActivityLog
        from app.extensions import db
 
        ip  = (
            request.headers.get("X-Forwarded-For", "").split(",")[0].strip() or
            request.remote_addr or ""
        )
        ua  = request.headers.get("User-Agent", "")[:512]
 
        rows = []
        for ev in events[:50]:   # cap at 50 per batch to prevent abuse
            if not isinstance(ev, dict):
                continue
            event_name = str(ev.get("event") or "unknown")[:64]
            props      = ev.get("properties") or {}
            session_id = str(ev.get("session_id") or "")[:128]
 
            # Client timestamp (ms) → datetime
            ts_ms = ev.get("ts")
            try:
                occurred_at = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
            except Exception:
                occurred_at = datetime.now(timezone.utc)
 
            rows.append(UserActivityLog(
                user_id    = user.id if user else None,
                session_id = session_id,
                event_name = event_name,
                url        = str(ev.get("url") or "")[:512],
                properties = json.dumps(props, default=str)[:4096],
                ip_address = ip,
                user_agent = ua,
                occurred_at = occurred_at,
            ))
 
        db.session.bulk_save_objects(rows)
        db.session.commit()
        written = len(rows)
 
    except Exception as e:
        log.debug("Activity log error: %s", e)
        # Never return an error to the client — tracking must be silent
        return jsonify({"ok": True, "written": 0})
 
    return jsonify({"ok": True, "written": written})
 
 
@bp_activity.route("/summary", methods=["GET"])
def activity_summary():
    hours    = min(int(request.args.get("hours", 24)), 168)
    force    = request.args.get("force", "false").lower() == "true"
    cache_key = f"kinetic:activity:summary:{hours}h"
 
    # ── Redis cache (skip DB if fresh result exists) ───────────────────────────
    try:
        from app.workers.match_window_service import _redis
        r   = _redis()
        raw = None if force else r.get(cache_key)
        if raw:
            return jsonify(json.loads(raw))
    except Exception:
        r = None
 
    # ── DB queries ─────────────────────────────────────────────────────────────
    try:
        from app.extensions import db
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
 
        # 1. Top events
        top_events = db.session.execute(db.text("""
            SELECT
                event_name,
                COUNT(*)                                                    AS cnt,
                COUNT(DISTINCT COALESCE(user_id::text, session_id))        AS unique_users,
                COUNT(DISTINCT CASE WHEN user_id IS NOT NULL
                               THEN user_id::text END)                     AS logged_in_users
            FROM user_activity_logs
            WHERE occurred_at >= :since
            GROUP BY event_name
            ORDER BY cnt DESC
            LIMIT 30
        """), {"since": since}).fetchall()
 
        # 2. Active sessions right now (last 15 min)
        active_sessions = db.session.execute(db.text("""
            SELECT COUNT(DISTINCT COALESCE(user_id::text, session_id))
            FROM user_activity_logs
            WHERE occurred_at >= NOW() - INTERVAL '15 minutes'
        """)).scalar() or 0
 
        # 3. Top sports switched to
        top_sports = db.session.execute(db.text("""
            SELECT
                properties->>'to'  AS sport,
                COUNT(*)           AS cnt
            FROM user_activity_logs
            WHERE event_name = 'sport_switch'
              AND occurred_at >= :since
              AND properties->>'to' IS NOT NULL
            GROUP BY sport
            ORDER BY cnt DESC
            LIMIT 10
        """), {"since": since}).fetchall()
 
        # 4. Top matches clicked
        top_matches = db.session.execute(db.text("""
            SELECT
                properties->>'join_key'  AS join_key,
                properties->>'home_team' AS home_team,
                properties->>'away_team' AS away_team,
                COUNT(*)                 AS cnt
            FROM user_activity_logs
            WHERE event_name = 'match_click'
              AND occurred_at >= :since
              AND properties->>'join_key' IS NOT NULL
            GROUP BY join_key, home_team, away_team
            ORDER BY cnt DESC
            LIMIT 10
        """), {"since": since}).fetchall()
 
        # 5. Hourly sparkline — how many events per hour for the last 24h
        sparkline = db.session.execute(db.text("""
            SELECT
                date_trunc('hour', occurred_at) AS hour,
                COUNT(*)                        AS cnt
            FROM user_activity_logs
            WHERE occurred_at >= NOW() - INTERVAL '24 hours'
            GROUP BY hour
            ORDER BY hour ASC
        """)).fetchall()
 
        # 6. Total events + users summary
        totals = db.session.execute(db.text("""
            SELECT
                COUNT(*)                                                    AS total_events,
                COUNT(DISTINCT COALESCE(user_id::text, session_id))        AS total_sessions,
                COUNT(DISTINCT CASE WHEN user_id IS NOT NULL
                               THEN user_id::text END)                     AS total_logged_in
            FROM user_activity_logs
            WHERE occurred_at >= :since
        """), {"since": since}).fetchone()
 
        result = {
            "period":          f"{hours}h",
            "generated_at":    datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_events":    totals[0] if totals else 0,
                "total_sessions":  totals[1] if totals else 0,
                "logged_in_users": totals[2] if totals else 0,
                "active_now":      int(active_sessions),
            },
            "top_events": [
                {
                    "event":         r[0],
                    "count":         r[1],
                    "unique_users":  r[2],
                    "logged_in":     r[3],
                }
                for r in top_events
            ],
            "top_sports": [
                {"sport": r[0], "count": r[1]}
                for r in top_sports
            ],
            "top_matches": [
                {
                    "join_key":  r[0],
                    "home_team": r[1],
                    "away_team": r[2],
                    "clicks":    r[3],
                }
                for r in top_matches
            ],
            "hourly_sparkline": [
                {
                    "hour":  r[0].isoformat() if r[0] else "",
                    "count": r[1],
                }
                for r in sparkline
            ],
        }
 
        # ── Cache for 60 seconds ───────────────────────────────────────────────
        if r:
            try:
                r.setex(cache_key, 60, json.dumps(result, default=str))
            except Exception:
                pass
 
        return jsonify(result)
 
    except Exception as e:
        return jsonify({"error": str(e)}), 500      