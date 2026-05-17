"""
app/api/activity_api.py
========================
Anonymous activity tracking — no user data stored or returned.

Writes session_id (random, client-generated), event name, properties,
url, ip (hashed), and timestamp only. No user_id, no auth required.

Register in create_app():
    from app.api.activity_api import bp_activity
    flask_app.register_blueprint(bp_activity)
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, request

log = logging.getLogger("kinetic.activity")

bp_activity = Blueprint("activity", __name__, url_prefix="/api/activity")


def _hash_ip(ip: str) -> str:
    """One-way hash the IP so we can see patterns but not the actual address."""
    if not ip:
        return ""
    return hashlib.sha256(ip.encode()).hexdigest()[:16]


def _redis():
    try:
        from app.workers.match_window_service import _redis as _r
        return _r()
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# LOG  — receive batched events from frontend
# ══════════════════════════════════════════════════════════════════════════════

@bp_activity.route("/log", methods=["POST"])
def log_activity():
    """
    Receive a batch of anonymous activity events.

    Body:
        {
          "events": [
            {
              "event":      "match_click",
              "properties": {"join_key": "...", "sport": "soccer"},
              "ts":         1716000000000,
              "url":        "/odds",
              "session_id": "anon_abc123"
            }
          ]
        }

    No auth required. No user_id stored.
    IP is SHA-256 hashed before storage.
    """
    body   = request.get_json(silent=True) or {}
    events = body.get("events", [])

    if not events or not isinstance(events, list):
        return jsonify({"ok": True, "written": 0})

    # Hash IP once per request
    raw_ip  = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.remote_addr
        or ""
    )
    ip_hash = _hash_ip(raw_ip)
    ua      = request.headers.get("User-Agent", "")[:256]

    written = 0
    try:
        from app.models.tracking_model import UserActivityLog
        from app.extensions import db

        rows = []
        for ev in events[:50]:       # hard cap — prevent abuse
            if not isinstance(ev, dict):
                continue

            event_name = str(ev.get("event") or "unknown")[:64]
            props      = dict(ev.get("properties") or {})
            session_id = str(ev.get("session_id") or "")[:128]

            # Scrub any accidental PII from properties before storing
            for key in ("user_id", "email", "phone", "name", "ip"):
                props.pop(key, None)

            try:
                occurred_at = datetime.fromtimestamp(
                    int(ev.get("ts", 0)) / 1000, tz=timezone.utc
                )
            except Exception:
                occurred_at = datetime.now(timezone.utc)

            rows.append(UserActivityLog(
                user_id     = None,       # always null — fully anonymous
                session_id  = session_id,
                event_name  = event_name,
                url         = str(ev.get("url") or "")[:512],
                properties  = json.dumps(props, default=str)[:4096],
                ip_address  = ip_hash,    # hashed, never raw
                user_agent  = ua,
                occurred_at = occurred_at,
            ))

        db.session.bulk_save_objects(rows)
        db.session.commit()
        written = len(rows)

    except Exception as e:
        log.debug("Activity log error: %s", e)

    # Always 200 — tracking must never break the app
    return jsonify({"ok": True, "written": written})


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY  — aggregate stats only, no individual records exposed
# ══════════════════════════════════════════════════════════════════════════════

@bp_activity.route("/summary", methods=["GET"])
def activity_summary():
    """
    Aggregate stats — counts, top sports, top matches, hourly sparkline.
    No session IDs, no IPs, no user identifiers returned.

    Query params:
      hours=24    window size (default 24, max 168)
      force=true  bypass the 60s Redis cache
    """
    hours     = min(max(1, int(request.args.get("hours", 24))), 168)
    force     = request.args.get("force", "false").lower() == "true"
    cache_key = f"kinetic:activity:summary:{hours}h"

    # ── Redis cache ────────────────────────────────────────────────────────────
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

    # ── DB queries (all aggregate — no individual rows returned) ───────────────
    try:
        from app.extensions import db
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

        # 1. Totals
        totals = db.session.execute(db.text("""
            SELECT
                COUNT(*)                   AS total_events,
                COUNT(DISTINCT session_id) AS total_sessions
            FROM user_activity_logs
            WHERE occurred_at >= :since
        """), {"since": since}).fetchone()

        # 2. Active right now (last 15 min)
        active_now = db.session.execute(db.text("""
            SELECT COUNT(DISTINCT session_id)
            FROM user_activity_logs
            WHERE occurred_at >= NOW() - INTERVAL '15 minutes'
        """)).scalar() or 0

        # 3. Top events by count
        top_events = db.session.execute(db.text("""
            SELECT
                event_name,
                COUNT(*)                   AS cnt,
                COUNT(DISTINCT session_id) AS unique_sessions
            FROM user_activity_logs
            WHERE occurred_at >= :since
            GROUP BY event_name
            ORDER BY cnt DESC
            LIMIT 30
        """), {"since": since}).fetchall()

        # 4. Top sports people switched to
        top_sports = db.session.execute(db.text("""
            SELECT
                properties->>'to' AS sport,
                COUNT(*)          AS cnt
            FROM user_activity_logs
            WHERE event_name = 'sport_switch'
              AND occurred_at >= :since
              AND properties->>'to' IS NOT NULL
            GROUP BY sport
            ORDER BY cnt DESC
            LIMIT 10
        """), {"since": since}).fetchall()

        # 5. Top matches clicked (team names only — no join_key)
        top_matches = db.session.execute(db.text("""
            SELECT
                properties->>'home_team' AS home_team,
                properties->>'away_team' AS away_team,
                properties->>'sport'     AS sport,
                COUNT(*)                 AS clicks
            FROM user_activity_logs
            WHERE event_name = 'match_click'
              AND occurred_at >= :since
              AND properties->>'home_team' IS NOT NULL
            GROUP BY home_team, away_team, sport
            ORDER BY clicks DESC
            LIMIT 10
        """), {"since": since}).fetchall()

        # 6. Hourly sparkline — always last 24h regardless of window
        sparkline = db.session.execute(db.text("""
            SELECT
                date_trunc('hour', occurred_at) AS hour,
                COUNT(*)                        AS cnt
            FROM user_activity_logs
            WHERE occurred_at >= NOW() - INTERVAL '24 hours'
            GROUP BY hour
            ORDER BY hour ASC
        """)).fetchall()

        result = {
            "period":       f"{hours}h",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "cached":       False,
            "summary": {
                "total_events":   int(totals[0]) if totals else 0,
                "total_sessions": int(totals[1]) if totals else 0,
                "active_now":     int(active_now),
            },
            "top_events": [
                {
                    "event":           r[0],
                    "count":           int(r[1]),
                    "unique_sessions": int(r[2]),
                }
                for r in top_events
            ],
            "top_sports": [
                {"sport": r[0], "count": int(r[1])}
                for r in top_sports
                if r[0]
            ],
            "top_matches": [
                {
                    "match":  f"{r[0]} v {r[1]}",
                    "sport":  r[2] or "",
                    "clicks": int(r[3]),
                }
                for r in top_matches
                if r[0] and r[1]
            ],
            "hourly_sparkline": [
                {
                    "hour":  r[0].strftime("%Y-%m-%dT%H:00Z") if r[0] else "",
                    "count": int(r[1]),
                }
                for r in sparkline
            ],
        }

        # Cache 60s
        if r:
            try:
                r.setex(cache_key, 60, json.dumps(result))
            except Exception:
                pass

        return jsonify(result)

    except Exception as e:
        log.error("activity_summary error: %s", e)
        return jsonify({"error": str(e)}), 500