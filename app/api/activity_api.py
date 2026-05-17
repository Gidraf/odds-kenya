"""
Improved activity_summary — fast, cached, richer data.

Changes vs original
───────────────────
• Redis cache (60s TTL) — raw SQL only runs once per minute
• Configurable window via ?hours=24 (default) up to 168 (7 days)
• Per-sport breakdown for sport_switch events
• Top matches clicked (join_key + team names)
• Active sessions count (distinct session_ids in last 15 min)
• Hourly sparkline (last 24 buckets) for trend visibility
• Auth commented-out in original left commented — easy to re-enable
"""
import json
from datetime import datetime, timedelta, timezone

from flask import jsonify, request


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