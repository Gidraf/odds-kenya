"""
app/api/notifications.py
=========================
Premium user notification system.

Architecture:
  Redis pub/sub  →  this module  →  WebSocket (SSE push)
                                 →  Email (Celery task)

Only premium/admin users receive push notifications.
Basic/pro users poll via SSE.

WebSocket endpoint:  GET /notifications/stream?token=<jwt>
                     Server-Sent Events (same auth pattern as odds stream)
"""
from __future__ import annotations

import json
import time
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Literal

from flask import Blueprint, Response, request, stream_with_context

log = logging.getLogger(__name__)

bp_notify = Blueprint("notifications", __name__, url_prefix="/notifications")

_KEEPALIVE   = 25
_TIER_RANK   = {"free": 0, "basic": 1, "pro": 2, "premium": 3, "admin": 4}

# Redis pubsub channels the notification stream subscribes to
_NOTIFY_CHANNELS = [
    "notify:arb:new",        # new arb opportunity detected
    "notify:ev:new",         # new EV opportunity
    "notify:harvest:done",   # harvest completed (for admin dashboard)
    "notify:alert:*",        # custom alerts
]


# =============================================================================
# NOTIFICATION TYPES
# =============================================================================

@dataclass
class ArbNotification:
    type:         str = "arb_opportunity"
    match:        str = ""
    market:       str = ""
    profit_pct:   float = 0.0
    legs:         list = None
    sport:        str = ""
    competition:  str = ""
    detected_at:  str = ""
    expires_in_s: int = 300

    def __post_init__(self):
        if self.legs is None:
            self.legs = []
        if not self.detected_at:
            self.detected_at = datetime.now(timezone.utc).isoformat()


@dataclass
class HarvestNotification:
    type:       str = "harvest_done"
    bookmaker:  str = ""
    sport:      str = ""
    count:      int = 0
    latency_ms: int = 0
    ts:         str = ""


# =============================================================================
# REDIS PUBLISHER (called from tasks)
# =============================================================================

def publish_arb_opportunity(arb_data: dict, sport: str) -> None:
    """
    Called from compute_ev_arb / tasks_ops when arb is detected.
    Pushes to Redis so all connected premium SSE streams receive it.
    """
    try:
        from app.workers.celery_tasks import _redis
        r = _redis()
        payload = json.dumps({
            "type":        "arb_opportunity",
            "match":       f"{arb_data.get('home_team')} vs {arb_data.get('away_team')}",
            "market":      arb_data.get("market", ""),
            "profit_pct":  arb_data.get("profit_pct", 0),
            "legs":        arb_data.get("legs", []),
            "sport":       sport,
            "competition": arb_data.get("competition", ""),
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "expires_in_s": 300,
        }, default=str)
        r.publish("notify:arb:new", payload)
        log.debug("[notify] published arb: %s/%s +%.2f%%", sport, arb_data.get("market"), arb_data.get("profit_pct", 0))
    except Exception as exc:
        log.warning("[notify] publish_arb failed: %s", exc)


def publish_harvest_done(bk: str, sport: str, count: int, latency_ms: int) -> None:
    """Called from harvest tasks — notifies admin dashboard."""
    try:
        from app.workers.celery_tasks import _redis
        r = _redis()
        r.publish("notify:harvest:done", json.dumps({
            "type":       "harvest_done",
            "bookmaker":  bk,
            "sport":      sport,
            "count":      count,
            "latency_ms": latency_ms,
            "ts":         datetime.now(timezone.utc).isoformat(),
        }))
    except Exception:
        pass


# =============================================================================
# EMAIL NOTIFICATION (Celery task)
# =============================================================================

def register_notify_tasks(celery):
    """Register Celery notification tasks. Called from app factory."""

    @celery.task(name="tasks.notify.arb_digest", soft_time_limit=6000, time_limit=90)
    def arb_digest_task():
        """
        Send arb digest email to premium users who have email alerts enabled.
        Runs every 5 minutes via beat schedule.
        """
        from app.extensions import db
        from app.models.customer import Customer
        from app.workers.celery_tasks import cache_get

        # Get premium users with email alerts ON
        users = Customer.query.filter(
            Customer.tier.in_(["premium", "admin"]),
            Customer.email_alerts_enabled == True,
            Customer.is_active == True,
        ).all()

        if not users:
            return {"sent": 0, "reason": "no eligible users"}

        # Collect arb opportunities from Redis
        from app.workers.celery_tasks import _redis
        r       = _redis()
        sports  = ["soccer", "basketball", "tennis", "cricket"]
        all_arb = []

        for sport in sports:
            raw = r.get(f"odds:unified:upcoming:{sport}")
            if not raw:
                continue
            try:
                data    = json.loads(raw)
                matches = data.get("matches", []) if isinstance(data, dict) else data
                for m in matches:
                    if m.get("has_arb") and m.get("best_arb_pct", 0) >= 1.0:
                        all_arb.append({
                            "match":      f"{m.get('home_team')} vs {m.get('away_team')}",
                            "sport":      sport,
                            "competition": m.get("competition", ""),
                            "profit_pct": m.get("best_arb_pct", 0),
                            "arbs":       m.get("arb_opportunities", []),
                        })
            except Exception:
                pass

        if not all_arb:
            return {"sent": 0, "reason": "no arb opportunities above 1%"}

        # Sort by profit
        all_arb.sort(key=lambda a: -a["profit_pct"])
        top_arb = all_arb[:10]

        sent = 0
        for user in users:
            try:
                send_arb_email.apply_async(
                    args=[user.email, user.first_name or "Trader", top_arb],
                    queue="notify",
                    countdown=0,
                )
                sent += 1
            except Exception as exc:
                log.warning("[notify] email dispatch failed for %s: %s", user.email, exc)

        return {"sent": sent, "arb_count": len(top_arb)}

    @celery.task(name="tasks.notify.send_arb_email", soft_time_limit=6000, time_limit=60)
    def send_arb_email(to_email: str, name: str, arb_list: list):
        """Send arb opportunity email."""
        try:
            from flask_mail import Message
            from app.extensions import mail

            rows = "\n".join([
                f"  • {a['match']} ({a['sport'].title()}) — +{a['profit_pct']:.2f}% guaranteed"
                for a in arb_list[:5]
            ])

            msg = Message(
                subject=f"⚡ {len(arb_list)} Arb Opportunities — Kinetic Odds Engine",
                recipients=[to_email],
                body=f"""Hi {name},

We found {len(arb_list)} arbitrage opportunities right now:

{rows}

Log in to Kinetic to see full placement instructions:
https://kinetic.gidraf.dev

These opportunities expire in ~5 minutes. Act fast.

─────────────────────────────────────────
⚠ Gamble responsibly. 18+ only.
Kinetic Odds Engine — Mathematical edges only.
""",
            )
            mail.send(msg)
            log.info("[notify] arb email sent to %s", to_email)
        except Exception as exc:
            log.error("[notify] email send failed to %s: %s", to_email, exc)
            raise

    @celery.task(name="tasks.notify.arb_digest")
    def arb_digest():
        return arb_digest_task()

    return arb_digest_task, send_arb_email


# =============================================================================
# SSE STREAM — premium users only
# =============================================================================

def _auth_user():
    from app.utils.customer_jwt_helpers import _decode_token
    from app.models.customer import Customer
    auth  = request.headers.get("Authorization", "")
    token = auth[7:] if auth.startswith("Bearer ") else None
    if not token:
        token = request.args.get("token", "").strip() or None
    if not token:
        return None
    try:
        payload = _decode_token(token)
        return Customer.query.get(int(payload["sub"]))
    except Exception:
        return None


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


@bp_notify.route("/stream", methods=["GET"])
def notification_stream():
    """
    Premium SSE stream — real-time arb alerts, harvest updates, system events.
    Only accessible to premium and admin tier users.
    Pass JWT via ?token= (EventSource can't set headers).
    """
    user = _auth_user()
    if not user:
        def _deny():
            yield _sse("error", {"error": "Unauthorized", "code": 401})
        return Response(stream_with_context(_deny()), mimetype="text/event-stream",
                        status=200, headers={"Cache-Control": "no-cache"})

    tier_rank = _TIER_RANK.get(getattr(user, "tier", "basic") or "basic", 1)
    is_admin  = tier_rank >= _TIER_RANK["admin"]
    is_premium = tier_rank >= _TIER_RANK["premium"]

    if not is_premium:
        def _deny():
            yield _sse("error", {"error": "Premium tier required for push notifications", "code": 403})
        return Response(stream_with_context(_deny()), mimetype="text/event-stream",
                        status=200, headers={"Cache-Control": "no-cache"})

    def generate():
        from app.workers.celery_tasks import _redis
        r = _r()

        yield _sse("connected", {
            "status":   "connected",
            "tier":     getattr(user, "tier", "premium"),
            "user_id":  user.id,
            "channels": ["arb", "ev"] + (["harvest", "system"] if is_admin else []),
        })

        pubsub = r.pubsub(ignore_subscribe_messages=True)
        channels = ["notify:arb:new", "notify:ev:new"]
        if is_admin:
            channels += ["notify:harvest:done", "notify:alert:*"]
        pubsub.subscribe(*channels)

        last_ka = time.time()
        try:
            while True:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg.get("type") == "message":
                    try:
                        data = json.loads(msg["data"])
                        ch   = (msg.get("channel") or b"")
                        if isinstance(ch, bytes):
                            ch = ch.decode()

                        if "arb" in ch:
                            yield _sse("arb_alert", data)
                        elif "ev" in ch:
                            yield _sse("ev_alert", data)
                        elif "harvest" in ch:
                            yield _sse("harvest_update", data)
                        else:
                            yield _sse("system", data)
                    except Exception:
                        pass

                if time.time() - last_ka > _KEEPALIVE:
                    yield ": keepalive\n\n"
                    last_ka = time.time()
        finally:
            try:
                pubsub.unsubscribe()
                pubsub.close()
            except Exception:
                pass

    def _r():
        from app.workers.celery_tasks import _redis
        return _redis()

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":                "no-cache",
            "X-Accel-Buffering":            "no",
            "Connection":                   "keep-alive",
            "Access-Control-Allow-Origin":  "*",
        },
    )


@bp_notify.route("/preferences", methods=["GET", "PATCH"])
def notification_preferences():
    """
    GET  — return user's notification prefs
    PATCH — update prefs (email_alerts_enabled, alert_min_arb_pct, etc.)
    """
    from app.api import _err, _signed_response
    from app.extensions import db

    user = _auth_user()
    if not user:
        return _err("Authentication required", 401)

    if request.method == "GET":
        return _signed_response({
            "email_alerts_enabled": getattr(user, "email_alerts_enabled", False),
            "push_alerts_enabled":  getattr(user, "push_alerts_enabled",  False),
            "alert_min_arb_pct":    getattr(user, "alert_min_arb_pct",    1.0),
            "alert_sports":         getattr(user, "alert_sports",          []),
        })

    data = request.get_json(silent=True) or {}
    for field in ("email_alerts_enabled", "push_alerts_enabled", "alert_min_arb_pct", "alert_sports"):
        if field in data:
            setattr(user, field, data[field])
    db.session.commit()
    return _signed_response({"ok": True})