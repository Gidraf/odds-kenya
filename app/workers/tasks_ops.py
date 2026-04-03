"""
app/workers/tasks_ops.py
========================
Operational tasks: EV/arb, results, notifications, health, subscriptions.
Also owns setup_periodic_tasks — the single beat schedule registration.

Celery beat entry point:
  celery -A app.workers.tasks_ops worker --beat ...
"""

from __future__ import annotations

import base64
import os
from datetime import datetime, timezone, timedelta

import requests
from celery.utils.log import get_task_logger

from app.workers.celery_tasks import (
    celery, cache_set, cache_get, _now_iso, _publish,
)

logger = get_task_logger(__name__)

WS_CHANNEL  = "odds:updates"
ARB_CHANNEL = "arb:updates"
EV_CHANNEL  = "ev:updates"

whatsapp_bot = os.environ.get("WA_BOT", "")
message_url  = f"{whatsapp_bot}/api/v1/send-message" if whatsapp_bot else ""

# Sports whose combined snapshots are flushed to PostgreSQL every 5 minutes
PERSIST_SPORTS: list[str] = [
    "soccer",
    "basketball",
    "tennis",
    "ice-hockey",
    "rugby",
    "handball",
    "volleyball",
    "cricket",
    "table-tennis",
    "esoccer",
    "mma",
    "boxing",
    "darts",
]


# =============================================================================
# Beat schedule — ALL periodic tasks registered here
# =============================================================================

@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    from app.workers.tasks_upcoming import (
        harvest_all_registry_upcoming, cleanup_old_snapshots,
        sp_harvest_all_upcoming, bt_harvest_all_upcoming,
        od_harvest_all_upcoming, b2b_harvest_all_upcoming,
        b2b_page_harvest_all_upcoming, sbo_harvest_all_upcoming,
    )
    from app.workers.tasks_live import (
        sp_harvest_all_live, bt_harvest_all_live,
        od_harvest_all_live, b2b_harvest_all_live,
        b2b_page_harvest_all_live, sbo_harvest_all_live,
        sp_poll_all_event_details,
    )

    # ── Registry pipeline ─────────────────────────────────────────────────────
    sender.add_periodic_task(14400.0, harvest_all_registry_upcoming.s(), name="registry-upcoming-4h")
    sender.add_periodic_task(86400.0, cleanup_old_snapshots.s(),          name="registry-cleanup-daily")

    # ── Upcoming ──────────────────────────────────────────────────────────────
    sender.add_periodic_task(300.0, sp_harvest_all_upcoming.s(),       name="sp-upcoming-5min")
    sender.add_periodic_task(360.0, bt_harvest_all_upcoming.s(),       name="bt-upcoming-6min")
    sender.add_periodic_task(420.0, od_harvest_all_upcoming.s(),       name="od-upcoming-7min")
    sender.add_periodic_task(480.0, b2b_harvest_all_upcoming.s(),      name="b2b-upcoming-8min")
    sender.add_periodic_task(300.0, b2b_page_harvest_all_upcoming.s(), name="b2b-page-5min")
    sender.add_periodic_task(180.0, sbo_harvest_all_upcoming.s(),      name="sbo-upcoming-3min")

    # ── Live (HTTP fallback — WS is primary for SP) ───────────────────────────
    sender.add_periodic_task( 60.0, sp_harvest_all_live.s(),           name="sp-live-60s")
    sender.add_periodic_task(  5.0, sp_poll_all_event_details.s(),     name="sp-details-5s")
    sender.add_periodic_task( 90.0, bt_harvest_all_live.s(),           name="bt-live-90s")
    sender.add_periodic_task( 90.0, od_harvest_all_live.s(),           name="od-live-90s")
    sender.add_periodic_task(120.0, b2b_harvest_all_live.s(),          name="b2b-live-2min")
    sender.add_periodic_task( 30.0, b2b_page_harvest_all_live.s(),     name="b2b-page-live-30s")
    sender.add_periodic_task( 60.0, sbo_harvest_all_live.s(),          name="sbo-live-60s")

    # ── Ops ───────────────────────────────────────────────────────────────────
    sender.add_periodic_task(300.0,  update_match_results.s(),         name="results-5min")
    sender.add_periodic_task(3600.0, cache_finished_games.s(),         name="cache-finished-hourly")
    sender.add_periodic_task( 30.0,  health_check.s(),                 name="health-30s")
    sender.add_periodic_task(3600.0, expire_subscriptions.s(),         name="expire-subs-hourly")

    # ── Persist combined snapshots to PostgreSQL every 5 minutes ─────────────
    sender.add_periodic_task(300.0, persist_all_sports.s(),            name="persist-combined-5min")

    logger.info("[beat] All periodic tasks registered — SP/BT/OD/B2B/SBO + persist")


# =============================================================================
# WS publish
# =============================================================================

@celery.task(name="tasks.ops.publish_ws_event", soft_time_limit=5, time_limit=10)
def publish_ws_event(channel: str, data: dict) -> bool:
    try:
        _publish(channel, data)
        return True
    except Exception as e:
        logger.warning("[ws:publish] %s: %s", channel, e)
        return False


# =============================================================================
# EV / Arbitrage
# =============================================================================

@celery.task(
    name="tasks.ops.compute_ev_arb", bind=True,
    max_retries=1, soft_time_limit=20, time_limit=30, acks_late=True,
)
def compute_ev_arb(self, match_id: int) -> dict:
    try:
        from app.models.odds_model import (
            UnifiedMatch, OpportunityDetector,
            ArbitrageOpportunity, EVOpportunity,
        )
        from app.extensions import db

        um = UnifiedMatch.query.get(match_id)
        if not um or not um.markets_json:
            return {"ok": False, "reason": "no markets"}

        detector = OpportunityDetector(min_profit_pct=0.5, min_ev_pct=3.0)
        arbs     = detector.find_arbs(um)
        evs      = detector.find_ev(um)

        for kwargs in arbs:
            db.session.add(ArbitrageOpportunity(**kwargs))
        for kwargs in evs:
            db.session.add(EVOpportunity(**kwargs))
        db.session.commit()

        if arbs:
            _publish(ARB_CHANNEL, {
                "event": "arb_updated", "match_id": match_id,
                "match": f"{um.home_team_name} v {um.away_team_name}",
                "sport": um.sport_name, "arbs": len(arbs), "ts": _now_iso(),
            })
        if evs:
            _publish(EV_CHANNEL, {"event": "ev_updated", "match_id": match_id,
                                   "evs": len(evs), "ts": _now_iso()})
        return {"ok": True, "match_id": match_id, "arbs": len(arbs), "evs": len(evs)}

    except Exception as exc:
        logger.error("[ev_arb] match %d: %s", match_id, exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        raise self.retry(exc=exc)


# =============================================================================
# Match results (every 5 min)
# =============================================================================

@celery.task(name="tasks.ops.update_match_results",
             soft_time_limit=120, time_limit=150)
def update_match_results() -> dict:
    try:
        from app.extensions import db
        from app.models.odds_model import UnifiedMatch

        now     = datetime.now(timezone.utc)
        updated = 0

        for um in UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now,
            UnifiedMatch.start_time >= now - timedelta(hours=3),
            UnifiedMatch.status.in_(["PRE_MATCH", "IN_PLAY"]),
        ).all():
            if um.status == "PRE_MATCH":
                um.status = "IN_PLAY"
                updated  += 1

        for um in UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now - timedelta(hours=2, minutes=30),
            UnifiedMatch.status == "IN_PLAY",
        ).all():
            um.status = "FINISHED"
            updated  += 1
            _settle_bankroll_bets(um.id)
            date_str = (um.start_time.strftime("%Y-%m-%d")
                        if um.start_time else now.strftime("%Y-%m-%d"))
            ck     = f"results:finished:{date_str}"
            cached = cache_get(ck) or []
            cached.append(um.to_dict())
            cache_set(ck, cached, ttl=30 * 86400)

        if updated:
            db.session.commit()
            _publish(WS_CHANNEL, {"event": "results_updated",
                                   "updated": updated, "ts": _now_iso()})
        return {"updated": updated}
    except Exception as exc:
        logger.error("[results] %s", exc)
        return {"error": str(exc)}


def _settle_bankroll_bets(match_id: int) -> None:
    try:
        from app.models.bank_roll import BankrollBet
        for bet in BankrollBet.query.filter_by(match_id=match_id, status="pending").all():
            bet.status = "manual_check"
    except Exception:
        pass


# =============================================================================
# Cache finished games (hourly)
# =============================================================================

@celery.task(name="tasks.ops.cache_finished_games",
             soft_time_limit=120, time_limit=150)
def cache_finished_games() -> dict:
    try:
        from app.models.odds_model import UnifiedMatch
        now    = datetime.now(timezone.utc)
        cached = 0
        for day_offset in range(0, 30):
            day_start = (now - timedelta(days=day_offset)).replace(
                hour=0, minute=0, second=0, microsecond=0)
            day_end  = day_start + timedelta(days=1)
            date_str = day_start.strftime("%Y-%m-%d")
            ck       = f"results:finished:{date_str}"
            if cache_get(ck):
                continue
            matches = UnifiedMatch.query.filter(
                UnifiedMatch.status == "FINISHED",
                UnifiedMatch.start_time >= day_start,
                UnifiedMatch.start_time <  day_end,
            ).all()
            if matches:
                cache_set(ck, [m.to_dict() for m in matches], ttl=30 * 86400)
                cached += 1
        return {"cached_days": cached}
    except Exception as exc:
        logger.error("[cache:finished] %s", exc)
        return {"error": str(exc)}


# =============================================================================
# Notifications
# =============================================================================

@celery.task(
    name="tasks.ops.dispatch_notifications", bind=True,
    max_retries=2, soft_time_limit=30, time_limit=45, acks_late=True,
)
def dispatch_notifications(self, match_id: int, event_type: str) -> dict:
    try:
        from app.models.odds_model import (
            UnifiedMatch, ArbitrageOpportunity, EVOpportunity,
        )
        from app.models.notifications import NotificationPref
        from app.models.customer import Customer
        from app.workers.notification_service import NotificationService

        um = UnifiedMatch.query.get(match_id)
        if not um:
            return {"ok": False}
        match_label = f"{um.home_team_name} v {um.away_team_name}"
        arbs = ArbitrageOpportunity.query.filter_by(match_id=match_id).filter(
            ArbitrageOpportunity.status == "OPEN").all()
        evs  = EVOpportunity.query.filter_by(match_id=match_id).filter(
            EVOpportunity.status == "OPEN").all()
        if not arbs and not evs:
            return {"ok": True, "sent": 0}

        prefs = NotificationPref.query.join(Customer).filter(Customer.is_active == True).all()  # noqa: E712
        sent  = 0
        for pref in prefs:
            user = pref.user
            if not pref.email_enabled:
                continue
            qa = [a for a in arbs if (a.profit_pct or 0) >= (pref.arb_min_profit or 0)]
            qe = [e for e in evs  if (e.ev_pct or 0)    >= (pref.ev_min_edge    or 0)]
            if not qa and not qe:
                continue
            try:
                NotificationService.send_alert(
                    user=user, match_label=match_label,
                    arbs=qa, evs=qe, event_type=event_type,
                )
                sent += 1
            except Exception as e:
                logger.warning("[notify] user %s: %s", user.id, e)
        return {"ok": True, "sent": sent}
    except Exception as exc:
        raise self.retry(exc=exc)


# =============================================================================
# Subscription expiry (hourly)
# =============================================================================

@celery.task(name="tasks.ops.expire_subscriptions",
             soft_time_limit=30, time_limit=45)
def expire_subscriptions() -> dict:
    try:
        from app.extensions import db
        from app.models.subscriptions import Subscription, SubscriptionStatus
        now     = datetime.now(timezone.utc)
        expired = 0
        for sub in Subscription.query.filter(
            Subscription.status   == SubscriptionStatus.TRIAL.value,
            Subscription.is_trial == True,  # noqa: E712
            Subscription.trial_ends <= now,
        ).all():
            sub.status   = SubscriptionStatus.EXPIRED.value
            sub.is_trial = False
            expired     += 1
        for sub in Subscription.query.filter(
            Subscription.status     == SubscriptionStatus.ACTIVE.value,
            Subscription.auto_renew == True,  # noqa: E712
            Subscription.period_end <= now,
        ).all():
            sub.status = SubscriptionStatus.EXPIRED.value
            expired   += 1
        if expired:
            db.session.commit()
        return {"processed": expired}
    except Exception as exc:
        logger.error("[subs:expire] %s", exc)
        return {"error": str(exc)}


# =============================================================================
# Health check (every 30 s)
# =============================================================================

@celery.task(name="tasks.ops.health_check",
             soft_time_limit=10, time_limit=15)
def health_check() -> dict:
    ts = _now_iso()
    cache_set("worker_heartbeat", {"alive": True, "checked_at": ts, "pid": os.getpid()}, ttl=120)
    return {"ok": True, "ts": ts}


# =============================================================================
# Email + WhatsApp
# =============================================================================

@celery.task(
    name="tasks.ops.send_async_email", bind=True,
    max_retries=3, default_retry_delay=30,
    soft_time_limit=60, time_limit=90,
)
def send_async_email(self, subject, recipients, body, body_type="plain",
                     attachments=None, username=None, password=None):
    try:
        from app import create_app
        from flask_mail import Mail, Message
        app = create_app()
        with app.app_context():
            mail = Mail(app)
            msg  = Message(subject=subject, recipients=recipients,
                           sender=os.environ.get("ADMIN_EMAIL"))
            if body_type == "html":
                msg.html = body
            else:
                msg.body = body
            for att in (attachments or []):
                fname   = att.get("filename", "file")
                mime    = att.get("mimetype", "application/octet-stream")
                content = att.get("content")
                if content:
                    try:
                        msg.attach(fname, mime, base64.b64decode(content))
                    except Exception as e:
                        logger.warning("[email] attachment: %s", e)
            mail.send(msg)
    except Exception as exc:
        raise self.retry(exc=exc)


@celery.task(name="tasks.ops.send_message",
             soft_time_limit=30, time_limit=45)
def send_message(msg: str, whatsapp_number: str):
    if not message_url:
        return {"error": "WA_BOT not configured"}
    r = requests.post(message_url, json={"message": msg, "number": whatsapp_number}, timeout=15)
    return r.text


# =============================================================================
# Persist combined snapshots to PostgreSQL
# =============================================================================

@celery.task(
    name="tasks.ops.persist_combined_batch",
    bind=True,
    max_retries=3,
    default_retry_delay=30,
    soft_time_limit=90,
    time_limit=120,
    acks_late=True,
)
def persist_combined_batch(
    self,
    match_dicts: list[dict],
    sport_slug: str = "soccer",
    mode: str = "upcoming",
) -> dict:
    """
    Persist a serialised list of CombinedMatch dicts (output of cm.to_dict())
    to PostgreSQL via persist_hook.persist_from_serialized.

    Called by:
      • persist_hook.persist_merged_async  — fire-and-forget after each SSE merge
      • persist_all_sports                 — beat fan-out every 5 minutes

    Retries up to 3× with a 30-second backoff on any exception.
    """
    if not match_dicts:
        logger.debug("[persist_combined_batch] %s/%s — empty batch, skipping", sport_slug, mode)
        return {"persisted": 0, "failed": 0, "sport": sport_slug, "mode": mode}

    logger.info(
        "[persist_combined_batch] %s/%s — %d matches",
        sport_slug, mode, len(match_dicts),
    )
    try:
        from app.services.persist_hook import persist_from_serialized
        stats = persist_from_serialized(match_dicts, sport_slug=sport_slug, mode=mode)
        stats.update({"sport": sport_slug, "mode": mode})
        return stats
    except Exception as exc:
        logger.error(
            "[persist_combined_batch] %s/%s error (retry %d/%d): %s",
            sport_slug, mode, self.request.retries, self.max_retries, exc,
        )
        raise self.retry(exc=exc)


@celery.task(
    name="tasks.ops.persist_all_sports",
    soft_time_limit=60,
    time_limit=90,
    acks_late=True,
)
def persist_all_sports() -> dict:
    """
    Beat task — runs every 5 minutes (registered in setup_periodic_tasks).

    Reads each sport's combined:upcoming and combined:live snapshots from
    Redis and fans out one persist_combined_batch task per sport/mode so
    every sport is persisted independently and a single failure is isolated.

    Live mode tasks get countdown=2 s; upcoming get countdown=10 s to
    spread the DB load across the 5-minute window.
    """
    dispatched = 0
    skipped    = 0
    summary: dict[str, dict] = {}

    for sport in PERSIST_SPORTS:
        summary[sport] = {"upcoming": 0, "live": 0}

        for mode, countdown in (("upcoming", 10), ("live", 2)):
            cached = cache_get(f"combined:{mode}:{sport}")

            if not cached or not cached.get("matches"):
                logger.debug("[persist_all_sports] %s/%s — cache miss, skipping", sport, mode)
                skipped += 1
                continue

            match_dicts: list[dict] = cached["matches"]
            if not match_dicts:
                skipped += 1
                continue

            persist_combined_batch.apply_async(
                args=[match_dicts, sport, mode],
                queue="results",
                countdown=countdown,
            )
            summary[sport][mode] = len(match_dicts)
            dispatched += 1
            logger.info(
                "[persist_all_sports] dispatched %s/%s — %d matches",
                sport, mode, len(match_dicts),
            )

    logger.info(
        "[persist_all_sports] cycle complete — dispatched=%d skipped=%d",
        dispatched, skipped,
    )
    return {"dispatched": dispatched, "skipped": skipped, "summary": summary}