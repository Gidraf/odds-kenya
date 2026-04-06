"""
app/workers/tasks_ops.py
========================
Operational tasks: EV/arb, results, notifications, health, subscriptions,
immediate startup harvesting (upcoming only), minute health reports, job logging.

LIVE_ENABLED = False
  • Beat schedule has NO live tasks.
  • Startup only dispatches upcoming harvests.
  • Set LIVE_ENABLED = True here AND in celery_app.py when upcoming is stable.

Celery entry point:
  celery -A app.workers.celery_tasks worker -B ...
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from celery.signals import worker_ready
from celery.utils.log import get_task_logger

from app.workers.celery_tasks import (
    celery, cache_set, cache_get, _now_iso, _publish,
)

logger = get_task_logger(__name__)

# ── Feature flag — must match celery_app.py ───────────────────────────────────
LIVE_ENABLED: bool = False

WS_CHANNEL  = "odds:updates"
ARB_CHANNEL = "arb:updates"
EV_CHANNEL  = "ev:updates"

whatsapp_bot = os.environ.get("WA_BOT", "")
message_url  = f"{whatsapp_bot}/api/v1/send-message" if whatsapp_bot else ""

PERSIST_SPORTS: list[str] = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "rugby", "handball", "volleyball", "cricket",
    "table-tennis", "esoccer", "mma", "boxing", "darts",
]

# ── Structured job logger ─────────────────────────────────────────────────────
LOG_DIR = Path(os.environ.get("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_job_logger = logging.getLogger("harvest_jobs")
if not _job_logger.handlers:
    _fh = logging.FileHandler(LOG_DIR / "harvest_jobs.log")
    _fh.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%dT%H:%M:%SZ"))
    _job_logger.addHandler(_fh)
    _job_logger.setLevel(logging.INFO)
    _job_logger.propagate = False


def log_job(bookmaker: str, sport: str, mode: str, count: int, status: str,
            unified_ok: int = 0, unified_fail: int = 0,
            latency_ms: int = 0, detail: str = "") -> None:
    entry = {
        "ts": _now_iso(), "bookmaker": bookmaker, "sport": sport,
        "mode": mode, "count": count, "status": status,
        "unified_ok": unified_ok, "unified_fail": unified_fail,
        "latency_ms": latency_ms, "detail": detail,
    }
    _job_logger.info(json.dumps(entry))
    try:
        from app.workers.celery_tasks import _redis
        r = _redis()
        r.lpush("monitor:job_log", json.dumps(entry, default=str))
        r.ltrim("monitor:job_log", 0, 499)
    except Exception:
        pass


# =============================================================================
# ── STARTUP HARVEST  (upcoming only while LIVE_ENABLED = False)
# =============================================================================

@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    logger.info("[startup] Worker ready — dispatching upcoming harvests")
    try:
        _dispatch_startup_harvests()
    except Exception as exc:
        logger.error("[startup] Dispatch failed: %s", exc)


def _dispatch_startup_harvests() -> None:
    """
    Fan-out upcoming harvest tasks immediately on worker boot.
    Staggered countdowns so external APIs aren't hit simultaneously.

    Live tasks are NOT dispatched here — add them back when LIVE_ENABLED = True.
    """
    upcoming_tasks = [
        ("tasks.sp.harvest_all_upcoming",       2),
        ("tasks.bt.harvest_all_upcoming",       5),
        ("tasks.od.harvest_all_upcoming",       8),
        ("tasks.b2b.harvest_all_upcoming",      11),
        ("tasks.b2b_page.harvest_all_upcoming", 13),
        ("tasks.sbo.harvest_all_upcoming",      15),
    ]
    for task_name, cd in upcoming_tasks:
        celery.send_task(task_name, queue="harvest", countdown=cd)
        logger.debug("[startup] dispatched %s (countdown=%ds)", task_name, cd)

    # Health check so heartbeat appears in Redis immediately
    celery.send_task("tasks.ops.health_check", queue="default", countdown=1)

    # First health report after 25 s (upcoming caches should be seeding by then)
    celery.send_task("tasks.ops.build_health_report", queue="default", countdown=25)

    logger.info("[startup] %d upcoming tasks dispatched (live DISABLED)",
                len(upcoming_tasks))


# =============================================================================
# Beat schedule  (upcoming only)
# =============================================================================

@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    from app.workers.tasks_upcoming import (
        harvest_all_registry_upcoming, cleanup_old_snapshots,
        sp_harvest_all_upcoming, bt_harvest_all_upcoming,
        od_harvest_all_upcoming, b2b_harvest_all_upcoming,
        b2b_page_harvest_all_upcoming, sbo_harvest_all_upcoming,
    )

    # ── Registry ──────────────────────────────────────────────────────────────
    sender.add_periodic_task(14400.0, harvest_all_registry_upcoming.s(), name="registry-upcoming-4h")
    sender.add_periodic_task(86400.0, cleanup_old_snapshots.s(),          name="registry-cleanup-daily")

    # ── Upcoming harvests (one per bookmaker, staggered) ──────────────────────
    # sender.add_periodic_task(300.0, sp_harvest_all_upcoming.s(),       name="sp-upcoming-5min")
    # sender.add_periodic_task(360.0, bt_harvest_all_upcoming.s(),       name="bt-upcoming-6min")
    # sender.add_periodic_task(420.0, od_harvest_all_upcoming.s(),       name="od-upcoming-7min")
    sender.add_periodic_task(480.0, b2b_harvest_all_upcoming.s(),      name="b2b-upcoming-8min")
    sender.add_periodic_task(300.0, b2b_page_harvest_all_upcoming.s(), name="b2b-page-upcoming-5min")
    sender.add_periodic_task(180.0, sbo_harvest_all_upcoming.s(),      name="sbo-upcoming-3min")

    sender.add_periodic_task(3600.0, sp_harvest_all_upcoming.s(), name="sp-upcoming-1h")
    sender.add_periodic_task(3600.0, bt_harvest_all_upcoming.s(), name="bt-upcoming-1h")
    sender.add_periodic_task(3600.0, od_harvest_all_upcoming.s(), name="od-upcoming-1h")

    # ── Ops ───────────────────────────────────────────────────────────────────
    sender.add_periodic_task(300.0,  update_match_results.s(),         name="results-5min")
    sender.add_periodic_task(3600.0, cache_finished_games.s(),         name="cache-finished-hourly")
    sender.add_periodic_task( 30.0,  health_check.s(),                 name="health-30s")
    sender.add_periodic_task(3600.0, expire_subscriptions.s(),         name="expire-subs-hourly")

    # ── DB persist (upcoming only) ─────────────────────────────────────────────
    sender.add_periodic_task(300.0, persist_all_sports.s(),            name="persist-upcoming-5min")

    # ── Monitor ───────────────────────────────────────────────────────────────
    sender.add_periodic_task(60.0, build_health_report.s(),            name="health-report-1min")

    # ── Live tasks (commented out — re-enable when LIVE_ENABLED = True) ───────
    # from app.workers.tasks_live import (
    #     sp_harvest_all_live, bt_harvest_all_live,
    #     od_harvest_all_live, b2b_harvest_all_live,
    #     b2b_page_harvest_all_live, sbo_harvest_all_live,
    #     sp_poll_all_event_details,
    # )
    # sender.add_periodic_task( 60.0, sp_harvest_all_live.s(),         name="sp-live-60s")
    # sender.add_periodic_task(  5.0, sp_poll_all_event_details.s(),   name="sp-details-5s")
    # sender.add_periodic_task( 90.0, bt_harvest_all_live.s(),         name="bt-live-90s")
    # sender.add_periodic_task( 90.0, od_harvest_all_live.s(),         name="od-live-90s")
    # sender.add_periodic_task(120.0, b2b_harvest_all_live.s(),        name="b2b-live-2min")
    # sender.add_periodic_task( 30.0, b2b_page_harvest_all_live.s(),   name="b2b-page-live-30s")
    # sender.add_periodic_task( 60.0, sbo_harvest_all_live.s(),        name="sbo-live-60s")

    logger.info("[beat] Beat schedule registered — UPCOMING ONLY (live disabled)")


# =============================================================================
# WS publish
# =============================================================================

@celery.task(name="tasks.ops.publish_ws_event", soft_time_limit=60, time_limit=180)
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

@celery.task(name="tasks.ops.compute_ev_arb", bind=True,
             max_retries=1, soft_time_limit=210, time_limit=300, acks_late=True)
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

        for kwargs in arbs: db.session.add(ArbitrageOpportunity(**kwargs))
        for kwargs in evs:  db.session.add(EVOpportunity(**kwargs))
        db.session.commit()

        if arbs:
            _publish(ARB_CHANNEL, {"event": "arb_updated", "match_id": match_id,
                                    "match": f"{um.home_team_name} v {um.away_team_name}",
                                    "sport": um.sport_name, "arbs": len(arbs), "ts": _now_iso()})
        if evs:
            _publish(EV_CHANNEL, {"event": "ev_updated", "match_id": match_id,
                                   "evs": len(evs), "ts": _now_iso()})
        return {"ok": True, "match_id": match_id, "arbs": len(arbs), "evs": len(evs)}

    except Exception as exc:
        logger.error("[ev_arb] match %d: %s", match_id, exc)
        try:
            from app.extensions import db; db.session.rollback()
        except Exception:
            pass
        raise self.retry(exc=exc)


# =============================================================================
# Match results
# =============================================================================

@celery.task(name="tasks.ops.update_match_results",
             soft_time_limit=1200, time_limit=1500)
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
                um.status = "IN_PLAY"; updated += 1

        for um in UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now - timedelta(hours=2, minutes=30),
            UnifiedMatch.status == "IN_PLAY",
        ).all():
            um.status = "FINISHED"; updated += 1
            _settle_bankroll_bets(um.id)
            date_str = um.start_time.strftime("%Y-%m-%d") if um.start_time else now.strftime("%Y-%m-%d")
            ck = f"results:finished:{date_str}"
            cached = cache_get(ck) or []
            cached.append(um.to_dict())
            cache_set(ck, cached, ttl=30 * 86400)

        if updated:
            db.session.commit()
            _publish(WS_CHANNEL, {"event": "results_updated", "updated": updated, "ts": _now_iso()})
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
# Cache finished games
# =============================================================================

@celery.task(name="tasks.ops.cache_finished_games",
             soft_time_limit=1200, time_limit=1500)
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
            if cache_get(ck): continue
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

@celery.task(name="tasks.ops.dispatch_notifications", bind=True,
             max_retries=2, soft_time_limit=300, time_limit=450, acks_late=True)
def dispatch_notifications(self, match_id: int, event_type: str) -> dict:
    try:
        from app.models.odds_model import (UnifiedMatch, ArbitrageOpportunity, EVOpportunity)
        from app.models.notifications import NotificationPref
        from app.models.customer import Customer
        from app.workers.notification_service import NotificationService

        um = UnifiedMatch.query.get(match_id)
        if not um: return {"ok": False}
        match_label = f"{um.home_team_name} v {um.away_team_name}"
        arbs = ArbitrageOpportunity.query.filter_by(match_id=match_id).filter(
            ArbitrageOpportunity.status == "OPEN").all()
        evs  = EVOpportunity.query.filter_by(match_id=match_id).filter(
            EVOpportunity.status == "OPEN").all()
        if not arbs and not evs: return {"ok": True, "sent": 0}

        prefs = NotificationPref.query.join(Customer).filter(Customer.is_active == True).all()  # noqa
        sent  = 0
        for pref in prefs:
            user = pref.user
            if not pref.email_enabled: continue
            qa = [a for a in arbs if (a.profit_pct or 0) >= (pref.arb_min_profit or 0)]
            qe = [e for e in evs  if (e.ev_pct     or 0) >= (pref.ev_min_edge    or 0)]
            if not qa and not qe: continue
            try:
                NotificationService.send_alert(user=user, match_label=match_label,
                                               arbs=qa, evs=qe, event_type=event_type)
                sent += 1
            except Exception as e:
                logger.warning("[notify] user %s: %s", user.id, e)
        return {"ok": True, "sent": sent}
    except Exception as exc:
        raise self.retry(exc=exc)


# =============================================================================
# Subscription expiry
# =============================================================================

@celery.task(name="tasks.ops.expire_subscriptions",
             soft_time_limit=300, time_limit=450)
def expire_subscriptions() -> dict:
    try:
        from app.extensions import db
        from app.models.subscriptions import Subscription, SubscriptionStatus
        now     = datetime.now(timezone.utc)
        expired = 0
        for sub in Subscription.query.filter(
            Subscription.status   == SubscriptionStatus.TRIAL.value,
            Subscription.is_trial == True,  # noqa
            Subscription.trial_ends <= now,
        ).all():
            sub.status = SubscriptionStatus.EXPIRED.value; sub.is_trial = False; expired += 1
        for sub in Subscription.query.filter(
            Subscription.status     == SubscriptionStatus.ACTIVE.value,
            Subscription.auto_renew == True,  # noqa
            Subscription.period_end <= now,
        ).all():
            sub.status = SubscriptionStatus.EXPIRED.value; expired += 1
        if expired: db.session.commit()
        return {"processed": expired}
    except Exception as exc:
        logger.error("[subs:expire] %s", exc)
        return {"error": str(exc)}


# =============================================================================
# Health check
# =============================================================================

@celery.task(name="tasks.ops.health_check", soft_time_limit=10, time_limit=15)
def health_check() -> dict:
    ts = _now_iso()
    cache_set("worker_heartbeat", {"alive": True, "checked_at": ts, "pid": os.getpid()}, ttl=120)
    return {"ok": True, "ts": ts}


# =============================================================================
# Email + WhatsApp
# =============================================================================

@celery.task(name="tasks.ops.send_async_email", bind=True,
             max_retries=3, default_retry_delay=300,
             soft_time_limit=600, time_limit=900)
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
            if body_type == "html": msg.html = body
            else:                   msg.body = body
            for att in (attachments or []):
                content = att.get("content")
                if content:
                    try:
                        msg.attach(att.get("filename","file"),
                                   att.get("mimetype","application/octet-stream"),
                                   base64.b64decode(content))
                    except Exception as e:
                        logger.warning("[email] attachment: %s", e)
            mail.send(msg)
    except Exception as exc:
        raise self.retry(exc=exc)


@celery.task(name="tasks.ops.send_message", soft_time_limit=300, time_limit=450)
def send_message(msg: str, whatsapp_number: str):
    if not message_url: return {"error": "WA_BOT not configured"}
    r = requests.post(message_url, json={"message": msg, "number": whatsapp_number}, timeout=15)
    return r.text


# =============================================================================
# Persist upcoming snapshots to PostgreSQL
# =============================================================================

@celery.task(name="tasks.ops.persist_combined_batch", bind=True,
             max_retries=3, default_retry_delay=300,
             soft_time_limit=900, time_limit=1200, acks_late=True)
def persist_combined_batch(self, match_dicts: list[dict],
                            sport_slug: str = "soccer",
                            mode: str = "upcoming") -> dict:
    if not match_dicts:
        return {"persisted": 0, "failed": 0, "sport": sport_slug, "mode": mode}

    t0 = time.perf_counter()
    try:
        from app.workers.persist_hook import persist_from_serialized
        stats = persist_from_serialized(match_dicts, sport_slug=sport_slug, mode=mode)
        stats.update({"sport": sport_slug, "mode": mode})
        log_job(bookmaker="combined", sport=sport_slug, mode=mode,
                count=len(match_dicts), status="ok",
                unified_ok=stats.get("persisted", 0),
                unified_fail=stats.get("failed", 0),
                latency_ms=int((time.perf_counter() - t0) * 1000))
        return stats
    except Exception as exc:
        log_job(bookmaker="combined", sport=sport_slug, mode=mode,
                count=len(match_dicts), status="error",
                detail=str(exc)[:200],
                latency_ms=int((time.perf_counter() - t0) * 1000))
        raise self.retry(exc=exc)


@celery.task(name="tasks.ops.persist_all_sports",
             soft_time_limit=600, time_limit=900, acks_late=True)
def persist_all_sports() -> dict:
    """
    Beat task — every 5 minutes.
    Reads combined:upcoming:{sport} from Redis for every sport in PERSIST_SPORTS
    and fans out one persist_combined_batch task per sport.

    Live mode is skipped while LIVE_ENABLED = False.
    """
    dispatched = 0
    skipped    = 0
    summary: dict[str, dict] = {}

    modes = [("upcoming", 10)]
    if LIVE_ENABLED:
        modes.append(("live", 2))

    for sport in PERSIST_SPORTS:
        summary[sport] = {m: 0 for m, _ in modes}
        for mode, countdown in modes:
            cached = cache_get(f"combined:{mode}:{sport}")
            if not cached or not cached.get("matches"):
                skipped += 1
                continue
            match_dicts = cached["matches"]
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

    logger.info("[persist_all_sports] dispatched=%d skipped=%d live_enabled=%s",
                dispatched, skipped, LIVE_ENABLED)
    return {"dispatched": dispatched, "skipped": skipped,
            "summary": summary, "live_enabled": LIVE_ENABLED}


# =============================================================================
# Health report  (every 60 s)
# =============================================================================

@celery.task(name="tasks.ops.build_health_report",
             soft_time_limit=300, time_limit=450, acks_late=True)
def build_health_report() -> dict:
    t0  = time.perf_counter()
    now = _now_iso()

    hb           = cache_get("worker_heartbeat") or {}
    worker_alive = bool(hb.get("alive"))
    hb_age: int | None = None
    if hb.get("checked_at"):
        try:
            checked = datetime.fromisoformat(hb["checked_at"].replace("Z", "+00:00"))
            hb_age  = int((datetime.now(timezone.utc) - checked).total_seconds())
        except Exception:
            pass

    redis_ok = False
    try:
        from app.workers.celery_tasks import _redis
        _redis().ping(); redis_ok = True
    except Exception:
        pass

    sports_report: list[dict] = []
    total_upcoming = total_arbs = total_evs = 0
    cold_sports: list[str] = []

    for sport in PERSIST_SPORTS:
        u   = cache_get(f"combined:upcoming:{sport}") or {}
        u_m = u.get("matches", [])
        u_arbs = sum(1 for m in u_m if m.get("has_arb"))
        u_evs  = sum(1 for m in u_m if m.get("has_ev"))
        populated = bool(u_m)
        if not populated: cold_sports.append(sport)
        total_upcoming += len(u_m)
        total_arbs     += u_arbs
        total_evs      += u_evs
        sports_report.append({
            "sport":           sport,
            "upcoming_count":  len(u_m),
            "live_count":      0,          # live disabled
            "upcoming_arbs":   u_arbs,
            "upcoming_evs":    u_evs,
            "harvested_at_up": u.get("harvested_at"),
            "bk_counts_up":    u.get("bk_counts", {}),
            "populated":       populated,
            "status":          "ok" if populated else "cold",
        })

    recent_jobs: list[dict] = []
    try:
        from app.workers.celery_tasks import _redis
        for rj in _redis().lrange("monitor:job_log", 0, 19):
            try: recent_jobs.append(json.loads(rj))
            except Exception: pass
    except Exception:
        pass

    checks: list[dict] = []
    def _chk(name, passed, detail=""):
        checks.append({"name": name, "passed": passed, "detail": detail})

    _chk("Worker heartbeat",  worker_alive,         f"age={hb_age}s")
    _chk("Redis reachable",   redis_ok)
    _chk("Sports cache warm", not cold_sports,
         f"{len(PERSIST_SPORTS)-len(cold_sports)}/{len(PERSIST_SPORTS)} warm"
         + (f" — cold: {', '.join(cold_sports)}" if cold_sports else ""))
    _chk("Upcoming matches",  total_upcoming > 0,   f"{total_upcoming} total")
    _chk("Arb opportunities", total_arbs >= 0,       f"{total_arbs}")
    _chk("EV opportunities",  total_evs  >= 0,       f"{total_evs}")

    passed = sum(1 for c in checks if c["passed"])
    failed = sum(1 for c in checks if not c["passed"])

    report = {
        "generated_at":  now,
        "latency_ms":    int((time.perf_counter() - t0) * 1000),
        "live_enabled":  LIVE_ENABLED,
        "overall": {"passed": passed, "failed": failed,
                    "total_checks": len(checks), "healthy": failed == 0},
        "worker":  {"alive": worker_alive, "heartbeat_at": hb.get("checked_at"),
                    "heartbeat_age_s": hb_age, "pid": hb.get("pid")},
        "redis":   {"ok": redis_ok},
        "totals":  {"upcoming_matches": total_upcoming, "live_matches": 0,
                    "arb_opportunities": total_arbs, "ev_opportunities": total_evs,
                    "cold_sports": cold_sports},
        "sports":      sports_report,
        "checks":      checks,
        "recent_jobs": recent_jobs,
    }

    cache_set("monitor:report", report, ttl=180)

    try:
        from app.workers.celery_tasks import _redis
        _redis().lpush("monitor:report_history", json.dumps({
            "ts": now, "healthy": failed == 0, "passed": passed, "failed": failed,
            "upcoming": total_upcoming, "live": 0,
            "arbs": total_arbs, "evs": total_evs,
        }, default=str))
        _redis().ltrim("monitor:report_history", 0, 59)
    except Exception:
        pass

    logger.info("[health_report] healthy=%s passed=%d failed=%d upcoming=%d",
                failed == 0, passed, failed, total_upcoming)
    return report