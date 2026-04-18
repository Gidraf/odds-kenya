"""
app/workers/tasks_ops.py
========================
Operational tasks: EV/arb, results, notifications, health, subscriptions,
persist pipeline, minute health reports, job logging.

LIVE_ENABLED = False
  • No live tasks in beat schedule.
  • Startup dispatches SP per-sport tasks only.
    BT and OD are handled automatically by sp_cross_bk_enrich (+10s) and
    tasks.bt_od.harvest_sport (+30s) which SP dispatches after each harvest.
    No separate BT/OD startup tasks needed.

Market Alignment Service
  • align_all_sports runs every 15 min (beat) and is also triggered
    automatically 30–60 s after each sp_cross_bk_enrich completes.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import random
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from celery.signals import worker_ready
from celery.utils.log import get_task_logger

from app.models.oppotunity_detector import OpportunityDetector
from app.workers.celery_tasks import (
    celery, cache_set, cache_get, _now_iso, _publish,
)
from app.workers.tasks_bt_od import bt_od_harvest_all

logger = get_task_logger(__name__)

LIVE_ENABLED: bool = True

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

# All cache prefixes to check when building health reports
# bt_od is the new team-name matched cache written by tasks_bt_od
_BK_SLUGS: list[str] = ["sp", "bt", "od", "b2b", "sbo", "bt_od"]

_ALL_SPORTS: list[str] = [
    "soccer",
    "basketball",
    "tennis",
    "cricket",
    "rugby",
    "ice-hockey",
    "volleyball",
    "handball",
    "table-tennis",
    "baseball",
    "mma",
    "boxing",
    "darts",
    "american-football",
    "esoccer",
]
 
# Subsets per bookmaker (what each BK actually supports)
_SP_SPORTS: list[str] = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
]
 
_B2B_SPORTS_STARTUP: list[str] = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
    "darts", "handball",
]
 
_SBO_SPORTS_STARTUP: list[str] = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "boxing",
    "handball", "mma", "table-tennis",
]

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
# STARTUP HARVEST
# =============================================================================

@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    logger.info(
        "[startup] Worker ready — dispatching SP harvests "
        "(BT/OD via cross-BK enrich +10s and bt_od team-name match +30s)"
    )
    try:
        _dispatch_startup_harvests()
    except Exception as exc:
        logger.error("[startup] Dispatch failed: %s", exc)


# app/workers/tasks_ops.py (excerpt – full file in final answer)
...
LIVE_ENABLED = True   # ← Already True in your code

@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    logger.info("[startup] Worker ready — dispatching SP harvests & starting live pollers")
    _dispatch_startup_harvests()
    _start_live_pollers()   # ← NEW: starts BT/OD pollers and SP WebSocket harvester

def _start_live_pollers():
    """Start Betika + OdiBets live pollers and SP WebSocket harvester."""
    from app.workers.bt_harvester import BetikaLivePoller
    from app.workers.od_harvester import OdiBetsLivePoller
    from app.workers.celery_tasks import _redis
    from app.workers.sp_live_harvester import start_harvester_thread

    r = _redis(db=1)

    # Betika live poller (already exists in bt_harvester)
    try:
        bt_poller = BetikaLivePoller(redis_client=r, interval=1.5)
        bt_poller.start()
        logger.info("[startup] Betika live poller started")
    except Exception as e:
        logger.error("[startup] Betika live poller failed: %s", e)

    # OdiBets live poller
    try:
        od_poller = OdiBetsLivePoller(redis_client=r, interval=2.0)
        od_poller.start()
        logger.info("[startup] OdiBets live poller started")
    except Exception as e:
        logger.error("[startup] OdiBets live poller failed: %s", e)

    # SP WebSocket harvester
    try:
        start_harvester_thread()
        logger.info("[startup] SP live WebSocket harvester started")
    except Exception as e:
        logger.error("[startup] SP live harvester failed: %s", e)

def setup_periodic_tasks(sender, **kw):
    from app.workers.tasks_harvest_pages import harvest_all_paged
    from app.workers.tasks_ops import (
        update_match_results, expire_subscriptions, health_check,
        cleanup_old_snapshots, build_health_report,
    )
    from app.workers.tasks_live import (
        live_cross_bk_refresh, live_snapshot_to_db,
        ensure_harvester_running, sp_harvest_all_live,
        bt_harvest_all_live, od_harvest_all_live,
    )

    # ── Full upcoming sweep every 4 hours ─────────────────────────────────
    sender.add_periodic_task(
        4 * 3600.0,
        harvest_all_paged.s(),
        name="harvest-all-paged-4h",
    )

    # ── Near‑term refresh every 15 min (optional, can be added later) ───
    # sender.add_periodic_task(900.0, harvest_near_term.s(), name="harvest-near-term-15m")

    # ── Live tasks (all lightweight, frequent) ───────────────────────────
    sender.add_periodic_task(15.0,   live_cross_bk_refresh.s(),    name="live-cross-bk-15s")
    sender.add_periodic_task(30.0,   live_snapshot_to_db.s(),      name="live-snapshot-30s")
    sender.add_periodic_task(60.0,   ensure_harvester_running.s(), name="live-ensure-harvester-60s")
    sender.add_periodic_task(60.0,   sp_harvest_all_live.s(),      name="sp-live-snapshot-60s")
    sender.add_periodic_task(90.0,   bt_harvest_all_live.s(),      name="bt-live-90s")
    sender.add_periodic_task(90.0,   od_harvest_all_live.s(),      name="od-live-90s")

    # ── Health & maintenance ──────────────────────────────────────────────
    sender.add_periodic_task(60.0,    health_check.s(),            name="health-1min")
    sender.add_periodic_task(300.0,   build_health_report.s(),     name="health-report-5min")
    sender.add_periodic_task(86400.0, cleanup_old_snapshots.s(),   name="cleanup-daily")
    sender.add_periodic_task(900.0,   _align_all.s(),              name="align-all-15min")

def _dispatch_startup_harvestsv1() -> None:
    dispatched = 0

    from app.workers.tasks_upcoming import SP_MAX_MATCHES
    for i, sport in enumerate(_SP_SPORTS):
        celery.send_task(
            "tasks.sp.harvest_sport",
            args=[sport, SP_MAX_MATCHES],
            queue="harvest",
            countdown=5 + i * 5,
        )
        dispatched += 1

    for i, sport in enumerate(_B2B_SPORTS_STARTUP):
        celery.send_task(
            "tasks.b2b.harvest_sport",
            args=[sport],
            queue="harvest",
            countdown=10 + i * 3,
        )
        dispatched += 1

    for i, sport in enumerate(_SBO_SPORTS_STARTUP):
        celery.send_task(
            "tasks.sbo.harvest_sport",
            args=[sport, 90],
            queue="harvest",
            countdown=15 + i * 3,
        )
        dispatched += 1

    celery.send_task("tasks.ops.health_check",        queue="default",  countdown=1)
    celery.send_task("tasks.ops.build_health_report", queue="default",  countdown=30)
    celery.send_task("tasks.align.all",               queue="results",  countdown=180)

    logger.info(
        "[startup] %d tasks dispatched (SP=%d, B2B=%d, SBO=%d). "
        "BT+OD via sp_cross_bk_enrich (+10s) and bt_od (+30s) per sport.",
        dispatched, len(_SP_SPORTS), len(_B2B_SPORTS_STARTUP), len(_SBO_SPORTS_STARTUP),
    )


def _dispatch_startup_harvests() -> None:
    """
    Dispatch paged SP + BT + OD harvests for all sports on worker startup.
    Each sport uses 10 parallel page workers (100 records/page = 1 000 total).
    """
    dispatched = 0
 
    # ── SP paged (10 pages × 100 = 1 000 records per sport) ──────────────────
    for i, sport in enumerate(_SP_SPORTS):
        celery.send_task(
            "tasks.sp.harvest_sport_paged",
            args=[sport],
            queue="harvest",
            countdown=5 + i * 3,
        )
        dispatched += 1
 
    # ── BT paged ──────────────────────────────────────────────────────────────
    from app.workers.tasks_harvest_pages import _BT_SPORTS
    for i, sport in enumerate(_BT_SPORTS):
        celery.send_task(
            "tasks.bt.harvest_sport_paged",
            args=[sport],
            queue="harvest",
            countdown=10 + i * 3,
        )
        dispatched += 1
 
    # ── OD paged ──────────────────────────────────────────────────────────────
    from app.workers.tasks_harvest_pages import _OD_SPORTS
    for i, sport in enumerate(_OD_SPORTS):
        celery.send_task(
            "tasks.od.harvest_sport_paged",
            args=[sport],
            queue="harvest",
            countdown=15 + i * 3,
        )
        dispatched += 1
 
    # ── B2B + SBO (unchanged) ─────────────────────────────────────────────────
    for i, sport in enumerate(_B2B_SPORTS_STARTUP):
        celery.send_task(
            "tasks.b2b.harvest_sport",
            args=[sport],
            queue="harvest",
            countdown=20 + i * 3,
        )
        dispatched += 1
 
    for i, sport in enumerate(_SBO_SPORTS_STARTUP):
        celery.send_task(
            "tasks.sbo.harvest_sport",
            args=[sport, 90],
            queue="harvest",
            countdown=25 + i * 3,
        )
        dispatched += 1
 
    # ── System tasks ──────────────────────────────────────────────────────────
    celery.send_task("tasks.ops.health_check",        queue="default",  countdown=1)
    celery.send_task("tasks.ops.build_health_report", queue="default",  countdown=60)
    celery.send_task("tasks.align.all",               queue="results",  countdown=240)
 
    logger.info(
        "[startup] %d tasks dispatched — SP/BT/OD paged (10 workers × 100 records each).",
        dispatched,
    )
 

# =============================================================================
# BEAT SCHEDULE
# =============================================================================

def setup_periodic_tasks(sender, **kw):
    from app.workers.tasks_harvest_pages import harvest_all_paged
    from app.workers.tasks_ops import (
        update_match_results,
        expire_subscriptions,
        health_check,
        cleanup_old_snapshots,
        build_health_report,
    )
 
    # ── Primary: paged SP + BT + OD every 5 min ──────────────────────────────
    sender.add_periodic_task(
        300.0,
        harvest_all_paged.s(),
        name="harvest-all-paged-5min",
    )
 
    # ── Health + cleanup ──────────────────────────────────────────────────────
    sender.add_periodic_task(60.0,    health_check.s(),         name="health-1min")
    sender.add_periodic_task(300.0,   build_health_report.s(),  name="health-report-5min")
    sender.add_periodic_task(86400.0, cleanup_old_snapshots.s(), name="cleanup-daily")
 
    # ── Results + subs (uncomment when ready) ─────────────────────────────────
    # sender.add_periodic_task(600.0,   update_match_results.s(),  name="results-10min")
    # sender.add_periodic_task(3600.0,  expire_subscriptions.s(),  name="expire-subs-1hr")
 
    # ── Market alignment every 15 min ─────────────────────────────────────────
    sender.add_periodic_task(900.0, _align_all.s(), name="align-all-15min")
 
    if not LIVE_ENABLED:
        return

@celery.task(name="ops.align_all_shim", soft_time_limit=30, time_limit=45)
def _align_all():
    celery.send_task("tasks.align.all", queue="results")
    return {"ok": True}

# =============================================================================
# WS PUBLISH
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
# EV / ARBITRAGE
# =============================================================================

def _extract_best_price(val) -> float | None:
    if isinstance(val, (int, float)):
        fv = float(val)
        return fv if fv > 1.0 else None

    if not isinstance(val, dict):
        return None

    for key in ("odds", "odd", "price", "value"):
        inner = val.get(key)
        if isinstance(inner, (int, float)):
            fv = float(inner)
            if fv > 1.0:
                return fv

    best: float | None = None
    for v in val.values():
        p = _extract_best_price(v)
        if p is not None and (best is None or p > best):
            best = p
    return best


def _sanitise_markets(raw: dict) -> dict:
    clean: dict = {}
    for mkt, selections in raw.items():
        if isinstance(selections, (int, float)):
            fv = float(selections)
            if fv > 1.0:
                clean[mkt] = {"value": {"odds": fv, "odd": fv, "price": fv}}
            continue

        if not isinstance(selections, dict):
            continue

        clean_sels: dict = {}
        for sel, val in selections.items():
            price = _extract_best_price(val)
            if price is not None and price > 1.0:
                clean_sels[sel] = {"odds": price, "odd": price, "price": price}

        if clean_sels:
            clean[mkt] = clean_sels

    return clean


@celery.task(name="tasks.ops.compute_ev_arb", bind=True,
             max_retries=3, soft_time_limit=210, time_limit=300, acks_late=True)
def compute_ev_arb(self, match_id: int) -> dict:
    try:
        from app.models.odds_model import UnifiedMatch, ArbitrageOpportunity, EVOpportunity
        from app.extensions import db

        um = UnifiedMatch.query.get(match_id)
        if not um or not um.markets_json:
            return {"ok": False, "reason": "no markets"}

        raw   = um.markets_json if isinstance(um.markets_json, dict) else {}
        clean = _sanitise_markets(raw)

        if not clean:
            return {"ok": False, "reason": "markets_json empty after sanitise"}

        original_markets = um.markets_json
        um.markets_json  = clean

        try:
            detector = OpportunityDetector(min_profit_pct=0.5, min_ev_pct=3.0)
            arbs     = detector.find_arbs(um)
            evs      = detector.find_ev(um)
        finally:
            um.markets_json = original_markets

        for kwargs in arbs:
            db.session.add(ArbitrageOpportunity(**kwargs))
        for kwargs in evs:
            db.session.add(EVOpportunity(**kwargs))

        db.session.commit()

        if arbs:
            _publish(ARB_CHANNEL, {
                "event":   "arb_updated", "match_id": match_id,
                "match":   f"{um.home_team_name} v {um.away_team_name}",
                "sport":   um.sport_name, "arbs": len(arbs), "ts": _now_iso(),
            })
        if evs:
            _publish(EV_CHANNEL, {
                "event": "ev_updated", "match_id": match_id,
                "evs": len(evs), "ts": _now_iso(),
            })

        return {"ok": True, "match_id": match_id, "arbs": len(arbs), "evs": len(evs)}

    except Exception as exc:
        logger.error("[ev_arb] match %d: %s", match_id, exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        raise self.retry(exc=exc, countdown=random.uniform(2.0, 8.0))


# =============================================================================
# MATCH RESULTS
# =============================================================================

@celery.task(name="tasks.ops.update_match_results", bind=True, max_retries=3,
             soft_time_limit=1200, time_limit=1500)
def update_match_results(self) -> dict:
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
            _publish(WS_CHANNEL, {
                "event": "results_updated", "updated": updated, "ts": _now_iso(),
            })

        return {"updated": updated}

    except Exception as exc:
        logger.error("[results] %s", exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        raise self.retry(exc=exc, countdown=random.uniform(5.0, 15.0))


def _settle_bankroll_bets(match_id: int) -> None:
    try:
        from app.models.bank_roll import BankrollBet
        for bet in BankrollBet.query.filter_by(match_id=match_id, status="pending").all():
            bet.status = "manual_check"
    except Exception:
        pass


# =============================================================================
# CACHE FINISHED GAMES
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
# NOTIFICATIONS
# =============================================================================

@celery.task(name="tasks.ops.dispatch_notifications", bind=True,
             max_retries=2, soft_time_limit=300, time_limit=450, acks_late=True)
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
        arbs = ArbitrageOpportunity.query.filter_by(match_id=match_id, status="OPEN").all()
        evs  = EVOpportunity.query.filter_by(match_id=match_id, status="OPEN").all()
        if not arbs and not evs:
            return {"ok": True, "sent": 0}
        prefs = NotificationPref.query.join(Customer).filter(
            Customer.is_active == True  # noqa
        ).all()
        sent = 0
        for pref in prefs:
            if not pref.email_enabled:
                continue
            qa = [a for a in arbs if (a.profit_pct or 0) >= (pref.arb_min_profit or 0)]
            qe = [e for e in evs  if (e.ev_pct     or 0) >= (pref.ev_min_edge    or 0)]
            if not qa and not qe:
                continue
            try:
                NotificationService.send_alert(
                    user=pref.user, match_label=match_label,
                    arbs=qa, evs=qe, event_type=event_type,
                )
                sent += 1
            except Exception as e:
                logger.warning("[notify] user %s: %s", pref.user.id, e)
        return {"ok": True, "sent": sent}
    except Exception as exc:
        raise self.retry(exc=exc)


# =============================================================================
# SUBSCRIPTION EXPIRY
# =============================================================================

@celery.task(name="tasks.ops.expire_subscriptions", bind=True, max_retries=2,
             soft_time_limit=300, time_limit=450)
def expire_subscriptions(self) -> dict:
    try:
        from app.extensions import db
        from app.models.subscriptions import Subscription, SubscriptionStatus
        now     = datetime.now(timezone.utc)
        expired = 0

        for sub in Subscription.query.filter(
            Subscription.status    == SubscriptionStatus.TRIAL.value,
            Subscription.is_trial  == True,        # noqa
            Subscription.trial_ends <= now,
        ).all():
            sub.status   = SubscriptionStatus.EXPIRED.value
            sub.is_trial = False
            expired     += 1

        for sub in Subscription.query.filter(
            Subscription.status     == SubscriptionStatus.ACTIVE.value,
            Subscription.auto_renew == True,        # noqa
            Subscription.period_end <= now,
        ).all():
            sub.status = SubscriptionStatus.EXPIRED.value
            expired   += 1

        if expired:
            db.session.commit()

        return {"processed": expired}

    except Exception as exc:
        logger.error("[subs:expire] %s", exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        raise self.retry(exc=exc, countdown=random.uniform(5.0, 10.0))


# =============================================================================
# HEALTH CHECK
# =============================================================================

@celery.task(name="tasks.ops.health_check", soft_time_limit=10, time_limit=15)
def health_check() -> dict:
    ts = _now_iso()
    cache_set("worker_heartbeat", {"alive": True, "checked_at": ts, "pid": os.getpid()}, ttl=120)
    return {"ok": True, "ts": ts}


# =============================================================================
# EMAIL + WHATSAPP
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
            if body_type == "html":
                msg.html = body
            else:
                msg.body = body
            for att in (attachments or []):
                content = att.get("content")
                if content:
                    try:
                        msg.attach(att.get("filename", "file"),
                                   att.get("mimetype", "application/octet-stream"),
                                   base64.b64decode(content))
                    except Exception as e:
                        logger.warning("[email] attachment: %s", e)
            mail.send(msg)
    except Exception as exc:
        raise self.retry(exc=exc)


@celery.task(name="tasks.ops.send_message", soft_time_limit=300, time_limit=450)
def send_message(msg: str, whatsapp_number: str):
    if not message_url:
        return {"error": "WA_BOT not configured"}
    r = requests.post(message_url, json={"message": msg, "number": whatsapp_number}, timeout=15)
    return r.text


# =============================================================================
# PERSIST UPCOMING SNAPSHOTS TO POSTGRESQL
# =============================================================================

@celery.task(name="tasks.ops.persist_combined_batch", bind=True,
             max_retries=5, default_retry_delay=5,
             soft_time_limit=900, time_limit=1200, acks_late=True)
def persist_combined_batch(
    self,
    match_dicts: list[dict],
    sport_slug:  str = "soccer",
    mode:        str = "upcoming",
) -> dict:
    if not match_dicts:
        return {"persisted": 0, "failed": 0, "sport": sport_slug, "mode": mode}

    t0 = time.perf_counter()
    try:
        from app.workers.persist_hook import persist_from_serialized
        stats = persist_from_serialized(match_dicts, sport_slug=sport_slug, mode=mode)
        stats.update({"sport": sport_slug, "mode": mode})

        logger.info("[persist] %s %s: persisted=%d failed=%d in %d ms",
                    sport_slug, mode, stats.get("persisted", 0), stats.get("failed", 0),
                    int((time.perf_counter() - t0) * 1000))

        log_job(bookmaker="combined", sport=sport_slug, mode=mode,
                count=len(match_dicts), status="ok",
                unified_ok=stats.get("persisted", 0), unified_fail=stats.get("failed", 0),
                latency_ms=int((time.perf_counter() - t0) * 1000))

        return stats

    except Exception as exc:
        log_job(bookmaker="combined", sport=sport_slug, mode=mode,
                count=len(match_dicts), status="error", detail=str(exc)[:200],
                latency_ms=int((time.perf_counter() - t0) * 1000))
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        raise self.retry(exc=exc, countdown=random.uniform(3.0, 10.0))


@celery.task(name="tasks.ops.persist_all_sports",
             soft_time_limit=600, time_limit=900, acks_late=True)
def persist_all_sports() -> dict:
    """Beat task — every 5 min. Reads per-bookmaker cache and re-dispatches persist."""
    dispatched = 0
    skipped    = 0
    summary: dict[str, dict] = {}
    modes = [("upcoming", 10)]

    if LIVE_ENABLED:
        modes.append(("live", 2))

    for sport in PERSIST_SPORTS:
        summary[sport] = {}
        for bk_slug in _BK_SLUGS:
            for mode, countdown in modes:
                # bt_od uses a different cache key pattern
                if bk_slug == "bt_od":
                    cached = cache_get(f"bt_od:upcoming:{sport}")
                else:
                    cached = cache_get(f"{bk_slug}:{mode}:{sport}")
                if not cached or not cached.get("matches"):
                    skipped += 1
                    continue
                match_dicts = cached["matches"]
                if not match_dicts:
                    skipped += 1
                    continue

                jittered_delay = countdown + random.uniform(0.0, 15.0)
                persist_combined_batch.apply_async(
                    args=[match_dicts, sport, mode],
                    queue="results",
                    countdown=jittered_delay,
                )
                summary[sport][f"{bk_slug}:{mode}"] = len(match_dicts)
                dispatched += 1

    logger.info("[persist_all_sports] dispatched=%d skipped=%d", dispatched, skipped)
    return {"dispatched": dispatched, "skipped": skipped, "summary": summary}


# =============================================================================
# HEALTH REPORT  (every 60 s)
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
        _redis().ping()
        redis_ok = True
    except Exception:
        pass

    sports_report: list[dict] = []
    total_upcoming = total_arbs = total_evs = 0
    cold_sports: list[str] = []

    for sport in PERSIST_SPORTS:
        u_matches: list[dict] = []
        bk_counts: dict[str, int] = {}
        harvested_at = None

        for bk_slug in _BK_SLUGS:
            # bt_od uses a different cache key pattern
            if bk_slug == "bt_od":
                bk_cache = cache_get(f"bt_od:upcoming:{sport}") or {}
            else:
                bk_cache = cache_get(f"{bk_slug}:upcoming:{sport}") or {}
            bk_m = bk_cache.get("matches", [])
            if bk_m:
                bk_counts[bk_slug] = len(bk_m)
                # For bt_od the matches have markets_by_bk — flatten for counting
                if bk_slug == "bt_od":
                    u_matches.extend(bk_m)
                else:
                    u_matches.extend(bk_m)
                if not harvested_at:
                    harvested_at = bk_cache.get("harvested_at")

        u_arbs    = sum(1 for m in u_matches if m.get("has_arb"))
        u_evs     = sum(1 for m in u_matches if m.get("has_ev"))
        populated = bool(u_matches)
        if not populated:
            cold_sports.append(sport)
        total_upcoming += len(u_matches)
        total_arbs     += u_arbs
        total_evs      += u_evs
        align_stats = cache_get(f"align:stats:{sport}") or {}

        # bt_od specific: count how many matched multi-bookie events exist
        bt_od_cache   = cache_get(f"bt_od:upcoming:{sport}") or {}
        bt_od_matched = bt_od_cache.get("match_count", 0)
        bt_od_bk_counts = bt_od_cache.get("bk_counts", {})

        sports_report.append({
            "sport":              sport,
            "upcoming_count":     len(u_matches),
            "live_count":         0,
            "upcoming_arbs":      u_arbs,
            "upcoming_evs":       u_evs,
            "harvested_at_up":    harvested_at,
            "bk_counts_up":       bk_counts,
            "populated":          populated,
            "status":             "ok" if populated else "cold",
            "align_last_run":     align_stats.get("ts"),
            "align_aligned":      align_stats.get("aligned", 0),
            "align_arbs":         align_stats.get("arbs_found", 0),
            # bt_od pipeline stats
            "bt_od_matched":      bt_od_matched,
            "bt_od_bk_counts":    bt_od_bk_counts,
        })

    recent_jobs: list[dict] = []
    try:
        from app.workers.celery_tasks import _redis
        for rj in _redis().lrange("monitor:job_log", 0, 19):
            try:
                recent_jobs.append(json.loads(rj))
            except Exception:
                pass
    except Exception:
        pass

    checks: list[dict] = []

    def _chk(name: str, passed: bool, detail: str = "") -> None:
        checks.append({"name": name, "passed": passed, "detail": detail})

    _chk("Worker heartbeat",  worker_alive,    f"age={hb_age}s")
    _chk("Redis reachable",   redis_ok)
    _chk("Sports cache warm", not cold_sports,
         f"{len(PERSIST_SPORTS) - len(cold_sports)}/{len(PERSIST_SPORTS)} warm"
         + (f" — cold: {', '.join(cold_sports)}" if cold_sports else ""))
    _chk("Upcoming matches",  total_upcoming > 0, f"{total_upcoming} total")
    _chk("Arb opportunities", total_arbs >= 0,    f"{total_arbs}")
    _chk("EV opportunities",  total_evs  >= 0,    f"{total_evs}")

    any_align    = cache_get("align:stats:soccer") or {}
    align_ts_raw = any_align.get("ts")
    align_recent = align_age_s = False
    if align_ts_raw:
        try:
            align_dt     = datetime.fromisoformat(align_ts_raw.replace("Z", "+00:00"))
            align_age_s  = int((datetime.now(timezone.utc) - align_dt).total_seconds())
            align_recent = align_age_s < 1200
        except Exception:
            pass
    _chk("Market alignment", align_recent,
         f"last run {align_age_s}s ago" if align_age_s else "never run")

    passed = sum(1 for c in checks if c["passed"])
    failed = sum(1 for c in checks if not c["passed"])

    report = {
        "generated_at": now,
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
        "live_enabled": LIVE_ENABLED,
        "overall":  {"passed": passed, "failed": failed,
                     "total_checks": len(checks), "healthy": failed == 0},
        "worker":   {"alive": worker_alive, "heartbeat_at": hb.get("checked_at"),
                     "heartbeat_age_s": hb_age, "pid": hb.get("pid")},
        "redis":    {"ok": redis_ok},
        "alignment": {"last_run": align_ts_raw, "age_seconds": align_age_s,
                      "is_recent": align_recent},
        "totals":   {"upcoming_matches": total_upcoming, "live_matches": 0,
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
            "upcoming": total_upcoming, "live": 0, "arbs": total_arbs, "evs": total_evs,
        }, default=str))
        _redis().ltrim("monitor:report_history", 0, 59)
    except Exception:
        pass

    logger.info("[health_report] healthy=%s passed=%d failed=%d upcoming=%d",
                failed == 0, passed, failed, total_upcoming)
    return report


# =============================================================================
# CLEANUP
# =============================================================================

@celery.task(name="harvest.cleanup", soft_time_limit=60, time_limit=90)
def cleanup_old_snapshots(days_keep: int = 30) -> dict:
    from app.extensions import db
    from app.models.odds_model import ArbitrageOpportunity, EVOpportunity
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_keep)
    n_a = ArbitrageOpportunity.query.filter(ArbitrageOpportunity.open_at < cutoff).delete()
    n_e = EVOpportunity.query.filter(EVOpportunity.open_at < cutoff).delete()
    db.session.commit()
    return {"ok": True, "arbs_deleted": n_a, "evs_deleted": n_e}