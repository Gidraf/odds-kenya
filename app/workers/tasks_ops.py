"""
app/workers/tasks_ops.py
========================
Operational tasks: EV/arb, results, notifications, health, subscriptions,
persist pipeline, minute health reports, job logging.

LIVE_ENABLED = False
  • No live tasks in beat schedule.
  • Startup dispatches SP per-sport tasks only.
    BT and OD are handled automatically by sp_cross_bk_enrich which SP
    dispatches 10 s after each harvest completes — no separate BT/OD
    startup tasks needed.

Market Alignment Service
  • align_all_sports runs every 15 min (beat) and is also triggered
    automatically 30–60 s after each sp_cross_bk_enrich completes.
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

from app.models.oppotunity_detector import OpportunityDetector
from app.workers.celery_tasks import (
    celery, cache_set, cache_get, _now_iso, _publish,
)

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

_BK_SLUGS: list[str] = ["sp", "bt", "od", "b2b", "sbo"]

# Sports dispatched per bookmaker at startup
_SP_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
]
# B2B / SBO run independently, still scheduled
_B2B_SPORTS_STARTUP = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
    "darts", "handball",
]
_SBO_SPORTS_STARTUP = [
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
# SP only — BT and OD are handled via sp_cross_bk_enrich (auto-dispatched
# 10 s after each SP harvest).
# =============================================================================

@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    logger.info("[startup] Worker ready — dispatching SP harvests (BT/OD via cross-BK enrich)")
    try:
        _dispatch_startup_harvests()
    except Exception as exc:
        logger.error("[startup] Dispatch failed: %s", exc)


def _dispatch_startup_harvests() -> None:
    """
    Dispatch individual SP per-sport harvest tasks on worker boot.

    SP → dispatches sp_cross_bk_enrich (BT+OD by betradar_id, 10 s countdown)
        → dispatches sp_enrich_analytics (SR stats, 60 s countdown)

    B2B and SBO still run independently (different data sources, no betradar_id link).
    BT and OD are NOT dispatched here — they are covered by sp_cross_bk_enrich.
    """
    dispatched = 0

    # ── SportPesa — individual sport tasks, staggered ─────────────────────────
    from app.workers.tasks_upcoming import SP_MAX_MATCHES
    for i, sport in enumerate(_SP_SPORTS):
        celery.send_task(
            "tasks.sp.harvest_sport",
            args=[sport, SP_MAX_MATCHES],
            queue="harvest",
            countdown=5 + i * 5,   # 5, 10, 15 … 40 s
        )
        dispatched += 1

    # ── B2B — fast API, can start sooner ─────────────────────────────────────
    for i, sport in enumerate(_B2B_SPORTS_STARTUP):
        celery.send_task(
            "tasks.b2b.harvest_sport",
            args=[sport],
            queue="harvest",
            countdown=10 + i * 3,
        )
        dispatched += 1

    # ── SBO — fast API ────────────────────────────────────────────────────────
    for i, sport in enumerate(_SBO_SPORTS_STARTUP):
        celery.send_task(
            "tasks.sbo.harvest_sport",
            args=[sport, 90],
            queue="harvest",
            countdown=15 + i * 3,
        )
        dispatched += 1

    # ── Ops ───────────────────────────────────────────────────────────────────
    celery.send_task("tasks.ops.health_check",        queue="default",  countdown=1)
    celery.send_task("tasks.ops.build_health_report", queue="default",  countdown=30)

    # Initial alignment — 180 s lets b2b/sbo finish before we align
    celery.send_task("tasks.align.all", queue="results", countdown=180)

    logger.info(
        "[startup] %d tasks dispatched (SP=%d, B2B=%d, SBO=%d). "
        "BT+OD handled via sp_cross_bk_enrich auto-dispatch.",
        dispatched, len(_SP_SPORTS), len(_B2B_SPORTS_STARTUP), len(_SBO_SPORTS_STARTUP),
    )


# =============================================================================
# BEAT SCHEDULE
# BT and OD are NOT in the beat — they run via sp_cross_bk_enrich every
# time SP completes (every ~1 h per sport).
# =============================================================================
def setup_periodic_tasks(sender, **kw):
    """
    Celery beat schedule.
 
    Upcoming section   — unchanged, always active
    Live section       — gated by LIVE_ENABLED; now includes cross-BK refresh
                         and harvester watchdog.
    """
 
    # ── Upcoming (always on) ──────────────────────────────────────────────────
    from app.workers.tasks_upcoming import (
        sp_harvest_all_upcoming,
        b2b_harvest_all_upcoming,
        b2b_page_harvest_all_upcoming,
        sbo_harvest_all_upcoming,
    )
    from app.workers.tasks_ops import (
        update_match_results,
        expire_subscriptions,
        health_check,
        cleanup_old_snapshots,
    )
 
    # SP is source of truth — runs every 5 min
    sender.add_periodic_task(300.0,  sp_harvest_all_upcoming.s(),       name="sp-upcoming-5min")
    # B2B / SBO run every 3–5 min
    sender.add_periodic_task(180.0,  b2b_harvest_all_upcoming.s(),      name="b2b-upcoming-3min")
    sender.add_periodic_task(240.0,  b2b_page_harvest_all_upcoming.s(), name="b2b-page-upcoming-4min")
    sender.add_periodic_task(300.0,  sbo_harvest_all_upcoming.s(),      name="sbo-upcoming-5min")
 
    # Ops
    sender.add_periodic_task(600.0,  update_match_results.s(),          name="results-10min")
    sender.add_periodic_task(3600.0, expire_subscriptions.s(),          name="expire-subs-1hr")
    sender.add_periodic_task(60.0,   health_check.s(),                  name="health-1min")
    sender.add_periodic_task(86400.0, cleanup_old_snapshots.s(),        name="cleanup-daily")
 
    # ── Live (gated by LIVE_ENABLED) ──────────────────────────────────────────
    if not LIVE_ENABLED:
        return
 
    from app.workers.tasks_live import (
        sp_harvest_all_live,
        bt_harvest_all_live,
        od_harvest_all_live,
        b2b_harvest_all_live,
        b2b_page_harvest_all_live,
        sbo_harvest_all_live,
        sp_poll_all_event_details,
        live_cross_bk_refresh,
        ensure_harvester_running,
    )
 
    # SP WebSocket harvester watchdog — restarts thread if it crashes
    sender.add_periodic_task(  60.0, ensure_harvester_running.s(),     name="sp-harvester-watchdog-1min")
 
    # SP event-detail poll — fast (5 s) for score + clock + market prices
    sender.add_periodic_task(   5.0, sp_poll_all_event_details.s(),    name="sp-details-5s")
 
    # Cross-BK enrichment — BT+OD prices refreshed every 15 s via betradar_id
    sender.add_periodic_task(  15.0, live_cross_bk_refresh.s(),        name="live-cross-bk-15s")
 
    # Full live snapshots (slower, used for new match discovery)
    sender.add_periodic_task(  60.0, sp_harvest_all_live.s(),          name="sp-live-60s")
    sender.add_periodic_task(  90.0, bt_harvest_all_live.s(),          name="bt-live-90s")
    sender.add_periodic_task(  90.0, od_harvest_all_live.s(),          name="od-live-90s")
    sender.add_periodic_task( 120.0, b2b_harvest_all_live.s(),         name="b2b-live-2min")
    sender.add_periodic_task(  30.0, b2b_page_harvest_all_live.s(),    name="b2b-page-live-30s")
    sender.add_periodic_task(  60.0, sbo_harvest_all_live.s(),         name="sbo-live-60s")

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
    """
    Recursively extract the best (highest valid) price from any value shape.
 
    Handles:
      1.85                              → bare float
      {"odds": 1.85}                    → standard odds dict
      {"sp": 1.85, "bt": 1.90}         → bookmaker_slug → price map
      {"sp": {"odds": 1.85}, ...}      → bookmaker_slug → odds_dict map
      {"None": {"home": 1.85, ...}}    → specifier → outcomes map
    """
    if isinstance(val, (int, float)):
        fv = float(val)
        return fv if fv > 1.0 else None
 
    if not isinstance(val, dict):
        return None
 
    # Try well-known scalar price keys first (cheapest path)
    for key in ("odds", "odd", "price", "value"):
        logger.log(logging.DEBUG - 1, "Checking for price key '%s' in %s", key, val) 
        inner = val.get(key)
        if isinstance(inner, (int, float)):
            fv = float(inner)
            if fv > 1.0:
                return fv
 
    # Recurse into every value and take the maximum valid price
    best: float | None = None
    for v in val.values():
        p = _extract_best_price(v)
        if p is not None and (best is None or p > best):
            best = p
    return best
 
 
def _sanitise_markets(raw: dict) -> dict:
    """
    Normalise markets_json to the flat shape OpportunityDetector expects:
      {market_slug: {selection: {"odds": float, "odd": float, "price": float}}}
 
    Any market/selection that yields no valid price (> 1.0) is dropped.
    """
    clean: dict = {}
    for mkt, selections in raw.items():
 
        # Bare float at market level — single-outcome market, wrap it
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
 
 
# ── replacement task ──────────────────────────────────────────────────────────
 
# In tasks_ops.py, replace the entire compute_ev_arb function with this:
 
@celery.task(name="tasks.ops.compute_ev_arb", bind=True,
             max_retries=1, soft_time_limit=210, time_limit=300, acks_late=True)
def compute_ev_arb(self, match_id: int) -> dict:
    try:
        from app.models.odds_model import (
            UnifiedMatch,
            ArbitrageOpportunity, EVOpportunity,
        )
        from app.extensions import db
 
        um = UnifiedMatch.query.get(match_id)
        if not um or not um.markets_json:
            return {"ok": False, "reason": "no markets"}
 
        raw = um.markets_json if isinstance(um.markets_json, dict) else {}
        clean = _sanitise_markets(raw)
 
        if not clean:
            return {"ok": False, "reason": "markets_json empty after sanitise"}
 
        # Temporarily swap in the normalised dict so OpportunityDetector
        # never encounters a bare float via .get()
        original_markets = um.markets_json
        um.markets_json  = clean
 
        try:
            detector = OpportunityDetector(min_profit_pct=0.5, min_ev_pct=3.0)
            arbs     = detector.find_arbs(um)
            evs      = detector.find_ev(um)
        finally:
            # Always restore — even if the detector raises
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
        logger.error("[ev_arb] match %d: %s", match_id, exc, exc_info=True) # Added exc_info
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        raise self.retry(exc=exc)


# =============================================================================
# MATCH RESULTS
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

@celery.task(name="tasks.ops.expire_subscriptions",
             soft_time_limit=300, time_limit=450)
def expire_subscriptions() -> dict:
    try:
        from app.extensions import db
        from app.models.subscriptions import Subscription, SubscriptionStatus
        now     = datetime.now(timezone.utc)
        expired = 0
        for sub in Subscription.query.filter(
            Subscription.status    == SubscriptionStatus.TRIAL.value,
            Subscription.is_trial  == True,       # noqa
            Subscription.trial_ends <= now,
        ).all():
            sub.status   = SubscriptionStatus.EXPIRED.value
            sub.is_trial = False
            expired     += 1
        for sub in Subscription.query.filter(
            Subscription.status     == SubscriptionStatus.ACTIVE.value,
            Subscription.auto_renew == True,       # noqa
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
             max_retries=3, default_retry_delay=300,
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
        raise self.retry(exc=exc)


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
                cached = cache_get(f"{bk_slug}:{mode}:{sport}")
                if not cached or not cached.get("matches"):
                    skipped += 1
                    continue
                match_dicts = cached["matches"]
                if not match_dicts:
                    skipped += 1
                    continue
                persist_combined_batch.apply_async(
                    args=[match_dicts, sport, mode],
                    queue="results", countdown=countdown,
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
            bk_cache = cache_get(f"{bk_slug}:upcoming:{sport}") or {}
            bk_m     = bk_cache.get("matches", [])
            if bk_m:
                bk_counts[bk_slug] = len(bk_m)
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
        sports_report.append({
            "sport":           sport,
            "upcoming_count":  len(u_matches),
            "live_count":      0,
            "upcoming_arbs":   u_arbs,
            "upcoming_evs":    u_evs,
            "harvested_at_up": harvested_at,
            "bk_counts_up":    bk_counts,
            "populated":       populated,
            "status":          "ok" if populated else "cold",
            "align_last_run":  align_stats.get("ts"),
            "align_aligned":   align_stats.get("aligned", 0),
            "align_arbs":      align_stats.get("arbs_found", 0),
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

    any_align = cache_get("align:stats:soccer") or {}
    align_ts_raw = any_align.get("ts")
    align_recent = align_age_s = False
    if align_ts_raw:
        try:
            align_dt    = datetime.fromisoformat(align_ts_raw.replace("Z", "+00:00"))
            align_age_s = int((datetime.now(timezone.utc) - align_dt).total_seconds())
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