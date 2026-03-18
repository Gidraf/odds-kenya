from __future__ import annotations

import base64
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
# ✅ FIX 1: Removed `from mailbox import Message` — it shadowed flask_mail.Message
import mimetypes
import os
import time
from datetime import datetime, timezone, timedelta

from flask_mail import Mail
from jinja2 import Environment, FileSystemLoader, select_autoescape
from app import create_app
import requests

from app.extensions import celery, init_celery
from celery.utils.log import get_task_logger
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import arrow

logger = get_task_logger(__name__)

_B2B_SPORTS = [
    "Football", "Basketball", "Tennis", "Ice Hockey",
    "Volleyball", "Cricket", "Rugby", "Table Tennis",
]
_SBO_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "boxing",
    "handball", "mma", "table-tennis",
]
PAGE_SIZE = 15


def _bootstrap():
    """
    Auto-initialise when loaded as the direct -A target:
        celery -A app.workers.celery_tasks worker ...

    Guard: uses a custom _flask_initialized flag set by init_celery().
    Safe to call multiple times (idempotent).
    """
    if getattr(celery, '_flask_initialized', False):
        return
    try:
        flask_app = create_app()
        make_celery(flask_app)
        logger.info("[celery_tasks] self-bootstrapped via create_app()")
    except Exception as exc:
        logger.error(f"[celery_tasks] bootstrap failed: {exc}")
        raise


def make_celery(app):
    """
    Bind the global Celery instance to the Flask app.
    Calls extensions.init_celery() (sets broker/backend + ContextTask),
    then layers on harvest-specific task_routes + beat_schedule.
    Returns the same global celery object — NOT a new instance.
    """
    init_celery(app)
    celery.conf.update(
        task_acks_late             = True,
        worker_prefetch_multiplier = 1,
        task_reject_on_worker_lost = True,
        task_default_queue         = "default",
        worker_max_tasks_per_child = 500,
        task_routes = {
            "app.workers.celery_tasks.harvest_b2b_page":         {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_sbo_sport":        {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_upcoming":     {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_live":         {"queue": "live"},
            "app.workers.celery_tasks.compute_ev_arb_for_match": {"queue": "ev_arb"},
            "app.workers.celery_tasks.update_match_results":     {"queue": "results"},
            "app.workers.celery_tasks.dispatch_notifications":   {"queue": "notify"},
            "app.workers.celery_tasks.health_check":             {"queue": "default"},
            "app.workers.celery_tasks.expire_subscriptions":     {"queue": "default"},
            "app.workers.celery_tasks.cache_finished_games":     {"queue": "results"},
        },
        beat_schedule = {
            "b2b-upcoming-5min":     {"task": "app.workers.celery_tasks.harvest_all_upcoming",     "schedule": 300, "args": ["upcoming"]},
            "b2b-live-60s":          {"task": "app.workers.celery_tasks.harvest_all_live",         "schedule": 60},
            "sbo-upcoming-3min":     {"task": "app.workers.celery_tasks.harvest_all_sbo_upcoming", "schedule": 180},
            "sbo-live-90s":          {"task": "app.workers.celery_tasks.harvest_all_sbo_live",     "schedule": 90},
            "results-5min":          {"task": "app.workers.celery_tasks.update_match_results",     "schedule": 300},
            "cache-finished-hourly": {"task": "app.workers.celery_tasks.cache_finished_games",     "schedule": 3600},
            "health-30s":            {"task": "app.workers.celery_tasks.health_check",             "schedule": 30},
            "expire-subs-hourly":    {"task": "app.workers.celery_tasks.expire_subscriptions",     "schedule": 3600},
        },
    )
    return celery


# =============================================================================
# Redis cache helpers
# =============================================================================

def _redis():
    import redis as _r
    url  = celery.conf.broker_url or "redis://localhost:6379/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    return _r.Redis.from_url(
        f"{base}/2",
        decode_responses       = False,
        socket_timeout         = 5,
        socket_connect_timeout = 5,
        retry_on_timeout       = True,
    )


def cache_set(key: str, data, ttl: int = 600) -> bool:
    try:
        _redis().set(key, json.dumps(data, default=str), ex=ttl)
        return True
    except Exception as e:
        logger.warning(f"[cache:set] {key}: {e}")
        return False


def cache_get(key: str):
    try:
        raw = _redis().get(key)
        return json.loads(raw) if raw else None
    except Exception as e:
        logger.warning(f"[cache:get] {key}: {e}")
        return None


def cache_keys(pattern: str) -> list[str]:
    try:
        return [k.decode() if isinstance(k, bytes) else k
                for k in _redis().keys(pattern)]
    except Exception as e:
        logger.warning(f"[cache:keys] {pattern}: {e}")
        return []


def task_status_set(name: str, status: dict):
    cache_set(f"task_status:{name}", {
        **status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }, ttl=300)


# =============================================================================
# DB helpers
# =============================================================================

def _load_bookmakers() -> list[dict]:
    try:
        from app.models.bookmakers_model import Bookmaker
        bms = Bookmaker.query.filter_by(is_active=True).all()
        result = []
        for bm in bms:
            cfg = getattr(bm, "harvest_config", None) or {}
            if isinstance(cfg, str):
                try: cfg = json.loads(cfg)
                except Exception: cfg = {}
            result.append({
                "id":          bm.id,
                "name":        bm.name or bm.domain,
                "domain":      bm.domain,
                "vendor_slug": getattr(bm, "vendor_slug", "betb2b") or "betb2b",
                "config":      cfg,
            })
        return result
    except Exception as exc:
        logger.error(f"[db] load_bookmakers: {exc}")
        return []


def _bookmaker_id_map() -> dict[str, int]:
    """Map bookmaker name → DB id for EV/Arb leg links."""
    try:
        from app.models.bookmakers_model import Bookmaker
        bms = Bookmaker.query.all()
        return {bm.name: bm.id for bm in bms}
    except Exception:
        return {}


def _upsert_unified_match(match_data: dict, bookmaker_id: int,
                           bookmaker_name: str) -> int | None:
    try:
        from app.extensions import db
        from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory

        parent_id = str(match_data.get("match_id") or match_data.get("betradar_id") or "")
        if not parent_id:
            return None

        home  = str(match_data.get("home_team")   or "")
        away  = str(match_data.get("away_team")   or "")
        sport = str(match_data.get("sport")       or "")
        comp  = str(match_data.get("competition") or "")

        start_time = None
        if st := match_data.get("start_time"):
            try:
                if isinstance(st, str):
                    start_time = datetime.fromisoformat(st.replace("Z", "+00:00"))
                elif isinstance(st, (int, float)):
                    start_time = datetime.utcfromtimestamp(st)
            except Exception:
                pass

        um = UnifiedMatch.query.filter_by(parent_match_id=parent_id).first()
        if not um:
            um = UnifiedMatch(
                parent_match_id  = parent_id,
                home_team_name   = home,
                away_team_name   = away,
                sport_name       = sport,
                competition_name = comp,
                start_time       = start_time,
            )
            db.session.add(um)
            db.session.flush()
        else:
            if home and home != um.home_team_name:  um.home_team_name   = home
            if away and away != um.away_team_name:  um.away_team_name   = away
            if sport and not um.sport_name:         um.sport_name       = sport
            if comp and not um.competition_name:    um.competition_name = comp
            if start_time and not um.start_time:    um.start_time       = start_time

        bmo = BookmakerMatchOdds.query.filter_by(
            match_id=um.id, bookmaker_id=bookmaker_id
        ).first()
        if not bmo:
            bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bookmaker_id)
            db.session.add(bmo)
            db.session.flush()

        markets = match_data.get("markets") or {}
        for mkt_key, outcomes in markets.items():
            for outcome, odds_data in outcomes.items():
                if isinstance(odds_data, (int, float)):
                    price = float(odds_data)
                elif isinstance(odds_data, dict):
                    price = float(odds_data.get("odds") or odds_data.get("odd") or 0)
                else:
                    continue
                if price <= 1.0:
                    continue

                price_changed, old_price = bmo.upsert_selection(
                    market=mkt_key, specifier=None, selection=outcome, price=price,
                )
                um.upsert_bookmaker_price(
                    market=mkt_key, specifier=None, selection=outcome,
                    price=price, bookmaker_id=bookmaker_id,
                )
                if price_changed:
                    h = BookmakerOddsHistory(
                        bmo_id       = bmo.id,
                        bookmaker_id = bookmaker_id,
                        match_id     = um.id,
                        market       = mkt_key,
                        specifier    = None,
                        selection    = outcome,
                        old_price    = old_price,
                        new_price    = price,
                        price_delta  = round(price - old_price, 4) if old_price else None,
                    )
                    db.session.add(h)

        db.session.commit()
        return um.id

    except Exception as exc:
        logger.error(f"[upsert] parent_id={match_data.get('match_id')} error: {exc}")
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        return None


def _upsert_sbo_match(sbo_match: dict, bk_name_to_id: dict[str, int]) -> int | None:
    try:
        from app.extensions import db
        from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory

        parent_id = str(sbo_match.get("betradar_id") or "")
        if not parent_id:
            return None

        home  = str(sbo_match.get("home_team")   or "")
        away  = str(sbo_match.get("away_team")   or "")
        sport = str(sbo_match.get("sport")       or "")
        comp  = str(sbo_match.get("competition") or "")

        start_time = None
        if st := sbo_match.get("start_time"):
            try:
                if isinstance(st, str):
                    start_time = datetime.fromisoformat(st.replace("Z", "+00:00"))
                elif isinstance(st, (int, float)):
                    start_time = datetime.utcfromtimestamp(float(st))
            except Exception:
                pass

        um = UnifiedMatch.query.filter_by(parent_match_id=parent_id).first()
        if not um:
            um = UnifiedMatch(
                parent_match_id  = parent_id,
                home_team_name   = home,
                away_team_name   = away,
                sport_name       = sport,
                competition_name = comp,
                start_time       = start_time,
            )
            db.session.add(um)
            db.session.flush()
        else:
            if home  and home  != um.home_team_name:   um.home_team_name   = home
            if away  and away  != um.away_team_name:   um.away_team_name   = away
            if sport and not um.sport_name:            um.sport_name       = sport
            if comp  and not um.competition_name:      um.competition_name = comp
            if start_time and not um.start_time:       um.start_time       = start_time

        unified = sbo_match.get("unified_markets") or {}
        for mkt_key, outcome_map in unified.items():
            for outcome, entries in outcome_map.items():
                for entry in entries:
                    bookie   = entry.get("bookie", "")
                    price    = float(entry.get("odd", 0))
                    bk_id    = bk_name_to_id.get(bookie)
                    if not bk_id or price <= 1.0:
                        continue

                    bmo = BookmakerMatchOdds.query.filter_by(
                        match_id=um.id, bookmaker_id=bk_id
                    ).first()
                    if not bmo:
                        bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bk_id)
                        db.session.add(bmo)
                        db.session.flush()

                    price_changed, old_price = bmo.upsert_selection(
                        market=mkt_key, specifier=None, selection=outcome, price=price,
                    )
                    um.upsert_bookmaker_price(
                        market=mkt_key, specifier=None, selection=outcome,
                        price=price, bookmaker_id=bk_id,
                    )
                    if price_changed:
                        h = BookmakerOddsHistory(
                            bmo_id=bmo.id, bookmaker_id=bk_id, match_id=um.id,
                            market=mkt_key, specifier=None, selection=outcome,
                            old_price=old_price, new_price=price,
                            price_delta=round(price - old_price, 4) if old_price else None,
                        )
                        db.session.add(h)

        db.session.commit()
        return um.id

    except Exception as exc:
        logger.error(f"[upsert_sbo] {sbo_match.get('betradar_id')}: {exc}")
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        return None


# =============================================================================
# B2B Harvest — paginated per bookmaker × sport
# =============================================================================

@celery.task(
    bind=True,
    name="app.workers.celery_tasks.harvest_b2b_page",
    max_retries=2,
    default_retry_delay=15,
    soft_time_limit=45,
    time_limit=60,
    acks_late=True,
)
def harvest_b2b_page(self, bookmaker: dict, sport: str, mode: str, page: int) -> dict:
    t0      = time.perf_counter()
    bk_name = bookmaker.get("name") or bookmaker.get("domain", "?")
    bk_id   = bookmaker.get("id")

    try:
        from app.views.odds_feed.bookmaker_fetcher import fetch_bookmaker
        matches = fetch_bookmaker(
            bookmaker, sport_name=sport, mode=mode,
            page=page, page_size=PAGE_SIZE, timeout=20,
        )
    except Exception as exc:
        logger.warning(f"[b2b] {bk_name}/{sport}/p{page}: {exc}")
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)

    cache_key = f"odds:{mode}:{sport.lower().replace(' ','_')}:{bk_id}:p{page}"
    cache_set(cache_key, {
        "bookmaker_id":   bk_id,
        "bookmaker_name": bk_name,
        "sport":          sport,
        "mode":           mode,
        "page":           page,
        "match_count":    len(matches),
        "harvested_at":   datetime.now(timezone.utc).isoformat(),
        "latency_ms":     latency,
        "matches":        matches,
    }, ttl=90 if mode == "live" else 360)

    match_ids: list[int] = []
    for m in matches:
        mid = _upsert_unified_match(m, bk_id, bk_name)
        if mid:
            match_ids.append(mid)

    for match_id in match_ids:
        compute_ev_arb_for_match.apply_async(
            args=[match_id], queue="ev_arb", countdown=1,
        )

    logger.info(f"[b2b] {bk_name}/{sport}/p{page}/{mode}: {len(matches)} matches, {latency}ms")
    return {"ok": True, "count": len(matches), "latency_ms": latency, "db_ids": match_ids}


# =============================================================================
# B2B Beat tasks
# =============================================================================

@celery.task(
    name="app.workers.celery_tasks.harvest_all_upcoming",
    soft_time_limit=120,
    time_limit=150,
)
def harvest_all_upcoming(mode: str = "upcoming") -> dict:
    bookmakers = _load_bookmakers()
    if not bookmakers:
        task_status_set("beat_upcoming", {"state": "error", "error": "No bookmakers", "dispatched": 0})
        return {"dispatched": 0}

    MAX_PAGES  = 4
    dispatched = 0
    for bm in bookmakers:
        for sport in _B2B_SPORTS:
            for page in range(1, MAX_PAGES + 1):
                harvest_b2b_page.apply_async(
                    args=[bm, sport, mode, page],
                    queue="harvest",
                    countdown=dispatched * 0.15,
                )
                dispatched += 1

    logger.info(f"[beat:upcoming] {dispatched} tasks")
    task_status_set("beat_upcoming", {
        "state": "ok", "dispatched": dispatched,
        "bookmakers": len(bookmakers), "sports": len(_B2B_SPORTS),
    })
    return {"dispatched": dispatched}


@celery.task(
    name="app.workers.celery_tasks.harvest_all_live",
    soft_time_limit=90,
    time_limit=120,
)
def harvest_all_live() -> dict:
    bookmakers  = _load_bookmakers()
    live_sports = ["Football", "Basketball", "Ice Hockey", "Tennis"]

    dispatched = 0
    for bm in bookmakers:
        for sport in live_sports:
            harvest_b2b_page.apply_async(
                args=[bm, sport, "live", 1], queue="live",
            )
            dispatched += 1

    task_status_set("beat_live", {"state": "ok", "dispatched": dispatched})
    return {"dispatched": dispatched}


# =============================================================================
# SBO Harvest
# =============================================================================

@celery.task(
    bind=True,
    name="app.workers.celery_tasks.harvest_sbo_sport",
    max_retries=1,
    default_retry_delay=30,
    soft_time_limit=120,
    time_limit=150,
    acks_late=True,
)
def harvest_sbo_sport(self, sport_slug: str, max_matches: int = 60) -> dict:
    t0 = time.perf_counter()
    try:
        from app.views.sbo.sbo_fetcher import OddsAggregator, SPORT_CONFIG
        cfg = next((c for c in SPORT_CONFIG if c["sport"] == sport_slug), None)
        if not cfg:
            return {"ok": False, "error": f"Unknown sport: {sport_slug}"}

        agg     = OddsAggregator(cfg, fetch_full_sp_markets=True,
                                 fetch_full_bt_markets=True, fetch_od_markets=True)
        matches = agg.run(max_matches=max_matches)
    except Exception as exc:
        logger.error(f"[sbo] {sport_slug}: {exc}")
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)

    cache_set(f"sbo:upcoming:{sport_slug}", {
        "sport":        sport_slug,
        "match_count":  len(matches),
        "harvested_at": datetime.now(timezone.utc).isoformat(),
        "latency_ms":   latency,
        "matches":      matches,
    }, ttl=180)

    bk_name_to_id = _bookmaker_id_map()
    match_ids: list[int] = []
    for m in matches:
        mid = _upsert_sbo_match(m, bk_name_to_id)
        if mid:
            match_ids.append(mid)

    for match_id in match_ids:
        compute_ev_arb_for_match.apply_async(
            args=[match_id], queue="ev_arb", countdown=2,
        )

    arb_count = sum(1 for m in matches if m.get("arbitrage"))
    logger.info(f"[sbo] {sport_slug}: {len(matches)} matches, {arb_count} arb, {latency}ms")
    return {"ok": True, "count": len(matches), "arb_count": arb_count, "latency_ms": latency}


@celery.task(
    name="app.workers.celery_tasks.harvest_all_sbo_upcoming",
    soft_time_limit=60,
    time_limit=90,
)
def harvest_all_sbo_upcoming() -> dict:
    dispatched = 0
    for i, sport in enumerate(_SBO_SPORTS):
        harvest_sbo_sport.apply_async(
            args=[sport, 60], queue="harvest", countdown=i * 3,
        )
        dispatched += 1
    logger.info(f"[beat:sbo-upcoming] {dispatched} sport tasks")
    return {"dispatched": dispatched}


@celery.task(
    name="app.workers.celery_tasks.harvest_all_sbo_live",
    soft_time_limit=60,
    time_limit=90,
)
def harvest_all_sbo_live() -> dict:
    live_sports = ["soccer", "basketball", "tennis"]
    dispatched  = 0
    for sport in live_sports:
        harvest_sbo_sport.apply_async(args=[sport, 30], queue="live")
        dispatched += 1
    return {"dispatched": dispatched}


# =============================================================================
# EV / Arbitrage Compute
# =============================================================================

@celery.task(
    bind=True,
    name="app.workers.celery_tasks.compute_ev_arb_for_match",
    max_retries=1,
    soft_time_limit=20,
    time_limit=30,
    acks_late=True,
)
def compute_ev_arb_for_match(self, match_id: int) -> dict:
    try:
        from app.models.odds_model import UnifiedMatch
        from app.workers.ev_arb_service import EVArbPersistenceService

        um = UnifiedMatch.query.get(match_id)
        if not um or not um.markets_json:
            return {"ok": False, "reason": "no markets"}

        unified: dict = {}
        bk_id_to_name = {v: k for k, v in _bookmaker_id_map().items()}

        for mkt, spec_map in (um.markets_json or {}).items():
            unified.setdefault(mkt, {})
            for spec, sel_map in spec_map.items():
                for sel, sel_data in sel_map.items():
                    if not sel_data.get("is_active", True):
                        continue
                    unified[mkt].setdefault(sel, [])
                    for bk_id_str, price in (sel_data.get("bookmakers") or {}).items():
                        bk_name = bk_id_to_name.get(int(bk_id_str), f"BK{bk_id_str}")
                        if price > 1.0:
                            unified[mkt][sel].append({"bookie": bk_name, "odd": float(price)})

        bk_id_map = _bookmaker_id_map()
        stats = EVArbPersistenceService.process_match(match_id, unified, bk_id_map)

        if stats.get("arb_new", 0) > 0 or stats.get("ev", 0) > 0:
            dispatch_notifications.apply_async(
                args=[match_id, "arb" if stats.get("arb_new") else "ev"],
                queue="notify",
                countdown=5,
            )

        return {"ok": True, "match_id": match_id, **stats}

    except Exception as exc:
        logger.error(f"[ev_arb] match {match_id}: {exc}")
        raise self.retry(exc=exc)


# =============================================================================
# Match result + metadata updates
# =============================================================================

@celery.task(
    name="app.workers.celery_tasks.update_match_results",
    soft_time_limit=90,
    time_limit=120,
)
def update_match_results() -> dict:
    try:
        from app.extensions import db
        from app.models.odds_model import UnifiedMatch

        now     = datetime.now(timezone.utc)
        updated = 0

        candidates = UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now,
            UnifiedMatch.start_time >= now - timedelta(hours=3),
            UnifiedMatch.status.in_(["PRE_MATCH", "IN_PLAY"]),
        ).all()

        for um in candidates:
            if um.status == "PRE_MATCH":
                um.status = "IN_PLAY"
                updated  += 1

        finished = UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now - timedelta(hours=2, minutes=30),
            UnifiedMatch.status == "IN_PLAY",
        ).all()

        for um in finished:
            um.status = "FINISHED"
            updated  += 1
            from app.workers.ev_arb_service import EVArbPersistenceService
            EVArbPersistenceService.collapse_stale_arbs(now)
            _settle_bankroll_bets(um.id)

        if updated:
            db.session.commit()

        logger.info(f"[results] updated {updated} match statuses")
        return {"updated": updated}

    except Exception as exc:
        logger.error(f"[results] {exc}")
        return {"error": str(exc)}


def _settle_bankroll_bets(match_id: int) -> None:
    try:
        from app.models.subscription_models import BankrollBet
        pending = BankrollBet.query.filter_by(match_id=match_id, status="pending").all()
        for bet in pending:
            bet.status = "manual_check"
    except Exception:
        pass


# =============================================================================
# Cache finished games
# =============================================================================

@celery.task(
    name="app.workers.celery_tasks.cache_finished_games",
    soft_time_limit=120,
    time_limit=150,
)
def cache_finished_games() -> dict:
    try:
        from app.models.odds_model import UnifiedMatch

        now    = datetime.now(timezone.utc)
        cached = 0

        for day_offset in range(0, 30):
            day_start = (now - timedelta(days=day_offset)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            day_end  = day_start + timedelta(days=1)
            date_str  = day_start.strftime("%Y-%m-%d")
            cache_key = f"results:finished:{date_str}"

            if cache_get(cache_key):
                continue

            matches = UnifiedMatch.query.filter(
                UnifiedMatch.status == "FINISHED",
                UnifiedMatch.start_time >= day_start,
                UnifiedMatch.start_time <  day_end,
            ).all()

            if matches:
                data = [m.to_dict() for m in matches]
                cache_set(cache_key, data, ttl=30 * 86400)
                cached += 1

        logger.info(f"[cache] finished games: {cached} new days cached")
        return {"cached_days": cached}

    except Exception as exc:
        logger.error(f"[cache:finished] {exc}")
        return {"error": str(exc)}


# =============================================================================
# Notifications dispatch
# =============================================================================

@celery.task(
    bind=True,
    name="app.workers.celery_tasks.dispatch_notifications",
    max_retries=2,
    soft_time_limit=30,
    time_limit=45,
    acks_late=True,
)
def dispatch_notifications(self, match_id: int, event_type: str) -> dict:
    try:
        from app.models.odds_model import UnifiedMatch, ArbitrageOpportunity, EVOpportunity
        # ✅ FIX 2: Import Customer (not User) — align with your actual model name
        from app.models.customer import Customer
        from app.models.subscription_models import NotificationPref, SubscriptionTier
        from app.workers.notification_service import NotificationService

        um = UnifiedMatch.query.get(match_id)
        if not um:
            return {"ok": False}

        match_label = f"{um.home_team_name} v {um.away_team_name}"

        arbs = ArbitrageOpportunity.query.filter_by(match_id=match_id, is_active=True).all()
        evs  = EVOpportunity.query.filter_by(match_id=match_id, is_active=True).all()

        if not arbs and not evs:
            return {"ok": True, "sent": 0}

        eligible_tiers = [SubscriptionTier.PRO.value, SubscriptionTier.PREMIUM.value]
        prefs = (
            NotificationPref.query
            .join(Customer, Customer.id == NotificationPref.user_id)
            .filter(Customer.is_active == True)
            .all()
        )

        sent = 0
        for pref in prefs:
            user = pref.user
            if user.tier not in eligible_tiers:
                continue
            if not pref.email_enabled:
                continue

            qualifying_arbs = [a for a in arbs if a.max_profit_percentage >= pref.arb_min_profit]
            qualifying_evs  = [e for e in evs  if e.edge_pct >= pref.ev_min_edge]

            if not qualifying_arbs and not qualifying_evs:
                continue

            if pref.sports_filter and um.sport_name:
                if um.sport_name.lower() not in [s.lower() for s in pref.sports_filter]:
                    continue

            try:
                NotificationService.send_alert(
                    user        = user,
                    match_label = match_label,
                    arbs        = qualifying_arbs,
                    evs         = qualifying_evs,
                    event_type  = event_type,
                )
                sent += 1
            except Exception as e:
                logger.warning(f"[notify] user {user.id}: {e}")

        logger.info(f"[notify] match {match_id}: {sent} alerts sent")
        return {"ok": True, "sent": sent}

    except Exception as exc:
        logger.error(f"[notify] match {match_id}: {exc}")
        raise self.retry(exc=exc)


# =============================================================================
# Subscription expiry
# =============================================================================

@celery.task(
    name="app.workers.celery_tasks.expire_subscriptions",
    soft_time_limit=30,
    time_limit=45,
)
def expire_subscriptions() -> dict:
    try:
        from app.extensions import db
        from app.models.subscription_models import Subscription, SubscriptionStatus

        now     = datetime.now(timezone.utc)
        expired = 0

        trials = Subscription.query.filter(
            Subscription.status   == SubscriptionStatus.TRIAL.value,
            Subscription.is_trial == True,
            Subscription.trial_ends <= now,
        ).all()
        for sub in trials:
            charged = _attempt_charge(sub)
            if charged:
                sub.activate()
            else:
                sub.status   = SubscriptionStatus.EXPIRED.value
                sub.is_trial = False
                sub._append_history("trial_expired_no_payment")
            expired += 1

        actives = Subscription.query.filter(
            Subscription.status     == SubscriptionStatus.ACTIVE.value,
            Subscription.auto_renew == True,
            Subscription.period_end <= now,
        ).all()
        for sub in actives:
            charged = _attempt_charge(sub)
            if charged:
                sub.activate()
            else:
                sub.status = SubscriptionStatus.EXPIRED.value
                sub._append_history("period_expired_no_payment")
            expired += 1

        if expired:
            db.session.commit()

        logger.info(f"[subs] processed {expired} subscription expirations")
        return {"processed": expired}

    except Exception as exc:
        logger.error(f"[subs:expire] {exc}")
        return {"error": str(exc)}


def _attempt_charge(sub) -> bool:
    """Stub — integrate with M-Pesa / Stripe here."""
    return False


# =============================================================================
# Health check
# =============================================================================

@celery.task(
    name="app.workers.celery_tasks.health_check",
    soft_time_limit=10,
    time_limit=15,
)
def health_check() -> dict:
    ts = datetime.now(timezone.utc).isoformat()
    cache_set("worker_heartbeat", {
        "alive": True, "checked_at": ts, "pid": os.getpid(),
    }, ttl=120)
    return {"ok": True, "ts": ts}


# =============================================================================
# On-demand probe (admin UI — synchronous)
# =============================================================================

@celery.task(
    name="app.workers.celery_tasks.probe_bookmaker_now",
    soft_time_limit=30,
    time_limit=45,
)
def probe_bookmaker_now(bookmaker: dict, sport: str, mode: str = "live") -> dict:
    from app.views.odds_feed.bookmaker_fetcher import fetch_bookmaker
    t0 = time.perf_counter()
    try:
        matches = fetch_bookmaker(bookmaker, sport_name=sport, mode=mode, timeout=20)
        latency = int((time.perf_counter() - t0) * 1000)
        return {
            "ok": True, "count": len(matches), "latency_ms": latency,
            "matches": matches[:10], "bookmaker": bookmaker.get("name"),
        }
    except Exception as exc:
        latency = int((time.perf_counter() - t0) * 1000)
        return {"ok": False, "error": str(exc), "latency_ms": latency}


# =============================================================================
# WhatsApp message task
# =============================================================================

whatsapp_bot = os.environ.get("WA_BOT", "")
message_url  = f"{whatsapp_bot}/api/v1/send-message" if whatsapp_bot else ""

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
]


@celery.task(
    name="app.workers.celery_tasks.send_message",
    soft_time_limit=30,
    time_limit=45,
)
def send_message(msg: str, whatsapp_number: str) -> str:
    if not message_url:
        logger.error("[send_message] WA_BOT env var not set")
        return "error: WA_BOT not configured"
    try:
        r = requests.post(
            message_url,
            json={"message": msg, "number": whatsapp_number},
            timeout=20,
        )
        r.raise_for_status()
        return r.text
    except Exception as exc:
        logger.error(f"[send_message] {exc}")
        return f"error: {exc}"


# =============================================================================
# ✅ FIX 3: send_async_email — removed shadowed mailbox.Message import,
#    now imports flask_mail.Message locally inside the task.
# =============================================================================

@celery.task(
    name="app.workers.celery_tasks.send_async_email",
    soft_time_limit=30,
    time_limit=45,
)
def send_async_email(
    subject: str,
    recipients: list[str],
    body: str,
    body_type: str = "plain",
    attachments: list | None = None,
    username: str | None = None,
    password: str | None = None,
) -> str:
    """
    Send an email asynchronously using Celery and Flask-Mail.
    Supports base64-encoded attachments with varying MIME types.
    """
    # ✅ Import flask_mail.Message HERE — not at module level — to avoid
    #    collision with the old `from mailbox import Message` at the top.
    from flask_mail import Message as MailMessage

    try:
        app = create_app()
        with app.app_context():
            # Override MAIL_USERNAME / MAIL_PASSWORD if caller supplies credentials
            if username:
                app.config["MAIL_USERNAME"] = username
            if password:
                app.config["MAIL_PASSWORD"] = password

            mail = Mail(app)

            msg = MailMessage(
                subject    = subject,
                sender     = username or app.config.get("MAIL_USERNAME"),
                recipients = recipients,
            )

            if body_type == "html":
                msg.html = body
            else:
                msg.body = body

            if attachments:
                for attachment in attachments:
                    filename    = attachment.get("filename", "file")
                    mimetype    = attachment.get("mimetype", "application/octet-stream")
                    content_b64 = attachment.get("content")

                    if not content_b64:
                        logger.warning(f"[send_async_email] skipping {filename}: no content")
                        continue

                    try:
                        file_data = base64.b64decode(content_b64)
                        msg.attach(filename, mimetype, file_data)
                    except Exception as decode_err:
                        logger.error(f"[send_async_email] decode failed for {filename}: {decode_err}")

            mail.send(msg)
            logger.info(f"[send_async_email] sent to {recipients}")
            return "ok"

    except Exception as exc:
        logger.error(f"[send_async_email] failed: {exc}")
        # Re-raise so Celery marks the task as FAILED (visible in Flower)
        raise


# =============================================================================
# Gmail OAuth email (partner integrations)
# =============================================================================

def _send_gmail_api_message(service, user_id: str, message: dict):
    """Send via Gmail API and return the sent message object."""
    try:
        sent = (
            service.users().messages()
            .send(userId=user_id, body=message)
            .execute()
        )
        logger.info(f"[gmail] Message Id: {sent['id']}")
        return sent
    except Exception as exc:
        logger.error(f"[gmail] send error: {exc}")
        raise


def _build_raw_message(sender, to, subject, html_message=None, text_message=None, files=None) -> dict:
    """
    Build a Gmail API raw message with optional HTML/text body and URL attachments.
    Raises ValueError if neither body type is provided.
    """
    if not html_message and not text_message:
        raise ValueError("Either html_message or text_message must be provided.")

    message = MIMEMultipart()
    message["to"]      = to
    message["from"]    = sender
    message["subject"] = subject

    msg_body = MIMEText(html_message, "html") if html_message else MIMEText(text_message, "plain")
    message.attach(msg_body)

    if files:
        for file in files:
            try:
                response = requests.get(file["url"], stream=True, timeout=30)
                if response.status_code != 200:
                    logger.warning(f"[gmail] skipping attachment {file['name']}: HTTP {response.status_code}")
                    continue

                content_type, encoding = mimetypes.guess_type(file["url"])
                if content_type is None or encoding is not None:
                    content_type = "application/octet-stream"

                main_type, sub_type = content_type.split("/", 1)
                attachment = MIMEApplication(response.content, _subtype=sub_type)
                attachment.add_header(
                    "Content-Disposition", "attachment", filename=file["name"]
                )
                message.attach(attachment)
            except Exception as exc:
                logger.warning(f"[gmail] attachment error {file.get('name')}: {exc}")

    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {"raw": raw}


@celery.task(
    name="app.workers.celery_tasks.send_email",
    soft_time_limit=30,
    time_limit=45,
)
def send_email(
    to: str,
    subject: str,
    html_message: str | None = None,
    text_message: str | None = None,
    files: list | None = None,
    partner_id: str | None = None,
    business_name: str | None = None,
    partner_email: str | None = None,
) -> str:
    """
    Send email via Gmail OAuth (partner integrations).

    ✅ FIX 4: token_raw was referenced before assignment (the DB query was
       commented out).  Now raises a clear error if not implemented, and
       falls back to send_async_email for credential-expiry notifications.
    """
    app = create_app()
    with app.app_context():

        # Build the credential-expiry fallback email body once
        env = Environment(
            loader     = FileSystemLoader("app/templates"),
            autoescape = select_autoescape(["html"]),
        )
        try:
            expiry_template = env.get_template("gmail-token-expiry.html")
            expiry_body = expiry_template.render(
                customer_name = business_name,
                web_url       = os.environ.get("ADMIN_WEB_URL"),
            )
        except Exception:
            expiry_body = "<p>Your Gmail credentials have expired. Please reconnect.</p>"

        def _notify_expiry():
            send_async_email.apply_async(args=[
                "Gmail Credentials Has Expired",
                [partner_email] if partner_email else [],
                expiry_body,
                "html",
                [],
                os.environ.get("ADMIN_EMAIL"),
                os.environ.get("ADMIN_EMAIL_PASSWORD"),
            ])

        # ── Fetch partner Gmail integration from DB ───────────────────────
        try:
            from app.models.integrations import Integrations  # adjust import path as needed
            token_raw = Integrations.query.filter_by(
                partner_id=partner_id, name="gmail"
            ).first()
        except Exception as exc:
            logger.error(f"[send_email] DB lookup failed: {exc}")
            return f"error: DB lookup failed: {exc}"

        if not token_raw:
            logger.error(f"[send_email] No Gmail credentials for partner_id={partner_id}")
            _notify_expiry()
            return f"error: No credentials found for partner_id: {partner_id}"

        # ── Parse + refresh OAuth credentials ────────────────────────────
        try:
            token_data = json.loads(token_raw.credentials)
            creds      = Credentials.from_authorized_user_info(token_data, SCOPES)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    # Persist refreshed token
                    from app.extensions import db
                    token_raw.credentials = creds.to_json()
                    db.session.commit()
                else:
                    logger.error(f"[send_email] Credentials invalid for partner_id={partner_id}")
                    _notify_expiry()
                    return "error: credentials invalid or expired"

        except Exception as exc:
            logger.error(f"[send_email] credential refresh failed: {exc}")
            _notify_expiry()
            return f"error: {exc}"

        # ── Build + send ──────────────────────────────────────────────────
        try:
            service = build("gmail", "v1", credentials=creds)
            sender  = token_raw.gmail
            raw_msg = _build_raw_message(
                sender       = sender,
                to           = to,
                subject      = subject,
                html_message = html_message,
                text_message = text_message,
                files        = files,
            )
            _send_gmail_api_message(service, "me", raw_msg)
            return "ok"

        except Exception as exc:
            logger.error(f"[send_email] send failed: {exc}")
            _notify_expiry()
            return f"error: {exc}"


# ── Auto-bootstrap when used as -A app.workers.celery_tasks ──────────────────
_bootstrap()