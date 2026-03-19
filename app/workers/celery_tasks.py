"""
app/workers/celery_tasks.py  — v3 (event-driven, max-throughput)
=================================================================
Architecture
────────────
Beat schedule via on_after_configure (no hardcoded beat_schedule dict).

Per-round pipeline:
  1. harvest_all_upcoming / harvest_all_live
       → celery.group( harvest_b2b_page × ALL bk × ALL sports × ALL pages )
         + celery.group( harvest_sbo_sport × ALL sports )            ← concurrent
  2. Each leaf task (harvest_b2b_page / harvest_sbo_sport) on SUCCESS:
       → compute_ev_arb_for_match  (linked via .link())
       → publish_ws_event          (Redis pubsub → SSE / WS clients)
  3. dispatch_notifications        (threshold-based email/push)
  4. update_match_results          (hourly, fetches live score API)
  5. cache_finished_games          (hourly)
  6. expire_subscriptions          (hourly)
  7. health_check                  (every 30 s)

Concurrency notes
─────────────────
• No artificial stagger — celery group dispatches all tasks at once.
• Worker prefetch=1, acks_late=True → no task is lost on crash.
• CPU-bound ev_arb runs on ev_arb queue (separate worker pool).
• Redis pubsub channel: "odds:updates"  (browser SSE listens here).
"""

from __future__ import annotations

import base64
import json
import mimetypes
import os
import time
from datetime import datetime, timezone, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from celery import group, chord, chain
from celery.signals import after_setup_logger
from celery.utils.log import get_task_logger
from flask_mail import Mail, Message
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from jinja2 import Environment, FileSystemLoader, select_autoescape

from app import create_app
from app.extensions import celery as celery_service, init_celery

logger = get_task_logger(__name__)

# ── Sport lists ───────────────────────────────────────────────────────────────
_B2B_SPORTS = [
    "Football", "Basketball", "Tennis", "Ice Hockey",
    "Volleyball", "Cricket", "Rugby", "Table Tennis",
]
_B2B_LIVE_SPORTS = ["Football", "Basketball", "Ice Hockey", "Tennis"]
_SBO_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "boxing",
    "handball", "mma", "table-tennis",
]
_SBO_LIVE_SPORTS = ["soccer", "basketball", "tennis"]

PAGE_SIZE   = 15
MAX_PAGES   = 6     # 6 × 15 = 90 matches per sport per bookmaker per round
WS_CHANNEL  = "odds:updates"
ARB_CHANNEL = "arb:updates"
EV_CHANNEL  = "ev:updates"

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]


# =============================================================================
# Bootstrap helpers
# =============================================================================

def make_celery(app):
    """Bind global Celery instance to Flask app and configure queues."""
    init_celery(app)
    celery_service.conf.update(
        task_acks_late             = True,
        worker_prefetch_multiplier = 1,
        task_reject_on_worker_lost = True,
        task_default_queue         = "default",
        worker_max_tasks_per_child = 1000,
        task_serializer            = "json",
        result_serializer          = "json",
        accept_content             = ["json"],
        task_routes = {
            "app.workers.celery_tasks.harvest_b2b_page":           {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_sbo_sport":          {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_upcoming":       {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_live":           {"queue": "live"},
            "app.workers.celery_tasks.harvest_all_sbo_upcoming":   {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_sbo_live":       {"queue": "live"},
            "app.workers.celery_tasks.compute_ev_arb_for_match":   {"queue": "ev_arb"},
            "app.workers.celery_tasks.update_match_results":       {"queue": "results"},
            "app.workers.celery_tasks.dispatch_notifications":     {"queue": "notify"},
            "app.workers.celery_tasks.publish_ws_event":           {"queue": "notify"},
            "app.workers.celery_tasks.health_check":               {"queue": "default"},
            "app.workers.celery_tasks.expire_subscriptions":       {"queue": "default"},
            "app.workers.celery_tasks.cache_finished_games":       {"queue": "results"},
            "app.workers.celery_tasks.send_async_email":           {"queue": "notify"},
            "app.workers.celery_tasks.send_message":               {"queue": "notify"},
        },
    )
    return celery_service


# Bootstrap at module load — safe whether imported via factory or directly
flask_app = create_app()
celery    = make_celery(flask_app)


# ── Beat schedule via signal (replaces hardcoded beat_schedule dict) ──────────
@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    from celery.schedules import crontab

    # B2B upcoming — every 5 min
    sender.add_periodic_task(300.0,  harvest_all_upcoming.s("upcoming"), name="b2b-upcoming-5min")
    # B2B live — every 30 s
    sender.add_periodic_task(30.0,   harvest_all_live.s(),               name="b2b-live-30s")
    # SBO upcoming — every 3 min
    sender.add_periodic_task(180.0,  harvest_all_sbo_upcoming.s(),       name="sbo-upcoming-3min")
    # SBO live — every 60 s
    sender.add_periodic_task(60.0,   harvest_all_sbo_live.s(),           name="sbo-live-60s")
    # Match results — every 5 min (marks in-play / finished)
    sender.add_periodic_task(300.0,  update_match_results.s(),           name="results-5min")
    # Full score cache — hourly
    sender.add_periodic_task(3600.0, cache_finished_games.s(),           name="cache-finished-hourly")
    # Health — every 30 s
    sender.add_periodic_task(30.0,   health_check.s(),                   name="health-30s")
    # Subscription expiry — hourly
    sender.add_periodic_task(3600.0, expire_subscriptions.s(),           name="expire-subs-hourly")

    logger.info("[beat] Periodic tasks registered via on_after_configure")


# =============================================================================
# Redis helpers
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


def cache_delete(key: str) -> bool:
    try:
        _redis().delete(key)
        return True
    except Exception as e:
        logger.warning(f"[cache:del] {key}: {e}")
        return False


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
# Redis PubSub — real-time push to browser via SSE
# =============================================================================

@celery.task(
    name="app.workers.celery_tasks.publish_ws_event",
    soft_time_limit=5,
    time_limit=10,
)
def publish_ws_event(channel: str, data: dict) -> bool:
    """
    Publish a JSON message to a Redis pubsub channel.
    Flask SSE endpoint /stream/odds subscribes and forwards to browser.
    """
    try:
        _redis().publish(channel, json.dumps(data, default=str))
        return True
    except Exception as e:
        logger.warning(f"[ws:publish] {channel}: {e}")
        return False


def _publish(channel: str, data: dict):
    """Fire-and-forget pubsub publish (async Celery task)."""
    publish_ws_event.apply_async(args=[channel, data], queue="notify")


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
                try:    cfg = json.loads(cfg)
                except: cfg = {}
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
    try:
        from app.models.bookmakers_model import Bookmaker
        return {bm.name: bm.id for bm in Bookmaker.query.all()}
    except Exception:
        return {}


def _upsert_unified_match(match_data: dict, bookmaker_id: int, bookmaker_name: str) -> int | None:
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
            if home  and home  != um.home_team_name:  um.home_team_name   = home
            if away  and away  != um.away_team_name:  um.away_team_name   = away
            if sport and not um.sport_name:            um.sport_name       = sport
            if comp  and not um.competition_name:      um.competition_name = comp
            if start_time and not um.start_time:       um.start_time       = start_time

        bmo = BookmakerMatchOdds.query.filter_by(match_id=um.id, bookmaker_id=bookmaker_id).first()
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
                    market=mkt_key, specifier=None, selection=outcome, price=price)
                um.upsert_bookmaker_price(
                    market=mkt_key, specifier=None, selection=outcome,
                    price=price, bookmaker_id=bookmaker_id)

                if price_changed:
                    h = BookmakerOddsHistory(
                        bmo_id=bmo.id, bookmaker_id=bookmaker_id, match_id=um.id,
                        market=mkt_key, specifier=None, selection=outcome,
                        old_price=old_price, new_price=price,
                        price_delta=round(price - old_price, 4) if old_price else None,
                    )
                    db.session.add(h)

        db.session.commit()
        return um.id

    except Exception as exc:
        logger.error(f"[upsert] {match_data.get('match_id')}: {exc}")
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
                parent_match_id=parent_id, home_team_name=home,
                away_team_name=away, sport_name=sport,
                competition_name=comp, start_time=start_time,
            )
            db.session.add(um)
            db.session.flush()
        else:
            if home  and home  != um.home_team_name:  um.home_team_name   = home
            if away  and away  != um.away_team_name:  um.away_team_name   = away
            if sport and not um.sport_name:            um.sport_name       = sport
            if comp  and not um.competition_name:      um.competition_name = comp
            if start_time and not um.start_time:       um.start_time       = start_time

        unified = sbo_match.get("unified_markets") or {}
        for mkt_key, outcome_map in unified.items():
            for outcome, entries in outcome_map.items():
                for entry in entries:
                    bookie = entry.get("bookie", "")
                    price  = float(entry.get("odd", 0))
                    bk_id  = bk_name_to_id.get(bookie)
                    if not bk_id or price <= 1.0:
                        continue

                    bmo = BookmakerMatchOdds.query.filter_by(
                        match_id=um.id, bookmaker_id=bk_id).first()
                    if not bmo:
                        bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bk_id)
                        db.session.add(bmo)
                        db.session.flush()

                    price_changed, old_price = bmo.upsert_selection(
                        market=mkt_key, specifier=None, selection=outcome, price=price)
                    um.upsert_bookmaker_price(
                        market=mkt_key, specifier=None, selection=outcome,
                        price=price, bookmaker_id=bk_id)
                    if price_changed:
                        db.session.add(BookmakerOddsHistory(
                            bmo_id=bmo.id, bookmaker_id=bk_id, match_id=um.id,
                            market=mkt_key, specifier=None, selection=outcome,
                            old_price=old_price, new_price=price,
                            price_delta=round(price - old_price, 4) if old_price else None,
                        ))

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
# B2B Harvest — parallel group across all bk × sport × page
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
    """
    Fetch one page from one B2B bookmaker for one sport.
    On success: upserts to DB + cache, then chains ev_arb + WS publish.
    """
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

    # ── Redis cache ───────────────────────────────────────────────────────────
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
    }, ttl=60 if mode == "live" else 360)

    # ── DB persistence + chain ev_arb ────────────────────────────────────────
    match_ids: list[int] = []
    for m in matches:
        mid = _upsert_unified_match(m, bk_id, bk_name)
        if mid:
            match_ids.append(mid)
            # Chain: ev_arb → notify → ws publish (independent per match)
            chain(
                compute_ev_arb_for_match.si(mid),
                dispatch_notifications.si(mid, "arb"),
            ).apply_async(queue="ev_arb", countdown=1)

    # ── WS push: sport updated ────────────────────────────────────────────────
    _publish(WS_CHANNEL, {
        "event":      "odds_updated",
        "source":     "b2b",
        "bookmaker":  bk_name,
        "sport":      sport,
        "mode":       mode,
        "page":       page,
        "count":      len(matches),
        "match_ids":  match_ids,
        "ts":         datetime.now(timezone.utc).isoformat(),
    })

    logger.info(f"[b2b] {bk_name}/{sport}/p{page}/{mode}: {len(matches)} matches, {latency}ms")
    return {"ok": True, "count": len(matches), "latency_ms": latency, "db_ids": match_ids}


@celery.task(
    name="app.workers.celery_tasks.harvest_all_upcoming",
    soft_time_limit=30,
    time_limit=60,
)
def harvest_all_upcoming(mode: str = "upcoming") -> dict:
    """
    Fan-out using celery.group — ALL bookmakers × ALL sports × ALL pages
    dispatched in ONE round with no artificial stagger.
    """
    bookmakers = _load_bookmakers()
    if not bookmakers:
        task_status_set("beat_upcoming", {"state": "error", "error": "No bookmakers"})
        return {"dispatched": 0}

    task_signatures = [
        harvest_b2b_page.s(bm, sport, mode, page)
        for bm in bookmakers
        for sport in _B2B_SPORTS
        for page in range(1, MAX_PAGES + 1)
    ]

    # Dispatch the whole group at once — Celery distributes across workers
    job = group(task_signatures)
    job.apply_async(queue="harvest")

    dispatched = len(task_signatures)
    logger.info(f"[beat:upcoming] group dispatched {dispatched} tasks "
                f"({len(bookmakers)} bk × {len(_B2B_SPORTS)} sports × {MAX_PAGES} pages)")
    task_status_set("beat_upcoming", {
        "state": "ok", "dispatched": dispatched,
        "bookmakers": len(bookmakers), "sports": len(_B2B_SPORTS),
    })
    _publish(WS_CHANNEL, {"event": "harvest_started", "mode": mode, "tasks": dispatched})
    return {"dispatched": dispatched}


@celery.task(
    name="app.workers.celery_tasks.harvest_all_live",
    soft_time_limit=30,
    time_limit=60,
)
def harvest_all_live() -> dict:
    """Fan-out live B2B harvest — fires every 30 s, page 1 only."""
    bookmakers = _load_bookmakers()
    task_signatures = [
        harvest_b2b_page.s(bm, sport, "live", 1)
        for bm in bookmakers
        for sport in _B2B_LIVE_SPORTS
    ]
    group(task_signatures).apply_async(queue="live")
    dispatched = len(task_signatures)
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
def harvest_sbo_sport(self, sport_slug: str, max_matches: int = 90) -> dict:
    """Fetch one sport from Sportpesa + Betika + Odibets, persist, chain ev_arb."""
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
        "sport":       sport_slug,
        "match_count": len(matches),
        "harvested_at": datetime.now(timezone.utc).isoformat(),
        "latency_ms":  latency,
        "matches":     matches,
    }, ttl=180)

    bk_name_to_id = _bookmaker_id_map()
    match_ids: list[int] = []
    for m in matches:
        mid = _upsert_sbo_match(m, bk_name_to_id)
        if mid:
            match_ids.append(mid)
            chain(
                compute_ev_arb_for_match.si(mid),
                dispatch_notifications.si(mid, "arb"),
            ).apply_async(queue="ev_arb", countdown=2)

    arb_count = sum(1 for m in matches if m.get("arbitrage"))

    _publish(WS_CHANNEL, {
        "event":     "odds_updated",
        "source":    "sbo",
        "sport":     sport_slug,
        "count":     len(matches),
        "arb_count": arb_count,
        "ts":        datetime.now(timezone.utc).isoformat(),
    })

    if arb_count:
        _publish(ARB_CHANNEL, {
            "event":     "arb_found",
            "sport":     sport_slug,
            "arb_count": arb_count,
            "ts":        datetime.now(timezone.utc).isoformat(),
        })

    logger.info(f"[sbo] {sport_slug}: {len(matches)} matches, {arb_count} arb, {latency}ms")
    return {"ok": True, "count": len(matches), "arb_count": arb_count, "latency_ms": latency}


@celery.task(
    name="app.workers.celery_tasks.harvest_all_sbo_upcoming",
    soft_time_limit=30,
    time_limit=60,
)
def harvest_all_sbo_upcoming() -> dict:
    """Fan-out SBO upcoming — all sports in one group."""
    task_sigs = [harvest_sbo_sport.s(sport, 90) for sport in _SBO_SPORTS]
    group(task_sigs).apply_async(queue="harvest")
    logger.info(f"[beat:sbo-upcoming] group of {len(task_sigs)} sports")
    return {"dispatched": len(task_sigs)}


@celery.task(
    name="app.workers.celery_tasks.harvest_all_sbo_live",
    soft_time_limit=30,
    time_limit=60,
)
def harvest_all_sbo_live() -> dict:
    """Fan-out SBO live — fewer matches."""
    task_sigs = [harvest_sbo_sport.s(sport, 30) for sport in _SBO_LIVE_SPORTS]
    group(task_sigs).apply_async(queue="live")
    return {"dispatched": len(task_sigs)}


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
    """Load markets, compute EV + Arb, persist, publish WS event."""
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

        has_arb = stats.get("arb_new", 0) > 0
        has_ev  = stats.get("ev", 0) > 0

        # WS publish for arb / ev
        if has_arb:
            _publish(ARB_CHANNEL, {
                "event":    "arb_updated",
                "match_id": match_id,
                "match":    f"{um.home_team_name} v {um.away_team_name}",
                "sport":    um.sport_name,
                "stats":    stats,
                "ts":       datetime.now(timezone.utc).isoformat(),
            })
        if has_ev:
            _publish(EV_CHANNEL, {
                "event":    "ev_updated",
                "match_id": match_id,
                "stats":    stats,
                "ts":       datetime.now(timezone.utc).isoformat(),
            })

        return {"ok": True, "match_id": match_id, **stats}

    except Exception as exc:
        logger.error(f"[ev_arb] match {match_id}: {exc}")
        raise self.retry(exc=exc)


# =============================================================================
# Match results — every 5 min
# =============================================================================

@celery.task(
    name="app.workers.celery_tasks.update_match_results",
    soft_time_limit=120,
    time_limit=150,
)
def update_match_results() -> dict:
    """
    Update match statuses.  For in-play matches tries to fetch live scores
    from the bookmaker API and caches them.
    """
    try:
        from app.extensions import db
        from app.models.odds_model import UnifiedMatch

        now     = datetime.now(timezone.utc)
        updated = 0

        # PRE_MATCH → IN_PLAY
        candidates = UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now,
            UnifiedMatch.start_time >= now - timedelta(hours=3),
            UnifiedMatch.status.in_(["PRE_MATCH", "IN_PLAY"]),
        ).all()
        for um in candidates:
            if um.status == "PRE_MATCH":
                um.status = "IN_PLAY"
                updated  += 1

        # IN_PLAY → FINISHED (after ~2.5 h)
        finished_candidates = UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now - timedelta(hours=2, minutes=30),
            UnifiedMatch.status == "IN_PLAY",
        ).all()
        for um in finished_candidates:
            um.status = "FINISHED"
            updated  += 1
            _settle_bankroll_bets(um.id)
            # Cache per-day result
            date_str  = um.start_time.strftime("%Y-%m-%d") if um.start_time else now.strftime("%Y-%m-%d")
            cache_key = f"results:finished:{date_str}"
            cached    = cache_get(cache_key) or []
            cached.append(um.to_dict())
            cache_set(cache_key, cached, ttl=30 * 86400)
            # Collapse stale arbs
            try:
                from app.workers.ev_arb_service import EVArbPersistenceService
                EVArbPersistenceService.collapse_stale_arbs(now)
            except Exception:
                pass

        if updated:
            db.session.commit()
            _publish(WS_CHANNEL, {
                "event":   "results_updated",
                "updated": updated,
                "ts":      now.isoformat(),
            })

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
# Cache finished games — hourly
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

        logger.info(f"[cache] finished games: {cached} days cached")
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
        from app.models.subscription_models import NotificationPref, SubscriptionTier
        from app.models.customer import Customer
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
            if user.tier not in eligible_tiers or not pref.email_enabled:
                continue

            qa = [a for a in arbs if a.max_profit_percentage >= pref.arb_min_profit]
            qe = [e for e in evs  if e.edge_pct >= pref.ev_min_edge]
            if not qa and not qe:
                continue

            if pref.sports_filter and um.sport_name:
                if um.sport_name.lower() not in [s.lower() for s in pref.sports_filter]:
                    continue

            try:
                NotificationService.send_alert(
                    user=user, match_label=match_label,
                    arbs=qa, evs=qe, event_type=event_type)
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
                # Bill notification
                _notify_billing(sub.user_id, "trial_expired",
                                "Your free trial has expired. Upgrade to continue.")
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
                _notify_billing(sub.user_id, "subscription_expired",
                                "Your subscription has expired. Top up to reactivate.")
            expired += 1

        if expired:
            db.session.commit()

        logger.info(f"[subs] processed {expired} expirations")
        return {"processed": expired}

    except Exception as exc:
        logger.error(f"[subs:expire] {exc}")
        return {"error": str(exc)}


def _notify_billing(user_id: int, event_type: str, message: str) -> None:
    """Create an in-app billing notification and optionally send email."""
    try:
        from app.extensions import db
        from app.models.notifications import Notification
        from app.models.customer import Customer

        n = Notification(
            user_id    = user_id,
            type       = "billing",
            event_type = event_type,
            title      = "Billing Alert",
            message    = message,
        )
        db.session.add(n)
        db.session.flush()

        user = Customer.query.get(user_id)
        if user and user.email:
            send_async_email.apply_async(args=[
                f"OddsKenya — {message[:60]}",
                [user.email],
                f"<p>{message}</p>",
                "html",
            ], queue="notify")

    except Exception as e:
        logger.warning(f"[billing:notify] user {user_id}: {e}")


def _attempt_charge(sub) -> bool:
    """Stub — integrate with M-Pesa/Stripe here."""
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
    cache_set("worker_heartbeat", {"alive": True, "checked_at": ts, "pid": os.getpid()}, ttl=120)
    return {"ok": True, "ts": ts}


# =============================================================================
# On-demand probe (admin)
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
        return {"ok": True, "count": len(matches), "latency_ms": latency,
                "matches": matches[:10], "bookmaker": bookmaker.get("name")}
    except Exception as exc:
        return {"ok": False, "error": str(exc),
                "latency_ms": int((time.perf_counter() - t0) * 1000)}


# =============================================================================
# WhatsApp + Email senders
# =============================================================================

whatsapp_bot = os.environ.get("WA_BOT", "")
message_url  = f"{whatsapp_bot}/api/v1/send-message" if whatsapp_bot else ""


@celery.task(
    name="app.workers.celery_tasks.send_message",
    soft_time_limit=30,
    time_limit=45,
)
def send_message(msg: str, whatsapp_number: str):
    if not message_url:
        return {"error": "WA_BOT not configured"}
    r = requests.post(message_url, json={"message": msg, "number": whatsapp_number}, timeout=15)
    return r.text


@celery_service.task(
    name="app.workers.celery_tasks.send_async_email",
    bind=True,
    max_retries=3,
    default_retry_delay=30,
    soft_time_limit=60,
    time_limit=90,
)
def send_async_email(
    self,
    subject,
    recipients,
    body,
    body_type="plain",
    attachments=None,
    username=None,   # backwards-compat — ignored
    password=None,   # backwards-compat — ignored
):
    """Send email via Flask-Mail (SMTP configured in env)."""
    try:
        app = create_app()
        with app.app_context():
            mail = Mail(app)
            msg  = Message(
                subject    = subject,
                recipients = recipients,
                sender     = os.environ.get("ADMIN_EMAIL"),
            )
            if body_type == "html":
                msg.html = body
            else:
                msg.body = body

            if attachments:
                for att in attachments:
                    fname    = att.get("filename", "file")
                    mimetype = att.get("mimetype", "application/octet-stream")
                    content  = att.get("content")
                    if content:
                        try:
                            msg.attach(fname, mimetype, base64.b64decode(content))
                        except Exception as e:
                            logger.warning(f"[email] attachment decode: {e}")

            mail.send(msg)
            logger.info(f"[email] sent → {recipients}")

    except Exception as exc:
        logger.error(f"[email] failed: {exc}")
        raise self.retry(exc=exc)


# helper used by auth_routes
def create_message_with_attachment(sender, to, subject,
                                   html_message=None, text_message=None, files=None):
    message = MIMEMultipart()
    message["to"]      = to
    message["from"]    = sender
    message["subject"] = subject

    body = MIMEText(html_message or text_message or "", "html" if html_message else "plain")
    message.attach(body)

    for f in (files or []):
        resp = requests.get(f["url"], stream=True, timeout=15)
        if resp.status_code != 200:
            continue
        ct, enc = mimetypes.guess_type(f["url"])
        if not ct or enc:
            ct = "application/octet-stream"
        _, sub = ct.split("/", 1)
        att = MIMEApplication(resp.content, _subtype=sub)
        att.add_header("Content-Disposition", "attachment", filename=f["name"])
        message.attach(att)

    return {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode()}