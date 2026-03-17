"""
app/workers/celery_tasks.py
============================
Production harvest engine — B2B (BetB2B family) + SBO (Sportpesa/Betika/Odibets).

Worker Architecture
--------------------
  20 concurrent workers × 15-record pages = 300 records/batch
  Beat fires every 3 s  → aims for ~6 000 refreshed records/minute

Queue topology
--------------
  harvest   — paginated upcoming market fetches (B2B + SBO)
  live      — live match updates every 60 s
  ev_arb    — EV/Arb calculation after each batch lands
  results   — match result / metadata updates
  notify    — user notification dispatch
  default   — health check + misc

Celery CLI (project root)
--------------------------
  # All queues, 20 concurrent workers
  celery -A app.celery_app worker --loglevel=info \
         -Q harvest,live,ev_arb,results,notify,default -c 20

  # Beat scheduler (separate process)
  celery -A app.celery_app beat --loglevel=info

  # Dev — single combined process (5 workers for dev)
  celery -A app.celery_app worker --beat --loglevel=info -c 5

Setup
------
  # app/celery_app.py
  from app import create_app
  from app.workers.celery_tasks import make_celery
  flask_app = create_app()
  celery    = make_celery(flask_app)
"""

from __future__ import annotations

import json
import os
import time
import traceback
from datetime import datetime, timezone, timedelta

from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

# ── Module-level instance — replaced by make_celery() ─────────────────────────

# ── Default sport lists ───────────────────────────────────────────────────────
_B2B_SPORTS = [
    "Football", "Basketball", "Tennis", "Ice Hockey",
    "Volleyball", "Cricket", "Rugby", "Table Tennis",
]
_SBO_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "boxing",
    "handball", "mma", "table-tennis",
]
# How many matches per page (15 × 20 workers = 300/cycle)
PAGE_SIZE = 15


# =============================================================================
# Factory
# =============================================================================

def make_celery(app) -> Celery:
    global celery

    broker  = app.config.get("CELERY_BROKER_URL",    "redis://localhost:6379/0")
    backend = app.config.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

    celery = Celery(app.import_name, broker=broker, backend=backend)
    celery.conf.update(
        task_serializer            = "json",
        result_serializer          = "json",
        accept_content             = ["json"],
        timezone                   = "UTC",
        enable_utc                 = True,
        task_acks_late             = True,
        worker_prefetch_multiplier = 1,       # fair queue for 20 workers
        task_reject_on_worker_lost = True,
        task_default_queue         = "default",
        worker_max_tasks_per_child = 500,     # recycle workers to avoid memory leaks

        task_routes = {
            "app.workers.celery_tasks.harvest_b2b_page":          {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_sbo_sport":         {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_upcoming":       {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_live":           {"queue": "live"},
            "app.workers.celery_tasks.compute_ev_arb_for_match":  {"queue": "ev_arb"},
            "app.workers.celery_tasks.update_match_results":       {"queue": "results"},
            "app.workers.celery_tasks.dispatch_notifications":     {"queue": "notify"},
            "app.workers.celery_tasks.health_check":               {"queue": "default"},
            "app.workers.celery_tasks.expire_subscriptions":       {"queue": "default"},
            "app.workers.celery_tasks.cache_finished_games":       {"queue": "results"},
        },

        beat_schedule = {
            # B2B upcoming — every 5 min
            "b2b-upcoming-5min": {
                "task":     "app.workers.celery_tasks.harvest_all_upcoming",
                "schedule": 300,
                "args":     ["upcoming"],
            },
            # B2B live — every 60 s
            "b2b-live-60s": {
                "task":     "app.workers.celery_tasks.harvest_all_live",
                "schedule": 60,
            },
            # SBO upcoming — every 3 min (slower API)
            "sbo-upcoming-3min": {
                "task":     "app.workers.celery_tasks.harvest_all_sbo_upcoming",
                "schedule": 180,
            },
            # SBO live — every 90 s
            "sbo-live-90s": {
                "task":     "app.workers.celery_tasks.harvest_all_sbo_live",
                "schedule": 90,
            },
            # Result updates every 5 min
            "results-5min": {
                "task":     "app.workers.celery_tasks.update_match_results",
                "schedule": 300,
            },
            # Cache finished games daily
            "cache-finished-daily": {
                "task":     "app.workers.celery_tasks.cache_finished_games",
                "schedule": 3600,
            },
            # Health heartbeat
            "health-30s": {
                "task":     "app.workers.celery_tasks.health_check",
                "schedule": 30,
            },
            # Subscription expiry check
            "expire-subs-hourly": {
                "task":     "app.workers.celery_tasks.expire_subscriptions",
                "schedule": 3600,
            },
        },
    )

    class ContextTask(celery.Task):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
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
    """
    Write one parsed match to unified_matches + bookmaker_match_odds.
    Returns the unified_match.id for downstream EV/Arb tasks.
    """
    try:
        from app.extensions import db
        from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory

        parent_id   = str(match_data.get("match_id") or match_data.get("betradar_id") or "")
        if not parent_id:
            return None

        home  = str(match_data.get("home_team")   or "")
        away  = str(match_data.get("away_team")   or "")
        sport = str(match_data.get("sport")       or "")
        comp  = str(match_data.get("competition") or "")

        # Parse start_time
        start_time = None
        if st := match_data.get("start_time"):
            try:
                if isinstance(st, str):
                    start_time = datetime.fromisoformat(st.replace("Z", "+00:00"))
                elif isinstance(st, (int, float)):
                    start_time = datetime.utcfromtimestamp(st)
            except Exception:
                pass

        # ── Find-or-create UnifiedMatch ────────────────────────────────────
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
            # Update metadata if changed
            if home and home != um.home_team_name:  um.home_team_name   = home
            if away and away != um.away_team_name:  um.away_team_name   = away
            if sport and not um.sport_name:         um.sport_name       = sport
            if comp and not um.competition_name:    um.competition_name = comp
            if start_time and not um.start_time:    um.start_time       = start_time

        # ── Find-or-create BookmakerMatchOdds ─────────────────────────────
        bmo = BookmakerMatchOdds.query.filter_by(
            match_id=um.id, bookmaker_id=bookmaker_id
        ).first()
        if not bmo:
            bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bookmaker_id)
            db.session.add(bmo)
            db.session.flush()

        # ── Write every market/outcome ─────────────────────────────────────
        markets = match_data.get("markets") or {}
        now_str = datetime.now(timezone.utc).isoformat()

        for mkt_key, outcomes in markets.items():
            for outcome, odds_data in outcomes.items():
                # odds_data can be float (raw fetcher) or {odds:float, bookmaker:str}
                if isinstance(odds_data, (int, float)):
                    price = float(odds_data)
                elif isinstance(odds_data, dict):
                    price = float(odds_data.get("odds") or odds_data.get("odd") or 0)
                else:
                    continue
                if price <= 1.0:
                    continue

                # Write to bookmaker_match_odds
                price_changed, old_price = bmo.upsert_selection(
                    market=mkt_key, specifier=None, selection=outcome, price=price,
                )

                # Write to unified_match aggregated view
                um.upsert_bookmaker_price(
                    market=mkt_key, specifier=None, selection=outcome,
                    price=price, bookmaker_id=bookmaker_id,
                )

                # Append history on price change
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
    """
    Write a unified SBO match (from OddsAggregator) to the DB.
    SBO matches have a different shape: bookmakers{} + best_odds + unified_markets.
    """
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

        # unified_markets: {market_key: {outcome: [{bookie, odd}]}}
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
def harvest_b2b_page(
    self,
    bookmaker: dict,
    sport: str,
    mode: str,
    page: int,
) -> dict:
    """
    Fetch one page (PAGE_SIZE records) from a B2B bookmaker.
    Writes to Redis cache + DB.  Queues EV/Arb compute task.
    """
    t0      = time.perf_counter()
    bk_name = bookmaker.get("name") or bookmaker.get("domain", "?")
    bk_id   = bookmaker.get("id")

    try:
        from app.workers.bookmaker_fetcher import fetch_bookmaker
        matches = fetch_bookmaker(
            bookmaker, sport_name=sport, mode=mode,
            page=page, page_size=PAGE_SIZE, timeout=20,
        )
    except Exception as exc:
        logger.warning(f"[b2b] {bk_name}/{sport}/p{page}: {exc}")
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)

    # ── Redis cache (raw results for fast API response) ────────────────────
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

    # ── DB persistence ────────────────────────────────────────────────────
    match_ids: list[int] = []
    for m in matches:
        mid = _upsert_unified_match(m, bk_id, bk_name)
        if mid:
            match_ids.append(mid)

    # ── Queue EV/Arb compute for each match ───────────────────────────────
    for match_id in match_ids:
        compute_ev_arb_for_match.apply_async(
            args=[match_id],
            queue="ev_arb",
            countdown=1,   # 1-second delay so DB write lands first
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
    """
    Fan-out B2B upcoming harvest.
    Creates 20 workers × PAGE_SIZE = 300 records per sport per bookmaker.
    Stagger by 0.15 s so all 20 workers don't hammer Redis simultaneously.
    """
    bookmakers = _load_bookmakers()
    if not bookmakers:
        task_status_set("beat_upcoming", {"state": "error", "error": "No bookmakers", "dispatched": 0})
        return {"dispatched": 0}

    # Estimate total pages needed (BetB2B returns up to 1000 per sport)
    MAX_PAGES = 4   # 4 pages × 15 = 60 matches per sport per bookmaker

    dispatched = 0
    for bm in bookmakers:
        for sport in _B2B_SPORTS:
            for page in range(1, MAX_PAGES + 1):
                harvest_b2b_page.apply_async(
                    args=[bm, sport, mode, page],
                    queue="harvest",
                    countdown=dispatched * 0.15,  # 150ms stagger
                )
                dispatched += 1

    logger.info(f"[beat:upcoming] {dispatched} tasks ({len(bookmakers)} bk × {len(_B2B_SPORTS)} sports × {MAX_PAGES} pages)")
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
    """Fan-out live B2B harvest — fires every 60 s."""
    bookmakers  = _load_bookmakers()
    live_sports = ["Football", "Basketball", "Ice Hockey", "Tennis"]

    dispatched = 0
    for bm in bookmakers:
        for sport in live_sports:
            # Live is always page 1 (live events don't paginate the same way)
            harvest_b2b_page.apply_async(
                args=[bm, sport, "live", 1],
                queue="live",
            )
            dispatched += 1

    task_status_set("beat_live", {"state": "ok", "dispatched": dispatched})
    return {"dispatched": dispatched}


# =============================================================================
# SBO Harvest  (Sportpesa / Betika / Odibets)
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
    """
    Fetch one sport from Sportpesa + Betika + Odibets and persist.
    Runs the full OddsAggregator pipeline for PAGE_SIZE × max_pages matches.
    """
    t0 = time.perf_counter()
    try:
        from app.workers.sbo_fetcher import OddsAggregator, SPORT_CONFIG
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

    # ── Redis cache ────────────────────────────────────────────────────────
    cache_set(f"sbo:upcoming:{sport_slug}", {
        "sport":       sport_slug,
        "match_count": len(matches),
        "harvested_at": datetime.now(timezone.utc).isoformat(),
        "latency_ms":  latency,
        "matches":     matches,
    }, ttl=180)

    # ── DB persistence ────────────────────────────────────────────────────
    bk_name_to_id = _bookmaker_id_map()
    match_ids: list[int] = []
    for m in matches:
        mid = _upsert_sbo_match(m, bk_name_to_id)
        if mid:
            match_ids.append(mid)

    # ── Queue EV/Arb ─────────────────────────────────────────────────────
    for match_id in match_ids:
        compute_ev_arb_for_match.apply_async(
            args=[match_id],
            queue="ev_arb",
            countdown=2,
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
    """Fan-out SBO upcoming harvest — fires every 3 min."""
    dispatched = 0
    for i, sport in enumerate(_SBO_SPORTS):
        harvest_sbo_sport.apply_async(
            args=[sport, 60],
            queue="harvest",
            countdown=i * 3,   # 3-second stagger between sports
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
    """SBO doesn't have a separate live endpoint — fetch soccer + basketball only."""
    live_sports = ["soccer", "basketball", "tennis"]
    dispatched  = 0
    for sport in live_sports:
        harvest_sbo_sport.apply_async(
            args=[sport, 30],  # fewer matches for live
            queue="live",
        )
        dispatched += 1
    return {"dispatched": dispatched}


# =============================================================================
# EV / Arbitrage Compute  (runs after each match is written to DB)
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
    """
    Load unified_markets for one match, compute EV + Arb, persist results,
    then queue notifications if thresholds met.
    """
    try:
        from app.models.odds_model import UnifiedMatch
        from app.workers.ev_arb_service import EVArbPersistenceService

        um = UnifiedMatch.query.get(match_id)
        if not um or not um.markets_json:
            return {"ok": False, "reason": "no markets"}

        # Convert unified_match markets_json → ev_arb_service format
        # markets_json: {market: {spec: {selection: {bookmakers: {bk_id: price}}}}}
        # unified format: {market_key: {outcome: [{bookie, odd}]}}
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

        # Queue notifications for active arbs and high-EV opportunities
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
    """
    Update status and scores for matches that have started or finished.
    Polls a results endpoint (configured per bookmaker) for score data.
    Also settles bankroll bets.
    """
    try:
        from app.extensions import db
        from app.models.odds_model import UnifiedMatch

        now        = datetime.now(timezone.utc)
        updated    = 0

        # Matches that started in the last 3 hours and aren't finished
        candidates = UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now,
            UnifiedMatch.start_time >= now - timedelta(hours=3),
            UnifiedMatch.status.in_(["PRE_MATCH", "IN_PLAY"]),
        ).all()

        for um in candidates:
            # Mark as in-play if start time has passed
            if um.status == "PRE_MATCH":
                um.status = "IN_PLAY"
                updated += 1

        # Matches that started > 2.5 hours ago → mark as finished
        finished = UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now - timedelta(hours=2, minutes=30),
            UnifiedMatch.status == "IN_PLAY",
        ).all()

        for um in finished:
            um.status = "FINISHED"
            updated  += 1
            # Collapse active arbs for this match
            from app.workers.ev_arb_service import EVArbPersistenceService
            EVArbPersistenceService.collapse_stale_arbs(now)

            # Settle pending bankroll bets (mark as void — user settles manually for now)
            _settle_bankroll_bets(um.id)

        if updated:
            db.session.commit()

        logger.info(f"[results] updated {updated} match statuses")
        return {"updated": updated}

    except Exception as exc:
        logger.error(f"[results] {exc}")
        return {"error": str(exc)}


def _settle_bankroll_bets(match_id: int) -> None:
    """Stub — full result parsing requires bookmaker-specific result APIs."""
    try:
        from app.models.subscription_models import BankrollBet
        pending = BankrollBet.query.filter_by(match_id=match_id, status="pending").all()
        for bet in pending:
            bet.status = "manual_check"   # flag for user review
    except Exception:
        pass


# =============================================================================
# Cache finished games (monthly cache to avoid DB reads on every request)
# =============================================================================

@celery.task(
    name="app.workers.celery_tasks.cache_finished_games",
    soft_time_limit=120,
    time_limit=150,
)
def cache_finished_games() -> dict:
    """
    Cache finished matches for the last 30 days into Redis.
    Key: results:finished:{YYYY-MM-DD}
    TTL: 30 days
    """
    try:
        from app.models.odds_model import UnifiedMatch

        now      = datetime.now(timezone.utc)
        cached   = 0

        for day_offset in range(0, 30):
            day_start = (now - timedelta(days=day_offset)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            day_end = day_start + timedelta(days=1)
            date_str = day_start.strftime("%Y-%m-%d")
            cache_key = f"results:finished:{date_str}"

            if cache_get(cache_key):
                continue   # already cached

            matches = UnifiedMatch.query.filter(
                UnifiedMatch.status == "FINISHED",
                UnifiedMatch.start_time >= day_start,
                UnifiedMatch.start_time <  day_end,
            ).all()

            if matches:
                data = [m.to_dict() for m in matches]
                cache_set(cache_key, data, ttl=30 * 86400)  # 30 days
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
    """
    Send email / push notifications to Pro + Premium users who have
    eligible thresholds configured.
    """
    try:
        from app.models.odds_model import UnifiedMatch, ArbitrageOpportunity, EVOpportunity
        from app.models.subscription_models import User, NotificationPref, SubscriptionTier
        from app.workers.notification_service import NotificationService

        um = UnifiedMatch.query.get(match_id)
        if not um:
            return {"ok": False}

        match_label = f"{um.home_team_name} v {um.away_team_name}"

        # Collect active arbs + EV opps for this match
        arbs = ArbitrageOpportunity.query.filter_by(match_id=match_id, is_active=True).all()
        evs  = EVOpportunity.query.filter_by(match_id=match_id, is_active=True).all()

        if not arbs and not evs:
            return {"ok": True, "sent": 0}

        # Find eligible subscribers
        eligible_tiers = [SubscriptionTier.PRO.value, SubscriptionTier.PREMIUM.value]
        prefs = (
            NotificationPref.query
            .join(User, User.id == NotificationPref.user_id)
            .filter(User.is_active == True)
            .all()
        )

        sent = 0
        for pref in prefs:
            user = pref.user
            if user.tier not in eligible_tiers:
                continue
            if not pref.email_enabled:
                continue

            # Filter arbs by user threshold
            qualifying_arbs = [a for a in arbs if a.max_profit_percentage >= pref.arb_min_profit]
            qualifying_evs  = [e for e in evs  if e.edge_pct >= pref.ev_min_edge]

            if not qualifying_arbs and not qualifying_evs:
                continue

            # Sports filter
            if pref.sports_filter and um.sport_name:
                if um.sport_name.lower() not in [s.lower() for s in pref.sports_filter]:
                    continue

            try:
                NotificationService.send_alert(
                    user         = user,
                    match_label  = match_label,
                    arbs         = qualifying_arbs,
                    evs          = qualifying_evs,
                    event_type   = event_type,
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
    """
    Expire subscriptions where trial_ends or period_end has passed.
    Triggers billing webhook for auto-renew.
    """
    try:
        from app.extensions import db
        from app.models.subscription_models import Subscription, SubscriptionStatus

        now     = datetime.now(timezone.utc)
        expired = 0

        # Trial expired
        trials = Subscription.query.filter(
            Subscription.status   == SubscriptionStatus.TRIAL.value,
            Subscription.is_trial == True,
            Subscription.trial_ends <= now,
        ).all()
        for sub in trials:
            # Attempt to charge — if no payment method, expire
            charged = _attempt_charge(sub)
            if charged:
                sub.activate()
            else:
                sub.status   = SubscriptionStatus.EXPIRED.value
                sub.is_trial = False
                sub._append_history("trial_expired_no_payment")
            expired += 1

        # Active subscriptions past period_end
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
    """Stub — integrate with M-Pesa / Stripe here. Returns True if charged."""
    # TODO: call payment gateway API
    # For now, always return False so subscriptions expire naturally
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
    from app.workers.bookmaker_fetcher import fetch_bookmaker
    t0 = time.perf_counter()
    try:
        matches = fetch_bookmaker(bookmaker, sport_name=sport, mode=mode, timeout=20)
        latency = int((time.perf_counter() - t0) * 1000)
        return {"ok": True, "count": len(matches), "latency_ms": latency,
                "matches": matches[:10], "bookmaker": bookmaker.get("name")}
    except Exception as exc:
        latency = int((time.perf_counter() - t0) * 1000)
        return {"ok": False, "error": str(exc), "latency_ms": latency}