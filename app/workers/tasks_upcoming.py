"""
app/workers/tasks_upcoming.py
==============================
All harvest tasks for SP, BT, OD, B2B.

Key design decisions:
  1. Each BK writes to its own Redis key independently.
     Cross-BK merging happens at READ TIME in odds_stream._get_unified().
  2. No SP dependency in bt_od_harvest_sport.
     BT/OD data for darts, handball, mma etc. is written even if SP has no cache.
  3. canonical sport slugs only — no aliases (soccer not football, ice-hockey not icehockey).
  4. HarvestJob is written per task for monitoring via flask harvest-health.
  5. MarketFailure is recorded on parse errors for monitoring via flask market-failures.
"""
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timezone, timedelta
from calendar import monthrange

from celery import group
from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded

from app.extensions import celery
from app.workers.celery_tasks import (
    cache_set, cache_get, _now_iso, _publish,
    _upsert_and_chain, _extract_betradar_id,
    _get_or_create_bookmaker, _redis,
)

logger = get_task_logger(__name__)

# =============================================================================
# CANONICAL SPORT LISTS — single source of truth, no aliases
# =============================================================================

_SP_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey", "volleyball",
    "cricket", "rugby", "table-tennis", "handball", "baseball",
    "mma", "boxing", "darts", "esoccer",
]

_BT_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey", "volleyball",
    "cricket", "rugby", "table-tennis", "darts", "handball", "mma", "boxing",
]

_OD_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey", "volleyball",
    "cricket", "rugby", "boxing", "handball", "mma", "table-tennis",
    "darts", "american-football", "esoccer",
]

_B2B_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey", "volleyball",
    "cricket", "rugby", "table-tennis", "darts", "handball",
]

# Union of all — used by beat schedule
_ALL_SPORTS = sorted(set(_SP_SPORTS + _BT_SPORTS + _OD_SPORTS))

# =============================================================================
# CONSTANTS
# =============================================================================

PAGE_SIZE        = 15
WS_CHANNEL       = "odds:updates"
SP_MAX_MATCHES   = 3_000
_CROSS_BK_WORKERS = 8
_ANALYTICS_TTL   = 86_400

_BK_NAMES: dict[str, str] = {
    "sp": "SportPesa", "bt": "Betika", "od": "OdiBets",
    "b2b": "1xBet", "1xbet": "1xBet", "22bet": "22Bet",
    "betwinner": "Betwinner", "melbet": "Melbet",
    "megapari": "Megapari", "helabet": "Helabet", "paripesa": "Paripesa",
}

_SPORT_SLUG_TO_DB: dict[str, str] = {
    "soccer": "Soccer", "basketball": "Basketball", "tennis": "Tennis",
    "ice-hockey": "Ice Hockey", "volleyball": "Volleyball", "cricket": "Cricket",
    "rugby": "Rugby", "table-tennis": "Table Tennis", "handball": "Handball",
    "mma": "MMA", "boxing": "Boxing", "darts": "Darts",
    "esoccer": "eSoccer", "american-football": "American Football", "baseball": "Baseball",
}


# =============================================================================
# HARVEST JOB LOGGING
# =============================================================================

def _log_harvest_job(bk_slug: str, sport_slug: str, mode: str,
                     started_at: float, match_count: int,
                     status: str = "ok", error: str = "") -> None:
    """Write a HarvestJob row. Safe to call — swallows its own exceptions."""
    try:
        from app.models.bookmakers_model import HarvestJob, Bookmaker
        from app.extensions import db
        bk = Bookmaker.query.filter_by(slug=bk_slug).first()
        db.session.add(HarvestJob(
            bookmaker_id  = bk.id if bk else None,
            sport_slug    = sport_slug,
            mode          = mode,
            started_at    = datetime.fromtimestamp(started_at, tz=timezone.utc),
            finished_at   = datetime.now(timezone.utc),
            latency_ms    = int((time.perf_counter() - started_at) * 1000),
            match_count   = match_count,
            status        = status,
            error_message = error[:500] if error else "",
        ))
        db.session.commit()
    except Exception:
        pass


# =============================================================================
# HELPERS
# =============================================================================

def _emit(source: str, sport: str, count: int, latency: int) -> None:
    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": source, "sport": sport,
        "mode": "upcoming", "count": count, "latency_ms": latency, "ts": _now_iso(),
    })


def _persist_bk_matches(matches: list[dict], bk_slug: str, sport_slug: str) -> None:
    """Serialize matches and dispatch DB persistence via Celery."""
    if not matches:
        return
    bk_id = _get_or_create_bookmaker(_BK_NAMES.get(bk_slug, bk_slug.upper()))
    if not bk_id:
        return
    canonical_sport = _SPORT_SLUG_TO_DB.get(sport_slug, sport_slug)
    serialized: list[dict] = []
    for m in matches:
        betradar_id = _extract_betradar_id(m)
        ext_id = str(
            m.get("sp_game_id") or m.get("bt_parent_id") or m.get("bt_match_id") or
            m.get("od_parent_id") or m.get("od_event_id") or m.get("od_match_id") or
            m.get("match_id") or m.get("event_id") or ""
        ).strip() or None

        join_key = f"br_{betradar_id}" if betradar_id else (f"{bk_slug}_{ext_id}" if ext_id else None)
        if not join_key:
            continue
        markets = m.get("markets") or {}
        if not markets:
            continue
        serialized.append({
            "join_key":       join_key,
            "home_team":      m.get("home_team") or m.get("home_team_name") or "",
            "away_team":      m.get("away_team") or m.get("away_team_name") or "",
            "competition":    m.get("competition") or m.get("competition_name") or "",
            "start_time":     m.get("start_time") or "",
            "is_live":        False,
            "betradar_id":    betradar_id,
            "sport":          canonical_sport,
            "bk_ids":         {bk_slug: ext_id or join_key},
            "markets":        markets,
            "bookmaker_slug": bk_slug,
        })
    if not serialized:
        return
    for i in range(0, len(serialized), 500):
        chunk = serialized[i: i + 500]
        try:
            celery.send_task(
                "tasks.ops.persist_combined_batch",
                args=[chunk, sport_slug, "upcoming"],
                queue="results", countdown=3,
            )
        except Exception as exc:
            logger.warning("[persist_bk] dispatch failed %s/%s: %s", bk_slug, sport_slug, exc)


def _schedule_alignment(sport_slug: str, countdown: int = 60) -> None:
    try:
        from app.workers.tasks_market_align import align_sport_markets
        align_sport_markets.apply_async(args=[sport_slug, 100], queue="results", countdown=countdown)
    except Exception:
        pass


# =============================================================================
# BT / OD FETCH HELPERS  (no SP dependency)
# =============================================================================

def _fetch_bt_sport(sport_slug: str) -> list[dict]:
    """Fetch Betika matches. Returns [] on any failure."""
    try:
        from app.workers.bt_harvester import fetch_upcoming_matches
        matches = fetch_upcoming_matches(sport_slug=sport_slug, days=30, max_pages=30, fetch_full=True)
        logger.info("[bt_fetch] %s: %d matches", sport_slug, len(matches or []))
        return matches or []
    except Exception as exc:
        logger.warning("[bt_fetch] %s: %s", sport_slug, exc)
        return []


def _fetch_od_sport(sport_slug: str) -> list[dict]:
    """Fetch OdiBets matches. Returns [] on any failure."""
    try:
        from app.workers.od_harvester import fetch_upcoming_matches
        matches = fetch_upcoming_matches(sport_slug=sport_slug, days=30, fetch_full_markets=True)
        logger.info("[od_fetch] %s: %d matches", sport_slug, len(matches or []))
        return matches or []
    except Exception as exc:
        logger.warning("[od_fetch] %s: %s", sport_slug, exc)
        return []


# =============================================================================
# SPORTPESA
# =============================================================================

@celery.task(
    name="tasks.sp.harvest_sport", bind=True,
    max_retries=2, default_retry_delay=20,
    soft_time_limit=6000, time_limit=3660, acks_late=True,
)
def sp_harvest_sport(self, sport_slug: str, max_matches: int = SP_MAX_MATCHES) -> dict:
    t0      = time.perf_counter()
    matches = []
    started = t0

    try:
        from app.workers.sp_harvester import fetch_upcoming_stream
        for match in fetch_upcoming_stream(
            sport_slug, fetch_full_markets=True,
            max_matches=max_matches, days=30, sleep_between=0.1,
        ):
            matches.append(match)
    except SoftTimeLimitExceeded:
        logger.warning("[sp] soft timeout %s — saving %d partial", sport_slug, len(matches))
    except Exception as exc:
        if matches:
            logger.warning("[sp] %s error saving partial: %s", sport_slug, exc)
        else:
            _log_harvest_job("sp", sport_slug, "upcoming", started, 0, "error", str(exc))
            raise self.retry(exc=exc)

    if not matches:
        _log_harvest_job("sp", sport_slug, "upcoming", started, 0, "error", "no matches")
        # Still dispatch bt_od so those sports get data
        try:
            celery.send_task("tasks.bt_od.harvest_sport", args=[sport_slug], queue="harvest", countdown=5)
        except Exception:
            pass
        return {"ok": False, "reason": "no_sp_matches"}

    latency     = int((time.perf_counter() - t0) * 1000)
    br_count    = sum(1 for m in matches if m.get("betradar_id"))
    avg_markets = int(sum(m.get("market_count", 0) for m in matches) / max(len(matches), 1))

    # 1. Write to Redis — key pattern that _merge_bks finds
    cache_set(f"sp:upcoming:{sport_slug}", {
        "source": "sportpesa", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
        "avg_markets": avg_markets, "br_count": br_count,
    }, ttl=3600)

    # 2. Also write to odds:sp:upcoming:{sport} (second pattern _merge_bks checks)
    from app.workers.redis_bus import publish_snapshot
    publish_snapshot("sp", "upcoming", sport_slug, matches, meta={
        "source": "sportpesa", "avg_markets": avg_markets, "br_count": br_count,
    })

    _emit("sportpesa", sport_slug, len(matches), latency)

    # 3. Persist to DB
    _upsert_and_chain(matches, "SportPesa")
    _persist_bk_matches(matches, "sp", sport_slug)

    # 4. Cross-BK enrichment via betradar_id
    if br_count > 0:
        sp_cross_bk_enrich.apply_async(args=[sport_slug], queue="harvest", countdown=10)

    sp_enrich_analytics.apply_async(args=[sport_slug], queue="harvest", countdown=60)
    _schedule_alignment(sport_slug, countdown=60)

    # 5. Trigger BT+OD harvest (independent — not waiting for SP cross-enrich)
    try:
        celery.send_task("tasks.bt_od.harvest_sport", args=[sport_slug], queue="harvest", countdown=30)
    except Exception as exc:
        logger.warning("[sp] bt_od dispatch failed %s: %s", sport_slug, exc)

    # 6. Notify admin of harvest completion
    from app.api.notifications import publish_harvest_done
    publish_harvest_done("sp", sport_slug, len(matches), latency)

    _log_harvest_job("sp", sport_slug, "upcoming", started, len(matches), "ok")

    return {
        "ok": True, "source": "sportpesa", "sport": sport_slug,
        "count": len(matches), "br_count": br_count, "latency_ms": latency,
    }


@celery.task(name="tasks.sp.harvest_all_upcoming", soft_time_limit=300, time_limit=600)
def sp_harvest_all_upcoming() -> dict:
    sigs = [sp_harvest_sport.s(s, SP_MAX_MATCHES) for s in _SP_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs), "sports": _SP_SPORTS}


# =============================================================================
# BT + OD (independent — no SP dependency)
# =============================================================================

@celery.task(
    name="tasks.bt_od.harvest_sport", bind=True,
    max_retries=2, default_retry_delay=30,
    soft_time_limit=600, time_limit=660, acks_late=True,
)
def bt_od_harvest_sport(self, sport_slug: str) -> dict:
    """
    Fetch BT + OD concurrently. Each is written to its own Redis key.
    NO SP cache dependency — works for all sports including darts/mma/handball.
    Cross-BK merging happens at read time in odds_stream._get_unified().
    """
    from app.workers.redis_bus import publish_snapshot
    from app.api.notifications import publish_harvest_done
    t0 = time.perf_counter()

    bt_matches: list[dict] = []
    od_matches: list[dict] = []

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            bt_fut = pool.submit(_fetch_bt_sport, sport_slug)
            od_fut = pool.submit(_fetch_od_sport, sport_slug)
            bt_matches = bt_fut.result() or []
            od_matches = od_fut.result() or []
    except SoftTimeLimitExceeded:
        raise
    except Exception as exc:
        raise self.retry(exc=exc)

    # ── Betika ────────────────────────────────────────────────────────────────
    if bt_matches:
        # Write to BOTH key patterns so _merge_bks finds it
        payload = {
            "source": "betika", "sport": sport_slug, "mode": "upcoming",
            "match_count": len(bt_matches), "harvested_at": _now_iso(),
            "matches": bt_matches,
        }
        cache_set(f"bt:upcoming:{sport_slug}", payload, ttl=3600)
        publish_snapshot("bt", "upcoming", sport_slug, bt_matches, meta={"source": "betika"})
        _upsert_and_chain(bt_matches, "Betika")
        _persist_bk_matches(bt_matches, "bt", sport_slug)
        publish_harvest_done("bt", sport_slug, len(bt_matches),
                             int((time.perf_counter() - t0) * 1000))
        logger.info("[bt_od] %s BT: %d matches", sport_slug, len(bt_matches))

    # ── OdiBets ───────────────────────────────────────────────────────────────
    if od_matches:
        payload = {
            "source": "odibets", "sport": sport_slug, "mode": "upcoming",
            "match_count": len(od_matches), "harvested_at": _now_iso(),
            "matches": od_matches,
        }
        cache_set(f"od:upcoming:{sport_slug}", payload, ttl=3600)
        publish_snapshot("od", "upcoming", sport_slug, od_matches, meta={"source": "odibets"})
        _upsert_and_chain(od_matches, "OdiBets")
        _persist_bk_matches(od_matches, "od", sport_slug)
        publish_harvest_done("od", sport_slug, len(od_matches),
                             int((time.perf_counter() - t0) * 1000))
        logger.info("[bt_od] %s OD: %d matches", sport_slug, len(od_matches))

    if not bt_matches and not od_matches:
        logger.warning("[bt_od] %s: both BT and OD returned empty", sport_slug)

    latency = int((time.perf_counter() - t0) * 1000)
    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": "bt_od", "sport": sport_slug,
        "bt_count": len(bt_matches), "od_count": len(od_matches),
        "latency_ms": latency, "ts": _now_iso(),
    })

    return {
        "ok": True, "sport": sport_slug,
        "bt_count": len(bt_matches), "od_count": len(od_matches),
        "latency_ms": latency,
    }


@celery.task(name="tasks.bt_od.harvest_all_upcoming", soft_time_limit=60, time_limit=90)
def bt_od_harvest_all_upcoming() -> dict:
    """Dispatch bt_od_harvest_sport for every sport BT or OD covers."""
    sports = sorted(set(_BT_SPORTS + _OD_SPORTS))
    sigs   = [bt_od_harvest_sport.s(s) for s in sports]
    group(sigs).apply_async(queue="harvest")
    logger.info("[bt_od:all] dispatched %d sports", len(sigs))
    return {"dispatched": len(sigs), "sports": sports}


# =============================================================================
# SP CROSS-BK ENRICHMENT (betradar_id driven)
# =============================================================================

@celery.task(
    name="tasks.sp.cross_bk_enrich", bind=True,
    max_retries=1, default_retry_delay=120,
    soft_time_limit=3600, time_limit=3660, acks_late=True,
)
def sp_cross_bk_enrich(self, sport_slug: str) -> dict:
    """
    For matches that have a betradar_id from SP, directly fetch
    BT and OD markets by that ID (faster than list-based harvest).
    """
    from app.workers.od_harvester import slug_to_od_sport_id

    cached = cache_get(f"sp:upcoming:{sport_slug}")
    if not cached:
        return {"ok": False, "reason": "no_sp_cache"}

    sp_matches = cached.get("matches") or []
    with_br    = [m for m in sp_matches if m.get("betradar_id")]
    if not with_br:
        return {"ok": True, "bt_enriched": 0, "od_enriched": 0}

    od_sport_id  = slug_to_od_sport_id(sport_slug)
    bt_batch:    list[dict] = []
    od_batch:    list[dict] = []
    bt_enriched = od_enriched = bt_errors = od_errors = 0

    def _fetch_bt(sp_m):
        from app.workers.bt_harvester import get_full_markets
        markets = get_full_markets(sp_m["betradar_id"], sport_slug)
        return sp_m, markets

    def _fetch_od(sp_m):
        from app.workers.od_harvester import fetch_event_detail
        markets, _meta = fetch_event_detail(sp_m["betradar_id"], od_sport_id)
        return sp_m, markets

    try:
        with ThreadPoolExecutor(max_workers=_CROSS_BK_WORKERS) as pool:
            bt_futs = {pool.submit(_fetch_bt, m): m for m in with_br}
            for fut in as_completed(bt_futs):
                try:
                    sp_m, bt_markets = fut.result()
                    if bt_markets:
                        bt_batch.append({**sp_m, "markets": bt_markets,
                                         "market_count": len(bt_markets),
                                         "bt_parent_id": sp_m["betradar_id"]})
                        bt_enriched += 1
                except SoftTimeLimitExceeded:
                    raise
                except Exception:
                    bt_errors += 1
    except SoftTimeLimitExceeded:
        raise

    if bt_batch:
        from app.workers.redis_bus import publish_snapshot
        cache_set(f"bt:upcoming:{sport_slug}", {
            "source": "betika", "sport": sport_slug, "mode": "upcoming",
            "match_count": len(bt_batch), "harvested_at": _now_iso(),
            "matches": bt_batch,
        }, ttl=3600)
        publish_snapshot("bt", "upcoming", sport_slug, bt_batch)
        _upsert_and_chain(bt_batch, "Betika")
        _persist_bk_matches(bt_batch, "bt", sport_slug)

    try:
        with ThreadPoolExecutor(max_workers=_CROSS_BK_WORKERS) as pool:
            od_futs = {pool.submit(_fetch_od, m): m for m in with_br}
            for fut in as_completed(od_futs):
                try:
                    sp_m, od_markets = fut.result()
                    if od_markets:
                        od_batch.append({**sp_m, "markets": od_markets,
                                         "market_count": len(od_markets),
                                         "od_event_id": sp_m["betradar_id"]})
                        od_enriched += 1
                except SoftTimeLimitExceeded:
                    raise
                except Exception:
                    od_errors += 1
    except SoftTimeLimitExceeded:
        raise

    if od_batch:
        from app.workers.redis_bus import publish_snapshot
        cache_set(f"od:upcoming:{sport_slug}", {
            "source": "odibets", "sport": sport_slug, "mode": "upcoming",
            "match_count": len(od_batch), "harvested_at": _now_iso(),
            "matches": od_batch,
        }, ttl=3600)
        publish_snapshot("od", "upcoming", sport_slug, od_batch)
        _upsert_and_chain(od_batch, "OdiBets")
        _persist_bk_matches(od_batch, "od", sport_slug)

    _schedule_alignment(sport_slug, countdown=30)
    return {
        "ok": True, "sport": sport_slug, "total": len(with_br),
        "bt_enriched": bt_enriched, "bt_errors": bt_errors,
        "od_enriched": od_enriched, "od_errors": od_errors,
    }


# =============================================================================
# SPORTRADAR ANALYTICS
# =============================================================================

@celery.task(
    name="tasks.sp.enrich_analytics", bind=True,
    max_retries=1, soft_time_limit=3600, time_limit=3660, acks_late=True,
)
def sp_enrich_analytics(self, sport_slug: str) -> dict:
    from app.workers.sr_analytics import get_match_details, get_match_analytics
    cached = cache_get(f"sp:upcoming:{sport_slug}")
    if not cached:
        return {"ok": False}
    with_br = [m for m in (cached.get("matches") or []) if m.get("betradar_id")]
    if not with_br:
        return {"ok": True, "fetched": 0}

    fetched = errors = 0
    for m in with_br:
        br_id = m["betradar_id"]
        try:
            existing = cache_get(f"sr:analytics:{br_id}")
            if existing and existing.get("available"):
                fetched += 1
                continue
            bundle = get_match_analytics(br_id, fetch_season=True)
            cache_set(f"sr:analytics:{br_id}", bundle, ttl=_ANALYTICS_TTL)
            fetched += 1
            time.sleep(0.2)
        except SoftTimeLimitExceeded:
            break
        except Exception:
            errors += 1

    return {"ok": True, "sport": sport_slug, "fetched": fetched, "errors": errors}


# =============================================================================
# B2B HARVEST
# =============================================================================

@celery.task(
    name="tasks.b2b.harvest_sport", bind=True,
    max_retries=2, default_retry_delay=30,
    soft_time_limit=300, time_limit=360, acks_late=True,
)
def b2b_harvest_sport(self, sport_slug: str) -> dict:
    t0 = time.perf_counter()
    try:
        from app.workers.b2b_harvester import fetch_b2b_sport
        matches = fetch_b2b_sport(sport_slug, mode="upcoming")
    except Exception as exc:
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)

    if matches:
        cache_set(f"b2b:upcoming:{sport_slug}", {
            "source": "b2b", "sport": sport_slug, "mode": "upcoming",
            "match_count": len(matches), "harvested_at": _now_iso(),
            "latency_ms": latency, "matches": matches,
        }, ttl=300)

        from app.workers.redis_bus import publish_snapshot
        publish_snapshot("b2b", "upcoming", sport_slug, matches)

    _emit("b2b", sport_slug, len(matches), latency)
    return {"ok": True, "source": "b2b", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.b2b.harvest_all_upcoming", soft_time_limit=300, time_limit=600)
def b2b_harvest_all_upcoming() -> dict:
    sigs = [b2b_harvest_sport.s(s) for s in _B2B_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


# =============================================================================
# STANDALONE BT / OD (manual triggers only — NOT in beat schedule)
# =============================================================================

@celery.task(
    name="tasks.bt.harvest_sport", bind=True,
    max_retries=2, soft_time_limit=600, time_limit=660, acks_late=True,
)
def bt_harvest_sport(self, sport_slug: str, max_matches: int = 3000) -> dict:
    """Manual BT harvest — use bt_od_harvest_sport for scheduled runs."""
    t0 = time.perf_counter()
    matches = _fetch_bt_sport(sport_slug)
    if not matches:
        return {"ok": False, "reason": "no matches"}

    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"bt:upcoming:{sport_slug}", {
        "source": "betika", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=3600)

    from app.workers.redis_bus import publish_snapshot
    publish_snapshot("bt", "upcoming", sport_slug, matches)
    _upsert_and_chain(matches, "Betika")
    _persist_bk_matches(matches, "bt", sport_slug)
    return {"ok": True, "sport": sport_slug, "count": len(matches), "latency_ms": latency}


@celery.task(
    name="tasks.od.harvest_sport", bind=True,
    max_retries=1, soft_time_limit=1800, time_limit=1860, acks_late=True,
)
def od_harvest_sport(self, sport_slug: str) -> dict:
    """Manual OD harvest — use bt_od_harvest_sport for scheduled runs."""
    t0 = time.perf_counter()
    matches = _fetch_od_sport(sport_slug)
    if not matches:
        return {"ok": False, "reason": "no matches"}

    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"od:upcoming:{sport_slug}", {
        "source": "odibets", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=3600)

    from app.workers.redis_bus import publish_snapshot
    publish_snapshot("od", "upcoming", sport_slug, matches)
    _upsert_and_chain(matches, "OdiBets")
    _persist_bk_matches(matches, "od", sport_slug)
    return {"ok": True, "sport": sport_slug, "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.bt.harvest_all_upcoming", soft_time_limit=300, time_limit=600)
def bt_harvest_all_upcoming() -> dict:
    sigs = [bt_harvest_sport.s(s) for s in _BT_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


@celery.task(name="tasks.od.harvest_all_upcoming", soft_time_limit=300, time_limit=600)
def od_harvest_all_upcoming() -> dict:
    sigs = [od_harvest_sport.s(s) for s in _OD_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}