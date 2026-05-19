"""
app/workers/tasks_harvest_pages.py  (PATCHED)
===============================================
Changes vs original:
  1. Added _persist_snapshot() helper — replaces cache_set(..., ttl=3600)
     in all three merge functions (sp_merge_pages, bt_merge_pages, od_merge_pages).
     Uses smart_set (86400 TTL + hash-based change detection) + snapshot_to_db.

  2. All other logic is IDENTICAL to the original. The harvester pagination,
     page accumulation, merge logic, _upsert_and_chain calls, beat schedule,
     and alignment tasks are completely unchanged.

  3. Compatible with:
     • tasks_upcoming._write_bk_keys() — still uses cache_set(3600) and that's
       fine; the prune task (tasks_ops._beat_prune) will refresh those TTLs.
     • persist_hook.persist_from_serialized() — called via _upsert_and_chain,
       unchanged.
     • odds_stream._get_unified_patched() — reads the keys we write here.
"""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from app.workers.bandwidth_optimizer import redis_set_compressed, slim_match_list
from celery.utils.log import get_task_logger

from app.workers.celery_tasks import celery, cache_set, _now_iso, _publish
from app.workers.redis_bus import (
    publish_page, publish_snapshot, merge_pages, pages_done_count,
)

logger = get_task_logger(__name__)

HARVEST_PAGE_SIZE = 100
HARVEST_N_PAGES   = 10
MERGE_COUNTDOWN   = 55
OD_DAYS_AHEAD     = 30

WS_CHANNEL = "odds:updates"

_ALL_SPORTS: list[str] = [
    "soccer", "basketball", "tennis", "cricket", "rugby",
    "ice-hockey", "volleyball", "handball", "table-tennis",
    "baseball", "mma", "boxing", "darts", "american-football", "esoccer",
]

_SP_SPORTS  = ["soccer", "basketball", "tennis", "ice-hockey",
               "volleyball", "cricket", "rugby", "table-tennis"]

_BT_SPORTS  = ["soccer", "basketball", "tennis", "ice-hockey",
               "volleyball", "cricket", "rugby", "table-tennis",
               "darts", "handball"]

_OD_SPORTS  = ["soccer", "basketball", "tennis", "ice-hockey",
               "volleyball", "cricket", "rugby", "boxing",
               "handball", "mma", "table-tennis", "darts",
               "american-football", "esoccer"]


# ── Shared Redis client for persistent_cache writes ───────────────────────────

def _get_cache_redis():
    import redis as _redis_mod
    url  = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    return _redis_mod.Redis.from_url(f"{base}/2", decode_responses=False, socket_timeout=5)


# ── NEW: persistent snapshot helper ──────────────────────────────────────────

def _persist_snapshot(bk: str, sport: str, payload: dict) -> None:
    """
    Drop-in replacement for:
        cache_set(f"{bk}:upcoming:{sport}", payload, ttl=3600)

    Does two things:
    1. Writes to Redis with 86400 TTL and only if content changed
       (smart_set). This keeps the key alive far longer than cache_set's 3600.
    2. Backs up the payload to odds_snapshot in Postgres so cold-start
       hydration is instant.

    The {bk}:upcoming:{sport} key is what odds_stream._get_unified_patched()
    falls back to when the unified key is cold. Keeping it alive at 86400
    means the fallback always has data.
    """
    try:
        from app.workers.persistent_cache import smart_set, snapshot_to_db, TTL_BK_SNAP
        key = f"{bk}:upcoming:{sport}"
        r   = _get_cache_redis()
        smart_set(r, key, payload, ttl=TTL_BK_SNAP)
        # Also write the odds:bk:mode:sport key that _BK_KEY_FORMATS in odds_stream checks
        smart_set(r, f"odds:{bk}:upcoming:{sport}", payload, ttl=TTL_BK_SNAP)
        # Snapshot to DB (async-safe — only writes if content changed)
        snapshot_to_db(key, payload)
    except Exception as exc:
        logger.warning("[persist_snapshot] %s/%s: %s — falling back to cache_set", bk, sport, exc)
        # Graceful fallback to original behaviour
        cache_set(f"{bk}:upcoming:{sport}", payload, ttl=3600)


# ══════════════════════════════════════════════════════════════════════════════
# SPORTPESA
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.sp.harvest_page",
    bind=True, max_retries=2, default_retry_delay=15,
    soft_time_limit=3600, time_limit=9000, acks_late=True,
)
def sp_harvest_page(self, sport_slug: str, page: int,
                    page_size: int = HARVEST_PAGE_SIZE) -> dict:
    t0      = time.perf_counter()
    matches: list[dict] = []
    try:
        from app.workers.sp_harvester import fetch_upcoming_page
        matches = fetch_upcoming_page(sport_slug, page=page, page_size=page_size,
                                      fetch_full_markets=True)
    except (AttributeError, ImportError):
        matches = _sp_stream_slice(sport_slug, page, page_size)
    except Exception as exc:
        raise self.retry(exc=exc, countdown=10)

    done    = publish_page("sp", "upcoming", sport_slug, page, matches, HARVEST_N_PAGES)
    latency = int((time.perf_counter() - t0) * 1000)
    logger.info("[sp:page] %s p%d → %d matches (%dms) [%d/%d done]",
                sport_slug, page, len(matches), latency, done, HARVEST_N_PAGES)
    return {"sport": sport_slug, "page": page, "count": len(matches),
            "latency_ms": latency, "pages_done": done}


def _sp_stream_slice(sport_slug: str, page: int, page_size: int) -> list[dict]:
    from app.workers.sp_harvester import fetch_upcoming_stream
    skip   = (page - 1) * page_size
    result: list[dict] = []
    try:
        for match in fetch_upcoming_stream(
            sport_slug, fetch_full_markets=True,
            max_matches=page_size, offset=skip, days=OD_DAYS_AHEAD, sleep_between=0.05,
        ):
            result.append(match)
    except Exception as exc:
        logger.warning("[sp:stream_slice] %s p%d: %s", sport_slug, page, exc)
    return result


@celery.task(
    name="tasks.sp.merge_pages",
    bind=True, max_retries=200, default_retry_delay=15,
    soft_time_limit=3600, time_limit=9000, acks_late=True,
)
def sp_merge_pages(self, sport_slug: str, expected_pages: int = HARVEST_N_PAGES,
                   attempt: int = 0) -> dict:
    from app.workers.tasks_upcoming import _persist_bk_matches, _upsert_and_chain

    done         = pages_done_count("sp", "upcoming", sport_slug)
    min_required = max(1, int(expected_pages * 0.6))

    if done < min_required and attempt < 150:
        raise self.retry(
            kwargs={"sport_slug": sport_slug, "expected_pages": expected_pages,
                    "attempt": attempt + 1},
            countdown=15,
        )

    t0          = time.perf_counter()
    all_matches = merge_pages("sp", "upcoming", sport_slug, expected_pages)

    if not all_matches:
        return {"ok": False, "reason": "empty", "sport": sport_slug}

    br_count    = sum(1 for m in all_matches if m.get("betradar_id"))
    avg_markets = _avg_markets(all_matches)

    publish_snapshot("sp", "upcoming", sport_slug, all_matches, meta={
        "source":      "sportpesa",
        "br_count":    br_count,
        "avg_markets": avg_markets,
    })

    # PATCHED: was cache_set(..., ttl=3600)
    _persist_snapshot("sp", sport_slug, {
        "source":       "sportpesa",
        "sport":        sport_slug,
        "mode":         "upcoming",
        "match_count":  len(all_matches),
        "harvested_at": _now_iso(),
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
        "matches":      all_matches,
        "avg_markets":  avg_markets,
        "br_count":     br_count,
    })

    _upsert_and_chain(all_matches, "SportPesa")
    _persist_bk_matches(all_matches, "sp", sport_slug)

    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": "sportpesa",
        "sport": sport_slug, "mode": "upcoming",
        "count": len(all_matches), "ts": _now_iso(),
    })

    if br_count > 0:
        celery.send_task("tasks.sp.cross_bk_enrich", args=[sport_slug],
                         queue="harvest", countdown=10)
        celery.send_task("tasks.bt_od.harvest_sport", args=[sport_slug],
                         queue="harvest", countdown=30)

    celery.send_task("tasks.sp.enrich_analytics", args=[sport_slug],
                     queue="harvest", countdown=60)
    celery.send_task("tasks.align.sport", args=[sport_slug, 100],
                     queue="results", countdown=90)

    latency = int((time.perf_counter() - t0) * 1000)
    logger.info("[sp:merge] %s → %d matches, %d br_ids, %dms",
                sport_slug, len(all_matches), br_count, latency)
    return {"ok": True, "sport": sport_slug, "count": len(all_matches),
            "br_count": br_count, "avg_markets": avg_markets, "latency_ms": latency}


@celery.task(
    name="tasks.sp.harvest_sport_paged",
    bind=True, max_retries=1, default_retry_delay=60,
    soft_time_limit=3600, time_limit=9000, acks_late=True,
)
def sp_harvest_sport_paged(self, sport_slug: str,
                            n_pages: int = HARVEST_N_PAGES,
                            page_size: int = HARVEST_PAGE_SIZE) -> dict:
    from celery import group as cgroup
    sigs = [sp_harvest_page.s(sport_slug, p, page_size) for p in range(1, n_pages + 1)]
    cgroup(sigs).apply_async(queue="harvest")
    sp_merge_pages.apply_async(
        args=[sport_slug, n_pages, 0], queue="results", countdown=MERGE_COUNTDOWN,
    )
    logger.info("[sp:paged] %s → %d page tasks dispatched", sport_slug, n_pages)
    return {"sport": sport_slug, "pages_dispatched": n_pages}


@celery.task(name="tasks.sp.harvest_all_paged", soft_time_limit=3600, time_limit=9000)
def sp_harvest_all_paged() -> dict:
    from celery import group as cgroup
    sigs = [sp_harvest_sport_paged.s(s) for s in _SP_SPORTS]
    cgroup(sigs).apply_async(queue="harvest")
    logger.info("[sp:all_paged] dispatched %d sports", len(_SP_SPORTS))
    return {"dispatched": len(_SP_SPORTS), "sports": _SP_SPORTS}


# ══════════════════════════════════════════════════════════════════════════════
# BETIKA
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.bt.harvest_page",
    bind=True, max_retries=2, default_retry_delay=15,
    soft_time_limit=3600, time_limit=9000, acks_late=True,
)
def bt_harvest_page(self, sport_slug: str, page: int,
                    page_size: int = HARVEST_PAGE_SIZE) -> dict:
    t0 = time.perf_counter()
    try:
        from app.workers.bt_harvester import fetch_upcoming_stream
        offset  = (page - 1) * page_size
        matches = []
        for match in fetch_upcoming_stream(
            sport_slug=sport_slug, days=OD_DAYS_AHEAD,
            max_matches=page_size, offset=offset, fetch_full_markets=False,
        ):
            matches.append(match)
    except Exception as exc:
        raise self.retry(exc=exc, countdown=10)

    done    = publish_page("bt", "upcoming", sport_slug, page, matches, HARVEST_N_PAGES)
    latency = int((time.perf_counter() - t0) * 1000)
    return {"sport": sport_slug, "page": page, "count": len(matches),
            "latency_ms": latency, "pages_done": done}


@celery.task(
    name="tasks.bt.merge_pages",
    bind=True, max_retries=200, default_retry_delay=15,
    soft_time_limit=3600, time_limit=9000, acks_late=True,
)
def bt_merge_pages(self, sport_slug: str, expected_pages: int = HARVEST_N_PAGES,
                   attempt: int = 0) -> dict:
    from app.workers.tasks_upcoming import _persist_bk_matches, _upsert_and_chain

    done         = pages_done_count("bt", "upcoming", sport_slug)
    min_required = max(1, int(expected_pages * 0.6))

    if done < min_required and attempt < 150:
        raise self.retry(
            kwargs={"sport_slug": sport_slug, "expected_pages": expected_pages,
                    "attempt": attempt + 1},
            countdown=15,
        )

    t0          = time.perf_counter()
    all_matches = merge_pages("bt", "upcoming", sport_slug, expected_pages)
    if not all_matches:
        return {"ok": False, "reason": "empty", "sport": sport_slug}

    avg_markets = _avg_markets(all_matches)
    publish_snapshot("bt", "upcoming", sport_slug, all_matches,
                     meta={"source": "betika", "avg_markets": avg_markets})

    # PATCHED: was cache_set(..., ttl=3600)
    _persist_snapshot("bt", sport_slug, {
        "source":       "betika",
        "sport":        sport_slug,
        "mode":         "upcoming",
        "match_count":  len(all_matches),
        "harvested_at": _now_iso(),
        "matches":      all_matches,
        "avg_markets":  avg_markets,
    })

    _upsert_and_chain(all_matches, "Betika")
    _persist_bk_matches(all_matches, "bt", sport_slug)
    celery.send_task("tasks.align.sport", args=[sport_slug, 100],
                     queue="results", countdown=60)

    latency = int((time.perf_counter() - t0) * 1000)
    return {"ok": True, "sport": sport_slug, "count": len(all_matches), "latency_ms": latency}


@celery.task(
    name="tasks.bt.harvest_sport_paged",
    bind=True, max_retries=1, default_retry_delay=60,
    soft_time_limit=3600, time_limit=9000, acks_late=True,
)
def bt_harvest_sport_paged(self, sport_slug: str,
                            n_pages: int = HARVEST_N_PAGES,
                            page_size: int = HARVEST_PAGE_SIZE) -> dict:
    from celery import group as cgroup
    sigs = [bt_harvest_page.s(sport_slug, p, page_size) for p in range(1, n_pages + 1)]
    cgroup(sigs).apply_async(queue="harvest")
    bt_merge_pages.apply_async(
        args=[sport_slug, n_pages, 0], queue="results", countdown=MERGE_COUNTDOWN,
    )
    return {"sport": sport_slug, "pages_dispatched": n_pages}


@celery.task(name="tasks.bt.harvest_all_paged", soft_time_limit=3600, time_limit=9000)
def bt_harvest_all_paged() -> dict:
    from celery import group as cgroup
    sigs = [bt_harvest_sport_paged.s(s) for s in _BT_SPORTS]
    cgroup(sigs).apply_async(queue="harvest")
    return {"dispatched": len(_BT_SPORTS), "sports": _BT_SPORTS}


# ══════════════════════════════════════════════════════════════════════════════
# ODIBETS
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.od.harvest_date_chunk",
    bind=True, max_retries=2, default_retry_delay=15,
    soft_time_limit=3600, time_limit=9000, acks_late=True,
)
def od_harvest_date_chunk(self, sport_slug: str, dates: list[str], chunk_idx: int) -> dict:
    from app.workers.od_harvester import _probe, _fetch_day_complete, _normalise
    t0 = time.perf_counter()
    matches: list[dict] = []
    seen: set[str] = set()
    api_id = _probe(sport_slug)
    for day in dates:
        try:
            raw_list = _fetch_day_complete(api_id, day)
            for r in raw_list:
                m = _normalise(r, sport_slug)
                if not m:
                    continue
                mid = m.get("od_match_id") or m.get("od_event_id")
                if mid and mid not in seen:
                    seen.add(mid)
                    matches.append(m)
        except Exception as exc:
            logger.warning("[od:chunk] %s day=%s: %s", sport_slug, day, exc)

    done    = publish_page("od", "upcoming", sport_slug, chunk_idx, matches, HARVEST_N_PAGES)
    latency = int((time.perf_counter() - t0) * 1000)
    return {"sport": sport_slug, "chunk": chunk_idx, "count": len(matches), "latency_ms": latency}


@celery.task(
    name="tasks.od.merge_pages",
    bind=True, max_retries=200, default_retry_delay=15,
    soft_time_limit=3600, time_limit=9000, acks_late=True,
)
def od_merge_pages(self, sport_slug: str, expected_pages: int = HARVEST_N_PAGES,
                   attempt: int = 0) -> dict:
    from app.workers.tasks_upcoming import _persist_bk_matches, _upsert_and_chain

    done         = pages_done_count("od", "upcoming", sport_slug)
    min_required = max(1, int(expected_pages * 0.6))

    if done < min_required and attempt < 150:
        raise self.retry(
            kwargs={"sport_slug": sport_slug, "expected_pages": expected_pages,
                    "attempt": attempt + 1},
            countdown=15,
        )

    t0          = time.perf_counter()
    all_matches = merge_pages("od", "upcoming", sport_slug, expected_pages)
    if not all_matches:
        return {"ok": False, "reason": "empty", "sport": sport_slug}

    avg_markets = _avg_markets(all_matches)
    publish_snapshot("od", "upcoming", sport_slug, all_matches,
                     meta={"source": "odibets", "avg_markets": avg_markets})

    # PATCHED: was cache_set(..., ttl=3600)
    _persist_snapshot("od", sport_slug, {
        "source":       "odibets",
        "sport":        sport_slug,
        "mode":         "upcoming",
        "match_count":  len(all_matches),
        "harvested_at": _now_iso(),
        "matches":      all_matches,
        "avg_markets":  avg_markets,
    })

    _upsert_and_chain(all_matches, "OdiBets")
    _persist_bk_matches(all_matches, "od", sport_slug)
    celery.send_task("tasks.align.sport", args=[sport_slug, 100],
                     queue="results", countdown=60)

    latency = int((time.perf_counter() - t0) * 1000)
    return {"ok": True, "sport": sport_slug, "count": len(all_matches), "latency_ms": latency}


@celery.task(
    name="tasks.od.harvest_sport_paged",
    bind=True, max_retries=1, default_retry_delay=60,
    soft_time_limit=3600, time_limit=9000, acks_late=True,
)
def od_harvest_sport_paged(self, sport_slug: str,
                            days_ahead: int = OD_DAYS_AHEAD,
                            n_chunks: int = HARVEST_N_PAGES) -> dict:
    from celery import group as cgroup
    today      = date.today()
    all_dates  = [(today + timedelta(days=i)).isoformat() for i in range(days_ahead)]
    chunk_size = max(1, len(all_dates) // n_chunks)
    chunks     = [all_dates[i:i + chunk_size]
                  for i in range(0, len(all_dates), chunk_size)][:n_chunks]
    sigs = [od_harvest_date_chunk.s(sport_slug, chunk, idx + 1)
            for idx, chunk in enumerate(chunks)]
    cgroup(sigs).apply_async(queue="harvest")
    od_merge_pages.apply_async(
        args=[sport_slug, len(chunks), 0],
        queue="results",
        countdown=MERGE_COUNTDOWN + 15,
    )
    return {"sport": sport_slug, "chunks_dispatched": len(chunks)}


@celery.task(name="tasks.od.harvest_all_paged", soft_time_limit=3600, time_limit=6000)
def od_harvest_all_paged() -> dict:
    from celery import group as cgroup
    sigs = [od_harvest_sport_paged.s(s) for s in _OD_SPORTS]
    cgroup(sigs).apply_async(queue="harvest")
    return {"dispatched": len(_OD_SPORTS), "sports": _OD_SPORTS}


# ══════════════════════════════════════════════════════════════════════════════
# MASTER ORCHESTRATOR (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(name="tasks.harvest.all_paged", soft_time_limit=3600, time_limit=9000)
def harvest_all_paged() -> dict:
    from app.workers.tasks_harvest_b2b import b2b_harvest_all_paged

    sp_harvest_all_paged.apply_async(queue="harvest", countdown=0)
    bt_harvest_all_paged.apply_async(queue="harvest", countdown=5)
    od_harvest_all_paged.apply_async(queue="harvest", countdown=10)
    b2b_harvest_all_paged.apply_async(queue="harvest", countdown=15)

    logger.info("[harvest:all_paged] SP+BT+OD+B2B(×7) dispatched")
    return {"ok": True, "bks": ["sp", "bt", "od", "b2b×7"], "sports": _ALL_SPORTS}


# ── Helpers (unchanged) ───────────────────────────────────────────────────────

def _avg_markets(matches: list[dict]) -> int:
    if not matches:
        return 0
    return int(sum(m.get("market_count", 0) for m in matches) / len(matches))