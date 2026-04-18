"""
app/workers/tasks_harvest_pages.py
====================================
Page-level Celery tasks for parallel harvest of all bookmakers.

Architecture
─────────────
  [beat every 5 min]
  sp_harvest_all_paged()  →  for each sport: sp_harvest_sport_paged(sport)
  bt_harvest_all_paged()  →  for each sport: bt_harvest_sport_paged(sport)
  od_harvest_all_paged()  →  for each sport: od_harvest_sport_paged(sport)

  sp_harvest_sport_paged(sport)
    → dispatches sp_harvest_page(sport, 1..10)  [10 workers × 100 = 1000 records]
    → schedules  sp_merge_pages(sport, countdown=50s)

  sp_harvest_page(sport, page)          ← runs in parallel on "harvest" queue
    → fetches 100 records from SP API
    → saves to Redis key: odds:sp:upcoming:{sport}:page:{page}
    → increments atomic counter: odds:sp:upcoming:{sport}:pages_done
    → publishes to channel: odds:sp:upcoming:{sport}:updates

  sp_merge_pages(sport)                 ← runs on "results" queue after ~50 s
    → reads all page keys from Redis
    → merges + deduplicates
    → saves snapshot: odds:sp:upcoming:{sport}
    → publishes snapshot_ready on: odds:sp:upcoming:{sport}:updates
                                   odds:all:upcoming:{sport}:updates
    → updates legacy cache key (backward compat)
    → triggers downstream: persist, align, bt_od

Same pattern for BT (real page params) and OD (date-chunk based).

All sports harvested
─────────────────────
  _ALL_SPORTS (15 sports) — union of SP + BT + OD supported sports
"""

from __future__ import annotations

import time
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

from celery.utils.log import get_task_logger

from app.workers.celery_tasks import celery, cache_set, _now_iso, _publish
from app.workers.redis_bus import (
    publish_page, publish_snapshot, merge_pages, pages_done_count,
)

logger = get_task_logger(__name__)

# ─── Configuration ─────────────────────────────────────────────────────────────

HARVEST_PAGE_SIZE = 100   # records per page
HARVEST_N_PAGES   = 10    # pages per sport  →  1 000 records max per BK per sport
MERGE_COUNTDOWN   = 55    # seconds after page dispatch before merge runs
OD_DAYS_AHEAD     = 30    # days of upcoming matches to fetch from OD

WS_CHANNEL = "odds:updates"

# ─── All supported sports (union of SP + BT + OD) ─────────────────────────────

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

# Sports each BK actually supports (subset of _ALL_SPORTS)
_SP_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
]
_BT_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
    "darts", "handball",
]
_OD_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "boxing",
    "handball", "mma", "table-tennis", "darts",
    "american-football", "esoccer",
]


# ══════════════════════════════════════════════════════════════════════════════
# SPORTPESA  —  page-based harvest
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.sp.harvest_page",
    bind=True, max_retries=2, default_retry_delay=15,
    soft_time_limit=180, time_limit=210, acks_late=True,
)
def sp_harvest_page(self, sport_slug: str, page: int,
                    page_size: int = HARVEST_PAGE_SIZE) -> dict:
    """
    Fetch one page (100 records) from SP and publish to Redis.
    Falls back to slicing the stream generator if fetch_upcoming_page
    is not available in sp_harvester.
    """
    t0 = time.perf_counter()
    matches: list[dict] = []

    try:
        # Try direct page function first (fastest)
        from app.workers.sp_harvester import fetch_upcoming_page
        matches = fetch_upcoming_page(
            sport_slug, page=page, page_size=page_size, fetch_full_markets=True,
        )
    except (AttributeError, ImportError):
        # Fallback: stream with offset
        matches = _sp_stream_slice(sport_slug, page, page_size)
    except Exception as exc:
        logger.warning("[sp:page] %s p%d: %s", sport_slug, page, exc)
        raise self.retry(exc=exc, countdown=10)

    done = publish_page("sp", "upcoming", sport_slug, page, matches, HARVEST_N_PAGES)
    latency = int((time.perf_counter() - t0) * 1000)
    logger.info("[sp:page] %s p%d → %d matches (%dms) [%d/%d done]",
                sport_slug, page, len(matches), latency, done, HARVEST_N_PAGES)
    return {"sport": sport_slug, "page": page, "count": len(matches),
            "latency_ms": latency, "pages_done": done}


def _sp_stream_slice(sport_slug: str, page: int, page_size: int) -> list[dict]:
    """
    Fallback page slice via the SP stream generator.
    Skips (page-1)*page_size items then takes page_size.
    NOTE: This re-fetches earlier pages, so page 10 is slowest.
          Add fetch_upcoming_page() to sp_harvester to avoid this.
    """
    from app.workers.sp_harvester import fetch_upcoming_stream
    skip   = (page - 1) * page_size
    taken  = 0
    result: list[dict] = []
    try:
        for match in fetch_upcoming_stream(
            sport_slug, fetch_full_markets=True,
            max_matches=skip + page_size, days=OD_DAYS_AHEAD, sleep_between=0.05,
        ):
            if taken < skip:
                taken += 1
                continue
            result.append(match)
            taken += 1
            if len(result) >= page_size:
                break
    except Exception as exc:
        logger.warning("[sp:stream_slice] %s p%d: %s", sport_slug, page, exc)
    return result


@celery.task(
    name="tasks.sp.merge_pages",
    bind=True, max_retries=5, default_retry_delay=15,
    soft_time_limit=180, time_limit=210, acks_late=True,
)
def sp_merge_pages(self, sport_slug: str, expected_pages: int = HARVEST_N_PAGES,
                   attempt: int = 0) -> dict:
    """
    Merge all SP page keys into one snapshot after pages are ready.
    Retries up to 5×15 s if pages are still in flight.
    """
    from app.workers.tasks_upcoming import _persist_bk_matches, _upsert_and_chain

    done = pages_done_count("sp", "upcoming", sport_slug)
    min_required = max(1, int(expected_pages * 0.6))   # accept 60% pages

    if done < min_required and attempt < 4:
        logger.info("[sp:merge] %s waiting (%d/%d pages), retry %d",
                    sport_slug, done, expected_pages, attempt + 1)
        raise self.retry(
            kwargs={"sport_slug": sport_slug, "expected_pages": expected_pages,
                    "attempt": attempt + 1},
            countdown=15,
        )

    t0 = time.perf_counter()
    all_matches = merge_pages("sp", "upcoming", sport_slug, expected_pages)

    if not all_matches:
        logger.warning("[sp:merge] %s — no matches after merge", sport_slug)
        return {"ok": False, "reason": "empty", "sport": sport_slug}

    br_count    = sum(1 for m in all_matches if m.get("betradar_id"))
    avg_markets = _avg_markets(all_matches)

    # ── Redis snapshot (primary store) ──────────────────────────────────────
    publish_snapshot("sp", "upcoming", sport_slug, all_matches, meta={
        "source":      "sportpesa",
        "br_count":    br_count,
        "avg_markets": avg_markets,
    })

    # ── Legacy cache key (backward compat with existing code) ───────────────
    cache_set(f"sp:upcoming:{sport_slug}", {
        "source":       "sportpesa",
        "sport":        sport_slug,
        "mode":         "upcoming",
        "match_count":  len(all_matches),
        "harvested_at": _now_iso(),
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
        "matches":      all_matches,
        "avg_markets":  avg_markets,
        "br_count":     br_count,
    }, ttl=3600)

    # ── Downstream pipeline ──────────────────────────────────────────────────
    _upsert_and_chain(all_matches, "SportPesa")
    _persist_bk_matches(all_matches, "sp", sport_slug)

    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": "sportpesa",
        "sport": sport_slug, "mode": "upcoming",
        "count": len(all_matches), "ts": _now_iso(),
    })

    if br_count > 0:
        celery.send_task("tasks.sp.cross_bk_enrich",
                         args=[sport_slug], queue="harvest", countdown=10)
        celery.send_task("tasks.bt_od.harvest_sport",
                         args=[sport_slug], queue="harvest", countdown=30)

    celery.send_task("tasks.sp.enrich_analytics",
                     args=[sport_slug], queue="harvest", countdown=60)
    celery.send_task("tasks.align.sport",
                     args=[sport_slug, 100], queue="results", countdown=90)

    latency = int((time.perf_counter() - t0) * 1000)
    logger.info("[sp:merge] %s → %d matches, %d br_ids, %dms",
                sport_slug, len(all_matches), br_count, latency)
    return {
        "ok":          True,
        "sport":       sport_slug,
        "count":       len(all_matches),
        "br_count":    br_count,
        "avg_markets": avg_markets,
        "latency_ms":  latency,
    }


@celery.task(
    name="tasks.sp.harvest_sport_paged",
    bind=True, max_retries=1, default_retry_delay=60,
    soft_time_limit=60, time_limit=90, acks_late=True,
)
def sp_harvest_sport_paged(self, sport_slug: str,
                            n_pages: int = HARVEST_N_PAGES,
                            page_size: int = HARVEST_PAGE_SIZE) -> dict:
    """Dispatch N page tasks then schedule merge."""
    from celery import group as cgroup
    sigs = [sp_harvest_page.s(sport_slug, p, page_size) for p in range(1, n_pages + 1)]
    cgroup(sigs).apply_async(queue="harvest")
    sp_merge_pages.apply_async(
        args=[sport_slug, n_pages, 0],
        queue="results",
        countdown=MERGE_COUNTDOWN,
    )
    logger.info("[sp:paged] %s → %d page tasks dispatched", sport_slug, n_pages)
    return {"sport": sport_slug, "pages_dispatched": n_pages}


@celery.task(name="tasks.sp.harvest_all_paged", soft_time_limit=30, time_limit=60)
def sp_harvest_all_paged() -> dict:
    """Beat task (every 5 min): dispatch paged SP harvest for all sports."""
    from celery import group as cgroup
    sigs = [sp_harvest_sport_paged.s(s) for s in _SP_SPORTS]
    cgroup(sigs).apply_async(queue="harvest")
    logger.info("[sp:all_paged] dispatched %d sports", len(_SP_SPORTS))
    return {"dispatched": len(_SP_SPORTS), "sports": _SP_SPORTS}


# ══════════════════════════════════════════════════════════════════════════════
# BETIKA  —  page-based harvest (native page/limit API)
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.bt.harvest_page",
    bind=True, max_retries=2, default_retry_delay=15,
    soft_time_limit=60, time_limit=90, acks_late=True,
)
def bt_harvest_page(self, sport_slug: str, page: int,
                    page_size: int = HARVEST_PAGE_SIZE) -> dict:
    """Fetch one page of BT upcoming matches and publish to Redis."""
    import httpx
    from app.workers.bt_harvester import (
        UPCOMING_URL, HEADERS,
        _normalise_match as _bt_norm,
        slug_to_bt_sport_id,
    )
    t0 = time.perf_counter()

    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    params = {
        "page":        page,
        "limit":       page_size,
        "tab":         "upcoming",
        "sub_type_id": "1,10,11,18,29,60,186,219,251,340,406",
        "sport_id":    bt_sport_id,
        "sort_id":     2,
        "period_id":   9,
        "esports":     "false",
    }

    try:
        r = httpx.get(UPCOMING_URL, params=params, headers=HEADERS, timeout=12.0)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        logger.warning("[bt:page] %s p%d: %s", sport_slug, page, exc)
        raise self.retry(exc=exc, countdown=10)

    raw_list = (data or {}).get("data") or []
    matches  = [m for m in (_bt_norm(r, source="upcoming") for r in raw_list) if m]

    done = publish_page("bt", "upcoming", sport_slug, page, matches, HARVEST_N_PAGES)
    latency = int((time.perf_counter() - t0) * 1000)
    logger.info("[bt:page] %s p%d → %d matches (%dms) [%d/%d done]",
                sport_slug, page, len(matches), latency, done, HARVEST_N_PAGES)
    return {"sport": sport_slug, "page": page, "count": len(matches),
            "latency_ms": latency, "pages_done": done}


@celery.task(
    name="tasks.bt.merge_pages",
    bind=True, max_retries=5, default_retry_delay=15,
    soft_time_limit=120, time_limit=150, acks_late=True,
)
def bt_merge_pages(self, sport_slug: str, expected_pages: int = HARVEST_N_PAGES,
                   attempt: int = 0) -> dict:
    """Merge BT pages into snapshot after pages are ready."""
    from app.workers.tasks_upcoming import _persist_bk_matches, _upsert_and_chain

    done        = pages_done_count("bt", "upcoming", sport_slug)
    min_required = max(1, int(expected_pages * 0.6))

    if done < min_required and attempt < 4:
        raise self.retry(
            kwargs={"sport_slug": sport_slug, "expected_pages": expected_pages,
                    "attempt": attempt + 1},
            countdown=15,
        )

    t0          = time.perf_counter()
    all_matches = merge_pages("bt", "upcoming", sport_slug, expected_pages)

    if not all_matches:
        return {"ok": False, "reason": "empty", "sport": sport_slug}

    publish_snapshot("bt", "upcoming", sport_slug, all_matches,
                     meta={"source": "betika", "avg_markets": _avg_markets(all_matches)})

    cache_set(f"bt:upcoming:{sport_slug}", {
        "source":       "betika",
        "sport":        sport_slug,
        "mode":         "upcoming",
        "match_count":  len(all_matches),
        "harvested_at": _now_iso(),
        "matches":      all_matches,
        "enriched":     False,
    }, ttl=3600)

    _upsert_and_chain(all_matches, "Betika")
    _persist_bk_matches(all_matches, "bt", sport_slug)
    celery.send_task("tasks.align.sport", args=[sport_slug, 100],
                     queue="results", countdown=60)

    latency = int((time.perf_counter() - t0) * 1000)
    logger.info("[bt:merge] %s → %d matches %dms", sport_slug, len(all_matches), latency)
    return {"ok": True, "sport": sport_slug, "count": len(all_matches), "latency_ms": latency}


@celery.task(
    name="tasks.bt.harvest_sport_paged",
    bind=True, max_retries=1, default_retry_delay=60,
    soft_time_limit=60, time_limit=90, acks_late=True,
)
def bt_harvest_sport_paged(self, sport_slug: str,
                            n_pages: int = HARVEST_N_PAGES,
                            page_size: int = HARVEST_PAGE_SIZE) -> dict:
    """Dispatch N BT page tasks then schedule merge."""
    from celery import group as cgroup
    sigs = [bt_harvest_page.s(sport_slug, p, page_size) for p in range(1, n_pages + 1)]
    cgroup(sigs).apply_async(queue="harvest")
    bt_merge_pages.apply_async(
        args=[sport_slug, n_pages, 0],
        queue="results",
        countdown=MERGE_COUNTDOWN,
    )
    logger.info("[bt:paged] %s → %d page tasks dispatched", sport_slug, n_pages)
    return {"sport": sport_slug, "pages_dispatched": n_pages}


@celery.task(name="tasks.bt.harvest_all_paged", soft_time_limit=30, time_limit=60)
def bt_harvest_all_paged() -> dict:
    """Beat task (every 5 min): paged BT harvest for all supported sports."""
    from celery import group as cgroup
    sigs = [bt_harvest_sport_paged.s(s) for s in _BT_SPORTS]
    cgroup(sigs).apply_async(queue="harvest")
    logger.info("[bt:all_paged] dispatched %d sports", len(_BT_SPORTS))
    return {"dispatched": len(_BT_SPORTS), "sports": _BT_SPORTS}


# ══════════════════════════════════════════════════════════════════════════════
# ODIBETS  —  date-chunk harvest (OD API is date-based, not page-based)
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.od.harvest_date_chunk",
    bind=True, max_retries=2, default_retry_delay=15,
    soft_time_limit=120, time_limit=150, acks_late=True,
)
def od_harvest_date_chunk(self, sport_slug: str, dates: list[str],
                          chunk_idx: int) -> dict:
    """Fetch OD matches for a list of dates and publish as one page."""
    from app.workers.od_harvester import fetch_upcoming_matches as od_fetch
    t0      = time.perf_counter()
    matches: list[dict] = []
    seen:   set[str]   = set()

    for day in dates:
        try:
            for m in od_fetch(sport_slug, day=day):
                mid = m.get("od_match_id") or m.get("od_event_id")
                if mid and mid in seen:
                    continue
                if mid:
                    seen.add(mid)
                matches.append(m)
        except Exception as exc:
            logger.warning("[od:chunk] %s day=%s: %s", sport_slug, day, exc)

    done    = publish_page("od", "upcoming", sport_slug, chunk_idx, matches, HARVEST_N_PAGES)
    latency = int((time.perf_counter() - t0) * 1000)
    logger.info("[od:chunk] %s chunk%d → %d matches (%dms) [%d/%d done]",
                sport_slug, chunk_idx, len(matches), latency, done, HARVEST_N_PAGES)
    return {"sport": sport_slug, "chunk": chunk_idx, "count": len(matches),
            "latency_ms": latency}


@celery.task(
    name="tasks.od.merge_pages",
    bind=True, max_retries=5, default_retry_delay=15,
    soft_time_limit=120, time_limit=150, acks_late=True,
)
def od_merge_pages(self, sport_slug: str, expected_pages: int = HARVEST_N_PAGES,
                   attempt: int = 0) -> dict:
    """Merge OD date-chunks into snapshot after all chunks are ready."""
    from app.workers.tasks_upcoming import _persist_bk_matches, _upsert_and_chain

    done         = pages_done_count("od", "upcoming", sport_slug)
    min_required = max(1, int(expected_pages * 0.6))

    if done < min_required and attempt < 4:
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

    cache_set(f"od:upcoming:{sport_slug}", {
        "source":       "odibets",
        "sport":        sport_slug,
        "mode":         "upcoming",
        "match_count":  len(all_matches),
        "harvested_at": _now_iso(),
        "matches":      all_matches,
        "avg_markets":  avg_markets,
    }, ttl=3600)

    _upsert_and_chain(all_matches, "OdiBets")
    _persist_bk_matches(all_matches, "od", sport_slug)
    celery.send_task("tasks.align.sport", args=[sport_slug, 100],
                     queue="results", countdown=60)

    latency = int((time.perf_counter() - t0) * 1000)
    logger.info("[od:merge] %s → %d matches %dms", sport_slug, len(all_matches), latency)
    return {"ok": True, "sport": sport_slug, "count": len(all_matches), "latency_ms": latency}


@celery.task(
    name="tasks.od.harvest_sport_paged",
    bind=True, max_retries=1, default_retry_delay=60,
    soft_time_limit=60, time_limit=90, acks_late=True,
)
def od_harvest_sport_paged(self, sport_slug: str,
                            days_ahead: int = OD_DAYS_AHEAD,
                            n_chunks: int = HARVEST_N_PAGES) -> dict:
    """
    Split upcoming N days into chunks, dispatch date-chunk tasks in parallel.
    10 chunks × 3 days each = 30 days of OD data fetched in parallel.
    """
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
        countdown=MERGE_COUNTDOWN + 15,   # OD is slightly slower
    )
    logger.info("[od:paged] %s → %d date-chunks dispatched", sport_slug, len(chunks))
    return {"sport": sport_slug, "chunks_dispatched": len(chunks)}


@celery.task(name="tasks.od.harvest_all_paged", soft_time_limit=30, time_limit=60)
def od_harvest_all_paged() -> dict:
    """Beat task (every 5 min): paged OD harvest for all supported sports."""
    from celery import group as cgroup
    sigs = [od_harvest_sport_paged.s(s) for s in _OD_SPORTS]
    cgroup(sigs).apply_async(queue="harvest")
    logger.info("[od:all_paged] dispatched %d sports", len(_OD_SPORTS))
    return {"dispatched": len(_OD_SPORTS), "sports": _OD_SPORTS}


# ══════════════════════════════════════════════════════════════════════════════
# COMBINED ALL-BK ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(name="tasks.harvest.all_paged", soft_time_limit=60, time_limit=90)
def harvest_all_paged() -> dict:
    """
    Master beat task: dispatch SP + BT + OD paged harvests for all sports.
    Called every 5 minutes from setup_periodic_tasks.
    """
    sp_harvest_all_paged.apply_async(queue="harvest", countdown=0)
    bt_harvest_all_paged.apply_async(queue="harvest", countdown=5)
    od_harvest_all_paged.apply_async(queue="harvest", countdown=10)
    logger.info("[harvest:all_paged] SP+BT+OD paged harvests dispatched")
    return {"ok": True, "bks": ["sp", "bt", "od"], "sports": _ALL_SPORTS}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _avg_markets(matches: list[dict]) -> int:
    if not matches:
        return 0
    return int(sum(m.get("market_count", 0) for m in matches) / len(matches))