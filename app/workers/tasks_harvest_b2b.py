"""
app/workers/tasks_harvest_b2b.py
==================================
Celery tasks for parallel B2B bookmaker harvest.

Architecture (mirrors SP/BT/OD pattern):
  beat every 5min
    b2b_harvest_all_paged()
      → for each sport: b2b_harvest_sport_paged(sport)
          → dispatches b2b_harvest_bk_sport(bk_slug, sport) × 7 in parallel
          → schedules  b2b_merge_sport(sport, countdown=70s)

  b2b_harvest_bk_sport(bk_slug, sport)
    → fetch_single_bk() → all pages for that BK
    → saves:  odds:b2b:{bk_slug}:upcoming:{sport}
    → incr:   odds:b2b:upcoming:{sport}:bks_done
    → pub:    odds:b2b:{bk_slug}:upcoming:{sport}:updates

  b2b_merge_sport(sport)
    → merges all 7 BK snapshots with fuzzy cross-BK matching
    → saves:  odds:b2b:upcoming:{sport}  (unified)
    → pub:    odds:all:upcoming:{sport}:updates
    → triggers: persist, align
"""
from __future__ import annotations

import json
import time
from celery.utils.log import get_task_logger

from app.workers.celery_tasks import celery, cache_set, _now_iso, _publish
from app.workers.b2b_harvester import (
    B2B_BOOKMAKERS, B2B_SUPPORTED_SPORTS,
    fetch_single_bk, merge_b2b_by_match,
)

logger = get_task_logger(__name__)

WS_CHANNEL = "odds:updates"
_B2B_SLUGS = [bk["slug"] for bk in B2B_BOOKMAKERS]   # 7 BKs

# ─── Config ───────────────────────────────────────────────────────────────────

B2B_BKS_REQUIRED_FRAC = 0.5   # accept merge if ≥ 50% BKs responded
B2B_MERGE_COUNTDOWN   = 70    # seconds after dispatch before merge
B2B_PAGE_SIZE         = 200   # BetB2B returns up to 500; we cap at 200 per page
B2B_N_PAGES           = 5     # pages per BK per sport = 1 000 max


# ─── Per-BK sport harvest task ────────────────────────────────────────────────

@celery.task(
    name="tasks.b2b.harvest_bk_sport",
    bind=True, max_retries=2, default_retry_delay=15,
    soft_time_limit=120, time_limit=150, acks_late=True,
)
def b2b_harvest_bk_sport(
    self,
    bk_slug: str,
    sport_slug: str,
    mode: str = "upcoming",
    page_size: int = B2B_PAGE_SIZE,
    n_pages: int = B2B_N_PAGES,
) -> dict:
    """
    Fetch all pages from a single B2B bookmaker for one sport.
    Saves snapshot and increments done counter.
    """
    from app.workers.celery_tasks import _redis
    from app.workers.b2b_harvester import get_bk_by_slug

    bk = get_bk_by_slug(bk_slug)
    if not bk:
        return {"ok": False, "reason": "unknown_bk"}

    t0 = time.perf_counter()
    all_matches: list[dict] = []

    # Fetch pages sequentially per BK (BetB2B API is per-domain)
    for page in range(1, n_pages + 1):
        try:
            page_matches = fetch_single_bk(bk, sport_slug, mode, page, page_size)
            all_matches.extend(page_matches)
            if len(page_matches) < page_size:
                break  # fewer than full page → we've got everything
        except Exception as e:
            logger.warning("[b2b:%s] %s p%d error: %s", bk_slug, sport_slug, page, e)

    r         = _redis()
    snap_key  = f"odds:b2b:{bk_slug}:{mode}:{sport_slug}"
    done_key  = f"odds:b2b:{mode}:{sport_slug}:bks_done"
    upd_ch    = f"odds:b2b:{bk_slug}:{mode}:{sport_slug}:updates"
    ttl       = 3600

    pipe = r.pipeline()
    pipe.setex(snap_key, ttl, json.dumps(all_matches, default=str))
    direct_key = f"odds:{bk_slug}:upcoming:{sport_slug}"
    pipe.setex(direct_key, ttl, json.dumps({
    "source": bk_slug,
    "sport": sport_slug,
    "mode": mode,
    "match_count": len(all_matches),
    "harvested_at": _now_iso(),
    "matches": all_matches,
}, default=str))
    pipe.incr(done_key)
    pipe.expire(done_key, 600)
    _, done_count, _ = pipe.execute()

    r.publish(upd_ch, json.dumps({
        "event":   "bk_snapshot_ready",
        "bk":      bk_slug,
        "sport":   sport_slug,
        "mode":    mode,
        "count":   len(all_matches),
        "ts":      _now_iso(),
    }))

    latency = int((time.perf_counter() - t0) * 1000)
    logger.info(
        "[b2b:%s] %s/%s → %d matches %dms [%d/7 done]",
        bk_slug, sport_slug, mode, len(all_matches), latency, int(done_count),
    )
    return {
        "ok":        True,
        "bk":        bk_slug,
        "sport":     sport_slug,
        "count":     len(all_matches),
        "latency_ms": latency,
        "bks_done":  int(done_count),
    }


# ─── Merge task ───────────────────────────────────────────────────────────────

@celery.task(
    name="tasks.b2b.merge_sport",
    bind=True, max_retries=5, default_retry_delay=15,
    soft_time_limit=180, time_limit=210, acks_late=True,
)
def b2b_merge_sport(
    self,
    sport_slug: str,
    mode: str = "upcoming",
    attempt: int = 0,
) -> dict:
    """
    Merge snapshots from all 7 B2B bookmakers into one unified snapshot.
    Retries until ≥50% of BKs have responded.
    """
    from app.workers.celery_tasks import _redis
    from app.workers.tasks_upcoming import _persist_bk_matches, _upsert_and_chain

    r         = _redis()
    done_key  = f"odds:b2b:{mode}:{sport_slug}:bks_done"
    done      = int(r.get(done_key) or 0)
    min_req   = max(1, int(len(B2B_BOOKMAKERS) * B2B_BKS_REQUIRED_FRAC))

    if done < min_req and attempt < 4:
        logger.info("[b2b:merge] %s waiting (%d/%d BKs), retry %d",
                    sport_slug, done, len(B2B_BOOKMAKERS), attempt + 1)
        raise self.retry(
            kwargs={"sport_slug": sport_slug, "mode": mode, "attempt": attempt + 1},
            countdown=15,
        )

    t0 = time.perf_counter()

    # Read all per-BK snapshots
    per_bk: dict[str, list[dict]] = {}
    for bk_slug in _B2B_SLUGS:
        snap_key = f"odds:b2b:{bk_slug}:{mode}:{sport_slug}"
        raw      = r.get(snap_key)
        if raw:
            try:
                per_bk[bk_slug] = json.loads(raw)
            except Exception:
                per_bk[bk_slug] = []

    if not any(per_bk.values()):
        logger.warning("[b2b:merge] %s — no data from any BK", sport_slug)
        return {"ok": False, "reason": "empty", "sport": sport_slug}

    # Merge across bookmakers
    unified = merge_b2b_by_match(per_bk, sport_slug)
    if not unified:
        return {"ok": False, "reason": "merge_empty", "sport": sport_slug}

    # Save unified snapshot
    snap_data = {
        "source":       "b2b",
        "sport":        sport_slug,
        "mode":         mode,
        "match_count":  len(unified),
        "harvested_at": _now_iso(),
        "matches":      unified,
        "bk_counts":    {slug: len(per_bk.get(slug, [])) for slug in _B2B_SLUGS},
    }
    r.setex(f"odds:b2b:{mode}:{sport_slug}", 3600, json.dumps(snap_data, default=str))

    # Legacy key for backward compat
    cache_set(f"b2b:{mode}:{sport_slug}", snap_data, ttl=3600)

    # Notify all subscribers
    _publish(f"odds:all:{mode}:{sport_slug}:updates", {
        "event":  "snapshot_ready",
        "bk":     "b2b",
        "sport":  sport_slug,
        "mode":   mode,
        "count":  len(unified),
        "ts":     _now_iso(),
    })
    _publish(WS_CHANNEL, {
        "event":   "odds_updated",
        "source":  "b2b",
        "sport":   sport_slug,
        "mode":    mode,
        "count":   len(unified),
        "ts":      _now_iso(),
    })

    # Downstream pipeline
    _upsert_and_chain(unified, "B2B")
    _persist_bk_matches(unified, "b2b", sport_slug)

    # Reset done counter for next cycle
    r.delete(done_key)

    latency = int((time.perf_counter() - t0) * 1000)
    logger.info("[b2b:merge] %s → %d unified matches %dms", sport_slug, len(unified), latency)
    return {
        "ok":          True,
        "sport":       sport_slug,
        "count":       len(unified),
        "latency_ms":  latency,
        "bk_counts":   {slug: len(per_bk.get(slug, [])) for slug in _B2B_SLUGS},
    }


# ─── Sport orchestrator task ──────────────────────────────────────────────────

@celery.task(
    name="tasks.b2b.harvest_sport_paged",
    bind=True, max_retries=1, default_retry_delay=60,
    soft_time_limit=60, time_limit=90, acks_late=True,
)
def b2b_harvest_sport_paged(
    self,
    sport_slug: str,
    mode: str = "upcoming",
) -> dict:
    """Dispatch all 7 B2B BK tasks in parallel then schedule merge."""
    from celery import group as cgroup

    sigs = [
        b2b_harvest_bk_sport.s(bk["slug"], sport_slug, mode)
        for bk in B2B_BOOKMAKERS
    ]
    cgroup(sigs).apply_async(queue="harvest")

    b2b_merge_sport.apply_async(
        args=[sport_slug, mode, 0],
        queue="results",
        countdown=B2B_MERGE_COUNTDOWN,
    )

    logger.info("[b2b:paged] %s → %d BK tasks dispatched", sport_slug, len(B2B_BOOKMAKERS))
    return {"sport": sport_slug, "bks_dispatched": len(B2B_BOOKMAKERS)}


# ─── All-sports beat task ─────────────────────────────────────────────────────

@celery.task(
    name="tasks.b2b.harvest_all_paged",
    soft_time_limit=30, time_limit=60,
)
def b2b_harvest_all_paged() -> dict:
    """Beat task (every 5 min): paged B2B harvest for all supported sports."""
    from celery import group as cgroup

    sigs = [b2b_harvest_sport_paged.s(s) for s in B2B_SUPPORTED_SPORTS]
    cgroup(sigs).apply_async(queue="harvest")

    logger.info("[b2b:all_paged] dispatched %d sports", len(B2B_SUPPORTED_SPORTS))
    return {"dispatched": len(B2B_SUPPORTED_SPORTS), "sports": B2B_SUPPORTED_SPORTS}


# ─── Live harvest tasks ───────────────────────────────────────────────────────

_B2B_LIVE_SPORTS = ["soccer", "basketball", "tennis", "ice-hockey", "volleyball", "table-tennis"]


@celery.task(
    name="tasks.b2b.harvest_bk_live",
    bind=True, max_retries=1, default_retry_delay=10,
    soft_time_limit=45, time_limit=60, acks_late=True,
)
def b2b_harvest_bk_live(self, bk_slug: str, sport_slug: str) -> dict:
    """Fetch live matches for one BK and publish immediately."""
    from app.workers.celery_tasks import _redis
    from app.workers.b2b_harvester import get_bk_by_slug
    from app.workers.redis_bus import publish_b2b_live_update

    bk = get_bk_by_slug(bk_slug)
    if not bk:
        return {"ok": False}

    t0 = time.perf_counter()
    try:
        matches = fetch_single_bk(bk, sport_slug, "live", page=1, page_size=500)
    except Exception as e:
        raise self.retry(exc=e, countdown=5)

    if matches:
        publish_b2b_live_update(sport_slug, matches, _redis(), bk_slug=bk_slug)

    ms = int((time.perf_counter() - t0) * 1000)
    return {"ok": True, "bk": bk_slug, "sport": sport_slug, "count": len(matches), "latency_ms": ms}


@celery.task(
    name="tasks.b2b.harvest_all_live",
    soft_time_limit=60, time_limit=90,
)
def b2b_harvest_all_live() -> dict:
    """Beat task (every 90s): live harvest for all B2B BKs."""
    from celery import group as cgroup

    sigs = [
        b2b_harvest_bk_live.s(bk["slug"], sport)
        for bk in B2B_BOOKMAKERS
        for sport in _B2B_LIVE_SPORTS
    ]
    cgroup(sigs).apply_async(queue="harvest")

    count = len(B2B_BOOKMAKERS) * len(_B2B_LIVE_SPORTS)
    logger.info("[b2b:live] dispatched %d tasks", count)
    return {"dispatched": count}