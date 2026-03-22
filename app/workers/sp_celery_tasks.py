"""
app/workers/sp_celery_tasks.py
================================
Celery tasks for Sportpesa football harvesting.

Merge these into your main celery_tasks.py.

Tasks
─────
  harvest_sp_sport(sport_slug, mode)        ← called by beat schedules
  harvest_all_sp_football_upcoming()        ← harvests soccer + esoccer upcoming
  harvest_all_sp_football_live()            ← harvests soccer + esoccer live

Beat schedule (add to setup_periodic_tasks)
─────────────────────────────────────────────
  sp-soccer-upcoming    every 5 min  → harvest_all_sp_football_upcoming
  sp-soccer-live        every 60 sec → harvest_all_sp_football_live

Cache keys written
───────────────────
  sp:upcoming:soccer    { matches, match_count, harvested_at, latency_ms }
  sp:upcoming:esoccer   { matches, match_count, harvested_at, latency_ms }
  sp:live:soccer        { matches, match_count, harvested_at, latency_ms }
  sp:live:esoccer       { matches, match_count, harvested_at, latency_ms }

Redis cache TTLs
─────────────────
  upcoming: 360 s (6 min) — a bit longer than the beat period as safety net
  live:     90  s          — short; live data changes fast

Usage in sp_module.py
──────────────────────
  cached = cache_get("sp:upcoming:soccer")
  matches = cached.get("matches") or []
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from celery import shared_task
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

# ── Sports harvested by this module ───────────────────────────────────────────
# soccer = real football (sportId 1)
# esoccer = eFootball / virtual (sportId 126)
_FOOTBALL_SPORTS = ["soccer", "esoccer"]

# Cache TTLs
_TTL_UPCOMING = 360    # seconds
_TTL_LIVE     = 90     # seconds


# =============================================================================
# Cache helpers (import lazily to avoid circular imports at module load)
# =============================================================================

def _cache_get(key: str):
    try:
        from app.workers.celery_tasks import cache_get   # noqa: PLC0415
        return cache_get(key)
    except ImportError:
        import json
        from app.workers.celery_tasks import _redis      # noqa: PLC0415
        raw = _redis().get(key)
        return json.loads(raw) if raw else None


def _cache_set(key: str, value, ttl: int):
    try:
        from app.workers.celery_tasks import cache_set   # noqa: PLC0415
        cache_set(key, value, ttl=ttl)
    except ImportError:
        import json
        from app.workers.celery_tasks import _redis      # noqa: PLC0415
        _redis().setex(key, ttl, json.dumps(value, default=str))


# =============================================================================
# Core harvest task
# =============================================================================

@shared_task(
    bind=True,
    name="sp.harvest_sport",
    max_retries=2,
    default_retry_delay=30,
    soft_time_limit=600,
    time_limit=660,
    acks_late=True,
    queue="harvest",
)
def harvest_sp_sport(self, sport_slug: str, mode: str = "upcoming") -> dict:
    """
    Harvest one SP sport and write result to Redis.

    Parameters
    ──────────
      sport_slug : "soccer" | "esoccer"
      mode       : "upcoming" | "live"

    Returns summary dict.
    """
    t0 = time.perf_counter()
    logger.info("[sp] Starting %s %s harvest", sport_slug, mode)

    try:
        from app.workers.sp_harvester import fetch_upcoming, fetch_live  # noqa: PLC0415

        if mode == "live":
            matches = fetch_live(sport_slug, fetch_full_markets=True)
        else:
            # Football: 3 days ahead, fetch all 33 markets per match
            # eFootball: shorter window (fast-turnover virtual matches)
            is_virtual = sport_slug in ("esoccer", "efootball")
            matches = fetch_upcoming(
                sport_slug,
                days=2 if is_virtual else 3,
                page_size=30,
                fetch_full_markets=True,
                max_matches=100 if is_virtual else 300,
                sleep_between=0.06 if is_virtual else 0.08,
            )

    except Exception as exc:
        logger.exception("[sp] harvest error %s %s: %s", sport_slug, mode, exc)
        try:
            raise self.retry(exc=exc)
        except self.MaxRetriesExceededError:
            return {"ok": False, "error": str(exc), "sport": sport_slug, "mode": mode}

    latency_ms = int((time.perf_counter() - t0) * 1000)
    harvested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = {
        "source":       "sportpesa",
        "sport":        sport_slug,
        "mode":         mode,
        "match_count":  len(matches),
        "harvested_at": harvested_at,
        "latency_ms":   latency_ms,
        "matches":      matches,
    }

    cache_key = f"sp:{mode}:{sport_slug}"
    ttl = _TTL_LIVE if mode == "live" else _TTL_UPCOMING
    _cache_set(cache_key, payload, ttl=ttl)

    logger.info(
        "[sp] %s %s → %d matches in %dms (key=%s ttl=%ds)",
        sport_slug, mode, len(matches), latency_ms, cache_key, ttl,
    )

    # Emit SSE event so the frontend auto-refreshes
    try:
        from app.workers.celery_tasks import emit_sse_event  # noqa: PLC0415
        emit_sse_event("odds_updated", {"source": "sportpesa", "sport": sport_slug, "mode": mode})
    except Exception:
        pass

    return {
        "ok":           True,
        "sport":        sport_slug,
        "mode":         mode,
        "match_count":  len(matches),
        "latency_ms":   latency_ms,
        "harvested_at": harvested_at,
    }


# =============================================================================
# Group tasks (called by beat)
# =============================================================================

@shared_task(
    name="sp.harvest_all_football_upcoming",
    queue="harvest",
    soft_time_limit=900,
    time_limit=960,
)
def harvest_all_sp_football_upcoming():
    """
    Harvest upcoming soccer + esoccer in sequence.
    Called by the beat every 5 minutes.
    """
    results = []
    for sport in _FOOTBALL_SPORTS:
        result = harvest_sp_sport.apply(args=[sport, "upcoming"])
        results.append(result.get())
    return results


@shared_task(
    name="sp.harvest_all_football_live",
    queue="live",
    soft_time_limit=120,
    time_limit=150,
)
def harvest_all_sp_football_live():
    """
    Harvest live soccer + esoccer in sequence.
    Called by the beat every 60 seconds.
    """
    results = []
    for sport in _FOOTBALL_SPORTS:
        result = harvest_sp_sport.apply(args=[sport, "live"])
        results.append(result.get())
    return results


# =============================================================================
# Beat schedule snippet — merge into your setup_periodic_tasks()
# =============================================================================
# In your celery_tasks.py:
#
#   from app.workers.sp_celery_tasks import (
#       harvest_all_sp_football_upcoming,
#       harvest_all_sp_football_live,
#   )
#
#   @app.on_after_finalize.connect
#   def setup_periodic_tasks(sender, **kwargs):
#       ...existing tasks...
#
#       # SP Football + eFootball — upcoming every 5 min
#       sender.add_periodic_task(
#           300.0,
#           harvest_all_sp_football_upcoming.s(),
#           name="sp-football-upcoming-5min",
#       )
#       # SP Football + eFootball — live every 60 sec
#       sender.add_periodic_task(
#           60.0,
#           harvest_all_sp_football_live.s(),
#           name="sp-football-live-60s",
#       )
# =============================================================================


# =============================================================================
# Manual trigger helper (for testing / admin)
# =============================================================================

def trigger_full_harvest(sport_slug: str = "soccer") -> dict:
    """
    Manually trigger a full harvest and return the result synchronously.

    Usage (from a Flask shell or admin view):
        from app.workers.sp_celery_tasks import trigger_full_harvest
        result = trigger_full_harvest("soccer")
        print(result)
    """
    return harvest_sp_sport.apply(args=[sport_slug, "upcoming"]).get(timeout=600)