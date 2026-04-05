"""
app/workers/celery_tasks.py
============================
Celery tasks for harvesting bookmaker odds.

Setup (add to your app factory / run script):
    from app.workers.celery_tasks import make_celery
    celery = make_celery(app)

Beat schedule (celery_beat_schedule in config):
    harvest_all_upcoming  → every 5 minutes
    harvest_all_live      → every 60 seconds
    health_check          → every 30 seconds

Worker command:
    celery -A app.workers.celery_tasks.celery worker --loglevel=info -c 4
Beat command:
    celery -A app.workers.celery_tasks.celery beat --loglevel=info
Monitor:
    celery -A app.workers.celery_tasks.celery flower
"""

from __future__ import annotations

import json
import time
import traceback
from datetime import datetime, timezone

from celery import Celery
from celery.schedules import crontab
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

# ── Celery factory ──────────────────────────────────────────────────────────────

def make_celery(app=None):
    """Call from your Flask app factory."""
    broker  = "redis://localhost:6379/0"
    backend = "redis://localhost:6379/1"

    if app:
        broker  = app.config.get("CELERY_BROKER_URL",  broker)
        backend = app.config.get("CELERY_RESULT_BACKEND", backend)

    celery = Celery(
        "odds_harvester",
        broker=broker,
        backend=backend,
    )
    celery.conf.update(
        task_serializer   = "json",
        result_serializer = "json",
        accept_content    = ["json"],
        timezone          = "UTC",
        enable_utc        = True,
        task_acks_late    = True,
        worker_prefetch_multiplier = 1,
        task_routes = {
            "app.workers.celery_tasks.harvest_bookmaker_sport": {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_upcoming":    {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_live":        {"queue": "live"},
            "app.workers.celery_tasks.health_check":            {"queue": "default"},
        },
        beat_schedule = {
            "harvest-upcoming-5min": {
                "task":     "app.workers.celery_tasks.harvest_all_upcoming",
                "schedule": 300,   # every 5 minutes
            },
            "harvest-live-60s": {
                "task":     "app.workers.celery_tasks.harvest_all_live",
                "schedule": 60,    # every 60 seconds
            },
            "health-check-30s": {
                "task":     "app.workers.celery_tasks.health_check",
                "schedule": 30,
            },
        },
    )

    if app:
        class ContextTask(celery.Task):
            def __call__(self, *args, **kwargs):
                with app.app_context():
                    return self.run(*args, **kwargs)
        celery.Task = ContextTask

    return celery


celery = make_celery()   # module-level instance — overridden by app factory


# ── Redis cache helpers ─────────────────────────────────────────────────────────

def _redis():
    import redis
    return redis.Redis.from_url(celery.conf.broker_url.replace("/0", "/2"))


def _cache_set(key: str, data: any, ttl: int = 600):
    try:
        r = _redis()
        r.set(key, json.dumps(data, default=str), ex=ttl)
    except Exception as e:
        logger.warning(f"Cache write failed for {key}: {e}")


def _cache_get(key: str) -> any:
    try:
        r = _redis()
        raw = r.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _cache_keys(pattern: str) -> list[str]:
    try:
        r = _redis()
        return [k.decode() for k in r.keys(pattern)]
    except Exception:
        return []


# ── DB helpers ──────────────────────────────────────────────────────────────────

def _get_active_bookmakers() -> list[dict]:
    """
    Load active bookmakers from DB.
    Expected columns on Bookmaker model:
        id, name, domain, is_active,
        vendor_slug (str),  ← "betb2b" | "sportpesa" | "generic"
        harvest_config (JSON) ← {headers, params, field_map, array_path, list_url}
    """
    try:
        from app.extensions import db
        from app.models.bookmakers_model import Bookmaker
        bms = Bookmaker.query.filter_by(is_active=True).all()
        result = []
        for bm in bms:
            config = {}
            if hasattr(bm, "harvest_config") and bm.harvest_config:
                config = bm.harvest_config if isinstance(bm.harvest_config, dict) else {}
            result.append({
                "id":          bm.id,
                "name":        bm.name,
                "domain":      bm.domain,
                "vendor_slug": getattr(bm, "vendor_slug", "betb2b"),
                "config":      config,
            })
        return result
    except Exception as e:
        logger.error(f"Failed to load bookmakers: {e}")
        return []


def _get_active_sports() -> list[str]:
    """Return list of sport names to harvest. Reads from DB or falls back to defaults."""
    try:
        from app.models.mapping_models import Sport
        sports = Sport.query.filter_by(is_active=True).all()
        return [s.name for s in sports] if sports else _DEFAULT_SPORTS
    except Exception:
        return _DEFAULT_SPORTS


_DEFAULT_SPORTS = [
    "Football", "Basketball", "Tennis", "Ice Hockey",
    "Volleyball", "Cricket", "Rugby",
]


def _update_task_status(task_name: str, status: dict):
    """Write task heartbeat to Redis for the monitor."""
    _cache_set(f"task_status:{task_name}", {
        **status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }, ttl=300)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Core tasks ───────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(
    bind=True,
    name="app.workers.celery_tasks.harvest_bookmaker_sport",
    max_retries=2,
    default_retry_delay=30,
    soft_time_limit=60,
    time_limit=90,
)
def harvest_bookmaker_sport(
    self,
    bookmaker: dict,
    sport: str,
    mode: str = "upcoming",
) -> dict:
    """
    Harvest one bookmaker × one sport.
    Stores result in Redis as:
        odds:{mode}:{sport}:{bookmaker_id}
    TTL: 360s (upcoming) / 90s (live)
    """
    t0   = time.perf_counter()
    bk_name = bookmaker.get("name") or bookmaker.get("domain", "?")
    task_key = f"{bk_name}_{sport}_{mode}"

    _update_task_status(task_key, {
        "bookmaker": bk_name,
        "sport":     sport,
        "mode":      mode,
        "state":     "running",
    })

    try:
        from app.views.odds_feed.bookmaker_fetcher import fetch_bookmaker
        matches = fetch_bookmaker(bookmaker, sport_name=sport, mode=mode, timeout=25)
    except Exception as exc:
        logger.error(f"[harvest] {bk_name}/{sport}: {exc}\n{traceback.format_exc()}")
        _update_task_status(task_key, {
            "bookmaker": bk_name, "sport": sport, "mode": mode,
            "state":     "error",
            "error":     str(exc),
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)
    count   = len(matches)
    ttl     = 90 if mode == "live" else 360

    cache_key = f"odds:{mode}:{sport.lower().replace(' ', '_')}:{bookmaker['id']}"
    _cache_set(cache_key, {
        "bookmaker_id":   bookmaker["id"],
        "bookmaker_name": bk_name,
        "sport":          sport,
        "mode":           mode,
        "match_count":    count,
        "harvested_at":   datetime.now(timezone.utc).isoformat(),
        "matches":        matches,
    }, ttl=ttl)

    _update_task_status(task_key, {
        "bookmaker":  bk_name,
        "sport":      sport,
        "mode":       mode,
        "state":      "ok",
        "count":      count,
        "latency_ms": latency,
    })

    logger.info(f"[harvest] ✓ {bk_name}/{sport}/{mode}: {count} matches in {latency}ms")
    return {"bookmaker": bk_name, "sport": sport, "count": count, "latency_ms": latency}


@celery.task(
    name="app.workers.celery_tasks.harvest_all_upcoming",
    soft_time_limit=240,
    time_limit=300,
)
def harvest_all_upcoming():
    """Fan out harvest tasks for all active bookmakers × sports (upcoming)."""
    bookmakers = _get_active_bookmakers()
    sports     = _get_active_sports()
    dispatched = 0

    for bm in bookmakers:
        for sport in sports:
            harvest_bookmaker_sport.apply_async(
                args=[bm, sport, "upcoming"],
                queue="harvest",
                countdown=dispatched * 0.5,   # stagger to avoid hammering
            )
            dispatched += 1

    logger.info(f"[beat] Dispatched {dispatched} upcoming harvest tasks")
    _update_task_status("beat_upcoming", {
        "state":      "ok",
        "dispatched": dispatched,
        "bookmakers": len(bookmakers),
        "sports":     len(sports),
    })
    return {"dispatched": dispatched}


@celery.task(
    name="app.workers.celery_tasks.harvest_all_live",
    soft_time_limit=120,
    time_limit=150,
)
def harvest_all_live():
    """Fan out live harvest tasks — higher frequency, fewer sports (just football + live active)."""
    bookmakers  = _get_active_bookmakers()
    live_sports = ["Football", "Basketball", "Ice Hockey", "Tennis"]
    dispatched  = 0

    for bm in bookmakers:
        for sport in live_sports:
            harvest_bookmaker_sport.apply_async(
                args=[bm, sport, "live"],
                queue="live",
            )
            dispatched += 1

    logger.info(f"[beat] Dispatched {dispatched} live harvest tasks")
    _update_task_status("beat_live", {
        "state":      "ok",
        "dispatched": dispatched,
    })
    return {"dispatched": dispatched}


@celery.task(name="app.workers.celery_tasks.health_check")
def health_check():
    """Writes a heartbeat to Redis so the monitor knows the worker is alive."""
    _cache_set("worker_heartbeat", {
        "alive":      True,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }, ttl=120)
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════════════════════
# ─── On-demand tasks (called from admin API) ──────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="app.workers.celery_tasks.probe_bookmaker_now",
    soft_time_limit=30,
    time_limit=45,
)
def probe_bookmaker_now(bookmaker: dict, sport: str, mode: str = "upcoming") -> dict:
    """
    Immediate single-bookmaker probe for the admin monitor.
    Returns the result directly (no Redis write) so the admin
    can display it without waiting for the next beat cycle.
    """
    from app.views.odds_feed.bookmaker_fetcher import fetch_bookmaker
    t0 = time.perf_counter()
    try:
        matches  = fetch_bookmaker(bookmaker, sport_name=sport, mode=mode, timeout=20)
        latency  = int((time.perf_counter() - t0) * 1000)
        return {
            "ok":         True,
            "bookmaker":  bookmaker.get("name"),
            "sport":      sport,
            "mode":       mode,
            "count":      len(matches),
            "latency_ms": latency,
            "matches":    matches[:5],   # preview only — full data in cache
        }
    except Exception as exc:
        return {
            "ok":    False,
            "error": str(exc),
        }