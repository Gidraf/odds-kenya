"""
app/workers/celery_tasks.py
============================
Celery tasks for harvesting bookmaker odds.

QUICK START
-----------
1.  Add to app/__init__.py (Flask factory):

        from app.workers.celery_tasks import make_celery
        celery = make_celery(app)         # replaces module-level instance

2.  Create app/celery_app.py (the -A target for CLI):

        from app import create_app
        from app.workers.celery_tasks import make_celery
        flask_app = create_app()
        celery    = make_celery(flask_app)

3.  Run from project root:

        # Worker
        celery -A app.celery_app worker --loglevel=info -Q harvest,live,default -c 4

        # Scheduler
        celery -A app.celery_app beat --loglevel=info

        # Dev — combined (single process)
        celery -A app.celery_app worker --beat --loglevel=info -c 2

TROUBLESHOOTING
---------------
  "No module named app"         → run from project root, not from app/
  Tasks not running             → celery -A app.celery_app status
  Redis refused                 → redis-cli ping
  DB errors inside tasks        → make sure make_celery(flask_app) is called
  406 / token expired           → update harvest_config headers in CONFIG tab
"""

from __future__ import annotations

import json
import os
import time
import traceback
from datetime import datetime, timezone

from celery import Celery
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

# ── Module-level instance (replaced by make_celery) ────────────────────────────
celery = Celery(
    "odds_harvester",
    broker  = "redis://localhost:6379/0",
    backend = "redis://localhost:6379/1",
)

_DEFAULT_SPORTS = [
    "Football", "Basketball", "Tennis", "Ice Hockey",
    "Volleyball", "Cricket", "Rugby",
]


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Factory ──────────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

# def make_celery(app) -> Celery:
#     """
#     Bind Celery to the Flask app and return the configured instance.
#     Every task will run inside an app context automatically.
#     """
#     global celery

#     broker  = app.config.get("CELERY_BROKER_URL",     "redis://localhost:6382/0")
#     backend = app.config.get("CELERY_RESULT_BACKEND",  "redis://localhost:6382/1")

#     celery = Celery(app.import_name, broker=broker, backend=backend)

#     celery.conf.update(
#         task_serializer            = "json",
#         result_serializer          = "json",
#         accept_content             = ["json"],
#         timezone                   = "UTC",
#         enable_utc                 = True,
#         task_acks_late             = True,
#         worker_prefetch_multiplier = 1,
#         task_reject_on_worker_lost = True,
#         task_default_queue         = "default",
#         task_routes = {
#             "app.workers.celery_tasks.harvest_bookmaker_sport": {"queue": "harvest"},
#             "app.workers.celery_tasks.harvest_all_upcoming":    {"queue": "harvest"},
#             "app.workers.celery_tasks.harvest_all_live":        {"queue": "live"},
#             "app.workers.celery_tasks.health_check":            {"queue": "default"},
#             "app.workers.celery_tasks.probe_bookmaker_now":     {"queue": "default"},
#         },
#         beat_schedule = {
#             "harvest-upcoming-5min": {
#                 "task":     "app.workers.celery_tasks.harvest_all_upcoming",
#                 "schedule": 300,
#             },
#             "harvest-live-60s": {
#                 "task":     "app.workers.celery_tasks.harvest_all_live",
#                 "schedule": 60,
#             },
#             "health-check-30s": {
#                 "task":     "app.workers.celery_tasks.health_check",
#                 "schedule": 30,
#             },
#         },
#     )

#     # Wrap every task call in a Flask app context
#     class ContextTask(celery.Task):
#         abstract = True
#         def __call__(self, *args, **kwargs):
#             with app.app_context():
#                 return self.run(*args, **kwargs)

#     celery.Task = ContextTask
#     return celery


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Redis cache ──────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _redis():
    import redis as _r
    url  = celery.conf.broker_url or "redis://localhost:6379/0"
    # Always use DB 2 for our odds cache (broker=0, backend=1, cache=2)
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    return _r.Redis.from_url(
        f"{base}/2",
        decode_responses    = False,
        socket_timeout      = 5,
        socket_connect_timeout = 5,
        retry_on_timeout    = True,
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


# ═══════════════════════════════════════════════════════════════════════════════
# ─── DB helpers (must run inside Flask app context) ───────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _load_bookmakers() -> list[dict]:
    """Load active bookmakers from DB. Returns JSON-safe dicts."""
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
        logger.info(f"[db] {len(result)} active bookmakers")
        return result
    except Exception as exc:
        logger.error(f"[db] load_bookmakers failed: {exc}")
        return []


def _load_sports() -> list[str]:
    try:
        from app.models.mapping_models import Sport
        sports = Sport.query.filter_by(is_active=True).all()
        if sports:
            return [s.name for s in sports]
    except Exception:
        pass
    return _DEFAULT_SPORTS


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Core harvest task ────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(
    bind=True,
    name="app.workers.celery_tasks.harvest_bookmaker_sport",
    max_retries=2,
    default_retry_delay=30,
    soft_time_limit=60,
    time_limit=90,
    acks_late=True,
)
def harvest_bookmaker_sport(self, bookmaker: dict, sport: str, mode: str = "live") -> dict:
    """
    Harvest one bookmaker × one sport and write to Redis.

    Cache key:  odds:{mode}:{sport_slug}:{bookmaker_id}
    TTL:        90s  (live)
                360s (upcoming)
    """
    t0      = time.perf_counter()
    bk_name = bookmaker.get("name") or bookmaker.get("domain", "?")
    bk_id   = bookmaker.get("id", "unknown")
    tkey    = f"{bk_name}_{sport}_{mode}"

    task_status_set(tkey, {
        "bookmaker": bk_name,
        "sport":     sport,
        "mode":      mode,
        "state":     "running",
        "task_id":   self.request.id,
    })

    try:
        from app.views.odds_feed.bookmaker_fetcher import fetch_bookmaker
        matches = fetch_bookmaker(bookmaker, sport_name=sport, mode=mode, timeout=25)
    except Exception as exc:
        latency = int((time.perf_counter() - t0) * 1000)
        err     = str(exc)
        logger.error(f"[harvest] {bk_name}/{sport}/{mode} failed: {err}")

        task_status_set(tkey, {
            "bookmaker":  bk_name, "sport": sport, "mode": mode,
            "state":      "error",
            "error":      err,
            "latency_ms": latency,
        })

        # Don't retry on auth failures — user needs to refresh headers
        if any(x in err.lower() for x in ("406", "token expired", "auth")):
            return {"ok": False, "error": err, "retried": False}

        raise self.retry(exc=exc)

    latency   = int((time.perf_counter() - t0) * 1000)
    count     = len(matches)
    ttl       = 90 if mode == "live" else 360
    sport_key = sport.lower().replace(" ", "_")

    cache_set(f"odds:{mode}:{sport_key}:{bk_id}", {
        "bookmaker_id":   bk_id,
        "bookmaker_name": bk_name,
        "sport":          sport,
        "mode":           mode,
        "match_count":    count,
        "harvested_at":   datetime.now(timezone.utc).isoformat(),
        "latency_ms":     latency,
        "matches":        matches,
    }, ttl=ttl)

    task_status_set(tkey, {
        "bookmaker":  bk_name, "sport": sport, "mode": mode,
        "state":      "ok",
        "count":      count,
        "latency_ms": latency,
    })

    logger.info(f"[harvest] ✓ {bk_name}/{sport}/{mode}: {count} matches in {latency}ms")
    return {"ok": True, "bookmaker": bk_name, "sport": sport, "count": count, "latency_ms": latency}


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Beat tasks ───────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="app.workers.celery_tasks.harvest_all_upcoming",
    soft_time_limit=240,
    time_limit=300,
)
def harvest_all_upcoming() -> dict:
    """Fan out upcoming harvest — fires every 5 minutes via beat."""
    bookmakers = _load_bookmakers()
    sports     = _load_sports()

    if not bookmakers:
        msg = "No active bookmakers in DB"
        logger.warning(f"[beat:upcoming] {msg}")
        task_status_set("beat_upcoming", {"state": "error", "error": msg, "dispatched": 0})
        return {"dispatched": 0, "error": msg}

    dispatched = 0
    for bm in bookmakers:
        for sport in sports:
            harvest_bookmaker_sport.apply_async(
                args=[bm, sport, "upcoming"],
                queue="harvest",
                countdown=dispatched * 0.3,   # gentle stagger
            )
            dispatched += 1

    logger.info(f"[beat:upcoming] {dispatched} tasks ({len(bookmakers)} bk × {len(sports)} sports)")
    task_status_set("beat_upcoming", {
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
def harvest_all_live() -> dict:
    """Fan out live harvest — fires every 60 seconds via beat."""
    bookmakers  = _load_bookmakers()
    live_sports = ["Football", "Basketball", "Ice Hockey", "Tennis"]

    if not bookmakers:
        msg = "No active bookmakers in DB"
        logger.warning(f"[beat:live] {msg}")
        task_status_set("beat_live", {"state": "error", "error": msg, "dispatched": 0})
        return {"dispatched": 0, "error": msg}

    dispatched = 0
    for bm in bookmakers:
        for sport in live_sports:
            harvest_bookmaker_sport.apply_async(
                args=[bm, sport, "live"],
                queue="live",
            )
            dispatched += 1

    logger.info(f"[beat:live] {dispatched} tasks ({len(bookmakers)} bk)")
    task_status_set("beat_live", {
        "state":      "ok",
        "dispatched": dispatched,
        "bookmakers": len(bookmakers),
    })
    return {"dispatched": dispatched}


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


# ═══════════════════════════════════════════════════════════════════════════════
# ─── On-demand probe (called from admin API synchronously) ────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="app.workers.celery_tasks.probe_bookmaker_now",
    soft_time_limit=30,
    time_limit=45,
)
def probe_bookmaker_now(bookmaker: dict, sport: str, mode: str = "live") -> dict:
    """
    Immediate probe — used by the admin UI PROBE tab.
    Does NOT write to the harvest cache, just returns results.
    """
    from app.views.odds_feed.bookmaker_fetcher import fetch_bookmaker
    t0 = time.perf_counter()
    try:
        matches = fetch_bookmaker(bookmaker, sport_name=sport, mode=mode, timeout=20)
        latency = int((time.perf_counter() - t0) * 1000)
        return {
            "ok":         True,
            "bookmaker":  bookmaker.get("name"),
            "sport":      sport,
            "mode":       mode,
            "count":      len(matches),
            "latency_ms": latency,
            "matches":    matches[:10],
        }
    except Exception as exc:
        latency = int((time.perf_counter() - t0) * 1000)
        return {"ok": False, "error": str(exc), "latency_ms": latency}