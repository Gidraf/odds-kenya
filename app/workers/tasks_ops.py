"""
app/workers/tasks_ops.py
=========================
Celery beat schedule registration + startup harvest dispatcher.

Beat tasks registered here:
  harvest_all_paged        every 5 min   — SP + BT + OD + B2B×7 upcoming
  b2b_harvest_all_live     every 90 s    — B2B live odds refresh
  run_alignment_pass       every 10 min  — cross-BK fuzzy alignment
  prune_redis_snapshots    every 30 min  — evict stale Redis keys

Startup:
  _dispatch_startup_harvests()   called once on worker boot
"""
from __future__ import annotations

import logging
import time

from celery.signals import worker_ready
from celery.utils.log import get_task_logger

from app.workers.celery_tasks import celery

logger = get_task_logger(__name__)
log    = logging.getLogger(__name__)

# ─── Sports universe ──────────────────────────────────────────────────────────

_ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby",
    "ice-hockey", "volleyball", "handball", "table-tennis",
    "baseball", "mma", "boxing", "darts", "american-football", "esoccer",
]

# ─────────────────────────────────────────────────────────────────────────────
# Beat schedule
# ─────────────────────────────────────────────────────────────────────────────

def setup_periodic_tasks(sender, **kwargs):
    """
    Register all periodic tasks with Celery Beat.
    Called via the `celery.on_after_configure` signal.
    """
    # ── Upcoming harvest: all 10 BKs every 5 min ─────────────────────────
    sender.add_periodic_task(
        300.0,
        _beat_harvest_all_paged.s(),
        name="harvest:all_paged every 5min",
    )

    # ── B2B live refresh every 90s ────────────────────────────────────────
    sender.add_periodic_task(
        90.0,
        _beat_b2b_harvest_all_live.s(),
        name="b2b:harvest_all_live every 90s",
    )

    # ── Cross-BK fuzzy alignment every 10 min ────────────────────────────
    sender.add_periodic_task(
        600.0,
        _beat_run_alignment_pass.s(),
        name="align:full_pass every 10min",
    )

    # ── Redis snapshot pruning every 30 min ──────────────────────────────
    sender.add_periodic_task(
        1800.0,
        _beat_prune_redis_snapshots.s(),
        name="ops:prune_redis every 30min",
    )

    logger.info("[tasks_ops] Beat schedule registered (4 tasks)")


# Wire up the signal
celery.on_after_configure.connect(setup_periodic_tasks)


# ─────────────────────────────────────────────────────────────────────────────
# Thin beat wrappers (avoid importing heavy modules at beat-scheduler time)
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(name="tasks.ops.beat.harvest_all_paged",
             soft_time_limit=30, time_limit=60)
def _beat_harvest_all_paged() -> dict:
    """Beat wrapper → delegates to master harvest orchestrator."""
    from app.workers.tasks_harvest_pages import harvest_all_paged
    harvest_all_paged.apply_async(queue="harvest", countdown=0)
    return {"ok": True, "ts": time.time()}


@celery.task(name="tasks.ops.beat.b2b_harvest_all_live",
             soft_time_limit=30, time_limit=60)
def _beat_b2b_harvest_all_live() -> dict:
    """Beat wrapper → fires B2B live harvest for all sports."""
    from app.workers.tasks_harvest_b2b import b2b_harvest_all_live
    b2b_harvest_all_live.apply_async(queue="harvest", countdown=0)
    return {"ok": True, "ts": time.time()}


@celery.task(name="tasks.ops.beat.run_alignment_pass",
             soft_time_limit=30, time_limit=60)
def _beat_run_alignment_pass() -> dict:
    """Beat wrapper → fires a fuzzy-alignment pass for all sports."""
    from celery import group as cgroup
    from app.workers.tasks_align import align_sport
    sigs = [align_sport.s(sport, 500) for sport in _ALL_SPORTS]
    cgroup(sigs).apply_async(queue="results")
    return {"ok": True, "sports": len(_ALL_SPORTS), "ts": time.time()}


@celery.task(name="tasks.ops.beat.prune_redis_snapshots",
             soft_time_limit=120, time_limit=180)
def _beat_prune_redis_snapshots() -> dict:
    """Remove Redis snapshot keys that haven't been refreshed in 2 hours."""
    from app.workers.celery_tasks import _redis
    r = _redis()
    pruned = 0
    try:
        # Scan all odds:* and sp:* keys with no TTL
        cursor = 0
        patterns = ["odds:b2b:*", "odds:sp:*", "odds:bt:*", "odds:od:*",
                    "odds:unified:*", "odds:pages:*"]
        for pat in patterns:
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor, match=pat, count=200)
                for key in keys:
                    ttl = r.ttl(key)
                    if ttl == -1:           # no expiry set
                        r.expire(key, 7200) # set 2h TTL
                        pruned += 1
                if cursor == 0:
                    break
    except Exception as exc:
        logger.warning("[ops:prune] error: %s", exc)
    logger.info("[ops:prune] set TTL on %d orphan keys", pruned)
    return {"ok": True, "pruned": pruned, "ts": time.time()}


# ─────────────────────────────────────────────────────────────────────────────
# Startup dispatcher
# ─────────────────────────────────────────────────────────────────────────────

@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    """Fire initial harvests on worker startup (with stagger)."""
    try:
        _dispatch_startup_harvests()
    except Exception as exc:
        log.warning("[tasks_ops] startup dispatch failed: %s", exc)


def _dispatch_startup_harvests() -> None:
    """
    Dispatch one round of harvests for all 10 bookmakers when the
    worker comes online.  Staggered to avoid thundering herd.
    """
    from app.workers.tasks_harvest_pages import (
        sp_harvest_all_paged,
        bt_harvest_all_paged,
        od_harvest_all_paged,
    )
    from app.workers.tasks_harvest_b2b import (
        b2b_harvest_all_paged,
        b2b_harvest_all_live,
    )

    log.info("[tasks_ops] Dispatching startup harvests...")

    # Upcoming — staggered 10s apart
    sp_harvest_all_paged.apply_async(queue="harvest", countdown=5)
    bt_harvest_all_paged.apply_async(queue="harvest", countdown=15)
    od_harvest_all_paged.apply_async(queue="harvest", countdown=25)
    b2b_harvest_all_paged.apply_async(queue="harvest", countdown=35)

    # B2B live — after upcoming is underway
    b2b_harvest_all_live.apply_async(queue="harvest", countdown=60)

    log.info("[tasks_ops] Startup harvests dispatched (SP+BT+OD+B2B upcoming + B2B live)")


# ─────────────────────────────────────────────────────────────────────────────
# Health check task
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(name="tasks.ops.healthcheck",
             soft_time_limit=10, time_limit=15)
def healthcheck() -> dict:
    """Returns worker liveness info. Used by monitoring."""
    from app.workers.celery_tasks import _redis
    r = _redis()
    redis_ok = False
    try:
        r.ping()
        redis_ok = True
    except Exception:
        pass
    return {
        "ok":       True,
        "redis":    redis_ok,
        "ts":       time.time(),
        "worker":   "celery",
    }