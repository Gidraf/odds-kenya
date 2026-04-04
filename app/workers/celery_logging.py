"""
app/workers/celery_logging.py
==============================
Configures Celery to write verbose debug logs to logs/celery.log.
Import this at the top of celery_tasks.py or call setup() from create_app().

Usage in start command:
  celery -A app.workers.celery_tasks worker -B \
    --queues=harvest,live,results,notify,ev_arb,default \
    --concurrency=8 \
    --loglevel=debug \
    --logfile=logs/celery.log \
    --hostname=all@%h
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

from celery.signals import (
    after_task_publish, task_prerun, task_postrun,
    task_failure, task_retry, beat_init, heartbeat_sent,
)

LOG_DIR = Path(os.environ.get("LOG_DIR", "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

CELERY_LOG_FILE = LOG_DIR / "celery.log"
BEAT_LOG_FILE   = LOG_DIR / "beat.log"
TASK_LOG_FILE   = LOG_DIR / "tasks.log"


def setup_file_logging():
    """
    Call once from celery_tasks.py after the celery instance is created.
    Sets up three log files:
      logs/celery.log  — all celery output (mirror of stdout)
      logs/beat.log    — beat scheduler ticks only
      logs/tasks.log   — task start / success / failure / retry
    """
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        "%Y-%m-%dT%H:%M:%S",
    )

    def _add_handler(logger_name: str, filepath: Path, level=logging.DEBUG):
        log = logging.getLogger(logger_name)
        if not any(isinstance(h, logging.FileHandler) and
                   getattr(h, 'baseFilename', '') == str(filepath)
                   for h in log.handlers):
            fh = logging.FileHandler(filepath, encoding="utf-8")
            fh.setFormatter(fmt)
            fh.setLevel(level)
            log.addHandler(fh)
            log.setLevel(level)

    # Main celery loggers
    for name in ("celery", "celery.app.trace", "celery.worker",
                 "celery.beat", "celery.task", "tasks_live",
                 "harvest_jobs"):
        _add_handler(name, CELERY_LOG_FILE)

    # Beat-only log
    _add_handler("celery.beat", BEAT_LOG_FILE)

    # Task execution log
    _add_handler("celery.app.trace", TASK_LOG_FILE)


# ── Celery signals → structured entries in tasks.log ─────────────────────────

import json as _json
from datetime import datetime, timezone as _tz

_task_fmt = logging.Formatter("%(message)s")

def _task_file_logger():
    log = logging.getLogger("task_events")
    if not log.handlers:
        fh = logging.FileHandler(TASK_LOG_FILE, encoding="utf-8")
        fh.setFormatter(_task_fmt)
        log.addHandler(fh)
        log.setLevel(logging.DEBUG)
    return log


def _ts():
    return datetime.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@task_prerun.connect
def on_task_prerun(task_id, task, args, kwargs, **_):
    _task_file_logger().info(_json.dumps({
        "event": "task_start", "task": task.name,
        "task_id": task_id, "ts": _ts(),
    }))
    # Store last-run time per task name in Redis for beat countdown
    try:
        from app.workers.celery_tasks import _redis
        _redis().hset("monitor:last_run", task.name, _ts())
    except Exception:
        pass


@task_postrun.connect
def on_task_postrun(task_id, task, args, kwargs, retval, state, **_):
    _task_file_logger().info(_json.dumps({
        "event": "task_done", "task": task.name,
        "task_id": task_id, "state": state, "ts": _ts(),
    }))


@task_failure.connect
def on_task_failure(task_id, exception, traceback, sender, **_):
    _task_file_logger().error(_json.dumps({
        "event": "task_failure", "task": sender.name,
        "task_id": task_id, "error": str(exception), "ts": _ts(),
    }))


@task_retry.connect
def on_task_retry(request, reason, einfo, **_):
    _task_file_logger().warning(_json.dumps({
        "event": "task_retry", "task": request.task,
        "reason": str(reason), "ts": _ts(),
    }))