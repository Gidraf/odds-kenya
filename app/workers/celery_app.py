"""
app/workers/celery_app.py
==========================
Celery application factory – single source of truth for Celery instance.
"""

from __future__ import annotations
import os
from celery import Celery

def make_celery() -> Celery:
    broker  = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    backend = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    app = Celery("kinetic", broker=broker, backend=backend)
    app.conf.update(
        task_serializer          = "json",
        result_serializer        = "json",
        accept_content           = ["json"],
        task_track_started       = True,
        task_acks_late           = True,
        worker_prefetch_multiplier = 4,
        task_soft_time_limit     = 30,
        task_time_limit          = 40,
        result_expires           = 300,
        broker_connection_retry_on_startup = True,
    )
    return app

celery_app = make_celery()