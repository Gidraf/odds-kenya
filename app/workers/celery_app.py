"""
app/workers/celery_app.py
==========================
Celery application factory.
One broker and one backend, both pointing at Redis.
Import `celery_app` everywhere tasks need to be registered or dispatched.
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
        worker_prefetch_multiplier = 4,      # 4 tasks pre-fetched per worker thread
        task_soft_time_limit     = 30,       # warn at 30 s
        task_time_limit          = 40,       # kill  at 40 s
        result_expires           = 300,      # clean results after 5 min
        broker_connection_retry_on_startup = True,
    )
    return app


celery_app = make_celery()