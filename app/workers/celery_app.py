# app/workers/celery_app.py
from __future__ import annotations
import os


def make_celery(flask_app=None):
    """
    If flask_app is passed (from create_app), every task runs inside
    a Flask app context automatically. Without it, tasks crash on any
    db.session / model access.
    """
    from app.extensions import celery

    broker  = os.getenv("CELERY_BROKER_URL",    "amqp://kinetic:kinetic_pass@localhost:5672/kinetic")
    backend = os.getenv("CELERY_RESULT_BACKEND", os.getenv("REDIS_URL", "redis://localhost:6382/1"))

    celery.conf.broker_url   = broker
    celery.conf.result_backend = backend

    celery.conf.update(
        task_serializer                    = "json",
        result_serializer                  = "json",
        accept_content                     = ["json"],
        task_track_started                 = True,
        task_acks_late                     = True,
        worker_prefetch_multiplier         = 1,
        task_soft_time_limit               = 300,
        task_time_limit                    = 360,
        result_expires                     = 3600,
        broker_connection_retry_on_startup = True,
        broker_heartbeat                   = 10,

        include = [
            "app.workers.tasks_ops",
            "app.workers.tasks_upcoming",
            "app.workers.tasks_live",
            "app.workers.tasks_market_align",
            "app.workers.tasks_harvest_pages",
            "app.workers.tasks_harvest_b2b",
            "app.workers.tasks_align",
        ],

        task_routes = {
            "tasks.sp.*":                  {"queue": "harvest"},
            "tasks.bt.*":                  {"queue": "harvest"},
            "tasks.od.*":                  {"queue": "harvest"},
            "tasks.bt_od.*":               {"queue": "harvest"},
            "tasks.b2b.*":                 {"queue": "harvest"},
            "tasks.sbo.*":                 {"queue": "harvest"},
            "harvest.*":                   {"queue": "harvest"},
            "tasks.live.*":                {"queue": "live"},
            "tasks.ops.compute_ev_arb":    {"queue": "ev_arb"},
            "tasks.ops.persist*":          {"queue": "results"},
            "harvest.persist*":            {"queue": "results"},
            "harvest.value_bets":          {"queue": "ev_arb"},
            "tasks.align.*":               {"queue": "results"},
            "tasks.market_align.*":        {"queue": "results"},
            "tasks.ops.beat.*":            {"queue": "default"},
            "tasks.ops.health_check":      {"queue": "default"},
            "tasks.ops.build_health*":     {"queue": "default"},
            "tasks.ops.expire*":           {"queue": "default"},
            "tasks.ops.publish_ws_event":  {"queue": "notify"},
            "tasks.ops.dispatch_notify*":  {"queue": "notify"},
            "tasks.ops.send_*":            {"queue": "notify"},
            "tasks.notify.*":              {"queue": "notify"},
            "tasks.sp.enrich_analytics":   {"queue": "analytics"},
            "tasks.sp.get_match_analytics":{"queue": "analytics"},
        },

        beat_schedule = {
            "sp-harvest-5min": {
                "task":     "tasks.sp.harvest_all_upcoming",
                "schedule": 300,
                "options":  {"queue": "harvest"},
            },
            "bt-od-harvest-5min": {
                "task":     "tasks.bt_od.harvest_all_upcoming",
                "schedule": 300,
                "options":  {"queue": "harvest", "countdown": 60},
            },
            "b2b-harvest-10min": {
                "task":     "tasks.b2b.harvest_all_upcoming",
                "schedule": 600,
                "options":  {"queue": "harvest"},
            },
            "harvest-all-paged-5min": {
                "task":     "tasks.ops.beat.harvest_all_paged",
                "schedule": 300,
                "options":  {"queue": "harvest"},
            },
            "b2b-live-90s": {
                "task":     "tasks.ops.beat.b2b_live",
                "schedule": 90,
                "options":  {"queue": "harvest"},
            },
            "alignment-10min": {
                "task":     "tasks.ops.beat.alignment",
                "schedule": 600,
                "options":  {"queue": "results"},
            },
            "prune-30min": {
                "task":     "tasks.ops.beat.prune",
                "schedule": 1800,
                "options":  {"queue": "default"},
            },
            "arb-digest-5min": {
                "task":     "tasks.notify.arb_digest",
                "schedule": 300,
                "options":  {"queue": "notify"},
            },
            "cleanup-daily-3am": {
                "task":     "harvest.cleanup",
                "schedule": 86400,
                "options":  {"queue": "results"},
            },
        },
    )

    # ── THIS IS THE CRITICAL FIX ───────────────────────────────────────────
    # Wrap every task execution in a Flask app context.
    # Without this, any task that calls db.session, Model.query etc.
    # crashes with "Working outside of application context."
    if flask_app is not None:
        class ContextTask(celery.Task):
            abstract = True

            def __call__(self, *args, **kwargs):
                with flask_app.app_context():
                    return self.run(*args, **kwargs)

        celery.Task = ContextTask

    return celery


# Module-level instance — used by:
#   celery -A app.workers.celery_app worker ...
#   celery -A app.workers.celery_app beat ...
#   celery -A app.workers.celery_app flower ...
#
# At this point flask_app is None, so ContextTask is NOT applied yet.
# It gets applied when create_app() calls make_celery(flask_app).
celery_app = make_celery()