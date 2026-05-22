"""
app/workers/celery_app.py
"""
from __future__ import annotations
import os


def make_celery(flask_app=None):
    """
    Two call patterns:
      1. make_celery(flask_app)  — from create_app(). Wraps every task in a
                                   real app context immediately.
      2. make_celery()           — from `celery -A app.workers.celery_app`.
                                   Uses LazyContextTask: creates the app on
                                   first task execution so the worker process
                                   doesn't need a running Flask server.
    """
    from app.extensions import celery

    broker  = os.getenv("CELERY_BROKER_URL",     "amqp://kinetic:kinetic_pass@localhost:5672/kinetic")
    backend = os.getenv("CELERY_RESULT_BACKEND",  os.getenv("REDIS_URL", "redis://localhost:6382/1"))

    celery.conf.broker_url      = broker
    celery.conf.result_backend  = backend

    celery.conf.update(
        task_serializer                    = "json",
        result_serializer                  = "json",
        accept_content                     = ["json"],
        task_track_started                 = True,
        task_acks_late                     = True,
        worker_prefetch_multiplier         = 1,
        task_soft_time_limit               = 3600,
        task_time_limit                    = 6000,
        result_expires                     = 3600,
        broker_connection_retry_on_startup = True,
        broker_heartbeat                   = 10,

        # ── Auto-discovery: every module listed here is imported by each
        #    worker process at startup. Tasks decorated with @celery.task
        #    inside these modules self-register.
        #    FIX: tasks_lifecycle added — previously lifecycle tasks were
        #    defined inside a closure in live_results_api.py and were never
        #    auto-discovered, causing "task not found" errors.
        include = [
            "app.workers.tasks_ops",
            "app.workers.tasks_upcoming",
            "app.workers.tasks_live",
            "app.workers.tasks_market_align",
            "app.workers.tasks_harvest_pages",
            "app.workers.tasks_harvest_b2b",
            "app.workers.tasks_align",
            "app.workers.tasks_lifecycle",   # ← NEW
        ],

        task_routes = {
            # ── Harvest ───────────────────────────────────────────────────────
            "tasks.sp.*":                       {"queue": "harvest"},
            "tasks.bt.*":                       {"queue": "harvest"},
            "tasks.od.*":                       {"queue": "harvest"},
            "tasks.bt_od.*":                    {"queue": "harvest"},
            "tasks.b2b.*":                      {"queue": "harvest"},
            "tasks.sbo.*":                      {"queue": "harvest"},
            "harvest.*":                        {"queue": "harvest"},

            # ── Live ─────────────────────────────────────────────────────────
            "tasks.live.*":                     {"queue": "live"},

            # ── Results / DB writes ───────────────────────────────────────────
            "tasks.ops.compute_ev_arb":         {"queue": "ev_arb"},
            "tasks.ops.persist*":               {"queue": "results"},
            "harvest.persist*":                 {"queue": "results"},
            "harvest.value_bets":               {"queue": "ev_arb"},
            "tasks.align.*":                    {"queue": "results"},
            "tasks.market_align.*":             {"queue": "results"},

            # ── Lifecycle (new) ───────────────────────────────────────────────
            "tasks.lifecycle.update_match_state": {"queue": "results"},
            "tasks.lifecycle.save_match_result":  {"queue": "results"},
            "tasks.lifecycle.flush_live_markets": {"queue": "results"},
            "tasks.lifecycle.notify_event":       {"queue": "notify"},
            "tasks.lifecycle.window_scan":        {"queue": "default"},

            # ── Beat / ops ────────────────────────────────────────────────────
            "tasks.ops.beat.*":                 {"queue": "default"},
            "tasks.ops.health_check":           {"queue": "default"},
            "tasks.ops.build_health*":          {"queue": "default"},
            "tasks.ops.expire*":                {"queue": "default"},

            # ── Notify ────────────────────────────────────────────────────────
            "tasks.ops.publish_ws_event":       {"queue": "notify"},
            "tasks.ops.dispatch_notify*":       {"queue": "notify"},
            "tasks.ops.send_*":                 {"queue": "notify"},
            "tasks.notify.*":                   {"queue": "notify"},

            # ── Analytics ─────────────────────────────────────────────────────
            "tasks.sp.enrich_analytics":        {"queue": "analytics"},
            "tasks.sp.get_match_analytics":     {"queue": "analytics"},
        },

        beat_schedule = {
            # ── Harvest ───────────────────────────────────────────────────────
            "sp-harvest-5min": {
                "task":     "tasks.sp.harvest_all_upcoming",
                "schedule": 300,
                "options":  {"queue": "harvest"},
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

            # ── Alignment / prune ─────────────────────────────────────────────
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

            # ── Notifications ─────────────────────────────────────────────────
            "arb-digest-5min": {
                "task":     "tasks.notify.arb_digest",
                "schedule": 300,
                "options":  {"queue": "notify"},
            },

            # ── Cleanup ───────────────────────────────────────────────────────
            "cleanup-daily-3am": {
                "task":     "harvest.cleanup",
                "schedule": 86400,
                "options":  {"queue": "results"},
            },

            # ── Lifecycle window (NEW) ────────────────────────────────────────
            # Beat-driven fallback that keeps the 3h window Redis set
            # consistent even if the leader process restarts.
            "window-scan-60s": {
                "task":     "tasks.lifecycle.window_scan",
                "schedule": 60,
                "options":  {"queue": "default"},
            },
        },
    )

    # ── App context wrapping ──────────────────────────────────────────────────

    if flask_app is not None:
        # Called from create_app() — app already exists, wrap immediately.
        class ContextTask(celery.Task):
            abstract = True

            def __call__(self, *args, **kwargs):
                with flask_app.app_context():
                    return self.run(*args, **kwargs)

        celery.Task = ContextTask

    else:
        # Called from `celery -A app.workers.celery_app worker`.
        # Create the app lazily on first task execution.
        # ENABLE_HARVESTER=0 prevents SP/BT/OD live pollers from starting
        # inside worker processes (they run in the web process only).
        class LazyContextTask(celery.Task):
            abstract = True
            _flask_app = None
            _lock      = None

            def __call__(self, *args, **kwargs):
                if self.__class__._flask_app is None:
                    import threading
                    if self.__class__._lock is None:
                        self.__class__._lock = threading.Lock()
                    with self.__class__._lock:
                        # Double-check after acquiring lock
                        if self.__class__._flask_app is None:
                            os.environ.setdefault("ENABLE_HARVESTER", "0")
                            from app import create_app
                            self.__class__._flask_app = create_app()
                with self.__class__._flask_app.app_context():
                    return self.run(*args, **kwargs)

        celery.Task = LazyContextTask

    # ── Worker signals — start window leader in Celery workers too ───────────
    # FIX: previously the window leader only started in the web process.
    # Celery workers also need it so the leader survives a web process restart.
    # worker_ready fires once per worker process after it connects to the broker.
    try:
        from celery.signals import worker_ready

        @worker_ready.connect(weak=False)
        def on_worker_ready(**kwargs):
            try:
                app = flask_app
                if app is None:
                    app = LazyContextTask._flask_app
                if app is None:
                    os.environ.setdefault("ENABLE_HARVESTER", "0")
                    from app import create_app
                    app = create_app()
                    LazyContextTask._flask_app = app
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(
                    "Window leader start failed in worker: %s", e
                )
    except Exception:
        pass  # celery.signals not available in some test environments

    return celery


# ── Module-level instance ─────────────────────────────────────────────────────
# Used by:
#   celery -A app.workers.celery_app worker --queues=harvest,results,notify
#   celery -A app.workers.celery_app beat
#   celery -A app.workers.celery_app flower
#
# flask_app is None here — ContextTask is applied later when create_app()
# calls make_celery(flask_app) a second time.
celery_app = make_celery()