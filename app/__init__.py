import os
import threading
from flask import Flask
from dotenv import load_dotenv
from app.extensions import db, init_celery, jwt, socketio, migrate, cors
from app.views.onboarding.playwright_onboarding import bp_fetcher, init_fetcher_manager

load_dotenv()

EMAIL_ADDRESS = os.environ.get("ADMIN_EMAIL")
EMAIL_PASSWORD = os.environ.get("ADMIN_EMAIL_PASSWORD")
SMTP_SERVER = f"mail.{os.environ.get('DOMAIN')}"
IMAP_SERVER = f"mail.{os.environ.get('DOMAIN')}"
SMTP_PORT = 587
IMAP_PORT = 993


def create_app() -> Flask:
    flask_app = Flask(__name__, instance_relative_config=True)

    mail_username = os.getenv("MAIL_USERNAME", EMAIL_ADDRESS)
    mail_password = os.getenv("MAIL_PASSWORD", EMAIL_PASSWORD)

    flask_app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
    flask_app.config["SECRET_KEY"]              = os.environ.get("SECRET_KEY")
    flask_app.config["JWT_SECRET_KEY"]          = os.environ.get("JWT_SECRET_KEY")
    flask_app.config["SQLALCHEMY_ECHO"]         = False
    flask_app.config["CELERY_BROKER_URL"]       = os.environ.get("CELERY_BROKER_URL", os.environ.get("REDIS_URL", "redis://localhost:6379/1"))
    flask_app.config["CELERY_RESULT_BACKEND"]   = os.environ.get("CELERY_RESULT_URL",  os.environ.get("REDIS_URL", "redis://localhost:6379/2"))
    flask_app.config["OPENAI_API_KEY"]          = os.environ.get("OPENAI_API_KEY")
    flask_app.config["MAIL_SERVER"]             = SMTP_SERVER
    flask_app.config["MAIL_PORT"]               = SMTP_PORT
    flask_app.config["MAIL_USE_TLS"]            = True
    flask_app.config["MAIL_USERNAME"]           = mail_username
    flask_app.config["MAIL_PASSWORD"]           = mail_password

    db.init_app(flask_app)
    jwt.init_app(flask_app)
    cors.init_app(flask_app, supports_credentials=True, origins="*")
    migrate.init_app(flask_app, db, compare_type=True)

    redis_url = flask_app.config["CELERY_BROKER_URL"]

    def _redis_available(url: str) -> bool:
        try:
            import redis
            r = redis.from_url(url, socket_connect_timeout=1)
            r.ping()
            return True
        except Exception:
            return False

    mq = redis_url if _redis_available(redis_url) else None
    if not mq:
        print("WARNING: Redis unavailable — SocketIO cross-process emits disabled.")

    socketio.init_app(
        flask_app,
        cors_allowed_origins="*",
        async_mode="threading",
        logger=False,
        engineio_logger=False,
        ping_timeout=60,
        ping_interval=25,
        **({"message_queue": mq, "channel": "flask-socketio"} if mq else {}),
    )

    flask_app.celery = init_celery(flask_app)

    # ── Blueprints ────────────────────────────────────────────────────────────
    from app.views.auth                              import authorization
    from app.views.research                          import bp_research
    from app.views.odds_feed                         import bp_odds as odds_bp
    from app.views.onboarding.playwright_onboarding  import bp_fetcher
    from app.views.bookmarkers                        import bookmarker
    from app.views.bookmakers_crud                    import bp_search
    from app.views.mapping                            import bp as mapping_bp
    from app.views.onboarding                         import bp_onboarding
    from app.views.vendors                            import bp_vendor
    from app.views.sbo                                import bp_sbo
    from app.views.admin                              import admin_bp
    from app.views.customer_auth                      import bp_customer
    from app.views.subscriptions                      import bp_customer_subscriptions
    from app.views.webhook                            import bp_interceptor
    from app.views.odds_feed.sportpesa_view           import bp_sp
    from app.views.odds_feed.sp_live_view             import bp_sp_live
    from app.views.odds_feed.odds_view                import bp_odds as bp_unified_odds
    from app.views.odds_feed.betika_view              import bp_betika
    from app.views.odds_feed.odibets_view             import bp as bp_od
    from app.views.odds_feed.combined_module import bp_combined 

    flask_app.register_blueprint(bp_search)
    flask_app.register_blueprint(authorization)
    flask_app.register_blueprint(bookmarker)
    flask_app.register_blueprint(bp_research)
    flask_app.register_blueprint(odds_bp)
    flask_app.register_blueprint(bp_sbo)
    flask_app.register_blueprint(mapping_bp)
    flask_app.register_blueprint(bp_vendor,     url_prefix="/api/vendors")
    flask_app.register_blueprint(bp_onboarding, url_prefix="/api/onboarding")
    flask_app.register_blueprint(admin_bp)
    flask_app.register_blueprint(bp_customer_subscriptions)
    flask_app.register_blueprint(bp_customer)
    flask_app.register_blueprint(bp_fetcher)
    flask_app.register_blueprint(bp_interceptor)
    flask_app.register_blueprint(bp_sp)
    flask_app.register_blueprint(bp_sp_live)
    flask_app.register_blueprint(bp_unified_odds)   # GET /api/odds/...
    flask_app.register_blueprint(bp_betika)          # GET /api/bt/...
    flask_app.register_blueprint(bp_od) 
    flask_app.register_blueprint(bp_combined)              # GET /api/od/...

    # ── Model imports (Flask-Migrate needs all models visible at startup) ─────
    with flask_app.app_context():
        from app.models.bookmakers_model import (
            Bookmaker, BookmakerEndpoint,
            BookmakerEntityValue, BookmakerPayment,
        )
        from app.models.research_model import (
            ResearchSession, ResearchFinding, ResearchEndpoint,
        )
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
            MarketDefinition, ArbitrageOpportunity, EVOpportunity,
        )
        from app.models.competions_model  import Team, Sport, Competition
        from app.models.harvest_workflow  import HarvestWorkflow, HarvestWorkflowStep
        from app.models.mapping_models    import (
            Market, MarketAlias, TeamAlias, CompetitionAlias,
            SportAlias, BookmakerEndpointMap,
        )
        from app.models.user_admin            import User
        from app.models.onboarding_model      import BookmakerOnboardingSession
        from app.models.vendor_template       import VendorTemplate, BookmakerVendorConfig
        from app.models.subscriptions         import Subscription, SubscriptionHistory
        from app.models.notifications         import NotificationPref
        from app.models.metrics               import MetricsEvent
        from app.models.api_key               import ApiKey
        from app.models.bank_roll             import BankrollAccount, BankrollTarget
        from app.models.customer              import Customer
        from app.models.email_tokens          import EmailToken

    import app.sockets  # noqa: registers /admin namespace handlers

    # ── Background threads ────────────────────────────────────────────────────
    # These must ONLY run in the web process (gunicorn / flask run).
    # Celery workers call create_app() too — if we start threads there we get:
    #   • Multiple WS connections to Sportpesa (one per worker replica)
    #   • Duplicate Redis pub/sub messages → duplicate odds flashes on the frontend
    #   • SP may rate-limit or ban the IP
    #
    # ENABLE_HARVESTER=1   →  set automatically by docker-compose on the `wss` service
    # ENABLE_HARVESTER=0   →  default for celery-worker / celery-beat / celery-flower
    #
    # In local dev without Docker just run:
    #   ENABLE_HARVESTER=1 flask run   (or set it in your .env)
    if os.environ.get("ENABLE_HARVESTER", "0") == "1":
        from app.workers.sp_live_harvester import start_harvester_thread
        start_harvester_thread()
        init_fetcher_manager()

        # OdiBets live poller (REST polling, 2 s interval)
        try:
            import redis as _redis_lib
            _rd = _redis_lib.from_url(
                flask_app.config.get("CELERY_BROKER_URL", "redis://localhost:6379/1"),
                decode_responses=False, socket_timeout=3,
            )
            from app.workers.od_harvester import init_live_poller as od_init
            od_init(_rd, interval=2.0)
        except Exception as _e:
            print(f"[init] OdiBets live poller skipped: {_e}")

        # Betika live poller (REST polling, 1.5 s interval)
        try:
            from app.workers.bt_harvester import init_live_poller as bt_init
            bt_init(_rd, interval=1.5)
        except Exception as _e:
            print(f"[init] Betika live poller skipped: {_e}")

    # Celery worker + beat — inline threads when NOT running as dedicated
    # Docker services (controlled by CELERY_INLINE env var).
    #
    # In Docker:   CELERY_INLINE=0  (celery-harvest + celery-beat services run separately)
    # In dev/local: CELERY_INLINE=1  (everything in one python process, no extra terminals)
    #
    # Start order: worker first (processes tasks), then beat (schedules them).
    if os.environ.get("CELERY_INLINE", "0") == "1":
        _start_inline_celery(flask_app)

    return flask_app


# ─────────────────────────────────────────────────────────────────────────────
# INLINE CELERY (dev mode — single process)
# ─────────────────────────────────────────────────────────────────────────────

def _start_inline_celery(flask_app: Flask) -> None:
    """
    Start a Celery worker and beat scheduler as daemon threads inside the
    Flask process.  Identical pattern to start_harvester_thread().

    Useful for local development — no extra terminal needed.
    NOT recommended for production (use dedicated Docker services instead).

    Worker runs with concurrency=2 so it doesn't saturate the machine.
    Beat runs the standard schedule from celery_app.beat_schedule.
    """
    import celery.bin.worker as celery_worker
    import celery.bin.beat   as celery_beat

    celery_app = flask_app.celery

    def _run_worker():
        print("[celery:inline] Starting worker (concurrency=2, queue=harvest)…")
        try:
            worker = celery_app.Worker(
                queues       = ["harvest"],
                concurrency  = 2,
                loglevel     = "WARNING",
                logfile      = None,
                pool         = "threads",   # threads pool — safe inside Flask process
                without_heartbeat = False,
                without_gossip    = True,
                without_mingle    = True,
            )
            worker.start()
        except Exception as exc:
            print(f"[celery:inline] Worker stopped: {exc}")

    def _run_beat():
        import time
        # Give the worker a moment to connect before beat starts sending tasks
        time.sleep(3)
        print("[celery:inline] Starting beat scheduler…")
        try:
            beat = celery_app.Beat(
                loglevel  = "WARNING",
                logfile   = None,
                schedule  = "/tmp/celerybeat-schedule-inline",
            )
            beat.run()
        except Exception as exc:
            print(f"[celery:inline] Beat stopped: {exc}")

    worker_thread = threading.Thread(target=_run_worker, name="celery-inline-worker", daemon=True)
    beat_thread   = threading.Thread(target=_run_beat,   name="celery-inline-beat",   daemon=True)

    worker_thread.start()
    beat_thread.start()

    print("[celery:inline] Worker and beat threads started.")