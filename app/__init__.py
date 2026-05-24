from app.workers.live_feed_bridge import start_live_bridge
import os
import threading
from flask import Flask, request
from dotenv import load_dotenv
from app.extensions import db, init_celery, jwt, socketio, migrate, cors

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
    from app.views.customer import config as customer_config
    flask_app.config["MAX_CONTENT_LENGTH"]      = customer_config.MAX_CONTENT_LENGTH
    flask_app.config["broker_url"]              = os.environ.get("CELERY_BROKER_URL", os.environ.get("REDIS_URL", "redis://localhost:6379/1"))
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

    @flask_app.after_request
    def add_cors_headers(response):
        origin = request.headers.get("Origin")
        if origin:
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Credentials"] = "true"
        else:
            response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With, Accept, X-Admin-Key, x-admin-key"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        return response

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

    from app.workers.celery_app import make_celery
    celery = make_celery(flask_app)

    # ── Register new lifecycle Celery tasks ───────────────────────────────────
    # BUG FIX 1: register_lifecycle_tasks was never called, so
    # tasks.ops.update_match_state / save_match_result / flush_live_markets
    # / notify.lifecycle_event would all fail with "task not found".
    try:
        from app.api.live_results_api import register_lifecycle_tasks
        register_lifecycle_tasks(celery)
    except Exception as _e:
        print(f"[init] lifecycle tasks registration skipped: {_e}")

    # ── Blueprints ────────────────────────────────────────────────────────────
    from app.views.auth                              import authorization
    from app.views.research                          import bp_research
    from app.views.odds_feed                         import bp_odds as odds_bp
    from app.views.bookmarkers                       import bookmarker
    from app.views.bookmakers_crud                   import bp_search
    from app.views.mapping                           import bp as mapping_bp
    from app.views.onboarding                        import bp_onboarding
    from app.views.vendors                           import bp_vendor
    from app.views.sbo                               import bp_sbo
    from app.views.admin                             import admin_bp
    from app.views.customer_auth                     import bp_customer
    from app.views.subscriptions                     import bp_customer_subscriptions
    from app.views.webhook                           import bp_interceptor
    from app.views.odds_feed.odds_view               import bp_odds as bp_unified_odds
    from app.views.odds_feed.combined_module         import bp_combined
    from app.views.odds_feed.odds_data_view          import bp_data
    from app.views.monitor                           import bp_monitor
    from app.views.monitor.harvest_control           import bp_harvest_ctrl
    from app.views.customer                          import bp_odds_customer
    from app.views.odds_feed.live_sse_routes         import bp_live_sse
    from app.views.monitor.analytics_view            import bp_analytics_dash
    from app.views.customer.sportradar_tracker       import bp_tracker
    from app.views.customer.deep_analytics           import bp_deep_analytics
    from app.views.customer.gemini_comentary         import bp_commentary
    from app.views.customer.ai_story                 import bp_story
    from app.views.customer.analytic_debug           import bp_raw_stream
    from app.views.customer.bk_streams               import bp_bk_streams
    from app.api.activity_api                       import bp_activity
    from app.api.match_refresh_api import bp_refresh

    # BUG FIX 2: bp_live was imported twice — second import silently overwrote
    # the first, meaning app.api's bp_live was never registered. Then the
    # same (live_results_api) bp_live was registered twice causing Flask to
    # raise "AssertionError: The name 'live_api' is already registered."
    #
    # Fix: import live_results_api blueprints under distinct aliases.
    from app.api import (
        bp_public, bp_matches,
        bp_live,            # app.api's existing live blueprint — keep original name
        bp_analytics, bp_arbitrage, bp_competitions, bp_bookmakers,
    )
    from app.api.odds_stream    import bp_stream, bp_monitor as bp_monitor_new, _register_lifecycle
    from app.api.notifications  import bp_notify
    from app.views.odds.admin   import bp_admin as debug_admin
    from app.api.video_render   import video_bp
    from app.api.narration      import bp_odds_narration
    # from app.workers.match_lifecycle import bp_lifecycle

    # New blueprints from live_results_api — aliased to avoid name collision
    from app.api.live_results_api import (
        bp_results,
        bp_live as bp_live_window,   # Blueprint("live_api", url_prefix="/api/live")
    )

    flask_app.register_blueprint(bp_stream)
    flask_app.register_blueprint(bp_monitor_new)
    flask_app.register_blueprint(bp_sbo)
    flask_app.register_blueprint(mapping_bp)
    flask_app.register_blueprint(bp_vendor,     url_prefix="/api/vendors")
    flask_app.register_blueprint(bp_onboarding, url_prefix="/api/onboarding")
    flask_app.register_blueprint(admin_bp)
    flask_app.register_blueprint(bp_customer_subscriptions)
    flask_app.register_blueprint(bp_customer)
    flask_app.register_blueprint(bp_odds_customer)
    flask_app.register_blueprint(bp_story)
    flask_app.register_blueprint(bp_raw_stream)
    flask_app.register_blueprint(bp_interceptor)
    flask_app.register_blueprint(bp_monitor)
    flask_app.register_blueprint(bp_harvest_ctrl)
    flask_app.register_blueprint(bp_live_sse)
    flask_app.register_blueprint(bp_analytics_dash)
    flask_app.register_blueprint(bp_tracker)
    flask_app.register_blueprint(bp_deep_analytics)
    flask_app.register_blueprint(bp_commentary)
    flask_app.register_blueprint(bp_bk_streams)
    flask_app.register_blueprint(bp_public)
    flask_app.register_blueprint(bp_matches)
    flask_app.register_blueprint(bp_live)          # app.api existing live routes
    flask_app.register_blueprint(bp_analytics)
    flask_app.register_blueprint(bp_arbitrage)
    flask_app.register_blueprint(bp_competitions)
    flask_app.register_blueprint(bp_bookmakers)
    flask_app.register_blueprint(debug_admin)
    flask_app.register_blueprint(bp_notify)
    flask_app.register_blueprint(bp_results)       # GET /api/results/<sport>
    flask_app.register_blueprint(bp_live_window)   # GET /api/live/*, SSE /api/live/stream/*
    flask_app.register_blueprint(video_bp)
    flask_app.register_blueprint(bp_odds_narration)
    # flask_app.register_blueprint(bp_lifecycle)     # /api/matches/* watch routes
    flask_app.register_blueprint(bp_activity)
    flask_app.register_blueprint(bp_refresh)

    # BUG FIX 3: _register_lifecycle already calls start_lifecycle_manager()
    # internally (inside a with app.app_context() block). Calling it again
    # immediately after starts TWO MatchLifecycleManager threads, meaning
    # every state-change fires notifications twice and every DB write happens
    # twice. Remove the bare start_lifecycle_manager() call.
    # _register_lifecycle(flask_app)
    start_live_bridge()
    # start_lifecycle_manager()  ← REMOVED: already called inside _register_lifecycle

    # ── Model imports (Flask-Migrate needs all models visible at startup) ─────
    with flask_app.app_context():
        from app.models.bookmakers_model import (
            Bookmaker, BookmakerEndpoint,
            BookmakerEntityValue, BookmakerPayment, BookmakerEntityMap,
            BookmakerMatchLink, HarvestJob, Countries, BookmakerCountry,
            MarketFailure, BkTier, BkStatus, PaymentMethod,
        )
        from app.models.research_model import (
            ResearchSession, ResearchFinding, ResearchEndpoint,
        )
        from app.models.odds import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
            MarketDefinition, ArbitrageOpportunity, EVOpportunity,
        )
        from app.models.competions_model  import Team, Sport, Competition, Player, TeamPlayer
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
        from app.models.match                 import (
            MatchEvent, MatchEventType, MatchLineup, PlayerPosition, MatchPeriod,
        )
        from app.models.live_snapshot_model   import LiveRawSnapshot
        from app.models.tracking_model        import UserActivityLog
        from app.models.bookmake_competition_data import (
            BookmakerCompetitionName, BookmakerCountryName, BookmakerTeamName,
        )
        from app.models.match_analytics       import MatchAnalytics
        from app.models.match_ev_arb          import MatchEvArb

    import app.sockets  # noqa: registers /admin namespace handlers

    # ── Background threads (harvester) ────────────────────────────────────────
    # ENABLE_HARVESTER=1 → set on the `wss` Docker service only.
    # ENABLE_HARVESTER=0 → default for celery-worker / celery-beat / celery-flower.
    if os.environ.get("ENABLE_HARVESTER", "0") == "1":
        from app.workers.sp_live_harvester import start_harvester_thread
        # start_harvester_thread()

        try:
            import redis as _redis_lib
            _rd = _redis_lib.from_url(
                flask_app.config.get("CELERY_BROKER_URL", "redis://localhost:6379/1"),
                decode_responses=False, socket_timeout=3,
            )
            from app.workers.od_harvester import init_live_poller as od_init
            # od_init(_rd, interval=2.0)
        except Exception as _e:
            print(f"[init] OdiBets live poller skipped: {_e}")

        try:
            from app.workers.bt_harvester import init_live_poller as bt_init
            # bt_init(_rd, interval=1.5)
        except Exception as _e:
            print(f"[init] Betika live poller skipped: {_e}")

    return flask_app


# ─────────────────────────────────────────────────────────────────────────────
# INLINE CELERY (dev mode — single process)
# ─────────────────────────────────────────────────────────────────────────────

def _start_inline_celery(flask_app: Flask) -> None:
    import celery.bin.worker as celery_worker
    import celery.bin.beat   as celery_beat

    celery_app = flask_app.celery

    def _run_worker():
        print("[celery:inline] Starting worker (concurrency=2, queue=harvest)…")
        try:
            worker = celery_app.Worker(
                queues            = ["harvest"],
                concurrency       = 2,
                loglevel          = "WARNING",
                logfile           = None,
                pool              = "threads",
                without_heartbeat = False,
                without_gossip    = True,
                without_mingle    = True,
            )
            worker.start()
        except Exception as exc:
            print(f"[celery:inline] Worker stopped: {exc}")

    def _run_beat():
        import time
        time.sleep(3)
        print("[celery:inline] Starting beat scheduler…")
        try:
            beat = celery_app.Beat(
                loglevel = "WARNING",
                logfile  = None,
                schedule = "/tmp/celerybeat-schedule-inline",
            )
            beat.run()
        except Exception as exc:
            print(f"[celery:inline] Beat stopped: {exc}")

    threading.Thread(target=_run_worker, name="celery-inline-worker", daemon=True).start()
    threading.Thread(target=_run_beat,   name="celery-inline-beat",   daemon=True).start()
    print("[celery:inline] Worker and beat threads started.")