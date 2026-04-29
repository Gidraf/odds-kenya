import os
import threading
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler

from flask import Flask
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

    # ─────────────────────────────────────────────────────────────────────────
    # LOGGING CONFIGURATION – creates /app/logs/flask.log
    # ─────────────────────────────────────────────────────────────────────────
    log_dir = Path(os.environ.get("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    flask_log = log_dir / "flask.log"
    file_handler = RotatingFileHandler(flask_log, maxBytes=10_485_760, backupCount=5)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    file_handler.setFormatter(formatter)
    flask_app.logger.addHandler(file_handler)
    logging.getLogger("werkzeug").addHandler(file_handler)
    # (Optional) basicConfig fallback
    logging.basicConfig(level=logging.INFO, handlers=[file_handler])

    # ─────────────────────────────────────────────────────────────────────────
    # CONFIGURATION
    # ─────────────────────────────────────────────────────────────────────────
    mail_username = os.getenv("MAIL_USERNAME", EMAIL_ADDRESS)
    mail_password = os.getenv("MAIL_PASSWORD", EMAIL_PASSWORD)

    flask_app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
    flask_app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY")
    flask_app.config["JWT_SECRET_KEY"] = os.environ.get("JWT_SECRET_KEY")
    flask_app.config["SQLALCHEMY_ECHO"] = False
    flask_app.config["CELERY_BROKER_URL"] = os.environ.get(
        "CELERY_BROKER_URL", os.environ.get("REDIS_URL", "redis://localhost:6379/1")
    )
    flask_app.config["CELERY_RESULT_BACKEND"] = os.environ.get(
        "CELERY_RESULT_URL", os.environ.get("REDIS_URL", "redis://localhost:6379/2")
    )
    flask_app.config["OPENAI_API_KEY"] = os.environ.get("OPENAI_API_KEY")
    flask_app.config["MAIL_SERVER"] = SMTP_SERVER
    flask_app.config["MAIL_PORT"] = SMTP_PORT
    flask_app.config["MAIL_USE_TLS"] = True
    flask_app.config["MAIL_USERNAME"] = mail_username
    flask_app.config["MAIL_PASSWORD"] = mail_password

    # ─────────────────────────────────────────────────────────────────────────
    # EXTENSIONS
    # ─────────────────────────────────────────────────────────────────────────
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

    # ─────────────────────────────────────────────────────────────────────────
    # BLUEPRINTS (all routes)
    # ─────────────────────────────────────────────────────────────────────────
    from app.views.auth import authorization
    from app.views.research import bp_research
    from app.views.odds_feed import bp_odds as odds_bp
    from app.views.bookmarkers import bookmarker
    from app.views.bookmakers_crud import bp_search
    from app.views.mapping import bp as mapping_bp
    from app.views.onboarding import bp_onboarding
    from app.views.vendors import bp_vendor
    from app.views.sbo import bp_sbo
    from app.views.admin import admin_bp
    from app.views.customer_auth import bp_customer
    from app.views.subscriptions import bp_customer_subscriptions
    from app.views.webhook import bp_interceptor
    from app.views.odds_feed.odds_view import bp_odds as bp_unified_odds
    from app.views.odds_feed.combined_module import bp_combined
    from app.views.odds_feed.odds_data_view import bp_data
    from app.views.monitor import bp_monitor
    from app.views.monitor.harvest_control import bp_harvest_ctrl
    from app.views.customer import bp_odds_customer
    from app.views.odds_feed.live_sse_routes import bp_live_sse
    from app.views.monitor.analytics_view import bp_analytics_dash
    from app.views.customer.sportradar_tracker import bp_tracker
    from app.views.customer.deep_analytics import bp_deep_analytics
    from app.views.customer.gemini_comentary import bp_commentary
    from app.views.customer.ai_story import bp_story
    from app.views.customer.analytic_debug import bp_raw_stream
    from app.views.customer.bk_streams import bp_bk_streams
    from app.api import bp_public, bp_matches, bp_live, bp_analytics, bp_arbitrage, bp_competitions, bp_bookmakers
    from app.views.odds.admin import bp_admin as debug_admin

    flask_app.register_blueprint(bp_search)
    flask_app.register_blueprint(authorization)
    flask_app.register_blueprint(bookmarker)
    flask_app.register_blueprint(bp_research)
    flask_app.register_blueprint(odds_bp)
    flask_app.register_blueprint(bp_sbo)
    flask_app.register_blueprint(mapping_bp)
    flask_app.register_blueprint(bp_vendor, url_prefix="/api/vendors")
    flask_app.register_blueprint(bp_onboarding, url_prefix="/api/onboarding")
    flask_app.register_blueprint(admin_bp)
    flask_app.register_blueprint(bp_customer_subscriptions)
    flask_app.register_blueprint(bp_customer)
    flask_app.register_blueprint(bp_story)
    flask_app.register_blueprint(bp_raw_stream)
    flask_app.register_blueprint(bp_interceptor)
    flask_app.register_blueprint(bp_unified_odds)
    flask_app.register_blueprint(bp_odds_customer)
    flask_app.register_blueprint(bp_combined)
    flask_app.register_blueprint(bp_data)
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
    flask_app.register_blueprint(bp_live)
    flask_app.register_blueprint(bp_analytics)
    flask_app.register_blueprint(bp_arbitrage)
    flask_app.register_blueprint(bp_competitions)
    flask_app.register_blueprint(bp_bookmakers)
    flask_app.register_blueprint(debug_admin)

    # ─────────────────────────────────────────────────────────────────────────
    # MODEL IMPORTS (for Flask‑Migrate)
    # ─────────────────────────────────────────────────────────────────────────
    with flask_app.app_context():
        from app.models.bookmakers_model import (
            Bookmaker, BookmakerEndpoint, BookmakerEntityValue,
            BookmakerPayment, BookmakerEntityMap, BookmakerMatchLink
        )
        from app.models.research_model import ResearchSession, ResearchFinding, ResearchEndpoint
        from app.models.odds import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
            MarketDefinition, ArbitrageOpportunity, EVOpportunity,
        )
        from app.models.competions_model import Team, Sport, Competition, Player, TeamPlayer
        from app.models.harvest_workflow import HarvestWorkflow, HarvestWorkflowStep
        from app.models.mapping_models import (
            Market, MarketAlias, TeamAlias, CompetitionAlias,
            SportAlias, BookmakerEndpointMap,
        )
        from app.models.user_admin import User
        from app.models.onboarding_model import BookmakerOnboardingSession
        from app.models.vendor_template import VendorTemplate, BookmakerVendorConfig
        from app.models.subscriptions import Subscription, SubscriptionHistory
        from app.models.notifications import NotificationPref
        from app.models.metrics import MetricsEvent
        from app.models.api_key import ApiKey
        from app.models.bank_roll import BankrollAccount, BankrollTarget
        from app.models.customer import Customer
        from app.models.email_tokens import EmailToken
        from app.models.match import (
            MatchEvent, MatchEventType, MatchLineup, PlayerPosition, MatchPeriod
        )
        from app.models.live_snapshot_model import LiveRawSnapshot
        from app.models.tracking_model import UserActivityLog
        from app.models.bookmake_competition_data import (
            BookmakerCompetitionName, BookmakerCountryName, BookmakerTeamName
        )
        from app.models.match_analytics import MatchAnalytics
        from app.models.match_ev_arb import MatchEvArb

    import app.sockets  # noqa: registers /admin namespace handlers

    # ─────────────────────────────────────────────────────────────────────────
    # BACKGROUND THREADS (live pollers) – only when ENABLE_HARVESTER=1
    # ─────────────────────────────────────────────────────────────────────────
    if os.environ.get("ENABLE_HARVESTER", "0") == "1":
        from app.workers.sp_live_harvester import start_harvester_thread
        start_harvester_thread()

        try:
            import redis as _redis_lib
            _rd = _redis_lib.from_url(
                flask_app.config.get("CELERY_BROKER_URL", "redis://localhost:6379/1"),
                decode_responses=False, socket_timeout=3,
            )
            from app.workers.od_harvester import init_live_poller as od_init
            od_init(_rd, interval=2.0)
        except Exception as e:
            print(f"[init] OdiBets live poller skipped: {e}")

        try:
            from app.workers.bt_harvester import init_live_poller as bt_init
            bt_init(_rd, interval=1.5)
        except Exception as e:
            print(f"[init] Betika live poller skipped: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # INLINE CELERY (dev mode) – only when CELERY_INLINE=1
    # ─────────────────────────────────────────────────────────────────────────
    if os.environ.get("CELERY_INLINE", "0") == "1":
        _start_inline_celery(flask_app)

    return flask_app


def _start_inline_celery(flask_app: Flask) -> None:
    """
    Start a Celery worker and beat scheduler as daemon threads inside the
    Flask process (local development only).
    """
    import celery.bin.worker as celery_worker
    import celery.bin.beat as celery_beat
    import time
    import threading

    celery_app = flask_app.celery

    def _run_worker():
        print("[celery:inline] Starting worker (concurrency=2, queue=harvest)…")
        try:
            worker = celery_app.Worker(
                queues=["harvest"],
                concurrency=2,
                loglevel="WARNING",
                logfile=None,
                pool="threads",
                without_heartbeat=False,
                without_gossip=True,
                without_mingle=True,
            )
            worker.start()
        except Exception as exc:
            print(f"[celery:inline] Worker stopped: {exc}")

    def _run_beat():
        time.sleep(3)
        print("[celery:inline] Starting beat scheduler…")
        try:
            beat = celery_app.Beat(
                loglevel="WARNING",
                logfile=None,
                schedule="/tmp/celerybeat-schedule-inline",
            )
            beat.run()
        except Exception as exc:
            print(f"[celery:inline] Beat stopped: {exc}")

    worker_thread = threading.Thread(target=_run_worker, name="celery-inline-worker", daemon=True)
    beat_thread = threading.Thread(target=_run_beat, name="celery-inline-beat", daemon=True)

    worker_thread.start()
    beat_thread.start()

    print("[celery:inline] Worker and beat threads started.")