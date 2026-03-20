import os
from flask import Flask
from dotenv import load_dotenv

from app.extensions import db, init_celery, jwt, socketio, migrate, cors
from app.views.onboarding.playwright_onboarding import bp_fetcher

load_dotenv()


EMAIL_ADDRESS = os.environ.get("ADMIN_EMAIL")
EMAIL_PASSWORD = os.environ.get("ADMIN_EMAIL_PASSWORD")
SMTP_SERVER = f"mail.{os.environ.get('DOMAIN')}"
IMAP_SERVER = f"mail.{os.environ.get('DOMAIN')}"
SMTP_PORT = 587
IMAP_PORT = 993

def create_app() -> Flask:
    flask_app = Flask(__name__, instance_relative_config=True)
    mail_username =  os.getenv("MAIL_USERNAME", EMAIL_ADDRESS)
    mail_password = os.getenv("MAIL_PASSWORD", EMAIL_PASSWORD)
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
    flask_app.config["SECRET_KEY"]              = os.environ.get("SECRET_KEY")
    flask_app.config["JWT_SECRET_KEY"]          = os.environ.get("JWT_SECRET_KEY")
    flask_app.config["SQLALCHEMY_ECHO"]         = False
    flask_app.config["CELERY_BROKER_URL"]       = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    flask_app.config["CELERY_RESULT_BACKEND"]   = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    flask_app.config["OPENAI_API_KEY"]          = os.environ.get("OPENAI_API_KEY")
    flask_app.config["MAIL_SERVER"] = SMTP_SERVER
    flask_app.config["MAIL_PORT"] = SMTP_PORT
    flask_app.config["MAIL_USE_TLS"] = True
    flask_app.config["MAIL_USERNAME"] = mail_username
    flask_app.config["MAIL_PASSWORD"] = mail_password
    flask_app.config["SQLALCHEMY_ECHO"] = False

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
        print("WARNING: Redis unavailable - SocketIO cross-process emits disabled.")

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

    # Celery: call init_celery() directly.
    # Do NOT import from celery_tasks here — that causes a circular import:
    #   celery_tasks -> _bootstrap() -> create_app() -> celery_tasks (again)
    #
    # init_celery() sets celery._flask_initialized = True, which is the guard
    # in celery_tasks._bootstrap() to prevent double-init.
    flask_app.celery = init_celery(flask_app)

    # Blueprints
    from app.views.auth import authorization
    from app.views.research import bp_research
    from app.views.odds_feed import bp_odds as odds_bp
    # from app.views.odds_feed.customer_odds_view import bp_odds as customer_odds
    from app.views.onboarding.playwright_onboarding import bp_fetcher
    from app.views.bookmarkers import bookmarker
    from app.views.bookmakers_crud import bp_search
    from app.views.mapping import bp as mapping_bp
    from app.views.onboarding import bp_onboarding
    from app.views.vendors import bp_vendor
    from app.views.sbo import bp_sbo
    from app.views.admin import admin_bp
    from app.views.customer_auth import bp_customer
    from app.views.subscriptions import bp_customer_subscriptions

    flask_app.register_blueprint(bp_search)
    flask_app.register_blueprint(authorization)
    flask_app.register_blueprint(bookmarker)
    flask_app.register_blueprint(bp_research)
    flask_app.register_blueprint(odds_bp)
    flask_app.register_blueprint(bp_sbo)
    flask_app.register_blueprint(mapping_bp)
    # flask_app.register_blueprint()
    flask_app.register_blueprint(bp_vendor,     url_prefix="/api/vendors")
    flask_app.register_blueprint(bp_onboarding, url_prefix="/api/onboarding")
    # flask_app.register_blueprint(bp_playwright, url_prefix="/api/playwright")
    flask_app.register_blueprint(admin_bp)
    flask_app.register_blueprint(bp_customer_subscriptions)
    flask_app.register_blueprint(bp_customer)
    flask_app.register_blueprint(bp_fetcher)

    # init_playwright_manager()

    with flask_app.app_context():
        from app.models.bookmakers_model import (
            Bookmaker, BookmakerEndpoint,
            BookmakerEntityValue, BookmakerPayment,
        )
        from app.models.research_model import (
            ResearchSession, ResearchFinding, ResearchEndpoint,
        )
        from app.models.odds_model import (
            BookmakerMatchOdds, BookmakerOddsHistory,
            UnifiedMatch, MarketDefinition,
        )
        from app.models.competions_model import Team, Sport, Competition
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

    import app.sockets  # noqa: registers /admin namespace handlers

    return flask_app