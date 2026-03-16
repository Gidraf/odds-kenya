import os
from flask import Flask
from celery import Celery
from dotenv import load_dotenv

from app.extensions import db, init_celery, jwt, socketio, migrate, cors
from app.services.harvest_tasks import register_beat
from app.views.onboarding.playwright_onboarding import init_playwright_manager

load_dotenv()


def make_celery(flask_app: Flask) -> Celery:
    """
    Parameter is named flask_app (not app) to avoid shadowing
    the `app` package when this module imports from itself.
    """
    broker_url  = flask_app.config.get("broker_url",      "redis://localhost:6379/0")
    backend_url = flask_app.config.get("result_backend",  "redis://localhost:6379/0")

    celery_instance = Celery(flask_app.import_name, broker=broker_url, backend=backend_url)

    celery_instance.conf.update(
        broker_url=broker_url,
        result_backend=backend_url,
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        timezone="UTC",
        enable_utc=True,
        task_routes={
            "run_playwright_discovery":    {"queue": "playwright"},
            "refresh_endpoint_headers":    {"queue": "playwright"},
            "research_unauthenticated":    {"queue": "playwright"},
            "research_authenticated":      {"queue": "playwright"},
            "harvest_all_active_endpoints":{"queue": "harvest"},
        },
        beat_schedule={
            "harvest-odds-every-minute": {
                "task": "harvest_all_active_endpoints",
                "schedule": 60.0,
                "options": {"queue": "harvest"},
            },
        },
    )

    class ContextTask(celery_instance.Task):
        def __call__(self, *args, **kwargs):
            with flask_app.app_context():
                return self.run(*args, **kwargs)

    celery_instance.Task = ContextTask
    celery_instance.set_default()
    return celery_instance


def create_app() -> Flask:
    flask_app = Flask(__name__, instance_relative_config=True)

    flask_app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
    flask_app.config["SECRET_KEY"]              = os.environ.get("SECRET_KEY")
    flask_app.config["JWT_SECRET_KEY"]          = os.environ.get("JWT_SECRET_KEY")
    flask_app.config["SQLALCHEMY_ECHO"]         = False
    flask_app.config["broker_url"]              = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    flask_app.config["result_backend"]          = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    flask_app.config["OPENAI_API_KEY"]          = os.environ.get("OPENAI_API_KEY")

    db.init_app(flask_app)
    jwt.init_app(flask_app)
    cors.init_app(flask_app, supports_credentials=True, origins="*")
    migrate.init_app(flask_app, db, compare_type=True)

    redis_url = flask_app.config.get("broker_url", "redis://localhost:6379/0")

    # Only attach message_queue if Redis is actually reachable.
    # Without it, socketio.emit() from Celery workers won't reach clients,
    # but the socket server itself will start fine.
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
        print("⚠️  WARNING: Redis unavailable — Celery→SocketIO emits disabled. Run: brew services start redis")

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

    # Celery must be created AFTER socketio so message_queue is set
    init_celery(flask_app) 
    flask_app.celery = make_celery(flask_app)

    from app.views.auth import authorization
    from app.views.research import bp_research
    from app.views.odds_feed import bp_odds as odds_bp
    from app.views.onboarding.playwright_onboarding import bp_playwright
    from app.views.bookmarkers import bookmarker
    from app.views.bookmakers_crud import bp_search
    from app.views.mapping import bp as mapping_bp
    from app.views.onboarding import bp_onboarding
    from app.views.vendors import bp_vendor
    from app.views.sbo import bp_sbo


    flask_app.register_blueprint(bp_search)
    flask_app.register_blueprint(authorization)
    flask_app.register_blueprint(bookmarker)
    flask_app.register_blueprint(bp_research)
    flask_app.register_blueprint(odds_bp)
    flask_app.register_blueprint(bp_sbo)
    flask_app.register_blueprint(mapping_bp)
    flask_app.register_blueprint(bp_vendor, url_prefix="/api/vendors" )
    flask_app.register_blueprint(bp_onboarding, url_prefix="/api/onboarding")
    # flask_app.register_blueprint(bp_odds, url_prefix="/api/odds")
    flask_app.register_blueprint(bp_playwright, url_prefix="/api/playwright")
    init_playwright_manager()
    # flask_app.register_blueprint()  # ← moved from __init__.py to avoid circular import  
    # flask_app.register_blueprint(research_bp)
    
    # register_beat(flask_app.celery)

    with flask_app.app_context():
        from app.models.bookmakers_model import Bookmaker, BookmakerEndpoint, BookmakerEntityValue, BookmakerPayment
        from app.models.research_model import ResearchSession, ResearchFinding, ResearchEndpoint
        from app.models.odds_model import BookmakerMatchOdds, BookmakerOddsHistory, UnifiedMatch, MarketDefinition
        from app.models.competions_model import Team, Sport, Competition
        from app.models.research_model import ResearchSession, ResearchFinding, ResearchEndpoint
        from app.models.harvest_workflow import HarvestWorkflow, HarvestWorkflowStep
        from app.models.mapping_models import (
                Market, MarketAlias, TeamAlias, CompetitionAlias, SportAlias,
                BookmakerEndpointMap,
            )
        from app.models.user_admin import User
        from app.models.onboarding_model import BookmakerOnboardingSession
        from app.models.vendor_template import VendorTemplate, BookmakerVendorConfig

    import app.sockets  # noqa: registers /admin namespace handlers

    return flask_app