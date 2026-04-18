"""
app/integration_wiring.py
==========================
Copy the relevant sections into your existing app factory and celery config.

This file is NOT meant to be imported directly — it shows exactly which
lines to add and where.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 1.  celery_config.py  (or wherever you configure Celery)
# ─────────────────────────────────────────────────────────────────────────────
"""
# app/celery_config.py

from celery.schedules import crontab

# Every 10 minutes (can also use: schedule=600)
beat_schedule = {
    "harvest-all-active-endpoints": {
        "task":     "harvest_all_active_endpoints",
        "schedule": 600,                      # seconds
        "options":  {"expires": 550},         # drop if worker hasn't picked it up
    },
}

timezone = "UTC"
broker_url        = "redis://localhost:6382/0"
result_backend    = "redis://localhost:6382/1"
task_serializer   = "json"
result_serializer = "json"
accept_content    = ["json"]
"""


# ─────────────────────────────────────────────────────────────────────────────
# 2.  app/__init__.py  (Flask app factory)
# ─────────────────────────────────────────────────────────────────────────────
"""
# app/__init__.py

from flask import Flask
from app.extensions import db, celery
from app.tasks.harvest_tasks import register_beat_schedule
from app.services.ws_odds_listener import OddsListenerRegistry


def create_app(config=None):
    app = Flask(__name__)
    app.config.from_object(config or "app.config.DevelopmentConfig")

    # Extensions
    db.init_app(app)
    celery.config_from_object("app.celery_config")

    # Register Celery Beat schedule (10-minute harvest)
    register_beat_schedule(celery)

    # Blueprints
    from app.api.research_api import bp_research
    app.register_blueprint(bp_research, url_prefix="/research")
    # ... other blueprints

    # Start WebSocket listeners (only in the main process, not in Celery workers)
    import os
    if os.environ.get("START_WS_LISTENERS", "0") == "1":
        _start_ws_listeners(app)

    return app


def _start_ws_listeners(app):
    \"\"\"
    Register and start all bookmaker WebSocket listeners.
    Called only when START_WS_LISTENERS=1 is set in the environment.
    Set this in your main web process, NOT in celery worker processes.
    \"\"\"
    from app.services.ws_odds_listener import (
        OddsListenerRegistry,
        BetwaySampleListener,
        GenericOddsChangeFeedListener,
    )

    registry = OddsListenerRegistry.get_instance(app)

    # ── Register your bookmakers ──────────────────────────────────────────
    # Replace URLs, bookmaker_ids, and headers with real values.

    registry.register(BetwaySampleListener(
        bookmaker_id = 3,
        url          = "wss://ws.betway.com/odds/football",
        headers      = {"Authorization": "Bearer YOUR_TOKEN"},
        name         = "Betway-Football",
    ))

    registry.register(GenericOddsChangeFeedListener(
        bookmaker_id = 7,
        url          = "wss://live.sportsbetting.example.com/ws",
        headers      = {"X-Api-Key": "YOUR_API_KEY"},
        name         = "Example-Generic",
    ))

    registry.start_all()

    # Graceful shutdown hook
    import atexit
    atexit.register(registry.stop_all)
"""


# ─────────────────────────────────────────────────────────────────────────────
# 3.  Running the services
# ─────────────────────────────────────────────────────────────────────────────
"""
# Terminal 1 — Flask web process (also runs WS listeners when flag is set)
START_WS_LISTENERS=1 flask run

# Terminal 2 — Celery worker (handles harvest tasks)
celery -A app.celery_worker worker --loglevel=info --concurrency=4

# Terminal 3 — Celery Beat scheduler (triggers harvest every 10 minutes)
celery -A app.celery_worker beat --loglevel=info

# Or combined worker+beat for development:
celery -A app.celery_worker worker --beat --loglevel=info --concurrency=2
"""


# ─────────────────────────────────────────────────────────────────────────────
# 4.  app/celery_worker.py  (entry point for celery CLI)
# ─────────────────────────────────────────────────────────────────────────────
"""
# app/celery_worker.py

from app import create_app
from app.extensions import celery

app = create_app()

# Push app context so tasks can use db.session, etc.
class ContextTask(celery.Task):
    def __call__(self, *args, **kwargs):
        with app.app_context():
            return self.run(*args, **kwargs)

celery.Task = ContextTask
"""


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Environment variables checklist
# ─────────────────────────────────────────────────────────────────────────────
"""
# .env
DATABASE_URL=postgresql://user:pass@localhost/db
REDIS_URL=redis://localhost:6382/0

# AI providers
GEMINI_API_KEY=your_gemini_key
OPENAI_API_KEY=your_openai_key       # optional fallback

# Langfuse prompt tracking
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_HOST=https://cloud.langfuse.com   # optional

# WebSocket listeners (set to 1 only in the web process)
START_WS_LISTENERS=1
"""


# ─────────────────────────────────────────────────────────────────────────────
# 6.  Adding a new bookmaker WebSocket listener
# ─────────────────────────────────────────────────────────────────────────────
"""
# app/services/ws_listeners/pinnacle.py

from app.services.ws_odds_listener import BookmakerWsListener, OddsDelta
import json, logging
log = logging.getLogger(__name__)


class PinnacleListener(BookmakerWsListener):
    \"\"\"
    Adapter for Pinnacle's WebSocket feed.
    Replace frame parsing with their actual format.
    \"\"\"

    def subscribe_msg(self):
        return {"type": "subscribe", "sport_ids": [1, 3]}   # 1=soccer, 3=basketball

    def on_message(self, raw: str) -> list[OddsDelta]:
        try:
            frame = json.loads(raw)
        except Exception:
            return []

        # Pinnacle sends: {"type": "odds", "match_id": "X", "market": "...", ...}
        if frame.get("type") != "odds":
            return []

        try:
            price = float(frame["price"])
            if price <= 1.0:
                return []
            return [OddsDelta(
                parent_match_id = str(frame["match_id"]),
                bookmaker_id    = self.bookmaker_id,
                market          = frame["market_type"],
                specifier       = frame.get("line"),
                selection       = frame["outcome"],
                price           = price,
            )]
        except (KeyError, ValueError):
            log.debug("Pinnacle unhandled frame: %s", raw[:100])
            return []


# Then in _start_ws_listeners():
# from app.services.ws_listeners.pinnacle import PinnacleListener
# registry.register(PinnacleListener(
#     bookmaker_id=12,
#     url="wss://ws.pinnacle.com/live/odds",
#     headers={"Authorization": "Basic YOUR_B64"},
#     name="Pinnacle",
# ))
"""