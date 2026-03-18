from flask_sqlalchemy import SQLAlchemy
from flask_jwt_extended import JWTManager
from flask_socketio import SocketIO
from flask_migrate import Migrate
from flask_cors import CORS
from celery import Celery

db       = SQLAlchemy()
jwt      = JWTManager()
socketio = SocketIO()
migrate  = Migrate()
cors     = CORS()
celery   = Celery()


def init_celery(app):
    """
    Bind the global Celery instance to the Flask app.

    Sets broker/backend from app.config, installs ContextTask so every
    task runs inside a Flask app context, and marks the instance as
    initialised via _flask_initialized so _bootstrap() in celery_tasks.py
    knows not to call create_app() again.

    Returns the same global celery object — NOT a new instance.
    """
    celery.conf.update(
        broker_url    = app.config.get("CELERY_BROKER_URL",    "redis://localhost:6379/0"),
        result_backend= app.config.get("CELERY_RESULT_BACKEND","redis://localhost:6379/0"),
        task_serializer  = "json",
        result_serializer= "json",
        accept_content   = ["json"],
        timezone         = "UTC",
        enable_utc       = True,
    )

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask

    # Mark as initialised — used as the guard in celery_tasks._bootstrap()
    # to avoid double-initialisation when the worker imports the module.
    celery._flask_initialized = True

    return celery