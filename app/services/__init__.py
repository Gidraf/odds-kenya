# """
# app/celery_app.py
# ==================
# Entry point for Celery CLI commands.
 
# Usage (from project root):
#     celery -A app.celery_app worker --loglevel=info -Q harvest,live,default -c 4
#     celery -A app.celery_app beat   --loglevel=info
#     celery -A app.celery_app flower                       # web monitor (pip install flower)
 
#     # Dev shortcut — worker + beat in one process:
#     celery -A app.celery_app worker --beat --loglevel=info -c 2
 
#     # Check connected workers:
#     celery -A app.celery_app status
 
#     # Run a task manually for testing:
#     celery -A app.celery_app call app.workers.celery_tasks.health_check
 
# This file MUST:
#   1. Create the Flask app (so DB, config, extensions are ready)
#   2. Call make_celery(flask_app) to bind Celery to Flask
#   3. Export `celery` at module level so the CLI can find it
# """
 
# from app import create_app
# from app.extensions import make_celery                          # your Flask factory

 
# flask_app = create_app()
# celery    = make_celery(flask_app)
 
# # Expose celery at module level so -A app.celery_app resolves
# __all__ = ["celery"]