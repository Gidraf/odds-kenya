import click
from flask import jsonify, request

from app import create_app

flask_app = create_app()

from app.extensions import db, socketio
from app.models.user_admin import User
from werkzeug.security import generate_password_hash

# Expose celery_app at module level so Celery CLI can find it:
# celery -A run.celery_app worker ...

from app.workers.celery_app import make_celery
make_celery(flask_app)
# init_celery(flask_app)
celery_app = make_celery(flask_app)



from app.cli import register_cli_commands
register_cli_commands(flask_app)
# register_cli(flask_app)

if __name__ == "__main__":
    socketio.run(flask_app, debug=True, host="0.0.0.0", port=5500, use_reloader=False, log_output=True)