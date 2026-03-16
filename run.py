from app import create_app

flask_app = create_app()

from app.extensions import db, init_celery, socketio
from app.models.user_admin import User
from werkzeug.security import generate_password_hash

# Expose celery_app at module level so Celery CLI can find it:
# celery -A run.celery_app worker ...
init_celery(flask_app)
celery_app = flask_app.celery


@flask_app.cli.command("seed-markets")
def seed_markets_cmd():
        from app.utils.mapping_seed import seed_all_markets
        seed_all_markets()
        print("Done.")

@flask_app.cli.command("create-admin")
def create_admin():
    email    = input("Enter admin email: ")
    password = input("Enter admin password: ")
    if User.query.filter_by(email=email).first():
        print("User already exists!")
        return
    db.session.add(User(email=email, password_hash=generate_password_hash(password)))
    db.session.commit()
    print(f"✅ Admin '{email}' created.")


@flask_app.cli.command("run-harvest")
def run_harvest():
    from app.services.agents_tasks import harvest_data
    harvest_data()


if __name__ == "__main__":
    socketio.run(flask_app, debug=True, host="0.0.0.0", port=5050, use_reloader=False, log_output=True)