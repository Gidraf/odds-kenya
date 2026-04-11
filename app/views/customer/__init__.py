from flask import Blueprint

# Create the Blueprint once
bp_odds_customer = Blueprint("customer-odds-web", __name__, url_prefix="/api")

# Import routes so they register with the blueprint
# (Flake8/Linters might complain about unused imports, this is intentional in Flask)
from . import routes_api
from . import routes_stream
from . import routes_meta
from . import routes_debug
