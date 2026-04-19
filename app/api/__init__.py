from flask import Blueprint
from app.utils.customer_jwt_helpers import _signed_response, _err, _current_user_from_header

# Create blueprints
bp_public = Blueprint("api_public", __name__, url_prefix="/api/v1/public")
bp_matches = Blueprint("api_matches", __name__, url_prefix="/api/v1")
bp_live = Blueprint("web_api_live", __name__, url_prefix="/api/v1")
bp_analytics = Blueprint("match_analytics", __name__, url_prefix="/api/v1")
bp_arbitrage = Blueprint("api_arbitrage", __name__, url_prefix="/api/v1")
bp_competitions = Blueprint("api_competitions", __name__, url_prefix="/api/v1")
bp_bookmakers = Blueprint("api_bookmakers", __name__, url_prefix="/api/v1")

# Before request: load user from token (for authenticated blueprints)
@bp_matches.before_request
@bp_live.before_request
@bp_analytics.before_request
@bp_arbitrage.before_request
@bp_competitions.before_request
@bp_bookmakers.before_request
def load_user():
    from flask import g
    g.user = _current_user_from_header()

# Import routes to register them
from . import public, matches, live, analytics, arbitrage, competitions, bookmakers