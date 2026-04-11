from .blueprint import bp_odds_customer

# Import routes so they attach to the blueprint
from . import routes_api
from . import routes_stream
from . import routes_meta
from . import routes_debug

__all__ = ["bp_odds_customer"]