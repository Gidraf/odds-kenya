from flask import Blueprint


bp_research = Blueprint("research", __name__, url_prefix="/api/research")

from . import research_view
from . import workflows_view