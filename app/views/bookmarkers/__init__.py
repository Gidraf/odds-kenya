from flask import Blueprint

bookmarker = Blueprint('bookmakers', __name__, url_prefix='/api/admin/sports-data')

from . import bookmarkers_view
# from . import research_view