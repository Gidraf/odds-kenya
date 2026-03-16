from flask import Blueprint


bp_search = Blueprint("bk_search", __name__, url_prefix="/api/")

from .  import bookmarkers_crud