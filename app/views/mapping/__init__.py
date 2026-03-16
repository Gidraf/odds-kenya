from flask import Blueprint


bp = Blueprint("mapping", __name__, url_prefix="/api/admin/mapping")
from . import mapping_view  