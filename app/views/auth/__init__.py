from flask import Blueprint

authorization = Blueprint('auth', __name__, url_prefix='/api/auth')

from . import auth_admin_view