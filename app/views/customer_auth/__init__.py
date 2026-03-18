



from flask import Blueprint


bp_customer = Blueprint("customer", __name__, url_prefix="/api")
from . import customer_auth