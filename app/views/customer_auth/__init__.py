
from celery.bootsteps import Blueprint


bp_customer = Blueprint("customer", __name__, url_prefix="/api")