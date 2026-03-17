
from datetime import datetime, timedelta

from flask import Blueprint, request
from pytz import timezone

from app.utils.customer_jwt_helpers import _signed_response


admin_bp = Blueprint("admin", __name__, url_prefix="/api/admin")    

@admin_bp.route("/admin/metrics/summary")
def metrics_summary():
    """Internal metrics endpoint — require admin header."""
    if request.headers.get("X-Admin-Key") != os.environ.get("ADMIN_KEY", "change-me"):
        return _err("Forbidden", 403)
 
    from app.models.metrics import MetricsEvent 
    from app.models.customer import Customer as User
    from app.models.subscriptions import Subscription   
    from app.extensions import db
    import sqlalchemy as sa
 
    now   = datetime.now(timezone.utc)
    day   = now - timedelta(days=1)
    week  = now - timedelta(days=7)
 
    def count_event(event: str, since: datetime) -> int:
        return MetricsEvent.query.filter(
            MetricsEvent.event == event,
            MetricsEvent.recorded_at >= since,
        ).count()
 
    return _signed_response({
        "ok": True,
        "signups_24h":          count_event("signup", day),
        "signups_7d":           count_event("signup", week),
        "logins_24h":           count_event("login", day),
        "arb_calculator_24h":   count_event("arb_calculator", day),
        "exports_24h":          count_event("export", day),
        "api_calls_24h":        count_event("api_call", day),
        "total_users":          User.query.count(),
        "active_subscriptions": Subscription.query.filter(
            Subscription.status.in_(["active", "trial"])
        ).count(),
    })