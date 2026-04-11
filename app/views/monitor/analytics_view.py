from flask import Blueprint
from app.extensions import db
from app.models.tracking_model import UserActivityLog
from app.utils.customer_jwt_helpers import _current_user_from_header, _err
from sqlalchemy import func
from datetime import datetime, timezone, timedelta

bp_analytics_dash = Blueprint("analytics_dash", __name__, url_prefix="/api/admin/analytics")

@bp_analytics_dash.route("/overview")
def get_analytics_overview():
    """
    Returns aggregated analytics data for the admin to monitor platform usage.
    """
    # user = _current_user_from_header()
    # if not user or not getattr(user, "is_admin", False):
    #     return _err("Admin access required", 403)

    now = datetime.now(timezone.utc)
    last_24h = now - timedelta(days=1)

    try:
        # 1. Total Unique IPs in the last 24h
        unique_ips_24h = db.session.query(func.count(func.distinct(UserActivityLog.ip_address)))\
            .filter(UserActivityLog.created_at >= last_24h).scalar() or 0

        # 2. Most clicked events (e.g., arb vs live vs upcomnig)
        event_breakdown = db.session.query(
            UserActivityLog.event_type, 
            func.count(UserActivityLog.id)
        ).group_by(UserActivityLog.event_type).all()
        
        events_dict = {row[0]: row[1] for row in event_breakdown}

        # 3. Most active IPs (Power Users)
        power_users = db.session.query(
            UserActivityLog.ip_address, 
            func.count(UserActivityLog.id).label('total_actions')
        ).group_by(UserActivityLog.ip_address)\
         .order_by(db.desc('total_actions')).limit(10).all()

        power_users_list = [{"ip": row[0], "actions": row[1]} for row in power_users]

        # 4. Most popular sports (Based on resource column)
        sport_clicks = db.session.query(
            UserActivityLog.resource, 
            func.count(UserActivityLog.id)
        ).filter(UserActivityLog.event_type.in_(["view_sport_live", "view_sport_upcoming"]))\
         .group_by(UserActivityLog.resource).all()

        sports_dict = {row[0]: row[1] for row in sport_clicks}

        return {
            "ok": True,
            "metrics": {
                "unique_ips_24h": unique_ips_24h,
                "event_breakdown": events_dict,
                "power_users": power_users_list,
                "popular_sports": sports_dict
            },
            "timestamp": now.isoformat()
        }

    except Exception as e:
        return _err(f"Analytics failure: {str(e)}", 500)