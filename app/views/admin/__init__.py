
from datetime import datetime, timedelta, timezone
import os

from flask import Blueprint, request

from app.utils.customer_jwt_helpers import _err, _signed_response


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

@admin_bp.route("/outreach/email", methods=["POST"])
def send_outreach_email():
    """Admin-only outreach email campaign with dynamic odds report attachment."""
    if request.headers.get("X-Admin-Key") != os.environ.get("ADMIN_KEY", "change-me"):
        return _err("Forbidden", 403)
        
    import base64
    data = request.get_json(force=True) or {}
    recipients = data.get("recipients") or []
    subject = data.get("subject") or "OddsKenya Odds Report Update"
    body = data.get("body") or ""
    sport = data.get("sport")
    arb_only = data.get("arb_only", False)
    
    if not recipients:
        return _err("Recipients list is required", 400)
    if not isinstance(recipients, list):
        recipients = [recipients]
        
    attachments = []
    if sport:
        try:
            from app.views.customer.routes_api import _generate_word_document
            f_stream = _generate_word_document(sport, arb_only)
            content_b64 = base64.b64encode(f_stream.read()).decode("utf-8")
            attachments.append({
                "filename": f"OddsKenya_Report_{sport}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx",
                "content": content_b64,
                "mimetype": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            })
        except Exception as e:
            return _err(f"Failed to generate Word report attachment: {str(e)}", 500)
            
    from app.workers.celery_tasks import send_async_email
    send_async_email.apply_async(args=[
        subject,
        recipients,
        body,
        "html",
        attachments,
        os.environ.get("ADMIN_EMAIL"),
        os.environ.get("ADMIN_EMAIL_PASSWORD")
    ])
    
    return _signed_response({
        "ok": True,
        "message": f"Successfully queued outreach email with attached odds report to {len(recipients)} recipients."
    })

@admin_bp.route("/analytics/monetization")
def monetization_analytics():
    """Admin-only monetization metrics, conversions funnel, and retention analytics."""
    if request.headers.get("X-Admin-Key") != os.environ.get("ADMIN_KEY", "change-me"):
        return _err("Forbidden", 403)
        
    from app.models.metrics import MetricsEvent
    from app.models.customer import Customer as User
    from app.extensions import db
    from sqlalchemy import func
    
    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)
    
    # 1. Signup Referral Funnel
    total_signups = MetricsEvent.query.filter(MetricsEvent.event == "signup").count()
    
    signup_events = MetricsEvent.query.filter(MetricsEvent.event == "signup").all()
    referral_counts = {}
    gated_attributed_signups = 0
    for e in signup_events:
        meta = e.meta or {}
        ref_sport = meta.get("referral_sport")
        if ref_sport:
            referral_counts[ref_sport] = referral_counts.get(ref_sport, 0) + 1
            gated_attributed_signups += 1
            
    # 2. Recurring Customer Retention
    # Count customers active on >= 2 distinct days in the last 30 days
    active_logs = db.session.query(
        MetricsEvent.customer_id,
        func.count(func.distinct(func.date(MetricsEvent.recorded_at))).label("distinct_days")
    ).filter(
        MetricsEvent.event.in_(["login", "report_download"]),
        MetricsEvent.customer_id.isnot(None),
        MetricsEvent.recorded_at >= thirty_days_ago
    ).group_by(MetricsEvent.customer_id).having(func.count(func.distinct(func.date(MetricsEvent.recorded_at))) >= 2).all()
    
    recurring_users_count = len(active_logs)
    
    # 3. Report Downloads Activity
    downloads = MetricsEvent.query.filter(MetricsEvent.event == "report_download").all()
    download_counts = {}
    for d in downloads:
        sport_slug = (d.meta or {}).get("sport", "unknown")
        download_counts[sport_slug] = download_counts.get(sport_slug, 0) + 1
        
    return _signed_response({
        "ok": True,
        "funnel": {
            "total_signups": total_signups,
            "gated_attributed_signups": gated_attributed_signups,
            "referrals_by_sport": referral_counts,
            "conversion_rate_pct": round((gated_attributed_signups / max(1, total_signups)) * 100, 2)
        },
        "retention": {
            "recurring_customers_30d": recurring_users_count,
            "recurring_percentage": round((recurring_users_count / max(1, User.query.count())) * 100, 2)
        },
        "activity": {
            "total_downloads": len(downloads),
            "downloads_by_sport": download_counts
        }
    })