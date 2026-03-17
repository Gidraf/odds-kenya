from flask import request, Blueprint

from app.utils.customer_jwt_helpers import _err, _signed_response
from app.utils.decorators_ import log_event, require_auth



from app.utils.fetcher_utils import TIER_LIMITS, TIER_PRICES


bp_customer_subscriptions = Blueprint("customer_subscriptions", __name__, url_prefix="/api")

@bp_customer_subscriptions.route("/subscribe/tiers")
def list_tiers():

    tiers = []
    for tier, price in TIER_PRICES.items():
        tiers.append({
            "tier":        tier,
            "name":        tier.title(),
            "amount_kes":  price["amount_kes"],
            "period_days": price["period_days"],
            "trial_days":  price["trial_days"],
            "features":    TIER_LIMITS[tier],
        })
    return _signed_response({"ok": True, "tiers": tiers})
 
 
@bp_customer_subscriptions.route("/subscribe/start", methods=["POST"])
@require_auth
def start_subscription():
    data = request.get_json(force=True) or {}
    tier = data.get("tier", "basic")
    if tier not in ("basic", "pro", "premium"):
        return _err("Invalid tier")
 
    from app.models.subscriptions import Subscription, SubscriptionStatus
    from app.extensions import db
 
    user = g.user
    if user.subscription and user.subscription.is_currently_active:
        return _err("Already have an active subscription. Cancel first.", 409)
 
    sub = Subscription.start_trial(user.id, tier)
    db.session.commit()
 
    log_event("trial_started", {"tier": tier})
    return _signed_response({"ok": True, "subscription": sub.to_dict()}, 201)
 
 
@bp_customer_subscriptions.route("/subscribe/activate", methods=["POST"])
@require_auth
def activate_subscription():
    """Called by payment webhook or manually after M-Pesa confirmation."""
    data = request.get_json(force=True) or {}
    ref  = data.get("payment_reference", "")
 
    from app.extensions import db
    user = g.user
    if not user.subscription:
        return _err("No pending subscription")
 
    user.subscription.activate(payment_reference=ref)
    db.session.commit()
    log_event("subscription_activated", {"tier": user.tier, "ref": ref})
    return _signed_response({"ok": True, "subscription": user.subscription.to_dict()})
 
 
@bp_customer_subscriptions.route("/subscribe/cancel", methods=["POST"])
@require_auth
def cancel_subscription():
    data   = request.get_json(force=True) or {}
    reason = data.get("reason", "")
 
    from app.extensions import db
    if not g.user.subscription:
        return _err("No active subscription")
 
    g.user.subscription.cancel(reason)
    db.session.commit()
    log_event("subscription_cancelled", {"reason": reason})
    return _signed_response({"ok": True, "message": "Subscription cancelled."})