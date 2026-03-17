from datetime import datetime

from flask import request
from openai import Customer
from pytz import timezone

from app.utils.customer_jwt_helpers import _decode_token, _err, _issue_token, _signed_response
from app.utils.decorators_ import require_auth

from . import bp_customer

@bp_customer.route("/auth/register", methods=["POST"])
def register():
    data = request.get_json(force=True) or {}
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password", "")
    tier     = data.get("tier", "basic")   # default trial tier
 
    if not email or "@" not in email:
        return _err("Valid email required")
    if len(password) < 8:
        return _err("Password must be at least 8 characters")
    if tier not in ("basic", "pro", "premium"):
        return _err("Invalid tier. Choose: basic, pro, premium")
 
    from app.models.subscriptions import Subscription
    from app.models.metrics import MetricsEvent
    from app.extensions import db
 
    if Customer.query.filter_by(email=email).first():
        return _err("Email already registered", 409)
 
    user = Customer(email=email, display_name=data.get("display_name", ""))
    user.set_password(password)
    db.session.add(user)
    db.session.flush()
 
    # Start 3-day trial
    Subscription.start_trial(user.id, tier)
    db.session.commit()
 
    # Metrics
    MetricsEvent.log("signup", user_id=user.id, tier=tier, ip=request.remote_addr)
    db.session.commit()
 
    access_token  = _issue_token(user.id, "access")
    refresh_token = _issue_token(user.id, "refresh")
 
    return _signed_response({
        "ok": True,
        "user": user.to_dict(),
        "subscription": user.subscription.to_dict(),
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "trial_message": f"You have a 3-day free trial of the {tier.title()} plan.",
    }, 201)
 
 
@bp_customer.route("/auth/login", methods=["POST"])
def login():
    data     = request.get_json(force=True) or {}
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password", "")
 
    from app.models.customer import Customer
    from app.models.metrics import MetricsEvent
    from app.extensions import db
 
    user = Customer.query.filter_by(email=email, is_active=True).first()
    if not user or not user.check_password(password):
        return _err("Invalid credentials", 401)
 
    user.last_login_at = datetime.now(timezone.utc)
    MetricsEvent.log("login", user_id=user.id, tier=user.tier, ip=request.remote_addr)
    db.session.commit()
 
    return _signed_response({
        "ok":            True,
        "user":          user.to_dict(),
        "subscription":  user.subscription.to_dict() if user.subscription else None,
        "access_token":  _issue_token(user.id, "access"),
        "refresh_token": _issue_token(user.id, "refresh"),
    })
 
 
@bp_customer.route("/auth/refresh", methods=["POST"])
def refresh_token():
    data  = request.get_json(force=True) or {}
    token = data.get("refresh_token", "")
    try:
        payload = _decode_token(token)
        if payload.get("type") != "refresh":
            raise ValueError("Not a refresh token")
        user_id = int(payload["sub"])
    except Exception:
        return _err("Invalid or expired refresh token", 401)
 
    from app.models.customer import Customer
    user = Customer.query.get(user_id)
    if not user or not user.is_active:
        return _err("Account not found", 401)
 
    return _signed_response({
        "ok": True,
        "access_token": _issue_token(user_id, "access"),
    })

@bp_customer.route("/auth/me")
@require_auth
def me():
    user = g.user
    return _signed_response({
        "ok":           True,
        "user":         user.to_dict(),
        "subscription": user.subscription.to_dict() if user.subscription else None,
        "limits":       user.limits,
    })