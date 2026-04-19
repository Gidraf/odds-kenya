from functools import wraps
from flask import g, request
from . import _err

def tier_required(min_tier: str):
    """Decorator to enforce minimum subscription tier."""
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            user = g.user
            if not user:
                return _err("Authentication required", 401)

            tier_order = {"basic": 1, "pro": 2, "premium": 3}
            user_tier = user.tier or "basic"
            if tier_order.get(user_tier, 0) < tier_order.get(min_tier, 0):
                return _err(f"This endpoint requires at least {min_tier} tier", 403)

            if not user.subscription or not user.subscription.is_active():
                return _err("Active subscription required", 403)

            return f(*args, **kwargs)
        return decorated
    return decorator

def rate_limit(tier_limits: dict):
    """Simple in‑memory rate limiter (can be replaced with Redis)."""
    # Implementation omitted for brevity – would use Flask-Limiter or custom.
    pass