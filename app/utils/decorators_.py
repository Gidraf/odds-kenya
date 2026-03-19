from functools import wraps
from flask import g, request                     # g was missing
from app.utils.customer_jwt_helpers import _current_user_from_header, _err


def require_auth(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = _current_user_from_header()
        if not user:
            return _err("Authentication required", 401)
        g.user = user
        return f(*args, **kwargs)
    return wrapper


def require_tier(*tiers: str):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            user = _current_user_from_header()
            if not user:
                return _err("Authentication required", 401)
            if user.tier not in tiers:
                return _err(
                    f"This feature requires a {' or '.join(t.title() for t in tiers)} subscription.",
                    403,
                )
            g.user = user
            return f(*args, **kwargs)
        return wrapper
    return decorator


def log_event(event: str, meta: dict | None = None):
    """Non-blocking metrics logging."""
    try:
        from app.models.metrics import MetricsEvent
        from app.extensions import db
        user = getattr(g, "user", None)          # was "Customer" — wrong key
        MetricsEvent.log(
            event       = event,
            customer_id = user.id   if user else None,
            tier        = user.tier if user else "free",
            meta        = meta,
            ip          = request.remote_addr,
        )
        db.session.commit()
    except Exception:
        pass