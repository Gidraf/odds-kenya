"""
app/api/decorators.py
======================
Auth / tier enforcement decorators.

EventSource (SSE) cannot set Authorization headers, so Pro/Premium
SSE endpoints accept `?token=<jwt>` as a fallback.

Usage:
    @bp_live.route("/upcoming/stream")
    @tier_required("pro")               # Requires Pro, Premium, or Admin
    def upcoming_stream(): ...

    @bp_live.route("/basic-only")
    @tier_required(["basic", "pro"])    # Exact match for Basic, Pro, or Admin
    def basic_and_pro_only(): ...
"""
from __future__ import annotations

import functools
import logging
from typing import Callable

import jwt
from flask import current_app, g, jsonify, request

logger = logging.getLogger(__name__)

# Tier ordering — higher index = more access
_TIER_RANK: dict[str, int] = {
    "free":    0,
    "basic":   1,
    "pro":     2,
    "premium": 3,
    "admin":   99,
}

# ─────────────────────────────────────────────────────────────────────────────


def _extract_token() -> str | None:
    """
    Try to find a JWT in:
      1. Authorization: Bearer <token>   (standard)
      2. ?token=<token>                  (EventSource / SSE fallback)
    """
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header[7:].strip()

    # SSE / EventSource fallback
    token = request.args.get("token", "").strip()
    if token:
        return token

    return None


def _decode_token(raw: str) -> dict | None:
    """Decode and validate JWT; return payload or None on failure."""
    try:
        secret = current_app.config.get("JWT_SECRET_KEY") or current_app.config.get("SECRET_KEY", "")
        payload = jwt.decode(raw, secret, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        logger.debug("JWT expired")
    except jwt.InvalidTokenError as exc:
        logger.debug("JWT invalid: %s", exc)
    return None


# ─────────────────────────────────────────────────────────────────────────────


def tier_required(required: str | list[str] | tuple[str, ...] | set[str] = "basic") -> Callable:
    """
    Decorator that enforces subscription tier limits.

    Args:
        required: 
            - If a string (e.g., "pro"), acts as a MINIMUM tier.
            - If an iterable (e.g., ["basic", "pro"]), restricts access EXACTLY 
              to those tiers.
            - Admins (rank 99) bypass these checks automatically.
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            raw = _extract_token()
            if not raw:
                return jsonify({
                    "error": "Authentication required",
                    "code":  "auth_required",
                }), 401

            payload = _decode_token(raw)
            if payload is None:
                return jsonify({
                    "error": "Invalid or expired token",
                    "code":  "auth_invalid",
                }), 401

            user_tier = (payload.get("tier") or payload.get("subscription_tier") or "basic").lower()
            user_rank = _TIER_RANK.get(user_tier, 0)
            admin_rank = _TIER_RANK.get("admin", 99)

            is_authorized = False

            if isinstance(required, str):
                # String acts as a minimum required rank
                min_rank = _TIER_RANK.get(required.lower(), 1)
                if user_rank >= min_rank:
                    is_authorized = True
            else:
                # Iterable acts as an exact match whitelist (plus admins)
                allowed_tiers = [t.lower() for t in required]
                if user_tier in allowed_tiers or user_rank >= admin_rank:
                    is_authorized = True

            if not is_authorized:
                req_display = required if isinstance(required, str) else " or ".join(required)
                return jsonify({
                    "error":    f"This endpoint requires {req_display} tier",
                    "code":     "tier_insufficient",
                    "required": required,
                    "current":  user_tier,
                    "upgrade":  "https://kinetic.bet/pricing",
                }), 403

            # Attach to Flask g for downstream use
            g.user_id      = payload.get("sub") or payload.get("user_id")
            g.tier         = user_tier
            g.token_payload = payload

            return fn(*args, **kwargs)
        return wrapper
    return decorator


def optional_auth(fn: Callable) -> Callable:
    """
    Soft auth: attaches `g.user_id` / `g.tier` if a valid token is present,
    but does NOT reject unauthenticated requests.  Tier defaults to 'free'.
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        g.user_id       = None
        g.tier          = "free"
        g.token_payload = {}
        raw = _extract_token()
        if raw:
            payload = _decode_token(raw)
            if payload:
                g.user_id       = payload.get("sub") or payload.get("user_id")
                g.tier          = (payload.get("tier") or "free").lower()
                g.token_payload = payload
        return fn(*args, **kwargs)
    return wrapper