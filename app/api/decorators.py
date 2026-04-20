"""
app/api/decorators.py
======================
Auth / tier enforcement decorators.

EventSource (SSE) cannot set Authorization headers, so Pro/Premium
SSE endpoints accept `?token=<jwt>` as a fallback.

Usage:
    @bp_live.route("/upcoming/stream")
    @tier_required("pro")
    def upcoming_stream(): ...
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


def tier_required(minimum_tier: str = "basic") -> Callable:
    """
    Decorator that enforces a minimum subscription tier.

    Attaches `g.user_id`, `g.tier`, and `g.token_payload` on success.

    Args:
        minimum_tier: One of "free", "basic", "pro", "premium", "admin".
    """
    min_rank = _TIER_RANK.get(minimum_tier, 1)

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

            if user_rank < min_rank:
                return jsonify({
                    "error":    f"This endpoint requires {minimum_tier} tier or above",
                    "code":     "tier_insufficient",
                    "required": minimum_tier,
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