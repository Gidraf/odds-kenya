"""
app/views/subscriptions/auth_routes.py
========================================
Customer auth routes with:
  • POST /auth/register          — register + send verification email
  • POST /auth/login             — login (checks if email verified)
  • POST /auth/refresh           — refresh access token
  • GET  /auth/me                — current user info
  • GET  /auth/verify-email      — verify email via token in URL
  • POST /auth/resend-verify     — resend verification email
  • POST /auth/forgot-password   — send password reset email
  • POST /auth/reset-password    — apply new password from reset token
"""

from __future__ import annotations

import os
import secrets
import hashlib
from datetime import datetime, timezone, timedelta

from flask import render_template, request, g, current_app

from app.utils.customer_jwt_helpers import (
    _decode_token, _err, _issue_token, _signed_response,
)
from app.utils.decorators_ import require_auth
from . import bp_customer


# ─── Token helpers ────────────────────────────────────────────────────────────

def _make_email_token(user_id: int, purpose: str, expires_hours: int = 24) -> str:
    """
    Create a short-lived, signed URL-safe token for email verification
    or password reset.  Stored as SHA-256 hash in the DB.

    Format:  <random_hex>.<user_id>.<purpose>  (URL-safe, not JWT)
    """
    random_part  = secrets.token_urlsafe(32)
    payload_raw  = f"{random_part}.{user_id}.{purpose}"
    return payload_raw


def _hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode()).hexdigest()


def _send_verification_email(user, raw_token: str) -> None:
    """Queue the welcome+verify email via Celery."""
    from app.workers.celery_tasks import send_async_email   # your existing celery task

    app_url   = os.environ.get("APP_URL", "https://oddskenya.com")
    verify_url = f"{app_url}/auth/verify-email?token={raw_token}"

    # Render the Jinja2 HTML template
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    env = Environment(
        loader      = FileSystemLoader("app/templates"),
        autoescape  = select_autoescape(["html"]),
    )
    # template = env.get_template("welcome_email.html")
    # html_body = template.render(
    #     display_name     = user.display_name or user.email.split("@")[0],
    #     tier             = user.tier,
    #     verification_url = verify_url,
    #     trial_ends       = (datetime.now(timezone.utc) + timedelta(days=3)).strftime("%d %b %Y"),
    #     app_url          = app_url,
    #     year             = datetime.now(timezone.utc).year,
    # )
    html_body = render_template(
        "welcome_email.html",
        display_name     = user.display_name or user.email.split("@")[0],
        tier             = user.tier,
        verification_url = verify_url,
        trial_ends       = (datetime.now(timezone.utc) + timedelta(days=3)).strftime("%d %b %Y"),
        app_url          = app_url,
        year             = datetime.now(timezone.utc).year,
    )
    
    send_async_email.apply_async(args=[
        "Welcome to OddsKenya — Please Verify Your Email ⚽",
        [user.email],
        html_body,
        "html",
        [],
        os.environ.get("ADMIN_EMAIL"),
        os.environ.get("ADMIN_EMAIL_PASSWORD"),
    ])


def _send_password_reset_email(user, raw_token: str) -> None:
    """Queue the password reset email via Celery."""
    from app.workers.celery_tasks import send_async_email

    app_url   = os.environ.get("APP_URL", "https://oddskenya.com")
    reset_url = f"{app_url}/auth/reset-password?token={raw_token}"

    from jinja2 import Environment, FileSystemLoader, select_autoescape
    env = Environment(
        loader     = FileSystemLoader("app/templates"),
        autoescape = select_autoescape(["html"]),
    )
    template  = env.get_template("password_reset_email.html")
    print("password",template)
    html_body = template.render(
        display_name = user.display_name or user.email.split("@")[0],
        email        = user.email,
        reset_url    = reset_url,
        app_url      = app_url,
        year         = datetime.now(timezone.utc).year,
    )

    send_async_email.apply_async(args=[
        "Reset Your OddsKenya Password 🔐",
        [user.email],
        html_body,
        "html",
        [],
        os.environ.get("ADMIN_EMAIL"),
        os.environ.get("ADMIN_EMAIL_PASSWORD"),
    ])


# =============================================================================
# POST /auth/register
# =============================================================================

@bp_customer.route("/auth/register", methods=["POST"])
def register():
    data     = request.get_json(force=True) or {}
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password", "")
    tier     = data.get("tier", "basic")

    if not email or "@" not in email:
        return _err("Valid email required")
    if len(password) < 8:
        return _err("Password must be at least 8 characters")
    if tier not in ("basic", "pro", "premium"):
        return _err("Invalid tier. Choose: basic, pro, premium")

    from app.models.customer     import Customer
    from app.models.subscriptions import Subscription
    from app.models.metrics      import MetricsEvent
    from app.models.email_tokens import EmailToken
    from app.extensions          import db

    if Customer.query.filter_by(email=email).first():
        return _err("Email already registered", 409)

    user = Customer(email=email, display_name=data.get("display_name", ""))
    user.set_password(password)
    user.is_verified = False          # must verify email before full access
    db.session.add(user)
    # db.session.flush()

    # Subscription.start_trial(user.id, tier)

    # Create verification token
    raw_token = _make_email_token("user.id", "verify")
    # token_rec = EmailToken(
    #     user_id    = user.id,
    #     token_hash = _hash_token(raw_token),
    #     purpose    = "verify",
    #     expires_at = datetime.now(timezone.utc) + timedelta(hours=24),
    # )
    # db.session.add(token_rec)
    # db.session.commit()

    # Queue welcome + verification email
    try:
        _send_verification_email(user, raw_token)
    except Exception as exc:
        current_app.logger.error(f"[auth] verification email failed for {email}: {exc}")

    MetricsEvent.log("signup", user_id=user.id, tier=tier, ip=request.remote_addr)
    db.session.commit()

    access_token  = _issue_token(user.id, "access")
    refresh_token = _issue_token(user.id, "refresh")

    return _signed_response({
        "ok":            True,
        # "user":          user.to_dict(),
        # "subscription":  user.subscription.to_dict(),
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "trial_message": f"You have a 3-day free trial of the {tier.title()} plan.",
        "verify_notice": "A verification email has been sent. Please check your inbox.",
    }, 201)


# =============================================================================
# POST /auth/login
# =============================================================================

@bp_customer.route("/auth/login", methods=["POST"])
def login():
    data     = request.get_json(force=True) or {}
    email    = (data.get("email") or "").strip().lower()
    password = data.get("password", "")

    from app.models.customer import Customer
    from app.models.metrics  import MetricsEvent
    from app.extensions      import db

    user = Customer.query.filter_by(email=email, is_active=True).first()
    if not user or not user.check_password(password):
        return _err("Invalid credentials", 401)

    user.last_login_at = datetime.now(timezone.utc)
    MetricsEvent.log("login", user_id=user.id, tier=user.tier, ip=request.remote_addr)
    db.session.commit()

    resp = {
        "ok":            True,
        "user":          user.to_dict(),
        "subscription":  user.subscription.to_dict() if user.subscription else None,
        "access_token":  _issue_token(user.id, "access"),
        "refresh_token": _issue_token(user.id, "refresh"),
    }

    # Warn if email not verified (still allow login but surface the notice)
    if not user.is_verified:
        resp["verify_notice"] = (
            "Your email is not yet verified. "
            "Some features may be limited until you verify."
        )

    return _signed_response(resp)


# =============================================================================
# POST /auth/refresh
# =============================================================================

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
        "ok":           True,
        "access_token": _issue_token(user_id, "access"),
    })


# =============================================================================
# GET /auth/me
# =============================================================================

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


# =============================================================================
# GET /auth/verify-email?token=<raw_token>
# =============================================================================

@bp_customer.route("/auth/verify-email", methods=["GET"])
def verify_email():
    raw_token = request.args.get("token", "").strip()
    if not raw_token:
        return _err("Missing verification token", 400)

    from app.models.email_tokens import EmailToken
    from app.extensions          import db

    token_hash = _hash_token(raw_token)
    now        = datetime.now(timezone.utc)

    rec = EmailToken.query.filter_by(
        token_hash = token_hash,
        purpose    = "verify",
        used       = False,
    ).first()

    if not rec:
        return _err("Invalid or already used verification link.", 400)

    if rec.expires_at.replace(tzinfo=timezone.utc) < now:
        return _err("Verification link has expired. Please request a new one.", 410)

    from app.models.customer import Customer
    user = Customer.query.get(rec.user_id)
    if not user:
        return _err("Account not found.", 404)

    user.is_verified = True
    rec.used         = True
    rec.used_at      = now
    db.session.commit()

    return _signed_response({
        "ok":      True,
        "message": "Email verified successfully! You now have full access.",
        "user":    user.to_dict(),
    })


# =============================================================================
# POST /auth/resend-verify
# =============================================================================

@bp_customer.route("/auth/resend-verify", methods=["POST"])
def resend_verify():
    data  = request.get_json(force=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        return _err("Email required")

    from app.models.customer     import Customer
    from app.models.email_tokens import EmailToken
    from app.extensions          import db

    user = Customer.query.filter_by(email=email, is_active=True).first()
    # Always return OK to avoid email enumeration
    if not user or user.is_verified:
        return _signed_response({"ok": True, "message": "If the email exists and is unverified, a new link has been sent."})

    # Invalidate old tokens
    EmailToken.query.filter_by(
        user_id = user.id,
        purpose = "verify",
        used    = False,
    ).update({"used": True, "used_at": datetime.now(timezone.utc)})

    raw_token = _make_email_token(user.id, "verify")
    rec = EmailToken(
        user_id    = user.id,
        token_hash = _hash_token(raw_token),
        purpose    = "verify",
        expires_at = datetime.now(timezone.utc) + timedelta(hours=24),
    )
    db.session.add(rec)
    db.session.commit()

    try:
        _send_verification_email(user, raw_token)
    except Exception as exc:
        current_app.logger.error(f"[auth] resend-verify email failed: {exc}")

    return _signed_response({"ok": True, "message": "Verification email re-sent. Please check your inbox."})


# =============================================================================
# POST /auth/forgot-password
# =============================================================================

@bp_customer.route("/auth/forgot-password", methods=["POST"])
def forgot_password():
    data  = request.get_json(force=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        return _err("Email required")

    from app.models.customer     import Customer
    from app.models.email_tokens import EmailToken
    from app.extensions          import db

    user = Customer.query.filter_by(email=email, is_active=True).first()
    # Anti-enumeration: always return the same response
    ok_resp = _signed_response({
        "ok":      True,
        "message": "If an account with that email exists, a reset link has been sent.",
    })

    if not user:
        return ok_resp

    # Rate-limit: only one valid reset token at a time per user
    recent = EmailToken.query.filter_by(
        user_id = user.id,
        purpose = "reset",
        used    = False,
    ).filter(
        EmailToken.expires_at > datetime.now(timezone.utc)
    ).first()

    if recent:
        # Already sent a valid token in the last hour — don't spam
        return ok_resp

    raw_token = _make_email_token(user.id, "reset")
    rec = EmailToken(
        user_id    = user.id,
        token_hash = _hash_token(raw_token),
        purpose    = "reset",
        expires_at = datetime.now(timezone.utc) + timedelta(hours=1),
    )
    db.session.add(rec)
    db.session.commit()

    try:
        _send_password_reset_email(user, raw_token)
    except Exception as exc:
        current_app.logger.error(f"[auth] password-reset email failed for {email}: {exc}")

    return ok_resp


# =============================================================================
# POST /auth/reset-password
# =============================================================================

@bp_customer.route("/auth/reset-password", methods=["POST"])
def reset_password():
    data      = request.get_json(force=True) or {}
    raw_token = (data.get("token") or "").strip()
    password  = data.get("password", "")

    if not raw_token:
        return _err("Reset token is required.", 400)
    if len(password) < 8:
        return _err("Password must be at least 8 characters.")

    from app.models.customer     import Customer
    from app.models.email_tokens import EmailToken
    from app.extensions          import db

    token_hash = _hash_token(raw_token)
    now        = datetime.now(timezone.utc)

    rec = EmailToken.query.filter_by(
        token_hash = token_hash,
        purpose    = "reset",
        used       = False,
    ).first()

    if not rec:
        return _err("Invalid or already used reset link.", 400)

    if rec.expires_at.replace(tzinfo=timezone.utc) < now:
        return _err("Reset link has expired. Please request a new one.", 410)

    user = Customer.query.get(rec.user_id)
    if not user:
        return _err("Account not found.", 404)

    user.set_password(password)
    rec.used    = True
    rec.used_at = now

    # Invalidate all other reset tokens for this user
    EmailToken.query.filter_by(
        user_id = user.id,
        purpose = "reset",
        used    = False,
    ).update({"used": True, "used_at": now})

    db.session.commit()

    return _signed_response({
        "ok":      True,
        "message": "Password reset successfully. You can now log in with your new password.",
    })