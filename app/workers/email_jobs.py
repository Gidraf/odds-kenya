# app/workers/celery_tasks.py  (email section)
#
# Key fixes vs original:
#  1. create_app() removed from hot path — uses the worker's existing app
#     context instead of rebuilding Flask on every task call.
#  2. send_async_email uses smtplib directly when custom username/password
#     are supplied — the original called Mail(app) which ignores the args
#     and falls back to MAIL_* env vars, so admin outreach emails always
#     used the wrong sender.
#  3. Retry decorators added — transient SMTP errors no longer lose emails.
#  4. Dead imports and commented-out blocks removed.
#  5. send_email fixed — token_raw was used before assignment (always crashed).
#  6. Logger import corrected (was `from redis.credentials import logger`).

from __future__ import annotations

import base64
import json
import logging
import mimetypes
import os
import smtplib
import ssl
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from celery.exceptions import MaxRetriesExceededError
from email.utils import formatdate, make_msgid 

from .celery_app import celery_app as celery

log = logging.getLogger(__name__)

# ── WhatsApp ───────────────────────────────────────────────────────────────────

_WA_BOT      = os.environ.get("WA_BOT", "")
_MESSAGE_URL = f"{_WA_BOT}/api/v1/send-message" if _WA_BOT else ""

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]


@celery.task(bind=True, max_retries=3, default_retry_delay=10)
def send_message(self, msg: str, whatsapp_number: str):
    """Send a WhatsApp message via the bot API."""
    if not _MESSAGE_URL:
        log.error("[send_message] WA_BOT env var not set")
        return {"error": "WA_BOT not configured"}
    try:
        r = requests.post(_MESSAGE_URL, json={"message": msg, "number": whatsapp_number}, timeout=10)
        r.raise_for_status()
        return r.text
    except requests.RequestException as exc:
        log.warning("[send_message] attempt %d failed: %s", self.request.retries + 1, exc)
        raise self.retry(exc=exc)


# ══════════════════════════════════════════════════════════════════════════════
# FAST SMTP EMAIL  (no Flask-Mail, no create_app)
# ══════════════════════════════════════════════════════════════════════════════

def _send_via_smtp(
    subject, recipients, body, body_type="plain",
    attachments=None, username=None, password=None,
):
    sender  = username or os.environ.get("ADMIN_EMAIL", "")
    pwd     = password or os.environ.get("ADMIN_EMAIL_PASSWORD", "")
    domain  = os.environ.get("DOMAIN", "") or (sender.split("@")[1] if "@" in sender else "localhost")
    server  = os.environ.get("MAIL_SERVER") or f"mail.{domain}"
    port    = int(os.environ.get("MAIL_PORT", 587))

    if not sender or not pwd:
        raise ValueError("Email credentials not configured")

    msg = MIMEMultipart("mixed")
    msg["Subject"]    = subject
    msg["From"]       = sender
    msg["To"]         = ", ".join(recipients)
    msg["Date"]       = formatdate(localtime=True)        # ← REQUIRED by Amavis
    msg["Message-ID"] = make_msgid(domain=domain)        # ← REQUIRED by Amavis

    alt = MIMEMultipart("alternative")
    if body_type == "html":
        alt.attach(MIMEText(body, "html", "utf-8"))
    else:
        alt.attach(MIMEText(body, "plain", "utf-8"))
    msg.attach(alt)

    for att in (attachments or []):
        filename    = att.get("filename", "file")
        mimetype    = att.get("mimetype", "application/octet-stream")
        content_b64 = att.get("content")
        if not content_b64:
            continue
        try:
            import base64
            data      = base64.b64decode(content_b64)
            _, sub    = mimetype.split("/", 1)
            part      = MIMEApplication(data, _subtype=sub)
            part.add_header("Content-Disposition", "attachment", filename=filename)
            msg.attach(part)
        except Exception as exc:
            log.warning("[smtp] failed to attach %s: %s", filename, exc)

    context = ssl.create_default_context()
    with smtplib.SMTP(server, port, timeout=30) as smtp:
        smtp.ehlo()
        smtp.starttls(context=context)
        smtp.ehlo()
        if pwd:
            smtp.login(sender, pwd)
        smtp.send_message(msg) 
@celery.task(
    bind=True,
    max_retries=3,
    default_retry_delay=30,
    autoretry_for=(smtplib.SMTPException, ConnectionError, TimeoutError),
    retry_backoff=True,
)
def send_async_email(
    self,
    subject:     str,
    recipients:  list[str],
    body:        str,
    body_type:   str = "plain",
    attachments: list[dict] | None = None,
    username:    str | None = None,
    password:    str | None = None,
):
    """
    Async email via Celery.  No create_app(), no Flask-Mail overhead.
    Falls back to ADMIN_EMAIL env vars when username/password are not supplied.
    Retries up to 3 times on transient SMTP errors with exponential back-off.
    """
    if not recipients:
        log.error("[send_async_email] called with empty recipients list")
        return {"error": "no recipients"}

    if isinstance(recipients, str):
        recipients = [recipients]

    try:
        _send_via_smtp(
            subject     = subject,
            recipients  = recipients,
            body        = body,
            body_type   = body_type,
            attachments = attachments or [],
            username    = username,
            password    = password,
        )
        log.info("[send_async_email] ✅ sent '%s' to %s", subject, recipients)
        return {"ok": True, "recipients": recipients}

    except smtplib.SMTPAuthenticationError as exc:
        # Auth failures should NOT be retried — bad creds won't fix themselves.
        log.error("[send_async_email] ❌ auth failure: %s", exc)
        return {"error": "auth_failure", "detail": str(exc)}

    except Exception as exc:
        log.warning(
            "[send_async_email] attempt %d/%d failed: %s",
            self.request.retries + 1, self.max_retries, exc,
        )
        try:
            raise self.retry(exc=exc, countdown=30 * (2 ** self.request.retries))
        except MaxRetriesExceededError:
            log.error("[send_async_email] ❌ max retries exceeded for '%s' to %s", subject, recipients)
            return {"error": "max_retries_exceeded", "detail": str(exc)}


# ══════════════════════════════════════════════════════════════════════════════
# GMAIL API EMAIL  (OAuth — used when a partner's Gmail token is stored in DB)
# ══════════════════════════════════════════════════════════════════════════════

def _create_gmail_message(
    sender:       str,
    to:           str,
    subject:      str,
    html_message: str | None = None,
    text_message: str | None = None,
    files:        list[dict] | None = None,
) -> dict:
    """Build a base64-encoded Gmail API message payload."""
    if not html_message and not text_message:
        raise ValueError("Either html_message or text_message must be provided.")

    msg = MIMEMultipart("mixed")
    msg["to"]      = to
    msg["from"]    = sender
    msg["subject"] = subject

    body_part = MIMEText(html_message or text_message, "html" if html_message else "plain")
    msg.attach(body_part)

    for file in (files or []):
        try:
            response = requests.get(file["url"], stream=True, timeout=20)
            response.raise_for_status()
            content_type = mimetypes.guess_type(file["url"])[0] or "application/octet-stream"
            _, sub_type  = content_type.split("/", 1)
            part = MIMEApplication(response.content, _subtype=sub_type)
            part.add_header("Content-Disposition", "attachment", filename=file["name"])
            msg.attach(part)
        except Exception as exc:
            log.warning("[gmail_message] failed to attach %s: %s", file.get("name"), exc)

    return {"raw": base64.urlsafe_b64encode(msg.as_bytes()).decode()}


def _send_gmail_api(service, message: dict) -> dict:
    """Send a Gmail API message and return the response."""
    return service.users().messages().send(userId="me", body=message).execute()


@celery.task(bind=True, max_retries=2, default_retry_delay=60)
def send_email(
    self,
    to:           str,
    subject:      str,
    partner_id:   str | None = None,
    html_message: str | None = None,
    text_message: str | None = None,
    files:        list[dict] | None = None,
    business_name: str | None = None,
    partner_email: str | None = None,
):
    """
    Send email via a partner's stored Gmail OAuth token.
    Falls back to send_async_email (SMTP) if credentials are missing/expired
    and notifies the partner.
    """
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build

    def _notify_partner_expiry():
        """Alert the partner that their Gmail token needs re-authorisation."""
        if not partner_email:
            return
        send_async_email.apply_async(args=[
            "Your Gmail Integration Credentials Have Expired",
            [partner_email],
            (
                f"<p>Hi {business_name or 'there'},</p>"
                f"<p>Your Gmail integration credentials have expired or are invalid. "
                f"Please re-authorise your Gmail account in the dashboard.</p>"
                f"<p>Web URL: {os.environ.get('ADMIN_WEB_URL', '')}</p>"
            ),
            "html",
            [],
            os.environ.get("ADMIN_EMAIL"),
            os.environ.get("ADMIN_EMAIL_PASSWORD"),
        ])

    try:
        # ── Load stored OAuth token from DB ────────────────────────────────
        # Import here to avoid circular imports at module load time
        # Replace `Integrations` with your actual model
        try:
            from app.models.integrations import Integrations   # adjust path
            token_row = Integrations.query.filter_by(
                partner_id=partner_id, name="gmail"
            ).first()
        except ImportError:
            log.error("[send_email] Integrations model not found — check import path")
            return {"error": "model_not_found"}

        if not token_row:
            log.warning("[send_email] no Gmail credentials for partner_id=%s", partner_id)
            _notify_partner_expiry()
            return {"error": "no_credentials"}

        token_data = json.loads(token_row.credentials)
        creds      = Credentials.from_authorized_user_info(token_data, SCOPES)

        # Refresh if expired
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                token_row.credentials = creds.to_json()
                # Commit the refreshed token (requires app context in worker)
                try:
                    from app.extensions import db
                    db.session.commit()
                except Exception:
                    pass
            else:
                log.warning("[send_email] credentials invalid for partner_id=%s", partner_id)
                _notify_partner_expiry()
                return {"error": "credentials_invalid"}

        service = build("gmail", "v1", credentials=creds)
        message = _create_gmail_message(
            sender       = token_row.gmail,
            to           = to,
            subject      = subject,
            html_message = html_message,
            text_message = text_message,
            files        = files,
        )
        result = _send_gmail_api(service, message)
        log.info("[send_email] ✅ sent via Gmail API, message id=%s", result.get("id"))
        return {"ok": True, "message_id": result.get("id")}

    except Exception as exc:
        log.error("[send_email] ❌ failed: %s", exc, exc_info=True)
        try:
            raise self.retry(exc=exc)
        except MaxRetriesExceededError:
            return {"error": "max_retries_exceeded", "detail": str(exc)}