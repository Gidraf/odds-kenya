# from app.model.admin.workgroups import Workgroup
import base64
import datetime
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from flask import render_template
from jinja2 import Environment, FileSystemLoader, select_autoescape

from flask_mail import Mail, Message
import io
import mimetypes
import random
import time
from random import randint

from redis.credentials import logger

# from app.model.odds import FreeOdds, OddsGeneratedData
# from app.model.tips import Competitions, Countries, Sports, Tips, TipsDetails
# from pypdf import PdfMerger
import requests
from sqlalchemy import or_, and_
from celery.schedules import crontab
# from datetime import datetime
# from flask import render_template, url_for

# import africastalking
import os
import os
from . import create_celery_app
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
# from playwright.sync_api import sync_playwright
import arrow
import json
# import base64

# Initialize SDK
# username = os.environ["YOUR_USERNAME"]   # use 'sandbox' for development in the test environment
# api_key = os.environ["SMS_API_KEY"]     # use your sandbox app API key for development in the test environment
# africastalking.initialize(username, api_key)
whatsapp_bot =  os.environ["WA_BOT"]
message_url = f"{whatsapp_bot}/api/v1/send-message"
SCOPES = [
    # "openid",
    # "email",
    # "profile",
    "https://www.googleapis.com/auth/gmail.modify",
    # "https://www.googleapis.com/auth/calendar"
]


from app.extensions import celery as celery_service, init_celery


def make_celery(app):
    """
    Bind the global Celery instance to the Flask app.
    Calls extensions.init_celery() (sets broker/backend + ContextTask),
    then layers on harvest-specific task_routes + beat_schedule.
    Returns the same global celery object — NOT a new instance.
    """
    init_celery(app)
    celery_service.conf.update(
        task_acks_late             = True,
        worker_prefetch_multiplier = 1,
        task_reject_on_worker_lost = True,
        task_default_queue         = "default",
        worker_max_tasks_per_child = 500,
    )
    return celery_service
# =============================================================================
# Redis cache helpers
# =============================================================================


def _redis():
    import redis as _r
    url  = celery.conf.broker_url or "redis://localhost:6379/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    return _r.Redis.from_url(
        f"{base}/2",
        decode_responses       = False,
        socket_timeout         = 5,
        socket_connect_timeout = 5,
        retry_on_timeout       = True,
    )


def cache_set(key: str, data, ttl: int = 600) -> bool:
    try:
        _redis().set(key, json.dumps(data, default=str), ex=ttl)
        return True
    except Exception as e:
        logger.warning(f"[cache:set] {key}: {e}")
        return False


def cache_get(key: str):
    try:
        raw = _redis().get(key)
        return json.loads(raw) if raw else None
    except Exception as e:
        logger.warning(f"[cache:get] {key}: {e}")
        return None


def cache_keys(pattern: str) -> list[str]:
    try:
        return [k.decode() if isinstance(k, bytes) else k
                for k in _redis().keys(pattern)]
    except Exception as e:
        logger.warning(f"[cache:keys] {pattern}: {e}")
        return []


def task_status_set(name: str, status: dict):
    cache_set(f"task_status:{name}", {
        **status,
        "updated_at": datetime.now(datetime.timezone.utc).isoformat(),
    }, ttl=300)



def _bootstrap():
    """
    Auto-initialise when loaded as the direct -A target:
        celery -A app.workers.celery_tasks worker ...

    Guard: uses a custom _flask_initialized flag set by init_celery().
    This avoids the broker_url pitfall — Celery() sets a default amqp://
    broker even when unconfigured, so `if celery.conf.broker_url` is
    always truthy and would incorrectly skip bootstrapping.

    Safe to call multiple times (idempotent).
    """
    if getattr(celery_service, '_flask_initialized', False):
        # Already initialised (e.g. imported via app.celery_app) — skip.
        return
    try:
        from app import create_app
        flask_app = create_app()
        # make_celery() is fully defined by the time _bootstrap() runs
        # (it is called at the very bottom of this file, after all defs).
        make_celery(flask_app)
        logger.info("[celery_tasks] self-bootstrapped via create_app()")
    except Exception as exc:
        logger.error(f"[celery_tasks] bootstrap failed: {exc}")
        raise


@celery_service.task
def send_message(msg, whatsapp_number):
    r = requests.post(message_url, json={"message":msg,"number":whatsapp_number})
    return r.text


def send_email_message(service, user_id, message):
    """Send an email message.

    Args:
    service: Authorized Gmail API service instance.
    user_id: User's email address. The special value "me"
    can be used to indicate the authenticated user.
    message: Message to be sent.

    Returns:
    Sent Message.
    """
    try:
        message = (service.users().messages().send(userId=user_id, body=message)
                .execute())
        print ('Message Id: %s' % message['id'])
        return message
    except Exception as error:
        print ('An error occurred: %s' % error)

def create_message(sender, to, subject, message_text):
  """Create a message for an email.
  Args:
    sender: Email address of the sender.
    to: Email address of the receiver.
    subject: The subject of the email message.
    message_text: The text of the email message.
  Returns:
    An object containing a base64url encoded email object.
  """
  message = MIMEText(message_text,"html")
  message['to'] = to
  message['from'] = sender
  message['subject'] = subject
  b64_bytes = base64.urlsafe_b64encode(message.as_bytes())
  b64_string = b64_bytes.decode()
  return {'raw': b64_string}

            

@celery_service.task
def send_async_email(subject, recipients, body, body_type="plain", attachments=None, username=None, password=None):
    """
    Send an email asynchronously using Celery and Flask-Mail.
    Supports base64-encoded attachments with varying MIME types.
    """

    try:
        app = create_celery_app(username, password)

        with app.app_context():
            mail = Mail(app)

            msg = Message(
                subject=subject,
                sender=username,
                recipients=recipients,
            )

            # Set the body format (plain text or HTML)
            if body_type == "html":
                msg.html = body
            else:
                msg.body = body

            # Handle attachments
            if attachments:
                for attachment in attachments:
                    """
                    Expected attachment format:
                    {
                        "filename": "report.pdf",
                        "content": "<base64 string>",
                        "mimetype": "application/pdf"
                    }
                    """
                    filename = attachment.get("filename", "file.pdf")
                    mimetype = attachment.get("mimetype", "application/pdf")
                    content_b64 = attachment.get("content")

                    if not content_b64:
                        print(f"⚠️ Skipping attachment {filename}: No content provided.")
                        continue

                    try:
                        file_data = base64.b64decode(content_b64)
                        msg.attach(filename, mimetype, file_data)
                    except Exception as decode_err:
                        print(f"❌ Failed to decode attachment {filename}: {decode_err}")

            # Send the email
            mail.send(msg)
            print(f"✅ Email sent successfully to {recipients}")

    except Exception as e:
        print("❌ Email sending failed:", e)




@celery_service.task
def send_email(
    to,
    subject,
    html_message=None,
    text_message=None,
    files=None,
    partner_id=None,
    business_name=None,
    partner_email=None,
):
    """
    Send email with optional HTML content, plain text fallback, and attachments.
    Args:
        to (str): Recipient email address.
        subject (str): Subject of the email.
        html_message (str): HTML content for the email body (optional).
        text_message (str): Plain text content for the email body (optional).
        files (list): List of dicts with 'url' and 'name' for attachments.
        partner_id (str): Partner ID for fetching Gmail credentials.
    """
    app = create_celery_app()
    with app.app_context():
        env = Environment(
                loader=FileSystemLoader("app/templates"),
                    autoescape=select_autoescape(["html"])
        )
        template = env.get_template("gmail-token-expiry.html")
        body = template.render(
                        customer_name=business_name,
                        web_url=os.environ.get("ADMIN_WEB_URL"),)
        # Load Google credentials from DB
        try:
            # token_raw = Integrations.query.filter_by(partner_id=partner_id, name="gmail").first()
            if not token_raw:
                return f"No credentials found for partner_id: {partner_id}"

            token_data = json.loads(token_raw.credentials)
            creds = Credentials.from_authorized_user_info(token_data, SCOPES)

            # Refresh if needed
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    task = send_async_email.apply_async(args=[
                        "Credentials Has Expired",
                        [partner_email],
                        body,
                        "html",
                        [],
                        os.environ.get("ADMIN_EMAIL"),
                        os.environ.get("ADMIN_EMAIL_PASSWORD")
                    ])
                    raise RuntimeError("Credentials invalid or expired.")

            # Save refreshed credentials
            token_raw.credentials = creds.to_json()

            service = build('gmail', 'v1', credentials=creds)

            sender = token_raw.gmail
            message = create_message_with_attachment(
                sender=sender,
                to=to,
                subject=subject,
                html_message=html_message,
                text_message=text_message,
                files=files
            )

            # Send email
            send_email_message(service, "me", message)

            return "Email sent successfully."
        except Exception as e:
            print("Error sending email:", e)
            send_async_email.apply_async(args=[
                    "Gmail Credentials Has Expired",
                    [partner_email],
                    body,
                    "html",
                    [],
                    os.environ.get("ADMIN_EMAIL"),
                    os.environ.get("ADMIN_EMAIL_PASSWORD")
                ])
            return str(e)


def create_message_with_attachment(sender, to, subject, html_message=None, text_message=None, files=None):
    """
    Create a Gmail API message with optional HTML/text body and attachments.
    """
    if not html_message and not text_message:
        raise ValueError("Either html_message or text_message must be provided.")

    message = MIMEMultipart()
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject

    # Add HTML or plain text
    if html_message:
        msg_body = MIMEText(html_message, 'html')
    else:
        msg_body = MIMEText(text_message, 'plain')
    message.attach(msg_body)

    # Attach files if provided
    if files:
        for file in files:
            response = requests.get(file['url'], stream=True)
            if response.status_code != 200:
                continue  # Skip failed download

            content_type, encoding = mimetypes.guess_type(file['url'])
            if content_type is None or encoding is not None:
                content_type = 'application/octet-stream'
            main_type, sub_type = content_type.split('/', 1)

            attachment = MIMEApplication(response.content, _subtype=sub_type)
            attachment.add_header(
                'Content-Disposition',
                'attachment',
                filename=file['name']
            )
            message.attach(attachment)

    # Encode message
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    return {'raw': raw_message}

_bootstrap()