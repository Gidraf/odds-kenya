"""
Drop-in replacement for send_async_email.
Uses your self-hosted Docker SMTP via create_celery_app().
No Gmail, no OAuth, no username/password args needed in the call.
"""

import base64

from flask_mail import Mail, Message
from . import celery_service
from app import create_celery_app


@celery_service.task(
    name="app.workers.celery_tasks.send_async_email",
    bind=True,
    max_retries=3,
    default_retry_delay=30,
    soft_time_limit=60,
    time_limit=90,
)
def send_async_email(
    self,
    subject,
    recipients,
    body,
    body_type="plain",
    attachments=None,
    username=None,     # kept for backwards-compat — ignored, SMTP creds from env
    password=None,     # kept for backwards-compat — ignored
):
    """
    Send email via self-hosted Docker SMTP.

    Call it the same way as before:
        send_async_email.delay(subject, [to], html_body, "html")
        send_async_email.apply_async(args=[subject, [to], body, "html"])
    """
    try:
        app  = create_celery_app()           # reads SMTP_HOST/PORT/USER/PASS from env

        with app.app_context():
            mail = Mail(app)

            msg = Message(
                subject    = subject,
                recipients = recipients,
                sender     = app.config["MAIL_DEFAULT_SENDER"],
            )

            if body_type == "html":
                msg.html = body
            else:
                msg.body = body

            if attachments:
                for attachment in attachments:
                    filename    = attachment.get("filename", "file")
                    mimetype    = attachment.get("mimetype", "application/octet-stream")
                    content_b64 = attachment.get("content")

                    if not content_b64:
                        print(f"⚠️  Skipping {filename}: no content")
                        continue

                    try:
                        msg.attach(filename, mimetype, base64.b64decode(content_b64))
                    except Exception as e:
                        print(f"⚠️  Attachment decode failed ({filename}): {e}")

            mail.send(msg)
            print(f"✅ Email sent → {recipients}")

    except Exception as exc:
        print(f"❌ Email failed: {exc}")
        raise self.retry(exc=exc)