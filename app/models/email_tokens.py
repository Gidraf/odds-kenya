"""
app/models/email_tokens.py
===========================
Stores one-time-use tokens for:
  • email verification  (purpose = "verify")
  • password reset      (purpose = "reset")

Token stored as SHA-256 hash — never the raw value.
"""

from __future__ import annotations
from datetime import datetime, timezone
from app.extensions import db


class EmailToken(db.Model):
    __tablename__ = "email_tokens"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("customers.id", ondelete="CASCADE"),
                           nullable=False, index=True)
    token_hash = db.Column(db.String(64), unique=True, nullable=False, index=True)
    purpose    = db.Column(db.String(20), nullable=False)   # "verify" | "reset"
    used       = db.Column(db.Boolean, default=False, nullable=False)
    used_at    = db.Column(db.DateTime, nullable=True)
    expires_at = db.Column(db.DateTime, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.Index("ix_et_user_purpose_active", "user_id", "purpose", "used"),
    )

    @property
    def is_valid(self) -> bool:
        return (
            not self.used and
            self.expires_at.replace(tzinfo=timezone.utc) > datetime.now(timezone.utc)
        )