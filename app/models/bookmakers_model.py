"""
Bookmaker Models
=================
Updated to include betting-related fields: tax, max_bets_per_day,
min_bet_amount, max_bet_amount, currency, and optional flat tax.
Also added Bookmaker.to_dict() for easy serialization.
"""
from datetime import datetime, timezone
from app.extensions import db


class Bookmaker(db.Model):
    __tablename__ = "bookmakers"

    id = db.Column(db.Integer, primary_key=True)
    domain = db.Column(db.String(100), unique=True, nullable=False)
    name = db.Column(db.String(100), nullable=True)
    vendor_slug = db.Column(db.String(), nullable=True)  # e.g. "betika", "sportpesa", "betway"

    # AI-observed UI metadata
    brand_color = db.Column(db.String(20))
    logo_url = db.Column(db.String(255))
    payments = db.relationship("BookmakerPayment")  # New relationship to payments
    harvest_config = db.Column(db.JSON, nullable=True)  # e.g. {"odds_format": "decimal", "supports_live_betting": true}

    # Configs
    paybill_number = db.Column(db.String(50))
    sms_number = db.Column(db.String(50))
    is_active = db.Column(db.Boolean, default=False)
    needs_ui_intervention = db.Column(db.Boolean, default=False)

    # Betting-specific settings (new)
    currency = db.Column(db.String(8), nullable=True, default="KES")  # e.g. KES, USD
    min_bet_amount = db.Column(db.Numeric(12, 2), nullable=True)
    max_bet_amount = db.Column(db.Numeric(12, 2), nullable=True)
    max_bets_per_day = db.Column(db.Integer, nullable=True)

    # Tax: percentage (e.g. 15.00 for 15%) or an optional flat amount
    tax_percent = db.Column(db.Numeric(5, 2), nullable=True)
    tax_flat_amount = db.Column(db.Numeric(12, 2), nullable=True)

    # Discovery audit
    last_discovery_at = db.Column(db.DateTime, nullable=True)

    # Relationships
    endpoints = db.relationship(
        "BookmakerEndpoint", backref="bookmaker", lazy="dynamic",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Bookmaker {self.name or self.domain}>"

    def to_dict(self):
        return {
            "id": self.id,
            "domain": self.domain,
            "name": self.name,
            "brand_color": self.brand_color,
            "logo_url": self.logo_url,
            "paybill_number": self.paybill_number,
            "sms_number": self.sms_number,
            "is_active": self.is_active,
            "needs_ui_intervention": self.needs_ui_intervention,
            "currency": self.currency,
            "min_bet_amount": (
                str(self.min_bet_amount) if self.min_bet_amount is not None else None
            ),
            "max_bet_amount": (
                str(self.max_bet_amount) if self.max_bet_amount is not None else None
            ),
            "max_bets_per_day": self.max_bets_per_day,
            "tax_percent": (
                str(self.tax_percent) if self.tax_percent is not None else None
            ),
            "tax_flat_amount": (
                str(self.tax_flat_amount) if self.tax_flat_amount is not None else None
            ),
            "last_discovery_at": self.last_discovery_at.isoformat() if self.last_discovery_at else None,
        }


class BookmakerPayment(db.Model):
    """
    One row per payment channel for a bookmaker.

    Examples:
      Betika  | M-Pesa Paybill | 290290  | paybill
      Betika  | SMS shortcode  | 29002   | sms
      Betika  | Airtel Money   | 0100000 | paybill
      Sporty  | M-Pesa Till    | 123456  | till
    """
    __tablename__ = "bookmaker_payments"
    __table_args__ = (
        db.UniqueConstraint(
            "bookmaker_id", "provider", "channel_type",
            name="uq_bookmaker_payment_channel"
        ),
    )

    id           = db.Column(db.Integer, primary_key=True)
    bookmaker_id = db.Column(
        db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

    # e.g. "M-Pesa", "Airtel Money", "T-Kash", "Equitel", "Bank"
    provider     = db.Column(db.String(60), nullable=False)

    # "paybill" | "till" | "sms" | "bank_account" | "other"
    channel_type = db.Column(db.String(30), nullable=False, default="paybill")

    # The actual number / account identifier
    number       = db.Column(db.String(50), nullable=False)

    # Optional human-readable note, e.g. "Deposit only", "Withdrawals"
    label        = db.Column(db.String(100), nullable=True)

    is_active    = db.Column(db.Boolean, default=True, nullable=False)

    bookmaker = db.relationship("Bookmaker")

    def to_dict(self) -> dict:
        return {
            "id":           self.id,
            "bookmaker_id": self.bookmaker_id,
            "provider":     self.provider,
            "channel_type": self.channel_type,
            "number":       self.number,
            "label":        self.label,
            "is_active":    self.is_active,
        }

class BookmakerEndpoint(db.Model):
    """
    Stores every intercepted API endpoint from a Playwright discovery session.

    Key fields added in v2:
    - curl_command      : copy-pasteable curl for manual testing / debugging
    - sample_response   : first 2000 chars of the intercepted JSON (for AI re-training)
    - parser_test_passed: whether the AI-generated parser ran successfully on sample data
    """
    __tablename__ = "bookmaker_endpoints"

    id = db.Column(db.Integer, primary_key=True)
    bookmaker_id = db.Column(
        db.Integer, db.ForeignKey("bookmakers.id"), nullable=False, index=True
    )

    # Classification
    endpoint_type = db.Column(db.String(50), nullable=False)
    # 'MATCH_LIST' | 'LIVE_ODDS' | 'DEEP_MARKETS' | 'RESULTS' | 'OTHER'

    intercept_type = db.Column(db.String(20), default="XHR")  # 'XHR' | 'WEBSOCKET'

    # URL pattern with .* replacing dynamic IDs
    url_pattern = db.Column(db.String(500), nullable=False)
    request_method = db.Column(db.String(10), default="GET")

    # ── Auth material harvested by Playwright ─────────────────────────────────
    headers_json = db.Column(db.JSON, nullable=True)

    # ── NEW: Curl command for debugging / manual replay ───────────────────────
    curl_command = db.Column(db.Text, nullable=True)

    # ── NEW: Snippet of the intercepted response (for re-training parsers) ────
    sample_response = db.Column(db.Text, nullable=True)

    # ── AI-generated parser ───────────────────────────────────────────────────
    parser_code = db.Column(db.Text, nullable=True)

    # ── NEW: Did the parser pass the local execution test? ────────────────────
    parser_test_passed = db.Column(db.Boolean, default=False)

    # ── NEW: Decoded URL query params (stored separately for harvest looping) ─
    url_params = db.Column(db.JSON, nullable=True)

    # ── NEW: Pagination metadata from AI analysis ─────────────────────────────
    pagination_info = db.Column(db.JSON, nullable=True)

    # ── Lifecycle ─────────────────────────────────────────────────────────────
    is_active = db.Column(db.Boolean, default=True)
    discovered_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc)
    )
    last_harvested_at = db.Column(db.DateTime, nullable=True)

    def __repr__(self):
        return (
            f"<BookmakerEndpoint {self.endpoint_type} "
            f"bookmaker={self.bookmaker_id} "
            f"active={self.is_active}>"
        )

    def to_dict(self):
        return {
            "id": self.id,
            "bookmaker_id": self.bookmaker_id,
            "endpoint_type": self.endpoint_type,
            "url_pattern": self.url_pattern,
            "request_method": self.request_method,
            "parser_test_passed": self.parser_test_passed,
            "is_active": self.is_active,
            "discovered_at": self.discovered_at.isoformat() if self.discovered_at else None,
            "last_harvested_at": self.last_harvested_at.isoformat() if self.last_harvested_at else None,
            # Omit headers_json and curl_command from public API (contains auth tokens)
        }


class BookmakerEntityValue(db.Model):
    """
    Per-bookmaker mapping of internal entity IDs → bookmaker-specific external IDs.

    Why this exists:
      Different bookmakers use their own numeric/string IDs for the same
      entity (country, sport, competition, team). This table stores the
      translation so dynamic endpoint parameters can substitute the right
      bookmaker-specific ID at harvest time.
    """
    __tablename__ = "bookmaker_entity_values"
    __table_args__ = (
        db.UniqueConstraint(
            "bookmaker_id", "entity_type", "internal_id",
            name="uq_bookmaker_entity"
        ),
    )

    id           = db.Column(db.Integer, primary_key=True)
    bookmaker_id = db.Column(db.Integer, db.ForeignKey("bookmakers.id"), nullable=False)
    entity_type  = db.Column(db.String(30), nullable=False)   # country|sport|competition|team
    internal_id  = db.Column(db.Integer, nullable=False)       # logical ref — no DB FK (polymorphic)
    external_id  = db.Column(db.String(200), nullable=False)   # bookmaker's own ID string
    label        = db.Column(db.String(200))                   # human-readable note
    extra_json   = db.Column(db.Text)                          # any extra metadata as JSON

    bookmaker = db.relationship("Bookmaker", lazy="joined", foreign_keys=[bookmaker_id])