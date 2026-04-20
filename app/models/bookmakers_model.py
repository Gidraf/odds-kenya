"""
Bookmaker Models
=================
Updated to include betting-related fields: tax, max_bets_per_day,
min_bet_amount, max_bet_amount, currency, and optional flat tax.
Also added Bookmaker.to_dict() for easy serialization.
"""
from datetime import datetime, timezone
from app.extensions import db
from app.models.enums_tools import _utcnow_naive


class Bookmaker(db.Model):
    __tablename__ = "bookmakers"

    id = db.Column(db.Integer, primary_key=True)
    domain = db.Column(db.String(100), unique=True, nullable=False)
    name = db.Column(db.String(100), nullable=True)
    vendor_slug = db.Column(db.String(), nullable=True)  # e.g. "betika", "sportpesa", "betway"

    # AI-observed UI metadata
    brand_color = db.Column(db.String(20))
    logo_url = db.Column(db.String(255))
    
    payments = db.relationship(
        "BookmakerPayment", 
        back_populates="bookmaker", 
        lazy="dynamic"  # (Keep whatever lazy/cascade settings you already had)
    )
    # payments = db.relationship("BookmakerPayment")  # New relationship to payments
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


class BookmakerEntityMap(db.Model):
    """
    Maps our canonical entity IDs to each bookmaker's external IDs.
 
    Examples:
      bookmaker=Betika, entity_type=team, our_id=42, external_id="BT_TEAM_8899",
        external_name="Arsenal FC"
 
      bookmaker=SportPesa, entity_type=competition, our_id=7,
        external_id="SP_LEAGUE_123", external_name="English Premier League"
 
    This replaces stringly-typed BookmakerEntityValue with proper structure.
    """
    __tablename__ = "bookmaker_entity_maps"
 
    id           = db.Column(db.Integer, primary_key=True)
    bookmaker_id = db.Column(db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"),
                             nullable=False, index=True)
 
    # What type of entity: "sport", "country", "competition", "team", "player"
    entity_type  = db.Column(db.String(20), nullable=False, index=True)
 
    # Our canonical ID (the FK target depends on entity_type — polymorphic)
    canonical_id = db.Column(db.Integer, nullable=False, index=True)
 
    # The bookmaker's own ID for this entity
    external_id   = db.Column(db.String(200), nullable=False)
    # The bookmaker's display name for this entity
    external_name = db.Column(db.String(250), nullable=True)
 
    # Optional betradar cross-ref stored at mapping time
    betradar_id = db.Column(db.String(32), nullable=True)
 
    is_verified = db.Column(db.Boolean, default=False,
                            comment="True if mapping was confirmed by betradar_id match")
    created_at  = db.Column(db.DateTime, default=_utcnow_naive)
    updated_at  = db.Column(db.DateTime, default=_utcnow_naive, onupdate=_utcnow_naive)
 
    __table_args__ = (
        db.UniqueConstraint("bookmaker_id", "entity_type", "canonical_id",
                            name="uq_bem_bk_type_canonical"),
        db.Index("ix_bem_bk_external", "bookmaker_id", "entity_type", "external_id"),
        db.Index("ix_bem_canonical",   "entity_type", "canonical_id"),
    )
 
    @classmethod
    def get_canonical_id(cls, bookmaker_id: int, entity_type: str,
                         external_id: str) -> int | None:
        """Look up our canonical ID from a bookmaker's external ID."""
        row = cls.query.filter_by(
            bookmaker_id=bookmaker_id,
            entity_type=entity_type,
            external_id=external_id,
        ).first()
        return row.canonical_id if row else None
 
    @classmethod
    def get_external_id(cls, bookmaker_id: int, entity_type: str,
                        canonical_id: int) -> str | None:
        """Look up a bookmaker's external ID from our canonical ID."""
        row = cls.query.filter_by(
            bookmaker_id=bookmaker_id,
            entity_type=entity_type,
            canonical_id=canonical_id,
        ).first()
        return row.external_id if row else None
 
    @classmethod
    def upsert(cls, bookmaker_id: int, entity_type: str, canonical_id: int,
               external_id: str, external_name: str | None = None,
               betradar_id: str | None = None) -> "BookmakerEntityMap":
        row = cls.query.filter_by(
            bookmaker_id=bookmaker_id,
            entity_type=entity_type,
            canonical_id=canonical_id,
        ).first()
        if not row:
            row = cls(
                bookmaker_id=bookmaker_id,
                entity_type=entity_type,
                canonical_id=canonical_id,
                external_id=external_id,
                external_name=external_name,
                betradar_id=betradar_id,
            )
            db.session.add(row)
        else:
            row.external_id   = external_id
            row.external_name = external_name or row.external_name
            if betradar_id:
                row.betradar_id = betradar_id
                row.is_verified = True
        db.session.flush()
        return row
 
    def to_dict(self) -> dict:
        return {
            "id":            self.id,
            "bookmaker_id":  self.bookmaker_id,
            "entity_type":   self.entity_type,
            "canonical_id":  self.canonical_id,
            "external_id":   self.external_id,
            "external_name": self.external_name,
            "betradar_id":   self.betradar_id,
            "is_verified":   self.is_verified,
        }
 
 
# ═════════════════════════════════════════════════════════════════════════════
# BOOKMAKER ↔ MATCH LINK  (maps each bk's match ID to our UnifiedMatch)
# ═════════════════════════════════════════════════════════════════════════════
 
class BookmakerMatchLink(db.Model):
    """
    Explicit: "For UnifiedMatch 42, Betika calls it BT_12345,
    SportPesa calls it SP_67890, OdiBets calls it OD_ABCDE."
    """
    __tablename__ = "bookmaker_match_links"
 
    id           = db.Column(db.Integer, primary_key=True)
    match_id     = db.Column(
        db.Integer, db.ForeignKey("unified_matches.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    bookmaker_id = db.Column(
        db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
 
    # The bookmaker's own match / event ID
    external_match_id = db.Column(db.String(100), nullable=False)
 
    # Betradar ID as seen by this bookmaker (may differ in format)
    betradar_id = db.Column(db.String(64), nullable=True, index=True)
 
    # Optional deep-link URL to the match on the bookmaker's site
    match_url = db.Column(db.String(500), nullable=True)
 
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
 
    __table_args__ = (
        db.UniqueConstraint("match_id", "bookmaker_id", name="uq_bml_match_bk"),
        db.Index("ix_bml_bk_external", "bookmaker_id", "external_match_id"),
    )
 
    @classmethod
    def upsert(cls, match_id: int, bookmaker_id: int,
               external_match_id: str, betradar_id: str | None = None,
               match_url: str | None = None) -> "BookmakerMatchLink":
        row = cls.query.filter_by(match_id=match_id, bookmaker_id=bookmaker_id).first()
        if not row:
            row = cls(match_id=match_id, bookmaker_id=bookmaker_id,
                      external_match_id=external_match_id,
                      betradar_id=betradar_id, match_url=match_url)
            db.session.add(row)
        else:
            row.external_match_id = external_match_id
            if betradar_id:
                row.betradar_id = betradar_id
            if match_url:
                row.match_url = match_url
        db.session.flush()
        return row
 
    def to_dict(self) -> dict:
        return {
            "id":                self.id,
            "match_id":          self.match_id,
            "bookmaker_id":      self.bookmaker_id,
            "external_match_id": self.external_match_id,
            "betradar_id":       self.betradar_id,
            "match_url":         self.match_url,
        }
