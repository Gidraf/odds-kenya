"""
app/models/bookmaker_catalog.py
================================
Single source of truth for all bookmaker configuration.
Replaces ALL hardcoded BK dicts scattered across the codebase.

Tables
------
  bookmakers          Core BK info (name, slug, logo, domain, tier)
  countries           Country list with flag emoji + currency
  bookmaker_countries BK ↔ Country with per-country payment methods + API URL
  market_failures     Rolling log of markets that fail per BK (for monitoring)
  harvest_jobs        Per-sport harvest job log with timing + counts
"""
from __future__ import annotations
import enum
from datetime import datetime, timezone
from app.extensions import db
from app.models.enums_tools import _utcnow_naive


def _now():
    return datetime.now(timezone.utc)


# =============================================================================
# ENUMS
# =============================================================================

class BkTier(str, enum.Enum):
    LOCAL   = "local"       # SP, BT, OD — Kenya-facing
    B2B     = "b2b"         # 1xBet, Melbet etc. — international via B2B API
    CUSTOM  = "custom"      # any future BK


class BkStatus(str, enum.Enum):
    ACTIVE   = "active"
    PAUSED   = "paused"
    DISABLED = "disabled"


class PaymentMethod(str, enum.Enum):
    MPESA        = "mpesa"
    AIRTEL_MONEY = "airtel_money"
    BANK         = "bank"
    CARD         = "card"
    CRYPTO       = "crypto"
    USSD         = "ussd"
    VOUCHER      = "voucher"


# =============================================================================
# BOOKMAKER
# =============================================================================

class Bookmaker(db.Model):
    __tablename__ = "bookmakers"

    id           = db.Column(db.Integer,     primary_key=True)
    slug         = db.Column(db.String(32),  nullable=True, unique=True, index=True)
    name         = db.Column(db.String(64),  nullable=True)
    short_code   = db.Column(db.String(8),   nullable=True)   # SP, BT, OD, 1X …
    tier         = db.Column(db.Enum(BkTier), nullable=True, default=BkTier.LOCAL)
    status       = db.Column(db.Enum(BkStatus), nullable=True, default=BkStatus.ACTIVE)

    # Display
    logo_url     = db.Column(db.Text)        # MinIO URL or CDN URL
    primary_color = db.Column(db.String(16))  # hex e.g. #22C55E
    website_url  = db.Column(db.Text)

    # API integration
    api_base_url = db.Column(db.Text)        # base URL for harvest API
    api_key_env  = db.Column(db.String(64))  # env var name that holds API key
    requires_auth = db.Column(db.Boolean, default=True)

    # Sports this BK covers (JSON list of canonical sport slugs)
    supported_sports = db.Column(db.JSON, default=list)

    # Harvest config
    harvest_interval_seconds = db.Column(db.Integer, default=300)
    max_pages_per_sport      = db.Column(db.Integer, default=30)
    page_size                = db.Column(db.Integer, default=50)
    redis_ttl_seconds        = db.Column(db.Integer, default=3600)

    # Metadata
    created_at   = db.Column(db.DateTime(timezone=True), default=_now)
    updated_at   = db.Column(db.DateTime(timezone=True), default=_now, onupdate=_now)

    # Relationships
    countries    = db.relationship("BookmakerCountry", back_populates="bookmaker",
                                   cascade="all, delete-orphan")
    market_failures = db.relationship("MarketFailure", back_populates="bookmaker",
                                      cascade="all, delete-orphan")
    harvest_jobs = db.relationship("HarvestJob", back_populates="bookmaker",
                                   cascade="all, delete-orphan")

    def to_dict(self) -> dict:
        return {
            "id":               self.id,
            "slug":             self.slug,
            "name":             self.name,
            "short_code":       self.short_code,
            "tier":             self.tier.value,
            "status":           self.status.value,
            "logo_url":         self.logo_url,
            "primary_color":    self.primary_color,
            "website_url":      self.website_url,
            "api_base_url":     self.api_base_url,
            "supported_sports": self.supported_sports or [],
            "harvest_interval": self.harvest_interval_seconds,
        }

    def __repr__(self):
        return f"<Bookmaker {self.slug}>"


# =============================================================================
# COUNTRY
# =============================================================================

class Countries(db.Model):
    __tablename__ = "bk_countries"

    id           = db.Column(db.Integer,    primary_key=True)
    code         = db.Column(db.String(3),  nullable=False, unique=True, index=True)  # ISO 3166-1 alpha-2
    name         = db.Column(db.String(64), nullable=False)
    flag_emoji   = db.Column(db.String(8))
    currency_code = db.Column(db.String(8))   # KES, USD, EUR …
    currency_symbol = db.Column(db.String(8)) # KSh, $, €

    bookmakers   = db.relationship("BookmakerCountry", back_populates="country",
                                   cascade="all, delete-orphan")

    def to_dict(self) -> dict:
        return {
            "code":            self.code,
            "name":            self.name,
            "flag_emoji":      self.flag_emoji,
            "currency_code":   self.currency_code,
            "currency_symbol": self.currency_symbol,
        }


# =============================================================================
# BOOKMAKER ↔ COUNTRY  (join table with extra data)
# =============================================================================

class BookmakerCountry(db.Model):
    __tablename__ = "bookmaker_countries"

    id             = db.Column(db.Integer, primary_key=True)
    bookmaker_id   = db.Column(db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"), nullable=False)
    country_id     = db.Column(db.Integer, db.ForeignKey("bk_countries.id", ondelete="CASCADE"), nullable=False)

    # Country-specific BK config
    local_domain   = db.Column(db.Text)     # e.g. ke.sportpesa.com
    api_url        = db.Column(db.Text)     # country-specific API endpoint
    is_primary     = db.Column(db.Boolean, default=False)  # main operating country
    is_licensed    = db.Column(db.Boolean, default=True)

    # Payment methods available in this country (JSON list of PaymentMethod values)
    payment_methods = db.Column(db.JSON, default=list)

    # Min/max deposit in local currency
    min_deposit    = db.Column(db.Numeric(12, 2))
    max_deposit    = db.Column(db.Numeric(12, 2))
    min_withdrawal = db.Column(db.Numeric(12, 2))
    

    # Metadata
    notes          = db.Column(db.Text)

    bookmaker      = db.relationship("Bookmaker",       back_populates="countries")
    country        = db.relationship("Countries",         back_populates="bookmakers")

    __table_args__ = (
        db.UniqueConstraint("bookmaker_id", "country_id", name="uq_bk_country"),
    )

    def to_dict(self) -> dict:
        return {
            "bookmaker_slug":  self.bookmaker.slug if self.bookmaker else None,
            "country_code":    self.country.code   if self.country   else None,
            "local_domain":    self.local_domain,
            "api_url":         self.api_url,
            "is_primary":      self.is_primary,
            "payment_methods": self.payment_methods or [],
            "min_deposit":     float(self.min_deposit)    if self.min_deposit    else None,
            "max_deposit":     float(self.max_deposit)    if self.max_deposit    else None,
            "min_withdrawal":  float(self.min_withdrawal) if self.min_withdrawal else None,
        }


# =============================================================================
# MARKET FAILURE LOG
# =============================================================================

class MarketFailure(db.Model):
    """
    Tracks which markets fail to parse per bookmaker.
    Written during harvest — helps prioritise mapper fixes.
    """
    __tablename__ = "market_failures"

    id            = db.Column(db.Integer, primary_key=True)
    bookmaker_id  = db.Column(db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"), nullable=False)
    sport_slug    = db.Column(db.String(32), nullable=False, index=True)
    market_name   = db.Column(db.String(128), nullable=False, index=True)
    failure_count = db.Column(db.Integer, default=1)
    last_error    = db.Column(db.Text)
    last_seen     = db.Column(db.DateTime(timezone=True), default=_now, onupdate=_now)
    first_seen    = db.Column(db.DateTime(timezone=True), default=_now)

    bookmaker     = db.relationship("Bookmaker", back_populates="market_failures")

    __table_args__ = (
        db.UniqueConstraint("bookmaker_id", "sport_slug", "market_name", name="uq_market_failure"),
        db.Index("ix_mf_count", "failure_count"),
    )

    def to_dict(self) -> dict:
        return {
            "bookmaker":     self.bookmaker.slug if self.bookmaker else None,
            "sport":         self.sport_slug,
            "market":        self.market_name,
            "failure_count": self.failure_count,
            "last_error":    self.last_error,
            "last_seen":     self.last_seen.isoformat() if self.last_seen else None,
        }


def record_market_failure(bk_slug: str, sport_slug: str, market_name: str, error: str = "") -> None:
    """
    Upsert a market failure record. Call from harvester exception handlers.
    Safe to call in background — swallows its own exceptions.
    """
    try:
        from app.extensions import db as _db
        bk = Bookmaker.query.filter_by(slug=bk_slug).first()
        if not bk:
            return
        existing = MarketFailure.query.filter_by(
            bookmaker_id=bk.id, sport_slug=sport_slug, market_name=market_name,
        ).first()
        if existing:
            existing.failure_count += 1
            existing.last_error     = error[:500] if error else existing.last_error
            existing.last_seen      = _now()
        else:
            _db.session.add(MarketFailure(
                bookmaker_id=bk.id, sport_slug=sport_slug,
                market_name=market_name, last_error=error[:500] if error else "",
            ))
        _db.session.commit()
    except Exception:
        pass


# =============================================================================
# HARVEST JOB LOG
# =============================================================================

class HarvestJob(db.Model):
    """
    One row per completed harvest task.
    Used by the monitoring dashboard to show last-run status per BK/sport.
    """
    __tablename__ = "harvest_jobs"

    id              = db.Column(db.Integer, primary_key=True)
    bookmaker_id    = db.Column(db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"))
    sport_slug      = db.Column(db.String(32), nullable=False, index=True)
    mode            = db.Column(db.String(16), default="upcoming")

    started_at      = db.Column(db.DateTime(timezone=True), default=_now)
    finished_at     = db.Column(db.DateTime(timezone=True))
    latency_ms      = db.Column(db.Integer)

    match_count     = db.Column(db.Integer, default=0)
    market_count    = db.Column(db.Integer, default=0)
    failure_count   = db.Column(db.Integer, default=0)

    status          = db.Column(db.String(16), default="ok")   # ok | error | partial
    error_message   = db.Column(db.Text)

    bookmaker       = db.relationship("Bookmaker", back_populates="harvest_jobs")

    __table_args__ = (
        db.Index("ix_hj_bk_sport", "bookmaker_id", "sport_slug"),
        db.Index("ix_hj_started",  "started_at"),
    )

    def to_dict(self) -> dict:
        return {
            "bookmaker":    self.bookmaker.slug if self.bookmaker else None,
            "sport":        self.sport_slug,
            "mode":         self.mode,
            "started_at":   self.started_at.isoformat()  if self.started_at  else None,
            "finished_at":  self.finished_at.isoformat() if self.finished_at else None,
            "latency_ms":   self.latency_ms,
            "match_count":  self.match_count,
            "market_count": self.market_count,
            "failure_count": self.failure_count,
            "status":       self.status,
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
