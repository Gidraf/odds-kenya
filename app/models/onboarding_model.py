"""
app/models/onboarding_model.py
==============================
BookmakerOnboardingSession — multi-phase API onboarding wizard.

Phases
──────
  LIST          Upcoming match list endpoint
  MARKETS       Per-match markets (URL uses {{match_id}} etc.)
  LIVE_LIST     Live match list endpoint
  LIVE_MARKETS  Per-live-match markets
  COMPLETE      All saved to HarvestWorkflow + BookmakerEndpoint

On completion the wizard writes:
  • HarvestWorkflow      (1 upcoming + 1 live)
  • HarvestWorkflowStep  (up to 4 steps)
  • BookmakerEndpoint    (4 intercept records)
  • BookmakerEntityValues (sport ID ↔ canonical Sport)

Sport ID handling
─────────────────
Each bookmaker uses its own numeric/string IDs for sports.
The wizard detects sport_id values in the list response, lets the
user map them to canonical Sport rows, then persists to
BookmakerEntityValues (entity_type='sport').

URL placeholder detection
─────────────────────────
When the user pastes a markets curl like:
  curl https://api.bookie.com/v2/events/12345/markets
…and the list response had match_id=12345, the wizard auto-suggests:
  https://api.bookie.com/v2/events/{{match_id}}/markets
"""

from datetime import datetime, timezone
from app.extensions import db


ONBOARDING_PHASES = ["LIST", "MARKETS", "LIVE_LIST", "LIVE_MARKETS", "COMPLETE"]


class BookmakerOnboardingSession(db.Model):
    __tablename__ = "bookmaker_onboarding_sessions"

    id           = db.Column(db.Integer, primary_key=True)
    bookmaker_id = db.Column(
        db.Integer,
        db.ForeignKey("bookmakers.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )

    # ── Wizard progress ───────────────────────────────────────────────────────
    current_phase   = db.Column(db.String(20), default="LIST", nullable=False)
    list_ok         = db.Column(db.Boolean, default=False, nullable=False)
    markets_ok      = db.Column(db.Boolean, default=False, nullable=False)
    live_list_ok    = db.Column(db.Boolean, default=False, nullable=False)
    live_markets_ok = db.Column(db.Boolean, default=False, nullable=False)

    # ── Phase 1: Upcoming match LIST ──────────────────────────────────────────
    list_url          = db.Column(db.String(600), nullable=True)
    list_method       = db.Column(db.String(10),  default="GET")
    list_headers      = db.Column(db.JSON, nullable=True)   # full header dict
    list_params       = db.Column(db.JSON, nullable=True)   # query param dict
    list_body         = db.Column(db.Text, nullable=True)
    list_array_path   = db.Column(db.String(300), nullable=True)
    # field_map: {role → dot_path}  e.g. {"match_id": "id", "home_team": "teams.home.name"}
    list_field_map    = db.Column(db.JSON, nullable=True)
    # First N items from list probe, stored as JSON string
    list_sample       = db.Column(db.Text, nullable=True)
    # Dot-path to the sport_id field inside each list item
    list_sport_id_path = db.Column(db.String(300), nullable=True)

    # ── Sport ID variants ─────────────────────────────────────────────────────
    # JSON array of:
    # {
    #   bk_sport_id: "1",           bookmaker's own sport ID value
    #   sport_id: 3,                canonical Sport.id (null until mapped)
    #   sport_name: "Football",     display name
    #   param_key: "sport_id",      which URL param carries this (null = path)
    #   param_in: "query|path",     where the param lives
    #   status: "pending|ok|error", last probe result
    #   item_count: 50              items returned for this sport
    # }
    sport_mappings = db.Column(db.JSON, nullable=True)

    # ── Phase 2: Per-match MARKETS ────────────────────────────────────────────
    # May contain {{match_id}}, {{sport_id}} etc.
    markets_url_template   = db.Column(db.String(600), nullable=True)
    markets_method         = db.Column(db.String(10),  default="GET")
    markets_headers        = db.Column(db.JSON, nullable=True)
    markets_params         = db.Column(db.JSON, nullable=True)
    markets_body_template  = db.Column(db.Text, nullable=True)
    markets_array_path     = db.Column(db.String(300), nullable=True)
    markets_field_map      = db.Column(db.JSON, nullable=True)
    markets_sample         = db.Column(db.Text, nullable=True)
    # {var_name → list_field_path}  e.g. {"match_id": "id"}
    markets_placeholder_map = db.Column(db.JSON, nullable=True)

    # ── Phase 3: LIVE match LIST ──────────────────────────────────────────────
    live_list_url          = db.Column(db.String(600), nullable=True)
    live_list_method       = db.Column(db.String(10),  default="GET")
    live_list_headers      = db.Column(db.JSON, nullable=True)
    live_list_params       = db.Column(db.JSON, nullable=True)
    live_list_body         = db.Column(db.Text, nullable=True)
    live_list_array_path   = db.Column(db.String(300), nullable=True)
    live_list_field_map    = db.Column(db.JSON, nullable=True)
    live_list_sample       = db.Column(db.Text, nullable=True)

    # ── Phase 4: Per-live-match MARKETS ──────────────────────────────────────
    live_markets_url_template    = db.Column(db.String(600), nullable=True)
    live_markets_method          = db.Column(db.String(10),  default="GET")
    live_markets_headers         = db.Column(db.JSON, nullable=True)
    live_markets_params          = db.Column(db.JSON, nullable=True)
    live_markets_body_template   = db.Column(db.Text, nullable=True)
    live_markets_array_path      = db.Column(db.String(300), nullable=True)
    live_markets_field_map       = db.Column(db.JSON, nullable=True)
    live_markets_sample          = db.Column(db.Text, nullable=True)
    live_markets_placeholder_map = db.Column(db.JSON, nullable=True)

    # ── Completion metadata ───────────────────────────────────────────────────
    # {"upcoming": workflow_id, "live": workflow_id}
    workflow_ids    = db.Column(db.JSON, nullable=True)
    # [endpoint_id, ...]
    endpoint_ids    = db.Column(db.JSON, nullable=True)
    is_complete     = db.Column(db.Boolean, default=False, nullable=False)

    created_at = db.Column(
        db.DateTime, nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = db.Column(
        db.DateTime, nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    bookmaker = db.relationship("Bookmaker", lazy="joined")

    def __repr__(self) -> str:
        return (
            f"<OnboardingSession #{self.id} "
            f"bk={self.bookmaker_id} phase={self.current_phase}>"
        )

    @property
    def phases_complete(self) -> list[str]:
        out = []
        if self.list_ok:         out.append("LIST")
        if self.markets_ok:      out.append("MARKETS")
        if self.live_list_ok:    out.append("LIVE_LIST")
        if self.live_markets_ok: out.append("LIVE_MARKETS")
        return out

    @property
    def ready_to_save(self) -> bool:
        """At minimum, list must be configured."""
        return self.list_ok

    def to_dict(self) -> dict:
        return {
            "id":             self.id,
            "bookmaker_id":   self.bookmaker_id,
            "bookmaker_name": self.bookmaker.name if self.bookmaker else None,
            "current_phase":  self.current_phase,
            # Phase status
            "list_ok":         self.list_ok,
            "markets_ok":      self.markets_ok,
            "live_list_ok":    self.live_list_ok,
            "live_markets_ok": self.live_markets_ok,
            "phases_complete": self.phases_complete,
            "ready_to_save":   self.ready_to_save,
            "is_complete":     self.is_complete,
            # List
            "list_url":           self.list_url,
            "list_method":        self.list_method,
            "list_headers":       self.list_headers or {},
            "list_params":        self.list_params  or {},
            "list_array_path":    self.list_array_path,
            "list_field_map":     self.list_field_map or {},
            "list_sport_id_path": self.list_sport_id_path,
            "sport_mappings":     self.sport_mappings or [],
            # Markets
            "markets_url_template":    self.markets_url_template,
            "markets_method":          self.markets_method,
            "markets_headers":         self.markets_headers  or {},
            "markets_params":          self.markets_params   or {},
            "markets_array_path":      self.markets_array_path,
            "markets_field_map":       self.markets_field_map or {},
            "markets_placeholder_map": self.markets_placeholder_map or {},
            # Live list
            "live_list_url":        self.live_list_url,
            "live_list_method":     self.live_list_method,
            "live_list_headers":    self.live_list_headers or {},
            "live_list_params":     self.live_list_params  or {},
            "live_list_array_path": self.live_list_array_path,
            "live_list_field_map":  self.live_list_field_map or {},
            # Live markets
            "live_markets_url_template":    self.live_markets_url_template,
            "live_markets_method":          self.live_markets_method,
            "live_markets_headers":         self.live_markets_headers  or {},
            "live_markets_params":          self.live_markets_params   or {},
            "live_markets_array_path":      self.live_markets_array_path,
            "live_markets_field_map":       self.live_markets_field_map or {},
            "live_markets_placeholder_map": self.live_markets_placeholder_map or {},
            # Results
            "workflow_ids": self.workflow_ids,
            "endpoint_ids": self.endpoint_ids,
            "created_at":   self.created_at.isoformat() if self.created_at else None,
            "updated_at":   self.updated_at.isoformat() if self.updated_at else None,
        }