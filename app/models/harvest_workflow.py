"""
app/models/harvest_workflow_model.py
=====================================
Stores the step-based harvest workflow for each bookmaker.

A HarvestWorkflow contains 1-N ordered HarvestWorkflowSteps.
Each step carries its own optional parse_data() function that converts
that step's response items into rows conforming to REQUIRED_PARSER_KEYS.

Step types
──────────
  FETCH_LIST      — one HTTP call, returns an array of matches (top-level)
  FETCH_PER_ITEM  — one HTTP call PER item from a previous step
                    URL contains {{role_name}} template vars
  FETCH_ONCE      — single call, no looping (e.g. static data)

Field roles (stored in fields_json as a list of {path, role, label} dicts)
───────────
  match_id, parent_match_id, home_team, away_team, start_time, sport,
  competition, market_name, specifier, selection_name, selection_price,
  home_lineup, away_lineup, home_form, away_form, match_status,
  score_home, score_away, lineup_confirmed, kickoff_in_mins, custom

Per-step parser contract
────────────────────────
  def parse_data(raw_data: dict | list) -> list[dict]:
      \"\"\"
      raw_data  — the raw response items for this step (list or dict)
      Returns   — list of rows, each satisfying REQUIRED_PARSER_KEYS:
                  parent_match_id, home_team, away_team, start_time,
                  sport, competition, market, selection, price (float > 1.0),
                  specifier (str | None)
      \"\"\"

Alembic migration hint
──────────────────────
  flask db migrate -m "move parser fields to workflow step"
  flask db upgrade
"""

from datetime import datetime, timezone
from app.extensions import db


class HarvestWorkflow(db.Model):
    __tablename__ = "harvest_workflow"

    id           = db.Column(db.Integer, primary_key=True)
    bookmaker_id = db.Column(
        db.Integer,
        db.ForeignKey("bookmakers.id", ondelete="CASCADE"),
        index=True,
    )
    name        = db.Column(db.String(), nullable=False)
    description = db.Column(db.Text, nullable=True)
    is_active   = db.Column(
        db.Boolean, nullable=False, default=True, server_default="1"
    )

    created_at = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    bookmaker = db.relationship("Bookmaker")
    sport_id = db.Column(
        db.Integer,
        db.ForeignKey("sports.id", ondelete="CASCADE"),
        index=True,
    )
    sport = db.relationship("Sport")
    steps = db.relationship(
        "HarvestWorkflowStep",
        backref="workflow",
        cascade="all, delete-orphan",
        lazy="joined",
        order_by="HarvestWorkflowStep.position",
    )

    def __repr__(self) -> str:
        return f"<HarvestWorkflow #{self.id} bk={self.bookmaker_id} '{self.name}'>"


class HarvestWorkflowStep(db.Model):
    __tablename__ = "harvest_workflow_step"

    id          = db.Column(db.Integer, primary_key=True)
    workflow_id = db.Column(
        db.Integer,
        db.ForeignKey("harvest_workflow.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # ── Ordering & identity ──────────────────────────────────────────────────
    position  = db.Column(
        db.Integer, nullable=False, default=1,
        comment="1-based position in the workflow",
    )
    name      = db.Column(db.String(), nullable=False, default="")
    step_type = db.Column(
        db.String(30), nullable=False, default="FETCH_LIST",
        comment="FETCH_LIST | FETCH_PER_ITEM | FETCH_ONCE",
    )

    # ── HTTP config ──────────────────────────────────────────────────────────
    url_template  = db.Column(
        db.String(), nullable=False, default="",
        comment="May contain {{role_name}} placeholders for FETCH_PER_ITEM",
    )
    method        = db.Column(db.String(), nullable=False, default="GET")
    headers_json  = db.Column(
        db.Text, nullable=False, default="{}", comment="JSON object"
    )
    params_json   = db.Column(
        db.Text, nullable=False, default="{}", comment="JSON object — query params"
    )
    body_template = db.Column(
        db.Text, nullable=True, comment="POST body; may contain {{}} vars"
    )

    # ── Result navigation ────────────────────────────────────────────────────
    result_array_path = db.Column(
        db.String(), nullable=True,
        comment=(
            "Dot-path to the array in the response (e.g. 'data' or 'events'). "
            "Null = root is the array."
        ),
    )

    # ── Field mappings ───────────────────────────────────────────────────────
    fields_json = db.Column(
        db.Text, nullable=False, default="[]",
        comment="""JSON array of field descriptors:
[
  {
    "path":  "teams.home.name",   // dot-path into each array item
    "role":  "home_team",         // canonical role (see FIELD_ROLES)
    "label": "Home Team",         // display label
    "store": true                 // whether to persist to DB (default true)
  }
]""",
    )

    # ── Step dependencies ────────────────────────────────────────────────────
    depends_on_pos = db.Column(
        db.Integer, nullable=True,
        comment=(
            "Position of the step whose items this step iterates over. "
            "Null = top-level (no dependency)."
        ),
    )

    # ── Per-step parser ──────────────────────────────────────────────────────
    parser_code = db.Column(
        db.Text, nullable=True,
        comment=(
            "Python source for parse_data(raw_data) -> list[dict].\n"
            "raw_data is the raw response for this specific step.\n"
            "Each returned dict must satisfy REQUIRED_PARSER_KEYS."
        ),
    )
    parser_test_passed = db.Column(
        db.Boolean, nullable=False, default=False, server_default="0",
        comment=(
            "True when this step's parser last passed validate_parser_row "
            "on every returned row."
        ),
    )

    # ── Meta ─────────────────────────────────────────────────────────────────
    enabled = db.Column(
        db.Boolean, nullable=False, default=True, server_default="1"
    )
    notes = db.Column(db.Text, nullable=True)

    # ── Unique constraint: one position per workflow ─────────────────────────
    __table_args__ = (
        db.UniqueConstraint(
            "workflow_id", "position", name="uq_workflow_step_position"
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<HarvestWorkflowStep wf={self.workflow_id} "
            f"pos={self.position} '{self.name}'>"
        )