"""
Odds Models  v2
===============
Table layout
────────────
  unified_matches         — One row per real-world match, keyed by parent_match_id
                            (betradar / cross-bookmaker canonical ID).
                            markets_json stores ALL bookmakers' prices grouped by
                            market → specifier → selection, each selection carrying
                            a per-bookmaker prices dict so you can compare in one read.

  bookmaker_match_odds    — One row per (match × bookmaker).
                            markets_json stores only that bookmaker's prices (same
                            shape, no cross-bookmaker data).  Only the price value
                            of an existing selection is ever changed; everything else
                            (market name, specifier, selection name) stays fixed once
                            created.  If the bookmaker starts offering a new market
                            it is added; if it stops, is_active is set False.

  bookmaker_odds_history  — Append-only log of every price change per
                            (bookmaker × match × market × specifier × selection).
                            Also stores the current price so you can reconstruct the
                            full timeline from history alone.

  market_definitions      — Lookup of known market names for display / EV grouping.
                            Created on-the-fly the first time a name is seen.

markets_json shape on UnifiedMatch
───────────────────────────────────
{
  "1X2": {                              <- market name
    "null": {                           <- specifier ("null" when N/A)
      "Home": {                         <- selection name
        "best_price":        1.95,      <- highest price across all bookmakers
        "best_bookmaker_id": 3,         <- who offers the best price
        "is_active":         true,
        "updated_at":        "2025-01-15T15:30:00+00:00",
        "bookmakers": {                 <- every bookmaker's current price
          "3": 1.95,
          "7": 1.90
        }
      },
      "Draw": { ... },
      "Away": { ... }
    }
  },
  "Over/Under": {
    "2.5": {
      "Over":  { ... },
      "Under": { ... }
    },
    "3.5": { ... }
  }
}

markets_json shape on BookmakerMatchOdds  (same hierarchy, no cross-BK data)
──────────────────────────────────────────
{
  "1X2": {
    "null": {
      "Home": { "price": 1.95, "is_active": true, "updated_at": "..." },
      "Draw": { "price": 3.40, "is_active": true, "updated_at": "..." },
      "Away": { "price": 4.20, "is_active": true, "updated_at": "..." }
    }
  }
}

Parser row contract  (REQUIRED_PARSER_KEYS)
────────────────────────────────────────────
Every row returned by parse_data() MUST include all required keys.
validate_parser_row() raises ValueError with the exact missing keys.

Migrations:
  flask db migrate -m "odds_model_v2"
  flask db upgrade

Register in app/models/__init__.py:
  from app.models.odds_model import (
      UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
      MarketDefinition, validate_parser_row, REQUIRED_PARSER_KEYS,
  )
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.extensions import db


# ─────────────────────────────────────────────────────────────────────────────
# PARSER ROW CONTRACT
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_PARSER_KEYS = frozenset({
    "parent_match_id",   # betradar / cross-bookmaker canonical ID  (str or int)
    "home_team",         # str  — home team name
    "away_team",         # str  — away team name
    "start_time",        # ISO-8601 string or None
    "sport",             # str or None
    "competition",       # str or None
    "market",            # str  — e.g. "1X2", "Over/Under", "BTTS"
    "selection",         # str  — e.g. "Home", "Over", "Yes"
    "price",             # float > 1.0  — decimal odds
    "specifier",         # str or None  — e.g. "2.5", "-1", None for 1X2
})

# These keys may be None without triggering a non-nullable validation error
NULLABLE_KEYS = frozenset({"specifier", "start_time", "sport", "competition"})


def validate_parser_row(row: dict) -> None:
    """
    Validate a single parser output row.

    Raises ValueError with a precise message describing exactly which keys
    are missing or have invalid values.  Called before any DB write.

    Example errors
    ──────────────
    ValueError: Parser row missing required keys: ['market', 'parent_match_id'].
                Row keys received: ['home_team', 'away_team', 'price', ...]
                All required keys: ['away_team', 'competition', ...]

    ValueError: Parser row has None for required non-nullable key 'selection'.

    ValueError: Parser row price='abc' is not a valid float.

    ValueError: Parser row price=0.85 is <= 1.0 (invalid decimal odds).
    """
    if not isinstance(row, dict):
        raise ValueError(
            f"Parser must return a list of dicts, got a row of type {type(row).__name__!r}."
        )

    missing = REQUIRED_PARSER_KEYS - row.keys()
    if missing:
        raise ValueError(
            f"Parser row missing required keys: {sorted(missing)}.\n"
            f"Row keys received:  {sorted(row.keys())}\n"
            f"All required keys:  {sorted(REQUIRED_PARSER_KEYS)}"
        )

    for key in REQUIRED_PARSER_KEYS - NULLABLE_KEYS:
        if row.get(key) is None:
            raise ValueError(
                f"Parser row has None for required non-nullable key {key!r}.\n"
                f"Row: {row}"
            )

    raw_price = row.get("price")
    try:
        p = float(raw_price)
    except (TypeError, ValueError):
        raise ValueError(
            f"Parser row price={raw_price!r} is not a valid float.\n"
            f"Row: {row}"
        )
    if p <= 1.0:
        raise ValueError(
            f"Parser row price={p} is <= 1.0 (invalid decimal odds).\n"
            f"Row: {row}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# MARKET DEFINITION
# ─────────────────────────────────────────────────────────────────────────────

class MarketDefinition(db.Model):
    """
    Canonical registry of known market names for display, EV grouping, and
    filtering player props from standard markets.
    Created on-the-fly when a new market name is encountered.
    """
    __tablename__ = "market_definitions"

    id             = db.Column(db.Integer, primary_key=True)
    name           = db.Column(db.String(120), unique=True, nullable=False, index=True)
    display_name   = db.Column(db.String(120), nullable=True)
    is_player_prop = db.Column(db.Boolean, default=False, nullable=False)
    created_at     = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    @classmethod
    def get_or_create(cls, name: str) -> "MarketDefinition":
        obj = cls.query.filter_by(name=name).first()
        if not obj:
            obj = cls(name=name, display_name=name)
            db.session.add(obj)
            db.session.flush()
        return obj

    def to_dict(self) -> dict:
        return {
            "id":             self.id,
            "name":           self.name,
            "display_name":   self.display_name or self.name,
            "is_player_prop": self.is_player_prop,
        }


# ─────────────────────────────────────────────────────────────────────────────
# UNIFIED MATCH
# ─────────────────────────────────────────────────────────────────────────────

class UnifiedMatch(db.Model):
    """
    Canonical match record — one row per real-world fixture, shared across all
    bookmakers.  Keyed by parent_match_id (betradar / any cross-bookmaker ID).

    markets_json aggregates ALL bookmakers' current prices into one JSON column.

    Update rules for markets_json
    ──────────────────────────────
    • If a (market, specifier, selection) tuple is new      → add the full entry.
    • If it exists for a new bookmaker                      → add to bookmakers{}.
    • If it exists and this bookmaker's price changed       → update bookmakers[id]
                                                              and recompute best_*.
    • Market name, specifier key, selection name are immutable once written.
    """
    __tablename__ = "unified_matches"

    id = db.Column(db.Integer, primary_key=True)
    parent_match_id = db.Column(
        db.String(64), unique=True, nullable=False, index=True,
        comment="Betradar / cross-bookmaker canonical match ID",
    )

    # FK references — nullable so partial feeds can be stored and enriched later
    competition_id = db.Column(db.Integer, db.ForeignKey("competitions.id"), nullable=True)
    home_team_id   = db.Column(db.Integer, db.ForeignKey("teams.id"),        nullable=True)
    away_team_id   = db.Column(db.Integer, db.ForeignKey("teams.id"),        nullable=True)

    # Denormalised strings — always populated, avoids JOINs for display
    home_team_name   = db.Column(db.String(120), nullable=True)
    away_team_name   = db.Column(db.String(120), nullable=True)
    sport_name       = db.Column(db.String(80),  nullable=True)
    competition_name = db.Column(db.String(120), nullable=True)

    start_time = db.Column(db.DateTime, nullable=True, index=True)
    status     = db.Column(db.String(20), default="PRE_MATCH", nullable=False, index=True)

    # Aggregated odds — see module docstring for full schema
    markets_json = db.Column(
        db.JSON, nullable=False, default=dict,
        comment=(
            "market → specifier → selection → "
            "{best_price, best_bookmaker_id, bookmakers:{id:price}, is_active, updated_at}"
        ),
    )

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(
        db.DateTime,
        default=lambda:  datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    bookmaker_odds = db.relationship(
        "BookmakerMatchOdds",
        back_populates="match",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        db.Index("ix_um_start_status", "start_time", "status"),
    )

    # ── Internal ─────────────────────────────────────────────────────────────

    @staticmethod
    def _spec_key(specifier: Any) -> str:
        """None → "null" so specifier is always a valid JSON object key."""
        return str(specifier) if specifier is not None else "null"

    # ── Public API ───────────────────────────────────────────────────────────

    def upsert_bookmaker_price(
        self,
        market:       str,
        specifier:    Any,
        selection:    str,
        price:        float,
        bookmaker_id: int,
    ) -> None:
        """
        Record bookmaker_id's current price for (market, specifier, selection).

        Only the price value for this bookmaker and the derived best_* fields
        are changed on an existing entry.  Everything else is immutable.
        """
        markets  = dict(self.markets_json or {})
        spec_key = self._spec_key(specifier)
        bk_key   = str(bookmaker_id)
        now_str  = datetime.now(timezone.utc).isoformat()

        markets.setdefault(market, {})
        markets[market].setdefault(spec_key, {})
        sel_map = markets[market][spec_key]

        if selection not in sel_map:
            sel_map[selection] = {
                "best_price":        price,
                "best_bookmaker_id": bookmaker_id,
                "is_active":         True,
                "updated_at":        now_str,
                "bookmakers":        {bk_key: price},
            }
        else:
            entry = sel_map[selection]
            entry["bookmakers"][bk_key] = price
            entry["updated_at"]         = now_str
            entry["is_active"]          = True

            # Recompute best across all known bookmakers for this selection
            best_bk, best_p = max(entry["bookmakers"].items(), key=lambda kv: kv[1])
            entry["best_price"]        = best_p
            entry["best_bookmaker_id"] = int(best_bk)

        self.markets_json = markets

    def get_selection(self, market: str, specifier: Any, selection: str) -> dict | None:
        spec_key = self._spec_key(specifier)
        return (
            (self.markets_json or {})
            .get(market, {})
            .get(spec_key, {})
            .get(selection)
        )

    def all_market_names(self) -> list[str]:
        return list((self.markets_json or {}).keys())

    def to_dict(self, include_bookmaker_odds: bool = False) -> dict:
        d: dict = {
            "id":              self.id,
            "parent_match_id": self.parent_match_id,
            "home_team":       self.home_team_name,
            "away_team":       self.away_team_name,
            "sport":           self.sport_name,
            "competition":     self.competition_name,
            "start_time":      self.start_time.isoformat() if self.start_time else None,
            "status":          self.status,
            "markets":         self.markets_json or {},
            "updated_at":      self.updated_at.isoformat() if self.updated_at else None,
        }
        if include_bookmaker_odds:
            d["bookmaker_odds"] = [b.to_dict() for b in self.bookmaker_odds]
        return d


# ─────────────────────────────────────────────────────────────────────────────
# BOOKMAKER MATCH ODDS
# ─────────────────────────────────────────────────────────────────────────────

class BookmakerMatchOdds(db.Model):
    """
    Per-bookmaker odds for one match.  One row per (match_id, bookmaker_id).

    markets_json holds only this bookmaker's prices (no cross-bookmaker data).
    Shape: market → specifier → selection → {price, is_active, updated_at}

    Update rule: only price, is_active, and updated_at are ever changed on an
    existing entry.  Market name, specifier key, and selection name are immutable.
    """
    __tablename__ = "bookmaker_match_odds"

    id = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(
        db.Integer,
        db.ForeignKey("unified_matches.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    bookmaker_id = db.Column(
        db.Integer,
        db.ForeignKey("bookmakers.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )

    markets_json = db.Column(
        db.JSON, nullable=False, default=dict,
        comment="This bookmaker's prices: market→specifier→selection→{price,is_active,updated_at}",
    )

    # Last raw parser rows kept for debugging / re-testing without re-fetching
    raw_snapshot = db.Column(
        db.JSON, nullable=True,
        comment="Raw list of last parser output rows for this bookmaker+match",
    )

    is_active  = db.Column(db.Boolean, default=True,  nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(
        db.DateTime,
        default=lambda:  datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        db.UniqueConstraint("match_id", "bookmaker_id", name="uq_bmo_match_bookmaker"),
        db.Index("ix_bmo_bookmaker_active", "bookmaker_id", "is_active"),
    )

    match   = db.relationship("UnifiedMatch",      back_populates="bookmaker_odds")
    history = db.relationship(
        "BookmakerOddsHistory",
        back_populates="bmo",
        lazy="dynamic",
        cascade="all, delete-orphan",
    )

    # ── Internal ─────────────────────────────────────────────────────────────

    @staticmethod
    def _spec_key(specifier: Any) -> str:
        return str(specifier) if specifier is not None else "null"

    # ── Public API ───────────────────────────────────────────────────────────

    def upsert_selection(
        self,
        market:    str,
        specifier: Any,
        selection: str,
        price:     float,
    ) -> tuple[bool, float | None]:
        """
        Insert or update one selection.

        Returns
        ───────
        (price_changed, old_price)
          price_changed=True, old_price=None   → brand-new selection
          price_changed=True, old_price=float  → price moved, record history
          price_changed=False, old_price=float → price identical, skip history
        """
        markets  = dict(self.markets_json or {})
        spec_key = self._spec_key(specifier)
        now_str  = datetime.now(timezone.utc).isoformat()

        markets.setdefault(market, {})
        markets[market].setdefault(spec_key, {})
        sel_map = markets[market][spec_key]

        if selection not in sel_map:
            sel_map[selection] = {
                "price":      price,
                "is_active":  True,
                "updated_at": now_str,
            }
            self.markets_json = markets
            self.updated_at   = datetime.now(timezone.utc)
            return True, None

        entry     = sel_map[selection]
        old_price = entry.get("price")

        # Always refresh updated_at so we know we saw this selection this cycle
        entry["is_active"]  = True
        entry["updated_at"] = now_str

        if old_price == price:
            self.markets_json = markets
            return False, old_price

        entry["price"]    = price
        self.markets_json = markets
        self.updated_at   = datetime.now(timezone.utc)
        return True, old_price

    def get_price(self, market: str, specifier: Any, selection: str) -> float | None:
        spec_key = self._spec_key(specifier)
        entry = (
            (self.markets_json or {})
            .get(market, {})
            .get(spec_key, {})
            .get(selection)
        )
        return entry.get("price") if entry else None

    def to_dict(self) -> dict:
        return {
            "id":           self.id,
            "match_id":     self.match_id,
            "bookmaker_id": self.bookmaker_id,
            "markets":      self.markets_json or {},
            "is_active":    self.is_active,
            "updated_at":   self.updated_at.isoformat() if self.updated_at else None,
        }


# ─────────────────────────────────────────────────────────────────────────────
# BOOKMAKER ODDS HISTORY
# ─────────────────────────────────────────────────────────────────────────────

class BookmakerOddsHistory(db.Model):
    """
    Append-only price change log per (bookmaker × match × market × selection).

    A row is appended:
      • When a selection is first seen         (old_price=None)
      • Every time the price changes           (old_price = previous value)

    new_price is always the current price so the complete price timeline can be
    reconstructed from this table alone, ordered by recorded_at.

    Query patterns covered by the three composite indexes:
      1. Line movement for one selection on one match:
           WHERE bmo_id=X AND market='1X2' AND selection='Home'
           ORDER BY recorded_at
      2. All price movements for a market across all matches today:
           WHERE match_id=X AND market='Over/Under' AND recorded_at > T
      3. All price changes from one bookmaker in the last N minutes:
           WHERE bookmaker_id=X AND recorded_at > T
    """
    __tablename__ = "bookmaker_odds_history"

    id = db.Column(db.Integer, primary_key=True)

    bmo_id = db.Column(
        db.Integer,
        db.ForeignKey("bookmaker_match_odds.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )

    # Denormalised FK columns — faster queries without extra JOINs
    bookmaker_id = db.Column(
        db.Integer, db.ForeignKey("bookmakers.id"),
        nullable=False, index=True,
    )
    match_id = db.Column(
        db.Integer, db.ForeignKey("unified_matches.id"),
        nullable=False, index=True,
    )

    market    = db.Column(db.String(120), nullable=False)
    specifier = db.Column(db.String(50),  nullable=True)
    selection = db.Column(db.String(100), nullable=False)

    old_price   = db.Column(db.Float, nullable=True,
                            comment="NULL on first-seen; previous price on change")
    new_price   = db.Column(db.Float, nullable=False,
                            comment="Current price at recorded_at")
    price_delta = db.Column(db.Float, nullable=True,
                            comment="new_price - old_price; positive = odds drifted out")

    recorded_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False, index=True,
    )

    __table_args__ = (
        db.Index("ix_boh_bmo_market_sel",  "bmo_id",       "market", "selection"),
        db.Index("ix_boh_match_market",    "match_id",     "market", "recorded_at"),
        db.Index("ix_boh_bookmaker_time",  "bookmaker_id", "recorded_at"),
    )

    bmo = db.relationship("BookmakerMatchOdds", back_populates="history")

    def to_dict(self) -> dict:
        return {
            "id":           self.id,
            "bookmaker_id": self.bookmaker_id,
            "match_id":     self.match_id,
            "market":       self.market,
            "specifier":    self.specifier,
            "selection":    self.selection,
            "old_price":    self.old_price,
            "new_price":    self.new_price,
            "price_delta":  self.price_delta,
            "recorded_at":  self.recorded_at.isoformat(),
        }