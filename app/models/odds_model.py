"""
Odds Models  v3
===============
Changes from v2
───────────────
• ArbitrageOpportunity  — detected arb windows: when found, when closed, profit %,
                          bookmaker breakdown, peak edge, how to exploit
• EVOpportunity         — positive expected value windows vs consensus/closing line
• UnifiedMatch          — version column (optimistic locking), result fields,
                          status machine (PRE_MATCH → LIVE → FINISHED → CANCELLED),
                          indexed for date-range + sport + competition queries
• BookmakerMatchOdds    — row_version for concurrent update safety
• BookmakerOddsHistory  — no change to schema; batch-insert helper added
• All write helpers use SELECT … FOR UPDATE so concurrent Celery workers /
  WebSocket pollers never produce phantom duplicates or lost updates

Table layout
────────────
  unified_matches          canonical match, all bookmakers aggregated in markets_json
  bookmaker_match_odds     per-bookmaker odds, single-bookmaker markets_json
  bookmaker_odds_history   append-only price change log
  market_definitions       known market name registry
  arbitrage_opportunities  arb windows with full leg breakdown
  ev_opportunities         positive-EV windows vs consensus / closing line

Concurrency model
─────────────────
  All row mutations use a helper pattern:
      row = Model.query.with_for_update().filter_by(id=X).first()
      row.field = new_value
      db.session.commit()

  UnifiedMatch also carries a `version` integer (SQLAlchemy optimistic locking).
  If two workers load the same row and both try to commit, the second raises
  StaleDataError which the caller must catch and retry.

  BookmakerMatchOdds uses a `row_version` integer incremented on each write,
  allowing clients to detect missed updates.

Arbitrage detection formula
────────────────────────────
  For a two-outcome market (e.g. Match Winner no draw):
      arb_pct = (1/odd_A + 1/odd_B)
      profit_pct = (1 / arb_pct - 1) * 100   # positive = profitable

  For three-outcome (1X2):
      arb_pct = 1/odd_1 + 1/odd_X + 1/odd_2
      profit_pct as above

  Stored as decimal, e.g. 0.023 = 2.3% guaranteed profit.

EV formula
──────────
  true_prob_estimate = fair_odd / (sum of 1/p for all outcomes in market)
  ev_pct = (best_odd * true_prob_estimate - 1) * 100
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import event as sa_event
from sqlalchemy.orm import Session

from app.extensions import db


# ═════════════════════════════════════════════════════════════════════════════
# ENUMS
# ═════════════════════════════════════════════════════════════════════════════

class MatchStatus(str, enum.Enum):
    PRE_MATCH  = "PRE_MATCH"
    LIVE       = "LIVE"
    FINISHED   = "FINISHED"
    CANCELLED  = "CANCELLED"
    POSTPONED  = "POSTPONED"
    SUSPENDED  = "SUSPENDED"


class OpportunityStatus(str, enum.Enum):
    OPEN   = "OPEN"    # still available right now
    CLOSED = "CLOSED"  # price moved, no longer profitable
    EXPIRED = "EXPIRED" # match kicked off before anyone acted


# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _utcnow_naive() -> datetime:
    """Some DB drivers prefer naive UTC; use consistently."""
    return  datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# PARSER ROW CONTRACT
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_PARSER_KEYS = frozenset({
    "parent_match_id",
    "home_team",
    "away_team",
    "start_time",
    "sport",
    "competition",
    "market",
    "selection",
    "price",
    "specifier",
})

NULLABLE_KEYS = frozenset({"specifier", "start_time", "sport", "competition"})


def validate_parser_row(row: dict) -> None:
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


# ═════════════════════════════════════════════════════════════════════════════
# MARKET DEFINITION
# ═════════════════════════════════════════════════════════════════════════════

class MarketDefinition(db.Model):
    __tablename__ = "market_definitions"

    id             = db.Column(db.Integer, primary_key=True)
    name           = db.Column(db.String(120), unique=True, nullable=False, index=True)
    display_name   = db.Column(db.String(120), nullable=True)
    is_player_prop = db.Column(db.Boolean, default=False, nullable=False)
    created_at     = db.Column(db.DateTime, default=_utcnow_naive)

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


# ═════════════════════════════════════════════════════════════════════════════
# UNIFIED MATCH
# ═════════════════════════════════════════════════════════════════════════════

class UnifiedMatch(db.Model):
    """
    Canonical match record shared across all bookmakers.

    Transaction safety
    ──────────────────
    Use .lock() before mutating markets_json to prevent concurrent write races:

        match = UnifiedMatch.lock(parent_match_id="12345")
        match.upsert_bookmaker_price(...)
        db.session.commit()

    The `version` column provides optimistic locking via SQLAlchemy's
    mapper version_id_col — any concurrent commit on a stale row raises
    sqlalchemy.orm.exc.StaleDataError.
    """
    __tablename__ = "unified_matches"

    id = db.Column(db.Integer, primary_key=True)
    parent_match_id = db.Column(
        db.String(64), unique=True, nullable=False, index=True,
        comment="Betradar / cross-bookmaker canonical match ID",
    )

    # Optional FK links (enriched separately — not required for odds storage)
    competition_id = db.Column(db.Integer, db.ForeignKey("competitions.id"), nullable=True)
    home_team_id   = db.Column(db.Integer, db.ForeignKey("teams.id"),        nullable=True)
    away_team_id   = db.Column(db.Integer, db.ForeignKey("teams.id"),        nullable=True)

    # Denormalised strings — always written, avoids JOINs for list queries
    home_team_name   = db.Column(db.String(120), nullable=True)
    away_team_name   = db.Column(db.String(120), nullable=True)
    sport_name       = db.Column(db.String(80),  nullable=True, index=True)
    competition_name = db.Column(db.String(120), nullable=True, index=True)
    country_name     = db.Column(db.String(80),  nullable=True)

    start_time  = db.Column(db.DateTime, nullable=True, index=True)
    status      = db.Column(
        db.Enum(MatchStatus, native_enum=False), default=MatchStatus.PRE_MATCH,
        nullable=False, index=True,
    )

    # Match result (populated after FINISHED)
    score_home    = db.Column(db.SmallInteger, nullable=True)
    score_away    = db.Column(db.SmallInteger, nullable=True)
    ht_score_home = db.Column(db.SmallInteger, nullable=True)
    ht_score_away = db.Column(db.SmallInteger, nullable=True)
    result        = db.Column(db.String(4),  nullable=True,
                               comment="1 / X / 2 — result for 1X2 market settlement")
    finished_at   = db.Column(db.DateTime, nullable=True)

    # Aggregated odds from all bookmakers
    markets_json = db.Column(
        db.JSON, nullable=False, default=dict,
        comment=(
            "market → specifier → selection → "
            "{best_price, best_bookmaker_id, bookmakers:{id:price}, is_active, updated_at}"
        ),
    )

    # Optimistic locking — SQLAlchemy increments this on every UPDATE
    version    = db.Column(db.Integer, nullable=True, default=0, server_default="0")

    created_at = db.Column(db.DateTime, default=_utcnow_naive)
    updated_at = db.Column(db.DateTime, default=_utcnow_naive, onupdate=_utcnow_naive)

    __mapper_args__ = {
        "version_id_col": version,
    }

    __table_args__ = (
        db.Index("ix_um_start_status",    "start_time",   "status"),
        db.Index("ix_um_sport_start",     "sport_name",   "start_time"),
        db.Index("ix_um_comp_start",      "competition_name", "start_time"),
        db.Index("ix_um_status_sport",    "status",       "sport_name"),
    )

    bookmaker_odds     = db.relationship(
        "BookmakerMatchOdds", back_populates="match",
        lazy="dynamic", cascade="all, delete-orphan",
    )
    arbitrage_opps     = db.relationship(
        "ArbitrageOpportunity", back_populates="match",
        lazy="dynamic", cascade="all, delete-orphan",
    )
    ev_opps            = db.relationship(
        "EVOpportunity", back_populates="match",
        lazy="dynamic", cascade="all, delete-orphan",
    )

    # ── Class-level helpers ──────────────────────────────────────────────────

    @classmethod
    def lock(cls, parent_match_id: str) -> "UnifiedMatch | None":
        """
        Fetch row with a row-level write lock (SELECT … FOR UPDATE).
        Use before any mutation to prevent concurrent write races.
        Returns None if not found.
        """
        return (
            cls.query
            .filter_by(parent_match_id=parent_match_id)
            .with_for_update()
            .first()
        )

    @classmethod
    def get_or_create_locked(cls, parent_match_id: str, **defaults) -> "UnifiedMatch":
        """
        Atomic get-or-create with a row lock.
        Pass default field values as kwargs (e.g. home_team_name="Arsenal").
        Always returns a locked, in-session row ready for mutation.
        """
        obj = cls.lock(parent_match_id)
        if not obj:
            obj = cls(parent_match_id=parent_match_id, **defaults)
            db.session.add(obj)
            db.session.flush()   # get ID without full commit
        return obj

    # ── Specifier key ────────────────────────────────────────────────────────

    @staticmethod
    def _spec_key(specifier: Any) -> str:
        return str(specifier) if specifier is not None else "null"

    # ── Odds mutation ────────────────────────────────────────────────────────

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
        Thread-safe only when called after .lock() or .get_or_create_locked().
        """
        markets  = dict(self.markets_json or {})
        spec_key = self._spec_key(specifier)
        bk_key   = str(bookmaker_id)
        now_str  = _utcnow().isoformat()

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
            best_bk, best_p = max(entry["bookmakers"].items(), key=lambda kv: float(kv[1]))
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

    def mark_finished(self, score_home: int, score_away: int,
                      ht_home: int | None = None, ht_away: int | None = None) -> None:
        self.score_home    = score_home
        self.score_away    = score_away
        self.ht_score_home = ht_home
        self.ht_score_away = ht_away
        self.status        = MatchStatus.FINISHED
        self.finished_at   = _utcnow_naive()
        if score_home > score_away: self.result = "1"
        elif score_home == score_away: self.result = "X"
        else: self.result = "2"

    def to_dict(self, include_bookmaker_odds: bool = False) -> dict:
        d: dict = {
            "id":              self.id,
            "parent_match_id": self.parent_match_id,
            "home_team":       self.home_team_name,
            "away_team":       self.away_team_name,
            "sport":           self.sport_name,
            "competition":     self.competition_name,
            "country":         self.country_name,
            "start_time":      self.start_time.isoformat() if self.start_time else None,
            "status":          self.status.value if self.status else None,
            "result":          self.result,
            "score_home":      self.score_home,
            "score_away":      self.score_away,
            "ht_score_home":   self.ht_score_home,
            "ht_score_away":   self.ht_score_away,
            "finished_at":     self.finished_at.isoformat() if self.finished_at else None,
            "markets":         self.markets_json or {},
            "updated_at":      self.updated_at.isoformat() if self.updated_at else None,
        }
        if include_bookmaker_odds:
            d["bookmaker_odds"] = [b.to_dict() for b in self.bookmaker_odds]
        return d


# ═════════════════════════════════════════════════════════════════════════════
# BOOKMAKER MATCH ODDS
# ═════════════════════════════════════════════════════════════════════════════

class BookmakerMatchOdds(db.Model):
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

    markets_json = db.Column(db.JSON, nullable=False, default=dict)
    raw_snapshot = db.Column(db.JSON, nullable=True)
    is_active    = db.Column(db.Boolean, default=True, nullable=False, index=True)

    # Manual row version — incremented by upsert_selection so callers can detect gaps
    row_version  = db.Column(db.Integer, nullable=True, default=0, server_default="0")

    created_at = db.Column(db.DateTime, default=_utcnow_naive)
    updated_at = db.Column(db.DateTime, default=_utcnow_naive, onupdate=_utcnow_naive)

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

    @classmethod
    def lock(cls, match_id: int, bookmaker_id: int) -> "BookmakerMatchOdds | None":
        return (
            cls.query
            .filter_by(match_id=match_id, bookmaker_id=bookmaker_id)
            .with_for_update()
            .first()
        )

    @classmethod
    def get_or_create_locked(cls, match_id: int, bookmaker_id: int) -> "BookmakerMatchOdds":
        obj = cls.lock(match_id, bookmaker_id)
        if not obj:
            obj = cls(match_id=match_id, bookmaker_id=bookmaker_id)
            db.session.add(obj)
            db.session.flush()
        return obj

    @staticmethod
    def _spec_key(specifier: Any) -> str:
        return str(specifier) if specifier is not None else "null"

    def upsert_selection(
        self,
        market:    str,
        specifier: Any,
        selection: str,
        price:     float,
    ) -> tuple[bool, float | None]:
        """
        Insert or update one selection.
        Returns (price_changed, old_price).
        Increments row_version on any change.
        """
        markets  = dict(self.markets_json or {})
        spec_key = self._spec_key(specifier)
        now_str  = _utcnow().isoformat()

        markets.setdefault(market, {})
        markets[market].setdefault(spec_key, {})
        sel_map = markets[market][spec_key]

        if selection not in sel_map:
            sel_map[selection] = {"price": price, "is_active": True, "updated_at": now_str}
            self.markets_json = markets
            self.row_version  = (self.row_version or 0) + 1
            self.updated_at   = _utcnow_naive()
            return True, None

        entry     = sel_map[selection]
        old_price = entry.get("price")
        entry["is_active"]  = True
        entry["updated_at"] = now_str

        if old_price == price:
            self.markets_json = markets
            return False, old_price

        entry["price"]    = price
        self.markets_json = markets
        self.row_version  = (self.row_version or 0) + 1
        self.updated_at   = _utcnow_naive()
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
            "row_version":  self.row_version,
            "updated_at":   self.updated_at.isoformat() if self.updated_at else None,
        }


# ═════════════════════════════════════════════════════════════════════════════
# BOOKMAKER ODDS HISTORY
# ═════════════════════════════════════════════════════════════════════════════

class BookmakerOddsHistory(db.Model):
    """
    Append-only price change log.
    A row is appended:
      • When a selection is first seen        (old_price=None)
      • Every time the price changes          (old_price = previous value)
    """
    __tablename__ = "bookmaker_odds_history"

    id           = db.Column(db.Integer, primary_key=True)
    bmo_id       = db.Column(
        db.Integer, db.ForeignKey("bookmaker_match_odds.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    bookmaker_id = db.Column(
        db.Integer, db.ForeignKey("bookmakers.id"), nullable=False, index=True,
    )
    match_id     = db.Column(
        db.Integer, db.ForeignKey("unified_matches.id"), nullable=False, index=True,
    )

    market    = db.Column(db.String(120), nullable=False)
    specifier = db.Column(db.String(50),  nullable=True)
    selection = db.Column(db.String(100), nullable=False)
    old_price   = db.Column(db.Float, nullable=True)
    new_price   = db.Column(db.Float, nullable=False)
    price_delta = db.Column(db.Float, nullable=True)

    recorded_at = db.Column(db.DateTime, default=_utcnow_naive, nullable=False, index=True)

    __table_args__ = (
        db.Index("ix_boh_bmo_market_sel",  "bmo_id",       "market", "selection"),
        db.Index("ix_boh_match_market",    "match_id",     "market", "recorded_at"),
        db.Index("ix_boh_bookmaker_time",  "bookmaker_id", "recorded_at"),
        db.Index("ix_boh_match_time",      "match_id",     "recorded_at"),
    )

    bmo = db.relationship("BookmakerMatchOdds", back_populates="history")

    @classmethod
    def bulk_append(cls, rows: list[dict]) -> None:
        """
        Efficient batch insert — does NOT go through the ORM mapper.
        rows: list of dicts matching the column names above.
        """
        if rows:
            db.session.execute(cls.__table__.insert(), rows)

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


# ═════════════════════════════════════════════════════════════════════════════
# ARBITRAGE OPPORTUNITY
# ═════════════════════════════════════════════════════════════════════════════

class ArbitrageOpportunity(db.Model):
    """
    Tracks every arbitrage window detected across bookmakers.

    An arb exists when betting all outcomes of a market across different
    bookmakers guarantees profit regardless of result.

    Stored data allows users to query:
      - All arbs detected in a date range
      - Best arb ever seen for a sport / market
      - How long arbs typically last (open_at → closed_at)
      - Which bookmaker pairs produce the most arbs
      - Theoretical profit if staked at optimal Kelly / flat stake

    legs_json shape
    ───────────────
    [
      {
        "selection":    "Home",
        "bookmaker_id": 3,
        "bookmaker":    "Sportpesa",
        "price":        2.10,
        "stake_pct":    47.62    <- % of total bank to stake on this leg
      },
      {
        "selection":    "Away",
        "bookmaker_id": 7,
        "bookmaker":    "Betika",
        "price":        2.20,
        "stake_pct":    52.38
      }
    ]

    peak tracking
    ─────────────
    While the window is OPEN, each price-change tick updates
    peak_profit_pct and peak_detected_at if the new profit is higher.
    """
    __tablename__ = "arbitrage_opportunities"

    id       = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(
        db.Integer, db.ForeignKey("unified_matches.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )

    # Match denorms for fast queries without JOINs
    home_team    = db.Column(db.String(120), nullable=True)
    away_team    = db.Column(db.String(120), nullable=True)
    sport        = db.Column(db.String(80),  nullable=True, index=True)
    competition  = db.Column(db.String(120), nullable=True)
    match_start  = db.Column(db.DateTime,    nullable=True, index=True)

    market    = db.Column(db.String(120), nullable=False, index=True)
    specifier = db.Column(db.String(50),  nullable=True)

    # Core arb metrics
    profit_pct       = db.Column(db.Float, nullable=False,
                                  comment="Guaranteed profit % at time of detection")
    peak_profit_pct  = db.Column(db.Float, nullable=False,
                                  comment="Highest profit % seen while window was open")
    peak_detected_at = db.Column(db.DateTime, nullable=True,
                                  comment="When peak_profit_pct was recorded")
    arb_sum          = db.Column(db.Float, nullable=False,
                                  comment="Sum of 1/price across all legs; <1.0 = profitable")

    # Exploitation guide
    legs_json = db.Column(
        db.JSON, nullable=False,
        comment="List of {selection, bookmaker_id, bookmaker, price, stake_pct}",
    )
    stake_100_returns = db.Column(
        db.Float, nullable=True,
        comment="Total return on a flat KSh 100 stake optimally distributed",
    )

    # Bookmaker pair summary for fast filtering
    bookmaker_ids = db.Column(
        db.JSON, nullable=False, default=list,
        comment="Sorted list of bookmaker_ids involved",
    )

    # Lifecycle
    status     = db.Column(
        db.Enum(OpportunityStatus, native_enum=False),
        default=OpportunityStatus.OPEN, nullable=False, index=True,
    )
    open_at    = db.Column(db.DateTime, default=_utcnow_naive, nullable=False, index=True)
    closed_at  = db.Column(db.DateTime, nullable=True, index=True)
    duration_s = db.Column(
        db.Integer, nullable=True,
        comment="Seconds the arb window was open (closed_at - open_at)",
    )

    # Result outcome (populated after FINISHED)
    result_outcome  = db.Column(db.String(100), nullable=True,
                                 comment="Which selection won (e.g. 'Home')")
    actual_profit   = db.Column(
        db.Float, nullable=True,
        comment="Actual profit % if user staked at open_at prices and all legs settled",
    )

    __table_args__ = (
        db.Index("ix_arb_sport_open",   "sport",   "open_at"),
        db.Index("ix_arb_market_open",  "market",  "open_at"),
        db.Index("ix_arb_status_open",  "status",  "open_at"),
        db.Index("ix_arb_profit",       "profit_pct"),
        db.Index("ix_arb_match_market", "match_id", "market"),
    )

    match = db.relationship("UnifiedMatch", back_populates="arbitrage_opps")

    def close(self, reason: str = "price_moved") -> None:
        """Mark this window as closed and compute duration."""
        self.status    = OpportunityStatus.CLOSED
        self.closed_at = _utcnow_naive()
        if self.open_at:
            self.duration_s = int(
                (self.closed_at - self.open_at).total_seconds()
            )

    def update_peak(self, new_profit_pct: float) -> None:
        if new_profit_pct > (self.peak_profit_pct or 0):
            self.peak_profit_pct  = new_profit_pct
            self.peak_detected_at = _utcnow_naive()

    def settle(self, winning_selection: str, stake_per_leg: float = 100.0) -> None:
        """
        After match finishes: record which selection won and compute actual profit.
        stake_per_leg = amount bet on each leg (assumes flat staking for simplicity).
        """
        self.result_outcome = winning_selection
        legs = self.legs_json or []
        winning_leg = next((l for l in legs if l["selection"] == winning_selection), None)
        if winning_leg:
            total_staked = stake_per_leg * len(legs)
            payout       = stake_per_leg * winning_leg["price"]
            self.actual_profit = (payout - total_staked) / total_staked * 100

    def to_dict(self) -> dict:
        return {
            "id":               self.id,
            "match_id":         self.match_id,
            "home_team":        self.home_team,
            "away_team":        self.away_team,
            "sport":            self.sport,
            "competition":      self.competition,
            "match_start":      self.match_start.isoformat() if self.match_start else None,
            "market":           self.market,
            "specifier":        self.specifier,
            "profit_pct":       self.profit_pct,
            "peak_profit_pct":  self.peak_profit_pct,
            "peak_detected_at": self.peak_detected_at.isoformat() if self.peak_detected_at else None,
            "arb_sum":          self.arb_sum,
            "legs":             self.legs_json or [],
            "stake_100_returns": self.stake_100_returns,
            "bookmaker_ids":    self.bookmaker_ids or [],
            "status":           self.status.value if self.status else None,
            "open_at":          self.open_at.isoformat() if self.open_at else None,
            "closed_at":        self.closed_at.isoformat() if self.closed_at else None,
            "duration_s":       self.duration_s,
            "result_outcome":   self.result_outcome,
            "actual_profit":    self.actual_profit,
        }


# ═════════════════════════════════════════════════════════════════════════════
# EV OPPORTUNITY
# ═════════════════════════════════════════════════════════════════════════════

class EVOpportunity(db.Model):
    """
    Positive expected-value window for a single selection at a single bookmaker.

    EV is computed by comparing one bookmaker's price against the consensus
    (the fair probability estimated from the market as a whole, using all
    bookmakers' prices to remove the over-round).

    Formula
    ───────
    For a market with N outcomes and bookmaker prices p_i (from all bookmakers):

      fair_prob_i = (1 / p_i_best) / sum(1 / p_j_best for j in outcomes)
      ev_pct      = (bk_price * fair_prob - 1) * 100

    A positive ev_pct means the bookmaker is paying more than the true
    probability of that outcome — exploitable over many bets (Kelly criterion).

    closing_line_value (CLV)
    ─────────────────────────
    If the match has kicked off, we also store CLV:
      clv_pct = (entry_price / closing_price - 1) * 100
    Positive CLV = you beat the closing line = long-run edge.

    Users can query:
      - All EV > 5% in the last 30 days for soccer
      - Best EV seen for Over/Under markets
      - CLV-positive records (proof of long-run profitability)
      - EV vs actual result settlement for model validation
    """
    __tablename__ = "ev_opportunities"

    id       = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(
        db.Integer, db.ForeignKey("unified_matches.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )

    # Match denorms
    home_team   = db.Column(db.String(120), nullable=True)
    away_team   = db.Column(db.String(120), nullable=True)
    sport       = db.Column(db.String(80),  nullable=True, index=True)
    competition = db.Column(db.String(120), nullable=True)
    match_start = db.Column(db.DateTime,    nullable=True, index=True)

    # Market details
    market       = db.Column(db.String(120), nullable=False, index=True)
    specifier    = db.Column(db.String(50),  nullable=True)
    selection    = db.Column(db.String(100), nullable=False)
    bookmaker_id = db.Column(db.Integer, db.ForeignKey("bookmakers.id"), nullable=False, index=True)
    bookmaker    = db.Column(db.String(80), nullable=True)

    # Prices at detection time
    offered_price    = db.Column(db.Float, nullable=False,
                                  comment="This bookmaker's price when EV was detected")
    consensus_price  = db.Column(db.Float, nullable=False,
                                  comment="Average best price across all bookmakers")
    fair_prob        = db.Column(db.Float, nullable=False,
                                  comment="Estimated true probability (0-1) at detection")
    ev_pct           = db.Column(db.Float, nullable=False, index=True,
                                  comment="Expected value % — positive = edge")
    peak_ev_pct      = db.Column(db.Float, nullable=False)
    peak_detected_at = db.Column(db.DateTime, nullable=True)

    # Number of bookmakers used in consensus
    bookmakers_in_consensus = db.Column(db.Integer, nullable=False, default=1)

    # Kelly criterion suggested stake
    kelly_fraction  = db.Column(db.Float, nullable=True,
                                 comment="Kelly fraction (0-1) — fraction of bankroll to bet")
    half_kelly      = db.Column(db.Float, nullable=True,
                                 comment="Half-Kelly stake (conservative practical sizing)")

    # Lifecycle
    status    = db.Column(
        db.Enum(OpportunityStatus, native_enum=False),
        default=OpportunityStatus.OPEN, nullable=False, index=True,
    )
    open_at   = db.Column(db.DateTime, default=_utcnow_naive, nullable=False, index=True)
    closed_at = db.Column(db.DateTime, nullable=True)
    duration_s = db.Column(db.Integer, nullable=True)

    # Closing line value (populated after match kick-off)
    closing_price = db.Column(db.Float, nullable=True,
                               comment="Market price at match kick-off (closing line)")
    clv_pct       = db.Column(db.Float, nullable=True, index=True,
                               comment="(entry/closing - 1)*100; positive = beat closing line")

    # Settlement (populated after match finishes)
    won            = db.Column(db.Boolean, nullable=True,
                                comment="True if this selection won")
    settlement_pct = db.Column(
        db.Float, nullable=True,
        comment="Actual P&L % on this bet: (price-1)*100 if won else -100",
    )

    __table_args__ = (
        db.Index("ix_ev_sport_open",   "sport",   "open_at"),
        db.Index("ix_ev_market_open",  "market",  "open_at"),
        db.Index("ix_ev_status_ev",    "status",  "ev_pct"),
        db.Index("ix_ev_bk_open",      "bookmaker_id", "open_at"),
        db.Index("ix_ev_clv",          "clv_pct"),
        db.Index("ix_ev_match_market", "match_id", "market"),
    )

    match = db.relationship("UnifiedMatch", back_populates="ev_opps")

    def close(self) -> None:
        self.status    = OpportunityStatus.CLOSED
        self.closed_at = _utcnow_naive()
        if self.open_at:
            self.duration_s = int((self.closed_at - self.open_at).total_seconds())

    def update_peak(self, new_ev_pct: float) -> None:
        if new_ev_pct > (self.peak_ev_pct or 0):
            self.peak_ev_pct      = new_ev_pct
            self.peak_detected_at = _utcnow_naive()

    def record_closing_line(self, closing_price: float) -> None:
        self.closing_price = closing_price
        if self.offered_price and closing_price > 0:
            self.clv_pct = (self.offered_price / closing_price - 1) * 100

    def settle(self, won: bool) -> None:
        self.won            = won
        self.settlement_pct = (self.offered_price - 1) * 100 if won else -100.0
        self.status         = OpportunityStatus.CLOSED

    def compute_kelly(self) -> None:
        """Compute Kelly fraction from ev_pct and fair_prob."""
        if not self.fair_prob or not self.offered_price:
            return
        p = self.fair_prob
        b = self.offered_price - 1   # net odds
        if b <= 0 or p <= 0 or p >= 1:
            return
        k = (b * p - (1 - p)) / b
        self.kelly_fraction = max(0.0, round(k, 4))
        self.half_kelly      = round(k / 2, 4)

    def to_dict(self) -> dict:
        return {
            "id":                      self.id,
            "match_id":                self.match_id,
            "home_team":               self.home_team,
            "away_team":               self.away_team,
            "sport":                   self.sport,
            "competition":             self.competition,
            "match_start":             self.match_start.isoformat() if self.match_start else None,
            "market":                  self.market,
            "specifier":               self.specifier,
            "selection":               self.selection,
            "bookmaker_id":            self.bookmaker_id,
            "bookmaker":               self.bookmaker,
            "offered_price":           self.offered_price,
            "consensus_price":         self.consensus_price,
            "fair_prob":               self.fair_prob,
            "ev_pct":                  self.ev_pct,
            "peak_ev_pct":             self.peak_ev_pct,
            "peak_detected_at":        self.peak_detected_at.isoformat() if self.peak_detected_at else None,
            "bookmakers_in_consensus": self.bookmakers_in_consensus,
            "kelly_fraction":          self.kelly_fraction,
            "half_kelly":              self.half_kelly,
            "status":                  self.status.value if self.status else None,
            "open_at":                 self.open_at.isoformat() if self.open_at else None,
            "closed_at":               self.closed_at.isoformat() if self.closed_at else None,
            "duration_s":              self.duration_s,
            "closing_price":           self.closing_price,
            "clv_pct":                 self.clv_pct,
            "won":                     self.won,
            "settlement_pct":          self.settlement_pct,
        }


# ═════════════════════════════════════════════════════════════════════════════
# OPPORTUNITY DETECTION UTILITIES
# ═════════════════════════════════════════════════════════════════════════════

class OpportunityDetector:
    """
    Stateless detector — called after every odds update.
    Returns lists of dicts ready to insert as Arb / EV rows.

    Usage (in harvest_tasks.py after upsert_bookmaker_price):
        from app.models.odds_model import OpportunityDetector
        detector = OpportunityDetector(min_profit_pct=0.5, min_ev_pct=3.0)
        arbs = detector.find_arbs(match)
        evs  = detector.find_ev(match)
    """

    def __init__(
        self,
        min_profit_pct: float = 0.5,   # minimum arb profit to record (%)
        min_ev_pct:     float = 3.0,   # minimum EV to record (%)
    ) -> None:
        self.min_profit_pct = min_profit_pct
        self.min_ev_pct     = min_ev_pct

    def find_arbs(self, match: UnifiedMatch) -> list[dict]:
        """
        Scan match.markets_json for arbitrage opportunities.
        Returns list of kwargs dicts for ArbitrageOpportunity(**kwargs).
        """
        results = []
        markets = match.markets_json or {}

        for market_name, specs in markets.items():
            for spec_key, selections in specs.items():
                if len(selections) < 2:
                    continue

                # Collect best price per selection
                legs = []
                for sel_name, sel_data in selections.items():
                    best_p = sel_data.get("best_price")
                    best_bk = sel_data.get("best_bookmaker_id")
                    if not best_p or best_p <= 1.0:
                        continue
                    # Find bookmaker name from legs_json context — use ID as fallback
                    bk_name = str(best_bk)
                    legs.append({
                        "selection":    sel_name,
                        "bookmaker_id": best_bk,
                        "bookmaker":    bk_name,
                        "price":        best_p,
                    })

                if len(legs) < 2:
                    continue

                arb_sum = sum(1.0 / leg["price"] for leg in legs)
                if arb_sum >= 1.0:
                    continue   # no arb

                profit_pct = (1.0 / arb_sum - 1.0) * 100
                if profit_pct < self.min_profit_pct:
                    continue

                # Compute optimal stake percentages
                total_bank = 100.0
                for leg in legs:
                    leg["stake_pct"] = round(
                        (1.0 / leg["price"]) / arb_sum * 100, 2
                    )

                specifier = None if spec_key == "null" else spec_key
                returns   = round(total_bank / arb_sum, 2)

                results.append(dict(
                    match_id          = match.id,
                    home_team         = match.home_team_name,
                    away_team         = match.away_team_name,
                    sport             = match.sport_name,
                    competition       = match.competition_name,
                    match_start       = match.start_time,
                    market            = market_name,
                    specifier         = specifier,
                    profit_pct        = round(profit_pct, 4),
                    peak_profit_pct   = round(profit_pct, 4),
                    arb_sum           = round(arb_sum, 6),
                    legs_json         = legs,
                    stake_100_returns = returns,
                    bookmaker_ids     = sorted({l["bookmaker_id"] for l in legs}),
                    status            = OpportunityStatus.OPEN,
                    open_at           = _utcnow_naive(),
                ))

        return results

    def find_ev(self, match: UnifiedMatch) -> list[dict]:
        """
        Scan match.markets_json for positive-EV selections.
        Returns list of kwargs dicts for EVOpportunity(**kwargs).
        """
        results = []
        markets = match.markets_json or {}

        for market_name, specs in markets.items():
            for spec_key, selections in specs.items():
                if len(selections) < 2:
                    continue

                # Build consensus: best price per selection
                best_prices = {
                    sel: d.get("best_price", 0)
                    for sel, d in selections.items()
                    if d.get("best_price", 0) > 1.0
                }
                if len(best_prices) < 2:
                    continue

                # Remove over-round to get fair probabilities
                inv_sum = sum(1.0 / p for p in best_prices.values())
                fair_probs = {sel: (1.0 / p) / inv_sum for sel, p in best_prices.items()}

                specifier = None if spec_key == "null" else spec_key

                # Check each bookmaker's price for each selection
                for sel_name, sel_data in selections.items():
                    fair_p = fair_probs.get(sel_name)
                    if not fair_p:
                        continue

                    bk_prices: dict = sel_data.get("bookmakers") or {}
                    consensus_p     = best_prices.get(sel_name, 0)

                    for bk_id_str, bk_price in bk_prices.items():
                        if not bk_price or float(bk_price) <= 1.0:
                            continue
                        bk_price = float(bk_price)
                        ev_pct   = (bk_price * fair_p - 1.0) * 100

                        if ev_pct < self.min_ev_pct:
                            continue

                        bk_id = int(bk_id_str)
                        b     = bk_price - 1
                        kelly = max(0.0, (b * fair_p - (1 - fair_p)) / b) if b > 0 else 0.0

                        results.append(dict(
                            match_id                = match.id,
                            home_team               = match.home_team_name,
                            away_team               = match.away_team_name,
                            sport                   = match.sport_name,
                            competition             = match.competition_name,
                            match_start             = match.start_time,
                            market                  = market_name,
                            specifier               = specifier,
                            selection               = sel_name,
                            bookmaker_id            = bk_id,
                            bookmaker               = str(bk_id),
                            offered_price           = bk_price,
                            consensus_price         = round(consensus_p, 4),
                            fair_prob               = round(fair_p, 6),
                            ev_pct                  = round(ev_pct, 4),
                            peak_ev_pct             = round(ev_pct, 4),
                            bookmakers_in_consensus = len(bk_prices),
                            kelly_fraction          = round(kelly, 4),
                            half_kelly              = round(kelly / 2, 4),
                            status                  = OpportunityStatus.OPEN,
                            open_at                 = _utcnow_naive(),
                        ))

        return results


# ═════════════════════════════════════════════════════════════════════════════
# QUERY HELPERS
# ═════════════════════════════════════════════════════════════════════════════

class OddsQueryHelper:
    """
    Pre-built queries for common user-facing requests.
    All methods return SQLAlchemy query objects — caller adds .all() / .paginate().

    Usage
    ─────
    from app.models.odds_model import OddsQueryHelper as Q

    # Games on 2026-03-27
    matches = Q.matches_by_date("2026-03-27", "2026-03-27").all()

    # Arbitrage history for soccer last 7 days
    arbs = Q.arb_history(sport="soccer", days=7).order_by(
        ArbitrageOpportunity.profit_pct.desc()
    ).limit(50).all()
    """

    @staticmethod
    def matches_by_date(
        date_from:   str,       # "YYYY-MM-DD"
        date_to:     str,       # "YYYY-MM-DD" (inclusive)
        sport:       str | None = None,
        competition: str | None = None,
        status:      MatchStatus | None = None,
    ):
        from datetime import date, timedelta
        df = datetime.strptime(date_from, "%Y-%m-%d")
        dt = datetime.strptime(date_to,   "%Y-%m-%d") + timedelta(days=1)

        q = UnifiedMatch.query.filter(
            UnifiedMatch.start_time >= df,
            UnifiedMatch.start_time <  dt,
        )
        if sport:       q = q.filter(UnifiedMatch.sport_name == sport)
        if competition: q = q.filter(UnifiedMatch.competition_name.ilike(f"%{competition}%"))
        if status:      q = q.filter(UnifiedMatch.status == status)
        return q.order_by(UnifiedMatch.start_time)

    @staticmethod
    def finished_matches(
        date_from:  str | None = None,
        date_to:    str | None = None,
        sport:      str | None = None,
    ):
        q = UnifiedMatch.query.filter(UnifiedMatch.status == MatchStatus.FINISHED)
        if date_from:
            df = datetime.strptime(date_from, "%Y-%m-%d")
            q  = q.filter(UnifiedMatch.finished_at >= df)
        if date_to:
            dt = datetime.strptime(date_to, "%Y-%m-%d")
            from datetime import timedelta
            q  = q.filter(UnifiedMatch.finished_at < dt + timedelta(days=1))
        if sport:
            q = q.filter(UnifiedMatch.sport_name == sport)
        return q.order_by(UnifiedMatch.finished_at.desc())

    @staticmethod
    def arb_history(
        sport:        str | None = None,
        market:       str | None = None,
        bookmaker_id: int | None = None,
        min_profit:   float = 0.0,
        days:         int   = 30,
        status:       OpportunityStatus | None = None,
    ):
        from datetime import timedelta
        cutoff = _utcnow_naive() - timedelta(days=days)
        q = ArbitrageOpportunity.query.filter(
            ArbitrageOpportunity.open_at >= cutoff,
            ArbitrageOpportunity.profit_pct >= min_profit,
        )
        if sport:  q = q.filter(ArbitrageOpportunity.sport == sport)
        if market: q = q.filter(ArbitrageOpportunity.market == market)
        if status: q = q.filter(ArbitrageOpportunity.status == status)
        if bookmaker_id:
            # JSON contains check — bookmaker_ids is a sorted list
            q = q.filter(
                ArbitrageOpportunity.bookmaker_ids.contains([bookmaker_id])
            )
        return q.order_by(ArbitrageOpportunity.open_at.desc())

    @staticmethod
    def arb_peak_summary(sport: str | None = None, days: int = 90):
        """Best ever arb by market, useful for 'what was the highest arb seen'."""
        from sqlalchemy import func
        from datetime import timedelta
        cutoff = _utcnow_naive() - timedelta(days=days)
        q = db.session.query(
            ArbitrageOpportunity.market,
            ArbitrageOpportunity.specifier,
            func.max(ArbitrageOpportunity.peak_profit_pct).label("best_profit_pct"),
            func.avg(ArbitrageOpportunity.profit_pct).label("avg_profit_pct"),
            func.count(ArbitrageOpportunity.id).label("count"),
            func.avg(ArbitrageOpportunity.duration_s).label("avg_duration_s"),
        ).filter(ArbitrageOpportunity.open_at >= cutoff)
        if sport:
            q = q.filter(ArbitrageOpportunity.sport == sport)
        return q.group_by(
            ArbitrageOpportunity.market,
            ArbitrageOpportunity.specifier,
        ).order_by(func.max(ArbitrageOpportunity.peak_profit_pct).desc())

    @staticmethod
    def ev_history(
        sport:        str | None = None,
        market:       str | None = None,
        bookmaker_id: int | None = None,
        min_ev:       float = 0.0,
        days:         int   = 30,
        clv_positive: bool  = False,
        status:       OpportunityStatus | None = None,
    ):
        from datetime import timedelta
        cutoff = _utcnow_naive() - timedelta(days=days)
        q = EVOpportunity.query.filter(
            EVOpportunity.open_at >= cutoff,
            EVOpportunity.ev_pct  >= min_ev,
        )
        if sport:        q = q.filter(EVOpportunity.sport == sport)
        if market:       q = q.filter(EVOpportunity.market == market)
        if bookmaker_id: q = q.filter(EVOpportunity.bookmaker_id == bookmaker_id)
        if status:       q = q.filter(EVOpportunity.status == status)
        if clv_positive: q = q.filter(EVOpportunity.clv_pct > 0)
        return q.order_by(EVOpportunity.open_at.desc())

    @staticmethod
    def odds_movement(
        match_id:     int,
        market:       str,
        selection:    str,
        bookmaker_id: int | None = None,
    ):
        """Full odds timeline for one selection — for line-movement charts."""
        q = BookmakerOddsHistory.query.filter(
            BookmakerOddsHistory.match_id  == match_id,
            BookmakerOddsHistory.market    == market,
            BookmakerOddsHistory.selection == selection,
        )
        if bookmaker_id:
            q = q.filter(BookmakerOddsHistory.bookmaker_id == bookmaker_id)
        return q.order_by(BookmakerOddsHistory.recorded_at)