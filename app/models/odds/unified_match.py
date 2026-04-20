from __future__ import annotations
from typing import Any
from sqlalchemy.dialects.postgresql import JSONB, ARRAY

from app.extensions import db
from .common import MatchStatus, _utcnow, _utcnow_naive

class UnifiedMatch(db.Model):
    __tablename__ = "unified_matches"

    id = db.Column(db.Integer, primary_key=True)
    parent_match_id = db.Column(
        db.String(64), unique=True, nullable=False, index=True,
        comment="Betradar / cross-bookmaker canonical match ID",
    )

    competition_id = db.Column(db.Integer, db.ForeignKey("competitions.id"), nullable=True)
    home_team_id   = db.Column(db.Integer, db.ForeignKey("teams.id"),        nullable=True)
    away_team_id   = db.Column(db.Integer, db.ForeignKey("teams.id"),        nullable=True)

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

    score_home    = db.Column(db.SmallInteger, nullable=True)
    score_away    = db.Column(db.SmallInteger, nullable=True)
    ht_score_home = db.Column(db.SmallInteger, nullable=True)
    ht_score_away = db.Column(db.SmallInteger, nullable=True)
    result        = db.Column(db.String(4),  nullable=True,
                               comment="1 / X / 2 — result for 1X2 market settlement")
    finished_at   = db.Column(db.DateTime, nullable=True)

    markets_json = db.Column(
        db.JSON, nullable=False, default=dict,
        comment=(
            "market → specifier → selection → "
            "{best_price, best_bookmaker_id, bookmakers:{id:price}, is_active, updated_at}"
        ),
    )
    
    # ─── MIGRATION 0004 COLUMNS ──────────────────────────────────────────────
    bookmaker_odds = db.Column(JSONB, nullable=True, server_default='{}')
    external_ids   = db.Column(JSONB, nullable=True, server_default='{}')
    aligned_bks    = db.Column(ARRAY(db.Text), default=list, server_default='{}')

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
        db.Index("idx_unified_bookmaker_odds", "bookmaker_odds", postgresql_using="gin"),
        db.Index("idx_unified_external_ids", "external_ids", postgresql_using="gin"),
    )

    match_odds_rel = db.relationship(
        "BookmakerMatchOdds", back_populates="match",
        lazy="dynamic", cascade="all, delete-orphan",
    )
    arbitrage_opps = db.relationship(
        "ArbitrageOpportunity", back_populates="match",
        lazy="dynamic", cascade="all, delete-orphan",
    )
    ev_opps = db.relationship(
        "EVOpportunity", back_populates="match",
        lazy="dynamic", cascade="all, delete-orphan",
    )

    @classmethod
    def lock(cls, parent_match_id: str) -> "UnifiedMatch | None":
        return (
            cls.query
            .filter_by(parent_match_id=parent_match_id)
            .with_for_update()
            .first()
        )

    @classmethod
    def get_or_create_locked(cls, parent_match_id: str, **defaults) -> "UnifiedMatch":
        obj = cls.lock(parent_match_id)
        if not obj:
            obj = cls(parent_match_id=parent_match_id, **defaults)
            db.session.add(obj)
            db.session.flush()
        return obj

    @staticmethod
    def _spec_key(specifier: Any) -> str:
        return str(specifier) if specifier is not None else "null"

    def upsert_bookmaker_price(
        self,
        market:       str,
        specifier:    Any,
        selection:    str,
        price:        float,
        bookmaker_id: int,
    ) -> None:
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
            d["bookmaker_odds"] = [b.to_dict() for b in self.match_odds_rel]
        return d