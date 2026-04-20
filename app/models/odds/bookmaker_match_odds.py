from typing import Any
from app.extensions import db
from .common import _utcnow_naive, _utcnow

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

    row_version  = db.Column(db.Integer, nullable=True, default=0, server_default="0")

    created_at = db.Column(db.DateTime, default=_utcnow_naive)
    updated_at = db.Column(db.DateTime, default=_utcnow_naive, onupdate=_utcnow_naive)

    __table_args__ = (
        db.UniqueConstraint("match_id", "bookmaker_id", name="uq_bmo_match_bookmaker"),
        db.Index("ix_bmo_bookmaker_active", "bookmaker_id", "is_active"),
    )

    match   = db.relationship("UnifiedMatch", back_populates="match_odds_rel")
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