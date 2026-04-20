from app.extensions import db
from .common import _utcnow_naive

class BookmakerOddsHistory(db.Model):
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