from app.extensions import db
from .common import OpportunityStatus, _utcnow_naive

class EVOpportunity(db.Model):
    __tablename__ = "ev_opportunities"

    id       = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(
        db.Integer, db.ForeignKey("unified_matches.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )

    home_team   = db.Column(db.String(120), nullable=True)
    away_team   = db.Column(db.String(120), nullable=True)
    sport       = db.Column(db.String(80),  nullable=True, index=True)
    competition = db.Column(db.String(120), nullable=True)
    match_start = db.Column(db.DateTime,    nullable=True, index=True)

    market       = db.Column(db.String(120), nullable=False, index=True)
    specifier    = db.Column(db.String(50),  nullable=True)
    selection    = db.Column(db.String(100), nullable=False)
    bookmaker_id = db.Column(db.Integer, db.ForeignKey("bookmakers.id"), nullable=False, index=True)
    bookmaker    = db.Column(db.String(80), nullable=True)

    offered_price    = db.Column(db.Float, nullable=False)
    consensus_price  = db.Column(db.Float, nullable=False)
    fair_prob        = db.Column(db.Float, nullable=False)
    ev_pct           = db.Column(db.Float, nullable=False, index=True)
    peak_ev_pct      = db.Column(db.Float, nullable=False)
    peak_detected_at = db.Column(db.DateTime, nullable=True)

    bookmakers_in_consensus = db.Column(db.Integer, nullable=False, default=1)

    kelly_fraction  = db.Column(db.Float, nullable=True)
    half_kelly      = db.Column(db.Float, nullable=True)

    status    = db.Column(
        db.Enum(OpportunityStatus, native_enum=False),
        default=OpportunityStatus.OPEN, nullable=False, index=True,
    )
    open_at   = db.Column(db.DateTime, default=_utcnow_naive, nullable=False, index=True)
    closed_at = db.Column(db.DateTime, nullable=True)
    duration_s = db.Column(db.Integer, nullable=True)

    closing_price = db.Column(db.Float, nullable=True)
    clv_pct       = db.Column(db.Float, nullable=True, index=True)

    won            = db.Column(db.Boolean, nullable=True)
    settlement_pct = db.Column(db.Float, nullable=True)

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
        if not self.fair_prob or not self.offered_price:
            return
        p = self.fair_prob
        b = self.offered_price - 1 
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