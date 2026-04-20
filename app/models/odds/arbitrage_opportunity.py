from app.extensions import db
from .common import OpportunityStatus, _utcnow_naive

class ArbitrageOpportunity(db.Model):
    __tablename__ = "arbitrage_opportunities"

    id       = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(
        db.Integer, db.ForeignKey("unified_matches.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )

    home_team    = db.Column(db.String(120), nullable=True)
    away_team    = db.Column(db.String(120), nullable=True)
    sport        = db.Column(db.String(80),  nullable=True, index=True)
    competition  = db.Column(db.String(120), nullable=True)
    match_start  = db.Column(db.DateTime,    nullable=True, index=True)

    market    = db.Column(db.String(120), nullable=False, index=True)
    specifier = db.Column(db.String(50),  nullable=True)

    profit_pct       = db.Column(db.Float, nullable=False)
    peak_profit_pct  = db.Column(db.Float, nullable=False)
    peak_detected_at = db.Column(db.DateTime, nullable=True)
    arb_sum          = db.Column(db.Float, nullable=False)

    legs_json = db.Column(db.JSON, nullable=False)
    stake_100_returns = db.Column(db.Float, nullable=True)
    bookmaker_ids = db.Column(db.JSON, nullable=False, default=list)

    status     = db.Column(
        db.Enum(OpportunityStatus, native_enum=False),
        default=OpportunityStatus.OPEN, nullable=False, index=True,
    )
    open_at    = db.Column(db.DateTime, default=_utcnow_naive, nullable=False, index=True)
    closed_at  = db.Column(db.DateTime, nullable=True, index=True)
    duration_s = db.Column(db.Integer, nullable=True)

    result_outcome  = db.Column(db.String(100), nullable=True)
    actual_profit   = db.Column(db.Float, nullable=True)

    __table_args__ = (
        db.Index("ix_arb_sport_open",   "sport",   "open_at"),
        db.Index("ix_arb_market_open",  "market",  "open_at"),
        db.Index("ix_arb_status_open",  "status",  "open_at"),
        db.Index("ix_arb_profit",       "profit_pct"),
        db.Index("ix_arb_match_market", "match_id", "market"),
    )

    match = db.relationship("UnifiedMatch", back_populates="arbitrage_opps")

    def close(self, reason: str = "price_moved") -> None:
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