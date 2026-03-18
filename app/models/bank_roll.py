from app.extensions import db
from datetime import datetime, timezone, timedelta

from app.utils.fetcher_utils import BankrollStrategy

class BankrollAccount(db.Model):
    """One row per bookmaker account the user holds."""
    __tablename__ = "bankroll_accounts"
 
    id           = db.Column(db.Integer, primary_key=True)
    user_id      = db.Column(db.Integer, db.ForeignKey("customers.id", ondelete="CASCADE"),
                             nullable=False, index=True)
    bookmaker_id = db.Column(db.Integer, db.ForeignKey("bookmakers.id"), nullable=True)
    bookmaker_name = db.Column(db.String(80), nullable=False)   # denorm for display
    balance_kes  = db.Column(db.Float, default=0.0, nullable=False)
    currency     = db.Column(db.String(8), default="KES", nullable=False)
    is_active    = db.Column(db.Boolean, default=True, nullable=False)
    created_at   = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at   = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                             onupdate=lambda: datetime.now(timezone.utc))
 
    user = db.relationship("Customer", back_populates="bankroll_accounts")
    bets = db.relationship("BankrollBet", back_populates="account", lazy="dynamic")
 
    def to_dict(self) -> dict:
        return {
            "id":             self.id,
            "bookmaker_name": self.bookmaker_name,
            "balance_kes":    self.balance_kes,
            "currency":       self.currency,
            "is_active":      self.is_active,
        }
 
 
class BankrollTarget(db.Model):
    """Rolling target for the bankroll engine."""
    __tablename__ = "bankroll_targets"
 
    id            = db.Column(db.Integer, primary_key=True)
    user_id       = db.Column(db.Integer, db.ForeignKey("customers.id", ondelete="CASCADE"),
                              nullable=False, index=True)
    name          = db.Column(db.String(80), default="My Target", nullable=False)
 
    # Goal
    target_kes    = db.Column(db.Float, nullable=False)   # profit target in KES
    starting_kes  = db.Column(db.Float, nullable=False)   # total starting bankroll
    current_kes   = db.Column(db.Float, nullable=False)   # computed from bets
 
    # Strategy config
    strategy          = db.Column(db.String(20), default=BankrollStrategy.KELLY.value)
    max_bet_pct       = db.Column(db.Float, default=0.05)  # max 5% of bankroll per bet
    min_edge_pct      = db.Column(db.Float, default=2.0)   # only bet if EV edge > 2%
    min_arb_profit    = db.Column(db.Float, default=1.0)   # only arb if profit > 1%
    max_loss_stop     = db.Column(db.Float, default=0.20)  # stop if lost 20% of start
    notify_on_arb     = db.Column(db.Boolean, default=True)
    notify_threshold  = db.Column(db.Float, default=1.5)  # % profit threshold for email
 
    # Lifecycle
    is_active     = db.Column(db.Boolean, default=True, nullable=False)
    started_at    = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    achieved_at   = db.Column(db.DateTime, nullable=True)  # when target was hit
    stopped_at    = db.Column(db.DateTime, nullable=True)   # when stop-loss triggered
 
    user = db.relationship("Customer", back_populates="bankroll_targets")
    bets = db.relationship("BankrollBet", back_populates="target", lazy="dynamic")
 
    @property
    def progress_pct(self) -> float:
        if self.starting_kes <= 0:
            return 0.0
        profit = self.current_kes - self.starting_kes
        return round((profit / self.target_kes) * 100, 2) if self.target_kes else 0.0
 
    @property
    def is_stop_loss_triggered(self) -> bool:
        if self.starting_kes <= 0:
            return False
        loss = (self.starting_kes - self.current_kes) / self.starting_kes
        return loss >= self.max_loss_stop
 
    def to_dict(self) -> dict:
        return {
            "id":               self.id,
            "name":             self.name,
            "target_kes":       self.target_kes,
            "starting_kes":     self.starting_kes,
            "current_kes":      self.current_kes,
            "profit_kes":       round(self.current_kes - self.starting_kes, 2),
            "progress_pct":     self.progress_pct,
            "strategy":         self.strategy,
            "max_bet_pct":      self.max_bet_pct,
            "min_edge_pct":     self.min_edge_pct,
            "min_arb_profit":   self.min_arb_profit,
            "notify_on_arb":    self.notify_on_arb,
            "notify_threshold": self.notify_threshold,
            "is_active":        self.is_active,
            "stop_loss_hit":    self.is_stop_loss_triggered,
        }
 
 
class BankrollBet(db.Model):
    """Every bet placed / suggested by the bankroll rolling engine."""
    __tablename__ = "bankroll_bets"
 
    id           = db.Column(db.Integer, primary_key=True)
    target_id    = db.Column(db.Integer, db.ForeignKey("bankroll_targets.id"), nullable=False, index=True)
    account_id   = db.Column(db.Integer, db.ForeignKey("bankroll_accounts.id"), nullable=False)
    match_id     = db.Column(db.Integer, db.ForeignKey("unified_matches.id"), nullable=True)
 
    bet_type       = db.Column(db.String(20), nullable=False)   # "arb", "ev", "kelly"
    market         = db.Column(db.String(80), nullable=False)
    selection      = db.Column(db.String(80), nullable=False)
    bookmaker_name = db.Column(db.String(80), nullable=False)
    odds           = db.Column(db.Float, nullable=False)
    stake_kes      = db.Column(db.Float, nullable=False)        # recommended stake
 
    # Outcome tracking
    status         = db.Column(db.String(20), default="pending")  # pending/won/lost/void
    pnl_kes        = db.Column(db.Float, nullable=True)           # actual P&L
    settled_at     = db.Column(db.DateTime, nullable=True)
    placed_at      = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
 
    # Kelly / EV metadata
    kelly_fraction = db.Column(db.Float, nullable=True)
    edge_pct       = db.Column(db.Float, nullable=True)
    arb_profit_pct = db.Column(db.Float, nullable=True)
 
    target  = db.relationship("BankrollTarget", back_populates="bets")
    account = db.relationship("BankrollAccount", back_populates="bets")
 
    def to_dict(self) -> dict:
        return {
            "id":             self.id,
            "bet_type":       self.bet_type,
            "market":         self.market,
            "selection":      self.selection,
            "bookmaker_name": self.bookmaker_name,
            "odds":           self.odds,
            "stake_kes":      self.stake_kes,
            "status":         self.status,
            "pnl_kes":        self.pnl_kes,
            "edge_pct":       self.edge_pct,
            "arb_profit_pct": self.arb_profit_pct,
            "placed_at":      self.placed_at.isoformat() if self.placed_at else None,
            "settled_at":     self.settled_at.isoformat() if self.settled_at else None,
        }