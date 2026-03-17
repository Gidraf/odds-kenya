from app.extensions import db
from datetime import datetime, timezone, timedelta
from app.utils.fetcher_utils import TIER_LIMITS, SubscriptionStatus, TIER_PRICES, SubscriptionTier


class Subscription(db.Model):
    __tablename__ = "subscriptions"
 
    id      = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("customers.id", ondelete="CASCADE"),
                        nullable=False, unique=True, index=True)
 
    tier   = db.Column(db.String(20), nullable=False, default=SubscriptionTier.FREE.value)
    status = db.Column(db.String(20), nullable=False, default=SubscriptionStatus.PENDING.value)
 
    # Trial window
    is_trial     = db.Column(db.Boolean, default=True, nullable=False)
    trial_ends   = db.Column(db.DateTime, nullable=True)
 
    # Active period
    period_start = db.Column(db.DateTime, nullable=True)
    period_end   = db.Column(db.DateTime, nullable=True)
 
    # Billing
    amount_kes           = db.Column(db.Integer, nullable=True)
    payment_reference    = db.Column(db.String(128), nullable=True)
    last_billed_at       = db.Column(db.DateTime, nullable=True)
    next_billing_at      = db.Column(db.DateTime, nullable=True)
    auto_renew           = db.Column(db.Boolean, default=True, nullable=False)
    cancellation_reason  = db.Column(db.String(256), nullable=True)
 
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))
 
    user    = db.relationship("User", back_populates="subscription")
    history = db.relationship("SubscriptionHistory", back_populates="subscription",
                              lazy="dynamic", cascade="all, delete-orphan")
 
    @property
    def is_currently_active(self) -> bool:
        now = datetime.now(timezone.utc)
        if self.status not in (SubscriptionStatus.ACTIVE.value, SubscriptionStatus.TRIAL.value):
            return False
        if self.is_trial and self.trial_ends:
            return now <= self.trial_ends.replace(tzinfo=timezone.utc)
        if self.period_end:
            return now <= self.period_end.replace(tzinfo=timezone.utc)
        return False
 
    @classmethod
    def start_trial(cls, user_id: int, tier: str) -> "Subscription":
        """Create a 3-day trial subscription."""
        config  = TIER_PRICES.get(tier, {})
        now     = datetime.now(timezone.utc)
        trial_d = config.get("trial_days", 3)
 
        sub = cls(
            user_id      = user_id,
            tier         = tier,
            status       = SubscriptionStatus.TRIAL.value,
            is_trial     = True,
            trial_ends   = now + timedelta(days=trial_d),
            amount_kes   = config.get("amount_kes"),
            period_start = now,
        )
        db.session.add(sub)
        return sub
 
    def activate(self, payment_reference: str | None = None) -> None:
        now    = datetime.now(timezone.utc)
        config = TIER_PRICES.get(self.tier, {})
        days   = config.get("period_days", 30)
 
        self.status            = SubscriptionStatus.ACTIVE.value
        self.is_trial          = False
        self.period_start      = now
        self.period_end        = now + timedelta(days=days)
        self.last_billed_at    = now
        self.next_billing_at   = now + timedelta(days=days)
        self.payment_reference = payment_reference
        self._append_history("activated")
 
    def cancel(self, reason: str = "") -> None:
        self.status               = SubscriptionStatus.CANCELLED.value
        self.auto_renew           = False
        self.cancellation_reason  = reason
        self._append_history("cancelled")
 
    def _append_history(self, event: str) -> None:
        h = SubscriptionHistory(
            subscription_id = self.id,
            user_id         = self.user_id,
            event           = event,
            tier            = self.tier,
            status          = self.status,
            amount_kes      = self.amount_kes,
        )
        db.session.add(h)
 
    def to_dict(self) -> dict:
        return {
            "tier":           self.tier,
            "status":         self.status,
            "is_trial":       self.is_trial,
            "trial_ends":     self.trial_ends.isoformat() if self.trial_ends else None,
            "period_start":   self.period_start.isoformat() if self.period_start else None,
            "period_end":     self.period_end.isoformat() if self.period_end else None,
            "next_billing_at":self.next_billing_at.isoformat() if self.next_billing_at else None,
            "is_active":      self.is_currently_active,
            "limits":         TIER_LIMITS.get(self.tier, {}),
        }
    
class SubscriptionHistory(db.Model):
    __tablename__ = "subscription_history"
 
    id              = db.Column(db.Integer, primary_key=True)
    subscription_id = db.Column(db.Integer, db.ForeignKey("subscriptions.id", ondelete="CASCADE"),
                                nullable=False, index=True)
    user_id         = db.Column(db.Integer, db.ForeignKey("customers.id"), nullable=False, index=True)
    event           = db.Column(db.String(40), nullable=False)  # activated/cancelled/renewed/upgraded
    tier            = db.Column(db.String(20), nullable=False)
    status          = db.Column(db.String(20), nullable=False)
    amount_kes      = db.Column(db.Integer, nullable=True)
    recorded_at     = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
 
    subscription = db.relationship("Subscription", back_populates="history")