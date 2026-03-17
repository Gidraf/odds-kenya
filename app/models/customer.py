from app.utils.fetcher_utils import TIER_LIMITS, SubscriptionTier
from app.extensions import db
from datetime import datetime, timezone


class Customer(db.Model):
    __tablename__ = "customers"
 
    id            = db.Column(db.Integer, primary_key=True)
    email         = db.Column(db.String(180), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(256), nullable=False)
    phone         = db.Column(db.String(20),  nullable=True)
    display_name  = db.Column(db.String(120), nullable=True)
    is_active     = db.Column(db.Boolean, default=True, nullable=False)
    is_verified   = db.Column(db.Boolean, default=False, nullable=False)
    created_at    = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    last_login_at = db.Column(db.DateTime, nullable=True)
 
    # RSA public key for encrypted payloads (optional — premium devs can register it)
    public_key_pem = db.Column(db.Text, nullable=True)
 
    # Relationships
    subscription      = db.relationship("Subscription",       back_populates="user", uselist=False)
    api_keys          = db.relationship("ApiKey",             back_populates="user",  lazy="dynamic")
    bankroll_accounts = db.relationship("BankrollAccount",    back_populates="user",  lazy="dynamic")
    bankroll_targets  = db.relationship("BankrollTarget",     back_populates="user",  lazy="dynamic")
    notification_pref = db.relationship("NotificationPref",   back_populates="user",  uselist=False)
    metrics_events    = db.relationship("MetricsEvent",       back_populates="user",  lazy="dynamic")
 
    def set_password(self, password: str) -> None:
        from werkzeug.security import generate_password_hash
        self.password_hash = generate_password_hash(password, method="pbkdf2:sha256", salt_length=16)
 
    def check_password(self, password: str) -> bool:
        from werkzeug.security import check_password_hash
        return check_password_hash(self.password_hash, password)
 
    @property
    def tier(self) -> str:
        if self.subscription and self.subscription.is_currently_active:
            return self.subscription.tier
        return SubscriptionTier.FREE.value
 
    @property
    def limits(self) -> dict:
        return TIER_LIMITS[self.tier]
 
    def can(self, feature: str) -> bool:
        return bool(self.limits.get(feature, False))
 
    def to_dict(self) -> dict:
        return {
            "id":           self.id,
            "email":        self.email,
            "display_name": self.display_name,
            "tier":         self.tier,
            "is_verified":  self.is_verified,
            "created_at":   self.created_at.isoformat() if self.created_at else None,
        }
 