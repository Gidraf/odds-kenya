from app.extensions import db
from datetime import datetime, timezone

class NotificationPref(db.Model):
    __tablename__ = "notification_prefs"
 
    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("customers.id", ondelete="CASCADE"),
                           nullable=False, unique=True)
 
    # Channels
    email_enabled = db.Column(db.Boolean, default=True, nullable=False)
    sms_enabled   = db.Column(db.Boolean, default=False, nullable=False)
    push_enabled  = db.Column(db.Boolean, default=False, nullable=False)
    push_token    = db.Column(db.String(256), nullable=True)
 
    # Thresholds
    arb_min_profit    = db.Column(db.Float, default=1.0)   # notify if arb > 1%
    ev_min_edge       = db.Column(db.Float, default=3.0)   # notify if EV edge > 3%
    steam_move        = db.Column(db.Boolean, default=True) # notify on sharp money
    odds_change_pct   = db.Column(db.Float, default=5.0)   # notify on >5% odds move
 
    # Sports filter  (null = all sports)
    sports_filter = db.Column(db.JSON, nullable=True)
 
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                           onupdate=lambda: datetime.now(timezone.utc))
 
    user = db.relationship("User", back_populates="notification_pref")
 
    def to_dict(self) -> dict:
        return {
            "email_enabled":    self.email_enabled,
            "sms_enabled":      self.sms_enabled,
            "arb_min_profit":   self.arb_min_profit,
            "ev_min_edge":      self.ev_min_edge,
            "steam_move":       self.steam_move,
            "odds_change_pct":  self.odds_change_pct,
            "sports_filter":    self.sports_filter,
        }