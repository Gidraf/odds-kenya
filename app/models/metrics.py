from app.extensions import db
from datetime import datetime, timezone

class MetricsEvent(db.Model):
    """
    Raw event log for analytics dashboard.
    Events: signup, login, subscribe, cancel, download, api_call, page_view,
            calculator_use, arb_alert_sent, ev_alert_sent
    """
    __tablename__ = "metrics_events"
 
    id          = db.Column(db.Integer, primary_key=True)
    customer_id     = db.Column(db.Integer, db.ForeignKey("customers.id"), nullable=True, index=True)
    event       = db.Column(db.String(60), nullable=False, index=True)
    tier        = db.Column(db.String(20), nullable=True)
    meta        = db.Column(db.JSON, nullable=True)   # extra context
    ip_address  = db.Column(db.String(48), nullable=True)
    recorded_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc),
                            nullable=False, index=True)
 
    __table_args__ = (
        db.Index("ix_me_event_time", "event", "recorded_at"),
    )
 
    customer = db.relationship("Customer", back_populates="metrics_events")
 
    @classmethod
    def log(cls, event: str, user_id: int | None = None, tier: str | None = None,
            meta: dict | None = None, ip: str | None = None) -> "MetricsEvent":
        e = cls(event=event, user_id=user_id, tier=tier, meta=meta, ip_address=ip)
        db.session.add(e)
        return e
 
    def to_dict(self) -> dict:
        return {
            "id":          self.id,
            "event":       self.event,
            "tier":        self.tier,
            "meta":        self.meta,
            "recorded_at": self.recorded_at.isoformat(),
        }