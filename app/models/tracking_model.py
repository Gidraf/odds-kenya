from app.extensions import db
from datetime import datetime, timezone

class UserActivityLog(db.Model):
    """
    Stores user analytics and engagement data for commercialization metrics.
    """
    __tablename__ = "user_activity_logs"

    id = db.Column(db.Integer, primary_key=True)
    ip_address = db.Column(db.String(50), index=True, nullable=False)
    user_id = db.Column(db.Integer, nullable=True)  # Nullable for anonymous users
    
    # What did they do? (e.g., 'view_live', 'click_arb', 'open_calculator')
    event_type = db.Column(db.String(50), index=True, nullable=False)
    
    # What did they do it on? (e.g., 'soccer', 'sr:match:12345')
    resource = db.Column(db.String(100), index=True, nullable=False)
    
    # Extra context (e.g., {"profit_pct": 2.5, "bookmakers": ["sp", "bt"]})
    meta_data = db.Column(db.JSON, nullable=True)
    
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), index=True)

    def to_dict(self):
        return {
            "id": self.id,
            "ip_address": self.ip_address,
            "event_type": self.event_type,
            "resource": self.resource,
            "meta_data": self.meta_data,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }