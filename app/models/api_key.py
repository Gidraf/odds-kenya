from app.extensions import db
from datetime import datetime, timezone, timedelta
import secrets
import string

def _gen_api_key() -> str:
    alphabet = string.ascii_letters + string.digits
    return "ok_" + "".join(secrets.choice(alphabet) for _ in range(48))
 
 
class ApiKey(db.Model):
    __tablename__ = "api_keys"
 
    id          = db.Column(db.Integer, primary_key=True)
    user_id     = db.Column(db.Integer, db.ForeignKey("customers.id", ondelete="CASCADE"),
                            nullable=False, index=True)
    key         = db.Column(db.String(64), unique=True, nullable=False,
                            default=_gen_api_key, index=True)
    label       = db.Column(db.String(80), nullable=True)
    is_active   = db.Column(db.Boolean, default=True, nullable=False)
    last_used   = db.Column(db.DateTime, nullable=True)
    created_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
 
    # Rate limiting
    requests_today   = db.Column(db.Integer, default=0, nullable=False)
    requests_limit   = db.Column(db.Integer, default=10_000, nullable=False)  # per day
    requests_reset   = db.Column(db.DateTime, nullable=True)  # next reset time
 
    user = db.relationship("User", back_populates="api_keys")
 
    def check_rate_limit(self) -> tuple[bool, int]:
        """Returns (allowed, remaining)."""
        now = datetime.now(timezone.utc)
        if not self.requests_reset or now >= self.requests_reset.replace(tzinfo=timezone.utc):
            self.requests_today = 0
            self.requests_reset = now + timedelta(days=1)
        if self.requests_today >= self.requests_limit:
            return False, 0
        self.requests_today += 1
        self.last_used = now
        return True, self.requests_limit - self.requests_today
 
    def to_dict(self) -> dict:
        return {
            "id":               self.id,
            "key":              self.key,
            "label":            self.label,
            "is_active":        self.is_active,
            "requests_today":   self.requests_today,
            "requests_limit":   self.requests_limit,
            "last_used":        self.last_used.isoformat() if self.last_used else None,
            "created_at":       self.created_at.isoformat() if self.created_at else None,
        }