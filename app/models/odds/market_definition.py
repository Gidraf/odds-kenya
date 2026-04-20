from app.extensions import db
from .common import _utcnow_naive

class MarketDefinition(db.Model):
    __tablename__ = "market_definitions"

    id             = db.Column(db.Integer, primary_key=True)
    name           = db.Column(db.String(120), unique=True, nullable=False, index=True)
    display_name   = db.Column(db.String(120), nullable=True)
    is_player_prop = db.Column(db.Boolean, default=False, nullable=False)
    created_at     = db.Column(db.DateTime, default=_utcnow_naive)

    @classmethod
    def get_or_create(cls, name: str) -> "MarketDefinition":
        obj = cls.query.filter_by(name=name).first()
        if not obj:
            obj = cls(name=name, display_name=name)
            db.session.add(obj)
            db.session.flush()
        return obj

    def to_dict(self) -> dict:
        return {
            "id":             self.id,
            "name":           self.name,
            "display_name":   self.display_name or self.name,
            "is_player_prop": self.is_player_prop,
        }