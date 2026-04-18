from app.extensions import db
from datetime import datetime

class MatchAnalytics(db.Model):
    """Scraped Sportradar analytics for a match (stats, H2H, standings)."""
    __tablename__ = "match_analytics"

    id = db.Column(db.Integer, primary_key=True)
    unified_match_id = db.Column(db.Integer, db.ForeignKey("unified_matches.id"), nullable=False, index=True)
    sp_match_id = db.Column(db.String(64), nullable=True, index=True, comment="SportPesa game ID")
    betradar_id = db.Column(db.String(64), nullable=True, index=True)

    # JSON blobs for different sections
    match_info = db.Column(db.JSON, nullable=True)       # venue, referee, attendance
    statistics = db.Column(db.JSON, nullable=True)       # possession, shots, corners, etc.
    head_to_head = db.Column(db.JSON, nullable=True)     # list of past meetings
    standings = db.Column(db.JSON, nullable=True)        # league table (home/away)
    team_form = db.Column(db.JSON, nullable=True)        # recent results

    scraped_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    match = db.relationship("UnifiedMatch", backref=db.backref("analytics", uselist=False))

    def to_dict(self):
        return {
            "unified_match_id": self.unified_match_id,
            "sp_match_id": self.sp_match_id,
            "betradar_id": self.betradar_id,
            "match_info": self.match_info,
            "statistics": self.statistics,
            "head_to_head": self.head_to_head,
            "standings": self.standings,
            "team_form": self.team_form,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
        }