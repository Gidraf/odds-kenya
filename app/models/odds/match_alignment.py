from app.extensions import db
from sqlalchemy.dialects.postgresql import JSONB

class MatchAlignment(db.Model):
    __tablename__ = "match_alignments"

    id = db.Column(db.BigInteger, primary_key=True)
    sport_slug = db.Column(db.String(32), nullable=False)
    group_id = db.Column(db.String(64), nullable=False, comment="betradar_id or synthetic group key")
    
    bk_refs = db.Column(JSONB, nullable=False, comment='["sp:123456", "bt:789012"]')
    
    aligned_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())

    __table_args__ = (
        db.UniqueConstraint("sport_slug", "group_id", name="uq_match_alignments_sport_group"),
        db.Index("idx_match_alignments_sport", "sport_slug"),
        db.Index("idx_match_alignments_bk_refs", "bk_refs", postgresql_using="gin"),
    )