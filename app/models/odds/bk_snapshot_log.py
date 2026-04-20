from app.extensions import db

class BkSnapshotLog(db.Model):
    __tablename__ = "bk_snapshot_log"

    id = db.Column(db.BigInteger, primary_key=True)
    bk_slug = db.Column(db.String(16), nullable=False)
    sport_slug = db.Column(db.String(32), nullable=False)
    mode = db.Column(db.String(16), nullable=False, default='upcoming', server_default='upcoming')
    match_count = db.Column(db.Integer, nullable=False, default=0, server_default='0')
    harvested_at = db.Column(db.DateTime(timezone=True), nullable=False, server_default=db.func.now())

    __table_args__ = (
        db.Index(
            "idx_bk_snapshot_log_bk_sport", 
            "bk_slug", 
            "sport_slug", 
            db.text("harvested_at DESC")
        ),
    )