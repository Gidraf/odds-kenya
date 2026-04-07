"""
app/models/live_snapshot_model.py
===================================
Raw live market snapshots captured during enrichment cycles.
Used for future analytics, drift analysis, and bookmaker comparison.

Migration (add to Alembic or run manually):
  flask db migrate -m "live_raw_snapshots"
  flask db upgrade
"""

from __future__ import annotations

from datetime import datetime

from app.extensions import db


class LiveRawSnapshot(db.Model):
    """
    One row per bookmaker per enrichment trigger per match.

    trigger values:
      "sp_ws_r0"      — immediate SP WebSocket trigger
      "sp_ws_r1"      — first retry after WS trigger
      "sp_ws_r2..5"   — subsequent retries
      "cross_bk_r0"   — BT or OD fetched at trigger-0
      "cross_bk_r1..5" — BT or OD retries
      "poll_default"  — SP live poller default-markets poll
      "poll_detail"   — SP live poller full-detail poll
    """
    __tablename__ = "live_raw_snapshots"

    id          = db.Column(db.BigInteger, primary_key=True)
    match_id    = db.Column(
        db.Integer,
        db.ForeignKey("unified_matches.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    betradar_id  = db.Column(db.String(40),  nullable=False, index=True)
    bk_slug      = db.Column(db.String(8),   nullable=False)   # sp | bt | od | b2b | sbo
    sport_slug   = db.Column(db.String(32),  nullable=True)
    trigger      = db.Column(db.String(24),  nullable=False)   # see docstring
    markets_json = db.Column(db.JSON,        nullable=True)    # {slug: {outcome: price}}
    event_json   = db.Column(db.JSON,        nullable=True)    # score/phase data
    recorded_at  = db.Column(db.DateTime,   nullable=False,
                              default=datetime.utcnow, index=True)

    __table_args__ = (
        db.Index("ix_lrs_br_bk_ts",    "betradar_id", "bk_slug", "recorded_at"),
        db.Index("ix_lrs_match_ts",     "match_id",    "recorded_at"),
        db.Index("ix_lrs_trigger",      "trigger",     "recorded_at"),
    )

    def to_dict(self) -> dict:
        return {
            "id":           self.id,
            "match_id":     self.match_id,
            "betradar_id":  self.betradar_id,
            "bk_slug":      self.bk_slug,
            "sport_slug":   self.sport_slug,
            "trigger":      self.trigger,
            "markets":      self.markets_json,
            "event":        self.event_json,
            "recorded_at":  self.recorded_at.isoformat() if self.recorded_at else None,
        }

    @classmethod
    def recent(
        cls,
        betradar_id: str,
        bk_slug: str | None = None,
        limit: int = 50,
    ) -> list["LiveRawSnapshot"]:
        q = cls.query.filter_by(betradar_id=betradar_id)
        if bk_slug:
            q = q.filter_by(bk_slug=bk_slug)
        return q.order_by(cls.recorded_at.desc()).limit(limit).all()