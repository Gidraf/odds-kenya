from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Index, text
from sqlalchemy.sql import func
from app.extensions import db


class MatchEvArb(db.Model):
    __tablename__ = "match_ev_arb"

    match_id = db.Column(db.BigInteger, primary_key=True)

    has_arb = db.Column(
        db.Boolean,
        nullable=True,
        default=False,
        server_default=text("false"),
    )

    best_arb_pct = db.Column(
        db.Numeric(8, 4),
        nullable=True,
        default=0,
        server_default=text("0"),
    )

    has_ev = db.Column(
        db.Boolean,
        nullable=True,
        default=False,
        server_default=text("false"),
    )

    best_ev_pct = db.Column(
        db.Numeric(8, 4),
        nullable=False,
        default=0,
        server_default=text("0"),
    )

    arb_count = db.Column(
        db.Integer,
        nullable=True,
        default=0,
        server_default=text("0"),
    )

    ev_count = db.Column(
        db.Integer,
        nullable=True,
        default=0,
        server_default=text("0"),
    )

    computed_at = db.Column(
        db.DateTime(timezone=True),
        nullable=True,
        server_default=func.now(),
    )

    created_at = db.Column(
        db.DateTime(timezone=True),
        nullable=True,
        server_default=func.now(),
    )

    __table_args__ = (
        # Partial index: has_arb = true
        Index(
            "idx_ev_arb_has_arb",
            "has_arb",
            postgresql_where=text("has_arb = true"),
        ),

        # Partial index: best_arb_pct DESC where has_arb = true
        Index(
            "idx_ev_arb_best_arb",
            db.desc("best_arb_pct"),
            postgresql_where=text("has_arb = true"),
        ),

        {
            "comment": "EV/arbitrage computation results per match, updated after each harvest cycle."
        },
    )