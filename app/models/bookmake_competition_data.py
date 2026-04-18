# ═════════════════════════════════════════════════════════════════════════════
# BOOKMAKER NAME MAPPING MODELS
# ═════════════════════════════════════════════════════════════════════════════

from datetime import datetime, timezone

from psycopg2 import IntegrityError
from app.extensions import db
from app.models.enums_tools import _utcnow_naive

class BookmakerTeamName(db.Model):
    __tablename__ = "bookmaker_team_names"
    id = db.Column(db.Integer, primary_key=True)
    bookmaker_key = db.Column(db.String(50), nullable=False, index=True)
    bookmaker_team_name = db.Column(db.String(200), nullable=False, index=True)
    team_id = db.Column(db.Integer, db.ForeignKey("teams.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
    __table_args__ = (db.UniqueConstraint("bookmaker_key", "bookmaker_team_name", name="uq_bm_team_name"),)
    team = db.relationship("Team", backref="bookmaker_names", lazy="joined")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "bookmaker_key": self.bookmaker_key,
            "bookmaker_team_name": self.bookmaker_team_name,
            "team_id": self.team_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    @classmethod
    def get_or_create(cls, bookmaker_key: str, bookmaker_team_name: str, team_id: int) -> "BookmakerTeamName":
        obj = cls.query.filter_by(
            bookmaker_key=bookmaker_key,
            bookmaker_team_name=bookmaker_team_name
        ).first()
        if not obj:
            try:
                with db.session.begin_nested():
                    obj = cls(
                        bookmaker_key=bookmaker_key,
                        bookmaker_team_name=bookmaker_team_name,
                        team_id=team_id
                    )
                    db.session.add(obj)
                    db.session.flush()
            except IntegrityError:
                obj = cls.query.filter_by(
                    bookmaker_key=bookmaker_key,
                    bookmaker_team_name=bookmaker_team_name
                ).first()
        return obj


class BookmakerCompetitionName(db.Model):
    __tablename__ = "bookmaker_competition_names"
    id = db.Column(db.Integer, primary_key=True)
    bookmaker_key = db.Column(db.String(50), nullable=False, index=True)
    bookmaker_competition_name = db.Column(db.String(200), nullable=False, index=True)
    competition_id = db.Column(db.Integer, db.ForeignKey("competitions.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
    __table_args__ = (db.UniqueConstraint("bookmaker_key", "bookmaker_competition_name", name="uq_bm_competition_name"),)
    competition = db.relationship("Competition", backref="bookmaker_names", lazy="joined")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "bookmaker_key": self.bookmaker_key,
            "bookmaker_competition_name": self.bookmaker_competition_name,
            "competition_id": self.competition_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    @classmethod
    def get_or_create(cls, bookmaker_key: str, bookmaker_competition_name: str, competition_id: int) -> "BookmakerCompetitionName":
        obj = cls.query.filter_by(
            bookmaker_key=bookmaker_key,
            bookmaker_competition_name=bookmaker_competition_name
        ).first()
        if not obj:
            try:
                with db.session.begin_nested():
                    obj = cls(
                        bookmaker_key=bookmaker_key,
                        bookmaker_competition_name=bookmaker_competition_name,
                        competition_id=competition_id
                    )
                    db.session.add(obj)
                    db.session.flush()
            except IntegrityError:
                obj = cls.query.filter_by(
                    bookmaker_key=bookmaker_key,
                    bookmaker_competition_name=bookmaker_competition_name
                ).first()
        return obj


class BookmakerCountryName(db.Model):
    __tablename__ = "bookmaker_country_names"
    id = db.Column(db.Integer, primary_key=True)
    bookmaker_key = db.Column(db.String(50), nullable=False, index=True)
    bookmaker_country_name = db.Column(db.String(100), nullable=False, index=True)
    country_id = db.Column(db.Integer, db.ForeignKey("countries.id", ondelete="CASCADE"), nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
    __table_args__ = (db.UniqueConstraint("bookmaker_key", "bookmaker_country_name", name="uq_bm_country_name"),)
    country = db.relationship("Country", backref="bookmaker_names", lazy="joined")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "bookmaker_key": self.bookmaker_key,
            "bookmaker_country_name": self.bookmaker_country_name,
            "country_id": self.country_id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    @classmethod
    def get_or_create(cls, bookmaker_key: str, bookmaker_country_name: str, country_id: int) -> "BookmakerCountryName":
        obj = cls.query.filter_by(
            bookmaker_key=bookmaker_key,
            bookmaker_country_name=bookmaker_country_name
        ).first()
        if not obj:
            try:
                with db.session.begin_nested():
                    obj = cls(
                        bookmaker_key=bookmaker_key,
                        bookmaker_country_name=bookmaker_country_name,
                        country_id=country_id
                    )
                    db.session.add(obj)
                    db.session.flush()
            except IntegrityError:
                obj = cls.query.filter_by(
                    bookmaker_key=bookmaker_key,
                    bookmaker_country_name=bookmaker_country_name
                ).first()
        return obj