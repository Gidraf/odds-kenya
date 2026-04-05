from datetime import datetime
from sqlalchemy.exc import IntegrityError
from app.extensions import db
from app.models.enums_tools import Gender, PlayerPosition, _utcnow_naive

# ═════════════════════════════════════════════════════════════════════════════
# COUNTRY
# ═════════════════════════════════════════════════════════════════════════════

class Country(db.Model):
    __tablename__ = "countries"
 
    id        = db.Column(db.Integer, primary_key=True)
    name      = db.Column(db.String(100), unique=True, nullable=False, index=True)
    iso_code  = db.Column(db.String(3), unique=True, nullable=True)
    iso_code2 = db.Column(db.String(2), nullable=True)
    flag_url  = db.Column(db.String(255), nullable=True)
 
    # Cross-reference
    betradar_id = db.Column(db.String(32), unique=True, nullable=True, index=True)
 
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
 
    # Relationships
    competitions = db.relationship("Competition", back_populates="country", lazy="dynamic")
 
    def to_dict(self) -> dict:
        return {
            "id":          self.id,
            "name":        self.name,
            "iso_code":    self.iso_code,
            "iso_code2":   self.iso_code2,
            "flag_url":    self.flag_url,
            "betradar_id": self.betradar_id,
        }
 
    @classmethod
    def get_or_create(cls, name: str, **kw) -> "Country":
        obj = cls.query.filter(db.func.lower(cls.name) == name.lower()).first()
        if not obj:
            try:
                with db.session.begin_nested():
                    obj = cls(name=name, **kw)
                    db.session.add(obj)
                    db.session.flush()
            except IntegrityError:
                # Race condition: another worker created it. Fetch it instead.
                obj = cls.query.filter(db.func.lower(cls.name) == name.lower()).first()
        return obj
 
 
# ═════════════════════════════════════════════════════════════════════════════
# SPORT
# ═════════════════════════════════════════════════════════════════════════════
 
class Sport(db.Model):
    __tablename__ = "sports"
 
    id        = db.Column(db.Integer, primary_key=True)
    name      = db.Column(db.String(60), unique=True, nullable=False, index=True)
    slug      = db.Column(db.String(60), unique=True, nullable=False, index=True)
    emoji     = db.Column(db.String(10), nullable=True)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
 
    # SP sport_id for direct mapping
    sp_sport_id = db.Column(db.Integer, nullable=True, unique=True)
    betradar_id = db.Column(db.String(32), nullable=True, unique=True, index=True)
 
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
 
    # Relationships
    competitions = db.relationship("Competition", back_populates="sport", lazy="dynamic")
    teams        = db.relationship("Team",        back_populates="sport", lazy="dynamic")
 
    def to_dict(self) -> dict:
        return {
            "id":          self.id,
            "name":        self.name,
            "slug":        self.slug,
            "emoji":       self.emoji,
            "is_active":   self.is_active,
            "sp_sport_id": self.sp_sport_id,
            "betradar_id": self.betradar_id,
        }
 
    @classmethod
    def get_or_create(cls, name: str, slug: str | None = None, **kw) -> "Sport":
        import re
        obj = cls.query.filter(db.func.lower(cls.name) == name.lower()).first()
        if not obj and slug:
            obj = cls.query.filter(db.func.lower(cls.slug) == slug.lower()).first()
        if not obj:
            try:
                with db.session.begin_nested():
                    auto_slug = slug or re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
                    obj = cls(name=name, slug=auto_slug, **kw)
                    db.session.add(obj)
                    db.session.flush()
            except IntegrityError:
                # Race condition: another worker created it. Fetch it instead.
                obj = cls.query.filter(db.func.lower(cls.name) == name.lower()).first()
        return obj
 
 
# ═════════════════════════════════════════════════════════════════════════════
# COMPETITION
# ═════════════════════════════════════════════════════════════════════════════
 
class Competition(db.Model):
    __tablename__ = "competitions"
 
    id         = db.Column(db.Integer, primary_key=True)
    sport_id   = db.Column(db.Integer, db.ForeignKey("sports.id"),    nullable=False, index=True)
    country_id = db.Column(db.Integer, db.ForeignKey("countries.id"), nullable=True,  index=True)
    name       = db.Column(db.String(200), nullable=False, index=True)
    short_name = db.Column(db.String(60),  nullable=True)
    slug       = db.Column(db.String(200), nullable=True, index=True)
    gender     = db.Column(db.Enum(Gender, native_enum=False), default=Gender.MALE, nullable=False)
    logo_url   = db.Column(db.String(255), nullable=True)
    tier       = db.Column(db.SmallInteger, nullable=True,
                           comment="1=top flight, 2=second div, etc.")
    is_active  = db.Column(db.Boolean, default=True, nullable=False)
 
    # Cross-references
    betradar_id = db.Column(db.String(32), nullable=True, index=True)
    sp_league_id = db.Column(db.String(32), nullable=True, index=True)
 
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
    updated_at = db.Column(db.DateTime, default=_utcnow_naive, onupdate=_utcnow_naive)
 
    __table_args__ = (
        db.Index("ix_comp_sport_country", "sport_id", "country_id"),
        db.UniqueConstraint("sport_id", "name", "gender", name="uq_comp_sport_name_gender"),
    )
 
    # Relationships
    sport   = db.relationship("Sport",   back_populates="competitions")
    country = db.relationship("Country", back_populates="competitions")
    seasons = db.relationship("Season",  back_populates="competition", lazy="dynamic",
                              order_by="Season.year_start.desc()")
 
    def to_dict(self, include_country: bool = True) -> dict:
        d = {
            "id":           self.id,
            "name":         self.name,
            "short_name":   self.short_name,
            "slug":         self.slug,
            "sport_id":     self.sport_id,
            "country_id":   self.country_id,
            "gender":       self.gender.value if self.gender else None,
            "tier":         self.tier,
            "logo_url":     self.logo_url,
            "is_active":    self.is_active,
            "betradar_id":  self.betradar_id,
        }
        if include_country and self.country:
            d["country"] = self.country.to_dict()
        return d
 
    @classmethod
    def get_or_create(cls, name: str, sport_id: int, **kw) -> "Competition":
        import re
        obj = cls.query.filter(
            db.func.lower(cls.name) == name.lower(),
            cls.sport_id == sport_id,
        ).first()
        if not obj:
            try:
                with db.session.begin_nested():
                    auto_slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
                    obj = cls(name=name, sport_id=sport_id, slug=auto_slug, **kw)
                    db.session.add(obj)
                    db.session.flush()
            except IntegrityError:
                # Race condition: another worker created it. Fetch it instead.
                obj = cls.query.filter(
                    db.func.lower(cls.name) == name.lower(),
                    cls.sport_id == sport_id,
                ).first()
        return obj
 
 
# ═════════════════════════════════════════════════════════════════════════════
# SEASON
# ═════════════════════════════════════════════════════════════════════════════
 
class Season(db.Model):
    """
    Tracks Competition + year so historical data doesn't merge.
    e.g. "English Premier League 2025/26"
    """
    __tablename__ = "seasons"
 
    id             = db.Column(db.Integer, primary_key=True)
    competition_id = db.Column(db.Integer, db.ForeignKey("competitions.id"), nullable=False, index=True)
    name           = db.Column(db.String(60), nullable=False)
    year_start     = db.Column(db.SmallInteger, nullable=False)
    year_end       = db.Column(db.SmallInteger, nullable=True)
    is_current     = db.Column(db.Boolean, default=False, nullable=False, index=True)
    betradar_id    = db.Column(db.String(32), nullable=True, index=True)
 
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
 
    __table_args__ = (
        db.UniqueConstraint("competition_id", "year_start", name="uq_season_comp_year"),
    )
 
    competition = db.relationship("Competition", back_populates="seasons")
    standings   = db.relationship("SeasonStanding", back_populates="season",
                                  lazy="dynamic", cascade="all, delete-orphan")
 
    def to_dict(self) -> dict:
        return {
            "competition_id": self.competition_id,
            "name":           self.name,
            "year_start":     self.year_start,
            "year_end":       self.year_end,
            "is_current":     self.is_current,
        }
 
 
# ═════════════════════════════════════════════════════════════════════════════
# TEAM
# ═════════════════════════════════════════════════════════════════════════════
 
class Team(db.Model):
    __tablename__ = "teams"
 
    id         = db.Column(db.Integer, primary_key=True)
    sport_id   = db.Column(db.Integer, db.ForeignKey("sports.id"), nullable=False, index=True)
    country_id = db.Column(db.Integer, db.ForeignKey("countries.id"), nullable=True, index=True)
    name       = db.Column(db.String(150), nullable=False, index=True)
    short_name = db.Column(db.String(40),  nullable=True)
    slug       = db.Column(db.String(150), nullable=True, index=True)
    gender     = db.Column(db.Enum(Gender, native_enum=False), default=Gender.MALE, nullable=False)
    logo_url   = db.Column(db.String(255), nullable=True)
    founded    = db.Column(db.SmallInteger, nullable=True)
    venue_name = db.Column(db.String(150), nullable=True)
    is_active  = db.Column(db.Boolean, default=True, nullable=False)
 
    # Cross-references
    betradar_id = db.Column(db.String(32), nullable=True, index=True)
 
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
    updated_at = db.Column(db.DateTime, default=_utcnow_naive, onupdate=_utcnow_naive)
 
    __table_args__ = (
        db.Index("ix_team_sport_name", "sport_id", "name"),
    )
 
    # Relationships
    sport   = db.relationship("Sport",   back_populates="teams")
    country = db.relationship("Country", lazy="joined")
    players = db.relationship("TeamPlayer", back_populates="team", lazy="dynamic")
 
    def to_dict(self, include_country: bool = False) -> dict:
        d = {
            "id":          self.id,
            "name":        self.name,
            "short_name":  self.short_name,
            "slug":        self.slug,
            "sport_id":    self.sport_id,
            "country_id":  self.country_id,
            "gender":      self.gender.value if self.gender else None,
            "logo_url":    self.logo_url,
            "venue_name":  self.venue_name,
            "betradar_id": self.betradar_id,
            "is_active":   self.is_active,
        }
        if include_country and self.country:
            d["country"] = self.country.to_dict()
        return d
 
    @classmethod
    def get_or_create(cls, name: str, sport_id: int, **kw) -> "Team":
        import re
        obj = cls.query.filter(
            db.func.lower(cls.name) == name.lower(),
            cls.sport_id == sport_id,
        ).first()
        if not obj:
            try:
                with db.session.begin_nested():
                    auto_slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
                    obj = cls(name=name, sport_id=sport_id, slug=auto_slug, **kw)
                    db.session.add(obj)
                    db.session.flush()
            except IntegrityError:
                # Race condition: another worker created it. Fetch it instead.
                obj = cls.query.filter(
                    db.func.lower(cls.name) == name.lower(),
                    cls.sport_id == sport_id,
                ).first()
        return obj
 
 
# ═════════════════════════════════════════════════════════════════════════════
# PLAYER
# ═════════════════════════════════════════════════════════════════════════════
 
class Player(db.Model):
    __tablename__ = "players"
 
    id           = db.Column(db.Integer, primary_key=True)
    name         = db.Column(db.String(150), nullable=False, index=True)
    short_name   = db.Column(db.String(60),  nullable=True)
    nationality_id = db.Column(db.Integer, db.ForeignKey("countries.id"), nullable=True)
    date_of_birth = db.Column(db.Date, nullable=True)
    position     = db.Column(db.Enum(PlayerPosition, native_enum=False), nullable=True)
    jersey_number = db.Column(db.SmallInteger, nullable=True)
    photo_url    = db.Column(db.String(255), nullable=True)
 
    # Cross-references
    betradar_id = db.Column(db.String(32), nullable=True, index=True)
 
    is_active  = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
    updated_at = db.Column(db.DateTime, default=_utcnow_naive, onupdate=_utcnow_naive)
 
    nationality = db.relationship("Country", lazy="joined")
    team_links  = db.relationship("TeamPlayer", back_populates="player", lazy="dynamic")
 
    def to_dict(self) -> dict:
        return {
            "id":            self.id,
            "name":          self.name,
            "short_name":    self.short_name,
            "nationality":   self.nationality.to_dict() if self.nationality else None,
            "date_of_birth": self.date_of_birth.isoformat() if self.date_of_birth else None,
            "position":      self.position.value if self.position else None,
            "jersey_number": self.jersey_number,
            "photo_url":     self.photo_url,
            "betradar_id":   self.betradar_id,
        }
 
    @classmethod
    def get_or_create(cls, name: str, **kw) -> "Player":
        obj = cls.query.filter(db.func.lower(cls.name) == name.lower()).first()
        if not obj:
            try:
                with db.session.begin_nested():
                    obj = cls(name=name, **kw)
                    db.session.add(obj)
                    db.session.flush()
            except IntegrityError:
                # Race condition: another worker created it. Fetch it instead.
                obj = cls.query.filter(db.func.lower(cls.name) == name.lower()).first()
        return obj
 
 
# ═════════════════════════════════════════════════════════════════════════════
# TEAM ↔ PLAYER  (M2M with contract metadata)
# ═════════════════════════════════════════════════════════════════════════════
 
class TeamPlayer(db.Model):
    """Current and historical squad membership."""
    __tablename__ = "team_players"
 
    id        = db.Column(db.Integer, primary_key=True)
    team_id   = db.Column(db.Integer, db.ForeignKey("teams.id",   ondelete="CASCADE"), nullable=False, index=True)
    player_id = db.Column(db.Integer, db.ForeignKey("players.id", ondelete="CASCADE"), nullable=False, index=True)
    season_id = db.Column(db.Integer, db.ForeignKey("seasons.id"), nullable=True)
 
    jersey_number = db.Column(db.SmallInteger, nullable=True)
    position      = db.Column(db.Enum(PlayerPosition, native_enum=False), nullable=True)
    is_active     = db.Column(db.Boolean, default=True, nullable=False)
    joined_at     = db.Column(db.Date, nullable=True)
    left_at       = db.Column(db.Date, nullable=True)
 
    __table_args__ = (
        db.Index("ix_tp_team_active", "team_id", "is_active"),
        db.UniqueConstraint("team_id", "player_id", "season_id", name="uq_tp_team_player_season"),
    )
 
    team   = db.relationship("Team",   back_populates="players")
    player = db.relationship("Player", back_populates="team_links")
 
    def to_dict(self) -> dict:
        return {
            "id":            self.id,
            "team_id":       self.team_id,
            "player_id":     self.player_id,
            "jersey_number": self.jersey_number,
            "position":      self.position.value if self.position else None,
            "is_active":     self.is_active,
        }