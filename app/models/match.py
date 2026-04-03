from app.models.enums_tools import LineupType, MatchEventType, MatchPeriod, PlayerPosition, _utcnow_naive
from app.extensions import db


class MatchEvent(db.Model):
    """
    Individual events during a match.
 
    Coordinates (x, y) represent pitch position:
      x: 0.0 (own goal line) → 100.0 (opponent goal line)
      y: 0.0 (left touchline) → 100.0 (right touchline)
    Used for goal animations, shot maps, heat maps.
    """
    __tablename__ = "match_events"
 
    id       = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(
        db.Integer, db.ForeignKey("unified_matches.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
 
    event_type = db.Column(db.Enum(MatchEventType, native_enum=False), nullable=False, index=True)
    period     = db.Column(db.Enum(MatchPeriod, native_enum=False), nullable=True)
    minute     = db.Column(db.SmallInteger, nullable=True)
    added_time = db.Column(db.SmallInteger, nullable=True, comment="e.g. 45+2 → minute=45, added_time=2")
 
    # Which team — "home" or "away"
    team_side  = db.Column(db.String(5), nullable=True, comment="home | away")
    team_id    = db.Column(db.Integer, db.ForeignKey("teams.id"), nullable=True)
 
    # Players involved
    player_id      = db.Column(db.Integer, db.ForeignKey("players.id"), nullable=True)
    player_name    = db.Column(db.String(150), nullable=True, comment="Denorm — always set even without FK")
    assist_player_id   = db.Column(db.Integer, db.ForeignKey("players.id"), nullable=True)
    assist_player_name = db.Column(db.String(150), nullable=True)
 
    # For substitutions
    player_in_id   = db.Column(db.Integer, db.ForeignKey("players.id"), nullable=True)
    player_out_id  = db.Column(db.Integer, db.ForeignKey("players.id"), nullable=True)
 
    # Pitch coordinates (0-100 scale, nullable if not available)
    coord_x = db.Column(db.Float, nullable=True, comment="0=own goal line, 100=opponent goal line")
    coord_y = db.Column(db.Float, nullable=True, comment="0=left touchline, 100=right touchline")
 
    # Score after this event (for goals)
    score_home = db.Column(db.SmallInteger, nullable=True)
    score_away = db.Column(db.SmallInteger, nullable=True)
 
    # Extra data (e.g. VAR details, card reason)
    detail = db.Column(db.String(200), nullable=True)
    extra_json = db.Column(db.JSON, nullable=True)
 
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
 
    __table_args__ = (
        db.Index("ix_me_match_minute", "match_id", "minute"),
        db.Index("ix_me_match_type",   "match_id", "event_type"),
        db.Index("ix_me_player",       "player_id", "event_type"),
    )
 
    def to_dict(self) -> dict:
        return {
            "id":           self.id,
            "match_id":     self.match_id,
            "event_type":   self.event_type.value,
            "period":       self.period.value if self.period else None,
            "minute":       self.minute,
            "added_time":   self.added_time,
            "team_side":    self.team_side,
            "team_id":      self.team_id,
            "player_id":    self.player_id,
            "player_name":  self.player_name,
            "assist_player_name": self.assist_player_name,
            "coord_x":      self.coord_x,
            "coord_y":      self.coord_y,
            "score_home":   self.score_home,
            "score_away":   self.score_away,
            "detail":       self.detail,
        }
 
 
# ═════════════════════════════════════════════════════════════════════════════
# MATCH STATS  (per-team aggregates: possession, shots, corners…)
# ═════════════════════════════════════════════════════════════════════════════
 
class MatchStats(db.Model):
    """
    Per-team stats for a match. One row per team per match.
    """
    __tablename__ = "match_stats"
 
    id       = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(
        db.Integer, db.ForeignKey("unified_matches.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    team_id   = db.Column(db.Integer, db.ForeignKey("teams.id"), nullable=True)
    team_side = db.Column(db.String(5), nullable=False, comment="home | away")
 
    # Core stats
    possession_pct    = db.Column(db.Float, nullable=True)
    total_shots       = db.Column(db.SmallInteger, nullable=True)
    shots_on_target   = db.Column(db.SmallInteger, nullable=True)
    shots_off_target  = db.Column(db.SmallInteger, nullable=True)
    blocked_shots     = db.Column(db.SmallInteger, nullable=True)
    corners           = db.Column(db.SmallInteger, nullable=True)
    fouls             = db.Column(db.SmallInteger, nullable=True)
    offsides          = db.Column(db.SmallInteger, nullable=True)
    yellow_cards      = db.Column(db.SmallInteger, nullable=True)
    red_cards         = db.Column(db.SmallInteger, nullable=True)
    free_kicks        = db.Column(db.SmallInteger, nullable=True)
    throw_ins         = db.Column(db.SmallInteger, nullable=True)
    goal_kicks        = db.Column(db.SmallInteger, nullable=True)
    saves             = db.Column(db.SmallInteger, nullable=True)
    passes_total      = db.Column(db.SmallInteger, nullable=True)
    passes_accurate   = db.Column(db.SmallInteger, nullable=True)
    tackles           = db.Column(db.SmallInteger, nullable=True)
    interceptions     = db.Column(db.SmallInteger, nullable=True)
    clearances        = db.Column(db.SmallInteger, nullable=True)
 
    # Expected goals (if available from data provider)
    xg = db.Column(db.Float, nullable=True, comment="Expected goals")
 
    # Overflow for any stat not in columns above
    extra_json = db.Column(db.JSON, nullable=True)
 
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
 
    __table_args__ = (
        db.UniqueConstraint("match_id", "team_side", name="uq_ms_match_side"),
    )
 
    def to_dict(self) -> dict:
        return {
            "match_id":        self.match_id,
            "team_id":         self.team_id,
            "team_side":       self.team_side,
            "possession_pct":  self.possession_pct,
            "total_shots":     self.total_shots,
            "shots_on_target": self.shots_on_target,
            "corners":         self.corners,
            "fouls":           self.fouls,
            "yellow_cards":    self.yellow_cards,
            "red_cards":       self.red_cards,
            "offsides":        self.offsides,
            "saves":           self.saves,
            "xg":              self.xg,
            "passes_total":    self.passes_total,
            "passes_accurate": self.passes_accurate,
        }
 
 
# ═════════════════════════════════════════════════════════════════════════════
# MATCH LINEUP  (starting XI + subs)
# ═════════════════════════════════════════════════════════════════════════════
 
class MatchLineup(db.Model):
    __tablename__ = "match_lineups"
 
    id       = db.Column(db.Integer, primary_key=True)
    match_id = db.Column(
        db.Integer, db.ForeignKey("unified_matches.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    team_id     = db.Column(db.Integer, db.ForeignKey("teams.id"), nullable=True)
    team_side   = db.Column(db.String(5), nullable=False, comment="home | away")
    player_id   = db.Column(db.Integer, db.ForeignKey("players.id"), nullable=True)
    player_name = db.Column(db.String(150), nullable=False)
 
    lineup_type   = db.Column(db.Enum(LineupType, native_enum=False), nullable=False)
    position      = db.Column(db.Enum(PlayerPosition, native_enum=False), nullable=True)
    jersey_number = db.Column(db.SmallInteger, nullable=True)
    formation_pos = db.Column(db.String(10), nullable=True,
                               comment="Position in formation grid, e.g. '2,3' for row,col")
 
    # Player match performance (populated post-match)
    rating     = db.Column(db.Float, nullable=True, comment="Player rating 0-10")
    minutes    = db.Column(db.SmallInteger, nullable=True)
    goals      = db.Column(db.SmallInteger, default=0)
    assists    = db.Column(db.SmallInteger, default=0)
    shots      = db.Column(db.SmallInteger, nullable=True)
    passes     = db.Column(db.SmallInteger, nullable=True)
    tackles    = db.Column(db.SmallInteger, nullable=True)
    saves      = db.Column(db.SmallInteger, nullable=True)
 
    created_at = db.Column(db.DateTime, default=_utcnow_naive)
 
    __table_args__ = (
        db.Index("ix_ml_match_side", "match_id", "team_side"),
        db.Index("ix_ml_player",     "player_id"),
    )
 
    def to_dict(self) -> dict:
        return {
            "match_id":      self.match_id,
            "team_side":     self.team_side,
            "player_id":     self.player_id,
            "player_name":   self.player_name,
            "lineup_type":   self.lineup_type.value,
            "position":      self.position.value if self.position else None,
            "jersey_number": self.jersey_number,
            "rating":        self.rating,
            "minutes":       self.minutes,
            "goals":         self.goals,
            "assists":       self.assists,
        }
 
 
# ═════════════════════════════════════════════════════════════════════════════
# SEASON STANDINGS
# ═════════════════════════════════════════════════════════════════════════════
 
class SeasonStanding(db.Model):
    """League table row — one per team per season."""
    __tablename__ = "season_standings"
 
    id        = db.Column(db.Integer, primary_key=True)
    season_id = db.Column(db.Integer, db.ForeignKey("seasons.id", ondelete="CASCADE"), nullable=False, index=True)
    team_id   = db.Column(db.Integer, db.ForeignKey("teams.id"),  nullable=False, index=True)
 
    position       = db.Column(db.SmallInteger, nullable=False)
    played         = db.Column(db.SmallInteger, default=0)
    won            = db.Column(db.SmallInteger, default=0)
    drawn          = db.Column(db.SmallInteger, default=0)
    lost           = db.Column(db.SmallInteger, default=0)
    goals_for      = db.Column(db.SmallInteger, default=0)
    goals_against  = db.Column(db.SmallInteger, default=0)
    goal_difference = db.Column(db.SmallInteger, default=0)
    points         = db.Column(db.SmallInteger, default=0)
 
    # Form string: "WDLWW" (last 5)
    form = db.Column(db.String(20), nullable=True)
 
    updated_at = db.Column(db.DateTime, default=_utcnow_naive, onupdate=_utcnow_naive)
 
    __table_args__ = (
        db.UniqueConstraint("season_id", "team_id", name="uq_standing_season_team"),
    )
 
    season = db.relationship("Season", back_populates="standings")
    team   = db.relationship("Team",   lazy="joined")
 
    def to_dict(self) -> dict:
        return {
            "position":        self.position,
            "team":            self.team.to_dict() if self.team else None,
            "played":          self.played,
            "won":             self.won,
            "drawn":           self.drawn,
            "lost":            self.lost,
            "goals_for":       self.goals_for,
            "goals_against":   self.goals_against,
            "goal_difference": self.goal_difference,
            "points":          self.points,
            "form":            self.form,
        }