import datetime
import enum


class Gender(str, enum.Enum):
    MALE   = "M"
    FEMALE = "F"
    MIXED  = "MIXED"
 
 
class MatchEventType(str, enum.Enum):
    GOAL            = "GOAL"
    OWN_GOAL        = "OWN_GOAL"
    PENALTY_GOAL    = "PENALTY_GOAL"
    PENALTY_MISS    = "PENALTY_MISS"
    YELLOW_CARD     = "YELLOW_CARD"
    RED_CARD        = "RED_CARD"
    SECOND_YELLOW   = "SECOND_YELLOW"
    SUBSTITUTION    = "SUBSTITUTION"
    VAR_DECISION    = "VAR_DECISION"
    CORNER          = "CORNER"
    FREE_KICK       = "FREE_KICK"
    OFFSIDE         = "OFFSIDE"
    SHOT_ON_TARGET  = "SHOT_ON_TARGET"
    SHOT_OFF_TARGET = "SHOT_OFF_TARGET"
    KICKOFF         = "KICKOFF"
    HALF_TIME       = "HALF_TIME"
    FULL_TIME       = "FULL_TIME"
    EXTRA_TIME      = "EXTRA_TIME"
    PENALTY_SHOOTOUT = "PENALTY_SHOOTOUT"
 
 
class MatchPeriod(str, enum.Enum):
    FIRST_HALF  = "1H"
    SECOND_HALF = "2H"
    EXTRA_FIRST = "ET1"
    EXTRA_SECOND = "ET2"
    PENALTIES   = "PEN"
    PRE_MATCH   = "PRE"
    FULL_TIME   = "FT"
 
 
class PlayerPosition(str, enum.Enum):
    GOALKEEPER = "GK"
    DEFENDER   = "DEF"
    MIDFIELDER = "MID"
    FORWARD    = "FWD"
    UNKNOWN    = "UNK"
 
 
class LineupType(str, enum.Enum):
    STARTING = "STARTING"
    SUBSTITUTE = "SUBSTITUTE"
    COACH = "COACH"

def _utcnow():
    return datetime.now(datetime.timezone.utc)
 
 
def _utcnow_naive():
    return  datetime.now(datetime.timezone.utc)