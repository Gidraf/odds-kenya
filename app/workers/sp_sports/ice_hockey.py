"""app/workers/sp_sports/ice_hockey.py — Ice Hockey (sportId=4)"""
from app.workers.sp_harvesterbase import SportConfig

CONFIG = SportConfig(
    slugs        = ("ice-hockey", "icehockey"),
    sport_id     = 4,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "1,10,"  # 1X2 (3-way incl. OT result)
        "382,"   # Match Winner (2-way OT / SO incl.)
        "52,"    # Total Goals O/U
        "51,"    # Puck Line (handicap)
        "45,"    # Odd/Even Goals
        "46,"    # Double Chance
        "353,"   # Home Team Goals O/U
        "352,"   # Away Team Goals O/U
        "208,"   # Result + O/U
        "43"     # Both Teams to Score
    ).replace("\n", "").replace(" ", ""),
)