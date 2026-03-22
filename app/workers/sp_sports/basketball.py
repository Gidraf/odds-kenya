"""app/workers/sp_sports/basketball.py — Basketball (sportId=2)"""
from app.workers.sp_harvester_base import SportConfig

CONFIG = SportConfig(
    slugs        = ("basketball",),
    sport_id     = 2,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "382,"   # Match Winner (2-way, OT incl.)
        "51,"    # Point Spread
        "52,"    # Total Points O/U
        "353,"   # Home Team Total Points O/U
        "352,"   # Away Team Total Points O/U
        "45,"    # Odd/Even Points
        "222,"   # Winning Margin (H15, H610, H_10…)
        "99,"    # Q1 Total Points
        "100"    # Alt Point Spread
    ).replace("\n", "").replace(" ", ""),
)