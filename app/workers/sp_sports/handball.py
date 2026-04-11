"""app/workers/sp_sports/handball.py — Handball (sportId=6)"""
from app.workers.sp_harvesterbase import SportConfig

CONFIG = SportConfig(
    slugs        = ("handball",),
    sport_id     = 6,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "1,10,"  # 1X2
        "382,"   # Match Winner (if SP offers 2-way)
        "52,"    # Total Goals O/U
        "51,"    # Asian Handicap
        "45,"    # Odd/Even Goals
        "46,"    # Double Chance
        "47,"    # Draw No Bet
        "353,"   # Home Team Goals O/U
        "352,"   # Away Team Goals O/U
        "208,"   # Result + O/U
        "43"     # BTTS
    ).replace("\n", "").replace(" ", ""),
)