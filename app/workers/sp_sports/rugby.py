"""app/workers/sp_sports/rugby.py — Rugby Union / League (sportId=12)"""
from app.workers.sp_harvester_base import SportConfig

CONFIG = SportConfig(
    slugs        = ("rugby", "rugby-league", "rugby-union"),
    sport_id     = 12,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "382,"   # Match Winner (2-way)
        "1,10,"  # 1X2
        "46,"    # Double Chance
        "51,"    # Asian Handicap (spread)
        "52,"    # Total Points O/U
        "45,"    # Odd/Even Points
        "353,"   # Home Team Points O/U
        "352"    # Away Team Points O/U
    ).replace("\n", "").replace(" ", ""),
)