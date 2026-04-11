"""app/workers/sp_sports/volleyball.py — Volleyball (sportId=23)"""
from app.workers.sp_harvesterbase import SportConfig

CONFIG = SportConfig(
    slugs        = ("volleyball",),
    sport_id     = 23,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "382,"   # Match Winner (2-way)
        "51,"    # Set Handicap
        "226,"   # Total Sets O/U
        "233,"   # Set Betting (3:0, 3:1, 3:2…)
        "45,"    # Odd/Even Points
        "353,"   # Home Team Points O/U
        "352"    # Away Team Points O/U
    ).replace("\n", "").replace(" ", ""),
)