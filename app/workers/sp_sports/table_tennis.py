"""app/workers/sp_sports/table_tennis.py — Table Tennis (sportId=16)"""
from app.workers.sp_harvesterbase import SportConfig

CONFIG = SportConfig(
    slugs        = ("table-tennis", "tabletennis"),
    sport_id     = 16,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "382,"   # Match Winner (2-way)
        "51,"    # Game Handicap
        "226,"   # Total Games O/U
        "45,"    # Odd/Even Points
        "233,"   # Set Betting
        "340"    # 1st Set Total Games O/U
    ).replace("\n", "").replace(" ", ""),
)