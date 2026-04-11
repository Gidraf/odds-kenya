"""app/workers/sp_sports/american_football.py — American Football / NFL (sportId=15)"""
from app.workers.sp_harvesterbase import SportConfig

CONFIG = SportConfig(
    slugs        = ("american-football", "americanfootball", "nfl"),
    sport_id     = 15,
    days_default = 7,
    max_default  = 50,
    market_ids   = (
        "382,"   # Match Winner / Moneyline (2-way)
        "51,"    # Point Spread
        "52,"    # Total Points O/U
        "45,"    # Odd/Even Points
        "353,"   # Home Team Points O/U
        "352"    # Away Team Points O/U
    ).replace("\n", "").replace(" ", ""),
)