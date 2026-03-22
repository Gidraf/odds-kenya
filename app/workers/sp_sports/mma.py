"""app/workers/sp_sports/mma.py — MMA / UFC (sportId=117)"""
from app.workers.sp_harvester_base import SportConfig

CONFIG = SportConfig(
    slugs        = ("mma", "ufc"),
    sport_id     = 117,
    days_default = 7,
    max_default  = 30,
    market_ids   = (
        "382,"   # Match Winner (Fight Winner)
        "51,"    # Round Betting
        "52"     # Total Rounds O/U
    ).replace("\n", "").replace(" ", ""),
)