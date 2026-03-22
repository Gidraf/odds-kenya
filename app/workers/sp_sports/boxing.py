"""app/workers/sp_sports/boxing.py — Boxing (sportId=10)"""
from app.workers.sp_harvester_base import SportConfig

CONFIG = SportConfig(
    slugs        = ("boxing",),
    sport_id     = 10,
    days_default = 7,   # fight cards are weekly / bi-weekly
    max_default  = 30,
    market_ids   = (
        "382,"   # Match Winner (Fight Winner)
        "51,"    # Round Betting (which round does the fight end)
        "52"     # Total Rounds O/U
    ).replace("\n", "").replace(" ", ""),
)