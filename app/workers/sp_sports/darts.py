"""app/workers/sp_sports/darts.py — Darts (sportId=49)"""
from app.workers.sp_harvester_base import SportConfig

CONFIG = SportConfig(
    slugs        = ("darts",),
    sport_id     = 49,
    days_default = 3,
    max_default  = 100,
    market_ids   = (
        "382,"   # Match Winner (2-way)
        "226,"   # Total Legs O/U
        "45,"    # Odd/Even Legs
        "51"     # Leg Handicap
    ).replace("\n", "").replace(" ", ""),
)