"""app/workers/sp_sports/cricket.py — Cricket (sportId=21)"""
from app.workers.sp_harvester_base import SportConfig

CONFIG = SportConfig(
    slugs        = ("cricket",),
    sport_id     = 21,
    days_default = 5,   # cricket matches run over multiple days
    max_default  = 50,
    market_ids   = (
        "382,"   # Match Winner (2-way)
        "1,"     # 1X2 (inc. draw for Tests)
        "51,"    # Asian Handicap (run handicap)
        "52,"    # Total Runs O/U
        "353,"   # Home Team Runs O/U
        "352"    # Away Team Runs O/U
    ).replace("\n", "").replace(" ", ""),
)