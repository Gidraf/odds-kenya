"""app/workers/sp_sports/tennis.py — Tennis (sportId=5)"""
from app.workers.sp_harvester_base import SportConfig

CONFIG = SportConfig(
    slugs        = ("tennis",),
    sport_id     = 5,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "382,"   # Match Winner (2-way)
        "204,"   # First Set Winner
        "231,"   # Second Set Winner
        "51,"    # Game Handicap (full match)
        "226,"   # Total Games O/U
        "233,"   # Set Betting (correct score in sets: 2:0, 2:1…)
        "439,"   # Set Handicap ±1.5
        "45,"    # Odd/Even Games
        "339,"   # 1st Set Game Handicap
        "340,"   # 1st Set Total Games O/U
        "433,"   # 1st Set / Match Winner combo
        "353,"   # Player 1 Games O/U
        "352"    # Player 2 Games O/U
    ).replace("\n", "").replace(" ", ""),
)