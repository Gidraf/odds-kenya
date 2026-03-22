"""app/workers/sp_sports/football.py — Football / Soccer (sportId=1)"""
from app.workers.sp_harvester_base import SportConfig

CONFIG = SportConfig(
    slugs        = ("soccer", "football"),
    sport_id     = 1,
    days_default = 3,
    max_default  = 150,
    is_esoccer   = False,
    market_ids   = (
        "10,1,"          # 1X2
        "46,47,"         # Double Chance, Draw No Bet
        "43,29,386,"     # BTTS, BTTS+Result
        "52,18,"         # O/U Goals (multi-line via specValue)
        "353,352,"       # Home / Away team goals O/U
        "208,"           # Result + O/U
        "258,202,"       # Exact Goals, Goal Groups
        "332,"           # Correct Score
        "51,53,"         # Asian HC FT + HT
        "55,"            # European HC
        "45,"            # Odd/Even Goals
        "41,"            # First Team to Score
        "207,"           # Highest Scoring Half
        "42,60,"         # HT 1X2
        "15,54,68,"      # HT O/U
        "44,"            # HT/FT
        "328,"           # HT BTTS
        "203,"           # HT Correct Score
        "162,166,"       # Total Corners
        "136,139"        # Total Bookings / Cards
    ).replace("\n", "").replace(" ", ""),
)