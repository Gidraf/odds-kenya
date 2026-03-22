"""app/workers/sp_sports/efootball.py — eFootball / eSoccer (sportId=126)"""
from app.workers.sp_harvester_base import SportConfig

CONFIG = SportConfig(
    slugs        = ("esoccer", "efootball", "e-football", "virtual-football"),
    sport_id     = 126,
    days_default = 1,
    max_default  = 60,
    is_esoccer   = True,       # SP uses single markets_layout for virtual
    market_ids   = (
        "381,1,10,"  # 1X2 (381 = eFootball-specific ID)
        "56,52,"     # O/U Goals (56 = eFootball ID, 52 = fallback)
        "46,47,"     # Double Chance, Draw No Bet
        "43,"        # BTTS
        "51,"        # Asian Handicap
        "45,"        # Odd/Even
        "208,"       # Result + O/U
        "258,202"    # Exact Goals
    ).replace("\n", "").replace(" ", ""),
)