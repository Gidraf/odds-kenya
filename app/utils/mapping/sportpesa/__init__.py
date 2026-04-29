"""
app/workers/mappers/sportpesa/__init__.py
==========================================
SportPesa mappers – dispatcher and registration.

Exports:
    normalize_sp_market(market_id, spec_value, sport_id) -> str
"""

from __future__ import annotations

from typing import Optional

# Import all sport-specific mappers
from .sportpesa_america_football_mapper import SportpesaAmericanFootballMapper
from .sportpesa_baseball_mapper import SportpesaBaseballMapper
from .sportpesa_basketball_mapper import SportpesaBasketballMapper
from .sportpesa_boxing_mapper import SportpesaBoxingMapper
from .sportpesa_cricket_mapper import SportpesaCricketMapper
from .sporpesa_efootball_mapper import SportpesaEFootballMapper
from .sportpesa_football_mapper import SportpesaSoccerMapper
from .sportpesa_handball_mapper import SportpesaHandballMapper
from .sportpesa_ice_hockey_mapper import SportpesaIceHockeyMapper
from .sportpesa_mma_mapper import SportpesaMMAMapper
from .sportpesa_rugby_mapper import SportpesaRugbyMapper
from .sportpesa_tennis_mapper import SportpesaTennisMapper
from .sportpesa_volleyball_mapper import SportpesaVolleyballMapper

# Import shared registration function
from app.workers.mappers.shared import register_sport

# =============================================================================
# Register outcome normalisers for each sport (if mapper has normalize_outcome)
# =============================================================================
register_sport("american_football", SportpesaAmericanFootballMapper)
register_sport("baseball",          SportpesaBaseballMapper)
register_sport("basketball",        SportpesaBasketballMapper)
register_sport("boxing",            SportpesaBoxingMapper)
register_sport("cricket",           SportpesaCricketMapper)
register_sport("efootball",         SportpesaEFootballMapper)
register_sport("esoccer",           SportpesaEFootballMapper)   # alias
register_sport("soccer",            SportpesaSoccerMapper)
register_sport("football",          SportpesaSoccerMapper)
register_sport("handball",          SportpesaHandballMapper)
register_sport("ice_hockey",        SportpesaIceHockeyMapper)
register_sport("hockey",            SportpesaIceHockeyMapper)
register_sport("mma",               SportpesaMMAMapper)
register_sport("rugby",             SportpesaRugbyMapper)
register_sport("rugby-union",       SportpesaRugbyMapper)
register_sport("rugby-league",      SportpesaRugbyMapper)
register_sport("table_tennis",      None)
register_sport("tennis",            SportpesaTennisMapper)
register_sport("volleyball",        SportpesaVolleyballMapper)

# =============================================================================
# Map sport_id → mapper class
# =============================================================================
_SPORT_ID_MAP = {
    1:   SportpesaSoccerMapper,      # soccer
    2:   SportpesaBasketballMapper,
    3:   SportpesaBaseballMapper,
    4:   SportpesaIceHockeyMapper,
    5:   SportpesaTennisMapper,
    6:   SportpesaHandballMapper,
    10:  SportpesaBoxingMapper,
    12:  SportpesaRugbyMapper,
    15:  SportpesaAmericanFootballMapper,
    16:  None,
    21:  SportpesaCricketMapper,
    23:  SportpesaVolleyballMapper,
    49:  None,   # darts – add when mapper exists
    117: SportpesaMMAMapper,
    126: SportpesaEFootballMapper,
}

# =============================================================================
# Main dispatcher (used by sp_harvester)
# =============================================================================
def normalize_sp_market(market_id: int, spec_value: float, sport_id: int) -> str:
    """
    Map a SportPesa market to a canonical slug.
    Returns a slug string, or a fallback "market_{market_id}" if not found.
    """
    mapper = _SPORT_ID_MAP.get(sport_id)
    if mapper:
        slug = mapper.get_market_slug(market_id, spec_value)
        if slug:
            return slug
    # Fallback
    return f"market_{market_id}"