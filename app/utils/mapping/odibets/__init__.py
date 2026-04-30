"""
app/workers/mappers/odibet/__init__.py
======================================
OdiBets mappers – dispatcher and registration.

Exports:
    get_od_market_info(sport, market_slug) -> (canonical_slug, specifiers) or None
"""

from __future__ import annotations

from typing import Dict, Optional, Tuple

# Import all sport-specific mappers
from .odibets_baseball_mapper import OdibetsBaseballMapper
from .odibets_basketball_mapper import OdibetsBasketballMapper
from .odibets_boxing_mapper import OdibetsBoxingMapper
from .odibets_cricket_mapper import OdibetsCricketMapper
from .odibets_darts_mapper import OdibetsDartsMapper
from .odibets_esoccer_mapper import OdibetsEsoccerMapper
from .odibets_football_mapper import OdibetsSoccerMapper      # or OdibetsSoccerMapper
from .odibets_handball_mapper import OdibetsHandballMapper
from .odibets_ice_hockey_mapper import OdibetsIceHockeyMapper
# from .odibets_mma_mapper import OdibetsMMAMapper
from .odibets_rugby_mapper import OdibetsRugbyMapper
from .odibets_table_tennis_mapper import OdibetsTableTennisMapper
from .odibets_tennis_mapper import OdibetsTennisMapper
from .odibets_volleyball_mapper import OdibetsVolleyballMapper

# Import shared registration function
from app.workers.mappers.shared import register_sport

# =============================================================================
# Register outcome normalisers for each sport (if mapper has normalize_outcome)
# =============================================================================
register_sport("baseball",        OdibetsBaseballMapper)
register_sport("basketball",      OdibetsBasketballMapper)
register_sport("boxing",          OdibetsBoxingMapper)
register_sport("cricket",         OdibetsCricketMapper)
register_sport("darts",           OdibetsDartsMapper)
register_sport("esoccer",         OdibetsEsoccerMapper)
register_sport("efootball",       OdibetsEsoccerMapper)   # alias
register_sport("soccer",          OdibetsSoccerMapper)
register_sport("football",        OdibetsSoccerMapper)
register_sport("handball",        OdibetsHandballMapper)
register_sport("ice-hockey",      OdibetsIceHockeyMapper)
register_sport("hockey",          OdibetsIceHockeyMapper)
register_sport("mma",             None)
register_sport("rugby",           OdibetsRugbyMapper)
register_sport("rugby-union",     OdibetsRugbyMapper)
register_sport("rugby-league",    OdibetsRugbyMapper)
register_sport("table-tennis",    OdibetsTableTennisMapper)
register_sport("tennis",          OdibetsTennisMapper)
register_sport("volleyball",      OdibetsVolleyballMapper)

# =============================================================================
# Market slug dispatcher (for the harvester)
# =============================================================================
_SPORT_MAPPER: Dict[str, type] = {
    "baseball":        OdibetsBaseballMapper,
    "basketball":      OdibetsBasketballMapper,
    "boxing":          OdibetsBoxingMapper,
    "cricket":         OdibetsCricketMapper,
    "darts":           OdibetsDartsMapper,
    "esoccer":         OdibetsEsoccerMapper,
    "efootball":       OdibetsEsoccerMapper,
    "football":        OdibetsSoccerMapper,
    "soccer":          OdibetsSoccerMapper,
    "handball":        OdibetsHandballMapper,
    "ice-hockey":      OdibetsIceHockeyMapper,
    "hockey":          OdibetsIceHockeyMapper,
    "mma":             None,
    "rugby":           OdibetsRugbyMapper,
    "rugby-union":     OdibetsRugbyMapper,
    "rugby-league":    OdibetsRugbyMapper,
    "table-tennis":    OdibetsTableTennisMapper,
    "tennis":          OdibetsTennisMapper,
    "volleyball":      OdibetsVolleyballMapper,
}

def get_od_market_info(sport: str, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    """
    Main entry point for OdiBets harvester.
    Returns (canonical_slug, specifiers) or None if sport not found.
    """
    mapper = _SPORT_MAPPER.get(sport.lower())
    if mapper and hasattr(mapper, "get_market_info"):
        return mapper.get_market_info(market_slug)
    return None