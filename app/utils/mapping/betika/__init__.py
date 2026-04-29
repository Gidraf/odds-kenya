"""
app/workers/mappers/betika/__init__.py
=======================================
Betika mappers – dispatcher and registration.

Exports:
    get_market_slug(sport, sub_type_id, parsed_specifiers, fallback_name) -> str
    normalize_outcome(sport, display) -> str
"""

from __future__ import annotations

from typing import Dict

# Import all sport-specific mappers
from .betika_aussie_rules_mapper import BetikaAussieRulesMapper
from .betika_baseball_mapper import BetikaBaseballMapper
from .betika_basketball_mapper import BetikaBasketballMapper
from .betika_boxing_mapper import BetikaBoxingMapper
from .betika_cricket_mapper import BetikaCricketMapper
from .betika_darts_mapper import BetikaDartsMapper
from .betika_efootball_mapper import BetikaEFootballMapper
from .betika_floorball_mapper import BetikaFloorballMapper
from .betika_football_mapper import BetikaFootballMapper
from .betika_futsal_mapper import BetikaFutsalMapper
from .betika_handball_mapper import BetikaHandballMapper
from .betika_ice_hockey_mapper import BetikaIceHockeyMapper
from .betika_mma_mapper import BetikaMMAMapper
from .betika_rugby_mapper import BetikaRugbyMapper
from .betika_snookers_mapper import BetikaSnookersMapper
from .betika_table_tennis_mapper import BetikaTableTennisMapper
from .betika_tennis_mapper import BetikaTennisMapper
from .betika_volleyball_mapper import BetikaVolleyballMapper

# Import shared registration function
from app.workers.mappers.shared import register_sport

# =============================================================================
# Register outcome normalisers for each sport
# =============================================================================
register_sport("aussie-rules",    BetikaAussieRulesMapper)
register_sport("baseball",        BetikaBaseballMapper)
register_sport("basketball",      BetikaBasketballMapper)
register_sport("boxing",          BetikaBoxingMapper)
register_sport("cricket",         BetikaCricketMapper)
register_sport("darts",           BetikaDartsMapper)
register_sport("efootball",       BetikaEFootballMapper)
register_sport("esoccer",         BetikaEFootballMapper)   # alias
register_sport("floorball",       BetikaFloorballMapper)
register_sport("football",        BetikaFootballMapper)
register_sport("soccer",          BetikaFootballMapper)
register_sport("futsal",          BetikaFutsalMapper)
register_sport("handball",        BetikaHandballMapper)
register_sport("ice-hockey",      BetikaIceHockeyMapper)
register_sport("hockey",          BetikaIceHockeyMapper)
register_sport("mma",             BetikaMMAMapper)
register_sport("rugby",           BetikaRugbyMapper)
register_sport("rugby-union",     BetikaRugbyMapper)
register_sport("rugby-league",    BetikaRugbyMapper)
register_sport("snooker",         BetikaSnookersMapper)
register_sport("table-tennis",    BetikaTableTennisMapper)
register_sport("tennis",          BetikaTennisMapper)
register_sport("volleyball",      BetikaVolleyballMapper)

# =============================================================================
# Map sport name → mapper class (for market slug generation)
# =============================================================================
_SPORT_MAPPER: Dict[str, type] = {
    "aussie-rules":    BetikaAussieRulesMapper,
    "baseball":        BetikaBaseballMapper,
    "basketball":      BetikaBasketballMapper,
    "boxing":          BetikaBoxingMapper,
    "cricket":         BetikaCricketMapper,
    "darts":           BetikaDartsMapper,
    "efootball":       BetikaEFootballMapper,
    "esoccer":         BetikaEFootballMapper,
    "floorball":       BetikaFloorballMapper,
    "football":        BetikaFootballMapper,
    "soccer":          BetikaFootballMapper,
    "futsal":          BetikaFutsalMapper,
    "handball":        BetikaHandballMapper,
    "ice-hockey":      BetikaIceHockeyMapper,
    "hockey":          BetikaIceHockeyMapper,
    "mma":             BetikaMMAMapper,
    "rugby":           BetikaRugbyMapper,
    "rugby-union":     BetikaRugbyMapper,
    "rugby-league":    BetikaRugbyMapper,
    "snooker":         BetikaSnookersMapper,
    "table-tennis":    BetikaTableTennisMapper,
    "tennis":          BetikaTennisMapper,
    "volleyball":      BetikaVolleyballMapper,
}

# =============================================================================
# Public API (used by bt_harvester)
# =============================================================================
def get_market_slug(
    sport: str,
    sub_type_id: str,
    parsed_specifiers: dict,
    fallback_name: str = ""
) -> str:
    """
    Main entry point for Betika harvester.
    Returns a canonical market slug.
    """
    mapper = _SPORT_MAPPER.get(sport.lower())
    if mapper:
        slug = mapper.get_market_slug(sub_type_id, parsed_specifiers, fallback_name)
        if slug:
            return slug
    # Fallback
    return f"{sport}_{sub_type_id}"

def normalize_outcome(sport: str, display: str) -> str:
    """
    Normalise outcome using sport‑specific mapper's normalize_outcome method.
    """
    mapper = _SPORT_MAPPER.get(sport.lower())
    if mapper and hasattr(mapper, "normalize_outcome"):
        return mapper.normalize_outcome(display)
    # Fallback to shared generic
    from app.workers.mappers.shared import normalize_outcome as shared_normalize
    return shared_normalize("", display)