"""
app/workers/sp_sports/
=======================
Per-sport Sportpesa harvester configurations.

Each module exposes a single `CONFIG: SportConfig` object describing:
  - Which URL slugs map to this sport
  - The SP numeric sport_id
  - Which market IDs to request from the SP API
  - Fetch defaults (days, max matches)

Usage
─────
    from app.workers.sp_sports import get_config
    from app.workers.sp_harvester_base import fetch_upcoming_stream

    cfg = get_config("basketball")
    for match in fetch_upcoming_stream("basketball", cfg):
        print(match["home_team"], match["markets"])

The unified dispatcher (sp_harvester.py) uses this registry automatically.
"""

from __future__ import annotations
from app.workers.sp_harvester_base import SportConfig

# Import every sport config
from app.workers.sp_sports.football          import CONFIG as _FOOTBALL
from app.workers.sp_sports.efootball         import CONFIG as _EFOOTBALL
from app.workers.sp_sports.basketball        import CONFIG as _BASKETBALL
from app.workers.sp_sports.tennis            import CONFIG as _TENNIS
from app.workers.sp_sports.ice_hockey        import CONFIG as _ICE_HOCKEY
from app.workers.sp_sports.volleyball        import CONFIG as _VOLLEYBALL
from app.workers.sp_sports.handball          import CONFIG as _HANDBALL
from app.workers.sp_sports.table_tennis      import CONFIG as _TABLE_TENNIS
from app.workers.sp_sports.rugby             import CONFIG as _RUGBY
from app.workers.sp_sports.cricket           import CONFIG as _CRICKET
from app.workers.sp_sports.boxing            import CONFIG as _BOXING
from app.workers.sp_sports.mma               import CONFIG as _MMA
from app.workers.sp_sports.darts             import CONFIG as _DARTS
from app.workers.sp_sports.american_football import CONFIG as _AMERICAN_FOOTBALL

# Registry: every slug → its SportConfig
_ALL: list[SportConfig] = [
    _FOOTBALL, _EFOOTBALL, _BASKETBALL, _TENNIS,
    _ICE_HOCKEY, _VOLLEYBALL, _HANDBALL, _TABLE_TENNIS,
    _RUGBY, _CRICKET, _BOXING, _MMA, _DARTS, _AMERICAN_FOOTBALL,
]

_SLUG_MAP: dict[str, SportConfig] = {}
for _cfg in _ALL:
    for _slug in _cfg.slugs:
        _SLUG_MAP[_slug.lower()] = _cfg


def get_config(sport_slug: str) -> SportConfig | None:
    """Return the SportConfig for a slug, or None if unknown."""
    return _SLUG_MAP.get(sport_slug.lower().replace(" ", "-"))


def all_slugs() -> list[str]:
    """All recognised sport slugs."""
    return sorted(_SLUG_MAP.keys())


def all_configs() -> list[SportConfig]:
    """All SportConfig objects (one per sport)."""
    return _ALL