"""
app/workers/sp_harvester.py
============================
Unified Sportpesa harvester dispatcher.

This is the single entry point for all sports.  It routes each call to the
correct per-sport config and delegates to the generic functions in
sp_harvester_base.py.

Public API (backwards-compatible with previous monolithic harvester)
─────────────────────────────────────────────────────────────────────
  fetch_upcoming(sport_slug, ...)       → list[dict]
  fetch_live(sport_slug, ...)           → list[dict]
  fetch_upcoming_stream(sport_slug, …)  → Generator[dict]
  fetch_live_stream(sport_slug, …)      → Generator[dict]
  fetch_match_markets(game_id, …)       → dict
  fetch_sport_ids()                     → dict

Architecture
────────────
  sp_harvester.py          ← YOU ARE HERE (unified dispatcher)
    ↓
  sp_harvester_base.py     ← shared HTTP / parse / stream utilities
    ↓
  sp_mapper.py             ← unified market ID → canonical slug (all sports)
    ↓
  canonical_mapper.py      ← shared outcome normalisation

  app/workers/sp_sports/
    __init__.py            ← registry (slug → SportConfig)
    football.py            ← soccer / football config
    efootball.py           ← esoccer / efootball config
    basketball.py
    tennis.py
    ice_hockey.py
    volleyball.py
    handball.py
    table_tennis.py
    rugby.py
    cricket.py
    boxing.py
    mma.py
    darts.py
    american_football.py

Adding a new sport
──────────────────
  1. Create app/workers/sp_sports/{sport}.py with a CONFIG = SportConfig(...)
  2. Import and register it in sp_sports/__init__.py
  That's it — the dispatcher and view layer pick it up automatically.
"""

from __future__ import annotations

from typing import Generator

from app.workers.sp_sports          import get_config, all_slugs
from app.workers.sp_harvester_base  import (
    SportConfig,
    fetch_upcoming_stream  as _stream_upcoming,
    fetch_live_stream      as _stream_live,
    fetch_upcoming         as _fetch_upcoming,
    fetch_live             as _fetch_live,
    fetch_match_markets    as _fetch_match_markets,
    _get,
)


# =============================================================================
# HELPERS
# =============================================================================

def _resolve(sport_slug: str) -> tuple[str, SportConfig]:
    """Normalise slug and return (normalised_slug, SportConfig)."""
    slug = sport_slug.lower().replace(" ", "-")
    cfg  = get_config(slug)
    if cfg is None:
        raise ValueError(
            f"Unknown sport slug: {sport_slug!r}. "
            f"Known slugs: {all_slugs()}"
        )
    return slug, cfg


# =============================================================================
# PUBLIC API — STREAMING
# =============================================================================

def fetch_upcoming_stream(
    sport_slug:         str,
    days:               int        | None = None,
    max_matches:        int        | None = None,
    fetch_full_markets: bool              = True,
    sleep_between:      float             = 0.3,
    debug_ou:           bool              = False,
    **_,
) -> Generator[dict, None, None]:
    """
    Yield one normalised match dict at a time as markets are fetched.
    Used by SSE endpoints in sp_module.py for real-time streaming.
    """
    slug, cfg = _resolve(sport_slug)
    yield from _stream_upcoming(
        slug, cfg,
        days               = days,
        max_matches        = max_matches,
        fetch_full_markets = fetch_full_markets,
        sleep_between      = sleep_between,
        debug_ou           = debug_ou,
    )


def fetch_live_stream(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.3,
    debug_ou:           bool  = False,
    **_,
) -> Generator[dict, None, None]:
    """Yield live matches one at a time."""
    slug, cfg = _resolve(sport_slug)
    yield from _stream_live(
        slug, cfg,
        fetch_full_markets = fetch_full_markets,
        sleep_between      = sleep_between,
        debug_ou           = debug_ou,
    )


# =============================================================================
# PUBLIC API — BLOCKING
# =============================================================================

def fetch_upcoming(
    sport_slug:         str,
    days:               int        | None = None,
    max_matches:        int        | None = None,
    fetch_full_markets: bool              = True,
    sleep_between:      float             = 0.3,
    debug_ou:           bool              = False,
    **_,
) -> list[dict]:
    """Blocking — collects all upcoming matches for a sport into a list."""
    slug, cfg = _resolve(sport_slug)
    results   = _fetch_upcoming(
        slug, cfg,
        days               = days,
        max_matches        = max_matches,
        fetch_full_markets = fetch_full_markets,
        sleep_between      = sleep_between,
        debug_ou           = debug_ou,
    )
    print(f"[sp] {slug}: {len(results)} matches total")
    return results


def fetch_live(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.3,
    debug_ou:           bool  = False,
    **_,
) -> list[dict]:
    """Blocking — collects all live matches for a sport into a list."""
    slug, cfg = _resolve(sport_slug)
    results   = _fetch_live(
        slug, cfg,
        fetch_full_markets = fetch_full_markets,
        sleep_between      = sleep_between,
        debug_ou           = debug_ou,
    )
    print(f"[sp:live] {slug}: {len(results)} live")
    return results


# =============================================================================
# PUBLIC API — ON-DEMAND
# =============================================================================

def fetch_match_markets(
    game_id:    str | int,
    sport_slug: str = "soccer",
) -> dict[str, dict[str, float]]:
    """
    Full market book for one SP game ID.

    sport_slug is used to select the right market IDs to request and the
    correct market ID → slug mapping.  Defaults to football.
    """
    try:
        _, cfg = _resolve(sport_slug)
    except ValueError:
        from app.workers.sp_sports.football import CONFIG as _FOOTBALL_CFG
        cfg = _FOOTBALL_CFG

    return _fetch_match_markets(
        game_id,
        market_ids = cfg.market_ids,
        sport_id   = cfg.sport_id,
    )


# =============================================================================
# PUBLIC API — SPORT LIST
# =============================================================================

def fetch_sport_ids() -> dict[str, int]:
    """Return {sport_name: live_event_count} from the SP live sports API."""
    raw, _ = _get("/api/live/sports")
    if not isinstance(raw, dict):
        return {}
    sports = raw.get("sports") or raw.get("data") or []
    if isinstance(sports, list):
        return {s.get("name", ""): s.get("eventNumber", 0) for s in sports}
    return {}


# =============================================================================
# INTERNAL — exposed for /debug endpoint in sp_module.py
# =============================================================================

def _fetch_markets_for_debug(
    game_id:    str,
    sport_slug: str = "soccer",
    debug:      bool = True,
) -> list[dict]:
    """Raw market list for a game — used by /api/sp/debug/markets/<id>."""
    from app.workers.sp_harvester_base import _fetch_markets
    try:
        _, cfg = _resolve(sport_slug)
        mids   = cfg.market_ids
    except ValueError:
        from app.workers.sp_sports.football import CONFIG as _FOOTBALL_CFG
        mids   = _FOOTBALL_CFG.market_ids
    return _fetch_markets(game_id, mids, debug=debug)


def _parse_markets_for_debug(
    raw_list:   list[dict],
    game_id:    str = "",
    sport_slug: str = "soccer",
) -> dict[str, dict[str, float]]:
    """Parsed markets for debug endpoint."""
    from app.workers.sp_harvester_base import _parse_markets
    try:
        _, cfg = _resolve(sport_slug)
        sid    = cfg.sport_id
    except ValueError:
        sid    = 1
    return _parse_markets(raw_list, game_id=game_id, sport_id=sid)