"""
app/utils/mapping/odibets/__init__.py
======================================
OdiBets market mapper dispatcher.

USAGE:
    from app.utils.mapping.odibets import resolve_od_market
    slug, outcomes = resolve_od_market("soccer", market_slug, raw_outcomes)
"""
from __future__ import annotations
import logging
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

# ── Lazy sport mapper loader ──────────────────────────────────────────────────

def _get_mapper(sport: str):
    """Return the mapper class for a sport, or None."""
    sport = sport.lower().strip()
    try:
        if sport in ("soccer", "football"):
            from app.utils.mapping.odibets.odibets_football_mapper import OdibetsFootballMapper
            return OdibetsFootballMapper
        if sport == "basketball":
            from app.utils.mapping.odibets.odibets_basketball_mapper import OdibetsBasketballMapper
            return OdibetsBasketballMapper
        if sport == "tennis":
            from app.utils.mapping.odibets.odibets_tennis_mapper import OdibetsTennisMapper
            return OdibetsTennisMapper
        if sport == "ice-hockey":
            from app.utils.mapping.odibets.odibets_ice_hockey_mapper import OdibetsIceHockeyMapper
            return OdibetsIceHockeyMapper
        if sport == "volleyball":
            from app.utils.mapping.odibets.odibets_volleyball_mapper import OdibetsVolleyballMapper
            return OdibetsVolleyballMapper
        if sport == "cricket":
            from app.utils.mapping.odibets.odibets_cricket_mapper import OdibetsCricketMapper
            return OdibetsCricketMapper
        if sport == "rugby":
            from app.utils.mapping.odibets.odibets_rugby_mapper import OdibetsRugbyMapper
            return OdibetsRugbyMapper
        if sport == "baseball":
            from app.utils.mapping.odibets.odibets_baseball_mapper import OdibetsBaseballMapper
            return OdibetsBaseballMapper
        if sport == "boxing":
            from app.utils.mapping.odibets.odibets_boxing_mapper import OdibetsBoxingMapper
            return OdibetsBoxingMapper
        if sport == "mma":
            from app.utils.mapping.odibets.odibets_mma_mapper import OdibetsMMAMapper
            return OdibetsMMAMapper
        if sport == "table-tennis":
            from app.utils.mapping.odibets.odibets_table_tennis_mapper import OdibetsTableTennisMapper
            return OdibetsTableTennisMapper
        if sport == "handball":
            from app.utils.mapping.odibets.odibets_handball_mapper import OdibetsHandballMapper
            return OdibetsHandballMapper
        if sport in ("esoccer", "efootball"):
            from app.utils.mapping.odibets.odibets_esoccer_mapper import OdibetsEsoccerMapper
            return OdibetsEsoccerMapper
        if sport == "darts":
            from app.utils.mapping.odibets.odibets_darts_mapper import OdibetsDartsMapper
            return OdibetsDartsMapper
    except ImportError as exc:
        log.debug("Mapper import error sport=%s: %s", sport, exc)
    return None


# ── Outcome key normalisation ─────────────────────────────────────────────────

_OUTCOME_MAP = {
    "over": "Over", "under": "Under", "yes": "Yes", "no": "No",
    "odd": "Odd", "even": "Even", "home": "1", "away": "2", "draw": "X",
    "home_or_draw": "1X", "1_or_x": "1X", "draw_or_away": "X2",
    "x_or_2": "X2", "home_or_away": "12", "1_or_2": "12",
}

def _normalise_outcome(key: str) -> str:
    return _OUTCOME_MAP.get(key.strip().lower(), key)


# ── Specifier embedding ───────────────────────────────────────────────────────

def _embed_specifiers(slug: str, spec: Dict[str, str]) -> str:
    period   = spec.get("period", "")
    line     = spec.get("line", "")
    handicap = spec.get("handicap", "")
    team     = spec.get("team", "")
    parts    = [slug]
    if period and period not in ("match", "full"):
        if period == "first_half":
            parts = ["first_half", slug]
        elif period not in ("", "full", "match"):
            parts.append(period.replace(" ", "_"))
    if team:
        parts.append(team)
    if line:
        parts.append(line.replace(".", "_").replace("-", "minus_"))
    if handicap and not line:
        hcp = handicap.replace(".", "_")
        if handicap.startswith("-"):
            hcp = "minus_" + hcp[1:].replace(".", "_")
        parts.append(hcp)
    return "_".join(p for p in parts if p)


# ── Main dispatcher ───────────────────────────────────────────────────────────

def resolve_od_market(
    sport: str,
    market_slug: str,
    raw_outcomes: Dict[str, Any],
) -> Tuple[str, Dict[str, Any]]:
    """
    Resolve one OdiBets market slug → (canonical_slug, canonical_outcomes).
    Falls back to (market_slug, raw_outcomes) when no mapper matches.
    """
    mapper_cls = _get_mapper(sport)
    if mapper_cls is None:
        return market_slug, raw_outcomes

    info = mapper_cls.get_market_info(market_slug) if hasattr(mapper_cls, "get_market_info") else None
    if info is None:
        return market_slug, raw_outcomes

    canonical_slug, specifiers = info
    final_slug = _embed_specifiers(canonical_slug, specifiers)

    canonical_outcomes: Dict[str, Any] = {}
    for raw_key, odd_val in raw_outcomes.items():
        try:
            can_key = mapper_cls.transform_outcome(market_slug, str(raw_key)) \
                if hasattr(mapper_cls, "transform_outcome") else str(raw_key)
        except Exception:
            can_key = str(raw_key)
        canonical_outcomes[_normalise_outcome(can_key)] = odd_val

    return final_slug, canonical_outcomes


def resolve_od_markets_batch(
    sport: str,
    markets_raw: list,
) -> Dict[str, Dict[str, Any]]:
    """Resolve a full list of OdiBets markets for one match."""
    result: Dict[str, Dict[str, Any]] = {}
    for mkt in markets_raw:
        slug = str(mkt.get("slug") or mkt.get("name") or mkt.get("market_slug") or "")
        if not slug:
            continue
        raw_outs = mkt.get("outcomes") or mkt.get("odds") or {}
        if isinstance(raw_outs, list):
            raw_outs = {
                str(o.get("key") or o.get("name") or i):
                o.get("odd") or o.get("value") or o.get("odd_value") or 0
                for i, o in enumerate(raw_outs)
            }
        can_slug, can_outs = resolve_od_market(sport, slug, raw_outs)
        if can_slug in result:
            result[can_slug].update(can_outs)
        else:
            result[can_slug] = can_outs
    return result