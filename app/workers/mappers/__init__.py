"""
app/workers/mappers/__init__.py
================================
Unified OdiBets market mapper dispatcher.

Wires all sport-specific mapper classes into a single entry point:

    from app.workers.mappers import resolve_od_market

    slug, outcomes = resolve_od_market(
        sport        = "soccer",
        market_slug  = "over_under_goals_2_5",
        raw_outcomes = {"over": 1.88, "under": 1.95},
    )
    # → ("over_under_goals_2_5", {"Over": 1.88, "Under": 1.95})

Fixes applied vs original files
---------------------------------
  • odibet.py        : OdibetBaseballMapper  → OdibetsBaseballMapper
  • odibet_cricket.py: OdibetCricketMapper   → OdibetsCricketMapper
  • odibet_esoccer.py: OdibetEsoccerMapper   → OdibetsEsoccerMapper
  • odibet_soccer.py : OdibetSoccerMapper    → OdibetsSoccerMapper
  • odibet_ice_hockey.py: OdibetIceHockeyMapper → OdibetsIceHockeyMapper
  • odibet_rugby.py  : OdibetRugbyMapper     → OdibetsRugbyMapper
  • odibet_table_tennis.py: OdibetTableTennisMapper → OdibetsTableTennisMapper
  • odibet_tennis.py : OdibetTennisMapper    → OdibetsTennisMapper
  • odibet_volleyball.py: OdibetVolleyballMapper → OdibetsVolleyballMapper
  • odibet_handball.py: OdibetHandballMapper → OdibetsHandballMapper
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

# ─── Lazy imports (avoids circular imports in worker context) ──────────────────
def _mapper(sport: str):
    """Return the mapper class for a sport, or None."""
    try:
        if sport in ("soccer", "football"):
            from app.utils.mapping.odibets.odibets_football_mapper import OdibetsSoccerMapper
            return OdibetsSoccerMapper
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
        if sport in ("mma", "boxing"):
            from app.utils.mapping.odibets.odibets_boxing_mapper import OdibetsBoxingMapper
            return OdibetsBoxingMapper
        if sport == "table-tennis":
            from app.utils.mapping.odibets.odibets_table_tennis_mapper import OdibetsTableTennisMapper
            return OdibetsTableTennisMapper
        if sport == "handball":
            from app.utils.mapping.odibets.odibets_handball_mapper import OdibetsHandballMapper
            return OdibetsHandballMapper
        if sport in ("esoccer", "efootball"):
            from app.utils.mapping.odibets.odibets_esoccer_mapper import OdibetsEsoccerMapper
            return OdibetsEsoccerMapper
    except ImportError as exc:
        log.debug("Mapper import failed for sport=%s: %s", sport, exc)
    return None


def resolve_od_market(
    sport:        str,
    market_slug:  str,
    raw_outcomes: Dict[str, Any],
) -> Tuple[str, Dict[str, Any]]:
    """
    Resolve one OdiBets market into (canonical_slug, canonical_outcomes).

    Args:
        sport:        canonical sport slug e.g. "soccer", "basketball"
        market_slug:  OdiBets market slug e.g. "over_under_goals_2_5"
        raw_outcomes: {raw_outcome_key → odd_value}

    Returns:
        (canonical_slug, {canonical_outcome_key → odd_value})
        Falls back to (market_slug, raw_outcomes) when no mapper matches.
    """
    mapper_cls = _mapper(sport)

    if mapper_cls is None:
        # No sport-specific mapper — return as-is
        return market_slug, raw_outcomes

    # ── Resolve canonical slug + specifiers ───────────────────────────────────
    info = mapper_cls.get_market_info(market_slug)

    if info is None:
        log.debug("No match for sport=%s slug=%s — keeping raw", sport, market_slug)
        return market_slug, raw_outcomes

    canonical_slug, specifiers = info

    # Embed specifiers in slug when they carry line/period info
    # e.g. over_under_goals + line=2.5 + period=match → over_under_goals_2_5
    final_slug = _embed_specifiers(canonical_slug, specifiers, market_slug)

    # ── Translate outcome keys ─────────────────────────────────────────────────
    canonical_outcomes: Dict[str, Any] = {}
    for raw_key, odd_val in raw_outcomes.items():
        try:
            can_key = mapper_cls.transform_outcome(market_slug, str(raw_key))
        except Exception:
            can_key = str(raw_key)
        # Normalise first letter capitalisation for standard keys
        can_key = _normalise_outcome_key(can_key)
        canonical_outcomes[can_key] = odd_val

    return final_slug, canonical_outcomes


def _embed_specifiers(slug: str, specifiers: Dict[str, str], original: str) -> str:
    """
    Produce a final slug that embeds the line/period specifiers so the
    frontend MarketTable can group and label them correctly.

    Examples:
      over_under_goals + {line: 2.5} → over_under_goals_2_5
      total_runs       + {line: 7.5, period: f5} → total_runs_f5_7_5
      asian_handicap   + {handicap: -1.5} → asian_handicap_minus_1_5
      1x2              + {period: first_half} → first_half_1x2
    """
    period   = specifiers.get("period", "")
    line     = specifiers.get("line", "")
    handicap = specifiers.get("handicap", "")
    team     = specifiers.get("team", "")

    parts = [slug]

    # Period prefix/suffix
    if period and period not in ("match", "full"):
        if period == "first_half":
            parts = ["first_half", slug]
        elif period == "f5":
            parts.append("f5")
        elif period not in ("", "full", "match"):
            parts.append(period.replace(" ", "_"))

    # Team
    if team:
        parts.append(team)

    # Line (over/under)
    if line:
        line_slug = line.replace(".", "_").replace("-", "minus_")
        parts.append(line_slug)

    # Handicap
    if handicap and not line:
        hcp_slug = handicap.replace(".", "_")
        if handicap.startswith("-"):
            hcp_slug = "minus_" + hcp_slug[1:].replace(".", "_")
        parts.append(hcp_slug)

    result = "_".join(p for p in parts if p)
    return result


def _normalise_outcome_key(key: str) -> str:
    """
    Normalise common outcome key variants to a consistent capitalisation.
    """
    MAP = {
        "over":   "Over",
        "under":  "Under",
        "yes":    "Yes",
        "no":     "No",
        "odd":    "Odd",
        "even":   "Even",
        "home":   "1",
        "away":   "2",
        "draw":   "X",
    }
    # Only map if the key is purely one of these words
    lower = key.strip().lower()
    return MAP.get(lower, key)


# ─── Batch helper for od_harvester.py ─────────────────────────────────────────

def resolve_od_markets(
    sport:        str,
    markets_raw:  list[dict],
) -> Dict[str, Dict[str, Any]]:
    """
    Resolve a full list of OdiBets markets for one match.

    Args:
        sport:       canonical sport slug
        markets_raw: list of market dicts from OdiBets API, each must have:
                       - "slug" or "name"   (market identifier)
                       - "outcomes"         ({key: odd} or list)

    Returns:
        {canonical_slug: {canonical_outcome_key: odd_value}}

    Merges duplicate slugs (different lines become separate entries like
    over_under_goals_2_5 and over_under_goals_3_5).
    """
    result: Dict[str, Dict[str, Any]] = {}

    for mkt in markets_raw:
        slug = str(mkt.get("slug") or mkt.get("name") or mkt.get("market_slug") or "")
        if not slug:
            continue

        # Normalise outcomes to {key: odd}
        raw_outs = mkt.get("outcomes") or mkt.get("odds") or {}
        if isinstance(raw_outs, list):
            raw_outs = {str(o.get("key") or o.get("name") or i): o.get("odd") or o.get("value") or 0
                        for i, o in enumerate(raw_outs)}

        can_slug, can_outs = resolve_od_market(sport, slug, raw_outs)

        # Merge (handles multi-line markets)
        if can_slug in result:
            result[can_slug].update(can_outs)
        else:
            result[can_slug] = can_outs

    return result