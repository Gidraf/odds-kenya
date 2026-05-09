"""
app/workers/mappers/odibet_esoccer.py
======================================
OdiBets Esoccer market mapper.
Converts OdiBets-specific market slugs (as produced by od_harvester.py)
into canonical market slugs + specifiers for internal use.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetsEsoccerMapper:
    """Maps OdiBets Esoccer (and eFootball) JSON market slugs to canonical slugs + specifiers."""

    # Direct mapping for simple markets (no specifiers)
    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "esoccer_1x2":                  ("1x2", {"period": "match"}),
        "efootball_double_chance":      ("double_chance", {"period": "match"}),
        "efootball_draw_no_bet":        ("draw_no_bet", {"period": "match"}),
    }

    @staticmethod
    def format_line(value: float) -> str:
        """Convert a numeric line into a URL-safe slug fragment."""
        if value == 0:
            return "0_0"
        val_str = f"{value:g}".replace(".", "_")
        return val_str.replace("-", "minus_") if value < 0 else val_str

    @classmethod
    def get_market_info(
        cls, market_slug: str
    ) -> Optional[Tuple[str, Dict[str, str]]]:
        """
        Parse an OdiBets market slug and return (canonical_slug, specifiers).

        Examples:
            "esoccer_1x2"                         → ("1x2", {"period": "match"})
            "efootball_double_chance"             → ("double_chance", {"period": "match"})
            "over_under_efootball_goals_1_5"      → ("over_under_goals", {"line": "1.5", "period": "match"})
            "efootball_asian_handicap_0_5"        → ("asian_handicap", {"handicap": "0.5"})
            "efootball_asian_handicap_minus_0_5"  → ("asian_handicap", {"handicap": "-0.5"})
        """
        # ----- Static mappings -----
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # ----- Over/Under Goals -----
        # Patterns: over_under_efootball_goals_1_5, over_under_efootball_goals_2_5, etc.
        ou_match = re.match(r"over_under_efootball_goals_([\d_]+)$", market_slug)
        if ou_match:
            line = float(ou_match.group(1).replace("_", "."))
            return ("over_under_goals", {"line": str(line), "period": "match"})

        # ----- Asian Handicap (positive) -----
        # Pattern: efootball_asian_handicap_0_5
        ah_match = re.match(r"efootball_asian_handicap_([\d_]+)$", market_slug)
        if ah_match:
            handicap = float(ah_match.group(1).replace("_", "."))
            return ("asian_handicap", {"handicap": str(handicap)})

        # ----- Asian Handicap (negative, with "minus") -----
        # Pattern: efootball_asian_handicap_minus_0_5
        ah_minus_match = re.match(r"efootball_asian_handicap_minus_([\d_]+)$", market_slug)
        if ah_minus_match:
            handicap = -float(ah_minus_match.group(1).replace("_", "."))
            return ("asian_handicap", {"handicap": str(handicap)})

        # ----- Unknown market -----
        return None

    @classmethod
    def get_canonical_slug(cls, market_slug: str) -> Optional[str]:
        """Return just the canonical slug (without specifiers)."""
        info = cls.get_market_info(market_slug)
        return info[0] if info else None

    @classmethod
    def transform_outcome(cls, market_slug: str, outcome_key: str) -> str:
        """
        Convert OdiBets outcome keys to canonical outcome names.
        """
        # For 1X2 markets
        if market_slug == "esoccer_1x2":
            mapping = {"1": "home", "X": "draw", "2": "away"}
            return mapping.get(outcome_key, outcome_key)

        # For double chance (e.g., "1_or_x", "1_or_2", "x_or_2")
        if market_slug == "efootball_double_chance":
            # Convert to e.g. "home_or_draw", "home_or_away", "draw_or_away"
            mapping = {
                "1_or_x": "home_or_draw",
                "1_or_2": "home_or_away",
                "x_or_2": "draw_or_away",
            }
            return mapping.get(outcome_key, outcome_key)

        # For draw no bet
        if market_slug == "efootball_draw_no_bet":
            mapping = {"1": "home", "2": "away"}
            return mapping.get(outcome_key, outcome_key)

        # For asian handicap: outcome keys "1" or "2"
        if market_slug.startswith("efootball_asian_handicap"):
            return "home" if outcome_key == "1" else "away"

        # For over/under: outcome keys "over" or "under"
        return outcome_key


# Generic dispatcher
def get_od_esoccer_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    return OdibetsEsoccerMapper.get_market_info(market_slug)