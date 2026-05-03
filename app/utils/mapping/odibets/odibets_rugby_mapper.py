"""
app/workers/mappers/odibet_rugby.py
====================================
OdiBets Rugby (Union & League) market mapper.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetsRugbyMapper:
    """Maps OdiBets Rugby JSON market slugs to canonical slugs + specifiers."""

    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "rugby_1x2":           ("rugby_result", {"period": "match"}),
        "rugby_double_chance": ("double_chance", {"period": "match"}),
        "rugby_draw_no_bet":   ("draw_no_bet", {"period": "match"}),
        "rugby_ht_ft":         ("ht_ft", {"period": "match"}),
        "rugby_first_half_1x2": ("first_half_result", {"period": "first_half"}),
        "rugby_":              None,   # ambiguous generic slug, skip
    }

    @classmethod
    def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
        """
        Parse an OdiBets market slug and return (canonical_slug, specifiers).
        """
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # ----- Winning Margin (banded) -----
        # Patterns: rugby_winning_margin_15, rugby__15 (from your JSON)
        win_margin_match = re.match(r"rugby(?:_winning_margin)?_(\d+)$", market_slug)
        if win_margin_match:
            band_max = win_margin_match.group(1)
            return ("winning_margin", {"band_max": band_max})

        # ----- Full Match Spread (Handicap) -----
        # Patterns: rugby_spread_minus_10_5, rugby_spread_minus_22_5, etc.
        spread_match = re.match(r"rugby_spread_minus_([\d_]+)$", market_slug)
        if spread_match:
            hcp_str = spread_match.group(1).replace("_", ".")
            handicap = -float(hcp_str)
            return ("point_spread", {"handicap": str(handicap), "period": "match"})

        # Could also be positive spread (unlikely, but handle)
        spread_plus_match = re.match(r"rugby_spread_plus_([\d_]+)$", market_slug)
        if spread_plus_match:
            hcp_str = spread_plus_match.group(1).replace("_", ".")
            handicap = float(hcp_str)
            return ("point_spread", {"handicap": str(handicap), "period": "match"})

        # ----- First Half Spread -----
        fh_spread_match = re.match(r"rugby_first_half_spread_minus_([\d_]+)$", market_slug)
        if fh_spread_match:
            hcp_str = fh_spread_match.group(1).replace("_", ".")
            handicap = -float(hcp_str)
            return ("point_spread", {"handicap": str(handicap), "period": "first_half"})

        fh_spread_plus_match = re.match(r"rugby_first_half_spread_plus_([\d_]+)$", market_slug)
        if fh_spread_plus_match:
            hcp_str = fh_spread_plus_match.group(1).replace("_", ".")
            handicap = float(hcp_str)
            return ("point_spread", {"handicap": str(handicap), "period": "first_half"})

        # Plain first half spread (e.g., rugby_first_half_spread_0_5)
        fh_spread_plain = re.match(r"rugby_first_half_spread_([\d_]+)$", market_slug)
        if fh_spread_plain:
            hcp_str = fh_spread_plain.group(1).replace("_", ".")
            handicap = float(hcp_str)
            return ("point_spread", {"handicap": str(handicap), "period": "first_half"})

        # ----- Generic spread (e.g., rugby_spread_0_5) - if it appears -----
        spread_generic = re.match(r"rugby_spread_([\d_]+)$", market_slug)
        if spread_generic:
            hcp_str = spread_generic.group(1).replace("_", ".")
            handicap = float(hcp_str)
            return ("point_spread", {"handicap": str(handicap), "period": "match"})

        # ----- Unknown market -----
        return None

    @classmethod
    def get_canonical_slug(cls, market_slug: str) -> Optional[str]:
        info = cls.get_market_info(market_slug)
        return info[0] if info else None

    @classmethod
    def transform_outcome(cls, market_slug: str, outcome_key: str) -> str:
        # 1X2 markets
        if market_slug in ("rugby_1x2", "rugby_first_half_1x2"):
            return {"1": "home", "X": "draw", "2": "away"}.get(outcome_key, outcome_key)

        # Double chance
        if market_slug == "rugby_double_chance":
            return {
                "1_or_x": "home_or_draw",
                "1_or_2": "home_or_away",
                "x_or_2": "draw_or_away"
            }.get(outcome_key, outcome_key)

        # Draw no bet
        if market_slug == "rugby_draw_no_bet":
            return "home" if outcome_key == "1" else "away"

        # Spread markets (handicap) - outcomes "1" (home) or "2" (away)
        if any(x in market_slug for x in ("spread", "first_half_spread")):
            return "home" if outcome_key == "1" else "away"

        # Winning margin outcomes: e.g., "1_by_15+", "1_by_1_7", "2_by_8_14", "X"
        if market_slug.startswith(("rugby_winning_margin", "rugby__")):
            # For "rugby__15" the outcomes are like "1_by_15+", etc.
            return outcome_key

        # Half-time / Full-time outcomes: e.g., "1/1", "X/2", etc.
        if market_slug == "rugby_ht_ft":
            # Keep as is (will be mapped later if needed)
            return outcome_key

        # Fallback
        return outcome_key


def get_od_rugby_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    return OdibetsRugbyMapper.get_market_info(market_slug)