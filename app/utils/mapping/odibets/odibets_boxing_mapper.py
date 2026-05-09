"""
app/workers/mappers/odibet_boxing.py
=====================================
OdiBets Boxing market mapper.
Converts OdiBets-specific market slugs (as produced by od_harvester.py)
into canonical market slugs + specifiers for internal use.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetsBoxingMapper:
    """Maps OdiBets Boxing JSON market slugs to canonical slugs + specifiers."""

    # Direct mapping for simple markets (no specifiers)
    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "boxing_1x2":                            ("boxing_winner", {"period": "full"}),
        "boxing_moneyline":                      ("boxing_winner", {"period": "full", "two_way": "true"}),
        "boxing_":                               ("fight_distance", {}),  # yes/no -> fight goes the distance
    }

    @staticmethod
    def format_round_number(round_num: int) -> str:
        """Convert round number to string, e.g., 1 -> '1'."""
        return str(round_num)

    @classmethod
    def get_market_info(
        cls, market_slug: str
    ) -> Optional[Tuple[str, Dict[str, str]]]:
        """
        Parse an OdiBets market slug and return (canonical_slug, specifiers).

        Examples:
            "boxing__sr_winning_method_ko_decision" -> ("method_of_victory", {})
            "boxing__sr_winner_and_rounds_12"       -> ("round_betting", {"total_rounds": "12"})
            "boxing__sr_winner_and_round_range_12"  -> ("round_group", {"total_rounds": "12"})
        """
        # ----- Static mappings -----
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # ----- Method of Victory (KO/Decision) -----
        if market_slug == "boxing__sr_winning_method_ko_decision":
            return ("method_of_victory", {})

        # ----- Winner + Exact Round -----
        # Pattern: boxing__sr_winner_and_rounds_12  (12 = total rounds in fight)
        exact_round_match = re.match(r"boxing__sr_winner_and_rounds_(\d+)$", market_slug)
        if exact_round_match:
            total_rounds = exact_round_match.group(1)
            return ("round_betting", {"total_rounds": total_rounds})

        # ----- Winner + Round Range -----
        # Pattern: boxing__sr_winner_and_round_range_12
        range_match = re.match(r"boxing__sr_winner_and_round_range_(\d+)$", market_slug)
        if range_match:
            total_rounds = range_match.group(1)
            return ("round_group", {"total_rounds": total_rounds})

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
        Used when building the final outcomes dict.
        """
        # For method_of_victory market
        if market_slug == "boxing__sr_winning_method_ko_decision":
            mapping = {
                "1_by_ko":       "home_ko",
                "2_by_ko":       "away_ko",
                "1_by_decision": "home_decision",
                "2_by_decision": "away_decision",
                "X":             "draw",
            }
            return mapping.get(outcome_key, outcome_key)

        # For winner + exact round market
        if market_slug.startswith("boxing__sr_winner_and_rounds_"):
            # outcomes: "1_1", "1_2", ..., "2_12", "1_decision", "2_decision", "X"
            if outcome_key == "X":
                return "draw"
            if "_" in outcome_key:
                parts = outcome_key.split("_")
                if len(parts) == 2:
                    fighter, value = parts
                    if value.isdigit():
                        return f"home_round_{value}" if fighter == "1" else f"away_round_{value}"
                    elif value == "decision":
                        return f"home_decision" if fighter == "1" else "away_decision"
            return outcome_key

        # For winner + round range market
        if market_slug.startswith("boxing__sr_winner_and_round_range_"):
            # outcomes: "1_1_3", "1_4_6", "2_1_3", "1_decision", etc.
            if outcome_key == "X":
                return "draw"
            if "_" in outcome_key:
                parts = outcome_key.split("_")
                if len(parts) == 2:
                    fighter, value = parts
                    if value == "decision":
                        return f"home_decision" if fighter == "1" else "away_decision"
                    # Could be range like "1_3" but actually pattern is "1_1_3"?
                elif len(parts) == 3:
                    fighter, start, end = parts
                    if start.isdigit() and end.isdigit():
                        return f"home_rounds_{start}_{end}" if fighter == "1" else f"away_rounds_{start}_{end}"
            return outcome_key

        # For fight distance market
        if market_slug == "boxing_":
            # outcome_key is "yes" or "no"
            return outcome_key

        # Default: keep original
        return outcome_key


# Generic dispatcher
def get_od_boxing_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    from app.utils.mapping.odibets.odibets_basketball_mapper import OdibetsBasketballMapper  # ✓
    return OdibetsBasketballMapper.get_market_info(market_slug)
 