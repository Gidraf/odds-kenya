"""
app/workers/mappers/odibet_volleyball.py
=========================================
OdiBets Volleyball market mapper.
Converts OdiBets-specific market slugs (as produced by od_harvester.py)
into canonical market slugs + specifiers for internal use.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetsVolleyballMapper:
    """Maps OdiBets Volleyball JSON market slugs to canonical slugs + specifiers."""

    # Direct mapping for simple markets (no specifiers)
    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "volleyball_match_winner": ("volleyball_winner", {"period": "match"}),
        "first_set_winner":        ("first_set_winner", {"set": "1"}),
        "volleyball__sr_correct_score_bestof_5": ("volleyball_set_betting", {"max_sets": "5"}),
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
            "volleyball_match_winner"                    → ("volleyball_winner", {"period": "match"})
            "volleyball_total_points_178_5"              → ("volleyball_total_points", {"line": "178.5", "period": "match"})
            "volleyball_point_handicap_minus_5_5"        → ("volleyball_point_handicap", {"handicap": "-5.5", "period": "match"})
            "volleyball_184_5"                           → ("volleyball_total_points", {"line": "184.5", "period": "match"})
            "first_set_total_points_45_5"                → ("volleyball_set_total_points", {"set": "1", "line": "45.5"})
            "first_set_point_handicap_minus_3_5"         → ("volleyball_set_point_handicap", {"set": "1", "handicap": "-3.5"})
            "set_1_volleyball_"                          → ("volleyball_set_odd_even", {"set": "1"})
        """
        # ----- Static mappings -----
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # ----- Total points (full match) -----
        # Patterns: volleyball_total_points_178_5, volleyball__184_5, volleyball__178_5
        total_match = re.match(r"volleyball_total_points_([\d_]+)$", market_slug)
        if not total_match:
            total_match = re.match(r"volleyball__(\d+_\d+)$", market_slug)
        if total_match:
            line = float(total_match.group(1).replace("_", "."))
            return ("volleyball_total_points", {"line": str(line), "period": "match"})

        # ----- Point handicap (full match) -----
        # Patterns: volleyball_point_handicap_minus_5_5, volleyball_point_handicap_minus_7_5, etc.
        hcp_match = re.match(r"volleyball_point_handicap_minus_([\d_]+)$", market_slug)
        if hcp_match:
            hcp = -float(hcp_match.group(1).replace("_", "."))
            return ("volleyball_point_handicap", {"handicap": str(hcp), "period": "match"})
        # Also positive? Unlikely but handle
        hcp_pos = re.match(r"volleyball_point_handicap_([\d_]+)$", market_slug)
        if hcp_pos:
            hcp = float(hcp_pos.group(1).replace("_", "."))
            return ("volleyball_point_handicap", {"handicap": str(hcp), "period": "match"})

        # ----- First set total points -----
        fs_total = re.match(r"first_set_total_points_([\d_]+)$", market_slug)
        if fs_total:
            line = float(fs_total.group(1).replace("_", "."))
            return ("volleyball_set_total_points", {"set": "1", "line": str(line)})

        # ----- First set point handicap -----
        fs_hcp_minus = re.match(r"first_set_point_handicap_minus_([\d_]+)$", market_slug)
        if fs_hcp_minus:
            hcp = -float(fs_hcp_minus.group(1).replace("_", "."))
            return ("volleyball_set_point_handicap", {"set": "1", "handicap": str(hcp)})
        fs_hcp_pos = re.match(r"first_set_point_handicap_([\d_]+)$", market_slug)
        if fs_hcp_pos:
            hcp = float(fs_hcp_pos.group(1).replace("_", "."))
            return ("volleyball_set_point_handicap", {"set": "1", "handicap": str(hcp)})

        # ----- First set odd/even -----
        if market_slug == "set_1_volleyball_":
            return ("volleyball_set_odd_even", {"set": "1"})

        # ----- Special ambiguous "volleyball_" – likely "Will there be a 5th set?" -----
        if market_slug == "volleyball_":
            return ("volleyball_5th_set", {})

        # ----- Unknown market -----
        return None

    @classmethod
    def get_canonical_slug(cls, market_slug: str) -> Optional[str]:
        info = cls.get_market_info(market_slug)
        return info[0] if info else None

    @classmethod
    def transform_outcome(cls, market_slug: str, outcome_key: str) -> str:
        """
        Convert OdiBets outcome keys to canonical outcome names.
        """
        # Match winner
        if market_slug == "volleyball_match_winner":
            return "home" if outcome_key == "1" else "away"

        # First set winner
        if market_slug == "first_set_winner":
            return "home" if outcome_key == "1" else "away"

        # Set betting (correct score) outcomes like "3:0", "2:3"
        if market_slug == "volleyball__sr_correct_score_bestof_5":
            return outcome_key  # already like "3:0"

        # Over/under outcomes
        if market_slug.startswith(("volleyball_total_points", "volleyball__", "first_set_total_points")):
            return outcome_key  # "over" or "under"

        # Point handicap outcomes
        if market_slug.startswith(("volleyball_point_handicap", "first_set_point_handicap")):
            return "home" if outcome_key == "1" else "away" if outcome_key == "2" else outcome_key

        # Odd/even
        if market_slug == "set_1_volleyball_":
            return outcome_key  # "odd" or "even"

        # 5th set yes/no
        if market_slug == "volleyball_":
            return outcome_key  # "yes" or "no"

        return outcome_key


def get_od_volleyball_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    return OdibetVolleyballMapper.get_market_info(market_slug)