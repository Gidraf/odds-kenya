"""
app/workers/mappers/odibet_darts.py
====================================
OdiBets Darts market mapper.
Converts OdiBets-specific market slugs (as produced by od_harvester.py)
into canonical market slugs + specifiers for internal use.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetDartsMapper:
    """Maps OdiBets Darts JSON market slugs to canonical slugs + specifiers."""

    # Direct mapping for simple markets (no specifiers)
    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "darts_winner":          ("darts_winner", {"period": "match"}),
        "darts_match_winner":    ("darts_winner", {"period": "match"}),
        # Some matches may use generic "soccer_winner" due to API glitch
        "soccer_winner":         ("darts_winner", {"period": "match"}),
        "soccer_":               ("darts_winner", {"period": "match"}),
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
            "darts_winner"                    → ("darts_winner", {"period": "match"})
            "over_under_darts_180s_3_5"       → ("total_180s", {"line": "3.5"})
            "darts_correct_score_4_2"         → ("darts_correct_score", {"sets": "4_2"})
            "darts_set_handicap_1_5"          → ("set_handicap", {"handicap": "1.5"})
            "darts_player_180s_2_5"           → ("player_180s", {"line": "2.5"})
            "darts_first_180"                 → ("first_180", {})
            "over_under_darts_total_legs_10_5"→ ("total_legs", {"line": "10.5"})
        """
        # ----- Static mappings (including fallbacks) -----
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # ----- Total 180s (Over/Under) -----
        total_180s_match = re.match(r"over_under_darts_180s_([\d_]+)$", market_slug)
        if total_180s_match:
            line = float(total_180s_match.group(1).replace("_", "."))
            return ("total_180s", {"line": str(line)})

        # ----- Player 180s (Over/Under) -----
        player_180s_match = re.match(r"darts_player_180s_([\d_]+)$", market_slug)
        if player_180s_match:
            line = float(player_180s_match.group(1).replace("_", "."))
            return ("player_180s", {"line": str(line)})

        # ----- Correct Score (Sets/Legs) -----
        # Format: darts_correct_score_4_2  (4-2 set score)
        correct_score_match = re.match(r"darts_correct_score_([\d_]+)$", market_slug)
        if correct_score_match:
            score = correct_score_match.group(1).replace("_", "-")
            return ("darts_correct_score", {"score": score})

        # ----- Set Handicap -----
        set_hcp_match = re.match(r"darts_set_handicap_([\d_]+)$", market_slug)
        if set_hcp_match:
            line = set_hcp_match.group(1).replace("_", ".")
            return ("set_handicap", {"handicap": line})

        # ----- Leg Handicap -----
        leg_hcp_match = re.match(r"darts_leg_handicap_([\d_]+)$", market_slug)
        if leg_hcp_match:
            line = leg_hcp_match.group(1).replace("_", ".")
            return ("leg_handicap", {"handicap": line})

        # ----- First To Score 180 -----
        if market_slug == "darts_first_180":
            return ("first_180", {})

        # ----- Highest Checkout -----
        if market_slug == "darts_highest_checkout":
            return ("highest_checkout", {})

        # ----- Total Legs Over/Under -----
        total_legs_match = re.match(r"over_under_darts_total_legs_([\d_]+)$", market_slug)
        if total_legs_match:
            line = float(total_legs_match.group(1).replace("_", "."))
            return ("total_legs", {"line": str(line)})

        # ----- Set Winner (specific set) -----
        set_winner_match = re.match(r"darts_set_(\d+)_winner$", market_slug)
        if set_winner_match:
            set_num = set_winner_match.group(1)
            return ("set_winner", {"set": set_num})

        # ----- Leg Winner (specific leg) -----
        leg_winner_match = re.match(r"darts_leg_(\d+)_winner$", market_slug)
        if leg_winner_match:
            leg_num = leg_winner_match.group(1)
            return ("leg_winner", {"leg": leg_num})

        # ----- Will Match Go To Final Set (deciding set) -----
        if market_slug == "darts_final_set" or market_slug == "darts_go_to_final_set":
            return ("darts_final_set", {})

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
        # For winner markets (home/away)
        if market_slug in ("darts_winner", "darts_match_winner", "soccer_winner", "soccer_"):
            mapping = {"1": "home", "2": "away"}
            return mapping.get(outcome_key, outcome_key)

        # For set/leg winner markets
        if "winner" in market_slug and outcome_key in ("1", "2"):
            return "home" if outcome_key == "1" else "away"

        # For highest checkout, outcome keys are player names or "draw"
        # Keep as is

        # For the first 180 market, outcome keys are player names
        return outcome_key


# Generic dispatcher
def get_od_darts_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    return OdibetDartsMapper.get_market_info(market_slug)