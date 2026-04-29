"""
app/workers/mappers/odibet_tennis.py
=====================================
OdiBets Tennis market mapper.
Converts OdiBets-specific market slugs (as produced by od_harvester.py)
into canonical market slugs + specifiers for internal use.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetTennisMapper:
    """Maps OdiBets Tennis JSON market slugs to canonical slugs + specifiers."""

    # Direct mapping for simple markets (no specifiers)
    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "tennis_match_winner": ("tennis_match_winner", {"period": "match"}),
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
            "tennis_match_winner"                → ("tennis_match_winner", {"period": "match"})
            "tennis_set_betting_2_0"             → ("set_betting", {"score": "2:0"})
            "over_under_tennis_games_22_5"       → ("total_games", {"line": "22.5", "period": "match"})
            "tennis_game_handicap_minus_1_5"     → ("games_handicap", {"handicap": "-1.5"})
            "first_set_winner"                   → ("first_set_winner", {"set": "1"})
            "first_set_match_winner"             → ("first_set_match_winner", {})
            "tennis_odd_even"                    → ("odd_even", {"period": "match"})
            "tennis_correct_score_2_1"           → ("correct_score_sets", {"score": "2:1"})
        """
        # ----- Static mappings -----
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # ----- Set betting (correct set score) -----
        # Patterns: tennis_set_betting_2_0, tennis_set_betting_2_1
        set_betting_match = re.match(r"tennis_set_betting_(\d+)_(\d+)$", market_slug)
        if set_betting_match:
            home = set_betting_match.group(1)
            away = set_betting_match.group(2)
            return ("set_betting", {"score": f"{home}:{away}"})

        # ----- Total games (over/under) -----
        ou_match = re.match(r"over_under_tennis_games_([\d_]+)$", market_slug)
        if ou_match:
            line = float(ou_match.group(1).replace("_", "."))
            return ("total_games", {"line": str(line), "period": "match"})

        # ----- Games handicap (Asian handicap on total games) -----
        # Patterns: tennis_game_handicap_minus_1_5, tennis_game_handicap_1_5
        hcp_match = re.match(r"tennis_game_handicap_minus_([\d_]+)$", market_slug)
        if hcp_match:
            hcp = -float(hcp_match.group(1).replace("_", "."))
            return ("games_handicap", {"handicap": str(hcp)})
        hcp_plus = re.match(r"tennis_game_handicap_([\d_]+)$", market_slug)
        if hcp_plus:
            hcp = float(hcp_plus.group(1).replace("_", "."))
            return ("games_handicap", {"handicap": str(hcp)})

        # ----- First set winner -----
        if market_slug == "first_set_winner":
            return ("first_set_winner", {"set": "1"})

        # ----- First set match winner combo -----
        if market_slug == "first_set_match_winner":
            return ("first_set_match_winner", {})

        # ----- Odd/Even total games -----
        if market_slug == "tennis_odd_even":
            return ("odd_even", {"period": "match"})

        # ----- Correct score (sets) legacy? -----
        if market_slug.startswith("tennis_correct_score_"):
            parts = market_slug.split("_")
            if len(parts) >= 4:
                score = f"{parts[3]}:{parts[4]}" if len(parts) == 5 else "unknown"
                return ("correct_score_sets", {"score": score})

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
        # Match winner outcomes
        if market_slug == "tennis_match_winner":
            mapping = {"1": "home", "2": "away"}
            return mapping.get(outcome_key, outcome_key)

        # Set betting outcomes (e.g., "2:0", "2:1")
        if market_slug.startswith("tennis_set_betting"):
            return outcome_key  # already like "2:0"

        # Total games over/under
        if market_slug.startswith("over_under_tennis_games"):
            return outcome_key  # "over" or "under"

        # Games handicap
        if market_slug.startswith("tennis_game_handicap"):
            return "home" if outcome_key == "1" else "away" if outcome_key == "2" else outcome_key

        # First set winner
        if market_slug == "first_set_winner":
            return "home" if outcome_key == "1" else "away"

        # First set match winner combo
        if market_slug == "first_set_match_winner":
            # outcomes like "11", "12", "21", "22"
            return outcome_key

        # Odd/even
        if market_slug == "tennis_odd_even":
            return outcome_key  # "odd" or "even"

        # Correct score (sets)
        if market_slug.startswith("tennis_correct_score"):
            return outcome_key

        return outcome_key


def get_od_tennis_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    return OdibetTennisMapper.get_market_info(market_slug)