"""
app/workers/mappers/odibet_ice_hockey.py
=========================================
OdiBets Ice Hockey market mapper.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetIceHockeyMapper:
    """Maps OdiBets Ice Hockey JSON market slugs to canonical slugs + specifiers."""

    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "ice-hockey_1x2":           ("1x2", {"period": "match"}),
        "hockey_double_chance":     ("double_chance", {"period": "match"}),
        "hockey_btts":              ("btts", {"period": "match"}),
        "hockey_draw_no_bet":       ("draw_no_bet", {"period": "match"}),
        "hockey_moneyline":         ("hockey_moneyline", {"period": "match"}),
        "hockey_":                  None,  # ambiguous generic slug - skip
    }

    @classmethod
    def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
        """
        Parse an OdiBets market slug and return (canonical_slug, specifiers).
        """
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # ----- First goal scorer -----
        if market_slug == "hockey_first_goal_1":
            return ("first_goal", {})

        # ----- European Handicap (3-way) -----
        # Patterns: hockey_european_handicap_0_1, hockey_european_handicap_1_0, etc.
        euro_hcp = re.match(r"hockey_european_handicap_([\d_]+)$", market_slug)
        if euro_hcp:
            hcp_str = euro_hcp.group(1).replace("_", ".")
            return ("european_handicap", {"handicap": hcp_str})

        # ----- Asian Handicap (2-way) -----
        # Positive: hockey_asian_handicap_0_5
        asian_hcp = re.match(r"hockey_asian_handicap_([\d_]+)$", market_slug)
        if asian_hcp:
            hcp = float(asian_hcp.group(1).replace("_", "."))
            return ("asian_handicap", {"handicap": str(hcp)})
        # Negative: hockey_asian_handicap_minus_1_5
        asian_minus = re.match(r"hockey_asian_handicap_minus_([\d_]+)$", market_slug)
        if asian_minus:
            hcp = -float(asian_minus.group(1).replace("_", "."))
            return ("asian_handicap", {"handicap": str(hcp)})

        # ----- Over/Under goals (full match) -----
        ou_match = re.match(r"over_under_hockey_goals_([\d_]+)$", market_slug)
        if ou_match:
            line = float(ou_match.group(1).replace("_", "."))
            return ("total_goals", {"line": str(line), "period": "match"})

        # ----- Team totals (home/away) -----
        home_total = re.match(r"hockey_home_team_total_([\d_]+)$", market_slug)
        if home_total:
            line = float(home_total.group(1).replace("_", "."))
            return ("team_total_goals", {"team": "home", "line": str(line), "period": "match"})

        away_total = re.match(r"hockey_away_team_total_([\d_]+)$", market_slug)
        if away_total:
            line = float(away_total.group(1).replace("_", "."))
            return ("team_total_goals", {"team": "away", "line": str(line), "period": "match"})

        # ----- Winning margin (banded) -----
        # Patterns: hockey_winning_margin_3 (maybe 3 goals band)
        win_margin = re.match(r"hockey_winning_margin_(\d+)$", market_slug)
        if win_margin:
            band = win_margin.group(1)
            return ("winning_margin", {"band": band})

        # ----- Correct score market -----
        correct_score = re.match(r"hockey_correct_score_(\d+)$", market_slug)
        if correct_score:
            # The number might indicate max goals? We'll just set a generic spec
            return ("correct_score", {})

        # ----- Highest scoring period -----
        if market_slug == "hockey_highest_scoring_period":
            return ("highest_scoring_period", {})

        # ----- Combined 1x2 + Over/Under -----
        combo = re.match(r"hockey_1x2_over_under_([\d_]+)$", market_slug)
        if combo:
            line = float(combo.group(1).replace("_", "."))
            return ("result_and_total", {"line": str(line)})

        # ----- Fallback: maybe just a spread (e.g., hockey__minus_0_5) -----
        spread_fallback = re.match(r"hockey__minus_([\d_]+)$", market_slug)
        if spread_fallback:
            hcp = -float(spread_fallback.group(1).replace("_", "."))
            return ("asian_handicap", {"handicap": str(hcp)})

        # ----- Unknown -----
        return None

    @classmethod
    def get_canonical_slug(cls, market_slug: str) -> Optional[str]:
        info = cls.get_market_info(market_slug)
        return info[0] if info else None

    @classmethod
    def transform_outcome(cls, market_slug: str, outcome_key: str) -> str:
        # 1X2 outcomes
        if market_slug in ("ice-hockey_1x2", "hockey_1x2"):
            return {"1": "home", "X": "draw", "2": "away"}.get(outcome_key, outcome_key)
        # Moneyline / draw no bet / asian handicap (2-way)
        if market_slug in ("hockey_moneyline", "hockey_draw_no_bet") or market_slug.startswith("hockey_asian_handicap"):
            return "home" if outcome_key == "1" else "away"
        # Double chance
        if market_slug == "hockey_double_chance":
            return {
                "1_or_x": "home_or_draw",
                "1_or_2": "home_or_away",
                "x_or_2": "draw_or_away"
            }.get(outcome_key, outcome_key)
        # BTTS
        if market_slug == "hockey_btts":
            return outcome_key  # "yes"/"no"
        # First goal
        if market_slug == "hockey_first_goal_1":
            return {"1": "home", "2": "away", "none": "none"}.get(outcome_key, outcome_key)
        # European handicap (3-way)
        if market_slug.startswith("hockey_european_handicap"):
            return {"1": "home", "X": "draw", "2": "away"}.get(outcome_key, outcome_key)
        # Winning margin outcomes like "1_by_3+", "2_by_1", "X" etc.
        if market_slug.startswith("hockey_winning_margin"):
            return outcome_key
        # Correct score outcomes like "3:2"
        if market_slug.startswith("hockey_correct_score"):
            return outcome_key
        # Over/under
        if market_slug.startswith("over_under"):
            return outcome_key  # "over"/"under"
        # Team totals
        if market_slug.startswith(("hockey_home_team_total", "hockey_away_team_total")):
            return outcome_key
        # Highest scoring period: "1st_period", "2nd_period", etc.
        if market_slug == "hockey_highest_scoring_period":
            return outcome_key
        # Combination market (result + over/under) outcomes like "1_over", "X_under"
        if market_slug.startswith("hockey_1x2_over_under"):
            return outcome_key
        # Generic slug "hockey__1" might contain multiple outcome types; we'll keep as is
        return outcome_key


def get_od_ice_hockey_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    return OdibetIceHockeyMapper.get_market_info(market_slug)