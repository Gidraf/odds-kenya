"""
app/workers/mappers/odibet_soccer.py
=====================================
OdiBets Soccer market mapper.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetsSoccerMapper:
    """Maps OdiBets Soccer JSON market slugs to canonical slugs + specifiers."""

    # Direct mapping for simple markets (no specifiers)
    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "1x2":                         ("1x2", {"period": "match"}),
        "soccer_1x2":                  ("1x2", {"period": "match"}),
        "double_chance":               ("double_chance", {"period": "match"}),
        "draw_no_bet":                 ("draw_no_bet", {"period": "match"}),
        "btts":                        ("btts", {"period": "match"}),
        "first_half_btts":             ("btts", {"period": "first_half"}),
        "ht_ft":                       ("ht_ft", {"period": "match"}),
        "first_half_1x2":              ("first_half_1x2", {"period": "first_half"}),
        "first_half_double_chance":    ("double_chance", {"period": "first_half"}),
        "first_team_to_score_1":       ("first_team_to_score", {}),
        "multigoals":                  ("multigoals", {}),
        "exact_goals_6":               ("exact_goals", {}),           # number appended, but we treat as same
        "anytime_goalscorer":          ("anytime_goalscorer", {}),
        "first_goalscorer_1":          ("first_goalscorer", {}),
        "last_goalscorer":             ("last_goalscorer", {}),
    }

    @classmethod
    def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
        """
        Parse an OdiBets market slug and return (canonical_slug, specifiers).
        """
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # ----- Over/Under Total Goals (full match) -----
        ou_match = re.match(r"over_under_goals_([\d_]+)$", market_slug)
        if ou_match:
            line = float(ou_match.group(1).replace("_", "."))
            return ("over_under_goals", {"line": str(line), "period": "match"})

        # ----- Over/Under Total Goals (first half) -----
        fh_ou = re.match(r"first_half_over_under_goals_([\d_]+)$", market_slug)
        if fh_ou:
            line = float(fh_ou.group(1).replace("_", "."))
            return ("over_under_goals", {"line": str(line), "period": "first_half"})

        # ----- Over/Under BTTS combined (e.g., over_under_btts_2_5) -----
        ou_btts = re.match(r"over_under_btts_([\d_]+)$", market_slug)
        if ou_btts:
            line = float(ou_btts.group(1).replace("_", "."))
            return ("btts_and_total", {"line": str(line), "period": "match"})

        # ----- European Handicap (3-way) -----
        # Patterns: european_handicap_0_1, european_handicap_1_0, etc.
        euro_hcp = re.match(r"european_handicap_([\d_]+)$", market_slug)
        if euro_hcp:
            hcp_str = euro_hcp.group(1).replace("_", ".")
            return ("european_handicap", {"handicap": hcp_str})

        # ----- Asian Handicap (2-way) -----
        # Positive: asian_handicap_0_5
        asian = re.match(r"asian_handicap_([\d_]+)$", market_slug)
        if asian:
            hcp = float(asian.group(1).replace("_", "."))
            return ("asian_handicap", {"handicap": str(hcp)})
        # Negative: asian_handicap_minus_1_5
        asian_minus = re.match(r"asian_handicap_minus_([\d_]+)$", market_slug)
        if asian_minus:
            hcp = -float(asian_minus.group(1).replace("_", "."))
            return ("asian_handicap", {"handicap": str(hcp)})

        # ----- First half Asian Handicap -----
        fh_asian = re.match(r"first_half_asian_handicap_minus_([\d_]+)$", market_slug)
        if fh_asian:
            hcp = -float(fh_asian.group(1).replace("_", "."))
            return ("asian_handicap", {"handicap": str(hcp), "period": "first_half"})
        fh_asian_pos = re.match(r"first_half_asian_handicap_([\d_]+)$", market_slug)
        if fh_asian_pos:
            hcp = float(fh_asian_pos.group(1).replace("_", "."))
            return ("asian_handicap", {"handicap": str(hcp), "period": "first_half"})

        # ----- Team Totals (home/away) -----
        home_total = re.match(r"home_over_under_([\d_]+)$", market_slug)
        if home_total:
            line = float(home_total.group(1).replace("_", "."))
            return ("team_total_goals", {"team": "home", "line": str(line), "period": "match"})
        away_total = re.match(r"away_over_under_([\d_]+)$", market_slug)
        if away_total:
            line = float(away_total.group(1).replace("_", "."))
            return ("team_total_goals", {"team": "away", "line": str(line), "period": "match"})

        # ----- First half team totals -----
        fh_home_total = re.match(r"first_half_home_over_under_([\d_]+)$", market_slug)
        if fh_home_total:
            line = float(fh_home_total.group(1).replace("_", "."))
            return ("team_total_goals", {"team": "home", "line": str(line), "period": "first_half"})
        fh_away_total = re.match(r"first_half_away_over_under_([\d_]+)$", market_slug)
        if fh_away_total:
            line = float(fh_away_total.group(1).replace("_", "."))
            return ("team_total_goals", {"team": "away", "line": str(line), "period": "first_half"})

        # ----- Winning Margin (banded) -----
        win_margin = re.match(r"winning_margin_(\d+)$", market_slug)
        if win_margin:
            band = win_margin.group(1)
            return ("winning_margin", {"band_max": band})

        # ----- Exact Goals (number of goals in match) -----
        exact_goals = re.match(r"exact_goals_(\d+)$", market_slug)
        if exact_goals:
            # The number is often the max goal, but we store as spec
            max_goals = exact_goals.group(1)
            return ("exact_goals", {"max_goals": max_goals})

        # ----- First half Exact Goals -----
        fh_exact = re.match(r"first_half_exact_goals_(\d+)$", market_slug)
        if fh_exact:
            max_goals = fh_exact.group(1)
            return ("exact_goals", {"period": "first_half", "max_goals": max_goals})

        # ----- Goals time band (e.g., soccer__1, soccer__1_15, etc.) -----
        # Pattern: soccer__1 or soccer__1_15 (time band) 
        time_band = re.match(r"soccer__(\d+)(?:_(\d+))?$", market_slug)
        if time_band:
            # This market has outcomes like "1_10", "11_20", etc. So we treat as goal_time_band
            band_type = time_band.group(1)  # e.g., "1" might indicate something, but we ignore
            return ("goal_time_band", {})

        # ----- Combination market: 1x2 + Over/Under -----
        combo_1x2_ou = re.match(r"1x2_over_under_([\d_]+)$", market_slug)
        if combo_1x2_ou:
            line = float(combo_1x2_ou.group(1).replace("_", "."))
            return ("result_and_total", {"line": str(line)})

        # ----- Double Chance + Over/Under -----
        dc_ou = re.match(r"double_chance_over_under_([\d_]+)$", market_slug)
        if dc_ou:
            line = float(dc_ou.group(1).replace("_", "."))
            return ("double_chance_and_total", {"line": str(line)})

        # ----- HT/FT + Over/Under -----
        htft_ou = re.match(r"ht_ft_over_under_([\d_]+)$", market_slug)
        if htft_ou:
            line = float(htft_ou.group(1).replace("_", "."))
            return ("ht_ft_and_total", {"line": str(line)})

        # ----- First half European Handicap -----
        fh_euro = re.match(r"first_half_european_handicap_([\d_]+)$", market_slug)
        if fh_euro:
            hcp_str = fh_euro.group(1).replace("_", ".")
            return ("european_handicap", {"handicap": hcp_str, "period": "first_half"})

        # ----- First half Double Chance + BTTS -----
        if market_slug == "first_half_double_chance_btts":
            return ("double_chance_and_btts", {"period": "first_half"})

        # ----- Second half 1x2 + BTTS / Over/Under (similar patterns) -----
        sh_1x2_btts = re.match(r"second_half_1x2_btts$", market_slug)
        if sh_1x2_btts:
            return ("result_and_btts", {"period": "second_half"})

        sh_1x2_ou = re.match(r"second_half_1x2_over_under_([\d_]+)$", market_slug)
        if sh_1x2_ou:
            line = float(sh_1x2_ou.group(1).replace("_", "."))
            return ("result_and_total", {"period": "second_half", "line": str(line)})

        # ----- 1x2 + BTTS -----
        if market_slug == "1x2_btts":
            return ("result_and_btts", {"period": "match"})

        # ----- Double Chance + BTTS -----
        if market_slug == "double_chance_btts":
            return ("double_chance_and_btts", {"period": "match"})

        # ----- Generic "soccer__" catch-all market (ambiguous) – skip -----
        if market_slug == "soccer_":
            return None

        # ----- Unknown slug – log or return None -----
        return None

    @classmethod
    def get_canonical_slug(cls, market_slug: str) -> Optional[str]:
        info = cls.get_market_info(market_slug)
        return info[0] if info else None

    @classmethod
    def transform_outcome(cls, market_slug: str, outcome_key: str) -> str:
        """
        Convert outcome keys into canonical names.
        """
        # 1X2 markets
        if market_slug in ("1x2", "soccer_1x2", "first_half_1x2"):
            return {"1": "home", "X": "draw", "2": "away"}.get(outcome_key, outcome_key)

        # Double chance
        if market_slug in ("double_chance", "first_half_double_chance"):
            return {
                "1_or_x": "home_or_draw",
                "1_or_2": "home_or_away",
                "x_or_2": "draw_or_away",
            }.get(outcome_key, outcome_key)

        # Draw no bet
        if market_slug == "draw_no_bet":
            return "home" if outcome_key == "1" else "away"

        # Asian handicap / European handicap (2-way or 3-way)
        if any(x in market_slug for x in ("asian_handicap", "european_handicap")):
            if market_slug.startswith("european_"):
                # 3-way: home/draw/away
                return {"1": "home", "X": "draw", "2": "away"}.get(outcome_key, outcome_key)
            else:
                # 2-way
                return "home" if outcome_key == "1" else "away"

        # Over/under outcomes
        if "over_under" in market_slug:
            return outcome_key  # "over" or "under"

        # BTTS
        if market_slug in ("btts", "first_half_btts"):
            return outcome_key  # "yes"/"no"

        # First team to score / first goal
        if market_slug == "first_team_to_score_1":
            return {"1": "home", "2": "away", "none": "none"}.get(outcome_key, outcome_key)

        # HT/FT outcomes like "1/1", "X/2"
        if market_slug == "ht_ft":
            return outcome_key

        # Winning margin outcomes like "1_by_1", "1_by_2", "1_by_3+", "X"
        if market_slug.startswith("winning_margin"):
            return outcome_key

        # Exact goals: keys like "0", "1", "2", "3", "4", "5", "6+"
        if market_slug.startswith("exact_goals") or market_slug.startswith("first_half_exact_goals"):
            return outcome_key

        # Multigoals: keys like "none", "1_2", "1_3", etc.
        if market_slug == "multigoals":
            return outcome_key

        # Goal time band: outcomes like "1_10", "11_20", "16_30", "1_goal_1", etc.
        if market_slug.startswith("soccer__"):
            # Many possible outcome formats – keep as is
            return outcome_key

        # Combos: result+total, result+btts, etc. – outcomes like "1_under", "X_over", "1_yes"
        if any(x in market_slug for x in ("1x2_over_under", "double_chance_over_under", "ht_ft_over_under",
                                          "second_half_1x2_over_under", "1x2_btts", "double_chance_btts",
                                          "first_half_double_chance_btts", "second_half_1x2_btts")):
            return outcome_key

        # Team totals
        if any(x in market_slug for x in ("home_over_under", "away_over_under", "first_half_home_over_under", "first_half_away_over_under")):
            return outcome_key  # "over"/"under"

        # Player markets: keep outcome keys as they are (player names or "none")
        if market_slug in ("anytime_goalscorer", "first_goalscorer_1", "last_goalscorer"):
            return outcome_key

        # Fallback
        return outcome_key


def get_od_soccer_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    return OdibetsSoccerMapper.get_market_info(market_slug)