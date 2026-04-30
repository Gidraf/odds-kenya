"""
app/workers/mappers/odibet.py
==============================
OdiBets Baseball market mapper.
Converts OdiBets-specific market slugs (as produced by od_harvester.py)
into canonical market slugs + specifiers for internal use.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetsBaseballMapper:
    """Maps OdiBets Baseball JSON market slugs to canonical slugs + specifiers."""

    # Direct mapping for simple markets (no specifiers)
    STATIC_MARKETS: Dict[str, str] = {
        "baseball_1x2":          "1x2",                      # 3-way match result (draw exists)
        "baseball_moneyline":    "baseball_moneyline",       # 2-way winner (no draw)
        "baseball_odd_even":     "odd_even",                 # Odd/Even total runs
        "baseball_f5_1x2":       "f5_winner",                # Winner after 5 innings (3-way)
        "baseball_f5_spread_0_0": "f5_winner",               # Actually F5 winner? No spread? We'll map
        "baseball_1st_inning_1x2_1": "first_inning_score",   # 1st inning 1X2
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
        Specifiers may include 'line', 'handicap', 'period', 'team'.

        Example:
            "over_under_baseball_runs_7_5" -> ("total_runs", {"line": "7.5", "period": "full"})
            "baseball_spread_minus_1_5"    -> ("run_line", {"handicap": "-1.5", "period": "full"})
            "baseball_f5_spread_0_5"       -> ("run_line", {"handicap": "0.5", "period": "f5"})
        """
        # ----- 1X2 (full game) -----
        if market_slug == "baseball_1x2":
            return ("1x2", {"period": "full"})

        # ----- Moneyline (full game) -----
        if market_slug == "baseball_moneyline":
            return ("baseball_moneyline", {"period": "full"})

        # ----- Odd/Even (full game) -----
        if market_slug == "baseball_odd_even":
            return ("odd_even", {"period": "full"})

        # ----- Full Game Run Line (Spread) -----
        # Patterns: baseball_spread_1_5  → handicap -1.5
        #           baseball_spread_minus_0_5 → -0.5
        spread_match = re.match(r"baseball_spread_([\d_]+)$", market_slug)
        if spread_match:
            handicap_str = spread_match.group(1).replace("_", ".")
            if handicap_str.startswith("minus"):
                handicap = -float(handicap_str[5:])
            else:
                handicap = float(handicap_str)
            return ("run_line", {"handicap": str(handicap), "period": "full"})

        # ----- Full Game Total Runs (Over/Under) -----
        total_match = re.match(r"over_under_baseball_runs_([\d_]+)$", market_slug)
        if total_match:
            line = float(total_match.group(1).replace("_", "."))
            return ("total_runs", {"line": str(line), "period": "full"})

        # ----- Home Team Total Runs -----
        home_total_match = re.match(r"baseball_home_team_total_([\d_]+)$", market_slug)
        if home_total_match:
            line = float(home_total_match.group(1).replace("_", "."))
            return ("team_total_runs", {"team": "home", "line": str(line), "period": "full"})

        # ----- Away Team Total Runs -----
        away_total_match = re.match(r"baseball_away_team_total_([\d_]+)$", market_slug)
        if away_total_match:
            line = float(away_total_match.group(1).replace("_", "."))
            return ("team_total_runs", {"team": "away", "line": str(line), "period": "full"})

        # ----- First 5 Innings Winner (already 3-way) -----
        if market_slug == "baseball_f5_1x2":
            return ("f5_winner", {"period": "f5"})

        # ----- First 5 Innings Run Line (Spread) -----
        f5_spread_match = re.match(r"baseball_f5_spread_([\d_]+)$", market_slug)
        if f5_spread_match:
            handicap_str = f5_spread_match.group(1).replace("_", ".")
            if handicap_str.startswith("minus"):
                handicap = -float(handicap_str[5:])
            else:
                handicap = float(handicap_str)
            return ("run_line", {"handicap": str(handicap), "period": "f5"})

        # ----- First 5 Innings Total Runs -----
        f5_total_match = re.match(r"over_under_baseball_f5_runs_([\d_]+)$", market_slug)
        if f5_total_match:
            line = float(f5_total_match.group(1).replace("_", "."))
            return ("total_runs", {"line": str(line), "period": "f5"})

        # ----- 1st Inning 1X2 -----
        inning_1x2_match = re.match(r"baseball_(\d+)(?:st|nd|rd|th)_inning_1x2_\d+$", market_slug)
        if inning_1x2_match:
            inning_num = inning_1x2_match.group(1)
            return ("first_inning_score" if inning_num == "1" else "inning_winner",
                    {"inning": inning_num})

        # ----- 1st Inning Total Runs -----
        inning_total_match = re.match(r"over_under_baseball_(\d+)(?:st|nd|rd|th)_inning_runs_([\d_]+)$", market_slug)
        if inning_total_match:
            inning_num = inning_total_match.group(1)
            line = float(inning_total_match.group(2).replace("_", "."))
            return ("inning_total_runs", {"inning": inning_num, "line": str(line)})

        # ----- Unknown market -----
        return None

    @classmethod
    def get_canonical_slug(cls, market_slug: str) -> Optional[str]:
        """Return just the canonical slug (without specifiers)."""
        info = cls.get_market_info(market_slug)
        return info[0] if info else None


# Optional: a generic function that dispatches by sport
def get_od_market_info(sport: str, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    """Dispatch to sport-specific mapper."""
    if sport == "baseball":
        return OdibetBaseballMapper.get_market_info(market_slug)
    # Add other sports here (soccer, basketball, etc.)
    return None