"""
app/workers/mappers/odibet_basketball.py
=========================================
OdiBets Basketball market mapper.
Converts OdiBets-specific market slugs (as produced by od_harvester.py)
into canonical market slugs + specifiers for internal use.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetBasketballMapper:
    """Maps OdiBets Basketball JSON market slugs to canonical slugs + specifiers."""

    # Direct mapping for simple markets (no specifiers)
    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "basketball_1x2":               ("1x2", {"period": "full"}),
        "basketball_moneyline":         ("basketball_moneyline", {"period": "full"}),
        "basketball_draw_no_bet":       ("draw_no_bet", {"period": "full"}),
        "basketball_ht_ft":             ("ht_ft", {"period": "full"}),
        "first_half_basketball_1x2":    ("first_half_1x2", {"period": "first_half"}),
        "basketball_":                  None,  # ambiguous, skip
        "first_quarter_winning_margin": ("quarter_winning_margin", {"quarter": "1"}),
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
        Specifiers may include 'line', 'handicap', 'period', 'quarter', 'team', 'include_ot'.

        Example:
            "over_under_basketball_points_215_5" → ("total_points", {"line": "215.5", "period": "full"})
            "basketball__minus_10_5"             → ("point_spread", {"handicap": "-10.5", "period": "full"})
            "first_half_basketball_spread_minus_7_5" → ("point_spread", {"handicap": "-7.5", "period": "first_half"})
            "basketball__1"                      → ("quarter_winner", {"quarter": "1"})
        """
        # ----- Static mappings -----
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # ----- Full Game Total Points (Over/Under) -----
        # Patterns:
        #   over_under_basketball_points_215_5
        #   over_under_basketball_points_incl_ot_215_5
        total_match = re.match(r"over_under_basketball_points(?:_incl_ot)?_([\d_]+)$", market_slug)
        if total_match:
            include_ot = "_incl_ot" in market_slug
            line = float(total_match.group(1).replace("_", "."))
            spec = {"line": str(line), "period": "full"}
            if include_ot:
                spec["include_ot"] = "true"
            return ("total_points", spec)

        # ----- Full Game Point Spread (Handicap) -----
        # Patterns:
        #   basketball__minus_10_5   (handicap -10.5)
        #   basketball__minus_1_5
        #   basketball__minus_0_5
        # Also basketball__minus_3_5 etc.
        spread_match = re.match(r"basketball__minus_([\d_]+)$", market_slug)
        if spread_match:
            handicap_str = spread_match.group(1).replace("_", ".")
            handicap = -float(handicap_str)  # negative because "minus"
            return ("point_spread", {"handicap": str(handicap), "period": "full"})

        # Also possible positive handicap? Not seen but could be "basketball__plus_"
        plus_spread_match = re.match(r"basketball__plus_([\d_]+)$", market_slug)
        if plus_spread_match:
            handicap_str = plus_spread_match.group(1).replace("_", ".")
            handicap = float(handicap_str)
            return ("point_spread", {"handicap": str(handicap), "period": "full"})

        # ----- First Half Point Spread -----
        fh_spread_match = re.match(r"first_half_basketball_spread_minus_([\d_]+)$", market_slug)
        if fh_spread_match:
            handicap_str = fh_spread_match.group(1).replace("_", ".")
            handicap = -float(handicap_str)
            return ("point_spread", {"handicap": str(handicap), "period": "first_half"})

        # ----- Quarter Winner (1X2) -----
        # Patterns: basketball__1, basketball__2, basketball__3, basketball__4
        quarter_match = re.match(r"basketball__([1-4])$", market_slug)
        if quarter_match:
            quarter_num = quarter_match.group(1)
            return ("quarter_winner", {"quarter": quarter_num})

        # ----- Team Totals (including overtime) -----
        # Home team: basketball_home_team_total_incl_ot_112_5
        home_total_match = re.match(r"basketball_home_team_total_incl_ot_([\d_]+)$", market_slug)
        if home_total_match:
            line = float(home_total_match.group(1).replace("_", "."))
            return ("team_total_points", {"team": "home", "line": str(line), "period": "full", "include_ot": "true"})

        # Away team total
        away_total_match = re.match(r"basketball_away_team_total_incl_ot_([\d_]+)$", market_slug)
        if away_total_match:
            line = float(away_total_match.group(1).replace("_", "."))
            return ("team_total_points", {"team": "away", "line": str(line), "period": "full", "include_ot": "true"})

        # ----- Combined Market: Moneyline + Total -----
        # Pattern: basketball_moneyline_and_total_215_5
        combo_match = re.match(r"basketball_moneyline_and_total_([\d_]+)$", market_slug)
        if combo_match:
            line = float(combo_match.group(1).replace("_", "."))
            # This market has outcomes like "1_under", "1_over", "2_under", "2_over"
            # We'll return a special slug; the actual canonicalization will split.
            return ("moneyline_and_total", {"line": str(line), "period": "full"})

        # ----- Points Range Market (e.g., "basketball__250_5") -----
        # Pattern: basketball__250_5  (could be any number)
        range_match = re.match(r"basketball__([\d_]+)$", market_slug)
        if range_match and "_" in range_match.group(1):
            # This is likely the points range market; outcomes are bands like "151_160", "161_170", etc.
            # We'll map to a generic "total_points_range" market.
            return ("total_points_range", {"period": "full"})

        # ----- Unknown market -----
        return None

    @classmethod
    def get_canonical_slug(cls, market_slug: str) -> Optional[str]:
        """Return just the canonical slug (without specifiers)."""
        info = cls.get_market_info(market_slug)
        return info[0] if info else None


# Optional: generic dispatcher for all sports
def get_od_basketball_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    return OdibetBasketballMapper.get_market_info(market_slug)