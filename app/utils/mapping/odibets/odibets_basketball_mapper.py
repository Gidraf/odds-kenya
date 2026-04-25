"""
app/workers/mappers/odibets_basketball_mapper.py
=================================================
OdiBets Basketball market mapper – maps sub_type_id + specifiers to canonical slugs.
"""

from typing import Dict

class OdiBetsBasketballMapper:
    """Maps OdiBets Basketball market IDs and specifiers to internal canonical slugs."""

    STATIC_MARKETS: Dict[str, str] = {
        "1":     "1x2",                       # 1X2 (3-way)
        "11":    "draw_no_bet",               # Draw no bet
        "47":    "basketball_ht_ft",          # Halftime/fulltime
        "60":    "first_half_1x2",            # 1st half - 1x2
        "64":    "first_half_draw_no_bet",    # 1st half - draw no bet
        "74":    "first_half_odd_even",       # 1st half - odd/even
        "83":    "second_half_1x2",           # 2nd half - 1x2
        "86":    "second_half_draw_no_bet",   # 2nd half - draw no bet
        "94":    "second_half_odd_even",      # 2nd half - odd/even
        "219":   "basketball_moneyline",      # Winner (incl. overtime) -> 2-way
        "220":   "overtime",                  # Will there be overtime
        "229":   "odd_even",                  # Odd/even (incl. overtime)
        "234":   "highest_scoring_quarter",   # Highest scoring quarter
        "298":   "point_range",               # Point range (variant=sr:point)
        "304":   "quarter_odd_even",          # {!quarternr} quarter - odd/even
    }

    @staticmethod
    def format_line(spec_value: str) -> str:
        """Format a specifier value to a canonical line string."""
        if spec_value == "0":
            return "0_0"
        val_str = spec_value.replace(".", "_")
        if spec_value.startswith("-"):
            val_str = "minus_" + val_str[1:]
        return val_str

    @classmethod
    def get_market_slug(cls, sub_type_id: str, specifiers: Dict[str, str], market_name: str = "") -> str | None:
        """
        Returns the canonical market slug.
        Args:
            sub_type_id: OdiBets sub_type_id (as string)
            specifiers: dict of parsed specifiers (e.g., {"total": "210.5", "hcp": "10.5"})
            market_name: fallback name (not used)
        """
        sid = str(sub_type_id)

        # Static markets
        if sid in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sid]

        # Euro Handicap (sub_type_id 14)
        if sid == "14":
            hcp = specifiers.get("hcp")
            if hcp:
                # format "8:0" → "8_0"
                line = hcp.replace(":", "_")
                return f"european_handicap_{line}"
            return "european_handicap"

        # Over/Under (incl. overtime) – sub_type_id 18 (full match) and 90 (2nd half)
        if sid in ("18", "90"):
            total = specifiers.get("total")
            if total:
                line = cls.format_line(total)
                if sid == "18":
                    return f"total_points_{line}"
                else:  # 2nd half total
                    return f"second_half_total_points_{line}"
            return "total_points" if sid == "18" else "second_half_total_points"

        # Handicap (incl. overtime) – sub_type_id 223 (full match)
        if sid == "223":
            hcp = specifiers.get("hcp")
            if hcp:
                line = cls.format_line(hcp)
                return f"point_spread_{line}"
            return "point_spread"

        # Team totals (incl. overtime) – sub_type_id 228 for away team (competitor2)
        if sid == "228":
            total = specifiers.get("total")
            if total:
                line = cls.format_line(total)
                return f"away_total_points_{line}"
            return "away_total_points"

        # Quarter markets – sub_type_id 235 (1x2), 236 (total), 301 (winning margin),
        # 302 (draw no bet), 303 (handicap)
        if sid == "235":
            quarternr = specifiers.get("quarternr")
            if quarternr:
                return f"quarter_{quarternr}_winner"
            return "quarter_winner"
        if sid == "236":
            quarternr = specifiers.get("quarternr")
            total = specifiers.get("total")
            if total and quarternr:
                line = cls.format_line(total)
                return f"quarter_{quarternr}_total_{line}"
            return "quarter_total"
        if sid == "301":
            quarternr = specifiers.get("quarternr")
            if quarternr:
                return f"quarter_{quarternr}_winning_margin"
            return "quarter_winning_margin"
        if sid == "302":
            quarternr = specifiers.get("quarternr")
            if quarternr:
                return f"quarter_{quarternr}_draw_no_bet"
            return "quarter_draw_no_bet"
        if sid == "303":
            quarternr = specifiers.get("quarternr")
            hcp = specifiers.get("hcp")
            if hcp and quarternr:
                line = cls.format_line(hcp)
                return f"quarter_{quarternr}_spread_{line}"
            return "quarter_spread"

        # 1st half handicap – sub_type_id 66
        if sid == "66":
            hcp = specifiers.get("hcp")
            if hcp:
                line = cls.format_line(hcp)
                return f"first_half_spread_{line}"
            return "first_half_spread"

        # 2nd half handicap – sub_type_id 88
        if sid == "88":
            hcp = specifiers.get("hcp")
            if hcp:
                line = cls.format_line(hcp)
                return f"second_half_spread_{line}"
            return "second_half_spread"

        # Winner & total combo – sub_type_id 292
        if sid == "292":
            total = specifiers.get("total")
            if total:
                line = cls.format_line(total)
                return f"winner_and_total_{line}"
            return "winner_and_total"

        # 2nd half 1x2 (incl. overtime) – sub_type_id 293
        if sid == "293":
            return "second_half_1x2_ot"

        # Unknown market
        return None