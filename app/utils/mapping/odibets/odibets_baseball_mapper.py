"""
app/workers/mappers/odibets_baseball_mapper.py
===============================================
OdiBets Baseball market mapper – maps sub_type_id + specifiers to canonical slugs.
"""

from typing import Dict

class OdiBetsBaseballMapper:
    """Maps OdiBets Baseball market IDs and specifiers to internal canonical slugs."""

    STATIC_MARKETS: Dict[str, str] = {
        "1":   "baseball_1x2",              # 1X2 (3-way)
        "251": "baseball_moneyline",        # Winner (incl. extra innings) -> 2-way
        "264": "odd_even",                  # Odd/even (incl. extra innings)
        "268": "extra_innings",             # Will there be an extra inning
        "274": "f5_winner",                 # Innings 1 to 5 - 1x2
        "276": "f5_total",                  # Innings 1 to 5 - total (added)
        "287": "first_inning_winner",       # Inning 1 - 1x2
    }

    @staticmethod
    def format_line(spec_value: str) -> str:
        """Format a specifier value to a canonical line string."""
        if spec_value == "0":
            return "0_0"
        # Replace '.' with '_', handle negative numbers
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
            specifiers: dict of parsed specifiers (e.g., {"total": "6.5", "hcp": "1.5"})
            market_name: fallback name (not used here, kept for consistency)
        """
        sid = str(sub_type_id)

        # Static markets
        if sid in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sid]

        # Handicap / Run Line (sub_type_id 256)
        if sid == "256":
            hcp = specifiers.get("hcp")
            if hcp:
                line = cls.format_line(hcp)
                return f"run_line_{line}"
            return "run_line"

        # Total runs (sub_type_id 258)
        if sid == "258":
            total = specifiers.get("total")
            if total:
                line = cls.format_line(total)
                return f"total_runs_{line}"
            return "total_runs"

        # Team totals (sub_type_id 260 for home/competitor1, 261 for away/competitor2)
        if sid == "260":
            total = specifiers.get("total")
            if total:
                line = cls.format_line(total)
                return f"home_total_runs_{line}"
            return "home_total_runs"
        if sid == "261":
            total = specifiers.get("total")
            if total:
                line = cls.format_line(total)
                return f"away_total_runs_{line}"
            return "away_total_runs"

        # Innings 1 to 5 - handicap (sub_type_id 275)
        if sid == "275":
            hcp = specifiers.get("hcp")
            if hcp:
                line = cls.format_line(hcp)
                return f"f5_handicap_{line}"
            return "f5_handicap"

        # Inning total (sub_type_id 288)
        if sid == "288":
            total = specifiers.get("total")
            inning = specifiers.get("inningnr", "1")
            if total:
                line = cls.format_line(total)
                return f"inning_{inning}_total_{line}"
            return f"inning_{inning}_total"

        # Unknown market
        return None