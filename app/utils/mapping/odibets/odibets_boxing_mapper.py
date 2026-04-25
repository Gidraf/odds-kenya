"""
app/workers/mappers/odibets_boxing_mapper.py
=============================================
OdiBets Boxing market mapper – maps sub_type_id + specifiers to canonical slugs.
"""

from typing import Dict

class OdiBetsBoxingMapper:
    """Maps OdiBets Boxing market IDs and specifiers to internal canonical slugs."""

    STATIC_MARKETS: Dict[str, str] = {
        "1":   "boxing_1x2",          # 1X2 (3-way)
        "186": "boxing_winner",       # Winner (2-way)
        "911": "knockdown_scored",    # Will there be a knockdown
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
            specifiers: dict of parsed specifiers (e.g., {"total": "7.5"})
            market_name: fallback name (not used)
        """
        sid = str(sub_type_id)

        # Static markets
        if sid in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sid]

        # Over/Under Rounds – sub_type_id 18
        if sid == "18":
            total = specifiers.get("total")
            if total:
                line = cls.format_line(total)
                return f"over_under_rounds_{line}"
            return "over_under_rounds"

        # Method of victory – sub_type_id 910
        if sid == "910":
            # The specifiers contain "variant=sr:winning_method:ko_decision"
            # We return a static slug because outcomes are parsed separately.
            return "method_of_victory"

        # Winner & exact round – sub_type_id 912
        if sid == "912":
            # Outcomes contain details like "1 & 5", "2 & 12", etc.
            # We return a static slug; the actual round is part of the outcome name.
            return "round_betting"

        # Winner & round range – sub_type_id 913
        if sid == "913":
            # Round ranges like 1-3, 4-6, 7-9, 10-12, plus decision
            return "winner_and_round_range"

        # Unknown market – fallback to generic
        return None