"""
app/workers/mappers/odibets_darts_mapper.py
============================================
OdiBets Darts market mapper – maps sub_type_id + specifiers to canonical slugs.
"""

from typing import Dict

class OdiBetsDartsMapper:
    """Maps OdiBets Darts market IDs and specifiers to internal canonical slugs."""

    STATIC_MARKETS: Dict[str, str] = {
        "186": "darts_winner",          # Winner (2-way)
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
            specifiers: dict of parsed specifiers (not used here)
            market_name: fallback name (not used)
        """
        sid = str(sub_type_id)

        # Static markets
        if sid in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sid]

        # No other markets observed; can be extended later for Set/Leg betting
        return None