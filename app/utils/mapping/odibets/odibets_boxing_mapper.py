"""
app/workers/mappers/odibets_boxing_mapper.py
=============================================
OdiBets Boxing market mapper – maps sub_type_id + specifiers to canonical slugs.
Also supports direct name mapping (e.g., "boxing_moneyline").
"""

from typing import Dict, Optional
import re


class OdiBetsBoxingMapper:
    """Maps OdiBets Boxing market IDs and specifiers or name strings to internal canonical slugs."""

    STATIC_MARKETS: Dict[str, str] = {
        "1":   "boxing_1x2",          # 1X2 (3-way)
        "186": "boxing_winner",       # Winner (2-way)
        "911": "knockdown_scored",    # Will there be a knockdown
    }

    # Direct name‑to‑slug mappings (for JSON where market name is the key)
    STATIC_NAME_MARKETS: Dict[str, str] = {
        "boxing_moneyline": "boxing_winner",   # OdiBets name → canonical slug
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
    def _map_by_name(cls, market_name: str) -> Optional[str]:
        """Map a market name string (e.g., from JSON keys) to a canonical slug."""
        # 1. Static name mappings
        if market_name in cls.STATIC_NAME_MARKETS:
            return cls.STATIC_NAME_MARKETS[market_name]

        # 2. Additional patterns can be added here if more name‑based markets appear
        #    (e.g., over_under_rounds_X_X, winner_in_round_X)
        return None

    @classmethod
    def get_market_slug(cls, sub_type_id: str, specifiers: Dict[str, str], market_name: str = "") -> Optional[str]:
        """
        Returns the canonical market slug.
        Args:
            sub_type_id: OdiBets sub_type_id (as string)
            specifiers: dict of parsed specifiers (e.g., {"total": "7.5"})
            market_name: optional market name (used when sub_type_id is not available)
        """
        # If a market name is given, try to map it directly (supports JSON data)
        if market_name:
            slug = cls._map_by_name(market_name)
            if slug:
                return slug
            # If not recognised by name, fall through to the old ID‑based logic

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
            return "method_of_victory"

        # Winner & exact round – sub_type_id 912
        if sid == "912":
            return "round_betting"

        # Winner & round range – sub_type_id 913
        if sid == "913":
            return "winner_and_round_range"

        # Unknown market
        return None