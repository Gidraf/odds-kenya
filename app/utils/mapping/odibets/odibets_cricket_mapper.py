"""
app/workers/mappers/odibets_cricket_mapper.py
==============================================
OdiBets Cricket market mapper – maps sub_type_id + specifiers to canonical slugs.
Also supports direct name mapping (e.g., "cricket_winner_incl_super_over").
"""

from typing import Dict, Optional


class OdiBetsCricketMapper:
    """Maps OdiBets Cricket market IDs and specifiers or name strings to internal canonical slugs."""

    STATIC_MARKETS: Dict[str, str] = {
        "340": "cricket_match_winner",   # Winner (incl. super over)
    }

    # Direct name‑to‑slug mappings (for JSON where market name is the key)
    STATIC_NAME_MARKETS: Dict[str, str] = {
        "cricket_winner_incl_super_over": "cricket_match_winner",
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
        return cls.STATIC_NAME_MARKETS.get(market_name)

    @classmethod
    def get_market_slug(cls, sub_type_id: str, specifiers: Dict[str, str], market_name: str = "") -> Optional[str]:
        """
        Returns the canonical market slug.
        Args:
            sub_type_id: OdiBets sub_type_id (as string)
            specifiers: dict of parsed specifiers (not used here)
            market_name: optional market name (used when sub_type_id is not available)
        """
        # If a market name is given, try to map it directly (supports JSON data)
        if market_name:
            slug = cls._map_by_name(market_name)
            if slug:
                return slug
            # If not recognised by name, fall through to ID‑based logic

        sid = str(sub_type_id)

        # Static markets
        if sid in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sid]

        # No other markets observed; can be extended later
        return None