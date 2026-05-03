"""
app/workers/mappers/odibet_handball.py
=======================================
OdiBets Handball market mapper.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetsHandballMapper:
    """Maps OdiBets Handball JSON market slugs to canonical slugs + specifiers."""

    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "handball_1x2": ("1x2", {"period": "match"}),
    }

    @classmethod
    def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
        """
        Parse an OdiBets market slug and return (canonical_slug, specifiers).
        """
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # Full match total goals
        total_match = re.match(r"over_under_handball_goals_([\d_]+)$", market_slug)
        if total_match:
            line = float(total_match.group(1).replace("_", "."))
            return ("total_goals", {"line": str(line), "period": "match"})

        # First half total goals
        half_total_match = re.match(r"first_half_over_under_handball_goals_([\d_]+)$", market_slug)
        if half_total_match:
            line = float(half_total_match.group(1).replace("_", "."))
            return ("total_goals", {"line": str(line), "period": "first_half"})

        return None

    @classmethod
    def get_canonical_slug(cls, market_slug: str) -> Optional[str]:
        info = cls.get_market_info(market_slug)
        return info[0] if info else None

    @classmethod
    def transform_outcome(cls, market_slug: str, outcome_key: str) -> str:
        if market_slug == "handball_1x2":
            return {"1": "home", "X": "draw", "2": "away"}.get(outcome_key, outcome_key)
        return outcome_key  # "over"/"under" stay as is


def get_od_handball_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    return OdibetsHandballMapper.get_market_info(market_slug)