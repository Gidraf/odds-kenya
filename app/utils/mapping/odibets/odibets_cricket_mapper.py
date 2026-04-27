"""
app/workers/mappers/odibet_cricket.py
======================================
OdiBets Cricket market mapper.
Converts OdiBets-specific market slugs (as produced by od_harvester.py)
into canonical market slugs + specifiers for internal use.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class OdibetCricketMapper:
    """Maps OdiBets Cricket JSON market slugs to canonical slugs + specifiers."""

    # Direct mapping for simple markets (no specifiers)
    STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
        "cricket_winner_incl_super_over": ("cricket_match_winner", {"include_super_over": "true"}),
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

        Examples:
            "cricket_winner_incl_super_over"    → ("cricket_match_winner", {"include_super_over": "true"})
            "over_under_cricket_runs_180_5"     → ("total_runs_match", {"line": "180.5", "period": "match"})
            "cricket_top_batsman"               → ("top_batsman", {})
            "cricket_top_bowler"                → ("top_bowler", {})
            "cricket_player_runs_50_5"          → ("player_runs", {"line": "50.5"})
            "cricket_player_wickets_2_5"        → ("player_wickets", {"line": "2.5"})
        """
        # ----- Static mappings -----
        if market_slug in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[market_slug]

        # ----- Match Winner (without super over) -----
        if market_slug == "cricket_winner":
            return ("cricket_match_winner", {"include_super_over": "false"})

        # ----- Total Runs in Match -----
        total_match = re.match(r"over_under_cricket_runs_([\d_]+)$", market_slug)
        if total_match:
            line = float(total_match.group(1).replace("_", "."))
            return ("total_runs_match", {"line": str(line), "period": "match"})

        # ----- Total Runs in Innings (e.g., 1st innings) -----
        innings_total_match = re.match(r"over_under_cricket_innings_(\d+)_runs_([\d_]+)$", market_slug)
        if innings_total_match:
            innings = innings_total_match.group(1)
            line = float(innings_total_match.group(2).replace("_", "."))
            return ("total_runs_innings", {"innings": innings, "line": str(line)})

        # ----- Player Runs (Over/Under) -----
        player_runs_match = re.match(r"cricket_player_runs_([\d_]+)$", market_slug)
        if player_runs_match:
            line = float(player_runs_match.group(1).replace("_", "."))
            return ("player_runs", {"line": str(line)})

        # ----- Player Wickets (Over/Under) -----
        player_wickets_match = re.match(r"cricket_player_wickets_([\d_]+)$", market_slug)
        if player_wickets_match:
            line = float(player_wickets_match.group(1).replace("_", "."))
            return ("player_wickets", {"line": str(line)})

        # ----- Top Batsman -----
        if market_slug == "cricket_top_batsman" or market_slug == "cricket_top_batter":
            return ("top_batsman", {})

        # ----- Top Bowler -----
        if market_slug == "cricket_top_bowler":
            return ("top_bowler", {})

        # ----- Man of the Match -----
        if market_slug == "cricket_man_of_match":
            return ("man_of_match_cricket", {})

        # ----- Method of Dismissal (next wicket) -----
        if market_slug == "cricket_method_of_dismissal":
            return ("method_of_dismissal", {})

        # ----- Fall of Next Wicket (runs band) -----
        fall_wicket_match = re.match(r"cricket_fall_next_wicket_([\d_]+)$", market_slug)
        if fall_wicket_match:
            line = fall_wicket_match.group(1).replace("_", ".")
            return ("fall_next_wicket", {"line": line})

        # ----- 1st Innings Lead -----
        if market_slug == "cricket_first_innings_lead":
            return ("first_innings_lead", {})

        # ----- Toss Winner -----
        if market_slug == "cricket_toss_winner":
            return ("toss_winner", {})

        # ----- Fifty Scored (yes/no) -----
        if market_slug == "cricket_fifty_scored":
            return ("fifty_scored", {})

        # ----- Century Scored (yes/no) -----
        if market_slug == "cricket_century_scored":
            return ("century_scored", {})

        # ----- Powerplay Runs (1st 6 overs) -----
        powerplay_match = re.match(r"cricket_powerplay_runs_([\d_]+)$", market_slug)
        if powerplay_match:
            line = float(powerplay_match.group(1).replace("_", "."))
            return ("powerplay_runs", {"line": str(line)})

        # ----- Unknown market -----
        return None

    @classmethod
    def get_canonical_slug(cls, market_slug: str) -> Optional[str]:
        """Return just the canonical slug (without specifiers)."""
        info = cls.get_market_info(market_slug)
        return info[0] if info else None

    @classmethod
    def transform_outcome(cls, market_slug: str, outcome_key: str) -> str:
        """
        Convert OdiBets outcome keys to canonical outcome names.
        Used when building the final outcomes dict.
        """
        # For match winner markets (1/2)
        if market_slug in ("cricket_winner", "cricket_winner_incl_super_over"):
            mapping = {"1": "home", "2": "away"}
            return mapping.get(outcome_key, outcome_key)

        # For top batsman/bowler (player names as outcome keys)
        # Usually outcome_key is the player name; keep as is
        return outcome_key


# Generic dispatcher
def get_od_cricket_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
    return OdibetCricketMapper.get_market_info(market_slug)