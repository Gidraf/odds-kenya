"""
app/workers/mappers/mozzart.py
==============================
Mozzart Market Mapper — Single source of truth for all 53 Mozzart gameIds
across 13 sports (Football, Basketball, Tennis, Table Tennis, Baseball,
Handball, Volleyball, eSports, Combat, Darts, Cricket, Rugby).
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional, Tuple

from app.workers.mappers.shared import (
    normalize_line,
    slug_with_line,
    normalize_outcome,
)

log = logging.getLogger(__name__)

# ── Mozzart gameId → (canonical_base_slug, uses_line) ──────────────────────
_MZ_GAME_TO_SLUG: dict[int, tuple[str, bool]] = {
    # ⚽ Football / Soccer (sportId: 1)
    1:    ("1x2",                             False),
    2:    ("over_under_goals",                True),
    8:    ("next_goal",                       False),
    15:   ("team_total_home",                 True),
    16:   ("team_total_away",                 True),
    500:  ("first_half_1x2",                  False),
    501:  ("first_half_over_under_goals",     True),
    493:  ("next_goal",                       False),

    # 🏀 Basketball (sportId: 2)
    105:  ("1x2",                             False),
    109:  ("total_points",                    True),
    111:  ("asian_handicap",                  True),
    157:  ("team_total_home",                 True),
    1424: ("match_winner",                    False),
    1430: ("team_total_home",                 True),
    1433: ("asian_handicap",                  True),
    1434: ("total_points",                    True),

    # 🎾 Tennis (sportId: 5)
    313:  ("match_winner",                    False),
    314:  ("set_winner",                      False),
    330:  ("game_handicap",                   True),
    331:  ("total_games",                     True),
    332:  ("set_total_games",                 True),

    # 🏓 Table Tennis (sportId: 20)
    830:  ("total_points",                    True),
    831:  ("match_winner",                    False),
    832:  ("set_winner",                      False),
    834:  ("point_handicap",                  True),
    840:  ("first_set_handicap",              True),
    859:  ("set_total_points",                True),

    # ⚾ Baseball (sportId: 3)
    167:  ("match_winner",                    False),
    168:  ("run_handicap",                    True),
    190:  ("inning_result",                   False),
    198:  ("inning_total",                    True),
    209:  ("over_under",                      True),

    # 🤾 Handball (sportId: 23)
    486:  ("1x2",                             False),
    488:  ("over_under_goals",                True),
    1110: ("1x2",                             False),
    1114: ("next_goal",                       False),
    1116: ("over_under_goals",                True),
    1118: ("team_total_home",                 True),
    1119: ("team_total_away",                 True),

    # 🏐 Volleyball & Futsal (sportId: 29 / 10)
    420:  ("total_points",                    True),
    421:  ("match_winner",                    False),
    422:  ("set_winner",                      False),
    423:  ("point_handicap",                  True),
    424:  ("set_handicap",                    True),

    # 🎮 eSports - LoL & Dota (sportId: 110)
    977:  ("match_winner",                    False),
    978:  ("map_handicap",                    True),
    979:  ("map_winner",                      False),
    983:  ("total_maps",                      True),

    # 🎮 eSports - CS (sportId: 111)
    990:  ("match_winner",                    False),
    991:  ("map_handicap",                    True),
    992:  ("map_winner",                      False),
    996:  ("total_maps",                      True),

    # Generic Winner / Total fallbacks
    17:   ("match_winner",                    False),
}

# ── Sport ID → canonical sport slug ───────────────────────────────────────
MZ_SPORT_SLUGS: dict[int, str] = {
    1:   "soccer",
    2:   "basketball",
    3:   "baseball",
    5:   "tennis",
    20:  "table-tennis",
    23:  "handball",
    29:  "volleyball",
    110: "esports-lol",
    111: "esports-cs",
    137: "mma",
    155: "darts",
}


class MozzartMapper:
    """
    Normalizes raw Mozzart markets, specialValues (lines), and subgames
    to standard canonical market slugs and outcome keys.
    """

    @staticmethod
    def get_canonical_slug(
        game_id: int,
        special_value: Any = None,
        raw_name: str = "",
        special_type: str = "",
    ) -> str:
        """
        Map a Mozzart gameId to a canonical slug.
        Appends normalized line if specialType is MARGIN or HANDICAP.
        """
        entry = _MZ_GAME_TO_SLUG.get(game_id)
        if not entry:
            # Fallback based on raw market name heuristics
            clean_name = re.sub(r"[^a-z0-9]+", "_", raw_name.lower()).strip("_")
            base_slug = f"mz_{game_id}_{clean_name}" if clean_name else f"mz_{game_id}"
            uses_line = special_type in ("MARGIN", "HANDICAP") or bool(special_value and str(special_value) != "-1")
        else:
            base_slug, uses_line = entry

        if uses_line and special_value is not None and str(special_value) != "-1":
            return slug_with_line(base_slug, special_value)

        return base_slug

    @staticmethod
    def normalize_outcome_key(
        market_slug: str,
        short_name: str,
        display_name: str = "",
    ) -> str:
        """
        Normalize Mozzart subgame outcome shortName (e.g. '1', 'X', '2', 'over', 'under')
        to canonical outcome key ('1', 'X', '2', 'over', 'under', 'yes', 'no').
        """
        sn = short_name.strip()

        # Handle 1X2 / Moneyline
        if sn == "1":
            return "1"
        if sn.upper() in ("X", "0", "DRAW"):
            return "X"
        if sn == "2":
            return "2"

        # Handle Over / Under
        if sn.lower() in ("over", "ov"):
            return "over"
        if sn.lower() in ("under", "un"):
            return "under"

        # Handle Both Teams to Score / Yes-No
        if sn.lower() in ("yes", "gg"):
            return "yes"
        if sn.lower() in ("no", "ng"):
            return "no"

        # Use shared outcome normalizer fallback
        return normalize_outcome(market_slug, short_name, display_name)
