"""
app/workers/sp_mapper.py
=========================
Sportpesa market-ID → canonical slug mapping for EVERY sport.

Each entry: (base_slug, uses_line)
  uses_line=True  → slug_with_line(base, specValue) is appended at parse time
  uses_line=False → slug is used as-is

Public API
──────────
  normalize_sp_market(mkt_id, spec_val, sport_id) → canonical slug
  get_sport_table(sport_id)                        → full id→(slug, uses_line) dict
  list_all_slugs(sport_id)                         → sorted list of base slugs
"""

from __future__ import annotations
from typing import Any
from app.workers.canonical_mapper import slug_with_line


# ══════════════════════════════════════════════════════════════════════════════
# PER-SPORT TABLES
# Each key is an integer market ID; value is (base_slug, uses_line).
# ══════════════════════════════════════════════════════════════════════════════

# ── Generic markets (shared across most sports) ───────────────────────────────
_GENERIC: dict[int, tuple[str, bool]] = {
    382: ("match_winner",            False),  # 2-way Winner
    51:  ("asian_handicap",          True),   # Asian Handicap / Spread
    52:  ("over_under",              True),   # Total O/U (generic)
    45:  ("odd_even",                False),  # Odd/Even
    353: ("home_total",              True),   # Home team total O/U
    352: ("away_total",              True),   # Away team total O/U
    226: ("total_games",             True),   # Total Games/Legs/Sets O/U
    233: ("set_betting",             False),  # Set/Score Betting
}

# ── Football / Soccer (sportId=1) ────────────────────────────────────────────
_FOOTBALL: dict[int, tuple[str, bool]] = {
    1:   ("1x2",                       False),  # 1X2 alias
    10:  ("1x2",                       False),  # 3-Way Full Time
    46:  ("double_chance",             False),  # Double Chance
    47:  ("draw_no_bet",               False),  # Draw No Bet
    43:  ("btts",                      False),  # Both Teams to Score
    29:  ("btts",                      False),  # BTTS alias
    386: ("btts_and_result",           False),  # BTTS + Result
    52:  ("over_under_goals",          True),   # O/U Goals (multi-line)
    18:  ("over_under_goals",          True),   # O/U Goals alt ID
    56:  ("over_under_goals",          True),   # O/U Goals alt ID (eFootball)
    353: ("home_goals",                True),   # Home Team Goals O/U
    352: ("away_goals",                True),   # Away Team Goals O/U
    208: ("result_and_over_under",     True),   # Result + O/U combo
    258: ("exact_goals",               False),  # Exact Number of Goals
    202: ("goal_groups",               False),  # Goal Range Groups (0-1, 2-3…)
    332: ("correct_score",             False),  # Correct Score
    51:  ("asian_handicap",            True),   # Asian Handicap FT
    53:  ("first_half_asian_handicap", True),   # Asian Handicap HT
    55:  ("european_handicap",         True),   # European Handicap
    45:  ("odd_even",                  False),  # Odd/Even Goals
    41:  ("first_team_to_score",       False),  # First Team to Score
    207: ("highest_scoring_half",      False),  # Half with Most Goals
    42:  ("first_half_1x2",            False),  # HT 1X2
    60:  ("first_half_1x2",            False),  # HT 1X2 alt
    15:  ("first_half_over_under",     True),   # HT O/U Goals
    54:  ("first_half_over_under",     True),   # HT O/U alt
    68:  ("first_half_over_under",     True),   # HT O/U alt 2
    44:  ("ht_ft",                     False),  # Half Time / Full Time
    328: ("first_half_btts",           False),  # HT Both Teams to Score
    203: ("first_half_correct_score",  False),  # HT Correct Score
    162: ("total_corners",             False),  # Total Corners
    166: ("total_corners",             True),   # Total Corners O/U
    136: ("total_bookings",            False),  # Total Bookings
    139: ("total_bookings",            True),   # Total Bookings O/U
    # eFootball-specific
    381: ("1x2",                       False),  # eFootball 1X2
}

# ── Basketball (sportId=2) ────────────────────────────────────────────────────
_BASKETBALL: dict[int, tuple[str, bool]] = {
    382: ("match_winner",              False),  # Match Winner (OT incl.)
    51:  ("point_spread",              True),   # Point Spread
    52:  ("total_points",              True),   # Total Points O/U
    353: ("home_total_points",         True),   # Home Team Points O/U
    352: ("away_total_points",         True),   # Away Team Points O/U
    45:  ("odd_even",                  False),  # Odd/Even Points
    222: ("winning_margin",            False),  # Winning Margin
    42:  ("first_half_winner",         False),  # First Half 3-Way
    53:  ("first_half_spread",         True),   # First Half Handicap
    54:  ("first_half_total",          True),   # First Half Total O/U
    224: ("highest_scoring_quarter",   False),  # Highest Scoring Quarter
    362: ("q1_total",                  True),   # 1st Quarter Total O/U
    363: ("q2_total",                  True),   # 2nd Quarter Total O/U
    364: ("q3_total",                  True),   # 3rd Quarter Total O/U
    365: ("q4_total",                  True),   # 4th Quarter Total O/U
    366: ("q1_spread",                 True),   # 1st Quarter Spread
    367: ("q2_spread",                 True),   # 2nd Quarter Spread
    368: ("q3_spread",                 True),   # 3rd Quarter Spread
    369: ("q4_spread",                 True),   # 4th Quarter Spread
}

# ── Tennis (sportId=5) ────────────────────────────────────────────────────────
_TENNIS: dict[int, tuple[str, bool]] = {
    382: ("match_winner",              False),  # Match Winner
    204: ("first_set_winner",          False),  # 1st Set Winner
    231: ("second_set_winner",         False),  # 2nd Set Winner
    51:  ("game_handicap",             True),   # Game Handicap
    226: ("total_games",               True),   # Total Games O/U
    233: ("set_betting",               False),  # Set Betting (2:0, 2:1…)
    439: ("set_handicap",              True),   # Set Handicap
    45:  ("odd_even_games",            False),  # Odd/Even Games
    339: ("first_set_game_handicap",   True),   # 1st Set Game HC
    340: ("first_set_total_games",     True),   # 1st Set Total Games O/U
    433: ("first_set_match_winner",    False),  # 1st Set / Match Winner combo
    353: ("player1_games",             True),   # Player 1 Games O/U
    352: ("player2_games",             True),   # Player 2 Games O/U
}

# ── Ice Hockey (sportId=4) ────────────────────────────────────────────────────
_ICE_HOCKEY: dict[int, tuple[str, bool]] = {
    1:   ("1x2",                       False),  # 3-Way (incl. draw)
    10:  ("1x2",                       False),  # 3-Way FT
    382: ("match_winner",              False),  # 2-Way (OT/SO incl.)
    52:  ("over_under_goals",          True),   # Total Goals O/U
    51:  ("puck_line",                 True),   # Puck Line (HC)
    45:  ("odd_even",                  False),  # Odd/Even Goals
    46:  ("double_chance",             False),  # Double Chance
    353: ("home_goals",                True),   # Home Goals O/U
    352: ("away_goals",                True),   # Away Goals O/U
    208: ("result_and_over_under",     True),   # Result + O/U
    43:  ("btts",                      False),  # Both Teams to Score
    210: ("first_period_winner",       False),  # 1st Period Winner
    378: ("match_winner_ot",           False),  # 2-Way OT incl. alias
}

# ── Volleyball (sportId=23) ───────────────────────────────────────────────────
_VOLLEYBALL: dict[int, tuple[str, bool]] = {
    382: ("match_winner",              False),  # Match Winner
    204: ("first_set_winner",          False),  # 1st Set Winner
    20:  ("match_winner",              False),  # 2-Way Winner alias
    51:  ("set_handicap",              True),   # Set Handicap
    226: ("total_sets",                True),   # Total Sets O/U
    233: ("set_betting",               False),  # Set Betting (3:0, 3:1…)
    45:  ("odd_even",                  False),  # Odd/Even Points
    353: ("home_points",               True),   # Home Points O/U
    352: ("away_points",               True),   # Away Points O/U
}

# ── Cricket (sportId=21) ──────────────────────────────────────────────────────
_CRICKET: dict[int, tuple[str, bool]] = {
    382: ("match_winner",              False),  # Match Winner (2-way)
    1:   ("1x2",                       False),  # 1X2 incl. draw (Tests)
    51:  ("run_handicap",              True),   # Run Handicap
    52:  ("total_runs",                True),   # Total Runs O/U
    353: ("home_runs",                 True),   # Home Team Runs O/U
    352: ("away_runs",                 True),   # Away Team Runs O/U
}

# ── Rugby Union/League (sportId=12) ──────────────────────────────────────────
_RUGBY: dict[int, tuple[str, bool]] = {
    10:  ("1x2",                       False),  # 3-Way Full Time
    1:   ("1x2",                       False),  # 1X2 alias
    382: ("match_winner",              False),  # 2-Way Match Winner
    46:  ("double_chance",             False),  # Double Chance
    42:  ("first_half_1x2",            False),  # First Half 1X2
    51:  ("asian_handicap",            True),   # Handicap Full Time
    53:  ("first_half_asian_handicap", True),   # Handicap First Half
    60:  ("total_points",              True),   # Total Points O/U
    52:  ("total_points",              True),   # Total Points alt
    353: ("home_points",               True),   # Home Points O/U
    352: ("away_points",               True),   # Away Points O/U
    45:  ("odd_even",                  False),  # Odd/Even Points
    379: ("winning_margin",            False),  # Winning Margin
    207: ("highest_scoring_half",      False),  # Highest Scoring Half
    44:  ("ht_ft",                     False),  # Half Time / Full Time
}

# ── Handball (sportId=6) ──────────────────────────────────────────────────────
_HANDBALL: dict[int, tuple[str, bool]] = {
    1:   ("1x2",                       False),  # 1X2
    10:  ("1x2",                       False),  # 3-Way
    382: ("match_winner",              False),  # 2-Way
    52:  ("over_under_goals",          True),   # Total Goals O/U
    51:  ("asian_handicap",            True),   # Asian Handicap
    45:  ("odd_even",                  False),  # Odd/Even Goals
    46:  ("double_chance",             False),  # Double Chance
    47:  ("draw_no_bet",               False),  # Draw No Bet
    353: ("home_goals",                True),   # Home Goals O/U
    352: ("away_goals",                True),   # Away Goals O/U
    208: ("result_and_over_under",     True),   # Result + O/U
    43:  ("btts",                      False),  # Both Teams to Score
    42:  ("first_half_1x2",            False),  # First Half 1X2
    207: ("highest_scoring_half",      False),  # Highest Scoring Half
}

# ── Table Tennis (sportId=16) ─────────────────────────────────────────────────
_TABLE_TENNIS: dict[int, tuple[str, bool]] = {
    382: ("match_winner",              False),  # Match Winner
    51:  ("game_handicap",             True),   # Game Handicap
    226: ("total_games",               True),   # Total Games O/U
    45:  ("odd_even",                  False),  # Odd/Even Points
    233: ("set_betting",               False),  # Set Betting
    340: ("first_set_total_games",     True),   # 1st Set Total Games
}

# ── MMA / UFC (sportId=117) ───────────────────────────────────────────────────
_MMA: dict[int, tuple[str, bool]] = {
    382: ("match_winner",              False),  # Fight Winner
    20:  ("match_winner",              False),  # Fight Winner alias
    51:  ("round_betting",             True),   # Round Betting
    52:  ("total_rounds",              True),   # Total Rounds O/U
}

# ── Boxing (sportId=10) ───────────────────────────────────────────────────────
_BOXING: dict[int, tuple[str, bool]] = {
    382: ("match_winner",              False),  # Fight Winner
    51:  ("round_betting",             True),   # Round Betting
    52:  ("total_rounds",              True),   # Total Rounds O/U
}

# ── Darts (sportId=49) ────────────────────────────────────────────────────────
_DARTS: dict[int, tuple[str, bool]] = {
    382: ("match_winner",              False),  # Match Winner
    226: ("total_legs",                True),   # Total Legs O/U
    45:  ("odd_even",                  False),  # Odd/Even Legs
    51:  ("leg_handicap",              True),   # Leg Handicap
}

# ── American Football / NFL (sportId=15) ─────────────────────────────────────
_AMERICAN_FOOTBALL: dict[int, tuple[str, bool]] = {
    382: ("match_winner",              False),  # Moneyline
    51:  ("point_spread",              True),   # Point Spread
    52:  ("total_points",              True),   # Total Points O/U
    45:  ("odd_even",                  False),  # Odd/Even Points
    353: ("home_total_points",         True),   # Home Points O/U
    352: ("away_total_points",         True),   # Away Points O/U
}

# ── eFootball / eSoccer (sportId=126) ─────────────────────────────────────────
_ESOCCER: dict[int, tuple[str, bool]] = {
    381: ("1x2",                       False),  # 1X2 (eFootball alias)
    1:   ("1x2",                       False),  # 1X2
    10:  ("1x2",                       False),  # 3-Way
    56:  ("over_under_goals",          True),   # O/U Goals
    52:  ("over_under_goals",          True),   # O/U Goals alt
    46:  ("double_chance",             False),  # Double Chance
    47:  ("draw_no_bet",               False),  # Draw No Bet
    43:  ("btts",                      False),  # BTTS
    51:  ("asian_handicap",            True),   # Asian HC
    45:  ("odd_even",                  False),  # Odd/Even
    208: ("result_and_over_under",     True),   # Result + O/U
    258: ("exact_goals",               False),  # Exact Goals
    202: ("goal_groups",               False),  # Goal Groups
}

# ── Baseball (sportId=3) ─────────────────────────────────────────────────────
_BASEBALL: dict[int, tuple[str, bool]] = {
    382: ("match_winner",              False),  # Moneyline
    51:  ("run_line",                  True),   # Run Line (Spread)
    52:  ("total_runs",                True),   # Total Runs O/U
    45:  ("odd_even",                  False),  # Odd/Even Runs
    353: ("home_runs_total",           True),   # Home Runs O/U
    352: ("away_runs_total",           True),   # Away Runs O/U
}


# ══════════════════════════════════════════════════════════════════════════════
# SPORT ID → TABLE MAP
# ══════════════════════════════════════════════════════════════════════════════

_SPORT_TABLES: dict[int, dict[int, tuple[str, bool]]] = {
    1:   _FOOTBALL,
    2:   _BASKETBALL,
    3:   _BASEBALL,
    4:   _ICE_HOCKEY,
    5:   _TENNIS,
    6:   _HANDBALL,
    10:  _BOXING,
    12:  _RUGBY,
    15:  _AMERICAN_FOOTBALL,
    16:  _TABLE_TENNIS,
    21:  _CRICKET,
    23:  _VOLLEYBALL,
    49:  _DARTS,
    117: _MMA,
    126: _ESOCCER,
}


# ══════════════════════════════════════════════════════════════════════════════
# CANONICAL MARKET DISPLAY NAMES
# Used by frontend to show human-readable market names.
# ══════════════════════════════════════════════════════════════════════════════

MARKET_DISPLAY_NAMES: dict[str, str] = {
    # Universal
    "match_winner":               "Match Winner",
    "1x2":                        "1X2",
    "asian_handicap":             "Asian Handicap",
    "european_handicap":          "European Handicap",
    "over_under":                 "Over/Under",
    "odd_even":                   "Odd/Even",
    "correct_score":              "Correct Score",
    "ht_ft":                      "HT/FT",
    "winning_margin":             "Winning Margin",
    # Football specific
    "over_under_goals":           "Goals Over/Under",
    "double_chance":              "Double Chance",
    "draw_no_bet":                "Draw No Bet",
    "btts":                       "Both Teams to Score",
    "btts_and_result":            "BTTS + Result",
    "result_and_over_under":      "Result + Over/Under",
    "first_team_to_score":        "First Team to Score",
    "exact_goals":                "Exact Goals",
    "goal_groups":                "Goal Groups",
    "highest_scoring_half":       "Half with Most Goals",
    "first_half_1x2":             "1st Half 1X2",
    "first_half_over_under":      "1st Half Over/Under",
    "first_half_btts":            "1st Half BTTS",
    "first_half_correct_score":   "1st Half Correct Score",
    "first_half_asian_handicap":  "1st Half Handicap",
    "home_goals":                 "Home Goals O/U",
    "away_goals":                 "Away Goals O/U",
    "total_corners":              "Total Corners",
    "total_bookings":             "Total Bookings",
    # Basketball
    "point_spread":               "Point Spread",
    "total_points":               "Total Points",
    "home_total_points":          "Home Points O/U",
    "away_total_points":          "Away Points O/U",
    "first_half_winner":          "1st Half Winner",
    "first_half_spread":          "1st Half Spread",
    "first_half_total":           "1st Half Total",
    "highest_scoring_quarter":    "Highest Scoring Quarter",
    "q1_total":                   "Q1 Total",
    "q2_total":                   "Q2 Total",
    "q3_total":                   "Q3 Total",
    "q4_total":                   "Q4 Total",
    "q1_spread":                  "Q1 Spread",
    "q2_spread":                  "Q2 Spread",
    "q3_spread":                  "Q3 Spread",
    "q4_spread":                  "Q4 Spread",
    # Tennis
    "first_set_winner":           "1st Set Winner",
    "second_set_winner":          "2nd Set Winner",
    "game_handicap":              "Game Handicap",
    "total_games":                "Total Games",
    "set_betting":                "Set Betting",
    "set_handicap":               "Set Handicap",
    "odd_even_games":             "Odd/Even Games",
    "first_set_game_handicap":    "1st Set Game HC",
    "first_set_total_games":      "1st Set Total Games",
    "first_set_match_winner":     "1st Set / Match Winner",
    "player1_games":              "Player 1 Games O/U",
    "player2_games":              "Player 2 Games O/U",
    # Ice Hockey
    "puck_line":                  "Puck Line",
    "first_period_winner":        "1st Period Winner",
    "match_winner_ot":            "Winner (OT incl.)",
    # Rugby
    "highest_scoring_half":       "Half with Most Points",
    "home_points":                "Home Points O/U",
    "away_points":                "Away Points O/U",
    # Volleyball
    "total_sets":                 "Total Sets",
    "home_points":                "Home Points O/U",
    "away_points":                "Away Points O/U",
    # Cricket
    "run_handicap":               "Run Handicap",
    "total_runs":                 "Total Runs",
    "home_runs":                  "Home Runs O/U",
    "away_runs":                  "Away Runs O/U",
    # Combat sports
    "round_betting":              "Round Betting",
    "total_rounds":               "Total Rounds",
    # Darts
    "total_legs":                 "Total Legs",
    "leg_handicap":               "Leg Handicap",
    # American Football
    "home_total_points":          "Home Points O/U",
    "away_total_points":          "Away Points O/U",
    # eFootball
    "goal_groups":                "Goal Groups",
}


# ══════════════════════════════════════════════════════════════════════════════
# CANONICAL OUTCOME DISPLAY NAMES  (per market)
# ══════════════════════════════════════════════════════════════════════════════

OUTCOME_DISPLAY: dict[str, dict[str, str]] = {
    "1x2": {
        "1": "Home",  "X": "Draw",  "2": "Away",
    },
    "match_winner": {
        "1": "Home",  "2": "Away",
    },
    "double_chance": {
        "1X": "Home or Draw", "X2": "Draw or Away", "12": "Home or Away",
    },
    "draw_no_bet": {
        "1": "Home",  "2": "Away",
    },
    "btts": {
        "yes": "Yes (GG)", "no": "No (NG)",
    },
    "odd_even": {
        "odd": "Odd", "even": "Even",
    },
    "highest_scoring_half": {
        "1st": "1st Half", "2nd": "2nd Half", "equal": "Equal",
    },
    "highest_scoring_quarter": {
        "1st_quarter": "Q1", "2nd_quarter": "Q2",
        "3rd_quarter": "Q3", "4th_quarter": "Q4",
    },
    "first_team_to_score": {
        "1": "Home", "2": "Away", "none": "No Goal",
    },
    "set_betting": {
        "2:0": "2-0", "2:1": "2-1", "0:2": "0-2", "1:2": "1-2",
        "3:0": "3-0", "3:1": "3-1", "3:2": "3-2",
        "0:3": "0-3", "1:3": "1-3", "2:3": "2-3",
    },
    "first_set_match_winner": {
        "1/1": "Home / Home", "1/2": "Home / Away",
        "2/1": "Away / Home", "2/2": "Away / Away",
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# SPORT METADATA  (for frontend display)
# ══════════════════════════════════════════════════════════════════════════════

SPORT_META: dict[int, dict] = {
    1:   {"name": "Football",         "emoji": "⚽", "slugs": ["soccer", "football"]},
    126: {"name": "eFootball",        "emoji": "🎮", "slugs": ["esoccer", "efootball"]},
    2:   {"name": "Basketball",       "emoji": "🏀", "slugs": ["basketball"]},
    5:   {"name": "Tennis",           "emoji": "🎾", "slugs": ["tennis"]},
    4:   {"name": "Ice Hockey",       "emoji": "🏒", "slugs": ["ice-hockey"]},
    12:  {"name": "Rugby Union",      "emoji": "🏉", "slugs": ["rugby"]},
    23:  {"name": "Volleyball",       "emoji": "🏐", "slugs": ["volleyball"]},
    21:  {"name": "Cricket",          "emoji": "🏏", "slugs": ["cricket"]},
    6:   {"name": "Handball",         "emoji": "🤾", "slugs": ["handball"]},
    16:  {"name": "Table Tennis",     "emoji": "🏓", "slugs": ["table-tennis"]},
    117: {"name": "MMA",              "emoji": "🥋", "slugs": ["mma", "ufc"]},
    10:  {"name": "Boxing",           "emoji": "🥊", "slugs": ["boxing"]},
    49:  {"name": "Darts",            "emoji": "🎯", "slugs": ["darts"]},
    15:  {"name": "Am. Football",     "emoji": "🏈", "slugs": ["american-football"]},
    3:   {"name": "Baseball",         "emoji": "⚾", "slugs": ["baseball"]},
}

# Primary markets to display in grid columns per sport (ordered)
SPORT_PRIMARY_MARKETS: dict[int, list[str]] = {
    1:   ["1x2", "over_under_goals", "btts", "double_chance", "asian_handicap",
          "first_half_1x2", "correct_score", "ht_ft"],
    126: ["1x2", "over_under_goals", "btts", "double_chance", "asian_handicap"],
    2:   ["match_winner", "point_spread", "total_points",
          "first_half_winner", "highest_scoring_quarter"],
    5:   ["match_winner", "first_set_winner", "total_games",
          "set_betting", "game_handicap"],
    4:   ["1x2", "match_winner", "over_under_goals", "puck_line", "btts"],
    12:  ["1x2", "match_winner", "asian_handicap", "total_points",
          "highest_scoring_half", "ht_ft"],
    23:  ["match_winner", "set_handicap", "total_sets", "set_betting"],
    21:  ["match_winner", "1x2", "total_runs", "run_handicap"],
    6:   ["1x2", "match_winner", "over_under_goals", "asian_handicap",
          "btts", "double_chance"],
    16:  ["match_winner", "game_handicap", "total_games"],
    117: ["match_winner", "total_rounds", "round_betting"],
    10:  ["match_winner", "total_rounds", "round_betting"],
    49:  ["match_winner", "total_legs", "odd_even", "leg_handicap"],
    15:  ["match_winner", "point_spread", "total_points", "odd_even"],
    3:   ["match_winner", "run_line", "total_runs"],
}


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def get_sport_table(sport_id: int) -> dict[int, tuple[str, bool]]:
    """Return the full market-ID lookup table for a sport (with _GENERIC fallbacks)."""
    base = _SPORT_TABLES.get(sport_id, {})
    # Merge: sport-specific wins over generic
    merged = dict(_GENERIC)
    merged.update(base)
    return merged


def normalize_sp_market(
    mkt_id:   int,
    spec_val: Any   = None,
    sport_id: int   = 1,
) -> str:
    """
    Convert (market_id, spec_val, sport_id) → canonical slug.

    Examples
    ────────
    normalize_sp_market(10, None, 1)        → "1x2"
    normalize_sp_market(52, "2.5", 1)       → "over_under_goals_2.5"
    normalize_sp_market(51, "-1.5", 2)      → "point_spread_-1.5"
    normalize_sp_market(382, None, 5)       → "match_winner"
    normalize_sp_market(233, None, 5)       → "set_betting"
    normalize_sp_market(226, "22.5", 5)     → "total_games_22.5"
    """
    table = get_sport_table(sport_id)
    entry = table.get(mkt_id)

    if entry is None:
        # Final fallback to generic
        entry = _GENERIC.get(mkt_id)

    if entry is None:
        return f"market_{mkt_id}"

    base, uses_line = entry
    if uses_line and spec_val is not None:
        return slug_with_line(base, spec_val)
    return base


def get_market_display_name(slug: str) -> str:
    """Return human-readable display name for a canonical market slug."""
    # Strip line suffix (e.g. "over_under_goals_2.5" → "over_under_goals")
    base = slug
    for sep in ["_0.", "_1.", "_2.", "_3.", "_4.", "_5.", "_6.",
                "_7.", "_8.", "_9.", "_-"]:
        idx = slug.find(sep)
        if idx != -1:
            base = slug[:idx]
            break
    return MARKET_DISPLAY_NAMES.get(base) or MARKET_DISPLAY_NAMES.get(slug) or \
           slug.replace("_", " ").title()


def list_all_slugs(sport_id: int = 1) -> list[str]:
    """Return sorted list of all canonical base slugs for a sport."""
    table = get_sport_table(sport_id)
    return sorted({slug for slug, _ in table.values()})


def get_sport_primary_markets(sport_id: int) -> list[str]:
    """Return ordered list of primary market slugs to display for a sport."""
    return SPORT_PRIMARY_MARKETS.get(sport_id, ["match_winner"])


def get_sport_meta(sport_id: int) -> dict:
    """Return display metadata for a sport."""
    return SPORT_META.get(sport_id, {"name": f"Sport {sport_id}", "emoji": "🏆", "slugs": []})