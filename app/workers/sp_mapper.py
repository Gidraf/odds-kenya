"""
app/workers/sp_mapper.py
=========================
Sportpesa market-ID → canonical slug mapping for EVERY sport.

Public API
──────────
  normalize_sp_market(mkt_id, spec_val, sport_id)  → canonical slug
  get_sport_table(sport_id)                         → full id→(slug, uses_line) dict
  list_all_slugs(sport_id)                          → sorted list of base slugs
  extract_base_and_line(slug)                       → (base_slug, line_str)
  get_market_display_name(slug)                     → "Goals O/U 2.5"
  get_outcome_display(slug, outcome_key)            → "Over 2.5" / "Home +1.5"

Changes in v2
──────────────
  • extract_base_and_line() — robust slug splitter that handles:
      - integer lines: "european_handicap_2" → ("european_handicap", "2")
      - negative lines: "asian_handicap_-1.25" → ("asian_handicap", "-1.25")
      - decimal lines: "over_under_goals_2.5" → ("over_under_goals", "2.5")
  • get_market_display_name() rewritten — includes line in display, e.g.
      "over_under_goals_2.5" → "Goals O/U 2.5"
      "asian_handicap_1.5"   → "Asian Handicap 1.5"
      "home_goals_0.5"       → "Home Goals O/U 0.5"
  • get_outcome_display(slug, key) — new function used by sp_module enrichment:
      ("over_under_goals_2.5", "over")   → "Over 2.5"
      ("over_under_goals_2.5", "under")  → "Under 2.5"
      ("asian_handicap_1.5",   "1")      → "+1.5 Home"
      ("asian_handicap_1.5",   "2")      → "-1.5 Away"
      ("european_handicap_2",  "1")      → "1 (+2)"
      ("1x2",                  "1")      → "Home"
      ("ht_ft",                "1/1")    → "1/1"
  • sp_module enrichment in get_match_markets() MUST use get_outcome_display()
    instead of OUTCOME_DISPLAY.get(slug.split("_")[0], {}).

All market maps updated to match sp_sports/__init__.py SportConfig market_ids.
"""

from __future__ import annotations

import re
from typing import Any
from app.workers.canonical_mapper import slug_with_line


# ══════════════════════════════════════════════════════════════════════════════
# PER-SPORT TABLES
# ══════════════════════════════════════════════════════════════════════════════

# ── Generic markets (shared fallbacks) ───────────────────────────────────────
_GENERIC: dict[int, tuple[str, bool]] = {
    382: ("match_winner",  False),
    51:  ("asian_handicap", True),
    52:  ("over_under",    True),
    45:  ("odd_even",      False),
    353: ("home_total",    True),
    352: ("away_total",    True),
    226: ("total_games",   True),
    233: ("set_betting",   False),
}

# ── Football / Soccer (sportId=1) ────────────────────────────────────────────
_FOOTBALL: dict[int, tuple[str, bool]] = {
    1:   ("1x2",                      False),
    10:  ("1x2",                      False),
    46:  ("double_chance",            False),
    47:  ("draw_no_bet",              False),
    43:  ("btts",                     False),
    29:  ("btts",                     False),
    386: ("btts_and_result",          False),
    52:  ("over_under_goals",         True),
    18:  ("over_under_goals",         True),
    56:  ("over_under_goals",         True),
    353: ("home_goals",               True),
    352: ("away_goals",               True),
    208: ("result_and_over_under",    True),
    258: ("exact_goals",              False),
    202: ("goal_groups",              False),
    332: ("correct_score",            False),
    51:  ("asian_handicap",           True),
    53:  ("first_half_asian_handicap",True),
    55:  ("european_handicap",        True),
    45:  ("odd_even",                 False),
    41:  ("first_team_to_score",      False),
    207: ("highest_scoring_half",     False),
    42:  ("first_half_1x2",           False),
    60:  ("first_half_1x2",           False),
    15:  ("first_half_over_under",    True),
    54:  ("first_half_over_under",    True),
    68:  ("first_half_over_under",    True),
    44:  ("ht_ft",                    False),
    328: ("first_half_btts",          False),
    203: ("first_half_correct_score", False),
    162: ("total_corners",            False),
    166: ("total_corners",            True),
    136: ("total_bookings",           False),
    139: ("total_bookings",           True),
    381: ("1x2",                      False),
}

# ── Basketball (sportId=2) ────────────────────────────────────────────────────
_BASKETBALL: dict[int, tuple[str, bool]] = {
    382: ("match_winner",            False),
    51:  ("point_spread",            True),
    52:  ("total_points",            True),
    353: ("home_total_points",       True),
    352: ("away_total_points",       True),
    45:  ("odd_even",                False),
    222: ("winning_margin",          False),
    42:  ("first_half_winner",       False),
    53:  ("first_half_spread",       True),
    54:  ("first_half_total",        True),
    224: ("highest_scoring_quarter", False),
    362: ("q1_total",                True),
    363: ("q2_total",                True),
    364: ("q3_total",                True),
    365: ("q4_total",                True),
    366: ("q1_spread",               True),
    367: ("q2_spread",               True),
    368: ("q3_spread",               True),
    369: ("q4_spread",               True),
}

# ── Tennis (sportId=5) ────────────────────────────────────────────────────────
_TENNIS: dict[int, tuple[str, bool]] = {
    382: ("match_winner",            False),
    204: ("first_set_winner",        False),
    231: ("second_set_winner",       False),
    51:  ("game_handicap",           True),
    226: ("total_games",             True),
    233: ("set_betting",             False),
    439: ("set_handicap",            True),
    45:  ("odd_even_games",          False),
    339: ("first_set_game_handicap", True),
    340: ("first_set_total_games",   True),
    433: ("first_set_match_winner",  False),
    353: ("player1_games",           True),
    352: ("player2_games",           True),
}

# ── Ice Hockey (sportId=4) ────────────────────────────────────────────────────
_ICE_HOCKEY: dict[int, tuple[str, bool]] = {
    1:   ("1x2",                  False),
    10:  ("1x2",                  False),
    382: ("match_winner",         False),
    52:  ("over_under_goals",     True),
    51:  ("puck_line",            True),
    45:  ("odd_even",             False),
    46:  ("double_chance",        False),
    353: ("home_goals",           True),
    352: ("away_goals",           True),
    208: ("result_and_over_under",True),
    43:  ("btts",                 False),
    210: ("first_period_winner",  False),
    378: ("match_winner_ot",      False),
}

# ── Volleyball (sportId=23) ───────────────────────────────────────────────────
_VOLLEYBALL: dict[int, tuple[str, bool]] = {
    382: ("match_winner",  False),
    204: ("first_set_winner", False),
    20:  ("match_winner",  False),
    51:  ("set_handicap",  True),
    226: ("total_sets",    True),
    233: ("set_betting",   False),
    45:  ("odd_even",      False),
    353: ("home_points",   True),
    352: ("away_points",   True),
}

# ── Cricket (sportId=21) ──────────────────────────────────────────────────────
_CRICKET: dict[int, tuple[str, bool]] = {
    382: ("match_winner", False),
    1:   ("1x2",          False),
    51:  ("run_handicap", True),
    52:  ("total_runs",   True),
    353: ("home_runs",    True),
    352: ("away_runs",    True),
}

# ── Rugby Union/League (sportId=12) ──────────────────────────────────────────
_RUGBY: dict[int, tuple[str, bool]] = {
    10:  ("1x2",                      False),
    1:   ("1x2",                      False),
    382: ("match_winner",             False),
    46:  ("double_chance",            False),
    42:  ("first_half_1x2",           False),
    51:  ("asian_handicap",           True),
    53:  ("first_half_asian_handicap",True),
    60:  ("total_points",             True),
    52:  ("total_points",             True),
    353: ("home_points",              True),
    352: ("away_points",              True),
    45:  ("odd_even",                 False),
    379: ("winning_margin",           False),
    207: ("highest_scoring_half",     False),
    44:  ("ht_ft",                    False),
}

# ── Handball (sportId=6) ──────────────────────────────────────────────────────
_HANDBALL: dict[int, tuple[str, bool]] = {
    1:   ("1x2",                  False),
    10:  ("1x2",                  False),
    382: ("match_winner",         False),
    52:  ("over_under_goals",     True),
    51:  ("asian_handicap",       True),
    45:  ("odd_even",             False),
    46:  ("double_chance",        False),
    47:  ("draw_no_bet",          False),
    353: ("home_goals",           True),
    352: ("away_goals",           True),
    208: ("result_and_over_under",True),
    43:  ("btts",                 False),
    42:  ("first_half_1x2",       False),
    207: ("highest_scoring_half", False),
}

# ── Table Tennis (sportId=16) ─────────────────────────────────────────────────
_TABLE_TENNIS: dict[int, tuple[str, bool]] = {
    382: ("match_winner",        False),
    51:  ("game_handicap",       True),
    226: ("total_games",         True),
    45:  ("odd_even",            False),
    233: ("set_betting",         False),
    340: ("first_set_total_games",True),
}

# ── MMA / UFC (sportId=117) ───────────────────────────────────────────────────
_MMA: dict[int, tuple[str, bool]] = {
    382: ("match_winner", False),
    20:  ("match_winner", False),
    51:  ("round_betting",True),
    52:  ("total_rounds", True),
}

# ── Boxing (sportId=10) ───────────────────────────────────────────────────────
_BOXING: dict[int, tuple[str, bool]] = {
    382: ("match_winner", False),
    51:  ("round_betting",True),
    52:  ("total_rounds", True),
}

# ── Darts (sportId=49) ────────────────────────────────────────────────────────
_DARTS: dict[int, tuple[str, bool]] = {
    382: ("match_winner", False),
    226: ("total_legs",   True),
    45:  ("odd_even",     False),
    51:  ("leg_handicap", True),
}

# ── American Football / NFL (sportId=15) ─────────────────────────────────────
_AMERICAN_FOOTBALL: dict[int, tuple[str, bool]] = {
    382: ("match_winner",     False),
    51:  ("point_spread",     True),
    52:  ("total_points",     True),
    45:  ("odd_even",         False),
    353: ("home_total_points",True),
    352: ("away_total_points",True),
}

# ── eFootball / eSoccer (sportId=126) ─────────────────────────────────────────
_ESOCCER: dict[int, tuple[str, bool]] = {
    381: ("1x2",                   False),
    1:   ("1x2",                   False),
    10:  ("1x2",                   False),
    56:  ("over_under_goals",      True),
    52:  ("over_under_goals",      True),
    46:  ("double_chance",         False),
    47:  ("draw_no_bet",           False),
    43:  ("btts",                  False),
    51:  ("asian_handicap",        True),
    45:  ("odd_even",              False),
    208: ("result_and_over_under", True),
    258: ("exact_goals",           False),
    202: ("goal_groups",           False),
}

# ── Baseball (sportId=3) ─────────────────────────────────────────────────────
_BASEBALL: dict[int, tuple[str, bool]] = {
    382: ("match_winner",     False),
    51:  ("run_line",         True),
    52:  ("total_runs",       True),
    45:  ("odd_even",         False),
    353: ("home_runs_total",  True),
    352: ("away_runs_total",  True),
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
# CANONICAL MARKET DISPLAY NAMES  (base slugs only, no line suffix)
# ══════════════════════════════════════════════════════════════════════════════

MARKET_DISPLAY_NAMES: dict[str, str] = {
    # Universal
    "match_winner":                "Match Winner",
    "1x2":                         "1X2",
    "asian_handicap":              "Asian Handicap",
    "european_handicap":           "European Handicap",
    "over_under":                  "Over/Under",
    "odd_even":                    "Odd/Even",
    "correct_score":               "Correct Score",
    "ht_ft":                       "HT/FT",
    "winning_margin":              "Winning Margin",
    # Football
    "over_under_goals":            "Goals O/U",
    "double_chance":               "Double Chance",
    "draw_no_bet":                 "Draw No Bet",
    "btts":                        "Both Teams to Score",
    "btts_and_result":             "BTTS + Result",
    "result_and_over_under":       "Result + O/U",
    "first_team_to_score":         "First Team to Score",
    "exact_goals":                 "Exact Goals",
    "goal_groups":                 "Goal Groups",
    "highest_scoring_half":        "Half with Most Goals",
    "first_half_1x2":              "1st Half 1X2",
    "first_half_over_under":       "1st Half O/U",
    "first_half_btts":             "1st Half BTTS",
    "first_half_correct_score":    "1st Half Correct Score",
    "first_half_asian_handicap":   "1st Half Handicap",
    "home_goals":                  "Home Goals O/U",
    "away_goals":                  "Away Goals O/U",
    "total_corners":               "Total Corners",
    "total_bookings":              "Total Bookings",
    # Basketball
    "point_spread":                "Point Spread",
    "total_points":                "Total Points",
    "home_total_points":           "Home Points O/U",
    "away_total_points":           "Away Points O/U",
    "first_half_winner":           "1st Half Winner",
    "first_half_spread":           "1st Half Spread",
    "first_half_total":            "1st Half Total",
    "highest_scoring_quarter":     "Highest Scoring Quarter",
    "q1_total":                    "Q1 Total",
    "q2_total":                    "Q2 Total",
    "q3_total":                    "Q3 Total",
    "q4_total":                    "Q4 Total",
    "q1_spread":                   "Q1 Spread",
    "q2_spread":                   "Q2 Spread",
    "q3_spread":                   "Q3 Spread",
    "q4_spread":                   "Q4 Spread",
    # Tennis
    "first_set_winner":            "1st Set Winner",
    "second_set_winner":           "2nd Set Winner",
    "game_handicap":               "Game Handicap",
    "total_games":                 "Total Games",
    "set_betting":                 "Set Betting",
    "set_handicap":                "Set Handicap",
    "odd_even_games":              "Odd/Even Games",
    "first_set_game_handicap":     "1st Set Game HC",
    "first_set_total_games":       "1st Set Total Games",
    "first_set_match_winner":      "1st Set / Match Winner",
    "player1_games":               "Player 1 Games O/U",
    "player2_games":               "Player 2 Games O/U",
    # Ice Hockey
    "puck_line":                   "Puck Line",
    "first_period_winner":         "1st Period Winner",
    "match_winner_ot":             "Winner (OT incl.)",
    # Rugby / Handball / Volleyball / Cricket
    "home_points":                 "Home Points O/U",
    "away_points":                 "Away Points O/U",
    "total_sets":                  "Total Sets",
    "total_runs":                  "Total Runs",
    "home_runs":                   "Home Runs O/U",
    "away_runs":                   "Away Runs O/U",
    "run_handicap":                "Run Handicap",
    # Combat sports
    "round_betting":               "Round Betting",
    "total_rounds":                "Total Rounds",
    # Darts
    "total_legs":                  "Total Legs",
    "leg_handicap":                "Leg Handicap",
    # Baseball
    "run_line":                    "Run Line",
    "home_runs_total":             "Home Runs O/U",
    "away_runs_total":             "Away Runs O/U",
    # Home/Away generic
    "home_total":                  "Home Total O/U",
    "away_total":                  "Away Total O/U",
}

# ── O/U slugs that carry a line suffix — used by get_outcome_display ──────────
# Maps base slug → label prefix used with the line value.
_OU_SLUGS: frozenset[str] = frozenset({
    "over_under_goals",
    "over_under",
    "total_points",
    "total_runs",
    "total_games",
    "total_legs",
    "total_sets",
    "total_rounds",
    "total_corners",
    "total_bookings",
    "home_goals",
    "away_goals",
    "home_points",
    "away_points",
    "home_total",
    "away_total",
    "home_total_points",
    "away_total_points",
    "home_runs",
    "away_runs",
    "home_runs_total",
    "away_runs_total",
    "player1_games",
    "player2_games",
    "first_half_over_under",
    "first_half_total",
    "second_half_over_under",
    "q1_total",
    "q2_total",
    "q3_total",
    "q4_total",
    "first_set_total_games",
})

# ── Handicap slugs — used by get_outcome_display to show +/- line ─────────────
_HC_SLUGS: frozenset[str] = frozenset({
    "asian_handicap",
    "first_half_asian_handicap",
    "point_spread",
    "puck_line",
    "run_line",
    "set_handicap",
    "game_handicap",
    "first_set_game_handicap",
    "leg_handicap",
    "run_handicap",
    "q1_spread",
    "q2_spread",
    "q3_spread",
    "q4_spread",
    "first_half_spread",
})

# ── European handicap — 3-way with explicit line ───────────────────────────────
_EUR_HC_SLUGS: frozenset[str] = frozenset({
    "european_handicap",
})

# ══════════════════════════════════════════════════════════════════════════════
# CANONICAL OUTCOME DISPLAY NAMES  (per market base slug)
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
    "odd_even_games": {
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
    "btts_and_result": {
        "1_yes": "Home + GG", "X_yes": "Draw + GG", "2_yes": "Away + GG",
        "1_no":  "Home + NG", "X_no":  "Draw + NG", "2_no":  "Away + NG",
    },
    "result_and_over_under": {
        "1_over":  "Home + Over",  "X_over":  "Draw + Over",  "2_over":  "Away + Over",
        "1_under": "Home + Under", "X_under": "Draw + Under", "2_under": "Away + Under",
    },
    "goal_groups": {
        "0-1": "0–1 Goals", "2-3": "2–3 Goals", "4-5": "4–5 Goals", "6+": "6+ Goals",
    },
    "exact_goals": {
        "0": "0 Goals", "1": "1 Goal", "2": "2 Goals", "3": "3 Goals",
        "4": "4 Goals", "5": "5 Goals", "6+": "6+ Goals",
        "other": "Any Other",
    },
    "first_half_btts": {
        "yes": "Yes (GG)", "no": "No (NG)",
    },
    "first_half_1x2": {
        "1": "Home", "X": "Draw", "2": "Away",
    },
    "correct_score": {
        "other": "Any Other",
    },
    "first_half_correct_score": {
        "other": "Any Other",
    },
    "ht_ft": {
        "1/1": "Home / Home", "1/X": "Home / Draw", "1/2": "Home / Away",
        "X/1": "Draw / Home", "X/X": "Draw / Draw", "X/2": "Draw / Away",
        "2/1": "Away / Home", "2/X": "Away / Draw", "2/2": "Away / Away",
    },
}

# ── Primary markets per sport (ordered, used for UI column defaults) ───────────
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

SPORT_META: dict[int, dict] = {
    1:   {"name": "Football",       "emoji": "⚽", "slugs": ["soccer", "football"]},
    126: {"name": "eFootball",      "emoji": "🎮", "slugs": ["esoccer", "efootball"]},
    2:   {"name": "Basketball",     "emoji": "🏀", "slugs": ["basketball"]},
    5:   {"name": "Tennis",         "emoji": "🎾", "slugs": ["tennis"]},
    4:   {"name": "Ice Hockey",     "emoji": "🏒", "slugs": ["ice-hockey"]},
    12:  {"name": "Rugby Union",    "emoji": "🏉", "slugs": ["rugby"]},
    23:  {"name": "Volleyball",     "emoji": "🏐", "slugs": ["volleyball"]},
    21:  {"name": "Cricket",        "emoji": "🏏", "slugs": ["cricket"]},
    6:   {"name": "Handball",       "emoji": "🤾", "slugs": ["handball"]},
    16:  {"name": "Table Tennis",   "emoji": "🏓", "slugs": ["table-tennis"]},
    117: {"name": "MMA",            "emoji": "🥋", "slugs": ["mma", "ufc"]},
    10:  {"name": "Boxing",         "emoji": "🥊", "slugs": ["boxing"]},
    49:  {"name": "Darts",          "emoji": "🎯", "slugs": ["darts"]},
    15:  {"name": "Am. Football",   "emoji": "🏈", "slugs": ["american-football"]},
    3:   {"name": "Baseball",       "emoji": "⚾", "slugs": ["baseball"]},
}


# ══════════════════════════════════════════════════════════════════════════════
# SLUG UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

# Regex: matches a trailing numeric line suffix (positive, negative, integer, decimal)
# Examples: "_2.5", "_-1.25", "_2", "_0.5", "_-1"
_LINE_SUFFIX_RE = re.compile(r"^(.+?)_(-?\d+(?:\.\d+)?)$")


def extract_base_and_line(slug: str) -> tuple[str, str]:
    """
    Split a line-suffixed slug into (base_slug, line_str).

    Examples
    ────────
    "over_under_goals_2.5"   → ("over_under_goals",   "2.5")
    "asian_handicap_-1.25"   → ("asian_handicap",     "-1.25")
    "european_handicap_2"    → ("european_handicap",  "2")
    "home_goals_0.5"         → ("home_goals",          "0.5")
    "result_and_over_under_2.5" → ("result_and_over_under", "2.5")
    "first_half_over_under_0.75" → ("first_half_over_under", "0.75")
    "1x2"                    → ("1x2",                 "")
    "btts"                   → ("btts",                "")
    """
    m = _LINE_SUFFIX_RE.match(slug)
    if m:
        candidate_base = m.group(1)
        line           = m.group(2)
        # Accept if candidate_base is a known slug
        if candidate_base in MARKET_DISPLAY_NAMES:
            return candidate_base, line
    return slug, ""


# ══════════════════════════════════════════════════════════════════════════════
# DISPLAY NAME FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def get_market_display_name(slug: str) -> str:
    """
    Return a human-readable display name for a canonical market slug.

    Line markets include the line value:
      "over_under_goals_2.5"       → "Goals O/U 2.5"
      "asian_handicap_1.5"         → "Asian Handicap 1.5"
      "european_handicap_-1"       → "European Handicap -1"
      "home_goals_0.5"             → "Home Goals O/U 0.5"
      "result_and_over_under_2.5"  → "Result + O/U 2.5"
      "first_half_over_under_0.75" → "1st Half O/U 0.75"

    Non-line markets:
      "1x2"       → "1X2"
      "btts"      → "Both Teams to Score"
      "ht_ft"     → "HT/FT"
    """
    # Exact match first (no line suffix)
    if slug in MARKET_DISPLAY_NAMES:
        return MARKET_DISPLAY_NAMES[slug]

    # Try splitting off line suffix
    base, line = extract_base_and_line(slug)
    base_name  = MARKET_DISPLAY_NAMES.get(base)
    if base_name:
        return f"{base_name} {line}" if line else base_name

    # Fallback: title-case the slug
    return slug.replace("_", " ").title()


def get_outcome_display(slug: str, outcome_key: str) -> str:
    """
    Return a human-readable label for an outcome within a market.

    Line markets show the line value in the label:
      ("over_under_goals_2.5",      "over")   → "Over 2.5"
      ("over_under_goals_2.5",      "under")  → "Under 2.5"
      ("home_goals_1.5",            "over")   → "Over 1.5"
      ("asian_handicap_1.5",        "1")      → "Home +1.5"
      ("asian_handicap_-1.25",      "2")      → "Away +1.25"
      ("european_handicap_2",       "1")      → "1 (+2)"
      ("european_handicap_2",       "X")      → "X (+2)"
      ("european_handicap_-1",      "2")      → "2 (+1)"  [away gets +1 if home -1]
      ("result_and_over_under_2.5", "1_over") → "Home Over 2.5"
      ("first_half_over_under_1",   "over")   → "Over 1"

    No-line markets use OUTCOME_DISPLAY lookup:
      ("1x2",          "1")   → "Home"
      ("btts",         "yes") → "Yes (GG)"
      ("ht_ft",        "1/2") → "Home / Away"
      ("goal_groups",  "2-3") → "2–3 Goals"
      ("exact_goals",  "3")   → "3 Goals"
    """
    base, line = extract_base_and_line(slug)

    # ── Over/Under line markets ───────────────────────────────────────────────
    if line and base in _OU_SLUGS:
        if outcome_key == "over":
            return f"Over {line}"
        if outcome_key == "under":
            return f"Under {line}"

    # ── Result + O/U combo (market 208) ───────────────────────────────────────
    if line and base == "result_and_over_under":
        _combo_labels: dict[str, str] = {
            "1_over":  f"Home Over {line}",  "X_over":  f"Draw Over {line}",  "2_over":  f"Away Over {line}",
            "1_under": f"Home Under {line}", "X_under": f"Draw Under {line}", "2_under": f"Away Under {line}",
        }
        if outcome_key in _combo_labels:
            return _combo_labels[outcome_key]

    # ── Asian / spread handicap line markets ──────────────────────────────────
    if line and base in _HC_SLUGS:
        try:
            f = float(line)
            if outcome_key in ("1", "home"):
                sign = "+" if f >= 0 else ""
                return f"Home {sign}{line}"
            if outcome_key in ("2", "away"):
                # Away gets the mirror line
                mirror = -f
                sign   = "+" if mirror >= 0 else ""
                mirror_str = str(int(mirror)) if mirror == int(mirror) else str(mirror)
                return f"Away {sign}{mirror_str}"
        except (ValueError, TypeError):
            pass

    # ── European handicap (3-way with a goal head-start) ──────────────────────
    if line and base in _EUR_HC_SLUGS:
        try:
            f = float(line)
            # Positive specValue = home team gives away 'f' goals
            if outcome_key == "1":
                return f"1 (+{line})" if f >= 0 else f"1 ({line})"
            if outcome_key == "X":
                return f"X (+{line})" if f >= 0 else f"X ({line})"
            if outcome_key == "2":
                mirror     = -f
                mirror_str = str(int(mirror)) if mirror == int(mirror) else str(mirror)
                sign       = "+" if mirror >= 0 else ""
                return f"2 ({sign}{mirror_str})"
        except (ValueError, TypeError):
            pass

    # ── Standard OUTCOME_DISPLAY lookup (no line) ─────────────────────────────
    out_map = OUTCOME_DISPLAY.get(base) or OUTCOME_DISPLAY.get(slug)
    if out_map and outcome_key in out_map:
        return out_map[outcome_key]

    # ── Fallback: uppercase key ────────────────────────────────────────────────
    return outcome_key.upper()


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def get_sport_table(sport_id: int) -> dict[int, tuple[str, bool]]:
    """Return the full market-ID lookup table for a sport (with _GENERIC fallbacks)."""
    base   = _SPORT_TABLES.get(sport_id, {})
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
    normalize_sp_market(10,  None,  1)      → "1x2"
    normalize_sp_market(52,  "2.5", 1)      → "over_under_goals_2.5"
    normalize_sp_market(52,  2,     1)      → "over_under_goals_2"
    normalize_sp_market(51, "-1.5", 2)      → "point_spread_-1.5"
    normalize_sp_market(55,  2,     1)      → "european_handicap_2"
    normalize_sp_market(382, None,  5)      → "match_winner"
    normalize_sp_market(226, "22.5",5)      → "total_games_22.5"
    """
    table = get_sport_table(sport_id)
    entry = table.get(mkt_id) or _GENERIC.get(mkt_id)

    if entry is None:
        return f"market_{mkt_id}"

    base, uses_line = entry
    if uses_line and spec_val is not None:
        return slug_with_line(base, spec_val)
    return base


def list_all_slugs(sport_id: int = 1) -> list[str]:
    """Return sorted list of all canonical base slugs for a sport."""
    table = get_sport_table(sport_id)
    return sorted({slug for slug, _ in table.values()})


def get_sport_primary_markets(sport_id: int) -> list[str]:
    """Return ordered list of primary market slugs for a sport."""
    return SPORT_PRIMARY_MARKETS.get(sport_id, ["match_winner"])


def get_sport_meta(sport_id: int) -> dict:
    """Return display metadata for a sport."""
    return SPORT_META.get(sport_id, {"name": f"Sport {sport_id}", "emoji": "🏆", "slugs": []})