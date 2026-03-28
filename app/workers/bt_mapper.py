"""
app/workers/bt_mapper.py
=========================
Betika sub_type_id → canonical slug mapping for every sport.

Public API  (mirrors sp_mapper.py exactly)
──────────────────────────────────────────
  normalize_bt_market(sub_type_id, mkt_name, sport_id) → canonical slug
  normalize_bt_outcome(display, odd_key, slug, sbv)     → canonical outcome key
  extract_bt_line(sbv, display, odd_key)                → line string
  get_bt_sport_table(sport_id)                          → {sub_type_id: (slug, uses_line)}
  get_market_display_name(slug)                         → "Goals O/U 2.5"
  get_outcome_display(slug, outcome_key)                → "Over 2.5"
  bt_sport_to_slug(bt_sport_id)                         → "soccer"
  slug_to_bt_sport_id(sport_slug)                       → 14

Betika live sport IDs (from /v1/uo/sports)
───────────────────────────────────────────
  14  Soccer         30  Basketball   28  Tennis       29  Ice Hockey
  41  Rugby          33  Handball     35  Volleyball   37  Cricket
  45  Table Tennis   32  Badminton   105  eSoccer     107  eBasketball
  40  Futsal        134  ESport LoL  132  ESport CS   133  ESport Dota
"""

from __future__ import annotations

import re
from typing import Any


# ══════════════════════════════════════════════════════════════════════════════
# SPORT ID MAPS
# ══════════════════════════════════════════════════════════════════════════════

BT_TO_CANONICAL_SPORT: dict[int, int] = {
    14:  1,    # Soccer
    30:  2,    # Basketball
    28:  5,    # Tennis
    29:  4,    # Ice Hockey
    41:  12,   # Rugby
    33:  6,    # Handball
    35:  23,   # Volleyball
    37:  21,   # Cricket
    45:  16,   # Table Tennis
    32:  5,    # Badminton (nearest)
    105: 126,  # eSoccer
    107: 2,    # eBasketball
    40:  1,    # Futsal
    134: 126,  # ESport LoL
    132: 126,  # ESport CS
    133: 126,  # ESport Dota
    36:  117,  # MMA
    39:  10,   # Boxing
    44:  49,   # Darts
}

# Betika sport_id → canonical slug
BT_SPORT_SLUGS: dict[int, str] = {
    14:  "soccer",
    30:  "basketball",
    28:  "tennis",
    29:  "ice-hockey",
    41:  "rugby",
    33:  "handball",
    35:  "volleyball",
    37:  "cricket",
    45:  "table-tennis",
    32:  "badminton",
    105: "esoccer",
    107: "basketball",
    40:  "soccer",       # futsal → soccer rules
    134: "esoccer",
    132: "esoccer",
    133: "esoccer",
    36:  "mma",
    39:  "boxing",
    44:  "darts",
}

# Reverse: canonical slug → primary BT sport_id
SLUG_TO_BT_SPORT: dict[str, int] = {
    "soccer":        14,
    "football":      14,
    "basketball":    30,
    "tennis":        28,
    "ice-hockey":    29,
    "rugby":         41,
    "rugby-union":   41,
    "rugby-league":  41,
    "handball":      33,
    "volleyball":    35,
    "cricket":       37,
    "table-tennis":  45,
    "esoccer":       105,
    "efootball":     105,
    "mma":           36,
    "boxing":        39,
    "darts":         44,
}

def bt_sport_to_slug(bt_sport_id: int) -> str:
    return BT_SPORT_SLUGS.get(bt_sport_id, "soccer")

def slug_to_bt_sport_id(sport_slug: str) -> int:
    return SLUG_TO_BT_SPORT.get(sport_slug.lower(), 14)


# ══════════════════════════════════════════════════════════════════════════════
# PER-SPORT SUB_TYPE_ID TABLES
# Format: sub_type_id → (canonical_slug, uses_line)
# ══════════════════════════════════════════════════════════════════════════════

# ── Generic / fallback ────────────────────────────────────────────────────────
_GENERIC_BT: dict[int, tuple[str, bool]] = {
    1:   ("1x2",                       False),
    186: ("match_winner",              False),   # live 1X2 / winner alt
    340: ("over_under",                True),    # live O/U alt
    342: ("asian_handicap",            True),    # live HC alt
    11:  ("draw_no_bet",               False),
    45:  ("correct_score",             False),   # sub_type_id 45 = correct score in Betika
    47:  ("ht_ft",                     False),
}

# ── Football / Soccer  (sport_id 14, 40 Futsal, 105 eSoccer) ────────────────
_FOOTBALL_BT: dict[int, tuple[str, bool]] = {
    # Core
    1:   ("1x2",                          False),
    186: ("match_winner",                 False),
    10:  ("double_chance",                False),
    11:  ("draw_no_bet",                  False),
    29:  ("btts",                         False),
    41:  ("first_team_to_score",          False),
    # Goals O/U (total=X.X in sbv)
    18:  ("over_under_goals",             True),
    340: ("over_under_goals",             True),   # live O/U
    19:  ("home_goals",                   True),
    20:  ("away_goals",                   True),
    # Handicap
    14:  ("european_handicap",            True),   # hcp=0:1
    342: ("asian_handicap",               True),
    # Exact / Goal Groups
    21:  ("exact_goals",                  False),
    23:  ("home_exact_goals",             False),
    24:  ("away_exact_goals",             False),
    548: ("goal_groups",                  False),  # MULTIGOALS
    549: ("home_goal_groups",             False),
    550: ("away_goal_groups",             False),
    # Correct Score / HT-FT
    45:  ("correct_score",                False),
    47:  ("ht_ft",                        False),
    44:  ("ht_ft",                        False),
    # Combos
    35:  ("btts_and_result",              False),
    36:  ("btts_and_over_under",          True),
    37:  ("result_and_over_under",        True),
    546: ("double_chance_and_btts",       False),
    547: ("double_chance_and_over_under", True),
    818: ("ht_ft_and_over_under",         True),
    # Winning margin
    15:  ("winning_margin",               False),
    # Clean sheet / win to nil
    31:  ("clean_sheet_home",             False),
    32:  ("clean_sheet_away",             False),
    33:  ("home_win_to_nil",              False),
    34:  ("away_win_to_nil",              False),
    # Both halves
    48:  ("home_win_both_halves",         False),
    49:  ("away_win_both_halves",         False),
    50:  ("home_win_either_half",         False),
    51:  ("away_win_either_half",         False),
    55:  ("both_halves_btts",             False),
    56:  ("home_score_both_halves",       False),
    57:  ("away_score_both_halves",       False),
    58:  ("both_halves_over",             True),
    59:  ("both_halves_under",            True),
    # 1st Half
    60:  ("first_half_1x2",              False),
    63:  ("first_half_double_chance",     False),
    65:  ("first_half_european_handicap", True),
    68:  ("first_half_over_under",        True),
    69:  ("first_half_home_goals",        True),
    70:  ("first_half_away_goals",        True),
    71:  ("first_half_exact_goals",       False),
    75:  ("first_half_btts",             False),
    78:  ("first_half_btts_and_result",   False),
    79:  ("first_half_result_and_over_under", True),
    81:  ("first_half_correct_score",    False),
    542: ("first_half_double_chance_and_btts", False),
    552: ("first_half_goal_groups",       False),
    # Odd/Even
    45:  ("correct_score",                False),  # duplicate in Betika, 45 = correct_score
}

# ── Basketball  (sport_id 30, 107) ────────────────────────────────────────────
_BASKETBALL_BT: dict[int, tuple[str, bool]] = {
    186: ("match_winner",            False),
    1:   ("match_winner",            False),
    340: ("total_points",            True),
    342: ("point_spread",            True),
    14:  ("point_spread",            True),
    18:  ("total_points",            True),
    11:  ("match_winner",            False),   # draw no bet → match winner for BB
    35:  ("odd_even",                False),
    362: ("q1_total",                True),
    363: ("q2_total",                True),
    364: ("q3_total",                True),
    365: ("q4_total",                True),
    366: ("q1_spread",               True),
    367: ("q2_spread",               True),
    368: ("q3_spread",               True),
    369: ("q4_spread",               True),
    60:  ("first_half_winner",       False),
    68:  ("first_half_total",        True),
    65:  ("first_half_spread",       True),
    222: ("winning_margin",          False),
}

# ── Tennis  (sport_id 28) ─────────────────────────────────────────────────────
_TENNIS_BT: dict[int, tuple[str, bool]] = {
    186: ("match_winner",             False),
    1:   ("match_winner",             False),
    204: ("first_set_winner",         False),
    231: ("second_set_winner",        False),
    14:  ("game_handicap",            True),
    18:  ("total_games",              True),
    340: ("total_games",              True),
    233: ("set_betting",              False),
    439: ("set_handicap",             True),
    35:  ("odd_even_games",           False),
    339: ("first_set_game_handicap",  True),
    340: ("first_set_total_games",    True),
    433: ("first_set_match_winner",   False),
}

# ── Ice Hockey  (sport_id 29) ─────────────────────────────────────────────────
_ICE_HOCKEY_BT: dict[int, tuple[str, bool]] = {
    1:   ("1x2",                  False),
    186: ("match_winner",         False),
    29:  ("btts",                 False),
    10:  ("double_chance",        False),
    11:  ("draw_no_bet",          False),
    14:  ("puck_line",            True),
    18:  ("over_under_goals",     True),
    340: ("over_under_goals",     True),
    60:  ("first_period_winner",  False),
    378: ("match_winner_ot",      False),
    19:  ("home_goals",           True),
    20:  ("away_goals",           True),
}

# ── Volleyball  (sport_id 35) ─────────────────────────────────────────────────
_VOLLEYBALL_BT: dict[int, tuple[str, bool]] = {
    186: ("match_winner",   False),
    1:   ("match_winner",   False),
    204: ("first_set_winner", False),
    14:  ("set_handicap",   True),
    18:  ("total_sets",     True),
    340: ("total_sets",     True),
    233: ("set_betting",    False),
    35:  ("odd_even",       False),
}

# ── Cricket  (sport_id 37) ────────────────────────────────────────────────────
_CRICKET_BT: dict[int, tuple[str, bool]] = {
    186: ("match_winner",  False),
    1:   ("1x2",           False),
    14:  ("run_handicap",  True),
    18:  ("total_runs",    True),
    340: ("total_runs",    True),
    19:  ("home_runs",     True),
    20:  ("away_runs",     True),
}

# ── Rugby  (sport_id 41) ──────────────────────────────────────────────────────
_RUGBY_BT: dict[int, tuple[str, bool]] = {
    1:   ("1x2",               False),
    10:  ("double_chance",     False),
    11:  ("draw_no_bet",       False),
    186: ("match_winner",      False),
    14:  ("asian_handicap",    True),
    18:  ("total_points",      True),
    340: ("total_points",      True),
    19:  ("home_points",       True),
    20:  ("away_points",       True),
    379: ("winning_margin",    False),
    47:  ("ht_ft",             False),
    60:  ("first_half_1x2",   False),
}

# ── Handball  (sport_id 33) ───────────────────────────────────────────────────
_HANDBALL_BT: dict[int, tuple[str, bool]] = {
    1:   ("1x2",               False),
    10:  ("double_chance",     False),
    11:  ("draw_no_bet",       False),
    186: ("match_winner",      False),
    14:  ("asian_handicap",    True),
    18:  ("over_under_goals",  True),
    340: ("over_under_goals",  True),
    29:  ("btts",              False),
    19:  ("home_goals",        True),
    20:  ("away_goals",        True),
    60:  ("first_half_1x2",   False),
    68:  ("first_half_over_under", True),
}

# ── Table Tennis  (sport_id 45) ───────────────────────────────────────────────
_TABLE_TENNIS_BT: dict[int, tuple[str, bool]] = {
    186: ("match_winner",         False),
    1:   ("match_winner",         False),
    14:  ("game_handicap",        True),
    18:  ("total_games",          True),
    340: ("total_games",          True),
    35:  ("odd_even",             False),
    233: ("set_betting",          False),
    340: ("first_set_total_games",True),
}

# ── MMA / Boxing  (sport_id 36, 39) ──────────────────────────────────────────
_COMBAT_BT: dict[int, tuple[str, bool]] = {
    186: ("match_winner",  False),
    1:   ("match_winner",  False),
    14:  ("round_betting", True),
    18:  ("total_rounds",  True),
    340: ("total_rounds",  True),
}

# ── Darts  (sport_id 44) ──────────────────────────────────────────────────────
_DARTS_BT: dict[int, tuple[str, bool]] = {
    186: ("match_winner", False),
    1:   ("match_winner", False),
    18:  ("total_legs",   True),
    340: ("total_legs",   True),
    14:  ("leg_handicap", True),
    35:  ("odd_even",     False),
}


# ══════════════════════════════════════════════════════════════════════════════
# BT_SPORT_ID → TABLE
# ══════════════════════════════════════════════════════════════════════════════

_BT_SPORT_TABLES: dict[int, dict[int, tuple[str, bool]]] = {
    14:  _FOOTBALL_BT,
    40:  _FOOTBALL_BT,   # futsal
    105: _FOOTBALL_BT,   # esoccer
    107: _BASKETBALL_BT, # ebasketball
    28:  _TENNIS_BT,
    29:  _ICE_HOCKEY_BT,
    30:  _BASKETBALL_BT,
    33:  _HANDBALL_BT,
    35:  _VOLLEYBALL_BT,
    37:  _CRICKET_BT,
    41:  _RUGBY_BT,
    45:  _TABLE_TENNIS_BT,
    32:  _TENNIS_BT,     # badminton ≈ tennis
    36:  _COMBAT_BT,     # MMA
    39:  _COMBAT_BT,     # boxing
    44:  _DARTS_BT,
    132: _FOOTBALL_BT,   # esport CS → soccer style
    133: _FOOTBALL_BT,   # esport Dota
    134: _FOOTBALL_BT,   # esport LoL
}


def get_bt_sport_table(bt_sport_id: int) -> dict[int, tuple[str, bool]]:
    """Return merged market table (sport-specific + generic fallbacks)."""
    merged = dict(_GENERIC_BT)
    merged.update(_BT_SPORT_TABLES.get(bt_sport_id, {}))
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# DISPLAY NAME TABLE  (re-export compatible with sp_mapper.MARKET_DISPLAY_NAMES)
# ══════════════════════════════════════════════════════════════════════════════

MARKET_DISPLAY_NAMES: dict[str, str] = {
    "match_winner":                    "Match Winner",
    "1x2":                             "1X2",
    "asian_handicap":                  "Asian Handicap",
    "european_handicap":               "European Handicap",
    "over_under":                      "Over/Under",
    "odd_even":                        "Odd/Even",
    "correct_score":                   "Correct Score",
    "ht_ft":                           "HT/FT",
    "winning_margin":                  "Winning Margin",
    "over_under_goals":                "Goals O/U",
    "double_chance":                   "Double Chance",
    "draw_no_bet":                     "Draw No Bet",
    "btts":                            "Both Teams to Score",
    "btts_and_result":                 "BTTS + Result",
    "btts_and_over_under":             "BTTS + O/U",
    "result_and_over_under":           "Result + O/U",
    "first_team_to_score":             "First Team to Score",
    "exact_goals":                     "Exact Goals",
    "home_exact_goals":                "Home Exact Goals",
    "away_exact_goals":                "Away Exact Goals",
    "goal_groups":                     "Goal Groups",
    "home_goal_groups":                "Home Goal Groups",
    "away_goal_groups":                "Away Goal Groups",
    "first_half_goal_groups":          "1st Half Goal Groups",
    "highest_scoring_half":            "Half with Most Goals",
    "first_half_1x2":                  "1st Half 1X2",
    "first_half_over_under":           "1st Half O/U",
    "first_half_btts":                 "1st Half BTTS",
    "first_half_correct_score":        "1st Half Correct Score",
    "first_half_european_handicap":    "1st Half HC",
    "first_half_asian_handicap":       "1st Half Asian HC",
    "first_half_home_goals":           "1st Half Home Goals O/U",
    "first_half_away_goals":           "1st Half Away Goals O/U",
    "first_half_exact_goals":          "1st Half Exact Goals",
    "first_half_double_chance":        "1st Half Double Chance",
    "first_half_double_chance_and_btts": "1st Half DC + BTTS",
    "first_half_btts_and_result":      "1st Half BTTS + Result",
    "first_half_result_and_over_under": "1st Half Result + O/U",
    "home_goals":                      "Home Goals O/U",
    "away_goals":                      "Away Goals O/U",
    "total_corners":                   "Total Corners",
    "total_bookings":                  "Total Bookings",
    "point_spread":                    "Point Spread",
    "total_points":                    "Total Points",
    "home_total_points":               "Home Points O/U",
    "away_total_points":               "Away Points O/U",
    "first_half_winner":               "1st Half Winner",
    "first_half_spread":               "1st Half Spread",
    "first_half_total":                "1st Half Total",
    "highest_scoring_quarter":         "Highest Scoring Quarter",
    "q1_total":                        "Q1 Total",
    "q2_total":                        "Q2 Total",
    "q3_total":                        "Q3 Total",
    "q4_total":                        "Q4 Total",
    "q1_spread":                       "Q1 Spread",
    "q2_spread":                       "Q2 Spread",
    "q3_spread":                       "Q3 Spread",
    "q4_spread":                       "Q4 Spread",
    "first_set_winner":                "1st Set Winner",
    "second_set_winner":               "2nd Set Winner",
    "game_handicap":                   "Game Handicap",
    "total_games":                     "Total Games",
    "set_betting":                     "Set Betting",
    "set_handicap":                    "Set Handicap",
    "odd_even_games":                  "Odd/Even Games",
    "first_set_game_handicap":         "1st Set Game HC",
    "first_set_total_games":           "1st Set Total Games",
    "first_set_match_winner":          "1st Set / Match Winner",
    "player1_games":                   "Player 1 Games O/U",
    "player2_games":                   "Player 2 Games O/U",
    "puck_line":                       "Puck Line",
    "first_period_winner":             "1st Period Winner",
    "match_winner_ot":                 "Winner (OT incl.)",
    "home_points":                     "Home Points O/U",
    "away_points":                     "Away Points O/U",
    "total_sets":                      "Total Sets",
    "total_runs":                      "Total Runs",
    "home_runs":                       "Home Runs O/U",
    "away_runs":                       "Away Runs O/U",
    "run_handicap":                    "Run Handicap",
    "round_betting":                   "Round Betting",
    "total_rounds":                    "Total Rounds",
    "total_legs":                      "Total Legs",
    "leg_handicap":                    "Leg Handicap",
    "run_line":                        "Run Line",
    "home_runs_total":                 "Home Runs O/U",
    "away_runs_total":                 "Away Runs O/U",
    "home_total":                      "Home Total O/U",
    "away_total":                      "Away Total O/U",
    # Betika-specific extras
    "clean_sheet_home":                "Home Clean Sheet",
    "clean_sheet_away":                "Away Clean Sheet",
    "home_win_to_nil":                 "Home Win to Nil",
    "away_win_to_nil":                 "Away Win to Nil",
    "double_chance_and_btts":          "Double Chance + BTTS",
    "double_chance_and_over_under":    "Double Chance + O/U",
    "ht_ft_and_over_under":            "HT/FT + O/U",
    "home_win_both_halves":            "Home Win Both Halves",
    "away_win_both_halves":            "Away Win Both Halves",
    "home_win_either_half":            "Home Win Either Half",
    "away_win_either_half":            "Away Win Either Half",
    "both_halves_btts":                "Both Halves BTTS",
    "home_score_both_halves":          "Home Score Both Halves",
    "away_score_both_halves":          "Away Score Both Halves",
    "both_halves_over":                "Both Halves Over",
    "both_halves_under":               "Both Halves Under",
    "match_winner_live":               "Match Winner (Live)",
    "over_under_live":                 "O/U (Live)",
    "over_under_live_alt":             "Handicap (Live)",
}

# ── O/U slugs (used in outcome display) ──────────────────────────────────────
_OU_SLUGS: frozenset[str] = frozenset({
    "over_under_goals","over_under","total_points","total_runs","total_games",
    "total_legs","total_sets","total_rounds","total_corners","total_bookings",
    "home_goals","away_goals","home_points","away_points","home_total","away_total",
    "home_total_points","away_total_points","home_runs","away_runs",
    "home_runs_total","away_runs_total","player1_games","player2_games",
    "first_half_over_under","first_half_total","second_half_over_under",
    "q1_total","q2_total","q3_total","q4_total","first_set_total_games",
    "over_under_live","both_halves_over","both_halves_under",
    "first_half_home_goals","first_half_away_goals",
    "btts_and_over_under","double_chance_and_over_under","ht_ft_and_over_under",
})

_HC_SLUGS: frozenset[str] = frozenset({
    "asian_handicap","first_half_asian_handicap","first_half_european_handicap",
    "point_spread","puck_line","run_line","set_handicap","game_handicap",
    "first_set_game_handicap","leg_handicap","run_handicap",
    "q1_spread","q2_spread","q3_spread","q4_spread","first_half_spread",
    "over_under_live_alt",
})

_EUR_HC_SLUGS: frozenset[str] = frozenset({
    "european_handicap",
})


# ══════════════════════════════════════════════════════════════════════════════
# OUTCOME DISPLAY TABLE
# ══════════════════════════════════════════════════════════════════════════════

OUTCOME_DISPLAY: dict[str, dict[str, str]] = {
    "1x2":                {"1":"Home","X":"Draw","2":"Away"},
    "match_winner":       {"1":"Home","2":"Away"},
    "double_chance":      {"1X":"Home or Draw","X2":"Draw or Away","12":"Home or Away"},
    "draw_no_bet":        {"1":"Home","2":"Away"},
    "btts":               {"yes":"Yes (GG)","no":"No (NG)"},
    "first_half_btts":    {"yes":"Yes (GG)","no":"No (NG)"},
    "both_halves_btts":   {"yes":"Yes","no":"No"},
    "home_score_both_halves": {"yes":"Yes","no":"No"},
    "away_score_both_halves": {"yes":"Yes","no":"No"},
    "home_win_both_halves":   {"yes":"Yes","no":"No"},
    "away_win_both_halves":   {"yes":"Yes","no":"No"},
    "home_win_either_half":   {"yes":"Yes","no":"No"},
    "away_win_either_half":   {"yes":"Yes","no":"No"},
    "clean_sheet_home":   {"yes":"Yes","no":"No"},
    "clean_sheet_away":   {"yes":"Yes","no":"No"},
    "home_win_to_nil":    {"yes":"Yes","no":"No"},
    "away_win_to_nil":    {"yes":"Yes","no":"No"},
    "odd_even":           {"odd":"Odd","even":"Even"},
    "odd_even_games":     {"odd":"Odd","even":"Even"},
    "first_team_to_score":{"1":"Home","2":"Away","none":"No Goal"},
    "highest_scoring_half":{"1st":"1st Half","2nd":"2nd Half","equal":"Equal"},
    "highest_scoring_quarter":{"1st_quarter":"Q1","2nd_quarter":"Q2","3rd_quarter":"Q3","4th_quarter":"Q4"},
    "set_betting":        {"2:0":"2-0","2:1":"2-1","0:2":"0-2","1:2":"1-2","3:0":"3-0","3:1":"3-1","3:2":"3-2","0:3":"0-3","1:3":"1-3","2:3":"2-3"},
    "goal_groups":        {"1-2":"1-2","2-3":"2-3","3-4":"3-4","4-5":"4-5","5-6":"5-6","6+":"6+","7+":"7+","0-1":"0-1","no goal":"No Goal"},
    "home_goal_groups":   {"1-2":"1-2","2-3":"2-3","4+":"4+","no goal":"No Goal"},
    "away_goal_groups":   {"1-2":"1-2","2-3":"2-3","4+":"4+","no goal":"No Goal"},
    "first_half_goal_groups": {"1-2":"1-2","2-3":"2-3","4+":"4+","no goal":"No Goal"},
    "exact_goals":        {"0":"0","1":"1","2":"2","3":"3","4":"4","5":"5","6+":"6+","other":"Other"},
    "home_exact_goals":   {"0":"0","1":"1","2":"2","3+":"3+"},
    "away_exact_goals":   {"0":"0","1":"1","2":"2","3+":"3+"},
    "first_half_exact_goals": {"0":"0","1":"1","2+":"2+"},
    "correct_score":      {"other":"Other"},
    "first_half_correct_score": {"other":"Other"},
    "ht_ft":              {"1/1":"H/H","1/X":"H/D","1/2":"H/A","X/1":"D/H","X/X":"D/D","X/2":"D/A","2/1":"A/H","2/X":"A/D","2/2":"A/A"},
    "btts_and_result":    {"1_yes":"Home+GG","X_yes":"Draw+GG","2_yes":"Away+GG","1_no":"Home+NG","X_no":"Draw+NG","2_no":"Away+NG"},
    "first_half_1x2":     {"1":"Home","X":"Draw","2":"Away"},
    "first_half_double_chance": {"1X":"H/D","X2":"D/A","12":"H/A"},
    "first_set_match_winner": {"1/1":"H/H","1/2":"H/A","2/1":"A/H","2/2":"A/A"},
}

SPORT_PRIMARY_MARKETS: dict[int, list[str]] = {
    14:  ["1x2","over_under_goals","btts","double_chance","european_handicap","first_half_1x2","correct_score","ht_ft"],
    105: ["1x2","over_under_goals","btts","double_chance","european_handicap"],
    30:  ["match_winner","point_spread","total_points","first_half_winner"],
    28:  ["match_winner","first_set_winner","total_games","set_betting","game_handicap"],
    29:  ["1x2","match_winner","over_under_goals","puck_line","btts"],
    41:  ["1x2","match_winner","asian_handicap","total_points"],
    35:  ["match_winner","set_handicap","total_sets","set_betting"],
    37:  ["match_winner","1x2","total_runs","run_handicap"],
    33:  ["1x2","match_winner","over_under_goals","asian_handicap","btts"],
    45:  ["match_winner","game_handicap","total_games"],
    36:  ["match_winner","total_rounds","round_betting"],
    39:  ["match_winner","total_rounds","round_betting"],
    44:  ["match_winner","total_legs","odd_even","leg_handicap"],
}


# ══════════════════════════════════════════════════════════════════════════════
# LINE EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

_LINE_RE = re.compile(r"^(.+?)_(-?\d+(?:\.\d+)?)$")


def extract_base_and_line(slug: str) -> tuple[str, str]:
    """Split "over_under_goals_2.5" → ("over_under_goals", "2.5")."""
    m = _LINE_RE.match(slug)
    if m and m.group(1) in MARKET_DISPLAY_NAMES:
        return m.group(1), m.group(2)
    return slug, ""


def _hcp_to_line(hcp_str: str) -> str:
    """
    Convert Betika handicap "0:1" → "-1"  (home gets -1 goal adjustment)
    "1:0" → "1"   "0:2" → "-2"   "2:1" → "1"
    """
    try:
        parts = hcp_str.strip().split(":")
        home, away = int(parts[0]), int(parts[1])
        val = home - away
        return str(val)
    except (ValueError, IndexError):
        return hcp_str


def extract_bt_line(sbv: str, display: str = "", odd_key: str = "") -> str:
    """
    Extract line specifier from Betika special_bet_value.

    "total=2.5"                → "2.5"
    "hcp=0:1"                  → "-1"   (home -1 perspective)
    "hcp=1:0"                  → "1"
    "variant=sr:exact_goals:6+"→ ""     (no numeric line)
    ""   + display "OVER 2.5"  → "2.5"  (fallback from display)
    """
    if sbv:
        if sbv.startswith("total="):
            return sbv[6:].strip()
        if sbv.startswith("hcp="):
            return _hcp_to_line(sbv[4:])
        if sbv.startswith("variant="):
            return ""
        if "=" in sbv:
            return sbv.split("=", 1)[1].strip()
    # Fallback: parse from display / odd_key
    for src in (display, odd_key):
        m = re.match(r"(?:over|under)\s+([\d.]+)", src.strip(), re.I)
        if m:
            return m.group(1)
    return ""


# ══════════════════════════════════════════════════════════════════════════════
# OUTCOME NORMALIZATION  (Betika display → canonical key)
# ══════════════════════════════════════════════════════════════════════════════

# Double-chance display variants → canonical
_DC_MAP: dict[str, str] = {
    "1/x": "1X", "x/2": "X2", "1/2": "12",
}

# HT/FT display → canonical
_HTFT_MAP: dict[str, str] = {
    "x/x":"X/X","x/2":"X/2","x/1":"X/1",
    "2/x":"2/X","2/2":"2/2","2/1":"2/1",
    "1/x":"1/X","1/2":"1/2","1/1":"1/1",
}


def normalize_bt_outcome(
    display:  str,
    odd_key:  str,
    slug:     str,
    sbv:      str = "",
) -> str:
    """
    Map a Betika odd's display/odd_key to our canonical outcome key.

    Canonical keys are the same vocabulary as SP:
      "1", "X", "2", "1X", "X2", "12", "yes", "no", "over", "under",
      "odd", "even",  "1/1", "X/X", "1/2", ... (HT/FT)
      "2-3", "6+", "other", "0:0", "1:2" (score), ...

    We prioritise `display` because Betika's `odd_key` often contains
    full team names like "Senegal" instead of "1".
    """
    d  = display.strip()
    dk = odd_key.strip().lower()
    d_up = d.upper()
    d_lo = d.lower()

    base, _line = extract_base_and_line(slug)

    # ── 1X2 / match winner ────────────────────────────────────────────────────
    if d_up in ("1", "X", "2"):
        return d_up

    # ── Double chance: "1/X", "X/2", "1/2" ───────────────────────────────────
    dc = _DC_MAP.get(d_lo)
    if dc:
        return dc

    # ── Yes / No ──────────────────────────────────────────────────────────────
    if d_up in ("YES", "NO"):
        return d_lo

    # ── Odd / Even ────────────────────────────────────────────────────────────
    if d_up in ("ODD", "EVEN"):
        return d_lo

    # ── Over / Under ─────────────────────────────────────────────────────────
    if dk.startswith("over ") or d_up.startswith("OVER "):
        return "over"
    if dk.startswith("under ") or d_up.startswith("UNDER "):
        return "under"

    # ── Handicap display: "1 (0:1)", "X (0:2)", "2 (1:0)" → "1", "X", "2" ──
    m = re.match(r"^([1X2])\s*\(", d_up)
    if m:
        return m.group(1)

    # ── HT/FT: "X/X", "1/1", "2/1" etc ─────────────────────────────────────
    htft = _HTFT_MAP.get(d_lo)
    if htft:
        return htft
    htft_m = re.match(r"^([12X])/([12X])$", d_up)
    if htft_m:
        return f"{htft_m.group(1)}/{htft_m.group(2)}"

    # ── Correct score: "0:0", "3:2" ──────────────────────────────────────────
    if re.match(r"^\d+:\d+$", d):
        return d

    # ── "OTHER" ───────────────────────────────────────────────────────────────
    if d_up == "OTHER":
        return "other"

    # ── No Goal ───────────────────────────────────────────────────────────────
    if d_up == "NO GOAL" or dk == "no goal":
        return "none"

    # ── Goal groups / multigoals: "1-2", "2-3", "7+" etc ────────────────────
    if re.match(r"^\d+-\d+$", d):
        return d
    if re.match(r"^\d+\+$", d):
        return d

    # ── Exact goals: "0", "1", "2", "3+", "6+" ───────────────────────────────
    if re.match(r"^\d+\+?$", d):
        return d

    # ── Winning margin: "1 BY 1", "2 BY 3+" → normalise ─────────────────────
    wm = re.match(r"^([12])\s+BY\s+(\d+\+?)$", d_up)
    if wm:
        return f"{wm.group(1)}_by_{wm.group(2)}"
    if d_up == "X":
        return "X"

    # ── Combo markets: "OVER 2.5 & YES", "1 & NO" → use odd_key ─────────────
    # Strip team names and compress
    clean = re.sub(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', '', d)
    clean = clean.strip().lower().replace(" & ", "_and_").replace("/", "_").replace(" ", "_")
    return clean or dk.replace(" ", "_")


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC NORMALISATION API
# ══════════════════════════════════════════════════════════════════════════════

def normalize_bt_market(
    sub_type_id: int | str,
    mkt_name:    str,
    bt_sport_id: int = 14,
    sbv:         str = "",
) -> str:
    """
    Return canonical slug for a Betika market.

    Tries sub_type_id lookup first, then falls back to name-based heuristics.
    """
    sid   = int(sub_type_id)
    table = get_bt_sport_table(bt_sport_id)
    entry = table.get(sid) or _GENERIC_BT.get(sid)

    if entry:
        base, uses_line = entry
        if uses_line and sbv:
            line = extract_bt_line(sbv)
            if line:
                return f"{base}_{line}"
        return base

    # Name-based fallback
    name = mkt_name.strip().upper()
    if "1X2" in name or name.startswith("1X2"):
        return "1x2"
    if "DOUBLE CHANCE" in name:
        return "double_chance"
    if "BTTS" in name or "BOTH TEAMS TO SCORE" in name:
        return "btts"
    if "TOTAL" in name and "HALF" not in name:
        line = extract_bt_line(sbv)
        return f"over_under_goals_{line}" if line else "over_under_goals"
    if "HANDICAP" in name:
        line = extract_bt_line(sbv)
        base = "first_half_european_handicap" if "1ST HALF" in name else "european_handicap"
        return f"{base}_{line}" if line else base
    if "CORRECT SCORE" in name:
        return "first_half_correct_score" if "1ST HALF" in name else "correct_score"
    if "HALFTIME" in name or "HT/FT" in name:
        return "ht_ft"
    if "MULTIGOAL" in name:
        return "goal_groups"

    return f"market_{sid}"


def normalize_bt_outcome_simple(display: str, odd_key: str, slug: str, sbv: str = "") -> str:
    """Alias for normalize_bt_outcome (matches canonical_mapper call signature)."""
    return normalize_bt_outcome(display, odd_key, slug, sbv)


# ══════════════════════════════════════════════════════════════════════════════
# DISPLAY NAME FUNCTIONS  (mirrors sp_mapper.py public API)
# ══════════════════════════════════════════════════════════════════════════════

def get_market_display_name(slug: str) -> str:
    if slug in MARKET_DISPLAY_NAMES:
        return MARKET_DISPLAY_NAMES[slug]
    base, line = extract_base_and_line(slug)
    name = MARKET_DISPLAY_NAMES.get(base)
    if name:
        return f"{name} {line}" if line else name
    return slug.replace("_", " ").title()


def get_outcome_display(slug: str, outcome_key: str) -> str:
    """Human label for an outcome within a market (same logic as sp_mapper)."""
    base, line = extract_base_and_line(slug)

    # O/U line markets
    if line and base in _OU_SLUGS:
        if outcome_key == "over":   return f"Over {line}"
        if outcome_key == "under":  return f"Under {line}"

    # Asian / spread HC
    if line and base in _HC_SLUGS:
        try:
            f = float(line)
            if outcome_key in ("1","home"):
                return f"Home {'+' if f>=0 else ''}{line}"
            if outcome_key in ("2","away"):
                r = -f; rs = str(int(r)) if r==int(r) else str(r)
                return f"Away {'+' if r>=0 else ''}{rs}"
        except (ValueError, TypeError):
            pass

    # European HC
    if line and base in _EUR_HC_SLUGS:
        try:
            f = float(line); sg = "+" if f >= 0 else ""
            if outcome_key == "1": return f"1 ({sg}{line})"
            if outcome_key == "X": return f"X ({sg}{line})"
            if outcome_key == "2":
                r = -f; rs = str(int(r)) if r==int(r) else str(r)
                return f"2 ({'+' if r>=0 else ''}{rs})"
        except (ValueError, TypeError):
            pass

    # Standard lookup
    om = OUTCOME_DISPLAY.get(base) or OUTCOME_DISPLAY.get(slug)
    if om and outcome_key in om:
        return om[outcome_key]

    if re.match(r"^\d+:\d+$", outcome_key):
        return outcome_key

    return outcome_key.upper()


def get_bt_sport_primary_markets(bt_sport_id: int) -> list[str]:
    return SPORT_PRIMARY_MARKETS.get(bt_sport_id, ["match_winner"])


# ── Convenience: build {outcome_key: odd} dict from a Betika odds list ───────

def parse_bt_odds_to_dict(
    odds_list:   list[dict],
    slug:        str,
    sbv:         str = "",
) -> dict[str, float]:
    """
    Convert Betika odds array to canonical {outcome_key: float} dict.

    Handles multiple lines by grouping under the slug's own key.
    For multi-line markets (O/U 0.5, 1.5, 2.5 ...) caller must pass
    the already-resolved slug (e.g. "over_under_goals_2.5").
    """
    result: dict[str, float] = {}
    for o in odds_list:
        try:
            odd_val = float(o.get("odd_value") or 0)
        except (TypeError, ValueError):
            odd_val = 0.0
        if odd_val <= 1.0:
            continue
        display  = (o.get("display") or "").strip()
        odd_key  = (o.get("odd_key")  or "").strip()
        item_sbv = (o.get("special_bet_value") or sbv).strip()
        canon    = normalize_bt_outcome(display, odd_key, slug, item_sbv)
        result[canon] = odd_val
    return result