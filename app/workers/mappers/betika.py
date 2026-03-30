"""
app/workers/mappers/betika.py
==============================
Betika market normalisation.

When Betika adds a new market or changes a sub_type_id, edit ONLY this file.

Exports
-------
  normalize_bt_market(name, sub_type_id=None)  → canonical slug

Resolution order
────────────────
  1. sub_type_id lookup in _BT_SUBTYPE  (fastest — use whenever available)
  2. Exact name match in _BT_NAME       (Betika API market name strings)
  3. Regex patterns in _BT_PATTERNS     (partial-name fallback)
  4. Sanitise fallback                  (slug of raw name)
"""

from __future__ import annotations

import re


# =============================================================================
# BETIKA SUB_TYPE_ID → SLUG
# =============================================================================
# Source: Betika /v1/uo/match?parent_match_id=... response `sub_type_id` field.
# When in doubt, prefer the sub_type_id branch — it's the most stable identifier.

_BT_SUBTYPE: dict[int, str] = {
    # ── Football ──────────────────────────────────────────────────────────────
    1:   "1x2",
    7:   "match_winner",
    8:   "next_goal",
    10:  "double_chance",
    11:  "draw_no_bet",
    14:  "european_handicap",
    15:  "winning_margin",
    18:  "over_under_goals",
    19:  "total_goals_home",
    20:  "total_goals_away",
    21:  "exact_goals",
    23:  "exact_goals",
    24:  "exact_goals",
    26:  "odd_even",
    29:  "btts",
    31:  "clean_sheet_home",
    32:  "clean_sheet_away",
    33:  "win_to_nil_home",
    34:  "win_to_nil_away",
    35:  "btts_and_result",
    36:  "btts_and_result",
    37:  "result_and_over_under",
    41:  "correct_score",
    45:  "correct_score",
    47:  "ht_ft",
    48:  "score_both_halves",
    49:  "score_both_halves",
    50:  "score_both_halves",
    51:  "score_both_halves",
    52:  "highest_scoring_half",
    55:  "goal_both_halves",
    56:  "score_both_halves",
    57:  "score_both_halves",
    58:  "goal_both_halves",
    59:  "goal_both_halves",
    60:  "first_half_1x2",
    62:  "next_goal",
    63:  "double_chance",
    65:  "european_handicap",
    68:  "first_half_over_under",
    75:  "btts",
    78:  "btts_and_result",
    79:  "result_and_over_under",
    81:  "correct_score",
    83:  "second_half_result",
    85:  "double_chance",
    90:  "second_half_over_under",
    95:  "btts",
    105: "1x2",              # Betika live 1x2 (alternate sub_type)

    # ── Bookings / Cards ──────────────────────────────────────────────────────
    136: "total_bookings",
    137: "total_corners",
    139: "total_bookings",
    142: "total_bookings",

    # ── Corners ───────────────────────────────────────────────────────────────
    162: "total_corners_away",
    163: "total_corners",
    165: "total_corners_home",
    166: "total_bookings",
    168: "1x2",
    172: "total_corners",
    177: "total_corners",
    182: "total_corners",

    # ── Result combos ─────────────────────────────────────────────────────────
    184: "btts_and_result",
    543: "btts_and_result",
    544: "result_and_over_under",
    546: "btts_and_result",
    547: "result_and_over_under",
    548: "number_of_goals",
    549: "total_goals_home",
    550: "total_goals_away",
    552: "number_of_goals",

    # ── Goalscorer markets ────────────────────────────────────────────────────
    638: "anytime_goalscorer",
    639: "first_goalscorer",
    640: "last_goalscorer",
    643: "player_booked",
    647: "clean_sheet_home",
    648: "clean_sheet_away",
    654: "win_to_nil_home",
    655: "win_to_nil_away",
    662: "player_hattrick",
    682: "score_both_halves",
    701: "anytime_goalscorer",
    775: "player_score_2plus",

    # ── HT/FT ─────────────────────────────────────────────────────────────────
    818: "ht_ft",

    # ── Basketball ───────────────────────────────────────────────────────────
    223: "basketball_moneyline",
    340: "match_winner",
    342: "asian_handicap",
}


# =============================================================================
# BETIKA MARKET NAME → SLUG
# =============================================================================
# Betika uses uppercase English names. This table maps the API `name` field.
# Add new rows when Betika introduces markets not yet in _BT_SUBTYPE.

_BT_NAME: dict[str, str] = {
    "1X2":                                   "1x2",
    "MATCH WINNER":                          "1x2",
    "DOUBLE CHANCE":                         "double_chance",
    "DRAW NO BET":                           "draw_no_bet",
    "WHO WILL WIN? (IF DRAW, MONEY BACK)":   "draw_no_bet",
    "TOTAL":                                 "over_under_goals",
    "TOTAL GOALS":                           "over_under_goals",
    "OVER/UNDER":                            "over_under_goals",
    "MULTIGOALS":                            "number_of_goals",
    "EXACT GOALS":                           "exact_goals",
    "NUMBER OF GOALS":                       "number_of_goals",
    "BOTH TEAMS TO SCORE":                   "btts",
    "BOTH TEAMS TO SCORE (GG/NG)":           "btts",
    "GG/NG":                                 "btts",
    "BOTH TEAMS TO SCORE & RESULT":          "btts_and_result",
    "1X2 & BOTH TEAMS TO SCORE":             "btts_and_result",
    "CORRECT SCORE":                         "correct_score",
    "HALFTIME/FULLTIME":                     "ht_ft",
    "HALF TIME / FULL TIME":                 "ht_ft",
    "HANDICAP":                              "european_handicap",
    "EUROPEAN HANDICAP":                     "european_handicap",
    "ASIAN HANDICAP":                        "asian_handicap",
    "1X2 & TOTAL":                           "result_and_over_under",
    "MATCH RESULT & OVER/UNDER":             "result_and_over_under",
    "1ST HALF - 1X2":                        "first_half_1x2",
    "FIRST HALF 1X2":                        "first_half_1x2",
    "HALF TIME RESULT":                      "first_half_1x2",
    "1ST HALF - TOTAL":                      "first_half_over_under",
    "FIRST HALF OVER/UNDER":                 "first_half_over_under",
    "2ND HALF - 1X2":                        "second_half_result",
    "2ND HALF - TOTAL":                      "second_half_over_under",
    "FIRST GOALSCORER":                      "first_goalscorer",
    "LAST GOALSCORER":                       "last_goalscorer",
    "ANYTIME GOALSCORER":                    "anytime_goalscorer",
    "FIRST TEAM TO SCORE":                   "first_team_to_score",
    "LAST TEAM TO SCORE":                    "last_team_to_score",
    "NEXT GOAL":                             "next_goal",
    "CLEAN SHEET":                           "clean_sheet_home",
    "WIN TO NIL":                            "win_to_nil_home",
    "WINNING MARGIN":                        "winning_margin",
    "TOTAL CORNERS":                         "total_corners",
    "ASIAN CORNERS":                         "asian_corners",
    "FIRST CORNER":                          "first_corner",
    "LAST CORNER":                           "last_corner",
    "TOTAL BOOKINGS":                        "total_bookings",
    "TOTAL CARDS":                           "total_bookings",
    "FIRST BOOKING":                         "first_booking",
    "DOUBLE CHANCE & BOTH TEAMS TO SCORE":   "btts_and_result",
    "DOUBLE CHANCE & TOTAL":                 "result_and_over_under",
    "GOAL IN BOTH HALVES":                   "goal_both_halves",
    "BOTH HALVES OVER 1.5":                  "goal_both_halves",
    "1ST/2ND HALF BOTH TEAMS TO SCORE":      "btts",
    "WHICH TEAM WINS THE REST OF THE MATCH": "match_winner",
    "HALFTIME/FULLTIME & TOTAL":             "ht_ft",
    # Basketball
    "MONEYLINE":                             "basketball_moneyline",
    "POINT SPREAD":                          "point_spread",
    "TOTAL POINTS":                          "total_points",
    "WILL THERE BE OVERTIME":                "overtime",
    # Tennis
    "SET BETTING":                           "set_betting",
    "TOTAL SETS":                            "total_sets",
    "TOTAL GAMES":                           "total_games",
    "FIRST SET WINNER":                      "first_set_winner",
    "TIEBREAK IN MATCH":                     "tiebreak_in_match",
}


# =============================================================================
# BETIKA PARTIAL-NAME PATTERNS (fallback for names not in _BT_NAME)
# =============================================================================
# Order matters — more specific patterns first.

_BT_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^HALFTIME/FULLTIME",          re.I), "ht_ft"),
    (re.compile(r"^1ST HALF\s*[–-]",           re.I), "first_half_1x2"),
    (re.compile(r"^2ND HALF\s*[–-]",           re.I), "second_half_result"),
    (re.compile(r"^\d+ MINUTES\s*[–-]",        re.I), "1x2"),
    (re.compile(r"\bCORRECT SCORE\s*\[",        re.I), "correct_score"),
    (re.compile(r"\bTO WIN BOTH HALVES$",        re.I), "score_both_halves"),
    (re.compile(r"\bTO WIN EITHER HALF$",        re.I), "score_both_halves"),
    (re.compile(r"\bTO SCORE IN BOTH HALVES$",   re.I), "score_both_halves"),
    (re.compile(r"WHICH TEAM WINS THE REST",     re.I), "match_winner"),
    (re.compile(r"WHO WILL SCORE \d+\w* GOAL",  re.I), "next_goal"),
    (re.compile(r"\bMULTIGOALS$",              re.I), "number_of_goals"),
    (re.compile(r"\bEXACT GOALS$",             re.I), "exact_goals"),
    (re.compile(r"\bCLEAN SHEET$",             re.I), "clean_sheet_home"),
    (re.compile(r"\bWIN TO NIL$",              re.I), "win_to_nil_home"),
    (re.compile(r"\bTOTAL$",                   re.I), "over_under_goals"),
]


# =============================================================================
# PUBLIC NORMALISER
# =============================================================================

def normalize_bt_market(
    name:         str,
    sub_type_id:  int | str | None = None,
) -> str:
    """
    Return a canonical market slug for a Betika market.

    Parameters
    ----------
    name:         Market name from the Betika API (e.g. "TOTAL GOALS")
    sub_type_id:  Numeric sub_type_id from the API (preferred when available)

    Examples
    --------
    >>> normalize_bt_market("TOTAL GOALS", 18)
    'over_under_goals'
    >>> normalize_bt_market("HALFTIME/FULLTIME", 47)
    'ht_ft'
    >>> normalize_bt_market("1X2 & TOTAL")
    'result_and_over_under'
    """
    # ── 1. sub_type_id lookup (most reliable) ─────────────────────────────────
    if sub_type_id is not None:
        try:
            slug = _BT_SUBTYPE.get(int(sub_type_id))
            if slug:
                return slug
        except (ValueError, TypeError):
            pass

    # ── 2. Exact name match ───────────────────────────────────────────────────
    upper = name.strip().upper()
    if upper in _BT_NAME:
        return _BT_NAME[upper]

    # ── 3. Regex patterns ─────────────────────────────────────────────────────
    for pattern, mapped in _BT_PATTERNS:
        if pattern.search(name):
            return mapped

    # ── 4. Sanitise fallback ──────────────────────────────────────────────────
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_") or "unknown"