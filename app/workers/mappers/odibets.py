"""
app/workers/mappers/odibets.py
================================
OdiBets market normalisation — single source of truth.

When OdiBets adds a new sub_type_id, edit ONLY this file.

Exports
-------
  normalize_od_market(sub_type_id, specifiers="")  → canonical slug

Table layout
------------
  _OD_MKT[str(sub_type_id)] = (base_slug, uses_line: bool)

  uses_line=True  → specifiers carries a numeric line value (e.g. "2.5")
                    appended to produce "over_under_goals_2.5".
  uses_line=False → specifiers is ignored.

Examples
--------
  >>> normalize_od_market(18, "2.5")
  'over_under_goals_2.5'
  >>> normalize_od_market(29)
  'btts'
  >>> normalize_od_market(999)
  'od_999'
"""

from __future__ import annotations

from app.workers.mappers.shared import slug_with_line


# =============================================================================
# ODIBETS SUB_TYPE_ID → CANONICAL SLUG TABLE  (144 entries)
# =============================================================================

_OD_MKT: dict[str, tuple[str, bool]] = {

    # ── Core football — 1X2 / match result ───────────────────────────────────
    "1":   ("1x2",                        False),
    "7":   ("match_winner",               False),
    "8":   ("next_goal",                  False),
    "9":   ("last_team_to_score",         False),
    "10":  ("double_chance",              False),
    "11":  ("draw_no_bet",                False),
    "12":  ("draw_no_bet",                False),
    "13":  ("draw_no_bet",                False),
    "14":  ("european_handicap",          True),
    "15":  ("winning_margin",             False),
    "16":  ("asian_handicap",             True),

    # ── Goals over/under ──────────────────────────────────────────────────────
    "18":  ("over_under_goals",           True),
    "19":  ("total_goals_home",           True),
    "20":  ("total_goals_away",           True),

    # ── Exact / number of goals ───────────────────────────────────────────────
    "21":  ("exact_goals",                False),
    "22":  ("number_of_goals",            False),
    "23":  ("exact_goals",                False),
    "24":  ("exact_goals",                False),
    "25":  ("exact_goals",                False),

    # ── Odd/even ──────────────────────────────────────────────────────────────
    "26":  ("odd_even",                   False),
    "27":  ("odd_even",                   False),
    "28":  ("odd_even",                   False),

    # ── BTTS / clean sheets / win-to-nil ──────────────────────────────────────
    "29":  ("btts",                       False),
    "30":  ("first_team_to_score",        False),
    "31":  ("clean_sheet_home",           False),
    "32":  ("clean_sheet_away",           False),
    "33":  ("win_to_nil_home",            False),
    "34":  ("win_to_nil_away",            False),

    # ── BTTS + result / result + over-under ───────────────────────────────────
    "35":  ("btts_and_result",            False),
    "36":  ("btts_and_result",            True),
    "37":  ("result_and_over_under",      True),

    # ── Goalscorer specials ───────────────────────────────────────────────────
    "38":  ("first_goalscorer",           False),
    "39":  ("last_goalscorer",            False),
    "40":  ("anytime_goalscorer",         False),

    # ── Correct score ─────────────────────────────────────────────────────────
    "41":  ("correct_score",              False),
    "45":  ("correct_score",              False),

    # ── HT/FT ─────────────────────────────────────────────────────────────────
    "46":  ("ht_ft",                      False),
    "47":  ("ht_ft",                      False),

    # ── Both halves / scoring ─────────────────────────────────────────────────
    "48":  ("score_both_halves",          False),
    "49":  ("score_both_halves",          False),
    "50":  ("score_both_halves",          False),
    "51":  ("score_both_halves",          False),
    "52":  ("highest_scoring_half",       False),
    "55":  ("goal_both_halves",           False),
    "56":  ("score_both_halves",          False),
    "57":  ("score_both_halves",          False),
    "58":  ("goal_both_halves",           True),
    "59":  ("goal_both_halves",           True),

    # ── First-half markets ────────────────────────────────────────────────────
    "60":  ("first_half_1x2",             False),
    "61":  ("first_half_1x2",             False),
    "62":  ("next_goal",                  False),
    "63":  ("double_chance",              False),
    "64":  ("draw_no_bet",                False),
    "65":  ("european_handicap",          True),
    "66":  ("asian_handicap",             True),
    "67":  ("btts",                       False),
    "68":  ("first_half_over_under",      True),
    "69":  ("total_goals_home",           True),
    "70":  ("total_goals_away",           True),
    "71":  ("exact_goals",                False),
    "72":  ("first_half_1x2",             False),
    "73":  ("double_chance",              False),
    "74":  ("odd_even",                   False),
    "75":  ("btts",                       False),
    "76":  ("clean_sheet_home",           False),
    "77":  ("clean_sheet_away",           False),
    "78":  ("btts_and_result",            False),
    "79":  ("result_and_over_under",      True),
    "81":  ("correct_score",              False),

    # ── Second-half markets ───────────────────────────────────────────────────
    "83":  ("second_half_result",         False),
    "85":  ("double_chance",              False),
    "86":  ("draw_no_bet",                False),
    "87":  ("european_handicap",          True),
    "88":  ("asian_handicap",             True),
    "90":  ("second_half_over_under",     True),
    "91":  ("total_goals_home",           True),
    "92":  ("total_goals_away",           True),
    "93":  ("exact_goals",                False),
    "94":  ("odd_even",                   False),
    "95":  ("btts",                       False),
    "98":  ("correct_score",              False),
    "100": ("next_goal",                  False),

    # ── Bookings / cards ──────────────────────────────────────────────────────
    "136": ("total_bookings",             False),
    "137": ("first_booking",              False),
    "138": ("total_bookings",             True),
    "139": ("total_bookings",             True),
    "142": ("total_bookings",             False),
    "146": ("total_bookings",             False),

    # ── Corners ───────────────────────────────────────────────────────────────
    "162": ("total_corners",              False),
    "163": ("total_corners",              False),
    "164": ("last_corner",                False),
    "165": ("asian_corners",              True),
    "166": ("total_corners",              True),
    "167": ("total_corners_home",         True),
    "168": ("total_corners_away",         True),
    "169": ("total_corners",              False),
    "170": ("total_corners",              False),
    "171": ("total_corners",              False),
    "172": ("total_corners",              False),
    "173": ("total_corners",              False),
    "177": ("total_corners",              True),
    "182": ("total_corners",              False),

    # ── Result combos ─────────────────────────────────────────────────────────
    "184": ("btts_and_result",            False),
    "543": ("btts_and_result",            False),
    "544": ("result_and_over_under",      True),
    "546": ("btts_and_result",            False),
    "547": ("result_and_over_under",      True),
    "548": ("number_of_goals",            False),
    "549": ("total_goals_home",           False),
    "550": ("total_goals_away",           False),
    "552": ("number_of_goals",            False),
    "682": ("score_both_halves",          False),
    "694": ("btts_and_result",            False),
    "695": ("btts_and_result",            False),
    "696": ("result_and_over_under",      True),
    "698": ("result_and_over_under",      True),
    "699": ("btts_and_result",            False),

    # ── HT/FT (with line) ─────────────────────────────────────────────────────
    "818": ("ht_ft",                      True),

    # ── Player markets ────────────────────────────────────────────────────────
    "638": ("anytime_goalscorer",         False),
    "639": ("first_goalscorer",           False),
    "640": ("last_goalscorer",            False),
    "643": ("player_booked",              False),
    "647": ("clean_sheet_home",           False),
    "648": ("clean_sheet_away",           False),
    "654": ("win_to_nil_home",            False),
    "655": ("win_to_nil_away",            False),
    "662": ("player_hattrick",            False),
    "701": ("anytime_goalscorer",         False),

    # ── Basketball ────────────────────────────────────────────────────────────
    "223": ("basketball_moneyline",       False),
    "224": ("point_spread",               True),
    "225": ("total_points",               True),
    "226": ("total_points",               True),
    "227": ("total_points",               True),

    # ── Tennis ────────────────────────────────────────────────────────────────
    "339": ("match_winner",               False),
    "340": ("match_winner",               False),
    "342": ("asian_handicap",             True),
    "362": ("total_games",                True),
    "363": ("set_betting",                False),
    "364": ("game_handicap",              True),
    "365": ("total_games",                True),
    "366": ("total_games",                True),
    "367": ("total_games",                True),
    "368": ("total_games",                True),
    "369": ("total_games",                True),
}


# =============================================================================
# PUBLIC NORMALISER
# =============================================================================

def normalize_od_market(
    sub_type_id: str | int,
    specifiers:  str = "",
) -> str:
    """
    Return a canonical market slug for an OdiBets market.

    Parameters
    ----------
    sub_type_id : int or str
        Numeric sub_type_id from the OdiBets API.
    specifiers : str, optional
        Line value string (e.g. "2.5" or "0:1").
        Only used when the market's uses_line flag is True.

    Returns
    -------
    str
        Canonical slug, e.g. "over_under_goals_2.5", "btts", "od_999".
        Never raises — falls back to "od_{id}" for unmapped IDs.
    """
    sid   = str(sub_type_id).strip()
    entry = _OD_MKT.get(sid)
    if not entry:
        return f"od_{sid}"
    base, uses_line = entry
    if uses_line and specifiers:
        return slug_with_line(base, specifiers)
    return base