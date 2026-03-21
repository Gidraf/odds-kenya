"""
app/workers/market_mapper.py
=============================
Single source of truth for market/outcome normalisation across ALL harvesters.
All public functions return slugs that exist in market_seeds.MARKETS_BY_SPORT.

Bookmakers covered
------------------
  Sportpesa   normalize_sp_market(sp_mkt_id, spec_value)
  Odibets     normalize_od_market(sub_type_id, specifiers)
  Betika      normalize_bt_market(name, sub_type_id)
  B2B family  normalize_b2b_market(mkt_name)   ← 1xBet/22Bet/Helabet/Paripesa…

Shared
------
  normalize_outcome(market_slug, raw_key, display="") → canonical outcome key
  normalize_line(raw_value)                           → clean line string "2.5"
  get_normalizer(source_name)                         → callable or None
"""

from __future__ import annotations

import re
from typing import Any, Callable


# =============================================================================
# SHARED HELPERS
# =============================================================================

def normalize_line(raw_value: Any) -> str:
    """
    Coerce a handicap / over-under line to a clean string.
      2.5   → "2.5"   |  2.0  → "2"  |  -0.5 → "-0.5"  |  "0:1" → "0:1"
    """
    if raw_value is None:
        return ""
    s = str(raw_value).strip()
    if not s:
        return ""
    if re.match(r"^-?\d+:-?\d+$", s):
        return s                          # Betradar hcp notation e.g. "0:1"
    if "=" in s:
        s = s.split("=")[-1].strip()     # "total=2.5" → "2.5"
    try:
        f = float(s)
        return str(int(f)) if f == int(f) else str(f)
    except ValueError:
        return s


def _slug_with_line(base: str, raw_line: Any) -> str:
    line = normalize_line(raw_line)
    if line and line not in ("0", ""):
        return f"{base}_{line}"
    return base


# =============================================================================
# 1.  SPORTPESA
# =============================================================================

_SP_MKT: dict[int, tuple[str, bool]] = {
    1:   ("1x2",                    False),
    10:  ("1x2",                    False),
    46:  ("double_chance",          False),
    47:  ("draw_no_bet",            False),
    43:  ("btts",                   False),
    29:  ("btts",                   False),
    386: ("btts_and_result",        False),
    51:  ("asian_handicap",         True),
    55:  ("european_handicap",      True),
    52:  ("over_under_goals",       True),
    18:  ("over_under_goals",       True),
    353: ("total_goals_home",       True),
    352: ("total_goals_away",       True),
    258: ("exact_goals",            False),
    202: ("number_of_goals",        False),
    332: ("correct_score",          False),
    208: ("result_and_over_under",  True),
    45:  ("odd_even",               False),
    60:  ("first_half_1x2",         False),
    15:  ("first_half_over_under",  True),
    68:  ("first_half_over_under",  True),
    162: ("total_corners",          False),
    166: ("total_corners",          True),
    136: ("total_bookings",         False),
    139: ("total_bookings",         True),
    382: ("basketball_moneyline",   False),
    99:  ("total_points",           True),
    100: ("point_spread",           True),
}


def normalize_sp_market(sp_mkt_id: int, spec_value: Any = None) -> str:
    entry = _SP_MKT.get(int(sp_mkt_id))
    if not entry:
        return f"sp_{sp_mkt_id}"
    base, uses_line = entry
    if uses_line:
        return _slug_with_line(base, spec_value)
    return base


# =============================================================================
# 2.  ODIBETS
# =============================================================================

_OD_MKT: dict[str, tuple[str, bool]] = {
    "1":   ("1x2",                     False),
    "7":   ("match_winner",            False),
    "8":   ("next_goal",               False),
    "9":   ("last_team_to_score",      False),
    "10":  ("double_chance",           False),
    "11":  ("draw_no_bet",             False),
    "12":  ("draw_no_bet",             False),
    "13":  ("draw_no_bet",             False),
    "14":  ("european_handicap",       True),
    "15":  ("winning_margin",          False),
    "16":  ("asian_handicap",          True),
    "18":  ("over_under_goals",        True),
    "19":  ("total_goals_home",        True),
    "20":  ("total_goals_away",        True),
    "21":  ("exact_goals",             False),
    "23":  ("exact_goals",             False),
    "24":  ("exact_goals",             False),
    "26":  ("odd_even",                False),
    "27":  ("odd_even",                False),
    "28":  ("odd_even",                False),
    "29":  ("btts",                    False),
    "30":  ("first_team_to_score",     False),
    "31":  ("clean_sheet_home",        False),
    "32":  ("clean_sheet_away",        False),
    "33":  ("win_to_nil_home",         False),
    "34":  ("win_to_nil_away",         False),
    "35":  ("btts_and_result",         False),
    "36":  ("btts_and_result",         True),
    "37":  ("result_and_over_under",   True),
    "38":  ("first_goalscorer",        False),
    "39":  ("last_goalscorer",         False),
    "40":  ("anytime_goalscorer",      False),
    "41":  ("correct_score",           False),
    "45":  ("correct_score",           False),
    "46":  ("ht_ft",                   False),
    "47":  ("ht_ft",                   False),
    "48":  ("score_both_halves",       False),
    "49":  ("score_both_halves",       False),
    "50":  ("score_both_halves",       False),
    "51":  ("score_both_halves",       False),
    "52":  ("highest_scoring_half",    False),
    "55":  ("goal_both_halves",        False),
    "56":  ("score_both_halves",       False),
    "57":  ("score_both_halves",       False),
    "58":  ("goal_both_halves",        True),
    "59":  ("goal_both_halves",        True),
    "60":  ("first_half_1x2",          False),
    "62":  ("next_goal",               False),
    "63":  ("double_chance",           False),
    "64":  ("draw_no_bet",             False),
    "65":  ("european_handicap",       True),
    "66":  ("asian_handicap",          True),
    "68":  ("first_half_over_under",   True),
    "69":  ("total_goals_home",        True),
    "70":  ("total_goals_away",        True),
    "71":  ("exact_goals",             False),
    "74":  ("odd_even",                False),
    "75":  ("btts",                    False),
    "76":  ("clean_sheet_home",        False),
    "77":  ("clean_sheet_away",        False),
    "78":  ("btts_and_result",         False),
    "79":  ("result_and_over_under",   True),
    "81":  ("correct_score",           False),
    "83":  ("second_half_result",      False),
    "85":  ("double_chance",           False),
    "86":  ("draw_no_bet",             False),
    "87":  ("european_handicap",       True),
    "88":  ("asian_handicap",          True),
    "90":  ("second_half_over_under",  True),
    "91":  ("total_goals_home",        True),
    "92":  ("total_goals_away",        True),
    "93":  ("exact_goals",             False),
    "94":  ("odd_even",                False),
    "95":  ("btts",                    False),
    "98":  ("correct_score",           False),
    "100": ("next_goal",               False),
    "136": ("total_bookings",          False),
    "137": ("first_booking",           False),
    "138": ("total_bookings",          True),
    "139": ("total_bookings",          True),
    "142": ("total_bookings",          False),
    "146": ("total_bookings",          False),
    "162": ("total_corners",           False),
    "163": ("total_corners",           False),
    "164": ("last_corner",             False),
    "165": ("asian_corners",           True),
    "166": ("total_corners",           True),
    "167": ("total_corners_home",      True),
    "168": ("total_corners_away",      True),
    "169": ("total_corners",           False),
    "172": ("total_corners",           False),
    "173": ("total_corners",           False),
    "177": ("total_corners",           True),
    "182": ("total_corners",           False),
    "184": ("btts_and_result",         False),
    "543": ("btts_and_result",         False),
    "544": ("result_and_over_under",   True),
    "546": ("btts_and_result",         False),
    "547": ("result_and_over_under",   True),
    "548": ("number_of_goals",         False),
    "549": ("total_goals_home",        False),
    "550": ("total_goals_away",        False),
    "818": ("ht_ft",                   True),
}


def normalize_od_market(sub_type_id: str | int, specifiers: str = "") -> str:
    sid   = str(sub_type_id).strip()
    entry = _OD_MKT.get(sid)
    if not entry:
        return f"od_{sid}"
    base, uses_line = entry
    if uses_line and specifiers:
        return _slug_with_line(base, specifiers)
    return base


# =============================================================================
# 3.  BETIKA
# =============================================================================

_BT_SUBTYPE: dict[int, str] = {
    1:   "1x2",             7:   "match_winner",        8:   "next_goal",
    10:  "double_chance",   11:  "draw_no_bet",         14:  "european_handicap",
    15:  "winning_margin",  18:  "over_under_goals",    19:  "total_goals_home",
    20:  "total_goals_away",21:  "exact_goals",         23:  "exact_goals",
    24:  "exact_goals",     29:  "btts",                31:  "clean_sheet_home",
    32:  "clean_sheet_away",33:  "win_to_nil_home",     34:  "win_to_nil_away",
    35:  "btts_and_result", 36:  "btts_and_result",     37:  "result_and_over_under",
    41:  "correct_score",   45:  "correct_score",       47:  "ht_ft",
    48:  "score_both_halves",49: "score_both_halves",   50:  "score_both_halves",
    51:  "score_both_halves",55: "goal_both_halves",    56:  "score_both_halves",
    57:  "score_both_halves",58: "goal_both_halves",    59:  "goal_both_halves",
    60:  "first_half_1x2",  62:  "next_goal",           63:  "double_chance",
    65:  "european_handicap",68: "first_half_over_under",75: "btts",
    78:  "btts_and_result", 79:  "result_and_over_under",81: "correct_score",
    83:  "second_half_result",85:"double_chance",        90: "second_half_over_under",
    95:  "btts",            105: "1x2",                 136: "total_bookings",
    137: "total_corners",   139: "total_bookings",      142: "total_bookings",
    162: "total_corners_away",163:"total_corners",      165: "total_corners_home",
    166: "total_bookings",  168: "1x2",                 184: "btts_and_result",
    223: "basketball_moneyline",340:"match_winner",     342: "asian_handicap",
    546: "btts_and_result", 547: "result_and_over_under",548:"number_of_goals",
    549: "total_goals_home",550: "total_goals_away",    552: "number_of_goals",
    638: "anytime_goalscorer",639:"first_goalscorer",   640: "last_goalscorer",
    643: "player_booked",   647: "clean_sheet_home",    648: "clean_sheet_away",
    654: "win_to_nil_home", 655: "win_to_nil_away",     662: "player_hattrick",
    682: "score_both_halves",701:"anytime_goalscorer",  775: "player_score_2plus",
    818: "ht_ft",
}

_BT_NAME: dict[str, str] = {
    "1X2":                                 "1x2",
    "MATCH WINNER":                        "1x2",
    "DOUBLE CHANCE":                       "double_chance",
    "DRAW NO BET":                         "draw_no_bet",
    "WHO WILL WIN? (IF DRAW, MONEY BACK)": "draw_no_bet",
    "TOTAL":                               "over_under_goals",
    "TOTAL GOALS":                         "over_under_goals",
    "OVER/UNDER":                          "over_under_goals",
    "MULTIGOALS":                          "number_of_goals",
    "EXACT GOALS":                         "exact_goals",
    "NUMBER OF GOALS":                     "number_of_goals",
    "BOTH TEAMS TO SCORE":                 "btts",
    "BOTH TEAMS TO SCORE (GG/NG)":         "btts",
    "GG/NG":                               "btts",
    "BOTH TEAMS TO SCORE & RESULT":        "btts_and_result",
    "1X2 & BOTH TEAMS TO SCORE":          "btts_and_result",
    "CORRECT SCORE":                       "correct_score",
    "HALFTIME/FULLTIME":                   "ht_ft",
    "HALF TIME / FULL TIME":               "ht_ft",
    "HANDICAP":                            "european_handicap",
    "EUROPEAN HANDICAP":                   "european_handicap",
    "ASIAN HANDICAP":                      "asian_handicap",
    "1X2 & TOTAL":                         "result_and_over_under",
    "MATCH RESULT & OVER/UNDER":           "result_and_over_under",
    "1ST HALF - 1X2":                      "first_half_1x2",
    "FIRST HALF 1X2":                      "first_half_1x2",
    "HALF TIME RESULT":                    "first_half_1x2",
    "1ST HALF - TOTAL":                    "first_half_over_under",
    "FIRST HALF OVER/UNDER":               "first_half_over_under",
    "2ND HALF - 1X2":                      "second_half_result",
    "2ND HALF - TOTAL":                    "second_half_over_under",
    "FIRST GOALSCORER":                    "first_goalscorer",
    "LAST GOALSCORER":                     "last_goalscorer",
    "ANYTIME GOALSCORER":                  "anytime_goalscorer",
    "FIRST TEAM TO SCORE":                 "first_team_to_score",
    "LAST TEAM TO SCORE":                  "last_team_to_score",
    "NEXT GOAL":                           "next_goal",
    "CLEAN SHEET":                         "clean_sheet_home",
    "WIN TO NIL":                          "win_to_nil_home",
    "WINNING MARGIN":                      "winning_margin",
    "TOTAL CORNERS":                       "total_corners",
    "ASIAN CORNERS":                       "asian_corners",
    "FIRST CORNER":                        "first_corner",
    "LAST CORNER":                         "last_corner",
    "TOTAL BOOKINGS":                      "total_bookings",
    "TOTAL CARDS":                         "total_bookings",
    "FIRST BOOKING":                       "first_booking",
    "DOUBLE CHANCE & BOTH TEAMS TO SCORE": "btts_and_result",
    "DOUBLE CHANCE & TOTAL":               "result_and_over_under",
    "GOAL IN BOTH HALVES":                 "goal_both_halves",
    "BOTH HALVES OVER 1.5":               "goal_both_halves",
    "1ST/2ND HALF BOTH TEAMS TO SCORE":   "btts",
    "WHICH TEAM WINS THE REST OF THE MATCH":"match_winner",
    "HALFTIME/FULLTIME & TOTAL":           "ht_ft",
    "MONEYLINE":                           "basketball_moneyline",
    "POINT SPREAD":                        "point_spread",
    "TOTAL POINTS":                        "total_points",
    "WILL THERE BE OVERTIME":              "overtime",
    "SET BETTING":                         "set_betting",
    "TOTAL SETS":                          "total_sets",
    "TOTAL GAMES":                         "total_games",
    "FIRST SET WINNER":                    "first_set_winner",
    "TIEBREAK IN MATCH":                   "tiebreak_in_match",
}

_BT_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bTOTAL$",                    re.I), "over_under_goals"),
    (re.compile(r"\bMULTIGOALS$",               re.I), "number_of_goals"),
    (re.compile(r"\bEXACT GOALS$",              re.I), "exact_goals"),
    (re.compile(r"\bCLEAN SHEET$",              re.I), "clean_sheet_home"),
    (re.compile(r"\bWIN TO NIL$",               re.I), "win_to_nil_home"),
    (re.compile(r"WHO WILL SCORE \d+\w* GOAL",  re.I), "next_goal"),
    (re.compile(r"CORRECT SCORE\s*\[",          re.I), "correct_score"),
    (re.compile(r"WHICH TEAM WINS THE REST",     re.I), "match_winner"),
    (re.compile(r"\bTO WIN BOTH HALVES$",        re.I), "score_both_halves"),
    (re.compile(r"\bTO WIN EITHER HALF$",        re.I), "score_both_halves"),
    (re.compile(r"\bTO SCORE IN BOTH HALVES$",   re.I), "score_both_halves"),
    (re.compile(r"^1ST HALF\s*[-–]",            re.I), "first_half_1x2"),
    (re.compile(r"^2ND HALF\s*[-–]",            re.I), "second_half_result"),
    (re.compile(r"^HALFTIME/FULLTIME",           re.I), "ht_ft"),
    (re.compile(r"^\d+ MINUTES\s*[-–]",         re.I), "1x2"),
]


def normalize_bt_market(name: str, sub_type_id: int | str | None = None) -> str:
    if sub_type_id is not None:
        try:
            slug = _BT_SUBTYPE.get(int(sub_type_id))
            if slug:
                return slug
        except (ValueError, TypeError):
            pass
    upper = name.strip().upper()
    if upper in _BT_NAME:
        return _BT_NAME[upper]
    for pat, mapped in _BT_PATTERNS:
        if pat.search(name):
            return mapped
    return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_") or "unknown"


# =============================================================================
# 4.  B2B FAMILY  (1xBet, 22Bet, Helabet, Paripesa, Melbet, Betwinner, Megapari)
# =============================================================================
# Markets arrive from bookmaker_fetcher with lines already embedded in the name:
#   "Total_2.5"      → "over_under_goals_2.5"
#   "Handicap_-1.5"  → "asian_handicap_-1.5"
#   "1H Total_1.5"   → "first_half_over_under_1.5"
#   "1X2"            → "1x2"
#   "GG/NG"          → "btts"

_B2B_BASE_NAME: dict[str, str] = {
    "1x2":               "1x2",
    "handicap":          "asian_handicap",
    "eur handicap":      "european_handicap",
    "correct score":     "correct_score",
    "ht correct score":  "correct_score",
    "ht/ft":             "ht_ft",
    "first scorer":      "first_goalscorer",
    "last scorer":       "last_goalscorer",
    "anytime scorer":    "anytime_goalscorer",
    "double chance":     "double_chance",
    "draw no bet":       "draw_no_bet",
    "win to nil":        "win_to_nil_home",
    "clean sheet":       "clean_sheet_home",
    "gg/ng":             "btts",
    "btts":              "btts",
    "next goal":         "next_goal",
    "1h total":          "first_half_over_under",
    "2h total":          "second_half_over_under",
    "total":             "over_under_goals",
    "result+total":      "result_and_over_under",
    "exact goals":       "exact_goals",
    "corners":           "total_corners",
    "asian corners":     "asian_corners",
    "bookings":          "total_bookings",
    "cards total":       "total_bookings",
    "1h result":         "first_half_1x2",
    "2h result":         "second_half_result",
    "1h double chance":  "double_chance",
    "win both halves":   "score_both_halves",
    "score both halves": "score_both_halves",
    "team total":        "total_goals_home",
    "1h handicap":       "european_handicap",
    "2h handicap":       "european_handicap",
    "corners handicap":  "asian_corners",
    "odd/even":          "odd_even",
}

# Regex to split e.g. "Total_2.5" → base="Total", line="2.5"
_B2B_LINE_RE = re.compile(r"^(.+?)_(-?[\d.]+)$")


def normalize_b2b_market(mkt_name: str) -> str:
    """
    Normalize a B2B market name (from bookmaker_fetcher) to canonical slug.

    Examples:
        normalize_b2b_market("1X2")          → "1x2"
        normalize_b2b_market("Total_2.5")    → "over_under_goals_2.5"
        normalize_b2b_market("Handicap_-1.5")→ "asian_handicap_-1.5"
        normalize_b2b_market("GG/NG")        → "btts"
        normalize_b2b_market("1H Total_1.5") → "first_half_over_under_1.5"
    """
    # Exact match first
    exact = _B2B_BASE_NAME.get(mkt_name.strip().lower())
    if exact:
        return exact

    # Split base + numeric line
    m = _B2B_LINE_RE.match(mkt_name.strip())
    if m:
        base_raw = m.group(1).strip()
        line     = m.group(2)
        slug     = _B2B_BASE_NAME.get(base_raw.lower())
        if slug:
            return f"{slug}_{line}"

    # Sanitised fallback
    return re.sub(r"[^a-z0-9]+", "_", mkt_name.strip().lower()).strip("_") or "unknown"


# =============================================================================
# 5.  STUB BOOKMAKERS  (populate _MKT / _NAME dicts from intercepted traffic)
# =============================================================================

def _generic_normalizer(
    id_map: dict[str, tuple[str, bool]],
    name_map: dict[str, str],
    prefix: str,
) -> Callable[[str, Any, str], str]:
    def _fn(name: str, sub_type_id: Any = None, specifiers: str = "") -> str:
        if sub_type_id is not None:
            entry = id_map.get(str(sub_type_id).strip())
            if entry:
                base, uses_line = entry
                if uses_line and specifiers:
                    return _slug_with_line(base, specifiers)
                return base
        upper = name.strip().upper()
        if upper in name_map:
            return name_map[upper]
        return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_") or f"{prefix}_unknown"
    return _fn


normalize_mozzartbet_market = _generic_normalizer({}, {}, "mz")
normalize_betin_market      = _generic_normalizer({}, {}, "betin")


# =============================================================================
# 6.  UNIFIED OUTCOME NORMALISER
# =============================================================================

_OUT_EXACT: dict[str, str] = {
    # 3-way
    "1": "1",  "x": "X",  "2": "2",
    "home": "1",  "draw": "X",  "away": "2",
    # Over / Under
    "over": "over",   "under": "under",
    "ov":   "over",   "un":    "under",
    # BTTS / Odd-Even
    "yes":  "yes",   "no":   "no",
    "gg":   "yes",   "ng":   "no",
    "odd":  "odd",   "even": "even",
    "od":   "odd",   "ev":   "even",
    # Double Chance — all notations (B2B uses "2X", others use "X2")
    "1x":         "1X",  "x2":         "X2",  "12":         "12",
    "2x":         "X2",  # ← B2B gap fixed
    "1/x":        "1X",  "x/2":        "X2",  "1/2":        "12",
    "1 or x":     "1X",  "1 or 2":     "12",  "x or 2":     "X2",
    "home or draw":"1X", "home or away":"12", "draw or away":"X2",
    # No-goal
    "none":    "none",
    "no goal": "none",
}

_OUT_OVER_RE  = re.compile(r"^over\s+([\d.]+)$",  re.I)
_OUT_UNDER_RE = re.compile(r"^under\s+([\d.]+)$", re.I)
_OUT_SCORE_RE = re.compile(r"^\d+:\d+$")
_OUT_HCP_RE   = re.compile(r"^([12X])\s*[\(\[].*[\)\]]$", re.I)

_SP_COMBO: dict[str, str] = {
    "ov_1": "1_over",   "ov_x": "X_over",   "ov_2": "2_over",
    "un_1": "1_under",  "un_x": "X_under",  "un_2": "2_under",
    "1gg":  "1_yes",    "xgg":  "X_yes",    "2gg":  "2_yes",
    "1ng":  "1_no",     "xng":  "X_no",     "2ng":  "2_no",
}


def normalize_outcome(
    market_slug: str,
    raw_key: str,
    display: str = "",
) -> str:
    """
    Return a canonical outcome key for any bookmaker's raw outcome.

    For Over/Under markets the line is already in market_slug
    so "over 2.5" → "over" (line stripped).
    For handicaps "1 (0:1)" → "1".
    For correct score "2:1" passes through.
    For B2B "2X" → "X2".
    Unknown T-prefixed B2B codes (T731, T3786) are sanitised.
    """
    for raw in (raw_key, display):
        if not raw:
            continue
        kl = raw.strip().lower()

        v = _OUT_EXACT.get(kl)
        if v:
            return v

        if _OUT_OVER_RE.match(kl):
            return "over"
        if _OUT_UNDER_RE.match(kl):
            return "under"

        if _OUT_SCORE_RE.match(raw.strip()):
            return raw.strip()

        m = _OUT_HCP_RE.match(raw.strip())
        if m:
            return m.group(1).upper()

    kl = raw_key.strip().lower()
    if kl in _SP_COMBO:
        return _SP_COMBO[kl]

    if re.match(r"^\d+[-–+]\d*$", raw_key.strip()):
        return raw_key.strip()

    # T-prefixed B2B internal codes — sanitise but keep
    raw = raw_key.strip() or display.strip()
    return re.sub(r"[^a-z0-9_:+./\-]+", "_", raw.lower()).strip("_") or "unknown"


# =============================================================================
# 7.  BOOKMAKER REGISTRY
# =============================================================================

BOOKMAKER_NORMALIZERS: dict[str, Callable] = {
    # Local Kenyan bookmakers
    "sportpesa":        lambda name, sid, spec="": normalize_sp_market(int(sid or 0), spec or None),
    "betika_upcoming":  lambda name, sid, spec="": normalize_bt_market(name, sid),
    "betika_live":      lambda name, sid, spec="": normalize_bt_market(name, sid),
    "betika":           lambda name, sid, spec="": normalize_bt_market(name, sid),
    "odibets":          lambda name, sid, spec="": normalize_od_market(sid, spec),
    # B2B family (all share the same market naming convention)
    "b2b":              lambda name, sid, spec="": normalize_b2b_market(name),
    "1xbet":            lambda name, sid, spec="": normalize_b2b_market(name),
    "22bet":            lambda name, sid, spec="": normalize_b2b_market(name),
    "helabet":          lambda name, sid, spec="": normalize_b2b_market(name),
    "paripesa":         lambda name, sid, spec="": normalize_b2b_market(name),
    "melbet":           lambda name, sid, spec="": normalize_b2b_market(name),
    "betwinner":        lambda name, sid, spec="": normalize_b2b_market(name),
    "megapari":         lambda name, sid, spec="": normalize_b2b_market(name),
    # Stubs
    "mozzartbet":       lambda name, sid, spec="": normalize_mozzartbet_market(name, sid, spec),
    "betin":            lambda name, sid, spec="": normalize_betin_market(name, sid, spec),
}


def get_normalizer(source_name: str) -> Callable | None:
    """
    Return the market normalizer for the given source/bookmaker name.

    Usage:
        fn = get_normalizer("odibets")
        slug = fn(market_name, sub_type_id, specifiers)
    """
    return BOOKMAKER_NORMALIZERS.get(source_name.lower().replace(" ", "").replace("-", ""))