"""
app/workers/canonical_mapper.py
================================
Shared normalisation utilities used by ALL bookmaker harvesters.

Exports
-------
  normalize_line(raw_value)                              → clean line string e.g. "2.5"
  slug_with_line(base, raw_line)                         → "over_under_goals_2.5"
  normalize_outcome(market_slug, raw_key, display="")   → canonical outcome key
  normalize_od_market(sub_type_id, specifiers)          → Odibets
  normalize_bt_market(name, sub_type_id)                → Betika
  normalize_b2b_market(mkt_name)                        → 1xBet / 22Bet / Helabet family
  normalize_mozzartbet_market(name, sid, spec)           → MozzartBet
  normalize_betin_market(name, sid, spec)                → Betin
  get_normalizer(source_name)                            → callable or None

Changelog
---------
  v3 — multi-sport normalize_outcome
    Round betting guard (step 0): "1"/"2"/"3" in round_betting / total_rounds
    markets now map to "round_1"/"round_2" etc. instead of colliding with
    the 1x2 Home/Away outcome keys.  _ROUND_RE handles "RD1", "R1", "Round 1".
    Added p1/p2 and "player 1"/"player 2" → "1"/"2" for tennis shortNames.
    get_normalizer sportpesa lambda now explicitly passes sport_id=1.

  v2 — slug_with_line fix
    specValue=0 is now kept as a suffix (Asian HC level ball).
    Removing "0" from the exclusion set is safe because all O/U markets
    never have specValue=0 — their lines are 0.5, 0.75, 1.0 … 5.5.

Import in your harvester
------------------------
  from app.workers.canonical_mapper import normalize_outcome, normalize_line
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
      2.5  → "2.5"  |  2.0  → "2"  |  -0.5 → "-0.5"  |  "0:1" → "0:1"
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


def slug_with_line(base: str, raw_line: Any) -> str:
    """Append a line suffix to a base market slug, e.g. 'over_under_goals_2.5'."""
    line = normalize_line(raw_line)
    # "0" is a valid line (Asian HC level ball — do NOT exclude it)
    if line and line != "":
        return f"{base}_{line}"
    return base


# =============================================================================
# OUTCOME NORMALISATION
# =============================================================================

_OUT_EXACT: dict[str, str] = {
    # 3-way
    "1": "1",  "x": "X",  "2": "2",
    "home": "1",  "draw": "X",  "away": "2",
    # Over / Under
    "over":  "over",   "under":  "under",
    "ov":    "over",   "un":     "under",
    # BTTS / Odd-Even
    "yes":  "yes",   "no":   "no",
    "gg":   "yes",   "ng":   "no",
    "odd":  "odd",   "even": "even",
    "od":   "odd",   "ev":   "even",
    # Double Chance
    "1x":         "1X",  "x2":         "X2",  "12":         "12",
    "2x":         "X2",
    "1/x":        "1X",  "x/2":        "X2",  "1/2":        "12",
    "1 or x":     "1X",  "1 or 2":     "12",  "x or 2":     "X2",
    "home or draw":"1X", "home or away":"12", "draw or away":"X2",
    # No-goal / none
    "none":    "none",
    "no goal": "none",
    # Highest scoring half (market 207)
    # SP shortName "Eql" → "equal"
    "eql":  "equal",   "equal": "equal",
    # Tennis player shortNames
    "p1":   "1",       "p2":    "2",
    "player 1": "1",   "player 2": "2",
    # Basketball highest-scoring-quarter shortNames (market 224)
    "1stq": "1st_quarter",  "2ndq": "2nd_quarter",
    "3rdq": "3rd_quarter",  "4thq": "4th_quarter",
    # "1st" and "2nd" fall through to sanitised return → "1st"/"2nd" ✓
}

# SP-specific combo outcomes (Result + O/U, BTTS + Result)
_SP_COMBO: dict[str, str] = {
    "ov_1": "1_over",   "ov_x": "X_over",   "ov_2": "2_over",
    "un_1": "1_under",  "un_x": "X_under",  "un_2": "2_under",
    "1gg":  "1_yes",    "xgg":  "X_yes",    "2gg":  "2_yes",
    "1ng":  "1_no",     "xng":  "X_no",     "2ng":  "2_no",
}

# HT/FT outcomes — SP shortNames use "11","1X","X2" style (no slash).
# These ONLY apply to the ht_ft market to avoid collision with double_chance.
_HTFT_MAP: dict[str, str] = {
    "11": "1/1",  "1x": "1/X",  "12": "1/2",
    "x1": "X/1",  "xx": "X/X",  "x2": "X/2",
    "21": "2/1",  "2x": "2/X",  "22": "2/2",
    # slash variants (already correct if SP ever returns them)
    "1/1": "1/1", "1/x": "1/X", "1/2": "1/2",
    "x/1": "X/1", "x/x": "X/X", "x/2": "X/2",
    "2/1": "2/1", "2/x": "2/X", "2/2": "2/2",
}

# ── First Set / Match Winner combo (tennis market 433) ────────────────────────
# shortNames "11"→"1/1", "12"→"1/2", "21"→"2/1", "22"→"2/2"
# Must resolve before _OUT_EXACT because "12" would map to DC "12".
_FSMW_MAP: dict[str, str] = {
    "11": "1/1",  "12": "1/2",
    "21": "2/1",  "22": "2/2",
}

# ── Winning Margin shortNames (basketball market 222) ─────────────────────────
# SP sends "H15" (Home by 1-5), "H610" (Home by 6-10), "H_10" (Home 11+), etc.
_WINNING_MARGIN: dict[str, str] = {
    "h15":  "home_1_5",     "h610": "home_6_10",
    "h_10": "home_11plus",  "h11":  "home_11plus",
    "a15":  "away_1_5",     "a610": "away_6_10",
    "a_10": "away_11plus",  "a11":  "away_11plus",
}

# Display-name fallback: "Home by 1-5 pts", "Away by 11+ pts"
_WINNING_MARGIN_DISPLAY = re.compile(
    r"(?P<side>home|away)\s+by\s+(?P<range>[\d]+[-–+][\d]*)\s*pts?",
    re.I,
)

# ── Combat sport round regex ─────────────────────────────────────────────────
# Maps "RD1", "R1", "Round 1", bare digit → "round_N" for round_betting markets
# so they don't collide with 1x2 outcome "1" (Home).
_ROUND_RE = re.compile(r"^(?:rd?|round\s*)(\d+)$", re.I)

_OUT_OVER_RE  = re.compile(r"^over\s+([\d.]+)$",  re.I)
_OUT_UNDER_RE = re.compile(r"^under\s+([\d.]+)$", re.I)
_OUT_SCORE_RE = re.compile(r"^\d+:\d+$")
_OUT_HCP_RE   = re.compile(r"^([12X])\s*[\(\[].*[\)\]]$", re.I)


def normalize_outcome(
    market_slug: str,
    raw_key:     str,
    display:     str = "",
) -> str:
    """
    Return a canonical outcome key for any bookmaker's raw outcome.

    Special handling per market_slug:
    • ht_ft       → "11"/"1X"/"X2" etc. mapped via _HTFT_MAP (not double_chance)
    • highest_scoring_half → "Eql" → "equal"
    • over/under  → line already in slug, so "over 2.5" → "over"
    • handicap    → "1 (0:1)" → "1"
    • correct_score → "2:1" passes through
    """
    kl = raw_key.strip().lower()

    # ── 0. Combat sport round betting ─────────────────────────────────────────
    # MUST be checked before _OUT_EXACT — otherwise "1"/"2"/"3" map to
    # Home/Away instead of round numbers.
    # Only intercepts digit-like values; "OV"/"UN" fall through to _OUT_EXACT.
    if market_slug.startswith("round_betting"):
        m_rd = _ROUND_RE.match(raw_key.strip())
        if m_rd:
            return f"round_{m_rd.group(1)}"
        if raw_key.strip().isdigit():
            return f"round_{raw_key.strip()}"
        # "KO", "TKO", "SUB", "DEC" or other method strings fall to sanitise below

    # ── HT/FT market — must resolve before general _OUT_EXACT ────────────────
    # "12" in double_chance = "12" (both teams win), but in ht_ft = "1/2"
    if "ht_ft" in market_slug:
        v = _HTFT_MAP.get(kl)
        if v:
            return v

    # ── First Set / Match Winner combo (tennis market 433) ────────────────────
    # shortNames "11"→"1/1", "12"→"1/2", "21"→"2/1", "22"→"2/2"
    # Must be before _OUT_EXACT because "12" would map to DC "12" otherwise.
    if market_slug == "first_set_match_winner":
        v = _FSMW_MAP.get(kl)
        if v:
            return v

    # ── Winning Margin (basketball, handball) ─────────────────────────────────
    # Must be before _OUT_EXACT because "H15" shortNames would hit the fallback.
    if market_slug == "winning_margin":
        v = _WINNING_MARGIN.get(kl)
        if v:
            return v
        # Display name fallback: "Home by 1-5 pts"
        for txt in (raw_key, display):
            m_wm = _WINNING_MARGIN_DISPLAY.match(txt.strip())
            if m_wm:
                side = "home" if m_wm.group("side").lower() == "home" else "away"
                rng  = m_wm.group("range").replace("–", "-").replace(" ", "")
                return f"{side}_{rng.replace('-','_').replace('+','plus')}"
        return re.sub(r"[^a-z0-9_]+", "_", kl).strip("_") or "unknown"

    # ── General exact match ───────────────────────────────────────────────────
    for raw in (raw_key, display):
        if not raw:
            continue
        kl2 = raw.strip().lower()

        v = _OUT_EXACT.get(kl2)
        if v:
            return v

        if _OUT_OVER_RE.match(kl2):
            return "over"
        if _OUT_UNDER_RE.match(kl2):
            return "under"

        if _OUT_SCORE_RE.match(raw.strip()):
            return raw.strip()

        m = _OUT_HCP_RE.match(raw.strip())
        if m:
            return m.group(1).upper()

    # ── SP combo outcomes ─────────────────────────────────────────────────────
    if kl in _SP_COMBO:
        return _SP_COMBO[kl]

    # ── Numeric ranges (goal groups) "0-1", "2-3" ────────────────────────────
    if re.match(r"^\d+[-–+]\d*$", raw_key.strip()):
        return raw_key.strip()

    # ── Fallback — sanitise ───────────────────────────────────────────────────
    raw = raw_key.strip() or display.strip()
    return re.sub(r"[^a-z0-9_:+./\-]+", "_", raw.lower()).strip("_") or "unknown"


# =============================================================================
# ODIBETS
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
        return slug_with_line(base, specifiers)
    return base


# =============================================================================
# BETIKA
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
    (re.compile(r"^1ST HALF\s*[–-]",            re.I), "first_half_1x2"),
    (re.compile(r"^2ND HALF\s*[–-]",            re.I), "second_half_result"),
    (re.compile(r"^HALFTIME/FULLTIME",           re.I), "ht_ft"),
    (re.compile(r"^\d+ MINUTES\s*[–-]",         re.I), "1x2"),
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
# B2B FAMILY  (1xBet, 22Bet, Helabet, Paripesa, Melbet, Betwinner, Megapari)
# =============================================================================

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

_B2B_LINE_RE = re.compile(r"^(.+?)_(-?[\d.]+)$")


def normalize_b2b_market(mkt_name: str) -> str:
    exact = _B2B_BASE_NAME.get(mkt_name.strip().lower())
    if exact:
        return exact
    m = _B2B_LINE_RE.match(mkt_name.strip())
    if m:
        base_raw = m.group(1).strip()
        line     = m.group(2)
        s        = _B2B_BASE_NAME.get(base_raw.lower())
        if s:
            return f"{s}_{line}"
    return re.sub(r"[^a-z0-9]+", "_", mkt_name.strip().lower()).strip("_") or "unknown"


# =============================================================================
# STUB BOOKMAKERS
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
                    return slug_with_line(base, specifiers)
                return base
        upper = name.strip().upper()
        if upper in name_map:
            return name_map[upper]
        return re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_") or f"{prefix}_unknown"
    return _fn


normalize_mozzartbet_market = _generic_normalizer({}, {}, "mz")
normalize_betin_market      = _generic_normalizer({}, {}, "betin")


# =============================================================================
# BOOKMAKER REGISTRY
# =============================================================================

def get_normalizer(source_name: str) -> Callable | None:
    """Return the normalizer for a bookmaker slug, or None if unknown."""
    # Import here to avoid circular dependency with sp_mapper
    from app.workers.sp_mapper import normalize_sp_market   # noqa: PLC0415

    registry: dict[str, Callable] = {
        # sport_id not in generic (name, sid, spec) interface — defaults to football.
        # The harvester calls normalize_sp_market directly with the correct sport_id.
        "sportpesa":        lambda name, sid, spec="": normalize_sp_market(int(sid or 0), spec or None, 1),
        "betika_upcoming":  lambda name, sid, spec="": normalize_bt_market(name, sid),
        "betika_live":      lambda name, sid, spec="": normalize_bt_market(name, sid),
        "betika":           lambda name, sid, spec="": normalize_bt_market(name, sid),
        "odibets":          lambda name, sid, spec="": normalize_od_market(sid, spec),
        "b2b":              lambda name, sid, spec="": normalize_b2b_market(name),
        "1xbet":            lambda name, sid, spec="": normalize_b2b_market(name),
        "22bet":            lambda name, sid, spec="": normalize_b2b_market(name),
        "helabet":          lambda name, sid, spec="": normalize_b2b_market(name),
        "paripesa":         lambda name, sid, spec="": normalize_b2b_market(name),
        "melbet":           lambda name, sid, spec="": normalize_b2b_market(name),
        "betwinner":        lambda name, sid, spec="": normalize_b2b_market(name),
        "megapari":         lambda name, sid, spec="": normalize_b2b_market(name),
        "mozzartbet":       lambda name, sid, spec="": normalize_mozzartbet_market(name, sid, spec),
        "betin":            lambda name, sid, spec="": normalize_betin_market(name, sid, spec),
    }
    return registry.get(
        source_name.lower().replace(" ", "").replace("-", "")
    )