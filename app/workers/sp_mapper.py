"""
app/workers/sp_mapper.py
=========================
Sportpesa market ID → canonical slug mapper — SPORT-AWARE edition.

The core problem
─────────────────
SP reuses the same market IDs across sports.  The SAME numeric ID means
completely different things depending on which sport you are parsing:

  ID 51  → asian_handicap      (football)
         → point_spread        (basketball)
         → game_handicap       (tennis)
         → puck_line           (ice hockey)
         → set_handicap        (volleyball)

  ID 52  → over_under_goals    (football)
         → total_points        (basketball)
         → total_goals         (ice hockey / handball)
         → total_runs          (cricket)

  ID 45  → odd_even            (football — goals)
         → odd_even_points     (basketball, volleyball, table tennis)
         → odd_even_games      (tennis)
         → odd_even_goals      (ice hockey, handball)

  ID 382 → match_winner        (basketball, tennis, all 2-way sports)
           (not used in football — football uses ID 1 / 10)

  ID 353 → total_goals_home    (football)
         → total_points_home   (basketball)
         → total_games_player1 (tennis)

  ID 352 → total_goals_away    (football)
         → total_points_away   (basketball)
         → total_games_player2 (tennis)

Public API
──────────
  normalize_sp_market(mkt_id, spec_val=None, sport_id=1) → str

  The sport_id defaults to 1 (football) for backwards compatibility.
  Pass the sport.id integer from the SP listing response.

  slug_with_line(base, raw_line) → str   (re-exported from canonical_mapper)

Sport IDs (SP)
───────────────
  1   Football / Soccer
  2   Basketball
  4   Ice Hockey
  5   Tennis
  6   Handball
  10  Boxing
  12  Rugby
  15  American Football
  16  Table Tennis
  21  Cricket
  23  Volleyball
  49  Darts
  117 MMA / UFC
  126 eFootball / eSoccer

Canonical output format
───────────────────────
Non-line market:  "match_winner", "1x2", "btts", "double_chance" …
Line market:      "over_under_goals_2.5", "point_spread_-5.5",
                  "total_points_173.5", "game_handicap_-1.5" …

All slugs use lowercase snake_case.  Line suffix is appended by
slug_with_line() which accepts the raw specValue (float, int, str).
specValue=0 is kept as a suffix for handicap markets (level ball).
"""

from __future__ import annotations
from typing import Any

# Re-export slug_with_line so harvesters only need one import
from app.workers.canonical_mapper import normalize_line, slug_with_line  # noqa: F401

# ── Type alias ─────────────────────────────────────────────────────────────────
_Entry = tuple[str, bool]   # (base_slug, uses_line)


# =============================================================================
# PER-SPORT MARKET TABLES
# Each entry: market_id → (canonical_base_slug, uses_line_suffix)
# =============================================================================

# ── Football (sportId=1) ──────────────────────────────────────────────────────
_FOOTBALL: dict[int, _Entry] = {
    1:   ("1x2",                       False),
    10:  ("1x2",                       False),
    46:  ("double_chance",             False),
    47:  ("draw_no_bet",               False),
    43:  ("btts",                      False),
    29:  ("btts_and_result",           False),
    386: ("btts_and_result",           False),
    328: ("first_half_btts",           False),
    52:  ("over_under_goals",          True),
    18:  ("over_under_goals",          True),
    353: ("total_goals_home",          True),
    352: ("total_goals_away",          True),
    208: ("result_and_over_under",     True),
    202: ("number_of_goals",           False),  # "Number of Goals in Groups" (0-1, 2-3, 4-5, 6+)
    258: ("exact_goals",               False),  # "Total Goals Exactly" (0, 1, 2, 3…)
    332: ("correct_score",             False),  # "Correct Score" (0:0, 1:0, 2:1…)
    203: ("first_half_correct_score",  False),
    51:  ("asian_handicap",            True),
    53:  ("first_half_asian_handicap", True),
    55:  ("european_handicap",         True),
    45:  ("odd_even",                  False),
    207: ("highest_scoring_half",      False),
    41:  ("first_team_to_score",       False),
    42:  ("first_half_1x2",            False),
    60:  ("first_half_1x2",            False),
    15:  ("first_half_over_under",     True),
    54:  ("first_half_over_under",     True),
    68:  ("first_half_over_under",     True),
    44:  ("ht_ft",                     False),
    162: ("total_corners",             False),
    166: ("total_corners",             True),
    136: ("total_bookings",            False),
    139: ("total_bookings",            True),
}

# ── eFootball / eSoccer (sportId=126) ─────────────────────────────────────────
_EFOOTBALL: dict[int, _Entry] = {
    381: ("1x2",                       False),
    1:   ("1x2",                       False),
    10:  ("1x2",                       False),
    56:  ("over_under_goals",          True),
    52:  ("over_under_goals",          True),
    46:  ("double_chance",             False),
    47:  ("draw_no_bet",               False),
    43:  ("btts",                      False),
    51:  ("asian_handicap",            True),
    45:  ("odd_even",                  False),
    208: ("result_and_over_under",     True),
    258: ("correct_score",             False),
    202: ("exact_goals",               False),
}

# ── Basketball (sportId=2) ─────────────────────────────────────────────────────
_BASKETBALL: dict[int, _Entry] = {
    382: ("match_winner",              False),
    51:  ("point_spread",              True),
    52:  ("total_points",              True),
    353: ("total_points_home",         True),
    352: ("total_points_away",         True),
    45:  ("odd_even_points",           False),
    222: ("winning_margin",            False),
    99:  ("total_points_q1",           True),
    100: ("point_spread_alt",          True),
}

# ── Tennis (sportId=5) ────────────────────────────────────────────────────────
# ── Tennis (sportId=5) ────────────────────────────────────────────────────────
# Drop this block into sp_mapper.py replacing the existing _TENNIS dict.
#
# Grid columns rendered in SportspesaTab (left → right):
#   match_winner  |  first_set_winner  |  second_set_winner
#   game_handicap_<line>  |  total_games_<line>  |  set_handicap_<line>
#   set_betting  |  odd_even_games
#   first_set_game_handicap_<line>  |  first_set_total_games_<line>
#   first_set_match_winner  |  total_games_player1_<line>  |  total_games_player2_<line>

_TENNIS: dict[int, "_Entry"] = {
    # ── Core ────────────────────────────────────────────────────────────────
    382: ("match_winner",              False),   # 2-way match result
    204: ("first_set_winner",          False),   # 1st set winner
    231: ("second_set_winner",         False),   # 2nd set winner
    # ── Full-match handicap / totals ─────────────────────────────────────────
    51:  ("game_handicap",             True),    # Game HC  (spec: -1.5 / -0.5 / +1.5)
    226: ("total_games",               True),    # Total games O/U  (spec: 20.5–24.5)
    439: ("set_handicap",              True),    # Set HC  (spec: -1.5 / +1.5)
    # ── Set score ────────────────────────────────────────────────────────────
    233: ("set_betting",               False),   # 2:0 / 2:1 / 1:2 / 0:2
    # ── Misc full-match ──────────────────────────────────────────────────────
    45:  ("odd_even_games",            False),   # Odd / Even total games
    # ── 1st-set markets ──────────────────────────────────────────────────────
    339: ("first_set_game_handicap",   True),    # 1st set game HC  (spec: -1.5/-0.5/+1.5)
    340: ("first_set_total_games",     True),    # 1st set total games O/U (spec: 8.5/9.5/10.5)
    433: ("first_set_match_winner",    False),   # 1st set + match winner combo (11/12/21/22)
    # ── Per-player games totals ──────────────────────────────────────────────
    353: ("total_games_player1",       True),    # P1 games O/U (spec: 11.5/12.5/13.5)
    352: ("total_games_player2",       True),    # P2 games O/U (spec: 11.5/12.5/13.5)
}
# ── Ice Hockey (sportId=4) ────────────────────────────────────────────────────
_ICE_HOCKEY: dict[int, _Entry] = {
    1:   ("1x2",                       False),
    10:  ("1x2",                       False),
    382: ("match_winner",              False),
    52:  ("total_goals",               True),
    51:  ("puck_line",                 True),
    45:  ("odd_even_goals",            False),
    46:  ("double_chance",             False),
    353: ("total_goals_home",          True),
    352: ("total_goals_away",          True),
    208: ("result_and_over_under",     True),
    43:  ("btts",                      False),
}

# ── Table Tennis (sportId=16) ─────────────────────────────────────────────────
_TABLE_TENNIS: dict[int, _Entry] = {
    382: ("match_winner",              False),
    51:  ("game_handicap",             True),
    226: ("total_games",               True),
    45:  ("odd_even_points",           False),
    233: ("set_betting",               False),
    340: ("first_set_total_games",     True),
}

# ── Volleyball (sportId=23) ───────────────────────────────────────────────────
_VOLLEYBALL: dict[int, _Entry] = {
    382: ("match_winner",              False),
    51:  ("set_handicap",              True),
    226: ("total_sets",                True),
    233: ("set_betting",               False),
    45:  ("odd_even_points",           False),
    353: ("total_points_home",         True),
    352: ("total_points_away",         True),
}

# ── Handball (sportId=6) ──────────────────────────────────────────────────────
_HANDBALL: dict[int, _Entry] = {
    1:   ("1x2",                       False),
    10:  ("1x2",                       False),
    382: ("match_winner",              False),
    52:  ("total_goals",               True),
    51:  ("asian_handicap",            True),
    45:  ("odd_even_goals",            False),
    46:  ("double_chance",             False),
    47:  ("draw_no_bet",               False),
    353: ("total_goals_home",          True),
    352: ("total_goals_away",          True),
    208: ("result_and_over_under",     True),
    43:  ("btts",                      False),
}

# ── American Football (sportId=15) ────────────────────────────────────────────
_AMERICAN_FOOTBALL: dict[int, _Entry] = {
    382: ("match_winner",              False),
    51:  ("point_spread",              True),
    52:  ("total_points",              True),
    45:  ("odd_even_points",           False),
    353: ("total_points_home",         True),
    352: ("total_points_away",         True),
}

# ── Cricket (sportId=21) ──────────────────────────────────────────────────────
_CRICKET: dict[int, _Entry] = {
    382: ("match_winner",              False),
    1:   ("match_winner",              False),
    51:  ("asian_handicap",            True),
    52:  ("total_runs",                True),
    353: ("total_runs_home",           True),
    352: ("total_runs_away",           True),
}

# ── Rugby (sportId=12) ────────────────────────────────────────────────────────
_RUGBY: dict[int, _Entry] = {
    382: ("match_winner",              False),
    1:   ("1x2",                       False),
    10:  ("1x2",                       False),
    46:  ("double_chance",             False),
    51:  ("asian_handicap",            True),
    52:  ("total_points",              True),
    45:  ("odd_even_points",           False),
    353: ("total_points_home",         True),
    352: ("total_points_away",         True),
}

# ── Boxing (sportId=10) ───────────────────────────────────────────────────────
_BOXING: dict[int, _Entry] = {
    382: ("match_winner",              False),
    51:  ("round_betting",             True),
    52:  ("total_rounds",              True),
}

# ── MMA (sportId=117) ─────────────────────────────────────────────────────────
_MMA: dict[int, _Entry] = {
    382: ("match_winner",              False),
    51:  ("round_betting",             True),
    52:  ("total_rounds",              True),
}

# ── Darts (sportId=49) ────────────────────────────────────────────────────────
_DARTS: dict[int, _Entry] = {
    382: ("match_winner",              False),
    226: ("total_legs",                True),
    45:  ("odd_even_legs",             False),
    51:  ("leg_handicap",              True),
}


# =============================================================================
# REGISTRY — sport_id → market table
# =============================================================================
_SPORT_TABLE: dict[int, dict[int, _Entry]] = {
    1:   _FOOTBALL,
    2:   _BASKETBALL,
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
    126: _EFOOTBALL,
}

# Fallback for any sport not in the registry — generic 2-way / O/U
_GENERIC: dict[int, _Entry] = {
    382: ("match_winner",  False),
    1:   ("1x2",           False),
    10:  ("1x2",           False),
    51:  ("handicap",      True),
    52:  ("total",         True),
    45:  ("odd_even",      False),
    46:  ("double_chance", False),
}


# =============================================================================
# PUBLIC FUNCTION
# =============================================================================

def normalize_sp_market(
    mkt_id:   int,
    spec_val: Any   = None,
    sport_id: int   = 1,
) -> str:
    """
    Convert a Sportpesa market ID to a canonical slug.

    Parameters
    ──────────
    mkt_id    int   The numeric market ID from the SP API response.
    spec_val  Any   The specValue field (float, int, str, or None).
                    Used to append the line suffix for handicap / O/U markets.
                    specValue=0 is kept for Asian Handicap (level ball).
    sport_id  int   SP sport.id integer (default 1 = football).
                    MUST be passed for non-football sports or IDs will map
                    to wrong markets (e.g. ID 52 → goals vs points).

    Returns
    ───────
    str   Canonical snake_case slug, e.g.:
            "match_winner"
            "over_under_goals_2.5"
            "point_spread_-5.5"
            "total_points_173.5"
            "game_handicap_-1.5"
            "total_games_22.5"
            "first_set_total_games_9.5"
            "set_handicap_-1.5"
            "sp_2_999"            ← unknown market, prefixed for traceability

    Examples
    ────────
    >>> normalize_sp_market(382, sport_id=2)          # basketball
    'match_winner'
    >>> normalize_sp_market(52, 173.5, sport_id=2)    # basketball
    'total_points_173.5'
    >>> normalize_sp_market(52, 2.5, sport_id=1)      # football
    'over_under_goals_2.5'
    >>> normalize_sp_market(51, -1.5, sport_id=5)     # tennis
    'game_handicap_-1.5'
    >>> normalize_sp_market(51, 0, sport_id=1)        # football AHC level ball
    'asian_handicap_0'
    >>> normalize_sp_market(382, sport_id=5)          # tennis
    'match_winner'
    >>> normalize_sp_market(999, sport_id=2)          # unknown
    'sp_2_999'
    """
    table = _SPORT_TABLE.get(sport_id, _GENERIC)
    entry = table.get(mkt_id)

    if entry is None:
        # Try generic as last resort before giving up
        entry = _GENERIC.get(mkt_id)

    if entry is None:
        return f"sp_{sport_id}_{mkt_id}"

    base, uses_line = entry
    if uses_line:
        return slug_with_line(base, spec_val)
    return base


def get_sport_table(sport_id: int) -> dict[int, _Entry]:
    """Return the raw market table for a sport (for introspection / testing)."""
    return _SPORT_TABLE.get(sport_id, _GENERIC)


def list_all_slugs(sport_id: int) -> list[str]:
    """
    Return all possible canonical base slugs for a sport (ignoring lines).
    Useful for building frontend market filters.
    """
    return sorted({base for base, _ in get_sport_table(sport_id).values()})