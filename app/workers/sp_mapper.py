"""
app/workers/sp_mapper.py
=========================
Sportpesa-specific market ID → canonical slug mapping.

Usage in the harvester
----------------------
  from app.workers.sp_mapper import normalize_sp_market
  from app.workers.canonical_mapper import normalize_outcome

Public API
----------
  normalize_sp_market(sp_mkt_id, spec_value=None) → canonical slug string

Market ID reference (confirmed from intercepted SP API traffic)
-------------------------------
  ID    Slug
  ──    ────────────────────────────────────────
  1     1x2
  10    1x2
  29    btts
  41    first_team_to_score           ← added v2
  42    first_half_1x2                ← added v2
  43    btts
  44    ht_ft                         ← added v2
  45    odd_even
  46    double_chance
  47    draw_no_bet
  51    asian_handicap
  52    over_under_goals  (multi-line via specValue)
  53    first_half_asian_handicap     ← added v2
  54    first_half_over_under         ← added v2
  55    european_handicap
  60    first_half_1x2
  15    first_half_over_under
  18    over_under_goals
  68    first_half_over_under
  99    total_points
  100   point_spread
  136   total_bookings
  139   total_bookings
  162   total_corners
  166   total_corners
  202   number_of_goals
  203   first_half_correct_score      ← added v2
  207   highest_scoring_half          ← added v2
  208   result_and_over_under
  258   exact_goals
  328   first_half_btts               ← added v2
  332   correct_score
  352   total_goals_away
  353   total_goals_home
  382   basketball_moneyline
  386   btts_and_result

Line markets (uses_line=True) produce keys like:
  over_under_goals_2.5, asian_handicap_-0.75, european_handicap_1, ...

Spec-value=0 note
-----------------
  The harvester MUST use `is None` checks (not `or`) when extracting specValue
  so that integer 0 is preserved.  See sp_harvester._parse_markets.
"""

from __future__ import annotations

from typing import Any

from app.workers.canonical_mapper import normalize_line, slug_with_line


# =============================================================================
# SP MARKET ID TABLE
# =============================================================================
#   (slug, uses_line)
#   uses_line=True  → append specValue as line suffix
#   uses_line=False → return slug as-is

_SP_MKT: dict[int, tuple[str, bool]] = {
    # ── Full-time core ─────────────────────────────────────────────────────
    1:   ("1x2",                       False),
    10:  ("1x2",                       False),
    46:  ("double_chance",             False),
    47:  ("draw_no_bet",               False),
    43:  ("btts",                      False),
    29:  ("btts",                      False),
    386: ("btts_and_result",           False),

    # ── Over/Under Goals — multiple lines via specValue ─────────────────────
    # Market 52 = Total Goals Over/Under (standard, most common)
    # Market 18 = alternate ID for same market type
    52:  ("over_under_goals",          True),
    18:  ("over_under_goals",          True),

    # ── Team goal totals ────────────────────────────────────────────────────
    353: ("total_goals_home",          True),
    352: ("total_goals_away",          True),

    # ── Combo markets ───────────────────────────────────────────────────────
    208: ("result_and_over_under",     True),
    258: ("exact_goals",               False),
    202: ("number_of_goals",           False),
    332: ("correct_score",             False),

    # ── Handicaps ───────────────────────────────────────────────────────────
    51:  ("asian_handicap",            True),
    53:  ("first_half_asian_handicap", True),   # ← v2: HT Asian HC
    55:  ("european_handicap",         True),

    # ── Special full-time ───────────────────────────────────────────────────
    45:  ("odd_even",                  False),
    41:  ("first_team_to_score",       False),  # ← v2
    207: ("highest_scoring_half",      False),  # ← v2

    # ── Half-time 1X2 ───────────────────────────────────────────────────────
    42:  ("first_half_1x2",            False),  # ← v2 (alias of 60)
    60:  ("first_half_1x2",            False),

    # ── Half-time Over/Under ─────────────────────────────────────────────────
    15:  ("first_half_over_under",     True),
    54:  ("first_half_over_under",     True),   # ← v2 (alias of 15 / 68)
    68:  ("first_half_over_under",     True),

    # ── Half-time specials ──────────────────────────────────────────────────
    44:  ("ht_ft",                     False),  # ← v2: Half Time / Full Time
    328: ("first_half_btts",           False),  # ← v2: HT Both Teams to Score
    203: ("first_half_correct_score",  False),  # ← v2: HT Correct Score

    # ── Corners / Bookings ──────────────────────────────────────────────────
    162: ("total_corners",             False),
    166: ("total_corners",             True),
    136: ("total_bookings",            False),
    139: ("total_bookings",            True),

    # ── Non-football ────────────────────────────────────────────────────────
    382: ("basketball_moneyline",      False),
    99:  ("total_points",              True),
    100: ("point_spread",              True),
}


# =============================================================================
# PUBLIC API
# =============================================================================

def normalize_sp_market(sp_mkt_id: int, spec_value: Any = None) -> str:
    """
    Map a Sportpesa market ID (and optional specValue) to a canonical slug.

    Examples
    --------
    normalize_sp_market(52, 2.5)   → "over_under_goals_2.5"
    normalize_sp_market(52, 3)     → "over_under_goals_3"
    normalize_sp_market(1,  None)  → "1x2"
    normalize_sp_market(55, -0.75) → "european_handicap_-0.75"
    normalize_sp_market(999, None) → "sp_999"  (unknown — pass through)

    specValue must be extracted with `is None` checks in the harvester
    to correctly handle specValue=0 (e.g. for markets with a 0 handicap).
    """
    try:
        entry = _SP_MKT.get(int(sp_mkt_id))
    except (TypeError, ValueError):
        return f"sp_{sp_mkt_id}"

    if not entry:
        return f"sp_{sp_mkt_id}"

    base, uses_line = entry
    if uses_line:
        return slug_with_line(base, spec_value)
    return base


def is_line_market(sp_mkt_id: int) -> bool:
    """Return True if this market ID uses specValue as a line suffix."""
    try:
        entry = _SP_MKT.get(int(sp_mkt_id))
        return bool(entry and entry[1])
    except (TypeError, ValueError):
        return False


def known_market_ids() -> list[int]:
    """Return all known SP market IDs."""
    return list(_SP_MKT.keys())