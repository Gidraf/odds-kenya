"""
app/workers/mappers/b2b.py
===========================
B2B bookmaker family market normalisation.

Covers: 1xBet, 22Bet, Helabet, Paripesa, Melbet, Betwinner, Megapari
— all share the same market name vocabulary.

When a B2B bookmaker introduces a new market name, edit ONLY this file.

Exports
-------
  normalize_b2b_market(mkt_name)  → canonical slug
"""

from __future__ import annotations

import re


# =============================================================================
# B2B BASE NAME → SLUG
# =============================================================================
# Key = lowercase market name as returned by the B2B API.
# The normaliser lowercases + strips input before lookup, so these keys must
# all be lowercase.

_B2B_BASE_NAME: dict[str, str] = {
    # ── Core ─────────────────────────────────────────────────────────────────
    "1x2":               "1x2",
    "handicap":          "asian_handicap",
    "eur handicap":      "european_handicap",
    "european handicap": "european_handicap",
    "asian handicap":    "asian_handicap",
    "correct score":     "correct_score",
    "ht correct score":  "correct_score",
    "ht/ft":             "ht_ft",
    "halftime/fulltime": "ht_ft",

    # ── Goalscorer ────────────────────────────────────────────────────────────
    "first scorer":    "first_goalscorer",
    "last scorer":     "last_goalscorer",
    "anytime scorer":  "anytime_goalscorer",

    # ── Chance markets ────────────────────────────────────────────────────────
    "double chance":     "double_chance",
    "draw no bet":       "draw_no_bet",

    # ── Clean sheet / Win to nil ──────────────────────────────────────────────
    "win to nil":     "win_to_nil_home",
    "clean sheet":    "clean_sheet_home",

    # ── BTTS ──────────────────────────────────────────────────────────────────
    "gg/ng":  "btts",
    "btts":   "btts",

    # ── Next goal ─────────────────────────────────────────────────────────────
    "next goal":  "next_goal",

    # ── Totals ────────────────────────────────────────────────────────────────
    "1h total":  "first_half_over_under",
    "2h total":  "second_half_over_under",
    "total":     "over_under_goals",

    # ── Result combos ─────────────────────────────────────────────────────────
    "result+total":  "result_and_over_under",
    "result + total": "result_and_over_under",

    # ── Goals ─────────────────────────────────────────────────────────────────
    "exact goals":   "exact_goals",
    "number of goals": "number_of_goals",

    # ── Corners ───────────────────────────────────────────────────────────────
    "corners":          "total_corners",
    "asian corners":    "asian_corners",
    "corners handicap": "asian_corners",

    # ── Bookings ──────────────────────────────────────────────────────────────
    "bookings":   "total_bookings",
    "cards total": "total_bookings",

    # ── Half results ──────────────────────────────────────────────────────────
    "1h result":  "first_half_1x2",
    "2h result":  "second_half_result",

    # ── Double chance 1H ──────────────────────────────────────────────────────
    "1h double chance":  "double_chance",

    # ── Halves ────────────────────────────────────────────────────────────────
    "win both halves":    "score_both_halves",
    "score both halves":  "score_both_halves",

    # ── Team totals ────────────────────────────────────────────────────────────
    "team total":  "total_goals_home",

    # ── Handicap variants ─────────────────────────────────────────────────────
    "1h handicap":  "european_handicap",
    "2h handicap":  "european_handicap",

    # ── Odd / Even ────────────────────────────────────────────────────────────
    "odd/even":  "odd_even",
}

# ── Compiled regex for "base_name_line" slug patterns ─────────────────────────
# e.g. "total_2.5" → base="total", line="2.5"
_B2B_LINE_RE = re.compile(r"^(.+?)_(-?[\d.]+)$")


# =============================================================================
# PUBLIC NORMALISER
# =============================================================================

def normalize_b2b_market(mkt_name: str) -> str:
    """
    Return a canonical market slug for a B2B-family bookmaker market.

    The B2B family (1xBet, 22Bet, Helabet, Paripesa, Melbet, Betwinner,
    Megapari) all share the same internal market vocabulary.

    Parameters
    ----------
    mkt_name:  Market name as returned by the B2B API (any case)

    Examples
    --------
    >>> normalize_b2b_market("1H Total")
    'first_half_over_under'
    >>> normalize_b2b_market("total_2.5")
    'over_under_goals_2.5'
    >>> normalize_b2b_market("Handicap")
    'asian_handicap'
    """
    clean = mkt_name.strip().lower()

    # ── 1. Direct name lookup ─────────────────────────────────────────────────
    exact = _B2B_BASE_NAME.get(clean)
    if exact:
        return exact

    # ── 2. "base_line" compound e.g. "total_2.5" or "corners_9.5" ────────────
    m = _B2B_LINE_RE.match(clean)
    if m:
        base_raw = m.group(1).strip()
        line     = m.group(2)
        slug     = _B2B_BASE_NAME.get(base_raw)
        if slug:
            return f"{slug}_{line}"

    # ── 3. Sanitise fallback ──────────────────────────────────────────────────
    return re.sub(r"[^a-z0-9]+", "_", clean).strip("_") or "unknown"