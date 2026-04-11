"""app/workers/sp_sports/rugby.py — Rugby Union / League (sportId=12)

Markets in raw SP API response (confirmed from live data):
  10   3 Way - Full Time            → 1x2
  42   3 Way - First Half           → first_half_1x2
  51   Handicap - Full Time         → asian_handicap  (multi-line: -11.5 … +2.5)
  53   Handicap - First Half        → first_half_asian_handicap
  60   Total points - Full Time     → total_points    (multi-line: 56.5 → 64.5)
  45   Odd/Even Point - Full Time   → odd_even
  46   Double Chance - Full Time    → double_chance
  379  Winning margin               → winning_margin  (rugby-specific ranges)
  207  Highest scoring half         → highest_scoring_half
  44   Half Time/Full Time          → ht_ft

NOTE: SP uses market ID 60 for rugby Total Points (not 52).
      Market 379 (Winning Margin) is rugby-specific — add to _RUGBY in sp_mapper.py.
      Market 53 (First Half HC) appears in sp_mapper._FOOTBALL as
      first_half_asian_handicap; add the same mapping to _RUGBY.
"""

from app.workers.sp_harvesterbase import SportConfig

CONFIG = SportConfig(
    slugs        = ("rugby", "rugby-league", "rugby-union"),
    sport_id     = 12,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        # ── Full-match result ────────────────────────────────────────────────
        "10,"    # 3 Way - Full Time (1X2)
        "1,"     # 1X2 alias
        "382,"   # Match Winner (2-way — some fixtures)
        "46,"    # Double Chance
        # ── Handicap ────────────────────────────────────────────────────────
        "51,"    # Handicap - Full Time  (multi-line: -11.5, -9.5, -7.5, -5.5)
        "53,"    # Handicap - First Half (multi-line: -9.5, -3.5, +2.5)
        # ── Totals ──────────────────────────────────────────────────────────
        "60,"    # Total Points - Full Time  (multi-line: 56.5 → 64.5)
        "52,"    # Total Points alt ID
        "353,"   # Home Team Points O/U
        "352,"   # Away Team Points O/U
        # ── Misc ────────────────────────────────────────────────────────────
        "45,"    # Odd/Even Points
        "379,"   # Winning Margin  (Home 1-7, 8-14, 14+ / Away 1-7, 8-14, 14+)
        "207,"   # Highest Scoring Half
        "44"     # Half Time / Full Time
    ).replace("\n", "").replace(" ", ""),
)

# ---------------------------------------------------------------------------
# sp_mapper._RUGBY additions — paste into sp_mapper.py _RUGBY dict
# ---------------------------------------------------------------------------
# Add these entries to the existing _RUGBY table:
#
#   42:  ("first_half_1x2",           False),   # 3 Way - First Half
#   53:  ("first_half_asian_handicap", True),    # Handicap - First Half
#   60:  ("total_points",             True),     # Total Points (rugby uses 60, not 52)
#   379: ("winning_margin",           False),    # Winning Margin (rugby-specific ranges)
#   207: ("highest_scoring_half",     False),    # Highest Scoring Half
#   44:  ("ht_ft",                    False),    # Half Time / Full Time
#
# The canonical_mapper already handles:
#   winning_margin outcome keys via _WINNING_MARGIN_DISPLAY regex
#   ht_ft via _HTFT_MAP  ("11"→"1/1", "22"→"2/2" etc.)