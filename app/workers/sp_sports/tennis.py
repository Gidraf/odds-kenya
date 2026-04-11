"""app/workers/sp_sports/tennis.py — Tennis (sportId=5)

Markets fetched (in grid-column priority order):
  382  Match Winner (2-way)
          → Sets 2-0 / Sets 2-1 / Sets 1-2 / Sets 0-2
"""

from app.workers.sp_harvesterbase import SportConfig

CONFIG = SportConfig(
    slugs        = ("tennis",),
    sport_id     = 5,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        # ── Core match markets ──────────────────────────────────────────────
        "382,"   # Match Winner (2-way, incl. retirement)
        "204,"   # First Set Winner
        "231,"   # Second Set Winner
        # ── Handicap / totals ────────────────────────────────────────────────
        "51,"    # Game Handicap full match  (spec: -1.5 / -0.5 / 0 / +1.5)
        "226,"   # Total Games O/U           (spec: 20.5-24.5)
        "439,"   # Set Handicap              (spec: -1.5 / +1.5)
        # ── Set score ────────────────────────────────────────────────────────
        "233,"   # Set Betting  2:0 / 2:1 / 1:2 / 0:2
        # ── Misc full-match ──────────────────────────────────────────────────
        "45,"    # Odd/Even Games
        # ── 1st-set markets ──────────────────────────────────────────────────
        "339,"   # 1st Set Game Handicap     (spec: -1.5 / -0.5 / +1.5)
        "340,"   # 1st Set Total Games O/U   (spec: 8.5 / 9.5 / 10.5)
        "433,"   # 1st Set / Match Winner combo
        # ── Per-player games totals ──────────────────────────────────────────
        "353,"   # Player 1 Games O/U        (spec: 11.5 / 12.5 / 13.5)
        "352"    # Player 2 Games O/U        (spec: 11.5 / 12.5 / 13.5)
    ).replace("\n", "").replace(" ", ""),
)

# ---------------------------------------------------------------------------
# Frontend grid-column definitions for SportspesaTab
# Import this dict in the React component (serialise to JSON via the API or
# embed directly in the TSX column config constant).
# ---------------------------------------------------------------------------

#: Ordered list of grid columns shown for tennis matches.
#: Each entry:  (slug_prefix, label, outcome_key, column_class)
TENNIS_GRID_COLS: list[dict] = [
    # ── Match Winner ────────────────────────────────────────────────────────
    {"market": "match_winner",     "outcome": "1",     "label": "Home",    "cls": "col-mw"},
    {"market": "match_winner",     "outcome": "2",     "label": "Away",    "cls": "col-mw"},
    # ── First Set Winner ────────────────────────────────────────────────────
    {"market": "first_set_winner", "outcome": "1",     "label": "1st Set Home", "cls": "col-set"},
    {"market": "first_set_winner", "outcome": "2",     "label": "1st Set Away", "cls": "col-set"},
    # ── Total Games O/U  (best line resolved at render time) ────────────────
    {"market": "total_games",      "outcome": "over",  "label": "Gms OV",  "cls": "col-ou",
     "note": "picks best available spec line (e.g. 22.5)"},
    {"market": "total_games",      "outcome": "under", "label": "Gms UN",  "cls": "col-ou"},
    # ── Set Handicap ────────────────────────────────────────────────────────
    {"market": "set_handicap",     "outcome": "1",     "label": "Sets HC",  "cls": "col-hc",
     "note": "spec -1.5 shown by default"},
    # ── Set Betting (correct score) ─────────────────────────────────────────
    {"market": "set_betting",      "outcome": "2:0",   "label": "Sets 2-0", "cls": "col-sb"},
    {"market": "set_betting",      "outcome": "2:1",   "label": "Sets 2-1", "cls": "col-sb"},
    {"market": "set_betting",      "outcome": "0:2",   "label": "Sets 0-2", "cls": "col-sb"},
]

# ---------------------------------------------------------------------------
# Helper used by SportspesaTab to pick the best available O/U line to show
# in the grid (prefers the line closest to 50/50 implied probability).
# ---------------------------------------------------------------------------

def best_total_games_line(markets: dict) -> str | None:
    """
    Given a normalised markets dict, return the spec-value suffix of the
   
    """
    import re
    best_key, best_diff = None, float("inf")
    for key, outcomes in markets.items():
        if not key.startswith("total_games_"):
            continue
        m = re.match(r"total_games_([\d.]+)$", key)
        if not m:
            continue
        over  = float(outcomes.get("over",  0) or 0)
        under = float(outcomes.get("under", 0) or 0)
        if over <= 1.0 or under <= 1.0:
            continue
        # implied probabilities
        p_over  = 1 / over
        p_under = 1 / under
        diff = abs(p_over - p_under)
        if diff < best_diff:
            best_diff = diff
            best_key  = m.group(1)
    return best_key