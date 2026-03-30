"""
app/workers/mappers/shared.py
==============================
Shared line-normalisation and outcome-normalisation utilities used by
every bookmaker mapper.

Exports
-------
  normalize_line(raw_value)                            → "2.5" / "-0.5" / ""
  slug_with_line(base, raw_line)                       → "over_under_goals_2.5"
  normalize_outcome(market_slug, raw_key, display="") → canonical outcome key

Lookup tables kept here (single source of truth):
  _OUT_EXACT       — shortName → canonical key (covers every SP shortName)
  _SP_COMBO        — OV_1, 1GG, XNG … combo outcomes
  _HTFT_MAP        — HT/FT "11" / "1/1" etc.
  _FSMW_MAP        — First Set / Match Winner (tennis)
  _WINNING_MARGIN  — Basketball winning-margin shortNames

Nothing in this file is bookmaker-specific.
"""

from __future__ import annotations

import re
from typing import Any


# =============================================================================
# LINE HELPERS
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
    if line:
        return f"{base}_{line}"
    return base


# =============================================================================
# OUTCOME LOOKUP TABLES
# =============================================================================

# ── Primary lookup — shortName → canonical key ───────────────────────────────
_OUT_EXACT: dict[str, str] = {
    # 3-way result
    "1":        "1",
    "x":        "X",
    "2":        "2",
    "home":     "1",
    "draw":     "X",
    "away":     "2",

    # Over / Under
    "ov":       "over",
    "un":       "under",
    "over":     "over",
    "under":    "under",

    # BTTS
    "yes":      "yes",
    "no":       "no",
    "gg":       "yes",
    "ng":       "no",

    # Odd / Even  (SP shortNames "OD" / "EV")
    "odd":      "odd",
    "even":     "even",
    "od":       "odd",
    "ev":       "even",

    # Double Chance
    "1x":           "1X",
    "x2":           "X2",
    "12":           "12",
    "2x":           "X2",
    "1/x":          "1X",
    "x/2":          "X2",
    "1/2":          "12",
    "1 or x":       "1X",
    "1 or 2":       "12",
    "x or 2":       "X2",
    "home or draw": "1X",
    "home or away": "12",
    "draw or away": "X2",

    # First Team to Score
    "none":         "none",
    "no goal":      "none",

    # Highest Scoring Half
    "eql":          "equal",
    "equal":        "equal",
    "1st":          "1st",
    "2nd":          "2nd",
    "first half":   "1st",
    "second half":  "2nd",

    # Correct Score / Any Other
    "othr":         "other",
    "any other":    "other",

    # Basketball quarter shortNames
    "1stq":         "1st_quarter",
    "2ndq":         "2nd_quarter",
    "3rdq":         "3rd_quarter",
    "4thq":         "4th_quarter",

    # Tennis player shortNames
    "p1":           "1",
    "p2":           "2",
    "player 1":     "1",
    "player 2":     "2",
}

# ── SP combo outcomes: Result + O/U (market 208) and Result + BTTS (386) ─────
_SP_COMBO: dict[str, str] = {
    "ov_1":  "1_over",    "ov_x":  "X_over",    "ov_2":  "2_over",
    "un_1":  "1_under",   "un_x":  "X_under",   "un_2":  "2_under",
    "1gg":   "1_yes",     "xgg":   "X_yes",     "2gg":   "2_yes",
    "1ng":   "1_no",      "xng":   "X_no",      "2ng":   "2_no",
}

# ── HT/FT (market 44) — checked BEFORE _OUT_EXACT because "12" collides ──────
_HTFT_MAP: dict[str, str] = {
    "11": "1/1",  "1x": "1/X",  "12": "1/2",
    "x1": "X/1",  "xx": "X/X",  "x2": "X/2",
    "21": "2/1",  "2x": "2/X",  "22": "2/2",
    "1/1": "1/1", "1/x": "1/X", "1/2": "1/2",
    "x/1": "X/1", "x/x": "X/X", "x/2": "X/2",
    "2/1": "2/1", "2/x": "2/X", "2/2": "2/2",
}

# ── First Set / Match Winner combo (tennis market 433) ────────────────────────
_FSMW_MAP: dict[str, str] = {
    "11": "1/1",  "12": "1/2",
    "21": "2/1",  "22": "2/2",
}

# ── Winning Margin (basketball market 222) ────────────────────────────────────
_WINNING_MARGIN: dict[str, str] = {
    "h15":  "home_1_5",     "h610": "home_6_10",
    "h_10": "home_11plus",  "h11":  "home_11plus",
    "a15":  "away_1_5",     "a610": "away_6_10",
    "a_10": "away_11plus",  "a11":  "away_11plus",
}

# ── Compiled regexes ──────────────────────────────────────────────────────────
_ROUND_RE           = re.compile(r"^(?:rd?|round\s*)(\d+)$",          re.I)
_OUT_OVER_RE        = re.compile(r"^over\s+([\d.]+)$",                 re.I)
_OUT_UNDER_RE       = re.compile(r"^under\s+([\d.]+)$",                re.I)
_OUT_SCORE_RE       = re.compile(r"^\d+:\d+$")
_OUT_HCP_RE         = re.compile(r"^([12X])\s*[\(\[].*[\)\]]$",        re.I)
_NUMERIC_RANGE_RE   = re.compile(r"^\d+[-–+]\d*$")
_WINNING_MARGIN_RE  = re.compile(
    r"(?P<side>home|away)\s+by\s+(?P<range>[\d]+[-–+][\d]*)\s*pts?", re.I,
)


# =============================================================================
# OUTCOME NORMALISATION
# =============================================================================

def normalize_outcome(
    market_slug: str,
    raw_key:     str,
    display:     str = "",
) -> str:
    """
    Return a canonical outcome key for any raw outcome string.

    Resolution order
    ────────────────
    0. Round betting markets → "round_N"
    1. ht_ft market          → _HTFT_MAP
    2. first_set_match_winner → _FSMW_MAP
    3. winning_margin        → _WINNING_MARGIN / regex
    4. _OUT_EXACT / shape-regexes (over/under, score, hcp bracket)
    5. _SP_COMBO             → OV_1, 1GG, XNG, etc.
    6. Numeric range         → "0-1", "2-3", "6+"
    7. Sanitise fallback
    """
    kl = raw_key.strip().lower()

    # ── 0. Combat sport round betting ────────────────────────────────────────
    if market_slug.startswith("round_betting"):
        m = _ROUND_RE.match(raw_key.strip())
        if m:
            return f"round_{m.group(1)}"
        if raw_key.strip().isdigit():
            return f"round_{raw_key.strip()}"

    # ── 1. HT/FT ─────────────────────────────────────────────────────────────
    if "ht_ft" in market_slug:
        v = _HTFT_MAP.get(kl)
        if v:
            return v

    # ── 2. First Set / Match Winner ───────────────────────────────────────────
    if market_slug == "first_set_match_winner":
        v = _FSMW_MAP.get(kl)
        if v:
            return v

    # ── 3. Winning Margin ─────────────────────────────────────────────────────
    if market_slug == "winning_margin":
        v = _WINNING_MARGIN.get(kl)
        if v:
            return v
        for txt in (raw_key, display):
            m = _WINNING_MARGIN_RE.match(txt.strip())
            if m:
                side = "home" if m.group("side").lower() == "home" else "away"
                rng  = m.group("range").replace("–", "-").replace(" ", "")
                return f"{side}_{rng.replace('-', '_').replace('+', 'plus')}"
        return re.sub(r"[^a-z0-9_]+", "_", kl).strip("_") or "unknown"

    # ── 4. General exact / regex matching ─────────────────────────────────────
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

        # Correct score "2:1" etc. — pass through
        if _OUT_SCORE_RE.match(raw.strip()):
            return raw.strip()

        # Handicap with bracket "1 (+1.5)"
        m = _OUT_HCP_RE.match(raw.strip())
        if m:
            return m.group(1).upper()

    # ── 5. SP combo shortNames ────────────────────────────────────────────────
    if kl in _SP_COMBO:
        return _SP_COMBO[kl]

    # ── 6. Numeric range / goal groups ───────────────────────────────────────
    raw_stripped = raw_key.strip()
    if _NUMERIC_RANGE_RE.match(raw_stripped):
        return raw_stripped

    # ── 7. Sanitise fallback ──────────────────────────────────────────────────
    raw = raw_stripped or display.strip()
    return re.sub(r"[^a-z0-9_:+./\-]+", "_", raw.lower()).strip("_") or "unknown"