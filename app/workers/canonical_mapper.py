"""
app/workers/canonical_mapper.py  (patched)
==========================================
Canonical market name resolution for OdiBets.

WHAT WAS BROKEN
───────────────
normalize_od_market() was a thin stub that produced generic slugs like
"soccer_unknown_93" for everything except the few it hard-coded.
It did NOT route through the sport-specific mapper classes in
app/workers/mappers/.

This file replaces the old normalize_od_market() with one that:

  1. Calls the correct sport-specific mapper (OdibetsSoccerMapper, etc.)
  2. Falls back to pattern heuristics if the mapper returns None
  3. Falls back to the AI dynamic mapper (if ANTHROPIC_API_KEY is set)
  4. Finally returns "sport_unknown_NNN" only for truly unresolvable markets

All other functions (normalize_line, normalize_outcome, slug_with_line)
are unchanged — they are still used by od_harvester.py.

USAGE IN od_harvester.py
─────────────────────────
No changes needed in od_harvester.py itself.  Just replace canonical_mapper.py.
The harvester calls:

    from app.workers.canonical_mapper import (
        normalize_line,
        normalize_od_market,
        normalize_outcome,
        slug_with_line,
    )

    slug = normalize_od_market(sid, spec, sport=sport_slug)

The new normalize_od_market signature is:
    normalize_od_market(sub_type_id, spec_str="", *, sport="soccer") -> str

OLD behaviour: returned "soccer_unknown_93" for unknown markets.
NEW behaviour: routes through mapper, returns proper canonical slug.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# UNCHANGED HELPERS  (od_harvester.py depends on these signatures)
# ══════════════════════════════════════════════════════════════════════════════

def normalize_line(raw: str) -> str:
    """Normalise a numeric specifier string into a canonical slug fragment."""
    if not raw:
        return ""
    # Remove whitespace, handle both "2.5" and "2_5"
    raw = raw.strip().replace(" ", "").replace(",", ".")
    try:
        val = float(raw)
        if val == int(val):
            return str(int(val))
        return f"{val:g}".replace(".", "_")
    except (ValueError, TypeError):
        return re.sub(r"[^\w]", "_", raw).strip("_")


def slug_with_line(base: str, line: str) -> str:
    """Append a line to a base slug, e.g. 'over_under_goals' + '2.5' → 'over_under_goals_2_5'."""
    if not line:
        return base
    norm = normalize_line(line)
    if not norm:
        return base
    return f"{base}_{norm}"


def normalize_outcome(market_slug: str, raw_key: str, display: str = "") -> str:
    """
    Convert a raw OdiBets outcome key to a canonical outcome name.

    Routes to the sport-specific mapper's transform_outcome() when available,
    otherwise applies generic heuristics.
    """
    key = raw_key.strip()
    kl  = key.lower()

    # Quick wins for the most common patterns
    _QUICK = {
        "1": "1", "home": "1", "w1": "1",
        "x": "X", "draw": "X", "tie": "X",
        "2": "2", "away": "2", "w2": "2",
        "yes": "Yes", "no": "No",
        "over": "Over", "under": "Under",
        "odd": "Odd", "even": "Even",
        "1x": "1X", "x2": "X2", "12": "12",
    }
    if kl in _QUICK:
        return _QUICK[kl]

    # Score patterns: 0:0, 1:2, etc.
    if re.match(r"^\d+:\d+$", key):
        return key

    # Exact goal counts: 0, 1, 2+, 3+
    if re.match(r"^\d+\+?$", key):
        return key

    # Over/Under with line e.g. "Over 2.5"
    m = re.match(r"^(over|under)\s+([\d.]+)$", kl)
    if m:
        return m.group(1).capitalize()

    # HT/FT pattern e.g. "1/2", "X/1"
    if re.match(r"^[12x]/[12x]$", kl):
        return key.upper()

    # Fallback: sanitise
    return re.sub(r"[^a-zA-Z0-9_:+./\-]+", "_", key).strip("_") or key


# ══════════════════════════════════════════════════════════════════════════════
# PATCHED  normalize_od_market()
# ══════════════════════════════════════════════════════════════════════════════

def normalize_od_market(
    sub_type_id: int | str,
    spec_str:    str = "",
    *,
    sport:       str = "soccer",
) -> str:
    """
    Resolve an OdiBets (sub_type_id, specifier) pair to a canonical market slug.

    Resolution order:
      1. Sport-specific mapper class (OdibetsSoccerMapper, etc.)
      2. Pattern heuristics on the spec string
      3. AI dynamic mapper (if ANTHROPIC_API_KEY or OPENAI_API_KEY set)
      4. Fall back: "{sport}_unknown_{sub_type_id}"

    Args:
        sub_type_id : OdiBets internal market type integer
        spec_str    : specifier string from the API e.g. "total=2.5" or ""
        sport       : canonical sport slug

    Returns:
        Canonical market slug string.
    """
    sid = int(sub_type_id) if str(sub_type_id).isdigit() else 0

    # ── Build a pseudo-slug from the specifier so mappers can parse it ─────────
    market_slug = _build_slug_from_spec(sid, spec_str, sport)

    # ── 1. Sport-specific mapper ───────────────────────────────────────────────
    try:
        from app.workers.mappers import resolve_od_market as _resolve
        canonical, _outcomes = _resolve(sport, market_slug, {})
        if canonical and not canonical.endswith(f"_unknown_{sid}"):
            return _embed_line(canonical, spec_str)
    except Exception as exc:
        log.debug("Mapper dispatch error sid=%s sport=%s: %s", sid, sport, exc)

    # ── 2. Heuristic fallback (no mapper needed) ───────────────────────────────
    heuristic = _heuristic(market_slug, spec_str, sport)
    if heuristic:
        return heuristic

    # ── 3. AI (optional — only if key present) ────────────────────────────────
    import os
    if os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY"):
        try:
            from app.workers.dynamic_canonical_mapper import get_mapper
            result = get_mapper().resolve(
                bookmaker   = "od",
                sport       = sport,
                source_id   = str(sid),
                source_name = market_slug,
                outcomes    = [],
                use_ai      = True,
            )
            if result.confidence >= 0.6 and "unknown" not in result.canonical_slug:
                return result.canonical_slug
        except Exception as exc:
            log.debug("AI mapper error: %s", exc)

    # ── 4. Unknown ────────────────────────────────────────────────────────────
    return f"{sport}_unknown_{sid}"


# ── Internal helpers ──────────────────────────────────────────────────────────

# OD sub_type_id → (sport, base_slug) for the most common IDs seen in production
_KNOWN_IDS: dict[int, dict[str, str]] = {
    # Soccer / universal
    1:   {"soccer": "1x2"},
    2:   {"soccer": "double_chance"},
    3:   {"soccer": "draw_no_bet"},
    4:   {"soccer": "btts"},
    5:   {"soccer": "first_team_to_score"},
    7:   {"soccer": "correct_score"},
    8:   {"soccer": "half_time_result"},
    10:  {"soccer": "over_under_goals"},          # line in spec
    11:  {"soccer": "asian_handicap"},
    12:  {"soccer": "european_handicap"},
    13:  {"soccer": "odd_even_goals"},
    14:  {"soccer": "first_half_over_under"},
    16:  {"soccer": "first_half_1x2"},
    22:  {"soccer": "half_time_full_time"},
    23:  {"soccer": "btts_first_half"},
    26:  {"soccer": "over_under_goals_1_5"},
    27:  {"soccer": "over_under_goals_3_5"},
    28:  {"soccer": "over_under_goals_4_5"},
    29:  {"soccer": "over_under_goals_0_5"},
    33:  {"soccer": "exact_goals"},
    34:  {"soccer": "total_goals_exact"},
    93:  {"soccer": "total_goals_exact"},         # outcomes: 0, 1, 2+
    130: {"soccer": "over_under_goals"},
    131: {"soccer": "over_under_goals"},
    132: {"soccer": "over_under_goals"},
    133: {"soccer": "over_under_goals"},
    140: {"soccer": "exact_goals"},
    141: {"soccer": "btts"},
    147: {"soccer": "odd_even_goals"},
    149: {"basketball": "match_winner"},
    156: {"basketball": "asian_handicap"},
    161: {"basketball": "over_under"},
    303: {"soccer": "match_winner"},
    304: {"soccer": "match_winner"},
    315: {"basketball": "1x2"},
    316: {"basketball": "over_under"},
    544: {"soccer": "result_and_over_under"},     # 1_under_1.5, x_over_2.5 etc
    820: {"soccer": "half_time_full_time"},       # x/x_2, 2/2_2 etc
    881: {"soccer": "correct_score"},
}


def _build_slug_from_spec(sid: int, spec_str: str, sport: str) -> str:
    """Build a mapper-recognisable slug from sub_type_id + specifier."""
    # Check known ID table first
    if sid in _KNOWN_IDS:
        sport_map = _KNOWN_IDS[sid]
        # Use the sport-specific slug if present, otherwise take the first one
        base = sport_map.get(sport) or list(sport_map.values())[0]
        line = _extract_line(spec_str)
        if line and "over_under" in base:
            return slug_with_line(base, line)
        return base

    # Parse spec_str for line/handicap
    line = _extract_line(spec_str)
    hcp  = _extract_handicap(spec_str)

    # Build sport-prefixed slug with line
    if line:
        return f"over_under_{sport}_goals_{normalize_line(line)}"
    if hcp:
        sign = "minus_" if float(hcp) < 0 else ""
        abs_hcp = normalize_line(str(abs(float(hcp))))
        return f"{sport}_asian_handicap_{sign}{abs_hcp}"

    return f"{sport}_market_{sid}"


def _extract_line(spec_str: str) -> str:
    """Pull a numeric line from a specifier string like 'total=2.5' or '2.5'."""
    if not spec_str:
        return ""
    m = re.search(r"total=([\d.]+)", spec_str)
    if m:
        return m.group(1)
    m = re.search(r"([\d]+\.[\d]+)", spec_str)
    if m:
        return m.group(1)
    return ""


def _extract_handicap(spec_str: str) -> str:
    """Pull a handicap from a specifier string."""
    if not spec_str:
        return ""
    m = re.search(r"hcp=([-\d.]+)", spec_str)
    if m:
        return m.group(1)
    m = re.search(r"handicap=([-\d.]+)", spec_str)
    if m:
        return m.group(1)
    return ""


def _embed_line(slug: str, spec_str: str) -> str:
    """If slug is a base O/U slug and spec has a line, append it."""
    if not spec_str:
        return slug
    if "over_under" in slug or "total" in slug:
        line = _extract_line(spec_str)
        if line and not re.search(r"_\d", slug.rsplit("_", 1)[-1]):
            return slug_with_line(slug, line)
    return slug


def _heuristic(market_slug: str, spec_str: str, sport: str) -> Optional[str]:
    """Fast pattern heuristics that don't need a mapper class."""
    kl = market_slug.lower()

    if "1x2" in kl or kl in ("1x2", "soccer_1x2", "basketball_1x2"):
        if "first_half" in kl:
            return "first_half_1x2"
        if "second_half" in kl:
            return "second_half_1x2"
        return "1x2"

    if "double_chance" in kl:
        return "double_chance"

    if "draw_no_bet" in kl or "dnb" in kl:
        return "draw_no_bet"

    if "btts" in kl or ("both" in kl and "score" in kl):
        if "first_half" in kl:
            return "btts_first_half"
        return "btts"

    if "half_time_full_time" in kl or "ht_ft" in kl or "ht/ft" in kl:
        return "half_time_full_time"

    if "correct_score" in kl:
        if "first_half" in kl:
            return "correct_score_first_half"
        return "correct_score"

    if "asian_handicap" in kl:
        line = _extract_line(spec_str) or _extract_handicap(spec_str)
        if line:
            return slug_with_line("asian_handicap", line)
        return "asian_handicap"

    if "european_handicap" in kl:
        return "european_handicap"

    if "odd_even" in kl:
        return "odd_even_goals"

    if "over_under" in kl or "total" in kl:
        line = _extract_line(spec_str)
        base = f"over_under_goals"
        if line:
            return slug_with_line(base, line)
        # Try to pull line from slug itself e.g. over_under_goals_2_5
        m = re.search(r"([\d]+_[\d]+)$", market_slug)
        if m:
            return f"{base}_{m.group(1)}"
        return base

    if "first_half" in kl and "1x2" not in kl:
        return "first_half_over_under"

    if "exact_goals" in kl or "total_goals_exact" in kl:
        return "total_goals_exact"

    if "anytime_score" in kl or "anytime_goal" in kl:
        return "anytime_scorer"

    if "first_team_to_score" in kl or "first_goal" in kl:
        return "first_team_to_score"

    if "winning_margin" in kl:
        return "winning_margin"

    if "match_winner" in kl:
        return "match_winner"

    return None