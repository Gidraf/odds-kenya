"""
app/workers/market_mapper.py
=============================
Backwards-compatibility shim.

All code that previously imported from market_mapper will continue to work
unchanged because this module re-exports everything from the two split files:

  canonical_mapper.py  — shared outcome/line logic + Odibets / Betika / B2B
  sp_mapper.py         — Sportpesa market ID tables (one per sport)

New code should import directly from the specific module:

  # Preferred
  from app.workers.sp_mapper        import normalize_sp_market
  from app.workers.canonical_mapper import normalize_outcome, normalize_line

  # Also works (backwards-compat)
  from app.workers.market_mapper import normalize_sp_market, normalize_outcome
"""

from app.workers.canonical_mapper import (   # noqa: F401
    normalize_line,
    slug_with_line,
    normalize_outcome,
    normalize_od_market,
    normalize_bt_market,
    normalize_b2b_market,
    normalize_mozzartbet_market,
    normalize_betin_market,
    get_normalizer,
)

from app.workers.sp_mapper import (          # noqa: F401
    normalize_sp_market,
    list_all_slugs,
)


# ── Compatibility aliases for old code that used these names ──────────────────
def is_line_market(mkt_id: int, sport_id: int = 1) -> bool:
    """
    Return True if this market ID uses a line suffix for the given sport.
    Equivalent to looking up the (base, uses_line) tuple in the sport table.
    """
    from app.workers.sp_mapper import _GENERIC
    # table = get_sport_table(sport_id)
    entry = table.get(mkt_id) or _GENERIC.get(mkt_id)
    return bool(entry and entry[1])


def known_market_ids(sport_id: int = 1) -> list[int]:
    """Return all market IDs known for a given sport."""
    return list(get_sport_table(sport_id).keys())