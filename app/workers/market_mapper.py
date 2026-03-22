"""
app/workers/market_mapper.py
=============================
Backwards-compatibility shim.

All code that previously imported from market_mapper will continue to work
unchanged because this module re-exports everything from the two split files:

  canonical_mapper.py  — shared outcome/line logic + Odibets / Betika / B2B
  sp_mapper.py         — Sportpesa-specific market ID table

New code should import directly from the specific module:

  # Preferred
  from app.workers.sp_mapper       import normalize_sp_market
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
    is_line_market,
    known_market_ids,
)