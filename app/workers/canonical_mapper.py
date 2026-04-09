"""
app/workers/canonical_mapper.py  (v5 — shim only, zero logic)
==============================================================
All logic has moved to app/workers/mappers/:

  mappers/shared.py   — normalize_line, slug_with_line, normalize_outcome
  mappers/betika.py   — normalize_bt_market
  mappers/odibets.py  — normalize_od_market
  mappers/b2b.py      — normalize_b2b_market
  mappers/stubs.py    — normalize_mozzartbet_market, normalize_betin_market
  mappers/__init__.py — get_normalizer registry

This file re-exports everything so existing imports continue to work
without any changes to callers:

  # Still works (backwards-compat)
  from app.workers.canonical_mapper import normalize_bt_market, normalize_outcome

  # Preferred going forward — import directly from the specific module
  from app.workers.mappers.betika import normalize_bt_market
  from app.workers.mappers.shared import normalize_outcome
"""

from app.workers.mappers.shared import (    # noqa: F401
    normalize_line,
    slug_with_line,
    normalize_outcome,
    _OUT_EXACT,
    _SP_COMBO,
    _HTFT_MAP,
    _FSMW_MAP,
    _WINNING_MARGIN,
)
from app.workers.mappers.betika import (    # noqa: F401
    _BT_SUBTYPE,
    _BT_NAME,
    _BT_PATTERNS,
)
from app.workers.mappers.odibets import (   # noqa: F401
    normalize_od_market,
    _OD_MKT,
)
from app.workers.mappers.b2b import (       # noqa: F401
    normalize_b2b_market,
    _B2B_BASE_NAME,
)
from app.workers.mappers.stubs import (     # noqa: F401
    normalize_mozzartbet_market,
    normalize_betin_market,
)
from app.workers.mappers import (           # noqa: F401
    get_normalizer,
)