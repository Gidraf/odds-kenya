"""
app/workers/mappers/__init__.py
================================
Package entry point.

Imports all normaliser functions and exposes the `get_normalizer` registry.
New code should import directly from the specific submodule:

  from app.workers.mappers.shared  import normalize_outcome, normalize_line, slug_with_line
  from app.workers.mappers.betika  import normalize_bt_market
  from app.workers.mappers.odibets import normalize_od_market
  from app.workers.mappers.b2b     import normalize_b2b_market
  from app.workers.mappers.stubs   import normalize_mozzartbet_market, normalize_betin_market

Legacy code can also import from `canonical_mapper` (unchanged shim) or here:

  from app.workers.mappers import get_normalizer, normalize_bt_market
"""

from __future__ import annotations

from typing import Callable

# ── Re-export everything from submodules ──────────────────────────────────────
from app.workers.mappers.shared import (    # noqa: F401
    normalize_line,
    slug_with_line,
    normalize_outcome,
)
# from app.workers.mappers.betika import (    # noqa: F401
#     normalize_bt_market,
# )
from app.workers.mappers.odibets import (   # noqa: F401
    normalize_od_market,
)
from app.workers.mappers.b2b import (       # noqa: F401
    normalize_b2b_market,
)
from app.workers.mappers.stubs import (     # noqa: F401
    normalize_mozzartbet_market,
    normalize_betin_market,
)


# =============================================================================
# NORMALISER REGISTRY
# =============================================================================

def get_normalizer(source_name: str) -> Callable | None:
    """
    Return the normaliser callable for a bookmaker source name, or None.

    Usage
    -----
    >>> fn = get_normalizer("betika")
    >>> fn("TOTAL GOALS", 18, "2.5")
    'over_under_goals_2.5'

    Supported source names (case-insensitive, spaces/hyphens stripped)
    ------------------------------------------------------------------
    sportpesa, betika, odibets, b2b, 1xbet, 22bet, helabet, paripesa,
    melbet, betwinner, megapari, mozzartbet, betin
    """
    # Normalise the source name for lookup
    key = source_name.lower().replace(" ", "").replace("-", "").replace("_", "")

    # SP normaliser imported lazily to avoid circular import at package load
    def _sp_fn(name: str, sid: Any, spec: str = "") -> str:
        from app.workers.sp_mapper import normalize_sp_market   # noqa: PLC0415
        return normalize_sp_market(int(sid or 0), spec or None, 1)

    registry: dict[str, Callable] = {
        # ── Sportpesa ─────────────────────────────────────────────────────────
        "sportpesa":       _sp_fn,

        # ── Betika (live + upcoming share same mapper) ─────────────────────────
        "betika":          lambda name, sid, spec="": normalize_bt_market(name, sid),
        "betikaupdoming":  lambda name, sid, spec="": normalize_bt_market(name, sid),
        "betikalive":      lambda name, sid, spec="": normalize_bt_market(name, sid),

        # ── Odibets ────────────────────────────────────────────────────────────
        "odibets":         lambda name, sid, spec="": normalize_od_market(sid, spec),

        # ── B2B family ────────────────────────────────────────────────────────
        "b2b":             lambda name, sid, spec="": normalize_b2b_market(name),
        "1xbet":           lambda name, sid, spec="": normalize_b2b_market(name),
        "22bet":           lambda name, sid, spec="": normalize_b2b_market(name),
        "helabet":         lambda name, sid, spec="": normalize_b2b_market(name),
        "paripesa":        lambda name, sid, spec="": normalize_b2b_market(name),
        "melbet":          lambda name, sid, spec="": normalize_b2b_market(name),
        "betwinner":       lambda name, sid, spec="": normalize_b2b_market(name),
        "megapari":        lambda name, sid, spec="": normalize_b2b_market(name),

        # ── Stubs ─────────────────────────────────────────────────────────────
        "mozzartbet":      lambda name, sid, spec="": normalize_mozzartbet_market(name, sid, spec),
        "betin":           lambda name, sid, spec="": normalize_betin_market(name, sid, spec),
    }

    return registry.get(key)


# Type alias used internally
from typing import Any  # noqa: E402 (must be after registry def)