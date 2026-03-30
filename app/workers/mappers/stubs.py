"""
app/workers/mappers/stubs.py
=============================
Stub market normalisers for bookmakers whose API schemas haven't been
fully reverse-engineered yet.

Each stub uses a generic factory that:
  1. Tries a sub_type_id lookup in an (empty) id_map
  2. Tries an exact name match in an (empty) name_map
  3. Sanitises the raw name as a fallback slug

When MozzartBet or Betin is fully mapped, replace the corresponding
empty dicts with populated tables (same pattern as betika.py / odibets.py).

Exports
-------
  normalize_mozzartbet_market(name, sub_type_id=None, specifiers="")
  normalize_betin_market(name, sub_type_id=None, specifiers="")
"""

from __future__ import annotations

import re
from typing import Any


# =============================================================================
# GENERIC NORMALISER FACTORY
# =============================================================================

def _make_normalizer(
    id_map:   dict[str, tuple[str, bool]],
    name_map: dict[str, str],
    prefix:   str,
):
    """
    Build a normaliser function from lookup tables.

    Parameters
    ----------
    id_map:   {str(sub_type_id): (base_slug, uses_line)}
    name_map: {UPPER_CASE_NAME: canonical_slug}
    prefix:   fallback prefix for unknown IDs e.g. "mz" or "betin"
    """
    from app.workers.mappers.shared import slug_with_line

    def _fn(
        name:         str,
        sub_type_id:  Any    = None,
        specifiers:   str    = "",
    ) -> str:
        # 1. sub_type_id lookup
        if sub_type_id is not None:
            entry = id_map.get(str(sub_type_id).strip())
            if entry:
                base, uses_line = entry
                if uses_line and specifiers:
                    return slug_with_line(base, specifiers)
                return base
        # 2. Exact name match
        upper = name.strip().upper()
        if upper in name_map:
            return name_map[upper]
        # 3. Sanitise fallback
        slug = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
        return slug or f"{prefix}_unknown"

    _fn.__name__ = f"normalize_{prefix}_market"
    return _fn


# =============================================================================
# MOZZARTBET
# =============================================================================
# TODO: populate these tables once MozzartBet's API schema is mapped.
# Pattern: same as odibets.py — add rows to _MZ_ID and _MZ_NAME.

_MZ_ID:   dict[str, tuple[str, bool]] = {}
_MZ_NAME: dict[str, str]              = {}

normalize_mozzartbet_market = _make_normalizer(_MZ_ID, _MZ_NAME, "mz")


# =============================================================================
# BETIN
# =============================================================================
# TODO: populate these tables once Betin's API schema is mapped.

_BETIN_ID:   dict[str, tuple[str, bool]] = {}
_BETIN_NAME: dict[str, str]              = {}

normalize_betin_market = _make_normalizer(_BETIN_ID, _BETIN_NAME, "betin")


# =============================================================================
# ADDING A NEW STUB BOOKMAKER
# =============================================================================
# 1. Define empty _XYZ_ID and _XYZ_NAME dicts above
# 2. Call _make_normalizer(_XYZ_ID, _XYZ_NAME, "xyz")
# 3. Register in mappers/__init__.py get_normalizer()
# 4. Fill in the dicts once the API is analysed