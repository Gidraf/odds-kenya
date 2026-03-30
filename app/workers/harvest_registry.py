# """
# app/workers/harvest_registry.py
# ================================
# Bookmaker plugin registry.

# Each bookmaker is a dict with:
#   slug        → Redis key prefix + DB bookmaker field
#   label       → human name
#   sports      → list of sport slugs to harvest
#   fetch_fn    → callable(sport_slug) → list[dict]  (SpMatch format)
#   enabled     → bool (easy toggle without code change)
#   interval_h  → harvest interval in hours

# Adding a new bookmaker = add one entry to BOOKMAKERS.
# The Celery task loop reads this registry — no other changes needed.

# fetch_fn contract:
#   - Returns list of SpMatch dicts (same shape as sp_harvester output)
#   - Each match MUST have: home_team, away_team, start_time, markets
#   - markets: {slug: {outcome_key: float}}  (canonical format)
#   - May have: betradar_id, competition, sp_game_id / bt_game_id / etc.
#   - Should NOT raise — catch internally, return [] on failure
# """

# from __future__ import annotations

# import os
# from typing import Callable

# # ── Sport slugs each bookmaker covers ────────────────────────────────────────

# _SOCCER_SPORTS = ["soccer", "esoccer"]
# _MAIN_SPORTS   = [
#     "soccer", "basketball", "tennis", "volleyball",
#     "handball", "rugby", "cricket", "table-tennis",
# ]
# _ALL_SPORTS    = _MAIN_SPORTS + ["esoccer", "ice-hockey", "boxing", "mma", "darts",
#                                    "american-football", "baseball"]


# # ── Sportpesa ─────────────────────────────────────────────────────────────────

# def _sp_fetch(sport_slug: str) -> list[dict]:
#     try:
#         from app.workers.sp_harvester import fetch_upcoming
#         return fetch_upcoming(sport_slug, days=3, max_matches=300,
#                               fetch_full_markets=True, sleep_between=0.2)
#     except Exception as exc:
#         print(f"[harvest:sp:{sport_slug}] fetch failed: {exc}")
#         return []


# # ── Betika ────────────────────────────────────────────────────────────────────

# def _betika_fetch(sport_slug: str) -> list[dict]:
#     try:
#         from app.workers.bt_harvester import fetch_upcoming_matches as bt_fetch
#         return bt_fetch(sport_slug, days=3, max_matches=300)
#     except Exception as exc:
#         print(f"[harvest:betika:{sport_slug}] fetch failed: {exc}")
#         return []


# # ── Odibets ───────────────────────────────────────────────────────────────────

# def _odibets_fetch(sport_slug: str) -> list[dict]:
#     try:
#         from app.workers.odibets_harvester import fetch_upcoming as od_fetch
#         return od_fetch(sport_slug, days=3, max_matches=300)
#     except Exception as exc:
#         print(f"[harvest:odibets:{sport_slug}] fetch failed: {exc}")
#         return []


# # ── 1xBet / Helabet / 22Bet (B2B family, same API base) ─────────────────────

# def _b2b_fetch_factory(slug: str) -> Callable[[str], list[dict]]:
#     def _fetch(sport_slug: str) -> list[dict]:
#         try:
#             from app.workers.b2b_harvester import fetch_upcoming as b2b_fetch
#             return b2b_fetch(slug, sport_slug, days=3, max_matches=300)
#         except Exception as exc:
#             print(f"[harvest:{slug}:{sport_slug}] fetch failed: {exc}")
#             return []
#     return _fetch


# # ── Betin ─────────────────────────────────────────────────────────────────────

# def _betin_fetch(sport_slug: str) -> list[dict]:
#     try:
#         from app.workers.betin_harvester import fetch_upcoming as betin_fetch
#         return betin_fetch(sport_slug, days=3, max_matches=300)
#     except Exception as exc:
#         print(f"[harvest:betin:{sport_slug}] fetch failed: {exc}")
#         return []


# # ── MozzartBet ────────────────────────────────────────────────────────────────

# def _mozzart_fetch(sport_slug: str) -> list[dict]:
#     try:
#         from app.workers.mozzartbet_harvester import fetch_upcoming as mz_fetch
#         return mz_fetch(sport_slug, days=3, max_matches=300)
#     except Exception as exc:
#         print(f"[harvest:mozzartbet:{sport_slug}] fetch failed: {exc}")
#         return []


# # ══════════════════════════════════════════════════════════════════════════════
# # REGISTRY
# # ══════════════════════════════════════════════════════════════════════════════

# BOOKMAKERS: list[dict] = [
#     {
#         "slug":       "sportpesa",
#         "label":      "Sportpesa 🇰🇪",
#         "sports":     _ALL_SPORTS,
#         "fetch_fn":   _sp_fetch,
#         "enabled":    True,
#         "interval_h": 4,
#         "redis_ttl":  14_400,   # 4 hours in seconds
#     },
#     {
#         "slug":       "betika",
#         "label":      "Betika 🇰🇪",
#         "sports":     _MAIN_SPORTS,
#         "fetch_fn":   _betika_fetch,
#         "enabled":    bool(int(os.getenv("HARVEST_BETIKA", "1"))),
#         "interval_h": 4,
#         "redis_ttl":  14_400,
#     },
#     {
#         "slug":       "odibets",
#         "label":      "Odibets 🇰🇪",
#         "sports":     _MAIN_SPORTS,
#         "fetch_fn":   _odibets_fetch,
#         "enabled":    bool(int(os.getenv("HARVEST_ODIBETS", "1"))),
#         "interval_h": 4,
#         "redis_ttl":  14_400,
#     },
#     {
#         "slug":       "1xbet",
#         "label":      "1xBet",
#         "sports":     _MAIN_SPORTS,
#         "fetch_fn":   _b2b_fetch_factory("1xbet"),
#         "enabled":    bool(int(os.getenv("HARVEST_1XBET", "1"))),
#         "interval_h": 4,
#         "redis_ttl":  14_400,
#     },
#     {
#         "slug":       "helabet",
#         "label":      "Helabet 🇰🇪",
#         "sports":     _MAIN_SPORTS,
#         "fetch_fn":   _b2b_fetch_factory("helabet"),
#         "enabled":    bool(int(os.getenv("HARVEST_HELABET", "1"))),
#         "interval_h": 6,
#         "redis_ttl":  21_600,
#     },
#     {
#         "slug":       "betin",
#         "label":      "Betin 🇰🇪",
#         "sports":     _SOCCER_SPORTS,
#         "fetch_fn":   _betin_fetch,
#         "enabled":    bool(int(os.getenv("HARVEST_BETIN", "1"))),
#         "interval_h": 6,
#         "redis_ttl":  21_600,
#     },
#     {
#         "slug":       "mozzartbet",
#         "label":      "MozzartBet 🇰🇪",
#         "sports":     _SOCCER_SPORTS,
#         "fetch_fn":   _mozzart_fetch,
#         "enabled":    bool(int(os.getenv("HARVEST_MOZZART", "1"))),
#         "interval_h": 6,
#         "redis_ttl":  21_600,
#     },
# ]

# # Quick lookup
# BOOKMAKER_BY_SLUG: dict[str, dict] = {b["slug"]: b for b in BOOKMAKERS}
# ENABLED_BOOKMAKERS = [b for b in BOOKMAKERS if b["enabled"]]


# def get_bookmaker(slug: str) -> dict | None:
#     return BOOKMAKER_BY_SLUG.get(slug)


# def get_enabled_sports() -> list[str]:
#     """All unique sport slugs across all enabled bookmakers."""
#     sports: set[str] = set()
#     for b in BOOKMAKERS:
#         sports.update(b["sports"])
#     return sorted(sports)

"""
app/workers/harvest_registry.py
================================
Bookmaker plugin registry.

Each bookmaker is a dict with:
  slug        → Redis key prefix + DB bookmaker field
  label       → human name
  sports      → list of sport slugs to harvest
  fetch_fn    → callable(sport_slug) → list[dict]  (SpMatch format)
  enabled     → bool (easy toggle without code change)
  interval_h  → harvest interval in hours

Adding a new bookmaker = add one entry to BOOKMAKERS.
The Celery task loop reads this registry — no other changes needed.

fetch_fn contract:
  - Returns list of SpMatch dicts (same shape as sp_harvester output)
  - Each match MUST have: home_team, away_team, start_time, markets
  - markets: {slug: {outcome_key: float}}  (canonical format)
  - May have: betradar_id, competition, sp_game_id / bt_game_id / etc.
  - Should NOT raise — catch internally, return [] on failure
"""

from __future__ import annotations

import os
from typing import Callable

# ── Sport slugs each bookmaker covers ────────────────────────────────────────

_SOCCER_SPORTS = ["soccer", "esoccer"]
_MAIN_SPORTS   = [
    "soccer", "basketball", "tennis", "volleyball",
    "handball", "rugby", "cricket", "table-tennis",
]
_ALL_SPORTS    = _MAIN_SPORTS + ["esoccer", "ice-hockey", "boxing", "mma", "darts",
                                   "american-football", "baseball"]


# ── Sportpesa ─────────────────────────────────────────────────────────────────

def _sp_fetch(sport_slug: str) -> list[dict]:
    try:
        from app.workers.sp_harvester import fetch_upcoming
        return fetch_upcoming(sport_slug, days=3, max_matches=300,
                              fetch_full_markets=True, sleep_between=0.2)
    except Exception as exc:
        print(f"[harvest:sp:{sport_slug}] fetch failed: {exc}")
        return []


# ── Betika ────────────────────────────────────────────────────────────────────

def _betika_fetch(sport_slug: str) -> list[dict]:
    try:
        from app.workers.betika_harvester import fetch_upcoming as bt_fetch
        return bt_fetch(sport_slug, days=3, max_matches=300)
    except Exception as exc:
        print(f"[harvest:betika:{sport_slug}] fetch failed: {exc}")
        return []


# ── Odibets ───────────────────────────────────────────────────────────────────

def _odibets_fetch(sport_slug: str) -> list[dict]:
    try:
        from app.workers.od_harvester import fetch_upcoming as od_fetch
        return od_fetch(sport_slug)
    except Exception as exc:
        print(f"[harvest:odibets:{sport_slug}] fetch failed: {exc}")
        return []


# ── 1xBet / Helabet / 22Bet (B2B family, same API base) ─────────────────────

def _b2b_fetch_factory(slug: str) -> Callable[[str], list[dict]]:
    def _fetch(sport_slug: str) -> list[dict]:
        try:
            from app.workers.b2b_harvester import fetch_upcoming as b2b_fetch
            return b2b_fetch(slug, sport_slug, days=3, max_matches=300)
        except Exception as exc:
            print(f"[harvest:{slug}:{sport_slug}] fetch failed: {exc}")
            return []
    return _fetch


# ── Betin ─────────────────────────────────────────────────────────────────────

def _betin_fetch(sport_slug: str) -> list[dict]:
    try:
        from app.workers.betin_harvester import fetch_upcoming as betin_fetch
        return betin_fetch(sport_slug, days=3, max_matches=300)
    except Exception as exc:
        print(f"[harvest:betin:{sport_slug}] fetch failed: {exc}")
        return []


# ── MozzartBet ────────────────────────────────────────────────────────────────

def _mozzart_fetch(sport_slug: str) -> list[dict]:
    try:
        from app.workers.mozzartbet_harvester import fetch_upcoming as mz_fetch
        return mz_fetch(sport_slug, days=3, max_matches=300)
    except Exception as exc:
        print(f"[harvest:mozzartbet:{sport_slug}] fetch failed: {exc}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# REGISTRY
# ══════════════════════════════════════════════════════════════════════════════

BOOKMAKERS: list[dict] = [
    {
        "slug":       "sportpesa",
        "label":      "Sportpesa 🇰🇪",
        "sports":     _ALL_SPORTS,
        "fetch_fn":   _sp_fetch,
        "enabled":    True,
        "interval_h": 4,
        "redis_ttl":  14_400,   # 4 hours in seconds
    },
    {
        "slug":       "betika",
        "label":      "Betika 🇰🇪",
        "sports":     _MAIN_SPORTS,
        "fetch_fn":   _betika_fetch,
        "enabled":    bool(int(os.getenv("HARVEST_BETIKA", "1"))),
        "interval_h": 4,
        "redis_ttl":  14_400,
    },
    {
        "slug":       "odibets",
        "label":      "Odibets 🇰🇪",
        "sports":     _MAIN_SPORTS,
        "fetch_fn":   _odibets_fetch,
        "enabled":    bool(int(os.getenv("HARVEST_ODIBETS", "1"))),
        "interval_h": 4,
        "redis_ttl":  14_400,
    },
    {
        "slug":       "1xbet",
        "label":      "1xBet",
        "sports":     _MAIN_SPORTS,
        "fetch_fn":   _b2b_fetch_factory("1xbet"),
        "enabled":    bool(int(os.getenv("HARVEST_1XBET", "1"))),
        "interval_h": 4,
        "redis_ttl":  14_400,
    },
    {
        "slug":       "helabet",
        "label":      "Helabet 🇰🇪",
        "sports":     _MAIN_SPORTS,
        "fetch_fn":   _b2b_fetch_factory("helabet"),
        "enabled":    bool(int(os.getenv("HARVEST_HELABET", "1"))),
        "interval_h": 6,
        "redis_ttl":  21_600,
    },
    {
        "slug":       "betin",
        "label":      "Betin 🇰🇪",
        "sports":     _SOCCER_SPORTS,
        "fetch_fn":   _betin_fetch,
        "enabled":    bool(int(os.getenv("HARVEST_BETIN", "1"))),
        "interval_h": 6,
        "redis_ttl":  21_600,
    },
    {
        "slug":       "mozzartbet",
        "label":      "MozzartBet 🇰🇪",
        "sports":     _SOCCER_SPORTS,
        "fetch_fn":   _mozzart_fetch,
        "enabled":    bool(int(os.getenv("HARVEST_MOZZART", "1"))),
        "interval_h": 6,
        "redis_ttl":  21_600,
    },
]

# Quick lookup
BOOKMAKER_BY_SLUG: dict[str, dict] = {b["slug"]: b for b in BOOKMAKERS}
ENABLED_BOOKMAKERS = [b for b in BOOKMAKERS if b["enabled"]]


def get_bookmaker(slug: str) -> dict | None:
    return BOOKMAKER_BY_SLUG.get(slug)


def get_enabled_sports() -> list[str]:
    """All unique sport slugs across all enabled bookmakers."""
    sports: set[str] = set()
    for b in ENABLED_BOOKMAKERS:
        sports.update(b["sports"])
    return sorted(sports)


def register_bookmaker(plugin) -> None:
    """
    Register a plugin-style bookmaker (OdiBetsHarvesterPlugin, etc.).
    Idempotent — duplicate slugs are ignored.

    Plugin interface:
      .bookie_id    → str  (unique slug, e.g. "odibets")
      .bookie_name  → str
      .sport_slugs  → list[str]
      .fetch_upcoming(sport_slug, **kwargs) → list[dict]
    """
    slug = getattr(plugin, "bookie_id", None) or getattr(plugin, "slug", None)
    if not slug or slug in BOOKMAKER_BY_SLUG:
        return
    entry: dict = {
        "slug":       slug,
        "label":      getattr(plugin, "bookie_name", slug),
        "sports":     getattr(plugin, "sport_slugs", _MAIN_SPORTS),
        "fetch_fn":   plugin.fetch_upcoming,
        "enabled":    True,
        "interval_h": 4,
        "redis_ttl":  14_400,
    }
    BOOKMAKERS.append(entry)
    BOOKMAKER_BY_SLUG[slug] = entry
    ENABLED_BOOKMAKERS.clear()
    ENABLED_BOOKMAKERS.extend(b for b in BOOKMAKERS if b.get("enabled"))