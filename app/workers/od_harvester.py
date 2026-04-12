"""
app/workers/od_harvester.py
============================
OdiBets upcoming & live harvester (Universal Multi-Sport Mapper).
"""

from __future__ import annotations

import json
import logging
import time
import re
from typing import Any, Generator

import httpx

logger = logging.getLogger(__name__)

CANONICAL_SPORT_IDS = {
    "soccer": 1, "basketball": 2, "tennis": 3, "cricket": 4, "rugby": 5,
    "ice-hockey": 6, "volleyball": 7, "handball": 8, "table-tennis": 9,
    "baseball": 10, "american-football": 11, "mma": 15, "boxing": 16, "darts": 17,
}

OD_SPORT_MAP = {
    "soccer": "1", "basketball": "2", "tennis": "3", "ice-hockey": "4",
    "volleyball": "5", "cricket": "6", "rugby": "7", "american-football": "8",
    "baseball": "9", "handball": "10", "table-tennis": "11", "mma": "12",
    "boxing": "13", "darts": "14"
}

def slug_to_od_sport_id(slug: str) -> int:
    return int(OD_SPORT_MAP.get(slug, "1"))

def od_sport_to_slug(od_id: int | str) -> str:
    rev = {v: k for k, v in OD_SPORT_MAP.items()}
    return rev.get(str(od_id), "soccer")

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "origin": "https://odibets.com",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

def _get(url: str, params: dict | None = None, timeout: float = 8.0) -> dict | None:
    for attempt in range(3):
        try:
            r = httpx.get(url, params=params, headers=HEADERS, timeout=timeout)
            if r.status_code >= 500: return None 
            if r.is_success: return r.json()
        except httpx.RequestError: pass
        time.sleep(0.5)
    return None

# ══════════════════════════════════════════════════════════════════════════════
# 🟢 UNIVERSAL MULTI-SPORT MAPPER
# ══════════════════════════════════════════════════════════════════════════════

def normalize_universal_outcome(display: str) -> str:
    """Forces all bookmakers to use the exact same outcome keys for perfect UI merging."""
    d = str(display).upper().strip()
    
    # 1X2 Standard
    if d in ("HOME", "1", "1.0", "TEAM 1"): return "1"
    if d in ("DRAW", "X", "X.0", "TIE"): return "X"
    if d in ("AWAY", "2", "2.0", "TEAM 2"): return "2"
    
    # Double Chance Standardization (Fixes OdiBets 1__X)
    if d in ("1 OR X", "1__X", "1X", "HOME OR DRAW"): return "1X"
    if d in ("1 OR 2", "1__2", "12", "HOME OR AWAY"): return "12"
    if d in ("X OR 2", "X__2", "X2", "DRAW OR AWAY"): return "X2"
    
    # Binary Standards
    if "YES" in d: return "Yes"
    if "NO" in d: return "No"
    if "OVER" in d: return "Over"
    if "UNDER" in d: return "Under"
    if "ODD" in d: return "Odd"
    if "EVEN" in d: return "Even"
    
    return d.title()

def get_universal_market_slug(name: str) -> str:
    """Forces all bookmakers to output identical market slugs across all 13 sports."""
    n = name.lower()
    
    if "winner" in n or "match result" in n or "moneyline" in n or "1x2" in n:
        if "1st half" in n: return "first_half_match_winner"
        return "match_winner"
        
    if "double chance" in n: return "double_chance"
    if "both teams to score" in n or "btts" in n: return "btts"
    if "draw no bet" in n: return "draw_no_bet"
        
    if "over" in n or "under" in n or "total" in n:
        match = re.search(r"([\d.]+)", n)
        if match:
            clean_line = str(float(match.group(1))).replace(".", "_")
            return f"over_under_{clean_line}"
        return "over_under"

    if "handicap" in n:
        match = re.search(r"([+-]?[\d.]+)", n)
        if match:
            clean = str(float(match.group(1))).replace(".", "_").replace("+", "plus_").replace("-", "minus_")
            if "asian" in n: return f"asian_handicap_{clean}"
            return f"european_handicap_{clean}"
            
    return re.sub(r"[^a-z0-9]+", "_", n).strip("_")

def _parse_markets(raw_mkts: list[dict] | dict) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    
    # OdiBets sometimes returns a dict of markets or a list
    iterable = raw_mkts.values() if isinstance(raw_mkts, dict) else raw_mkts
    
    for mkt in iterable:
        name = mkt.get("name", mkt.get("market_name", ""))
        odds_raw = mkt.get("odds", mkt.get("outcomes", {}))
        
        # Handle dict or list outcomes
        odds_iterable = odds_raw.values() if isinstance(odds_raw, dict) else odds_raw

        for o in odds_iterable:
            try: val = float(o.get("odd") or o.get("price") or 0)
            except: continue
            if val <= 1.0: continue

            # Clean name with specifier if it exists
            specifier = o.get("specifier", "")
            full_name = f"{name} {specifier}" if specifier else name

            slug = get_universal_market_slug(full_name)
            outcome_key = normalize_universal_outcome(o.get("name", o.get("outcome_name", "")))

            if slug not in result: result[slug] = {}
            result[slug][outcome_key] = val

    return result

# 🟢 THE CRITICAL FUNCTION FOR PROGRESSIVE HYDRATION
def fetch_event_detail(betradar_id: str | int, sport_id: int) -> tuple[dict[str, dict[str, float]], dict]:
    """
    Fetches full live/upcoming markets directly from OdiBets event endpoint.
    Called explicitly by the unified stream endpoint in routes_debug.py
    """
    url = f"https://odibets.com/api/front/event/{betradar_id}"
    data = _get(url)
    if not data or not data.get("data"):
        return {}, {}
    
    event_data = data["data"]
    markets = _parse_markets(event_data.get("markets", []))
    return markets, event_data

def _normalise_match(raw: dict, *, source: str = "upcoming") -> dict | None:
    try:
        match_id = str(raw.get("event_id") or "")
        betradar_id = str(raw.get("sportradar_id") or raw.get("provider_id") or match_id)
        home = str(raw.get("home_team") or raw.get("competitor1") or "").strip()
        away = str(raw.get("away_team") or raw.get("competitor2") or "").strip()
        
        markets = _parse_markets(raw.get("markets", []) or raw.get("market", []))
        
        return {
            "od_match_id": match_id,
            "od_parent_id": betradar_id,
            "betradar_id": betradar_id,
            "home_team": home,
            "away_team": away,
            "sport": od_sport_to_slug(raw.get("sport_id", "1")),
            "markets": markets,
            "market_count": len(markets),
            "is_live": source == "live"
        }
    except Exception as exc:
        logger.debug("OD match normalise error: %s", exc)
        return None

def fetch_live_matches(sport_slug: str | None = None) -> list[dict]:
    od_sport_id = slug_to_od_sport_id(sport_slug) if sport_slug else "1"
    url = "https://odibets.com/api/front/events"
    params = {"tab": "live", "sport_id": str(od_sport_id), "limit": 1000}
    data = _get(url, params=params, timeout=6.0)
    
    results = []
    if data and data.get("data") and data["data"].get("events"):
        for r in data["data"]["events"]:
            n = _normalise_match(r, source="live")
            if n:
                results.append(n)
    return results

class OdibetsHarvesterPlugin:
    bookie_id   = "odibets"
    bookie_name = "OdiBets"
    sport_slugs = list(CANONICAL_SPORT_IDS.keys())
    
    def fetch_upcoming(self, sport_slug: str, max_pages: int = 10, fetch_full: bool = False) -> list[dict]:
        return [] 
        
    def fetch_live(self, sport_slug: str | None = None) -> list[dict]:
        return fetch_live_matches(sport_slug)

# 🟢 EXPLICIT EXPORT - Guarantees routes_debug.py can import it!
__all__ = ["fetch_event_detail", "fetch_live_matches", "slug_to_od_sport_id", "od_sport_to_slug"]