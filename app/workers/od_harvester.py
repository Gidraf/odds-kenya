"""
app/workers/bt_harvester.py
============================
Betika upcoming & live harvester (Universal Multi-Sport Mapper).
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Generator

import httpx

logger = logging.getLogger(__name__)

# Standard Canonical IDs for downstream unified matching
CANONICAL_SPORT_IDS = {
    "soccer": 1, "basketball": 2, "tennis": 3, "cricket": 4, "rugby": 5,
    "ice-hockey": 6, "volleyball": 7, "handball": 8, "table-tennis": 9,
    "baseball": 10, "american-football": 11, "mma": 15, "boxing": 16, "darts": 17,
}

BT_SPORT_MAP = {
    "soccer": "14", "basketball": "13", "tennis": "15", "ice-hockey": "17",
    "volleyball": "91", "cricket": "21", "rugby": "22", "american-football": "23",
    "baseball": "24", "handball": "25", "table-tennis": "26", "mma": "29",
    "boxing": "30", "darts": "31"
}

def slug_to_bt_sport_id(slug: str) -> int:
    return int(BT_SPORT_MAP.get(slug, "14"))

def bt_sport_to_slug(bt_id: int | str) -> str:
    rev = {v: k for k, v in BT_SPORT_MAP.items()}
    return rev.get(str(bt_id), "soccer")

# ── Betika API endpoints ──────────────────────────────────────────────────────
LIVE_BASE  = "https://live.betika.com/v1/uo"
API_BASE   = "https://api.betika.com/v1/uo"

LIVE_SPORTS_URL    = f"{LIVE_BASE}/sports"
LIVE_MATCHES_URL   = f"{LIVE_BASE}/matches"
LIVE_MATCH_URL     = f"{LIVE_BASE}/match"      
UPCOMING_URL       = f"{API_BASE}/matches"
MATCH_MARKETS_URL  = f"{API_BASE}/match"       

HEADERS: dict[str, str] = {
    "accept":          "application/json, text/plain, */*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "origin":          "https://www.betika.com",
    "referer":         "https://www.betika.com/",
    "user-agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

# ── Redis key patterns ────────────────────────────────────────────────────────
_LIVE_DATA_KEY   = "bt:live:{sport_id}:data"
_LIVE_HASH_KEY   = "bt:live:{sport_id}:hash"
_LIVE_CHAN_KEY   = "bt:live:{sport_id}:updates"
_LIVE_SPORTS_KEY = "bt:live:sports"
_UPC_DATA_KEY    = "bt:upcoming:{sport_slug}:data"
_UPC_HASH_KEY    = "bt:upcoming:{sport_slug}:hash"
_UPC_CHAN_KEY    = "bt:upcoming:{sport_slug}:updates"


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
    
    if d in ("HOME", "1", "1.0", "TEAM 1"): return "1"
    if d in ("DRAW", "X", "X.0", "TIE"): return "X"
    if d in ("AWAY", "2", "2.0", "TEAM 2"): return "2"
    
    if d in ("1 OR X", "1__X", "1X", "HOME OR DRAW"): return "1X"
    if d in ("1 OR 2", "1__2", "12", "HOME OR AWAY"): return "12"
    if d in ("X OR 2", "X__2", "X2", "DRAW OR AWAY"): return "X2"
    
    if "YES" in d: return "Yes"
    if "NO" in d: return "No"
    if "OVER" in d: return "Over"
    if "UNDER" in d: return "Under"
    if "ODD" in d: return "Odd"
    if "EVEN" in d: return "Even"
    
    return d.title()

def get_universal_market_slug(sid: str, n: str, specs: dict) -> str:
    """Forces all bookmakers to output identical market slugs across all 13 sports."""
    n = n.lower()
    
    if sid in ("1", "10", "149", "112", "123") or "winner" in n or "match result" in n or "moneyline" in n or "1x2" in n:
        if "1st half" in n: return "first_half_match_winner"
        return "match_winner"
        
    if sid == "11" or "double chance" in n: return "double_chance"
    if sid == "29" or "both teams to score" in n or "btts" in n: return "btts"
    if sid == "186" or "draw no bet" in n: return "draw_no_bet"
        
    if "over" in n or "under" in n or "total" in n:
        line = specs.get("total") or specs.get("handicap")
        if not line:
            match = re.search(r"([\d.]+)", n)
            if match: line = match.group(1)
        if line:
            clean_line = str(float(line)).replace(".", "_")
            if "1st half" in n: return f"first_half_over_under_{clean_line}"
            return f"over_under_{clean_line}"
        return "over_under"

    if "handicap" in n or sid == "219":
        line = specs.get("handicap")
        if not line:
            match = re.search(r"([+-]?[\d.]+)", n)
            if match: line = match.group(1)
        if line:
            clean = str(float(line)).replace(".", "_").replace("+", "plus_").replace("-", "minus_")
            if "asian" in n: return f"asian_handicap_{clean}"
            return f"european_handicap_{clean}"
            
    if "1st half" in n and ("winner" in n or "1x2" in n):
        return "first_half_match_winner"
        
    return re.sub(r"[^a-z0-9]+", "_", n).strip("_")

def _parse_all_inline_markets(raw_mkts: list[dict], sport_slug: str) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}

    for mkt in raw_mkts:
        sid = str(mkt.get("sub_type_id", ""))
        name = mkt.get("name", "")
        odds_raw = mkt.get("odds") or []

        for o in odds_raw:
            try: val = float(o.get("odd_value") or 0)
            except: continue
            if val <= 1.0: continue

            parsed_specs = o.get("parsed_special_bet_value") or {}
            slug = get_universal_market_slug(sid, name, parsed_specs)
            outcome_key = normalize_universal_outcome(o.get("display", ""))

            if slug not in result: result[slug] = {}
            result[slug][outcome_key] = val

    return result

def _normalise_match(raw: dict, *, source: str = "upcoming") -> dict | None:
    try:
        bt_sport_id   = str(raw.get("sport_id") or "14")
        match_id      = str(raw.get("match_id") or raw.get("game_id") or "")
        betradar_id   = str(raw.get("parent_match_id") or "")
        parent_id     = betradar_id or match_id
        home          = str(raw.get("home_team") or "").strip()
        away          = str(raw.get("away_team") or "").strip()
        competition   = str(raw.get("competition_name") or "").strip()
        category      = str(raw.get("category") or "").strip()
        start_time    = str(raw.get("start_time") or "")
        sport_name    = str(raw.get("sport_name") or "").strip()

        if not match_id: return None
        if start_time and " " in start_time: start_time = start_time.replace(" ", "T")

        sport_slug      = bt_sport_to_slug(bt_sport_id)
        can_sport_id    = CANONICAL_SPORT_IDS.get(sport_slug, 1)

        is_live       = source == "live"
        match_time    = str(raw.get("match_time") or "").strip()
        event_status  = str(raw.get("event_status") or "").strip()
        match_status  = str(raw.get("match_status") or "").strip()
        bet_status    = str(raw.get("bet_status") or "").strip()
        bet_stop      = str(raw.get("bet_stop_reason") or "").strip()
        current_score = str(raw.get("current_score") or "").strip()
        ht_score      = str(raw.get("ht_score") or "").strip()

        score_home = score_away = None
        if current_score and "-" in current_score:
            parts = current_score.split("-", 1)
            try:
                score_home = parts[0].strip()
                score_away = parts[1].strip()
            except IndexError: pass

        markets = _parse_all_inline_markets(raw.get("odds") or [], sport_slug)

        if "home_odd" in raw and not any(k.endswith("1x2") for k in markets):
            try:
                ho, no, ao = float(raw.get("home_odd") or 0), float(raw.get("neutral_odd") or 0), float(raw.get("away_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    base = f"{sport_slug}_1x2" if sport_slug != "soccer" else "match_winner"
                    markets[base] = {k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1}
            except: pass

        return {
            "bt_match_id":       match_id,
            "bt_parent_id":      parent_id,
            "sp_game_id":        None,
            "betradar_id":       betradar_id if betradar_id else None,
            "home_team":         home,
            "away_team":         away,
            "competition":       competition,
            "category":          category,
            "sport_name":        sport_name,
            "sport":             sport_slug,
            "bt_sport_id":       int(bt_sport_id),
            "canonical_sport_id": can_sport_id,
            "start_time":        start_time,
            "source":            "betika",
            "is_live":           is_live,
            "is_suspended":      bet_status in ("STOPPED", "BET_STOP"),
            "match_time":        match_time,
            "event_status":      event_status,
            "match_status":      match_status,
            "bet_status":        bet_status,
            "bet_stop_reason":   bet_stop,
            "current_score":     current_score,
            "score_home":        score_home,
            "score_away":        score_away,
            "ht_score":          ht_score,
            "markets":           markets,
            "market_count":      len(markets),
        }
    except Exception as exc:
        logger.debug("BT match normalise error: %s", exc)
        return None

# ══════════════════════════════════════════════════════════════════════════════
# FULL MARKETS  (on-demand via /v1/uo/match)
# ══════════════════════════════════════════════════════════════════════════════

def get_full_markets(parent_match_id: str | int, sport_slug: str) -> dict[str, dict[str, float]]:
    data = _get(MATCH_MARKETS_URL, params={"parent_match_id": str(parent_match_id)})
    raw_mkts = (data or {}).get("data") or []
    if not raw_mkts:
        data = _get(MATCH_MARKETS_URL, params={"match_id": str(parent_match_id)})
        raw_mkts = (data or {}).get("data") or []
    if not raw_mkts:
        live_data = _get(LIVE_MATCH_URL, params={"id": str(parent_match_id)})
        raw_mkts = (live_data or {}).get("data") or []
    return _parse_all_inline_markets(raw_mkts, sport_slug)

def get_live_match_markets(match_id: str | int, sport_slug: str) -> tuple[dict[str, dict[str, float]], dict]:
    data = _get(LIVE_MATCH_URL, params={"id": str(match_id)}, timeout=6.0)
    if not data: return {}, {}
    return _parse_all_inline_markets(data.get("data") or [], sport_slug), data.get("meta") or {}

def enrich_matches_with_full_markets(matches: list[dict], max_workers: int = 8) -> list[dict]:
    def _fetch(match: dict) -> dict:
        pid = match.get("bt_parent_id")
        if not pid: return match
        full = get_full_markets(pid, match.get("sport", "soccer"))
        if full:
            merged = {**match, "markets": {**match.get("markets", {}), **full}}
            merged["market_count"] = len(merged["markets"])
            return merged
        return match

    ordered: dict[int, dict] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_fetch, m): i for i, m in enumerate(matches)}
        for fut in as_completed(futs):
            idx = futs[fut]
            try: ordered[idx] = fut.result()
            except: ordered[idx] = matches[idx]
    return [ordered.get(i, matches[i]) for i in range(len(matches))]


# ══════════════════════════════════════════════════════════════════════════════
# STREAMING GENERATORS & LIST FEEDS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_stream(sport_slug: str = "soccer", max_matches: int | None = None, max_pages: int = 10, period_id: int = 9, fetch_full_markets: bool = True, sleep_between: float= 0.3, **kwargs) -> Generator[dict, None, None]:
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    count = 0
    for page in range(1, max_pages + 1):
        data = _get(UPCOMING_URL, params={"page": page, "limit": 50, "tab": "upcoming", "sport_id": bt_sport_id, "sort_id": 2, "period_id": period_id, "esports": "false"}, timeout=8.0)
        if not data or not data.get("data"): break
        for r in data["data"]:
            if max_matches and count >= max_matches: return
            norm = _normalise_match(r, source="upcoming")
            if not norm: continue
            if fetch_full_markets and norm.get("bt_parent_id"):
                full = get_full_markets(norm["bt_parent_id"], sport_slug)
                if full:
                    norm["markets"].update(full)
                    norm["market_count"] = len(norm["markets"])
                time.sleep(sleep_between)
            count += 1
            yield norm
        meta = data.get("meta") or {}
        if page * int(meta.get("limit") or 50) >= int(meta.get("total") or 0): break

def fetch_live_stream(sport_slug: str, **kwargs) -> Generator[dict, None, None]:
    yield from []

def fetch_live_sports() -> list[dict]:
    return (_get(LIVE_SPORTS_URL, timeout=5.0) or {}).get("data") or []

def fetch_live_matches(bt_sport_id: str | int | None = None) -> list[dict]:
    data = _get(LIVE_MATCHES_URL, params={"page": 1, "limit": 1000, "sport": str(bt_sport_id) if bt_sport_id is not None else "null", "sort": 1}, timeout=6.0)
    return [n for r in (data or {}).get("data", []) if (n := _normalise_match(r, source="live"))]

def fetch_upcoming_matches(sport_slug: str = "soccer", max_pages: int = 10, period_id: int = 9, fetch_full: bool = False, max_workers: int = 8) -> list[dict]:
    all_matches = []
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    for page in range(1, max_pages + 1):
        data = _get(UPCOMING_URL, params={"page": page, "limit": 50, "tab": "upcoming", "sport_id": bt_sport_id, "sort_id": 2, "period_id": period_id, "esports": "false"}, timeout=8.0)
        if not data or not data.get("data"): break
        for r in data["data"]:
            if n := _normalise_match(r, source="upcoming"): all_matches.append(n)
        meta = data.get("meta") or {}
        if page * int(meta.get("limit") or 50) >= int(meta.get("total") or 0): break
    if fetch_full and all_matches:
        all_matches = enrich_matches_with_full_markets(all_matches, max_workers=max_workers)
    return all_matches


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND LIVE POLLER & CACHING STUBS (For backward compatibility)
# ══════════════════════════════════════════════════════════════════════════════

def _payload_hash(matches: list[dict]) -> str:
    frags = [f"{m.get('bt_match_id')}:{m.get('bet_status')}:{m.get('current_score')}:{m.get('match_time')}:" + "|".join(f"{s}:{k}:{v}" for s, o in sorted((m.get("markets") or {}).items()) for k, v in sorted(o.items())) for m in matches]
    return hashlib.md5("\n".join(frags).encode()).hexdigest()

def cache_upcoming(redis_client: Any, sport_slug: str, matches: list[dict], ttl: int = 300) -> None:
    key = _UPC_DATA_KEY.format(sport_slug=sport_slug)
    redis_client.set(key, json.dumps(matches, ensure_ascii=False), ex=ttl)
    redis_client.set(_UPC_HASH_KEY.format(sport_slug=sport_slug), _payload_hash(matches), ex=ttl)
    redis_client.publish(_UPC_CHAN_KEY.format(sport_slug=sport_slug), json.dumps({"type": "upcoming_refresh", "sport": sport_slug, "count": len(matches), "ts": time.time()}))

def get_cached_upcoming(redis_client: Any, sport_slug: str) -> list[dict] | None:
    data = redis_client.get(_UPC_DATA_KEY.format(sport_slug=sport_slug))
    return json.loads(data) if data else None

def get_cached_live(redis_client: Any, bt_sport_id: int) -> list[dict] | None:
    data = redis_client.get(_LIVE_DATA_KEY.format(sport_id=bt_sport_id))
    return json.loads(data) if data else None

def get_cached_live_sports(redis_client: Any) -> list[dict] | None:
    data = redis_client.get(_LIVE_SPORTS_KEY)
    return json.loads(data).get("sports") or [] if data else None

def start_live_poller(redis_client: Any, interval: float = 1.5): pass
def get_live_poller(): return None

class BetikaHarvesterPlugin:
    bookie_id   = "betika"
    bookie_name = "Betika"
    sport_slugs = list(CANONICAL_SPORT_IDS.keys())
    def fetch_upcoming(self, sport_slug: str, max_pages: int = 10, fetch_full: bool = False) -> list[dict]:
        return fetch_upcoming_matches(sport_slug=sport_slug, max_pages=max_pages, fetch_full=fetch_full)
    def fetch_live(self, sport_slug: str | None = None) -> list[dict]:
        return fetch_live_matches(slug_to_bt_sport_id(sport_slug) if sport_slug else None)
    def get_live_sports(self) -> list[dict]:
        return fetch_live_sports()

# Ensure ALL functions used by external files (like routes_debug.py) are explicitly exported
__all__ = [
    "fetch_upcoming_matches", 
    "fetch_live_matches", 
    "fetch_upcoming_stream", 
    "fetch_live_stream", 
    "get_full_markets", 
    "get_live_match_markets", 
    "cache_upcoming", 
    "get_cached_upcoming",
    "get_cached_live", 
    "get_cached_live_sports", 
    "start_live_poller", 
    "get_live_poller",
    "slug_to_bt_sport_id",
    "bt_sport_to_slug"
]