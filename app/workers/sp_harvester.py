"""
app/workers/sp_harvester.py
============================
Standalone Sportpesa Kenya harvester.  No Celery dependency.

KEY FIXES (v3)
──────────────
1. _CORE_MARKET_IDS expanded — now includes all IDs seen in real SP traffic:
     41  First Team to Score
     42  HT 1X2  (alias of 60)
     44  HT/FT
     53  Asian HC - Half Time
     54  HT Over/Under  (alias of 15/68)
    203  HT Correct Score
    207  Highest Scoring Half
    328  HT BTTS

2. specValue=0 falsy bug fixed:
     BEFORE:  spec_val = mkt.get("specValue") or mkt.get("spec") or ...
              → integer 0 treated as falsy, spec_val became None
     AFTER:   spec_val = mkt.get("specValue")
              if spec_val is None: spec_val = mkt.get("spec") or mkt.get("handicap")
              → preserves 0 correctly

3. _fetch_markets now also tries integer key (int(gid)) for Shape-A responses
   where the game_id key is returned as an integer rather than a string.

O/U note
─────────
Over/Under (market ID 52) IS requested for every match. If it shows "—" in
the UI it means the SP API genuinely doesn't offer O/U for that match
(common for smaller local leagues). For major competitions (Champions League,
La Liga, SPL) O/U will be present and display correctly.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from app.workers.market_mapper import normalize_sp_market, normalize_outcome

# ── Constants ─────────────────────────────────────────────────────────────────

_BASE = "https://www.ke.sportpesa.com"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Mobile) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Timezone":   "Africa/Nairobi",
    "Origin":           "https://www.ke.sportpesa.com",
    "Referer":          "https://www.ke.sportpesa.com/",
}

# ── Market IDs — ALL confirmed from real SP traffic ───────────────────────────
# Requesting by explicit ID list (not "all") because "all" sometimes returns
# an empty set. IDs 41,42,44,53,54,203,207,328 were missing before — now added.
_CORE_MARKET_IDS = (
    # Full-time
    "10,1,"          # 1X2 (two IDs for same market)
    "46,47,"         # Double Chance, Draw No Bet
    "43,29,386,"     # BTTS, BTTS+Result
    "52,18,"         # O/U Goals (multiple lines via specValue)
    "353,352,"       # Home / Away team goals O/U
    "208,"           # Result + O/U
    "258,202,"       # Exact Goals, Goal Groups
    "332,"           # Correct Score
    "51,"            # Asian HC - Full Time
    "53,"            # Asian HC - Half Time        ← NEW
    "55,"            # European HC
    "45,"            # Odd/Even
    "41,"            # First Team to Score          ← NEW
    "207,"           # Highest Scoring Half         ← NEW
    # Half-time
    "42,60,"         # HT 1X2
    "15,54,68,"      # HT O/U (54 = alias)         ← 54 NEW
    "44,"            # HT/FT                        ← NEW
    "328,"           # HT BTTS                      ← NEW
    "203,"           # HT Correct Score             ← NEW
    # Set-piece / discipline
    "162,166,"       # Total Corners
    "136,139,"       # Total Bookings
    # Non-football
    "382,"           # Basketball Moneyline
    "99,100"         # Total Points, Point Spread
).replace("\n", "").replace(" ", "")

SP_SPORT_ID: dict[str, str] = {
    "soccer":          "1",
    "football":        "1",
    "basketball":      "2",
    "tennis":          "5",
    "ice-hockey":      "4",
    "volleyball":      "23",
    "cricket":         "21",
    "rugby":           "12",
    "boxing":          "10",
    "handball":        "6",
    "mma":             "117",
    "table-tennis":    "16",
    "esoccer":         "126",
    "efootball":       "126",
    "e-football":      "126",
}


# =============================================================================
# HTTP
# =============================================================================

def _get(path: str, params: dict | None = None, timeout: int = 15) -> Any:
    url = f"{_BASE}{path}"
    try:
        resp = requests.get(url, headers=_HEADERS, params=params,
                            timeout=timeout, allow_redirects=True)
        if resp.status_code == 304:
            return None
        if not resp.ok:
            print(f"[sp] HTTP {resp.status_code} → {url}")
            return None
        return resp.json()
    except Exception as exc:
        print(f"[sp] request error {url}: {exc}")
        return None


# =============================================================================
# Raw fetchers
# =============================================================================

def _fetch_upcoming_page(sport_id: str, ts_start: int, ts_end: int,
                          page_size: int, offset: int) -> list[dict]:
    raw = _get("/api/upcoming/games", params={
        "type":           "prematch",
        "sportId":        sport_id,
        "section":        "upcoming",
        "markets_layout": "multiple",
        "o":              "leagues",
        "pag_count":      str(page_size),
        "pag_min":        str(offset),
        "from":           str(ts_start),
        "to":             str(ts_end),
    })
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "games", "items", "results"):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def _fetch_live_list(sport_id: str) -> list[dict]:
    raw = _get("/api/live/games", params={"sportId": sport_id})
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "games", "items"):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def _fetch_markets(game_id: str) -> list[dict]:
    """
    Fetch the full market book for one SP game.

    Handles four response shapes:
      A  { "12345": [ {id, name, selections:[...]}, ... ] }   ← most common
      B  { "data": { "12345": [...] } }
      C  [ {id, name, selections:[...]} ]                     ← flat list
      D  { "markets": [...] }
    """
    raw = _get("/api/games/markets", params={
        "games":   game_id,
        "markets": _CORE_MARKET_IDS,
    })

    if raw is None:
        return []

    gid = str(game_id)

    if isinstance(raw, dict):
        # Shape A — string key
        if gid in raw and isinstance(raw[gid], list):
            return raw[gid]
        # Shape A — integer key (some SP responses use int)
        try:
            ikey = int(gid)
            if ikey in raw and isinstance(raw[ikey], list):
                return raw[ikey]
        except (ValueError, TypeError):
            pass
        # Shape B — nested under "data" or "result"
        data = raw.get("data") or raw.get("result") or {}
        if isinstance(data, dict):
            if gid in data and isinstance(data[gid], list):
                return data[gid]
            for v in data.values():
                if isinstance(v, list) and v:
                    return v
        # Shape D
        if isinstance(raw.get("markets"), list):
            return raw["markets"]
        # Last resort — return first list value
        for v in raw.values():
            if isinstance(v, list) and v:
                return v

    # Shape C — already a flat list
    if isinstance(raw, list):
        return raw

    return []


# =============================================================================
# Parsers
# =============================================================================

def _str_field(v: Any) -> str:
    if isinstance(v, dict):
        return str(v.get("name") or v.get("title") or v.get("short") or "")
    return str(v) if v else ""


def _parse_match_item(item: dict) -> dict | None:
    """Extract identity fields from a raw SP listing item."""
    sp_game_id  = str(item.get("id") or "")
    betradar_id = str(item.get("betradarId") or item.get("betradar_id") or "")
    if not sp_game_id:
        return None

    comps = item.get("competitors") or item.get("teams") or []
    if isinstance(comps, list) and len(comps) >= 2:
        home = _str_field(comps[0].get("name") or comps[0])
        away = _str_field(comps[1].get("name") or comps[1])
    else:
        home = _str_field(item.get("home") or item.get("homeName") or "")
        away = _str_field(item.get("away") or item.get("awayName") or "")

    competition = _str_field(
        item.get("league") or item.get("competition") or
        item.get("leagueName") or item.get("competitionName") or ""
    )
    sport = _str_field(item.get("sport") or item.get("sportName") or "")

    start_time = None
    if ts := item.get("dateTimestamp") or item.get("startTimestamp"):
        try:
            ts_int = int(ts)
            if ts_int > 1_000_000_000_000:   # milliseconds → seconds
                ts_int //= 1000
            start_time = datetime.utcfromtimestamp(ts_int).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
        except Exception:
            pass
    elif dt := item.get("date") or item.get("startTime"):
        start_time = str(dt)

    inline = (item.get("markets") or item.get("odds") or [])
    if isinstance(inline, dict):
        inline = list(inline.values())

    return {
        "betradar_id":  betradar_id,
        "sp_game_id":   sp_game_id,
        "home_team":    home,
        "away_team":    away,
        "start_time":   start_time,
        "competition":  competition,
        "sport":        sport,
        "_inline_mkts": inline,
    }


def _parse_markets(raw_list: list[dict]) -> dict[str, dict[str, float]]:
    """
    Convert SP market list → {canonical_slug: {outcome_key: float}}.

    BUG FIX: specValue=0 was previously lost because of:
        spec_val = mkt.get("specValue") or ...   ← 0 is falsy in Python!
    Now uses explicit `is None` check to preserve integer 0.
    """
    markets: dict[str, dict[str, float]] = {}

    for mkt in raw_list:
        if not isinstance(mkt, dict):
            continue

        mkt_id = mkt.get("id") or mkt.get("marketId") or mkt.get("typeId")
        if not mkt_id:
            continue

        try:
            mkt_id = int(mkt_id)
        except (TypeError, ValueError):
            continue

        # ── specValue: use `is None` so integer 0 is preserved ───────────────
        spec_val = mkt.get("specValue")
        if spec_val is None:
            spec_val = mkt.get("spec")
        if spec_val is None:
            spec_val = mkt.get("handicap")
        # ─────────────────────────────────────────────────────────────────────

        mkt_key = normalize_sp_market(mkt_id, spec_val)
        markets.setdefault(mkt_key, {})

        sels = mkt.get("selections") or mkt.get("outcomes") or mkt.get("odds") or []
        if isinstance(sels, dict):
            sels = list(sels.values())

        for sel in sels:
            if not isinstance(sel, dict):
                continue

            short = str(
                sel.get("shortName") or sel.get("name") or
                sel.get("label") or sel.get("outcome") or ""
            )
            try:
                price = float(
                    sel.get("odds") or sel.get("price") or sel.get("value") or 0
                )
            except (TypeError, ValueError):
                price = 0.0

            if price <= 1.0:
                continue

            out_key = normalize_outcome(mkt_key, short)
            if out_key not in markets[mkt_key] or price > markets[mkt_key][out_key]:
                markets[mkt_key][out_key] = round(price, 3)

    return {k: v for k, v in markets.items() if v}


def _build_match(parsed: dict, markets: dict, sport_slug: str,
                  status: str = "upcoming") -> dict:
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "betradar_id":  parsed["betradar_id"],
        "sp_game_id":   parsed["sp_game_id"],
        "home_team":    parsed["home_team"],
        "away_team":    parsed["away_team"],
        "start_time":   parsed["start_time"],
        "competition":  parsed["competition"],
        "sport":        parsed["sport"] or sport_slug,
        "source":       "sportpesa",
        "status":       status,
        "markets":      markets,
        "market_count": len(markets),
        "harvested_at": now_iso,
    }


# =============================================================================
# Public API
# =============================================================================

def fetch_upcoming(
    sport_slug: str,
    days: int = 5,
    pages_per_day: int = 5,
    page_size: int = 30,
    fetch_full_markets: bool = True,
    max_matches: int | None = None,
    timeout: int = 15,
) -> list[dict]:
    """
    Fetch upcoming matches for a sport.  Returns canonical match dicts.

    fetch_full_markets=True (default) hits /api/games/markets per match.
    This is REQUIRED to get 1X2 and all other markets — the listing endpoint
    only returns a subset (usually O/U + BTTS) inline.

    O/U note: If over_under_goals shows as "—" in the UI it means the SP API
    genuinely doesn't offer O/U for those matches (common for smaller leagues).
    """
    sport_id = SP_SPORT_ID.get(sport_slug.lower())
    if not sport_id:
        print(f"[sp] unknown sport: '{sport_slug}'")
        return []

    raw_items: list[dict] = []
    seen_ids:  set[str]   = set()
    now_eat = datetime.now(timezone.utc) + timedelta(hours=3)   # EAT = UTC+3

    for day_off in range(days):
        day   = now_eat + timedelta(days=day_off)
        ts_s  = int(day.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        ts_e  = ts_s + 86400
        offset = 0

        for _ in range(pages_per_day):
            items = _fetch_upcoming_page(sport_id, ts_s, ts_e, page_size, offset)
            if not items:
                break
            added = 0
            for it in items:
                gid = str(it.get("id") or "")
                if gid and gid not in seen_ids:
                    seen_ids.add(gid)
                    raw_items.append(it)
                    added += 1
            if added < page_size:
                break
            offset += page_size

    print(f"[sp] {sport_slug}: {len(raw_items)} raw matches")

    results = []
    targets = raw_items[:max_matches] if max_matches else raw_items

    for item in targets:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"])
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
                print(f"[sp] fallback to inline: {parsed['sp_game_id']} "
                      f"({parsed['home_team']} v {parsed['away_team']})")
            time.sleep(0.05)   # polite rate limit
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts)
        if not markets:
            print(f"[sp] no markets for {parsed['sp_game_id']} "
                  f"({parsed['home_team']} v {parsed['away_team']})")

        results.append(_build_match(parsed, markets, sport_slug))

    print(f"[sp] {sport_slug}: {len(results)} normalised (full_markets={fetch_full_markets})")
    return results


def fetch_live(
    sport_slug: str,
    fetch_full_markets: bool = True,
    timeout: int = 15,
) -> list[dict]:
    """Fetch live matches for a sport."""
    sport_id = SP_SPORT_ID.get(sport_slug.lower())
    if not sport_id:
        return []

    raw_items = _fetch_live_list(sport_id)
    print(f"[sp:live] {sport_slug}: {len(raw_items)} live items")

    results = []
    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"])
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
            time.sleep(0.05)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts)
        results.append(_build_match(parsed, markets, sport_slug, status="live"))

    return results


def fetch_match_markets(game_id: str) -> dict:
    """Fetch complete market book for one SP game (canonical slugs)."""
    return _parse_markets(_fetch_markets(game_id))


def fetch_sport_ids() -> dict[str, int]:
    """Get live sport IDs and event counts from SP."""
    raw = _get("/api/live/sports")
    if not isinstance(raw, dict):
        return {}
    sports = raw.get("sports") or raw.get("data") or []
    if isinstance(sports, list):
        return {s.get("name", ""): s.get("eventNumber", 0) for s in sports}
    return {}