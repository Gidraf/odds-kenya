"""
app/workers/sp_harvester.py
============================
Standalone Sportpesa Kenya harvester.  No Celery dependency.

KEY FIX (v2)
────────────
The SP listing endpoint  (/api/upcoming/games)  returns inline markets that
usually contain ONLY Over/Under and BTTS — NOT 1X2.  To get 1X2 + all other
markets you MUST hit  /api/games/markets?games=<id>&markets=<ids>.

_fetch_markets() now handles four different response shapes seen in the wild:
  Shape A: { "12345": [ {id,selections,...}, ... ] }   ← most common
  Shape B: { "data": { "12345": [...] } }              ← some responses
  Shape C: [ {id, selections, ...} ]                   ← flat list
  Shape D: { "markets": [...] }                        ← older endpoint

Market IDs requested explicitly (not "all") because "all" sometimes returns
an empty set — requesting by ID is more reliable.

Normalised match shape (output)
─────────────────────────────────
{
    "betradar_id":  str,
    "sp_game_id":   str,
    "home_team":    str,
    "away_team":    str,
    "start_time":   str | None,   # ISO-8601 UTC
    "competition":  str,
    "sport":        str,
    "source":       "sportpesa",
    "markets": {
        "1x2":                   {"1": 2.07, "X": 2.95,  "2": 3.50},
        "btts":                  {"yes": 2.16, "no": 1.57},
        "over_under_goals_2.5":  {"over": 1.90, "under": 1.80},
        "double_chance":         {"1X": 1.20, "X2": 1.47, "12": 1.26},
        "draw_no_bet":           {"1": 1.48, "2": 2.47},
        ...
    },
    "market_count":  int,
    "harvested_at":  str,
}
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

# Core market IDs to request explicitly  (reliable; "all" sometimes returns nothing)
# Ordered: 1X2 first so it always appears as the first market
_CORE_MARKET_IDS = "10,1,46,47,43,29,52,18,208,258,202,332,353,352,386,51,55,45,60,15,68,162,166,136,139,382,99,100"

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
        # Some SP API versions wrap the list
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
      A  { "12345": [ {id, name, selections:[...]}, ... ] }
      B  { "data": { "12345": [...] } }
      C  [ {id, name, selections:[...]} ]          ← flat list
      D  { "markets": [...] }
    """
    raw = _get("/api/games/markets", params={
        "games":   game_id,
        "markets": _CORE_MARKET_IDS,
    })

    if raw is None:
        return []

    gid = str(game_id)

    # Shape A — most common
    if isinstance(raw, dict):
        # Try exact key first
        if gid in raw and isinstance(raw[gid], list):
            return raw[gid]
        # Try int key
        if int(gid) in raw:
            v = raw[int(gid)]
            if isinstance(v, list):
                return v
        # Shape B — nested under "data"
        data = raw.get("data") or raw.get("result") or {}
        if isinstance(data, dict):
            if gid in data and isinstance(data[gid], list):
                return data[gid]
        # Shape D — flat {"markets": [...]}
        if isinstance(raw.get("markets"), list):
            return raw["markets"]
        # Last resort: return any top-level list value
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
            # SP timestamps are in milliseconds when > 1e10
            if ts_int > 1_000_000_000_000:
                ts_int //= 1000
            start_time = datetime.utcfromtimestamp(ts_int).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
        except Exception:
            pass
    elif dt := item.get("date") or item.get("startTime"):
        start_time = str(dt)

    # Inline markets from the listing (usually O/U + BTTS only — no 1X2)
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

    Handles two selection shapes:
      Standard  {"shortName": "1", "odds": 2.07}
      Extended  {"shortName": "OV", "odds": 1.90, "specValue": 2.5}
    """
    markets: dict[str, dict[str, float]] = {}

    for mkt in raw_list:
        if not isinstance(mkt, dict):
            continue

        mkt_id   = mkt.get("id") or mkt.get("marketId") or mkt.get("typeId")
        spec_val = mkt.get("specValue") or mkt.get("spec") or mkt.get("handicap")

        if not mkt_id:
            continue

        try:
            mkt_id = int(mkt_id)
        except (TypeError, ValueError):
            continue

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
                price = float(sel.get("odds") or sel.get("price") or sel.get("value") or 0)
            except (TypeError, ValueError):
                price = 0.0

            if price <= 1.0:
                continue

            out_key = normalize_outcome(mkt_key, short)
            # Keep highest price if same outcome appears twice (different lines collapsed)
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

    fetch_full_markets=True (default) hits /api/games/markets per match to get
    1X2 + all markets — without this 1X2 will be empty for most matches.
    """
    sport_id = SP_SPORT_ID.get(sport_slug.lower())
    if not sport_id:
        print(f"[sp] unknown sport: '{sport_slug}'")
        return []

    raw_items: list[dict] = []
    seen_ids:  set[str]   = set()
    now_eat = datetime.now(timezone.utc) + timedelta(hours=3)

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

    results  = []
    targets  = raw_items[:max_matches] if max_matches else raw_items

    for item in targets:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"])
            if not raw_mkts:
                # Fallback to inline markets if the markets endpoint fails
                raw_mkts = parsed["_inline_mkts"]
            time.sleep(0.05)   # polite rate limit
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts)
        if not markets:
            # Still include the match — no markets yet is not an error
            print(f"[sp] no markets for game_id={parsed['sp_game_id']} "
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
    """
    Fetch complete market book for one specific SP game.
    Returns the full markets dict (canonical slugs).
    """
    raw_mkts = _fetch_markets(game_id)
    return _parse_markets(raw_mkts)


def fetch_sport_ids() -> dict[str, int]:
    """Get live sport IDs and event counts from SP."""
    raw = _get("/api/live/sports")
    if not isinstance(raw, dict):
        return {}
    sports = raw.get("sports") or raw.get("data") or []
    if isinstance(sports, list):
        return {s.get("name", ""): s.get("eventNumber", 0) for s in sports}
    return {}