"""
app/workers/sp_harvester.py
============================
Standalone Sportpesa Kenya harvester.  No Celery dependency — call directly
or wrap in a Celery task (see harvest_tasks.py).

Market keys are canonical slugs from market_seeds.py via market_mapper.

Endpoints used
--------------
  Upcoming list : GET /api/upcoming/games
  Markets       : GET /api/games/markets?games={id}&markets=all
  Live list     : GET /api/live/games

Normalised match shape (output)
--------------------------------
{
    "betradar_id":  str,
    "sp_game_id":   str,
    "home_team":    str,
    "away_team":    str,
    "start_time":   str | None,
    "competition":  str,
    "sport":        str,
    "source":       "sportpesa",
    "markets": {
        "1x2":                   {"1": 2.07, "X": 2.95, "2": 3.50},
        "btts":                  {"yes": 2.16, "no": 1.57},
        "over_under_goals_2.5":  {"over": 2.60, "under": 1.44},
        "double_chance":         {"1X": 1.20, "X2": 1.47, "12": 1.26},
        "draw_no_bet":           {"1": 1.48, "2": 2.47},
        "european_handicap_1":   {"1": 1.24, "X": 4.50, "2": 8.80},
        "asian_handicap_-0.25":  {"1": 1.75, "2": 1.89},
        "correct_score":         {"1:0": 4.60, "0:0": 5.20, ...},
        "exact_goals":           {"1": 3.10, "2": 2.90, ...},
        ...
    },
    "market_count": int,
    "harvested_at": str,
}
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from app.workers.market_mapper import normalize_sp_market, normalize_outcome

# ── Sportpesa base URL + headers ─────────────────────────────────────────────

_BASE = "https://www.ke.sportpesa.com"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Mobile) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":              "application/json, text/plain, */*",
    "Accept-Language":     "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With":    "XMLHttpRequest",
    "X-App-Timezone":      "Africa/Nairobi",
    "Origin":              "https://www.ke.sportpesa.com",
    "Referer":             "https://www.ke.sportpesa.com/",
}

# ── Sport slug → Sportpesa sportId ───────────────────────────────────────────

SP_SPORT_ID: dict[str, str] = {
    "soccer":       "1",
    "football":     "1",
    "basketball":   "2",
    "tennis":       "5",
    "ice-hockey":   "4",
    "volleyball":   "23",
    "cricket":      "21",
    "rugby":        "12",
    "boxing":       "10",
    "handball":     "6",
    "mma":          "117",
    "table-tennis": "16",
    "esoccer":      "126",
}


# =============================================================================
# HTTP helpers
# =============================================================================

def _get(path: str, params: dict | None = None, timeout: int = 15) -> Any:
    url = f"{_BASE}{path}"
    try:
        resp = requests.get(url, headers=_HEADERS, params=params, timeout=timeout)
        if not resp.ok:
            print(f"[sp] HTTP {resp.status_code} -> {url}")
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
    if not raw or not isinstance(raw, list):
        return []
    return raw


def _fetch_live_list(sport_id: str) -> list[dict]:
    raw = _get("/api/live/games", params={"sportId": sport_id})
    if not raw or not isinstance(raw, list):
        return []
    return raw


def _fetch_markets(game_id: str) -> list[dict]:
    raw = _get("/api/games/markets", params={"games": game_id, "markets": "all"})
    if not isinstance(raw, dict):
        return []
    return raw.get(str(game_id), [])


# =============================================================================
# Parsers
# =============================================================================

def _parse_match_item(item: dict) -> dict | None:
    betradar_id = str(item.get("betradarId") or "")
    sp_game_id  = str(item.get("id") or "")
    if not sp_game_id:
        return None

    comps = item.get("competitors") or []
    home  = comps[0].get("name", "") if len(comps) > 0 else ""
    away  = comps[1].get("name", "") if len(comps) > 1 else ""

    def _str_field(v: Any) -> str:
        if isinstance(v, dict):
            return str(v.get("name") or v.get("title") or "")
        return str(v) if v else ""

    competition = _str_field(item.get("league") or item.get("competition") or "")
    sport       = _str_field(item.get("sport") or "")

    start_time = None
    if ts := item.get("dateTimestamp"):
        try:
            start_time = datetime.utcfromtimestamp(int(ts) / 1000).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            )
        except Exception:
            pass
    elif dt := item.get("date"):
        start_time = str(dt)

    return {
        "betradar_id":  betradar_id,
        "sp_game_id":   sp_game_id,
        "home_team":    home,
        "away_team":    away,
        "start_time":   start_time,
        "competition":  competition,
        "sport":        sport,
        "_inline_mkts": item.get("markets") or [],
    }


def _parse_markets(raw_list: list[dict]) -> dict[str, dict[str, float]]:
    """
    Convert SP market list → {canonical_slug: {outcome: float}}.
    Uses market_mapper.normalize_sp_market + normalize_outcome.
    """
    markets: dict[str, dict[str, float]] = {}

    for mkt in raw_list:
        mkt_id   = int(mkt.get("id") or 0)
        spec_val = mkt.get("specValue")

        # Skip markets with no id
        if not mkt_id:
            continue

        mkt_key = normalize_sp_market(mkt_id, spec_val)

        if mkt_key not in markets:
            markets[mkt_key] = {}

        for sel in (mkt.get("selections") or []):
            short = str(sel.get("shortName") or sel.get("name") or "")
            try:
                price = float(sel.get("odds") or 0)
            except (TypeError, ValueError):
                price = 0.0
            if price <= 1.0:
                continue

            out_key = normalize_outcome(mkt_key, short)
            if out_key not in markets[mkt_key] or price > markets[mkt_key][out_key]:
                markets[mkt_key][out_key] = price

    return {k: v for k, v in markets.items() if v}


# =============================================================================
# Public API
# =============================================================================

def fetch_upcoming(
    sport_slug: str,
    days: int = 5,
    pages_per_day: int = 4,
    page_size: int = 30,
    fetch_full_markets: bool = True,
    max_matches: int | None = None,
    timeout: int = 15,
) -> list[dict]:
    sport_id = SP_SPORT_ID.get(sport_slug.lower())
    if not sport_id:
        print(f"[sp] unknown sport slug: '{sport_slug}'")
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

    results = []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    targets = raw_items[:max_matches] if max_matches else raw_items

    for item in targets:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"])
            time.sleep(0.04)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts)

        results.append({
            "betradar_id":  parsed["betradar_id"],
            "sp_game_id":   parsed["sp_game_id"],
            "home_team":    parsed["home_team"],
            "away_team":    parsed["away_team"],
            "start_time":   parsed["start_time"],
            "competition":  parsed["competition"],
            "sport":        parsed["sport"] or sport_slug,
            "source":       "sportpesa",
            "markets":      markets,
            "market_count": len(markets),
            "harvested_at": now_iso,
        })

    print(f"[sp] {sport_slug}: {len(results)} normalised matches")
    return results


def fetch_live(
    sport_slug: str,
    fetch_full_markets: bool = True,
    timeout: int = 15,
) -> list[dict]:
    sport_id = SP_SPORT_ID.get(sport_slug.lower())
    if not sport_id:
        return []

    raw_items = _fetch_live_list(sport_id)
    print(f"[sp:live] {sport_slug}: {len(raw_items)} live")

    results = []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"])
            time.sleep(0.04)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts)

        results.append({
            "betradar_id":  parsed["betradar_id"],
            "sp_game_id":   parsed["sp_game_id"],
            "home_team":    parsed["home_team"],
            "away_team":    parsed["away_team"],
            "start_time":   parsed["start_time"],
            "competition":  parsed["competition"],
            "sport":        parsed["sport"] or sport_slug,
            "source":       "sportpesa",
            "status":       "live",
            "markets":      markets,
            "market_count": len(markets),
            "harvested_at": now_iso,
        })

    return results


def fetch_sport_ids() -> dict[str, int]:
    raw = _get("/api/live/sports")
    if not isinstance(raw, dict):
        return {}
    return {s["name"]: s.get("eventNumber", 0) for s in (raw.get("sports") or [])}