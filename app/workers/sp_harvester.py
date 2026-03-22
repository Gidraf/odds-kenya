"""
app/workers/sp_harvester.py
============================
Sportpesa Kenya harvester — streaming-capable, fully logged.

Public API
──────────
  fetch_upcoming(sport_slug, ...)          → list[dict]   (blocking, all at once)
  fetch_upcoming_stream(sport_slug, ...)   → Generator    (yields one match at a time)
  fetch_live(sport_slug, ...)              → list[dict]
  fetch_live_stream(sport_slug, ...)       → Generator
  fetch_match_markets(game_id)             → dict

O/U diagnostic note
───────────────────
  If over_under_goals is missing despite market ID 52 being requested, check:
    GET /api/sp/match/<game_id>/markets
  This calls _fetch_markets() directly and returns the raw parsed result.
  Compare against: GET https://www.ke.sportpesa.com/api/games/markets?games=<id>&markets=52,18
  If 52 is absent from the SP response for that game, SP simply doesn't offer
  O/U for it (common for niche leagues).  If 52 IS present but parsed wrong,
  the logs will show "[sp:mkt] <game_id> raw keys: ..." to help trace the issue.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Generator

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

# ── Market IDs ────────────────────────────────────────────────────────────────
# 52 / 18 = O/U Goals.  If these don't appear in parsed output, add
# ?markets=52,18 to a manual curl against /api/games/markets?games=<id>
# to confirm whether SP is returning them for that game.
_CORE_MARKET_IDS = (
    "10,1,"          # 1X2
    "46,47,"         # Double Chance, Draw No Bet
    "43,29,386,"     # BTTS, BTTS+Result
    "52,18,"         # O/U Goals (multiple lines via specValue)  ← key market
    "353,352,"       # Home / Away team goals O/U
    "208,"           # Result + O/U
    "258,202,"       # Exact Goals, Goal Groups
    "332,"           # Correct Score
    "51,53,"         # Asian HC - FT, HT
    "55,"            # European HC
    "45,"            # Odd/Even
    "41,"            # First Team to Score
    "207,"           # Highest Scoring Half
    "42,60,"         # HT 1X2
    "15,54,68,"      # HT O/U
    "44,"            # HT/FT
    "328,"           # HT BTTS
    "203,"           # HT Correct Score
    "162,166,"       # Total Corners
    "136,139,"       # Total Bookings
    "382,"           # Basketball Moneyline
    "99,100"         # Total Points, Point Spread
).replace("\n", "").replace(" ", "")

SP_SPORT_ID: dict[str, str] = {
    "soccer": "1", "football": "1",
    "basketball": "2", "tennis": "5", "ice-hockey": "4",
    "volleyball": "23", "cricket": "21", "rugby": "12",
    "boxing": "10", "handball": "6", "table-tennis": "16",
    "mma": "117", "esoccer": "126", "efootball": "126",
    "e-football": "126",
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
        "type": "prematch", "sportId": sport_id, "section": "upcoming",
        "markets_layout": "multiple", "o": "leagues",
        "pag_count": str(page_size), "pag_min": str(offset),
        "from": str(ts_start), "to": str(ts_end),
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


def _fetch_markets(game_id: str, debug: bool = False) -> list[dict]:
    """
    Fetch full market book for one SP game.

    debug=True logs the raw top-level keys and which shape was matched —
    useful for diagnosing missing O/U markets.

    Response shapes handled:
      A  { "12345": [...] }            ← most common (string or int key)
      B  { "data": { "12345": [...] }}
      C  [ {id, selections}, ... ]     ← flat list
      D  { "markets": [...] }
    """
    raw = _get("/api/games/markets", params={
        "games":   game_id,
        "markets": _CORE_MARKET_IDS,
    })

    if raw is None:
        print(f"[sp:mkt] {game_id} → None response from SP API")
        return []

    gid = str(game_id)

    if isinstance(raw, dict):
        if debug:
            top_keys = list(raw.keys())[:8]
            print(f"[sp:mkt] {game_id} raw dict keys (first 8): {top_keys}")

        # Shape A — string key
        if gid in raw and isinstance(raw[gid], list):
            result = raw[gid]
            if debug:
                mkt_ids = [m.get("id") for m in result]
                print(f"[sp:mkt] {game_id} shape-A str: {len(result)} mkts, ids={mkt_ids[:10]}")
            return result

        # Shape A — int key
        try:
            ikey = int(gid)
            if ikey in raw and isinstance(raw[ikey], list):
                result = raw[ikey]
                if debug:
                    mkt_ids = [m.get("id") for m in result]
                    print(f"[sp:mkt] {game_id} shape-A int: {len(result)} mkts, ids={mkt_ids[:10]}")
                return result
        except (ValueError, TypeError):
            pass

        # Shape B
        for wrapper in ("data", "result"):
            data = raw.get(wrapper)
            if isinstance(data, dict):
                if gid in data and isinstance(data[gid], list):
                    result = data[gid]
                    if debug:
                        print(f"[sp:mkt] {game_id} shape-B: {len(result)} mkts")
                    return result
                for v in data.values():
                    if isinstance(v, list) and v:
                        if debug:
                            print(f"[sp:mkt] {game_id} shape-B fallback: {len(v)} mkts")
                        return v

        # Shape D
        if isinstance(raw.get("markets"), list):
            result = raw["markets"]
            if debug:
                print(f"[sp:mkt] {game_id} shape-D: {len(result)} mkts")
            return result

        # Last resort
        for v in raw.values():
            if isinstance(v, list) and v:
                if debug:
                    print(f"[sp:mkt] {game_id} shape-last-resort: {len(v)} mkts")
                return v

        if debug:
            print(f"[sp:mkt] {game_id} no list found in dict response")

    elif isinstance(raw, list):
        if debug:
            print(f"[sp:mkt] {game_id} shape-C flat list: {len(raw)} mkts")
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
    sp_game_id = str(item.get("id") or "")
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
            if ts_int > 1_000_000_000_000:
                ts_int //= 1000
            start_time = datetime.utcfromtimestamp(ts_int).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except Exception:
            pass
    elif dt := item.get("date") or item.get("startTime"):
        start_time = str(dt)

    inline = item.get("markets") or item.get("odds") or []
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


def _parse_markets(raw_list: list[dict], game_id: str = "") -> dict[str, dict[str, float]]:
    """
    Convert SP market list → {canonical_slug: {outcome_key: float}}.

    specValue fix: uses explicit `is None` check so integer 0 is preserved.
    """
    markets: dict[str, dict[str, float]] = {}
    ou_seen = False  # diagnostic flag for missing O/U

    for mkt in raw_list:
        if not isinstance(mkt, dict):
            continue

        mkt_id = mkt.get("id") or mkt.get("marketId") or mkt.get("typeId")
        if mkt_id is None:
            continue

        try:
            mkt_id = int(mkt_id)
        except (TypeError, ValueError):
            continue

        # Track if O/U (52 / 18) is present in raw list
        if mkt_id in (52, 18):
            ou_seen = True

        # ── specValue: explicit None check preserves integer 0 ────────────────
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
                sel.get("label")    or sel.get("outcome") or ""
            )
            try:
                price = float(sel.get("odds") or sel.get("price") or sel.get("value") or 0)
            except (TypeError, ValueError):
                price = 0.0
            if price <= 1.0:
                continue

            out_key = normalize_outcome(mkt_key, short)
            if out_key not in markets[mkt_key] or price > markets[mkt_key][out_key]:
                markets[mkt_key][out_key] = round(price, 3)

    result = {k: v for k, v in markets.items() if v}

    # ── Diagnostic: warn when O/U absent ─────────────────────────────────────
    if game_id and not ou_seen:
        print(f"[sp:ou] {game_id}: market IDs 52/18 NOT in raw_list ({len(raw_list)} mkts total) "
              f"— SP API did not return O/U for this game")
    elif game_id and ou_seen:
        ou_keys = [k for k in result if k.startswith("over_under_goals")]
        if not ou_keys:
            print(f"[sp:ou] {game_id}: IDs 52/18 present but no 'over_under_goals' after parse — "
                  f"check outcome shortNames and price filter")
        else:
            print(f"[sp:ou] {game_id}: O/U OK → {ou_keys}")

    return result


def _build_match(parsed: dict, markets: dict, sport_slug: str,
                  status: str = "upcoming") -> dict:
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
        "harvested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# =============================================================================
# Raw item collector (shared by streaming + blocking paths)
# =============================================================================

def _collect_raw_items(
    sport_id:   str,
    days:       int,
    pages_per_day: int,
    page_size:  int,
    max_matches: int | None,
) -> list[dict]:
    raw_items: list[dict] = []
    seen_ids:  set[str]   = set()
    now_eat = datetime.now(timezone.utc) + timedelta(hours=3)

    for day_off in range(days):
        day  = now_eat + timedelta(days=day_off)
        ts_s = int(day.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        ts_e = ts_s + 86400
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

        if max_matches and len(raw_items) >= max_matches:
            break

    return raw_items


# =============================================================================
# Public API — STREAMING GENERATORS
# =============================================================================

def fetch_upcoming_stream(
    sport_slug:         str,
    days:               int   = 3,
    pages_per_day:      int   = 5,
    page_size:          int   = 30,
    fetch_full_markets: bool  = True,
    max_matches:        int | None = None,
    sleep_between:      float = 0.05,
    debug_ou:           bool  = False,
) -> Generator[dict, None, None]:
    """
    Generator version of fetch_upcoming — yields each normalised match dict
    as soon as it is processed rather than waiting for all matches.

    Use this for streaming endpoints so the client sees results immediately
    instead of waiting for the full harvest to complete.

    Yields dicts in the same shape as fetch_upcoming() list items.
    """
    sport_id = SP_SPORT_ID.get(sport_slug.lower())
    if not sport_id:
        print(f"[sp:stream] unknown sport: '{sport_slug}'")
        return

    raw_items = _collect_raw_items(sport_id, days, pages_per_day, page_size, max_matches)
    targets   = raw_items[:max_matches] if max_matches else raw_items

    print(f"[sp:stream] {sport_slug}: {len(targets)} items to process")

    for item in targets:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"], debug=debug_ou)
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts, game_id=parsed["sp_game_id"] if debug_ou else "")
        yield _build_match(parsed, markets, sport_slug)


def fetch_live_stream(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.05,
    debug_ou:           bool  = False,
) -> Generator[dict, None, None]:
    """Generator version of fetch_live."""
    sport_id = SP_SPORT_ID.get(sport_slug.lower())
    if not sport_id:
        return

    raw_items = _fetch_live_list(sport_id)
    print(f"[sp:stream:live] {sport_slug}: {len(raw_items)} items")

    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"], debug=debug_ou)
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts, game_id=parsed["sp_game_id"] if debug_ou else "")
        yield _build_match(parsed, markets, sport_slug, status="live")


# =============================================================================
# Public API — BLOCKING (wraps streaming generators)
# =============================================================================

def fetch_upcoming(
    sport_slug:         str,
    days:               int   = 3,
    pages_per_day:      int   = 5,
    page_size:          int   = 30,
    fetch_full_markets: bool  = True,
    max_matches:        int | None = None,
    timeout:            int   = 15,
) -> list[dict]:
    """
    Blocking version — collects all streaming results into a list.
    Signature-compatible with the old harvester.
    """
    results = list(fetch_upcoming_stream(
        sport_slug,
        days=days,
        pages_per_day=pages_per_day,
        page_size=page_size,
        fetch_full_markets=fetch_full_markets,
        max_matches=max_matches,
        debug_ou=True,   # always log O/U diagnostics
    ))
    print(f"[sp] {sport_slug}: {len(results)} normalised (full_mkts={fetch_full_markets})")
    return results


def fetch_live(
    sport_slug:         str,
    fetch_full_markets: bool = True,
    timeout:            int  = 15,
) -> list[dict]:
    results = list(fetch_live_stream(
        sport_slug,
        fetch_full_markets=fetch_full_markets,
        debug_ou=True,
    ))
    print(f"[sp:live] {sport_slug}: {len(results)} live")
    return results


def fetch_match_markets(game_id: str) -> dict[str, dict[str, float]]:
    """
    Full market book for one SP game.
    Runs with debug=True so O/U parsing is logged.
    """
    raw = _fetch_markets(game_id, debug=True)
    result = _parse_markets(raw, game_id=game_id)
    print(f"[sp:match] {game_id}: {len(result)} market slugs → {list(result.keys())[:10]}")
    return result


def fetch_sport_ids() -> dict[str, int]:
    raw = _get("/api/live/sports")
    if not isinstance(raw, dict):
        return {}
    sports = raw.get("sports") or raw.get("data") or []
    if isinstance(sports, list):
        return {s.get("name", ""): s.get("eventNumber", 0) for s in sports}
    return {}