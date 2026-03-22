"""
app/workers/sp_harvester.py
============================
Sportpesa Kenya harvester — Football & eFootball focus.

Sports supported
────────────────
  soccer    / football  → sportId 1   (standard football)
  esoccer   / efootball → sportId 126 (virtual / eFootball)
  basketball            → sportId 2
  tennis                → sportId 5
  ice-hockey            → sportId 4
  volleyball            → sportId 23
  cricket               → sportId 21
  rugby                 → sportId 12
  boxing                → sportId 10
  handball              → sportId 6
  table-tennis          → sportId 16
  mma                   → sportId 117

Key design decisions
─────────────────────
1.  _CORE_MARKET_IDS — 33 IDs requested explicitly per-match.
    "all" is unreliable (sometimes returns nothing).
    Includes ALL football markets from the intercepted traffic:
      1X2 (10,1) · DC (46) · DNB (47) · BTTS (43,29,386)
      O/U goals (52,18) · Home/Away goals (353,352)
      Result+O/U (208) · Exact goals (258) · Goal groups (202)
      Correct score (332) · Asian HC FT+HT (51,53) · Euro HC (55)
      Odd/Even (45) · First team to score (41) · HT 1X2 (42,60)
      HT O/U (15,54,68) · HT/FT (44) · HT BTTS (328)
      HT Correct Score (203) · Highest scoring half (207)
      Corners (162,166) · Bookings (136,139)

2.  Pagination uses Content-Range header → "0-14/24" means 24 total.
    eFootball uses markets_layout=single (not multiple).

3.  _fetch_markets() handles 4 response shapes (A/B/C/D).

4.  Timestamps: divide by 1000 when > 1e10 (milliseconds).

Normalised match output
────────────────────────
{
  "betradar_id":  str,
  "sp_game_id":   str,
  "home_team":    str,
  "away_team":    str,
  "start_time":   str | None,   # ISO-8601 UTC
  "competition":  str,
  "sport":        str,          # canonical slug e.g. "soccer" / "esoccer"
  "source":       "sportpesa",
  "status":       "upcoming" | "live",
  "markets": {
    "1x2":                      {"1": 2.07, "X": 2.95, "2": 3.50},
    "over_under_goals_2.5":     {"over": 1.90, "under": 1.80},
    "btts":                     {"yes": 2.16, "no": 1.57},
    "double_chance":            {"1X": 1.20, "X2": 1.47, "12": 1.26},
    "draw_no_bet":              {"1": 1.48, "2": 2.47},
    "first_half_1x2":           {"1": 2.85, "X": 2.17, "2": 3.70},
    "first_half_over_under_1.0":{"over": 1.83, "under": 1.87},
    "first_half_btts":          {"yes": 4.20, "no": 1.19},
    "ht_ft":                    {"1/1": 3.30, "1/X": 12.00, ...},
    "first_team_to_score":      {"1": 1.86, "2": 2.23, "none": 11.0},
    "highest_scoring_half":     {"1st": 3.10, "2nd": 2.12, "equal": 3.25},
    "asian_handicap_-0.5":      {"1": 2.17, "2": 1.57},
    "european_handicap_1":      {"1": 1.34, "X": 4.90, "2": 6.80},
    "correct_score":            {"1:0": 8.00, "1:1": 5.60, ...},
    "first_half_correct_score": {"0:0": 2.55, "1:0": 3.95, ...},
    "exact_goals":              {"0": 9.80, "1": 4.70, ...},
    "number_of_goals":          {"0-1": 3.30, "2-3": 1.92, ...},
    "odd_even":                 {"odd": 1.95, "even": 1.76},
    "result_and_over_under_2.5":{"1_over": 3.80, "X_over": 12.0, ...},
    "btts_and_result":          {"1_yes": 4.80, "X_yes": 4.30, ...},
    "total_goals_home_1.5":     {"over": 2.18, "under": 1.61},
    "total_goals_away_1.5":     {"over": 2.70, "under": 1.41},
    "first_half_asian_handicap_0": {"1": 1.65, "2": 2.11},
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

# ── Constants ──────────────────────────────────────────────────────────────────

_BASE = "https://www.ke.sportpesa.com"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Mobile) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Timezone":   "Africa/Nairobi",
    "Origin":           "https://www.ke.sportpesa.com",
    "Referer":          "https://www.ke.sportpesa.com/",
}

# ── ALL football market IDs ────────────────────────────────────────────────────
# 33 IDs total — every market observed in intercepted SP football traffic.
# Requesting by explicit ID list is more reliable than "all".
#
# NEW in v3 (8 IDs added):
#   41  first_team_to_score
#   42  first_half_1x2  (alias to 60)
#   44  ht_ft
#   53  first_half_asian_handicap
#   54  first_half_over_under  (alias to 15/68)
#   203 first_half_correct_score
#   207 highest_scoring_half
#   328 first_half_btts
_CORE_MARKET_IDS = (
    "10,1,"          # 1X2
    "46,47,"         # Double Chance, Draw No Bet
    "43,29,386,"     # BTTS, BTTS+Result
    "52,18,"         # O/U Goals (multiple lines)
    "353,352,"       # Home / Away team goals
    "208,"           # Result + O/U
    "258,202,"       # Exact goals, Goal groups
    "332,"           # Correct Score
    "51,53,"         # Asian HC - FT, HT
    "55,"            # Euro HC
    "45,"            # Odd/Even
    "41,"            # First Team to Score ← NEW
    "42,60,"         # HT 1X2
    "15,54,68,"      # HT O/U (54 = new alias)
    "44,"            # HT/FT  ← NEW
    "328,"           # HT BTTS ← NEW
    "203,"           # HT Correct Score ← NEW
    "207,"           # Highest Scoring Half ← NEW
    "162,166,"       # Total Corners
    "136,139"        # Total Bookings
).replace("\n", "").replace(" ", "")

# ── Sport ID mapping ───────────────────────────────────────────────────────────
SP_SPORT_ID: dict[str, str] = {
    # Football (real + virtual)
    "soccer":           "1",
    "football":         "1",
    "esoccer":          "126",
    "efootball":        "126",
    "e-football":       "126",
    "virtual-football": "126",
    # Other sports
    "basketball":       "2",
    "tennis":           "5",
    "ice-hockey":       "4",
    "volleyball":       "23",
    "cricket":          "21",
    "rugby":            "12",
    "boxing":           "10",
    "handball":         "6",
    "table-tennis":     "16",
    "mma":              "117",
    "darts":            "49",
    "american-football":"15",
}

# eFootball uses single-market layout, real football uses multiple
_EFOOTBALL_IDS = {"126"}


# =============================================================================
# HTTP helpers
# =============================================================================

def _get(
    path: str,
    params: dict | None = None,
    timeout: int = 20,
) -> tuple[Any, dict]:
    """GET → (parsed_json | None, response_headers)."""
    url = f"{_BASE}{path}"
    try:
        r = requests.get(
            url, headers=_HEADERS, params=params,
            timeout=timeout, allow_redirects=True,
        )
        if r.status_code == 304:
            return None, {}
        if not r.ok:
            print(f"[sp] HTTP {r.status_code} → {url}")
            return None, dict(r.headers)
        return r.json(), dict(r.headers)
    except requests.exceptions.JSONDecodeError as e:
        print(f"[sp] JSON decode error {url}: {e}")
        return None, {}
    except Exception as exc:
        print(f"[sp] request error {url}: {exc}")
        return None, {}


def _parse_content_range(header: str | None) -> int | None:
    """
    Parse Content-Range: 0-14/24 → 24 (total items).
    Returns None if unparseable.
    """
    if not header:
        return None
    try:
        # format: "0-14/24"
        total_part = header.split("/")[-1].strip()
        return int(total_part)
    except (IndexError, ValueError):
        return None


# =============================================================================
# Raw fetchers
# =============================================================================

def _fetch_upcoming_page(
    sport_id: str,
    ts_start: int,
    ts_end: int,
    page_size: int,
    offset: int,
) -> tuple[list[dict], int | None]:
    """
    Fetch one page of upcoming matches.
    Returns (items, total_from_content_range).
    """
    is_efootball = sport_id in _EFOOTBALL_IDS
    raw, headers = _get("/api/upcoming/games", params={
        "type":           "prematch",
        "sportId":        sport_id,
        "section":        "upcoming",
        "markets_layout": "single" if is_efootball else "multiple",
        "o":              "leagues",
        "pag_count":      str(page_size),
        "pag_min":        str(offset + 1),   # SP uses 1-based offset
        "from":           str(ts_start),
        "to":             str(ts_end),
    })

    total = _parse_content_range(headers.get("content-range") or headers.get("Content-Range"))

    if isinstance(raw, list):
        return raw, total
    if isinstance(raw, dict):
        for key in ("data", "games", "items", "results"):
            if isinstance(raw.get(key), list):
                return raw[key], total
    return [], total


def _fetch_live_list(sport_id: str) -> list[dict]:
    """Fetch current live matches."""
    raw, _ = _get("/api/live/games", params={"sportId": sport_id})
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "games", "items"):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def _fetch_markets(game_id: str | int) -> list[dict]:
    """
    Fetch the full market book for one SP game.

    Response shape A (most common):
      { "8735919": [ {id, specValue, name, selections:[...]}, ... ] }

    Response shape B (nested):
      { "data": { "8735919": [...] } }

    Response shape C (flat list):
      [ {id, name, selections:[...]} ]

    Response shape D (wrapper):
      { "markets": [...] }
    """
    raw, _ = _get("/api/games/markets", params={
        "games":   str(game_id),
        "markets": _CORE_MARKET_IDS,
    })

    if raw is None:
        return []

    gid = str(game_id)

    if isinstance(raw, dict):
        # Shape A — game_id as string key
        if gid in raw and isinstance(raw[gid], list):
            return raw[gid]
        # Shape A — game_id as int key
        try:
            ikey = int(gid)
            if ikey in raw and isinstance(raw[ikey], list):
                return raw[ikey]
        except (ValueError, TypeError):
            pass
        # Shape B — nested under "data"
        for wrapper in ("data", "result"):
            data = raw.get(wrapper)
            if isinstance(data, dict):
                if gid in data:
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


def _parse_timestamp(item: dict) -> str | None:
    """Extract ISO-8601 UTC start time from an SP item."""
    for key in ("dateTimestamp", "startTimestamp", "timestamp"):
        ts = item.get(key)
        if ts:
            try:
                t = int(ts)
                if t > 1_000_000_000_000:   # milliseconds
                    t //= 1000
                return datetime.utcfromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            except Exception:
                pass
    for key in ("date", "startTime", "start_time"):
        dt = item.get(key)
        if dt:
            return str(dt)
    return None


def _parse_match_item(item: dict) -> dict | None:
    """Extract identity fields from a raw SP listing item."""
    sp_game_id  = str(item.get("id") or "")
    if not sp_game_id:
        return None

    betradar_id = str(
        item.get("betradarId") or item.get("betradar_id") or
        item.get("betRadarId") or ""
    )

    comps = item.get("competitors") or item.get("teams") or []
    if isinstance(comps, list) and len(comps) >= 2:
        home = _str_field(comps[0].get("name") or comps[0])
        away = _str_field(comps[1].get("name") or comps[1])
    else:
        home = _str_field(item.get("home") or item.get("homeName") or "")
        away = _str_field(item.get("away") or item.get("awayName") or "")

    comp_raw = item.get("competition") or item.get("league") or {}
    competition = _str_field(
        comp_raw if isinstance(comp_raw, (str, dict)) else ""
    ) or _str_field(item.get("leagueName") or item.get("competitionName") or "")

    sport_raw = item.get("sport") or {}
    sport_name = _str_field(
        sport_raw if isinstance(sport_raw, (str, dict)) else ""
    )

    start_time = _parse_timestamp(item)

    # Inline markets from the listing (O/U + BTTS only usually — no 1X2)
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
        "sport":        sport_name,
        "_inline_mkts": inline,
    }


def _parse_markets(raw_list: list[dict]) -> dict[str, dict[str, float]]:
    """
    Convert SP market list → { canonical_slug: { outcome_key: float } }.

    Handles every market in _CORE_MARKET_IDS.

    Special cases handled:
      • market id 52 appears 20+ times with different specValue (0.5 … 5.5)
        → each becomes over_under_goals_<line>
      • market id 55 (Euro HC) appears with specValue -3,-2,-1,1,2
        → each becomes european_handicap_<line>
      • shortName "OV_1" for Result+O/U → outcome "1_over"
      • shortName "11","1X","X2" for HT/FT → outcome "1/1","1/X","X/2"
      • shortName "1st","2nd","Eql" for Highest scoring half
    """
    markets: dict[str, dict[str, float]] = {}

    for mkt in raw_list:
        if not isinstance(mkt, dict):
            continue

        mkt_id   = mkt.get("id") or mkt.get("marketId") or mkt.get("typeId")
        spec_val = mkt.get("specValue")
        if spec_val is None:
            spec_val = mkt.get("spec") or mkt.get("handicap")

        if mkt_id is None:
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
                sel.get("label")    or sel.get("outcome") or ""
            )
            try:
                price = float(
                    sel.get("odds") or sel.get("price") or
                    sel.get("value") or 0
                )
            except (TypeError, ValueError):
                price = 0.0

            if price <= 1.0:
                continue

            out_key = normalize_outcome(mkt_key, short)

            # Keep highest price if the same outcome appears more than once
            existing = markets[mkt_key].get(out_key, 0.0)
            if price > existing:
                markets[mkt_key][out_key] = round(price, 3)

    # Remove empty market dicts
    return {k: v for k, v in markets.items() if v}


def _build_match(
    parsed: dict,
    markets: dict,
    sport_slug: str,
    status: str = "upcoming",
) -> dict:
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
    days: int = 3,
    page_size: int = 30,
    fetch_full_markets: bool = True,
    max_matches: int | None = None,
    sleep_between: float = 0.08,
) -> list[dict]:
    """
    Fetch upcoming matches for a sport. Returns canonical match dicts.

    For football (soccer/esoccer), fetch_full_markets=True is required to
    get 1X2 and all 33 market types — the listing endpoint only returns
    a subset inline.

    Pagination: Uses Content-Range header from SP API to know total pages.
    eFootball: uses sportId=126, markets_layout=single.
    """
    sport_id = SP_SPORT_ID.get(sport_slug.lower().replace(" ", "-"))
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

        while True:
            items, total_hint = _fetch_upcoming_page(
                sport_id, ts_s, ts_e, page_size, offset
            )
            if not items:
                break

            added = 0
            for it in items:
                gid = str(it.get("id") or "")
                if gid and gid not in seen_ids:
                    seen_ids.add(gid)
                    raw_items.append(it)
                    added += 1

            offset += page_size

            # Stop if we've fetched all or less than a full page
            if added < page_size:
                break
            if total_hint and len(seen_ids) >= total_hint:
                break
            if max_matches and len(raw_items) >= max_matches:
                break

    print(f"[sp] {sport_slug}: {len(raw_items)} raw items (sportId={sport_id})")

    targets = raw_items[:max_matches] if max_matches else raw_items
    results = []

    for item in targets:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"])
            if not raw_mkts:
                # Fallback to inline markets (usually O/U + BTTS only)
                raw_mkts = parsed["_inline_mkts"]
                print(f"[sp] fallback to inline: {parsed['sp_game_id']}")
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts)
        results.append(_build_match(parsed, markets, sport_slug))

    print(f"[sp] {sport_slug}: {len(results)} normalised  full_mkts={fetch_full_markets}")
    return results


def fetch_live(
    sport_slug: str,
    fetch_full_markets: bool = True,
    sleep_between: float = 0.06,
) -> list[dict]:
    """Fetch live matches for a sport."""
    sport_id = SP_SPORT_ID.get(sport_slug.lower().replace(" ", "-"))
    if not sport_id:
        return []

    raw_items = _fetch_live_list(sport_id)
    print(f"[sp:live] {sport_slug}: {len(raw_items)} live (sportId={sport_id})")

    results = []
    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"])
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts)
        results.append(_build_match(parsed, markets, sport_slug, status="live"))

    return results


def fetch_match_markets(game_id: str | int) -> dict[str, dict[str, float]]:
    """
    Fetch the complete normalised market book for one SP game.
    Use this in /api/sp/match/<id>/markets for on-demand market detail.
    """
    return _parse_markets(_fetch_markets(game_id))


def fetch_sport_ids() -> dict[str, int]:
    """Get live sport IDs and event counts from SP status API."""
    raw, _ = _get("/api/live/sports")
    if not isinstance(raw, dict):
        return {}
    sports = raw.get("sports") or raw.get("data") or []
    if isinstance(sports, list):
        return {s.get("name", ""): s.get("eventNumber", 0) for s in sports}
    return {}