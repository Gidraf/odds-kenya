"""
app/workers/sp_harvester.py
============================
Sportpesa Kenya harvester.

KEY FINDINGS from live API analysis (curl samples)
────────────────────────────────────────────────────
• The SP listing API (/api/upcoming/games) ALWAYS uses markets_layout=single,
  regardless of sport. This means the inline `markets` array on each listing
  row contains exactly ONE market (the primary one). All extra markets —
  O/U, handicap, BTTS, correct score — come exclusively from the separate
  /api/games/markets call.

• marketsCount=0 in boxing/mma inline data is SP's flag meaning "no extra
  markets beyond the inline primary". The full /api/games/markets fetch still
  returns that primary market.

• Using markets_layout=multiple (old code) for non-esoccer sports caused the
  listing call to return a response format different from what SP sends to its
  own frontend, potentially corrupting inline market parsing.

CHANGES vs v1
──────────────
• markets_layout is now always "single" — matches SP's own requests exactly.
  is_esoccer no longer controls layout; it only controls pagination cadence.
• Added "baseball": "3" to SP_SPORT_ID  (was missing → 0 matches returned)
• Added "3" (baseball) to _SPORT_MARKET_IDS
• Added market IDs 210 (1st Period) + 378 (OT alias) to ice-hockey markets
• specValue fallback: reads per-selection specValue for inline market format
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Generator

import requests

from app.workers.sp_mapper        import normalize_sp_market
from app.workers.canonical_mapper import normalize_outcome

# =============================================================================
# CONSTANTS
# =============================================================================

_BASE = "https://www.ke.sportpesa.com"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Timezone":   "Africa/Nairobi",
    "Origin":           "https://www.ke.sportpesa.com",
    "Referer":          "https://www.ke.sportpesa.com/",
}

# ── Sport slug → SP sport_id ──────────────────────────────────────────────────
SP_SPORT_ID: dict[str, str] = {
    "soccer":            "1",
    "football":          "1",
    "esoccer":           "126",
    "efootball":         "126",
    "e-football":        "126",
    "virtual-football":  "126",
    "basketball":        "2",
    "tennis":            "5",
    "ice-hockey":        "4",
    "icehockey":         "4",
    "volleyball":        "23",
    "cricket":           "21",
    "rugby":             "12",
    "rugby-league":      "12",
    "rugby-union":       "12",
    "boxing":            "10",
    "handball":          "6",
    "table-tennis":      "16",
    "tabletennis":       "16",
    "mma":               "117",
    "ufc":               "117",
    "darts":             "49",
    "american-football": "15",
    "americanfootball":  "15",
    "nfl":               "15",
    "baseball":          "3",   # ← FIX: was missing entirely
}

# ── Sport ID → market IDs for /api/games/markets ─────────────────────────────
# The listing API only returns 1 inline market per match. These IDs are what
# unlocks the full book: O/U, handicap, BTTS, correct score, etc.
_SPORT_MARKET_IDS: dict[str, str] = {
    # Football / Soccer
    "1": (
        "10,1,46,47,"
        "43,29,386,"
        "52,18,"
        "353,352,"
        "208,"
        "258,202,"
        "332,"
        "51,53,"
        "55,"
        "45,"
        "41,"
        "207,"
        "42,60,"
        "15,54,68,"
        "44,"
        "328,"
        "203,"
        "162,166,"
        "136,139"
    ).replace("\n","").replace(" ",""),

    # eFootball / eSoccer
    "126": (
        "381,1,10,"
        "56,52,"
        "46,47,"
        "43,"
        "51,"
        "45,"
        "208,258,202"
    ).replace("\n","").replace(" ",""),

    # Basketball
    "2": (
        "382,"
        "51,"
        "52,"
        "353,352,"
        "45,"
        "222,"
        "42,"
        "53,"
        "54,"
        "224,"
        "362,363,364,365,"
        "366,367,368,369"
    ).replace("\n","").replace(" ",""),

    # Tennis
    "5": (
        "382,"
        "204,231,"
        "51,"
        "226,"
        "233,"
        "439,"
        "45,"
        "339,340,"
        "433,"
        "353,352"
    ).replace("\n","").replace(" ",""),

    # Ice Hockey — added 210 (1st Period) + 378 (OT alias)
    "4": (
        "1,10,"
        "382,"
        "52,"
        "51,"
        "45,"
        "46,"
        "353,352,"
        "208,"
        "43,"
        "210,"
        "378"
    ).replace("\n","").replace(" ",""),

    # Volleyball
    "23": (
        "382,"
        "204,"
        "20,"
        "51,"
        "226,"
        "233,"
        "45,"
        "353,352"
    ).replace("\n","").replace(" ",""),

    # Handball
    "6": (
        "1,10,382,"
        "52,"
        "51,"
        "45,"
        "46,47,"
        "353,352,"
        "208,43,"
        "42,"
        "207"
    ).replace("\n","").replace(" ",""),

    # Table Tennis
    "16": "382,51,226,45,233,340",

    # Rugby Union / League
    "12": (
        "10,1,"
        "382,"
        "46,"
        "42,"
        "51,"
        "53,"
        "60,52,"
        "353,352,"
        "45,"
        "379,"
        "207,"
        "44"
    ).replace("\n","").replace(" ",""),

    # Cricket
    "21": "382,1,51,52,353,352",

    # Boxing
    "10": "382,51,52",

    # MMA / UFC
    "117": "382,20,51,52",

    # Darts
    "49": "382,226,45,51",

    # American Football / NFL
    "15": "382,51,52,45,353,352",

    # Baseball — FIX: was entirely missing
    "3": "382,51,52,45,353,352",
}

_DEFAULT_MARKET_IDS = "382,1,10,51,52,45,46"
_ESOCCER_IDS        = {"126"}


# =============================================================================
# HTTP
# =============================================================================

def _get(
    path:    str,
    params:  dict | None = None,
    timeout: int         = 20,
) -> tuple[Any, dict]:
    url = f"{_BASE}{path}"
    try:
        r = requests.get(url, headers=_HEADERS, params=params,
                         timeout=timeout, allow_redirects=True)
        if r.status_code == 304:
            return None, {}
        if not r.ok:
            print(f"[sp] HTTP {r.status_code} → {url}")
            return None, dict(r.headers)
        return r.json(), dict(r.headers)
    except requests.exceptions.JSONDecodeError as e:
        print(f"[sp] JSON decode {url}: {e}")
        return None, {}
    except Exception as exc:
        print(f"[sp] request error {url}: {exc}")
        return None, {}


def _parse_content_range(header: str | None) -> int | None:
    if not header:
        return None
    try:
        return int(header.split("/")[-1].strip())
    except (IndexError, ValueError):
        return None


# =============================================================================
# RAW SP API FETCHERS
# =============================================================================

def _fetch_upcoming_page(
    sport_id:  str,
    ts_start:  int,
    ts_end:    int,
    page_size: int,
    offset:    int,
) -> tuple[list[dict], int | None]:
    """
    Fetch one page of upcoming matches.

    CRITICAL: markets_layout is always "single".
    The SP website itself always sends 'single' regardless of sport.
    Each match in the response has exactly 1 inline market (the primary one).
    All other markets come from the separate /api/games/markets fetch.
    """
    raw, headers = _get("/api/upcoming/games", params={
        "type":           "prematch",
        "sportId":        sport_id,
        "section":        "upcoming",
        "markets_layout": "single",   # always single — matches SP's own requests
        "o":              "leagues",
        "pag_count":      str(page_size),
        "pag_min":        str(offset + 1),
        "from":           str(ts_start),
        "to":             str(ts_end),
    })
    total = _parse_content_range(
        headers.get("content-range") or headers.get("Content-Range")
    )
    if isinstance(raw, list):
        return raw, total
    if isinstance(raw, dict):
        for key in ("data", "games", "items", "results"):
            if isinstance(raw.get(key), list):
                return raw[key], total
    return [], total


def _fetch_live_list(sport_id: str) -> list[dict]:
    """
    Fetch live events for a sport.
    Confirmed endpoint from SP browser traffic:
      GET /api/live/sports/{sportId}/events?limit=100
    Returns {"events": [...]} with id, competitors, state, externalId, kickoffTimeUTC
    """
    # Primary endpoint (confirmed from SP frontend network traffic)
    raw, _ = _get(f"/live/sports/{sport_id}/events", params={"limit": 100})
    if raw:
        for key in ("events", "data", "items"):
            if isinstance(raw.get(key), list) and raw[key]:
                print(f"[sp:{sport_id}:live] _fetch_live_list: {len(raw[key])} events via /live/sports/{sport_id}/events")
                return raw[key]
        if isinstance(raw, list) and raw:
            return raw

    # Fallback: old endpoint (kept for safety)
    print(f"[sp:{sport_id}:live] primary endpoint empty — trying /live/games fallback")
    raw, _ = _get("/live/games", params={"sportId": sport_id})
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "games", "items", "events"):
            if isinstance(raw.get(key), list):
                return raw[key]

    print(f"[sp:{sport_id}:live] both live endpoints returned nothing")
    return []


def _fetch_live_event_details_markets(event_id: str | int) -> list[dict]:
    """
    GET /api/live/events/{eventId}/details → markets[]
    Returns live-specific market IDs (194=1x2, 147=DC, 105=Total, 138=BTTS, etc.)
    with selection names as full team names ("Afghanistan", "draw", "Myanmar").
    """
    raw, _ = _get(f"/live/events/{event_id}/details")
    if not raw or not isinstance(raw, dict):
        return []
    markets = raw.get("markets") or []
    if markets:
        print(f"[sp:live:details] event={event_id}: {len(markets)} markets")
    return markets if isinstance(markets, list) else []


def _extract_market_list(raw: Any, gid: str) -> list[dict]:
    """Extract market list from any observed /api/games/markets response shape."""
    if raw is None:
        return []
    if isinstance(raw, dict):
        # Shape A: {"<game_id>": [...]}
        if gid in raw and isinstance(raw[gid], list):
            return raw[gid]
        try:
            ikey = int(gid)
            if ikey in raw and isinstance(raw[ikey], list):
                return raw[ikey]
        except (ValueError, TypeError):
            pass
        # Shape B: {"data": {"<game_id>": [...]}}
        for wrapper in ("data", "result"):
            inner = raw.get(wrapper)
            if isinstance(inner, dict):
                if gid in inner and isinstance(inner[gid], list):
                    return inner[gid]
                for v in inner.values():
                    if isinstance(v, list) and v:
                        return v
        # Shape C: {"markets": [...]}
        if isinstance(raw.get("markets"), list):
            return raw["markets"]
        for v in raw.values():
            if isinstance(v, list) and v:
                return v
    # Shape D: flat list
    if isinstance(raw, list):
        return raw
    return []


def _fetch_markets(
    game_id:    str | int,
    market_ids: str,
    debug:      bool = False,
    max_tries:  int  = 2,
) -> list[dict]:
    gid     = str(game_id)
    backoff = [0, 1.5]

    for attempt in range(max_tries):
        if attempt > 0:
            time.sleep(backoff[min(attempt, len(backoff) - 1)])

        raw, headers = _get("/api/games/markets", params={
            "games":   gid,
            "markets": market_ids,
        })

        rl = headers.get("x-rate-limit-remaining") or headers.get("X-RateLimit-Remaining")
        if rl == "0":
            print(f"[sp:mkts] rate-limit=0 game={gid} — sleeping 2s")
            time.sleep(2.0)

        result = _extract_market_list(raw, gid)

        if not result:
            print(f"[sp:mkts] empty game={gid} attempt={attempt}")
            continue

        if debug:
            mkt_ids = [m.get("id") for m in result]
            ou_ids  = [i for i in mkt_ids if i in (52, 18, 56)]
            print(f"[sp:mkts] OK game={gid}  {len(result)} mkts  "
                  f"ou_ids={ou_ids}  ids={mkt_ids[:12]}")
        return result

    print(f"[sp:mkts] all {max_tries} attempts empty for game={gid}")
    return []


# =============================================================================
# PARSERS
# =============================================================================

def _str_field(v: Any) -> str:
    if isinstance(v, dict):
        return str(v.get("name") or v.get("title") or v.get("short") or "")
    return str(v) if v else ""


def _parse_timestamp(item: dict) -> str | None:
    for key in ("dateTimestamp", "startTimestamp", "timestamp"):
        ts = item.get(key)
        if ts is not None:
            try:
                t = int(ts)
                if t > 1_000_000_000_000:
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
    sp_game_id = str(item.get("id") or "")
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

    comp_raw    = item.get("competition") or item.get("league") or {}
    competition = _str_field(comp_raw) or _str_field(
        item.get("leagueName") or item.get("competitionName") or ""
    )

    sport_raw   = item.get("sport") or {}
    sport_name  = _str_field(sport_raw)
    sp_sport_id: int = 1
    if isinstance(sport_raw, dict):
        try:
            sp_sport_id = int(sport_raw.get("id") or 1)
        except (TypeError, ValueError):
            pass
    elif isinstance(sport_raw, int):
        sp_sport_id = sport_raw

    # Inline markets: always exactly 1 entry with markets_layout=single.
    # Used only as a last-resort fallback when the full market fetch fails.
    inline = item.get("markets") or item.get("odds") or []
    if isinstance(inline, dict):
        inline = list(inline.values())

    return {
        "betradar_id":     betradar_id,
        "sp_game_id":      sp_game_id,
        "home_team":       home,
        "away_team":       away,
        "start_time":      _parse_timestamp(item),
        "competition":     competition,
        "sport":           sport_name,
        "sp_sport_id":     sp_sport_id,
        "_inline_mkts":    inline,
        "_markets_count":  item.get("marketsCount", 0),
    }


def _parse_markets(
    raw_list: list[dict],
    game_id:  str = "",
    sport_id: int = 1,
) -> dict[str, dict[str, float]]:
    """
    Convert SP market list → {canonical_slug: {outcome_key: float}}.

    specValue resolution:
      1. mkt["specValue"]                (market-level, from full markets fetch)
      2. mkt["spec"] / mkt["handicap"]
      3. first sel["specValue"] != 0     (per-selection, from inline listing)
         Note: boxing/tennis inline markets all have specValue=0 on both the
         market and selections since they are match-winner markets (no line).
    """
    markets: dict[str, dict[str, float]] = {}
    ou_seen = False

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

        if mkt_id in (52, 18, 56):
            ou_seen = True

        # specValue: use `is None` check — 0 is a valid line (Asian HC level ball)
        spec_val = mkt.get("specValue")
        if spec_val is None:
            spec_val = mkt.get("spec") or mkt.get("handicap")

        # Fallback: read specValue from first selection that has a non-zero value.
        # Handles line markets in the inline listing format.
        if spec_val is None or spec_val == 0:
            sels_raw = mkt.get("selections") or mkt.get("outcomes") or []
            if isinstance(sels_raw, dict):
                sels_raw = list(sels_raw.values())
            for s in sels_raw:
                if isinstance(s, dict):
                    sv = s.get("specValue")
                    if sv is not None and sv != 0:
                        spec_val = sv
                        break

        mkt_key = normalize_sp_market(mkt_id, spec_val, sport_id)
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
                    sel.get("odds") or sel.get("price") or sel.get("value") or 0
                )
            except (TypeError, ValueError):
                price = 0.0

            if price <= 1.0:
                continue

            out_key = normalize_outcome(mkt_key, short)
            if price > markets[mkt_key].get(out_key, 0.0):
                markets[mkt_key][out_key] = round(price, 3)

    result = {k: v for k, v in markets.items() if v}

    if game_id:
        ou_keys = [k for k in result if "total" in k or "over_under" in k]
        if not ou_seen:
            print(f"[sp:ou] {game_id}: IDs 52/18/56 NOT in raw ({len(raw_list)} mkts) "
                  "— SP doesn't offer O/U for this game")
        elif not ou_keys:
            print(f"[sp:ou] {game_id}: O/U IDs present but no total*/over_under* keys "
                  "— check shortNames")
        else:
            print(f"[sp:ou] {game_id}: O/U OK → {ou_keys[:5]}")

    return result


def _build_match(
    parsed:     dict,
    markets:    dict,
    sport_slug: str,
    status:     str = "upcoming",
) -> dict:
    return {
        "betradar_id":  parsed["betradar_id"],
        "sp_game_id":   parsed["sp_game_id"],
        "home_team":    parsed["home_team"],
        "away_team":    parsed["away_team"],
        "start_time":   parsed["start_time"],
        "competition":  parsed["competition"],
        "sport":        parsed["sport"] or sport_slug,
        "sp_sport_id":  parsed.get("sp_sport_id", 1),
        "source":       "sportpesa",
        "status":       status,
        "markets":      markets,
        "market_count": len(markets),
        "harvested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# =============================================================================
# RAW ITEM COLLECTOR
# =============================================================================

def _collect_raw_items(
    sport_id:   str,
    days:       int,
    page_size:  int,
    max_items:  int | None,
    is_esoccer: bool = False,
) -> list[dict]:
    """
    Walk day-by-day pages of the listing API and collect raw match rows.
    is_esoccer: only used to tune pagination window, not the layout param.
    """
    raw_items: list[dict] = []
    seen_ids:  set[str]   = set()
    now_eat = datetime.now(timezone.utc) + timedelta(hours=3)

    for day_off in range(days):
        day  = now_eat + timedelta(days=day_off)
        ts_s = int(day.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        ts_e = ts_s + 86400
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
            if added < page_size:
                break
            if total_hint and len(seen_ids) >= total_hint:
                break
            if max_items and len(raw_items) >= max_items:
                break

        if max_items and len(raw_items) >= max_items:
            break

    return raw_items[:max_items] if max_items else raw_items


def _get_config(sport_slug: str) -> tuple[str, str, bool, int, int]:
    """Returns (sport_id_str, market_ids, is_esoccer, days_default, max_default)."""
    slug      = sport_slug.lower().replace(" ", "-")
    sport_id  = SP_SPORT_ID.get(slug)
    if not sport_id:
        print(f"[sp] unknown sport: {sport_slug!r}")
        return "", "", False, 3, 150
    is_esoccer   = sport_id in _ESOCCER_IDS
    market_ids   = _SPORT_MARKET_IDS.get(sport_id, _DEFAULT_MARKET_IDS)
    market_ids   = market_ids.replace("\n", "").replace(" ", "")
    days_default = 1 if is_esoccer else 3
    max_default  = 60 if is_esoccer else 150
    return sport_id, market_ids, is_esoccer, days_default, max_default


# =============================================================================
# PUBLIC API — STREAMING GENERATORS
# =============================================================================

def fetch_upcoming_stream(
    sport_slug:         str,
    days:               int | None   = None,
    max_matches:        int | None   = None,
    fetch_full_markets: bool         = True,
    sleep_between:      float        = 0.3,
    debug_ou:           bool         = False,
    **_,
) -> Generator[dict, None, None]:
    """
    Yield one normalised match dict at a time as markets are fetched.
    fetch_full_markets=True (default) is required to see O/U, handicap, etc.
    """
    sport_id, market_ids, is_esoccer, days_default, max_default = _get_config(sport_slug)
    if not sport_id:
        return

    days  = days        or days_default
    max_m = max_matches or max_default

    raw_items = _collect_raw_items(sport_id, days, 30, max_m, is_esoccer=is_esoccer)
    print(f"[sp:{sport_slug}] {len(raw_items)} raw items (sportId={sport_id})")

    inline_count = 0
    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(
                parsed["sp_game_id"], market_ids, debug=debug_ou
            )
            if not raw_mkts:
                # Fallback: 1 inline market only
                raw_mkts = parsed["_inline_mkts"]
                inline_count += 1
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(
            raw_mkts,
            game_id  = parsed["sp_game_id"] if debug_ou else "",
            sport_id = parsed.get("sp_sport_id") or int(sport_id),
        )
        yield _build_match(parsed, markets, sport_slug)

    if inline_count:
        print(f"[sp:{sport_slug}] WARNING: {inline_count}/{len(raw_items)} "
              "matches used inline fallback (primary market only)")


def fetch_live_stream(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.3,
    debug_ou:           bool  = False,
    **_,
) -> Generator[dict, None, None]:
    """
    Yield live matches one at a time.

    API flow:
      1. _fetch_live_list()  →  GET /api/live/sports/{sportId}/events  (FIXED)
      2. _fetch_live_event_details_markets()  →  GET /api/live/events/{id}/details
         Returns live-specific market IDs (194, 147, 105, 138…) with full team names.
      3. Falls back to /api/games/markets if details returns nothing.
    """
    sport_id, market_ids, _, _, _ = _get_config(sport_slug)
    if not sport_id:
        return

    raw_items = _fetch_live_list(sport_id)
    print(f"[sp:{sport_slug}:live] {len(raw_items)} live events (sportId={sport_id})")

    inline_count = 0
    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            # Try live details endpoint first (correct live market IDs + team name sels)
            raw_mkts = _fetch_live_event_details_markets(parsed["sp_game_id"])
            if not raw_mkts:
                # Fallback: upcoming /api/games/markets endpoint
                raw_mkts = _fetch_markets(parsed["sp_game_id"], market_ids, debug=debug_ou)
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
                inline_count += 1
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(
            raw_mkts,
            game_id  = parsed["sp_game_id"] if debug_ou else "",
            sport_id = parsed.get("sp_sport_id") or int(sport_id),
        )
        yield _build_match(parsed, markets, sport_slug, status="live")

    if inline_count:
        print(f"[sp:{sport_slug}:live] {inline_count}/{len(raw_items)} used inline fallback")


# =============================================================================
# PUBLIC API — BLOCKING
# =============================================================================

def fetch_upcoming(
    sport_slug:         str,
    days:               int | None = None,
    max_matches:        int | None = None,
    fetch_full_markets: bool       = True,
    sleep_between:      float      = 0.3,
    debug_ou:           bool       = False,
    **_,
) -> list[dict]:
    results = list(fetch_upcoming_stream(
        sport_slug,
        days               = days,
        max_matches        = max_matches,
        fetch_full_markets = fetch_full_markets,
        sleep_between      = sleep_between,
        debug_ou           = debug_ou,
    ))
    print(f"[sp] {sport_slug}: {len(results)} normalised")
    return results


def fetch_live(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.3,
    debug_ou:           bool  = False,
    **_,
) -> list[dict]:
    results = list(fetch_live_stream(
        sport_slug,
        fetch_full_markets = fetch_full_markets,
        sleep_between      = sleep_between,
        debug_ou           = debug_ou,
    ))
    print(f"[sp:live] {sport_slug}: {len(results)} live")
    return results


# =============================================================================
# PUBLIC API — ON-DEMAND SINGLE MATCH
# =============================================================================

def fetch_match_markets(
    game_id:    str | int,
    sport_slug: str = "soccer",
) -> dict[str, dict[str, float]]:
    """Full market book for one SP game ID."""
    sport_id, market_ids, _, _, _ = _get_config(sport_slug)
    if not sport_id:
        sport_id   = "1"
        market_ids = _SPORT_MARKET_IDS["1"]

    raw    = _fetch_markets(game_id, market_ids, debug=True)
    result = _parse_markets(raw, game_id=str(game_id), sport_id=int(sport_id))
    print(f"[sp:match] {game_id}: {len(result)} slugs → {list(result.keys())[:8]}")
    return result


def _fetch_markets_for_debug(
    game_id:    str,
    sport_slug: str  = "soccer",
    debug:      bool = True,
) -> list[dict]:
    sport_id, market_ids, _, _, _ = _get_config(sport_slug)
    if not sport_id:
        market_ids = _SPORT_MARKET_IDS["1"]
    return _fetch_markets(game_id, market_ids, debug=debug)


def _parse_markets_for_debug(
    raw_list:   list[dict],
    game_id:    str = "",
    sport_slug: str = "soccer",
) -> dict[str, dict[str, float]]:
    sport_id, _, _, _, _ = _get_config(sport_slug)
    sid = int(sport_id) if sport_id else 1
    return _parse_markets(raw_list, game_id=game_id, sport_id=sid)


def fetch_sport_ids() -> dict[str, int]:
    """Return {sport_name: live_event_count} from the SP live sports API."""
    raw, _ = _get("/api/live/sports")
    if not isinstance(raw, dict):
        return {}
    sports = raw.get("sports") or raw.get("data") or []
    if isinstance(sports, list):
        return {s.get("name", ""): s.get("eventNumber", 0) for s in sports}
    return {}


# =============================================================================
# PUBLIC EXPORTS
# =============================================================================

__all__ = [
    # Streaming generators (used by SSE endpoints in sp_module.py)
    "fetch_upcoming_stream",
    "fetch_live_stream",
    # Blocking wrappers (used by direct/* endpoints)
    "fetch_upcoming",
    "fetch_live",
    # On-demand single match
    "fetch_match_markets",
    # Debug helpers (used by /debug endpoint)
    "_fetch_markets_for_debug",
    "_parse_markets_for_debug",
    # Sport event counts
    "fetch_sport_ids",
    # Sport ID / slug lookup
    "SP_SPORT_ID",
]