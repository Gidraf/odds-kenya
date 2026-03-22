"""
app/workers/sp_harvester.py
============================
Sportpesa Kenya harvester — single-file edition.

Self-contained: only requires sp_mapper + canonical_mapper.
No sp_harvester_base.py or sp_sports/ package needed.

Public API
──────────
  fetch_upcoming(sport_slug, ...)       → list[dict]
  fetch_live(sport_slug, ...)           → list[dict]
  fetch_upcoming_stream(sport_slug, …)  → Generator[dict]
  fetch_live_stream(sport_slug, …)      → Generator[dict]
  fetch_match_markets(game_id, …)       → dict
  fetch_sport_ids()                     → dict
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
}

# ── Sport slug → market IDs to request ────────────────────────────────────────
_SPORT_MARKET_IDS: dict[str, str] = {
    "1": (                          # Football / Soccer
        "10,1,46,47,"               # 1X2, Double Chance, Draw No Bet
        "43,29,386,"                # BTTS, BTTS+Result
        "52,18,"                    # O/U Goals (multi-line via specValue)
        "353,352,"                  # Home / Away team goals O/U
        "208,"                      # Result + O/U
        "258,202,"                  # Exact Goals, Goal Groups
        "332,"                      # Correct Score
        "51,53,"                    # Asian HC FT + HT
        "55,"                       # European HC
        "45,"                       # Odd/Even Goals
        "41,"                       # First Team to Score
        "207,"                      # Highest Scoring Half
        "42,60,"                    # HT 1X2
        "15,54,68,"                 # HT O/U
        "44,"                       # HT/FT
        "328,"                      # HT BTTS
        "203,"                      # HT Correct Score
        "162,166,"                  # Total Corners
        "136,139"                   # Total Bookings / Cards
    ),
    "126": (                        # eFootball / eSoccer
        "381,1,10,"                 # 1X2
        "56,52,"                    # O/U Goals
        "46,47,"                    # Double Chance, Draw No Bet
        "43,"                       # BTTS
        "51,"                       # Asian Handicap
        "45,"                       # Odd/Even
        "208,258,202"               # Result+O/U, Exact Goals
    ),
    "2": (                          # Basketball
        "382,"                      # Match Winner (2-way, OT incl.)
        "51,"                       # Point Spread (multi-line)
        "52,"                       # Total Points O/U (multi-line)
        "353,"                      # Home Team Total Points O/U
        "352,"                      # Away Team Total Points O/U
        "45,"                       # Odd/Even Points
        "222,"                      # Winning Margin
        "42,"                       # 3 Way - First Half
        "53,"                       # Handicap - First Half
        "54,"                       # Total Points - First Half
        "224,"                      # Highest Scoring Quarter
        "362,363,364,365,"          # Q1-Q4 O/U Total Points
        "366,367,368,369"           # Q1-Q4 Handicap
    ),
    "5": (                          # Tennis
        "382,"                      # Match Winner
        "204,231,"                  # First / Second Set Winner
        "51,"                       # Game Handicap (multi-line)
        "226,"                      # Total Games O/U
        "233,"                      # Set Betting
        "439,"                      # Set Handicap
        "45,"                       # Odd/Even Games
        "339,340,"                  # 1st Set Game HC / 1st Set Total Games
        "433,"                      # 1st Set / Match Winner combo
        "353,352"                   # Player 1/2 Games O/U
    ),
    "4": (                          # Ice Hockey
        "1,10,"                     # 1X2
        "382,"                      # Match Winner (2-way OT/SO)
        "52,"                       # Total Goals O/U
        "51,"                       # Puck Line
        "45,"                       # Odd/Even Goals
        "46,"                       # Double Chance
        "353,352,"                  # Home / Away Goals O/U
        "208,43"                    # Result+O/U, BTTS
    ),
    "23": (                         # Volleyball
        "382,"                      # Match Winner
        "51,"                       # Set Handicap
        "226,"                      # Total Sets O/U
        "233,"                      # Set Betting
        "45,"                       # Odd/Even Points
        "353,352"                   # Home / Away Points O/U
    ),
    "6": (                          # Handball
        "1,10,382,"                 # 1X2 / Match Winner
        "52,"                       # Total Goals O/U
        "51,"                       # Asian Handicap
        "45,"                       # Odd/Even Goals
        "46,47,"                    # Double Chance, Draw No Bet
        "353,352,"                  # Home / Away Goals O/U
        "208,43"                    # Result+O/U, BTTS
    ),
    "16": (                         # Table Tennis
        "382,"                      # Match Winner
        "51,"                       # Game Handicap
        "226,"                      # Total Games O/U
        "45,233,340"                # Odd/Even, Set Betting, 1st Set Games
    ),
    "12": (                         # Rugby
        "382,1,10,46,"              # Match Winner / 1X2
        "51,"                       # Asian Handicap
        "52,"                       # Total Points O/U
        "45,353,352"                # Odd/Even, Home/Away Points
    ),
    "21": (                         # Cricket
        "382,1,"                    # Match Winner
        "51,"                       # Handicap
        "52,353,352"                # Total Runs O/U
    ),
    "10": (                         # Boxing
        "382,51,52"                 # Fight Winner, Round Betting, Total Rounds
    ),
    "117": (                        # MMA
        "382,51,52"                 # Fight Winner, Round Betting, Total Rounds
    ),
    "49": (                         # Darts
        "382,226,45,51"             # Match Winner, Total Legs, Odd/Even, Handicap
    ),
    "15": (                         # American Football
        "382,51,52,45,353,352"      # Moneyline, Spread, Total, Odd/Even
    ),
}

_DEFAULT_MARKET_IDS = "382,1,10,51,52,45,46"  # generic fallback

_ESOCCER_IDS = {"126"}


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
    sport_id:   str,
    ts_start:   int,
    ts_end:     int,
    page_size:  int,
    offset:     int,
    is_esoccer: bool = False,
) -> tuple[list[dict], int | None]:
    raw, headers = _get("/api/upcoming/games", params={
        "type":           "prematch",
        "sportId":        sport_id,
        "section":        "upcoming",
        "markets_layout": "single" if is_esoccer else "multiple",
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
    raw, _ = _get("/api/live/games", params={"sportId": sport_id})
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "games", "items"):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def _extract_market_list(raw: Any, gid: str) -> list[dict]:
    if raw is None:
        return []
    if isinstance(raw, dict):
        if gid in raw and isinstance(raw[gid], list):
            return raw[gid]
        try:
            ikey = int(gid)
            if ikey in raw and isinstance(raw[ikey], list):
                return raw[ikey]
        except (ValueError, TypeError):
            pass
        for wrapper in ("data", "result"):
            inner = raw.get(wrapper)
            if isinstance(inner, dict):
                if gid in inner and isinstance(inner[gid], list):
                    return inner[gid]
                for v in inner.values():
                    if isinstance(v, list) and v:
                        return v
        if isinstance(raw.get("markets"), list):
            return raw["markets"]
        for v in raw.values():
            if isinstance(v, list) and v:
                return v
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

    inline = item.get("markets") or item.get("odds") or []
    if isinstance(inline, dict):
        inline = list(inline.values())

    return {
        "betradar_id":  betradar_id,
        "sp_game_id":   sp_game_id,
        "home_team":    home,
        "away_team":    away,
        "start_time":   _parse_timestamp(item),
        "competition":  competition,
        "sport":        sport_name,
        "sp_sport_id":  sp_sport_id,
        "_inline_mkts": inline,
    }


def _parse_markets(
    raw_list: list[dict],
    game_id:  str = "",
    sport_id: int = 1,
) -> dict[str, dict[str, float]]:
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

        # MUST use is None — specValue=0 is a valid line (Asian HC level ball)
        spec_val = mkt.get("specValue")
        if spec_val is None:
            spec_val = mkt.get("spec") or mkt.get("handicap")

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
            print(f"[sp:ou] {game_id}: IDs 52/18/56 NOT in raw ({len(raw_list)} mkts) — SP doesn't offer O/U")
        elif not ou_keys:
            print(f"[sp:ou] {game_id}: O/U IDs present but no total* keys after parse — check shortNames")
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
                sport_id, ts_s, ts_e, page_size, offset, is_esoccer=is_esoccer
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
    """
    Returns (sport_id_str, market_ids, is_esoccer, days_default, max_default).
    """
    slug      = sport_slug.lower().replace(" ", "-")
    sport_id  = SP_SPORT_ID.get(slug)
    if not sport_id:
        print(f"[sp] unknown sport: {sport_slug!r}")
        return "", "", False, 3, 150
    is_esoccer   = sport_id in _ESOCCER_IDS
    market_ids   = _SPORT_MARKET_IDS.get(sport_id, _DEFAULT_MARKET_IDS)
    # Clean concatenated string
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
    Used by SSE endpoints in sp_module.py.
    """
    sport_id, market_ids, is_esoccer, days_default, max_default = _get_config(sport_slug)
    if not sport_id:
        return

    days    = days        or days_default
    max_m   = max_matches or max_default

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
        print(f"[sp:{sport_slug}] WARNING: {inline_count}/{len(raw_items)} used inline fallback")


def fetch_live_stream(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.3,
    debug_ou:           bool  = False,
    **_,
) -> Generator[dict, None, None]:
    """Yield live matches one at a time."""
    sport_id, market_ids, _, _, _ = _get_config(sport_slug)
    if not sport_id:
        return

    raw_items = _fetch_live_list(sport_id)
    print(f"[sp:{sport_slug}:live] {len(raw_items)} live (sportId={sport_id})")

    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(
                parsed["sp_game_id"], market_ids, debug=debug_ou
            )
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(
            raw_mkts,
            game_id  = parsed["sp_game_id"] if debug_ou else "",
            sport_id = parsed.get("sp_sport_id") or int(sport_id),
        )
        yield _build_match(parsed, markets, sport_slug, status="live")


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


# =============================================================================
# INTERNAL — used by /debug endpoint in sp_module.py
# =============================================================================

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


# =============================================================================
# PUBLIC API — SPORT EVENT COUNTS
# =============================================================================

def fetch_sport_ids() -> dict[str, int]:
    """Return {sport_name: live_event_count} from the SP live sports API."""
    raw, _ = _get("/api/live/sports")
    if not isinstance(raw, dict):
        return {}
    sports = raw.get("sports") or raw.get("data") or []
    if isinstance(sports, list):
        return {s.get("name", ""): s.get("eventNumber", 0) for s in sports}
    return {}