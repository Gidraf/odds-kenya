"""
app/workers/sp_harvester_base.py
=================================
Shared HTTP utilities, market fetching, parsing, and match-building
used by ALL per-sport Sportpesa harvesters.

Nothing in this file is sport-specific.  Import it from a sport module:

    from app.workers.sp_harvesterbase import (
        fetch_upcoming_stream, fetch_live_stream,
        fetch_upcoming, fetch_live, fetch_match_markets,
    )
    from app.workers.sp_sports.basketball import CONFIG as BBALL

    list(fetch_upcoming_stream("basketball", CONFIG))
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Generator

import requests

from app.workers.sp_mapper        import normalize_sp_market
from app.workers.canonical_mapper import normalize_outcome

# =============================================================================
# SPORT CONFIG DATACLASS
# =============================================================================

@dataclass(frozen=True)
class SportConfig:
    """
    Everything a per-sport harvester needs to know — used by all generic
    fetch functions so there is zero sport-specific code in the base.

    Attributes
    ──────────
    slugs         All URL slugs that map to this sport (e.g. ["soccer","football"])
    sport_id      SP numeric sport ID (1=football, 2=basketball, …)
    market_ids    Comma-separated market IDs to request from SP API
    days_default  Default number of upcoming days to fetch
    max_default   Default max matches per fetch
    is_esoccer    True → use single markets_layout; different pagination
    """
    slugs:        tuple[str, ...]
    sport_id:     int
    market_ids:   str
    days_default: int   = 3
    max_default:  int   = 150
    is_esoccer:   bool  = False


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

_ESOCCER_IDS = {"126"}


# =============================================================================
# HTTP
# =============================================================================

def _get(
    path:    str,
    params:  dict | None = None,
    timeout: int         = 20,
) -> tuple[Any, dict]:
    """GET → (json_body | None, response_headers_dict)."""
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


# =============================================================================
# MARKET FETCHING
# =============================================================================

def _extract_market_list(raw: Any, gid: str) -> list[dict]:
    """Extract market list from any SP /api/games/markets response shape."""
    if raw is None:
        return []
    if isinstance(raw, dict):
        # Shape A — string or int game_id key
        if gid in raw and isinstance(raw[gid], list):
            return raw[gid]
        try:
            ikey = int(gid)
            if ikey in raw and isinstance(raw[ikey], list):
                return raw[ikey]
        except (ValueError, TypeError):
            pass
        # Shape B — wrapped under "data" or "result"
        for wrapper in ("data", "result"):
            inner = raw.get(wrapper)
            if isinstance(inner, dict):
                if gid in inner and isinstance(inner[gid], list):
                    return inner[gid]
                for v in inner.values():
                    if isinstance(v, list) and v:
                        return v
        # Shape D — {"markets": [...]}
        if isinstance(raw.get("markets"), list):
            return raw["markets"]
        # Last resort
        for v in raw.values():
            if isinstance(v, list) and v:
                return v
    # Shape C — flat list
    if isinstance(raw, list):
        return raw
    return []


def _fetch_markets(
    game_id:    str | int,
    market_ids: str,
    debug:      bool = False,
    max_tries:  int  = 2,
) -> list[dict]:
    """
    Fetch the full market book for one SP game.

    Only retries on truly empty responses (network error / 429).
    Non-empty responses with only specValue=0 are accepted immediately —
    many lower-league / esoccer / youth matches don't offer multi-line O/U.
    """
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
            ou_present = any(i in mkt_ids for i in (52, 18, 56))
            print(f"[sp:mkts] OK game={gid}  {len(result)} mkts  "
                  f"ou={'yes' if ou_present else 'no'}  ids={mkt_ids[:12]}")
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
    """Extract identity fields from one SP listing row."""
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
    """
    Convert SP market list → {canonical_slug: {outcome_key: float}}.

    Uses explicit `is None` check for specValue so integer 0 is preserved
    (Asian Handicap level-ball line).
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

        # ── specValue: explicit None check — 0 is a valid line ───────────────
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
        if not ou_seen:
            print(f"[sp:ou] {game_id}: IDs 52/18/56 NOT in raw ({len(raw_list)} mkts) "
                  f"— SP doesn't offer O/U for this game")
        else:
            ou_keys = [k for k in result if "over_under" in k or "total" in k]
            print(f"[sp:ou] {game_id}: O/U keys → {ou_keys[:5]}")

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
# RAW ITEM COLLECTOR (shared by all sports)
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


# =============================================================================
# GENERIC STREAMING GENERATORS  (called by per-sport modules)
# =============================================================================

def fetch_upcoming_stream(
    sport_slug:          str,
    config:              SportConfig,
    days:                int        | None = None,
    max_matches:         int        | None = None,
    fetch_full_markets:  bool              = True,
    sleep_between:       float             = 0.3,
    debug_ou:            bool              = False,
) -> Generator[dict, None, None]:
    """
    Generic upcoming stream — driven by SportConfig.
    Yields one normalised match dict at a time (SSE-compatible).
    """
    sport_id = str(config.sport_id)
    days     = days        or config.days_default
    max_m    = max_matches or config.max_default

    raw_items = _collect_raw_items(
        sport_id, days, 30, max_m, is_esoccer=config.is_esoccer
    )
    print(f"[sp:{sport_slug}] {len(raw_items)} raw items (sportId={sport_id})")

    inline_count = 0
    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(
                parsed["sp_game_id"], config.market_ids, debug=debug_ou
            )
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
                inline_count += 1
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        sport_id_int = parsed.get("sp_sport_id") or config.sport_id
        markets = _parse_markets(
            raw_mkts,
            game_id  = parsed["sp_game_id"] if debug_ou else "",
            sport_id = sport_id_int,
        )
        yield _build_match(parsed, markets, sport_slug)

    if inline_count:
        print(f"[sp:{sport_slug}] {inline_count}/{len(raw_items)} used inline fallback")


def fetch_live_stream(
    sport_slug:         str,
    config:             SportConfig,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.3,
    debug_ou:           bool  = False,
) -> Generator[dict, None, None]:
    """Generic live stream — driven by SportConfig."""
    sport_id = str(config.sport_id)
    raw_items = _fetch_live_list(sport_id)
    print(f"[sp:{sport_slug}:live] {len(raw_items)} live (sportId={sport_id})")

    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(
                parsed["sp_game_id"], config.market_ids, debug=debug_ou
            )
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        sport_id_int = parsed.get("sp_sport_id") or config.sport_id
        markets = _parse_markets(
            raw_mkts,
            game_id  = parsed["sp_game_id"] if debug_ou else "",
            sport_id = sport_id_int,
        )
        yield _build_match(parsed, markets, sport_slug, status="live")


# =============================================================================
# BLOCKING WRAPPERS  (used by /direct/* endpoints)
# =============================================================================

def fetch_upcoming(
    sport_slug: str,
    config:     SportConfig,
    **kwargs,
) -> list[dict]:
    return list(fetch_upcoming_stream(sport_slug, config, **kwargs))


def fetch_live(
    sport_slug: str,
    config:     SportConfig,
    **kwargs,
) -> list[dict]:
    return list(fetch_live_stream(sport_slug, config, **kwargs))


def fetch_match_markets(
    game_id:    str | int,
    market_ids: str,
    sport_id:   int = 1,
) -> dict[str, dict[str, float]]:
    """On-demand full market book for one SP game."""
    raw    = _fetch_markets(game_id, market_ids, debug=True)
    result = _parse_markets(raw, game_id=str(game_id), sport_id=sport_id)
    print(f"[sp:match] {game_id}: {len(result)} slugs → {list(result.keys())[:8]}")
    return result