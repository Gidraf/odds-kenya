"""
app/workers/sp_harvester.py
============================
Sportpesa Kenya harvester — modular edition.

Imports from
────────────
  app.workers.sp_mapper        → normalize_sp_market
  app.workers.canonical_mapper → normalize_outcome

Public API
──────────
  fetch_upcoming(sport_slug, ...)  → list[dict]         blocking, returns all matches
  fetch_live(sport_slug, ...)      → list[dict]         blocking
  fetch_upcoming_stream(...)       → Generator[dict]    yields one match at a time (SSE)
  fetch_live_stream(...)           → Generator[dict]    yields one match at a time (SSE)
  fetch_match_markets(game_id)     → dict               on-demand market book
  fetch_sport_ids()                → dict               sport → live event count

Key fixes in this version
──────────────────────────
  specValue=0 bug
    ─────────────
    Asian handicap market 51 has specValue=0 (the "level ball" line).
    The previous code used:
      spec_val = mkt.get("specValue") or mkt.get("spec")
    This evaluates 0 as falsy, drops the specValue, and the slug becomes
    "asian_handicap" (no line) instead of "asian_handicap_0".
    Fix: use `is None` check — not a bool/or test.

  Generator / streaming support
    ─────────────────────────────
    fetch_upcoming_stream() and fetch_live_stream() are Python generators.
    They yield each normalised match dict as soon as _fetch_markets() returns
    for that match, instead of collecting everything first.
    The SSE endpoints in sp_module.py iterate these generators and push
    each match to the browser immediately — giving real-time progress.

  Debug mode in _fetch_markets / _parse_markets
    ───────────────────────────────────────────
    Pass debug=True to _fetch_markets to get raw-response logging.
    Pass game_id to _parse_markets to include it in debug output.
    The /api/sp/debug/markets/<id> endpoint uses this.

Market IDs requested (33 total — full football + esoccer set)
──────────────────────────────────────────────────────────────
  Core 1X2 + result:   1, 10, 46, 47
  BTTS combos:         43, 29, 386
  O/U goals (main):    52, 18          ← multi-line via specValue 0.5…5.5
  Team goals:          353, 352
  Result + O/U:        208
  Exact/Groups:        258, 202, 207
  Correct score:       332
  Handicap:            51, 53, 55
  First team/special:  45, 41
  HT 1X2:             42, 60
  HT O/U:             15, 54, 68
  HT specials:         44, 328, 203
  Corners/bookings:    162, 166, 136, 139

Normalised match output shape
──────────────────────────────
{
  "betradar_id":  str,
  "sp_game_id":   str,
  "home_team":    str,
  "away_team":    str,
  "start_time":   str | None,
  "competition":  str,
  "sport":        str,
  "source":       "sportpesa",
  "status":       "upcoming" | "live",
  "markets": {
    "1x2":                        {"1": 1.86, "X": 4.10, "2": 3.90},
    "over_under_goals_1.25":      {"over": 1.10, "under": 6.40},
    "over_under_goals_1.5":       {"over": 1.16, "under": 4.70},
    "over_under_goals_2.5":       {"over": 1.54, "under": 2.37},
    ...
    "btts":                       {"yes": 1.53, "no": 2.35},
    "asian_handicap_-0.75":       {"1": 1.92, "2": 1.67},
    "asian_handicap_0":           {"1": 1.35, "2": 2.60},   ← specValue=0 fixed
    "first_half_btts":            {"yes": 3.45, "no": 1.26},
    "ht_ft":                      {"1/1": 2.50, "1/X": 13.0, ...},
    "highest_scoring_half":       {"1st": 2.95, "2nd": 2.02, "equal": 3.75},
    ...
  },
  "market_count": int,
  "harvested_at": str,
}
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Generator, Iterator

import requests

# ── Modular imports ────────────────────────────────────────────────────────────
from app.workers.sp_mapper        import normalize_sp_market
from app.workers.canonical_mapper import normalize_outcome

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

# 33 market IDs — explicit list (more reliable than "all" which sometimes returns nothing)
_CORE_MARKET_IDS = (
    "10,1,46,47,"         # 1X2, Double Chance, Draw No Bet
    "43,29,386,"          # BTTS, BTTS+Result
    "52,18,"              # O/U Goals (main — multiple lines via specValue)
    "353,352,"            # Home / Away team goals
    "208,"                # Result + O/U
    "258,202,207,"        # Exact goals, Goal groups, Highest scoring half
    "332,"                # Correct Score
    "51,53,"              # Asian HC FT + HT  (specValue=0 = level ball line)
    "55,"                 # Euro HC
    "45,41,"              # Odd/Even, First Team to Score
    "42,60,"              # HT 1X2
    "15,54,68,"           # HT O/U
    "44,328,203,"         # HT/FT, HT BTTS, HT Correct Score
    "162,166,"            # Corners
    "136,139"             # Bookings
).replace("\n", "").replace(" ", "")

SP_SPORT_ID: dict[str, str] = {
    "soccer":           "1",
    "football":         "1",
    "esoccer":          "126",
    "efootball":        "126",
    "e-football":       "126",
    "virtual-football": "126",
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

_ESOCCER_IDS = {"126"}


# =============================================================================
# HTTP
# =============================================================================

def _get(
    path: str,
    params: dict | None = None,
    timeout: int = 20,
) -> tuple[Any, dict]:
    """GET → (json_body | None, response_headers_dict)."""
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
        print(f"[sp] JSON decode {url}: {e}")
        return None, {}
    except Exception as exc:
        print(f"[sp] request error {url}: {exc}")
        return None, {}


def _parse_content_range(header: str | None) -> int | None:
    """Content-Range: 0-14/24  →  24 (total items on server)."""
    if not header:
        return None
    try:
        return int(header.split("/")[-1].strip())
    except (IndexError, ValueError):
        return None


# =============================================================================
# Raw SP API fetchers
# =============================================================================

def _fetch_upcoming_page(
    sport_id: str,
    ts_start: int,
    ts_end: int,
    page_size: int,
    offset: int,
) -> tuple[list[dict], int | None]:
    """One page of upcoming listings.  Returns (items, total_from_server)."""
    is_esoccer = sport_id in _ESOCCER_IDS
    raw, headers = _get("/api/upcoming/games", params={
        "type":           "prematch",
        "sportId":        sport_id,
        "section":        "upcoming",
        "markets_layout": "single" if is_esoccer else "multiple",
        "o":              "leagues",
        "pag_count":      str(page_size),
        "pag_min":        str(offset + 1),   # SP uses 1-based
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


def _parse_markets_response(raw: Any, gid: str) -> list[dict]:
    """
    Extract the market list from a /api/games/markets response.
    Handles 4 shapes; returns empty list if no known shape matches.
    """
    if raw is None:
        return []

    if isinstance(raw, dict):
        # Shape A — game_id as string key  {"7966057": [...]}
        if gid in raw and isinstance(raw[gid], list):
            return raw[gid]
        # Shape A — game_id as integer key
        try:
            ikey = int(gid)
            if ikey in raw and isinstance(raw[ikey], list):
                return raw[ikey]
        except (ValueError, TypeError):
            pass
        # Shape B — nested under "data" or "result"  {"data": {"7966057": [...]}}
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
        # Last resort: first non-empty list value
        for v in raw.values():
            if isinstance(v, list) and v:
                return v

    # Shape C — already a flat list  [{id:10,...}, {id:52,...}, ...]
    if isinstance(raw, list):
        return raw

    return []


def _is_from_markets_endpoint(result: list[dict], mkt_list_from_inline: list[dict]) -> bool:
    """
    Return True if `result` came from /api/games/markets (i.e. is the full book)
    rather than being literally identical to the inline listing data.

    We distinguish by market count: the inline listing embeds at most ~5 markets
    (usually just O/U + BTTS).  The markets endpoint returns everything that match
    offers — for some lower-league games this is genuinely only specValue=0 entries,
    but there are typically 15+ of them.

    Note: specValue=0 for ALL markets is VALID — it means SP doesn't offer
    multi-line O/U for this fixture.  That is correct data, not an error.
    Only retry when the response is completely empty (network/rate-limit failure).
    """
    return len(result) >= len(mkt_list_from_inline)


def _fetch_markets(
    game_id:         str | int,
    debug:           bool = False,
    max_tries:       int  = 2,
    inline_mkt_count: int = 0,
) -> list[dict]:
    """
    Fetch the full market book for one SP game from /api/games/markets.

    RETRY POLICY (corrected)
    ─────────────────────────
    Only retry when the response is EMPTY (network failure, rate-limit 429/503).

    Do NOT retry when SP returns non-empty data that contains only specValue=0
    markets — this is valid!  Many lower-league, esoccer, and youth matches
    genuinely do not offer multi-line O/U.  The previous code retried 3×
    (burning 0+1+3 = 4 seconds per match) before accepting the same data,
    making 147-match streams take 10+ minutes and triggering SSE timeouts.

    Behaviour:
      • Empty response  → retry (up to max_tries=2)
      • Non-empty       → accept immediately (even if all specValue=0)
      • HTTP error      → retry

    The caller (_fetch_upcoming_stream etc.) still falls back to _inline_mkts
    if ALL attempts return empty.

    Response shapes handled (see _parse_markets_response):
      A  { "7966057": [{id, specValue, name, selections}, ...] }   ← most common
      B  { "data": { "7966057": [...] } }
      C  [ {id, name, selections} ]
      D  { "markets": [...] }
    """
    gid     = str(game_id)
    backoff = [0, 1.5]     # seconds before each attempt (shorter — we only retry on empty)

    for attempt in range(max_tries):
        if attempt > 0:
            delay = backoff[min(attempt, len(backoff) - 1)]
            if debug:
                print(f"[sp:mkts] retry {attempt}/{max_tries - 1} "
                      f"game={gid} (wait {delay}s — empty response)")
            time.sleep(delay)

        raw, headers = _get("/api/games/markets", params={
            "games":   gid,
            "markets": _CORE_MARKET_IDS,
        })

        # Detect HTTP-level rate limiting
        status = headers.get("x-rate-limit-remaining") or headers.get("X-RateLimit-Remaining")
        if status == "0":
            print(f"[sp:mkts] rate-limit remaining=0 game={gid} — waiting 2s")
            time.sleep(2.0)

        if debug:
            t = type(raw).__name__
            k = list(raw.keys()) if isinstance(raw, dict) else len(raw) if isinstance(raw, list) else "N/A"
            print(f"[sp:mkts] attempt={attempt} game={gid} type={t} keys={k}")

        result = _parse_markets_response(raw, gid)

        if not result:
            # Truly empty — network error, 429, or malformed response
            print(f"[sp:mkts] empty game={gid} attempt={attempt} — will retry")
            continue

        # ── Accept any non-empty response ─────────────────────────────────
        # SP returns only specValue=0 markets for many legitimate matches —
        # this is correct data (no multi-line O/U offered), NOT an error.
        ou_keys = [m.get("id") for m in result if m.get("id") in (52, 18)]
        any_multiline = any(
            m.get("specValue") not in (0, None)
            for m in result
        )
        if debug:
            print(f"[sp:mkts] OK game={gid}  {len(result)} mkts  "
                  f"ou_present={'yes' if ou_keys else 'no'}  "
                  f"multi_line={'yes' if any_multiline else 'no (all specValue=0)'}")

        return result

    print(f"[sp:mkts] all {max_tries} attempts empty for game={gid} — using inline fallback")
    return []


# =============================================================================
# Parsers
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
                if t > 1_000_000_000_000:   # milliseconds → seconds
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

    comp_raw = item.get("competition") or item.get("league") or {}
    competition = _str_field(comp_raw) or _str_field(
        item.get("leagueName") or item.get("competitionName") or ""
    )

    sport_raw  = item.get("sport") or {}
    sport_name = _str_field(sport_raw)
    # Numeric sport ID for sport-aware market mapping (e.g. ID 52 = goals vs points)
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
        "sp_sport_id":  sp_sport_id,   # int — passed to normalize_sp_market
        "_inline_mkts": inline,
    }


def _parse_markets(
    raw_list:  list[dict],
    game_id:   str | None = None,
    debug_ou:  bool       = False,
    sport_id:  int        = 1,
) -> dict[str, dict[str, float]]:
    """
    Convert SP market list  →  { canonical_slug: { outcome_key: float } }.

    ┌─ specValue=0 handling ──────────────────────────────────────────────────
    │ Asian Handicap specValue=0 is the "level ball" line (neither team gets
    │ a head start).  JSON delivers this as integer 0.
    │
    │ WRONG (old code):
    │   spec_val = mkt.get("specValue") or mkt.get("spec")
    │   → 0 is falsy, falls through to None, slug becomes "asian_handicap"
    │
    │ CORRECT (this code):
    │   spec_val = mkt.get("specValue")         # may be 0, 2.5, None …
    │   if spec_val is None:                    # explicit None check
    │       spec_val = mkt.get("spec") or mkt.get("handicap")
    │
    │ Result: "asian_handicap_0" stored correctly. ✓
    └─────────────────────────────────────────────────────────────────────────

    ┌─ O/U lines ─────────────────────────────────────────────────────────────
    │ Market 52 appears once per line (e.g. specValue=2.5, 2.75, 3.0 …).
    │ Each produces its own key: "over_under_goals_2.5", "over_under_goals_3"
    │ The frontend byBase() finds them all with the "over_under_goals" prefix.
    └─────────────────────────────────────────────────────────────────────────
    """
    markets: dict[str, dict[str, float]] = {}

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

        # ── specValue extraction — MUST use is None, NOT or/bool ────────────
        # specValue=0 is a valid line (Asian HC level ball).
        # Using `or` would treat 0 as falsy and lose the line.
        spec_val = mkt.get("specValue")
        if spec_val is None:
            # Only fall through when specValue is truly absent (None),
            # not when it is 0 or any other falsy numeric value.
            spec_val = mkt.get("spec") or mkt.get("handicap")

        mkt_key = normalize_sp_market(mkt_id, spec_val, sport_id)
        markets.setdefault(mkt_key, {})

        # Debug O/U
        if debug_ou and mkt_id in (52, 18, 56):
            print(f"[sp:ou] game={game_id} mkt_id={mkt_id} "
                  f"specValue={spec_val!r} → slug={mkt_key!r}")

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

            # Skip odds ≤ 1.0 (suspended / unavailable)
            if price <= 1.0:
                continue

            out_key = normalize_outcome(mkt_key, short)

            # Keep highest price when same outcome appears multiple times
            if price > markets[mkt_key].get(out_key, 0.0):
                markets[mkt_key][out_key] = round(price, 3)

    result = {k: v for k, v in markets.items() if v}

    if debug_ou:
        ou_keys = [k for k in result if k.startswith("over_under_goals")]
        print(f"[sp:ou] game={game_id} O/U slugs found: {ou_keys}")

    return result


def _build_match(
    parsed:     dict,
    markets:    dict,
    sport_slug: str,
    status:     str = "upcoming",
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
        "sp_sport_id":  parsed.get("sp_sport_id", 1),
        "source":       "sportpesa",
        "status":       status,
        "markets":      markets,
        "market_count": len(markets),
        "harvested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# =============================================================================
# Raw item collector (shared by blocking + streaming paths)
# =============================================================================

def _collect_raw_items(
    sport_id:   str,
    days:       int,
    page_size:  int,
    max_items:  int | None,
) -> list[dict]:
    """
    Page through SP upcoming API and return all raw listing items.
    Uses Content-Range header to know when to stop.
    """
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
# Public: blocking fetch
# =============================================================================

def fetch_upcoming(
    sport_slug:          str,
    days:                int   = 3,
    page_size:           int   = 30,
    fetch_full_markets:  bool  = True,
    max_matches:         int | None = None,
    sleep_between:       float = 0.3,   # 0.3s ≈ 3 req/s — well under SP's ~30 req/min limit
    debug_ou:            bool  = False,
) -> list[dict]:
    """
    Fetch upcoming matches (blocking).  Returns list of normalised match dicts.
    fetch_full_markets=True is required to get O/U and all 33 markets.

    sleep_between default is 0.5s.  SP's /api/games/markets endpoint rate-limits
    at approximately 30 requests/minute.  At 0.5s sleep + ~0.3s request time we
    stay well under that limit.  You can lower it to 0.2s for fast local testing.
    """
    sport_id = SP_SPORT_ID.get(sport_slug.lower().replace(" ", "-"))
    if not sport_id:
        print(f"[sp] unknown sport: {sport_slug!r}")
        return []

    raw_items = _collect_raw_items(sport_id, days, page_size, max_matches)
    print(f"[sp] {sport_slug}: {len(raw_items)} raw items (sportId={sport_id})")

    inline_fallback_count = 0
    results = []
    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"], debug=debug_ou)
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
                inline_fallback_count += 1
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts, game_id=parsed["sp_game_id"],
                                  debug_ou=debug_ou,
                                  sport_id=parsed.get("sp_sport_id", 1))
        results.append(_build_match(parsed, markets, sport_slug))

    if inline_fallback_count:
        print(f"[sp] WARNING: {inline_fallback_count}/{len(raw_items)} matches "
              f"used inline fallback (no full market data from SP API)")
    print(f"[sp] {sport_slug}: {len(results)} normalised  full_mkts={fetch_full_markets}")
    return results


def fetch_live(
    sport_slug:          str,
    fetch_full_markets:  bool  = True,
    sleep_between:       float = 0.3,
    debug_ou:            bool  = False,
) -> list[dict]:
    """Fetch live matches (blocking)."""
    sport_id = SP_SPORT_ID.get(sport_slug.lower().replace(" ", "-"))
    if not sport_id:
        return []

    raw_items = _fetch_live_list(sport_id)
    print(f"[sp:live] {sport_slug}: {len(raw_items)} live (sportId={sport_id})")

    inline_fallback_count = 0
    results = []
    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue
        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"], debug=debug_ou)
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
                inline_fallback_count += 1
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]
        markets = _parse_markets(raw_mkts, game_id=parsed["sp_game_id"],
                                  debug_ou=debug_ou,
                                  sport_id=parsed.get("sp_sport_id", 1))
        results.append(_build_match(parsed, markets, sport_slug, status="live"))

    if inline_fallback_count:
        print(f"[sp:live] WARNING: {inline_fallback_count} fallbacks to inline mkts")
    return results


# =============================================================================
# Public: streaming generators (used by SSE endpoints)
# =============================================================================

def fetch_upcoming_stream(
    sport_slug:          str,
    days:                int   = 3,
    page_size:           int   = 30,
    fetch_full_markets:  bool  = True,
    max_matches:         int | None = None,
    sleep_between:       float = 0.3,   # 0.3s ≈ 3 req/s — fast enough for SSE, safe for SP rate limit
    debug_ou:            bool  = False,
) -> Generator[dict, None, None]:
    """
    Generator version of fetch_upcoming.  Yields each normalised match dict
    as soon as its market book is fetched.

    With the corrected _fetch_markets (no inline-only retries), each match
    takes ~0.3-0.6s (sleep + request).  150 matches ≈ 60-90s total — well
    within SSE connection timeout.

    SSE TIMEOUT NOTE:
    ─────────────────
    Nginx default proxy_read_timeout is 60s.  For large fetches (150+ matches)
    add a heartbeat comment in sp_module.py's stream_upcoming endpoint, or
    configure nginx:
        proxy_read_timeout 300s;
        proxy_buffering off;
    """
    sport_id = SP_SPORT_ID.get(sport_slug.lower().replace(" ", "-"))
    if not sport_id:
        print(f"[sp:stream] unknown sport: {sport_slug!r}")
        return

    raw_items = _collect_raw_items(sport_id, days, page_size, max_matches)
    print(f"[sp:stream] {sport_slug}: {len(raw_items)} raw items")

    inline_count = 0
    for item in raw_items:
        parsed = _parse_match_item(item)
        if not parsed:
            continue

        if fetch_full_markets and parsed["sp_game_id"]:
            raw_mkts = _fetch_markets(parsed["sp_game_id"], debug=debug_ou)
            if not raw_mkts:
                raw_mkts = parsed["_inline_mkts"]
                inline_count += 1
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]

        markets = _parse_markets(raw_mkts, game_id=parsed["sp_game_id"],
                                  debug_ou=debug_ou,
                                  sport_id=parsed.get("sp_sport_id", 1))
        yield _build_match(parsed, markets, sport_slug)

    if inline_count:
        print(f"[sp:stream] WARNING: {inline_count}/{len(raw_items)} used inline "
              f"fallback — consider increasing sleep_between or checking rate limits")


def fetch_live_stream(
    sport_slug:          str,
    fetch_full_markets:  bool  = True,
    sleep_between:       float = 0.3,
    debug_ou:            bool  = False,
) -> Generator[dict, None, None]:
    """
    Generator version of fetch_live.  Yields each live match as it's fetched.
    """
    sport_id = SP_SPORT_ID.get(sport_slug.lower().replace(" ", "-"))
    if not sport_id:
        return

    raw_items = _fetch_live_list(sport_id)
    print(f"[sp:live:stream] {sport_slug}: {len(raw_items)} live")

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
        markets = _parse_markets(raw_mkts, game_id=parsed["sp_game_id"],
                                  debug_ou=debug_ou,
                                  sport_id=parsed.get("sp_sport_id", 1))
        yield _build_match(parsed, markets, sport_slug, status="live")


# =============================================================================
# Public: on-demand single-match market book
# =============================================================================

def fetch_match_markets(game_id: str | int) -> dict[str, dict[str, float]]:
    """
    Fetch the complete normalised market book for one SP game ID.
    Used by /api/sp/match/<game_id>/markets and the debug endpoint.
    """
    return _parse_markets(
        _fetch_markets(game_id, debug=False),
        game_id=str(game_id),
    )


# =============================================================================
# Public: sport event counts (for status display)
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