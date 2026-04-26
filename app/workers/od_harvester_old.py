"""
app/workers/od_harvester.py
============================
OdiBets upcoming + live harvester.

Critical API facts confirmed from response inspection:
  1. sport_id must be the STRING SLUG ("soccer", not 1).
  2. The API caps per_page at 25 regardless of what we request.
     meta.per_page reflects the actual page size, NOT our request.
     Pagination must use meta.per_page for the "last page" check — using
     our requested value (200) caused a break after page 1, returning only
     25 of 442 matches per day.
  3. meta.total is reliable for day-level calls (no competition_id).
  4. meta.total = 0 for competition-filtered calls; use "fewer than
     meta.per_page" as the only termination signal for those.
  5. Each day has its own competition list (the overview list is today-only).

Fetch flow:
  For each day:
    Step A – day overview: get that day's competition list + meta.total.
    Step B – paginate all matches:  day&page=N  (no competition_id)
             using ceil(total / actual_per_page) pages.
    Fallback – if day-level returns 0, retry per competition.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as _date, timedelta
from typing import Any, Generator

import httpx

from app.workers.mappers.betika import get_market_slug, normalize_outcome

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# SPORT SLUG MAP  —  value is the exact string sent as sport_id=
# ══════════════════════════════════════════════════════════════════════════════

OD_SPORT_IDS: dict[str, str] = {
    "soccer":            "soccer",
    "basketball":        "basketball",
    "tennis":            "tennis",
    "ice-hockey":        "ice-hockey",
    "rugby":             "rugby",
    "handball":          "handball",
    "table-tennis":      "table-tennis",
    "cricket":           "cricket",
    "volleyball":        "volleyball",
    "baseball":          "baseball",
    "american-football": "american-football",
    "mma":               "mma",
    "boxing":            "boxing",
    "darts":             "darts",
    "esoccer":           "esoccer",
}

# Alternative slugs tried when the primary returns 0 matches
_SLUG_FALLBACKS: dict[str, list[str]] = {
    "american-football": ["americanfootball", "american_football", "nfl"],
    "mma":               ["mixed-martial-arts", "mixedmartialarts", "ufc"],
    "table-tennis":      ["tabletennis", "table_tennis"],
    "ice-hockey":        ["icehockey", "ice_hockey", "hockey"],
}

_NUMERIC_TO_SLUG: dict[str, str] = {
    "1": "soccer",  "2": "basketball", "5": "tennis",  "4": "ice-hockey",
    "12": "rugby",  "6": "handball",   "20": "table-tennis", "21": "cricket",
    "23": "volleyball", "3": "baseball", "11": "american-football",
    "117": "mma",   "10": "boxing",    "22": "darts", "137": "esoccer",
}

def slug_to_od_sport_id(slug: str) -> str:
    return OD_SPORT_IDS.get(slug, slug)

def _resolve_sport(raw: Any, fallback: str) -> str:
    if raw is None:
        return fallback
    s = str(raw).lower().strip()
    if s in OD_SPORT_IDS:
        return s
    return _NUMERIC_TO_SLUG.get(s, fallback)


# ══════════════════════════════════════════════════════════════════════════════
# API + HTTP
# ══════════════════════════════════════════════════════════════════════════════

SBOOK_V1 = "https://api.odi.site/sportsbook/v1"

HEADERS: dict[str, str] = {
    "accept":             "application/json, text/plain, */*",
    "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "authorization":      "Bearer",
    "content-type":       "application/json",
    "origin":             "https://odibets.com",
    "referer":            "https://odibets.com/",
    "user-agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36"
    ),
    "sec-ch-ua":          '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile":   "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "cross-site",
}

_POOL_LIMITS = httpx.Limits(max_connections=150, max_keepalive_connections=60, keepalive_expiry=30.0)
_shared_client: httpx.Client | None = None
_client_lock = threading.Lock()

def _get_client() -> httpx.Client:
    global _shared_client
    if _shared_client is None:
        with _client_lock:
            if _shared_client is None:
                _shared_client = httpx.Client(headers=HEADERS, timeout=20.0, limits=_POOL_LIMITS)
    return _shared_client

_REQUEST_SEMAPHORE = threading.Semaphore(60)

def configure_concurrency(max_concurrent: int = 60) -> None:
    global _REQUEST_SEMAPHORE
    _REQUEST_SEMAPHORE = threading.Semaphore(max_concurrent)


def _get(url: str, params: dict | None = None, timeout: float = 20.0) -> dict | list | None:
    client = _get_client()
    for attempt in range(3):
        with _REQUEST_SEMAPHORE:
            try:
                r = client.get(url, params=params, timeout=timeout)
                if r.status_code in (429, 503):
                    wait = float(r.headers.get("Retry-After", 2 ** (attempt + 1)))
                    logger.warning("OD rate-limited (%s) – waiting %.1fs", r.status_code, wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                logger.warning("OD HTTP %s %s (attempt %d)", e.response.status_code, url, attempt + 1)
            except httpx.TimeoutException:
                logger.warning("OD timeout %s (attempt %d)", url, attempt + 1)
            except Exception as e:
                logger.warning("OD error %s (attempt %d): %s", url, attempt + 1, e)
        if attempt < 2:
            time.sleep(0.5 * (attempt + 1))
    return None


# ══════════════════════════════════════════════════════════════════════════════
# RESPONSE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _data(resp: dict | list) -> dict:
    return (resp.get("data") if isinstance(resp, dict) and isinstance(resp.get("data"), dict) else {})

def _unwrap(resp: dict | list) -> tuple[list[dict], dict]:
    """Return (matches, meta) from any resource=sport response."""
    d = _data(resp)
    matches = d.get("matches") or []
    return (matches if isinstance(matches, list) else []), (d.get("meta") or {})

def _competitions(resp: dict | list) -> list[dict]:
    return _data(resp).get("competitions") or []

def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v or default)
    except (TypeError, ValueError):
        return default

def _unwrap_live(resp: dict | list) -> list[dict]:
    if isinstance(resp, list):
        return resp
    d = _data(resp)
    for k in ("matches", "events", "live", "results"):
        if isinstance(d.get(k), list) and d[k]:
            return d[k]
    if isinstance(resp, dict):
        for k in ("matches", "events", "live", "results", "data"):
            if isinstance(resp.get(k), list) and resp[k]:
                return resp[k]
    return []


# ══════════════════════════════════════════════════════════════════════════════
# BASE PARAMS BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _base(sport_id: str, **extra) -> dict:
    return {
        "resource": "sport", "sport_id": sport_id,
        "sportsbook": "sportsbook", "ua": HEADERS["user-agent"],
        "sub_type_id": "", "competition_id": "", "hour": "",
        "day_tmp": "", "day": "", "country_id": "",
        "sort_by": "", "filter": "", "cs": "", "hs": "",
        **extra,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SLUG PROBE  (auto-detect working slug for sports that return 0)
# ══════════════════════════════════════════════════════════════════════════════

_probed: dict[str, str] = {}   # cache: slug → working api_id

def _probe(slug: str) -> str:
    if slug in _probed:
        return _probed[slug]
    candidates = [OD_SPORT_IDS.get(slug, slug)] + _SLUG_FALLBACKS.get(slug, [])
    today = _date.today().isoformat()
    for candidate in candidates:
        data = _get(SBOOK_V1, params={**_base(candidate), "day": today, "page": 1, "per_page": 25})
        if data:
            matches, meta = _unwrap(data)
            if matches or _safe_int(meta.get("total")) > 0:
                _probed[slug] = candidate
                if candidate != slug:
                    logger.info("OD slug probe: %s → %s", slug, candidate)
                return candidate
    _probed[slug] = slug
    logger.warning("OD slug probe: no working slug for %s (tried %s)", slug, candidates)
    return slug


# ══════════════════════════════════════════════════════════════════════════════
# MARKET PARSING
# ══════════════════════════════════════════════════════════════════════════════

def _parse_specifiers(s: str) -> dict:
    if not s:
        return {}
    out = {}
    for part in str(s).split("|"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out

def _outcomes(mkt: dict) -> list[tuple[dict, str]]:
    ms = str(mkt.get("specifiers") or "").strip()
    results: list[tuple[dict, str]] = []
    for o in (mkt.get("outcomes") or []):
        if isinstance(o, dict):
            results.append((o, ms or str(o.get("specifiers") or "").strip()))
    if results:
        return results
    for line in (mkt.get("lines") or []):
        if not isinstance(line, dict):
            continue
        ls = str(line.get("specifiers") or "").strip()
        for o in (line.get("outcomes") or []):
            if isinstance(o, dict):
                results.append((o, ls or str(o.get("specifiers") or "").strip()))
    if results:
        return results
    for o in (mkt.get("odds") or []):
        if isinstance(o, dict):
            results.append((o, ms or str(o.get("special_bet_value") or o.get("specifiers") or "").strip()))
    return results

def _translate(ds: str, home: str, away: str) -> str:
    ds = ds.lower().strip()
    if not ds or ds in ("1", "x", "2", "yes", "no", "over", "under", "odd", "even", "none"):
        return ds
    for src, tgt in sorted([
        (home.lower().replace(" ", "_"), "1"),
        (away.lower().replace(" ", "_"), "2"),
        (home.lower(), "1"), (away.lower(), "2"), ("draw", "X"),
    ], key=lambda x: -len(x[0])):
        if len(src) > 2 and src in ds:
            ds = ds.replace(src, tgt)
    return ds

def _parse_markets(
    raw: list[dict], sport: str, home: str, away: str
) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    for mkt in raw:
        if not isinstance(mkt, dict) or str(mkt.get("status") or "") == "0":
            continue
        sid  = str(mkt.get("sub_type_id") or mkt.get("type_id") or "")
        name = str(mkt.get("odd_type") or mkt.get("name") or mkt.get("type_name") or "")
        for o, spec in _outcomes(mkt):
            if str(o.get("active") or "") in ("0", "false"): continue
            if str(o.get("status") or "") == "0": continue
            try:
                val = float(o.get("odd_value") or 0)
            except (TypeError, ValueError):
                continue
            if val <= 1.0: continue
            slug = get_market_slug(sport, sid, _parse_specifiers(spec), fallback_name=name)
            display = str(o.get("outcome_key") or o.get("odd_key") or o.get("outcome_name") or o.get("odd_def") or "")
            key = normalize_outcome(sport, _translate(display, home, away))
            result.setdefault(slug, {})[key] = val
    return result


# ══════════════════════════════════════════════════════════════════════════════
# MARKET ENRICHMENT
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_sub_type(sid: str, params: dict, sport: str, home: str, away: str) -> dict:
    data = _get(SBOOK_V1, params={**params, "sub_type_id": sid})
    if not data: return {}
    d = _data(data) or (data if isinstance(data, dict) else {})
    return _parse_markets(d.get("markets") or [], sport, home, away)


def fetch_full_markets_for_match(
    event_id: str | int,
    sport_slug: str = "soccer",
    sub_type_workers: int = 10,
) -> dict[str, dict[str, float]]:
    all_markets: dict[str, dict[str, float]] = {}
    base = {"resource": "sportevent", "id": str(event_id), "category_id": "", "sub_type_id": "",
            "builder": 0, "sportsbook": "sportsbook", "ua": HEADERS["user-agent"]}
    data = _get(SBOOK_V1, params=base)
    if not data or not isinstance(data, dict): return {}
    d     = _data(data) or data
    info  = d.get("info") or {}
    home  = str(info.get("home_team") or "")
    away  = str(info.get("away_team") or "")
    sport = _resolve_sport(info.get("s_binomen") or info.get("sport_id"), sport_slug)

    for slug, outcomes in _parse_markets(d.get("markets") or [], sport, home, away).items():
        all_markets.setdefault(slug, {}).update(outcomes)

    sub_ids = {str(m["sub_type_id"]) for m in (d.get("markets_list") or []) if m.get("sub_type_id")}
    if not sub_ids:
        return all_markets

    with ThreadPoolExecutor(max_workers=min(sub_type_workers, len(sub_ids))) as pool:
        futures = {pool.submit(_fetch_sub_type, sid, base, sport, home, away): sid for sid in sub_ids}
        for f in as_completed(futures):
            try:
                for slug, outcomes in f.result().items():
                    all_markets.setdefault(slug, {}).update(outcomes)
            except Exception as e:
                logger.warning("OD sub_type %s error: %s", futures[f], e)
    return all_markets


# ══════════════════════════════════════════════════════════════════════════════
# MATCH NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _normalise(raw: dict, sport_slug: str, is_live: bool = False) -> dict | None:
    try:
        mid = str(raw.get("parent_match_id") or raw.get("game_id") or raw.get("id") or raw.get("match_id") or "")
        if not mid: return None
        home   = str(raw.get("home_team") or raw.get("home") or "Home")
        away   = str(raw.get("away_team") or raw.get("away") or "Away")
        sport  = _resolve_sport(raw.get("s_binomen") or raw.get("sport_id") or raw.get("sport"), sport_slug)
        score  = str(raw.get("current_score") or raw.get("result") or "")
        parts  = score.split(":") if ":" in score else score.split("-") if "-" in score else []
        bet_st = str(raw.get("bet_status") or raw.get("b_status") or "")

        mkts_raw = raw.get("markets") or raw.get("odds") or []
        markets  = (_parse_markets(mkts_raw, sport, home, away) if isinstance(mkts_raw, list)
                    else mkts_raw if isinstance(mkts_raw, dict) else {})

        if "1x2" not in markets:
            try:
                ho = float(raw.get("home_odd") or raw.get("h_odd") or 0)
                no = float(raw.get("draw_odd") or raw.get("d_odd") or raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or raw.get("a_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    base = "1x2" if sport == "soccer" else f"{sport}_1x2"
                    markets[base] = {k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1}
            except (TypeError, ValueError):
                pass

        return {
            "od_match_id":   mid,
            "od_event_id":   str(raw.get("game_id") or mid),
            "od_parent_id":  mid,
            "sp_game_id":    None,
            "betradar_id":   str(raw.get("betradar_id") or raw.get("sr_id") or mid) or None,
            "home_team":     home,
            "away_team":     away,
            "competition":   str(raw.get("competition_name") or raw.get("competition") or raw.get("league") or ""),
            "category":      str(raw.get("category_name") or raw.get("category") or raw.get("country_name") or ""),
            "sport":         sport,
            "od_sport_id":   sport,
            "start_time":    str(raw.get("start_time") or raw.get("event_date") or raw.get("date") or ""),
            "source":        "odibets",
            "is_live":       is_live,
            "is_suspended":  bet_st in ("STOPPED", "BET_STOP", "SUSPENDED"),
            "match_time":    str(raw.get("match_time") or raw.get("game_time") or raw.get("periodic_time") or ""),
            "event_status":  str(raw.get("event_status") or raw.get("status_desc") or raw.get("status") or ""),
            "bet_status":    bet_st,
            "current_score": score,
            "score_home":    parts[0].strip() if len(parts) >= 2 else None,
            "score_away":    parts[1].strip() if len(parts) >= 2 else None,
            "markets":       markets,
            "market_count":  len(markets),
        }
    except Exception as e:
        logger.debug("OD normalise error: %s | %s", e, str(raw)[:200])
        return None


# ══════════════════════════════════════════════════════════════════════════════
# CORE PAGE FETCHER  — correctly uses meta.per_page, NOT our requested size
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_all_pages(
    sport_id: str,
    day_str: str,
    competition_id: str = "",
    requested_per_page: int = 200,
    max_pages: int = 200,
) -> tuple[list[dict], list[dict]]:
    """
    Fetch all pages for a sport+day (or sport+day+competition).

    Returns (all_matches, competitions_from_first_page).

    KEY FIX: the API ignores our per_page and caps at 25.  We always read
    meta.per_page from the RESPONSE and use that for the page-count math,
    never our requested value.  Without this, pagination stopped after 1 page
    (25 < 200 = True → break), returning only 25 of 442 matches.
    """
    all_matches: list[dict] = []
    first_page_comps: list[dict] = []
    page = 1

    while page <= max_pages:
        params = {
            **_base(sport_id),
            "day":            day_str,
            "competition_id": competition_id,
            "page":           page,
            "per_page":       requested_per_page,
        }
        data = _get(SBOOK_V1, params=params)
        if not data:
            break

        matches, meta = _unwrap(data)
        if not matches:
            break

        all_matches.extend(matches)

        if page == 1:
            first_page_comps = _competitions(data)

        # ── CORRECT termination logic ──────────────────────────────────────
        # Use the page size the API actually returned, not what we requested.
        # The API caps at 25; sending per_page=200 still returns 25.
        actual_per_page = _safe_int(meta.get("per_page"), len(matches))
        if actual_per_page <= 0:
            actual_per_page = len(matches)

        # Signal 1: fewer results than the actual page size → last page
        if len(matches) < actual_per_page:
            break

        # Signal 2: page count covers total (reliable for no-competition calls)
        total = _safe_int(meta.get("total"))
        if total > 0 and page * actual_per_page >= total:
            break

        page += 1

    logger.debug("OD %s %s comp=%s: %d matches in %d pages",
                 sport_id, day_str, competition_id or "all", len(all_matches), page)
    return all_matches, first_page_comps


# ══════════════════════════════════════════════════════════════════════════════
# PER-DAY COMPLETE FETCH  (day-level + competition fallback)
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_day_complete(
    sport_id: str,
    day_str: str,
    concurrent_comps: int = 10,
) -> list[dict]:
    """
    Fetch ALL matches for one sport on one day.

    Step 1: paginate the day-level endpoint (no competition_id).
            Captures the competition list from page 1.
    Step 2: for each competition found, also paginate with competition_id.
            This catches any matches the day-level pager misses (some competitions
            are not included in the aggregate day listing on the API).
    Deduplicate by parent_match_id.
    """
    # Step 1: day-level pagination
    day_matches, comps = _fetch_all_pages(sport_id, day_str)

    seen: set[str] = set()
    result: list[dict] = []

    def _add(batch: list[dict]) -> None:
        for m in batch:
            uid = str(m.get("parent_match_id") or m.get("game_id") or "")
            if uid and uid not in seen:
                seen.add(uid)
                result.append(m)

    _add(day_matches)

    # Step 2: competition-level sweep (catches any gaps)
    comp_ids = [str(c["competition_id"]) for c in comps if c.get("competition_id")]
    if comp_ids:
        with ThreadPoolExecutor(max_workers=min(concurrent_comps, len(comp_ids))) as pool:
            futures = {
                pool.submit(_fetch_all_pages, sport_id, day_str, cid): cid
                for cid in comp_ids
            }
            for f in as_completed(futures):
                try:
                    _add(f.result()[0])
                except Exception as e:
                    logger.warning("OD comp fetch %s %s %s: %s",
                                   sport_id, day_str, futures[f], e)

    return result


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING MATCHES — main public API
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_matches(
    sport_slug: str = "soccer",
    days: int = 30,
    offset: int = 0,
    max_matches: int | None = None,
    fetch_full_markets: bool = True,
    max_workers: int = 12,          # market enrichment concurrency
    concurrent_days: int = 8,       # parallel day fetches
    concurrent_comps: int = 10,     # parallel competition fetches per day
    skip_enrich_threshold: int = 3,
    **kwargs,
) -> list[dict]:
    api_id = _probe(sport_slug)

    # ── Esoccer: single no-day request ────────────────────────────────────
    if sport_slug == "esoccer":
        data = _get(SBOOK_V1, params=_base(api_id))
        if not data: return []
        raw, _ = _unwrap(data)
        return _finalise(raw, sport_slug, fetch_full_markets, max_workers,
                         skip_enrich_threshold, offset, max_matches)

    # ── Build day list ─────────────────────────────────────────────────────
    today = _date.today()
    day_strings = [(today + timedelta(days=i)).isoformat() for i in range(days)]

    # ── Parallel day fetches ───────────────────────────────────────────────
    all_raw: list[dict] = []
    seen_ids: set[str] = set()

    def _collect(batch: list[dict]) -> None:
        for m in batch:
            uid = str(m.get("parent_match_id") or m.get("game_id") or "")
            if uid and uid not in seen_ids:
                seen_ids.add(uid)
                all_raw.append(m)

    with ThreadPoolExecutor(max_workers=min(concurrent_days, len(day_strings))) as executor:
        futures = {
            executor.submit(_fetch_day_complete, api_id, ds, concurrent_comps): ds
            for ds in day_strings
        }
        for f in as_completed(futures):
            try:
                _collect(f.result())
            except Exception as e:
                logger.error("OD day fetch error %s %s: %s", sport_slug, futures[f], e)

    logger.info("OD upcoming %s (%d days): %d matches", sport_slug, days, len(all_raw))
    return _finalise(all_raw, sport_slug, fetch_full_markets, max_workers,
                     skip_enrich_threshold, offset, max_matches)


def _finalise(
    raw: list[dict],
    sport_slug: str,
    fetch_full_markets: bool,
    max_workers: int,
    skip_enrich_threshold: int,
    offset: int,
    max_matches: int | None,
) -> list[dict]:
    # Normalise + deduplicate
    normalised: list[dict] = []
    seen: set[str] = set()
    for r in raw:
        m = _normalise(r, sport_slug)
        if not m: continue
        if m["od_match_id"] not in seen:
            seen.add(m["od_match_id"])
            normalised.append(m)

    # Market enrichment
    if fetch_full_markets and normalised:
        needs = [m for m in normalised if m["market_count"] < skip_enrich_threshold]
        rich  = [m for m in normalised if m["market_count"] >= skip_enrich_threshold]
        if needs:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                enriched = list(pool.map(_enrich, needs))
        else:
            enriched = []
        normalised = enriched + rich

    if offset:       normalised = normalised[offset:]
    if max_matches:  normalised = normalised[:max_matches]
    return normalised


def _enrich(match: dict) -> dict:
    br = match.get("betradar_id")
    if not br: return match
    full = fetch_full_markets_for_match(br, match.get("sport", "soccer"))
    if full:
        match["markets"].update(full)
        match["market_count"] = len(match["markets"])
    return match


# ══════════════════════════════════════════════════════════════════════════════
# LIVE MATCHES
# ══════════════════════════════════════════════════════════════════════════════

def fetch_live_matches(sport_slug: str | None = None) -> list[dict]:
    params: dict[str, Any] = {
        "resource": "live", "sportsbook": "sportsbook",
        "ua": HEADERS["user-agent"], "sub_type_id": "1", "sport_id": "",
    }
    if sport_slug:
        params["sport_id"] = slug_to_od_sport_id(sport_slug)
    data = _get(SBOOK_V1, params=params, timeout=10.0)
    if not data: return []
    matches: list[dict] = []
    for raw in _unwrap_live(data):
        if not isinstance(raw, dict): continue
        sl = _resolve_sport(raw.get("s_binomen") or raw.get("sport_id"), sport_slug or "soccer")
        m = _normalise(raw, sl, is_live=True)
        if m: matches.append(m)
    logger.info("OD live: %d matches (sport=%s)", len(matches), sport_slug or "all")
    return matches


# ══════════════════════════════════════════════════════════════════════════════
# STREAMING + ALIASES
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_stream(sport_slug: str = "soccer", days: int = 30,
                          max_matches: int | None = None,
                          fetch_full_markets: bool = True, **kwargs) -> Generator[dict, None, None]:
    yield from fetch_upcoming_matches(sport_slug=sport_slug, days=days,
                                      fetch_full_markets=fetch_full_markets,
                                      max_matches=max_matches)

def fetch_live_stream(sport_slug: str, fetch_full_markets: bool = True,
                      **kwargs) -> Generator[dict, None, None]:
    for m in fetch_live_matches(sport_slug):
        if fetch_full_markets and m.get("betradar_id"):
            full = fetch_full_markets_for_match(m["betradar_id"], m.get("sport", "soccer"))
            if full:
                m["markets"].update(full)
                m["market_count"] = len(m["markets"])
        yield m

def fetch_upcoming(sport_slug: str = "soccer", days: int = 30,
                   fetch_full_markets: bool = True, **kwargs) -> list[dict]:
    return fetch_upcoming_matches(sport_slug, days=days,
                                  fetch_full_markets=fetch_full_markets, **kwargs)

def fetch_live(sport_slug: str | None = None, **kwargs) -> list[dict]:
    return fetch_live_matches(sport_slug)

# ── Stubs ─────────────────────────────────────────────────────────────────────
class OdiBetsLivePoller:
    def __init__(self, redis_client: Any, interval: float = 2.0):
        self.redis = redis_client; self.interval = interval; self._running = False
    def start(self): self._running = True
    def stop(self):  self._running = False
    @property
    def alive(self) -> bool: return False

_live_poller = None
def get_live_poller(): return _live_poller
def init_live_poller(redis_client: Any, interval: float = 2.0):
    global _live_poller
    if _live_poller is None:
        _live_poller = OdiBetsLivePoller(redis_client, interval); _live_poller.start()
    return _live_poller
def get_cached_upcoming(r: Any, s: str) -> list[dict] | None: return None
def cache_upcoming(r: Any, s: str, m: list[dict], ttl: int = 300) -> None: pass
def get_cached_live(r: Any, i: Any) -> list[dict] | None: return None

class OdiBetsHarvesterPlugin:
    bookie_id = "odibets"; bookie_name = "OdiBets"
    sport_slugs = list(OD_SPORT_IDS.keys())
    def fetch_upcoming(self, sport_slug: str, days: int = 30, **kwargs) -> list[dict]:
        return fetch_upcoming_matches(sport_slug, days=days, **kwargs)
    def fetch_live(self, sport_slug: str | None = None) -> list[dict]:
        return fetch_live_matches(sport_slug)

__all__ = [
    "fetch_upcoming_matches", "fetch_live_matches",
    "fetch_upcoming_stream", "fetch_live_stream",
    "fetch_full_markets_for_match", "fetch_upcoming", "fetch_live",
    "OdiBetsHarvesterPlugin", "OD_SPORT_IDS",
    "slug_to_od_sport_id", "configure_concurrency",
]