"""
app/workers/od_harvester.py
============================
OdiBets upcoming + live harvester.

Fetch strategy (confirmed from API evidence):
  sport_id = STRING SLUG  ("soccer", "basketball", …) — never an integer.

  Day-level pagination (fast path):
    GET /v1?sport_id=soccer&day=2026-04-26&page=1&per_page=200
    → meta.total is the real total for that day (e.g. 442).
    → Paginate until (page * 200 >= total) OR (results < per_page).
    → Max pages capped at MAX_PAGES_PER_DAY (safety).

  Esoccer: single request, no day/page needed.

  Competition-level (used only when day-level returns 0 to rescue missing data):
    GET /v1?sport_id=soccer&day=2026-04-26&competition_id=8&page=1&per_page=200
    → meta.total always 0 for these; use "fewer than per_page" termination.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as _date, timedelta
from typing import Any, Generator

import httpx

from app.workers.mappers.betika import get_market_slug, normalize_outcome

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# SPORT SLUG MAP  (value = exact string sent as sport_id= in the API)
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

# Alternative slugs to try when the primary slug returns 0 matches.
# The API may use a different binomen for some sports.
_SLUG_FALLBACKS: dict[str, list[str]] = {
    "american-football": ["americanfootball", "american_football", "nfl"],
    "mma":               ["mixed-martial-arts", "mixedmartialarts", "ufc"],
    "table-tennis":      ["tabletennis", "table_tennis", "pingpong"],
    "ice-hockey":        ["icehockey", "ice_hockey", "hockey"],
}

# Reverse map: numeric IDs → slug (for raw match data that still uses ints)
_NUMERIC_TO_SLUG: dict[str, str] = {
    "1": "soccer", "2": "basketball", "5": "tennis", "4": "ice-hockey",
    "12": "rugby", "6": "handball", "20": "table-tennis", "21": "cricket",
    "23": "volleyball", "3": "baseball", "11": "american-football",
    "117": "mma", "10": "boxing", "22": "darts", "137": "esoccer",
}

def slug_to_od_sport_id(slug: str) -> str:
    return OD_SPORT_IDS.get(slug, slug)

def _resolve_sport(raw_sport: Any, fallback_slug: str) -> str:
    if raw_sport is None:
        return fallback_slug
    s = str(raw_sport).lower().strip()
    if s in OD_SPORT_IDS:
        return s
    if s in _NUMERIC_TO_SLUG:
        return _NUMERIC_TO_SLUG[s]
    return fallback_slug


# ══════════════════════════════════════════════════════════════════════════════
# API ENDPOINTS + HEADERS
# ══════════════════════════════════════════════════════════════════════════════

API_BASE = "https://api.odi.site"
SBOOK_V1 = f"{API_BASE}/sportsbook/v1"

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

# ── Shared connection pool ────────────────────────────────────────────────────
_POOL_LIMITS = httpx.Limits(
    max_connections=150,
    max_keepalive_connections=60,
    keepalive_expiry=30.0,
)
_shared_client: httpx.Client | None = None
_client_lock = threading.Lock()

def _get_client() -> httpx.Client:
    global _shared_client
    if _shared_client is None:
        with _client_lock:
            if _shared_client is None:
                _shared_client = httpx.Client(
                    headers=HEADERS,
                    timeout=20.0,
                    limits=_POOL_LIMITS,
                )
    return _shared_client


# ── Global request semaphore ─────────────────────────────────────────────────
_REQUEST_SEMAPHORE = threading.Semaphore(60)

def configure_concurrency(max_concurrent_requests: int = 60) -> None:
    """Call once before harvesting to cap total in-flight HTTP requests."""
    global _REQUEST_SEMAPHORE
    _REQUEST_SEMAPHORE = threading.Semaphore(max_concurrent_requests)


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
            except httpx.HTTPStatusError as exc:
                logger.warning("OD HTTP %s %s (attempt %d)", exc.response.status_code, url, attempt + 1)
            except httpx.TimeoutException:
                logger.warning("OD timeout %s (attempt %d)", url, attempt + 1)
            except Exception as exc:
                logger.warning("OD request error %s (attempt %d): %s", url, attempt + 1, exc)
        if attempt < 2:
            time.sleep(0.5 * (attempt + 1))
    return None


# ══════════════════════════════════════════════════════════════════════════════
# RESPONSE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _inner(data: dict | list) -> dict:
    """Safely extract data.data as a dict."""
    if not isinstance(data, dict):
        return {}
    inner = data.get("data")
    return inner if isinstance(inner, dict) else {}

def _unwrap_matches_and_meta(data: dict | list) -> tuple[list[dict], dict]:
    d = _inner(data)
    matches = d.get("matches") or []
    return (matches if isinstance(matches, list) else []), (d.get("meta") or {})

def _unwrap_competitions(data: dict | list) -> list[dict]:
    return _inner(data).get("competitions") or []

def _unwrap_live(data: dict | list) -> list[dict]:
    if isinstance(data, list):
        return data
    d = _inner(data)
    for key in ("matches", "events", "live", "results"):
        if isinstance(d.get(key), list) and d[key]:
            return d[key]
    if isinstance(data, dict):
        for key in ("matches", "events", "live", "results", "data"):
            if isinstance(data.get(key), list) and data[key]:
                return data[key]
    return []

def _meta_total(meta: dict) -> int:
    """Parse meta.total robustly (may be int, string, or 0/missing)."""
    try:
        return int(meta.get("total") or 0)
    except (TypeError, ValueError):
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# MARKET PARSING
# ══════════════════════════════════════════════════════════════════════════════

def _parse_od_specifiers(spec_string: str) -> dict:
    if not spec_string:
        return {}
    parsed = {}
    for part in str(spec_string).split("|"):
        if "=" in part:
            k, v = part.split("=", 1)
            parsed[k.strip()] = v.strip()
    return parsed

def _flat_outcomes_from_market(mkt: dict) -> list[tuple[dict, str]]:
    results: list[tuple[dict, str]] = []
    market_spec = str(mkt.get("specifiers") or "").strip()

    direct = mkt.get("outcomes")
    if isinstance(direct, list) and direct:
        for o in direct:
            if isinstance(o, dict):
                results.append((o, market_spec or str(o.get("specifiers") or "").strip()))
        return results

    lines = mkt.get("lines")
    if isinstance(lines, list):
        for line in lines:
            if not isinstance(line, dict):
                continue
            ls = str(line.get("specifiers") or "").strip()
            for o in (line.get("outcomes") or []):
                if isinstance(o, dict):
                    results.append((o, ls or str(o.get("specifiers") or "").strip()))
        return results

    for o in (mkt.get("odds") or []):
        if isinstance(o, dict):
            spec = market_spec or str(o.get("special_bet_value") or o.get("specifiers") or "").strip()
            results.append((o, spec))
    return results

def _translate_team_names(display_str: str, home: str, away: str) -> str:
    ds = display_str.lower().strip()
    if not ds:
        return ""
    if ds in ("1", "x", "2", "yes", "no", "over", "under", "odd", "even", "none"):
        return ds
    replacements = [
        (home.lower().replace(" ", "_"), "1"),
        (away.lower().replace(" ", "_"), "2"),
        (home.lower(), "1"),
        (away.lower(), "2"),
        ("draw", "X"),
    ]
    replacements.sort(key=lambda x: len(x[0]), reverse=True)
    for t_name, t_val in replacements:
        if len(t_name) > 2 and t_name in ds:
            ds = ds.replace(t_name, t_val)
    return ds

def _parse_all_markets(
    markets_raw: list[dict],
    sport_slug: str,
    home_team: str,
    away_team: str,
) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    for mkt in markets_raw:
        if not isinstance(mkt, dict):
            continue
        if str(mkt.get("status") or "") == "0":
            continue
        sid      = str(mkt.get("sub_type_id") or mkt.get("type_id") or "")
        mkt_name = str(mkt.get("odd_type") or mkt.get("name") or mkt.get("type_name") or "")
        for o, spec_str in _flat_outcomes_from_market(mkt):
            if str(o.get("active") or "") in ("0", "false"):
                continue
            if str(o.get("status") or "") == "0":
                continue
            try:
                val = float(o.get("odd_value") or 0)
            except (TypeError, ValueError):
                continue
            if val <= 1.0:
                continue
            slug = get_market_slug(sport_slug, sid, _parse_od_specifiers(spec_str), fallback_name=mkt_name)
            display_str = str(
                o.get("outcome_key") or o.get("odd_key") or
                o.get("outcome_name") or o.get("odd_def") or ""
            )
            outcome_key = normalize_outcome(
                sport_slug, _translate_team_names(display_str, home_team, away_team)
            )
            result.setdefault(slug, {})[outcome_key] = val
    return result


# ══════════════════════════════════════════════════════════════════════════════
# MARKET ENRICHMENT
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_one_sub_type(sid: str, base_params: dict, sport_slug: str, home: str, away: str) -> dict:
    data = _get(SBOOK_V1, params={**base_params, "sub_type_id": sid})
    if not data or not isinstance(data, dict):
        return {}
    d = _inner(data) or data
    markets = d.get("markets") or []
    return _parse_all_markets(markets, sport_slug, home, away) if markets else {}


def fetch_full_markets_for_match(
    event_id: str | int,
    sport_slug: str = "soccer",
    sub_type_workers: int = 8,
) -> dict[str, dict[str, float]]:
    all_markets: dict[str, dict[str, float]] = {}
    base_params = {
        "resource": "sportevent", "id": str(event_id),
        "category_id": "", "sub_type_id": "", "builder": 0,
        "sportsbook": "sportsbook", "ua": HEADERS["user-agent"],
    }
    data = _get(SBOOK_V1, params=base_params)
    if not data or not isinstance(data, dict):
        return {}

    d         = _inner(data) or data
    info      = d.get("info") or {}
    home      = str(info.get("home_team") or "")
    away      = str(info.get("away_team") or "")
    sl        = _resolve_sport(info.get("s_binomen") or info.get("sport_id"), sport_slug)

    for slug, outcomes in _parse_all_markets(d.get("markets") or [], sl, home, away).items():
        all_markets.setdefault(slug, {}).update(outcomes)

    sub_type_ids = {str(m["sub_type_id"]) for m in (d.get("markets_list") or []) if m.get("sub_type_id")}
    if not sub_type_ids:
        return all_markets

    with ThreadPoolExecutor(max_workers=min(sub_type_workers, len(sub_type_ids))) as pool:
        futures = {pool.submit(_fetch_one_sub_type, sid, base_params, sl, home, away): sid for sid in sub_type_ids}
        for future in as_completed(futures):
            try:
                for slug, outcomes in future.result().items():
                    all_markets.setdefault(slug, {}).update(outcomes)
            except Exception as exc:
                logger.warning("OD sub_type fetch error sid=%s: %s", futures[future], exc)

    return all_markets


# ══════════════════════════════════════════════════════════════════════════════
# MATCH NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_match(raw: dict, sport_slug: str, is_live: bool = False) -> dict | None:
    try:
        match_id = str(
            raw.get("parent_match_id") or raw.get("game_id") or
            raw.get("id") or raw.get("match_id") or ""
        )
        if not match_id:
            return None

        betradar_id = str(raw.get("betradar_id") or raw.get("sr_id") or match_id) or None
        home        = str(raw.get("home_team") or raw.get("home") or "Home")
        away        = str(raw.get("away_team") or raw.get("away") or "Away")
        competition = str(raw.get("competition_name") or raw.get("competition") or raw.get("league") or "")
        category    = str(raw.get("category_name") or raw.get("category") or raw.get("country_name") or "")
        sl          = _resolve_sport(raw.get("s_binomen") or raw.get("sport_id") or raw.get("sport"), sport_slug)
        start_time  = str(raw.get("start_time") or raw.get("event_date") or raw.get("date") or "")
        score       = str(raw.get("current_score") or raw.get("result") or "")
        match_time  = str(raw.get("match_time") or raw.get("game_time") or raw.get("periodic_time") or "")
        event_status = str(raw.get("event_status") or raw.get("status_desc") or raw.get("status") or "")
        bet_status  = str(raw.get("bet_status") or raw.get("b_status") or "")

        score_parts = score.split(":") if ":" in score else score.split("-") if "-" in score else []
        score_home  = score_parts[0].strip() if len(score_parts) >= 2 else None
        score_away  = score_parts[1].strip() if len(score_parts) >= 2 else None

        markets_raw = raw.get("markets") or raw.get("odds") or []
        markets = (
            _parse_all_markets(markets_raw, sl, home, away)
            if isinstance(markets_raw, list) else
            markets_raw if isinstance(markets_raw, dict) else {}
        )

        if "1x2" not in markets:
            try:
                ho = float(raw.get("home_odd") or raw.get("h_odd") or 0)
                no = float(raw.get("draw_odd") or raw.get("d_odd") or raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or raw.get("a_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    base = "1x2" if sl == "soccer" else f"{sl}_1x2"
                    markets[base] = {k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1}
            except (TypeError, ValueError):
                pass

        return {
            "od_match_id":   match_id,
            "od_event_id":   str(raw.get("game_id") or match_id),
            "od_parent_id":  match_id,
            "sp_game_id":    None,
            "betradar_id":   betradar_id,
            "home_team":     home,
            "away_team":     away,
            "competition":   competition,
            "category":      category,
            "sport":         sl,
            "od_sport_id":   sl,
            "start_time":    start_time,
            "source":        "odibets",
            "is_live":       is_live,
            "is_suspended":  bet_status in ("STOPPED", "BET_STOP", "SUSPENDED"),
            "match_time":    match_time,
            "event_status":  event_status,
            "bet_status":    bet_status,
            "current_score": score,
            "score_home":    score_home,
            "score_away":    score_away,
            "markets":       markets,
            "market_count":  len(markets),
        }
    except Exception as exc:
        logger.debug("OD normalise error: %s | raw=%s", exc, str(raw)[:200])
        return None


# ══════════════════════════════════════════════════════════════════════════════
# DAY-LEVEL FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

# Safety cap: at 200 per page, this allows 10,000 matches per sport per day.
MAX_PAGES_PER_DAY = 50


def _fetch_day(api_sport_id: str, day_str: str, per_page: int = 200) -> list[dict]:
    """
    Fetch ALL matches for one sport on one day using proper pagination.

    Termination (first match):
      1. Empty page returned.
      2. Fewer results than per_page (last page).
      3. page * per_page >= meta.total (when total > 0).
      4. MAX_PAGES_PER_DAY reached (safety).
    """
    all_raw: list[dict] = []
    page = 1

    while page <= MAX_PAGES_PER_DAY:
        params = {
            "resource":       "sport",
            "sport_id":       api_sport_id,
            "sportsbook":     "sportsbook",
            "ua":             HEADERS["user-agent"],
            "day":            day_str,
            "page":           page,
            "per_page":       per_page,
            "sub_type_id":    "",
            "competition_id": "",
            "hour":           "",
            "day_tmp":        "",
            "country_id":     "",
            "sort_by":        "",
            "filter":         "",
            "cs":             "",
            "hs":             "",
        }
        data = _get(SBOOK_V1, params=params)
        if not data:
            break

        matches, meta = _unwrap_matches_and_meta(data)
        if not matches:
            break

        all_raw.extend(matches)

        # Termination: last page signal
        if len(matches) < per_page:
            break

        # Termination: meta.total (reliable for day-level calls)
        total = _meta_total(meta)
        if total > 0 and page * per_page >= total:
            break

        page += 1

    if page > MAX_PAGES_PER_DAY:
        logger.warning(
            "OD pagination cap hit: sport=%s day=%s page=%d (%d matches). "
            "Results may be incomplete.",
            api_sport_id, day_str, page, len(all_raw),
        )

    logger.debug("OD %s %s: %d matches (%d pages)", api_sport_id, day_str, len(all_raw), page)
    return all_raw


def _fetch_competition_day(
    api_sport_id: str, competition_id: str, day_str: str, per_page: int = 200
) -> list[dict]:
    """
    Fallback: fetch one competition × day.
    meta.total is always 0 for these; use "fewer than per_page" termination only.
    """
    all_raw: list[dict] = []
    page = 1
    while page <= 20:
        params = {
            "resource":       "sport",
            "sport_id":       api_sport_id,
            "sportsbook":     "sportsbook",
            "ua":             HEADERS["user-agent"],
            "day":            day_str,
            "competition_id": competition_id,
            "page":           page,
            "per_page":       per_page,
            "sub_type_id":    "",
            "hour":           "",
            "day_tmp":        "",
            "country_id":     "",
            "sort_by":        "",
            "filter":         "",
            "cs":             "",
            "hs":             "",
        }
        data = _get(SBOOK_V1, params=params)
        if not data:
            break
        matches, _ = _unwrap_matches_and_meta(data)
        if not matches:
            break
        all_raw.extend(matches)
        if len(matches) < per_page:
            break
        page += 1
    return all_raw


def _probe_sport_slug(slug: str, days: int) -> str:
    """
    Try the primary slug and fallbacks; return the first one that returns data.
    Caches the result on OD_SPORT_IDS so subsequent calls are free.
    """
    candidates = [slug] + _SLUG_FALLBACKS.get(slug, [])
    today = _date.today().isoformat()

    for candidate in candidates:
        params = {
            "resource":   "sport",
            "sport_id":   candidate,
            "sportsbook": "sportsbook",
            "ua":         HEADERS["user-agent"],
            "day":        today,
            "page":       1,
            "per_page":   5,
            "sub_type_id": "", "competition_id": "", "hour": "",
            "day_tmp": "", "country_id": "", "sort_by": "",
            "filter": "", "cs": "", "hs": "",
        }
        data = _get(SBOOK_V1, params=params)
        if data:
            matches, meta = _unwrap_matches_and_meta(data)
            total = _meta_total(meta)
            if matches or total > 0:
                if candidate != slug:
                    logger.info("OD slug probe: %s → resolved to %s", slug, candidate)
                    OD_SPORT_IDS[slug] = candidate  # cache for this process lifetime
                return candidate

    logger.warning("OD slug probe: no working slug found for %s (tried %s)", slug, candidates)
    return slug  # return original; fetcher will return []


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING MATCHES — main public API
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_matches(
    sport_slug: str = "soccer",
    days: int = 30,
    offset: int = 0,
    max_matches: int | None = None,
    fetch_full_markets: bool = True,
    max_workers: int = 8,          # market enrichment concurrency
    concurrent_days: int = 10,     # parallel day fetches
    skip_enrich_threshold: int = 3,
    **kwargs,
) -> list[dict]:
    """
    Fetch upcoming matches for one sport over `days` days.

    Strategy:
      - Esoccer: single overview request.
      - All others: parallel day-level pagination (fast, complete).
        If a day returns 0 matches despite the overview showing >0,
        falls back to competition-level fetching for that day.
    """
    # Resolve the correct API slug (probe fallbacks for 0-match sports)
    api_sport_id = _probe_sport_slug(sport_slug, days)

    # ── Esoccer: single no-day request ─────────────────────────────────────
    if sport_slug == "esoccer":
        params = {
            "resource": "sport", "sport_id": api_sport_id,
            "sportsbook": "sportsbook", "ua": HEADERS["user-agent"],
            "day": "", "hour": "", "day_tmp": "", "country_id": "",
            "sort_by": "", "sub_type_id": "", "competition_id": "",
            "filter": "", "cs": "", "hs": "",
        }
        data = _get(SBOOK_V1, params=params)
        if not data:
            return []
        raw, _ = _unwrap_matches_and_meta(data)
        return _finalise(raw, sport_slug, fetch_full_markets, max_workers,
                         skip_enrich_threshold, offset, max_matches)

    # ── Step 1: overview to discover which days have data ──────────────────
    overview_params = {
        "resource": "sport", "sport_id": api_sport_id,
        "sportsbook": "sportsbook", "ua": HEADERS["user-agent"],
        "day": "", "page": 1, "per_page": 25,
        "sub_type_id": "", "competition_id": "", "hour": "",
        "day_tmp": "", "country_id": "", "sort_by": "",
        "filter": "", "cs": "", "hs": "",
    }
    overview_data = _get(SBOOK_V1, params=overview_params)
    available_days: set[str] = set()
    all_competitions: list[str] = []

    if overview_data:
        d = _inner(overview_data)
        for day_entry in (d.get("days") or []):
            if day_entry.get("unit") == "day":
                sd = str(day_entry.get("schedule_date", ""))
                if sd:
                    available_days.add(sd)
        all_competitions = [
            str(c["competition_id"])
            for c in (d.get("competitions") or [])
            if c.get("competition_id")
        ]

    today = _date.today()
    day_strings = [(today + timedelta(days=i)).isoformat() for i in range(days)]

    # ── Step 2: parallel day fetches ───────────────────────────────────────
    all_raw: list[dict] = []
    seen_ids: set[str] = set()

    def _add_batch(batch: list[dict]) -> None:
        for m in batch:
            uid = str(m.get("parent_match_id") or m.get("game_id") or "")
            if uid and uid not in seen_ids:
                seen_ids.add(uid)
                all_raw.append(m)

    with ThreadPoolExecutor(max_workers=min(concurrent_days, len(day_strings))) as executor:
        day_futures = {
            executor.submit(_fetch_day, api_sport_id, ds): ds
            for ds in day_strings
        }
        day_results: dict[str, list[dict]] = {}
        for future in as_completed(day_futures):
            ds = day_futures[future]
            try:
                batch = future.result()
                day_results[ds] = batch
                _add_batch(batch)
            except Exception as exc:
                logger.error("OD day fetch error %s %s: %s", sport_slug, ds, exc)
                day_results[ds] = []

    # ── Step 3: fallback — competition-level for days that returned 0 ──────
    # Only do this if we have a competition list from the overview.
    empty_days = [
        ds for ds in day_strings
        if not day_results.get(ds) and (not available_days or ds in available_days)
    ]

    if empty_days and all_competitions:
        logger.info(
            "OD %s: day-level returned 0 for %d days; trying competition-level fallback",
            sport_slug, len(empty_days),
        )
        fallback_tasks = [
            (api_sport_id, comp_id, ds)
            for ds in empty_days
            for comp_id in all_competitions
        ]
        with ThreadPoolExecutor(max_workers=min(concurrent_days, len(fallback_tasks) or 1)) as executor:
            fb_futures = {
                executor.submit(_fetch_competition_day, sid, cid, ds): (sid, cid, ds)
                for sid, cid, ds in fallback_tasks
            }
            for future in as_completed(fb_futures):
                try:
                    _add_batch(future.result())
                except Exception as exc:
                    logger.error("OD comp fallback error %s: %s", fb_futures[future], exc)

    logger.info("OD upcoming %s (%d days): %d raw matches", sport_slug, days, len(all_raw))
    return _finalise(all_raw, sport_slug, fetch_full_markets, max_workers,
                     skip_enrich_threshold, offset, max_matches)


def _finalise(
    raw_matches: list[dict],
    sport_slug: str,
    fetch_full_markets: bool,
    max_workers: int,
    skip_enrich_threshold: int,
    offset: int,
    max_matches: int | None,
) -> list[dict]:
    """Normalise, deduplicate, enrich, apply offset/limit."""
    normalised: list[dict] = []
    seen: set[str] = set()
    for raw in raw_matches:
        m = _normalise_match(raw, sport_slug, is_live=False)
        if not m:
            continue
        key = m["od_match_id"]
        if key not in seen:
            seen.add(key)
            normalised.append(m)

    if fetch_full_markets and normalised:
        needs = [m for m in normalised if m["market_count"] < skip_enrich_threshold]
        rich  = [m for m in normalised if m["market_count"] >= skip_enrich_threshold]
        if needs:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                enriched = list(pool.map(_fetch_markets, needs))
        else:
            enriched = []
        normalised = enriched + rich

    if offset:
        normalised = normalised[offset:]
    if max_matches is not None:
        normalised = normalised[:max_matches]

    return normalised


def _fetch_markets(match: dict) -> dict:
    br_id = match.get("betradar_id")
    if not br_id:
        return match
    full = fetch_full_markets_for_match(br_id, match.get("sport", "soccer"))
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
    if not data:
        return []
    matches: list[dict] = []
    for raw in _unwrap_live(data):
        if not isinstance(raw, dict):
            continue
        sl = _resolve_sport(raw.get("s_binomen") or raw.get("sport_id"), sport_slug or "soccer")
        m = _normalise_match(raw, sl, is_live=True)
        if m:
            matches.append(m)
    logger.info("OD live: %d matches (sport=%s)", len(matches), sport_slug or "all")
    return matches


# ══════════════════════════════════════════════════════════════════════════════
# STREAMING GENERATORS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_stream(
    sport_slug: str = "soccer",
    days: int = 30,
    max_matches: int | None = None,
    fetch_full_markets: bool = True,
    **kwargs,
) -> Generator[dict, None, None]:
    yield from fetch_upcoming_matches(
        sport_slug=sport_slug, days=days,
        fetch_full_markets=fetch_full_markets, max_matches=max_matches,
    )


def fetch_live_stream(
    sport_slug: str,
    fetch_full_markets: bool = True,
    **kwargs,
) -> Generator[dict, None, None]:
    for m in fetch_live_matches(sport_slug):
        if fetch_full_markets and m.get("betradar_id"):
            full = fetch_full_markets_for_match(m["betradar_id"], m.get("sport", "soccer"))
            if full:
                m["markets"].update(full)
                m["market_count"] = len(m["markets"])
        yield m


# ══════════════════════════════════════════════════════════════════════════════
# ALIASES & STUBS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming(sport_slug: str = "soccer", days: int = 30, fetch_full_markets: bool = True, **kwargs) -> list[dict]:
    return fetch_upcoming_matches(sport_slug, days=days, fetch_full_markets=fetch_full_markets, **kwargs)

def fetch_live(sport_slug: str | None = None, **kwargs) -> list[dict]:
    return fetch_live_matches(sport_slug)

class OdiBetsLivePoller:
    def __init__(self, redis_client: Any, interval: float = 2.0):
        self.redis    = redis_client
        self.interval = interval
        self._running = False

    def start(self):
        self._running = True

    def stop(self):
        self._running = False

    @property
    def alive(self) -> bool:
        return False

_live_poller = None

def get_live_poller():
    return _live_poller

def init_live_poller(redis_client: Any, interval: float = 2.0):
    global _live_poller
    if _live_poller is None:
        _live_poller = OdiBetsLivePoller(redis_client, interval)
        _live_poller.start()
    return _live_poller

def get_cached_upcoming(redis_client: Any, sport_slug: str) -> list[dict] | None:
    return None

def cache_upcoming(redis_client: Any, sport_slug: str, matches: list[dict], ttl: int = 300) -> None:
    pass

def get_cached_live(redis_client: Any, od_sport_id: Any) -> list[dict] | None:
    return None

class OdiBetsHarvesterPlugin:
    bookie_id   = "odibets"
    bookie_name = "OdiBets"
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