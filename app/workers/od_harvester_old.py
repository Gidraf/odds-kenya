"""
app/workers/od_harvester.py
============================
OdiBets upcoming + live harvester.

API contract (confirmed from curl evidence):
  - sport_id must be the STRING slug ("soccer", "basketball", etc.), NOT an integer.
  - Overview call (no day, no competition_id): returns competitions list + days breakdown
    + first page of matches.  meta.total is reliable here.
  - Competition call (day + competition_id): returns that competition's matches for that
    day.  meta.total = 0 always, so termination uses "fewer results than per_page".
  - Esoccer: sport_id="esoccer", no day, no page/per_page params needed.
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
# SPORT SLUG MAP  (sport_id in API calls == the slug itself)
# ══════════════════════════════════════════════════════════════════════════════

# Key   = our internal slug
# Value = the string sent as sport_id= in every API request
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

def slug_to_od_sport_id(slug: str) -> str:
    return OD_SPORT_IDS.get(slug, slug)

def od_sport_to_slug(sport_id: str) -> str:
    # sport_id already IS the slug in this API
    return str(sport_id)

def _resolve_sport(raw_sport: Any, fallback_slug: str) -> tuple[str, str]:
    if raw_sport is None:
        return fallback_slug, fallback_slug
    s = str(raw_sport).lower().strip()
    if s in OD_SPORT_IDS:
        return s, s
    # numeric fallback — map old integers just in case they appear in raw data
    _NUMERIC_MAP: dict[str, str] = {
        "1": "soccer", "2": "basketball", "5": "tennis", "4": "ice-hockey",
        "12": "rugby", "6": "handball", "20": "table-tennis", "21": "cricket",
        "23": "volleyball", "3": "baseball", "11": "american-football",
        "117": "mma", "10": "boxing", "22": "darts", "137": "esoccer",
    }
    if s in _NUMERIC_MAP:
        slug = _NUMERIC_MAP[s]
        return slug, slug
    return fallback_slug, fallback_slug


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
# RESPONSE UNWRAPPING
# ══════════════════════════════════════════════════════════════════════════════

def _unwrap_matches(data: dict | list) -> tuple[list[dict], dict]:
    """
    Return (matches, meta) from a /sportsbook/v1?resource=sport response.
    Handles both the overview shape (competitions + days + matches) and the
    competition-specific shape (matches only, meta.total=0).
    """
    if not isinstance(data, dict):
        return [], {}
    inner = data.get("data")
    if not isinstance(inner, dict):
        return [], {}
    matches = inner.get("matches") or []
    if not isinstance(matches, list):
        matches = []
    meta = inner.get("meta") or {}
    return matches, meta


def _unwrap_competitions(data: dict | list) -> list[dict]:
    """Extract competitions list from the overview response."""
    if not isinstance(data, dict):
        return []
    inner = data.get("data")
    if not isinstance(inner, dict):
        return []
    return inner.get("competitions") or []


def _unwrap_days(data: dict | list) -> list[dict]:
    """Extract days breakdown from the overview response."""
    if not isinstance(data, dict):
        return []
    inner = data.get("data")
    if not isinstance(inner, dict):
        return []
    return inner.get("days") or []


def _unwrap_live_response(data: dict | list) -> list[dict]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    inner = data.get("data")
    if isinstance(inner, list):
        return inner
    if isinstance(inner, dict):
        for key in ("matches", "events", "live", "results"):
            if isinstance(inner.get(key), list) and inner[key]:
                return inner[key]
    for key in ("matches", "events", "live", "results", "data"):
        candidate = data.get(key)
        if isinstance(candidate, list) and candidate:
            return candidate
    return []


# ══════════════════════════════════════════════════════════════════════════════
# MARKET PARSING
# ══════════════════════════════════════════════════════════════════════════════

def _parse_od_specifiers(spec_string: str) -> dict:
    if not spec_string:
        return {}
    parsed = {}
    for part in str(spec_string).split("|"):
        if "=" in part:
            key, val = part.split("=", 1)
            parsed[key.strip()] = val.strip()
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
            line_spec = str(line.get("specifiers") or "").strip()
            for o in (line.get("outcomes") or []):
                if isinstance(o, dict):
                    results.append((o, line_spec or str(o.get("specifiers") or "").strip()))
        return results

    odds = mkt.get("odds")
    if isinstance(odds, list):
        for o in odds:
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
# MARKET ENRICHMENT (full markets per event)
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_one_sub_type(
    sid: str,
    base_params: dict,
    sport_slug: str,
    home_team: str,
    away_team: str,
) -> dict[str, dict[str, float]]:
    data = _get(SBOOK_V1, params={**base_params, "sub_type_id": sid})
    if not data or not isinstance(data, dict):
        return {}
    inner = data.get("data") if isinstance(data.get("data"), dict) else data
    markets = inner.get("markets") or []
    return _parse_all_markets(markets, sport_slug, home_team, away_team) if markets else {}


def fetch_full_markets_for_match(
    event_id: str | int,
    od_sport_slug: str = "soccer",
    sub_type_workers: int = 8,
) -> dict[str, dict[str, float]]:
    all_markets: dict[str, dict[str, float]] = {}

    base_params = {
        "resource":    "sportevent",
        "id":          str(event_id),
        "category_id": "",
        "sub_type_id": "",
        "builder":     0,
        "sportsbook":  "sportsbook",
        "ua":          HEADERS["user-agent"],
    }
    data = _get(SBOOK_V1, params=base_params)
    if not data or not isinstance(data, dict):
        return {}

    inner     = data.get("data") if isinstance(data.get("data"), dict) else data
    info      = inner.get("info") or {}
    home_team = str(info.get("home_team") or "")
    away_team = str(info.get("away_team") or "")
    _, sport_slug = _resolve_sport(info.get("s_binomen") or info.get("sport_id"), od_sport_slug)

    initial_markets = inner.get("markets") or []
    if initial_markets:
        all_markets.update(_parse_all_markets(initial_markets, sport_slug, home_team, away_team))

    sub_type_ids = {
        str(m["sub_type_id"])
        for m in (inner.get("markets_list") or [])
        if m.get("sub_type_id")
    }
    if not sub_type_ids:
        return all_markets

    with ThreadPoolExecutor(max_workers=min(sub_type_workers, len(sub_type_ids))) as pool:
        futures = {
            pool.submit(_fetch_one_sub_type, sid, base_params, sport_slug, home_team, away_team): sid
            for sid in sub_type_ids
        }
        for future in as_completed(futures):
            try:
                for slug, outcomes in future.result().items():
                    all_markets.setdefault(slug, {}).update(outcomes)
            except Exception as exc:
                logger.warning("OD sub_type fetch error sid=%s: %s", futures[future], exc)

    logger.debug("OD fetch_full_markets_for_match: %s → %d slugs", event_id, len(all_markets))
    return all_markets


# ══════════════════════════════════════════════════════════════════════════════
# MATCH NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_match(raw: dict, sport_slug: str, is_live: bool = False) -> dict | None:
    try:
        match_id    = str(raw.get("parent_match_id") or raw.get("game_id") or raw.get("id") or raw.get("match_id") or "")
        betradar_id = str(raw.get("betradar_id") or raw.get("sr_id") or match_id) or None
        if not match_id:
            return None

        home        = str(raw.get("home_team") or raw.get("home") or "Home")
        away        = str(raw.get("away_team") or raw.get("away") or "Away")
        competition = str(raw.get("competition_name") or raw.get("competition") or raw.get("league") or "")
        category    = str(raw.get("category_name") or raw.get("category") or raw.get("country_name") or raw.get("country") or "")

        # Determine actual sport slug from raw data, fallback to passed slug
        raw_sport = raw.get("s_binomen") or raw.get("sport_id") or raw.get("sport")
        _, resolved_slug = _resolve_sport(raw_sport, sport_slug)

        start_time    = str(raw.get("start_time") or raw.get("event_date") or raw.get("date") or "")
        current_score = str(raw.get("current_score") or raw.get("result") or "")
        match_time    = str(raw.get("match_time") or raw.get("game_time") or raw.get("periodic_time") or "")
        event_status  = str(raw.get("event_status") or raw.get("status_desc") or raw.get("status") or "")
        bet_status    = str(raw.get("bet_status") or raw.get("b_status") or "")

        score_parts = (
            current_score.split(":") if ":" in current_score else
            current_score.split("-") if "-" in current_score else []
        )
        score_home = score_parts[0].strip() if len(score_parts) >= 2 else None
        score_away = score_parts[1].strip() if len(score_parts) >= 2 else None

        markets_raw = raw.get("markets") or raw.get("odds") or []
        markets = (
            _parse_all_markets(markets_raw, resolved_slug, home, away)
            if isinstance(markets_raw, list) else
            markets_raw if isinstance(markets_raw, dict) else {}
        )

        if "1x2" not in markets:
            try:
                ho = float(raw.get("home_odd") or raw.get("h_odd") or 0)
                no = float(raw.get("draw_odd") or raw.get("d_odd") or raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or raw.get("a_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    base = f"{resolved_slug}_1x2" if resolved_slug != "soccer" else "1x2"
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
            "sport":         resolved_slug,
            "od_sport_id":   resolved_slug,
            "start_time":    start_time,
            "source":        "odibets",
            "is_live":       is_live,
            "is_suspended":  bet_status in ("STOPPED", "BET_STOP", "SUSPENDED"),
            "match_time":    match_time,
            "event_status":  event_status,
            "bet_status":    bet_status,
            "current_score": current_score,
            "score_home":    score_home,
            "score_away":    score_away,
            "markets":       markets,
            "market_count":  len(markets),
        }
    except Exception as exc:
        logger.debug("OD match normalise error: %s | raw=%s", exc, str(raw)[:200])
        return None


# ══════════════════════════════════════════════════════════════════════════════
# CORE FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

def _build_base_params(sport_slug: str) -> dict:
    return {
        "resource":       "sport",
        "sport_id":       sport_slug,          # ← string slug, not integer
        "sportsbook":     "sportsbook",
        "ua":             HEADERS["user-agent"],
        "sub_type_id":    "",
        "competition_id": "",
        "hour":           "",
        "day_tmp":        "",
        "day":            "",
        "country_id":     "",
        "sort_by":        "",
        "filter":         "",
        "cs":             "",
        "hs":             "",
    }


def _fetch_overview(sport_slug: str) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Call the overview endpoint (no day, no competition_id).
    Returns (competitions, days, first_page_matches).
    The competitions list tells us which competition_ids exist for this sport.
    The days list tells us which dates have matches and how many.
    """
    params = {**_build_base_params(sport_slug), "page": 1, "per_page": 200}
    data = _get(SBOOK_V1, params=params)
    if not data:
        return [], [], []
    competitions = _unwrap_competitions(data)
    days         = _unwrap_days(data)
    matches, _   = _unwrap_matches(data)
    return competitions, days, matches


def _fetch_competition_day(
    sport_slug: str,
    competition_id: str,
    day_str: str,
    per_page: int = 200,
) -> list[dict]:
    """
    Fetch ALL matches for one competition on one day.

    meta.total is always 0 for competition-filtered responses, so termination
    uses the "fewer results than requested" signal (last page).
    """
    all_matches: list[dict] = []
    page = 1
    MAX_PAGES = 20  # safety cap: 20 × 200 = 4000 matches per competition per day

    while page <= MAX_PAGES:
        params = {
            **_build_base_params(sport_slug),
            "day":            day_str,
            "competition_id": competition_id,
            "page":           page,
            "per_page":       per_page,
        }
        data = _get(SBOOK_V1, params=params)
        if not data:
            break
        matches, _ = _unwrap_matches(data)
        if not matches:
            break
        all_matches.extend(matches)
        if len(matches) < per_page:
            break   # last page — API returned fewer than requested
        page += 1

    return all_matches


def _fetch_markets_for_match(match: dict) -> dict:
    br_id = match.get("betradar_id")
    if not br_id:
        return match
    full = fetch_full_markets_for_match(br_id, match.get("sport", "soccer"))
    if full:
        match["markets"].update(full)
        match["market_count"] = len(match["markets"])
    return match


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING MATCHES — main public API
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_matches(
    sport_slug: str = "soccer",
    days: int = 30,
    offset: int = 0,
    max_matches: int | None = None,
    fetch_full_markets: bool = True,
    max_workers: int = 8,       # market-enrichment concurrency
    concurrent_days: int = 5,   # day×competition fetch concurrency
    skip_enrich_threshold: int = 3,
    **kwargs,
) -> list[dict]:
    """
    Fetch upcoming matches for one sport using competition-level fetching:

      1. Overview call → competitions list + days breakdown
      2. For each day × competition → fetch matches (parallel)
      3. Deduplicate + normalise
      4. Optionally enrich markets
    """
    api_sport_id = slug_to_od_sport_id(sport_slug)

    # ── Esoccer: single request, no day/pagination needed ──────────────────
    if sport_slug == "esoccer":
        params = {**_build_base_params(api_sport_id)}
        data = _get(SBOOK_V1, params=params)
        if not data:
            return []
        raw_matches, _ = _unwrap_matches(data)
        return _finalise(raw_matches, sport_slug, fetch_full_markets, max_workers,
                         skip_enrich_threshold, offset, max_matches)

    # ── Step 1: overview (no day) ──────────────────────────────────────────
    competitions, days_info, overview_matches = _fetch_overview(api_sport_id)

    if not competitions:
        logger.warning("OD %s: no competitions found in overview", sport_slug)
        # Fall back: just use whatever matches came in the overview
        return _finalise(overview_matches, sport_slug, fetch_full_markets, max_workers,
                         skip_enrich_threshold, offset, max_matches)

    competition_ids = [str(c["competition_id"]) for c in competitions if c.get("competition_id")]

    # ── Step 2: determine which days to fetch ─────────────────────────────
    # Use days_info from the overview if available; otherwise generate our own list.
    today = _date.today()
    if days_info:
        target_days = [
            d["schedule_date"]
            for d in days_info
            if d.get("unit") == "day" and
               _date.fromisoformat(str(d["schedule_date"])) <= today + timedelta(days=days - 1)
        ]
    else:
        target_days = [(today + timedelta(days=i)).isoformat() for i in range(days)]

    logger.info(
        "OD %s: %d competitions × %d days = %d fetch tasks",
        sport_slug, len(competition_ids), len(target_days),
        len(competition_ids) * len(target_days),
    )

    # ── Step 3: parallel fetch day × competition ──────────────────────────
    all_raw: list[dict] = list(overview_matches)  # overview already has today's first page
    seen_raw: set[str] = {
        str(m.get("parent_match_id") or m.get("game_id") or "")
        for m in all_raw
    }

    tasks = [
        (api_sport_id, comp_id, day_str)
        for day_str in target_days
        for comp_id in competition_ids
    ]

    with ThreadPoolExecutor(max_workers=concurrent_days) as executor:
        futures = {
            executor.submit(_fetch_competition_day, sport_id, comp_id, day_str): (sport_id, comp_id, day_str)
            for sport_id, comp_id, day_str in tasks
        }
        for future in as_completed(futures):
            try:
                batch = future.result()
                for m in batch:
                    uid = str(m.get("parent_match_id") or m.get("game_id") or "")
                    if uid and uid not in seen_raw:
                        seen_raw.add(uid)
                        all_raw.append(m)
            except Exception as exc:
                logger.error("OD fetch task %s error: %s", futures[future], exc)

    logger.info("OD %s: %d raw matches collected", sport_slug, len(all_raw))
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
    """Normalise, deduplicate, enrich markets, apply offset/limit."""
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
                enriched = list(pool.map(_fetch_markets_for_match, needs))
        else:
            enriched = []
        normalised = enriched + rich

    if offset:
        normalised = normalised[offset:]
    if max_matches is not None:
        normalised = normalised[:max_matches]

    return normalised


# ══════════════════════════════════════════════════════════════════════════════
# LIVE MATCHES
# ══════════════════════════════════════════════════════════════════════════════

def fetch_live_matches(sport_slug: str | None = None) -> list[dict]:
    params: dict[str, Any] = {
        "resource":   "live",
        "sportsbook": "sportsbook",
        "ua":         HEADERS["user-agent"],
        "sub_type_id": "1",
        "sport_id":   "",
    }
    if sport_slug:
        params["sport_id"] = slug_to_od_sport_id(sport_slug)
    data = _get(SBOOK_V1, params=params, timeout=10.0)
    if not data:
        return []
    matches: list[dict] = []
    for raw in _unwrap_live_response(data):
        if not isinstance(raw, dict):
            continue
        raw_slug = str(raw.get("s_binomen") or raw.get("sport_id") or sport_slug or "soccer")
        _, sl = _resolve_sport(raw_slug, sport_slug or "soccer")
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
    matches = fetch_upcoming_matches(
        sport_slug=sport_slug,
        days=days,
        fetch_full_markets=fetch_full_markets,
        max_matches=max_matches,
    )
    yield from matches


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
    "fetch_upcoming_matches",
    "fetch_live_matches",
    "fetch_upcoming_stream",
    "fetch_live_stream",
    "fetch_full_markets_for_match",
    "fetch_upcoming",
    "fetch_live",
    "OdiBetsHarvesterPlugin",
    "OD_SPORT_IDS",
    "slug_to_od_sport_id",
    "od_sport_to_slug",
    "configure_concurrency",
]