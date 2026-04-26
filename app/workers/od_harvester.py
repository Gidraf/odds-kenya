"""
app/workers/od_harvester.py
============================
Optimized OdiBets upcoming + live harvester.
- Shared httpx.Client with connection pool (thread-safe).
- Global request semaphore caps total concurrent HTTP calls regardless of
  how many sports / workers are running simultaneously in the CLI.
- 429 / 503 responses trigger exponential back-off instead of a hard fail.
- Sub-type market fetches parallelised inside fetch_full_markets_for_match.
- Deduplication by betradar_id before expensive market enrichment.
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
# SPORT ID MAPS
# ══════════════════════════════════════════════════════════════════════════════

OD_SPORT_IDS: dict[str, int | str] = {
    "soccer":            1,
    "basketball":        2,
    "tennis":            5,
    "ice-hockey":        4,
    "rugby":             12,
    "handball":          6,
    "table-tennis":      20,
    "cricket":           21,
    "volleyball":        23,
    "baseball":          3,
    "american-football": 11,
    "mma":               117,
    "boxing":            10,
    "darts":             22,
    "esoccer":           "esoccer",
}

OD_SPORT_SLUGS: dict[int | str, str] = {v: k for k, v in OD_SPORT_IDS.items()}

_OD_STRING_SPORT_MAP: dict[str, str] = {
    "soccer":            "soccer",
    "football":          "soccer",
    "basketball":        "basketball",
    "tennis":            "tennis",
    "cricket":           "cricket",
    "rugby":             "rugby",
    "ice-hockey":        "ice-hockey",
    "volleyball":        "volleyball",
    "handball":          "handball",
    "table-tennis":      "table-tennis",
    "baseball":          "baseball",
    "mma":               "mma",
    "boxing":            "boxing",
    "darts":             "darts",
    "american football": "american-football",
    "esoccer":           "esoccer",
}

def slug_to_od_sport_id(slug: str) -> int | str:
    if slug == "esoccer":
        return "esoccer"
    return OD_SPORT_IDS.get(slug, 1)

def od_sport_to_slug(sport_id: int | str) -> str:
    return OD_SPORT_SLUGS.get(sport_id, "soccer")

def _resolve_sport(raw_sport: Any, fallback_od_id: int | str) -> tuple[int | str, str]:
    if raw_sport is None:
        return fallback_od_id, od_sport_to_slug(fallback_od_id)
    try:
        od_id = int(raw_sport)
        return od_id, od_sport_to_slug(od_id)
    except (TypeError, ValueError):
        pass
    str_val = str(raw_sport).lower().strip()
    canonical = _OD_STRING_SPORT_MAP.get(str_val)
    if canonical:
        return slug_to_od_sport_id(canonical), canonical
    logger.warning("Unknown OdiBets sport string: %s, using fallback %s", str_val, fallback_od_id)
    return fallback_od_id, od_sport_to_slug(fallback_od_id)


# ══════════════════════════════════════════════════════════════════════════════
# API ENDPOINTS + HEADERS
# ══════════════════════════════════════════════════════════════════════════════

API_BASE  = "https://api.odi.site"
SBOOK_V1  = f"{API_BASE}/sportsbook/v1"
SBOOK_ODI = f"{API_BASE}/odi/sportsbook"

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
    "sec-ch-ua":          '"Chromium";v="147", "Not.A/Brand";v="8", "Google Chrome";v="147"',
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
# Caps the total number of in-flight HTTP calls across ALL sports at once.
# Without this: 12 sports × 8 market-workers × ~15 sub-types = 1440 queued
# requests that all try to grab one of 150 connections → timeouts for large
# sports like soccer and basketball.
_REQUEST_SEMAPHORE = threading.Semaphore(60)

def configure_concurrency(max_concurrent_requests: int = 60) -> None:
    """
    Tune the global HTTP concurrency cap before starting a harvest.
    Lower = safer when many sports run in parallel.
    Higher = faster when harvesting a single sport.
    """
    global _REQUEST_SEMAPHORE
    _REQUEST_SEMAPHORE = threading.Semaphore(max_concurrent_requests)


def _get(url: str, params: dict | None = None, timeout: float = 20.0) -> dict | list | None:
    """
    Semaphore-gated, retry-aware GET.
    - Acquires the global semaphore before sending (back-pressure across sports).
    - On 429 / 503 backs off and retries up to 3 times total.
    """
    client = _get_client()

    for attempt in range(3):
        with _REQUEST_SEMAPHORE:
            try:
                r = client.get(url, params=params, timeout=timeout)

                if r.status_code in (429, 503):
                    wait = float(r.headers.get("Retry-After", 2 ** (attempt + 1)))
                    logger.warning(
                        "OD rate-limited (%s) %s – waiting %.1fs (attempt %d)",
                        r.status_code, url, wait, attempt + 1,
                    )
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

def _unwrap_sport_response(data: dict | list) -> tuple[list[dict], dict]:
    if not isinstance(data, dict):
        return [], {}
    inner = data.get("data")
    if not isinstance(inner, dict):
        return [], {}
    matches = inner.get("matches") or []
    if not isinstance(matches, list):
        matches = []
    return matches, (inner.get("meta") or {})

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

def _unwrap_esoccer_response(data: dict | list) -> list[dict]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    inner = data.get("data")
    if isinstance(inner, dict):
        matches = inner.get("matches")
        if isinstance(matches, list):
            return matches
    matches = data.get("matches")
    if isinstance(matches, list):
        return matches
    return _unwrap_sport_response(data)[0]


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
    ds_lower = display_str.lower().strip()
    if not ds_lower:
        return ""
    if ds_lower in ("1", "x", "2", "yes", "no", "over", "under", "odd", "even", "none"):
        return ds_lower
    replacements = [
        (home.lower().replace(" ", "_"), "1"),
        (away.lower().replace(" ", "_"), "2"),
        (home.lower(), "1"),
        (away.lower(), "2"),
        ("draw", "X"),
    ]
    replacements.sort(key=lambda x: len(x[0]), reverse=True)
    for t_name, t_val in replacements:
        if len(t_name) > 2 and t_name in ds_lower:
            ds_lower = ds_lower.replace(t_name, t_val)
    return ds_lower

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
# SMART MARKET FETCHER — sub_type_ids fetched in parallel
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_one_sub_type(
    sid: str,
    base_params: dict,
    sport_slug: str,
    home_team: str,
    away_team: str,
) -> dict[str, dict[str, float]]:
    params = {**base_params, "sub_type_id": sid}
    data = _get(SBOOK_V1, params=params)
    if not data or not isinstance(data, dict):
        return {}
    inner = data.get("data")
    if not isinstance(inner, dict):
        inner = data
    markets = inner.get("markets") or []
    return _parse_all_markets(markets, sport_slug, home_team, away_team) if markets else {}


def fetch_full_markets_for_match(
    event_id: str | int,
    od_sport_id: int | str = 1,
    sub_type_workers: int = 8,
) -> dict[str, dict[str, float]]:
    all_markets: dict[str, dict[str, float]] = {}

    base_params = {
        "resource": "sportevent",
        "id": str(event_id),
        "category_id": "",
        "sub_type_id": "",
        "builder": 0,
        "sportsbook": "sportsbook",
        "ua": HEADERS["user-agent"],
    }
    data = _get(SBOOK_V1, params=base_params)
    if not data or not isinstance(data, dict):
        return {}

    inner = data.get("data")
    if not isinstance(inner, dict):
        inner = data

    info      = inner.get("info") or {}
    home_team = str(info.get("home_team") or "")
    away_team = str(info.get("away_team") or "")
    _, sport_slug = _resolve_sport(info.get("s_binomen") or info.get("sport_id"), od_sport_id)

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
                logger.warning("OD sub_type fetch error (sid=%s): %s", futures[future], exc)

    logger.debug("OD fetch_full_markets_for_match: %s → %d slugs", event_id, len(all_markets))
    return all_markets


# ══════════════════════════════════════════════════════════════════════════════
# MATCH NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_match(raw: dict, od_sport_id: int | str, is_live: bool = False) -> dict | None:
    try:
        match_id    = str(raw.get("game_id") or raw.get("id") or raw.get("match_id") or raw.get("event_id") or "")
        parent_id   = str(raw.get("parent_match_id") or raw.get("parent_id") or match_id)
        betradar_id = str(raw.get("betradar_id") or raw.get("sr_id") or "") or None
        if not betradar_id and parent_id:
            betradar_id = parent_id
        if not match_id:
            return None

        home        = str(raw.get("home_team") or raw.get("home") or "Home")
        away        = str(raw.get("away_team") or raw.get("away") or "Away")
        competition = str(raw.get("competition_name") or raw.get("competition") or raw.get("league") or "")
        category    = str(raw.get("category_name") or raw.get("category") or raw.get("country_name") or raw.get("country") or "")
        od_sport_id_, sport_slug = _resolve_sport(
            raw.get("sport_id") or raw.get("sport") or raw.get("s_binomen"), od_sport_id
        )
        start_time    = str(raw.get("start_time") or raw.get("event_date") or raw.get("date") or "")
        current_score = str(raw.get("current_score") or raw.get("score") or raw.get("result") or "")
        match_time    = str(raw.get("match_time") or raw.get("game_time") or raw.get("periodic_time") or "")
        event_status  = str(raw.get("event_status") or raw.get("status_desc") or raw.get("status") or "")
        bet_status    = str(raw.get("bet_status") or raw.get("b_status") or "")

        score_parts = (
            current_score.split(":") if ":" in current_score
            else current_score.split("-") if "-" in current_score
            else []
        )
        score_home = score_parts[0].strip() if len(score_parts) >= 2 else None
        score_away = score_parts[1].strip() if len(score_parts) >= 2 else None

        markets_raw = raw.get("markets") or raw.get("odds") or []
        markets = (
            _parse_all_markets(markets_raw, sport_slug, home, away)
            if isinstance(markets_raw, list) else
            markets_raw if isinstance(markets_raw, dict) else {}
        )

        if "1x2" not in markets:
            try:
                ho = float(raw.get("home_odd") or raw.get("h_odd") or 0)
                no = float(raw.get("draw_odd") or raw.get("d_odd") or raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or raw.get("a_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    base = f"{sport_slug}_1x2" if sport_slug != "soccer" else "1x2"
                    markets[base] = {k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1}
            except (TypeError, ValueError):
                pass

        return {
            "od_match_id":   match_id,
            "od_event_id":   match_id,
            "od_parent_id":  parent_id,
            "sp_game_id":    None,
            "betradar_id":   betradar_id,
            "home_team":     home,
            "away_team":     away,
            "competition":   competition,
            "category":      category,
            "sport":         sport_slug,
            "od_sport_id":   od_sport_id_,
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
# FETCH HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_raw_matches_for_day(od_sport_id: int | str, day_str: str) -> list[dict]:
    all_raw: list[dict] = []
    page = 1
    per_page = 200
    while True:
        params = {
            "resource": "sport", "sport_id": od_sport_id,
            "sportsbook": "sportsbook", "ua": HEADERS["user-agent"],
            "day": day_str, "hour": "", "day_tmp": "", "country_id": "",
            "sort_by": "", "sub_type_id": "", "competition_id": "",
            "filter": "", "cs": "", "hs": "",
            "page": page, "per_page": per_page,
        }
        data = _get(SBOOK_V1, params=params)
        if not data:
            break
        matches, meta = _unwrap_sport_response(data)
        if not matches:
            break
        all_raw.extend(matches)
        total           = meta.get("total", 0)
        current_page    = meta.get("page", page)
        per_page_actual = meta.get("per_page", per_page)
        if total == 0 or current_page * per_page_actual >= total:
            break
        page += 1
    return all_raw


def _fetch_markets_for_match(match: dict) -> dict:
    br_id = match.get("betradar_id")
    if not br_id:
        return match
    full = fetch_full_markets_for_match(br_id, match.get("od_sport_id", 1))
    if full:
        match["markets"].update(full)
        match["market_count"] = len(match["markets"])
    return match


def _process_raw_matches(
    raw_matches: list[dict],
    od_sport_id: int | str,
    fetch_full_markets: bool,
    max_workers: int,
    skip_enrich_threshold: int = 5,
) -> list[dict]:
    normalised: list[dict] = []
    for raw in raw_matches:
        m = _normalise_match(raw, od_sport_id, is_live=False)
        if m:
            normalised.append(m)

    if not fetch_full_markets or not normalised:
        return normalised

    # Deduplicate — enrich each unique event at most once
    seen: set[str] = set()
    deduped: list[dict] = []
    for m in normalised:
        key = m.get("betradar_id") or m["od_match_id"]
        if key not in seen:
            seen.add(key)
            deduped.append(m)

    needs_enrich = [m for m in deduped if m["market_count"] < skip_enrich_threshold]
    already_rich = [m for m in deduped if m["market_count"] >= skip_enrich_threshold]

    if needs_enrich:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            enriched = list(pool.map(_fetch_markets_for_match, needs_enrich))
    else:
        enriched = []

    return enriched + already_rich


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING MATCHES
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_matches(
    sport_slug: str = "soccer",
    days: int = 30,
    offset: int = 0,
    max_matches: int | None = None,
    fetch_full_markets: bool = True,
    max_workers: int = 8,
    concurrent_days: int = 5,
    skip_enrich_threshold: int = 5,
    **kwargs,
) -> list[dict]:
    od_sport_id = slug_to_od_sport_id(sport_slug)

    if sport_slug == "esoccer":
        params = {
            "resource": "sport", "sport_id": od_sport_id,
            "sportsbook": "sportsbook", "ua": HEADERS["user-agent"],
            "day": "", "hour": "", "day_tmp": "", "country_id": "",
            "sort_by": "", "sub_type_id": "", "competition_id": "",
            "filter": "", "cs": "", "hs": "",
        }
        data = _get(SBOOK_V1, params=params)
        if not data:
            return []
        raw_events = _unwrap_esoccer_response(data)
        matches = _process_raw_matches(raw_events, od_sport_id, fetch_full_markets, max_workers, skip_enrich_threshold)
        if offset > 0:
            matches = matches[offset:]
        if max_matches is not None:
            matches = matches[:max_matches]
        logger.info("OD upcoming esoccer: %d matches", len(matches))
        return matches

    start_date  = _date.today()
    day_strings = [(start_date + timedelta(days=i)).isoformat() for i in range(days)]

    all_raw: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(concurrent_days, len(day_strings))) as executor:
        futures = {executor.submit(_fetch_raw_matches_for_day, od_sport_id, ds): ds for ds in day_strings}
        for future in as_completed(futures):
            try:
                all_raw.extend(future.result())
            except Exception as exc:
                logger.error("Error fetching day %s: %s", futures[future], exc)

    matches = _process_raw_matches(all_raw, od_sport_id, fetch_full_markets, max_workers, skip_enrich_threshold)
    if offset > 0:
        matches = matches[offset:]
    if max_matches is not None:
        matches = matches[:max_matches]

    logger.info("OD upcoming %s (next %d days): %d matches", sport_slug, days, len(matches))
    return matches


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
    for raw in _unwrap_live_response(data):
        if not isinstance(raw, dict):
            continue
        raw_sport_id = raw.get("sport_id") or (slug_to_od_sport_id(sport_slug) if sport_slug else 1)
        try:
            raw_sport_id = int(raw_sport_id)
        except (TypeError, ValueError):
            pass
        m = _normalise_match(raw, raw_sport_id, is_live=True)
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
    od_sport_id = slug_to_od_sport_id(sport_slug)
    count = 0

    if sport_slug == "esoccer":
        params = {
            "resource": "sport", "sport_id": od_sport_id,
            "sportsbook": "sportsbook", "ua": HEADERS["user-agent"],
            "day": "", "hour": "", "day_tmp": "", "country_id": "",
            "sort_by": "", "sub_type_id": "", "competition_id": "",
            "filter": "", "cs": "", "hs": "",
        }
        data = _get(SBOOK_V1, params=params)
        if data:
            for raw in _unwrap_esoccer_response(data):
                if max_matches and count >= max_matches:
                    return
                m = _normalise_match(raw, od_sport_id, is_live=False)
                if not m:
                    continue
                if fetch_full_markets and m.get("betradar_id"):
                    full = fetch_full_markets_for_match(m["betradar_id"], m.get("od_sport_id", od_sport_id))
                    if full:
                        m["markets"].update(full)
                        m["market_count"] = len(m["markets"])
                count += 1
                yield m
        return

    start_date = _date.today()
    for day_offset in range(days):
        day_str  = (start_date + timedelta(days=day_offset)).isoformat()
        page     = 1
        per_page = 200
        while True:
            params = {
                "resource": "sport", "sport_id": od_sport_id,
                "sportsbook": "sportsbook", "ua": HEADERS["user-agent"],
                "day": day_str, "hour": "", "day_tmp": "", "country_id": "",
                "sort_by": "", "sub_type_id": "", "competition_id": "",
                "filter": "", "cs": "", "hs": "",
                "page": page, "per_page": per_page,
            }
            data = _get(SBOOK_V1, params=params)
            if not data:
                break
            matches, meta = _unwrap_sport_response(data)
            if not matches:
                break
            for raw in matches:
                if max_matches and count >= max_matches:
                    return
                m = _normalise_match(raw, od_sport_id, is_live=False)
                if not m:
                    continue
                if fetch_full_markets and m.get("betradar_id"):
                    full = fetch_full_markets_for_match(m["betradar_id"], m.get("od_sport_id", od_sport_id))
                    if full:
                        m["markets"].update(full)
                        m["market_count"] = len(m["markets"])
                count += 1
                yield m
            total           = meta.get("total", 0)
            current_page    = meta.get("page", page)
            per_page_actual = meta.get("per_page", per_page)
            if total == 0 or current_page * per_page_actual >= total:
                break
            page += 1


def fetch_live_stream(
    sport_slug: str,
    fetch_full_markets: bool = True,
    **kwargs,
) -> Generator[dict, None, None]:
    for m in fetch_live_matches(sport_slug):
        if fetch_full_markets and m.get("betradar_id"):
            full = fetch_full_markets_for_match(m["betradar_id"], m.get("od_sport_id", 1))
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
        logger.info("OdiBetsLivePoller started (stub)")

    def stop(self):
        self._running = False

    @property
    def alive(self):
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

def get_cached_live(redis_client: Any, od_sport_id: int | str) -> list[dict] | None:
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