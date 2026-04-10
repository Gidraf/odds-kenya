"""
app/workers/od_harvester.py
============================
OdiBets upcoming + live harvester.

Now fully integrated with the universal Sportradar dynamic mappers 
(`mappers/betika.py`) to automatically parse and normalize all sports.

**PATCHED**: Dynamically translates literal team names (e.g. "south_west_slammers")
in Odibets outcome keys back into standard "1", "X", "2" formats.

**PATCHED**: Added ID 2 (Moneyline) to fetch list to restore Tennis/Cricket/Rugby.
Implemented "Smart Batching" to fetch all missing market tabs in a single network 
request, avoiding timeouts while guaranteeing 100% market coverage.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import threading
import time
from datetime import date as _date
from typing import Any

import httpx

# Import the new dynamic multi-sport mappers (Universal Sportradar mapping)
from app.workers.mappers.betika import (
    get_market_slug,
    normalize_outcome,
)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# SPORT ID MAPS
# ══════════════════════════════════════════════════════════════════════════════

OD_SPORT_IDS: dict[str, int] = {
    "soccer":            1,
    "basketball":        2,
    "tennis":            3,
    "cricket":           4,
    "rugby":             5,
    "ice-hockey":        6,
    "volleyball":        7,
    "handball":          8,
    "table-tennis":      9,
    "baseball":          10,
    "american-football": 11,
    "mma":               15,
    "boxing":            16,
    "darts":             17,
    "esoccer":           1001,
}

OD_SPORT_SLUGS: dict[int, str] = {v: k for k, v in OD_SPORT_IDS.items()}

_OD_STRING_SPORT_MAP: dict[str, str] = {
    "soccer":            "soccer",
    "football":          "soccer",
    "internationals":    "soccer",
    "itl":               "soccer",
    "basketball":        "basketball",
    "tennis":            "tennis",
    "cricket":           "cricket",
    "rugby":             "rugby",
    "rugby union":       "rugby",
    "rugby league":      "rugby",
    "ice-hockey":        "ice-hockey",
    "icehockey":         "ice-hockey",
    "ice hockey":        "ice-hockey",
    "volleyball":        "volleyball",
    "handball":          "handball",
    "table-tennis":      "table-tennis",
    "tabletennis":       "table-tennis",
    "table tennis":      "table-tennis",
    "baseball":          "baseball",
    "mma":               "mma",
    "boxing":            "boxing",
    "darts":             "darts",
    "american football": "american-football",
    "american-football": "american-football",
    "esoccer":           "esoccer",
    "e-soccer":          "esoccer",
}


def slug_to_od_sport_id(slug: str) -> int:
    return OD_SPORT_IDS.get(slug, 1)

def od_sport_to_slug(sport_id: int) -> str:
    return OD_SPORT_SLUGS.get(sport_id, "soccer")

def _resolve_sport(raw_sport: Any, fallback_od_id: int) -> tuple[int, str]:
    if raw_sport is None:
        return fallback_od_id, od_sport_to_slug(fallback_od_id)
    try:
        od_id = int(raw_sport)
        return od_id, od_sport_to_slug(od_id)
    except (TypeError, ValueError):
        pass
    str_val   = str(raw_sport).lower().strip()
    canonical = _OD_STRING_SPORT_MAP.get(str_val)
    if canonical:
        return slug_to_od_sport_id(canonical), canonical
    od_id = OD_SPORT_IDS.get(str_val)
    if od_id:
        return od_id, str_val
    return fallback_od_id, od_sport_to_slug(fallback_od_id)


# ══════════════════════════════════════════════════════════════════════════════
# API ENDPOINTS + HEADERS
# ══════════════════════════════════════════════════════════════════════════════

API_BASE  = "https://api.odi.site"
SBOOK_V1  = f"{API_BASE}/sportsbook/v1"
SBOOK_ODI = f"{API_BASE}/odi/sportsbook"

HEADERS: dict[str, str] = {
    "accept":             "application/json, text/plain, */*",
    "accept-language":    "en-GB,en;q=0.9",
    "authorization":      "Bearer",
    "content-type":       "application/json",
    "origin":             "https://odibets.com",
    "referer":            "https://odibets.com/",
    "user-agent": (
        "Mozilla/5.0 (Linux; Android 10; K) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
    "sec-ch-ua":          '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile":   "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "cross-site",
}

_LIVE_DATA_KEY   = "od:live:{sport_id}:data"
_LIVE_HASH_KEY   = "od:live:{sport_id}:hash"
_LIVE_CHAN_KEY   = "od:live:{sport_id}:updates"
_LIVE_SPORTS_KEY = "od:live:sports"
_UPC_DATA_KEY    = "od:upcoming:{sport_slug}:data"
_UPC_HASH_KEY    = "od:upcoming:{sport_slug}:hash"
_UPC_CHAN_KEY    = "od:upcoming:{sport_slug}:updates"

# CRITICAL FIX: Added 2, 3, 4, 5 to catch Moneyline and niche sport primary markets
_BROAD_SUB_TYPE_IDS = "1,2,3,4,5,8,10,11,14,15,16,18,19,20,29,37,47,60,63,66,68,186,187,188,189,199,202,204,219,225,230,234,237,251,256,258,264,274,309,310,340,406,432"


# ══════════════════════════════════════════════════════════════════════════════
# HTTP
# ══════════════════════════════════════════════════════════════════════════════

def _get(url: str, params: dict | None = None, timeout: float = 15.0) -> dict | list | None:
    time.sleep(random.uniform(0.05, 0.15))
    for attempt in range(2):
        try:
            r = httpx.get(url, params=params, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("OD HTTP %s %s (attempt %d)", exc.response.status_code, url, attempt + 1)
        except Exception as exc:
            logger.warning("OD request error %s (attempt %d): %s", url, attempt + 1, exc)
        if attempt == 0:
            time.sleep(1.0)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# RESPONSE UNWRAPPING
# ══════════════════════════════════════════════════════════════════════════════

def _unwrap_upcoming_response(data: dict | list, fallback_sport_id: int) -> list[dict]:
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    inner = data.get("data")
    if isinstance(inner, list):
        return inner
    if isinstance(inner, dict):
        leagues: list[dict] = inner.get("leagues") or []
        raw_events: list[dict] = []
        for league in leagues:
            comp_name = str(league.get("competition_name") or "")
            cat_name  = str(league.get("category_name")   or "")
            for m in (league.get("matches") or []):
                if not isinstance(m, dict):
                    continue
                if not m.get("competition_name") and comp_name:
                    m["competition_name"] = comp_name
                if not m.get("category_name") and cat_name:
                    m["category_name"] = cat_name
                raw_events.append(m)
        if raw_events:
            return raw_events
        for key in ("events", "matches", "results", "sport_events", "sportevents"):
            if isinstance(inner.get(key), list) and inner[key]:
                return inner[key]
        return []
    for key in ("events", "matches", "results", "sport_events", "sportevents"):
        if isinstance(data.get(key), list) and data[key]:
            return data[key]
    return []


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
# UNIVERSAL MARKET PARSING (VIA BETIKA/SPORTRADAR MAPPER)
# ══════════════════════════════════════════════════════════════════════════════

def _parse_od_specifiers(spec_string: str) -> dict:
    if not spec_string:
        return {}
    parsed = {}
    parts = str(spec_string).split('|')
    for part in parts:
        if '=' in part:
            key, val = part.split('=', 1)
            parsed[key.strip()] = val.strip()
    return parsed


def _flat_outcomes_from_market(mkt: dict) -> list[tuple[dict, str]]:
    results: list[tuple[dict, str]] = []
    market_spec = str(mkt.get("specifiers") or "").strip()

    direct = mkt.get("outcomes")
    if isinstance(direct, list) and direct:
        for o in direct:
            if isinstance(o, dict):
                spec = market_spec or str(o.get("specifiers") or "").strip()
                results.append((o, spec))
        return results

    lines = mkt.get("lines")
    if isinstance(lines, list):
        for line in lines:
            if not isinstance(line, dict):
                continue
            line_spec = str(line.get("specifiers") or "").strip()
            for o in (line.get("outcomes") or []):
                if isinstance(o, dict):
                    spec = line_spec or str(o.get("specifiers") or "").strip()
                    results.append((o, spec))
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
        
    h_norm = home.lower().replace(" ", "_")
    a_norm = away.lower().replace(" ", "_")
    h_norm_space = home.lower()
    a_norm_space = away.lower()
    
    replacements = [
        (h_norm, "1"), (a_norm, "2"),
        (h_norm_space, "1"), (a_norm_space, "2"),
        ("draw", "X")
    ]
    replacements.sort(key=lambda x: len(x[0]), reverse=True)
    
    for t_name, t_val in replacements:
        if len(t_name) > 2 and t_name in ds_lower:
            ds_lower = ds_lower.replace(t_name, t_val)
            
    return ds_lower


def _parse_all_markets(markets_raw: list[dict], sport_slug: str, home_team: str, away_team: str) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}

    for mkt in markets_raw:
        if not isinstance(mkt, dict):
            continue

        mkt_status = mkt.get("status")
        if mkt_status is not None and str(mkt_status) == "0":
            continue

        sid = str(mkt.get("sub_type_id") or mkt.get("type_id") or "")
        mkt_name = str(mkt.get("odd_type") or mkt.get("name") or mkt.get("type_name") or "")

        flat_outcomes = _flat_outcomes_from_market(mkt)

        for o, spec_str in flat_outcomes:
            active = o.get("active")
            status = o.get("status")
            if active is not None and str(active) in ("0", "false"):
                continue
            if status is not None and str(status) == "0":
                continue

            try:
                val = float(o.get("odd_value") or 0)
            except (TypeError, ValueError):
                continue
            if val <= 1.0:
                continue

            parsed_specs = _parse_od_specifiers(spec_str)
            slug = get_market_slug(sport_slug, sid, parsed_specs, fallback_name=mkt_name)
            
            display_str = str(o.get("outcome_key") or o.get("odd_key") or o.get("outcome_name") or o.get("odd_def") or "")
            translated_str = _translate_team_names(display_str, home_team, away_team)
            outcome_key = normalize_outcome(sport_slug, translated_str)

            if slug not in result:
                result[slug] = {}
                
            result[slug][outcome_key] = val

    return result


# ══════════════════════════════════════════════════════════════════════════════
# MATCH NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_match(raw: dict, od_sport_id: int, is_live: bool = False) -> dict | None:
    try:
        match_id = str(
            raw.get("game_id")  or raw.get("id") or
            raw.get("match_id") or raw.get("event_id") or ""
        )
        parent_id   = str(raw.get("parent_match_id") or raw.get("parent_id") or match_id)
        betradar_id = str(raw.get("betradar_id") or raw.get("sr_id") or "") or None

        if not betradar_id and parent_id:
            betradar_id = parent_id

        if not match_id:
            return None

        home        = str(raw.get("home_team") or raw.get("home") or "Home")
        away        = str(raw.get("away_team") or raw.get("away") or "Away")
        competition = str(raw.get("competition_name") or raw.get("competition") or raw.get("league") or "")
        category    = str(
            raw.get("category_name") or raw.get("category") or
            raw.get("country_name")  or raw.get("country") or ""
        )
        raw_sport = raw.get("sport_id") or raw.get("sport") or raw.get("s_binomen")
        od_sport_id_, sport_slug = _resolve_sport(raw_sport, od_sport_id)
        start_time   = str(raw.get("start_time") or raw.get("event_date") or raw.get("date") or "")
        current_score = str(raw.get("current_score") or raw.get("score") or raw.get("result") or "")
        match_time   = str(raw.get("match_time") or raw.get("game_time") or raw.get("periodic_time") or "")
        event_status = str(raw.get("event_status") or raw.get("status_desc") or raw.get("status") or "")
        bet_status   = str(raw.get("bet_status") or raw.get("b_status") or "")

        score_parts = (
            current_score.split(":") if ":" in current_score
            else current_score.split("-") if "-" in current_score
            else []
        )
        score_home = score_parts[0].strip() if len(score_parts) >= 2 else None
        score_away = score_parts[1].strip() if len(score_parts) >= 2 else None

        markets_raw = raw.get("markets") or raw.get("odds") or []
        if isinstance(markets_raw, list):
            markets = _parse_all_markets(markets_raw, sport_slug, home, away)
        elif isinstance(markets_raw, dict):
            markets = markets_raw
        else:
            markets = {}

        if "1x2" not in markets:
            try:
                ho = float(raw.get("home_odd") or raw.get("h_odd") or 0)
                no = float(raw.get("draw_odd") or raw.get("d_odd") or raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or raw.get("a_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    base = f"{sport_slug}_1x2" if sport_slug != "soccer" else "1x2"
                    markets[base] = {
                        k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1
                    }
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
# UPCOMING
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_matches(
    sport_slug:         str        = "soccer",
    day:                str        = "",
    competition_id:     str        = "",
    sub_type_id:        str        = _BROAD_SUB_TYPE_IDS,
    mode:               int        = 1,
    fetch_full_markets: bool       = False,
    fetch_extended:     bool       = False,
    max_matches:        int | None = None,
    **kwargs,
) -> list[dict]:
    od_sport_id = slug_to_od_sport_id(sport_slug)
    if not day:
        day = _date.today().isoformat()
    params: dict[str, Any] = {
        "resource":    "sportevents",
        "platform":    "mobile",
        "mode":        mode,
        "sport_id":    od_sport_id,
        "sub_type_id": sub_type_id,
        "day":         day,
    }
    if competition_id:
        params["competition_id"] = competition_id
        
    data = _get(SBOOK_ODI, params=params)
    if not data:
        logger.warning("OD upcoming %s %s: no response", sport_slug, day)
        return []
        
    raw_events = _unwrap_upcoming_response(data, od_sport_id)
    if not raw_events:
        logger.warning("OD upcoming %s %s: 0 events after unwrap", sport_slug, day)
        return []
        
    matches: list[dict] = []
    for raw in raw_events:
        if not isinstance(raw, dict):
            continue
        m = _normalise_match(raw, od_sport_id, is_live=False)
        if m:
            matches.append(m)
            
    logger.info("OD upcoming %s %s: %d matches", sport_slug, day, len(matches))
    return matches


def fetch_upcoming_all_sports(
    sports: list[str] | None = None,
    day:    str = "",
) -> list[dict]:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    if sports is None:
        sports = ["soccer", "basketball", "tennis", "cricket"]
    all_matches: list[dict] = []
    with ThreadPoolExecutor(max_workers=len(sports)) as pool:
        futs = {pool.submit(fetch_upcoming_matches, s, day): s for s in sports}
        for fut in as_completed(futs):
            try:
                all_matches.extend(fut.result())
            except Exception as exc:
                logger.warning("OD upcoming all sports error: %s", exc)
    return all_matches


# ══════════════════════════════════════════════════════════════════════════════
# LIVE
# ══════════════════════════════════════════════════════════════════════════════

def fetch_live_matches(sport_slug: str | None = None) -> list[dict]:
    params: dict[str, Any] = {
        "resource":    "live",
        "sportsbook":  "sportsbook",
        "ua":          HEADERS["user-agent"],
        "sub_type_id": _BROAD_SUB_TYPE_IDS,
        "sport_id":    "",
    }
    if sport_slug:
        params["sport_id"] = slug_to_od_sport_id(sport_slug)
        
    data = _get(SBOOK_V1, params=params)
    if not data:
        return []
        
    raw_events = _unwrap_live_response(data)
    matches: list[dict] = []
    for raw in raw_events:
        if not isinstance(raw, dict):
            continue
        try:
            raw_sport_id = int(raw.get("sport_id") or 1)
        except (TypeError, ValueError):
            raw_sport_id = 1
        m = _normalise_match(raw, raw_sport_id, is_live=True)
        if m:
            matches.append(m)
            
    logger.info("OD live: %d matches (sport=%s)", len(matches), sport_slug or "all")
    return matches


# ══════════════════════════════════════════════════════════════════════════════
# EVENT DETAIL  (used by sp_cross_bk_enrich)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_event_detail(
    event_id:    str | int,
    od_sport_id: int = 1,
) -> tuple[dict[str, dict[str, float]], dict]:
    """
    Fetch full markets for one OD event using its Sportradar/betradar ID.
    PATCHED: Uses Smart Batching to fetch all missing sub_types in chunks of 20 
    to prevent overwhelming the server while still ensuring 100% market coverage.
    """
    params = {
        "resource":    "sportevent",
        "id":          str(event_id),
        "category_id": "",
        "sub_type_id": "",
        "builder":     0,
        "sportsbook":  "sportsbook",
        "ua":          HEADERS["user-agent"],
    }
    data = _get(SBOOK_V1, params=params)
    if not data or not isinstance(data, dict):
        return {}, {}

    inner = data.get("data")
    if not isinstance(inner, dict):
        inner = data

    info = inner.get("info") or {}
    if not isinstance(info, dict):
        info = {}

    meta: dict = {
        "parent_match_id": str(info.get("parent_match_id") or ""),
        "home_team":       str(info.get("home_team")       or ""),
        "away_team":       str(info.get("away_team")       or ""),
        "competition":     str(info.get("competition_name") or info.get("competition") or ""),
        "category":        str(info.get("category_name")   or info.get("country_name") or ""),
        "start_time":      str(info.get("start_time")      or ""),
        "current_score":   str(info.get("result")          or ""),
        "match_time":      str(info.get("periodic_time")   or ""),
        "event_status":    str(info.get("status_desc")     or ""),
        "bet_status":      str(info.get("b_status")        or ""),
        "sport_id":        str(info.get("sport_id")        or ""),
        "s_binomen":       str(info.get("s_binomen")       or ""),
        "game_id":         str(info.get("game_id")         or ""),
    }

    raw_sport = info.get("s_binomen") or info.get("sport_id")
    od_sport_id_, sport_slug = _resolve_sport(raw_sport, od_sport_id)

    markets_raw = inner.get("markets") or []
    if not isinstance(markets_raw, list):
        markets_raw = []
        
    markets_list = inner.get("markets_list") or []
    
    # CRITICAL FIX: Smart Batching. Group missing sub_types and