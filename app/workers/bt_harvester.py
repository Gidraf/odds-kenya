# app/workers/bt_harvester.py

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Generator

import httpx

from app.workers.mappers.betika import get_market_slug
from app.workers.mappers.shared import normalize_outcome

logger = logging.getLogger(__name__)

# FORCE PRINT TO CONSOLE (stderr) FOR ALL DEBUG STEPS
def _debug(msg: str):
    print(f"[BT DEBUG] {msg}", file=sys.stderr)
    logger.debug(msg)


# ══════════════════════════════════════════════════════════════════════════════
# CORRECT SPORT ID MAPPING (from Betika live sports endpoint)
# ══════════════════════════════════════════════════════════════════════════════

CANONICAL_SPORT_IDS: dict[str, int] = {
    "soccer":            14,
    "tennis":            28,
    "ice-hockey":        29,
    "basketball":        30,
    "baseball":          31,
    "handball":          33,
    "snooker":           34,
    "volleyball":        35,
    "mma":               36,
    "cricket":           37,
    "waterpolo":         38,
    "boxing":            39,
    "futsal":            40,
    "rugby":             41,
    "aussie-rules":      43,
    "darts":             44,
    "table-tennis":      45,
    "floorball":         84,
    "squash":            85,
    "esoccer":           105,
    "esport-king-glory": 97,
    "esport-cs":         132,
    "esport-dota":       133,
    "esport-lol":        134,
    "esport-cod":        137,
}

BT_SPORT_ID_TO_SLUG: dict[int, str] = {v: k for k, v in CANONICAL_SPORT_IDS.items()}

def slug_to_bt_sport_id(slug: str) -> int:
    return CANONICAL_SPORT_IDS.get(slug, 14)

def bt_sport_to_slug(sport_id: int) -> str:
    return BT_SPORT_ID_TO_SLUG.get(sport_id, "soccer")

# ══════════════════════════════════════════════════
# HELPER: map days to Betika period_id
# ══════════════════════════════════════════════════
def days_to_period_id(days: int) -> int:
    if days <= 1:
        return -1
    if days <= 2:
        return -2
    return 9

# ══════════════════════════════════════════════════
# COMPREHENSIVE SUB_TYPE_ID LISTS
# ══════════════════════════════════════════════════
_ALL_SUB_TYPE_IDS = ",".join(str(i) for i in range(1, 501))
_LIVE_SUB_TYPE_IDS = "1,186,340"

# ══════════════════════════════════════════════════
# HTTP HELPERS
# ══════════════════════════════════════════════════
API_BASE = "https://api.betika.com/v1/uo"
HEADERS: dict[str, str] = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "origin": "https://www.betika.com",
    "referer": "https://www.betika.com/",
    "user-agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36",
}

def _get(url: str, params: dict | None = None, timeout: float = 8.0) -> dict | None:
    _debug(f"REQUEST: GET {url} params={params}")
    for attempt in range(3):
        try:
            r = httpx.get(url, params=params, headers=HEADERS, timeout=timeout)
            _debug(f"RESPONSE: status={r.status_code}, content-length={len(r.content)}")
            if not r.is_success:
                _debug(f"HTTP {r.status_code} -> {url}")
                if r.status_code >= 500:
                    return None
                continue
            # Log first 500 chars of response
            resp_text = r.text[:500] + ("..." if len(r.text) > 500 else "")
            _debug(f"RESPONSE BODY (first 500): {resp_text}")
            return r.json()
        except httpx.RequestError as exc:
            _debug(f"Request error: {exc}")
        time.sleep(0.5)
    return None

# ══════════════════════════════════════════════════
# MARKET PARSING – using shared outcome normaliser
# ══════════════════════════════════════════════════
def _parse_all_inline_markets(raw_mkts: list[dict], sport_slug: str) -> dict[str, dict[str, float]]:
    _debug(f"Parsing {len(raw_mkts)} inline markets for {sport_slug}")
    result: dict[str, dict[str, float]] = {}
    for mkt in raw_mkts:
        sid = str(mkt.get("sub_type_id", ""))
        name = mkt.get("name", "")
        odds_raw = mkt.get("odds") or []
        for o in odds_raw:
            try:
                val = float(o.get("odd_value") or 0)
            except (TypeError, ValueError):
                continue
            if val <= 1.0:
                continue
            parsed_specs = o.get("parsed_special_bet_value") or {}
            slug = get_market_slug(sport_slug, sid, parsed_specs, fallback_name=name)
            outcome_key = normalize_outcome(slug, o.get("display", ""))
            if slug not in result:
                result[slug] = {}
            result[slug][outcome_key] = val
    _debug(f"Parsed {len(result)} unique market slugs")
    return result

# ══════════════════════════════════════════════════
# FULL MARKETS (via /match endpoint)
# ══════════════════════════════════════════════════
def get_full_markets(parent_match_id: str | int, sport_slug: str) -> dict[str, dict[str, float]]:
    _debug(f"Fetching full markets for parent_match_id={parent_match_id}, sport={sport_slug}")
    data = _get(f"{API_BASE}/match", params={"parent_match_id": str(parent_match_id)})
    if not data:
        _debug(f"No data returned for match {parent_match_id}")
        return {}
    raw_mkts = data.get("data") or []
    _debug(f"Got {len(raw_mkts)} markets")
    return _parse_all_inline_markets(raw_mkts, sport_slug)

def enrich_matches_with_full_markets(matches: list[dict], max_workers: int = 8) -> list[dict]:
    _debug(f"Enriching {len(matches)} matches (workers={max_workers})")
    def _fetch(match: dict) -> dict:
        pid = match.get("bt_parent_id")
        sport_slug = match.get("sport", "soccer")
        if not pid:
            return match
        full = get_full_markets(pid, sport_slug)
        if full:
            match["markets"].update(full)
            match["market_count"] = len(match["markets"])
        return match
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        enriched = list(pool.map(_fetch, matches))
    return enriched

# ══════════════════════════════════════════════════
# MATCH NORMALISATION
# ══════════════════════════════════════════════════
def _normalise_match(raw: dict, *, source: str = "upcoming", override_sport_id: int | None = None) -> dict | None:
    _debug(f"Normalising match from {source}: raw keys = {list(raw.keys())}")
    try:
        bt_sport_id = override_sport_id or int(raw.get("sport_id") or 14)
        match_id = str(raw.get("match_id") or raw.get("game_id") or "")
        parent_id = str(raw.get("parent_match_id") or match_id)
        betradar_id = str(raw.get("parent_match_id") or "")
        if not match_id:
            _debug("Skipping: no match_id")
            return None

        home = str(raw.get("home_team") or "").strip()
        away = str(raw.get("away_team") or "").strip()
        competition = str(raw.get("competition_name") or "").strip()
        category = str(raw.get("category") or "").strip()
        start_time = str(raw.get("start_time") or "")
        sport_name = str(raw.get("sport_name") or "").strip()
        if start_time and " " in start_time:
            start_time = start_time.replace(" ", "T")

        sport_slug = bt_sport_to_slug(bt_sport_id)
        _debug(f"Sport mapping: raw_sport_id={bt_sport_id} -> slug={sport_slug}")

        is_live = source == "live"
        match_time = str(raw.get("match_time") or "").strip()
        event_status = str(raw.get("event_status") or "").strip()
        match_status = str(raw.get("match_status") or "").strip()
        bet_status = str(raw.get("bet_status") or "").strip()
        bet_stop = str(raw.get("bet_stop_reason") or "").strip()
        current_score = str(raw.get("current_score") or "").strip()
        ht_score = str(raw.get("ht_score") or "").strip()

        score_home = score_away = None
        if current_score and "-" in current_score:
            parts = current_score.split("-", 1)
            try:
                score_home = parts[0].strip()
                score_away = parts[1].strip()
            except IndexError:
                pass

        # Parse inline odds (markets)
        markets = _parse_all_inline_markets(raw.get("odds") or [], sport_slug)

        # Fallback 1x2 from top-level odds
        if "home_odd" in raw and not any(k.endswith("1x2") for k in markets):
            try:
                ho = float(raw.get("home_odd") or 0)
                no = float(raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    base = f"{sport_slug}_1x2" if sport_slug != "soccer" else "1x2"
                    markets[base] = {k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1}
            except (TypeError, ValueError):
                pass

        _debug(f"Match {match_id} normalised with {len(markets)} markets")
        return {
            "bt_match_id": match_id,
            "bt_parent_id": parent_id,
            "betradar_id": betradar_id,
            "home_team": home,
            "away_team": away,
            "competition": competition,
            "category": category,
            "sport_name": sport_name,
            "sport": sport_slug,
            "bt_sport_id": bt_sport_id,
            "canonical_sport_id": bt_sport_id,
            "start_time": start_time,
            "source": "betika",
            "is_live": is_live,
            "is_suspended": bet_status in ("STOPPED", "BET_STOP"),
            "match_time": match_time,
            "event_status": event_status,
            "match_status": match_status,
            "bet_status": bet_status,
            "bet_stop_reason": bet_stop,
            "current_score": current_score,
            "score_home": score_home,
            "score_away": score_away,
            "ht_score": ht_score,
            "markets": markets,
            "market_count": len(markets),
        }
    except Exception as exc:
        _debug(f"Normalisation error: {exc}")
        logger.debug("BT match normalise error: %s", exc)
        return None

# ══════════════════════════════════════════════════
# UPCOMING MATCHES
# ══════════════════════════════════════════════════
def fetch_upcoming_matches(
    sport_slug: str = "soccer",
    days: int = 30,
    max_pages: int = 30,
    fetch_full: bool = True,
    max_workers: int = 8,
) -> list[dict]:
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    period_id = days_to_period_id(days)
    _debug(f"Starting upcoming fetch: sport={sport_slug} -> bt_sport_id={bt_sport_id}, period_id={period_id}")
    all_matches: list[dict] = []

    for page in range(1, max_pages + 1):
        _debug(f"Fetching page {page}")
        params = {
            "page": page,
            "limit": 50,
            "tab": "upcoming",
            "sub_type_id": _ALL_SUB_TYPE_IDS,
            "sport_id": bt_sport_id,
            "sort_id": 2,
            "period_id": period_id,
            "esports": "false",
        }
        data = _get(f"{API_BASE}/matches", params=params, timeout=8.0)
        if not data:
            _debug("No data from API - breaking")
            break
        raw = data.get("data") or []
        meta = data.get("meta") or {}
        total = int(meta.get("total") or 0)
        _debug(f"Page {page}: total={total}, received {len(raw)} raw matches")
        if not raw:
            _debug("No raw matches on this page - breaking")
            break
        for r in raw:
            norm = _normalise_match(r, source="upcoming")
            if norm:
                all_matches.append(norm)
        _debug(f"After page {page}, total normalised = {len(all_matches)}")
        limit = int(meta.get("limit") or 50)
        if page * limit >= total:
            _debug("Reached last page")
            break

    _debug(f"Fetched {len(all_matches)} matches before enrichment")
    if fetch_full and all_matches:
        _debug("Enriching with full markets")
        all_matches = enrich_matches_with_full_markets(all_matches, max_workers=max_workers)
        _debug(f"After enrichment: {len(all_matches)} matches")

    logger.info("BT upcoming %s (days=%d, period_id=%d): %d matches", sport_slug, days, period_id, len(all_matches))
    return all_matches

# ══════════════════════════════════════════════════
# LIVE MATCHES
# ══════════════════════════════════════════════════
def fetch_live_matches(bt_sport_id: int | None = None) -> list[dict]:
    _debug(f"Fetching live matches for bt_sport_id={bt_sport_id}")
    params: dict[str, Any] = {
        "page": 1,
        "limit": 1000,
        "sub_type_id": _LIVE_SUB_TYPE_IDS,
        "sort": 1,
    }
    if bt_sport_id is not None:
        params["sport"] = bt_sport_id
    data = _get(f"{API_BASE}/matches", params=params, timeout=6.0)
    if not data:
        _debug("No data from live API")
        return []
    raw_matches = data.get("data") or []
    _debug(f"Live API returned {len(raw_matches)} raw events")
    results: list[dict] = []
    for raw in raw_matches:
        raw_sport_id = raw.get("sport_id")
        if raw_sport_id is None and bt_sport_id is not None:
            raw_sport_id = bt_sport_id
        else:
            raw_sport_id = raw_sport_id or 14
        try:
            raw_sport_id = int(raw_sport_id)
        except (TypeError, ValueError):
            raw_sport_id = 14
        norm = _normalise_match(raw, source="live", override_sport_id=raw_sport_id)
        if norm:
            results.append(norm)
    _debug(f"Live matches normalised: {len(results)}")
    return results

# ══════════════════════════════════════════════════
# STREAMING GENERATORS (unchanged)
# ══════════════════════════════════════════════════
def fetch_upcoming_stream(
    sport_slug: str = "soccer",
    days: int = 30,
    max_matches: int | None = None,
    max_pages: int = 30,
    fetch_full_markets: bool = True,
    sleep_between: float = 0.3,
    **kwargs,
) -> Generator[dict, None, None]:
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    period_id = days_to_period_id(days)
    count = 0
    for page in range(1, max_pages + 1):
        params = {
            "page": page,
            "limit": 50,
            "tab": "upcoming",
            "sub_type_id": _ALL_SUB_TYPE_IDS,
            "sport_id": bt_sport_id,
            "sort_id": 2,
            "period_id": period_id,
            "esports": "false",
        }
        data = _get(f"{API_BASE}/matches", params=params, timeout=8.0)
        if not data:
            break
        raw = data.get("data") or []
        if not raw:
            break
        for r in raw:
            if max_matches and count >= max_matches:
                return
            norm = _normalise_match(r, source="upcoming")
            if not norm:
                continue
            if fetch_full_markets and norm.get("bt_parent_id"):
                full = get_full_markets(norm["bt_parent_id"], sport_slug)
                if full:
                    norm["markets"].update(full)
                    norm["market_count"] = len(norm["markets"])
                time.sleep(sleep_between)
            count += 1
            yield norm
        meta = data.get("meta") or {}
        total = int(meta.get("total") or 0)
        limit = int(meta.get("limit") or 50)
        if page * limit >= total:
            break

def fetch_live_stream(sport_slug: str, **kwargs) -> Generator[dict, None, None]:
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    matches = fetch_live_matches(bt_sport_id)
    for m in matches:
        yield m

__all__ = [
    "fetch_upcoming_matches",
    "fetch_live_matches",
    "fetch_upcoming_stream",
    "fetch_live_stream",
    "get_full_markets",
    "CANONICAL_SPORT_IDS",
    "slug_to_bt_sport_id",
    "bt_sport_to_slug",
]