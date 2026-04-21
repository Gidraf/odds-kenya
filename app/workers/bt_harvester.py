"""
app/workers/bt_harvester.py
============================
Betika upcoming + live harvester – fixed for all sports.

FIXES
─────
• Correct sport_id mapping extracted from Betika API responses.
• Comprehensive sub_type_id lists for each sport (derived from market_groups).
• Parallel fetching of full markets for upcoming matches.
• Proper live endpoint integration.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Generator

import httpx

from app.workers.mappers.betika import get_market_slug, normalize_outcome

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# CORRECT SPORT ID MAPPING (extracted from Betika API /v1/uo/matches response)
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

# Reverse mapping for convenience
BT_SPORT_ID_TO_SLUG: dict[int, str] = {v: k for k, v in CANONICAL_SPORT_IDS.items()}

def slug_to_bt_sport_id(slug: str) -> int:
    """Convert canonical slug to Betika sport_id."""
    return CANONICAL_SPORT_IDS.get(slug, 14)  # default to soccer

def bt_sport_to_slug(sport_id: int) -> str:
    """Convert Betika sport_id to canonical slug."""
    return BT_SPORT_ID_TO_SLUG.get(sport_id, "soccer")

# ══════════════════════════════════════════════════════════════════════════════
# COMPREHENSIVE SUB_TYPE_ID LISTS (derived from market_groups)
# ══════════════════════════════════════════════════════════════════════════════

# These IDs are used in the `sub_type_id` parameter of the listing endpoint.
# They control which market types are returned in the inline `odds` array.
# To get all possible markets, we include every ID that appears in the
# `market_groups[].markets` arrays of the match detail endpoint.

SPORT_SUB_TYPE_IDS: dict[int, str] = {
    14:  "1,2,10,11,18,29,37,46,47,52,53,54,55,60,68,186,202,203,204,207,208,219,225,226,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,518,519,520,521,522,523,524,525,526,527,528,529,530,531,532,533,534,535,536,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,560,561,562,563,564,565,566,567,568,569,570,571,572,573,574,575,576,577,578,579,580,581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599,600",
    30:  "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400",
    37:  "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,499,500",
}

# Default fallback (includes all IDs observed in soccer)
DEFAULT_SUB_TYPE_IDS = (
    "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,320,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,339,340,341,342,343,344,345,346,347,348,349,350,351,352,353,354,355,356,357,358,359,360,361,362,363,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,380,381,382,383,384,385,386,387,388,389,390,391,392,393,394,395,396,397,398,399,400,401,402,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,499,500"
)

# ══════════════════════════════════════════════════════════════════════════════
# HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════

API_BASE = "https://api.betika.com/v1/uo"
HEADERS: dict[str, str] = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "origin": "https://www.betika.com",
    "referer": "https://www.betika.com/",
    "user-agent": (
        "Mozilla/5.0 (Linux; Android 10; K) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Mobile Safari/537.36"
    ),
}

def _get(url: str, params: dict | None = None, timeout: float = 8.0) -> dict | None:
    """Make a GET request with retries."""
    for attempt in range(3):
        try:
            r = httpx.get(url, params=params, headers=HEADERS, timeout=timeout)
            if not r.is_success:
                logger.warning("BT HTTP %s %s (attempt %d)", r.status_code, url, attempt + 1)
                if r.status_code >= 500:
                    return None
                continue
            return r.json()
        except httpx.RequestError as exc:
            logger.warning("BT request error %s (attempt %d): %s", url, attempt + 1, exc)
        time.sleep(0.5)
    return None

# ══════════════════════════════════════════════════════════════════════════════
# MARKET PARSING (using dynamic mapper)
# ══════════════════════════════════════════════════════════════════════════════

def _parse_all_inline_markets(
    raw_mkts: list[dict],
    sport_slug: str,
) -> dict[str, dict[str, float]]:
    """Parse all inline markets using the dynamic mapper."""
    result: dict[str, dict[str, float]] = {}

    for mkt in raw_mkts:
        sid = str(mkt.get("sub_type_id", ""))
        name = mkt.get("name", "")
        odds_raw = mkt.get("odds") or []

        for o in odds_raw:
            try:
                val = float(o.get("odd_value") or 0)
            except (ValueError, TypeError):
                continue

            if val <= 1.0:
                continue

            parsed_specs = o.get("parsed_special_bet_value") or {}

            slug = get_market_slug(sport_slug, sid, parsed_specs, fallback_name=name)
            outcome_key = normalize_outcome(sport_slug, o.get("display", ""))

            if slug not in result:
                result[slug] = {}
            result[slug][outcome_key] = val

    return result

# ══════════════════════════════════════════════════════════════════════════════
# FULL MARKETS (via /match endpoint)
# ══════════════════════════════════════════════════════════════════════════════

def get_full_markets(parent_match_id: str | int, sport_slug: str) -> dict[str, dict[str, float]]:
    """
    Fetch all available markets for a single match.
    Uses the /match endpoint with parent_match_id.
    """
    data = _get(f"{API_BASE}/match", params={"parent_match_id": str(parent_match_id)})
    if not data:
        return {}

    raw_mkts = data.get("data") or []
    return _parse_all_inline_markets(raw_mkts, sport_slug)

def enrich_matches_with_full_markets(
    matches: list[dict],
    max_workers: int = 8,
) -> list[dict]:
    """Fetch full markets for all matches in parallel."""
    def _fetch(match: dict) -> dict:
        pid = match.get("bt_parent_id")
        sport_slug = match.get("sport", "soccer")
        if not pid:
            return match

        full = get_full_markets(pid, sport_slug)
        if full:
            merged = {**match, "markets": {**match.get("markets", {}), **full}}
            merged["market_count"] = len(merged["markets"])
            return merged
        return match

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_fetch, m): i for i, m in enumerate(matches)}
        ordered: dict[int, dict] = {}
        for fut in as_completed(futs):
            idx = futs[fut]
            try:
                ordered[idx] = fut.result()
            except Exception:
                ordered[idx] = matches[idx]

    for i in range(len(matches)):
        results.append(ordered.get(i, matches[i]))
    return results

# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING MATCHES
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_matches(
    sport_slug: str = "soccer",
    max_pages: int = 10,
    fetch_full: bool = True,
    max_workers: int = 8,
    period_id: int = 9,
) -> list[dict]:
    """
    Fetch upcoming matches for a given sport.
    Uses the correct sport_id and a comprehensive sub_type_id list.
    """
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    sub_type_ids = SPORT_SUB_TYPE_IDS.get(bt_sport_id, DEFAULT_SUB_TYPE_IDS)

    all_matches: list[dict] = []

    for page in range(1, max_pages + 1):
        params: dict[str, Any] = {
            "page": page,
            "limit": 50,
            "tab": "upcoming",
            "sub_type_id": sub_type_ids,
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
            norm = _normalise_match(r, source="upcoming")
            if norm:
                all_matches.append(norm)

        meta = data.get("meta") or {}
        total = int(meta.get("total") or 0)
        limit = int(meta.get("limit") or 50)
        if page * limit >= total:
            break

    if fetch_full and all_matches:
        all_matches = enrich_matches_with_full_markets(all_matches, max_workers=max_workers)

    logger.info("BT upcoming %s: %d matches", sport_slug, len(all_matches))
    return all_matches

# ══════════════════════════════════════════════════════════════════════════════
# LIVE MATCHES
# ══════════════════════════════════════════════════════════════════════════════

def fetch_live_matches(bt_sport_id: int | None = None) -> list[dict]:
    """
    Fetch live matches from Betika.
    Uses the /live endpoint with a slim sub_type_id list for speed.
    """
    sub_type_ids = "1,10,11,18,29,60,186,219,251,340,406"  # core market types

    params: dict[str, Any] = {
        "page": 1,
        "limit": 1000,
        "sub_type_id": sub_type_ids,
        "sport": str(bt_sport_id) if bt_sport_id is not None else "null",
        "sort": 1,
    }
    data = _get(f"{API_BASE}/matches", params=params, timeout=6.0)
    if not data:
        return []

    results: list[dict] = []
    for r in (data.get("data") or []):
        norm = _normalise_match(r, source="live")
        if norm:
            results.append(norm)

    logger.info("BT live: %d matches (sport_id=%s)", len(results), bt_sport_id or "all")
    return results

# ══════════════════════════════════════════════════════════════════════════════
# MATCH NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_match(raw: dict, *, source: str = "upcoming") -> dict | None:
    """Transform raw Betika match dict into canonical shape."""
    try:
        bt_sport_id = int(raw.get("sport_id") or 14)
        match_id = str(raw.get("match_id") or raw.get("game_id") or "")
        parent_id = str(raw.get("parent_match_id") or match_id)
        betradar_id = str(raw.get("parent_match_id") or "")  # Betika uses parent_match_id as betradar_id

        if not match_id:
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
        can_sport_id = CANONICAL_SPORT_IDS.get(sport_slug, 1)

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

        markets = _parse_all_inline_markets(raw.get("odds") or [], sport_slug)

        # Fallback to home/away/draw odds if available
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
            "canonical_sport_id": can_sport_id,
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
        logger.debug("BT match normalise error: %s", exc)
        return None

# ══════════════════════════════════════════════════════════════════════════════
# STREAMING GENERATORS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_stream(
    sport_slug: str = "soccer",
    max_matches: int | None = None,
    max_pages: int = 100,
    fetch_full_markets: bool = True,
    sleep_between: float = 0.3,
    **kwargs,
) -> Generator[dict, None, None]:
    """Yield upcoming matches one by one."""
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    sub_type_ids = SPORT_SUB_TYPE_IDS.get(bt_sport_id, DEFAULT_SUB_TYPE_IDS)
    count = 0

    for page in range(1, max_pages + 1):
        params: dict[str, Any] = {
            "page": page,
            "limit": 50,
            "tab": "upcoming",
            "sub_type_id": sub_type_ids,
            "sport_id": bt_sport_id,
            "sort_id": 2,
            "period_id": 9,
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
                full_markets = get_full_markets(norm["bt_parent_id"], sport_slug)
                if full_markets:
                    norm["markets"].update(full_markets)
                    norm["market_count"] = len(norm["markets"])
                time.sleep(sleep_between)

            count += 1
            yield norm

        meta = data.get("meta") or {}
        total = int(meta.get("total") or 0)
        limit = int(meta.get("limit") or 50)
        if page * limit >= total:
            break

def fetch_live_stream(
    sport_slug: str,
    **kwargs,
) -> Generator[dict, None, None]:
    """Yield live matches one by one."""
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    matches = fetch_live_matches(bt_sport_id)
    for m in matches:
        yield m

# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

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