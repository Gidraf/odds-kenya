"""
app/workers/betika_harvester.py
================================
Betika live & upcoming match harvester.

Mirrors the SportPesa harvester pattern:
  • Polls live.betika.com  every ~1 s  (live matches + sports list)
  • Polls api.betika.com   every 30 s  (upcoming matches, per sport)
  • Normalises odds via normalize_bt_market / normalize_outcome
  • Enriches each match with canonical market slugs + display labels
  • Emits normalised payloads via the project's emit helper

Betika sport-ID → canonical sport-ID map
─────────────────────────────────────────
  14  Soccer          →  1
  30  Basketball      →  2
  28  Tennis          →  5
  29  Ice Hockey      →  4
  41  Rugby           →  12
  33  Handball        →  6
  35  Volleyball      →  23
  37  Cricket         →  21
  45  Table Tennis    →  16
  32  Badminton       →  5   (nearest: tennis-style)
  105 eSoccer         →  126
  107 eBasketball     →  2
  40  Futsal          →  1   (soccer rules)
  134 ESport LoL      →  126
  132 ESport CS       →  126

Endpoints used
──────────────
  LIVE    https://live.betika.com/v1/uo/matches?page=1&limit=1000&sub_type_id=1,186,340&sport=null&sort=1
  SPORTS  https://live.betika.com/v1/uo/sports
  UPC     https://api.betika.com/v1/uo/matches?page={p}&limit=50&tab=upcoming&sub_type_id=1,186,340&sport_id={sid}&sort_id=2&period_id=9&esports=false
  MKTS    https://api.betika.com/v1/uo/match?parent_match_id={pid}
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any

import httpx

# ── project imports ────────────────────────────────────────────────────────────
# Adjust the import paths to match your actual project layout.
from app.workers.canonical_mapper import (

    normalize_outcome,
    slug_with_line,
)
from app.workers.sp_mapper import (
    MARKET_DISPLAY_NAMES,
    OUTCOME_DISPLAY,
    get_market_display_name,
    get_outcome_display,
)

# Replace with your actual emit / cache / celery imports:
# from app.core.celery_app import celery_app
# from app.core.emit import emit_event
# from app.core.cache import redis_client

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

LIVE_BASE  = "https://live.betika.com/v1/uo"
API_BASE   = "https://api.betika.com/v1/uo"

LIVE_MATCHES_URL  = f"{LIVE_BASE}/matches"
LIVE_SPORTS_URL   = f"{LIVE_BASE}/sports"
UPCOMING_URL      = f"{API_BASE}/matches"
MATCH_MARKETS_URL = f"{API_BASE}/match"

# Default query params for 1X2 / winner market in live feed
LIVE_PARAMS: dict[str, Any] = {
    "page":        1,
    "limit":       1000,
    "sub_type_id": "1,186,340",
    "sport":       "null",
    "sort":        1,
}

UPCOMING_PARAMS_BASE: dict[str, Any] = {
    "page":        1,
    "limit":       50,
    "tab":         "upcoming",
    "sub_type_id": "1,186,340",
    "sort_id":     2,
    "period_id":   9,         # "All" period
    "esports":     "false",
}

# Sub-type IDs that carry a line value in their specValue / special_bet_value
LINE_MARKET_SUBTYPES: frozenset[int] = frozenset({
    14, 15, 16, 18, 19, 20, 36, 37, 51, 52, 53, 54, 55, 56,
    58, 59, 65, 66, 68, 69, 70, 71, 74, 79, 86, 87, 88, 90,
    91, 92, 93, 94, 95, 98, 138, 139, 142, 166, 167, 168,
    172, 177, 226, 339, 340, 342, 362, 363, 364, 365,
    366, 367, 368, 369, 439, 544, 547, 549, 550,
})

# Betika sport_id → canonical (sp_mapper) sport_id
BT_TO_CANONICAL_SPORT: dict[int, int] = {
    14:  1,    # Soccer
    30:  2,    # Basketball
    28:  5,    # Tennis
    29:  4,    # Ice Hockey
    41:  12,   # Rugby
    33:  6,    # Handball
    35:  23,   # Volleyball
    37:  21,   # Cricket
    45:  16,   # Table Tennis
    32:  5,    # Badminton (nearest)
    105: 126,  # eSoccer
    107: 2,    # eBasketball
    40:  1,    # Futsal
    134: 126,  # ESport LoL
    132: 126,  # ESport CS
}

HEADERS: dict[str, str] = {
    "accept":          "application/json, text/plain, */*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "origin":          "https://www.betika.com",
    "referer":         "https://www.betika.com/",
    "user-agent": (
        "Mozilla/5.0 (Linux; Android 10; K) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
}

# =============================================================================
# HTTP HELPERS
# =============================================================================

def _get(url: str, params: dict | None = None, timeout: float = 8.0) -> dict | None:
    """GET with retry (2 attempts). Returns parsed JSON or None on failure."""
    for attempt in range(2):
        try:
            resp = httpx.get(url, params=params, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("Betika HTTP %s for %s (attempt %d)", exc.response.status_code, url, attempt + 1)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Betika request error %s (attempt %d): %s", url, attempt + 1, exc)
        if attempt == 0:
            time.sleep(0.4)
    return None


# =============================================================================
# NORMALISATION HELPERS
# =============================================================================

def _extract_line_from_odd(odd: dict) -> str:
    """
    Pull the line/specifier from an individual odd entry.

    Betika stores it in ``special_bet_value`` as "total=2.5" or "hcp=0:1".
    We also inspect ``odd_def`` for "{total}" / "{hcp}" templates.
    """
    sbv = odd.get("special_bet_value", "") or ""
    if sbv:
        # "total=2.5" → "2.5",  "hcp=0:1" → "0:1"
        if "=" in sbv:
            return sbv.split("=", 1)[-1].strip()
        return sbv.strip()
    # Fallback: parse from display label "OVER 2.5" / "UNDER 1.75"
    display = odd.get("display", "") or ""
    import re
    m = re.match(r"^(?:over|under)\s+([\d.]+)$", display.strip(), re.I)
    if m:
        return m.group(1)
    return ""


def _normalise_odd_key(raw_key: str, display: str, market_slug: str) -> str:
    """Return a canonical outcome key for a single odd."""
    return normalize_outcome(market_slug, raw_key, display)


def _normalise_market(mkt: dict) -> dict | None:
    """
    Transform one Betika market dict into our canonical structure.

    Input shape (from /v1/uo/match):
      {
        "sub_type_id": "18",
        "name": "TOTAL",
        "odds": [
          { "display": "OVER 2.5", "odd_key": "over 2.5",
            "odd_value": "1.56", "special_bet_value": "total=2.5", ... }
        ]
      }

    Output shape:
      {
        "slug":         "over_under_goals_2.5",
        "base_slug":    "over_under_goals",
        "name":         "Goals O/U 2.5",
        "sub_type_id":  18,
        "outcomes": [
          { "key": "over", "display": "Over 2.5", "odds": 1.56,
            "odd_key": "over 2.5", "outcome_id": "12", "active": true }
        ]
      }
    """
    try:
        sid       = int(mkt.get("sub_type_id", 0))
        mkt_name  = (mkt.get("name") or "").strip()
        odds_list = mkt.get("odds") or []

        if not odds_list:
            return None

        # -- Determine specifier from first odd that has one ------------------
        specifier = ""
        for o in odds_list:
            specifier = _extract_line_from_odd(o)
            if specifier:
                break

        # -- Canonical base slug ----------------------------------------------
        base_slug = normalize_bt_market(mkt_name, sid)

        # -- Full slug with line if applicable --------------------------------
        slug = (
            slug_with_line(base_slug, specifier)
            if (sid in LINE_MARKET_SUBTYPES and specifier)
            else base_slug
        )

        # -- Human display name -----------------------------------------------
        display_name = get_market_display_name(slug)

        # -- Outcomes ---------------------------------------------------------
        outcomes: list[dict] = []
        for o in odds_list:
            raw_key    = (o.get("odd_key") or "").strip()
            raw_display = (o.get("display") or "").strip()
            canonical  = _normalise_odd_key(raw_key, raw_display, slug)
            outcome_display = get_outcome_display(slug, canonical)
            try:
                odd_value = float(o.get("odd_value") or 0)
            except (TypeError, ValueError):
                odd_value = 0.0

            outcomes.append({
                "key":        canonical,
                "display":    outcome_display,
                "odds":       odd_value,
                "odd_key":    raw_key,
                "outcome_id": str(o.get("outcome_id") or ""),
                "active":     odd_value > 1.0,
            })

        if not outcomes:
            return None

        return {
            "slug":        slug,
            "base_slug":   base_slug,
            "name":        display_name,
            "sub_type_id": sid,
            "outcomes":    outcomes,
        }

    except Exception as exc:  # noqa: BLE001
        logger.debug("Betika market normalise error: %s | mkt=%s", exc, mkt)
        return None


def _normalise_match(raw: dict, *, source: str = "live") -> dict | None:
    """
    Transform a single Betika match dict into our canonical event shape.

    Works for both /live/matches entries and /api/matches entries.
    The inline ``odds`` array contains only the sub_type_ids requested
    (1, 186, 340) — i.e. 1X2 / Winner.  Full markets require a separate
    call to get_match_markets().
    """
    try:
        match_id      = str(raw.get("match_id") or raw.get("game_id") or "")
        parent_id     = str(raw.get("parent_match_id") or "")
        bt_sport_id   = int(raw.get("sport_id") or 14)
        can_sport_id  = BT_TO_CANONICAL_SPORT.get(bt_sport_id, 1)
        home          = str(raw.get("home_team") or "").strip()
        away          = str(raw.get("away_team") or "").strip()
        competition   = str(raw.get("competition_name") or "").strip()
        category      = str(raw.get("category") or "").strip()
        start_time    = str(raw.get("start_time") or "")
        sport_name    = str(raw.get("sport_name") or "").strip()

        # Live-specific fields
        match_time    = str(raw.get("match_time") or "").strip()
        event_status  = str(raw.get("event_status") or "").strip()
        match_status  = str(raw.get("match_status") or "").strip()
        bet_status    = str(raw.get("bet_status") or "").strip()
        bet_stop      = str(raw.get("bet_stop_reason") or "").strip()
        current_score = str(raw.get("current_score") or "").strip()
        ht_score      = str(raw.get("ht_score") or "").strip()
        ft_score      = str(raw.get("ft_score") or "").strip()
        set_scores    = raw.get("set_score") or []

        # Cards / corners (live only)
        home_red      = int(raw.get("home_red_card")    or 0)
        away_red      = int(raw.get("away_red_card")    or 0)
        home_yellow   = int(raw.get("home_yellow_card") or 0)
        away_yellow   = int(raw.get("away_yellow_card") or 0)
        home_corners  = int(raw.get("home_corners")     or 0)
        away_corners  = int(raw.get("away_corners")     or 0)

        # Inline markets (1X2 only in the list feed)
        inline_mkts: list[dict] = []
        for mkt in (raw.get("odds") or []):
            norm = _normalise_market(mkt)
            if norm:
                inline_mkts.append(norm)

        # Scalar odds shortcut (home/away/draw from top-level fields)
        home_odd    = raw.get("home_odd",    raw.get("home_odd_key"))
        away_odd    = raw.get("away_odd",    raw.get("away_odd_key"))
        neutral_odd = raw.get("neutral_odd", raw.get("neutral_odd_key"))

        return {
            # identity
            "match_id":       match_id,
            "parent_match_id": parent_id,
            "source":         "betika",
            "feed":           source,          # "live" | "upcoming"
            # teams / competition
            "home_team":      home,
            "away_team":      away,
            "competition":    competition,
            "category":       category,
            "sport_name":     sport_name,
            "sport_id":       bt_sport_id,
            "canonical_sport_id": can_sport_id,
            "start_time":     start_time,
            # live state
            "match_time":     match_time,
            "event_status":   event_status,
            "match_status":   match_status,
            "bet_status":     bet_status,
            "bet_stop_reason": bet_stop,
            "is_live":        source == "live",
            "is_suspended":   bet_status in ("STOPPED", "BET_STOP"),
            # scores
            "current_score":  current_score,
            "ht_score":       ht_score,
            "ft_score":       ft_score,
            "set_scores":     set_scores,
            # cards / corners
            "home_red_cards":    home_red,
            "away_red_cards":    away_red,
            "home_yellow_cards": home_yellow,
            "away_yellow_cards": away_yellow,
            "home_corners":      home_corners,
            "away_corners":      away_corners,
            # inline odds (quick access)
            "home_odd":    home_odd,
            "away_odd":    away_odd,
            "neutral_odd": neutral_odd,
            # markets
            "markets":  inline_mkts,
        }

    except Exception as exc:  # noqa: BLE001
        logger.debug("Betika match normalise error: %s", exc)
        return None


# =============================================================================
# FULL MARKETS FETCH
# =============================================================================

def get_match_markets(parent_match_id: str | int) -> list[dict]:
    """
    Fetch all available markets for a match and return normalised list.

    Hits: GET /v1/uo/match?parent_match_id={parent_match_id}
    Returns a list of canonical market dicts (same shape as _normalise_market).
    """
    data = _get(MATCH_MARKETS_URL, params={"parent_match_id": str(parent_match_id)})
    if not data:
        return []

    raw_mkts = data.get("data") or []
    result: list[dict] = []
    for mkt in raw_mkts:
        norm = _normalise_market(mkt)
        if norm:
            result.append(norm)
    return result


# =============================================================================
# LIVE FEED FETCHER
# =============================================================================

def fetch_live_sports() -> list[dict]:
    """
    Fetch current live sport counts.
    GET https://live.betika.com/v1/uo/sports
    """
    data = _get(LIVE_SPORTS_URL)
    if not data:
        return []
    return data.get("data") or []


def fetch_live_matches(sport_id: int | None = None) -> list[dict]:
    """
    Fetch all live matches (or filter by sport_id).
    GET https://live.betika.com/v1/uo/matches?...

    Returns a list of normalised match dicts.
    """
    params = dict(LIVE_PARAMS)
    if sport_id is not None:
        params["sport"] = sport_id

    data = _get(LIVE_MATCHES_URL, params=params)
    if not data:
        return []

    raw_matches = data.get("data") or []
    result: list[dict] = []
    for raw in raw_matches:
        norm = _normalise_match(raw, source="live")
        if norm:
            result.append(norm)

    logger.info(
        "Betika live: fetched %d/%d matches%s",
        len(result), len(raw_matches),
        f" (sport {sport_id})" if sport_id else "",
    )
    return result


# =============================================================================
# UPCOMING FEED FETCHER
# =============================================================================

def fetch_upcoming_matches(
    sport_id:    int   = 14,
    max_pages:   int   = 5,
    period_id:   int   = 9,
) -> list[dict]:
    """
    Fetch upcoming matches for one sport across multiple pages.
    GET https://api.betika.com/v1/uo/matches?tab=upcoming&sport_id={sport_id}...

    Returns a list of normalised match dicts.
    """
    all_matches: list[dict] = []

    for page in range(1, max_pages + 1):
        params = {
            **UPCOMING_PARAMS_BASE,
            "sport_id":  sport_id,
            "period_id": period_id,
            "page":      page,
        }
        data = _get(UPCOMING_URL, params=params)
        if not data:
            break

        raw_matches = data.get("data") or []
        if not raw_matches:
            break

        for raw in raw_matches:
            norm = _normalise_match(raw, source="upcoming")
            if norm:
                all_matches.append(norm)

        # Check if we've reached last page
        meta    = data.get("meta") or {}
        total   = int(meta.get("total") or 0)
        limit   = int(meta.get("limit") or 50)
        fetched = page * limit
        if fetched >= total:
            break

    logger.info("Betika upcoming: fetched %d matches for sport_id=%d", len(all_matches), sport_id)
    return all_matches


# =============================================================================
# PAYLOAD HASH  (skip emit when nothing changed)
# =============================================================================

def _payload_hash(matches: list[dict]) -> str:
    """MD5 over match IDs + bet_status + current_score + odds — cheap change detect."""
    fragments = []
    for m in matches:
        mkt_fragment = "|".join(
            f"{mk['slug']}:{o['odds']}"
            for mk in (m.get("markets") or [])
            for o  in (mk.get("outcomes") or [])
        )
        fragments.append(
            f"{m['match_id']}:{m.get('bet_status')}:"
            f"{m.get('current_score')}:{m.get('match_time')}:{mkt_fragment}"
        )
    digest = hashlib.md5("\n".join(fragments).encode()).hexdigest()  # noqa: S324
    return digest


# =============================================================================
# CELERY TASKS
# =============================================================================
# Uncomment / adapt once you wire in your celery_app, redis_client, emit_event.

# from app.core.celery_app import celery_app
# from app.core.cache import redis_client
# from app.core.emit import emit_event

# _LIVE_HASH_KEY     = "betika:live:hash"
# _UPCOMING_HASH_KEY = "betika:upcoming:hash:{sport_id}"
# _LIVE_DATA_KEY     = "betika:live:data"


# @celery_app.task(
#     name="betika.poll_live",
#     bind=True,
#     max_retries=None,
#     soft_time_limit=8,
#     time_limit=10,
# )
# def poll_betika_live(self):
#     """
#     Poll Betika live feed every second and emit changes.
#     Schedule via Celery beat: every 1 second.
#     """
#     matches = fetch_live_matches()
#     if not matches:
#         return
#
#     new_hash = _payload_hash(matches)
#     old_hash = redis_client.get(_LIVE_HASH_KEY)
#
#     if new_hash == old_hash:
#         return  # nothing changed – skip emit
#
#     redis_client.set(_LIVE_HASH_KEY, new_hash, ex=30)
#     redis_client.set(
#         _LIVE_DATA_KEY,
#         __import__("json").dumps(matches),
#         ex=60,
#     )
#
#     emit_event("betika:live:update", {
#         "source":  "betika",
#         "feed":    "live",
#         "count":   len(matches),
#         "matches": matches,
#     })


# @celery_app.task(
#     name="betika.poll_upcoming",
#     bind=True,
#     max_retries=3,
#     soft_time_limit=60,
#     time_limit=90,
# )
# def poll_betika_upcoming(self, sport_id: int = 14):
#     """
#     Poll upcoming matches for one sport. Schedule per sport_id via beat.
#     """
#     try:
#         matches = fetch_upcoming_matches(sport_id=sport_id)
#         if not matches:
#             return
#
#         new_hash = _payload_hash(matches)
#         cache_key = _UPCOMING_HASH_KEY.format(sport_id=sport_id)
#         old_hash  = redis_client.get(cache_key)
#
#         if new_hash == old_hash:
#             return
#
#         redis_client.set(cache_key, new_hash, ex=60)
#         emit_event(f"betika:upcoming:{sport_id}:update", {
#             "source":   "betika",
#             "feed":     "upcoming",
#             "sport_id": sport_id,
#             "count":    len(matches),
#             "matches":  matches,
#         })
#     except Exception as exc:
#         logger.exception("poll_betika_upcoming failed for sport %d: %s", sport_id, exc)
#         raise self.retry(exc=exc, countdown=5)


# @celery_app.task(name="betika.enrich_markets", bind=True, max_retries=2, soft_time_limit=15)
# def enrich_match_markets(self, parent_match_id: str, match_id: str):
#     """
#     Fetch full market list for a specific match and emit via socket.
#     Triggered on-demand when a user opens a match page.
#     """
#     try:
#         markets = get_match_markets(parent_match_id)
#         emit_event(f"betika:markets:{match_id}", {
#             "source":          "betika",
#             "match_id":        match_id,
#             "parent_match_id": parent_match_id,
#             "markets":         markets,
#         })
#     except Exception as exc:
#         logger.exception("enrich_match_markets failed for %s: %s", parent_match_id, exc)
#         raise self.retry(exc=exc, countdown=3)


# =============================================================================
# STANDALONE RUNNER (for local testing without Celery)
# =============================================================================

def run_live_loop(interval: float = 1.0) -> None:
    """
    Blocking live-poll loop — for dev/testing without Celery.
    Press Ctrl+C to stop.

    Usage:
        python -m app.workers.betika_harvester
    """
    import json

    print("── Betika live harvester starting (press Ctrl+C to stop) ──")
    last_hash = ""

    while True:
        tick = time.time()
        try:
            matches   = fetch_live_matches()
            new_hash  = _payload_hash(matches)

            if new_hash != last_hash:
                last_hash = new_hash
                # Pretty-print first 3 matches as a sanity check
                sample = matches[:3]
                print(f"\n[{time.strftime('%H:%M:%S')}] {len(matches)} live matches  (hash={new_hash[:8]})")
                print(json.dumps(sample, indent=2, ensure_ascii=False))

        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as exc:  # noqa: BLE001
            logger.error("Live loop error: %s", exc)

        elapsed = time.time() - tick
        sleep_s = max(0.0, interval - elapsed)
        time.sleep(sleep_s)


def run_upcoming_snapshot(sport_id: int = 14) -> None:
    """Fetch + print one page of upcoming matches for quick debugging."""
    import json
    matches = fetch_upcoming_matches(sport_id=sport_id, max_pages=1)
    print(f"Upcoming matches for sport_id={sport_id}: {len(matches)}")
    print(json.dumps(matches[:5], indent=2, ensure_ascii=False))


def run_markets_snapshot(parent_match_id: str = "62322462") -> None:
    """Fetch + print all markets for a given parent_match_id."""
    import json
    markets = get_match_markets(parent_match_id)
    print(f"Markets for parent_match_id={parent_match_id}: {len(markets)}")
    print(json.dumps(markets, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    cmd = sys.argv[1] if len(sys.argv) > 1 else "live"

    if cmd == "upcoming":
        sport = int(sys.argv[2]) if len(sys.argv) > 2 else 14
        run_upcoming_snapshot(sport_id=sport)
    elif cmd == "markets":
        pid = sys.argv[2] if len(sys.argv) > 2 else "62322462"
        run_markets_snapshot(parent_match_id=pid)
    else:
        run_live_loop()