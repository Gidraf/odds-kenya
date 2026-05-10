"""
app/workers/b2b_harvester.py
=============================
BetB2B family harvester — covers all 7 Kenya bookmakers:

  BK          | domain              | partnerId | gr  | feed
  ────────────┼─────────────────────┼───────────┼─────┼──────────
  1xBet       | 1xbet.co.ke         | 61        | 656 | LiveFeed
  22Bet       | 22bet.co.ke         | 2         | 656 | LiveFeed
  Betwinner   | betwinner.co.ke     | 3         | 656 | LiveFeed
  Melbet      | melbet.co.ke        | 4         | 656 | LiveFeed
  Megapari    | megapari.com        | 6         | 656 | LiveFeed
  Helabet     | helabetke.com       | 237       |  —  | LineFeed
  Paripesa    | paripesa.cool       | 188       | 764 | LiveFeed

API Endpoints (LiveFeed example):
  GET https://{domain}/service-api/LiveFeed/Get1x2_VZip?
      count=50&lng=en&gr={gr}&mode=4&country=87&partner={pid}
      &virtualSports=true&noFilterBlockEvent=true&sportId={sid}

  LineFeed (Helabet upcoming):
    GET https://{domain}/LineFeed/GetGameZip?sportId={sid}&partnerID={pid}&gr={gr}
                         &tf=1200&tz=0&lng=en&GroupEvents=true&countryId=0
                         &partner=0&getEmpty=true&hot=false&grMode=2

Normalised output shape (same as sp/od/bt harvesters):
{
    "betradar_id":    "",
    "b2b_match_id":  str,          # "{partner_id}:{game_id}"
    "home_team":      str,
    "away_team":      str,
    "start_time":     str | None,
    "competition":    str,
    "country":        str,
    "sport":          str,
    "source":         str,         # "1xbet" | "22bet" | etc.
    "partner_id":     int,
    "bookmakers":     { bk_slug: { "match_id": str, "markets": {...} } },
    "markets":        { mkt_slug: { outcome: price } },
    "market_count":   int,
    "status":         "upcoming" | "live",
    "score_home":     int | None,
    "score_away":     int | None,
    "harvested_at":   str (ISO),
}
"""
from __future__ import annotations

import json
import re
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import logging

logger = logging.getLogger(__name__)

# ─── Bookmaker registry ──────────────────────────────────────────────────────

# Sports known to work across all BetB2B bookmakers
_B2B_COMMON_SPORTS = {1, 2, 3, 5, 8, 17, 21}

_BK_EXTRA_SPORTS: dict[str, set] = {
    "1xbet":     {4, 9, 11, 13, 14, 16, 47},
    "22bet":     {4, 9, 11, 13, 14, 16, 47},
    "betwinner": {4, 9, 11, 13, 14, 16, 47},
    "melbet":    {4, 9, 11, 13, 14, 16, 47},
    "megapari":  {4, 9, 11, 13, 14, 47},
    "helabet":   {9, 11},                        # LineFeed, limited sports
    "paripesa":  {4, 9, 11, 13, 14},
}

def _bk_supports_sport(bk_slug: str, sport_id: int) -> bool:
    if sport_id in _B2B_COMMON_SPORTS:
        return True
    extras = _BK_EXTRA_SPORTS.get(bk_slug, set())
    return sport_id in extras

B2B_BOOKMAKERS: list[dict] = [
    {
        "slug":       "1xbet",
        "name":       "1xBet",
        "domain":     "1xbet.co.ke",
        "partner_id": 61,
        "gr":         656,
        "feed":       "LiveFeed",
        "color":      "#1F8AEB",
    },
    {
        "slug":       "22bet",
        "name":       "22Bet",
        "domain":     "22bet.co.ke",
        "partner_id": 2,
        "gr":         656,
        "feed":       "LiveFeed",
        "color":      "#0B2133",
    },
    {
        "slug":       "betwinner",
        "name":       "Betwinner",
        "domain":     "betwinner.co.ke",
        "partner_id": 3,
        "gr":         656,
        "feed":       "LiveFeed",
        "color":      "#FF6600",
    },
    {
        "slug":       "melbet",
        "name":       "Melbet",
        "domain":     "melbet.co.ke",
        "partner_id": 4,
        "gr":         656,
        "feed":       "LiveFeed",
        "color":      "#FF0000",
    },
    {
        "slug":       "megapari",
        "name":       "Megapari",
        "domain":     "megapari.com",
        "partner_id": 6,
        "gr":         656,
        "feed":       "LiveFeed",
        "color":      "#7B2FBE",
    },
    {
        "slug":       "helabet",
        "name":       "Helabet",
        "domain":     "helabetke.com",
        "partner_id": 237,
        "gr":         None,
        "feed":       "LineFeed",
        "color":      "#9C27B0",
    },
    {
        "slug":       "paripesa",
        "name":       "Paripesa",
        "domain":     "paripesa.cool",
        "partner_id": 188,
        "gr":         764,
        "feed":       "LiveFeed",
        "color":      "#FF6B35",
    },
]

# ─── Sport ID mapping ────────────────────────────────────────────────────────

_B2B_SPORT_IDS: dict[str, int] = {
    "soccer":            1,
    "football":          1,
    "ice-hockey":        2,
    "basketball":        3,
    "baseball":          4,
    "tennis":            5,
    "volleyball":        8,
    "mma":               9,
    "boxing":            9,
    "handball":          11,
    "table-tennis":      13,
    "darts":             14,
    "american-football": 16,
    "rugby":             17,
    "cricket":           21,
    "esoccer":           47,
    "esports":           47,
}

# ─── Market group → canonical slug mapping (updated for real API) ────────────

_B2B_GROUP_TO_SLUG: dict[int, str] = {
    1:   "match_winner",         # 1X2
    2:   "asian_handicap",       # handicap (Asian) with line P
    8:   "double_chance",        # 1X, 12, X2
    15:  "btts",                 # Both Teams to Score
    17:  "over_under",           # total goals
    19:  "first_half_over_under", # 1st half O/U
    62:  "handicap_result",      # handicap result (European)
}

# Outcome T mapping for known groups
_OUTCOME_T_MAP = {
    # 1X2
    1:  {1: "1", 2: "X", 3: "2"},
    # Double chance (8)
    8:  {4: "1X", 5: "12", 6: "X2"},
    # Over/Under (17, 19) → T=9=Over, T=10=Under
    17: {9: "Over", 10: "Under"},
    19: {9: "Over", 10: "Under"},
    # BTTS (15) → T=11=Yes, T=12=No
    15: {11: "Yes", 12: "No"},
    # Handicap (2) → T=7=Home, T=8=Away
    2:  {7: "1", 8: "2"},
    # Handicap result (62) → T=13=Home, T=14=Away
    62: {13: "1", 14: "2"},
}

# ─── Helpers ─────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _ts_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _curl_get(url: str, referer: str, debug: bool = False) -> dict | None:
    """Use system curl to bypass Cloudflare TLS fingerprint blocks."""
    if debug:
        print(f"\n🔍 [CURL] {url}")
        print(f"   Referer: {referer}")
        print(f"   curl -s -m 15 -H 'accept: application/json' -H 'referer: {referer}' '{url}'")
    try:
        res = subprocess.run(
            [
                "curl", "-s", "-m", "15",
                "-H", "accept: application/json, text/plain, */*",
                "-H", f"referer: {referer}",
                url
            ],
            capture_output=True, text=True, check=False
        )
        if res.returncode == 0 and res.stdout:
            return json.loads(res.stdout)
    except Exception as e:
        if debug:
            print(f"❌ curl error: {e}")
    return None

# ─── Low-level API calls ─────────────────────────────────────────────────────

def _build_livefeed_url(bk: dict, sport_id: int) -> str:
    domain = bk["domain"]
    pid    = bk["partner_id"]
    gr     = bk.get("gr")
    base = f"https://{domain}/service-api/LiveFeed/Get1x2_VZip"
    params = (
        f"count=50&lng=en&mode=4&country=87&partner={pid}"
        f"&virtualSports=true&noFilterBlockEvent=true"
        f"&sportId={sport_id}"
    )
    if gr is not None:
        params += f"&gr={gr}"
    return f"{base}?{params}"

def _build_linefeed_url(bk: dict, sport_id: int) -> str:
    domain = bk["domain"]
    pid    = bk["partner_id"]
    gr     = bk.get("gr") or ""
    return (
        f"https://{domain}/LineFeed/GetGameZip?"
        f"sportId={sport_id}&partnerID={pid}&gr={gr}"
        f"&tf=1200&tz=0&lng=en&GroupEvents=true&countryId=0"
        f"&partner=0&getEmpty=true&hot=false&grMode=2"
    )

# Cache for unsupported combos
_UNSUPPORTED_SPORT_CACHE: set = set()

def _fetch_b2b_raw(bk: dict, sport_id: int, mode: str = "upcoming", debug: bool = False) -> list[dict]:
    """
    Fetch raw game list from BetB2B API.
    Uses LiveFeed/Get1x2_VZip for live/upcoming, LineFeed for Helabet.
    """
    slug      = bk["slug"]
    cache_key = (slug, sport_id, mode)

    if not _bk_supports_sport(slug, sport_id):
        return []
    if cache_key in _UNSUPPORTED_SPORT_CACHE:
        return []

    domain = bk["domain"]
    referer = f"https://{domain}/"

    # Determine endpoint
    if bk["feed"] == "LineFeed":
        url = _build_linefeed_url(bk, sport_id)
    else:
        url = _build_livefeed_url(bk, sport_id)

    if debug:
        print(f"\n📡 Fetching {slug} / sport {sport_id} ({mode})")

    data = _curl_get(url, referer, debug=debug)
    if not data:
        if debug:
            print("   → No response / curl failed")
        _UNSUPPORTED_SPORT_CACHE.add(cache_key)
        return []

    # Check API error
    err = data.get("ErrorCode") or data.get("Error") or data.get("err") or 0
    if err and err != 0:
        if debug:
            print(f"   → API error: {err}")
        _UNSUPPORTED_SPORT_CACHE.add(cache_key)
        return []

    games = data.get("Value") or data.get("value") or []
    if not games:
        _UNSUPPORTED_SPORT_CACHE.add(cache_key)
    if debug:
        print(f"   → Received {len(games)} games")
        print("   Raw JSON (first 2000 chars):")
        print(json.dumps(games[:2], indent=2, ensure_ascii=False)[:2000])
    return games

# ─── Market parsing (real API format) ───────────────────────────────────────

def _parse_market_group(gid: int, outcomes: dict) -> tuple[str, dict[str, float]]:
    """Convert group ID and raw outcomes to canonical slug + normalised prices."""
    slug = _B2B_GROUP_TO_SLUG.get(gid)
    if not slug:
        return f"unknown_{gid}", dict(outcomes)

    # If the slug is generic like "over_under", we'll keep it as is
    # but we can append line info? Kept simple for now.
    return slug, dict(outcomes)

def _parse_events(raw_events: list[dict], additional_events: list[dict]) -> dict[str, dict[str, float]]:
    """
    Parse main E and additional AE arrays into canonical market dict.
    E format: [ {"G":1, "C":1.968, "T":3}, ... ]
    AE format: [ {"G":2, "ME":[...]}, ... ]
    """
    markets: dict[str, dict[str, float]] = {}

    def _process_event(ev: dict):
        gid = ev.get("G")
        if not gid:
            return
        coeff = ev.get("C") or ev.get("CV")
        try:
            price = float(coeff)
        except (TypeError, ValueError):
            return
        if price <= 1.0:
            return
        t = ev.get("T")
        if t is None:
            return

        # Map outcome name
        outcome = None
        mapping = _OUTCOME_T_MAP.get(gid, {})
        outcome = mapping.get(t)
        if outcome is None:
            outcome = str(t)

        # For handicap/over_under, add line info
        p = ev.get("P")
        if p is not None and gid in (2, 17, 19, 62):
            outcome = f"{outcome}@{p}"

        # Determine slug
        slug, _ = _parse_market_group(gid, {outcome: price})
        if slug not in markets:
            markets[slug] = {}
        if outcome not in markets[slug] or price > markets[slug][outcome]:
            markets[slug][outcome] = price

    # Main events
    for ev in raw_events:
        if isinstance(ev, dict):
            _process_event(ev)

    # Additional events (AE) contain sub-events in ME
    for ae in additional_events:
        if not isinstance(ae, dict):
            continue
        gid = ae.get("G")
        me_list = ae.get("ME", [])
        for me in me_list:
            if not isinstance(me, dict):
                continue
            # ME objects have G (inherit from parent?), C, P, T
            # Use parent G if present
            me_gid = me.get("G") or gid
            me["G"] = me_gid
            _process_event(me)

    # Remove empty markets
    return {k: v for k, v in markets.items() if v}


# ─── Game normalisation (real API) ──────────────────────────────────────────

def _parse_game(game: dict, bk: dict, sport_slug: str, mode: str = "upcoming") -> dict | None:
    """Normalise one game dict from LiveFeed/LineFeed into canonical match dict."""
    home = game.get("O1E") or game.get("O1") or ""
    away = game.get("O2E") or game.get("O2") or ""
    if not home or not away:
        return None

    game_id  = game.get("I") or game.get("GameId")
    comp     = game.get("LE") or game.get("L") or ""
    country  = game.get("CN") or ""
    start_ts = game.get("S")  # Unix timestamp (seconds)
    start_dt = _ts_to_iso(start_ts) if start_ts else None

    # Parse odds from E and AE
    raw_events   = game.get("E") or []
    raw_add_events = game.get("AE") or []
    markets = _parse_events(raw_events, raw_add_events)

    if not markets:
        return None

    # Score from SC object
    score_home = score_away = None
    sc = game.get("SC")
    if isinstance(sc, dict):
        fs = sc.get("FS") or {}
        # Sometimes integer scores
        s1 = fs.get("S1")
        s2 = fs.get("S2")
        try:
            score_home = int(s1) if s1 is not None else None
            score_away = int(s2) if s2 is not None else None
        except (ValueError, TypeError):
            pass

    match_id = f"{bk['partner_id']}:{game_id}"

    return {
        "betradar_id":   "",
        "b2b_match_id":  match_id,
        "external_id":   str(game_id or ""),
        "partner_id":    bk["partner_id"],
        "home_team":     home.strip(),
        "away_team":     away.strip(),
        "start_time":    start_dt,
        "competition":   comp.strip(),
        "country":       country.strip(),
        "sport":         sport_slug,
        "source":        bk["slug"],
        "bookmakers": {
            bk["slug"]: {
                "match_id": match_id,
                "markets":  markets,
            }
        },
        "markets":      markets,
        "market_count": len(markets),
        "status":       mode if mode == "live" else "upcoming",
        "score_home":   score_home,
        "score_away":   score_away,
        "match_time":   None,   # can be derived from SC if needed
        "is_live":      mode == "live",
        "harvested_at": _now_iso(),
    }

# ─── Single-bookmaker fetch ─────────────────────────────────────────────────

def fetch_single_bk(
    bk: dict,
    sport_slug: str,
    mode: str = "upcoming",
    page: int = 1,
    page_size: int = 100,
    debug: bool = False,
) -> list[dict]:
    """
    Fetch one bookmaker for one sport.
    Returns list of normalised match dicts.
    """
    sport_id = _B2B_SPORT_IDS.get(sport_slug.lower(), 1)
    t0 = time.perf_counter()

    raw_games = _fetch_b2b_raw(bk, sport_id, mode, debug=debug)

    # Apply pagination (BetB2B returns everything; we slice)
    start = (page - 1) * page_size
    end   = start + page_size
    page_games = raw_games[start:end]

    matches = []
    for game in page_games:
        try:
            m = _parse_game(game, bk, sport_slug, mode)
            if m:
                matches.append(m)
        except Exception as e:
            logger.debug("[b2b:%s] parse error: %s", bk["slug"], e)

    ms = int((time.perf_counter() - t0) * 1000)
    logger.info(
        "[b2b:%s] %s/%s page%d → %d/%d matches (%dms)",
        bk["slug"], sport_slug, mode, page, len(matches), len(raw_games), ms,
    )
    if debug:
        print(f"\n📊 Parsed {len(matches)} valid matches for {bk['slug']}/{sport_slug}")
        print("Unified matches (first 1):")
        print(json.dumps(matches[:1], indent=2, ensure_ascii=False))
    return matches

# ─── All-bookmakers parallel fetch ─────────────────────────────────────────

def fetch_all_b2b_sport(
    sport_slug: str,
    mode: str = "upcoming",
    bookmakers: list[dict] | None = None,
    max_workers: int = 7,
    debug: bool = False,
) -> dict[str, list[dict]]:
    bks = bookmakers or B2B_BOOKMAKERS
    results: dict[str, list[dict]] = {}

    with ThreadPoolExecutor(max_workers=min(max_workers, len(bks)), thread_name_prefix="b2b") as pool:
        futures = {
            pool.submit(fetch_single_bk, bk, sport_slug, mode, 1, 200, debug): bk
            for bk in bks
        }
        for fut in as_completed(futures):
            bk = futures[fut]
            try:
                matches = fut.result()
                results[bk["slug"]] = matches
            except Exception as e:
                logger.error("[b2b:%s] %s/%s failed: %s", bk["slug"], sport_slug, mode, e)
                results[bk["slug"]] = []

    total = sum(len(v) for v in results.values())
    logger.info("[b2b:all] %s/%s → %d total across %d BKs", sport_slug, mode, total, len(bks))
    return results

# ─── Cross-BK merge (exact match on home/away/time) ────────────────────────

def merge_b2b_by_match(per_bk_results: dict[str, list[dict]], sport_slug: str) -> list[dict]:
    all_matches: list[dict] = []
    for bk_slug, matches in per_bk_results.items():
        for m in matches:
            m["_bk_slug"] = bk_slug
            all_matches.append(m)

    if not all_matches:
        return []

    unified: list[dict] = []
    seen_key: dict[str, int] = {}

    for m in all_matches:
        bk_slug = m["_bk_slug"]
        home = m.get("home_team", "").lower().strip()
        away = m.get("away_team", "").lower().strip()
        start = (m.get("start_time") or "")[:16]
        key = f"{home}|||{away}|||{start}"

        if key in seen_key:
            idx = seen_key[key]
            existing = unified[idx]
            bk_data = m["bookmakers"].get(bk_slug, {})
            if bk_data:
                existing["bookmakers"][bk_slug] = bk_data
                for slug, outs in (bk_data.get("markets") or {}).items():
                    if slug not in existing["markets"]:
                        existing["markets"][slug] = {}
                    for outcome, price in outs.items():
                        if outcome not in existing["markets"][slug] or price > existing["markets"][slug][outcome]:
                            existing["markets"][slug][outcome] = price
                existing["market_count"] = len(existing["markets"])
        else:
            merged = dict(m)
            merged.pop("_bk_slug", None)
            merged["bookmakers"] = dict(m.get("bookmakers") or {})
            merged["markets"] = dict(m.get("markets") or {})
            seen_key[key] = len(unified)
            unified.append(merged)

    logger.info("[b2b:merge] %s → %d unified from %d raw", sport_slug, len(unified), len(all_matches))
    return unified

# ─── Full B2B harvest for a sport ──────────────────────────────────────────

def harvest_b2b_sport(
    sport_slug: str,
    mode: str = "upcoming",
    bookmakers: list[dict] | None = None,
    debug: bool = False,
) -> list[dict]:
    per_bk = fetch_all_b2b_sport(sport_slug, mode, bookmakers, debug=debug)
    merged = merge_b2b_by_match(per_bk, sport_slug)
    if debug:
        print(f"\n🔗 Unified matches for {sport_slug}:")
        print(json.dumps(merged[:2], indent=2, ensure_ascii=False))
    return merged

# ─── Paged harvest (for Celery compatibility) ───────────────────────────────

def harvest_b2b_page(
    bk_slug: str,
    sport_slug: str,
    page: int,
    page_size: int = 100,
    mode: str = "upcoming",
    debug: bool = False,
) -> list[dict]:
    bk = next((b for b in B2B_BOOKMAKERS if b["slug"] == bk_slug), None)
    if not bk:
        logger.warning("[b2b] unknown bk_slug: %s", bk_slug)
        return []
    return fetch_single_bk(bk, sport_slug, mode, page, page_size, debug=debug)

# ─── Live poller (unchanged, but using corrected fetch) ─────────────────────

class B2BLivePoller:
    _POLL_SPORTS: list[str] = [
        "soccer", "basketball", "tennis", "ice-hockey",
        "volleyball", "table-tennis",
    ]

    def __init__(self, redis_client, interval: float = 8.0):
        self._r        = redis_client
        self._interval = interval
        self._running  = False
        self._thread   = None

    def start(self):
        import threading
        self._running = True
        self._thread  = threading.Thread(
            target=self._loop, daemon=True, name="b2b-live-poller"
        )
        self._thread.start()
        logger.info("[b2b:live] poller started (interval=%.1fs)", self._interval)

    def stop(self):
        self._running = False

    def _loop(self):
        from app.workers.redis_bus import publish_b2b_live_update
        while self._running:
            t0 = time.perf_counter()
            for sport_slug in self._POLL_SPORTS:
                try:
                    matches = harvest_b2b_sport(sport_slug, mode="live")
                    if matches:
                        publish_b2b_live_update(sport_slug, matches, self._r)
                except Exception as e:
                    logger.warning("[b2b:live] %s error: %s", sport_slug, e)
            elapsed = time.perf_counter() - t0
            time.sleep(max(0, self._interval - elapsed))

# ─── Bookmaker registry helpers ─────────────────────────────────────────────

def get_bk_by_slug(slug: str) -> dict | None:
    return next((b for b in B2B_BOOKMAKERS if b["slug"] == slug), None)

def get_all_slugs() -> list[str]:
    return [b["slug"] for b in B2B_BOOKMAKERS]

B2B_SUPPORTED_SPORTS: list[str] = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "boxing",
    "handball", "mma", "table-tennis", "darts",
    "american-football", "esoccer", "baseball",
]