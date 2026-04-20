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

API Endpoints:
  LineFeed (upcoming):
    GET https://{domain}/LineFeed/GetGameZip?sportId={sid}&partnerID={pid}&gr={gr}
                         &tf=1200&tz=0&lng=en&GroupEvents=true&countryId=0
                         &partner=0&getEmpty=true&hot=false&grMode=2

  LiveFeed (live matches):
    GET https://{domain}/LiveFeed/GetFeedProper?LangId=1&sportId={sid}
                         &partnerId={pid}&gr={gr}&typeId=0&levelId=1

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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import httpx

import logging
logger = logging.getLogger(__name__)

# ─── Bookmaker registry ───────────────────────────────────────────────────────

# Sports known to work across all BetB2B bookmakers
_B2B_COMMON_SPORTS = {1, 2, 3, 5, 8, 17, 21}   # soccer, ice-hockey, basketball, tennis, volleyball, rugby, cricket

# Per-BK additional sports (discovered empirically; niche sports vary by BK)
# None = use common sports only; explicit set = union with common
_BK_EXTRA_SPORTS: dict[str, set] = {
    "1xbet":     {4, 9, 11, 13, 14, 16, 47},   # widest offering
    "22bet":     {4, 9, 11, 13, 14, 16, 47},
    "betwinner": {4, 9, 11, 13, 14, 16, 47},
    "melbet":    {4, 9, 11, 13, 14, 16, 47},
    "megapari":  {4, 9, 11, 13, 14, 47},
    "helabet":   {9, 11},                        # LineFeed, limited sports
    "paripesa":  {4, 9, 11, 13, 14},
}


def _bk_supports_sport(bk_slug: str, sport_id: int) -> bool:
    """Return True if this BK is likely to have data for this sport_id."""
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

# ─── Sport ID mapping (BetB2B internal sport IDs) ─────────────────────────────

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
    "baseball":          12,
    "table-tennis":      13,
    "darts":             14,
    "american-football": 16,
    "rugby":             17,
    "cricket":           21,
    "esoccer":           47,
    "esports":           47,
}

# ─── Market group → canonical slug mapping ────────────────────────────────────
# BetB2B uses numeric Group IDs (G field) for market types

_B2B_GROUP_TO_SLUG: dict[int, str] = {
    1:   "match_winner",       # 1X2
    2:   "double_chance",
    3:   "draw_no_bet",
    5:   "btts",
    6:   "match_winner",       # 1X2 variant
    17:  "over_under_2_5",
    19:  "over_under_0_5",
    20:  "over_under_1_5",
    21:  "over_under_2_5",
    22:  "over_under_3_5",
    23:  "over_under_4_5",
    24:  "over_under_5_5",
    30:  "first_half_match_winner",
    45:  "first_half_over_under_0_5",
    46:  "first_half_over_under_1_5",
    47:  "first_half_over_under_2_5",
    56:  "asian_handicap",
    57:  "european_handicap",
    83:  "correct_score",
    102: "ht_ft",
    161: "odd_even",
    162: "first_half_odd_even",
}

# Outcome name normalisation
_B2B_OUTCOME_MAP: dict[str, str] = {
    "1":       "1",
    "x":       "X",
    "2":       "2",
    "w1":      "1",
    "w2":      "2",
    "home":    "1",
    "draw":    "X",
    "away":    "2",
    "yes":     "Yes",
    "no":      "No",
    "over":    "Over",
    "under":   "Under",
    "odd":     "Odd",
    "even":    "Even",
    "1x":      "1X",
    "12":      "12",
    "x2":      "X2",
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_dotnet_date(val: Any) -> str | None:
    """Parse /Date(ms)/ or ISO string → ISO string."""
    if val is None:
        return None
    if isinstance(val, str):
        m = re.match(r"/Date\((\d+)\)/", val)
        if m:
            ts = int(m.group(1)) / 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return val  # already ISO
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(val / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return None


def _norm_outcome(name: str) -> str:
    """Normalise outcome label."""
    k = name.strip().lower()
    return _B2B_OUTCOME_MAP.get(k, name.strip())


def _slug_for_group(group_id: int, group_name: str = "") -> str:
    """Map BetB2B group ID to canonical market slug."""
    if group_id in _B2B_GROUP_TO_SLUG:
        return _B2B_GROUP_TO_SLUG[group_id]

    # Derive from group name
    gn = group_name.lower().strip()
    if "1x2" in gn or "match result" in gn:
        return "match_winner"
    if "both teams" in gn or "btts" in gn or "gg" in gn:
        return "btts"
    if "double" in gn:
        return "double_chance"
    if "draw no bet" in gn:
        return "draw_no_bet"
    if "over" in gn and "under" in gn:
        line_m = re.search(r'([\d.]+)', gn)
        line = line_m.group(1).replace(".", "_") if line_m else "2_5"
        half = "first_half_" if "half" in gn or "1st" in gn else ""
        return f"{half}over_under_{line}"
    if "handicap" in gn:
        kind = "asian" if "asian" in gn else "european"
        return f"{kind}_handicap"
    if "correct score" in gn:
        return "correct_score"
    if "half" in gn and "time" in gn:
        return "first_half_match_winner"
    if "odd" in gn or "even" in gn:
        return "odd_even"
    if "winner" in gn:
        return "match_winner"

    # Fallback: slugify
    slug = re.sub(r'[^a-z0-9]', '_', gn)
    slug = re.sub(r'_+', '_', slug).strip('_')
    return slug or f"market_{group_id}"


# ─── HTTP client (shared, connection-pooled) ──────────────────────────────────

_CLIENT = httpx.Client(
    timeout=httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=2.0),
    limits=httpx.Limits(max_connections=40, max_keepalive_connections=20),
    follow_redirects=True,
    http2=True,
)

_HEADERS = {
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control":   "no-cache",
    "Connection":      "keep-alive",
    "User-Agent":      (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


# ─── Low-level API calls ──────────────────────────────────────────────────────

def _build_linefeed_url(bk: dict, sport_id: int) -> str:
    domain = bk["domain"]
    pid    = bk["partner_id"]
    gr     = bk.get("gr")

    params = (
        f"sportId={sport_id}"
        f"&partnerID={pid}"
        f"&tf=1200"
        f"&tz=0"
        f"&lng=en"
        f"&GroupEvents=true"
        f"&countryId=0"
        f"&partner=0"
        f"&getEmpty=true"
        f"&hot=false"
        f"&grMode=2"
    )
    if gr:
        params += f"&gr={gr}"

    return f"https://{domain}/LineFeed/GetGameZip?{params}"


def _build_livefeed_url(bk: dict, sport_id: int) -> str:
    domain = bk["domain"]
    pid    = bk["partner_id"]
    gr     = bk.get("gr", 0) or 0

    return (
        f"https://{domain}/LiveFeed/GetFeedProper"
        f"?LangId=1&sportId={sport_id}&partnerId={pid}"
        f"&gr={gr}&typeId=0&levelId=1"
    )


# In-process cache: (bk_slug, sport_id, mode) combos known to return empty.
# Populated at runtime; cleared on worker restart so BKs can add sports.
_UNSUPPORTED_SPORT_CACHE: set = set()


def _fetch_b2b_raw(bk: dict, sport_id: int, mode: str = "upcoming") -> list[dict]:
    """
    Fetch raw game list from BetB2B API.
    Returns the Value[] array, or [] when sport is not offered.

    Empty/null body = sport not available on this BK.
    These combos are cached in-process so we stop making pointless requests.
    """
    slug      = bk["slug"]
    cache_key = (slug, sport_id, mode)

    # Skip sports this BK is known not to support (before making any HTTP request)
    if not _bk_supports_sport(slug, sport_id):
        return []

    # Skip known-empty combos discovered at runtime
    if cache_key in _UNSUPPORTED_SPORT_CACHE:
        return []

    url = (
        _build_livefeed_url(bk, sport_id)
        if mode == "live"
        else _build_linefeed_url(bk, sport_id)
    )
    try:
        resp = _CLIENT.get(url, headers={
            **_HEADERS,
            "Referer": f"https://{bk['domain']}/",
            "Origin":  f"https://{bk['domain']}",
        })

        # Empty body = sport not offered — cache and skip silently
        body = resp.content
        if not body or body.strip() in (b"", b"null", b"[]", b"{}"):
            logger.debug("[b2b:%s] sport %d not offered (empty body)", slug, sport_id)
            _UNSUPPORTED_SPORT_CACHE.add(cache_key)
            return []

        resp.raise_for_status()

        try:
            data = resp.json()
        except ValueError:
            # Non-JSON body (HTML error page, plain text) — cache as unsupported
            logger.debug("[b2b:%s] sport %d non-JSON response (%d bytes) — skipping",
                         slug, sport_id, len(body))
            _UNSUPPORTED_SPORT_CACHE.add(cache_key)
            return []

        # BetB2B wraps response in {"Value": [...], "Err": 0}
        if isinstance(data, dict):
            err = data.get("Err") or data.get("err") or 0
            if err and err != 0:
                logger.debug("[b2b:%s] sport %d API err=%s — skipping", slug, sport_id, err)
                _UNSUPPORTED_SPORT_CACHE.add(cache_key)
                return []
            result = data.get("Value") or data.get("value") or []
            if not result:
                _UNSUPPORTED_SPORT_CACHE.add(cache_key)   # empty Value → nothing to offer
            return result

        if isinstance(data, list):
            return data
        return []

    except httpx.HTTPStatusError as e:
        code = e.response.status_code
        if code in (400, 404, 422, 204):
            logger.debug("[b2b:%s] sport %d HTTP %d — caching as unsupported", slug, sport_id, code)
            _UNSUPPORTED_SPORT_CACHE.add(cache_key)
        else:
            logger.warning("[b2b:%s] HTTP %d for sport %d", slug, code, sport_id)
        return []

    except httpx.TimeoutException:
        logger.warning("[b2b:%s] timeout for sport %d", slug, sport_id)
        return []

    except Exception as e:
        err_str = str(e)
        # "Expecting value" = JSON parse on empty body; demote to debug
        if any(x in err_str for x in ("Expecting value", "No data", "JSONDecodeError")):
            logger.debug("[b2b:%s] sport %d empty/invalid response — caching skip", slug, sport_id)
            _UNSUPPORTED_SPORT_CACHE.add(cache_key)
        else:
            logger.warning("[b2b:%s] fetch error sport %d: %s", slug, sport_id, e)
        return []


# ─── Market parsing ───────────────────────────────────────────────────────────

def _parse_events(events: list[dict]) -> dict[str, dict[str, float]]:
    """
    Parse BetB2B 'Events' (market odds) into canonical {slug: {outcome: price}}.

    Event object:
      { "Id": 11, "G": 1, "GN": "1X2", "E": [
          {"Id": 1, "Name": "1", "Coef": 2.10},
          {"Id": 2, "Name": "X", "Coef": 3.20},
          {"Id": 3, "Name": "2", "Coef": 3.50}
      ]}
    """
    markets: dict[str, dict[str, float]] = {}

    for ev in (events or []):
        if not isinstance(ev, dict):
            continue

        group_id   = ev.get("G") or ev.get("GroupId") or 0
        group_name = ev.get("GN") or ev.get("GroupName") or ""
        slug       = _slug_for_group(int(group_id), group_name)

        selections = ev.get("E") or ev.get("Selections") or []
        if not selections:
            continue

        if slug not in markets:
            markets[slug] = {}

        for sel in selections:
            if not isinstance(sel, dict):
                continue
            name  = str(sel.get("Name") or sel.get("N") or "")
            coef  = sel.get("Coef") or sel.get("C") or 0
            try:
                price = float(coef)
            except (TypeError, ValueError):
                continue
            if price <= 1.0:
                continue
            outcome = _norm_outcome(name)
            # Keep highest price per outcome (some events duplicate)
            if outcome not in markets[slug] or price > markets[slug][outcome]:
                markets[slug][outcome] = price

    return {k: v for k, v in markets.items() if v}


def _parse_live_events(events: list[dict]) -> dict[str, dict[str, float]]:
    """
    LiveFeed events have same structure as LineFeed events.
    Additional live-specific fields (score, timer) handled separately.
    """
    return _parse_events(events)


# ─── Game normalisation ───────────────────────────────────────────────────────

def _parse_game(game: dict, bk: dict, sport_slug: str, mode: str = "upcoming") -> dict | None:
    """
    Normalise one BetB2B game object into our canonical match dict.

    LineFeed game keys:
      Id, Oc (home), Odc (away), Sid (sport), CI (competition),
      Cid (country_id), CN (country_name), StartDate, Events
    """
    home  = game.get("Oc") or game.get("Team1") or ""
    away  = game.get("Odc") or game.get("Team2") or ""
    if not home or not away:
        return None

    game_id  = game.get("Id") or game.get("GameId")
    comp     = game.get("CI") or game.get("CompetitionName") or ""
    country  = game.get("CN") or game.get("CountryName") or ""
    start_dt = _parse_dotnet_date(game.get("StartDate") or game.get("SD"))

    events_raw = game.get("Events") or game.get("E") or []
    markets    = _parse_events(events_raw)

    if not markets:
        return None

    # Live-specific fields
    score_home = score_away = None
    match_time = None
    if mode == "live":
        score_raw = game.get("Sc") or game.get("Score") or ""
        if isinstance(score_raw, str) and ":" in score_raw:
            parts = score_raw.split(":")
            try:
                score_home = int(parts[0])
                score_away = int(parts[1])
            except (ValueError, IndexError):
                pass
        match_time = str(game.get("Tm") or game.get("Time") or "")

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
        "match_time":   match_time,
        "is_live":      mode == "live",
        "harvested_at": _now_iso(),
    }


# ─── Single-bookmaker fetch ───────────────────────────────────────────────────

def fetch_single_bk(
    bk: dict,
    sport_slug: str,
    mode: str = "upcoming",
    page: int = 1,
    page_size: int = 100,
) -> list[dict]:
    """
    Fetch one bookmaker for one sport.
    Returns list of normalised match dicts.
    """
    sport_id = _B2B_SPORT_IDS.get(sport_slug.lower(), 1)
    t0 = time.perf_counter()

    raw_games = _fetch_b2b_raw(bk, sport_id, mode)

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
    return matches


# ─── All-bookmakers parallel fetch ───────────────────────────────────────────

def fetch_all_b2b_sport(
    sport_slug: str,
    mode: str = "upcoming",
    bookmakers: list[dict] | None = None,
    max_workers: int = 7,
) -> dict[str, list[dict]]:
    """
    Fetch all B2B bookmakers in parallel for one sport.

    Returns: { bk_slug: [match, ...] }
    """
    bks = bookmakers or B2B_BOOKMAKERS
    results: dict[str, list[dict]] = {}

    with ThreadPoolExecutor(max_workers=min(max_workers, len(bks)), thread_name_prefix="b2b") as pool:
        futures = {
            pool.submit(fetch_single_bk, bk, sport_slug, mode): bk
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


# ─── Cross-BK merge (by match identity) ─────────────────────────────────────

def merge_b2b_by_match(
    per_bk_results: dict[str, list[dict]],
    sport_slug: str,
) -> list[dict]:
    """
    Merge results across all B2B bookmakers into unified match objects.
    Uses exact home/away + time matching (B2B games share same structure).

    Returns list of merged matches with all bookmaker data.
    """
    from app.workers.fuzzy_matcher import (
        MatchCandidate, FuzzyMatcher, match_dict_to_candidate,
        bulk_align, _parse_dt,
    )

    # Build a flat list of all matches with their BK
    all_matches: list[dict] = []
    for bk_slug, matches in per_bk_results.items():
        for m in matches:
            m["_bk_slug"] = bk_slug
            all_matches.append(m)

    if not all_matches:
        return []

    # Build unified list by merging same games across BKs
    unified: list[dict] = []
    seen_key: dict[str, int] = {}  # "home|away|start" → index in unified

    for m in all_matches:
        bk_slug = m["_bk_slug"]
        home    = m.get("home_team", "").lower().strip()
        away    = m.get("away_team", "").lower().strip()
        start   = (m.get("start_time") or "")[:16]   # truncate to minute

        key = f"{home}|||{away}|||{start}"

        if key in seen_key:
            # Merge into existing
            idx = seen_key[key]
            existing = unified[idx]
            bk_data  = m["bookmakers"].get(bk_slug, {})
            if bk_data:
                existing["bookmakers"][bk_slug] = bk_data
                # Merge markets: keep best odds per outcome
                for slug, outs in (bk_data.get("markets") or {}).items():
                    if slug not in existing["markets"]:
                        existing["markets"][slug] = {}
                    for outcome, price in outs.items():
                        existing_price = existing["markets"][slug].get(outcome, 0)
                        if price > existing_price:
                            existing["markets"][slug][outcome] = price
                existing["market_count"] = len(existing["markets"])
        else:
            # New match
            merged = dict(m)
            merged.pop("_bk_slug", None)
            merged["bookmakers"] = dict(m.get("bookmakers") or {})
            merged["markets"]    = dict(m.get("markets") or {})
            seen_key[key] = len(unified)
            unified.append(merged)

    logger.info("[b2b:merge] %s → %d unified from %d raw", sport_slug, len(unified), len(all_matches))
    return unified


# ─── Full B2B harvest for a sport ─────────────────────────────────────────────

def harvest_b2b_sport(
    sport_slug: str,
    mode: str = "upcoming",
    bookmakers: list[dict] | None = None,
) -> list[dict]:
    """
    Full harvest pipeline for one sport:
      1. Fetch all B2B bookmakers in parallel
      2. Merge by match identity
      3. Return unified list

    This is the main entry point called by Celery tasks.
    """
    per_bk = fetch_all_b2b_sport(sport_slug, mode, bookmakers)
    merged = merge_b2b_by_match(per_bk, sport_slug)
    return merged


# ─── Paged harvest (called by tasks_harvest_b2b.py) ──────────────────────────

def harvest_b2b_page(
    bk_slug: str,
    sport_slug: str,
    page: int,
    page_size: int = 100,
    mode: str = "upcoming",
) -> list[dict]:
    """
    Fetch one page from one B2B bookmaker.
    Called in parallel by Celery tasks.
    """
    bk = next((b for b in B2B_BOOKMAKERS if b["slug"] == bk_slug), None)
    if not bk:
        logger.warning("[b2b] unknown bk_slug: %s", bk_slug)
        return []
    return fetch_single_bk(bk, sport_slug, mode, page, page_size)


# ─── Live harvest ─────────────────────────────────────────────────────────────

class B2BLivePoller:
    """
    Background thread that polls all B2B LiveFeeds every `interval` seconds
    and publishes updates to Redis pub/sub.
    """

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
            sleep_s = max(0, self._interval - elapsed)
            time.sleep(sleep_s)


# ─── Bookmaker registry helpers ───────────────────────────────────────────────

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