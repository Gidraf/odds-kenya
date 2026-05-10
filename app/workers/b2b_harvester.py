"""
app/workers/b2b_harvester.py
=============================
BetB2B family harvester — covers all 7 Kenya bookmakers.

HOW THE API WORKS
─────────────────
Every BetB2B site exposes two endpoints, both returning JSON:

  Live (in-play):
    GET /service-api/LiveFeed/Get1x2_VZip
        ?count=50&lng=en_GB&gr={gr}&mode=4&country=87
         &partner={partner_id}&getEmpty=true

  Upcoming (pre-match):
    GET /service-api/LineFeed/Get1x2_VZip
        ?count=50&lng=en_GB&tz=3&mode=4&country=87
         &partner={partner_id}&getEmpty=true&gr={gr}

Response shape:
  {
    "ErrorCode": 0,
    "Value": [
      {                          ← sport wrapper  (I = sport ID)
        "I": 1, "N": "Football",
        "L": [                   ← country/region list
          {
            "L": "Italy",
            "SC": [              ← sub-competitions (leagues)
              {
                "L": "Italy. Serie A",
                "G": [           ← actual game objects
                  { "I": ..., "O1E": "...", "O2E": "...",
                    "S": <unix_ts>, "E": [...odds...], ... }
                ]
              }
            ]
          }
        ]
      }
    ]
  }

Odds live inside each game's "E" (events) list and optionally
"AE" (additional events).  Each event:
  { "G": <group_id>, "T": <outcome_type>, "C": <coefficient>,
    "P": <line/handicap> }

Confirmed sport IDs (from live API responses):
  1=Football  2=Ice Hockey  3=Basketball  4=Tennis
  6=Volleyball  7=Rugby  8=Handball  10=Table Tennis
  21=Darts  66=Cricket  47=eSoccer

Bookmaker registry (confirmed partner IDs / gr values from real curls):
  1xBet    partner=61,  gr=657
  22Bet    partner=151, gr=515   ← NOTE: NOT partner=2/gr=656 as docs said
  Betwinner partner=3,  gr=656
  Melbet   partner=4,  gr=656
  Megapari partner=6,  gr=656
  Helabet  partner=237, gr=None  (uses LineFeed only)
  Paripesa partner=188, gr=764
"""
from __future__ import annotations

import json
import os
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import logging
logger = logging.getLogger(__name__)


# ─── Sport ID mapping (confirmed from real API responses) ────────────────────
#
# These are the "I" (sport index) values the BetB2B platform uses.
# DO NOT guess — these come from the actual sports-list endpoint.

_B2B_SPORT_IDS: dict[str, int] = {
    "soccer":            1,
    "football":          1,
    "ice-hockey":        2,
    "basketball":        3,
    "tennis":            4,   # confirmed (was wrongly 5 before)
    "volleyball":        6,   # confirmed (was wrongly 8 before)
    "rugby":             7,   # confirmed (was wrongly 17 before)
    "handball":          8,   # confirmed (was wrongly 11 before)
    "table-tennis":      10,  # confirmed (was wrongly 13 before)
    "darts":             21,  # confirmed (was wrongly 14 before)
    "cricket":           66,  # confirmed (was wrongly 21 before)
    "mma":               9,
    "boxing":            9,
    "american-football": 16,
    "baseball":          4,   # needs verification — may conflict with tennis on some BKs
    "esoccer":           47,
    "esports":           47,
}

# Which sports each bookmaker actually offers.
# "common" sports are available on every BK; extras are per-BK.
_B2B_COMMON_SPORTS: set[int] = {1, 2, 3, 4, 6, 7, 8}  # football..handball

_BK_EXTRA_SPORTS: dict[str, set[int]] = {
    "1xbet":     {9, 10, 16, 21, 47, 66},
    "22bet":     {9, 10, 16, 21, 47, 66},
    "betwinner": {9, 10, 16, 21, 47, 66},
    "melbet":    {9, 10, 16, 21, 47, 66},
    "megapari":  {9, 10, 21, 47, 66},
    "helabet":   {10, 21},
    "paripesa":  {9, 10, 21, 66},
}

def _bk_supports_sport(bk_slug: str, sport_id: int) -> bool:
    if sport_id in _B2B_COMMON_SPORTS:
        return True
    return sport_id in _BK_EXTRA_SPORTS.get(bk_slug, set())


# ─── Bookmaker registry ──────────────────────────────────────────────────────
#
# partner_id and gr come from observed network requests to each site.
# If a bookmaker stops returning data, verify these from browser DevTools.

B2B_BOOKMAKERS: list[dict] = [
    {
        "slug":       "1xbet",
        "name":       "1xBet",
        "domain":     "1xbet.co.ke",
        "partner_id": 61,
        "gr":         657,
        "color":      "#1F8AEB",
    },
    {
        "slug":       "22bet",
        "name":       "22Bet",
        "domain":     "22bet.co.ke",
        "partner_id": 151,   # ← FIXED (was 2)
        "gr":         515,   # ← FIXED (was 656)
        "color":      "#0B2133",
    },
    {
        "slug":       "betwinner",
        "name":       "Betwinner",
        "domain":     "betwinner.co.ke",
        "partner_id": 3,
        "gr":         656,
        "color":      "#FF6600",
    },
    {
        "slug":       "melbet",
        "name":       "Melbet",
        "domain":     "melbet.co.ke",
        "partner_id": 4,
        "gr":         656,
        "color":      "#FF0000",
    },
    {
        "slug":       "megapari",
        "name":       "Megapari",
        "domain":     "megapari.com",
        "partner_id": 6,
        "gr":         656,
        "color":      "#7B2FBE",
    },
    {
        "slug":       "helabet",
        "name":       "Helabet",
        "domain":     "helabetke.com",
        "partner_id": 237,
        "gr":         None,   # Helabet has no gr; param omitted from URL
        "color":      "#9C27B0",
        "live_only":  False,  # Helabet only has upcoming (LineFeed) data
    },
    {
        "slug":       "paripesa",
        "name":       "Paripesa",
        "domain":     "paripesa.cool",
        "partner_id": 188,
        "gr":         764,
        "color":      "#FF6B35",
    },
]


# ─── Market group → canonical slug + outcome mapping ────────────────────────

_B2B_GROUP_TO_SLUG: dict[int, str] = {
    1:   "match_winner",
    2:   "asian_handicap",
    8:   "double_chance",
    15:  "btts",
    17:  "over_under",
    19:  "first_half_over_under",
    62:  "handicap_result",
}

# Maps outcome T-value → human label, per group
_OUTCOME_T_MAP: dict[int, dict[int, str]] = {
    1:  {1: "1", 2: "X", 3: "2"},
    8:  {4: "1X", 5: "12", 6: "X2"},
    17: {9: "Over", 10: "Under"},
    19: {9: "Over", 10: "Under"},
    15: {11: "Yes", 12: "No"},
    2:  {7: "1", 8: "2"},
    62: {13: "1", 14: "2"},
}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _ts_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# Headers that replicate a real Chrome/Android browser request.
# The dynamic x-hd token is omitted (JS-generated, changes per session).
_BROWSER_HEADERS: dict[str, str] = {
    "accept":               "application/json, text/plain, */*",
    "accept-language":      "en-GB,en-US;q=0.9,en;q=0.8",
    "sec-ch-ua":            '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile":     "?1",
    "sec-ch-ua-platform":   '"Android"',
    "sec-fetch-dest":       "empty",
    "sec-fetch-mode":       "cors",
    "sec-fetch-site":       "same-origin",
    "user-agent":           (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36"
    ),
    "x-requested-with":     "XMLHttpRequest",
    # BetB2B internal headers — required or requests get blocked
    "is-srv":               "false",
    "x-app-n":              "__BETTING_APP__",
    "x-mobile-project-id":  "0",
    "x-svc-source":         "__BETTING_APP__",
}


# ─── URL builders ────────────────────────────────────────────────────────────

def _build_url(bk: dict, mode: str) -> str:
    """
    Build the correct API URL for a given bookmaker and mode.

    Live:     /service-api/LiveFeed/Get1x2_VZip?...
    Upcoming: /service-api/LineFeed/Get1x2_VZip?...

    Both endpoints return the same nested JSON shape.
    No sport filter in the URL — sport is filtered client-side via SI field.
    """
    domain    = bk["domain"]
    partner   = bk["partner_id"]
    gr        = bk.get("gr")
    feed_path = "LiveFeed" if mode == "live" else "LineFeed"

    params = (
        f"count=50"
        f"&lng=en_GB"
        f"&mode=4"
        f"&country=87"
        f"&partner={partner}"
        f"&getEmpty=true"
    )
    if gr is not None:
        params += f"&gr={gr}"
    if mode == "upcoming":
        params += "&tz=3"

    return f"https://{domain}/service-api/{feed_path}/Get1x2_VZip?{params}"


def _build_curl_command(url: str, referer: str) -> str:
    """
    Return a copy-pasteable curl command for this request.

    Uses single-quote wrapping so embedded double-quotes in header values
    (e.g. sec-ch-ua brand strings) survive copy-paste into a shell unchanged.
    Matches the flags used in _curl_get: -g (no globbing) and -- (end of options).
    """
    def _q(v: str) -> str:
        # Escape any literal single-quotes inside the value
        return v.replace("'", "\'\''")

    header_args = " ".join(
        f"-H '{k}: {_q(v)}'" for k, v in _BROWSER_HEADERS.items()
    )
    return (
        f"curl -s -g -m 20 "
        f"{header_args} "
        f"-H 'referer: {_q(referer)}' "
        f"-- '{_q(url)}'"
    )


# ─── HTTP fetch via system curl ───────────────────────────────────────────────

def _curl_get(url: str, referer: str) -> dict | None:
    """
    Execute the API request through system curl.

    Returns parsed JSON dict, or None on failure.
    Using system curl rather than requests/httpx because BetB2B sites
    actively block Python HTTP libraries; curl with browser headers works.

    Key flags:
      -g   Disable URL globbing — prevents curl misreading { } [ ] in query strings
      --   Explicit end-of-options marker before the URL — prevents curl treating
           a URL that starts with '-' or contains special chars as an option flag
    """
    if not url:
        logger.error("[curl] _curl_get called with empty URL")
        return None

    cmd = [
        "curl", "-s", "-g", "-m", "20",   # -g = no globbing
        "-H", f"referer: {referer}",
    ]
    for key, val in _BROWSER_HEADERS.items():
        cmd += ["-H", f"{key}: {val}"]
    cmd += ["--", url]                     # -- separates options from URL

    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if res.returncode != 0:
            # Surface the actual curl error so it's visible in logs / terminal
            stderr_snippet = (res.stderr or "").strip()[:300]
            print(f"  ✗ curl exit {res.returncode}: {stderr_snippet or '(no stderr)'}")
            logger.warning("[curl] exit=%d stderr=%s url=%s",
                           res.returncode, stderr_snippet, url)
            return None

        if not res.stdout.strip():
            print("  ✗ curl returned empty body")
            logger.warning("[curl] empty body for url=%s", url)
            return None

        return json.loads(res.stdout)

    except json.JSONDecodeError as exc:
        snippet = (res.stdout if res else "")[:200]
        print(f"  ✗ JSON decode error: {exc} — body starts: {snippet!r}")
        logger.warning("[curl] JSON decode: %s", exc)
        return None
    except Exception as exc:
        print(f"  ✗ curl exception: {exc}")
        logger.warning("[curl] exception: %s", exc)
        return None


# ─── Logging helpers ──────────────────────────────────────────────────────────

def _log_curl(
    output_dir: str,
    bk_slug: str,
    sport_id: int,
    mode: str,
    curl_cmd: str,
    success: bool,
) -> None:
    """
    Append the exact copy-pasteable curl command to b2b_curl_log.txt.
    One line per request, tab-separated metadata prefix.
    """
    if not output_dir:
        return
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "b2b_curl_log.txt")
    status = "OK  " if success else "FAIL"
    line = (
        f"[{_now_iso()}] {bk_slug:>10} | sport={sport_id:<3} "
        f"| {mode:<9} | {status} | {curl_cmd}\n"
    )
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(line)


def _log_json(
    output_dir: str,
    bk_slug: str,
    sport_id: int,
    mode: str,
    url: str,
    curl_cmd: str,
    raw: Any,
    games: list[dict],
) -> None:
    """
    Save the full raw API response + parsed game sample to a JSON file.
    File: b2b_raw_{slug}_{sport_id}_{mode}.json
    """
    if not output_dir:
        return
    os.makedirs(output_dir, exist_ok=True)
    filename = f"b2b_raw_{bk_slug}_{sport_id}_{mode}.json"
    path = os.path.join(output_dir, filename)
    dump = {
        "meta": {
            "bookmaker":     bk_slug,
            "sport_id":      sport_id,
            "mode":          mode,
            "url":           url,
            "curl_command":  curl_cmd,
            "harvested_at":  _now_iso(),
            "games_found":   len(games),
        },
        "raw_response": raw,                # full original API response
        "parsed_games_sample": games[:10],  # first 10 parsed games for inspection
    }
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(dump, fh, indent=2, ensure_ascii=False, default=str)
    logger.info("[b2b:%s] raw JSON saved → %s", bk_slug, path)


# ─── Response parser ──────────────────────────────────────────────────────────

def _extract_games_from_value(value: list, sport_id: int) -> list[dict]:
    """
    Flatten the nested BetB2B API response into a plain list of game dicts.

    The API returns sport-wrapper objects, each containing:
      sport_obj.L[]           — countries / regions
        .SC[]                 — sub-competitions (leagues)
          .G[]                — individual games  ← what we want

    Some responses (e.g. live with very few matches) may return games
    directly as flat list items — we handle both shapes.
    """
    games: list[dict] = []

    for item in value:
        if not isinstance(item, dict):
            continue

        # ── Shape A: sport wrapper (has "L" = leagues list) ──────────────
        if "L" in item:
            # "I" is the sport ID on the wrapper
            if item.get("I") != sport_id:
                continue
            for country in (item.get("L") or []):
                if not isinstance(country, dict):
                    continue
                for sc in (country.get("SC") or []):
                    if not isinstance(sc, dict):
                        continue
                    for game in (sc.get("G") or []):
                        if isinstance(game, dict):
                            games.append(game)

        # ── Shape B: flat game object (has "O1" or "O1E" team name) ──────
        elif "O1" in item or "O1E" in item:
            if item.get("SI") == sport_id:
                games.append(item)

    return games


def _parse_events(
    raw_events: list[dict],
    additional_events: list[dict],
) -> dict[str, dict[str, float]]:
    """
    Convert raw E / AE event lists into:
      { "market_slug": { "OutcomeLabel": price, ... }, ... }

    Each raw event: { "G": group_id, "T": outcome_type, "C": coefficient,
                      "P": optional handicap/line }
    """
    markets: dict[str, dict[str, float]] = {}

    def _process(ev: dict, override_gid: int | None = None) -> None:
        gid = override_gid if override_gid is not None else ev.get("G")
        if gid is None:
            return

        # Parse coefficient
        coeff = ev.get("C") or ev.get("CV")
        try:
            price = float(coeff)
        except (TypeError, ValueError):
            return
        if price <= 1.0:
            return  # suspended or invalid

        t = ev.get("T")
        if t is None:
            return

        # Map T → human label
        outcome = _OUTCOME_T_MAP.get(gid, {}).get(t, str(t))

        # Append handicap / line value for markets that use it
        p = ev.get("P")
        if p is not None and gid in (2, 17, 19, 62):
            outcome = f"{outcome}@{p}"

        slug = _B2B_GROUP_TO_SLUG.get(gid, f"group_{gid}")
        if slug not in markets:
            markets[slug] = {}
        # Keep the highest available price for each outcome
        if outcome not in markets[slug] or price > markets[slug][outcome]:
            markets[slug][outcome] = price

    for ev in raw_events:
        if isinstance(ev, dict):
            _process(ev)

    # Additional events may carry sub-events in "ME" list
    for ae in additional_events:
        if not isinstance(ae, dict):
            continue
        parent_gid = ae.get("G")
        for me in (ae.get("ME") or []):
            if isinstance(me, dict):
                _process(me, override_gid=me.get("G") or parent_gid)

    return {k: v for k, v in markets.items() if v}


def _parse_game(game: dict, bk: dict, sport_slug: str, mode: str) -> dict | None:
    """
    Convert a raw game dict into our canonical match format.
    Returns None if the game has no parseable odds or team names.
    """
    home = (game.get("O1E") or game.get("O1") or "").strip()
    away = (game.get("O2E") or game.get("O2") or "").strip()
    if not home or not away:
        return None

    game_id  = game.get("I") or game.get("GameId")
    comp     = (game.get("LE") or game.get("L") or "").strip()
    country  = (game.get("CN") or "").strip()
    start_ts = game.get("S")
    start_dt = _ts_to_iso(start_ts) if start_ts else None

    markets = _parse_events(
        game.get("E") or [],
        game.get("AE") or [],
    )
    if not markets:
        return None  # game has no usable odds — skip

    # Live score extraction
    score_home = score_away = None
    sc_block = game.get("SC")
    if isinstance(sc_block, dict):
        fs = sc_block.get("FS") or {}
        try:
            score_home = int(fs["S1"]) if "S1" in fs else None
            score_away = int(fs["S2"]) if "S2" in fs else None
        except (ValueError, TypeError):
            pass

    match_id = f"{bk['partner_id']}:{game_id}"

    return {
        "betradar_id":   "",
        "b2b_match_id":  match_id,
        "external_id":   str(game_id or ""),
        "partner_id":    bk["partner_id"],
        "home_team":     home,
        "away_team":     away,
        "start_time":    start_dt,
        "competition":   comp,
        "country":       country,
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
        "status":       "live" if mode == "live" else "upcoming",
        "score_home":   score_home,
        "score_away":   score_away,
        "match_time":   None,
        "is_live":      mode == "live",
        "harvested_at": _now_iso(),
    }


# ─── Per-bookmaker fetch ──────────────────────────────────────────────────────

# Simple cache: skip sport/mode combos that returned nothing last time.
# Cleared automatically when the process restarts (i.e. on Celery worker restart).
_UNSUPPORTED_CACHE: set[tuple] = set()


def fetch_single_bk(
    bk: dict,
    sport_slug: str,
    mode: str = "upcoming",
    page: int = 1,
    page_size: int = 100,
    output_dir: str = "",
) -> list[dict]:
    """
    Fetch, parse, log, and return matches for one bookmaker + sport + mode.

    Steps:
      1. Guard — skip if sport not supported or previously empty
      2. Build URL + referer
      3. Print and log the curl command
      4. Execute request via system curl
      5. Parse nested Value → flat game list, filtered by sport_id
      6. Save raw JSON and parsed results to output_dir
      7. Convert raw games → canonical match dicts
      8. Print success summary
    """
    slug     = bk["slug"]
    sport_id = _B2B_SPORT_IDS.get(sport_slug.lower())
    if sport_id is None:
        logger.warning("[b2b:%s] unknown sport slug: %s", slug, sport_slug)
        return []

    cache_key = (slug, sport_id, mode)

    # ── Guard: sport not offered by this BK ──────────────────────────────────
    if not _bk_supports_sport(slug, sport_id):
        logger.debug("[b2b:%s] sport %s (id=%d) not in support list — skip",
                     slug, sport_slug, sport_id)
        return []

    # ── Guard: previously returned nothing (avoid hammering) ─────────────────
    if cache_key in _UNSUPPORTED_CACHE:
        logger.debug("[b2b:%s] %s/%s previously empty — skip", slug, sport_slug, mode)
        return []

    # ── Build URL and curl command ────────────────────────────────────────────
    domain  = bk["domain"]
    url     = _build_url(bk, mode)
    referer = f"https://{domain}/"
    curl_cmd = _build_curl_command(url, referer)

    # Always print the curl so the operator can reproduce any request manually
    print(f"\n{'─'*70}")
    print(f"[b2b:{slug}] {sport_slug}/{mode}")
    print(f"URL:  {url}")
    print(f"CURL: {curl_cmd}")

    t0 = time.perf_counter()

    # ── HTTP request ──────────────────────────────────────────────────────────
    raw = _curl_get(url, referer)

    success = False
    raw_games: list[dict] = []

    if raw:
        err = raw.get("ErrorCode") or raw.get("Error") or 0
        if err == 0 or err == "":
            value = raw.get("Value") or raw.get("value") or []
            raw_games = _extract_games_from_value(value, sport_id)
            success = True  # API responded correctly even if 0 games

    # ── Log curl command (always, success or not) ─────────────────────────────
    _log_curl(output_dir, slug, sport_id, mode, curl_cmd, success)

    if not raw:
        print(f"  ✗ No response or curl error")
        _UNSUPPORTED_CACHE.add(cache_key)
        return []

    if not success:
        err_detail = raw.get("ErrorCode") or raw.get("Error") or "unknown"
        print(f"  ✗ API error: {err_detail}")
        _UNSUPPORTED_CACHE.add(cache_key)
        return []

    print(f"  ✓ API OK — {len(raw_games)} raw games for sport_id={sport_id}")

    # ── Paginate ──────────────────────────────────────────────────────────────
    start      = (page - 1) * page_size
    page_games = raw_games[start : start + page_size]

    # ── Parse raw games → canonical match dicts ───────────────────────────────
    matches: list[dict] = []
    for game in page_games:
        try:
            m = _parse_game(game, bk, sport_slug, mode)
            if m:
                matches.append(m)
        except Exception as exc:
            logger.debug("[b2b:%s] parse error on game: %s", slug, exc)

    elapsed_ms = int((time.perf_counter() - t0) * 1000)

    # ── Save raw JSON + parsed sample to disk ─────────────────────────────────
    _log_json(output_dir, slug, sport_id, mode, url, curl_cmd, raw, matches)

    # ── Print result summary ──────────────────────────────────────────────────
    if matches:
        print(f"  ✅ SUCCESS — {len(matches)} matches parsed "
              f"(page {page}, {elapsed_ms}ms)")
        for m in matches[:3]:
            print(f"     {m['home_team']} vs {m['away_team']}  "
                  f"| {m['competition']}  | markets={m['market_count']}")
        if len(matches) > 3:
            print(f"     ... and {len(matches)-3} more")
    else:
        print(f"  ⚠ 0 matches with parseable odds ({elapsed_ms}ms)")
        if not raw_games:
            # Real empty — cache so we don't retry this combo every poll cycle
            _UNSUPPORTED_CACHE.add(cache_key)

    logger.info(
        "[b2b:%s] %s/%s page=%d → %d/%d matches (%dms)",
        slug, sport_slug, mode, page, len(matches), len(raw_games), elapsed_ms,
    )
    return matches


# ─── Parallel fetch across all bookmakers ────────────────────────────────────

def fetch_all_b2b_sport(
    sport_slug: str,
    mode: str = "upcoming",
    bookmakers: list[dict] | None = None,
    max_workers: int = 7,
    output_dir: str = "",
) -> dict[str, list[dict]]:
    """
    Fetch sport/mode from every registered bookmaker in parallel.

    Returns { bk_slug: [match, ...] }
    """
    bks = bookmakers or B2B_BOOKMAKERS
    results: dict[str, list[dict]] = {}

    print(f"\n{'═'*70}")
    print(f"Harvesting {sport_slug.upper()} / {mode.upper()} "
          f"from {len(bks)} bookmakers...")

    with ThreadPoolExecutor(
        max_workers=min(max_workers, len(bks)),
        thread_name_prefix="b2b",
    ) as pool:
        futures = {
            pool.submit(
                fetch_single_bk, bk, sport_slug, mode, 1, 200, output_dir
            ): bk
            for bk in bks
        }
        for fut in as_completed(futures):
            bk = futures[fut]
            try:
                results[bk["slug"]] = fut.result()
            except Exception as exc:
                logger.error("[b2b:%s] %s/%s unhandled: %s",
                             bk["slug"], sport_slug, mode, exc)
                results[bk["slug"]] = []

    total = sum(len(v) for v in results.values())
    print(f"\n{'═'*70}")
    print(f"TOTAL: {total} matches across {len(bks)} bookmakers "
          f"({sport_slug}/{mode})")
    for slug, matches in sorted(results.items()):
        status = "✅" if matches else "⚠ "
        print(f"  {status} {slug:<12} {len(matches):>4} matches")

    logger.info("[b2b:all] %s/%s → %d total", sport_slug, mode, total)
    return results


# ─── Cross-bookmaker match merger ─────────────────────────────────────────────

def merge_b2b_by_match(
    per_bk_results: dict[str, list[dict]],
    sport_slug: str,
) -> list[dict]:
    """
    Deduplicate and merge matches from multiple bookmakers.

    Two games are considered the same if (home, away, kickoff_minute) match.
    When the same game appears on multiple BKs, their bookmakers{} dicts and
    markets are merged, keeping the best available price per outcome.
    """
    # Flatten all matches, tagging each with its source BK slug
    all_matches: list[dict] = []
    for bk_slug, matches in per_bk_results.items():
        for m in matches:
            m = dict(m)
            m["_bk_slug"] = bk_slug
            all_matches.append(m)

    if not all_matches:
        return []

    unified: list[dict] = []
    key_to_idx: dict[str, int] = {}

    for m in all_matches:
        bk_slug = m.pop("_bk_slug")
        home    = m.get("home_team", "").lower().strip()
        away    = m.get("away_team", "").lower().strip()
        start   = (m.get("start_time") or "")[:16]   # minute precision
        key     = f"{home}|||{away}|||{start}"

        if key in key_to_idx:
            # Merge into existing record
            existing  = unified[key_to_idx[key]]
            bk_data   = m["bookmakers"].get(bk_slug) or {}

            if bk_data:
                existing["bookmakers"][bk_slug] = bk_data
                for market_slug, outcomes in (bk_data.get("markets") or {}).items():
                    if market_slug not in existing["markets"]:
                        existing["markets"][market_slug] = {}
                    for outcome, price in outcomes.items():
                        prev = existing["markets"][market_slug].get(outcome, 0.0)
                        if price > prev:
                            existing["markets"][market_slug][outcome] = price
                existing["market_count"] = len(existing["markets"])
        else:
            # First time we see this game
            merged              = dict(m)
            merged["bookmakers"] = dict(m.get("bookmakers") or {})
            merged["markets"]    = dict(m.get("markets") or {})
            key_to_idx[key]      = len(unified)
            unified.append(merged)

    logger.info("[b2b:merge] %s → %d unified from %d raw",
                sport_slug, len(unified), len(all_matches))
    return unified


# ─── Full harvest entry-points ────────────────────────────────────────────────

def harvest_b2b_sport(
    sport_slug: str,
    mode: str = "upcoming",
    bookmakers: list[dict] | None = None,
    output_dir: str = "",
) -> list[dict]:
    """
    Fetch all BKs in parallel, then merge by match.
    This is the main function called by Celery tasks.
    """
    per_bk = fetch_all_b2b_sport(
        sport_slug, mode, bookmakers, output_dir=output_dir
    )
    return merge_b2b_by_match(per_bk, sport_slug)


def harvest_b2b_page(
    bk_slug: str,
    sport_slug: str,
    page: int,
    page_size: int = 100,
    mode: str = "upcoming",
    output_dir: str = "",
) -> list[dict]:
    """
    Fetch a single page from one bookmaker — used by Celery paged tasks.
    """
    bk = next((b for b in B2B_BOOKMAKERS if b["slug"] == bk_slug), None)
    if not bk:
        logger.warning("[b2b] unknown bk_slug: %s", bk_slug)
        return []
    return fetch_single_bk(bk, sport_slug, mode, page, page_size, output_dir)


# ─── Live poller (background thread) ─────────────────────────────────────────

class B2BLivePoller:
    """
    Continuously polls all bookmakers for in-play data and publishes
    updates to Redis for SSE streaming.

    Runs in a daemon thread — start() once at app startup.
    """
    _POLL_SPORTS: list[str] = [
        "soccer", "basketball", "tennis", "ice-hockey",
        "volleyball", "table-tennis",
    ]

    def __init__(self, redis_client, interval: float = 8.0, output_dir: str = ""):
        self._r         = redis_client
        self._interval  = interval
        self._output_dir = output_dir
        self._running   = False
        self._thread    = None

    def start(self) -> None:
        import threading
        self._running = True
        self._thread  = threading.Thread(
            target=self._loop, daemon=True, name="b2b-live-poller"
        )
        self._thread.start()
        logger.info("[b2b:live] poller started (interval=%.1fs)", self._interval)

    def stop(self) -> None:
        self._running = False

    def _loop(self) -> None:
        from app.workers.redis_bus import publish_b2b_live_update
        while self._running:
            t0 = time.perf_counter()
            for sport_slug in self._POLL_SPORTS:
                try:
                    matches = harvest_b2b_sport(
                        sport_slug, mode="live", output_dir=self._output_dir
                    )
                    if matches:
                        publish_b2b_live_update(sport_slug, matches, self._r)
                except Exception as exc:
                    logger.warning("[b2b:live] %s error: %s", sport_slug, exc)
            elapsed = time.perf_counter() - t0
            time.sleep(max(0.0, self._interval - elapsed))


# ─── Registry helpers ─────────────────────────────────────────────────────────

def get_bk_by_slug(slug: str) -> dict | None:
    return next((b for b in B2B_BOOKMAKERS if b["slug"] == slug), None)

def get_all_slugs() -> list[str]:
    return [b["slug"] for b in B2B_BOOKMAKERS]

B2B_SUPPORTED_SPORTS: list[str] = sorted(_B2B_SPORT_IDS.keys())


# ─── CLI quick-test ───────────────────────────────────────────────────────────
# Run directly:  python b2b_harvester.py
# Fetches football/upcoming from all BKs, dumps to ./b2b_debug/

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    sport = sys.argv[1] if len(sys.argv) > 1 else "soccer"
    mode  = sys.argv[2] if len(sys.argv) > 2 else "upcoming"
    outdir = "./b2b_debug"

    print(f"\n🚀 B2B Harvester test: sport={sport}, mode={mode}")
    print(f"   Output dir: {outdir}")

    results = harvest_b2b_sport(sport, mode, output_dir=outdir)
    print(f"\n🏁 Done — {len(results)} merged matches total")
    if results:
        print("\nSample match:")
        import pprint
        pprint.pprint(results[0])