"""
app/workers/b2b_harvester.py
=============================
BetB2B family harvester — 7 Kenya bookmakers on the same platform.

KEY FIXES vs previous version
──────────────────────────────
• Correct partner IDs from real browser requests:
    betwinner=152, megapari=192, melbet=417
• Correct domains:
    betwinner → betwinner.ke
    melbet    → mel-bet.co.ke
    megapari  → 1849932mp.pro
• Response is FLAT: Value[] contains game objects directly (not sport wrappers)
  Filter by game["SI"] == sport_id
• All sports fetched concurrently per bookmaker
• --sample mode: prints one match per sport for market mapping development
• GetSportsShortZip endpoint for browsing sport/competition tree

API ENDPOINTS
─────────────
Upcoming: GET /service-api/LineFeed/Get1x2_VZip
             ?count=50&lng=en&mode=4&country=87&partner={P}
              &virtualSports=true[&gr={gr}]

Live:     GET /service-api/LiveFeed/Get1x2_VZip
             ?count=50&lng=en&mode=4&country=87&partner={P}
              &virtualSports=true[&gr={gr}]

Sports:   GET /service-api/LineFeed/GetSportsShortZip
             ?lng=en&country=87&partner={P}&virtualSports=true
              [&gr={gr}]&groupChamps=true

Match detail (full markets):
          GET /service-api/LineFeed/GetSportsShortZip
             ?lng=en&country=87&partner={P}&virtualSports=true
              &gr={gr}&groupChamps=true
              (then navigate to game by ID in the tree)

RESPONSE SHAPE (flat)
─────────────────────
{
  "ErrorCode": 0,
  "Value": [          ← flat list of game objects
    {
      "I":   716170552,          ← game ID
      "SI":  1,                  ← sport ID (1=soccer, 2=ice-hockey, ...)
      "O1E": "West Ham United",  ← home team (English)
      "O2E": "Arsenal",          ← away team (English)
      "LE":  "England. Premier League",
      "S":   1778427000,         ← start Unix timestamp
      "E": [                     ← events/odds
        {"G": 1, "T": 1, "C": 6.4},   ← G=group, T=type, C=coefficient
        ...
      ]
    },
    ...
  ]
}

SPORT IDs (same across all BetB2B bookmakers — shared platform)
───────────────────────────────────────────────────────────────
  1=Football/Soccer   2=Ice Hockey    3=Basketball    4=Tennis
  5=Baseball          6=Volleyball    7=Rugby         8=Handball
  9=MMA/Boxing       10=Table Tennis 16=American Football
  21=Darts           47=eSoccer      66=Cricket

MARKET GROUPS (G field in E array)
───────────────────────────────────
  G=1   → match_winner     (T:1=Home, 2=Draw, 3=Away)
  G=2   → asian_handicap   (T:7=Home, 8=Away, P=line)
  G=8   → double_chance    (T:4=1X, 5=12, 6=X2)
  G=15  → btts             (T:11=Yes, 12=No, P=line)
  G=17  → over_under       (T:9=Over, 10=Under, P=line)
  G=19  → first_half_ou    (T:180=Over, 181=Under)
  G=62  → handicap_result  (T:13=Home, 14=Away, P=line)
  G=99  → asian_total      (T:3827=Over, 3828=Under, P=line)
  G=2854→ asian_handicap2  (T:3829=Home, 3830=Away, P=line)
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


# =============================================================================
# BOOKMAKER REGISTRY  (verified from real browser DevTools, May 2026)
# =============================================================================

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
        "partner_id": 151,
        "gr":         515,
        "color":      "#0B2133",
    },
    {
        "slug":       "betwinner",
        "name":       "Betwinner",
        "domain":     "betwinner.ke",           # ← .ke not .co.ke
        "partner_id": 152,                      # ← 152 not 3
        "gr":         None,
        "color":      "#FF6600",
    },
    {
        "slug":       "melbet",
        "name":       "Melbet",
        "domain":     "mel-bet.co.ke",          # ← mel-bet not melbet
        "partner_id": 417,                      # ← 417 not 4
        "gr":         None,
        "color":      "#FF0000",
    },
    {
        "slug":       "megapari",
        "name":       "Megapari",
        "domain":     "1849932mp.pro",          # ← different domain
        "partner_id": 192,                      # ← 192 not 6
        "gr":         None,
        "color":      "#7B2FBE",
    },
    {
        "slug":       "helabet",
        "name":       "Helabet",
        "domain":     "helabetke.com",
        "partner_id": 237,
        "gr":         None,
        "color":      "#9C27B0",
    },
    {
        "slug":       "paripesa",
        "name":       "Paripesa",
        "domain":     "paripesa.cool",
        "partner_id": 188,
        "gr":         None,
        "color":      "#FF6B35",
    },
]

# Indexed for quick lookup
_BK_BY_SLUG: dict[str, dict] = {b["slug"]: b for b in B2B_BOOKMAKERS}


# =============================================================================
# SPORT ID MAP  (shared across all BetB2B bookmakers — same platform)
# =============================================================================

B2B_SPORT_IDS: dict[str, int] = {
    # ── Soccer / Football ────────────────────────────────────────────────────
    "soccer":            1,
    "football":          1,
    # ── eSoccer / Virtual ────────────────────────────────────────────────────
    "esoccer":           47,
    "efootball":         47,
    "e-football":        47,
    "virtual-football":  47,
    # ── Ice Hockey ───────────────────────────────────────────────────────────
    "ice-hockey":        2,
    "icehockey":         2,
    # ── Basketball ───────────────────────────────────────────────────────────
    "basketball":        3,
    # ── Tennis ───────────────────────────────────────────────────────────────
    "tennis":            4,
    # ── Baseball ─────────────────────────────────────────────────────────────
    "baseball":          5,
    # ── Volleyball ───────────────────────────────────────────────────────────
    "volleyball":        6,
    # ── Rugby ────────────────────────────────────────────────────────────────
    "rugby":             7,
    "rugby-league":      7,
    "rugby-union":       7,
    # ── Handball ─────────────────────────────────────────────────────────────
    "handball":          8,
    # ── MMA / Combat ─────────────────────────────────────────────────────────
    "mma":               9,
    "ufc":               9,
    "boxing":            9,
    # ── Table Tennis ─────────────────────────────────────────────────────────
    "table-tennis":      10,
    "tabletennis":       10,
    # ── American Football ────────────────────────────────────────────────────
    "american-football": 16,
    "americanfootball":  16,
    "nfl":               16,
    # ── Darts ────────────────────────────────────────────────────────────────
    "darts":             21,
    # ── Cricket ──────────────────────────────────────────────────────────────
    "cricket":           66,
}

# Reverse: sport_id → canonical slug
_ID_TO_SLUG: dict[int, str] = {
    1:  "soccer",
    2:  "ice-hockey",
    3:  "basketball",
    4:  "tennis",
    5:  "baseball",
    6:  "volleyball",
    7:  "rugby",
    8:  "handball",
    9:  "mma",
    10: "table-tennis",
    16: "american-football",
    21: "darts",
    47: "esoccer",
    66: "cricket",
}

ALL_SPORT_SLUGS: list[str] = [
    "soccer", "basketball", "tennis", "ice-hockey", "volleyball",
    "cricket", "rugby", "handball", "table-tennis", "mma",
    "boxing", "darts", "american-football", "baseball", "esoccer",
]


# =============================================================================
# MARKET GROUP → CANONICAL SLUG + OUTCOME LABELS
# =============================================================================

_GROUP_TO_SLUG: dict[int, str] = {
    1:    "match_winner",
    2:    "asian_handicap",
    8:    "double_chance",
    15:   "btts",
    17:   "over_under",
    19:   "first_half_over_under",
    62:   "handicap_result",
    99:   "asian_total",
    2854: "asian_handicap_2",
}

# T-value → outcome label, per group
_T_LABELS: dict[int, dict[int, str]] = {
    1:    {1: "1",    2: "X",     3: "2"},
    2:    {7: "1",    8: "2"},
    8:    {4: "1X",   5: "12",    6: "X2"},
    15:   {11: "Yes", 12: "No"},
    17:   {9: "Over", 10: "Under"},
    19:   {180: "Over", 181: "Under"},
    62:   {13: "1",   14: "2"},
    99:   {3827: "Over", 3828: "Under"},
    2854: {3829: "1",    3830: "2"},
}


# =============================================================================
# HTTP — system curl (Python HTTP libs are blocked by BetB2B sites)
# =============================================================================

_BASE_HEADERS: dict[str, str] = {
    "accept":               "application/json, text/plain, */*",
    "accept-language":      "en-GB,en-US;q=0.9,en;q=0.8",
    "sec-ch-ua":            '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile":     "?1",
    "sec-ch-ua-platform":   '"Android"',
    "sec-fetch-dest":       "empty",
    "sec-fetch-mode":       "cors",
    "sec-fetch-site":       "same-origin",
    "user-agent":           "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36",
    "x-requested-with":     "XMLHttpRequest",
    "is-srv":               "false",
    "x-app-n":              "__BETTING_APP__",
    "x-mobile-project-id":  "0",
    "x-svc-source":         "__BETTING_APP__",
    "content-type":         "application/json",
}


def _curl(url: str, referer: str, timeout: int = 20) -> dict | None:
    """
    Execute a request via system curl.
    BetB2B sites block Python HTTP libraries — system curl with browser headers works.
    """
    cmd = ["curl", "-s", "-g", f"-m{timeout}"]
    for k, v in _BASE_HEADERS.items():
        cmd += ["-H", f"{k}: {v}"]
    cmd += ["-H", f"referer: {referer}", "--", url]

    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if res.returncode != 0 or not res.stdout.strip():
            return None
        return json.loads(res.stdout)
    except json.JSONDecodeError:
        return None
    except Exception as exc:
        logger.debug("[b2b] curl exception: %s", exc)
        return None


# =============================================================================
# URL BUILDERS
# =============================================================================

def _line_url(bk: dict, *, count: int = 50, start: int = 0,
              extra: dict | None = None) -> str:
    """LineFeed (upcoming) URL. Use start= for pagination."""
    p  = bk["partner_id"]
    gr = bk.get("gr")
    q  = f"count={count}&lng=en&mode=4&country=87&partner={p}&virtualSports=true"
    if gr:    q += f"&gr={gr}"
    if start: q += f"&start={start}"
    if extra: q += "&" + "&".join(f"{k}={v}" for k, v in extra.items())
    return f"https://{bk['domain']}/service-api/LineFeed/Get1x2_VZip?{q}"


def _live_url(bk: dict, *, count: int = 50, start: int = 0) -> str:
    """LiveFeed (in-play) URL. Use start= for pagination."""
    p  = bk["partner_id"]
    gr = bk.get("gr")
    q  = f"count={count}&lng=en&mode=4&country=87&partner={p}&virtualSports=true"
    if gr:    q += f"&gr={gr}"
    if start: q += f"&start={start}"
    return f"https://{bk['domain']}/service-api/LiveFeed/Get1x2_VZip?{q}"


def _sports_url(bk: dict) -> str:
    """GetSportsShortZip — sport/competition tree."""
    p  = bk["partner_id"]
    gr = bk.get("gr") or ""
    q  = f"lng=en&country=87&partner={p}&virtualSports=true&groupChamps=true"
    if gr:
        q += f"&gr={gr}"
    return f"https://{bk['domain']}/service-api/LineFeed/GetSportsShortZip?{q}"


def _referer(bk: dict, path: str = "/en/line") -> str:
    return f"https://{bk['domain']}{path}"


# =============================================================================
# RESPONSE PARSERS
# =============================================================================

def _parse_events(events: list[dict], extra_events: list[dict] | None = None) -> dict[str, dict]:
    """
    Convert E[] and AE[] arrays into canonical markets dict:
      { "market_slug": { "OutcomeLabel[@line]": price, ... } }
    """
    markets: dict[str, dict[str, float]] = defaultdict(dict)

    def _process(ev: dict) -> None:
        gid = ev.get("G")
        t   = ev.get("T")
        c   = ev.get("C") or ev.get("CV")
        if gid is None or t is None or c is None:
            return
        try:
            price = float(c)
        except (TypeError, ValueError):
            return
        if price <= 1.0:
            return

        # Outcome label
        label = _T_LABELS.get(gid, {}).get(t, f"T{t}")

        # Append line/handicap for relevant markets
        p = ev.get("P")
        if p is not None and gid in (2, 17, 19, 62, 99, 2854):
            label = f"{label}@{p}"

        slug = _GROUP_TO_SLUG.get(gid, f"group_{gid}")

        # Keep best price per outcome
        prev = markets[slug].get(label, 0.0)
        if price > prev:
            markets[slug][label] = price

    for ev in events or []:
        if isinstance(ev, dict):
            _process(ev)

    # AE may have nested ME sub-events
    for ae in extra_events or []:
        if not isinstance(ae, dict):
            continue
        for me in ae.get("ME") or [ae]:
            if isinstance(me, dict):
                _process(me)

    return {k: v for k, v in markets.items() if v}


def _parse_game(game: dict, bk: dict, sport_slug: str, mode: str) -> dict | None:
    """Convert a flat game object into canonical match format."""
    home = (game.get("O1E") or game.get("O1") or "").strip()
    away = (game.get("O2E") or game.get("O2") or "").strip()
    if not home or not away:
        return None

    # Try sport-specific mapper first; fall back to built-in parser
    try:
        from app.utils.mapping.b2b import normalize_b2b_markets
        all_events = list(game.get("E") or []) + list(game.get("AE") or [])
        markets = normalize_b2b_markets(sport_slug, all_events)
    except ImportError:
        markets = _parse_events(game.get("E") or [], game.get("AE"))
    except Exception:
        markets = _parse_events(game.get("E") or [], game.get("AE"))

    if not markets:
        return None

    game_id  = game.get("I") or game.get("GameId")
    comp     = (game.get("LE") or game.get("L") or "").strip()
    start_ts = game.get("S")
    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if start_ts else None
    match_id = f"{bk['slug']}:{game_id}"

    return {
        "b2b_match_id":  match_id,
        "external_id":   str(game_id or ""),
        "betradar_id":   "",
        "home_team":     home,
        "away_team":     away,
        "start_time":    start_dt,
        "competition":   comp,
        "sport":         sport_slug,
        "source":        bk["slug"],
        "is_live":       mode == "live",
        "status":        "live" if mode == "live" else "upcoming",
        "markets":       markets,
        "market_count":  len(markets),
        "bookmakers": {
            bk["slug"]: {
                "bookmaker": bk["name"],
                "slug":      bk["slug"],
                "match_id":  match_id,
                "markets":   markets,
            }
        },
        "harvested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _parse_value(value: list, bk: dict, sport_slug: str, mode: str) -> list[dict]:
    """
    Parse the flat Value[] array.
    Each element is either a direct game object (has O1E/O1) or
    a sport-wrapper (has "L" = leagues). Handle both.
    """
    sport_id = B2B_SPORT_IDS.get(sport_slug.lower())
    matches: list[dict] = []

    def _process_game(game: dict) -> None:
        # Filter by sport
        if sport_id and game.get("SI") not in (sport_id, None):
            return
        m = _parse_game(game, bk, sport_slug, mode)
        if m:
            matches.append(m)

    for item in value:
        if not isinstance(item, dict):
            continue

        # Shape A: flat game (has team name fields)
        if "O1E" in item or "O1" in item:
            _process_game(item)

        # Shape B: sport wrapper (has "L" list of countries)
        elif "L" in item and isinstance(item.get("L"), list):
            # Only descend into matching sport
            if sport_id and item.get("I") != sport_id:
                continue
            for country in item["L"]:
                for sc in (country.get("SC") or []):
                    for game in (sc.get("G") or []):
                        if isinstance(game, dict):
                            _process_game(game)

    return matches


# =============================================================================
# PER-BOOKMAKER FETCH
# =============================================================================

def fetch_bk_sport(
    bk: dict,
    sport_slug: str,
    mode: str = "upcoming",
    count: int = 200,
    verbose: bool = True,
) -> list[dict]:
    """
    Fetch all matches for one bookmaker + sport + mode.
    Returns canonical match list.
    """
    slug = bk["slug"]
    url  = _live_url(bk, count=count) if mode == "live" else _line_url(bk, count=count)
    ref  = _referer(bk)

    t0  = time.perf_counter()
    raw = _curl(url, ref)
    ms  = int((time.perf_counter() - t0) * 1000)

    if not raw:
        if verbose:
            print(f"  ❌ {slug:<12} {sport_slug:<16} {mode:<9} — no response ({ms}ms)")
        return []

    if raw.get("ErrorCode", -1) not in (0, ""):
        if verbose:
            print(f"  ❌ {slug:<12} {sport_slug:<16} {mode:<9} — API error {raw.get('ErrorCode')} ({ms}ms)")
        return []

    value   = raw.get("Value") or []
    matches = _parse_value(value, bk, sport_slug, mode)

    if verbose:
        status = "✅" if matches else "⚠ "
        print(f"  {status} {slug:<12} {sport_slug:<16} {mode:<9} — {len(matches):4} matches ({ms}ms)")

    return matches


# =============================================================================
# ALL-SPORTS CONCURRENT FETCH (single bookmaker)
# =============================================================================

def fetch_bk_all_sports(
    bk: dict,
    mode: str = "upcoming",
    sports: list[str] | None = None,
    workers: int = 8,
    verbose: bool = True,
    page_size: int = 50,
    max_pages: int = 20,
) -> dict[str, list[dict]]:
    """
    Fetch ALL sports from ONE bookmaker using paginated requests.

    The Get1x2_VZip API ignores sport filters and returns top-N matches
    sorted by popularity. To get all sports we paginate with start=N until
    the response returns fewer than page_size results.

    Returns {sport_slug: [matches]}.
    """
    sports = sports or ALL_SPORT_SLUGS
    slug   = bk["slug"]
    ref    = _referer(bk)

    if verbose:
        print(f"\n  [{slug}] paginating all sports ({page_size}/page, max {max_pages} pages)…")

    # ── Paginate until exhausted ───────────────────────────────────────────────
    all_games: list[dict] = []
    t0 = time.perf_counter()

    for page in range(max_pages):
        start = page * page_size
        url   = (_live_url(bk, count=page_size, start=start)
                 if mode == "live"
                 else _line_url(bk, count=page_size, start=start))
        raw   = _curl(url, ref)

        if not raw or raw.get("ErrorCode", -1) not in (0, ""):
            if page == 0 and verbose:
                print(f"  ❌ {slug} — no response on page 0")
            break

        value = raw.get("Value") or []
        page_games: list[dict] = []

        for item in value:
            if not isinstance(item, dict): continue
            if "O1E" in item or "O1" in item:
                page_games.append(item)
            elif "L" in item and isinstance(item.get("L"), list):
                for country in item["L"]:
                    for sc in (country.get("SC") or []):
                        for game in (sc.get("G") or []):
                            if isinstance(game, dict):
                                page_games.append(game)

        all_games.extend(page_games)

        if verbose:
            print(f"    page {page+1}: {len(page_games)} games (start={start})")

        # Stop if this page returned fewer than page_size — we have everything
        if len(page_games) < page_size:
            break

    ms = int((time.perf_counter() - t0) * 1000)

    # ── Deduplicate by game ID ─────────────────────────────────────────────────
    seen: set = set()
    unique: list[dict] = []
    for g in all_games:
        gid = g.get("I") or g.get("GameId")
        if gid and gid in seen: continue
        if gid: seen.add(gid)
        unique.append(g)

    # ── Split into per-sport results ───────────────────────────────────────────
    # Build {sport_id: primary_slug} for quick lookup
    id_to_primary: dict[int, str] = {}
    for sp in sports:
        sid = B2B_SPORT_IDS.get(sp.lower())
        if sid and sid not in id_to_primary:
            id_to_primary[sid] = sp

    games_by_sport: dict[int, list[dict]] = defaultdict(list)
    for game in unique:
        si = game.get("SI")
        if si is not None:
            games_by_sport[int(si)].append(game)

    results: dict[str, list[dict]] = {s: [] for s in sports}
    sport_totals: dict[str, int]   = {}

    for sport_slug in sports:
        sid = B2B_SPORT_IDS.get(sport_slug.lower())
        if sid is None:
            continue
        raw_games = games_by_sport.get(sid, [])
        matches: list[dict] = []
        for game in raw_games:
            m = _parse_game(game, bk, sport_slug, mode)
            if m:
                matches.append(m)
        results[sport_slug] = matches
        if matches:
            sport_totals[sport_slug] = len(matches)

    if verbose:
        for sp, cnt in sorted(sport_totals.items(), key=lambda x: -x[1]):
            print(f"    {sp:<18} {cnt:4} matches")

    total = sum(len(v) for v in results.values())
    if verbose:
        print(f"  → {slug}: {total} matches from {len(unique)} games ({ms}ms)")

    return results


def harvest_all_b2b(
    mode:        str = "upcoming",
    sports:      list[str] | None = None,
    bookmakers:  list[dict] | None = None,
    bk_workers:  int = 7,
    verbose:     bool = True,
) -> dict[str, dict[str, list[dict]]]:
    """
    Fetch ALL bookmakers × ALL sports concurrently.

    Returns nested dict:
      { bk_slug: { sport_slug: [match, ...] } }

    Architecture:
      • One thread per bookmaker (up to bk_workers)
      • Each thread does ONE HTTP request to get all sports at once
        (client-side split by SI field — much faster than N×S requests)
    """
    bks    = bookmakers or B2B_BOOKMAKERS
    sports = sports or ALL_SPORT_SLUGS

    if verbose:
        print(f"\n{'═'*65}")
        print(f"B2B Harvest: {len(bks)} bookmakers × {len(sports)} sports [{mode}]")
        print(f"{'═'*65}")

    results: dict[str, dict[str, list[dict]]] = {}

    with ThreadPoolExecutor(max_workers=min(bk_workers, len(bks)),
                             thread_name_prefix="b2b") as pool:
        futures = {
            pool.submit(fetch_bk_all_sports, bk, mode, sports, verbose=verbose): bk
            for bk in bks
        }
        for fut in as_completed(futures):
            bk = futures[fut]
            try:
                results[bk["slug"]] = fut.result()
            except Exception as exc:
                logger.error("[b2b:%s] unhandled: %s", bk["slug"], exc)
                results[bk["slug"]] = {s: [] for s in sports}

    return results


# =============================================================================
# MERGE ACROSS BOOKMAKERS (by home|away|kickoff)
# =============================================================================

def merge_b2b(
    all_results: dict[str, dict[str, list[dict]]],
    sport_slug:  str,
) -> list[dict]:
    """
    Merge the same sport's matches across all bookmakers.
    Dedup by home|away|start_hour key.
    """
    unified: list[dict]       = []
    key_idx: dict[str, int]   = {}

    for bk_slug, sport_data in all_results.items():
        for m in sport_data.get(sport_slug, []):
            home  = m.get("home_team", "").lower().strip()
            away  = m.get("away_team", "").lower().strip()
            start = (m.get("start_time") or "")[:16]
            key   = f"{home}|||{away}|||{start}"

            if key in key_idx:
                existing = unified[key_idx[key]]
                bk_info  = m["bookmakers"].get(bk_slug) or {}
                if bk_info.get("markets"):
                    existing["bookmakers"][bk_slug] = bk_info
                    for mkt, outs in bk_info["markets"].items():
                        em = existing["markets"].setdefault(mkt, {})
                        for out, price in outs.items():
                            if price > em.get(out, 0.0):
                                em[out] = price
                    existing["market_count"] = len(existing["markets"])
                    existing["bk_count"]     = len(existing["bookmakers"])
            else:
                entry = {
                    **m,
                    "bk_count": 1,
                    "bookmakers": dict(m.get("bookmakers") or {}),
                    "markets":    dict(m.get("markets") or {}),
                }
                key_idx[key] = len(unified)
                unified.append(entry)

    return unified


# =============================================================================
# SPORTS TREE (GetSportsShortZip — for browsing competitions)
# =============================================================================

def fetch_sports_tree(bk: dict | None = None, verbose: bool = True) -> dict:
    """
    Fetch the sports/competition tree from any BK.
    All BKs use the same sport IDs so one request is enough.

    Returns {sport_name: {competition_name: [game_count]}}
    """
    bk  = bk or B2B_BOOKMAKERS[0]   # default to 1xBet
    url = _sports_url(bk)
    ref = _referer(bk)

    raw = _curl(url, ref)
    if not raw:
        return {}

    tree: dict[str, dict] = {}
    for sport in raw.get("Value") or []:
        if not isinstance(sport, dict):
            continue
        sport_name = sport.get("SE") or sport.get("SN") or f"sport_{sport.get('I')}"
        sport_id   = sport.get("I")
        tree[sport_name] = {"id": sport_id, "competitions": {}}

        for country in sport.get("L") or []:
            for sc in country.get("SC") or []:
                comp = sc.get("LE") or sc.get("L") or "Unknown"
                count = len(sc.get("G") or [])
                tree[sport_name]["competitions"][comp] = count

    if verbose:
        for sport_name, data in sorted(tree.items()):
            total = sum(data["competitions"].values())
            print(f"  {sport_name:<20} (id={data['id']}) — {total} matches")
            for comp, cnt in sorted(data["competitions"].items(), key=lambda x: -x[1])[:5]:
                print(f"      {comp:<40} {cnt:>4}")

    return tree


# =============================================================================
# SAMPLE PRINTER — one match per sport showing all market data
# =============================================================================

def print_sample_per_sport(
    all_results: dict[str, dict[str, list[dict]]],
    sport_filter: str | None = None,
) -> None:
    """
    Print one sample match per sport across all bookmakers.
    Use this to understand available markets and design the mapping table.
    """
    print(f"\n{'═'*70}")
    print("SAMPLE MATCHES — one per sport (all available markets shown)")
    print(f"{'═'*70}\n")

    # Merge across BKs first so we see all markets in one place
    for sport_slug in ALL_SPORT_SLUGS:
        if sport_filter and sport_slug != sport_filter:
            continue

        merged = merge_b2b(all_results, sport_slug)
        if not merged:
            print(f"  {sport_slug:<18} — no matches found")
            continue

        # Pick the match with the most markets
        best = max(merged, key=lambda m: m.get("market_count", 0))
        bk_list = list(best.get("bookmakers", {}).keys())

        print(f"{'─'*70}")
        print(f"  SPORT: {sport_slug.upper()}")
        print(f"  MATCH: {best['home_team']} vs {best['away_team']}")
        print(f"  COMP:  {best.get('competition', '?')}")
        print(f"  START: {best.get('start_time', '?')}")
        print(f"  BKS:   {', '.join(bk_list)}")
        print(f"  MARKETS ({best.get('market_count', 0)}):")

        for mkt_slug, outcomes in sorted(best.get("markets", {}).items()):
            outcomes_str = "  ".join(
                f"{out}={price:.2f}" for out, price in sorted(outcomes.items())
            )
            print(f"    {mkt_slug:<28} {outcomes_str}")

        print()


def print_raw_sample(bk: dict, sport_slug: str = "soccer", mode: str = "upcoming") -> None:
    """
    Print raw API response for ONE bookmaker/sport — useful for
    discovering new market group IDs (G values) in the E array.
    """
    url = _live_url(bk) if mode == "live" else _line_url(bk)
    ref = _referer(bk)

    print(f"\nFetching raw: {bk['slug']} / {sport_slug} / {mode}")
    print(f"URL: {url}\n")

    raw = _curl(url, ref)
    if not raw:
        print("❌ No response"); return

    value = raw.get("Value") or []
    sport_id = B2B_SPORT_IDS.get(sport_slug)

    # Find first game matching sport
    sample_game = None
    for item in value:
        if not isinstance(item, dict): continue
        if "O1E" in item or "O1" in item:
            if item.get("SI") == sport_id or sport_id is None:
                sample_game = item; break
        elif "L" in item and item.get("I") == sport_id:
            for country in item["L"]:
                for sc in country.get("SC") or []:
                    games = sc.get("G") or []
                    if games:
                        sample_game = games[0]; break

    if not sample_game:
        print(f"⚠ No {sport_slug} game in response"); return

    print(f"Match: {sample_game.get('O1E')} vs {sample_game.get('O2E')}")
    print(f"Competition: {sample_game.get('LE')}")
    print(f"Start: {sample_game.get('S')}")
    print(f"\nRaw E[] events ({len(sample_game.get('E', []))} entries):")

    # Group events by G
    by_group: dict[int, list] = defaultdict(list)
    for ev in sample_game.get("E") or []:
        by_group[ev.get("G", 0)].append(ev)

    for gid, evs in sorted(by_group.items()):
        slug = _GROUP_TO_SLUG.get(gid, f"group_{gid} (UNMAPPED)")
        print(f"\n  G={gid}  →  {slug}")
        for ev in evs:
            t, c, p = ev.get("T"), ev.get("C"), ev.get("P")
            label = _T_LABELS.get(gid, {}).get(t, f"T{t}")
            line  = f"@{p}" if p is not None else ""
            print(f"    T={t:<5} {label:<15} C={c}  {line}")


# =============================================================================
# FLASK CLI COMMANDS
# =============================================================================


# ─── Public API aliases (used by CLI, Celery tasks, tasks_harvest_b2b.py) ────
B2B_SUPPORTED_SPORTS: list[str] = ALL_SPORT_SLUGS  # canonical export name

def fetch_single_bk(
    bk: dict,
    sport_slug: str,
    mode: str = "upcoming",
    page: int = 1,
    page_size: int = 200,
    output_dir: str = "",
    verbose: bool = True,
) -> list[dict]:
    """
    Alias for fetch_bk_sport — matches the signature used by
    tasks_harvest_b2b.py and the harvest-b2b-all CLI command.

    Note: page/page_size are accepted for API compat but the BetB2B
    endpoint returns all results in a single call (no server-side pagination).
    The page/page_size are used to slice the returned list.
    """
    matches = fetch_bk_sport(bk, sport_slug, mode, verbose=verbose)
    start   = (page - 1) * page_size
    sliced  = matches[start : start + page_size]
    if output_dir and matches:
        _save_bk_file(output_dir, bk["slug"], sport_slug, mode, matches)
    return sliced


def merge_b2b_by_match(
    per_bk: dict[str, list[dict]],
    sport_slug: str,
) -> list[dict]:
    """
    Alias for merge_b2b — matches the name used by tasks_harvest_b2b.py
    and the harvest-b2b-all CLI command.
    """
    return merge_b2b(per_bk, sport_slug)


def get_bk_by_slug(slug: str) -> dict | None:
    """Return bookmaker config dict by slug."""
    return _BK_BY_SLUG.get(slug)


def _save_bk_file(output_dir: str, bk_slug: str, sport: str, mode: str, matches: list) -> None:
    """Save raw matches to a JSON file in output_dir."""
    import os
    from datetime import datetime
    os.makedirs(output_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"b2b_{bk_slug}_{sport}_{mode}_{ts}.json")
    with open(path, "w") as f:
        json.dump(matches, f, indent=2, default=str)

def register_cli(flask_app) -> None:
    """Register all B2B CLI commands on the Flask app."""
    import click, os, json, traceback
    from datetime import datetime

    @flask_app.cli.command("harvest-b2b")
    @click.option("--mode",   default="upcoming", type=click.Choice(["upcoming", "live"]))
    @click.option("--sport",  default=None,  help="Limit to one sport slug")
    @click.option("--bk",     default=None,  help="Limit to one bookmaker slug")
    @click.option("--sample", is_flag=True,  help="Print one sample match per sport")
    @click.option("--raw",    is_flag=True,  help="Print raw E[] events for one game")
    @click.option("--sports-tree", is_flag=True, help="Print sports/competitions tree")
    @click.option("--save",   is_flag=True,  help="Save to Redis after harvest")
    @click.option("--output-dir", default="harvest_dumps")
    def harvest_b2b_cmd(mode, sport, bk, sample, raw, sports_tree, save, output_dir):
        """Harvest all B2B bookmakers."""
        if sports_tree:
            click.echo("\n📋 Sports tree:")
            fetch_sports_tree(verbose=True); return

        bks    = [_BK_BY_SLUG[bk]] if bk else B2B_BOOKMAKERS
        sports = [sport] if sport else ALL_SPORT_SLUGS

        if raw:
            print_raw_sample(bks[0], sports[0], mode); return

        all_results = harvest_all_b2b(mode=mode, sports=sports, bookmakers=bks, verbose=True)

        if sample:
            print_sample_per_sport(all_results, sport_filter=sport); return

        if save:
            _save_results_to_redis(all_results, mode)

        os.makedirs(output_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        for bk_slug, sport_data in all_results.items():
            for sp, ms in sport_data.items():
                if ms:
                    path = os.path.join(output_dir, f"b2b_{bk_slug}_{sp}_{mode}_{ts}.json")
                    with open(path, "w") as f:
                        json.dump(ms, f, indent=2, default=str)
        click.echo(f"\n✅ Files saved to {output_dir}/")

    @flask_app.cli.command("harvest-b2b-all")
    @click.option("--output-dir", default="harvest_dumps")
    @click.option("--sport",  default=None)
    @click.option("--debug",  is_flag=True)
    def harvest_b2b_all(output_dir, sport, debug):
        """Fetch and parse odds from all B2B bookmakers. Saves to files + Redis."""
        import logging
        if debug:
            logging.getLogger("app.workers.b2b_harvester").setLevel(logging.DEBUG)

        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sports    = [sport] if sport else B2B_SUPPORTED_SPORTS
        errors    = {}

        click.echo(f"\n🚀 Harvesting B2B bookmakers for {len(sports)} sport(s)...")

        for s in sports:
            click.echo(f"\n{'─'*60}")
            click.echo(f"Sport: {s.upper()}")
            per_bk: dict[str, list[dict]] = {}

            for bk in B2B_BOOKMAKERS:
                try:
                    matches = fetch_single_bk(
                        bk, s, mode="upcoming", page=1,
                        page_size=500, output_dir=output_dir, verbose=True,
                    )
                    per_bk[bk["slug"]] = matches

                    # Per-BK file (raw matches before merge)
                    out_file = os.path.join(output_dir, f"b2b_{bk['slug']}_{s}_{timestamp}.json")
                    with open(out_file, "w") as f:
                        json.dump(matches, f, indent=2, default=str)

                except Exception as e:
                    traceback.print_exc()
                    errors[f"{bk['slug']}/{s}"] = str(e)
                    per_bk[bk["slug"]] = []

            # Merge across all BKs for this sport
            merged = merge_b2b_by_match(per_bk, s)
            out_unified = os.path.join(output_dir, f"b2b_unified_{s}_{timestamp}.json")
            with open(out_unified, "w") as f:
                json.dump(merged, f, indent=2, default=str)
            click.echo(f"\n  🔗 Unified {s}: {len(merged)} matches → {out_unified}")

            # Save to Redis so odds_stream can serve it immediately
            if merged:
                _save_sport_to_redis(s, "upcoming", merged)
                # Also save per-BK slices for individual BK key lookup
                for bk_slug, bk_matches in per_bk.items():
                    if bk_matches:
                        _save_sport_to_redis(s, "upcoming", bk_matches, bk_slug=bk_slug)

        click.echo(f"\n✅ Done. Files saved to: {output_dir}/")
        if errors:
            click.echo(f"\n⚠️  {len(errors)} error(s):")
            for key, err in errors.items():
                click.echo(f"   {key}: {err}")

    @flask_app.cli.command("b2b-sample")
    @click.option("--sport", default="soccer")
    @click.option("--mode",  default="upcoming")
    @click.option("--bk",    default=None)
    def b2b_sample_cmd(sport, mode, bk):
        """Print all E[] events for one match — use to build market mappings."""
        bk_obj = _BK_BY_SLUG.get(bk) if bk else B2B_BOOKMAKERS[0]
        print_raw_sample(bk_obj, sport, mode)

    @flask_app.cli.command("b2b-sports-tree")
    @click.option("--bk", default="paripesa")
    def b2b_sports_tree_cmd(bk):
        """Print available sports and competitions."""
        bk_obj = _BK_BY_SLUG.get(bk, B2B_BOOKMAKERS[-1])
        fetch_sports_tree(bk_obj, verbose=True)


def _save_sport_to_redis(sport: str, mode: str, matches: list, bk_slug: str = "b2b") -> None:
    """Save matches to Redis using redis_bus.publish_snapshot."""
    try:
        from app.workers.redis_bus import publish_snapshot
        publish_snapshot(bk_slug, mode, sport, matches)
        logger.info("[b2b] saved %d matches to Redis: %s/%s/%s", len(matches), bk_slug, mode, sport)
    except Exception as exc:
        logger.warning("[b2b] Redis save failed %s/%s/%s: %s", bk_slug, mode, sport, exc)


def _save_results_to_redis(all_results: dict, mode: str = "upcoming") -> None:
    """Save all harvest results to Redis."""
    for bk_slug, sport_data in all_results.items():
        for sport, matches in sport_data.items():
            if matches:
                _save_sport_to_redis(sport, mode, matches, bk_slug=bk_slug)
        # Also save merged view per sport
        all_sports = set(all_results[bk_slug].keys())
    for sport in all_sports:
        per_bk = {bk: data.get(sport, []) for bk, data in all_results.items()}
        merged = merge_b2b(per_bk, sport)
        if merged:
            _save_sport_to_redis(sport, mode, merged, bk_slug="b2b")




if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.WARNING)

    sport = sys.argv[1] if len(sys.argv) > 1 else "soccer"
    mode  = sys.argv[2] if len(sys.argv) > 2 else "upcoming"
    cmd   = sys.argv[3] if len(sys.argv) > 3 else "sample"

    print(f"\n🚀 B2B Harvester — sport={sport} mode={mode} cmd={cmd}")

    if cmd == "raw":
        bk_slug = sys.argv[4] if len(sys.argv) > 4 else "paripesa"
        print_raw_sample(_BK_BY_SLUG[bk_slug], sport, mode)

    elif cmd == "tree":
        fetch_sports_tree(_BK_BY_SLUG.get(sys.argv[4], B2B_BOOKMAKERS[-1]), verbose=True)

    else:  # default: sample
        all_results = harvest_all_b2b(mode=mode, sports=[sport])
        print_sample_per_sport(all_results, sport_filter=sport)