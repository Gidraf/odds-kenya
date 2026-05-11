"""
app/workers/b2b_playwright.py
==============================
Playwright-based B2B harvester.

Uses a real Chromium browser to intercept LineFeed/LiveFeed JSON responses.
The browser handles x-hd HMAC tokens and SESSION cookies automatically.

INSTALL
--------
  pip install playwright --break-system-packages
  playwright install chromium

FIRST-TIME SESSION SETUP (run once per bookmaker)
---------------------------------------------------
  flask b2b-pw-setup --bk 1xbet      # opens browser, log in, saves cookies
  flask b2b-pw-setup --bk 22bet
  ... repeat for each BK

CLI COMMANDS
------------
  flask harvest-b2b                   # all BKs x all sports
  flask harvest-b2b --sport soccer    # one sport
  flask harvest-b2b --bk 1xbet       # one BK
  flask harvest-b2b --live            # live matches
  flask harvest-b2b --sample          # print sample match per sport
  flask harvest-b2b --sports-tree     # print sport list
  flask harvest-b2b --save            # push to Redis after harvest
  flask harvest-b2b-all               # per-sport files + unified + Redis
  flask b2b-sample --sport tennis     # raw E[] events for mapper building
  flask b2b-sports-tree               # sport list with match counts
  flask b2b-pw-setup --bk 1xbet      # first-time browser login
  flask b2b-pw-test --bk 1xbet       # test one BK/sport
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# =============================================================================
# BOOKMAKER REGISTRY
# =============================================================================

B2B_BOOKMAKERS: list[dict] = [
    {"slug":"1xbet",    "name":"1xBet",    "domain":"1xbet.co.ke",   "base":"https://1xbet.co.ke",   "partner_id":61,  "gr":657},
    {"slug":"22bet",    "name":"22Bet",    "domain":"22bet.co.ke",   "base":"https://22bet.co.ke",   "partner_id":151, "gr":515},
    {"slug":"betwinner","name":"Betwinner","domain":"betwinner.ke",  "base":"https://betwinner.ke",  "partner_id":152, "gr":None},
    {"slug":"melbet",   "name":"Melbet",   "domain":"mel-bet.co.ke", "base":"https://mel-bet.co.ke", "partner_id":417, "gr":None},
    {"slug":"megapari", "name":"Megapari", "domain":"1849932mp.pro", "base":"https://1849932mp.pro", "partner_id":192, "gr":None},
    {"slug":"helabet",  "name":"Helabet",  "domain":"helabetke.com", "base":"https://helabetke.com", "partner_id":237, "gr":None},
    {"slug":"paripesa", "name":"Paripesa", "domain":"paripesa.cool", "base":"https://paripesa.cool", "partner_id":188, "gr":None},
]

_BK_BY_SLUG: dict[str, dict] = {b["slug"]: b for b in B2B_BOOKMAKERS}

# =============================================================================
# SPORT ID + URL SLUG MAP
# =============================================================================

B2B_SPORT_IDS: dict[str, int] = {
    "soccer":1,"football":1,
    "esoccer":40,"efootball":40,"e-football":40,"virtual-football":40,
    "basketball":3,"tennis":4,"table-tennis":10,"tabletennis":10,
    "ice-hockey":2,"icehockey":2,"volleyball":6,"handball":8,"baseball":5,
    "american-football":13,"americanfootball":13,"nfl":13,
    "rugby":7,"rugby-league":7,"rugby-union":7,
    "boxing":9,"mma":56,"ufc":189,
    "cricket":66,"darts":21,"golf":41,"futsal":14,"snooker":30,"squash":39,
}

_ID_TO_SLUG: dict[int, str] = {
    1:"soccer",2:"ice-hockey",3:"basketball",4:"tennis",5:"baseball",
    6:"volleyball",7:"rugby",8:"handball",9:"boxing",10:"table-tennis",
    13:"american-football",14:"futsal",21:"darts",30:"snooker",
    39:"squash",40:"esoccer",41:"golf",56:"mma",66:"cricket",189:"ufc",
}

# URL slug used on the betting site's line page
SPORT_PAGE_SLUG: dict[str, str] = {
    "soccer":"football","football":"football",
    "basketball":"basketball","tennis":"tennis",
    "ice-hockey":"ice-hockey","volleyball":"volleyball",
    "cricket":"cricket","rugby":"rugby","handball":"handball",
    "table-tennis":"table-tennis","mma":"martial-arts",
    "boxing":"boxing","ufc":"ufc","darts":"darts",
    "american-football":"american-football","baseball":"baseball",
    "esoccer":"esports","efootball":"esports",
    "golf":"golf","futsal":"futsal","snooker":"snooker",
}

ALL_SPORT_SLUGS: list[str] = [
    "soccer","basketball","tennis","ice-hockey","volleyball",
    "cricket","rugby","handball","table-tennis","mma",
    "boxing","darts","american-football","baseball","esoccer",
]

B2B_SUPPORTED_SPORTS: list[str] = ALL_SPORT_SLUGS

# =============================================================================
# MARKET GROUPS
# =============================================================================

_GROUP_TO_SLUG: dict[int, str] = {
    1:"match_winner",2:"asian_handicap",8:"double_chance",
    15:"btts",17:"over_under",19:"first_half_over_under",
    62:"handicap_result",99:"asian_total",2854:"asian_handicap_2",
}

_T_LABELS: dict[int, dict[int, str]] = {
    1:{1:"1",2:"X",3:"2"},2:{7:"1",8:"2"},8:{4:"1X",5:"12",6:"X2"},
    15:{11:"Yes",12:"No"},17:{9:"Over",10:"Under"},
    19:{180:"Over",181:"Under"},62:{13:"1",14:"2"},
    99:{3827:"Over",3828:"Under"},2854:{3829:"1",3830:"2"},
}

# Profile dir — persists session cookies across runs
PROFILE_DIR = Path(os.environ.get("B2B_PROFILE_DIR", "/tmp/b2b_pw_profiles"))

# API path fragments to intercept
_API_PATTERNS = (
    "/service-api/LineFeed/Get1x2_VZip",
    "/service-api/LiveFeed/Get1x2_VZip",
    "/service-api/LineFeed/GetSportsShortZip",
)

# =============================================================================
# PARSERS
# =============================================================================

def _parse_events(events: list[dict], extra: list[dict] | None = None) -> dict:
    markets: dict[str, dict[str, float]] = defaultdict(dict)
    def _p(ev: dict):
        gid=ev.get("G"); t=ev.get("T"); c=ev.get("C") or ev.get("CV")
        if None in (gid,t,c): return
        try: price=float(c)
        except: return
        if price<=1.0: return
        label=_T_LABELS.get(gid,{}).get(t,f"T{t}")
        p=ev.get("P")
        if p is not None and gid in (2,17,19,62,99,2854): label=f"{label}@{p}"
        slug=_GROUP_TO_SLUG.get(gid,f"group_{gid}")
        if price>markets[slug].get(label,0.0): markets[slug][label]=price
    for ev in events or []:
        if isinstance(ev,dict): _p(ev)
    for ae in extra or []:
        if not isinstance(ae,dict): continue
        for me in ae.get("ME") or [ae]:
            if isinstance(me,dict): _p(me)
    return {k:v for k,v in markets.items() if v}


def _parse_game(game: dict, bk: dict, sport_slug: str, mode: str) -> dict | None:
    home=(game.get("O1E") or game.get("O1") or "").strip()
    away=(game.get("O2E") or game.get("O2") or "").strip()
    if not home or not away: return None
    try:
        from app.utils.mapping.b2b import normalize_b2b_markets
        markets=normalize_b2b_markets(sport_slug,list(game.get("E") or [])+list(game.get("AE") or []))
    except Exception:
        markets=_parse_events(game.get("E") or [],game.get("AE"))
    if not markets: return None
    game_id=game.get("I") or game.get("GameId")
    comp=(game.get("LE") or game.get("L") or "").strip()
    start_ts=game.get("S")
    start_dt=(datetime.fromtimestamp(start_ts,tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
              if start_ts else None)
    match_id=f"{bk['slug']}:{game_id}"
    return {
        "b2b_match_id":match_id,"external_id":str(game_id or ""),"betradar_id":"",
        "home_team":home,"away_team":away,"start_time":start_dt,"competition":comp,
        "sport":sport_slug,"source":bk["slug"],"is_live":mode=="live",
        "status":"live" if mode=="live" else "upcoming",
        "markets":markets,"market_count":len(markets),
        "bookmakers":{bk["slug"]:{"bookmaker":bk["name"],"slug":bk["slug"],
                                   "match_id":match_id,"markets":markets}},
        "harvested_at":datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _games_to_matches(value: list, bk: dict, sport_slug: str,
                       mode: str, sport_id: int | None) -> list[dict]:
    matches=[]; seen:set=set()
    for item in value:
        if not isinstance(item,dict): continue
        if "O1E" in item or "O1" in item:
            if sport_id and item.get("SI") not in (sport_id,None): continue
            gid=item.get("I")
            if gid and gid in seen: continue
            if gid: seen.add(gid)
            m=_parse_game(item,bk,sport_slug,mode)
            if m: matches.append(m)
        elif "L" in item and isinstance(item.get("L"),list):
            if sport_id and item.get("I") not in (sport_id,None): continue
            for country in item["L"]:
                for sc in country.get("SC") or []:
                    for game in sc.get("G") or []:
                        if not isinstance(game,dict): continue
                        gid=game.get("I")
                        if gid and gid in seen: continue
                        if gid: seen.add(gid)
                        m=_parse_game(game,bk,sport_slug,mode)
                        if m: matches.append(m)
    return matches

# =============================================================================
# PLAYWRIGHT CORE — async intercept
# =============================================================================

async def _pw_intercept(
    bk: dict,
    sport_slug: str,
    mode: str = "upcoming",
    headless: bool = True,
    wait_s: int = 30,
) -> list[dict]:
    """Navigate to sport page, intercept JSON, return parsed matches."""
    from playwright.async_api import async_playwright

    page_slug = SPORT_PAGE_SLUG.get(sport_slug, sport_slug)
    feed      = "live" if mode == "live" else "line"
    page_url  = f"{bk['base']}/en/{feed}/{page_slug}"
    profile   = PROFILE_DIR / bk["slug"]
    profile.mkdir(parents=True, exist_ok=True)

    sport_id  = B2B_SPORT_IDS.get(sport_slug.lower())
    captured: list[dict] = []

    async with async_playwright() as pw:
        ctx = await pw.chromium.launch_persistent_context(
            str(profile),
            headless=headless,
            args=["--no-sandbox","--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"],
            ignore_https_errors=True,
            viewport={"width":1366,"height":768},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()

        async def _on_response(resp):
            if not any(p in resp.url for p in _API_PATTERNS): return
            try:
                if resp.status == 200:
                    body = await resp.json()
                    if body and body.get("ErrorCode") in (0,""):
                        captured.append(body)
            except Exception: pass

        page.on("response", _on_response)

        print(f"  [{bk['slug']}] → {page_url}")
        try:
            await page.goto(page_url, wait_until="domcontentloaded",
                            timeout=wait_s * 1000)
        except Exception as exc:
            logger.warning("[pw:%s] goto error: %s", bk["slug"], exc)
            await ctx.close()
            return []

        # Wait for API responses
        deadline = time.perf_counter() + wait_s
        while time.perf_counter() < deadline:
            await asyncio.sleep(1)
            if captured:
                await asyncio.sleep(3)
                break

        await ctx.close()

    # Parse all captured payloads
    matches: list[dict] = []
    seen: set = set()
    for payload in captured:
        for m in _games_to_matches(payload.get("Value") or [], bk, sport_slug, mode, sport_id):
            gid = m.get("external_id")
            if gid and gid in seen: continue
            if gid: seen.add(gid)
            matches.append(m)

    return matches


async def _pw_intercept_many(
    bks: list[dict],
    sport_slug: str,
    mode: str = "upcoming",
    headless: bool = True,
    wait_s: int = 30,
) -> dict[str, list[dict]]:
    """
    Open one browser tab per bookmaker CONCURRENTLY in a single event loop.
    All BK pages load in parallel — much faster than sequential.
    """
    from playwright.async_api import async_playwright

    sport_id   = B2B_SPORT_IDS.get(sport_slug.lower())
    page_slug  = SPORT_PAGE_SLUG.get(sport_slug, sport_slug)
    feed       = "live" if mode == "live" else "line"
    results:   dict[str, list[dict]] = {}
    captured:  dict[str, list[dict]] = {bk["slug"]: [] for bk in bks}

    async with async_playwright() as pw:
        # Launch one shared browser — tabs are lightweight
        browser = await pw.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"],
        )

        async def _fetch_one(bk: dict) -> tuple[str, list[dict]]:
            slug     = bk["slug"]
            page_url = f"{bk['base']}/en/{feed}/{page_slug}"
            profile  = PROFILE_DIR / slug
            profile.mkdir(parents=True, exist_ok=True)

            # Load saved cookies from persistent profile if they exist
            storage_file = profile / "storage.json"
            ctx_opts: dict = {
                "viewport": {"width": 1366, "height": 768},
                "user_agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/148.0.0.0 Safari/537.36"
                ),
                "ignore_https_errors": True,
            }
            if storage_file.exists():
                try:
                    ctx_opts["storage_state"] = str(storage_file)
                except Exception:
                    pass

            ctx  = await browser.new_context(**ctx_opts)
            page = await ctx.new_page()
            bk_captured: list[dict] = []

            async def _on_response(resp):
                if not any(p in resp.url for p in _API_PATTERNS): return
                try:
                    if resp.status == 200:
                        body = await resp.json()
                        if body and body.get("ErrorCode") in (0, ""):
                            bk_captured.append(body)
                            logger.debug("[pw:%s] %d items from %s",
                                         slug, len(body.get("Value") or []), resp.url[:80])
                except Exception: pass

            page.on("response", _on_response)
            print(f"  [{slug}] → {page_url}")

            try:
                await page.goto(page_url, wait_until="domcontentloaded",
                                timeout=wait_s * 1000)
            except Exception as exc:
                logger.warning("[pw:%s] goto: %s", slug, exc)
                await ctx.close()
                return slug, []

            # Wait for API data — up to wait_s seconds
            deadline = time.perf_counter() + wait_s
            while time.perf_counter() < deadline:
                await asyncio.sleep(1)
                if bk_captured:
                    await asyncio.sleep(2)   # let more requests finish
                    break

            # Save updated cookies back to storage file
            try:
                state = await ctx.storage_state()
                with open(storage_file, "w") as f:
                    json.dump(state, f)
            except Exception: pass

            await ctx.close()

            # Parse captured JSON
            matches: list[dict] = []
            seen:    set        = set()
            for payload in bk_captured:
                for m in _games_to_matches(
                    payload.get("Value") or [], bk, sport_slug, mode, sport_id
                ):
                    gid = m.get("external_id")
                    if gid and gid in seen: continue
                    if gid: seen.add(gid)
                    matches.append(m)

            return slug, matches

        # Run all BKs concurrently as async tasks
        tasks    = [asyncio.create_task(_fetch_one(bk)) for bk in bks]
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()

    for outcome in outcomes:
        if isinstance(outcome, Exception):
            logger.error("[pw] task error: %s", outcome)
            continue
        slug, matches = outcome
        results[slug] = matches

    return results


# =============================================================================
# PUBLIC HARVEST FUNCTIONS
# =============================================================================

def fetch_bk_sport(bk: dict, sport_slug: str, mode: str = "upcoming",
                   headless: bool = True, wait_s: int = 30,
                   verbose: bool = True) -> list[dict]:
    """Fetch one sport from one bookmaker via Playwright."""
    t0      = time.perf_counter()
    result  = asyncio.run(_pw_intercept_many([bk], sport_slug, mode, headless, wait_s))
    matches = result.get(bk["slug"], [])
    ms      = int((time.perf_counter() - t0) * 1000)
    if verbose:
        status = "✅" if matches else "⚠ "
        print(f"  {status} {bk['slug']:<12} {sport_slug:<16} {mode:<9} — {len(matches):4} matches ({ms}ms)")
    return matches


def fetch_sport_all_bks(
    sport_slug: str,
    mode: str = "upcoming",
    bookmakers: list[dict] | None = None,
    headless: bool = True,
    wait_s: int = 30,
    verbose: bool = True,
) -> dict[str, list[dict]]:
    """
    Fetch ONE sport from ALL bookmakers concurrently.
    Opens one browser tab per BK in parallel — much faster than sequential.
    Returns {bk_slug: [matches]}.
    """
    bks = bookmakers or B2B_BOOKMAKERS
    t0  = time.perf_counter()
    if verbose:
        print(f"\n  Fetching {sport_slug}/{mode} from {len(bks)} BKs concurrently…")
    result = asyncio.run(_pw_intercept_many(bks, sport_slug, mode, headless, wait_s))
    ms     = int((time.perf_counter() - t0) * 1000)
    if verbose:
        for slug, matches in sorted(result.items()):
            status = "✅" if matches else "⚠ "
            print(f"  {status} {slug:<12} {sport_slug:<16} {mode:<9} — {len(matches):4} matches")
        total = sum(len(v) for v in result.values())
        print(f"  → {total} total matches across all BKs ({ms}ms)")
    return result


def fetch_bk_all_sports(bk: dict, mode: str = "upcoming",
                         sports: list[str] | None = None,
                         headless: bool = True, wait_s: int = 30,
                         verbose: bool = True) -> dict[str, list[dict]]:
    """Fetch all sports from ONE bookmaker (one concurrent batch per sport)."""
    sports  = sports or ALL_SPORT_SLUGS
    results = {}
    for sp in sports:
        per_bk    = asyncio.run(_pw_intercept_many([bk], sp, mode, headless, wait_s))
        results[sp] = per_bk.get(bk["slug"], [])
    return results


def harvest_all_b2b(
    mode:       str = "upcoming",
    sports:     list[str] | None = None,
    bookmakers: list[dict] | None = None,
    bk_workers: int = 7,          # all 7 BKs at once by default
    headless:   bool = True,
    wait_s:     int = 30,
    verbose:    bool = True,
) -> dict[str, dict[str, list[dict]]]:
    """
    Harvest ALL bookmakers × ALL sports.

    For each sport: opens one browser tab per bookmaker concurrently.
    All 7 BKs load in parallel per sport — total time ≈ N_sports × wait_s.
    """
    bks    = bookmakers or B2B_BOOKMAKERS
    sports = sports or ALL_SPORT_SLUGS

    if verbose:
        print(f"\n{'═'*65}")
        print(f"B2B Playwright [{mode.upper()}]  {len(bks)} BKs × {len(sports)} sports")
        print(f"All {len(bks)} bookmakers open in parallel per sport")
        print(f"{'═'*65}")

    # results[bk_slug][sport_slug] = [matches]
    results: dict[str, dict[str, list[dict]]] = {bk["slug"]: {} for bk in bks}

    for sp in sports:
        if verbose:
            print(f"\n  ── {sp.upper()} ──")
        per_bk = asyncio.run(
            _pw_intercept_many(bks, sp, mode, headless, wait_s)
        )
        for bk in bks:
            results[bk["slug"]][sp] = per_bk.get(bk["slug"], [])
        if verbose:
            for slug, matches in sorted(per_bk.items()):
                status = "✅" if matches else "⚠ "
                print(f"    {status} {slug:<12} {len(matches):4} matches")

    return results


# =============================================================================
# MERGE
# =============================================================================

def merge_b2b(all_results: dict[str, dict[str, list[dict]]], sport_slug: str) -> list[dict]:
    unified: list[dict] = []; key_idx: dict[str, int] = {}
    for bk_slug, sport_data in all_results.items():
        for m in sport_data.get(sport_slug, []):
            home=(m.get("home_team","")).lower().strip()
            away=(m.get("away_team","")).lower().strip()
            start=(m.get("start_time") or "")[:16]
            key=f"{home}|||{away}|||{start}"
            if key in key_idx:
                ex=unified[key_idx[key]]
                bi=(m.get("bookmakers") or {}).get(bk_slug) or {}
                if bi.get("markets"):
                    ex["bookmakers"][bk_slug]=bi
                    for mkt,outs in bi["markets"].items():
                        em=ex["markets"].setdefault(mkt,{})
                        for out,price in outs.items():
                            if price>em.get(out,0.0): em[out]=price
                    ex["market_count"]=len(ex["markets"])
            else:
                entry={**m,"bk_count":1,
                       "bookmakers":dict(m.get("bookmakers") or {}),
                       "markets":dict(m.get("markets") or {})}
                key_idx[key]=len(unified); unified.append(entry)
    return unified


def merge_b2b_by_match(per_bk: dict[str, list[dict]], sport_slug: str) -> list[dict]:
    return merge_b2b({bk:{sport_slug:ms} for bk,ms in per_bk.items()}, sport_slug)

# =============================================================================
# SAMPLE PRINTERS (for market mapper building)
# =============================================================================

def print_raw_sample(bk: dict, sport_slug: str = "soccer", mode: str = "upcoming") -> None:
    """Fetch one match and print all raw E[] G/T values — for building mappers."""
    print(f"\n🎭 Fetching raw sample: {bk['slug']} / {sport_slug}")
    matches = fetch_bk_sport(bk, sport_slug, mode, headless=True, wait_s=30, verbose=False)
    if not matches:
        print("⚠  No matches found"); return
    m = matches[0]
    print(f"Match: {m['home_team']} vs {m['away_team']}")
    print(f"Comp:  {m.get('competition')}")
    print(f"Markets ({m['market_count']}):")
    for mkt, outcomes in sorted(m.get("markets", {}).items()):
        out_str = "  ".join(f"{o}={p:.2f}" for o,p in sorted(outcomes.items()))
        print(f"  {mkt:<28} {out_str}")


def print_sample_per_sport(all_results: dict, sport_filter: str | None = None) -> None:
    """Print one sample match per sport with all markets."""
    print(f"\n{'═'*70}\nSAMPLE MATCHES\n{'═'*70}\n")
    for sport_slug in ALL_SPORT_SLUGS:
        if sport_filter and sport_slug != sport_filter: continue
        merged = merge_b2b(all_results, sport_slug)
        if not merged: print(f"  {sport_slug:<18} — no matches"); continue
        best = max(merged, key=lambda m: m.get("market_count", 0))
        print(f"{'─'*70}")
        print(f"  {sport_slug.upper()}  —  {best['home_team']} vs {best['away_team']}")
        print(f"  {best.get('competition','?')}  | {best.get('start_time','?')}")
        print(f"  BKs: {', '.join(best.get('bookmakers',{}).keys())}")
        for mkt, outcomes in sorted(best.get("markets", {}).items()):
            row = "  ".join(f"{o}={p:.2f}" for o,p in sorted(outcomes.items()))
            print(f"    {mkt:<28} {row}")
        print()


def fetch_sports_tree(bk: dict | None = None, verbose: bool = True) -> dict:
    """Navigate to /en/line, intercept GetSportsShortZip response."""
    bk = bk or B2B_BOOKMAKERS[0]

    async def _get_tree():
        from playwright.async_api import async_playwright
        profile = PROFILE_DIR / bk["slug"]
        profile.mkdir(parents=True, exist_ok=True)
        tree_data: list[dict] = []
        async with async_playwright() as pw:
            ctx = await pw.chromium.launch_persistent_context(
                str(profile), headless=True,
                args=["--no-sandbox"], ignore_https_errors=True,
                viewport={"width":1366,"height":768},
            )
            page = await ctx.new_page()
            async def _on_resp(resp):
                if "GetSportsShortZip" in resp.url and resp.status == 200:
                    try:
                        body = await resp.json()
                        if body: tree_data.append(body)
                    except Exception: pass
            page.on("response", _on_resp)
            try:
                await page.goto(f"{bk['base']}/en/line",
                                wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(5)
            except Exception: pass
            await ctx.close()
        return tree_data

    payloads = asyncio.run(_get_tree())
    tree: dict = {}
    for payload in payloads:
        for item in payload.get("Value") or []:
            if not isinstance(item, dict): continue
            name = item.get("N") or f"sport_{item.get('I')}"
            sid  = item.get("I")
            cnt  = item.get("C", 0)
            if name not in tree or tree[name]["count"] < cnt:
                tree[name] = {"id": sid, "count": cnt}
    if verbose:
        print(f"\n{'─'*50}\n{'Sport':<25}{'ID':>5}  {'Matches':>7}\n{'─'*50}")
        for name, data in sorted(tree.items(), key=lambda x: -x[1]["count"]):
            print(f"  {name:<23} {data['id']:>4}  {data['count']:>7}")
    return tree

# =============================================================================
# REDIS HELPERS
# =============================================================================

def _save_sport_to_redis(sport: str, mode: str, matches: list, bk_slug: str = "b2b") -> None:
    try:
        from app.workers.redis_bus import publish_snapshot
        publish_snapshot(bk_slug, mode, sport, matches)
        logger.info("[pw] Redis %s/%s/%s: %d", bk_slug, mode, sport, len(matches))
    except Exception as exc:
        logger.warning("[pw] Redis failed: %s", exc)


def _save_results_to_redis(all_results: dict, mode: str = "upcoming") -> None:
    sports_seen: set[str] = set()
    for bk_slug, sport_data in all_results.items():
        for sport, matches in sport_data.items():
            if matches:
                _save_sport_to_redis(sport, mode, matches, bk_slug=bk_slug)
                sports_seen.add(sport)
    for sport in sports_seen:
        per_bk = {bk: data.get(sport, []) for bk, data in all_results.items()}
        merged = merge_b2b_by_match(per_bk, sport)
        if merged: _save_sport_to_redis(sport, mode, merged, bk_slug="b2b")

# =============================================================================
# FLASK CLI — identical interface to b2b_harvester.py
# =============================================================================

def register_cli(flask_app) -> None:
    import click, traceback as _tb

    def _check_pw():
        try:
            import playwright  # noqa
            return True
        except ImportError:
            click.echo("❌ Playwright not installed. Run:")
            click.echo("   pip install playwright --break-system-packages")
            click.echo("   playwright install chromium")
            return False

    # ── harvest-b2b ──────────────────────────────────────────────────────────

    @flask_app.cli.command("harvest-b2b")
    @click.option("--mode",        default="upcoming", type=click.Choice(["upcoming","live"]))
    @click.option("--sport",       default=None,  help="Limit to one sport slug")
    @click.option("--bk",          default=None,  help="Limit to one bookmaker slug")
    @click.option("--sample",      is_flag=True,  help="Print one sample match per sport")
    @click.option("--raw",         is_flag=True,  help="Print raw events for one game")
    @click.option("--sports-tree", is_flag=True,  help="Print sports/competitions tree")
    @click.option("--save",        is_flag=True,  help="Save to Redis after harvest")
    @click.option("--output-dir",  default="harvest_dumps")
    @click.option("--headless",    default=True,  help="Run browser headless")
    @click.option("--wait",        default=30,    help="Seconds to wait per page")
    @click.option("--workers",     default=2,     help="Parallel bookmakers")
    def harvest_b2b_cmd(mode, sport, bk, sample, raw, sports_tree, save,
                         output_dir, headless, wait, workers):
        """Harvest B2B bookmakers via Playwright (handles x-hd automatically)."""
        if not _check_pw(): return

        if sports_tree:
            click.echo("\n📋 Sports tree:")
            bk_obj = _BK_BY_SLUG.get(bk) if bk else B2B_BOOKMAKERS[0]
            fetch_sports_tree(bk_obj, verbose=True)
            return

        bks    = [_BK_BY_SLUG[bk]] if bk else B2B_BOOKMAKERS
        sports = [sport] if sport else ALL_SPORT_SLUGS

        if raw:
            print_raw_sample(bks[0], sports[0], mode)
            return

        all_results = harvest_all_b2b(
            mode=mode, sports=sports, bookmakers=bks,
            bk_workers=workers, headless=headless, wait_s=wait, verbose=True,
        )

        if sample:
            print_sample_per_sport(all_results, sport_filter=sport)
            return

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

    # ── harvest-b2b-all ──────────────────────────────────────────────────────

    @flask_app.cli.command("harvest-b2b-all")
    @click.option("--output-dir", default="harvest_dumps")
    @click.option("--sport",      default=None)
    @click.option("--headless",   default=True)
    @click.option("--wait",       default=30)
    @click.option("--workers",    default=2)
    @click.option("--debug",      is_flag=True)
    def harvest_b2b_all(output_dir, sport, headless, wait, workers, debug):
        """Playwright harvest — per-sport files + unified + Redis."""
        if not _check_pw(): return
        import logging as _log
        if debug: _log.getLogger("app.workers.b2b_playwright").setLevel(_log.DEBUG)

        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sports    = [sport] if sport else B2B_SUPPORTED_SPORTS

        click.echo(f"\n🎭 B2B Playwright — {len(B2B_BOOKMAKERS)} BKs × {len(sports)} sports")

        for s in sports:
            click.echo(f"\n{'─'*60}\nSport: {s.upper()}")
            per_bk: dict[str, list[dict]] = {}

            for bk in B2B_BOOKMAKERS:
                try:
                    matches = fetch_bk_sport(bk, s, "upcoming", headless, wait, verbose=True)
                    per_bk[bk["slug"]] = matches
                    if matches:
                        out = os.path.join(output_dir, f"b2b_{bk['slug']}_{s}_{timestamp}.json")
                        with open(out, "w") as f:
                            json.dump(matches, f, indent=2, default=str)
                except Exception as e:
                    _tb.print_exc()
                    per_bk[bk["slug"]] = []

            merged = merge_b2b_by_match(per_bk, s)
            out_u  = os.path.join(output_dir, f"b2b_unified_{s}_{timestamp}.json")
            with open(out_u, "w") as f:
                json.dump(merged, f, indent=2, default=str)
            click.echo(f"\n  🔗 Unified {s}: {len(merged)} matches → {out_u}")

            if merged:
                _save_sport_to_redis(s, "upcoming", merged)
                for bk_slug, bk_ms in per_bk.items():
                    if bk_ms:
                        _save_sport_to_redis(s, "upcoming", bk_ms, bk_slug=bk_slug)

        click.echo(f"\n✅ Done. Files in: {output_dir}/")

    # ── b2b-sample ───────────────────────────────────────────────────────────

    @flask_app.cli.command("b2b-sample")
    @click.option("--sport",    default="soccer")
    @click.option("--mode",     default="upcoming")
    @click.option("--bk",       default=None)
    @click.option("--headless", default=True)
    def b2b_sample_cmd(sport, mode, bk, headless):
        """Print markets for one match — use to build sport mappers."""
        if not _check_pw(): return
        bk_obj = _BK_BY_SLUG.get(bk) if bk else B2B_BOOKMAKERS[0]
        if not bk_obj: click.echo(f"❌ Unknown: {bk}"); return
        print_raw_sample(bk_obj, sport, mode)

    # ── b2b-sports-tree ──────────────────────────────────────────────────────

    @flask_app.cli.command("b2b-sports-tree")
    @click.option("--bk", default="paripesa")
    def b2b_sports_tree_cmd(bk):
        """Print available sports and match counts."""
        if not _check_pw(): return
        bk_obj = _BK_BY_SLUG.get(bk, B2B_BOOKMAKERS[-1])
        fetch_sports_tree(bk_obj, verbose=True)

    # ── b2b-pw-setup ─────────────────────────────────────────────────────────

    @flask_app.cli.command("b2b-pw-setup")
    @click.option("--bk", default="1xbet")
    def b2b_pw_setup(bk):
        """
        Open browser so you can log in manually.
        Cookies are saved and reused by all future harvest commands.
        """
        if not _check_pw(): return
        from playwright.sync_api import sync_playwright
        bk_obj = _BK_BY_SLUG.get(bk)
        if not bk_obj: click.echo(f"❌ Unknown: {bk}"); return
        profile = PROFILE_DIR / bk
        profile.mkdir(parents=True, exist_ok=True)
        click.echo(f"\n🎭 Opening {bk_obj['base']} — log in, then press ENTER here.")
        with sync_playwright() as pw:
            # Use persistent context so all cookies/localStorage are captured
            ctx = pw.chromium.launch_persistent_context(
                str(profile), headless=False,
                args=["--no-sandbox", "--start-maximized"],
                viewport={"width": 1366, "height": 768},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/148.0.0.0 Safari/537.36"
                ),
            )
            page = ctx.new_page()
            page.goto(bk_obj["base"])
            input("  ↳ Logged in? Press ENTER to save session and close browser…")
            # Save storage state so _pw_intercept_many can reload cookies
            storage_file = profile / "storage.json"
            try:
                state = ctx.storage_state()
                with open(storage_file, "w") as f:
                    import json as _json
                    _json.dump(state, f)
                click.echo(f"  💾 Storage state saved → {storage_file}")
            except Exception as exc:
                click.echo(f"  ⚠️  Could not save storage state: {exc}")
            ctx.close()
        click.echo(f"  ✅ Session saved → {profile}")

    # ── b2b-pw-test ──────────────────────────────────────────────────────────

    @flask_app.cli.command("b2b-pw-test")
    @click.option("--bk",       default="1xbet")
    @click.option("--sport",    default="soccer")
    @click.option("--headless", default=True)
    @click.option("--wait",     default=30)
    def b2b_pw_test(bk, sport, headless, wait):
        """Quick test — one bookmaker, one sport."""
        if not _check_pw(): return
        bk_obj = _BK_BY_SLUG.get(bk)
        if not bk_obj: click.echo(f"❌ Unknown: {bk}"); return
        matches = fetch_bk_sport(bk_obj, sport, "upcoming", headless, wait, verbose=True)
        click.echo(f"\n✅ {bk}/{sport}: {len(matches)} matches")
        if matches:
            m = matches[0]
            click.echo(f"   {m['home_team']} vs {m['away_team']} | markets={m['market_count']}")


# =============================================================================
# STANDALONE TEST
# =============================================================================

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    bk_slug = sys.argv[1] if len(sys.argv) > 1 else "paripesa"
    sport   = sys.argv[2] if len(sys.argv) > 2 else "soccer"
    hl      = sys.argv[3] != "false" if len(sys.argv) > 3 else True
    bk = _BK_BY_SLUG.get(bk_slug, B2B_BOOKMAKERS[-1])
    print(f"\n🎭 {bk['slug']} / {sport} / headless={hl}")
    matches = fetch_bk_sport(bk, sport, "upcoming", hl, 35)
    print(f"✅ {len(matches)} matches")
    if matches:
        import pprint; pprint.pprint(matches[0])