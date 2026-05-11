"""
app/workers/b2b_playwright.py
==============================
Playwright-based B2B harvester — competition-by-competition approach.

DISCOVERY (from real browser DevTools, May 2026)
-------------------------------------------------
  Step 1: GetSportsShortZip?sports={sport_id}
    → Returns all competitions for a sport with LI (league ID) and GC (game count)
    → Some competitions already include G[] with match objects

  Step 2: Get1x2_VZip?sports={sport_id}&champs={LI}&count=40
    → Returns all matches for ONE competition (no pagination needed)
    → Each competition page fires this automatically when browsed

  Step 3 (full markets): GetGameZip?id={game_id}
    → Full market data for one match
    → Fired by browser when user clicks a match

APPROACH
--------
  1. Navigate to sport page (e.g. /en/line/football)
  2. Intercept GetSportsShortZip → get all LI competition IDs + GC
  3. For each competition with GC > 0:
     - Use page.evaluate(fetch(...)) to call Get1x2_VZip?champs={LI}
     - Browser generates x-hd automatically
  4. Parse all matches from all competitions
  5. Optionally enrich with GetGameZip for full markets

INSTALL
-------
  pip install playwright --break-system-packages
  playwright install chromium

FIRST-TIME SESSION
------------------
  flask b2b-pw-setup --bk betwinner
  flask b2b-pw-setup --bk 1xbet
  ... (one per BK)
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
# SPORT MAPS
# =============================================================================

B2B_SPORT_IDS: dict[str, int] = {
    "soccer":1,"football":1,
    "esoccer":40,"efootball":40,"e-football":40,
    "basketball":3,"tennis":4,"table-tennis":10,"tabletennis":10,
    "ice-hockey":2,"icehockey":2,"volleyball":6,"handball":8,"baseball":5,
    "american-football":13,"americanfootball":13,"nfl":13,
    "rugby":7,"rugby-league":7,"rugby-union":7,
    "boxing":9,"mma":56,"ufc":189,
    "cricket":66,"darts":21,"golf":41,"futsal":14,"snooker":30,
}

SPORT_PAGE_SLUG: dict[str, str] = {
    "soccer":"football","football":"football",
    "basketball":"basketball","tennis":"tennis",
    "ice-hockey":"ice-hockey","volleyball":"volleyball",
    "cricket":"cricket","rugby":"rugby","handball":"handball",
    "table-tennis":"table-tennis","mma":"martial-arts",
    "boxing":"boxing","ufc":"ufc","darts":"darts",
    "american-football":"american-football","baseball":"baseball",
    "esoccer":"esports","efootball":"esports","golf":"golf",
}

ALL_SPORT_SLUGS: list[str] = [
    "soccer","basketball","tennis","ice-hockey","volleyball",
    "cricket","rugby","handball","table-tennis","mma",
    "boxing","darts","american-football","baseball","esoccer",
]

B2B_SUPPORTED_SPORTS: list[str] = ALL_SPORT_SLUGS

# Profile dir — persists session cookies
PROFILE_DIR = Path(os.environ.get("B2B_PROFILE_DIR", "/tmp/b2b_pw_profiles"))

# =============================================================================
# MARKET PARSERS
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


def _parse_events(events: list, extra: list | None = None) -> dict:
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


def _parse_game_zip(payload: dict) -> dict:
    """Parse GetGameZip full-market response — Gn[] group structure."""
    markets: dict[str, dict[str, float]] = defaultdict(dict)
    val = payload.get("Value") or {}
    if not isinstance(val, dict): return {}
    for group in val.get("Gn") or []:
        if not isinstance(group, dict): continue
        gid  = group.get("G") or group.get("GrpId")
        slug = _GROUP_TO_SLUG.get(gid, f"group_{gid}") if gid else "unknown"
        for ev in group.get("E") or []:
            if not isinstance(ev, dict): continue
            t=ev.get("T"); c=ev.get("C") or ev.get("CV")
            if t is None or c is None: continue
            try: price=float(c)
            except: continue
            if price<=1.0: continue
            label=_T_LABELS.get(gid,{}).get(t,f"T{t}") if gid else f"T{t}"
            p=ev.get("P")
            if p is not None and gid in (2,17,19,62,99,2854): label=f"{label}@{p}"
            if price>markets[slug].get(label,0.0): markets[slug][label]=price
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


def _extract_games_from_value(value: list, bk: dict, sport_slug: str,
                               mode: str, sport_id: int | None) -> list[dict]:
    """Parse flat or nested Value[] into canonical match list."""
    matches=[]; seen:set=set()
    for item in value:
        if not isinstance(item,dict): continue
        # Flat game shape
        if "O1E" in item or "O1" in item:
            if sport_id and item.get("SI") not in (sport_id,None): continue
            gid=item.get("I")
            if gid and gid in seen: continue
            if gid: seen.add(gid)
            m=_parse_game(item,bk,sport_slug,mode)
            if m: matches.append(m)
        # Sport-wrapper with nested L > SC > G
        elif "L" in item and isinstance(item.get("L"),list):
            if sport_id and item.get("I") not in (sport_id,None): continue
            for country in item["L"]:
                if not isinstance(country,dict): continue
                for sc in country.get("SC") or []:
                    if not isinstance(sc,dict): continue
                    for game in sc.get("G") or []:
                        if not isinstance(game,dict): continue
                        gid=game.get("I")
                        if gid and gid in seen: continue
                        if gid: seen.add(gid)
                        m=_parse_game(game,bk,sport_slug,mode)
                        if m: matches.append(m)
    return matches

# =============================================================================
# COMPETITION TREE — from GetSportsShortZip
# =============================================================================

def _extract_competitions(value: list, sport_id: int) -> list[dict]:
    """
    Extract all competitions for a sport from GetSportsShortZip Value[].
    Returns [{LI, name, GC, games:[...]}] sorted by GC desc.
    """
    comps: list[dict] = []
    for item in value:
        if not isinstance(item,dict): continue
        if item.get("I") != sport_id: continue
        # Top-level SC (competitions without country grouping)
        for sc in item.get("SC") or []:
            if isinstance(sc,dict) and sc.get("LI"):
                comps.append({
                    "LI":   sc["LI"],
                    "name": sc.get("LE") or sc.get("L") or "",
                    "GC":   sc.get("GC") or 0,
                    "games": sc.get("G") or [],
                })
        # L[] = countries, each with SC[]
        for country in item.get("L") or []:
            if not isinstance(country,dict): continue
            # Direct flat competitions at country level
            if country.get("LI"):
                comps.append({
                    "LI":   country["LI"],
                    "name": country.get("L") or "",
                    "GC":   country.get("GC") or 0,
                    "games": country.get("G") or [],
                })
            # Nested SC[] under country
            for sc in country.get("SC") or []:
                if isinstance(sc,dict) and sc.get("LI"):
                    comps.append({
                        "LI":   sc["LI"],
                        "name": sc.get("LE") or sc.get("L") or "",
                        "GC":   sc.get("GC") or 0,
                        "games": sc.get("G") or [],
                    })
    # Deduplicate by LI
    seen_li: set = set()
    unique: list[dict] = []
    for c in comps:
        if c["LI"] not in seen_li:
            seen_li.add(c["LI"])
            unique.append(c)
    return sorted(unique, key=lambda x: -x["GC"])

# =============================================================================
# CORE PLAYWRIGHT — one browser, all BKs concurrent, competition-by-competition
# =============================================================================

async def _pw_harvest_sport(
    bks:          list[dict],
    sport_slug:   str,
    mode:         str  = "upcoming",
    headless:     bool = True,
    wait_s:       int  = 30,
    full_markets: bool = True,
) -> dict[str, list[dict]]:
    """
    Harvest one sport from ALL bookmakers concurrently.

    Per BK:
      1. Navigate to /en/line/{sport_page_slug}
      2. Intercept GetSportsShortZip → get all competition LI IDs + GC
      3. For each competition: page.evaluate(fetch(Get1x2_VZip?champs={LI}))
         — browser auto-adds x-hd token
      4. Parse all games from all competitions
      5. Optionally enrich with GetGameZip for full market data
    """
    from playwright.async_api import async_playwright

    sport_id   = B2B_SPORT_IDS.get(sport_slug.lower())
    page_slug  = SPORT_PAGE_SLUG.get(sport_slug, sport_slug)
    feed_path  = "live" if mode == "live" else "line"
    feed_api   = "LiveFeed" if mode == "live" else "LineFeed"
    results:   dict[str, list[dict]] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=["--no-sandbox","--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"],
        )

        async def _harvest_one_bk(bk: dict) -> tuple[str, list[dict]]:
            slug      = bk["slug"]
            p         = bk["partner_id"]
            base      = bk["base"]
            domain    = bk["domain"]
            page_url  = f"{base}/en/{feed_path}/{page_slug}"
            profile   = PROFILE_DIR / slug
            profile.mkdir(parents=True, exist_ok=True)
            storage_f = profile / "storage.json"

            ctx_opts: dict = {
                "viewport":            {"width":1366,"height":900},
                "user_agent":          (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/148.0.0.0 Safari/537.36"
                ),
                "ignore_https_errors": True,
            }
            if storage_f.exists():
                try: ctx_opts["storage_state"] = str(storage_f)
                except Exception: pass

            ctx  = await browser.new_context(**ctx_opts)
            page = await ctx.new_page()

            # ── Intercept API responses ───────────────────────────────────────
            tree_payloads:   list[dict]      = []   # GetSportsShortZip
            champ_payloads:  list[dict]      = []   # Get1x2_VZip?champs=
            detail_payloads: dict[int, dict] = {}   # GetGameZip keyed by game_id

            async def _on_response(resp):
                url = resp.url
                if resp.status != 200: return
                try:
                    if "GetSportsShortZip" in url:
                        body = await resp.json()
                        if body and body.get("ErrorCode") in (0,""):
                            tree_payloads.append(body)
                            logger.debug("[pw:%s] GetSportsShortZip: %d items",
                                         slug, len(body.get("Value") or []))
                    elif "Get1x2_VZip" in url:
                        body = await resp.json()
                        if body and body.get("ErrorCode") in (0,""):
                            champ_payloads.append(body)
                            logger.debug("[pw:%s] Get1x2_VZip: %d items url=%s",
                                         slug, len(body.get("Value") or []), url[:100])
                    elif "GetGameZip" in url:
                        body = await resp.json()
                        if body and body.get("ErrorCode") in (0,""):
                            import re as _re
                            m = _re.search(r'[?&]id=(\d+)', url)
                            if m: detail_payloads[int(m.group(1))] = body
                except Exception: pass

            page.on("response", _on_response)
            print(f"  [{slug}] → {page_url}")

            try:
                await page.goto(page_url, wait_until="domcontentloaded",
                                timeout=wait_s * 1000)
                # Extra wait — some sites fire API requests after domcontentloaded
                await asyncio.sleep(3)
            except Exception as exc:
                logger.warning("[pw:%s] goto error: %s", slug, exc)
                await ctx.close()
                return slug, []

            # ── Wait for EITHER GetSportsShortZip OR Get1x2_VZip ─────────────
            deadline = time.perf_counter() + wait_s
            while time.perf_counter() < deadline:
                await asyncio.sleep(1)
                if tree_payloads or champ_payloads: await asyncio.sleep(2); break

            # ── Extract competitions from tree (if GetSportsShortZip fired) ──
            comps: list[dict] = []
            if tree_payloads:
                for tp in tree_payloads:
                    comps.extend(_extract_competitions(tp.get("Value") or [], sport_id or 0))
                # Deduplicate by LI
                seen_li: set = set()
                unique_comps: list[dict] = []
                for c in comps:
                    if c["LI"] not in seen_li:
                        seen_li.add(c["LI"])
                        unique_comps.append(c)
                comps = unique_comps
                print(f"  [{slug}] {sport_slug}: {len(comps)} competitions "
                      f"({sum(c['GC'] for c in comps)} games)")
            else:
                # GetSportsShortZip didn't fire — request it explicitly via fetch()
                # This gives us competition LI IDs to iterate
                try:
                    gr_param = f"&gr={bk['gr']}" if bk.get("gr") else ""
                    tree_js = (
                        f'fetch("https://{domain}/service-api/LineFeed/GetSportsShortZip'
                        f'?sports={sport_id or 1}&lng=en&country=87&partner={p}'
                        f'&virtualSports=true&groupChamps=true{gr_param}",'
                        f'{{"headers":{{"accept":"application/json","is-srv":"false",'
                        f'"x-app-n":"__BETTING_APP__","x-svc-source":"__BETTING_APP__"}}}}'
                        f').then(r=>r.json()).catch(()=>null)'
                    )
                    tree_resp = await page.evaluate(tree_js)
                    if tree_resp and tree_resp.get("ErrorCode") in (0,""):
                        comps = _extract_competitions(tree_resp.get("Value") or [], sport_id or 0)
                        print(f"  [{slug}] {sport_slug}: {len(comps)} competitions "
                              f"(fetched via evaluate, {sum(c['GC'] for c in comps)} games)")
                    else:
                        print(f"  [{slug}] {sport_slug}: GetSportsShortZip returned no data")
                except Exception as exc:
                    logger.debug("[pw:%s] GetSportsShortZip evaluate: %s", slug, exc)

            # Collect games already embedded in GetSportsShortZip response
            embedded_games: list[dict] = []
            seen_embed: set = set()
            for c in comps:
                for game in c.get("games") or []:
                    if isinstance(game,dict):
                        gid = game.get("I")
                        if gid and gid not in seen_embed:
                            seen_embed.add(gid)
                            embedded_games.append(game)

            # ── Fetch each competition via browser fetch() ────────────────────
            # Group into batches of 10 to avoid Promise.all timeout
            batch_size = 10
            champ_li_list = [c["LI"] for c in comps if c["GC"] > 0]

            for i in range(0, len(champ_li_list), batch_size):
                batch = champ_li_list[i:i+batch_size]
                fetches = ",\n".join(
                    f'''fetch("https://{domain}/service-api/{feed_api}/Get1x2_VZip'''
                    f'''?sports={sport_id or 1}&champs={li}&count=40&lng=en&mode=4'''
                    f'''&country=87&partner={p}&getEmpty=true&virtualSports=true",'''
                    f'''{{"headers":{{"accept":"application/json","is-srv":"false",'''
                    f'''"x-app-n":"__BETTING_APP__","x-svc-source":"__BETTING_APP__",'''
                    f'''"content-type":"application/json"}}}})'''
                    f'''.then(r=>r.json()).catch(()=>null)'''
                    for li in batch
                )
                js = f"Promise.all([{fetches}])"
                try:
                    responses = await page.evaluate(js)
                    for resp_body in responses or []:
                        if not resp_body: continue
                        if resp_body.get("ErrorCode") not in (0,"0","",None): continue
                        champ_payloads.append(resp_body)
                except Exception as exc:
                    logger.debug("[pw:%s] champs batch %d: %s", slug, i, exc)
                await asyncio.sleep(0.3)

            # ── Parse all matches ─────────────────────────────────────────────
            matches: list[dict] = []
            seen_gids: set = set()

            # First: embedded games from GetSportsShortZip
            for game in embedded_games:
                m = _parse_game(game, bk, sport_slug, mode)
                if m:
                    gid = game.get("I")
                    if gid and gid in seen_gids: continue
                    if gid: seen_gids.add(gid)
                    matches.append(m)

            # Then: games from champs responses
            for payload in champ_payloads:
                for m in _extract_games_from_value(
                    payload.get("Value") or [], bk, sport_slug, mode, sport_id
                ):
                    gid_str = m.get("external_id","")
                    try: gid_int = int(gid_str)
                    except: gid_int = None
                    if gid_int and gid_int in seen_gids: continue
                    if gid_int: seen_gids.add(gid_int)
                    matches.append(m)

            # ── GetGameZip: full markets for each match ───────────────────────
            if full_markets and matches:
                game_ids = []
                for m in matches:
                    try: game_ids.append(int(m["external_id"]))
                    except: pass

                for i in range(0, len(game_ids), batch_size):
                    batch = game_ids[i:i+batch_size]
                    fetches = ",\n".join(
                        f'''fetch("https://{domain}/service-api/{feed_api}/GetGameZip'''
                        f'''?id={gid}&lng=en&isSubGames=true&GroupEvents=true'''
                        f'''&countevents=250&grMode=4&partner={p}&topGroups='''
                        f'''&country=87&marketType=1&isNewBuilder=true",'''
                        f'''{{"headers":{{"accept":"application/json","is-srv":"false",'''
                        f'''"x-app-n":"__BETTING_APP__","x-svc-source":"__BETTING_APP__"}}}})'''
                        f'''.then(r=>r.json()).catch(()=>null)'''
                        for gid in batch
                    )
                    try:
                        responses = await page.evaluate(f"Promise.all([{fetches}])")
                        for rb in responses or []:
                            if not rb or rb.get("ErrorCode") not in (0,"0","",None): continue
                            val = rb.get("Value")
                            if val and isinstance(val,dict):
                                gid = val.get("I") or val.get("Id")
                                if gid: detail_payloads[int(gid)] = rb
                    except Exception as exc:
                        logger.debug("[pw:%s] GetGameZip batch: %s", slug, exc)
                    await asyncio.sleep(0.2)

            # ── Enrich with full markets ──────────────────────────────────────
            enriched = 0
            for m in matches:
                try: gid_int = int(m.get("external_id",""))
                except: continue
                detail = detail_payloads.get(gid_int)
                if not detail: continue
                full_mkts = _parse_game_zip(detail)
                if full_mkts:
                    m["markets"]      = full_mkts
                    m["market_count"] = len(full_mkts)
                    enriched += 1

            # ── Save updated cookies ──────────────────────────────────────────
            try:
                state = await ctx.storage_state()
                with open(storage_f,"w") as f: json.dump(state,f)
            except Exception: pass

            await ctx.close()
            avg = int(sum(m.get("market_count",0) for m in matches)/len(matches)) if matches else 0
            print(f"  [{slug}] {sport_slug}: {len(matches)} matches, "
                  f"{enriched} enriched (avg {avg} markets)")
            return slug, matches

        # Run all BKs as concurrent async tasks
        tasks    = [asyncio.create_task(_harvest_one_bk(bk)) for bk in bks]
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

def fetch_bk_sport(
    bk: dict, sport_slug: str, mode: str = "upcoming",
    headless: bool = True, wait_s: int = 30,
    full_markets: bool = True, verbose: bool = True,
) -> list[dict]:
    """Fetch one sport from one BK via Playwright + competition iteration."""
    t0      = time.perf_counter()
    result  = asyncio.run(_pw_harvest_sport([bk], sport_slug, mode, headless, wait_s, full_markets))
    matches = result.get(bk["slug"], [])
    ms      = int((time.perf_counter() - t0) * 1000)
    if verbose:
        status = "✅" if matches else "⚠ "
        print(f"  {status} {bk['slug']:<12} {sport_slug:<16} {mode:<9} — {len(matches):4} matches ({ms}ms)")
    return matches


def fetch_sport_all_bks(
    sport_slug:   str,
    mode:         str  = "upcoming",
    bookmakers:   list[dict] | None = None,
    headless:     bool = True,
    wait_s:       int  = 30,
    max_matches:  int  = 1500,
    full_markets: bool = True,
    verbose:      bool = True,
) -> dict[str, list[dict]]:
    """
    Fetch ONE sport from ALL bookmakers concurrently.
    Opens one browser tab per BK — all in parallel.
    Returns {bk_slug: [matches]}.
    """
    bks = bookmakers or B2B_BOOKMAKERS
    t0  = time.perf_counter()
    if verbose:
        print(f"\n  Fetching {sport_slug}/{mode} from {len(bks)} BKs concurrently…")
    result = asyncio.run(
        _pw_harvest_sport(bks, sport_slug, mode, headless, wait_s, full_markets)
    )
    ms = int((time.perf_counter() - t0) * 1000)
    if verbose:
        total = sum(len(v) for v in result.values())
        for slug, matches in sorted(result.items()):
            status = "✅" if matches else "⚠ "
            avg = int(sum(m.get("market_count",0) for m in matches)/len(matches)) if matches else 0
            print(f"  {status} {slug:<12} {sport_slug:<16} — {len(matches):4} matches  avg {avg} mkts")
        print(f"  → {total} total matches ({ms}ms)")
    return result


def harvest_all_b2b(
    mode:         str  = "upcoming",
    sports:       list[str] | None  = None,
    bookmakers:   list[dict] | None = None,
    bk_workers:   int  = 7,
    headless:     bool = True,
    wait_s:       int  = 30,
    max_matches:  int  = 1500,
    full_markets: bool = True,
    verbose:      bool = True,
) -> dict[str, dict[str, list[dict]]]:
    """
    Harvest ALL bookmakers × ALL sports.
    Per sport: opens one browser tab per BK concurrently.
    """
    bks    = bookmakers or B2B_BOOKMAKERS
    sports = sports or ALL_SPORT_SLUGS

    if verbose:
        print(f"\n{'═'*65}")
        print(f"B2B Playwright [{mode.upper()}]  {len(bks)} BKs × {len(sports)} sports")
        print(f"full_markets={full_markets}")
        print(f"{'═'*65}")

    results: dict[str, dict[str, list[dict]]] = {bk["slug"]: {} for bk in bks}

    for sp in sports:
        if verbose: print(f"\n  ── {sp.upper()} ──")
        per_bk = asyncio.run(
            _pw_harvest_sport(bks, sp, mode, headless, wait_s, full_markets)
        )
        for bk in bks:
            results[bk["slug"]][sp] = per_bk.get(bk["slug"], [])

    return results

# =============================================================================
# MERGE
# =============================================================================

def merge_b2b(all_results: dict[str, dict[str, list[dict]]], sport_slug: str) -> list[dict]:
    unified: list[dict]=[]; key_idx: dict[str,int]={}
    for bk_slug, sport_data in all_results.items():
        for m in sport_data.get(sport_slug,[]):
            home=m.get("home_team","").lower().strip()
            away=m.get("away_team","").lower().strip()
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


def merge_b2b_by_match(per_bk: dict[str,list[dict]], sport_slug: str) -> list[dict]:
    return merge_b2b({bk:{sport_slug:ms} for bk,ms in per_bk.items()}, sport_slug)

# =============================================================================
# SAMPLE PRINTERS
# =============================================================================

def print_raw_sample(bk: dict, sport_slug: str = "soccer", mode: str = "upcoming") -> None:
    """Fetch one match and print all markets — for mapper building."""
    print(f"\n🎭 Fetching raw sample: {bk['slug']} / {sport_slug}")
    matches = fetch_bk_sport(bk, sport_slug, mode, headless=True, wait_s=30,
                              full_markets=True, verbose=False)
    if not matches: print("⚠  No matches"); return
    best = max(matches, key=lambda m: m.get("market_count", 0))
    print(f"Match: {best['home_team']} vs {best['away_team']}")
    print(f"Comp:  {best.get('competition')}")
    print(f"Markets ({best['market_count']}):")
    for mkt, outcomes in sorted(best.get("markets",{}).items()):
        out_str = "  ".join(f"{o}={p:.2f}" for o,p in sorted(outcomes.items()))
        print(f"  {mkt:<30} {out_str}")


def print_sample_per_sport(all_results: dict, sport_filter: str | None = None) -> None:
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
        for mkt, outcomes in sorted(best.get("markets",{}).items()):
            row = "  ".join(f"{o}={p:.2f}" for o,p in sorted(outcomes.items()))
            print(f"    {mkt:<28} {row}")
        print()


def fetch_sports_tree(bk: dict | None = None, verbose: bool = True) -> dict:
    """Navigate to /en/line and show available sports + competitions."""
    bk = bk or B2B_BOOKMAKERS[0]

    async def _get_tree():
        from playwright.async_api import async_playwright
        profile = PROFILE_DIR / bk["slug"]
        profile.mkdir(parents=True, exist_ok=True)
        payloads: list[dict] = []
        async with async_playwright() as pw:
            ctx = await pw.chromium.launch_persistent_context(
                str(profile), headless=True, args=["--no-sandbox"],
                ignore_https_errors=True, viewport={"width":1366,"height":768},
            )
            page = await ctx.new_page()
            async def _on(resp):
                if "GetSportsShortZip" in resp.url and resp.status==200:
                    try:
                        body = await resp.json()
                        if body: payloads.append(body)
                    except Exception: pass
            page.on("response", _on)
            try:
                await page.goto(f"{bk['base']}/en/line",
                                wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(5)
            except Exception: pass
            await ctx.close()
        return payloads

    payloads = asyncio.run(_get_tree())
    tree: dict = {}
    for payload in payloads:
        for item in payload.get("Value") or []:
            if not isinstance(item,dict): continue
            if item.get("CID",0) not in (1,2): continue
            name=item.get("N") or f"sport_{item.get('I')}"
            sid=item.get("I"); cnt=item.get("C",0)
            if name not in tree or tree[name]["count"]<cnt:
                tree[name]={"id":sid,"count":cnt}
    if verbose:
        print(f"\n{'─'*50}\n{'Sport':<25}{'ID':>5}  {'Matches':>7}\n{'─'*50}")
        for name,data in sorted(tree.items(),key=lambda x:-x[1]["count"]):
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
            if matches: _save_sport_to_redis(sport, mode, matches, bk_slug); sports_seen.add(sport)
    for sport in sports_seen:
        per_bk={bk:data.get(sport,[]) for bk,data in all_results.items()}
        merged=merge_b2b_by_match(per_bk,sport)
        if merged: _save_sport_to_redis(sport, mode, merged, "b2b")

# =============================================================================
# FLASK CLI
# =============================================================================

def register_cli(flask_app) -> None:
    import click

    def _check_pw() -> bool:
        try: import playwright; return True  # noqa
        except ImportError:
            click.echo("❌ pip install playwright --break-system-packages && playwright install chromium")
            return False

    @flask_app.cli.command("harvest-b2b-pw-test")
    @click.option("--bk",         default=None)
    @click.option("--sport",      default="soccer")
    @click.option("--mode",       default="upcoming", type=click.Choice(["upcoming","live"]))
    @click.option("--headless",   default=True, is_flag=False, type=bool)
    @click.option("--wait",       default=30)
    @click.option("--no-full-markets", is_flag=True)
    def _test(bk, sport, mode, headless, wait, no_full_markets):
        """Test: one sport, all BKs concurrently (or --bk for one BK)."""
        if not _check_pw(): return
        bks = [_BK_BY_SLUG[bk]] if bk else B2B_BOOKMAKERS
        click.echo(f"\n🎭 {len(bks)} BK(s) × {sport} [{mode}]  full_markets={not no_full_markets}")
        per_bk = fetch_sport_all_bks(
            sport_slug=sport, mode=mode, bookmakers=bks,
            headless=headless, wait_s=wait,
            full_markets=not no_full_markets, verbose=True,
        )
        total = sum(len(v) for v in per_bk.values())
        click.echo(f"\n{'─'*55}\n  TOTAL: {total} matches\n{'─'*55}")
        for slug, matches in sorted(per_bk.items()):
            status = "✅" if matches else "⚠ "
            if matches:
                best = max(matches, key=lambda m: m.get("market_count", 0))
                click.echo(f"  {status} {slug:<12} {len(matches):4}  best: {best['home_team']} vs {best['away_team']}  [{best['market_count']} mkts]")
            else:
                click.echo(f"  {status} {slug:<12}    0")

    @flask_app.cli.command("b2b-pw-setup")
    @click.option("--bk", default="1xbet")
    def _setup(bk):
        """Open browser for manual login — saves session cookies."""
        if not _check_pw(): return
        bk_obj = _BK_BY_SLUG.get(bk)
        if not bk_obj: click.echo(f"❌ Unknown: {bk}"); return
        from playwright.sync_api import sync_playwright
        profile = PROFILE_DIR / bk; profile.mkdir(parents=True, exist_ok=True)
        storage_f = profile / "storage.json"
        click.echo(f"\n🎭 Opening {bk_obj['base']} — log in, then press ENTER.")
        with sync_playwright() as pw:
            ctx = pw.chromium.launch_persistent_context(
                str(profile), headless=False,
                args=["--no-sandbox","--start-maximized"],
                viewport={"width":1366,"height":768},
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
                ),
            )
            page = ctx.new_page(); page.goto(bk_obj["base"])
            input("\n  ↳ Logged in? Press ENTER to save and close…\n")
            try:
                state = ctx.storage_state()
                with open(storage_f,"w") as f: json.dump(state,f)
                click.echo(f"  💾 Storage saved → {storage_f}")
            except Exception as e:
                click.echo(f"  ⚠️  {e}")
            ctx.close()


# =============================================================================
# STANDALONE
# =============================================================================

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    bk_slug = sys.argv[1] if len(sys.argv) > 1 else "betwinner"
    sport   = sys.argv[2] if len(sys.argv) > 2 else "soccer"
    hl      = sys.argv[3] != "false" if len(sys.argv) > 3 else True
    bk = _BK_BY_SLUG.get(bk_slug, B2B_BOOKMAKERS[0])
    print(f"\n🎭 {bk['slug']} / {sport} / headless={hl}")
    matches = fetch_bk_sport(bk, sport, "upcoming", hl, 35, full_markets=True)
    print(f"✅ {len(matches)} matches")
    if matches:
        best = max(matches, key=lambda m: m.get("market_count", 0))
        import pprint; pprint.pprint(best)