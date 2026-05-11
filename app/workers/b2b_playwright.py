"""
app/workers/b2b_playwright.py
==============================
Playwright-based B2B harvester.

STRATEGY: hardcoded competition LI IDs → no waiting for GetSportsShortZip.
  1. Navigate to sport page (establishes session + cookies)
  2. Use page.evaluate(fetch(Get1x2_VZip?champs={LI})) for EVERY competition
     — browser auto-generates x-hd token
  3. Collect all matches across all competitions
  4. Optionally enrich each match with GetGameZip (full markets)

Competition LI IDs are hardcoded from the GetSportsShortZip JSON snapshot
(May 2026). They are stable and don't change — leagues keep the same ID forever.

INSTALL:
  pip install playwright --break-system-packages
  playwright install chromium

FIRST-TIME LOGIN (saves session cookies):
  flask b2b-pw-setup --bk 1xbet
  flask b2b-pw-setup --bk betwinner
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
# SPORT IDS + PAGE SLUGS
# =============================================================================

B2B_SPORT_IDS: dict[str, int] = {
    "soccer":1,"football":1,"esoccer":40,"efootball":40,
    "basketball":3,"tennis":4,"table-tennis":10,
    "ice-hockey":2,"volleyball":6,"handball":8,"baseball":5,
    "american-football":13,"rugby":7,"boxing":9,"mma":56,"ufc":189,
    "cricket":66,"darts":21,"golf":41,"futsal":14,
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
PROFILE_DIR = Path(os.environ.get("B2B_PROFILE_DIR", "/tmp/b2b_pw_profiles"))

# =============================================================================
# HARDCODED COMPETITION LI IDs (from GetSportsShortZip snapshot, May 2026)
# These are stable IDs — they never change for existing leagues.
# Add new ones as needed; just append (LI, name, GC_approx).
# =============================================================================

_SPORT_COMPETITIONS: dict[int, list[tuple[int, str, int]]] = {
    # ── Football (sport_id=1) ────────────────────────────────────────────────
    1: [
        # (LI, name, approx_GC) — sorted biggest-first for priority
        (2708736, "World Cup 2026", 67),
        (125983,  "Friendlies. National Teams", 59),
        (2973812, "WC Qual 2027. Europe. Women", 40),
        (828065,  "USA. MLS", 34),
        (127733,  "Spain. La Liga", 25),
        (2809583, "Spain. La Liga. Team vs Player", 22),
        (1268397, "Brazil. Campeonato. Serie A", 21),
        (2489433, "World Cup 2026. Winner", 19),
        (44797,   "Sweden. Division 1", 17),
        (8777,    "Greece. SuperLeague", 17),
        (40369,   "Germany. Oberliga Bayern", 16),
        (120013,  "Brazil. Copa do Brasil", 16),
        (2922491, "Argentina. Primera B Nacional", 16),
        (142091,  "Copa Libertadores", 16),
        (1528791, "Copa Sudamericana", 16),
        (13521,   "Scotland. Premier League", 14),
        (27695,   "Switzerland. SuperLeague", 14),
        (27687,   "Spain. Segunda Division", 13),
        (12821,   "France. Ligue 1", 13),
        (1371789, "Sweden. Superettan", 13),
        (2924971, "Bulgaria. First League", 13),
        (2892390, "Peru. Liga 1", 11),
        (225733,  "Russia. Premier League", 11),
        (212425,  "Sweden. Allsvenskan", 11),
        (27731,   "Poland. Ekstraklasa", 11),
        (96463,   "Germany. Bundesliga", 10),
        (109313,  "Germany. 2. Bundesliga", 10),
        (2579233, "Germany. 3. Liga", 10),
        (88637,   "England. Premier League", 10),
        (1793471, "Norway. Eliteserien", 10),
        (11113,   "Turkey. SuperLiga", 10),
        (57265,   "Brazil. Serie B", 9),
        (27707,   "Czech Republic. Chance Liga", 9),
        (1173855, "Switzerland. Challenge League", 9),
        (28787,   "Belgium. Jupiler League", 8),
        (28465,   "Germany. Oberliga NOFV-Süd", 8),
        (29949,   "Ukraine. Premier League", 8),
        (8773,    "Denmark. Superliga", 8),
        (26031,   "Austria. Bundesliga", 8),
        (110163,  "Italy. Serie A", 8),
        (276999,  "Ecuador. Serie A", 8),
        (11249,   "Indonesia. Super League", 8),
        (16819,   "Saudi Arabia. Pro League", 8),
        (90523,   "South Africa. PSL", 8),
        (118663,  "Portugal. Primeira Liga", 8),
        (1015483, "Belarus. Premier League", 7),
        (31508,   "Norway. Adeccoligaen", 7),
        (30693,   "Poland. Liga 1", 7),
        (2960706, "Japan. J1 Division", 7),
        (120501,  "Latvia. Virsliga", 7),
        (55427,   "Lithuania. League 1", 7),
        (147087,  "Egypt. Premier League", 7),
        (7067,    "Italy. Serie B", 4),
        (1122087, "India. Super League", 4),
        (1924563, "Canada. Premier League", 4),
        (33021,   "Kazakhstan. Premier League", 8),
        (2018750, "Netherlands. Eredivisie", 10),
        (6,       "Russia. League 1", 6),
        (2421233, "Russia. League 1", 6),
        (1692148, "France. National", 8),
        (30467,   "South Korea. K League 1", 8),
        (33137,   "South Korea. K League 2", 7),
        (119599,  "Argentina. Primera Division", 6),
        (52183,   "Uruguay. Primera Division", 2),
        (214147,  "Colombia. Primera A", 4),
        (28298,   "Chile. Primera Division", 8),
        (58043,   "China. Super League", 8),
        (11121,   "Romania. Liga 1", 8),
        (27735,   "Croatia. HNL", 5),
        (30049,   "Slovenia. League 1", 4),
        (27701,   "Slovakia. Super League", 6),
        (166963,  "Serbia. 1st League", 2),
        (39969,   "Denmark. 1st Division", 6),
        (52591,   "Denmark. 2nd Division", 6),
        (119445,  "Ireland. Premier League", 5),
        (29975,   "Ireland. Division 1", 5),
        (2905446, "Australia. A League", 2),
        (118587,  "UEFA Champions League", 1),
        (118593,  "UEFA Europa League", 1),
        (2252762, "UEFA Conference League", 1),
        (38317,   "CAF Champions League", 1),
        (31429,   "Morocco. Botola", 3),
        (28207,   "Tunisia. Ligue 1", 1),
        (156433,  "Ivory Coast. League 1", 2),
        (108319,  "England. FA Cup", 1),
        (105759,  "England. Championship", 2),
        (13709,   "England. League One", 2),
        (24637,   "England. League Two", 3),
        (316897,  "England. Superleague. Women", 5),
        (1299397, "England. Premier League 2", 2),
        (34275,   "UEFA CL Women", 1),
        (190409,  "USA. NWSL Women", 7),
        (2306111, "Mexico. Liga MX", 2),
    ],

    # ── Basketball (sport_id=3) ───────────────────────────────────────────────
    3: [
        (0, "_all", 0),   # placeholder — fetch unfiltered (sport page gives results)
    ],

    # ── Tennis (sport_id=4) ──────────────────────────────────────────────────
    4: [
        (0, "_all", 0),
    ],

    # ── Ice Hockey (sport_id=2) ──────────────────────────────────────────────
    2: [
        (0, "_all", 0),
    ],

    # ── Volleyball (sport_id=6) ──────────────────────────────────────────────
    6: [
        (0, "_all", 0),
    ],

    # ── Cricket (sport_id=66) ────────────────────────────────────────────────
    66: [
        (0, "_all", 0),
    ],

    # ── Table Tennis (sport_id=10) ───────────────────────────────────────────
    10: [
        (0, "_all", 0),
    ],

    # ── MMA/Martial Arts (sport_id=56) ──────────────────────────────────────
    56: [
        (0, "_all", 0),
    ],

    # ── Rugby (sport_id=7) ───────────────────────────────────────────────────
    7: [
        (0, "_all", 0),
    ],

    # ── Handball (sport_id=8) ────────────────────────────────────────────────
    8: [
        (0, "_all", 0),
    ],

    # ── Darts (sport_id=21) ──────────────────────────────────────────────────
    21: [
        (0, "_all", 0),
    ],

    # ── American Football (sport_id=13) ─────────────────────────────────────
    13: [
        (0, "_all", 0),
    ],

    # ── Baseball (sport_id=5) ────────────────────────────────────────────────
    5: [
        (0, "_all", 0),
    ],

    # ── Esports (sport_id=40) ────────────────────────────────────────────────
    40: [
        (0, "_all", 0),
    ],

    # ── Boxing (sport_id=9) ──────────────────────────────────────────────────
    9: [
        (0, "_all", 0),
    ],

    # ── UFC (sport_id=189) ───────────────────────────────────────────────────
    189: [
        (0, "_all", 0),
    ],
}

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


def _parse_value(value: list, bk: dict, sport_slug: str,
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
# CORE PLAYWRIGHT HARVEST — hardcoded competition IDs, no waiting
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
      1. Navigate to sport page (establishes session, x-hd context)
      2. page.evaluate(Promise.all([fetch(Get1x2_VZip?champs=LI), ...]))
         for every hardcoded competition — browser handles x-hd
      3. For sports with no hardcoded comps: intercept the natural page response
      4. Optionally enrich with GetGameZip
    """
    from playwright.async_api import async_playwright

    sport_id   = B2B_SPORT_IDS.get(sport_slug.lower())
    page_slug  = SPORT_PAGE_SLUG.get(sport_slug, sport_slug)
    feed_path  = "live" if mode == "live" else "line"
    feed_api   = "LiveFeed" if mode == "live" else "LineFeed"
    comps_raw  = _SPORT_COMPETITIONS.get(sport_id or 0, [(0,"_all",0)])
    # Filter out placeholder (LI=0 means no comps hardcoded — use page interception)
    hardcoded_lis = [(li,name) for li,name,gc in comps_raw if li > 0]
    use_interception = len(hardcoded_lis) == 0

    results: dict[str, list[dict]] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=["--no-sandbox","--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"],
        )

        async def _harvest_one(bk: dict) -> tuple[str, list[dict]]:
            slug     = bk["slug"]
            p        = bk["partner_id"]
            domain   = bk["domain"]
            profile  = PROFILE_DIR / slug
            profile.mkdir(parents=True, exist_ok=True)
            storage_f = profile / "storage.json"

            ctx_opts: dict = {
                "viewport": {"width":1366,"height":900},
                "user_agent": (
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

            intercepted: list[dict] = []  # for use_interception mode
            detail_payloads: dict[int, dict] = {}

            async def _on_response(resp):
                url = resp.url
                if resp.status != 200: return
                try:
                    if "Get1x2_VZip" in url and use_interception:
                        body = await resp.json()
                        if body and body.get("ErrorCode") in (0,""):
                            intercepted.append(body)
                    elif "GetGameZip" in url:
                        body = await resp.json()
                        if body and body.get("ErrorCode") in (0,""):
                            import re as _re
                            m = _re.search(r'[?&]id=(\d+)', url)
                            if m: detail_payloads[int(m.group(1))] = body
                except Exception: pass

            page.on("response", _on_response)

            page_url = f"{bk['base']}/en/{feed_path}/{page_slug}"
            print(f"  [{slug}] → {page_url}")
            try:
                await page.goto(page_url, wait_until="domcontentloaded",
                                timeout=wait_s * 1000)
                await asyncio.sleep(3)   # let cookies + x-hd context settle
            except Exception as exc:
                logger.warning("[pw:%s] goto: %s", slug, exc)
                await ctx.close()
                return slug, []

            # ── Fetch all competitions via page.evaluate ──────────────────────
            all_payloads: list[dict] = list(intercepted)   # page-load responses

            if hardcoded_lis:
                batch_size = 8   # stay under Promise.all limits
                for i in range(0, len(hardcoded_lis), batch_size):
                    batch = hardcoded_lis[i:i+batch_size]
                    fetches = ",\n".join(
                        f'fetch("https://{domain}/service-api/{feed_api}/Get1x2_VZip'
                        f'?sports={sport_id or 1}&champs={li}&count=40&lng=en&mode=4'
                        f'&country=87&partner={p}&getEmpty=true&virtualSports=true",'
                        f'{{"headers":{{"accept":"application/json",'
                        f'"is-srv":"false","x-app-n":"__BETTING_APP__",'
                        f'"x-svc-source":"__BETTING_APP__",'
                        f'"content-type":"application/json"}}}}'
                        f').then(r=>r.json()).catch(()=>null)'
                        for li, _ in batch
                    )
                    try:
                        responses = await page.evaluate(f"Promise.all([{fetches}])")
                        for rb in responses or []:
                            if rb and rb.get("ErrorCode") in (0,"0","",None):
                                all_payloads.append(rb)
                    except Exception as exc:
                        logger.debug("[pw:%s] champs batch %d: %s", slug, i, exc)
                    await asyncio.sleep(0.2)
            else:
                # No hardcoded comps — wait for intercepted page-load responses
                deadline = time.perf_counter() + wait_s
                while time.perf_counter() < deadline:
                    await asyncio.sleep(1)
                    if intercepted: await asyncio.sleep(2); break

            # ── Parse all payloads ────────────────────────────────────────────
            matches: list[dict] = []
            seen_gids: set = set()
            for payload in all_payloads:
                for m in _parse_value(payload.get("Value") or [], bk, sport_slug, mode, sport_id):
                    gid_str = m.get("external_id","")
                    try: gid_int = int(gid_str)
                    except: gid_int = None
                    if gid_int and gid_int in seen_gids: continue
                    if gid_int: seen_gids.add(gid_int)
                    matches.append(m)

            # ── GetGameZip: full markets ──────────────────────────────────────
            if full_markets and matches:
                game_ids = []
                for m in matches:
                    try: game_ids.append(int(m["external_id"]))
                    except: pass

                for i in range(0, len(game_ids), 8):
                    batch = game_ids[i:i+8]
                    fetches = ",\n".join(
                        f'fetch("https://{domain}/service-api/{feed_api}/GetGameZip'
                        f'?id={gid}&lng=en&isSubGames=true&GroupEvents=true'
                        f'&countevents=250&grMode=4&partner={p}&topGroups='
                        f'&country=87&marketType=1&isNewBuilder=true",'
                        f'{{"headers":{{"accept":"application/json","is-srv":"false",'
                        f'"x-app-n":"__BETTING_APP__","x-svc-source":"__BETTING_APP__"}}}}'
                        f').then(r=>r.json()).catch(()=>null)'
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

            # ── Enrich matches with full markets ──────────────────────────────
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

            # ── Save cookies ──────────────────────────────────────────────────
            try:
                state = await ctx.storage_state()
                with open(storage_f,"w") as f: json.dump(state,f)
            except Exception: pass

            await ctx.close()
            avg = int(sum(m.get("market_count",0) for m in matches)/len(matches)) if matches else 0
            print(f"  [{slug}] {sport_slug}: {len(matches)} matches "
                  f"({enriched} enriched, avg {avg} markets)")
            return slug, matches

        tasks    = [asyncio.create_task(_harvest_one(bk)) for bk in bks]
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()

    for outcome in outcomes:
        if isinstance(outcome, Exception):
            logger.error("[pw] task: %s", outcome)
            continue
        slug, matches = outcome
        results[slug] = matches
    return results

# =============================================================================
# PUBLIC API
# =============================================================================

def fetch_bk_sport(bk, sport_slug, mode="upcoming", headless=True,
                   wait_s=30, full_markets=True, verbose=True):
    t0=time.perf_counter()
    res=asyncio.run(_pw_harvest_sport([bk],sport_slug,mode,headless,wait_s,full_markets))
    matches=res.get(bk["slug"],[])
    ms=int((time.perf_counter()-t0)*1000)
    if verbose:
        s="✅" if matches else "⚠ "
        print(f"  {s} {bk['slug']:<12} {sport_slug:<16} {mode:<9} — {len(matches):4} ({ms}ms)")
    return matches


def fetch_sport_all_bks(sport_slug, mode="upcoming", bookmakers=None,
                         headless=True, wait_s=30, max_matches=1500,
                         full_markets=True, verbose=True):
    bks=bookmakers or B2B_BOOKMAKERS
    t0=time.perf_counter()
    if verbose: print(f"\n  {len(bks)} BKs × {sport_slug} [{mode}] concurrently…")
    res=asyncio.run(_pw_harvest_sport(bks,sport_slug,mode,headless,wait_s,full_markets))
    if verbose:
        total=sum(len(v) for v in res.values())
        for slug,ms in sorted(res.items()):
            s="✅" if ms else "⚠ "
            print(f"  {s} {slug:<12} {len(ms):4} matches")
        print(f"  → {total} total ({int((time.perf_counter()-t0)*1000)}ms)")
    return res


def harvest_all_b2b(mode="upcoming", sports=None, bookmakers=None,
                     bk_workers=7, headless=True, wait_s=30,
                     max_matches=1500, full_markets=True, verbose=True):
    bks=bookmakers or B2B_BOOKMAKERS
    sports=sports or ALL_SPORT_SLUGS
    if verbose:
        print(f"\n{'═'*65}")
        print(f"B2B Playwright [{mode.upper()}] {len(bks)} BKs × {len(sports)} sports")
        print(f"{'═'*65}")
    results={bk["slug"]:{} for bk in bks}
    for sp in sports:
        if verbose: print(f"\n  ── {sp.upper()} ──")
        per_bk=asyncio.run(_pw_harvest_sport(bks,sp,mode,headless,wait_s,full_markets))
        for bk in bks:
            results[bk["slug"]][sp]=per_bk.get(bk["slug"],[])
    return results


def merge_b2b(all_results, sport_slug):
    unified=[]; key_idx={}
    for bk_slug,sport_data in all_results.items():
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
                entry={**m,"bk_count":1,"bookmakers":dict(m.get("bookmakers") or {}),
                       "markets":dict(m.get("markets") or {})}
                key_idx[key]=len(unified); unified.append(entry)
    return unified


def merge_b2b_by_match(per_bk, sport_slug):
    return merge_b2b({bk:{sport_slug:ms} for bk,ms in per_bk.items()},sport_slug)


def print_raw_sample(bk, sport_slug="soccer", mode="upcoming"):
    print(f"\n🎭 {bk['slug']} / {sport_slug}")
    matches=fetch_bk_sport(bk,sport_slug,mode,True,30,True,False)
    if not matches: print("⚠  No matches"); return
    best=max(matches,key=lambda m:m.get("market_count",0))
    print(f"Match: {best['home_team']} vs {best['away_team']}  |  {best.get('competition')}")
    print(f"Markets ({best['market_count']}):")
    for mkt,outcomes in sorted(best.get("markets",{}).items()):
        print(f"  {mkt:<28} " + "  ".join(f"{o}={p:.2f}" for o,p in sorted(outcomes.items())))


def print_sample_per_sport(all_results, sport_filter=None):
    for sp in ALL_SPORT_SLUGS:
        if sport_filter and sp!=sport_filter: continue
        merged=merge_b2b(all_results,sp)
        if not merged: print(f"  {sp:<18} — no matches"); continue
        best=max(merged,key=lambda m:m.get("market_count",0))
        print(f"\n{sp.upper()}: {best['home_team']} vs {best['away_team']}")
        for mkt,outcomes in sorted(best.get("markets",{}).items()):
            print(f"  {mkt:<28} " + "  ".join(f"{o}={p:.2f}" for o,p in sorted(outcomes.items())))


def fetch_sports_tree(bk=None, verbose=True):
    bk=bk or B2B_BOOKMAKERS[0]
    tree={}
    for sport_slug in ALL_SPORT_SLUGS:
        sid=B2B_SPORT_IDS.get(sport_slug)
        comps=_SPORT_COMPETITIONS.get(sid or 0,[])
        total=sum(gc for _,_,gc in comps if _ > 0)
        tree[sport_slug]={"id":sid,"count":total,"competitions":len([c for c in comps if c[0]>0])}
    if verbose:
        print(f"\n{'─'*55}\n{'Sport':<22}{'ID':>4}  {'Comps':>6}  {'~Games':>7}\n{'─'*55}")
        for sp,d in sorted(tree.items(),key=lambda x:-x[1]["count"]):
            print(f"  {sp:<20} {d['id']:>4}  {d['competitions']:>6}  {d['count']:>7}")
    return tree


def _save_sport_to_redis(sport, mode, matches, bk_slug="b2b"):
    try:
        from app.workers.redis_bus import publish_snapshot
        publish_snapshot(bk_slug, mode, sport, matches)
    except Exception as exc:
        logger.warning("[pw] Redis: %s", exc)


def _save_results_to_redis(all_results, mode="upcoming"):
    seen=set()
    for bk_slug,sport_data in all_results.items():
        for sport,matches in sport_data.items():
            if matches: _save_sport_to_redis(sport,mode,matches,bk_slug); seen.add(sport)
    for sport in seen:
        per_bk={bk:d.get(sport,[]) for bk,d in all_results.items()}
        merged=merge_b2b_by_match(per_bk,sport)
        if merged: _save_sport_to_redis(sport,mode,merged,"b2b")

# =============================================================================
# CLI
# =============================================================================

def register_cli(flask_app):
    import click

    def _check_pw():
        try: import playwright; return True  # noqa
        except ImportError:
            click.echo("❌ pip install playwright --break-system-packages && playwright install chromium")
            return False

    @flask_app.cli.command("b2b-pw-setup")
    @click.option("--bk", default="1xbet")
    def _setup(bk):
        """Log in to a bookmaker — saves session cookies."""
        if not _check_pw(): return
        bk_obj=_BK_BY_SLUG.get(bk)
        if not bk_obj: click.echo(f"❌ Unknown: {bk}"); return
        from playwright.sync_api import sync_playwright
        profile=PROFILE_DIR/bk; profile.mkdir(parents=True,exist_ok=True)
        click.echo(f"\n🎭 Opening {bk_obj['base']} — log in then press ENTER.")
        with sync_playwright() as pw:
            ctx=pw.chromium.launch_persistent_context(
                str(profile),headless=False,args=["--no-sandbox","--start-maximized"],
                viewport={"width":1366,"height":768},
                user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"),
            )
            page=ctx.new_page(); page.goto(bk_obj["base"])
            input("\n  ↳ Logged in? Press ENTER to save & close…\n")
            try:
                state=ctx.storage_state()
                with open(profile/"storage.json","w") as f: json.dump(state,f)
                click.echo(f"  💾 Saved → {profile/'storage.json'}")
            except Exception as e: click.echo(f"  ⚠️  {e}")
            ctx.close()

    @flask_app.cli.command("b2b-pw-test")
    @click.option("--bk",    default=None)
    @click.option("--sport", default="soccer")
    @click.option("--mode",  default="upcoming",type=click.Choice(["upcoming","live"]))
    @click.option("--headless",default=True,is_flag=False,type=bool)
    @click.option("--wait",  default=30)
    @click.option("--no-full-markets",is_flag=True)
    def _test(bk, sport, mode, headless, wait, no_full_markets):
        """Test: all BKs concurrently for one sport."""
        if not _check_pw(): return
        bks=[_BK_BY_SLUG[bk]] if bk else B2B_BOOKMAKERS
        click.echo(f"\n🎭 {len(bks)} BK(s) × {sport} [{mode}]")
        per_bk=fetch_sport_all_bks(sport_slug=sport,mode=mode,bookmakers=bks,
                                    headless=headless,wait_s=wait,
                                    full_markets=not no_full_markets,verbose=True)
        total=sum(len(v) for v in per_bk.values())
        click.echo(f"\n  TOTAL: {total} matches")


if __name__ == "__main__":
    import sys, logging as _log
    _log.basicConfig(level=_log.INFO)
    bk_slug=sys.argv[1] if len(sys.argv)>1 else "betwinner"
    sport  =sys.argv[2] if len(sys.argv)>2 else "soccer"
    hl     =sys.argv[3]!="false" if len(sys.argv)>3 else True
    bk=_BK_BY_SLUG.get(bk_slug,B2B_BOOKMAKERS[0])
    print(f"\n🎭 {bk['slug']} / {sport} / headless={hl}")
    ms=fetch_bk_sport(bk,sport,"upcoming",hl,35,full_markets=True)
    print(f"✅ {len(ms)} matches")
    if ms:
        best=max(ms,key=lambda m:m.get("market_count",0))
        import pprint; pprint.pprint(best)