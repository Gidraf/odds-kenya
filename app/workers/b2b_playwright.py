"""
app/workers/b2b_playwright.py
==============================
Playwright B2B harvester — pure navigation approach.

For each competition, we navigate to its page URL:
  https://betwinner.ke/en/line/football/96463-germany-bundesliga

The browser fires Get1x2_VZip?champs=96463 naturally.
We intercept that response — no evaluate(), no manual x-hd.

Competition LI IDs are hardcoded from GetSportsShortZip snapshot (May 2026).
LI IDs are permanent — leagues keep the same ID forever.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# =============================================================================
# BOOKMAKERS
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
B2B_SUPPORTED_SPORTS = ALL_SPORT_SLUGS
PROFILE_DIR = Path(os.environ.get("B2B_PROFILE_DIR", "/tmp/b2b_pw_profiles"))

# =============================================================================
# COMPETITION LIST — (LI, url_slug, approx_GC)
# Hardcoded from GetSportsShortZip snapshot May 2026.
# Navigate to: /en/line/{sport_page}/{LI}-{url_slug}
# =============================================================================

# (LI, url-slug, approx_game_count)
_COMPETITIONS: dict[int, list[tuple[int, str, int]]] = {

    1: [  # Football
        (2708736, "world-cup-2026",                  67),
        (828065,  "usa-mls",                         34),
        (125983,  "friendlies-national-teams",       59),
        (127733,  "spain-la-liga",                   25),
        (1268397, "brazil-serie-a",                  22),
        (2809583, "spain-la-liga-team-vs-player",    22),
        (44797,   "sweden-division-1",               17),
        (8777,    "greece-superleague",               17),
        (2922491, "argentina-primera-b-nacional",    17),
        (40369,   "germany-oberliga-bayern",         16),
        (142091,  "copa-libertadores",               16),
        (1528791, "copa-sudamericana",               16),
        (120013,  "brazil-copa-do-brasil",           16),
        (13521,   "scotland-premier-league",         14),
        (27695,   "switzerland-superleague",         14),
        (12821,   "france-ligue-1",                  13),
        (27687,   "spain-segunda-division",          13),
        (1371789, "sweden-superettan",               13),
        (2924971, "bulgaria-first-league",           15),
        (2892390, "peru-liga-1",                     11),
        (225733,  "russia-premier-league",           11),
        (212425,  "sweden-allsvenskan",              11),
        (27731,   "poland-ekstraklasa",              11),
        (2960706, "japan-j1-division",               11),
        (96463,   "germany-bundesliga",              10),
        (109313,  "germany-2-bundesliga",            10),
        (2579233, "germany-3-liga",                  10),
        (88637,   "england-premier-league",          10),
        (1793471, "norway-eliteserien",              10),
        (11113,   "turkey-superliga",                10),
        (2018750, "netherlands-eredivisie",          10),
        (190409,  "usa-nwsl-women",                   7),
        (57265,   "brazil-serie-b",                   9),
        (27707,   "czech-republic-chance-liga",       9),
        (1173855, "switzerland-challenge-league",     9),
        (28787,   "belgium-jupiler-league",           8),
        (28465,   "germany-oberliga-nofv-sud",        8),
        (29949,   "ukraine-premier-league",           8),
        (8773,    "denmark-superliga",                8),
        (26031,   "austria-bundesliga",               8),
        (110163,  "italy-serie-a",                    8),
        (276999,  "ecuador-serie-a",                  8),
        (11249,   "indonesia-super-league",           8),
        (16819,   "saudi-arabia-pro-league",          8),
        (90523,   "south-africa-psl",                 8),
        (118663,  "portugal-primeira-liga",           8),
        (33021,   "kazakhstan-premier-league",        8),
        (147087,  "egypt-premier-league",             8),
        (1692148, "france-national",                  8),
        (30467,   "south-korea-k-league-1",           8),
        (2421233, "russia-league-1",                  9),
        (31508,   "norway-adeccoligaen",               8),
        (30693,   "poland-liga-1",                     7),
        (120501,  "latvia-virsliga",                   7),
        (55427,   "lithuania-league-1",                7),
        (33137,   "south-korea-k-league-2",            7),
        (1015483, "belarus-premier-league",            7),
        (316897,  "england-superleague-women",         5),
        (119445,  "ireland-premier-league",            5),
        (29975,   "ireland-division-1",                5),
        (7067,    "italy-serie-b",                     4),
        (1122087, "india-super-league",                4),
        (1924563, "canada-premier-league",             4),
        (214147,  "colombia-primera-a",                4),
        (28298,   "chile-primera-division",            8),
        (58043,   "china-super-league",                8),
        (11121,   "romania-liga-1",                    8),
        (27735,   "croatia-hnl",                       5),
        (30049,   "slovenia-league-1",                 4),
        (27701,   "slovakia-super-league",             6),
        (166963,  "serbia-1st-league",                 2),
        (39969,   "denmark-1st-division",              6),
        (52591,   "denmark-2nd-division",              6),
        (2905446, "australia-a-league",                2),
        (118587,  "uefa-champions-league",             1),
        (118593,  "uefa-europa-league",                1),
        (2252762, "uefa-conference-league",            1),
        (38317,   "caf-champions-league",              1),
        (31429,   "morocco-botola",                    3),
        (108319,  "england-fa-cup",                    1),
        (105759,  "england-championship",              2),
        (13709,   "england-league-one",                2),
        (24637,   "england-league-two",                3),
        (52183,   "uruguay-primera-division",          2),
        (119599,  "argentina-primera-division",        6),
        (176125,  "russian-cup",                       1),
        (2306111, "mexico-liga-mx",                    2),
    ],

    3: [  # Basketball — sport page gives all; navigate to root only
        (0, "_sport_page", 0),
    ],

    4: [  # Tennis
        (0, "_sport_page", 0),
    ],

    2: [  # Ice Hockey
        (0, "_sport_page", 0),
    ],

    6: [  # Volleyball
        (0, "_sport_page", 0),
    ],

    66: [  # Cricket
        (0, "_sport_page", 0),
    ],

    10: [  # Table Tennis
        (0, "_sport_page", 0),
    ],

    56: [  # MMA
        (0, "_sport_page", 0),
    ],

    7: [  # Rugby
        (0, "_sport_page", 0),
    ],

    8: [  # Handball
        (0, "_sport_page", 0),
    ],

    21: [  # Darts
        (0, "_sport_page", 0),
    ],

    13: [  # American Football
        (0, "_sport_page", 0),
    ],

    5: [  # Baseball
        (0, "_sport_page", 0),
    ],

    40: [  # Esports
        (0, "_sport_page", 0),
    ],

    9: [  # Boxing
        (0, "_sport_page", 0),
    ],

    189: [  # UFC
        (0, "_sport_page", 0),
    ],
}

# =============================================================================
# PARSERS
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
    markets: dict = defaultdict(dict)
    def _p(ev):
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
    markets: dict = defaultdict(dict)
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
    out=[]; seen:set=set()
    for item in value:
        if not isinstance(item,dict): continue
        if "O1E" in item or "O1" in item:
            if sport_id and item.get("SI") not in (sport_id,None): continue
            gid=item.get("I")
            if gid and gid in seen: continue
            if gid: seen.add(gid)
            m=_parse_game(item,bk,sport_slug,mode)
            if m: out.append(m)
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
                        if m: out.append(m)
    return out

# =============================================================================
# CORE — navigate to each competition page, collect natural response
# =============================================================================

async def _harvest_one_bk(
    bk:           dict,
    sport_slug:   str,
    mode:         str  = "upcoming",
    headless:     bool = True,
    wait_s:       int  = 15,
    full_markets: bool = True,
    output_dir:   str  = "harvest_dumps",
    tab_concurrency: int = 5,   # unused, kept for API compat
) -> list[dict]:
    """
    Navigate to the sport page then each competition page on a SINGLE tab.
    For every page that returns data, saves a JSON file:

      {output_dir}/{bk_slug}/{sport_slug}/
          _sports_root.json            ← raw response from sport index page
          germany-bundesliga.json      ← raw + parsed for each competition
          england-premier-league.json
          ...

    Returns all parsed matches (flat list, deduped by game ID).
    """
    import re as _re
    from playwright.async_api import async_playwright

    sport_id   = B2B_SPORT_IDS.get(sport_slug.lower())
    page_slug  = SPORT_PAGE_SLUG.get(sport_slug, sport_slug)
    feed_path  = "live" if mode == "live" else "line"
    feed_api   = "LiveFeed" if mode == "live" else "LineFeed"
    profile    = PROFILE_DIR / bk["slug"]
    profile.mkdir(parents=True, exist_ok=True)
    storage_f  = profile / "storage.json"

    # Output directory for this BK + sport
    bk_sport_dir = os.path.join(output_dir, bk["slug"], sport_slug)
    os.makedirs(bk_sport_dir, exist_ok=True)

    comps     = _COMPETITIONS.get(sport_id or 0, [(0, "_sport_page", 0)])
    hardcoded = [(li, slug) for li, slug, _ in comps if li > 0]
    sport_only = not hardcoded

    ctx_opts: dict = {
        "viewport":            {"width": 1366, "height": 900},
        "user_agent":          (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
        ),
        "ignore_https_errors": True,
    }
    if storage_f.exists():
        try: ctx_opts["storage_state"] = str(storage_f)
        except Exception: pass

    all_matches:     list[dict] = []
    seen_gids:       set        = set()
    detail_payloads: dict[int, dict] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--disable-blink-features=AutomationControlled"],
        )
        ctx  = await browser.new_context(**ctx_opts)
        page = await ctx.new_page()

        pending: dict = {"body": None}

        async def _on_resp(resp):
            url = resp.url
            print(f"    [NET:{bk['slug']}] {resp.status} {url[:110]}")
            if "Get1x2_VZip" in url and resp.status == 200:
                try:
                    body = await resp.json()
                    err  = body.get("ErrorCode") if body else "none"
                    cnt  = len(body.get("Value") or []) if body else 0
                    print(f"    [API:{bk['slug']}] ✅ Get1x2_VZip ErrorCode={err} items={cnt}")
                    if body and body.get("ErrorCode") in (0, ""):
                        pending["body"] = body
                    else:
                        print(f"    [API:{bk['slug']}] ❌ bad body={str(body)[:200]}")
                except Exception as exc:
                    print(f"    [API:{bk['slug']}] ❌ json error: {exc}")
            elif "Get1x2_VZip" in url:
                print(f"    [API:{bk['slug']}] ⚠️  status={resp.status}")
            elif "GetGameZip" in url and resp.status == 200:
                try:
                    body = await resp.json()
                    if body and body.get("ErrorCode") in (0, ""):
                        m = _re.search(r'[?&]id=(\d+)', url)
                        if m: detail_payloads[int(m.group(1))] = body
                except Exception: pass

        page.on("response", _on_resp)

        def _collect_and_reset() -> list[dict]:
            body = pending.get("body")
            pending["body"] = None
            if not body: return []
            return _parse_value(body.get("Value") or [], bk, sport_slug, mode, sport_id)

        def _add_matches(new_matches: list[dict]):
            for m in new_matches:
                try: gid_int = int(m.get("external_id", ""))
                except: gid_int = None
                if gid_int and gid_int in seen_gids: continue
                if gid_int: seen_gids.add(gid_int)
                all_matches.append(m)

        def _save_comp(filename: str, raw_body: dict | None, matches: list[dict]):
            """Save raw API response + parsed matches into per-competition file."""
            path = os.path.join(bk_sport_dir, filename)
            payload = {
                "bk":          bk["slug"],
                "sport":       sport_slug,
                "competition": filename.replace(".json", ""),
                "match_count": len(matches),
                "raw_value":   raw_body.get("Value") if raw_body else [],
                "matches":     matches,
            }
            with open(path, "w") as f:
                json.dump(payload, f, indent=2, default=str)
            print(f"    [SAVE:{bk['slug']}] {bk_sport_dir}/{filename} ({len(matches)} matches)")

        async def _navigate_and_collect(url: str, save_as: str) -> int:
            """Navigate, wait for API, parse, save file, return match count added."""
            pending["body"] = None
            try:
                await page.goto(url, wait_until="domcontentloaded",
                                timeout=wait_s * 1000)
            except Exception as exc:
                print(f"    [NAV:{bk['slug']}] ❌ {url[-70:]}: {exc}")
                return 0

            deadline = time.perf_counter() + wait_s
            while time.perf_counter() < deadline:
                await asyncio.sleep(0.5)
                if pending["body"]: break

            raw_body = pending.get("body")
            pending["body"] = None
            if not raw_body:
                print(f"    [TIMEOUT:{bk['slug']}] no response for {url[-70:]}")
                return 0

            new_matches = _parse_value(
                raw_body.get("Value") or [], bk, sport_slug, mode, sport_id
            )
            before = len(all_matches)
            _add_matches(new_matches)
            added = len(all_matches) - before

            _save_comp(save_as, raw_body, new_matches)
            return added

        # ── Step 1: sport root page ───────────────────────────────────────────
        sport_root = f"{bk['base']}/en/{feed_path}/{page_slug}"
        print(f"  [{bk['slug']}] ── {sport_root}")
        added = await _navigate_and_collect(sport_root, "_sports_root.json")
        print(f"  [{bk['slug']}] root → {added} (total={len(all_matches)})")

        # ── Step 2: each competition page ─────────────────────────────────────
        if not sport_only:
            print(f"  [{bk['slug']}] fetching {len(hardcoded)} competitions…")
            for i, (li, comp_slug) in enumerate(hardcoded, 1):
                comp_url  = f"{bk['base']}/en/{feed_path}/{page_slug}/{li}-{comp_slug}"
                save_name = f"{comp_slug}.json"
                added     = await _navigate_and_collect(comp_url, save_name)
                print(f"  [{bk['slug']}] ({i:>2}/{len(hardcoded)}) {comp_slug}: "
                      f"+{added} → total={len(all_matches)}")

        print(f"  [{bk['slug']}] {sport_slug}: {len(all_matches)} matches total")

        # ── Step 3: GetGameZip full markets ───────────────────────────────────
        if full_markets and all_matches:
            p        = bk["partner_id"]
            game_ids = []
            for m in all_matches:
                try: game_ids.append(int(m["external_id"]))
                except: pass

            try:
                await page.goto(sport_root, wait_until="domcontentloaded",
                                timeout=wait_s * 1000)
                await asyncio.sleep(2)
            except Exception: pass

            enriched = 0
            for i in range(0, len(game_ids), 8):
                batch   = game_ids[i:i+8]
                fetches = ",\n".join(
                    f'fetch("https://{bk["domain"]}/service-api/{feed_api}/GetGameZip'
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
                        if val and isinstance(val, dict):
                            gid = val.get("I") or val.get("Id")
                            if gid: detail_payloads[int(gid)] = rb
                except Exception as exc:
                    logger.debug("[pw:%s] GetGameZip: %s", bk["slug"], exc)
                await asyncio.sleep(0.2)

            for m in all_matches:
                try: gid_int = int(m.get("external_id", ""))
                except: continue
                detail = detail_payloads.get(gid_int)
                if not detail: continue
                full_mkts = _parse_game_zip(detail)
                if full_mkts:
                    m["markets"]      = full_mkts
                    m["market_count"] = len(full_mkts)
                    enriched += 1

            avg = int(sum(m.get("market_count",0) for m in all_matches)/len(all_matches)) if all_matches else 0
            print(f"  [{bk['slug']}] enriched {enriched}/{len(all_matches)}, avg {avg} markets")

            # Save enriched matches per competition back to files
            comp_buckets: dict[str, list[dict]] = {}
            for m in all_matches:
                comp = m.get("competition", "unknown")
                comp_buckets.setdefault(comp, []).append(m)
            for comp_name, comp_matches in comp_buckets.items():
                safe = re.sub(r'[^\w\-]', '-', comp_name.lower().strip()).strip('-') or "misc"
                path = os.path.join(bk_sport_dir, f"{safe}_enriched.json")
                with open(path, "w") as f:
                    json.dump(comp_matches, f, indent=2, default=str)

        # ── Save cookies ──────────────────────────────────────────────────────
        try:
            state = await ctx.storage_state()
            with open(storage_f, "w") as f: json.dump(state, f)
        except Exception: pass

        await ctx.close()
        await browser.close()

    return all_matches



async def _pw_harvest_sport(
    bks:          list[dict],
    sport_slug:   str,
    mode:         str  = "upcoming",
    headless:     bool = True,
    wait_s:       int  = 15,
    full_markets: bool = True,
    output_dir:   str  = "harvest_dumps",
    tab_concurrency: int = 5,
) -> dict[str, list[dict]]:
    """Run _harvest_one_bk for all BKs concurrently (one browser per BK)."""
    tasks = [
        asyncio.create_task(
            _harvest_one_bk(bk, sport_slug, mode, headless,
                            wait_s, full_markets, output_dir)
        )
        for bk in bks
    ]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    results: dict[str, list[dict]] = {}
    for bk, outcome in zip(bks, outcomes):
        if isinstance(outcome, Exception):
            logger.error("[pw:%s] error: %s", bk["slug"], outcome)
            results[bk["slug"]] = []
        else:
            results[bk["slug"]] = outcome
    return results

# =============================================================================
# PUBLIC API
# =============================================================================

def fetch_bk_sport(bk, sport_slug, mode="upcoming", headless=True,
                   wait_s=15, full_markets=True, verbose=True,
                   output_dir="harvest_dumps"):
    t0 = time.perf_counter()
    matches = asyncio.run(
        _harvest_one_bk(bk, sport_slug, mode, headless, wait_s, full_markets, output_dir)
    )
    ms = int((time.perf_counter() - t0) * 1000)
    if verbose:
        s = "✅" if matches else "⚠ "
        print(f"  {s} {bk['slug']:<12} {sport_slug:<16} {mode:<9} — {len(matches):4} ({ms}ms)")
    return matches


def fetch_sport_all_bks(sport_slug, mode="upcoming", bookmakers=None,
                         headless=True, wait_s=15, max_matches=1500,
                         full_markets=True, verbose=True,
                         output_dir="harvest_dumps"):
    bks = bookmakers or B2B_BOOKMAKERS
    t0  = time.perf_counter()
    if verbose:
        print(f"\n  {len(bks)} BKs × {sport_slug} [{mode}] concurrently…")
    res = asyncio.run(
        _pw_harvest_sport(bks, sport_slug, mode, headless, wait_s, full_markets, output_dir)
    )
    if verbose:
        total = sum(len(v) for v in res.values())
        for slug, ms in sorted(res.items()):
            s = "✅" if ms else "⚠ "
            avg = int(sum(m.get("market_count",0) for m in ms) / len(ms)) if ms else 0
            print(f"  {s} {slug:<12} {len(ms):4} matches  avg {avg} mkts")
        print(f"  → {total} total ({int((time.perf_counter()-t0)*1000)}ms)")
    return res


def harvest_all_b2b(mode="upcoming", sports=None, bookmakers=None,
                     bk_workers=7, headless=True, wait_s=15,
                     max_matches=1500, full_markets=True, verbose=True,
                     output_dir="harvest_dumps"):
    bks    = bookmakers or B2B_BOOKMAKERS
    sports = sports or ALL_SPORT_SLUGS
    if verbose:
        print(f"\n{'═'*65}")
        print(f"B2B Playwright [{mode.upper()}] {len(bks)} BKs × {len(sports)} sports")
        print(f"{'═'*65}")
    results = {bk["slug"]: {} for bk in bks}
    for sp in sports:
        if verbose: print(f"\n  ── {sp.upper()} ──")
        per_bk = asyncio.run(
            _pw_harvest_sport(bks, sp, mode, headless, wait_s, full_markets, output_dir)
        )
        for bk in bks:
            results[bk["slug"]][sp] = per_bk.get(bk["slug"], [])
    return results


def merge_b2b(all_results, sport_slug):
    unified = []; key_idx = {}
    for bk_slug, sport_data in all_results.items():
        for m in sport_data.get(sport_slug, []):
            home  = m.get("home_team","").lower().strip()
            away  = m.get("away_team","").lower().strip()
            start = (m.get("start_time") or "")[:16]
            key   = f"{home}|||{away}|||{start}"
            if key in key_idx:
                ex = unified[key_idx[key]]
                bi = (m.get("bookmakers") or {}).get(bk_slug) or {}
                if bi.get("markets"):
                    ex["bookmakers"][bk_slug] = bi
                    for mkt,outs in bi["markets"].items():
                        em = ex["markets"].setdefault(mkt, {})
                        for out,price in outs.items():
                            if price > em.get(out, 0.0): em[out] = price
                    ex["market_count"] = len(ex["markets"])
            else:
                entry = {**m, "bk_count":1,
                         "bookmakers": dict(m.get("bookmakers") or {}),
                         "markets":    dict(m.get("markets") or {})}
                key_idx[key] = len(unified); unified.append(entry)
    return unified


def merge_b2b_by_match(per_bk, sport_slug):
    return merge_b2b({bk:{sport_slug:ms} for bk,ms in per_bk.items()}, sport_slug)


def print_raw_sample(bk, sport_slug="soccer", mode="upcoming"):
    print(f"\n🎭 {bk['slug']} / {sport_slug}")
    matches = fetch_bk_sport(bk, sport_slug, mode, True, 15, True, False)
    if not matches: print("⚠  No matches"); return
    best = max(matches, key=lambda m: m.get("market_count", 0))
    print(f"Match: {best['home_team']} vs {best['away_team']}  |  {best.get('competition')}")
    print(f"Markets ({best['market_count']}):")
    for mkt, outcomes in sorted(best.get("markets", {}).items()):
        print(f"  {mkt:<28} " + "  ".join(f"{o}={p:.2f}" for o,p in sorted(outcomes.items())))


def print_sample_per_sport(all_results, sport_filter=None):
    for sp in ALL_SPORT_SLUGS:
        if sport_filter and sp != sport_filter: continue
        merged = merge_b2b(all_results, sp)
        if not merged: print(f"  {sp:<18} — no matches"); continue
        best = max(merged, key=lambda m: m.get("market_count", 0))
        print(f"\n{sp.upper()}: {best['home_team']} vs {best['away_team']}")
        for mkt, outcomes in sorted(best.get("markets", {}).items()):
            print(f"  {mkt:<28} " + "  ".join(f"{o}={p:.2f}" for o,p in sorted(outcomes.items())))


def fetch_sports_tree(bk=None, verbose=True):
    tree = {}
    for sp in ALL_SPORT_SLUGS:
        sid   = B2B_SPORT_IDS.get(sp)
        comps = [c for c in _COMPETITIONS.get(sid or 0, []) if c[0] > 0]
        total = sum(gc for _,_,gc in comps)
        tree[sp] = {"id": sid, "competitions": len(comps), "count": total}
    if verbose:
        print(f"\n{'─'*55}\n{'Sport':<22}{'ID':>4}  {'Comps':>6}  {'~Games':>7}\n{'─'*55}")
        for sp, d in sorted(tree.items(), key=lambda x: -x[1]["count"]):
            print(f"  {sp:<20} {d['id']:>4}  {d['competitions']:>6}  {d['count']:>7}")
    return tree


def _save_sport_to_redis(sport, mode, matches, bk_slug="b2b"):
    try:
        from app.workers.redis_bus import publish_snapshot
        publish_snapshot(bk_slug, mode, sport, matches)
    except Exception as exc:
        logger.warning("[pw] Redis: %s", exc)


def _save_results_to_redis(all_results, mode="upcoming"):
    seen = set()
    for bk_slug, sport_data in all_results.items():
        for sport, matches in sport_data.items():
            if matches: _save_sport_to_redis(sport, mode, matches, bk_slug); seen.add(sport)
    for sport in seen:
        merged = merge_b2b_by_match({bk:d.get(sport,[]) for bk,d in all_results.items()}, sport)
        if merged: _save_sport_to_redis(sport, mode, merged, "b2b")


if __name__ == "__main__":
    import sys, logging as _log
    _log.basicConfig(level=_log.INFO)
    bk_slug = sys.argv[1] if len(sys.argv) > 1 else "betwinner"
    sport   = sys.argv[2] if len(sys.argv) > 2 else "soccer"
    hl      = sys.argv[3] != "false" if len(sys.argv) > 3 else True
    bk = _BK_BY_SLUG.get(bk_slug, B2B_BOOKMAKERS[0])
    print(f"\n🎭 {bk['slug']} / {sport} / headless={hl}")
    ms = fetch_bk_sport(bk, sport, "upcoming", hl, 15, full_markets=True)
    print(f"✅ {len(ms)} matches")
    if ms:
        best = max(ms, key=lambda m: m.get("market_count", 0))
        import pprint; pprint.pprint(best)