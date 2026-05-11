"""
app/workers/b2b_harvester.py
=============================
BetB2B family harvester — 7 Kenya bookmakers on the same platform.

CONFIRMED WORKING (from real browser DevTools, May 2026)
  URL param: sports=4 (PLURAL not sport=)
  Multiple:  sports=3,4 (comma-separated)
  Required:  getEmpty=true&noFilterBlockEvent=true
  count=500 gets all matches for a sport in one request

CORRECT PARTNER IDs (verified May 2026)
  1xBet:     1xbet.co.ke      partner=61  gr=657
  22Bet:     22bet.co.ke      partner=151 gr=515
  Betwinner: betwinner.ke     partner=152
  Melbet:    mel-bet.co.ke    partner=417
  Megapari:  1849932mp.pro    partner=192
  Helabet:   helabetke.com    partner=237
  Paripesa:  paripesa.cool    partner=188
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
# BOOKMAKER REGISTRY
# =============================================================================

B2B_BOOKMAKERS: list[dict] = [
    {"slug":"1xbet",    "name":"1xBet",    "domain":"1xbet.co.ke",   "partner_id":61,  "gr":657,  "color":"#1F8AEB"},
    {"slug":"22bet",    "name":"22Bet",    "domain":"22bet.co.ke",   "partner_id":151, "gr":515,  "color":"#0B2133"},
    {"slug":"betwinner","name":"Betwinner","domain":"betwinner.ke",  "partner_id":152, "gr":None, "color":"#FF6600"},
    {"slug":"melbet",   "name":"Melbet",   "domain":"mel-bet.co.ke", "partner_id":417, "gr":None, "color":"#FF0000"},
    {"slug":"megapari", "name":"Megapari", "domain":"1849932mp.pro", "partner_id":192, "gr":None, "color":"#7B2FBE"},
    {"slug":"helabet",  "name":"Helabet",  "domain":"helabetke.com", "partner_id":237, "gr":None, "color":"#9C27B0"},
    {"slug":"paripesa", "name":"Paripesa", "domain":"paripesa.cool", "partner_id":188, "gr":None, "color":"#FF6B35"},
]

_BK_BY_SLUG: dict[str, dict] = {b["slug"]: b for b in B2B_BOOKMAKERS}

# =============================================================================
# SPORT ID MAP  (same across all BetB2B — shared platform)
# =============================================================================

B2B_SPORT_IDS: dict[str, int] = {
    "soccer":1,"football":1,
    "esoccer":40,"efootball":40,"e-football":40,"virtual-football":40,
    "basketball":3,
    "tennis":4,
    "table-tennis":10,"tabletennis":10,
    "ice-hockey":2,"icehockey":2,
    "volleyball":6,
    "handball":8,
    "baseball":5,
    "american-football":13,"americanfootball":13,"nfl":13,
    "rugby":7,"rugby-league":7,"rugby-union":7,
    "boxing":9,
    "mma":56,"ufc":189,
    "cricket":66,
    "darts":21,
    "golf":41,
    "futsal":14,
    "snooker":30,
    "squash":39,
}

_ID_TO_SLUG: dict[int, str] = {
    1:"soccer",2:"ice-hockey",3:"basketball",4:"tennis",5:"baseball",
    6:"volleyball",7:"rugby",8:"handball",9:"boxing",10:"table-tennis",
    13:"american-football",14:"futsal",21:"darts",30:"snooker",
    39:"squash",40:"esoccer",41:"golf",56:"mma",66:"cricket",189:"ufc",
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
    1:{1:"1",2:"X",3:"2"},
    2:{7:"1",8:"2"},
    8:{4:"1X",5:"12",6:"X2"},
    15:{11:"Yes",12:"No"},
    17:{9:"Over",10:"Under"},
    19:{180:"Over",181:"Under"},
    62:{13:"1",14:"2"},
    99:{3827:"Over",3828:"Under"},
    2854:{3829:"1",3830:"2"},
}

# =============================================================================
# HTTP
# =============================================================================

# Per-bookmaker session cookies — paste fresh values when harvesting fails.
# Get them from browser DevTools → Application → Cookies after visiting the site.
# Only SESSION and sh.session.id are strictly needed. They expire in ~24h.
_BK_COOKIES: dict[str, str] = {
    "1xbet":     "",   # sh.session.id=...; SESSION=...
    "22bet":     "",
    "betwinner": "",
    "melbet":    "",
    "megapari":  "",
    "helabet":   "",
    "paripesa":  "",
}

_BASE_HEADERS: dict[str, str] = {
    "accept":              "application/json, text/plain, */*",
    "accept-language":     "en-GB,en-US;q=0.9,en;q=0.8",
    "content-type":        "application/json",
    "sec-ch-ua":           '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile":    "?0",
    "sec-ch-ua-platform":  '"macOS"',
    "sec-fetch-dest":      "empty",
    "sec-fetch-mode":      "cors",
    "sec-fetch-site":      "same-origin",
    "user-agent":          (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
    ),
    "x-requested-with":    "XMLHttpRequest",
    "is-srv":              "false",
    "x-app-n":             "__BETTING_APP__",
    "x-mobile-project-id": "0",
    "x-svc-source":        "__BETTING_APP__",
}


def _curl(url: str, referer: str, cookie: str = "", timeout: int = 25) -> dict | None:
    """
    Execute request via system curl, matching the working browser curl exactly.
    Cookie is passed via -b flag (same as working curl uses -b '...').
    """
    cmd = ["curl", "-s", "-g", f"-m{timeout}"]
    for k, v in _BASE_HEADERS.items():
        cmd += ["-H", f"{k}: {v}"]
    cmd += ["-H", f"referer: {referer}"]
    if cookie:
        # -b sends as Cookie header, same as working curl
        cmd += ["-b", cookie]
    cmd += ["--", url]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if res.returncode != 0:
            logger.debug("[curl] exit=%d url=%s stderr=%s",
                         res.returncode, url, res.stderr.strip()[:200])
            return None
        body = res.stdout.strip()
        if not body:
            logger.debug("[curl] empty body url=%s", url)
            return None
        data = json.loads(body)
        err = data.get("ErrorCode")
        if err and err not in (0, "0", ""):
            logger.debug("[curl] API error=%s url=%s", err, url)
        return data
    except json.JSONDecodeError:
        snippet = res.stdout[:300] if res else ""
        logger.debug("[curl] JSON error url=%s body=%s", url, snippet)
        return None
    except Exception as exc:
        logger.debug("[curl] exception: %s url=%s", exc, url)
        return None


def _get_cookie(bk_slug: str) -> str:
    """Get session cookie for a bookmaker. Returns empty string if not set."""
    return _BK_COOKIES.get(bk_slug, "")

# =============================================================================
# URL BUILDERS  — sports= (plural), getEmpty, noFilterBlockEvent confirmed
# =============================================================================

def _line_url(bk:dict, *, count:int=50, champ:int|None=None,
              sport_id:int|None=None) -> str:
    """
    LineFeed URL.
    champ=LI fetches all matches for a specific competition (no x-hd needed).
    Falls back to unfiltered if neither given.
    NOTE: sports= filter requires x-hd HMAC token we can't generate.
    """
    p = bk["partner_id"]
    parts = [f"count={count}", "lng=en_GB", "mode=4",
             f"country=87", f"partner={p}", "getEmpty=true",
             "tz=3"]
    if champ:    parts.append(f"champ={champ}")
    return f"https://{bk['domain']}/service-api/LineFeed/Get1x2_VZip?{'&'.join(parts)}"


def _live_url(bk:dict, *, count:int=50, champ:int|None=None) -> str:
    """LiveFeed URL. champ= filters by competition."""
    p  = bk["partner_id"]
    gr = bk.get("gr")
    parts = [f"count={count}", "lng=en_GB", "mode=4",
             f"country=87", f"partner={p}", "getEmpty=true"]
    if gr:    parts.append(f"gr={gr}")
    if champ: parts.append(f"champ={champ}")
    return f"https://{bk['domain']}/service-api/LiveFeed/Get1x2_VZip?{'&'.join(parts)}"


def _sports_tree_url(bk:dict) -> str:
    """GetSportsShortZip — returns sport list with all competition LI IDs."""
    p  = bk["partner_id"]
    gr = bk.get("gr") or ""
    parts = ["lng=en_GB", f"country=87", f"partner={p}",
             "virtualSports=true", "groupChamps=true"]
    if gr: parts.append(f"gr={gr}")
    return f"https://{bk['domain']}/service-api/LineFeed/GetSportsShortZip?{'&'.join(parts)}"





def _sports_url(bk:dict) -> str:
    p=bk["partner_id"]; gr=bk.get("gr") or ""
    q=f"lng=en&country=87&partner={p}&virtualSports=true&groupChamps=true"
    if gr: q+=f"&gr={gr}"
    return f"https://{bk['domain']}/service-api/LineFeed/GetSportsShortZip?{q}"


def _referer(bk:dict, path:str="/en/line") -> str:
    return f"https://{bk['domain']}{path}"

# =============================================================================
# PARSERS
# =============================================================================

def _parse_events(events:list[dict], extra_events:list[dict]|None=None) -> dict:
    markets: dict[str, dict[str, float]] = defaultdict(dict)
    def _p(ev:dict):
        gid=ev.get("G"); t=ev.get("T"); c=ev.get("C") or ev.get("CV")
        if gid is None or t is None or c is None: return
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
    for ae in extra_events or []:
        if not isinstance(ae,dict): continue
        for me in ae.get("ME") or [ae]:
            if isinstance(me,dict): _p(me)
    return {k:v for k,v in markets.items() if v}


def _parse_game(game:dict, bk:dict, sport_slug:str, mode:str) -> dict|None:
    home=(game.get("O1E") or game.get("O1") or "").strip()
    away=(game.get("O2E") or game.get("O2") or "").strip()
    if not home or not away: return None
    try:
        from app.utils.mapping.b2b import normalize_b2b_markets
        markets=normalize_b2b_markets(sport_slug, list(game.get("E") or [])+list(game.get("AE") or []))
    except Exception:
        markets=_parse_events(game.get("E") or [], game.get("AE"))
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


def _extract_games(value:list, bk:dict, sport_slug:str, mode:str) -> list[dict]:
    sport_id=B2B_SPORT_IDS.get(sport_slug.lower())
    matches:list[dict]=[]
    for item in value:
        if not isinstance(item,dict): continue
        if "O1E" in item or "O1" in item:
            if sport_id and item.get("SI") not in (sport_id,None): continue
            m=_parse_game(item,bk,sport_slug,mode)
            if m: matches.append(m)
        elif "L" in item and isinstance(item.get("L"),list):
            if sport_id and item.get("I")!=sport_id: continue
            for country in item["L"]:
                for sc in country.get("SC") or []:
                    for game in sc.get("G") or []:
                        if isinstance(game,dict):
                            m=_parse_game(game,bk,sport_slug,mode)
                            if m: matches.append(m)
    return matches

# =============================================================================
# SINGLE BK + SINGLE SPORT
# =============================================================================

def fetch_bk_sport(bk:dict, sport_slug:str, mode:str="upcoming",
                   count:int=500, verbose:bool=True) -> list[dict]:
    """Fetch one sport from one BK using sports= filter."""
    slug=bk["slug"]
    sport_id=B2B_SPORT_IDS.get(sport_slug.lower())
    sids=[sport_id] if sport_id else None
    url=(_live_url(bk,count=count,sports_ids=sids) if mode=="live"
         else _line_url(bk,count=count,sports_ids=sids))
    ref=_referer(bk)
    t0=time.perf_counter()
    raw=_curl(url, ref, cookie=_get_cookie(slug))
    ms=int((time.perf_counter()-t0)*1000)
    if not raw or raw.get("ErrorCode",-1) not in (0,""):
        if verbose: print(f"  ❌ {slug:<12} {sport_slug:<16} — no response ({ms}ms)")
        return []
    matches=_extract_games(raw.get("Value") or [],bk,sport_slug,mode)
    if verbose:
        status="✅" if matches else "⚠ "
        print(f"  {status} {slug:<12} {sport_slug:<16} {mode:<9} — {len(matches):4} matches ({ms}ms)")
    return matches

# =============================================================================
# ALL SPORTS — batched concurrent with sports= filter
# =============================================================================

def _get_competitions(bk:dict, sport_id:int) -> list[dict]:
    """
    Fetch all competitions for a sport via GetSportsShortZip.
    Returns [{LI: comp_id, name: str, count: int}]
    No x-hd required — this endpoint works with just session cookie.
    """
    url = _sports_tree_url(bk)
    ref = _referer(bk)
    raw = _curl(url, ref, cookie=_get_cookie(bk["slug"]), timeout=20)
    if not raw: return []
    comps = []
    for item in raw.get("Value") or []:
        if not isinstance(item, dict): continue
        if item.get("I") != sport_id: continue
        # item has nested L[] of countries each with SC[] of competitions
        for country in item.get("L") or []:
            for sc in country.get("SC") or []:
                li = sc.get("LI") or sc.get("CI")
                if li:
                    comps.append({
                        "LI":   li,
                        "name": sc.get("LE") or sc.get("L") or "",
                        "count": sc.get("C") or sc.get("GC") or 0,
                    })
    return comps


def _fetch_competition(bk:dict, comp:dict, sport_slug:str, mode:str) -> list[dict]:
    """Fetch all matches for ONE competition using champ= filter."""
    champ = comp["LI"]
    url   = (_live_url(bk, count=200, champ=champ)
             if mode == "live"
             else _line_url(bk, count=200, champ=champ))
    ref   = _referer(bk)
    raw   = _curl(url, ref, cookie=_get_cookie(bk["slug"]), timeout=15)
    if not raw or raw.get("ErrorCode", -1) not in (0, ""): return []

    matches = []
    sid = B2B_SPORT_IDS.get(sport_slug.lower())
    for item in raw.get("Value") or []:
        if not isinstance(item, dict): continue
        if "O1E" in item or "O1" in item:
            m = _parse_game(item, bk, sport_slug, mode)
            if m: matches.append(m)
        elif "L" in item and isinstance(item.get("L"), list):
            if sid and item.get("I") not in (sid, None): continue
            for country in item["L"]:
                for sc in country.get("SC") or []:
                    for game in sc.get("G") or []:
                        if isinstance(game, dict):
                            m = _parse_game(game, bk, sport_slug, mode)
                            if m: matches.append(m)
    return matches


def fetch_bk_all_sports(bk:dict, mode:str="upcoming", sports:list[str]|None=None,
                         workers:int=8, verbose:bool=True) -> dict[str,list[dict]]:
    """
    Fetch ALL sports from ONE bookmaker using competition-based fetching.

    Flow (no x-hd required):
      1. GetSportsShortZip → get all competitions per sport (with LI ids)
      2. Get1x2_VZip?champ={LI} → fetch all matches per competition

    This bypasses the ~50-match global cap and doesn't need the x-hd token.
    All competition requests run concurrently.
    """
    sports  = sports or ALL_SPORT_SLUGS
    slug    = bk["slug"]
    results = {s: [] for s in sports}

    # Deduplicate by sport_id
    seen_ids:   set[int]       = set()
    id_to_slug: dict[int, str] = {}
    for sp in sports:
        sid = B2B_SPORT_IDS.get(sp.lower())
        if sid is not None and sid not in seen_ids:
            seen_ids.add(sid)
            id_to_slug[sid] = sp

    if verbose:
        print(f"\n  [{slug}] fetching {len(id_to_slug)} sports via competitions…")

    t0    = time.perf_counter()
    total = 0

    def _fetch_sport(sport_slug:str, sport_id:int) -> tuple[str, list[dict]]:
        comps = _get_competitions(bk, sport_id)
        if not comps:
            if verbose: print(f"    {sport_slug}: no competitions found, trying direct fetch")
            # Fallback: try unfiltered request
            url = _live_url(bk) if mode == "live" else _line_url(bk)
            ref = _referer(bk)
            raw = _curl(url, ref, cookie=_get_cookie(slug), timeout=20)
            if not raw or raw.get("ErrorCode", -1) not in (0, ""): return sport_slug, []
            matches = []
            for item in raw.get("Value") or []:
                if not isinstance(item, dict): continue
                if item.get("SI") == sport_id and ("O1E" in item or "O1" in item):
                    m = _parse_game(item, bk, sport_slug, mode)
                    if m: matches.append(m)
            return sport_slug, matches

        if verbose:
            print(f"    {sport_slug}: {len(comps)} competitions")

        # Fetch all competitions concurrently
        all_matches: list[dict] = []
        seen_gids:   set        = set()

        with ThreadPoolExecutor(max_workers=min(workers, len(comps)),
                                 thread_name_prefix=f"b2b-{slug}-{sport_slug}") as pool:
            futures = [pool.submit(_fetch_competition, bk, c, sport_slug, mode)
                       for c in comps]
            for fut in as_completed(futures):
                try:
                    for m in fut.result():
                        gid = m.get("external_id")
                        if gid and gid in seen_gids: continue
                        if gid: seen_gids.add(gid)
                        all_matches.append(m)
                except Exception as exc:
                    logger.debug("[b2b:%s/%s] comp error: %s", slug, sport_slug, exc)

        return sport_slug, all_matches

    # Fetch all sports concurrently
    with ThreadPoolExecutor(max_workers=min(workers, len(id_to_slug)),
                             thread_name_prefix=f"b2b-{slug}") as pool:
        sport_futures = {
            pool.submit(_fetch_sport, sp, sid): (sp, sid)
            for sp, sid in id_to_slug.items()
        }
        for fut in as_completed(sport_futures):
            sp, sid = sport_futures[fut]
            try:
                sport_slug, matches = fut.result()
                results[sport_slug] = matches
                # Mirror to aliases (rugby/rugby-league share id)
                for alias in sports:
                    if alias != sport_slug and B2B_SPORT_IDS.get(alias) == sid:
                        results[alias] = matches
                if matches and verbose:
                    print(f"    {sport_slug:<18} {len(matches):4} matches")
                total += len(matches)
            except Exception as exc:
                logger.error("[b2b:%s/%s] unhandled: %s", slug, sp, exc)

    elapsed = int((time.perf_counter() - t0) * 1000)
    if verbose:
        print(f"  → {slug}: {total} total matches ({elapsed}ms)")
    return results


def harvest_all_b2b(mode:str="upcoming", sports:list[str]|None=None,
                     bookmakers:list[dict]|None=None, bk_workers:int=7,
                     verbose:bool=True) -> dict[str,dict[str,list[dict]]]:
    """Fetch ALL bookmakers × ALL sports concurrently.
    Returns { bk_slug: { sport_slug: [match,...] } }"""
    bks=bookmakers or B2B_BOOKMAKERS; sports=sports or ALL_SPORT_SLUGS
    if verbose:
        print(f"\n{'═'*65}")
        print(f"B2B Harvest: {len(bks)} bookmakers × {len(sports)} sports [{mode}]")
        print(f"{'═'*65}")
    results:dict[str,dict[str,list[dict]]]={}
    with ThreadPoolExecutor(max_workers=min(bk_workers,len(bks)),
                             thread_name_prefix="b2b") as pool:
        futures={pool.submit(fetch_bk_all_sports,bk,mode,sports,verbose=verbose):bk
                 for bk in bks}
        for fut in as_completed(futures):
            bk=futures[fut]
            try: results[bk["slug"]]=fut.result()
            except Exception as exc:
                logger.error("[b2b:%s] unhandled: %s",bk["slug"],exc)
                results[bk["slug"]]={s:[] for s in sports}
    return results

# =============================================================================
# MERGE
# =============================================================================

def merge_b2b(all_results:dict[str,dict[str,list[dict]]], sport_slug:str) -> list[dict]:
    unified:list[dict]=[]; key_idx:dict[str,int]={}
    for bk_slug,sport_data in all_results.items():
        for m in sport_data.get(sport_slug,[]):
            home=m.get("home_team","").lower().strip()
            away=m.get("away_team","").lower().strip()
            start=(m.get("start_time") or "")[:16]
            key=f"{home}|||{away}|||{start}"
            if key in key_idx:
                ex=unified[key_idx[key]]
                bi=m["bookmakers"].get(bk_slug) or {}
                if bi.get("markets"):
                    ex["bookmakers"][bk_slug]=bi
                    for mkt,outs in bi["markets"].items():
                        em=ex["markets"].setdefault(mkt,{})
                        for out,price in outs.items():
                            if price>em.get(out,0.0): em[out]=price
                    ex["market_count"]=len(ex["markets"])
                    ex["bk_count"]=len(ex["bookmakers"])
            else:
                entry={**m,"bk_count":1,
                       "bookmakers":dict(m.get("bookmakers") or {}),
                       "markets":dict(m.get("markets") or {})}
                key_idx[key]=len(unified); unified.append(entry)
    return unified


def merge_b2b_by_match(per_bk:dict[str,list[dict]], sport_slug:str) -> list[dict]:
    """Alias: merge {bk_slug: [matches]} for one sport."""
    return merge_b2b({bk:{sport_slug:ms} for bk,ms in per_bk.items()}, sport_slug)

# =============================================================================
# SPORTS TREE
# =============================================================================

def fetch_sports_tree(bk:dict|None=None, verbose:bool=True) -> dict:
    """Fetch sport list with match counts from GetSportsShortZip."""
    bk=bk or B2B_BOOKMAKERS[0]
    raw=_curl(_sports_url(bk), _referer(bk), cookie=_get_cookie(bk["slug"]))
    if not raw: return {}
    tree:dict={}
    for item in raw.get("Value") or []:
        if not isinstance(item,dict): continue
        if item.get("CID",0) not in (1,2): continue
        name=item.get("N") or f"sport_{item.get('I')}"
        sid=item.get("I"); count=item.get("C",0)
        if name not in tree or tree[name]["count"]<count:
            tree[name]={"id":sid,"count":count}
    if verbose:
        print(f"\n{'─'*50}\n{'Sport':<25}{'ID':>5}  {'Matches':>7}\n{'─'*50}")
        for name,data in sorted(tree.items(),key=lambda x:-x[1]["count"]):
            print(f"  {name:<23} {data['id']:>4}  {data['count']:>7}")
    return tree

# =============================================================================
# SAMPLE PRINTERS
# =============================================================================

def print_raw_sample(bk:dict, sport_slug:str="soccer", mode:str="upcoming") -> None:
    """Print raw E[] G/T values for one match — for building market mappers."""
    sport_id=B2B_SPORT_IDS.get(sport_slug.lower())
    sids=[sport_id] if sport_id else None
    url=(_live_url(bk,count=50,sports_ids=sids) if mode=="live"
         else _line_url(bk,count=50,sports_ids=sids))
    print(f"\nFetching: {bk['slug']} / {sport_slug} / {mode}")
    print(f"URL: {url}\n")
    raw=_curl(url, _referer(bk), cookie=_get_cookie(bk["slug"]))
    if not raw: print("❌ No response"); return
    value=raw.get("Value") or []
    sample=None
    for item in value:
        if not isinstance(item,dict): continue
        if "O1E" in item or "O1" in item:
            if sport_id is None or item.get("SI")==sport_id:
                sample=item; break
    if not sample:
        sis={v.get("SI") for v in value if isinstance(v,dict)}
        print(f"⚠  No {sport_slug} match. Available SI values: {sorted(sis)}")
        return
    print(f"Match:  {sample.get('O1E')} vs {sample.get('O2E')}")
    print(f"Comp:   {sample.get('LE')}")
    print(f"SI:     {sample.get('SI')}")
    print(f"\nE[] grouped by G:\n")
    by_group:dict[int,list]=defaultdict(list)
    for ev in sample.get("E") or []: by_group[ev.get("G",0)].append(ev)
    for gid,evs in sorted(by_group.items()):
        slug=_GROUP_TO_SLUG.get(gid,f"group_{gid}  ← UNMAPPED")
        print(f"  G={gid:<5} → {slug}")
        for ev in evs:
            t,c,p=ev.get("T"),ev.get("C"),ev.get("P")
            label=_T_LABELS.get(gid,{}).get(t,f"T{t}  ← unmapped")
            line=f"  @{p}" if p is not None else ""
            print(f"    T={t:<6} {str(label):<20} C={c}{line}")
    print()


def print_sample_per_sport(all_results:dict, sport_filter:str|None=None) -> None:
    """Print one sample match per sport with all markets."""
    print(f"\n{'═'*70}\nSAMPLE MATCHES — one per sport\n{'═'*70}\n")
    for sport_slug in ALL_SPORT_SLUGS:
        if sport_filter and sport_slug!=sport_filter: continue
        merged=merge_b2b(all_results,sport_slug)
        if not merged: print(f"  {sport_slug:<18} — no matches"); continue
        best=max(merged,key=lambda m:m.get("market_count",0))
        print(f"{'─'*70}")
        print(f"  SPORT: {sport_slug.upper()}")
        print(f"  MATCH: {best['home_team']} vs {best['away_team']}")
        print(f"  COMP:  {best.get('competition','?')}")
        print(f"  BKS:   {', '.join(best.get('bookmakers',{}).keys())}")
        print(f"  MARKETS ({best.get('market_count',0)}):")
        for mkt,outcomes in sorted(best.get("markets",{}).items()):
            out_str="  ".join(f"{o}={p:.2f}" for o,p in sorted(outcomes.items()))
            print(f"    {mkt:<28} {out_str}")
        print()

# =============================================================================
# PUBLIC ALIASES
# =============================================================================

def fetch_single_bk(bk:dict, sport_slug:str, mode:str="upcoming",
                     page:int=1, page_size:int=500, output_dir:str="",
                     verbose:bool=True) -> list[dict]:
    """Alias for Celery tasks + CLI."""
    matches=fetch_bk_sport(bk,sport_slug,mode,count=page_size,verbose=verbose)
    start=(page-1)*page_size; sliced=matches[start:start+page_size]
    if output_dir and matches: _save_bk_file(output_dir,bk["slug"],sport_slug,mode,matches)
    return sliced


def get_bk_by_slug(slug:str) -> dict|None: return _BK_BY_SLUG.get(slug)


def _save_bk_file(output_dir:str, bk_slug:str, sport:str, mode:str, matches:list) -> None:
    os.makedirs(output_dir,exist_ok=True)
    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    path=os.path.join(output_dir,f"b2b_{bk_slug}_{sport}_{mode}_{ts}.json")
    with open(path,"w") as f: json.dump(matches,f,indent=2,default=str)


def _save_sport_to_redis(sport:str, mode:str, matches:list, bk_slug:str="b2b") -> None:
    try:
        from app.workers.redis_bus import publish_snapshot
        publish_snapshot(bk_slug,mode,sport,matches)
        logger.info("[b2b] Redis: %d → %s/%s/%s",len(matches),bk_slug,mode,sport)
    except Exception as exc:
        logger.warning("[b2b] Redis save failed: %s",exc)


def _save_results_to_redis(all_results:dict, mode:str="upcoming") -> None:
    sports_seen:set[str]=set()
    for bk_slug,sport_data in all_results.items():
        for sport,matches in sport_data.items():
            if matches: _save_sport_to_redis(sport,mode,matches,bk_slug=bk_slug); sports_seen.add(sport)
    for sport in sports_seen:
        per_bk={bk:data.get(sport,[]) for bk,data in all_results.items()}
        merged=merge_b2b_by_match(per_bk,sport)
        if merged: _save_sport_to_redis(sport,mode,merged,bk_slug="b2b")

# =============================================================================
# FLASK CLI
# =============================================================================

def register_cli(flask_app) -> None:
    import click, traceback as _tb

    @flask_app.cli.command("harvest-b2b")
    @click.option("--mode",default="upcoming",type=click.Choice(["upcoming","live"]))
    @click.option("--sport",default=None)
    @click.option("--bk",default=None)
    @click.option("--sample",is_flag=True)
    @click.option("--raw",is_flag=True)
    @click.option("--sports-tree",is_flag=True)
    @click.option("--save",is_flag=True)
    @click.option("--output-dir",default="harvest_dumps")
    def harvest_b2b_cmd(mode,sport,bk,sample,raw,sports_tree,save,output_dir):
        """Harvest B2B bookmakers (all sports + BKs concurrently)."""
        if sports_tree: click.echo("\n📋 Sports tree:"); fetch_sports_tree(verbose=True); return
        bks=[_BK_BY_SLUG[bk]] if bk else B2B_BOOKMAKERS
        sports=[sport] if sport else ALL_SPORT_SLUGS
        if raw: print_raw_sample(bks[0],sports[0],mode); return
        all_results=harvest_all_b2b(mode=mode,sports=sports,bookmakers=bks,verbose=True)
        if sample: print_sample_per_sport(all_results,sport_filter=sport); return
        if save: _save_results_to_redis(all_results,mode)
        os.makedirs(output_dir,exist_ok=True)
        ts=datetime.now().strftime("%Y%m%d_%H%M%S")
        for bk_slug,sport_data in all_results.items():
            for sp,ms in sport_data.items():
                if ms:
                    path=os.path.join(output_dir,f"b2b_{bk_slug}_{sp}_{mode}_{ts}.json")
                    with open(path,"w") as f: json.dump(ms,f,indent=2,default=str)
        click.echo(f"\n✅ Files saved to {output_dir}/")

    @flask_app.cli.command("harvest-b2b-all")
    @click.option("--output-dir",default="harvest_dumps")
    @click.option("--sport",default=None)
    @click.option("--debug",is_flag=True)
    def harvest_b2b_all(output_dir,sport,debug):
        """Fetch all B2B bookmakers. Saves files + pushes to Redis."""
        import logging as _log
        if debug: _log.getLogger("app.workers.b2b_harvester").setLevel(_log.DEBUG)
        os.makedirs(output_dir,exist_ok=True)
        timestamp=datetime.now().strftime("%Y%m%d_%H%M%S")
        sports=[sport] if sport else B2B_SUPPORTED_SPORTS
        errors:dict[str,str]={}
        click.echo(f"\n🚀 B2B — {len(B2B_BOOKMAKERS)} bookmakers × {len(sports)} sports")
        for s in sports:
            click.echo(f"\n{'─'*60}\nSport: {s.upper()}")
            per_bk:dict[str,list[dict]]={}
            for bk in B2B_BOOKMAKERS:
                try:
                    matches=fetch_single_bk(bk,s,mode="upcoming",page=1,
                                            page_size=500,output_dir=output_dir,verbose=True)
                    per_bk[bk["slug"]]=matches
                    with open(os.path.join(output_dir,f"b2b_{bk['slug']}_{s}_{timestamp}.json"),"w") as f:
                        json.dump(matches,f,indent=2,default=str)
                except Exception as e:
                    _tb.print_exc(); errors[f"{bk['slug']}/{s}"]=str(e); per_bk[bk["slug"]]=[]
            merged=merge_b2b_by_match(per_bk,s)
            with open(os.path.join(output_dir,f"b2b_unified_{s}_{timestamp}.json"),"w") as f:
                json.dump(merged,f,indent=2,default=str)
            click.echo(f"\n  🔗 Unified {s}: {len(merged)} matches")
            if merged:
                _save_sport_to_redis(s,"upcoming",merged)
                for bk_slug,bk_matches in per_bk.items():
                    if bk_matches: _save_sport_to_redis(s,"upcoming",bk_matches,bk_slug=bk_slug)
        click.echo(f"\n✅ Done. Files in: {output_dir}/")
        if errors:
            click.echo(f"\n⚠️  {len(errors)} error(s):")
            for key,err in errors.items(): click.echo(f"   {key}: {err}")

    @flask_app.cli.command("b2b-sample")
    @click.option("--sport",default="soccer")
    @click.option("--mode",default="upcoming")
    @click.option("--bk",default=None)
    def b2b_sample_cmd(sport,mode,bk):
        """Print raw E[] market events for one match."""
        bk_obj=_BK_BY_SLUG.get(bk) if bk else B2B_BOOKMAKERS[0]
        if not bk_obj: click.echo(f"❌ Unknown bookmaker: {bk}"); return
        print_raw_sample(bk_obj,sport,mode)

    @flask_app.cli.command("b2b-sports-tree")
    @click.option("--bk",default="paripesa")
    def b2b_sports_tree_cmd(bk):
        """Print available sports and match counts."""
        fetch_sports_tree(_BK_BY_SLUG.get(bk,B2B_BOOKMAKERS[-1]),verbose=True)

# =============================================================================
# STANDALONE
# =============================================================================

if __name__=="__main__":
    import sys
    logging.basicConfig(level=logging.WARNING)
    sport=sys.argv[1] if len(sys.argv)>1 else "soccer"
    mode=sys.argv[2] if len(sys.argv)>2 else "upcoming"
    cmd=sys.argv[3] if len(sys.argv)>3 else "sample"
    print(f"\n🚀 B2B — sport={sport} mode={mode} cmd={cmd}")
    if cmd=="raw":
        bk_slug=sys.argv[4] if len(sys.argv)>4 else "paripesa"
        print_raw_sample(_BK_BY_SLUG[bk_slug],sport,mode)
    elif cmd=="tree":
        bk_slug=sys.argv[4] if len(sys.argv)>4 else "paripesa"
        fetch_sports_tree(_BK_BY_SLUG.get(bk_slug,B2B_BOOKMAKERS[-1]),verbose=True)
    else:
        all_results=harvest_all_b2b(mode=mode,sports=[sport])
        print_sample_per_sport(all_results,sport_filter=sport)