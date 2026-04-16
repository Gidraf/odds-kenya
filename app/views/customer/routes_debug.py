"""
unified_stream.py  —  drop-in replacement for the route in bp_odds_customer.
"""

from flask import request, Response, stream_with_context
from .blueprint import bp_odds_customer
from . import config
from .utils import _now_utc, _normalise_sport_slug, _sse, _keepalive
from app.utils.decorators_ import log_event
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import re, json, time, os, traceback
from urllib.parse import quote
import queue
import threading


def _redis_url() -> str:
    full = os.getenv("REDIS_URL", "").strip()
    if full:
        return full

    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6379")
    auth = os.getenv("REDIS_AUTH", os.getenv("REDIS_PASSWORD", ""))
    db   = os.getenv("REDIS_DB",   "0")

    if auth:
        return f"redis://:{quote(auth, safe='')}@{host}:{port}/{db}"
    return f"redis://{host}:{port}/{db}"


def _get_redis(timeout: int = 2):
    try:
        import redis as _rm
        r = _rm.from_url(
            _redis_url(),
            decode_responses=True,
            socket_connect_timeout=timeout,
            socket_timeout=timeout,
        )
        r.ping()
        return r
    except Exception as e:
        print(f"[redis] unavailable ({e.__class__.__name__}: {e})")
        return None

# ══════════════════════════════════════════════════════════════════════════════
# COMPILED PATTERNS 
# ══════════════════════════════════════════════════════════════════════════════
_RE_NON_ALPHANUM     = re.compile(r'[^a-z0-9]')
_RE_SPORT_PREFIX     = re.compile(
    r"^(soccer|basketball|tennis|ice_hockey|volleyball|cricket|rugby|"
    r"table_tennis|handball|mma|boxing|darts|esoccer|baseball|american_football)_+"
)
_RE_EXACT_GOALS      = re.compile(r"exact_goals_\d+$")
_RE_TRAILING_ZERO    = re.compile(r"_0_0$")
_RE_MULTI_UNDERSCORE = re.compile(r"_+")


# ══════════════════════════════════════════════════════════════════════════════
# MARKET SLUG NORMALIZER
# ══════════════════════════════════════════════════════════════════════════════
@lru_cache(maxsize=4096)
def _normalize_market_slug(raw_slug: str, h_clean: str, a_clean: str) -> str:
    m = raw_slug.lower()
    m = _RE_SPORT_PREFIX.sub("", m)
    m = m.replace("1st_half___","first_half_").replace("1st_half_","first_half_")
    m = m.replace("2nd_half___","second_half_").replace("2nd_half_","second_half_")
    if h_clean and len(h_clean)>3 and h_clean in m: m = m.replace(h_clean,"home")
    if a_clean and len(a_clean)>3 and a_clean in m: m = m.replace(a_clean,"away")
    m = m.replace("___","_").replace("_&_","_and_").replace(" ","_")
    m = (m
         .replace("both_teams_to_score","btts")
         .replace("over_under_goals","over_under")
         .replace("total_goals","over_under")
         .replace("total_points","over_under"))
    if m in ("1x2","moneyline","3_way","match_winner"):      m = "match_winner"
    elif m in ("btts","both_teams_to_score","gg_ng","both_teams_to_score__gg_ng_"): m = "btts"
    if m.endswith("_1x2"):                                   m = m.replace("_1x2","_match_winner")
    if m.startswith("1x2_"):                                 m = m.replace("1x2_","match_winner_")
    if m == "match_winner_btts":                             m = "btts_and_result"
    if m.startswith("match_winner_over_under") or m.startswith("match_winner_and_over_under"):
        m = m.replace("match_winner_over_under","result_and_over_under") \
             .replace("match_winner_and_over_under","result_and_over_under")
    if m == "draw_no_bet_full_time":                         m = "draw_no_bet"
    m = _RE_EXACT_GOALS.sub("exact_goals", m)
    m = m.replace(".","_")
    if "handicap_-" in m:   m = m.replace("handicap_-","handicap_minus_")
    elif "handicap_" in m:  m = m.replace("handicap_+","handicap_")
    m = _RE_TRAILING_ZERO.sub("_0", m)
    m = _RE_MULTI_UNDERSCORE.sub("_", m).strip("_")
    return m


# ══════════════════════════════════════════════════════════════════════════════
# BEST-ODDS MERGE HELPER
# ══════════════════════════════════════════════════════════════════════════════
def _merge_best(best: dict, markets_by_bk: dict) -> None:
    for _bk_slug, mkts in markets_by_bk.items():
        for mkt_slug, outcomes in mkts.items():
            bk_mkt = best.setdefault(mkt_slug, {})
            for out_key, price_dict in outcomes.items():
                p = price_dict["price"]
                existing = bk_mkt.get(out_key)
                if existing is None or p > existing["odd"]:
                    bk_mkt[out_key] = {"odd": p, "bk": _bk_slug}


# ══════════════════════════════════════════════════════════════════════════════
# UNIVERSAL MATCH NORMALIZER 
# ══════════════════════════════════════════════════════════════════════════════
def _unify_match_payload(raw_match, count, mode, bk_slug, bk_name):
    if not raw_match: return {}

    st = str(raw_match.get("start_time") or "")
    if st and not st.endswith("Z") and "+" not in st:
        st = st.replace(" ","T")
        if len(st)==19: st += ".000Z"

    h_val = raw_match.get("home_team") or raw_match.get("home_team_name")
    a_val = raw_match.get("away_team") or raw_match.get("away_team_name")
    comps = raw_match.get("competitors")
    if isinstance(comps,list):
        if not h_val and len(comps)>0 and isinstance(comps[0],dict): h_val=comps[0].get("name")
        if not a_val and len(comps)>1 and isinstance(comps[1],dict): a_val=comps[1].get("name")

    h_team,a_team = str(h_val or "Home"), str(a_val or "Away")
    h_clean = _RE_NON_ALPHANUM.sub('_', h_team.lower()).strip('_')
    a_clean = _RE_NON_ALPHANUM.sub('_', a_team.lower()).strip('_')

    sport_val = raw_match.get("sport") or raw_match.get("sport_name") or "soccer"
    if isinstance(sport_val,dict): sport_val = sport_val.get("name","soccer")
    sport_slug = _normalise_sport_slug(str(sport_val))

    best_mock, bk_markets, standardized_markets = {}, {}, {}
    raw_list = raw_match.get("markets",{}) or []

    if isinstance(raw_list,list):
        for mkt_obj in raw_list:
            if not isinstance(mkt_obj,dict): continue
            m_id = mkt_obj.get("id")
            spec = str(mkt_obj.get("specValue") or mkt_obj.get("specialValue") or "")
            if spec.endswith(".00"): spec=spec[:-3]
            elif spec.endswith(".0"): spec=spec[:-2]
            name = str(mkt_obj.get("name") or "").lower()
            m = name
            if m_id in (52,105):      m = f"over_under_{spec}"
            elif m_id==54:            m = f"first_half_over_under_{spec}"
            elif m_id in (51,184):    m = f"asian_handicap_{spec}"
            elif m_id==53:            m = f"first_half_asian_handicap_{spec}"
            elif m_id in (55,151):    m = f"european_handicap_{spec}"
            elif m_id in (208,196):   m = f"result_and_over_under_{spec}"
            elif m_id in (386,124):   m = "btts_and_result"
            elif m_id in (10,194):    m = "match_winner"
            elif m_id in (43,138):    m = "btts"
            elif m_id in (46,147):    m = "double_chance"
            elif m_id in (47,166):    m = "draw_no_bet"
            standardized_markets[m] = {}
            for sel in mkt_obj.get("selections",[]):
                if not isinstance(sel,dict): continue
                ok = str(sel.get("shortName") or sel.get("name") or "")
                try: price = float(sel.get("odds",0))
                except: price = 0.0
                if price>1.0: standardized_markets[m][ok] = price
    else:
        for mkt_slug,outcomes in raw_list.items():
            m = str(mkt_slug or "")
            standardized_markets[m] = {}
            if not isinstance(outcomes,dict): continue
            for out_key,price in outcomes.items():
                try: p = float(price) if not isinstance(price,dict) else float(price.get("price",0))
                except: p=0.0
                if p>1.0: standardized_markets[m][str(out_key)] = p

    for mkt_slug,outcomes in standardized_markets.items():
        if not outcomes: continue
        m = _normalize_market_slug(str(mkt_slug), h_clean, a_clean)
        bk_markets[m] = {}; best_mock[m] = {}

        for ok_raw,p in outcomes.items():
            ok_raw,ok_lower = str(ok_raw), str(ok_raw).lower()
            ok = "X" if ("draw" in ok_lower and "or" not in ok_lower and "&" not in ok_lower) else ok_raw
            if h_clean and h_clean in ok_lower.replace(" ","_") and "or" not in ok_lower and "&" not in ok_lower: ok="1"
            if a_clean and a_clean in ok_lower.replace(" ","_") and "or" not in ok_lower and "&" not in ok_lower: ok="2"
            ok_lower = ok.lower()

            if "double_chance" in m:
                ok = (ok_lower.replace("or","").replace("/","").replace("draw","x")
                      .replace(h_clean,"1").replace(a_clean,"2")
                      .replace("home","1").replace("away","2").replace(" ","").upper())
                if ok=="X1": ok="1X"
                elif ok=="2X": ok="X2"
                elif ok=="21": ok="12"
            elif "btts_and" in m or "result_and" in m:
                suffix = ("Yes" if "yes" in ok_lower else "No" if "no" in ok_lower
                          else "Over" if "over" in ok_lower else "Under" if "under" in ok_lower else "")
                prefix = ("X" if "draw" in ok_lower or "x" in ok_lower.split() else
                          "1" if h_clean in ok_lower.replace(" ","_") or "home" in ok_lower or "1" in ok_lower.split() else
                          "2" if a_clean in ok_lower.replace(" ","_") or "away" in ok_lower or "2" in ok_lower.split() else "")
                ok = f"{prefix} {suffix}" if prefix and suffix else ok
            else:
                if ok_lower in ("yes","y"):          ok="Yes"
                elif ok_lower in ("no","n"):         ok="No"
                elif "over" in ok_lower or ok_lower=="ov": ok="Over"
                elif "under" in ok_lower or ok_lower=="un": ok="Under"
                elif ok_lower=="odd":  ok="Odd"
                elif ok_lower=="even": ok="Even"
                elif ok_lower=="home": ok="1"
                elif ok_lower=="away": ok="2"
                elif ok_lower=="1x":  ok="1X"
                elif ok_lower=="x2":  ok="X2"
                elif ok_lower=="12":  ok="12"
                if "over_under" in m and ok not in ("Over","Under"):
                    if ok_lower.startswith("o"): ok="Over"
                    elif ok_lower.startswith("u"): ok="Under"
                if "exact_goals" in m:  ok=ok.replace(" OR MORE","+").replace(" or more","+").replace(" and over","+")
                if "correct_score" in m: ok=ok.replace("-",":")

            best_mock[m][ok]  = {"odd":p,"bk":bk_slug}
            bk_markets[m][ok] = {"price":p}

    actual_id = str(raw_match.get(f"{bk_slug}_match_id") or raw_match.get("match_id") or count)
    b_id      = str(raw_match.get("betradar_id") or raw_match.get(f"{bk_slug}_parent_id") or "")

    return {
        "match_id":        actual_id,
        "join_key":        f"br_{b_id}" if b_id and b_id!="None" else f"{bk_slug}_{actual_id}",
        "parent_match_id": b_id if b_id and b_id!="None" else None,
        "home_team":       h_team, "away_team": a_team,
        "competition":     str(raw_match.get("competition_name") or raw_match.get("competition","")),
        "sport":           sport_slug, "start_time": st,
        "status":          "IN_PLAY" if mode=="live" else "PRE_MATCH",
        "is_live":         mode=="live",
        "bk_count":        1, "market_count": len(bk_markets),
        "market_slugs":    list(bk_markets.keys()),
        "bookmakers":      {bk_slug:{"bookmaker":bk_name,"slug":bk_slug,"markets":bk_markets,"market_count":len(bk_markets)}},
        "markets_by_bk":   {bk_slug: bk_markets},
        "best": best_mock, "best_odds": best_mock,
        "has_arb":False,"has_ev":False,"has_sharp":False,
    }


# ══════════════════════════════════════════════════════════════════════════════
# STREAMING ENDPOINT - CONCURRENT QUEUE ARCHITECTURE
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/debug/unified/stream/<mode>/<sport_slug>")
def debug_stream_unified(mode: str, sport_slug: str):
    fetch_full = str(request.args.get("full") or "true").lower() in ("1","true","yes")
    max_m      = int(request.args.get("max", 50))
    log_event("debug_unified_stream", {"sport":sport_slug,"mode":mode})

    def _gen():
        yield _sse("meta", {"source":"unified_direct","sport":sport_slug,"mode":mode,"now":_now_utc().isoformat()})
        try:
            count = 0
            seen_br_ids = set()
            active_live_br_ids = []
            sp_event_map = {}
            q = queue.Queue()
            all_matches_cache = {}

            # Worker function for the concurrent queue
            def stream_worker(bk_slug, bk_name, fetch_func, *args, **kwargs):
                try:
                    for raw_match in fetch_func(*args, **kwargs):
                        q.put({"type": "match", "bk_slug": bk_slug, "bk_name": bk_name, "data": raw_match})
                except Exception as e:
                    print(f"[{bk_slug}] stream error: {e}")
                finally:
                    q.put({"type": "done", "bk_slug": bk_slug})

            # Check cache for Upcoming Mode
            cache_key = f"unified:upcoming:{sport_slug}:{max_m}:{fetch_full}"
            _redis = _get_redis()
            
            if mode != "live" and _redis:
                try:
                    cached_data = _redis.get(cache_key)
                    if cached_data:
                        matches = json.loads(cached_data)
                        for i in range(0, len(matches), 15):
                            chunk = matches[i:i+15]
                            yield _sse("batch", {"matches":chunk,"batch":min(i+15,len(matches)),"of":len(matches),"offset":i})
                            yield _keepalive()
                        yield _sse("list_done", {"total_sent":len(matches)})
                        yield _sse("done", {"status":"finished","total_sent":len(matches),"cached":True})
                        return
                except Exception as _ce:
                    print(f"[unified] Redis cache read failed: {_ce} — fetching fresh")

            # ── START CONCURRENT THREADS ──
            if mode == "live":
                from app.workers.sp_live_harvester import fetch_live_stream as sp_fetch_live_stream
                from app.workers.bt_harvester import fetch_live_stream as bt_fetch_live_stream
                from app.workers.od_harvester import fetch_live_stream as od_fetch_live_stream

                active_workers = 3
                threading.Thread(target=stream_worker, args=("sp", "SPORTPESA", sp_fetch_live_stream, sport_slug, True)).start()
                threading.Thread(target=stream_worker, args=("bt", "BETIKA", bt_fetch_live_stream, sport_slug)).start()
                threading.Thread(target=stream_worker, args=("od", "ODIBETS", od_fetch_live_stream, sport_slug)).start()
            else:
                from app.workers.sp_harvester import fetch_upcoming_stream as sp_fetch_upcoming_stream
                from app.workers.bt_harvester import fetch_upcoming_stream as bt_fetch_upcoming_stream
                from app.workers.od_harvester import fetch_upcoming_stream as od_fetch_upcoming_stream

                active_workers = 3
                threading.Thread(target=stream_worker, args=("sp", "SPORTPESA", sp_fetch_upcoming_stream, sport_slug, max_m, fetch_full)).start()
                threading.Thread(target=stream_worker, args=("bt", "BETIKA", bt_fetch_upcoming_stream, sport_slug, max_m, 10, 9, fetch_full)).start()
                threading.Thread(target=stream_worker, args=("od", "ODIBETS", od_fetch_upcoming_stream, sport_slug, max_m)).start()

            # ── PROCESS QUEUE IN REAL TIME ──
            while active_workers > 0:
                msg = q.get()
                if msg["type"] == "done":
                    active_workers -= 1
                    continue

                raw_match = msg["data"]
                bk_slug   = msg["bk_slug"]
                bk_name   = msg["bk_name"]

                c = _unify_match_payload(raw_match, count, mode, bk_slug, bk_name)
                br_id = str(c.get("parent_match_id") or c.get("match_id") or "")

                if not br_id or br_id == "None": 
                    continue

                # Build maps required for the Live Pub/Sub phase
                if mode == "live" and bk_slug == "sp":
                    sp_match_id = str(raw_match.get("sp_match_id") or raw_match.get("id") or "")
                    if sp_match_id:
                        sp_event_map[br_id] = sp_match_id
                    active_live_br_ids.append(br_id)

                if br_id in seen_br_ids:
                    # Enrichment: match already exists, send UI update
                    bk_data = c["bookmakers"].get(bk_slug, {})
                    if bk_data:
                        yield _sse("live_update", {
                            "parent_match_id": br_id,
                            "home_team": "dummy",
                            "bookmakers": {bk_slug: bk_data},
                            "markets_by_bk": {bk_slug: c["markets_by_bk"].get(bk_slug, {})}
                        })
                        if br_id in all_matches_cache:
                            all_matches_cache[br_id]["bookmakers"][bk_slug] = bk_data
                            all_matches_cache[br_id]["markets_by_bk"][bk_slug] = c["markets_by_bk"].get(bk_slug, {})
                            all_matches_cache[br_id]["bk_count"] = len(all_matches_cache[br_id]["bookmakers"])
                            _merge_best(all_matches_cache[br_id]["best"], all_matches_cache[br_id]["markets_by_bk"])
                            all_matches_cache[br_id]["market_slugs"] = list(all_matches_cache[br_id]["best"].keys())
                            all_matches_cache[br_id]["market_count"] = len(all_matches_cache[br_id]["best"])
                else:
                    # First time seeing this match: draw the card
                    count += 1
                    seen_br_ids.add(br_id)
                    c["is_live"] = (mode == "live")
                    _merge_best(c["best"], c["markets_by_bk"])
                    c["market_slugs"] = list(c["best"].keys())
                    c["market_count"] = len(c["best"])
                    
                    all_matches_cache[br_id] = c
                    yield _sse("batch", {"matches": [c], "batch": count, "of": "unknown", "offset": count - 1})

                yield _keepalive()

            yield _sse("list_done", {"total_sent": count})

            # ── POST-QUEUE LOGIC ──
            if mode == "live":
                # Fall back to Redis pub/sub for live odds ticking (exactly as in original code)
                _r_live = _get_redis()
                pubsub  = None
                if _r_live:
                    from app.workers.sp_live_harvester import SPORT_SLUG_MAP
                    from app.workers.bt_harvester import fetch_live_matches as bt_fetch_live, slug_to_bt_sport_id
                    from app.workers.od_harvester import fetch_live_matches as od_fetch_live

                    sport_id = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)
                    try:
                        pubsub = _r_live.pubsub(ignore_subscribe_messages=True)
                        pubsub.subscribe(f"sp:live:sport:{sport_id}")
                    except Exception as _rle:
                        print(f"[unified:live] pubsub subscribe failed: {_rle}")
                        pubsub = None

                    last_poll = time.time()
                    bg_pool   = ThreadPoolExecutor(max_workers=2)
                    bg_future = None

                    def _fetch_bg():
                        try:
                            return bt_fetch_live(slug_to_bt_sport_id(sport_slug)) or [], od_fetch_live(sport_slug) or []
                        except: return [],[]

                    while True:
                        msg = pubsub.get_message(timeout=0.2) if pubsub else None
                        if msg and msg["type"]=="message":
                            try:
                                payload  = json.loads(msg["data"])
                                msg_type = payload.get("type")
                                ev_id    = payload.get("event_id")
                                br_id    = next((k for k,v in sp_event_map.items() if v==ev_id), str(ev_id))
                                if msg_type=="event_update":
                                    state = payload.get("state",{})
                                    yield _sse("live_update",{
                                        "parent_match_id":br_id,"home_team":"dummy",
                                        "match_time":state.get("matchTime",payload.get("match_time","")),
                                        "score_home":payload.get("score_home"),
                                        "score_away":payload.get("score_away"),
                                        "status":payload.get("phase",state.get("currentEventPhase","")),
                                        "is_live":True,
                                    })
                                elif msg_type=="market_update":
                                    norm_sels = payload.get("normalised_selections",[])
                                    slug      = payload.get("market_slug")
                                    if norm_sels and slug:
                                        outs = {s["outcome_key"]:{"price":float(s["odds"])} for s in norm_sels if float(s.get("odds",0))>1}
                                        if outs:
                                            yield _sse("live_update",{
                                                "parent_match_id":br_id,"home_team":"dummy",
                                                "bookmakers":{"sp":{"slug":"sp","markets":{slug:outs}}},
                                                "markets_by_bk":{"sp":{slug:outs}},
                                            })
                            except: pass

                        if time.time()-last_poll > 10.0:
                            last_poll = time.time()
                            if bg_future is None or bg_future.done():
                                bg_future = bg_pool.submit(_fetch_bg)

                        if bg_future and bg_future.done():
                            try:
                                bt_live, od_live = bg_future.result()
                                bt_lm = {str(m.get("betradar_id") or m.get("bt_parent_id")):m for m in bt_live if isinstance(m,dict)}
                                od_lm = {str(m.get("betradar_id") or m.get("od_parent_id")):m for m in od_live if isinstance(m,dict)}
                                for br_id in active_live_br_ids:
                                    up = {"parent_match_id":br_id,"home_team":"dummy","bookmakers":{},"markets_by_bk":{}}
                                    if br_id in bt_lm:
                                        b_c = _unify_match_payload(bt_lm[br_id],0,"live","bt","BETIKA")
                                        up["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                        up["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                                    if br_id in od_lm:
                                        o_c = _unify_match_payload(od_lm[br_id],0,"live","od","ODIBETS")
                                        up["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                        up["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                                    if up["bookmakers"]:
                                        yield _sse("live_update", up)
                            except: pass
                            bg_future = None

                        yield _keepalive()
            else:
                # Save fully assembled upcoming cache
                if _redis and all_matches_cache:
                    try:
                        _redis.setex(cache_key, 300, json.dumps(list(all_matches_cache.values())))
                    except Exception as _we:
                        print(f"[unified] Redis cache write failed: {_we}")
                yield _sse("done", {"status": "finished", "total_sent": count})

        except Exception as exc:
            tb = traceback.format_exc()
            print("=== STREAM CRASH ==="); print(tb); print("====================")
            yield _sse("error", {"error": str(exc), "traceback": tb})

    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)