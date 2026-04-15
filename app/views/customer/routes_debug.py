"""
unified_stream.py  —  drop-in replacement for the route in bp_odds_customer.

KEY CHANGES vs original:
  1. SP matches are yielded ONE AT A TIME the moment they are parsed.
     The frontend sees each card appear as soon as SportPesa data arrives —
     like ChatGPT streaming tokens.

  2. A `list_done` event signals "all SP data sent; enrichment starting".
     The frontend switches its status pill from "Receiving SportPesa data"
     to "Adding Betika & OdiBets".

  3. BT / OD enrichment is done with as_completed() — each bookmaker's odds
     are sent as a `live_update` the instant they arrive, not after all are done.

  4. LIVE mode:  SP events stream first (one per yield), then BT+OD updates
     arrive via the background pub/sub loop — unchanged logic.

  5. Redis cache: still caches the fully-assembled result for 5 minutes.
     A cache hit streams all matches in 15-item chunks so the UI still
     animates them in progressively (not one huge batch dump).
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


def _redis_url() -> str:
    """
    Build the Redis connection URL from environment variables.

    Priority order:
      1. REDIS_URL      — full URL already set (e.g. by a managed service)
      2. REDIS_HOST / REDIS_PORT / REDIS_AUTH — individual vars (docker-compose)
      3. Fallback       — redis://localhost:6379/0

    The password may start with '@' (e.g. @Winners1127) so we percent-encode it.
    """
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
    """
    Return a connected Redis client or None if Redis is unavailable.
    Uses a 2-second connect/read timeout so failures are fast.
    """
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
# COMPILED PATTERNS  (unchanged)
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
# MARKET SLUG NORMALIZER  (unchanged)
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
# BEST-ODDS MERGE HELPER  (unchanged)
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
# UNIVERSAL MATCH NORMALIZER  (unchanged)
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
# STREAMING ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/debug/unified/stream/<mode>/<sport_slug>")
def debug_stream_unified(mode: str, sport_slug: str):
    fetch_full = str(request.args.get("full") or "true").lower() in ("1","true","yes")
    max_m      = int(request.args.get("max", 50))
    log_event("debug_unified_stream", {"sport":sport_slug,"mode":mode})

    def _gen():
        yield _sse("meta", {"source":"unified_direct","sport":sport_slug,
                            "mode":mode,"now":_now_utc().isoformat()})
        try:
            count = 0

            # ══════════════════════════════════════════════════════════════════
            # LIVE MODE
            # ══════════════════════════════════════════════════════════════════
            if mode == "live":
                from app.workers.sp_live_harvester import fetch_live_stream, SPORT_SLUG_MAP
                from app.workers.bt_harvester      import fetch_live_matches as bt_fetch_live, slug_to_bt_sport_id
                from app.workers.od_harvester      import fetch_live_matches as od_fetch_live

                sport_id = {v:k for k,v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)

                # ── Fire BT + OD fetches in background threads BEFORE touching SP.
                #    SP is a generator — we iterate it immediately without list().
                #    BT/OD results arrive while we are already streaming SP cards.
                _bg_pool = ThreadPoolExecutor(max_workers=2)
                _f_bt    = _bg_pool.submit(lambda: bt_fetch_live(slug_to_bt_sport_id(sport_slug)) or [])
                _f_od    = _bg_pool.submit(lambda: od_fetch_live(sport_slug) or [])

                # SP generator — yields one match at a time, no list() conversion
                sp_stream = fetch_live_stream(sport_slug, fetch_full_markets=True)

                # Lazy BT/OD map — built from futures only once they complete
                _bt_done = _od_done = False
                bt_map: dict = {}
                od_map: dict = {}

                def _refresh_bk_maps():
                    nonlocal _bt_done, _od_done, bt_map, od_map
                    if not _bt_done and _f_bt.done():
                        try:
                            bt_data = _f_bt.result() or []
                            bt_map  = {str(m.get("betradar_id") or m.get("bt_parent_id")): m
                                       for m in bt_data if isinstance(m, dict)}
                        except Exception as _e:
                            print(f"[live] BT fetch failed: {_e}")
                        _bt_done = True
                    if not _od_done and _f_od.done():
                        try:
                            od_data = _f_od.result() or []
                            od_map  = {str(m.get("betradar_id") or m.get("od_parent_id")): m
                                       for m in od_data if isinstance(m, dict)}
                        except Exception as _e:
                            print(f"[live] OD fetch failed: {_e}")
                        _od_done = True

                sp_event_map, active_live_br_ids, seen_br_ids = {}, [], set()

                # ── SP matches — yield ONE AT A TIME as the generator produces them ──
                for sp_match in sp_stream:
                    if not isinstance(sp_match,dict): continue
                    count += 1
                    betradar_id = str(sp_match.get("betradar_id") or "")
                    if betradar_id and betradar_id not in ("0","None"):
                        active_live_br_ids.append(betradar_id)
                        seen_br_ids.add(betradar_id)
                        sp_event_map[betradar_id] = str(sp_match.get("sp_match_id") or sp_match.get("match_id") or "")

                    # Refresh BT/OD maps if their futures completed while we were iterating SP
                    _refresh_bk_maps()

                    sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                    sp_clean["is_live"] = True

                    # Eagerly attach BT/OD data if already available in memory
                    if betradar_id:
                        bt_match = bt_map.get(betradar_id)
                        if bt_match and bt_match.get("markets"):
                            b_clean = _unify_match_payload(bt_match, count, mode, "bt", "BETIKA")
                            sp_clean["bookmakers"]["bt"]    = b_clean["bookmakers"]["bt"]
                            sp_clean["markets_by_bk"]["bt"] = b_clean["markets_by_bk"]["bt"]
                        od_match = od_map.get(betradar_id)
                        if od_match and od_match.get("markets"):
                            o_clean = _unify_match_payload(od_match, count, mode, "od", "ODIBETS")
                            sp_clean["bookmakers"]["od"]    = o_clean["bookmakers"]["od"]
                            sp_clean["markets_by_bk"]["od"] = o_clean["markets_by_bk"]["od"]

                    sp_clean["bk_count"] = len(sp_clean["bookmakers"])
                    _merge_best(sp_clean["best"], sp_clean["markets_by_bk"])
                    sp_clean["market_slugs"]  = list(sp_clean["best"].keys())
                    sp_clean["market_count"]  = len(sp_clean["best"])

                    # ← one card at a time, client renders it immediately
                    yield _sse("batch", {"matches":[sp_clean],"batch":count,
                                         "of":"unknown","offset":count-1})
                    yield _keepalive()

                # ── Ensure BT/OD futures are resolved before the enrichment pass ──
                if not _bt_done:
                    try:
                        bt_data = _f_bt.result(timeout=15) or []
                        bt_map  = {str(m.get("betradar_id") or m.get("bt_parent_id")): m
                                   for m in bt_data if isinstance(m, dict)}
                    except Exception as _e:
                        print(f"[live] BT result timeout: {_e}")
                    _bt_done = True
                if not _od_done:
                    try:
                        od_data = _f_od.result(timeout=15) or []
                        od_map  = {str(m.get("betradar_id") or m.get("od_parent_id")): m
                                   for m in od_data if isinstance(m, dict)}
                    except Exception as _e:
                        print(f"[live] OD result timeout: {_e}")
                    _od_done = True
                _bg_pool.shutdown(wait=False)

                # ── Send BT/OD enrichments for already-streamed SP cards ─────
                for br_id in list(seen_br_ids):
                    update = {"parent_match_id": br_id, "home_team":"dummy",
                              "bookmakers": {}, "markets_by_bk": {}}
                    bt_match = bt_map.get(br_id)
                    if bt_match and bt_match.get("markets"):
                        b_c = _unify_match_payload(bt_match, 0, mode, "bt", "BETIKA")
                        update["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                        update["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                    od_match = od_map.get(br_id)
                    if od_match and od_match.get("markets"):
                        o_c = _unify_match_payload(od_match, 0, mode, "od", "ODIBETS")
                        update["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                        update["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                    if update["bookmakers"]:
                        yield _sse("live_update", update)
                        yield _keepalive()

                # BT/OD-only events (matches SP didn't have)
                for br_id, bt_match in bt_map.items():
                    if br_id not in seen_br_ids and br_id and br_id!="None":
                        count += 1
                        active_live_br_ids.append(br_id)
                        bt_clean = _unify_match_payload(bt_match, count, mode, "bt", "BETIKA")
                        bt_clean["is_live"] = True
                        od_match = od_map.get(br_id)
                        if od_match and od_match.get("markets"):
                            o_clean = _unify_match_payload(od_match, count, mode, "od", "ODIBETS")
                            bt_clean["bookmakers"]["od"]    = o_clean["bookmakers"]["od"]
                            bt_clean["markets_by_bk"]["od"] = o_clean["markets_by_bk"]["od"]
                            seen_br_ids.add(br_id)
                        bt_clean["bk_count"] = len(bt_clean["bookmakers"])
                        _merge_best(bt_clean["best"], bt_clean["markets_by_bk"])
                        bt_clean["market_slugs"] = list(bt_clean["best"].keys())
                        bt_clean["market_count"] = len(bt_clean["best"])
                        yield _sse("batch", {"matches":[bt_clean],"batch":count,"of":"unknown","offset":count-1})
                        yield _keepalive()
                        seen_br_ids.add(br_id)

                for br_id, od_match in od_map.items():
                    if br_id not in seen_br_ids and br_id and br_id!="None":
                        count += 1
                        od_clean = _unify_match_payload(od_match, count, mode, "od", "ODIBETS")
                        od_clean["is_live"] = True
                        _merge_best(od_clean["best"], od_clean["markets_by_bk"])
                        od_clean["market_slugs"] = list(od_clean["best"].keys())
                        od_clean["market_count"] = len(od_clean["best"])
                        yield _sse("batch", {"matches":[od_clean],"batch":count,"of":"unknown","offset":count-1})
                        yield _keepalive()
                        seen_br_ids.add(br_id)

                yield _sse("list_done", {"total_sent":count})

                # Live pub/sub background loop — Redis optional
                _r_live = _get_redis()
                pubsub  = None
                if _r_live:
                    try:
                        pubsub = _r_live.pubsub(ignore_subscribe_messages=True)
                        pubsub.subscribe(f"sp:live:sport:{sport_id}")
                    except Exception as _rle:
                        print(f"[unified:live] pubsub subscribe failed: {_rle}")
                        pubsub = None
                else:
                    print("[unified:live] Redis unavailable — pub/sub disabled")
                r = _r_live  # keep `r` alias used in the polling loop
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

            # ══════════════════════════════════════════════════════════════════
            # UPCOMING MODE — SP streams first, BT+OD enrichment follows
            # ══════════════════════════════════════════════════════════════════
            else:
                # ── Redis is optional — gracefully skip cache when unavailable ─
                cache_key = f"unified:upcoming:{sport_slug}:{max_m}:{fetch_full}"
                _redis = _get_redis()
                if _redis is None:
                    print("[unified] Redis unavailable — fetching fresh data")

                # ── Cache hit — stream in small chunks for progressive reveal ─
                if _redis:
                    try:
                        cached_data = _redis.get(cache_key)
                        if cached_data:
                            matches = json.loads(cached_data)
                            for i in range(0, len(matches), 15):
                                chunk = matches[i:i+15]
                                yield _sse("batch", {"matches":chunk,"batch":min(i+15,len(matches)),
                                                     "of":len(matches),"offset":i})
                                yield _keepalive()
                            yield _sse("list_done", {"total_sent":len(matches)})
                            yield _sse("done",      {"status":"finished","total_sent":len(matches),"cached":True})
                            return
                    except Exception as _ce:
                        print(f"[unified] Redis cache read failed: {_ce} — fetching fresh")

                # ── Cache miss — fetch SP first ───────────────────────────────
                from app.workers.sp_harvester  import fetch_upcoming_stream
                from app.workers.bt_harvester  import get_full_markets
                from app.workers.od_harvester  import fetch_event_detail, slug_to_od_sport_id

                od_sport_id    = slug_to_od_sport_id(sport_slug)
                all_sp_matches = {}  # betradar_id → unified dict (for enrichment later)

                # ── PHASE 1: Stream every SP match immediately, one at a time ─
                for sp_match in fetch_upcoming_stream(sport_slug, max_matches=max_m,
                                                      fetch_full_markets=fetch_full):
                    if not isinstance(sp_match,dict): continue
                    count += 1
                    sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                    sp_clean["market_slugs"] = list(sp_clean["best"].keys())
                    sp_clean["market_count"] = len(sp_clean["best"])

                    brid = str(sp_clean.get("parent_match_id") or sp_clean.get("match_id") or "")
                    if brid:
                        all_sp_matches[brid] = sp_clean

                    # ← client renders this card NOW, before BT/OD are fetched
                    yield _sse("batch", {"matches":[sp_clean],"batch":count,
                                         "of":"unknown","offset":count-1})
                    yield _keepalive()

                yield _sse("list_done", {"total_sent":count})

                # ── PHASE 2: Enrich with BT + OD concurrently ─────────────────
                def _get_deep(brid):
                    bt_mkts, od_mkts = {}, {}
                    try: bt_mkts = get_full_markets(brid, sport_slug)
                    except: pass
                    try: od_mkts = fetch_event_detail(brid, od_sport_id)[0]
                    except: pass
                    return brid, bt_mkts, od_mkts

                n_workers = min(len(all_sp_matches)*2+4, 60)
                with ThreadPoolExecutor(max_workers=n_workers) as pool:
                    futs = [
                        pool.submit(_get_deep, brid)
                        for brid in all_sp_matches
                        if brid and brid!="None"
                    ]

                    for fut in as_completed(futs):
                        brid, bt_markets, od_markets = fut.result()
                        if not bt_markets and not od_markets:
                            continue

                        update = {
                            "parent_match_id": brid,
                            "home_team":       "dummy",   # merge key only
                            "bookmakers":      {},
                            "markets_by_bk":   {},
                        }

                        if bt_markets:
                            b_c = _unify_match_payload(
                                {"markets":bt_markets,"betradar_id":brid,
                                 "home_team":"","away_team":"","sport":sport_slug},
                                0, mode, "bt", "BETIKA")
                            update["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                            update["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                            all_sp_matches[brid]["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                            all_sp_matches[brid]["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]

                        if od_markets:
                            o_c = _unify_match_payload(
                                {"markets":od_markets,"betradar_id":brid,
                                 "home_team":"","away_team":"","sport":sport_slug},
                                0, mode, "od", "ODIBETS")
                            update["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                            update["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                            all_sp_matches[brid]["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                            all_sp_matches[brid]["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]

                        # recompute best on the stored match
                        _merge_best(all_sp_matches[brid]["best"], all_sp_matches[brid]["markets_by_bk"])
                        all_sp_matches[brid]["market_slugs"] = list(all_sp_matches[brid]["best"].keys())
                        all_sp_matches[brid]["market_count"] = len(all_sp_matches[brid]["best"])
                        all_sp_matches[brid]["bk_count"]     = len(all_sp_matches[brid]["bookmakers"])

                        # ← client updates the existing card in-place (flash animation)
                        yield _sse("live_update", update)
                        yield _keepalive()

                # ── Cache fully assembled payload (skip if Redis unavailable) ──────
                if _redis and all_sp_matches:
                    try:
                        _redis.setex(cache_key, 300, json.dumps(list(all_sp_matches.values())))
                    except Exception as _we:
                        print(f"[unified] Redis cache write failed: {_we}")

                yield _sse("done", {"status":"finished","total_sent":count})

        except Exception as exc:
            tb = traceback.format_exc()
            print("=== STREAM CRASH ==="); print(tb); print("====================")
            yield _sse("error", {"error":str(exc),"traceback":tb})

    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)