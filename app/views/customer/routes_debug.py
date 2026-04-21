"""
unified_stream.py  —  drop-in replacement for the route in bp_odds_customer.

KEY CHANGES vs previous version:
  LIVE FIX:
    Phase 2 (SP market futures) and Phase 3 (BT/OD futures) were sequential —
    BT/OD data sat ready but was blocked waiting for all SP market calls.
    Now all futures are merged into one as_completed() pool, tagged by type,
    so BT/OD enrichment fires the moment the bulk fetch resolves (~300 ms)
    regardless of where SP market fetches are.

  UPCOMING FIX:
    Previous code did N × 2 per-event API calls (fetch_event_detail + get_full_markets).
    For 50 matches = 100 HTTP calls in a thread pool — slow and noisy.
    New approach:
      1. Fire ONE BT bulk fetch + ONE OD bulk fetch immediately (concurrently)
         while SP is still streaming its first matches.
      2. Build betradar_id → match maps from bulk responses.
      3. Enrichment = O(1) map lookup per SP match.
      4. Only fall back to per-event fetch for betradar_ids not in bulk maps.
    This cuts enrichment from ~100 HTTP calls to ~2 bulk calls.
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

def _get_redis():
    import redis
    full = os.getenv("REDIS_URL", "").strip()
    if full: return redis.from_url(full, decode_responses=True, socket_timeout=3)
    
    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6379")
    auth = os.getenv("REDIS_AUTH", os.getenv("REDIS_PASSWORD", ""))
    db   = os.getenv("REDIS_DB", "0")
    if auth: return redis.from_url(f"redis://:{quote(auth, safe='')}@{host}:{port}/{db}", decode_responses=True)
    return redis.from_url(f"redis://{host}:{port}/{db}", decode_responses=True)


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
# BULK MAP BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def _build_bk_map(matches: list, *id_keys) -> dict:
    """Build {betradar_id: match} from a list of normalised BK matches."""
    result = {}
    for m in (matches or []):
        if not isinstance(m, dict):
            continue
        for key in id_keys:
            bid = str(m.get(key) or "")
            if bid and bid not in ("None", "0", ""):
                result[bid] = m
                break
    return result


def _norm_team_key(h: str, a: str) -> str:
    """Fuzzy team-name match key (first 8 chars, lowercased)."""
    return f"{h[:8].lower().strip()}|{a[:8].lower().strip()}"


# ══════════════════════════════════════════════════════════════════════════════
# STREAMING ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/old/debug/unified/stream/<mode>/<sport_slug>")
def debug_stream_unified_(mode: str, sport_slug: str):
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
                from app.workers.sp_live_harvester import (
                    fetch_live_events, fetch_live_markets,
                    SPORT_SLUG_MAP, live_market_slug, normalize_live_outcome,
                )
                from app.workers.bt_harvester import (
                    fetch_live_matches as bt_fetch_live, slug_to_bt_sport_id,
                )
                from app.workers.od_harvester import fetch_live_matches as od_fetch_live

                sport_id = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)

                # ── Phase 1: one fast HTTP call for raw events ────────────────
                raw_events = fetch_live_events(sport_id, limit=100)
                if not raw_events:
                    yield _sse("list_done", {"total_sent": 0})
                    yield _sse("done", {"status": "no_live_events"})
                    return

                betradar_map: dict = {}
                for ev in raw_events:
                    bid = str(ev.get("externalId") or "")
                    if bid and bid != "0":
                        betradar_map[ev["id"]] = bid

                _CORE_MARKET_TYPES = [194, 105, 138]  # 1x2, O/U, BTTS

                def _fetch_sp_mkt(ev_id: int) -> tuple:
                    collected = []
                    try:
                        res = fetch_live_markets([ev_id], sport_id, _CORE_MARKET_TYPES[0])
                        for m in res:
                            inner = m.get("markets") or []
                            if inner: collected.extend(inner)
                    except Exception:
                        pass
                    for m_type in _CORE_MARKET_TYPES[1:]:
                        try:
                            res = fetch_live_markets([ev_id], sport_id, m_type)
                            for m in res:
                                inner = m.get("markets") or []
                                if inner: collected.extend(inner)
                        except Exception:
                            pass
                    return ev_id, collected

                # ── Start ALL background work concurrently right away ─────────
                # Tag each future so we can route it in one as_completed loop.
                # "sp_mkt"  → SP market fetch for one event
                # "bt_bulk" → BT full live match list
                # "od_bulk" → OD full live match list
                _bg_pool = ThreadPoolExecutor(max_workers=12)

                all_futs: dict = {}
                for ev in raw_events:
                    f = _bg_pool.submit(_fetch_sp_mkt, ev["id"])
                    all_futs[f] = ("sp_mkt", ev["id"])

                f_bt = _bg_pool.submit(
                    lambda: bt_fetch_live(slug_to_bt_sport_id(sport_slug)) or []
                )
                f_od = _bg_pool.submit(lambda: od_fetch_live(sport_slug) or [])
                all_futs[f_bt] = ("bt_bulk", None)
                all_futs[f_od] = ("od_bulk", None)

                # Running BK maps — populated as bulk futures complete
                bt_map: dict = {}
                od_map: dict = {}
                sp_event_map: dict = {}  # betradar_id → str(sp_ev_id)
                active_live_br_ids: list = []
                seen_br_ids: set = set()

                # ── Phase 1 yield: stream raw events immediately ──────────────
                sp_team_index: dict = {}
                for ev in raw_events:
                    count += 1
                    state = ev.get("state") or {}
                    score = state.get("matchScore") or {}
                    comps = ev.get("competitors") or [{}, {}]
                    betradar_id = str(ev.get("externalId") or "")
                    sp_ev_id    = ev.get("id")

                    if betradar_id and betradar_id not in ("0", "None"):
                        active_live_br_ids.append(betradar_id)
                        seen_br_ids.add(betradar_id)
                        sp_event_map[betradar_id] = str(sp_ev_id or "")

                    h = comps[0].get("name", "Home") if len(comps) > 0 else "Home"
                    a = comps[1].get("name", "Away") if len(comps) > 1 else "Away"
                    if h and a:
                        sp_team_index[_norm_team_key(h, a)] = betradar_id

                    sp_match = {
                        "sp_match_id":  sp_ev_id,
                        "betradar_id":  betradar_id if betradar_id and betradar_id != "0" else None,
                        "home_team":    h,
                        "away_team":    a,
                        "competition":  (ev.get("tournament") or {}).get("name", ""),
                        "sport":        sport_slug,
                        "start_time":   ev.get("kickoffTimeUTC", ""),
                        "match_time":   str(state.get("matchTime", "")),
                        "current_score": f"{score.get('home','')}-{score.get('away','')}",
                        "score_home":   str(score.get("home", "")) or None,
                        "score_away":   str(score.get("away", "")) or None,
                        "event_status": state.get("currentEventPhase", ""),
                        "markets":      [],
                    }

                    sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                    sp_clean["is_live"]    = True
                    sp_clean["match_time"] = sp_match["match_time"]
                    sp_clean["score_home"] = sp_match["score_home"]
                    sp_clean["score_away"] = sp_match["score_away"]

                    yield _sse("batch", {"matches": [sp_clean], "batch": count,
                                         "of": len(raw_events), "offset": count - 1})
                    yield _keepalive()

                yield _sse("list_done", {"total_sent": count})

                # ── Phase 2+3 MERGED: process all futures as they complete ────
                # BT/OD bulk futures typically resolve in ~300 ms.
                # SP market futures take 1-3 s each.
                # By processing in one loop, BT/OD updates fire immediately
                # without waiting for SP market futures.
                matched_bk_br_ids: set = set()

                for fut in as_completed(all_futs, timeout=20):
                    tag, ctx = all_futs[fut]

                    # ── SP market update for one event ────────────────────────
                    if tag == "sp_mkt":
                        ev_id = ctx
                        betradar_id = betradar_map.get(ev_id, "")
                        if not betradar_id:
                            continue
                        try:
                            _, raw_mkts = fut.result(timeout=1)
                        except Exception:
                            continue
                        if not raw_mkts:
                            continue

                        bk_markets: dict = {}
                        for mkt in raw_mkts:
                            if not isinstance(mkt, dict): continue
                            mkt_type = mkt.get("id") or mkt.get("typeId")
                            handicap = mkt.get("specValue") or mkt.get("handicap")
                            slug = live_market_slug(int(mkt_type), handicap, sport_id) if mkt_type else None
                            if not slug: continue
                            sels = mkt.get("selections") or []
                            for idx, sel in enumerate(sels):
                                if not isinstance(sel, dict): continue
                                try: odd = float(sel.get("odds") or 0)
                                except: odd = 0.0
                                if odd <= 1.0: continue
                                out_key = normalize_live_outcome(slug, sel.get("name",""), idx, sels)
                                bk_markets.setdefault(slug, {})[out_key] = {"price": odd}

                        if bk_markets:
                            yield _sse("live_update", {
                                "parent_match_id": betradar_id,
                                "home_team":       "dummy",
                                "bookmakers":      {"sp": {"bookmaker":"SPORTPESA","slug":"sp",
                                                           "markets":bk_markets,
                                                           "market_count":len(bk_markets)}},
                                "markets_by_bk":   {"sp": bk_markets},
                            })
                            yield _keepalive()

                    # ── BT bulk result ────────────────────────────────────────
                    elif tag == "bt_bulk":
                        try:
                            raw_list = fut.result() or []
                        except Exception as _e:
                            print(f"[live] BT fetch error: {_e}")
                            raw_list = []

                        bt_map = _build_bk_map(raw_list, "betradar_id", "bt_parent_id")
                        _send_bk_enrichment(
                            raw_list, bt_map, "bt", "BETIKA", mode,
                            seen_br_ids, sp_team_index, sp_event_map,
                            matched_bk_br_ids, count,
                        )
                        for ev_payload in _bk_enrich_generator(
                            raw_list, bt_map, "bt", "BETIKA", mode,
                            seen_br_ids, sp_team_index, sp_event_map,
                            matched_bk_br_ids,
                        ):
                            yield ev_payload
                            yield _keepalive()

                    # ── OD bulk result ────────────────────────────────────────
                    elif tag == "od_bulk":
                        try:
                            raw_list = fut.result() or []
                        except Exception as _e:
                            print(f"[live] OD fetch error: {_e}")
                            raw_list = []

                        od_map = _build_bk_map(raw_list, "betradar_id", "od_parent_id", "od_match_id")
                        for ev_payload in _bk_enrich_generator(
                            raw_list, od_map, "od", "ODIBETS", mode,
                            seen_br_ids, sp_team_index, sp_event_map,
                            matched_bk_br_ids,
                        ):
                            yield ev_payload
                            yield _keepalive()

                _bg_pool.shutdown(wait=False)
                yield _sse("list_done", {"total_sent": count})

                # ── Phase 4: pub/sub for real-time updates ────────────────────
                _r_live = _get_redis()
                pubsub = None
                if _r_live:
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
                        return (
                            bt_fetch_live(slug_to_bt_sport_id(sport_slug)) or [],
                            od_fetch_live(sport_slug) or [],
                        )
                    except Exception:
                        return [], []

                while True:
                    msg = pubsub.get_message(timeout=0.2) if pubsub else None
                    if msg and msg["type"] == "message":
                        try:
                            payload  = json.loads(msg["data"])
                            msg_type = payload.get("type")
                            ev_id    = payload.get("event_id")
                            br_id    = next(
                                (k for k, v in sp_event_map.items() if v == ev_id),
                                str(ev_id),
                            )
                            if msg_type == "event_update":
                                state = payload.get("state", {})
                                yield _sse("live_update", {
                                    "parent_match_id": br_id, "home_team": "dummy",
                                    "match_time":      state.get("matchTime", payload.get("match_time", "")),
                                    "score_home":      payload.get("score_home"),
                                    "score_away":      payload.get("score_away"),
                                    "status":          payload.get("phase", state.get("currentEventPhase", "")),
                                    "is_live": True,
                                })
                            elif msg_type == "market_update":
                                norm_sels = payload.get("normalised_selections", [])
                                slug = payload.get("market_slug")
                                if norm_sels and slug:
                                    outs = {
                                        s["outcome_key"]: {"price": float(s["odds"])}
                                        for s in norm_sels if float(s.get("odds", 0)) > 1
                                    }
                                    if outs:
                                        yield _sse("live_update", {
                                            "parent_match_id": br_id, "home_team": "dummy",
                                            "bookmakers":      {"sp": {"slug": "sp", "markets": {slug: outs}}},
                                            "markets_by_bk":   {"sp": {slug: outs}},
                                        })
                        except Exception:
                            pass

                    if time.time() - last_poll > 10.0:
                        last_poll = time.time()
                        if bg_future is None or bg_future.done():
                            bg_future = bg_pool.submit(_fetch_bg)

                    if bg_future and bg_future.done():
                        try:
                            bt_live, od_live = bg_future.result()
                            bt_lm = _build_bk_map(bt_live, "betradar_id", "bt_parent_id")
                            od_lm = _build_bk_map(od_live, "betradar_id", "od_parent_id", "od_match_id")
                            for br_id in active_live_br_ids:
                                up = {"parent_match_id": br_id, "home_team": "dummy",
                                      "bookmakers": {}, "markets_by_bk": {}}
                                if br_id in bt_lm:
                                    b_c = _unify_match_payload(bt_lm[br_id], 0, "live", "bt", "BETIKA")
                                    up["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                    up["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                                if br_id in od_lm:
                                    o_c = _unify_match_payload(od_lm[br_id], 0, "live", "od", "ODIBETS")
                                    up["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                    up["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                                if up["bookmakers"]:
                                    yield _sse("live_update", up)
                        except Exception:
                            pass
                        bg_future = None

                    yield _keepalive()

            # ══════════════════════════════════════════════════════════════════
            # UPCOMING MODE
            # ══════════════════════════════════════════════════════════════════
            else:
                cache_key = f"unified:upcoming:{sport_slug}:{max_m}:{fetch_full}"
                _redis = _get_redis()
                if _redis is None:
                    print("[unified] Redis unavailable — fetching fresh data")

                # ── Cache hit ─────────────────────────────────────────────────
                if _redis:
                    try:
                        cached_data = _redis.get(cache_key)
                        if cached_data:
                            matches = json.loads(cached_data)
                            for i in range(0, len(matches), 15):
                                chunk = matches[i:i+15]
                                yield _sse("batch", {"matches": chunk, "batch": min(i+15, len(matches)),
                                                     "of": len(matches), "offset": i})
                                yield _keepalive()
                            yield _sse("list_done", {"total_sent": len(matches)})
                            yield _sse("done", {"status": "finished", "total_sent": len(matches), "cached": True})
                            return
                    except Exception as _ce:
                        print(f"[unified] Redis cache read failed: {_ce} — fetching fresh")

                from app.workers.sp_harvester  import fetch_upcoming_stream
                from app.workers.bt_harvester  import (
                    fetch_upcoming_matches as bt_fetch_upcoming,
                    get_full_markets as bt_get_full_markets,
                )
                from app.workers.od_harvester  import (
                    fetch_upcoming_matches as od_fetch_upcoming,
                    fetch_event_detail,
                    slug_to_od_sport_id,
                )

                od_sport_id = slug_to_od_sport_id(sport_slug)

                # ── CRITICAL: fire BT + OD bulk fetches IMMEDIATELY ───────────
                # These run concurrently while SP streams its first matches.
                # BT fetch_upcoming_matches with fetch_full=True returns all
                # matches with full markets in ~2-4 API calls.
                # OD fetch_upcoming_matches returns all matches with markets.
                # Result: 2 bulk calls instead of N×2 per-event calls.
                _bulk_pool = ThreadPoolExecutor(max_workers=2)

                def _bt_bulk():
                    try:
                        return bt_fetch_upcoming(sport_slug, max_pages=5, fetch_full=True) or []
                    except Exception as e:
                        print(f"[upcoming] BT bulk error: {e}")
                        return []

                def _od_bulk():
                    try:
                        return od_fetch_upcoming(sport_slug) or []
                    except Exception as e:
                        print(f"[upcoming] OD bulk error: {e}")
                        return []

                _f_bt_bulk = _bulk_pool.submit(_bt_bulk)
                _f_od_bulk = _bulk_pool.submit(_od_bulk)

                # Lazy bulk maps — populated as futures resolve
                bt_bulk_map: dict = {}
                od_bulk_map: dict = {}
                bt_bulk_done = False
                od_bulk_done = False

                def _refresh_bulk_maps():
                    nonlocal bt_bulk_map, od_bulk_map, bt_bulk_done, od_bulk_done
                    if not bt_bulk_done and _f_bt_bulk.done():
                        try:
                            matches_list = _f_bt_bulk.result() or []
                            bt_bulk_map = _build_bk_map(
                                matches_list, "betradar_id", "bt_parent_id"
                            )
                            print(f"[upcoming] BT bulk ready: {len(bt_bulk_map)} events")
                        except Exception as e:
                            print(f"[upcoming] BT bulk result error: {e}")
                        bt_bulk_done = True
                    if not od_bulk_done and _f_od_bulk.done():
                        try:
                            matches_list = _f_od_bulk.result() or []
                            od_bulk_map = _build_bk_map(
                                matches_list, "betradar_id", "od_parent_id", "od_match_id"
                            )
                            print(f"[upcoming] OD bulk ready: {len(od_bulk_map)} events")
                        except Exception as e:
                            print(f"[upcoming] OD bulk result error: {e}")
                        od_bulk_done = True

                all_sp_matches: dict = {}  # betradar_id → unified dict

                # ── Phase 1: Stream every SP match immediately ────────────────
                for sp_match in fetch_upcoming_stream(sport_slug, max_matches=max_m,
                                                      fetch_full_markets=fetch_full):
                    if not isinstance(sp_match, dict): continue
                    count += 1

                    # Check if bulk maps have resolved while we were streaming
                    _refresh_bulk_maps()

                    sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                    sp_clean["market_slugs"] = list(sp_clean["best"].keys())
                    sp_clean["market_count"] = len(sp_clean["best"])

                    brid = str(sp_clean.get("parent_match_id") or sp_clean.get("match_id") or "")
                    if brid:
                        all_sp_matches[brid] = sp_clean

                    # ── Instant enrichment from bulk maps (O(1) lookup) ───────
                    enriched = False
                    if brid:
                        bt_match = bt_bulk_map.get(brid)
                        if bt_match and bt_match.get("markets"):
                            b_c = _unify_match_payload(bt_match, count, mode, "bt", "BETIKA")
                            sp_clean["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                            sp_clean["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                            if brid in all_sp_matches:
                                all_sp_matches[brid]["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                all_sp_matches[brid]["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                            enriched = True

                        od_match = od_bulk_map.get(brid)
                        if od_match and od_match.get("markets"):
                            o_c = _unify_match_payload(od_match, count, mode, "od", "ODIBETS")
                            sp_clean["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                            sp_clean["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                            if brid in all_sp_matches:
                                all_sp_matches[brid]["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                all_sp_matches[brid]["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                            enriched = True

                    if enriched:
                        _merge_best(sp_clean["best"], sp_clean.get("markets_by_bk", {}))
                        sp_clean["market_slugs"] = list(sp_clean["best"].keys())
                        sp_clean["market_count"] = len(sp_clean["best"])
                        sp_clean["bk_count"]     = len(sp_clean["bookmakers"])

                    yield _sse("batch", {"matches": [sp_clean], "batch": count,
                                         "of": "unknown", "offset": count - 1})
                    yield _keepalive()

                yield _sse("list_done", {"total_sent": count})

                # ── Phase 2: Wait for bulk maps if not ready yet ──────────────
                # Then enrich all remaining unmatched events.
                if not bt_bulk_done or not od_bulk_done:
                    try:
                        _f_bt_bulk.result(timeout=15)
                        _f_od_bulk.result(timeout=15)
                    except Exception:
                        pass
                    _refresh_bulk_maps()

                # ── Phase 3: send live_updates for events not yet enriched ────
                # Also kick off per-event fallback for betradar_ids missing from bulk.
                missing_brids: list = []

                for brid, sp_match_data in all_sp_matches.items():
                    if brid and brid != "None":
                        has_bt = "bt" in sp_match_data.get("bookmakers", {})
                        has_od = "od" in sp_match_data.get("bookmakers", {})

                        # Send enrichment for matches now in bulk maps
                        update: dict = {"parent_match_id": brid, "home_team": "dummy",
                                        "bookmakers": {}, "markets_by_bk": {}}
                        changed = False

                        if not has_bt:
                            bt_match = bt_bulk_map.get(brid)
                            if bt_match and bt_match.get("markets"):
                                b_c = _unify_match_payload(bt_match, 0, mode, "bt", "BETIKA")
                                update["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                update["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                                sp_match_data["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                sp_match_data["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                                changed = True

                        if not has_od:
                            od_match = od_bulk_map.get(brid)
                            if od_match and od_match.get("markets"):
                                o_c = _unify_match_payload(od_match, 0, mode, "od", "ODIBETS")
                                update["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                update["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                                sp_match_data["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                sp_match_data["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                                changed = True

                        if changed:
                            _merge_best(sp_match_data["best"], sp_match_data["markets_by_bk"])
                            sp_match_data["market_slugs"] = list(sp_match_data["best"].keys())
                            sp_match_data["market_count"] = len(sp_match_data["best"])
                            sp_match_data["bk_count"]     = len(sp_match_data["bookmakers"])
                            yield _sse("live_update", update)
                            yield _keepalive()

                        # Queue for per-event fallback if still missing
                        still_no_bt = "bt" not in sp_match_data.get("bookmakers", {})
                        still_no_od = "od" not in sp_match_data.get("bookmakers", {})
                        if still_no_bt or still_no_od:
                            missing_brids.append(brid)

                # ── Phase 4: per-event fallback for cache misses ──────────────
                # Only fire for events genuinely absent from bulk responses.
                # In practice this should be a small minority (<10%).
                if missing_brids:
                    print(f"[upcoming] per-event fallback for {len(missing_brids)} events")

                    def _get_deep_fallback(brid):
                        bt_mkts, od_mkts = {}, {}
                        sp_data = all_sp_matches.get(brid, {})
                        has_bt = "bt" in sp_data.get("bookmakers", {})
                        has_od = "od" in sp_data.get("bookmakers", {})
                        if not has_bt:
                            try: bt_mkts = bt_get_full_markets(brid, sport_slug)
                            except: pass
                        if not has_od:
                            try: od_mkts, _ = fetch_event_detail(brid, od_sport_id)
                            except: pass
                        return brid, bt_mkts, od_mkts

                    n_workers = min(len(missing_brids) * 2, 20)
                    with ThreadPoolExecutor(max_workers=n_workers) as pool:
                        futs = [pool.submit(_get_deep_fallback, brid) for brid in missing_brids]
                        for fut in as_completed(futs):
                            try:
                                brid, bt_markets, od_markets = fut.result()
                            except Exception:
                                continue
                            if not bt_markets and not od_markets:
                                continue

                            update = {"parent_match_id": brid, "home_team": "dummy",
                                      "bookmakers": {}, "markets_by_bk": {}}

                            if bt_markets:
                                b_c = _unify_match_payload(
                                    {"markets": bt_markets, "betradar_id": brid,
                                     "home_team": "", "away_team": "", "sport": sport_slug},
                                    0, mode, "bt", "BETIKA")
                                update["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                update["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                                if brid in all_sp_matches:
                                    all_sp_matches[brid]["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                    all_sp_matches[brid]["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]

                            if od_markets:
                                o_c = _unify_match_payload(
                                    {"markets": od_markets, "betradar_id": brid,
                                     "home_team": "", "away_team": "", "sport": sport_slug},
                                    0, mode, "od", "ODIBETS")
                                update["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                update["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                                if brid in all_sp_matches:
                                    all_sp_matches[brid]["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                    all_sp_matches[brid]["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]

                            if brid in all_sp_matches:
                                _merge_best(all_sp_matches[brid]["best"],
                                            all_sp_matches[brid]["markets_by_bk"])
                                all_sp_matches[brid]["market_slugs"] = list(all_sp_matches[brid]["best"].keys())
                                all_sp_matches[brid]["market_count"] = len(all_sp_matches[brid]["best"])
                                all_sp_matches[brid]["bk_count"]     = len(all_sp_matches[brid]["bookmakers"])

                            yield _sse("live_update", update)
                            yield _keepalive()

                _bulk_pool.shutdown(wait=False)

                # ── Cache final result ────────────────────────────────────────
                if _redis and all_sp_matches:
                    try:
                        _redis.setex(cache_key, 300,
                                     json.dumps(list(all_sp_matches.values())))
                    except Exception as _we:
                        print(f"[unified] Redis cache write failed: {_we}")

                yield _sse("done", {"status": "finished", "total_sent": count})

        except Exception as exc:
            tb = traceback.format_exc()
            print("=== STREAM CRASH ==="); print(tb); print("====================")
            yield _sse("error", {"error": str(exc), "traceback": tb})

    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)


@bp_odds_customer.route("/odds/debug/unified/stream/<mode>/<sport_slug>")
def debug_stream_unified(mode: str, sport_slug: str):
    r = _get_redis()

    def _event_stream():
        yield _sse("meta", {"source": "redis_event_bus", "sport": sport_slug, "mode": mode})

        try:   
        # ══════════════════════════════════════════════════════════════════════
        # PHASE 1: INSTANT SNAPSHOT LOAD
        # ══════════════════════════════════════════════════════════════════════
            if mode == "live":
                # Get all active Betradar IDs for this sport
                active_ids = r.smembers(f"live:active_matches:{sport_slug}")
                initial_matches = []
                
                for br_id in active_ids:
                    match_data = r.hgetall(f"live:match:{br_id}")
                    if not match_data:
                        continue
                    
                    # Build the unified shell the UI expects
                    unified_shell = {
                        "parent_match_id": br_id,
                        "sport": sport_slug,
                        "is_live": True,
                        "bookmakers": {},
                        "markets_by_bk": {},
                        "best": {}
                    }
                    
                    # Inject Match State (Score, Time, Phase)
                    state_raw = match_data.get("state", "{}")
                    state = json.loads(state_raw)
                    unified_shell.update(state)
                    
                    # Inject Bookmaker Markets
                    for bk_slug, bk_name in [("sp", "SPORTPESA"), ("bt", "BETIKA"), ("od", "ODIBETS")]:
                        raw_bk = match_data.get(f"{bk_slug}_raw")
                        if raw_bk:
                            mkts = json.loads(raw_bk)
                            unified_shell["bookmakers"][bk_slug] = {
                                "slug": bk_slug, 
                                "bookmaker": bk_name, 
                                "markets": mkts, 
                                "market_count": len(mkts)
                            }
                            unified_shell["markets_by_bk"][bk_slug] = mkts

                    initial_matches.append(unified_shell)

                # Stream snapshot in chunks
                for i in range(0, len(initial_matches), 15):
                    chunk = initial_matches[i:i+15]
                    yield _sse("batch", {
                        "matches": chunk, 
                        "batch": min(i+15, len(initial_matches)), 
                        "of": len(initial_matches)
                    })
                    yield _keepalive()
                    
                yield _sse("list_done", {"total_sent": len(initial_matches)})

                # ══════════════════════════════════════════════════════════════════════
                # PHASE 2: SUBSCRIBE TO DELTAS
                # ══════════════════════════════════════════════════════════════════════
                pubsub = r.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(f"bus:live_updates:{sport_slug}")
                
                last_keepalive = time.time()
                while True:
                    msg = pubsub.get_message(timeout=1.0)
                    if msg and msg["type"] == "message":
                        try:
                            payload = json.loads(msg["data"])
                            yield _sse("live_update", payload)
                        except Exception:
                            pass
                    
                    if time.time() - last_keepalive > 10.0:
                        yield _keepalive()
                        last_keepalive = time.time()
            else:
                    cache_key = f"unified:upcoming:{sport_slug}:{max_m}:{fetch_full}"
                    _redis = _get_redis()
                    if _redis is None:
                        print("[unified] Redis unavailable — fetching fresh data")

                    # ── Cache hit ─────────────────────────────────────────────────
                    if _redis:
                        try:
                            cached_data = _redis.get(cache_key)
                            if cached_data:
                                matches = json.loads(cached_data)
                                for i in range(0, len(matches), 15):
                                    chunk = matches[i:i+15]
                                    yield _sse("batch", {"matches": chunk, "batch": min(i+15, len(matches)),
                                                        "of": len(matches), "offset": i})
                                    yield _keepalive()
                                yield _sse("list_done", {"total_sent": len(matches)})
                                yield _sse("done", {"status": "finished", "total_sent": len(matches), "cached": True})
                                return
                        except Exception as _ce:
                            print(f"[unified] Redis cache read failed: {_ce} — fetching fresh")

                    from app.workers.sp_harvester  import fetch_upcoming_stream
                    from app.workers.bt_harvester  import (
                        fetch_upcoming_matches as bt_fetch_upcoming,
                        get_full_markets as bt_get_full_markets,
                    )
                    from app.workers.od_harvester  import (
                        fetch_upcoming_matches as od_fetch_upcoming,
                        fetch_event_detail,
                        slug_to_od_sport_id,
                    )

                    od_sport_id = slug_to_od_sport_id(sport_slug)

                    # ── CRITICAL: fire BT + OD bulk fetches IMMEDIATELY ───────────
                    # These run concurrently while SP streams its first matches.
                    # BT fetch_upcoming_matches with fetch_full=True returns all
                    # matches with full markets in ~2-4 API calls.
                    # OD fetch_upcoming_matches returns all matches with markets.
                    # Result: 2 bulk calls instead of N×2 per-event calls.
                    _bulk_pool = ThreadPoolExecutor(max_workers=2)

                    def _bt_bulk():
                        try:
                            return bt_fetch_upcoming(sport_slug, max_pages=5, fetch_full=True) or []
                        except Exception as e:
                            print(f"[upcoming] BT bulk error: {e}")
                            return []

                    def _od_bulk():
                        try:
                            return od_fetch_upcoming(sport_slug) or []
                        except Exception as e:
                            print(f"[upcoming] OD bulk error: {e}")
                            return []

                    _f_bt_bulk = _bulk_pool.submit(_bt_bulk)
                    _f_od_bulk = _bulk_pool.submit(_od_bulk)

                    # Lazy bulk maps — populated as futures resolve
                    bt_bulk_map: dict = {}
                    od_bulk_map: dict = {}
                    bt_bulk_done = False
                    od_bulk_done = False

                    def _refresh_bulk_maps():
                        nonlocal bt_bulk_map, od_bulk_map, bt_bulk_done, od_bulk_done
                        if not bt_bulk_done and _f_bt_bulk.done():
                            try:
                                matches_list = _f_bt_bulk.result() or []
                                bt_bulk_map = _build_bk_map(
                                    matches_list, "betradar_id", "bt_parent_id"
                                )
                                print(f"[upcoming] BT bulk ready: {len(bt_bulk_map)} events")
                            except Exception as e:
                                print(f"[upcoming] BT bulk result error: {e}")
                            bt_bulk_done = True
                        if not od_bulk_done and _f_od_bulk.done():
                            try:
                                matches_list = _f_od_bulk.result() or []
                                od_bulk_map = _build_bk_map(
                                    matches_list, "betradar_id", "od_parent_id", "od_match_id"
                                )
                                print(f"[upcoming] OD bulk ready: {len(od_bulk_map)} events")
                            except Exception as e:
                                print(f"[upcoming] OD bulk result error: {e}")
                            od_bulk_done = True

                    all_sp_matches: dict = {}  # betradar_id → unified dict

                    # ── Phase 1: Stream every SP match immediately ────────────────
                    for sp_match in fetch_upcoming_stream(sport_slug, max_matches=max_m,
                                                        fetch_full_markets=fetch_full):
                        if not isinstance(sp_match, dict): continue
                        count += 1

                        # Check if bulk maps have resolved while we were streaming
                        _refresh_bulk_maps()

                        sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                        sp_clean["market_slugs"] = list(sp_clean["best"].keys())
                        sp_clean["market_count"] = len(sp_clean["best"])

                        brid = str(sp_clean.get("parent_match_id") or sp_clean.get("match_id") or "")
                        if brid:
                            all_sp_matches[brid] = sp_clean

                        # ── Instant enrichment from bulk maps (O(1) lookup) ───────
                        enriched = False
                        if brid:
                            bt_match = bt_bulk_map.get(brid)
                            if bt_match and bt_match.get("markets"):
                                b_c = _unify_match_payload(bt_match, count, mode, "bt", "BETIKA")
                                sp_clean["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                sp_clean["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                                if brid in all_sp_matches:
                                    all_sp_matches[brid]["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                    all_sp_matches[brid]["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                                enriched = True

                            od_match = od_bulk_map.get(brid)
                            if od_match and od_match.get("markets"):
                                o_c = _unify_match_payload(od_match, count, mode, "od", "ODIBETS")
                                sp_clean["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                sp_clean["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                                if brid in all_sp_matches:
                                    all_sp_matches[brid]["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                    all_sp_matches[brid]["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                                enriched = True

                        if enriched:
                            _merge_best(sp_clean["best"], sp_clean.get("markets_by_bk", {}))
                            sp_clean["market_slugs"] = list(sp_clean["best"].keys())
                            sp_clean["market_count"] = len(sp_clean["best"])
                            sp_clean["bk_count"]     = len(sp_clean["bookmakers"])

                        yield _sse("batch", {"matches": [sp_clean], "batch": count,
                                            "of": "unknown", "offset": count - 1})
                        yield _keepalive()

                    yield _sse("list_done", {"total_sent": count})

                    # ── Phase 2: Wait for bulk maps if not ready yet ──────────────
                    # Then enrich all remaining unmatched events.
                    if not bt_bulk_done or not od_bulk_done:
                        try:
                            _f_bt_bulk.result(timeout=15)
                            _f_od_bulk.result(timeout=15)
                        except Exception:
                            pass
                        _refresh_bulk_maps()

                    # ── Phase 3: send live_updates for events not yet enriched ────
                    # Also kick off per-event fallback for betradar_ids missing from bulk.
                    missing_brids: list = []

                    for brid, sp_match_data in all_sp_matches.items():
                        if brid and brid != "None":
                            has_bt = "bt" in sp_match_data.get("bookmakers", {})
                            has_od = "od" in sp_match_data.get("bookmakers", {})

                            # Send enrichment for matches now in bulk maps
                            update: dict = {"parent_match_id": brid, "home_team": "dummy",
                                            "bookmakers": {}, "markets_by_bk": {}}
                            changed = False

                            if not has_bt:
                                bt_match = bt_bulk_map.get(brid)
                                if bt_match and bt_match.get("markets"):
                                    b_c = _unify_match_payload(bt_match, 0, mode, "bt", "BETIKA")
                                    update["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                    update["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                                    sp_match_data["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                    sp_match_data["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                                    changed = True

                            if not has_od:
                                od_match = od_bulk_map.get(brid)
                                if od_match and od_match.get("markets"):
                                    o_c = _unify_match_payload(od_match, 0, mode, "od", "ODIBETS")
                                    update["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                    update["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                                    sp_match_data["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                    sp_match_data["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                                    changed = True

                            if changed:
                                _merge_best(sp_match_data["best"], sp_match_data["markets_by_bk"])
                                sp_match_data["market_slugs"] = list(sp_match_data["best"].keys())
                                sp_match_data["market_count"] = len(sp_match_data["best"])
                                sp_match_data["bk_count"]     = len(sp_match_data["bookmakers"])
                                yield _sse("live_update", update)
                                yield _keepalive()

                            # Queue for per-event fallback if still missing
                            still_no_bt = "bt" not in sp_match_data.get("bookmakers", {})
                            still_no_od = "od" not in sp_match_data.get("bookmakers", {})
                            if still_no_bt or still_no_od:
                                missing_brids.append(brid)

                    # ── Phase 4: per-event fallback for cache misses ──────────────
                    # Only fire for events genuinely absent from bulk responses.
                    # In practice this should be a small minority (<10%).
                    if missing_brids:
                        print(f"[upcoming] per-event fallback for {len(missing_brids)} events")

                        def _get_deep_fallback(brid):
                            bt_mkts, od_mkts = {}, {}
                            sp_data = all_sp_matches.get(brid, {})
                            has_bt = "bt" in sp_data.get("bookmakers", {})
                            has_od = "od" in sp_data.get("bookmakers", {})
                            if not has_bt:
                                try: bt_mkts = bt_get_full_markets(brid, sport_slug)
                                except: pass
                            if not has_od:
                                try: od_mkts, _ = fetch_event_detail(brid, od_sport_id)
                                except: pass
                            return brid, bt_mkts, od_mkts

                        n_workers = min(len(missing_brids) * 2, 20)
                        with ThreadPoolExecutor(max_workers=n_workers) as pool:
                            futs = [pool.submit(_get_deep_fallback, brid) for brid in missing_brids]
                            for fut in as_completed(futs):
                                try:
                                    brid, bt_markets, od_markets = fut.result()
                                except Exception:
                                    continue
                                if not bt_markets and not od_markets:
                                    continue

                                update = {"parent_match_id": brid, "home_team": "dummy",
                                        "bookmakers": {}, "markets_by_bk": {}}

                                if bt_markets:
                                    b_c = _unify_match_payload(
                                        {"markets": bt_markets, "betradar_id": brid,
                                        "home_team": "", "away_team": "", "sport": sport_slug},
                                        0, mode, "bt", "BETIKA")
                                    update["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                    update["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]
                                    if brid in all_sp_matches:
                                        all_sp_matches[brid]["bookmakers"]["bt"]    = b_c["bookmakers"]["bt"]
                                        all_sp_matches[brid]["markets_by_bk"]["bt"] = b_c["markets_by_bk"]["bt"]

                                if od_markets:
                                    o_c = _unify_match_payload(
                                        {"markets": od_markets, "betradar_id": brid,
                                        "home_team": "", "away_team": "", "sport": sport_slug},
                                        0, mode, "od", "ODIBETS")
                                    update["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                    update["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]
                                    if brid in all_sp_matches:
                                        all_sp_matches[brid]["bookmakers"]["od"]    = o_c["bookmakers"]["od"]
                                        all_sp_matches[brid]["markets_by_bk"]["od"] = o_c["markets_by_bk"]["od"]

                                if brid in all_sp_matches:
                                    _merge_best(all_sp_matches[brid]["best"],
                                                all_sp_matches[brid]["markets_by_bk"])
                                    all_sp_matches[brid]["market_slugs"] = list(all_sp_matches[brid]["best"].keys())
                                    all_sp_matches[brid]["market_count"] = len(all_sp_matches[brid]["best"])
                                    all_sp_matches[brid]["bk_count"]     = len(all_sp_matches[brid]["bookmakers"])

                                yield _sse("live_update", update)
                                yield _keepalive()

                    _bulk_pool.shutdown(wait=False)

                    # ── Cache final result ────────────────────────────────────────
                    if _redis and all_sp_matches:
                        try:
                            _redis.setex(cache_key, 300,
                                        json.dumps(list(all_sp_matches.values())))
                        except Exception as _we:
                            print(f"[unified] Redis cache write failed: {_we}")

                    yield _sse("done", {"status": "finished", "total_sent": count})

        except Exception as exc:
                tb = traceback.format_exc()
                print("=== STREAM CRASH ==="); print(tb); print("====================")
                yield _sse("error", {"error": str(exc), "traceback": tb})
                yield _sse("done", {"status": "finished"})

    return Response(stream_with_context(_event_stream()), headers=config._SSE_HEADERS)
# ══════════════════════════════════════════════════════════════════════════════
# HELPER: emit live_update SSE events for one BK bulk result
# ══════════════════════════════════════════════════════════════════════════════

def _bk_enrich_generator(
    raw_list:      list,
    bk_map:        dict,
    bk_slug:       str,
    bk_name:       str,
    mode:          str,
    seen_br_ids:   set,
    sp_team_index: dict,
    sp_event_map:  dict,
    matched_ids:   set,
):
    """
    Yield _sse payloads for each BK match that corresponds to an SP event.
    Mutates matched_ids in-place to avoid duplicate sends.
    """
    if not raw_list:
        return

    for br_id in list(seen_br_ids):
        match_raw = bk_map.get(br_id)

        # Fallback: team-name match
        if not match_raw:
            for bk_m in raw_list:
                if not isinstance(bk_m, dict): continue
                h = bk_m.get("home_team", "")
                a = bk_m.get("away_team", "")
                if h and a and sp_team_index.get(_norm_team_key(h, a)) == br_id:
                    match_raw = bk_m
                    bk_br = str(bk_m.get("betradar_id") or "")
                    if bk_br and bk_br not in ("None", ""):
                        bk_map[bk_br] = bk_m
                    break

        if not match_raw or not match_raw.get("markets"):
            continue

        c = _unify_match_payload(match_raw, 0, mode, bk_slug, bk_name)
        if bk_slug not in c.get("bookmakers", {}):
            continue

        matched_ids.add(br_id)
        yield _sse("live_update", {
            "parent_match_id": br_id,
            "home_team":       "dummy",
            "bookmakers":      {bk_slug: c["bookmakers"][bk_slug]},
            "markets_by_bk":   {bk_slug: c["markets_by_bk"][bk_slug]},
        })

    # New cards this BK has that SP didn't send
    for bk_br_id, raw_m in bk_map.items():
        if not isinstance(raw_m, dict): continue
        if bk_br_id in seen_br_ids or bk_br_id in matched_ids: continue
        if not bk_br_id or bk_br_id in ("None", ""): continue
        h = raw_m.get("home_team", "")
        a = raw_m.get("away_team", "")
        if h and a and sp_team_index.get(_norm_team_key(h, a)):
            continue
        c = _unify_match_payload(raw_m, 0, mode, bk_slug, bk_name)
        c["is_live"] = True
        _merge_best(c["best"], c["markets_by_bk"])
        c["market_slugs"] = list(c["best"].keys())
        c["market_count"] = len(c["best"])
        yield _sse("batch", {"matches": [c], "batch": 0, "of": "unknown", "offset": 0})
        seen_br_ids.add(bk_br_id)


def _send_bk_enrichment(*args, **kwargs):
    """No-op shim — actual work is done by _bk_enrich_generator."""
    pass