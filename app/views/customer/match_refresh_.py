from flask import request, Response, stream_with_context
from .blueprint import bp_odds_customer
from . import config
from .utils import _now_utc, _normalise_sport_slug, _sse, _keepalive
from app.utils.decorators_ import log_event
import re
import json
import time
import os

# ══════════════════════════════════════════════════════════════════════════════
# UNIVERSAL MATCH NORMALIZER (BULLETPROOF CANONICAL PARSER)
# ══════════════════════════════════════════════════════════════════════════════
def _unify_match_payload(raw_match: dict, count: int, mode: str, bk_slug: str, bk_name: str) -> dict:
    if not raw_match: return {}
    
    st = str(raw_match.get("start_time") or "")
    if st and not st.endswith("Z") and "+" not in st:
        st = st.replace(" ", "T")
        if len(st) == 19: st += ".000Z"
            
    # 🟢 FIXED: Aggressive string casting for NULL team names
    h_val = raw_match.get("home_team") or raw_match.get("home_team_name")
    a_val = raw_match.get("away_team") or raw_match.get("away_team_name")
    comps = raw_match.get("competitors")
    if isinstance(comps, list):
        if not h_val and len(comps) > 0 and isinstance(comps[0], dict): h_val = comps[0].get("name")
        if not a_val and len(comps) > 1 and isinstance(comps[1], dict): a_val = comps[1].get("name")
            
    h_team = str(h_val or "Home")
    a_team = str(a_val or "Away")
    
    h_clean = re.sub(r'[^a-z0-9]', '_', h_team.lower()).strip('_')
    a_clean = re.sub(r'[^a-z0-9]', '_', a_team.lower()).strip('_')
    
    sport_val = raw_match.get("sport") or raw_match.get("sport_name") or "soccer"
    if isinstance(sport_val, dict): sport_val = sport_val.get("name", "soccer")
    sport_slug = _normalise_sport_slug(str(sport_val))

    best_mock = {}
    bk_markets = {}
    
    raw_list = raw_match.get("markets", {})
    if not raw_list: raw_list = []
    
    standardized_markets = {}
    
    if isinstance(raw_list, list):
        for mkt_obj in raw_list:
            if not isinstance(mkt_obj, dict): continue
            m_id = mkt_obj.get("id")
            spec = str(mkt_obj.get("specValue") or mkt_obj.get("specialValue") or "")
            if spec.endswith(".00"): spec = spec[:-3]
            elif spec.endswith(".0"): spec = spec[:-2]
            
            name = str(mkt_obj.get("name") or "").lower()
            m = name
            
            if m_id in (52, 105): m = f"over_under_{spec}"
            elif m_id == 54: m = f"first_half_over_under_{spec}"
            elif m_id in (51, 184): m = f"asian_handicap_{spec}"
            elif m_id == 53: m = f"first_half_asian_handicap_{spec}"
            elif m_id in (55, 151): m = f"european_handicap_{spec}"
            elif m_id in (208, 196): m = f"result_and_over_under_{spec}"
            elif m_id in (386, 124): m = "btts_and_result"
            elif m_id in (10, 194): m = "match_winner"
            elif m_id in (43, 138): m = "btts"
            elif m_id in (46, 147): m = "double_chance"
            elif m_id in (47, 166): m = "draw_no_bet"
            
            standardized_markets[m] = {}
            for sel in mkt_obj.get("selections", []):
                if not isinstance(sel, dict): continue
                ok = str(sel.get("shortName") or sel.get("name") or "")
                try: price = float(sel.get("odds", 0))
                except (ValueError, TypeError): price = 0.0
                if price > 1.0: standardized_markets[m][ok] = price
    else:
        for mkt_slug, outcomes in raw_list.items():
            m = str(mkt_slug or "")
            standardized_markets[m] = {}
            if not isinstance(outcomes, dict): continue
            for out_key, price in outcomes.items():
                try: p = float(price) if not isinstance(price, dict) else float(price.get("price", 0))
                except (ValueError, TypeError): p = 0.0
                if p > 1.0: standardized_markets[m][str(out_key)] = p

    for mkt_slug, outcomes in standardized_markets.items():
        if not outcomes: continue
        
        m = str(mkt_slug).lower()
        m = re.sub(r"^(soccer|basketball|tennis|ice_hockey|volleyball|cricket|rugby|table_tennis|handball|mma|boxing|darts|esoccer|baseball|american_football)_+", "", m)
        m = m.replace("1st_half___", "first_half_").replace("1st_half_", "first_half_")
        m = m.replace("2nd_half___", "second_half_").replace("2nd_half_", "second_half_")
        if h_clean and len(h_clean) > 3 and h_clean in m: m = m.replace(h_clean, "home")
        if a_clean and len(a_clean) > 3 and a_clean in m: m = m.replace(a_clean, "away")
        m = m.replace("___", "_").replace("_&_", "_and_").replace(" ", "_")
        m = m.replace("both_teams_to_score", "btts").replace("over_under_goals", "over_under").replace("total_goals", "over_under").replace("total_points", "over_under")
        
        if m in ("1x2", "moneyline", "3_way", "match_winner"): m = "match_winner"
        elif m in ("btts", "both_teams_to_score", "gg_ng", "both_teams_to_score__gg_ng_"): m = "btts"
        if m.endswith("_1x2"): m = m.replace("_1x2", "_match_winner")
        if m.startswith("1x2_"): m = m.replace("1x2_", "match_winner_")
        if m == "match_winner_btts": m = "btts_and_result"
        if m.startswith("match_winner_over_under") or m.startswith("match_winner_and_over_under"): m = m.replace("match_winner_over_under", "result_and_over_under").replace("match_winner_and_over_under", "result_and_over_under")
        if m == "draw_no_bet_full_time": m = "draw_no_bet"
        
        m = re.sub(r"exact_goals_\d+$", "exact_goals", m)
        m = m.replace(".", "_")
        if "handicap_-" in m: m = m.replace("handicap_-", "handicap_minus_")
        elif "handicap_" in m: m = m.replace("handicap_+", "handicap_")
        m = re.sub(r"_0_0$", "_0", m)
        m = re.sub(r"_+", "_", m).strip("_")

        bk_markets[m] = {}
        best_mock[m] = {}
        for ok_raw, p in outcomes.items():
            ok_raw = str(ok_raw)
            ok_lower = ok_raw.lower()
            
            ok = "X" if ("draw" in ok_lower and "or" not in ok_lower and "&" not in ok_lower) else ok_raw
            if h_clean and h_clean in ok_lower.replace(" ", "_") and "or" not in ok_lower and "&" not in ok_lower: ok = "1"
            if a_clean and a_clean in ok_lower.replace(" ", "_") and "or" not in ok_lower and "&" not in ok_lower: ok = "2"
            
            ok_lower = ok.lower()
            
            if "double_chance" in m:
                ok = ok_lower.replace("or", "").replace("/", "").replace("draw", "x").replace(h_clean, "1").replace(a_clean, "2").replace("home", "1").replace("away", "2").replace(" ", "").upper()
                if ok == "X1": ok = "1X"
                elif ok == "2X": ok = "X2"
                elif ok == "21": ok = "12"
            elif "btts_and" in m or "result_and" in m:
                suffix = "Yes" if "yes" in ok_lower else "No" if "no" in ok_lower else "Over" if "over" in ok_lower else "Under" if "under" in ok_lower else ""
                prefix = "X" if "draw" in ok_lower or "x" in ok_lower.split() else "1" if h_clean in ok_lower.replace(" ", "_") or "home" in ok_lower or "1" in ok_lower.split() else "2" if a_clean in ok_lower.replace(" ", "_") or "away" in ok_lower or "2" in ok_lower.split() else ""
                
                if prefix and suffix: ok = f"{prefix} {suffix}"
                else:
                    ok = ok_lower.replace(" & ", " ").replace("and", "").replace("yes", "Yes").replace("no", "No").replace("draw", "X").replace("home", "1").replace("away", "2").replace(h_clean, "1").replace(a_clean, "2")
                    ok = ok.replace("1", "1 ").replace("x", "X ").replace("2", "2 ").replace("  ", " ").strip().title().replace(" X", " X")
            else:
                if ok_lower in ("yes", "y"): ok = "Yes"
                elif ok_lower in ("no", "n"): ok = "No"
                elif "over" in ok_lower or ok_lower == "ov": ok = "Over"
                elif "under" in ok_lower or ok_lower == "un": ok = "Under"
                elif ok_lower == "odd": ok = "Odd"
                elif ok_lower == "even": ok = "Even"
                elif ok_lower == "home": ok = "1"
                elif ok_lower == "away": ok = "2"
                elif ok_lower == "1x": ok = "1X"
                elif ok_lower == "x2": ok = "X2"
                elif ok_lower == "12": ok = "12"
                
                if "over_under" in m and ok not in ("Over", "Under"):
                    if ok_lower.startswith("o"): ok = "Over"
                    elif ok_lower.startswith("u"): ok = "Under"
                
                if "exact_goals" in m: ok = ok.replace(" OR MORE", "+").replace(" or more", "+").replace(" and over", "+")
                if "correct_score" in m: ok = ok.replace("-", ":")

            best_mock[m][ok] = {"odd": p, "bk": bk_slug}
            bk_markets[m][ok] = {"price": p}

    actual_id = str(raw_match.get(f"{bk_slug}_match_id") or raw_match.get("match_id") or count)
    b_id = str(raw_match.get("betradar_id") or raw_match.get(f"{bk_slug}_parent_id") or "")

    return {
        "match_id": actual_id,
        "join_key": f"br_{b_id}" if b_id and b_id != "None" else f"{bk_slug}_{actual_id}",
        "parent_match_id": b_id if b_id and b_id != "None" else None, 
        "home_team": h_team, 
        "away_team": a_team,
        "competition": str(raw_match.get("competition_name") or raw_match.get("competition", "")), 
        "sport": sport_slug,
        "start_time": st, 
        "status": "IN_PLAY" if mode == "live" else "PRE_MATCH",
        "is_live": mode == "live", 
        "bk_count": 1, 
        "market_count": len(bk_markets),
        "market_slugs": list(bk_markets.keys()),
        "bookmakers": {
            bk_slug: {"bookmaker": bk_name, "slug": bk_slug, "markets": bk_markets, "market_count": len(bk_markets)}
        },
        "markets_by_bk": {bk_slug: bk_markets}, 
        "best": best_mock, 
        "best_odds": best_mock,
        "has_arb": False, "has_ev": False, "has_sharp": False
    }


# ══════════════════════════════════════════════════════════════════════════════
# UNIFIED DIRECT STREAM (BULK FETCH + REAL-TIME PUBSUB)
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/debug/unified/stream/<mode>/<sport_slug>")
def debug_stream_unified(mode: str, sport_slug: str):
    fetch_full = request.args.get("full", "true").lower() in ("1", "true")
    max_m      = int(request.args.get("max", 50))
    log_event("debug_unified_stream", {"sport": sport_slug, "mode": mode})

    def _gen():
        yield _sse("meta", {"source": "unified_direct", "sport": sport_slug, "mode": mode, "now": _now_utc().isoformat()})
        
        try:
            count = 0

            if mode == "live":
                from app.workers.sp_live_harvester import fetch_live_stream, SPORT_SLUG_MAP
                from app.workers.bt_harvester import fetch_live_matches as bt_fetch_live, slug_to_bt_sport_id
                from app.workers.od_harvester import fetch_live_matches as od_fetch_live

                sport_id = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)
                
                bt_data = bt_fetch_live(slug_to_bt_sport_id(sport_slug)) or []
                od_data = od_fetch_live(sport_slug) or []
                
                bt_map = {str(m.get("betradar_id") or m.get("bt_parent_id")): m for m in bt_data}
                od_map = {str(m.get("betradar_id") or m.get("od_parent_id")): m for m in od_data}
                
                sp_event_map = {}
                active_live_br_ids = []
                seen_br_ids = set()

                stream = fetch_live_stream(sport_slug, fetch_full_markets=True)
                for sp_match in stream:
                    count += 1
                    betradar_id = str(sp_match.get("betradar_id") or "")
                    if betradar_id and betradar_id != "0" and betradar_id != "None":
                        active_live_br_ids.append(betradar_id)
                        seen_br_ids.add(betradar_id)
                        sp_event_map[betradar_id] = str(sp_match.get("sp_match_id") or sp_match.get("match_id") or "")

                    sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                    sp_clean["is_live"] = True

                    bt_match = bt_map.get(betradar_id)
                    if bt_match and bt_match.get("markets"):
                        b_clean = _unify_match_payload(bt_match, count, mode, "bt", "BETIKA")
                        sp_clean["bookmakers"]["bt"] = b_clean["bookmakers"]["bt"]
                        sp_clean["markets_by_bk"]["bt"] = b_clean["markets_by_bk"]["bt"]
                        
                    od_match = od_map.get(betradar_id)
                    if od_match and od_match.get("markets"):
                        o_clean = _unify_match_payload(od_match, count, mode, "od", "ODIBETS")
                        sp_clean["bookmakers"]["od"] = o_clean["bookmakers"]["od"]
                        sp_clean["markets_by_bk"]["od"] = o_clean["markets_by_bk"]["od"]

                    sp_clean["bk_count"] = len(sp_clean["bookmakers"])

                    for bk_slug, mkts in sp_clean["markets_by_bk"].items():
                        for mkt_slug, outcomes in mkts.items():
                            if mkt_slug not in sp_clean["best"]: sp_clean["best"][mkt_slug] = {}
                            for out_key, price_dict in outcomes.items():
                                p = price_dict["price"]
                                if out_key not in sp_clean["best"][mkt_slug] or p > sp_clean["best"][mkt_slug][out_key]["odd"]:
                                    sp_clean["best"][mkt_slug][out_key] = {"odd": p, "bk": bk_slug}

                    sp_clean["market_slugs"] = list(sp_clean["best"].keys())
                    sp_clean["market_count"] = len(sp_clean["best"])

                    yield _sse("batch", {"matches": [sp_clean], "batch": count, "of": "unknown", "offset": count - 1})
                    yield _keepalive()
                    
                for br_id, bt_match in bt_map.items():
                    if br_id not in seen_br_ids and br_id and br_id != "None":
                        count += 1
                        active_live_br_ids.append(br_id)
                        bt_clean = _unify_match_payload(bt_match, count, mode, "bt", "BETIKA")
                        bt_clean["is_live"] = True
                        
                        od_match = od_map.get(br_id)
                        if od_match and od_match.get("markets"):
                            o_clean = _unify_match_payload(od_match, count, mode, "od", "ODIBETS")
                            bt_clean["bookmakers"]["od"] = o_clean["bookmakers"]["od"]
                            bt_clean["markets_by_bk"]["od"] = o_clean["markets_by_bk"]["od"]
                            seen_br_ids.add(br_id) 
                            
                        bt_clean["bk_count"] = len(bt_clean["bookmakers"])
                        for bk_slug, mkts in bt_clean["markets_by_bk"].items():
                            for mkt_slug, outcomes in mkts.items():
                                if mkt_slug not in bt_clean["best"]: bt_clean["best"][mkt_slug] = {}
                                for out_key, price_dict in outcomes.items():
                                    p = price_dict["price"]
                                    if out_key not in bt_clean["best"][mkt_slug] or p > bt_clean["best"][mkt_slug][out_key]["odd"]:
                                        bt_clean["best"][mkt_slug][out_key] = {"odd": p, "bk": bk_slug}
                        bt_clean["market_slugs"] = list(bt_clean["best"].keys())
                        bt_clean["market_count"] = len(bt_clean["best"])
                        yield _sse("batch", {"matches": [bt_clean], "batch": count, "of": "unknown", "offset": count - 1})
                        yield _keepalive()
                        seen_br_ids.add(br_id)
                        
                for br_id, od_match in od_map.items():
                    if br_id not in seen_br_ids and br_id and br_id != "None":
                        count += 1
                        active_live_br_ids.append(br_id)
                        od_clean = _unify_match_payload(od_match, count, mode, "od", "ODIBETS")
                        od_clean["is_live"] = True
                        for bk_slug, mkts in od_clean["markets_by_bk"].items():
                            for mkt_slug, outcomes in mkts.items():
                                if mkt_slug not in od_clean["best"]: od_clean["best"][mkt_slug] = {}
                                for out_key, price_dict in outcomes.items():
                                    p = price_dict["price"]
                                    if out_key not in od_clean["best"][mkt_slug] or p > od_clean["best"][mkt_slug][out_key]["odd"]:
                                        od_clean["best"][mkt_slug][out_key] = {"odd": p, "bk": bk_slug}
                        od_clean["market_slugs"] = list(od_clean["best"].keys())
                        od_clean["market_count"] = len(od_clean["best"])
                        yield _sse("batch", {"matches": [od_clean], "batch": count, "of": "unknown", "offset": count - 1})
                        yield _keepalive()
                        seen_br_ids.add(br_id)

                yield _sse("list_done", {"total_sent": count})

            # ── UPCOMING BATCH INITIALIZATION ──
            else:
                from app.workers.sp_harvester import fetch_upcoming_stream
                from app.workers.bt_harvester import get_full_markets
                from app.workers.od_harvester import fetch_event_detail, slug_to_od_sport_id
                
                od_sport_id = slug_to_od_sport_id(sport_slug)
                stream = fetch_upcoming_stream(sport_slug, max_matches=max_m, fetch_full_markets=fetch_full)
                for sp_match in stream:
                    count += 1
                    betradar_id = str(sp_match.get("betradar_id") or "")
                    bt_markets = {}; od_markets = {}
                    
                    if betradar_id and betradar_id != "None":
                        try: bt_markets = get_full_markets(betradar_id, sport_slug)
                        except Exception: pass
                        try: od_markets, _ = fetch_event_detail(betradar_id, od_sport_id)
                        except Exception: pass
                    
                    sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                    
                    if bt_markets:
                        bt_clean = _unify_match_payload({"markets": bt_markets, "betradar_id": betradar_id, "home_team": sp_clean["home_team"], "away_team": sp_clean["away_team"]}, count, mode, "bt", "BETIKA")
                        sp_clean["bookmakers"]["bt"] = bt_clean["bookmakers"]["bt"]
                        sp_clean["markets_by_bk"]["bt"] = bt_clean["markets_by_bk"]["bt"]
                    if od_markets:
                        od_clean = _unify_match_payload({"markets": od_markets, "betradar_id": betradar_id, "home_team": sp_clean["home_team"], "away_team": sp_clean["away_team"]}, count, mode, "od", "ODIBETS")
                        sp_clean["bookmakers"]["od"] = od_clean["bookmakers"]["od"]
                        sp_clean["markets_by_bk"]["od"] = od_clean["markets_by_bk"]["od"]

                    sp_clean["bk_count"] = len(sp_clean["bookmakers"])
                    for bk_slug, mkts in sp_clean["markets_by_bk"].items():
                        for mkt_slug, outcomes in mkts.items():
                            if mkt_slug not in sp_clean["best"]: sp_clean["best"][mkt_slug] = {}
                            for out_key, price_dict in outcomes.items():
                                p = price_dict["price"]
                                if out_key not in sp_clean["best"][mkt_slug] or p > sp_clean["best"][mkt_slug][out_key]["odd"]:
                                    sp_clean["best"][mkt_slug][out_key] = {"odd": p, "bk": bk_slug}

                    sp_clean["market_slugs"] = list(sp_clean["best"].keys())
                    sp_clean["market_count"] = len(sp_clean["best"])
                    
                    yield _sse("batch", {"matches": [sp_clean], "batch": count, "of": "unknown", "offset": count - 1})
                    yield _keepalive()
                    
                yield _sse("list_done", {"total_sent": count})
                yield _sse("done", {"status": "finished", "total_sent": count})
                return

            # ── REAL-TIME LIVE PUB/SUB ──
            import redis
            r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6382/0"), decode_responses=True)
            pubsub = r.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(f"sp:live:sport:{sport_id}")

            last_poll = time.time()
            
            while True:
                msg = pubsub.get_message(timeout=0.5)
                if msg and msg["type"] == "message":
                    try:
                        payload = json.loads(msg["data"])
                        msg_type = payload.get("type")

                        ev_id = payload.get("event_id")
                        br_id = next((k for k, v in sp_event_map.items() if v == ev_id), None)
                        if not br_id: br_id = str(ev_id)

                        if msg_type == "event_update":
                            state = payload.get("state", {})
                            yield _sse("live_update", {
                                "parent_match_id": br_id,
                                "home_team": "dummy", 
                                "match_time": state.get("matchTime", payload.get("match_time", "")),
                                "score_home": payload.get("score_home"),
                                "score_away": payload.get("score_away"),
                                "status": payload.get("phase", state.get("currentEventPhase", "")),
                                "is_live": True
                            })

                        elif msg_type == "market_update":
                            norm_sels = payload.get("normalised_selections", [])
                            slug = payload.get("market_slug")
                            if norm_sels and slug:
                                outs = {s["outcome_key"]: {"price": float(s["odds"])} for s in norm_sels if float(s.get("odds",0)) > 1}
                                if outs:
                                    yield _sse("live_update", {
                                        "parent_match_id": br_id,
                                        "home_team": "dummy",
                                        "bookmakers": {"sp": {"slug": "sp", "markets": {slug: outs}}},
                                        "markets_by_bk": {"sp": {slug: outs}}
                                    })
                    except Exception: pass

                if time.time() - last_poll > 10.0:
                    last_poll = time.time()
                    try:
                        bt_data = bt_fetch_live(slug_to_bt_sport_id(sport_slug)) or []
                        od_data = od_fetch_live(sport_slug) or []
                        bt_map = {str(m.get("betradar_id") or m.get("bt_parent_id")): m for m in bt_data}
                        od_map = {str(m.get("betradar_id") or m.get("od_parent_id")): m for m in od_data}
                        
                        for br_id in active_live_br_ids:
                            up_payload = {"parent_match_id": br_id, "home_team": "dummy", "bookmakers": {}, "markets_by_bk": {}}
                            if br_id in bt_map:
                                b_clean = _unify_match_payload(bt_map[br_id], 0, "live", "bt", "BETIKA")
                                up_payload["bookmakers"]["bt"] = b_clean["bookmakers"]["bt"]
                                up_payload["markets_by_bk"]["bt"] = b_clean["markets_by_bk"]["bt"]
                                
                            if br_id in od_map:
                                o_clean = _unify_match_payload(od_map[br_id], 0, "live", "od", "ODIBETS")
                                up_payload["bookmakers"]["od"] = o_clean["bookmakers"]["od"]
                                up_payload["markets_by_bk"]["od"] = o_clean["markets_by_bk"]["od"]

                            if up_payload["bookmakers"]:
                                yield _sse("live_update", up_payload)
                    except Exception: pass

                yield _keepalive()
                
        except Exception as exc: 
            yield _sse("error", {"error": str(exc)})
            
    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)