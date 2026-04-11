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
    st = raw_match.get("start_time") or ""
    if st and not st.endswith("Z") and "+" not in st:
        st = st.replace(" ", "T")
        if len(st) == 19: st += ".000Z"
            
    h_clean = re.sub(r'[^a-z0-9]', '_', raw_match.get("home_team", "").lower()).strip('_')
    a_clean = re.sub(r'[^a-z0-9]', '_', raw_match.get("away_team", "").lower()).strip('_')

    best_mock = {}
    bk_markets = {}
    
    raw_list = raw_match.get("markets", {})
    items = raw_list.items() if isinstance(raw_list, dict) else []

    # ── AGGRESSIVE CANONICAL ALIGNMENT (SP + BT + OD) ──
    for mkt_slug, outcomes in items:
        m = mkt_slug.lower()
        
        # 1. Clean Sport Prefixes
        m = re.sub(r"^(soccer|basketball|tennis|ice_hockey|volleyball|cricket|rugby|table_tennis|handball|mma|boxing|darts|esoccer|baseball|american_football)_+", "", m)
        
        # 2. Clean Time Prefixes
        m = m.replace("1st_half___", "first_half_").replace("1st_half_", "first_half_")
        m = m.replace("2nd_half___", "second_half_").replace("2nd_half_", "second_half_")
        
        # 3. Clean Team Names out of Slugs
        if h_clean and len(h_clean) > 3 and h_clean in m: m = m.replace(h_clean, "home")
        if a_clean and len(a_clean) > 3 and a_clean in m: m = m.replace(a_clean, "away")
        m = m.replace("___", "_").replace("_&_", "_and_").replace(" ", "_")
        
        # 4. Standardize Core Markets
        m = m.replace("both_teams_to_score", "btts").replace("over_under_goals", "over_under").replace("total_goals", "over_under").replace("total_points", "over_under")
        if m in ("1x2", "moneyline", "match_winner", "3_way"): m = "match_winner"
        if m.endswith("_1x2"): m = m.replace("_1x2", "_match_winner")
        if m.startswith("1x2_"): m = m.replace("1x2_", "match_winner_")
        if m == "match_winner_btts": m = "btts_and_result"
        if m.startswith("match_winner_over_under") or m.startswith("match_winner_and_over_under"): m = m.replace("match_winner_over_under", "result_and_over_under").replace("match_winner_and_over_under", "result_and_over_under")
        if m == "draw_no_bet_full_time": m = "draw_no_bet"
        
        # 5. Format Decimals & Handicaps perfectly
        m = re.sub(r"exact_goals_\d+$", "exact_goals", m)
        m = m.replace(".", "_")
        if "handicap_-" in m: m = m.replace("handicap_-", "handicap_minus_")
        elif "handicap_" in m: m = m.replace("handicap_+", "handicap_")
        m = m.replace("_0_0", "_0") # Fix OdiBets Asian Handicap 0.0 -> 0
        m = re.sub(r"_+", "_", m).strip("_")

        bk_markets[m] = {}
        best_mock[m] = {}
        for out_key, price in outcomes.items():
            p = float(price) if not isinstance(price, dict) else float(price.get("price", 0))
            if p > 1.0:
                ok = str(out_key)
                ok_lower = ok.lower()
                
                # Normalize Base 1, X, 2
                if "draw" in ok_lower and "or" not in ok_lower and "&" not in ok_lower: ok = "X"
                elif h_clean and h_clean in ok_lower.replace(" ", "_") and "or" not in ok_lower and "&" not in ok_lower: ok = "1"
                elif a_clean and a_clean in ok_lower.replace(" ", "_") and "or" not in ok_lower and "&" not in ok_lower: ok = "2"
                
                ok_lower = ok.lower()
                
                # Force Double Chance Harmony (e.g. OdiBets "1 or X" vs Betika "1X")
                if "double_chance" in m:
                    ok = ok_lower.replace("or", "").replace("/", "").replace("draw", "x").replace(h_clean, "1").replace(a_clean, "2").replace(" ", "").upper()
                    if ok == "X1": ok = "1X"
                    elif ok == "2X": ok = "X2"
                    elif ok == "21": ok = "12"
                    
                # Force BTTS Combos Harmony
                elif "btts_and" in m or "result_and" in m:
                    if h_clean in ok_lower and "yes" in ok_lower: ok = "1 Yes"
                    elif h_clean in ok_lower and "no" in ok_lower: ok = "1 No"
                    elif a_clean in ok_lower and "yes" in ok_lower: ok = "2 Yes"
                    elif a_clean in ok_lower and "no" in ok_lower: ok = "2 No"
                    elif "draw" in ok_lower and "yes" in ok_lower: ok = "X Yes"
                    elif "draw" in ok_lower and "no" in ok_lower: ok = "X No"
                    else:
                        ok = ok_lower.replace(" & ", " ").replace("and", "").replace("yes", "Yes").replace("no", "No").replace("draw", "X")
                        ok = ok.replace("home", "1").replace("away", "2").replace(h_clean, "1").replace(a_clean, "2")
                        ok = ok.replace("1", "1 ").replace("x", "X ").replace("2", "2 ").replace("  ", " ").strip()
                
                # General Cleanup
                else:
                    if ok_lower == "yes": ok = "Yes"
                    elif ok_lower == "no": ok = "No"
                    elif ok_lower == "over": ok = "Over"
                    elif ok_lower == "under": ok = "Under"
                    elif ok_lower == "odd": ok = "Odd"
                    elif ok_lower == "even": ok = "Even"
                    elif ok_lower == "home": ok = "1"
                    elif ok_lower == "away": ok = "2"
                    elif ok_lower == "1x": ok = "1X"
                    elif ok_lower == "x2": ok = "X2"
                    elif ok_lower == "12": ok = "12"
                    if "exact_goals" in m: ok = ok.replace(" OR MORE", "+").replace(" or more", "+").replace(" and over", "+")
                    if "correct_score" in m: ok = ok.replace("-", ":")

                best_mock[m][ok] = {"odd": p, "bk": bk_slug}
                bk_markets[m][ok] = {"price": p}

    actual_id = raw_match.get(f"{bk_slug}_match_id") or raw_match.get("match_id") or count
    b_id = raw_match.get("betradar_id") or raw_match.get(f"{bk_slug}_parent_id")

    return {
        "match_id": int(actual_id),
        "join_key": f"br_{b_id}" if b_id else f"{bk_slug}_{actual_id}",
        "parent_match_id": b_id, 
        "home_team": raw_match.get("home_team", "Home"), 
        "away_team": raw_match.get("away_team", "Away"),
        "competition": raw_match.get("competition_name") or raw_match.get("competition", ""), 
        "sport": _normalise_sport_slug(raw_match.get("sport", "soccer")),
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

            # ── 1. LIVE BATCH INITIALIZATION (ULTRA-FAST BULK FETCH) ─────────
            if mode == "live":
                from app.workers.sp_live_harvester import fetch_live_stream, SPORT_SLUG_MAP
                from app.workers.bt_harvester import fetch_live_matches as bt_fetch_live, slug_to_bt_sport_id
                from app.workers.od_harvester import fetch_live_matches as od_fetch_live

                sport_id = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)
                
                # 1. Bulk Fetch Background APIs Instantly
                bt_data = bt_fetch_live(slug_to_bt_sport_id(sport_slug)) or []
                od_data = od_fetch_live(sport_slug) or []
                
                # Map them entirely in memory via Betradar ID
                bt_map = {str(m.get("betradar_id") or m.get("bt_parent_id")): m for m in bt_data}
                od_map = {str(m.get("betradar_id") or m.get("od_parent_id")): m for m in od_data}
                
                sp_event_map = {}
                active_live_br_ids = []
                seen_br_ids = set()

                # 2. Iterate SP Stream (which parses markets automatically)
                stream = fetch_live_stream(sport_slug, fetch_full_markets=True)
                for sp_match in stream:
                    count += 1
                    betradar_id = str(sp_match.get("betradar_id") or "")
                    if betradar_id and betradar_id != "0":
                        active_live_br_ids.append(betradar_id)
                        seen_br_ids.add(betradar_id)
                        sp_event_map[betradar_id] = str(sp_match.get("sp_match_id") or sp_match.get("match_id") or "")

                    sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                    sp_clean["is_live"] = True

                    # Instantly Merge BT and OD from bulk dictionaries
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

                    # Build Global Best
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
                    
                # 3. Inject any remaining matches that Betika/OdiBets have but SP doesn't
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

            # ── 1. UPCOMING BATCH INITIALIZATION ───────────────────────────────
            else:
                from app.workers.sp_harvester import fetch_upcoming_stream
                from app.workers.bt_harvester import get_full_markets
                from app.workers.od_harvester import fetch_event_detail, slug_to_od_sport_id
                
                od_sport_id = slug_to_od_sport_id(sport_slug)
                stream = fetch_upcoming_stream(sport_slug, max_matches=max_m, fetch_full_markets=fetch_full)
                for sp_match in stream:
                    count += 1
                    betradar_id = sp_match.get("betradar_id")
                    bt_markets = {}; od_markets = {}
                    
                    if betradar_id:
                        try: bt_markets = get_full_markets(betradar_id, sport_slug)
                        except Exception: pass
                        try: od_markets, _ = fetch_event_detail(betradar_id, od_sport_id)
                        except Exception: pass
                    
                    sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                    
                    if bt_markets:
                        bt_clean = _unify_match_payload({"markets": bt_markets, "betradar_id": betradar_id}, count, mode, "bt", "BETIKA")
                        sp_clean["bookmakers"]["bt"] = bt_clean["bookmakers"]["bt"]
                        sp_clean["markets_by_bk"]["bt"] = bt_clean["markets_by_bk"]["bt"]
                    if od_markets:
                        od_clean = _unify_match_payload({"markets": od_markets, "betradar_id": betradar_id}, count, mode, "od", "ODIBETS")
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

            # ── 2. REAL-TIME LIVE PUB/SUB ──────────────────────────────────────
            import redis
            r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
            pubsub = r.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(f"sp:live:sport:{sport_id}")

            last_poll = time.time()
            
            while True:
                msg = pubsub.get_message(timeout=0.5)
                if msg and msg["type"] == "message":
                    try:
                        payload = json.loads(msg["data"])
                        msg_type = payload.get("type")

                        # Resolve Betradar ID
                        ev_id = payload.get("event_id")
                        br_id = next((k for k, v in sp_event_map.items() if v == ev_id), None)
                        if not br_id: br_id = str(ev_id)

                        # Fast Event Updates
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

                        # Fast Market Updates
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

                # Fetch fresh Betika/OdiBets data every 10 seconds to save bandwidth
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