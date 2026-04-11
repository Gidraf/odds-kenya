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
# UNIVERSAL MATCH NORMALIZER
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
    
    raw_list = raw_match.get("markets", [])
    items = raw_list.items() if isinstance(raw_list, dict) else []

    # SportPesa Raw List Parser
    if isinstance(raw_list, list):
        for mkt_obj in raw_list:
            m_id = mkt_obj.get("id")
            spec = mkt_obj.get("specValue")
            name = mkt_obj.get("name", "").lower()
            
            m = name
            if m_id in (52, 54, 226, 340, 352, 353): 
                line = str(spec).replace(".", "_")
                prefix = "first_half_" if m_id == 54 else "home_" if m_id == 353 else "away_" if m_id == 352 else ""
                m = f"{prefix}over_under_{line}"
            elif m_id in (51, 53, 339): 
                line = str(spec).replace(".", "_").replace("-", "minus_")
                prefix = "first_half_" if m_id == 53 else ""
                m = f"{prefix}asian_handicap_{line}"
            elif m_id == 55: 
                line = str(spec).replace("-", "minus_").replace("+", "")
                m = f"european_handicap_{line}"
            elif m_id == 208: 
                m = f"result_and_over_under_{str(spec).replace('.','_')}"
            
            m = re.sub(r"^(soccer|volleyball|tennis|basketball)_+", "", m)
            if h_clean in m: m = m.replace(h_clean, "home")
            if a_clean in m: m = m.replace(a_clean, "away")
            m = m.replace("3_way", "match_winner").replace("draw_no_bet_full_time", "draw_no_bet")
            m = m.replace(" ", "_").replace("-", "_").replace("___", "_")
            m = re.sub(r"_+", "_", m).strip("_")

            bk_markets[m] = {}
            best_mock[m] = {}
            for sel in mkt_obj.get("selections", []):
                ok = sel.get("shortName") or sel.get("name")
                ok = ok.replace("UN", "Under").replace("OV", "Over").replace("Yes", "Yes").replace("No", "No")
                price = float(sel.get("odds", 0))
                if price > 1.0:
                    bk_markets[m][ok] = {"price": price}
                    best_mock[m][ok] = {"odd": price, "bk": bk_slug}

    # Betika & OdiBets Canonical Parser
    else:
        for mkt_slug, outcomes in items:
            m = mkt_slug.lower()
            m = re.sub(r"^(soccer|basketball|tennis|ice_hockey|volleyball|cricket|rugby|table_tennis|handball|mma|boxing|darts|esoccer|baseball|american_football)_+", "", m)
            m = m.replace("1st_half___", "first_half_").replace("1st_half_", "first_half_")
            m = m.replace("2nd_half___", "second_half_").replace("2nd_half_", "second_half_")
            m = m.replace("1st_2nd_half_", "both_halves_")
            if h_clean and len(h_clean) > 3 and h_clean in m: m = m.replace(h_clean, "home")
            if a_clean and len(a_clean) > 3 and a_clean in m: m = m.replace(a_clean, "away")
            m = m.replace("___", "_").replace("_&_", "_and_").replace(" ", "_")
            m = m.replace("both_teams_to_score", "btts")
            m = m.replace("over_under_goals", "over_under").replace("total_goals", "over_under")
            
            if m == "1x2" or m == "moneyline": m = "match_winner"
            if m.endswith("_1x2"): m = m.replace("_1x2", "_match_winner")
            if m.startswith("1x2_"): m = m.replace("1x2_", "match_winner_")
            if m == "match_winner_btts": m = "btts_and_result"
            if m.startswith("match_winner_over_under"): m = m.replace("match_winner_over_under", "result_and_over_under")
            
            m = re.sub(r"exact_goals_\d+$", "exact_goals", m)
            m = re.sub(r"_+", "_", m).strip("_")

            bk_markets[m] = {}
            best_mock[m] = {}
            for out_key, price in outcomes.items():
                p = float(price) if not isinstance(price, dict) else float(price.get("price", 0))
                if p > 1.0:
                    ok = out_key.replace("draw", "X").replace("Draw", "X")
                    if "exact_goals" in m: ok = ok.replace(" OR MORE", "+").replace(" or more", "+")
                    if "correct_score" in m: ok = ok.replace("-", ":")
                        
                    ok_lower = ok.lower()
                    if ok_lower == "yes": ok = "Yes"
                    elif ok_lower == "no": ok = "No"
                    elif ok_lower == "over": ok = "Over"
                    elif ok_lower == "under": ok = "Under"
                    elif ok_lower == "odd": ok = "Odd"
                    elif ok_lower == "even": ok = "Even"
                    elif ok_lower == "home": ok = "1"
                    elif ok_lower == "away": ok = "2"

                    best_mock[m][ok] = {"odd": p, "bk": bk_slug}
                    bk_markets[m][ok] = {"price": p}

    actual_id = raw_match.get(f"{bk_slug}_match_id") or raw_match.get(f"{bk_slug}_game_id") or raw_match.get("match_id") or count
    b_id = raw_match.get("betradar_id") or raw_match.get(f"{bk_slug}_parent_id")

    return {
        "match_id": int(actual_id),
        "join_key": f"br_{b_id}" if b_id else f"{bk_slug}_{actual_id}",
        "parent_match_id": b_id, 
        "home_team": raw_match.get("home_team", ""), 
        "away_team": raw_match.get("away_team", ""),
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
# UNIFIED DIRECT STREAM (NOW WITH REAL-TIME PUBSUB)
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/debug/unified/stream/<mode>/<sport_slug>")
def debug_stream_unified(mode: str, sport_slug: str):
    fetch_full = request.args.get("full", "true").lower() in ("1", "true")
    max_m      = int(request.args.get("max", 50))
    log_event("debug_unified_stream", {"sport": sport_slug, "mode": mode})

    def _gen():
        from app.workers.sp_harvester import fetch_upcoming_stream
        from app.workers.sp_live_harvester import fetch_live_events, fetch_live_markets, SPORT_SLUG_MAP
        from app.workers.bt_harvester import get_full_markets, get_live_match_markets
        from app.workers.od_harvester import fetch_event_detail, slug_to_od_sport_id
        
        od_sport_id = slug_to_od_sport_id(sport_slug)
        active_live_br_ids = []
        sp_event_map = {} # Maps externalId (betradar) to internal SP id

        yield _sse("meta", {"source": "unified_direct", "sport": sport_slug, "mode": mode, "now": _now_utc().isoformat()})
        
        try:
            count = 0

            # ── 1. LIVE INITIALIZATION ─────────────────────────────────────────
            if mode == "live":
                sport_id = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)
                
                # Use the new sp_live_harvester method to get accurate externalIds
                raw_events = fetch_live_events(sport_id, limit=max_m)
                sp_event_ids = [ev["id"] for ev in raw_events]
                sp_markets_raw = fetch_live_markets(sp_event_ids, sport_id) if sp_event_ids else []

                mkt_by_event = {}
                for m in sp_markets_raw:
                    eid = m.get("eventId")
                    if eid: mkt_by_event.setdefault(eid, []).append(m)

                for ev in raw_events:
                    count += 1
                    betradar_id = str(ev.get("externalId") or "")
                    sp_event_id = ev.get("id")
                    if betradar_id and betradar_id != "0":
                        active_live_br_ids.append(betradar_id)
                        sp_event_map[betradar_id] = sp_event_id

                    state = ev.get("state", {})
                    score = state.get("matchScore", {})
                    
                    sp_match = {
                        "sp_match_id": sp_event_id,
                        "betradar_id": betradar_id if betradar_id and betradar_id != "0" else None,
                        "home_team": ev.get("competitors", [{},{}])[0].get("name", "Home"),
                        "away_team": ev.get("competitors", [{},{}])[1].get("name", "Away"),
                        "competition": ev.get("tournament", {}).get("name", ""),
                        "start_time": ev.get("kickoffTimeUTC", ""),
                        "match_time": state.get("matchTime", ""),
                        "current_score": f"{score.get('home','')}-{score.get('away','')}",
                        "event_status": state.get("currentEventPhase", ""),
                        "markets": mkt_by_event.get(sp_event_id, [])
                    }

                    bt_markets, od_markets = {}, {}
                    if sp_match["betradar_id"]:
                        try: bt_markets, _ = get_live_match_markets(sp_match["betradar_id"], sport_slug)
                        except Exception: pass
                        try: od_markets, _ = fetch_event_detail(sp_match["betradar_id"], od_sport_id)
                        except Exception: pass

                    sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                    sp_clean["match_time"] = sp_match["match_time"]
                    sp_clean["score_home"] = score.get("home", "")
                    sp_clean["score_away"] = score.get("away", "")
                    sp_clean["is_live"] = True

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

            # ── 1. UPCOMING INITIALIZATION ─────────────────────────────────────
            else:
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
            # Only runs if mode == "live"
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

                        # Fast Event Updates (Clock, Score, Phase)
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

                        # Market Odds Updates from SP
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

                # 6-Second polling loop for BT and OD
                if time.time() - last_poll > 6.0:
                    last_poll = time.time()
                    for br_id in active_live_br_ids:
                        try:
                            bt_mkts, _ = get_live_match_markets(br_id, sport_slug)
                            od_mkts, _ = fetch_event_detail(br_id, od_sport_id)
                            
                            up_payload = {
                                "parent_match_id": br_id, 
                                "home_team": "dummy", 
                                "bookmakers": {}, 
                                "markets_by_bk": {}
                            }
                            
                            if bt_mkts:
                                bt_clean = _unify_match_payload({"markets": bt_mkts, "betradar_id": br_id}, 0, "live", "bt", "BETIKA")
                                up_payload["bookmakers"]["bt"] = bt_clean["bookmakers"]["bt"]
                                up_payload["markets_by_bk"]["bt"] = bt_clean["markets_by_bk"]["bt"]
                                
                            if od_mkts:
                                od_clean = _unify_match_payload({"markets": od_mkts, "betradar_id": br_id}, 0, "live", "od", "ODIBETS")
                                up_payload["bookmakers"]["od"] = od_clean["bookmakers"]["od"]
                                up_payload["markets_by_bk"]["od"] = od_clean["markets_by_bk"]["od"]

                            if up_payload["bookmakers"]:
                                yield _sse("live_update", up_payload)
                        except Exception: pass

                yield _keepalive()
                
        except Exception as exc: 
            yield _sse("error", {"error": str(exc)})
            
    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)