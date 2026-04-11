from flask import request, Response, stream_with_context
from .blueprint import bp_odds_customer
from . import config
from .utils import _now_utc, _normalise_sport_slug, _sse, _keepalive
from app.utils.decorators_ import log_event
import re

# ══════════════════════════════════════════════════════════════════════════════
# UNIVERSAL MATCH NORMALIZER
# Guarantees Betika and SportPesa JSON payloads are 100% identical for the UI
# ══════════════════════════════════════════════════════════════════════════════
def _unify_match_payload(raw_match: dict, count: int, mode: str, bk_slug: str, bk_name: str) -> dict:
    
    # 1. Normalize Time
    st = raw_match.get("start_time") or ""
    if st and not st.endswith("Z") and "+" not in st:
        st = st.replace(" ", "T")
        if len(st) == 19: st += ".000Z"
            
    # Clean Team Names for regex matching
    h_clean = re.sub(r'[^a-z0-9]', '_', raw_match.get("home_team", "").lower()).strip('_')
    a_clean = re.sub(r'[^a-z0-9]', '_', raw_match.get("away_team", "").lower()).strip('_')

    best_mock = {}
    bk_markets = {}
    
    for mkt, outs in raw_match.get("markets", {}).items():
        m = mkt.lower()
        
        # 2. Strip sports prefixes
        m = re.sub(r"^(soccer|basketball|tennis|ice_hockey|volleyball|cricket|rugby|table_tennis|handball|mma|boxing|darts|esoccer|baseball|american_football)_+", "", m)
        
        # 3. Strip Team Names (Betika injects them into slugs)
        if h_clean and h_clean in m: m = m.replace(h_clean, "home")
        if a_clean and a_clean in m: m = m.replace(a_clean, "away")
            
        # 4. Standardize core names
        m = m.replace("___", "_").replace("_&_", "_and_").replace(" ", "_")
        m = m.replace("1st_half", "first_half").replace("2nd_half", "second_half")
        m = m.replace("both_teams_to_score", "btts")
        m = m.replace("over_under_goals", "over_under").replace("total_goals", "over_under")
        
        # Normalize 1x2 to match_winner
        if m == "1x2" or m == "moneyline": m = "match_winner"
        m = m.replace("_1x2", "_match_winner").replace("1x2_", "match_winner_")

        # 5. Exact Goals Cleanup (Betika appends the highest cap, e.g. exact_goals_6)
        m = re.sub(r"exact_goals_\d+$", "exact_goals", m)
        
        # 6. Normalize Numbers/Lines to X_Y format (e.g. over_under_5 -> over_under_5_0)
        def fix_line(match):
            val = match.group(1)
            if "_" not in val: return f"_{val}_0"
            return f"_{val}"
        
        if "over_under" in m or "handicap" in m:
            m = re.sub(r"_(\d+(?:_\d+)?)$", fix_line, m)

        m = re.sub(r"_+", "_", m) # Clean double underscores

        best_mock[m] = {}
        bk_markets[m] = {}
        
        for out_key, price in outs.items():
            p = float(price) if not isinstance(price, dict) else float(price.get("price", 0))
            if p > 1.0:
                # Standardize Outcomes
                ok = out_key.replace("draw", "X").replace("Draw", "X")
                
                # Standardize exact goals (6+ vs 6 OR MORE)
                if "exact_goals" in m:
                    ok = ok.replace(" OR MORE", "+").replace(" or more", "+")
                    
                # Standardize Correct score keys (e.g. "0-0" -> "0:0")
                if "correct_score" in m:
                    ok = ok.replace("-", ":")
                    
                ok_lower = ok.lower()
                if ok_lower == "yes": ok = "Yes"
                if ok_lower == "no": ok = "No"
                if ok_lower == "over": ok = "Over"
                if ok_lower == "under": ok = "Under"

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
# UNIFIED DIRECT STREAM
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/debug/unified/stream/<mode>/<sport_slug>")
def debug_stream_unified(mode: str, sport_slug: str):
    fetch_full = request.args.get("full", "true").lower() in ("1", "true")
    max_m      = int(request.args.get("max", 20))
    log_event("debug_unified_stream", {"sport": sport_slug, "mode": mode})

    def _gen():
        from app.workers.sp_harvester import fetch_upcoming_stream, fetch_live_stream
        from app.workers.bt_harvester import get_full_markets
        yield _sse("meta", {"source": "unified_direct", "sport": sport_slug, "mode": mode, "now": _now_utc().isoformat()})
        
        try:
            stream = fetch_live_stream(sport_slug, fetch_full_markets=fetch_full) if mode == "live" else fetch_upcoming_stream(sport_slug, max_matches=max_m, fetch_full_markets=fetch_full)
            count = 0
            
            for sp_match in stream:
                count += 1
                betradar_id = sp_match.get("betradar_id")
                bt_markets = {}
                if betradar_id:
                    try: bt_markets = get_full_markets(betradar_id, sport_slug)
                    except Exception: pass
                
                # Run BOTH payloads through the normalizer to guarantee identical strings
                sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                
                if bt_markets:
                    bt_mock = {"markets": bt_markets}
                    bt_clean = _unify_match_payload(bt_mock, count, mode, "bt", "BETIKA")
                    
                    # Merge them
                    sp_clean["bookmakers"]["bt"] = bt_clean["bookmakers"]["bt"]
                    sp_clean["markets_by_bk"]["bt"] = bt_clean["markets_by_bk"]["bt"]
                    sp_clean["bk_count"] = 2
                    
                    for mkt_slug, outcomes in bt_clean["markets_by_bk"]["bt"].items():
                        if mkt_slug not in sp_clean["best"]:
                            sp_clean["best"][mkt_slug] = {}
                        for out_key, price_dict in outcomes.items():
                            p = price_dict["price"]
                            if out_key not in sp_clean["best"][mkt_slug] or p > sp_clean["best"][mkt_slug][out_key]["odd"]:
                                sp_clean["best"][mkt_slug][out_key] = {"odd": p, "bk": "bt"}

                sp_clean["market_slugs"] = list(sp_clean["best"].keys())
                sp_clean["market_count"] = len(sp_clean["best"])
                
                yield _sse("batch", {"matches": [sp_clean], "batch": count, "of": "unknown", "offset": count - 1})
                yield _keepalive()
                
            yield _sse("list_done", {"total_sent": count})
            yield _sse("done", {"status": "finished", "total_sent": count})
        except Exception as exc: 
            yield _sse("error", {"error": str(exc)})
            
    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)