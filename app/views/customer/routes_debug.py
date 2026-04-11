from flask import request, Response, stream_with_context
from .blueprint import bp_odds_customer
from . import config
from .utils import _now_utc, _normalise_sport_slug, _sse, _keepalive
from app.utils.decorators_ import log_event

# ══════════════════════════════════════════════════════════════════════════════
# UNIFIED DIRECT STREAM (Replicates tasks_market_align.py in real-time)
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
            # 1. Fetch SportPesa Stream (Source of Truth)
            stream = fetch_live_stream(sport_slug, fetch_full_markets=fetch_full) if mode == "live" else fetch_upcoming_stream(sport_slug, max_matches=max_m, fetch_full_markets=fetch_full)
            count = 0
            
            for sp_match in stream:
                count += 1
                betradar_id = sp_match.get("betradar_id")
                
                # 2. Replicate tasks_upcoming.py: Fetch Betika explicitly by Betradar ID!
                bt_markets = {}
                if betradar_id:
                    try:
                        bt_markets = get_full_markets(betradar_id, sport_slug)
                    except Exception:
                        pass
                
                # 3. Merge Markets exactly like tasks_market_align.py
                markets_by_bk = {
                    "sp": sp_match.get("markets", {})
                }
                if bt_markets:
                    markets_by_bk["bt"] = bt_markets

                best_mock = {}
                for bk_slug, mkts in markets_by_bk.items():
                    for mkt_slug, outcomes in mkts.items():
                        if mkt_slug not in best_mock:
                            best_mock[mkt_slug] = {}
                        for out_key, price in outcomes.items():
                            p = float(price) if not isinstance(price, dict) else float(price.get("price", 0))
                            if p > 1.0:
                                if out_key not in best_mock[mkt_slug] or p > best_mock[mkt_slug][out_key]["odd"]:
                                    best_mock[mkt_slug][out_key] = {"odd": p, "bk": bk_slug}

                # 4. Standardize Time Format
                st = sp_match.get("start_time") or ""
                if st and not st.endswith("Z") and "+" not in st:
                    st = st.replace(" ", "T")
                    if len(st) == 19: st += ".000Z"

                # 5. Yield fully combined payload to React
                ui_match = {
                    "match_id": count,
                    "join_key": f"br_{betradar_id}" if betradar_id else f"sp_{sp_match.get('sp_game_id')}",
                    "parent_match_id": betradar_id, 
                    "home_team": sp_match.get("home_team"), 
                    "away_team": sp_match.get("away_team"),
                    "competition": sp_match.get("competition"), 
                    "sport": sport_slug,
                    "start_time": st, 
                    "status": "IN_PLAY" if mode == "live" else "PRE_MATCH",
                    "is_live": mode == "live", 
                    "bk_count": len(markets_by_bk), 
                    "market_count": len(best_mock),
                    "market_slugs": list(best_mock.keys()),
                    "bookmakers": {
                        bk: {"slug": bk, "markets": mkts} for bk, mkts in markets_by_bk.items()
                    },
                    "markets_by_bk": markets_by_bk, 
                    "best": best_mock, 
                    "best_odds": best_mock,
                    "has_arb": False, "has_ev": False, "has_sharp": False
                }
                
                yield _sse("batch", {"matches": [ui_match], "batch": count, "of": "unknown", "offset": count - 1})
                yield _keepalive()
                
            yield _sse("list_done", {"total_sent": count})
            yield _sse("done", {"status": "finished", "total_sent": count})
        except Exception as exc: 
            yield _sse("error", {"error": str(exc)})
            
    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)