from flask import request, Response, stream_with_context
from .blueprint import bp_odds_customer
from . import config
from .utils import _now_utc, _normalise_sport_slug, _sse, _keepalive

@bp_odds_customer.route("/odds/debug/sportpesa/stream/<mode>/<sport_slug>")
def debug_stream_sportpesa(mode: str, sport_slug: str):
    from app.utils.decorators_ import log_event
    fetch_full = request.args.get("full", "true").lower() in ("1", "true")
    debug_ou   = request.args.get("debug_ou", "false").lower() in ("1", "true")
    max_m      = int(request.args.get("max", 20))
    
    log_event("debug_sp_stream", {"sport": sport_slug, "mode": mode})

    def _gen():
        from app.workers.sp_harvester import fetch_upcoming_stream, fetch_live_stream
        yield _sse("meta", {"source": "sportpesa_direct_harvester", "sport": _normalise_sport_slug(sport_slug), "mode": mode, "fetch_full_markets": fetch_full, "now": _now_utc().isoformat(), "total": max_m if mode == "upcoming" else 100})
        
        try:
            stream = fetch_live_stream(sport_slug, fetch_full_markets=fetch_full, debug_ou=debug_ou) if mode == "live" else fetch_upcoming_stream(sport_slug, max_matches=max_m, fetch_full_markets=fetch_full, debug_ou=debug_ou)
            count = 0
            for sp_match in stream:
                count += 1
                best_mock = {}; bk_markets = {}
                for mkt, outs in sp_match["markets"].items():
                    best_mock[mkt] = {}; bk_markets[mkt] = {}
                    for out_key, price in outs.items():
                        best_mock[mkt][out_key] = {"odd": price, "bk": "sp"}
                        bk_markets[mkt][out_key] = {"price": price}

                ui_match = {
                    "match_id": int(sp_match["sp_game_id"]),
                    "join_key": f"br_{sp_match['betradar_id']}" if sp_match.get("betradar_id") else f"sp_{sp_match['sp_game_id']}",
                    "parent_match_id": sp_match.get("betradar_id"), "home_team": sp_match["home_team"], "away_team": sp_match["away_team"],
                    "competition": sp_match["competition"], "sport": _normalise_sport_slug(sp_match["sport"]),
                    "start_time": sp_match["start_time"], "status": "IN_PLAY" if mode == "live" else "PRE_MATCH",
                    "is_live": mode == "live", "bk_count": 1, "market_count": sp_match["market_count"],
                    "market_slugs": list(sp_match["markets"].keys()),
                    "bookmakers": {"sp": {"bookmaker": "SPORTPESA", "slug": "sp", "markets": bk_markets, "market_count": sp_match["market_count"]}},
                    "markets_by_bk": {"sp": bk_markets}, "best": best_mock, "best_odds": best_mock,
                    "has_arb": False, "has_ev": False, "has_sharp": False
                }
                yield _sse("batch", {"matches": [ui_match], "batch": count, "of": "unknown", "offset": count - 1})
                yield _keepalive()
            yield _sse("list_done", {"total_sent": count}); yield _sse("done", {"status": "finished", "total_sent": count})
        except Exception as exc: yield _sse("error", {"error": str(exc)})
            
    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)