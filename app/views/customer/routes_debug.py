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
    
    # 1. Normalize Time (Force .000Z to match Javascript ISO standards)
    st = raw_match.get("start_time") or ""
    if st and not st.endswith("Z") and "+" not in st:
        st = st.replace(" ", "T")
        if len(st) == 19:  # e.g. "2026-04-11T15:30:00"
            st += ".000Z"
            
    # 2. Normalize Market Slugs (Fixing Betika vs SportPesa naming conflicts)
    best_mock = {}
    bk_markets = {}
    
    for mkt, outs in raw_match.get("markets", {}).items():
        m = mkt.lower()
        
        # Standardize main markets
        m = m.replace("over_under_goals", "over_under")
        if m == "1x2": 
            m = "match_winner"
        elif m.endswith("_1x2"): 
            m = m.replace("_1x2", "_match_winner")
            
        # Clean up messy Betika fallback slugs
        m = m.replace("___", "_").replace("_&_", "_and_").replace(" ", "_")
        m = m.replace("1st_half", "first_half").replace("2nd_half", "second_half")
        m = m.replace("both_teams_to_score", "btts")
        
        # Remove double underscores just in case
        m = re.sub(r"_+", "_", m)
        
        best_mock[m] = {}
        bk_markets[m] = {}
        
        for out_key, price in outs.items():
            # Standardize outcome keys (e.g. Draw -> X)
            ok = out_key.replace("draw", "X").replace("Draw", "X")
            best_mock[m][ok] = {"odd": price, "bk": bk_slug}
            bk_markets[m][ok] = {"price": price}

    # 3. Handle ID fallbacks correctly
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
            bk_slug: {
                "bookmaker": bk_name, 
                "slug": bk_slug, 
                "markets": bk_markets, 
                "market_count": len(bk_markets)
            }
        },
        "markets_by_bk": {bk_slug: bk_markets}, 
        "best": best_mock, 
        "best_odds": best_mock,
        "has_arb": False, "has_ev": False, "has_sharp": False
    }


# ══════════════════════════════════════════════════════════════════════════════
# DIRECT SPORTPESA STREAM
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/debug/sportpesa/stream/<mode>/<sport_slug>")
def debug_stream_sportpesa(mode: str, sport_slug: str):
    fetch_full = request.args.get("full", "true").lower() in ("1", "true")
    debug_ou   = request.args.get("debug_ou", "false").lower() in ("1", "true")
    max_m      = int(request.args.get("max", 20))
    log_event("debug_sp_stream", {"sport": sport_slug, "mode": mode})

    def _gen():
        from app.workers.sp_harvester import fetch_upcoming_stream, fetch_live_stream
        yield _sse("meta", {"source": "sportpesa_direct", "sport": _normalise_sport_slug(sport_slug), "mode": mode, "now": _now_utc().isoformat(), "total": max_m if mode == "upcoming" else 100})
        try:
            stream = fetch_live_stream(sport_slug, fetch_full_markets=fetch_full, debug_ou=debug_ou) if mode == "live" else fetch_upcoming_stream(sport_slug, max_matches=max_m, fetch_full_markets=fetch_full, debug_ou=debug_ou)
            count = 0
            for sp_match in stream:
                count += 1
                ui_match = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")
                yield _sse("batch", {"matches": [ui_match], "batch": count, "of": "unknown", "offset": count - 1})
                yield _keepalive()
            yield _sse("list_done", {"total_sent": count})
            yield _sse("done", {"status": "finished", "total_sent": count})
        except Exception as exc: 
            yield _sse("error", {"error": str(exc)})
            
    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)


# ══════════════════════════════════════════════════════════════════════════════
# DIRECT BETIKA STREAM
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/debug/betika/stream/<mode>/<sport_slug>")
def debug_stream_betika(mode: str, sport_slug: str):
    fetch_full = request.args.get("full", "true").lower() in ("1", "true")
    max_m      = int(request.args.get("max", 20))
    log_event("debug_bt_stream", {"sport": sport_slug, "mode": mode})

    def _gen():
        from app.workers.bt_harvester import fetch_upcoming_stream, fetch_live_stream
        yield _sse("meta", {"source": "betika_direct", "sport": _normalise_sport_slug(sport_slug), "mode": mode, "now": _now_utc().isoformat(), "total": max_m if mode == "upcoming" else 100})
        try:
            stream = fetch_live_stream(sport_slug, fetch_full_markets=fetch_full) if mode == "live" else fetch_upcoming_stream(sport_slug, max_matches=max_m, fetch_full_markets=fetch_full)
            count = 0
            for bt_match in stream:
                count += 1
                ui_match = _unify_match_payload(bt_match, count, mode, "bt", "BETIKA")
                yield _sse("batch", {"matches": [ui_match], "batch": count, "of": "unknown", "offset": count - 1})
                yield _keepalive()
            yield _sse("list_done", {"total_sent": count})
            yield _sse("done", {"status": "finished", "total_sent": count})
        except Exception as exc: 
            yield _sse("error", {"error": str(exc)})
            
    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)