from flask import request, Response, stream_with_context
from .blueprint import bp_odds_customer
from . import config
from .stream_services import _stream_matches, _sse_stream
from .cache_services import _stream_from_cache
from .utils import _now_utc, _sse

@bp_odds_customer.route("/odds/stream/upcoming/<sport_slug>")
def stream_upcoming(sport_slug: str):
    from app.utils.decorators_ import log_event
    batch = min(50, max(5, int(request.args.get("batch", config._STREAM_BATCH))))
    log_event("odds_stream_upcoming", {"sport": sport_slug})

    def _gen():
        try:
            yield from _stream_matches(
                sport_slug, mode="upcoming", comp_filter=(request.args.get("comp", "") or "").strip(), team_filter=(request.args.get("team", "") or "").strip(),
                has_arb=request.args.get("has_arb", "") in ("1", "true"), sort=request.args.get("sort", "start_time"), date_str=request.args.get("date", ""), from_dt=request.args.get("from_dt", ""), to_dt=request.args.get("to_dt", ""),
                batch_size=batch, include_analytics=request.args.get("analytics", "") in ("1", "true"), listen_live=True
            )
        except Exception: yield from _stream_from_cache("upcoming", sport_slug, batch)

    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)

@bp_odds_customer.route("/odds/stream/live/<sport_slug>")
def stream_live(sport_slug: str):
    from app.utils.decorators_ import log_event
    batch = min(50, max(5, int(request.args.get("batch", config._STREAM_BATCH))))
    log_event("odds_stream_live", {"sport": sport_slug})

    def _gen():
        try: yield from _stream_matches(sport_slug, mode="live", comp_filter=(request.args.get("comp", "") or "").strip(), team_filter=(request.args.get("team", "") or "").strip(), sort=request.args.get("sort", "start_time"), batch_size=batch, include_analytics=request.args.get("analytics", "") in ("1", "true"), listen_live=True)
        except Exception: yield from _stream_from_cache("live", sport_slug, batch)

    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)

@bp_odds_customer.route("/odds/stream/results")
@bp_odds_customer.route("/odds/stream/results/<date_str>")
def stream_results(date_str: str = ""):
    from app.utils.decorators_ import log_event
    if not date_str: date_str = _now_utc().strftime("%Y-%m-%d")
    batch = min(50, max(5, int(request.args.get("batch", config._STREAM_BATCH))))
    log_event("odds_stream_results", {"date": date_str})

    def _gen():
        try: yield from _stream_matches((request.args.get("sport", "") or "").strip() or "all", mode="finished", date_str=date_str, batch_size=batch, listen_live=False)
        except Exception as exc:
            yield _sse("error", {"error": str(exc)})
            yield _sse("done", {"total_sent": 0})

    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)

@bp_odds_customer.route("/stream/odds")
def stream_odds_ws(): return Response(stream_with_context(_sse_stream(config._WS_CHANNEL)), mimetype="text/event-stream", headers=config._SSE_HEADERS)

@bp_odds_customer.route("/stream/arb")
def stream_arb(): return Response(stream_with_context(_sse_stream(config._ARB_CHANNEL)), mimetype="text/event-stream", headers=config._SSE_HEADERS)

@bp_odds_customer.route("/stream/ev")
def stream_ev(): return Response(stream_with_context(_sse_stream(config._EV_CHANNEL)), mimetype="text/event-stream", headers=config._SSE_HEADERS)