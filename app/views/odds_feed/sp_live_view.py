"""
app/views/odds_feed/sp_live_module.py
=======================================
Flask Blueprint — Sportpesa Live Odds + Events.

All SSE endpoints subscribe to Redis pub/sub channels published by
sp_live_harvester.py.  Only delta messages (changed odds) are forwarded.

REST endpoints
──────────────
GET  /api/sp/live/sports                      — current live sport list
GET  /api/sp/live/events/<sport_id>           — events for one sport
GET  /api/sp/live/snapshot/<sport_id>         — full cached snapshot
GET  /api/sp/live/markets/<event_id>          — current market odds snapshot
GET  /api/sp/live/odds-history/<eid>/<mid>    — last N odds ticks for a market
GET  /api/sp/live/state/<event_id>            — last known event state/score
GET  /api/sp/live/status                      — harvester health

SSE stream endpoints
────────────────────
GET  /api/sp/live/stream                      — all live updates
GET  /api/sp/live/stream/sport/<sport_id>     — sport-scoped stream
GET  /api/sp/live/stream/event/<event_id>     — event-scoped stream

Test trigger endpoints
──────────────────────
POST /api/sp/live/test/snapshot               — force HTTP snapshot
POST /api/sp/live/test/start-harvester        — launch WS harvester thread
POST /api/sp/live/test/stop-harvester         — stop WS harvester
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from flask import Blueprint, Response, request, stream_with_context

from app.utils.customer_jwt_helpers import _err, _signed_response

bp_sp_live = Blueprint("sp_live", __name__, url_prefix="/api/sp/live")


# ── Internal imports (lazy, to avoid circular) ────────────────────────────────

def _harvester():
    from app.workers import sp_live_harvester as h
    return h


def _get_redis():
    from app.workers.sp_live_harvester import _get_redis as gr
    return gr()


# ── Constants (mirrors sp_live_harvester) ─────────────────────────────────────

CH_ALL   = "sp:live:all"
CH_SPORT = "sp:live:sport:{sport_id}"
CH_EVENT = "sp:live:event:{event_id}"

SSE_HEADERS = {
    "Content-Type":      "text/event-stream",
    "Cache-Control":     "no-cache",
    "X-Accel-Buffering": "no",
    "Connection":        "keep-alive",
}


# ── SSE frame helpers ─────────────────────────────────────────────────────────

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _sse_keep_alive() -> str:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    return f": keep-alive {ts}\n\n"


# ── Generic SSE generator from a Redis pub/sub channel ───────────────────────

def _stream_channel(channel: str, label: str = ""):
    """
    Generator: subscribe to a Redis pub/sub channel, yield SSE frames.
    Sends a keep-alive comment every 15 s to prevent proxy timeouts.
    Only publishes if message has changed keys (odds delta already ensured
    at publisher side, but we double-check the type field here).
    """
    r        = _get_redis()
    pubsub   = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(channel)

    yield _sse({"type": "connected", "channel": channel, "label": label,
                "ts": datetime.now(timezone.utc).isoformat()})

    last_ka = time.monotonic()
    try:
        while True:
            msg = pubsub.get_message(timeout=0.5)

            if msg and msg["type"] == "message":
                yield _sse(json.loads(msg["data"]))

            # Keep-alive every 15 s
            if time.monotonic() - last_ka > 15:
                yield _sse_keep_alive()
                last_ka = time.monotonic()

    except GeneratorExit:
        pass
    finally:
        try:
            pubsub.unsubscribe(channel)
            pubsub.close()
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════════════════
# SSE STREAM ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@bp_sp_live.route("/stream")
def stream_all():
    """
    Stream every live update across all sports.
    Clients receive both `market_update` and `event_update` messages.
    """
    @stream_with_context
    def generate():
        yield from _stream_channel(CH_ALL, label="all")

    return Response(generate(), headers=SSE_HEADERS)


@bp_sp_live.route("/stream/sport/<int:sport_id>")
def stream_sport(sport_id: int):
    """
    Stream all live updates for one sport (e.g. /stream/sport/1 = Soccer).
    """
    @stream_with_context
    def generate():
        yield from _stream_channel(
            CH_SPORT.format(sport_id=sport_id),
            label=f"sport_{sport_id}",
        )

    return Response(generate(), headers=SSE_HEADERS)


@bp_sp_live.route("/stream/event/<int:event_id>")
def stream_event(event_id: int):
    """
    Stream all updates for a single live event.
    Receives market_update (odds changes) and event_update (score/clock).
    """
    @stream_with_context
    def generate():
        yield from _stream_channel(
            CH_EVENT.format(event_id=event_id),
            label=f"event_{event_id}",
        )

    return Response(generate(), headers=SSE_HEADERS)


# ═════════════════════════════════════════════════════════════════════════════
# REST — SNAPSHOT / QUERY ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@bp_sp_live.route("/sports")
def live_sports():
    """Return current live sport list (from Redis cache or HTTP fallback)."""
    t0 = time.perf_counter()
    r  = _get_redis()

    cached = r.get("sp:live:sports")
    if cached:
        sports     = json.loads(cached)
        from_cache = True
    else:
        sports     = _harvester().fetch_live_sports()
        from_cache = False

    return _signed_response({
        "ok":         True,
        "source":     "sportpesa_live",
        "from_cache": from_cache,
        "sports":     sports,
        "count":      len(sports),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/events/<int:sport_id>")
def live_events(sport_id: int):
    """Return current live events for one sport (HTTP, not cached)."""
    t0     = time.perf_counter()
    limit  = min(int(request.args.get("limit",  50) or 50), 200)
    offset = int(request.args.get("offset", 0) or 0)

    events = _harvester().fetch_live_events(sport_id, limit=limit, offset=offset)

    return _signed_response({
        "ok":         True,
        "sport_id":   sport_id,
        "events":     events,
        "count":      len(events),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/snapshot/<int:sport_id>")
def live_snapshot(sport_id: int):
    """Return the cached snapshot for a sport (events + markets)."""
    t0 = time.perf_counter()
    r  = _get_redis()

    raw = r.get(f"sp:live:snapshot:{sport_id}")
    if not raw:
        return _err(f"No snapshot for sport_id={sport_id}. "
                    "POST /api/sp/live/test/snapshot to refresh.", 404)

    data = json.loads(raw)
    data["latency_ms"] = int((time.perf_counter() - t0) * 1000)
    data["ok"] = True
    return _signed_response(data)


@bp_sp_live.route("/markets/<int:event_id>")
def live_markets(event_id: int):
    """
    Return current market odds snapshot for one event from Redis.
    Keys: sp:live:odds:{eventId}:{marketId}
    Also lists all marketIds stored for this event.
    """
    t0     = time.perf_counter()
    r      = _get_redis()
    prefix = f"sp:live:odds:{event_id}:"
    keys   = r.keys(f"{prefix}*")

    if not keys:
        return _signed_response({
            "ok":       True,
            "event_id": event_id,
            "markets":  [],
            "count":    0,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })

    markets = []
    pipe    = r.pipeline()
    for k in keys:
        pipe.get(k)
    values = pipe.execute()

    for k, v in zip(keys, values):
        if v:
            market_id = k.split(":")[-1]
            markets.append({
                "market_id": int(market_id),
                "odds":      json.loads(v),
            })

    return _signed_response({
        "ok":         True,
        "event_id":   event_id,
        "markets":    markets,
        "count":      len(markets),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/odds-history/<int:event_id>/<int:market_id>")
def odds_history(event_id: int, market_id: int):
    """Return last N odds ticks for a specific market (newest first)."""
    t0    = time.perf_counter()
    limit = min(int(request.args.get("limit", 20) or 20), 50)
    ticks = _harvester().get_odds_history(event_id, market_id, limit=limit)

    return _signed_response({
        "ok":         True,
        "event_id":   event_id,
        "market_id":  market_id,
        "ticks":      ticks,
        "count":      len(ticks),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/state/<int:event_id>")
def event_state(event_id: int):
    """Return last known event state (score, phase, clock) from Redis."""
    t0  = time.perf_counter()
    r   = _get_redis()
    raw = r.get(f"sp:live:state:{event_id}")

    if not raw:
        return _err(f"No state cached for event {event_id}", 404)

    state = json.loads(raw)
    return _signed_response({
        "ok":         True,
        "event_id":   event_id,
        "state":      state,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/status")
def live_status():
    """Harvester health + channel subscriber counts."""
    t0 = time.perf_counter()
    h  = _harvester()

    # Redis pub/sub subscriber counts
    r    = _get_redis()
    info = {}
    try:
        ps_info = r.execute_command("PUBSUB", "NUMSUB",
                                     CH_ALL, "sp:live:sport:1", "sp:live:sport:2")
        # Returns [channel, count, channel, count, …]
        for i in range(0, len(ps_info), 2):
            info[ps_info[i]] = ps_info[i + 1]
    except Exception:
        pass

    return _signed_response({
        "ok":             True,
        "harvester_alive": h.harvester_alive(),
        "channels":        info,
        "redis_connected": bool(r.ping()),
        "latency_ms":      int((time.perf_counter() - t0) * 1000),
    })


# ═════════════════════════════════════════════════════════════════════════════
# TEST / ADMIN ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@bp_sp_live.route("/test/snapshot", methods=["POST"])
def test_snapshot():
    """
    Trigger a full HTTP snapshot of all live sports/events/markets.
    Warm the Redis cache without needing the WS harvester.
    """
    t0 = time.perf_counter()
    try:
        result = _harvester().snapshot_all_sports()
        summary = {
            sport_id: len(events)
            for sport_id, events in result.items()
        }
        return _signed_response({
            "ok":          True,
            "sports_done": len(result),
            "event_counts": summary,
            "latency_ms":  int((time.perf_counter() - t0) * 1000),
        })
    except Exception as exc:
        return _err(f"snapshot failed: {exc}", 500)


@bp_sp_live.route("/test/start-harvester", methods=["POST"])
def test_start_harvester():
    """Launch the WebSocket harvester in a background thread."""
    t0     = time.perf_counter()
    thread = _harvester().start_harvester_thread()
    return _signed_response({
        "ok":      True,
        "alive":   thread.is_alive(),
        "thread":  thread.name,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/test/stop-harvester", methods=["POST"])
def test_stop_harvester():
    """Stop the WebSocket harvester."""
    t0 = time.perf_counter()
    _harvester().stop_harvester()
    return _signed_response({
        "ok":      True,
        "alive":   _harvester().harvester_alive(),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/test/publish", methods=["POST"])
def test_publish():
    """
    Manually publish a test message to a channel.
    Body: {"channel": "sp:live:all", "payload": {...}}
    """
    t0   = time.perf_counter()
    body = request.get_json(silent=True) or {}

    channel = body.get("channel", CH_ALL)
    payload = body.get("payload", {
        "type":     "test",
        "message":  "hello from test endpoint",
        "ts":       datetime.now(timezone.utc).isoformat(),
    })

    r  = _get_redis()
    n  = r.publish(channel, json.dumps(payload))

    return _signed_response({
        "ok":          True,
        "channel":     channel,
        "subscribers": n,
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/test/fetch-markets", methods=["GET"])
def test_fetch_markets():
    """
    Test the HTTP market fetch directly.
    Query params: sport_id (default 1), event_ids (comma-separated), type (default 194)
    """
    t0        = time.perf_counter()
    sport_id  = int(request.args.get("sport_id", 1))
    ids_str   = request.args.get("event_ids", "")
    mkt_type  = int(request.args.get("type", 194))

    if ids_str:
        event_ids = [int(i.strip()) for i in ids_str.split(",") if i.strip().isdigit()]
    else:
        # Fetch first batch of live events for this sport
        events    = _harvester().fetch_live_events(sport_id, limit=15)
        event_ids = [ev["id"] for ev in events]

    markets = _harvester().fetch_live_markets(event_ids, sport_id, mkt_type)

    return _signed_response({
        "ok":         True,
        "sport_id":   sport_id,
        "event_ids":  event_ids,
        "market_type": mkt_type,
        "markets":    markets,
        "count":      len(markets),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })