"""
app/api/live.py
================
Live and Upcoming REST + SSE endpoints.

All 10 bookmakers stream through the same Redis channels.
Basic tier: REST polling endpoints.
Pro tier: SSE unified stream.

Endpoints:
  GET /live/matches          Basic+  Paginated live match list
  GET /upcoming/matches      Basic+  Paginated upcoming match list
  GET /stream                Pro     Unified SSE (live + upcoming)
  GET /upcoming/stream       Pro     Upcoming-only SSE
"""
from __future__ import annotations

import json
import time
from flask import request, Response, stream_with_context, current_app, g

from . import bp_live, _signed_response, _err
from .decorators import tier_required

_SPORT_SLUG_TO_ID = {
    "soccer": 1, "basketball": 2, "tennis": 3, "ice-hockey": 5,
    "volleyball": 6, "handball": 7, "cricket": 9, "table-tennis": 13,
    "rugby": 17, "boxing": 9, "mma": 9, "baseball": 4,
    "darts": 14, "american-football": 16, "esoccer": 47,
}

SSE_HEADERS = {
    "Cache-Control":     "no-cache, no-transform",
    "X-Accel-Buffering": "no",
    "Connection":        "keep-alive",
    "Content-Type":      "text/event-stream; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
}

KEEPALIVE_SEC  = 15
SSE_TIMEOUT    = 3600   # max SSE session length (1 hr)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_redis():
    from app.workers.celery_tasks import _redis
    return _redis()


def _sse_event(event_type: str, data: dict | str) -> str:
    if isinstance(data, dict):
        data = json.dumps(data, default=str)
    return f"event: {event_type}\ndata: {data}\n\n"


def _sse_keepalive() -> str:
    return f": keepalive ts={int(time.time())}\n\n"


# ─────────────────────────────────────────────────────────────────────────────
# REST: Paginated live matches  (Basic + Pro)
# ─────────────────────────────────────────────────────────────────────────────

@bp_live.route("/live/matches", methods=["GET"])
@tier_required("basic")
def live_matches_list():
    """
    Paginated live matches.
    Aggregates SP, BT, OD, and B2B live snapshots.
    """
    from app.workers.redis_bus import get_matches_for_api

    sport = request.args.get("sport", "soccer")
    page  = max(1, int(request.args.get("page", 1)))
    limit = min(200, max(1, int(request.args.get("limit", 50))))

    matches = get_matches_for_api("live", sport)

    # Additional SP live snapshot
    r         = _get_redis()
    sport_id  = _SPORT_SLUG_TO_ID.get(sport, 1)
    sp_raw    = r.get(f"sp:live:snapshot:{sport_id}")
    if sp_raw:
        try:
            sp_data = json.loads(sp_raw)
            sp_matches = sp_data if isinstance(sp_data, list) else sp_data.get("events", [])
            from app.workers.redis_bus import _add_to_unified
            seen = set()
            for m in matches:
                key = (m.get("betradar_id") or
                       m.get("sp_game_id") or
                       f"{m.get('home_team','')}|{m.get('away_team','')}")
                seen.add(key)
            for m in sp_matches:
                _add_to_unified(matches, seen, m, "sp")
        except Exception:
            pass

    # Enrich with live state
    for m in matches:
        ev_id = m.get("sp_game_id") or m.get("betradar_id") or m.get("id")
        if ev_id:
            state_raw = r.get(f"sp:live:state:{ev_id}")
            if state_raw:
                try:
                    m["live_state"] = json.loads(state_raw)
                except Exception:
                    pass

    total        = len(matches)
    start_idx    = (page - 1) * limit
    paginated    = matches[start_idx: start_idx + limit]

    return _signed_response({
        "mode":    "live",
        "sport":   sport,
        "page":    page,
        "limit":   limit,
        "total":   total,
        "matches": paginated,
    })


# ─────────────────────────────────────────────────────────────────────────────
# REST: Paginated upcoming matches  (Basic + Pro)
# ─────────────────────────────────────────────────────────────────────────────

@bp_live.route("/upcoming/matches", methods=["GET"])
@tier_required("basic")
def upcoming_matches_list():
    """
    Paginated upcoming matches.
    Reads from the unified cross-BK snapshot (all 10 bookmakers merged).
    Falls back to per-BK snapshots if unified not ready.
    """
    from app.workers.redis_bus import get_matches_for_api

    sport = request.args.get("sport", "soccer")
    page  = max(1, int(request.args.get("page", 1)))
    limit = min(200, max(1, int(request.args.get("limit", 50))))

    matches = get_matches_for_api("upcoming", sport)

    total     = len(matches)
    start_idx = (page - 1) * limit
    paginated = matches[start_idx: start_idx + limit]

    return _signed_response({
        "mode":    "upcoming",
        "sport":   sport,
        "page":    page,
        "limit":   limit,
        "total":   total,
        "matches": paginated,
    })


# ─────────────────────────────────────────────────────────────────────────────
# SSE: Upcoming stream  (Pro)
# ─────────────────────────────────────────────────────────────────────────────

@bp_live.route("/upcoming/stream", methods=["GET"])
@tier_required("pro")
def upcoming_stream():
    """
    SSE stream for upcoming matches.
    - Sends cached snapshot immediately on connect (event: batch)
    - Pushes incremental updates as bookmakers re-harvest (event: update)
    - Pushes arb/EV alerts (event: arb_update)
    """
    sport = request.args.get("sport", "soccer")

    def generate():
        from app.workers.redis_bus import (
            get_matches_for_api, ch_merged, ch_updates,
        )
        r = _get_redis()

        # ── Step 1: Send cached snapshot immediately ─────────────────────────
        matches = get_matches_for_api("upcoming", sport)
        if matches:
            yield _sse_event("batch", {
                "matches": matches,
                "sport":   sport,
                "count":   len(matches),
                "source":  "cache",
                "ts":      time.time(),
            })

        yield _sse_event("connected", {
            "status":  "connected",
            "sport":   sport,
            "mode":    "upcoming",
            "count":   len(matches),
            "ts":      time.time(),
        })

        # ── Step 2: Subscribe to all update channels ─────────────────────────
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        channels = [
            ch_merged("upcoming", sport),
            f"odds:updates",
            f"arb:updates:{sport}",
            f"ev:updates:{sport}",
        ]
        # Also subscribe to per-BK channels
        for bk in ["sp", "bt", "od", "b2b"]:
            channels.append(ch_updates(bk, "upcoming", sport))

        pubsub.subscribe(*channels)

        last_ka   = time.time()
        deadline  = time.time() + SSE_TIMEOUT

        try:
            while time.time() < deadline:
                msg = pubsub.get_message(timeout=1.0)

                if msg and msg["type"] == "message":
                    channel = msg.get("channel", "")
                    raw     = msg.get("data", "")
                    try:
                        payload  = json.loads(raw)
                        ev_type  = _classify_event(payload, channel)

                        if ev_type == "snapshot_ready":
                            # Re-read full unified snapshot and push batch
                            fresh = get_matches_for_api("upcoming", sport)
                            if fresh:
                                yield _sse_event("batch", {
                                    "matches": fresh,
                                    "sport":   sport,
                                    "count":   len(fresh),
                                    "source":  "live",
                                    "bk":      payload.get("bk"),
                                    "ts":      time.time(),
                                })
                        elif ev_type in ("arb_updated", "ev_updated"):
                            yield _sse_event("arb_update", payload)
                        elif ev_type == "live_update":
                            yield _sse_event("live_update", payload)
                        else:
                            yield _sse_event("update", payload)

                    except json.JSONDecodeError:
                        pass
                    except GeneratorExit:
                        return

                if time.time() - last_ka > KEEPALIVE_SEC:
                    yield _sse_keepalive()
                    last_ka = time.time()

        finally:
            try:
                pubsub.unsubscribe()
                pubsub.close()
            except Exception:
                pass

    resp = Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
    )
    for k, v in SSE_HEADERS.items():
        resp.headers[k] = v
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# SSE: Unified live + upcoming stream  (Pro)
# ─────────────────────────────────────────────────────────────────────────────

@bp_live.route("/stream", methods=["GET"])
@tier_required("pro")
def unified_stream():
    """
    Unified SSE stream for both live and upcoming data.
    Handles: match status changes, goals, cards, arb/ev alerts, odds updates.

    Query Params:
      mode:        'live' | 'upcoming' | 'all'  (default: 'all')
      sport:       e.g. 'soccer'
      match_ids[]: specific event IDs to track
    """
    mode      = request.args.get("mode", "all")
    sport     = request.args.get("sport", "soccer")
    match_ids = request.args.getlist("match_ids[]")

    def generate():
        from app.workers.redis_bus import (
            get_matches_for_api, ch_merged, ch_updates,
        )
        r = _get_redis()

        # ── Initial snapshot ─────────────────────────────────────────────────
        if mode in ("upcoming", "all"):
            matches = get_matches_for_api("upcoming", sport)
            if matches:
                yield _sse_event("batch", {
                    "mode":    "upcoming",
                    "matches": matches,
                    "sport":   sport,
                    "count":   len(matches),
                    "ts":      time.time(),
                })

        if mode in ("live", "all"):
            live_matches = get_matches_for_api("live", sport)
            if live_matches:
                yield _sse_event("live_batch", {
                    "mode":    "live",
                    "matches": live_matches,
                    "sport":   sport,
                    "count":   len(live_matches),
                    "ts":      time.time(),
                })

        # ── Subscribe channels ───────────────────────────────────────────────
        pubsub   = r.pubsub(ignore_subscribe_messages=True)
        channels = ["odds:updates", f"arb:updates:{sport}", f"ev:updates:{sport}"]
        sport_id = _SPORT_SLUG_TO_ID.get(sport, 1)

        if mode in ("live", "all"):
            channels += [
                ch_merged("live", sport),
                f"sp:live:sport:{sport_id}",
                "sp:live:all",
                "odds:all:live:updates",
            ]

        if mode in ("upcoming", "all"):
            channels += [
                ch_merged("upcoming", sport),
            ]
            for bk in ["sp", "bt", "od", "b2b"]:
                channels.append(ch_updates(bk, "upcoming", sport))

        for mid in match_ids:
            channels += [
                f"sp:live:event:{mid}",
                f"live:match:{mid}:all",
            ]

        pubsub.subscribe(*channels)

        yield _sse_event("connected", {
            "status":   "connected",
            "mode":     mode,
            "sport":    sport,
            "channels": len(channels),
            "ts":       time.time(),
        })

        last_ka  = time.time()
        deadline = time.time() + SSE_TIMEOUT

        try:
            while time.time() < deadline:
                msg = pubsub.get_message(timeout=1.0)

                if msg and msg["type"] == "message":
                    channel = msg.get("channel", "")
                    raw     = msg.get("data", "")
                    try:
                        payload  = json.loads(raw)
                        ev_type  = _classify_event(payload, channel)

                        if ev_type == "snapshot_ready" and mode in ("upcoming", "all"):
                            fresh = get_matches_for_api("upcoming", sport)
                            if fresh:
                                yield _sse_event("batch", {
                                    "mode":    "upcoming",
                                    "matches": fresh,
                                    "sport":   sport,
                                    "count":   len(fresh),
                                    "bk":      payload.get("bk"),
                                    "ts":      time.time(),
                                })
                        elif ev_type in ("live_update", "market_update", "event_update"):
                            yield _sse_event(ev_type, payload)
                        elif ev_type in ("arb_updated", "ev_updated"):
                            yield _sse_event("arb_update", payload)
                        elif ev_type == "goal":
                            yield _sse_event("goal", payload)
                        elif ev_type == "status_change":
                            yield _sse_event("status_change", payload)
                        else:
                            yield _sse_event("update", payload)

                    except json.JSONDecodeError:
                        pass
                    except GeneratorExit:
                        return

                if time.time() - last_ka > KEEPALIVE_SEC:
                    yield _sse_keepalive()
                    last_ka = time.time()

        finally:
            try:
                pubsub.unsubscribe()
                pubsub.close()
            except Exception:
                pass

    resp = Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
    )
    for k, v in SSE_HEADERS.items():
        resp.headers[k] = v
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _classify_event(payload: dict, channel: str) -> str:
    """Determine SSE event type from payload and channel."""
    # Explicit type field
    explicit = payload.get("type") or payload.get("event") or ""
    if explicit:
        return str(explicit).lower()

    # Channel-based classification
    if "arb" in channel:
        return "arb_updated"
    if "ev:" in channel:
        return "ev_updated"
    if "live" in channel:
        return "live_update"
    if "upcoming" in channel or "snapshot_ready" in str(payload.get("event", "")):
        return "snapshot_ready"

    return "update"