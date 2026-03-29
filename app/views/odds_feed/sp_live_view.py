"""
app/views/odds_feed/sp_live_view.py  (v5)
==========================================
Changes from v4
─────────────────
NEW-A  GET /api/sp/live/stream-event-live/<event_id>
  SSE endpoint that combines TWO sources:
    1. Redis pub/sub sp:live:event:{id}  — instant WS + Celery details-poll updates
    2. Direct HTTP poll every 8 s        — guaranteed freshness even if Redis is quiet
  The Celery task sp_poll_event_details publishes to Redis every 5 s.
  This endpoint subscribes and forwards every frame immediately.

NEW-B  POST /api/sp/live/trigger-details/<event_id>
  On-demand: dispatch sp_poll_event_details for one event immediately.
  Frontend calls this when user taps a row to force a fresh poll.

NEW-C  GET /api/sp/live/stream-sport-live/<int:sport_id>
  Combines sport-level Redis pub/sub with a periodic full-sport refresh.
  Replaces the old /stream/sport/{id} as the primary live feed endpoint.

All previous endpoints (v4) are unchanged.
Beat schedule addition (add in tasks_ops.py setup_periodic_tasks):
  sender.add_periodic_task(5.0, sp_poll_all_event_details.s(), name="sp-details-5s")
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from flask import Blueprint, Response, request, stream_with_context

from app.utils.customer_jwt_helpers import _err, _signed_response

log = logging.getLogger("sp_live")

bp_sp_live = Blueprint("sp_live", __name__, url_prefix="/api/sp/live")


# ── Internal helpers ──────────────────────────────────────────────────────────

def _harvester():
    from app.workers import sp_live_harvester as h
    return h


def _get_redis():
    from app.workers.sp_live_harvester import _get_redis as gr
    return gr()


# ── Redis channel names ───────────────────────────────────────────────────────

CH_ALL   = "sp:live:all"
CH_SPORT = "sp:live:sport:{sport_id}"
CH_EVENT = "sp:live:event:{event_id}"

SSE_HEADERS = {
    "Content-Type":      "text/event-stream",
    "Cache-Control":     "no-cache",
    "X-Accel-Buffering": "no",
    "Connection":        "keep-alive",
}


# ── SSE helpers ───────────────────────────────────────────────────────────────

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _sse_keep_alive() -> str:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    return f": keep-alive {ts}\n\n"


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _stream_redis_channel(channel: str, label: str = ""):
    """Subscribe to one Redis pub/sub channel and yield SSE frames."""
    r      = _get_redis()
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(channel)
    yield _sse({
        "type":    "connected",
        "channel": channel,
        "label":   label,
        "ts":      datetime.now(timezone.utc).isoformat(),
    })
    last_ka = time.monotonic()
    try:
        while True:
            msg = pubsub.get_message(timeout=0.5)
            if msg and msg["type"] == "message":
                try:
                    yield _sse(json.loads(msg["data"]))
                except Exception:
                    pass
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


# ── Cache helpers ─────────────────────────────────────────────────────────────

def _cache_get(key: str):
    try:
        from app.workers.celery_tasks import cache_get
        return cache_get(key)
    except Exception:
        return None


def _cache_set(key: str, data, ttl: int = 90):
    try:
        from app.workers.celery_tasks import cache_set
        cache_set(key, data, ttl=ttl)
    except Exception:
        pass


# =============================================================================
# SSE STREAM ENDPOINTS (existing — unchanged from v4)
# =============================================================================

@bp_sp_live.route("/stream")
def stream_all():
    """Subscribe to all live updates via Redis pub/sub."""
    @stream_with_context
    def generate():
        yield from _stream_redis_channel(CH_ALL, label="all")
    return Response(generate(), headers=SSE_HEADERS)


@bp_sp_live.route("/stream/sport/<int:sport_id>")
def stream_sport(sport_id: int):
    """Subscribe to sport-scoped updates via Redis pub/sub."""
    @stream_with_context
    def generate():
        yield from _stream_redis_channel(
            CH_SPORT.format(sport_id=sport_id), label=f"sport_{sport_id}")
    return Response(generate(), headers=SSE_HEADERS)


@bp_sp_live.route("/stream/event/<int:event_id>")
def stream_event(event_id: int):
    """Subscribe to one event's updates via Redis pub/sub."""
    @stream_with_context
    def generate():
        yield from _stream_redis_channel(
            CH_EVENT.format(event_id=event_id), label=f"event_{event_id}")
    return Response(generate(), headers=SSE_HEADERS)


# =============================================================================
# NEW-A: /stream-event-live/<event_id>
# Combined Redis pub/sub + direct HTTP poll fallback.
# This is what the frontend subscribribeToEvent() should use.
# =============================================================================

@bp_sp_live.route("/stream-event-live/<int:event_id>")
def stream_event_live(event_id: int):
    """
    GET /api/sp/live/stream-event-live/<event_id>?sport_id=1

    SSE endpoint for one event. Combines:
      1. Redis pub/sub sp:live:event:{id} — receives WS + Celery details-poll pushes
      2. Direct HTTP poll to /api/live/events/{id}/details every 8 s

    The Celery task sp_poll_event_details (runs every 5 s) publishes to Redis,
    so under normal operation the client receives updates within 5 s of any change.
    The direct HTTP poll is the fallback in case Celery is slow or Redis is quiet.

    Client receives:
      {type:"connected", event_id, ts}         ← on subscribe
      {type:"event_update", ...state fields}   ← score/phase/clock
      {type:"market_update", ...market fields} ← odds change (changed_selections only)
      {type:"snapshot_done", markets_seen}     ← after initial direct poll
    """
    sport_id = int(request.args.get("sport_id", 1) or 1)

    @stream_with_context
    def generate():
        from app.workers.tasks_live import stream_event_details_sse
        yield from stream_event_details_sse(event_id, sport_id)

    return Response(generate(), headers=SSE_HEADERS)


# =============================================================================
# NEW-B: /trigger-details/<event_id>
# On-demand Celery task dispatch for one event — frontend calls on row tap.
# =============================================================================

@bp_sp_live.route("/trigger-details/<int:event_id>", methods=["POST"])
def trigger_details(event_id: int):
    """
    POST /api/sp/live/trigger-details/<event_id>?sport_id=1

    Dispatch sp_poll_event_details Celery task immediately for one event.
    Returns within 50 ms (fire-and-forget). The task will publish to Redis
    within ~1-3 s, which the open SSE connection will pick up automatically.

    Frontend calls this when:
    - User taps a row to force refresh
    - Detail drawer opens (to get fresh market data)
    - User clicks ↺ NOW in the drawer
    """
    sport_id = int(request.args.get("sport_id", 1) or 1)
    t0       = time.perf_counter()
    try:
        from app.workers.tasks_live import sp_poll_event_details
        sp_poll_event_details.apply_async(
            args=[event_id, sport_id],
            queue="live",
            countdown=0,
        )
        return _signed_response({
            "ok":       True,
            "event_id": event_id,
            "sport_id": sport_id,
            "queued":   True,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })
    except Exception as exc:
        log.warning("trigger-details %d: %s", event_id, exc)
        # Fallback: run synchronously if Celery not available
        try:
            from app.workers.tasks_live import _fetch_and_publish_details
            result = _fetch_and_publish_details(event_id, sport_id)
            return _signed_response({
                "ok":       True,
                "event_id": event_id,
                "sport_id": sport_id,
                "queued":   False,
                "sync":     True,
                **result,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
            })
        except Exception as exc2:
            return _err(f"trigger failed: {exc2}", 500)


# =============================================================================
# NEW-C: /stream-sport-live/<sport_id>
# Enhanced sport-level feed: Redis + periodic events-list refresh.
# =============================================================================

@bp_sp_live.route("/stream-sport-live/<int:sport_id>")
def stream_sport_live(sport_id: int):
    """
    GET /api/sp/live/stream-sport-live/<sport_id>

    Enhanced version of /stream/sport/{id}. Combines:
      1. Redis pub/sub sp:live:sport:{id} — immediate WS + details-poll pushes
      2. Periodic events-list refresh every 30 s via HTTP

    Sends {type:"events_list", events:[...]} frames alongside market updates.
    The frontend can use this to keep the event roster up to date (new events
    joining, events finishing) without a separate REST call.
    """
    @stream_with_context
    def generate():
        r      = _get_redis()
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        channel = f"sp:live:sport:{sport_id}"
        pubsub.subscribe(channel)

        yield _sse({
            "type":     "connected",
            "sport_id": sport_id,
            "channel":  channel,
            "ts":       _now_ts(),
        })

        # Initial events list
        try:
            events = _harvester().fetch_live_events(sport_id, limit=200)
            yield _sse({
                "type":     "events_list",
                "sport_id": sport_id,
                "events":   events,
                "count":    len(events),
                "ts":       _now_ts(),
            })
        except Exception:
            pass

        last_ka      = time.monotonic()
        last_refresh = time.monotonic()
        REFRESH_SEC  = 30.0

        try:
            while True:
                msg = pubsub.get_message(timeout=0.5)
                if msg and msg["type"] == "message":
                    try:
                        yield _sse(json.loads(msg["data"]))
                    except Exception:
                        pass

                now = time.monotonic()
                if now - last_ka > 15:
                    yield _sse_keep_alive()
                    last_ka = now

                if now - last_refresh >= REFRESH_SEC:
                    try:
                        events = _harvester().fetch_live_events(sport_id, limit=200)
                        yield _sse({
                            "type":     "events_list",
                            "sport_id": sport_id,
                            "events":   events,
                            "count":    len(events),
                            "ts":       _now_ts(),
                        })
                    except Exception:
                        pass
                    last_refresh = now

        except GeneratorExit:
            pass
        finally:
            try:
                pubsub.unsubscribe(channel)
                pubsub.close()
            except Exception:
                pass

    return Response(generate(), headers=SSE_HEADERS)


# =============================================================================
# Existing endpoints (v4 — all unchanged)
# =============================================================================

@bp_sp_live.route("/stream-matches/<sport_slug>")
def stream_live_matches(sport_slug: str):
    """Progressive HTTP snapshot — yields one {type:"match"} SSE frame per event."""
    @stream_with_context
    def generate():
        t0          = time.perf_counter()
        all_matches = []
        try:
            from app.workers.sp_live_harvester import fetch_live_stream, _SLUG_TO_SPORT_ID

            events_preview = _harvester().fetch_live_events(
                _SLUG_TO_SPORT_ID.get(sport_slug.lower(), 1), limit=5)
            estimated = max(len(events_preview) * 5, 20) if events_preview else 80

            yield _sse({"type": "start", "sport": sport_slug,
                        "mode": "live", "estimated_max": estimated})

            idx = 0
            for match in fetch_live_stream(sport_slug, fetch_full_markets=True,
                                           sleep_between=0.15):
                idx += 1
                all_matches.append(match)
                yield _sse({"type": "match", "index": idx, "match": match})

            harvested_at = _now_ts()
            latency_ms   = int((time.perf_counter() - t0) * 1000)
            _cache_set(f"sp:live:{sport_slug}", {
                "source": "sportpesa", "sport": sport_slug, "mode": "live",
                "match_count": len(all_matches), "harvested_at": harvested_at,
                "latency_ms": latency_ms, "matches": all_matches,
            }, ttl=60)
            yield _sse({"type": "done", "total": len(all_matches),
                        "latency_ms": latency_ms, "harvested_at": harvested_at})
        except Exception as exc:
            log.exception("stream_live_matches %s: %s", sport_slug, exc)
            yield _sse({"type": "error", "message": str(exc)})

    return Response(generate(), headers=SSE_HEADERS)


@bp_sp_live.route("/match-now/<int:event_id>")
def stream_match_now(event_id: int):
    """On-demand single-match refresh — SSE with one match_snapshot frame."""
    @stream_with_context
    def generate():
        t0       = time.perf_counter()
        sport_id = int(request.args.get("sport_id", 1) or 1)
        try:
            h        = _harvester()
            events   = h.fetch_live_events(sport_id, limit=200)
            ev       = next((e for e in events if str(e.get("id")) == str(event_id)), None)
            if not ev:
                yield _sse({"type": "error", "message": f"event {event_id} not found"})
                return
            all_mkts = h.fetch_all_market_types_for_events([event_id], sport_id)
            markets  = all_mkts.get(str(event_id), {})
            match    = h._build_live_match(ev, markets)
            yield _sse({
                "type":       "match_snapshot",
                "event_id":   event_id,
                "match":      match,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
            })
        except Exception as exc:
            log.exception("stream_match_now %d: %s", event_id, exc)
            yield _sse({"type": "error", "message": str(exc)})

    return Response(generate(), headers=SSE_HEADERS)


# ── Correct SP endpoints (confirmed from browser traffic) ────────────────────

_SP_BASE = "https://www.ke.sportpesa.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Timezone":   "Africa/Nairobi",
    "Origin":           _SP_BASE,
    "Referer":          _SP_BASE + "/en/live/",
}


def _sp_get(path: str, params: dict | None = None, timeout: int = 12):
    try:
        r = requests.get(f"{_SP_BASE}{path}", headers=_HEADERS,
                         params=params, timeout=timeout, allow_redirects=True)
        if not r.ok:
            log.warning("SP HTTP %d → %s", r.status_code, path)
            return None
        return r.json()
    except Exception as exc:
        log.warning("SP HTTP error %s: %s", path, exc)
        return None


@bp_sp_live.route("/market-types/<int:sport_id>")
def live_market_types(sport_id: int):
    t0   = time.perf_counter()
    data = _sp_get("/api/live/default/markets", params={"sportId": sport_id})
    if data is None:
        return _err("Could not fetch market types from SP", 503)
    return _signed_response({
        "ok":         True,
        "sport_id":   sport_id,
        "markets":    data if isinstance(data, list) else data.get("markets", []),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/event-markets")
def live_event_markets():
    t0        = time.perf_counter()
    event_ids = request.args.get("eventIds", "")
    mkt_type  = request.args.get("type", "194")
    sport_id  = request.args.get("sportId", "1")
    if not event_ids:
        return _err("eventIds query param required", 400)
    data = _sp_get("/api/live/event/markets",
                   params={"eventId": event_ids, "type": mkt_type, "sportId": sport_id})
    if data is None:
        return _err("Could not fetch event markets from SP", 503)
    events = (data if isinstance(data, list)
              else data.get("events") or data.get("markets") or [])
    return _signed_response({
        "ok":          True,
        "sport_id":    sport_id,
        "market_type": mkt_type,
        "events":      events,
        "count":       len(events),
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
    })


# ── Direct details proxy ──────────────────────────────────────────────────────

@bp_sp_live.route("/event-details/<int:event_id>")
def event_details(event_id: int):
    try:
        """
        GET /api/sp/live/event-details/<event_id>

        Proxy to /api/live/events/{id}/details — returns the full event state +
        all markets with team-name selections (handled with positional fallback).
        """
        t0   = time.perf_counter()
        data = _sp_get(f"/api/live/events/{event_id}/details")
        if data is None:
            return _err(f"Details unavailable for event {event_id}", 503)
        return _signed_response({
            "ok":         True,
            "event_id":   event_id,
            "data":       data,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })
    except Exception as exc:
        log.warning("event_details %d: %s", event_id, exc)
        return _err(f"Error fetching details for event {event_id}: {exc}", 500)


# ── REST endpoints (unchanged from v4) ───────────────────────────────────────

@bp_sp_live.route("/sports")
def live_sports():
    t0 = time.perf_counter()
    r  = _get_redis()
    cached = r.get("sp:live:sports")
    if cached:
        sports, from_cache = json.loads(cached), True
    else:
        sports, from_cache = _harvester().fetch_live_sports(), False
    return _signed_response({
        "ok": True, "source": "sportpesa_live",
        "from_cache": from_cache, "sports": sports,
        "count": len(sports), "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/events/<int:sport_id>")
def live_events(sport_id: int):
    t0     = time.perf_counter()
    limit  = min(int(request.args.get("limit", 50) or 50), 200)
    offset = int(request.args.get("offset", 0) or 0)
    events = _harvester().fetch_live_events(sport_id, limit=limit, offset=offset)
    return _signed_response({
        "ok": True, "sport_id": sport_id, "events": events,
        "count": len(events), "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/snapshot/<int:sport_id>")
def live_snapshot(sport_id: int):
    t0  = time.perf_counter()
    r   = _get_redis()
    raw = r.get(f"sp:live:snapshot:{sport_id}")
    if not raw:
        return _err(f"No snapshot for sport_id={sport_id}.", 404)
    data = json.loads(raw)
    data["ok"]         = True
    data["latency_ms"] = int((time.perf_counter() - t0) * 1000)
    return _signed_response(data)


@bp_sp_live.route("/snapshot-canonical/<sport_slug>")
def live_snapshot_canonical(sport_slug: str):
    t0     = time.perf_counter()
    cached = _cache_get(f"sp:live:{sport_slug}")
    if cached and cached.get("matches"):
        data, from_cache = cached, True
    else:
        try:
            from app.workers.sp_live_harvester import fetch_live
            matches      = fetch_live(sport_slug, fetch_full_markets=True)
            harvested_at = _now_ts()
            data = {
                "source": "sportpesa", "sport": sport_slug, "mode": "live",
                "match_count": len(matches), "harvested_at": harvested_at,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "matches": matches,
            }
            _cache_set(f"sp:live:{sport_slug}", data, ttl=90)
            from_cache = False
        except Exception as exc:
            log.warning("live canonical fetch failed for %s: %s", sport_slug, exc)
            return _err(f"Live fetch failed: {exc}", 503)
    return _signed_response({
        "ok": True, "source": "sportpesa_live_canonical",
        "sport": sport_slug, "from_cache": from_cache,
        "matches": data.get("matches", []),
        "total": data.get("match_count", len(data.get("matches", []))),
        "harvested_at": data.get("harvested_at"),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/markets/<int:event_id>")
def live_markets(event_id: int):
    t0     = time.perf_counter()
    r      = _get_redis()
    keys   = r.keys(f"sp:live:odds:{event_id}:*")
    if not keys:
        return _signed_response({"ok": True, "event_id": event_id, "markets": [], "count": 0,
                                  "latency_ms": int((time.perf_counter() - t0) * 1000)})
    pipe   = r.pipeline()
    for k in keys:
        pipe.get(k)
    markets = [{"sel_id": k.split(":")[-1], "odds": v}
               for k, v in zip(keys, pipe.execute()) if v]
    return _signed_response({"ok": True, "event_id": event_id, "markets": markets,
                              "count": len(markets),
                              "latency_ms": int((time.perf_counter() - t0) * 1000)})


@bp_sp_live.route("/state/<int:event_id>")
def event_state(event_id: int):
    t0  = time.perf_counter()
    r   = _get_redis()
    raw = r.get(f"sp:live:state:{event_id}")
    if not raw:
        return _err(f"No state cached for event {event_id}", 404)
    return _signed_response({"ok": True, "event_id": event_id, "state": json.loads(raw),
                              "latency_ms": int((time.perf_counter() - t0) * 1000)})


@bp_sp_live.route("/match/<int:event_id>")
def live_match_detail(event_id: int):
    t0    = time.perf_counter()
    r     = _get_redis()
    limit = min(int(request.args.get("limit", 50) or 50), 200)
    state_raw    = r.get(f"sp:live:state:{event_id}")
    state        = json.loads(state_raw) if state_raw else None
    odds_keys    = r.keys(f"sp:live:odds:{event_id}:*")
    pipe         = r.pipeline()
    for k in odds_keys:
        pipe.get(k)
    current_odds = {k.split(":")[-1]: v for k, v in zip(odds_keys, pipe.execute()) if v}
    market_history = _harvester().get_market_snapshot_history(event_id, limit=limit)
    state_history  = _harvester().get_state_snapshot_history(event_id, limit=limit)
    return _signed_response({
        "ok": True, "event_id": event_id, "state": state,
        "current_odds": current_odds, "market_history": market_history,
        "state_history": state_history,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/status")
def live_status():
    t0 = time.perf_counter()
    h  = _harvester()
    r  = _get_redis()
    channels = {}
    try:
        ps_info = r.execute_command("PUBSUB", "NUMSUB",
                                    CH_ALL, "sp:live:sport:1", "sp:live:sport:2")
        for i in range(0, len(ps_info), 2):
            channels[ps_info[i]] = ps_info[i + 1]
    except Exception:
        pass
    return _signed_response({
        "ok": True, "harvester_alive": h.harvester_alive(),
        "channels": channels, "redis_connected": bool(r.ping()),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# ── History endpoints (unchanged from v4) ────────────────────────────────────

@bp_sp_live.route("/history/market/<int:event_id>")
def history_market(event_id: int):
    t0    = time.perf_counter()
    date  = request.args.get("date")
    limit = min(int(request.args.get("limit", 100) or 100), 500)
    ticks = _harvester().get_market_snapshot_history(event_id, date=date, limit=limit)
    return _signed_response({"ok": True, "event_id": event_id, "date": date,
                              "ticks": ticks, "count": len(ticks),
                              "latency_ms": int((time.perf_counter() - t0) * 1000)})


@bp_sp_live.route("/history/state/<int:event_id>")
def history_state(event_id: int):
    t0    = time.perf_counter()
    date  = request.args.get("date")
    limit = min(int(request.args.get("limit", 100) or 100), 500)
    ticks = _harvester().get_state_snapshot_history(event_id, date=date, limit=limit)
    return _signed_response({"ok": True, "event_id": event_id, "date": date,
                              "ticks": ticks, "count": len(ticks),
                              "latency_ms": int((time.perf_counter() - t0) * 1000)})


@bp_sp_live.route("/history/dates/<int:event_id>")
def history_dates(event_id: int):
    t0 = time.perf_counter()
    r  = _get_redis()
    dates: set[str] = set()
    for k in list(r.keys(f"sp:live:snap:{event_id}:*")) + \
             list(r.keys(f"sp:live:state_hist:{event_id}:*")):
        parts = k.split(":")
        if parts:
            dates.add(parts[-1])
    return _signed_response({"ok": True, "event_id": event_id,
                              "dates": sorted(dates, reverse=True), "count": len(dates),
                              "latency_ms": int((time.perf_counter() - t0) * 1000)})


# ── Compare (unchanged from v4) ───────────────────────────────────────────────

@bp_sp_live.route("/compare/<sport_slug>")
def compare_odds(sport_slug: str):
    t0          = time.perf_counter()
    market_slug = request.args.get("market")
    raw = _cache_get(f"odds:upcoming:all:{sport_slug}")
    if not raw:
        for source in ("sp", "bt", "od", "b2b"):
            raw = _cache_get(f"{source}:upcoming:{sport_slug}")
            if raw:
                break
    if not raw or not raw.get("matches"):
        return _err(f"No upcoming data for {sport_slug}. Harvest must run first.", 404)
    comparison: list[dict] = []
    for match in raw.get("matches", []):
        markets = match.get("markets") or {}
        if market_slug:
            markets = {k: v for k, v in markets.items()
                       if k == market_slug or k.startswith(market_slug)}
        if not markets:
            continue
        entry: dict = {
            "match_id":    match.get("match_id") or match.get("betradar_id", ""),
            "home_team":   match.get("home_team", ""),
            "away_team":   match.get("away_team", ""),
            "competition": match.get("competition", ""),
            "start_time":  match.get("start_time"),
            "markets":     {},
        }
        for mkt_slug, outcomes in markets.items():
            mkt_entry: dict = {"bookmakers": {}, "best": {}, "worst": {}, "margin": {}}
            for outcome, val in outcomes.items():
                if isinstance(val, dict):
                    bk_prices = {bk: float(p) for bk, p in val.items() if float(p) > 1}
                elif isinstance(val, (int, float)) and float(val) > 1:
                    bk_prices = {raw.get("source", "unknown"): float(val)}
                else:
                    continue
                if not bk_prices:
                    continue
                best_bk  = max(bk_prices, key=lambda b: bk_prices[b])
                worst_bk = min(bk_prices, key=lambda b: bk_prices[b])
                mkt_entry["bookmakers"][outcome] = bk_prices
                mkt_entry["best"][outcome]       = {"bookmaker": best_bk, "price": bk_prices[best_bk]}
                mkt_entry["worst"][outcome]      = {"bookmaker": worst_bk, "price": bk_prices[worst_bk]}
            all_bks: set[str] = set()
            for bk_dict in mkt_entry["bookmakers"].values():
                if isinstance(bk_dict, dict):
                    all_bks.update(bk_dict.keys())
            for bk in all_bks:
                bk_odds = [d.get(bk) for d in mkt_entry["bookmakers"].values()
                           if isinstance(d, dict) and d.get(bk, 0) > 1]
                if len(bk_odds) >= 2:
                    inv_sum = sum(1.0 / p for p in bk_odds)
                    mkt_entry["margin"][bk] = round((inv_sum - 1.0) * 100, 3)
            entry["markets"][mkt_slug] = mkt_entry
        if entry["markets"]:
            comparison.append(entry)
    return _signed_response({
        "ok": True, "sport": sport_slug, "market": market_slug or "all",
        "source": raw.get("source", "merged"), "bookmakers": raw.get("bookmakers", []),
        "harvested_at": raw.get("harvested_at"), "count": len(comparison),
        "matches": comparison, "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# ── Sportradar proxy (unchanged from v4) ─────────────────────────────────────

_SR_BASE           = "https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo"
_SR_ORIGIN         = "https://www.ke.sportpesa.com"
_SR_TOKEN_FALLBACK = (
    "exp=1774545287~acl=/*"
    "~data=eyJvIjoiaHR0cHM6Ly93d3cua2Uuc3BvcnRwZXNhLmNvbSIsImEiOiJmODYx"
    "N2E4OTZkMzU1MWJhNTBkNTFmMDE0OWQ0YjZkZCIsImFjdCI6Im9yaWdpbmNoZWNrIiwi"
    "b3NyYyI6Im9yaWdpbiJ9"
    "~hmac=2a52533b3d171493eba79a54b75c4c708836d6e01bbf762f4d5856b28d24ba18"
)
_SR_STAT_KEYS: list[tuple[str, str]] = [
    ("124", "Corners"), ("40", "Yellow Cards"), ("45", "Yellow/Red Cards"),
    ("50", "Red Cards"), ("1030", "Ball Safe"), ("1126", "Attacks"),
    ("1029", "Dangerous Attacks"), ("ballsafepercentage", "Possession %"),
    ("attackpercentage", "Attack %"), ("dangerousattackpercentage", "Danger Att %"),
    ("60", "Substitutions"), ("158", "Injuries"),
]


def _sr_token() -> str:
    return os.getenv("SPORTRADAR_TOKEN", _SR_TOKEN_FALLBACK)


def _sr_get(endpoint: str, timeout: int = 8) -> dict | None:
    url = f"{_SR_BASE}/{endpoint}?T={_sr_token()}"
    try:
        r = requests.get(url, headers={"Origin": _SR_ORIGIN, "Referer": _SR_ORIGIN + "/",
                                       "User-Agent": _HEADERS["User-Agent"]},
                         timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("SR %s → %s", endpoint, exc)
        return None


@bp_sp_live.route("/sportradar/match-details/<int:external_id>")
def sr_match_details(external_id: int):
    t0   = time.perf_counter()
    data = _sr_get(f"match_detailsextended/{external_id}")
    if not data:
        return _err(f"SR stats unavailable for externalId={external_id}.", 503)
    doc    = (data.get("doc") or [{}])[0]
    inner  = doc.get("data", {})
    values = inner.get("values", {})
    teams  = inner.get("teams", {})
    stats_rows = []
    for key, label in _SR_STAT_KEYS:
        entry = values.get(key)
        if not entry:
            continue
        val = entry.get("value")
        if not isinstance(val, dict):
            continue
        h, a = val.get("home", ""), val.get("away", "")
        if h == "" and a == "":
            continue
        stats_rows.append({"name": label, "home": h or 0, "away": a or 0})
    return _signed_response({
        "ok": True, "external_id": external_id,
        "stats": {"home": teams.get("home", "Home"), "away": teams.get("away", "Away"),
                  "stats": stats_rows},
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/sportradar/match-info/<int:external_id>")
def sr_match_info(external_id: int):
    t0   = time.perf_counter()
    data = _sr_get(f"match_info/{external_id}")
    if not data:
        return _err(f"SR match_info unavailable for externalId={external_id}.", 503)
    doc   = (data.get("doc") or [{}])[0]
    inner = doc.get("data", {})
    match = inner.get("match", {})
    venue = match.get("venue", {})
    ref   = match.get("referee", {})
    teams = inner.get("teams", {})
    tourn = match.get("tournament", {})
    ref_name = ref.get("name", "")
    ref_nat  = ref.get("nationality", "")
    ref_str  = f"{ref_name} ({ref_nat})".strip(" ()") if ref_name else ""
    return _signed_response({
        "ok": True, "external_id": external_id,
        "match": {
            "id": match.get("id"), "tournament": tourn.get("name"),
            "venue": venue.get("name"), "venue_city": venue.get("cityName"),
            "referee": ref_str,
            "home": (teams.get("home") or {}).get("name"),
            "away": (teams.get("away") or {}).get("name"),
        },
        "raw": inner,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# ── Admin / test endpoints (unchanged from v4) ────────────────────────────────

@bp_sp_live.route("/test/snapshot", methods=["POST"])
def test_snapshot():
    t0 = time.perf_counter()
    try:
        result  = _harvester().snapshot_all_sports()
        summary = {sid: len(evs) for sid, evs in result.items()}
        return _signed_response({"ok": True, "sports_done": len(result),
                                  "event_counts": summary,
                                  "latency_ms": int((time.perf_counter() - t0) * 1000)})
    except Exception as exc:
        return _err(f"snapshot failed: {exc}", 500)


@bp_sp_live.route("/test/start-harvester", methods=["POST"])
def test_start_harvester():
    t0     = time.perf_counter()
    thread = _harvester().start_harvester_thread()
    return _signed_response({"ok": True, "alive": thread.is_alive(), "thread": thread.name,
                              "latency_ms": int((time.perf_counter() - t0) * 1000)})


@bp_sp_live.route("/test/stop-harvester", methods=["POST"])
def test_stop_harvester():
    t0 = time.perf_counter()
    _harvester().stop_harvester()
    return _signed_response({"ok": True, "alive": _harvester().harvester_alive(),
                              "latency_ms": int((time.perf_counter() - t0) * 1000)})


@bp_sp_live.route("/test/publish", methods=["POST"])
def test_publish():
    t0   = time.perf_counter()
    body = request.get_json(silent=True) or {}
    r    = _get_redis()
    n    = r.publish(body.get("channel", CH_ALL),
                     json.dumps(body.get("payload", {"type": "test",
                                                      "ts": datetime.now(timezone.utc).isoformat()})))
    return _signed_response({"ok": True, "channel": body.get("channel", CH_ALL),
                              "subscribers": n,
                              "latency_ms": int((time.perf_counter() - t0) * 1000)})


@bp_sp_live.route("/test/fetch-markets", methods=["GET"])
def test_fetch_markets():
    t0       = time.perf_counter()
    sport_id = int(request.args.get("sport_id", 1))
    ids_str  = request.args.get("event_ids", "")
    mkt_type = int(request.args.get("type", 194))
    if ids_str:
        event_ids = [int(i.strip()) for i in ids_str.split(",") if i.strip().isdigit()]
    else:
        events    = _harvester().fetch_live_events(sport_id, limit=15)
        event_ids = [ev["id"] for ev in events]
    markets = _harvester().fetch_live_markets(event_ids, sport_id, mkt_type)
    return _signed_response({"ok": True, "sport_id": sport_id, "event_ids": event_ids,
                              "market_type": mkt_type, "markets": markets,
                              "count": len(markets),
                              "latency_ms": int((time.perf_counter() - t0) * 1000)})


@bp_sp_live.route("/test/poll-details/<int:event_id>", methods=["POST"])
def test_poll_details(event_id: int):
    """
    POST /api/sp/live/test/poll-details/<event_id>
    Synchronously poll /api/live/events/{id}/details and publish to Redis.
    Returns the diff result immediately.
    """
    t0       = time.perf_counter()
    sport_id = int(request.args.get("sport_id", 1) or 1)
    try:
        from app.workers.tasks_live import _fetch_and_publish_details
        result = _fetch_and_publish_details(event_id, sport_id)
        return _signed_response({
            "ok": True, "event_id": event_id, "sport_id": sport_id,
            **result,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })
    except Exception as exc:
        return _err(f"poll failed: {exc}", 500)