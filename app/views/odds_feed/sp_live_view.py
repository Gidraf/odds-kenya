"""
app/views/odds_feed/sp_live_view.py  (v2 — patched from user's original)
=========================================================================
Changes applied to the original file
─────────────────────────────────────
① GET /api/sp/live/stream-matches/<sport_slug>  — NEW endpoint
    SSE stream that yields one canonical SpMatch per "match" frame as it is
    fetched from SP's API, so the client can append rows immediately rather
    than waiting for the full list.

    Frame sequence:
      {type:"start",  sport, mode:"live", estimated_max}
      {type:"match",  index, match:{SpMatch}}   ← one per live event
      {type:"done",   total, latency_ms, harvested_at}
      {type:"error",  message}

    Also writes the result to the sp:live:{sport_slug} cache (TTL 60 s) on
    completion so snapshot-canonical returns fresh data on the next REFRESH.

② _cache_get / _cache_set helpers moved above the snapshot-canonical route
    that uses them (they were referenced before definition in the original).

③ _now_ts() helper added (was missing; referenced by stream-matches).

Everything else (all existing routes, SSE helpers, Sportradar proxy,
test endpoints) is UNCHANGED from the original.
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


# ── Internal helpers (lazy imports to avoid circular deps) ────────────────────

def _harvester():
    from app.workers import sp_live_harvester as h
    return h


def _get_redis():
    from app.workers.sp_live_harvester import _get_redis as gr
    return gr()


# ── Redis channel names (mirrors harvester) ───────────────────────────────────

CH_ALL   = "sp:live:all"
CH_SPORT = "sp:live:sport:{sport_id}"
CH_EVENT = "sp:live:event:{event_id}"

SSE_HEADERS = {
    "Content-Type":      "text/event-stream",
    "Cache-Control":     "no-cache",
    "X-Accel-Buffering": "no",
    "Connection":        "keep-alive",
}


# ─────────────────────────────────────────────────────────────────────────────
# SSE helpers
# ─────────────────────────────────────────────────────────────────────────────

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _sse_keep_alive() -> str:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    return f": keep-alive {ts}\n\n"


def _stream_channel(channel: str, label: str = ""):
    """
    Generator: subscribe to one Redis pub/sub channel, yield SSE frames.
    Keep-alive comment emitted every 15 s to prevent proxy timeouts.
    """
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
                yield _sse(json.loads(msg["data"]))
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


# ② ③ Cache helpers + timestamp helper (referenced by multiple routes)

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


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ═════════════════════════════════════════════════════════════════════════════
# SSE STREAM ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@bp_sp_live.route("/stream")
def stream_all():
    """Stream every live update across all sports."""
    @stream_with_context
    def generate():
        yield from _stream_channel(CH_ALL, label="all")
    return Response(generate(), headers=SSE_HEADERS)


@bp_sp_live.route("/stream/sport/<int:sport_id>")
def stream_sport(sport_id: int):
    """Stream all live updates for one sport (e.g. /stream/sport/1 = Soccer)."""
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
    Stream all updates for a single live event (odds + score/clock).
    The harvester publishes to sp:live:event:{id} on every WS market_update
    and Sportradar event_update, so each per-event EventSource in the
    frontend receives independent real-time updates.
    """
    @stream_with_context
    def generate():
        yield from _stream_channel(
            CH_EVENT.format(event_id=event_id),
            label=f"event_{event_id}",
        )
    return Response(generate(), headers=SSE_HEADERS)


# ① NEW — stream-matches endpoint
@bp_sp_live.route("/stream-matches/<sport_slug>")
def stream_live_matches(sport_slug: str):
    """
    GET /api/sp/live/stream-matches/<sport_slug>

    SSE stream that yields one live match per frame as it is fetched.
    The client appends each row to the grid immediately without waiting
    for the full list, then calls subscribeToEvent(id) to wire up the
    per-event SSE channel for real-time updates on that match.

    Frame sequence:
      {type:"start",  sport, mode:"live", estimated_max}
      {type:"match",  index, match:{SpMatch}}   ← appended immediately
      {type:"done",   total, latency_ms, harvested_at}
      {type:"error",  message}

    Writes result to sp:live:{sport_slug} cache (TTL 60 s) on completion.
    """
    @stream_with_context
    def generate():
        t0          = time.perf_counter()
        all_matches = []
        try:
            from app.workers.sp_harvester import fetch_live_stream

            yield _sse({
                "type":          "start",
                "sport":         sport_slug,
                "mode":          "live",
                "estimated_max": 80,   # rough upper bound for progress bar
            })

            idx = 0
            for match in fetch_live_stream(
                sport_slug,
                fetch_full_markets=True,
                sleep_between=0.15,
            ):
                idx += 1
                all_matches.append(match)
                yield _sse({"type": "match", "index": idx, "match": match})

            harvested_at = _now_ts()
            latency_ms   = int((time.perf_counter() - t0) * 1000)

            # Cache so snapshot-canonical returns fresh data on next REFRESH
            _cache_set(f"sp:live:{sport_slug}", {
                "source":       "sportpesa",
                "sport":        sport_slug,
                "mode":         "live",
                "match_count":  len(all_matches),
                "harvested_at": harvested_at,
                "latency_ms":   latency_ms,
                "matches":      all_matches,
            }, ttl=60)

            yield _sse({
                "type":         "done",
                "total":        len(all_matches),
                "latency_ms":   latency_ms,
                "harvested_at": harvested_at,
            })

        except Exception as exc:
            log.exception("stream_live_matches %s: %s", sport_slug, exc)
            yield _sse({"type": "error", "message": str(exc)})

    return Response(generate(), headers=SSE_HEADERS)


# ═════════════════════════════════════════════════════════════════════════════
# REST — SPORTS / EVENTS / SNAPSHOT / MARKETS
# ═════════════════════════════════════════════════════════════════════════════

@bp_sp_live.route("/sports")
def live_sports():
    """Return current live sport list (Redis cache → HTTP fallback)."""
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
    """Return current live events for one sport (live HTTP call)."""
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
    """Return the Redis-cached snapshot for a sport (events + markets)."""
    t0  = time.perf_counter()
    r   = _get_redis()
    raw = r.get(f"sp:live:snapshot:{sport_id}")

    if not raw:
        return _err(
            f"No snapshot for sport_id={sport_id}. "
            "POST /api/sp/live/test/snapshot to warm the cache.",
            404,
        )

    data = json.loads(raw)
    data["ok"]         = True
    data["latency_ms"] = int((time.perf_counter() - t0) * 1000)
    return _signed_response(data)


@bp_sp_live.route("/snapshot-canonical/<sport_slug>")
def live_snapshot_canonical(sport_slug: str):
    """
    GET /api/sp/live/snapshot-canonical/<sport_slug>

    Returns live matches in the SAME canonical format as the pre-match harvester
    (i.e. SpMatch[] with markets: {slug: {outcome: odd}}).

    Cache key: sp:live:{sport_slug}  (TTL 90 s)
    Falls back to a fresh HTTP fetch if cache is cold.
    """
    t0 = time.perf_counter()

    # 1. Try existing cache written by stream-matches or Celery harvester
    cached = _cache_get(f"sp:live:{sport_slug}")
    if cached and cached.get("matches"):
        data       = cached
        from_cache = True
    else:
        # 2. Fresh fetch via sp_harvester → sp_mapper (blocking, ~1–3 s)
        try:
            from app.workers.sp_harvester import fetch_live  # noqa
            matches      = fetch_live(sport_slug, fetch_full_markets=True)
            harvested_at = _now_ts()
            data = {
                "source":       "sportpesa",
                "sport":        sport_slug,
                "mode":         "live",
                "match_count":  len(matches),
                "harvested_at": harvested_at,
                "latency_ms":   int((time.perf_counter() - t0) * 1000),
                "matches":      matches,
            }
            _cache_set(f"sp:live:{sport_slug}", data, ttl=90)
            from_cache = False
        except Exception as exc:
            log.warning("live canonical fetch failed for %s: %s", sport_slug, exc)
            return _err(f"Live fetch failed: {exc}", 503)

    return _signed_response({
        "ok":           True,
        "source":       "sportpesa_live_canonical",
        "sport":        sport_slug,
        "from_cache":   from_cache,
        "matches":      data.get("matches", []),
        "total":        data.get("match_count", len(data.get("matches", []))),
        "harvested_at": data.get("harvested_at"),
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/markets/<int:event_id>")
def live_markets(event_id: int):
    """Return all current market odds for one event from Redis."""
    t0     = time.perf_counter()
    r      = _get_redis()
    prefix = f"sp:live:odds:{event_id}:"
    keys   = r.keys(f"{prefix}*")

    if not keys:
        return _signed_response({
            "ok":         True,
            "event_id":   event_id,
            "markets":    [],
            "count":      0,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })

    pipe   = r.pipeline()
    for k in keys:
        pipe.get(k)
    values = pipe.execute()

    markets = []
    for k, v in zip(keys, values):
        if v:
            market_id = k.split(":")[-1]
            markets.append({"market_id": int(market_id), "odds": json.loads(v)})

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

    return _signed_response({
        "ok":         True,
        "event_id":   event_id,
        "state":      json.loads(raw),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/status")
def live_status():
    """Harvester health + Redis pub/sub subscriber counts."""
    t0 = time.perf_counter()
    h  = _harvester()
    r  = _get_redis()

    channels = {}
    try:
        ps_info = r.execute_command(
            "PUBSUB", "NUMSUB",
            CH_ALL, "sp:live:sport:1", "sp:live:sport:2",
        )
        for i in range(0, len(ps_info), 2):
            channels[ps_info[i]] = ps_info[i + 1]
    except Exception:
        pass

    return _signed_response({
        "ok":              True,
        "harvester_alive": h.harvester_alive(),
        "channels":        channels,
        "redis_connected": bool(r.ping()),
        "latency_ms":      int((time.perf_counter() - t0) * 1000),
    })


# ═════════════════════════════════════════════════════════════════════════════
# SPORTRADAR PROXY  — unchanged from original
# ═════════════════════════════════════════════════════════════════════════════

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
    ("124",                       "Corners"),
    ("40",                        "Yellow Cards"),
    ("45",                        "Yellow/Red Cards"),
    ("50",                        "Red Cards"),
    ("1030",                      "Ball Safe"),
    ("1126",                      "Attacks"),
    ("1029",                      "Dangerous Attacks"),
    ("ballsafepercentage",        "Possession %"),
    ("attackpercentage",          "Attack %"),
    ("dangerousattackpercentage", "Danger Att %"),
    ("60",                        "Substitutions"),
    ("158",                       "Injuries"),
]


def _sr_token() -> str:
    return os.getenv("SPORTRADAR_TOKEN", _SR_TOKEN_FALLBACK)


def _sr_get(endpoint: str, timeout: int = 8) -> dict | None:
    url = f"{_SR_BASE}/{endpoint}?T={_sr_token()}"
    try:
        r = requests.get(
            url,
            headers={
                "Origin":     _SR_ORIGIN,
                "Referer":    _SR_ORIGIN + "/",
                "User-Agent": (
                    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
                ),
            },
            timeout=timeout,
        )
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
        return _err(
            f"Sportradar stats unavailable for externalId={external_id}. "
            "Token may have expired — set SPORTRADAR_TOKEN env var.",
            503,
        )

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
        h = val.get("home", "")
        a = val.get("away", "")
        if h == "" and a == "":
            continue
        stats_rows.append({
            "name": label,
            "home": h if h != "" else 0,
            "away": a if a != "" else 0,
        })

    return _signed_response({
        "ok":          True,
        "external_id": external_id,
        "stats": {
            "home":  teams.get("home", "Home"),
            "away":  teams.get("away", "Away"),
            "stats": stats_rows,
        },
        "raw_maxage":  inner.get("_maxage"),
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/sportradar/match-info/<int:external_id>")
def sr_match_info(external_id: int):
    t0   = time.perf_counter()
    data = _sr_get(f"match_info/{external_id}")

    if not data:
        return _err(
            f"Sportradar match_info unavailable for externalId={external_id}.",
            503,
        )

    doc   = (data.get("doc") or [{}])[0]
    inner = doc.get("data", {})
    match = inner.get("match", {})
    venue = match.get("venue",    {})
    ref   = match.get("referee",  {})
    teams = inner.get("teams",    {})
    tourn = match.get("tournament", {})

    ref_name = ref.get("name", "")
    ref_nat  = ref.get("nationality", "")
    ref_str  = f"{ref_name} ({ref_nat})".strip(" ()") if ref_name else ""

    return _signed_response({
        "ok":          True,
        "external_id": external_id,
        "match": {
            "id":         match.get("id"),
            "tournament": tourn.get("name"),
            "venue":      venue.get("name"),
            "venue_city": venue.get("cityName"),
            "referee":    ref_str,
            "home":       (teams.get("home") or {}).get("name"),
            "away":       (teams.get("away") or {}).get("name"),
        },
        "raw":        inner,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# ═════════════════════════════════════════════════════════════════════════════
# TEST / ADMIN ENDPOINTS  — unchanged from original
# ═════════════════════════════════════════════════════════════════════════════

@bp_sp_live.route("/test/snapshot", methods=["POST"])
def test_snapshot():
    t0 = time.perf_counter()
    try:
        result  = _harvester().snapshot_all_sports()
        summary = {sport_id: len(events) for sport_id, events in result.items()}
        return _signed_response({
            "ok":           True,
            "sports_done":  len(result),
            "event_counts": summary,
            "latency_ms":   int((time.perf_counter() - t0) * 1000),
        })
    except Exception as exc:
        return _err(f"snapshot failed: {exc}", 500)


@bp_sp_live.route("/test/start-harvester", methods=["POST"])
def test_start_harvester():
    """Launch the WebSocket harvester + Sportradar polling threads."""
    t0     = time.perf_counter()
    thread = _harvester().start_harvester_thread()
    return _signed_response({
        "ok":         True,
        "alive":      thread.is_alive(),
        "thread":     thread.name,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/test/stop-harvester", methods=["POST"])
def test_stop_harvester():
    t0 = time.perf_counter()
    _harvester().stop_harvester()
    return _signed_response({
        "ok":         True,
        "alive":      _harvester().harvester_alive(),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/test/publish", methods=["POST"])
def test_publish():
    t0   = time.perf_counter()
    body = request.get_json(silent=True) or {}

    channel = body.get("channel", CH_ALL)
    payload = body.get("payload", {
        "type":    "test",
        "message": "hello from test endpoint",
        "ts":      datetime.now(timezone.utc).isoformat(),
    })

    r = _get_redis()
    n = r.publish(channel, json.dumps(payload))

    return _signed_response({
        "ok":          True,
        "channel":     channel,
        "subscribers": n,
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
    })


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

    return _signed_response({
        "ok":          True,
        "sport_id":    sport_id,
        "event_ids":   event_ids,
        "market_type": mkt_type,
        "markets":     markets,
        "count":       len(markets),
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
    })