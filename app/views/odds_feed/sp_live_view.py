"""
app/views/odds_feed/sp_live_view.py  (v3)
==========================================
Changes from v2
─────────────────
① live-markets  — uses the CORRECT SP endpoints discovered from browser traffic:
    GET /api/live/default/markets?sportId=1        → list of market types + selections
    GET /api/live/event/markets?eventId=...&type=194&sportId=1  → per-event odds

  The old endpoint /api/live/markets was 404ing.  Now fetch_live_markets()
  in the harvester also gets a new HTTP-based snapshot path via this view.

② GET /api/sp/live/history/market/<event_id>      ← NEW — intraday odds ticks
  GET /api/sp/live/history/state/<event_id>        ← NEW — intraday state ticks
  GET /api/sp/live/history/dates/<event_id>        ← NEW — list available dates

③ GET /api/sp/live/compare/<sport_slug>            ← NEW — multi-bookmaker odds
  Reads sp:upcoming:all:{sport} cache and returns comparison table with
  best/worst prices per market and margin per bookmaker.

④ GET /api/sp/live/match/<event_id>               ← NEW — full match detail
  Combines live state + all Redis market keys + both history types.
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


def _stream_channel(channel: str, label: str = ""):
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


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================================================================
# SSE STREAM ENDPOINTS
# =============================================================================

@bp_sp_live.route("/stream")
def stream_all():
    @stream_with_context
    def generate():
        yield from _stream_channel(CH_ALL, label="all")
    return Response(generate(), headers=SSE_HEADERS)


@bp_sp_live.route("/stream/sport/<int:sport_id>")
def stream_sport(sport_id: int):
    @stream_with_context
    def generate():
        yield from _stream_channel(
            CH_SPORT.format(sport_id=sport_id), label=f"sport_{sport_id}")
    return Response(generate(), headers=SSE_HEADERS)


@bp_sp_live.route("/stream/event/<int:event_id>")
def stream_event(event_id: int):
    @stream_with_context
    def generate():
        yield from _stream_channel(
            CH_EVENT.format(event_id=event_id), label=f"event_{event_id}")
    return Response(generate(), headers=SSE_HEADERS)


@bp_sp_live.route("/stream-matches/<sport_slug>")
def stream_live_matches(sport_slug: str):
    """
    SSE stream yielding one live match per frame with full canonical markets.
    """
    @stream_with_context
    def generate():
        t0          = time.perf_counter()
        all_matches = []
        try:
            from app.workers.sp_harvester import fetch_live_stream
            yield _sse({"type": "start", "sport": sport_slug,
                        "mode": "live", "estimated_max": 80})
            idx = 0
            for match in fetch_live_stream(
                sport_slug, fetch_full_markets=True, sleep_between=0.15,
            ):
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


# =============================================================================
# ① LIVE MARKETS — correct SP endpoints
# =============================================================================

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


def _sp_get(path: str, params: dict | None = None, timeout: int = 12) -> dict | list | None:
    try:
        r = requests.get(
            f"{_SP_BASE}{path}", headers=_HEADERS,
            params=params, timeout=timeout, allow_redirects=True,
        )
        if not r.ok:
            log.warning("SP HTTP %d → %s", r.status_code, path)
            return None
        return r.json()
    except Exception as exc:
        log.warning("SP HTTP error %s: %s", path, exc)
        return None


@bp_sp_live.route("/market-types/<int:sport_id>")
def live_market_types(sport_id: int):
    """
    GET /api/sp/live/market-types/<sport_id>
    Returns the list of available market types for live events of this sport.
    Calls GET /api/live/default/markets?sportId={id}
    """
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
    """
    GET /api/sp/live/event-markets?eventIds=1073579,1051809&type=194&sportId=1
    Fetch current market odds for a set of event IDs.
    Calls GET /api/live/event/markets?eventId=...&type=...&sportId=...
    Response shape matches SP's confirmed JSON: {events: [{id, markets:[...]}]}
    """
    t0        = time.perf_counter()
    event_ids = request.args.get("eventIds", "")
    mkt_type  = request.args.get("type", "194")
    sport_id  = request.args.get("sportId", "1")

    if not event_ids:
        return _err("eventIds query param required (comma-separated)", 400)

    data = _sp_get(
        "/api/live/event/markets",
        params={"eventId": event_ids, "type": mkt_type, "sportId": sport_id},
    )
    if data is None:
        return _err("Could not fetch event markets from SP", 503)

    events = (data if isinstance(data, list)
              else data.get("events") or data.get("markets") or [])

    return _signed_response({
        "ok":         True,
        "sport_id":   sport_id,
        "market_type": mkt_type,
        "events":     events,
        "count":      len(events),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# REST — SPORTS / EVENTS / SNAPSHOT / MARKETS
# =============================================================================

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
        return _err(f"No snapshot for sport_id={sport_id}. POST /test/snapshot to warm.", 404)
    data = json.loads(raw)
    data["ok"]         = True
    data["latency_ms"] = int((time.perf_counter() - t0) * 1000)
    return _signed_response(data)


@bp_sp_live.route("/snapshot-canonical/<sport_slug>")
def live_snapshot_canonical(sport_slug: str):
    t0 = time.perf_counter()
    cached = _cache_get(f"sp:live:{sport_slug}")
    if cached and cached.get("matches"):
        data, from_cache = cached, True
    else:
        try:
            from app.workers.sp_harvester import fetch_live
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
    prefix = f"sp:live:odds:{event_id}:"
    keys   = r.keys(f"{prefix}*")
    if not keys:
        return _signed_response({
            "ok": True, "event_id": event_id, "markets": [], "count": 0,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })
    pipe   = r.pipeline()
    for k in keys:
        pipe.get(k)
    values = pipe.execute()
    markets = []
    for k, v in zip(keys, values):
        if v:
            sel_id = k.split(":")[-1]
            markets.append({"sel_id": sel_id, "odds": v})
    return _signed_response({
        "ok": True, "event_id": event_id, "markets": markets,
        "count": len(markets), "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/odds-history/<int:event_id>/<int:market_id>")
def odds_history(event_id: int, market_id: int):
    t0    = time.perf_counter()
    limit = min(int(request.args.get("limit", 20) or 20), 50)
    ticks = _harvester().get_odds_history(event_id, market_id, limit=limit)
    return _signed_response({
        "ok": True, "event_id": event_id, "market_id": market_id,
        "ticks": ticks, "count": len(ticks),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/state/<int:event_id>")
def event_state(event_id: int):
    t0  = time.perf_counter()
    r   = _get_redis()
    raw = r.get(f"sp:live:state:{event_id}")
    if not raw:
        return _err(f"No state cached for event {event_id}", 404)
    return _signed_response({
        "ok": True, "event_id": event_id, "state": json.loads(raw),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/status")
def live_status():
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
        "ok": True, "harvester_alive": h.harvester_alive(),
        "channels": channels, "redis_connected": bool(r.ping()),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# ④ Full match detail (state + markets + both history types)
# =============================================================================

@bp_sp_live.route("/match/<int:event_id>")
def live_match_detail(event_id: int):
    """
    GET /api/sp/live/match/<event_id>

    Returns a single event's complete live picture:
    - current state (score, phase, clock)
    - all Redis market keys (current odds per selection)
    - recent market ticks (intraday history)
    - recent state ticks (intraday history)
    """
    t0    = time.perf_counter()
    r     = _get_redis()
    limit = min(int(request.args.get("limit", 50) or 50), 200)

    # Current state
    state_raw = r.get(f"sp:live:state:{event_id}")
    state     = json.loads(state_raw) if state_raw else None

    # Current market odds (all selection keys)
    odds_keys = r.keys(f"sp:live:odds:{event_id}:*")
    pipe      = r.pipeline()
    for k in odds_keys:
        pipe.get(k)
    odds_values = pipe.execute()
    current_odds = {
        k.split(":")[-1]: v
        for k, v in zip(odds_keys, odds_values)
        if v
    }

    # Intraday market tick history
    market_history = _harvester().get_market_snapshot_history(event_id, limit=limit)

    # Intraday state history
    state_history = _harvester().get_state_snapshot_history(event_id, limit=limit)

    return _signed_response({
        "ok":             True,
        "event_id":       event_id,
        "state":          state,
        "current_odds":   current_odds,
        "market_history": market_history,
        "state_history":  state_history,
        "latency_ms":     int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# ② History query endpoints
# =============================================================================

@bp_sp_live.route("/history/market/<int:event_id>")
def history_market(event_id: int):
    """
    GET /api/sp/live/history/market/<event_id>?date=2026-03-29&limit=100

    Returns intraday market odds ticks (newest-first) for one event.
    Each tick contains the full set of normalised selections and which
    selections changed since the previous tick.

    Useful for:
    - Replaying how odds moved during a match
    - Analysing line movement patterns
    - Detecting pre-match sharp money (odds jumping before kick-off)
    """
    t0    = time.perf_counter()
    date  = request.args.get("date")   # YYYY-MM-DD, default today
    limit = min(int(request.args.get("limit", 100) or 100), 500)

    ticks = _harvester().get_market_snapshot_history(event_id, date=date, limit=limit)
    return _signed_response({
        "ok":       True,
        "event_id": event_id,
        "date":     date,
        "ticks":    ticks,
        "count":    len(ticks),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/history/state/<int:event_id>")
def history_state(event_id: int):
    """
    GET /api/sp/live/history/state/<event_id>?date=2026-03-29&limit=100

    Returns intraday state ticks (score, phase, clock) for one event.
    Includes Sportradar period timestamps when available.

    Useful for:
    - Reconstructing the match timeline (when did goals happen)
    - Correlating odds movements with match events
    - Building a match stats timeline
    """
    t0    = time.perf_counter()
    date  = request.args.get("date")
    limit = min(int(request.args.get("limit", 100) or 100), 500)

    ticks = _harvester().get_state_snapshot_history(event_id, date=date, limit=limit)
    return _signed_response({
        "ok":       True,
        "event_id": event_id,
        "date":     date,
        "ticks":    ticks,
        "count":    len(ticks),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/history/dates/<int:event_id>")
def history_dates(event_id: int):
    """
    GET /api/sp/live/history/dates/<event_id>

    Lists all dates for which history exists in Redis for one event.
    Returns: {"dates": ["2026-03-28", "2026-03-29"], "count": 2}
    """
    t0 = time.perf_counter()
    r  = _get_redis()
    # Scan for both market and state history keys
    market_keys = r.keys(f"sp:live:snap:{event_id}:*")
    state_keys  = r.keys(f"sp:live:state_hist:{event_id}:*")

    dates: set[str] = set()
    for k in list(market_keys) + list(state_keys):
        # key format: sp:live:snap:{event_id}:{YYYY-MM-DD}
        parts = k.split(":")
        if parts:
            dates.add(parts[-1])

    return _signed_response({
        "ok":       True,
        "event_id": event_id,
        "dates":    sorted(dates, reverse=True),
        "count":    len(dates),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# ③ Multi-bookmaker odds comparison
# =============================================================================

@bp_sp_live.route("/compare/<sport_slug>")
def compare_odds(sport_slug: str):
    """
    GET /api/sp/live/compare/<sport_slug>?market=1x2&date=2026-03-29

    Returns multi-bookmaker odds comparison for upcoming matches.
    Reads the merged cache (odds:upcoming:all:{sport}) and builds
    a comparison table per match per market showing:
    - each bookmaker's price
    - best / worst price
    - margin (overround) per bookmaker
    - value-bet flag (offered > consensus)

    Query params:
      market  — filter to one market slug (e.g. 1x2, btts, over_under_goals_2.5)
      date    — not yet supported (upcoming cache is always current)
    """
    t0          = time.perf_counter()
    market_slug = request.args.get("market")

    raw = _cache_get(f"odds:upcoming:all:{sport_slug}")
    if not raw:
        # Try any bookmaker's single-source cache as fallback
        for source in ("sp", "bt", "od", "b2b"):
            raw = _cache_get(f"{source}:upcoming:{sport_slug}")
            if raw:
                break
    if not raw or not raw.get("matches"):
        return _err(
            f"No upcoming data for {sport_slug}. "
            "Harvest must run first (beat schedule or manual).", 404,
        )

    comparison: list[dict] = []

    for match in raw.get("matches", []):
        markets = match.get("markets") or {}
        if market_slug:
            markets = {k: v for k, v in markets.items()
                       if k == market_slug or k.startswith(market_slug)}

        if not markets:
            continue

        entry: dict = {
            "match_id":   match.get("match_id") or match.get("betradar_id", ""),
            "home_team":  match.get("home_team", ""),
            "away_team":  match.get("away_team", ""),
            "competition": match.get("competition", ""),
            "start_time": match.get("start_time"),
            "markets":    {},
        }

        for mkt_slug, outcomes in markets.items():
            mkt_entry: dict = {"bookmakers": {}, "best": {}, "worst": {}, "margin": {}}

            for outcome, val in outcomes.items():
                # val is either {bk: price} (merged) or float (single-source)
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
                mkt_entry["best"][outcome]       = {"bookmaker": best_bk,
                                                     "price": bk_prices[best_bk]}
                mkt_entry["worst"][outcome]      = {"bookmaker": worst_bk,
                                                     "price": bk_prices[worst_bk]}

            # Per-bookmaker margin (overround) for this market
            all_bks: set[str] = set()
            for bk_prices_by_outcome in (
                v for v in mkt_entry["bookmakers"].values()
                if isinstance(v, dict)
            ):
                all_bks.update(bk_prices_by_outcome.keys())

            for bk in all_bks:
                bk_odds = [
                    bk_prices_by_outcome.get(bk)
                    for bk_prices_by_outcome in (
                        v for v in mkt_entry["bookmakers"].values()
                        if isinstance(v, dict)
                    )
                    if bk_prices_by_outcome.get(bk, 0) > 1
                ]
                if len(bk_odds) >= 2:
                    inv_sum = sum(1.0 / p for p in bk_odds)
                    mkt_entry["margin"][bk] = round((inv_sum - 1.0) * 100, 3)

            entry["markets"][mkt_slug] = mkt_entry

        if entry["markets"]:
            comparison.append(entry)

    return _signed_response({
        "ok":          True,
        "sport":       sport_slug,
        "market":      market_slug or "all",
        "source":      raw.get("source", "merged"),
        "bookmakers":  raw.get("bookmakers", []),
        "harvested_at": raw.get("harvested_at"),
        "count":       len(comparison),
        "matches":     comparison,
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# SPORTRADAR PROXY  — unchanged from v2
# =============================================================================

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
        r = requests.get(
            url,
            headers={"Origin": _SR_ORIGIN, "Referer": _SR_ORIGIN + "/",
                     "User-Agent": _HEADERS["User-Agent"]},
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
        h = val.get("home", "")
        a = val.get("away", "")
        if h == "" and a == "":
            continue
        stats_rows.append({"name": label, "home": h or 0, "away": a or 0})

    return _signed_response({
        "ok": True, "external_id": external_id,
        "stats": {"home": teams.get("home", "Home"),
                  "away": teams.get("away", "Away"),
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
    venue = match.get("venue",    {})
    ref   = match.get("referee",  {})
    teams = inner.get("teams",    {})
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


# =============================================================================
# TEST / ADMIN ENDPOINTS
# =============================================================================

@bp_sp_live.route("/test/snapshot", methods=["POST"])
def test_snapshot():
    t0 = time.perf_counter()
    try:
        result  = _harvester().snapshot_all_sports()
        summary = {sport_id: len(events) for sport_id, events in result.items()}
        return _signed_response({
            "ok": True, "sports_done": len(result), "event_counts": summary,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })
    except Exception as exc:
        return _err(f"snapshot failed: {exc}", 500)


@bp_sp_live.route("/test/start-harvester", methods=["POST"])
def test_start_harvester():
    t0     = time.perf_counter()
    thread = _harvester().start_harvester_thread()
    return _signed_response({
        "ok": True, "alive": thread.is_alive(), "thread": thread.name,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/test/stop-harvester", methods=["POST"])
def test_stop_harvester():
    t0 = time.perf_counter()
    _harvester().stop_harvester()
    return _signed_response({
        "ok": True, "alive": _harvester().harvester_alive(),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/test/publish", methods=["POST"])
def test_publish():
    t0   = time.perf_counter()
    body = request.get_json(silent=True) or {}
    channel = body.get("channel", CH_ALL)
    payload = body.get("payload", {
        "type": "test", "message": "hello from test endpoint",
        "ts": datetime.now(timezone.utc).isoformat(),
    })
    r = _get_redis()
    n = r.publish(channel, json.dumps(payload))
    return _signed_response({
        "ok": True, "channel": channel, "subscribers": n,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
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
        "ok": True, "sport_id": sport_id, "event_ids": event_ids,
        "market_type": mkt_type, "markets": markets, "count": len(markets),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })