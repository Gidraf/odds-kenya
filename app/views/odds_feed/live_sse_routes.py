"""
app/views/odds_feed/live_sse_routes.py
=========================================
Server-Sent Events endpoints for real-time live data.

All channels are keyed by internal match_id where possible,
with betradar_id fallback channels also subscribed automatically.

Endpoints
─────────────────────────────────────────────────────────────────────────────
  GET /api/live/stream/sports
      → Subscribes to  live:sports
        Emits sport-count updates whenever any live match changes.

  GET /api/live/stream/matches/<sport_slug>
      → Subscribes to  live:matches:{sport_slug}
        Emits full match-list refresh hints + delta market changes.

  GET /api/live/stream/match/<match_id>
      → Subscribes to  live:match:{match_id}:all  (internal id)
        + legacy        sp:live:event:{sp_event_id}  (if known)
        Emits merged market + event updates for one match.

  GET /api/live/stream/match/<match_id>/markets
      → Subscribes to  live:match:{match_id}:markets
        Pure odds changes only.

  GET /api/live/stream/match/<match_id>/events
      → Subscribes to  live:match:{match_id}:events
        Score / phase / status changes only.

  GET /api/live/stream/all
      → Subscribes to  live:all
        Raw broadcast of every live event (admin/debug use).

  GET /api/live/snapshot/sports
      → Returns current live sport counts (JSON, not SSE).

  GET /api/live/snapshot/matches/<sport_slug>
      → Returns current live matches for sport (JSON, not SSE).

  GET /api/live/snapshot/match/<match_id>
      → Returns latest snapshot for one match (JSON, not SSE).

  GET /api/live/history/<match_id>
      → Returns LiveRawSnapshot rows for analytics (JSON, last N rows).

Protocol
─────────────────────────────────────────────────────────────────────────────
  The SSE stream sends:
    event: connected       — on subscribe (includes channel info)
    event: market_update   — odds changed
    event: event_update    — score / phase / status changed
    event: match_refresh   — match list should be refreshed
    event: sport_update    — sport count changed
    event: keepalive       — sent every 20 s to prevent proxy timeouts
    event: error           — something went wrong

  All data fields are JSON. Each message has at minimum:
    { "type": "...", "match_id": N, "betradar_id": "...", "ts": "..." }
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import redis as _redis_lib
from flask import Blueprint, Response, request, stream_with_context

from app.utils.customer_jwt_helpers import _signed_response, _err

bp_live_sse = Blueprint("live-sse", __name__, url_prefix="/api/live")

_KEEPALIVE_INTERVAL = 20   # seconds
_SSE_HEADERS = {
    "Content-Type":               "text/event-stream",
    "Cache-Control":              "no-cache",
    "X-Accel-Buffering":          "no",
    "Access-Control-Allow-Origin": "*",
    "Connection":                 "keep-alive",
}


# ─── Redis helpers ────────────────────────────────────────────────────────────

def _redis_url() -> str:
    import os
    return os.getenv("REDIS_URL", "redis://localhost:6379/0")


def _new_redis() -> _redis_lib.Redis:
    return _redis_lib.from_url(_redis_url(), decode_responses=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── SSE formatting ───────────────────────────────────────────────────────────

def _sse_event(event_name: str, data: dict) -> str:
    return f"event: {event_name}\ndata: {json.dumps(data, default=str)}\n\n"


def _sse_keepalive() -> str:
    return f"event: keepalive\ndata: {json.dumps({'ts': _now_iso()})}\n\n"


def _sse_connected(channels: list[str], extra: dict | None = None) -> str:
    payload = {
        "type":     "connected",
        "channels": channels,
        "ts":       _now_iso(),
    }
    if extra:
        payload.update(extra)
    return f"event: connected\ndata: {json.dumps(payload)}\n\n"


# ─── Core SSE stream generator ────────────────────────────────────────────────

def _multi_channel_stream(channels: list[str], connect_extra: dict | None = None):
    """
    Subscribe to multiple Redis pub/sub channels and yield SSE frames.
    Automatically reconnects if Redis drops.
    """
    def _gen():
        yield _sse_connected(channels, connect_extra)
        while True:
            r = None
            ps = None
            try:
                r  = _new_redis()
                ps = r.pubsub(ignore_subscribe_messages=True)
                ps.subscribe(*channels)
                last_hb = time.monotonic()
                for msg in ps.listen():
                    # Keepalive
                    if time.monotonic() - last_hb >= _KEEPALIVE_INTERVAL:
                        yield _sse_keepalive()
                        last_hb = time.monotonic()
                    if msg.get("type") != "message":
                        continue
                    raw = msg.get("data", "")
                    try:
                        data = json.loads(raw)
                    except (TypeError, ValueError):
                        data = {"raw": raw}
                    event_name = data.get("type", "update")
                    yield _sse_event(event_name, data)
                    last_hb = time.monotonic()
            except GeneratorExit:
                return
            except Exception as exc:
                yield _sse_event("error", {"error": str(exc), "ts": _now_iso()})
                time.sleep(2)
            finally:
                try:
                    if ps:
                        ps.unsubscribe()
                except Exception:
                    pass
                try:
                    if r:
                        r.close()
                except Exception:
                    pass

    return Response(stream_with_context(_gen()), headers=_SSE_HEADERS)


# ─── Resolve internal match_id from URL param ─────────────────────────────────

def _match_id_from_param(raw: str) -> tuple[int | None, str | None]:
    """
    Accept either an internal integer match_id or a betradar_id string.
    Returns (match_id, betradar_id).
    """
    try:
        mid = int(raw)
        # Resolve betradar_id from DB
        from app.models.odds_model import UnifiedMatch
        um = UnifiedMatch.query.get(mid)
        return (mid, um.parent_match_id if um else None)
    except ValueError:
        pass
    # It's a betradar_id — resolve to internal
    from app.workers.live_cross_bk_updater import _resolve_match_id
    mid = _resolve_match_id(raw)
    return (mid, raw)


# ═════════════════════════════════════════════════════════════════════════════
# SSE ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@bp_live_sse.route("/stream/sports")
def stream_sports():
    """
    Subscribe to live sport-count updates.
    Fires whenever any match changes status or a new match goes live.
    """
    return _multi_channel_stream(
        ["live:sports"],
        connect_extra={"description": "Live sport counts"},
    )


@bp_live_sse.route("/stream/matches/<sport_slug>")
def stream_matches(sport_slug: str):
    """
    Subscribe to live match list updates for a sport.
    Fires on every odds change + status change for any match in the sport.
    """
    channels = [
        f"live:matches:{sport_slug}",
        "live:sports",
    ]
    return _multi_channel_stream(
        channels,
        connect_extra={"sport_slug": sport_slug},
    )


@bp_live_sse.route("/stream/match/<match_ref>")
def stream_match_all(match_ref: str):
    """
    Subscribe to all events (markets + scores) for one match.
    Accepts internal match_id (int) or betradar_id (string).
    """
    match_id, betradar_id = _match_id_from_param(match_ref)
    channels: list[str] = []
    if match_id:
        channels += [
            f"live:match:{match_id}:all",
            f"live:match:{match_id}:markets",
            f"live:match:{match_id}:events",
        ]
    if betradar_id:
        channels += [
            f"live:br:{betradar_id}:all",
            f"live:br:{betradar_id}:markets",
            f"live:br:{betradar_id}:events",
        ]
    if not channels:
        return _err("Match not found", 404)
    return _multi_channel_stream(
        channels,
        connect_extra={"match_id": match_id, "betradar_id": betradar_id},
    )


@bp_live_sse.route("/stream/match/<match_ref>/markets")
def stream_match_markets(match_ref: str):
    """Odds changes only for one match."""
    match_id, betradar_id = _match_id_from_param(match_ref)
    channels: list[str] = []
    if match_id:
        channels.append(f"live:match:{match_id}:markets")
    if betradar_id:
        channels.append(f"live:br:{betradar_id}:markets")
    if not channels:
        return _err("Match not found", 404)
    return _multi_channel_stream(
        channels,
        connect_extra={"match_id": match_id, "betradar_id": betradar_id, "feed": "markets"},
    )


@bp_live_sse.route("/stream/match/<match_ref>/events")
def stream_match_events(match_ref: str):
    """Score / phase / status changes only for one match."""
    match_id, betradar_id = _match_id_from_param(match_ref)
    channels: list[str] = []
    if match_id:
        channels.append(f"live:match:{match_id}:events")
    if betradar_id:
        channels.append(f"live:br:{betradar_id}:events")
    if not channels:
        return _err("Match not found", 404)
    return _multi_channel_stream(
        channels,
        connect_extra={"match_id": match_id, "betradar_id": betradar_id, "feed": "events"},
    )


@bp_live_sse.route("/stream/all")
def stream_all():
    """Global broadcast of every live event. Admin / debug use only."""
    return _multi_channel_stream(
        ["live:all"],
        connect_extra={"description": "Global live broadcast"},
    )


# ═════════════════════════════════════════════════════════════════════════════
# JSON SNAPSHOT ENDPOINTS  (not SSE — instant JSON response)
# ═════════════════════════════════════════════════════════════════════════════

@bp_live_sse.route("/snapshot/sports")
def snapshot_sports():
    """Return current live sport counts from Redis cache."""
    import os, redis as _rl
    r = _rl.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    raw = r.get("sp:live:sports")
    sports_list = json.loads(raw) if raw else []
    return _signed_response({
        "ok":     True,
        "sports": sports_list,
        "count":  len(sports_list),
        "ts":     _now_iso(),
    })


@bp_live_sse.route("/snapshot/matches/<sport_slug>")
def snapshot_matches(sport_slug: str):
    """
    Return current live matches for a sport.
    Reads from the combined live cache written by the pollers.
    """
    from app.workers.celery_tasks import cache_get
    from app.views.odds_feed.customer_odds_view import _load_db_matches, _normalise_sport_slug
    t0 = time.perf_counter()
    try:
        matches, total, pages = _load_db_matches(
            sport_slug, mode="live", page=1, per_page=100,
        )
    except Exception as exc:
        return _err(str(exc), 500)
    return _signed_response({
        "ok":        True,
        "sport":     _normalise_sport_slug(sport_slug),
        "mode":      "live",
        "total":     total,
        "matches":   matches,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
        "ts":        _now_iso(),
    })


@bp_live_sse.route("/snapshot/match/<match_ref>")
def snapshot_match(match_ref: str):
    """
    Return latest odds snapshot for one match across all bookmakers.
    Reads from both Redis (live snap) and DB (stored odds).
    """
    t0 = time.perf_counter()
    match_id, betradar_id = _match_id_from_param(match_ref)
    if not match_id and not betradar_id:
        return _err("Match not found", 404)

    import os, redis as _rl
    r = _rl.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)

    # Load Redis snapshots for each BK
    bk_slugs = ["sp", "bt", "od", "b2b", "sbo"]
    snap_key_ref = match_id or betradar_id
    redis_snaps: dict[str, dict] = {}
    for slug in bk_slugs:
        key = f"live:snap:{snap_key_ref}:{slug}"
        raw = r.get(key)
        if raw:
            try:
                redis_snaps[slug] = json.loads(raw)
            except Exception:
                pass

    # Also read from DB for completeness
    db_markets: dict[str, dict] = {}
    if match_id:
        try:
            from app.models.odds_model import BookmakerMatchOdds
            from app.models.bookmakers_model import Bookmaker
            from app.views.odds_feed.customer_odds_view import _flatten_db_markets, _bk_slug
            bmos = BookmakerMatchOdds.query.filter_by(match_id=match_id).all()
            bk_map = {b.id: b for b in Bookmaker.query.all()}
            for bmo in bmos:
                bk_obj = bk_map.get(bmo.bookmaker_id)
                if bk_obj:
                    slug = _bk_slug(bk_obj.name.lower())
                    db_markets[slug] = _flatten_db_markets(bmo.markets_json or {})
        except Exception:
            pass

    # Merge: live Redis snap overrides DB
    merged = {**db_markets, **redis_snaps}

    # Best odds across BKs
    best: dict[str, dict] = {}
    for slug, mkts in merged.items():
        for mkt, outcomes in (mkts or {}).items():
            best.setdefault(mkt, {})
            for out, val in (outcomes or {}).items():
                try:
                    fv = float(val)
                except (TypeError, ValueError):
                    continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]):
                    best[mkt][out] = {"odd": fv, "bk": slug}

    # Current event state
    state: dict = {}
    if betradar_id:
        state_raw = r.get(f"sp:live:state:{betradar_id}")
        if state_raw:
            try:
                state = json.loads(state_raw)
            except Exception:
                pass

    return _signed_response({
        "ok":            True,
        "match_id":      match_id,
        "betradar_id":   betradar_id,
        "markets_by_bk": merged,
        "best":          best,
        "market_count":  len(best),
        "market_slugs":  sorted(best.keys()),
        "bookmakers":    sorted(merged.keys()),
        "live_state":    state,
        "latency_ms":    int((time.perf_counter() - t0) * 1000),
        "ts":            _now_iso(),
    })


# ═════════════════════════════════════════════════════════════════════════════
# ANALYTICS HISTORY ENDPOINT
# ═════════════════════════════════════════════════════════════════════════════

@bp_live_sse.route("/history/<match_ref>")
def live_history(match_ref: str):
    """
    Return LiveRawSnapshot rows for a match.
    Query params:
      ?bk=sp|bt|od    filter by bookmaker (default: all)
      ?limit=50       max rows (default 50, max 200)
      ?trigger=sp_ws  filter by trigger type
    """
    t0       = time.perf_counter()
    match_id, betradar_id = _match_id_from_param(match_ref)
    bk_filter = (request.args.get("bk") or "").strip() or None
    limit     = min(200, max(1, int(request.args.get("limit", 50))))
    trigger_f = (request.args.get("trigger") or "").strip() or None

    try:
        from app.models.live_snapshot_model import LiveRawSnapshot
        q = LiveRawSnapshot.query
        if match_id:
            q = q.filter_by(match_id=match_id)
        elif betradar_id:
            q = q.filter_by(betradar_id=betradar_id)
        else:
            return _err("Match not found", 404)
        if bk_filter:
            q = q.filter_by(bk_slug=bk_filter)
        if trigger_f:
            q = q.filter_by(trigger=trigger_f)
        rows = q.order_by(LiveRawSnapshot.recorded_at.desc()).limit(limit).all()
        return _signed_response({
            "ok":        True,
            "match_id":  match_id,
            "betradar_id": betradar_id,
            "count":     len(rows),
            "history":   [r.to_dict() for r in rows],
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "ts":        _now_iso(),
        })
    except Exception as exc:
        return _err(str(exc), 500)


# ═════════════════════════════════════════════════════════════════════════════
# LIVE ANALYTICS AGGREGATION
# ═════════════════════════════════════════════════════════════════════════════

@bp_live_sse.route("/analytics/<match_ref>")
def live_analytics(match_ref: str):
    """
    Aggregate price drift and bookmaker response times from LiveRawSnapshot.
    Returns per-market, per-bookmaker price history for analysis.
    """
    t0 = time.perf_counter()
    match_id, betradar_id = _match_id_from_param(match_ref)
    limit = min(500, max(10, int(request.args.get("limit", 200))))

    try:
        from app.models.live_snapshot_model import LiveRawSnapshot
        q = LiveRawSnapshot.query
        if match_id:
            q = q.filter_by(match_id=match_id)
        elif betradar_id:
            q = q.filter_by(betradar_id=betradar_id)
        else:
            return _err("Match not found", 404)
        rows = q.order_by(LiveRawSnapshot.recorded_at.asc()).limit(limit).all()
    except Exception as exc:
        return _err(str(exc), 500)

    # Build price-drift timeline per bk per market per outcome
    timeline: dict[str, dict] = {}  # {bk: {market: {outcome: [{ts, price}]}}}
    for row in rows:
        bk   = row.bk_slug
        ts   = row.recorded_at.isoformat() if row.recorded_at else None
        mkts = row.markets_json or {}
        for mkt, outcomes in mkts.items():
            if not isinstance(outcomes, dict):
                continue
            for out, price in outcomes.items():
                try:
                    fv = float(price)
                except (TypeError, ValueError):
                    continue
                (timeline
                 .setdefault(bk, {})
                 .setdefault(mkt, {})
                 .setdefault(out, [])
                 .append({"ts": ts, "price": fv}))

    # Summary stats: first/last/min/max per bk/market/outcome
    summary: dict = {}
    for bk, mkts in timeline.items():
        for mkt, outcomes in mkts.items():
            for out, ticks in outcomes.items():
                prices = [t["price"] for t in ticks]
                key = f"{bk}.{mkt}.{out}"
                summary[key] = {
                    "bk":       bk,
                    "market":   mkt,
                    "outcome":  out,
                    "first":    prices[0],
                    "last":     prices[-1],
                    "min":      min(prices),
                    "max":      max(prices),
                    "drift":    round(prices[-1] - prices[0], 4),
                    "ticks":    len(prices),
                }

    return _signed_response({
        "ok":        True,
        "match_id":  match_id,
        "betradar_id": betradar_id,
        "timeline":  timeline,
        "summary":   summary,
        "row_count": len(rows),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
        "ts":        _now_iso(),
    })


# ═════════════════════════════════════════════════════════════════════════════
# LIVE STATUS  (admin dashboard)
# ═════════════════════════════════════════════════════════════════════════════

@bp_live_sse.route("/status")
def live_status():
    """Return harvester health and live counts."""
    try:
        from app.workers.sp_live_harvester import harvester_alive
        ws_ok = harvester_alive()
    except Exception:
        ws_ok = False

    import os, redis as _rl
    r = _rl.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)

    sports_raw = r.get("sp:live:sports")
    sports     = json.loads(sports_raw) if sports_raw else []
    total_live = sum(s.get("eventNumber", 0) for s in sports if isinstance(s, dict))

    # Count active pub/sub subscribers per channel pattern
    try:
        sub_counts = {
            "live:all":     r.pubsub_numsub("live:all")[b"live:all"]
                            if hasattr(r.pubsub_numsub("live:all"), "__getitem__") else 0,
        }
    except Exception:
        sub_counts = {}

    return _signed_response({
        "ok":            True,
        "ws_harvester":  ws_ok,
        "live_sports":   sports,
        "total_live":    total_live,
        "ts":            _now_iso(),
    })