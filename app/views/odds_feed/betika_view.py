"""
app/views/bt_module.py
=======================
Flask Blueprint for all Betika endpoints.

Mirrors sp_module.py + sp_live_module.py in a single file since
Betika uses REST polling (not WebSocket) for live data.

Endpoint map
────────────
  Upcoming (pre-match)
  ─────────────────────
    GET  /api/bt/upcoming/<sport>              ← cached (Redis, 5 min TTL)
    GET  /api/bt/direct/upcoming/<sport>       ← direct API call (no cache)
    SSE  /api/bt/stream/upcoming/<sport>       ← SSE stream (same format as SP)

  Live
  ────
    GET  /api/bt/live/snapshot/<sport>         ← Redis snapshot
    SSE  /api/bt/live/stream/<sport>           ← SSE from Redis pub/sub
    GET  /api/bt/live/sports                   ← live sport counts

  Metadata
  ────────
    GET  /api/bt/meta/sports                   ← sport list with slugs
    GET  /api/bt/meta/markets/<sport>          ← primary markets for a sport

SSE event format  (matches SP for uniform frontend consumption)
──────────────────────────────────────────────────────────────
  Upcoming stream:
    {"type":"start",  "estimated_max": N}
    {"type":"match",  "match": {...}}      ← repeated for each match
    {"type":"done",   "total": N, "latency_ms": X, "harvested_at": "..."}
    {"type":"error",  "message": "..."}

  Live stream:
    {"type":"connected",    "sport": "...", "bt_sport_id": N}
    {"type":"batch_update", "events": [...], "total": N}  ← every poll cycle
    {"type":"snapshot",     "matches": [...]}              ← on subscribe
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from flask import Blueprint, Response, current_app, request, stream_with_context

from app.workers.bt_harvester import (
    fetch_live_matches,
    fetch_live_sports,
    fetch_upcoming_matches,
    get_cached_live,
    get_cached_live_sports,
    get_cached_upcoming,
    get_full_markets,
    cache_upcoming,
    slug_to_bt_sport_id,
    bt_sport_to_slug,
    _LIVE_CHAN_KEY,
    _UPC_CHAN_KEY,
    BT_SPORT_SLUGS,
    SLUG_TO_BT_SPORT,
)
from app.workers.bt_mapper import (
    MARKET_DISPLAY_NAMES,
    SPORT_PRIMARY_MARKETS,
    get_bt_sport_primary_markets,
    get_market_display_name,
    bt_sport_to_slug as slug_from_id,
)

logger = logging.getLogger(__name__)

bp_betika = Blueprint("bt", __name__, url_prefix="/api/bt")

# TTL constants
_UPC_TTL = 300    # 5 min cache for upcoming
_LIVE_TTL = 60    # 1 min cache for live

# ── Helpers ───────────────────────────────────────────────────────────────────

def _redis():
    return current_app.extensions.get("redis") or current_app.config.get("REDIS_CLIENT")


def _sse(data: dict, event: str | None = None) -> str:
    payload = f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    if event:
        payload = f"event: {event}\n{payload}"
    return payload


def _paginate(matches: list[dict], page: int, per_page: int) -> tuple[list[dict], int, int]:
    """Return (page_matches, total, pages)."""
    total = len(matches)
    pages = max(1, (total + per_page - 1) // per_page)
    start = (page - 1) * per_page
    return matches[start:start + per_page], total, pages


def _apply_filters(
    matches: list[dict],
    comp:    str = "",
    team:    str = "",
    market:  str = "",
    date:    str = "",
) -> list[dict]:
    """Client-side filter over cached match list."""
    if comp:
        clo = comp.lower()
        matches = [m for m in matches if clo in (m.get("competition") or "").lower()]
    if team:
        tlo = team.lower()
        matches = [m for m in matches if
                   tlo in (m.get("home_team") or "").lower() or
                   tlo in (m.get("away_team") or "").lower()]
    if market:
        matches = [m for m in matches if
                   any(market in k for k in (m.get("markets") or {}).keys())]
    if date:
        matches = [m for m in matches if (m.get("start_time") or "").startswith(date)]
    return matches


def _sort_matches(matches: list[dict], sort: str, order: str) -> list[dict]:
    reverse = (order == "desc")
    if sort == "competition":
        return sorted(matches, key=lambda m: m.get("competition") or "", reverse=reverse)
    if sort == "market_count":
        return sorted(matches, key=lambda m: m.get("market_count") or 0, reverse=reverse)
    # default: start_time
    return sorted(matches, key=lambda m: m.get("start_time") or "", reverse=reverse)


def _market_metas(bt_sport_id: int) -> list[dict]:
    """Build market metadata list for a sport (same format as SP's meta/markets)."""
    primary = set(get_bt_sport_primary_markets(bt_sport_id))
    result = []
    for slug, label in MARKET_DISPLAY_NAMES.items():
        result.append({
            "slug":       slug,
            "label":      label,
            "is_primary": slug in primary,
        })
    return sorted(result, key=lambda x: (0 if x["is_primary"] else 1, x["slug"]))


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING — CACHED
# ══════════════════════════════════════════════════════════════════════════════

@bp_betika.route("/upcoming/<sport>")
def upcoming_cached(sport: str):
    """
    GET /api/bt/upcoming/<sport>
    Returns paginated upcoming matches from Redis cache.
    Falls back to direct API call if cache is cold (and caches result).
    """
    rd = _redis()
    t0 = time.time()

    page     = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 25))
    comp     = request.args.get("comp", "")
    team     = request.args.get("team", "")
    market   = request.args.get("market", "")
    date     = request.args.get("date", "")
    sort     = request.args.get("sort", "start_time")
    order    = request.args.get("order", "asc")

    matches = get_cached_upcoming(rd, sport) if rd else None
    source  = "cache"

    if not matches:
        # Cache miss → fetch directly and cache
        source  = "direct"
        matches = fetch_upcoming_matches(sport_slug=sport, max_pages=10, fetch_full=False)
        if matches and rd:
            try:
                cache_upcoming(rd, sport, matches)
            except Exception:  # noqa: BLE001
                pass

    # Filter + sort
    matches = _apply_filters(matches or [], comp=comp, team=team, market=market, date=date)
    matches = _sort_matches(matches, sort, order)

    page_matches, total, pages = _paginate(matches, page, per_page)
    latency = int((time.time() - t0) * 1000)

    return {
        "ok":           True,
        "matches":      page_matches,
        "total":        total,
        "pages":        pages,
        "page":         page,
        "per_page":     per_page,
        "sport":        sport,
        "source":       source,
        "latency_ms":   latency,
        "harvested_at": None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING — DIRECT
# ══════════════════════════════════════════════════════════════════════════════

@bp_betika.route("/direct/upcoming/<sport>")
def upcoming_direct(sport: str):
    """
    GET /api/bt/direct/upcoming/<sport>
    Always calls Betika API directly, bypassing Redis.
    """
    t0       = time.time()
    page     = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 25))
    comp     = request.args.get("comp", "")
    team     = request.args.get("team", "")
    market   = request.args.get("market", "")
    date     = request.args.get("date", "")
    sort     = request.args.get("sort", "start_time")
    order    = request.args.get("order", "asc")

    matches = fetch_upcoming_matches(sport_slug=sport, max_pages=5, fetch_full=False)
    matches = _apply_filters(matches, comp=comp, team=team, market=market, date=date)
    matches = _sort_matches(matches, sort, order)

    page_matches, total, pages = _paginate(matches, page, per_page)
    latency = int((time.time() - t0) * 1000)

    return {
        "ok":         True,
        "matches":    page_matches,
        "total":      total,
        "pages":      pages,
        "page":       page,
        "per_page":   per_page,
        "sport":      sport,
        "source":     "direct",
        "latency_ms": latency,
    }


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING — SSE STREAM  (same format as sp_module /stream/upcoming)
# ══════════════════════════════════════════════════════════════════════════════

@bp_betika.route("/stream/upcoming/<sport>")
def stream_upcoming(sport: str):
    """
    SSE GET /api/bt/stream/upcoming/<sport>
    Streams all upcoming matches one-by-one, then caches result in Redis.
    Frontend receives: start → match × N → done
    """
    def generate():
        rd  = _redis()
        t0  = time.time()
        bt_sport_id = slug_to_bt_sport_id(sport)
        all_matches: list[dict] = []

        try:
            # Emit start
            page = 1; limit = 50; total_est = 500
            yield _sse({"type": "start", "estimated_max": total_est})

            while True:
                from app.workers.bt_harvester import _get, UPCOMING_URL
                params: dict[str, Any] = {
                    "page":        page,
                    "limit":       limit,
                    "tab":         "upcoming",
                    "sub_type_id": "1,186,340",
                    "sport_id":    bt_sport_id,
                    "sort_id":     2,
                    "period_id":   9,
                    "esports":     "false",
                }
                data = _get(UPCOMING_URL, params=params, timeout=8.0)
                if not data:
                    break

                raw = data.get("data") or []
                if not raw:
                    break

                meta  = data.get("meta") or {}
                total = int(meta.get("total") or 0)
                if page == 1:
                    yield _sse({"type": "start", "estimated_max": total})

                from app.workers.bt_harvester import _normalise_match
                for r in raw:
                    norm = _normalise_match(r, source="upcoming")
                    if norm:
                        all_matches.append(norm)
                        yield _sse({"type": "match", "match": norm})

                if page * limit >= total:
                    break
                page += 1

            latency = int((time.time() - t0) * 1000)

            # Cache completed set in Redis
            if all_matches and rd:
                try:
                    cache_upcoming(rd, sport, all_matches, ttl=_UPC_TTL)
                except Exception:  # noqa: BLE001
                    pass

            yield _sse({
                "type":         "done",
                "total":        len(all_matches),
                "latency_ms":   latency,
                "harvested_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "sport":        sport,
            })

        except GeneratorExit:
            pass
        except Exception as exc:  # noqa: BLE001
            logger.error("BT stream error for %s: %s", sport, exc)
            yield _sse({"type": "error", "message": str(exc)})

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# LIVE — SNAPSHOT  (Redis cache from BetikaLivePoller)
# ══════════════════════════════════════════════════════════════════════════════

@bp_betika.route("/live/snapshot/<sport>")
def live_snapshot(sport: str):
    """
    GET /api/bt/live/snapshot/<sport>
    Returns the most recent live matches from Redis.
    Falls back to a direct API call if Redis is cold.
    """
    rd          = _redis()
    bt_sport_id = slug_to_bt_sport_id(sport)
    t0          = time.time()

    matches = get_cached_live(rd, bt_sport_id) if rd else None
    source  = "cache"

    if not matches:
        source  = "direct"
        matches = fetch_live_matches(bt_sport_id)

    latency = int((time.time() - t0) * 1000)
    return {
        "ok":         True,
        "matches":    matches or [],
        "total":      len(matches or []),
        "sport":      sport,
        "bt_sport_id": bt_sport_id,
        "source":     source,
        "latency_ms": latency,
    }


# ══════════════════════════════════════════════════════════════════════════════
# LIVE — SSE STREAM  (Redis pub/sub → browser)
# ══════════════════════════════════════════════════════════════════════════════

@bp_betika.route("/live/stream/<sport>")
def live_stream(sport: str):
    """
    SSE GET /api/bt/live/stream/<sport>
    Subscribes to Redis pub/sub channel for this sport and forwards
    market_update events to the browser in real-time.

    First event: snapshot of current live state.
    Subsequent events: batch_update dicts from BetikaLivePoller.

    Format mirrors sp_live_module SSE for uniform frontend handling.
    """
    bt_sport_id = slug_to_bt_sport_id(sport)
    channel     = _LIVE_CHAN_KEY.format(sport_id=bt_sport_id)

    def generate():
        rd = _redis()

        # Send initial snapshot
        try:
            snapshot = get_cached_live(rd, bt_sport_id) if rd else None
            if not snapshot:
                snapshot = fetch_live_matches(bt_sport_id)
            yield _sse({
                "type":        "connected",
                "sport":       sport,
                "bt_sport_id": bt_sport_id,
            })
            yield _sse({
                "type":    "snapshot",
                "matches": snapshot or [],
                "total":   len(snapshot or []),
            })
        except Exception as exc:  # noqa: BLE001
            yield _sse({"type": "error", "message": str(exc)})
            return

        # Subscribe to Redis pub/sub for real-time updates
        if not rd:
            # No Redis → fall back to polling loop
            prev_hash = ""
            while True:
                try:
                    matches = fetch_live_matches(bt_sport_id)
                    from app.workers.bt_harvester import _payload_hash
                    h = _payload_hash(matches)
                    if h != prev_hash:
                        prev_hash = h
                        yield _sse({
                            "type":    "snapshot",
                            "matches": matches,
                            "total":   len(matches),
                        })
                except GeneratorExit:
                    return
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(2.0)
        else:
            pubsub = rd.pubsub()
            pubsub.subscribe(channel)
            try:
                while True:
                    msg = pubsub.get_message(timeout=1.0)
                    if msg and msg.get("type") == "message":
                        try:
                            data = json.loads(msg["data"])
                            yield _sse(data)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    else:
                        yield ": heartbeat\n\n"   # keep-alive
            except GeneratorExit:
                pass
            finally:
                try:
                    pubsub.unsubscribe(channel)
                    pubsub.close()
                except Exception:  # noqa: BLE001
                    pass

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# LIVE — SPORTS
# ══════════════════════════════════════════════════════════════════════════════

@bp_betika.route("/live/sports")
def live_sports():
    """GET /api/bt/live/sports — current live sport counts."""
    rd = _redis()
    sports = get_cached_live_sports(rd) if rd else None
    if not sports:
        sports = fetch_live_sports()
    # Enrich with canonical slug
    enriched = []
    for s in (sports or []):
        sid = int(s.get("sport_id") or 0)
        enriched.append({
            **s,
            "sport_slug":   bt_sport_to_slug(sid),
            "event_count":  s.get("count") or 0,
        })
    return {"ok": True, "sports": enriched, "total": len(enriched)}


# ══════════════════════════════════════════════════════════════════════════════
# MATCH FULL MARKETS  (on-demand)
# ══════════════════════════════════════════════════════════════════════════════

@bp_betika.route("/match/markets")
def match_markets():
    """
    GET /api/bt/match/markets?parent_match_id=...&sport=soccer
    Fetch full market list for one match (all sub_type_ids).
    """
    parent_id   = request.args.get("parent_match_id", "")
    sport       = request.args.get("sport", "soccer")
    bt_sport_id = slug_to_bt_sport_id(sport)
    t0          = time.time()

    if not parent_id:
        return {"ok": False, "error": "parent_match_id required"}, 400

    markets = get_full_markets(parent_id, bt_sport_id)
    latency = int((time.time() - t0) * 1000)

    return {
        "ok":             True,
        "parent_match_id": parent_id,
        "sport":          sport,
        "markets":        markets,
        "market_count":   len(markets),
        "latency_ms":     latency,
    }


# ══════════════════════════════════════════════════════════════════════════════
# TRIGGER HARVEST  (mirrors sp /stream/trigger)
# ══════════════════════════════════════════════════════════════════════════════

@bp_betika.route("/stream/trigger/<sport>", methods=["POST"])
def trigger_harvest(sport: str):
    """
    POST /api/bt/stream/trigger/<sport>
    Force-refetch upcoming matches, busting the cache first so fresh data
    includes all market sub_type_ids (1,10,18,29,186,340).
    """
    rd = _redis()
    # Bust stale cache so fresh fetch replaces it immediately
    if rd:
        try:
            from app.workers.bt_harvester import _UPC_DATA_KEY, _UPC_HASH_KEY
            rd.delete(_UPC_DATA_KEY.format(sport_slug=sport))
            rd.delete(_UPC_HASH_KEY.format(sport_slug=sport))
        except Exception:
            pass
    # Synchronous fetch with all markets
    try:
        from app.workers.bt_harvester import fetch_upcoming_matches, cache_upcoming
        matches = fetch_upcoming_matches(sport_slug=sport, max_pages=5, fetch_full=False)
        if matches and rd:
            cache_upcoming(rd, sport, matches)
        try:
            from celery import current_app as celery_app
            celery_app.send_task("tasks.bt.harvest_all_upcoming")
        except Exception:
            pass
        return {"ok": True, "count": len(matches), "sport": sport,
                "markets_sample": list((matches[0].get("markets") or {}).keys())[:8] if matches else []}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)}, 500


@bp_betika.route("/cache/bust/<sport>", methods=["POST"])
def cache_bust(sport: str):
    """POST /api/bt/cache/bust/<sport> — delete cached data so next GET re-fetches."""
    rd = _redis()
    if not rd:
        return {"ok": False, "error": "Redis unavailable"}, 503
    try:
        from app.workers.bt_harvester import _UPC_DATA_KEY, _UPC_HASH_KEY
        deleted = sum(rd.delete(k) for k in [
            _UPC_DATA_KEY.format(sport_slug=sport),
            _UPC_HASH_KEY.format(sport_slug=sport),
        ])
        return {"ok": True, "sport": sport, "keys_deleted": deleted}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}, 500


# ══════════════════════════════════════════════════════════════════════════════
# METADATA
# ══════════════════════════════════════════════════════════════════════════════

@bp_betika.route("/meta/sports")
def meta_sports():
    """GET /api/bt/meta/sports — sport list for the sport pill row."""
    SPORT_NAMES = {
        "soccer":       ("Football",     "⚽"),
        "basketball":   ("Basketball",   "🏀"),
        "tennis":       ("Tennis",       "🎾"),
        "ice-hockey":   ("Ice Hockey",   "🏒"),
        "rugby":        ("Rugby",        "🏉"),
        "handball":     ("Handball",     "🤾"),
        "volleyball":   ("Volleyball",   "🏐"),
        "cricket":      ("Cricket",      "🏏"),
        "table-tennis": ("Table Tennis", "🏓"),
        "esoccer":      ("eFootball",    "🎮"),
        "mma":          ("MMA",          "🥋"),
        "boxing":       ("Boxing",       "🥊"),
        "darts":        ("Darts",        "🎯"),
    }
    sports = []
    seen   = set()
    for bt_sid, slug in BT_SPORT_SLUGS.items():
        if slug in seen:
            continue
        seen.add(slug)
        name, emoji = SPORT_NAMES.get(slug, (slug.replace("-"," ").title(), "🏆"))
        sports.append({
            "primary_slug":   slug,
            "name":           name,
            "emoji":          emoji,
            "bt_sport_id":    bt_sid,
        })
    return {"ok": True, "sports": sports}


@bp_betika.route("/meta/markets/<sport>")
def meta_markets(sport: str):
    """GET /api/bt/meta/markets/<sport> — market list for the market filter drawer."""
    bt_sport_id = slug_to_bt_sport_id(sport)
    markets     = _market_metas(bt_sport_id)
    return {"ok": True, "markets": markets, "sport": sport}


@bp_betika.route("/status")
def status():
    """Health check."""
    rd = _redis()
    redis_ok = False
    live_count = 0
    if rd:
        try:
            rd.ping()
            redis_ok = True
            sports = get_cached_live_sports(rd) or []
            live_count = sum(s.get("count") or 0 for s in sports)
        except Exception:  # noqa: BLE001
            pass

    from app.workers.bt_harvester import get_live_poller
    poller = get_live_poller()

    return {
        "ok":          True,
        "source":      "betika",
        "redis":       redis_ok,
        "poller_alive": poller is not None and (poller._thread and poller._thread.is_alive()),
        "live_events": live_count,
    }