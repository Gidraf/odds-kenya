"""
app/views/odds_feed/odds_view.py
=================================
Unified upcoming odds API — all bookmakers, one stream.

Endpoints
---------
GET  /api/odds/upcoming/<sport>
     Merged view of all bookmakers for a sport (from Redis cache).
     Query params:
       bookmakers   comma-sep list (default: all enabled)
       market       filter by market slug
       comp         filter by competition name
       team         filter by team name
       date         YYYY-MM-DD
       page / per_page / sort / order
       best_only    if=1, only return rows where this bookie has the best odd

GET  /api/odds/upcoming/<sport>/<bookmaker>
     Single bookmaker upcoming (from that bookmaker's Redis cache).

GET  /api/odds/compare/<sport>/<event_id>
     Side-by-side odds for one event across all bookmakers.
     event_id = betradar_id (cross-bookmaker canonical ID)

GET  /api/odds/value-bets/<sport>
     Latest value bets detected for a sport.

SSE  /api/odds/stream/upcoming/<sport>
     ONE stream for ALL bookmakers.
     Receives:
       {type:"odds_updated",   sport, bookmakers, count, ts}  ← after each harvest
       {type:"harvest_done",   bookmaker, sport, count, ts}   ← after each bk finishes
       {type:"value_bets",     sport, count, bets[], ts}      ← when value bets found

GET  /api/odds/history/<sport>/<betradar_id>/<market_slug>
     Odds movement for a specific event+market across time and bookmakers.

GET  /api/odds/status
     Health check: last harvest time per bookmaker+sport.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, Response, request, stream_with_context

from app.utils.customer_jwt_helpers import _err, _signed_response

bp_odds = Blueprint("odds-customer", __name__, url_prefix="/api/odds")


# ── Imports ───────────────────────────────────────────────────────────────────

def _registry():
    from app.workers.harvest_registry import ENABLED_BOOKMAKERS, BOOKMAKER_BY_SLUG
    return ENABLED_BOOKMAKERS, BOOKMAKER_BY_SLUG


def _get_redis():
    import redis, os
    return redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)


# Redis channel pattern (matches harvest_tasks.py)
CH_HARVEST_DONE = "odds:harvest:done"
CH_ODDS_UPDATED = "odds:upcoming:{sport}"
CH_VALUE_BET    = "odds:value_bets:{sport}"
KEY_UPCOMING    = "odds:upcoming:{bookmaker}:{sport}"
KEY_ALL_BK      = "odds:upcoming:all:{sport}"

SSE_HEADERS = {
    "Content-Type":      "text/event-stream",
    "Cache-Control":     "no-cache",
    "X-Accel-Buffering": "no",
    "Connection":        "keep-alive",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _sse_ka() -> str:
    return f": ka {datetime.now(timezone.utc).strftime('%H:%M:%S')}\n\n"


def _cache_get(key: str) -> dict | None:
    r   = _get_redis()
    raw = r.get(key)
    return json.loads(raw) if raw else None


def _parse_bookmakers(param: str | None) -> list[str]:
    enabled, _ = _registry()
    enabled_slugs = [b["slug"] for b in enabled]
    if not param:
        return enabled_slugs
    requested = [s.strip().lower() for s in param.split(",") if s.strip()]
    return [s for s in requested if s in enabled_slugs] or enabled_slugs


def _apply_filter(matches: list[dict], args) -> list[dict]:
    comp   = (args.get("comp")   or "").strip().lower()
    team   = (args.get("team")   or "").strip().lower()
    market = (args.get("market") or "").strip().lower()
    date_s = (args.get("date")   or "").strip()

    fdt = tdt = None
    if date_s:
        try:
            day = datetime.strptime(date_s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            fdt, tdt = day, day + timedelta(days=1)
        except ValueError:
            pass

    out = []
    for m in matches:
        if comp   and comp   not in m.get("competition", "").lower():   continue
        if team   and team   not in m.get("home_team", "").lower() and team not in m.get("away_team", "").lower(): continue
        if market and not any(market in k for k in (m.get("markets") or {})): continue
        if fdt:
            st = m.get("start_time")
            if st:
                try:
                    dt = datetime.fromisoformat(str(st).replace("Z", "+00:00"))
                    if dt < fdt or dt >= tdt: continue
                except Exception: pass
        out.append(m)
    return out


def _sort_matches(matches: list[dict], sort: str, order: str) -> list[dict]:
    reverse = order == "desc"
    if sort == "competition":
        return sorted(matches, key=lambda m: m.get("competition", ""), reverse=reverse)
    # default: start_time
    def _dt(m):
        st = m.get("start_time")
        if not st: return datetime.max.replace(tzinfo=timezone.utc)
        try: return datetime.fromisoformat(str(st).replace("Z", "+00:00"))
        except Exception: return datetime.max.replace(tzinfo=timezone.utc)
    return sorted(matches, key=_dt, reverse=reverse)


def _paginate(matches: list[dict], page: int, per_page: int):
    total = len(matches)
    return matches[(page-1)*per_page : page*per_page], total


# ══════════════════════════════════════════════════════════════════════════════
# REST ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/upcoming/<sport_slug>")
def upcoming_all_bookmakers(sport_slug: str):
    """
    GET /api/odds/upcoming/soccer
    Returns merged matches with per-bookmaker odds and best-odd flags.
    Uses the pre-computed KEY_ALL_BK cache written by merge_and_broadcast().
    Falls back to building the merge on-the-fly if cache is cold.
    """
    t0 = time.perf_counter()

    page     = max(1, int(request.args.get("page", 1) or 1))
    per_page = min(int(request.args.get("per_page", 25) or 25), 100)
    sort     = request.args.get("sort", "start_time") or "start_time"
    order    = request.args.get("order", "asc") or "asc"
    bk_param = request.args.get("bookmakers")

    bk_slugs = _parse_bookmakers(bk_param)

    # Try pre-computed merged cache first
    cached = _cache_get(KEY_ALL_BK.format(sport=sport_slug))
    if cached and cached.get("matches"):
        matches = cached["matches"]
        from_cache = True
    else:
        # On-the-fly merge (cache cold)
        from app.workers.celery_tasks import merge_and_broadcast_task
        merge_and_broadcast_task.apply_async(args=[sport_slug], queue="harvest")
        cached = _cache_get(KEY_ALL_BK.format(sport=sport_slug))
        matches = (cached or {}).get("matches", [])
        from_cache = False

    # Filter by requested bookmakers (prune others from markets dict)
    if bk_param:
        for m in matches:
            m["markets"] = {
                slug: {
                    out: {bk: odd for bk, odd in bk_odds.items() if bk in bk_slugs}
                    for out, bk_odds in outcomes.items()
                }
                for slug, outcomes in (m.get("markets") or {}).items()
            }

    matches = _apply_filter(matches, request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response({
        "ok":           True,
        "sport":        sport_slug,
        "bookmakers":   bk_slugs,
        "from_cache":   from_cache,
        "total":        total,
        "page":         page,
        "per_page":     per_page,
        "pages":        max(1, (total + per_page - 1) // per_page),
        "harvested_at": (cached or {}).get("harvested_at"),
        "matches":      paged,
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
    })


@bp_odds.route("/upcoming/<sport_slug>/<bookmaker_slug>")
def upcoming_single_bookmaker(sport_slug: str, bookmaker_slug: str):
    """GET /api/odds/upcoming/soccer/sportpesa — single bookmaker view."""
    t0 = time.perf_counter()

    _, bk_map = _registry()
    if bookmaker_slug not in bk_map:
        return _err(f"Unknown bookmaker: {bookmaker_slug}", 404)

    page     = max(1, int(request.args.get("page", 1) or 1))
    per_page = min(int(request.args.get("per_page", 25) or 25), 100)
    sort     = request.args.get("sort", "start_time") or "start_time"
    order    = request.args.get("order", "asc") or "asc"

    cached = _cache_get(KEY_UPCOMING.format(bookmaker=bookmaker_slug, sport=sport_slug))

    if not cached or not cached.get("matches"):
        return _signed_response({
            "ok": True, "bookmaker": bookmaker_slug, "sport": sport_slug,
            "matches": [], "total": 0, "from_cache": False,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })

    matches = _apply_filter(cached["matches"], request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response({
        "ok":           True,
        "bookmaker":    bookmaker_slug,
        "sport":        sport_slug,
        "from_cache":   True,
        "total":        total,
        "page":         page,
        "per_page":     per_page,
        "pages":        max(1, (total + per_page - 1) // per_page),
        "harvested_at": cached.get("harvested_at"),
        "matches":      paged,
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
    })


@bp_odds.route("/compare/<sport_slug>/<betradar_id>")
def compare_event(sport_slug: str, betradar_id: str):
    """
    GET /api/odds/compare/soccer/12345678
    Side-by-side odds for one event across all bookmakers.
    Uses pre-built merged cache — O(1) lookup.
    """
    t0 = time.perf_counter()

    cached = _cache_get(KEY_ALL_BK.format(sport=sport_slug))
    if not cached:
        return _err(f"No cached data for sport: {sport_slug}", 404)

    match = next(
        (m for m in cached.get("matches", []) if str(m.get("betradar_id")) == betradar_id),
        None,
    )
    if not match:
        return _err(f"Event {betradar_id} not found in {sport_slug} cache", 404)

    return _signed_response({
        "ok":          True,
        "betradar_id": betradar_id,
        "sport":       sport_slug,
        "match":       match,
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
    })


@bp_odds.route("/value-bets/<sport_slug>")
def value_bets(sport_slug: str):
    """GET /api/odds/value-bets/soccer — latest value bets from Postgres."""
    t0      = time.perf_counter()
    min_pct = float(request.args.get("min_pct", 5))
    limit   = min(int(request.args.get("limit", 50) or 50), 200)
    kind    = request.args.get("kind", "arb")   # "arb" | "ev" | "all"

    try:
        from app.models.odds_model import (
            ArbitrageOpportunity, EVOpportunity, OpportunityStatus,
        )
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        arbs, evs = [], []
        if kind in ("arb", "all"):
            arbs = (
                ArbitrageOpportunity.query
                .filter(
                    ArbitrageOpportunity.sport      == sport_slug,
                    ArbitrageOpportunity.profit_pct >= min_pct,
                    ArbitrageOpportunity.open_at    >= cutoff,
                    ArbitrageOpportunity.status     == OpportunityStatus.OPEN,
                )
                .order_by(ArbitrageOpportunity.profit_pct.desc())
                .limit(limit).all()
            )
        if kind in ("ev", "all"):
            evs = (
                EVOpportunity.query
                .filter(
                    EVOpportunity.sport    == sport_slug,
                    EVOpportunity.ev_pct   >= min_pct,
                    EVOpportunity.open_at  >= cutoff,
                    EVOpportunity.status   == OpportunityStatus.OPEN,
                )
                .order_by(EVOpportunity.ev_pct.desc())
                .limit(limit).all()
            )

        return _signed_response({
            "ok":         True,
            "sport":      sport_slug,
            "arb_count":  len(arbs),
            "ev_count":   len(evs),
            "arbs":       [a.to_dict() for a in arbs],
            "evs":        [e.to_dict() for e in evs],
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })
    except Exception as exc:
        return _err(f"DB error: {exc}", 503)


@bp_odds.route("/history/<sport_slug>/<betradar_id>/<market_slug>")
def odds_history(sport_slug: str, betradar_id: str, market_slug: str):
    """
    GET /api/odds/history/soccer/12345678/1x2
    Time-series odds for one event+market across all bookmakers.
    Returns last 30 days of OddsSnapshot rows, grouped by bookmaker+outcome.
    """
    t0      = time.perf_counter()
    days    = min(int(request.args.get("days", 7) or 7), 30)
    outcome = request.args.get("outcome")

    try:
        from app.models.odds_model import BookmakerOddsHistory, UnifiedMatch
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        # Resolve match_id from betradar_id
        um = UnifiedMatch.query.filter_by(parent_match_id=betradar_id).first()
        if not um:
            return _err(f"Event {betradar_id} not found", 404)

        q = (
            BookmakerOddsHistory.query
            .filter(
                BookmakerOddsHistory.match_id  == um.id,
                BookmakerOddsHistory.market    == market_slug,
                BookmakerOddsHistory.recorded_at >= cutoff,
            )
            .order_by(BookmakerOddsHistory.recorded_at.asc())
        )
        if outcome:
            q = q.filter(BookmakerOddsHistory.selection == outcome)

        rows = q.all()

        # Group: {bookmaker_id: {selection: [{ts, old, new, delta}]}}
        grouped: dict[str, dict[str, list]] = {}
        for row in rows:
            bk_key = str(row.bookmaker_id)
            sel    = row.selection or ""
            grouped.setdefault(bk_key, {}).setdefault(sel, [])
            grouped[bk_key][sel].append({
                "ts":          row.recorded_at.isoformat(),
                "old_price":   row.old_price,
                "new_price":   row.new_price,
                "price_delta": row.price_delta,
            })

        return _signed_response({
            "ok":          True,
            "betradar_id": betradar_id,
            "match_id":    um.id,
            "sport":       sport_slug,
            "market":      market_slug,
            "days":        days,
            "history":     grouped,
            "total_rows":  len(rows),
            "latency_ms":  int((time.perf_counter() - t0) * 1000),
        })
    except Exception as exc:
        return _err(f"DB error: {exc}", 503)


@bp_odds.route("/status")
def odds_status():
    """GET /api/odds/status — harvest health per bookmaker+sport."""
    t0 = time.perf_counter()
    enabled, _ = _registry()
    r  = _get_redis()

    status = []
    for bk in enabled:
        bk_status = {"bookmaker": bk["slug"], "label": bk["label"], "sports": []}
        for sport in bk["sports"][:6]:   # cap to first 6 sports per bookmaker
            key    = KEY_UPCOMING.format(bookmaker=bk["slug"], sport=sport)
            cached = r.get(key)
            if cached:
                d = json.loads(cached)
                bk_status["sports"].append({
                    "sport":        sport,
                    "match_count":  d.get("match_count", 0),
                    "harvested_at": d.get("harvested_at"),
                    "ttl_s":        r.ttl(key),
                })
            else:
                bk_status["sports"].append({
                    "sport": sport, "match_count": 0,
                    "harvested_at": None, "ttl_s": -2,
                })
        status.append(bk_status)

    return _signed_response({
        "ok": True, "bookmakers": status,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# ══════════════════════════════════════════════════════════════════════════════
# SSE STREAM — ONE STREAM FOR ALL BOOKMAKERS
# ══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/stream/upcoming/<sport_slug>")
def stream_upcoming(sport_slug: str):
    """
    GET /api/odds/stream/upcoming/soccer

    SSE stream that notifies clients whenever:
      1. Any bookmaker finishes harvesting (harvest_done)
      2. The merged view is rebuilt (odds_updated)
      3. Value bets are detected (value_bets)

    Client should call GET /api/odds/upcoming/{sport} on receipt of odds_updated
    to get fresh data. The stream itself doesn't carry full match data (too large).

    Optional query param: bookmakers=sp,betika (filter events to these only)
    """
    bk_filter = set(_parse_bookmakers(request.args.get("bookmakers")))

    @stream_with_context
    def generate():
        r      = _get_redis()
        pubsub = r.pubsub(ignore_subscribe_messages=True)

        # Subscribe to: sport-specific updates + harvest done + value bets
        pubsub.subscribe(
            CH_ODDS_UPDATED.format(sport=sport_slug),
            CH_VALUE_BET.format(sport=sport_slug),
            CH_HARVEST_DONE,
        )

        yield _sse({
            "type":       "connected",
            "sport":      sport_slug,
            "bookmakers": sorted(bk_filter),
            "ts":         datetime.now(timezone.utc).isoformat(),
        })

        last_ka = time.monotonic()
        try:
            while True:
                msg = pubsub.get_message(timeout=0.5)
                if msg and msg["type"] == "message":
                    try:
                        data = json.loads(msg["data"])
                    except Exception:
                        continue

                    # Filter harvest_done to only relevant bookmakers
                    if data.get("type") == "harvest_done":
                        if data.get("bookmaker") not in bk_filter:
                            continue
                        if data.get("sport") != sport_slug:
                            continue

                    yield _sse(data)

                if time.monotonic() - last_ka > 15:
                    yield _sse_ka()
                    last_ka = time.monotonic()

        except GeneratorExit:
            pass
        finally:
            try:
                pubsub.unsubscribe()
                pubsub.close()
            except Exception:
                pass

    return Response(generate(), headers=SSE_HEADERS)


@bp_odds.route("/stream/trigger/<sport_slug>", methods=["POST"])
def trigger_harvest(sport_slug: str):
    """
    POST /api/odds/stream/trigger/soccer
    Manually trigger an immediate harvest for a sport across all bookmakers.
    Use for on-demand refresh without waiting for Celery Beat.
    """
    t0 = time.perf_counter()
    try:
        from app.workers.celery_tasks import harvest_bookmaker_sport_task
        from app.workers.harvest_registry import ENABLED_BOOKMAKERS

        bk_param = request.args.get("bookmakers")
        bk_slugs = _parse_bookmakers(bk_param)

        scheduled = []
        for bk in ENABLED_BOOKMAKERS:
            if bk["slug"] not in bk_slugs:
                continue
            if sport_slug not in bk["sports"]:
                continue
            harvest_bookmaker_sport_task.apply_async(
                args=[bk["slug"], sport_slug], queue="harvest")
            scheduled.append(bk["slug"])

        return _signed_response({
            "ok":         True,
            "sport":      sport_slug,
            "scheduled":  scheduled,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })
    except Exception as exc:
        return _err(f"Trigger failed: {exc}", 500)