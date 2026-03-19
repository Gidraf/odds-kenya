"""
app/views/odds_feed/odds_routes.py
====================================
Comprehensive Odds API — all filters, live/upcoming/results, SSE stream.

Endpoints
─────────
  GET  /odds/sports                        list available sports
  GET  /odds/bookmakers                    list bookmakers + harvest status
  GET  /odds/markets                       list known market keys

  GET  /odds/upcoming/<sport_slug>         upcoming matches (filtered)
  GET  /odds/live/<sport_slug>             live matches
  GET  /odds/results                       finished matches (date-ranged)
  GET  /odds/results/<date_str>            finished for a specific date
  GET  /odds/match/<parent_match_id>       full market detail for one match
  GET  /odds/search                        search by team / competition

  GET  /stream/odds                        SSE real-time odds updates
  GET  /stream/arb                         SSE arbitrage alerts
  GET  /stream/ev                          SSE EV alerts

Query params (upcoming / live)
──────────────────────────────
  bookmaker   str     filter by bookmaker name (case-insensitive)
  market      str     only matches that have this market key
  team        str     search home_team or away_team (partial, case-insensitive)
  competition str     partial match on competition name
  date        str     YYYY-MM-DD  (upcoming only)
  from_dt     str     ISO-8601 start datetime
  to_dt       str     ISO-8601 end datetime
  sort        str     start_time | market_count | arb   (default: start_time)
  page        int     1-based
  per_page    int     max 100

Tier policy (unchanged from existing)
──────────────────────────────────────
  free   → 100 matches, today only, no EV/arb legs
  basic  → all today, no EV/arb legs
  pro    → 30 days ahead, basic arb/ev
  premium→ full access
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

from flask import Blueprint, Response, g, request, stream_with_context

from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
from app.utils.decorators_ import log_event, require_auth, require_tier
from app.utils.fetcher_utils import TIER_LIMITS, _is_upcoming

bp_odds = Blueprint("odds", __name__, url_prefix="/api/")

FREE_MATCH_LIMIT = 100

# ── SSE channels (must match celery_tasks.py) ─────────────────────────────────
_WS_CHANNEL  = "odds:updates"
_ARB_CHANNEL = "arb:updates"
_EV_CHANNEL  = "ev:updates"


# =============================================================================
# Helpers
# =============================================================================

def _redis_client():
    import redis
    from app.workers.celery_tasks import _redis
    return _redis()


def _filter_by_tier(matches: list[dict], user) -> tuple[list[dict], bool]:
    tier   = user.tier if user else "free"
    limits = user.limits if user else {"max_matches": FREE_MATCH_LIMIT, "days_ahead": 0}

    days_ahead = limits.get("days_ahead", 0)
    if days_ahead == 0:
        cutoff = datetime.now(timezone.utc) + timedelta(days=1)
        matches = [
            m for m in matches
            if not m.get("start_time") or
            _parse_dt(m["start_time"]) <= cutoff
        ]
    elif days_ahead > 0:
        cutoff = datetime.now(timezone.utc) + timedelta(days=days_ahead)
        matches = [
            m for m in matches
            if not m.get("start_time") or
            _parse_dt(m["start_time"]) <= cutoff
        ]

    max_m = limits.get("max_matches") or FREE_MATCH_LIMIT
    if max_m and len(matches) > max_m:
        return matches[:max_m], True
    return matches, False


def _parse_dt(val: str | None) -> datetime:
    if not val:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


def _apply_filters(matches: list[dict], args) -> list[dict]:
    """Apply all query-param filters to a match list."""
    bookmaker   = (args.get("bookmaker",   "") or "").strip().lower()
    market      = (args.get("market",      "") or "").strip().lower()
    team        = (args.get("team",        "") or "").strip().lower()
    competition = (args.get("competition", "") or "").strip().lower()
    from_dt     = args.get("from_dt")
    to_dt       = args.get("to_dt")
    date_str    = args.get("date")

    if date_str:
        try:
            d         = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            from_dt   = from_dt or d.isoformat()
            to_dt     = to_dt   or (d + timedelta(days=1)).isoformat()
        except ValueError:
            pass

    from_dt_obj = _parse_dt(from_dt) if from_dt else None
    to_dt_obj   = _parse_dt(to_dt)   if to_dt   else None

    result: list[dict] = []
    for m in matches:
        # bookmaker filter — match must have this bookmaker's odds
        if bookmaker:
            bks = m.get("bookmakers") or {}
            if not any(bookmaker in (k or "").lower() for k in bks):
                continue

        # market filter
        if market:
            mkts = m.get("markets") or m.get("best_odds") or m.get("unified_markets") or {}
            if not any(market in (k or "").lower() for k in mkts):
                continue

        # team filter
        if team:
            home = (m.get("home_team") or "").lower()
            away = (m.get("away_team") or "").lower()
            if team not in home and team not in away:
                continue

        # competition filter
        if competition:
            comp = (m.get("competition") or "").lower()
            if competition not in comp:
                continue

        # datetime range filter
        if from_dt_obj or to_dt_obj:
            st = _parse_dt(m.get("start_time"))
            if from_dt_obj and st < from_dt_obj:
                continue
            if to_dt_obj   and st > to_dt_obj:
                continue

        result.append(m)

    return result


def _sort_matches(matches: list[dict], sort: str) -> list[dict]:
    if sort == "market_count":
        return sorted(matches, key=lambda m: m.get("market_count", len(m.get("markets", {}))), reverse=True)
    if sort == "arb":
        return sorted(matches, key=lambda m: bool(m.get("arbitrage")), reverse=True)
    # default: start_time asc, None last
    return sorted(matches, key=lambda m: _parse_dt(m.get("start_time")))


def _strip_sensitive(matches: list[dict], tier: str) -> list[dict]:
    """
    Remove arbitrage legs / EV details for free/basic users.
    Tier policy: arb legs only for pro+.
    """
    if tier in ("pro", "premium"):
        return matches
    for m in matches:
        m.pop("arbitrage", None)
        m.pop("ev_opportunities", None)
    return matches


def _paginate(items: list, page: int, per_page: int) -> tuple[list, int]:
    total = len(items)
    start = (page - 1) * per_page
    return items[start: start + per_page], total


# =============================================================================
# /odds/sports
# =============================================================================

@bp_odds.route("/odds/sports")
def list_sports():
    """List available sports from cache keys."""
    from app.workers.celery_tasks import cache_keys
    b2b = cache_keys("odds:upcoming:*:*:p1")
    sbo = cache_keys("sbo:upcoming:*")
    sports: set[str] = set()
    for k in b2b:
        parts = k.split(":")
        if len(parts) >= 3:
            sports.add(parts[2].replace("_", " ").title())
    for k in sbo:
        parts = k.split(":")
        if len(parts) >= 3:
            sports.add(parts[2].replace("-", " ").title())
    return _signed_response({"ok": True, "sports": sorted(sports)})


# =============================================================================
# /odds/bookmakers
# =============================================================================

@bp_odds.route("/odds/bookmakers")
def list_bookmakers():
    """List bookmakers with their last-harvest timestamp and match count."""
    from app.workers.celery_tasks import cache_keys, cache_get
    from app.models.bookmakers_model import Bookmaker

    bms    = Bookmaker.query.filter_by(is_active=True).all()
    result = []
    for bm in bms:
        keys       = cache_keys(f"odds:*:*:{bm.id}:p1")
        last_ts    = None
        total_matches = 0
        for k in keys:
            data = cache_get(k)
            if data:
                total_matches += data.get("match_count", 0)
                ts = data.get("harvested_at")
                if ts and (not last_ts or ts > last_ts):
                    last_ts = ts
        result.append({
            "id":           bm.id,
            "name":         bm.name,
            "domain":       bm.domain,
            "is_active":    bm.is_active,
            "last_harvest": last_ts,
            "match_count":  total_matches,
        })

    return _signed_response({"ok": True, "bookmakers": result})


# =============================================================================
# /odds/markets
# =============================================================================

@bp_odds.route("/odds/markets")
def list_markets():
    """List known market keys from DB."""
    try:
        from app.models.odds_model import MarketDefinition
        mkts = MarketDefinition.query.order_by(MarketDefinition.name).all()
        return _signed_response({"ok": True, "markets": [m.to_dict() for m in mkts]})
    except Exception as exc:
        return _err(str(exc), 500)


# =============================================================================
# /odds/upcoming/<sport_slug>
# =============================================================================

@bp_odds.route("/odds/upcoming/<sport_slug>")
def get_upcoming(sport_slug: str):
    """
    Upcoming matches for a sport — merged from all bookmakers.

    Filters: bookmaker, market, team, competition, date, from_dt, to_dt
    Sort:    start_time (default) | market_count | arb
    """
    user = _current_user_from_header()
    tier = user.tier if user else "free"

    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)
    sort     = request.args.get("sort", "start_time")

    log_event("odds_upcoming", {"sport": sport_slug, "tier": tier})

    from app.workers.celery_tasks import cache_get, cache_keys
    from app.views.odds_feed.bookmaker_fetcher import merge_bookmaker_results

    # ── Gather all cached data ────────────────────────────────────────────────
    matches: list[dict] = []

    sbo_data = cache_get(f"sbo:upcoming:{sport_slug.lower()}")
    if sbo_data:
        matches.extend(sbo_data.get("matches", []))

    b2b_data: list[list[dict]] = []
    for k in cache_keys(f"odds:upcoming:{sport_slug.lower().replace(' ','_')}:*"):
        d = cache_get(k)
        if d and d.get("matches"):
            b2b_data.append(d["matches"])
    if b2b_data:
        matches.extend(merge_bookmaker_results(b2b_data))

    # ── Deduplicate ───────────────────────────────────────────────────────────
    seen: set[str] = set()
    unique: list[dict] = []
    for m in matches:
        k = f"{(m.get('home_team') or '').lower()}|{(m.get('away_team') or '').lower()}"
        if k not in seen:
            seen.add(k)
            unique.append(m)
    matches = unique

    # ── Only upcoming ─────────────────────────────────────────────────────────
    now     = datetime.now(timezone.utc)
    matches = [m for m in matches if _is_upcoming(m, now)]

    # ── Filters ───────────────────────────────────────────────────────────────
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort)

    # ── Tier gate ─────────────────────────────────────────────────────────────
    matches, truncated = _filter_by_tier(matches, user)
    matches = _strip_sensitive(matches, tier)

    paged, total = _paginate(matches, page, per_page)

    resp = {
        "ok":        True,
        "sport":     sport_slug,
        "mode":      "upcoming",
        "tier":      tier,
        "total":     total,
        "page":      page,
        "per_page":  per_page,
        "pages":     max(1, (total + per_page - 1) // per_page),
        "truncated": truncated,
        "matches":   paged,
    }
    if truncated:
        resp["upgrade_message"] = "Upgrade your plan to see all matches."

    return _signed_response(resp, encrypt_for=user)


# =============================================================================
# /odds/live/<sport_slug>
# =============================================================================

@bp_odds.route("/odds/live/<sport_slug>")
def get_live(sport_slug: str):
    """Live matches — all tiers see live, but free is capped."""
    user = _current_user_from_header()
    tier = user.tier if user else "free"

    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)
    sort     = request.args.get("sort", "start_time")

    log_event("odds_live", {"sport": sport_slug, "tier": tier})

    from app.workers.celery_tasks import cache_get, cache_keys
    from app.views.odds_feed.bookmaker_fetcher import merge_bookmaker_results

    b2b_data: list[list[dict]] = []
    for k in cache_keys(f"odds:live:{sport_slug.lower().replace(' ','_')}:*"):
        d = cache_get(k)
        if d and d.get("matches"):
            b2b_data.append(d["matches"])

    matches = merge_bookmaker_results(b2b_data) if b2b_data else []
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort)
    matches, truncated = _filter_by_tier(matches, user)
    matches = _strip_sensitive(matches, tier)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response({
        "ok":        True,
        "sport":     sport_slug,
        "mode":      "live",
        "tier":      tier,
        "total":     total,
        "page":      page,
        "per_page":  per_page,
        "pages":     max(1, (total + per_page - 1) // per_page),
        "truncated": truncated,
        "matches":   paged,
    }, encrypt_for=user)


# =============================================================================
# /odds/results  &  /odds/results/<date_str>
# =============================================================================

@bp_odds.route("/odds/results")
@require_tier("basic", "pro", "premium")
def get_results():
    """Finished matches — defaults to today. Supports date, from_dt, to_dt."""
    date_str = request.args.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    return _get_finished_by_date(date_str)


@bp_odds.route("/odds/results/<date_str>")
@require_tier("basic", "pro", "premium")
def get_results_by_date(date_str: str):
    return _get_finished_by_date(date_str)


def _get_finished_by_date(date_str: str) -> Response:
    from app.workers.celery_tasks import cache_get
    from app.models.odds_model import UnifiedMatch

    log_event("finished_games_view", {"date": date_str})

    # ── Redis cache first ─────────────────────────────────────────────────────
    cached = cache_get(f"results:finished:{date_str}")
    if cached:
        matches = _apply_filters(cached, request.args)
        matches = _sort_matches(matches, request.args.get("sort", "start_time"))
        paged, total = _paginate(
            matches,
            max(1, int(request.args.get("page", 1))),
            min(int(request.args.get("per_page", 20)), 100),
        )
        return _signed_response({
            "ok": True, "date": date_str, "source": "cache",
            "total": total, "matches": paged,
        })

    # ── DB fallback ───────────────────────────────────────────────────────────
    try:
        day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        day_end   = day_start + timedelta(days=1)
    except ValueError:
        return _err("Invalid date format. Use YYYY-MM-DD.", 400)

    qs = UnifiedMatch.query.filter(
        UnifiedMatch.status     == "FINISHED",
        UnifiedMatch.start_time >= day_start,
        UnifiedMatch.start_time <  day_end,
    )

    sport = request.args.get("sport")
    if sport:
        qs = qs.filter(UnifiedMatch.sport_name.ilike(f"%{sport}%"))
    competition = request.args.get("competition")
    if competition:
        qs = qs.filter(UnifiedMatch.competition_name.ilike(f"%{competition}%"))
    team = request.args.get("team")
    if team:
        qs = qs.filter(
            (UnifiedMatch.home_team_name.ilike(f"%{team}%")) |
            (UnifiedMatch.away_team_name.ilike(f"%{team}%"))
        )

    matches_db = qs.order_by(UnifiedMatch.start_time).all()
    data       = [m.to_dict() for m in matches_db]
    paged, total = _paginate(
        data,
        max(1, int(request.args.get("page", 1))),
        min(int(request.args.get("per_page", 20)), 100),
    )
    return _signed_response({
        "ok": True, "date": date_str, "source": "db",
        "total": total, "matches": paged,
    })


# =============================================================================
# /odds/match/<parent_match_id>
# =============================================================================

@bp_odds.route("/odds/match/<parent_match_id>")
def get_match(parent_match_id: str):
    """
    Full market detail for one match.
    Includes per-bookmaker odds and best-price summary.
    Pro+ also gets arb legs and EV opportunities.
    """
    user = _current_user_from_header()
    tier = user.tier if user else "free"

    from app.models.odds_model import UnifiedMatch, ArbitrageOpportunity, EVOpportunity

    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um:
        return _err("Match not found", 404)

    log_event("match_view", {"match_id": parent_match_id, "tier": tier})

    data = um.to_dict(include_bookmaker_odds=True)

    if tier in ("pro", "premium"):
        arbs = ArbitrageOpportunity.query.filter_by(match_id=um.id, is_active=True).all()
        evs  = EVOpportunity.query.filter_by(match_id=um.id, is_active=True).all()
        data["arbitrage_opportunities"] = [
            {"id": a.id, "profit_pct": a.max_profit_percentage,
             "market": a.market_definition.name if a.market_definition else None,
             "legs": [l.to_dict() for l in a.legs]}
            for a in arbs
        ]
        data["ev_opportunities"] = [
            {"id": e.id, "market": e.market_definition.name if e.market_definition else None,
             "bookmaker": e.bookmaker_name, "selection": e.selection,
             "odds": e.odds, "edge_pct": e.edge_pct}
            for e in evs
        ]

    return _signed_response({"ok": True, "match": data}, encrypt_for=user)


# =============================================================================
# /odds/search
# =============================================================================

@bp_odds.route("/odds/search")
def search_matches():
    """
    Search matches by team / competition across all cached sports.

    Params:
      q           str   — search query (matches home, away, competition)
      sport       str   — filter by sport slug
      mode        str   — upcoming | live | finished (default upcoming)
      from_dt     str   — ISO-8601
      to_dt       str   — ISO-8601
      page        int
      per_page    int
    """
    user = _current_user_from_header()
    q    = (request.args.get("q") or "").strip().lower()
    mode = request.args.get("mode", "upcoming")
    sport_filter = (request.args.get("sport") or "").strip().lower()

    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)

    if not q:
        return _err("Provide query param 'q'", 400)

    # Search DB — always accurate for historical + current
    from app.models.odds_model import UnifiedMatch

    qs = UnifiedMatch.query.filter(
        (UnifiedMatch.home_team_name.ilike(f"%{q}%")) |
        (UnifiedMatch.away_team_name.ilike(f"%{q}%")) |
        (UnifiedMatch.competition_name.ilike(f"%{q}%"))
    )

    if sport_filter:
        qs = qs.filter(UnifiedMatch.sport_name.ilike(f"%{sport_filter}%"))

    if mode == "upcoming":
        qs = qs.filter(UnifiedMatch.start_time >= datetime.now(timezone.utc),
                       UnifiedMatch.status == "PRE_MATCH")
    elif mode == "live":
        qs = qs.filter(UnifiedMatch.status == "IN_PLAY")
    elif mode == "finished":
        qs = qs.filter(UnifiedMatch.status == "FINISHED")

    from_dt = request.args.get("from_dt")
    to_dt   = request.args.get("to_dt")
    if from_dt:
        qs = qs.filter(UnifiedMatch.start_time >= _parse_dt(from_dt))
    if to_dt:
        qs = qs.filter(UnifiedMatch.start_time <= _parse_dt(to_dt))

    total   = qs.count()
    matches = qs.order_by(UnifiedMatch.start_time).offset((page - 1) * per_page).limit(per_page).all()

    log_event("odds_search", {"q": q, "mode": mode, "total": total})

    return _signed_response({
        "ok":      True,
        "q":       q,
        "mode":    mode,
        "total":   total,
        "page":    page,
        "pages":   max(1, (total + per_page - 1) // per_page),
        "matches": [m.to_dict() for m in matches],
    }, encrypt_for=user)


# =============================================================================
# SSE streams  — /stream/odds   /stream/arb   /stream/ev
# =============================================================================

def _sse_stream(channel: str):
    """
    Server-Sent Events generator that subscribes to a Redis pubsub channel
    and forwards messages to the browser.

    Browser usage:
        const es = new EventSource('/api/stream/odds');
        es.onmessage = e => console.log(JSON.parse(e.data));
    """
    import redis as _redis_lib

    url  = celery.conf.broker_url or "redis://localhost:6379/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    r    = _redis_lib.Redis.from_url(f"{base}/2", decode_responses=True)
    ps   = r.pubsub()
    ps.subscribe(channel)

    # Heartbeat every 20 s to keep connection alive
    last_hb = datetime.now(timezone.utc)

    try:
        for message in ps.listen():
            now = datetime.now(timezone.utc)
            if message["type"] == "message":
                yield f"data: {message['data']}\n\n"
            # Heartbeat
            if (now - last_hb).seconds >= 20:
                yield f": ping\n\n"
                last_hb = now
    except GeneratorExit:
        ps.unsubscribe(channel)
    except Exception:
        ps.unsubscribe(channel)


@bp_odds.route("/stream/odds")
def stream_odds():
    """SSE stream for all odds updates (B2B + SBO harvests)."""
    return Response(
        stream_with_context(_sse_stream(_WS_CHANNEL)),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":       "no-cache",
            "X-Accel-Buffering":   "no",   # disables nginx buffering
            "Access-Control-Allow-Origin": "*",
        },
    )


@bp_odds.route("/stream/arb")
def stream_arb():
    """SSE stream for arbitrage alerts."""
    return Response(
        stream_with_context(_sse_stream(_ARB_CHANNEL)),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Access-Control-Allow-Origin": "*"},
    )


@bp_odds.route("/stream/ev")
def stream_ev():
    """SSE stream for Expected Value alerts."""
    return Response(
        stream_with_context(_sse_stream(_EV_CHANNEL)),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Access-Control-Allow-Origin": "*"},
    )


# =============================================================================
# Worker / harvest status (admin)
# =============================================================================

@bp_odds.route("/odds/status")
def harvest_status():
    """Health and harvest status for admin dashboard."""
    from app.workers.celery_tasks import cache_get, cache_keys

    heartbeat = cache_get("worker_heartbeat") or {}
    upcoming  = cache_get("task_status:beat_upcoming") or {}
    live      = cache_get("task_status:beat_live") or {}

    # Count cached matches
    all_keys   = cache_keys("odds:*:*:*:*")
    sbo_keys   = cache_keys("sbo:upcoming:*")
    match_total = 0
    for k in all_keys + sbo_keys:
        d = cache_get(k)
        if d:
            match_total += d.get("match_count", 0)

    return _signed_response({
        "ok":             True,
        "worker_alive":   heartbeat.get("alive", False),
        "last_heartbeat": heartbeat.get("checked_at"),
        "harvest": {
            "upcoming":      upcoming,
            "live":          live,
            "cached_matches": match_total,
        },
    })