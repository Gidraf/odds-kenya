"""
app/views/odds_feed/odds_routes.py
====================================
Comprehensive Odds API — unified output shape on every endpoint.

Fixes vs broken version
────────────────────────
1.  _sse_stream() referenced `celery` which was never imported in this file.
    Fixed: imports `celery` lazily from app.workers.celery_tasks inside the
    function where it is needed.

2.  All match-returning endpoints now run every raw dict through
    _normalise_match() so the output shape is identical to /admin/odds/probe-all:
      best_odds  → always lowercase keys {"1x2": {"1": {odd, bookie}}}
      bookmakers → always {"Name": {fetched_at, market_count}}
      arbitrage  → always list of {market, profit_pct, implied_prob, bets:[]}

3.  Every list response now includes latency_ms, arb_count, bookie_count
    and per_bookmaker stats (same envelope as probe-all).

4.  Added `import time` (was missing).

5.  Blueprint url_prefix changed from "/api/" to "/api" (trailing slash
    caused double-slash URLs on some Flask versions).

Endpoints
─────────
  GET  /api/odds/sports
  GET  /api/odds/bookmakers
  GET  /api/odds/markets
  GET  /api/odds/upcoming/<sport_slug>
  GET  /api/odds/live/<sport_slug>
  GET  /api/odds/results
  GET  /api/odds/results/<date_str>
  GET  /api/odds/match/<parent_match_id>
  GET  /api/odds/search
  GET  /api/odds/status
  GET  /api/stream/odds
  GET  /api/stream/arb
  GET  /api/stream/ev
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, Response, request, stream_with_context

from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
from app.utils.decorators_ import log_event, require_tier
from app.utils.fetcher_utils import TIER_LIMITS, _is_upcoming

bp_odds = Blueprint("odds", __name__, url_prefix="/api")

FREE_MATCH_LIMIT = 100
_WS_CHANNEL  = "odds:updates"
_ARB_CHANNEL = "arb:updates"
_EV_CHANNEL  = "ev:updates"


# =============================================================================
# Shape normaliser — converts any source shape to unified probe-all shape
# =============================================================================

def _normalise_match(m: dict) -> dict:
    """
    Accepts any of:
      • bookmaker_fetcher merged shape  (markets["1X2"]["Home"] = {odds, bookmaker})
      • sbo_fetcher shape               (best_odds["1x2"]["1"] = {odd, bookie})
      • DB .to_dict() shape             (markets_json nested)
    Returns the unified shape used by every API response.
    """
    home = str(m.get("home_team") or m.get("home_team_name") or "")
    away = str(m.get("away_team") or m.get("away_team_name") or "")

    # ── Bookmakers summary ────────────────────────────────────────────────────
    raw_bk = m.get("bookmakers") or {}
    bk_summary: dict = {}
    for bk_name, bk_data in raw_bk.items():
        if not isinstance(bk_data, dict):
            continue
        raw_mkts = bk_data.get("raw_markets") or bk_data.get("markets") or {}
        bk_summary[bk_name] = {
            "fetched_at":   bk_data.get("fetched_at"),
            "market_count": len(raw_mkts) if isinstance(raw_mkts, (list, dict)) else 0,
        }

    # ── Best odds — normalise to lowercase keys ───────────────────────────────
    raw_best = m.get("best_odds") or {}
    best_odds: dict = {}
    for mkt, outcomes in raw_best.items():
        mk = mkt.lower().replace(" ", "_")
        best_odds[mk] = {}
        for out, entry in outcomes.items():
            if isinstance(entry, dict):
                odd    = entry.get("odd") or entry.get("odds") or 0
                bookie = entry.get("bookie") or entry.get("bookmaker") or ""
            else:
                odd    = float(entry) if entry else 0
                bookie = ""
            if float(odd) > 1.0:
                best_odds[mk][out.lower()] = {
                    "odd": round(float(odd), 3), "bookie": bookie}

    # Derive best_odds from raw markets if still empty
    if not best_odds:
        raw_mkts = m.get("unified_markets") or m.get("markets") or {}
        for mkt, outcomes in raw_mkts.items():
            mk = mkt.lower().replace(" ", "_")
            best_odds[mk] = {}
            for out, entries in outcomes.items():
                if isinstance(entries, list):
                    valid = [e for e in entries
                             if isinstance(e, dict) and float(e.get("odd", 0)) > 1.0]
                    if valid:
                        top = max(valid, key=lambda x: x["odd"])
                        best_odds[mk][out.lower()] = {
                            "odd": round(float(top["odd"]), 3),
                            "bookie": top.get("bookie", "")}
                elif isinstance(entries, (int, float)) and entries > 1.0:
                    best_odds[mk][out.lower()] = {
                        "odd": round(float(entries), 3), "bookie": ""}
                elif isinstance(entries, dict):
                    p = (entries.get("best_price") or entries.get("price") or
                         entries.get("odds") or 0)
                    if float(p or 0) > 1.0:
                        best_odds[mk][out.lower()] = {
                            "odd": round(float(p), 3), "bookie": ""}

    arb = m.get("arbitrage") or []
    if isinstance(arb, dict):
        arb = [arb]

    return {
        "betradar_id":     m.get("betradar_id") or m.get("parent_match_id") or m.get("match_id") or "",
        "parent_match_id": m.get("parent_match_id") or m.get("betradar_id") or "",
        "home_team":       home,
        "away_team":       away,
        "competition":     str(m.get("competition") or m.get("competition_name") or ""),
        "sport":           str(m.get("sport") or m.get("sport_name") or ""),
        "start_time":      m.get("start_time"),
        "status":          m.get("status") or "upcoming",
        "score_home":      m.get("score_home"),
        "score_away":      m.get("score_away"),
        "bookie_count":    len(bk_summary) or int(m.get("bookie_count") or 0),
        "market_count":    len(best_odds)  or int(m.get("market_count") or 0),
        "best_odds":       best_odds,
        "bookmakers":      bk_summary,
        "arbitrage":       arb,
    }


def _per_bookmaker_stats(matches: list[dict]) -> dict:
    stats: dict = {}
    for m in matches:
        for bk in (m.get("bookmakers") or {}):
            if bk not in stats:
                stats[bk] = {"ok": True, "count": 0,
                             "latency_ms": None, "error": None}
            stats[bk]["count"] += 1
    return stats


def _build_envelope(
    matches: list[dict],
    sport: str,
    mode: str,
    tier: str,
    page: int,
    per_page: int,
    truncated: bool,
    latency_ms: int,
    extra: dict | None = None,
) -> dict:
    arb_count = sum(1 for m in matches if m.get("arbitrage"))
    bk_names  = set()
    for m in matches:
        bk_names.update((m.get("bookmakers") or {}).keys())

    total = len(matches)
    start = (page - 1) * per_page
    env = {
        "ok":           True,
        "sport":        sport,
        "mode":         mode,
        "tier":         tier,
        "total":        total,
        "page":         page,
        "per_page":     per_page,
        "pages":        max(1, (total + per_page - 1) // per_page),
        "truncated":    truncated,
        "latency_ms":   latency_ms,
        "arb_count":    arb_count,
        "bookie_count": len(bk_names),
        "bookmakers":   sorted(bk_names),
        "per_bookmaker":_per_bookmaker_stats(matches),
        "matches":      matches[start: start + per_page],
    }
    if truncated:
        env["upgrade_message"] = "Upgrade your plan to see all matches."
    if extra:
        env.update(extra)
    return env


# =============================================================================
# Filter / sort / tier helpers
# =============================================================================

def _parse_dt(val: str | None) -> datetime:
    if not val:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


def _apply_filters(matches: list[dict], args) -> list[dict]:
    bookmaker   = (args.get("bookmaker",   "") or "").strip().lower()
    market      = (args.get("market",      "") or "").strip().lower()
    team        = (args.get("team",        "") or "").strip().lower()
    competition = (args.get("competition", "") or "").strip().lower()
    from_dt     = args.get("from_dt")
    to_dt       = args.get("to_dt")
    date_str    = args.get("date")

    if date_str:
        try:
            d       = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            from_dt = from_dt or d.isoformat()
            to_dt   = to_dt   or (d + timedelta(days=1)).isoformat()
        except ValueError:
            pass

    fdt = _parse_dt(from_dt) if from_dt else None
    tdt = _parse_dt(to_dt)   if to_dt   else None

    result = []
    for m in matches:
        if bookmaker and not any(bookmaker in k.lower()
                                 for k in (m.get("bookmakers") or {})):
            continue
        if market and not any(market in k.lower()
                              for k in (m.get("best_odds") or {})):
            continue
        if team:
            if (team not in m.get("home_team", "").lower() and
                    team not in m.get("away_team", "").lower()):
                continue
        if competition and competition not in m.get("competition", "").lower():
            continue
        if fdt or tdt:
            st = _parse_dt(m.get("start_time"))
            if fdt and st < fdt: continue
            if tdt and st > tdt: continue
        result.append(m)
    return result


def _sort_matches(matches: list[dict], sort: str) -> list[dict]:
    if sort == "market_count":
        return sorted(matches, key=lambda m: m.get("market_count", 0), reverse=True)
    if sort == "arb":
        return sorted(matches,
                      key=lambda m: -max(
                          (a.get("profit_pct", 0) for a in m.get("arbitrage", [])),
                          default=0))
    return sorted(matches, key=lambda m: _parse_dt(m.get("start_time")))


def _filter_by_tier(matches: list[dict], user) -> tuple[list[dict], bool]:
    limits     = (user.limits if user else None) or \
                 {"max_matches": FREE_MATCH_LIMIT, "days_ahead": 0}
    days_ahead = limits.get("days_ahead", 0)

    if days_ahead == 0:
        cutoff  = datetime.now(timezone.utc) + timedelta(days=1)
        matches = [m for m in matches
                   if not m.get("start_time") or
                   _parse_dt(m["start_time"]) <= cutoff]
    elif days_ahead > 0:
        cutoff  = datetime.now(timezone.utc) + timedelta(days=days_ahead)
        matches = [m for m in matches
                   if not m.get("start_time") or
                   _parse_dt(m["start_time"]) <= cutoff]

    max_m = limits.get("max_matches") or FREE_MATCH_LIMIT
    if max_m and len(matches) > max_m:
        return matches[:max_m], True
    return matches, False


def _strip_arb_bets(matches: list[dict], tier: str) -> list[dict]:
    """
    Free / basic users see arb flag + profit_pct but NOT the individual bet
    legs (which reveal exact stake splits — pro+ feature).
    """
    if tier in ("pro", "premium"):
        return matches
    for m in matches:
        for arb in (m.get("arbitrage") or []):
            arb.pop("bets", None)
    return matches


# =============================================================================
# /odds/sports
# =============================================================================

@bp_odds.route("/odds/sports")
def list_sports():
    from app.workers.celery_tasks import cache_keys
    sports: set[str] = set()
    for k in cache_keys("odds:upcoming:*:*:p1"):
        parts = k.split(":")
        if len(parts) >= 3:
            sports.add(parts[2].replace("_", " ").title())
    for k in cache_keys("sbo:upcoming:*"):
        parts = k.split(":")
        if len(parts) >= 3:
            sports.add(parts[2].replace("-", " ").title())
    return _signed_response({"ok": True, "sports": sorted(sports)})


# =============================================================================
# /odds/bookmakers
# =============================================================================

@bp_odds.route("/odds/bookmakers")
def list_bookmakers():
    from app.workers.celery_tasks import cache_keys, cache_get
    from app.models.bookmakers_model import Bookmaker

    result = []
    for bm in Bookmaker.query.filter_by(is_active=True).all():
        keys    = cache_keys(f"odds:*:*:{bm.id}:p1")
        last_ts = None
        total   = 0
        for k in keys:
            d = cache_get(k)
            if d:
                total += d.get("match_count", 0)
                ts = d.get("harvested_at")
                if ts and (not last_ts or ts > last_ts):
                    last_ts = ts
        result.append({
            "id":           bm.id,
            "name":         bm.name,
            "domain":       bm.domain,
            "vendor_slug":  getattr(bm, "vendor_slug", "betb2b"),
            "is_active":    bm.is_active,
            "last_harvest": last_ts,
            "match_count":  total,
        })
    return _signed_response({"ok": True, "bookmakers": result, "total": len(result)})


# =============================================================================
# /odds/markets
# =============================================================================

@bp_odds.route("/odds/markets")
def list_markets():
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
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = user.tier if user else "free"
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)
    sort     = request.args.get("sort", "start_time")

    log_event("odds_upcoming", {"sport": sport_slug, "tier": tier})

    from app.workers.celery_tasks import cache_get, cache_keys
    from app.views.odds_feed.bookmaker_fetcher import merge_bookmaker_results

    raw: list[dict] = []

    # SBO cache
    sbo = cache_get(f"sbo:upcoming:{sport_slug.lower()}")
    if sbo:
        raw.extend(sbo.get("matches", []))

    # B2B cache — merge pages first then extend
    b2b_pages: list[list[dict]] = []
    for k in cache_keys(f"odds:upcoming:{sport_slug.lower().replace(' ','_')}:*"):
        d = cache_get(k)
        if d and d.get("matches"):
            b2b_pages.append(d["matches"])
    if b2b_pages:
        raw.extend(merge_bookmaker_results(b2b_pages))

    # Normalise + deduplicate
    seen:    set[str]   = set()
    matches: list[dict] = []
    for nm in (_normalise_match(x) for x in raw):
        key = f"{nm['home_team'].lower()}|{nm['away_team'].lower()}"
        if key not in seen:
            seen.add(key)
            matches.append(nm)

    now     = datetime.now(timezone.utc)
    matches = [m for m in matches if _is_upcoming(m, now)]
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort)
    matches, truncated = _filter_by_tier(matches, user)
    matches = _strip_arb_bets(matches, tier)

    latency = int((time.perf_counter() - t0) * 1000)
    return _signed_response(
        _build_envelope(matches, sport_slug, "upcoming", tier,
                        page, per_page, truncated, latency),
        encrypt_for=user,
    )


# =============================================================================
# /odds/live/<sport_slug>
# =============================================================================

@bp_odds.route("/odds/live/<sport_slug>")
def get_live(sport_slug: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = user.tier if user else "free"
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)
    sort     = request.args.get("sort", "start_time")

    log_event("odds_live", {"sport": sport_slug, "tier": tier})

    from app.workers.celery_tasks import cache_get, cache_keys
    from app.views.odds_feed.bookmaker_fetcher import merge_bookmaker_results

    b2b_pages: list[list[dict]] = []
    for k in cache_keys(f"odds:live:{sport_slug.lower().replace(' ','_')}:*"):
        d = cache_get(k)
        if d and d.get("matches"):
            b2b_pages.append(d["matches"])

    raw     = merge_bookmaker_results(b2b_pages) if b2b_pages else []
    matches = [_normalise_match(m) for m in raw]
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort)
    matches, truncated = _filter_by_tier(matches, user)
    matches = _strip_arb_bets(matches, tier)

    latency = int((time.perf_counter() - t0) * 1000)
    return _signed_response(
        _build_envelope(matches, sport_slug, "live", tier,
                        page, per_page, truncated, latency),
        encrypt_for=user,
    )


# =============================================================================
# /odds/results  &  /odds/results/<date_str>
# =============================================================================

@bp_odds.route("/odds/results")
@require_tier("basic", "pro", "premium")
def get_results():
    date_str = request.args.get(
        "date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    return _get_finished_by_date(date_str)


@bp_odds.route("/odds/results/<date_str>")
@require_tier("basic", "pro", "premium")
def get_results_by_date(date_str: str):
    return _get_finished_by_date(date_str)


def _get_finished_by_date(date_str: str) -> Response:
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = user.tier if user else "free"
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)

    log_event("finished_games_view", {"date": date_str})

    from app.workers.celery_tasks import cache_get
    from app.models.odds_model import UnifiedMatch

    raw: list[dict] = []
    cached = cache_get(f"results:finished:{date_str}")
    if cached:
        raw = cached
    else:
        try:
            day_start = datetime.strptime(
                date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            day_end   = day_start + timedelta(days=1)
        except ValueError:
            return _err("Invalid date format. Use YYYY-MM-DD.", 400)

        qs = UnifiedMatch.query.filter(
            UnifiedMatch.status     == "FINISHED",
            UnifiedMatch.start_time >= day_start,
            UnifiedMatch.start_time <  day_end,
        )
        if sport := request.args.get("sport"):
            qs = qs.filter(UnifiedMatch.sport_name.ilike(f"%{sport}%"))
        if comp := request.args.get("competition"):
            qs = qs.filter(UnifiedMatch.competition_name.ilike(f"%{comp}%"))
        if team := request.args.get("team"):
            qs = qs.filter(
                (UnifiedMatch.home_team_name.ilike(f"%{team}%")) |
                (UnifiedMatch.away_team_name.ilike(f"%{team}%"))
            )
        raw = [m.to_dict() for m in
               qs.order_by(UnifiedMatch.start_time).all()]

    matches = [_normalise_match(m) for m in raw]
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, request.args.get("sort", "start_time"))

    latency = int((time.perf_counter() - t0) * 1000)
    return _signed_response(
        _build_envelope(matches, date_str, "finished", tier,
                        page, per_page, False, latency,
                        extra={"date": date_str}),
    )


# =============================================================================
# /odds/match/<parent_match_id>
# =============================================================================

@bp_odds.route("/odds/match/<parent_match_id>")
def get_match(parent_match_id: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = user.tier if user else "free"

    from app.models.odds_model import UnifiedMatch

    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um:
        return _err("Match not found", 404)

    log_event("match_view", {"match_id": parent_match_id, "tier": tier})

    match = _normalise_match(um.to_dict(include_bookmaker_odds=True))
    # Include full market book for the detail view
    match["markets"] = um.markets_json or {}

    if tier in ("pro", "premium"):
        from app.models.odds_model import ArbitrageOpportunity, EVOpportunity
        arbs = ArbitrageOpportunity.query.filter_by(
            match_id=um.id, is_active=True).all()
        evs  = EVOpportunity.query.filter_by(
            match_id=um.id, is_active=True).all()
        match["arbitrage_opportunities"] = [
            {
                "id":         a.id,
                "profit_pct": a.max_profit_percentage,
                "market":     a.market_definition.name if a.market_definition else None,
                "legs":       [leg.to_dict() for leg in a.legs],
            }
            for a in arbs
        ]
        match["ev_opportunities"] = [
            {
                "id":        e.id,
                "market":    e.market_definition.name if e.market_definition else None,
                "bookmaker": e.bookmaker_name,
                "selection": e.selection,
                "odds":      e.odds,
                "edge_pct":  e.edge_pct,
            }
            for e in evs
        ]

    return _signed_response({
        "ok":         True,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
        "match":      match,
    }, encrypt_for=user)


# =============================================================================
# /odds/search
# =============================================================================

@bp_odds.route("/odds/search")
def search_matches():
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = user.tier if user else "free"
    q    = (request.args.get("q") or "").strip().lower()
    mode = request.args.get("mode", "upcoming")
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)

    if not q:
        return _err("Provide query param 'q'", 400)

    from app.models.odds_model import UnifiedMatch

    qs = UnifiedMatch.query.filter(
        (UnifiedMatch.home_team_name.ilike(f"%{q}%")) |
        (UnifiedMatch.away_team_name.ilike(f"%{q}%")) |
        (UnifiedMatch.competition_name.ilike(f"%{q}%"))
    )
    if sport := (request.args.get("sport") or "").strip():
        qs = qs.filter(UnifiedMatch.sport_name.ilike(f"%{sport}%"))
    if mode == "upcoming":
        qs = qs.filter(UnifiedMatch.start_time >= datetime.now(timezone.utc),
                       UnifiedMatch.status == "PRE_MATCH")
    elif mode == "live":
        qs = qs.filter(UnifiedMatch.status == "IN_PLAY")
    elif mode == "finished":
        qs = qs.filter(UnifiedMatch.status == "FINISHED")
    if fdt := request.args.get("from_dt"):
        qs = qs.filter(UnifiedMatch.start_time >= _parse_dt(fdt))
    if tdt := request.args.get("to_dt"):
        qs = qs.filter(UnifiedMatch.start_time <= _parse_dt(tdt))

    total   = qs.count()
    db_rows = (qs.order_by(UnifiedMatch.start_time)
               .offset((page - 1) * per_page).limit(per_page).all())
    matches = [_normalise_match(m.to_dict()) for m in db_rows]
    matches = _strip_arb_bets(matches, tier)

    log_event("odds_search", {"q": q, "mode": mode, "total": total})

    return _signed_response({
        "ok":          True,
        "q":           q,
        "mode":        mode,
        "tier":        tier,
        "total":       total,
        "page":        page,
        "per_page":    per_page,
        "pages":       max(1, (total + per_page - 1) // per_page),
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
        "per_bookmaker": _per_bookmaker_stats(matches),
        "matches":     matches,
    }, encrypt_for=user)


# =============================================================================
# /odds/status
# =============================================================================

@bp_odds.route("/odds/status")
def harvest_status():
    from app.workers.celery_tasks import cache_get, cache_keys
    heartbeat   = cache_get("worker_heartbeat") or {}
    match_total = 0
    for k in cache_keys("odds:*:*:*:*") + cache_keys("sbo:upcoming:*"):
        d = cache_get(k)
        if d:
            match_total += d.get("match_count", 0)
    return _signed_response({
        "ok":             True,
        "worker_alive":   heartbeat.get("alive", False),
        "last_heartbeat": heartbeat.get("checked_at"),
        "cached_matches": match_total,
        "harvest": {
            "upcoming": cache_get("task_status:beat_upcoming") or {},
            "live":     cache_get("task_status:beat_live") or {},
        },
    })


# =============================================================================
# SSE streams
# =============================================================================

def _sse_stream(channel: str):
    import redis as _rl
    # Import celery lazily — avoids a circular import at module load time
    from app.workers.celery_tasks import celery as _celery
    url  = _celery.conf.broker_url or "redis://localhost:6379/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    r    = _rl.Redis.from_url(f"{base}/2", decode_responses=True)
    ps   = r.pubsub()
    ps.subscribe(channel)
    last_hb = datetime.now(timezone.utc)
    try:
        for msg in ps.listen():
            now = datetime.now(timezone.utc)
            if msg["type"] == "message":
                yield f"data: {msg['data']}\n\n"
            if (now - last_hb).seconds >= 20:
                yield ": ping\n\n"
                last_hb = now
    except (GeneratorExit, Exception):
        ps.unsubscribe(channel)


_SSE_HEADERS = {
    "Cache-Control":               "no-cache",
    "X-Accel-Buffering":           "no",
    "Access-Control-Allow-Origin": "*",
}


@bp_odds.route("/stream/odds")
def stream_odds():
    return Response(stream_with_context(_sse_stream(_WS_CHANNEL)),
                    mimetype="text/event-stream", headers=_SSE_HEADERS)


@bp_odds.route("/stream/arb")
def stream_arb():
    return Response(stream_with_context(_sse_stream(_ARB_CHANNEL)),
                    mimetype="text/event-stream", headers=_SSE_HEADERS)


@bp_odds.route("/stream/ev")
def stream_ev():
    return Response(stream_with_context(_sse_stream(_EV_CHANNEL)),
                    mimetype="text/event-stream", headers=_SSE_HEADERS)