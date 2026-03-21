"""
app/views/odds_feed/odds_routes.py
====================================
Unified Odds API.

Access model
------------
All endpoints are FREE by default (FREE_ACCESS = True).
Use  POST /api/admin/odds/access  to toggle per-endpoint or global access.
Use  GET  /api/odds/access        to read current config (admin only).

Data sources read (all cache prefixes merged)
---------------------------------------------
  sbo:upcoming:{sport}   legacy SBO cache
  sp:{mode}:{sport}      Sportpesa
  bt:{mode}:{sport}      Betika
  od:{mode}:{sport}      Odibets
  b2b:{mode}:{sport}     B2B family (1xBet, 22Bet, Helabet, Paripesa …)

Endpoints
---------
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
  GET  /api/odds/access                  ← current access config (admin)
  POST /api/admin/odds/access            ← update access config (admin)
  GET  /api/stream/odds
  GET  /api/stream/arb
  GET  /api/stream/ev
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, Response, request, stream_with_context

from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
from app.utils.decorators_ import log_event

bp_odds = Blueprint("odds", __name__, url_prefix="/api")

# ─── Access configuration ────────────────────────────────────────────────────
# Set FREE_ACCESS = True to bypass all tier restrictions platform-wide.
# The /api/admin/odds/access endpoint can toggle this at runtime.
FREE_ACCESS: bool = True

# Per-endpoint overrides: endpoint_key → "free" | "basic" | "pro" | "premium"
# endpoint_key matches the function name (e.g. "get_upcoming", "get_results")
_ENDPOINT_ACCESS: dict[str, str] = {
    "get_upcoming":      "free",
    "get_live":          "free",
    "get_results":       "free",
    "get_results_by_date":"free",
    "get_match":         "free",
    "search_matches":    "free",
    "list_sports":       "free",
    "list_bookmakers":   "free",
    "list_markets":      "free",
    "harvest_status":    "free",
}

_TIER_ORDER = {"free": 0, "basic": 1, "pro": 2, "premium": 3}

FREE_MATCH_LIMIT = 1000   # effectively unlimited for free tier
_WS_CHANNEL  = "odds:updates"
_ARB_CHANNEL = "arb:updates"
_EV_CHANNEL  = "ev:updates"

# All cache prefixes that contain match data
_CACHE_PREFIXES = ["sbo", "sp", "bt", "od", "b2b"]


def _required_tier(endpoint_name: str) -> str:
    if FREE_ACCESS:
        return "free"
    return _ENDPOINT_ACCESS.get(endpoint_name, "free")


def _user_has_access(user, endpoint_name: str) -> bool:
    if FREE_ACCESS:
        return True
    req_tier = _ENDPOINT_ACCESS.get(endpoint_name, "free")
    if req_tier == "free":
        return True
    user_tier = getattr(user, "tier", "free") if user else "free"
    return _TIER_ORDER.get(user_tier, 0) >= _TIER_ORDER.get(req_tier, 0)


# =============================================================================
# Shape normaliser
# =============================================================================

def _normalise_match(m: dict) -> dict:
    home = str(m.get("home_team") or m.get("home_team_name") or "")
    away = str(m.get("away_team") or m.get("away_team_name") or "")

    # Bookmakers summary
    raw_bk = m.get("bookmakers") or {}
    bk_summary: dict = {}
    for bk_name, bk_data in raw_bk.items():
        if not isinstance(bk_data, dict):
            continue
        raw_mkts = bk_data.get("markets") or bk_data.get("raw_markets") or {}
        bk_summary[bk_name] = {
            "fetched_at":   bk_data.get("fetched_at"),
            "market_count": len(raw_mkts),
        }

    # Best odds — normalise to canonical lowercase keys
    raw_best = m.get("best_odds") or m.get("markets") or {}
    best_odds: dict = {}
    for mkt, outcomes in raw_best.items():
        mk = mkt.lower()
        best_odds[mk] = {}
        for out, entry in outcomes.items():
            if isinstance(entry, dict):
                odd    = float(entry.get("odd") or entry.get("odds") or 0)
                bookie = str(entry.get("bookie") or entry.get("bookmaker") or "")
            else:
                odd    = float(entry) if entry else 0
                bookie = ""
            if odd > 1.0:
                best_odds[mk][out.lower()] = {"odd": round(odd, 3), "bookie": bookie}

    # Derive from unified_markets if best_odds still empty
    if not best_odds:
        for mkt, outcomes in (m.get("unified_markets") or {}).items():
            mk = mkt.lower()
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
                    best_odds[mk][out.lower()] = {"odd": round(float(entries), 3), "bookie": ""}
                elif isinstance(entries, dict):
                    p = (entries.get("best_price") or entries.get("price") or 0)
                    if float(p or 0) > 1.0:
                        best_odds[mk][out.lower()] = {"odd": round(float(p), 3), "bookie": ""}

    arb = m.get("arbitrage") or []
    if isinstance(arb, dict):
        arb = [arb]

    # Detect source from cache data
    source = m.get("source") or ""

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
        "source":          source,
        "bookie_count":    len(bk_summary) or int(m.get("bookie_count") or 0),
        "market_count":    len(best_odds) or int(m.get("market_count") or 0),
        "best_odds":       best_odds,
        "bookmakers":      bk_summary,
        "arbitrage":       arb,
    }


def _per_bookmaker_stats(matches: list[dict]) -> dict:
    stats: dict = {}
    for m in matches:
        for bk in (m.get("bookmakers") or {}):
            if bk not in stats:
                stats[bk] = {"ok": True, "count": 0, "latency_ms": None, "error": None}
            stats[bk]["count"] += 1
    return stats


def _build_envelope(
    matches: list[dict], sport: str, mode: str, tier: str,
    page: int, per_page: int, truncated: bool, latency_ms: int,
    extra: dict | None = None,
) -> dict:
    arb_count = sum(1 for m in matches if m.get("arbitrage"))
    bk_names  = set()
    for m in matches:
        bk_names.update((m.get("bookmakers") or {}).keys())
    total  = len(matches)
    start  = (page - 1) * per_page
    env = {
        "ok": True, "sport": sport, "mode": mode, "tier": tier,
        "total": total, "page": page, "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
        "truncated": truncated, "latency_ms": latency_ms,
        "arb_count": arb_count, "bookie_count": len(bk_names),
        "bookmakers": sorted(bk_names),
        "per_bookmaker": _per_bookmaker_stats(matches),
        "matches": matches[start: start + per_page],
    }
    if truncated:
        env["upgrade_message"] = "Upgrade your plan to see all matches."
    if extra:
        env.update(extra)
    return env


# =============================================================================
# Filter / sort helpers
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
    source_f    = (args.get("source",      "") or "").strip().lower()
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
        if bookmaker and not any(bookmaker in k.lower() for k in (m.get("bookmakers") or {})):
            continue
        if market and not any(market in k for k in (m.get("best_odds") or {})):
            continue
        if team:
            if (team not in m.get("home_team", "").lower() and
                    team not in m.get("away_team", "").lower()):
                continue
        if competition and competition not in m.get("competition", "").lower():
            continue
        if source_f and source_f not in m.get("source", "").lower():
            continue
        if fdt or tdt:
            st = _parse_dt(m.get("start_time"))
            if fdt and st < fdt:
                continue
            if tdt and st > tdt:
                continue
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


def _apply_tier_limits(matches: list[dict], user) -> tuple[list[dict], bool]:
    if FREE_ACCESS:
        return matches, False
    limits     = (user.limits if user else None) or {"max_matches": FREE_MATCH_LIMIT, "days_ahead": 0}
    days_ahead = limits.get("days_ahead", 0)
    if days_ahead >= 0:
        cutoff  = datetime.now(timezone.utc) + timedelta(days=max(1, days_ahead))
        matches = [m for m in matches
                   if not m.get("start_time") or _parse_dt(m["start_time"]) <= cutoff]
    max_m = limits.get("max_matches") or FREE_MATCH_LIMIT
    if max_m and len(matches) > max_m:
        return matches[:max_m], True
    return matches, False


def _strip_arb_bets(matches: list[dict], tier: str) -> list[dict]:
    if FREE_ACCESS or tier in ("pro", "premium"):
        return matches
    for m in matches:
        for arb in (m.get("arbitrage") or []):
            arb.pop("bets", None)
    return matches


# =============================================================================
# Cache read helpers
# =============================================================================

def _read_all_sources(mode: str, sport_slug: str) -> list[dict]:
    """Read match lists from all cache prefixes for this sport+mode."""
    from app.workers.celery_tasks import cache_get

    raw: list[dict] = []
    slug = sport_slug.lower().replace(" ", "_").replace("-", "_")

    for prefix in _CACHE_PREFIXES:
        cached = cache_get(f"{prefix}:{mode}:{slug}")
        if cached and cached.get("matches"):
            for m in cached["matches"]:
                m.setdefault("_cache_source", prefix)
            raw.extend(cached["matches"])

    # Also read paged B2B/SBO cache keys (legacy pattern odds:upcoming:sport:bkid:pN)
    from app.workers.celery_tasks import cache_keys
    for k in cache_keys(f"odds:{mode}:{slug}:*") + cache_keys(f"odds:{mode}:{slug}"):
        d = cache_get(k)
        if d and d.get("matches"):
            raw.extend(d["matches"])

    return raw


def _deduplicate(matches: list[dict]) -> list[dict]:
    seen: set[str] = set()
    result = []
    for m in matches:
        key = (
            f"{m.get('home_team', '').lower().strip()}"
            f"|{m.get('away_team', '').lower().strip()}"
            f"|{m.get('start_time', '')[:13]}"
        )
        if key not in seen:
            seen.add(key)
            result.append(m)
    return result


# =============================================================================
# /odds/sports
# =============================================================================

@bp_odds.route("/odds/sports")
def list_sports():
    from app.workers.celery_tasks import cache_keys

    sports: dict[str, int] = {}
    for prefix in _CACHE_PREFIXES:
        for k in cache_keys(f"{prefix}:upcoming:*") + cache_keys(f"{prefix}:live:*"):
            parts = k.split(":")
            if len(parts) >= 3:
                sp = parts[2].replace("_", " ").title()
                sports[sp] = sports.get(sp, 0) + 1

    # Also DB
    try:
        from app.models.odds_model import UnifiedMatch
        from sqlalchemy import func
        for row in UnifiedMatch.query.with_entities(
            UnifiedMatch.sport_name, func.count(UnifiedMatch.id)
        ).group_by(UnifiedMatch.sport_name).all():
            if row[0]:
                sports[row[0]] = sports.get(row[0], 0) + row[1]
    except Exception:
        pass

    return _signed_response({"ok": True, "sports": sorted(sports.keys()),
                              "counts": sports})


# =============================================================================
# /odds/bookmakers
# =============================================================================

@bp_odds.route("/odds/bookmakers")
def list_bookmakers():
    from app.workers.celery_tasks import cache_keys, cache_get
    from app.models.bookmakers_model import Bookmaker

    result = []
    for bm in Bookmaker.query.filter_by(is_active=True).order_by(Bookmaker.name).all():
        total  = 0
        last_ts = None
        # Check both legacy and new cache keys
        for pattern in [f"odds:*:*:{bm.id}:p1", f"sp:*:*", f"bt:*:*", f"od:*:*", f"b2b:*:*"]:
            for k in cache_keys(pattern):
                d = cache_get(k)
                if d and str(bm.name).lower() in str(d.get("source", "")).lower():
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
            "group":        "local" if getattr(bm, "vendor_slug", "") in ("sportpesa", "betika", "odibets") else "international",
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
    tier = getattr(user, "tier", "free") if user else "free"
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)
    sort     = request.args.get("sort", "start_time")

    log_event("odds_upcoming", {"sport": sport_slug, "tier": tier})

    raw     = _read_all_sources("upcoming", sport_slug)
    matches = [_normalise_match(x) for x in _deduplicate(raw)]
    now     = datetime.now(timezone.utc)
    matches = [m for m in matches if not m.get("start_time") or
               _parse_dt(m["start_time"]) >= now - timedelta(hours=1)]
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort)
    matches, truncated = _apply_tier_limits(matches, user)
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
    tier = getattr(user, "tier", "free") if user else "free"
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)
    sort     = request.args.get("sort", "start_time")

    log_event("odds_live", {"sport": sport_slug, "tier": tier})

    raw     = _read_all_sources("live", sport_slug)
    matches = [_normalise_match(m) for m in _deduplicate(raw)]
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort)
    matches, truncated = _apply_tier_limits(matches, user)
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
def get_results():
    date_str = request.args.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    return _get_finished_by_date(date_str)


@bp_odds.route("/odds/results/<date_str>")
def get_results_by_date(date_str: str):
    return _get_finished_by_date(date_str)


def _get_finished_by_date(date_str: str) -> Response:
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
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
            day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
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
        raw = [m.to_dict() for m in qs.order_by(UnifiedMatch.start_time).all()]

    matches = [_normalise_match(m) for m in raw]
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, request.args.get("sort", "start_time"))
    _, truncated = _apply_tier_limits(matches, user)

    latency = int((time.perf_counter() - t0) * 1000)
    return _signed_response(
        _build_envelope(matches, date_str, "finished", tier,
                        page, per_page, truncated, latency,
                        extra={"date": date_str}),
    )


# =============================================================================
# /odds/match/<parent_match_id>
# =============================================================================

@bp_odds.route("/odds/match/<parent_match_id>")
def get_match(parent_match_id: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"

    from app.models.odds_model import UnifiedMatch

    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um:
        return _err("Match not found", 404)

    log_event("match_view", {"match_id": parent_match_id, "tier": tier})

    match = _normalise_match(um.to_dict(include_bookmaker_odds=True))
    match["markets"] = um.markets_json or {}

    # Always include arb + EV data (all free)
    try:
        from app.models.odds_model import ArbitrageOpportunity, EVOpportunity
        arbs = ArbitrageOpportunity.query.filter_by(match_id=um.id, is_active=True).all()
        evs  = EVOpportunity.query.filter_by(match_id=um.id, is_active=True).all()
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
    except Exception:
        pass

    return _signed_response({
        "ok": True,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
        "match": match,
    }, encrypt_for=user)


# =============================================================================
# /odds/search
# =============================================================================

@bp_odds.route("/odds/search")
def search_matches():
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
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

    total   = qs.count()
    db_rows = (qs.order_by(UnifiedMatch.start_time)
               .offset((page - 1) * per_page).limit(per_page).all())
    matches = [_normalise_match(m.to_dict()) for m in db_rows]
    matches = _strip_arb_bets(matches, tier)

    log_event("odds_search", {"q": q, "mode": mode, "total": total})
    return _signed_response({
        "ok": True, "q": q, "mode": mode, "tier": tier,
        "total": total, "page": page, "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
        "per_bookmaker": _per_bookmaker_stats(matches),
        "matches": matches,
    }, encrypt_for=user)


# =============================================================================
# /odds/status
# =============================================================================

@bp_odds.route("/odds/status")
def harvest_status():
    from app.workers.celery_tasks import cache_get, cache_keys

    heartbeat   = cache_get("worker_heartbeat") or {}
    source_stats: dict = {}
    match_total = 0

    for prefix in _CACHE_PREFIXES:
        for k in cache_keys(f"{prefix}:*:*"):
            d = cache_get(k)
            if d:
                cnt = d.get("match_count", 0)
                match_total += cnt
                src = d.get("source", prefix)
                if src not in source_stats:
                    source_stats[src] = {"match_count": 0, "last_harvest": None}
                source_stats[src]["match_count"] += cnt
                ts = d.get("harvested_at")
                if ts and (not source_stats[src]["last_harvest"] or
                           ts > source_stats[src]["last_harvest"]):
                    source_stats[src]["last_harvest"] = ts

    return _signed_response({
        "ok":             True,
        "free_access":    FREE_ACCESS,
        "worker_alive":   heartbeat.get("alive", False),
        "last_heartbeat": heartbeat.get("checked_at"),
        "cached_matches": match_total,
        "sources":        source_stats,
    })


# =============================================================================
# /odds/access  (read config)  &  /admin/odds/access  (write config)
# =============================================================================

@bp_odds.route("/odds/access")
def get_access_config():
    user = _current_user_from_header()
    if not user or not getattr(user, "is_admin", False):
        return _err("Admin only", 403)
    return _signed_response({
        "ok":              True,
        "free_access":     FREE_ACCESS,
        "endpoint_access": _ENDPOINT_ACCESS,
    })


@bp_odds.route("/admin/odds/access", methods=["POST"])
def set_access_config():
    global FREE_ACCESS
    user = _current_user_from_header()
    if not user or not getattr(user, "is_admin", False):
        return _err("Admin only", 403)

    body = request.get_json(force=True) or {}

    if "free_access" in body:
        FREE_ACCESS = bool(body["free_access"])

    if isinstance(body.get("endpoint_access"), dict):
        valid_tiers = {"free", "basic", "pro", "premium"}
        for endpoint, tier in body["endpoint_access"].items():
            if endpoint in _ENDPOINT_ACCESS and tier in valid_tiers:
                _ENDPOINT_ACCESS[endpoint] = tier

    return _signed_response({
        "ok":              True,
        "free_access":     FREE_ACCESS,
        "endpoint_access": _ENDPOINT_ACCESS,
    })


# =============================================================================
# SSE streams
# =============================================================================

def _sse_stream(channel: str):
    import redis as _rl
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