"""
app/views/odds_feed/odds_routes.py
====================================
Unified Odds API.

Data sources (priority order)
------------------------------
  1. PostgreSQL  — UnifiedMatch + BookmakerMatchOdds  (primary, always fresh)
  2. Redis cache — sp/bt/od/b2b/sbo prefixes          (fallback / enrichment)

Every endpoint that returns match data now uses the DB as the primary source.
The response always includes:
  • markets_by_bk  {bk_slug: {market_slug: {outcome: odd}}}
  • best           {market_slug: {outcome: {odd, bk}}}
  • arb_markets    [{market, profit_pct, arb_sum, legs}]
  • bookmakers     {bk_slug: {bookmaker, market_count, markets, link}}

These shapes are consumed directly by OddsComparisonTab (odds_api source mode).

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
  GET  /api/odds/access
  POST /api/admin/odds/access
  GET  /api/stream/odds
  GET  /api/stream/arb
  GET  /api/stream/ev
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from typing import Any

from flask import Blueprint, Response, request, stream_with_context

from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
from app.utils.decorators_ import log_event

bp_odds_customer = Blueprint("odds-customer", __name__, url_prefix="/api")

FREE_ACCESS: bool = True
_ENDPOINT_ACCESS: dict[str, str] = {
    "get_upcoming": "free", "get_live": "free",
    "get_results": "free", "get_results_by_date": "free",
    "get_match": "free", "search_matches": "free",
    "list_sports": "free", "list_bookmakers": "free",
    "list_markets": "free", "harvest_status": "free",
}
_TIER_ORDER    = {"free": 0, "basic": 1, "pro": 2, "premium": 3}
FREE_MATCH_LIMIT = 1000
_WS_CHANNEL    = "odds:updates"
_ARB_CHANNEL   = "arb:updates"
_EV_CHANNEL    = "ev:updates"
_CACHE_PREFIXES = ["sbo", "sp", "bt", "od", "b2b"]

_BK_SLUG: dict[str, str] = {
    "sportpesa": "sp", "betika": "bt", "odibets": "od",
    "sp": "sp", "bt": "bt", "od": "od",
    "sbo": "sbo", "b2b": "b2b",
}


def _bk_slug(name: str) -> str:
    return _BK_SLUG.get(name.lower(), name.lower()[:4])


def _user_has_access(user, endpoint_name: str) -> bool:
    if FREE_ACCESS:
        return True
    req   = _ENDPOINT_ACCESS.get(endpoint_name, "free")
    if req == "free":
        return True
    utier = getattr(user, "tier", "free") if user else "free"
    return _TIER_ORDER.get(utier, 0) >= _TIER_ORDER.get(req, 0)


def _load_db_matches(
    sport_slug:  str,
    mode:        str = "upcoming",
    page:        int = 1,
    per_page:    int = 20,
    comp_filter: str = "",
    team_filter: str = "",
    has_arb:     bool = False,
    sort:        str = "start_time",
    date_str:    str = "",
    from_dt:     str = "",
    to_dt:       str = "",
    status_filter: str = "",
) -> tuple[list[dict], int, int]:
    from app.extensions import db
    from app.models.odds_model import (
        UnifiedMatch, BookmakerMatchOdds, ArbitrageOpportunity,
    )
    from app.models.bookmakers_model import Bookmaker, BookmakerMatchLink
    from sqlalchemy import or_, func

    now = datetime.now(timezone.utc)

    q = UnifiedMatch.query
    if sport_slug and sport_slug.lower() not in ("all", ""):
        q = q.filter(UnifiedMatch.sport_name.ilike(f"%{sport_slug}%"))

    # ── Mode filter ───────────────────────────────────────────────────────────
    # Freshly persisted rows have NULL status (update_match_results hasn't run
    # yet).  Treat NULL as "upcoming" — only exclude explicitly terminal states.
    if mode == "upcoming":
        q = q.filter(
            or_(
                UnifiedMatch.status.notin_(["FINISHED", "CANCELLED", "POSTPONED"]),
                UnifiedMatch.status.is_(None),
            )
        )
    elif mode == "live":
        q = q.filter(UnifiedMatch.status.in_(["IN_PLAY", "live", "INPLAY"]))
    elif mode == "finished":
        q = q.filter(UnifiedMatch.status == "FINISHED")
    # mode == "all" → no status filter

    if status_filter and status_filter.upper() not in ("ALL", ""):
        q = q.filter(UnifiedMatch.status == status_filter.upper())

    if comp_filter:
        q = q.filter(UnifiedMatch.competition_name.ilike(f"%{comp_filter}%"))
    if team_filter:
        q = q.filter(or_(
            UnifiedMatch.home_team_name.ilike(f"%{team_filter}%"),
            UnifiedMatch.away_team_name.ilike(f"%{team_filter}%"),
        ))
    if date_str:
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            q   = q.filter(UnifiedMatch.start_time >= day,
                           UnifiedMatch.start_time <  day + timedelta(days=1))
        except ValueError:
            pass
    if from_dt:
        try:
            q = q.filter(UnifiedMatch.start_time >= datetime.fromisoformat(from_dt.replace("Z", "+00:00")))
        except Exception:
            pass
    if to_dt:
        try:
            q = q.filter(UnifiedMatch.start_time <= datetime.fromisoformat(to_dt.replace("Z", "+00:00")))
        except Exception:
            pass

    sort_col = {
        "start_time":  UnifiedMatch.start_time,
        "home_team":   UnifiedMatch.home_team_name,
        "competition": UnifiedMatch.competition_name,
    }.get(sort, UnifiedMatch.start_time)
    q = q.order_by(sort_col.asc())

    total   = q.count()
    um_list = q.offset((page - 1) * per_page).limit(per_page).all()

    if not um_list:
        return [], total, max(1, (total + per_page - 1) // per_page)

    match_ids = [um.id for um in um_list]

    bmo_rows = BookmakerMatchOdds.query.filter(
        BookmakerMatchOdds.match_id.in_(match_ids)
    ).all()

    bk_ids   = {bmo.bookmaker_id for bmo in bmo_rows}
    bk_objs  = {b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(bk_ids)).all()} if bk_ids else {}

    link_rows = BookmakerMatchLink.query.filter(
        BookmakerMatchLink.match_id.in_(match_ids)
    ).all()
    links_by_match: dict[int, dict] = {}
    for lnk in link_rows:
        links_by_match.setdefault(lnk.match_id, {})[lnk.bookmaker_id] = lnk.to_dict()

    arb_set: set[int] = set()
    try:
        arb_set = {
            r.match_id
            for r in ArbitrageOpportunity.query
            .filter(ArbitrageOpportunity.match_id.in_(match_ids),
                    ArbitrageOpportunity.status == "OPEN")
            .with_entities(ArbitrageOpportunity.match_id).all()
        }
    except Exception:
        pass

    bmo_by_match: dict[int, list] = {}
    for bmo in bmo_rows:
        bmo_by_match.setdefault(bmo.match_id, []).append(bmo)

    result: list[dict] = []
    for um in um_list:
        bmos = bmo_by_match.get(um.id, [])

        bookmakers: dict[str, dict] = {}
        markets_by_bk: dict[str, dict] = {}

        for bmo in bmos:
            bk_obj   = bk_objs.get(bmo.bookmaker_id)
            bk_name  = (bk_obj.name if bk_obj else str(bmo.bookmaker_id)).lower()
            slug     = _bk_slug(bk_name)
            label    = bk_obj.name if bk_obj else slug.upper()
            mkt_data = bmo.markets_json or {}

            bookmakers[slug] = {
                "bookmaker_id": bmo.bookmaker_id,
                "bookmaker":    label,
                "slug":         slug,
                "markets":      mkt_data,
                "market_count": len(mkt_data),
                "link":         links_by_match.get(um.id, {}).get(bmo.bookmaker_id),
            }
            markets_by_bk[slug] = mkt_data

        best: dict[str, dict[str, dict]] = {}
        for sl, bk_mkts in markets_by_bk.items():
            for mkt, outcomes in bk_mkts.items():
                best.setdefault(mkt, {})
                for out, odd in (outcomes or {}).items():
                    try:
                        fv = float(odd)
                    except (TypeError, ValueError):
                        continue
                    if fv <= 1.0:
                        continue
                    if out not in best[mkt] or fv > best[mkt][out]["odd"]:
                        best[mkt][out] = {"odd": fv, "bk": sl}

        best_odds: dict[str, dict] = {
            mkt: {out: {"odd": v["odd"], "bookie": v["bk"]} for out, v in outcomes.items()}
            for mkt, outcomes in best.items()
        }

        arb_markets: list[dict] = []
        for mkt, outcomes in best.items():
            if len(outcomes) < 2:
                continue
            arb_sum = sum(1.0 / v["odd"] for v in outcomes.values())
            if arb_sum < 1.0:
                profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
                legs = [{"outcome": out, "bk": v["bk"], "odd": v["odd"]}
                        for out, v in outcomes.items()]
                breakdown = [{
                    **leg,
                    "stake_pct":  round((1.0/leg["odd"]/arb_sum)*100, 3),
                    "stake_kes":  round(1000*(1.0/leg["odd"]/arb_sum), 2),
                    "return_kes": round(1000*(1.0/leg["odd"]/arb_sum)*leg["odd"], 2),
                } for leg in legs]
                arb_markets.append({
                    "market":         mkt,
                    "market_slug":    mkt,
                    "profit_pct":     profit_pct,
                    "arb_sum":        round(arb_sum, 6),
                    "legs":           legs,
                    "breakdown_1000": breakdown,
                })
        arb_markets.sort(key=lambda x: -x["profit_pct"])

        has_arb_flag = bool(arb_markets) or um.id in arb_set
        if has_arb and not has_arb_flag:
            total -= 1
            continue

        all_slugs = sorted({
            slug for bk_mkts in markets_by_bk.values()
            for slug in bk_mkts.keys()
        })

        result.append({
            "match_id":        um.id,
            "parent_match_id": um.parent_match_id,
            "betradar_id":     um.parent_match_id,
            "join_key":        f"br_{um.parent_match_id}" if um.parent_match_id else f"db_{um.id}",
            "home_team":       um.home_team_name  or "",
            "away_team":       um.away_team_name  or "",
            "competition":     um.competition_name or "",
            "sport":           um.sport_name       or sport_slug,
            "start_time":      um.start_time.isoformat() if um.start_time else None,
            "status":          getattr(um, "status", "PRE_MATCH") or "PRE_MATCH",
            "is_live":         (getattr(um, "status", "") or "") in ("IN_PLAY", "live"),
            "bk_count":        len(bookmakers),
            "bookie_count":    len(bookmakers),
            "bookmaker_count": len(bookmakers),
            "market_count":    len(all_slugs),
            "market_slugs":    all_slugs,
            "bookmakers":      bookmakers,
            "markets_by_bk":   markets_by_bk,
            "markets":         markets_by_bk,
            "best":            best,
            "best_odds":       best_odds,
            "aggregated":      um.markets_json or {},
            "has_arb":         has_arb_flag,
            "arb_markets":     arb_markets,
            "arbs":            arb_markets,
            "best_arb_pct":    arb_markets[0]["profit_pct"] if arb_markets else 0.0,
            "has_ev":          False,
            "evs":             [],
            "has_sharp":       False,
            "sharp":           [],
            "best_ev_pct":     0.0,
            "bk_ids": {
                slug: str(bk_data["bookmaker_id"])
                for slug, bk_data in bookmakers.items()
            },
            "source": "postgresql",
        })

    pages = max(1, (total + per_page - 1) // per_page)
    return result, total, pages


def _read_cache_sources(mode: str, sport_slug: str) -> list[dict]:
    from app.workers.celery_tasks import cache_get, cache_keys
    raw: list[dict] = []
    slug = sport_slug.lower().replace(" ", "_").replace("-", "_")
    for prefix in _CACHE_PREFIXES:
        cached = cache_get(f"{prefix}:{mode}:{slug}")
        if cached and cached.get("matches"):
            for m in cached["matches"]:
                m.setdefault("_cache_source", prefix)
            raw.extend(cached["matches"])
    for k in cache_keys(f"odds:{mode}:{slug}:*") + cache_keys(f"odds:{mode}:{slug}"):
        d = cache_get(k)
        if d and d.get("matches"):
            raw.extend(d["matches"])
    return raw


def _deduplicate(matches: list[dict]) -> list[dict]:
    seen: set[str] = set()
    result = []
    for m in matches:
        key = (f"{m.get('home_team','').lower().strip()}"
               f"|{m.get('away_team','').lower().strip()}"
               f"|{str(m.get('start_time',''))[:13]}")
        if key not in seen:
            seen.add(key)
            result.append(m)
    return result


def _normalise_cache_match(m: dict) -> dict:
    home = str(m.get("home_team") or m.get("home_team_name") or "")
    away = str(m.get("away_team") or m.get("away_team_name") or "")
    markets_by_bk: dict[str, dict] = {}
    src      = m.get("_cache_source", "")
    raw_mkts = m.get("markets") or {}
    if src and raw_mkts:
        markets_by_bk[src] = raw_mkts
    elif isinstance(raw_mkts, dict):
        first_val = next(iter(raw_mkts.values()), None)
        if isinstance(first_val, dict):
            inner = next(iter(first_val.values()), None)
            if isinstance(inner, (int, float)):
                markets_by_bk[_bk_slug(src or "sp")] = raw_mkts
            else:
                markets_by_bk = {_bk_slug(bk): v for bk, v in raw_mkts.items()}
    best: dict[str, dict] = {}
    for sl, bk_mkts in markets_by_bk.items():
        for mkt, outcomes in bk_mkts.items():
            best.setdefault(mkt, {})
            for out, odd in (outcomes or {}).items():
                try:
                    fv = float(odd)
                except (TypeError, ValueError):
                    continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]):
                    best[mkt][out] = {"odd": fv, "bk": sl}
    slugs = sorted({s for bk in markets_by_bk.values() for s in bk.keys()})
    return {
        "match_id": None,
        "parent_match_id": m.get("betradar_id") or m.get("parent_match_id") or "",
        "betradar_id": m.get("betradar_id") or "",
        "join_key": (f"br_{m['betradar_id']}" if m.get("betradar_id")
                     else f"fuzzy_{home.lower()[:8]}_{away.lower()[:8]}"),
        "home_team": home, "away_team": away,
        "competition": str(m.get("competition") or m.get("competition_name") or ""),
        "sport": str(m.get("sport") or ""),
        "start_time": m.get("start_time"),
        "status": m.get("status") or "PRE_MATCH",
        "is_live": bool(m.get("is_live")),
        "bk_count": len(markets_by_bk), "bookie_count": len(markets_by_bk),
        "bookmaker_count": len(markets_by_bk), "market_count": len(slugs),
        "market_slugs": slugs,
        "bookmakers": {
            sl: {"bookmaker": sl.upper(), "slug": sl, "markets": mkts,
                 "market_count": len(mkts), "link": None}
            for sl, mkts in markets_by_bk.items()
        },
        "markets_by_bk": markets_by_bk, "markets": markets_by_bk,
        "best": best,
        "best_odds": {
            mkt: {out: {"odd": v["odd"], "bookie": v["bk"]} for out, v in outs.items()}
            for mkt, outs in best.items()
        },
        "has_arb": False, "arb_markets": [], "arbs": [], "best_arb_pct": 0.0,
        "has_ev": False, "evs": [], "has_sharp": False, "sharp": [], "best_ev_pct": 0.0,
        "bk_ids": {sl: sl for sl in markets_by_bk}, "source": "cache",
    }


def _apply_filters(matches: list[dict], args) -> list[dict]:
    bookmaker   = (args.get("bookmaker",   "") or "").strip().lower()
    market      = (args.get("market",      "") or "").strip().lower()
    team        = (args.get("team",        "") or "").strip().lower()
    competition = (args.get("competition", "") or "").strip().lower()
    from_dt = args.get("from_dt")
    to_dt   = args.get("to_dt")
    def _dt(val):
        if not val: return None
        try: return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        except Exception: return None
    fdt = _dt(from_dt); tdt = _dt(to_dt)
    result = []
    for m in matches:
        if bookmaker and not any(bookmaker in k for k in (m.get("bookmakers") or {})): continue
        if market and not any(market in k for k in (m.get("best_odds") or m.get("best") or {})): continue
        if team and team not in m.get("home_team","").lower() and team not in m.get("away_team","").lower(): continue
        if competition and competition not in m.get("competition","").lower(): continue
        if fdt or tdt:
            try:
                st = _dt(m.get("start_time"))
                if st:
                    if fdt and st < fdt: continue
                    if tdt and st > tdt: continue
            except Exception: pass
        result.append(m)
    return result


def _sort_matches(matches: list[dict], sort: str) -> list[dict]:
    def _dt(m):
        try: return datetime.fromisoformat(str(m.get("start_time","")).replace("Z","+00:00"))
        except Exception: return datetime.max.replace(tzinfo=timezone.utc)
    if sort == "arb":          return sorted(matches, key=lambda m: -(m.get("best_arb_pct") or 0))
    if sort == "market_count": return sorted(matches, key=lambda m: -(m.get("market_count") or 0))
    if sort == "bk_count":     return sorted(matches, key=lambda m: -(m.get("bk_count") or 0))
    return sorted(matches, key=_dt)


def _apply_tier_limits(matches, user):
    if FREE_ACCESS: return matches, False
    limits = (user.limits if user else None) or {"max_matches": FREE_MATCH_LIMIT}
    max_m  = limits.get("max_matches") or FREE_MATCH_LIMIT
    if max_m and len(matches) > max_m: return matches[:max_m], True
    return matches, False


def _build_envelope(matches, sport, mode, tier, page, per_page, truncated,
                    latency_ms, total=None, pages=None, extra=None) -> dict:
    arb_count = sum(1 for m in matches if m.get("has_arb"))
    bk_names  = set()
    for m in matches: bk_names.update((m.get("bookmakers") or {}).keys())
    _total = total if total is not None else len(matches)
    _pages = pages if pages is not None else max(1, (_total + per_page - 1) // per_page)
    env = {
        "ok": True, "sport": sport, "mode": mode, "tier": tier,
        "total": _total, "page": page, "per_page": per_page, "pages": _pages,
        "truncated": truncated, "latency_ms": latency_ms,
        "arb_count": arb_count, "bookie_count": len(bk_names),
        "bookmakers": sorted(bk_names), "matches": matches, "source": "postgresql",
    }
    if truncated: env["upgrade_message"] = "Upgrade your plan to see all matches."
    if extra: env.update(extra)
    return env


@bp_odds.route("/odds/sports")
def list_sports():
    from app.models.odds_model import UnifiedMatch
    from sqlalchemy import func
    sports: dict[str, int] = {}
    try:
        for row in (UnifiedMatch.query
                    .with_entities(UnifiedMatch.sport_name, func.count(UnifiedMatch.id))
                    .group_by(UnifiedMatch.sport_name).all()):
            if row[0]: sports[row[0]] = row[1]
    except Exception: pass
    from app.workers.celery_tasks import cache_keys
    for prefix in _CACHE_PREFIXES:
        for k in cache_keys(f"{prefix}:upcoming:*"):
            parts = k.split(":")
            if len(parts) >= 3:
                sp = parts[2].replace("_"," ").title()
                sports.setdefault(sp, 0)
    return _signed_response({"ok": True, "sports": sorted(sports.keys()), "counts": sports})


@bp_odds.route("/odds/bookmakers")
def list_bookmakers():
    from app.models.bookmakers_model import Bookmaker
    from app.models.odds_model import BookmakerMatchOdds
    from sqlalchemy import func
    bk_counts = dict(
        BookmakerMatchOdds.query
        .with_entities(BookmakerMatchOdds.bookmaker_id, func.count(BookmakerMatchOdds.match_id))
        .group_by(BookmakerMatchOdds.bookmaker_id).all()
    )
    result = []
    for bm in Bookmaker.query.filter_by(is_active=True).order_by(Bookmaker.name).all():
        result.append({
            "id": bm.id, "name": bm.name, "slug": _bk_slug(bm.name),
            "domain": bm.domain, "is_active": bm.is_active,
            "match_count": bk_counts.get(bm.id, 0),
            "group": "local" if _bk_slug(bm.name) in ("sp","bt","od") else "international",
        })
    return _signed_response({"ok": True, "bookmakers": result, "total": len(result)})


@bp_odds.route("/odds/markets")
def list_markets():
    try:
        from app.models.odds_model import MarketDefinition
        mkts = MarketDefinition.query.order_by(MarketDefinition.name).all()
        return _signed_response({"ok": True, "markets": [m.to_dict() for m in mkts]})
    except Exception as exc:
        return _err(str(exc), 500)


@bp_odds.route("/odds/upcoming/<sport_slug>")
def get_upcoming(sport_slug: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    page     = max(1,   int(request.args.get("page",     1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    sort     = request.args.get("sort",    "start_time")
    comp_f   = (request.args.get("comp",   "") or "").strip()
    team_f   = (request.args.get("team",   "") or "").strip()
    has_arb  = request.args.get("has_arb", "") in ("1","true")
    date_f   = request.args.get("date",    "")
    from_dt  = request.args.get("from_dt", "")
    to_dt    = request.args.get("to_dt",   "")
    log_event("odds_upcoming", {"sport": sport_slug, "tier": tier})
    try:
        matches, total, pages = _load_db_matches(
            sport_slug, mode="upcoming",
            page=page, per_page=per_page,
            comp_filter=comp_f, team_filter=team_f,
            has_arb=has_arb, sort=sort,
            date_str=date_f, from_dt=from_dt, to_dt=to_dt,
        )
    except Exception:
        raw     = _read_cache_sources("upcoming", sport_slug)
        matches = [_normalise_cache_match(m) for m in _deduplicate(raw)]
        matches = _apply_filters(matches, request.args)
        matches = _sort_matches(matches, sort)
        total   = len(matches)
        pages   = max(1, (total + per_page - 1) // per_page)
        matches = matches[(page-1)*per_page: page*per_page]
    matches, truncated = _apply_tier_limits(matches, user)
    latency = int((time.perf_counter() - t0) * 1000)
    return _signed_response(
        _build_envelope(matches, sport_slug, "upcoming", tier,
                        page, per_page, truncated, latency, total=total, pages=pages),
        encrypt_for=user,
    )


@bp_odds.route("/odds/live/<sport_slug>")
def get_live(sport_slug: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    page     = max(1,   int(request.args.get("page",     1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    sort     = request.args.get("sort", "start_time")
    comp_f   = (request.args.get("comp", "") or "").strip()
    team_f   = (request.args.get("team", "") or "").strip()
    log_event("odds_live", {"sport": sport_slug, "tier": tier})
    try:
        matches, total, pages = _load_db_matches(
            sport_slug, mode="live",
            page=page, per_page=per_page,
            comp_filter=comp_f, team_filter=team_f, sort=sort,
        )
    except Exception:
        raw     = _read_cache_sources("live", sport_slug)
        matches = [_normalise_cache_match(m) for m in _deduplicate(raw)]
        matches = _apply_filters(matches, request.args)
        matches = _sort_matches(matches, sort)
        total   = len(matches)
        pages   = max(1, (total + per_page - 1) // per_page)
        matches = matches[(page-1)*per_page: page*per_page]
    matches, truncated = _apply_tier_limits(matches, user)
    latency = int((time.perf_counter() - t0) * 1000)
    return _signed_response(
        _build_envelope(matches, sport_slug, "live", tier,
                        page, per_page, truncated, latency, total=total, pages=pages),
        encrypt_for=user,
    )


@bp_odds.route("/odds/results")
def get_results():
    date_str = request.args.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    return _get_finished_by_date(date_str)


@bp_odds.route("/odds/results/<date_str>")
def get_results_by_date(date_str: str):
    return _get_finished_by_date(date_str)


def _get_finished_by_date(date_str: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    page     = max(1,   int(request.args.get("page",     1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    sport    = request.args.get("sport", "")
    log_event("finished_games_view", {"date": date_str})
    try:
        matches, total, pages = _load_db_matches(
            sport or "all", mode="finished",
            page=page, per_page=per_page, date_str=date_str,
            comp_filter=(request.args.get("competition") or ""),
            team_filter=(request.args.get("team") or ""),
        )
    except Exception:
        from app.workers.celery_tasks import cache_get
        cached  = cache_get(f"results:finished:{date_str}")
        matches = [_normalise_cache_match(m) for m in (cached or [])]
        total   = len(matches)
        pages   = max(1, (total + per_page - 1) // per_page)
        matches = matches[(page-1)*per_page: page*per_page]
    matches, truncated = _apply_tier_limits(matches, user)
    latency = int((time.perf_counter() - t0) * 1000)
    return _signed_response(
        _build_envelope(matches, date_str, "finished", tier,
                        page, per_page, truncated, latency,
                        total=total, pages=pages, extra={"date": date_str}),
    )


@bp_odds.route("/odds/match/<parent_match_id>")
def get_match(parent_match_id: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    from app.models.odds_model import (UnifiedMatch, BookmakerMatchOdds,
                                        ArbitrageOpportunity, EVOpportunity,
                                        BookmakerOddsHistory)
    from app.models.bookmakers_model import Bookmaker, BookmakerMatchLink
    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um: return _err("Match not found", 404)
    log_event("match_view", {"match_id": parent_match_id, "tier": tier})
    bmos   = BookmakerMatchOdds.query.filter_by(match_id=um.id).all()
    bk_ids = {bmo.bookmaker_id for bmo in bmos}
    bk_map = {b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(bk_ids)).all()} if bk_ids else {}
    links  = {lnk.bookmaker_id: lnk.to_dict()
               for lnk in BookmakerMatchLink.query.filter_by(match_id=um.id).all()}
    bookmakers: dict[str, dict] = {}
    markets_by_bk: dict[str, dict] = {}
    for bmo in bmos:
        bk_obj = bk_map.get(bmo.bookmaker_id)
        sl     = _bk_slug((bk_obj.name if bk_obj else str(bmo.bookmaker_id)).lower())
        mkts   = bmo.markets_json or {}
        bookmakers[sl] = {"bookmaker_id": bmo.bookmaker_id,
                          "bookmaker": bk_obj.name if bk_obj else sl.upper(),
                          "slug": sl, "markets": mkts, "market_count": len(mkts),
                          "link": links.get(bmo.bookmaker_id)}
        markets_by_bk[sl] = mkts
    best: dict[str, dict] = {}
    for sl, bk_mkts in markets_by_bk.items():
        for mkt, outcomes in bk_mkts.items():
            best.setdefault(mkt, {})
            for out, odd in (outcomes or {}).items():
                try: fv = float(odd)
                except Exception: continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]):
                    best[mkt][out] = {"odd": fv, "bk": sl}
    history = (BookmakerOddsHistory.query.filter_by(match_id=um.id)
               .order_by(BookmakerOddsHistory.recorded_at.desc()).limit(50).all())
    history_rows = [{
        "bookmaker":   bk_map[h.bookmaker_id].name if h.bookmaker_id in bk_map else str(h.bookmaker_id),
        "market": h.market, "selection": h.selection,
        "old_price": h.old_price, "new_price": h.new_price, "price_delta": h.price_delta,
        "recorded_at": h.recorded_at.isoformat() if h.recorded_at else None,
    } for h in history]
    try:
        arbs = ArbitrageOpportunity.query.filter_by(match_id=um.id, status="OPEN").all()
        evs  = EVOpportunity.query.filter_by(match_id=um.id, status="OPEN").all()
        arb_list = [a.to_dict() for a in arbs]; ev_list = [e.to_dict() for e in evs]
    except Exception:
        arb_list = ev_list = []
    return _signed_response({
        "ok": True, "match_id": um.id, "parent_match_id": um.parent_match_id,
        "betradar_id": um.parent_match_id, "home_team": um.home_team_name,
        "away_team": um.away_team_name, "competition": um.competition_name,
        "sport": um.sport_name, "start_time": um.start_time.isoformat() if um.start_time else None,
        "status": getattr(um, "status", "PRE_MATCH"),
        "bookmakers": bookmakers, "markets_by_bk": markets_by_bk, "markets": markets_by_bk,
        "best": best, "aggregated": um.markets_json or {},
        "odds_history": history_rows, "arbs": arb_list, "evs": ev_list,
        "bk_ids": {sl: str(bk_data["bookmaker_id"]) for sl, bk_data in bookmakers.items()},
        "latency_ms": int((time.perf_counter() - t0) * 1000), "source": "postgresql",
    }, encrypt_for=user)


@bp_odds.route("/odds/search")
def search_matches():
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    q_str    = (request.args.get("q") or "").strip()
    mode     = request.args.get("mode", "upcoming")
    page     = max(1,   int(request.args.get("page",     1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    sport    = (request.args.get("sport") or "").strip()
    if not q_str: return _err("Provide query param 'q'", 400)
    from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds
    from sqlalchemy import or_, func as sqlfunc
    from app.extensions import db
    qs = UnifiedMatch.query.filter(or_(
        UnifiedMatch.home_team_name.ilike(f"%{q_str}%"),
        UnifiedMatch.away_team_name.ilike(f"%{q_str}%"),
        UnifiedMatch.competition_name.ilike(f"%{q_str}%"),
        UnifiedMatch.parent_match_id.ilike(f"%{q_str}%"),
    ))
    if sport: qs = qs.filter(UnifiedMatch.sport_name.ilike(f"%{sport}%"))
    if mode == "live":     qs = qs.filter(UnifiedMatch.status.in_(["IN_PLAY","live"]))
    elif mode == "finished": qs = qs.filter(UnifiedMatch.status == "FINISHED")
    # upcoming/all — no extra filter needed
    total   = qs.count()
    um_list = qs.order_by(UnifiedMatch.start_time).offset((page-1)*per_page).limit(per_page).all()
    match_ids = [um.id for um in um_list]
    bk_counts = dict(
        db.session.query(BookmakerMatchOdds.match_id, sqlfunc.count(BookmakerMatchOdds.bookmaker_id))
        .filter(BookmakerMatchOdds.match_id.in_(match_ids))
        .group_by(BookmakerMatchOdds.match_id).all()
    ) if match_ids else {}
    results = [{
        "match_id": um.id, "parent_match_id": um.parent_match_id,
        "betradar_id": um.parent_match_id,
        "join_key": f"br_{um.parent_match_id}" if um.parent_match_id else f"db_{um.id}",
        "home_team": um.home_team_name, "away_team": um.away_team_name,
        "competition": um.competition_name, "sport": um.sport_name,
        "start_time": um.start_time.isoformat() if um.start_time else None,
        "status": getattr(um, "status", "PRE_MATCH"),
        "bookie_count": bk_counts.get(um.id, 0),
        "detail_url": f"/api/odds/match/{um.parent_match_id}",
    } for um in um_list]
    log_event("odds_search", {"q": q_str, "mode": mode, "total": total})
    return _signed_response({
        "ok": True, "q": q_str, "mode": mode, "tier": tier,
        "total": total, "page": page, "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
        "matches": results, "source": "postgresql",
    }, encrypt_for=user)


@bp_odds.route("/odds/status")
def harvest_status():
    from app.workers.celery_tasks import cache_get
    heartbeat = cache_get("worker_heartbeat") or {}
    try:
        from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds
        from app.models.bookmakers_model import Bookmaker
        from sqlalchemy import func
        from app.extensions import db
        total_db = UnifiedMatch.query.count()
        bk_cov   = dict(
            db.session.query(BookmakerMatchOdds.bookmaker_id, func.count(BookmakerMatchOdds.match_id))
            .group_by(BookmakerMatchOdds.bookmaker_id).all()
        )
        bk_names = {b.id: b.name for b in Bookmaker.query.all()}
        coverage = [
            {"bookmaker": bk_names.get(bk_id, str(bk_id)), "match_count": cnt,
             "coverage_pct": round(cnt/total_db*100, 1) if total_db else 0}
            for bk_id, cnt in bk_cov.items()
        ]
    except Exception:
        total_db = 0; coverage = []
    return _signed_response({
        "ok": True, "free_access": FREE_ACCESS,
        "worker_alive": heartbeat.get("alive", False),
        "last_heartbeat": heartbeat.get("checked_at"),
        "db_match_count": total_db, "bookmaker_coverage": coverage, "source": "postgresql",
    })


@bp_odds.route("/odds/access")
def get_access_config():
    user = _current_user_from_header()
    if not user or not getattr(user, "is_admin", False): return _err("Admin only", 403)
    return _signed_response({"ok": True, "free_access": FREE_ACCESS, "endpoint_access": _ENDPOINT_ACCESS})


@bp_odds.route("/admin/odds/access", methods=["POST"])
def set_access_config():
    global FREE_ACCESS
    user = _current_user_from_header()
    if not user or not getattr(user, "is_admin", False): return _err("Admin only", 403)
    body = request.get_json(force=True) or {}
    if "free_access" in body: FREE_ACCESS = bool(body["free_access"])
    if isinstance(body.get("endpoint_access"), dict):
        valid = {"free","basic","pro","premium"}
        for ep, tier in body["endpoint_access"].items():
            if ep in _ENDPOINT_ACCESS and tier in valid: _ENDPOINT_ACCESS[ep] = tier
    return _signed_response({"ok": True, "free_access": FREE_ACCESS, "endpoint_access": _ENDPOINT_ACCESS})


def _sse_stream(channel: str):
    import redis as _rl
    from app.workers.celery_tasks import celery as _celery
    url  = _celery.conf.broker_url or "redis://localhost:6379/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    r    = _rl.Redis.from_url(f"{base}/2", decode_responses=True)
    ps   = r.pubsub(); ps.subscribe(channel)
    last_hb = datetime.now(timezone.utc)
    try:
        for msg in ps.listen():
            now = datetime.now(timezone.utc)
            if msg["type"] == "message": yield f"data: {msg['data']}\n\n"
            if (now - last_hb).seconds >= 20: yield ": ping\n\n"; last_hb = now
    except (GeneratorExit, Exception): ps.unsubscribe(channel)


_SSE_HEADERS = {"Cache-Control":"no-cache","X-Accel-Buffering":"no","Access-Control-Allow-Origin":"*"}


@bp_odds.route("/stream/odds")
def stream_odds():
    return Response(stream_with_context(_sse_stream(_WS_CHANNEL)), mimetype="text/event-stream", headers=_SSE_HEADERS)


@bp_odds.route("/stream/arb")
def stream_arb():
    return Response(stream_with_context(_sse_stream(_ARB_CHANNEL)), mimetype="text/event-stream", headers=_SSE_HEADERS)


@bp_odds.route("/stream/ev")
def stream_ev():
    return Response(stream_with_context(_sse_stream(_EV_CHANNEL)), mimetype="text/event-stream", headers=_SSE_HEADERS)