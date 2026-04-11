import time
from datetime import timedelta, timezone
from flask import request
from . import bp_odds_customer
from .utils import _apply_tier_limits, _normalise_sport_slug, _bk_slug, _flatten_db_markets, _effective_status, _is_live, _now_utc
from .formatters import _build_envelope, _build_analytics_full, _build_analytics_summary
from .db_services import _load_db_matches, _get_analytics, _build_base_query, _sport_filter, _mode_time_filter, _multi_bk_filter
from .cache_services import _read_cache_sources, _deduplicate, _normalise_cache_match
from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
from app.utils.decorators_ import log_event

@bp_odds_customer.route("/odds/upcoming/<sport_slug>")
def get_upcoming(sport_slug: str):
    t0       = time.perf_counter()
    user     = _current_user_from_header()
    tier     = getattr(user, "tier", "free") if user else "free"
    page     = max(1,   int(request.args.get("page",     1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    sort     = request.args.get("sort",      "start_time")
    comp_f   = (request.args.get("comp",     "") or "").strip()
    team_f   = (request.args.get("team",     "") or "").strip()
    has_arb  = request.args.get("has_arb",   "") in ("1", "true")
    date_f   = request.args.get("date",      "")
    from_dt  = request.args.get("from_dt",   "")
    to_dt    = request.args.get("to_dt",     "")
    analytics = request.args.get("analytics","") in ("1", "true")
    log_event("odds_upcoming", {"sport": sport_slug, "tier": tier})

    try:
        matches, total, pages = _load_db_matches(
            sport_slug, mode="upcoming", page=page, per_page=per_page,
            comp_filter=comp_f, team_filter=team_f, has_arb=has_arb,
            sort=sort, date_str=date_f, from_dt=from_dt, to_dt=to_dt, include_analytics=analytics,
        )
    except Exception:
        raw = _read_cache_sources("upcoming", sport_slug)
        matches = [x for m in _deduplicate(raw) if (x := _normalise_cache_match(m, "upcoming")) is not None]
        total = len(matches); pages = max(1, (total + per_page - 1) // per_page)
        matches = matches[(page-1)*per_page: page*per_page]

    matches, truncated = _apply_tier_limits(matches, user)
    return _signed_response(_build_envelope(matches, sport_slug, "upcoming", tier, page, per_page, truncated, int((time.perf_counter() - t0) * 1000), total=total, pages=pages), encrypt_for=user)

@bp_odds_customer.route("/odds/live/<sport_slug>")
def get_live(sport_slug: str):
    t0       = time.perf_counter()
    user     = _current_user_from_header()
    tier     = getattr(user, "tier", "free") if user else "free"
    page     = max(1,   int(request.args.get("page",     1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    sort     = request.args.get("sort",      "start_time")
    comp_f   = (request.args.get("comp",     "") or "").strip()
    team_f   = (request.args.get("team",     "") or "").strip()
    analytics = request.args.get("analytics","") in ("1", "true")
    log_event("odds_live", {"sport": sport_slug, "tier": tier})

    try:
        matches, total, pages = _load_db_matches(sport_slug, mode="live", page=page, per_page=per_page, comp_filter=comp_f, team_filter=team_f, sort=sort, include_analytics=analytics)
    except Exception:
        raw = _read_cache_sources("live", sport_slug)
        matches = [x for m in _deduplicate(raw) if (x := _normalise_cache_match(m, "live")) is not None]
        total = len(matches); pages = max(1, (total + per_page - 1) // per_page)
        matches = matches[(page-1)*per_page: page*per_page]

    matches, truncated = _apply_tier_limits(matches, user)
    return _signed_response(_build_envelope(matches, sport_slug, "live", tier, page, per_page, truncated, int((time.perf_counter() - t0) * 1000), total=total, pages=pages), encrypt_for=user)

@bp_odds_customer.route("/odds/results")
@bp_odds_customer.route("/odds/results/<date_str>")
def get_results_by_date(date_str: str = ""):
    if not date_str: date_str = _now_utc().strftime("%Y-%m-%d")
    t0       = time.perf_counter()
    user     = _current_user_from_header()
    tier     = getattr(user, "tier", "free") if user else "free"
    page     = max(1,   int(request.args.get("page",     1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    sport    = request.args.get("sport", "")
    log_event("finished_games_view", {"date": date_str})
    try:
        matches, total, pages = _load_db_matches(sport or "all", mode="finished", page=page, per_page=per_page, date_str=date_str, comp_filter=(request.args.get("competition") or ""), team_filter=(request.args.get("team") or ""))
    except Exception:
        from app.workers.celery_tasks import cache_get
        cached  = cache_get(f"results:finished:{date_str}")
        matches = [x for m in (cached or []) if (x := _normalise_cache_match(m, "finished")) is not None]
        total = len(matches); pages = max(1, (total + per_page - 1) // per_page)
        matches = matches[(page-1)*per_page: page*per_page]
    matches, truncated = _apply_tier_limits(matches, user)
    return _signed_response(_build_envelope(matches, date_str, "finished", tier, page, per_page, truncated, int((time.perf_counter() - t0) * 1000), total=total, pages=pages, extra={"date": date_str}))

class _SiblingBMO:
    def __init__(self, bmo, primary_match_id):
        self._bmo = bmo
        self._primary_match_id = primary_match_id
    def __getattr__(self, name): return getattr(self._bmo, name)

@bp_odds_customer.route("/odds/match/<parent_match_id>")
def get_match(parent_match_id: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds, ArbitrageOpportunity, EVOpportunity, BookmakerOddsHistory
    from app.models.bookmakers_model import Bookmaker, BookmakerMatchLink
    from sqlalchemy import and_, func

    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um: return _err("Match not found", 404)
    log_event("match_view", {"match_id": parent_match_id, "tier": tier})

    bmos = list(BookmakerMatchOdds.query.filter_by(match_id=um.id).all())
    try:
        if um.home_team_name and um.away_team_name:
            conds = [func.lower(UnifiedMatch.home_team_name) == um.home_team_name.lower().strip(), func.lower(UnifiedMatch.away_team_name) == um.away_team_name.lower().strip(), UnifiedMatch.id != um.id]
            if um.start_time:
                w = timedelta(minutes=90)
                conds += [UnifiedMatch.start_time >= um.start_time - w, UnifiedMatch.start_time <= um.start_time + w]
            for sib in UnifiedMatch.query.filter(and_(*conds)).all():
                for b in BookmakerMatchOdds.query.filter_by(match_id=sib.id).all():
                    bmos.append(_SiblingBMO(b, um.id))
    except Exception: pass

    bk_ids = {bmo.bookmaker_id for bmo in bmos}
    bk_map = ({b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(bk_ids)).all()} if bk_ids else {})
    links  = {lnk.bookmaker_id: lnk.to_dict() for lnk in BookmakerMatchLink.query.filter_by(match_id=um.id).all()}

    bookmakers = {}; markets_by_bk = {}
    for bmo in bmos:
        bk_obj = bk_map.get(bmo.bookmaker_id)
        sl     = _bk_slug((bk_obj.name if bk_obj else str(bmo.bookmaker_id)).lower())
        mkts   = _flatten_db_markets(bmo.markets_json or {})
        if not mkts: continue
        if sl in bookmakers:
            bookmakers[sl]["markets"].update(mkts); markets_by_bk[sl].update(mkts)
        else:
            bookmakers[sl] = {"bookmaker_id": bmo.bookmaker_id, "bookmaker": bk_obj.name if bk_obj else sl.upper(), "slug": sl, "markets": mkts, "market_count": len(mkts), "link": links.get(bmo.bookmaker_id)}
            markets_by_bk[sl] = mkts

    best = {}
    for sl, bk_mkts in markets_by_bk.items():
        for mkt, outcomes in bk_mkts.items():
            best.setdefault(mkt, {})
            for out, odd_data in (outcomes or {}).items():
                try: fv = (float(odd_data.get("price") or odd_data.get("odd") or 0) if isinstance(odd_data, dict) else float(odd_data))
                except Exception: continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]):
                    best[mkt][out] = {"odd": fv, "bk": sl}

    status_out      = _effective_status(getattr(um, "status", None), um.start_time)
    live_flag       = status_out == "IN_PLAY"
    minutes_elapsed = None
    if um.start_time and live_flag:
        st = um.start_time if um.start_time.tzinfo else um.start_time.replace(tzinfo=timezone.utc)
        minutes_elapsed = int((_now_utc() - st).total_seconds() / 60)

    history = (BookmakerOddsHistory.query.filter_by(match_id=um.id).order_by(BookmakerOddsHistory.recorded_at.desc()).limit(50).all())
    history_rows = [{"bookmaker": bk_map[h.bookmaker_id].name if h.bookmaker_id in bk_map else str(h.bookmaker_id), "market": h.market, "selection": h.selection, "old_price": h.old_price, "new_price": h.new_price, "price_delta": h.price_delta, "recorded_at": h.recorded_at.isoformat() if h.recorded_at else None} for h in history]
    
    try:
        arb_list = [a.to_dict() for a in ArbitrageOpportunity.query.filter_by(match_id=um.id, status="OPEN").all()]
        ev_list  = [e.to_dict() for e in EVOpportunity.query.filter_by(match_id=um.id, status="OPEN").all()]
    except Exception:
        arb_list = ev_list = []

    br_id = um.parent_match_id or ""
    analytics_bundle  = _get_analytics(br_id, trigger_if_missing=True)
    analytics_full    = _build_analytics_full(analytics_bundle)
    analytics_summary = _build_analytics_summary(analytics_bundle)

    return _signed_response({
        "ok": True, "match_id": um.id, "parent_match_id": br_id, "betradar_id": br_id, "join_key": f"br_{br_id}" if br_id else f"db_{um.id}",
        "home_team": um.home_team_name, "away_team": um.away_team_name, "competition": um.competition_name, "sport": _normalise_sport_slug(um.sport_name or ""),
        "start_time": um.start_time.isoformat() if um.start_time else None, "status": status_out, "is_live": live_flag, "minutes_elapsed": minutes_elapsed,
        "bookmakers": bookmakers, "markets_by_bk": markets_by_bk, "markets": markets_by_bk, "best": best, "aggregated": _flatten_db_markets(um.markets_json or {}),
        "odds_history": history_rows, "arbs": arb_list, "evs": ev_list, "bk_ids": {sl: str(d["bookmaker_id"]) for sl, d in bookmakers.items()},
        "has_analytics": analytics_full.get("available", False), "analytics": analytics_full, "analytics_summary": analytics_summary,
        "latency_ms": int((time.perf_counter() - t0) * 1000), "server_time": _now_utc().isoformat(), "source": "postgresql",
    }, encrypt_for=user)

@bp_odds_customer.route("/odds/match/<parent_match_id>/markets")
def get_match_full_markets(parent_match_id: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds
    from app.models.bookmakers_model import Bookmaker, BookmakerMatchLink

    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um: return _err("Match not found", 404)

    bmo_rows  = BookmakerMatchOdds.query.filter_by(match_id=um.id).all()
    all_bk_ids = {bmo.bookmaker_id for bmo in bmo_rows}
    bk_map    = ({b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(all_bk_ids)).all()} if all_bk_ids else {})

    stored_markets = {}
    for bmo in bmo_rows:
        bk_obj = bk_map.get(bmo.bookmaker_id)
        if not bk_obj: continue
        slug = _bk_slug(bk_obj.name.lower())
        stored_markets[slug] = _flatten_db_markets(bmo.markets_json or {})

    fresh_bt = {}; fetch_errors = {}
    bt_external_id = parent_match_id
    try:
        from app.workers.bt_harvester import get_full_markets
        sport_slug = _normalise_sport_slug(um.sport_name or "soccer")
        fresh_bt = get_full_markets(bt_external_id, sport_slug=sport_slug)
    except Exception as exc:
        fetch_errors["bt"] = str(exc)

    combined = dict(stored_markets)
    if fresh_bt: combined["bt"] = {**(stored_markets.get("bt") or {}), **fresh_bt}

    best = {}
    for sl, mkts in combined.items():
        for mkt, outcomes in (mkts or {}).items():
            best.setdefault(mkt, {})
            for out, odd_data in (outcomes or {}).items():
                try: fv = (float(odd_data.get("price") or odd_data.get("odd") or 0) if isinstance(odd_data, dict) else float(odd_data))
                except Exception: continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]):
                    best[mkt][out] = {"odd": fv, "bk": sl}

    arb_markets = []
    if len(combined) >= 2:
        for mkt, outcomes in best.items():
            if len(outcomes) < 2: continue
            arb_sum = sum(1.0 / v["odd"] for v in outcomes.values())
            if arb_sum < 1.0:
                profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
                legs = [{"outcome": o, "bk": v["bk"], "odd": v["odd"]} for o, v in outcomes.items()]
                arb_markets.append({"market": mkt, "profit_pct": profit_pct, "arb_sum": round(arb_sum, 6), "legs": legs})
        arb_markets.sort(key=lambda x: -x["profit_pct"])

    status_out = _effective_status(getattr(um, "status", None), um.start_time)
    analytics_bundle  = _get_analytics(parent_match_id, trigger_if_missing=True)
    analytics_summary = _build_analytics_summary(analytics_bundle)

    return _signed_response({
        "ok": True, "match_id": um.id, "parent_match_id": parent_match_id, "home_team": um.home_team_name, "away_team": um.away_team_name, "competition": um.competition_name,
        "sport": _normalise_sport_slug(um.sport_name or ""), "start_time": um.start_time.isoformat() if um.start_time else None, "status": status_out, "is_live": status_out == "IN_PLAY",
        "markets_by_bk": combined, "best": best, "arb_markets": arb_markets, "has_arb": bool(arb_markets), "market_count": len(best), "market_slugs": sorted(best.keys()),
        "bk_market_counts": {sl: len(mkts) for sl, mkts in combined.items()}, "bookmakers": sorted(combined.keys()), "bt_live_fetched": bool(fresh_bt), "bt_external_id": bt_external_id,
        "stored_bookmakers": sorted(stored_markets.keys()), "fetch_errors": fetch_errors, "has_analytics": analytics_summary.get("available", False), "analytics": analytics_summary,
        "latency_ms": int((time.perf_counter() - t0) * 1000), "server_time": _now_utc().isoformat(),
    }, encrypt_for=user)

@bp_odds_customer.route("/odds/match/<parent_match_id>/analytics")
def get_match_analytics(parent_match_id: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    from app.models.odds_model import UnifiedMatch
    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um: return _err("Match not found", 404)
    log_event("match_analytics_view", {"match_id": parent_match_id})

    bundle    = _get_analytics(parent_match_id, trigger_if_missing=True)
    analytics = _build_analytics_full(bundle)
    available = analytics.get("available", False)
    
    return _signed_response({
        "ok": True, "match_id": um.id, "parent_match_id": parent_match_id, "betradar_id": parent_match_id, "home_team": um.home_team_name, "away_team": um.away_team_name,
        "competition": um.competition_name, "sport": _normalise_sport_slug(um.sport_name or ""), "start_time": um.start_time.isoformat() if um.start_time else None,
        "available": available, "fetching": not available, "analytics": analytics, "latency_ms": int((time.perf_counter() - t0) * 1000), "server_time": _now_utc().isoformat(),
    }, encrypt_for=user)

@bp_odds_customer.route("/odds/match/<parent_match_id>/analytics/refresh", methods=["POST"])
def refresh_match_analytics(parent_match_id: str):
    from app.models.odds_model import UnifiedMatch
    from app.workers.celery_tasks import celery
    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um: return _err("Match not found", 404)
    try:
        celery.send_task("tasks.sp.get_match_analytics", args=[parent_match_id, True], queue="harvest", countdown=0)
        dispatched = True
    except Exception:
        dispatched = False
    return _signed_response({"ok": dispatched, "parent_match_id": parent_match_id, "dispatched": dispatched, "message": "Analytics refresh queued.", "server_time": _now_utc().isoformat()})

@bp_odds_customer.route("/odds/search")
def search_matches():
    t0       = time.perf_counter()
    user     = _current_user_from_header()
    tier     = getattr(user, "tier", "free") if user else "free"
    q_str    = (request.args.get("q") or "").strip()
    mode     = request.args.get("mode", "upcoming")
    page     = max(1,   int(request.args.get("page",     1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    sport    = (request.args.get("sport") or "").strip()
    if not q_str: return _err("Provide query param 'q'", 400)

    from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds
    from sqlalchemy import or_, func as sqlfunc
    from app.extensions import db

    qs = UnifiedMatch.query.filter(or_(UnifiedMatch.home_team_name.ilike(f"%{q_str}%"), UnifiedMatch.away_team_name.ilike(f"%{q_str}%"), UnifiedMatch.competition_name.ilike(f"%{q_str}%"), UnifiedMatch.parent_match_id.ilike(f"%{q_str}%")))
    if sport: qs = _sport_filter(qs, sport)
    qs = _mode_time_filter(qs, mode)
    if mode in ("upcoming", "live"): qs = _multi_bk_filter(qs)
    
    total   = qs.count()
    um_list = qs.order_by(UnifiedMatch.start_time).offset((page-1)*per_page).limit(per_page).all()
    match_ids = [um.id for um in um_list]
    bk_counts = dict(db.session.query(BookmakerMatchOdds.match_id, sqlfunc.count(BookmakerMatchOdds.bookmaker_id)).filter(BookmakerMatchOdds.match_id.in_(match_ids)).group_by(BookmakerMatchOdds.match_id).all()) if match_ids else {}

    results = [{"match_id": um.id, "parent_match_id": um.parent_match_id, "betradar_id": um.parent_match_id, "join_key": f"br_{um.parent_match_id}" if um.parent_match_id else f"db_{um.id}", "home_team": um.home_team_name, "away_team": um.away_team_name, "competition": um.competition_name, "sport": _normalise_sport_slug(um.sport_name or ""), "start_time": um.start_time.isoformat() if um.start_time else None, "status": _effective_status(getattr(um, "status", None), um.start_time), "is_live": _is_live(getattr(um, "status", None), um.start_time), "bookie_count": bk_counts.get(um.id, 0), "detail_url": f"/api/odds/match/{um.parent_match_id}", "analytics_url": f"/api/odds/match/{um.parent_match_id}/analytics"} for um in um_list]
    log_event("odds_search", {"q": q_str, "mode": mode, "total": total})
    return _signed_response({"ok": True, "q": q_str, "mode": mode, "tier": tier, "total": total, "page": page, "per_page": per_page, "pages": max(1, (total + per_page - 1) // per_page), "latency_ms": int((time.perf_counter() - t0) * 1000), "matches": results, "source": "postgresql"}, encrypt_for=user)