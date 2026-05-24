import io
import time
from flask import request
from .blueprint import bp_odds_customer
from .utils import _apply_tier_limits, _now_utc, _normalise_sport_slug, _bk_slug, _flatten_db_markets, _effective_status
from .formatters import _build_envelope, _build_analytics_full, _build_analytics_summary
from .db_services import _load_db_matches, _get_analytics, _build_base_query, _sport_filter, _mode_time_filter, _multi_bk_filter
from .cache_services import _read_cache_sources, _deduplicate, _normalise_cache_match

@bp_odds_customer.route("/odds/upcoming/<sport_slug>")
def get_upcoming(sport_slug: str):
    from app.utils.customer_jwt_helpers import _current_user_from_header, _signed_response
    from app.utils.decorators_ import log_event
    t0 = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    
    log_event("odds_upcoming", {"sport": sport_slug, "tier": tier})

    try:
        matches, total, pages = _load_db_matches(
            sport_slug, mode="upcoming", page=page, per_page=per_page,
            comp_filter=(request.args.get("comp", "") or "").strip(), team_filter=(request.args.get("team", "") or "").strip(),
            has_arb=request.args.get("has_arb", "") in ("1", "true"), sort=request.args.get("sort", "start_time"),
            date_str=request.args.get("date", ""), from_dt=request.args.get("from_dt", ""), to_dt=request.args.get("to_dt", ""),
            include_analytics=request.args.get("analytics","") in ("1", "true"),
        )
    except Exception:
        matches = [x for m in _deduplicate(_read_cache_sources("upcoming", sport_slug)) if (x := _normalise_cache_match(m, "upcoming")) is not None]
        total = len(matches); pages = max(1, (total + per_page - 1) // per_page)
        matches = matches[(page-1)*per_page: page*per_page]

    matches, truncated = _apply_tier_limits(matches, user)
    return _signed_response(_build_envelope(matches, sport_slug, "upcoming", tier, page, per_page, truncated, int((time.perf_counter() - t0) * 1000), total=total, pages=pages), encrypt_for=user)

@bp_odds_customer.route("/odds/_customer/live/<sport_slug>")
def get_live(sport_slug: str):
    from app.utils.customer_jwt_helpers import _current_user_from_header, _signed_response
    from app.utils.decorators_ import log_event
    t0 = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    
    log_event("odds_live", {"sport": sport_slug, "tier": tier})

    try:
        matches, total, pages = _load_db_matches(sport_slug, mode="live", page=page, per_page=per_page, comp_filter=(request.args.get("comp", "") or "").strip(), team_filter=(request.args.get("team", "") or "").strip(), sort=request.args.get("sort", "start_time"), include_analytics=request.args.get("analytics","") in ("1", "true"))
    except Exception:
        matches = [x for m in _deduplicate(_read_cache_sources("live", sport_slug)) if (x := _normalise_cache_match(m, "live")) is not None]
        total = len(matches); pages = max(1, (total + per_page - 1) // per_page)
        matches = matches[(page-1)*per_page: page*per_page]

    matches, truncated = _apply_tier_limits(matches, user)
    return _signed_response(_build_envelope(matches, sport_slug, "live", tier, page, per_page, truncated, int((time.perf_counter() - t0) * 1000), total=total, pages=pages), encrypt_for=user)

@bp_odds_customer.route("/odds/results")
@bp_odds_customer.route("/odds/results/<date_str>")
def get_results_by_date(date_str: str = ""):
    from app.utils.customer_jwt_helpers import _current_user_from_header, _signed_response
    from app.utils.decorators_ import log_event
    from app.workers.celery_tasks import cache_get
    if not date_str: date_str = _now_utc().strftime("%Y-%m-%d")
    t0 = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    
    log_event("finished_games_view", {"date": date_str})
    try: matches, total, pages = _load_db_matches(request.args.get("sport", "") or "all", mode="finished", page=page, per_page=per_page, date_str=date_str, comp_filter=(request.args.get("competition") or ""), team_filter=(request.args.get("team") or ""))
    except Exception:
        matches = [x for m in (cache_get(f"results:finished:{date_str}") or []) if (x := _normalise_cache_match(m, "finished")) is not None]
        total = len(matches); pages = max(1, (total + per_page - 1) // per_page)
        matches = matches[(page-1)*per_page: page*per_page]
        
    matches, truncated = _apply_tier_limits(matches, user)
    return _signed_response(_build_envelope(matches, date_str, "finished", tier, page, per_page, truncated, int((time.perf_counter() - t0) * 1000), total=total, pages=pages, extra={"date": date_str}), encrypt_for=user)

@bp_odds_customer.route("/odds/match/<parent_match_id>")
def get_match(parent_match_id: str):
    from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
    from app.utils.decorators_ import log_event
    from app.models.odds import UnifiedMatch, BookmakerMatchOdds, ArbitrageOpportunity, EVOpportunity, BookmakerOddsHistory
    from app.models.bookmakers_model import Bookmaker, BookmakerMatchLink
    from sqlalchemy import and_, func
    from datetime import timedelta, timezone

    t0 = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"

    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um: return _err("Match not found", 404)
    log_event("match_view", {"match_id": parent_match_id, "tier": tier})

    class _SiblingBMO:
        def __init__(self, bmo): self._bmo = bmo
        def __getattr__(self, name): return getattr(self._bmo, name)

    bmos = list(BookmakerMatchOdds.query.filter_by(match_id=um.id).all())
    try:
        if um.home_team_name and um.away_team_name:
            conds = [func.lower(UnifiedMatch.home_team_name) == um.home_team_name.lower().strip(), func.lower(UnifiedMatch.away_team_name) == um.away_team_name.lower().strip(), UnifiedMatch.id != um.id]
            if um.start_time: conds += [UnifiedMatch.start_time >= um.start_time - timedelta(minutes=90), UnifiedMatch.start_time <= um.start_time + timedelta(minutes=90)]
            for sib in UnifiedMatch.query.filter(and_(*conds)).all():
                for b in BookmakerMatchOdds.query.filter_by(match_id=sib.id).all(): bmos.append(_SiblingBMO(b))
    except Exception: pass

    bk_ids = {bmo.bookmaker_id for bmo in bmos}
    bk_map = ({b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(bk_ids)).all()} if bk_ids else {})
    links = {lnk.bookmaker_id: lnk.to_dict() for lnk in BookmakerMatchLink.query.filter_by(match_id=um.id).all()}

    bookmakers = {}; markets_by_bk = {}
    for bmo in bmos:
        bk_obj = bk_map.get(bmo.bookmaker_id)
        sl = _bk_slug((bk_obj.name if bk_obj else str(bmo.bookmaker_id)).lower())
        mkts = _flatten_db_markets(bmo.markets_json or {})
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
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]): best[mkt][out] = {"odd": fv, "bk": sl}

    status_out = _effective_status(getattr(um, "status", None), um.start_time)
    minutes_elapsed = int((_now_utc() - (um.start_time if um.start_time.tzinfo else um.start_time.replace(tzinfo=timezone.utc))).total_seconds() / 60) if um.start_time and status_out == "IN_PLAY" else None

    history_rows = [{"bookmaker": bk_map[h.bookmaker_id].name if h.bookmaker_id in bk_map else str(h.bookmaker_id), "market": h.market, "selection": h.selection, "old_price": h.old_price, "new_price": h.new_price, "price_delta": h.price_delta, "recorded_at": h.recorded_at.isoformat() if h.recorded_at else None} for h in BookmakerOddsHistory.query.filter_by(match_id=um.id).order_by(BookmakerOddsHistory.recorded_at.desc()).limit(50).all()]
    
    try:
        arb_list = [a.to_dict() for a in ArbitrageOpportunity.query.filter_by(match_id=um.id, status="OPEN").all()]
        ev_list  = [e.to_dict() for e in EVOpportunity.query.filter_by(match_id=um.id, status="OPEN").all()]
    except Exception: arb_list = ev_list = []

    br_id = um.parent_match_id or ""
    analytics_bundle  = _get_analytics(br_id, trigger_if_missing=True)

    return _signed_response({
        "ok": True, "match_id": um.id, "parent_match_id": br_id, "betradar_id": br_id, "join_key": f"br_{br_id}" if br_id else f"db_{um.id}",
        "home_team": um.home_team_name, "away_team": um.away_team_name, "competition": um.competition_name, "sport": _normalise_sport_slug(um.sport_name or ""),
        "start_time": um.start_time.isoformat() if um.start_time else None, "status": status_out, "is_live": status_out == "IN_PLAY", "minutes_elapsed": minutes_elapsed,
        "bookmakers": bookmakers, "markets_by_bk": markets_by_bk, "markets": markets_by_bk, "best": best, "aggregated": _flatten_db_markets(um.markets_json or {}),
        "odds_history": history_rows, "arbs": arb_list, "evs": ev_list, "bk_ids": {sl: str(d["bookmaker_id"]) for sl, d in bookmakers.items()},
        "has_analytics": _build_analytics_full(analytics_bundle).get("available", False), "analytics": _build_analytics_full(analytics_bundle), "analytics_summary": _build_analytics_summary(analytics_bundle),
        "latency_ms": int((time.perf_counter() - t0) * 1000), "server_time": _now_utc().isoformat(), "source": "postgresql",
    }, encrypt_for=user)

@bp_odds_customer.route("/odds/match/<parent_match_id>/markets")
def get_match_full_markets(parent_match_id: str):
    from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
    from app.models.odds import UnifiedMatch, BookmakerMatchOdds
    from app.models.bookmakers_model import Bookmaker
    t0 = time.perf_counter()
    user = _current_user_from_header()

    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um: return _err("Match not found", 404)

    bmo_rows = BookmakerMatchOdds.query.filter_by(match_id=um.id).all()
    all_bk_ids = {bmo.bookmaker_id for bmo in bmo_rows}
    bk_map = ({b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(all_bk_ids)).all()} if all_bk_ids else {})

    stored_markets = {}
    for bmo in bmo_rows:
        if bk_obj := bk_map.get(bmo.bookmaker_id): stored_markets[_bk_slug(bk_obj.name.lower())] = _flatten_db_markets(bmo.markets_json or {})

    fresh_bt = {}; fetch_errors = {}
    try:
        from app.workers.bt_harvester import get_full_markets
        fresh_bt = get_full_markets(parent_match_id, sport_slug=_normalise_sport_slug(um.sport_name or "soccer"))
    except Exception as exc: fetch_errors["bt"] = str(exc)

    combined = dict(stored_markets)
    if fresh_bt: combined["bt"] = {**(stored_markets.get("bt") or {}), **fresh_bt}

    best = {}
    for sl, mkts in combined.items():
        for mkt, outcomes in (mkts or {}).items():
            best.setdefault(mkt, {})
            for out, odd_data in (outcomes or {}).items():
                try: fv = (float(odd_data.get("price") or odd_data.get("odd") or 0) if isinstance(odd_data, dict) else float(odd_data))
                except Exception: continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]): best[mkt][out] = {"odd": fv, "bk": sl}

    arb_markets = []
    if len(combined) >= 2:
        for mkt, outcomes in best.items():
            if len(outcomes) < 2: continue
            arb_sum = sum(1.0 / v["odd"] for v in outcomes.values())
            if arb_sum < 1.0:
                arb_markets.append({"market": mkt, "profit_pct": round((1.0 / arb_sum - 1.0) * 100, 4), "arb_sum": round(arb_sum, 6), "legs": [{"outcome": o, "bk": v["bk"], "odd": v["odd"]} for o, v in outcomes.items()]})
        arb_markets.sort(key=lambda x: -x["profit_pct"])

    status_out = _effective_status(getattr(um, "status", None), um.start_time)
    analytics_summary = _build_analytics_summary(_get_analytics(parent_match_id, trigger_if_missing=True))

    return _signed_response({
        "ok": True, "match_id": um.id, "parent_match_id": parent_match_id, "home_team": um.home_team_name, "away_team": um.away_team_name, "competition": um.competition_name,
        "sport": _normalise_sport_slug(um.sport_name or ""), "start_time": um.start_time.isoformat() if um.start_time else None, "status": status_out, "is_live": status_out == "IN_PLAY",
        "markets_by_bk": combined, "best": best, "arb_markets": arb_markets, "has_arb": bool(arb_markets), "market_count": len(best), "market_slugs": sorted(best.keys()),
        "bk_market_counts": {sl: len(mkts) for sl, mkts in combined.items()}, "bookmakers": sorted(combined.keys()), "bt_live_fetched": bool(fresh_bt), "bt_external_id": parent_match_id,
        "stored_bookmakers": sorted(stored_markets.keys()), "fetch_errors": fetch_errors, "has_analytics": analytics_summary.get("available", False), "analytics": analytics_summary,
        "latency_ms": int((time.perf_counter() - t0) * 1000), "server_time": _now_utc().isoformat(),
    }, encrypt_for=user)

@bp_odds_customer.route("/odds/match/<parent_match_id>/analytics")
def get_match_analytics(parent_match_id: str):
    from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
    from app.utils.decorators_ import log_event
    from app.models.odds import UnifiedMatch
    t0 = time.perf_counter()
    user = _current_user_from_header()
    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um: return _err("Match not found", 404)
    log_event("match_analytics_view", {"match_id": parent_match_id})

    analytics = _build_analytics_full(_get_analytics(parent_match_id, trigger_if_missing=True))
    return _signed_response({
        "ok": True, "match_id": um.id, "parent_match_id": parent_match_id, "betradar_id": parent_match_id, "home_team": um.home_team_name, "away_team": um.away_team_name,
        "competition": um.competition_name, "sport": _normalise_sport_slug(um.sport_name or ""), "start_time": um.start_time.isoformat() if um.start_time else None,
        "available": analytics.get("available", False), "fetching": not analytics.get("available", False), "analytics": analytics, "latency_ms": int((time.perf_counter() - t0) * 1000), "server_time": _now_utc().isoformat(),
    }, encrypt_for=user)

@bp_odds_customer.route("/odds/match/<parent_match_id>/analytics/refresh", methods=["POST"])
def refresh_match_analytics(parent_match_id: str):
    from app.utils.customer_jwt_helpers import _err, _signed_response
    from app.models.odds import UnifiedMatch
    from app.workers.celery_tasks import celery
    if not UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first(): return _err("Match not found", 404)
    try: celery.send_task("tasks.sp.get_match_analytics", args=[parent_match_id, True], queue="harvest", countdown=0); dispatched = True
    except Exception: dispatched = False
    return _signed_response({"ok": dispatched, "parent_match_id": parent_match_id, "dispatched": dispatched, "message": "Analytics refresh queued.", "server_time": _now_utc().isoformat()})

@bp_odds_customer.route("/odds/search")
def search_matches():
    from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
    from app.utils.decorators_ import log_event
    from app.models.odds import UnifiedMatch, BookmakerMatchOdds
    from sqlalchemy import or_, func as sqlfunc
    from app.extensions import db
    t0 = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    q_str = (request.args.get("q") or "").strip()
    mode = request.args.get("mode", "upcoming")
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, int(request.args.get("per_page", 20)))
    sport = (request.args.get("sport") or "").strip()
    if not q_str: return _err("Provide query param 'q'", 400)

    qs = UnifiedMatch.query.filter(or_(UnifiedMatch.home_team_name.ilike(f"%{q_str}%"), UnifiedMatch.away_team_name.ilike(f"%{q_str}%"), UnifiedMatch.competition_name.ilike(f"%{q_str}%"), UnifiedMatch.parent_match_id.ilike(f"%{q_str}%")))
    if sport: qs = _sport_filter(qs, sport)
    qs = _mode_time_filter(qs, mode)
    if mode in ("upcoming", "live"): qs = _multi_bk_filter(qs)
    
    total = qs.count()
    um_list = qs.order_by(UnifiedMatch.start_time).offset((page-1)*per_page).limit(per_page).all()
    match_ids = [um.id for um in um_list]
    bk_counts = dict(db.session.query(BookmakerMatchOdds.match_id, sqlfunc.count(BookmakerMatchOdds.bookmaker_id)).filter(BookmakerMatchOdds.match_id.in_(match_ids)).group_by(BookmakerMatchOdds.match_id).all()) if match_ids else {}

    results = [{"match_id": um.id, "parent_match_id": um.parent_match_id, "betradar_id": um.parent_match_id, "join_key": f"br_{um.parent_match_id}" if um.parent_match_id else f"db_{um.id}", "home_team": um.home_team_name, "away_team": um.away_team_name, "competition": um.competition_name, "sport": _normalise_sport_slug(um.sport_name or ""), "start_time": um.start_time.isoformat() if um.start_time else None, "status": _effective_status(getattr(um, "status", None), um.start_time), "is_live": _is_live(getattr(um, "status", None), um.start_time), "bookie_count": bk_counts.get(um.id, 0), "detail_url": f"/api/odds/match/{um.parent_match_id}", "analytics_url": f"/api/odds/match/{um.parent_match_id}/analytics"} for um in um_list]
    log_event("odds_search", {"q": q_str, "mode": mode, "total": total})
    return _signed_response({"ok": True, "q": q_str, "mode": mode, "tier": tier, "total": total, "page": page, "per_page": per_page, "pages": max(1, (total + per_page - 1) // per_page), "latency_ms": int((time.perf_counter() - t0) * 1000), "matches": results, "source": "postgresql"}, encrypt_for=user)

# ── Bookmaker slug → full display name ──────────────────────────────────────
_BK_DISPLAY = {
    "sp":        "SportPesa",
    "bt":        "Betika",
    "od":        "OdiBets",
    "1xbet":     "1xBet",
    "22bet":     "22Bet",
    "betwinner": "BetWinner",
    "melbet":    "Melbet",
    "megapari":  "Megapari",
    "helabet":   "Helabet",
    "paripesa":  "PariPesa",
    "sbo":       "SBO",
}

def _bk_display(slug: str) -> str:
    """Return the full bookmaker display name for a given slug."""
    return _BK_DISPLAY.get(slug.lower(), slug.upper())


# ── MinIO helpers for pre-generated Word document caching ───────────────────
def _get_minio_client():
    """Return an initialised MinIO client, or None if unavailable."""
    import os
    try:
        from minio import Minio
        endpoint = os.environ.get("STORAGE_ENDPOINT", "5.78.137.59:6500")
        clean    = endpoint.replace("http://", "").replace("https://", "")
        secure   = endpoint.startswith("https")
        client   = Minio(
            clean,
            access_key=os.environ.get("STORAGE_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("STORAGE_SECRET_KEY", "minioadmin"),
            secure=secure,
        )
        bucket = "odds-reports"
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
        return client, bucket
    except Exception:
        return None, None


def _minio_report_key(sport: str, arb_only: bool) -> str:
    suffix = "_arb" if arb_only else "_full"
    return f"reports/{sport}{suffix}_latest.docx"


def _serve_minio_report(sport: str, arb_only: bool):
    """Try to fetch the pre-generated report from MinIO. Returns BytesIO or None."""
    try:
        client, bucket = _get_minio_client()
        if not client:
            return None
        key  = _minio_report_key(sport, arb_only)
        resp = client.get_object(bucket, key)
        data = resp.read()
        resp.close(); resp.release_conn()
        return io.BytesIO(data)
    except Exception:
        return None


def _save_minio_report(sport: str, arb_only: bool, buf: io.BytesIO) -> bool:
    """Upload a generated Word report to MinIO. buf position is preserved."""
    try:
        client, bucket = _get_minio_client()
        if not client:
            return False
        key    = _minio_report_key(sport, arb_only)
        buf.seek(0)
        length = buf.getbuffer().nbytes
        client.put_object(
            bucket, key, buf, length,
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        buf.seek(0)
        return True
    except Exception:
        return False


def _generate_word_document(sport: str, arb_only: bool) -> io.BytesIO:
    from docx import Document
    from docx.shared import Inches, Pt, RGBColor
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml import OxmlElement, parse_xml
    from docx.oxml.ns import nsdecls, qn
    import io
    import time
    from datetime import datetime as _dt, timezone as _tz
    
    # ─── STYLE CONSTANTS FOR REBRANDING & EASY FORMATTING ───
    FONT_FAMILY = "Arial"
    
    # Brand Hex Colors for Table Shading / Cell Shading (HEX string without #)
    HEX_PRIMARY_BG = "0F172A"       # Deep slate for table header background
    HEX_ARB_HEADER_BG = "EA580C"    # Arbitrage header background (Vibrant Orange)
    HEX_ARB_CELL_BG = "FFF7ED"      # Light orange background for arbitrage table cells
    HEX_BEST_ODD_BG = "DCFCE7"      # Light green for best odd cell background (#dcfce7)
    HEX_ALT_ROW_BG = "F8FAFC"       # Light gray for alternating row background
    HEX_MUTED_BG = "F1F5F9"         # Muted gray for standard header background
    
    # Brand RGB Colors for Runs/Texts
    RGB_PRIMARY = RGBColor(0x0F, 0x17, 0x2A)      # Deep slate for titles
    RGB_TEXT = RGBColor(0x2D, 0x37, 0x48)         # Charcoal for regular text
    RGB_MUTED = RGBColor(0x64, 0x74, 0x8B)        # Slate for secondary / metadata
    RGB_GREEN = RGBColor(0x15, 0x80, 0x3D)        # Forest green for SMS IDs and best odds (#15803d)
    RGB_ORANGE = RGBColor(0xEA, 0x58, 0x0C)       # Arbitrage orange
    RGB_WHITE = RGBColor(0xFF, 0xFF, 0xFF)
    
    # Font Sizes
    SIZE_TITLE = Pt(20)
    SIZE_MATCH_HEADER = Pt(11.5)
    SIZE_HEADER = Pt(9.5)
    SIZE_TEXT = Pt(9.5)
    SIZE_TINY = Pt(8.5)

    # ─── HELPERS FOR XML / CELL MANIPULATION ───
    def _set_cell_shading(cell, color_hex: str):
        shading = parse_xml(f'<w:shd {nsdecls("w")} w:fill="{color_hex}"/>')
        cell._tc.get_or_add_tcPr().append(shading)

    def _add_styled_paragraph(cell, text: str, bold: bool = False, italic: bool = False, color = None, size = None, align = None):
        p = cell.paragraphs[0]
        p.alignment = align or WD_ALIGN_PARAGRAPH.LEFT
        run = p.add_run(text)
        run.bold = bold
        run.italic = italic
        if color:
            run.font.color.rgb = color
        if size:
            run.font.size = size
        return p

    def _get_float_odd(val) -> float | None:
        if val is None: return None
        if isinstance(val, (int, float)): return float(val)
        if isinstance(val, dict):
            for fld in ("price", "odd", "odds", "value"):
                if val.get(fld):
                    try: return float(val[fld])
                    except: pass
        try: return float(val)
        except: return None

    # ── 1. Fetch upcoming + live from high-speed Redis cache ────────────────
    matches_raw = []
    _now_ts = time.time()
    try:
        from app.api.odds_stream import _get_unified_patched
        raw_upcoming = _get_unified_patched("upcoming", sport)
        raw_live     = _get_unified_patched("live",     sport)

        # Build set of join-keys already seen in live so we don't double-count
        live_jks = set()
        for m in raw_live:
            if not isinstance(m, dict): continue
            jk = m.get("join_key") or m.get("parent_match_id")
            if jk: live_jks.add(jk)

        seen_jks = set()

        # Add live matches first (they have scores / match-time)
        for m in raw_live:
            if not isinstance(m, dict): continue
            jk = m.get("join_key") or m.get("parent_match_id")
            if jk and jk not in seen_jks:
                seen_jks.add(jk)
                m.setdefault("_is_live_doc", True)
                if m.get("arb_opportunities") and not m.get("arbitrage"):
                    m["arbitrage"] = m["arb_opportunities"]; m["has_arb"] = True
                matches_raw.append(m)

        # Add upcoming only if NOT already seen via live, and NOT already started
        for m in raw_upcoming:
            if not isinstance(m, dict): continue
            jk = m.get("join_key") or m.get("parent_match_id")
            if jk in live_jks: continue          # skip — already in live list
            if jk and jk in seen_jks: continue
            # Skip matches that have already kicked off (start_time in the past)
            st_raw = m.get("start_time") or ""
            if st_raw:
                try:
                    st_dt = _dt.fromisoformat(st_raw.replace("Z", "+00:00"))
                    if st_dt.tzinfo is None:
                        st_dt = st_dt.replace(tzinfo=_tz.utc)
                    if st_dt.timestamp() < _now_ts - 90:  # started >90s ago → live
                        continue
                except Exception:
                    pass
            if jk: seen_jks.add(jk)
            if m.get("arb_opportunities") and not m.get("arbitrage"):
                m["arbitrage"] = m["arb_opportunities"]; m["has_arb"] = True
            matches_raw.append(m)
    except Exception:
        pass

    # ── 2. DB fallback if Redis cache is empty ──────────────────────────────
    if not matches_raw:
        try:
            db_upcoming, _, _ = _load_db_matches(sport, mode="upcoming", page=1, per_page=500)
            db_live,     _, _ = _load_db_matches(sport, mode="live",     page=1, per_page=100)
            matches_raw.extend(db_upcoming + db_live)
        except Exception:
            pass

    # ── 3. Dynamically compute arbitrage for every match ───────────────────
    for m in matches_raw:
        if not m.get("has_arb") or not m.get("arbitrage"):
            best = m.get("best") or {}
            arb_markets = []
            if len(m.get("bookmakers") or {}) >= 2:
                for mkt, outcomes in best.items():
                    if len(outcomes) < 2: continue
                    try:
                        arb_sum = sum(1.0 / float(v["odd"]) for v in outcomes.values())
                        if arb_sum < 1.0:
                            profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
                            legs = [{"outcome": o, "bk": v["bk"], "odd": v["odd"]} for o, v in outcomes.items()]
                            arb_markets.append({"market": mkt, "profit_pct": profit_pct,
                                                "arb_sum": round(arb_sum, 6), "legs": legs})
                    except Exception:
                        pass
                arb_markets.sort(key=lambda x: -x["profit_pct"])
            if arb_markets:
                m["has_arb"] = True
                m["arbitrage"] = arb_markets
                m["best_arb_pct"] = arb_markets[0]["profit_pct"]

    # ── 4. Filter & sort ───────────────────────────────────────────────────
    matches = matches_raw
    if arb_only:
        matches = [m for m in matches if m.get("has_arb") or m.get("arbitrage")]
    matches.sort(key=lambda x: x.get("start_time") or "")

    # 5. GENERATE WORD REPORT USING python-docx
    doc = Document()
    
    sections = doc.sections
    for section in sections:
        section.top_margin = Inches(0.8)
        section.bottom_margin = Inches(0.8)
        section.left_margin = Inches(0.8)
        section.right_margin = Inches(0.8)

    style_normal = doc.styles['Normal']
    font = style_normal.font
    font.name = FONT_FAMILY
    font.size = SIZE_TEXT
    font.color.rgb = RGB_TEXT

    # Center Title
    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title_run = title.add_run(f"OddsKenya — {sport.title()} Odds Booklet")
    title_run.bold = True
    title_run.font.size = SIZE_TITLE
    title_run.font.color.rgb = RGB_PRIMARY

    # Sub-header
    sub = doc.add_paragraph()
    sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    sub_run = sub.add_run(
        f"Generated on {time.strftime('%A, %B %d, %Y at %H:%M:%S EAT')} | "
        f"Matches: {len(matches)} "
        f"{' (Arbitrage Only)' if arb_only else ''}"
    )
    sub_run.font.size = SIZE_TINY
    sub_run.font.italic = True
    sub_run.font.color.rgb = RGB_MUTED

    doc.add_paragraph().paragraph_format.space_after = Pt(12)

    p_div = doc.add_paragraph()
    p_div.paragraph_format.space_after = Pt(20)
    p_div_run = p_div.add_run("―" * 58)
    p_div_run.font.color.rgb = RGBColor(0xe2, 0xe8, 0xf0)

    if not matches:
        p_none = doc.add_paragraph()
        p_none.alignment = WD_ALIGN_PARAGRAPH.CENTER
        r_none = p_none.add_run("No matches matching filters found.")
        r_none.italic = True
    else:
        # DB lookup for BookmakerMatchLinks using parent_match_id / betradar_id
        br_ids = [m.get("betradar_id") or m.get("parent_match_id") for m in matches if m.get("betradar_id") or m.get("parent_match_id")]
        link_dict = {}
        if br_ids:
            try:
                from app.models.odds import UnifiedMatch
                from app.models.bookmakers_model import BookmakerMatchLink, Bookmaker
                ums = UnifiedMatch.query.filter(UnifiedMatch.parent_match_id.in_(br_ids)).all()
                um_id_to_br = {um.id: um.parent_match_id for um in ums}
                um_ids = list(um_id_to_br.keys())
                
                if um_ids:
                    bml_list = BookmakerMatchLink.query.filter(BookmakerMatchLink.match_id.in_(um_ids)).all()
                    for bml in bml_list:
                        bk = Bookmaker.query.get(bml.bookmaker_id)
                        slug = bk.slug if bk else str(bml.bookmaker_id)
                        br_id = um_id_to_br.get(bml.match_id)
                        if br_id:
                            link_dict.setdefault(br_id, {})[slug] = bml.external_match_id
            except Exception as e:
                log.warning("Error fetching match links for booklet: %s", e)

        for idx, m in enumerate(matches):
            h_team = m.get("home_team", "Home Team")
            a_team = m.get("away_team", "Away Team")
            comp = m.get("competition", "General")
            time_raw = m.get("start_time", "")
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(time_raw.replace("Z", "+00:00"))
                time_str = dt.strftime("%A, %b %d at %H:%M EAT")
            except Exception:
                time_str = time_raw

            # Match Header Paragraph
            p_match = doc.add_paragraph()
            p_match.paragraph_format.space_before = Pt(14)
            p_match.paragraph_format.space_after = Pt(2)
            
            run_match = p_match.add_run(f"{idx+1}. {h_team} vs {a_team}")
            run_match.bold = True
            run_match.font.size = SIZE_MATCH_HEADER
            run_match.font.color.rgb = RGB_PRIMARY

            # Meta information (competition, time)
            p_meta = doc.add_paragraph()
            p_meta.paragraph_format.space_after = Pt(4)
            r_meta = p_meta.add_run(f"🏆 {comp}  |  📅 {time_str}")
            r_meta.font.size = SIZE_TINY
            r_meta.font.color.rgb = RGB_MUTED

            # Resolve external Match/SMS/Game IDs
            m_id = m.get("match_id")
            br_id = m.get("betradar_id") or m.get("parent_match_id")
            m_links = {}
            if br_id and br_id in link_dict:
                m_links.update(link_dict[br_id])
            
            # Local fallback search in case database link table is not yet matched
            for slug in ("sp", "bt", "od"):
                if slug not in m_links:
                    val = m.get("bk_ids", {}).get(slug)
                    if val:
                        m_links[slug] = val
                    else:
                        bk_obj = m.get("bookmakers", {}).get(slug) if isinstance(m.get("bookmakers"), dict) else None
                        if bk_obj and isinstance(bk_obj, dict):
                            val2 = bk_obj.get("link", {}).get("external_match_id") if isinstance(bk_obj.get("link"), dict) else None
                            if val2:
                                m_links[slug] = val2

            # Render SMS IDs prominently
            if m_links:
                p_ids = doc.add_paragraph()
                p_ids.paragraph_format.space_after = Pt(6)
                ids_str = []
                if "sp" in m_links: ids_str.append(f"SportPesa ID: #{m_links['sp']}")
                if "bt" in m_links: ids_str.append(f"Betika ID: #{m_links['bt']}")
                if "od" in m_links: ids_str.append(f"OdiBets ID: #{m_links['od']}")
                r_ids = p_ids.add_run("📲 BOOKMAKER GAME IDs → " + " | ".join(ids_str))
                r_ids.font.size = SIZE_TINY
                r_ids.font.bold = True
                r_ids.font.color.rgb = RGB_GREEN

            # Arbitrage opportunities for this match
            arbs = m.get("arbitrage") or []
            arb_markets = set() # Keep track of markets with arbitrage to exclude from standard table
            
            if arbs:
                p_arb_title = doc.add_paragraph()
                p_arb_title.paragraph_format.space_before = Pt(6)
                p_arb_title.paragraph_format.space_after = Pt(4)
                r_arb_title = p_arb_title.add_run("⚡ ACTIVE ARBITRAGE OPPORTUNITIES (GUARANTEED PROFIT)")
                r_arb_title.bold = True
                r_arb_title.font.size = SIZE_TINY
                r_arb_title.font.color.rgb = RGB_ORANGE

                # ── TABLE 1: Arbitrage Opportunities Table ──
                # Columns: Market | Selection | Best Odd | Bookmaker
                arb_table = doc.add_table(rows=1, cols=4)
                arb_table.style = 'Light Shading Accent 1'
                
                # Header formatting
                hdr_cells = arb_table.rows[0].cells
                hdr_cells[0].text = 'Arb Market'
                hdr_cells[1].text = 'Selection'
                hdr_cells[2].text = 'Best Odd'
                hdr_cells[3].text = 'Bookmaker Source'
                for cell in hdr_cells:
                    cell.paragraphs[0].runs[0].font.bold = True
                    cell.paragraphs[0].runs[0].font.size = SIZE_HEADER
                    cell.paragraphs[0].runs[0].font.color.rgb = RGB_WHITE
                    _set_cell_shading(cell, HEX_ARB_HEADER_BG)

                for arb in arbs:
                    mkt = arb.get("market", "")
                    arb_markets.add(mkt)
                    profit = float(arb.get("profit_pct", 0))
                    mkt_label = _mkt_label(mkt) + f" (+{profit:.2f}%)"
                    
                    for leg in arb.get("legs", []):
                        row_cells = arb_table.add_row().cells
                        _set_cell_shading(row_cells[0], HEX_ARB_CELL_BG)
                        _set_cell_shading(row_cells[1], HEX_ARB_CELL_BG)
                        _set_cell_shading(row_cells[2], HEX_ARB_CELL_BG)
                        _set_cell_shading(row_cells[3], HEX_ARB_CELL_BG)
                        
                        _add_styled_paragraph(row_cells[0], mkt_label, bold=True, color=RGB_PRIMARY, size=SIZE_TEXT)
                        _add_styled_paragraph(row_cells[1], str(leg.get("outcome", "")).upper(), bold=True, color=RGB_TEXT, size=SIZE_TEXT)
                        
                        # Color arbitrage odd in orange
                        odd_val = float(leg.get("odd") or 0)
                        _add_styled_paragraph(row_cells[2], f"{odd_val:.2f}", bold=True, color=RGB_ORANGE, size=SIZE_TEXT)
                        _add_styled_paragraph(row_cells[3], _bk_display(str(leg.get("bk", ""))), bold=True, color=RGB_PRIMARY, size=SIZE_TEXT)

                p_spacer = doc.add_paragraph()
                p_spacer.paragraph_format.space_after = Pt(6)

            # ── TABLE 2: Standard Side-by-Side Odds Comparison Table ──
            # Columns: Market | Selection | SportPesa | Betika | OdiBets
            best_odds = m.get("best_odds") or m.get("best") or {}

            # Market pretty-print helper
            _MARKET_NAMES = {
                "1x2":               "Full-Time 1X2",
                "match_winner":       "Match Winner",
                "moneyline":          "Moneyline",
                "btts":               "Both Teams to Score",
                "double_chance":      "Double Chance",
                "dnb":                "Draw No Bet",
                "half_time":          "Half-Time Result",
                "ht_ft":              "Half-Time / Full-Time",
                "correct_score":      "Correct Score",
                "winner":             "Winner",
                "total_goals":        "Total Goals",
                "asian_handicap":     "Asian Handicap",
                "european_handicap":  "European Handicap",
                "odd_even":           "Odd / Even Goals",
                "first_goal":         "First Goal",
                "last_goal":          "Last Goal",
                "anytime_scorer":     "Anytime Goal-Scorer",
                "first_scorer":       "First Goal-Scorer",
                "clean_sheet_home":   "Home Clean Sheet",
                "clean_sheet_away":   "Away Clean Sheet",
                "win_to_nil_home":    "Home Win to Nil",
                "win_to_nil_away":    "Away Win to Nil",
                "both_score_win":     "BTTS & Win",
            }

            def _mkt_label(mkt: str) -> str:
                if mkt in _MARKET_NAMES:
                    return _MARKET_NAMES[mkt]
                if mkt.startswith(("over_under_goals_", "over_under_")):
                    line = mkt.replace("over_under_goals_", "").replace("over_under_", "").replace("_", ".")
                    return f"Over/Under {line} Goals"
                if mkt.startswith("asian_handicap_"):
                    line = mkt.replace("asian_handicap_", "").replace("_", ".")
                    return f"AH {line}"
                return mkt.replace("_", " ").title()

            # Priority order: key markets first, then everything else
            priority = ["1x2", "match_winner", "moneyline", "btts", "double_chance",
                        "dnb", "half_time", "ht_ft", "total_goals", "winner"]
            ou_sorted = sorted(
                [k for k in best_odds if k.startswith(("over_under_goals_", "over_under_"))],
                key=lambda k: float(k.replace("over_under_goals_", "").replace("over_under_", "").replace("_", ".")) if k.replace("over_under_goals_", "").replace("over_under_", "").replace("_", ".").replace(".", "", 1).isdigit() else 999
            )
            other_mkts = [k for k in best_odds if k not in priority and k not in ou_sorted]
            ordered_mkts = priority + ou_sorted + sorted(other_mkts)

            # Filter out arbitrage markets to prevent duplicate clutter
            filtered_mkts = [mkt for mkt in ordered_mkts if mkt not in arb_markets]

            if filtered_mkts:
                p_std_title = doc.add_paragraph()
                p_std_title.paragraph_format.space_after = Pt(4)
                r_std_title = p_std_title.add_run("📊 COMPARISON GRID (NO ARBITRAGE)")
                r_std_title.bold = True
                r_std_title.font.size = SIZE_TINY
                r_std_title.font.color.rgb = RGB_SECONDARY

                table = doc.add_table(rows=1, cols=5)
                table.style = 'Light Shading Accent 1'
                
                # Header columns: Market | Selection | SportPesa | Betika | OdiBets
                hdr_cells = table.rows[0].cells
                hdr_cells[0].text = 'Market'
                hdr_cells[1].text = 'Selection'
                hdr_cells[2].text = 'SportPesa'
                hdr_cells[3].text = 'Betika'
                hdr_cells[4].text = 'OdiBets'
                for cell in hdr_cells:
                    cell.paragraphs[0].runs[0].font.bold = True
                    cell.paragraphs[0].runs[0].font.size = SIZE_HEADER
                    cell.paragraphs[0].runs[0].font.color.rgb = RGB_PRIMARY
                    _set_cell_shading(cell, HEX_MUTED_BG)

                rows_added = 0
                for mkt in filtered_mkts:
                    mkt_data = best_odds.get(mkt)
                    if not mkt_data or not isinstance(mkt_data, dict):
                        continue
                    label = _mkt_label(mkt)
                    
                    for out in mkt_data.keys():
                        # Read odds for each local bookmaker
                        sp_odd = _get_float_odd(m.get("bookmakers", {}).get("sp", {}).get("markets", {}).get(mkt, {}).get(out))
                        bt_odd = _get_float_odd(m.get("bookmakers", {}).get("bt", {}).get("markets", {}).get(mkt, {}).get(out))
                        od_odd = _get_float_odd(m.get("bookmakers", {}).get("od", {}).get("markets", {}).get(mkt, {}).get(out))
                        
                        # Only render row if we have at least one valid odd
                        if sp_odd is None and bt_odd is None and od_odd is None:
                            continue
                            
                        # Find best odd to highlight
                        odds_list = [o for o in (sp_odd, bt_odd, od_odd) if o is not None]
                        best_val = max(odds_list) if odds_list else 0.0

                        row_cells = table.add_row().cells
                        
                        # Alternating row background for enhanced readability
                        if rows_added % 2 == 1:
                            for cell in row_cells:
                                _set_cell_shading(cell, HEX_ALT_ROW_BG)

                        # Write Market and Selection
                        _add_styled_paragraph(row_cells[0], label, size=SIZE_TEXT, color=RGB_TEXT)
                        _add_styled_paragraph(row_cells[1], str(out).upper(), bold=True, size=SIZE_TEXT, color=RGB_SECONDARY)

                        # Write SportPesa odd
                        if sp_odd is not None:
                            is_best = (sp_odd == best_val and best_val > 1.0)
                            cell_color = RGB_GREEN if is_best else RGB_TEXT
                            _add_styled_paragraph(row_cells[2], f"{sp_odd:.2f}", bold=is_best, size=SIZE_TEXT, color=cell_color)
                            if is_best:
                                _set_cell_shading(row_cells[2], HEX_BEST_ODD_BG)
                        else:
                            _add_styled_paragraph(row_cells[2], "—", size=SIZE_TEXT, color=RGB_MUTED, align=WD_ALIGN_PARAGRAPH.CENTER)

                        # Write Betika odd
                        if bt_odd is not None:
                            is_best = (bt_odd == best_val and best_val > 1.0)
                            cell_color = RGB_GREEN if is_best else RGB_TEXT
                            _add_styled_paragraph(row_cells[3], f"{bt_odd:.2f}", bold=is_best, size=SIZE_TEXT, color=cell_color)
                            if is_best:
                                _set_cell_shading(row_cells[3], HEX_BEST_ODD_BG)
                        else:
                            _add_styled_paragraph(row_cells[3], "—", size=SIZE_TEXT, color=RGB_MUTED, align=WD_ALIGN_PARAGRAPH.CENTER)

                        # Write OdiBets odd
                        if od_odd is not None:
                            is_best = (od_odd == best_val and best_val > 1.0)
                            cell_color = RGB_GREEN if is_best else RGB_TEXT
                            _add_styled_paragraph(row_cells[4], f"{od_odd:.2f}", bold=is_best, size=SIZE_TEXT, color=cell_color)
                            if is_best:
                                _set_cell_shading(row_cells[4], HEX_BEST_ODD_BG)
                        else:
                            _add_styled_paragraph(row_cells[4], "—", size=SIZE_TEXT, color=RGB_MUTED, align=WD_ALIGN_PARAGRAPH.CENTER)

                        rows_added += 1

                if rows_added == 0:
                    doc.paragraphs[-1]._element.getparent().remove(table._element)
                    p_no_odds = doc.add_paragraph()
                    r_no_odds = p_no_odds.add_run("  No standard odds comparison currently available.")
                    r_no_odds.italic = True

            doc.add_paragraph().paragraph_format.space_after = Pt(12)

    f_stream = io.BytesIO()
    doc.save(f_stream)
    f_stream.seek(0)
    return f_streamows_added += 1

            if rows_added == 0:
                doc.paragraphs[-1]._element.getparent().remove(table._element)
                p_no_odds = doc.add_paragraph()
                r_no_odds = p_no_odds.add_run("  No odds parsed or currently available.")
                r_no_odds.italic = True

            doc.add_paragraph().paragraph_format.space_after = Pt(8)

    f_stream = io.BytesIO()
    doc.save(f_stream)
    f_stream.seek(0)
    return f_stream

def download_odds_word():
    from app.utils.customer_jwt_helpers import _current_user_from_header
    from flask import send_file, make_response
    import time

    sport    = request.args.get("sport", "soccer").lower().strip()
    arb_only = request.args.get("arb_only", "") in ("1", "true")

    # 1. AUTH RULE: Only soccer (football) is free & anonymous.
    #    Other sports require an authenticated session.
    user = None
    if sport != "soccer":
        user = _current_user_from_header()
        if not user:
            return make_response("Authentication required to download reports for this sport.", 401)

    # Log report download for monetization funnel analytics
    from app.utils.decorators_ import log_event
    log_event("report_download", {"sport": sport, "arb_only": arb_only})

    # 2. Try serving the pre-generated MinIO-cached document first (fast path)
    f_stream = _serve_minio_report(sport, arb_only)

    # 3. Fall back to on-demand generation if MinIO is unavailable or cache is stale
    if f_stream is None:
        f_stream = _generate_word_document(sport, arb_only)
        # Persist to MinIO in the background so the next request is instant
        try:
            _save_minio_report(sport, arb_only, f_stream)
        except Exception:
            pass

    filename = f"OddsKenya_Report_{sport}_{time.strftime('%Y%m%d_%H%M%S')}.docx"
    response = make_response(send_file(
        f_stream,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        as_attachment=True,
        download_name=filename,
    ))
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response