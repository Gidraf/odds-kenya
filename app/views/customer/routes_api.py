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
    import json as _json
    
    # ─── STYLE CONSTANTS FOR REBRANDING & PREMIUM DESIGN ───
    FONT_FAMILY = "Arial"
    
    # Brand Hex Colors for Table Shading / Cell Shading (HEX string without #)
    HEX_PRIMARY_BG = "0F172A"       # Deep slate for table header background
    HEX_BEST_ODD_BG = "DCFCE7"      # Light green for best odd cell background (#dcfce7)
    HEX_ALT_ROW_BG = "F8FAFC"       # Light gray for alternating row background
    HEX_MUTED_BG = "F1F5F9"         # Muted gray background for standard matches card
    HEX_ARB_CARD_BG = "F0FDF4"      # Vibrant soft green background for arbitrage card
    HEX_ARB_BORDER = "BBF7D0"       # Border for arbitrage card
    HEX_STD_BORDER = "E2E8F0"       # Border for standard card
    
    # Brand RGB Colors for Runs/Texts
    RGB_PRIMARY = RGBColor(0x0F, 0x17, 0x2A)      # Deep slate for titles
    RGB_TEXT = RGBColor(0x2D, 0x37, 0x48)         # Charcoal for regular text
    RGB_MUTED = RGBColor(0x64, 0x74, 0x8B)        # Slate for secondary / metadata
    RGB_GREEN = RGBColor(0x15, 0x80, 0x3D)        # Forest green for SMS IDs and best odds (#15803d)
    RGB_RED = RGBColor(0xDC, 0x26, 0x26)          # Red for NO arbitrage
    RGB_ORANGE = RGBColor(0xEA, 0x58, 0x0C)       # Arbitrage orange
    RGB_WHITE = RGBColor(0xFF, 0xFF, 0xFF)
    RGB_SECONDARY = RGBColor(0x47, 0x55, 0x69)    # Slate secondary color
    
    # Bookmaker Specific RGB Colors for Legend and Stake Distribution
    RGB_SP = RGBColor(0x25, 0x63, 0xEB)           # Blue for SportPesa
    RGB_BT = RGBColor(0x16, 0xA3, 0x4A)           # Green for Betika
    RGB_OD = RGBColor(0xD9, 0x77, 0x06)           # Gold/Yellow for OdiBets
    
    # Font Sizes
    SIZE_TITLE = Pt(18)
    SIZE_MATCH_HEADER = Pt(11.5)
    SIZE_HEADER = Pt(9.0)
    SIZE_TEXT = Pt(9.0)
    SIZE_TINY = Pt(8.0)

    # ─── HELPERS FOR XML / CELL MANIPULATION ───
    def _set_cell_shading(cell, color_hex: str):
        shading = parse_xml(f'<w:shd {nsdecls("w")} w:fill="{color_hex}"/>')
        cell._tc.get_or_add_tcPr().append(shading)

    def _set_cell_margins(cell, top=100, bottom=100, left=150, right=150):
        tcPr = cell._tc.get_or_add_tcPr()
        tcMar = OxmlElement('w:tcMar')
        for m, val in [('top', top), ('bottom', bottom), ('left', left), ('right', right)]:
            node = OxmlElement(f'w:{m}')
            node.set(qn('w:w'), str(val))
            node.set(qn('w:type'), 'dxa')
            tcMar.append(node)
        tcPr.append(tcMar)

    def _set_cell_borders(cell, color_hex: str = "CBD5E1", sz: str = "6"):
        tcPr = cell._tc.get_or_add_tcPr()
        tcBorders = OxmlElement('w:tcBorders')
        for border_name in ['top', 'left', 'bottom', 'right']:
            border = OxmlElement(f'w:{border_name}')
            border.set(qn('w:val'), 'single')
            border.set(qn('w:sz'), sz)
            border.set(qn('w:space'), '0')
            border.set(qn('w:color'), color_hex)
            tcBorders.append(border)
        tcPr.append(tcBorders)

    def _set_cell_border_bottom(cell, color_hex: str = "E2E8F0", sz: str = "4"):
        tcPr = cell._tc.get_or_add_tcPr()
        tcBorders = tcPr.first_child_found_in("w:tcBorders")
        if tcBorders is None:
            tcBorders = OxmlElement('w:tcBorders')
            tcPr.append(tcBorders)
        bottom = OxmlElement('w:bottom')
        bottom.set(qn('w:val'), 'single')
        bottom.set(qn('w:sz'), sz)
        bottom.set(qn('w:space'), '0')
        bottom.set(qn('w:color'), color_hex)
        tcBorders.append(bottom)

    def _remove_table_borders(table):
        tblPr = table._tbl.tblPr
        tblBorders = OxmlElement('w:tblBorders')
        for border_name in ['top', 'left', 'bottom', 'right', 'insideH', 'insideV']:
            border = OxmlElement(f'w:{border_name}')
            border.set(qn('w:val'), 'none')
            tblBorders.append(border)
        tblPr.append(tblBorders)

    def _add_styled_paragraph(cell, text: str, bold: bool = False, italic: bool = False, color = None, size = None, align = None, space_after = 0):
        if len(cell.paragraphs) == 1 and not cell.paragraphs[0].text:
            p = cell.paragraphs[0]
        else:
            p = cell.add_paragraph()
        p.alignment = align or WD_ALIGN_PARAGRAPH.LEFT
        p.paragraph_format.space_after = Pt(space_after)
        p.paragraph_format.line_spacing = 1.1
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

    from app.workers.celery_tasks import _redis as _get_redis_for_sp
    _r_sp = _get_redis_for_sp()

    def _read_sp_raw(sport_slug: str) -> list:
        for key in (f"odds:sp:upcoming:{sport_slug}", f"sp:upcoming:{sport_slug}"):
            try:
                raw = _r_sp.get(key)
                if not raw:
                    continue
                obj = _json.loads(raw)
                if isinstance(obj, list):
                    return [m for m in obj if isinstance(m, dict)]
                if isinstance(obj, dict):
                    ms = obj.get("matches") or obj.get("data") or []
                    if isinstance(ms, list):
                        return [m for m in ms if isinstance(m, dict)]
            except Exception:
                pass
        return []

    sp_harvested = False
    sp_raw_list = _read_sp_raw(sport)
    if not sp_raw_list:
        try:
            from app.workers.tasks_upcoming import sp_harvest_sport
            sp_harvest_sport.apply(args=[sport])
            sp_raw_list = _read_sp_raw(sport)
            sp_harvested = True
        except Exception:
            pass

    matches_raw = []
    _now_ts = time.time()
    try:
        from app.api.odds_stream import _get_unified_patched
        raw_upcoming = _get_unified_patched("upcoming", sport, force_refresh=sp_harvested)
        raw_live     = _get_unified_patched("live",     sport, force_refresh=sp_harvested)
        live_jks = set()
        for m in raw_live:
            if not isinstance(m, dict): continue
            jk = m.get("join_key") or m.get("parent_match_id")
            if jk: live_jks.add(jk)
        seen_jks = set()
        for m in raw_live:
            if not isinstance(m, dict): continue
            jk = m.get("join_key") or m.get("parent_match_id")
            if jk and jk not in seen_jks:
                seen_jks.add(jk)
                m.setdefault("_is_live_doc", True)
                if m.get("arb_opportunities") and not m.get("arbitrage"):
                    m["arbitrage"] = m["arb_opportunities"]; m["has_arb"] = True
                matches_raw.append(m)
        for m in raw_upcoming:
            if not isinstance(m, dict): continue
            jk = m.get("join_key") or m.get("parent_match_id")
            if jk in live_jks: continue
            if jk and jk in seen_jks: continue
            st_raw = m.get("start_time") or ""
            if st_raw:
                try:
                    st_dt = _dt.fromisoformat(st_raw.replace("Z", "+00:00"))
                    if st_dt.tzinfo is None: st_dt = st_dt.replace(tzinfo=_tz.utc)
                    if st_dt.timestamp() < _now_ts - 90: continue
                except Exception: pass
            if jk: seen_jks.add(jk)
            if m.get("arb_opportunities") and not m.get("arbitrage"):
                m["arbitrage"] = m["arb_opportunities"]; m["has_arb"] = True
            matches_raw.append(m)
    except Exception: pass

    try:
        _sp_by_br: dict  = {}
        _sp_by_name: dict = {}
        for sp_m in sp_raw_list:
            br = str(sp_m.get("betradar_id") or "").strip()
            if br: _sp_by_br[br] = sp_m
            h = (sp_m.get("home_team") or "").lower().strip()[:10]
            a = (sp_m.get("away_team") or "").lower().strip()[:10]
            if h and a: _sp_by_name[f"{h}|{a}"] = sp_m

        def _find_sp_match(unified_m: dict):
            br = str(unified_m.get("betradar_id") or unified_m.get("join_key", "").replace("br_", "") or "").strip()
            if br and br in _sp_by_br: return _sp_by_br[br]
            h = (unified_m.get("home_team") or "").lower().strip()[:10]
            a = (unified_m.get("away_team") or "").lower().strip()[:10]
            if h and a: return _sp_by_name.get(f"{h}|{a}")
            return None

        for m in matches_raw:
            sp_m = _find_sp_match(m)
            if sp_m is None: continue
            if not m.get("sp_game_id") and sp_m.get("sp_game_id"): m["sp_game_id"] = sp_m["sp_game_id"]
            if not m.get("sms_id") and sp_m.get("sms_id"): m["sms_id"] = sp_m["sms_id"]
            sp_mkts = sp_m.get("markets") or {}
            if sp_mkts:
                if "bookmakers" not in m or not isinstance(m.get("bookmakers"), dict): m["bookmakers"] = {}
                if "sp" not in m["bookmakers"]: m["bookmakers"]["sp"] = {"bookmaker": "SportPesa", "slug": "sp", "markets": {}}
                existing_sp_mkts = m["bookmakers"]["sp"].get("markets") or {}
                if len(existing_sp_mkts) < len(sp_mkts): m["bookmakers"]["sp"]["markets"] = sp_mkts
            if sp_mkts and m.get("best") is not None:
                try:
                    from app.api.odds_stream import _normalise_markets, _get_price
                    norm_sp = _normalise_markets(sp_mkts)
                    for mkt, outcomes in norm_sp.items():
                        if not isinstance(outcomes, dict): continue
                        m["best"].setdefault(mkt, {})
                        for out, p in outcomes.items():
                            price = _get_price(p)
                            if price > 1.0:
                                existing = m["best"][mkt].get(out)
                                if not existing or price > existing.get("odd", 0):
                                    m["best"][mkt][out] = {"odd": price, "bk": "sp"}
                except Exception: pass
    except Exception: pass

    if not matches_raw:
        try:
            from app.api.odds_stream import _load_db_matches
            db_upcoming, _, _ = _load_db_matches(sport, mode="upcoming", page=1, per_page=500)
            db_live,     _, _ = _load_db_matches(sport, mode="live",     page=1, per_page=100)
            matches_raw.extend(db_upcoming + db_live)
        except Exception: pass

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
                            arb_markets.append({"market": mkt, "profit_pct": profit_pct, "arb_sum": round(arb_sum, 6), "legs": legs})
                    except Exception: pass
                arb_markets.sort(key=lambda x: -x["profit_pct"])
            if arb_markets:
                m["has_arb"] = True
                m["arbitrage"] = arb_markets
                m["best_arb_pct"] = arb_markets[0]["profit_pct"]

    matches = matches_raw
    if arb_only: matches = [m for m in matches if m.get("has_arb") or m.get("arbitrage")]
    matches.sort(key=lambda x: x.get("start_time") or "")

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

    sport_emojis = {
        "soccer": "⚽ SOCCER", "esoccer": "⚽ ESOCCER", "basketball": "🏀 BASKETBALL",
        "tennis": "🎾 TENNIS", "ice-hockey": "🏒 ICE HOCKEY", "volleyball": "🏐 VOLLEYBALL",
        "cricket": "🏏 CRICKET", "rugby": "🏉 RUGBY", "table-tennis": "🏓 TABLE TENNIS",
        "handball": "🤾 HANDBALL", "baseball": "⚾ BASEBALL", "mma": "🥊 MMA",
        "boxing": "🥊 BOXING", "darts": "🎯 DARTS", "american-football": "🏈 AMERICAN FOOTBALL"
    }
    sport_title = sport_emojis.get(sport.lower(), "🏆 " + sport.upper())
    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title.paragraph_format.space_before = Pt(0)
    title.paragraph_format.space_after = Pt(2)
    title_run = title.add_run(f"{sport_title} BETTING ANALYSIS")
    title_run.bold = True
    title_run.font.size = Pt(18)
    title_run.font.color.rgb = RGB_PRIMARY

    sub = doc.add_paragraph()
    sub.alignment = WD_ALIGN_PARAGRAPH.CENTER
    sub.paragraph_format.space_after = Pt(6)
    sub_run = sub.add_run(f"🗓️ {time.strftime('%A, %b %d, %Y (EAT)')}  |  📋 {len(matches)} Matches{'  [Arbitrage Only]' if arb_only else ''}")
    sub_run.font.size = Pt(9.5)
    sub_run.font.bold = True
    sub_run.font.color.rgb = RGB_MUTED

    p_legend = doc.add_paragraph()
    p_legend.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_legend.paragraph_format.space_after = Pt(18)
    r_lbl = p_legend.add_run("BOOKMAKERS:  ")
    r_lbl.font.size = SIZE_TINY
    r_lbl.font.bold = True
    r_lbl.font.color.rgb = RGB_MUTED
    r_sp_badge = p_legend.add_run(" SP ")
    r_sp_badge.font.size = SIZE_TINY
    r_sp_badge.font.bold = True
    r_sp_badge.font.color.rgb = RGB_SP
    r_sp_lbl = p_legend.add_run(" SportPesa  |  ")
    r_sp_lbl.font.size = SIZE_TINY
    r_sp_lbl.font.color.rgb = RGB_TEXT
    r_bt_badge = p_legend.add_run(" BT ")
    r_bt_badge.font.size = SIZE_TINY
    r_bt_badge.font.bold = True
    r_bt_badge.font.color.rgb = RGB_BT
    r_bt_lbl = p_legend.add_run(" Betika  |  ")
    r_bt_lbl.font.size = SIZE_TINY
    r_bt_lbl.font.color.rgb = RGB_TEXT
    r_od_badge = p_legend.add_run(" OD ")
    r_od_badge.font.size = SIZE_TINY
    r_od_badge.font.bold = True
    r_od_badge.font.color.rgb = RGB_OD
    r_od_lbl = p_legend.add_run(" OdiBets")
    r_od_lbl.font.size = SIZE_TINY
    r_od_lbl.font.color.rgb = RGB_TEXT

    p_div = doc.add_paragraph()
    p_div.paragraph_format.space_after = Pt(14)
    p_div_run = p_div.add_run("―" * 58)
    p_div_run.font.color.rgb = RGBColor(0xe2, 0xe8, 0xf0)

    if not matches:
        p_none = doc.add_paragraph()
        p_none.alignment = WD_ALIGN_PARAGRAPH.CENTER
        r_none = p_none.add_run("No matches matching filters found.")
        r_none.italic = True
    else:
        link_dict = {}
        br_ids = [m.get("betradar_id") or m.get("parent_match_id") for m in matches if m.get("betradar_id") or m.get("parent_match_id")]
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
                        if br_id: link_dict.setdefault(br_id, {})[slug] = bml.external_match_id
            except Exception as e:
                import logging; logging.warning("Error fetching match links for booklet: %s", e)

        _MARKET_NAMES = {
            "1x2": "Full-Time 1X2", "match_winner": "Match Winner", "moneyline": "Moneyline", "btts": "Both Teams to Score",
            "double_chance": "Double Chance", "dnb": "Draw No Bet", "half_time": "Half-Time Result", "ht_ft": "Half-Time / Full-Time",
            "correct_score": "Correct Score", "winner": "Winner", "total_goals": "Total Goals", "asian_handicap": "Asian Handicap",
            "european_handicap": "European Handicap", "odd_even": "Odd / Even Goals", "first_goal": "First Goal",
            "last_goal": "Last Goal", "anytime_scorer": "Anytime Goal-Scorer", "first_scorer": "First Goal-Scorer",
            "clean_sheet_home": "Home Clean Sheet", "clean_sheet_away": "Away Clean Sheet", "win_to_nil_home": "Home Win to Nil",
            "win_to_nil_away": "Away Win to Nil", "both_score_win": "BTTS & Win",
        }

        def _mkt_label(mkt: str) -> str:
            if mkt in _MARKET_NAMES: return _MARKET_NAMES[mkt]
            if mkt.startswith(("over_under_goals_", "over_under_")):
                line = mkt.replace("over_under_goals_", "").replace("over_under_", "").replace("_", ".")
                return f"Over/Under {line} Goals"
            if mkt.startswith("asian_handicap_"):
                line = mkt.replace("asian_handicap_", "").replace("_", ".")
                return f"AH {line}"
            return mkt.replace("_", " ").title()

        def _bk_display(slug: str) -> str:
            bk_map = {"sp": "SportPesa", "bt": "Betika", "od": "OdiBets"}
            return bk_map.get(slug.lower(), slug.upper())

        for idx, m in enumerate(matches):
            h_team = m.get("home_team", "Home Team")
            a_team = m.get("away_team", "Away Team")
            comp = m.get("competition", "General")
            time_raw = m.get("start_time", "")
            try:
                dt = _dt.fromisoformat(time_raw.replace("Z", "+00:00"))
                time_str = dt.strftime("%A, %b %d at %H:%M EAT")
            except Exception: time_str = time_raw

            p_match = doc.add_paragraph()
            p_match.paragraph_format.space_before = Pt(14)
            p_match.paragraph_format.space_after = Pt(2)
            p_match.paragraph_format.keep_with_next = True
            run_match = p_match.add_run(f"{idx+1}. {h_team} vs {a_team}")
            run_match.bold = True
            run_match.font.size = SIZE_MATCH_HEADER
            run_match.font.color.rgb = RGB_PRIMARY

            p_meta = doc.add_paragraph()
            p_meta.paragraph_format.space_after = Pt(8)
            p_meta.paragraph_format.keep_with_next = True
            br_id   = m.get("betradar_id") or m.get("parent_match_id")
            m_links = {}
            sp_gid = m.get("sms_id") or m.get("sp_game_id")
            if sp_gid: m_links["sp"] = str(sp_gid)
            if br_id and br_id in link_dict:
                for slug, ext_id in link_dict[br_id].items():
                    if slug not in m_links and ext_id: m_links[slug] = ext_id
            for slug in ("sp", "bt", "od"):
                if slug not in m_links:
                    val = (m.get("bk_ids") or {}).get(slug)
                    if val: m_links[slug] = str(val)
            ids_str = []
            if "sp" in m_links: ids_str.append(f"SportPesa: #{m_links['sp']}")
            if "bt" in m_links: ids_str.append(f"Betika: #{m_links['bt']}")
            if "od" in m_links: ids_str.append(f"OdiBets: #{m_links['od']}")
            ids_part = f"  |  📲 Game IDs: {', '.join(ids_str)}" if ids_str else ""
            r_meta = p_meta.add_run(f"🏆 {comp}  |  📅 {time_str}{ids_part}")
            r_meta.font.size = SIZE_TINY
            r_meta.font.color.rgb = RGB_MUTED

            grid_table = doc.add_table(rows=1, cols=2)
            _remove_table_borders(grid_table)
            grid_row = grid_table.rows[0]
            left_cell, right_cell = grid_row.cells[0], grid_row.cells[1]
            left_cell.width = Inches(4.6)
            right_cell.width = Inches(2.3)
            _set_cell_margins(left_cell, top=0, bottom=0, left=0, right=80)
            _set_cell_margins(right_cell, top=80, bottom=80, left=100, right=100)

            best_odds = m.get("best_odds") or m.get("best") or {}
            arbs = m.get("arbitrage") or []
            arb_markets = {arb.get("market") for arb in arbs if arb.get("market")}
            priority = ["1x2", "match_winner", "moneyline", "btts", "double_chance", "dnb", "half_time", "ht_ft", "total_goals", "winner"]
            ou_sorted = sorted([k for k in best_odds if k.startswith(("over_under_goals_", "over_under_"))], key=lambda k: float(k.replace("over_under_goals_", "").replace("over_under_", "").replace("_", ".")) if k.replace("over_under_goals_", "").replace("over_under_", "").replace("_", ".").replace(".", "", 1).isdigit() else 999)
            other_mkts = [k for k in best_odds if k not in priority and k not in ou_sorted]
            ordered_mkts = priority + ou_sorted + sorted(other_mkts)
            filtered_mkts = [mkt for mkt in ordered_mkts if mkt not in arb_markets]

            if filtered_mkts:
                nested_table = left_cell.add_table(rows=1, cols=7)
                nested_table.autofit = False
                col_widths = [Inches(1.15), Inches(0.55), Inches(0.50), Inches(0.50), Inches(0.50), Inches(0.65), Inches(0.75)]
                for row in nested_table.rows:
                    for c_idx, width in enumerate(col_widths): row.cells[c_idx].width = width
                hdr_cells = nested_table.rows[0].cells
                hdr_cells[0].text = 'Market'
                hdr_cells[1].text = 'Sel.'
                hdr_cells[2].text = 'SP'
                hdr_cells[3].text = 'BT'
                hdr_cells[4].text = 'OD'
                hdr_cells[5].text = 'Best Odd'
                hdr_cells[6].text = 'Best BK'
                for c_idx, cell in enumerate(hdr_cells):
                    p_hdr = cell.paragraphs[0]
                    p_hdr.alignment = WD_ALIGN_PARAGRAPH.LEFT if c_idx < 2 else WD_ALIGN_PARAGRAPH.CENTER
                    p_hdr.runs[0].font.bold = True
                    p_hdr.runs[0].font.size = SIZE_HEADER
                    p_hdr.runs[0].font.color.rgb = RGB_WHITE
                    _set_cell_shading(cell, HEX_PRIMARY_BG)
                    _set_cell_margins(cell, top=80, bottom=80, left=60, right=60)
                rows_added = 0
                for mkt in filtered_mkts[:12]:
                    mkt_data = best_odds.get(mkt)
                    if not mkt_data or not isinstance(mkt_data, dict): continue
                    label = _mkt_label(mkt)
                    for out in mkt_data.keys():
                        sp_odd = _get_float_odd(m.get("bookmakers", {}).get("sp", {}).get("markets", {}).get(mkt, {}).get(out))
                        bt_odd = _get_float_odd(m.get("bookmakers", {}).get("bt", {}).get("markets", {}).get(mkt, {}).get(out))
                        od_odd = _get_float_odd(m.get("bookmakers", {}).get("od", {}).get("markets", {}).get(mkt, {}).get(out))
                        if sp_odd is None and bt_odd is None and od_odd is None: continue
                        odds_list = [o for o in (sp_odd, bt_odd, od_odd) if o is not None]
                        best_val = max(odds_list) if odds_list else 0.0
                        best_bks_list = []
                        if sp_odd == best_val and best_val > 1.0: best_bks_list.append("SP")
                        if bt_odd == best_val and best_val > 1.0: best_bks_list.append("BT")
                        if od_odd == best_val and best_val > 1.0: best_bks_list.append("OD")
                        best_bks_str = " / ".join(best_bks_list)
                        row_cells = nested_table.add_row().cells
                        for c_idx, width in enumerate(col_widths): row_cells[c_idx].width = width
                        if rows_added % 2 == 1:
                            for cell in row_cells: _set_cell_shading(cell, HEX_ALT_ROW_BG)
                        for cell in row_cells:
                            _set_cell_border_bottom(cell, HEX_STD_BORDER, sz="2")
                            _set_cell_margins(cell, top=60, bottom=60, left=50, right=50)
                        _add_styled_paragraph(row_cells[0], label, size=SIZE_TEXT, color=RGB_TEXT)
                        _add_styled_paragraph(row_cells[1], str(out).upper(), bold=True, size=SIZE_TEXT, color=RGB_SECONDARY)
                        if sp_odd is not None:
                            is_best = (sp_odd == best_val and best_val > 1.0)
                            _add_styled_paragraph(row_cells[2], f"{sp_odd:.2f}", bold=is_best, size=SIZE_TEXT, color=RGB_GREEN if is_best else RGB_SP, align=WD_ALIGN_PARAGRAPH.CENTER)
                            if is_best: _set_cell_shading(row_cells[2], HEX_BEST_ODD_BG)
                        else: _add_styled_paragraph(row_cells[2], "—", size=SIZE_TEXT, color=RGB_MUTED, align=WD_ALIGN_PARAGRAPH.CENTER)
                        if bt_odd is not None:
                            is_best = (bt_odd == best_val and best_val > 1.0)
                            _add_styled_paragraph(row_cells[3], f"{bt_odd:.2f}", bold=is_best, size=SIZE_TEXT, color=RGB_GREEN if is_best else RGB_BT, align=WD_ALIGN_PARAGRAPH.CENTER)
                            if is_best: _set_cell_shading(row_cells[3], HEX_BEST_ODD_BG)
                        else: _add_styled_paragraph(row_cells[3], "—", size=SIZE_TEXT, color=RGB_MUTED, align=WD_ALIGN_PARAGRAPH.CENTER)
                        if od_odd is not None:
                            is_best = (od_odd == best_val and best_val > 1.0)
                            _add_styled_paragraph(row_cells[4], f"{od_odd:.2f}", bold=is_best, size=SIZE_TEXT, color=RGB_GREEN if is_best else RGB_OD, align=WD_ALIGN_PARAGRAPH.CENTER)
                            if is_best: _set_cell_shading(row_cells[4], HEX_BEST_ODD_BG)
                        else: _add_styled_paragraph(row_cells[4], "—", size=SIZE_TEXT, color=RGB_MUTED, align=WD_ALIGN_PARAGRAPH.CENTER)
                        _add_styled_paragraph(row_cells[5], f"{best_val:.2f}" if best_val > 0 else "—", bold=True, size=SIZE_TEXT, color=RGB_TEXT, align=WD_ALIGN_PARAGRAPH.CENTER)
                        _set_cell_shading(row_cells[5], HEX_BEST_ODD_BG)
                        _add_styled_paragraph(row_cells[6], best_bks_str, bold=True, size=SIZE_TINY, color=RGB_GREEN, align=WD_ALIGN_PARAGRAPH.CENTER)
                        rows_added += 1

                # Clean up left cell default paragraph if nested table was populated
                if rows_added > 0 and len(left_cell.paragraphs) > 1:
                    p0 = left_cell.paragraphs[0]
                    p0._element.getparent().remove(p0._element)
            else:
                _add_styled_paragraph(left_cell, "No odds comparison available for this match.", italic=True, size=SIZE_TEXT, color=RGB_MUTED)

            # ── 4. RIGHT COLUMN: Arbitrage Sidebar Card Box ───────────────────
            # Set styled border and color background depending on arb presence!
            if arbs:
                _set_cell_shading(right_cell, HEX_ARB_CARD_BG)
                _set_cell_borders(right_cell, color_hex=HEX_ARB_BORDER, sz="8")
                
                # Header
                _add_styled_paragraph(right_cell, "✅ ARBITRAGE ACTIVE", bold=True, color=RGB_GREEN, size=SIZE_HEADER, align=WD_ALIGN_PARAGRAPH.CENTER, space_after=4)
                
                # Active opportunity details
                best_arb = arbs[0]
                profit = float(best_arb.get("profit_pct", 0))
                
                _add_styled_paragraph(right_cell, f"Profit Margin: {profit:.2f}%", bold=True, color=RGB_ORANGE, size=SIZE_TEXT, align=WD_ALIGN_PARAGRAPH.CENTER, space_after=8)
                
                _add_styled_paragraph(right_cell, "Stake Distribution (2,000 KES):", bold=True, color=RGB_SECONDARY, size=SIZE_TINY, space_after=6)
                
                # Calculate optimal stakes based on KES 2,000 total bet
                total_stake = 2000.0
                legs = best_arb.get("legs", [])
                s_inv = sum(1.0 / float(leg.get("odd") or 1) for leg in legs)
                
                for leg in legs:
                    odd = float(leg.get("odd") or 1)
                    bk_slug = str(leg.get("bk") or "").lower()
                    
                    if s_inv > 0:
                        stake_pct = (1.0 / odd) / s_inv * 100
                        stake_amt = (1.0 / odd) / s_inv * total_stake
                    else:
                        stake_pct = 0.0
                        stake_amt = 0.0
                        
                    # BK color badge
                    p_leg = right_cell.add_paragraph()
                    p_leg.paragraph_format.space_after = Pt(4)
                    p_leg.paragraph_format.line_spacing = 1.0
                    
                    r_bk = p_leg.add_run(f"[{bk_slug.upper()}] ")
                    r_bk.bold = True
                    r_bk.font.size = SIZE_TINY
                    r_bk.font.color.rgb = RGB_SP if bk_slug == "sp" else (RGB_BT if bk_slug == "bt" else RGB_OD)
                    
                    r_det = p_leg.add_run(f"{stake_pct:.1f}% → ")
                    r_det.font.size = SIZE_TINY
                    r_det.font.color.rgb = RGB_TEXT
                    
                    r_amt = p_leg.add_run(f"KES {stake_amt:.0f}")
                    r_amt.bold = True
                    r_amt.font.size = SIZE_TINY
                    r_amt.font.color.rgb = RGB_TEXT
                    
                    r_odd = p_leg.add_run(f" @ {odd:.2f}")
                    r_odd.font.size = SIZE_TINY
                    r_odd.font.italic = True
                    r_odd.font.color.rgb = RGB_MUTED

                # Display Total Return
                p_ret = right_cell.add_paragraph()
                p_ret.paragraph_format.space_before = Pt(8)
                p_ret.paragraph_format.space_after = Pt(2)
                p_ret.alignment = WD_ALIGN_PARAGRAPH.CENTER
                
                ret_amt = total_stake * (1.0 / s_inv) if s_inv > 0 else total_stake
                r_ret = p_ret.add_run(f"Payout KES {ret_amt:.0f}")
                r_ret.bold = True
                r_ret.font.size = SIZE_HEADER
                r_ret.font.color.rgb = RGB_GREEN
                
            else:
                _set_cell_shading(right_cell, HEX_ALT_ROW_BG)
                _set_cell_borders(right_cell, color_hex=HEX_STD_BORDER, sz="6")
                
                # Standard Match Header
                _add_styled_paragraph(right_cell, "❌ NO ARBITRAGE", bold=True, color=RGB_SECONDARY, size=SIZE_HEADER, align=WD_ALIGN_PARAGRAPH.CENTER, space_after=6)
                
                _add_styled_paragraph(right_cell, "No guaranteed profit opportunities detected.", italic=True, size=SIZE_TINY, color=RGB_MUTED, align=WD_ALIGN_PARAGRAPH.CENTER, space_after=8)
                
                _add_styled_paragraph(right_cell, "📲 Quick Tip:", bold=True, color=RGB_PRIMARY, size=SIZE_TINY, space_after=2)
                
                _add_styled_paragraph(right_cell, "Bet on the Best Odds column highlighted in green to maximize your long term payout.", size=SIZE_TINY, color=RGB_TEXT, space_after=8)
                
                _add_styled_paragraph(right_cell, "🔄 Dynamic Updates:", bold=True, color=RGB_PRIMARY, size=SIZE_TINY, space_after=2)
                
                _add_styled_paragraph(right_cell, "All comparisons are refreshed and pre-built every 10 minutes to guarantee data freshness.", size=SIZE_TINY, color=RGB_TEXT)
            
            # Clean up right cell first empty paragraph if needed
            if len(right_cell.paragraphs) > 1 and not right_cell.paragraphs[0].text:
                p_to_del = right_cell.paragraphs[0]
                p_to_del._element.getparent().remove(p_to_del._element)

            # Spacer between matches
            doc.add_paragraph().paragraph_format.space_after = Pt(12)

    # ── 8. Footer Legend ───────────────────────────────────────────────────
    p_foot = doc.add_paragraph()
    p_foot.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p_foot.paragraph_format.space_before = Pt(24)
    r_foot = p_foot.add_run("Arbitrage opportunities are calculated based on the best available odds across the selected bookmakers.")
    r_foot.font.size = SIZE_TINY
    r_foot.font.italic = True
    r_foot.font.color.rgb = RGB_MUTED

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