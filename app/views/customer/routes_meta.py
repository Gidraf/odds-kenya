import time
from flask import request
from .blueprint import bp_odds_customer
from . import config
from .utils import _normalise_sport_slug, _bk_slug, _get_country_weight, _now_utc

@bp_odds_customer.route("/odds/sports")
def list_sports():
    from app.models.odds import UnifiedMatch
    from sqlalchemy import func
    from app.workers.celery_tasks import cache_keys
    from app.utils.customer_jwt_helpers import _signed_response
    sports = {}
    try:
        for row in (UnifiedMatch.query.with_entities(UnifiedMatch.sport_name, func.count(UnifiedMatch.id)).group_by(UnifiedMatch.sport_name).all()):
            if row[0]: sports[_normalise_sport_slug(row[0])] = sports.get(_normalise_sport_slug(row[0]), 0) + row[1]
    except Exception: pass
    for prefix in config._CACHE_PREFIXES:
        for k in cache_keys(f"{prefix}:upcoming:*"):
            if len(k.split(":")) >= 3: sports.setdefault(_normalise_sport_slug(k.split(":")[2].replace("_", " ")), 0)
    return _signed_response({"ok": True, "sports": sorted(sports.keys()), "counts": sports})

@bp_odds_customer.route("/odds/bookmakers")
def list_bookmakers():
    from app.models.bookmakers_model import Bookmaker
    from app.models.odds import BookmakerMatchOdds
    from sqlalchemy import func
    from app.utils.customer_jwt_helpers import _signed_response
    bk_counts = dict(BookmakerMatchOdds.query.with_entities(BookmakerMatchOdds.bookmaker_id, func.count(BookmakerMatchOdds.match_id)).group_by(BookmakerMatchOdds.bookmaker_id).all())
    result = [{"id": bm.id, "name": bm.name, "slug": _bk_slug(bm.name), "domain": bm.domain, "is_active": bm.is_active, "match_count": bk_counts.get(bm.id, 0), "group": "local" if _bk_slug(bm.name) in ("sp", "bt", "od") else "international"} for bm in Bookmaker.query.filter_by(is_active=True).order_by(Bookmaker.name).all()]
    return _signed_response({"ok": True, "bookmakers": result, "total": len(result)})

@bp_odds_customer.route("/odds/markets")
def list_markets():
    from app.utils.customer_jwt_helpers import _err, _signed_response
    try:
        from app.models.odds import MarketDefinition
        return _signed_response({"ok": True, "markets": [m.to_dict() for m in MarketDefinition.query.order_by(MarketDefinition.name).all()]})
    except Exception as exc: return _err(str(exc), 500)

@bp_odds_customer.route("/odds/status")
def harvest_status():
    from app.workers.celery_tasks import cache_get
    from app.utils.customer_jwt_helpers import _signed_response
    heartbeat = cache_get("worker_heartbeat") or {}
    now = _now_utc()
    try:
        from app.models.odds import UnifiedMatch, BookmakerMatchOdds
        from app.models.bookmakers_model import Bookmaker
        from sqlalchemy import func
        from app.extensions import db
        total_db = UnifiedMatch.query.count()
        live_count = UnifiedMatch.query.filter(UnifiedMatch.start_time <= now, UnifiedMatch.start_time > now - config._LIVE_WINDOW, UnifiedMatch.status.notin_(list(config._TERMINAL_STATUSES))).count()
        upcoming_count = UnifiedMatch.query.filter(UnifiedMatch.start_time > now, UnifiedMatch.status.notin_(list(config._TERMINAL_STATUSES))).count()
        bk_cov = dict(db.session.query(BookmakerMatchOdds.bookmaker_id, func.count(BookmakerMatchOdds.match_id)).group_by(BookmakerMatchOdds.bookmaker_id).all())
        bk_names = {b.id: b.name for b in Bookmaker.query.all()}
        coverage = [{"bookmaker": bk_names.get(bk_id, str(bk_id)), "match_count": cnt, "coverage_pct": round(cnt / total_db * 100, 1) if total_db else 0} for bk_id, cnt in bk_cov.items()]
    except Exception: total_db = live_count = upcoming_count = 0; coverage = []
    return _signed_response({"ok": True, "free_access": config.FREE_ACCESS, "worker_alive": heartbeat.get("alive", False), "last_heartbeat": heartbeat.get("checked_at"), "db_match_count": total_db, "live_count": live_count, "upcoming_count": upcoming_count, "live_window_minutes": int(config._LIVE_WINDOW.total_seconds() / 60), "server_time": now.isoformat(), "bookmaker_coverage": coverage, "source": "postgresql"})

@bp_odds_customer.route("/odds/access")
def get_access_config():
    from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
    user = _current_user_from_header()
    if not user or not getattr(user, "is_admin", False): return _err("Admin only", 403)
    return _signed_response({"ok": True, "free_access": config.FREE_ACCESS, "endpoint_access": config._ENDPOINT_ACCESS})

@bp_odds_customer.route("/admin/odds/access", methods=["POST"])
def set_access_config():
    from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
    user = _current_user_from_header()
    if not user or not getattr(user, "is_admin", False): return _err("Admin only", 403)
    body = request.get_json(force=True) or {}
    if "free_access" in body: config.FREE_ACCESS = bool(body["free_access"])
    if isinstance(body.get("endpoint_access"), dict):
        for ep, tier in body["endpoint_access"].items():
            if ep in config._ENDPOINT_ACCESS and tier in {"free", "basic", "pro", "premium"}: config._ENDPOINT_ACCESS[ep] = tier
    return _signed_response({"ok": True, "free_access": config.FREE_ACCESS, "endpoint_access": config._ENDPOINT_ACCESS})

@bp_odds_customer.route("/odds/navigation")
def get_navigation_tree():
    from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
    from app.models.odds import UnifiedMatch
    from app.extensions import db
    from sqlalchemy import func
    t0 = time.perf_counter()
    user = _current_user_from_header()
    now = _now_utc()
    
    try:
        rows = db.session.query(UnifiedMatch.sport_name, UnifiedMatch.competition_name, func.count(UnifiedMatch.id)).filter(UnifiedMatch.start_time > now).filter(UnifiedMatch.status.notin_(list(config._TERMINAL_STATUSES))).group_by(UnifiedMatch.sport_name, UnifiedMatch.competition_name).all()
        nav_tree = {}
        for sport, comp, count in rows:
            if not sport or not comp: continue
            sport_slug = _normalise_sport_slug(sport)
            category = "International"
            comp_clean = comp
            
            if " - " in comp: parts = comp.split(" - ", 1); category = parts[0].strip(); comp_clean = parts[1].strip()
            elif ": " in comp: parts = comp.split(": ", 1); category = parts[0].strip(); comp_clean = parts[1].strip()
            
            if sport_slug == "tennis":
                comp_upper = comp.upper()
                if "ATP" in comp_upper: category = "ATP"
                elif "WTA" in comp_upper: category = "WTA"
                elif "ITF" in comp_upper: category = "ITF"
                elif "CHALLENGER" in comp_upper: category = "Challenger"
            else: category = category.title()
            
            if sport_slug not in nav_tree: nav_tree[sport_slug] = {"sport": sport, "slug": sport_slug, "count": 0, "categories": {}}
            nav_tree[sport_slug]["count"] += count
            if category not in nav_tree[sport_slug]["categories"]: nav_tree[sport_slug]["categories"][category] = {"name": category, "weight": _get_country_weight(sport_slug, category), "count": 0, "competitions": {}}
            nav_tree[sport_slug]["categories"][category]["count"] += count
            comps_dict = nav_tree[sport_slug]["categories"][category]["competitions"]
            if comp_clean not in comps_dict: comps_dict[comp_clean] = {"name": comp_clean, "original_name": comp, "count": 0}
            comps_dict[comp_clean]["count"] += count
            
        sorted_sports = []
        for s_slug, s_data in nav_tree.items():
            sorted_categories = []
            for c_name, c_data in s_data["categories"].items():
                comps_list = list(c_data["competitions"].values())
                comps_list.sort(key=lambda x: (-x["count"], x["name"]))
                c_data["competitions"] = comps_list
                sorted_categories.append(c_data)
            sorted_categories.sort(key=lambda x: (x["weight"], -x["count"], x["name"]))
            sorted_sports.append({"slug": s_slug, "name": s_data["sport"], "count": s_data["count"], "categories": sorted_categories})
        
        sorted_sports.sort(key=lambda x: -x["count"])
        return _signed_response({"ok": True, "navigation": sorted_sports, "latency_ms": int((time.perf_counter() - t0) * 1000)}, encrypt_for=user)
    except Exception as exc: return _err(str(exc), 500)