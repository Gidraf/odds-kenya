from datetime import datetime, timezone, timedelta
from . import config
from .utils import _now_utc, _normalise_sport_slug, _is_upcoming_safe, _is_available
from .formatters import _build_match_dict

def _get_analytics(betradar_id: str | None, trigger_if_missing: bool = True) -> dict:
    if not betradar_id: return {}
    from app.workers.celery_tasks import cache_get, celery
    bundle = cache_get(f"sr:analytics:{betradar_id}") or {}
    if trigger_if_missing and not bundle.get("available"):
        try: celery.send_task("tasks.sp.get_match_analytics", args=[betradar_id, False], queue="harvest", countdown=0)
        except Exception: pass
    return bundle

def _fetch_analytics_map(um_list: list, include: bool) -> dict[str, dict]:
    if not include: return {}
    from app.workers.celery_tasks import cache_get
    result = {}
    for br_id in [um.parent_match_id for um in um_list if um.parent_match_id]:
        bundle = cache_get(f"sr:analytics:{br_id}")
        if bundle: result[br_id] = bundle
    return result

def _sport_filter(q, sport_slug: str):
    from sqlalchemy import or_
    from app.models.odds import UnifiedMatch
    if not sport_slug or sport_slug.lower() in ("all", ""): return q
    canonical = _normalise_sport_slug(sport_slug)
    db_names = config._SPORT_ALIASES.get(canonical, [sport_slug])
    if len(db_names) == 1: return q.filter(UnifiedMatch.sport_name == db_names[0])
    return q.filter(or_(*[UnifiedMatch.sport_name == n for n in db_names]))

def _mode_time_filter(q, mode: str):
    from sqlalchemy import or_
    from app.models.odds import UnifiedMatch
    now = _now_utc()
    if mode == "upcoming": return q.filter(UnifiedMatch.status.notin_(list(config._EXCLUDE_FROM_UPCOMING)), UnifiedMatch.start_time.isnot(None), UnifiedMatch.start_time > now)
    elif mode == "live": return q.filter(UnifiedMatch.start_time <= now, UnifiedMatch.start_time > now - config._LIVE_WINDOW, UnifiedMatch.status.notin_(list(config._TERMINAL_STATUSES)))
    elif mode == "finished": return q.filter(or_(UnifiedMatch.start_time <= now - config._LIVE_WINDOW, UnifiedMatch.status.in_(list(config._TERMINAL_STATUSES))))
    return q

def _multi_bk_filter(q):
    from app.extensions import db
    from app.models.odds import UnifiedMatch, BookmakerMatchOdds
    from sqlalchemy import func
    bk_count_sq = db.session.query(BookmakerMatchOdds.match_id, func.count(BookmakerMatchOdds.bookmaker_id.distinct()).label("bk_count")).group_by(BookmakerMatchOdds.match_id).having(func.count(BookmakerMatchOdds.bookmaker_id.distinct()) >= config.MIN_BOOKMAKERS).subquery()
    return q.join(bk_count_sq, UnifiedMatch.id == bk_count_sq.c.match_id)

def _build_base_query(sport_slug, mode, comp_filter, team_filter, date_str, from_dt, to_dt, sort, apply_multi_bk: bool = True):
    from sqlalchemy import or_
    from app.models.odds import UnifiedMatch
    q = UnifiedMatch.query
    q = _sport_filter(q, sport_slug)
    q = _mode_time_filter(q, mode)
    if apply_multi_bk and mode in ("upcoming", "live"): q = _multi_bk_filter(q)

    if comp_filter: q = q.filter(UnifiedMatch.competition_name.ilike(f"%{comp_filter}%"))
    if team_filter: q = q.filter(or_(UnifiedMatch.home_team_name.ilike(f"%{team_filter}%"), UnifiedMatch.away_team_name.ilike(f"%{team_filter}%")))
    if date_str and mode == "finished":
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            q = q.filter(UnifiedMatch.start_time >= day, UnifiedMatch.start_time < day + timedelta(days=1))
        except ValueError: pass
    if from_dt:
        try: q = q.filter(UnifiedMatch.start_time >= datetime.fromisoformat(from_dt.replace("Z", "+00:00")))
        except Exception: pass
    if to_dt:
        try: q = q.filter(UnifiedMatch.start_time <= datetime.fromisoformat(to_dt.replace("Z", "+00:00")))
        except Exception: pass

    sort_col = {"start_time": UnifiedMatch.start_time, "home_team": UnifiedMatch.home_team_name, "competition": UnifiedMatch.competition_name}.get(sort, UnifiedMatch.start_time)
    return q.order_by(sort_col.asc())

def _fetch_batch_data(match_ids: list[int]):
    from app.models.odds import BookmakerMatchOdds, ArbitrageOpportunity
    from app.models.bookmakers_model import Bookmaker, BookmakerMatchLink
    bmo_rows = BookmakerMatchOdds.query.filter(BookmakerMatchOdds.match_id.in_(match_ids)).all()
    all_bk_ids = {bmo.bookmaker_id for bmo in bmo_rows}
    bk_objs = {b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(all_bk_ids)).all()} if all_bk_ids else {}
    
    links_by_match = {}
    for lnk in BookmakerMatchLink.query.filter(BookmakerMatchLink.match_id.in_(match_ids)).all():
        links_by_match.setdefault(lnk.match_id, {})[lnk.bookmaker_id] = lnk.to_dict()
        
    arb_set = set()
    try: arb_set = {r.match_id for r in ArbitrageOpportunity.query.filter(ArbitrageOpportunity.match_id.in_(match_ids), ArbitrageOpportunity.status == "OPEN").with_entities(ArbitrageOpportunity.match_id).all()}
    except Exception: pass
    return bmo_rows, bk_objs, links_by_match, arb_set

def _load_db_matches(sport_slug, mode="upcoming", page=1, per_page=20, comp_filter="", team_filter="", has_arb=False, sort="start_time", date_str="", from_dt="", to_dt="", include_analytics=False):
    q = _build_base_query(sport_slug, mode, comp_filter, team_filter, date_str, from_dt, to_dt, sort)
    total = q.count()
    um_list = q.offset((page - 1) * per_page).limit(per_page).all()
    if not um_list: return [], total, max(1, (total + per_page - 1) // per_page)

    bmo_rows, bk_objs, links_by_match, arb_set = _fetch_batch_data([um.id for um in um_list])
    analytics_map = _fetch_analytics_map(um_list, include_analytics)

    bmo_by_match = {}
    for bmo in bmo_rows: bmo_by_match.setdefault(bmo.match_id, []).append(bmo)

    result = []
    for um in um_list:
        db_st = getattr(um, "status", None)
        if mode == "upcoming" and not _is_upcoming_safe(db_st, um.start_time): total -= 1; continue
        elif mode == "live" and not _is_available(db_st, um.start_time): total -= 1; continue
            
        d = _build_match_dict(um, bmo_by_match.get(um.id, []), bk_objs, links_by_match, arb_set, sport_slug, analytics_map=analytics_map)

        if mode in ("upcoming", "live") and d["bk_count"] < config.MIN_BOOKMAKERS: total -= 1; continue
        if has_arb and not d["has_arb"]: total -= 1; continue
        result.append(d)
    return result, total, max(1, (total + per_page - 1) // per_page)