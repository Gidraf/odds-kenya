"""
app/views/odds_feed/odds_routes.py
===================================
Smart status logic:
  - upcoming  → start_time > now
  - live      → start_time <= now AND start_time > now - 2h30m
  - finished  → start_time <= now - 2h30m OR status == FINISHED

Terminal statuses (never returned in live/upcoming):
  FINISHED, CANCELLED, POSTPONED, SUSPENDED

Streaming endpoints:
  GET /api/odds/stream/upcoming/<sport>
  GET /api/odds/stream/live/<sport>
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Generator
from zoneinfo import ZoneInfo

from flask import Blueprint, Response, request, stream_with_context

from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
from app.utils.decorators_ import log_event

bp_odds_customer = Blueprint("web", __name__, url_prefix="/api")

FREE_ACCESS: bool = True
_ENDPOINT_ACCESS: dict[str, str] = {
    "get_upcoming": "free", "get_live": "free",
    "get_results": "free", "get_results_by_date": "free",
    "get_match": "free", "search_matches": "free",
    "list_sports": "free", "list_bookmakers": "free",
    "list_markets": "free", "harvest_status": "free",
    "get_match_analytics": "free",
}
FREE_MATCH_LIMIT = 1000
_WS_CHANNEL      = "odds:updates"
_ARB_CHANNEL     = "arb:updates"
_EV_CHANNEL      = "ev:updates"
_CACHE_PREFIXES  = ["sbo", "sp", "bt", "od", "b2b", "bt_od"]
_STREAM_BATCH    = 20
MIN_BOOKMAKERS   = 2
_LIVE_WINDOW     = timedelta(hours=2, minutes=30)

_TERMINAL_STATUSES = frozenset({"FINISHED", "CANCELLED", "POSTPONED", "SUSPENDED"})

_EXCLUDE_FROM_UPCOMING = frozenset({
    "FINISHED", "CANCELLED", "POSTPONED", "SUSPENDED",
    "IN_PLAY", "LIVE", "INPLAY", "IN PLAY",
})

_BK_SLUG: dict[str, str] = {
    "sportpesa": "sp", "betika": "bt", "odibets": "od",
    "sp": "sp", "bt": "bt", "od": "od", "sbo": "sbo", "b2b": "b2b",
}

_SPORT_ALIASES: dict[str, list[str]] = {
    "soccer":            ["Soccer", "Football"],
    "football":          ["Soccer", "Football"],
    "basketball":        ["Basketball"],
    "tennis":            ["Tennis"],
    "ice-hockey":        ["Ice Hockey"],
    "volleyball":        ["Volleyball"],
    "cricket":           ["Cricket"],
    "rugby":             ["Rugby"],
    "table-tennis":      ["Table Tennis"],
    "handball":          ["Handball"],
    "mma":               ["MMA"],
    "boxing":            ["Boxing"],
    "darts":             ["Darts"],
    "esoccer":           ["eSoccer", "eFootball"],
    "baseball":          ["Baseball"],
    "american-football": ["American Football"],
}

_CANONICAL_SLUG: dict[str, str] = {
    "Football": "soccer",      "football": "soccer",
    "Soccer":   "soccer",      "soccer":   "soccer",
    "Ice Hockey":   "ice-hockey", "ice hockey":   "ice-hockey", "ice-hockey": "ice-hockey",
    "Table Tennis": "table-tennis","table tennis": "table-tennis","table-tennis":"table-tennis",
    "Basketball": "basketball", "Tennis": "tennis",
    "Cricket": "cricket",    "Volleyball": "volleyball",
    "Rugby":   "rugby",      "Handball": "handball",
    "MMA":     "mma",        "Boxing": "boxing",
    "Darts":   "darts",      "eSoccer": "esoccer", "eFootball": "esoccer",
    "Baseball": "baseball",  "baseball": "baseball",
    "American Football": "american-football", "american football": "american-football", "american-football": "american-football",
}

_SSE_HEADERS = {
    "Content-Type":               "text/event-stream",
    "Cache-Control":              "no-cache",
    "X-Accel-Buffering":          "no",
    "Access-Control-Allow-Origin": "*",
    "Connection":                 "keep-alive",
}


# ══════════════════════════════════════════════════════════════════════════════
# TIME-BASED STATUS HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _now_utc() -> datetime:
    # CRITICAL FIX: Base math MUST use actual UTC to compare against DB UTC records
    return datetime.now(timezone.utc)


def _effective_status(db_status: str | None, start_time: datetime | None) -> str:
    if db_status in _TERMINAL_STATUSES:
        return db_status
    if start_time is None:
        return db_status or "PRE_MATCH"
        
    st = start_time
    if st.tzinfo is None:
        st = st.replace(tzinfo=timezone.utc)
        
    now = _now_utc()
    if st > now:
        return "PRE_MATCH"
    if now - st <= _LIVE_WINDOW:
        return "IN_PLAY"
    return "FINISHED"


def _is_live(db_status, start_time) -> bool:
    return _effective_status(db_status, start_time) == "IN_PLAY"


def _is_upcoming(db_status, start_time) -> bool:
    return _effective_status(db_status, start_time) == "PRE_MATCH"


def _is_finished(db_status, start_time) -> bool:
    return _effective_status(db_status, start_time) in _TERMINAL_STATUSES


def _is_available(db_status, start_time) -> bool:
    return _effective_status(db_status, start_time) in ("PRE_MATCH", "IN_PLAY")


def _is_upcoming_safe(db_status, start_time) -> bool:
    if (db_status or "").upper() in _EXCLUDE_FROM_UPCOMING:
        return False
    if not start_time:
        return False
    # Secondary aggressive time check
    st = start_time if start_time.tzinfo else start_time.replace(tzinfo=timezone.utc)
    if st <= _now_utc():
        return False
    return _effective_status(db_status, start_time) == "PRE_MATCH"


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get_analytics(betradar_id: str | None, trigger_if_missing: bool = True) -> dict:
    if not betradar_id:
        return {}
    from app.workers.celery_tasks import cache_get, celery
    bundle = cache_get(f"sr:analytics:{betradar_id}") or {}
    if trigger_if_missing and not bundle.get("available"):
        try:
            celery.send_task(
                "tasks.sp.get_match_analytics",
                args=[betradar_id, False],
                queue="harvest",
                countdown=0,
            )
        except Exception:
            pass
    return bundle


def _build_analytics_summary(analytics: dict) -> dict:
    if not analytics or not analytics.get("available"):
        return {"available": False}
    match      = analytics.get("match") or {}
    teams      = match.get("teams") or {}
    home_t     = teams.get("home") or {}
    away_t     = teams.get("away") or {}
    tournament = match.get("tournament") or {}
    season     = match.get("season") or {}
    stadium    = match.get("stadium") or {}
    coverage   = match.get("statscoverage") or {}
    home_next  = analytics.get("home_next") or {}
    away_next  = analytics.get("away_next") or {}

    tennis_info: dict = {}
    t_info = tournament.get("tennisinfo") or {}
    if t_info:
        prize = t_info.get("prize") or {}
        tennis_info = {
            "surface":         (match.get("ground") or {}).get("name"),
            "tournament_level": tournament.get("tournamentlevelname"),
            "prize_amount":  prize.get("amount"),
            "prize_currency": prize.get("currency"),
            "gender":         t_info.get("gender"),
            "event_type":    t_info.get("type"),
        }
    active_coverage = [k for k, v in coverage.items() if v is True]

    return {
        "available":           True,
        "sr_match_id":         analytics.get("sr_match_id"),
        "tournament":          tournament.get("name"),
        "tournament_level":    tournament.get("tournamentlevelname"),
        "season":              season.get("name"),
        "season_year":         season.get("year"),
        "stadium":             stadium.get("name"),
        "stadium_city":        stadium.get("city"),
        "stadium_country":     stadium.get("country"),
        "coverage_flags":      active_coverage,
        "home_uid":            home_t.get("uid"),
        "away_uid":            away_t.get("uid"),
        "home_next_count":     len(home_next.get("matches") or []),
        "away_next_count":     len(away_next.get("matches") or []),
        "tennis":              tennis_info if tennis_info else None,
    }

def _build_analytics_full(analytics: dict) -> dict:
    if not analytics or not analytics.get("available"):
        return {"available": False}
    # [Analytics Full builder kept identical]
    match      = analytics.get("match") or {}
    home_next  = analytics.get("home_next") or {}
    away_next  = analytics.get("away_next") or {}
    sf         = analytics.get("season_fixtures") or {}
    teams      = match.get("teams") or {}
    tournament = match.get("tournament") or {}
    season     = match.get("season") or {}
    stadium    = match.get("stadium") or {}
    coverage   = match.get("statscoverage") or {}
    t_info     = tournament.get("tennisinfo") or {}

    def _fmt_match(m: dict) -> dict:
        home = (m.get("teams") or {}).get("home") or {}
        away = (m.get("teams") or {}).get("away") or {}
        t    = m.get("time") or {}
        rn   = m.get("roundname") or {}
        g    = m.get("ground") or {}
        res  = m.get("result") or {}
        return {
            "sr_match_id": m.get("_id"), "home": home.get("name"), "away": away.get("name"),
            "date": t.get("date"), "time": t.get("time"), "tz": t.get("tz"), "uts": t.get("uts"),
            "round": rn.get("name"), "surface": g.get("name"),
            "result": {"home": res.get("home"), "away": res.get("away"), "winner": res.get("winner")} if any(v is not None for v in [res.get("home"), res.get("away")]) else None,
            "status": m.get("status"), "canceled": m.get("canceled"), "postponed": m.get("postponed"),
            "walkover": m.get("walkover"), "retired": m.get("retired"), "bestof": m.get("bestof"),
        }

    def _fmt_team(t: dict) -> dict:
        cc = t.get("cc") or {}
        return {"uid": t.get("uid"), "name": t.get("name"), "abbr": t.get("abbr"), "country": cc.get("name"), "cc_a2": cc.get("a2")}

    prize_info: dict | None = None
    if t_info:
        prize = t_info.get("prize") or {}
        prize_info = {"amount": prize.get("amount"), "currency": prize.get("currency")}

    return {
        "available":       True,
        "sr_match_id":     analytics.get("sr_match_id"),
        "tournament": {
            "name": tournament.get("name"), "level": tournament.get("tournamentlevelname"),
            "gender": t_info.get("gender"), "type": t_info.get("type"), "prize": prize_info,
            "surface": (tournament.get("ground") or {}).get("name"), "start": t_info.get("start"),
            "end": t_info.get("end"), "qualification": t_info.get("qualification"),
            "city": t_info.get("city"), "country": t_info.get("country"),
        },
        "season": {"id": season.get("_id"), "name": season.get("name"), "year": season.get("year"), "start": (season.get("start") or {}).get("date"), "end": (season.get("end") or {}).get("date")},
        "venue": {"name": stadium.get("name"), "city": stadium.get("city"), "country": stadium.get("country"), "capacity": stadium.get("capacity")},
        "surface": (match.get("ground") or {}).get("name"),
        "home_team": _fmt_team(teams.get("home") or {}), "away_team": _fmt_team(teams.get("away") or {}),
        "coverage": {k: v for k, v in coverage.items() if isinstance(v, bool)},
        "home_next": {"team": _fmt_team(home_next.get("team") or {}), "matches": [_fmt_match(m) for m in (home_next.get("matches") or [])]},
        "away_next": {"team": _fmt_team(away_next.get("team") or {}), "matches": [_fmt_match(m) for m in (away_next.get("matches") or [])]},
        "season_fixtures": {"total": len(sf.get("matches") or []), "matches": [_fmt_match(m) for m in (sf.get("matches") or [])], "cups": list((sf.get("cups") or {}).keys())} if sf else None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# QUERY HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_sport_slug(raw: str) -> str:
    if not raw:
        return raw
    return _CANONICAL_SLUG.get(raw, raw.lower().replace(" ", "-"))

def _sport_filter(q, sport_slug: str):
    from sqlalchemy import or_
    from app.models.odds import UnifiedMatch
    if not sport_slug or sport_slug.lower() in ("all", ""):
        return q
    canonical = _normalise_sport_slug(sport_slug)
    db_names  = _SPORT_ALIASES.get(canonical, [sport_slug])
    if len(db_names) == 1:
        return q.filter(UnifiedMatch.sport_name == db_names[0])
    return q.filter(or_(*[UnifiedMatch.sport_name == n for n in db_names]))

def _mode_time_filter(q, mode: str):
    from app.models.odds import UnifiedMatch
    from sqlalchemy import or_, and_
    
    now         = _now_utc()
    live_cutoff = now - _LIVE_WINDOW

    _no_upcoming = list(_EXCLUDE_FROM_UPCOMING)
    _dead        = list(_TERMINAL_STATUSES)

    if mode == "upcoming":
        # CRITICAL FIX: Enforce start_time > now strictly in DB layer
        return q.filter(
            UnifiedMatch.status.notin_(_no_upcoming),
            UnifiedMatch.start_time.isnot(None), 
            UnifiedMatch.start_time > now
        )
    elif mode == "live":
        return q.filter(
            UnifiedMatch.start_time <= now,
            UnifiedMatch.start_time >  live_cutoff,
            UnifiedMatch.status.notin_(_dead),
        )
    elif mode == "finished":
        return q.filter(or_(
            UnifiedMatch.start_time <= live_cutoff,
            UnifiedMatch.status.in_(list(_TERMINAL_STATUSES)),
        ))
    return q

def _multi_bk_filter(q):
    from app.models.odds import BookmakerMatchOdds, UnifiedMatch
    from app.extensions import db
    from sqlalchemy import func
    bk_count_sq = (
        db.session.query(
            BookmakerMatchOdds.match_id,
            func.count(BookmakerMatchOdds.bookmaker_id.distinct()).label("bk_count"),
        )
        .group_by(BookmakerMatchOdds.match_id)
        .having(func.count(BookmakerMatchOdds.bookmaker_id.distinct()) >= MIN_BOOKMAKERS)
        .subquery()
    )
    return q.join(bk_count_sq, UnifiedMatch.id == bk_count_sq.c.match_id)


def _bk_slug(name: str) -> str:
    return _BK_SLUG.get(name.lower(), name.lower()[:4])


def _flatten_db_markets(raw_markets: dict) -> dict:
    flat: dict = {}
    for mkt_slug, spec_dict in (raw_markets or {}).items():
        if not isinstance(spec_dict, dict):
            flat[mkt_slug] = spec_dict
            continue
        outcomes: dict = {}
        for spec_val, inner in spec_dict.items():
            if isinstance(inner, dict):
                for out_key, out_val in inner.items():
                    outcomes[out_key] = out_val
            else:
                outcomes[spec_val] = inner
        flat[mkt_slug] = outcomes
    return flat


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _keepalive() -> str:
    return ": ping\n\n"

# ══════════════════════════════════════════════════════════════════════════════
# SHARED MATCH BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _build_match_dict(um, bmos, bk_objs, links_by_match, arb_set, sport_slug, analytics_map=None) -> dict:
    bookmakers:    dict[str, dict] = {}
    markets_by_bk: dict[str, dict] = {}

    for bmo in bmos:
        bk_obj   = bk_objs.get(bmo.bookmaker_id)
        bk_name  = (bk_obj.name if bk_obj else str(bmo.bookmaker_id)).lower()
        slug     = _bk_slug(bk_name)
        label    = bk_obj.name if bk_obj else slug.upper()
        mkt_data = _flatten_db_markets(bmo.markets_json or {})
        if not mkt_data:
            continue
        if slug in bookmakers:
            bookmakers[slug]["markets"].update(mkt_data)
            bookmakers[slug]["market_count"] = len(bookmakers[slug]["markets"])
            markets_by_bk[slug].update(mkt_data)
        else:
            bookmakers[slug] = {
                "bookmaker_id": bmo.bookmaker_id, "bookmaker": label, "slug": slug,
                "markets": mkt_data, "market_count": len(mkt_data),
                "link": links_by_match.get(um.id, {}).get(bmo.bookmaker_id),
            }
            markets_by_bk[slug] = mkt_data

    best: dict[str, dict] = {}
    for sl, bk_mkts in markets_by_bk.items():
        for mkt, outcomes in bk_mkts.items():
            best.setdefault(mkt, {})
            for out, odd_data in (outcomes or {}).items():
                try:
                    fv = float(odd_data.get("price") or odd_data.get("odd") or 0) if isinstance(odd_data, dict) else float(odd_data)
                except (TypeError, ValueError):
                    continue
                if fv <= 1.0:
                    continue
                if out not in best[mkt] or fv > best[mkt][out]["odd"]:
                    best[mkt][out] = {"odd": fv, "bk": sl}

    best_odds = {mkt: {out: {"odd": v["odd"], "bookie": v["bk"]} for out, v in outs.items()} for mkt, outs in best.items()}
    arb_markets: list[dict] = []
    if len(bookmakers) >= 2:
        for mkt, outcomes in best.items():
            if len(outcomes) < 2:
                continue
            arb_sum = sum(1.0 / v["odd"] for v in outcomes.values())
            if arb_sum < 1.0:
                profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
                legs = [{"outcome": o, "bk": v["bk"], "odd": v["odd"]} for o, v in outcomes.items()]
                breakdown = [{
                    **leg, "stake_pct": round((1.0 / leg["odd"] / arb_sum) * 100, 3),
                    "stake_kes": round(1000 * (1.0 / leg["odd"] / arb_sum), 2),
                    "return_kes": round(1000 * (1.0 / leg["odd"] / arb_sum) * leg["odd"], 2),
                } for leg in legs]
                arb_markets.append({
                    "market": mkt, "market_slug": mkt, "profit_pct": profit_pct, "arb_sum": round(arb_sum, 6),
                    "legs": legs, "breakdown_1000": breakdown,
                })
        arb_markets.sort(key=lambda x: -x["profit_pct"])

    db_status       = getattr(um, "status", None)
    status_out      = _effective_status(db_status, um.start_time)
    live_flag       = status_out == "IN_PLAY"
    minutes_elapsed = None
    if um.start_time and live_flag:
        st = um.start_time if um.start_time.tzinfo else um.start_time.replace(tzinfo=timezone.utc)
        minutes_elapsed = int((_now_utc() - st).total_seconds() / 60)

    has_arb_flag     = bool(arb_markets) or um.id in arb_set
    all_market_slugs = sorted(best.keys())
    sport_out        = _normalise_sport_slug(um.sport_name or sport_slug)
    br_id            = um.parent_match_id or ""

    analytics_summary: dict = {"available": False}
    if analytics_map is not None and br_id:
        bundle = analytics_map.get(br_id) or {}
        if bundle:
            analytics_summary = _build_analytics_summary(bundle)

    return {
        "match_id":         um.id,
        "parent_match_id":  br_id, "betradar_id": br_id, "join_key": f"br_{br_id}" if br_id else f"db_{um.id}",
        "home_team":        um.home_team_name  or "", "away_team": um.away_team_name  or "",
        "competition":      um.competition_name or "", "sport": sport_out,
        "start_time":       um.start_time.isoformat() if um.start_time else None,
        "status":           status_out, "is_live": live_flag, "minutes_elapsed": minutes_elapsed,
        "bk_count":         len(bookmakers), "bookie_count": len(bookmakers), "bookmaker_count": len(bookmakers),
        "market_count":     len(all_market_slugs), "market_slugs": all_market_slugs,
        "bookmakers":       bookmakers, "markets_by_bk": markets_by_bk, "markets": markets_by_bk,
        "best":             best, "best_odds": best_odds, "aggregated": _flatten_db_markets(um.markets_json or {}),
        "has_arb":          has_arb_flag, "arb_markets": arb_markets, "arbs": arb_markets,
        "best_arb_pct":     arb_markets[0]["profit_pct"] if arb_markets else 0.0,
        "has_ev": False, "evs": [], "has_sharp": False, "sharp": [], "best_ev_pct": 0.0,
        "bk_ids": {sl: str(d["bookmaker_id"]) for sl, d in bookmakers.items()},
        "has_analytics":    analytics_summary.get("available", False), "analytics": analytics_summary,
        "source":           "postgresql",
    }


def _build_base_query(sport_slug, mode, comp_filter, team_filter, date_str, from_dt, to_dt, sort, apply_multi_bk: bool = True):
    from app.models.odds import UnifiedMatch
    from sqlalchemy import or_
    q = UnifiedMatch.query
    q = _sport_filter(q, sport_slug)
    q = _mode_time_filter(q, mode)
 
    if apply_multi_bk and mode in ("upcoming", "live"):
        q = _multi_bk_filter(q)
 
    if comp_filter:
        q = q.filter(UnifiedMatch.competition_name.ilike(f"%{comp_filter}%"))
    if team_filter:
        q = q.filter(or_(UnifiedMatch.home_team_name.ilike(f"%{team_filter}%"), UnifiedMatch.away_team_name.ilike(f"%{team_filter}%")))
    if date_str and mode == "finished":
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            q = q.filter(UnifiedMatch.start_time >= day, UnifiedMatch.start_time < day + timedelta(days=1))
        except ValueError:
            pass
    if from_dt:
        try:
            q = q.filter(UnifiedMatch.start_time >= datetime.fromisoformat(from_dt.replace("Z", "+00:00")))
        except Exception: pass
    if to_dt:
        try:
            q = q.filter(UnifiedMatch.start_time <= datetime.fromisoformat(to_dt.replace("Z", "+00:00")))
        except Exception: pass
    sort_col = {"start_time": UnifiedMatch.start_time, "home_team": UnifiedMatch.home_team_name, "competition": UnifiedMatch.competition_name}.get(sort, UnifiedMatch.start_time)
    return q.order_by(sort_col.asc())


def _fetch_batch_data(match_ids: list[int]):
    from app.models.odds import BookmakerMatchOdds, ArbitrageOpportunity
    from app.models.bookmakers_model import Bookmaker, BookmakerMatchLink
    bmo_rows  = BookmakerMatchOdds.query.filter(BookmakerMatchOdds.match_id.in_(match_ids)).all()
    all_bk_ids = {bmo.bookmaker_id for bmo in bmo_rows}
    bk_objs   = ({b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(all_bk_ids)).all()} if all_bk_ids else {})
    link_rows = BookmakerMatchLink.query.filter(BookmakerMatchLink.match_id.in_(match_ids)).all()
    links_by_match: dict[int, dict] = {}
    for lnk in link_rows:
        links_by_match.setdefault(lnk.match_id, {})[lnk.bookmaker_id] = lnk.to_dict()
    arb_set: set[int] = set()
    try:
        arb_set = {r.match_id for r in ArbitrageOpportunity.query.filter(ArbitrageOpportunity.match_id.in_(match_ids), ArbitrageOpportunity.status == "OPEN").with_entities(ArbitrageOpportunity.match_id).all()}
    except Exception:
        pass
    return bmo_rows, bk_objs, links_by_match, arb_set


def _fetch_analytics_map(um_list: list, include: bool) -> dict[str, dict]:
    if not include: return {}
    from app.workers.celery_tasks import cache_get
    result: dict[str, dict] = {}
    br_ids = [um.parent_match_id for um in um_list if um.parent_match_id]
    for br_id in br_ids:
        bundle = cache_get(f"sr:analytics:{br_id}")
        if bundle: result[br_id] = bundle
    return result


# ══════════════════════════════════════════════════════════════════════════════
# HYBRID SSE STREAM GENERATOR (DB LIST -> REDIS LIVE)
# ══════════════════════════════════════════════════════════════════════════════

def _stream_matches(
    sport_slug:  str,
    mode:        str  = "upcoming",
    comp_filter: str  = "",
    team_filter: str  = "",
    has_arb:     bool = False,
    sort:        str  = "start_time",
    date_str:    str  = "",
    from_dt:     str  = "",
    to_dt:       str  = "",
    batch_size:  int  = _STREAM_BATCH,
    include_analytics: bool = False,
    listen_live: bool = True,  # Keep connection open for real-time updates
) -> Generator[str, None, None]:
    
    t0 = time.perf_counter()
    match_ids_for_redis = set()
    
    try:
        q     = _build_base_query(sport_slug, mode, comp_filter, team_filter, date_str, from_dt, to_dt, sort)
        total = q.count()
    except Exception as exc:
        yield _sse("error", {"error": str(exc)})
        return

    total_batches = max(1, (total + batch_size - 1) // batch_size)
    yield _sse("meta", {
        "total":         total,
        "sport":         _normalise_sport_slug(sport_slug),
        "mode":          mode,
        "batch_size":    batch_size,
        "total_batches": total_batches,
        "now":           _now_utc().isoformat(),
        "live_window_minutes": int(_LIVE_WINDOW.total_seconds() / 60),
        "analytics_included": include_analytics,
    })

    if total == 0:
        yield _sse("done", {"total_sent": 0, "latency_ms": 0})
        return

    offset = 0; total_sent = 0; batch_num = 0

    # ── PHASE 1: DB BATCH LISTING ─────────────────────────────────────────────
    while offset < total:
        try:
            um_list = q.offset(offset).limit(batch_size).all()
            if not um_list: break

            match_ids = [um.id for um in um_list]
            bmo_rows, bk_objs, links_by_match, arb_set = _fetch_batch_data(match_ids)
            analytics_map = _fetch_analytics_map(um_list, include_analytics)

            bmo_by_match: dict[int, list] = {}
            for bmo in bmo_rows: bmo_by_match.setdefault(bmo.match_id, []).append(bmo)

            batch_matches = []
            for um in um_list:
                db_st = getattr(um, "status", None)
                if mode == "upcoming" and not _is_upcoming_safe(db_st, um.start_time):
                    continue
                elif mode != "upcoming" and not _is_available(db_st, um.start_time):
                    continue
                d = _build_match_dict(um, bmo_by_match.get(um.id, []), bk_objs, links_by_match, arb_set, sport_slug, analytics_map=analytics_map)

                if d["bk_count"] < MIN_BOOKMAKERS: continue
                if has_arb and not d["has_arb"]: continue
                    
                batch_matches.append(d)
                match_ids_for_redis.add(um.id) 

            batch_num  += 1
            total_sent += len(batch_matches)
            yield _sse("batch", {"matches": batch_matches, "batch": batch_num, "of": total_batches, "offset": offset})
            yield _keepalive()
            offset += batch_size

        except Exception as exc:
            yield _sse("error", {"error": str(exc), "offset": offset})
            break

    yield _sse("list_done", {
        "total_sent": total_sent,
        "batches":    batch_num,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })

    # ── PHASE 2: REDIS PUB/SUB LIVE UPDATES ───────────────────────────────────
    if not listen_live or not match_ids_for_redis:
        yield _sse("done", {"status": "finished"})
        return
        
    import redis as _rl
    from app.workers.celery_tasks import celery as _celery
    url  = _celery.conf.broker_url or "redis://localhost:6382/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    r    = _rl.Redis.from_url(f"{base}/2", decode_responses=True)
    ps   = r.pubsub()

    # Subscribe to targeted updates for ONLY the matches just listed
    channels = [f"match:update:{mid}" for mid in match_ids_for_redis]
    ps.subscribe(*channels)

    last_hb = _now_utc()
    try:
        for msg in ps.listen():
            now_tz = _now_utc()
            
            if msg["type"] == "message":
                try:
                    payload = json.loads(msg["data"])
                    yield _sse("live_update", payload)
                except json.JSONDecodeError:
                    yield _sse("live_update", {"raw_data": msg["data"]})
            
            # 20-second keepalive to prevent browser timeout
            if (now_tz - last_hb).seconds >= 20:
                yield _keepalive()
                last_hb = now_tz

    except (GeneratorExit, Exception):
        ps.unsubscribe(*channels)


def _load_db_matches(sport_slug, mode="upcoming", page=1, per_page=20, comp_filter="", team_filter="", has_arb=False, sort="start_time", date_str="", from_dt="", to_dt="", include_analytics=False):
    q = _build_base_query(sport_slug, mode, comp_filter, team_filter, date_str, from_dt, to_dt, sort)
    total   = q.count()
    um_list = q.offset((page - 1) * per_page).limit(per_page).all()
    if not um_list:
        return [], total, max(1, (total + per_page - 1) // per_page)

    match_ids = [um.id for um in um_list]
    bmo_rows, bk_objs, links_by_match, arb_set = _fetch_batch_data(match_ids)
    analytics_map = _fetch_analytics_map(um_list, include_analytics)

    bmo_by_match: dict[int, list] = {}
    for bmo in bmo_rows: bmo_by_match.setdefault(bmo.match_id, []).append(bmo)

    result = []
    for um in um_list:
        db_st = getattr(um, "status", None)
        if mode == "upcoming" and not _is_upcoming_safe(db_st, um.start_time):
            total -= 1; continue
        elif mode == "live" and not _is_available(db_st, um.start_time):
            total -= 1; continue
            
        d = _build_match_dict(um, bmo_by_match.get(um.id, []), bk_objs, links_by_match, arb_set, sport_slug, analytics_map=analytics_map)

        if mode in ("upcoming", "live") and d["bk_count"] < MIN_BOOKMAKERS:
            total -= 1; continue
        if has_arb and not d["has_arb"]:
            total -= 1; continue
        result.append(d)
    return result, total, max(1, (total + per_page - 1) // per_page)


def _read_cache_sources(mode, sport_slug):
    from app.workers.celery_tasks import cache_get, cache_keys
    canonical    = _normalise_sport_slug(sport_slug)
    slugs_to_try = list({sport_slug.lower().replace(" ", "_").replace("-", "_"), canonical.replace("-", "_")})

    def _fetch_prefix(prefix, slug):
        c = cache_get(f"bt_od:upcoming:{slug}") if prefix == "bt_od" else cache_get(f"{prefix}:{mode}:{slug}")
        if c and c.get("matches"):
            for m in c["matches"]: m.setdefault("_cache_source", prefix)
            return list(c["matches"])
        return []

    def _fetch_key(key):
        d = cache_get(key)
        return list(d["matches"]) if d and d.get("matches") else []

    raw: list[dict] = []
    work = [(p, s) for p in _CACHE_PREFIXES for s in slugs_to_try]
    with ThreadPoolExecutor(max_workers=min(16, len(work) or 1)) as pool:
        for fut in as_completed({pool.submit(_fetch_prefix, p, s): (p, s) for p, s in work}):
            try: raw.extend(fut.result())
            except Exception: pass

    extra_keys: list[str] = []
    for s in slugs_to_try:
        extra_keys += cache_keys(f"odds:{mode}:{s}:*") + cache_keys(f"odds:{mode}:{s}")
    if extra_keys:
        with ThreadPoolExecutor(max_workers=min(8, len(extra_keys))) as pool:
            for fut in as_completed({pool.submit(_fetch_key, k): k for k in extra_keys}):
                try: raw.extend(fut.result())
                except Exception: pass
    return raw


def _deduplicate(matches):
    seen: set[str] = set()
    result = []
    for m in matches:
        key = f"{m.get('home_team','').lower().strip()}|{m.get('away_team','').lower().strip()}|{str(m.get('start_time',''))[:13]}"
        if key not in seen:
            seen.add(key)
            result.append(m)
    return result


def _normalise_cache_match(m: dict, mode: str = "upcoming") -> dict | None:
    home = str(m.get("home_team") or m.get("home_team_name") or "")
    away = str(m.get("away_team") or m.get("away_team_name") or "")
    start_dt: datetime | None = None
    raw_st = m.get("start_time")
    if raw_st:
        try: start_dt = datetime.fromisoformat(str(raw_st).replace("Z", "+00:00"))
        except Exception: pass
        
    db_status  = m.get("status") or "PRE_MATCH"
    status_out = _effective_status(db_status, start_dt)

    # CRITICAL CACHE LEAKAGE FIX: Aggressive time verification against UTC now
    if mode == "upcoming":
        if (db_status or "").upper() in _EXCLUDE_FROM_UPCOMING:
            return None
        if not start_dt:
            return None
        
        # Verify it is actually in the future!
        st_utc = start_dt if start_dt.tzinfo else start_dt.replace(tzinfo=timezone.utc)
        if st_utc <= _now_utc():
            return None
            
        if status_out != "PRE_MATCH":
            return None
            
    if mode == "live" and status_out in _TERMINAL_STATUSES: return None
    if mode == "live"     and status_out != "IN_PLAY": return None
    if mode == "finished" and status_out not in _TERMINAL_STATUSES: return None

    live_flag = status_out == "IN_PLAY"
    minutes_elapsed: int | None = None
    if start_dt and live_flag:
        minutes_elapsed = int((_now_utc() - (start_dt if start_dt.tzinfo else start_dt.replace(tzinfo=timezone.utc))).total_seconds() / 60)

    markets_by_bk: dict[str, dict] = {}
    src      = m.get("_cache_source", "")
    raw_mkts = m.get("markets") or m.get("markets_by_bk") or {} 
 
    if src == "bt_od":
        markets_by_bk = {_bk_slug(bk): v for bk, v in raw_mkts.items() if isinstance(v, dict)}
    elif src and isinstance(raw_mkts, dict):
        first_val = next(iter(raw_mkts.values()), None)
        if isinstance(first_val, dict):
            inner = next(iter(first_val.values()), None)
            markets_by_bk = ({src: raw_mkts} if isinstance(inner, (int, float)) else {_bk_slug(bk): v for bk, v in raw_mkts.items()})
        else:
            markets_by_bk[src] = raw_mkts
    elif isinstance(raw_mkts, dict):
        markets_by_bk = raw_mkts
 
    if mode in ("upcoming", "live") and len(markets_by_bk) < MIN_BOOKMAKERS:
        return None

    best: dict[str, dict] = {}
    for sl, bk_mkts in markets_by_bk.items():
        for mkt, outcomes in (bk_mkts or {}).items():
            best.setdefault(mkt, {})
            for out, odd in (outcomes or {}).items():
                try: fv = float(odd)
                except (TypeError, ValueError): continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]):
                    best[mkt][out] = {"odd": fv, "bk": sl}

    slugs     = sorted(best.keys())
    sport_out = _normalise_sport_slug(str(m.get("sport") or ""))
    br_id     = str(m.get("betradar_id") or m.get("parent_match_id") or "")

    return {
        "match_id":        None, "parent_match_id": br_id, "betradar_id":     br_id,
        "join_key": (f"br_{br_id}" if br_id else f"fuzzy_{home.lower()[:8]}_{away.lower()[:8]}"),
        "home_team":        home, "away_team":        away,
        "competition":      str(m.get("competition") or m.get("competition_name") or ""),
        "sport":            sport_out, "start_time":       raw_st, "status":           status_out,
        "is_live":          live_flag, "minutes_elapsed":  minutes_elapsed,
        "bk_count":         len(markets_by_bk), "bookie_count":     len(markets_by_bk), "bookmaker_count":  len(markets_by_bk),
        "market_count":     len(slugs), "market_slugs":     slugs,
        "bookmakers": {sl: {"bookmaker": sl.upper(), "slug": sl, "markets": mkts, "market_count": len(mkts), "link": None} for sl, mkts in markets_by_bk.items()},
        "markets_by_bk": markets_by_bk, "markets": markets_by_bk, "best": best,
        "best_odds": {mkt: {out: {"odd": v["odd"], "bookie": v["bk"]} for out, v in outs.items()} for mkt, outs in best.items()},
        "has_arb": False, "arb_markets": [], "arbs": [], "best_arb_pct": 0.0,
        "has_ev":  False, "evs": [], "has_sharp": False, "sharp": [], "best_ev_pct": 0.0,
        "bk_ids": {sl: sl for sl in markets_by_bk},
        "has_analytics": False, "analytics": {"available": False}, "source": "cache",
    }


def _stream_from_cache(mode, sport_slug, batch_size=_STREAM_BATCH):
    raw     = _read_cache_sources(mode, sport_slug)
    matches = [x for m in _deduplicate(raw) if (x := _normalise_cache_match(m, mode)) is not None]
    total   = len(matches)
    yield _sse("meta", {"total": total, "sport": _normalise_sport_slug(sport_slug), "mode": mode, "source": "cache", "now": _now_utc().isoformat()})
    for i in range(0, total, batch_size):
        batch = matches[i: i + batch_size]
        yield _sse("batch", {"matches": batch, "batch": i // batch_size + 1, "of": max(1, (total + batch_size - 1) // batch_size), "offset": i})
        yield _keepalive()
    yield _sse("done", {"total_sent": total, "source": "cache"})


def _apply_tier_limits(matches, user):
    if FREE_ACCESS: return matches, False
    limits = (user.limits if user else None) or {"max_matches": FREE_MATCH_LIMIT}
    max_m  = limits.get("max_matches") or FREE_MATCH_LIMIT
    if max_m and len(matches) > max_m:
        return matches[:max_m], True
    return matches, False


def _build_envelope(matches, sport, mode, tier, page, per_page, truncated, latency_ms, total=None, pages=None, extra=None):
    arb_count  = sum(1 for m in matches if m.get("has_arb"))
    live_count = sum(1 for m in matches if m.get("is_live"))
    bk_names: set[str] = set()
    for m in matches: bk_names.update((m.get("bookmakers") or {}).keys())
    _total = total if total is not None else len(matches)
    _pages = pages if pages is not None else max(1, (_total + per_page - 1) // per_page)
    env = {
        "ok": True, "sport": _normalise_sport_slug(sport), "mode": mode, "tier": tier,
        "total": _total, "page": page, "per_page": per_page, "pages": _pages,
        "truncated": truncated, "latency_ms": latency_ms,
        "arb_count": arb_count, "live_count": live_count,
        "bookie_count": len(bk_names), "bookmakers": sorted(bk_names),
        "matches": matches, "source": "postgresql",
        "server_time": _now_utc().isoformat(), "min_bookmakers": MIN_BOOKMAKERS,
    }
    if truncated: env["upgrade_message"] = "Upgrade your plan to see all matches."
    if extra: env.update(extra)
    return env


# ══════════════════════════════════════════════════════════════════════════════
# STREAMING ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@bp_odds_customer.route("/odds/stream/upcoming/<sport_slug>")
def stream_upcoming(sport_slug: str):
    comp_f    = (request.args.get("comp",      "") or "").strip()
    team_f    = (request.args.get("team",      "") or "").strip()
    sort      = request.args.get("sort",       "start_time")
    has_arb   = request.args.get("has_arb",    "") in ("1", "true")
    date_f    = request.args.get("date",       "")
    from_dt   = request.args.get("from_dt",    "")
    to_dt     = request.args.get("to_dt",      "")
    batch     = min(50, max(5, int(request.args.get("batch", _STREAM_BATCH))))
    analytics = request.args.get("analytics",  "") in ("1", "true")
    log_event("odds_stream_upcoming", {"sport": sport_slug})

    def _gen():
        try:
            yield from _stream_matches(
                sport_slug, mode="upcoming", comp_filter=comp_f, team_filter=team_f,
                has_arb=has_arb, sort=sort, date_str=date_f, from_dt=from_dt, to_dt=to_dt,
                batch_size=batch, include_analytics=analytics, listen_live=True
            )
        except Exception:
            yield from _stream_from_cache("upcoming", sport_slug, batch)

    return Response(stream_with_context(_gen()), headers=_SSE_HEADERS)


@bp_odds_customer.route("/odds/stream/live/<sport_slug>")
def stream_live(sport_slug: str):
    comp_f    = (request.args.get("comp", "") or "").strip()
    team_f    = (request.args.get("team", "") or "").strip()
    sort      = request.args.get("sort",  "start_time")
    batch     = min(50, max(5, int(request.args.get("batch", _STREAM_BATCH))))
    analytics = request.args.get("analytics", "") in ("1", "true")
    log_event("odds_stream_live", {"sport": sport_slug})

    def _gen():
        try:
            yield from _stream_matches(
                sport_slug, mode="live", comp_filter=comp_f, team_filter=team_f,
                sort=sort, batch_size=batch, include_analytics=analytics, listen_live=True
            )
        except Exception:
            yield from _stream_from_cache("live", sport_slug, batch)

    return Response(stream_with_context(_gen()), headers=_SSE_HEADERS)


@bp_odds_customer.route("/odds/stream/results")
@bp_odds_customer.route("/odds/stream/results/<date_str>")
def stream_results(date_str: str = ""):
    if not date_str: date_str = _now_utc().strftime("%Y-%m-%d")
    sport = (request.args.get("sport", "") or "").strip()
    batch = min(50, max(5, int(request.args.get("batch", _STREAM_BATCH))))
    log_event("odds_stream_results", {"date": date_str})

    def _gen():
        try: yield from _stream_matches(sport or "all", mode="finished", date_str=date_str, batch_size=batch, listen_live=False)
        except Exception as exc:
            yield _sse("error", {"error": str(exc)})
            yield _sse("done", {"total_sent": 0})

    return Response(stream_with_context(_gen()), headers=_SSE_HEADERS)


# ══════════════════════════════════════════════════════════════════════════════
# PAGINATED JSON ROUTES
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# SINGLE MATCH DETAIL
# ══════════════════════════════════════════════════════════════════════════════

@bp_odds_customer.route("/odds/match/<parent_match_id>")
def get_match(parent_match_id: str):
    t0   = time.perf_counter()
    user = _current_user_from_header()
    tier = getattr(user, "tier", "free") if user else "free"
    from app.models.odds import UnifiedMatch, BookmakerMatchOdds, ArbitrageOpportunity, EVOpportunity, BookmakerOddsHistory
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

    bookmakers:    dict[str, dict] = {}
    markets_by_bk: dict[str, dict] = {}
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

    best: dict[str, dict] = {}
    for sl, bk_mkts in markets_by_bk.items():
        for mkt, outcomes in bk_mkts.items():
            best.setdefault(mkt, {})
            for out, odd_data in (outcomes or {}).items():
                try: fv = (float(odd_data.get("price") or odd_data.get("odd") or 0) if isinstance(odd_data, dict) else float(odd_data))
                except Exception: continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]):
                    best[mkt][out] = {"odd": fv, "bk": sl}

    db_status      = getattr(um, "status", None)
    status_out     = _effective_status(db_status, um.start_time)
    live_flag      = status_out == "IN_PLAY"
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
    from app.models.odds import UnifiedMatch, BookmakerMatchOdds
    from app.models.bookmakers_model import Bookmaker, BookmakerMatchLink

    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um: return _err("Match not found", 404)

    bmo_rows  = BookmakerMatchOdds.query.filter_by(match_id=um.id).all()
    all_bk_ids = {bmo.bookmaker_id for bmo in bmo_rows}
    bk_map    = ({b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(all_bk_ids)).all()} if all_bk_ids else {})

    stored_markets: dict[str, dict] = {}
    for bmo in bmo_rows:
        bk_obj = bk_map.get(bmo.bookmaker_id)
        if not bk_obj: continue
        slug = _bk_slug(bk_obj.name.lower())
        stored_markets[slug] = _flatten_db_markets(bmo.markets_json or {})

    fresh_bt: dict = {}
    fetch_errors: dict[str, str] = {}
    bt_external_id = parent_match_id
    try:
        from app.workers.bt_harvester import get_full_markets
        sport_slug = _normalise_sport_slug(um.sport_name or "soccer")
        fresh_bt = get_full_markets(bt_external_id, sport_slug=sport_slug)
    except Exception as exc:
        fetch_errors["bt"] = str(exc)

    combined: dict[str, dict] = dict(stored_markets)
    if fresh_bt: combined["bt"] = {**(stored_markets.get("bt") or {}), **fresh_bt}

    best: dict[str, dict] = {}
    for sl, mkts in combined.items():
        for mkt, outcomes in (mkts or {}).items():
            best.setdefault(mkt, {})
            for out, odd_data in (outcomes or {}).items():
                try: fv = (float(odd_data.get("price") or odd_data.get("odd") or 0) if isinstance(odd_data, dict) else float(odd_data))
                except Exception: continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]):
                    best[mkt][out] = {"odd": fv, "bk": sl}

    arb_markets: list[dict] = []
    if len(combined) >= 2:
        for mkt, outcomes in best.items():
            if len(outcomes) < 2: continue
            arb_sum = sum(1.0 / v["odd"] for v in outcomes.values())
            if arb_sum < 1.0:
                profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
                legs = [{"outcome": o, "bk": v["bk"], "odd": v["odd"]} for o, v in outcomes.items()]
                arb_markets.append({"market": mkt, "profit_pct": profit_pct, "arb_sum": round(arb_sum, 6), "legs": legs})
        arb_markets.sort(key=lambda x: -x["profit_pct"])

    db_status  = getattr(um, "status", None)
    status_out = _effective_status(db_status, um.start_time)
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
    from app.models.odds import UnifiedMatch
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
    from app.models.odds import UnifiedMatch
    from app.workers.celery_tasks import celery
    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um: return _err("Match not found", 404)
    try:
        celery.send_task("tasks.sp.get_match_analytics", args=[parent_match_id, True], queue="harvest", countdown=0)
        dispatched = True
    except Exception:
        dispatched = False
    return _signed_response({"ok": dispatched, "parent_match_id": parent_match_id, "dispatched": dispatched, "message": "Analytics refresh queued.", "server_time": _now_utc().isoformat()})


# ══════════════════════════════════════════════════════════════════════════════
# OTHER ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@bp_odds_customer.route("/odds/sports")
def list_sports():
    from app.models.odds import UnifiedMatch
    from sqlalchemy import func
    sports: dict[str, int] = {}
    try:
        for row in (UnifiedMatch.query.with_entities(UnifiedMatch.sport_name, func.count(UnifiedMatch.id)).group_by(UnifiedMatch.sport_name).all()):
            if row[0]:
                slug = _normalise_sport_slug(row[0])
                sports[slug] = sports.get(slug, 0) + row[1]
    except Exception: pass
    from app.workers.celery_tasks import cache_keys
    for prefix in _CACHE_PREFIXES:
        for k in cache_keys(f"{prefix}:upcoming:*"):
            parts = k.split(":")
            if len(parts) >= 3: sports.setdefault(_normalise_sport_slug(parts[2].replace("_", " ")), 0)
    return _signed_response({"ok": True, "sports": sorted(sports.keys()), "counts": sports})


@bp_odds_customer.route("/odds/bookmakers")
def list_bookmakers():
    from app.models.bookmakers_model import Bookmaker
    from app.models.odds import BookmakerMatchOdds
    from sqlalchemy import func
    bk_counts = dict(BookmakerMatchOdds.query.with_entities(BookmakerMatchOdds.bookmaker_id, func.count(BookmakerMatchOdds.match_id)).group_by(BookmakerMatchOdds.bookmaker_id).all())
    result = [{"id": bm.id, "name": bm.name, "slug": _bk_slug(bm.name), "domain": bm.domain, "is_active": bm.is_active, "match_count": bk_counts.get(bm.id, 0), "group": "local" if _bk_slug(bm.name) in ("sp", "bt", "od") else "international"} for bm in Bookmaker.query.filter_by(is_active=True).order_by(Bookmaker.name).all()]
    return _signed_response({"ok": True, "bookmakers": result, "total": len(result)})


@bp_odds_customer.route("/odds/markets")
def list_markets():
    try:
        from app.models.odds import MarketDefinition
        return _signed_response({"ok": True, "markets": [m.to_dict() for m in MarketDefinition.query.order_by(MarketDefinition.name).all()]})
    except Exception as exc: return _err(str(exc), 500)


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

    from app.models.odds import UnifiedMatch, BookmakerMatchOdds
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


@bp_odds_customer.route("/odds/status")
def harvest_status():
    from app.workers.celery_tasks import cache_get
    heartbeat = cache_get("worker_heartbeat") or {}
    now = _now_utc()
    try:
        from app.models.odds import UnifiedMatch, BookmakerMatchOdds
        from app.models.bookmakers_model import Bookmaker
        from sqlalchemy import func
        from app.extensions import db
        total_db       = UnifiedMatch.query.count()
        live_count     = UnifiedMatch.query.filter(UnifiedMatch.start_time <= now, UnifiedMatch.start_time > now - _LIVE_WINDOW, UnifiedMatch.status.notin_(list(_TERMINAL_STATUSES))).count()
        upcoming_count = UnifiedMatch.query.filter(UnifiedMatch.start_time > now, UnifiedMatch.status.notin_(list(_TERMINAL_STATUSES))).count()
        bk_cov = dict(db.session.query(BookmakerMatchOdds.bookmaker_id, func.count(BookmakerMatchOdds.match_id)).group_by(BookmakerMatchOdds.bookmaker_id).all())
        bk_names = {b.id: b.name for b in Bookmaker.query.all()}
        coverage = [{"bookmaker": bk_names.get(bk_id, str(bk_id)), "match_count": cnt, "coverage_pct": round(cnt / total_db * 100, 1) if total_db else 0} for bk_id, cnt in bk_cov.items()]
    except Exception:
        total_db = live_count = upcoming_count = 0; coverage = []

    return _signed_response({"ok": True, "free_access": FREE_ACCESS, "worker_alive": heartbeat.get("alive", False), "last_heartbeat": heartbeat.get("checked_at"), "db_match_count": total_db, "live_count": live_count, "upcoming_count": upcoming_count, "live_window_minutes": int(_LIVE_WINDOW.total_seconds() / 60), "server_time": now.isoformat(), "bookmaker_coverage": coverage, "source": "postgresql"})


@bp_odds_customer.route("/odds/access")
def get_access_config():
    user = _current_user_from_header()
    if not user or not getattr(user, "is_admin", False): return _err("Admin only", 403)
    return _signed_response({"ok": True, "free_access": FREE_ACCESS, "endpoint_access": _ENDPOINT_ACCESS})


@bp_odds_customer.route("/admin/odds/access", methods=["POST"])
def set_access_config():
    global FREE_ACCESS
    user = _current_user_from_header()
    if not user or not getattr(user, "is_admin", False): return _err("Admin only", 403)
    body = request.get_json(force=True) or {}
    if "free_access" in body: FREE_ACCESS = bool(body["free_access"])
    if isinstance(body.get("endpoint_access"), dict):
        for ep, tier in body["endpoint_access"].items():
            if ep in _ENDPOINT_ACCESS and tier in {"free", "basic", "pro", "premium"}: _ENDPOINT_ACCESS[ep] = tier
    return _signed_response({"ok": True, "free_access": FREE_ACCESS, "endpoint_access": _ENDPOINT_ACCESS})


# ══════════════════════════════════════════════════════════════════════════════
# REAL-TIME ODDS UPDATE STREAMS (Redis pub/sub)
# ══════════════════════════════════════════════════════════════════════════════

def _sse_stream(channel):
    import redis as _rl
    from app.workers.celery_tasks import celery as _celery
    url  = _celery.conf.broker_url or "redis://localhost:6382/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    r    = _rl.Redis.from_url(f"{base}/2", decode_responses=True)
    ps   = r.pubsub()
    ps.subscribe(channel)
    last_hb = _now_utc()
    try:
        for msg in ps.listen():
            now = _now_utc()
            if msg["type"] == "message": yield f"data: {msg['data']}\n\n"
            if (now - last_hb).seconds >= 20:
                yield ": ping\n\n"
                last_hb = now
    except (GeneratorExit, Exception): ps.unsubscribe(channel)


@bp_odds_customer.route("/stream/odds")
def stream_odds_ws():
    return Response(stream_with_context(_sse_stream(_WS_CHANNEL)), mimetype="text/event-stream", headers=_SSE_HEADERS)

@bp_odds_customer.route("/stream/arb")
def stream_arb():
    return Response(stream_with_context(_sse_stream(_ARB_CHANNEL)), mimetype="text/event-stream", headers=_SSE_HEADERS)

@bp_odds_customer.route("/stream/ev")
def stream_ev():
    return Response(stream_with_context(_sse_stream(_EV_CHANNEL)), mimetype="text/event-stream", headers=_SSE_HEADERS)


class _SiblingBMO:
    def __init__(self, bmo, primary_match_id):
        self._bmo = bmo
        self._primary_match_id = primary_match_id
    def __getattr__(self, name): return getattr(self._bmo, name)


# ══════════════════════════════════════════════════════════════════════════════
# NAVIGATION & POPULARITY SORTING API
# ══════════════════════════════════════════════════════════════════════════════

# 1. Define custom popularity tiers per sport. 
# We removed "world/europe" buckets. Unlisted countries default to weight 99 
# and will be dynamically sorted by match volume underneath these popular ones.
_POPULARITY_WEIGHTS = {
    "soccer": {
        "england": 1, "spain": 2, "germany": 3, "italy": 4, "france": 5, 
        "brazil": 6, "argentina": 7, "netherlands": 8, "portugal": 9
    },
    "basketball": {
        "usa": 1, "spain": 2, "greece": 3, "turkey": 4, "italy": 5
    },
    "cricket": {
        "india": 1, "australia": 2, "england": 3, "pakistan": 4, "south africa": 5, "new zealand": 6
    },
    "tennis": {
        "atp": 1, "wta": 2, "challenger": 3, "itf": 4
    }
}

def _get_country_weight(sport_slug: str, category_name: str) -> int:
    """Returns a sorting weight. 99 is the default for remaining countries."""
    if not category_name: return 99
    weights = _POPULARITY_WEIGHTS.get(sport_slug, {})
    return weights.get(category_name.lower(), 99)

@bp_odds_customer.route("/odds/navigation")
def get_navigation_tree():
    t0 = time.perf_counter()
    user = _current_user_from_header()
    
    from app.models.odds import UnifiedMatch
    from app.extensions import db
    from sqlalchemy import func
    
    now = _now_utc()
    
    try:
        # Group all upcoming/live matches by Sport and Competition
        rows = (
            db.session.query(
                UnifiedMatch.sport_name,
                UnifiedMatch.competition_name,
                func.count(UnifiedMatch.id)
            )
            .filter(UnifiedMatch.start_time > now)
            .filter(UnifiedMatch.status.notin_(list(_TERMINAL_STATUSES)))
            .group_by(UnifiedMatch.sport_name, UnifiedMatch.competition_name)
            .all()
        )
        
        nav_tree = {}
        
        for sport, comp, count in rows:
            if not sport or not comp: continue
            
            sport_slug = _normalise_sport_slug(sport)
            
            category = "International"
            comp_clean = comp
            
            # Extract Country/Category
            if " - " in comp:
                parts = comp.split(" - ", 1)
                category = parts[0].strip()
                comp_clean = parts[1].strip()
            elif ": " in comp:
                parts = comp.split(": ", 1)
                category = parts[0].strip()
                comp_clean = parts[1].strip()
            
            # Special parsing for Tennis
            if sport_slug == "tennis":
                comp_upper = comp.upper()
                if "ATP" in comp_upper: category = "ATP"
                elif "WTA" in comp_upper: category = "WTA"
                elif "ITF" in comp_upper: category = "ITF"
                elif "CHALLENGER" in comp_upper: category = "Challenger"
            
            # DEDUPLICATION FIX 1: Normalize category casing (e.g., "ENGLAND" -> "England")
            category = category.title() if sport_slug != "tennis" else category
            
            if sport_slug not in nav_tree:
                nav_tree[sport_slug] = {"sport": sport, "slug": sport_slug, "count": 0, "categories": {}}
            
            nav_tree[sport_slug]["count"] += count
            
            if category not in nav_tree[sport_slug]["categories"]:
                nav_tree[sport_slug]["categories"][category] = {
                    "name": category,
                    "weight": _get_country_weight(sport_slug, category),
                    "count": 0,
                    "competitions": {} # Use dict here to act as a Set for deduplication
                }
                
            nav_tree[sport_slug]["categories"][category]["count"] += count
            
            # DEDUPLICATION FIX 2: Squashing identical competitions together
            comps_dict = nav_tree[sport_slug]["categories"][category]["competitions"]
            
            if comp_clean not in comps_dict:
                comps_dict[comp_clean] = {
                    "name": comp_clean,
                    "original_name": comp, 
                    "count": 0
                }
                
            comps_dict[comp_clean]["count"] += count
            
        # Format and Sort the Tree
        sorted_sports = []
        for s_slug, s_data in nav_tree.items():
            sorted_categories = []
            
            for c_name, c_data in s_data["categories"].items():
                # Convert the deduplicated competitions dict back to a list
                comps_list = list(c_data["competitions"].values())
                
                # Sort competitions inside a country (Biggest count first, then alphabetical)
                comps_list.sort(key=lambda x: (-x["count"], x["name"]))
                c_data["competitions"] = comps_list
                
                sorted_categories.append(c_data)
            
            # THE HYBRID SORT:
            # 1. x["weight"] -> Popular countries go to the top (1, 2, 3...)
            #    (Remaining countries default to 99, so they are pushed below the popular ones)
            # 2. -x["count"] -> The remaining countries are sorted by match volume (Highest to lowest)
            # 3. x["name"]   -> If match volume is identical, sort alphabetically
            sorted_categories.sort(key=lambda x: (x["weight"], -x["count"], x["name"]))
            
            sorted_sports.append({
                "slug": s_slug,
                "name": s_data["sport"],
                "count": s_data["count"],
                "categories": sorted_categories
            })
        
        # Sort the main sports tabs by total match volume
        sorted_sports.sort(key=lambda x: -x["count"])
        
        return _signed_response({
            "ok": True,
            "navigation": sorted_sports,
            "latency_ms": int((time.perf_counter() - t0) * 1000)
        }, encrypt_for=user)
        
    except Exception as exc:
        return _err(str(exc), 500)