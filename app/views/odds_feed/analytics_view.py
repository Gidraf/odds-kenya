"""
app/views/odds_feed/analytics_routes.py
=========================================
Dedicated analytics API blueprint.

All endpoints are free-access (no auth required) and read from the
sr:analytics:{betradar_id} Redis cache populated by the SR analytics worker.

Endpoints
─────────────────────────────────────────────────────────────────────────────
  GET  /api/analytics/match/<betradar_id>            Full analytics bundle
  POST /api/analytics/match/<betradar_id>/refresh    Force re-fetch (async)
  GET  /api/analytics/match/<betradar_id>/fixtures   Home + away next matches
  GET  /api/analytics/match/<betradar_id>/tournament Tournament + season info
  GET  /api/analytics/match/<betradar_id>/venue      Stadium / venue info
  GET  /api/analytics/batch                          Batch fetch (POST body or ?ids=)
  POST /api/analytics/batch                          Batch fetch (POST body)
  GET  /api/analytics/status                         Cache hit stats
─────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from flask import Blueprint, request

from app.utils.customer_jwt_helpers import _err, _signed_response
from app.utils.decorators_ import log_event

bp_analytics = Blueprint("analytics", __name__, url_prefix="/api/analytics")


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cache_get(key: str) -> dict | None:
    from app.workers.celery_tasks import cache_get
    return cache_get(key)


def _dispatch_refresh(betradar_id: str, force: bool = False) -> bool:
    try:
        from app.workers.celery_tasks import celery
        celery.send_task(
            "tasks.sp.get_match_analytics",
            args=[betradar_id, force],
            queue="harvest",
            countdown=0,
        )
        return True
    except Exception:
        return False


def _get_bundle(betradar_id: str, trigger: bool = True) -> dict:
    bundle = _cache_get(f"sr:analytics:{betradar_id}") or {}
    if trigger and not bundle.get("available"):
        _dispatch_refresh(betradar_id, force=False)
    return bundle


def _fmt_team(t: dict) -> dict:
    cc = t.get("cc") or {}
    return {
        "uid":     t.get("uid"),
        "name":    t.get("name"),
        "abbr":    t.get("abbr"),
        "country": cc.get("name"),
        "cc_a2":   cc.get("a2"),
    }


def _fmt_match(m: dict) -> dict:
    home = (m.get("teams") or {}).get("home") or {}
    away = (m.get("teams") or {}).get("away") or {}
    t    = m.get("time") or {}
    res  = m.get("result") or {}
    rn   = m.get("roundname") or {}
    g    = m.get("ground") or {}
    return {
        "sr_match_id": m.get("_id"),
        "home":        home.get("name"),
        "away":        away.get("name"),
        "date":        t.get("date"),
        "time":        t.get("time"),
        "uts":         t.get("uts"),
        "round":       rn.get("name"),
        "surface":     g.get("name"),
        "status":      m.get("status"),
        "canceled":    m.get("canceled"),
        "postponed":   m.get("postponed"),
        "walkover":    m.get("walkover"),
        "retired":     m.get("retired"),
        "bestof":      m.get("bestof"),
        "result": {
            "home":   res.get("home"),
            "away":   res.get("away"),
            "winner": res.get("winner"),
        } if any(v is not None for v in [res.get("home"), res.get("away")]) else None,
    }


def _resolve_match(betradar_id: str) -> tuple[object | None, str]:
    """
    Resolve a betradar_id to a UnifiedMatch row.
    Returns (um, error_message). If um is None the caller should return 404.
    """
    try:
        from app.models.odds_model import UnifiedMatch
        um = UnifiedMatch.query.filter_by(parent_match_id=betradar_id).first()
        return um, ""
    except Exception as exc:
        return None, str(exc)


def _match_stub(um) -> dict:
    """Compact match identifier block included in every analytics response."""
    from app.views.odds_feed.customer_odds_view import _normalise_sport_slug, _effective_status
    return {
        "match_id":        um.id,
        "parent_match_id": um.parent_match_id,
        "betradar_id":     um.parent_match_id,
        "home_team":       um.home_team_name,
        "away_team":       um.away_team_name,
        "competition":     um.competition_name,
        "sport":           _normalise_sport_slug(um.sport_name or ""),
        "start_time":      um.start_time.isoformat() if um.start_time else None,
        "status":          _effective_status(getattr(um, "status", None), um.start_time),
    }


# ══════════════════════════════════════════════════════════════════════════════
# FULL ANALYTICS BUNDLE
# ══════════════════════════════════════════════════════════════════════════════

@bp_analytics.route("/match/<betradar_id>")
def get_analytics(betradar_id: str):
    """
    GET /api/analytics/match/<betradar_id>

    Full Sportradar analytics bundle:
      - Tournament, season, venue metadata
      - Home + away team next fixtures (30 days)
      - Season draw / fixtures (when available)
      - Live coverage flags

    If not yet cached, a background fetch is triggered and
    {"available": false, "fetching": true} is returned.
    Poll again in ~5 s.
    """
    t0  = time.perf_counter()
    log_event("analytics_match", {"id": betradar_id})

    um, err = _resolve_match(betradar_id)
    if err:
        return _err(err, 500)
    if not um:
        return _err("Match not found", 404)

    bundle   = _get_bundle(betradar_id, trigger=True)
    available = bundle.get("available", False)

    if not available:
        return _signed_response({
            "ok":              True,
            "available":       False,
            "fetching":        True,
            "message":         "Analytics not yet cached. Background fetch dispatched. Retry in 5s.",
            **_match_stub(um),
            "latency_ms":      int((time.perf_counter() - t0) * 1000),
            "server_time":     _now(),
        })

    match      = bundle.get("match") or {}
    teams      = match.get("teams") or {}
    tournament = match.get("tournament") or {}
    season     = match.get("season") or {}
    stadium    = match.get("stadium") or {}
    coverage   = match.get("statscoverage") or {}
    home_next  = bundle.get("home_next") or {}
    away_next  = bundle.get("away_next") or {}
    sf         = bundle.get("season_fixtures") or {}
    t_info     = tournament.get("tennisinfo") or {}

    prize_info = None
    if t_info:
        prize     = t_info.get("prize") or {}
        prize_info = {"amount": prize.get("amount"), "currency": prize.get("currency")}

    return _signed_response({
        "ok":           True,
        "available":    True,
        "fetching":     False,
        "sr_match_id":  bundle.get("sr_match_id"),
        **_match_stub(um),

        "tournament": {
            "name":          tournament.get("name"),
            "level":         tournament.get("tournamentlevelname"),
            "gender":        t_info.get("gender"),
            "type":          t_info.get("type"),
            "prize":         prize_info,
            "surface":       (tournament.get("ground") or {}).get("name"),
            "start":         t_info.get("start"),
            "end":           t_info.get("end"),
            "qualification": t_info.get("qualification"),
            "city":          t_info.get("city"),
            "country":       t_info.get("country"),
        },

        "season": {
            "id":    season.get("_id"),
            "name":  season.get("name"),
            "year":  season.get("year"),
            "start": (season.get("start") or {}).get("date"),
            "end":   (season.get("end") or {}).get("date"),
        },

        "venue": {
            "name":     stadium.get("name"),
            "city":     stadium.get("city"),
            "country":  stadium.get("country"),
            "capacity": stadium.get("capacity"),
        },

        "surface": (match.get("ground") or {}).get("name"),

        "home_team": _fmt_team(teams.get("home") or {}),
        "away_team": _fmt_team(teams.get("away") or {}),

        "coverage": {k: v for k, v in coverage.items() if isinstance(v, bool)},
        "active_coverage": [k for k, v in coverage.items() if v is True],

        "home_next": {
            "team":        _fmt_team(home_next.get("team") or {}),
            "match_count": len(home_next.get("matches") or []),
            "matches":     [_fmt_match(m) for m in (home_next.get("matches") or [])],
        },
        "away_next": {
            "team":        _fmt_team(away_next.get("team") or {}),
            "match_count": len(away_next.get("matches") or []),
            "matches":     [_fmt_match(m) for m in (away_next.get("matches") or [])],
        },

        "season_fixtures": {
            "total":   len(sf.get("matches") or []),
            "matches": [_fmt_match(m) for m in (sf.get("matches") or [])],
            "cups":    list((sf.get("cups") or {}).keys()),
        } if sf else None,

        "latency_ms":  int((time.perf_counter() - t0) * 1000),
        "server_time": _now(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# FORCE REFRESH
# ══════════════════════════════════════════════════════════════════════════════

@bp_analytics.route("/match/<betradar_id>/refresh", methods=["POST"])
def refresh_analytics(betradar_id: str):
    """
    POST /api/analytics/match/<betradar_id>/refresh

    Force a background re-fetch of SR analytics.
    Returns immediately — poll GET .../match/<id> in ~5 s.

    Optional JSON body:
      { "fetch_season": true }   → also fetch the full season fixture list
    """
    body         = request.get_json(silent=True) or {}
    fetch_season = bool(body.get("fetch_season", False))

    um, err = _resolve_match(betradar_id)
    if err:
        return _err(err, 500)
    if not um:
        return _err("Match not found", 404)

    dispatched = _dispatch_refresh(betradar_id, force=True)
    log_event("analytics_refresh", {"id": betradar_id, "fetch_season": fetch_season})

    return _signed_response({
        "ok":              dispatched,
        "parent_match_id": betradar_id,
        "dispatched":      dispatched,
        "fetch_season":    fetch_season,
        "message":         "Analytics refresh queued. Poll GET .../match/<id> in ~5s.",
        "server_time":     _now(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# NEXT FIXTURES ONLY  (lightweight)
# ══════════════════════════════════════════════════════════════════════════════

@bp_analytics.route("/match/<betradar_id>/fixtures")
def get_fixtures(betradar_id: str):
    """
    GET /api/analytics/match/<betradar_id>/fixtures

    Returns only the home and away team upcoming fixtures.
    Lighter than the full bundle — useful for pre-match panels.

    Query params:
      ?days=30    Max days ahead (default 30, informational only — data comes from cache)
    """
    t0 = time.perf_counter()
    um, err = _resolve_match(betradar_id)
    if err:
        return _err(err, 500)
    if not um:
        return _err("Match not found", 404)

    bundle    = _get_bundle(betradar_id, trigger=True)
    available = bundle.get("available", False)

    home_next = bundle.get("home_next") or {}
    away_next = bundle.get("away_next") or {}

    match      = bundle.get("match") or {}
    teams      = match.get("teams") or {}

    return _signed_response({
        "ok":        True,
        "available": available,
        "fetching":  not available,
        **_match_stub(um),

        "home_next": {
            "team":        _fmt_team(home_next.get("team") or teams.get("home") or {}),
            "match_count": len(home_next.get("matches") or []),
            "matches":     [_fmt_match(m) for m in (home_next.get("matches") or [])],
        },
        "away_next": {
            "team":        _fmt_team(away_next.get("team") or teams.get("away") or {}),
            "match_count": len(away_next.get("matches") or []),
            "matches":     [_fmt_match(m) for m in (away_next.get("matches") or [])],
        },

        "latency_ms":  int((time.perf_counter() - t0) * 1000),
        "server_time": _now(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# TOURNAMENT + SEASON INFO ONLY
# ══════════════════════════════════════════════════════════════════════════════

@bp_analytics.route("/match/<betradar_id>/tournament")
def get_tournament(betradar_id: str):
    """
    GET /api/analytics/match/<betradar_id>/tournament

    Returns tournament, season, venue and surface.
    """
    t0 = time.perf_counter()
    um, err = _resolve_match(betradar_id)
    if err:
        return _err(err, 500)
    if not um:
        return _err("Match not found", 404)

    bundle    = _get_bundle(betradar_id, trigger=True)
    available = bundle.get("available", False)

    match      = bundle.get("match") or {}
    tournament = match.get("tournament") or {}
    season     = match.get("season") or {}
    stadium    = match.get("stadium") or {}
    t_info     = tournament.get("tennisinfo") or {}

    prize_info = None
    if t_info:
        prize     = t_info.get("prize") or {}
        prize_info = {"amount": prize.get("amount"), "currency": prize.get("currency")}

    return _signed_response({
        "ok":        True,
        "available": available,
        "fetching":  not available,
        **_match_stub(um),

        "tournament": {
            "name":          tournament.get("name"),
            "level":         tournament.get("tournamentlevelname"),
            "gender":        t_info.get("gender"),
            "type":          t_info.get("type"),
            "prize":         prize_info,
            "surface":       (tournament.get("ground") or {}).get("name"),
            "start":         t_info.get("start"),
            "end":           t_info.get("end"),
            "qualification": t_info.get("qualification"),
            "city":          t_info.get("city"),
            "country":       t_info.get("country"),
        },
        "season": {
            "id":    season.get("_id"),
            "name":  season.get("name"),
            "year":  season.get("year"),
            "start": (season.get("start") or {}).get("date"),
            "end":   (season.get("end") or {}).get("date"),
        },
        "venue": {
            "name":     stadium.get("name"),
            "city":     stadium.get("city"),
            "country":  stadium.get("country"),
            "capacity": stadium.get("capacity"),
        },
        "surface": (match.get("ground") or {}).get("name"),

        "latency_ms":  int((time.perf_counter() - t0) * 1000),
        "server_time": _now(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# VENUE ONLY
# ══════════════════════════════════════════════════════════════════════════════

@bp_analytics.route("/match/<betradar_id>/venue")
def get_venue(betradar_id: str):
    """
    GET /api/analytics/match/<betradar_id>/venue

    Returns stadium / venue details only.
    """
    t0 = time.perf_counter()
    um, err = _resolve_match(betradar_id)
    if err:
        return _err(err, 500)
    if not um:
        return _err("Match not found", 404)

    bundle  = _get_bundle(betradar_id, trigger=True)
    match   = bundle.get("match") or {}
    stadium = match.get("stadium") or {}

    return _signed_response({
        "ok":        True,
        "available": bundle.get("available", False),
        **_match_stub(um),
        "venue": {
            "name":     stadium.get("name"),
            "city":     stadium.get("city"),
            "country":  stadium.get("country"),
            "capacity": stadium.get("capacity"),
        },
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
        "server_time": _now(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# BATCH ANALYTICS  (GET ?ids=id1,id2,... OR POST {"ids": [...]})
# ══════════════════════════════════════════════════════════════════════════════

@bp_analytics.route("/batch", methods=["GET", "POST"])
def batch_analytics():
    """
    GET  /api/analytics/batch?ids=id1,id2,id3
    POST /api/analytics/batch   body: {"ids": ["id1", "id2"], "trigger_missing": true}

    Batch-fetch analytics summaries for multiple betradar IDs.
    Returns a compact summary per match — use the single-match endpoint
    for the full bundle.

    Capped at 50 IDs per request.
    """
    t0 = time.perf_counter()

    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        ids  = body.get("ids") or []
        trigger_missing = bool(body.get("trigger_missing", True))
    else:
        raw = (request.args.get("ids") or "").strip()
        ids = [x.strip() for x in raw.split(",") if x.strip()] if raw else []
        trigger_missing = request.args.get("trigger_missing", "1") in ("1", "true")

    if not ids:
        return _err("Provide 'ids' list (comma-separated GET or JSON POST body)", 400)

    ids = ids[:50]  # hard cap
    log_event("analytics_batch", {"count": len(ids)})

    results: list[dict] = []
    cached_count  = 0
    missing_count = 0

    for br_id in ids:
        bundle    = _cache_get(f"sr:analytics:{br_id}") or {}
        available = bundle.get("available", False)

        if available:
            cached_count += 1
        else:
            missing_count += 1
            if trigger_missing:
                _dispatch_refresh(br_id, force=False)

        # Resolve match name for context (best-effort — don't fail if not found)
        home = away = competition = sport = start_time = None
        try:
            from app.models.odds_model import UnifiedMatch
            from app.views.odds_feed.customer_odds_view import _normalise_sport_slug, _effective_status
            um = UnifiedMatch.query.filter_by(parent_match_id=br_id).first()
            if um:
                home        = um.home_team_name
                away        = um.away_team_name
                competition = um.competition_name
                sport       = _normalise_sport_slug(um.sport_name or "")
                start_time  = um.start_time.isoformat() if um.start_time else None
        except Exception:
            pass

        match      = bundle.get("match") or {}
        tournament = (match.get("tournament") or {})
        season     = (match.get("season") or {})
        stadium    = (match.get("stadium") or {})
        teams      = (match.get("teams") or {})
        home_t     = teams.get("home") or {}
        away_t     = teams.get("away") or {}
        coverage   = match.get("statscoverage") or {}
        home_next  = bundle.get("home_next") or {}
        away_next  = bundle.get("away_next") or {}

        results.append({
            "betradar_id":     br_id,
            "available":       available,
            "fetching":        not available,
            "home_team":       home or home_t.get("name"),
            "away_team":       away or away_t.get("name"),
            "competition":     competition or tournament.get("name"),
            "sport":           sport,
            "start_time":      start_time,
            "sr_match_id":     bundle.get("sr_match_id"),
            "tournament":      tournament.get("name"),
            "season":          season.get("name"),
            "season_year":     season.get("year"),
            "venue":           stadium.get("name"),
            "venue_city":      stadium.get("city"),
            "surface":         (match.get("ground") or {}).get("name"),
            "home_uid":        home_t.get("uid"),
            "away_uid":        away_t.get("uid"),
            "home_next_count": len(home_next.get("matches") or []),
            "away_next_count": len(away_next.get("matches") or []),
            "active_coverage": [k for k, v in coverage.items() if v is True],
        })

    return _signed_response({
        "ok":           True,
        "total":        len(ids),
        "cached":       cached_count,
        "missing":      missing_count,
        "triggered":    missing_count if trigger_missing else 0,
        "results":      results,
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
        "server_time":  _now(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# ANALYTICS CACHE STATUS
# ══════════════════════════════════════════════════════════════════════════════

@bp_analytics.route("/status")
def analytics_status():
    """
    GET /api/analytics/status

    Returns cache hit statistics for analytics data.
    Scans sr:analytics:* keys in Redis.
    Capped at 10 000 keys to avoid blocking.
    """
    t0 = time.perf_counter()
    total = cached = 0

    try:
        from app.workers.celery_tasks import cache_keys, cache_get
        keys  = (cache_keys("sr:analytics:*") or [])[:10_000]
        total = len(keys)
        for k in keys:
            b = cache_get(k)
            if b and b.get("available"):
                cached += 1
    except Exception:
        pass

    hit_rate = round(cached / total * 100, 1) if total else 0.0

    return _signed_response({
        "ok":           True,
        "total_keys":   total,
        "cached":       cached,
        "missing":      total - cached,
        "hit_rate_pct": hit_rate,
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
        "server_time":  _now(),
    })