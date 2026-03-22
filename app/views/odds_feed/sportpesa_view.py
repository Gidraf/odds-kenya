"""
app/views/odds_feed/sp_module.py
=================================
Dedicated Sportpesa API module.

Endpoints
---------
  GET  /api/sp/sports                     → available sports + live counts
  GET  /api/sp/upcoming/<sport_slug>      → cached upcoming matches
  GET  /api/sp/live/<sport_slug>          → cached live matches
  GET  /api/sp/realtime/<sport_slug>      → fresh fetch from SP API (updates cache)
  GET  /api/sp/match/<game_id>/markets    → full market book for one match
  GET  /api/sp/status                     → harvest timestamps + health

Query params (upcoming / live / realtime)
------------------------------------------
  sort    = start_time | competition | market_count    default: start_time
  order   = asc | desc                                 default: asc
  comp    = <partial name>                             competition filter
  team    = <partial name>                             team filter
  market  = <slug>                                     market filter
  date    = YYYY-MM-DD                                 date filter
  page    = int                                        default 1
  per_page= int                                        default 25, max 100

Response shape (every list endpoint)
--------------------------------------
{
  "ok":           true,
  "source":       "sportpesa",
  "sport":        "soccer",
  "mode":         "upcoming" | "live" | "realtime",
  "total":        int,
  "page":         int,
  "per_page":     int,
  "pages":        int,
  "harvested_at": str | null,
  "latency_ms":   int,
  "matches":      [ <match_dict>, ... ]
}

Each match_dict
---------------
{
  "sp_game_id":   str,
  "betradar_id":  str,
  "home_team":    str,
  "away_team":    str,
  "start_time":   str,
  "competition":  str,
  "sport":        str,
  "source":       "sportpesa",
  "status":       "upcoming" | "live",
  "markets": {
    "1x2":                  {"1": 2.07, "X": 3.10, "2": 3.50},
    "over_under_goals_2.5": {"over": 1.90, "under": 1.80},
    "btts":                 {"yes": 1.75, "no": 1.95},
    ...
  },
  "market_count": int,
  "harvested_at": str
}
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, request

from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response

bp_sp = Blueprint("sp", __name__, url_prefix="/api/sp")

# Sport slugs exposed via this module
_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
    "boxing", "handball", "mma",
]


# =============================================================================
# Redis cache helpers (thin wrappers so we don't import celery_tasks at module load)
# =============================================================================

def _cache_get(key: str):
    try:
        from app.workers.celery_tasks import cache_get
        return cache_get(key)
    except Exception:
        return None


def _cache_set(key: str, data, ttl: int = 300):
    try:
        from app.workers.celery_tasks import cache_set
        cache_set(key, data, ttl=ttl)
    except Exception:
        pass


# =============================================================================
# Sort / filter helpers
# =============================================================================

def _parse_dt(val: str | None) -> datetime:
    if not val:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


def _apply_filters(matches: list[dict], args) -> list[dict]:
    comp   = (args.get("comp")   or args.get("competition") or "").strip().lower()
    team   = (args.get("team")   or "").strip().lower()
    market = (args.get("market") or "").strip().lower()
    date_s = (args.get("date")   or "").strip()

    fdt = tdt = None
    if date_s:
        try:
            day = datetime.strptime(date_s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            fdt, tdt = day, day + timedelta(days=1)
        except ValueError:
            pass

    result = []
    for m in matches:
        if comp and comp not in m.get("competition", "").lower():
            continue
        if team:
            if (team not in m.get("home_team", "").lower() and
                    team not in m.get("away_team", "").lower()):
                continue
        if market and not any(market in k for k in (m.get("markets") or {})):
            continue
        if fdt:
            st = _parse_dt(m.get("start_time"))
            if st < fdt or st >= tdt:
                continue
        result.append(m)
    return result


def _sort_matches(matches: list[dict], sort: str, order: str) -> list[dict]:
    reverse = order == "desc"
    if sort == "competition":
        return sorted(matches, key=lambda m: m.get("competition", ""), reverse=reverse)
    if sort == "market_count":
        return sorted(matches, key=lambda m: m.get("market_count", 0), reverse=not reverse)
    # Default: start_time
    return sorted(matches, key=lambda m: _parse_dt(m.get("start_time")), reverse=reverse)


def _paginate(matches: list[dict], page: int, per_page: int) -> tuple[list[dict], int]:
    total  = len(matches)
    start  = (page - 1) * per_page
    return matches[start: start + per_page], total


def _envelope(
    matches: list[dict], total: int, sport: str, mode: str,
    page: int, per_page: int, harvested_at: str | None, latency_ms: int,
) -> dict:
    """Standard response envelope."""
    # Collect unique competitions for the summary
    comps = sorted({m.get("competition", "") for m in matches if m.get("competition")})
    return {
        "ok":           True,
        "source":       "sportpesa",
        "sport":        sport,
        "mode":         mode,
        "total":        total,
        "page":         page,
        "per_page":     per_page,
        "pages":        max(1, (total + per_page - 1) // per_page),
        "harvested_at": harvested_at,
        "latency_ms":   latency_ms,
        "competitions": comps,
        "matches":      matches,
    }


# =============================================================================
# /api/sp/sports
# =============================================================================

@bp_sp.route("/sports")
def list_sports():
    """List available sports with cached match counts."""
    t0     = time.perf_counter()
    result = []
    for slug in _SPORTS:
        cached = _cache_get(f"sp:upcoming:{slug}")
        live   = _cache_get(f"sp:live:{slug}")
        result.append({
            "slug":          slug,
            "label":         slug.replace("-", " ").title(),
            "upcoming":      cached.get("match_count", 0) if cached else 0,
            "live":          live.get("match_count", 0) if live else 0,
            "last_harvest":  (cached or {}).get("harvested_at"),
        })
    return _signed_response({
        "ok":         True,
        "source":     "sportpesa",
        "sports":     result,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# /api/sp/upcoming/<sport_slug>  — from Redis cache
# =============================================================================

@bp_sp.route("/upcoming/<sport_slug>")
def get_upcoming_cached(sport_slug: str):
    """
    Return upcoming matches from the Redis cache written by the Celery harvest task.
    This is instant (no SP API call).  Use /realtime to force a fresh fetch.
    """
    t0       = time.perf_counter()
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 25)), 100)
    sort     = request.args.get("sort", "start_time")
    order    = request.args.get("order", "asc")

    cached = _cache_get(f"sp:upcoming:{sport_slug}")
    if not cached:
        return _signed_response(_envelope(
            [], 0, sport_slug, "upcoming", page, per_page, None,
            int((time.perf_counter() - t0) * 1000),
        ))

    matches = cached.get("matches") or []
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response(_envelope(
        paged, total, sport_slug, "upcoming",
        page, per_page, cached.get("harvested_at"),
        int((time.perf_counter() - t0) * 1000),
    ))


# =============================================================================
# /api/sp/live/<sport_slug>  — from Redis cache
# =============================================================================

@bp_sp.route("/live/<sport_slug>")
def get_live_cached(sport_slug: str):
    """Return live matches from the Redis cache."""
    t0       = time.perf_counter()
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 25)), 100)
    sort     = request.args.get("sort", "start_time")
    order    = request.args.get("order", "asc")

    cached = _cache_get(f"sp:live:{sport_slug}")
    if not cached:
        return _signed_response(_envelope(
            [], 0, sport_slug, "live", page, per_page, None,
            int((time.perf_counter() - t0) * 1000),
        ))

    matches = cached.get("matches") or []
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response(_envelope(
        paged, total, sport_slug, "live",
        page, per_page, cached.get("harvested_at"),
        int((time.perf_counter() - t0) * 1000),
    ))


# =============================================================================
# /api/sp/realtime/<sport_slug>  — live fetch from SP API
# =============================================================================

@bp_sp.route("/realtime/<sport_slug>")
def get_realtime(sport_slug: str):
    """
    Fetch fresh data directly from the Sportpesa API, update the cache,
    and return the result.

    ⚠ This makes N HTTP calls to SP (one per match for full markets).
    Use sparingly — the beat schedule keeps the cache fresh automatically.

    Query params
    ────────────
      days       = int    how many days ahead to fetch (default 2)
      max        = int    max matches to fetch (default 30)
      live       = 1|0    fetch live instead of upcoming (default 0)
    """
    t0       = time.perf_counter()
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 25)), 100)
    sort     = request.args.get("sort", "start_time")
    order    = request.args.get("order", "asc")
    days     = min(int(request.args.get("days", 2)), 7)
    max_m    = min(int(request.args.get("max", 30)), 150)
    is_live  = request.args.get("live", "0") == "1"

    try:
        from app.workers.sp_harvester import fetch_upcoming, fetch_live
        if is_live:
            matches = fetch_live(sport_slug, fetch_full_markets=True)
            mode    = "live"
            ttl     = 60
        else:
            matches = fetch_upcoming(
                sport_slug, days=days, max_matches=max_m,
                fetch_full_markets=True,
            )
            mode = "realtime"
            ttl  = 300
    except Exception as exc:
        return _err(f"SP fetch error: {exc}", 500)

    harvested_at = datetime.now(timezone.utc).isoformat()

    # Update the cache so subsequent /upcoming calls see the fresh data
    cache_key = f"sp:{('live' if is_live else 'upcoming')}:{sport_slug}"
    _cache_set(cache_key, {
        "source":       "sportpesa",
        "sport":        sport_slug,
        "mode":         mode,
        "match_count":  len(matches),
        "harvested_at": harvested_at,
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
        "matches":      matches,
    }, ttl=ttl)

    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response(_envelope(
        paged, total, sport_slug, mode,
        page, per_page, harvested_at,
        int((time.perf_counter() - t0) * 1000),
    ))


# =============================================================================
# /api/sp/match/<game_id>/markets  — full market book for one match
# =============================================================================

@bp_sp.route("/match/<game_id>/markets")
def get_match_markets(game_id: str):
    """
    Fetch the complete market book for one SP match directly from the API.
    Returns all markets normalised to canonical slugs.
    """
    t0 = time.perf_counter()
    try:
        from app.workers.sp_harvester import fetch_match_markets
        markets = fetch_match_markets(game_id)
    except Exception as exc:
        return _err(f"SP markets fetch error: {exc}", 500)

    return _signed_response({
        "ok":         True,
        "sp_game_id": game_id,
        "source":     "sportpesa",
        "markets":    markets,
        "market_count": len(markets),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# /api/sp/status
# =============================================================================

@bp_sp.route("/status")
def sp_status():
    """Show cache freshness and harvest stats for all SP sports."""
    t0     = time.perf_counter()
    sports = []
    for slug in _SPORTS:
        up   = _cache_get(f"sp:upcoming:{slug}") or {}
        live = _cache_get(f"sp:live:{slug}") or {}
        sports.append({
            "sport":              slug,
            "upcoming_count":     up.get("match_count", 0),
            "upcoming_harvested": up.get("harvested_at"),
            "upcoming_latency":   up.get("latency_ms"),
            "live_count":         live.get("match_count", 0),
            "live_harvested":     live.get("harvested_at"),
        })

    # Worker heartbeat
    hb = _cache_get("worker_heartbeat") or {}

    return _signed_response({
        "ok":           True,
        "source":       "sportpesa",
        "worker_alive": hb.get("alive", False),
        "last_heartbeat": hb.get("checked_at"),
        "sports":       sports,
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
    })