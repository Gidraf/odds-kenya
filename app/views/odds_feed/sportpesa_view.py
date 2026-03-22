"""
app/views/odds_feed/sp_module.py
=================================
Dedicated Sportpesa API module.

Endpoints
---------
  GET  /api/sp/sports                       → available sports + live counts
  GET  /api/sp/upcoming/<sport_slug>        → cached upcoming matches
  GET  /api/sp/live/<sport_slug>            → cached live matches
  GET  /api/sp/realtime/<sport_slug>        → legacy: fresh fetch (updates cache)
  GET  /api/sp/direct/upcoming/<sport_slug> → fresh fetch upcoming from SP API
  GET  /api/sp/direct/live/<sport_slug>     → fresh fetch live from SP API
  GET  /api/sp/match/<game_id>/markets      → full market book for one match
  GET  /api/sp/status                       → harvest timestamps + health

Query params (all list endpoints)
----------------------------------
  sort     = start_time | competition | market_count    default: start_time
  order    = asc | desc                                 default: asc
  comp     = <partial name>                             competition filter
  team     = <partial name>                             team filter
  market   = <slug>                                     market filter
  date     = YYYY-MM-DD                                 date filter
  page     = int                                        default 1
  per_page = int                                        default 25, max 100

  Direct / realtime only:
  days     = int    days ahead to fetch      (default 1 esoccer / 3 soccer, max 7)
  max      = int    max matches              (default 60 esoccer / 150 soccer)
  live     = 0|1    fetch live (realtime only, default 0)

Response shape (every list endpoint)
--------------------------------------
{
  "ok":           true,
  "source":       "sportpesa",
  "sport":        "soccer",
  "mode":         "upcoming" | "live" | "direct_upcoming" | "direct_live",
  "is_football":  bool,
  "total":        int,
  "page":         int,
  "per_page":     int,
  "pages":        int,
  "harvested_at": str | null,
  "latency_ms":   int,
  "competitions": [...],
  "matches":      [ <match_dict>, ... ],

  # cached endpoints only:
  "cached_total":        int,
  "harvest_latency_ms":  int,

  # direct endpoints only:
  "fresh":         true,
  "fetched_total": int,
  "days_fetched":  int,
}
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, request

from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response

bp_sp = Blueprint("sp", __name__, url_prefix="/api/sp")

_SPORTS = [
    "soccer", "esoccer",
    "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
    "boxing", "handball", "mma",
]

_FOOTBALL_SPORTS = {"soccer", "esoccer", "football", "efootball"}
_ESOCCER_SLUGS   = {"esoccer", "efootball", "e-football"}


# =============================================================================
# Cache helpers
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
# Filter / sort / paginate helpers
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
    return sorted(matches, key=lambda m: _parse_dt(m.get("start_time")), reverse=reverse)


def _paginate(matches: list[dict], page: int, per_page: int) -> tuple[list[dict], int]:
    total = len(matches)
    start = (page - 1) * per_page
    return matches[start: start + per_page], total


def _envelope(
    matches:      list[dict],
    total:        int,
    sport:        str,
    mode:         str,
    page:         int,
    per_page:     int,
    harvested_at: str | None,
    latency_ms:   int,
    extra:        dict | None = None,
) -> dict:
    comps = sorted({m.get("competition", "") for m in matches if m.get("competition")})
    out = {
        "ok":           True,
        "source":       "sportpesa",
        "sport":        sport,
        "mode":         mode,
        "is_football":  sport in _FOOTBALL_SPORTS,
        "total":        total,
        "page":         page,
        "per_page":     per_page,
        "pages":        max(1, (total + per_page - 1) // per_page),
        "harvested_at": harvested_at,
        "latency_ms":   latency_ms,
        "competitions": comps,
        "matches":      matches,
    }
    if extra:
        out.update(extra)
    return out


def _get_args():
    page     = max(1, int(request.args.get("page",     1)  or 1))
    per_page = min(int(request.args.get("per_page",   25)  or 25), 100)
    sort     = request.args.get("sort",  "start_time") or "start_time"
    order    = request.args.get("order", "asc")        or "asc"
    return page, per_page, sort, order


# =============================================================================
# Internal: run a direct SP fetch + write Redis + emit SSE
# =============================================================================

def _run_direct_fetch(
    sport_slug: str,
    is_live:    bool,
    days:       int,
    max_m:      int,
) -> tuple[list[dict], str, int]:
    """
    Call the harvester directly (no sleep_between — uses harvester default),
    persist result to Redis, fire SSE update.

    Returns (matches, harvested_at_iso, days_fetched).
    Raises on harvester failure so callers can return _err().
    """
    from app.workers.sp_harvester import fetch_upcoming, fetch_live  # noqa: PLC0415

    if is_live:
        matches      = fetch_live(sport_slug, fetch_full_markets=True)
        mode, ttl    = "live", 90
        days_fetched = 0
    else:
        matches      = fetch_upcoming(
            sport_slug,
            days=days,
            max_matches=max_m,
            fetch_full_markets=True,
        )
        mode, ttl    = "upcoming", 360
        days_fetched = days

    harvested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Write back to Redis so cached endpoints stay warm
    _cache_set(f"sp:{mode}:{sport_slug}", {
        "source":       "sportpesa",
        "sport":        sport_slug,
        "mode":         mode,
        "match_count":  len(matches),
        "harvested_at": harvested_at,
        "latency_ms":   0,
        "matches":      matches,
    }, ttl=ttl)

    # SSE push so cached-mode browser tabs auto-refresh
    try:
        from app.workers.celery_tasks import emit_sse_event  # noqa: PLC0415
        emit_sse_event("odds_updated", {
            "source": "sportpesa",
            "sport":  sport_slug,
            "mode":   mode,
            "count":  len(matches),
        })
    except Exception:
        pass

    return matches, harvested_at, days_fetched


# =============================================================================
# /api/sp/sports
# =============================================================================

@bp_sp.route("/sports")
def list_sports():
    t0     = time.perf_counter()
    result = []
    for slug in _SPORTS:
        cached = _cache_get(f"sp:upcoming:{slug}")
        live   = _cache_get(f"sp:live:{slug}")
        result.append({
            "slug":          slug,
            "label":         slug.replace("-", " ").title(),
            "is_football":   slug in _FOOTBALL_SPORTS,
            "upcoming":      (cached or {}).get("match_count", 0),
            "live":          (live   or {}).get("match_count", 0),
            "last_harvest":  (cached or {}).get("harvested_at"),
            "latency_ms":    (cached or {}).get("latency_ms"),
        })
    return _signed_response({
        "ok":         True,
        "source":     "sportpesa",
        "sports":     result,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# /api/sp/upcoming/<sport_slug>  — cached
# =============================================================================

@bp_sp.route("/upcoming/<sport_slug>")
def get_upcoming_cached(sport_slug: str):
    """Return upcoming matches from Redis. Instant — no SP API call."""
    t0                          = time.perf_counter()
    page, per_page, sort, order = _get_args()

    cached = _cache_get(f"sp:upcoming:{sport_slug}")
    if not cached:
        return _signed_response(_envelope(
            [], 0, sport_slug, "upcoming",
            page, per_page, None,
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
        extra={
            "cached_total":       cached.get("match_count", 0),
            "harvest_latency_ms": cached.get("latency_ms"),
        },
    ))


# =============================================================================
# /api/sp/live/<sport_slug>  — cached
# =============================================================================

@bp_sp.route("/live/<sport_slug>")
def get_live_cached(sport_slug: str):
    """Return live matches from Redis (refreshed every 60 s by beat)."""
    t0                          = time.perf_counter()
    page, per_page, sort, order = _get_args()

    cached = _cache_get(f"sp:live:{sport_slug}")
    if not cached:
        return _signed_response(_envelope(
            [], 0, sport_slug, "live",
            page, per_page, None,
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
        extra={
            "cached_total":       cached.get("match_count", 0),
            "harvest_latency_ms": cached.get("latency_ms"),
        },
    ))


# =============================================================================
# /api/sp/direct/upcoming/<sport_slug>  — fresh from SP API
# =============================================================================

@bp_sp.route("/direct/upcoming/<sport_slug>")
def direct_upcoming(sport_slug: str):
    """
    Fetch upcoming matches DIRECTLY from the Sportpesa API.
    Always hits SP API, never serves from Redis.
    Always writes result back to Redis so /upcoming stays warm.

    Extra params: days (default 1/3), max (default 60/150)
    """
    t0                          = time.perf_counter()
    page, per_page, sort, order = _get_args()

    is_virtual = sport_slug.lower() in _ESOCCER_SLUGS
    days  = min(int(request.args.get("days", 1 if is_virtual else 3) or 3), 7)
    max_m = min(int(request.args.get("max",  60 if is_virtual else 150) or 150), 300)

    try:
        matches, harvested_at, days_fetched = _run_direct_fetch(
            sport_slug, is_live=False, days=days, max_m=max_m,
        )
    except Exception as exc:
        return _err(f"SP direct fetch error: {exc}", 500)

    fetched_total = len(matches)

    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response(_envelope(
        paged, total, sport_slug, "direct_upcoming",
        page, per_page, harvested_at,
        int((time.perf_counter() - t0) * 1000),
        extra={
            "fresh":         True,
            "fetched_total": fetched_total,
            "days_fetched":  days_fetched,
        },
    ))


# =============================================================================
# /api/sp/direct/live/<sport_slug>  — fresh from SP API
# =============================================================================

@bp_sp.route("/direct/live/<sport_slug>")
def direct_live(sport_slug: str):
    """
    Fetch LIVE matches directly from the Sportpesa API.
    Always hits SP API and always updates the Redis live cache.
    """
    t0                          = time.perf_counter()
    page, per_page, sort, order = _get_args()

    try:
        matches, harvested_at, _ = _run_direct_fetch(
            sport_slug, is_live=True, days=1, max_m=300,
        )
    except Exception as exc:
        return _err(f"SP direct live error: {exc}", 500)

    fetched_total = len(matches)

    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response(_envelope(
        paged, total, sport_slug, "direct_live",
        page, per_page, harvested_at,
        int((time.perf_counter() - t0) * 1000),
        extra={
            "fresh":         True,
            "fetched_total": fetched_total,
        },
    ))


# =============================================================================
# /api/sp/match/<game_id>/markets
# =============================================================================

@bp_sp.route("/match/<game_id>/markets")
def get_match_markets(game_id: str):
    """Full market book for one SP match (on-demand, direct API call)."""
    t0 = time.perf_counter()
    try:
        from app.workers.sp_harvester import fetch_match_markets
        markets = fetch_match_markets(game_id)
    except Exception as exc:
        return _err(f"SP markets fetch error: {exc}", 500)

    return _signed_response({
        "ok":           True,
        "sp_game_id":   game_id,
        "source":       "sportpesa",
        "markets":      markets,
        "market_count": len(markets),
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# /api/sp/status
# =============================================================================

@bp_sp.route("/status")
def sp_status():
    """Cache freshness and harvest stats for all SP sports."""
    t0     = time.perf_counter()
    sports = []
    for slug in _SPORTS:
        up   = _cache_get(f"sp:upcoming:{slug}") or {}
        live = _cache_get(f"sp:live:{slug}")     or {}
        sports.append({
            "sport":              slug,
            "is_football":        slug in _FOOTBALL_SPORTS,
            "upcoming_count":     up.get("match_count",   0),
            "upcoming_harvested": up.get("harvested_at"),
            "upcoming_latency":   up.get("latency_ms"),
            "live_count":         live.get("match_count", 0),
            "live_harvested":     live.get("harvested_at"),
        })

    hb = _cache_get("worker_heartbeat") or {}
    return _signed_response({
        "ok":              True,
        "source":          "sportpesa",
        "worker_alive":    hb.get("alive", False),
        "last_heartbeat":  hb.get("checked_at"),
        "sports":          sports,
        "football_sports": sorted(_FOOTBALL_SPORTS),
        "latency_ms":      int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# /api/sp/realtime/<sport_slug>  — LEGACY ALIAS
# =============================================================================

@bp_sp.route("/realtime/<sport_slug>")
def get_realtime(sport_slug: str):
    """
    Legacy endpoint — kept so existing callers don't break.
    Delegates to _run_direct_fetch (same as /direct/upcoming or /direct/live).

    Extra params: days, max, live (0|1)
    """
    t0                          = time.perf_counter()
    page, per_page, sort, order = _get_args()

    is_live    = request.args.get("live", "0") == "1"
    is_virtual = sport_slug.lower() in _ESOCCER_SLUGS
    days  = min(int(request.args.get("days", 1 if is_virtual else 2) or 2), 7)
    max_m = min(int(request.args.get("max",  60 if is_virtual else 80) or 80), 300)

    try:
        matches, harvested_at, days_fetched = _run_direct_fetch(
            sport_slug, is_live=is_live, days=days, max_m=max_m,
        )
    except Exception as exc:
        return _err(f"SP realtime error: {exc}", 500)

    fetched_total = len(matches)

    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response(_envelope(
        paged, total, sport_slug,
        "direct_live" if is_live else "direct_upcoming",
        page, per_page, harvested_at,
        int((time.perf_counter() - t0) * 1000),
        extra={
            "fresh":         True,
            "fetched_total": fetched_total,
            "days_fetched":  days_fetched,
        },
    ))