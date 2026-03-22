"""
app/views/odds_feed/sp_module.py
=================================
Dedicated Sportpesa Flask blueprint.

Register in your app factory:
    from app.views.odds_feed.sp_module import bp_sp
    app.register_blueprint(bp_sp)

Endpoints
─────────
  ── Cache-served (instant, from Redis) ───────────────────────────────────
  GET  /api/sp/sports                           → sport list + cache stats
  GET  /api/sp/upcoming/<sport>                 → cached upcoming
  GET  /api/sp/live/<sport>                     → cached live
  GET  /api/sp/status                           → harvest timestamps + health

  ── Direct SP API (always fresh, always writes cache) ────────────────────
  GET  /api/sp/direct/upcoming/<sport>          → live fetch upcoming + update cache
  GET  /api/sp/direct/live/<sport>              → live fetch live + update cache
  GET  /api/sp/direct/refresh                   → bulk refresh 1+ sports
  GET  /api/sp/direct/competition/<sport>/<comp>→ fetch one competition directly

  ── On-demand market detail ───────────────────────────────────────────────
  GET  /api/sp/match/<game_id>/markets          → full market book (on-demand)

  ── Legacy alias (kept for back-compat) ──────────────────────────────────
  GET  /api/sp/realtime/<sport>                 → alias for /direct/upcoming

Sports served
─────────────
  soccer    real football   (cache key: sp:upcoming:soccer)
  esoccer   eFootball/virtual football  (cache key: sp:upcoming:esoccer)
  basketball, tennis, ice-hockey, volleyball, cricket, rugby,
  table-tennis, boxing, handball, mma

Query params (all list endpoints)
──────────────────────────────────
  sort     = start_time | competition | market_count   (default: start_time)
  order    = asc | desc                                (default: asc)
  comp     = partial competition name
  team     = partial team name
  market   = market slug (e.g. "1x2", "over_under_goals")
  date     = YYYY-MM-DD
  page     = int  (default 1)
  per_page = int  (default 25, max 100)

Standard response envelope
───────────────────────────
{
  "ok":           true,
  "source":       "sportpesa",
  "sport":        "soccer",
  "mode":         "upcoming"|"live"|"realtime",
  "total":        int,
  "page":         int,
  "per_page":     int,
  "pages":        int,
  "harvested_at": str | null,
  "latency_ms":   int,
  "competitions": [...],
  "matches":      [...]
}
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, request

bp_sp = Blueprint("sp", __name__, url_prefix="/api/sp")

# ── All sports served ─────────────────────────────────────────────────────────
_SPORTS = [
    # Football (real + virtual) — the focus of this module
    "soccer",
    "esoccer",
    # Other sports
    "basketball",
    "tennis",
    "ice-hockey",
    "volleyball",
    "cricket",
    "rugby",
    "table-tennis",
    "boxing",
    "handball",
    "mma",
]

# Sports that share football market structure (used for hint in API response)
_FOOTBALL_SPORTS = {"soccer", "esoccer", "football", "efootball"}


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

def _parse_dt(val) -> datetime:
    if not val:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


def _apply_filters(matches: list[dict], args) -> list[dict]:
    comp    = (args.get("comp")   or args.get("competition") or "").strip().lower()
    team    = (args.get("team")   or "").strip().lower()
    market  = (args.get("market") or "").strip().lower()
    date_s  = (args.get("date")   or "").strip()

    fdt = tdt = None
    if date_s:
        try:
            day = datetime.strptime(date_s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            fdt, tdt = day, day + timedelta(days=1)
        except ValueError:
            pass

    result = []
    for m in matches:
        if comp and comp not in (m.get("competition") or "").lower():
            continue
        if team:
            ht = (m.get("home_team") or "").lower()
            at = (m.get("away_team") or "").lower()
            if team not in ht and team not in at:
                continue
        if market:
            mkt_keys = (m.get("markets") or {}).keys()
            # Prefix match: "over_under" matches "over_under_goals_2.5"
            if not any(market in k for k in mkt_keys):
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
        return sorted(matches, key=lambda m: m.get("competition") or "", reverse=reverse)
    if sort == "market_count":
        return sorted(matches, key=lambda m: m.get("market_count") or 0, reverse=not reverse)
    # Default: start_time ascending
    return sorted(matches, key=lambda m: _parse_dt(m.get("start_time")), reverse=reverse)


def _paginate(matches: list[dict], page: int, per_page: int):
    total = len(matches)
    start = (page - 1) * per_page
    return matches[start: start + per_page], total


def _envelope(
    matches: list[dict],
    total: int,
    sport: str,
    mode: str,
    page: int,
    per_page: int,
    harvested_at,
    latency_ms: int,
    extra: dict | None = None,
) -> dict:
    comps = sorted({m.get("competition") or "" for m in matches if m.get("competition")})
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
    """Extract and validate common query params."""
    page     = max(1, int(request.args.get("page", 1) or 1))
    per_page = min(int(request.args.get("per_page", 25) or 25), 100)
    sort     = request.args.get("sort", "start_time") or "start_time"
    order    = request.args.get("order", "asc") or "asc"
    return page, per_page, sort, order


def _err(msg: str, code: int = 400):
    from flask import jsonify
    return jsonify({"ok": False, "error": msg}), code


def _ok(data: dict):
    from flask import jsonify
    return jsonify(data)


# =============================================================================
# /api/sp/sports
# =============================================================================

@bp_sp.route("/sports")
def list_sports():
    t0 = time.perf_counter()
    result = []
    for slug in _SPORTS:
        up   = _cache_get(f"sp:upcoming:{slug}") or {}
        live = _cache_get(f"sp:live:{slug}")     or {}
        result.append({
            "slug":          slug,
            "label":         slug.replace("-", " ").title(),
            "is_football":   slug in _FOOTBALL_SPORTS,
            "upcoming":      up.get("match_count", 0),
            "live":          live.get("match_count", 0),
            "last_harvest":  up.get("harvested_at"),
            "latency_ms":    up.get("latency_ms"),
        })
    return _ok({
        "ok":         True,
        "source":     "sportpesa",
        "sports":     result,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# /api/sp/upcoming/<sport_slug>
# =============================================================================

@bp_sp.route("/upcoming/<sport_slug>")
def get_upcoming(sport_slug: str):
    """
    Return upcoming matches from the Redis cache.
    Data is written by the Celery beat task every 5 minutes.
    Instant response — no SP API call.
    """
    t0                        = time.perf_counter()
    page, per_page, sort, ord = _get_args()

    cached = _cache_get(f"sp:upcoming:{sport_slug}")
    if not cached:
        return _ok(_envelope(
            [], 0, sport_slug, "upcoming",
            page, per_page, None,
            int((time.perf_counter() - t0) * 1000),
        ))

    matches = cached.get("matches") or []
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, ord)
    paged, total = _paginate(matches, page, per_page)

    return _ok(_envelope(
        paged, total, sport_slug, "upcoming",
        page, per_page, cached.get("harvested_at"),
        int((time.perf_counter() - t0) * 1000),
        extra={
            "cached_total":  cached.get("match_count", 0),
            "harvest_latency_ms": cached.get("latency_ms"),
        },
    ))


# =============================================================================
# /api/sp/live/<sport_slug>
# =============================================================================

@bp_sp.route("/live/<sport_slug>")
def get_live(sport_slug: str):
    """Return live matches from the Redis cache (refreshed every 60 s)."""
    t0                        = time.perf_counter()
    page, per_page, sort, ord = _get_args()

    cached = _cache_get(f"sp:live:{sport_slug}")
    if not cached:
        return _ok(_envelope(
            [], 0, sport_slug, "live",
            page, per_page, None,
            int((time.perf_counter() - t0) * 1000),
        ))

    matches = cached.get("matches") or []
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, ord)
    paged, total = _paginate(matches, page, per_page)

    return _ok(_envelope(
        paged, total, sport_slug, "live",
        page, per_page, cached.get("harvested_at"),
        int((time.perf_counter() - t0) * 1000),
    ))


# =============================================================================
# /api/sp/realtime/<sport_slug>
# =============================================================================

@bp_sp.route("/realtime/<sport_slug>")
def get_realtime(sport_slug: str):
    """
    Fetch fresh data directly from the SP API, update the Redis cache,
    and return the result.

    ⚠ Makes N HTTP requests to SP (one per match for full markets).
    Use for manual refresh only — the beat task keeps the cache warm.

    Extra query params
    ──────────────────
      days    int  days ahead to fetch           (default 2, max 7)
      max     int  max matches (default 40, max 150)
      live    0|1  fetch live instead of upcoming (default 0)
    """
    t0                        = time.perf_counter()
    page, per_page, sort, ord = _get_args()

    days    = min(int(request.args.get("days", 2) or 2), 7)
    max_m   = min(int(request.args.get("max",  40) or 40), 150)
    is_live = request.args.get("live", "0") == "1"

    try:
        from app.workers.sp_harvester import fetch_upcoming, fetch_live
        if is_live:
            matches = fetch_live(sport_slug, fetch_full_markets=True)
            mode, ttl = "live", 90
        else:
            matches = fetch_upcoming(
                sport_slug,
                days=days,
                max_matches=max_m,
                fetch_full_markets=True,
            )
            mode, ttl = "realtime", 300
    except Exception as exc:
        return _err(f"SP fetch error: {exc}", 500)

    harvested_at = datetime.now(timezone.utc).isoformat()
    latency_ms   = int((time.perf_counter() - t0) * 1000)

    # Update cache
    cache_key = f"sp:{'live' if is_live else 'upcoming'}:{sport_slug}"
    _cache_set(cache_key, {
        "source":       "sportpesa",
        "sport":        sport_slug,
        "mode":         mode,
        "match_count":  len(matches),
        "harvested_at": harvested_at,
        "latency_ms":   latency_ms,
        "matches":      matches,
    }, ttl=ttl)

    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, ord)
    paged, total = _paginate(matches, page, per_page)

    return _ok(_envelope(
        paged, total, sport_slug, mode,
        page, per_page, harvested_at, latency_ms,
        extra={"fresh": True},
    ))


# =============================================================================
# /api/sp/match/<game_id>/markets
# =============================================================================

@bp_sp.route("/match/<game_id>/markets")
def get_match_markets(game_id: str):
    """
    Fetch the complete normalised market book for one SP game (on-demand).
    Returns all 33 market types including all O/U lines.
    """
    t0 = time.perf_counter()
    try:
        from app.workers.sp_harvester import fetch_match_markets
        markets = fetch_match_markets(game_id)
    except Exception as exc:
        return _err(f"SP markets fetch error: {exc}", 500)

    return _ok({
        "ok":           True,
        "sp_game_id":   game_id,
        "source":       "sportpesa",
        "market_count": len(markets),
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
        "markets":      markets,
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

    # Worker heartbeat
    hb = _cache_get("worker_heartbeat") or {}

    return _ok({
        "ok":              True,
        "source":          "sportpesa",
        "worker_alive":    hb.get("alive", False),
        "last_heartbeat":  hb.get("checked_at"),
        "sports":          sports,
        "football_sports": sorted(_FOOTBALL_SPORTS),
        "latency_ms":      int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# DIRECT-FETCH HELPERS
# =============================================================================

def _do_direct_fetch(
    sport_slug: str,
    is_live: bool,
    days: int,
    max_matches: int,
) -> tuple[list[dict], str, int]:
    """
    Call the SP harvester directly, write result to Redis, return
    (matches, harvested_at, latency_ms).
    Raises RuntimeError on failure.
    """
    t0 = time.perf_counter()

    from app.workers.sp_harvester import fetch_upcoming, fetch_live  # noqa: PLC0415

    if is_live:
        matches = fetch_live(sport_slug, fetch_full_markets=True)
        mode, ttl = "live", 90
    else:
        is_virtual = sport_slug in ("esoccer", "efootball")
        matches = fetch_upcoming(
            sport_slug,
            days=days,
            fetch_full_markets=True,
            max_matches=max_matches,
            sleep_between=0.06 if is_virtual else 0.08,
        )
        mode, ttl = "upcoming", 360

    harvested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    latency_ms   = int((time.perf_counter() - t0) * 1000)

    # ── Always update the Redis cache ──────────────────────────────────────
    cache_key = f"sp:{mode}:{sport_slug}"
    _cache_set(cache_key, {
        "source":       "sportpesa",
        "sport":        sport_slug,
        "mode":         mode,
        "match_count":  len(matches),
        "harvested_at": harvested_at,
        "latency_ms":   latency_ms,
        "matches":      matches,
    }, ttl=ttl)

    # ── Emit SSE so connected browsers refresh automatically ───────────────
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

    return matches, harvested_at, latency_ms


# =============================================================================
# /api/sp/direct/upcoming/<sport>
# =============================================================================

@bp_sp.route("/direct/upcoming/<sport_slug>")
def direct_upcoming(sport_slug: str):
    """
    Fetch upcoming matches DIRECTLY from the Sportpesa API.

    Always hits SP API (never serves from cache).
    Always writes result back to Redis so /upcoming/<sport> is up to date.
    Returns fresh data in the same envelope as /upcoming/<sport>.

    Query params
    ──────────────
      days       int  days ahead (default 2 for esoccer, 3 for soccer, max 7)
      max        int  max matches (default 60 esoccer / 150 soccer, hard cap 300)
      page       int  (default 1)
      per_page   int  (default 25, max 100)
      sort / order / comp / team / market / date  — same as cached endpoints
    """
    t0 = time.perf_counter()
    page, per_page, sort, ord_ = _get_args()

    is_virtual = sport_slug in ("esoccer", "efootball")
    days    = min(int(request.args.get("days",  1 if is_virtual else 3) or 3), 7)
    max_m   = min(int(request.args.get("max",  60 if is_virtual else 150) or 150), 300)

    try:
        matches, harvested_at, latency_ms = _do_direct_fetch(
            sport_slug, is_live=False, days=days, max_matches=max_m,
        )
    except Exception as exc:
        return _err(f"SP direct fetch error: {exc}", 500)

    all_matches = _apply_filters(matches, request.args)
    all_matches = _sort_matches(all_matches, sort, ord_)
    paged, total = _paginate(all_matches, page, per_page)

    return _ok(_envelope(
        paged, total, sport_slug, "direct_upcoming",
        page, per_page, harvested_at,
        int((time.perf_counter() - t0) * 1000),
        extra={
            "fresh":         True,
            "fetched_total": len(matches),
            "days_fetched":  days,
        },
    ))


# =============================================================================
# /api/sp/direct/live/<sport>
# =============================================================================

@bp_sp.route("/direct/live/<sport_slug>")
def direct_live(sport_slug: str):
    """
    Fetch LIVE matches directly from the Sportpesa API.

    Always hits SP API and always updates the Redis live cache.
    Use this for the live ticker; the Celery beat refreshes every 60 s
    but this endpoint lets you force an immediate update.
    """
    t0 = time.perf_counter()
    page, per_page, sort, ord_ = _get_args()

    try:
        matches, harvested_at, latency_ms = _do_direct_fetch(
            sport_slug, is_live=True, days=1, max_matches=200,
        )
    except Exception as exc:
        return _err(f"SP direct live error: {exc}", 500)

    all_matches = _apply_filters(matches, request.args)
    all_matches = _sort_matches(all_matches, sort, ord_)
    paged, total = _paginate(all_matches, page, per_page)

    return _ok(_envelope(
        paged, total, sport_slug, "direct_live",
        page, per_page, harvested_at,
        int((time.perf_counter() - t0) * 1000),
        extra={
            "fresh":         True,
            "fetched_total": len(matches),
        },
    ))


# =============================================================================
# /api/sp/direct/refresh
# =============================================================================

@bp_sp.route("/direct/refresh")
def direct_refresh():
    """
    Bulk-refresh one or more sports directly from the SP API.
    Updates the Redis cache for every sport fetched.

    Query params
    ──────────────
      sports   comma-separated slugs, default "soccer,esoccer"
               e.g. ?sports=soccer,esoccer,basketball
      mode     "upcoming" | "live" | "both"  (default: upcoming)
      days     int  (default 2, max 5)
      max      int  per sport (default 80, max 200)

    Response
    ─────────
    {
      "ok": true,
      "refreshed": [
        { "sport": "soccer",  "mode": "upcoming", "count": 120, "latency_ms": 4200 },
        { "sport": "esoccer", "mode": "upcoming", "count": 24,  "latency_ms": 890  },
        ...
      ],
      "errors": [...],
      "total_latency_ms": int
    }
    """
    t0 = time.perf_counter()

    sports_raw = request.args.get("sports", "soccer,esoccer")
    sports = [s.strip() for s in sports_raw.split(",") if s.strip()]
    # Safety cap — don't let a bad actor request 20 sports at once
    sports = sports[:6]

    fetch_mode = request.args.get("mode", "upcoming").lower()
    is_virtual = lambda s: s in ("esoccer", "efootball")
    days  = min(int(request.args.get("days", 2) or 2), 5)
    max_m = min(int(request.args.get("max",  80) or 80), 200)

    refreshed = []
    errors    = []

    modes_to_run = []
    if fetch_mode in ("upcoming", "both"):
        modes_to_run.append(False)   # is_live=False
    if fetch_mode in ("live", "both"):
        modes_to_run.append(True)    # is_live=True

    for sport_slug in sports:
        for live_flag in modes_to_run:
            tt = time.perf_counter()
            try:
                matches, harvested_at, _ = _do_direct_fetch(
                    sport_slug,
                    is_live=live_flag,
                    days=1 if live_flag else days,
                    max_matches=200 if live_flag else max_m,
                )
                refreshed.append({
                    "sport":        sport_slug,
                    "mode":         "live" if live_flag else "upcoming",
                    "count":        len(matches),
                    "harvested_at": harvested_at,
                    "latency_ms":   int((time.perf_counter() - tt) * 1000),
                })
            except Exception as exc:
                errors.append({
                    "sport": sport_slug,
                    "mode":  "live" if live_flag else "upcoming",
                    "error": str(exc),
                })

    return _ok({
        "ok":               len(errors) == 0,
        "source":           "sportpesa",
        "refreshed":        refreshed,
        "errors":           errors,
        "total_sports":     len(refreshed) + len(errors),
        "total_latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# /api/sp/direct/competition/<sport>/<competition>
# =============================================================================

@bp_sp.route("/direct/competition/<sport_slug>/<path:competition_name>")
def direct_competition(sport_slug: str, competition_name: str):
    """
    Fetch upcoming matches for ONE competition directly from the SP API.

    Fetches 1–2 days of data for the sport, filters to the requested
    competition (case-insensitive partial match), updates the full cache,
    and returns just the competition's matches.

    Example:
      GET /api/sp/direct/competition/soccer/Premier%20League
      GET /api/sp/direct/competition/soccer/SportPesa%20League

    Query params
    ─────────────
      days     int  (default 2, max 5)
      max      int  (default 200)
      sort / order / team / market / date
    """
    t0 = time.perf_counter()
    page, per_page, sort, ord_ = _get_args()

    is_virtual = sport_slug in ("esoccer", "efootball")
    days  = min(int(request.args.get("days", 1 if is_virtual else 2) or 2), 5)
    max_m = min(int(request.args.get("max", 200) or 200), 300)

    try:
        matches, harvested_at, _ = _do_direct_fetch(
            sport_slug, is_live=False, days=days, max_matches=max_m,
        )
    except Exception as exc:
        return _err(f"SP competition fetch error: {exc}", 500)

    # Filter to the requested competition
    comp_lower = competition_name.strip().lower()
    comp_matches = [
        m for m in matches
        if comp_lower in (m.get("competition") or "").lower()
    ]

    # Collect unique competition names that matched (for the caller)
    matched_comps = sorted({m.get("competition", "") for m in comp_matches})

    comp_matches = _apply_filters(comp_matches, request.args)
    comp_matches = _sort_matches(comp_matches, sort, ord_)
    paged, total  = _paginate(comp_matches, page, per_page)

    return _ok(_envelope(
        paged, total, sport_slug, "direct_competition",
        page, per_page, harvested_at,
        int((time.perf_counter() - t0) * 1000),
        extra={
            "fresh":              True,
            "competition_filter": competition_name,
            "matched_competitions": matched_comps,
            "total_fetched":      len(matches),
        },
    ))


# =============================================================================
# /api/sp/realtime/<sport>  — legacy alias  (kept for back-compat)
# =============================================================================

@bp_sp.route("/realtime/<sport_slug>")
def get_realtime(sport_slug: str):
    """
    Legacy endpoint — forwards to /direct/upcoming/<sport>.
    Kept so existing callers don't break.

    Extra params: days, max, live (0|1)
    """
    t0 = time.perf_counter()
    page, per_page, sort, ord_ = _get_args()

    is_virtual = sport_slug in ("esoccer", "efootball")
    days    = min(int(request.args.get("days", 1 if is_virtual else 2) or 2), 7)
    max_m   = min(int(request.args.get("max",  60 if is_virtual else 80) or 80), 300)
    is_live = request.args.get("live", "0") == "1"

    try:
        matches, harvested_at, _ = _do_direct_fetch(
            sport_slug, is_live=is_live, days=days, max_matches=max_m,
        )
    except Exception as exc:
        return _err(f"SP realtime error: {exc}", 500)

    all_matches = _apply_filters(matches, request.args)
    all_matches = _sort_matches(all_matches, sort, ord_)
    paged, total = _paginate(all_matches, page, per_page)

    return _ok(_envelope(
        paged, total, sport_slug,
        "live" if is_live else "realtime",
        page, per_page, harvested_at,
        int((time.perf_counter() - t0) * 1000),
        extra={"fresh": True, "fetched_total": len(matches)},
    ))