"""
app/views/odds_feed/sp_module.py  (v3 — all sports + market metadata + line-aware labels)
==========================================================================================

Changes in v3
──────────────
• get_match_markets() enrichment now uses get_outcome_display(slug, key) from
  sp_mapper instead of OUTCOME_DISPLAY.get(slug.split("_")[0]) which was
  broken for all line-suffixed slugs (O/U, handicap, Euro HC, Result+O/U).
• markets_list[] added to /match/<id>/markets response — ordered list with
  primary markets first, each entry has slug/label/base_slug/line/is_primary.
• get_market_display_name() now returns "Goals O/U 2.5" instead of generic
  title-case for line markets.
• Import set updated: OUTCOME_DISPLAY no longer needed in this module.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, Response, request, stream_with_context

from app.utils.customer_jwt_helpers import _err, _signed_response

bp_sp = Blueprint("sp", __name__, url_prefix="/api/sp")

_SPORTS = [
    "soccer", "esoccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis", "boxing",
    "handball", "mma", "darts", "american-football", "baseball",
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

def _parse_dt(val) -> datetime:
    if not val:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


def _apply_filters(matches: list[dict], args) -> list[dict]:
    comp      = (args.get("comp")       or args.get("competition") or "").strip().lower()
    team      = (args.get("team")       or "").strip().lower()
    market    = (args.get("market")     or "").strip().lower()
    date_s    = (args.get("date")       or "").strip()
    league_id = (args.get("league_id")  or "").strip()

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
        if league_id and str(m.get("league_id", "")) != league_id:
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
    return matches[(page - 1) * per_page: page * per_page], total


def _envelope(matches, total, sport, mode, page, per_page,
              harvested_at, latency_ms, extra=None) -> dict:
    comps = sorted({m.get("competition", "") for m in matches if m.get("competition")})
    out = {
        "ok": True, "source": "sportpesa", "sport": sport, "mode": mode,
        "is_football": sport in _FOOTBALL_SPORTS,
        "total": total, "page": page, "per_page": per_page,
        "pages": max(1, (total + per_page - 1) // per_page),
        "harvested_at": harvested_at, "latency_ms": latency_ms,
        "competitions": comps, "matches": matches,
    }
    if extra:
        out.update(extra)
    return out


def _get_args():
    page     = max(1, int(request.args.get("page",    1)  or 1))
    per_page = min(int(request.args.get("per_page",  25)  or 25), 100)
    sort     = request.args.get("sort",  "start_time") or "start_time"
    order    = request.args.get("order", "asc")        or "asc"
    return page, per_page, sort, order


# =============================================================================
# Internal: blocking direct fetch + cache write
# =============================================================================

def _run_direct_fetch(sport_slug, is_live, days, max_m):
    from app.workers.sp_harvester import fetch_upcoming, fetch_live  # noqa

    if is_live:
        matches   = fetch_live(sport_slug, fetch_full_markets=True)
        mode, ttl = "live", 90
        days_out  = 0
    else:
        matches   = fetch_upcoming(sport_slug, days=days, max_matches=max_m,
                                   fetch_full_markets=True)
        mode, ttl = "upcoming", 360
        days_out  = days

    harvested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _cache_set(f"sp:{mode}:{sport_slug}", {
        "source": "sportpesa", "sport": sport_slug, "mode": mode,
        "match_count": len(matches), "harvested_at": harvested_at,
        "latency_ms": 0, "matches": matches,
    }, ttl=ttl)

    try:
        from app.workers.celery_tasks import emit_sse_event  # noqa
        emit_sse_event("odds_updated", {"source": "sportpesa", "sport": sport_slug,
                                        "mode": mode, "count": len(matches)})
    except Exception:
        pass

    return matches, harvested_at, days_out


# =============================================================================
# SSE helpers
# =============================================================================

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _sse_headers() -> dict:
    return {
        "Content-Type":      "text/event-stream",
        "Cache-Control":     "no-cache",
        "X-Accel-Buffering": "no",
        "Connection":        "keep-alive",
    }


# =============================================================================
# GET /api/sp/meta/sports  — sport list + primary markets
# =============================================================================

@bp_sp.route("/meta/sports")
def meta_sports():
    """Return all sports with their primary market slugs and display names."""
    from app.workers.sp_mapper import SPORT_META, SPORT_PRIMARY_MARKETS, get_market_display_name
    from app.workers.sp_sports import SPORT_ID_CONFIGS

    t0     = time.perf_counter()
    result = []

    for sport_id, meta in SPORT_META.items():
        cfg        = SPORT_ID_CONFIGS.get(sport_id)
        primary_ms = SPORT_PRIMARY_MARKETS.get(sport_id, ["match_winner"])
        cached_up  = _cache_get(f"sp:upcoming:{meta['slugs'][0]}") or {}
        cached_lv  = _cache_get(f"sp:live:{meta['slugs'][0]}")     or {}

        result.append({
            "sport_id":       sport_id,
            "name":           meta["name"],
            "emoji":          meta["emoji"],
            "slugs":          meta["slugs"],
            "primary_slug":   meta["slugs"][0],
            "days_default":   cfg.days_default if cfg else 3,
            "primary_markets": [
                {"slug": s, "label": get_market_display_name(s)}
                for s in primary_ms
            ],
            "upcoming_count": cached_up.get("match_count", 0),
            "live_count":     cached_lv.get("match_count", 0),
            "last_harvest":   cached_up.get("harvested_at"),
        })

    order = [1, 126, 2, 5, 4, 12, 23, 21, 6, 16, 117, 10, 49, 15, 3]
    result.sort(key=lambda r: order.index(r["sport_id"]) if r["sport_id"] in order else 99)

    return _signed_response({
        "ok": True, "source": "sportpesa",
        "sports": result,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# GET /api/sp/meta/markets/<sport_slug>  — all markets for a sport
# =============================================================================

@bp_sp.route("/meta/markets/<sport_slug>")
def meta_markets(sport_slug: str):
    """Return all canonical market slugs + display names for a sport."""
    from app.workers.sp_mapper import (
        get_sport_table, get_market_display_name,
        get_sport_primary_markets, get_sport_meta,
        OUTCOME_DISPLAY,
    )
    from app.workers.sp_sports import get_config

    t0  = time.perf_counter()
    cfg = get_config(sport_slug)
    if not cfg:
        return _err(f"Unknown sport: {sport_slug}", 404)

    table      = get_sport_table(cfg.sport_id)
    base_slugs = sorted({slug for slug, _ in table.values()})
    primary    = get_sport_primary_markets(cfg.sport_id)
    meta       = get_sport_meta(cfg.sport_id)

    markets = []
    for slug in base_slugs:
        markets.append({
            "slug":       slug,
            "label":      get_market_display_name(slug),
            "is_primary": slug in primary,
            "outcomes":   OUTCOME_DISPLAY.get(slug, {}),
        })

    markets.sort(key=lambda m: (0 if m["is_primary"] else 1, m["slug"]))

    return _signed_response({
        "ok":           True,
        "sport_slug":   sport_slug,
        "sport_id":     cfg.sport_id,
        "sport_name":   meta["name"],
        "markets":      markets,
        "market_count": len(markets),
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# GET /api/sp/sports
# =============================================================================

@bp_sp.route("/sports")
def list_sports():
    t0 = time.perf_counter()
    result = []
    for slug in _SPORTS:
        cached = _cache_get(f"sp:upcoming:{slug}")
        live   = _cache_get(f"sp:live:{slug}")
        result.append({
            "slug":         slug,
            "label":        slug.replace("-", " ").title(),
            "is_football":  slug in _FOOTBALL_SPORTS,
            "upcoming":     (cached or {}).get("match_count", 0),
            "live":         (live   or {}).get("match_count", 0),
            "last_harvest": (cached or {}).get("harvested_at"),
            "latency_ms":   (cached or {}).get("latency_ms"),
        })
    return _signed_response({
        "ok": True, "source": "sportpesa", "sports": result,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# GET /api/sp/upcoming/<sport>  — cached
# =============================================================================

@bp_sp.route("/upcoming/<sport_slug>")
def get_upcoming_cached(sport_slug: str):
    t0                          = time.perf_counter()
    page, per_page, sort, order = _get_args()

    cached = _cache_get(f"sp:upcoming:{sport_slug}")
    if not cached:
        return _signed_response(_envelope([], 0, sport_slug, "upcoming",
            page, per_page, None, int((time.perf_counter() - t0) * 1000)))

    matches = _apply_filters(cached.get("matches") or [], request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response(_envelope(
        paged, total, sport_slug, "upcoming", page, per_page,
        cached.get("harvested_at"), int((time.perf_counter() - t0) * 1000),
        extra={"cached_total": cached.get("match_count", 0),
               "harvest_latency_ms": cached.get("latency_ms")},
    ))


# =============================================================================
# GET /api/sp/live/<sport>  — cached
# =============================================================================

@bp_sp.route("/live/<sport_slug>")
def get_live_cached(sport_slug: str):
    t0                          = time.perf_counter()
    page, per_page, sort, order = _get_args()

    cached = _cache_get(f"sp:live:{sport_slug}")
    if not cached:
        return _signed_response(_envelope([], 0, sport_slug, "live",
            page, per_page, None, int((time.perf_counter() - t0) * 1000)))

    matches = _apply_filters(cached.get("matches") or [], request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response(_envelope(
        paged, total, sport_slug, "live", page, per_page,
        cached.get("harvested_at"), int((time.perf_counter() - t0) * 1000),
        extra={"cached_total": cached.get("match_count", 0),
               "harvest_latency_ms": cached.get("latency_ms")},
    ))


# =============================================================================
# GET /api/sp/direct/upcoming/<sport>  — blocking direct fetch
# =============================================================================

@bp_sp.route("/direct/upcoming/<sport_slug>")
def direct_upcoming(sport_slug: str):
    t0                          = time.perf_counter()
    page, per_page, sort, order = _get_args()

    is_virtual = sport_slug.lower() in _ESOCCER_SLUGS
    days  = min(int(request.args.get("days", 1 if is_virtual else 3) or 3), 7)
    max_m = min(int(request.args.get("max",  60 if is_virtual else 150) or 150), 300)

    try:
        matches, harvested_at, days_fetched = _run_direct_fetch(
            sport_slug, is_live=False, days=days, max_m=max_m)
    except Exception as exc:
        return _err(f"SP direct fetch error: {exc}", 500)

    fetched_total = len(matches)
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response(_envelope(
        paged, total, sport_slug, "direct_upcoming", page, per_page,
        harvested_at, int((time.perf_counter() - t0) * 1000),
        extra={"fresh": True, "fetched_total": fetched_total, "days_fetched": days_fetched},
    ))


# =============================================================================
# GET /api/sp/direct/live/<sport>  — blocking direct fetch
# =============================================================================

@bp_sp.route("/direct/live/<sport_slug>")
def direct_live(sport_slug: str):
    t0                          = time.perf_counter()
    page, per_page, sort, order = _get_args()

    try:
        matches, harvested_at, _ = _run_direct_fetch(
            sport_slug, is_live=True, days=1, max_m=300)
    except Exception as exc:
        return _err(f"SP direct live error: {exc}", 500)

    fetched_total = len(matches)
    matches = _apply_filters(matches, request.args)
    matches = _sort_matches(matches, sort, order)
    paged, total = _paginate(matches, page, per_page)

    return _signed_response(_envelope(
        paged, total, sport_slug, "direct_live", page, per_page,
        harvested_at, int((time.perf_counter() - t0) * 1000),
        extra={"fresh": True, "fetched_total": fetched_total},
    ))


# =============================================================================
# GET /api/sp/stream/upcoming/<sport>  — SSE streaming
# =============================================================================

@bp_sp.route("/stream/upcoming/<sport_slug>")
def stream_upcoming(sport_slug: str):
    is_virtual = sport_slug.lower() in _ESOCCER_SLUGS
    days  = min(int(request.args.get("days", 1 if is_virtual else 3) or 3), 7)
    max_m = min(int(request.args.get("max",  60 if is_virtual else 150) or 150), 300)

    @stream_with_context
    def generate():
        t0          = time.perf_counter()
        all_matches = []

        try:
            from app.workers.sp_harvester import fetch_upcoming_stream  # noqa

            gen = fetch_upcoming_stream(
                sport_slug, days=days, max_matches=max_m,
                fetch_full_markets=True, debug_ou=True,
            )

            yield _sse({"type": "start", "sport": sport_slug, "days": days,
                        "estimated_max": max_m})

            idx = 0
            for match in gen:
                idx += 1
                all_matches.append(match)
                yield _sse({"type": "match", "index": idx, "match": match})

            harvested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            latency_ms   = int((time.perf_counter() - t0) * 1000)
            _cache_set(f"sp:upcoming:{sport_slug}", {
                "source": "sportpesa", "sport": sport_slug, "mode": "upcoming",
                "match_count": len(all_matches), "harvested_at": harvested_at,
                "latency_ms": latency_ms, "matches": all_matches,
            }, ttl=360)

            try:
                from app.workers.celery_tasks import emit_sse_event  # noqa
                emit_sse_event("odds_updated", {"source": "sportpesa",
                               "sport": sport_slug, "mode": "upcoming",
                               "count": len(all_matches)})
            except Exception:
                pass

            yield _sse({"type": "done", "total": len(all_matches),
                        "latency_ms": latency_ms, "harvested_at": harvested_at,
                        "days_fetched": days})

        except Exception as exc:
            yield _sse({"type": "error", "message": str(exc)})

    return Response(generate(), headers=_sse_headers())


# =============================================================================
# GET /api/sp/stream/live/<sport>  — SSE streaming
# =============================================================================

@bp_sp.route("/stream/live/<sport_slug>")
def stream_live(sport_slug: str):
    @stream_with_context
    def generate():
        t0          = time.perf_counter()
        all_matches = []

        try:
            from app.workers.sp_harvester import fetch_live_stream  # noqa

            yield _sse({"type": "start", "sport": sport_slug, "mode": "live"})

            idx = 0
            for match in fetch_live_stream(sport_slug, fetch_full_markets=True,
                                            debug_ou=True):
                idx += 1
                all_matches.append(match)
                yield _sse({"type": "match", "index": idx, "match": match})

            harvested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            latency_ms   = int((time.perf_counter() - t0) * 1000)

            _cache_set(f"sp:live:{sport_slug}", {
                "source": "sportpesa", "sport": sport_slug, "mode": "live",
                "match_count": len(all_matches), "harvested_at": harvested_at,
                "latency_ms": latency_ms, "matches": all_matches,
            }, ttl=90)

            yield _sse({"type": "done", "total": len(all_matches),
                        "latency_ms": latency_ms, "harvested_at": harvested_at})

        except Exception as exc:
            yield _sse({"type": "error", "message": str(exc)})

    return Response(generate(), headers=_sse_headers())


# =============================================================================
# GET /api/sp/match/<game_id>/markets  — on-demand single match, full book
# =============================================================================

@bp_sp.route("/match/<game_id>/markets")
def get_match_markets(game_id: str):
    """
    Full market book for one SP game, with line-aware display labels.

    Every market entry in the response:
    {
      "slug":       "over_under_goals_2.5",
      "label":      "Goals O/U 2.5",
      "base_slug":  "over_under_goals",
      "line":       "2.5",
      "is_primary": true,
      "outcomes": {
        "over":  { "price": 1.38, "label": "Over 2.5"  },
        "under": { "price": 2.80, "label": "Under 2.5" }
      }
    }

    For Asian handicap 1.5:
      outcome "1" → { "price": 2.14, "label": "Home +1.5" }
      outcome "2" → { "price": 1.60, "label": "Away -1.5" }

    For Result+O/U 2.5:
      outcome "1_over"  → { "price": 1.70, "label": "Home Over 2.5" }
      outcome "2_under" → { "price": 4.60, "label": "Away Under 2.5" }
    """
    t0         = time.perf_counter()
    sport_slug = request.args.get("sport", "soccer")

    try:
        from app.workers.sp_harvester import fetch_match_markets
        from app.workers.sp_mapper    import (
            get_market_display_name,
            get_outcome_display,
            get_sport_primary_markets,
            extract_base_and_line,
        )
        from app.workers.sp_sports import get_config

        markets_raw = fetch_match_markets(game_id, sport_slug)
    except Exception as exc:
        return _err(f"SP markets fetch error: {exc}", 500)

    # Determine which markets are "primary" for this sport
    cfg         = get_config(sport_slug)
    sport_id    = cfg.sport_id if cfg else 1
    primary_set = set(get_sport_primary_markets(sport_id))

    # Build enriched list with line-aware labels
    enriched_list: list[dict] = []
    for slug, outcomes in markets_raw.items():
        base, line = extract_base_and_line(slug)
        is_primary = base in primary_set or slug in primary_set

        enriched_list.append({
            "slug":       slug,
            "label":      get_market_display_name(slug),
            "base_slug":  base,
            "line":       line,
            "is_primary": is_primary,
            "outcomes":   {
                k: {
                    "price": v,
                    "label": get_outcome_display(slug, k),
                }
                for k, v in outcomes.items()
            },
        })

    # Sort: primary first, then alphabetical by slug
    enriched_list.sort(key=lambda m: (0 if m["is_primary"] else 1, m["slug"]))

    # Also expose as dict keyed by slug for backward compat
    enriched_dict = {m["slug"]: m for m in enriched_list}

    return _signed_response({
        "ok":             True,
        "sp_game_id":     game_id,
        "source":         "sportpesa",
        "sport":          sport_slug,
        "markets":        enriched_dict,   # dict — backward compat
        "markets_list":   enriched_list,   # ordered list — primary first
        "market_count":   len(enriched_list),
        "has_over_under": any(
            "over_under" in k or k.startswith("total_")
            for k in markets_raw
        ),
        "latency_ms":     int((time.perf_counter() - t0) * 1000),
    })


# =============================================================================
# GET /api/sp/debug/markets/<game_id>
# =============================================================================

@bp_sp.route("/debug/markets/<game_id>")
def debug_markets(game_id: str):
    t0         = time.perf_counter()
    sport_slug = request.args.get("sport", "soccer")
    try:
        from app.workers.sp_harvester import _fetch_markets_for_debug, _parse_markets_for_debug  # noqa
        raw    = _fetch_markets_for_debug(game_id, sport_slug=sport_slug, debug=True)
        parsed = _parse_markets_for_debug(raw, game_id=game_id, sport_slug=sport_slug)
        mkt_ids = [m.get("id") for m in raw if isinstance(m, dict)]

        return _signed_response({
            "ok":            True,
            "game_id":       game_id,
            "sport_slug":    sport_slug,
            "raw_count":     len(raw),
            "market_ids":    mkt_ids,
            "has_ou_raw":    any(i in mkt_ids for i in [52, 18, 56]),
            "parsed_slugs":  list(parsed.keys()),
            "has_ou_parsed": any("over_under" in k or "total" in k for k in parsed),
            "parsed":        parsed,
            "latency_ms":    int((time.perf_counter() - t0) * 1000),
        })
    except Exception as exc:
        return _err(f"debug error: {exc}", 500)


# =============================================================================
# GET /api/sp/status
# =============================================================================

@bp_sp.route("/status")
def sp_status():
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
        "ok":             True,
        "source":         "sportpesa",
        "worker_alive":   hb.get("alive", False),
        "last_heartbeat": hb.get("checked_at"),
        "sports":         sports,
        "football_sports": sorted(_FOOTBALL_SPORTS),
        "latency_ms":     int((time.perf_counter() - t0) * 1000),
    })