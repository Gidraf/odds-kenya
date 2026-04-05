"""
app/views/odds_feed/combined_module.py
=============================
Multi-bookmaker combined data endpoints — SP + BT + OD.

LIVE is disabled by default (LIVE_ENABLED = False).
Set LIVE_ENABLED = True once upcoming is stable.

Endpoint map
────────────
  HEALTH
    GET  /api/combined/health
    GET  /api/combined/status

  UPCOMING  (active)
    SSE  GET /api/combined/stream/upcoming/<sport>
    GET  /api/combined/upcoming/<sport>

  LIVE  (disabled — returns 503 until LIVE_ENABLED = True)
    SSE  GET /api/combined/stream/live/<sport>
    GET  /api/combined/live/<sport>

  OPPORTUNITIES
    GET  /api/combined/opportunities/<sport>

  COMPARE
    GET  /api/combined/compare/<sport>?market=1x2

  OPS
    POST /api/combined/ops/trigger/<task_name>
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from flask import Blueprint, Response, request, stream_with_context

logger = logging.getLogger(__name__)

bp_combined = Blueprint("combined", __name__, url_prefix="/api/combined")

# ── Feature flags ─────────────────────────────────────────────────────────────
# Set LIVE_ENABLED = True once upcoming is fully stable and saving to DB.
LIVE_ENABLED: bool = False

# ── Tuning ────────────────────────────────────────────────────────────────────
LIVE_INTERVAL   = 4
LIVE_TTL_CACHE  = 30
UPC_TTL_CACHE   = 300
STREAM_KA_SEC   = 15

SSE_HEADERS = {
    "Content-Type":      "text/event-stream",
    "Cache-Control":     "no-cache",
    "X-Accel-Buffering": "no",
    "Connection":        "keep-alive",
}

ALL_SPORTS: list[str] = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "rugby", "handball", "volleyball", "cricket",
    "table-tennis", "esoccer", "mma", "boxing", "darts",
]


# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

def _ka(ts: str | None = None) -> str:
    return f": keep-alive {ts or _now()}\n\n"

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _redis():
    try:
        from flask import current_app
        return current_app.extensions.get("redis") or current_app.config.get("REDIS_CLIENT")
    except Exception:
        return None

def _cache_get(key: str):
    try:
        from app.workers.celery_tasks import cache_get
        return cache_get(key)
    except Exception:
        return None

def _cache_set(key: str, data: Any, ttl: int = 300) -> None:
    try:
        from app.workers.celery_tasks import cache_set
        cache_set(key, data, ttl=ttl)
    except Exception:
        pass

def _normalise_betradar(matches: list[dict], bk: str) -> list[dict]:
    out = []
    for m in (matches or []):
        if m.get("betradar_id") and str(m["betradar_id"]).strip() not in ("", "0", "None"):
            out.append(m); continue
        for field in ("betradarId", "sr_id", "sportradar_id", "betradar_event_id", "betradar"):
            val = m.get(field)
            if val and str(val).strip() not in ("", "0", "None", "null"):
                m = {**m, "betradar_id": str(val).strip()}
                break
        out.append(m)
    return out

def _live_disabled_response():
    return {
        "ok":      False,
        "error":   "Live mode is currently disabled. Set LIVE_ENABLED=True when upcoming is stable.",
        "status":  503,
    }, 503


# ─────────────────────────────────────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/health")
def health():
    t0 = time.perf_counter()
    redis_ok = False
    try:
        from app.workers.celery_tasks import _redis as _get_redis
        _get_redis().ping()
        redis_ok = True
    except Exception:
        pass

    worker_alive = False
    heartbeat_age = None
    heartbeat_at  = None
    try:
        hb = _cache_get("worker_heartbeat")
        if hb and hb.get("alive"):
            worker_alive = True
            heartbeat_at = hb.get("checked_at", "")
            if heartbeat_at:
                checked = datetime.fromisoformat(heartbeat_at.replace("Z", "+00:00"))
                heartbeat_age = int((datetime.now(timezone.utc) - checked).total_seconds())
    except Exception:
        pass

    return {
        "ok":              True,
        "api":             "combined",
        "redis":           redis_ok,
        "worker_alive":    worker_alive,
        "checked_at":      heartbeat_at,
        "heartbeat_age_s": heartbeat_age,
        "live_enabled":    LIVE_ENABLED,
        "latency_ms":      int((time.perf_counter() - t0) * 1000),
        "ts":              _now(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# OPS TRIGGER
# ─────────────────────────────────────────────────────────────────────────────

_ALLOWED_TRIGGER_TASKS: set[str] = {
    "persist_all_sports", "health_check",
    "update_match_results", "cache_finished_games", "expire_subscriptions",
}

@bp_combined.route("/ops/trigger/<task_name>", methods=["POST"])
def ops_trigger(task_name: str):
    if task_name not in _ALLOWED_TRIGGER_TASKS:
        return {"ok": False, "error": f"Task '{task_name}' not in allowed list",
                "allowed": sorted(_ALLOWED_TRIGGER_TASKS)}, 400
    full_name = f"tasks.ops.{task_name}"
    try:
        from app.workers.celery_tasks import celery
        result = celery.send_task(full_name, queue="default")
        return {"ok": True, "task": full_name, "task_id": result.id, "ts": _now()}
    except Exception as exc:
        logger.error("[ops_trigger] %s: %s", full_name, exc)
        return {"ok": False, "error": str(exc)}, 500


# ═════════════════════════════════════════════════════════════════════════════
# UPCOMING — BOOKMAKER FETCHERS
# ═════════════════════════════════════════════════════════════════════════════

def _fetch_sp_upcoming(sport_slug: str) -> tuple[str, list[dict], float]:
    t0 = time.perf_counter()
    try:
        cached = _cache_get(f"sp:upcoming:{sport_slug}")
        if cached and cached.get("matches"):
            return "sp", cached["matches"], time.perf_counter() - t0
        from app.workers.sp_harvester import fetch_upcoming
        matches = fetch_upcoming(sport_slug, fetch_full_markets=True, max_matches=200)
        return "sp", matches or [], time.perf_counter() - t0
    except Exception as exc:
        logger.warning("combined: SP upcoming %s: %s", sport_slug, exc)
        return "sp", [], time.perf_counter() - t0


def _fetch_bt_upcoming(sport_slug: str) -> tuple[str, list[dict], float]:
    t0 = time.perf_counter()
    try:
        rd = _redis()
        from app.workers.bt_harvester import get_cached_upcoming, fetch_upcoming_matches
        matches = get_cached_upcoming(rd, sport_slug) if rd else None
        if not matches:
            matches = fetch_upcoming_matches(sport_slug=sport_slug, max_pages=8, fetch_full=False)
        return "bt", _normalise_betradar(matches or [], "bt"), time.perf_counter() - t0
    except Exception as exc:
        logger.warning("combined: BT upcoming %s: %s", sport_slug, exc)
        return "bt", [], time.perf_counter() - t0


def _fetch_od_upcoming(sport_slug: str) -> tuple[str, list[dict], float]:
    t0 = time.perf_counter()
    try:
        rd = _redis()
        from app.workers.od_harvester import get_cached_upcoming, fetch_upcoming_matches
        matches = get_cached_upcoming(rd, sport_slug) if rd else None
        if not matches:
            matches = fetch_upcoming_matches(sport_slug=sport_slug)
        return "od", _normalise_betradar(matches or [], "od"), time.perf_counter() - t0
    except Exception as exc:
        logger.warning("combined: OD upcoming %s: %s", sport_slug, exc)
        return "od", [], time.perf_counter() - t0


def _fetch_all_upcoming(sport_slug: str) -> dict[str, tuple[list[dict], int]]:
    """
    Parallel-fetch upcoming from all 3 bookmakers.
    Returns {bk: (matches, latency_ms)}.
    """
    fetchers = {
        "sp": _fetch_sp_upcoming,
        "bt": _fetch_bt_upcoming,
        "od": _fetch_od_upcoming,
    }
    result: dict[str, tuple[list[dict], int]] = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(fn, sport_slug): bk for bk, fn in fetchers.items()}
        for fut in as_completed(futures):
            bk, matches, elapsed = fut.result()
            result[bk] = (matches, int(elapsed * 1000))
    return result


# ═════════════════════════════════════════════════════════════════════════════
# UPCOMING — MERGE + PERSIST HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _merge_and_cache_upcoming(
    bk_results: dict[str, tuple[list[dict], int]],
    sport_slug: str,
) -> list:
    """
    Merge all bookmaker results, cache in Redis, and fire async DB persist.
    Returns list of CombinedMatch objects.
    """
    from app.workers.combined_merger import MultiBookMerger

    merger = MultiBookMerger(min_arb_profit=0.05, min_ev_pct=2.5)
    sp_matches = bk_results.get("sp", ([], 0))[0]
    bt_matches = bk_results.get("bt", ([], 0))[0]
    od_matches = bk_results.get("od", ([], 0))[0]

    combined = merger.merge(sp_matches, bt_matches, od_matches, is_live=False)

    bk_counts = {bk: len(bk_results.get(bk, ([], 0))[0]) for bk in ["sp", "bt", "od"]}

    # Cache merged result in Redis
    _cache_set(
        f"combined:upcoming:{sport_slug}",
        {
            "matches":      [m.to_dict() for m in combined],
            "harvested_at": _now(),
            "bk_counts":    bk_counts,
            "total":        len(combined),
            "arb_count":    sum(1 for m in combined if m.has_arb),
            "ev_count":     sum(1 for m in combined if m.has_ev),
        },
        ttl=UPC_TTL_CACHE,
    )

    # Persist to PostgreSQL async (fire-and-forget via Celery)
    _persist_upcoming_async(combined, sport_slug)

    logger.info(
        "[upcoming] %s merged: sp=%d bt=%d od=%d → combined=%d arbs=%d",
        sport_slug,
        len(sp_matches), len(bt_matches), len(od_matches),
        len(combined),
        sum(1 for m in combined if m.has_arb),
    )
    return combined


def _persist_upcoming_async(combined: list, sport_slug: str) -> None:
    """
    Fire-and-forget: serialise combined matches and dispatch Celery task
    to persist to PostgreSQL.  Does not block the SSE stream.
    """
    if not combined:
        return
    try:
        serialized = [cm.to_dict() for cm in combined]
        from app.workers.celery_tasks import celery
        celery.send_task(
            "tasks.ops.persist_combined_batch",
            args=[serialized, sport_slug, "upcoming"],
            queue="results",
            countdown=2,
        )
        logger.debug("[upcoming] persist task dispatched for %s (%d matches)",
                     sport_slug, len(serialized))
    except Exception as exc:
        logger.warning("[upcoming] persist dispatch failed for %s: %s", sport_slug, exc)


def _opps_payload(combined_list: list) -> dict:
    from app.workers.combined_merger import compute_opportunities, detect_value_bets

    opp  = compute_opportunities(combined_list, min_ev=2.0)
    vbs  = detect_value_bets(combined_list, min_ev_pct=2.0)[:20]
    steam = [
        {
            "join_key":    m.join_key,
            "home_team":   m.home_team,
            "away_team":   m.away_team,
            "competition": m.competition,
            "market_slug": s.market_slug,
            "outcome":     s.outcome,
            "bk":          s.bk,
            "delta":       s.delta,
            "from_odd":    s.from_odd,
            "to_odd":      s.to_odd,
        }
        for m in combined_list
        for s in m.sharp
        if s.direction == "steam_down"
    ]
    return {
        "type":          "opportunities",
        "total_matches": opp.total_matches,
        "arb_count":     opp.arb_count,
        "ev_count":      opp.ev_count,
        "sharp_count":   opp.sharp_count,
        "total_arb_pct": opp.total_arb_pct,
        "best_arb": (
            {"market_slug": opp.best_arb.market_slug, "profit_pct": opp.best_arb.profit_pct,
             "legs": [{"outcome": l.outcome, "bk": l.bk, "odd": l.odd, "stake_pct": l.stake_pct}
                      for l in opp.best_arb.legs]}
            if opp.best_arb else None
        ),
        "best_ev": (
            {"market_slug": opp.best_ev.market_slug, "outcome": opp.best_ev.outcome,
             "bk": opp.best_ev.bk, "odd": opp.best_ev.odd, "ev_pct": opp.best_ev.ev_pct}
            if opp.best_ev else None
        ),
        "top_value_bets": vbs,
        "steam_moves":    steam[:15],
        "ts":             _now(),
    }


# ═════════════════════════════════════════════════════════════════════════════
# UPCOMING — SSE STREAM
# ═════════════════════════════════════════════════════════════════════════════

@bp_combined.route("/stream/upcoming/<sport_slug>")
def stream_upcoming(sport_slug: str):
    """
    SSE GET /api/combined/stream/upcoming/<sport_slug>

    Flow:
      1. Parallel fetch from SP + BT + OD
      2. Stream individual "match" events per bookmaker as they arrive
      3. Merge all three sets server-side
      4. Cache merged result in Redis
      5. Dispatch persist_combined_batch Celery task → PostgreSQL
      6. Stream merge_update events (one per combined match)
      7. Stream opportunities summary
      8. Stream done event
    """
    min_ev_pct  = float(request.args.get("min_ev_pct",  2.5))
    min_arb_pct = float(request.args.get("min_arb_pct", 0.05))

    @stream_with_context
    def generate():
        t0 = time.perf_counter()

        yield _sse({
            "type":       "start",
            "sport":      sport_slug,
            "mode":       "upcoming",
            "bookmakers": ["sp", "bt", "od"],
            "ts":         _now(),
        })

        # ── Phase 1: parallel fetch, stream raw matches per bk as done ────────
        bk_results: dict[str, tuple[list[dict], int]] = {}
        fetchers = {
            "sp": _fetch_sp_upcoming,
            "bt": _fetch_bt_upcoming,
            "od": _fetch_od_upcoming,
        }

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(fn, sport_slug): bk for bk, fn in fetchers.items()}
            for fut in as_completed(futures):
                bk, matches, elapsed = fut.result()
                latency_ms = int(elapsed * 1000)
                bk_results[bk] = (matches, latency_ms)

                yield _sse({
                    "type":       "bk_done",
                    "bk":         bk,
                    "count":      len(matches),
                    "latency_ms": latency_ms,
                })

                # Stream individual raw matches immediately so the frontend
                # can populate the table before the merge completes.
                for m in matches:
                    m_out = _normalise_betradar([m], bk)[0] if m else m
                    yield _sse({"type": "match", "bk": bk, "match": m_out})

        # ── Phase 2: merge + cache + persist ─────────────────────────────────
        try:
            combined = _merge_and_cache_upcoming(bk_results, sport_slug)
        except Exception as exc:
            logger.error("[stream_upcoming] merge failed for %s: %s", sport_slug, exc)
            yield _sse({"type": "error", "message": f"Merge failed: {exc}", "ts": _now()})
            return

        # ── Phase 3: stream merged rows ───────────────────────────────────────
        for cm in combined:
            yield _sse({"type": "merge_update", "combined_match": cm.to_dict()})

        # ── Phase 4: opportunities summary ────────────────────────────────────
        yield _sse(_opps_payload(combined))

        bk_counts = {bk: len(bk_results.get(bk, ([], 0))[0]) for bk in ["sp", "bt", "od"]}
        yield _sse({
            "type":         "done",
            "total":        len(combined),
            "bk_counts":    bk_counts,
            "arb_count":    sum(1 for m in combined if m.has_arb),
            "ev_count":     sum(1 for m in combined if m.has_ev),
            "latency_ms":   int((time.perf_counter() - t0) * 1000),
            "harvested_at": _now(),
            "persisting":   True,    # signals client that DB write was dispatched
        })

    return Response(generate(), headers=SSE_HEADERS)


# ═════════════════════════════════════════════════════════════════════════════
# UPCOMING — REST (read from Redis cache, fall back to DB or fresh fetch)
# ═════════════════════════════════════════════════════════════════════════════

@bp_combined.route("/upcoming/<sport_slug>")
def upcoming_rest(sport_slug: str):
    """
    GET /api/combined/upcoming/<sport_slug>

    Priority:
      1. Redis cache  (warm — set by stream/beat)
      2. PostgreSQL   (always has data after first stream)
      3. Fresh fetch  (cold start)

    Query params: page, per_page, sort, filter_arb, filter_ev, comp, team
    """
    t0          = time.perf_counter()
    page        = max(1,   int(request.args.get("page",     1)))
    per_page    = min(100, int(request.args.get("per_page", 30)))
    sort_by     = request.args.get("sort",       "start_time")
    filter_arb  = request.args.get("filter_arb", "") in ("1", "true")
    filter_ev   = request.args.get("filter_ev",  "") in ("1", "true")
    comp_filter = (request.args.get("comp", "") or "").lower()
    team_filter = (request.args.get("team", "") or "").lower()

    matches_raw: list[dict] = []
    source = "cache"

    # ── 1. Redis cache ────────────────────────────────────────────────────────
    cached = _cache_get(f"combined:upcoming:{sport_slug}")
    if cached and cached.get("matches"):
        matches_raw = cached["matches"]
        source = "cache"

    # ── 2. PostgreSQL fallback ────────────────────────────────────────────────
    if not matches_raw:
        try:
            from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds
            from app.models.bookmakers_model import Bookmaker
            from app.extensions import db
            from sqlalchemy import or_
            from datetime import timedelta

            now = datetime.now(timezone.utc)
            um_list = (UnifiedMatch.query
                       .filter(
                           or_(UnifiedMatch.start_time >= now - timedelta(hours=1),
                               UnifiedMatch.start_time.is_(None)),
                           or_(UnifiedMatch.status.in_(["PRE_MATCH", "upcoming", "PREMATCH"]),
                               UnifiedMatch.status.is_(None)),
                       )
                       .filter(UnifiedMatch.sport_name.ilike(f"%{sport_slug}%"))
                       .order_by(UnifiedMatch.start_time.asc())
                       .limit(500).all())

            if um_list:
                match_ids = [um.id for um in um_list]
                bmo_rows  = BookmakerMatchOdds.query.filter(
                    BookmakerMatchOdds.match_id.in_(match_ids)
                ).all()
                bk_objs   = {b.id: b for b in Bookmaker.query.all()}

                _BK_SLUG = {"sportpesa":"sp","betika":"bt","odibets":"od"}
                bmo_by_match: dict[int, list] = {}
                for bmo in bmo_rows:
                    bmo_by_match.setdefault(bmo.match_id, []).append(bmo)

                for um in um_list:
                    bmos = bmo_by_match.get(um.id, [])
                    markets_by_bk: dict[str, dict] = {}
                    bk_ids: dict[str, str] = {}
                    for bmo in bmos:
                        bk_obj  = bk_objs.get(bmo.bookmaker_id)
                        bk_name = (bk_obj.name if bk_obj else "").lower()
                        slug    = _BK_SLUG.get(bk_name, bk_name[:3])
                        markets_by_bk[slug] = bmo.markets_json or {}
                        bk_ids[slug]        = str(bmo.bookmaker_id)

                    # Build best map
                    best: dict[str, dict] = {}
                    for sl, mkts in markets_by_bk.items():
                        for mkt, outs in mkts.items():
                            best.setdefault(mkt, {})
                            for out, odd in (outs or {}).items():
                                try:
                                    fv = float(odd)
                                except Exception:
                                    continue
                                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["best_odd"]):
                                    best[mkt][out] = {"best_odd": fv, "best_bk": sl,
                                                      "spread_pct": 0, "all": [{"bk": sl, "odd": fv}]}

                    slugs = sorted({s for bk in markets_by_bk.values() for s in bk.keys()})
                    matches_raw.append({
                        "join_key":     f"br_{um.parent_match_id}" if um.parent_match_id else f"db_{um.id}",
                        "match_id":     um.id,
                        "betradar_id":  um.parent_match_id,
                        "home_team":    um.home_team_name  or "",
                        "away_team":    um.away_team_name  or "",
                        "competition":  um.competition_name or "",
                        "sport":        um.sport_name       or sport_slug,
                        "start_time":   um.start_time.isoformat() if um.start_time else None,
                        "status":       getattr(um, "status", None) or "PRE_MATCH",
                        "is_live":      False,
                        "bk_ids":       bk_ids,
                        "bk_count":     len(markets_by_bk),
                        "market_slugs": slugs,
                        "markets":      markets_by_bk,
                        "best":         best,
                        "arbs":         [],
                        "evs":          [],
                        "sharp":        [],
                        "has_arb":      False,
                        "has_ev":       False,
                        "has_sharp":    False,
                        "best_arb_pct": 0.0,
                        "best_ev_pct":  0.0,
                    })
                source = "postgresql"
        except Exception as exc:
            logger.warning("[upcoming_rest] DB fallback failed for %s: %s", sport_slug, exc)

    # ── 3. Cold-start fresh fetch (last resort) ───────────────────────────────
    if not matches_raw:
        try:
            bk_results = _fetch_all_upcoming(sport_slug)
            combined   = _merge_and_cache_upcoming(bk_results, sport_slug)
            matches_raw = [m.to_dict() for m in combined]
            source = "fresh_fetch"
        except Exception as exc:
            logger.error("[upcoming_rest] fresh fetch failed for %s: %s", sport_slug, exc)

    # ── Filters ───────────────────────────────────────────────────────────────
    matches = matches_raw
    if filter_arb:  matches = [m for m in matches if m.get("has_arb")]
    if filter_ev:   matches = [m for m in matches if m.get("has_ev")]
    if comp_filter: matches = [m for m in matches if comp_filter in (m.get("competition") or "").lower()]
    if team_filter: matches = [m for m in matches if
                               team_filter in (m.get("home_team") or "").lower() or
                               team_filter in (m.get("away_team") or "").lower()]

    # ── Sort ──────────────────────────────────────────────────────────────────
    if sort_by == "arb":         matches.sort(key=lambda m: -(m.get("best_arb_pct") or 0))
    elif sort_by == "ev":        matches.sort(key=lambda m: -(m.get("best_ev_pct") or 0))
    elif sort_by == "bk_count":  matches.sort(key=lambda m: -(m.get("bk_count") or 0))
    elif sort_by == "competition": matches.sort(key=lambda m: m.get("competition") or "")
    else:                        matches.sort(key=lambda m: m.get("start_time") or "")

    total = len(matches)
    paged = matches[(page - 1) * per_page: page * per_page]
    pages = max(1, (total + per_page - 1) // per_page)

    return {
        "ok":           True,
        "sport":        sport_slug,
        "mode":         "upcoming",
        "source":       source,
        "total":        total,
        "page":         page,
        "per_page":     per_page,
        "pages":        pages,
        "arb_count":    sum(1 for m in matches if m.get("has_arb")),
        "ev_count":     sum(1 for m in matches if m.get("has_ev")),
        "matches":      paged,
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
        "harvested_at": (cached or {}).get("harvested_at") if source == "cache" else _now(),
        "bk_counts":    (cached or {}).get("bk_counts", {}) if source == "cache" else {},
    }


# ═════════════════════════════════════════════════════════════════════════════
# LIVE — DISABLED UNTIL UPCOMING IS STABLE
# ═════════════════════════════════════════════════════════════════════════════

@bp_combined.route("/stream/live/<sport_slug>")
def stream_live(sport_slug: str):
    """
    SSE GET /api/combined/stream/live/<sport_slug>
    Disabled until LIVE_ENABLED = True.
    """
    if not LIVE_ENABLED:
        def _disabled():
            yield _sse({"type": "error",
                        "message": "Live mode is disabled. Upcoming must be stable first.",
                        "live_enabled": False, "ts": _now()})
        return Response(_disabled(), headers=SSE_HEADERS)

    # ── Live stream (only runs when LIVE_ENABLED = True) ─────────────────────
    interval    = max(2, int(request.args.get("interval", LIVE_INTERVAL)))
    min_ev_pct  = float(request.args.get("min_ev_pct",  2.0))
    min_arb_pct = float(request.args.get("min_arb_pct", 0.01))

    @stream_with_context
    def generate():
        import hashlib
        from app.workers.combined_merger import MultiBookMerger

        merger      = MultiBookMerger(min_arb_profit=min_arb_pct, min_ev_pct=min_ev_pct,
                                      sharp_min_delta=0.015)
        prev_hashes: dict[str, str] = {}

        def _match_hash(m_dict: dict) -> str:
            return hashlib.md5(
                json.dumps({k: m_dict[k] for k in ("markets","score_home","score_away","match_time")
                             if k in m_dict}, sort_keys=True, default=str).encode()
            ).hexdigest()[:12]

        def _poll_all():
            fetchers = {"sp": _fetch_sp_live, "bt": _fetch_bt_live, "od": _fetch_od_live}
            res: dict[str, list] = {"sp": [], "bt": [], "od": []}
            lats: dict[str, int] = {}
            with ThreadPoolExecutor(max_workers=3) as pool:
                futs = {pool.submit(fn, sport_slug): bk for bk, fn in fetchers.items()}
                for fut in as_completed(futs):
                    bk, matches, elapsed = fut.result()
                    res[bk]  = matches
                    lats[bk] = int(elapsed * 1000)
            return res["sp"], res["bt"], res["od"], lats

        yield _sse({"type": "connected", "sport": sport_slug, "mode": "live",
                    "bookmakers": ["sp","bt","od"], "interval": interval, "ts": _now()})

        sp, bt, od, lats = _poll_all()
        combined = merger.merge(sp, bt, od, is_live=True)
        for cm in combined:
            prev_hashes[cm.join_key] = _match_hash(cm.to_dict())

        yield _sse({"type": "snapshot", "matches": [m.to_dict() for m in combined],
                    "total": len(combined),
                    "bk_counts": {"sp":len(sp),"bt":len(bt),"od":len(od)},
                    "latencies": lats, "ts": _now()})
        yield _sse(_opps_payload(combined))

        last_ka = time.monotonic()
        while True:
            loop_start = time.monotonic()
            while time.monotonic() - loop_start < interval:
                time.sleep(0.3)
                if time.monotonic() - last_ka >= STREAM_KA_SEC:
                    yield _ka(); last_ka = time.monotonic()

            t0 = time.perf_counter()
            try:
                sp, bt, od, lats = _poll_all()
            except GeneratorExit:
                return
            except Exception as exc:
                logger.warning("combined live poll error %s: %s", sport_slug, exc)
                yield _sse({"type": "error", "message": str(exc), "ts": _now()})
                continue

            combined   = merger.merge(sp, bt, od, is_live=True)
            changed    = []
            new_hashes = {}
            for cm in combined:
                cm_d = cm.to_dict(); h = _match_hash(cm_d)
                new_hashes[cm.join_key] = h
                if prev_hashes.get(cm.join_key) != h: changed.append(cm_d)
            prev_hashes.update(new_hashes)

            if changed:
                yield _sse({"type": "live_update", "changed_count": len(changed),
                             "total": len(combined), "matches": changed,
                             "bk_counts": {"sp":len(sp),"bt":len(bt),"od":len(od)},
                             "latencies": lats,
                             "latency_ms": int((time.perf_counter()-t0)*1000), "ts": _now()})

            yield _sse(_opps_payload(combined))
            _cache_set(f"combined:live:{sport_slug}",
                       {"matches":[m.to_dict() for m in combined],"harvested_at":_now(),
                        "bk_counts":{"sp":len(sp),"bt":len(bt),"od":len(od)}},
                       ttl=LIVE_TTL_CACHE)

    return Response(generate(), headers=SSE_HEADERS)


@bp_combined.route("/live/<sport_slug>")
def live_rest(sport_slug: str):
    """GET /api/combined/live/<sport_slug> — disabled until LIVE_ENABLED = True."""
    if not LIVE_ENABLED:
        return _live_disabled_response()

    t0       = time.perf_counter()
    page     = max(1,   int(request.args.get("page",     1)))
    per_page = min(100, int(request.args.get("per_page", 30)))
    sort_by  = request.args.get("sort", "start_time")
    f_arb    = request.args.get("filter_arb",   "") in ("1","true")
    f_ev     = request.args.get("filter_ev",    "") in ("1","true")
    f_sharp  = request.args.get("filter_sharp", "") in ("1","true")
    comp_flt = (request.args.get("comp","") or "").lower()
    team_flt = (request.args.get("team","") or "").lower()

    cached = _cache_get(f"combined:live:{sport_slug}")
    if cached and cached.get("matches"):
        matches = cached["matches"]
    else:
        sp = (_cache_get(f"sp:live:{sport_slug}") or {}).get("matches", [])
        from app.workers.combined_merger import merge_live
        from app.workers.bt_harvester import slug_to_bt_sport_id, get_cached_live as bt_live
        from app.workers.od_harvester import slug_to_od_sport_id, get_cached_live as od_live
        rd = _redis()
        bt = bt_live(rd, slug_to_bt_sport_id(sport_slug)) if rd else []
        od = od_live(rd, slug_to_od_sport_id(sport_slug)) if rd else []
        matches = [m.to_dict() for m in merge_live(sp or [], bt or [], od or [])]

    if f_arb:    matches = [m for m in matches if m.get("has_arb")]
    if f_ev:     matches = [m for m in matches if m.get("has_ev")]
    if f_sharp:  matches = [m for m in matches if m.get("has_sharp")]
    if comp_flt: matches = [m for m in matches if comp_flt in (m.get("competition") or "").lower()]
    if team_flt: matches = [m for m in matches if
                            team_flt in (m.get("home_team") or "").lower() or
                            team_flt in (m.get("away_team") or "").lower()]

    if sort_by=="arb":        matches.sort(key=lambda m: -(m.get("best_arb_pct") or 0))
    elif sort_by=="ev":       matches.sort(key=lambda m: -(m.get("best_ev_pct") or 0))
    elif sort_by=="bk_count": matches.sort(key=lambda m: -(m.get("bk_count") or 0))
    else:                     matches.sort(key=lambda m: m.get("start_time") or "")

    total = len(matches)
    paged = matches[(page-1)*per_page: page*per_page]
    pages = max(1, (total+per_page-1)//per_page)

    return {
        "ok": True, "sport": sport_slug, "mode": "live",
        "total": total, "page": page, "per_page": per_page, "pages": pages,
        "arb_count":   sum(1 for m in matches if m.get("has_arb")),
        "ev_count":    sum(1 for m in matches if m.get("has_ev")),
        "sharp_count": sum(1 for m in matches if m.get("has_sharp")),
        "matches":     paged,
        "latency_ms":   int((time.perf_counter()-t0)*1000),
        "harvested_at": (cached or {}).get("harvested_at"),
        "bk_counts":    (cached or {}).get("bk_counts",{}),
    }


# ─────────────────────────────────────────────────────────────────────────────
# LIVE FETCHERS  (defined here so they're available when LIVE_ENABLED = True)
# ─────────────────────────────────────────────────────────────────────────────

_SP_SLUG_TO_ID: dict[str, int] = {
    "soccer":1,"football":1,"esoccer":1,"efootball":1,
    "basketball":2,"baseball":3,"ice-hockey":4,"tennis":5,
    "handball":6,"rugby":8,"cricket":9,"volleyball":10,
    "table-tennis":13,"boxing":10,"mma":117,"darts":49,"american-football":15,
}

def _sp_live_from_harvester(sport_slug: str) -> list[dict]:
    cached = _cache_get(f"sp:live:{sport_slug}")
    if cached and cached.get("matches"): return cached["matches"]
    sport_id = _SP_SLUG_TO_ID.get(sport_slug.lower(), 1)
    rd = _redis()
    if rd:
        try:
            raw = rd.get(f"sp:live:snapshot:{sport_id}")
            if raw:
                snap=json.loads(raw); events=snap.get("events") or []
                if events:
                    matches=[]
                    for ev in events:
                        comps=ev.get("competitors") or []
                        home=comps[0].get("name","") if len(comps)>0 else ""
                        away=comps[1].get("name","") if len(comps)>1 else ""
                        state=ev.get("state") or {}; score=state.get("matchScore") or {}
                        matches.append({"sp_game_id":ev.get("id"),"home_team":home,"away_team":away,
                            "competition":(ev.get("competition") or {}).get("name",""),
                            "start_time":ev.get("startTime") or ev.get("kickOffTime"),
                            "sport":sport_slug,"is_live":True,"match_time":state.get("matchTime",""),
                            "score_home":str(score.get("home","")),"score_away":str(score.get("away","")),
                            "markets":{},"market_count":0,"source":"sp_live","betradar_id":ev.get("betradarId")})
                    if matches: return matches
        except Exception as exc:
            logger.debug("combined: SP live Redis: %s", exc)
    try:
        from app.workers.sp_live_harvester import fetch_live_events
        events=fetch_live_events(sport_id,limit=200); matches=[]
        for ev in events:
            comps=ev.get("competitors") or []
            home=comps[0].get("name","") if len(comps)>0 else ""
            away=comps[1].get("name","") if len(comps)>1 else ""
            state=ev.get("state") or {}; score=state.get("matchScore") or {}
            matches.append({"sp_game_id":ev.get("id"),"home_team":home,"away_team":away,
                "competition":(ev.get("competition") or {}).get("name",""),
                "start_time":ev.get("startTime") or ev.get("kickOffTime"),
                "sport":sport_slug,"is_live":True,"match_time":state.get("matchTime",""),
                "score_home":str(score.get("home","")),"score_away":str(score.get("away","")),
                "markets":{},"market_count":0,"source":"sp_live_http","betradar_id":ev.get("betradarId")})
        return matches
    except Exception as exc:
        logger.warning("combined: SP live HTTP %s: %s", sport_slug, exc)
        return []

def _fetch_sp_live(sport_slug:str)->tuple[str,list[dict],float]:
    t0=time.perf_counter()
    try: return "sp",_sp_live_from_harvester(sport_slug),time.perf_counter()-t0
    except Exception as exc: logger.warning("combined: SP live %s: %s",sport_slug,exc); return "sp",[],time.perf_counter()-t0

def _fetch_bt_live(sport_slug:str)->tuple[str,list[dict],float]:
    t0=time.perf_counter()
    try:
        from app.workers.bt_harvester import slug_to_bt_sport_id,fetch_live_matches,get_cached_live
        rd=_redis(); sid=slug_to_bt_sport_id(sport_slug)
        matches=get_cached_live(rd,sid) if rd else None
        if not matches: matches=fetch_live_matches(sid)
        return "bt",_normalise_betradar(matches or [],"bt"),time.perf_counter()-t0
    except Exception as exc: logger.warning("combined: BT live %s: %s",sport_slug,exc); return "bt",[],time.perf_counter()-t0

def _fetch_od_live(sport_slug:str)->tuple[str,list[dict],float]:
    t0=time.perf_counter()
    try:
        from app.workers.od_harvester import slug_to_od_sport_id,fetch_live_matches,get_cached_live
        rd=_redis(); sid=slug_to_od_sport_id(sport_slug)
        matches=get_cached_live(rd,sid) if rd else None
        if not matches: matches=fetch_live_matches(sport_slug=sport_slug)
        return "od",_normalise_betradar(matches or [],"od"),time.perf_counter()-t0
    except Exception as exc: logger.warning("combined: OD live %s: %s",sport_slug,exc); return "od",[],time.perf_counter()-t0


# ═════════════════════════════════════════════════════════════════════════════
# OPPORTUNITIES  (upcoming only while LIVE is disabled)
# ═════════════════════════════════════════════════════════════════════════════

@bp_combined.route("/opportunities/<sport_slug>")
def opportunities(sport_slug: str):
    t0     = time.perf_counter()
    mode   = request.args.get("mode", "upcoming")
    limit  = min(int(request.args.get("limit", 20)), 100)
    min_ev = float(request.args.get("min_ev", 2.0))

    # When live is disabled, force upcoming-only
    if not LIVE_ENABLED and mode == "all":
        mode = "upcoming"

    all_matches: list[dict] = []
    if mode in ("all", "upcoming"):
        cached = _cache_get(f"combined:upcoming:{sport_slug}")
        if cached: all_matches.extend(cached.get("matches", []))
    if mode in ("all", "live") and LIVE_ENABLED:
        cached = _cache_get(f"combined:live:{sport_slug}")
        if cached: all_matches.extend(cached.get("matches", []))

    arbs: list[dict] = []; evs: list[dict] = []; steam: list[dict] = []
    for m in all_matches:
        base = {k: m[k] for k in ("join_key","home_team","away_team","competition","start_time","is_live") if k in m}
        for a in (m.get("arbs") or []):  arbs.append({**a,**base})
        for e in (m.get("evs")  or []):
            if e.get("ev_pct",0) >= min_ev: evs.append({**e,**base})
        for s in (m.get("sharp") or []):
            if s.get("direction")=="steam_down": steam.append({**s,**base})

    arbs.sort( key=lambda x: -(x.get("profit_pct") or 0))
    evs.sort(  key=lambda x: -(x.get("ev_pct")     or 0))
    steam.sort(key=lambda x: -(x.get("delta")       or 0))

    return {
        "ok": True, "sport": sport_slug, "mode": mode,
        "live_enabled":   LIVE_ENABLED,
        "total_matches":  len(all_matches),
        "arbs":           arbs[:limit], "evs": evs[:limit], "steam": steam[:limit],
        "arb_count":      len(arbs), "ev_count": len(evs), "sharp_count": len(steam),
        "latency_ms":     int((time.perf_counter()-t0)*1000),
        "ts":             _now(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# COMPARE MATRIX
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/compare/<sport_slug>")
def compare_matrix(sport_slug: str):
    t0          = time.perf_counter()
    market_slug = request.args.get("market", "1x2")
    mode        = request.args.get("mode", "upcoming")
    limit       = min(int(request.args.get("limit", 100)), 500)

    if not LIVE_ENABLED and mode == "live":
        return _live_disabled_response()

    cached = _cache_get(f"combined:{mode}:{sport_slug}")
    if not cached or not cached.get("matches"):
        return {"ok": False, "error": f"No cached {mode} data for {sport_slug}. "
                f"Connect to /stream/{mode}/{sport_slug} first."}, 404

    rows: list[dict] = []
    for m in cached["matches"][:limit]:
        best_map = m.get("best", {}).get(market_slug) or next(
            (v for k, v in m.get("best", {}).items() if k.startswith(market_slug)), None)
        if not best_map: continue
        outcomes = sorted(best_map.keys())
        row: dict = {"join_key":m["join_key"],"home_team":m["home_team"],"away_team":m["away_team"],
                     "competition":m["competition"],"start_time":m.get("start_time"),
                     "is_live":m.get("is_live"),"has_arb":m.get("has_arb"),"best_arb_pct":m.get("best_arb_pct")}
        for bk in ["sp","bt","od"]:
            for out in outcomes:
                odd=(m.get("markets",{}).get(bk) or {}).get(market_slug,{}).get(out)
                row[f"{bk}_{out}"]=round(odd,3) if odd else None
        for out in outcomes:
            b=best_map.get(out)
            if b: row[f"best_{out}"]=b.get("best_odd"); row[f"best_bk_{out}"]=b.get("best_bk")
        rows.append(row)

    return {"ok":True,"sport":sport_slug,"market":market_slug,"mode":mode,
            "total":len(rows),"rows":rows,"arb_rows":[r for r in rows if r.get("has_arb")],
            "latency_ms":int((time.perf_counter()-t0)*1000),"ts":_now()}


# ─────────────────────────────────────────────────────────────────────────────
# STATUS
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/status")
def status():
    t0      = time.perf_counter()
    summary = []

    for sport in ALL_SPORTS:
        u = _cache_get(f"combined:upcoming:{sport}") or {}
        # Only show live if enabled
        l = (_cache_get(f"combined:live:{sport}") or {}) if LIVE_ENABLED else {}
        summary.append({
            "sport":           sport,
            "upcoming_count":  len(u.get("matches", [])),
            "live_count":      len(l.get("matches", [])),
            "upcoming_arbs":   sum(1 for m in u.get("matches", []) if m.get("has_arb")),
            "live_arbs":       sum(1 for m in l.get("matches", []) if m.get("has_arb")),
            "upcoming_evs":    sum(1 for m in u.get("matches", []) if m.get("has_ev")),
            "live_evs":        sum(1 for m in l.get("matches", []) if m.get("has_ev")),
            "harvested_at_up": u.get("harvested_at"),
            "harvested_at_lv": l.get("harvested_at") if LIVE_ENABLED else None,
            "cache_populated": bool(u.get("matches")),
            "bk_counts":       u.get("bk_counts", {}),
        })

    populated = sum(1 for s in summary if s["cache_populated"])

    return {
        "ok":               True,
        "source":           "combined:sp+bt+od",
        "live_enabled":     LIVE_ENABLED,
        "total_sports":     len(ALL_SPORTS),
        "populated_sports": populated,
        "sports":           summary,
        "latency_ms":       int((time.perf_counter() - t0) * 1000),
        "ts":               _now(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# DEBUG
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/debug/fields/<sport_slug>")
def debug_fields(sport_slug: str):
    fetchers = {"sp": _fetch_sp_upcoming, "bt": _fetch_bt_upcoming, "od": _fetch_od_upcoming}
    results  = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(fn, sport_slug): bk for bk, fn in fetchers.items()}
        for fut in as_completed(futures):
            bk, matches, elapsed = fut.result()
            sample = matches[:3] if matches else []
            results[bk] = {
                "total": len(matches), "latency_ms": int(elapsed * 1000),
                "sample": [{
                    "fields": list(m.keys()),
                    "betradar_fields": {k: m.get(k) for k in ("betradar_id","betradarId","sr_id","sportradar_id","betradar_event_id") if m.get(k)},
                    "join_key_would_be": (f"br_{m.get('betradar_id') or m.get('betradarId')}"
                        if (m.get("betradar_id") or m.get("betradarId"))
                        else f"bt_p_{m.get('bt_parent_id')}" if bk=="bt" and m.get("bt_parent_id")
                        else f"od_p_{m.get('od_parent_id')}" if bk=="od" and m.get("od_parent_id")
                        else f"fuzzy_{m.get('home_team','?')}_vs_{m.get('away_team','?')}"),
                    "home_team": m.get("home_team"),
                    "away_team": m.get("away_team"),
                    "start_time": m.get("start_time"),
                } for m in sample],
            }
    return json.dumps(results, indent=2, default=str), 200, {"Content-Type": "application/json"}