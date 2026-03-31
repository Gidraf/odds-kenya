"""
app/views/combined_module.py
=============================
Multi-bookmaker combined data endpoints — SP + BT + OD.

All endpoints emit JSON-normalised, canonical-slug data using
app/workers/combined_merger.py for the merge + opportunity engine.

Endpoint map
────────────
  UPCOMING
    SSE  GET /api/combined/stream/upcoming/<sport>
         — streams SP+BT+OD match-by-match as they arrive, then emits
           merge_complete with all arb/EV/sharp analysis
    GET  /api/combined/upcoming/<sport>
         — REST: returns the last cached merged upcoming snapshot

  LIVE
    SSE  GET /api/combined/stream/live/<sport>
         — long-lived SSE: polls all three bookmakers every LIVE_INTERVAL s,
           merges, diffs, and streams only changed matches + global stats
    GET  /api/combined/live/<sport>
         — REST: returns last cached merged live snapshot

  OPPORTUNITIES
    GET  /api/combined/opportunities/<sport>
         — top arb / EV+ / steam moves across upcoming + live

  COMPARE
    GET  /api/combined/compare/<sport>?market=1x2
         — head-to-head odds matrix for all three books (tabular)

SSE event shapes
────────────────
  {type:"start",        sport, mode, bookmakers, ts}
  {type:"bk_start",     bk, sport, estimated}
  {type:"match",        bk, match}          ← individual match as it arrives
  {type:"bk_done",      bk, count, latency_ms}
  {type:"merge_update", combined_match}     ← merged row emitted after each bk batch
  {type:"opportunities", arbs, evs, sharp, ts}
  {type:"done",          total, bk_counts, latency_ms, ts}
  {type:"error",         message}
  {type:"heartbeat",     ts}

  Live only:
  {type:"live_update",  changed_keys: [...], matches: [...]}  ← delta push
"""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED
from datetime import datetime, timezone
from typing import Any

from flask import Blueprint, Response, request, stream_with_context

logger = logging.getLogger(__name__)

bp_combined = Blueprint("combined", __name__, url_prefix="/api/combined")

# ── Tuning constants ─────────────────────────────────────────────────────────
LIVE_INTERVAL   = 4      # seconds between full live polls
LIVE_TTL_CACHE  = 30     # seconds to cache live snapshot in Redis
UPC_TTL_CACHE   = 300    # seconds to cache upcoming snapshot in Redis
STREAM_KA_SEC   = 15     # seconds between SSE keep-alives

SSE_HEADERS = {
    "Content-Type":      "text/event-stream",
    "Cache-Control":     "no-cache",
    "X-Accel-Buffering": "no",
    "Connection":        "keep-alive",
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
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


# ─────────────────────────────────────────────────────────────────────────────
# BOOKMAKER FETCHERS  (wrapped with error isolation)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_sp_upcoming(sport_slug: str) -> tuple[str, list[dict], float]:
    t0 = time.perf_counter()
    try:
        cached = _cache_get(f"sp:upcoming:{sport_slug}")
        if cached and cached.get("matches"):
            return "sp", cached["matches"], time.perf_counter() - t0
        from app.workers.sp_harvester import fetch_upcoming
        matches = fetch_upcoming(sport_slug, fetch_full_markets=True, max_matches=200)
        return "sp", matches, time.perf_counter() - t0
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
        return "bt", matches or [], time.perf_counter() - t0
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
        return "od", matches or [], time.perf_counter() - t0
    except Exception as exc:
        logger.warning("combined: OD upcoming %s: %s", sport_slug, exc)
        return "od", [], time.perf_counter() - t0


def _fetch_sp_live(sport_slug: str) -> tuple[str, list[dict], float]:
    t0 = time.perf_counter()
    try:
        cached = _cache_get(f"sp:live:{sport_slug}")
        if cached and cached.get("matches"):
            return "sp", cached["matches"], time.perf_counter() - t0
        from app.workers.sp_harvester import fetch_live
        matches = fetch_live(sport_slug, fetch_full_markets=True)
        return "sp", matches, time.perf_counter() - t0
    except Exception as exc:
        logger.warning("combined: SP live %s: %s", sport_slug, exc)
        return "sp", [], time.perf_counter() - t0


def _fetch_bt_live(sport_slug: str) -> tuple[str, list[dict], float]:
    t0 = time.perf_counter()
    try:
        from app.workers.bt_harvester import (
            slug_to_bt_sport_id, fetch_live_matches, get_cached_live,
        )
        rd  = _redis()
        sid = slug_to_bt_sport_id(sport_slug)
        matches = get_cached_live(rd, sid) if rd else None
        if not matches:
            matches = fetch_live_matches(sid)
        return "bt", matches or [], time.perf_counter() - t0
    except Exception as exc:
        logger.warning("combined: BT live %s: %s", sport_slug, exc)
        return "bt", [], time.perf_counter() - t0


def _fetch_od_live(sport_slug: str) -> tuple[str, list[dict], float]:
    t0 = time.perf_counter()
    try:
        from app.workers.od_harvester import (
            slug_to_od_sport_id, fetch_live_matches, get_cached_live,
        )
        rd  = _redis()
        sid = slug_to_od_sport_id(sport_slug)
        matches = get_cached_live(rd, sid) if rd else None
        if not matches:
            matches = fetch_live_matches(sport_slug=sport_slug)
        return "od", matches or [], time.perf_counter() - t0
    except Exception as exc:
        logger.warning("combined: OD live %s: %s", sport_slug, exc)
        return "od", [], time.perf_counter() - t0


# ─────────────────────────────────────────────────────────────────────────────
# OPPORTUNITY SERIALISER
# ─────────────────────────────────────────────────────────────────────────────

def _opps_payload(combined_list: list) -> dict:
    """Build the opportunities SSE payload from a list of CombinedMatch."""
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
            {
                "market_slug": opp.best_arb.market_slug,
                "profit_pct":  opp.best_arb.profit_pct,
                "legs": [
                    {"outcome": l.outcome, "bk": l.bk,
                     "odd": l.odd, "stake_pct": l.stake_pct}
                    for l in opp.best_arb.legs
                ],
            }
            if opp.best_arb else None
        ),
        "best_ev": (
            {
                "market_slug": opp.best_ev.market_slug,
                "outcome":     opp.best_ev.outcome,
                "bk":          opp.best_ev.bk,
                "odd":         opp.best_ev.odd,
                "ev_pct":      opp.best_ev.ev_pct,
            }
            if opp.best_ev else None
        ),
        "top_value_bets": vbs,
        "steam_moves":    steam[:15],
        "ts":             _now(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING — SSE STREAM
# ══════════════════════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/stream/upcoming/<sport_slug>")
def stream_upcoming(sport_slug: str):
    """
    SSE GET /api/combined/stream/upcoming/<sport_slug>

    Fetches SP + BT + OD upcoming concurrently (thread pool), streams
    individual match events as each bookmaker batch completes, then emits
    a merged snapshot with full arb/EV/sharp analysis.

    Query params:
      min_ev_pct   default 2.5
      min_arb_pct  default 0.05
    """
    min_ev_pct  = float(request.args.get("min_ev_pct",  2.5))
    min_arb_pct = float(request.args.get("min_arb_pct", 0.05))

    @stream_with_context
    def generate():
        from app.workers.combined_merger import MultiBookMerger

        merger = MultiBookMerger(
            min_arb_profit=min_arb_pct,
            min_ev_pct=min_ev_pct,
        )

        t0 = time.perf_counter()
        bk_matches: dict[str, list[dict]] = {"sp": [], "bt": [], "od": []}
        bk_latencies: dict[str, int] = {}

        yield _sse({
            "type":        "start",
            "sport":       sport_slug,
            "mode":        "upcoming",
            "bookmakers":  ["sp", "bt", "od"],
            "ts":          _now(),
        })

        # Parallel fetch — stream results as each completes
        fetchers = {
            "sp": _fetch_sp_upcoming,
            "bt": _fetch_bt_upcoming,
            "od": _fetch_od_upcoming,
        }

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(fn, sport_slug): bk for bk, fn in fetchers.items()}
            for fut in as_completed(futures):
                bk, matches, elapsed = fut.result()
                bk_matches[bk] = matches
                bk_latencies[bk] = int(elapsed * 1000)

                yield _sse({
                    "type":       "bk_done",
                    "bk":         bk,
                    "count":      len(matches),
                    "latency_ms": bk_latencies[bk],
                })

                # Stream individual matches from this bk
                for m in matches[:50]:    # cap streaming matches per bk
                    yield _sse({
                        "type":  "match",
                        "bk":    bk,
                        "match": m,
                    })

        # Merge all three sets
        combined = merger.merge(
            bk_matches["sp"],
            bk_matches["bt"],
            bk_matches["od"],
            is_live=False,
        )

        # Cache the merged result
        _cache_set(
            f"combined:upcoming:{sport_slug}",
            {
                "matches":      [m.to_dict() for m in combined],
                "harvested_at": _now(),
                "bk_counts":    {bk: len(bk_matches[bk]) for bk in ["sp", "bt", "od"]},
            },
            ttl=UPC_TTL_CACHE,
        )

        # Stream merged matches with arb/EV flags
        for cm in combined:
            if cm.bk_count >= 2 or cm.has_arb or cm.has_ev:
                yield _sse({
                    "type":           "merge_update",
                    "combined_match": cm.to_dict(),
                })

        # Opportunities summary
        yield _sse(_opps_payload(combined))

        total_latency = int((time.perf_counter() - t0) * 1000)
        yield _sse({
            "type":        "done",
            "total":       len(combined),
            "bk_counts":   {bk: len(bk_matches[bk]) for bk in ["sp", "bt", "od"]},
            "latency_ms":  total_latency,
            "harvested_at": _now(),
        })

    return Response(generate(), headers=SSE_HEADERS)


# ─────────────────────────────────────────────────────────────────────────────
# UPCOMING — REST
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/upcoming/<sport_slug>")
def upcoming_rest(sport_slug: str):
    """
    GET /api/combined/upcoming/<sport_slug>
    Returns last cached merged upcoming snapshot.
    Falls back to direct fetch if cache is cold.

    Query params: page, per_page, sort (start_time|arb|ev|bk_count|competition),
                  filter_arb=1, filter_ev=1, comp=<text>, team=<text>
    """
    t0       = time.perf_counter()
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 30)), 100)
    sort_by  = request.args.get("sort", "start_time")
    filter_arb = request.args.get("filter_arb", "") in ("1", "true")
    filter_ev  = request.args.get("filter_ev",  "") in ("1", "true")
    comp_filter = request.args.get("comp", "").lower()
    team_filter = request.args.get("team", "").lower()

    cached = _cache_get(f"combined:upcoming:{sport_slug}")
    if cached and cached.get("matches"):
        matches_raw = cached["matches"]
    else:
        # Cold cache — do a quick synchronous merge from per-bk caches
        sp = (_cache_get(f"sp:upcoming:{sport_slug}") or {}).get("matches", [])
        bt_data = _cache_get(f"bt:upcoming:{sport_slug}")
        bt = bt_data.get("matches", []) if bt_data else []
        od_data = _cache_get(f"od:upcoming:{sport_slug}")
        od = od_data.get("matches", []) if od_data else []

        from app.workers.combined_merger import merge_upcoming
        combined = merge_upcoming(sp, bt, od)
        matches_raw = [m.to_dict() for m in combined]

    # Apply filters
    matches = matches_raw
    if filter_arb:
        matches = [m for m in matches if m.get("has_arb")]
    if filter_ev:
        matches = [m for m in matches if m.get("has_ev")]
    if comp_filter:
        matches = [m for m in matches if comp_filter in (m.get("competition") or "").lower()]
    if team_filter:
        matches = [m for m in matches if
                   team_filter in (m.get("home_team") or "").lower() or
                   team_filter in (m.get("away_team") or "").lower()]

    # Sort
    if sort_by == "arb":
        matches.sort(key=lambda m: -(m.get("best_arb_pct") or 0))
    elif sort_by == "ev":
        matches.sort(key=lambda m: -(m.get("best_ev_pct") or 0))
    elif sort_by == "bk_count":
        matches.sort(key=lambda m: -(m.get("bk_count") or 0))
    elif sort_by == "competition":
        matches.sort(key=lambda m: m.get("competition") or "")
    else:
        matches.sort(key=lambda m: m.get("start_time") or "")

    total  = len(matches)
    paged  = matches[(page - 1) * per_page: page * per_page]
    pages  = max(1, (total + per_page - 1) // per_page)

    # Summary stats
    arb_count = sum(1 for m in matches if m.get("has_arb"))
    ev_count  = sum(1 for m in matches if m.get("has_ev"))

    return {
        "ok":           True,
        "sport":        sport_slug,
        "mode":         "upcoming",
        "total":        total,
        "page":         page,
        "per_page":     per_page,
        "pages":        pages,
        "arb_count":    arb_count,
        "ev_count":     ev_count,
        "matches":      paged,
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
        "harvested_at": (cached or {}).get("harvested_at"),
        "bk_counts":    (cached or {}).get("bk_counts", {}),
    }


# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
# LIVE — SSE STREAM  (long-lived, delta push, Odispedia-style grouping)
# ══════════════════════════════════════════════════════════════════════════════
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/stream/live/<sport_slug>")
def stream_live(sport_slug: str):
    """
    SSE GET /api/combined/stream/live/<sport_slug>

    Long-lived SSE that:
      1. Sends an initial snapshot of all merged live matches
      2. Polls SP + BT + OD every LIVE_INTERVAL seconds in parallel
      3. Diffs against previous state and emits only changed matches
      4. Emits opportunities summary after every refresh cycle

    Matches are grouped by join_key (betradar_id → parent_id → fuzzy).
    Each live_update event contains only the delta (changed join_keys).

    Query params:
      interval     default 4 (seconds between polls, min 2)
      min_ev_pct   default 2.0
      min_arb_pct  default 0.01
    """
    interval    = max(2, int(request.args.get("interval", LIVE_INTERVAL)))
    min_ev_pct  = float(request.args.get("min_ev_pct",  2.0))
    min_arb_pct = float(request.args.get("min_arb_pct", 0.01))

    @stream_with_context
    def generate():
        from app.workers.combined_merger import MultiBookMerger

        merger = MultiBookMerger(
            min_arb_profit=min_arb_pct,
            min_ev_pct=min_ev_pct,
            sharp_min_delta=0.015,
        )

        # State for delta detection
        prev_hashes: dict[str, str] = {}   # join_key → hash of to_dict()

        def _match_hash(m_dict: dict) -> str:
            import hashlib, json
            return hashlib.md5(
                json.dumps(
                    {k: m_dict[k] for k in ("markets", "score_home", "score_away", "match_time")
                     if k in m_dict},
                    sort_keys=True, default=str,
                ).encode()
            ).hexdigest()[:12]

        def _poll_all() -> tuple[list[dict], list[dict], list[dict], dict]:
            t0 = time.perf_counter()
            fetchers = {
                "sp": _fetch_sp_live,
                "bt": _fetch_bt_live,
                "od": _fetch_od_live,
            }
            results = {"sp": [], "bt": [], "od": []}
            latencies: dict[str, int] = {}
            with ThreadPoolExecutor(max_workers=3) as pool:
                futs = {pool.submit(fn, sport_slug): bk for bk, fn in fetchers.items()}
                for fut in as_completed(futs):
                    bk, matches, elapsed = fut.result()
                    results[bk] = matches
                    latencies[bk] = int(elapsed * 1000)
            return results["sp"], results["bt"], results["od"], latencies

        # ── Initial snapshot ─────────────────────────────────────────────────
        yield _sse({
            "type":       "connected",
            "sport":      sport_slug,
            "mode":       "live",
            "bookmakers": ["sp", "bt", "od"],
            "interval":   interval,
            "ts":         _now(),
        })

        sp, bt, od, lats = _poll_all()
        combined = merger.merge(sp, bt, od, is_live=True)

        # Seed hashes
        for cm in combined:
            cm_d = cm.to_dict()
            prev_hashes[cm.join_key] = _match_hash(cm_d)

        yield _sse({
            "type":    "snapshot",
            "matches": [m.to_dict() for m in combined],
            "total":   len(combined),
            "bk_counts": {
                "sp": len(sp), "bt": len(bt), "od": len(od),
            },
            "latencies": lats,
            "ts":        _now(),
        })
        yield _sse(_opps_payload(combined))

        # ── Delta loop ───────────────────────────────────────────────────────
        last_ka = time.monotonic()
        while True:
            loop_start = time.monotonic()

            # Sleep for the polling interval (check for client disconnect)
            while time.monotonic() - loop_start < interval:
                time.sleep(0.3)
                if time.monotonic() - last_ka >= STREAM_KA_SEC:
                    yield _ka()
                    last_ka = time.monotonic()

            t0 = time.perf_counter()
            try:
                sp, bt, od, lats = _poll_all()
            except GeneratorExit:
                return
            except Exception as exc:
                logger.warning("combined live poll error %s: %s", sport_slug, exc)
                yield _sse({"type": "error", "message": str(exc), "ts": _now()})
                continue

            combined = merger.merge(sp, bt, od, is_live=True)

            # Detect changed matches
            changed: list[dict] = []
            new_hashes: dict[str, str] = {}
            for cm in combined:
                cm_d = cm.to_dict()
                h    = _match_hash(cm_d)
                new_hashes[cm.join_key] = h
                if prev_hashes.get(cm.join_key) != h:
                    changed.append(cm_d)

            prev_hashes.update(new_hashes)

            if changed:
                yield _sse({
                    "type":         "live_update",
                    "changed_count": len(changed),
                    "total":        len(combined),
                    "matches":      changed,
                    "bk_counts":    {"sp": len(sp), "bt": len(bt), "od": len(od)},
                    "latencies":    lats,
                    "latency_ms":   int((time.perf_counter() - t0) * 1000),
                    "ts":           _now(),
                })

            # Always emit opportunities summary each cycle
            yield _sse(_opps_payload(combined))

            # Cache current snapshot
            _cache_set(
                f"combined:live:{sport_slug}",
                {
                    "matches":      [m.to_dict() for m in combined],
                    "harvested_at": _now(),
                    "bk_counts":    {"sp": len(sp), "bt": len(bt), "od": len(od)},
                },
                ttl=LIVE_TTL_CACHE,
            )

    return Response(generate(), headers=SSE_HEADERS)


# ─────────────────────────────────────────────────────────────────────────────
# LIVE — REST
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/live/<sport_slug>")
def live_rest(sport_slug: str):
    """
    GET /api/combined/live/<sport_slug>
    Returns last cached merged live snapshot, or fetches fresh if cold.

    Query params: filter_arb, filter_ev, filter_sharp, team, comp,
                  sort (start_time|arb|ev|bk_count), page, per_page
    """
    t0        = time.perf_counter()
    page      = max(1, int(request.args.get("page", 1)))
    per_page  = min(int(request.args.get("per_page", 30)), 100)
    sort_by   = request.args.get("sort", "start_time")
    f_arb     = request.args.get("filter_arb",   "") in ("1", "true")
    f_ev      = request.args.get("filter_ev",    "") in ("1", "true")
    f_sharp   = request.args.get("filter_sharp", "") in ("1", "true")
    comp_flt  = request.args.get("comp", "").lower()
    team_flt  = request.args.get("team", "").lower()

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
        combined = merge_live(sp or [], bt or [], od or [])
        matches = [m.to_dict() for m in combined]

    if f_arb:
        matches = [m for m in matches if m.get("has_arb")]
    if f_ev:
        matches = [m for m in matches if m.get("has_ev")]
    if f_sharp:
        matches = [m for m in matches if m.get("has_sharp")]
    if comp_flt:
        matches = [m for m in matches if comp_flt in (m.get("competition") or "").lower()]
    if team_flt:
        matches = [m for m in matches if
                   team_flt in (m.get("home_team") or "").lower() or
                   team_flt in (m.get("away_team") or "").lower()]

    if sort_by == "arb":
        matches.sort(key=lambda m: -(m.get("best_arb_pct") or 0))
    elif sort_by == "ev":
        matches.sort(key=lambda m: -(m.get("best_ev_pct") or 0))
    elif sort_by == "bk_count":
        matches.sort(key=lambda m: -(m.get("bk_count") or 0))
    else:
        matches.sort(key=lambda m: m.get("start_time") or "")

    total = len(matches)
    paged = matches[(page - 1) * per_page: page * per_page]
    pages = max(1, (total + per_page - 1) // per_page)

    return {
        "ok":           True,
        "sport":        sport_slug,
        "mode":         "live",
        "total":        total,
        "page":         page,
        "per_page":     per_page,
        "pages":        pages,
        "arb_count":    sum(1 for m in matches if m.get("has_arb")),
        "ev_count":     sum(1 for m in matches if m.get("has_ev")),
        "sharp_count":  sum(1 for m in matches if m.get("has_sharp")),
        "matches":      paged,
        "latency_ms":   int((time.perf_counter() - t0) * 1000),
        "harvested_at": (cached or {}).get("harvested_at"),
        "bk_counts":    (cached or {}).get("bk_counts", {}),
    }


# ─────────────────────────────────────────────────────────────────────────────
# OPPORTUNITIES ENDPOINT
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/opportunities/<sport_slug>")
def opportunities(sport_slug: str):
    """
    GET /api/combined/opportunities/<sport_slug>
    Returns top arb / EV+ / steam moves across both upcoming and live snapshots.

    Query params:
      mode    all | upcoming | live   (default: all)
      limit   max opportunities to return per type  (default: 20)
      min_ev  minimum EV% to include  (default: 2.0)
    """
    t0      = time.perf_counter()
    mode    = request.args.get("mode", "all")
    limit   = min(int(request.args.get("limit", 20)), 100)
    min_ev  = float(request.args.get("min_ev", 2.0))

    from app.workers.combined_merger import MultiBookMerger, detect_value_bets

    all_matches: list[dict] = []

    if mode in ("all", "upcoming"):
        cached = _cache_get(f"combined:upcoming:{sport_slug}")
        if cached:
            all_matches.extend(cached.get("matches", []))

    if mode in ("all", "live"):
        cached = _cache_get(f"combined:live:{sport_slug}")
        if cached:
            all_matches.extend(cached.get("matches", []))

    # Build flat lists from serialised dicts
    arbs:   list[dict] = []
    evs:    list[dict] = []
    steam:  list[dict] = []

    for m in all_matches:
        for a in (m.get("arbs") or []):
            arbs.append({
                **a,
                "join_key":    m["join_key"],
                "home_team":   m["home_team"],
                "away_team":   m["away_team"],
                "competition": m["competition"],
                "start_time":  m.get("start_time"),
                "is_live":     m.get("is_live"),
            })
        for e in (m.get("evs") or []):
            if e.get("ev_pct", 0) >= min_ev:
                evs.append({
                    **e,
                    "join_key":    m["join_key"],
                    "home_team":   m["home_team"],
                    "away_team":   m["away_team"],
                    "competition": m["competition"],
                    "start_time":  m.get("start_time"),
                    "is_live":     m.get("is_live"),
                })
        for s in (m.get("sharp") or []):
            if s.get("direction") == "steam_down":
                steam.append({
                    **s,
                    "join_key":    m["join_key"],
                    "home_team":   m["home_team"],
                    "away_team":   m["away_team"],
                    "competition": m["competition"],
                    "start_time":  m.get("start_time"),
                    "is_live":     m.get("is_live"),
                })

    arbs.sort(key=lambda x: -(x.get("profit_pct") or 0))
    evs.sort(key=lambda x:  -(x.get("ev_pct") or 0))
    steam.sort(key=lambda x: -(x.get("delta") or 0))

    return {
        "ok":          True,
        "sport":       sport_slug,
        "mode":        mode,
        "total_matches": len(all_matches),
        "arbs":        arbs[:limit],
        "evs":         evs[:limit],
        "steam":       steam[:limit],
        "arb_count":   len(arbs),
        "ev_count":    len(evs),
        "sharp_count": len(steam),
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
        "ts":          _now(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# HEAD-TO-HEAD COMPARE MATRIX
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/compare/<sport_slug>")
def compare_matrix(sport_slug: str):
    """
    GET /api/combined/compare/<sport_slug>?market=1x2&mode=upcoming

    Returns a tabular odds matrix for a single market across all merged matches.
    Useful for the "1X2 comparison" view.

    Response:
    {
      "headers": ["Match", "SP 1", "SP X", "SP 2", "BT 1", ..., "Best 1", ...],
      "rows": [...],
      "arb_rows": [...],   ← only rows with arb
    }
    """
    t0         = time.perf_counter()
    market_slug = request.args.get("market", "1x2")
    mode        = request.args.get("mode", "upcoming")
    limit       = min(int(request.args.get("limit", 100)), 500)

    cache_key = f"combined:{mode}:{sport_slug}"
    cached    = _cache_get(cache_key)
    if not cached or not cached.get("matches"):
        return {
            "ok": False,
            "error": f"No cached {mode} data for {sport_slug}. "
                     f"Connect to /stream/{mode}/{sport_slug} first.",
        }, 404

    rows: list[dict] = []
    for m in cached["matches"][:limit]:
        best_map = m.get("best", {}).get(market_slug)
        if not best_map:
            # Try line-variant slugs (e.g. over_under_goals_2.5)
            best_map = next(
                (v for k, v in m.get("best", {}).items() if k.startswith(market_slug)),
                None,
            )
        if not best_map:
            continue

        outcomes = sorted(best_map.keys())
        row: dict = {
            "join_key":    m["join_key"],
            "home_team":   m["home_team"],
            "away_team":   m["away_team"],
            "competition": m["competition"],
            "start_time":  m.get("start_time"),
            "is_live":     m.get("is_live"),
            "has_arb":     m.get("has_arb"),
            "best_arb_pct": m.get("best_arb_pct"),
        }
        for bk in ["sp", "bt", "od"]:
            for out in outcomes:
                odd = (m.get("markets", {}).get(bk) or {}).get(market_slug, {}).get(out)
                row[f"{bk}_{out}"] = round(odd, 3) if odd else None

        for out in outcomes:
            b = best_map.get(out)
            if b:
                row[f"best_{out}"]    = b.get("best_odd")
                row[f"best_bk_{out}"] = b.get("best_bk")

        rows.append(row)

    # Build column headers
    sample_outcomes = sorted(list(rows[0].get("best", {}).keys())) if rows else []

    return {
        "ok":          True,
        "sport":       sport_slug,
        "market":      market_slug,
        "mode":        mode,
        "total":       len(rows),
        "rows":        rows,
        "arb_rows":    [r for r in rows if r.get("has_arb")],
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
        "ts":          _now(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# STATUS
# ─────────────────────────────────────────────────────────────────────────────

@bp_combined.route("/status")
def status():
    t0 = time.perf_counter()
    sports_to_check = [
        "soccer", "basketball", "tennis", "ice-hockey",
        "volleyball", "cricket", "rugby", "table-tennis",
    ]
    summary = []
    for sport in sports_to_check:
        u = _cache_get(f"combined:upcoming:{sport}") or {}
        l = _cache_get(f"combined:live:{sport}") or {}
        if u or l:
            summary.append({
                "sport":           sport,
                "upcoming_count":  len(u.get("matches", [])),
                "live_count":      len(l.get("matches", [])),
                "upcoming_arbs":   sum(1 for m in u.get("matches", []) if m.get("has_arb")),
                "live_arbs":       sum(1 for m in l.get("matches", []) if m.get("has_arb")),
                "upcoming_evs":    sum(1 for m in u.get("matches", []) if m.get("has_ev")),
                "live_evs":        sum(1 for m in l.get("matches", []) if m.get("has_ev")),
                "harvested_at_up": u.get("harvested_at"),
                "harvested_at_lv": l.get("harvested_at"),
            })

    return {
        "ok":        True,
        "source":    "combined:sp+bt+od",
        "sports":    summary,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
        "ts":         _now(),
    }