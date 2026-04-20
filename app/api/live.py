"""
app/api/live.py
================
Live + Upcoming REST + SSE endpoints.

Tier access:
  FREE    — SSE stream for today's soccer only (1 sport, today)
  BASIC   — SSE stream all sports, today only
  PRO     — SSE stream all sports, 7 days ahead, weekly calendar
  PREMIUM — Everything + combined markets API

Streaming (SSE) is available to ALL tiers, including free.
Limits are on: number of sports, days ahead, markets detail level.

Endpoints:
  GET /live/matches          free+   Paginated live matches
  GET /upcoming/matches      free+   Paginated upcoming (tier-filtered)
  GET /stream                free+   Unified SSE (live + upcoming)
  GET /upcoming/stream       free+   Upcoming-only SSE
  GET /markets/local         pro+    SP+BT+OD combined markets for one match
  GET /markets/b2b           pro+    B2B combined markets for one match
  GET /markets/all           premium All 10 BK markets for one match
  GET /markets/best          free+   Best odds per market across available BKs
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

from flask import request, Response, stream_with_context, g

from . import bp_live, _signed_response, _err
from .decorators import tier_required, optional_auth

# ─── Tier limits ──────────────────────────────────────────────────────────────
_TIER_SPORTS = {
    "free":    ["soccer"],
    "basic":   None,   # None = all sports
    "pro":     None,
    "premium": None,
    "admin":   None,
}
_TIER_DAYS = {
    "free":    1,
    "basic":   1,
    "pro":     7,
    "premium": 30,
    "admin":   90,
}
_LOCAL_BKS = ["sp", "bt", "od"]
_B2B_BKS   = ["1xbet", "22bet", "betwinner", "melbet", "megapari", "helabet", "paripesa"]
_ALL_BKS   = _LOCAL_BKS + _B2B_BKS

_ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby", "ice-hockey",
    "volleyball", "handball", "table-tennis", "baseball", "mma", "boxing",
    "darts", "american-football", "esoccer",
]

SSE_HEADERS = {
    "Cache-Control":     "no-cache, no-transform",
    "X-Accel-Buffering": "no",
    "Connection":        "keep-alive",
    "Content-Type":      "text/event-stream; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
}
KEEPALIVE_SEC  = 15
SSE_TIMEOUT    = 3600


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get_redis():
    from app.workers.celery_tasks import _redis
    return _redis()


def _sse(event_type: str, data) -> str:
    body = json.dumps(data, default=str) if isinstance(data, dict) else data
    return f"event: {event_type}\ndata: {body}\n\n"


def _keepalive() -> str:
    return f": keepalive ts={int(time.time())}\n\n"


def _tier_filter_matches(matches: list, tier: str, sport_param: str | None) -> list:
    """Filter matches by tier: allowed sports + days ahead."""
    allowed_sports = _TIER_SPORTS.get(tier)
    days_ahead     = _TIER_DAYS.get(tier, 1)
    cutoff         = (datetime.now(timezone.utc) + timedelta(days=days_ahead)).isoformat()

    out = []
    for m in matches:
        # Sport filter
        if allowed_sports and sport_param and sport_param not in allowed_sports:
            continue   # requested sport not allowed for this tier
        if allowed_sports:
            ms = (m.get("sport") or "").lower()
            if ms not in allowed_sports and not any(ms in s for s in allowed_sports):
                continue

        # Days-ahead filter
        st = m.get("start_time")
        if st and str(st) > cutoff:
            continue

        out.append(m)
    return out


def _load_unified(mode: str, sport: str) -> list:
    """Load the unified snapshot from Redis (real-time)."""
    r   = _get_redis()
    raw = r.get(f"odds:unified:{mode}:{sport}")
    if raw:
        try:
            d = json.loads(raw)
            ms = d if isinstance(d, list) else d.get("matches", [])
            return ms
        except Exception:
            pass
    # Fallback: aggregate per-BK snapshots
    all_matches: list = []
    seen: set = set()
    for bk in _ALL_BKS:
        bk_raw = r.get(f"odds:{bk}:{mode}:{sport}")
        if not bk_raw:
            continue
        try:
            d  = json.loads(bk_raw)
            ms = d if isinstance(d, list) else d.get("matches", [])
            for m in ms:
                jk = str(m.get("join_key") or m.get("parent_match_id") or m.get("match_id") or "")
                nk = f"{(m.get('home_team',''))[:8]}|{(m.get('away_team',''))[:8]}"
                key = jk or nk
                if key and key in seen:
                    continue
                seen.add(key)
                all_matches.append(m)
        except Exception:
            pass
    return all_matches


# ══════════════════════════════════════════════════════════════════════════════
# REST: LIVE MATCHES  (free+)
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/live/matches", methods=["GET"])
@optional_auth
def live_matches_list():
    sport = request.args.get("sport", "soccer")
    page  = max(1, int(request.args.get("page", 1)))
    limit = min(200, max(1, int(request.args.get("limit", 50))))
    tier  = getattr(g, "tier", "free")

    allowed = _TIER_SPORTS.get(tier)
    if allowed and sport not in allowed:
        return _err(f"Sport '{sport}' requires basic tier or above", 403)

    matches   = _load_unified("live", sport)
    matches   = _tier_filter_matches(matches, tier, sport)
    total     = len(matches)
    paginated = matches[(page - 1) * limit: page * limit]

    return _signed_response({
        "mode": "live", "sport": sport, "page": page,
        "limit": limit, "total": total, "matches": paginated,
    })


# ══════════════════════════════════════════════════════════════════════════════
# REST: UPCOMING MATCHES  (free+)
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/upcoming/matches", methods=["GET"])
@optional_auth
def upcoming_matches_list():
    sport = request.args.get("sport", "soccer")
    page  = max(1, int(request.args.get("page", 1)))
    limit = min(200, max(1, int(request.args.get("limit", 50))))
    tier  = getattr(g, "tier", "free")

    allowed = _TIER_SPORTS.get(tier)
    if allowed and sport not in allowed:
        return _err(f"Sport '{sport}' requires basic tier or above", 403)

    matches   = _load_unified("upcoming", sport)
    matches   = _tier_filter_matches(matches, tier, sport)
    total     = len(matches)
    paginated = matches[(page - 1) * limit: page * limit]

    return _signed_response({
        "mode": "upcoming", "sport": sport, "page": page,
        "limit": limit, "total": total, "matches": paginated,
    })


# ══════════════════════════════════════════════════════════════════════════════
# SSE: UPCOMING STREAM  (free+ with tier limits)
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/upcoming/stream", methods=["GET"])
@optional_auth
def upcoming_stream():
    sport = request.args.get("sport", "soccer")
    tier  = getattr(g, "tier", "free")

    allowed = _TIER_SPORTS.get(tier)
    if allowed and sport not in allowed:
        def _deny():
            yield _sse("error", {
                "code": "tier_required", "sport": sport,
                "message": f"Sport '{sport}' requires basic tier. Upgrade at kinetic.bet/pricing",
            })
        return Response(stream_with_context(_deny()),
                        mimetype="text/event-stream", headers=SSE_HEADERS)

    def generate():
        r = _get_redis()
        # Send cached snapshot immediately
        matches = _tier_filter_matches(_load_unified("upcoming", sport), tier, sport)
        if matches:
            yield _sse("batch", {
                "matches": matches, "sport": sport,
                "count": len(matches), "source": "cache", "ts": time.time(),
            })
        yield _sse("connected", {
            "status": "connected", "sport": sport, "mode": "upcoming",
            "tier": tier, "count": len(matches), "ts": time.time(),
        })

        pubsub   = r.pubsub(ignore_subscribe_messages=True)
        channels = [
            f"odds:all:upcoming:{sport}:updates",
            f"arb:updates:{sport}", f"ev:updates:{sport}",
        ]
        for bk in (_LOCAL_BKS if tier == "free" else _ALL_BKS):
            channels.append(f"odds:{bk}:upcoming:{sport}:ready")
        pubsub.subscribe(*channels)

        last_ka  = time.time()
        deadline = time.time() + SSE_TIMEOUT

        try:
            while time.time() < deadline:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg["type"] == "message":
                    try:
                        payload  = json.loads(msg.get("data", "{}"))
                        channel  = msg.get("channel", "")
                        ev_type  = _classify(payload, channel)

                        if ev_type == "snapshot_ready":
                            fresh = _tier_filter_matches(
                                _load_unified("upcoming", sport), tier, sport
                            )
                            if fresh:
                                yield _sse("batch", {
                                    "matches": fresh, "sport": sport,
                                    "count": len(fresh), "source": "live",
                                    "bk": payload.get("bk"), "ts": time.time(),
                                })
                        elif ev_type in ("arb_updated", "ev_updated"):
                            yield _sse("arb_update", payload)
                        else:
                            yield _sse("update", payload)
                    except (json.JSONDecodeError, GeneratorExit):
                        return

                if time.time() - last_ka > KEEPALIVE_SEC:
                    yield _keepalive()
                    last_ka = time.time()
        finally:
            try:
                pubsub.unsubscribe(); pubsub.close()
            except Exception:
                pass

    resp = Response(stream_with_context(generate()), mimetype="text/event-stream")
    for k, v in SSE_HEADERS.items():
        resp.headers[k] = v
    return resp


# ══════════════════════════════════════════════════════════════════════════════
# SSE: UNIFIED LIVE + UPCOMING  (free+ with tier limits)
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/stream", methods=["GET"])
@optional_auth
def unified_stream():
    mode      = request.args.get("mode", "all")
    sport     = request.args.get("sport", "soccer")
    match_ids = request.args.getlist("match_ids[]")
    tier      = getattr(g, "tier", "free")

    allowed = _TIER_SPORTS.get(tier)
    if allowed and sport not in allowed:
        def _deny():
            yield _sse("error", {
                "code": "tier_required",
                "message": f"Sport '{sport}' requires basic tier or above",
            })
        return Response(stream_with_context(_deny()),
                        mimetype="text/event-stream", headers=SSE_HEADERS)

    def generate():
        r = _get_redis()

        # Initial snapshots
        if mode in ("upcoming", "all"):
            ms = _tier_filter_matches(_load_unified("upcoming", sport), tier, sport)
            if ms:
                yield _sse("batch", {
                    "mode": "upcoming", "matches": ms,
                    "sport": sport, "count": len(ms), "ts": time.time(),
                })
        if mode in ("live", "all"):
            ms = _tier_filter_matches(_load_unified("live", sport), tier, sport)
            if ms:
                yield _sse("live_batch", {
                    "mode": "live", "matches": ms,
                    "sport": sport, "count": len(ms), "ts": time.time(),
                })

        # Subscribe channels
        channels = [
            f"odds:all:upcoming:{sport}:updates",
            f"odds:all:live:{sport}:updates",
            f"arb:updates:{sport}", f"ev:updates:{sport}",
        ]
        bk_pool = _LOCAL_BKS if tier == "free" else _ALL_BKS
        for bk in bk_pool:
            channels.append(f"odds:{bk}:upcoming:{sport}:ready")
            channels.append(f"odds:{bk}:live:{sport}:ready")
        for mid in match_ids:
            channels.append(f"live:match:{mid}:all")

        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(*channels)

        yield _sse("connected", {
            "status": "connected", "mode": mode, "sport": sport,
            "tier": tier, "channels": len(channels), "ts": time.time(),
        })

        last_ka  = time.time()
        deadline = time.time() + SSE_TIMEOUT

        try:
            while time.time() < deadline:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg["type"] == "message":
                    try:
                        payload = json.loads(msg.get("data", "{}"))
                        channel = msg.get("channel", "")
                        ev_type = _classify(payload, channel)

                        if ev_type == "snapshot_ready":
                            if mode in ("upcoming", "all"):
                                fresh = _tier_filter_matches(
                                    _load_unified("upcoming", sport), tier, sport
                                )
                                if fresh:
                                    yield _sse("batch", {
                                        "mode": "upcoming", "matches": fresh,
                                        "sport": sport, "count": len(fresh),
                                        "bk": payload.get("bk"), "ts": time.time(),
                                    })
                            if mode in ("live", "all") and "live" in channel:
                                fresh = _tier_filter_matches(
                                    _load_unified("live", sport), tier, sport
                                )
                                if fresh:
                                    yield _sse("live_batch", {
                                        "mode": "live", "matches": fresh,
                                        "sport": sport, "count": len(fresh),
                                        "ts": time.time(),
                                    })
                        elif ev_type in ("live_update", "event_update", "market_update"):
                            yield _sse(ev_type, payload)
                        elif ev_type in ("arb_updated", "ev_updated"):
                            yield _sse("arb_update", payload)
                        elif ev_type == "goal":
                            yield _sse("goal", payload)
                        elif ev_type == "status_change":
                            yield _sse("status_change", payload)
                        else:
                            yield _sse("update", payload)
                    except (json.JSONDecodeError, GeneratorExit):
                        return

                if time.time() - last_ka > KEEPALIVE_SEC:
                    yield _keepalive()
                    last_ka = time.time()
        finally:
            try:
                pubsub.unsubscribe(); pubsub.close()
            except Exception:
                pass

    resp = Response(stream_with_context(generate()), mimetype="text/event-stream")
    for k, v in SSE_HEADERS.items():
        resp.headers[k] = v
    return resp


# ══════════════════════════════════════════════════════════════════════════════
# MARKETS ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/markets/best", methods=["GET"])
@optional_auth
def markets_best():
    """
    Best odds per market/outcome across all available BKs for one match.
    Available to ALL tiers (free+). Returns top-level best_price per outcome.
    """
    match_id = request.args.get("match_id") or request.args.get("join_key")
    sport    = request.args.get("sport", "soccer")
    tier     = getattr(g, "tier", "free")

    if not match_id:
        return _err("match_id required", 400)

    m = _find_match_any(match_id, sport)
    if not m:
        return _err("Match not found", 404)

    # Build best-odds matrix
    bk_pool  = _LOCAL_BKS if tier == "free" else _ALL_BKS
    best     = {}
    for bk_slug, bk_data in (m.get("bookmakers") or {}).items():
        if bk_slug not in bk_pool:
            continue
        mkts = (bk_data.get("markets") if isinstance(bk_data, dict) else None) or {}
        for mkt, outcomes in mkts.items():
            if not isinstance(outcomes, dict):
                continue
            best.setdefault(mkt, {})
            for out, p in outcomes.items():
                price = _xp(p)
                if price > 1.0:
                    existing = best[mkt].get(out)
                    if not existing or price > existing["best_price"]:
                        best[mkt][out] = {"best_price": price, "bk_slug": bk_slug}

    # Also use top-level markets (DB format)
    for mkt, outcomes in (m.get("markets") or {}).items():
        if not isinstance(outcomes, dict):
            continue
        best.setdefault(mkt, {})
        for out, p in outcomes.items():
            price = _xp_db(p)
            bk    = (p.get("bk_slug") if isinstance(p, dict) else None) or "sp"
            if bk not in bk_pool:
                continue
            if price > 1.0:
                existing = best[mkt].get(out)
                if not existing or price > existing["best_price"]:
                    best[mkt][out] = {"best_price": price, "bk_slug": bk}

    return _signed_response({
        "match_id":  match_id,
        "home_team": m.get("home_team"),
        "away_team": m.get("away_team"),
        "sport":     sport,
        "markets":   best,
        "bks_used":  bk_pool,
        "tier":      tier,
    })


@bp_live.route("/markets/local", methods=["GET"])
@tier_required("pro")
def markets_local():
    """
    SP + BT + OD combined markets for one match.
    Pro tier required.
    """
    match_id = request.args.get("match_id") or request.args.get("join_key")
    sport    = request.args.get("sport", "soccer")
    if not match_id:
        return _err("match_id required", 400)

    m = _find_match_any(match_id, sport)
    if not m:
        return _err("Match not found", 404)

    markets_by_bk = {}
    for bk in _LOCAL_BKS:
        bk_data = (m.get("bookmakers") or {}).get(bk)
        if bk_data:
            markets_by_bk[bk] = bk_data.get("markets") if isinstance(bk_data, dict) else {}

    # Also load directly from per-BK Redis keys if not in unified
    r = _get_redis()
    for bk in _LOCAL_BKS:
        if bk not in markets_by_bk:
            bk_ms = _find_in_bk_snapshot(r, bk, "upcoming", sport, match_id)
            if bk_ms:
                markets_by_bk[bk] = bk_ms.get("markets") or {}

    best = _compute_best(markets_by_bk)

    return _signed_response({
        "match_id":    match_id,
        "home_team":   m.get("home_team"),
        "away_team":   m.get("away_team"),
        "sport":       sport,
        "bookmakers":  markets_by_bk,
        "best":        best,
        "bk_group":    "local",
    })


@bp_live.route("/markets/b2b", methods=["GET"])
@tier_required("pro")
def markets_b2b():
    """
    All B2B bookmaker markets for one match (7 BKs combined).
    Pro tier required.
    """
    match_id = request.args.get("match_id") or request.args.get("join_key")
    sport    = request.args.get("sport", "soccer")
    if not match_id:
        return _err("match_id required", 400)

    m = _find_match_any(match_id, sport)
    if not m:
        return _err("Match not found", 404)

    markets_by_bk = {}
    for bk in _B2B_BKS:
        bk_data = (m.get("bookmakers") or {}).get(bk)
        if bk_data:
            markets_by_bk[bk] = bk_data.get("markets") if isinstance(bk_data, dict) else {}

    r = _get_redis()
    for bk in _B2B_BKS:
        if bk not in markets_by_bk:
            bk_ms = _find_in_bk_snapshot(r, bk, "upcoming", sport, match_id)
            if bk_ms:
                markets_by_bk[bk] = bk_ms.get("markets") or {}

    best = _compute_best(markets_by_bk)

    return _signed_response({
        "match_id":   match_id,
        "home_team":  m.get("home_team"),
        "away_team":  m.get("away_team"),
        "sport":      sport,
        "bookmakers": markets_by_bk,
        "best":       best,
        "bk_group":   "b2b",
    })


@bp_live.route("/markets/all", methods=["GET"])
@tier_required("premium")
def markets_all():
    """
    All 10 bookmaker markets combined.
    Premium tier required.
    """
    match_id = request.args.get("match_id") or request.args.get("join_key")
    sport    = request.args.get("sport", "soccer")
    if not match_id:
        return _err("match_id required", 400)

    m = _find_match_any(match_id, sport)
    if not m:
        return _err("Match not found", 404)

    markets_by_bk = {}
    for bk in _ALL_BKS:
        bk_data = (m.get("bookmakers") or {}).get(bk)
        if bk_data:
            markets_by_bk[bk] = bk_data.get("markets") if isinstance(bk_data, dict) else {}

    r = _get_redis()
    for bk in _ALL_BKS:
        if bk not in markets_by_bk:
            bk_ms = _find_in_bk_snapshot(r, bk, "upcoming", sport, match_id)
            if bk_ms:
                markets_by_bk[bk] = bk_ms.get("markets") or {}

    best     = _compute_best(markets_by_bk)
    arb_info = _quick_arb(best)

    return _signed_response({
        "match_id":          match_id,
        "home_team":         m.get("home_team"),
        "away_team":         m.get("away_team"),
        "sport":             sport,
        "bookmakers":        markets_by_bk,
        "best":              best,
        "bk_group":          "all",
        "bk_count":          len(markets_by_bk),
        "arb_opportunities": arb_info,
        "has_arb":           bool(arb_info),
    })


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _classify(payload: dict, channel: str) -> str:
    explicit = payload.get("type") or payload.get("event") or ""
    if explicit:
        return str(explicit).lower()
    if "arb" in channel:    return "arb_updated"
    if "ev:" in channel:    return "ev_updated"
    if "live" in channel:   return "live_update"
    if "upcoming" in channel or payload.get("event") == "snapshot_ready":
        return "snapshot_ready"
    return "update"


def _xp(p) -> float:
    if isinstance(p, (int, float)): return float(p)
    if isinstance(p, dict):
        return float(p.get("odd") or p.get("odds") or p.get("price") or 0)
    return 0.0


def _xp_db(p) -> float:
    if isinstance(p, (int, float)): return float(p)
    if isinstance(p, dict):
        return float(p.get("best_price") or p.get("odd") or p.get("odds") or p.get("price") or 0)
    return 0.0


def _find_match_any(match_id: str, sport: str) -> dict | None:
    """Find a match in Redis (unified or per-BK) or DB."""
    r = _get_redis()
    for mode in ("upcoming", "live"):
        raw = r.get(f"odds:unified:{mode}:{sport}")
        if raw:
            try:
                d  = json.loads(raw)
                ms = d if isinstance(d, list) else d.get("matches", [])
                for m in ms:
                    mid = str(m.get("match_id") or m.get("parent_match_id") or m.get("join_key") or "")
                    jk  = str(m.get("join_key") or "")
                    if mid == str(match_id) or jk == str(match_id):
                        return m
            except Exception:
                pass
    # DB fallback
    try:
        from app.models.odds import UnifiedMatch
        um = UnifiedMatch.query.get(match_id)
        if um:
            return {
                "match_id":  um.id, "join_key": um.parent_match_id,
                "home_team": um.home_team_name, "away_team": um.away_team_name,
                "sport":     sport,
                "markets":   getattr(um, "markets", {}),
                "bookmakers": getattr(um, "bookmaker_odds", {}),
            }
    except Exception:
        pass
    return None


def _find_in_bk_snapshot(r, bk: str, mode: str, sport: str, match_id: str) -> dict | None:
    raw = r.get(f"odds:{bk}:{mode}:{sport}")
    if not raw:
        return None
    try:
        d  = json.loads(raw)
        ms = d if isinstance(d, list) else d.get("matches", [])
        for m in ms:
            mid = str(m.get("match_id") or m.get("parent_match_id") or m.get("external_id") or "")
            jk  = str(m.get("join_key") or "")
            if mid == str(match_id) or jk == str(match_id):
                return m
    except Exception:
        pass
    return None


def _compute_best(markets_by_bk: dict) -> dict:
    """Compute best-price per outcome across multiple BKs."""
    best = {}
    for bk_slug, mkts in markets_by_bk.items():
        if not isinstance(mkts, dict):
            continue
        for mkt, outcomes in mkts.items():
            if not isinstance(outcomes, dict):
                continue
            best.setdefault(mkt, {})
            for out, p in outcomes.items():
                price = _xp(p)
                if price > 1.0:
                    existing = best[mkt].get(out)
                    if not existing or price > existing["best_price"]:
                        best[mkt][out] = {"best_price": price, "bk_slug": bk_slug}
    return best


def _quick_arb(best: dict) -> list:
    """Quick arb scan on a best-odds dict. Returns list of arb opportunities."""
    arbs = []
    for mkt, ob in best.items():
        keys = list(ob.keys())
        n    = len(keys)
        if mkt in ("match_winner", "1x2"):
            exp = 3
        elif mkt.startswith("over_under_") or mkt in ("btts", "odd_even"):
            exp = 2
        else:
            exp = n
        if n < max(exp, 2):
            continue
        use = keys[:exp]
        sum_inv = sum(1.0 / ob[k]["best_price"] for k in use if ob[k]["best_price"] > 1)
        if 0 < sum_inv < 1.0:
            arbs.append({
                "market":     mkt,
                "profit_pct": round((1.0 - sum_inv) * 100, 3),
                "legs":       [{"outcome": k, **ob[k]} for k in use],
            })
    return arbs