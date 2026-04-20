"""
app/api/live.py
================
Live + Upcoming REST + SSE endpoints.

AUTH: All endpoints are open (no authentication required).
      Tier decorators are commented out — uncomment when pricing is added.

Endpoints:
  GET /live/matches          Live matches (paginated)
  GET /upcoming/matches      Upcoming matches (paginated, filterable)
  GET /stream                Unified SSE stream (live + upcoming)
  GET /upcoming/stream       Upcoming-only SSE stream
  GET /competitions          List competitions for a sport + mode
  GET /markets/best          Best odds per market across all BKs
  GET /markets/local         SP + BT + OD markets for one match
  GET /markets/b2b           B2B markets for one match
  GET /markets/all           All 10 BK markets for one match
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

from flask import request, Response, stream_with_context, jsonify

# ─── Blueprint import ─────────────────────────────────────────────────────────
from . import bp_live

# ─── Tier limits (will be enforced once pricing is live) ──────────────────────
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
KEEPALIVE_SEC = 15
SSE_TIMEOUT   = 3600   # 1 hour max per SSE connection


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get_redis():
    from app.workers.celery_tasks import _redis
    return _redis()


def _ok(data: dict):
    """JSON success response."""
    resp = jsonify(data)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


def _err(msg: str, code: int = 400):
    resp = jsonify({"error": msg, "code": code})
    resp.status_code = code
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


def _sse(event_type: str, data) -> str:
    body = json.dumps(data, default=str) if not isinstance(data, str) else data
    return f"event: {event_type}\ndata: {body}\n\n"


def _keepalive() -> str:
    return f": keepalive ts={int(time.time())}\n\n"


def _load_unified(mode: str, sport: str) -> list:
    """
    Load unified snapshot from Redis.
    Falls back to aggregating per-BK snapshots if unified not built yet.
    """
    r   = _get_redis()
    raw = r.get(f"odds:unified:{mode}:{sport}")
    if raw:
        try:
            d  = json.loads(raw)
            ms = d if isinstance(d, list) else d.get("matches", [])
            if ms:
                return ms
        except Exception:
            pass

    # Fallback: aggregate all per-BK snapshots
    all_matches: list = []
    seen: set         = set()
    for bk in _ALL_BKS:
        bk_raw = r.get(f"odds:{bk}:{mode}:{sport}")
        if not bk_raw:
            continue
        try:
            d  = json.loads(bk_raw)
            ms = d if isinstance(d, list) else d.get("matches", [])
            for m in ms:
                jk = str(
                    m.get("join_key") or m.get("parent_match_id") or
                    m.get("match_id") or m.get("betradar_id") or ""
                )
                nk = (
                    f"{(m.get('home_team') or '')[:10].lower()}"
                    f"|{(m.get('away_team') or '')[:10].lower()}"
                )
                key = jk or nk
                if key and key in seen:
                    continue
                seen.add(key)
                # Attach BK source to bookmakers dict
                if not m.get("bookmakers"):
                    m["bookmakers"] = {}
                if m.get("markets"):
                    m["bookmakers"].setdefault(bk, {
                        "match_id": m.get("match_id") or m.get("external_id") or "",
                        "markets":  m["markets"],
                    })
                all_matches.append(m)
        except Exception:
            pass
    return all_matches


def _filter_matches(matches: list, request_args) -> list:
    """
    Apply query filters:
      sport=soccer
      days=7          (only matches starting within N days; default: show all)
      from=2026-04-20 (ISO date, inclusive)
      to=2026-04-27   (ISO date, inclusive)
      competition=Premier League
      team=Arsenal    (home or away team contains this string)
      has_arb=true    (only arbitrage opportunities)
      mode=live|upcoming
    """
    now         = datetime.now(timezone.utc)
    days        = request_args.get("days", type=int)
    from_str    = request_args.get("from")
    to_str      = request_args.get("to")
    comp_filter = (request_args.get("competition") or "").lower()
    team_filter = (request_args.get("team") or "").lower()
    has_arb     = request_args.get("has_arb", "").lower() == "true"

    # Build datetime bounds
    cutoff_dt = now + timedelta(days=days) if days else None
    from_dt   = None
    to_dt     = None
    if from_str:
        try: from_dt = datetime.fromisoformat(from_str).replace(tzinfo=timezone.utc)
        except Exception: pass
    if to_str:
        try: to_dt = datetime.fromisoformat(to_str).replace(tzinfo=timezone.utc) + timedelta(days=1)
        except Exception: pass

    out = []
    for m in matches:
        st_raw = m.get("start_time")
        st_dt  = None
        if st_raw:
            try:
                s     = str(st_raw)
                s     = s if (s.endswith("Z") or "+" in s) else s + "Z"
                st_dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                pass

        # Days-ahead filter
        if cutoff_dt and st_dt and st_dt > cutoff_dt:
            continue
        # from/to date filter
        if from_dt and st_dt and st_dt < from_dt:
            continue
        if to_dt and st_dt and st_dt > to_dt:
            continue

        # Competition filter
        if comp_filter:
            mc = (m.get("competition") or "").lower()
            if comp_filter not in mc:
                continue

        # Team filter
        if team_filter:
            mh = (m.get("home_team") or "").lower()
            ma = (m.get("away_team") or "").lower()
            if team_filter not in mh and team_filter not in ma:
                continue

        # Arb filter
        if has_arb and not m.get("has_arb"):
            continue

        out.append(m)

    # Sort by start_time ascending
    out.sort(key=lambda m: str(m.get("start_time") or ""))
    return out


def _classify_sse(payload: dict, channel: str) -> str:
    explicit = payload.get("type") or payload.get("event") or ""
    if explicit: return str(explicit).lower()
    if "arb"      in channel: return "arb_updated"
    if "ev:"      in channel: return "ev_updated"
    if "live"     in channel: return "live_update"
    if "upcoming" in channel: return "snapshot_ready"
    return "update"


def _xp(p) -> float:
    if isinstance(p, (int, float)): return float(p)
    if isinstance(p, dict):
        return float(p.get("odd") or p.get("odds") or p.get("price") or p.get("best_price") or 0)
    return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# REST: COMPETITIONS
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/competitions", methods=["GET"])
def competitions_list():
    """
    Return unique competitions available in Redis for a given sport/mode.

    Query params:
      sport=soccer       (default: soccer)
      mode=upcoming|live (default: upcoming)
    """
    sport = request.args.get("sport", "soccer")
    mode  = request.args.get("mode", "upcoming")

    matches = _load_unified(mode, sport)
    if not matches:
        # Try all sports if specific sport has nothing
        for s in _ALL_SPORTS:
            matches = _load_unified(mode, s)
            if matches:
                break

    seen  = set()
    comps = []
    for m in matches:
        c = m.get("competition") or m.get("league") or ""
        if c and c not in seen:
            seen.add(c)
            comps.append(c)

    comps.sort()
    return _ok({
        "sport":        sport,
        "mode":         mode,
        "competitions": comps,
        "count":        len(comps),
    })


@bp_live.route("/sports", methods=["GET"])
def sports_list():
    """Return list of all supported sports with availability info."""
    r       = _get_redis()
    mode    = request.args.get("mode", "upcoming")
    result  = []
    for sport in _ALL_SPORTS:
        raw = r.get(f"odds:unified:{mode}:{sport}")
        count = 0
        if raw:
            try:
                d  = json.loads(raw)
                ms = d if isinstance(d, list) else d.get("matches", [])
                count = len(ms)
            except Exception:
                pass
        result.append({"slug": sport, "match_count": count, "has_data": count > 0})
    return _ok({"sports": result, "mode": mode})


# ══════════════════════════════════════════════════════════════════════════════
# REST: LIVE MATCHES
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/live/matches", methods=["GET"])
# @tier_required("basic")  # DISABLED — uncomment when pricing is live
def live_matches_list():
    """
    Paginated live matches.

    Query params:
      sport=soccer
      page=1
      limit=50
      competition=Premier League
      team=Arsenal
    """
    sport = request.args.get("sport", "soccer")
    page  = max(1, int(request.args.get("page", 1)))
    limit = min(200, max(1, int(request.args.get("limit", 50))))

    matches   = _load_unified("live", sport)
    matches   = _filter_matches(matches, request.args)
    total     = len(matches)
    paginated = matches[(page - 1) * limit: page * limit]

    return _ok({
        "mode":    "live",
        "sport":   sport,
        "page":    page,
        "limit":   limit,
        "total":   total,
        "matches": paginated,
        "ts":      time.time(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# REST: UPCOMING MATCHES
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/upcoming/matches", methods=["GET"])
# @tier_required("basic")  # DISABLED — uncomment when pricing is live
def upcoming_matches_list():
    """
    Paginated upcoming matches.

    Query params:
      sport=soccer
      page=1
      limit=50
      days=7             (only show next N days)
      from=2026-04-20    (ISO date)
      to=2026-04-27      (ISO date)
      competition=Champions League
      team=Real Madrid
      has_arb=true       (only arb opportunities)
    """
    sport = request.args.get("sport", "soccer")
    page  = max(1, int(request.args.get("page", 1)))
    limit = min(200, max(1, int(request.args.get("limit", 50))))

    matches   = _load_unified("upcoming", sport)
    matches   = _filter_matches(matches, request.args)
    total     = len(matches)
    paginated = matches[(page - 1) * limit: page * limit]

    return _ok({
        "mode":    "upcoming",
        "sport":   sport,
        "page":    page,
        "limit":   limit,
        "total":   total,
        "matches": paginated,
        "ts":      time.time(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# SSE: UPCOMING STREAM
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/upcoming/stream", methods=["GET"])
# @tier_required("basic")  # DISABLED — uncomment when pricing is live
def upcoming_stream():
    """
    SSE stream for upcoming matches.
    - Immediately sends current cache as `event: batch`
    - Pushes incremental updates as bookmakers re-harvest
    - Pushes arb/EV alerts as `event: arb_update`

    Query params: sport=soccer, days=7, competition=..., team=...
    """
    sport = request.args.get("sport", "soccer")

    def generate():
        r = _get_redis()

        # Step 1: send snapshot immediately
        matches = _filter_matches(_load_unified("upcoming", sport), request.args)
        if matches:
            yield _sse("batch", {
                "matches": matches, "sport": sport,
                "count":   len(matches), "source": "cache", "ts": time.time(),
            })

        yield _sse("connected", {
            "status": "connected", "sport": sport, "mode": "upcoming",
            "count": len(matches), "ts": time.time(),
        })

        # Step 2: subscribe to update channels
        pubsub   = r.pubsub(ignore_subscribe_messages=True)
        channels = [
            f"odds:all:upcoming:{sport}:updates",
            f"arb:updates:{sport}",
            f"ev:updates:{sport}",
        ]
        for bk in _ALL_BKS:
            channels.append(f"odds:{bk}:upcoming:{sport}:ready")

        pubsub.subscribe(*channels)

        last_ka  = time.time()
        deadline = time.time() + SSE_TIMEOUT

        try:
            while time.time() < deadline:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg["type"] == "message":
                    try:
                        payload  = json.loads(msg.get("data", "{}") or "{}")
                        channel  = msg.get("channel", b"")
                        if isinstance(channel, bytes):
                            channel = channel.decode("utf-8", errors="replace")
                        ev_type  = _classify_sse(payload, channel)

                        if ev_type == "snapshot_ready":
                            fresh = _filter_matches(_load_unified("upcoming", sport), request.args)
                            if fresh:
                                yield _sse("batch", {
                                    "matches": fresh, "sport": sport,
                                    "count":   len(fresh), "source": "live",
                                    "bk":      payload.get("bk"), "ts": time.time(),
                                })
                        elif ev_type in ("arb_updated", "ev_updated"):
                            yield _sse("arb_update", payload)
                        else:
                            yield _sse("update", payload)

                    except (json.JSONDecodeError, GeneratorExit):
                        return
                    except Exception:
                        pass

                if time.time() - last_ka > KEEPALIVE_SEC:
                    yield _keepalive()
                    last_ka = time.time()
        finally:
            try:
                pubsub.unsubscribe()
                pubsub.close()
            except Exception:
                pass

    resp = Response(stream_with_context(generate()), mimetype="text/event-stream")
    for k, v in SSE_HEADERS.items():
        resp.headers[k] = v
    return resp


# ══════════════════════════════════════════════════════════════════════════════
# SSE: UNIFIED LIVE + UPCOMING STREAM
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/stream", methods=["GET"])
# @tier_required("basic")  # DISABLED — uncomment when pricing is live
def unified_stream():
    """
    Unified SSE stream for both live and upcoming.

    Query params:
      mode=all|live|upcoming  (default: all)
      sport=soccer
      days=7
      competition=...
      team=...
      match_ids[]=...   (specific match IDs to track)
    """
    mode      = request.args.get("mode", "all")
    sport     = request.args.get("sport", "soccer")
    match_ids = request.args.getlist("match_ids[]")

    def generate():
        r = _get_redis()

        # Initial snapshots
        if mode in ("upcoming", "all"):
            ms = _filter_matches(_load_unified("upcoming", sport), request.args)
            if ms:
                yield _sse("batch", {
                    "mode": "upcoming", "matches": ms,
                    "sport": sport, "count": len(ms), "ts": time.time(),
                })

        if mode in ("live", "all"):
            ms = _filter_matches(_load_unified("live", sport), request.args)
            if ms:
                yield _sse("live_batch", {
                    "mode": "live", "matches": ms,
                    "sport": sport, "count": len(ms), "ts": time.time(),
                })

        # Subscribe channels
        channels = [
            f"odds:all:upcoming:{sport}:updates",
            f"odds:all:live:{sport}:updates",
            f"arb:updates:{sport}",
            f"ev:updates:{sport}",
        ]
        for bk in _ALL_BKS:
            channels.append(f"odds:{bk}:upcoming:{sport}:ready")
            channels.append(f"odds:{bk}:live:{sport}:ready")
        for mid in match_ids:
            channels.append(f"live:match:{mid}:all")

        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(*channels)

        yield _sse("connected", {
            "status": "connected", "mode": mode, "sport": sport,
            "channels": len(channels), "ts": time.time(),
        })

        last_ka  = time.time()
        deadline = time.time() + SSE_TIMEOUT

        try:
            while time.time() < deadline:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg["type"] == "message":
                    try:
                        payload = json.loads(msg.get("data", "{}") or "{}")
                        channel = msg.get("channel", b"")
                        if isinstance(channel, bytes):
                            channel = channel.decode("utf-8", errors="replace")
                        ev_type = _classify_sse(payload, channel)

                        if ev_type == "snapshot_ready":
                            if mode in ("upcoming", "all"):
                                fresh = _filter_matches(_load_unified("upcoming", sport), request.args)
                                if fresh:
                                    yield _sse("batch", {
                                        "mode": "upcoming", "matches": fresh,
                                        "sport": sport, "count": len(fresh),
                                        "bk": payload.get("bk"), "ts": time.time(),
                                    })
                            if mode in ("live", "all") and "live" in channel:
                                fresh = _filter_matches(_load_unified("live", sport), request.args)
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
                    except Exception:
                        pass

                if time.time() - last_ka > KEEPALIVE_SEC:
                    yield _keepalive()
                    last_ka = time.time()
        finally:
            try:
                pubsub.unsubscribe()
                pubsub.close()
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
# @tier_required("basic")  # DISABLED
def markets_best():
    """
    Best odds per market/outcome across all BKs for one match.

    Query params: match_id=..., sport=soccer
    """
    match_id = request.args.get("match_id") or request.args.get("join_key")
    sport    = request.args.get("sport", "soccer")
    if not match_id:
        return _err("match_id required", 400)

    m = _find_match(match_id, sport)
    if not m:
        return _err("Match not found", 404)

    best = _build_best(m, _ALL_BKS)
    return _ok({
        "match_id":  match_id,
        "home_team": m.get("home_team"),
        "away_team": m.get("away_team"),
        "sport":     sport,
        "markets":   best,
        "bk_count":  len(m.get("bookmakers") or {}),
        "ts":        time.time(),
    })


@bp_live.route("/markets/local", methods=["GET"])
# @tier_required("pro")  # DISABLED
def markets_local():
    """SP + BT + OD markets for one match."""
    match_id = request.args.get("match_id") or request.args.get("join_key")
    sport    = request.args.get("sport", "soccer")
    if not match_id:
        return _err("match_id required", 400)

    m = _find_match(match_id, sport)
    if not m:
        return _err("Match not found", 404)

    by_bk = _extract_bk_markets(m, _LOCAL_BKS, sport)
    return _ok({
        "match_id":   match_id,
        "home_team":  m.get("home_team"),
        "away_team":  m.get("away_team"),
        "sport":      sport,
        "bookmakers": by_bk,
        "best":       _best_from_dict(by_bk),
        "bk_group":   "local",
        "ts":         time.time(),
    })


@bp_live.route("/markets/b2b", methods=["GET"])
# @tier_required("pro")  # DISABLED
def markets_b2b():
    """All 7 B2B bookmaker markets for one match."""
    match_id = request.args.get("match_id") or request.args.get("join_key")
    sport    = request.args.get("sport", "soccer")
    if not match_id:
        return _err("match_id required", 400)

    m = _find_match(match_id, sport)
    if not m:
        return _err("Match not found", 404)

    by_bk = _extract_bk_markets(m, _B2B_BKS, sport)
    return _ok({
        "match_id":   match_id,
        "home_team":  m.get("home_team"),
        "away_team":  m.get("away_team"),
        "sport":      sport,
        "bookmakers": by_bk,
        "best":       _best_from_dict(by_bk),
        "bk_group":   "b2b",
        "ts":         time.time(),
    })


@bp_live.route("/markets/all", methods=["GET"])
# @tier_required("premium")  # DISABLED
def markets_all():
    """All 10 bookmaker markets combined with arb detection."""
    match_id = request.args.get("match_id") or request.args.get("join_key")
    sport    = request.args.get("sport", "soccer")
    if not match_id:
        return _err("match_id required", 400)

    m = _find_match(match_id, sport)
    if not m:
        return _err("Match not found", 404)

    by_bk    = _extract_bk_markets(m, _ALL_BKS, sport)
    best     = _best_from_dict(by_bk)
    arb_opps = _scan_arb(best)

    return _ok({
        "match_id":          match_id,
        "home_team":         m.get("home_team"),
        "away_team":         m.get("away_team"),
        "sport":             sport,
        "bookmakers":        by_bk,
        "best":              best,
        "bk_group":          "all",
        "bk_count":          len(by_bk),
        "arb_opportunities": arb_opps,
        "has_arb":           bool(arb_opps),
        "ts":                time.time(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _find_match(match_id: str, sport: str) -> dict | None:
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
                    br  = str(m.get("betradar_id") or "")
                    if match_id in (mid, jk, br):
                        return m
            except Exception:
                pass
    # DB fallback
    try:
        from app.models.odds import UnifiedMatch
        um = UnifiedMatch.query.get(match_id)
        if um:
            return {
                "match_id":   um.id,
                "join_key":   um.parent_match_id,
                "home_team":  um.home_team_name,
                "away_team":  um.away_team_name,
                "sport":      sport,
                "markets":    getattr(um, "markets", {}),
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
            if match_id in (mid, jk):
                return m
    except Exception:
        pass
    return None


def _extract_bk_markets(m: dict, bk_list: list, sport: str) -> dict:
    """Extract per-BK market dicts, supplementing from per-BK Redis if missing."""
    r     = _get_redis()
    by_bk = {}
    for bk in bk_list:
        bk_data = (m.get("bookmakers") or {}).get(bk)
        if bk_data and isinstance(bk_data, dict):
            by_bk[bk] = bk_data.get("markets") or {}
        else:
            # Try per-BK Redis snapshot
            for mode in ("upcoming", "live"):
                hit = _find_in_bk_snapshot(r, bk, mode, sport,
                                            str(m.get("match_id") or m.get("join_key") or ""))
                if hit:
                    by_bk[bk] = hit.get("markets") or {}
                    break
    return by_bk


def _build_best(m: dict, bk_list: list) -> dict:
    """Build best-odds matrix from bookmakers dict."""
    best = {}
    for bk, bk_data in (m.get("bookmakers") or {}).items():
        if bk not in bk_list:
            continue
        mkts = (bk_data.get("markets") if isinstance(bk_data, dict) else None) or {}
        for mkt, outcomes in mkts.items():
            if not isinstance(outcomes, dict):
                continue
            best.setdefault(mkt, {})
            for out, p in outcomes.items():
                price = _xp(p)
                if price > 1.0:
                    ex = best[mkt].get(out)
                    if not ex or price > ex["best_price"]:
                        best[mkt][out] = {"best_price": price, "bk_slug": bk}

    # DB-format markets fallback
    for mkt, outcomes in (m.get("markets") or {}).items():
        if not isinstance(outcomes, dict):
            continue
        best.setdefault(mkt, {})
        for out, p in outcomes.items():
            price = _xp(p)
            bk    = (p.get("bk_slug") if isinstance(p, dict) else None) or "sp"
            if price > 1.0:
                ex = best[mkt].get(out)
                if not ex or price > ex["best_price"]:
                    best[mkt][out] = {"best_price": price, "bk_slug": bk}
    return best


def _best_from_dict(by_bk: dict) -> dict:
    best = {}
    for bk, mkts in by_bk.items():
        if not isinstance(mkts, dict):
            continue
        for mkt, outcomes in mkts.items():
            if not isinstance(outcomes, dict):
                continue
            best.setdefault(mkt, {})
            for out, p in outcomes.items():
                price = _xp(p)
                if price > 1.0:
                    ex = best[mkt].get(out)
                    if not ex or price > ex["best_price"]:
                        best[mkt][out] = {"best_price": price, "bk_slug": bk}
    return best


def _scan_arb(best: dict) -> list:
    arbs = []
    for mkt, ob in best.items():
        keys = list(ob.keys())
        n    = len(keys)
        if mkt in ("match_winner", "1x2", "moneyline"):
            exp = 3
        elif mkt.startswith("over_under_") or mkt in ("btts", "odd_even"):
            exp = 2
        else:
            exp = n
        if n < max(exp, 2):
            continue
        use = keys[:exp]
        prices = [ob[k]["best_price"] for k in use if ob[k]["best_price"] > 1]
        if len(prices) < exp:
            continue
        sum_inv = sum(1.0 / p for p in prices)
        if 0 < sum_inv < 1.0:
            arbs.append({
                "market":     mkt,
                "profit_pct": round((1.0 - sum_inv) * 100, 3),
                "sum_inv":    round(sum_inv, 6),
                "legs":       [{"outcome": k, **ob[k]} for k in use],
            })
    return arbs