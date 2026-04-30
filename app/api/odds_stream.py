"""
app/api/odds_stream.py
======================
Unified SSE stream + REST snapshot for ALL 10 bookmakers.

Routes
------
GET /odds/stream/<mode>/<sport>      – SSE (token via ?token= because EventSource
                                       cannot send Authorization headers)
GET /odds/snapshot/<mode>/<sport>    – REST snapshot (Bearer auth OK)
GET /api/monitor/competitions        – competition list extracted from Redis
GET /api/monitor/stats               – per-sport match / bk counts

Tier rules
----------
basic   → SP + BT + OD only, single snapshot on connect, no live push
pro     → all 10 BKs, live pubsub push on every harvest
premium → all 10 BKs + live push + arb/ev events
admin   → same as premium (internal use)
"""
from __future__ import annotations

import json
import time
import logging
from functools import wraps

from flask import Blueprint, Response, request, stream_with_context, g

log = logging.getLogger(__name__)

# ── Blueprints ────────────────────────────────────────────────────────────────
bp_stream  = Blueprint("odds_stream",  __name__)          # /odds/...
bp_monitor = Blueprint("odds_monitor", __name__, url_prefix="/api/monitor")

# ── Constants ─────────────────────────────────────────────────────────────────
_TIER_RANK = {"free": 0, "basic": 1, "pro": 2, "premium": 3, "admin": 4}
_LOCAL_BKS = {"sp", "bt", "od"}
_ALL_BKS   = {"sp","bt","od","1xbet","22bet","betwinner","melbet","megapari","helabet","paripesa"}
_KEEPALIVE_INTERVAL = 20   # seconds

ALL_SPORTS = [
    "soccer","basketball","tennis","cricket","rugby","ice-hockey",
    "volleyball","handball","table-tennis","baseball","mma","boxing",
    "darts","american-football","esoccer",
]


# ─────────────────────────────────────────────────────────────────────────────
# Auth helpers
# ─────────────────────────────────────────────────────────────────────────────

def _auth_user():
    """
    Resolve Customer from:
      1. Authorization: Bearer <jwt>   (normal REST)
      2. ?token=<jwt>                  (EventSource / SSE — no header support)
      3. X-Api-Key: <key>              (API key auth)
    Returns Customer | None.
    """
    from app.utils.customer_jwt_helpers import _decode_token
    from app.models.customer import Customer

    # 1. Bearer header
    auth = request.headers.get("Authorization", "")
    token = auth[7:] if auth.startswith("Bearer ") else None

    # 2. Query-param token (SSE only path)
    if not token:
        token = request.args.get("token", "").strip() or None

    if token:
        try:
            payload = _decode_token(token)
            if payload.get("type") not in ("access", "api"):
                return None
            return Customer.query.get(int(payload["sub"]))
        except Exception:
            return None

    # 3. API key
    api_key = request.headers.get("X-Api-Key", "").strip()
    if api_key:
        try:
            from app.models.api_key import ApiKey
            ak = ApiKey.query.filter_by(key=api_key, is_active=True).first()
            if ak:
                user = Customer.query.get(ak.user_id)
                return user if (user and user.is_active) else None
        except Exception:
            pass

    return None


def _tier_rank(user) -> int:
    if not user:
        return -1
    return _TIER_RANK.get(getattr(user, "tier", "basic") or "basic", 1)


def require_tier(min_tier: str):
    """Decorator for REST endpoints (Bearer auth only)."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            from app.api import _err
            user = _auth_user()
            if not user:
                return _err("Authentication required", 401)
            if _tier_rank(user) < _TIER_RANK.get(min_tier, 99):
                return _err(f"{min_tier.title()} tier or higher required", 403)
            g.user = user
            return fn(*args, **kwargs)
        return wrapper
    return decorator


# ─────────────────────────────────────────────────────────────────────────────
# Redis helpers
# ─────────────────────────────────────────────────────────────────────────────

def _r():
    from app.workers.celery_tasks import _redis
    return _redis()


def _get_unified(mode: str, sport: str) -> list[dict]:
    """Read the unified match list from Redis. Returns [] on miss."""
    try:
        raw = _r().get(f"odds:unified:{mode}:{sport}")
        if not raw:
            return []
        data = json.loads(raw)
        return data.get("matches", []) if isinstance(data, dict) else (data or [])
    except Exception as exc:
        log.debug("[stream] redis read error: %s", exc)
        return []


def _strip_to_local(matches: list[dict]) -> list[dict]:
    """Return matches with only SP/BT/OD bookmaker data (basic tier)."""
    out = []
    for m in matches:
        mc = {**m}
        bks = mc.get("bookmakers") or {}
        mc["bookmakers"] = {k: v for k, v in bks.items() if k in _LOCAL_BKS}
        out.append(mc)
    return out


def _enrich_matches(matches: list[dict], tier: str) -> list[dict]:
    """Apply tier-based bookmaker filtering."""
    if tier in ("pro", "premium", "admin"):
        return matches
    return _strip_to_local(matches)


# ─────────────────────────────────────────────────────────────────────────────
# SSE generator factory
# ─────────────────────────────────────────────────────────────────────────────

def _sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _make_generator(mode: str, sport: str, user, is_live_tier: bool):
    """
    Core SSE generator.
    - Sends snapshot immediately.
    - basic: keepalive then closes (polling model).
    - pro/premium: stays open, subscribes to Redis pubsub for live pushes.
    """
    tier = getattr(user, "tier", "basic") or "basic"

    def generate():
        r = _r()
        snapshot_key = f"odds:unified:{mode}:{sport}"

        # ── 1. Immediate snapshot ────────────────────────────────────────────
        matches = _enrich_matches(_get_unified(mode, sport), tier)
        yield _sse_event("batch", {
            "matches": matches,
            "source":  "snapshot",
            "sport":   sport,
            "mode":    mode,
            "count":   len(matches),
            "tier":    tier,
        })
        yield _sse_event("connected", {
            "status":  "connected",
            "sport":   sport,
            "mode":    mode,
            "tier":    tier,
            "live_push": is_live_tier,
        })

        # ── 2. Basic tier: single snapshot + keepalive, done ─────────────────
        if not is_live_tier:
            yield ": keepalive\n\n"
            return

        # ── 3. Pro/Premium: subscribe to live pubsub pushes ──────────────────
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        channels = [
            f"odds:all:{mode}:{sport}:updates",   # harvest completions
            f"arb:updates:{sport}",               # arb computations
            f"ev:updates:{sport}",                # ev computations
        ]
        if mode == "live":
            channels.append(f"bus:live_updates:{sport}")

        pubsub.subscribe(*channels)
        last_ka = time.time()

        try:
            while True:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg.get("type") == "message":
                    try:
                        payload = json.loads(msg["data"])
                        ch = msg.get("channel", b"")
                        if isinstance(ch, bytes):
                            ch = ch.decode()

                        if "arb:" in ch:
                            yield _sse_event("arb_update", payload)

                        elif "ev:" in ch:
                            yield _sse_event("ev_update", payload)

                        elif "live_updates" in ch:
                            # Lightweight per-match score/time delta
                            yield _sse_event("live_update", payload)

                        else:
                            # Full unified snapshot refresh after a harvest
                            fresh = _enrich_matches(_get_unified(mode, sport), tier)
                            yield _sse_event("batch", {
                                "matches": fresh,
                                "source":  "live",
                                "sport":   sport,
                                "mode":    mode,
                                "count":   len(fresh),
                            })

                    except Exception as exc:
                        log.debug("[stream] pubsub parse error: %s", exc)

                # keepalive every N seconds to prevent proxy timeouts
                if time.time() - last_ka > _KEEPALIVE_INTERVAL:
                    yield ": keepalive\n\n"
                    last_ka = time.time()

        finally:
            try:
                pubsub.unsubscribe(*channels)
                pubsub.close()
            except Exception:
                pass

    return generate


# ─────────────────────────────────────────────────────────────────────────────
# SSE route  –  /odds/stream/<mode>/<sport>
# ─────────────────────────────────────────────────────────────────────────────

@bp_stream.route("/odds/stream/<mode>/<sport>", methods=["GET"])
def stream_odds(mode: str, sport: str):
    """
    Server-Sent Events endpoint.
    Auth: pass ?token=<jwt> (EventSource cannot set Authorization header).
    """
    from app.api import _err

    if mode not in ("upcoming", "live"):
        return _err("mode must be 'upcoming' or 'live'", 400)

    user = _auth_user()
    if not user:
        # Return a valid SSE error event so the client can read it
        def _deny():
            yield _sse_event("error", {"error": "Unauthorized", "code": 401})
        return Response(
            stream_with_context(_deny()),
            mimetype="text/event-stream",
            status=200,   # keep 200 so EventSource doesn't retry endlessly
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    is_live_tier = _tier_rank(user) >= _TIER_RANK["pro"]
    generator    = _make_generator(mode, sport, user, is_live_tier)

    return Response(
        stream_with_context(generator()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
            "Access-Control-Allow-Origin":  "*",
            "Access-Control-Allow-Headers": "Authorization,Content-Type",
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# REST snapshot  –  /odds/snapshot/<mode>/<sport>
# ─────────────────────────────────────────────────────────────────────────────

@bp_stream.route("/odds/snapshot/<mode>/<sport>", methods=["GET"])
@require_tier("basic")
def snapshot_odds(mode: str, sport: str):
    """REST alternative for clients that prefer polling over SSE."""
    from app.api import _signed_response
    tier    = getattr(g.user, "tier", "basic") or "basic"
    matches = _enrich_matches(_get_unified(mode, sport), tier)
    return _signed_response({
        "matches": matches,
        "sport":   sport,
        "mode":    mode,
        "count":   len(matches),
    })


# ─────────────────────────────────────────────────────────────────────────────
# Monitor – /api/monitor/competitions  &  /api/monitor/stats
# ─────────────────────────────────────────────────────────────────────────────

@bp_monitor.route("/competitions", methods=["GET"])
def monitor_competitions():
    """
    Extract unique competition names from the Redis unified snapshot.
    Called by the dashboard filter bar.  No auth required (metadata only).
    """
    from app.api import _signed_response
    sport = request.args.get("sport", "soccer")
    mode  = request.args.get("mode",  "upcoming")

    matches = _get_unified(mode, sport)
    comps   = sorted({
        str(m.get("competition_name") or m.get("competition") or "").strip()
        for m in matches
        if m.get("competition_name") or m.get("competition")
    })
    return _signed_response({"competitions": comps, "sport": sport, "mode": mode})


@bp_monitor.route("/stats", methods=["GET"])
def monitor_stats():
    """Per-sport match count and bookmaker coverage from Redis."""
    from app.api import _signed_response
    r = _r()

    stats = {}
    for sport in ALL_SPORTS:
        for mode in ("upcoming", "live"):
            raw = r.get(f"odds:unified:{mode}:{sport}")
            if not raw:
                continue
            try:
                data    = json.loads(raw)
                matches = data.get("matches", []) if isinstance(data, dict) else data
                bk_seen: set[str] = set()
                for m in matches:
                    bk_seen.update((m.get("bookmakers") or {}).keys())

                stats.setdefault(sport, {})[mode] = {
                    "count":    len(matches),
                    "bks":      sorted(bk_seen),
                    "bk_count": len(bk_seen),
                }
            except Exception:
                pass

    return _signed_response({"stats": stats})