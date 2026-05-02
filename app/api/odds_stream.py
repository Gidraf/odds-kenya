"""
app/api/odds_stream.py
======================
Unified SSE stream + REST endpoints for ALL 10 bookmakers.

KEY FIX: _get_unified no longer short-circuits on the unified snapshot key.
That key gets written after page-1 merge (50-100 matches). If we returned
it early, clients would see 50 matches even though SP has 1476.
We always read ALL individual BK keys and merge them fresh.

Routes
------
GET /odds/stream/<mode>/<sport>      SSE  (?token= for auth — EventSource limitation)
GET /odds/snapshot/<mode>/<sport>    REST full snapshot
GET /odds/page/<mode>/<sport>        REST paginated  (?page=1&per_page=100)
GET /api/monitor/competitions        Competition list (no auth needed)
GET /api/monitor/stats               Per-sport counts
GET /api/monitor/redis-keys          Debug: which keys exist for a sport
"""
from __future__ import annotations

import json
import time
import logging
from functools import wraps

from flask import Blueprint, Response, request, stream_with_context, g

log = logging.getLogger(__name__)

bp_stream  = Blueprint("odds_stream",  __name__,  url_prefix="/api")
bp_monitor = Blueprint("odds_monitor", __name__, url_prefix="/api/monitor")

_TIER_RANK = {"free": 0, "basic": 1, "pro": 2, "premium": 3, "admin": 4}
_LOCAL_BKS = {"sp", "bt", "od"}
_KEEPALIVE  = 20

ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby", "ice-hockey",
    "volleyball", "handball", "table-tennis", "baseball", "mma", "boxing",
    "darts", "american-football", "esoccer",
]

# Every key pattern any harvester might write to — checked in order
_BK_KEY_FORMATS: list[tuple[str, list[str]]] = [
    ("sp",        ["odds:sp:upcoming:{sport}", "sp:upcoming:{sport}"]),
    ("bt",        ["odds:bt:upcoming:{sport}", "bt:upcoming:{sport}"]),
    ("od",        ["odds:od:upcoming:{sport}", "od:upcoming:{sport}"]),
    ("b2b",       ["odds:b2b:upcoming:{sport}", "b2b:upcoming:{sport}"]),
    ("1xbet",     ["odds:1xbet:upcoming:{sport}",     "1xbet:upcoming:{sport}"]),
    ("22bet",     ["odds:22bet:upcoming:{sport}",     "22bet:upcoming:{sport}"]),
    ("betwinner", ["odds:betwinner:upcoming:{sport}", "betwinner:upcoming:{sport}"]),
    ("melbet",    ["odds:melbet:upcoming:{sport}",    "melbet:upcoming:{sport}"]),
    ("megapari",  ["odds:megapari:upcoming:{sport}",  "megapari:upcoming:{sport}"]),
    ("helabet",   ["odds:helabet:upcoming:{sport}",   "helabet:upcoming:{sport}"]),
    ("paripesa",  ["odds:paripesa:upcoming:{sport}",  "paripesa:upcoming:{sport}"]),
]

_BK_KEY_FORMATS_LIVE: list[tuple[str, list[str]]] = [
    ("sp", ["odds:sp:live:{sport}", "sp:live:{sport}"]),
    ("bt", ["odds:bt:live:{sport}", "bt:live:{sport}"]),
    ("od", ["odds:od:live:{sport}", "od:live:{sport}"]),
]


# =============================================================================
# AUTH
# =============================================================================

def _auth_user():
    from app.utils.customer_jwt_helpers import _decode_token
    from app.models.customer import Customer

    auth  = request.headers.get("Authorization", "")
    token = auth[7:] if auth.startswith("Bearer ") else None
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

    api_key = request.headers.get("X-Api-Key", "").strip()
    if api_key:
        try:
            from app.models.api_key import ApiKey
            from app.models.customer import Customer as C
            ak   = ApiKey.query.filter_by(key=api_key, is_active=True).first()
            user = ak and C.query.get(ak.user_id)
            return user if (user and user.is_active) else None
        except Exception:
            pass

    return None


def _tier_rank(user) -> int:
    if not user:
        return -1
    return _TIER_RANK.get(getattr(user, "tier", "basic") or "basic", 1)


def require_tier(min_tier: str):
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


# =============================================================================
# REDIS
# =============================================================================

def _r():
    from app.workers.celery_tasks import _redis
    return _redis()


# =============================================================================
# DATA LAYER — the critical fix lives here
# =============================================================================

def _read_key(r, patterns: list[str], sport: str) -> list[dict] | None:
    best: list[dict] | None = None
    for pat in patterns:
        try:
            raw = r.get(pat.format(sport=sport))
            if not raw:
                continue
            data    = json.loads(raw)
            matches = data.get("matches", []) if isinstance(data, dict) else data
            if matches and (best is None or len(matches) > len(best)):
                best = matches   # ← keeps the largest dataset found
        except Exception:
            continue
    return best


def _get_unified(mode: str, sport: str) -> list[dict]:
    """
    THE KEY FIX:
    We NEVER short-circuit on odds:unified:upcoming:{sport}.
    That key is written after every publish_snapshot call — including
    after the very first page (50-100 matches from the paged harvest).
    Returning early would mean clients always see only 50 matches.

    Instead we always read every individual BK key and merge them.
    SP has all 1476 soccer matches in sp:upcoming:soccer.
    BT has its matches in bt:upcoming:soccer.
    OD has its matches in od:upcoming:soccer.
    We merge all three (and any B2B keys) into one list.
    Then we write the merged result back as the unified key so the
    next pubsub-triggered refresh is faster.
    """
    r = _r()

    if mode == "live":
        return _merge_bks(r, sport, _BK_KEY_FORMATS_LIVE)

    merged = _merge_bks(r, sport, _BK_KEY_FORMATS)

    # Write merged result back so pubsub refreshes are fast
    if merged:
        try:
            r.setex(
                f"odds:unified:{mode}:{sport}",
                3600,
                json.dumps({
                    "mode": mode, "sport": sport,
                    "match_count": len(merged),
                    "updated_at":  time.time(),
                    "matches":     merged,
                }, default=str),
            )
        except Exception:
            pass

    return merged


def _merge_bks(r, sport: str, bk_formats: list[tuple[str, list[str]]]) -> list[dict]:
    """
    Read every BK's Redis key for this sport and merge into one deduplicated list.
    Dedup priority: betradar_id > home|away team name.
    """
    result:  list[dict] = []
    by_jk:   dict[str, int] = {}   # join_key → index
    by_name: dict[str, int] = {}   # home|away → index

    def jk(m: dict) -> str:
        return str(
            m.get("join_key") or m.get("parent_match_id") or
            m.get("betradar_id") or m.get("match_id") or ""
        )

    def nk(m: dict) -> str:
        h = (m.get("home_team") or m.get("home_team_name") or "")[:14].lower().strip()
        a = (m.get("away_team") or m.get("away_team_name") or "")[:14].lower().strip()
        return f"{h}|{a}" if h and a else ""

    for bk_slug, patterns in bk_formats:
        matches = _read_key(r, patterns, sport)
        if not matches:
            continue

        for m in matches:
            key_jk = jk(m)
            key_nk = nk(m)

            pos = by_jk.get(key_jk) if key_jk else None
            if pos is None and key_nk:
                pos = by_name.get(key_nk)

            # Extract markets for this BK
            mkts = (
                m.get("markets") or
                m.get("bookmakers", {}).get(bk_slug, {}).get("markets") or
                {}
            )

            if pos is not None:
                # Entry already exists — merge this BK's markets in
                ex = result[pos]
                ex.setdefault("bookmakers", {})[bk_slug] = {
                    "bookmaker": bk_slug.upper(), "slug": bk_slug, "markets": mkts,
                }
                ex["bk_count"] = len(ex["bookmakers"])
                if not ex.get("competition") and m.get("competition"):
                    ex["competition"] = m["competition"]
            else:
                # New match
                entry: dict = {
                    "match_id":         m.get("match_id") or key_jk,
                    "join_key":         key_jk,
                    "parent_match_id":  m.get("parent_match_id") or m.get("betradar_id") or key_jk,
                    "betradar_id":      m.get("betradar_id") or "",
                    "home_team":        m.get("home_team")  or m.get("home_team_name")  or "",
                    "away_team":        m.get("away_team")  or m.get("away_team_name")  or "",
                    "competition":      m.get("competition") or m.get("competition_name") or "",
                    "sport":            m.get("sport") or sport,
                    "start_time":       m.get("start_time") or "",
                    "status":           m.get("status") or "PRE_MATCH",
                    "is_live":          m.get("is_live", False),
                    "has_arb":          False,
                    "has_ev":           False,
                    "best_arb_pct":     0,
                    "arb_opportunities": [],
                    "market_slugs":     list((mkts or {}).keys()),
                    "bookmakers": {
                        bk_slug: {"bookmaker": bk_slug.upper(), "slug": bk_slug, "markets": mkts}
                    },
                    "bk_count": 1,
                }
                pos = len(result)
                result.append(entry)
                if key_jk:  by_jk[key_jk]   = pos
                if key_nk:  by_name[key_nk] = pos

    # Compute best odds + arb after all BKs are merged
    for m in result:
        m["best"]           = _build_best(m["bookmakers"])
        has_arb, pct, arbs  = _detect_arb(m["best"])
        m["has_arb"]        = has_arb
        m["best_arb_pct"]   = pct
        m["arb_opportunities"] = arbs
        m["market_slugs"]   = list(m["best"].keys())

    return result


def _build_best(bookmakers: dict) -> dict:
    """Best price per market/outcome across all bookmakers."""
    best: dict = {}
    for bk_slug, bd in bookmakers.items():
        for mkt, outcomes in (bd.get("markets") or {}).items():
            if not isinstance(outcomes, dict):
                continue
            best.setdefault(mkt, {})
            for outcome, p in outcomes.items():
                try:
                    price = float(
                        p.get("price") or p.get("odd") or p.get("odds") or 0
                        if isinstance(p, dict) else p or 0
                    )
                except (TypeError, ValueError):
                    price = 0.0
                if price > 1.0:
                    existing = best[mkt].get(outcome)
                    if not existing or price > existing.get("odd", 0):
                        best[mkt][outcome] = {"odd": price, "bk": bk_slug}
    return best


def _detect_arb(best: dict) -> tuple[bool, float, list]:
    """
    Real arb only — legs must span 2+ different bookmakers.
    Single-BK arb is impossible; never flag it.
    """
    arbs = []
    for mkt, ob in best.items():
        keys = list(ob.keys())
        exp  = 3 if mkt in ("match_winner", "1x2", "moneyline") else 2
        if len(keys) < exp:
            continue
        use  = keys[:exp]
        bks  = {ob[k]["bk"] for k in use if ob[k].get("bk")}
        if len(bks) < 2:
            continue
        odds = [ob[k]["odd"] for k in use]
        if any(o <= 1 for o in odds):
            continue
        s = sum(1 / o for o in odds)
        if 0 < s < 1.0:
            profit = round((1 / s - 1) * 100, 3)
            legs   = [
                {"outcome": k, "odd": ob[k]["odd"], "bk": ob[k]["bk"],
                 "stake_pct": round((1 / ob[k]["odd"]) / s, 4)}
                for k in use
            ]
            arbs.append({"market": mkt, "profit_pct": profit, "legs": legs})

    if not arbs:
        return False, 0.0, []
    arbs.sort(key=lambda a: -a["profit_pct"])
    return True, arbs[0]["profit_pct"], arbs


def _filter_tier(matches: list[dict], tier: str) -> list[dict]:
    """Basic tier sees SP + BT + OD only. Pro/premium see all 10."""
    if tier in ("pro", "premium", "admin"):
        return matches
    out = []
    for m in matches:
        mc  = {**m}
        bks = mc.get("bookmakers") or {}
        mc["bookmakers"] = {k: v for k, v in bks.items() if k in _LOCAL_BKS}
        mc["bk_count"]   = len(mc["bookmakers"])
        out.append(mc)
    return out


# =============================================================================
# SSE GENERATOR
# =============================================================================

def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _make_generator(mode: str, sport: str, user, live_tier: bool):
    tier = getattr(user, "tier", "basic") or "basic"

    def generate():
        r = _r()

        # Send full snapshot immediately on connect
        matches = _filter_tier(_get_unified(mode, sport), tier)
        yield _sse("batch", {
            "matches": matches, "source": "snapshot",
            "sport":   sport,   "mode":   mode,
            "count":   len(matches), "tier": tier,
        })
        yield _sse("connected", {
            "status":    "connected", "sport":     sport,
            "mode":      mode,        "tier":      tier,
            "live_push": live_tier,   "count":     len(matches),
        })

        if not live_tier:
            yield ": keepalive\n\n"
            return

        # Pro/premium: subscribe to Redis pubsub for live pushes
        pubsub   = r.pubsub(ignore_subscribe_messages=True)
        channels = [
            f"odds:all:{mode}:{sport}:updates",
            f"arb:updates:{sport}",
            f"ev:updates:{sport}",
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
                        ch = msg.get("channel") or b""
                        if isinstance(ch, bytes):
                            ch = ch.decode()

                        if "arb:" in ch:
                            yield _sse("arb_update", payload)
                        elif "ev:" in ch:
                            yield _sse("ev_update", payload)
                        elif "live_updates" in ch:
                            yield _sse("live_update", payload)
                        else:
                            # Harvest completed — send fresh full snapshot
                            fresh = _filter_tier(_get_unified(mode, sport), tier)
                            yield _sse("batch", {
                                "matches": fresh, "source": "live",
                                "sport":   sport, "mode":   mode,
                                "count":   len(fresh),
                            })
                    except Exception as exc:
                        log.debug("[stream] pubsub: %s", exc)

                if time.time() - last_ka > _KEEPALIVE:
                    yield ": keepalive\n\n"
                    last_ka = time.time()
        finally:
            try:
                pubsub.unsubscribe(*channels)
                pubsub.close()
            except Exception:
                pass

    return generate


# =============================================================================
# ROUTES
# =============================================================================

@bp_stream.route("/odds/stream/<mode>/<sport>", methods=["GET"])
def stream_odds(mode: str, sport: str):
    """SSE — pass JWT via ?token= (EventSource can't set headers)."""
    from app.api import _err

    if mode not in ("upcoming", "live"):
        return _err("mode must be 'upcoming' or 'live'", 400)

    # user = _auth_user()
    # if not user:
    #     def _deny():
    #         yield _sse("error", {"error": "Unauthorized", "code": 401})
    #     return Response(stream_with_context(_deny()), mimetype="text/event-stream",
    #                     status=200, headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    live_tier = _tier_rank(user) >= _TIER_RANK["pro"]
    return Response(
        stream_with_context(_make_generator(mode, sport, user, live_tier)()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":                "no-cache",
            "X-Accel-Buffering":            "no",
            "Connection":                   "keep-alive",
            "Access-Control-Allow-Origin":  "*",
            "Access-Control-Allow-Headers": "Authorization,Content-Type",
        },
    )


@bp_stream.route("/odds/snapshot/<mode>/<sport>", methods=["GET"])
@require_tier("basic")
def snapshot_odds(mode: str, sport: str):
    """Full REST snapshot — all matches for polling clients."""
    from app.api import _signed_response
    tier    = getattr(g.user, "tier", "basic") or "basic"
    matches = _filter_tier(_get_unified(mode, sport), tier)
    return _signed_response({"matches": matches, "sport": sport,
                             "mode": mode, "count": len(matches)})


@bp_stream.route("/odds/page/<mode>/<sport>", methods=["GET"])
@require_tier("basic")
def paged_odds(mode: str, sport: str):
    """
    Paginated REST for 1000+ match datasets.
    Used by dashboard Load More / infinite scroll.

    Query params:
      page      int   1-based (default 1)
      per_page  int   max 200 (default 100)
      sort      str   'start_time' | 'arb'
    """
    from app.api import _signed_response

    tier     = getattr(g.user, "tier", "basic") or "basic"
    page     = max(1, request.args.get("page",     1,   type=int))
    per_page = min(200, request.args.get("per_page", 100, type=int))
    sort_by  = request.args.get("sort", "start_time")

    all_matches = _filter_tier(_get_unified(mode, sport), tier)

    if sort_by == "arb":
        all_matches.sort(key=lambda m: -(m.get("best_arb_pct") or 0))
    else:
        all_matches.sort(key=lambda m: m.get("start_time") or "")

    total     = len(all_matches)
    offset    = (page - 1) * per_page
    page_data = all_matches[offset: offset + per_page]

    return _signed_response({
        "matches":  page_data,
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    -(-total // per_page),
        "has_more": (offset + per_page) < total,
        "sport":    sport,
        "mode":     mode,
    })


# =============================================================================
# MONITOR
# =============================================================================

@bp_monitor.route("/competitions", methods=["GET"])
def monitor_competitions():
    """Competition names from Redis — used by filter bar. No auth required."""
    from app.api import _signed_response
    sport = request.args.get("sport", "soccer")
    mode  = request.args.get("mode",  "upcoming")
    matches = _get_unified(mode, sport)
    comps = sorted({
        str(m.get("competition_name") or m.get("competition") or "").strip()
        for m in matches
        if (m.get("competition_name") or m.get("competition"))
    } - {""})
    return _signed_response({"competitions": comps, "sport": sport, "mode": mode})


@bp_monitor.route("/stats", methods=["GET"])
def monitor_stats():
    """Per-sport match count + BK coverage."""
    from app.api import _signed_response
    r = _r()
    stats: dict = {}
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
                    "count": len(matches), "bks": sorted(bk_seen), "bk_count": len(bk_seen),
                }
            except Exception:
                pass
    return _signed_response({"stats": stats})


@bp_monitor.route("/redis-keys", methods=["GET"])
def monitor_redis_keys():
    """
    Debug: which BK Redis keys exist for a sport and how many matches each has.
    Call this when BT/OD data isn't appearing to diagnose the issue.
    e.g. GET /api/monitor/redis-keys?sport=soccer
    """
    from app.api import _signed_response
    sport = request.args.get("sport", "soccer")
    r     = _r()

    found: dict[str, int]    = {}
    missing: list[str]       = []

    all_patterns = [
        pat.format(sport=sport)
        for _, patterns in _BK_KEY_FORMATS
        for pat in patterns
    ] + [
        f"odds:unified:upcoming:{sport}",
        f"odds:unified:live:{sport}",
    ]

    for key in all_patterns:
        try:
            raw = r.get(key)
            if raw:
                data  = json.loads(raw)
                count = len(data.get("matches", data) if isinstance(data, dict) else data)
                found[key] = count
            else:
                missing.append(key)
        except Exception as exc:
            missing.append(f"{key} (err: {exc})")

    return _signed_response({
        "sport":   sport,
        "found":   found,
        "missing": missing,
        "summary": f"{len(found)} keys found, {sum(found.values())} total matches across all BKs",
    })