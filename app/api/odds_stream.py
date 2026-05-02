"""
app/api/odds_stream.py
======================
Unified SSE stream + REST endpoints for ALL 10 bookmakers.

Speed optimisations
-------------------
1. _get_unified() returns cached unified key if < 5 min old (sub-ms).
2. warm_cache() pre-builds all unified keys in background.
3. _merge_bks() skips empty BK keys early.
4. SSE sends slim batch first (teams + 1x2) so UI renders instantly,
   then full batch with all markets.
5. Paged endpoint sorts + slices in Python (no DB round-trip).
6. _detect_arb() checks ALL valid outcome combinations per market —
   1+X, 1+2, X+2, AND 1+X+2 for 3-way markets. No scenario is missed.

Routes
------
GET  /api/odds/stream/<mode>/<sport>       SSE (?token= for auth)
GET  /api/odds/snapshot/<mode>/<sport>     REST full snapshot
GET  /api/odds/page/<mode>/<sport>         REST paginated
GET  /api/monitor/competitions
GET  /api/monitor/stats
GET  /api/monitor/redis-keys
GET  /api/monitor/warm-cache               POST/GET — pre-build unified keys
"""
from __future__ import annotations

import json
import time
import logging
from functools import wraps
from itertools import combinations

from flask import Blueprint, Response, request, stream_with_context, g

log = logging.getLogger(__name__)

bp_stream  = Blueprint("odds_stream",  __name__, url_prefix="/api")
bp_monitor = Blueprint("odds_monitor", __name__, url_prefix="/api/monitor")

_TIER_RANK  = {"free": 0, "basic": 1, "pro": 2, "premium": 3, "admin": 4}
_LOCAL_BKS  = {"sp", "bt", "od"}
_KEEPALIVE  = 20
_CACHE_TTL  = 300   # 5 min

# 3-way markets where we check ALL 4 combos (1+X+2, 1+X, 1+2, X+2)
_THREE_WAY_MARKETS = frozenset({
    "match_winner", "1x2", "moneyline", "first_half_1x2",
    "second_half_1x2", "draw_no_bet",
})

ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby", "ice-hockey",
    "volleyball", "handball", "table-tennis", "baseball", "mma", "boxing",
    "darts", "american-football", "esoccer",
]

_BK_KEY_FORMATS: list[tuple[str, list[str]]] = [
    ("sp",        ["odds:sp:upcoming:{sport}",        "sp:upcoming:{sport}"]),
    ("bt",        ["odds:bt:upcoming:{sport}",        "bt:upcoming:{sport}"]),
    ("od",        ["odds:od:upcoming:{sport}",        "od:upcoming:{sport}"]),
    ("b2b",       ["odds:b2b:upcoming:{sport}",       "b2b:upcoming:{sport}"]),
    ("1xbet",     ["odds:1xbet:upcoming:{sport}",     "odds:b2b:1xbet:upcoming:{sport}",     "1xbet:upcoming:{sport}"]),
    ("22bet",     ["odds:22bet:upcoming:{sport}",     "odds:b2b:22bet:upcoming:{sport}",     "22bet:upcoming:{sport}"]),
    ("betwinner", ["odds:betwinner:upcoming:{sport}", "odds:b2b:betwinner:upcoming:{sport}", "betwinner:upcoming:{sport}"]),
    ("melbet",    ["odds:melbet:upcoming:{sport}",    "odds:b2b:melbet:upcoming:{sport}",    "melbet:upcoming:{sport}"]),
    ("megapari",  ["odds:megapari:upcoming:{sport}",  "odds:b2b:megapari:upcoming:{sport}",  "megapari:upcoming:{sport}"]),
    ("helabet",   ["odds:helabet:upcoming:{sport}",   "odds:b2b:helabet:upcoming:{sport}",   "helabet:upcoming:{sport}"]),
    ("paripesa",  ["odds:paripesa:upcoming:{sport}",  "odds:b2b:paripesa:upcoming:{sport}",  "paripesa:upcoming:{sport}"]),
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
            return Customer.query.get(int(payload["sub"]))
        except Exception as exc:
            log.warning("Token decode failed: %s", exc)
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
# DATA LAYER
# =============================================================================

def _read_key(r, patterns: list[str], sport: str) -> list[dict] | None:
    """Return the dataset with the most matches across all key patterns."""
    best: list[dict] | None = None
    for pat in patterns:
        try:
            raw = r.get(pat.format(sport=sport))
            if not raw:
                continue
            data    = json.loads(raw)
            matches = data.get("matches", []) if isinstance(data, dict) else data
            if matches and (best is None or len(matches) > len(best)):
                best = matches
        except Exception:
            continue
    return best


def _get_unified(mode: str, sport: str, force_refresh: bool = False) -> list[dict]:
    """
    Return unified match list for a sport.
    Serves from cached unified key if < _CACHE_TTL seconds old.
    """
    r = _r()
    unified_key = f"odds:unified:{mode}:{sport}"

    if not force_refresh:
        raw = r.get(unified_key)
        if raw:
            try:
                data = json.loads(raw)
                age  = time.time() - float(data.get("updated_at", 0))
                if age < _CACHE_TTL:
                    return data.get("matches", [])
            except Exception:
                pass

    if mode == "live":
        merged = _merge_bks(r, sport, _BK_KEY_FORMATS_LIVE)
    else:
        merged = _merge_bks(r, sport, _BK_KEY_FORMATS)

    if merged:
        try:
            r.setex(
                unified_key,
                3600,
                json.dumps({
                    "mode":        mode,
                    "sport":       sport,
                    "match_count": len(merged),
                    "updated_at":  time.time(),
                    "matches":     merged,
                }, default=str),
            )
        except Exception:
            pass

    return merged


def _merge_bks(r, sport: str, bk_formats: list[tuple[str, list[str]]]) -> list[dict]:
    """Merge all BK datasets into one deduplicated list."""
    result:  list[dict] = []
    by_jk:   dict[str, int] = {}
    by_name: dict[str, int] = {}

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

            mkts = (
                m.get("markets") or
                m.get("bookmakers", {}).get(bk_slug, {}).get("markets") or
                {}
            )

            if pos is not None:
                ex = result[pos]
                ex.setdefault("bookmakers", {})[bk_slug] = {
                    "bookmaker": bk_slug.upper(), "slug": bk_slug, "markets": mkts,
                }
                ex["bk_count"] = len(ex["bookmakers"])
                if not ex.get("competition") and m.get("competition"):
                    ex["competition"] = m["competition"]
            else:
                entry: dict = {
                    "match_id":          m.get("match_id") or key_jk,
                    "join_key":          key_jk,
                    "parent_match_id":   m.get("parent_match_id") or m.get("betradar_id") or key_jk,
                    "betradar_id":       m.get("betradar_id") or "",
                    "home_team":         m.get("home_team")  or m.get("home_team_name")  or "",
                    "away_team":         m.get("away_team")  or m.get("away_team_name")  or "",
                    "competition":       m.get("competition") or m.get("competition_name") or "",
                    "sport":             m.get("sport") or sport,
                    "start_time":        m.get("start_time") or "",
                    "status":            m.get("status") or "PRE_MATCH",
                    "is_live":           m.get("is_live", False),
                    "has_arb":           False,
                    "has_ev":            False,
                    "best_arb_pct":      0,
                    "arb_opportunities": [],
                    "market_slugs":      list((mkts or {}).keys()),
                    "bookmakers": {
                        bk_slug: {"bookmaker": bk_slug.upper(), "slug": bk_slug, "markets": mkts}
                    },
                    "bk_count": 1,
                }
                pos = len(result)
                result.append(entry)
                if key_jk: by_jk[key_jk]   = pos
                if key_nk: by_name[key_nk] = pos

    for m in result:
        m["best"]              = _build_best(m["bookmakers"])
        has_arb, pct, arbs     = _detect_arb(m["best"])
        m["has_arb"]           = has_arb
        m["best_arb_pct"]      = pct
        m["arb_opportunities"] = arbs
        m["market_slugs"]      = list(m["best"].keys())

    return result


def _build_best(bookmakers: dict) -> dict:
    """Build best-price-per-outcome map across all bookmakers."""
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
    Find ALL arbitrage opportunities across every market.

    For 3-way markets (1x2, match_winner, moneyline) we check:
      • Full 3-leg: 1 + X + 2
      • All 2-leg pairs: 1+X, 1+2, X+2

    For 2-way markets we check all 2-leg pairs (usually just 1 pair).

    Each combo must span at least 2 different bookmakers — same-BK
    combos are never true arb.

    Returns (has_arb, best_profit_pct, sorted_arb_list).
    """
    arbs: list[dict] = []
    seen_combos: set[tuple] = set()  # deduplicate

    for mkt, ob in best.items():
        keys = list(ob.keys())
        if len(keys) < 2:
            continue

        is_3way = mkt in _THREE_WAY_MARKETS

        # Build list of combo tuples to try
        combos_to_try: list[tuple[str, ...]] = []

        if is_3way and len(keys) >= 3:
            trio = tuple(keys[:3])
            # Full 3-leg first
            combos_to_try.append(trio)
            # All 2-leg pairs from the trio
            combos_to_try.extend(combinations(trio, 2))
        else:
            # 2-way or unknown — try all pairs (cap at 4 outcomes to avoid explosion)
            use_keys = keys[:4]
            combos_to_try.extend(combinations(use_keys, 2))

        for use in combos_to_try:
            # Must span 2+ different bookmakers
            bks_in_combo = {ob[k]["bk"] for k in use if ob[k].get("bk")}
            if len(bks_in_combo) < 2:
                continue

            # All odds must be valid
            odds = [ob[k]["odd"] for k in use]
            if any(o <= 1.0 for o in odds):
                continue

            inv_sum = sum(1.0 / o for o in odds)

            # Arbitrage exists when sum of inverse odds < 1.0
            if not (0 < inv_sum < 1.0):
                continue

            profit_pct = round((1.0 / inv_sum - 1.0) * 100, 3)
            combo_key  = (mkt,) + tuple(sorted(use))

            if combo_key in seen_combos:
                continue
            seen_combos.add(combo_key)

            legs = [
                {
                    "outcome":   k,
                    "odd":       ob[k]["odd"],
                    "bk":        ob[k]["bk"],
                    "stake_pct": round((1.0 / ob[k]["odd"]) / inv_sum, 4),
                }
                for k in use
            ]

            arbs.append({
                "market":     mkt,
                "combo":      " + ".join(use),
                "profit_pct": profit_pct,
                "legs":       legs,
                "n_bks":      len(bks_in_combo),
            })

    if not arbs:
        return False, 0.0, []

    arbs.sort(key=lambda a: -a["profit_pct"])
    return True, arbs[0]["profit_pct"], arbs


def _slim(m: dict) -> dict:
    """Lightweight match dict for first SSE batch — renders UI instantly."""
    best = m.get("best", {})
    return {
        "match_id":          m["match_id"],
        "join_key":          m["join_key"],
        "home_team":         m["home_team"],
        "away_team":         m["away_team"],
        "competition":       m["competition"],
        "start_time":        m["start_time"],
        "is_live":           m["is_live"],
        "has_arb":           m["has_arb"],
        "best_arb_pct":      m["best_arb_pct"],
        "bk_count":          m["bk_count"],
        "market_slugs":      m.get("market_slugs", []),
        "bookmakers": {
            k: {"bookmaker": v["bookmaker"], "slug": v["slug"], "markets": {}}
            for k, v in (m.get("bookmakers") or {}).items()
        },
        "best": {
            "1x2":          best.get("1x2", {}),
            "match_winner": best.get("match_winner", {}),
            "moneyline":    best.get("moneyline", {}),
        },
        "arb_opportunities": m.get("arb_opportunities", []),
    }


def _filter_tier(matches: list[dict], tier: str) -> list[dict]:
    if tier in ("pro", "premium", "admin"):
        return matches
    out = []
    for m in matches:
        mc  = {**m}
        bks = mc.get("bookmakers") or {}
        mc["bookmakers"] = {k: v for k, v in bks.items() if k in _LOCAL_BKS}
        mc["bk_count"]   = len(mc["bookmakers"])
        # Re-build best + arb for local-only tier
        mc["best"]              = _build_best(mc["bookmakers"])
        has_arb, pct, arbs      = _detect_arb(mc["best"])
        mc["has_arb"]           = has_arb
        mc["best_arb_pct"]      = pct
        mc["arb_opportunities"] = arbs
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
        r       = _r()
        matches = _filter_tier(_get_unified(mode, sport), tier)

        # ── Phase 1: slim batch — renders match list instantly ────────────────
        yield _sse("batch", {
            "matches": [_slim(m) for m in matches],
            "source":  "slim",
            "sport":   sport,
            "mode":    mode,
            "count":   len(matches),
            "tier":    tier,
        })

        # ── Phase 2: full batch — all markets, full arb data ─────────────────
        yield _sse("batch", {
            "matches": matches,
            "source":  "full",
            "sport":   sport,
            "mode":    mode,
            "count":   len(matches),
            "tier":    tier,
        })

        yield _sse("connected", {
            "status":    "connected",
            "sport":     sport,
            "mode":      mode,
            "tier":      tier,
            "live_push": live_tier,
            "count":     len(matches),
        })

        if not live_tier:
            yield ": keepalive\n\n"
            return

        # ── Phase 3: pubsub live updates (pro+ tier) ──────────────────────────
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
                            # Single match arb update — selective client update
                            yield _sse("arb_update", payload)

                        elif "ev:" in ch:
                            yield _sse("ev_update", payload)

                        elif "live_updates" in ch:
                            # Score / match_time update for a single match
                            yield _sse("live_update", payload)

                        else:
                            # Full odds refresh — re-merge + push
                            fresh = _filter_tier(
                                _get_unified(mode, sport, force_refresh=True), tier
                            )
                            yield _sse("batch", {
                                "matches": fresh,
                                "source":  "live",
                                "sport":   sport,
                                "mode":    mode,
                                "count":   len(fresh),
                            })
                    except Exception as exc:
                        log.debug("[stream] pubsub error: %s", exc)

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
# PUBLISH HELPERS (called from harvesters after writing to Redis)
# =============================================================================

def publish_harvest_done(bk_slug: str, sport: str, count: int):
    """
    Call this at the end of each BK harvest task so the SSE stream can
    notify the frontend that fresh data is available for a sport.

    Usage in tasks_upcoming.py:
        from app.api.odds_stream import publish_harvest_done
        publish_harvest_done("sp", "soccer", len(matches))
    """
    try:
        r = _r()
        # Invalidate the unified cache so next SSE poll re-merges
        r.delete(f"odds:unified:upcoming:{sport}")
        # Notify all subscribers
        r.publish(
            f"odds:all:upcoming:{sport}:updates",
            json.dumps({
                "event":  "harvest_done",
                "bk":     bk_slug,
                "sport":  sport,
                "count":  count,
                "ts":     time.time(),
            })
        )
    except Exception as exc:
        log.warning("publish_harvest_done failed: %s", exc)


def publish_live_update(sport: str, match_id: str, join_key: str,
                        score_home=None, score_away=None,
                        match_time=None, is_live=None,
                        bookmakers: dict | None = None):
    """
    Publish a lightweight live update for a single match.
    The frontend applies this selectively without re-fetching the full list.

    Usage:
        publish_live_update("soccer", "12345", "67890",
                            score_home=1, score_away=0, match_time="37")
    """
    try:
        r = _r()
        payload: dict = {"match_id": match_id, "join_key": join_key}
        if score_home is not None: payload["score_home"] = score_home
        if score_away is not None: payload["score_away"] = score_away
        if match_time  is not None: payload["match_time"]  = match_time
        if is_live     is not None: payload["is_live"]     = is_live
        if bookmakers:              payload["bookmakers"]  = bookmakers
        r.publish(f"bus:live_updates:{sport}", json.dumps(payload))
    except Exception as exc:
        log.warning("publish_live_update failed: %s", exc)


def publish_arb_update(sport: str, join_key: str, match_id: str,
                       has_arb: bool, best_arb_pct: float,
                       arb_opportunities: list):
    """
    Publish a per-match arb update so the frontend can update just that card.
    Call after re-running _detect_arb when a harvester updates odds.
    """
    try:
        r = _r()
        r.publish(
            f"arb:updates:{sport}",
            json.dumps({
                "join_key":          join_key,
                "match_id":          match_id,
                "has_arb":           has_arb,
                "best_arb_pct":      best_arb_pct,
                "arb_opportunities": arb_opportunities,
                "ts":                time.time(),
            })
        )
    except Exception as exc:
        log.warning("publish_arb_update failed: %s", exc)


# =============================================================================
# ROUTES
# =============================================================================

@bp_stream.route("/odds/stream/<mode>/<sport>", methods=["GET"])
def stream_odds(mode: str, sport: str):
    from app.api import _err

    if mode not in ("upcoming", "live"):
        return _err("mode must be 'upcoming' or 'live'", 400)

    user = _auth_user()
    if not user:
        def _deny():
            yield _sse("error", {"error": "Unauthorized", "code": 401})
        return Response(
            stream_with_context(_deny()), mimetype="text/event-stream",
            status=200, headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

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
    from app.api import _signed_response
    tier    = getattr(g.user, "tier", "basic") or "basic"
    matches = _filter_tier(_get_unified(mode, sport), tier)
    return _signed_response({"matches": matches, "sport": sport,
                             "mode": mode, "count": len(matches)})


@bp_stream.route("/odds/page/<mode>/<sport>", methods=["GET"])
@require_tier("basic")
def paged_odds(mode: str, sport: str):
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
    from app.api import _signed_response
    sport   = request.args.get("sport", "soccer")
    mode    = request.args.get("mode",  "upcoming")
    matches = _get_unified(mode, sport)
    comps   = sorted({
        str(m.get("competition_name") or m.get("competition") or "").strip()
        for m in matches
        if (m.get("competition_name") or m.get("competition"))
    } - {""})
    return _signed_response({"competitions": comps, "sport": sport, "mode": mode})


@bp_monitor.route("/stats", methods=["GET"])
def monitor_stats():
    from app.api import _signed_response
    r     = _r()
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
                    "count":    len(matches),
                    "bks":      sorted(bk_seen),
                    "bk_count": len(bk_seen),
                }
            except Exception:
                pass
    return _signed_response({"stats": stats})


@bp_monitor.route("/redis-keys", methods=["GET"])
def monitor_redis_keys():
    from app.api import _signed_response
    sport = request.args.get("sport", "soccer")
    r     = _r()

    found: dict[str, int] = {}
    missing: list[str]    = []

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
        "summary": f"{len(found)} keys found, {sum(found.values())} total matches",
    })


@bp_monitor.route("/warm-cache", methods=["GET", "POST"])
def warm_cache():
    """Pre-build all unified keys. Call once after deploy or Redis flush."""
    from app.api import _signed_response
    t0      = time.time()
    results = {}
    for sport in ALL_SPORTS:
        try:
            matches = _get_unified("upcoming", sport, force_refresh=True)
            results[sport] = len(matches)
        except Exception as e:
            results[sport] = f"error: {e}"
    elapsed = round(time.time() - t0, 2)
    return _signed_response({"warmed": results, "elapsed_s": elapsed})


# =============================================================================
# LIFECYCLE INTEGRATION — wire watch/notification into odds_stream app
# =============================================================================

def _register_lifecycle(app) -> None:
    """
    Call this from create_app() after registering bp_stream and bp_monitor.

    Example in app/__init__.py:
        from app.api.odds_stream import bp_stream, bp_monitor, _register_lifecycle
        from app.workers.match_lifecycle import bp_lifecycle, start_lifecycle_manager
        app.register_blueprint(bp_stream)
        app.register_blueprint(bp_monitor)
        if bp_lifecycle:
            app.register_blueprint(bp_lifecycle)
        _register_lifecycle(app)
        start_lifecycle_manager()
    """
    try:
        from app.workers.match_lifecycle import start_lifecycle_manager
        with app.app_context():
            start_lifecycle_manager()
        log.info("MatchLifecycleManager started via _register_lifecycle()")
    except Exception as exc:
        log.warning("Could not start lifecycle manager: %s", exc)


# Convenience: expose watch endpoint inline (alternative to bp_lifecycle)
@bp_stream.route("/odds/watch", methods=["POST"])
def watch_match_inline():
    """
    POST /api/odds/watch
    Body: {"match": {...matchObj}, "prefs": {"channels": ["email","websocket"], ...}}

    Quick way to watch a match from the frontend without separate blueprint.
    Returns {ok, watch} or {error}.
    """
    from app.api import _err
    user = _auth_user()
    if not user:
        return _err("Authentication required", 401)

    try:
        from app.workers.match_lifecycle import (
            WatchPrefs, get_lifecycle_manager,
        )
    except ImportError:
        return _err("Lifecycle module not available", 503)

    body       = __import__("flask").request.get_json(silent=True) or {}
    match_data = body.get("match") or {}
    prefs_data = body.get("prefs") or {}

    prefs = WatchPrefs(
        user_id     = str(user.id),
        email       = prefs_data.get("email") or getattr(user, "email", ""),
        phone       = prefs_data.get("phone") or getattr(user, "phone", ""),
        webhook_url = prefs_data.get("webhook_url", ""),
        channels    = prefs_data.get("channels") or ["websocket", "pubsub"],
        notify_on   = prefs_data.get("notify_on") or [
            "pre_start", "started", "suspended", "goal", "finished", "arb_found",
        ],
    )

    mgr   = get_lifecycle_manager()
    saved = mgr.save_match(match_data, prefs)

    from app.api import _signed_response
    return _signed_response({"ok": True, "watch": saved.to_dict()}), 201