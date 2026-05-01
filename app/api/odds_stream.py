"""
app/api/odds_stream.py
======================
Unified SSE stream + REST endpoints for ALL 10 bookmakers.

DATA SOURCES (revised):
  UPCOMING → PostgreSQL (UnifiedMatch + BookmakerMatchOdds)
             Persisted directly by entity_resolver / celery tasks.
             Redis is NOT the source of truth for upcoming any more.

  LIVE     → Redis (unchanged — sp:live:*, odds:*:live:*)
             Written by live harvesters every few seconds.

SSE push for upcoming:
  Harvesters publish to odds:all:upcoming:{sport}:updates after each
  DB commit so connected clients get a fresh DB read promptly.
  Keepalive every 20s while no updates arrive.

Routes
------
GET /odds/stream/<mode>/<sport>      SSE  (?token= for auth)
GET /odds/snapshot/<mode>/<sport>    REST full snapshot
GET /odds/page/<mode>/<sport>        REST paginated (?page=1&per_page=100&sort=start_time|arb)
GET /api/monitor/competitions        Competition list (no auth)
GET /api/monitor/stats               Per-sport counts
GET /api/monitor/redis-keys          Debug Redis keys for live sport
"""
from __future__ import annotations

import json
import time
import logging
from datetime import datetime, timedelta
from functools import wraps

from flask import Blueprint, Response, request, stream_with_context, g

log = logging.getLogger(__name__)

bp_stream  = Blueprint("odds_stream",  __name__)
bp_monitor = Blueprint("odds_monitor", __name__, url_prefix="/api/monitor")

_TIER_RANK = {"free": 0, "basic": 1, "pro": 2, "premium": 3, "admin": 4}
_LOCAL_BKS  = {"sp", "bt", "od"}
_KEEPALIVE  = 20

# sport URL slug → DB sport_name used in UnifiedMatch.sport_name
SPORT_SLUG_TO_NAME: dict[str, str] = {
    "soccer":            "Soccer",
    "basketball":        "Basketball",
    "tennis":            "Tennis",
    "cricket":           "Cricket",
    "rugby":             "Rugby",
    "ice-hockey":        "Ice Hockey",
    "volleyball":        "Volleyball",
    "handball":          "Handball",
    "table-tennis":      "Table Tennis",
    "baseball":          "Baseball",
    "mma":               "MMA",
    "boxing":            "Boxing",
    "darts":             "Darts",
    "american-football": "American Football",
    "esoccer":           "eSoccer",
}

ALL_SPORTS = list(SPORT_SLUG_TO_NAME.keys())

# Live Redis key patterns (upcoming removed — now served from DB)
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
# REDIS (live only)
# =============================================================================

def _r():
    from app.workers.celery_tasks import _redis
    return _redis()


# =============================================================================
# DATA LAYER
# =============================================================================

# ── UPCOMING: read from PostgreSQL ──────────────────────────────────────────

def _get_upcoming_from_db(sport: str) -> list[dict]:
    """
    Query UnifiedMatch (+ joined BookmakerMatchOdds) for all upcoming
    PRE_MATCH events of the given sport slug.

    Returns a list of dicts in the same shape the stream / REST endpoints
    expect: home_team, away_team, start_time, bookmakers{slug→{markets}},
    best, has_arb, best_arb_pct, arb_opportunities, …
    """
    from app.models.odds import UnifiedMatch, MatchStatus  # type: ignore

    sport_name = SPORT_SLUG_TO_NAME.get(sport, sport.replace("-", " ").title())
    now        = datetime.utcnow()

    try:
        db_matches = (
            UnifiedMatch.query
            .filter(
                UnifiedMatch.sport_name == sport_name,
                UnifiedMatch.start_time >= now,
                UnifiedMatch.status    == MatchStatus.PRE_MATCH,
            )
            .order_by(UnifiedMatch.start_time.asc())
            .all()
        )
    except Exception as exc:
        log.error("[db] _get_upcoming_from_db failed: %s", exc)
        return []

    results: list[dict] = []
    for um in db_matches:
        try:
            entry = _unified_match_to_stream_dict(um, sport)
            results.append(entry)
        except Exception as exc:
            log.debug("[db] serialise match %s: %s", getattr(um, "id", "?"), exc)

    return results


def _unified_match_to_stream_dict(um, sport: str) -> dict:
    """
    Convert a UnifiedMatch ORM row to the canonical stream dict.

    Calls um.to_dict() for the base fields, then rebuilds the
    bookmakers / best / arb fields so clients always get the same
    schema regardless of what to_dict() omits.
    """
    base: dict = um.to_dict() if hasattr(um, "to_dict") else {}

    # Ensure core identity fields are present
    parent_id   = getattr(um, "parent_match_id", None) or base.get("parent_match_id", "")
    join_key    = getattr(um, "parent_match_id", None) or base.get("join_key", parent_id)
    betradar_id = getattr(um, "betradar_id", None)     or base.get("betradar_id", "")
    start_time  = getattr(um, "start_time", None)
    if isinstance(start_time, datetime):
        start_time = start_time.isoformat()

    # Build bookmakers dict from BookmakerMatchOdds rows
    bookmakers: dict[str, dict] = base.get("bookmakers") or {}
    if not bookmakers and hasattr(um, "bookmaker_odds"):
        # um.bookmaker_odds is the relationship to BookmakerMatchOdds
        for bmo in (um.bookmaker_odds or []):
            try:
                slug = (
                    getattr(bmo.bookmaker, "slug", None)
                    or getattr(bmo.bookmaker, "name", "unk").lower()
                )
                markets: dict = {}
                if hasattr(bmo, "selections") and bmo.selections:
                    # Reconstruct market→outcome→price from flat selections
                    for sel in bmo.selections:
                        mkt = getattr(sel, "market", "")
                        out = getattr(sel, "selection", "")
                        prc = getattr(sel, "price", 0)
                        if mkt and out:
                            markets.setdefault(mkt, {})[out] = prc
                elif hasattr(bmo, "markets_json"):
                    try:
                        markets = json.loads(bmo.markets_json or "{}") or {}
                    except Exception:
                        pass
                bookmakers[slug] = {
                    "bookmaker": slug.upper(),
                    "slug":      slug,
                    "markets":   markets,
                }
            except Exception:
                pass

    # Compute cross-BK best odds
    best            = _build_best(bookmakers)
    has_arb, pct, arbs = _detect_arb(best)

    return {
        # Identity
        "match_id":         base.get("match_id") or getattr(um, "id", None),
        "join_key":         join_key,
        "parent_match_id":  parent_id,
        "betradar_id":      betradar_id,
        # Teams / competition
        "home_team":        getattr(um, "home_team_name", "") or base.get("home_team", ""),
        "away_team":        getattr(um, "away_team_name", "") or base.get("away_team", ""),
        "competition":      getattr(um, "competition_name", "") or base.get("competition", ""),
        "country":          getattr(um, "country_name", "")     or base.get("country", ""),
        "sport":            sport,
        "start_time":       start_time or base.get("start_time", ""),
        "status":           (getattr(um, "status", None) or "PRE_MATCH"),
        "is_live":          False,
        # Odds & arb
        "bookmakers":       bookmakers,
        "bk_count":         len(bookmakers),
        "market_slugs":     list(best.keys()),
        "best":             best,
        "has_arb":          has_arb,
        "best_arb_pct":     pct,
        "arb_opportunities": arbs,
        "has_ev":           False,
    }


# ── LIVE: read from Redis (unchanged) ──────────────────────────────────────

def _read_key(r, patterns: list[str], sport: str) -> list[dict] | None:
    """Try each Redis key pattern, return first non-empty match list."""
    for pat in patterns:
        try:
            raw = r.get(pat.format(sport=sport))
            if not raw:
                continue
            data    = json.loads(raw)
            matches = data.get("matches", []) if isinstance(data, dict) else data
            if matches:
                return matches
        except Exception:
            pass
    return None


def _get_live_from_redis(sport: str) -> list[dict]:
    """Merge all live BK Redis keys for this sport."""
    r = _r()
    return _merge_bks(r, sport, _BK_KEY_FORMATS_LIVE)


def _merge_bks(r, sport: str, bk_formats: list[tuple[str, list[str]]]) -> list[dict]:
    """
    Read every BK's Redis key for this sport and merge into one
    deduplicated list.  Dedup priority: betradar_id > home|away name.
    """
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
                    "match_id":         m.get("match_id") or key_jk,
                    "join_key":         key_jk,
                    "parent_match_id":  m.get("parent_match_id") or m.get("betradar_id") or key_jk,
                    "betradar_id":      m.get("betradar_id") or "",
                    "home_team":        m.get("home_team")  or m.get("home_team_name")  or "",
                    "away_team":        m.get("away_team")  or m.get("away_team_name")  or "",
                    "competition":      m.get("competition") or m.get("competition_name") or "",
                    "sport":            m.get("sport") or sport,
                    "start_time":       m.get("start_time") or "",
                    "status":           m.get("status") or "LIVE",
                    "is_live":          True,
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

    for m in result:
        m["best"]            = _build_best(m["bookmakers"])
        has_arb, pct, arbs   = _detect_arb(m["best"])
        m["has_arb"]         = has_arb
        m["best_arb_pct"]    = pct
        m["arb_opportunities"] = arbs
        m["market_slugs"]    = list(m["best"].keys())

    return result


# ── Unified router ──────────────────────────────────────────────────────────

def _get_unified(mode: str, sport: str) -> list[dict]:
    """
    Route to the correct data source:
      upcoming → PostgreSQL  (always fresh, no Redis short-circuit)
      live     → Redis       (sub-second latency, no DB)
    """
    if mode == "live":
        return _get_live_from_redis(sport)
    return _get_upcoming_from_db(sport)


# =============================================================================
# ODDS / ARB HELPERS
# =============================================================================

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
        # Recompute best / arb for reduced BK set
        mc["best"]            = _build_best(mc["bookmakers"])
        has_arb, pct, arbs    = _detect_arb(mc["best"])
        mc["has_arb"]         = has_arb
        mc["best_arb_pct"]    = pct
        mc["arb_opportunities"] = arbs
        mc["market_slugs"]    = list(mc["best"].keys())
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

        # ── Initial snapshot on connect ──────────────────────────────────
        matches = _filter_tier(_get_unified(mode, sport), tier)
        yield _sse("batch", {
            "matches": matches,
            "source":  "db" if mode == "upcoming" else "redis",
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
            # Free/basic get a one-shot snapshot, no live push
            yield ": keepalive\n\n"
            return

        # ── Pro/premium: subscribe to Redis pub/sub ───────────────────────
        # For UPCOMING mode the harvesters publish to this channel after
        # each DB commit so we re-query Postgres for the freshest data.
        # For LIVE mode the channel carries full Redis payloads.
        pubsub   = r.pubsub(ignore_subscribe_messages=True)
        channels = [
            f"odds:all:{mode}:{sport}:updates",   # harvester signals DB write done
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
                        ch      = msg.get("channel") or b""
                        if isinstance(ch, bytes):
                            ch = ch.decode()

                        if "arb:" in ch:
                            yield _sse("arb_update", payload)

                        elif "ev:" in ch:
                            yield _sse("ev_update", payload)

                        elif "live_updates" in ch and mode == "live":
                            yield _sse("live_update", payload)

                        else:
                            # Harvester finished a cycle — push fresh data
                            if mode == "upcoming":
                                # Re-read from DB (source of truth)
                                fresh = _filter_tier(_get_upcoming_from_db(sport), tier)
                            else:
                                # Re-read from Redis for live
                                fresh = _filter_tier(_get_live_from_redis(sport), tier)

                            yield _sse("batch", {
                                "matches": fresh,
                                "source":  "db" if mode == "upcoming" else "redis",
                                "sport":   sport,
                                "mode":    mode,
                                "count":   len(fresh),
                            })

                    except Exception as exc:
                        log.debug("[stream] pubsub msg error: %s", exc)

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
    """SSE endpoint — pass JWT via ?token= (EventSource can't set headers)."""
    from app.api import _err

    if mode not in ("upcoming", "live"):
        return _err("mode must be 'upcoming' or 'live'", 400)

    user = _auth_user()
    if not user:
        def _deny():
            yield _sse("error", {"error": "Unauthorized", "code": 401})
        return Response(
            stream_with_context(_deny()), mimetype="text/event-stream",
            status=200,
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
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
    """Full REST snapshot — all matches (DB for upcoming, Redis for live)."""
    from app.api import _signed_response
    tier    = getattr(g.user, "tier", "basic") or "basic"
    matches = _filter_tier(_get_unified(mode, sport), tier)
    return _signed_response({
        "matches": matches,
        "sport":   sport,
        "mode":    mode,
        "count":   len(matches),
        "source":  "db" if mode == "upcoming" else "redis",
    })


@bp_stream.route("/odds/page/<mode>/<sport>", methods=["GET"])
@require_tier("basic")
def paged_odds(mode: str, sport: str):
    """
    Paginated REST for 1 000+ match datasets.
    Used by dashboard Load More / infinite scroll.

    Query params:
      page      int  1-based (default 1)
      per_page  int  max 200 (default 100)
      sort      str  'start_time' | 'arb'
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
        "source":   "db" if mode == "upcoming" else "redis",
    })


# =============================================================================
# MONITOR
# =============================================================================

@bp_monitor.route("/competitions", methods=["GET"])
def monitor_competitions():
    """Competition names — used by filter bar.  No auth required."""
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
    """Per-sport match counts from DB (upcoming) and Redis (live)."""
    from app.api import _signed_response
    from app.models.odds import UnifiedMatch, MatchStatus  # type: ignore

    stats: dict = {}
    now = datetime.utcnow()

    # ── upcoming counts from DB ──────────────────────────────────────────
    try:
        rows = (
            UnifiedMatch.query
            .filter(
                UnifiedMatch.start_time >= now,
                UnifiedMatch.status    == MatchStatus.PRE_MATCH,
            )
            .with_entities(UnifiedMatch.sport_name)
            .all()
        )
        from collections import Counter
        db_counts = Counter(r.sport_name for (r,) in rows if r)

        # Reverse SPORT_SLUG_TO_NAME for label lookup
        name_to_slug = {v: k for k, v in SPORT_SLUG_TO_NAME.items()}

        for sport_name, count in db_counts.items():
            slug = name_to_slug.get(sport_name, sport_name.lower().replace(" ", "-"))
            stats.setdefault(slug, {})["upcoming"] = {"count": count, "source": "db"}
    except Exception as exc:
        log.error("[monitor/stats] DB query failed: %s", exc)

    # ── live counts from Redis ───────────────────────────────────────────
    r = _r()
    for sport_slug in ALL_SPORTS:
        live_matches = _get_live_from_redis(sport_slug)
        if live_matches:
            bk_seen: set[str] = set()
            for m in live_matches:
                bk_seen.update((m.get("bookmakers") or {}).keys())
            stats.setdefault(sport_slug, {})["live"] = {
                "count":    len(live_matches),
                "bks":      sorted(bk_seen),
                "bk_count": len(bk_seen),
                "source":   "redis",
            }

    return _signed_response({"stats": stats})


@bp_monitor.route("/redis-keys", methods=["GET"])
def monitor_redis_keys():
    """
    Debug: which LIVE BK Redis keys exist for a sport.
    (Upcoming is now in Postgres, not Redis — use /api/monitor/stats instead.)
    """
    from app.api import _signed_response
    sport = request.args.get("sport", "soccer")
    r     = _r()

    found:   dict[str, int] = {}
    missing: list[str]      = []

    live_patterns = [
        pat.format(sport=sport)
        for _, patterns in _BK_KEY_FORMATS_LIVE
        for pat in patterns
    ] + [f"odds:unified:live:{sport}"]

    for key in live_patterns:
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

    # Also report DB count for context
    db_count = 0
    try:
        from app.models.odds import UnifiedMatch, MatchStatus  # type: ignore
        sport_name = SPORT_SLUG_TO_NAME.get(sport, sport.replace("-", " ").title())
        db_count   = (
            UnifiedMatch.query
            .filter(
                UnifiedMatch.sport_name == sport_name,
                UnifiedMatch.start_time >= datetime.utcnow(),
                UnifiedMatch.status    == MatchStatus.PRE_MATCH,
            )
            .count()
        )
    except Exception:
        pass

    return _signed_response({
        "sport":   sport,
        "note":    "Only LIVE keys shown here — upcoming matches live in Postgres.",
        "db_upcoming_count": db_count,
        "live_redis_keys": {
            "found":   found,
            "missing": missing,
        },
        "summary": (
            f"{db_count} upcoming in DB | "
            f"{len(found)} live Redis keys found "
            f"({sum(found.values())} total live matches)"
        ),
    })