"""
app/api/live_results_api.py
============================
Results API, DB-backed odds stream, and live Celery task hooks.

Endpoints
─────────
  GET /api/results/{sport}              — finished matches today + last 7 days
  GET /api/live/matches/{sport}         — currently live (from DB + Redis)
  GET /api/live/match/{join_key}        — single match full live state
  SSE /api/live/stream/{sport}          — real-time live feed (replaces odds_stream for live)
  SSE /api/live/match/{join_key}/stream — per-match detail stream (countdown + live)
  GET /api/monitor/window               — window service debug

Celery tasks registered here
────────────────────────────
  tasks.ops.update_match_state    — write state transition to DB
  tasks.ops.save_match_result     — write final result to DB
  tasks.ops.flush_live_markets    — batch-write live market changes to DB
  tasks.notify.lifecycle_event    — fan-out notifications for state changes
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone

from flask import Blueprint, Response, g, jsonify, request, stream_with_context

log = logging.getLogger("kinetic.results_api")

bp_results = Blueprint("results", __name__, url_prefix="/api")
bp_live    = Blueprint("customer_live_api",  __name__, url_prefix="/api/live")

_KEEPALIVE = 20
_TIER_RANK = {"free": 0, "basic": 1, "pro": 2, "premium": 3, "admin": 4}


# ─── Auth (reuses odds_stream pattern) ────────────────────────────────────────

def _auth_user():
    from app.utils.customer_jwt_helpers import _decode_token
    from app.models.customer import Customer
    auth  = request.headers.get("Authorization", "")
    token = auth[7:] if auth.startswith("Bearer ") else request.args.get("token", "")
    if not token:
        return None
    try:
        payload = _decode_token(token)
        return Customer.query.get(int(payload["sub"]))
    except Exception:
        return None


def _tier(user) -> str:
    if not user:
        return "free"
    return (
        getattr(user, "subscription_tier", None) or
        getattr(user, "tier", None) or "basic"
    )


def _r(db: int = 2):
    from app.workers.match_window_service import _redis
    return _redis(db)


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


# ══════════════════════════════════════════════════════════════════════════════
# RESULTS API — finished matches
# ══════════════════════════════════════════════════════════════════════════════

@bp_results.route("/results/<sport>", methods=["GET"])
def get_results(sport: str):
    user = _auth_user()
    if not user:
        return jsonify({"error": "Authentication required"}), 401

    date_str  = request.args.get("date", datetime.now(timezone.utc).date().isoformat())
    days_back = min(int(request.args.get("days", 1)), 30)
    page      = max(1, int(request.args.get("page", 1)))
    per_page  = min(200, int(request.args.get("per_page", 50)))

    try:
        from app.models.odds import UnifiedMatch
        from app.extensions import db
        from app.utils.entity_resolver import SPORT_SLUG_MAP

        sport_name = SPORT_SLUG_MAP.get(sport, sport.title())
        ref_date   = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
        since      = ref_date - timedelta(days=days_back - 1)

        query = db.session.execute(
            db.select(UnifiedMatch).where(
                UnifiedMatch.sport_name.ilike(f"%{sport_name}%"),
                UnifiedMatch.start_time >= since,
                UnifiedMatch.start_time <= ref_date + timedelta(days=1),
                UnifiedMatch.status.in_(["finished", "ft", "complete", "ended"]),
            ).order_by(UnifiedMatch.start_time.desc())
        ).scalars().all()

        total    = len(query)
        offset   = (page - 1) * per_page
        page_res = query[offset: offset + per_page]

        results = []
        for m in page_res:
            results.append({
                "join_key":      str(m.parent_match_id or m.id),
                "match_id":      str(m.id),
                "home_team":     m.home_team_name or "",
                "away_team":     m.away_team_name or "",
                "competition":   m.competition_name or "",
                "sport":         sport,
                "start_time":    m.start_time.isoformat() if m.start_time else "",
                "status":        m.status or "finished",
                "score_home":    getattr(m, "final_score_home", None),
                "score_away":    getattr(m, "final_score_away", None),
                "winner":        _calc_winner(
                    getattr(m, "final_score_home", None),
                    getattr(m, "final_score_away", None),
                ),
                "result_source": getattr(m, "result_source", "lifecycle"),
                "finished_at":   (getattr(m, "finished_at", None) or ""),
            })

        return jsonify({
            "results":  results,
            "total":    total,
            "page":     page,
            "per_page": per_page,
            "has_more": offset + per_page < total,
            "sport":    sport,
            "date":     date_str,
            "days":     days_back,
        })

    except Exception as e:
        log.error("Results API error: %s", e)
        return jsonify({"error": str(e)}), 500


def _calc_winner(home, away) -> str | None:
    try:
        h = int(home or 0); a = int(away or 0)
        return "home" if h > a else "away" if a > h else "draw"
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# LIVE MATCH API
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/matches/<sport>", methods=["GET"])
def live_matches(sport: str):
    user = _auth_user()
    if not user:
        return jsonify({"error": "Authentication required"}), 401

    r        = _r()
    live_jks = list(r.smembers("kinetic:window:live") or [])
    matches  = []
    for jk in live_jks:
        meta  = r.hgetall(f"kinetic:match:{jk}:meta") or {}
        if meta.get("sport", "") not in (sport, ""):
            continue
        state = r.hgetall(f"kinetic:match:{jk}:state") or {}
        score = r.hgetall(f"kinetic:match:{jk}:score") or {}
        delay = r.hgetall(f"kinetic:match:{jk}:delay") or {}
        matches.append({
            "join_key":      jk,
            "home_team":     meta.get("home_team", ""),
            "away_team":     meta.get("away_team", ""),
            "competition":   meta.get("competition", ""),
            "sport":         meta.get("sport", sport),
            "start_time":    meta.get("start_time", ""),
            "status":        "live",
            "phase":         state.get("phase", "live"),
            "live_since":    state.get("live_since", ""),
            "live_reason":   state.get("live_reason", ""),
            "score_home":    score.get("home"),
            "score_away":    score.get("away"),
            "match_time":    score.get("time"),
            "has_delay":     bool(delay),
            "delay_minutes": round(float(delay.get("delay_s", 0)) / 60, 1) if delay else 0,
            "bk_consensus":  _get_consensus(r, jk),
        })
    return jsonify({"live": matches, "count": len(matches), "sport": sport})


@bp_live.route("/match/<join_key>", methods=["GET"])
def live_match_detail(join_key: str):
    user = _auth_user()
    if not user:
        return jsonify({"error": "Authentication required"}), 401

    r     = _r()
    meta  = r.hgetall(f"kinetic:match:{join_key}:meta") or {}
    state = r.hgetall(f"kinetic:match:{join_key}:state") or {}
    score = r.hgetall(f"kinetic:match:{join_key}:score") or {}
    delay = r.hgetall(f"kinetic:match:{join_key}:delay") or {}

    db_markets: dict = {}
    try:
        from app.models.odds import UnifiedMatch
        from app.extensions import db as _db
        um = _db.session.execute(
            _db.select(UnifiedMatch).where(UnifiedMatch.parent_match_id == join_key)
        ).scalar_one_or_none()
        if um:
            db_markets = _extract_db_markets(um, _tier(user))
    except Exception as e:
        log.debug("DB market fetch error: %s", e)

    from app.workers.match_window_service import MatchMarketState
    redis_markets  = MatchMarketState(r, join_key).best_odds()
    merged_markets = {**db_markets, **redis_markets}
    arb = _detect_arb_fast(merged_markets)

    return jsonify({
        "join_key":      join_key,
        "home_team":     meta.get("home_team", ""),
        "away_team":     meta.get("away_team", ""),
        "competition":   meta.get("competition", ""),
        "sport":         meta.get("sport", ""),
        "start_time":    meta.get("start_time", ""),
        "phase":         state.get("phase", "countdown"),
        "live_since":    state.get("live_since", ""),
        "score_home":    score.get("home"),
        "score_away":    score.get("away"),
        "match_time":    score.get("time"),
        "has_delay":     bool(delay),
        "delay_minutes": round(float(delay.get("delay_s", 0)) / 60, 1) if delay else 0,
        "bk_consensus":  _get_consensus(r, join_key),
        "markets":       merged_markets,
        "has_arb":       arb is not None,
        "best_arb":      arb,
        "ts":            _now_iso(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# SSE STREAMS
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/stream/<sport>", methods=["GET"])
def live_stream(sport: str):
    user = _auth_user()
    if not user:
        def _deny():
            yield _sse("error", {"error": "Unauthorized"})
        return Response(stream_with_context(_deny()), mimetype="text/event-stream")

    def generate():
        r      = _r()
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(f"ws:sport:{sport}:live", f"ws:sport:{sport}:results")
        last_ka = time.time()

        live_jks = list(r.smembers("kinetic:window:live") or [])
        initial  = []
        for jk in live_jks:
            meta = r.hgetall(f"kinetic:match:{jk}:meta") or {}
            if meta.get("sport", "") == sport:
                score = r.hgetall(f"kinetic:match:{jk}:score") or {}
                initial.append({
                    "join_key":   jk,
                    "home_team":  meta.get("home_team", ""),
                    "away_team":  meta.get("away_team", ""),
                    "score_home": score.get("home"),
                    "score_away": score.get("away"),
                    "match_time": score.get("time"),
                    "phase":      "live",
                })
        yield _sse("snapshot", {"matches": initial, "sport": sport, "count": len(initial)})

        try:
            while True:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg.get("type") == "message":
                    try:
                        data = json.loads(msg["data"])
                        ch   = (msg.get("channel") or "")
                        evt  = "result" if "results" in ch else "live_update"
                        yield _sse(evt, data)
                    except Exception:
                        pass
                if time.time() - last_ka > _KEEPALIVE:
                    yield ": keepalive\n\n"
                    last_ka = time.time()
        finally:
            pubsub.unsubscribe()
            pubsub.close()

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Connection": "keep-alive", "Access-Control-Allow-Origin": "*"},
    )


@bp_live.route("/match/<join_key>/stream", methods=["GET"])
def match_stream(join_key: str):
    user = _auth_user()
    if not user:
        def _deny():
            yield _sse("error", {"error": "Unauthorized"})
        return Response(stream_with_context(_deny()), mimetype="text/event-stream")

    def generate():
        r      = _r()
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(
            f"ws:match:{join_key}:countdown",
            f"ws:match:{join_key}:live",
            f"ws:match:{join_key}:state",
            f"ws:match:{join_key}:result",
        )
        last_ka = time.time()

        meta  = r.hgetall(f"kinetic:match:{join_key}:meta") or {}
        state = r.hgetall(f"kinetic:match:{join_key}:state") or {}
        score = r.hgetall(f"kinetic:match:{join_key}:score") or {}

        yield _sse("connected", {
            "join_key":     join_key,
            "home_team":    meta.get("home_team", ""),
            "away_team":    meta.get("away_team", ""),
            "start_time":   meta.get("start_time", ""),
            "phase":        state.get("phase", "countdown"),
            "score_home":   score.get("home"),
            "score_away":   score.get("away"),
            "match_time":   score.get("time"),
            "bk_consensus": _get_consensus(r, join_key),
        })

        try:
            while True:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg.get("type") == "message":
                    try:
                        data = json.loads(msg["data"])
                        ch   = (msg.get("channel") or "")
                        if   "countdown" in ch: yield _sse("countdown",    data)
                        elif "live"      in ch: yield _sse("live_tick",     data)
                        elif "state"     in ch: yield _sse("state_change",  data)
                        elif "result"    in ch: yield _sse("result",        data)
                    except Exception:
                        pass
                if time.time() - last_ka > _KEEPALIVE:
                    yield ": keepalive\n\n"
                    last_ka = time.time()
        finally:
            pubsub.unsubscribe()
            pubsub.close()

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Connection": "keep-alive", "Access-Control-Allow-Origin": "*"},
    )


# ══════════════════════════════════════════════════════════════════════════════
# MONITOR
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/window", methods=["GET"])
def window_status():
    r        = _r()
    live_jks = list(r.smembers("kinetic:window:live") or [])
    window   = r.zrange("kinetic:window:active", 0, -1, withscores=True)
    finished = list(r.smembers("kinetic:window:finished") or [])
    return jsonify({
        "window_count":   len(window),
        "live_count":     len(live_jks),
        "finished_today": len(finished),
        "live_matches":   live_jks[:20],
        "window_matches": [
            {"jk": jk, "start_ts": ts,
             "phase": r.hget(f"kinetic:match:{jk}:state", "phase") or "countdown"}
            for jk, ts in window[:20]
        ],
    })


# ══════════════════════════════════════════════════════════════════════════════
# CELERY TASKS
# ══════════════════════════════════════════════════════════════════════════════

def register_lifecycle_tasks(celery):
    @celery.task(name="tasks.ops.update_match_state",
                 bind=True, max_retries=3, default_retry_delay=5)
    def update_match_state(self, join_key: str, new_state: str, meta: dict):
        try:
            from app.models.odds import UnifiedMatch
            from app.extensions import db
            um = db.session.execute(
                db.select(UnifiedMatch).where(UnifiedMatch.parent_match_id == join_key)
            ).scalar_one_or_none()
            if um:
                um.status = new_state
                if new_state == "live":
                    um.live_since = datetime.fromisoformat(
                        meta.get("live_since", _now_iso()).replace("Z", "+00:00")
                    )
                db.session.commit()
        except Exception as exc:
            raise self.retry(exc=exc)

    @celery.task(name="tasks.ops.save_match_result",
                 bind=True, max_retries=3, default_retry_delay=10)
    def save_match_result(self, join_key: str, result: dict):
        try:
            from app.models.odds import UnifiedMatch
            from app.extensions import db
            um = db.session.execute(
                db.select(UnifiedMatch).where(UnifiedMatch.parent_match_id == join_key)
            ).scalar_one_or_none()
            if um:
                um.status           = "finished"
                um.final_score_home = result.get("score_home")
                um.final_score_away = result.get("score_away")
                um.result_source    = result.get("source", "lifecycle")
                um.finished_at      = datetime.now(timezone.utc)
                db.session.commit()
        except Exception as exc:
            raise self.retry(exc=exc)

    @celery.task(name="tasks.ops.flush_live_markets",
                 bind=True, max_retries=2, default_retry_delay=3, acks_late=True)
    def flush_live_markets(self, join_key: str, writes: list[dict]):
        if not writes:
            return
        try:
            from app.models.odds import UnifiedMatch, BookmakerMatchOdds
            from app.models.bookmakers_model import Bookmaker
            from app.extensions import db
            um = db.session.execute(
                db.select(UnifiedMatch).where(UnifiedMatch.parent_match_id == join_key)
            ).scalar_one_or_none()
            if not um:
                return
            by_bk: dict[str, list] = {}
            for w in writes:
                by_bk.setdefault(w["bk"], []).append(w)
            for bk_slug, bk_writes in by_bk.items():
                bm = Bookmaker.query.filter_by(slug=bk_slug).first()
                if not bm:
                    continue
                bmo = BookmakerMatchOdds.query.filter_by(
                    match_id=um.id, bookmaker_id=bm.id
                ).first()
                if not bmo:
                    bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bm.id)
                    db.session.add(bmo)
                    db.session.flush()
                for w in bk_writes:
                    try:
                        bmo.upsert_selection(
                            market=w["slug"], specifier=None,
                            selection=w["outcome"], price=float(w["odd"]),
                        )
                    except Exception:
                        pass
            db.session.commit()
        except Exception as exc:
            raise self.retry(exc=exc)

    @celery.task(name="tasks.notify.lifecycle_event",
                 bind=True, max_retries=2, default_retry_delay=5)
    def lifecycle_event(self, event_type: str, join_key: str, payload: dict):
        try:
            from app.workers.match_lifecycle import get_lifecycle_manager
            mgr   = get_lifecycle_manager()
            saved = mgr.get_watch(join_key)
            if not saved:
                return
            from app.workers.match_lifecycle import Notification, NotificationDispatcher
            dispatcher = NotificationDispatcher()
            event_to_notif = {
                "state_change":  ("State Changed", payload.get("new_state", "").upper()),
                "result":        ("Match Finished", _result_body(payload)),
                "kickoff_delay": ("Kickoff Delayed",
                                  f"Delayed by {payload.get('delay_minutes', 0):.0f} min"),
            }
            title, body = event_to_notif.get(event_type, (event_type, str(payload)))
            for watcher in saved.watchers:
                if event_type not in watcher.notify_on:
                    continue
                notif = Notification(
                    match=saved, watcher=watcher,
                    event_type=event_type, title=title, body=body, data=payload,
                )
                dispatcher.dispatch(notif)
        except Exception as exc:
            raise self.retry(exc=exc)

    return update_match_state, save_match_result, flush_live_markets, lifecycle_event


# ── FIXED: removed hardcoded IP fallback ──────────────────────────────────────
def _sp_session_safe():
    """
    Returns a requests.Session configured with proxy from environment.
    FIXED: no longer hardcodes socks5h://[100.68.207.107] as fallback.
    Proxy is always read from ALL_PROXY env var set in docker-compose.
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    import os

    # FIXED: read from env only — never hardcode an IP
    proxy = os.environ.get("ALL_PROXY") or os.environ.get("HTTP_PROXY") or ""

    session = requests.Session()
    if proxy:
        session.proxies = {"http": proxy, "https": proxy}

    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    session.headers.update({
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
    })

    try:
        from app.workers.bandwidth_optimizer import compressed_session
        return compressed_session(proxy=proxy)
    except ImportError:
        return session


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_consensus(r, jk: str) -> dict:
    from app.workers.match_window_service import BkConsensus, ALL_BKS
    return BkConsensus(r, jk).to_dict()


def _extract_db_markets(um, tier: str) -> dict:
    try:
        prices = getattr(um, "bookmaker_prices", None) or {}
        if isinstance(prices, str):
            prices = json.loads(prices)
        return prices
    except Exception:
        return {}


def _detect_arb_fast(markets: dict) -> dict | None:
    from itertools import combinations
    best_arb = None
    for slug, outcomes in markets.items():
        outs = {k: v["odd"] if isinstance(v, dict) else float(v)
                for k, v in outcomes.items()
                if isinstance(v, (int, float, dict))}
        keys = [k for k, v in outs.items() if isinstance(v, float) and v > 1]
        for combo in combinations(keys[:4], 2):
            odds = [outs[k] for k in combo]
            inv  = sum(1 / o for o in odds)
            if inv < 1.0:
                pct = round((1 / inv - 1) * 100, 3)
                if not best_arb or pct > best_arb["profit_pct"]:
                    best_arb = {
                        "market": slug, "profit_pct": pct,
                        "legs": [{"outcome": k, "odd": outs[k]} for k in combo],
                    }
    return best_arb


def _result_body(payload: dict) -> str:
    r = payload.get("result") or {}
    return f"Final: {r.get('score_home', '?')}–{r.get('score_away', '?')}"