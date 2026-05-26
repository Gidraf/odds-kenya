"""
app/api/live_results_api.py
============================
Live data + results API.

Reads live state from LiveFeedBridge / Redis.
Results (finished matches) are read from the DB — works for ALL sports
because every match is persisted to UnifiedMatch by the harvest pipeline.

Endpoints
─────────
  GET  /api/results/<sport>                  — finished matches from DB
  GET  /api/live/matches/<sport>             — live matches from LiveFeedBridge
  GET  /api/live/match/<join_key>            — single match state
  SSE  /api/live/stream/<sport>              — real-time via Redis pub/sub
  SSE  /api/live/match/<join_key>/stream     — per-match score stream
  GET  /api/live/window                      — debug / live window status
  POST /api/live/save-result                 — save result (called by broadcaster)
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from flask import Blueprint, Response, jsonify, request, stream_with_context

log = logging.getLogger("kinetic.live_api")

bp_results = Blueprint("results",           __name__, url_prefix="/api")
bp_live    = Blueprint("customer_live_api", __name__, url_prefix="/api/live")

_KEEPALIVE = 20

# ── CORS ──────────────────────────────────────────────────────────────────────

def _add_cors(resp):
    origin = request.headers.get("Origin") or "*"
    resp.headers["Access-Control-Allow-Origin"]      = origin
    resp.headers["Access-Control-Allow-Headers"]     = "Content-Type, Authorization"
    resp.headers["Access-Control-Allow-Methods"]     = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    return resp


@bp_results.before_request
def _handle_options_results():
    if request.method == "OPTIONS":
        return ("", 204)

@bp_live.before_request
def _handle_options_live():
    if request.method == "OPTIONS":
        return ("", 204)

bp_results.after_request(_add_cors)
bp_live.after_request(_add_cors)


# ── Redis ─────────────────────────────────────────────────────────────────────

def _r():
    import redis as _redis
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return _redis.Redis.from_url(url, decode_responses=True, socket_timeout=5)


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Auth (open access) ────────────────────────────────────────────────────────

def _auth_user():
    try:
        from app.utils.customer_jwt_helpers import _decode_token
        from app.models.customer import Customer
        auth  = request.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else request.args.get("token", "")
        if token:
            payload = _decode_token(token)
            return Customer.query.get(int(payload["sub"]))
    except Exception as exc:
        log.debug("Auth (open): %s", exc)
    return None


# ── DB result helpers ─────────────────────────────────────────────────────────

def _save_result_now(
    join_key:   str,
    score_home: str | int | None,
    score_away: str | int | None,
    source:     str = "api",
) -> bool:
    """
    Save a finished match result to UnifiedMatch.
    join_key can be "br_<betradar_id>" or "db_<id>".
    Safe to call multiple times (idempotent).
    """
    try:
        from app.models.odds import UnifiedMatch
        from app.extensions import db

        um = None
        if join_key.startswith("br_"):
            br_id = join_key[3:]
            um = UnifiedMatch.query.filter_by(parent_match_id=br_id).first()
        elif join_key.startswith("db_"):
            db_id = join_key[3:]
            um = UnifiedMatch.query.get(int(db_id))
        else:
            # Try both
            um = UnifiedMatch.query.filter_by(parent_match_id=join_key).first()
            if not um:
                try:
                    um = UnifiedMatch.query.get(int(join_key))
                except (ValueError, TypeError):
                    pass

        if not um:
            log.warning("[save_result] match not found: %s", join_key)
            return False

        # Parse scores
        def _to_int(v) -> int | None:
            try:
                return int(str(v).strip()) if v is not None else None
            except (TypeError, ValueError):
                return None

        sh = _to_int(score_home)
        sa = _to_int(score_away)

        um.status           = "finished"
        um.final_score_home = sh
        um.final_score_away = sa
        um.result_source    = source
        um.finished_at      = datetime.now(timezone.utc)
        db.session.commit()

        log.info("[save_result] ✅ %s  %s %s-%s %s",
                 join_key, um.home_team_name, sh, sa, um.away_team_name)
        return True

    except Exception as exc:
        log.error("[save_result] error %s: %s", join_key, exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        return False


def _calc_winner(home, away) -> str | None:
    try:
        h = int(home or 0); a = int(away or 0)
        return "home" if h > a else "away" if a > h else "draw"
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# RESULTS — DB-backed, all sports
# ══════════════════════════════════════════════════════════════════════════════

@bp_results.route("/results/<sport>", methods=["GET", "OPTIONS"])
def get_results(sport: str):
    """
    GET /api/results/<sport>
    Reads finished matches from DB for the given sport.
    Works for ALL sports (not just soccer) because every match is persisted
    to UnifiedMatch after harvest.
    Open access, no auth required.
    """
    date_str  = request.args.get("date", datetime.now(timezone.utc).date().isoformat())
    days_back = min(int(request.args.get("days", 1)), 30)
    page      = max(1,   int(request.args.get("page", 1)))
    per_page  = min(200, int(request.args.get("per_page", 50)))

    try:
        from app.models.odds import UnifiedMatch
        from app.extensions import db
        from sqlalchemy import or_

        # Sport name mapping (same as word_generator_v2)
        _SPORT_DB_NAMES = {
            "soccer":            ["Soccer", "Football"],
            "basketball":        ["Basketball"],
            "tennis":            ["Tennis"],
            "ice-hockey":        ["Ice Hockey"],
            "volleyball":        ["Volleyball"],
            "cricket":           ["Cricket"],
            "rugby":             ["Rugby"],
            "table-tennis":      ["Table Tennis"],
            "handball":          ["Handball"],
            "mma":               ["MMA"],
            "boxing":            ["Boxing"],
            "darts":             ["Darts"],
            "american-football": ["American Football"],
            "baseball":          ["Baseball"],
            "esoccer":           ["eSoccer", "eFootball"],
        }

        sport_names   = _SPORT_DB_NAMES.get(sport.lower(), [sport.replace("-", " ").title()])
        sport_filters = [UnifiedMatch.sport_name.ilike(f"%{n}%") for n in sport_names]

        ref_date = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
        since    = ref_date - timedelta(days=days_back - 1)

        rows = db.session.execute(
            db.select(UnifiedMatch).where(
                or_(*sport_filters),
                UnifiedMatch.start_time >= since,
                UnifiedMatch.start_time <= ref_date + timedelta(days=1),
                UnifiedMatch.status.in_(["finished", "ft", "complete", "ended", "FT"]),
            ).order_by(UnifiedMatch.start_time.desc())
        ).scalars().all()

        total    = len(rows)
        offset   = (page - 1) * per_page
        page_res = rows[offset: offset + per_page]

        results = []
        for m in page_res:
            sh = getattr(m, "final_score_home", None)
            sa = getattr(m, "final_score_away", None)
            results.append({
                "join_key":    f"br_{m.parent_match_id}" if m.parent_match_id else f"db_{m.id}",
                "match_id":    str(m.id),
                "home_team":   m.home_team_name  or "",
                "away_team":   m.away_team_name  or "",
                "competition": m.competition_name or "",
                "sport":       sport,
                "sport_name":  m.sport_name or sport,
                "start_time":  m.start_time.isoformat() if m.start_time else "",
                "status":      m.status or "finished",
                "score_home":  sh,
                "score_away":  sa,
                "score":       f"{sh}-{sa}" if sh is not None and sa is not None else None,
                "winner":      _calc_winner(sh, sa),
                "finished_at": str(getattr(m, "finished_at", "") or ""),
                "result_source": getattr(m, "result_source", None),
            })

        return jsonify({
            "ok":       True,
            "results":  results,
            "total":    total,
            "page":     page,
            "per_page": per_page,
            "has_more": offset + per_page < total,
            "sport":    sport,
            "date":     date_str,
            "days":     days_back,
        })

    except Exception as exc:
        log.error("Results API error %s: %s", sport, exc, exc_info=True)
        return jsonify({"ok": False, "error": str(exc), "results": [], "total": 0}), 500


# ══════════════════════════════════════════════════════════════════════════════
# SAVE RESULT WEBHOOK — called by live_broadcaster & external harvesters
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/save-result", methods=["POST", "OPTIONS"])
def save_result_endpoint():
    """
    POST /api/live/save-result
    Body JSON:
        {join_key, score_home, score_away, source?}
    Called by live_broadcaster.py (and can be called by any harvester) to
    persist a full-time result immediately without waiting for Celery.
    """
    data = request.get_json(silent=True) or {}

    join_key   = (data.get("join_key") or "").strip()
    score_home = data.get("score_home")
    score_away = data.get("score_away")
    source     = data.get("source", "webhook")

    # Also accept betradar_id directly
    if not join_key and data.get("betradar_id"):
        join_key = f"br_{data['betradar_id']}"

    if not join_key:
        return jsonify({"ok": False, "error": "join_key required"}), 400

    saved = _save_result_now(join_key, score_home, score_away, source)
    return jsonify({"ok": saved, "join_key": join_key, "ts": _now_iso()})


# ══════════════════════════════════════════════════════════════════════════════
# LIVE MATCHES — from LiveFeedBridge
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/matches/<sport>", methods=["GET", "OPTIONS"])
def live_matches(sport: str):
    """
    GET /api/live/matches/<sport>
    Returns live matches. Primary: LiveFeedBridge. Fallback: Redis window set.
    Open access.
    """
    try:
        from app.workers.live_feed_bridge import get_live_bridge
        bridge  = get_live_bridge()
        matches = bridge.get_live_matches(sport=sport)
        return jsonify({
            "ok":     True,
            "live":   matches,
            "count":  len(matches),
            "sport":  sport,
            "source": "sp_websocket_primary",
            "ts":     _now_iso(),
        })
    except Exception as exc:
        log.error("live_matches error %s: %s", sport, exc)
        try:
            rv        = _r()
            live_jks  = list(rv.smembers("kinetic:window:live") or [])
            matches   = []
            for jk in live_jks:
                score = rv.hgetall(f"kinetic:match:{jk}:score") or {}
                matches.append({
                    "join_key":   jk,
                    "score_home": score.get("home"),
                    "score_away": score.get("away"),
                    "match_time": score.get("time"),
                    "sport":      sport,
                    "is_live":    True,
                })
            return jsonify({"ok": True, "live": matches, "count": len(matches),
                            "sport": sport, "source": "redis_fallback"})
        except Exception:
            return jsonify({"ok": False, "live": [], "count": 0,
                            "sport": sport, "error": str(exc)})


@bp_live.route("/match/<join_key>", methods=["GET", "OPTIONS"])
def live_match_detail(join_key: str):
    """
    GET /api/live/match/<join_key>
    Reads: LiveFeedBridge → Redis score hash → DB.
    """
    # 1. LiveFeedBridge
    try:
        from app.workers.live_feed_bridge import get_live_bridge
        state = get_live_bridge().get_match_state(join_key)
        if state:
            return jsonify({
                "ok":          True,
                "join_key":    join_key,
                "home_team":   state.get("home_team", ""),
                "away_team":   state.get("away_team", ""),
                "sport":       state.get("sport", ""),
                "score_home":  state.get("score_home"),
                "score_away":  state.get("score_away"),
                "match_time":  state.get("match_time"),
                "is_live":     not state.get("is_finished", False),
                "is_finished": state.get("is_finished", False),
                "phase":       "finished" if state.get("is_finished") else "live",
                "source":      "bridge",
                "ts":          _now_iso(),
            })
    except Exception as exc:
        log.debug("Bridge state error %s: %s", join_key, exc)

    # 2. Redis score hash
    try:
        rv    = _r()
        score = rv.hgetall(f"kinetic:match:{join_key}:score") or {}
        if score:
            is_live = rv.sismember("kinetic:window:live", join_key)
            return jsonify({
                "ok":         True,
                "join_key":   join_key,
                "score_home": score.get("home"),
                "score_away": score.get("away"),
                "match_time": score.get("time"),
                "is_live":    bool(is_live),
                "phase":      "live" if is_live else "countdown",
                "source":     "redis_hash",
                "ts":         _now_iso(),
            })
    except Exception as exc:
        log.debug("Redis score hash error %s: %s", join_key, exc)

    # 3. DB fallback (for recently finished matches)
    try:
        from app.models.odds import UnifiedMatch
        um = None
        if join_key.startswith("br_"):
            um = UnifiedMatch.query.filter_by(parent_match_id=join_key[3:]).first()
        elif join_key.startswith("db_"):
            um = UnifiedMatch.query.get(int(join_key[3:]))
        if um:
            sh = getattr(um, "final_score_home", None)
            sa = getattr(um, "final_score_away", None)
            status = (um.status or "").lower()
            return jsonify({
                "ok":          True,
                "join_key":    join_key,
                "home_team":   um.home_team_name or "",
                "away_team":   um.away_team_name or "",
                "score_home":  sh,
                "score_away":  sa,
                "is_live":     status in ("in_play", "live"),
                "is_finished": status in ("finished", "ft", "complete"),
                "phase":       status,
                "source":      "db",
                "ts":          _now_iso(),
            })
    except Exception as exc:
        log.debug("DB state error %s: %s", join_key, exc)

    return jsonify({
        "ok":       True,
        "join_key": join_key,
        "phase":    "countdown",
        "is_live":  False,
        "source":   "not_found",
        "ts":       _now_iso(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# SSE STREAMS
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/stream/<sport>", methods=["GET"])
def live_stream(sport: str):
    """SSE GET /api/live/stream/<sport> — subscribes to bus:live_updates:{sport}."""

    def generate():
        try:
            rv     = _r()
            pubsub = rv.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(f"bus:live_updates:{sport}")
            last_ka = time.time()

            # Initial snapshot
            try:
                from app.workers.live_feed_bridge import get_live_bridge
                live_now = get_live_bridge().get_live_matches(sport)
            except Exception:
                live_now = []

            yield _sse("snapshot", {
                "matches": live_now,
                "sport":   sport,
                "count":   len(live_now),
                "source":  "sp_websocket",
            })

            while True:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg.get("type") == "message":
                    try:
                        data = json.loads(msg["data"])
                        evt  = "result" if data.get("is_finished") else "live_update"

                        # Auto-save result when FT comes through the stream
                        if evt == "result" and data.get("join_key"):
                            _save_result_now(
                                data["join_key"],
                                data.get("score_home"),
                                data.get("score_away"),
                                source="sse_stream",
                            )

                        yield _sse(evt, data)
                    except Exception:
                        pass
                if time.time() - last_ka > _KEEPALIVE:
                    yield ": keepalive\n\n"
                    last_ka = time.time()
        except GeneratorExit:
            pass
        finally:
            try:
                pubsub.unsubscribe(); pubsub.close()
            except Exception:
                pass

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Connection":                  "keep-alive",
            "Access-Control-Allow-Origin": "*",
        },
    )


@bp_live.route("/match/<join_key>/stream", methods=["GET"])
def match_stream(join_key: str):
    """SSE GET /api/live/match/<join_key>/stream — per-match score stream."""

    def generate():
        try:
            rv     = _r()
            pubsub = rv.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(f"live:match:{join_key}:all")
            last_ka = time.time()

            # Initial state
            try:
                from app.workers.live_feed_bridge import get_live_bridge
                state = get_live_bridge().get_match_state(join_key) or {}
            except Exception:
                state = {}

            yield _sse("connected", {
                "join_key":   join_key,
                "score_home": state.get("score_home"),
                "score_away": state.get("score_away"),
                "match_time": state.get("match_time"),
                "phase":      "finished" if state.get("is_finished") else "live",
                "home_team":  state.get("home_team", ""),
                "away_team":  state.get("away_team", ""),
            })

            while True:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg.get("type") == "message":
                    try:
                        data        = json.loads(msg["data"])
                        is_finished = data.get("is_finished") or str(
                            data.get("status") or data.get("event_status") or ""
                        ).lower() in ("ft", "finished", "ended", "complete")

                        evt = "result" if is_finished else "live_tick"

                        # Auto-save when FT comes through per-match channel
                        if is_finished:
                            _save_result_now(
                                join_key,
                                data.get("score_home"),
                                data.get("score_away"),
                                source="match_stream",
                            )

                        yield _sse(evt, {
                            "join_key":    join_key,
                            "score_home":  data.get("score_home"),
                            "score_away":  data.get("score_away"),
                            "match_time":  data.get("match_time") or data.get("matchTime"),
                            "is_finished": is_finished,
                            "bookmakers":  data.get("bookmakers", {}),
                            "ts":          time.time(),
                        })
                    except Exception:
                        pass
                if time.time() - last_ka > _KEEPALIVE:
                    yield ": keepalive\n\n"
                    last_ka = time.time()
        except GeneratorExit:
            pass
        finally:
            try:
                pubsub.unsubscribe(); pubsub.close()
            except Exception:
                pass

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":               "no-cache",
            "X-Accel-Buffering":           "no",
            "Connection":                  "keep-alive",
            "Access-Control-Allow-Origin": "*",
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# DEBUG / MONITOR
# ══════════════════════════════════════════════════════════════════════════════

@bp_live.route("/window", methods=["GET", "OPTIONS"])
def window_status():
    """GET /api/live/window — live match debug."""
    try:
        from app.workers.live_feed_bridge import get_live_bridge
        bridge   = get_live_bridge()
        all_live = bridge.get_live_matches()
        return jsonify({
            "ok":         True,
            "live_count": len(all_live),
            "matches":    all_live[:20],
            "source":     "live_feed_bridge",
        })
    except Exception as exc:
        try:
            rv       = _r()
            live_jks = list(rv.smembers("kinetic:window:live") or [])
            return jsonify({"ok": True, "live_count": len(live_jks),
                            "join_keys": live_jks[:20], "source": "redis_set"})
        except Exception:
            return jsonify({"ok": False, "error": str(exc)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# CELERY TASKS
# ══════════════════════════════════════════════════════════════════════════════

def register_lifecycle_tasks(celery):
    """Minimal result-saving Celery tasks."""

    @celery.task(name="tasks.ops.save_match_result",
                 bind=True, max_retries=3, default_retry_delay=10)
    def save_match_result(self, join_key: str, result: dict):
        """Save a finished match result to DB. Fired by LiveFeedBridge / broadcaster."""
        ok = _save_result_now(
            join_key,
            result.get("score_home"),
            result.get("score_away"),
            source=result.get("source", "celery"),
        )
        if not ok:
            raise self.retry(exc=Exception(f"save_result_now returned False for {join_key}"))

    @celery.task(name="tasks.ops.update_match_state",
                 bind=True, max_retries=2, default_retry_delay=5)
    def update_match_state(self, join_key: str, new_state: str, meta: dict):
        """Minimal DB status update."""
        try:
            from app.models.odds import UnifiedMatch
            from app.extensions import db
            um = None
            if join_key.startswith("br_"):
                um = UnifiedMatch.query.filter_by(parent_match_id=join_key[3:]).first()
            elif join_key.startswith("db_"):
                um = UnifiedMatch.query.get(int(join_key[3:]))
            else:
                um = UnifiedMatch.query.filter_by(parent_match_id=join_key).first()
            if um:
                um.status = new_state
                db.session.commit()
        except Exception as exc:
            raise self.retry(exc=exc)

    return save_match_result, update_match_state


# ══════════════════════════════════════════════════════════════════════════════
# PROXY SESSION HELPER
# ══════════════════════════════════════════════════════════════════════════════

def _sp_session_safe():
    import os, requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    proxy   = os.environ.get("ALL_PROXY") or os.environ.get("HTTP_PROXY") or ""
    session = requests.Session()
    if proxy:
        session.proxies = {"http": proxy, "https": proxy}
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    try:
        from app.workers.bandwidth_optimizer import compressed_session
        return compressed_session(proxy=proxy)
    except ImportError:
        return session