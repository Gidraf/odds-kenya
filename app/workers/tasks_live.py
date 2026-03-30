"""
app/workers/tasks_live.py
==========================
All Celery tasks for live-event detail polling and harvesting.

Public API (imported by sp_live_view.py + tasks_ops.py)
────────────────────────────────────────────────────────
  _fetch_and_publish_details(event_id, sport_id)
  sp_poll_event_details          Celery task
  sp_poll_all_event_details      Celery task (beat: every 5 s)
  stream_event_details_sse(event_id, sport_id)   SSE generator
  sp_harvest_all_live            beat snapshot
  bt_harvest_all_live            beat snapshot
  od_harvest_all_live            beat snapshot
  b2b_harvest_all_live / b2b_page_harvest_all_live / sbo_harvest_all_live  stubs
  sp_harvest_all_upcoming / bt_harvest_all_upcoming / od_harvest_all_upcoming
  b2b_harvest_all_upcoming / b2b_page_harvest_all_upcoming / sbo_harvest_all_upcoming
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Generator

import redis as _redis_lib
import requests

from app.workers.celery_tasks import celery, _publish, cache_set

log = logging.getLogger("tasks_live")

# ─────────────────────────────────────────────────────────────────────────────
# Redis helpers  (self-contained so we never fail on sp_live_harvester import)
# ─────────────────────────────────────────────────────────────────────────────

_redis_client: _redis_lib.Redis | None = None
_redis_lock   = threading.Lock()


def _get_redis() -> _redis_lib.Redis:
    """Return a Redis client on DB 1 (live data)."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    with _redis_lock:
        if _redis_client is None:
            url  = os.environ.get("REDIS_URL",
                   os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0"))
            base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
            _redis_client = _redis_lib.Redis.from_url(
                f"{base}/1",
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
            )
    return _redis_client


# Channel helpers
CH_ALL = "sp:live:all"

def _ch_sport(sport_id: int) -> str:
    return f"sp:live:sport:{sport_id}"

def _ch_event(event_id: int) -> str:
    return f"sp:live:event:{event_id}"


def _publish_live(r: _redis_lib.Redis, sport_id: int, event_id: int, payload: dict) -> None:
    raw = json.dumps(payload, ensure_ascii=False)
    try:
        pipe = r.pipeline()
        pipe.publish(CH_ALL, raw)
        pipe.publish(_ch_sport(sport_id), raw)
        pipe.publish(_ch_event(event_id), raw)
        pipe.execute()
    except Exception as exc:
        log.debug("publish ev=%d: %s", event_id, exc)


# ─────────────────────────────────────────────────────────────────────────────
# SP HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

_SP_BASE = "https://www.ke.sportpesa.com"

_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Timezone":   "Africa/Nairobi",
    "Origin":           _SP_BASE,
    "Referer":          _SP_BASE + "/en/live/",
}

_LIVE_SPORT_IDS = [1, 2, 3, 5, 6, 7, 8, 9, 13]   # soccer, basketball, tennis, ...


def _sp_get(path: str, params: dict | None = None, timeout: int = 10) -> Any:
    url = f"{_SP_BASE}{path}"
    try:
        r = requests.get(url, headers=_HEADERS, params=params,
                         timeout=timeout, allow_redirects=True)
        if not r.ok:
            log.debug("SP HTTP %d → %s", r.status_code, url)
            return None
        return r.json()
    except Exception as exc:
        log.debug("SP HTTP error %s: %s", url, exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Outcome / market normalisation (self-contained, no sp_live_harvester dep)
# ─────────────────────────────────────────────────────────────────────────────

_SPORT_TOTAL_SLUG: dict[int, str] = {
    1: "over_under_goals",  5: "over_under_goals", 126: "over_under_goals",
    2: "total_points",      8: "total_points",
    4: "total_games",      13: "total_games",
    10: "total_sets",       9: "total_runs",
    23: "total_sets",       6: "over_under_goals",
    16: "total_games",     21: "total_runs",
}

_TYPE_TO_SLUG: dict[int, str] = {
    194: "1x2",                 149: "match_winner",
    147: "double_chance",       138: "btts",
    140: "first_half_btts",     145: "odd_even",
    166: "draw_no_bet",         151: "european_handicap",
    184: "asian_handicap",      183: "correct_score",
    154: "exact_goals",         135: "first_half_1x2",
    129: "first_team_to_score", 155: "highest_scoring_half",
    106: "first_half_over_under",
}

_POS_OUTCOME: dict[int, list[str]] = {
    194: ["1", "X", "2"],       149: ["1", "2"],
    147: ["1X", "X2", "12"],    138: ["yes", "no"],
    140: ["yes", "no"],         145: ["odd", "even"],
    166: ["1", "2"],            135: ["1", "X", "2"],
    155: ["1st", "2nd", "equal"],
    129: ["none", "1", "2"],    151: ["1", "X", "2"],
}

_DIRECT_KEYS: dict[str, str] = {
    "1": "1", "x": "X", "2": "2", "draw": "X",
    "over": "over", "under": "under",
    "yes": "yes", "no": "no",
    "odd": "odd", "even": "even",
    "1x": "1X", "x2": "X2", "12": "12",
    "home": "1", "away": "2",
    "ov": "over", "un": "under",
    "gg": "yes", "ng": "no",
    "eql": "equal", "none": "none",
    "1st": "1st", "2nd": "2nd",
    "p1": "1",  "p2": "2",
}


def _market_slug_from_type(market_type: int, sport_id: int, spec_val: str) -> str:
    if market_type == 105:
        base = _SPORT_TOTAL_SLUG.get(sport_id, "over_under")
    else:
        base = _TYPE_TO_SLUG.get(market_type, f"market_{market_type}")
    if spec_val and spec_val not in ("0.00", "0", ""):
        try:
            fv   = float(spec_val)
            line = str(int(fv)) if fv == int(fv) else str(fv)
            return f"{base}_{line}"
        except (ValueError, TypeError):
            pass
    return base


def _normalise_outcome_key(name: str, idx: int, market_type: int) -> str:
    nl = name.strip().lower()
    if nl in _DIRECT_KEYS:
        return _DIRECT_KEYS[nl]
    if nl.startswith("over"):
        return "over"
    if nl.startswith("under"):
        return "under"
    stripped = name.strip()
    if ":" in stripped and len(stripped) <= 5:
        return stripped
    if "/" in stripped and len(stripped) <= 5:
        return stripped
    ll = nl
    if " or draw" in ll or "or draw" in ll:
        return "1X" if ll.startswith(("home", "1", "local")) else "X2"
    if " or " in ll:
        parts = ll.split(" or ")
        if len(parts) == 2 and "draw" not in parts[0] and "draw" not in parts[1]:
            return "12"
    if "/" in name and 3 < len(name) < 30:
        parts = name.split("/")
        if len(parts) == 2:
            def _side(s: str) -> str:
                sl = s.strip().lower()
                if sl in ("draw", "x"):    return "X"
                if sl in ("1", "home"):    return "1"
                if sl in ("2", "away"):    return "2"
                return "?"
            h, a = _side(parts[0]), _side(parts[1])
            if h != "?" and a != "?":
                return f"{h}/{a}"
    pos_map = _POS_OUTCOME.get(market_type)
    if pos_map and idx < len(pos_map):
        return pos_map[idx]
    if market_type == 105:
        return "over" if idx == 0 else "under"
    slug = nl.replace(" ", "_").replace("-", "_")
    slug = "".join(c for c in slug if c.isalnum() or c == "_")
    return slug[:14] or f"sel_{idx}"


# ─────────────────────────────────────────────────────────────────────────────
# Event tracking (WS harvester populates these via sp_live_harvester)
# We read them safely — if the harvester isn't running we fall back to HTTP
# ─────────────────────────────────────────────────────────────────────────────

_sr_event_sport: dict[int, int] = {}   # {event_id: sport_id}
_sr_map_lock    = threading.Lock()


def _get_tracked_events() -> dict[int, int]:
    """
    Try to read the WS harvester's event→sport map.
    Falls back to empty dict if sp_live_harvester isn't loaded / running.
    """
    try:
        from app.workers.sp_live_harvester import _sr_event_sport as _wse, _sr_map_lock as _wsl
        with _wsl:
            return dict(_wse)
    except Exception:
        pass
    with _sr_map_lock:
        return dict(_sr_event_sport)


def _fetch_live_events_http(sport_id: int, limit: int = 200) -> list[dict]:
    """HTTP fallback: fetch live events for one sport."""
    raw = _sp_get(f"/api/live/sports/{sport_id}/events", params={"limit": limit})
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    return raw.get("events") or raw.get("data") or []


def _snapshot_all_sports_http() -> dict[int, list]:
    """HTTP snapshot of all live sports → {sport_id: [events]}."""
    result: dict[int, list] = {}
    for sid in _LIVE_SPORT_IDS:
        try:
            events = _fetch_live_events_http(sid)
            if events:
                result[sid] = events
        except Exception as exc:
            log.debug("snapshot sport %d: %s", sid, exc)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot helpers
# ─────────────────────────────────────────────────────────────────────────────

def _record_state_snapshot(r: _redis_lib.Redis, event_id: int, state: dict) -> None:
    try:
        r.set(f"sp:live:state:{event_id}",
              json.dumps({k: v for k, v in state.items() if k != "type"}), ex=300)
    except Exception:
        pass


def _record_market_snapshot(r: _redis_lib.Redis, event_id: int, payload: dict) -> None:
    try:
        slug = payload.get("market_slug", "unknown")
        r.set(f"sp:live:mkt:{event_id}:{slug}",
              json.dumps(payload), ex=300)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Helpers: build update payloads
# ─────────────────────────────────────────────────────────────────────────────

def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_selection(sel: dict, idx: int, mkt_type: int) -> dict:
    raw_name    = str(sel.get("name")      or sel.get("shortName") or "")
    short_name  = str(sel.get("shortName") or "")
    outcome_key = _normalise_outcome_key(short_name or raw_name, idx, mkt_type)
    return {
        "id":          sel.get("id"),
        "name":        raw_name,
        "shortName":   short_name,
        "outcome_key": outcome_key,
        "odds":        str(sel.get("odds") or "0"),
        "status":      sel.get("status") or "Open",
    }


def _build_market_update(
    event_id: int,
    sport_id: int,
    mkt:      dict,
    all_prev: dict,
) -> dict | None:
    mkt_type = int(mkt.get("id") or mkt.get("typeId") or 0)
    if not mkt_type:
        return None
    sv       = str(mkt.get("specialValue") or mkt.get("specValue") or "0.00")
    raw_sels = mkt.get("selections") or []
    if not raw_sels:
        return None

    slug      = _market_slug_from_type(mkt_type, sport_id, sv)
    norm_sels = [_parse_selection(s, i, mkt_type) for i, s in enumerate(raw_sels)]

    changed: list[dict] = []
    for ns in norm_sels:
        sel_id   = ns.get("id")
        new_odds = ns["odds"]
        if sel_id and new_odds and float(new_odds) > 1.0:
            prev = all_prev.get(sel_id)
            if prev is not None and prev != new_odds:
                changed.append({"id": sel_id, "odds": new_odds, "prev": prev})
            all_prev[sel_id] = new_odds

    return {
        "type":                  "market_update",
        "event_id":              event_id,
        "sport_id":              sport_id,
        "market_id":             mkt.get("eventMarketId") or mkt_type,
        "market_type":           mkt_type,
        "market_name":           mkt.get("name") or "",
        "market_slug":           slug,
        "handicap":              sv,
        "market_status":         mkt.get("status") or "Open",
        "all_selections":        [
            {"id": s.get("id"), "name": s.get("name"),
             "odds": str(s.get("odds") or "0"), "status": s.get("status") or "Open"}
            for s in raw_sels
        ],
        "normalised_selections": norm_sels,
        "changed_selections":    changed,
        "ts":                    _now_ts(),
        "source":                "details_poll",
    }


def _build_event_update(event_id: int, sport_id: int, raw: dict) -> dict:
    state = raw.get("state") or {}
    score = state.get("matchScore") or {}
    return {
        "type":          "event_update",
        "event_id":      event_id,
        "sport_id":      sport_id,
        "status":        raw.get("status") or "",
        "phase":         state.get("currentEventPhase") or "",
        "match_time":    state.get("matchTime") or "",
        "clock_running": state.get("clockRunning", True),
        "remaining_ms":  state.get("remainingTimeMillis"),
        "score_home":    str(score.get("home") or "0"),
        "score_away":    str(score.get("away") or "0"),
        "is_paused":     raw.get("isPaused") or False,
        "ts":            _now_ts(),
        "source":        "details_poll",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Core: fetch + parse + publish one event's details
# ─────────────────────────────────────────────────────────────────────────────

_prev_odds_cache: dict[int, dict] = {}   # {event_id: {sel_id: odds_str}}


def _fetch_and_publish_details(event_id: int, sport_id: int = 1) -> dict:
    """
    Fetch /api/live/events/{event_id}/details, parse all markets,
    publish event_update + market_update frames to Redis pub/sub.
    """
    t0  = time.perf_counter()
    r   = _get_redis()
    raw = _sp_get(f"/api/live/events/{event_id}/details", timeout=8)

    if not raw or not isinstance(raw, dict):
        return {"ok": False, "reason": "no data from SP", "event_id": event_id}

    actual_id = int(raw.get("id") or event_id)
    prev_odds = _prev_odds_cache.setdefault(actual_id, {})
    markets_raw = raw.get("markets") or []

    # ── Event state ──────────────────────────────────────────────────────────
    ev_payload = _build_event_update(actual_id, sport_id, raw)
    try:
        _publish_live(r, sport_id, actual_id, ev_payload)
        _record_state_snapshot(r, actual_id, ev_payload)
    except Exception as exc:
        log.debug("event_update publish %d: %s", actual_id, exc)

    # ── Markets ───────────────────────────────────────────────────────────────
    published = 0
    for mkt in markets_raw:
        payload = _build_market_update(actual_id, sport_id, mkt, prev_odds)
        if not payload:
            continue
        try:
            _publish_live(r, sport_id, actual_id, payload)
            _record_market_snapshot(r, actual_id, payload)
            published += 1
        except Exception as exc:
            log.debug("market_update publish %d: %s", actual_id, exc)

    return {
        "ok":                True,
        "event_id":          actual_id,
        "sport_id":          sport_id,
        "markets_fetched":   len(markets_raw),
        "markets_published": published,
        "phase":             ev_payload.get("phase", ""),
        "score":             f"{ev_payload['score_home']}-{ev_payload['score_away']}",
        "latency_ms":        int((time.perf_counter() - t0) * 1000),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Celery tasks — per-event + all-events fan-out
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(
    name="tasks.live.sp_poll_event_details",
    bind=True,
    max_retries=2,
    soft_time_limit=10,
    time_limit=15,
    acks_late=True,
)
def sp_poll_event_details(self, event_id: int, sport_id: int = 1) -> dict:
    try:
        return _fetch_and_publish_details(event_id, sport_id)
    except Exception as exc:
        log.warning("sp_poll_event_details %d: %s", event_id, exc)
        raise self.retry(exc=exc, countdown=3)


@celery.task(
    name="tasks.sp.poll_all_event_details",
    soft_time_limit=25,
    time_limit=30,
    acks_late=True,
)
def sp_poll_all_event_details() -> dict:
    """
    Beat task (every 5 s): poll SP event-details for every tracked live event.
    Reads event→sport map from WS harvester; falls back to HTTP fetch.
    """
    t0 = time.perf_counter()
    ev_sport = _get_tracked_events()

    if not ev_sport:
        for sport_id in _LIVE_SPORT_IDS:
            try:
                events = _fetch_live_events_http(sport_id, limit=200)
                for ev in events:
                    eid = ev.get("id")
                    if eid:
                        ev_sport[int(eid)] = sport_id
            except Exception:
                pass

    if not ev_sport:
        return {"ok": True, "polled": 0, "reason": "no tracked events"}

    polled = 0
    errors = 0
    for event_id, sport_id in ev_sport.items():
        try:
            _fetch_and_publish_details(int(event_id), sport_id)
            polled += 1
        except Exception as exc:
            log.debug("poll detail ev=%d: %s", event_id, exc)
            errors += 1
        time.sleep(0.05)

    return {
        "ok":        True,
        "polled":    polled,
        "errors":    errors,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SSE generator — used by sp_live_view.stream_event_live()
# ─────────────────────────────────────────────────────────────────────────────

def stream_event_details_sse(
    event_id: int,
    sport_id: int = 1,
) -> Generator[str, None, None]:
    """
    SSE generator combining:
      Layer 1 — Redis pub/sub sp:live:event:{id}  (instant)
      Layer 2 — Direct HTTP poll every 8 s        (fallback)
    """
    r       = _get_redis()
    channel = f"sp:live:event:{event_id}"

    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

    def _ka() -> str:
        return f": keep-alive {_now_ts()}\n\n"

    yield _sse({
        "type":     "connected",
        "event_id": event_id,
        "sport_id": sport_id,
        "channel":  channel,
        "ts":       _now_ts(),
    })

    # Immediate snapshot on connect
    try:
        _fetch_and_publish_details(event_id, sport_id)
        yield _sse({"type": "snapshot_done", "event_id": event_id, "ts": _now_ts()})
    except Exception as exc:
        log.debug("initial poll %d: %s", event_id, exc)

    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(channel)

    last_ka     = time.monotonic()
    last_direct = time.monotonic()
    KA_SEC      = 15.0
    DIRECT_SEC  = 8.0

    try:
        while True:
            msg = pubsub.get_message(timeout=0.5)
            if msg and msg.get("type") == "message":
                try:
                    yield _sse(json.loads(msg["data"]))
                except Exception:
                    pass

            now = time.monotonic()

            if now - last_ka >= KA_SEC:
                yield _ka()
                last_ka = now

            if now - last_direct >= DIRECT_SEC:
                try:
                    _fetch_and_publish_details(event_id, sport_id)
                except Exception as exc:
                    log.debug("fallback poll %d: %s", event_id, exc)
                last_direct = now

    except GeneratorExit:
        pass
    finally:
        try:
            pubsub.unsubscribe(channel)
            pubsub.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Live snapshot tasks
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(name="tasks.sp.harvest_all_live", soft_time_limit=50, time_limit=60, acks_late=True)
def sp_harvest_all_live() -> dict:
    """Beat (every 60 s): snapshot all SP live sports into Redis."""
    try:
        # Prefer WS harvester's snapshot_all_sports if available
        try:
            from app.workers.sp_live_harvester import snapshot_all_sports
            result = snapshot_all_sports()
            total  = sum(len(v) for v in result.values())
            return {"ok": True, "sports": len(result), "events": total}
        except ImportError:
            pass

        # HTTP fallback
        r      = _get_redis()
        result = _snapshot_all_sports_http()
        total  = 0
        for sid, events in result.items():
            r.set(f"sp:live:snapshot:{sid}", json.dumps({"events": events}), ex=90)
            total += len(events)
        return {"ok": True, "sports": len(result), "events": total}
    except Exception as exc:
        log.error("sp_harvest_all_live: %s", exc)
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.bt.harvest_all_live", soft_time_limit=30, time_limit=40, acks_late=True)
def bt_harvest_all_live() -> dict:
    """Beat (every 90 s): snapshot Betika live matches into Redis."""
    try:
        from app.workers.bt_harvester import (
            fetch_live_matches, _LIVE_DATA_KEY,
        )
        r        = _get_redis()
        matches  = fetch_live_matches()
        by_sport: dict[int, list] = {}
        for m in matches:
            by_sport.setdefault(m["bt_sport_id"], []).append(m)
        for bt_sport_id, sport_matches in by_sport.items():
            r.set(_LIVE_DATA_KEY.format(sport_id=bt_sport_id),
                  json.dumps(sport_matches, ensure_ascii=False), ex=60)
        return {"ok": True, "sports": len(by_sport), "events": len(matches)}
    except Exception as exc:
        log.error("bt_harvest_all_live: %s", exc)
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.od.harvest_all_live", soft_time_limit=60, time_limit=75, acks_late=True)
def od_harvest_all_live() -> dict:
    """Beat (every 90 s): snapshot OdiBets live matches into Redis."""
    try:
        from app.workers.od_harvester import (
            fetch_live_matches, get_live_poller, init_live_poller, _LIVE_DATA_KEY,
        )
        r = _get_redis()
        if get_live_poller() is None or not get_live_poller().alive:
            try:
                init_live_poller(r, interval=2.0)
            except Exception as pe:
                log.debug("od poller init: %s", pe)

        matches  = fetch_live_matches()
        by_sport: dict[int, list] = {}
        for m in matches:
            by_sport.setdefault(m["od_sport_id"], []).append(m)
        for od_sport_id, sport_matches in by_sport.items():
            r.set(_LIVE_DATA_KEY.format(sport_id=od_sport_id),
                  json.dumps(sport_matches, ensure_ascii=False), ex=60)
        return {"ok": True, "sports": len(by_sport), "events": len(matches)}
    except Exception as exc:
        log.error("od_harvest_all_live: %s", exc)
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.b2b.harvest_all_live",      soft_time_limit=30, time_limit=40)
def b2b_harvest_all_live() -> dict:
    return {"ok": True, "note": "b2b live stub"}


@celery.task(name="tasks.b2b_page.harvest_all_live", soft_time_limit=30, time_limit=40)
def b2b_page_harvest_all_live() -> dict:
    return {"ok": True, "note": "b2b_page live stub"}


@celery.task(name="tasks.sbo.harvest_all_live",      soft_time_limit=30, time_limit=40)
def sbo_harvest_all_live() -> dict:
    return {"ok": True, "note": "sbo live stub"}


# ─────────────────────────────────────────────────────────────────────────────
# Upcoming harvest tasks
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(name="tasks.sp.harvest_all_upcoming", soft_time_limit=120, time_limit=150)
def sp_harvest_all_upcoming() -> dict:
    try:
        from app.workers.sp_harvester import fetch_upcoming
        SPORTS = ["soccer", "basketball", "tennis", "ice-hockey", "volleyball",
                  "rugby", "handball", "cricket", "table-tennis", "darts", "mma"]
        total = 0
        for slug in SPORTS:
            try:
                matches = fetch_upcoming(slug)
                if matches:
                    cache_set(f"sp:upcoming:{slug}",
                              {"matches": matches, "harvested_at": _now_ts()}, ttl=360)
                    total += len(matches)
            except Exception as exc:
                log.debug("sp_upcoming %s: %s", slug, exc)
        return {"ok": True, "total": total}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.bt.harvest_all_upcoming", soft_time_limit=180, time_limit=200)
def bt_harvest_all_upcoming() -> dict:
    try:
        from app.workers.bt_harvester import fetch_upcoming_matches, cache_upcoming, SLUG_TO_BT_SPORT
        r     = _get_redis()
        total = 0
        for slug in list(SLUG_TO_BT_SPORT.keys())[:8]:
            try:
                matches = fetch_upcoming_matches(sport_slug=slug, max_pages=5)
                if matches:
                    cache_upcoming(r, slug, matches, ttl=360)
                    total += len(matches)
            except Exception as exc:
                log.debug("bt_upcoming %s: %s", slug, exc)
        return {"ok": True, "total": total}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.od.harvest_all_upcoming", soft_time_limit=300, time_limit=330, acks_late=True)
def od_harvest_all_upcoming() -> dict:
    try:
        from app.workers.od_harvester import fetch_upcoming_matches, cache_upcoming
        r      = _get_redis()
        SPORTS = ["soccer", "basketball", "tennis", "volleyball",
                  "handball", "rugby", "cricket", "table-tennis"]
        total  = 0
        for slug in SPORTS:
            try:
                matches = fetch_upcoming_matches(sport_slug=slug)
                if matches:
                    cache_upcoming(r, slug, matches, ttl=420)
                    total += len(matches)
            except Exception as exc:
                log.warning("od_upcoming %s: %s", slug, exc)
        return {"ok": True, "total": total, "sports": len(SPORTS)}
    except Exception as exc:
        log.error("od_harvest_all_upcoming: %s", exc)
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.b2b.harvest_all_upcoming",      soft_time_limit=60, time_limit=70)
def b2b_harvest_all_upcoming() -> dict:
    return {"ok": True, "note": "b2b upcoming stub"}


@celery.task(name="tasks.b2b_page.harvest_all_upcoming", soft_time_limit=60, time_limit=70)
def b2b_page_harvest_all_upcoming() -> dict:
    return {"ok": True, "note": "b2b_page upcoming stub"}


@celery.task(name="tasks.sbo.harvest_all_upcoming",      soft_time_limit=60, time_limit=70)
def sbo_harvest_all_upcoming() -> dict:
    return {"ok": True, "note": "sbo upcoming stub"}