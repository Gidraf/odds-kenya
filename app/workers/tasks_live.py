"""
app/workers/tasks_live.py  v2
==============================
Changes from v1
───────────────
NEW  ensure_harvester_running()  — beat watchdog (60 s): starts/restarts the
     SP WebSocket harvester thread if it's dead.  Safe to call repeatedly.

NEW  live_cross_bk_refresh()     — beat task (15 s): for every tracked live
     match that has a betradar_id, triggers LiveCrossBkUpdater so BT + OD
     prices stay fresh between SP WebSocket bursts.

NEW  live_snapshot_to_db()       — beat task (30 s): writes current Redis
     live snapshots back to UnifiedMatch so the REST /api/odds/live/* endpoint
     always has fresh data even when WS is quiet.

UPD  sp_poll_all_event_details() — now also calls live_cross_bk_refresh path
     after publishing SP event details, ensuring BT/OD sync on every SP tick.

UPD  sp_harvest_all_live()       — auto-starts harvester thread if not running.

UPD  bt_harvest_all_live() /     — push parsed matches to entity_resolver
     od_harvest_all_live()         so live BT/OD odds appear in unified_matches.

UNCHANGED  stream_event_details_sse(), sp_poll_event_details(),
           b2b_*/sbo_* stubs, *_harvest_all_upcoming tasks.
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

from app.workers.celery_tasks import celery, _publish, cache_set, cache_get

log = logging.getLogger("tasks_live")

# ─────────────────────────────────────────────────────────────────────────────
# Redis helpers
# ─────────────────────────────────────────────────────────────────────────────

_redis_client: _redis_lib.Redis | None = None
_redis_lock   = threading.Lock()


def _get_redis(db: int = 1) -> _redis_lib.Redis:
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    with _redis_lock:
        if _redis_client is None:
            url  = os.environ.get("REDIS_URL",
                   os.environ.get("CELERY_BROKER_URL", "redis://localhost:6382/0"))
            base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
            _redis_client = _redis_lib.Redis.from_url(
                f"{base}/{db}",
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                retry_on_timeout=True,
            )
    return _redis_client


CH_ALL = "sp:live:all"

def _ch_sport(sport_id: int) -> str: return f"sp:live:sport:{sport_id}"
def _ch_event(event_id: int) -> str: return f"sp:live:event:{event_id}"

def _publish_live(r: _redis_lib.Redis, sport_id: int, event_id: int, payload: dict) -> None:
    raw = json.dumps(payload, ensure_ascii=False)
    try:
        pipe = r.pipeline()
        pipe.publish(CH_ALL,              raw)
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
    "User-Agent":       "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36",
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Timezone":   "Africa/Nairobi",
    "Origin":           _SP_BASE,
    "Referer":          _SP_BASE + "/en/live/",
}
_LIVE_SPORT_IDS = [1, 2, 3, 5, 6, 7, 8, 9, 13]

# sport_id → slug mapping for cross-BK trigger
_SPORT_ID_TO_SLUG: dict[int, str] = {
    1: "soccer",  2: "basketball", 3: "tennis",
    5: "ice-hockey", 6: "volleyball", 7: "handball",
    8: "basketball", 9: "cricket", 13: "table-tennis",
}


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
# Outcome / market normalisation  (unchanged from v1)
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
    194: "1x2",       149: "match_winner", 147: "double_chance",
    138: "btts",      140: "first_half_btts", 145: "odd_even",
    166: "draw_no_bet", 151: "european_handicap", 184: "asian_handicap",
    183: "correct_score", 154: "exact_goals", 135: "first_half_1x2",
    129: "first_team_to_score", 155: "highest_scoring_half",
    106: "first_half_over_under",
}
_POS_OUTCOME: dict[int, list[str]] = {
    194: ["1","X","2"], 149: ["1","2"], 147: ["1X","X2","12"],
    138: ["yes","no"], 140: ["yes","no"], 145: ["odd","even"],
    166: ["1","2"],    135: ["1","X","2"],
    155: ["1st","2nd","equal"], 129: ["none","1","2"], 151: ["1","X","2"],
}
_DIRECT_KEYS: dict[str, str] = {
    "1":"1","x":"X","2":"2","draw":"X","over":"over","under":"under",
    "yes":"yes","no":"no","odd":"odd","even":"even",
    "1x":"1X","x2":"X2","12":"12","home":"1","away":"2",
    "ov":"over","un":"under","gg":"yes","ng":"no",
    "eql":"equal","none":"none","1st":"1st","2nd":"2nd","p1":"1","p2":"2",
}


def _market_slug_from_type(market_type: int, sport_id: int, spec_val: str) -> str:
    if market_type == 105:
        base = _SPORT_TOTAL_SLUG.get(sport_id, "over_under")
    else:
        base = _TYPE_TO_SLUG.get(market_type, f"market_{market_type}")
    if spec_val and spec_val not in ("0.00","0",""):
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
    if nl.startswith("over"):  return "over"
    if nl.startswith("under"): return "under"
    pos_map = _POS_OUTCOME.get(market_type)
    if pos_map and idx < len(pos_map):
        return pos_map[idx]
    if market_type == 105:
        return "over" if idx == 0 else "under"
    slug = nl.replace(" ","_").replace("-","_")
    slug = "".join(c for c in slug if c.isalnum() or c == "_")
    return slug[:14] or f"sel_{idx}"


# ─────────────────────────────────────────────────────────────────────────────
# Tracked events  (populated by WS harvester + HTTP fallback)
# ─────────────────────────────────────────────────────────────────────────────

_sr_event_sport: dict[int, int] = {}
_sr_map_lock    = threading.Lock()


def _get_tracked_events() -> dict[int, int]:
    """Return {event_id: sport_id} map from WS harvester or local fallback."""
    try:
        from app.workers.sp_live_harvester import _sr_event_sport as _wse, _sr_map_lock as _wsl
        with _wsl:
            return dict(_wse)
    except Exception:
        pass
    with _sr_map_lock:
        return dict(_sr_event_sport)


def _fetch_live_events_http(sport_id: int, limit: int = 200) -> list[dict]:
    raw = _sp_get(f"/api/live/sports/{sport_id}/events", params={"limit": limit})
    if not raw:
        return []
    return raw if isinstance(raw, list) else raw.get("events") or raw.get("data") or []


def _snapshot_all_sports_http() -> dict[int, list]:
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
# betradar_id resolution (event_id → betradar_id)
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_betradar_id(sp_event_id: int) -> str | None:
    """
    sp_event_id → betradar_id.
    Check order:
      1. sp_live_harvester Redis cache  sp:event_betradar:{event_id}
      2. BookmakerMatchLink table via external_match_id
      3. SP event details API (parent_match_id field)
    """
    eid_str = str(sp_event_id)

    # 1. Harvester Redis cache (fastest)
    try:
        r   = _get_redis()
        val = r.get(f"sp:event_betradar:{eid_str}")
        if val and val not in ("0","null","None",""):
            return val
    except Exception:
        pass

    # 2. DB lookup
    try:
        from app.models.bookmakers_model import BookmakerMatchLink
        links = BookmakerMatchLink.query.filter_by(
            external_match_id=eid_str
        ).all()
        for link in links:
            if link.betradar_id and link.betradar_id not in ("0","null"):
                # Cache it
                try: _get_redis().set(f"sp:event_betradar:{eid_str}", link.betradar_id, ex=3600)
                except Exception: pass
                return link.betradar_id
    except Exception as exc:
        log.debug("betradar lookup ev=%d: %s", sp_event_id, exc)

    # 3. SP API fallback (parent_match_id)
    try:
        raw = _sp_get(f"/api/live/events/{sp_event_id}/details", timeout=5)
        if raw and isinstance(raw, dict):
            br_id = str(raw.get("betradarId") or raw.get("parent_match_id") or "")
            if br_id and br_id not in ("0","null","None",""):
                try: _get_redis().set(f"sp:event_betradar:{eid_str}", br_id, ex=3600)
                except Exception: pass
                return br_id
    except Exception:
        pass

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Build payloads (unchanged from v1)
# ─────────────────────────────────────────────────────────────────────────────

def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_selection(sel: dict, idx: int, mkt_type: int) -> dict:
    raw_name   = str(sel.get("name") or sel.get("shortName") or "")
    short_name = str(sel.get("shortName") or "")
    outcome_key = _normalise_outcome_key(short_name or raw_name, idx, mkt_type)
    return {
        "id": sel.get("id"), "name": raw_name, "shortName": short_name,
        "outcome_key": outcome_key,
        "odds": str(sel.get("odds") or "0"),
        "status": sel.get("status") or "Open",
    }


def _build_market_update(event_id: int, sport_id: int, mkt: dict, all_prev: dict) -> dict | None:
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
        "type": "market_update", "event_id": event_id, "sport_id": sport_id,
        "market_type": mkt_type, "market_name": mkt.get("name") or "",
        "market_slug": slug, "handicap": sv,
        "market_status": mkt.get("status") or "Open",
        "all_selections": [
            {"id": s.get("id"), "name": s.get("name"),
             "odds": str(s.get("odds") or "0"), "status": s.get("status") or "Open"}
            for s in raw_sels
        ],
        "normalised_selections": norm_sels,
        "changed_selections": changed,
        "ts": _now_ts(), "source": "details_poll",
    }


def _build_event_update(event_id: int, sport_id: int, raw: dict) -> dict:
    state = raw.get("state") or {}
    score = state.get("matchScore") or {}
    return {
        "type": "event_update", "event_id": event_id, "sport_id": sport_id,
        "status": raw.get("status") or "",
        "phase": state.get("currentEventPhase") or "",
        "match_time": state.get("matchTime") or "",
        "clock_running": state.get("clockRunning", True),
        "remaining_ms": state.get("remainingTimeMillis"),
        "score_home": str(score.get("home") or "0"),
        "score_away": str(score.get("away") or "0"),
        "is_paused": raw.get("isPaused") or False,
        "ts": _now_ts(), "source": "details_poll",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Core: fetch + parse + publish one event's details
# ─────────────────────────────────────────────────────────────────────────────

_prev_odds_cache: dict[int, dict] = {}


def _fetch_and_publish_details(event_id: int, sport_id: int = 1) -> dict:
    t0  = time.perf_counter()
    r   = _get_redis()
    raw = _sp_get(f"/api/live/events/{event_id}/details", timeout=8)

    if not raw or not isinstance(raw, dict):
        return {"ok": False, "reason": "no data from SP", "event_id": event_id}

    actual_id  = int(raw.get("id") or event_id)
    prev_odds  = _prev_odds_cache.setdefault(actual_id, {})
    markets_raw = raw.get("markets") or []

    ev_payload = _build_event_update(actual_id, sport_id, raw)
    try:
        _publish_live(r, sport_id, actual_id, ev_payload)
        r.set(f"sp:live:state:{actual_id}",
              json.dumps({k:v for k,v in ev_payload.items() if k!="type"}), ex=300)
    except Exception as exc:
        log.debug("event_update publish %d: %s", actual_id, exc)

    published = 0
    for mkt in markets_raw:
        payload = _build_market_update(actual_id, sport_id, mkt, prev_odds)
        if not payload:
            continue
        try:
            _publish_live(r, sport_id, actual_id, payload)
            r.set(f"sp:live:mkt:{actual_id}:{payload['market_slug']}",
                  json.dumps(payload), ex=300)
            published += 1
        except Exception as exc:
            log.debug("market_update publish %d: %s", actual_id, exc)

    # Cache betradar_id while we have the raw response
    br_id = str(raw.get("betradarId") or raw.get("parent_match_id") or "")
    if br_id and br_id not in ("0","null","None",""):
        try: r.set(f"sp:event_betradar:{actual_id}", br_id, ex=3600)
        except Exception: pass

    return {
        "ok": True, "event_id": actual_id, "sport_id": sport_id,
        "markets_fetched": len(markets_raw), "markets_published": published,
        "phase": ev_payload.get("phase",""),
        "score": f"{ev_payload['score_home']}-{ev_payload['score_away']}",
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    }


# ═════════════════════════════════════════════════════════════════════════════
# CELERY TASKS
# ═════════════════════════════════════════════════════════════════════════════

# ── Harvester watchdog ────────────────────────────────────────────────────────

@celery.task(
    name="tasks.live.ensure_harvester",
    soft_time_limit=15, time_limit=20, acks_late=True,
)
def ensure_harvester_running() -> dict:
    """
    Beat task (60 s): Ensure the SP WebSocket harvester thread is alive.
    Starts it if missing; restarts if it died.
    Safe to call multiple times — idempotent.
    """
    try:
        from app.workers.sp_live_harvester import (
            start_harvester_thread, stop_harvester,
            get_harvester_status,
        )
        status = get_harvester_status()
        if status.get("running") and status.get("ws_connected"):
            return {"ok": True, "action": "already_running", **status}

        if status.get("running") and not status.get("ws_connected"):
            # Thread alive but WS dropped — stop cleanly then restart
            log.warning("[harvester-watchdog] WS disconnected — restarting")
            try:
                stop_harvester()
                time.sleep(1)
            except Exception:
                pass

        log.info("[harvester-watchdog] starting SP live harvester thread")
        start_harvester_thread()
        return {"ok": True, "action": "started"}
    except ImportError:
        log.debug("[harvester-watchdog] sp_live_harvester not available")
        return {"ok": True, "action": "skipped", "reason": "module_not_found"}
    except Exception as exc:
        log.error("[harvester-watchdog] %s", exc)
        return {"ok": False, "error": str(exc)}


# ── Cross-BK live enrichment ──────────────────────────────────────────────────

@celery.task(
    name="tasks.live.cross_bk_refresh",
    soft_time_limit=25, time_limit=30, acks_late=True,
)
def live_cross_bk_refresh() -> dict:
    """
    Beat task (15 s): For every tracked live match, trigger LiveCrossBkUpdater
    so BT + OD prices are fetched using betradar_id.

    This bridges the gap between SP WebSocket events — ensuring cross-BK prices
    stay fresh even in quiet periods.  The updater deduplicates in-flight
    requests so rapid calls are safe.
    """
    t0 = time.perf_counter()
    try:
        from app.workers.live_cross_bk_updater import get_updater
    except ImportError:
        return {"ok": True, "triggered": 0, "reason": "updater_module_not_found"}

    ev_sport = _get_tracked_events()

    # If WS harvester has no events, fall back to HTTP to discover them
    if not ev_sport:
        for sport_id in _LIVE_SPORT_IDS[:4]:   # only top sports to stay under 25 s
            try:
                events = _fetch_live_events_http(sport_id, limit=100)
                for ev in events:
                    eid = ev.get("id")
                    if eid:
                        ev_sport[int(eid)] = sport_id
            except Exception:
                pass

    if not ev_sport:
        return {"ok": True, "triggered": 0, "reason": "no_live_events"}

    updater   = get_updater()
    triggered = 0
    skipped   = 0
    errors    = 0

    for event_id, sport_id in ev_sport.items():
        try:
            betradar_id = _resolve_betradar_id(event_id)
            if not betradar_id:
                skipped += 1
                continue
            sport_slug = _SPORT_ID_TO_SLUG.get(sport_id, "soccer")
            updater.trigger(betradar_id, sport_slug=sport_slug)
            triggered += 1
        except Exception as exc:
            log.debug("[cross_bk_refresh] ev=%d: %s", event_id, exc)
            errors += 1

    return {
        "ok":        True,
        "triggered": triggered,
        "skipped":   skipped,
        "errors":    errors,
        "total":     len(ev_sport),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    }


# ── Live snapshot → DB sync ───────────────────────────────────────────────────

@celery.task(
    name="tasks.live.snapshot_to_db",
    soft_time_limit=55, time_limit=60, acks_late=True,
)
def live_snapshot_to_db() -> dict:
    """
    Beat task (30 s): Write current Redis live snapshots into unified_matches
    so the REST /api/odds/live/* endpoint always has fresh status + scores
    even if SSE consumers aren't subscribed.
    """
    from app.extensions import db
    from app.models.odds_model import UnifiedMatch, MatchStatus
    r       = _get_redis()
    updated = 0
    errors  = 0

    for sport_id in _LIVE_SPORT_IDS:
        try:
            raw = r.get(f"sp:live:snapshot:{sport_id}")
            if not raw:
                continue
            data = json.loads(raw)
            events = data if isinstance(data, list) else data.get("events") or []
            for ev in events:
                eid     = ev.get("id")
                br_id   = str(ev.get("betradarId") or ev.get("parent_match_id") or "")
                state   = ev.get("state") or {}
                score   = state.get("matchScore") or {}
                if not br_id or br_id in ("0","null"):
                    continue
                try:
                    um = UnifiedMatch.query.filter_by(
                        parent_match_id=br_id
                    ).with_for_update().first()
                    if not um:
                        continue
                    um.status = MatchStatus.LIVE
                    sh = score.get("home")
                    sa = score.get("away")
                    if sh is not None: um.score_home = int(sh)
                    if sa is not None: um.score_away = int(sa)
                    updated += 1
                except Exception as exc:
                    log.debug("snapshot_to_db %s: %s", br_id, exc)
                    errors += 1
        except Exception as exc:
            log.debug("snapshot_to_db sport %d: %s", sport_id, exc)

    try:
        db.session.commit()
    except Exception as exc:
        log.error("snapshot_to_db commit: %s", exc)
        db.session.rollback()

    return {"ok": True, "updated": updated, "errors": errors}


# ── SP event-detail poll ──────────────────────────────────────────────────────

@celery.task(
    name="tasks.live.sp_poll_event_details",
    bind=True, max_retries=2, soft_time_limit=10, time_limit=15, acks_late=True,
)
def sp_poll_event_details(self, event_id: int, sport_id: int = 1) -> dict:
    try:
        return _fetch_and_publish_details(event_id, sport_id)
    except Exception as exc:
        log.warning("sp_poll_event_details %d: %s", event_id, exc)
        raise self.retry(exc=exc, countdown=3)


@celery.task(
    name="tasks.sp.poll_all_event_details",
    soft_time_limit=25, time_limit=30, acks_late=True,
)
def sp_poll_all_event_details() -> dict:
    """
    Beat task (5 s): poll SP event-details for every tracked live event.
    After publishing SP prices, also triggers cross-BK enrichment so
    BT + OD prices are updated in the same tick.
    """
    t0       = time.perf_counter()
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

    polled = errors = 0

    # Import updater once outside the loop
    updater = None
    try:
        from app.workers.live_cross_bk_updater import get_updater
        updater = get_updater()
    except ImportError:
        pass

    for event_id, sport_id in ev_sport.items():
        try:
            result = _fetch_and_publish_details(int(event_id), sport_id)
            polled += 1
            # Trigger cross-BK enrichment if we got a betradar_id back
            if updater and result.get("ok"):
                br_id = _resolve_betradar_id(event_id)
                if br_id:
                    sport_slug = _SPORT_ID_TO_SLUG.get(sport_id, "soccer")
                    updater.trigger(br_id, sport_slug=sport_slug)
        except Exception as exc:
            log.debug("poll detail ev=%d: %s", event_id, exc)
            errors += 1
        time.sleep(0.05)

    return {
        "ok": True, "polled": polled, "errors": errors,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    }


# ── SP live snapshot ──────────────────────────────────────────────────────────

@celery.task(
    name="tasks.sp.harvest_all_live",
    soft_time_limit=50, time_limit=60, acks_late=True,
)
def sp_harvest_all_live() -> dict:
    """
    Beat task (60 s): snapshot all SP live sports into Redis +
    ensure WS harvester thread is alive.
    """
    # Ensure harvester is running (non-Celery call — just calls the function)
    try:
        from app.workers.sp_live_harvester import (
            start_harvester_thread, get_harvester_status,
        )
        status = get_harvester_status()
        if not status.get("running") or not status.get("ws_connected"):
            log.info("[sp_live] starting harvester thread from harvest task")
            start_harvester_thread()
    except ImportError:
        pass
    except Exception as exc:
        log.warning("[sp_live] harvester start: %s", exc)

    # HTTP snapshot (parallel source for match discovery)
    try:
        r      = _get_redis()
        result = _snapshot_all_sports_http()
        total  = 0
        for sid, events in result.items():
            r.set(f"sp:live:snapshot:{sid}",
                  json.dumps({"events": events, "ts": _now_ts()}), ex=90)
            
            try:
                sport_slug = _SPORT_ID_TO_SLUG.get(sid, "soccer")
                _publish("odds:updates", {
                    "event": "odds_updated", "source": "sportpesa",
                    "sport": sport_slug, "mode": "live",
                    "count": len(events), "ts": _now_ts()
                })
            except Exception:
                pass

            total += len(events)

            # Register events in local tracking map for poll task
            with _sr_map_lock:
                for ev in events:
                    eid = ev.get("id")
                    if eid:
                        _sr_event_sport[int(eid)] = sid

        return {"ok": True, "sports": len(result), "events": total}
    except Exception as exc:
        log.error("sp_harvest_all_live: %s", exc)
        return {"ok": False, "error": str(exc)}


# ── BT live harvest ───────────────────────────────────────────────────────────

@celery.task(
    name="tasks.bt.harvest_all_live",
    soft_time_limit=30, time_limit=40, acks_late=True,
)
def bt_harvest_all_live() -> dict:
    """
    Beat task (90 s): fetch all Betika live matches and push to entity_resolver
    so they appear in unified_matches with BT prices.
    """
    try:
        from app.workers.bt_harvester import fetch_live_matches
        from app.workers.celery_tasks import _persist_bk_matches

        matches = fetch_live_matches()
        if not matches:
            return {"ok": True, "events": 0}

        r = _get_redis()

        # Group by sport
        by_sport: dict[str, list] = {}
        for m in matches:
            slug = m.get("sport_slug") or "soccer"
            by_sport.setdefault(slug, []).append(m)

        total = 0
        for slug, sport_matches in by_sport.items():
            try:
                # 1. UPDATE REDIS & CACHE BEFORE DB PERSISTENCE
                r.setex(f"bt:live:{slug}", 300, json.dumps({
                    "source": "betika", "sport": slug, "mode": "live",
                    "match_count": len(sport_matches), "harvested_at": _now_ts(),
                    "matches": sport_matches,
                }))

                try:
                    from app.workers.redis_bus import publish_snapshot
                    publish_snapshot("bt", "live", slug, sport_matches, meta={"source": "betika"})
                except ImportError:
                    pass

                _publish("odds:updates", {
                    "event": "odds_updated", "source": "betika",
                    "sport": slug, "mode": "live",
                    "count": len(sport_matches), "ts": _now_ts()
                })

                # 2. PERSIST TO DB
                _persist_bk_matches(sport_matches, "bt", slug)
                total += len(sport_matches)
            except Exception as exc:
                log.warning("[bt_live] persist %s: %s", slug, exc)

        return {"ok": True, "sports": len(by_sport), "events": total}
    except ImportError as exc:
        log.debug("[bt_live] import: %s", exc)
        return {"ok": True, "events": 0, "reason": str(exc)}
    except Exception as exc:
        log.error("bt_harvest_all_live: %s", exc)
        return {"ok": False, "error": str(exc)}


# ── OD live harvest ───────────────────────────────────────────────────────────

@celery.task(
    name="tasks.od.harvest_all_live",
    soft_time_limit=60, time_limit=75, acks_late=True,
)
def od_harvest_all_live() -> dict:
    """
    Beat task (90 s): fetch all OdiBets live matches and push to entity_resolver.
    """
    try:
        from app.workers.od_harvester import fetch_live_matches
        from app.workers.celery_tasks import _persist_bk_matches

        matches = fetch_live_matches()
        if not matches:
            return {"ok": True, "events": 0}

        r = _get_redis()

        by_sport: dict[str, list] = {}
        for m in matches:
            slug = m.get("sport_slug") or "soccer"
            by_sport.setdefault(slug, []).append(m)

        total = 0
        for slug, sport_matches in by_sport.items():
            try:
                # 1. UPDATE REDIS & CACHE BEFORE DB PERSISTENCE
                r.setex(f"od:live:{slug}", 300, json.dumps({
                    "source": "odibets", "sport": slug, "mode": "live",
                    "match_count": len(sport_matches), "harvested_at": _now_ts(),
                    "matches": sport_matches,
                }))

                try:
                    from app.workers.redis_bus import publish_snapshot
                    publish_snapshot("od", "live", slug, sport_matches, meta={"source": "odibets"})
                except ImportError:
                    pass

                _publish("odds:updates", {
                    "event": "odds_updated", "source": "odibets",
                    "sport": slug, "mode": "live",
                    "count": len(sport_matches), "ts": _now_ts()
                })

                # 2. PERSIST TO DB
                _persist_bk_matches(sport_matches, "od", slug)
                total += len(sport_matches)
            except Exception as exc:
                log.warning("[od_live] persist %s: %s", slug, exc)

        return {"ok": True, "sports": len(by_sport), "events": total}
    except ImportError as exc:
        log.debug("[od_live] import: %s", exc)
        return {"ok": True, "events": 0, "reason": str(exc)}
    except Exception as exc:
        log.error("od_harvest_all_live: %s", exc)
        return {"ok": False, "error": str(exc)}


# ── B2B / SBO stubs ───────────────────────────────────────────────────────────

@celery.task(name="tasks.b2b.harvest_all_live",      soft_time_limit=30, time_limit=40)
def b2b_harvest_all_live() -> dict:
    return {"ok": True, "note": "b2b live stub"}

@celery.task(name="tasks.b2b_page.harvest_all_live", soft_time_limit=30, time_limit=40)
def b2b_page_harvest_all_live() -> dict:
    return {"ok": True, "note": "b2b_page live stub"}

@celery.task(name="tasks.sbo.harvest_all_live",      soft_time_limit=30, time_limit=40)
def sbo_harvest_all_live() -> dict:
    return {"ok": True, "note": "sbo live stub"}


# ── SSE generator  (unchanged from v1) ───────────────────────────────────────

def stream_event_details_sse(
    event_id: int,
    sport_id: int = 1,
) -> Generator[str, None, None]:
    r       = _get_redis()
    channel = f"sp:live:event:{event_id}"

    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    def _ka() -> str:
        return f": keep-alive {_now_ts()}\n\n"

    yield _sse({"type":"connected","event_id":event_id,"sport_id":sport_id,"channel":channel,"ts":_now_ts()})

    try:
        _fetch_and_publish_details(event_id, sport_id)
        yield _sse({"type":"snapshot_done","event_id":event_id,"ts":_now_ts()})
    except Exception as exc:
        log.debug("initial poll %d: %s", event_id, exc)

    pubsub  = r.pubsub(ignore_subscribe_messages=True)
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


# ── Upcoming tasks (unchanged from v1) ───────────────────────────────────────

@celery.task(name="tasks.sp.harvest_all_upcoming", soft_time_limit=120, time_limit=150)
def sp_harvest_all_upcoming() -> dict:
    try:
        from app.workers.sp_harvester import fetch_upcoming
        SPORTS = ["soccer","basketball","tennis","ice-hockey","volleyball",
                  "rugby","handball","cricket","table-tennis","darts","mma"]
        total = 0
        for slug in SPORTS:
            try:
                matches = fetch_upcoming(slug)
                if matches:
                    cache_set(f"sp:upcoming:{slug}",
                              {"matches":matches,"harvested_at":_now_ts()}, ttl=360)
                    total += len(matches)
            except Exception as exc:
                log.debug("sp_upcoming %s: %s", slug, exc)
        return {"ok": True, "total": total}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.bt.harvest_all_upcoming", soft_time_limit=180, time_limit=200)
def bt_harvest_all_upcoming() -> dict:
    try:
        from app.workers.bt_harvester import fetch_upcoming_matches
        r = _get_redis()
        total = 0
        for slug in ["soccer","basketball","tennis","ice-hockey","volleyball","rugby","handball","cricket"][:8]:
            try:
                matches = fetch_upcoming_matches(sport_slug=slug, max_pages=5)
                if matches:
                    r.set(f"bt:upcoming:{slug}", json.dumps({"matches":matches,"harvested_at":_now_ts()}), ex=360)
                    total += len(matches)
            except Exception as exc:
                log.debug("bt_upcoming %s: %s", slug, exc)
        return {"ok": True, "total": total}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.od.harvest_all_upcoming", soft_time_limit=300, time_limit=330, acks_late=True)
def od_harvest_all_upcoming() -> dict:
    try:
        from app.workers.od_harvester import fetch_upcoming_matches
        r      = _get_redis()
        SPORTS = ["soccer","basketball","tennis","volleyball","handball","rugby","cricket","table-tennis"]
        total  = 0
        for slug in SPORTS:
            try:
                matches = fetch_upcoming_matches(sport_slug=slug)
                if matches:
                    r.set(f"od:upcoming:{slug}", json.dumps({"matches":matches,"harvested_at":_now_ts()}), ex=420)
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