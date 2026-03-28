"""
app/workers/sp_live_harvester.py  (v4 — patched from user's v3)
================================================================
Changes applied to the original file
─────────────────────────────────────
① CORRECT WS URL  — wss://realtime-notificator.ke.sportpesa.com/socket.io/
  (was wss://www.ke.sportpesa.com/socket.io/ — that host does not serve
   the realtime socket feed, so no messages ever arrived)

② CORRECT HEARTBEAT — EIO=3 protocol requires:
     server sends "2"  → client replies "3"        (already present ✓)
     client sends "21" every 20 s proactively       (NEW — was missing)
     server replies "31"                            (NEW — ignore it)
  Without the client-initiated "21" ping SP silently drops the
  connection after ~60 s, which is why the feed appeared to work
  briefly then stop.

③ SUBSCRIBE PATTERN  — only subscribe buffered-event-{id}-194-0.00
  per event.  This is exactly what the SP browser sends.  Subscribing
  all 11 market types (old _SUBSCRIBE_TYPES list) exceeds SP's
  per-connection subscription limit and causes the server to silently
  stop sending data.  All other markets arrive automatically once
  the 1x2 channel is subscribed.

④ SPORTRADAR TIMELINE POLLING  — new background thread that calls
  match_timelinedelta/{externalId} every 5 s for each live event and
  publishes event_update payloads with sr_ptime (period-start unix
  timestamp).  The frontend uses this for pixel-perfect clock sync.

Everything else (Redis helpers, market normalisation, snapshot,
_diff_and_record, _cache_event_state, thread management) is unchanged
from the original file.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import requests

log = logging.getLogger("sp_live_harvester")

# ── Redis ─────────────────────────────────────────────────────────────────────
_redis_lock = threading.Lock()
_redis_client = None


def _get_redis():
    global _redis_client
    if _redis_client is not None:
        try:
            _redis_client.ping()
            return _redis_client
        except Exception:
            _redis_client = None

    with _redis_lock:
        if _redis_client is not None:
            return _redis_client
        import redis as _r
        url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        # Use DB 1 for live data (harvester uses DB 0/2 for tasks)
        base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
        _redis_client = _r.Redis.from_url(
            f"{base}/1",
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        return _redis_client


# ── Channel name helpers ───────────────────────────────────────────────────────
CH_ALL   = "sp:live:all"
CH_SPORT = "sp:live:sport:{sport_id}"
CH_EVENT = "sp:live:event:{event_id}"


def _ch_sport(sport_id: int) -> str:
    return CH_SPORT.format(sport_id=sport_id)


def _ch_event(event_id: int) -> str:
    return CH_EVENT.format(event_id=event_id)


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Core publish — fans out to ALL three channels ─────────────────────────────
def _publish_live_update(
    r,
    sport_id: int,
    event_id: int,
    payload: dict,
) -> None:
    """
    Publish one normalised update to:
      • sp:live:all              (all-sports firehose)
      • sp:live:sport:{sport_id} (sport-scoped — used by LiveTab sport SSE)
      • sp:live:event:{event_id} (per-event    — used by per-match SSE sub)
    All three in a single pipeline to minimise RTTs.
    """
    raw = json.dumps(payload, ensure_ascii=False)
    try:
        pipe = r.pipeline()
        pipe.publish(CH_ALL, raw)
        pipe.publish(_ch_sport(sport_id), raw)
        pipe.publish(_ch_event(event_id), raw)
        pipe.execute()
    except Exception as exc:
        log.warning("publish_live_update failed: %s", exc)


# ── SP HTTP base ───────────────────────────────────────────────────────────────
_SP_BASE = "https://www.ke.sportpesa.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Timezone":   "Africa/Nairobi",
    "Origin":           _SP_BASE,
    "Referer":          _SP_BASE + "/",
}

_LIVE_SPORT_IDS = [1, 2, 4, 5, 6, 8, 9, 10, 13, 21, 23]

_TYPE_TO_SLUG: dict[int, str] = {
    194: "1x2",
    149: "match_winner",
    147: "double_chance",
    138: "btts",
    140: "first_half_btts",
    145: "odd_even",
    166: "draw_no_bet",
    151: "european_handicap",
    184: "asian_handicap",
    183: "correct_score",
    154: "exact_goals",
    129: "first_team_to_score",
    155: "highest_scoring_half",
    135: "first_half_1x2",
}

_SPORT_TOTAL_SLUG: dict[int, str] = {
    1:   "over_under_goals",
    5:   "over_under_goals",
    126: "over_under_goals",
    2:   "total_points",
    8:   "total_points",
    4:   "total_games",
    13:  "total_games",
    10:  "total_sets",
    9:   "total_runs",
    23:  "total_sets",
    6:   "over_under_goals",
    16:  "total_games",
    21:  "total_runs",
}

_POS_OUTCOME: dict[int, list[str]] = {
    194: ["1", "X", "2"],
    149: ["1", "2"],
    147: ["1X", "X2", "12"],
    138: ["yes", "no"],
    140: ["yes", "no"],
    145: ["odd", "even"],
    166: ["1", "2"],
    135: ["1", "X", "2"],
    155: ["1st", "2nd", "equal"],
}


def _market_slug(market_type: int, sport_id: int, handicap: str | None) -> str:
    if market_type == 105:
        base = _SPORT_TOTAL_SLUG.get(sport_id, "over_under")
    else:
        base = _TYPE_TO_SLUG.get(market_type, f"market_{market_type}")
    if handicap and handicap not in ("0.00", "0", "", None):
        try:
            fv   = float(handicap)
            line = str(int(fv)) if fv == int(fv) else str(fv)
            return f"{base}_{line}"
        except (ValueError, TypeError):
            pass
    return base


def _normalise_outcome_key(
    sel_name: str,
    sel_idx: int,
    all_sels: list[dict],
    market_type: int,
    handicap: str | None,
    sport_id: int,
) -> str:
    name_l  = sel_name.strip().lower()
    _direct: dict[str, str] = {
        "1": "1", "x": "X", "2": "2", "draw": "X",
        "over": "over", "under": "under",
        "yes": "yes", "no": "no",
        "odd": "odd", "even": "even",
        "1x": "1X", "x2": "X2", "12": "12",
        "home": "1", "away": "2",
    }
    if name_l in _direct:
        return _direct[name_l]
    if name_l.startswith("over"):
        return "over"
    if name_l.startswith("under"):
        return "under"
    if ":" in sel_name and len(sel_name) <= 5:
        return sel_name.strip()
    if "/" in sel_name and len(sel_name) <= 5:
        return sel_name.strip()
    pos_map = _POS_OUTCOME.get(market_type)
    if pos_map and sel_idx < len(pos_map):
        return pos_map[sel_idx]
    if market_type == 105:
        return "over" if sel_idx == 0 else "under"
    slug = name_l.replace(" ", "_").replace("-", "_")
    slug = "".join(c for c in slug if c.isalnum() or c == "_")
    return slug[:14] or f"sel_{sel_idx}"


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _http_get(path: str, params: dict | None = None, timeout: int = 15) -> Any:
    url = f"{_SP_BASE}{path}"
    try:
        r = requests.get(url, headers=_HEADERS, params=params,
                         timeout=timeout, allow_redirects=True)
        if not r.ok:
            log.warning("HTTP %d → %s", r.status_code, url)
            return None
        return r.json()
    except Exception as exc:
        log.warning("HTTP error %s: %s", url, exc)
        return None


# ── Public API — REST data fetchers (called by sp_live_view.py) ───────────────

def fetch_live_sports() -> list[dict]:
    r      = _get_redis()
    cached = r.get("sp:live:sports")
    if cached:
        return json.loads(cached)
    raw    = _http_get("/api/live/sports")
    sports: list = []
    if isinstance(raw, dict):
        sports = raw.get("sports") or raw.get("data") or []
    elif isinstance(raw, list):
        sports = raw
    if sports:
        r.set("sp:live:sports", json.dumps(sports), ex=60)
    return sports


def fetch_live_events(sport_id: int, limit: int = 100, offset: int = 0) -> list[dict]:
    raw = _http_get(
        f"/api/live/sports/{sport_id}/events",
        params={"limit": limit, "offset": offset},
    )
    if raw:
        for key in ("events", "data", "items"):
            if isinstance(raw.get(key), list) and raw[key]:
                log.debug("fetch_live_events sport=%d → %d events", sport_id, len(raw[key]))
                return raw[key]
        if isinstance(raw, list):
            return raw
    raw = _http_get("/api/live/games", params={"sportId": sport_id})
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "games", "items", "events"):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def fetch_live_markets(
    event_ids: list[int],
    sport_id:  int,
    market_type: int = 194,
) -> list[dict]:
    if not event_ids:
        return []
    ids_str = ",".join(str(i) for i in event_ids)
    raw = _http_get(
        "/api/live/markets",
        params={"eventIds": ids_str, "marketType": market_type, "sportId": sport_id},
    )
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "markets", "events"):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def get_odds_history(event_id: int, market_id: int, limit: int = 20) -> list[dict]:
    r   = _get_redis()
    key = f"sp:live:history:{event_id}:{market_id}"
    try:
        items = r.lrange(key, 0, limit - 1)
        return [json.loads(i) for i in items]
    except Exception as exc:
        log.warning("get_odds_history %s: %s", key, exc)
        return []


# ── Snapshot ───────────────────────────────────────────────────────────────────

def _snapshot_sport(sport_id: int) -> list[dict]:
    r      = _get_redis()
    events = fetch_live_events(sport_id, limit=200)
    if not events:
        return []
    snap_events = []
    for ev in events:
        ev_id = ev.get("id")
        if not ev_id:
            continue
        raw_detail = _http_get(f"/api/live/events/{ev_id}/details")
        markets    = []
        if raw_detail and isinstance(raw_detail, dict):
            markets = raw_detail.get("markets") or []
        snap_events.append({"eventId": ev_id, "event": ev, "markets": markets})
    snapshot = {
        "sport_id":     sport_id,
        "event_count":  len(snap_events),
        "harvested_at": _now_ts(),
        "events":       snap_events,
    }
    r.set(f"sp:live:snapshot:{sport_id}", json.dumps(snapshot), ex=90)
    return snap_events


def snapshot_all_sports() -> dict[int, list]:
    result = {}
    for sport_id in _LIVE_SPORT_IDS:
        try:
            evs = _snapshot_sport(sport_id)
            result[sport_id] = evs
            log.info("[snapshot] sport=%d → %d events", sport_id, len(evs))
        except Exception as exc:
            log.warning("[snapshot] sport=%d error: %s", sport_id, exc)
    return result


# =============================================================================
# ④  SPORTRADAR TIMELINE POLLING  (NEW)
# =============================================================================

_SR_BASE           = "https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo"
_SR_ORIGIN         = "https://www.ke.sportpesa.com"
_SR_TOKEN_FALLBACK = (
    "exp=1774813127~acl=/*"
    "~data=eyJvIjoiaHR0cHM6Ly93d3cua2Uuc3BvcnRwZXNhLmNvbSIsImEiOiJmODYx"
    "N2E4OTZkMzU1MWJhNTBkNTFmMDE0OWQ0YjZkZCIsImFjdCI6Im9yaWdpbmNoZWNrIiwi"
    "b3NyYyI6Im9yaWdpbiJ9"
    "~hmac=b884ebf888ce9db171eab4009630c51fbc7ef37d97344b7028c5449a12700b13"
)
_SR_HEADERS = {
    "Origin":     _SR_ORIGIN,
    "Referer":    _SR_ORIGIN + "/",
    "User-Agent": _HEADERS["User-Agent"],
}

# Period number → base seconds from kick-off
_PERIOD_BASE_SEC: dict[str, int] = {
    "1": 0,          # 1st half   0′–45′
    "2": 45 * 60,    # 2nd half  45′–90′
    "3": 90 * 60,    # ET 1st   90′–105′
    "4": 105 * 60,   # ET 2nd  105′–120′
}

# Sportradar status_id → canonical LiveEvent phase
_SR_STATUS_TO_PHASE: dict[int, str] = {
    6:  "FirstHalf",
    7:  "SecondHalf",
    31: "HalfTime",
    41: "ExtraTimeFirst",
    42: "ExtraTimeSecond",
    50: "PenaltyShootout",
    80: "AwaitingExtraTime",
    17: "Paused",
    11: "Interrupted",
}

# Shared maps: WS thread writes, SR thread reads
_sr_event_map:   dict[int, int] = {}   # event_id → external_id
_sr_event_sport: dict[int, int] = {}   # event_id → sport_id
_sr_map_lock     = threading.Lock()

_SR_THREAD: threading.Thread | None = None
_SR_STOP    = threading.Event()


def _sr_token() -> str:
    return os.getenv("SPORTRADAR_TOKEN", _SR_TOKEN_FALLBACK)


def _fetch_sr_timeline(external_id: int) -> dict | None:
    url = f"{_SR_BASE}/match_timelinedelta/{external_id}?T={_sr_token()}"
    try:
        resp = requests.get(url, headers=_SR_HEADERS, timeout=8)
        return resp.json() if resp.ok else None
    except Exception:
        return None


def _parse_sr_event_update(
    external_id: int, data: dict,
    event_id: int, sport_id: int,
) -> dict | None:
    try:
        doc      = (data.get("doc") or [{}])[0]
        match    = doc.get("data", {}).get("match", {})
        p        = str(match.get("p") or "")      # "1"|"2"|"3"|"4"
        ptime    = match.get("ptime")              # unix ts: period started
        timeinfo = match.get("timeinfo") or {}
        running  = bool(timeinfo.get("running", False))
        result   = match.get("result") or {}
        status   = match.get("status") or {}
        status_id = int(status.get("_id") or 0)

        if p and ptime:
            base_sec  = _PERIOD_BASE_SEC.get(p, 0)
            elapsed   = int(time.time()) - int(ptime)
            total     = base_sec + (elapsed if running else 0)
            mm, ss    = divmod(max(0, total), 60)
            match_time_str = f"{mm}:{ss:02d}"
        else:
            match_time_str = ""

        return {
            "type":          "event_update",
            "event_id":      event_id,
            "sport_id":      sport_id,
            "status":        "Started",
            "phase":         _SR_STATUS_TO_PHASE.get(status_id, ""),
            "match_time":    match_time_str,
            "clock_running": running,
            "remaining_ms":  None,
            "score_home":    str(result.get("home") or "0"),
            "score_away":    str(result.get("away") or "0"),
            "is_paused":     not running,
            # These two fields are used by the frontend useLiveClock hook
            # to interpolate the clock without waiting for the next update.
            "sr_period":     p,
            "sr_ptime":      int(ptime) if ptime else None,
            "ts":            _now_ts(),
            "source":        "sportradar",
        }
    except Exception as exc:
        log.debug("[SR] parse error ext=%d: %s", external_id, exc)
        return None


def _sr_poll_loop() -> None:
    """
    Background thread: poll Sportradar match_timelinedelta every 5 s for
    every live event that has an external (betradar) ID.
    Publishes event_update to all three Redis channels.
    """
    r = _get_redis()
    log.info("[SR] timeline polling started")

    while not _SR_STOP.is_set():
        with _sr_map_lock:
            snap_events = dict(_sr_event_map)
            snap_sports = dict(_sr_event_sport)

        for event_id, external_id in snap_events.items():
            if external_id <= 0:
                continue
            sport_id = snap_sports.get(event_id, 1)
            try:
                data    = _fetch_sr_timeline(external_id)
                if not data:
                    continue
                payload = _parse_sr_event_update(external_id, data, event_id, sport_id)
                if not payload:
                    continue
                # Cache state
                r.set(
                    f"sp:live:state:{event_id}",
                    json.dumps({k: v for k, v in payload.items() if k != "type"}),
                    ex=300,
                )
                _publish_live_update(r, sport_id, event_id, payload)
            except Exception as exc:
                log.debug("[SR] event=%d: %s", event_id, exc)

            # Small pause between individual event fetches to avoid hammering SR
            _SR_STOP.wait(0.25)
            if _SR_STOP.is_set():
                break

        # Full sweep every 5 s
        _SR_STOP.wait(5)

    log.info("[SR] timeline polling stopped")


def _start_sr_thread() -> None:
    global _SR_THREAD, _SR_STOP
    if _SR_THREAD and _SR_THREAD.is_alive():
        return
    _SR_STOP.clear()
    _SR_THREAD = threading.Thread(
        target=_sr_poll_loop,
        name="sp-live-sr-poll",
        daemon=True,
    )
    _SR_THREAD.start()
    log.info("[SR] thread started")


def _stop_sr_thread() -> None:
    _SR_STOP.set()
    if _SR_THREAD and _SR_THREAD.is_alive():
        _SR_THREAD.join(timeout=5)


# =============================================================================
# WS HARVESTER  (original structure preserved, 3 fixes applied)
# =============================================================================

_HARVESTER_THREAD: threading.Thread | None = None
_HARVESTER_STOP   = threading.Event()

# ③ Only type 194 per event — mirrors exactly what SP browser sends.
#    DO NOT subscribe all 11 types; that exceeds SP's subscription limit.
#    (Old _SUBSCRIBE_TYPES list removed.)

# ① CORRECT WS URL — realtime-notificator, not www.
_WS_BASE = "wss://realtime-notificator.ke.sportpesa.com"


def _ws_url() -> str:
    return f"{_WS_BASE}/socket.io/?EIO=3&transport=websocket"


def _ws_extra_headers() -> list[tuple[str, str]]:
    return [
        ("Origin",          _SR_ORIGIN),
        ("User-Agent",      _HEADERS["User-Agent"]),
        ("Referer",         _SR_ORIGIN + "/"),
        ("Accept-Language", "en-GB,en-US;q=0.9,en;q=0.8"),
    ]


def _parse_ws_frame(raw: str) -> tuple[str | None, Any]:
    if raw.startswith("42"):
        try:
            payload = json.loads(raw[2:])
            if isinstance(payload, list) and payload:
                return str(payload[0]), payload[1] if len(payload) > 1 else None
        except (json.JSONDecodeError, IndexError):
            pass
    return None, None


def _state_from_event_update(msg: dict) -> dict:
    state = msg.get("state") or {}
    score = state.get("matchScore") or {}
    return {
        "type":          "event_update",
        "event_id":      msg.get("id"),
        "sport_id":      msg.get("sportId", 1),
        "status":        msg.get("status", ""),
        "phase":         state.get("currentEventPhase", ""),
        "match_time":    state.get("matchTime", ""),
        "clock_running": state.get("clockRunning", False),
        "remaining_ms":  state.get("remainingTimeMillis"),
        "score_home":    str(score.get("home") or "0"),
        "score_away":    str(score.get("away") or "0"),
        "is_paused":     msg.get("isPaused", False),
        "ts":            _now_ts(),
        "source":        "sportpesa_ws",
    }


def _market_from_buffered_update(
    msg: dict,
    sport_id_map: dict[int, int],
) -> dict | None:
    event_id    = msg.get("eventId")
    market_type = msg.get("type")
    if not event_id or market_type is None:
        return None

    sport_id = sport_id_map.get(event_id, 1)
    handicap = str(msg.get("handicap") or "0.00")
    slug     = _market_slug(market_type, sport_id, handicap)
    all_sels = msg.get("selections") or []

    norm_sels = []
    for idx, sel in enumerate(all_sels):
        out_key = _normalise_outcome_key(
            sel_name    = sel.get("name", ""),
            sel_idx     = idx,
            all_sels    = all_sels,
            market_type = market_type,
            handicap    = handicap,
            sport_id    = sport_id,
        )
        norm_sels.append({
            "id":          sel.get("id"),
            "name":        sel.get("name", ""),
            "outcome_key": out_key,
            "odds":        str(sel.get("odds", "0")),
            "status":      sel.get("status", "Open"),
        })

    return {
        "type":                   "market_update",
        "event_id":               event_id,
        "sport_id":               sport_id,
        "market_id":              msg.get("eventMarketId"),
        "market_type":            market_type,
        "market_name":            msg.get("name", ""),
        "market_slug":            slug,
        "handicap":               handicap,
        "all_selections":         all_sels,
        "normalised_selections":  norm_sels,
        "changed_selections":     [],
        "ts":                     _now_ts(),
    }


def _diff_and_record(r, payload: dict) -> dict:
    event_id    = payload["event_id"]
    market_id   = payload.get("market_id") or 0
    changed     = []
    pipe        = r.pipeline()
    history_key = f"sp:live:history:{event_id}:{market_id}"

    for sel in payload["normalised_selections"]:
        sel_id   = sel.get("id")
        if sel_id is None:
            continue
        odds_key = f"sp:live:odds:{event_id}:{sel_id}"
        new_odds = str(sel["odds"])
        prev     = r.get(odds_key)
        if prev != new_odds:
            changed.append({"id": sel_id, "odds": new_odds, "prev": prev})
            pipe.set(odds_key, new_odds, ex=300)

    if changed:
        tick = {
            "t":       _now_ts(),
            "market":  payload["market_slug"],
            "changed": [{"id": c["id"], "odds": c["odds"]} for c in changed],
        }
        pipe.lpush(history_key, json.dumps(tick))
        pipe.ltrim(history_key, 0, 49)
        pipe.expire(history_key, 600)

    pipe.execute()
    payload["changed_selections"] = changed
    return payload


def _cache_event_state(r, payload: dict) -> None:
    event_id = payload.get("event_id")
    if not event_id:
        return
    r.set(
        f"sp:live:state:{event_id}",
        json.dumps({k: v for k, v in payload.items() if k != "type"}),
        ex=300,
    )


def _harvester_loop() -> None:
    """
    Main WS harvester loop — original structure, three fixes applied:
    ① Correct WS URL (realtime-notificator)
    ② Client sends "21" heartbeat every 20 s; ignores "31" server ack
    ③ Only subscribes buffered-event-{id}-194-0.00 per event
    """
    try:
        import websocket
    except ImportError:
        log.error("websocket-client not installed — pip install websocket-client")
        return

    r = _get_redis()

    while not _HARVESTER_STOP.is_set():

        # ── 1. Build event map ────────────────────────────────────────────────
        sport_id_map:    dict[int, int] = {}   # event_id → sport_id
        external_id_map: dict[int, int] = {}   # event_id → betradar/externalId
        subscribed:      set[int]       = set()

        log.info("[WS] fetching live event list …")
        for sport_id in _LIVE_SPORT_IDS:
            try:
                events = fetch_live_events(sport_id, limit=200)
                for ev in events:
                    ev_id  = ev.get("id")
                    ext_id = (ev.get("externalId") or ev.get("betradarId")
                              or ev.get("betRadarId") or 0)
                    if ev_id:
                        sport_id_map[ev_id]    = sport_id
                        external_id_map[ev_id] = int(ext_id or 0)

                # Patch event count in the sports cache
                sports_raw = r.get("sp:live:sports")
                if sports_raw:
                    current = json.loads(sports_raw)
                    for sp in current:
                        if sp.get("id") == sport_id:
                            sp["eventNumber"] = len(events)
                            break
                    else:
                        current.append({"id": sport_id, "eventNumber": len(events)})
                    r.set("sp:live:sports", json.dumps(current), ex=60)
            except Exception as exc:
                log.warning("[WS] snapshot sport=%d: %s", sport_id, exc)

        # Push external IDs to SR polling thread
        with _sr_map_lock:
            _sr_event_map.clear()
            _sr_event_map.update(external_id_map)
            _sr_event_sport.clear()
            _sr_event_sport.update(sport_id_map)

        log.info("[WS] tracking %d live events across %d sports",
                 len(sport_id_map), len(_LIVE_SPORT_IDS))

        if not sport_id_map:
            log.info("[WS] no live events — sleeping 30 s")
            _HARVESTER_STOP.wait(30)
            continue

        # ── 2. WebSocket connection ───────────────────────────────────────────
        ws_connected  = threading.Event()
        ws_error_flag = [None]

        def on_open(ws):
            log.info("[WS] connected to realtime-notificator")
            ws_connected.set()

            # ③ One subscription per event — only type 194
            for ev_id in sport_id_map:
                sub = f"buffered-event-{ev_id}-194-0.00"
                ws.send(f'42["subscribe","{sub}"]')
                subscribed.add(ev_id)

            log.info("[WS] subscribed to %d event channels", len(subscribed))

        def on_message(ws, raw: str):
            try:
                # ② EIO=3 heartbeat
                if raw == "2":
                    ws.send("3")   # server PING → client PONG (unchanged)
                    return
                if raw == "31":
                    return         # server ack for our "21" → ignore

                event_name, data = _parse_ws_frame(raw)
                if not event_name or not data:
                    return

                # ── BUFFERED_MARKET_UPDATE ──────────────────────────────────
                if event_name == "BUFFERED_MARKET_UPDATE":
                    payload = _market_from_buffered_update(data, sport_id_map)
                    if not payload:
                        return
                    payload  = _diff_and_record(r, payload)
                    event_id = payload["event_id"]
                    sport_id = payload["sport_id"]
                    _publish_live_update(r, sport_id, event_id, payload)

                # ── EVENT_UPDATE ────────────────────────────────────────────
                elif event_name == "EVENT_UPDATE":
                    event_id = data.get("id")
                    if not event_id:
                        return
                    sport_id = data.get("sportId") or sport_id_map.get(event_id, 1)
                    payload  = _state_from_event_update(data)
                    _cache_event_state(r, payload)
                    _publish_live_update(r, sport_id, event_id, payload)

                    if event_id not in sport_id_map:
                        sport_id_map[event_id] = sport_id

                    # Subscribe newly seen events
                    if event_id not in subscribed:
                        sub = f"buffered-event-{event_id}-194-0.00"
                        ws.send(f'42["subscribe","{sub}"]')
                        subscribed.add(event_id)

            except Exception as exc:
                log.warning("[WS:on_message] %s", exc)

        def on_error(ws, error):
            log.warning("[WS] error: %s", error)
            ws_error_flag[0] = error

        def on_close(ws, code, msg):
            log.info("[WS] closed code=%s msg=%s", code, msg)
            ws_connected.clear()

        # ① Use correct URL; pass headers as list of "Key: Value" strings
        ws = websocket.WebSocketApp(
            _ws_url(),
            header=[f"{k}: {v}" for k, v in _ws_extra_headers()],
            on_open    = on_open,
            on_message = on_message,
            on_error   = on_error,
            on_close   = on_close,
        )

        ws_thread = threading.Thread(
            target=ws.run_forever,
            # ping_interval=0 disables websocket-client's own TCP keepalive;
            # we handle the socket.io "21"/"31" heartbeat ourselves below.
            kwargs={"ping_interval": 0},
            daemon=True,
            name="sp-live-ws",
        )
        ws_thread.start()

        if not ws_connected.wait(timeout=15):
            log.warning("[WS] connect timeout — retrying in 10 s")
            ws.close()
            _HARVESTER_STOP.wait(10)
            continue

        # ── 3. Heartbeat + event-list refresh loop ────────────────────────────
        last_refresh = time.monotonic()
        last_ping    = time.monotonic()   # ② track when we last sent "21"

        while not _HARVESTER_STOP.is_set() and ws_thread.is_alive():
            now_m = time.monotonic()

            # ② Send client-initiated heartbeat "21" every 20 s
            if now_m - last_ping >= 20:
                try:
                    ws.send("21")
                except Exception:
                    pass   # connection dropped; outer loop reconnects
                last_ping = now_m

            # Refresh event list every 60 s (subscribe newly started events)
            if now_m - last_refresh >= 60:
                for sport_id in _LIVE_SPORT_IDS:
                    try:
                        events = fetch_live_events(sport_id, limit=200)
                        for ev in events:
                            ev_id  = ev.get("id")
                            ext_id = (ev.get("externalId") or ev.get("betradarId") or 0)
                            if not ev_id:
                                continue
                            sport_id_map[ev_id]    = sport_id
                            external_id_map[ev_id] = int(ext_id or 0)
                            if ev_id not in subscribed:
                                sub = f"buffered-event-{ev_id}-194-0.00"
                                try:
                                    ws.send(f'42["subscribe","{sub}"]')
                                    subscribed.add(ev_id)
                                except Exception:
                                    pass
                    except Exception as exc:
                        log.warning("[WS] refresh sport=%d: %s", sport_id, exc)

                # Keep SR polling map up to date
                with _sr_map_lock:
                    _sr_event_map.update(external_id_map)
                    _sr_event_sport.update(sport_id_map)

                last_refresh = now_m

            _HARVESTER_STOP.wait(1)

        log.info("[WS] disconnected — reconnecting in 5 s …")
        try:
            ws.close()
        except Exception:
            pass
        _HARVESTER_STOP.wait(5)


# ── Public thread management ───────────────────────────────────────────────────

def harvester_alive() -> bool:
    """Return True if the WS harvester thread is running."""
    return (
        _HARVESTER_THREAD is not None
        and _HARVESTER_THREAD.is_alive()
    )


def start_harvester_thread() -> threading.Thread:
    """
    Launch the WS harvester + Sportradar polling threads.
    Safe to call multiple times — no-op if already running.
    """
    global _HARVESTER_THREAD, _HARVESTER_STOP

    if not harvester_alive():
        _HARVESTER_STOP.clear()
        _HARVESTER_THREAD = threading.Thread(
            target=_harvester_loop,
            name="sp-live-harvester",
            daemon=True,
        )
        _HARVESTER_THREAD.start()
        log.info("[harvester] WS thread started")

    # Always start SR polling alongside WS harvester
    _start_sr_thread()

    return _HARVESTER_THREAD  # type: ignore[return-value]


def stop_harvester() -> None:
    """Stop both the WS harvester and Sportradar polling threads."""
    global _HARVESTER_THREAD
    _HARVESTER_STOP.set()
    _stop_sr_thread()
    if _HARVESTER_THREAD and _HARVESTER_THREAD.is_alive():
        _HARVESTER_THREAD.join(timeout=5)
        log.info("[harvester] stopped")
    _HARVESTER_THREAD = None