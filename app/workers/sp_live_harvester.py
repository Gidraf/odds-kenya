"""
app/workers/sp_live_harvester.py
=================================
Sportpesa Live Odds Harvester
──────────────────────────────
Manages the WebSocket connection to Sportpesa's live odds feed and
publishes normalised delta messages to Redis pub/sub channels consumed
by the SSE endpoints in sp_live_view.py.

Architecture
────────────
1.  HTTP snapshot  — GET /api/live/sports/{sportId}/events
      Gives us the current event list + initial state (score, phase, clock).
      Called once on startup and on every REFRESH button click.

2.  WebSocket (socket.io v2/v3 framing) — wss://www.ke.sportpesa.com/socket.io/
      After connecting we subscribe to every visible event's market channels:
        "subscribe" → "buffered-event-{eventId}-{marketType}-{handicap}"
      Incoming messages:
        BUFFERED_MARKET_UPDATE  → normalise + publish market_update
        EVENT_UPDATE            → normalise + publish event_update

3.  Redis pub/sub channels
      sp:live:all                  — every update (used by /stream)
      sp:live:sport:{sport_id}     — sport-scoped (used by /stream/sport/<id>)
      sp:live:event:{event_id}     — per-event  (used by /stream/event/<id>)

      Publishing to both sport AND event channels on every WS message means
      the frontend can subscribe at whichever granularity it needs.

4.  Redis key/value (snapshot / state cache)
      sp:live:sports               — live sport list  (TTL 60 s)
      sp:live:snapshot:{sport_id}  — full raw snapshot (TTL 90 s)
      sp:live:state:{event_id}     — last known score/phase (TTL 300 s)
      sp:live:odds:{event_id}:{market_id}  — latest odds per market (TTL 300 s)
      sp:live:history:{event_id}:{market_id}  — last 20 ticks (LIST, TTL 600 s)

Normalised message shapes published to Redis
────────────────────────────────────────────
market_update:
  {
    "type": "market_update",
    "event_id": 1093987,
    "sport_id": 1,
    "market_id": 68048759,          # eventMarketId
    "market_type": 194,             # SP type ID  (194=1x2, 105=total, …)
    "market_name": "1x2",
    "market_slug": "1x2",           # canonical slug
    "handicap": "0.00",
    "all_selections": [
      {"id": 140732331, "name": "NK Bjelovar", "status": "Open", "odds": "3.35"},
      …
    ],
    "normalised_selections": [
      {"id": 140732331, "name": "NK Bjelovar", "outcome_key": "1",
       "odds": "3.35", "status": "Open"},
      …
    ],
    "changed_selections": [          # only those whose odds changed
      {"id": 140732331, "odds": "3.35"},
    ],
    "ts": "2026-03-28T16:05:01Z"
  }

event_update:
  {
    "type": "event_update",
    "event_id": 1120445,
    "sport_id": 1,
    "status": "Started",
    "phase": "SecondHalf",
    "match_time": "45:00",
    "clock_running": false,
    "remaining_ms": null,
    "score_home": "1",
    "score_away": "0",
    "is_paused": false,
    "ts": "2026-03-28T16:02:22Z"
  }
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
        "Mozilla/5.0 (Linux; Android 13; Mobile) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Timezone":   "Africa/Nairobi",
    "Origin":           _SP_BASE,
    "Referer":          _SP_BASE + "/",
}

# Sport IDs supported for live
_LIVE_SPORT_IDS = [1, 2, 4, 5, 6, 8, 9, 10, 13, 21, 23]

# SP market type → canonical slug (for normalised_selections outcome_key)
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

# For market type 105 (total/over-under) the slug depends on sport
_SPORT_TOTAL_SLUG: dict[int, str] = {
    1:  "over_under_goals",
    5:  "over_under_goals",    # football variants
    126: "over_under_goals",
    2:  "total_points",
    8:  "total_points",
    4:  "total_games",
    13: "total_games",
    10: "total_sets",
    9:  "total_runs",
    23: "total_sets",
    6:  "over_under_goals",
    16: "total_games",
    21: "total_runs",
}

# Canonical outcome key by position for common market types
_POS_OUTCOME: dict[int, list[str]] = {
    194: ["1", "X", "2"],      # 1x2
    149: ["1", "2"],            # match winner
    147: ["1X", "X2", "12"],   # double chance
    138: ["yes", "no"],         # btts
    140: ["yes", "no"],         # 1H btts
    145: ["odd", "even"],
    166: ["1", "2"],            # draw no bet
    135: ["1", "X", "2"],      # 1H 1x2
    155: ["1st", "2nd", "equal"],
}


def _market_slug(market_type: int, sport_id: int, handicap: str | None) -> str:
    """Return canonical slug for a market, appending line where appropriate."""
    if market_type == 105:
        base = _SPORT_TOTAL_SLUG.get(sport_id, "over_under")
    else:
        base = _TYPE_TO_SLUG.get(market_type, f"market_{market_type}")

    if handicap and handicap not in ("0.00", "0", "", None):
        try:
            fv = float(handicap)
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
    """
    Convert a raw SP selection name to a canonical outcome key.

    Priority:
      1. Known shortname map (1/X/2/yes/no/over/under/…)
      2. Positional map from _POS_OUTCOME
      3. O/U prefix detection ("over …" / "under …")
      4. Slugified fallback
    """
    name_l = sel_name.strip().lower()

    # Direct canonical names
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

    # O/U with line value embedded in name ("Over 2.50", "Under 45.5")
    if name_l.startswith("over"):
        return "over"
    if name_l.startswith("under"):
        return "under"

    # Correct score patterns  "2:1", "0:0", …
    if ":" in sel_name and len(sel_name) <= 5:
        return sel_name.strip()

    # HT/FT combos  "1/1", "X/2", …
    if "/" in sel_name and len(sel_name) <= 5:
        return sel_name.strip()

    # Positional map (covers 194, 149, 147, 138, 145, …)
    pos_map = _POS_OUTCOME.get(market_type)
    if pos_map and sel_idx < len(pos_map):
        return pos_map[sel_idx]

    # Market 105: always over/under by position
    if market_type == 105:
        return "over" if sel_idx == 0 else "under"

    # Generic slugify
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
    """
    Return current live sport list from SP API.
    Cached in Redis as sp:live:sports (TTL 60 s).
    """
    r = _get_redis()
    cached = r.get("sp:live:sports")
    if cached:
        return json.loads(cached)

    raw = _http_get("/api/live/sports")
    sports = []
    if isinstance(raw, dict):
        sports = raw.get("sports") or raw.get("data") or []
    elif isinstance(raw, list):
        sports = raw

    if sports:
        r.set("sp:live:sports", json.dumps(sports), ex=60)
    return sports


def fetch_live_events(sport_id: int, limit: int = 100, offset: int = 0) -> list[dict]:
    """
    Fetch live events for one sport via HTTP (fresh, no cache).
    Returns list of raw SP event dicts.
    """
    # Primary endpoint (confirmed from SP browser traffic)
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

    # Fallback: old endpoint
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
    """
    Fetch market odds for a batch of event IDs.
    Returns list of {eventId, markets: [...]} dicts.
    """
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


def get_odds_history(
    event_id:  int,
    market_id: int,
    limit:     int = 20,
) -> list[dict]:
    """Return last `limit` odds ticks for a market from Redis history list."""
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
    """Fetch events + markets for one sport, store in Redis snapshot key."""
    r      = _get_redis()
    events = fetch_live_events(sport_id, limit=200)
    if not events:
        return []

    # Try to get initial market odds for each event via details endpoint
    snap_events = []
    for ev in events:
        ev_id = ev.get("id")
        if not ev_id:
            continue
        raw_detail = _http_get(f"/api/live/events/{ev_id}/details")
        markets = []
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
    """Force HTTP snapshot of all known live sports. Returns {sport_id: events}."""
    result = {}
    for sport_id in _LIVE_SPORT_IDS:
        try:
            evs = _snapshot_sport(sport_id)
            result[sport_id] = evs
            log.info("[snapshot] sport=%d → %d events", sport_id, len(evs))
        except Exception as exc:
            log.warning("[snapshot] sport=%d error: %s", sport_id, exc)
    return result


# ── WS Harvester ───────────────────────────────────────────────────────────────
#
# Sportpesa uses socket.io v2 over WebSocket.
# Frame format:  "42" prefix + JSON array  ["EVENT_NAME", {...}]
# Ping/pong:     "2" (server ping) → reply "3" (client pong)
# Connect:       on open send  42["subscribe", "live-sport-{sportId}"]
#                then per-event: 42["subscribe", "buffered-event-{evId}-{type}-{sv}"]

_HARVESTER_THREAD: threading.Thread | None = None
_HARVESTER_STOP   = threading.Event()

# Market type IDs we subscribe to per event
_SUBSCRIBE_TYPES: list[tuple[int, str]] = [
    (194, "0.00"),  # 1x2
    (149, "0.00"),  # match_winner
    (105, "2.50"),  # O/U 2.5
    (105, "1.50"),  # O/U 1.5
    (105, "3.50"),  # O/U 3.5
    (147, "0.00"),  # double chance
    (138, "0.00"),  # btts
    (145, "0.00"),  # odd/even
    (166, "0.00"),  # draw no bet
    (135, "0.00"),  # 1H 1x2
    (140, "0.00"),  # 1H btts
]


def _ws_url() -> str:
    """Build socket.io WebSocket URL."""
    return (
        "wss://www.ke.sportpesa.com/socket.io/"
        "?EIO=3&transport=websocket"
    )


def _ws_headers() -> dict:
    return {
        "Origin":     _SP_BASE,
        "User-Agent": _HEADERS["User-Agent"],
    }


def _parse_ws_frame(raw: str) -> tuple[str | None, Any]:
    """
    Parse socket.io frame.
    Returns (event_name, data) or (None, None).
    """
    # socket.io packet types
    # 0=CONNECT 1=DISCONNECT 2=EVENT 3=ACK 4=ERROR 5=BINARY_EVENT 6=BINARY_ACK
    # The prefix "42" = engine.io type 4 (message) + socket.io type 2 (EVENT)
    if raw.startswith("42"):
        try:
            payload = json.loads(raw[2:])
            if isinstance(payload, list) and payload:
                return str(payload[0]), payload[1] if len(payload) > 1 else None
        except (json.JSONDecodeError, IndexError):
            pass
    return None, None


def _state_from_event_update(msg: dict) -> dict:
    """Extract normalised event_update payload from SP EVENT_UPDATE message."""
    state = msg.get("state") or {}
    score = state.get("matchScore") or {}
    return {
        "type":        "event_update",
        "event_id":    msg.get("id"),
        "sport_id":    msg.get("sportId", 1),
        "status":      msg.get("status", ""),
        "phase":       state.get("currentEventPhase", ""),
        "match_time":  state.get("matchTime", ""),
        "clock_running": state.get("clockRunning", False),
        "remaining_ms":  state.get("remainingTimeMillis"),
        "score_home":  str(score.get("home") or "0"),
        "score_away":  str(score.get("away") or "0"),
        "is_paused":   msg.get("isPaused", False),
        "ts":          _now_ts(),
    }


def _market_from_buffered_update(
    msg: dict,
    sport_id_map: dict[int, int],
) -> dict | None:
    """
    Normalise BUFFERED_MARKET_UPDATE → market_update dict.
    sport_id_map: {event_id → sport_id}
    """
    event_id    = msg.get("eventId")
    market_type = msg.get("type")
    if not event_id or market_type is None:
        return None

    sport_id  = sport_id_map.get(event_id, 1)
    handicap  = str(msg.get("handicap") or "0.00")
    slug      = _market_slug(market_type, sport_id, handicap)
    all_sels  = msg.get("selections") or []

    # Build normalised selections (outcome_key resolved)
    norm_sels = []
    for idx, sel in enumerate(all_sels):
        out_key = _normalise_outcome_key(
            sel_name   = sel.get("name", ""),
            sel_idx    = idx,
            all_sels   = all_sels,
            market_type = market_type,
            handicap   = handicap,
            sport_id   = sport_id,
        )
        norm_sels.append({
            "id":          sel.get("id"),
            "name":        sel.get("name", ""),
            "outcome_key": out_key,
            "odds":        str(sel.get("odds", "0")),
            "status":      sel.get("status", "Open"),
        })

    return {
        "type":          "market_update",
        "event_id":      event_id,
        "sport_id":      sport_id,
        "market_id":     msg.get("eventMarketId"),
        "market_type":   market_type,
        "market_name":   msg.get("name", ""),
        "market_slug":   slug,
        "handicap":      handicap,
        "all_selections":         all_sels,
        "normalised_selections":  norm_sels,
        "changed_selections":     [],   # will be filled after prev-odds diff
        "ts":            _now_ts(),
    }


def _diff_and_record(
    r,
    payload: dict,
) -> dict:
    """
    Compare current odds against Redis-cached previous odds.
    Fills payload["changed_selections"] with selections whose odds changed.
    Appends to odds history list.
    Caches latest odds per selection.
    Returns updated payload.
    """
    event_id  = payload["event_id"]
    market_id = payload["market_id"]
    changed   = []

    pipe = r.pipeline()
    history_key = f"sp:live:history:{event_id}:{market_id}"

    for sel in payload["normalised_selections"]:
        sel_id    = sel["id"]
        odds_key  = f"sp:live:odds:{event_id}:{sel_id}"
        new_odds  = sel["odds"]

        prev_odds = r.get(odds_key)
        if prev_odds != new_odds:
            changed.append({
                "id":   sel_id,
                "odds": new_odds,
                "prev": prev_odds,
            })
            pipe.set(odds_key, new_odds, ex=300)

    # Push a compact tick to the history list (newest first)
    if changed:
        tick = {
            "t":       _now_ts(),
            "market":  payload["market_slug"],
            "changed": [{"id": c["id"], "odds": c["odds"]} for c in changed],
        }
        pipe.lpush(history_key, json.dumps(tick))
        pipe.ltrim(history_key, 0, 49)   # keep last 50 ticks
        pipe.expire(history_key, 600)

    pipe.execute()
    payload["changed_selections"] = changed
    return payload


def _cache_event_state(r, payload: dict) -> None:
    """Cache the latest event state in Redis for /state/<event_id> endpoint."""
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
    Main harvester loop.
    Opens a WebSocket to SP, subscribes to all live events, and
    forwards normalised messages to Redis pub/sub.
    Reconnects automatically on disconnect.
    """
    try:
        import websocket  # websocket-client library
    except ImportError:
        log.error("websocket-client not installed — run: pip install websocket-client")
        return

    r = _get_redis()

    while not _HARVESTER_STOP.is_set():
        # ── 1. HTTP snapshot: get current live events for all sports ──────────
        sport_id_map: dict[int, int] = {}    # event_id → sport_id
        subscribed_events: set[int]  = set()

        log.info("[harvester] fetching live event list …")
        for sport_id in _LIVE_SPORT_IDS:
            try:
                events = fetch_live_events(sport_id, limit=200)
                for ev in events:
                    ev_id = ev.get("id")
                    if ev_id:
                        sport_id_map[ev_id] = sport_id
                # Update sports cache
                sports_raw = r.get("sp:live:sports")
                if sports_raw:
                    current = json.loads(sports_raw)
                    # Patch eventNumber for this sport
                    for sp in current:
                        if sp.get("id") == sport_id:
                            sp["eventNumber"] = len(events)
                            break
                    else:
                        current.append({"id": sport_id, "eventNumber": len(events)})
                    r.set("sp:live:sports", json.dumps(current), ex=60)
            except Exception as exc:
                log.warning("[harvester] snapshot sport=%d: %s", sport_id, exc)

        log.info("[harvester] tracking %d live events across %d sports",
                 len(sport_id_map), len(_LIVE_SPORT_IDS))

        if not sport_id_map:
            log.info("[harvester] no live events — sleeping 30 s")
            _HARVESTER_STOP.wait(30)
            continue

        # ── 2. WebSocket connection ───────────────────────────────────────────
        ws_connected  = threading.Event()
        ws_error_flag = [None]

        def on_open(ws):
            log.info("[ws] connected")
            ws_connected.set()
            # Subscribe to each sport's live feed
            for sport_id in _LIVE_SPORT_IDS:
                if sport_id in {ev_sport for ev_sport in sport_id_map.values()}:
                    frame = json.dumps(["subscribe", f"live-sport-{sport_id}"])
                    ws.send(f"42{frame}")

            # Subscribe to each live event's market channels
            for event_id, sport_id in sport_id_map.items():
                for mtype, sv in _SUBSCRIBE_TYPES:
                    chan = f"buffered-event-{event_id}-{mtype}-{sv}"
                    frame = json.dumps(["subscribe", chan])
                    ws.send(f"42{frame}")
                subscribed_events.add(event_id)

        def on_message(ws, raw):
            try:
                # socket.io ping → pong
                if raw == "2":
                    ws.send("3")
                    return

                event_name, data = _parse_ws_frame(raw)
                if not event_name or not data:
                    return

                # ── BUFFERED_MARKET_UPDATE ─────────────────────────────────
                if event_name == "BUFFERED_MARKET_UPDATE":
                    payload = _market_from_buffered_update(data, sport_id_map)
                    if not payload:
                        return

                    # Diff against previous odds, fill changed_selections
                    payload = _diff_and_record(r, payload)

                    # Publish
                    event_id = payload["event_id"]
                    sport_id = payload["sport_id"]
                    _publish_live_update(r, sport_id, event_id, payload)

                # ── EVENT_UPDATE (score / phase / clock) ───────────────────
                elif event_name == "EVENT_UPDATE":
                    event_id = data.get("id")
                    if not event_id:
                        return
                    sport_id = data.get("sportId") or sport_id_map.get(event_id, 1)
                    payload  = _state_from_event_update(data)

                    # Cache state
                    _cache_event_state(r, payload)

                    # Publish
                    _publish_live_update(r, sport_id, event_id, payload)

                    # Also update our local sport→event mapping
                    if event_id not in sport_id_map:
                        sport_id_map[event_id] = sport_id

                    # Subscribe new events we haven't seen yet
                    if event_id not in subscribed_events:
                        for mtype, sv in _SUBSCRIBE_TYPES:
                            chan = f"buffered-event-{event_id}-{mtype}-{sv}"
                            frame = json.dumps(["subscribe", chan])
                            ws.send(f"42{frame}")
                        subscribed_events.add(event_id)

            except Exception as exc:
                log.warning("[ws:on_message] %s", exc)

        def on_error(ws, error):
            log.warning("[ws] error: %s", error)
            ws_error_flag[0] = error

        def on_close(ws, code, msg):
            log.info("[ws] closed code=%s msg=%s", code, msg)
            ws_connected.clear()

        ws = websocket.WebSocketApp(
            _ws_url(),
            header=_ws_headers(),
            on_open    = on_open,
            on_message = on_message,
            on_error   = on_error,
            on_close   = on_close,
        )

        # Run WS in a daemon thread so we can monitor it
        ws_thread = threading.Thread(
            target=ws.run_forever,
            kwargs={"ping_interval": 20, "ping_timeout": 10},
            daemon=True,
            name="sp-live-ws",
        )
        ws_thread.start()

        # Wait for connect (timeout 15 s)
        if not ws_connected.wait(timeout=15):
            log.warning("[harvester] WS connect timeout — retrying in 10 s")
            ws.close()
            _HARVESTER_STOP.wait(10)
            continue

        # ── 3. While connected: refresh event list every 60 s ─────────────
        last_refresh = time.monotonic()
        while not _HARVESTER_STOP.is_set() and ws_thread.is_alive():
            elapsed = time.monotonic() - last_refresh
            if elapsed >= 60:
                # Refresh event list — subscribe to any newly started events
                for sport_id in _LIVE_SPORT_IDS:
                    try:
                        events = fetch_live_events(sport_id, limit=200)
                        for ev in events:
                            ev_id = ev.get("id")
                            if not ev_id:
                                continue
                            sport_id_map[ev_id] = sport_id
                            if ev_id not in subscribed_events:
                                for mtype, sv in _SUBSCRIBE_TYPES:
                                    chan = f"buffered-event-{ev_id}-{mtype}-{sv}"
                                    frame = json.dumps(["subscribe", chan])
                                    try:
                                        ws.send(f"42{frame}")
                                    except Exception:
                                        pass
                                subscribed_events.add(ev_id)
                    except Exception as exc:
                        log.warning("[harvester] refresh sport=%d: %s", sport_id, exc)
                last_refresh = time.monotonic()

            _HARVESTER_STOP.wait(2)

        log.info("[harvester] WS disconnected — reconnecting in 5 s …")
        try:
            ws.close()
        except Exception:
            pass
        _HARVESTER_STOP.wait(5)


# ── Public thread management ───────────────────────────────────────────────────

def harvester_alive() -> bool:
    """Return True if the harvester thread is running."""
    return (
        _HARVESTER_THREAD is not None
        and _HARVESTER_THREAD.is_alive()
    )


def start_harvester_thread() -> threading.Thread:
    """
    Launch the WS harvester in a daemon background thread.
    Safe to call multiple times — returns existing thread if already running.
    """
    global _HARVESTER_THREAD, _HARVESTER_STOP

    if harvester_alive():
        log.info("[harvester] already running — skipping start")
        return _HARVESTER_THREAD  # type: ignore[return-value]

    _HARVESTER_STOP.clear()
    _HARVESTER_THREAD = threading.Thread(
        target=_harvester_loop,
        name="sp-live-harvester",
        daemon=True,
    )
    _HARVESTER_THREAD.start()
    log.info("[harvester] started thread=%s", _HARVESTER_THREAD.name)
    return _HARVESTER_THREAD


def stop_harvester() -> None:
    """Signal the harvester thread to stop and wait up to 5 s for it."""
    global _HARVESTER_THREAD
    _HARVESTER_STOP.set()
    if _HARVESTER_THREAD and _HARVESTER_THREAD.is_alive():
        _HARVESTER_THREAD.join(timeout=5)
        log.info("[harvester] stopped")
    _HARVESTER_THREAD = None