"""
app/workers/sp_live_harvester.py
=================================
Sportpesa Live WebSocket harvester.

Connects to:
  wss://realtime-notificator.ke.sportpesa.com/socket.io/?EIO=3&transport=websocket

Protocol — Socket.IO v3 (EIO=3):
  Recv  0{...}    OPEN handshake  → parse pingInterval, pingTimeout
  Recv  40        CONNECT ack     → send sport subscriptions
  Recv  42[...]   EVENT message   → BUFFERED_MARKET_UPDATE | EVENT_UPDATE
  Send  2         PING (every pingInterval ms)
  Recv  3         PONG

On every BUFFERED_MARKET_UPDATE:
  • Compare incoming selection odds against Redis-cached previous odds.
  • Skip publish entirely if nothing changed (delta-only pub/sub).
  • Upsert odds history hash: sp:odds_history:{eventId}:{marketId} → {selId: [ts, odds]}
  • Publish to Redis channels:
      sp:live:all                  – every change
      sp:live:sport:{sportId}      – sport-scoped
      sp:live:event:{eventId}      – event-scoped

On every EVENT_UPDATE:
  • Store state snapshot in Redis: sp:live:state:{eventId}
  • Publish to same three channels.

HTTP snapshot helpers (used by Celery tasks & Flask endpoints):
  fetch_live_sports()              → list of {id, name, eventNumber}
  fetch_live_events(sport_id)      → list of event dicts
  fetch_live_markets(event_ids, sport_id, market_type) → raw market list
  fetch_event_details(event_id)    → single event details dict
  snapshot_all_sports()            → fetch + cache all live sports/events/markets

Usage (run as a daemon process):
  python -m app.workers.sp_live_harvester

Or from Celery:
  from app.workers.sp_live_harvester import start_harvester_thread
  start_harvester_thread()
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

import requests
import websocket  # websocket-client

log = logging.getLogger("sp_live")

# ── Config ────────────────────────────────────────────────────────────────────

WS_URL    = "wss://realtime-notificator.ke.sportpesa.com/socket.io/?EIO=3&transport=websocket"
API_BASE  = "https://www.ke.sportpesa.com/api/live"
ORIGIN    = "https://www.ke.sportpesa.com"
USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
)

# Primary market type per sport (type 194 = 1x2 / match winner)
DEFAULT_MARKET_TYPE = 194

# Sports present on Sportpesa Live (from /api/live/sports)
LIVE_SPORT_IDS = [1, 2, 4, 5, 8, 9, 10, 13]
# {sp_live_id: our canonical slug}
SPORT_SLUG_MAP = {
    1:  "soccer",
    2:  "basketball",
    4:  "tennis",
    5:  "handball",
    8:  "rugby",
    9:  "cricket",
    10: "volleyball",
    13: "table-tennis",
}

# Redis pub/sub channels
CH_ALL    = "sp:live:all"
CH_SPORT  = "sp:live:sport:{sport_id}"
CH_EVENT  = "sp:live:event:{event_id}"

# Redis key TTLs (seconds)
TTL_ODDS    = 7200   # 2 h
TTL_STATE   = 3600   # 1 h
TTL_EVENTS  = 1800   # 30 min
TTL_SNAPSHOT = 300   # 5 min

# ── Redis singleton ───────────────────────────────────────────────────────────

_redis_client = None

def _get_redis():
    global _redis_client
    if _redis_client is None:
        import redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _redis_client = redis.from_url(url, decode_responses=True)
    return _redis_client


# ── HTTP helpers ──────────────────────────────────────────────────────────────

_SESSION: requests.Session | None = None

def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({
            "Origin":     ORIGIN,
            "Referer":    ORIGIN + "/",
            "User-Agent": USER_AGENT,
        })
    return _SESSION


def _get(path: str, params: dict | None = None, timeout: int = 10) -> Any:
    url = f"{API_BASE}{path}"
    try:
        r = _session().get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("HTTP %s → %s", url, exc)
        return None


# ── Public HTTP helpers (used by Celery + Flask endpoints) ────────────────────

def fetch_live_sports() -> list[dict]:
    """Return [{id, name, eventNumber}, …] from /api/live/sports."""
    data = _get("/sports")
    if not data:
        return []
    return data.get("sports") or []


def fetch_live_events(sport_id: int, limit: int = 50, offset: int = 0) -> list[dict]:
    """Return events list for one live sport."""
    data = _get(f"/sports/{sport_id}/events", {"limit": limit, "offset": offset})
    if not data:
        return []
    return data.get("events") or data.get("data") or []


def fetch_live_markets(
    event_ids: list[int],
    sport_id:  int,
    market_type: int = DEFAULT_MARKET_TYPE,
) -> list[dict]:
    """Return market list for up to 15 event IDs."""
    if not event_ids:
        return []
    ids_str = ",".join(str(i) for i in event_ids[:15])
    data = _get("/event/markets", {
        "eventId":  ids_str,
        "type":     market_type,
        "sportId":  sport_id,
    })
    if not data:
        return []
    return data.get("markets") or data.get("data") or []


def fetch_event_details(event_id: int) -> dict | None:
    """Return single event detail dict from /api/live/events/{id}/details."""
    data = _get(f"/events/{event_id}/details")
    return data if isinstance(data, dict) else None


def snapshot_all_sports() -> dict[int, list[dict]]:
    """
    Fetch all live sports + events + initial markets, cache in Redis.
    Returns {sport_id: [event, …]}.
    Used by Celery beat task and on-startup warm-up.
    """
    r     = _get_redis()
    sports = fetch_live_sports()
    result: dict[int, list[dict]] = {}

    for sport in sports:
        sport_id  = sport["id"]
        sport_slug = SPORT_SLUG_MAP.get(sport_id, f"sport_{sport_id}")

        events = fetch_live_events(sport_id, limit=100)
        if not events:
            result[sport_id] = []
            continue

        # Cache event → sport mapping for pub/sub routing
        pipe = r.pipeline()
        for ev in events:
            pipe.setex(f"sp:live:event_sport:{ev['id']}", TTL_EVENTS, sport_id)
            pipe.setex(f"sp:live:event_slug:{ev['id']}",  TTL_EVENTS, sport_slug)
        pipe.execute()

        # Fetch markets for batches of 15
        event_ids = [ev["id"] for ev in events]
        markets: list[dict] = []
        for i in range(0, len(event_ids), 15):
            batch = fetch_live_markets(event_ids[i:i+15], sport_id)
            markets.extend(batch)

        # Cache snapshot
        snapshot = {
            "sport_id":    sport_id,
            "sport_slug":  sport_slug,
            "sport_name":  sport.get("name", sport_slug),
            "event_count": sport.get("eventNumber", len(events)),
            "events":      events,
            "markets":     markets,
            "fetched_at":  _now_iso(),
        }
        r.setex(f"sp:live:snapshot:{sport_id}", TTL_SNAPSHOT, json.dumps(snapshot))

        result[sport_id] = events

    # Cache full sports list
    r.setex("sp:live:sports", TTL_EVENTS, json.dumps(sports))
    log.info("snapshot_all_sports: %d sports, %d total events",
             len(result), sum(len(v) for v in result.values()))
    return result


# ── Odds delta / history helpers ──────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _upsert_odds_and_diff(
    event_id:  int,
    market:    dict,
) -> tuple[list[dict], list[dict]]:
    """
    Compare incoming selections vs cached previous odds for this market.

    Returns:
        changed_selections – subset of selections whose odds differ from cache.
        all_selections     – full current selections list.

    Side effects:
        • Overwrites sp:live:odds:{eventId}:{marketId}  (current snapshot)
        • Appends to   sp:live:odds_history:{eventId}:{marketId}  (last 50 ticks)
    """
    r          = _get_redis()
    market_id  = market["eventMarketId"]
    selections = market.get("selections") or []

    snap_key = f"sp:live:odds:{event_id}:{market_id}"
    hist_key = f"sp:live:odds_history:{event_id}:{market_id}"

    # Previous snapshot {sel_id: odds_str}
    prev_raw = r.get(snap_key)
    prev     = json.loads(prev_raw) if prev_raw else {}

    # Current {sel_id: odds_str}
    current  = {str(s["id"]): s.get("odds", "0") for s in selections}

    # Delta
    changed_ids = {sid for sid, odds in current.items() if prev.get(sid) != odds}
    changed_sels = [s for s in selections if str(s["id"]) in changed_ids]

    if not changed_ids:
        return [], selections  # nothing changed

    # Persist current snapshot
    r.setex(snap_key, TTL_ODDS, json.dumps(current))

    # Append history tick (keep last 50)
    tick = {
        "ts":   _now_iso(),
        "odds": {str(s["id"]): s.get("odds") for s in changed_sels},
    }
    pipe = r.pipeline()
    pipe.lpush(hist_key, json.dumps(tick))
    pipe.ltrim(hist_key, 0, 49)
    pipe.expire(hist_key, TTL_ODDS)
    pipe.execute()

    return changed_sels, selections


def get_odds_history(event_id: int, market_id: int, limit: int = 20) -> list[dict]:
    """Return last `limit` odds ticks for a market (newest first)."""
    r   = _get_redis()
    key = f"sp:live:odds_history:{event_id}:{market_id}"
    raw = r.lrange(key, 0, limit - 1)
    return [json.loads(x) for x in raw]


def get_current_odds(event_id: int, market_id: int) -> dict:
    """Return current {sel_id: odds_str} for a market."""
    r   = _get_redis()
    raw = r.get(f"sp:live:odds:{event_id}:{market_id}")
    return json.loads(raw) if raw else {}


# ── Pub/sub publisher ─────────────────────────────────────────────────────────

def _publish(sport_id: int | None, event_id: int, payload: dict) -> None:
    r = _get_redis()
    msg = json.dumps(payload, ensure_ascii=False)
    pipe = r.pipeline()
    pipe.publish(CH_ALL, msg)
    pipe.publish(CH_EVENT.format(event_id=event_id), msg)
    if sport_id:
        pipe.publish(CH_SPORT.format(sport_id=sport_id), msg)
    pipe.execute()


def _event_sport_id(event_id: int) -> int | None:
    r   = _get_redis()
    val = r.get(f"sp:live:event_sport:{event_id}")
    return int(val) if val else None


# ── Market + event handlers ───────────────────────────────────────────────────

def _handle_market_update(data: dict) -> None:
    event_id  = data.get("eventId")
    market_id = data.get("eventMarketId")
    if not event_id or not market_id:
        return

    changed_sels, all_sels = _upsert_odds_and_diff(event_id, data)
    if not changed_sels:
        return  # identical odds — no publish

    sport_id = _event_sport_id(event_id)

    payload = {
        "type":              "market_update",
        "event_id":          event_id,
        "market_id":         market_id,
        "sport_id":          sport_id,
        "sport_slug":        SPORT_SLUG_MAP.get(sport_id) if sport_id else None,
        "market_name":       data.get("name"),
        "market_type":       data.get("type"),
        "handicap":          data.get("handicap"),
        "status":            data.get("status"),
        "template":          data.get("template"),
        "sequence":          data.get("sequence"),
        "changed_count":     len(changed_sels),
        "changed_selections": changed_sels,   # only what changed
        "all_selections":    all_sels,         # full current book
        "ts":                _now_iso(),
    }

    _publish(sport_id, event_id, payload)
    log.debug("market_update event=%s market=%s changed=%d",
              event_id, market_id, len(changed_sels))


def _handle_event_update(data: dict) -> None:
    event_id = data.get("id")
    sport_id = data.get("sportId")
    if not event_id:
        return

    # Cache event→sport mapping
    if sport_id:
        r = _get_redis()
        r.setex(f"sp:live:event_sport:{event_id}", TTL_EVENTS, sport_id)

    # Store state snapshot
    r = _get_redis()
    r.setex(f"sp:live:state:{event_id}", TTL_STATE, json.dumps(data))

    state  = data.get("state") or {}
    score  = state.get("matchScore") or {}
    payload = {
        "type":        "event_update",
        "event_id":    event_id,
        "sport_id":    sport_id,
        "sport_slug":  SPORT_SLUG_MAP.get(sport_id) if sport_id else None,
        "status":      data.get("status"),
        "is_paused":   data.get("isPaused"),
        "phase":       state.get("currentEventPhase"),
        "clock_running": state.get("clockRunning"),
        "remaining_ms":  state.get("remainingTimeMillis"),
        "score_home":  score.get("home"),
        "score_away":  score.get("away"),
        "state":       state,
        "ts":          _now_iso(),
    }

    _publish(sport_id, event_id, payload)
    log.debug("event_update event=%s phase=%s score=%s-%s",
              event_id, state.get("currentEventPhase"),
              score.get("home"), score.get("away"))


# ═════════════════════════════════════════════════════════════════════════════
# WebSocket harvester class
# ═════════════════════════════════════════════════════════════════════════════

class SpLiveHarvester:
    """
    Long-running Socket.IO v3 WebSocket client.

    Thread-safe. Reconnects automatically on disconnect.
    Sends periodic PINGs to keep the connection alive.
    """

    def __init__(self):
        self._ws:        websocket.WebSocketApp | None = None
        self._stop:      threading.Event = threading.Event()
        self._ping_thread: threading.Thread | None = None
        self._ping_interval: float = 20.0  # seconds, overridden by handshake
        self._connected: bool = False
        self._lock:      threading.Lock = threading.Lock()

    # ── Internal WebSocket callbacks ──────────────────────────────────────────

    def _on_open(self, ws):
        log.info("WS connected")
        self._connected = True
        # Ping thread
        self._ping_thread = threading.Thread(target=self._ping_loop, daemon=True)
        self._ping_thread.start()
        # Subscribe to all live sports once the Socket.IO CONNECT ack arrives

    def _on_message(self, ws, raw: str):
        # ── Socket.IO framing ──────────────────────────────────────────────
        # "2"    → PING, reply with PONG "3"
        # "0{…}" → OPEN handshake: parse pingInterval
        # "40"   → CONNECT ack: now safe to subscribe
        # "42[…]"→ EVENT message

        if raw == "2":
            with self._lock:
                if self._ws:
                    self._ws.send("3")
            return

        if raw.startswith("0"):
            try:
                hs = json.loads(raw[1:])
                self._ping_interval = hs.get("pingInterval", 20000) / 1000
                log.debug("handshake: sid=%s pingInterval=%ss",
                          hs.get("sid"), self._ping_interval)
            except Exception:
                pass
            return

        if raw == "40":
            log.info("Socket.IO connected — subscribing to sports")
            self._subscribe_sports()
            return

        if raw.startswith("42"):
            try:
                parts = json.loads(raw[2:])
                name  = parts[0]
                data  = parts[1] if len(parts) > 1 else {}

                if name == "BUFFERED_MARKET_UPDATE":
                    _handle_market_update(data)
                elif name == "EVENT_UPDATE":
                    _handle_event_update(data)

            except Exception as exc:
                log.warning("parse error: %s | raw: %.200s", exc, raw)
            return

    def _on_error(self, ws, error):
        log.error("WS error: %s", error)

    def _on_close(self, ws, status, msg):
        self._connected = False
        log.warning("WS closed: %s %s", status, msg)

    # ── Subscribe helpers ─────────────────────────────────────────────────────

    def _send(self, msg: str) -> None:
        with self._lock:
            if self._ws and self._connected:
                try:
                    self._ws.send(msg)
                except Exception as exc:
                    log.warning("send error: %s", exc)

    def _subscribe_sports(self) -> None:
        """Subscribe to all sport channels and load initial event/sport mappings."""
        for sport_id in LIVE_SPORT_IDS:
            self._send(f'42["subscribe","sport-{sport_id}"]')
            log.debug("subscribed sport-%d", sport_id)

        # Populate event→sport cache from HTTP snapshot
        threading.Thread(
            target=self._warm_event_cache, daemon=True
        ).start()

    def _warm_event_cache(self) -> None:
        """Background: HTTP-fetch all live events to build event→sport map."""
        r = _get_redis()
        for sport_id in LIVE_SPORT_IDS:
            try:
                events = fetch_live_events(sport_id, limit=100)
                pipe = r.pipeline()
                for ev in events:
                    pipe.setex(f"sp:live:event_sport:{ev['id']}", TTL_EVENTS, sport_id)
                    pipe.setex(f"sp:live:event_slug:{ev['id']}",
                               TTL_EVENTS, SPORT_SLUG_MAP.get(sport_id, ""))
                pipe.execute()
                log.debug("warm_cache sport=%d events=%d", sport_id, len(events))
                time.sleep(0.3)  # gentle rate limiting
            except Exception as exc:
                log.warning("warm_event_cache sport=%d: %s", sport_id, exc)

    # ── Ping loop ─────────────────────────────────────────────────────────────

    def _ping_loop(self) -> None:
        while not self._stop.is_set() and self._connected:
            time.sleep(self._ping_interval)
            self._send("2")  # PING

    # ── Start / stop ──────────────────────────────────────────────────────────

    def run_forever(self) -> None:
        """
        Blocking run loop with automatic reconnect.
        Call from a daemon thread or process.
        """
        backoff = 2.0
        while not self._stop.is_set():
            log.info("Connecting to %s", WS_URL)
            try:
                self._ws = websocket.WebSocketApp(
                    WS_URL,
                    header=[
                        f"Origin: {ORIGIN}",
                        f"User-Agent: {USER_AGENT}",
                    ],
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=0)  # we handle pings manually
                backoff = 2.0  # reset on clean close
            except Exception as exc:
                log.error("run_forever error: %s", exc)

            if not self._stop.is_set():
                log.info("Reconnecting in %.0fs…", backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def stop(self) -> None:
        self._stop.set()
        self._connected = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass


# ── Module-level singleton ────────────────────────────────────────────────────

_harvester_instance: SpLiveHarvester | None = None
_harvester_thread:   threading.Thread | None = None


def start_harvester_thread() -> threading.Thread:
    """
    Start the harvester in a background daemon thread.
    Safe to call multiple times (idempotent).
    """
    global _harvester_instance, _harvester_thread

    if _harvester_thread and _harvester_thread.is_alive():
        log.info("harvester already running")
        return _harvester_thread

    _harvester_instance = SpLiveHarvester()

    def _run():
        _harvester_instance.run_forever()

    _harvester_thread = threading.Thread(target=_run, name="sp-live-harvester", daemon=True)
    _harvester_thread.start()
    log.info("harvester thread started")
    return _harvester_thread


def stop_harvester() -> None:
    global _harvester_instance
    if _harvester_instance:
        _harvester_instance.stop()


def harvester_alive() -> bool:
    return bool(_harvester_thread and _harvester_thread.is_alive())


# ── Entry point (run as script) ───────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # On startup: warm HTTP snapshot into Redis
    log.info("Warming live snapshot…")
    try:
        snapshot_all_sports()
    except Exception as exc:
        log.warning("snapshot failed: %s", exc)

    # Run WebSocket harvester (blocking)
    harvester = SpLiveHarvester()
    harvester.run_forever()