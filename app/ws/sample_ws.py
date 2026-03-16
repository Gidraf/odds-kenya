"""
WebSocket Odds Listener Service  —  app/services/ws_odds_listener.py
=====================================================================
Real-time odds update receiver.

Architecture
────────────
  BookmakerWsListener        — base class every bookmaker adapter subclasses
  ├── on_message(raw)        — parse raw WS frame → list[OddsDelta]
  └── on_open / on_close     — connection lifecycle hooks

  OddsListenerRegistry       — singleton that tracks all running listeners
  ├── register(listener)
  ├── start_all()
  └── stop_all()

  OddsDelta                  — typed payload: one price change event
  ├── parent_match_id
  ├── bookmaker_id
  ├── market / specifier / selection / price
  └── raw                    — original frame for debugging

  _WsPriceWriter             — thread-safe batch writer that debounces DB writes.
  ├── push(delta)            — enqueues a delta
  └── _flush()               — commits a batch every FLUSH_INTERVAL_MS ms

Integration with odds_upsert
─────────────────────────────
  Single-price changes  → update_single_price()  (fast, surgical)
  Full re-sync frames   → upsert_odds_rows()     (same as harvest task)

Usage
─────
  # In your app factory or a Celery task:
  from app.services.ws_odds_listener import OddsListenerRegistry, BetwaySampleListener

  registry = OddsListenerRegistry.get_instance()
  registry.register(BetwaySampleListener(bookmaker_id=3, url="wss://ws.betway.com/odds"))
  registry.start_all()

  # To stop cleanly (e.g. on app shutdown):
  registry.stop_all()

Adding a new bookmaker
──────────────────────
  1. Subclass BookmakerWsListener.
  2. Override on_message() to parse the bookmaker's WS frame format.
  3. Return a list of OddsDelta objects — one per selection change.
  4. Register the listener in your app factory.

See BetwaySampleListener at the bottom for a complete example.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import ClassVar

log = logging.getLogger(__name__)

# ── Lazy import of upsert service (avoids circular imports at module load) ────
def _get_update_single_price():
    from app.services.odds_upsert import update_single_price
    return update_single_price

def _get_upsert_odds_rows():
    from app.services.odds_upsert import upsert_odds_rows
    return upsert_odds_rows

# ── websocket-client library ──────────────────────────────────────────────────
try:
    import websocket   # pip install websocket-client
    _WS_OK = True
except ImportError:
    _WS_OK = False
    log.warning("websocket-client not installed — pip install websocket-client")


# ─────────────────────────────────────────────────────────────────────────────
# DATA TYPES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OddsDelta:
    """
    One selection price update received from a WebSocket feed.

    If is_full_sync=True the listener received a full market snapshot
    (e.g. on connect or reconnect) and the raw_rows list will be used
    for a batch upsert_odds_rows() call instead of update_single_price().
    """
    parent_match_id: str | int
    bookmaker_id:    int
    market:          str
    selection:       str
    price:           float
    specifier:       str | None = None
    is_full_sync:    bool       = False      # True → use upsert_odds_rows
    raw_rows:        list[dict] = field(default_factory=list)  # for full sync
    raw_frame:       str        = ""         # original frame for debugging


# ─────────────────────────────────────────────────────────────────────────────
# FLUSH WRITER  (debounces DB writes)
# ─────────────────────────────────────────────────────────────────────────────

FLUSH_INTERVAL_MS = 200   # collect deltas for 200ms, then write in one batch


class _WsPriceWriter(threading.Thread):
    """
    Background thread that drains a queue of OddsDeltas and commits them
    in small batches.  One instance is shared across all listeners.
    """

    def __init__(self, flask_app):
        super().__init__(daemon=True, name="ws-price-writer")
        self._q:     queue.Queue[OddsDelta | None] = queue.Queue()
        self._app    = flask_app
        self._stop   = threading.Event()

    def push(self, delta: OddsDelta) -> None:
        """Thread-safe enqueue from any listener thread."""
        self._q.put_nowait(delta)

    def stop(self) -> None:
        self._stop.set()
        self._q.put_nowait(None)   # unblock the get()

    def run(self) -> None:
        while not self._stop.is_set():
            batch: list[OddsDelta] = []

            # Block until the first item arrives
            try:
                first = self._q.get(timeout=1.0)
                if first is None:
                    break
                batch.append(first)
            except queue.Empty:
                continue

            # Drain everything that arrived in the same FLUSH_INTERVAL_MS window
            deadline = time.monotonic() + FLUSH_INTERVAL_MS / 1000.0
            while time.monotonic() < deadline:
                try:
                    item = self._q.get_nowait()
                    if item is None:
                        self._stop.set()
                        break
                    batch.append(item)
                except queue.Empty:
                    time.sleep(0.010)

            if batch:
                self._flush(batch)

    def _flush(self, batch: list[OddsDelta]) -> None:
        """Write a batch of deltas inside a Flask app context."""
        update_fn  = _get_update_single_price()
        upsert_fn  = _get_upsert_odds_rows()

        with self._app.app_context():
            changed = 0
            full_syncs = 0

            for delta in batch:
                try:
                    if delta.is_full_sync and delta.raw_rows:
                        # Full snapshot: use batch upsert
                        stats = upsert_fn(
                            rows         = delta.raw_rows,
                            bookmaker_id = delta.bookmaker_id,
                            commit       = False,     # we commit once below
                        )
                        changed += stats.get("odds_changed", 0) + stats.get("odds_new", 0)
                        full_syncs += 1
                    else:
                        # Single price tick
                        did_change = update_fn(
                            parent_match_id = delta.parent_match_id,
                            bookmaker_id    = delta.bookmaker_id,
                            market          = delta.market,
                            specifier       = delta.specifier,
                            selection       = delta.selection,
                            new_price       = delta.price,
                            commit          = False,  # commit once below
                        )
                        if did_change:
                            changed += 1

                except Exception as exc:
                    log.warning("WS flush error for delta %s: %s", delta, exc)

            # Single commit for the whole batch
            try:
                from app.extensions import db
                db.session.commit()
                if changed or full_syncs:
                    log.debug(
                        "WS flush: %d price changes, %d full syncs (%d deltas total)",
                        changed, full_syncs, len(batch),
                    )
            except Exception as exc:
                log.error("WS flush commit failed: %s", exc)
                from app.extensions import db
                db.session.rollback()


# ─────────────────────────────────────────────────────────────────────────────
# BASE LISTENER
# ─────────────────────────────────────────────────────────────────────────────

class BookmakerWsListener:
    """
    Base class for a bookmaker WebSocket connection.

    Subclass and override:
        on_message(raw: str)  → list[OddsDelta]
        subscribe_msg()       → dict | None   (sent after connect)
    """

    RECONNECT_DELAY_S: ClassVar[int] = 5
    MAX_RECONNECT:     ClassVar[int] = 20

    def __init__(
        self,
        bookmaker_id: int,
        url:          str,
        headers:      dict[str, str] | None = None,
        name:         str | None            = None,
    ):
        self.bookmaker_id = bookmaker_id
        self.url          = url
        self.headers      = headers or {}
        self.name         = name or f"BK-{bookmaker_id}"
        self._writer:     _WsPriceWriter | None = None
        self._ws:         "websocket.WebSocketApp | None" = None
        self._thread:     threading.Thread | None = None
        self._stop        = threading.Event()
        self._reconnects  = 0

    # ── To override ──────────────────────────────────────────────────────────

    def on_message(self, raw: str) -> list[OddsDelta]:
        """
        Parse one raw WebSocket frame and return a list of OddsDeltas.
        Must not raise — log and return [] on any error.
        """
        raise NotImplementedError

    def subscribe_msg(self) -> dict | None:
        """
        Optional: return a JSON-serialisable dict to send after connect.
        Return None to skip.
        """
        return None

    def ping_msg(self) -> dict | None:
        """
        Optional: return a keep-alive ping payload to send periodically.
        Many bookmaker WS servers require application-level pings.
        Return None to skip.
        """
        return None

    # ── Internal WS callbacks ─────────────────────────────────────────────────

    def _on_open(self, ws):
        self._reconnects = 0
        log.info("[%s] WS connected → %s", self.name, self.url[:80])
        sub = self.subscribe_msg()
        if sub:
            ws.send(json.dumps(sub))
            log.debug("[%s] Subscription sent: %s", self.name, sub)

    def _on_message(self, ws, raw: str):
        try:
            deltas = self.on_message(raw)
        except Exception as exc:
            log.warning("[%s] on_message raised: %s", self.name, exc)
            return
        if self._writer and deltas:
            for d in deltas:
                self._writer.push(d)

    def _on_error(self, ws, err):
        log.warning("[%s] WS error: %s", self.name, err)

    def _on_close(self, ws, code, msg):
        log.info("[%s] WS closed (code=%s msg=%s)", self.name, code, msg)
        if not self._stop.is_set():
            self._schedule_reconnect()

    def _schedule_reconnect(self):
        self._reconnects += 1
        if self._reconnects > self.MAX_RECONNECT:
            log.error("[%s] Max reconnects reached — giving up", self.name)
            return
        delay = min(self.RECONNECT_DELAY_S * self._reconnects, 60)
        log.info("[%s] Reconnecting in %ss (attempt %d)…",
                 self.name, delay, self._reconnects)
        threading.Timer(delay, self._connect).start()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self, writer: _WsPriceWriter) -> None:
        if not _WS_OK:
            raise RuntimeError("websocket-client not installed")
        self._writer = writer
        self._stop.clear()
        self._connect()

    def _connect(self):
        self._ws = websocket.WebSocketApp(
            self.url,
            header          = [f"{k}: {v}" for k, v in self.headers.items()],
            on_open         = self._on_open,
            on_message      = self._on_message,
            on_error        = self._on_error,
            on_close        = self._on_close,
        )
        self._thread = threading.Thread(
            target  = self._ws.run_forever,
            kwargs  = {"ping_interval": 30, "ping_timeout": 10},
            daemon  = True,
            name    = f"ws-{self.name}",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────────────────
# REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

class OddsListenerRegistry:
    """
    Singleton that owns all BookmakerWsListeners and the shared writer thread.

    Usage:
        registry = OddsListenerRegistry.get_instance(flask_app)
        registry.register(MyListener(bookmaker_id=3, url="wss://…"))
        registry.start_all()
    """

    _instance: "OddsListenerRegistry | None" = None

    def __init__(self, flask_app):
        self._app       = flask_app
        self._listeners: list[BookmakerWsListener] = []
        self._writer:    _WsPriceWriter | None     = None
        self._running    = False

    @classmethod
    def get_instance(cls, flask_app=None) -> "OddsListenerRegistry":
        if cls._instance is None:
            if flask_app is None:
                raise RuntimeError(
                    "OddsListenerRegistry not yet initialised — pass flask_app on first call"
                )
            cls._instance = cls(flask_app)
        return cls._instance

    def register(self, listener: BookmakerWsListener) -> None:
        self._listeners.append(listener)
        log.info("[Registry] Registered listener: %s → %s",
                 listener.name, listener.url[:80])

    def start_all(self) -> None:
        if self._running:
            log.warning("[Registry] Already running — call stop_all() first")
            return
        self._writer = _WsPriceWriter(self._app)
        self._writer.start()
        for listener in self._listeners:
            try:
                listener.start(self._writer)
                log.info("[Registry] Started: %s", listener.name)
            except Exception as exc:
                log.error("[Registry] Failed to start %s: %s", listener.name, exc)
        self._running = True
        log.info("[Registry] %d listener(s) running", len(self._listeners))

    def stop_all(self) -> None:
        for listener in self._listeners:
            try:
                listener.stop()
            except Exception as exc:
                log.warning("[Registry] Error stopping %s: %s", listener.name, exc)
        if self._writer:
            self._writer.stop()
            self._writer.join(timeout=5)
        self._running = False
        log.info("[Registry] All listeners stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    def status(self) -> list[dict]:
        return [
            {"name": l.name, "url": l.url, "bookmaker_id": l.bookmaker_id,
             "reconnects": l._reconnects}
            for l in self._listeners
        ]


# ─────────────────────────────────────────────────────────────────────────────
# EXAMPLE ADAPTER — Betway-style generic WS
# ─────────────────────────────────────────────────────────────────────────────
# Replace the parsing logic with the actual bookmaker frame format.
# The frame shapes shown here are illustrative.

class BetwaySampleListener(BookmakerWsListener):
    """
    Example adapter for a bookmaker whose WS sends frames in one of two shapes:

    Price tick (single selection update):
    {
      "type": "price_update",
      "event_id": "12345",
      "market": "1X2",
      "specifier": null,
      "selection": "Home",
      "price": 1.92
    }

    Full snapshot (sent on connect / reconnect):
    {
      "type": "snapshot",
      "events": [
        {
          "event_id": "12345",
          "home": "Arsenal",  "away": "Chelsea",
          "kickoff": "2025-06-01T15:00:00Z",
          "sport": "Football", "competition": "Premier League",
          "markets": [
            {
              "market": "1X2", "specifier": null,
              "selections": [
                {"name": "Home",  "price": 1.92},
                {"name": "Draw",  "price": 3.40},
                {"name": "Away",  "price": 4.20}
              ]
            }
          ]
        }
      ]
    }
    """

    def subscribe_msg(self) -> dict:
        return {"action": "subscribe", "feed": "odds", "sport": "football"}

    def ping_msg(self) -> dict:
        return {"action": "ping"}

    def on_message(self, raw: str) -> list[OddsDelta]:
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            log.debug("[%s] Non-JSON frame ignored: %s", self.name, raw[:80])
            return []

        msg_type = frame.get("type") or frame.get("action", "")

        # ── Application-level pong — ignore ──────────────────────────────────
        if msg_type in ("pong", "heartbeat", "ping"):
            return []

        # ── Single price tick ─────────────────────────────────────────────────
        if msg_type == "price_update":
            return self._parse_tick(frame)

        # ── Full snapshot (sent on connect / subscribed channel) ──────────────
        if msg_type == "snapshot":
            return self._parse_snapshot(frame)

        # ── Unknown frame type ────────────────────────────────────────────────
        log.debug("[%s] Unhandled frame type '%s'", self.name, msg_type)
        return []

    # ── Tick parser ───────────────────────────────────────────────────────────

    def _parse_tick(self, frame: dict) -> list[OddsDelta]:
        try:
            price = float(frame["price"])
            if price <= 1.0:
                return []
            return [OddsDelta(
                parent_match_id = str(frame["event_id"]),
                bookmaker_id    = self.bookmaker_id,
                market          = str(frame["market"]),
                specifier       = frame.get("specifier"),
                selection       = str(frame["selection"]),
                price           = price,
                raw_frame       = json.dumps(frame),
            )]
        except (KeyError, ValueError, TypeError) as exc:
            log.warning("[%s] Tick parse error: %s — frame: %s",
                        self.name, exc, str(frame)[:120])
            return []

    # ── Snapshot parser ───────────────────────────────────────────────────────

    def _parse_snapshot(self, frame: dict) -> list[OddsDelta]:
        """
        Convert a full snapshot frame into OddsDelta objects with is_full_sync=True.
        Each event gets its own OddsDelta that carries the parsed rows list.
        """
        deltas: list[OddsDelta] = []
        events = frame.get("events") or []

        for event in events:
            try:
                rows = self._event_to_rows(event)
                if not rows:
                    continue
                # One OddsDelta per event in full-sync mode — the writer will
                # call upsert_odds_rows(delta.raw_rows, bookmaker_id)
                deltas.append(OddsDelta(
                    parent_match_id = str(event["event_id"]),
                    bookmaker_id    = self.bookmaker_id,
                    market          = "",        # unused in full_sync mode
                    selection       = "",
                    price           = 0.0,
                    is_full_sync    = True,
                    raw_rows        = rows,
                    raw_frame       = json.dumps(event),
                ))
            except Exception as exc:
                log.warning("[%s] Snapshot event parse error: %s", self.name, exc)

        log.debug("[%s] Snapshot: %d events parsed", self.name, len(deltas))
        return deltas

    def _event_to_rows(self, event: dict) -> list[dict]:
        """
        Convert one event object from a snapshot frame into the flat parser
        row format that upsert_odds_rows() expects.
        """
        rows: list[dict] = []
        parent_match_id = str(event.get("event_id", ""))
        home_team       = str(event.get("home", ""))
        away_team       = str(event.get("away", ""))
        start_time      = event.get("kickoff")
        sport           = event.get("sport")
        competition     = event.get("competition")

        for mkt in event.get("markets", []):
            market    = str(mkt.get("market", ""))
            specifier = mkt.get("specifier")

            for sel in mkt.get("selections", []):
                try:
                    price = float(sel["price"])
                    if price <= 1.0:
                        continue
                    rows.append({
                        "parent_match_id": parent_match_id,
                        "home_team":       home_team,
                        "away_team":       away_team,
                        "start_time":      start_time,
                        "sport":           sport,
                        "competition":     competition,
                        "market":          market,
                        "specifier":       specifier,
                        "selection":       str(sel["name"]),
                        "price":           price,
                    })
                except (KeyError, ValueError, TypeError):
                    pass

        return rows


# ─────────────────────────────────────────────────────────────────────────────
# SECOND EXAMPLE — generic "odds-change" feed (common pattern)
# ─────────────────────────────────────────────────────────────────────────────

class GenericOddsChangeFeedListener(BookmakerWsListener):
    """
    Adapter for bookmakers that push individual odds-change events like:
    {
      "op":         "odds_change",
      "match_id":   67890,
      "markets": [
        {
          "type": "Over/Under",
          "line": "2.5",
          "outcomes": [
            {"name": "Over",  "odds": 1.85},
            {"name": "Under", "odds": 1.97}
          ]
        }
      ]
    }
    """

    def on_message(self, raw: str) -> list[OddsDelta]:
        try:
            frame = json.loads(raw)
        except json.JSONDecodeError:
            return []

        if frame.get("op") != "odds_change":
            return []

        match_id = str(frame.get("match_id", ""))
        deltas: list[OddsDelta] = []

        for mkt in frame.get("markets", []):
            market    = str(mkt.get("type", ""))
            specifier = mkt.get("line")

            for outcome in mkt.get("outcomes", []):
                try:
                    price = float(outcome["odds"])
                    if price <= 1.0:
                        continue
                    deltas.append(OddsDelta(
                        parent_match_id = match_id,
                        bookmaker_id    = self.bookmaker_id,
                        market          = market,
                        specifier       = str(specifier) if specifier is not None else None,
                        selection       = str(outcome["name"]),
                        price           = price,
                        raw_frame       = raw,
                    ))
                except (KeyError, ValueError, TypeError):
                    pass

        return deltas