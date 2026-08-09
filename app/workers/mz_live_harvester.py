"""
app/workers/mz_live_harvester.py
=================================
Mozzart Live WebSocket Harvester.
Persistent Socket.IO listener for real-time live odds & score updates.

Channels & Redis Keys:
  odds:mz:live:{sport}       — snapshot key (TTL 3600s)
  mz:live:all                — pub/sub channel (all live events)
  mz:live:sport:{sport_id}   — pub/sub per-sport
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any

import requests
try:
    import websocket
    _WS_OK = True
except ImportError:
    _WS_OK = False

from app.workers.mappers.mozzart import MozzartMapper, MZ_SPORT_SLUGS

log = logging.getLogger("mz_live")

WS_URL     = "wss://www.mozzartbet.co.ke/socket.io/?EIO=3&transport=websocket"
ORIGIN     = "https://www.mozzartbet.co.ke"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

# All 11+ Mozzart live sport IDs
LIVE_SPORT_IDS = [1, 2, 3, 5, 20, 23, 29, 110, 111, 137, 155]


def _get_redis():
    """Connect to Redis for Mozzart live snapshots and pub/sub."""
    import redis
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(url, decode_responses=True, socket_connect_timeout=3, socket_timeout=3)


class MozzartLiveHarvester:
    """
    Background WebSocket thread subscribing to Mozzart live feeds.
    Pushes normalized live odds directly to Redis.
    """

    def __init__(self):
        self._ws: websocket.WebSocketApp | None = None
        self._thread: threading.Thread | None = None
        self._running = False
        self._last_ping = 0.0
        self._lock = threading.Lock()
        self._redis = None
        self.live_state: dict[int, dict] = {}  # matchId -> normalized match dict

    @property
    def r(self):
        if self._redis is None:
            self._redis = _get_redis()
        return self._redis

    def start(self):
        if not _WS_OK:
            log.error("websocket-client library not installed; cannot start Mozzart WS harvester")
            return
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._run_loop, name="MozzartWsLive", daemon=True)
            self._thread.start()
            log.info("Mozzart Live WebSocket thread started")

    def stop(self):
        with self._lock:
            self._running = False
            if self._ws:
                try:
                    self._ws.close()
                except Exception:
                    pass

    def _run_loop(self):
        backoff = 1
        while self._running:
            try:
                log.info("Connecting to Mozzart WebSocket: %s", WS_URL)
                self._ws = websocket.WebSocketApp(
                    WS_URL,
                    header={"User-Agent": USER_AGENT, "Origin": ORIGIN},
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=25, ping_timeout=10)
                backoff = 1
            except Exception as exc:
                log.error("Mozzart WS connection crashed: %s", exc)
                time.sleep(min(backoff, 30))
                backoff *= 2

    def _on_open(self, ws):
        log.info("Mozzart WebSocket connection opened. Activating live room...")
        # 1. Activate live room
        ws.send('42["activateLive",{"room":"liveroom_en","lang":"en"}]')
        # 2. Subscribe to all supported sports
        for sport_id in LIVE_SPORT_IDS:
            ws.send(f'42["subLiveSport",{sport_id},"WEB_MAIN","en"]')
            time.sleep(0.05)

    def _on_message(self, ws, message: str):
        if not message:
            return
        # Handle Engine.IO ping/pong
        if message == "2":
            ws.send("3")  # pong
            return

        if not message.startswith("42"):
            return

        try:
            payload = json.loads(message[2:])
            if not isinstance(payload, list) or len(payload) < 2:
                return
            event_name = payload[0]
            event_data = payload[1]

            if event_name == "livematches":
                self._handle_live_matches(event_data)
            elif event_name == "live_match":
                self._handle_single_live_match(event_data)
            elif event_name == "destroyed":
                self._handle_destroyed(event_data)
        except Exception as exc:
            log.debug("Error processing Mozzart WS msg: %s", exc)

    def _on_error(self, ws, error):
        log.warning("Mozzart WS error: %s", error)

    def _on_close(self, ws, status, msg):
        log.info("Mozzart WS closed (%s: %s)", status, msg)

    def _handle_live_matches(self, data: dict):
        """Process bulk grouped match snapshots."""
        grouped = data.get("grouped", {})
        if not isinstance(grouped, dict):
            return

        for match_id_str, match_info in grouped.items():
            if isinstance(match_info, dict):
                self._parse_and_store_match(match_info)

        self._flush_snapshots()

    def _handle_single_live_match(self, match_info: dict):
        """Process fast-paced single match update frame."""
        if isinstance(match_info, dict):
            self._parse_and_store_match(match_info)
            self._flush_snapshots()

    def _handle_destroyed(self, data: Any):
        """Handle match removal when event ends."""
        if isinstance(data, str) and data.startswith("en_l_"):
            # Extract match ID from string e.g. "en_l_71913018_full"
            parts = data.split("_")
            if len(parts) >= 3:
                try:
                    mid = int(parts[2])
                    self.live_state.pop(mid, None)
                except ValueError:
                    pass

    def _parse_and_store_match(self, raw: dict):
        """Extract and normalize match data + odds."""
        match_id = raw.get("id") or raw.get("matchId")
        if not match_id:
            return

        comp = raw.get("competitionName") or ""
        country = raw.get("countryName") or ""
        participants = raw.get("participants") or []
        home_name = participants[0].get("name", "") if len(participants) > 0 else ""
        away_name = participants[1].get("name", "") if len(participants) > 1 else ""

        if not home_name or not away_name:
            return

        # Sourcing IDs distinctly
        betradar_id = str(raw.get("betradarStreamId") or raw.get("betradarId") or "").strip()
        sms_id = str(raw.get("matchNumber") or "").strip()
        bookmaker_match_id = str(match_id)
        parent_match_id = f"mz_{bookmaker_match_id}"

        # Scores & state
        add_info = raw.get("additionalMatchInfo") or {}
        score_info = add_info.get("info") or {}
        score_home = str(score_info.get("home", "0"))
        score_away = str(score_info.get("visitor", "0"))
        event_status = raw.get("eventStatus", "")

        # Sport mapping
        comp_info = raw.get("competition") or {}
        sport_id = comp_info.get("sport", {}).get("id") or raw.get("sportId") or 1
        sport_slug = MZ_SPORT_SLUGS.get(sport_id, "soccer")

        # Parse markets from groupOfSuperGames
        normalized_markets: dict[str, dict[str, float]] = {}
        gosg = raw.get("groupOfSuperGames") or {}

        for group_id, mkt_list in gosg.items():
            if not isinstance(mkt_list, list):
                continue
            for mkt in mkt_list:
                game_id = mkt.get("gameId")
                if not game_id:
                    continue
                raw_name = mkt.get("name", "")
                special_type = mkt.get("specialType", "")
                special_value = mkt.get("specialValue", "")

                canonical_slug = MozzartMapper.get_canonical_slug(
                    game_id, special_value, raw_name, special_type
                )
                normalized_markets.setdefault(canonical_slug, {})

                for sg in mkt.get("subgames", []):
                    if not sg.get("active", True):
                        continue
                    sn = str(sg.get("shortName", "") or sg.get("name", "")).strip()
                    val = sg.get("value")
                    try:
                        price = float(val)
                    except (TypeError, ValueError):
                        continue

                    if price <= 1.0:
                        continue

                    out_key = MozzartMapper.normalize_outcome_key(canonical_slug, sn)
                    normalized_markets[canonical_slug][out_key] = price

        match_payload = {
            "match_id": parent_match_id,
            "bookmaker_match_id": bookmaker_match_id,
            "sms_id": sms_id,
            "betradar_id": betradar_id,
            "home_team": home_name,
            "away_team": away_name,
            "competition": comp,
            "country": country,
            "sport": sport_slug,
            "sport_id": sport_id,
            "status": "IN_PLAY",
            "is_live": True,
            "score_home": score_home,
            "score_away": score_away,
            "event_status": event_status,
            "markets": normalized_markets,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        self.live_state[match_id] = match_payload

    def _flush_snapshots(self):
        """Write live snapshots to Redis for all sports and publish updates."""
        try:
            r = self.r
            by_sport: dict[str, list[dict]] = {}
            for match in self.live_state.values():
                sp = match["sport"]
                by_sport.setdefault(sp, []).append(match)

            for sport_slug, matches in by_sport.items():
                redis_key = f"odds:mz:live:{sport_slug}"
                r.setex(redis_key, 3600, json.dumps(matches))
                r.publish(f"mz:live:sport:{sport_slug}", json.dumps(matches))

            # Master live channel
            all_matches = list(self.live_state.values())
            r.publish("mz:live:all", json.dumps({"count": len(all_matches), "timestamp": time.time()}))
        except Exception as exc:
            log.warning("Failed flushing Mozzart live snapshot to Redis: %s", exc)


_harvester_instance: MozzartLiveHarvester | None = None

def get_mozzart_live_harvester() -> MozzartLiveHarvester:
    global _harvester_instance
    if _harvester_instance is None:
        _harvester_instance = MozzartLiveHarvester()
    return _harvester_instance
