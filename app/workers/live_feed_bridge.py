"""
app/workers/live_feed_bridge.py
================================
Lightweight live data bridge. Replaces MatchLifecycleManager.

Design
──────
  • Reads SP WebSocket Redis channels (already published by sp_live_harvester.py)
  • Falls back to BT/OD Redis keys if SP has no data for a match
  • Republishes to the unified SSE channel: bus:live_updates:{sport}
  • When a match ends, fires a Celery task to save the result to DB
  • No state machine, no SofaScore, no notification threads
  • One background thread, ~5MB RAM

Redis channels consumed (written by sp_live_harvester.py)
─────────────────────────────────────────────────────────
  live:all                    — all SP live events (score + market)
  live:match:{br_id}:all      — per-match SP events

Redis keys consumed for BT/OD fallback
───────────────────────────────────────
  odds:bt:live:{sport}        — BT live snapshot (LIST)
  odds:od:live:{sport}        — OD live snapshot (LIST)

Unified channels produced (consumed by live_results_api.py SSE)
────────────────────────────────────────────────────────────────
  bus:live_updates:{sport}    — score/time/odds updates
  kinetic:match:{jk}:score    — HASH: home, away, time
  kinetic:window:live         — SET of currently live join_keys
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

log = logging.getLogger("live_feed_bridge")

# ─── Sport slug mappings (SP sport_id → slug) ─────────────────────────────────
_SP_SPORT_SLUG = {
    1: "soccer",      2: "basketball",  4: "tennis",
    5: "handball",    8: "rugby",       9: "cricket",
    10: "volleyball", 13: "table-tennis",
    126: "esoccer",
}

# ─── Match finish detection thresholds (minutes after kickoff) ────────────────
_FINISH_THRESHOLD_MIN = {
    "soccer":       105,   # 90 + 15 extra time buffer
    "basketball":   55,
    "tennis":       180,
    "ice-hockey":   75,
    "rugby":        95,
    "cricket":      600,
    "volleyball":   120,
    "table-tennis": 60,
    "esoccer":      30,
}
_DEFAULT_FINISH_MIN = 120


# =============================================================================
# REDIS
# =============================================================================

def _r():
    """Connect to the same Redis as the harvesters."""
    import redis as _redis
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return _redis.Redis.from_url(url, decode_responses=True, socket_timeout=5)


# =============================================================================
# LIVE STATE STORE  (lightweight — just a dict in memory + Redis SET)
# =============================================================================

class LiveStateStore:
    """
    Tracks which matches are currently live and their last known score.
    Stored in memory (no DB). Redis SET `kinetic:window:live` is kept in sync
    so the SSE endpoint can read it.
    """

    def __init__(self, r):
        self._r   = r
        self._matches: dict[str, dict] = {}  # join_key → state dict
        self._lock = threading.Lock()

    def update(self, join_key: str, sport: str,
               score_home=None, score_away=None,
               match_time=None, is_finished=False,
               home_team="", away_team="", competition="") -> dict:
        with self._lock:
            prev = self._matches.get(join_key, {})
            state = {
                **prev,
                "join_key":    join_key,
                "sport":       sport,
                "home_team":   home_team  or prev.get("home_team", ""),
                "away_team":   away_team  or prev.get("away_team", ""),
                "competition": competition or prev.get("competition", ""),
                "score_home":  score_home if score_home is not None else prev.get("score_home"),
                "score_away":  score_away if score_away is not None else prev.get("score_away"),
                "match_time":  match_time if match_time is not None else prev.get("match_time"),
                "is_finished": is_finished,
                "updated_at":  time.time(),
            }
            self._matches[join_key] = state

        # Keep Redis SET in sync
        try:
            pipe = self._r.pipeline()
            if is_finished:
                pipe.srem("kinetic:window:live", join_key)
            else:
                pipe.sadd("kinetic:window:live", join_key)
                # Store score hash for countdown component
                score_data = {}
                if score_home is not None: score_data["home"] = str(score_home)
                if score_away is not None: score_data["away"] = str(score_away)
                if match_time is not None: score_data["time"] = str(match_time)
                if score_data:
                    pipe.hset(f"kinetic:match:{join_key}:score", mapping=score_data)
                    pipe.expire(f"kinetic:match:{join_key}:score", 86400)
            pipe.execute()
        except Exception as exc:
            log.debug("LiveStateStore Redis sync error: %s", exc)

        return state

    def get_all_live(self) -> list[dict]:
        with self._lock:
            return [m for m in self._matches.values() if not m.get("is_finished")]

    def get(self, join_key: str) -> dict | None:
        with self._lock:
            return self._matches.get(join_key)

    def mark_finished(self, join_key: str) -> dict | None:
        with self._lock:
            m = self._matches.get(join_key)
            if m:
                m["is_finished"] = True
        if m:
            self.update(join_key, m["sport"], is_finished=True)
        return m


# =============================================================================
# RESULT SAVER (Celery task or direct DB write)
# =============================================================================

def _save_result_to_db(join_key: str, sport: str,
                       score_home, score_away,
                       home_team: str, away_team: str) -> None:
    """Save finished match result to DB. Called in a thread to not block the bridge."""
    try:
        from app.models.odds import UnifiedMatch
        from app.extensions import db
        from app import create_app

        app = create_app()
        with app.app_context():
            um = db.session.execute(
                db.select(UnifiedMatch).where(
                    UnifiedMatch.parent_match_id == join_key
                )
            ).scalar_one_or_none()

            if um:
                um.status           = "finished"
                um.final_score_home = score_home
                um.final_score_away = score_away
                um.finished_at      = datetime.now(timezone.utc)
                um.result_source    = "sp_live"
            else:
                log.debug("Result: no DB row for join_key=%s (%s vs %s)", join_key, home_team, away_team)

            db.session.commit()
            log.info("Result saved: %s %s–%s (%s)", join_key, score_home, score_away, sport)

    except Exception as exc:
        log.warning("Result DB save failed for %s: %s", join_key, exc)


# =============================================================================
# LIVE FEED BRIDGE
# =============================================================================

class LiveFeedBridge:
    """
    Single background thread that:
      1. Subscribes to SP WebSocket Redis channels (published by sp_live_harvester)
      2. Re-publishes normalised updates to bus:live_updates:{sport}
      3. Detects match finish and saves result to DB (in a separate thread)
      4. Every 30s: polls BT/OD Redis keys for matches NOT seen on SP
    """

    def __init__(self) -> None:
        self._r       = _r()
        self._store   = LiveStateStore(self._r)
        self._running = False
        self._thread: threading.Thread | None = None
        self._bt_od_thread: threading.Thread | None = None
        # Track kickoff times to detect finished matches
        self._kickoff_ts: dict[str, float] = {}  # join_key → unix timestamp

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._sp_listen, daemon=True, name="live-feed-sp"
        )
        self._bt_od_thread = threading.Thread(
            target=self._bt_od_poll_loop, daemon=True, name="live-feed-bt-od"
        )
        self._thread.start()
        self._bt_od_thread.start()
        log.info("LiveFeedBridge started")

    def stop(self) -> None:
        self._running = False

    # ── SP WebSocket channel listener ─────────────────────────────────────────

    def _sp_listen(self) -> None:
        while self._running:
            try:
                r      = _r()
                pubsub = r.pubsub(ignore_subscribe_messages=True)
                # Subscribe to SP and MZ live channels
                pubsub.subscribe("live:all", "mz:live:all")
                log.info("LiveFeedBridge: subscribed to live:all (SP) and mz:live:all (MZ)")

                while self._running:
                    msg = pubsub.get_message(timeout=2.0)
                    if msg and msg.get("type") == "message":
                        try:
                            self._handle_sp_message(json.loads(msg["data"]))
                        except Exception as exc:
                            log.debug("SP message error: %s", exc)

            except Exception as exc:
                log.error("LiveFeedBridge SP reconnect: %s", exc)
                time.sleep(5)

    def _handle_sp_message(self, data: dict) -> None:
        """Process one SP live message and republish to unified channel."""
        join_key = str(
            data.get("parent_match_id") or
            data.get("betradar_id") or
            data.get("join_key") or ""
        )
        if not join_key:
            return

        sport_id   = int(data.get("sport_id") or data.get("sportId") or 1)
        sport      = _SP_SPORT_SLUG.get(sport_id, "soccer")
        score_home = data.get("score_home")
        score_away = data.get("score_away")
        match_time = data.get("match_time") or data.get("matchTime")
        home_team  = data.get("home_team", "")
        away_team  = data.get("away_team", "")
        status     = str(data.get("status") or data.get("event_status") or "")

        # Detect finish
        is_finished = status.lower() in ("ft", "finished", "ended", "complete", "finaltime")
        if not is_finished and match_time:
            # Heuristic: match_time > threshold → finished
            try:
                mt_min = int(str(match_time).split(":")[0]) if ":" in str(match_time) else int(match_time)
                threshold = _FINISH_THRESHOLD_MIN.get(sport, _DEFAULT_FINISH_MIN)
                if mt_min >= threshold:
                    is_finished = True
            except (ValueError, TypeError):
                pass

        # Update in-memory store
        state = self._store.update(
            join_key, sport,
            score_home=score_home, score_away=score_away,
            match_time=match_time, is_finished=is_finished,
            home_team=home_team, away_team=away_team,
        )

        # Publish to unified SSE channel
        payload = {
            "source":     "sp",
            "join_key":   join_key,
            "sport":      sport,
            "home_team":  home_team,
            "away_team":  away_team,
            "score_home": score_home,
            "score_away": score_away,
            "match_time": match_time,
            "is_live":    not is_finished,
            "is_finished": is_finished,
            "bookmakers": data.get("bookmakers", {}),
            "ts":         time.time(),
        }
        try:
            self._r.publish(f"bus:live_updates:{sport}", json.dumps(payload))
        except Exception:
            pass

        # Save result if finished
        if is_finished:
            threading.Thread(
                target=_save_result_to_db,
                args=(join_key, sport, score_home, score_away, home_team, away_team),
                daemon=True,
            ).start()
            log.info("Match finished (SP): %s %s–%s", join_key, score_home, score_away)

    # ── BT/OD fallback poller ──────────────────────────────────────────────────

    def _bt_od_poll_loop(self) -> None:
        """
        Every 30s: scan BT and OD live keys for matches not seen on SP.
        Publishes their data to the unified channel as a fallback.
        """
        while self._running:
            time.sleep(30)
            try:
                self._poll_bt_od()
            except Exception as exc:
                log.debug("BT/OD poll error: %s", exc)

    def _poll_bt_od(self) -> None:
        r = _r()
        # Sports that SP covers
        sp_sports = set(_SP_SPORT_SLUG.values())

        for bk_slug in ("bt", "mz"):
            for sport in sp_sports:
                key = f"odds:{bk_slug}:live:{sport}"
                try:
                    key_type = r.type(key)
                    if key_type == "list":
                        items = r.lrange(key, 0, -1)
                        matches = []
                        for raw in items:
                            try:
                                obj = json.loads(raw)
                                if isinstance(obj, list):
                                    matches.extend(obj)
                                elif isinstance(obj, dict):
                                    matches.append(obj)
                            except Exception:
                                pass
                    elif key_type == "string":
                        raw = r.get(key)
                        if raw:
                            obj = json.loads(raw)
                            matches = obj.get("matches", obj) if isinstance(obj, dict) else obj
                        else:
                            matches = []
                    else:
                        continue

                    for m in (matches or []):
                        if not isinstance(m, dict):
                            continue
                        jk = str(
                            m.get("betradar_id") or
                            m.get("parent_match_id") or
                            m.get("join_key") or ""
                        )
                        if not jk:
                            continue

                        # Only publish if we haven't seen this from SP
                        existing = self._store.get(jk)
                        if existing and existing.get("source") == "sp":
                            continue  # SP is primary, don't overwrite

                        score_home = m.get("score_home")
                        score_away = m.get("score_away")
                        match_time = m.get("match_time")

                        state = self._store.update(
                            jk, sport,
                            score_home=score_home,
                            score_away=score_away,
                            match_time=match_time,
                            home_team=m.get("home_team", ""),
                            away_team=m.get("away_team", ""),
                        )

                        payload = {
                            "source":     bk_slug,
                            "join_key":   jk,
                            "sport":      sport,
                            "home_team":  m.get("home_team", ""),
                            "away_team":  m.get("away_team", ""),
                            "score_home": score_home,
                            "score_away": score_away,
                            "match_time": match_time,
                            "is_live":    True,
                            "is_finished": False,
                            "ts":         time.time(),
                        }
                        r.publish(f"bus:live_updates:{sport}", json.dumps(payload))

                except Exception as exc:
                    log.debug("BT/OD poll %s/%s: %s", bk_slug, sport, exc)

    # ── Public API (used by live_results_api.py) ──────────────────────────────

    def get_live_matches(self, sport: str | None = None) -> list[dict]:
        """Return all currently live matches, optionally filtered by sport."""
        matches = self._store.get_all_live()
        if sport:
            matches = [m for m in matches if m.get("sport") == sport]
        return matches

    def get_match_state(self, join_key: str) -> dict | None:
        return self._store.get(join_key)


# =============================================================================
# SINGLETON
# =============================================================================

_bridge: LiveFeedBridge | None = None


def get_live_bridge() -> LiveFeedBridge:
    global _bridge
    if _bridge is None:
        _bridge = LiveFeedBridge()
    return _bridge


def start_live_bridge() -> LiveFeedBridge:
    """Call once at app startup. Safe to call multiple times."""
    bridge = get_live_bridge()
    bridge.start()
    return bridge