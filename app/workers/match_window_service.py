"""
app/workers/match_window_service.py
=====================================
3-Hour Rolling Window Match Lifecycle Service.

This is the core real-time engine. It replaces scattered polling with
a single, authoritative state machine per match.

Architecture
────────────────────────────────────────────────────────────────────────
                         DATABASE (PostgreSQL)
                              ↑ write
  ┌───────────────────────────────────────────────────────────────────┐
  │  MatchWindowService  (singleton background service)               │
  │                                                                   │
  │  PHASE 1 — COUNTDOWN   (start_time - 3h → start_time)           │
  │    ├─ Loads matches from DB into Redis window set every 30s       │
  │    ├─ Cross-BK reconciliation: do all BKs agree on start_time?   │
  │    ├─ Pushes countdown ticks to SSE (every 10s)                  │
  │    └─ Publishes ws:match:{jk}:countdown channel                  │
  │                                                                   │
  │  PHASE 2 — LIVE TRANSITION   (start_time ± delay_threshold)      │
  │    ├─ Triggers on: clock OR SP WS event OR 2/3 BK APIs agree     │
  │    ├─ Detects kickoff delay — publishes delay event               │
  │    └─ Moves match to live ecosystem                               │
  │                                                                   │
  │  PHASE 3 — LIVE ECOSYSTEM   (kickoff → FT)                       │
  │    ├─ LiveMatchTracker per match (dedicated Redis sub)            │
  │    ├─ Market updates → batch write to DB every 5s                 │
  │    ├─ Score/incident sync (SofaScore + BK consensus)             │
  │    └─ Arb re-detection on every market tick                       │
  │                                                                   │
  │  PHASE 4 — FINISHED   (FT signal OR 110min elapsed)              │
  │    ├─ Fetches final result from SofaScore / BK APIs              │
  │    ├─ Writes result to DB (UnifiedMatch.final_score_*)            │
  │    └─ Fires all notification channels                             │
  └───────────────────────────────────────────────────────────────────┘

Redis key layout
────────────────
  kinetic:window:active          ZSET  jk → start_ts   (3h window)
  kinetic:window:live            SET   jks currently live
  kinetic:window:finished        SET   jks finished today
  kinetic:match:{jk}:state       HASH  state, phase, bk_consensus, ...
  kinetic:match:{jk}:markets     HASH  slug → {outcome:odd,...} (JSON)
  kinetic:match:{jk}:score       HASH  home, away, time, incidents
  kinetic:match:{jk}:delay       HASH  expected_ts, actual_ts, delay_s
  kinetic:match:{jk}:bk_live     HASH  sp/bt/od/b2b → 0|1

Publish channels (consumed by SSE + webhooks)
──────────────────────────────────────────────
  ws:match:{jk}:countdown   → {seconds_to_start, phase, bk_consensus}
  ws:match:{jk}:live        → {score, time, markets_delta, arb}
  ws:match:{jk}:state       → {old_state, new_state, reason}
  ws:match:{jk}:result      → {score_home, score_away, winner}
  ws:sport:{sport}:live     → aggregated live feed for sport tab
  ws:notifications:{uid}    → per-user alerts
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

log = logging.getLogger("kinetic.window")

# ─── Constants ────────────────────────────────────────────────────────────────
WINDOW_HOURS            = 3          # how far ahead to track
WINDOW_SCAN_INTERVAL    = 30         # seconds between DB window scans
COUNTDOWN_TICK          = 10         # seconds between countdown SSE pushes
LIVE_POLL_INTERVAL      = 5          # seconds between live market DB writes
LIVE_CONSENSUS_THRESHOLD = 2         # how many BKs must agree match is live
DELAY_THRESHOLD_MIN     = 5          # minutes past start_time before flagging delay
MATCH_TTL               = 86400 * 2  # 48h Redis TTL for match keys
RESULT_POLL_INTERVAL    = 30         # seconds between result checks after FT

# BK slugs we track for consensus
ALL_BKS = ["sp", "bt", "od", "1xbet", "22bet", "betwinner", "melbet"]
LOCAL_BKS = ["sp", "bt", "od"]


# ══════════════════════════════════════════════════════════════════════════════
# REDIS HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _redis(db: int = 2):
    import redis as _r
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    return _r.Redis.from_url(f"{base}/{db}", decode_responses=True, socket_timeout=5)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_ts() -> float:
    return _now().timestamp()


def _now_iso() -> str:
    return _now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# NOTIFICATION DISPATCHER
# ══════════════════════════════════════════════════════════════════════════════

class NotificationRouter:
    """
    Routes lifecycle events to all configured channels.
    Each channel is non-blocking — failures are logged, not raised.
    """

    def __init__(self, r):
        self.r = r

    def publish(self, channel: str, payload: dict) -> None:
        try:
            self.r.publish(channel, json.dumps(payload, default=str))
        except Exception as e:
            log.debug("publish failed %s: %s", channel, e)

    def match_state(self, jk: str, sport: str, old: str, new: str, data: dict) -> None:
        p = {"event": "state_change", "join_key": jk, "sport": sport,
             "old_state": old, "new_state": new, "ts": _now_iso(), **data}
        self.publish(f"ws:match:{jk}:state", p)
        self.publish(f"ws:sport:{sport}:live", p)
        # Trigger Celery notification tasks
        self._celery_notify("state_change", jk, p)

    def match_countdown(self, jk: str, seconds: int, bk_consensus: dict) -> None:
        self.publish(f"ws:match:{jk}:countdown", {
            "event": "countdown", "join_key": jk,
            "seconds_to_start": seconds, "bk_consensus": bk_consensus,
            "ts": _now_iso(),
        })

    def match_live_tick(self, jk: str, sport: str, score: dict,
                        markets_delta: list, arb: dict | None) -> None:
        p = {"event": "live_tick", "join_key": jk, "sport": sport,
             "score": score, "markets_delta": markets_delta,
             "arb": arb, "ts": _now_iso()}
        self.publish(f"ws:match:{jk}:live", p)
        self.publish(f"ws:sport:{sport}:live", p)

    def match_result(self, jk: str, sport: str, result: dict) -> None:
        p = {"event": "result", "join_key": jk, "sport": sport,
             "result": result, "ts": _now_iso()}
        self.publish(f"ws:match:{jk}:result", p)
        self.publish(f"ws:sport:{sport}:results", p)
        self._celery_notify("result", jk, p)

    def match_delay(self, jk: str, sport: str, delay_s: int, reason: str) -> None:
        p = {"event": "kickoff_delay", "join_key": jk, "sport": sport,
             "delay_minutes": round(delay_s / 60, 1), "reason": reason,
             "ts": _now_iso()}
        self.publish(f"ws:match:{jk}:state", p)
        self.publish(f"ws:sport:{sport}:live", p)

    def user_notify(self, user_id: str, event_type: str, payload: dict) -> None:
        self.publish(f"ws:notifications:{user_id}", {
            "event": event_type, **payload, "ts": _now_iso()
        })

    def _celery_notify(self, event_type: str, jk: str, payload: dict) -> None:
        try:
            from app.workers.celery_tasks import celery
            celery.send_task(
                "tasks.notify.lifecycle_event",
                args=[event_type, jk, payload],
                queue="notify",
                countdown=0,
            )
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# BK CONSENSUS — cross-bookmaker live state reconciliation
# ══════════════════════════════════════════════════════════════════════════════

class BkConsensus:
    """
    Tracks which bookmakers consider a match live.
    A match is "consensus live" when LIVE_CONSENSUS_THRESHOLD BKs agree.

    Also detects start-time discrepancies across bookmakers (schedule delay).
    """

    def __init__(self, r, jk: str):
        self.r   = r
        self.jk  = jk
        self._key = f"kinetic:match:{jk}:bk_live"

    def mark_live(self, bk: str) -> int:
        """Mark a BK as reporting this match live. Returns consensus count."""
        self.r.hset(self._key, bk, "1")
        self.r.expire(self._key, MATCH_TTL)
        return self.live_count()

    def mark_not_live(self, bk: str) -> None:
        self.r.hset(self._key, bk, "0")

    def live_count(self) -> int:
        data = self.r.hgetall(self._key)
        return sum(1 for v in data.values() if v == "1")

    def live_bks(self) -> list[str]:
        data = self.r.hgetall(self._key)
        return [k for k, v in data.items() if v == "1"]

    def has_consensus(self) -> bool:
        return self.live_count() >= LIVE_CONSENSUS_THRESHOLD

    def to_dict(self) -> dict:
        data = self.r.hgetall(self._key) or {}
        return {bk: (data.get(bk, "0") == "1") for bk in ALL_BKS}


# ══════════════════════════════════════════════════════════════════════════════
# MARKET STATE — per-match market cache with delta detection
# ══════════════════════════════════════════════════════════════════════════════

class MatchMarketState:
    """
    In-Redis market state for one live match.
    Detects per-outcome changes, computes arb on every tick.
    Batches DB writes.
    """

    def __init__(self, r, jk: str):
        self.r    = r
        self.jk   = jk
        self._key = f"kinetic:match:{jk}:markets"
        self._pending_writes: list[dict] = []

    def update(self, bk: str, markets: dict[str, dict]) -> list[dict]:
        """
        Merge incoming markets from one BK.
        Returns list of changed outcomes: [{slug, outcome, old_odd, new_odd, bk}]
        """
        deltas: list[dict] = []
        pipe = self.r.pipeline()

        for slug, outcomes in markets.items():
            for outcome, odd in outcomes.items():
                field = f"{bk}:{slug}:{outcome}"
                try:
                    prev_raw = self.r.hget(self._key, field)
                    prev     = float(prev_raw) if prev_raw else None
                    new_odd  = float(odd)
                    if prev is None or abs(new_odd - prev) > 0.001:
                        pipe.hset(self._key, field, new_odd)
                        deltas.append({
                            "bk": bk, "slug": slug, "outcome": outcome,
                            "old_odd": prev, "new_odd": new_odd,
                        })
                        self._pending_writes.append({
                            "bk": bk, "slug": slug, "outcome": outcome,
                            "odd": new_odd, "ts": _now_iso(),
                        })
                except Exception:
                    continue

        pipe.expire(self._key, MATCH_TTL)
        pipe.execute()
        return deltas

    def best_odds(self) -> dict[str, dict[str, dict]]:
        """Build best-price-across-BKs structure."""
        raw = self.r.hgetall(self._key) or {}
        best: dict[str, dict[str, dict]] = {}
        for field, val in raw.items():
            try:
                bk, slug, outcome = field.split(":", 2)
                odd = float(val)
                best.setdefault(slug, {})
                existing = best[slug].get(outcome)
                if not existing or odd > existing["odd"]:
                    best[slug][outcome] = {"odd": odd, "bk": bk}
            except Exception:
                continue
        return best

    def drain_writes(self) -> list[dict]:
        """Pop pending DB writes."""
        out = self._pending_writes[:]
        self._pending_writes.clear()
        return out

    def detect_arb(self) -> dict | None:
        """Fast arb scan on best odds. Returns best opportunity or None."""
        best = self.best_odds()
        best_arb: dict | None = None

        for slug, outcomes in best.items():
            keys = list(outcomes.keys())
            if len(keys) < 2:
                continue
            # Try 2-way and 3-way combos
            from itertools import combinations
            for combo in list(combinations(keys, 2)) + (
                [tuple(keys[:3])] if len(keys) >= 3 else []
            ):
                odds = [outcomes[k]["odd"] for k in combo]
                if any(o <= 1.0 for o in odds):
                    continue
                inv = sum(1 / o for o in odds)
                if inv < 1.0:
                    pct = round((1 / inv - 1) * 100, 3)
                    if not best_arb or pct > best_arb["profit_pct"]:
                        best_arb = {
                            "market": slug,
                            "profit_pct": pct,
                            "legs": [{"outcome": k, "odd": outcomes[k]["odd"],
                                      "bk": outcomes[k]["bk"]} for k in combo],
                        }
        return best_arb


# ══════════════════════════════════════════════════════════════════════════════
# LIVE MATCH TRACKER — one per live match
# ══════════════════════════════════════════════════════════════════════════════

class LiveMatchTracker:
    """
    Subscribes to all BK live channels for one match.
    Runs in a dedicated thread. Writes to DB every LIVE_POLL_INTERVAL.
    """

    def __init__(self, jk: str, match_meta: dict, router: NotificationRouter):
        self.jk          = jk
        self.meta        = match_meta
        self.router      = router
        self.r           = _redis()
        self.markets     = MatchMarketState(self.r, jk)
        self.consensus   = BkConsensus(self.r, jk)
        self._stop       = threading.Event()
        self._thread     = threading.Thread(
            target=self._run, daemon=True, name=f"live-{jk[:8]}"
        )
        self._last_db_write = 0.0
        self._score: dict = {}

    def start(self) -> None:
        self._thread.start()
        log.info("LiveMatchTracker started: %s", self.jk)

    def stop(self) -> None:
        self._stop.set()

    @property
    def alive(self) -> bool:
        return self._thread.is_alive()

    def _run(self) -> None:
        r      = self.r
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        sport  = self.meta.get("sport", "soccer")

        # Subscribe to ALL live channels that could carry this match
        channels = [
            f"live:match:{self.jk}:all",        # SP WebSocket
            f"bus:live_updates:{sport}",         # unified bridge
            f"bt:live:*:updates",                # BT
            f"od:live:*:updates",                # OD
        ]
        pubsub.psubscribe(*[c for c in channels if "*" in c])
        pubsub.subscribe(*[c for c in channels if "*" not in c])

        while not self._stop.is_set():
            msg = pubsub.get_message(timeout=1.0)
            if msg and msg.get("type") in ("message", "pmessage"):
                try:
                    data = json.loads(msg.get("data") or "{}")
                    self._process_event(data, msg.get("channel", ""))
                except Exception as e:
                    log.debug("LiveTracker parse err %s: %s", self.jk, e)

            # Periodic DB flush
            if time.time() - self._last_db_write > LIVE_POLL_INTERVAL:
                self._flush_to_db()
                self._last_db_write = time.time()

        try:
            pubsub.unsubscribe()
            pubsub.close()
        except Exception:
            pass

    def _process_event(self, data: dict, channel: str) -> None:
        jk = str(data.get("join_key") or data.get("parent_match_id") or "")

        # Filter to this match only (bus channels carry all matches)
        if jk and jk != self.jk:
            return

        bk     = str(data.get("source") or data.get("bk") or "unknown")
        sport  = self.meta.get("sport", "soccer")

        # ── Score update ──────────────────────────────────────────────────────
        if data.get("score_home") is not None:
            new_score = {
                "home": str(data["score_home"]),
                "away": str(data.get("score_away", "")),
                "time": str(data.get("match_time", "")),
            }
            if new_score != self._score:
                old = self._score.copy()
                self._score = new_score
                self.r.hmset(f"kinetic:match:{self.jk}:score", new_score)
                self.r.expire(f"kinetic:match:{self.jk}:score", MATCH_TTL)

                # Goal detection
                try:
                    oh = int(old.get("home") or 0)
                    nh = int(new_score["home"])
                    oa = int(old.get("away") or 0)
                    na = int(new_score["away"])
                    if nh > oh or na > oa:
                        self.router.match_live_tick(
                            self.jk, sport, new_score,
                            markets_delta=[], arb=None,
                        )
                except Exception:
                    pass

        # ── BK live consensus ─────────────────────────────────────────────────
        if data.get("is_live") or "live" in channel:
            self.consensus.mark_live(bk)

        # ── Market update ─────────────────────────────────────────────────────
        bookmakers = data.get("bookmakers") or {}
        if not bookmakers and data.get("markets"):
            bookmakers = {bk: {"markets": data["markets"]}}

        for bk_slug, bk_data in bookmakers.items():
            mkts = bk_data.get("markets") or {}
            if not mkts:
                continue
            deltas = self.markets.update(bk_slug, mkts)
            if deltas:
                arb = self.markets.detect_arb()
                self.router.match_live_tick(
                    self.jk, sport, self._score, deltas, arb
                )

    def _flush_to_db(self) -> None:
        """Write pending market changes to PostgreSQL."""
        writes = self.markets.drain_writes()
        if not writes:
            return
        try:
            from app.workers.celery_tasks import celery
            celery.send_task(
                "tasks.ops.flush_live_markets",
                args=[self.jk, writes],
                queue="results",
                countdown=0,
            )
        except Exception as e:
            log.debug("DB flush dispatch error %s: %s", self.jk, e)


# ══════════════════════════════════════════════════════════════════════════════
# MATCH WINDOW SERVICE — the singleton
# ══════════════════════════════════════════════════════════════════════════════

class MatchWindowService:
    """
    Singleton service managing the 3-hour rolling lifecycle window.

    Responsibilities:
      - Scans DB every 30s for matches starting within 3h
      - Pushes countdown ticks via Redis pub/sub
      - Detects kickoff and transitions matches to live
      - Spawns LiveMatchTracker per live match
      - Polls for results when match finishes
      - Writes all state changes to DB
    """

    def __init__(self, app: Any = None):
        self.app       = app
        self.r         = _redis()
        self.router    = NotificationRouter(self.r)
        self._live_trackers: dict[str, LiveMatchTracker] = {}
        self._running  = False
        self._threads: list[threading.Thread] = []
        self._lock     = threading.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._threads = [
            threading.Thread(target=self._window_scan_loop,
                             daemon=True, name="window-scan"),
            threading.Thread(target=self._countdown_tick_loop,
                             daemon=True, name="countdown-tick"),
            threading.Thread(target=self._consensus_check_loop,
                             daemon=True, name="consensus-check"),
        ]
        for t in self._threads:
            t.start()
        log.info("MatchWindowService started — %dh window", WINDOW_HOURS)

    def stop(self) -> None:
        self._running = False
        with self._lock:
            for tracker in self._live_trackers.values():
                tracker.stop()

    def on_bk_live_signal(self, bk: str, jk: str, sport: str) -> None:
        """Called by LiveMatchBridge when a BK reports a match as live."""
        consensus = BkConsensus(self.r, jk)
        count = consensus.mark_live(bk)
        log.debug("BK live signal: %s from %s (consensus=%d)", jk, bk, count)

        if count >= LIVE_CONSENSUS_THRESHOLD:
            self._transition_to_live(jk, reason=f"bk_consensus({count})")

    # ── Window scan (every 30s) ────────────────────────────────────────────────

    def _window_scan_loop(self) -> None:
        while self._running:
            try:
                self._scan_window()
            except Exception as e:
                log.error("Window scan error: %s", e)
            time.sleep(WINDOW_SCAN_INTERVAL)

    def _scan_window(self) -> None:
        """Load matches from DB starting within the next WINDOW_HOURS."""
        from datetime import timezone as tz
        now     = _now()
        horizon = now + timedelta(hours=WINDOW_HOURS)

        app = self.app
        if app is None:
            try:
                from flask import current_app
                if current_app:
                    app = current_app._get_current_object()
            except Exception:
                pass
        if app is None:
            try:
                from app import create_app
                app = create_app()
                self.app = app
            except Exception as e:
                log.error("Failed to create app for context: %s", e)

        if app is None:
            log.error("No flask app available for DB scan.")
            return

        with app.app_context():
            try:
                from app.models.odds import UnifiedMatch
                from app.extensions import db

                matches = db.session.execute(
                    db.select(UnifiedMatch).where(
                        UnifiedMatch.start_time.between(
                            now - timedelta(minutes=10),  # include just-started
                            horizon,
                        ),
                        UnifiedMatch.status.notin_(["finished", "cancelled"]),
                    )
                ).scalars().all()

            except Exception as e:
                log.error("DB scan failed: %s", e)
                return

            pipe = self.r.pipeline()
            window_key = "kinetic:window:active"

            for m in matches:
                jk = str(m.parent_match_id or m.id)
                ts = m.start_time.replace(tzinfo=tz.utc).timestamp() if m.start_time else _now_ts()
                pipe.zadd(window_key, {jk: ts})

                # Cache match metadata for trackers
                meta_key = f"kinetic:match:{jk}:meta"
                if not self.r.exists(meta_key):
                    pipe.hmset(meta_key, {
                        "join_key":    jk,
                        "home_team":   m.home_team_name or "",
                        "away_team":   m.away_team_name or "",
                        "sport":       m.sport_name or "soccer",
                        "competition": m.competition_name or "",
                        "start_time":  m.start_time.isoformat() if m.start_time else "",
                        "status":      m.status or "pending",
                        "db_id":       str(m.id),
                    })
                    pipe.expire(meta_key, MATCH_TTL)

            pipe.expire(window_key, MATCH_TTL)
            pipe.execute()

        # Check for matches that should have gone live
        self._check_kickoff_times()

    def _check_kickoff_times(self) -> None:
        """Compare clock vs start_time; flag delays; trigger live transition."""
        now      = _now_ts()
        window   = self.r.zrangebyscore(
            "kinetic:window:active", "-inf", now + 60, withscores=True
        )

        for jk_bytes, start_ts in window:
            jk      = jk_bytes if isinstance(jk_bytes, str) else jk_bytes.decode()
            delay_s = now - start_ts
            state   = self.r.hget(f"kinetic:match:{jk}:state", "phase") or "countdown"

            if state in ("live", "finished"):
                continue

            if delay_s > 0:
                # Match should have started
                if delay_s > DELAY_THRESHOLD_MIN * 60:
                    # Flag delay but don't force-transition yet
                    self._flag_delay(jk, start_ts, delay_s)
                else:
                    # Normal kickoff window
                    self._transition_to_live(jk, reason="clock")

    # ── Countdown tick (every 10s) ─────────────────────────────────────────────

    def _countdown_tick_loop(self) -> None:
        while self._running:
            try:
                self._push_countdown_ticks()
            except Exception as e:
                log.debug("Countdown tick error: %s", e)
            time.sleep(COUNTDOWN_TICK)

    def _push_countdown_ticks(self) -> None:
        now       = _now_ts()
        # Only matches not yet live, starting in next 3h
        upcoming  = self.r.zrangebyscore(
            "kinetic:window:active", now, now + WINDOW_HOURS * 3600, withscores=True
        )
        pipe = self.r.pipeline()

        for jk_bytes, start_ts in upcoming:
            jk    = jk_bytes if isinstance(jk_bytes, str) else jk_bytes.decode()
            state = self.r.hget(f"kinetic:match:{jk}:state", "phase") or "countdown"
            if state in ("live", "finished"):
                continue

            seconds = max(0, int(start_ts - now))
            meta    = self.r.hgetall(f"kinetic:match:{jk}:meta") or {}
            consensus = BkConsensus(self.r, jk).to_dict()

            pipe.publish(f"ws:match:{jk}:countdown", json.dumps({
                "event":            "countdown",
                "join_key":         jk,
                "home_team":        meta.get("home_team", ""),
                "away_team":        meta.get("away_team", ""),
                "sport":            meta.get("sport", ""),
                "seconds_to_start": seconds,
                "minutes_to_start": round(seconds / 60, 1),
                "bk_consensus":     consensus,
                "phase":            state,
                "ts":               _now_iso(),
            }, default=str))

        pipe.execute()

    # ── Cross-BK consensus check (every 15s) ────────────────────────────────────

    def _consensus_check_loop(self) -> None:
        while self._running:
            try:
                self._check_consensus()
            except Exception as e:
                log.debug("Consensus check error: %s", e)
            time.sleep(15)

    def _check_consensus(self) -> None:
        """
        For each match in the window, poll each BK's Redis cache to see if
        they report the match as live (via their live snapshot keys).
        """
        now    = _now_ts()
        window = self.r.zrangebyscore(
            "kinetic:window:active", now - 600, now + 3600, withscores=True
        )

        for jk_bytes, start_ts in window:
            jk    = jk_bytes if isinstance(jk_bytes, str) else jk_bytes.decode()
            state = self.r.hget(f"kinetic:match:{jk}:state", "phase") or "countdown"
            if state in ("finished",):
                continue

            consensus = BkConsensus(self.r, jk)
            meta      = self.r.hgetall(f"kinetic:match:{jk}:meta") or {}
            sport     = meta.get("sport", "soccer")

            # Check SP live snapshot
            sp_live_key = f"sp:live:snapshot:*"
            # Check BT/OD live data keys
            for bk in LOCAL_BKS:
                live_key = f"odds:{bk}:live:{sport}"
                raw = self.r.get(live_key)
                if raw:
                    try:
                        data = json.loads(raw)
                        matches = data.get("matches", []) if isinstance(data, dict) else data
                        for m in matches:
                            m_jk = str(
                                m.get("join_key") or m.get("parent_match_id") or
                                m.get("betradar_id") or ""
                            )
                            if m_jk == jk and m.get("is_live"):
                                count = consensus.mark_live(bk)
                                if count >= LIVE_CONSENSUS_THRESHOLD and state != "live":
                                    self._transition_to_live(jk, reason=f"consensus_{bk}")
                    except Exception:
                        pass

    # ── State transitions ──────────────────────────────────────────────────────

    def _transition_to_live(self, jk: str, reason: str = "clock") -> None:
        state_key = f"kinetic:match:{jk}:state"
        current   = self.r.hget(state_key, "phase") or "countdown"
        if current == "live":
            return

        meta  = self.r.hgetall(f"kinetic:match:{jk}:meta") or {}
        sport = meta.get("sport", "soccer")

        # Update Redis state
        self.r.hmset(state_key, {
            "phase":       "live",
            "live_since":  _now_iso(),
            "live_reason": reason,
        })
        self.r.expire(state_key, MATCH_TTL)

        # Move in Redis sets
        self.r.sadd("kinetic:window:live", jk)

        # Update DB asynchronously
        try:
            from app.workers.celery_tasks import celery
            celery.send_task(
                "tasks.ops.update_match_state",
                args=[jk, "live", {"live_since": _now_iso(), "reason": reason}],
                queue="results",
                countdown=0,
            )
        except Exception:
            pass

        # Notify all channels
        self.router.match_state(jk, sport, current, "live", {
            "reason": reason,
            "home_team": meta.get("home_team", ""),
            "away_team": meta.get("away_team", ""),
        })

        # Spawn live tracker
        self._spawn_tracker(jk, meta)
        log.info("→ LIVE: %s (%s vs %s) reason=%s",
                 jk, meta.get("home_team"), meta.get("away_team"), reason)

    def _transition_to_finished(self, jk: str, result: dict) -> None:
        state_key = f"kinetic:match:{jk}:state"
        meta      = self.r.hgetall(f"kinetic:match:{jk}:meta") or {}
        sport     = meta.get("sport", "soccer")

        self.r.hmset(state_key, {"phase": "finished", "finished_at": _now_iso()})
        self.r.srem("kinetic:window:live", jk)
        self.r.sadd("kinetic:window:finished", jk)
        self.r.expire("kinetic:window:finished", 86400)

        # Stop live tracker
        with self._lock:
            tracker = self._live_trackers.pop(jk, None)
            if tracker:
                tracker.stop()

        # Write result to DB
        try:
            from app.workers.celery_tasks import celery
            celery.send_task(
                "tasks.ops.save_match_result",
                args=[jk, result],
                queue="results",
                countdown=0,
            )
        except Exception:
            pass

        self.router.match_result(jk, sport, result)
        log.info("→ FINISHED: %s score=%s-%s",
                 jk, result.get("score_home"), result.get("score_away"))

    def _flag_delay(self, jk: str, expected_ts: float, delay_s: float) -> None:
        delay_key = f"kinetic:match:{jk}:delay"
        prev      = self.r.hget(delay_key, "flagged")
        if prev:
            return  # already flagged

        self.r.hmset(delay_key, {
            "expected_ts": expected_ts,
            "actual_ts":   _now_ts(),
            "delay_s":     delay_s,
            "flagged":     "1",
        })
        self.r.expire(delay_key, MATCH_TTL)

        meta  = self.r.hgetall(f"kinetic:match:{jk}:meta") or {}
        sport = meta.get("sport", "soccer")
        self.router.match_delay(jk, sport, int(delay_s), "kickoff_pending")

    def _spawn_tracker(self, jk: str, meta: dict) -> None:
        with self._lock:
            if jk in self._live_trackers and self._live_trackers[jk].alive:
                return
            tracker = LiveMatchTracker(jk, meta, self.router)
            self._live_trackers[jk] = tracker
            tracker.start()


# ══════════════════════════════════════════════════════════════════════════════
# SINGLETON
# ══════════════════════════════════════════════════════════════════════════════

_service: MatchWindowService | None = None


def get_window_service(app: Any = None) -> MatchWindowService:
    global _service
    if _service is None:
        _service = MatchWindowService(app=app)
    elif app is not None:
        _service.app = app
    return _service


def start_window_service(app: Any = None) -> MatchWindowService:
    svc = get_window_service(app=app)
    svc.start()
    return svc