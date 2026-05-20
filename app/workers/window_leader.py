"""
app/workers/window_leader.py
==============================
Redis-based leader election for MatchWindowService.

Guarantees exactly ONE instance runs across:
  - Multiple gunicorn worker processes
  - Multiple Celery worker processes
  - Any combination of the above

How it works
────────────
  1. Any process that wants to run the window service tries to SET a Redis
     key with NX (only if not exists) and a short TTL.
  2. The process that wins the SET becomes the leader.
  3. The leader renews its lock every HEARTBEAT seconds.
  4. If the leader dies (crash, OOM, SIGKILL), the lock expires after TTL
     and another process automatically takes over.
  5. Non-leaders poll every POLL_INTERVAL seconds and take over if the
     lock disappears.

Usage (in create_app or celery worker init):
  from app.workers.window_leader import ensure_window_leader
  ensure_window_leader()

Call this from EVERY process. Only one will actually run the service.
"""
from __future__ import annotations

import logging
import os
import socket
import threading
import time
from typing import Any

log = logging.getLogger("kinetic.window_leader")

LEADER_KEY       = "kinetic:window:leader"
LEADER_TTL       = 30        # seconds — lock expires if leader dies
HEARTBEAT_EVERY  = 10        # seconds — leader renews lock this often
POLL_EVERY       = 15        # seconds — non-leaders check this often


def _redis():
    import redis as _r
    url  = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    return _r.Redis.from_url(f"{base}/2", decode_responses=True, socket_timeout=5)


def _identity() -> str:
    """Unique ID for this process — host + PID."""
    return f"{socket.gethostname()}:{os.getpid()}"


class WindowLeader:
    """
    Runs in every process. Exactly one becomes leader and runs the service.
    All others wait and take over if the leader disappears.
    """

    def __init__(self, app: Any = None):
        self.app       = app
        self._id       = _identity()
        self._r        = _redis()
        self._is_leader = False
        self._service   = None
        self._thread    = threading.Thread(
            target=self._loop, daemon=True, name="window-leader"
        )

    def start(self) -> None:
        self._thread.start()
        log.info("WindowLeader started (id=%s)", self._id)

    def _try_acquire(self) -> bool:
        """Try to become leader. Returns True if we won."""
        # SET key value NX EX ttl — atomic, only sets if key doesn't exist
        result = self._r.set(LEADER_KEY, self._id, nx=True, ex=LEADER_TTL)
        return result is True

    def _is_still_leader(self) -> bool:
        """Verify we still hold the lock (not expired and replaced)."""
        return self._r.get(LEADER_KEY) == self._id

    def _renew(self) -> bool:
        """Renew TTL. Returns False if we lost the lock (shouldn't happen normally)."""
        if not self._is_still_leader():
            return False
        self._r.expire(LEADER_KEY, LEADER_TTL)
        return True

    def _start_service(self) -> None:
        """Start the actual window service."""
        try:
            from app.workers.match_window_service import start_window_service
            self._service = start_window_service(self.app)
            log.info("[leader] MatchWindowService started (pid=%s)", os.getpid())
        except Exception as e:
            log.error("[leader] Failed to start window service: %s", e)
            self._is_leader = False

    def _stop_service(self) -> None:
        """Stop the window service when we lose leadership."""
        try:
            if self._service:
                self._service.stop()
                self._service = None
                log.info("[leader] MatchWindowService stopped (pid=%s)", os.getpid())
        except Exception as e:
            log.warning("[leader] Error stopping window service: %s", e)

    def _loop(self) -> None:
        while True:
            try:
                if self._is_leader:
                    # We're leader — keep renewing
                    if self._renew():
                        time.sleep(HEARTBEAT_EVERY)
                    else:
                        # Lost lock somehow (Redis restart? eviction?)
                        log.warning("[leader] Lost lock! Stopping service.")
                        self._is_leader = False
                        self._stop_service()
                else:
                    # Not leader — try to acquire
                    if self._try_acquire():
                        log.info("[leader] Acquired leadership (pid=%s)", os.getpid())
                        self._is_leader = True
                        self._start_service()
                    else:
                        # Someone else is leader — check who
                        current = self._r.get(LEADER_KEY)
                        log.debug("[leader] %s is leader, we wait", current)
                        time.sleep(POLL_EVERY)

            except Exception as e:
                log.error("[leader] Loop error: %s", e)
                time.sleep(5)


# ── Module-level singleton ────────────────────────────────────────────────────

_leader: WindowLeader | None = None
_lock   = threading.Lock()


def ensure_window_leader(app: Any = None) -> None:
    """
    Call this from every process (web worker, Celery worker, etc).
    Only ONE process will actually run the window service.
    Safe to call multiple times — idempotent.

    Example usage in create_app():
        from app.workers.window_leader import ensure_window_leader
        ensure_window_leader(flask_app)

    Example usage in Celery worker init signal:
        from celery.signals import worker_ready
        @worker_ready.connect
        def on_worker_ready(**kwargs):
            ensure_window_leader(flask_app)
    """
    global _leader
    if _leader is not None:
        if app is not None:
            _leader.app = app
        return  # already started in this process

    with _lock:
        if _leader is not None:
            if app is not None:
                _leader.app = app
            return
        try:
            _leader = WindowLeader(app=app)
            _leader.start()
        except Exception as e:
            log.error("Failed to start WindowLeader: %s", e)