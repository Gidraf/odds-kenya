"""
app/workers/redis_store.py
===========================
Single write point for harvest data.

Write:  save_snapshot(bk, sport, matches, mode)
         → writes to Redis (fast cache, 2h TTL)
         → writes to DB   (permanent, no TTL)

Read:   _get_unified() in odds_stream.py calls this via fallback.

Usage in harvesters / Celery tasks:
    from app.workers.redis_store import save_snapshot
    save_snapshot("sp", "soccer", matches)   # that's it
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

log = logging.getLogger(__name__)

REDIS_TTL       = 7200   # 2 hours
LIVE_REDIS_TTL  = 120    # 2 minutes for live

# Key patterns — must match what odds_stream._merge_bks reads
_BK_KEYS: dict[str, list[str]] = {
    "sp": ["odds:sp:{mode}:{sport}", "sp:{mode}:{sport}"],
    "bt": ["odds:bt:{mode}:{sport}", "bt:{mode}:{sport}"],
    "od": ["odds:od:{mode}:{sport}", "od:{mode}:{sport}"],
}

# B2B bookmakers — same pattern
for _bk in ("1xbet", "22bet", "betwinner", "melbet", "megapari", "helabet", "paripesa"):
    _BK_KEYS[_bk] = [f"odds:{_bk}:{{mode}}:{{sport}}", f"odds:b2b:{_bk}:{{mode}}:{{sport}}"]


def _redis():
    """Get Redis client."""
    try:
        from app.workers.celery_tasks import _redis as _r
        return _r()
    except Exception:
        import redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        return redis.from_url(url, decode_responses=True)


def _write_redis(bk: str, sport: str, mode: str, matches: list) -> None:
    """Write matches to all Redis key variants for this BK."""
    try:
        r       = _redis()
        payload = json.dumps(matches, default=str)
        ttl     = LIVE_REDIS_TTL if mode == "live" else REDIS_TTL
        for pat in _BK_KEYS.get(bk, [f"odds:{bk}:{{mode}}:{{sport}}"]):
            key = pat.format(mode=mode, sport=sport)
            r.setex(key, ttl, payload)
        # Invalidate unified cache so it rebuilds with fresh data
        r.delete(f"odds:unified:{mode}:{sport}")
        log.debug("redis_store: wrote %d matches → %s/%s/%s", len(matches), bk, mode, sport)
    except Exception as exc:
        log.warning("redis_store: Redis write failed bk=%s sport=%s: %s", bk, sport, exc)


def _write_db(bk: str, sport: str, mode: str, matches: list) -> None:
    """Write matches to OddsSnapshot table (permanent storage)."""
    try:
        from app.models.odds_snapshot import OddsSnapshot
        OddsSnapshot.upsert(bk=bk, sport=sport, matches=matches, mode=mode)
        log.debug("redis_store: DB upsert %d matches → %s/%s/%s", len(matches), bk, mode, sport)
    except Exception as exc:
        log.warning("redis_store: DB write failed bk=%s sport=%s: %s", bk, sport, exc)


def save_snapshot(
    bk:      str,
    sport:   str,
    matches: list,
    mode:    str = "upcoming",
) -> None:
    """
    Dual-write: Redis (fast, TTL) + DB (permanent, no TTL).
    Call this after every successful harvest.
    """
    if not matches:
        log.debug("redis_store: skip empty snapshot bk=%s sport=%s", bk, sport)
        return
    _write_redis(bk, sport, mode, matches)
    _write_db(bk, sport, mode, matches)


def read_from_db(bk: str, sport: str, mode: str = "upcoming") -> list:
    """
    Read matches from DB (used as Redis fallback).
    Returns [] if no snapshot exists.
    """
    try:
        from app.models.odds_snapshot import OddsSnapshot
        return OddsSnapshot.get(bk=bk, sport=sport, mode=mode)
    except Exception as exc:
        log.debug("redis_store: DB read failed bk=%s sport=%s: %s", bk, sport, exc)
        return []


def publish_harvest_done(bk: str, sport: str, count: int, mode: str = "upcoming") -> None:
    """Notify SSE subscribers that a harvest completed."""
    try:
        r = _redis()
        r.publish(f"odds:all:{mode}:{sport}:updates", json.dumps({
            "event": "harvest_done", "bk": bk, "sport": sport,
            "count": count, "mode": mode,
        }))
    except Exception as exc:
        log.debug("publish_harvest_done failed: %s", exc)