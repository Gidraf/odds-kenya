"""
app/workers/redis_bus.py
=========================
Redis pub/sub bus for real-time odds delivery.

Channel topology
────────────────
  odds:{bk}:{mode}:{sport}              - latest snapshot (get/setex)
  odds:{bk}:{mode}:{sport}:updates      - pub/sub per bk
  odds:{bk}:{mode}:{sport}:page:{n}     - per-page temp key (TTL 600s)
  odds:{bk}:{mode}:{sport}:pages_done   - atomic counter (incr)
  odds:all:{mode}:{sport}:updates       - merged cross-bk pub/sub channel

Modes : upcoming | live
BKs   : sp | bt | od | b2b | sbo

Usage (publisher side):
    from app.workers.redis_bus import publish_page, publish_snapshot
    publish_page("sp", "upcoming", "soccer", page=3, matches=[...], total_pages=10)
    publish_snapshot("bt", "upcoming", "tennis", matches=[...])

Usage (subscriber side — Flask SSE or websocket):
    ps = subscribe_merged("upcoming", "soccer")
    for msg in ps.listen():
        if msg["type"] == "message":
            payload = json.loads(msg["data"])
            ...  # {"event": "snapshot_ready", "bk": "sp", "count": 312, ...}
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _r():
    from app.workers.celery_tasks import _redis
    return _redis()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── Channel names ─────────────────────────────────────────────────────────────

def ch_data(bk: str, mode: str, sport: str) -> str:
    """Redis key holding the latest snapshot JSON."""
    return f"odds:{bk}:{mode}:{sport}"


def ch_updates(bk: str, mode: str, sport: str) -> str:
    """Pub/sub channel for one bk+mode+sport combination."""
    return f"odds:{bk}:{mode}:{sport}:updates"


def ch_merged(mode: str, sport: str) -> str:
    """Pub/sub channel that receives events from ALL bookmakers for sport+mode."""
    return f"odds:all:{mode}:{sport}:updates"


def ch_page(bk: str, mode: str, sport: str, page: int) -> str:
    """Temp key for one harvested page (TTL 600 s)."""
    return f"odds:{bk}:{mode}:{sport}:page:{page}"


def ch_pages_done(bk: str, mode: str, sport: str) -> str:
    """Atomic counter: how many pages have been saved."""
    return f"odds:{bk}:{mode}:{sport}:pages_done"


# ─── Publisher helpers ────────────────────────────────────────────────────────

def publish_page(
    bk: str, mode: str, sport: str, page: int,
    matches: list, total_pages: int, ttl: int = 600,
) -> int:
    """
    Persist one page to Redis and notify subscribers.
    Returns total pages completed so far (atomic counter value).
    """
    r = _r()
    pipe = r.pipeline()
    pipe.setex(ch_page(bk, mode, sport, page), ttl, json.dumps(matches, default=str))
    pipe.incr(ch_pages_done(bk, mode, sport))
    pipe.expire(ch_pages_done(bk, mode, sport), ttl)
    _, done_count, _ = pipe.execute()

    r.publish(ch_updates(bk, mode, sport), json.dumps({
        "event":       "page_ready",
        "bk":          bk,
        "mode":        mode,
        "sport":       sport,
        "page":        page,
        "count":       len(matches),
        "total_pages": total_pages,
        "pages_done":  int(done_count),
        "ts":          _now(),
    }))
    return int(done_count)


def publish_snapshot(
    bk: str, mode: str, sport: str, matches: list,
    meta: dict | None = None, ttl: int = 3600,
) -> None:
    """
    Persist merged snapshot to Redis and broadcast on both the
    bk-specific channel and the all-bk merged channel.
    """
    r = _r()
    snapshot = {
        "bk":          bk,
        "mode":        mode,
        "sport":       sport,
        "match_count": len(matches),
        "harvested_at": _now(),
        "matches":     matches,
        **(meta or {}),
    }
    r.setex(ch_data(bk, mode, sport), ttl, json.dumps(snapshot, default=str))

    event = json.dumps({
        "event":  "snapshot_ready",
        "bk":     bk,
        "mode":   mode,
        "sport":  sport,
        "count":  len(matches),
        "ts":     _now(),
    })
    r.publish(ch_updates(bk, mode, sport), event)
    r.publish(ch_merged(mode, sport), event)

    # Reset page counter for the next harvest cycle
    r.delete(ch_pages_done(bk, mode, sport))


def publish_live_update(sport: str, betradar_id: str, payload: dict) -> None:
    """Publish a live odds delta on both sport and global live channels."""
    r = _r()
    msg = json.dumps({"ts": _now(), **payload}, default=str)
    r.publish(ch_merged("live", sport), msg)
    r.publish(f"odds:all:live:updates", msg)


# ─── Reader helpers ───────────────────────────────────────────────────────────

def merge_pages(bk: str, mode: str, sport: str, total_pages: int) -> list:
    """Read all page keys and combine into a single deduplicated match list."""
    r = _r()
    seen_ids: set = set()
    all_matches: list = []
    for page in range(1, total_pages + 1):
        raw = r.get(ch_page(bk, mode, sport, page))
        if not raw:
            continue
        try:
            for m in json.loads(raw):
                mid = (
                    m.get("betradar_id") or m.get("bt_match_id") or
                    m.get("od_match_id") or m.get("sp_game_id") or
                    f"{m.get('home_team','')}|{m.get('away_team','')}"
                )
                if mid not in seen_ids:
                    seen_ids.add(mid)
                    all_matches.append(m)
        except Exception:
            pass
    return all_matches


def pages_done_count(bk: str, mode: str, sport: str) -> int:
    """Return how many pages have been saved so far."""
    r = _r()
    val = r.get(ch_pages_done(bk, mode, sport))
    return int(val) if val else 0


def get_snapshot(bk: str, mode: str, sport: str) -> dict | None:
    """Return the cached snapshot dict or None."""
    r = _r()
    raw = r.get(ch_data(bk, mode, sport))
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return None


def get_all_snapshots(mode: str, sport: str, bks: list[str] | None = None) -> dict[str, list]:
    """Return {bk: matches} for all available snapshots for a sport+mode."""
    if bks is None:
        bks = ["sp", "bt", "od", "b2b", "sbo"]
    result: dict[str, list] = {}
    for bk in bks:
        snap = get_snapshot(bk, mode, sport)
        if snap and snap.get("matches"):
            result[bk] = snap["matches"]
    return result


# ─── Subscriber helpers ───────────────────────────────────────────────────────

def subscribe(bk: str, mode: str, sport: str):
    """Return a pubsub subscribed to one bk+mode+sport channel."""
    r = _r()
    ps = r.pubsub(ignore_subscribe_messages=True)
    ps.subscribe(ch_updates(bk, mode, sport))
    return ps


def subscribe_merged(mode: str, sport: str):
    """Return a pubsub subscribed to the all-bk merged channel."""
    r = _r()
    ps = r.pubsub(ignore_subscribe_messages=True)
    ps.subscribe(ch_merged(mode, sport))
    return ps


def subscribe_multiple(mode: str, sports: list[str]):
    """Subscribe to merged channels for multiple sports at once."""
    r = _r()
    ps = r.pubsub(ignore_subscribe_messages=True)
    channels = [ch_merged(mode, s) for s in sports]
    ps.subscribe(*channels)
    return ps