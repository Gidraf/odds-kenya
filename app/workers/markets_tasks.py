"""
app/workers/market_tasks.py
============================
Celery tasks that run in background workers and push enriched market data
back to the SSE stream via Redis pub/sub.

SESSION CHANNEL PROTOCOL
─────────────────────────
Every message published to  `session:{session_id}:market_updates`  is a JSON
object with this envelope:

    {"event": "<sse_event_name>", "data": <payload_dict>}

The SSE stream handler reads this and yields:
    yield _sse(msg["event"], msg["data"])

Supported event names:
  • "live_update"  — incremental market enrichment (same shape as live updates)
  • "done"         — all enrichment tasks for this session are complete

SESSION ALIVE KEY
──────────────────
The SSE stream refreshes `session:{session_id}:alive` (TTL=90 s) on every
keepalive tick.  The live refresh task checks this key and stops re-scheduling
itself when it's gone — so Celery automatically stops polling when the client
disconnects.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import redis

from app.workers.celery_app import celery_app
from app.workers.market_normalizer import unify_match_payload, merge_best

logger = logging.getLogger(__name__)

# ── Redis helper (per-process singleton) ─────────────────────────────────────
_redis_client: redis.Redis | None = None

def _r() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
    return _redis_client


def _pub(session_id: str, event: str, data: dict) -> None:
    """Publish one SSE-shaped message to the session channel."""
    channel = f"session:{session_id}:market_updates"
    _r().publish(channel, json.dumps({"event": event, "data": data}))


def _session_alive(session_id: str) -> bool:
    return bool(_r().exists(f"session:{session_id}:alive"))


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING — per-match enrichment task
# ══════════════════════════════════════════════════════════════════════════════

@celery_app.task(
    bind=True,
    name="market_tasks.enrich_upcoming_match",
    max_retries=2,
    default_retry_delay=3,
    soft_time_limit=28,
    time_limit=35,
    acks_late=True,
)
def enrich_upcoming_match(
    self,
    brid:       str,
    sport_slug: str,
    session_id: str,
    home_team:  str = "",
    away_team:  str = "",
) -> None:
    """
    Fetch Betika + OdiBets markets for ONE upcoming match, normalise them,
    and publish a `live_update` to the session channel.
    Also decrements the session's pending counter; when it hits zero the task
    fires a `done` event so the SSE stream can close cleanly.
    """
    from app.workers.bt_harvester import get_full_markets
    from app.workers.od_harvester import fetch_event_detail, slug_to_od_sport_id

    if not _session_alive(session_id):
        # Client already disconnected — no point fetching anything
        _r().decr(f"session:{session_id}:pending")
        return

    od_sport_id = slug_to_od_sport_id(sport_slug)
    bt_markets: dict = {}
    od_markets: dict = {}

    try:
        bt_markets = get_full_markets(brid, sport_slug) or {}
    except Exception as exc:
        logger.debug("BT fetch failed brid=%s: %s", brid, exc)

    try:
        od_markets = fetch_event_detail(brid, od_sport_id)[0] or {}
    except Exception as exc:
        logger.debug("OD fetch failed brid=%s: %s", brid, exc)

    if bt_markets or od_markets:
        update: dict[str, Any] = {
            "parent_match_id": brid,
            "home_team":       home_team,
            "bookmakers":      {},
            "markets_by_bk":   {},
        }
        base = {"betradar_id": brid, "home_team": home_team, "away_team": away_team}

        if bt_markets:
            b = unify_match_payload({**base, "markets": bt_markets}, 0, "upcoming", "bt", "BETIKA")
            update["bookmakers"]["bt"]    = b["bookmakers"]["bt"]
            update["markets_by_bk"]["bt"] = b["markets_by_bk"]["bt"]

        if od_markets:
            o = unify_match_payload({**base, "markets": od_markets}, 0, "upcoming", "od", "ODIBETS")
            update["bookmakers"]["od"]    = o["bookmakers"]["od"]
            update["markets_by_bk"]["od"] = o["markets_by_bk"]["od"]

        _pub(session_id, "live_update", update)

    # ── Decrement pending counter; fire "done" when all tasks have finished ──
    remaining = _r().decr(f"session:{session_id}:pending")
    if remaining is not None and int(remaining) <= 0:
        _pub(session_id, "done", {"status": "finished", "session_id": session_id})


# ══════════════════════════════════════════════════════════════════════════════
# LIVE — periodic BT / OD refresh task
# ══════════════════════════════════════════════════════════════════════════════

# Maximum re-schedule iterations (iteration * LIVE_REFRESH_INTERVAL seconds total)
LIVE_REFRESH_INTERVAL = 30   # seconds between each BT/OD poll
LIVE_MAX_ITERATIONS   = 40   # 40 × 30 s = 20 minutes max per session


@celery_app.task(
    bind=True,
    name="market_tasks.refresh_live_bk_markets",
    max_retries=0,       # don't retry on failure — just re-schedule on next tick
    soft_time_limit=55,
    time_limit=65,
    acks_late=True,
)
def refresh_live_bk_markets(
    self,
    sport_slug:     str,
    session_id:     str,
    active_br_ids:  list[str],
    iteration:      int = 0,
) -> None:
    """
    Fetch Betika + OdiBets live market snapshots for every active betradar ID,
    diff against the last known snapshot, and publish only changed markets.

    Re-schedules itself every LIVE_REFRESH_INTERVAL seconds while the SSE
    client is still connected (session alive key present in Redis).
    """
    from app.workers.bt_harvester import fetch_live_matches as bt_fetch_live, slug_to_bt_sport_id
    from app.workers.od_harvester import fetch_live_matches as od_fetch_live

    if not _session_alive(session_id):
        logger.debug("live refresh: session %s gone, stopping", session_id)
        return

    bt_data: list = []
    od_data: list = []

    try:
        bt_data = bt_fetch_live(slug_to_bt_sport_id(sport_slug)) or []
    except Exception as exc:
        logger.debug("BT live fetch error: %s", exc)

    try:
        od_data = od_fetch_live(sport_slug) or []
    except Exception as exc:
        logger.debug("OD live fetch error: %s", exc)

    bt_map = {
        str(m.get("betradar_id") or m.get("bt_parent_id")): m
        for m in bt_data if isinstance(m, dict)
    }
    od_map = {
        str(m.get("betradar_id") or m.get("od_parent_id")): m
        for m in od_data if isinstance(m, dict)
    }

    snapshot_key_prefix = f"session:{session_id}:live_snap"

    for br_id in active_br_ids:
        if not br_id or br_id == "None":
            continue

        update: dict[str, Any] = {
            "parent_match_id": br_id,
            "home_team":       "dummy",  # UI uses parent_match_id for matching
            "bookmakers":      {},
            "markets_by_bk":   {},
        }

        if br_id in bt_map:
            b = unify_match_payload(bt_map[br_id], 0, "live", "bt", "BETIKA")
            new_mkts = b["markets_by_bk"]["bt"]

            # ── Diff: only publish markets that have changed since last snapshot
            snap_key  = f"{snapshot_key_prefix}:{br_id}:bt"
            old_snap  = _r().get(snap_key)
            old_mkts  = json.loads(old_snap) if old_snap else {}

            changed = _diff_markets(old_mkts, new_mkts)
            if changed:
                update["bookmakers"]["bt"]    = b["bookmakers"]["bt"]
                update["markets_by_bk"]["bt"] = changed
                _r().setex(snap_key, LIVE_REFRESH_INTERVAL * 3, json.dumps(new_mkts))

        if br_id in od_map:
            o = unify_match_payload(od_map[br_id], 0, "live", "od", "ODIBETS")
            new_mkts = o["markets_by_bk"]["od"]

            snap_key = f"{snapshot_key_prefix}:{br_id}:od"
            old_snap = _r().get(snap_key)
            old_mkts = json.loads(old_snap) if old_snap else {}

            changed = _diff_markets(old_mkts, new_mkts)
            if changed:
                update["bookmakers"]["od"]    = o["bookmakers"]["od"]
                update["markets_by_bk"]["od"] = changed
                _r().setex(snap_key, LIVE_REFRESH_INTERVAL * 3, json.dumps(new_mkts))

        if update["bookmakers"]:
            _pub(session_id, "live_update", update)

    # ── Re-schedule for the next cycle if session is still alive ─────────────
    if iteration < LIVE_MAX_ITERATIONS and _session_alive(session_id):
        refresh_live_bk_markets.apply_async(
            args=[sport_slug, session_id, active_br_ids, iteration + 1],
            countdown=LIVE_REFRESH_INTERVAL,
        )
    else:
        logger.info("live refresh: session %s ended after %d iterations", session_id, iteration)


# ──────────────────────────────────────────────────────────────────────────────
# DIFF HELPER — only send odds that actually changed
# ──────────────────────────────────────────────────────────────────────────────

def _diff_markets(
    old: dict[str, dict[str, dict]],
    new: dict[str, dict[str, dict]],
    threshold: float = 0.001,
) -> dict[str, dict[str, dict]]:
    """
    Return only the markets/outcomes from `new` whose price differs from `old`
    by more than `threshold`.  Returns an empty dict if nothing changed.
    """
    changed: dict = {}
    for mkt, outcomes in new.items():
        old_mkt  = old.get(mkt, {})
        mkt_diff = {}
        for out_key, price_info in outcomes.items():
            new_price = price_info.get("price", 0) if isinstance(price_info, dict) else float(price_info)
            old_entry = old_mkt.get(out_key, {})
            old_price = old_entry.get("price", 0) if isinstance(old_entry, dict) else float(old_entry)
            if abs(new_price - old_price) > threshold:
                mkt_diff[out_key] = price_info
        # Also include new outcomes that didn't exist before
        for out_key in set(new_mkt for new_mkt in outcomes if new_mkt not in old_mkt):
            mkt_diff[out_key] = outcomes[out_key]
        if mkt_diff:
            changed[mkt] = mkt_diff
    # Include new markets entirely absent from old
    for mkt in set(new) - set(old):
        changed[mkt] = new[mkt]
    return changed