"""
app/workers/redis_bus.py
=========================
Redis pub/sub bus — unified event backbone for all 10 bookmakers.

Channel topology
────────────────
  PER-BK UPCOMING:
    odds:{bk}:upcoming:{sport}              snapshot key (setex)
    odds:{bk}:upcoming:{sport}:updates      pub/sub channel
    odds:{bk}:upcoming:{sport}:page:{n}     temp page key (TTL 600s)
    odds:{bk}:{mode}:{sport}:pages_done     atomic counter

  UNIFIED:
    odds:unified:upcoming:{sport}           merged cross-BK snapshot
    odds:all:upcoming:{sport}:updates       pub/sub: all BKs merged
    odds:all:live:{sport}:updates           pub/sub: live updates

  B2B SPECIFIC:
    odds:b2b:{bk_slug}:upcoming:{sport}     per-B2B-BK snapshot
    odds:b2b:upcoming:{sport}:bks_done      how many B2B BKs done

  LIVE:
    sp:live:snapshot:{sport_id}             SP live snapshot
    sp:live:state:{event_id}                SP event live state
    live:{sport}:all                        live cross-BK pub/sub

  OPERATIONAL:
    odds:updates                            master WS channel (all events)
    arb:updates:{sport}                     arb opportunities
    ev:updates:{sport}                      EV opportunities
    monitor:job_log                         harvest job log (list, last 500)
    monitor:report                          health report (hash)
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
    return f"odds:{bk}:{mode}:{sport}"


def ch_updates(bk: str, mode: str, sport: str) -> str:
    return f"odds:{bk}:{mode}:{sport}:updates"


def ch_merged(mode: str, sport: str) -> str:
    return f"odds:all:{mode}:{sport}:updates"


def ch_unified(mode: str, sport: str) -> str:
    return f"odds:unified:{mode}:{sport}"


def ch_page(bk: str, mode: str, sport: str, page: int) -> str:
    return f"odds:{bk}:{mode}:{sport}:page:{page}"


def ch_pages_done(bk: str, mode: str, sport: str) -> str:
    return f"odds:{bk}:{mode}:{sport}:pages_done"


# All bookmaker slugs (for reading unified snapshot)
ALL_BK_SLUGS = ["sp", "bt", "od", "b2b", "1xbet", "22bet", "betwinner",
                 "melbet", "megapari", "helabet", "paripesa"]


# ─── Publisher helpers ────────────────────────────────────────────────────────

def publish_page(
    bk: str, mode: str, sport: str, page: int,
    matches: list, total_pages: int, ttl: int = 600,
) -> int:
    """Persist one page and notify subscribers. Returns pages done so far."""
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
    Persist merged BK snapshot and broadcast on both bk-specific
    and merged channel. Also updates the unified cross-BK snapshot.
    """
    r = _r()
    snapshot = {
        "bk":           bk,
        "mode":         mode,
        "sport":        sport,
        "match_count":  len(matches),
        "harvested_at": _now(),
        "matches":      matches,
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

    # Reset page counter for next cycle
    r.delete(ch_pages_done(bk, mode, sport))

    # Rebuild unified snapshot asynchronously
    _rebuild_unified_snapshot(r, mode, sport, ttl)


def _rebuild_unified_snapshot(r, mode: str, sport: str, ttl: int = 3600) -> None:
    """
    Read all per-BK snapshots and write a merged unified snapshot.
    Called after every BK snapshot update.
    """
    from app.workers.fuzzy_matcher import (
        match_dict_to_candidate, bulk_align, MatchCandidate,
    )

    all_matches: list[dict] = []
    seen_ids: set = set()

    # Collect from all standard BKs
    standard_bks = ["sp", "bt", "od"]
    for bk in standard_bks:
        raw = r.get(ch_data(bk, mode, sport))
        if not raw:
            continue
        try:
            data = json.loads(raw)
            bk_matches = data.get("matches") or (data if isinstance(data, list) else [])
            for m in bk_matches:
                _add_to_unified(all_matches, seen_ids, m, bk)
        except Exception:
            pass

    # Collect from B2B unified snapshot
    b2b_raw = r.get(ch_data("b2b", mode, sport))
    if b2b_raw:
        try:
            b2b_data = json.loads(b2b_raw)
            b2b_matches = b2b_data.get("matches") or []
            for m in b2b_matches:
                _add_to_unified(all_matches, seen_ids, m, "b2b")
        except Exception:
            pass

    if not all_matches:
        return

    unified_snap = {
        "mode":         mode,
        "sport":        sport,
        "match_count":  len(all_matches),
        "updated_at":   _now(),
        "matches":      all_matches,
    }
    r.setex(ch_unified(mode, sport), ttl, json.dumps(unified_snap, default=str))

    # Also publish to the main WS channel
    r.publish("odds:updates", json.dumps({
        "event":  "unified_ready",
        "mode":   mode,
        "sport":  sport,
        "count":  len(all_matches),
        "ts":     _now(),
    }))


def _add_to_unified(
    unified: list[dict],
    seen_ids: set,
    match: dict,
    bk: str,
) -> None:
    """
    Merge a match into the unified list.
    Deduplicates by betradar_id, then by home+away+time fuzzy key.
    """
    br = match.get("betradar_id")
    ext_id = (
        match.get("sp_game_id") or match.get("bt_match_id") or
        match.get("od_match_id") or match.get("b2b_match_id") or
        match.get("external_id")
    )

    dedup_key = None
    if br:
        dedup_key = f"br:{br}"
    elif ext_id:
        dedup_key = f"{bk}:{ext_id}"
    else:
        home  = (match.get("home_team") or "").lower().strip()[:12]
        away  = (match.get("away_team") or "").lower().strip()[:12]
        start = (match.get("start_time") or "")[:13]
        dedup_key = f"name:{home}|{away}|{start}"

    if dedup_key and dedup_key in seen_ids:
        # Match exists — merge bookmaker odds
        for existing in unified:
            ek = None
            if existing.get("betradar_id") and br:
                if existing["betradar_id"] == br:
                    ek = dedup_key
            if not ek:
                existing_home  = (existing.get("home_team") or "").lower().strip()[:12]
                incoming_home  = (match.get("home_team") or "").lower().strip()[:12]
                if existing_home == incoming_home:
                    ek = dedup_key
            if ek == dedup_key:
                # Merge bookmakers
                existing_bks = existing.get("bookmakers") or {}
                incoming_bks = match.get("bookmakers") or {}
                for bk_slug, bk_data in incoming_bks.items():
                    if bk_slug not in existing_bks:
                        existing_bks[bk_slug] = bk_data
                    else:
                        # Merge markets
                        existing_mkts = existing_bks[bk_slug].get("markets") or {}
                        incoming_mkts = bk_data.get("markets") or {}
                        for mkt, outs in incoming_mkts.items():
                            if mkt not in existing_mkts:
                                existing_mkts[mkt] = outs
                existing["bookmakers"] = existing_bks
                break
        return

    if dedup_key:
        seen_ids.add(dedup_key)

    # Add bk info to top-level bookmakers dict if missing
    if "bookmakers" not in match or not match["bookmakers"]:
        markets = match.get("markets") or {}
        match["bookmakers"] = {
            bk: {"match_id": ext_id or "", "markets": markets}
        }
    unified.append(dict(match))


def publish_live_update(sport: str, betradar_id: str, payload: dict) -> None:
    """Publish a live odds delta on both sport and global live channels."""
    r = _r()
    msg = json.dumps({"ts": _now(), **payload}, default=str)
    r.publish(ch_merged("live", sport), msg)
    r.publish("odds:all:live:updates", msg)


def publish_b2b_live_update(
    sport: str,
    matches: list[dict],
    r=None,
    bk_slug: str = "b2b",
) -> None:
    """Publish B2B live match updates."""
    if r is None:
        r = _r()
    msg = json.dumps({
        "event":   "live_update",
        "bk":      bk_slug,
        "sport":   sport,
        "matches": matches,
        "count":   len(matches),
        "ts":      _now(),
    }, default=str)
    r.publish(ch_merged("live", sport), msg)
    r.publish("odds:all:live:updates", msg)
    r.publish("odds:updates", msg)


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
                    m.get("b2b_match_id") or
                    f"{m.get('home_team','')}|{m.get('away_team','')}"
                )
                if mid not in seen_ids:
                    seen_ids.add(mid)
                    all_matches.append(m)
        except Exception:
            pass
    return all_matches


def pages_done_count(bk: str, mode: str, sport: str) -> int:
    val = _r().get(ch_pages_done(bk, mode, sport))
    return int(val) if val else 0


def get_snapshot(bk: str, mode: str, sport: str) -> dict | None:
    raw = _r().get(ch_data(bk, mode, sport))
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return None


def get_unified_snapshot(mode: str, sport: str) -> dict | None:
    """Return the latest unified cross-BK snapshot."""
    raw = _r().get(ch_unified(mode, sport))
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            pass
    return None


def get_all_snapshots(mode: str, sport: str, bks: list[str] | None = None) -> dict[str, list]:
    if bks is None:
        bks = ["sp", "bt", "od", "b2b"]
    result: dict[str, list] = {}
    for bk in bks:
        snap = get_snapshot(bk, mode, sport)
        if snap and snap.get("matches"):
            result[bk] = snap["matches"]
    return result


def get_matches_for_api(mode: str, sport: str) -> list[dict]:
    """
    Primary read path for API endpoints.
    Tries unified snapshot first, then aggregates per-BK snapshots.
    """
    # Try unified first (fastest)
    unified = get_unified_snapshot(mode, sport)
    if unified and unified.get("matches"):
        return unified["matches"]

    # Fallback: aggregate on the fly
    r = _r()
    all_matches: list[dict] = []
    seen_ids: set = set()

    for bk in ["sp", "bt", "od", "b2b"]:
        raw = r.get(ch_data(bk, mode, sport))
        if not raw:
            continue
        try:
            data = json.loads(raw)
            bk_matches = data.get("matches") or (data if isinstance(data, list) else [])
            for m in bk_matches:
                _add_to_unified(all_matches, seen_ids, m, bk)
        except Exception:
            pass

    return all_matches


# ─── Subscriber helpers ───────────────────────────────────────────────────────

def subscribe(bk: str, mode: str, sport: str):
    r = _r()
    ps = r.pubsub(ignore_subscribe_messages=True)
    ps.subscribe(ch_updates(bk, mode, sport))
    return ps


def subscribe_merged(mode: str, sport: str):
    r = _r()
    ps = r.pubsub(ignore_subscribe_messages=True)
    ps.subscribe(ch_merged(mode, sport))
    return ps


def subscribe_all_live(sport: str):
    """Subscribe to all live update channels for a sport."""
    r = _r()
    ps = r.pubsub(ignore_subscribe_messages=True)
    ps.subscribe(
        ch_merged("live", sport),
        "odds:all:live:updates",
        f"sp:live:sport:{sport}",
    )
    return ps


def subscribe_multiple(mode: str, sports: list[str]):
    r = _r()
    ps = r.pubsub(ignore_subscribe_messages=True)
    channels = [ch_merged(mode, s) for s in sports]
    ps.subscribe(*channels)
    return ps