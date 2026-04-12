"""
routes_debug.py — Unified SSE Stream (Celery Edition)
=======================================================

ARCHITECTURE
─────────────────────────────────────────────────────────────────────────────

UPCOMING
  1. Check Redis cache  →  serve all matches instantly + send "done"
  2. Cache miss:
       a. Fetch SP matches (fast, no per-match HTTP)  →  stream as "batch"
          immediately so the UI paints within ~1 s.
       b. For every match with a betradar_id, dispatch ONE Celery task:
              enrich_upcoming_match.delay(brid, sport_slug, session_id, ...)
          These run in the worker pool — fully parallel, don't block the SSE.
       c. Subscribe to Redis channel  session:{session_id}:market_updates
          Forward every message as its named SSE event.
       d. Celery tasks publish "live_update" per match as they finish.
          When the last task finishes it publishes "done", which closes the
          stream and triggers a full-match Redis cache write.

LIVE
  1. Fetch initial snapshot (SP + BT + OD, parallel, 3 threads)  →  stream.
  2. Dispatch  refresh_live_bk_markets.delay(...)  — a self-rescheduling
     Celery task that polls BT/OD every 30 s and publishes diffs.
  3. Subscribe to BOTH:
       • sp:live:sport:{sport_id}  — SP WebSocket harvester publishes here
       • session:{session_id}:market_updates  — Celery BT/OD diffs land here
  4. Forward all messages to the SSE client.
  5. Refresh the "session alive" key on every keepalive tick so Celery knows
     the client is still connected.

SESSION ID
  Generated server-side (uuid4) and sent in the "meta" SSE event.
  The frontend doesn't need it — it just receives the enriched events.
"""

from __future__ import annotations

import json
import os
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor

import redis
from flask import Response, request, stream_with_context

from .blueprint import bp_odds_customer
from . import config
from .utils import _now_utc, _normalise_sport_slug, _sse, _keepalive
from app.utils.decorators_ import log_event
from app.workers.market_normalizer import unify_match_payload, merge_best
from app.workers.market_tasks import (
    enrich_upcoming_match,
    refresh_live_bk_markets,
    LIVE_REFRESH_INTERVAL,
)

# How long the SSE stream waits for Celery enrichments (seconds)
UPCOMING_ENRICH_TIMEOUT = 90
# Session alive TTL (seconds) — refreshed on every keepalive
SESSION_ALIVE_TTL = 90


def _get_redis() -> redis.Redis:
    return redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
    )


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@bp_odds_customer.route("/odds/debug/unified/stream/<mode>/<sport_slug>")
def debug_stream_unified(mode: str, sport_slug: str):
    full_arg_raw = request.args.get("full")
    fetch_full   = str(full_arg_raw or "true").lower() in ("1", "true", "yes")
    max_m        = int(request.args.get("max", 50))
    # Allow the client to resume with an existing session (e.g., on reconnect)
    session_id   = request.args.get("session_id") or str(uuid.uuid4())

    log_event("debug_unified_stream", {
        "sport": sport_slug, "mode": mode, "session": session_id,
    })

    def _gen():
        r = _get_redis()

        # Mark session alive immediately
        r.setex(f"session:{session_id}:alive", SESSION_ALIVE_TTL, "1")

        yield _sse("meta", {
            "source":     "unified_direct",
            "sport":      sport_slug,
            "mode":       mode,
            "session_id": session_id,
            "now":        _now_utc().isoformat(),
        })

        try:
            if mode == "live":
                yield from _stream_live(r, sport_slug, session_id)
            else:
                yield from _stream_upcoming(r, sport_slug, session_id, max_m, fetch_full)
        except Exception as exc:
            tb_str = traceback.format_exc()
            print("=== STREAM CRASH ===\n", tb_str, "\n====================")
            yield _sse("error", {"error": str(exc), "traceback": tb_str})

    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING  —  base stream + Celery background enrichment
# ══════════════════════════════════════════════════════════════════════════════

def _stream_upcoming(
    r:          redis.Redis,
    sport_slug: str,
    session_id: str,
    max_m:      int,
    fetch_full: bool,
):
    cache_key   = f"unified:upcoming:{sport_slug}:{max_m}:{fetch_full}"
    cached_data = r.get(cache_key)

    # ── 1. CACHE HIT — serve all matches in chunks, no Celery needed ─────────
    if cached_data:
        matches = json.loads(cached_data)
        for i in range(0, len(matches), 15):
            chunk = matches[i: i + 15]
            yield _sse("batch", {
                "matches": chunk,
                "batch":   min(i + 15, len(matches)),
                "of":      len(matches),
                "offset":  i,
            })
        yield _sse("list_done", {"total_sent": len(matches), "source": "cache"})
        yield _sse("done",      {"status": "finished", "total_sent": len(matches)})
        return

    # ── 2. CACHE MISS — fetch SP base and dispatch Celery tasks ──────────────
    from app.workers.sp_harvester import fetch_upcoming_stream

    sp_matches_raw = list(
        fetch_upcoming_stream(sport_slug, max_matches=max_m, fetch_full_markets=fetch_full)
    )

    count           = 0
    # Track all completed matches so we can cache them after enrichment
    completed: dict[str, dict] = {}
    brids_to_enrich: list[str] = []

    # Stream SP base matches immediately (UI paints right away)
    for sp_match in sp_matches_raw:
        if not isinstance(sp_match, dict):
            continue
        count += 1
        sp_clean  = unify_match_payload(sp_match, count, "upcoming", "sp", "SPORTPESA")
        sp_clean["market_slugs"] = list(sp_clean["best"].keys())
        sp_clean["market_count"] = len(sp_clean["best"])

        brid = str(sp_clean.get("parent_match_id") or sp_clean.get("match_id") or "")
        if brid and brid != "None":
            completed[brid]       = sp_clean
            brids_to_enrich.append(brid)

        yield _sse("batch", {
            "matches": [sp_clean],
            "batch":   count,
            "of":      "unknown",
            "offset":  count - 1,
        })

    yield _sse("list_done", {"total_sent": count})

    if not brids_to_enrich:
        yield _sse("done", {"status": "finished", "total_sent": count})
        return

    # ── 3. Dispatch one Celery task per match ────────────────────────────────
    #    Set the pending counter BEFORE dispatching so there's no race where
    #    the last task decrements before the counter is written.
    r.setex(f"session:{session_id}:pending", UPCOMING_ENRICH_TIMEOUT + 30, str(len(brids_to_enrich)))

    for brid in brids_to_enrich:
        m = completed[brid]
        enrich_upcoming_match.delay(
            brid,
            sport_slug,
            session_id,
            m.get("home_team", ""),
            m.get("away_team", ""),
        )

    # ── 4. Subscribe to session channel and forward enrichments ──────────────
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(f"session:{session_id}:market_updates")

    deadline = time.time() + UPCOMING_ENRICH_TIMEOUT

    while time.time() < deadline:
        # Keep session alive so Celery tasks don't bail
        r.expire(f"session:{session_id}:alive", SESSION_ALIVE_TTL)

        msg = pubsub.get_message(timeout=0.4)
        if msg and msg.get("type") == "message":
            try:
                envelope = json.loads(msg["data"])
                event    = envelope.get("event", "live_update")
                data     = envelope.get("data", {})

                yield _sse(event, data)

                # Merge enrichment into our local completed map for caching
                if event == "live_update":
                    brid = str(data.get("parent_match_id") or "")
                    if brid in completed:
                        _apply_enrichment(completed[brid], data)

                if event == "done":
                    # All tasks finished — write cache and exit
                    _write_cache(r, cache_key, list(completed.values()))
                    return

            except Exception:
                pass

        yield _keepalive()

    # Timeout reached — cache whatever we have and close
    yield _sse("done", {"status": "timeout", "total_sent": count})
    _write_cache(r, cache_key, list(completed.values()))


def _apply_enrichment(match: dict, update: dict) -> None:
    """Merge a Celery enrichment payload into the local match dict."""
    bk_in   = update.get("bookmakers",   {})
    mbk_in  = update.get("markets_by_bk", {})

    if not match.get("bookmakers"):   match["bookmakers"]   = {}
    if not match.get("markets_by_bk"): match["markets_by_bk"] = {}

    for bk, bk_data in bk_in.items():
        match["bookmakers"][bk] = bk_data

    for bk, mkts in mbk_in.items():
        if bk not in match["markets_by_bk"]:
            match["markets_by_bk"][bk] = {}
        for mkt, outs in mkts.items():
            match["markets_by_bk"][bk][mkt] = {
                **(match["markets_by_bk"][bk].get(mkt) or {}),
                **outs,
            }

    # Rebuild best odds
    if not match.get("best"):
        match["best"] = {}
    merge_best(match["best"], match["markets_by_bk"])
    match["market_slugs"] = list(match["best"].keys())
    match["market_count"] = len(match["best"])
    match["bk_count"]     = len(match.get("bookmakers", {}))


def _write_cache(r: redis.Redis, key: str, matches: list[dict]) -> None:
    """Cache fully enriched match list for 5 minutes."""
    if matches:
        try:
            r.setex(key, 300, json.dumps(matches))
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# LIVE  —  initial snapshot + SP pub/sub + Celery BT/OD refresh
# ══════════════════════════════════════════════════════════════════════════════

def _stream_live(
    r:          redis.Redis,
    sport_slug: str,
    session_id: str,
):
    from app.workers.sp_live_harvester import fetch_live_stream, SPORT_SLUG_MAP
    from app.workers.bt_harvester      import fetch_live_matches as bt_fetch_live, slug_to_bt_sport_id
    from app.workers.od_harvester      import fetch_live_matches as od_fetch_live

    sport_id = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)

    # ── 1. Parallel initial fetch (SP + BT + OD) ─────────────────────────────
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_bt = pool.submit(bt_fetch_live, slug_to_bt_sport_id(sport_slug))
        f_od = pool.submit(od_fetch_live, sport_slug)
        f_sp = pool.submit(lambda: list(fetch_live_stream(sport_slug, fetch_full_markets=True)))

        bt_data = f_bt.result() or []
        od_data = f_od.result() or []
        stream  = f_sp.result() or []

    bt_map = {str(m.get("betradar_id") or m.get("bt_parent_id")): m
              for m in bt_data if isinstance(m, dict)}
    od_map = {str(m.get("betradar_id") or m.get("od_parent_id")): m
              for m in od_data if isinstance(m, dict)}

    sp_event_map: dict[str, str] = {}   # betradar_id → sp event id
    active_br_ids: list[str]     = []
    seen_br_ids:   set[str]      = set()
    count = 0

    # Stream SP matches (merged with inline BT/OD if available)
    for sp_match in stream:
        if not isinstance(sp_match, dict):
            continue
        count += 1
        betradar_id = str(sp_match.get("betradar_id") or "")
        if betradar_id and betradar_id not in ("0", "None"):
            active_br_ids.append(betradar_id)
            seen_br_ids.add(betradar_id)
            sp_event_map[betradar_id] = str(
                sp_match.get("sp_match_id") or sp_match.get("match_id") or ""
            )

        sp_clean = unify_match_payload(sp_match, count, "live", "sp", "SPORTPESA")
        sp_clean["is_live"] = True

        bt_match = bt_map.get(betradar_id)
        if bt_match and bt_match.get("markets"):
            b = unify_match_payload(bt_match, count, "live", "bt", "BETIKA")
            sp_clean["bookmakers"]["bt"]    = b["bookmakers"]["bt"]
            sp_clean["markets_by_bk"]["bt"] = b["markets_by_bk"]["bt"]

        od_match = od_map.get(betradar_id)
        if od_match and od_match.get("markets"):
            o = unify_match_payload(od_match, count, "live", "od", "ODIBETS")
            sp_clean["bookmakers"]["od"]    = o["bookmakers"]["od"]
            sp_clean["markets_by_bk"]["od"] = o["markets_by_bk"]["od"]

        sp_clean["bk_count"] = len(sp_clean["bookmakers"])
        merge_best(sp_clean["best"], sp_clean["markets_by_bk"])
        sp_clean["market_slugs"] = list(sp_clean["best"].keys())
        sp_clean["market_count"] = len(sp_clean["best"])

        yield _sse("batch", {
            "matches": [sp_clean], "batch": count,
            "of": "unknown", "offset": count - 1,
        })
        yield _keepalive()

    # Stream BT-only matches (no SP counterpart)
    for br_id, bt_match in bt_map.items():
        if br_id not in seen_br_ids and br_id and br_id != "None":
            count += 1
            active_br_ids.append(br_id)
            bt_clean = unify_match_payload(bt_match, count, "live", "bt", "BETIKA")
            bt_clean["is_live"] = True

            od_match = od_map.get(br_id)
            if od_match and od_match.get("markets"):
                o = unify_match_payload(od_match, count, "live", "od", "ODIBETS")
                bt_clean["bookmakers"]["od"]    = o["bookmakers"]["od"]
                bt_clean["markets_by_bk"]["od"] = o["markets_by_bk"]["od"]
                seen_br_ids.add(br_id)

            bt_clean["bk_count"] = len(bt_clean["bookmakers"])
            merge_best(bt_clean["best"], bt_clean["markets_by_bk"])
            bt_clean["market_slugs"] = list(bt_clean["best"].keys())
            bt_clean["market_count"] = len(bt_clean["best"])
            yield _sse("batch", {
                "matches": [bt_clean], "batch": count,
                "of": "unknown", "offset": count - 1,
            })
            yield _keepalive()
            seen_br_ids.add(br_id)

    # Stream OD-only matches
    for br_id, od_match in od_map.items():
        if br_id not in seen_br_ids and br_id and br_id != "None":
            count += 1
            active_br_ids.append(br_id)
            od_clean = unify_match_payload(od_match, count, "live", "od", "ODIBETS")
            od_clean["is_live"] = True
            merge_best(od_clean["best"], od_clean["markets_by_bk"])
            od_clean["market_slugs"] = list(od_clean["best"].keys())
            od_clean["market_count"] = len(od_clean["best"])
            yield _sse("batch", {
                "matches": [od_clean], "batch": count,
                "of": "unknown", "offset": count - 1,
            })
            yield _keepalive()
            seen_br_ids.add(br_id)

    yield _sse("list_done", {"total_sent": count})

    # ── 2. Dispatch Celery live refresh task ──────────────────────────────────
    if active_br_ids:
        refresh_live_bk_markets.delay(sport_slug, session_id, active_br_ids, 0)

    # ── 3. Subscribe to SP pub/sub AND session channel ────────────────────────
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(
        f"sp:live:sport:{sport_id}",
        f"session:{session_id}:market_updates",
    )

    while True:
        # Keep session alive so the Celery refresh task doesn't stop
        r.expire(f"session:{session_id}:alive", SESSION_ALIVE_TTL)

        msg = pubsub.get_message(timeout=0.2)
        if msg and msg.get("type") == "message":
            channel = msg.get("channel", "")
            raw     = msg["data"]

            try:
                payload = json.loads(raw)

                # ── Message from SP WebSocket harvester ───────────────────────
                if channel == f"sp:live:sport:{sport_id}":
                    msg_type = payload.get("type")
                    ev_id    = payload.get("event_id")
                    br_id    = next(
                        (k for k, v in sp_event_map.items() if v == ev_id), None
                    ) or str(ev_id)

                    if msg_type == "event_update":
                        state = payload.get("state", {})
                        yield _sse("live_update", {
                            "parent_match_id": br_id,
                            "home_team":       "dummy",
                            "match_time":      state.get("matchTime", payload.get("match_time", "")),
                            "score_home":      payload.get("score_home"),
                            "score_away":      payload.get("score_away"),
                            "status":          payload.get("phase", state.get("currentEventPhase", "")),
                            "is_live":         True,
                        })
                    elif msg_type == "market_update":
                        norm_sels = payload.get("normalised_selections", [])
                        slug      = payload.get("market_slug")
                        if norm_sels and slug:
                            outs = {
                                s["outcome_key"]: {"price": float(s["odds"])}
                                for s in norm_sels if float(s.get("odds", 0)) > 1
                            }
                            if outs:
                                yield _sse("live_update", {
                                    "parent_match_id": br_id,
                                    "home_team":       "dummy",
                                    "bookmakers":      {"sp": {"slug": "sp", "markets": {slug: outs}}},
                                    "markets_by_bk":   {"sp": {slug: outs}},
                                })

                # ── Message from Celery BT/OD refresh task ────────────────────
                elif channel == f"session:{session_id}:market_updates":
                    envelope = payload
                    event    = envelope.get("event", "live_update")
                    data     = envelope.get("data", {})
                    yield _sse(event, data)

            except Exception:
                pass

        yield _keepalive()