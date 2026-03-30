# """
# app/workers/tasks_live.py
# ==========================
# All LIVE harvest tasks.

# Beat schedule entries (in tasks_ops.py setup_periodic_tasks):
#   sender.add_periodic_task(60.0,   sp_harvest_all_live.s(),          name="sp-live-60s")
#   sender.add_periodic_task(5.0,    sp_poll_all_event_details.s(),    name="sp-details-5s")
#   sender.add_periodic_task(90.0,   bt_harvest_all_live.s(),          name="bt-live-90s")
#   sender.add_periodic_task(90.0,   od_harvest_all_live.s(),          name="od-live-90s")
#   sender.add_periodic_task(120.0,  b2b_harvest_all_live.s(),         name="b2b-live-2min")
#   sender.add_periodic_task(30.0,   b2b_page_harvest_all_live.s(),    name="b2b-page-live-30s")
#   sender.add_periodic_task(60.0,   sbo_harvest_all_live.s(),         name="sbo-live-60s")

# KEY ADDITION: sp_poll_all_event_details runs every 5 s.
#   For every tracked live event it calls:
#     GET /api/live/events/{id}/details
#   Diffs the returned markets against Redis-cached previous odds.
#   Publishes ONLY changed selections to:
#     sp:live:event:{id}
#     sp:live:sport:{sport_id}
#     sp:live:all
#   Format mirrors the WS BUFFERED_MARKET_UPDATE shape so the same
#   processSseMessage() handler in the frontend handles both.

# NOTE: Sportpesa's TRUE real-time feed lives in sp_live_harvester.py
# (WebSocket thread + 1-second Redis republisher). The details-polling
# tasks here are a reliable fallback that works even when the WS thread
# is disconnected, and they additionally cover markets not sent over WS.
# """

# from __future__ import annotations

# import json
# import time
# from datetime import datetime, timezone
# from typing import Any

# import requests
# from celery import group
# from celery.utils.log import get_task_logger

# from app.workers.celery_tasks import (
#     celery, cache_set, cache_get, _now_iso, _upsert_and_chain, _publish,
# )

# logger = get_task_logger(__name__)

# WS_CHANNEL = "odds:updates"

# _LIVE_SPORTS     = ["soccer", "basketball", "tennis"]
# _B2B_LIVE_SPORTS = ["Football", "Basketball", "Ice Hockey", "Tennis"]
# _SBO_LIVE_SPORTS = ["soccer", "basketball", "tennis"]

# # SP API base — same as sp_live_harvester
# _SP_BASE = "https://www.ke.sportpesa.com"
# _HEADERS = {
#     "User-Agent": (
#         "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
#         "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
#     ),
#     "Accept":           "application/json, text/plain, */*",
#     "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
#     "X-Requested-With": "XMLHttpRequest",
#     "X-App-Timezone":   "Africa/Nairobi",
#     "Origin":           _SP_BASE,
#     "Referer":          _SP_BASE + "/en/live/",
# }

# # Outcome positional fallback (mirrors sp_live_harvester._POS_OUTCOME)
# _POS_OUTCOME: dict[int, list[str]] = {
#     194: ["1", "X", "2"],
#     149: ["1", "2"],
#     147: ["1X", "X2", "12"],
#     138: ["yes", "no"],
#     140: ["yes", "no"],
#     145: ["odd", "even"],
#     166: ["1", "2"],
#     135: ["1", "X", "2"],
#     155: ["1st", "2nd", "equal"],
#     129: ["none", "1", "2"],
#     151: ["1", "X", "2"],
# }

# _DIRECT_KEYS: dict[str, str] = {
#     "1": "1", "x": "X", "2": "2", "draw": "X",
#     "over": "over", "under": "under",
#     "yes": "yes", "no": "no",
#     "odd": "odd", "even": "even",
#     "1x": "1X", "x2": "X2", "12": "12",
#     "home": "1", "away": "2",
#     "ov": "over", "un": "under",
#     "gg": "yes", "ng": "no",
#     "eql": "equal", "none": "none",
#     "1st": "1st", "2nd": "2nd",
#     "p1": "1", "p2": "2",
# }

# _SPORT_TOTAL_SLUG: dict[int, str] = {
#     1: "over_under_goals", 5: "over_under_goals", 126: "over_under_goals",
#     2: "total_points",     8: "total_points",
#     4: "total_games",     13: "total_games",
#     10: "total_sets",      9: "total_runs",
#     23: "total_sets",      6: "over_under_goals",
#     16: "total_games",    21: "total_runs",
# }

# _TYPE_TO_SLUG: dict[int, str] = {
#     194: "1x2",            149: "match_winner",     147: "double_chance",
#     138: "btts",           140: "first_half_btts",   145: "odd_even",
#     166: "draw_no_bet",    151: "european_handicap",  184: "asian_handicap",
#     183: "correct_score",  154: "exact_goals",        135: "first_half_1x2",
#     129: "first_team_to_score", 155: "highest_scoring_half",
#     106: "first_half_over_under",
# }


# def _emit(source: str, sport: str, count: int, latency: int) -> None:
#     _publish(WS_CHANNEL, {
#         "event": "odds_updated", "source": source, "sport": sport,
#         "mode": "live", "count": count, "latency_ms": latency,
#         "ts": _now_iso(),
#     })


# # =============================================================================
# # Helpers: SP HTTP, outcome normalisation, market slug
# # =============================================================================

# def _sp_get(path: str, params: dict | None = None, timeout: int = 12) -> Any:
#     url = f"{_SP_BASE}{path}"
#     try:
#         r = requests.get(url, headers=_HEADERS, params=params,
#                          timeout=timeout, allow_redirects=True)
#         if not r.ok:
#             return None
#         return r.json()
#     except Exception:
#         return None


# def _market_slug(market_type: int, sport_id: int, spec_val: str) -> str:
#     """Convert SP market type + specialValue → canonical slug."""
#     if market_type == 105:
#         base = _SPORT_TOTAL_SLUG.get(sport_id, "over_under")
#     else:
#         base = _TYPE_TO_SLUG.get(market_type, f"market_{market_type}")
#     if spec_val and spec_val not in ("0.00", "0", ""):
#         try:
#             fv   = float(spec_val)
#             line = str(int(fv)) if fv == int(fv) else str(fv)
#             return f"{base}_{line}"
#         except (ValueError, TypeError):
#             pass
#     return base


# def _normalise_outcome(name: str, idx: int, market_type: int) -> str:
#     """Convert SP selection name → canonical outcome key with positional fallback."""
#     nl = name.strip().lower()
#     # Direct lookup first
#     if nl in _DIRECT_KEYS:
#         return _DIRECT_KEYS[nl]
#     if nl.startswith("over"):
#         return "over"
#     if nl.startswith("under"):
#         return "under"
#     # Correct score / HT-FT: short token with colon or slash
#     stripped = name.strip()
#     if ":" in stripped and len(stripped) <= 5:
#         return stripped
#     if "/" in stripped and len(stripped) <= 5:
#         return stripped
#     # Double chance patterns like "Aruba or draw"
#     ll = nl
#     if " or draw" in ll or "or draw" in ll:
#         return "1X" if ll.startswith(("home", "aruba", "1", "local")) else "X2"
#     if " or " in ll:
#         parts = ll.split(" or ")
#         if len(parts) == 2:
#             if "draw" not in parts[0] and "draw" not in parts[1]:
#                 return "12"
#     # HT/FT combos like "Aruba/Aruba", "draw/Liechtenstein"
#     if "/" in name and 3 < len(name) < 30:
#         parts = name.split("/")
#         if len(parts) == 2:
#             def _side(s: str) -> str:
#                 sl = s.strip().lower()
#                 if sl in ("draw", "x"):
#                     return "X"
#                 if sl in ("1", "home"):
#                     return "1"
#                 if sl in ("2", "away"):
#                     return "2"
#                 # Team name — use positional fallback
#                 return "?"
#             h, a = _side(parts[0]), _side(parts[1])
#             if h != "?" and a != "?":
#                 return f"{h}/{a}"
#     # Positional fallback — handles team names like "Aruba", "Liechtenstein"
#     pos_map = _POS_OUTCOME.get(market_type)
#     if pos_map and idx < len(pos_map):
#         return pos_map[idx]
#     if market_type == 105:
#         return "over" if idx == 0 else "under"
#     # Last resort: sanitise
#     slug = nl.replace(" ", "_").replace("-", "_")
#     slug = "".join(c for c in slug if c.isalnum() or c == "_")
#     return slug[:14] or f"sel_{idx}"


# # =============================================================================
# # Redis helpers used by details poller
# # =============================================================================

# def _get_live_redis():
#     """Get Redis client on DB 1 (live data, same as sp_live_harvester)."""
#     try:
#         from app.workers.sp_live_harvester import _get_redis
#         return _get_redis()
#     except Exception:
#         import redis as _r
#         import os
#         url  = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
#         base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
#         return _r.Redis.from_url(f"{base}/1", decode_responses=True,
#                                  socket_timeout=5, retry_on_timeout=True)


# def _publish_live(r, sport_id: int, event_id: int, payload: dict) -> None:
#     """Publish to all three Redis live channels."""
#     raw = json.dumps(payload, ensure_ascii=False)
#     try:
#         pipe = r.pipeline()
#         pipe.publish("sp:live:all", raw)
#         pipe.publish(f"sp:live:sport:{sport_id}", raw)
#         pipe.publish(f"sp:live:event:{event_id}", raw)
#         pipe.execute()
#     except Exception as exc:
#         logger.debug("[details:publish] event=%d: %s", event_id, exc)


# # =============================================================================
# # Core: fetch details, diff, publish
# # =============================================================================

# def _fetch_and_publish_details(event_id: int, sport_id: int) -> dict:
#     """
#     Fetch /api/live/events/{id}/details, diff against Redis-cached odds,
#     publish ONLY changed selections to Redis pub/sub channels.

#     Returns {"published": N, "markets_seen": M}
#     """
#     r   = _get_live_redis()
#     raw = _sp_get(f"/api/live/events/{event_id}/details")
#     if not raw or not isinstance(raw, dict):
#         return {"published": 0, "markets_seen": 0}

#     # ── Update event state (score / phase / clock) ────────────────────────────
#     ev     = raw.get("event") or {}
#     state  = ev.get("state") or {}
#     score  = state.get("matchScore") or {}
#     phase  = state.get("currentEventPhase", "")
#     mtime  = state.get("matchTime", "")
#     paused = ev.get("isPaused", False)
#     sh     = str(score.get("home") or "0")
#     sa     = str(score.get("away") or "0")

#     state_payload = {
#         "type":          "event_update",
#         "event_id":      event_id,
#         "sport_id":      sport_id,
#         "status":        ev.get("status", "Started"),
#         "phase":         phase,
#         "match_time":    mtime,
#         "clock_running": not paused and bool(phase),
#         "score_home":    sh,
#         "score_away":    sa,
#         "is_paused":     paused,
#         "source":        "sp_details_poll",
#         "ts":            _now_iso(),
#     }

#     # Cache state and publish
#     r.set(f"sp:live:state:{event_id}",
#           json.dumps({k: v for k, v in state_payload.items() if k != "type"}),
#           ex=300)
#     _publish_live(r, sport_id, event_id, state_payload)

#     # ── Process markets ───────────────────────────────────────────────────────
#     markets     = raw.get("markets") or []
#     published   = 1   # 1 for the state payload
#     markets_seen = 0

#     for mkt in markets:
#         if not isinstance(mkt, dict):
#             continue

#         market_id   = mkt.get("id")
#         spec_val    = str(mkt.get("specialValue") or "0.00")
#         mkt_status  = mkt.get("status", "Open")
#         sels_raw    = mkt.get("selections") or []
#         if not market_id or not sels_raw:
#             continue

#         markets_seen += 1
#         slug = _market_slug(market_id, sport_id, spec_val)

#         # Build normalised selections + detect changes
#         norm_sels: list[dict] = []
#         changed: list[dict]   = []
#         pipe = r.pipeline()

#         for idx, sel in enumerate(sels_raw):
#             sel_id   = sel.get("id")
#             odds_str = str(sel.get("odds") or "0")
#             sel_stat = sel.get("status", "Open")
#             name     = sel.get("name") or sel.get("shortName") or ""

#             try:
#                 price = float(odds_str)
#             except (TypeError, ValueError):
#                 price = 0.0

#             out_key = _normalise_outcome(name, idx, market_id)

#             norm_sel = {
#                 "id":          sel_id,
#                 "name":        name,
#                 "outcome_key": out_key,
#                 "odds":        odds_str,
#                 "status":      sel_stat,
#             }
#             norm_sels.append(norm_sel)

#             if not sel_id:
#                 continue

#             # Diff against cached value
#             odds_key = f"sp:live:odds:{event_id}:{sel_id}"
#             prev_val = r.get(odds_key)

#             if sel_stat in ("Open", "open") and price > 1.0:
#                 if prev_val != odds_str:
#                     changed.append({
#                         "id":   sel_id,
#                         "odds": odds_str,
#                         "prev": prev_val,
#                     })
#                     pipe.set(odds_key, odds_str, ex=300)

#         pipe.execute()

#         # Only publish if something changed (or market is new)
#         if not changed and markets_seen > 1:
#             continue

#         market_payload = {
#             "type":                  "market_update",
#             "event_id":              event_id,
#             "sport_id":              sport_id,
#             "market_id":             mkt.get("eventMarketId") or market_id,
#             "market_type":           market_id,
#             "market_name":           mkt.get("name", slug),
#             "market_slug":           slug,
#             "handicap":              spec_val,
#             "market_status":         mkt_status,
#             "normalised_selections": norm_sels,
#             "all_selections":        sels_raw,
#             "changed_selections":    changed,
#             "source":                "sp_details_poll",
#             "ts":                    _now_iso(),
#         }
#         _publish_live(r, sport_id, event_id, market_payload)
#         published += 1

#     return {"published": published, "markets_seen": markets_seen}


# # =============================================================================
# # Celery task: poll ONE event's details
# # =============================================================================

# @celery.task(
#     name="tasks.sp.poll_event_details", bind=True,
#     max_retries=0,
#     soft_time_limit=15, time_limit=20,
#     acks_late=True,
#     queue="live",
# )
# def sp_poll_event_details(self, event_id: int, sport_id: int = 1) -> dict:
#     """
#     Poll /api/live/events/{id}/details for ONE event.
#     Publishes state + changed market odds to Redis pub/sub.
#     Called every 5 s by sp_poll_all_event_details fan-out.
#     """
#     t0 = time.perf_counter()
#     try:
#         result = _fetch_and_publish_details(event_id, sport_id)
#     except Exception as exc:
#         logger.debug("[details] event=%d error: %s", event_id, exc)
#         return {"ok": False, "error": str(exc)}
#     latency = int((time.perf_counter() - t0) * 1000)
#     return {
#         "ok":           True,
#         "event_id":     event_id,
#         "sport_id":     sport_id,
#         "latency_ms":   latency,
#         **result,
#     }


# # =============================================================================
# # Celery task: poll ALL active live events (fan-out, runs every 5 s)
# # =============================================================================

# @celery.task(
#     name="tasks.sp.poll_all_event_details",
#     soft_time_limit=30, time_limit=45,
#     queue="live",
# )
# def sp_poll_all_event_details() -> dict:
#     """
#     Fan-out: dispatch sp_poll_event_details for every tracked live event.
#     Runs every 5 seconds via beat schedule.

#     Event list comes from Redis (written by sp_live_harvester WS thread).
#     Falls back to a fresh HTTP fetch if Redis is empty.
#     """
#     r = _get_live_redis()

#     # Try to get sport → events mapping from Redis sports key
#     sports_raw = r.get("sp:live:sports")
#     sport_events: list[tuple[int, int]] = []   # [(event_id, sport_id), ...]

#     if sports_raw:
#         # Events are tracked in the WS harvester's sport_id_map
#         # We can read from existing snapshot keys
#         try:
#             sports = json.loads(sports_raw)
#             for sp in sports:
#                 sid = sp.get("id")
#                 if not sid:
#                     continue
#                 snap_raw = r.get(f"sp:live:snapshot:{sid}")
#                 if snap_raw:
#                     snap = json.loads(snap_raw)
#                     for ev in snap.get("events") or []:
#                         eid = ev.get("eventId") or (ev.get("event") or {}).get("id")
#                         if eid:
#                             sport_events.append((int(eid), int(sid)))
#         except Exception as exc:
#             logger.warning("[details:fanout] redis parse: %s", exc)

#     # Fallback: fetch live events via HTTP for soccer only
#     if not sport_events:
#         try:
#             raw = _sp_get("/api/live/sports/1/events", params={"limit": 100})
#             if raw:
#                 events = (raw if isinstance(raw, list)
#                           else raw.get("events") or raw.get("data") or [])
#                 for ev in events:
#                     eid = ev.get("id")
#                     if eid:
#                         sport_events.append((int(eid), 1))
#         except Exception as exc:
#             logger.warning("[details:fanout] HTTP fallback: %s", exc)

#     if not sport_events:
#         return {"dispatched": 0}

#     # Deduplicate
#     seen: set[int] = set()
#     unique_events: list[tuple[int, int]] = []
#     for eid, sid in sport_events:
#         if eid not in seen:
#             seen.add(eid)
#             unique_events.append((eid, sid))

#     # Dispatch per-event tasks
#     sigs = [sp_poll_event_details.s(eid, sid) for eid, sid in unique_events]
#     group(sigs).apply_async(queue="live")

#     logger.debug("[details:fanout] dispatched %d event polls", len(unique_events))
#     return {"dispatched": len(unique_events)}


# # =============================================================================
# # SSE streaming from Celery-published Redis data
# # (called by Flask view, not a Celery task)
# # =============================================================================

# def stream_event_details_sse(event_id: int, sport_id: int = 1):
#     """
#     Generator: yields SSE frames for one event.
#     Two sources combined in one stream:
#       1. Redis pub/sub sp:live:event:{id}  — instant WS/details-poll updates
#       2. Direct HTTP poll every 8 s        — guaranteed freshness even if Redis is quiet

#     This function is called by the Flask SSE endpoint (not Celery).
#     """
#     import time as _time

#     r      = _get_live_redis()
#     pubsub = r.pubsub(ignore_subscribe_messages=True)
#     pubsub.subscribe(f"sp:live:event:{event_id}")

#     def _sse(data: dict) -> str:
#         return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

#     def _ka() -> str:
#         return f": keep-alive {datetime.now(timezone.utc).strftime('%H:%M:%S')}\n\n"

#     # Send connected frame
#     yield _sse({
#         "type":     "connected",
#         "event_id": event_id,
#         "sport_id": sport_id,
#         "source":   "sp_details_sse",
#         "ts":       _now_iso(),
#     })

#     # Immediately send a snapshot so the client has data before first poll
#     try:
#         snap = _fetch_and_publish_details(event_id, sport_id)
#         yield _sse({"type": "snapshot_done", "event_id": event_id,
#                     "markets_seen": snap.get("markets_seen", 0), "ts": _now_iso()})
#     except Exception as exc:
#         yield _sse({"type": "snapshot_error", "message": str(exc)})

#     last_ka    = _time.monotonic()
#     last_poll  = _time.monotonic()
#     POLL_EVERY = 8.0   # seconds between direct HTTP polls

#     try:
#         while True:
#             # 1. Drain Redis pub/sub messages (non-blocking, 0.3 s timeout)
#             msg = pubsub.get_message(timeout=0.3)
#             if msg and msg["type"] == "message":
#                 try:
#                     yield _sse(json.loads(msg["data"]))
#                 except Exception:
#                     pass

#             now = _time.monotonic()

#             # 2. Keep-alive
#             if now - last_ka > 15:
#                 yield _ka()
#                 last_ka = now

#             # 3. Direct HTTP poll every POLL_EVERY seconds
#             if now - last_poll >= POLL_EVERY:
#                 try:
#                     _fetch_and_publish_details(event_id, sport_id)
#                 except Exception:
#                     pass
#                 last_poll = now

#     except GeneratorExit:
#         pass
#     finally:
#         try:
#             pubsub.unsubscribe(f"sp:live:event:{event_id}")
#             pubsub.close()
#         except Exception:
#             pass


# # =============================================================================
# # SPORTPESA LIVE — HTTP snapshot fallback (unchanged from previous version)
# # =============================================================================

# @celery.task(
#     name="tasks.sp.harvest_sport_live", bind=True,
#     max_retries=2, default_retry_delay=10,
#     soft_time_limit=120, time_limit=150, acks_late=True,
# )
# def sp_harvest_sport_live(self, sport_slug: str) -> dict:
#     """
#     HTTP snapshot of live SP matches. Runs every 60 s as a fallback.
#     The WebSocket harvester (sp_live_harvester.py) + details-poll tasks
#     provide the real sub-second feed; this keeps cache warm.
#     """
#     t0 = time.perf_counter()
#     try:
#         from app.workers.sp_live_harvester import fetch_live
#         matches = fetch_live(sport_slug, fetch_full_markets=True)
#     except Exception as exc:
#         raise self.retry(exc=exc)
#     latency = int((time.perf_counter() - t0) * 1000)
#     cache_set(f"sp:live:{sport_slug}", {
#         "source": "sportpesa", "sport": sport_slug, "mode": "live",
#         "match_count": len(matches), "harvested_at": _now_iso(),
#         "latency_ms": latency, "matches": matches,
#     }, ttl=90)
#     _upsert_and_chain(matches, "Sportpesa")
#     _emit("sportpesa", sport_slug, len(matches), latency)
#     logger.info("[sp:live] %s → %d matches %dms", sport_slug, len(matches), latency)
#     return {"ok": True, "source": "sportpesa", "sport": sport_slug,
#             "count": len(matches), "latency_ms": latency}


# @celery.task(name="tasks.sp.harvest_all_live",
#              soft_time_limit=30, time_limit=60)
# def sp_harvest_all_live() -> dict:
#     sigs = [sp_harvest_sport_live.s(s) for s in _LIVE_SPORTS]
#     group(sigs).apply_async(queue="live")
#     return {"dispatched": len(sigs)}


# # =============================================================================
# # BETIKA LIVE
# # =============================================================================

# @celery.task(
#     name="tasks.bt.harvest_sport_live", bind=True,
#     max_retries=2, default_retry_delay=15,
#     soft_time_limit=120, time_limit=150, acks_late=True,
# )
# def bt_harvest_sport_live(self, sport_slug: str) -> dict:
#     t0 = time.perf_counter()
#     try:
#         from app.workers.bt_harvester import fetch_live
#         matches = fetch_live(sport_slug, fetch_full_markets=True)
#     except Exception as exc:
#         raise self.retry(exc=exc)
#     latency = int((time.perf_counter() - t0) * 1000)
#     cache_set(f"bt:live:{sport_slug}", {
#         "source": "betika", "sport": sport_slug, "mode": "live",
#         "match_count": len(matches), "harvested_at": _now_iso(),
#         "latency_ms": latency, "matches": matches,
#     }, ttl=90)
#     _upsert_and_chain(matches, "Betika")
#     _emit("betika", sport_slug, len(matches), latency)
#     return {"ok": True, "source": "betika", "sport": sport_slug,
#             "count": len(matches), "latency_ms": latency}


# @celery.task(name="tasks.bt.harvest_all_live",
#              soft_time_limit=30, time_limit=60)
# def bt_harvest_all_live() -> dict:
#     sigs = [bt_harvest_sport_live.s(s) for s in _LIVE_SPORTS]
#     group(sigs).apply_async(queue="live")
#     return {"dispatched": len(sigs)}


# # =============================================================================
# # ODIBETS LIVE
# # =============================================================================

# @celery.task(
#     name="tasks.od.harvest_sport_live", bind=True,
#     max_retries=1, default_retry_delay=20,
#     soft_time_limit=150, time_limit=180, acks_late=True,
# )
# def od_harvest_sport_live(self, sport_slug: str) -> dict:
#     t0 = time.perf_counter()
#     try:
#         from app.workers.od_harvester import fetch_live
#         matches = fetch_live(sport_slug, fetch_full_markets=True)
#     except Exception as exc:
#         raise self.retry(exc=exc)
#     latency = int((time.perf_counter() - t0) * 1000)
#     cache_set(f"od:live:{sport_slug}", {
#         "source": "odibets", "sport": sport_slug, "mode": "live",
#         "match_count": len(matches), "harvested_at": _now_iso(),
#         "latency_ms": latency, "matches": matches,
#     }, ttl=90)
#     _upsert_and_chain(matches, "Odibets")
#     _emit("odibets", sport_slug, len(matches), latency)
#     return {"ok": True, "source": "odibets", "sport": sport_slug,
#             "count": len(matches), "latency_ms": latency}


# @celery.task(name="tasks.od.harvest_all_live",
#              soft_time_limit=30, time_limit=60)
# def od_harvest_all_live() -> dict:
#     sigs = [od_harvest_sport_live.s(s) for s in _LIVE_SPORTS]
#     group(sigs).apply_async(queue="live")
#     return {"dispatched": len(sigs)}


# # =============================================================================
# # B2B DIRECT LIVE
# # =============================================================================

# @celery.task(
#     name="tasks.b2b.harvest_sport_live", bind=True,
#     max_retries=2, default_retry_delay=20,
#     soft_time_limit=180, time_limit=210, acks_late=True,
# )
# def b2b_harvest_sport_live(self, sport_slug: str) -> dict:
#     t0 = time.perf_counter()
#     try:
#         from app.workers.b2b_harvester import fetch_b2b_sport
#         matches = fetch_b2b_sport(sport_slug, mode="live")
#     except Exception as exc:
#         raise self.retry(exc=exc)
#     latency = int((time.perf_counter() - t0) * 1000)
#     cache_set(f"b2b:live:{sport_slug}", {
#         "source": "b2b", "sport": sport_slug, "mode": "live",
#         "match_count": len(matches), "harvested_at": _now_iso(),
#         "latency_ms": latency, "matches": matches,
#     }, ttl=60)
#     for m in matches:
#         for bk_name, bk_data in (m.get("bookmakers") or {}).items():
#             bk_match = dict(m)
#             bk_match["markets"] = bk_data.get("markets") or {}
#             _upsert_and_chain([bk_match], bk_name)
#     _emit("b2b", sport_slug, len(matches), latency)
#     return {"ok": True, "source": "b2b", "sport": sport_slug,
#             "count": len(matches), "latency_ms": latency}


# @celery.task(name="tasks.b2b.harvest_all_live",
#              soft_time_limit=30, time_limit=60)
# def b2b_harvest_all_live() -> dict:
#     sigs = [b2b_harvest_sport_live.s(s) for s in _LIVE_SPORTS]
#     group(sigs).apply_async(queue="live")
#     return {"dispatched": len(sigs)}


# # =============================================================================
# # B2B PAGE FAN-OUT LIVE (legacy)
# # =============================================================================

# @celery.task(
#     name="tasks.b2b_page.harvest_page_live", bind=True,
#     max_retries=2, default_retry_delay=10,
#     soft_time_limit=45, time_limit=60, acks_late=True,
# )
# def b2b_page_harvest_page_live(self, bookmaker: dict, sport: str) -> dict:
#     from app.workers.celery_tasks import _upsert_unified_match
#     bk_name = bookmaker.get("name") or bookmaker.get("domain", "?")
#     bk_id   = bookmaker.get("id")
#     t0      = time.perf_counter()
#     try:
#         from app.workers.bookmaker_fetcher import fetch_bookmaker
#         matches = fetch_bookmaker(bookmaker, sport_name=sport, mode="live",
#                                   page=1, page_size=50, timeout=20)
#     except Exception as exc:
#         raise self.retry(exc=exc)
#     latency = int((time.perf_counter() - t0) * 1000)
#     cache_set(f"odds:live:{sport.lower().replace(' ', '_')}:{bk_id}", {
#         "bookmaker_id": bk_id, "bookmaker_name": bk_name,
#         "sport": sport, "mode": "live",
#         "match_count": len(matches), "harvested_at": _now_iso(),
#         "latency_ms": latency, "matches": matches,
#     }, ttl=60)
#     for m in matches:
#         _upsert_unified_match(m, bk_id, bk_name)
#     _publish(WS_CHANNEL, {
#         "event": "odds_updated", "source": "b2b",
#         "bookmaker": bk_name, "sport": sport, "mode": "live",
#         "count": len(matches), "ts": _now_iso(),
#     })
#     return {"ok": True, "count": len(matches), "latency_ms": latency}


# @celery.task(name="tasks.b2b_page.harvest_all_live",
#              soft_time_limit=30, time_limit=60)
# def b2b_page_harvest_all_live() -> dict:
#     import json as _json
#     from app.workers.celery_tasks import _redis
#     raw        = _redis().get("cache:bookmakers:active")
#     bookmakers = _json.loads(raw) if raw else []
#     sigs = [
#         b2b_page_harvest_page_live.s(bm, sport)
#         for bm in bookmakers
#         for sport in _B2B_LIVE_SPORTS
#     ]
#     group(sigs).apply_async(queue="live")
#     return {"dispatched": len(sigs)}


# # =============================================================================
# # SBO LIVE
# # =============================================================================

# @celery.task(
#     name="tasks.sbo.harvest_sport_live", bind=True,
#     max_retries=1, default_retry_delay=20,
#     soft_time_limit=120, time_limit=150, acks_late=True,
# )
# def sbo_harvest_sport_live(self, sport_slug: str) -> dict:
#     t0 = time.perf_counter()
#     try:
#         from app.views.sbo.sbo_fetcher import OddsAggregator, SPORT_CONFIG
#         cfg = next((c for c in SPORT_CONFIG if c["sport"] == sport_slug), None)
#         if not cfg:
#             return {"ok": False, "error": f"Unknown sport: {sport_slug}"}
#         agg     = OddsAggregator(cfg, fetch_full_sp_markets=True, fetch_full_bt_markets=True)
#         matches = agg.run(max_matches=30, mode="live")
#     except Exception as exc:
#         raise self.retry(exc=exc)
#     latency = int((time.perf_counter() - t0) * 1000)
#     cache_set(f"sbo:live:{sport_slug}", {
#         "sport": sport_slug, "match_count": len(matches),
#         "harvested_at": _now_iso(), "latency_ms": latency, "matches": matches,
#     }, ttl=60)
#     _publish(WS_CHANNEL, {
#         "event": "odds_updated", "source": "sbo",
#         "sport": sport_slug, "mode": "live", "count": len(matches), "ts": _now_iso(),
#     })
#     return {"ok": True, "count": len(matches), "latency_ms": latency}


# @celery.task(name="tasks.sbo.harvest_all_live",
#              soft_time_limit=30, time_limit=60)
# def sbo_harvest_all_live() -> dict:
#     sigs = [sbo_harvest_sport_live.s(s) for s in _SBO_LIVE_SPORTS]
#     group(sigs).apply_async(queue="live")
#     return {"dispatched": len(sigs)}

"""
app/workers/tasks_live.py
==========================
All Celery tasks for live-event detail polling and harvesting.

Public API (imported by sp_live_view.py + tasks_ops.py)
────────────────────────────────────────────────────────
  _fetch_and_publish_details(event_id, sport_id)  → dict
      Synchronous: fetch SP /event-details/{id}, normalise markets,
      publish market_update + event_update frames to Redis pub/sub.

  sp_poll_event_details          Celery task wrapping the above.
  sp_poll_all_event_details      Celery task: poll every tracked live event.

  stream_event_details_sse(event_id, sport_id)    → Generator[str, None, None]
      SSE generator for sp_live_view.stream_event_live().
      Layer 1: Redis pub/sub (instant — from WS harvester + this task)
      Layer 2: direct HTTP poll every 8 s as fallback

  sp_harvest_all_live            snapshot all SP live sports → Redis
  bt_harvest_all_live            poll all Betika live sports → Redis
  od_harvest_all_live            stub (placeholder)
  b2b_harvest_all_live           stub (placeholder)
  b2b_page_harvest_all_live      stub (placeholder)
  sbo_harvest_all_live           stub (placeholder)

SP event-details response shape (flat, no "event" wrapper)
──────────────────────────────────────────────────────────
  {
    "id": 12345,
    "status": "Started",
    "isPaused": false,
    "externalId": 99999,
    "state": {
      "currentEventPhase": "SecondHalf",
      "matchTime": "67",
      "clockRunning": true,
      "remainingTimeMillis": null,
      "matchScore": {"home": 1, "away": 0}
    },
    "markets": [
      {
        "id": 194,
        "name": "1X2",
        "specialValue": "0.00",
        "eventMarketId": 98765,
        "selections": [
          {"id": 111111, "name": "Arsenal", "shortName": "1", "odds": "1.45", "status": "Open"},
          {"id": 222222, "name": "draw",    "shortName": "X", "odds": "4.50", "status": "Open"},
          {"id": 333333, "name": "Chelsea", "shortName": "2", "odds": "7.20", "status": "Open"}
        ]
      },
      ...
    ]
  }
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Generator

import requests

from app.workers.celery_tasks import celery, _publish
from app.workers.sp_live_harvester import (
    _get_redis,
    _ch_sport,
    _ch_event,
    _HEADERS,
    _SP_BASE,
    _LIVE_SPORT_IDS,
    _SLUG_TO_SPORT_ID,
    _market_slug_from_type,
    _normalise_outcome_key,
    _record_market_snapshot,
    _record_state_snapshot,
    _update_registry,
    _sr_event_map,
    _sr_event_sport,
    _sr_map_lock,
    CH_ALL,
    fetch_live_events,
    fetch_live_sports,
    snapshot_all_sports,
)

log = logging.getLogger("tasks_live")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sp_get(path: str, params: dict | None = None, timeout: int = 10) -> Any:
    url = f"{_SP_BASE}{path}"
    try:
        r = requests.get(url, headers=_HEADERS, params=params,
                         timeout=timeout, allow_redirects=True)
        if not r.ok:
            log.debug("SP HTTP %d → %s", r.status_code, url)
            return None
        return r.json()
    except Exception as exc:
        log.debug("SP HTTP error %s: %s", url, exc)
        return None


def _parse_selection(sel: dict, idx: int, mkt_type: int) -> dict:
    """Normalise one SP selection into the standard {id, name, outcome_key, odds, status} shape."""
    raw_name   = str(sel.get("name") or sel.get("shortName") or "")
    short_name = str(sel.get("shortName") or "")
    outcome_key = _normalise_outcome_key(
        short_name or raw_name, idx, mkt_type,
    )
    return {
        "id":          sel.get("id"),
        "name":        raw_name,
        "shortName":   short_name,
        "outcome_key": outcome_key,
        "odds":        str(sel.get("odds") or "0"),
        "status":      sel.get("status") or "Open",
    }


def _build_market_update(
    event_id:  int,
    sport_id:  int,
    mkt:       dict,
    all_prev:  dict,          # {sel_id: odds_str} — for changed_selections diff
) -> dict | None:
    """Turn one raw SP market dict into a market_update payload."""
    mkt_type = int(mkt.get("id") or mkt.get("typeId") or 0)
    if not mkt_type:
        return None

    sv       = str(mkt.get("specialValue") or mkt.get("specValue") or "0.00")
    raw_sels = mkt.get("selections") or []
    if not raw_sels:
        return None

    slug     = _market_slug_from_type(mkt_type, sport_id, sv)
    norm_sels = [_parse_selection(s, i, mkt_type) for i, s in enumerate(raw_sels)]

    # Compute changed_selections (price moved since last publish)
    changed: list[dict] = []
    for ns in norm_sels:
        sel_id   = ns.get("id")
        new_odds = ns["odds"]
        if sel_id and new_odds and float(new_odds) > 1.0:
            prev = all_prev.get(sel_id)
            if prev is not None and prev != new_odds:
                changed.append({"id": sel_id, "odds": new_odds, "prev": prev})
            all_prev[sel_id] = new_odds  # update in-place

    return {
        "type":                  "market_update",
        "event_id":              event_id,
        "sport_id":              sport_id,
        "market_id":             mkt.get("eventMarketId") or mkt_type,
        "market_type":           mkt_type,
        "market_name":           mkt.get("name") or "",
        "market_slug":           slug,
        "handicap":              sv,
        "market_status":         mkt.get("status") or "Open",
        "all_selections":        [
            {"id": s.get("id"), "name": s.get("name"), "odds": str(s.get("odds") or "0"),
             "status": s.get("status") or "Open"}
            for s in raw_sels
        ],
        "normalised_selections": norm_sels,
        "changed_selections":    changed,
        "ts":                    _now_ts(),
        # Tag so frontend SSE handler knows this came from a details poll
        "source":                "details_poll",
    }


def _build_event_update(event_id: int, sport_id: int, raw: dict) -> dict:
    """Turn raw SP event-details top-level fields into an event_update payload."""
    state = raw.get("state") or {}
    score = state.get("matchScore") or {}
    return {
        "type":          "event_update",
        "event_id":      event_id,
        "sport_id":      sport_id,
        "status":        raw.get("status") or "",
        "phase":         state.get("currentEventPhase") or "",
        "match_time":    state.get("matchTime") or "",
        "clock_running": state.get("clockRunning") if "clockRunning" in state else True,
        "remaining_ms":  state.get("remainingTimeMillis"),
        "score_home":    str(score.get("home") or "0"),
        "score_away":    str(score.get("away") or "0"),
        "is_paused":     raw.get("isPaused") or False,
        "ts":            _now_ts(),
        "source":        "details_poll",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Core: fetch + parse + publish
# ─────────────────────────────────────────────────────────────────────────────

# Per-event selection-odds cache for changed_selections diff
# {event_id: {sel_id: odds_str}}
_prev_odds_cache: dict[int, dict] = {}


def _fetch_and_publish_details(
    event_id: int,
    sport_id: int = 1,
) -> dict:
    """
    Fetch /api/live/events/{event_id}/details from SP, parse ALL markets,
    publish one market_update frame per market + one event_update frame
    to the three Redis pub/sub channels (all / sport / event).

    Returns summary dict suitable for direct HTTP response.
    """
    t0   = time.perf_counter()
    r    = _get_redis()
    raw  = _sp_get(f"/api/live/events/{event_id}/details", timeout=8)

    if not raw or not isinstance(raw, dict):
        return {"ok": False, "reason": "no data from SP", "event_id": event_id}

    # SP returns flat: {id, state, markets, ...}
    # (NOT {event:{...}, markets:[...]} — that was the bug in v8)
    actual_id = int(raw.get("id") or event_id)

    prev_odds = _prev_odds_cache.setdefault(actual_id, {})

    markets_raw = raw.get("markets") or []
    published   = 0

    # ── Event-state update (publish first for lowest latency) ───────────────
    ev_payload = _build_event_update(actual_id, sport_id, raw)
    try:
        raw_ev = json.dumps(ev_payload, ensure_ascii=False)
        pipe = r.pipeline()
        pipe.publish(CH_ALL, raw_ev)
        pipe.publish(_ch_sport(sport_id), raw_ev)
        pipe.publish(_ch_event(actual_id), raw_ev)
        pipe.execute()
        _record_state_snapshot(r, actual_id, ev_payload)
        _update_registry(actual_id, sport_id, state=ev_payload)
        # Cache current state for /state/<event_id> endpoint
        r.set(f"sp:live:state:{actual_id}",
              json.dumps({k: v for k, v in ev_payload.items() if k != "type"}),
              ex=300)
    except Exception as exc:
        log.debug("publish event_update %d: %s", actual_id, exc)

    # ── Market updates ───────────────────────────────────────────────────────
    for mkt in markets_raw:
        payload = _build_market_update(actual_id, sport_id, mkt, prev_odds)
        if not payload:
            continue
        try:
            raw_mkt = json.dumps(payload, ensure_ascii=False)
            pipe = r.pipeline()
            pipe.publish(CH_ALL, raw_mkt)
            pipe.publish(_ch_sport(sport_id), raw_mkt)
            pipe.publish(_ch_event(actual_id), raw_mkt)
            pipe.execute()
            _record_market_snapshot(r, actual_id, payload)
            _update_registry(actual_id, sport_id, market=payload)
            published += 1
        except Exception as exc:
            log.debug("publish market_update %d mkt%d: %s", actual_id, mkt.get("id", "?"), exc)

    latency_ms = int((time.perf_counter() - t0) * 1000)
    return {
        "ok":          True,
        "event_id":    actual_id,
        "sport_id":    sport_id,
        "markets_fetched": len(markets_raw),
        "markets_published": published,
        "phase":       ev_payload.get("phase", ""),
        "score":       f"{ev_payload['score_home']}-{ev_payload['score_away']}",
        "latency_ms":  latency_ms,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Celery tasks — single event + all events
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(
    name="tasks.live.sp_poll_event_details",
    bind=True,
    max_retries=2,
    soft_time_limit=10,
    time_limit=15,
    acks_late=True,
)
def sp_poll_event_details(self, event_id: int, sport_id: int = 1) -> dict:
    """
    Celery task: fetch + publish details for one SP live event.
    Dispatched on-demand by POST /api/sp/live/trigger-details/<event_id>.
    Also called by sp_poll_all_event_details.
    """
    try:
        return _fetch_and_publish_details(event_id, sport_id)
    except Exception as exc:
        log.warning("sp_poll_event_details %d failed: %s", event_id, exc)
        raise self.retry(exc=exc, countdown=3)


@celery.task(
    name="tasks.live.sp_poll_all_event_details",
    soft_time_limit=25,
    time_limit=30,
    acks_late=True,
)
def sp_poll_all_event_details() -> dict:
    """
    Celery beat task (every 5 s): poll SP event-details for every event
    currently tracked by the WS harvester.

    The WS harvester maintains _sr_event_map {event_id → external_id} and
    _sr_event_sport {event_id → sport_id}.  We read those maps here.

    For events not in the map (e.g. before WS connects), we fall back to
    fetching the live events list for each live sport.
    """
    t0 = time.perf_counter()

    with _sr_map_lock:
        ev_sport = dict(_sr_event_sport)   # {event_id: sport_id}

    # If WS hasn't connected yet, fetch live event list directly
    if not ev_sport:
        for sport_id in _LIVE_SPORT_IDS:
            try:
                events = fetch_live_events(sport_id, limit=200)
                for ev in events:
                    eid = ev.get("id")
                    if eid:
                        ev_sport[int(eid)] = sport_id
            except Exception:
                pass

    if not ev_sport:
        return {"ok": True, "polled": 0, "reason": "no tracked events"}

    polled = 0
    errors = 0
    for event_id, sport_id in ev_sport.items():
        try:
            _fetch_and_publish_details(int(event_id), sport_id)
            polled += 1
        except Exception as exc:
            log.debug("poll_all detail fail ev=%d: %s", event_id, exc)
            errors += 1
        # Small sleep between calls so we don't hammer SP
        time.sleep(0.05)

    return {
        "ok":      True,
        "polled":  polled,
        "errors":  errors,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    }


# ─────────────────────────────────────────────────────────────────────────────
# SSE generator — used by sp_live_view.stream_event_live()
# ─────────────────────────────────────────────────────────────────────────────

def stream_event_details_sse(
    event_id: int,
    sport_id: int = 1,
) -> Generator[str, None, None]:
    """
    SSE generator that combines:
      Layer 1 — Redis pub/sub sp:live:event:{id}
                Receives WS market_update + event_update instantly.
                Also receives frames published by sp_poll_event_details (every 5 s).
      Layer 2 — Direct HTTP poll every 8 s as fallback
                Guarantees freshness even if Celery beat is slow.

    Yields raw SSE text ("data: {...}\\n\\n" or ": keep-alive\\n\\n").

    Called from Flask view with stream_with_context(), so generator cleanup
    fires on client disconnect.
    """
    r       = _get_redis()
    channel = f"sp:live:event:{event_id}"

    def _sse(data: dict) -> str:
        return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

    def _ka() -> str:
        return f": keep-alive {_now_ts()}\n\n"

    # ── Connected frame ──────────────────────────────────────────────────────
    yield _sse({
        "type":     "connected",
        "event_id": event_id,
        "sport_id": sport_id,
        "channel":  channel,
        "ts":       _now_ts(),
    })

    # ── Immediate detail poll so client has odds on first frame ──────────────
    try:
        _fetch_and_publish_details(event_id, sport_id)
        yield _sse({"type": "snapshot_done", "event_id": event_id, "ts": _now_ts()})
    except Exception as exc:
        log.debug("initial poll %d: %s", event_id, exc)

    # ── Subscribe and stream ──────────────────────────────────────────────────
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(channel)

    last_ka     = time.monotonic()
    last_direct = time.monotonic()
    DIRECT_SEC  = 8.0    # direct HTTP fallback cadence
    KA_SEC      = 15.0   # keep-alive cadence

    try:
        while True:
            # Non-blocking message read
            msg = pubsub.get_message(timeout=0.5)
            if msg and msg.get("type") == "message":
                try:
                    yield _sse(json.loads(msg["data"]))
                except Exception:
                    pass

            now = time.monotonic()

            # Keep-alive
            if now - last_ka >= KA_SEC:
                yield _ka()
                last_ka = now

            # Fallback direct poll (fires if Redis is quiet / Celery beat lagging)
            if now - last_direct >= DIRECT_SEC:
                try:
                    _fetch_and_publish_details(event_id, sport_id)
                except Exception as exc:
                    log.debug("fallback poll %d: %s", event_id, exc)
                last_direct = now

    except GeneratorExit:
        pass
    finally:
        try:
            pubsub.unsubscribe(channel)
            pubsub.close()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Live harvest tasks — snapshot all live matches into Redis
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(
    name="tasks.sp.harvest_all_live",
    soft_time_limit=50,
    time_limit=60,
    acks_late=True,
)
def sp_harvest_all_live() -> dict:
    """
    Celery beat task (every 60 s): snapshot all SP live sports into Redis.
    This is the HTTP fallback — the WS harvester is primary.
    Each snapshot is stored at sp:live:snapshot:{sport_id} (TTL 90 s).
    """
    try:
        result = snapshot_all_sports()
        total  = sum(len(v) for v in result.values())
        return {"ok": True, "sports": len(result), "events": total}
    except Exception as exc:
        log.error("sp_harvest_all_live: %s", exc)
        return {"ok": False, "error": str(exc)}


@celery.task(
    name="tasks.bt.harvest_all_live",
    soft_time_limit=30,
    time_limit=40,
    acks_late=True,
)
def bt_harvest_all_live() -> dict:
    """
    Celery beat task (every 90 s): snapshot all Betika live matches into Redis.
    The BetikaLivePoller background thread is primary (polls every 1.5 s),
    this task is a backup snapshot in case the thread is down.
    """
    try:
        from app.workers.bt_harvester import (
            fetch_live_matches, fetch_live_sports,
            get_live_poller, start_live_poller,
            _LIVE_DATA_KEY, _LIVE_SPORTS_KEY,
        )
        import json as _json
        r = _get_redis()

        # Refresh sport counts
        sports = fetch_live_sports()
        if sports:
            r.set(_LIVE_SPORTS_KEY,
                  _json.dumps({"ok": True, "sports": sports}), ex=30)

        # Snapshot per sport
        total = 0
        by_sport: dict[int, list] = {}
        matches = fetch_live_matches()
        for m in matches:
            by_sport.setdefault(m["bt_sport_id"], []).append(m)
            total += 1

        for bt_sport_id, sport_matches in by_sport.items():
            r.set(_LIVE_DATA_KEY.format(sport_id=bt_sport_id),
                  _json.dumps(sport_matches, ensure_ascii=False), ex=60)

        return {"ok": True, "sports": len(by_sport), "events": total}
    except Exception as exc:
        log.error("bt_harvest_all_live: %s", exc)
        return {"ok": False, "error": str(exc)}


# Stubs for other bookmakers (wire real harvesters when available)

@celery.task(name="tasks.od.harvest_all_live", soft_time_limit=60, time_limit=75, acks_late=True)
def od_harvest_all_live() -> dict:
    """
    OdiBets live harvester — polls /sportsbook/v1?resource=live, writes
    per-sport snapshots to Redis, ensures the OdiBetsLivePoller is running.
    Runs every 90 s as a belt-and-suspenders complement to the live poller thread.
    """
    try:
        import json as _json
        from app.workers.od_harvester import (
            fetch_live_matches,
            get_live_poller, init_live_poller,
            _LIVE_DATA_KEY,
            od_sport_to_slug,
        )
        r = _get_redis()

        # Ensure poller thread is running
        if get_live_poller() is None or not get_live_poller().alive:
            try:
                init_live_poller(r, interval=2.0)
            except Exception as _pe:
                log.debug("od poller init in task: %s", _pe)

        matches = fetch_live_matches()
        by_sport: dict[int, list] = {}
        for m in matches:
            by_sport.setdefault(m["od_sport_id"], []).append(m)

        for od_sport_id, sport_matches in by_sport.items():
            r.set(
                _LIVE_DATA_KEY.format(sport_id=od_sport_id),
                _json.dumps(sport_matches, ensure_ascii=False),
                ex=60,
            )

        total = sum(len(v) for v in by_sport.values())
        return {"ok": True, "sports": len(by_sport), "events": total}
    except Exception as exc:
        log.error("od_harvest_all_live: %s", exc)
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.b2b.harvest_all_live", soft_time_limit=30, time_limit=40)
def b2b_harvest_all_live() -> dict:
    """B2B direct live harvester stub."""
    return {"ok": True, "note": "b2b harvester not yet implemented"}


@celery.task(name="tasks.b2b_page.harvest_all_live", soft_time_limit=30, time_limit=40)
def b2b_page_harvest_all_live() -> dict:
    """B2B page-scrape live harvester stub."""
    return {"ok": True, "note": "b2b_page harvester not yet implemented"}


@celery.task(name="tasks.sbo.harvest_all_live", soft_time_limit=30, time_limit=40)
def sbo_harvest_all_live() -> dict:
    """SBO live harvester stub."""
    return {"ok": True, "note": "sbo harvester not yet implemented"}


# ─────────────────────────────────────────────────────────────────────────────
# Upcoming harvest stubs (tasks_ops.py imports these too)
# ─────────────────────────────────────────────────────────────────────────────

@celery.task(name="tasks.sp.harvest_all_upcoming", soft_time_limit=120, time_limit=150)
def sp_harvest_all_upcoming() -> dict:
    """SP upcoming — delegates to sp_harvester tasks."""
    try:
        from app.workers.sp_harvester import fetch_upcoming
        from app.workers.celery_tasks import cache_set
        SPORTS = ["soccer","basketball","tennis","ice-hockey","volleyball","rugby",
                  "handball","cricket","table-tennis","darts","mma"]
        total = 0
        for slug in SPORTS:
            try:
                matches = fetch_upcoming(slug)
                if matches:
                    cache_set(f"sp:upcoming:{slug}", {"matches": matches,
                              "harvested_at": _now_ts()}, ttl=360)
                    total += len(matches)
            except Exception as exc:
                log.debug("sp_harvest_upcoming %s: %s", slug, exc)
        return {"ok": True, "total": total}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.bt.harvest_all_upcoming", soft_time_limit=180, time_limit=200)
def bt_harvest_all_upcoming() -> dict:
    """Betika upcoming — delegates to bt_harvester."""
    try:
        from app.workers.bt_harvester import (
            fetch_upcoming_matches, cache_upcoming, SLUG_TO_BT_SPORT,
        )
        r     = _get_redis()
        total = 0
        for slug in list(SLUG_TO_BT_SPORT.keys())[:8]:   # cap for beat safety
            try:
                matches = fetch_upcoming_matches(sport_slug=slug, max_pages=5)
                if matches:
                    cache_upcoming(r, slug, matches, ttl=360)
                    total += len(matches)
            except Exception as exc:
                log.debug("bt_harvest_upcoming %s: %s", slug, exc)
        return {"ok": True, "total": total}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.od.harvest_all_upcoming", soft_time_limit=300, time_limit=330, acks_late=True)
def od_harvest_all_upcoming() -> dict:
    """
    OdiBets upcoming harvest — fetches all main sports and writes
    to od:upcoming:{sport_slug}:data Redis cache.
    Runs every 7 min via beat schedule.
    """
    try:
        from app.workers.od_harvester import (
            fetch_upcoming_matches,
            cache_upcoming,
            OD_SPORT_IDS,
        )
        r = _get_redis()
        SPORTS = ["soccer", "basketball", "tennis", "volleyball",
                  "handball", "rugby", "cricket", "table-tennis"]
        total = 0
        for slug in SPORTS:
            try:
                matches = fetch_upcoming_matches(sport_slug=slug)
                if matches:
                    cache_upcoming(r, slug, matches, ttl=420)
                    total += len(matches)
                    log.debug("od_upcoming %s: %d matches", slug, len(matches))
            except Exception as exc:
                log.warning("od_upcoming %s: %s", slug, exc)
        return {"ok": True, "total": total, "sports": len(SPORTS)}
    except Exception as exc:
        log.error("od_harvest_all_upcoming: %s", exc)
        return {"ok": False, "error": str(exc)}


@celery.task(name="tasks.b2b.harvest_all_upcoming", soft_time_limit=60, time_limit=70)
def b2b_harvest_all_upcoming() -> dict:
    return {"ok": True, "note": "b2b upcoming stub"}


@celery.task(name="tasks.b2b_page.harvest_all_upcoming", soft_time_limit=60, time_limit=70)
def b2b_page_harvest_all_upcoming() -> dict:
    return {"ok": True, "note": "b2b_page upcoming stub"}


@celery.task(name="tasks.sbo.harvest_all_upcoming", soft_time_limit=60, time_limit=70)
def sbo_harvest_all_upcoming() -> dict:
    return {"ok": True, "note": "sbo upcoming stub"}