# """
# app/views/od_module.py
# =======================
# OdiBets Flask Blueprint.

# Endpoints
# ─────────
#   GET  /api/od/upcoming/<sport>          — upcoming matches (paginated, cached)
#   GET  /api/od/stream/upcoming/<sport>   — SSE stream of upcoming fetch
#   POST /api/od/stream/trigger/<sport>    — force fresh harvest
#   GET  /api/od/live/<sport>              — live snapshot from Redis
#   GET  /api/od/live/stream/<sport>       — SSE stream of live updates
#   GET  /api/od/event/<event_id>          — full market detail for one event
#   POST /api/od/cache/bust/<sport>        — delete stale upcoming cache
#   GET  /api/od/meta/sports               — sport list
#   GET  /api/od/meta/markets/<sport>      — market list for filter drawer
#   GET  /api/od/status                    — health check
# """

# from __future__ import annotations

# import json
# import logging
# import time
# from datetime import datetime

# import redis as redis_lib
# from flask import Blueprint, Response, request, stream_with_context

# from app.workers.od_harvester import (
#     OD_SPORT_IDS,
#     OD_SPORT_SLUGS,
#     _LIVE_CHAN_KEY,
#     _UPC_CHAN_KEY,
#     _UPC_DATA_KEY,
#     cache_upcoming,
#     fetch_event_detail,
#     fetch_live_matches,
#     fetch_upcoming_matches,
#     get_cached_live,
#     get_cached_upcoming,
#     get_live_poller,
#     init_live_poller,
#     od_sport_to_slug,
#     slug_to_od_sport_id,
# )

# logger = logging.getLogger(__name__)
# bp     = Blueprint("od", __name__, url_prefix="/api/od")

# PER_PAGE = 25


# # ── Helpers ───────────────────────────────────────────────────────────────────

# def _redis() -> redis_lib.Redis | None:
#     try:
#         from flask import current_app
#         return current_app.extensions.get("redis") or current_app.config.get("REDIS_CLIENT")
#     except Exception:
#         return None


# def _sse(data: dict) -> str:
#     return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


# def _market_metas(od_sport_id: int) -> list[dict]:
#     """Primary markets for the market-filter drawer."""
#     SOCCER = [
#         {"slug": "1x2",                    "label": "1X2",             "is_primary": True},
#         {"slug": "over_under_goals",       "label": "Goals O/U",       "is_primary": True},
#         {"slug": "btts",                   "label": "BTTS (GG/NG)",    "is_primary": True},
#         {"slug": "double_chance",          "label": "Double Chance",   "is_primary": True},
#         {"slug": "draw_no_bet",            "label": "Draw No Bet",     "is_primary": True},
#         {"slug": "european_handicap",      "label": "Handicap",        "is_primary": True},
#         {"slug": "correct_score",          "label": "Correct Score",   "is_primary": False},
#         {"slug": "ht_ft",                  "label": "HT/FT",           "is_primary": False},
#         {"slug": "first_half_1x2",         "label": "1H 1X2",          "is_primary": False},
#         {"slug": "first_half_over_under",  "label": "1H O/U",          "is_primary": False},
#         {"slug": "btts_and_result",        "label": "BTTS + Result",   "is_primary": False},
#         {"slug": "result_and_over_under",  "label": "Result + O/U",    "is_primary": False},
#         {"slug": "winning_margin",         "label": "Winning Margin",  "is_primary": False},
#         {"slug": "total_goals_home",       "label": "Home Goals O/U",  "is_primary": False},
#         {"slug": "total_goals_away",       "label": "Away Goals O/U",  "is_primary": False},
#         {"slug": "asian_handicap",         "label": "Asian HC",        "is_primary": False},
#         {"slug": "exact_goals",            "label": "Exact Goals",     "is_primary": False},
#         {"slug": "odd_even",               "label": "Odd/Even Goals",  "is_primary": False},
#         {"slug": "first_team_to_score",    "label": "1st Team to Score","is_primary": False},
#         {"slug": "clean_sheet_home",       "label": "Clean Sheet H",   "is_primary": False},
#         {"slug": "clean_sheet_away",       "label": "Clean Sheet A",   "is_primary": False},
#         {"slug": "total_corners",          "label": "Total Corners",   "is_primary": False},
#         {"slug": "total_bookings",         "label": "Total Bookings",  "is_primary": False},
#     ]
#     BASKET = [
#         {"slug": "match_winner",     "label": "Winner",       "is_primary": True},
#         {"slug": "point_spread",     "label": "Spread",       "is_primary": True},
#         {"slug": "total_points",     "label": "Total Pts",    "is_primary": True},
#     ]
#     TENNIS = [
#         {"slug": "match_winner",     "label": "Winner",       "is_primary": True},
#         {"slug": "total_games",      "label": "Total Games",  "is_primary": True},
#         {"slug": "set_betting",      "label": "Set Betting",  "is_primary": True},
#     ]
#     sport_slug = od_sport_to_slug(od_sport_id)
#     if sport_slug == "basketball":
#         return BASKET
#     if sport_slug == "tennis":
#         return TENNIS
#     return SOCCER


# # ══════════════════════════════════════════════════════════════════════════════
# # UPCOMING
# # ══════════════════════════════════════════════════════════════════════════════

# @bp.route("/upcoming/<sport>")
# def upcoming(sport: str):
#     """
#     GET /api/od/upcoming/<sport>
#     Query params: page, per_page, day, competition_id, sort, order,
#                   comp (filter), team (filter), market (filter slug)
#     """
#     t0          = time.time()
#     page        = int(request.args.get("page", 1))
#     per_page    = int(request.args.get("per_page", PER_PAGE))
#     day         = request.args.get("day", "")
#     comp_id     = request.args.get("competition_id", "")
#     sort_by     = request.args.get("sort", "start_time")
#     order       = request.args.get("order", "asc")
#     comp_filter = request.args.get("comp", "").lower()
#     team_filter = request.args.get("team", "").lower()
#     mkt_filter  = request.args.get("market", "").lower()

#     rd          = _redis()
#     od_sport_id = slug_to_od_sport_id(sport)

#     # Try cache
#     matches = get_cached_upcoming(rd, sport) if rd else None

#     if not matches:
#         matches = fetch_upcoming_matches(
#             sport_slug=sport, day=day, competition_id=comp_id,
#         )
#         if rd and matches:
#             cache_upcoming(rd, sport, matches, ttl=300)

#     # Filters
#     if comp_filter:
#         matches = [m for m in matches if comp_filter in (m.get("competition") or "").lower()]
#     if team_filter:
#         matches = [m for m in matches if
#             team_filter in (m.get("home_team") or "").lower() or
#             team_filter in (m.get("away_team") or "").lower()]
#     if mkt_filter:
#         matches = [m for m in matches if mkt_filter in (m.get("markets") or {})]

#     # Sort
#     rev = order == "desc"
#     if sort_by == "competition":
#         matches.sort(key=lambda m: m.get("competition") or "", reverse=rev)
#     elif sort_by == "market_count":
#         matches.sort(key=lambda m: m.get("market_count") or 0, reverse=not rev)
#     else:
#         matches.sort(key=lambda m: m.get("start_time") or "", reverse=rev)

#     total = len(matches)
#     pages = max(1, -(-total // per_page))   # ceil div
#     start = (page - 1) * per_page
#     page_matches = matches[start: start + per_page]

#     return {
#         "ok":           True,
#         "sport":        sport,
#         "od_sport_id":  od_sport_id,
#         "matches":      page_matches,
#         "total":        total,
#         "pages":        pages,
#         "page":         page,
#         "latency_ms":   int((time.time() - t0) * 1000),
#         "harvested_at": datetime.utcnow().isoformat(),
#     }


# @bp.route("/stream/upcoming/<sport>")
# def stream_upcoming(sport: str):
#     """
#     GET /api/od/stream/upcoming/<sport>
#     SSE stream — yields 'match' events then 'done'.
#     """
#     od_sport_id  = slug_to_od_sport_id(sport)
#     day          = request.args.get("day", "")
#     comp_id      = request.args.get("competition_id", "")

#     @stream_with_context
#     def _gen():
#         yield _sse({"type": "start", "sport": sport, "estimated_max": 500})
#         t0 = time.time()
#         try:
#             matches = fetch_upcoming_matches(sport_slug=sport, day=day, competition_id=comp_id)
#             for m in matches:
#                 yield _sse({"type": "match", "match": m})
#             latency = int((time.time() - t0) * 1000)
#             yield _sse({
#                 "type": "done", "total": len(matches),
#                 "latency_ms": latency, "harvested_at": datetime.utcnow().isoformat(),
#             })
#             # Cache the result
#             rd = _redis()
#             if rd and matches:
#                 cache_upcoming(rd, sport, matches, ttl=300)
#         except Exception as exc:
#             yield _sse({"type": "error", "message": str(exc)})

#     return Response(_gen(), mimetype="text/event-stream", headers={
#         "Cache-Control":  "no-cache",
#         "X-Accel-Buffering": "no",
#     })


# @bp.route("/stream/trigger/<sport>", methods=["POST"])
# def trigger_harvest(sport: str):
#     """POST /api/od/stream/trigger/<sport> — bust cache + re-harvest."""
#     t0 = time.time()
#     rd = _redis()

#     # Bust cache
#     if rd:
#         try:
#             rd.delete(_UPC_DATA_KEY.format(sport_slug=sport))
#         except Exception:
#             pass

#     try:
#         matches = fetch_upcoming_matches(sport_slug=sport)
#         if rd and matches:
#             cache_upcoming(rd, sport, matches, ttl=300)
#         return {
#             "ok":      True,
#             "sport":   sport,
#             "count":   len(matches),
#             "latency_ms": int((time.time() - t0) * 1000),
#             "markets_sample": list((matches[0].get("markets") or {}).keys())[:6] if matches else [],
#         }
#     except Exception as exc:
#         return {"ok": False, "error": str(exc)}, 500


# # ══════════════════════════════════════════════════════════════════════════════
# # LIVE
# # ══════════════════════════════════════════════════════════════════════════════

# @bp.route("/live/<sport>")
# def live_snapshot(sport: str):
#     """GET /api/od/live/<sport> — current live snapshot from Redis (or fresh fetch)."""
#     t0          = time.time()
#     od_sport_id = slug_to_od_sport_id(sport)
#     rd          = _redis()

#     matches = get_cached_live(rd, od_sport_id) if rd else None
#     if not matches:
#         matches = fetch_live_matches(sport_slug=sport) or []

#     return {
#         "ok":         True,
#         "sport":      sport,
#         "matches":    matches,
#         "total":      len(matches),
#         "latency_ms": int((time.time() - t0) * 1000),
#     }


# @bp.route("/live/stream/<sport>")
# def live_stream(sport: str):
#     """
#     GET /api/od/live/stream/<sport>
#     SSE stream of live match updates. Client connects once and stays connected.

#     Event types (same shape as Betika/SP):
#       {type: "connected", sport, ts}
#       {type: "snapshot",  matches: [...], total}
#       {type: "batch_update", events: [{match_id, market_slug, outcome_key, odd, ...}]}
#     """
#     od_sport_id = slug_to_od_sport_id(sport)
#     rd          = _redis()

#     # Ensure poller is running
#     if rd:
#         try:
#             init_live_poller(rd, interval=2.0)
#         except Exception as exc:
#             logger.warning("OD live poller init: %s", exc)

#     @stream_with_context
#     def _gen():
#         yield _sse({"type": "connected", "sport": sport, "ts": time.time()})

#         # Send initial snapshot
#         snap = get_cached_live(rd, od_sport_id) if rd else None
#         if not snap:
#             snap = fetch_live_matches(sport_slug=sport) or []
#         yield _sse({"type": "snapshot", "matches": snap, "total": len(snap)})

#         if not rd:
#             # No Redis — just keep polling manually every 3 s
#             prev = {m["od_match_id"]: m for m in snap}
#             while True:
#                 time.sleep(3)
#                 try:
#                     fresh = fetch_live_matches(sport_slug=sport) or []
#                     events: list[dict] = []
#                     for m in fresh:
#                         mid = m["od_match_id"]
#                         old = prev.get(mid, {})
#                         for slug, outs in (m.get("markets") or {}).items():
#                             for ok, ov in outs.items():
#                                 po = (old.get("markets") or {}).get(slug, {}).get(ok)
#                                 if po is None or abs(ov - po) > 0.001:
#                                     events.append({
#                                         "type": "market_update", "match_id": mid,
#                                         "home_team": m.get("home_team"), "away_team": m.get("away_team"),
#                                         "match_time": m.get("match_time"), "score_home": m.get("score_home"),
#                                         "score_away": m.get("score_away"), "market_slug": slug,
#                                         "outcome_key": ok, "odd": ov, "prev_odd": po, "is_new": po is None,
#                                     })
#                     prev = {m["od_match_id"]: m for m in fresh}
#                     if events:
#                         yield _sse({"type": "batch_update", "sport_slug": sport, "events": events,
#                                     "total": len(fresh), "ts": time.time()})
#                     else:
#                         yield _sse({"type": "heartbeat", "ts": time.time()})
#                 except GeneratorExit:
#                     return
#                 except Exception as exc:
#                     yield _sse({"type": "error", "message": str(exc)})
#             return

#         # Redis pub/sub mode
#         channel = _LIVE_CHAN_KEY.format(sport_id=od_sport_id)
#         pubsub  = rd.pubsub()
#         pubsub.subscribe(channel)
#         try:
#             last_hb = time.time()
#             for msg in pubsub.listen():
#                 if msg["type"] != "message":
#                     continue
#                 try:
#                     payload = json.loads(msg["data"])
#                     # Re-filter events to this sport only
#                     if payload.get("sport_id") != od_sport_id:
#                         continue
#                     yield _sse(payload)
#                 except Exception:
#                     pass
#                 # Heartbeat every 20 s
#                 if time.time() - last_hb > 20:
#                     yield _sse({"type": "heartbeat", "ts": time.time()})
#                     last_hb = time.time()
#         except GeneratorExit:
#             pass
#         finally:
#             pubsub.unsubscribe(channel)
#             pubsub.close()

#     return Response(_gen(), mimetype="text/event-stream", headers={
#         "Cache-Control":     "no-cache",
#         "X-Accel-Buffering": "no",
#     })


# # ══════════════════════════════════════════════════════════════════════════════
# # MATCH DETAIL
# # ══════════════════════════════════════════════════════════════════════════════

# @bp.route("/event/<event_id>")
# def event_detail(event_id: str):
#     """
#     GET /api/od/event/<event_id>?od_sport_id=1
#     Returns full market list from /sportsbook/v1?resource=sportevent&id=X.
#     Cached 90 s (pre-match) or 30 s (live).
#     """
#     t0          = time.time()
#     od_sport_id = int(request.args.get("od_sport_id", 1))
#     is_live     = request.args.get("is_live", "0") in ("1", "true", "yes")
#     ttl         = 30 if is_live else 90

#     rd        = _redis()
#     cache_key = f"od:event:{event_id}:markets"

#     if rd:
#         try:
#             cached = rd.get(cache_key)
#             if cached:
#                 payload = json.loads(cached)
#                 payload["source"]     = "cache"
#                 payload["latency_ms"] = int((time.time() - t0) * 1000)
#                 return payload
#         except Exception:
#             pass

#     markets, meta = fetch_event_detail(event_id, od_sport_id)
#     if not markets and not meta:
#         return {"ok": False, "error": "No data from OdiBets API"}, 404

#     payload = {
#         "ok":           True,
#         "event_id":     event_id,
#         "markets":      markets,
#         "market_count": len(markets),
#         "meta":         meta,
#         "source":       "live",
#         "latency_ms":   int((time.time() - t0) * 1000),
#     }

#     if rd:
#         try:
#             rd.set(cache_key, json.dumps(payload, ensure_ascii=False), ex=ttl)
#         except Exception:
#             pass

#     return payload


# # ══════════════════════════════════════════════════════════════════════════════
# # CACHE / OPS
# # ══════════════════════════════════════════════════════════════════════════════

# @bp.route("/cache/bust/<sport>", methods=["POST"])
# def cache_bust(sport: str):
#     """POST /api/od/cache/bust/<sport> — delete upcoming cache."""
#     rd = _redis()
#     if not rd:
#         return {"ok": False, "error": "Redis unavailable"}, 503
#     try:
#         deleted = rd.delete(_UPC_DATA_KEY.format(sport_slug=sport))
#         return {"ok": True, "sport": sport, "keys_deleted": deleted}
#     except Exception as exc:
#         return {"ok": False, "error": str(exc)}, 500


# # ══════════════════════════════════════════════════════════════════════════════
# # METADATA
# # ══════════════════════════════════════════════════════════════════════════════

# @bp.route("/meta/sports")
# def meta_sports():
#     """GET /api/od/meta/sports"""
#     SPORT_NAMES: dict[str, tuple[str, str]] = {
#         "soccer":            ("Football",      "⚽"),
#         "basketball":        ("Basketball",    "🏀"),
#         "tennis":            ("Tennis",        "🎾"),
#         "cricket":           ("Cricket",       "🏏"),
#         "rugby":             ("Rugby",         "🏉"),
#         "ice-hockey":        ("Ice Hockey",    "🏒"),
#         "volleyball":        ("Volleyball",    "🏐"),
#         "handball":          ("Handball",      "🤾"),
#         "table-tennis":      ("Table Tennis",  "🏓"),
#         "esoccer":           ("eFootball",     "🎮"),
#         "mma":               ("MMA",           "🥋"),
#         "boxing":            ("Boxing",        "🥊"),
#         "darts":             ("Darts",         "🎯"),
#         "american-football": ("American Football", "🏈"),
#     }
#     sports = []
#     for slug, od_id in OD_SPORT_IDS.items():
#         name, emoji = SPORT_NAMES.get(slug, (slug.replace("-", " ").title(), "🏆"))
#         sports.append({"primary_slug": slug, "name": name, "emoji": emoji, "od_sport_id": od_id})
#     return {"ok": True, "sports": sports}


# @bp.route("/meta/markets/<sport>")
# def meta_markets(sport: str):
#     """GET /api/od/meta/markets/<sport>"""
#     od_sport_id = slug_to_od_sport_id(sport)
#     return {"ok": True, "markets": _market_metas(od_sport_id), "sport": sport}


# # ══════════════════════════════════════════════════════════════════════════════
# # STATUS
# # ══════════════════════════════════════════════════════════════════════════════

# @bp.route("/status")
# def status():
#     """GET /api/od/status — health check."""
#     rd       = _redis()
#     redis_ok = False
#     if rd:
#         try:
#             rd.ping()
#             redis_ok = True
#         except Exception:
#             pass

#     poller = get_live_poller()
#     return {
#         "ok":           True,
#         "source":       "odibets",
#         "redis":        redis_ok,
#         "poller_alive": poller is not None and poller.alive,
#     }