"""
app/views/odds_feed/sp_live_view.py
=====================================
Flask Blueprint — Sportpesa Live Odds.

All live data flows through TWO correct SP API endpoints (confirmed from browser traffic):

  GET /api/live/sports/{sportId}/events          → event list (id, competitors, state, externalId)
  GET /api/live/default/markets?sportId=N        → default market (1x2) for ALL events at once
  GET /api/live/events/{eventId}/details         → full market book for ONE event
  GET /api/live/sports                           → sport list with eventNumber

WebSocket delta (via Redis pub/sub, published by sp_live_harvester.py):
  sp:live:sport:{sportId}  →  market_update / event_update messages

CANONICAL SNAPSHOT BUILD FLOW (endpoint: GET /api/sp/live/snapshot-canonical/<sport_slug>)
─────────────────────────────────────────────────────────────────────────────────────────
1. fetch_live_events(sport_id)                    → get all live event stubs
2. fetch_live_default_markets(sport_id)           → batch: 1x2 odds for every event
3. fetch_event_details(event_id) per event        → full markets (DC, BTTS, O/U, etc.)
4. live_market_slug() + normalize_live_outcome()  → canonical slug + outcome keys
5. Build SpMatch[] identical in shape to pre-match matches

This avoids the broken sp_harvester._fetch_live_list("/api/live/games") call entirely.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests
from flask import Blueprint, Response, request, stream_with_context

from app.utils.customer_jwt_helpers import _err, _signed_response

log = logging.getLogger("sp_live")

bp_sp_live = Blueprint("sp_live", __name__, url_prefix="/api/sp/live")

# ── SP live slug → sport_id ───────────────────────────────────────────────────
_SLUG_TO_SPORT_ID: dict[str, int] = {
    "soccer": 1,       "football": 1,
    "basketball": 2,   "tennis": 4,
    "volleyball": 10,  "handball": 5,
    "rugby": 8,        "cricket": 9,
    "table-tennis": 13,
}
_SPORT_SLUG_MAP: dict[int, str] = {v: k for k, v in _SLUG_TO_SPORT_ID.items() if k not in ("football",)}


def _slug_to_sport_id(slug: str) -> int | None:
    return _SLUG_TO_SPORT_ID.get(slug.lower().replace("_", "-"))


# ── Lazy imports ──────────────────────────────────────────────────────────────

def _harvester():
    from app.workers import sp_live_harvester as h
    return h


def _get_redis():
    from app.workers.sp_live_harvester import _get_redis as gr
    return gr()


# ── Redis channel names ───────────────────────────────────────────────────────

CH_ALL   = "sp:live:all"
CH_SPORT = "sp:live:sport:{sport_id}"
CH_EVENT = "sp:live:event:{event_id}"

SSE_HEADERS = {
    "Content-Type":      "text/event-stream",
    "Cache-Control":     "no-cache",
    "X-Accel-Buffering": "no",
    "Connection":        "keep-alive",
}


# ═════════════════════════════════════════════════════════════════════════════
# CANONICAL LIVE MATCH BUILDER
# ═════════════════════════════════════════════════════════════════════════════

def _parse_live_markets(
    raw_markets: list[dict],
    sport_id:    int,
) -> dict[str, dict[str, float]]:
    """
    Convert a live markets list (from /api/live/events/{id}/details or
    /api/live/default/markets) to canonical {slug: {outcome_key: odd}}.

    Uses LIVE_MARKET_MAP type IDs (different from upcoming /api/games/markets).
    Selection names are full team names, not shortNames — handled by
    normalize_live_outcome().
    """
    from app.workers.sp_live_harvester import live_market_slug, normalize_live_outcome

    result: dict[str, dict[str, float]] = {}

    for mkt in raw_markets:
        if not isinstance(mkt, dict):
            continue
        mkt_type = mkt.get("id")
        if mkt_type is None:
            continue
        try:
            mkt_type = int(mkt_type)
        except (TypeError, ValueError):
            continue

        # Skip suspended/closed markets
        if mkt.get("status") == "Suspended":
            continue

        handicap = mkt.get("specialValue")
        slug     = live_market_slug(mkt_type, handicap, sport_id)
        sels     = mkt.get("selections") or []

        if not sels:
            continue

        outcomes: dict[str, float] = {}
        for idx, sel in enumerate(sels):
            if not isinstance(sel, dict):
                continue
            if sel.get("status") == "Suspended":
                continue
            try:
                odd = float(sel.get("odds") or "0")
            except (TypeError, ValueError):
                odd = 0.0
            if odd <= 1.0:
                continue

            out_key = normalize_live_outcome(slug, sel.get("name", ""), idx, sels)
            if odd > outcomes.get(out_key, 0.0):
                outcomes[out_key] = round(odd, 3)

        if outcomes:
            result[slug] = outcomes

    return result


def _build_sp_match(
    event:       dict,
    markets_raw: list[dict],
    sport_slug:  str,
    sport_id:    int,
) -> dict:
    """Build a canonical SpMatch dict from a live event + its markets."""
    comps = event.get("competitors") or []
    home  = comps[0].get("name", "") if len(comps) > 0 else ""
    away  = comps[1].get("name", "") if len(comps) > 1 else ""

    state   = event.get("state") or {}
    score   = state.get("matchScore") or {}
    tourn   = event.get("tournament") or {}
    country = event.get("country") or {}

    markets   = _parse_live_markets(markets_raw, sport_id)
    kick_utc  = event.get("kickoffTimeUTC")

    return {
        "sp_game_id":    event.get("id"),
        "betradar_id":   str(event.get("externalId") or ""),
        "home_team":     home,
        "away_team":     away,
        "start_time":    kick_utc,
        "competition":   tourn.get("name", ""),
        "country":       country.get("name", ""),
        "sport":         sport_slug,
        "sp_sport_id":   sport_id,
        "source":        "sportpesa_live",
        "status":        event.get("status", "Started"),
        "phase":         state.get("currentEventPhase", ""),
        "match_time":    state.get("matchTime", ""),
        "score_home":    score.get("home", ""),
        "score_away":    score.get("away", ""),
        "is_paused":     event.get("isPaused", False),
        "markets":       markets,
        "market_count":  len(markets),
        "harvested_at":  _now_ts(),
    }


def _fetch_canonical_live(sport_slug: str, sport_id: int) -> list[dict]:
    """
    Build canonical SpMatch[] for a live sport using correct SP live APIs:
      1. GET /api/live/sports/{sportId}/events     → event stubs
      2. GET /api/live/default/markets?sportId=N   → 1x2 for all events (fast)
      3. GET /api/live/events/{eventId}/details    → full markets per event (capped at 50)
    """
    h = _harvester()

    # ── 1. Event list ──────────────────────────────────────────────────────────
    events = h.fetch_live_events(sport_id, limit=100)
    if not events:
        log.info("snapshot-canonical %s: 0 events from API", sport_slug)
        return []

    log.info("snapshot-canonical %s: %d events", sport_slug, len(events))

    # ── 2. Default markets batch (1x2 only, but fast) ─────────────────────────
    default_data = h.fetch_live_default_markets(sport_id)
    # {eventId: [market_dict, ...]}
    default_by_event: dict[int, list[dict]] = {}
    for item in default_data:
        eid  = item.get("eventId")
        mkts = item.get("markets") or []
        if eid:
            default_by_event[int(eid)] = mkts

    # ── 3. Full market details per event ──────────────────────────────────────
    # Cap at 50 events to avoid rate limiting; remainder get default 1x2 only
    MAX_DETAIL = 50
    detail_by_event: dict[int, list[dict]] = {}
    for ev in events[:MAX_DETAIL]:
        event_id = ev.get("id")
        if not event_id:
            continue
        details = h.fetch_event_details(event_id)
        if details and isinstance(details, dict):
            mkts = details.get("markets") or []
            if mkts:
                detail_by_event[int(event_id)] = mkts
                # Update state/score from details (more accurate)
                if isinstance(details.get("event"), dict):
                    ev.update({
                        k: v for k, v in details["event"].items()
                        if k in ("state", "isPaused", "status", "externalId",
                                 "kickoffTimeUTC", "tournament", "country", "competitors")
                    })
        time.sleep(0.05)   # gentle rate-limit: 50 events × 50ms = 2.5s max

    # ── 4. Build SpMatch[] ────────────────────────────────────────────────────
    matches: list[dict] = []
    for ev in events:
        event_id  = ev.get("id")
        if not event_id:
            continue
        eid_int   = int(event_id)
        # Prefer full details, fall back to default 1x2
        mkts_raw  = detail_by_event.get(eid_int) or default_by_event.get(eid_int) or []
        match     = _build_sp_match(ev, mkts_raw, sport_slug, sport_id)
        if match["home_team"] or match["away_team"]:
            matches.append(match)

    log.info("snapshot-canonical %s: %d matches built (%d with full markets)",
             sport_slug, len(matches), len(detail_by_event))
    return matches


# ═════════════════════════════════════════════════════════════════════════════
# SSE HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _sse_keep_alive() -> str:
    return f": keep-alive {datetime.now(timezone.utc).strftime('%H:%M:%S')}\n\n"


def _stream_channel(channel: str, label: str = ""):
    r      = _get_redis()
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(channel)
    yield _sse({"type":"connected","channel":channel,"label":label,"ts":datetime.now(timezone.utc).isoformat()})
    last_ka = time.monotonic()
    try:
        while True:
            msg = pubsub.get_message(timeout=0.5)
            if msg and msg["type"] == "message":
                yield _sse(json.loads(msg["data"]))
            if time.monotonic() - last_ka > 15:
                yield _sse_keep_alive()
                last_ka = time.monotonic()
    except GeneratorExit:
        pass
    finally:
        try:
            pubsub.unsubscribe(channel)
            pubsub.close()
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════════════════
# SSE ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@bp_sp_live.route("/stream")
def stream_all():
    @stream_with_context
    def g(): yield from _stream_channel(CH_ALL, "all")
    return Response(g(), headers=SSE_HEADERS)


@bp_sp_live.route("/stream/sport/<int:sport_id>")
def stream_sport(sport_id: int):
    @stream_with_context
    def g(): yield from _stream_channel(CH_SPORT.format(sport_id=sport_id), f"sport_{sport_id}")
    return Response(g(), headers=SSE_HEADERS)


@bp_sp_live.route("/stream/event/<int:event_id>")
def stream_event(event_id: int):
    @stream_with_context
    def g(): yield from _stream_channel(CH_EVENT.format(event_id=event_id), f"event_{event_id}")
    return Response(g(), headers=SSE_HEADERS)


# ═════════════════════════════════════════════════════════════════════════════
# REST — SPORTS / EVENTS / SNAPSHOTS
# ═════════════════════════════════════════════════════════════════════════════

@bp_sp_live.route("/sports")
def live_sports():
    t0 = time.perf_counter()
    r  = _get_redis()
    cached = r.get("sp:live:sports")
    if cached:
        sports, from_cache = json.loads(cached), True
    else:
        sports, from_cache = _harvester().fetch_live_sports(), False
    return _signed_response({
        "ok": True, "source": "sportpesa_live", "from_cache": from_cache,
        "sports": sports, "count": len(sports),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/events/<int:sport_id>")
def live_events(sport_id: int):
    t0     = time.perf_counter()
    limit  = min(int(request.args.get("limit", 50) or 50), 200)
    offset = int(request.args.get("offset", 0) or 0)
    events = _harvester().fetch_live_events(sport_id, limit=limit, offset=offset)
    return _signed_response({
        "ok": True, "sport_id": sport_id, "events": events, "count": len(events),
        "latency_ms": int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/snapshot/<int:sport_id>")
def live_snapshot(sport_id: int):
    """Raw SP-format snapshot (events + default 1x2 markets) from Redis."""
    t0  = time.perf_counter()
    r   = _get_redis()
    raw = r.get(f"sp:live:snapshot:{sport_id}")
    if not raw:
        return _err(f"No snapshot for sport_id={sport_id}. POST /api/sp/live/test/snapshot first.", 404)
    data = json.loads(raw)
    data["ok"]         = True
    data["latency_ms"] = int((time.perf_counter() - t0) * 1000)
    return _signed_response(data)


@bp_sp_live.route("/snapshot-canonical/<sport_slug>")
def live_snapshot_canonical(sport_slug: str):
    """
    GET /api/sp/live/snapshot-canonical/<sport_slug>

    Returns live matches in canonical SpMatch[] format (same shape as pre-match).
    Uses the correct SP live API endpoints:
      • /api/live/sports/{sportId}/events          — event stubs
      • /api/live/default/markets?sportId=N        — 1x2 for all events
      • /api/live/events/{eventId}/details         — full markets per event

    Cache key: sp:live:canonical:{sport_slug}  TTL 30s (short because live data changes fast)
    """
    t0 = time.perf_counter()

    sport_id = _slug_to_sport_id(sport_slug)
    if not sport_id:
        return _err(f"Unknown live sport: {sport_slug}", 404)

    # ── Cache check ───────────────────────────────────────────────────────────
    cache_key = f"sp:live:canonical:{sport_slug}"
    cached    = _cache_get(cache_key)
    if cached and cached.get("matches"):
        return _signed_response({
            "ok":          True,
            "source":      "sportpesa_live_canonical",
            "sport":       sport_slug,
            "sport_id":    sport_id,
            "from_cache":  True,
            "matches":     cached["matches"],
            "total":       len(cached["matches"]),
            "harvested_at": cached.get("harvested_at"),
            "latency_ms":  int((time.perf_counter() - t0) * 1000),
        })

    # ── Fresh fetch ───────────────────────────────────────────────────────────
    try:
        matches = _fetch_canonical_live(sport_slug, sport_id)
    except Exception as exc:
        log.exception("snapshot-canonical failed for %s", sport_slug)
        return _err(f"Live fetch failed: {exc}", 503)

    harvested_at = _now_ts()
    _cache_set(cache_key, {"matches": matches, "harvested_at": harvested_at}, ttl=30)

    # Also write to sp:live:{sport_slug} for the Celery harvester / upcoming fallback
    _cache_set(f"sp:live:{sport_slug}", {
        "source": "sportpesa", "sport": sport_slug, "mode": "live",
        "match_count": len(matches), "harvested_at": harvested_at,
        "latency_ms": int((time.perf_counter() - t0) * 1000),
        "matches": matches,
    }, ttl=60)

    return _signed_response({
        "ok":          True,
        "source":      "sportpesa_live_canonical",
        "sport":       sport_slug,
        "sport_id":    sport_id,
        "from_cache":  False,
        "matches":     matches,
        "total":       len(matches),
        "harvested_at": harvested_at,
        "latency_ms":  int((time.perf_counter() - t0) * 1000),
    })


@bp_sp_live.route("/stream-matches/<sport_slug>")
def live_stream_matches(sport_slug: str):
    """
    GET /api/sp/live/stream-matches/<sport_slug>

    SSE stream of live matches — identical event format to /api/sp/stream/upcoming/:
      {type:"start",  sport, mode:"live", estimated_max}
      {type:"match",  index, match}         ← one SpMatch per live event
      {type:"done",   total, latency_ms, harvested_at}
      {type:"error",  message}

    Each match is in canonical format (same as snapshot-canonical and upcoming).
    Writes result to sp:live:canonical:{sport_slug} cache on completion.
    """
    sport_id = _slug_to_sport_id(sport_slug)
    if not sport_id:
        def _err_gen():
            yield _sse({"type": "error", "message": f"Unknown live sport: {sport_slug}"})
        return Response(stream_with_context(_err_gen)(), headers=SSE_HEADERS)

    @stream_with_context
    def generate():
        t0          = time.perf_counter()
        all_matches = []

        try:
            # Pre-fetch event count for estimated_max
            h = _harvester()
            events_preview = h.fetch_live_events(sport_id, limit=5)
            estimated = max(len(events_preview) * 5, 20) if events_preview else 50
            estimated = min(estimated, 200)

            yield _sse({"type": "start", "sport": sport_slug, "mode": "live",
                        "sport_id": sport_id, "estimated_max": estimated})

            idx = 0
            for match in _fetch_canonical_live(sport_slug, sport_id):
                idx += 1
                all_matches.append(match)
                yield _sse({"type": "match", "index": idx, "match": match})

            harvested_at = _now_ts()
            latency_ms   = int((time.perf_counter() - t0) * 1000)

            # Write to both canonical and general live cache
            _cache_set(f"sp:live:canonical:{sport_slug}",
                       {"matches": all_matches, "harvested_at": harvested_at}, ttl=30)
            _cache_set(f"sp:live:{sport_slug}", {
                "source": "sportpesa", "sport": sport_slug, "mode": "live",
                "match_count": len(all_matches), "harvested_at": harvested_at,
                "latency_ms": latency_ms, "matches": all_matches,
            }, ttl=60)

            yield _sse({"type": "done", "total": len(all_matches),
                        "latency_ms": latency_ms, "harvested_at": harvested_at})

        except Exception as exc:
            log.exception("live stream-matches failed for %s", sport_slug)
            yield _sse({"type": "error", "message": str(exc)})

    return Response(generate(), headers=SSE_HEADERS)


@bp_sp_live.route("/markets/<int:event_id>")
def live_markets(event_id: int):
    """All current market odds for one event from Redis."""
    t0 = time.perf_counter()
    r  = _get_redis()
    prefix = f"sp:live:odds:{event_id}:"
    keys   = r.keys(f"{prefix}*")
    if not keys:
        return _signed_response({"ok":True,"event_id":event_id,"markets":[],"count":0,
                                  "latency_ms":int((time.perf_counter()-t0)*1000)})
    pipe   = r.pipeline()
    for k in keys: pipe.get(k)
    values = pipe.execute()
    markets = []
    for k, v in zip(keys, values):
        if v:
            markets.append({"market_id": int(k.split(":")[-1]), "odds": json.loads(v)})
    return _signed_response({"ok":True,"event_id":event_id,"markets":markets,"count":len(markets),
                              "latency_ms":int((time.perf_counter()-t0)*1000)})


@bp_sp_live.route("/odds-history/<int:event_id>/<int:market_id>")
def odds_history(event_id: int, market_id: int):
    t0    = time.perf_counter()
    limit = min(int(request.args.get("limit", 20) or 20), 50)
    ticks = _harvester().get_odds_history(event_id, market_id, limit=limit)
    return _signed_response({"ok":True,"event_id":event_id,"market_id":market_id,
                              "ticks":ticks,"count":len(ticks),
                              "latency_ms":int((time.perf_counter()-t0)*1000)})


@bp_sp_live.route("/state/<int:event_id>")
def event_state(event_id: int):
    t0  = time.perf_counter()
    r   = _get_redis()
    raw = r.get(f"sp:live:state:{event_id}")
    if not raw:
        return _err(f"No state cached for event {event_id}", 404)
    return _signed_response({"ok":True,"event_id":event_id,"state":json.loads(raw),
                              "latency_ms":int((time.perf_counter()-t0)*1000)})


@bp_sp_live.route("/status")
def live_status():
    t0 = time.perf_counter()
    h  = _harvester()
    r  = _get_redis()
    channels = {}
    try:
        ps_info = r.execute_command("PUBSUB","NUMSUB",CH_ALL,"sp:live:sport:1","sp:live:sport:2")
        for i in range(0, len(ps_info), 2): channels[ps_info[i]] = ps_info[i+1]
    except Exception: pass
    return _signed_response({
        "ok":True, "harvester_alive":h.harvester_alive(),
        "channels":channels, "redis_connected":bool(r.ping()),
        "latency_ms":int((time.perf_counter()-t0)*1000),
    })


# ═════════════════════════════════════════════════════════════════════════════
# SPORTRADAR PROXY
# ═════════════════════════════════════════════════════════════════════════════

_SR_BASE           = "https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo"
_SR_ORIGIN         = "https://www.ke.sportpesa.com"
_SR_TOKEN_FALLBACK = (
    "exp=1774614407~acl=/*"
    "~data=eyJvIjoiaHR0cHM6Ly93d3cua2Uuc3BvcnRwZXNhLmNvbSIsImEiOiJmODYx"
    "N2E4OTZkMzU1MWJhNTBkNTFmMDE0OWQ0YjZkZCIsImFjdCI6Im9yaWdpbmNoZWNrIiwi"
    "b3NyYyI6Im9yaWdpbiJ9"
    "~hmac=0d8d06021a71f8f1fab2ae28608742ad1ecb093eb38b5858b905a10115948b1e"
)

_SR_STAT_KEYS = [
    ("124","Corners"),("40","Yellow Cards"),("45","Yellow/Red Cards"),("50","Red Cards"),
    ("1030","Ball Safe"),("1126","Attacks"),("1029","Dangerous Attacks"),
    ("ballsafepercentage","Possession %"),("attackpercentage","Attack %"),
    ("dangerousattackpercentage","Danger Att %"),("60","Substitutions"),("158","Injuries"),
]


def _sr_token() -> str:
    return os.getenv("SPORTRADAR_TOKEN", _SR_TOKEN_FALLBACK)


def _sr_get(endpoint: str, timeout: int = 8) -> dict | None:
    url = f"{_SR_BASE}/{endpoint}?T={_sr_token()}"
    try:
        r = requests.get(url, headers={
            "Origin": _SR_ORIGIN, "Referer": _SR_ORIGIN + "/",
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36",
        }, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("SR %s → %s", endpoint, exc)
        return None


@bp_sp_live.route("/sportradar/match-details/<int:external_id>")
def sr_match_details(external_id: int):
    t0   = time.perf_counter()
    data = _sr_get(f"match_detailsextended/{external_id}")
    if not data:
        return _err(f"Sportradar stats unavailable for externalId={external_id}. Rotate SPORTRADAR_TOKEN.", 503)
    doc    = (data.get("doc") or [{}])[0]
    inner  = doc.get("data", {})
    values = inner.get("values", {})
    teams  = inner.get("teams", {})
    stats_rows = []
    for key, label in _SR_STAT_KEYS:
        entry = values.get(key)
        if not entry: continue
        val = entry.get("value")
        if not isinstance(val, dict): continue
        h_v = val.get("home",""); a_v = val.get("away","")
        if h_v == "" and a_v == "": continue
        stats_rows.append({"name":label,"home":h_v or 0,"away":a_v or 0})
    return _signed_response({
        "ok":True,"external_id":external_id,
        "stats":{"home":teams.get("home","Home"),"away":teams.get("away","Away"),"stats":stats_rows},
        "latency_ms":int((time.perf_counter()-t0)*1000),
    })


@bp_sp_live.route("/sportradar/match-info/<int:external_id>")
def sr_match_info(external_id: int):
    t0   = time.perf_counter()
    data = _sr_get(f"match_info/{external_id}")
    if not data:
        return _err(f"Sportradar match_info unavailable for externalId={external_id}.", 503)
    doc   = (data.get("doc") or [{}])[0]
    inner = doc.get("data", {})
    match = inner.get("match", {})
    venue = match.get("venue", {})
    ref   = match.get("referee", {})
    teams = inner.get("teams", {})
    tourn = match.get("tournament", {})
    ref_name = ref.get("name",""); ref_nat = ref.get("nationality","")
    ref_str  = f"{ref_name} ({ref_nat})".strip(" ()") if ref_name else ""
    return _signed_response({
        "ok":True,"external_id":external_id,
        "match":{
            "id":match.get("id"),"tournament":tourn.get("name"),
            "venue":venue.get("name"),"venue_city":venue.get("cityName"),
            "referee":ref_str,
            "home":(teams.get("home") or {}).get("name"),
            "away":(teams.get("away") or {}).get("name"),
        },
        "raw":inner,
        "latency_ms":int((time.perf_counter()-t0)*1000),
    })


# ═════════════════════════════════════════════════════════════════════════════
# TEST / ADMIN
# ═════════════════════════════════════════════════════════════════════════════

@bp_sp_live.route("/test/snapshot", methods=["POST"])
def test_snapshot():
    t0 = time.perf_counter()
    try:
        result  = _harvester().snapshot_all_sports()
        summary = {sport_id: len(events) for sport_id, events in result.items()}
        return _signed_response({"ok":True,"sports_done":len(result),"event_counts":summary,
                                  "latency_ms":int((time.perf_counter()-t0)*1000)})
    except Exception as exc:
        return _err(f"snapshot failed: {exc}", 500)


@bp_sp_live.route("/test/start-harvester", methods=["POST"])
def test_start_harvester():
    t0     = time.perf_counter()
    thread = _harvester().start_harvester_thread()
    return _signed_response({"ok":True,"alive":thread.is_alive(),"thread":thread.name,
                              "latency_ms":int((time.perf_counter()-t0)*1000)})


@bp_sp_live.route("/test/stop-harvester", methods=["POST"])
def test_stop_harvester():
    t0 = time.perf_counter()
    _harvester().stop_harvester()
    return _signed_response({"ok":True,"alive":_harvester().harvester_alive(),
                              "latency_ms":int((time.perf_counter()-t0)*1000)})


@bp_sp_live.route("/test/publish", methods=["POST"])
def test_publish():
    t0   = time.perf_counter()
    body = request.get_json(silent=True) or {}
    channel = body.get("channel", CH_ALL)
    payload = body.get("payload", {"type":"test","message":"hello","ts":_now_ts()})
    r = _get_redis()
    n = r.publish(channel, json.dumps(payload))
    return _signed_response({"ok":True,"channel":channel,"subscribers":n,
                              "latency_ms":int((time.perf_counter()-t0)*1000)})


@bp_sp_live.route("/test/fetch-markets")
def test_fetch_markets():
    """
    Test the live default markets endpoint directly.
    Query params: sport_id (default 1)
    """
    t0       = time.perf_counter()
    sport_id = int(request.args.get("sport_id", 1))
    items    = _harvester().fetch_live_default_markets(sport_id)
    return _signed_response({"ok":True,"sport_id":sport_id,"items":items,
                              "count":len(items),"latency_ms":int((time.perf_counter()-t0)*1000)})


@bp_sp_live.route("/test/canonical/<sport_slug>")
def test_canonical(sport_slug: str):
    """
    Debug endpoint: show what snapshot-canonical returns without caching.
    Also shows which market slugs were resolved per event.
    """
    t0       = time.perf_counter()
    sport_id = _slug_to_sport_id(sport_slug)
    if not sport_id:
        return _err(f"Unknown sport: {sport_slug}", 404)

    matches = _fetch_canonical_live(sport_slug, sport_id)

    # Summary for debugging
    summary = []
    for m in matches:
        summary.append({
            "sp_game_id":   m["sp_game_id"],
            "home":         m["home_team"],
            "away":         m["away_team"],
            "phase":        m.get("phase"),
            "score":        f"{m.get('score_home','-')}:{m.get('score_away','-')}",
            "market_count": m["market_count"],
            "slugs":        list(m["markets"].keys()),
        })

    return _signed_response({
        "ok":True,"sport_slug":sport_slug,"sport_id":sport_id,
        "total":len(matches),"summary":summary,
        "latency_ms":int((time.perf_counter()-t0)*1000),
    })


# ═════════════════════════════════════════════════════════════════════════════
# CACHE HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _cache_get(key: str):
    try:
        from app.workers.celery_tasks import cache_get
        return cache_get(key)
    except Exception:
        return None


def _cache_set(key: str, data: Any, ttl: int = 30):
    try:
        from app.workers.celery_tasks import cache_set
        cache_set(key, data, ttl=ttl)
    except Exception:
        pass


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")