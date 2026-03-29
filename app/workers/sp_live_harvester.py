"""
app/workers/sp_live_harvester.py  (v6)
=======================================
Fixes applied
─────────────
FIX-1  Correct live market endpoint
  Old code used /api/live/markets (404s) and /api/live/events/{id}/details
  (404s for many events). The CONFIRMED working endpoint from SP's own
  frontend traffic is:
    GET /api/live/event/markets?eventId=1073579,1051809,...&type=194&sportId=1
  This returns market data for up to 15 events per request.

FIX-2  Positional outcome fallback for team-name selections
  The live markets API returns selections with full team names:
    {"name": "Union Omaha SC", "odds": "1.14"}   → should be "1"
    {"name": "draw",           "odds": "5.70"}   → "X"  (already worked)
    {"name": "Corpus Christi FC", "odds": "21.00"} → should be "2"
  Added _parse_live_selections() which uses _POS_OUTCOME positional fallback
  (same logic as _normalise_outcome_key used by the WS harvester).

FIX-3  fetch_live_stream() — live match generator
  New public generator that streams one fully-normalised match dict per yield,
  identical in shape to sp_harvester.fetch_upcoming_stream(). Each match
  includes:
    • markets: {slug: {outcome: float}}  — correctly normalised
    • state fields: phase, match_time, score_home, score_away, is_paused,
                    clock_running, kickoffTimeUTC, externalId

FIX-4  fetch_live() — blocking wrapper around fetch_live_stream()
  Drop-in replacement callable by sp_live_view.py snapshot-canonical and
  stream-matches endpoints.

FIX-5  WS harvester extended subscriptions
  The WS now subscribes to BOTH type 194 (1x2) AND type 105 (O/U 2.5) per
  event, giving immediate live odds for both the primary result market and the
  most popular totals market. Additional market types still arrive via the HTTP
  batch fetch which runs at the 60-second refresh.

All existing history recording (snapshot, state, Sportradar polling) and the
WS heartbeat/reconnect logic are preserved unchanged from v5.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Generator

import requests

log = logging.getLogger("sp_live_harvester")

# =============================================================================
# Redis
# =============================================================================

_redis_lock   = threading.Lock()
_redis_client = None


def _get_redis():
    global _redis_client
    if _redis_client is not None:
        try:
            _redis_client.ping()
            return _redis_client
        except Exception:
            _redis_client = None
    with _redis_lock:
        if _redis_client is not None:
            return _redis_client
        import redis as _r
        url  = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
        _redis_client = _r.Redis.from_url(
            f"{base}/1",
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        return _redis_client


# =============================================================================
# Channel / pub-sub helpers
# =============================================================================

CH_ALL   = "sp:live:all"
CH_SPORT = "sp:live:sport:{sport_id}"
CH_EVENT = "sp:live:event:{event_id}"


def _ch_sport(sport_id: int) -> str:
    return CH_SPORT.format(sport_id=sport_id)


def _ch_event(event_id: int) -> str:
    return CH_EVENT.format(event_id=event_id)


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _publish_live_update(r, sport_id: int, event_id: int, payload: dict) -> None:
    raw = json.dumps(payload, ensure_ascii=False)
    try:
        pipe = r.pipeline()
        pipe.publish(CH_ALL, raw)
        pipe.publish(_ch_sport(sport_id), raw)
        pipe.publish(_ch_event(event_id), raw)
        pipe.execute()
    except Exception as exc:
        log.warning("publish_live_update failed: %s", exc)


# =============================================================================
# Live snapshot history (from v5 — unchanged)
# =============================================================================

_SNAP_MAX  = 200
_STATE_MAX = 100
_SNAP_TTL  = 86_400
_STATE_TTL = 86_400


def _record_market_snapshot(r, event_id: int, payload: dict) -> None:
    date = _today()
    key  = f"sp:live:snap:{event_id}:{date}"
    tick = {
        "ts":      _now_ts(),
        "market":  payload.get("market_slug", ""),
        "type":    payload.get("market_type"),
        "changed": payload.get("changed_selections", []),
        "sels": [
            {"id": s["id"], "key": s.get("outcome_key", s.get("name", "")),
             "odds": s["odds"]}
            for s in payload.get("normalised_selections", [])
        ],
    }
    try:
        pipe = r.pipeline()
        pipe.lpush(key, json.dumps(tick, ensure_ascii=False))
        pipe.ltrim(key, 0, _SNAP_MAX - 1)
        pipe.expire(key, _SNAP_TTL)
        pipe.execute()
    except Exception as exc:
        log.debug("[snap] market record failed event=%d: %s", event_id, exc)
    _try_persist_snapshot(event_id, payload)


def _record_state_snapshot(r, event_id: int, payload: dict) -> None:
    date = _today()
    key  = f"sp:live:state_hist:{event_id}:{date}"
    tick = {
        "ts":         _now_ts(),
        "phase":      payload.get("phase",         ""),
        "match_time": payload.get("match_time",    ""),
        "running":    payload.get("clock_running", False),
        "score_h":    payload.get("score_home",    "0"),
        "score_a":    payload.get("score_away",    "0"),
        "is_paused":  payload.get("is_paused",     False),
        "source":     payload.get("source",        "sportpesa_ws"),
        "sr_period":  payload.get("sr_period"),
        "sr_ptime":   payload.get("sr_ptime"),
    }
    try:
        pipe = r.pipeline()
        pipe.lpush(key, json.dumps(tick, ensure_ascii=False))
        pipe.ltrim(key, 0, _STATE_MAX - 1)
        pipe.expire(key, _STATE_TTL)
        pipe.execute()
    except Exception as exc:
        log.debug("[snap] state record failed event=%d: %s", event_id, exc)
    _try_persist_state(event_id, tick)


def _try_persist_snapshot(event_id: int, payload: dict) -> None:
    try:
        from app.extensions import db
        from app.models.live_snapshot import LiveOddsSnapshot
        db.session.add(LiveOddsSnapshot(
            event_id    = event_id,
            market_slug = payload.get("market_slug", ""),
            market_type = payload.get("market_type"),
            selections  = [
                {"id": s["id"], "key": s.get("outcome_key", s.get("name", "")),
                 "odds": s["odds"]}
                for s in payload.get("normalised_selections", [])
            ],
            changed = payload.get("changed_selections", []),
        ))
        db.session.commit()
    except ImportError:
        pass
    except Exception as exc:
        log.debug("[snap:db] market persist event=%d: %s", event_id, exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass


def _try_persist_state(event_id: int, tick: dict) -> None:
    try:
        from app.extensions import db
        from app.models.live_snapshot import LiveEventState
        db.session.add(LiveEventState(
            event_id   = event_id,
            phase      = tick.get("phase", ""),
            match_time = tick.get("match_time", ""),
            score_home = tick.get("score_h", "0"),
            score_away = tick.get("score_a", "0"),
            is_paused  = tick.get("is_paused", False),
            source     = tick.get("source", ""),
        ))
        db.session.commit()
    except ImportError:
        pass
    except Exception as exc:
        log.debug("[snap:db] state persist event=%d: %s", event_id, exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass


def get_market_snapshot_history(
    event_id: int, date: str | None = None, limit: int = 100,
) -> list[dict]:
    r   = _get_redis()
    key = f"sp:live:snap:{event_id}:{date or _today()}"
    try:
        return [json.loads(i) for i in r.lrange(key, 0, limit - 1)]
    except Exception as exc:
        log.warning("get_market_snapshot_history %d: %s", event_id, exc)
        return []


def get_state_snapshot_history(
    event_id: int, date: str | None = None, limit: int = 100,
) -> list[dict]:
    r   = _get_redis()
    key = f"sp:live:state_hist:{event_id}:{date or _today()}"
    try:
        return [json.loads(i) for i in r.lrange(key, 0, limit - 1)]
    except Exception as exc:
        log.warning("get_state_snapshot_history %d: %s", event_id, exc)
        return []


# =============================================================================
# SP HTTP base
# =============================================================================

_SP_BASE = "https://www.ke.sportpesa.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "X-App-Timezone":   "Africa/Nairobi",
    "Origin":           _SP_BASE,
    "Referer":          _SP_BASE + "/en/live/",
}

_LIVE_SPORT_IDS = [1, 2, 4, 5, 6, 8, 9, 10, 13, 21, 23]


def _http_get(path: str, params: dict | None = None, timeout: int = 15) -> Any:
    url = f"{_SP_BASE}{path}"
    try:
        r = requests.get(url, headers=_HEADERS, params=params,
                         timeout=timeout, allow_redirects=True)
        if not r.ok:
            log.warning("HTTP %d → %s", r.status_code, url)
            return None
        return r.json()
    except Exception as exc:
        log.warning("HTTP error %s: %s", url, exc)
        return None


# =============================================================================
# Sport slug → sport ID
# =============================================================================

_SLUG_TO_SPORT_ID: dict[str, int] = {
    "soccer": 1, "football": 1, "esoccer": 126, "efootball": 126,
    "basketball": 2, "tennis": 5, "ice-hockey": 4, "volleyball": 23,
    "cricket": 21, "rugby": 12, "boxing": 10, "handball": 6,
    "table-tennis": 16, "mma": 117, "darts": 49,
    "american-football": 15, "baseball": 3,
}

# =============================================================================
# FIX-2: Market type → positional outcome keys
# Mirrors _POS_OUTCOME + _normalise_outcome_key from WS harvester.
# Used by _parse_live_selections() to convert team names → "1"/"X"/"2".
# =============================================================================

_POS_OUTCOME: dict[int, list[str]] = {
    194:  ["1", "X", "2"],       # 1x2
    149:  ["1", "2"],             # match winner
    147:  ["1X", "X2", "12"],    # double chance
    138:  ["yes", "no"],          # btts
    140:  ["yes", "no"],          # 1H btts
    145:  ["odd", "even"],        # odd/even
    166:  ["1", "2"],             # draw no bet
    135:  ["1", "X", "2"],       # 1H 1x2
    155:  ["1st", "2nd", "equal"],
    129:  ["none", "1", "2"],     # first team to score
}

_SPORT_TOTAL_SLUG: dict[int, str] = {
    1: "over_under_goals", 5: "over_under_goals", 126: "over_under_goals",
    2: "total_points",     8: "total_points",
    4: "total_games",     13: "total_games",
    10: "total_sets",      9: "total_runs",
    23: "total_sets",      6: "over_under_goals",
    16: "total_games",    21: "total_runs",
}

_TYPE_TO_SLUG: dict[int, str] = {
    194: "1x2",          149: "match_winner",     147: "double_chance",
    138: "btts",         140: "first_half_btts",   145: "odd_even",
    166: "draw_no_bet",  151: "european_handicap",  184: "asian_handicap",
    183: "correct_score", 154: "exact_goals",      135: "first_half_1x2",
    129: "first_team_to_score",
}

# Direct shortname → canonical key (fastest path)
_DIRECT_KEYS: dict[str, str] = {
    "1": "1", "x": "X", "2": "2", "draw": "X",
    "over": "over", "under": "under",
    "yes": "yes", "no": "no",
    "odd": "odd", "even": "even",
    "1x": "1X", "x2": "X2", "12": "12",
    "home": "1", "away": "2",
    "ov": "over", "un": "under",
    "gg": "yes", "ng": "no",
    "eql": "equal", "none": "none",
    "1st": "1st", "2nd": "2nd",
    "p1": "1", "p2": "2",
}


def _market_slug_from_type(market_type: int, sport_id: int, handicap: str | None) -> str:
    """Convert SP market type ID + handicap → canonical slug."""
    if market_type == 105:
        base = _SPORT_TOTAL_SLUG.get(sport_id, "over_under")
    else:
        base = _TYPE_TO_SLUG.get(market_type, f"market_{market_type}")
    if handicap and handicap not in ("0.00", "0", "", None):
        try:
            fv   = float(handicap)
            line = str(int(fv)) if fv == int(fv) else str(fv)
            return f"{base}_{line}"
        except (ValueError, TypeError):
            pass
    return base


def _parse_live_selections(
    sels:        list[dict],
    market_type: int,
    sport_id:    int,
    handicap:    str,
) -> dict[str, float]:
    """
    FIX-2: Convert live event selections → {outcome_key: price}.

    SP returns team names as selection names in the live markets API.
    Resolution order:
      1. Direct shortName lookup (_DIRECT_KEYS)
      2. Over/Under prefix
      3. Correct score "2:1" / HT-FT "1/2" (short tokens only)
      4. Positional fallback from _POS_OUTCOME (handles team names)
      5. O/U type 105 positional fallback (over/under by position)
    """
    pos_map = _POS_OUTCOME.get(market_type)
    result: dict[str, float] = {}

    for idx, sel in enumerate(sels):
        name  = str(sel.get("name") or sel.get("shortName") or "")
        short = str(sel.get("shortName") or "")
        try:
            price = float(sel.get("odds") or sel.get("price") or 0)
        except (TypeError, ValueError):
            price = 0.0
        if price <= 1.0:
            continue

        # 1. Direct shortName
        key = _DIRECT_KEYS.get(short.strip().lower())
        if not key:
            key = _DIRECT_KEYS.get(name.strip().lower())

        # 2. Over/Under prefix
        if not key:
            nl = name.strip().lower()
            if nl.startswith("over"):
                key = "over"
            elif nl.startswith("under"):
                key = "under"

        # 3. Correct score "2:1" or HT-FT "1/2" (≤5 chars)
        if not key:
            stripped = name.strip()
            if ":" in stripped and len(stripped) <= 5:
                key = stripped
            elif "/" in stripped and len(stripped) <= 5:
                key = stripped

        # 4. Positional fallback (covers team names like "Union Omaha SC")
        if not key and pos_map and idx < len(pos_map):
            key = pos_map[idx]

        # 5. O/U type 105 positional fallback
        if not key and market_type == 105:
            key = "over" if idx == 0 else "under"

        # 6. Generic sanitise
        if not key:
            key = name.lower().replace(" ", "_")[:14] or f"sel_{idx}"

        if price > result.get(key, 0.0):
            result[key] = round(price, 3)

    return result


# =============================================================================
# FIX-1: Correct live market endpoint
# =============================================================================

_MARKET_BATCH_SIZE = 12   # SP accepts up to ~15 event IDs per request


def fetch_live_markets_for_events(
    event_ids: list[int | str],
    sport_id:  int,
    market_type: int = 194,
) -> dict[str, list[dict]]:
    """
    FIX-1: Use the CONFIRMED working endpoint:
      GET /api/live/event/markets?eventId=...&type=194&sportId=1

    Returns {str(event_id): [market_dict, ...]} for fast lookup.
    Fetches in batches of _MARKET_BATCH_SIZE to stay within SP limits.
    """
    if not event_ids:
        return {}

    result: dict[str, list[dict]] = {}

    for i in range(0, len(event_ids), _MARKET_BATCH_SIZE):
        batch = event_ids[i: i + _MARKET_BATCH_SIZE]
        ids_str = ",".join(str(e) for e in batch)

        raw = _http_get(
            "/api/live/event/markets",
            params={"eventId": ids_str, "type": str(market_type), "sportId": str(sport_id)},
            timeout=12,
        )

        if not raw:
            continue

        # Response shape: {"events": [{id, markets: [...]}]} or list directly
        events_list = (
            raw if isinstance(raw, list)
            else raw.get("events") or raw.get("markets") or []
        )

        for ev_entry in events_list:
            if not isinstance(ev_entry, dict):
                continue
            ev_id   = str(ev_entry.get("id") or ev_entry.get("eventId") or "")
            markets = ev_entry.get("markets") or []
            if ev_id and isinstance(markets, list):
                result[ev_id] = markets

    return result


def fetch_all_market_types_for_events(
    event_ids: list[int | str],
    sport_id:  int,
) -> dict[str, dict[str, dict[str, float]]]:
    """
    Fetch multiple market types for a batch of events.
    Returns {event_id: {market_slug: {outcome: price}}}.

    Market types fetched: 194 (1x2), 105 (O/U 2.5), 147 (DC), 138 (BTTS),
                          145 (Odd/Even), 166 (DNB), 135 (1H 1x2).
    """
    if not event_ids:
        return {}

    # Map event_id → markets dict
    all_markets: dict[str, dict[str, dict[str, float]]] = {
        str(eid): {} for eid in event_ids
    }

    # Market types to fetch + representative handicap/line for O/U
    _TYPES_TO_FETCH = [194, 105, 147, 138, 145]

    for mkt_type in _TYPES_TO_FETCH:
        batch_result = fetch_live_markets_for_events(event_ids, sport_id, mkt_type)
        for ev_id_str, markets in batch_result.items():
            if ev_id_str not in all_markets:
                all_markets[ev_id_str] = {}

            for mkt in markets:
                if not isinstance(mkt, dict):
                    continue
                mid      = mkt.get("id") or mkt.get("typeId") or mkt_type
                spec_val = str(mkt.get("specialValue") or mkt.get("specValue") or "0.00")
                slug     = _market_slug_from_type(int(mid), sport_id, spec_val)
                sels     = mkt.get("selections") or []
                parsed   = _parse_live_selections(sels, int(mid), sport_id, spec_val)
                if parsed:
                    # Merge: keep best price per outcome
                    existing = all_markets[ev_id_str].get(slug, {})
                    for outcome, price in parsed.items():
                        if price > existing.get(outcome, 0.0):
                            existing[outcome] = price
                    all_markets[ev_id_str][slug] = existing

    return all_markets


# =============================================================================
# Event list fetcher (unchanged from v5)
# =============================================================================

def fetch_live_events(sport_id: int, limit: int = 100, offset: int = 0) -> list[dict]:
    """Fetch live events for one sport ID."""
    raw = _http_get(
        f"/api/live/sports/{sport_id}/events",
        params={"limit": limit, "offset": offset},
    )
    if raw:
        for key in ("events", "data", "items"):
            if isinstance(raw.get(key), list) and raw[key]:
                return raw[key]
        if isinstance(raw, list):
            return raw

    raw = _http_get("/api/live/games", params={"sportId": sport_id})
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "games", "items", "events"):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def fetch_live_sports() -> list[dict]:
    r      = _get_redis()
    cached = r.get("sp:live:sports")
    if cached:
        return json.loads(cached)
    raw = _http_get("/api/live/sports")
    sports: list = []
    if isinstance(raw, dict):
        sports = raw.get("sports") or raw.get("data") or []
    elif isinstance(raw, list):
        sports = raw
    if sports:
        r.set("sp:live:sports", json.dumps(sports), ex=60)
    return sports


def fetch_live_markets(
    event_ids: list[int],
    sport_id:  int,
    market_type: int = 194,
) -> list[dict]:
    """
    FIX-1: Uses the correct /api/live/event/markets endpoint.
    Returns raw list of {eventId, markets} dicts.
    """
    batch = fetch_live_markets_for_events(event_ids, sport_id, market_type)
    result = []
    for ev_id_str, mkts in batch.items():
        result.append({"eventId": int(ev_id_str), "markets": mkts})
    return result


def get_odds_history(event_id: int, market_id: int, limit: int = 20) -> list[dict]:
    r   = _get_redis()
    key = f"sp:live:history:{event_id}:{market_id}"
    try:
        return [json.loads(i) for i in r.lrange(key, 0, limit - 1)]
    except Exception as exc:
        log.warning("get_odds_history %s: %s", key, exc)
        return []


# =============================================================================
# FIX-3: parse_event_to_match — convert raw SP event + markets → SpMatch
# =============================================================================

def _parse_event_state(ev: dict) -> dict:
    """
    Extract match state fields from a live events API response row.
    The events list (/api/live/sports/{id}/events) includes:
      state.currentEventPhase, state.matchTime, state.matchScore,
      state.clockRunning, isPaused, status, kickoffTimeUTC, externalId
    """
    state = ev.get("state") or {}
    score = state.get("matchScore") or {}
    comps = ev.get("competitors") or []

    home = ""
    away = ""
    if isinstance(comps, list) and len(comps) >= 2:
        home = str(comps[0].get("name") or "")
        away = str(comps[1].get("name") or "")
    elif isinstance(comps, dict):
        home = str(comps.get("home") or comps.get("0") or "")
        away = str(comps.get("away") or comps.get("1") or "")

    sport_raw   = ev.get("sport") or {}
    sport_name  = str(sport_raw.get("name") or "") if isinstance(sport_raw, dict) else str(sport_raw)
    comp_raw    = ev.get("tournament") or ev.get("competition") or ev.get("league") or {}
    competition = str(comp_raw.get("name") or "") if isinstance(comp_raw, dict) else str(comp_raw)
    country_raw = ev.get("country") or {}
    country     = str(country_raw.get("name") or "") if isinstance(country_raw, dict) else str(country_raw)

    return {
        # Match identity
        "sp_game_id":    str(ev.get("id") or ""),
        "betradar_id":   str(ev.get("externalId") or ev.get("betradarId") or ev.get("betRadarId") or ""),
        "home_team":     home,
        "away_team":     away,
        "competition":   competition,
        "country":       country,
        "sport":         sport_name,
        # Timing
        "start_time":    ev.get("kickoffTimeUTC") or ev.get("startTime"),
        "status":        ev.get("status", "Started"),
        # Match state (live)
        "phase":         state.get("currentEventPhase", ""),
        "match_time":    state.get("matchTime", ""),
        "clock_running": state.get("clockRunning", False),
        "score_home":    str(score.get("home") or "0"),
        "score_away":    str(score.get("away") or "0"),
        "is_paused":     ev.get("isPaused", False),
        "period_start":  state.get("periodStartTime"),
        # Source metadata
        "source":        "sportpesa",
        "mode":          "live",
        "harvested_at":  _now_ts(),
    }


def _build_live_match(ev: dict, markets: dict[str, dict[str, float]]) -> dict:
    """Combine event state + normalised markets into a full SpMatch dict."""
    base = _parse_event_state(ev)
    base["markets"]      = markets
    base["market_count"] = len(markets)
    return base


# =============================================================================
# FIX-3/FIX-4: fetch_live_stream + fetch_live
# =============================================================================

def fetch_live_stream(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.2,
    batch_size:         int   = 12,
    **_,
) -> Generator[dict, None, None]:
    """
    FIX-3: Stream live matches one at a time, identical shape to
    sp_harvester.fetch_upcoming_stream().

    Flow:
      1. GET /api/live/sports/{sportId}/events  → event list with state
      2. Batch fetch markets via /api/live/event/markets  (FIX-1)
      3. Normalise selections with positional fallback  (FIX-2)
      4. yield one complete SpMatch per event immediately

    Each yielded match includes:
      sp_game_id, betradar_id, home_team, away_team, competition, sport,
      start_time, status, phase, match_time, clock_running, score_home,
      score_away, is_paused, markets: {slug: {outcome: float}}
    """
    sport_id = _SLUG_TO_SPORT_ID.get(sport_slug.lower())
    if not sport_id:
        log.warning("[live:stream] unknown sport: %s", sport_slug)
        return

    events = fetch_live_events(sport_id, limit=200)
    log.info("[live:stream] %s: %d events (sportId=%d)", sport_slug, len(events), sport_id)

    if not events:
        return

    # Index events by ID for fast lookup
    ev_index: dict[str, dict] = {str(ev.get("id")): ev for ev in events if ev.get("id")}

    # Process events in batches to fetch markets
    event_ids = list(ev_index.keys())

    # If markets not needed, yield state-only matches immediately
    if not fetch_full_markets:
        for ev_id, ev in ev_index.items():
            yield _build_live_match(ev, {})
        return

    for i in range(0, len(event_ids), batch_size):
        batch_ids = event_ids[i: i + batch_size]

        # Fetch all relevant market types for this batch
        all_mkts = fetch_all_market_types_for_events(batch_ids, sport_id)

        for ev_id in batch_ids:
            ev      = ev_index.get(ev_id, {})
            markets = all_mkts.get(ev_id, {})
            match   = _build_live_match(ev, markets)

            if markets:
                log.debug("[live:stream] %s markets=%s", ev_id, list(markets.keys())[:5])
            else:
                log.warning("[live:stream] %s no markets fetched", ev_id)

            yield match

        if i + batch_size < len(event_ids):
            time.sleep(sleep_between)


def fetch_live(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.2,
    **_,
) -> list[dict]:
    """
    FIX-4: Blocking wrapper — fetch all live matches for a sport.
    Drop-in replacement callable by snapshot-canonical and celery tasks.
    """
    results = list(fetch_live_stream(
        sport_slug,
        fetch_full_markets = fetch_full_markets,
        sleep_between      = sleep_between,
    ))
    log.info("[live] %s: %d live matches", sport_slug, len(results))
    return results


# =============================================================================
# Snapshot helpers (updated to use correct endpoints)
# =============================================================================

def _snapshot_sport(sport_id: int) -> list[dict]:
    """Build a full Redis snapshot for one sport using the correct market endpoint."""
    r      = _get_redis()
    events = fetch_live_events(sport_id, limit=200)
    if not events:
        return []

    ev_ids  = [str(ev.get("id")) for ev in events if ev.get("id")]
    ev_map  = {str(ev.get("id")): ev for ev in events if ev.get("id")}
    all_mkt = fetch_all_market_types_for_events(ev_ids, sport_id)

    snap_events = []
    for ev_id in ev_ids:
        ev        = ev_map[ev_id]
        markets   = all_mkt.get(ev_id, {})
        snap_events.append({
            "eventId": int(ev_id),
            "event":   ev,
            "markets": markets,
            "state":   _parse_event_state(ev),
        })

    r.set(f"sp:live:snapshot:{sport_id}", json.dumps({
        "sport_id":     sport_id,
        "event_count":  len(snap_events),
        "harvested_at": _now_ts(),
        "events":       snap_events,
    }), ex=90)
    return snap_events


def snapshot_all_sports() -> dict[int, list]:
    result = {}
    for sport_id in _LIVE_SPORT_IDS:
        try:
            evs = _snapshot_sport(sport_id)
            result[sport_id] = evs
            log.info("[snapshot] sport=%d → %d events", sport_id, len(evs))
        except Exception as exc:
            log.warning("[snapshot] sport=%d error: %s", sport_id, exc)
    return result


# =============================================================================
# Sportradar timeline polling (unchanged from v5)
# =============================================================================

_SR_BASE           = "https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo"
_SR_ORIGIN         = "https://www.ke.sportpesa.com"
_SR_TOKEN_FALLBACK = (
    "exp=1774813127~acl=/*"
    "~data=eyJvIjoiaHR0cHM6Ly93d3cua2Uuc3BvcnRwZXNhLmNvbSIsImEiOiJmODYx"
    "N2E4OTZkMzU1MWJhNTBkNTFmMDE0OWQ0YjZkZCIsImFjdCI6Im9yaWdpbmNoZWNrIiwi"
    "b3NyYyI6Im9yaWdpbiJ9"
    "~hmac=b884ebf888ce9db171eab4009630c51fbc7ef37d97344b7028c5449a12700b13"
)
_SR_HEADERS = {
    "Origin":     _SR_ORIGIN,
    "Referer":    _SR_ORIGIN + "/",
    "User-Agent": _HEADERS["User-Agent"],
}

_PERIOD_BASE_SEC: dict[str, int] = {
    "1": 0, "2": 45 * 60, "3": 90 * 60, "4": 105 * 60,
}
_SR_STATUS_TO_PHASE: dict[int, str] = {
    6: "FirstHalf",    7: "SecondHalf",
    31: "HalfTime",    41: "ExtraTimeFirst",
    42: "ExtraTimeSecond", 50: "PenaltyShootout",
    80: "AwaitingExtraTime", 17: "Paused", 11: "Interrupted",
}

_sr_event_map:   dict[int, int] = {}
_sr_event_sport: dict[int, int] = {}
_sr_map_lock     = threading.Lock()

_SR_THREAD: threading.Thread | None = None
_SR_STOP    = threading.Event()


def _sr_token() -> str:
    return os.getenv("SPORTRADAR_TOKEN", _SR_TOKEN_FALLBACK)


def _fetch_sr_timeline(external_id: int) -> dict | None:
    url = f"{_SR_BASE}/match_timelinedelta/{external_id}?T={_sr_token()}"
    try:
        resp = requests.get(url, headers=_SR_HEADERS, timeout=8)
        return resp.json() if resp.ok else None
    except Exception:
        return None


def _parse_sr_event_update(
    external_id: int, data: dict, event_id: int, sport_id: int,
) -> dict | None:
    try:
        doc      = (data.get("doc") or [{}])[0]
        match    = doc.get("data", {}).get("match", {})
        p        = str(match.get("p") or "")
        ptime    = match.get("ptime")
        timeinfo = match.get("timeinfo") or {}
        running  = bool(timeinfo.get("running", False))
        result   = match.get("result") or {}
        status   = match.get("status") or {}
        status_id = int(status.get("_id") or 0)

        if p and ptime:
            base_sec = _PERIOD_BASE_SEC.get(p, 0)
            elapsed  = int(time.time()) - int(ptime)
            total    = base_sec + (elapsed if running else 0)
            mm, ss   = divmod(max(0, total), 60)
            match_time_str = f"{mm}:{ss:02d}"
        else:
            match_time_str = ""

        return {
            "type":          "event_update",
            "event_id":      event_id,
            "sport_id":      sport_id,
            "status":        "Started",
            "phase":         _SR_STATUS_TO_PHASE.get(status_id, ""),
            "match_time":    match_time_str,
            "clock_running": running,
            "remaining_ms":  None,
            "score_home":    str(result.get("home") or "0"),
            "score_away":    str(result.get("away") or "0"),
            "is_paused":     not running,
            "sr_period":     p,
            "sr_ptime":      int(ptime) if ptime else None,
            "ts":            _now_ts(),
            "source":        "sportradar",
        }
    except Exception as exc:
        log.debug("[SR] parse error ext=%d: %s", external_id, exc)
        return None


def _sr_poll_loop() -> None:
    r = _get_redis()
    log.info("[SR] timeline polling started")

    while not _SR_STOP.is_set():
        with _sr_map_lock:
            snap_events = dict(_sr_event_map)
            snap_sports = dict(_sr_event_sport)

        for event_id, external_id in snap_events.items():
            if external_id <= 0:
                continue
            sport_id = snap_sports.get(event_id, 1)
            try:
                data    = _fetch_sr_timeline(external_id)
                if not data:
                    continue
                payload = _parse_sr_event_update(external_id, data, event_id, sport_id)
                if not payload:
                    continue
                r.set(
                    f"sp:live:state:{event_id}",
                    json.dumps({k: v for k, v in payload.items() if k != "type"}),
                    ex=300,
                )
                _record_state_snapshot(r, event_id, payload)
                _publish_live_update(r, sport_id, event_id, payload)
            except Exception as exc:
                log.debug("[SR] event=%d: %s", event_id, exc)

            _SR_STOP.wait(0.05)
            if _SR_STOP.is_set():
                break

        _SR_STOP.wait(5)

    log.info("[SR] timeline polling stopped")


def _start_sr_thread() -> None:
    global _SR_THREAD, _SR_STOP
    if _SR_THREAD and _SR_THREAD.is_alive():
        return
    _SR_STOP.clear()
    _SR_THREAD = threading.Thread(
        target=_sr_poll_loop, name="sp-live-sr-poll", daemon=True,
    )
    _SR_THREAD.start()
    log.info("[SR] thread started")


def _stop_sr_thread() -> None:
    _SR_STOP.set()
    if _SR_THREAD and _SR_THREAD.is_alive():
        _SR_THREAD.join(timeout=5)


# =============================================================================
# WS HARVESTER (FIX-5: also subscribe type 105 per event)
# =============================================================================

_HARVESTER_THREAD: threading.Thread | None = None
_HARVESTER_STOP   = threading.Event()

_WS_BASE = "wss://realtime-notificator.ke.sportpesa.com"

# FIX-5: Subscribe to both 1x2 (194) and O/U 2.5 (105) per event.
# SP sends all markets for an event once either channel is open,
# but subscribing both ensures O/U updates arrive even when the
# 1x2 market is suspended.
_WS_SUBSCRIBE_CHANNELS: list[str] = [
    "194-0.00",    # 1x2
    "105-2.50",    # O/U 2.5
]


def _ws_url() -> str:
    return f"{_WS_BASE}/socket.io/?EIO=3&transport=websocket"


def _ws_extra_headers() -> list[tuple[str, str]]:
    return [
        ("Origin",          _SR_ORIGIN),
        ("User-Agent",      _HEADERS["User-Agent"]),
        ("Referer",         _SR_ORIGIN + "/"),
        ("Accept-Language", "en-GB,en-US;q=0.9,en;q=0.8"),
    ]


def _parse_ws_frame(raw: str) -> tuple[str | None, Any]:
    if raw.startswith("42"):
        try:
            payload = json.loads(raw[2:])
            if isinstance(payload, list) and payload:
                return str(payload[0]), payload[1] if len(payload) > 1 else None
        except (json.JSONDecodeError, IndexError):
            pass
    return None, None


def _normalise_outcome_key(
    sel_name: str, sel_idx: int, all_sels: list[dict],
    market_type: int, handicap: str | None, sport_id: int,
) -> str:
    """WS outcome normaliser — positional fallback handles team names."""
    name_l = sel_name.strip().lower()
    if name_l in _DIRECT_KEYS:
        return _DIRECT_KEYS[name_l]
    if name_l.startswith("over"):
        return "over"
    if name_l.startswith("under"):
        return "under"
    if ":" in sel_name and len(sel_name) <= 5:
        return sel_name.strip()
    if "/" in sel_name and len(sel_name) <= 5:
        return sel_name.strip()
    pos_map = _POS_OUTCOME.get(market_type)
    if pos_map and sel_idx < len(pos_map):
        return pos_map[sel_idx]
    if market_type == 105:
        return "over" if sel_idx == 0 else "under"
    slug = name_l.replace(" ", "_").replace("-", "_")
    slug = "".join(c for c in slug if c.isalnum() or c == "_")
    return slug[:14] or f"sel_{sel_idx}"


def _market_from_buffered_update(
    msg: dict, sport_id_map: dict[int, int],
) -> dict | None:
    event_id    = msg.get("eventId")
    market_type = msg.get("type")
    if not event_id or market_type is None:
        return None

    sport_id = sport_id_map.get(event_id, 1)
    handicap = str(msg.get("handicap") or "0.00")
    slug     = _market_slug_from_type(market_type, sport_id, handicap)
    all_sels = msg.get("selections") or []

    norm_sels = []
    for idx, sel in enumerate(all_sels):
        out_key = _normalise_outcome_key(
            sel_name    = sel.get("name", ""),
            sel_idx     = idx,
            all_sels    = all_sels,
            market_type = market_type,
            handicap    = handicap,
            sport_id    = sport_id,
        )
        norm_sels.append({
            "id":          sel.get("id"),
            "name":        sel.get("name", ""),
            "outcome_key": out_key,
            "odds":        str(sel.get("odds", "0")),
            "status":      sel.get("status", "Open"),
        })

    return {
        "type":                   "market_update",
        "event_id":               event_id,
        "sport_id":               sport_id,
        "market_id":              msg.get("eventMarketId"),
        "market_type":            market_type,
        "market_name":            msg.get("name", ""),
        "market_slug":            slug,
        "handicap":               handicap,
        "all_selections":         all_sels,
        "normalised_selections":  norm_sels,
        "changed_selections":     [],
        "ts":                     _now_ts(),
    }


def _state_from_event_update(msg: dict) -> dict:
    state = msg.get("state") or {}
    score = state.get("matchScore") or {}
    return {
        "type":          "event_update",
        "event_id":      msg.get("id"),
        "sport_id":      msg.get("sportId", 1),
        "status":        msg.get("status", ""),
        "phase":         state.get("currentEventPhase", ""),
        "match_time":    state.get("matchTime", ""),
        "clock_running": state.get("clockRunning", False),
        "remaining_ms":  state.get("remainingTimeMillis"),
        "score_home":    str(score.get("home") or "0"),
        "score_away":    str(score.get("away") or "0"),
        "is_paused":     msg.get("isPaused", False),
        "period_start":  state.get("periodStartTime"),
        "ts":            _now_ts(),
        "source":        "sportpesa_ws",
    }


def _diff_and_record(r, payload: dict) -> dict:
    event_id    = payload["event_id"]
    market_id   = payload.get("market_id") or 0
    changed     = []
    pipe        = r.pipeline()
    history_key = f"sp:live:history:{event_id}:{market_id}"

    for sel in payload["normalised_selections"]:
        sel_id   = sel.get("id")
        if sel_id is None:
            continue
        odds_key = f"sp:live:odds:{event_id}:{sel_id}"
        new_odds = str(sel["odds"])
        prev     = r.get(odds_key)
        if prev != new_odds:
            changed.append({"id": sel_id, "odds": new_odds, "prev": prev})
            pipe.set(odds_key, new_odds, ex=300)

    if changed:
        tick = {
            "t":       _now_ts(),
            "market":  payload["market_slug"],
            "changed": [{"id": c["id"], "odds": c["odds"]} for c in changed],
        }
        pipe.lpush(history_key, json.dumps(tick))
        pipe.ltrim(history_key, 0, 49)
        pipe.expire(history_key, 600)

    pipe.execute()
    payload["changed_selections"] = changed
    return payload


def _cache_event_state(r, payload: dict) -> None:
    event_id = payload.get("event_id")
    if not event_id:
        return
    r.set(
        f"sp:live:state:{event_id}",
        json.dumps({k: v for k, v in payload.items() if k != "type"}),
        ex=300,
    )


def _harvester_loop() -> None:
    try:
        import websocket
    except ImportError:
        log.error("websocket-client not installed — pip install websocket-client")
        return

    r = _get_redis()

    while not _HARVESTER_STOP.is_set():

        sport_id_map:    dict[int, int] = {}
        external_id_map: dict[int, int] = {}
        subscribed:      set[int]       = set()

        log.info("[WS] fetching live event list …")
        for sport_id in _LIVE_SPORT_IDS:
            try:
                events = fetch_live_events(sport_id, limit=200)
                for ev in events:
                    ev_id  = ev.get("id")
                    ext_id = (ev.get("externalId") or ev.get("betradarId")
                              or ev.get("betRadarId") or 0)
                    if ev_id:
                        sport_id_map[ev_id]    = sport_id
                        external_id_map[ev_id] = int(ext_id or 0)

                sports_raw = r.get("sp:live:sports")
                if sports_raw:
                    current = json.loads(sports_raw)
                    for sp in current:
                        if sp.get("id") == sport_id:
                            sp["eventNumber"] = len(events)
                            break
                    else:
                        current.append({"id": sport_id, "eventNumber": len(events)})
                    r.set("sp:live:sports", json.dumps(current), ex=60)
            except Exception as exc:
                log.warning("[WS] snapshot sport=%d: %s", sport_id, exc)

        with _sr_map_lock:
            _sr_event_map.clear()
            _sr_event_map.update(external_id_map)
            _sr_event_sport.clear()
            _sr_event_sport.update(sport_id_map)

        log.info("[WS] tracking %d live events", len(sport_id_map))

        if not sport_id_map:
            log.info("[WS] no live events — sleeping 30 s")
            _HARVESTER_STOP.wait(30)
            continue

        ws_connected = threading.Event()

        def on_open(ws):
            log.info("[WS] connected to realtime-notificator")
            ws_connected.set()
            # FIX-5: subscribe both 194 (1x2) and 105 (O/U 2.5) per event
            for ev_id in sport_id_map:
                for channel_suffix in _WS_SUBSCRIBE_CHANNELS:
                    ws.send(f'42["subscribe","buffered-event-{ev_id}-{channel_suffix}"]')
                subscribed.add(ev_id)
            log.info("[WS] subscribed to %d events × %d channels",
                     len(subscribed), len(_WS_SUBSCRIBE_CHANNELS))

        def on_message(ws, raw: str):
            try:
                if raw == "2":
                    ws.send("3")
                    return
                if raw == "31":
                    return

                event_name, data = _parse_ws_frame(raw)
                if not event_name or not data:
                    return

                if event_name == "BUFFERED_MARKET_UPDATE":
                    payload = _market_from_buffered_update(data, sport_id_map)
                    if not payload:
                        return
                    payload  = _diff_and_record(r, payload)
                    event_id = payload["event_id"]
                    sport_id = payload["sport_id"]
                    _record_market_snapshot(r, event_id, payload)
                    _publish_live_update(r, sport_id, event_id, payload)

                elif event_name == "EVENT_UPDATE":
                    event_id = data.get("id")
                    if not event_id:
                        return
                    sport_id = data.get("sportId") or sport_id_map.get(event_id, 1)
                    payload  = _state_from_event_update(data)
                    _cache_event_state(r, payload)
                    _record_state_snapshot(r, event_id, payload)
                    _publish_live_update(r, sport_id, event_id, payload)

                    if event_id not in sport_id_map:
                        sport_id_map[event_id] = sport_id
                    if event_id not in subscribed:
                        for ch in _WS_SUBSCRIBE_CHANNELS:
                            ws.send(f'42["subscribe","buffered-event-{event_id}-{ch}"]')
                        subscribed.add(event_id)

            except Exception as exc:
                log.warning("[WS:on_message] %s", exc)

        def on_error(ws, error):
            log.warning("[WS] error: %s", error)

        def on_close(ws, code, msg):
            log.info("[WS] closed code=%s", code)
            ws_connected.clear()

        ws = websocket.WebSocketApp(
            _ws_url(),
            header=[f"{k}: {v}" for k, v in _ws_extra_headers()],
            on_open    = on_open,
            on_message = on_message,
            on_error   = on_error,
            on_close   = on_close,
        )

        ws_thread = threading.Thread(
            target=ws.run_forever,
            kwargs={"ping_interval": 0},
            daemon=True,
            name="sp-live-ws",
        )
        ws_thread.start()

        if not ws_connected.wait(timeout=15):
            log.warning("[WS] connect timeout — retrying in 10 s")
            ws.close()
            _HARVESTER_STOP.wait(10)
            continue

        last_refresh = time.monotonic()
        last_ping    = time.monotonic()

        while not _HARVESTER_STOP.is_set() and ws_thread.is_alive():
            now_m = time.monotonic()

            if now_m - last_ping >= 20:
                try:
                    ws.send("21")
                except Exception:
                    pass
                last_ping = now_m

            if now_m - last_refresh >= 60:
                for sport_id in _LIVE_SPORT_IDS:
                    try:
                        events = fetch_live_events(sport_id, limit=200)
                        for ev in events:
                            ev_id  = ev.get("id")
                            ext_id = (ev.get("externalId") or ev.get("betradarId") or 0)
                            if not ev_id:
                                continue
                            sport_id_map[ev_id]    = sport_id
                            external_id_map[ev_id] = int(ext_id or 0)
                            if ev_id not in subscribed:
                                try:
                                    for ch in _WS_SUBSCRIBE_CHANNELS:
                                        ws.send(f'42["subscribe","buffered-event-{ev_id}-{ch}"]')
                                    subscribed.add(ev_id)
                                except Exception:
                                    pass
                    except Exception as exc:
                        log.warning("[WS] refresh sport=%d: %s", sport_id, exc)

                with _sr_map_lock:
                    _sr_event_map.update(external_id_map)
                    _sr_event_sport.update(sport_id_map)

                last_refresh = now_m

            _HARVESTER_STOP.wait(1)

        log.info("[WS] disconnected — reconnecting in 5 s …")
        try:
            ws.close()
        except Exception:
            pass
        _HARVESTER_STOP.wait(5)


# =============================================================================
# Public thread management
# =============================================================================

def harvester_alive() -> bool:
    return _HARVESTER_THREAD is not None and _HARVESTER_THREAD.is_alive()


def start_harvester_thread() -> threading.Thread:
    global _HARVESTER_THREAD, _HARVESTER_STOP
    if not harvester_alive():
        _HARVESTER_STOP.clear()
        _HARVESTER_THREAD = threading.Thread(
            target=_harvester_loop, name="sp-live-harvester", daemon=True,
        )
        _HARVESTER_THREAD.start()
        log.info("[harvester] WS thread started")
    _start_sr_thread()
    return _HARVESTER_THREAD  # type: ignore[return-value]


def stop_harvester() -> None:
    global _HARVESTER_THREAD
    _HARVESTER_STOP.set()
    _stop_sr_thread()
    if _HARVESTER_THREAD and _HARVESTER_THREAD.is_alive():
        _HARVESTER_THREAD.join(timeout=5)
        log.info("[harvester] stopped")
    _HARVESTER_THREAD = None


# =============================================================================
# Public exports
# =============================================================================

__all__ = [
    # Streaming generators (used by SSE stream-matches endpoint)
    "fetch_live_stream",
    # Blocking wrappers (used by snapshot-canonical and celery)
    "fetch_live",
    # REST data fetchers (used by sp_live_view.py)
    "fetch_live_sports",
    "fetch_live_events",
    "fetch_live_markets",
    "fetch_live_markets_for_events",
    "fetch_all_market_types_for_events",
    # History
    "get_odds_history",
    "get_market_snapshot_history",
    "get_state_snapshot_history",
    # Snapshot
    "snapshot_all_sports",
    # Thread management
    "start_harvester_thread",
    "stop_harvester",
    "harvester_alive",
]