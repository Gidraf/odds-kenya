"""
app/workers/sp_live_harvester.py  (v7 — real-time zero-lag)
=============================================================
Key changes from v6
───────────────────
WS-1  Subscribe to ALL market types per event, not just 1x2 + O/U 2.5.
  Every event now gets channels for:
    194-0.00   1x2
    105-1.50   O/U 1.5
    105-2.50   O/U 2.5
    105-3.50   O/U 3.5
    147-0.00   Double Chance
    138-0.00   BTTS
    140-0.00   1H BTTS
    145-0.00   Odd/Even
    166-0.00   Draw No Bet
    135-0.00   First Half 1x2
    184-0.00   Asian Handicap
    151-0.00   European Handicap
  Format: 42["subscribe","buffered-event-{id}-{type}-{handicap}"]

WS-2  Ping strategy exactly mirrors browser traffic:
  - Connection handshake: receive "0{...}", send nothing, receive "40",
    THEN send subscriptions.
  - Keep-alive: send "21" every 20 s (matching pingInterval from server).
  - Pong: receive "31" → no response needed (engine.io transport heartbeat).

WS-3  1-second REPUBLISHER thread.
  A dedicated background thread reads current state + odds from Redis
  every second and publishes to all sport/event channels.  This means
  the frontend NEVER waits more than 1 s to see the latest odds even
  if no WS message arrived.

WS-4  Immediate re-publish on every WS message.
  On BUFFERED_MARKET_UPDATE and EVENT_UPDATE, we publish to Redis pub/sub
  instantly BEFORE writing history or updating odds keys.  This shaves
  latency for the first subscriber frame.

WS-5  Auto-subscribe new events.
  EVENT_UPDATE frames for unseen event IDs trigger an immediate
  subscription batch so we never miss a match that started mid-session.

WS-6  Sportradar timeline thread publishes every 5 s (was same cadence but
  now also re-publishes to the 1-s republisher bucket).
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
# Channel helpers
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
    """Publish to all three channels: all, sport, event."""
    raw = json.dumps(payload, ensure_ascii=False)
    try:
        pipe = r.pipeline()
        pipe.publish(CH_ALL, raw)
        pipe.publish(_ch_sport(sport_id), raw)
        pipe.publish(_ch_event(event_id), raw)
        pipe.execute()
    except Exception as exc:
        log.warning("publish failed: %s", exc)


# =============================================================================
# WS subscription channels
# WS-1: subscribe to ALL market types per event
# Format: buffered-event-{eventId}-{type}-{handicap}
# =============================================================================

# Primary markets — always subscribe
_WS_CHANNELS_PRIMARY = [
    "194-0.00",    # 1x2
    "105-2.50",    # O/U 2.5 goals
    "105-1.50",    # O/U 1.5 goals
    "105-3.50",    # O/U 3.5 goals
    "147-0.00",    # Double Chance
    "138-0.00",    # BTTS
    "145-0.00",    # Odd/Even
    "166-0.00",    # Draw No Bet
    "135-0.00",    # 1H 1x2
    "140-0.00",    # 1H BTTS
    "151-0.00",    # European Handicap
    "184-0.00",    # Asian Handicap
]

# Additional totals (subscribe for soccer/football only)
_WS_CHANNELS_SOCCER_EXTRA = [
    "105-0.50",
    "105-4.50",
    "105-5.50",
]

# Per-sport channel sets (sport_id → list of suffixes)
_SPORT_WS_CHANNELS: dict[int, list[str]] = {
    1:  _WS_CHANNELS_PRIMARY + _WS_CHANNELS_SOCCER_EXTRA,  # soccer
    2:  _WS_CHANNELS_PRIMARY + ["105-180.50", "105-200.50"],  # basketball totals
    4:  _WS_CHANNELS_PRIMARY + ["105-20.50", "105-22.50"],    # tennis
    5:  _WS_CHANNELS_PRIMARY,
    6:  _WS_CHANNELS_PRIMARY,
    8:  _WS_CHANNELS_PRIMARY,
    9:  _WS_CHANNELS_PRIMARY,
    10: _WS_CHANNELS_PRIMARY,
    13: _WS_CHANNELS_PRIMARY,
    21: _WS_CHANNELS_PRIMARY,
    23: _WS_CHANNELS_PRIMARY,
}

_LIVE_SPORT_IDS = [1, 2, 4, 5, 6, 8, 9, 10, 13, 21, 23]


def _channels_for_event(event_id: int, sport_id: int) -> list[str]:
    suffixes = _SPORT_WS_CHANNELS.get(sport_id, _WS_CHANNELS_PRIMARY)
    return [f"buffered-event-{event_id}-{s}" for s in suffixes]


def _subscribe_event(ws, event_id: int, sport_id: int) -> None:
    """Send all subscribe frames for one event. Mirrors browser exactly."""
    for channel in _channels_for_event(event_id, sport_id):
        try:
            ws.send(f'42["subscribe","{channel}"]')
        except Exception as exc:
            log.debug("[WS] subscribe %s: %s", channel, exc)


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
# Sport slug ↔ sport ID maps
# =============================================================================

_SLUG_TO_SPORT_ID: dict[str, int] = {
    "soccer": 1, "football": 1, "esoccer": 126, "efootball": 126,
    "basketball": 2, "tennis": 5, "ice-hockey": 4, "volleyball": 23,
    "cricket": 21, "rugby": 12, "boxing": 10, "handball": 6,
    "table-tennis": 16, "mma": 117, "darts": 49,
    "american-football": 15, "baseball": 3,
}

# =============================================================================
# Outcome normalisation
# =============================================================================

_POS_OUTCOME: dict[int, list[str]] = {
    194:  ["1", "X", "2"],
    149:  ["1", "2"],
    147:  ["1X", "X2", "12"],
    138:  ["yes", "no"],
    140:  ["yes", "no"],
    145:  ["odd", "even"],
    166:  ["1", "2"],
    135:  ["1", "X", "2"],
    155:  ["1st", "2nd", "equal"],
    129:  ["none", "1", "2"],
    151:  ["1", "X", "2"],
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
    194: "1x2",            149: "match_winner",      147: "double_chance",
    138: "btts",           140: "first_half_btts",    145: "odd_even",
    166: "draw_no_bet",    151: "european_handicap",  184: "asian_handicap",
    183: "correct_score",  154: "exact_goals",        135: "first_half_1x2",
    129: "first_team_to_score",
}

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


def _normalise_outcome_key(
    sel_name: str, sel_idx: int, market_type: int, sport_id: int,
) -> str:
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


# =============================================================================
# Snapshot history helpers (unchanged from v6)
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


def get_market_snapshot_history(
    event_id: int, date: str | None = None, limit: int = 100,
) -> list[dict]:
    r   = _get_redis()
    key = f"sp:live:snap:{event_id}:{date or _today()}"
    try:
        return [json.loads(i) for i in r.lrange(key, 0, limit - 1)]
    except Exception:
        return []


def get_state_snapshot_history(
    event_id: int, date: str | None = None, limit: int = 100,
) -> list[dict]:
    r   = _get_redis()
    key = f"sp:live:state_hist:{event_id}:{date or _today()}"
    try:
        return [json.loads(i) for i in r.lrange(key, 0, limit - 1)]
    except Exception:
        return []


def get_odds_history(event_id: int, market_id: int, limit: int = 20) -> list[dict]:
    r   = _get_redis()
    key = f"sp:live:history:{event_id}:{market_id}"
    try:
        return [json.loads(i) for i in r.lrange(key, 0, limit - 1)]
    except Exception:
        return []


# =============================================================================
# Event list + sports fetchers
# =============================================================================

def fetch_live_events(sport_id: int, limit: int = 200, offset: int = 0) -> list[dict]:
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
    raw    = _http_get("/api/live/sports")
    sports: list = []
    if isinstance(raw, dict):
        sports = raw.get("sports") or raw.get("data") or []
    elif isinstance(raw, list):
        sports = raw
    if sports:
        r.set("sp:live:sports", json.dumps(sports), ex=60)
    return sports


# =============================================================================
# Market fetchers (HTTP batch)
# =============================================================================

_MARKET_BATCH_SIZE = 12


def fetch_live_markets_for_events(
    event_ids: list[int | str],
    sport_id:  int,
    market_type: int = 194,
) -> dict[str, list[dict]]:
    if not event_ids:
        return {}
    result: dict[str, list[dict]] = {}
    for i in range(0, len(event_ids), _MARKET_BATCH_SIZE):
        batch   = event_ids[i: i + _MARKET_BATCH_SIZE]
        ids_str = ",".join(str(e) for e in batch)
        raw     = _http_get(
            "/api/live/event/markets",
            params={"eventId": ids_str, "type": str(market_type), "sportId": str(sport_id)},
            timeout=12,
        )
        if not raw:
            continue
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


def _parse_live_selections(
    sels: list[dict], market_type: int, sport_id: int, handicap: str,
) -> dict[str, float]:
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
        key = _DIRECT_KEYS.get(short.strip().lower())
        if not key:
            key = _DIRECT_KEYS.get(name.strip().lower())
        if not key:
            nl = name.strip().lower()
            if nl.startswith("over"):   key = "over"
            elif nl.startswith("under"): key = "under"
        if not key:
            stripped = name.strip()
            if ":" in stripped and len(stripped) <= 5:
                key = stripped
            elif "/" in stripped and len(stripped) <= 5:
                key = stripped
        if not key and pos_map and idx < len(pos_map):
            key = pos_map[idx]
        if not key and market_type == 105:
            key = "over" if idx == 0 else "under"
        if not key:
            key = name.lower().replace(" ", "_")[:14] or f"sel_{idx}"
        if price > result.get(key, 0.0):
            result[key] = round(price, 3)
    return result


def fetch_all_market_types_for_events(
    event_ids: list[int | str],
    sport_id:  int,
) -> dict[str, dict[str, dict[str, float]]]:
    if not event_ids:
        return {}
    all_markets: dict[str, dict[str, dict[str, float]]] = {
        str(eid): {} for eid in event_ids
    }
    for mkt_type in [194, 105, 147, 138, 145, 166, 135, 140]:
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
                    existing = all_markets[ev_id_str].get(slug, {})
                    for outcome, price in parsed.items():
                        if price > existing.get(outcome, 0.0):
                            existing[outcome] = price
                    all_markets[ev_id_str][slug] = existing
    return all_markets


def fetch_live_markets(
    event_ids: list[int], sport_id: int, market_type: int = 194,
) -> list[dict]:
    batch  = fetch_live_markets_for_events(event_ids, sport_id, market_type)
    return [{"eventId": int(k), "markets": v} for k, v in batch.items()]


# =============================================================================
# Event state parser + match builder
# =============================================================================

def _parse_event_state(ev: dict) -> dict:
    state = ev.get("state") or {}
    score = state.get("matchScore") or {}
    comps = ev.get("competitors") or []
    home = away = ""
    if isinstance(comps, list) and len(comps) >= 2:
        home = str(comps[0].get("name") or "")
        away = str(comps[1].get("name") or "")
    elif isinstance(comps, dict):
        home = str(comps.get("home") or "")
        away = str(comps.get("away") or "")
    sport_raw   = ev.get("sport") or {}
    sport_name  = str(sport_raw.get("name") or "") if isinstance(sport_raw, dict) else str(sport_raw)
    comp_raw    = ev.get("tournament") or ev.get("competition") or ev.get("league") or {}
    competition = str(comp_raw.get("name") or "") if isinstance(comp_raw, dict) else str(comp_raw)
    country_raw = ev.get("country") or {}
    country     = str(country_raw.get("name") or "") if isinstance(country_raw, dict) else str(country_raw)
    return {
        "sp_game_id":    str(ev.get("id") or ""),
        "betradar_id":   str(ev.get("externalId") or ev.get("betradarId") or ""),
        "home_team":     home,
        "away_team":     away,
        "competition":   competition,
        "country":       country,
        "sport":         sport_name,
        "start_time":    ev.get("kickoffTimeUTC") or ev.get("startTime"),
        "status":        ev.get("status", "Started"),
        "phase":         state.get("currentEventPhase", ""),
        "match_time":    state.get("matchTime", ""),
        "clock_running": state.get("clockRunning", False),
        "score_home":    str(score.get("home") or "0"),
        "score_away":    str(score.get("away") or "0"),
        "is_paused":     ev.get("isPaused", False),
        "period_start":  state.get("periodStartTime"),
        "source":        "sportpesa",
        "mode":          "live",
        "harvested_at":  _now_ts(),
    }


def _build_live_match(ev: dict, markets: dict[str, dict[str, float]]) -> dict:
    base = _parse_event_state(ev)
    base["markets"]      = markets
    base["market_count"] = len(markets)
    return base


# =============================================================================
# Streaming generators (for snapshot-canonical SSE endpoint)
# =============================================================================

def fetch_live_stream(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.2,
    batch_size:         int   = 12,
    **_,
) -> Generator[dict, None, None]:
    sport_id = _SLUG_TO_SPORT_ID.get(sport_slug.lower())
    if not sport_id:
        log.warning("[live:stream] unknown sport: %s", sport_slug)
        return
    events = fetch_live_events(sport_id, limit=200)
    if not events:
        return
    ev_index = {str(ev.get("id")): ev for ev in events if ev.get("id")}
    event_ids = list(ev_index.keys())

    if not fetch_full_markets:
        for ev_id, ev in ev_index.items():
            yield _build_live_match(ev, {})
        return

    for i in range(0, len(event_ids), batch_size):
        batch_ids = event_ids[i: i + batch_size]
        all_mkts  = fetch_all_market_types_for_events(batch_ids, sport_id)
        for ev_id in batch_ids:
            ev      = ev_index.get(ev_id, {})
            markets = all_mkts.get(ev_id, {})
            yield _build_live_match(ev, markets)
        if i + batch_size < len(event_ids):
            time.sleep(sleep_between)


def fetch_live(
    sport_slug: str, fetch_full_markets: bool = True, **_,
) -> list[dict]:
    return list(fetch_live_stream(sport_slug, fetch_full_markets=fetch_full_markets))


# =============================================================================
# Diff-and-record (WS BUFFERED_MARKET_UPDATE)
# =============================================================================

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


# =============================================================================
# WS message builders
# =============================================================================

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
    norm_sels = [
        {
            "id":          sel.get("id"),
            "name":        sel.get("name", ""),
            "outcome_key": _normalise_outcome_key(
                sel.get("name", ""), idx, market_type, sport_id,
            ),
            "odds":        str(sel.get("odds", "0")),
            "status":      sel.get("status", "Open"),
        }
        for idx, sel in enumerate(all_sels)
    ]
    return {
        "type":                  "market_update",
        "event_id":              event_id,
        "sport_id":              sport_id,
        "market_id":             msg.get("eventMarketId"),
        "market_type":           market_type,
        "market_name":           msg.get("name", ""),
        "market_slug":           slug,
        "handicap":              handicap,
        "market_status":         msg.get("status", "Open"),
        "all_selections":        all_sels,
        "normalised_selections": norm_sels,
        "changed_selections":    [],
        "ts":                    _now_ts(),
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


def _parse_ws_frame(raw: str) -> tuple[str | None, Any]:
    if raw.startswith("42"):
        try:
            payload = json.loads(raw[2:])
            if isinstance(payload, list) and payload:
                return str(payload[0]), payload[1] if len(payload) > 1 else None
        except (json.JSONDecodeError, IndexError):
            pass
    return None, None


# =============================================================================
# WS-3: 1-SECOND REPUBLISHER THREAD
#
# Reads current state + odds from Redis every second and re-publishes
# them to the sport/event SSE channels.  Even if the WS is silent
# (no new odds), the frontend receives a frame every ≤1 s confirming
# the odds are still current.
# =============================================================================

_REPUBLISHER_STOP   = threading.Event()
_REPUBLISHER_THREAD: threading.Thread | None = None

# Shared state registry written by WS thread, read by republisher
_live_registry_lock = threading.Lock()
_live_registry: dict[int, dict] = {}
# {event_id: {sport_id, state_payload, market_payloads: [...]}}


def _update_registry(event_id: int, sport_id: int,
                     state: dict | None = None,
                     market: dict | None = None) -> None:
    with _live_registry_lock:
        entry = _live_registry.setdefault(event_id, {
            "sport_id": sport_id,
            "state":    None,
            "markets":  {},
        })
        if state is not None:
            entry["state"] = state
            entry["sport_id"] = sport_id
        if market is not None:
            slug = market.get("market_slug", market.get("market_type", "?"))
            entry["markets"][slug] = market
            entry["sport_id"] = sport_id


def _republisher_loop() -> None:
    """Publish current state + all markets for every tracked event every 1 s."""
    r   = _get_redis()
    log.info("[republisher] started — publishing every 1 s")

    while not _REPUBLISHER_STOP.is_set():
        try:
            with _live_registry_lock:
                snapshot = {
                    eid: {
                        "sport_id": e["sport_id"],
                        "state":    e.get("state"),
                        "markets":  dict(e.get("markets", {})),
                    }
                    for eid, e in _live_registry.items()
                }

            for event_id, entry in snapshot.items():
                sport_id = entry["sport_id"]

                # Re-publish current state (even if unchanged — clients
                # use this as a heartbeat to confirm match is still live)
                if entry["state"]:
                    st = dict(entry["state"])
                    st["ts"] = _now_ts()  # refresh timestamp
                    st["republished"] = True
                    try:
                        pipe = r.pipeline()
                        pipe.publish(_ch_sport(sport_id), json.dumps(st))
                        pipe.publish(_ch_event(event_id), json.dumps(st))
                        pipe.execute()
                    except Exception:
                        pass

                # Re-publish most recent market for every slug we've seen
                for slug, mkt in entry["markets"].items():
                    mkt_out = dict(mkt)
                    mkt_out["ts"] = _now_ts()
                    mkt_out["republished"] = True
                    try:
                        pipe = r.pipeline()
                        pipe.publish(_ch_sport(sport_id), json.dumps(mkt_out))
                        pipe.publish(_ch_event(event_id), json.dumps(mkt_out))
                        pipe.execute()
                    except Exception:
                        pass

        except Exception as exc:
            log.warning("[republisher] error: %s", exc)

        _REPUBLISHER_STOP.wait(1.0)  # ← 1-second cadence

    log.info("[republisher] stopped")


# =============================================================================
# Sportradar timeline poller (unchanged from v6)
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

_sr_event_map:   dict[int, int] = {}   # event_id → external_id
_sr_event_sport: dict[int, int] = {}   # event_id → sport_id
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
            base_sec   = _PERIOD_BASE_SEC.get(p, 0)
            elapsed    = int(time.time()) - int(ptime)
            total      = base_sec + (elapsed if running else 0)
            mm, ss     = divmod(max(0, total), 60)
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
                _update_registry(event_id, sport_id, state=payload)
            except Exception as exc:
                log.debug("[SR] event=%d: %s", event_id, exc)
            _SR_STOP.wait(0.05)
            if _SR_STOP.is_set():
                break
        _SR_STOP.wait(5)
    log.info("[SR] timeline polling stopped")


# =============================================================================
# WS HARVESTER LOOP (WS-1 through WS-5)
# =============================================================================

_HARVESTER_THREAD: threading.Thread | None = None
_HARVESTER_STOP   = threading.Event()

_WS_BASE = "wss://realtime-notificator.ke.sportpesa.com"
_WS_URL  = f"{_WS_BASE}/socket.io/?EIO=3&transport=websocket"


def _ws_extra_headers() -> list[tuple[str, str]]:
    return [
        ("Origin",          _SR_ORIGIN),
        ("User-Agent",      _HEADERS["User-Agent"]),
        ("Referer",         _SR_ORIGIN + "/"),
        ("Accept-Language", "en-GB,en-US;q=0.9,en;q=0.8"),
    ]


def _harvester_loop() -> None:
    try:
        import websocket
    except ImportError:
        log.error("websocket-client not installed — pip install websocket-client")
        return

    r = _get_redis()

    while not _HARVESTER_STOP.is_set():

        sport_id_map:    dict[int, int] = {}   # event_id → sport_id
        external_id_map: dict[int, int] = {}   # event_id → external_id
        subscribed:      set[int]       = set()

        # ── 1. Snapshot all live events ───────────────────────────────────────
        log.info("[WS] fetching live event list …")
        for sport_id in _LIVE_SPORT_IDS:
            try:
                events = fetch_live_events(sport_id, limit=200)
                for ev in events:
                    ev_id  = ev.get("id")
                    ext_id = int(ev.get("externalId") or ev.get("betradarId") or 0)
                    if ev_id:
                        sport_id_map[ev_id]    = sport_id
                        external_id_map[ev_id] = ext_id
            except Exception as exc:
                log.warning("[WS] snapshot sport=%d: %s", sport_id, exc)

        with _sr_map_lock:
            _sr_event_map.clear()
            _sr_event_map.update(external_id_map)
            _sr_event_sport.clear()
            _sr_event_sport.update(sport_id_map)

        log.info("[WS] tracking %d live events across %d sports",
                 len(sport_id_map), len(set(sport_id_map.values())))

        if not sport_id_map:
            log.info("[WS] no live events — sleeping 30 s")
            _HARVESTER_STOP.wait(30)
            continue

        ws_connected = threading.Event()

        def on_open(ws):
            log.info("[WS] connected — subscribing %d events", len(sport_id_map))
            ws_connected.set()
            # WS-1: subscribe ALL market channels per event
            for ev_id, sport_id in sport_id_map.items():
                _subscribe_event(ws, ev_id, sport_id)
                subscribed.add(ev_id)
            log.info("[WS] all subscriptions sent (%d events × up to %d channels)",
                     len(subscribed), len(_WS_CHANNELS_PRIMARY))

        def on_message(ws, raw: str):
            try:
                # WS-2: Engine.io ping/pong
                if raw == "2":
                    ws.send("3")
                    return
                if raw == "31":
                    # server heartbeat ACK — no reply needed for EIO=3 transport
                    return

                event_name, data = _parse_ws_frame(raw)
                if not event_name or not data:
                    return

                # ── BUFFERED_MARKET_UPDATE ────────────────────────────────────
                if event_name == "BUFFERED_MARKET_UPDATE":
                    payload = _market_from_buffered_update(data, sport_id_map)
                    if not payload:
                        return

                    # WS-4: publish IMMEDIATELY before writing to Redis
                    event_id = payload["event_id"]
                    sport_id = payload["sport_id"]
                    _publish_live_update(r, sport_id, event_id, payload)

                    # Then diff + record
                    payload = _diff_and_record(r, payload)
                    _record_market_snapshot(r, event_id, payload)

                    # Update 1-s republisher registry
                    _update_registry(event_id, sport_id, market=payload)

                # ── EVENT_UPDATE ─────────────────────────────────────────────
                elif event_name == "EVENT_UPDATE":
                    event_id = data.get("id")
                    if not event_id:
                        return
                    sport_id = data.get("sportId") or sport_id_map.get(event_id, 1)
                    payload  = _state_from_event_update(data)

                    # WS-4: publish IMMEDIATELY
                    _publish_live_update(r, sport_id, event_id, payload)

                    _cache_event_state(r, payload)
                    _record_state_snapshot(r, event_id, payload)

                    # Update registry for republisher
                    _update_registry(event_id, sport_id, state=payload)

                    # Update maps
                    if event_id not in sport_id_map:
                        sport_id_map[event_id] = sport_id

                    # WS-5: auto-subscribe new events we haven't seen yet
                    if event_id not in subscribed:
                        _subscribe_event(ws, event_id, sport_id)
                        subscribed.add(event_id)
                        log.info("[WS] auto-subscribed new event %d", event_id)

            except Exception as exc:
                log.warning("[WS:on_message] %s", exc)

        def on_error(ws, error):
            log.warning("[WS] error: %s", error)

        def on_close(ws, code, msg):
            log.info("[WS] closed code=%s", code)
            ws_connected.clear()

        ws = None
        try:
            import websocket as _ws_lib
            ws = _ws_lib.WebSocketApp(
                _WS_URL,
                header=[f"{k}: {v}" for k, v in _ws_extra_headers()],
                on_open    = on_open,
                on_message = on_message,
                on_error   = on_error,
                on_close   = on_close,
            )
        except Exception as exc:
            log.error("[WS] init failed: %s", exc)
            _HARVESTER_STOP.wait(10)
            continue

        ws_thread = threading.Thread(
            target=ws.run_forever,
            kwargs={"ping_interval": 0},  # we handle keep-alive manually (WS-2)
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

            # WS-2: send keep-alive "21" every 20 s (matches server pingInterval)
            if now_m - last_ping >= 20:
                try:
                    ws.send("21")
                except Exception:
                    break
                last_ping = now_m

            # Refresh event list every 60 s and subscribe any new events
            if now_m - last_refresh >= 60:
                for sport_id in _LIVE_SPORT_IDS:
                    try:
                        events = fetch_live_events(sport_id, limit=200)
                        for ev in events:
                            ev_id  = ev.get("id")
                            ext_id = int(ev.get("externalId") or ev.get("betradarId") or 0)
                            if not ev_id:
                                continue
                            sport_id_map[ev_id]    = sport_id
                            external_id_map[ev_id] = ext_id
                            if ev_id not in subscribed:
                                _subscribe_event(ws, ev_id, sport_id)
                                subscribed.add(ev_id)
                                log.info("[WS] new event subscribed: %d", ev_id)
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
# Snapshot helpers (unchanged from v6)
# =============================================================================

def _snapshot_sport(sport_id: int) -> list[dict]:
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
        except Exception as exc:
            log.warning("[snapshot] sport=%d error: %s", sport_id, exc)
    return result


# =============================================================================
# Thread management
# =============================================================================

def harvester_alive() -> bool:
    return _HARVESTER_THREAD is not None and _HARVESTER_THREAD.is_alive()


def republisher_alive() -> bool:
    return _REPUBLISHER_THREAD is not None and _REPUBLISHER_THREAD.is_alive()


def start_harvester_thread() -> threading.Thread:
    global _HARVESTER_THREAD, _HARVESTER_STOP, _REPUBLISHER_THREAD, _REPUBLISHER_STOP

    # Start WS thread
    if not harvester_alive():
        _HARVESTER_STOP.clear()
        _HARVESTER_THREAD = threading.Thread(
            target=_harvester_loop, name="sp-live-ws", daemon=True,
        )
        _HARVESTER_THREAD.start()
        log.info("[harvester] WS thread started")

    # WS-3: Start 1-second republisher
    if not republisher_alive():
        _REPUBLISHER_STOP.clear()
        _REPUBLISHER_THREAD = threading.Thread(
            target=_republisher_loop, name="sp-live-republisher", daemon=True,
        )
        _REPUBLISHER_THREAD.start()
        log.info("[harvester] republisher thread started")

    # Start Sportradar poller
    _start_sr_thread()

    return _HARVESTER_THREAD  # type: ignore[return-value]


def stop_harvester() -> None:
    global _HARVESTER_THREAD, _REPUBLISHER_THREAD
    _HARVESTER_STOP.set()
    _REPUBLISHER_STOP.set()
    _stop_sr_thread()
    for t in [_HARVESTER_THREAD, _REPUBLISHER_THREAD]:
        if t and t.is_alive():
            t.join(timeout=5)
    _HARVESTER_THREAD     = None
    _REPUBLISHER_THREAD   = None
    log.info("[harvester] all threads stopped")


def _start_sr_thread() -> None:
    global _SR_THREAD
    if _SR_THREAD and _SR_THREAD.is_alive():
        return
    _SR_STOP.clear()
    _SR_THREAD = threading.Thread(
        target=_sr_poll_loop, name="sp-live-sr-poll", daemon=True,
    )
    _SR_THREAD.start()


def _stop_sr_thread() -> None:
    _SR_STOP.set()
    if _SR_THREAD and _SR_THREAD.is_alive():
        _SR_THREAD.join(timeout=5)


# =============================================================================
# Public exports
# =============================================================================

__all__ = [
    "fetch_live_stream",
    "fetch_live",
    "fetch_live_sports",
    "fetch_live_events",
    "fetch_live_markets",
    "fetch_live_markets_for_events",
    "fetch_all_market_types_for_events",
    "get_odds_history",
    "get_market_snapshot_history",
    "get_state_snapshot_history",
    "snapshot_all_sports",
    "start_harvester_thread",
    "stop_harvester",
    "harvester_alive",
    "republisher_alive",
]