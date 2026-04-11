"""
app/workers/sp_live_harvester.py
=================================
Sportpesa Live WebSocket harvester.

CHANGES v2 — Cross-bookmaker live enrichment
─────────────────────────────────────────────────────────────────────────────
Every SP WS event (MARKET_UPDATE or EVENT_UPDATE) now triggers:
  1. LiveCrossBkUpdater.trigger()  — immediately fetches BT + OD in parallel
     then retries 5× at 1 s intervals (only re-publishes if data changed).
  2. All bookmaker data is written to BookmakerMatchOdds + LiveRawSnapshot.
  3. Published to Redis pub/sub channels keyed by internal match_id:
       live:match:{id}:markets   — odds changes
       live:match:{id}:events    — score / phase changes
       live:match:{id}:all       — merged feed
       live:matches:{sport_slug} — list-level refresh hint
       live:sports               — sport count updates
       live:all                  — global broadcast

BUGS FIXED (original)
─────────────────────────────────────────────────────────────────────────────
1. CRITICAL: was listening for "BUFFERED_MARKET_UPDATE" — actual event name
   from SP is "MARKET_UPDATE".
2. Live market type IDs differ from upcoming /api/games/markets IDs.
3. Live selection names are FULL names, not shortNames.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import re
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any

import requests
import websocket

# ═════════════════════════════════════════════════════════════════════════════
# DIAGNOSTIC LOGGER
# ═════════════════════════════════════════════════════════════════════════════

def _setup_diag_logger() -> logging.Logger:
    diag = logging.getLogger("sp_live_diag")
    if diag.handlers:
        return diag
    diag.setLevel(logging.DEBUG)
    diag.propagate = False
    log_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "logs",
    )
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "sp_live_debug.log")
    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
    ))
    diag.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter("[sp_live] %(message)s"))
    diag.addHandler(sh)
    diag.info("=" * 70)
    diag.info("sp_live_harvester v2 — log: %s", log_path)
    diag.info("=" * 70)
    return diag


_diag = _setup_diag_logger()
log   = logging.getLogger("sp_live")


def _D(msg: str, *args, level: str = "debug") -> None:
    getattr(_diag, level)(msg, *args)
    getattr(log,   level)(msg, *args)


def _D_section(title: str) -> None:
    _diag.info("")
    _diag.info("─── %s %s", title, "─" * max(0, 60 - len(title)))

# ═════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═════════════════════════════════════════════════════════════════════════════

WS_URL     = "wss://realtime-notificator.ke.sportpesa.com/socket.io/?EIO=3&transport=websocket"
API_BASE   = "https://www.ke.sportpesa.com/api/live"
ORIGIN     = "https://www.ke.sportpesa.com"
USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
)

DEFAULT_MARKET_TYPE = 194

LIVE_SPORT_IDS = [1, 2, 4, 5, 8, 9, 10, 13]
SPORT_SLUG_MAP = {
    1:  "soccer",   2: "basketball", 4: "tennis",
    5:  "handball", 8: "rugby",      9: "cricket",
    10: "volleyball", 13: "table-tennis",
}

LIVE_MARKET_MAP: dict[int, tuple[str, bool]] = {
    194: ("1x2",                     False),
    149: ("match_winner",            False),
    147: ("double_chance",           False),
    138: ("btts",                    False),
    140: ("first_half_btts",         False),
    145: ("odd_even",                False),
    166: ("draw_no_bet",             False),
    151: ("european_handicap",       True),
    184: ("asian_handicap",          True),
    105: ("__ou__",                  True),
    183: ("correct_score",           False),
    154: ("exact_goals",             False),
    129: ("first_team_to_score",     False),
    155: ("highest_scoring_half",    False),
    135: ("first_half_1x2",          False),
    303: ("match_winner",            False),
    304: ("match_winner",            False),
}

LIVE_OU_SLUG: dict[int, str] = {
    1:  "over_under_goals",
    5:  "over_under_goals",
    2:  "total_points",
    8:  "total_points",
    4:  "total_games",
    13: "total_games",
    10: "total_sets",
    9:  "total_runs",
}


def live_market_slug(mkt_type: int, handicap: Any, sport_id: int) -> str:
    entry = LIVE_MARKET_MAP.get(mkt_type)
    if not entry:
        return f"market_{mkt_type}"
    base, uses_line = entry
    if base == "__ou__":
        base = LIVE_OU_SLUG.get(sport_id, "over_under")
    if uses_line and handicap is not None:
        try:
            f = float(handicap)
            line = str(int(f)) if f == int(f) else str(f)
        except (TypeError, ValueError):
            line = str(handicap)
        if line and line != "0":
            return f"{base}_{line}"
    return base

# ═════════════════════════════════════════════════════════════════════════════
# LIVE OUTCOME NORMALIZER
# ═════════════════════════════════════════════════════════════════════════════

_OVER_RE  = re.compile(r"^over\s+[\d.]+$",  re.I)
_UNDER_RE = re.compile(r"^under\s+[\d.]+$", re.I)
_SCORE_RE = re.compile(r"^\d+:\d+$")


def normalize_live_outcome(
    slug:      str,
    sel_name:  str,
    sel_index: int,
    all_sels:  list[dict],
) -> str:
    kl = sel_name.strip().lower()
    if _OVER_RE.match(kl):  return "over"
    if _UNDER_RE.match(kl): return "under"
    exact = {
        "yes": "yes", "no": "no",
        "odd": "odd", "even": "even",
        "1": "1", "x": "X", "2": "2",
        "draw": "X",
        "1x": "1X", "x2": "X2", "12": "12",
        "1st": "1st", "2nd": "2nd",
        "equal": "equal", "eql": "equal",
        "none": "none",
    }
    if kl in exact:
        return exact[kl]
    raw = sel_name.strip()
    if _SCORE_RE.match(raw):
        return raw
    if kl.startswith("draw"):
        return "X"
    if " or " in kl:
        n = len(all_sels)
        dc_map = {0: "1X", 1: "X2", 2: "12"}
        return dc_map.get(sel_index, f"dc_{sel_index}")
    if "(" in kl:
        pos_map = {0: "1", 1: "X", 2: "2"}
        if len(all_sels) == 2:
            return "1" if sel_index == 0 else "2"
        return pos_map.get(sel_index, str(sel_index + 1))
    if kl in ("other", "othr", "any other"):
        return "other"
    if re.match(r"^\d+\+?$", raw):
        return raw
    if len(all_sels) == 2:
        return "1" if sel_index == 0 else "2"
    if len(all_sels) == 3:
        pos_map = {0: "1", 1: "X", 2: "2"}
        return pos_map.get(sel_index, kl[:8])
    return re.sub(r"[^a-z0-9_:+./\-]+", "_", kl).strip("_") or f"sel_{sel_index}"

# ═════════════════════════════════════════════════════════════════════════════
# REDIS
# ═════════════════════════════════════════════════════════════════════════════

_redis_client = None


def _get_redis():
    global _redis_client
    if _redis_client is None:
        import redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _D("Redis: connecting to %s", url, level="info")
        try:
            _redis_client = redis.from_url(url, decode_responses=True)
            _D("Redis: PING → %s ✓", _redis_client.ping(), level="info")
        except Exception as exc:
            _D("Redis: CONNECT FAILED — %s", exc, level="error")
            raise
    return _redis_client

# ═════════════════════════════════════════════════════════════════════════════
# HTTP SESSION
# ═════════════════════════════════════════════════════════════════════════════

_SESSION: requests.Session | None = None

CH_ALL   = "sp:live:all"
CH_SPORT = "sp:live:sport:{sport_id}"
CH_EVENT = "sp:live:event:{event_id}"
TTL_ODDS = 7200; TTL_STATE = 3600; TTL_EVENTS = 1800; TTL_SNAPSHOT = 300


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({
            "Origin": ORIGIN, "Referer": ORIGIN + "/", "User-Agent": USER_AGENT,
        })
    return _SESSION


def _get(path: str, params: dict | None = None, timeout: int = 10) -> Any:
    url = f"{API_BASE}{path}"
    _D("HTTP GET %s params=%s", url, params)
    t0 = time.perf_counter()
    try:
        r = _session().get(url, params=params, timeout=timeout)
        ms = int((time.perf_counter() - t0) * 1000)
        _D("HTTP %s → status=%d  %dms  body_len=%d", url, r.status_code, ms, len(r.content))
        if not r.ok:
            _D("HTTP ERROR %d: %s", r.status_code, r.text[:300], level="warning")
            return None
        data = r.json()
        return data
    except Exception as exc:
        _D("HTTP EXCEPTION %s: %s", url, exc, level="error")
        return None

# ═════════════════════════════════════════════════════════════════════════════
# PUBLIC HTTP HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def fetch_live_sports() -> list[dict]:
    _D_section("fetch_live_sports")
    data = _get("/sports")
    if not data:
        return []
    sports = data.get("sports") or []
    _D("fetch_live_sports: %d sports", len(sports), level="info")
    return sports


def fetch_live_events(sport_id: int, limit: int = 50, offset: int = 0) -> list[dict]:
    _D_section(f"fetch_live_events(sport_id={sport_id})")
    data = _get(f"/sports/{sport_id}/events", {"limit": limit, "offset": offset})
    if not data:
        return []
    if isinstance(data, list):
        events = data
    else:
        events = data.get("events") or data.get("data") or []
    events = [e for e in events if isinstance(e, dict)]
    _D("fetch_live_events sport=%d: %d events", sport_id, len(events), level="info")
    return events


def fetch_live_default_markets(sport_id: int) -> list[dict]:
    _D_section(f"fetch_live_default_markets(sport_id={sport_id})")
    data = _get("/default/markets", {"sportId": sport_id})
    if not data:
        return []
    if isinstance(data, list):
        items = data
    else:
        items = data.get("markets") or []
    items = [i for i in items if isinstance(i, dict)]
    _D("fetch_live_default_markets sport=%d: %d bundles", sport_id, len(items), level="info")
    return items


def fetch_live_markets(
    event_ids:   list[int],
    sport_id:    int,
    market_type: int = DEFAULT_MARKET_TYPE,
) -> list[dict]:
    if not event_ids:
        return []
    ids_str = ",".join(str(i) for i in event_ids[:15])
    data = _get("/event/markets", {
        "eventId": ids_str, "type": market_type, "sportId": sport_id,
    })
    if not data:
        return []
    return data.get("markets") or data.get("data") or []


def _get_quiet(path: str, params: dict | None = None, timeout: int = 10) -> Any:
    url = f"{API_BASE}{path}"
    try:
        r = _session().get(url, params=params, timeout=timeout)
        if r.status_code == 404:
            return None
        if not r.ok:
            return None
        return r.json()
    except Exception:
        return None


def fetch_event_details(event_id: int) -> dict | None:
    data = _get_quiet(f"/events/{event_id}/details")
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return None


def snapshot_all_sports() -> dict[int, list[dict]]:
    _D_section("snapshot_all_sports")
    r      = _get_redis()
    sports = fetch_live_sports()
    result: dict[int, list[dict]] = {}
    if not sports:
        _D("snapshot_all_sports: 0 sports", level="error")
        return result
    for sport in sports:
        sport_id   = sport["id"]
        sport_slug = SPORT_SLUG_MAP.get(sport_id, f"sport_{sport_id}")
        events = fetch_live_events(sport_id, limit=100)
        if not events:
            result[sport_id] = []
            continue
        pipe = r.pipeline()
        for ev in events:
            pipe.setex(f"sp:live:event_sport:{ev['id']}", TTL_EVENTS, sport_id)
            pipe.setex(f"sp:live:event_slug:{ev['id']}",  TTL_EVENTS, sport_slug)
        pipe.execute()
        default_mkts = fetch_live_default_markets(sport_id)
        snapshot = {
            "sport_id":    sport_id,
            "sport_slug":  sport_slug,
            "sport_name":  sport.get("name", sport_slug),
            "event_count": sport.get("eventNumber", len(events)),
            "events":      events,
            "markets":     default_mkts,
            "fetched_at":  _now_iso(),
        }
        r.setex(f"sp:live:snapshot:{sport_id}", TTL_SNAPSHOT, json.dumps(snapshot))
        result[sport_id] = events
        _D("snapshot sport_id=%d: %d events → Redis", sport_id, len(events), level="info")
    r.setex("sp:live:sports", TTL_EVENTS, json.dumps(sports))
    return result

# ═════════════════════════════════════════════════════════════════════════════
# UTILS
# ═════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ═════════════════════════════════════════════════════════════════════════════
# ODDS DELTA + HISTORY  (SP internal WS diff)
# ═════════════════════════════════════════════════════════════════════════════

def _upsert_odds_and_diff(event_id: int, market: dict) -> tuple[list[dict], list[dict]]:
    r          = _get_redis()
    market_id  = market["eventMarketId"]
    selections = market.get("selections") or []
    snap_key = f"sp:live:odds:{event_id}:{market_id}"
    hist_key = f"sp:live:odds_history:{event_id}:{market_id}"
    prev_raw = r.get(snap_key)
    prev     = json.loads(prev_raw) if prev_raw else {}
    current  = {str(s["id"]): s.get("odds", "0") for s in selections}
    changed_ids  = {sid for sid, odds in current.items() if prev.get(sid) != odds}
    changed_sels = [s for s in selections if str(s["id"]) in changed_ids]
    if not changed_ids:
        return [], selections
    r.setex(snap_key, TTL_ODDS, json.dumps(current))
    tick = {"ts": _now_iso(), "odds": {str(s["id"]): s.get("odds") for s in changed_sels}}
    pipe = r.pipeline()
    pipe.lpush(hist_key, json.dumps(tick))
    pipe.ltrim(hist_key, 0, 49)
    pipe.expire(hist_key, TTL_ODDS)
    pipe.execute()
    return changed_sels, selections


def get_odds_history(event_id: int, market_id: int, limit: int = 20) -> list[dict]:
    r   = _get_redis()
    raw = r.lrange(f"sp:live:odds_history:{event_id}:{market_id}", 0, limit - 1)
    return [json.loads(x) for x in raw]

# ═════════════════════════════════════════════════════════════════════════════
# SP-INTERNAL PUB/SUB  (legacy channels — kept for compatibility)
# ═════════════════════════════════════════════════════════════════════════════

def _sp_publish(sport_id: int | None, event_id: int, payload: dict) -> None:
    r   = _get_redis()
    msg = json.dumps(payload, ensure_ascii=False)
    pipe = r.pipeline()
    pipe.publish(CH_ALL, msg)
    pipe.publish(CH_EVENT.format(event_id=event_id), msg)
    if sport_id:
        pipe.publish(CH_SPORT.format(sport_id=sport_id), msg)
    pipe.execute()


def _event_sport_id(event_id: int) -> int | None:
    r   = _get_redis()
    val = r.get(f"sp:live:event_sport:{event_id}")
    return int(val) if val else None


def _event_betradar_id(event_id: int) -> str | None:
    """
    Resolve SP event_id → betradar_id (parent_match_id).
    SP's event_id is NOT the betradar_id — we look it up from the DB.
    Cached in Redis for TTL_EVENTS seconds.
    """
    key = f"sp:live:event_br:{event_id}"
    r = _get_redis()
    cached = r.get(key)
    if cached:
        return cached
    try:
        from app.models.bookmakers_model import BookmakerMatchLink, Bookmaker
        from app.models.odds_model import UnifiedMatch
        from sqlalchemy import func
        # SP stores event_id in the external_match_id field of BookmakerMatchLink
        bk = Bookmaker.query.filter(
            func.lower(Bookmaker.name).in_(["sportpesa", "sport pesa"])
        ).first()
        if bk:
            link = BookmakerMatchLink.query.filter_by(
                bookmaker_id=bk.id, external_match_id=str(event_id)
            ).first()
            if link:
                um = UnifiedMatch.query.get(link.match_id)
                if um and um.parent_match_id:
                    r.setex(key, TTL_EVENTS, um.parent_match_id)
                    return um.parent_match_id
    except Exception as exc:
        _D("event_betradar_id(%s): %s", event_id, exc)
    return None

# ═════════════════════════════════════════════════════════════════════════════
# MESSAGE HANDLERS
# ═════════════════════════════════════════════════════════════════════════════

_msg_counter = 0


def _handle_market_update(data: dict) -> None:
    global _msg_counter
    _msg_counter += 1

    event_id   = data.get("eventId")
    market_id  = data.get("eventMarketId")
    mkt_type   = data.get("type")
    handicap   = data.get("handicap")
    mkt_name   = data.get("name", "?")
    status     = data.get("status")
    selections = data.get("selections") or []

    _D("MARKET_UPDATE #%d event=%s market=%s type=%s name=%r status=%s sels=%d",
       _msg_counter, event_id, market_id, mkt_type, mkt_name, status, len(selections))

    if not event_id or not market_id:
        return

    if status in ("Closed", "Suspended") and not selections:
        return

    changed_sels, all_sels = _upsert_odds_and_diff(event_id, data)
    if not changed_sels:
        return

    sport_id = _event_sport_id(event_id)
    slug     = live_market_slug(mkt_type, handicap, sport_id or 1)

    normalised_sels = []
    for idx, sel in enumerate(all_sels):
        if sel.get("status") == "Suspended":
            continue
        try:
            odd = float(sel.get("odds") or "0")
        except (TypeError, ValueError):
            odd = 0.0
        if odd <= 1.0:
            continue
        out_key = normalize_live_outcome(slug, sel.get("name", ""), idx, all_sels)
        normalised_sels.append({
            "id":          sel.get("id"),
            "name":        sel.get("name"),
            "outcome_key": out_key,
            "odds":        sel.get("odds"),
            "status":      sel.get("status"),
        })

    _D("  → slug=%r  %d/%d sels changed  normalised=%d",
       slug, len(changed_sels), len(all_sels), len(normalised_sels))

    payload = {
        "type":                  "market_update",
        "event_id":              event_id,
        "market_id":             market_id,
        "sport_id":              sport_id,
        "sport_slug":            SPORT_SLUG_MAP.get(sport_id) if sport_id else None,
        "market_name":           mkt_name,
        "market_type":           mkt_type,
        "market_slug":           slug,
        "handicap":              handicap,
        "status":                status,
        "template":              data.get("template"),
        "sequence":              data.get("sequence"),
        "changed_count":         len(changed_sels),
        "changed_selections":    changed_sels,
        "all_selections":        all_sels,
        "normalised_selections": normalised_sels,
        "ts":                    _now_iso(),
    }

    # ── Legacy SP-internal pub/sub (kept for existing consumers) ─────────────
    _sp_publish(sport_id, event_id, payload)

    # ── Cross-BK enrichment  ──────────────────────────────────────────────────
    # Resolve betradar_id so we can fetch BT + OD by the same key
    betradar_id = _event_betradar_id(event_id) or str(event_id)
    try:
        from app.workers.live_cross_bk_updater import get_updater
        get_updater().trigger(
            betradar_id = betradar_id,
            sp_payload  = payload,
            sport_id    = sport_id,
            kind        = "market_update",
        )
        from app.workers.live_broadcaster import broadcast_market_odds
        # Format the outcomes into a simple { "1": 2.50, "X": 3.10 } dictionary
        formatted_outcomes = {sel["outcome_key"]: float(sel["odds"]) for sel in normalised_sels}
        broadcast_market_odds(betradar_id, "sp", slug, formatted_outcomes)
    except Exception as exc:
        _D("cross_bk trigger market_update: %s", exc, level="warning")



def _handle_event_update(data: dict) -> None:
    global _msg_counter
    _msg_counter += 1

    event_id = data.get("id")
    sport_id = data.get("sportId")
    state    = data.get("state") or {}
    score    = state.get("matchScore") or {}
    phase    = state.get("currentEventPhase", "?")

    _D("EVENT_UPDATE #%d event=%s sport=%s phase=%r score=%s-%s",
       _msg_counter, event_id, sport_id, phase,
       score.get("home", "?"), score.get("away", "?"))

    if not event_id:
        return

    r = _get_redis()
    if sport_id:
        r.setex(f"sp:live:event_sport:{event_id}", TTL_EVENTS, sport_id)
    r.setex(f"sp:live:state:{event_id}", TTL_STATE, json.dumps(data))

    payload = {
        "type":          "event_update",
        "event_id":      event_id,
        "sport_id":      sport_id,
        "sport_slug":    SPORT_SLUG_MAP.get(sport_id) if sport_id else None,
        "status":        data.get("status"),
        "is_paused":     data.get("isPaused"),
        "phase":         phase,
        "clock_running": state.get("clockRunning"),
        "remaining_ms":  state.get("remainingTimeMillis"),
        "score_home":    score.get("home"),
        "score_away":    score.get("away"),
        "state":         state,
        "ts":            _now_iso(),
    }

    # ── Legacy SP-internal pub/sub ────────────────────────────────────────────
    _sp_publish(sport_id, event_id, payload)

    # ── Cross-BK enrichment ───────────────────────────────────────────────────
    betradar_id = _event_betradar_id(event_id) or str(event_id)
    try:
        from app.workers.live_cross_bk_updater import get_updater
        get_updater().trigger(
            betradar_id = betradar_id,
            sp_payload  = payload,
            sport_id    = sport_id,
            kind        = "event_update",
        )
    except Exception as exc:
        _D("cross_bk trigger event_update: %s", exc, level="warning")

# ═════════════════════════════════════════════════════════════════════════════
# WEBSOCKET HARVESTER
# ═════════════════════════════════════════════════════════════════════════════

class SpLiveHarvester:

    def __init__(self) -> None:
        self._ws:            websocket.WebSocketApp | None = None
        self._stop:          threading.Event = threading.Event()
        self._ping_thread:   threading.Thread | None = None
        self._ping_interval: float = 20.0
        self._connected:     bool = False
        self._lock:          threading.Lock = threading.Lock()
        self._connect_count: int = 0

    def _on_open(self, ws) -> None:
        self._connected = True
        _D("WS OPEN (attempt #%d)", self._connect_count, level="info")
        self._ping_thread = threading.Thread(
            target=self._ping_loop, daemon=True, name="sp-live-ping",
        )
        self._ping_thread.start()

    def _on_message(self, ws, raw: str) -> None:
        if raw == "2":
            with self._lock:
                if self._ws:
                    self._ws.send("3")
            return
        if raw.startswith("0"):
            try:
                hs = json.loads(raw[1:])
                self._ping_interval = hs.get("pingInterval", 20000) / 1000
                _D("WS HANDSHAKE sid=%s pingInterval=%.1fs",
                   hs.get("sid"), self._ping_interval, level="info")
            except Exception as exc:
                _D("WS HANDSHAKE parse error: %s  raw=%r", exc, raw[:200], level="warning")
            return
        if raw == "40":
            _D("WS Socket.IO CONNECT ack → subscribing sports", level="info")
            self._subscribe_sports()
            return
        if raw.startswith("44"):
            _D("WS ERROR frame: %s", raw[:300], level="error")
            return
        if raw.startswith("42"):
            try:
                parts = json.loads(raw[2:])
                name  = parts[0]
                data  = parts[1] if len(parts) > 1 else {}
                if name == "MARKET_UPDATE":
                    _handle_market_update(data)
                elif name == "EVENT_UPDATE":
                    _handle_event_update(data)
                elif name == "BUFFERED_MARKET_UPDATE":
                    _D("WS BUFFERED_MARKET_UPDATE (legacy) → routing to handler")
                    _handle_market_update(data)
                else:
                    _D("WS EVENT name=%r data_keys=%s",
                       name, list(data.keys())[:8] if isinstance(data, dict) else type(data))
            except Exception as exc:
                _D("WS MESSAGE parse error: %s\nraw: %.400s\n%s",
                   exc, raw, traceback.format_exc(), level="error")
            return
        if raw == "3":
            return
        _D("WS RECV unknown frame (len=%d): %s", len(raw), raw[:100])

    def _on_error(self, ws, error) -> None:
        _D("WS ERROR: %s  type=%s", error, type(error).__name__, level="error")

    def _on_close(self, ws, status, msg) -> None:
        self._connected = False
        _D("WS CLOSED  status=%s  msg=%s", status, msg, level="warning")

    def _send(self, msg: str) -> None:
        with self._lock:
            if self._ws and self._connected:
                try:
                    self._ws.send(msg)
                    _D("WS SEND: %s", msg[:120])
                except Exception as exc:
                    _D("WS SEND error: %s", exc, level="warning")

    def _subscribe_sports(self) -> None:
        _D_section("subscribe_sports")
        for sport_id in LIVE_SPORT_IDS:
            self._send(f'42["subscribe","sport-{sport_id}"]')
            _D("  subscribed sport-%d (%s)", sport_id, SPORT_SLUG_MAP.get(sport_id, "?"), level="info")
        threading.Thread(
            target=self._warm_event_cache, daemon=True, name="sp-live-warm",
        ).start()

    def _warm_event_cache(self) -> None:
        _D_section("warm_event_cache")
        r = _get_redis()
        total = 0
        for sport_id in LIVE_SPORT_IDS:
            try:
                events = fetch_live_events(sport_id, limit=100)
                total += len(events)
                pipe = r.pipeline()
                for ev in events:
                    pipe.setex(f"sp:live:event_sport:{ev['id']}", TTL_EVENTS, sport_id)
                    pipe.setex(f"sp:live:event_slug:{ev['id']}",
                               TTL_EVENTS, SPORT_SLUG_MAP.get(sport_id, ""))
                pipe.execute()
                _D("warm_cache sport=%d: %d events → Redis", sport_id, len(events), level="info")
                time.sleep(0.3)
            except Exception as exc:
                _D("warm_cache sport=%d FAILED: %s", sport_id, exc, level="error")
        _D("warm_event_cache DONE: %d total events", total, level="info")

    def _ping_loop(self) -> None:
        n = 0
        while not self._stop.is_set() and self._connected:
            time.sleep(self._ping_interval)
            if self._connected:
                self._send("2")
                n += 1
                if n % 5 == 0:
                    _D("ping_loop: %d pings sent", n)

    def run_forever(self) -> None:
        backoff = 2.0
        while not self._stop.is_set():
            self._connect_count += 1
            _D_section(f"WS CONNECT attempt #{self._connect_count}")
            _D("Connecting → %s", WS_URL, level="info")
            try:
                self._ws = websocket.WebSocketApp(
                    WS_URL,
                    header=[f"Origin: {ORIGIN}", f"User-Agent: {USER_AGENT}"],
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=0, sslopt={"check_hostname": True})
                backoff = 2.0
            except Exception as exc:
                _D("run_forever EXCEPTION: %s\n%s", exc, traceback.format_exc(), level="error")
            if not self._stop.is_set():
                _D("Reconnecting in %.0fs…", backoff, level="info")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def stop(self) -> None:
        self._stop.set()
        self._connected = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

# ═════════════════════════════════════════════════════════════════════════════
# SINGLETON
# ═════════════════════════════════════════════════════════════════════════════

_harvester_instance: SpLiveHarvester | None = None
_harvester_thread:   threading.Thread | None = None

# ═════════════════════════════════════════════════════════════════════════════
# LIVE POLLER
# ═════════════════════════════════════════════════════════════════════════════

POLL_DEFAULT_INTERVAL = 4
POLL_DETAIL_INTERVAL  = 8
POLL_EVENT_LIST_TTL   = 30


def _normalise_sels(slug: str, sels: list[dict]) -> list[dict]:
    result = []
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
        result.append({
            "id":          sel.get("id"),
            "name":        sel.get("name"),
            "outcome_key": out_key,
            "odds":        sel.get("odds"),
            "status":      sel.get("status"),
        })
    return result


class LivePoller:
    """
    Active polling thread — runs alongside the WebSocket harvester.
    Also triggers cross-BK enrichment on every detected odds change.
    """

    def __init__(self) -> None:
        self._stop  = threading.Event()
        self._threads: list[threading.Thread] = []
        self._event_cache: dict[int, tuple[list[dict], float]] = {}
        self._lock = threading.Lock()

    def start(self) -> None:
        t1 = threading.Thread(target=self._default_loop, daemon=True, name="sp-live-poll-default")
        t2 = threading.Thread(target=self._detail_loop,  daemon=True, name="sp-live-poll-detail")
        self._threads = [t1, t2]
        t1.start(); t2.start()
        _D("LivePoller started (default=%ds  detail=%ds)",
           POLL_DEFAULT_INTERVAL, POLL_DETAIL_INTERVAL, level="info")

    def stop(self) -> None:
        self._stop.set()

    def alive(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    def _get_live_events(self, sport_id: int) -> list[dict]:
        with self._lock:
            cached, fetched_at = self._event_cache.get(sport_id, ([], 0.0))
            if time.monotonic() - fetched_at < POLL_EVENT_LIST_TTL and cached:
                return cached
        events = fetch_live_events(sport_id, limit=100)
        with self._lock:
            self._event_cache[sport_id] = (events, time.monotonic())
        return events

    def _invalidate_event_cache(self, sport_id: int) -> None:
        with self._lock:
            self._event_cache.pop(sport_id, None)

    def _default_loop(self) -> None:
        while not self._stop.is_set():
            for sport_id in LIVE_SPORT_IDS:
                if self._stop.is_set():
                    break
                try:
                    self._poll_default(sport_id)
                except Exception as exc:
                    _D("default_loop sport=%d: %s", sport_id, exc, level="error")
                self._stop.wait(POLL_DEFAULT_INTERVAL / len(LIVE_SPORT_IDS))

    def _poll_default(self, sport_id: int) -> None:
        items = fetch_live_default_markets(sport_id)
        if not items:
            return
        for item in items:
            event_id = item.get("eventId")
            mkts     = item.get("markets") or []
            if not event_id or not mkts:
                continue
            try:
                event_id = int(event_id)
            except (TypeError, ValueError):
                continue
            for mkt in mkts:
                if not isinstance(mkt, dict):
                    continue
                if not mkt.get("eventMarketId"):
                    mkt["eventMarketId"] = mkt.get("id", 0)
                changed_sels, all_sels = _upsert_odds_and_diff(event_id, mkt)
                if not changed_sels:
                    continue
                mkt_type = mkt.get("id")
                handicap = mkt.get("specialValue")
                slug     = live_market_slug(mkt_type or 0, handicap, sport_id)
                norm_sels = _normalise_sels(slug, all_sels)
                poll_payload = {
                    "type":                  "market_update",
                    "source":                "poll_default",
                    "event_id":              event_id,
                    "sport_id":              sport_id,
                    "sport_slug":            SPORT_SLUG_MAP.get(sport_id),
                    "market_id":             mkt.get("eventMarketId"),
                    "market_type":           mkt_type,
                    "market_slug":           slug,
                    "market_name":           mkt.get("name", ""),
                    "handicap":              handicap,
                    "status":                mkt.get("status"),
                    "changed_selections":    changed_sels,
                    "all_selections":        all_sels,
                    "normalised_selections": norm_sels,
                    "ts":                    _now_iso(),
                }
                _sp_publish(sport_id, event_id, poll_payload)

                # Also trigger cross-BK enrichment from poll events
                betradar_id = _event_betradar_id(event_id) or str(event_id)
                try:
                    from app.workers.live_cross_bk_updater import get_updater
                    get_updater().trigger(
                        betradar_id = betradar_id,
                        sp_payload  = poll_payload,
                        sport_id    = sport_id,
                        kind        = "market_update",
                    )
                except Exception as exc:
                    _D("poll_default cross_bk trigger: %s", exc, level="warning")

    def _detail_loop(self) -> None:
        while not self._stop.is_set():
            for sport_id in LIVE_SPORT_IDS:
                if self._stop.is_set():
                    break
                try:
                    events = self._get_live_events(sport_id)
                    live = [
                        ev for ev in events
                        if ev.get("status", "").lower() in ("started", "inprogress", "live", "")
                        and (ev.get("state") or {}).get("currentEventPhase", "") not in ("", "NotStarted")
                    ]
                    if not live:
                        self._invalidate_event_cache(sport_id)
                        continue
                    for ev in live[:30]:
                        if self._stop.is_set():
                            break
                        try:
                            self._poll_detail(ev, sport_id)
                        except Exception as exc:
                            _D("detail_loop event=%s: %s", ev.get("id"), exc, level="error")
                        self._stop.wait(0.15)
                except Exception as exc:
                    _D("detail_loop sport=%d: %s", sport_id, exc, level="error")
                self._stop.wait(POLL_DETAIL_INTERVAL / len(LIVE_SPORT_IDS))

    def _poll_detail(self, ev_stub: dict, sport_id: int) -> None:
        event_id = ev_stub.get("id")
        if not event_id:
            return
        details = fetch_event_details(event_id)
        if not details or not isinstance(details, dict):
            return

        betradar_id = _event_betradar_id(event_id) or str(event_id)

        # ── 1. Market odds diff ───────────────────────────────────────────────
        for mkt in details.get("markets") or []:
            if not isinstance(mkt, dict):
                continue
            if not mkt.get("eventMarketId"):
                mkt["eventMarketId"] = mkt.get("id", 0)
            if mkt.get("status") == "Suspended":
                continue
            changed_sels, all_sels = _upsert_odds_and_diff(event_id, mkt)
            if not changed_sels:
                continue
            mkt_type  = mkt.get("id")
            handicap  = mkt.get("specialValue")
            slug      = live_market_slug(mkt_type or 0, handicap, sport_id)
            norm_sels = _normalise_sels(slug, all_sels)
            poll_payload = {
                "type":                  "market_update",
                "source":                "poll_detail",
                "event_id":              event_id,
                "sport_id":              sport_id,
                "sport_slug":            SPORT_SLUG_MAP.get(sport_id),
                "market_id":             mkt.get("eventMarketId"),
                "market_type":           mkt_type,
                "market_slug":           slug,
                "market_name":           mkt.get("name", ""),
                "handicap":              handicap,
                "status":                mkt.get("status"),
                "changed_selections":    changed_sels,
                "all_selections":        all_sels,
                "normalised_selections": norm_sels,
                "ts":                    _now_iso(),
            }
            _sp_publish(sport_id, event_id, poll_payload)
            # Cross-BK trigger from detail poll
            try:
                from app.workers.live_cross_bk_updater import get_updater
                get_updater().trigger(
                    betradar_id = betradar_id,
                    sp_payload  = poll_payload,
                    sport_id    = sport_id,
                    kind        = "market_update",
                )
            except Exception:
                pass

        # ── 2. Event state diff ────────────────────────────────────────────────
        ev_detail = details.get("event") or {}
        new_state = ev_detail.get("state") or details.get("state") or {}
        if not new_state:
            new_state = {k: details[k] for k in (
                "currentEventPhase", "matchTime", "matchScore",
                "clockRunning", "remainingTimeMillis"
            ) if k in details}
        if not new_state:
            return

        score     = new_state.get("matchScore") or {}
        new_phase = new_state.get("currentEventPhase", "")
        new_time  = new_state.get("matchTime", "")
        new_home  = str(score.get("home", ""))
        new_away  = str(score.get("away", ""))
        new_paused = ev_detail.get("isPaused", False)
        clock_run  = new_state.get("clockRunning", True)
        remain_ms  = new_state.get("remainingTimeMillis")

        state_key = f"sp:live:state:{event_id}"
        r         = _get_redis()
        prev_raw  = r.get(state_key)
        prev      = json.loads(prev_raw) if prev_raw else {}

        state_changed = (
            prev.get("phase")     != new_phase or
            prev.get("matchTime") != new_time  or
            prev.get("scoreHome") != new_home  or
            prev.get("scoreAway") != new_away  or
            prev.get("isPaused")  != new_paused
        )
        if not state_changed:
            return
        
        # ---- INJECT LIVE BROADCASTER HERE ----
        from app.workers.live_broadcaster import broadcast_event_state
        old_state_dict = {"score": f"{prev.get('scoreHome', '')}-{prev.get('scoreAway', '')}", "phase": prev.get("phase", "")}
        new_state_dict = {"score": f"{new_home}-{new_away}", "phase": new_phase, "match_time": new_time}
        broadcast_event_state(betradar_id, "sp", old_state_dict, new_state_dict)
        # --------------------------------------

        r.setex(state_key, TTL_STATE, json.dumps({
            "phase": new_phase, "matchTime": new_time,
            "scoreHome": new_home, "scoreAway": new_away, "isPaused": new_paused,
        }))

        ev_payload = {
            "type":          "event_update",
            "source":        "poll_detail",
            "event_id":      event_id,
            "sport_id":      sport_id,
            "sport_slug":    SPORT_SLUG_MAP.get(sport_id),
            "status":        ev_detail.get("status"),
            "phase":         new_phase,
            "is_paused":     new_paused,
            "clock_running": clock_run,
            "remaining_ms":  remain_ms,
            "score_home":    new_home,
            "score_away":    new_away,
            "state":         {"matchTime": new_time, "currentEventPhase": new_phase,
                              "matchScore": score, "clockRunning": clock_run,
                              "remainingTimeMillis": remain_ms},
            "ts":            _now_iso(),
        }
        _sp_publish(sport_id, event_id, ev_payload)
        try:
            from app.workers.live_cross_bk_updater import get_updater
            get_updater().trigger(
                betradar_id = betradar_id,
                sp_payload  = ev_payload,
                sport_id    = sport_id,
                kind        = "event_update",
            )
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════════════════
# SINGLETON + START
# ═════════════════════════════════════════════════════════════════════════════

_poller_instance: LivePoller | None = None
_poller_thread:   threading.Thread | None = None


def start_harvester_thread() -> threading.Thread:
    global _harvester_instance, _harvester_thread, _poller_instance, _poller_thread
    if _harvester_thread and _harvester_thread.is_alive():
        _D("harvester already running (%s)", _harvester_thread.name, level="info")
        return _harvester_thread

    _D_section("start_harvester_thread")

    # Pre-warm the cross-BK updater so the thread pool is ready
    try:
        from app.workers.live_cross_bk_updater import get_updater
        get_updater()
    except Exception as exc:
        _D("cross_bk_updater init: %s", exc, level="warning")

    _harvester_instance = SpLiveHarvester()
    _harvester_thread = threading.Thread(
        target=_harvester_instance.run_forever, name="sp-live-harvester", daemon=True,
    )
    _harvester_thread.start()
    _D("harvester thread started: %s", _harvester_thread.name, level="info")

    _poller_instance = LivePoller()
    _poller_instance.start()
    _D("poller started", level="info")

    return _harvester_thread


def stop_harvester() -> None:
    if _harvester_instance:
        _harvester_instance.stop()
    if _poller_instance:
        _poller_instance.stop()
    try:
        from app.workers.live_cross_bk_updater import get_updater
        get_updater().shutdown()
    except Exception:
        pass


def harvester_alive() -> bool:
    ws_alive   = bool(_harvester_thread and _harvester_thread.is_alive())
    poll_alive = bool(_poller_instance and _poller_instance.alive())
    return ws_alive or poll_alive


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    try:
        snapshot_all_sports()
    except Exception as exc:
        _D("snapshot failed: %s", exc, level="error")
    SpLiveHarvester().run_forever()


def fetch_live_stream(sport_slug: str, fetch_full_markets: bool = True) -> list[dict]:
    """
    Ultra-fast bulk fetcher for SportPesa live games. 
    Grabs all live events for a given sport and their markets in just 2 API calls.
    """
    sport_id = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)
    
    # 1. Fetch all live events
    events = fetch_live_events(sport_id, limit=100)
    if not events:
        return []

    sp_event_ids = [ev["id"] for ev in events]
    mkt_by_event = {}

    # 2. Bulk fetch core markets (1X2, Total, BTTS, Double Chance, Handicap)
    if fetch_full_markets and sp_event_ids:
        # 194=1X2, 105=Total, 138=BTTS, 147=DC, 184=Handicap
        for m_type in [194, 105, 138, 147, 184]: 
            try:
                res = fetch_live_markets(sp_event_ids, sport_id, m_type)
                if res:
                    for m in res:
                        eid = m.get("eventId")
                        if eid:
                            mkt_by_event.setdefault(eid, []).append(m)
            except Exception:
                pass

    stream_data = []
    for ev in events:
        betradar_id = str(ev.get("externalId") or "")
        sp_event_id = ev.get("id")
        
        state = ev.get("state", {})
        score = state.get("matchScore", {})
        
        sp_match = {
            "sp_match_id": sp_event_id,
            "betradar_id": betradar_id if betradar_id and betradar_id != "0" else None,
            "home_team": ev.get("competitors", [{},{}])[0].get("name", "Home"),
            "away_team": ev.get("competitors", [{},{}])[1].get("name", "Away"),
            "competition": ev.get("tournament", {}).get("name", ""),
            "sport": sport_slug,
            "start_time": ev.get("kickoffTimeUTC", ""),
            "match_time": state.get("matchTime", ""),
            "current_score": f"{score.get('home','')}-{score.get('away','')}",
            "event_status": state.get("currentEventPhase", ""),
            "markets": mkt_by_event.get(sp_event_id, [])
        }
        stream_data.append(sp_match)
        
    return stream_data