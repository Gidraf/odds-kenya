"""
app/workers/sp_live_harvester.py
=================================
Sportpesa Live WebSocket harvester.

CHANGES v6 — Multi-Sport Complete Mapping
─────────────────────────────────────────────────────────────────────────────
Added deep mappings for Basketball (149, 156, 161, 315, 316) and 
Tennis (112, 141, 195, 163, 170) to ensure SportPesa data bridges perfectly
with Betika and OdiBets in the Unified UI.
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
    if diag.handlers: return diag
    diag.setLevel(logging.DEBUG)
    diag.propagate = False
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "sp_live_debug.log")
    fh = logging.handlers.RotatingFileHandler(log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    diag.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter("[sp_live] %(message)s"))
    diag.addHandler(sh)
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
USER_AGENT = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"

DEFAULT_MARKET_TYPE = 194

LIVE_SPORT_IDS = [1, 2, 4, 5, 8, 9, 10, 13]
SPORT_SLUG_MAP = {
    1: "soccer", 2: "basketball", 4: "tennis", 5: "handball", 
    8: "rugby", 9: "cricket", 10: "volleyball", 13: "table-tennis",
}

# 🟢 NEW: Complete Multi-Sport Mapping Dictionary
LIVE_MARKET_MAP: dict[int, tuple[str, bool]] = {
    # Soccer
    194: ("1x2", False), 147: ("double_chance", False), 138: ("btts", False), 
    140: ("first_half_btts", False), 145: ("odd_even", False), 166: ("draw_no_bet", False), 
    151: ("european_handicap", True), 184: ("asian_handicap", True), 105: ("__ou__", True), 
    183: ("correct_score", False), 154: ("exact_goals", False), 129: ("first_team_to_score", False), 
    155: ("highest_scoring_half", False), 135: ("first_half_1x2", False), 
    303: ("match_winner", False), 304: ("match_winner", False),
    
    # Basketball
    149: ("match_winner", False), 156: ("asian_handicap", True), 161: ("__ou__", True),
    315: ("1x2", False), 316: ("__ou__", True), 116: ("odd_even", False),
    
    # Tennis
    112: ("match_winner", False), 141: ("__ou__", True), 195: ("asian_handicap", True),
    163: ("odd_even", False), 170: ("tiebreak", False),
    
    # Volleyball & Ice Hockey (Fallbacks)
    123: ("match_winner", False)
}

LIVE_OU_SLUG: dict[int, str] = {
    1: "over_under_goals", 5: "over_under_goals", 2: "over_under", 8: "total_points",
    4: "over_under", 13: "total_games", 10: "total_sets", 9: "total_runs",
}

def live_market_slug(mkt_type: int, handicap: Any, sport_id: int) -> str:
    entry = LIVE_MARKET_MAP.get(mkt_type)
    if not entry: return f"market_{mkt_type}"
    base, uses_line = entry
    if base == "__ou__": base = LIVE_OU_SLUG.get(sport_id, "over_under")
    if uses_line and handicap is not None:
        try:
            f = float(handicap)
            line = str(int(f)) if f == int(f) else str(f)
        except (TypeError, ValueError):
            line = str(handicap)
        if line and line != "0": return f"{base}_{line}"
    return base

# ═════════════════════════════════════════════════════════════════════════════
# NORMALIZER & REDIS
# ═════════════════════════════════════════════════════════════════════════════

_OVER_RE  = re.compile(r"^over\s+[\d.]+$",  re.I)
_UNDER_RE = re.compile(r"^under\s+[\d.]+$", re.I)
_SCORE_RE = re.compile(r"^\d+:\d+$")

def normalize_live_outcome(slug: str, sel_name: str, sel_index: int, all_sels: list[dict]) -> str:
    kl = sel_name.strip().lower()
    if _OVER_RE.match(kl): return "over"
    if _UNDER_RE.match(kl): return "under"
    exact = {"yes": "yes", "no": "no", "odd": "odd", "even": "even", "1": "1", "x": "X", "2": "2", "draw": "X", "1x": "1X", "x2": "X2", "12": "12", "1st": "1st", "2nd": "2nd", "equal": "equal", "eql": "equal", "none": "none"}
    if kl in exact: return exact[kl]
    raw = sel_name.strip()
    if _SCORE_RE.match(raw): return raw
    if kl.startswith("draw"): return "X"
    if " or " in kl: return {0: "1X", 1: "X2", 2: "12"}.get(sel_index, f"dc_{sel_index}")
    if "(" in kl:
        if len(all_sels) == 2: return "1" if sel_index == 0 else "2"
        return {0: "1", 1: "X", 2: "2"}.get(sel_index, str(sel_index + 1))
    if kl in ("other", "othr", "any other"): return "other"
    if re.match(r"^\d+\+?$", raw): return raw
    if len(all_sels) == 2: return "1" if sel_index == 0 else "2"
    if len(all_sels) == 3: return {0: "1", 1: "X", 2: "2"}.get(sel_index, kl[:8])
    return re.sub(r"[^a-z0-9_:+./\-]+", "_", kl).strip("_") or f"sel_{sel_index}"

_redis_client = None
def _get_redis():
    global _redis_client
    if _redis_client is None:
        import redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _redis_client = redis.from_url(url, decode_responses=True)
    return _redis_client

# ═════════════════════════════════════════════════════════════════════════════
# HTTP SESSION & FETCH
# ═════════════════════════════════════════════════════════════════════════════

_SESSION: requests.Session | None = None
CH_ALL = "sp:live:all"; CH_SPORT = "sp:live:sport:{sport_id}"; CH_EVENT = "sp:live:event:{event_id}"
TTL_ODDS = 7200; TTL_STATE = 3600; TTL_EVENTS = 1800; TTL_SNAPSHOT = 300

def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        _SESSION.headers.update({"Origin": ORIGIN, "Referer": ORIGIN + "/", "User-Agent": USER_AGENT})
    return _SESSION

def _get(path: str, params: dict | None = None, timeout: int = 10) -> Any:
    try:
        r = _session().get(f"{API_BASE}{path}", params=params, timeout=timeout)
        if not r.ok: return None
        return r.json()
    except Exception:
        return None

def fetch_live_sports() -> list[dict]:
    data = _get("/sports")
    return data.get("sports") or [] if data else []

def fetch_live_events(sport_id: int, limit: int = 50, offset: int = 0) -> list[dict]:
    data = _get(f"/sports/{sport_id}/events", {"limit": limit, "offset": offset})
    if not data: return []
    events = data if isinstance(data, list) else data.get("events") or data.get("data") or []
    return [e for e in events if isinstance(e, dict)]

def fetch_live_markets(event_ids: list[int], sport_id: int, market_type: int = DEFAULT_MARKET_TYPE) -> list[dict]:
    if not event_ids: return []
    ids_str = ",".join(str(i) for i in event_ids)
    data = _get("/event/markets", {"eventId": ids_str, "type": market_type, "sportId": sport_id})
    return data.get("markets") or data.get("data") or [] if data else []

def _get_quiet(path: str, params: dict | None = None, timeout: int = 10) -> Any:
    url = f"{API_BASE}{path}"
    try:
        r = _session().get(url, params=params, timeout=timeout)
        if r.status_code == 404: return None
        if not r.ok: return None
        return r.json()
    except Exception:
        return None

def fetch_event_details(event_id: int) -> dict | None:
    data = _get_quiet(f"/events/{event_id}/details")
    if isinstance(data, dict): return data
    if isinstance(data, list) and data and isinstance(data[0], dict): return data[0]
    return None

def snapshot_all_sports() -> dict[int, list[dict]]:
    _D_section("snapshot_all_sports")
    r      = _get_redis()
    sports = fetch_live_sports()
    result: dict[int, list[dict]] = {}
    if not sports: return result
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
            "sport_id": sport_id, "sport_slug": sport_slug, "sport_name": sport.get("name", sport_slug),
            "event_count": sport.get("eventNumber", len(events)), "events": events,
            "markets": default_mkts, "fetched_at": _now_iso(),
        }
        r.setex(f"sp:live:snapshot:{sport_id}", TTL_SNAPSHOT, json.dumps(snapshot))
        result[sport_id] = events
    r.setex("sp:live:sports", TTL_EVENTS, json.dumps(sports))
    return result

# ═════════════════════════════════════════════════════════════════════════════
# WEBSOCKET HARVESTER
# ═════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

class SpLiveHarvester:
    def __init__(self) -> None:
        self._ws: websocket.WebSocketApp | None = None
        self._stop: threading.Event = threading.Event()
        self._ping_thread: threading.Thread | None = None
        self._ping_interval: float = 20.0
        self._connected: bool = False
        self._lock: threading.Lock = threading.Lock()
        self._connect_count: int = 0

    def _on_open(self, ws) -> None:
        self._connected = True
        _D("WS OPEN (attempt #%d)", self._connect_count, level="info")
        self._ping_thread = threading.Thread(target=self._ping_loop, daemon=True, name="sp-live-ping")
        self._ping_thread.start()

    def _on_message(self, ws, raw: str) -> None:
        if raw == "2":
            with self._lock:
                if self._ws: self._ws.send("3")
            return
        if raw.startswith("0"):
            try:
                hs = json.loads(raw[1:])
                self._ping_interval = hs.get("pingInterval", 20000) / 1000
            except Exception: pass
            return
        if raw == "40":
            _D("WS Socket.IO CONNECT ack → subscribing sports and buffered events", level="info")
            self._subscribe_sports()
            return
        if raw.startswith("42"):
            try:
                parts = json.loads(raw[2:])
                name = parts[0]
                data = parts[1] if len(parts) > 1 else {}
                
                if name in ("MARKET_UPDATE", "BUFFERED_MARKET_UPDATE"):
                    self._handle_market_update(data)
                elif name == "EVENT_UPDATE":
                    self._handle_event_update(data)
            except Exception as exc:
                _D("WS MESSAGE parse error: %s", exc, level="error")

    def _on_error(self, ws, error) -> None:
        pass

    def _on_close(self, ws, status, msg) -> None:
        self._connected = False

    def _send(self, msg: str) -> None:
        with self._lock:
            if self._ws and self._connected:
                try: self._ws.send(msg)
                except Exception: pass

    def _subscribe_sports(self) -> None:
        for sport_id in LIVE_SPORT_IDS:
            self._send(f'42["subscribe","sport-{sport_id}"]')
        threading.Thread(target=self._warm_and_subscribe_events, daemon=True, name="sp-live-warm").start()

    def _warm_and_subscribe_events(self) -> None:
        r = _get_redis()
        for sport_id in LIVE_SPORT_IDS:
            try:
                events = fetch_live_events(sport_id, limit=100)
                pipe = r.pipeline()
                
                # 🟢 NEW: Master subscription list for WebSockets
                core_markets = [
                    194, 105, 138, 147, 184, 123, # Soccer & Volley
                    149, 156, 161, 315, 316,      # Basketball
                    112, 141, 195                 # Tennis
                ] 
                
                for ev in events:
                    ev_id = ev['id']
                    pipe.setex(f"sp:live:event_sport:{ev_id}", TTL_EVENTS, sport_id)
                    pipe.setex(f"sp:live:event_slug:{ev_id}", TTL_EVENTS, SPORT_SLUG_MAP.get(sport_id, ""))
                    for m_type in core_markets:
                        self._send(f'42["subscribe","buffered-event-{ev_id}-{m_type}-0.00"]')
                        
                pipe.execute()
                time.sleep(0.3)
            except Exception as exc: pass

    def _ping_loop(self) -> None:
        while not self._stop.is_set() and self._connected:
            time.sleep(self._ping_interval)
            if self._connected: self._send("2")

    def run_forever(self) -> None:
        backoff = 2.0
        while not self._stop.is_set():
            self._connect_count += 1
            try:
                self._ws = websocket.WebSocketApp(
                    WS_URL, header=[f"Origin: {ORIGIN}", f"User-Agent: {USER_AGENT}"],
                    on_open=self._on_open, on_message=self._on_message,
                    on_error=self._on_error, on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=0, sslopt={"check_hostname": True})
                backoff = 2.0
            except Exception: pass
            if not self._stop.is_set():
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def stop(self) -> None:
        self._stop.set()
        self._connected = False
        if self._ws:
            try: self._ws.close()
            except Exception: pass
    
    def _handle_market_update(self, data: dict) -> None:
        event_id = data.get("eventId")
        market_id = data.get("eventMarketId")
        mkt_type = data.get("type")
        handicap = data.get("handicap")
        status = data.get("status")
        selections = data.get("selections") or []

        if not event_id or not market_id or (status in ("Closed", "Suspended") and not selections): return
        
        r = _get_redis()
        sport_id = r.get(f"sp:live:event_sport:{event_id}")
        sport_id = int(sport_id) if sport_id else 1
        
        slug = live_market_slug(mkt_type, handicap, sport_id)
        norm_sels = []
        for idx, sel in enumerate(selections):
            if sel.get("status") == "Suspended": continue
            try: odd = float(sel.get("odds") or "0")
            except: odd = 0.0
            if odd <= 1.0: continue
            norm_sels.append({
                "id": sel.get("id"), "name": sel.get("name"),
                "outcome_key": normalize_live_outcome(slug, sel.get("name", ""), idx, selections),
                "odds": sel.get("odds"), "status": sel.get("status")
            })

        if not norm_sels: return

        betradar_id = str(event_id)
        br_cached = r.get(f"sp:live:event_br:{event_id}")
        if br_cached: betradar_id = br_cached
        
        sport_slug = SPORT_SLUG_MAP.get(sport_id)
        outcomes = {s["outcome_key"]: {"price": float(s["odds"])} for s in norm_sels}
        update_payload = {
            "parent_match_id": betradar_id,
            "bookmakers": {"sp": {"bookmaker": "SPORTPESA", "slug": "sp", "markets": {slug: outcomes}, "market_count": 1}},
            "markets_by_bk": {"sp": {slug: outcomes}}
        }
        try:
            msg = json.dumps(update_payload)
            p = r.pipeline()
            p.publish(f"live:match:{betradar_id}:all", msg)
            if sport_slug: p.publish(f"live:matches:{sport_slug}", msg)
            p.publish("live:all", msg)
            p.execute()
        except Exception: pass

        try:
            from app.workers.live_cross_bk_updater import get_updater
            get_updater().trigger(betradar_id=betradar_id, sp_payload=data, sport_id=sport_id, kind="market_update")
        except Exception: pass

    def _handle_event_update(self, data: dict) -> None:
        event_id = data.get("id")
        sport_id = data.get("sportId")
        state = data.get("state") or {}
        score = state.get("matchScore") or {}
        phase = state.get("currentEventPhase", "?")
        
        if not event_id: return
        r = _get_redis()
        betradar_id = r.get(f"sp:live:event_br:{event_id}") or str(event_id)
        
        update_payload = {"parent_match_id": betradar_id}
        if state.get("matchTime"): update_payload["match_time"] = str(state.get("matchTime"))
        if score.get("home"): update_payload["score_home"] = str(score.get("home"))
        if score.get("away"): update_payload["score_away"] = str(score.get("away"))
        update_payload["status"] = str(phase)

        try:
            msg = json.dumps(update_payload)
            p = r.pipeline()
            p.publish(f"live:match:{betradar_id}:all", msg)
            p.publish("live:all", msg)
            p.execute()
        except Exception: pass

# ═════════════════════════════════════════════════════════════════════════════
# BOOTSTRAP
# ═════════════════════════════════════════════════════════════════════════════

_harvester_instance = None
def start_harvester_thread() -> threading.Thread:
    global _harvester_instance
    try:
        from app.workers.live_cross_bk_updater import get_updater
        get_updater()
    except Exception: pass

    _harvester_instance = SpLiveHarvester()
    t = threading.Thread(target=_harvester_instance.run_forever, name="sp-live-harvester", daemon=True)
    t.start()
    return t

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    SpLiveHarvester().run_forever()

def fetch_live_stream(sport_slug: str, fetch_full_markets: bool = True) -> list[dict]:
    sport_id = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)
    events = fetch_live_events(sport_id, limit=100)
    if not events: return []

    sp_event_ids = [ev["id"] for ev in events]
    mkt_by_event = {}

    if fetch_full_markets and sp_event_ids:
        chunk_size = 15
        chunks = [sp_event_ids[i:i + chunk_size] for i in range(0, len(sp_event_ids), chunk_size)]
        
        # 🟢 NEW: Master HTTP Request List
        request_markets = [
            194, 105, 138, 147, 184, 123, 154, 145, 151, 166, 196, 124, # Core
            149, 156, 161, 315, 316,                                    # Basketball
            112, 141, 195, 163, 170                                     # Tennis
        ]
        
        for chunk in chunks:
            for m_type in request_markets: 
                try:
                    res = fetch_live_markets(chunk, sport_id, m_type)
                    if res:
                        for m in res:
                            eid = m.get("eventId")
                            inner_mkts = m.get("markets") or []
                            if eid and inner_mkts:
                                mkt_by_event.setdefault(eid, []).extend(inner_mkts)
                except Exception: pass

    stream_data = []
    for ev in events:
        betradar_id = str(ev.get("externalId") or "")
        sp_event_id = ev.get("id")
        state = ev.get("state") or {}
        score = state.get("matchScore") or {}
        
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