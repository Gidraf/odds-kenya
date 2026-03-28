"""
app/workers/sp_live_harvester.py
=================================
Sportpesa Live WebSocket harvester.

BUGS FIXED (from live WS traffic analysis)
─────────────────────────────────────────────────────────────────────────────
1. CRITICAL: was listening for "BUFFERED_MARKET_UPDATE" — actual event name
   from SP is "MARKET_UPDATE". This single bug caused zero live updates.

2. Live market type IDs differ from upcoming /api/games/markets IDs:
     Live  194 → 1x2          Upcoming  1/10 → 1x2
     Live  147 → double_chance Upcoming  46   → double_chance
     Live  138 → btts          Upcoming  43   → btts
     Live  145 → odd_even      Upcoming  45   → odd_even
     Live  166 → draw_no_bet   Upcoming  47   → draw_no_bet
     Live  151 → eur_handicap  Upcoming  55   → european_handicap
     Live  184 → asian_handicap Upcoming 51   → asian_handicap
     Live  105 → total (O/U)   Upcoming  52   → over_under_goals
     Live  149 → match_winner  (2-way)
     Live  183 → correct_score
     Live  154 → exact_goals
     Live  129 → first_team_to_score
     Live  155 → highest_scoring_half
     Live  135 → first_half_1x2
     Live  140 → first_half_btts

3. Live selection names are FULL names ("over 3.5", "Afghanistan or draw"),
   NOT shortNames ("OV", "1X") used by upcoming. Outcome normalizer updated.

4. The SPORT subscription is correct (42["subscribe","sport-1"]) — but
   the SP frontend also subscribes to individual event channels for detail
   views. We only need sport-level for the grid; event-level is optional.

Subscription protocol (confirmed from browser WS traffic):
   SEND  42["subscribe","sport-1"]    ← one per live sport
   RECV  42["MARKET_UPDATE", {...}]   ← odds change for any event in sport
   RECV  42["EVENT_UPDATE",  {...}]   ← score/phase update
   SEND  2   (every pingInterval ms) ← keep-alive PING
   RECV  3                            ← server PONG (or server sends 2, we reply 3)

Key live API endpoint (polls all events for sport with their default market):
   GET /api/live/default/markets?sportId=1
   Returns: {markets: [{eventId, markets:[{id,status,specialValue,selections:[{name,odds}]}]}]}
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
import websocket  # websocket-client

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
    diag.info("sp_live_harvester — log: %s", log_path)
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

# ── Live market type ID → (base_slug, uses_line) ─────────────────────────────
# Confirmed from actual SP live WS MARKET_UPDATE messages.
# These IDs are DIFFERENT from the upcoming /api/games/markets IDs.
LIVE_MARKET_MAP: dict[int, tuple[str, bool]] = {
    194: ("1x2",                     False),
    149: ("match_winner",            False),
    147: ("double_chance",           False),
    138: ("btts",                    False),
    140: ("first_half_btts",         False),
    145: ("odd_even",                False),
    166: ("draw_no_bet",             False),
    151: ("european_handicap",       True),   # specialValue = line e.g. "2.00"
    184: ("asian_handicap",          True),   # specialValue = line e.g. "0.50"
    105: ("__ou__",                  True),   # sport-aware, handled separately
    183: ("correct_score",           False),
    154: ("exact_goals",             False),
    129: ("first_team_to_score",     False),
    155: ("highest_scoring_half",    False),
    135: ("first_half_1x2",          False),
    303: ("match_winner",            False),  # home_no_bet alias
    304: ("match_winner",            False),  # away_no_bet alias
}

# ── O/U base slug per live sport ID ──────────────────────────────────────────
LIVE_OU_SLUG: dict[int, str] = {
    1:  "over_under_goals",   # Soccer
    5:  "over_under_goals",   # Handball
    2:  "total_points",       # Basketball
    8:  "total_points",       # Rugby
    4:  "total_games",        # Tennis
    13: "total_games",        # Table Tennis
    10: "total_sets",         # Volleyball
    9:  "total_runs",         # Cricket
}


def live_market_slug(mkt_type: int, handicap: Any, sport_id: int) -> str:
    """Convert live market_type + handicap + sport_id → canonical slug."""
    entry = LIVE_MARKET_MAP.get(mkt_type)
    if not entry:
        return f"market_{mkt_type}"
    base, uses_line = entry

    if base == "__ou__":
        base = LIVE_OU_SLUG.get(sport_id, "over_under")

    if uses_line and handicap is not None:
        # Normalize "3.50" → "3.5", "2.00" → "2"
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
# Live selection.name is the FULL name, not shortName.
# Examples from actual WS messages:
#   "over 3.5" → "over"      "under 3.5" → "under"
#   "Afghanistan or draw" → "1X"   "draw or Myanmar" → "X2"
#   "Afghanistan or Myanmar" → "12"
#   "Afghanistan (4:0)" → "1"  (positional — home team first)
#   "draw (4:0)" → "X"         "Myanmar (4:0)" → "2"
#   "odd" → "odd"              "even" → "even"
#   "Yes" → "yes"              "No" → "no"

_OVER_RE  = re.compile(r"^over\s+[\d.]+$",  re.I)
_UNDER_RE = re.compile(r"^under\s+[\d.]+$", re.I)
_SCORE_RE = re.compile(r"^\d+:\d+$")


def normalize_live_outcome(
    slug:      str,
    sel_name:  str,
    sel_index: int,
    all_sels:  list[dict],
) -> str:
    """
    Map a live selection.name → canonical outcome key.

    Uses sel_index (0-based position) as ultimate fallback for team-name
    selections so home=1, draw=X, away=2 still works even for obscure names.
    """
    kl = sel_name.strip().lower()

    # Over / Under (e.g. "over 3.5", "under 3.5")
    if _OVER_RE.match(kl):  return "over"
    if _UNDER_RE.match(kl): return "under"

    # Simple exact matches
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

    # Correct score "1:2" etc.
    raw = sel_name.strip()
    if _SCORE_RE.match(raw):
        return raw

    # "draw (...)" → X
    if kl.startswith("draw"):
        return "X"

    # Double Chance patterns: "X or Y" → figure out which combo
    if " or " in kl:
        n = len(all_sels)
        # Standard order: [1X, X2, 12]  or  [home_or_draw, draw_or_away, home_or_away]
        dc_map = {0: "1X", 1: "X2", 2: "12"}
        return dc_map.get(sel_index, f"dc_{sel_index}")

    # Handicap: "Team Name (2:0)" — use position
    if "(" in kl:
        pos_map = {0: "1", 1: "X", 2: "2"}
        # Only map positional if 3 sels (European HC), else 2-way
        if len(all_sels) == 2:
            return "1" if sel_index == 0 else "2"
        return pos_map.get(sel_index, str(sel_index + 1))

    # "other" outcome in correct score
    if kl in ("other", "othr", "any other"):
        return "other"

    # Numeric exact goals "0","1","2"…,"5+"
    if re.match(r"^\d+\+?$", raw):
        return raw

    # Positional fallback for 2-way markets (home/away team names)
    if len(all_sels) == 2:
        return "1" if sel_index == 0 else "2"
    if len(all_sels) == 3:
        pos_map = {0: "1", 1: "X", 2: "2"}
        return pos_map.get(sel_index, kl[:8])

    # Generic sanitise
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
        if isinstance(data, dict):
            _D("HTTP response keys: %s", list(data.keys())[:10])
            for k in ("sports", "events", "data", "markets"):
                if isinstance(data.get(k), list):
                    _D("  response[%r] = list(%d items)", k, len(data[k]))
        elif isinstance(data, list):
            _D("HTTP response = list(%d items)", len(data))
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
        _D("fetch_live_sports: NO DATA from API", level="warning")
        return []
    sports = data.get("sports") or []
    _D("fetch_live_sports: %d sports", len(sports), level="info")
    for s in sports:
        _D("  id=%s name=%r events=%s", s.get("id"), s.get("name"), s.get("eventNumber"))
    return sports


def fetch_live_events(sport_id: int, limit: int = 50, offset: int = 0) -> list[dict]:
    _D_section(f"fetch_live_events(sport_id={sport_id})")
    data = _get(f"/sports/{sport_id}/events", {"limit": limit, "offset": offset})
    if not data:
        _D("fetch_live_events sport=%d: no data", sport_id, level="warning")
        return []
    # SP API sometimes returns a list directly
    if isinstance(data, list):
        events = data
    else:
        events = data.get("events") or data.get("data") or []
    # Filter to dicts only (guard against nested lists)
    events = [e for e in events if isinstance(e, dict)]
    _D("fetch_live_events sport=%d: %d events", sport_id, len(events), level="info")
    for ev in events[:3]:
        comps = ev.get("competitors") or []
        names = " v ".join(c.get("name", "?") for c in comps[:2])
        _D("  id=%s  %r  state=%s", ev.get("id"), names,
           (ev.get("state") or {}).get("currentEventPhase", "?"))
    return events


def fetch_live_default_markets(sport_id: int) -> list[dict]:
    """
    GET /api/live/default/markets?sportId=1
    Returns the default market for all live events in one call.
    Response: {markets: [{eventId, markets:[...]}]}
    """
    _D_section(f"fetch_live_default_markets(sport_id={sport_id})")
    data = _get("/default/markets", {"sportId": sport_id})
    if not data:
        _D("fetch_live_default_markets sport=%d: no data", sport_id, level="warning")
        return []
    if isinstance(data, list):
        items = data
    else:
        items = data.get("markets") or []
    items = [i for i in items if isinstance(i, dict)]
    _D("fetch_live_default_markets sport=%d: %d event-market bundles", sport_id, len(items), level="info")
    return items


def fetch_live_markets(
    event_ids:   list[int],
    sport_id:    int,
    market_type: int = DEFAULT_MARKET_TYPE,
) -> list[dict]:
    if not event_ids:
        return []
    ids_str = ",".join(str(i) for i in event_ids[:15])
    _D("fetch_live_markets sport=%d type=%d ids=[%s]", sport_id, market_type, ids_str[:80])
    data = _get("/event/markets", {
        "eventId": ids_str, "type": market_type, "sportId": sport_id,
    })
    if not data:
        _D("fetch_live_markets: no data sport=%d type=%d", sport_id, market_type, level="warning")
        return []
    markets = data.get("markets") or data.get("data") or []
    _D("fetch_live_markets: %d objects returned", len(markets))
    return markets


def _get_quiet(path: str, params: dict | None = None, timeout: int = 10) -> Any:
    """Like _get but silently returns None on 404 (expected for ended/pre-match events)."""
    url = f"{API_BASE}{path}"
    try:
        r = _session().get(url, params=params, timeout=timeout)
        if r.status_code == 404:
            return None   # silent — event not live, expected
        if not r.ok:
            _D("HTTP ERROR %d: %s", r.status_code, r.text[:200], level="warning")
            return None
        return r.json()
    except Exception as exc:
        _D("HTTP EXCEPTION %s: %s", url, exc, level="error")
        return None


def fetch_event_details(event_id: int) -> dict | None:
    data = _get_quiet(f"/events/{event_id}/details")
    if isinstance(data, dict):
        return data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0]
    return None


def snapshot_all_sports() -> dict[int, list[dict]]:
    """
    Warm the Redis cache: fetch all live sports + events + default markets.
    Also fetches from /api/live/default/markets?sportId=N for the full market map.
    """
    _D_section("snapshot_all_sports")
    r      = _get_redis()
    sports = fetch_live_sports()
    result: dict[int, list[dict]] = {}

    if not sports:
        _D("snapshot_all_sports: 0 sports — SP live API may be blocking this IP", level="error")
        return result

    for sport in sports:
        sport_id   = sport["id"]
        sport_slug = SPORT_SLUG_MAP.get(sport_id, f"sport_{sport_id}")
        _D("snapshot sport_id=%d slug=%r expected=%s",
           sport_id, sport_slug, sport.get("eventNumber"), level="info")

        events = fetch_live_events(sport_id, limit=100)
        if not events:
            _D("snapshot sport_id=%d: 0 events", sport_id, level="warning")
            result[sport_id] = []
            continue

        # Cache event→sport mapping for WS message routing
        pipe = r.pipeline()
        for ev in events:
            pipe.setex(f"sp:live:event_sport:{ev['id']}", TTL_EVENTS, sport_id)
            pipe.setex(f"sp:live:event_slug:{ev['id']}",  TTL_EVENTS, sport_slug)
        pipe.execute()

        # Fetch the default markets (type 194 / match winner) for all events
        # This is the lightweight snapshot — the WS harvester fills in the rest
        default_mkts = fetch_live_default_markets(sport_id)

        # Also fetch a few more market types for richer initial display
        event_ids = [ev["id"] for ev in events]
        extended_mkts: list[dict] = []
        for i in range(0, min(len(event_ids), 30), 15):
            batch = fetch_live_markets(event_ids[i:i+15], sport_id, 105)   # O/U
            extended_mkts.extend(batch)
            time.sleep(0.2)

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
        _D("snapshot sport_id=%d: %d events, %d market bundles → Redis written",
           sport_id, len(events), len(default_mkts), level="info")

    r.setex("sp:live:sports", TTL_EVENTS, json.dumps(sports))
    _D("snapshot_all_sports DONE: %d sports, %d total events",
       len(result), sum(len(v) for v in result.values()), level="info")
    return result

# ═════════════════════════════════════════════════════════════════════════════
# UTILS
# ═════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ═════════════════════════════════════════════════════════════════════════════
# ODDS DELTA + HISTORY
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


def get_current_odds(event_id: int, market_id: int) -> dict:
    r   = _get_redis()
    raw = r.get(f"sp:live:odds:{event_id}:{market_id}")
    return json.loads(raw) if raw else {}

# ═════════════════════════════════════════════════════════════════════════════
# PUB/SUB
# ═════════════════════════════════════════════════════════════════════════════

def _publish(sport_id: int | None, event_id: int, payload: dict) -> None:
    r   = _get_redis()
    msg = json.dumps(payload, ensure_ascii=False)
    pipe = r.pipeline()
    pipe.publish(CH_ALL, msg)
    pipe.publish(CH_EVENT.format(event_id=event_id), msg)
    if sport_id:
        pipe.publish(CH_SPORT.format(sport_id=sport_id), msg)
    results = pipe.execute()
    _D("publish type=%s event=%s sport=%s → subs all=%s event=%s sport=%s",
       payload.get("type"), event_id, sport_id,
       results[0], results[1], results[2] if len(results) > 2 else "n/a")


def _event_sport_id(event_id: int) -> int | None:
    r   = _get_redis()
    val = r.get(f"sp:live:event_sport:{event_id}")
    if val is None:
        _D("event_sport_id(%s): no Redis mapping — event not in warm cache", event_id, level="warning")
    return int(val) if val else None

# ═════════════════════════════════════════════════════════════════════════════
# MESSAGE HANDLERS
# ═════════════════════════════════════════════════════════════════════════════

_msg_counter = 0


def _handle_market_update(data: dict) -> None:
    """
    Handle MARKET_UPDATE (was incorrectly named BUFFERED_MARKET_UPDATE).

    The live data uses market type IDs that DIFFER from the upcoming API:
      live 105 = Total (O/U, sport-aware slug)
      live 147 = Double Chance
      live 138 = BTTS
      live 151 = European Handicap (specialValue = line)
      live 184 = Asian Handicap   (specialValue = line)
    """
    global _msg_counter
    _msg_counter += 1

    event_id  = data.get("eventId")
    market_id = data.get("eventMarketId")
    mkt_type  = data.get("type")
    handicap  = data.get("handicap")
    mkt_name  = data.get("name", "?")
    status    = data.get("status")
    selections = data.get("selections") or []

    _D("MARKET_UPDATE #%d event=%s market=%s type=%s name=%r handicap=%r status=%s sels=%d",
       _msg_counter, event_id, market_id, mkt_type, mkt_name, handicap, status, len(selections))

    if not event_id or not market_id:
        _D("  ⚠ missing event_id or market_id", level="warning")
        return

    # Skip suspended/closed markets with no selections
    if status in ("Closed", "Suspended") and not selections:
        _D("  → market closed/suspended with no sels — skipping publish")
        return

    changed_sels, all_sels = _upsert_odds_and_diff(event_id, data)
    if not changed_sels:
        _D("  → no odds changed")
        return

    sport_id = _event_sport_id(event_id)
    slug     = live_market_slug(mkt_type, handicap, sport_id or 1)

    # Build normalised outcome→odd map using live outcome normalizer
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
            "id":         sel.get("id"),
            "name":       sel.get("name"),
            "outcome_key": out_key,
            "odds":        sel.get("odds"),
            "status":      sel.get("status"),
        })

    _D("  → slug=%r  %d/%d sels changed  normalised=%d",
       slug, len(changed_sels), len(all_sels), len(normalised_sels))
    for s in normalised_sels[:4]:
        _D("    outcome_key=%r  name=%r  odds=%s", s["outcome_key"], s["name"], s["odds"])

    payload = {
        "type":               "market_update",
        "event_id":           event_id,
        "market_id":          market_id,
        "sport_id":           sport_id,
        "sport_slug":         SPORT_SLUG_MAP.get(sport_id) if sport_id else None,
        "market_name":        mkt_name,
        "market_type":        mkt_type,
        "market_slug":        slug,            # ← NEW: canonical slug pre-computed
        "handicap":           handicap,
        "status":             status,
        "template":           data.get("template"),
        "sequence":           data.get("sequence"),
        "changed_count":      len(changed_sels),
        "changed_selections": changed_sels,
        "all_selections":     all_sels,
        "normalised_selections": normalised_sels,   # ← NEW: with outcome_key
        "ts":                 _now_iso(),
    }
    _publish(sport_id, event_id, payload)


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
    _publish(sport_id, event_id, payload)

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
        # Server PING → reply PONG
        if raw == "2":
            with self._lock:
                if self._ws:
                    self._ws.send("3")
            return

        # OPEN handshake
        if raw.startswith("0"):
            try:
                hs = json.loads(raw[1:])
                self._ping_interval = hs.get("pingInterval", 20000) / 1000
                _D("WS HANDSHAKE sid=%s pingInterval=%.1fs",
                   hs.get("sid"), self._ping_interval, level="info")
            except Exception as exc:
                _D("WS HANDSHAKE parse error: %s  raw=%r", exc, raw[:200], level="warning")
            return

        # Socket.IO CONNECT ack
        if raw == "40":
            _D("WS Socket.IO CONNECT ack → subscribing sports", level="info")
            self._subscribe_sports()
            return

        # Error frame
        if raw.startswith("44"):
            _D("WS ERROR frame: %s", raw[:300], level="error")
            return

        # EVENT payload
        if raw.startswith("42"):
            try:
                parts = json.loads(raw[2:])
                name  = parts[0]
                data  = parts[1] if len(parts) > 1 else {}

                if name == "MARKET_UPDATE":
                    # ← FIXED: was BUFFERED_MARKET_UPDATE (incorrect!)
                    _handle_market_update(data)
                elif name == "EVENT_UPDATE":
                    _handle_event_update(data)
                elif name == "BUFFERED_MARKET_UPDATE":
                    # Some SP environments may still use the old name
                    _D("WS BUFFERED_MARKET_UPDATE received (legacy) — routing to handler")
                    _handle_market_update(data)
                else:
                    _D("WS EVENT name=%r data_keys=%s",
                       name, list(data.keys())[:8] if isinstance(data, dict) else type(data))

            except Exception as exc:
                _D("WS MESSAGE parse error: %s\nraw: %.400s\n%s",
                   exc, raw, traceback.format_exc(), level="error")
            return

        # Our PONG — ignore silently
        if raw == "3":
            return

        _D("WS RECV unknown frame (len=%d): %s", len(raw), raw[:100])

    def _on_error(self, ws, error) -> None:
        _D("WS ERROR: %s  type=%s", error, type(error).__name__, level="error")

    def _on_close(self, ws, status, msg) -> None:
        self._connected = False
        _D("WS CLOSED  status=%s  msg=%s", status, msg, level="warning")
        if status == 1006:
            _D("  → 1006 = abnormal close (network drop / server kick)", level="warning")

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
# LIVE POLLER — active polling loop for real-time odds + state
# ═════════════════════════════════════════════════════════════════════════════
#
# The WebSocket only fires on changes (not every second).
# The LivePoller fills the gap:
#   • Every POLL_DEFAULT_INTERVAL s → GET /api/live/default/markets?sportId=N
#     This is ONE call per sport that returns 1x2 odds for every live event.
#     Cheap. Detects 1x2 changes in near-real-time.
#
#   • Every POLL_DETAIL_INTERVAL s → GET /api/live/events/{id}/details
#     Full market book + event state (score, phase, matchTime, isPaused).
#     Detects DC / O/U / BTTS changes + updates the live clock.
#
# All changes are diffed against the Redis odds snapshot and published
# ONLY if something actually changed → zero noise on the SSE channel.
#
# Diagram:
#   LivePoller thread
#     ├─ default_poll_loop() → sport loop → diff → publish market_update
#     └─ detail_poll_loop()  → event loop → diff → publish market_update
#                                                 → compare state → publish event_update

POLL_DEFAULT_INTERVAL = 4    # seconds between default-markets polls per sport
POLL_DETAIL_INTERVAL  = 8    # seconds between details polls per event
POLL_EVENT_LIST_TTL   = 30   # seconds before refreshing the live event list


class LivePoller:
    """
    Active polling thread — runs alongside the WebSocket harvester.
    Ensures odds and match state are synced even when WS is quiet.
    """

    def __init__(self) -> None:
        self._stop  = threading.Event()
        self._threads: list[threading.Thread] = []
        # Cached live event list per sport: {sport_id: (events, fetched_at)}
        self._event_cache: dict[int, tuple[list[dict], float]] = {}
        self._lock = threading.Lock()

    def start(self) -> None:
        t1 = threading.Thread(target=self._default_loop, daemon=True, name="sp-live-poll-default")
        t2 = threading.Thread(target=self._detail_loop,  daemon=True, name="sp-live-poll-detail")
        self._threads = [t1, t2]
        t1.start(); t2.start()
        _D("LivePoller started (default=%ds  detail=%ds)", POLL_DEFAULT_INTERVAL, POLL_DETAIL_INTERVAL, level="info")

    def stop(self) -> None:
        self._stop.set()

    def alive(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_live_events(self, sport_id: int) -> list[dict]:
        """Return cached event list, refreshing if stale."""
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

    # ── Default markets loop (fast batch — 1x2 for all events) ───────────────

    def _default_loop(self) -> None:
        """Poll /api/live/default/markets?sportId=N every POLL_DEFAULT_INTERVAL seconds."""
        while not self._stop.is_set():
            for sport_id in LIVE_SPORT_IDS:
                if self._stop.is_set():
                    break
                try:
                    self._poll_default(sport_id)
                except Exception as exc:
                    _D("default_loop sport=%d: %s", sport_id, exc, level="error")
                # Spread polls across the interval window
                self._stop.wait(POLL_DEFAULT_INTERVAL / len(LIVE_SPORT_IDS))

    def _poll_default(self, sport_id: int) -> None:
        """One default-markets poll for a sport."""
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

                _publish(sport_id, event_id, {
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
                })
                _D("poll_default sport=%d event=%d slug=%r → %d changed",
                   sport_id, event_id, slug, len(changed_sels))

    # ── Detail loop (full markets + state per event) ──────────────────────────

    def _detail_loop(self) -> None:
        """Poll /api/live/events/{id}/details every POLL_DETAIL_INTERVAL seconds."""
        while not self._stop.is_set():
            for sport_id in LIVE_SPORT_IDS:
                if self._stop.is_set():
                    break
                try:
                    events = self._get_live_events(sport_id)
                    # Filter to actually in-play events only
                    live = [
                        ev for ev in events
                        if ev.get("status", "").lower() in ("started", "inprogress", "live", "")
                        and (ev.get("state") or {}).get("currentEventPhase", "") not in ("", "NotStarted")
                    ]
                    if not live:
                        self._invalidate_event_cache(sport_id)
                        continue
                    for ev in live[:30]:   # cap at 30 in-play events
                        if self._stop.is_set():
                            break
                        try:
                            self._poll_detail(ev, sport_id)
                        except Exception as exc:
                            _D("detail_loop event=%s: %s", ev.get("id"), exc, level="error")
                        self._stop.wait(0.15)   # 150ms between detail calls
                except Exception as exc:
                    _D("detail_loop sport=%d: %s", sport_id, exc, level="error")
                self._stop.wait(POLL_DETAIL_INTERVAL / len(LIVE_SPORT_IDS))

    def _poll_detail(self, ev_stub: dict, sport_id: int) -> None:
        """Fetch full details for one event, publish any changes."""
        event_id = ev_stub.get("id")
        if not event_id:
            return

        details = fetch_event_details(event_id)
        if not details or not isinstance(details, dict):
            return

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

            mkt_type = mkt.get("id")
            handicap = mkt.get("specialValue")
            slug     = live_market_slug(mkt_type or 0, handicap, sport_id)
            norm_sels = _normalise_sels(slug, all_sels)

            _publish(sport_id, event_id, {
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
            })
            _D("poll_detail event=%d slug=%r → %d changed", event_id, slug, len(changed_sels))

        # ── 2. Event state diff (score + phase + matchTime) ───────────────────
        ev_detail = details.get("event") or {}
        new_state = ev_detail.get("state") or details.get("state") or {}
        if not new_state:
            # Fallback: state might be top-level in some response shapes
            new_state = {k: details[k] for k in ("currentEventPhase", "matchTime", "matchScore", "clockRunning", "remainingTimeMillis") if k in details}

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

        # Diff against Redis
        state_key = f"sp:live:state:{event_id}"
        r         = _get_redis()
        prev_raw  = r.get(state_key)
        prev      = json.loads(prev_raw) if prev_raw else {}

        state_changed = (
            prev.get("phase")      != new_phase or
            prev.get("matchTime")  != new_time  or
            prev.get("scoreHome")  != new_home  or
            prev.get("scoreAway")  != new_away  or
            prev.get("isPaused")   != new_paused
        )

        if not state_changed:
            return

        new_state_snap = {
            "phase": new_phase, "matchTime": new_time,
            "scoreHome": new_home, "scoreAway": new_away, "isPaused": new_paused,
        }
        r.setex(state_key, TTL_STATE, json.dumps(new_state_snap))

        _publish(sport_id, event_id, {
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
        })
        _D("poll_detail event=%d state changed: phase=%r time=%r score=%s-%s",
           event_id, new_phase, new_time, new_home, new_away)


def _normalise_sels(slug: str, sels: list[dict]) -> list[dict]:
    """Build normalised_selections with outcome_key for the frontend."""
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

    # ── WebSocket harvester (receives push events from SP) ────────────────────
    _harvester_instance = SpLiveHarvester()
    _harvester_thread = threading.Thread(
        target=_harvester_instance.run_forever, name="sp-live-harvester", daemon=True,
    )
    _harvester_thread.start()
    _D("harvester thread started: %s", _harvester_thread.name, level="info")

    # ── Active poller (fills gaps — real-time odds + state sync) ─────────────
    _poller_instance = LivePoller()
    _poller_instance.start()
    _D("poller started", level="info")

    return _harvester_thread


def stop_harvester() -> None:
    if _harvester_instance:
        _harvester_instance.stop()
    if _poller_instance:
        _poller_instance.stop()


def harvester_alive() -> bool:
    ws_alive    = bool(_harvester_thread and _harvester_thread.is_alive())
    poll_alive  = bool(_poller_instance and _poller_instance.alive())
    return ws_alive or poll_alive


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    try:
        snapshot_all_sports()
    except Exception as exc:
        _D("snapshot failed: %s", exc, level="error")
    SpLiveHarvester().run_forever()
    