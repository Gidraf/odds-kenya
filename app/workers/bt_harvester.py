"""
app/workers/bt_harvester.py
============================
Betika upcoming harvester (Dynamic Mapper Integrated).

Architecture
────────────
  ┌─────────────────────────────────────────────────────────────────┐
  │  BetikaHarvester (registry plugin)                              │
  │    ├─ fetch_upcoming(sport_slug) → list[CanonicalMatch]         │
  │    ├─ fetch_live()               → list[CanonicalMatch]         │
  │    └─ fetch_upcoming_stream()    → Generator[CanonicalMatch]    │
  ├─────────────────────────────────────────────────────────────────┤
  │  BetikaLivePoller (background thread, REST → Redis pub/sub)     │
  │    • Polls /v1/uo/sports every 10 s → updates live sport counts │
  │    • Polls /v1/uo/matches every 1.5 s → publishes delta events  │
  │    • Writes bt:live:{sport_id}:data to Redis (TTL 60 s)         │
  │    • Publishes bt:live:{sport_id}:updates channel (SSE feed)    │
  └─────────────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Generator

import httpx

# Import the new dynamic multi-sport mappers
from app.workers.mappers.betika import (
    get_market_slug,
    normalize_outcome,
    slug_to_bt_sport_id,
    bt_sport_to_slug,
)

logger = logging.getLogger(__name__)

# Standard Canonical IDs for downstream unified matching
CANONICAL_SPORT_IDS = {
    "soccer": 1,
    "basketball": 2,
    "tennis": 3,
    "cricket": 4,
    "rugby": 5,
    "ice-hockey": 6,
    "volleyball": 7,
    "handball": 8,
    "table-tennis": 9,
    "baseball": 10,
    "american-football": 11,
    "mma": 15,
    "boxing": 16,
    "darts": 17,
    "esoccer": 1001,
}

# ── Betika API endpoints ──────────────────────────────────────────────────────
LIVE_BASE  = "https://live.betika.com/v1/uo"
API_BASE   = "https://api.betika.com/v1/uo"

LIVE_SPORTS_URL    = f"{LIVE_BASE}/sports"
LIVE_MATCHES_URL   = f"{LIVE_BASE}/matches"
LIVE_MATCH_URL     = f"{LIVE_BASE}/match"      # GET ?id=match_id  (live detail)
UPCOMING_URL       = f"{API_BASE}/matches"
MATCH_MARKETS_URL  = f"{API_BASE}/match"       # GET ?parent_match_id=X  (pre-match detail)

HEADERS: dict[str, str] = {
    "accept":          "application/json, text/plain, */*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "origin":          "https://www.betika.com",
    "referer":         "https://www.betika.com/",
    "user-agent": (
        "Mozilla/5.0 (Linux; Android 10; K) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
}

# ── Redis key patterns ────────────────────────────────────────────────────────
_LIVE_DATA_KEY   = "bt:live:{sport_id}:data"
_LIVE_HASH_KEY   = "bt:live:{sport_id}:hash"
_LIVE_CHAN_KEY   = "bt:live:{sport_id}:updates"
_LIVE_SPORTS_KEY = "bt:live:sports"
_UPC_DATA_KEY    = "bt:upcoming:{sport_slug}:data"
_UPC_HASH_KEY    = "bt:upcoming:{sport_slug}:hash"
_UPC_CHAN_KEY    = "bt:upcoming:{sport_slug}:updates"


# ══════════════════════════════════════════════════════════════════════════════
# HTTP HELPERS (With 500 Error Protection)
# ══════════════════════════════════════════════════════════════════════════════

def _get(url: str, params: dict | None = None, timeout: float = 8.0) -> dict | None:
    for attempt in range(3):
        try:
            r = httpx.get(url, params=params, headers=HEADERS, timeout=timeout)
            
            if not r.is_success:
                logger.warning("BT HTTP %s %s (attempt %d)", r.status_code, url, attempt + 1)
                # If the server is crashing, don't hammer it or pass bad data downstream
                if r.status_code >= 500:
                    return None 
                continue
                
            return r.json()
        except httpx.RequestError as exc:
            logger.warning("BT request error %s (attempt %d): %s", url, attempt + 1, exc)
            
        time.sleep(0.5)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _parse_all_inline_markets(
    raw_mkts: list[dict],
    sport_slug: str,
) -> dict[str, dict[str, float]]:
    """
    Parse ALL inline markets using the new dynamic multi-sport mapper.
    Automatically groups lines (e.g. over_under_goals_2.5) by building the 
    slug directly from the parsed_special_bet_value attached to each odd.
    """
    result: dict[str, dict[str, float]] = {}

    for mkt in raw_mkts:
        sid = str(mkt.get("sub_type_id", ""))
        name = mkt.get("name", "")
        odds_raw = mkt.get("odds") or []

        for o in odds_raw:
            try:
                val = float(o.get("odd_value") or 0)
            except (ValueError, TypeError):
                continue

            if val <= 1.0:
                continue

            parsed_specs = o.get("parsed_special_bet_value") or {}

            # Route to the dynamic mapper
            slug = get_market_slug(sport_slug, sid, parsed_specs, fallback_name=name)
            outcome_key = normalize_outcome(sport_slug, o.get("display", ""))

            if slug not in result:
                result[slug] = {}
                
            result[slug][outcome_key] = val

    return result

def _normalise_match(raw: dict, *, source: str = "upcoming") -> dict | None:
    """
    Transform one Betika match dict (from either endpoint) into canonical shape.
    """
    try:
        bt_sport_id   = str(raw.get("sport_id") or "14")
        match_id      = str(raw.get("match_id") or raw.get("game_id") or "")
        
        # 🟢 FIX: Betika uses parent_match_id for the Betradar/Sportradar ID!
        betradar_id   = str(raw.get("parent_match_id") or "")
        parent_id     = betradar_id or match_id
        
        home          = str(raw.get("home_team") or "").strip()
        away          = str(raw.get("away_team") or "").strip()
        competition   = str(raw.get("competition_name") or "").strip()
        category      = str(raw.get("category") or "").strip()
        start_time    = str(raw.get("start_time") or "")
        sport_name    = str(raw.get("sport_name") or "").strip()

        if not match_id:
            return None

        # Start time → ISO format
        if start_time and " " in start_time:
            start_time = start_time.replace(" ", "T")

        sport_slug      = bt_sport_to_slug(bt_sport_id)
        can_sport_id    = CANONICAL_SPORT_IDS.get(sport_slug, 1)

        is_live       = source == "live"
        match_time    = str(raw.get("match_time") or "").strip()
        event_status  = str(raw.get("event_status") or "").strip()
        match_status  = str(raw.get("match_status") or "").strip()
        bet_status    = str(raw.get("bet_status") or "").strip()
        bet_stop      = str(raw.get("bet_stop_reason") or "").strip()
        current_score = str(raw.get("current_score") or "").strip()
        ht_score      = str(raw.get("ht_score") or "").strip()

        score_home = score_away = None
        if current_score and "-" in current_score:
            parts = current_score.split("-", 1)
            try:
                score_home = parts[0].strip()
                score_away = parts[1].strip()
            except IndexError: pass

        markets = _parse_all_inline_markets(raw.get("odds") or [], sport_slug)

        if "home_odd" in raw and not any(k.endswith("1x2") for k in markets):
            try:
                ho = float(raw.get("home_odd") or 0)
                no = float(raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    base = f"{sport_slug}_1x2" if sport_slug != "soccer" else "1x2"
                    markets[base] = {k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1}
            except (TypeError, ValueError): pass

        return {
            "bt_match_id":       match_id,
            "bt_parent_id":      parent_id,
            "sp_game_id":        None,
            "betradar_id":       betradar_id if betradar_id else None, # 🟢 FIX
            "home_team":         home,
            "away_team":         away,
            "competition":       competition,
            "category":          category,
            "sport_name":        sport_name,
            "sport":             sport_slug,
            "bt_sport_id":       int(bt_sport_id),
            "canonical_sport_id": can_sport_id,
            "start_time":        start_time,
            "source":            "betika",
            "is_live":           is_live,
            "is_suspended":      bet_status in ("STOPPED", "BET_STOP"),
            "match_time":        match_time,
            "event_status":      event_status,
            "match_status":      match_status,
            "bet_status":        bet_status,
            "bet_stop_reason":   bet_stop,
            "current_score":     current_score,
            "score_home":        score_home,
            "score_away":        score_away,
            "ht_score":          ht_score,
            "markets":           markets,
            "market_count":      len(markets),
        }

    except Exception as exc:
        logger.debug("BT match normalise error: %s", exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# FULL MARKETS  (on-demand via /v1/uo/match)
# ══════════════════════════════════════════════════════════════════════════════

def get_full_markets(parent_match_id: str | int, sport_slug: str) -> dict[str, dict[str, float]]:
    """
    Fetch all markets for one event.
    CRITICAL FIX: Checks by parent_match_id first, then standard match_id.
    If empty (because the match went LIVE), it automatically falls back to the live endpoint.
    """
    # 1. Try Upcoming Endpoint with parent_match_id
    data = _get(MATCH_MARKETS_URL, params={"parent_match_id": str(parent_match_id)})
    raw_mkts = (data or {}).get("data") or []
    
    # 2. Try Upcoming Endpoint with match_id (just in case)
    if not raw_mkts:
        data = _get(MATCH_MARKETS_URL, params={"match_id": str(parent_match_id)})
        raw_mkts = (data or {}).get("data") or []
        
    # 3. If empty, the match is likely LIVE. Try Live Endpoint.
    if not raw_mkts:
        live_data = _get(LIVE_MATCH_URL, params={"id": str(parent_match_id)})
        raw_mkts = (live_data or {}).get("data") or []
        
    return _parse_all_inline_markets(raw_mkts, sport_slug)

def get_live_match_markets(match_id: str | int, sport_slug: str) -> tuple[dict[str, dict[str, float]], dict]:
    """
    Fetch all available markets for one LIVE event from live.betika.com.
    """
    data = _get(LIVE_MATCH_URL, params={"id": str(match_id)}, timeout=6.0)
    if not data:
        return {}, {}
    raw_mkts = data.get("data") or []
    meta     = data.get("meta") or {}
    markets  = _parse_all_inline_markets(raw_mkts, sport_slug)
    return markets, meta


def enrich_matches_with_full_markets(
    matches:   list[dict],
    max_workers: int = 8,
) -> list[dict]:
    """
    Parallel-fetch full markets for every match in the list and merge them.
    """
    def _fetch(match: dict) -> dict:
        pid = match.get("bt_parent_id")
        sport_slug = match.get("sport", "soccer")
        if not pid:
            return match
            
        full = get_full_markets(pid, sport_slug)
        if full:
            merged = {**match, "markets": {**match.get("markets", {}), **full}}
            merged["market_count"] = len(merged["markets"])
            return merged
        return match

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_fetch, m): i for i, m in enumerate(matches)}
        ordered: dict[int, dict] = {}
        for fut in as_completed(futs):
            idx = futs[fut]
            try:
                ordered[idx] = fut.result()
            except Exception:  # noqa: BLE001
                ordered[idx] = matches[idx]
                
    for i in range(len(matches)):
        results.append(ordered.get(i, matches[i]))
    return results


# ══════════════════════════════════════════════════════════════════════════════
# NEW: UI STREAMING GENERATORS (FOR DEBUG SSE)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_stream(
    sport_slug:         str  = "soccer",
    max_matches:        int | None = None,
    max_pages:          int  = 10,
    period_id:          int  = 9,
    fetch_full_markets: bool = True,
    sleep_between:      float= 0.3,
    **kwargs,
) -> Generator[dict, None, None]:
    """
    Yields UPCOMING Betika matches one at a time for fast SSE streaming.
    """
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    _PRIMARY_SUB_TYPE_IDS = "1,10,11,18,29,60,186,219,251,340,406"
    count = 0

    for page in range(1, max_pages + 1):
        params: dict[str, Any] = {
            "page":        page,
            "limit":       50,
            "tab":         "upcoming",
            "sub_type_id": _PRIMARY_SUB_TYPE_IDS,
            "sport_id":    bt_sport_id,
            "sort_id":     2,
            "period_id":   period_id,
            "esports":     "false",
        }
        data = _get(UPCOMING_URL, params=params, timeout=8.0)
        if not data: break

        raw = data.get("data") or []
        if not raw: break

        for r in raw:
            if max_matches and count >= max_matches:
                return

            norm = _normalise_match(r, source="upcoming")
            if not norm: continue

            # Fetch extra markets (O/U, Correct Score, Handicaps) for this specific match
            if fetch_full_markets and norm.get("bt_parent_id"):
                full_markets = get_full_markets(norm["bt_parent_id"], sport_slug)
                if full_markets:
                    norm["markets"].update(full_markets)
                    norm["market_count"] = len(norm["markets"])
                time.sleep(sleep_between)

            count += 1
            yield norm

        meta  = data.get("meta") or {}
        total = int(meta.get("total") or 0)
        limit = int(meta.get("limit") or 50)
        if page * limit >= total:
            break


def fetch_live_stream(
    sport_slug:         str,
    **kwargs,
) -> Generator[dict, None, None]:
    """
    Stub generator. Live streaming is explicitly disabled for Betika per user request.
    """
    yield from []


# ══════════════════════════════════════════════════════════════════════════════
# OLD LIST-BASED FEED
# ══════════════════════════════════════════════════════════════════════════════

def fetch_live_sports() -> list[dict]:
    data = _get(LIVE_SPORTS_URL, timeout=5.0)
    return (data or {}).get("data") or []

def fetch_live_matches(bt_sport_id: str | int | None = None) -> list[dict]:
    _LIVE_SUB_TYPES = "1,10,11,18,29,60,186,219,251,340,406"
    params: dict[str, Any] = {
        "page":        1, "limit":       1000, "sub_type_id": _LIVE_SUB_TYPES,
        "sport":       str(bt_sport_id) if bt_sport_id is not None else "null", "sort":        1,
    }
    data = _get(LIVE_MATCHES_URL, params=params, timeout=6.0)
    if not data: return []
    results = []
    for r in (data.get("data") or []):
        norm = _normalise_match(r, source="live")
        if norm: results.append(norm)
    return results

def fetch_upcoming_matches(sport_slug: str = "soccer", max_pages: int = 10, period_id: int = 9, fetch_full: bool = False, max_workers: int = 8) -> list[dict]:
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    all_matches: list[dict] = []
    _PRIMARY_SUB_TYPE_IDS = "1,10,11,18,29,60,186,219,251,340,406"

    for page in range(1, max_pages + 1):
        params: dict[str, Any] = {
            "page": page, "limit": 50, "tab": "upcoming", "sub_type_id": _PRIMARY_SUB_TYPE_IDS,
            "sport_id": bt_sport_id, "sort_id": 2, "period_id": period_id, "esports": "false",
        }
        data = _get(UPCOMING_URL, params=params, timeout=8.0)
        if not data or not data.get("data"): break
        for r in data["data"]:
            norm = _normalise_match(r, source="upcoming")
            if norm: all_matches.append(norm)

        meta = data.get("meta") or {}
        if page * int(meta.get("limit") or 50) >= int(meta.get("total") or 0): break

    if fetch_full and all_matches:
        all_matches = enrich_matches_with_full_markets(all_matches, max_workers=max_workers)
    return all_matches


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND LIVE POLLER
# ══════════════════════════════════════════════════════════════════════════════

def _payload_hash(matches: list[dict]) -> str:
    frags = []
    for m in matches:
        mkt_frag = "|".join(f"{slug}:{k}:{v}" for slug, outcomes in sorted((m.get("markets") or {}).items()) for k, v in sorted(outcomes.items()))
        frags.append(f"{m.get('bt_match_id')}:{m.get('bet_status')}:{m.get('current_score')}:{m.get('match_time')}:{mkt_frag}")
    return hashlib.md5("\n".join(frags).encode()).hexdigest()  # noqa: S324

class BetikaLivePoller:
    def __init__(self, redis_client: Any, interval: float = 1.5, sports_interval: float = 10.0):
        self.redis = redis_client
        self.interval = interval
        self.sports_interval = sports_interval
        self._running = False
        self._thread: threading.Thread | None = None
        self._sports_thread: threading.Thread | None = None
        self._prev_hashes: dict[int, str] = {}
        self._live_sports: list[dict] = []
        self._lock = threading.Lock()

    def start(self) -> None:
        if self._running: return
        self._running = True
        self._sports_thread = threading.Thread(target=self._sports_loop, daemon=True, name="bt-sports")
        self._thread = threading.Thread(target=self._poll_loop, daemon=True, name="bt-live")
        self._sports_thread.start()
        time.sleep(0.5)   
        self._thread.start()
        logger.info("BetikaLivePoller started")

    def stop(self) -> None: self._running = False

    def get_live_sports(self) -> list[dict]:
        with self._lock: return list(self._live_sports)

    def _sports_loop(self) -> None:
        while self._running:
            tick = time.time()
            try:
                sports = fetch_live_sports()
                if sports:
                    with self._lock: self._live_sports = sports
                    self.redis.set(_LIVE_SPORTS_KEY, json.dumps({"ok": True, "sports": sports}), ex=30)
            except Exception as exc: logger.warning("BT sports loop error: %s", exc)
            time.sleep(max(0, self.sports_interval - (time.time() - tick)))

    def _poll_loop(self) -> None:
        while self._running:
            tick = time.time()
            try:
                matches = fetch_live_matches()
                if matches: self._process_live_batch(matches)
            except Exception as exc: logger.error("BT live poll error: %s", exc)
            time.sleep(max(0, self.interval - (time.time() - tick)))

    def _process_live_batch(self, matches: list[dict]) -> None:
        by_sport: dict[int, list[dict]] = {}
        for m in matches: by_sport.setdefault(m["bt_sport_id"], []).append(m)

        for bt_sport_id, sport_matches in by_sport.items():
            new_hash = _payload_hash(sport_matches)
            old_hash = self._prev_hashes.get(bt_sport_id, "")
            if new_hash == old_hash: continue

            self._prev_hashes[bt_sport_id] = new_hash
            sport_slug = bt_sport_to_slug(bt_sport_id)

            self.redis.set(_LIVE_DATA_KEY.format(sport_id=bt_sport_id), json.dumps(sport_matches, ensure_ascii=False), ex=60)
            events = self._build_delta_events(sport_matches, bt_sport_id)
            if events:
                self.redis.publish(_LIVE_CHAN_KEY.format(sport_id=bt_sport_id), json.dumps({"type": "batch_update", "sport_id": bt_sport_id, "sport_slug": sport_slug, "events": events, "total": len(sport_matches), "ts": time.time()}, ensure_ascii=False))

    def _build_delta_events(self, matches: list[dict], bt_sport_id: int) -> list[dict]:
        events: list[dict] = []
        for m in matches:
            mid = m["bt_match_id"]
            for slug, outcomes in (m.get("markets") or {}).items():
                for outcome_key, odd_val in outcomes.items():
                    redis_key = f"bt:prev:{mid}:{slug}:{outcome_key}"
                    prev_str = self.redis.get(redis_key)
                    prev_val = float(prev_str) if prev_str else None

                    if prev_val is None or abs(odd_val - prev_val) > 0.001:
                        self.redis.set(redis_key, str(odd_val), ex=300)
                        events.append({"type": "market_update", "match_id": mid, "home_team": m["home_team"], "away_team": m["away_team"], "match_time": m.get("match_time"), "score_home": m.get("score_home"), "score_away": m.get("score_away"), "market_slug": slug, "outcome_key": outcome_key, "odd": odd_val, "prev_odd": prev_val, "is_new": prev_val is None})
        return events


class BetikaHarvesterPlugin:
    bookie_id   = "betika"
    bookie_name = "Betika"
    sport_slugs = list(CANONICAL_SPORT_IDS.keys())

    def fetch_upcoming(self, sport_slug: str, max_pages: int = 10, fetch_full: bool = False) -> list[dict]:
        return fetch_upcoming_matches(sport_slug=sport_slug, max_pages=max_pages, fetch_full=fetch_full)
    def fetch_live(self, sport_slug: str | None = None) -> list[dict]:
        return fetch_live_matches(slug_to_bt_sport_id(sport_slug) if sport_slug else None)
    def get_live_sports(self) -> list[dict]:
        return fetch_live_sports()

__all__ = [
    "fetch_upcoming_matches",
    "fetch_live_matches",
    "fetch_upcoming_stream",
    "fetch_live_stream",
    "get_full_markets",
    "get_live_match_markets",
]


# ══════════════════════════════════════════════════════════════════════════════
# REDIS CACHE HELPERS (Restored for backward compatibility with betika_view.py)
# ══════════════════════════════════════════════════════════════════════════════

def cache_upcoming(
    redis_client: Any,
    sport_slug:   str,
    matches:      list[dict],
    ttl:          int = 300,
) -> None:
    key = _UPC_DATA_KEY.format(sport_slug=sport_slug)
    redis_client.set(key, json.dumps(matches, ensure_ascii=False), ex=ttl)
    redis_client.set(_UPC_HASH_KEY.format(sport_slug=sport_slug), _payload_hash(matches), ex=ttl)

    channel = _UPC_CHAN_KEY.format(sport_slug=sport_slug)
    redis_client.publish(channel, json.dumps({
        "type":      "upcoming_refresh",
        "sport":     sport_slug,
        "count":     len(matches),
        "ts":        time.time(),
    }))

def get_cached_upcoming(redis_client: Any, sport_slug: str) -> list[dict] | None:
    data = redis_client.get(_UPC_DATA_KEY.format(sport_slug=sport_slug))
    if data:
        return json.loads(data)
    return None

def get_cached_live(redis_client: Any, bt_sport_id: int) -> list[dict] | None:
    data = redis_client.get(_LIVE_DATA_KEY.format(sport_id=bt_sport_id))
    if data:
        return json.loads(data)
    return None

def get_cached_live_sports(redis_client: Any) -> list[dict] | None:
    data = redis_client.get(_LIVE_SPORTS_KEY)
    if data:
        payload = json.loads(data)
        return payload.get("sports") or []
    return None


# ══════════════════════════════════════════════════════════════════════════════
# SINGLETON POLLER STUBS (Prevents import crashes in other files)
# ══════════════════════════════════════════════════════════════════════════════

_poller_instance = None

def start_live_poller(redis_client: Any, interval: float = 1.5):
    pass

def get_live_poller():
    return None


# ══════════════════════════════════════════════════════════════════════════════
# REGISTRY EXPORTS
# ══════════════════════════════════════════════════════════════════════════════

__all__ = [
    "fetch_upcoming_matches",
    "fetch_live_matches",
    "fetch_upcoming_stream",
    "fetch_live_stream",
    "get_full_markets",
    "get_live_match_markets",
    # Restored exports below:
    "cache_upcoming",
    "get_cached_upcoming",
    "get_cached_live",
    "get_cached_live_sports",
    "start_live_poller",
    "get_live_poller",
]