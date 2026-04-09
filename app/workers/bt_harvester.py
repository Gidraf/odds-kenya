"""
app/workers/bt_harvester.py
============================
Betika live + upcoming harvester (Dynamic Mapper Integrated).

Architecture
────────────
  ┌─────────────────────────────────────────────────────────────────┐
  │  BetikaHarvester (registry plugin)                              │
  │    ├─ fetch_upcoming(sport_slug) → list[CanonicalMatch]         │
  │    └─ fetch_live()               → list[CanonicalMatch]         │
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
from typing import Any

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
        parent_id     = str(raw.get("parent_match_id") or match_id)
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

        # Live-only fields
        is_live       = source == "live"
        match_time    = str(raw.get("match_time") or "").strip()
        event_status  = str(raw.get("event_status") or "").strip()
        match_status  = str(raw.get("match_status") or "").strip()
        bet_status    = str(raw.get("bet_status") or "").strip()
        bet_stop      = str(raw.get("bet_stop_reason") or "").strip()
        current_score = str(raw.get("current_score") or "").strip()
        ht_score      = str(raw.get("ht_score") or "").strip()

        score_home: str | None = None
        score_away: str | None = None
        if current_score and "-" in current_score:
            parts = current_score.split("-", 1)
            try:
                score_home = parts[0].strip()
                score_away = parts[1].strip()
            except IndexError:
                pass

        # Parse inline odds using the dynamic mapper
        markets = _parse_all_inline_markets(raw.get("odds") or [], sport_slug)

        # Also parse quick top-level odds (home_odd / neutral_odd / away_odd) if missing standard 1x2
        if "home_odd" in raw and not any(k.endswith("1x2") for k in markets):
            try:
                ho = float(raw.get("home_odd") or 0)
                no = float(raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    base = f"{sport_slug}_1x2" if sport_slug != "soccer" else "1x2"
                    markets[base] = {
                        k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1
                    }
            except (TypeError, ValueError):
                pass

        return {
            "bt_match_id":       match_id,
            "bt_parent_id":      parent_id,
            "sp_game_id":        None,
            "betradar_id":       None,
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

    except Exception as exc:  # noqa: BLE001
        logger.debug("BT match normalise error: %s", exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# FULL MARKETS  (on-demand via /v1/uo/match)
# ══════════════════════════════════════════════════════════════════════════════

def get_full_markets(parent_match_id: str | int, sport_slug: str) -> dict[str, dict[str, float]]:
    """
    Fetch all markets for one pre-match event from api.betika.com.
    """
    data = _get(MATCH_MARKETS_URL, params={"parent_match_id": str(parent_match_id)})
    if not data:
        return {}
    raw_mkts = data.get("data") or []
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
# LIVE FEED
# ══════════════════════════════════════════════════════════════════════════════

def fetch_live_sports() -> list[dict]:
    """GET /v1/uo/sports → list of {sport_id, sport_name, count}."""
    data = _get(LIVE_SPORTS_URL, timeout=5.0)
    return (data or {}).get("data") or []


def fetch_live_matches(bt_sport_id: str | int | None = None) -> list[dict]:
    """
    GET /v1/uo/matches with common live sub_type_ids.
    """
    # Expanded list of primary IDs covering most major live sports
    _LIVE_SUB_TYPES = "1,10,11,18,29,60,186,219,251,340,406"
    
    params: dict[str, Any] = {
        "page":        1,
        "limit":       1000,
        "sub_type_id": _LIVE_SUB_TYPES,
        "sport":       str(bt_sport_id) if bt_sport_id is not None else "null",
        "sort":        1,
    }
    data = _get(LIVE_MATCHES_URL, params=params, timeout=6.0)
    if not data:
        return []

    raw = data.get("data") or []
    results = []
    for r in raw:
        norm = _normalise_match(r, source="live")
        if norm:
            results.append(norm)

    logger.debug("BT live: %d/%d matches (sport=%s)", len(results), len(raw), bt_sport_id)
    return results


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING FEED
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_matches(
    sport_slug:  str = "soccer",
    max_pages:   int = 10,
    period_id:   int = 9,
    fetch_full:  bool = False,
    max_workers: int = 8,
) -> list[dict]:
    """
    Paginate through Betika upcoming matches for one sport.
    """
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    all_matches: list[dict] = []

    # Broad list of typical primary IDs across different sports
    _PRIMARY_SUB_TYPE_IDS = "1,10,11,18,29,60,186,219,251,340,406"

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
        if not data:
            break

        raw = data.get("data") or []
        if not raw:
            break

        for r in raw:
            norm = _normalise_match(r, source="upcoming")
            if norm:
                all_matches.append(norm)

        meta    = data.get("meta") or {}
        total   = int(meta.get("total") or 0)
        limit   = int(meta.get("limit") or 50)
        if page * limit >= total:
            break

    if fetch_full and all_matches:
        all_matches = enrich_matches_with_full_markets(all_matches, max_workers=max_workers)

    logger.info("BT upcoming: %d matches for sport=%s", len(all_matches), sport_slug)
    return all_matches


# ══════════════════════════════════════════════════════════════════════════════
# HASH  (change detection)
# ══════════════════════════════════════════════════════════════════════════════

def _payload_hash(matches: list[dict]) -> str:
    frags = []
    for m in matches:
        mkt_frag = "|".join(
            f"{slug}:{k}:{v}"
            for slug, outcomes in sorted((m.get("markets") or {}).items())
            for k, v in sorted(outcomes.items())
        )
        frags.append(
            f"{m.get('bt_match_id')}:{m.get('bet_status')}:"
            f"{m.get('current_score')}:{m.get('match_time')}:{mkt_frag}"
        )
    return hashlib.md5("\n".join(frags).encode()).hexdigest()  # noqa: S324


# ══════════════════════════════════════════════════════════════════════════════
# LIVE POLLER  (background thread — REST-based, mirrors SP WS pattern)
# ══════════════════════════════════════════════════════════════════════════════

class BetikaLivePoller:
    """
    Polls Betika live REST endpoint every `interval` seconds.
    Publishes change events to Redis pub/sub channels.
    """

    def __init__(
        self,
        redis_client: Any,
        interval:      float = 1.5,
        sports_interval: float = 10.0,
    ):
        self.redis   = redis_client
        self.interval         = interval
        self.sports_interval  = sports_interval
        self._running         = False
        self._thread:  threading.Thread | None = None
        self._sports_thread: threading.Thread | None = None
        self._prev_hashes:   dict[int, str]   = {}
        self._live_sports:   list[dict]        = []
        self._lock           = threading.Lock()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._sports_thread = threading.Thread(target=self._sports_loop, daemon=True, name="bt-sports")
        self._thread        = threading.Thread(target=self._poll_loop,   daemon=True, name="bt-live")
        self._sports_thread.start()
        time.sleep(0.5)   
        self._thread.start()
        logger.info("BetikaLivePoller started (interval=%.1fs)", self.interval)

    def stop(self) -> None:
        self._running = False

    def get_live_sports(self) -> list[dict]:
        with self._lock:
            return list(self._live_sports)

    # ── Internal loops ────────────────────────────────────────────────────────

    def _sports_loop(self) -> None:
        while self._running:
            tick = time.time()
            try:
                sports = fetch_live_sports()
                if sports:
                    with self._lock:
                        self._live_sports = sports
                    self.redis.set(
                        _LIVE_SPORTS_KEY,
                        json.dumps({"ok": True, "sports": sports}),
                        ex=30,
                    )
            except Exception as exc:  # noqa: BLE001
                logger.warning("BT sports loop error: %s", exc)
            elapsed = time.time() - tick
            time.sleep(max(0, self.sports_interval - elapsed))

    def _poll_loop(self) -> None:
        while self._running:
            tick = time.time()
            try:
                matches = fetch_live_matches()
                if matches:
                    self._process_live_batch(matches)
            except Exception as exc:  # noqa: BLE001
                logger.error("BT live poll error: %s", exc)
            elapsed = time.time() - tick
            time.sleep(max(0, self.interval - elapsed))

    def _process_live_batch(self, matches: list[dict]) -> None:
        by_sport: dict[int, list[dict]] = {}
        for m in matches:
            by_sport.setdefault(m["bt_sport_id"], []).append(m)

        for bt_sport_id, sport_matches in by_sport.items():
            new_hash = _payload_hash(sport_matches)
            old_hash = self._prev_hashes.get(bt_sport_id, "")
            if new_hash == old_hash:
                continue

            self._prev_hashes[bt_sport_id] = new_hash
            sport_slug = bt_sport_to_slug(bt_sport_id)

            self.redis.set(
                _LIVE_DATA_KEY.format(sport_id=bt_sport_id),
                json.dumps(sport_matches, ensure_ascii=False),
                ex=60,
            )

            events = self._build_delta_events(sport_matches, bt_sport_id)
            if events:
                channel = _LIVE_CHAN_KEY.format(sport_id=bt_sport_id)
                payload = json.dumps({
                    "type":         "batch_update",
                    "sport_id":     bt_sport_id,
                    "sport_slug":   sport_slug,
                    "events":       events,
                    "total":        len(sport_matches),
                    "ts":           time.time(),
                }, ensure_ascii=False)
                self.redis.publish(channel, payload)

    def _build_delta_events(
        self,
        matches: list[dict],
        bt_sport_id: int,
    ) -> list[dict]:
        events: list[dict] = []
        for m in matches:
            mid       = m["bt_match_id"]
            markets   = m.get("markets") or {}

            for slug, outcomes in markets.items():
                for outcome_key, odd_val in outcomes.items():
                    redis_key = f"bt:prev:{mid}:{slug}:{outcome_key}"
                    prev_str  = self.redis.get(redis_key)
                    prev_val  = float(prev_str) if prev_str else None

                    if prev_val is None or abs(odd_val - prev_val) > 0.001:
                        self.redis.set(redis_key, str(odd_val), ex=300)
                        events.append({
                            "type":           "market_update",
                            "match_id":       mid,
                            "home_team":      m["home_team"],
                            "away_team":      m["away_team"],
                            "match_time":     m.get("match_time"),
                            "score_home":     m.get("score_home"),
                            "score_away":     m.get("score_away"),
                            "market_slug":    slug,
                            "outcome_key":    outcome_key,
                            "odd":            odd_val,
                            "prev_odd":       prev_val,
                            "is_new":         prev_val is None,
                        })

        return events


# ══════════════════════════════════════════════════════════════════════════════
# REGISTRY PLUGIN
# ══════════════════════════════════════════════════════════════════════════════

class BetikaHarvesterPlugin:
    bookie_id   = "betika"
    bookie_name = "Betika"
    sport_slugs = list(CANONICAL_SPORT_IDS.keys())

    def fetch_upcoming(
        self,
        sport_slug: str,
        max_pages:  int  = 10,
        fetch_full: bool = False,
    ) -> list[dict]:
        return fetch_upcoming_matches(
            sport_slug=sport_slug,
            max_pages=max_pages,
            fetch_full=fetch_full,
        )

    def fetch_live(self, sport_slug: str | None = None) -> list[dict]:
        bt_sid = slug_to_bt_sport_id(sport_slug) if sport_slug else None
        return fetch_live_matches(bt_sid)

    def get_live_sports(self) -> list[dict]:
        return fetch_live_sports()


# ══════════════════════════════════════════════════════════════════════════════
# REDIS CACHE HELPERS
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
# SINGLETON POLLER
# ══════════════════════════════════════════════════════════════════════════════

_poller_instance: BetikaLivePoller | None = None

def start_live_poller(redis_client: Any, interval: float = 1.5) -> BetikaLivePoller:
    global _poller_instance
    if _poller_instance is None:
        _poller_instance = BetikaLivePoller(redis_client, interval=interval)
        _poller_instance.start()
    return _poller_instance

def get_live_poller() -> BetikaLivePoller | None:
    return _poller_instance


# ══════════════════════════════════════════════════════════════════════════════
# STANDALONE RUNNER
# ══════════════════════════════════════════════════════════════════════════════

def run_live_loop(interval: float = 1.5) -> None:
    import logging as _log
    _log.basicConfig(level=_log.INFO, format="%(levelname)s  %(message)s")
    print("── Betika live harvester (Ctrl-C to stop) ──")
    prev: dict[int, str] = {}
    while True:
        tick = time.time()
        try:
            matches  = fetch_live_matches()
            by_sport: dict[int, list] = {}
            for m in matches:
                by_sport.setdefault(m["bt_sport_id"], []).append(m)
            for sid, sm in by_sport.items():
                h = _payload_hash(sm)
                if h != prev.get(sid):
                    prev[sid] = h
                    print(f"  [{time.strftime('%H:%M:%S')}] sport={sid} "
                          f"count={len(sm)}  hash={h[:8]}")
        except KeyboardInterrupt:
            print("\nStopped.")
            break
        except Exception as exc:  # noqa: BLE001
            logger.error("Live loop error: %s", exc)
        elapsed = time.time() - tick
        time.sleep(max(0, interval - elapsed))


def run_upcoming_snapshot(sport_slug: str = "soccer") -> None:
    matches = fetch_upcoming_matches(sport_slug=sport_slug, max_pages=1)
    print(f"Upcoming {sport_slug}: {len(matches)} matches")
    print(json.dumps(matches[:3], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "live"
    if cmd == "upcoming":
        run_upcoming_snapshot(sys.argv[2] if len(sys.argv) > 2 else "soccer")
    else:
        run_live_loop()