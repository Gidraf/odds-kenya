"""
app/workers/bt_harvester.py
============================
Betika live + upcoming harvester.

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
  ├─────────────────────────────────────────────────────────────────┤
  │  Celery tasks (uncomment to wire into beat schedule)            │
  └─────────────────────────────────────────────────────────────────┘

Canonical output format (matches SP pipeline SpMatch shape)
───────────────────────────────────────────────────────────
  {
    "bt_match_id":    "10688359",
    "bt_parent_id":   "69164878",
    "sp_game_id":     None,
    "betradar_id":    None,
    "home_team":      "Senegal",
    "away_team":      "Peru",
    "competition":    "Int. Friendly Games",
    "category":       "International",
    "sport":          "soccer",          ← canonical slug
    "bt_sport_id":    14,
    "canonical_sport_id": 1,
    "start_time":     "2026-03-28T19:00:00",
    "source":         "betika",
    "is_live":        False,
    "market_count":   3,
    "markets": {
        "1x2":                    {"1": 1.47, "X": 4.20, "2": 7.00},
        "over_under_goals_2.5":   {"over": 1.99, "under": 1.80},
        ...
    },
  }
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

from app.workers.bt_mapper import (
    BT_TO_CANONICAL_SPORT,
    BT_SPORT_SLUGS,
    SLUG_TO_BT_SPORT,
    extract_bt_line,
    get_bt_sport_table,
    get_bt_sport_primary_markets,
    normalize_bt_market,
    normalize_bt_outcome,
    parse_bt_odds_to_dict,
    bt_sport_to_slug,
    slug_to_bt_sport_id,
    _GENERIC_BT,
)

logger = logging.getLogger(__name__)

# ── Betika API endpoints ──────────────────────────────────────────────────────
LIVE_BASE  = "https://live.betika.com/v1/uo"
API_BASE   = "https://api.betika.com/v1/uo"

LIVE_SPORTS_URL    = f"{LIVE_BASE}/sports"
LIVE_MATCHES_URL   = f"{LIVE_BASE}/matches"
UPCOMING_URL       = f"{API_BASE}/matches"
MATCH_MARKETS_URL  = f"{API_BASE}/match"

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

# Line market sub_type_ids that need a specifier parsed from sbv
LINE_SIDS: frozenset[int] = frozenset({
    14,15,16,18,19,20,36,37,51,52,53,54,55,56,58,59,65,66,68,69,70,
    71,74,79,86,87,88,90,91,92,93,94,95,98,138,139,142,166,167,168,
    172,177,226,339,340,342,362,363,364,365,366,367,368,369,439,544,
    547,549,550,
})

# ── Redis key patterns ────────────────────────────────────────────────────────
_LIVE_DATA_KEY   = "bt:live:{sport_id}:data"
_LIVE_HASH_KEY   = "bt:live:{sport_id}:hash"
_LIVE_CHAN_KEY   = "bt:live:{sport_id}:updates"
_LIVE_SPORTS_KEY = "bt:live:sports"
_UPC_DATA_KEY    = "bt:upcoming:{sport_slug}:data"
_UPC_HASH_KEY    = "bt:upcoming:{sport_slug}:hash"
_UPC_CHAN_KEY    = "bt:upcoming:{sport_slug}:updates"


# ══════════════════════════════════════════════════════════════════════════════
# HTTP HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get(url: str, params: dict | None = None, timeout: float = 8.0) -> dict | None:
    for attempt in range(2):
        try:
            r = httpx.get(url, params=params, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("BT HTTP %s %s (attempt %d)", exc.response.status_code, url, attempt+1)
        except Exception as exc:  # noqa: BLE001
            logger.warning("BT request error %s (attempt %d): %s", url, attempt+1, exc)
        if attempt == 0:
            time.sleep(0.4)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _parse_inline_market(mkt: dict, bt_sport_id: int) -> tuple[str, dict[str, float]] | None:
    """
    Parse ONE Betika market dict from the list-endpoint inline odds.
    Returns (canonical_slug, {outcome: odd}) or None if no valid odds.

    Handles the multi-line Total market correctly: each specValue variant
    becomes a separate slug entry (e.g. over_under_goals_2.5, _3.5 ...).
    """
    try:
        sid      = int(mkt.get("sub_type_id", 0))
        odds_raw = mkt.get("odds") or []
        if not odds_raw:
            return None

        # Group by specValue so we create one slug per line
        by_line: dict[str, list[dict]] = {}
        for o in odds_raw:
            sbv = (o.get("special_bet_value") or "").strip()
            by_line.setdefault(sbv, []).append(o)

        results: list[tuple[str, dict[str, float]]] = []
        for sbv, group in by_line.items():
            line  = extract_bt_line(sbv)
            table = get_bt_sport_table(bt_sport_id)
            entry = table.get(sid) or _GENERIC_BT.get(sid)
            if entry:
                base, uses_line = entry
                slug = f"{base}_{line}" if (uses_line and line) else base
            else:
                slug = normalize_bt_market(sid, mkt.get("name",""), bt_sport_id, sbv)

            outcomes = parse_bt_odds_to_dict(group, slug, sbv)
            if outcomes:
                results.append((slug, outcomes))

        if not results:
            return None

        # If single line, return directly
        if len(results) == 1:
            return results[0]

        # Multiple lines: return the first (caller can call again for each)
        # Caller should use _parse_all_inline_markets for full expansion
        return results[0]

    except Exception as exc:  # noqa: BLE001
        logger.debug("BT market parse error: %s | mkt=%s", exc, mkt)
        return None


def _parse_all_inline_markets(
    raw_mkts:    list[dict],
    bt_sport_id: int,
) -> dict[str, dict[str, float]]:
    """
    Parse ALL inline markets from a match/list entry into the canonical
    markets dict: {slug → {outcome → odd}}.

    Multi-line markets (Total O/U at multiple lines) are expanded into
    individual slugs: over_under_goals_0.5, over_under_goals_1.5, etc.
    """
    result: dict[str, dict[str, float]] = {}

    for mkt in raw_mkts:
        sid      = int(mkt.get("sub_type_id", 0))
        odds_raw = mkt.get("odds") or []
        if not odds_raw:
            continue

        # Group by specValue
        by_sbv: dict[str, list[dict]] = {}
        for o in odds_raw:
            sbv = (o.get("special_bet_value") or "").strip()
            by_sbv.setdefault(sbv, []).append(o)

        table = get_bt_sport_table(bt_sport_id)

        for sbv, group in by_sbv.items():
            line  = extract_bt_line(sbv)
            entry = table.get(sid) or _GENERIC_BT.get(sid)
            if entry:
                base, uses_line = entry
                slug = f"{base}_{line}" if (uses_line and line) else base
            else:
                slug = normalize_bt_market(sid, mkt.get("name",""), bt_sport_id, sbv)

            outcomes = parse_bt_odds_to_dict(group, slug, sbv)
            if outcomes:
                existing = result.get(slug, {})
                existing.update(outcomes)
                result[slug] = existing

    return result


def _normalise_match(raw: dict, *, source: str = "upcoming") -> dict | None:
    """
    Transform one Betika match dict (from either endpoint) into canonical shape.
    Inline odds are parsed; full markets require get_full_markets().
    """
    try:
        bt_sport_id   = int(raw.get("sport_id") or 14)
        match_id      = str(raw.get("match_id") or raw.get("game_id") or "")
        parent_id     = str(raw.get("parent_match_id") or "")
        home          = str(raw.get("home_team") or "").strip()
        away          = str(raw.get("away_team") or "").strip()
        competition   = str(raw.get("competition_name") or "").strip()
        category      = str(raw.get("category") or "").strip()
        start_time    = str(raw.get("start_time") or "")
        sport_name    = str(raw.get("sport_name") or "").strip()

        # Start time → ISO format
        if start_time and " " in start_time:
            start_time = start_time.replace(" ", "T")

        sport_slug      = bt_sport_to_slug(bt_sport_id)
        can_sport_id    = BT_TO_CANONICAL_SPORT.get(bt_sport_id, 1)

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

        # Cards / corners (live)
        home_red    = int(raw.get("home_red_card")    or 0)
        away_red    = int(raw.get("away_red_card")    or 0)
        home_yellow = int(raw.get("home_yellow_card") or 0)
        away_yellow = int(raw.get("away_yellow_card") or 0)
        home_corners= int(raw.get("home_corners")     or 0)
        away_corners= int(raw.get("away_corners")     or 0)

        # Parse inline odds
        markets = _parse_all_inline_markets(raw.get("odds") or [], bt_sport_id)

        # Also parse quick top-level odds (home_odd / neutral_odd / away_odd)
        if "home_odd" in raw and "1x2" not in markets:
            try:
                ho = float(raw.get("home_odd") or 0)
                no = float(raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    markets["1x2"] = {
                        k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1
                    }
            except (TypeError, ValueError):
                pass

        return {
            # Identity
            "bt_match_id":       match_id,
            "bt_parent_id":      parent_id,
            "sp_game_id":        None,
            "betradar_id":       None,
            # Teams / competition
            "home_team":         home,
            "away_team":         away,
            "competition":       competition,
            "category":          category,
            "sport_name":        sport_name,
            "sport":             sport_slug,
            "bt_sport_id":       bt_sport_id,
            "canonical_sport_id": can_sport_id,
            "start_time":        start_time,
            # Source
            "source":            "betika",
            "is_live":           is_live,
            "is_suspended":      bet_status in ("STOPPED", "BET_STOP"),
            # Live state
            "match_time":        match_time,
            "event_status":      event_status,
            "match_status":      match_status,
            "bet_status":        bet_status,
            "bet_stop_reason":   bet_stop,
            # Scores
            "current_score":     current_score,
            "score_home":        score_home,
            "score_away":        score_away,
            "ht_score":          ht_score,
            # Cards / corners
            "home_red_cards":    home_red,
            "away_red_cards":    away_red,
            "home_yellow_cards": home_yellow,
            "away_yellow_cards": away_yellow,
            "home_corners":      home_corners,
            "away_corners":      away_corners,
            # Markets
            "markets":           markets,
            "market_count":      len(markets),
        }

    except Exception as exc:  # noqa: BLE001
        logger.debug("BT match normalise error: %s", exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# FULL MARKETS  (on-demand via /v1/uo/match)
# ══════════════════════════════════════════════════════════════════════════════

def get_full_markets(parent_match_id: str | int, bt_sport_id: int = 14) -> dict[str, dict[str, float]]:
    """
    Fetch all markets for one match from the Betika detail endpoint.
    Returns canonical {slug → {outcome → odd}} dict.
    """
    data = _get(MATCH_MARKETS_URL, params={"parent_match_id": str(parent_match_id)})
    if not data:
        return {}
    raw_mkts = data.get("data") or []
    return _parse_all_inline_markets(raw_mkts, bt_sport_id)


def enrich_matches_with_full_markets(
    matches:   list[dict],
    max_workers: int = 8,
) -> list[dict]:
    """
    Parallel-fetch full markets for every match in the list and merge them.
    Returns enriched copies (original dicts untouched).
    """
    def _fetch(match: dict) -> dict:
        pid = match.get("bt_parent_id")
        sid = match.get("bt_sport_id", 14)
        if not pid:
            return match
        full = get_full_markets(pid, sid)
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


def fetch_live_matches(bt_sport_id: int | None = None) -> list[dict]:
    """
    GET /v1/uo/matches with live sub_type_ids (1, 186, 340).
    Returns list of normalised canonical match dicts.
    """
    params: dict[str, Any] = {
        "page":        1,
        "limit":       1000,
        "sub_type_id": "1,186,340",
        "sport":       bt_sport_id if bt_sport_id is not None else "null",
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
    Optionally fetches full markets (slower but richer data).
    """
    bt_sport_id = slug_to_bt_sport_id(sport_slug)
    all_matches: list[dict] = []

    for page in range(1, max_pages + 1):
        params: dict[str, Any] = {
            "page":        page,
            "limit":       50,
            "tab":         "upcoming",
            "sub_type_id": "1,186,340",
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
    Publishes change events to Redis pub/sub channels, mirrors the
    SP WebSocket + Redis pub/sub architecture so the same SSE endpoints
    and frontend components can consume both bookmakers uniformly.
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
        time.sleep(0.5)   # let sports populate first
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
                    # Cache sport counts in Redis
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
        """Main polling loop — fetches all live matches and publishes deltas."""
        while self._running:
            tick = time.time()
            try:
                # Fetch all live matches in one call (sport=null)
                matches = fetch_live_matches()
                if matches:
                    self._process_live_batch(matches)
            except Exception as exc:  # noqa: BLE001
                logger.error("BT live poll error: %s", exc)
            elapsed = time.time() - tick
            time.sleep(max(0, self.interval - elapsed))

    def _process_live_batch(self, matches: list[dict]) -> None:
        """Group matches by sport, detect changes, publish deltas."""
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

            # Write snapshot to Redis
            self.redis.set(
                _LIVE_DATA_KEY.format(sport_id=bt_sport_id),
                json.dumps(sport_matches, ensure_ascii=False),
                ex=60,
            )

            # Compute delta events (market changes per match)
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
        """
        Compare current markets against cached snapshot and return only
        changed outcomes as SSE-style event dicts (same shape as SP events).
        """
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
    """
    Registry-compatible plugin.  Wire into harvest_registry.py:

        from app.workers.bt_harvester import BetikaHarvesterPlugin
        from app.workers.harvest_registry import register_bookmaker
        register_bookmaker(BetikaHarvesterPlugin())
    """

    bookie_id   = "betika"
    bookie_name = "Betika"
    sport_slugs = list(SLUG_TO_BT_SPORT.keys())

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
# REDIS CACHE HELPERS  (used by bt_module.py Flask views)
# ══════════════════════════════════════════════════════════════════════════════

def cache_upcoming(
    redis_client: Any,
    sport_slug:   str,
    matches:      list[dict],
    ttl:          int = 300,
) -> None:
    """Write upcoming matches to Redis cache."""
    key = _UPC_DATA_KEY.format(sport_slug=sport_slug)
    redis_client.set(key, json.dumps(matches, ensure_ascii=False), ex=ttl)
    redis_client.set(_UPC_HASH_KEY.format(sport_slug=sport_slug), _payload_hash(matches), ex=ttl)

    # Publish refresh event on pub/sub channel
    channel = _UPC_CHAN_KEY.format(sport_slug=sport_slug)
    redis_client.publish(channel, json.dumps({
        "type":      "upcoming_refresh",
        "sport":     sport_slug,
        "count":     len(matches),
        "ts":        time.time(),
    }))


def get_cached_upcoming(redis_client: Any, sport_slug: str) -> list[dict] | None:
    key  = _UPC_DATA_KEY.format(sport_slug=sport_slug)
    data = redis_client.get(key)
    if data:
        return json.loads(data)
    return None


def get_cached_live(redis_client: Any, bt_sport_id: int) -> list[dict] | None:
    key  = _LIVE_DATA_KEY.format(sport_id=bt_sport_id)
    data = redis_client.get(key)
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
# CELERY TASKS  (wire in once celery_app is available)
# ══════════════════════════════════════════════════════════════════════════════
#
# from app.core.celery_app import celery_app
# from app.core.cache import redis_client
#
# @celery_app.task(name="betika.harvest_upcoming", bind=True,
#                  max_retries=3, soft_time_limit=120, time_limit=150)
# def harvest_bt_upcoming(self, sport_slug: str = "soccer"):
#     try:
#         matches = fetch_upcoming_matches(sport_slug=sport_slug, max_pages=10, fetch_full=True)
#         if matches:
#             cache_upcoming(redis_client, sport_slug, matches)
#     except Exception as exc:
#         logger.exception("harvest_bt_upcoming failed for %s: %s", sport_slug, exc)
#         raise self.retry(exc=exc, countdown=10)
#
# @celery_app.task(name="betika.poll_live", bind=True,
#                  max_retries=None, soft_time_limit=8, time_limit=10)
# def poll_bt_live(self):
#     """Called by beat every 2 seconds as a fallback if the poller thread isn't running."""
#     try:
#         matches = fetch_live_matches()
#         for m in matches:
#             sid = m["bt_sport_id"]
#             cached = get_cached_live(redis_client, sid) or []
#             # merge
#             by_id = {x["bt_match_id"]: x for x in cached}
#             by_id[m["bt_match_id"]] = m
#             redis_client.set(_LIVE_DATA_KEY.format(sport_id=sid),
#                              json.dumps(list(by_id.values())), ex=60)
#     except Exception as exc:
#         logger.exception("poll_bt_live failed: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# SINGLETON POLLER  (for ENABLE_HARVESTER=1 mode)
# ══════════════════════════════════════════════════════════════════════════════

_poller_instance: BetikaLivePoller | None = None


def start_live_poller(redis_client: Any, interval: float = 1.5) -> BetikaLivePoller:
    """Start the singleton live polling thread (idempotent)."""
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
    """Dev runner — press Ctrl+C to stop."""
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
    """Fetch + print upcoming matches for quick debugging."""
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