"""
app/workers/od_harvester.py
============================
OdiBets upcoming + live harvester.

Architecture
────────────
  ┌─────────────────────────────────────────────────────────────────┐
  │  OdiBetsHarvester (registry plugin)                             │
  │    ├─ fetch_upcoming(sport_slug, day, competition_id)           │
  │    └─ fetch_live(sport_slug)                                    │
  ├─────────────────────────────────────────────────────────────────┤
  │  OdiBetsLivePoller (background thread, REST → Redis pub/sub)    │
  │    • Polls /sportsbook/v1?resource=live every 2 s               │
  │    • Detects per-outcome changes → publishes delta events        │
  │    • Writes od:live:{sport_id}:data to Redis (TTL 60 s)         │
  │    • Publishes od:live:{sport_id}:updates channel (SSE feed)    │
  └─────────────────────────────────────────────────────────────────┘

OdiBets API endpoints
─────────────────────
  Upcoming list:  GET https://api.odi.site/odi/sportsbook
                      ?resource=sportevents&platform=mobile&mode=1
                      &sport_id=1&day=2026-03-30&sub_type_id=1

  Live list:      GET https://api.odi.site/sportsbook/v1
                      ?resource=live&sport_id=&sub_type_id=

  Match detail:   GET https://api.odi.site/sportsbook/v1
                      ?resource=sportevent&id={event_id}

  Sports:         GET https://api.odi.site/sportsbook/v1
                      ?resource=sport&sport_id=soccer

Canonical output format (mirrors BT/SP pipeline shape)
───────────────────────────────────────────────────────
  {
    "od_match_id":      "70352408",
    "od_event_id":      "70352408",
    "sp_game_id":       None,
    "betradar_id":      None,
    "home_team":        "Arsenal",
    "away_team":        "Chelsea",
    "competition":      "Premier League",
    "category":         "England",
    "sport":            "soccer",
    "od_sport_id":      1,
    "start_time":       "2026-03-30T15:00:00",
    "source":           "odibets",
    "is_live":          False,
    "markets": {
        "1x2":                    {"1": 2.10, "X": 3.40, "2": 3.55},
        "over_under_goals_2.5":   {"over": 1.88, "under": 1.95},
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
from typing import Any

import httpx

from app.workers.canonical_mapper import (
    normalize_line,
    normalize_od_market,
    normalize_outcome,
    slug_with_line,
)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# SPORT ID MAPS
# ══════════════════════════════════════════════════════════════════════════════

OD_SPORT_IDS: dict[str, int] = {
    "soccer":       1,
    "basketball":   2,
    "tennis":       3,
    "cricket":      4,
    "rugby":        5,
    "ice-hockey":   6,
    "volleyball":   7,
    "handball":     8,
    "table-tennis": 9,
    "baseball":     10,
    "esoccer":      1001,
    "mma":          15,
    "boxing":       16,
    "darts":        17,
    "american-football": 11,
}

OD_SPORT_SLUGS: dict[int, str] = {v: k for k, v in OD_SPORT_IDS.items()}


def slug_to_od_sport_id(slug: str) -> int:
    return OD_SPORT_IDS.get(slug, 1)


def od_sport_to_slug(sport_id: int) -> str:
    return OD_SPORT_SLUGS.get(sport_id, "soccer")


# ══════════════════════════════════════════════════════════════════════════════
# API ENDPOINTS + HEADERS
# ══════════════════════════════════════════════════════════════════════════════

API_BASE  = "https://api.odi.site"
SBOOK_V1  = f"{API_BASE}/sportsbook/v1"    # live, sportevent, sport
SBOOK_ODI = f"{API_BASE}/odi/sportsbook"   # upcoming (sportevents)

HEADERS: dict[str, str] = {
    "accept":           "application/json, text/plain, */*",
    "accept-language":  "en-GB,en;q=0.9",
    "authorization":    "Bearer",
    "content-type":     "application/json",
    "origin":           "https://odibets.com",
    "referer":          "https://odibets.com/",
    "user-agent": (
        "Mozilla/5.0 (Linux; Android 10; K) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
    "sec-ch-ua":          '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile":   "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "cross-site",
}

# ── Redis key patterns ────────────────────────────────────────────────────────
_LIVE_DATA_KEY   = "od:live:{sport_id}:data"
_LIVE_HASH_KEY   = "od:live:{sport_id}:hash"
_LIVE_CHAN_KEY   = "od:live:{sport_id}:updates"
_LIVE_SPORTS_KEY = "od:live:sports"
_UPC_DATA_KEY    = "od:upcoming:{sport_slug}:data"
_UPC_HASH_KEY    = "od:upcoming:{sport_slug}:hash"
_UPC_CHAN_KEY    = "od:upcoming:{sport_slug}:updates"


# ══════════════════════════════════════════════════════════════════════════════
# HTTP
# ══════════════════════════════════════════════════════════════════════════════

def _get(url: str, params: dict | None = None, timeout: float = 10.0) -> dict | list | None:
    for attempt in range(2):
        try:
            r = httpx.get(url, params=params, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as exc:
            logger.warning("OD HTTP %s %s (attempt %d)", exc.response.status_code, url, attempt + 1)
        except Exception as exc:  # noqa: BLE001
            logger.warning("OD request error %s (attempt %d): %s", url, attempt + 1, exc)
        if attempt == 0:
            time.sleep(0.5)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# MARKET NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _parse_specifiers(spec_str: str) -> dict[str, str]:
    """
    Parse OdiBets specifier string into key-value pairs.
    e.g. "total=2.5" → {"total": "2.5"}
         "hcp=0:1"   → {"hcp": "0:1"}
    """
    if not spec_str:
        return {}
    out: dict[str, str] = {}
    for part in spec_str.split(";"):
        if "=" in part:
            k, _, v = part.partition("=")
            out[k.strip()] = v.strip()
    return out


def _extract_line(spec_str: str) -> str:
    """Pull the canonical line from a specifier string."""
    specs = _parse_specifiers(spec_str)
    raw = specs.get("total") or specs.get("hcp") or specs.get("handicap") or ""
    return normalize_line(raw) if raw else ""


def _parse_market_group(
    sub_type_id: int | str,
    mkt_name: str,
    odds_list: list[dict],
    od_sport_id: int,
) -> dict[str, dict[str, float]]:
    """
    Parse one OdiBets market group (sub_type_id + odds list) into
    {canonical_slug → {canonical_outcome → odd}} — potentially multiple
    slugs when a market has multiple specifier values (e.g. Total at O/U 1.5, 2.5…).
    """
    if not odds_list:
        return {}

    sid = str(sub_type_id)

    # Group by specifier value so O/U 2.5 and O/U 3.5 become separate slugs
    by_spec: dict[str, list[dict]] = {}
    for odd in odds_list:
        spec = str(odd.get("special_bet_value") or "").strip()
        by_spec.setdefault(spec, []).append(odd)

    result: dict[str, dict[str, float]] = {}

    for spec, group in by_spec.items():
        line = _extract_line(spec)
        slug = normalize_od_market(sid, spec)

        outcomes: dict[str, float] = {}
        for o in group:
            try:
                val = float(o.get("odd_value") or 0)
            except (TypeError, ValueError):
                continue
            if val <= 1.0:
                continue  # suspended / void

            raw_key  = str(o.get("odd_key")  or o.get("odd_def") or "")
            display  = str(o.get("display")  or "")
            can_key  = normalize_outcome(slug, raw_key, display)
            if can_key:
                outcomes[can_key] = val

        if outcomes:
            result[slug] = outcomes

    return result


def _parse_all_markets(
    markets_raw: list[dict],
    od_sport_id: int,
) -> dict[str, dict[str, float]]:
    """Parse a full markets list (from sportevent detail) into canonical form."""
    merged: dict[str, dict[str, float]] = {}
    for mkt in markets_raw:
        sub_type_id = mkt.get("sub_type_id") or mkt.get("type_id") or 0
        mkt_name    = str(mkt.get("name") or mkt.get("type_name") or "")
        odds_list   = mkt.get("odds") or mkt.get("outcomes") or []
        if not isinstance(odds_list, list):
            continue
        parsed = _parse_market_group(sub_type_id, mkt_name, odds_list, od_sport_id)
        for slug, outcomes in parsed.items():
            if slug in merged:
                merged[slug].update(outcomes)   # merge multi-line variants
            else:
                merged[slug] = outcomes
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# MATCH NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_match(raw: dict, od_sport_id: int, is_live: bool = False) -> dict | None:
    """
    Convert one raw OdiBets API match/event dict into canonical form.
    Handles both list-endpoint rows and sportevent detail responses.
    """
    try:
        # Identity
        match_id    = str(raw.get("id") or raw.get("match_id") or raw.get("event_id") or "")
        parent_id   = str(raw.get("parent_match_id") or raw.get("parent_id") or match_id)
        betradar_id = str(raw.get("betradar_id") or raw.get("sr_id") or "") or None

        if not match_id:
            return None

        # Teams / competition
        home        = str(raw.get("home_team") or raw.get("home") or "Home")
        away        = str(raw.get("away_team") or raw.get("away") or "Away")
        competition = str(raw.get("competition_name") or raw.get("competition") or raw.get("league") or "")
        category    = str(raw.get("category") or raw.get("country") or "")

        # Sport
        raw_sport   = raw.get("sport_id") or raw.get("sport") or od_sport_id
        try:
            od_sport_id_ = int(raw_sport)
        except (TypeError, ValueError):
            od_sport_id_ = slug_to_od_sport_id(str(raw_sport).lower())
        sport_slug  = od_sport_to_slug(od_sport_id_)

        # Time
        start_time  = str(raw.get("start_time") or raw.get("event_date") or raw.get("date") or "")

        # Live state
        current_score = str(raw.get("current_score") or raw.get("score") or "")
        match_time    = str(raw.get("match_time") or raw.get("game_time") or "")
        event_status  = str(raw.get("event_status") or raw.get("status") or "")
        bet_status    = str(raw.get("bet_status") or "")
        score_parts   = current_score.split(":") if ":" in current_score else current_score.split("-")
        score_home    = score_parts[0].strip() if len(score_parts) >= 2 else None
        score_away    = score_parts[1].strip() if len(score_parts) >= 2 else None

        # Markets — handle both list format and detail format
        markets_raw  = raw.get("markets") or raw.get("odds") or []
        if isinstance(markets_raw, list):
            markets = _parse_all_markets(markets_raw, od_sport_id_)
        elif isinstance(markets_raw, dict):
            # Already parsed (e.g. cached entry)
            markets = markets_raw
        else:
            markets = {}

        # Inline top-level 1X2 odds (list endpoint quick odds)
        if "1x2" not in markets:
            try:
                ho = float(raw.get("home_odd") or raw.get("h_odd") or 0)
                no = float(raw.get("draw_odd") or raw.get("d_odd") or raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or raw.get("a_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    markets["1x2"] = {
                        k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1
                    }
            except (TypeError, ValueError):
                pass

        return {
            # Identity
            "od_match_id":   match_id,
            "od_event_id":   match_id,
            "od_parent_id":  parent_id,
            "sp_game_id":    None,
            "betradar_id":   betradar_id,
            # Teams / competition
            "home_team":     home,
            "away_team":     away,
            "competition":   competition,
            "category":      category,
            "sport":         sport_slug,
            "od_sport_id":   od_sport_id_,
            "start_time":    start_time,
            # Source
            "source":        "odibets",
            "is_live":       is_live,
            "is_suspended":  bet_status in ("STOPPED", "BET_STOP", "SUSPENDED"),
            # Live state
            "match_time":    match_time,
            "event_status":  event_status,
            "bet_status":    bet_status,
            "current_score": current_score,
            "score_home":    score_home,
            "score_away":    score_away,
            # Markets
            "markets":       markets,
            "market_count":  len(markets),
        }

    except Exception as exc:  # noqa: BLE001
        logger.debug("OD match normalise error: %s | raw=%s", exc, str(raw)[:200])
        return None


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_matches(
    sport_slug:     str  = "soccer",
    day:            str  = "",
    competition_id: str  = "",
    sub_type_id:    int  = 1,
    mode:           int  = 1,
    # Celery task compat params — ignored but accepted
    fetch_full_markets: bool = False,
    fetch_extended:     bool = False,
    max_matches:        int | None = None,
    **kwargs,
) -> list[dict]:
    """
    Fetch upcoming matches from:
    GET https://api.odi.site/odi/sportsbook?resource=sportevents&platform=mobile&mode=1
        &sport_id=1&day=YYYY-MM-DD&sub_type_id=1
    """
    od_sport_id = slug_to_od_sport_id(sport_slug)
    params: dict[str, Any] = {
        "resource":   "sportevents",
        "platform":   "mobile",
        "mode":       mode,
        "sport_id":   od_sport_id,
        "sub_type_id": sub_type_id,
    }
    if day:
        params["day"] = day
    if competition_id:
        params["competition_id"] = competition_id

    data = _get(SBOOK_ODI, params=params)
    if not data:
        return []

    # Response can be a list or {"data": [...], "events": [...]}
    if isinstance(data, list):
        raw_events = data
    elif isinstance(data, dict):
        raw_events = (
            data.get("data") or data.get("events") or
            data.get("matches") or data.get("results") or []
        )
    else:
        raw_events = []

    matches = []
    for raw in raw_events:
        if not isinstance(raw, dict):
            continue
        m = _normalise_match(raw, od_sport_id, is_live=False)
        if m:
            matches.append(m)

    logger.info("OD upcoming %s: %d matches", sport_slug, len(matches))
    return matches


def fetch_upcoming_all_sports(
    sports: list[str] | None = None,
    day:    str = "",
) -> list[dict]:
    """Fetch upcoming across multiple sports concurrently."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    if sports is None:
        sports = ["soccer", "basketball", "tennis", "cricket"]

    all_matches: list[dict] = []
    with ThreadPoolExecutor(max_workers=len(sports)) as pool:
        futs = {pool.submit(fetch_upcoming_matches, s, day): s for s in sports}
        for fut in as_completed(futs):
            try:
                all_matches.extend(fut.result())
            except Exception as exc:  # noqa: BLE001
                logger.warning("OD upcoming all sports error: %s", exc)
    return all_matches


# ══════════════════════════════════════════════════════════════════════════════
# LIVE
# ══════════════════════════════════════════════════════════════════════════════

def fetch_live_matches(sport_slug: str | None = None) -> list[dict]:
    """
    Fetch currently live matches from:
    GET https://api.odi.site/sportsbook/v1?resource=live&sport_id=&sub_type_id=
    """
    params: dict[str, Any] = {
        "resource":   "live",
        "sportsbook": "sportsbook",
        "ua":         HEADERS["user-agent"],
        "sub_type_id": "",
        "sport_id":   "",
    }
    if sport_slug:
        params["sport_id"] = slug_to_od_sport_id(sport_slug)

    data = _get(SBOOK_V1, params=params)
    if not data:
        return []

    if isinstance(data, list):
        raw_events = data
    elif isinstance(data, dict):
        raw_events = (
            data.get("data") or data.get("events") or
            data.get("matches") or data.get("live") or []
        )
    else:
        raw_events = []

    matches = []
    for raw in raw_events:
        if not isinstance(raw, dict):
            continue
        od_sport_id = int(raw.get("sport_id") or 1)
        m = _normalise_match(raw, od_sport_id, is_live=True)
        if m:
            matches.append(m)

    logger.info("OD live: %d matches (sport=%s)", len(matches), sport_slug or "all")
    return matches


# ══════════════════════════════════════════════════════════════════════════════
# MATCH DETAIL (single event — all markets)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_event_detail(event_id: str | int, od_sport_id: int = 1) -> tuple[dict[str, dict[str, float]], dict]:
    """
    Fetch full market list for one event.
    GET https://api.odi.site/sportsbook/v1?resource=sportevent&id={event_id}

    Returns (markets, meta_dict).
    """
    params = {
        "resource":   "sportevent",
        "id":         str(event_id),
        "category_id": "",
        "sub_type_id": "",
        "builder":    0,
        "sportsbook": "sportsbook",
        "ua":         HEADERS["user-agent"],
    }
    data = _get(SBOOK_V1, params=params)
    if not data:
        return {}, {}

    if isinstance(data, dict):
        raw_event   = data.get("event") or data.get("data") or data
        markets_raw = raw_event.get("markets") or raw_event.get("odds") or []
        meta = {
            "home_team":     raw_event.get("home_team", ""),
            "away_team":     raw_event.get("away_team", ""),
            "competition":   raw_event.get("competition_name") or raw_event.get("competition", ""),
            "category":      raw_event.get("category", ""),
            "start_time":    raw_event.get("start_time", ""),
            "current_score": raw_event.get("current_score", ""),
            "match_time":    raw_event.get("match_time", ""),
            "event_status":  raw_event.get("event_status", ""),
            "bet_status":    raw_event.get("bet_status", ""),
        }
    elif isinstance(data, list):
        markets_raw = data
        meta = {}
    else:
        return {}, {}

    markets = _parse_all_markets(markets_raw, od_sport_id)
    return markets, meta


# ══════════════════════════════════════════════════════════════════════════════
# LIVE POLLER (background thread)
# ══════════════════════════════════════════════════════════════════════════════

def _payload_hash(obj: Any) -> str:
    return hashlib.md5(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class OdiBetsLivePoller:
    """
    Background thread: polls /sportsbook/v1?resource=live every `interval` seconds,
    detects per-outcome changes, publishes deltas to Redis pub/sub.

    Redis channels:
      od:live:{sport_id}:updates  — JSON delta events (same shape as Betika/SP)
      od:live:{sport_id}:data     — Full snapshot (TTL 60 s)
    """

    def __init__(
        self,
        redis_client: Any,
        interval:     float = 2.0,   # polling interval seconds
    ) -> None:
        self.redis       = redis_client
        self.interval    = interval
        self._running    = False
        self._thread:    threading.Thread | None = None
        self._prev_hashes: dict[int, str]  = {}
        self._prev_odds:   dict[str, float] = {}   # "od_match_id:slug:outcome" → float

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._poll_loop, daemon=True, name="od-live")
        self._thread.start()
        logger.info("OdiBetsLivePoller started (interval=%.1fs)", self.interval)

    def stop(self) -> None:
        self._running = False

    @property
    def alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # ── Internal ──────────────────────────────────────────────────────────────

    def _poll_loop(self) -> None:
        while self._running:
            tick = time.time()
            try:
                matches = fetch_live_matches()
                if matches is not None:
                    self._process_batch(matches)
            except Exception as exc:  # noqa: BLE001
                logger.error("OD live poll error: %s", exc)
            elapsed = time.time() - tick
            time.sleep(max(0.1, self.interval - elapsed))

    def _process_batch(self, matches: list[dict]) -> None:
        """Group by sport, detect changes, publish to Redis."""
        by_sport: dict[int, list[dict]] = {}
        for m in matches:
            by_sport.setdefault(m["od_sport_id"], []).append(m)

        for od_sport_id, sport_matches in by_sport.items():
            new_hash = _payload_hash(sport_matches)
            if new_hash == self._prev_hashes.get(od_sport_id, ""):
                continue

            self._prev_hashes[od_sport_id] = new_hash
            sport_slug = od_sport_to_slug(od_sport_id)

            # Write full snapshot to Redis
            self.redis.set(
                _LIVE_DATA_KEY.format(sport_id=od_sport_id),
                json.dumps(sport_matches, ensure_ascii=False),
                ex=60,
            )

            # Build delta events
            events = self._build_delta_events(sport_matches, od_sport_id)
            if events:
                channel = _LIVE_CHAN_KEY.format(sport_id=od_sport_id)
                payload = json.dumps({
                    "type":       "batch_update",
                    "sport_id":   od_sport_id,
                    "sport_slug": sport_slug,
                    "events":     events,
                    "total":      len(sport_matches),
                    "ts":         time.time(),
                }, ensure_ascii=False)
                self.redis.publish(channel, payload)

                logger.debug(
                    "OD live delta: sport=%s matches=%d events=%d",
                    sport_slug, len(sport_matches), len(events),
                )

    def _build_delta_events(
        self,
        matches:     list[dict],
        od_sport_id: int,
    ) -> list[dict]:
        """
        Compare current odds against previous snapshot.
        Returns only changed/new outcome events — same shape as Betika/SP.
        """
        events: list[dict] = []
        for m in matches:
            mid     = m["od_match_id"]
            markets = m.get("markets") or {}

            for slug, outcomes in markets.items():
                for outcome_key, odd_val in outcomes.items():
                    cache_key = f"{mid}:{slug}:{outcome_key}"
                    prev_val  = self._prev_odds.get(cache_key)

                    if prev_val is None or abs(odd_val - prev_val) > 0.001:
                        self._prev_odds[cache_key] = odd_val
                        events.append({
                            "type":        "market_update",
                            "match_id":    mid,
                            "home_team":   m.get("home_team", ""),
                            "away_team":   m.get("away_team", ""),
                            "match_time":  m.get("match_time"),
                            "score_home":  m.get("score_home"),
                            "score_away":  m.get("score_away"),
                            "market_slug": slug,
                            "outcome_key": outcome_key,
                            "odd":         odd_val,
                            "prev_odd":    prev_val,
                            "is_new":      prev_val is None,
                        })

        return events


# ── Module-level singleton ────────────────────────────────────────────────────
_live_poller: OdiBetsLivePoller | None = None


def get_live_poller() -> OdiBetsLivePoller | None:
    return _live_poller


def init_live_poller(redis_client: Any, interval: float = 2.0) -> OdiBetsLivePoller:
    global _live_poller  # noqa: PLW0603
    if _live_poller is None or not _live_poller.alive:
        _live_poller = OdiBetsLivePoller(redis_client, interval=interval)
        _live_poller.start()
    return _live_poller


# ══════════════════════════════════════════════════════════════════════════════
# REDIS CACHE HELPERS  (used by od_module.py Flask views)
# ══════════════════════════════════════════════════════════════════════════════

def get_cached_upcoming(redis_client: Any, sport_slug: str) -> list[dict] | None:
    key = _UPC_DATA_KEY.format(sport_slug=sport_slug)
    try:
        raw = redis_client.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def cache_upcoming(
    redis_client: Any,
    sport_slug:   str,
    matches:      list[dict],
    ttl:          int = 300,
) -> None:
    key = _UPC_DATA_KEY.format(sport_slug=sport_slug)
    try:
        redis_client.set(key, json.dumps(matches, ensure_ascii=False), ex=ttl)
    except Exception as exc:
        logger.warning("OD cache_upcoming error: %s", exc)


def get_cached_live(redis_client: Any, od_sport_id: int) -> list[dict] | None:
    key = _LIVE_DATA_KEY.format(sport_id=od_sport_id)
    try:
        raw = redis_client.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# REGISTRY PLUGIN
# ══════════════════════════════════════════════════════════════════════════════

class OdiBetsHarvesterPlugin:
    """
    Registry-compatible plugin. Wire into harvest_registry.py:

        from app.workers.od_harvester import OdiBetsHarvesterPlugin
        from app.workers.harvest_registry import register_bookmaker
        register_bookmaker(OdiBetsHarvesterPlugin())
    """

    bookie_id   = "odibets"
    bookie_name = "OdiBets"
    sport_slugs = list(OD_SPORT_IDS.keys())

    def fetch_upcoming(
        self,
        sport_slug: str,
        day:        str = "",
        **kwargs,
    ) -> list[dict]:
        return fetch_upcoming_matches(sport_slug, day=day)

    def fetch_live(self, sport_slug: str | None = None) -> list[dict]:
        return fetch_live_matches(sport_slug)


# ── Celery task compatibility aliases ─────────────────────────────────────────
# tasks_live.py and tasks_upcoming.py call:
#   from app.workers.od_harvester import fetch_live, fetch_upcoming
# These are thin wrappers that absorb any extra kwargs.

def fetch_upcoming(
    sport_slug:         str  = "soccer",
    fetch_full_markets: bool = False,
    fetch_extended:     bool = False,
    max_matches:        int | None = None,
    **kwargs,
) -> list[dict]:
    """Celery-task-compatible alias for fetch_upcoming_matches."""
    return fetch_upcoming_matches(sport_slug, **kwargs)


def fetch_live(
    sport_slug:         str | None = None,
    fetch_full_markets: bool       = False,
    **kwargs,
) -> list[dict]:
    """Celery-task-compatible alias for fetch_live_matches."""
    return fetch_live_matches(sport_slug)