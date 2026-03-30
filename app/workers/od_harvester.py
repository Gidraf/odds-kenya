"""
app/workers/od_harvester.py
============================
OdiBets upcoming + live harvester.

API response shapes (confirmed 2026-03-30)
──────────────────────────────────────────
Upcoming  markets[i]:
  {
    "sub_type_id": "1",
    "odd_type":    "1X2",
    "lines": [                          ← outcomes nested under lines[]
      {
        "specifiers": "",
        "outcomes": [
          { "outcome_key": "1", "odd_value": "1.15", "active": 1, ... }
        ]
      }
    ]
  }

Live  markets[i]:
  {
    "sub_type_id": "1",
    "odd_type":    "1X2",
    "status": 1,
    "outcomes": [                       ← outcomes directly on market
      { "outcome_key": "1", "odd_value": "3.20", "active": 1, "specifiers": "", ... }
    ]
  }

Key differences vs old assumptions
────────────────────────────────────
• Upcoming wraps outcomes in lines[] — old code looked for mkt["odds"] / mkt["outcomes"] directly
• outcome_key for Double Chance is "1 or X" / "1 or 2" / "X or 2" — normalised to "1X" / "12" / "X2"
• game_id is the match id field (upcoming); id / match_id (live)
• sport_id is a string slug on upcoming ("soccer"), int on live (1)
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from datetime import date as _date
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
    "soccer":            1,
    "basketball":        2,
    "tennis":            3,
    "cricket":           4,
    "rugby":             5,
    "ice-hockey":        6,
    "volleyball":        7,
    "handball":          8,
    "table-tennis":      9,
    "baseball":          10,
    "american-football": 11,
    "mma":               15,
    "boxing":            16,
    "darts":             17,
    "esoccer":           1001,
}

OD_SPORT_SLUGS: dict[int, str] = {v: k for k, v in OD_SPORT_IDS.items()}

# OdiBets string sport_id values seen in API responses
_OD_STRING_SPORT_MAP: dict[str, str] = {
    "soccer":            "soccer",
    "football":          "soccer",
    "internationals":    "soccer",
    "itl":               "soccer",
    "basketball":        "basketball",
    "tennis":            "tennis",
    "cricket":           "cricket",
    "rugby":             "rugby",
    "rugby union":       "rugby",
    "rugby league":      "rugby",
    "ice-hockey":        "ice-hockey",
    "icehockey":         "ice-hockey",
    "ice hockey":        "ice-hockey",
    "volleyball":        "volleyball",
    "handball":          "handball",
    "table-tennis":      "table-tennis",
    "tabletennis":       "table-tennis",
    "table tennis":      "table-tennis",
    "baseball":          "baseball",
    "mma":               "mma",
    "boxing":            "boxing",
    "darts":             "darts",
    "american football": "american-football",
    "american-football": "american-football",
    "esoccer":           "esoccer",
    "e-soccer":          "esoccer",
}


def slug_to_od_sport_id(slug: str) -> int:
    return OD_SPORT_IDS.get(slug, 1)


def od_sport_to_slug(sport_id: int) -> str:
    return OD_SPORT_SLUGS.get(sport_id, "soccer")


def _resolve_sport(raw_sport: Any, fallback_od_id: int) -> tuple[int, str]:
    """Resolve raw sport value (int id OR string slug) to (od_sport_id, sport_slug)."""
    if raw_sport is None:
        return fallback_od_id, od_sport_to_slug(fallback_od_id)
    try:
        od_id = int(raw_sport)
        return od_id, od_sport_to_slug(od_id)
    except (TypeError, ValueError):
        pass
    str_val   = str(raw_sport).lower().strip()
    canonical = _OD_STRING_SPORT_MAP.get(str_val)
    if canonical:
        return slug_to_od_sport_id(canonical), canonical
    od_id = OD_SPORT_IDS.get(str_val)
    if od_id:
        return od_id, str_val
    return fallback_od_id, od_sport_to_slug(fallback_od_id)


# ══════════════════════════════════════════════════════════════════════════════
# API ENDPOINTS + HEADERS
# ══════════════════════════════════════════════════════════════════════════════

API_BASE  = "https://api.odi.site"
SBOOK_V1  = f"{API_BASE}/sportsbook/v1"
SBOOK_ODI = f"{API_BASE}/odi/sportsbook"

HEADERS: dict[str, str] = {
    "accept":             "application/json, text/plain, */*",
    "accept-language":    "en-GB,en;q=0.9",
    "authorization":      "Bearer",
    "content-type":       "application/json",
    "origin":             "https://odibets.com",
    "referer":            "https://odibets.com/",
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
# RESPONSE UNWRAPPING
# ══════════════════════════════════════════════════════════════════════════════

def _unwrap_upcoming_response(data: dict | list, fallback_sport_id: int) -> list[dict]:
    """
    Unwrap the confirmed shape:
      { "data": { "leagues": [ { "competition_name": "...", "matches": [...] } ] } }
    """
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []

    inner = data.get("data")

    if isinstance(inner, list):
        return inner

    if isinstance(inner, dict):
        leagues: list[dict] = inner.get("leagues") or []
        raw_events: list[dict] = []
        for league in leagues:
            comp_name = str(league.get("competition_name") or "")
            cat_name  = str(league.get("category_name")   or "")
            for m in (league.get("matches") or []):
                if not isinstance(m, dict):
                    continue
                if not m.get("competition_name") and comp_name:
                    m["competition_name"] = comp_name
                if not m.get("category_name") and cat_name:
                    m["category_name"] = cat_name
                raw_events.append(m)
        if raw_events:
            return raw_events
        for key in ("events", "matches", "results", "sport_events", "sportevents"):
            if isinstance(inner.get(key), list) and inner[key]:
                return inner[key]
        return []

    for key in ("events", "matches", "results", "sport_events", "sportevents"):
        if isinstance(data.get(key), list) and data[key]:
            return data[key]
    return []


def _unwrap_live_response(data: dict | list) -> list[dict]:
    """Unwrap live endpoint response into a flat list of match dicts."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    inner = data.get("data")
    if isinstance(inner, list):
        return inner
    if isinstance(inner, dict):
        for key in ("matches", "events", "live", "results"):
            if isinstance(inner.get(key), list) and inner[key]:
                return inner[key]
    for key in ("matches", "events", "live", "results", "data"):
        candidate = data.get(key)
        if isinstance(candidate, list) and candidate:
            return candidate
    return []


# ══════════════════════════════════════════════════════════════════════════════
# OUTCOME KEY NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

# OdiBets Double Chance sends verbose keys like "1 or X" — normalise to canonical
_DC_KEY_MAP: dict[str, str] = {
    "1 or x":  "1X",  "1 or X":  "1X",
    "x or 2":  "X2",  "X or 2":  "X2",
    "1 or 2":  "12",
    "1x":      "1X",  "x2":      "X2",
    "1X":      "1X",  "X2":      "X2",  "12": "12",
}

_BTTS_KEY_MAP: dict[str, str] = {
    "yes": "yes", "no": "no",
    "gg":  "yes", "ng": "no",
    "Yes": "yes", "No": "no",
    "GG":  "yes", "NG": "no",
}


def _normalise_outcome_key(slug: str, raw_key: str, outcome_name: str = "") -> str:
    """Map OdiBets raw outcome_key to canonical form."""
    rk   = str(raw_key).strip()
    rk_l = rk.lower()

    if "double_chance" in slug:
        mapped = _DC_KEY_MAP.get(rk) or _DC_KEY_MAP.get(rk_l)
        if mapped:
            return mapped

    if "btts" in slug:
        mapped = _BTTS_KEY_MAP.get(rk) or _BTTS_KEY_MAP.get(rk_l)
        if mapped:
            return mapped

    if rk_l in ("over", "under"):
        return rk_l

    if rk in ("1", "X", "2"):
        return rk
    if rk == "x":
        return "X"

    can = normalize_outcome(slug, rk, outcome_name)
    return can if can else rk


# ══════════════════════════════════════════════════════════════════════════════
# MARKET NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _parse_specifiers(spec_str: str) -> dict[str, str]:
    if not spec_str:
        return {}
    out: dict[str, str] = {}
    for part in spec_str.split(";"):
        if "=" in part:
            k, _, v = part.partition("=")
            out[k.strip()] = v.strip()
    return out


def _extract_line(spec_str: str) -> str:
    specs = _parse_specifiers(spec_str)
    raw = specs.get("total") or specs.get("hcp") or specs.get("handicap") or ""
    return normalize_line(raw) if raw else ""


def _flat_outcomes_from_market(mkt: dict) -> list[tuple[dict, str]]:
    """
    Yield (outcome_dict, specifier_str) pairs handling both shapes:

    Shape A — live:     market.outcomes[]
    Shape B — upcoming: market.lines[].outcomes[]
    """
    results: list[tuple[dict, str]] = []

    # Shape A — outcomes directly on market (live endpoint)
    direct = mkt.get("outcomes")
    if isinstance(direct, list) and direct:
        for o in direct:
            if isinstance(o, dict):
                spec = str(o.get("specifiers") or "").strip()
                results.append((o, spec))
        return results

    # Shape B — outcomes nested under lines[] (upcoming endpoint)
    lines = mkt.get("lines")
    if isinstance(lines, list):
        for line in lines:
            if not isinstance(line, dict):
                continue
            line_spec = str(line.get("specifiers") or "").strip()
            for o in (line.get("outcomes") or []):
                if isinstance(o, dict):
                    spec = line_spec or str(o.get("specifiers") or "").strip()
                    results.append((o, spec))
        return results

    # Legacy fallback — "odds" list
    odds = mkt.get("odds")
    if isinstance(odds, list):
        for o in odds:
            if isinstance(o, dict):
                spec = str(o.get("special_bet_value") or o.get("specifiers") or "").strip()
                results.append((o, spec))

    return results


def _parse_market_group(
    sub_type_id: int | str,
    mkt_name:    str,
    mkt:         dict,
    od_sport_id: int,
) -> dict[str, dict[str, float]]:
    """
    Parse one market dict → {canonical_slug: {outcome: odd}}.
    Handles both upcoming (lines[]) and live (outcomes[]) shapes.
    """
    flat = _flat_outcomes_from_market(mkt)
    if not flat:
        return {}

    sid = str(sub_type_id).strip()

    # Group by specifier so O/U 2.5 and O/U 3.5 become separate canonical slugs
    by_spec: dict[str, list[tuple[dict, str]]] = {}
    for outcome, spec in flat:
        by_spec.setdefault(spec, []).append((outcome, spec))

    result: dict[str, dict[str, float]] = {}

    for spec, group in by_spec.items():
        slug = normalize_od_market(sid, spec)

        outcomes: dict[str, float] = {}
        for o, _ in group:
            # Skip suspended / inactive
            active = o.get("active")
            status = o.get("status")
            if active is not None and str(active) in ("0", "false"):
                continue
            if status is not None and str(status) == "0":
                continue

            try:
                val = float(o.get("odd_value") or 0)
            except (TypeError, ValueError):
                continue
            if val <= 1.0:
                continue

            raw_key      = str(o.get("outcome_key") or o.get("odd_key") or o.get("odd_def") or "")
            outcome_name = str(o.get("outcome_name") or o.get("display") or "")
            can_key      = _normalise_outcome_key(slug, raw_key, outcome_name)
            if can_key:
                outcomes[can_key] = val

        if outcomes:
            result[slug] = outcomes

    return result


def _parse_all_markets(
    markets_raw: list[dict],
    od_sport_id: int,
) -> dict[str, dict[str, float]]:
    """Parse a full markets list into canonical form."""
    merged: dict[str, dict[str, float]] = {}
    for mkt in markets_raw:
        if not isinstance(mkt, dict):
            continue
        sub_type_id = mkt.get("sub_type_id") or mkt.get("type_id") or 0
        mkt_name    = str(mkt.get("odd_type") or mkt.get("name") or mkt.get("type_name") or "")
        parsed      = _parse_market_group(sub_type_id, mkt_name, mkt, od_sport_id)
        for slug, outcomes in parsed.items():
            if slug in merged:
                merged[slug].update(outcomes)
            else:
                merged[slug] = outcomes
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# MATCH NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _normalise_match(raw: dict, od_sport_id: int, is_live: bool = False) -> dict | None:
    """Convert one raw OdiBets match dict into canonical form."""
    try:
        # game_id = upcoming; id / match_id = live/detail
        match_id = str(
            raw.get("game_id")  or raw.get("id") or
            raw.get("match_id") or raw.get("event_id") or ""
        )
        parent_id   = str(raw.get("parent_match_id") or raw.get("parent_id") or match_id)
        betradar_id = str(raw.get("betradar_id") or raw.get("sr_id") or "") or None

        if not match_id:
            return None

        home        = str(raw.get("home_team") or raw.get("home") or "Home")
        away        = str(raw.get("away_team") or raw.get("away") or "Away")
        competition = str(raw.get("competition_name") or raw.get("competition") or raw.get("league") or "")
        category    = str(
            raw.get("category_name") or raw.get("category") or
            raw.get("country_name")  or raw.get("country") or ""
        )

        raw_sport = raw.get("sport_id") or raw.get("sport") or raw.get("s_binomen")
        od_sport_id_, sport_slug = _resolve_sport(raw_sport, od_sport_id)

        start_time = str(raw.get("start_time") or raw.get("event_date") or raw.get("date") or "")

        current_score = str(
            raw.get("current_score") or raw.get("score") or raw.get("result") or ""
        )
        match_time = str(
            raw.get("match_time") or raw.get("game_time") or raw.get("periodic_time") or ""
        )
        event_status = str(
            raw.get("event_status") or raw.get("status_desc") or raw.get("status") or ""
        )
        bet_status = str(raw.get("bet_status") or raw.get("b_status") or "")

        score_parts = (
            current_score.split(":") if ":" in current_score
            else current_score.split("-") if "-" in current_score
            else []
        )
        score_home = score_parts[0].strip() if len(score_parts) >= 2 else None
        score_away = score_parts[1].strip() if len(score_parts) >= 2 else None

        markets_raw = raw.get("markets") or raw.get("odds") or []
        if isinstance(markets_raw, list):
            markets = _parse_all_markets(markets_raw, od_sport_id_)
        elif isinstance(markets_raw, dict):
            markets = markets_raw
        else:
            markets = {}

        # Inline 1X2 quick-odds fallback
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
            "od_match_id":   match_id,
            "od_event_id":   match_id,
            "od_parent_id":  parent_id,
            "sp_game_id":    None,
            "betradar_id":   betradar_id,
            "home_team":     home,
            "away_team":     away,
            "competition":   competition,
            "category":      category,
            "sport":         sport_slug,
            "od_sport_id":   od_sport_id_,
            "start_time":    start_time,
            "source":        "odibets",
            "is_live":       is_live,
            "is_suspended":  bet_status in ("STOPPED", "BET_STOP", "SUSPENDED"),
            "match_time":    match_time,
            "event_status":  event_status,
            "bet_status":    bet_status,
            "current_score": current_score,
            "score_home":    score_home,
            "score_away":    score_away,
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
    sport_slug:         str        = "soccer",
    day:                str        = "",
    competition_id:     str        = "",
    sub_type_id:        int        = 1,
    mode:               int        = 1,
    fetch_full_markets: bool       = False,
    fetch_extended:     bool       = False,
    max_matches:        int | None = None,
    **kwargs,
) -> list[dict]:
    """
    Fetch upcoming matches. Defaults day to today — OdiBets returns empty
    leagues[] without a day param.
    """
    od_sport_id = slug_to_od_sport_id(sport_slug)

    if not day:
        day = _date.today().isoformat()

    params: dict[str, Any] = {
        "resource":    "sportevents",
        "platform":    "mobile",
        "mode":        mode,
        "sport_id":    od_sport_id,
        "sub_type_id": sub_type_id,
        "day":         day,
    }
    if competition_id:
        params["competition_id"] = competition_id

    data = _get(SBOOK_ODI, params=params)
    if not data:
        logger.warning("OD upcoming %s %s: no response", sport_slug, day)
        return []

    raw_events = _unwrap_upcoming_response(data, od_sport_id)
    if not raw_events:
        logger.warning("OD upcoming %s %s: 0 events after unwrap", sport_slug, day)
        return []

    matches: list[dict] = []
    for raw in raw_events:
        if not isinstance(raw, dict):
            continue
        m = _normalise_match(raw, od_sport_id, is_live=False)
        if m:
            matches.append(m)

    logger.info("OD upcoming %s %s: %d matches", sport_slug, day, len(matches))
    return matches


def fetch_upcoming_all_sports(
    sports: list[str] | None = None,
    day:    str = "",
) -> list[dict]:
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
    Fetch live matches. Live endpoint returns outcomes[] directly on each
    market (no lines[] wrapper).
    """
    params: dict[str, Any] = {
        "resource":    "live",
        "sportsbook":  "sportsbook",
        "ua":          HEADERS["user-agent"],
        "sub_type_id": "",
        "sport_id":    "",
    }
    if sport_slug:
        params["sport_id"] = slug_to_od_sport_id(sport_slug)

    data = _get(SBOOK_V1, params=params)
    if not data:
        return []

    raw_events = _unwrap_live_response(data)

    matches: list[dict] = []
    for raw in raw_events:
        if not isinstance(raw, dict):
            continue
        try:
            raw_sport_id = int(raw.get("sport_id") or 1)
        except (TypeError, ValueError):
            raw_sport_id = 1
        m = _normalise_match(raw, raw_sport_id, is_live=True)
        if m:
            matches.append(m)

    logger.info("OD live: %d matches (sport=%s)", len(matches), sport_slug or "all")
    return matches


# ══════════════════════════════════════════════════════════════════════════════
# MATCH DETAIL
# ══════════════════════════════════════════════════════════════════════════════

def fetch_event_detail(
    event_id:    str | int,
    od_sport_id: int = 1,
) -> tuple[dict[str, dict[str, float]], dict]:
    params = {
        "resource":    "sportevent",
        "id":          str(event_id),
        "category_id": "",
        "sub_type_id": "",
        "builder":     0,
        "sportsbook":  "sportsbook",
        "ua":          HEADERS["user-agent"],
    }
    data = _get(SBOOK_V1, params=params)
    if not data:
        return {}, {}

    if isinstance(data, dict):
        inner     = data.get("data") or data
        raw_event = (inner.get("event") if isinstance(inner, dict) else None) or inner
        if not isinstance(raw_event, dict):
            raw_event = data

        markets_raw = raw_event.get("markets") or raw_event.get("odds") or []
        meta = {
            "home_team":     raw_event.get("home_team", ""),
            "away_team":     raw_event.get("away_team", ""),
            "competition":   raw_event.get("competition_name") or raw_event.get("competition", ""),
            "category":      raw_event.get("category_name") or raw_event.get("category", ""),
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
    return hashlib.md5(
        json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()


class OdiBetsLivePoller:
    def __init__(self, redis_client: Any, interval: float = 2.0) -> None:
        self.redis         = redis_client
        self.interval      = interval
        self._running      = False
        self._thread:      threading.Thread | None = None
        self._prev_hashes: dict[int, str]   = {}
        self._prev_odds:   dict[str, float] = {}

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
        by_sport: dict[int, list[dict]] = {}
        for m in matches:
            by_sport.setdefault(m["od_sport_id"], []).append(m)

        for od_sport_id, sport_matches in by_sport.items():
            new_hash = _payload_hash(sport_matches)
            if new_hash == self._prev_hashes.get(od_sport_id, ""):
                continue
            self._prev_hashes[od_sport_id] = new_hash
            sport_slug = od_sport_to_slug(od_sport_id)

            self.redis.set(
                _LIVE_DATA_KEY.format(sport_id=od_sport_id),
                json.dumps(sport_matches, ensure_ascii=False),
                ex=60,
            )

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

    def _build_delta_events(self, matches: list[dict], od_sport_id: int) -> list[dict]:
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
# REDIS CACHE HELPERS
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
    bookie_id   = "odibets"
    bookie_name = "OdiBets"
    sport_slugs = list(OD_SPORT_IDS.keys())

    def fetch_upcoming(self, sport_slug: str, day: str = "", **kwargs) -> list[dict]:
        return fetch_upcoming_matches(sport_slug, day=day)

    def fetch_live(self, sport_slug: str | None = None) -> list[dict]:
        return fetch_live_matches(sport_slug)


# ══════════════════════════════════════════════════════════════════════════════
# CELERY TASK ALIASES
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming(
    sport_slug:         str        = "soccer",
    fetch_full_markets: bool       = False,
    fetch_extended:     bool       = False,
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