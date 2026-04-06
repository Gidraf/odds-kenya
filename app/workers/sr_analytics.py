"""
app/workers/sr_analytics.py
==============================
Sportradar widget analytics client — SportPesa flavour.

Base URL:
  https://stats.fn.sportradar.com/sportpesa/en/Europe:Moscow/gismo/

Endpoints covered
──────────────────
  config_sports/{config_id}               → sports list covered by SP's SR widget
  stats_match_get/{sr_match_id}           → full match details + stat coverage flags
  stats_team_nextx/{team_uid}/{days}      → team's upcoming fixtures (next N days)
  stats_season_fixtures2/{season_id}/{p}  → all fixtures + cup bracket for a season

The sr_match_id stored on each SP harvested match (betradar_id field) maps
directly to the SR match ID used in these endpoints — no translation needed.

Typical usage (inside a Celery task)
──────────────────────────────────────
  from app.workers.sr_analytics import get_match_analytics, get_config_sports

  # Full bundle: match details + home/away next fixtures + season bracket
  bundle = get_match_analytics(betradar_id)
  cache_set(f"sr:analytics:{betradar_id}", bundle, ttl=86400)

  # Just match details (lightweight)
  details = get_match_details(betradar_id)
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# HTTP session
# ─────────────────────────────────────────────────────────────────────────────

_BASE = "https://stats.fn.sportradar.com/sportpesa/en/Europe:Moscow/gismo"

_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":             "*/*",
    "Accept-Language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "Origin":             "https://s5.sir.sportradar.com",
    "Referer":            "https://s5.sir.sportradar.com/",
    "sec-ch-ua":          '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile":   "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-site",
}

_SR_SESSION = requests.Session()
_SR_SESSION.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
        ),
        pool_connections=5,
        pool_maxsize=10,
    ),
)


# ─────────────────────────────────────────────────────────────────────────────
# Low-level helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get(endpoint: str, timeout: int = 15) -> dict | None:
    url = f"{_BASE}/{endpoint}"
    try:
        r = _SR_SESSION.get(url, headers=_HEADERS, timeout=timeout)
        if r.status_code == 304:
            return None
        if r.ok:
            return r.json()
        logger.warning("[sr] HTTP %s → %s", r.status_code, url)
    except Exception as exc:
        logger.warning("[sr] request error %s: %s", url, exc)
    return None


def _extract_doc_data(raw: dict | None) -> Any:
    """
    Unwrap the standard Sportradar GISMO envelope:
      { "doc": [ { "event": "...", "data": <payload> } ] }
    Returns the payload or None.
    """
    if not raw or not isinstance(raw, dict):
        return None
    docs = raw.get("doc") or []
    if docs and isinstance(docs[0], dict):
        return docs[0].get("data")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def get_config_sports(config_id: int = 41) -> list[dict]:
    """
    config_sports/{config_id}[/{version}]
    Returns the list of sports available in the SP Sportradar widget.
    Response cached by SR for up to 600 s.
    """
    raw  = _get(f"config_sports/{config_id}")
    data = _extract_doc_data(raw)
    if isinstance(data, list):
        return data
    return []


def get_match_details(sr_match_id: str | int) -> dict | None:
    """
    stats_match_get/{sr_match_id}
    Full match record including:
      - teams (home/away) with uid, abbr, cc
      - result + period scores
      - tournament, season, stadium
      - statscoverage flags (headtohead, tennisranking, …)
      - history.previous / history.next for navigation
    Returns the data dict or None on failure.
    """
    raw  = _get(f"stats_match_get/{sr_match_id}")
    data = _extract_doc_data(raw)
    return data if isinstance(data, dict) else None


def get_team_next_matches(team_uid: str | int, days: int = 30) -> dict:
    """
    stats_team_nextx/{team_uid}/{days}
    Returns:
      {
        "team":        { ... uniqueteam ... },
        "matches":     [ ... upcoming match dicts ... ],
        "tournaments": { id: { ... } },
      }
    """
    raw  = _get(f"stats_team_nextx/{team_uid}/{days}")
    data = _extract_doc_data(raw)
    if isinstance(data, dict):
        return {
            "team":        data.get("team") or {},
            "matches":     data.get("matches") or [],
            "tournaments": data.get("tournaments") or {},
        }
    return {"team": {}, "matches": [], "tournaments": {}}


def get_season_fixtures(season_id: str | int, page: int = 1) -> dict:
    """
    stats_season_fixtures2/{season_id}/{page}
    Returns:
      {
        "season":      { _id, name, start, end, … },
        "matches":     [ ... ],
        "cups":        { cup_id: { matches: [...] } },
        "tournaments": { tid: { ... } },
      }
    """
    raw  = _get(f"stats_season_fixtures2/{season_id}/{page}")
    data = _extract_doc_data(raw)
    if isinstance(data, dict):
        meta_keys = {"matches", "cups", "tables", "tournaments"}
        return {
            "season":      {k: v for k, v in data.items() if k not in meta_keys},
            "matches":     data.get("matches") or [],
            "cups":        data.get("cups") or {},
            "tournaments": data.get("tournaments") or {},
        }
    return {"season": {}, "matches": [], "cups": {}, "tournaments": {}}


def get_match_analytics(sr_match_id: str | int, fetch_season: bool = False) -> dict:
    """
    Full analytics bundle for one SR match:
      - match details (teams, result, tournament, statscoverage)
      - home team next matches (next 30 days)
      - away team next matches (next 30 days)
      - season fixtures (optional, set fetch_season=True)

    Returns a dict with key "available": True/False.
    """
    sr_id   = str(sr_match_id)
    details = get_match_details(sr_id)
    if not details:
        logger.debug("[sr] match not found: %s", sr_id)
        return {"sr_match_id": sr_id, "available": False}

    teams    = details.get("teams") or {}
    home     = teams.get("home") or {}
    away     = teams.get("away") or {}
    home_uid = home.get("uid") or home.get("_id")
    away_uid = away.get("uid") or away.get("_id")

    bundle: dict = {
        "sr_match_id": sr_id,
        "available":   True,
        "match":       details,
        "home_next":   {},
        "away_next":   {},
    }

    if home_uid:
        try:
            bundle["home_next"] = get_team_next_matches(home_uid)
        except Exception as exc:
            logger.debug("[sr] home_next uid=%s: %s", home_uid, exc)

    if away_uid:
        try:
            bundle["away_next"] = get_team_next_matches(away_uid)
        except Exception as exc:
            logger.debug("[sr] away_next uid=%s: %s", away_uid, exc)

    if fetch_season:
        season_id = (details.get("season") or {}).get("_id")
        if season_id:
            try:
                bundle["season_fixtures"] = get_season_fixtures(season_id)
            except Exception as exc:
                logger.debug("[sr] season_fixtures sid=%s: %s", season_id, exc)

    return bundle


# ─────────────────────────────────────────────────────────────────────────────
# Batch helper
# ─────────────────────────────────────────────────────────────────────────────

def get_match_analytics_batch(
    sr_match_ids: list[str | int],
    sleep_between: float = 0.2,
    fetch_season: bool   = False,
) -> dict[str, dict]:
    """
    Fetch analytics for a list of SR match IDs.
    Returns {sr_match_id: analytics_bundle}.
    Politely sleeps between requests to avoid rate-limiting.
    """
    import time
    results: dict[str, dict] = {}
    for mid in sr_match_ids:
        try:
            bundle = get_match_analytics(mid, fetch_season=fetch_season)
            results[str(mid)] = bundle
        except Exception as exc:
            logger.warning("[sr:batch] %s: %s", mid, exc)
            results[str(mid)] = {"sr_match_id": str(mid), "available": False}
        time.sleep(sleep_between)
    return results


__all__ = [
    "get_config_sports",
    "get_match_details",
    "get_team_next_matches",
    "get_season_fixtures",
    "get_match_analytics",
    "get_match_analytics_batch",
]