"""
app/workers/od_harvester.py
============================
Odibets Kenya harvester — built from real intercepted API traffic.

Market keys are canonical slugs from market_seeds.py via market_mapper.

Real Endpoints
--------------
1. MATCH LISTING  GET https://api.odi.site/odi/sportsbook
       ?producer=0&day=2026-03-23&sport_id=1
       &resource=sportevents&platform=mobile&mode=1

2. FULL MATCH     GET https://api.odi.site/sportsbook/v1
       ?id=61515118&category_id=&sub_type_id=
       &builder=0&sportsbook=sportsbook&ua=...&resource=sportevent

3. EXTENDED MKT   GET https://api.odi.site/sportsbook/v1
       ?id=61624502&sub_type_id=33&sportsbook=sportsbook
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from app.workers.market_mapper import normalize_od_market, normalize_outcome

# ── Base URL ──────────────────────────────────────────────────────────────────

_BASE = "https://api.odi.site"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Origin":          "https://odibets.com",
    "Referer":         "https://odibets.com/",
}

_UA = (
    "Mozilla/5.0+(Linux;+Android+10;+K)+AppleWebKit/537.36+"
    "(KHTML,+like+Gecko)+Chrome/146.0.0.0+Mobile+Safari/537.36"
)

# ── Sport slug → Odibets sport_id ────────────────────────────────────────────

OD_SPORT_ID: dict[str, int] = {
    "soccer":       1,
    "football":     1,
    "basketball":   2,
    "ice-hockey":   4,
    "tennis":       5,
    "handball":     6,
    "volleyball":   23,
    "table-tennis": 20,
    "darts":        22,
    "badminton":    31,
    "mma":          10,
    "boxing":       9,
    "cricket":      21,
    "rugby":        12,
    "esoccer":      137,
    "esport-cs":    109,
}

# Sub-type IDs to fetch for extended market data
_EXTENDED_SIDS = [
    "45",   # correct score
    "47",   # ht/ft
    "15",   # winning margin
    "81",   # ht correct score
    "548",  # multigoals / number_of_goals
    "31",   # home clean sheet
    "32",   # away clean sheet
    "33",   # home win to nil
    "34",   # away win to nil
    "48",   # home win both halves
    "52",   # highest scoring half
    "184",  # next goal & 1x2
]


# =============================================================================
# HTTP helper
# =============================================================================

def _get(path: str, params: dict | None = None, timeout: int = 15) -> Any:
    url = f"{_BASE}{path}"
    try:
        r = requests.get(url, headers=_HEADERS, params=params, timeout=timeout)
        if not r.ok:
            print(f"[od] HTTP {r.status_code} {path} params={params}")
            return None
        return r.json()
    except Exception as exc:
        print(f"[od] {path}: {exc}")
        return None


# =============================================================================
# Market parsers
# =============================================================================

def _parse_full_markets(payload: dict) -> dict[str, dict[str, float]]:
    """
    Parse the full match response from /sportsbook/v1?id=...&resource=sportevent.
    Same sub_type_id appears multiple times with different specifiers
    (handicap lines, O/U lines) — each becomes a separate market key.
    """
    data = payload.get("data") or {}
    markets: dict[str, dict[str, float]] = {}

    for mkt in (data.get("markets") or []):
        # Skip suspended market blocks
        if str(mkt.get("status") or "1") == "0":
            continue

        outcomes_raw = mkt.get("outcomes") or []
        if not outcomes_raw:
            continue

        sid  = str(mkt.get("sub_type_id") or "")
        spec = str(mkt.get("specifiers") or "")

        mkt_key = normalize_od_market(sid, spec)
        if mkt_key not in markets:
            markets[mkt_key] = {}

        for o in outcomes_raw:
            if str(o.get("status") or "1") == "-1":
                continue
            if int(o.get("active", 1)) != 1:
                continue

            ok = str(o.get("outcome_key") or "")
            try:
                price = float(o.get("odd_value") or 0)
            except (TypeError, ValueError):
                continue
            if price <= 1.0:
                continue

            out_key = normalize_outcome(mkt_key, ok)
            if out_key not in markets[mkt_key] or price > markets[mkt_key][out_key]:
                markets[mkt_key][out_key] = price

    return {k: v for k, v in markets.items() if v}


def _parse_listing_markets(match_raw: dict) -> dict[str, dict[str, float]]:
    """
    Parse inline markets from the listing endpoint.
    Listing structure: markets[].lines[].outcomes[]
    """
    markets: dict[str, dict[str, float]] = {}

    for mkt in (match_raw.get("markets") or []):
        sid = str(mkt.get("sub_type_id") or "")
        for line in (mkt.get("lines") or []):
            spec    = str(line.get("specifiers") or "")
            mkt_key = normalize_od_market(sid, spec)
            if mkt_key not in markets:
                markets[mkt_key] = {}

            for o in (line.get("outcomes") or []):
                if int(o.get("active", 1)) != 1:
                    continue
                if int(o.get("status", 1)) < 0:
                    continue
                ok = str(o.get("outcome_key") or "")
                try:
                    price = float(o.get("odd_value") or 0)
                except (TypeError, ValueError):
                    continue
                if price <= 1.0:
                    continue
                out_key = normalize_outcome(mkt_key, ok)
                if out_key not in markets[mkt_key] or price > markets[mkt_key][out_key]:
                    markets[mkt_key][out_key] = price

    return {k: v for k, v in markets.items() if v}


def _merge_markets(base: dict, extra: dict) -> dict:
    merged = {k: dict(v) for k, v in base.items()}
    for mkt_key, outcomes in extra.items():
        if mkt_key not in merged:
            merged[mkt_key] = {}
        for out, price in outcomes.items():
            if out not in merged[mkt_key] or price > merged[mkt_key][out]:
                merged[mkt_key][out] = price
    return merged


def _extract_match_info(payload: dict) -> dict:
    info = (payload.get("data") or {}).get("info") or {}
    return {
        "home_team":   str(info.get("home_team") or ""),
        "away_team":   str(info.get("away_team") or ""),
        "start_time":  info.get("start_time"),
        "competition": str(info.get("competition_name") or ""),
        "sport":       str(info.get("sport_name") or ""),
        "result":      str(info.get("result") or ""),
        "status":      int(info.get("status") or 0),
    }


# =============================================================================
# Endpoint fetchers
# =============================================================================

def _fetch_match_listing(
    day_str: str,
    sport_id: int | None = None,
    producer: int = 0,
    timeout: int = 15,
) -> list[dict]:
    params: dict = {
        "producer": producer,
        "day":      day_str,
        "resource": "sportevents",
        "platform": "mobile",
        "mode":     1,
    }
    if sport_id is not None:
        params["sport_id"] = sport_id

    payload = _get("/odi/sportsbook", params=params, timeout=timeout)
    if not payload or not isinstance(payload.get("data"), dict):
        return []

    data = payload["data"]
    raw: list[dict] = []
    for m in (data.get("matches") or []):        # top-level (live)
        raw.append(m)
    for league in (data.get("leagues") or []):   # grouped (upcoming)
        for m in (league.get("matches") or []):
            raw.append(m)
    return raw


def _fetch_full_match(match_id: str, timeout: int = 15) -> dict:
    payload = _get("/sportsbook/v1", params={
        "id":          match_id,
        "category_id": "",
        "sub_type_id": "",
        "builder":     "0",
        "sportsbook":  "sportsbook",
        "ua":          _UA,
        "resource":    "sportevent",
    }, timeout=timeout)
    return payload or {}


def _fetch_extended_market(
    match_id: str, sub_type_id: str, timeout: int = 10
) -> dict[str, dict[str, float]]:
    payload = _get("/sportsbook/v1", params={
        "id":          match_id,
        "sub_type_id": sub_type_id,
        "sportsbook":  "sportsbook",
    }, timeout=timeout)
    return _parse_full_markets(payload or {})


def _parse_listing_match(raw: dict) -> dict | None:
    mid = str(raw.get("parent_match_id") or "")
    if not mid:
        return None
    return {
        "id":           mid,
        "home_team":    str(raw.get("home_team") or ""),
        "away_team":    str(raw.get("away_team") or ""),
        "start_time":   raw.get("start_time"),
        "competition":  str(raw.get("competition_name") or ""),
        "sport":        str(raw.get("sport_name") or raw.get("s_binomen") or ""),
        "status":       int(raw.get("status") or 0),
        "result":       str(raw.get("result") or ""),
        "_inline_mkts": _parse_listing_markets(raw),
    }


# =============================================================================
# Public API
# =============================================================================

def fetch_upcoming(
    sport_slug: str,
    days: int = 5,
    fetch_full_markets: bool = True,
    fetch_extended: bool = True,
    max_matches: int | None = None,
    timeout: int = 15,
) -> list[dict]:
    sport_id = OD_SPORT_ID.get(sport_slug.lower())
    now_utc  = datetime.now(timezone.utc)
    now_iso  = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    raw_items: list[dict] = []
    seen:      set[str]   = set()

    for day_off in range(days):
        day_str = (now_utc + timedelta(days=day_off)).strftime("%Y-%m-%d")
        for raw in _fetch_match_listing(day_str, sport_id=sport_id, timeout=timeout):
            parsed = _parse_listing_match(raw)
            if not parsed:
                continue
            mid = parsed["id"]
            if mid not in seen and parsed["status"] == 0:
                seen.add(mid)
                raw_items.append(parsed)

    print(f"[od] {sport_slug}: {len(raw_items)} upcoming matches found")
    targets = raw_items[:max_matches] if max_matches else raw_items

    results: list[dict] = []
    for item in targets:
        mid = item["id"]

        if fetch_full_markets:
            full_payload = _fetch_full_match(mid, timeout=timeout)
            markets = _parse_full_markets(full_payload)
            meta    = _extract_match_info(full_payload)
            time.sleep(0.06)
        else:
            markets = item["_inline_mkts"]
            meta    = {}

        if fetch_extended and fetch_full_markets and markets:
            covered: set[str] = set()
            for sid in _EXTENDED_SIDS:
                mkt_key = normalize_od_market(sid)
                if any(k == mkt_key or k.startswith(f"{mkt_key}_") for k in markets):
                    covered.add(sid)
            for sid in _EXTENDED_SIDS:
                if sid in covered:
                    continue
                extra = _fetch_extended_market(mid, sid, timeout=timeout)
                if extra:
                    markets = _merge_markets(markets, extra)
                time.sleep(0.03)

        results.append({
            "betradar_id":  mid,
            "od_match_id":  mid,
            "home_team":    meta.get("home_team") or item["home_team"],
            "away_team":    meta.get("away_team") or item["away_team"],
            "start_time":   meta.get("start_time") or item["start_time"],
            "competition":  meta.get("competition") or item["competition"],
            "sport":        meta.get("sport") or item["sport"] or sport_slug,
            "source":       "odibets",
            "status":       "upcoming",
            "markets":      markets,
            "market_count": len(markets),
            "harvested_at": now_iso,
        })

    print(f"[od] {sport_slug}: {len(results)} normalised upcoming")
    return results


def fetch_live(
    sport_slug: str,
    fetch_full_markets: bool = True,
    timeout: int = 15,
) -> list[dict]:
    sport_id = OD_SPORT_ID.get(sport_slug.lower())
    now_utc  = datetime.now(timezone.utc)
    now_iso  = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    raw_all: list[dict] = []
    seen: set[str] = set()

    for day_off in (0, 1):
        day_str = (now_utc + timedelta(days=day_off)).strftime("%Y-%m-%d")
        for raw in _fetch_match_listing(day_str, sport_id=sport_id,
                                         producer=3, timeout=timeout):
            parsed = _parse_listing_match(raw)
            if not parsed:
                continue
            mid = parsed["id"]
            if mid not in seen and parsed["status"] == 1:
                seen.add(mid)
                raw_all.append(parsed)

    print(f"[od:live] {sport_slug}: {len(raw_all)} live")

    results: list[dict] = []
    for item in raw_all:
        mid = item["id"]

        if fetch_full_markets:
            full_payload = _fetch_full_match(mid, timeout=timeout)
            markets = _parse_full_markets(full_payload)
            meta    = _extract_match_info(full_payload)
            time.sleep(0.06)
        else:
            markets = item["_inline_mkts"]
            meta    = {}

        results.append({
            "betradar_id":  mid,
            "od_match_id":  mid,
            "home_team":    meta.get("home_team") or item["home_team"],
            "away_team":    meta.get("away_team") or item["away_team"],
            "start_time":   meta.get("start_time") or item["start_time"],
            "competition":  meta.get("competition") or item["competition"],
            "sport":        meta.get("sport") or item["sport"] or sport_slug,
            "source":       "odibets",
            "status":       "live",
            "result":       item.get("result", ""),
            "markets":      markets,
            "market_count": len(markets),
            "harvested_at": now_iso,
        })

    return results


def fetch_for_betradar_ids(
    betradar_ids: list[str],
    fetch_extended: bool = True,
    timeout: int = 15,
) -> list[dict]:
    results: list[dict] = []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for brid in betradar_ids:
        full_payload = _fetch_full_match(brid, timeout=timeout)
        markets = _parse_full_markets(full_payload)
        time.sleep(0.06)

        if not markets:
            continue

        meta = _extract_match_info(full_payload)

        if fetch_extended:
            covered: set[str] = set()
            for sid in _EXTENDED_SIDS:
                mkt_key = normalize_od_market(sid)
                if any(k == mkt_key or k.startswith(f"{mkt_key}_") for k in markets):
                    covered.add(sid)
            for sid in _EXTENDED_SIDS:
                if sid in covered:
                    continue
                extra = _fetch_extended_market(brid, sid, timeout=timeout)
                if extra:
                    markets = _merge_markets(markets, extra)
                time.sleep(0.03)

        results.append({
            "betradar_id":  brid,
            "od_match_id":  brid,
            "home_team":    meta.get("home_team", ""),
            "away_team":    meta.get("away_team", ""),
            "start_time":   meta.get("start_time"),
            "competition":  meta.get("competition", ""),
            "sport":        meta.get("sport", ""),
            "source":       "odibets",
            "status":       "live" if meta.get("status") == 1 else "upcoming",
            "markets":      markets,
            "market_count": len(markets),
            "harvested_at": now_iso,
        })

    return results


def poll_live_match(match_id: str, timeout: int = 10) -> dict[str, dict[str, float]]:
    return _parse_full_markets(_fetch_full_match(match_id, timeout=timeout))