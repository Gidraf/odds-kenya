"""
app/workers/bt_harvester.py
============================
Betika Kenya harvester — upcoming + live.
Market keys are canonical slugs from market_seeds.py via market_mapper.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import requests

from app.workers.market_mapper import normalize_bt_market, normalize_outcome

_API_BASE  = "https://api.betika.com"
_LIVE_BASE = "https://live.betika.com"

_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en-US;q=0.9",
    "Origin":          "https://www.betika.com",
    "Referer":         "https://www.betika.com/",
}

BT_SPORT_ID: dict[str, int] = {
    "soccer":       14,
    "football":     14,
    "basketball":   30,
    "tennis":       28,
    "ice-hockey":   29,
    "volleyball":   35,
    "cricket":      37,
    "rugby":        41,
    "boxing":       39,
    "handball":     33,
    "mma":          36,
    "table-tennis": 45,
    "esoccer":      105,
}


def _get(base: str, path: str, params: dict | None = None, timeout: int = 15) -> Any:
    url = f"{base}{path}"
    try:
        resp = requests.get(url, headers=_HEADERS, params=params, timeout=timeout)
        if not resp.ok:
            print(f"[bt] HTTP {resp.status_code} -> {url}")
            return None
        return resp.json()
    except Exception as exc:
        print(f"[bt] {url}: {exc}")
        return None


def _api_get(path: str, params: dict | None = None, timeout: int = 15) -> Any:
    return _get(_API_BASE, path, params, timeout)


def _live_get(path: str, params: dict | None = None, timeout: int = 15) -> Any:
    return _get(_LIVE_BASE, path, params, timeout)


def _parse_markets(raw_data: list[dict]) -> dict[str, dict[str, float]]:
    """Convert Betika odds list -> canonical market dict via market_mapper."""
    if not raw_data:
        return {}

    # Try full production normalizer first (if sbo_fetcher is available)
    try:
        from app.workers.sbo_fetcher import MarketNormalizer, BetikaFetcher  # type: ignore
        raw_mkts = BetikaFetcher.parse_markets(raw_data)
        markets: dict[str, dict[str, float]] = {}
        for rm in raw_mkts:
            mkt_key = MarketNormalizer.normalize_market(
                rm["name"], rm.get("specifier"), rm.get("sub_type_id")
            )
            if mkt_key not in markets:
                markets[mkt_key] = {}
            for out in rm.get("outcomes") or []:
                out_key = MarketNormalizer.normalize_outcome(
                    mkt_key, out["key"], out.get("display", "")
                )
                price = float(out.get("value") or 0)
                if price > 1.0:
                    markets[mkt_key][out_key] = price
        return {k: v for k, v in markets.items() if v}
    except ImportError:
        pass

    # Use unified market_mapper
    markets: dict[str, dict[str, float]] = {}

    for mkt in raw_data:
        name     = str(mkt.get("name") or "")
        sub_type = mkt.get("sub_type_id")
        mkt_key  = normalize_bt_market(name, sub_type)

        if mkt_key not in markets:
            markets[mkt_key] = {}

        for o in mkt.get("odds") or []:
            if not int(o.get("odd_active") or 1):
                continue
            odd_key = str(o.get("odd_key") or "")
            display = str(o.get("display") or "")
            try:
                price = float(o.get("odd_value") or 0)
            except (TypeError, ValueError):
                price = 0.0
            if price <= 1.0:
                continue
            out_key = normalize_outcome(mkt_key, odd_key, display)
            if out_key not in markets[mkt_key] or markets[mkt_key][out_key] < price:
                markets[mkt_key][out_key] = price

    return {k: v for k, v in markets.items() if v}


def _inline_markets(match_record: dict) -> dict[str, dict[str, float]]:
    return _parse_markets(match_record.get("odds") or [])


def fetch_live_sport_counts() -> list[dict]:
    payload = _live_get("/v1/uo/sports")
    return (payload or {}).get("data") or []


def fetch_upcoming(
    sport_slug: str,
    per_page: int = 50,
    max_pages: int = 5,
    fetch_full_markets: bool = True,
    max_matches: int | None = None,
    timeout: int = 15,
) -> list[dict]:
    sport_id = BT_SPORT_ID.get(sport_slug.lower())
    if not sport_id:
        print(f"[bt:upcoming] unknown sport: {sport_slug}")
        return []

    raw_items: list[dict] = []
    seen_ids:  set[str]   = set()

    for page in range(1, max_pages + 1):
        payload = _api_get("/v1/uo/matches", params={
            "sport_id":    sport_id,
            "sub_type_id": "1,186,340",
            "sort_id":     1,
            "period_id":   9,
            "esports":     "false",
            "per_page":    per_page,
            "page":        page,
            "tab":         "",
        }, timeout=timeout)

        if not payload:
            break
        data = payload.get("data") or []
        if not data:
            break
        for m in data:
            brid = str(m.get("parent_match_id") or "")
            if brid and brid not in seen_ids:
                seen_ids.add(brid)
                raw_items.append(m)
        meta  = payload.get("meta") or {}
        total = int(meta.get("total") or 0)
        if page * per_page >= total:
            break

    print(f"[bt:upcoming] {sport_slug}: {len(raw_items)} raw matches")

    results = []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    targets = raw_items[:max_matches] if max_matches else raw_items

    for m in targets:
        brid = str(m.get("parent_match_id") or "")
        if fetch_full_markets and brid:
            raw      = _api_get("/v1/uo/match", params={"parent_match_id": brid}, timeout=timeout)
            markets  = _parse_markets((raw or {}).get("data") or [])
            time.sleep(0.05)
        else:
            markets = _inline_markets(m)

        results.append({
            "betradar_id":  brid,
            "bt_match_id":  str(m.get("match_id") or m.get("game_id") or ""),
            "home_team":    str(m.get("home_team") or ""),
            "away_team":    str(m.get("away_team") or ""),
            "start_time":   m.get("start_time"),
            "competition":  str(m.get("competition_name") or ""),
            "sport":        sport_slug,
            "source":       "betika_upcoming",
            "markets":      markets,
            "market_count": len(markets),
            "harvested_at": now_iso,
        })

    print(f"[bt:upcoming] {sport_slug}: {len(results)} normalised")
    return results


def fetch_live(
    sport_slug: str | None = None,
    fetch_full_markets: bool = True,
    max_matches: int | None = None,
    timeout: int = 15,
) -> list[dict]:
    sport_id: int | str = "null"
    effective_slug = sport_slug or "all"

    if sport_slug:
        sid = BT_SPORT_ID.get(sport_slug.lower())
        if not sid:
            print(f"[bt:live] unknown sport: {sport_slug}")
            return []
        sport_id = sid

    payload = _live_get("/v1/uo/matches", params={
        "page":        1,
        "limit":       1000,
        "sub_type_id": "1,186,340",
        "sport":       sport_id,
        "sort":        1,
    }, timeout=timeout)

    if not payload:
        return []

    raw_items: list[dict] = payload.get("data") or []
    print(f"[bt:live] {effective_slug}: {len(raw_items)} raw matches")

    results = []
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    targets = raw_items[:max_matches] if max_matches else raw_items

    for m in targets:
        match_id = str(m.get("match_id") or "")
        brid     = str(m.get("parent_match_id") or "")

        if fetch_full_markets and match_id:
            raw      = _live_get("/v1/uo/match", params={"id": match_id}, timeout=timeout)
            markets  = _parse_markets((raw or {}).get("data") or [])
            time.sleep(0.05)
        else:
            markets = _inline_markets(m)

        m_sport = str(m.get("sport_name") or effective_slug).lower().replace(" ", "-")

        results.append({
            "betradar_id":      brid,
            "bt_match_id":      match_id,
            "home_team":        str(m.get("home_team") or ""),
            "away_team":        str(m.get("away_team") or ""),
            "start_time":       m.get("start_time"),
            "competition":      str(m.get("competition_name") or m.get("competition") or ""),
            "sport":            m_sport,
            "source":           "betika_live",
            "markets":          markets,
            "market_count":     len(markets),
            "harvested_at":     now_iso,
            "live_score":       m.get("current_score"),
            "match_time":       m.get("match_time"),
            "event_status":     m.get("event_status"),
            "match_status":     m.get("match_status"),
            "bet_stop_reason":  m.get("bet_stop_reason") or None,
            "home_red_card":    int(m.get("home_red_card") or 0),
            "away_red_card":    int(m.get("away_red_card") or 0),
            "home_yellow_card": int(m.get("home_yellow_card") or 0),
            "away_yellow_card": int(m.get("away_yellow_card") or 0),
            "home_corners":     int(m.get("home_corners") or 0),
            "away_corners":     int(m.get("away_corners") or 0),
            "ht_score":         m.get("ht_score"),
            "ft_score":         m.get("ft_score"),
            "set_scores":       m.get("set_score") or [],
            "odd_active":       bool(m.get("odd_active")),
            "market_active":    bool(m.get("market_active")),
        })

    print(f"[bt:live] {effective_slug}: {len(results)} normalised")
    return results


def fetch_all_live(fetch_full_markets: bool = False, **kwargs) -> list[dict]:
    return fetch_live(sport_slug=None, fetch_full_markets=fetch_full_markets, **kwargs)