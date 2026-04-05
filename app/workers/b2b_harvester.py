"""
app/workers/b2b_harvester.py
=============================
BetB2B family harvester — wraps bookmaker_fetcher with canonical
market normalisation via market_mapper.normalize_b2b_market.

Covered bookmakers (any active BetB2B domain in DB):
    1xBet, 22Bet, Helabet, Paripesa, Melbet, Betwinner, Megapari

Normalised output shape (identical to sp/od/bt harvesters):
{
    "betradar_id":    "",          # B2B has no betradar ID
    "b2b_match_id":   str,         # bookmaker internal ID
    "home_team":      str,
    "away_team":      str,
    "start_time":     str | None,
    "competition":    str,
    "sport":          str,
    "source":         "b2b",
    "bookmakers": {                 # per-bk raw odds (from merge_bookmaker_results)
        "1xBet": { "match_id": "123", "markets": {...} }
    },
    "best_odds": {                  # best price per outcome across all bks
        "1x2": { "1": {"odds": 2.10, "bookie": "1xBet"}, ... }
    },
    "markets": { ... },             # alias for best_odds (normalised slugs)
    "market_count": int,
    "status":         "upcoming" | "live",
    "score_home":     int | None,
    "score_away":     int | None,
    "harvested_at":   str,
}
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from app.workers.market_mapper import normalize_b2b_market, normalize_outcome

# Sport slug (harvester convention) → BetB2B sport name
_SPORT_SLUG_TO_B2B: dict[str, str] = {
    "soccer":           "Football",
    "football":         "Football",
    "basketball":       "Basketball",
    "tennis":           "Tennis",
    "ice-hockey":       "Ice Hockey",
    "volleyball":       "Volleyball",
    "cricket":          "Cricket",
    "rugby":            "Rugby",
    "boxing":           "Boxing",
    "handball":         "Handball",
    "table-tennis":     "Table Tennis",
    "darts":            "Darts",
    "american-football":"American Football",
    "baseball":         "Baseball",
    "esoccer":          "Esports",
    "mma":              "Martial Arts",
    "golf":             "Golf",
    "motorsport":       "Formula 1",
}


# =============================================================================
# Internal helpers
# =============================================================================

def _b2b_sport_name(sport_slug: str) -> str:
    return _SPORT_SLUG_TO_B2B.get(sport_slug.lower(), "Football")


def _normalize_b2b_markets(raw_markets: dict) -> dict[str, dict[str, float]]:
    """
    Convert bookmaker_fetcher market dict (float values per outcome) to
    canonical slug dict.

    Input:  {"Total_2.5": {"Over": 1.95, "Under": 1.85}, "1X2": {"Home": 2.10, ...}}
    Output: {"over_under_goals_2.5": {"over": 1.95, "under": 1.85}, "1x2": {"1": 2.10, ...}}
    """
    result: dict[str, dict[str, float]] = {}

    for mkt_name, outcomes in raw_markets.items():
        if not isinstance(outcomes, dict):
            continue
        canonical = normalize_b2b_market(mkt_name)
        if canonical not in result:
            result[canonical] = {}
        for out_label, price in outcomes.items():
            try:
                price = float(price)
            except (TypeError, ValueError):
                continue
            if price <= 1.0:
                continue
            out_key = normalize_outcome(canonical, out_label)
            if out_key not in result[canonical] or price > result[canonical][out_key]:
                result[canonical][out_key] = price

    return {k: v for k, v in result.items() if v}


def _normalize_best_odds(raw_markets: dict) -> dict[str, dict[str, dict]]:
    """
    Convert merge_bookmaker_results 'markets' dict
    ({"1X2": {"Home": {"odds": 2.10, "bookmaker": "1xBet"}}}) to canonical slugs.
    """
    result: dict[str, dict[str, dict]] = {}

    for mkt_name, outcomes in raw_markets.items():
        if not isinstance(outcomes, dict):
            continue
        canonical = normalize_b2b_market(mkt_name)
        if canonical not in result:
            result[canonical] = {}
        for out_label, entry in outcomes.items():
            if isinstance(entry, dict):
                price  = float(entry.get("odds") or 0)
                bookie = str(entry.get("bookmaker") or "")
            else:
                try:
                    price = float(entry)
                except (TypeError, ValueError):
                    price = 0.0
                bookie = ""
            if price <= 1.0:
                continue
            out_key = normalize_outcome(canonical, out_label)
            existing = result[canonical].get(out_key, {})
            if not existing or price > existing.get("odds", 0):
                result[canonical][out_key] = {"odds": price, "bookie": bookie}

    return {k: v for k, v in result.items() if v}


def _normalize_bookmakers_odds(raw_bookmakers: dict) -> dict:
    """Normalize per-bookmaker raw market dicts inside the merged result."""
    norm = {}
    for bk_name, bk_data in raw_bookmakers.items():
        if not isinstance(bk_data, dict):
            continue
        raw_mkts = bk_data.get("markets") or {}
        norm[bk_name] = {
            "match_id": bk_data.get("match_id", ""),
            "markets":  _normalize_b2b_markets(raw_mkts),
        }
    return norm


def _shape(merged_match: dict, sport_slug: str) -> dict:
    """Build the normalised output shape from a merge_bookmaker_results item."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    best_odds = _normalize_best_odds(merged_match.get("markets") or {})
    bookmakers = _normalize_bookmakers_odds(merged_match.get("bookmakers") or {})

    return {
        "betradar_id":  "",
        "b2b_match_id": str(merged_match.get("match_id") or ""),
        "home_team":    str(merged_match.get("home_team") or ""),
        "away_team":    str(merged_match.get("away_team") or ""),
        "start_time":   merged_match.get("start_time"),
        "competition":  str(merged_match.get("competition") or ""),
        "sport":        str(merged_match.get("sport") or sport_slug),
        "source":       "b2b",
        "bookmakers":   bookmakers,
        "best_odds":    best_odds,
        "markets":      best_odds,   # alias — same data, both keys present for compat
        "market_count": len(best_odds),
        "status":       str(merged_match.get("status") or "upcoming"),
        "score_home":   merged_match.get("score_home"),
        "score_away":   merged_match.get("score_away"),
        "harvested_at": now_iso,
    }


# =============================================================================
# DB helper — get active B2B bookmakers
# =============================================================================

def _get_active_b2b_bookmakers() -> list[dict]:
    """
    Load active bookmakers from DB where vendor_slug = 'betb2b'.
    Returns list of bookmaker dicts compatible with bookmaker_fetcher.fetch_bookmaker.
    Falls back to empty list if app context unavailable.
    """
    try:
        from app.models.bookmakers_model import Bookmaker
        bms = Bookmaker.query.filter_by(vendor_slug="betb2b", is_active=True).all()
        return [
            {
                "id":          bm.id,
                "name":        bm.name,
                "domain":      bm.domain,
                "vendor_slug": bm.vendor_slug,
                "config":      bm.config or {},
            }
            for bm in bms
        ]
    except Exception:
        return []


# =============================================================================
# Public API
# =============================================================================

def fetch_b2b_sport(
    sport_slug: str,
    mode: str = "upcoming",
    bookmakers: list[dict] | None = None,
    max_workers: int = 8,
    timeout: int = 20,
) -> list[dict]:
    """
    Fetch all active B2B bookmakers for one sport + mode concurrently.
    Returns list of normalised merged matches.

    Args:
        sport_slug: harvester slug e.g. "soccer", "basketball"
        mode:       "upcoming" | "live"
        bookmakers: override DB lookup (for testing)
        max_workers: thread pool size
        timeout:    per-request timeout

    Returns:
        List of normalised match dicts (same shape as sp/od/bt harvesters).
    """
    from app.views.odds_feed.bookmaker_fetcher import fetch_all_bookmakers

    bms = bookmakers if bookmakers is not None else _get_active_b2b_bookmakers()
    if not bms:
        print(f"[b2b] no active bookmakers in DB for sport={sport_slug}")
        return []

    sport_name = _b2b_sport_name(sport_slug)
    print(f"[b2b] fetching {sport_name} mode={mode} from {len(bms)} bookmakers")

    t0 = time.perf_counter()
    result = fetch_all_bookmakers(
        bms,
        sport_name=sport_name,
        mode=mode,
        timeout=timeout,
        max_workers=max_workers,
    )
    elapsed = int((time.perf_counter() - t0) * 1000)

    raw_merged = result.get("matches") or []
    per_bk     = result.get("per_bookmaker") or {}

    # Log per-bookmaker stats
    for bk_name, stats in per_bk.items():
        status = "ok" if stats.get("ok") else "fail"
        print(
            f"[b2b] {bk_name}: {status} "
            f"count={stats['count']} "
            f"latency={stats['latency_ms']}ms"
            + (f" err={stats['error'][:60]}" if stats.get("error") else "")
        )

    normalised = [_shape(m, sport_slug) for m in raw_merged if m.get("markets")]
    print(f"[b2b] {sport_slug}/{mode}: {len(normalised)} normalised in {elapsed}ms")
    return normalised


def fetch_b2b_single(
    bookmaker: dict,
    sport_slug: str,
    mode: str = "upcoming",
    timeout: int = 20,
) -> list[dict]:
    """
    Fetch a single B2B bookmaker.
    Returns per-match normalised list (not merged across bookmakers).
    """
    from app.views.odds_feed.bookmaker_fetcher import fetch_bookmaker

    sport_name  = _b2b_sport_name(sport_slug)
    raw_matches = fetch_bookmaker(bookmaker, sport_name=sport_name,
                                  mode=mode, timeout=timeout)
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    bk_name = str(bookmaker.get("name") or bookmaker.get("domain") or "b2b")

    result = []
    for raw in raw_matches:
        norm_mkts = _normalize_b2b_markets(raw.get("markets") or {})
        if not norm_mkts:
            continue
        result.append({
            "betradar_id":    "",
            "b2b_match_id":   str(raw.get("match_id") or ""),
            "home_team":      str(raw.get("home_team") or ""),
            "away_team":      str(raw.get("away_team") or ""),
            "start_time":     raw.get("start_time"),
            "competition":    str(raw.get("competition") or ""),
            "sport":          str(raw.get("sport") or sport_slug),
            "source":         f"b2b_{bk_name.lower().replace(' ', '_')}",
            "bookmakers":     {bk_name: {"match_id": str(raw.get("match_id") or ""), "markets": norm_mkts}},
            "best_odds":      {k: {o: {"odds": v, "bookie": bk_name} for o, v in outcomes.items()} for k, outcomes in norm_mkts.items()},
            "markets":        {k: {o: {"odds": v, "bookie": bk_name} for o, v in outcomes.items()} for k, outcomes in norm_mkts.items()},
            "market_count":   len(norm_mkts),
            "status":         str(raw.get("status") or mode),
            "score_home":     raw.get("score_home"),
            "score_away":     raw.get("score_away"),
            "harvested_at":   now_iso,
        })
    return result


def fetch_b2b_full_markets(
    bookmaker: dict,
    match_id: str,
    feed: str = "LineFeed",
    timeout: int = 15,
) -> dict[str, dict[str, dict]]:
    """
    Fetch the full market book for a single B2B match via GetGameZip.
    Returns canonical best_odds dict.
    """
    from app.views.odds_feed.bookmaker_fetcher import fetch_betb2b_markets

    bk_name  = str(bookmaker.get("name") or "b2b")
    config   = bookmaker.get("config") or {}
    headers  = config.get("headers") or {}
    params   = dict(config.get("params") or {})
    domain   = bookmaker.get("domain", "")

    raw = fetch_betb2b_markets(domain, headers, params, match_id, feed=feed, timeout=timeout)
    raw_mkts = raw.get("markets") or {}

    return _normalize_best_odds(raw_mkts)