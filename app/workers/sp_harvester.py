"""
app/workers/sp_harvester.py – Playwright version (dynamic cookies, fixed intercepts)
Sportpesa Kenya harvester – FULL MARKETS for ALL sports (markets=all).
Uses a real Chromium browser to intercept all API calls.
"""

from __future__ import annotations

import time
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Generator

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from app.workers.sp_mapper import normalize_sp_market
from app.workers.canonical_mapper import normalize_outcome
from app.workers.tasks_analytics import scrape_sportpesa_match_analytics

# =============================================================================
# CONSTANTS
# =============================================================================

_BASE = "https://www.ke.sportpesa.com"

# Sport slug → SP sport_id
SP_SPORT_ID: dict[str, str] = {
    "soccer":            "1",
    "football":          "1",
    "esoccer":           "126",
    "efootball":         "126",
    "e-football":        "126",
    "virtual-football":  "126",
    "basketball":        "2",
    "tennis":            "5",
    "ice-hockey":        "4",
    "icehockey":         "4",
    "volleyball":        "23",
    "cricket":           "21",
    "rugby":             "12",
    "rugby-league":      "12",
    "rugby-union":       "12",
    "boxing":            "10",
    "handball":          "6",
    "table-tennis":      "16",
    "tabletennis":       "16",
    "mma":               "117",
    "ufc":               "117",
    "darts":             "49",
    "american-football": "15",
    "americanfootball":  "15",
    "nfl":               "15",
    "baseball":          "3",
}

# SP sport_id → URL slug used on the website
_SPORT_ID_TO_SLUG: dict[str, str] = {
    "1":   "football",
    "2":   "basketball",
    "5":   "tennis",
    "4":   "ice-hockey",
    "23":  "volleyball",
    "6":   "handball",
    "16":  "table-tennis",
    "12":  "rugby",
    "21":  "cricket",
    "10":  "boxing",
    "117": "mma",
    "49":  "darts",
    "15":  "american-football",
    "3":   "baseball",
    "126": "esports",          # eFootball
}

_ESOCCER_IDS = {"126"}

# =============================================================================
# PARSERS (unchanged from original)
# =============================================================================

def _str_field(v: Any) -> str:
    if isinstance(v, dict):
        return str(v.get("name") or v.get("title") or v.get("short") or "")
    return str(v) if v else ""

def _parse_timestamp(item: dict) -> str | None:
    for key in ("dateTimestamp", "startTimestamp", "timestamp"):
        ts = item.get(key)
        if ts is not None:
            try:
                t = int(ts)
                if t > 1_000_000_000_000:
                    t //= 1000
                return datetime.fromtimestamp(t).strftime("%Y-%m-%dT%H:%M:%S.000Z")
            except Exception:
                pass
    for key in ("date", "startTime", "start_time"):
        dt = item.get(key)
        if dt:
            return str(dt)
    return None

def _parse_match_item(item: dict) -> dict | None:
    sp_game_id = str(item.get("id") or "")
    if not sp_game_id:
        return None
    betradar_id = str(
        item.get("betradarId") or item.get("betradar_id") or
        item.get("betRadarId") or ""
    )
    comps = item.get("competitors") or item.get("teams") or []
    if isinstance(comps, list) and len(comps) >= 2:
        home = _str_field(comps[0].get("name") or comps[0])
        away = _str_field(comps[1].get("name") or comps[1])
    else:
        home = _str_field(item.get("home") or item.get("homeName") or "")
        away = _str_field(item.get("away") or item.get("awayName") or "")
    comp_raw = item.get("competition") or item.get("league") or {}
    competition = _str_field(comp_raw) or _str_field(
        item.get("leagueName") or item.get("competitionName") or ""
    )
    sport_raw = item.get("sport") or {}
    sport_name = _str_field(sport_raw)
    sp_sport_id: int = 1
    if isinstance(sport_raw, dict):
        try:
            sp_sport_id = int(sport_raw.get("id") or 1)
        except (TypeError, ValueError):
            pass
    elif isinstance(sport_raw, int):
        sp_sport_id = sport_raw
    return {
        "betradar_id": betradar_id,
        "sp_game_id": sp_game_id,
        "home_team": home,
        "away_team": away,
        "start_time": _parse_timestamp(item),
        "competition": competition,
        "sport": sport_name,
        "sp_sport_id": sp_sport_id,
        "_inline_mkts": item.get("markets") or item.get("odds") or [],
    }

def _parse_markets(
    raw_list: list[dict],
    game_id: str = "",
    sport_id: int = 1,
) -> dict[str, dict[str, float]]:
    markets: dict[str, dict[str, float]] = {}
    for mkt in raw_list:
        if not isinstance(mkt, dict):
            continue
        mkt_id = mkt.get("id") or mkt.get("marketId") or mkt.get("typeId")
        if mkt_id is None:
            continue
        try:
            mkt_id = int(mkt_id)
        except (TypeError, ValueError):
            continue
        spec_val = mkt.get("specValue")
        if spec_val is None:
            spec_val = mkt.get("spec") or mkt.get("handicap")
        if spec_val is None or spec_val == 0:
            sels_raw = mkt.get("selections") or mkt.get("outcomes") or []
            if isinstance(sels_raw, dict):
                sels_raw = list(sels_raw.values())
            for s in sels_raw:
                if isinstance(s, dict):
                    sv = s.get("specValue")
                    if sv is not None and sv != 0:
                        spec_val = sv
                        break
        mkt_key = normalize_sp_market(mkt_id, spec_val, sport_id)
        markets.setdefault(mkt_key, {})
        sels = mkt.get("selections") or mkt.get("outcomes") or mkt.get("odds") or []
        if isinstance(sels, dict):
            sels = list(sels.values())
        for sel in sels:
            if not isinstance(sel, dict):
                continue
            short = str(
                sel.get("shortName") or sel.get("name") or
                sel.get("label") or sel.get("outcome") or ""
            )
            try:
                price = float(sel.get("odds") or sel.get("price") or sel.get("value") or 0)
            except (TypeError, ValueError):
                price = 0.0
            if price <= 1.0:
                continue
            out_key = normalize_outcome(mkt_key, short)
            if price > markets[mkt_key].get(out_key, 0.0):
                markets[mkt_key][out_key] = round(price, 3)
    return {k: v for k, v in markets.items() if v}

def _build_match(parsed: dict, markets: dict, sport_slug: str, status: str = "upcoming") -> dict:
    return {
        "betradar_id": parsed["betradar_id"],
        "sp_game_id": parsed["sp_game_id"],
        "home_team": parsed["home_team"],
        "away_team": parsed["away_team"],
        "start_time": parsed["start_time"],
        "competition": parsed["competition"],
        "sport": parsed["sport"] or sport_slug,
        "sp_sport_id": parsed.get("sp_sport_id", 1),
        "source": "sportpesa",
        "status": status,
        "markets": markets,
        "market_count": len(markets),
        "harvested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

def _is_near_term(start_time_str: str, days: int = 3) -> bool:
    if not start_time_str:
        return False
    try:
        if start_time_str.endswith('Z'):
            st = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
            st = st.astimezone(timezone(timedelta(hours=3)))
        else:
            st = datetime.fromisoformat(start_time_str)
            if st.tzinfo is None:
                st = st.replace(tzinfo=timezone(timedelta(hours=3)))
        now = datetime.now(timezone(timedelta(hours=3)))
        delta = st - now
        return timedelta(0) <= delta <= timedelta(days=days)
    except Exception:
        return False

# =============================================================================
# PLAYWRIGHT HELPERS
# =============================================================================

def _new_context(playwright, headless=True):
    """Creates a Chromium browser context with dynamic cookies."""
    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
        ),
        extra_http_headers={
            "X-App-Timezone": "Africa/Nairobi",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    page = context.new_page()

    # Visit the homepage to obtain all necessary cookies (ak_bmsc, bm_sv, etc.)
    page.goto("https://www.ke.sportpesa.com", wait_until="domcontentloaded", timeout=30000)

    # Set a few harmless cookies to avoid pop-ups and enforce desired settings
    page.evaluate("""
        document.cookie = "locale=en; path=/; domain=.ke.sportpesa.com";
        document.cookie = "device_view=full; path=/; domain=.ke.sportpesa.com";
        document.cookie = "settings=" + encodeURIComponent(JSON.stringify({
            betslip: {
                acceptOdds: true,
                amount: null,
                direct: false,
                betSpinnerSkipAnimation: false,
                globalBetSpinnerEnabled: true
            },
            markets_layout: "multiple",
            "single-wallet-first-phase": "1"
        })) + "; path=/; domain=.ke.sportpesa.com";
    """)

    # Wait for the bot‑protection cookie to appear (up to 15 seconds)
    try:
        page.wait_for_function("document.cookie.indexOf('ak_bmsc') > -1", timeout=15000)
    except PlaywrightTimeout:
        print("[pw] WARNING: ak_bmsc cookie not set – bot detection may block requests")
    # Extra settle time
    page.wait_for_timeout(2000)
    page.close()
    return browser, context

def _intercept_api(page, url_substring: str, trigger, timeout=30000) -> Any:
    """
    Execute trigger() while waiting for a response whose URL contains url_substring.
    Returns the parsed JSON body.
    """
    with page.expect_response(
        lambda resp: url_substring in resp.url and resp.status == 200,
        timeout=timeout
    ) as resp_info:
        trigger()
    return resp_info.value.json()

def _extract_items(raw: Any) -> list[dict]:
    """Extract a list of game items from different API response formats."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "games", "items", "results"):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []

# =============================================================================
# UPCOMING – collect all game IDs, then fetch markets
# =============================================================================

def _collect_upcoming_game_ids(
    page, upcoming_url: str, sport_id: str, max_items: int | None
) -> list[dict]:
    """Collect all raw game objects from all pages for a sport."""
    collected = []

    # Initial page load – intercept the upcoming games API response
    data = _intercept_api(page, "/api/upcoming/games", lambda: page.goto(
        upcoming_url, wait_until="domcontentloaded", timeout=30000
    ))
    items = _extract_items(data)
    collected.extend(items)

    # Pagination loop
    while True:
        if max_items and len(collected) >= max_items:
            break

        # Find a clickable "Next" element
        next_btn = page.query_selector('button.next-button:not([disabled])')
        if not next_btn:
            next_li = page.query_selector(
                'ul.event-list-pagination li[translate="next"]:not(.page-disabled)'
            )
            if next_li:
                next_btn = next_li
            else:
                break

        data = _intercept_api(page, "/api/upcoming/games", lambda: next_btn.click())
        items = _extract_items(data)
        collected.extend(items)

    return collected[:max_items] if max_items else collected

def _fetch_markets_for_game(page, game_id: str, sport_id: str) -> list[dict]:
    """Navigate to the game's markets page and intercept the /api/games/markets response."""
    url = f"{_BASE}/games/{game_id}/markets?sportId={sport_id}&section=upcoming-games&filterDay=-1"
    data = _intercept_api(page, "/api/games/markets", lambda: page.goto(
        url, wait_until="domcontentloaded", timeout=30000
    ))
    # Extract the market list from the response
    if isinstance(data, dict):
        for key in ("data", "result", game_id, int(game_id) if game_id.isdigit() else None):
            if key and isinstance(data.get(key), list):
                return data[key]
        for v in data.values():
            if isinstance(v, list):
                return v
    elif isinstance(data, list):
        return data
    return []

# =============================================================================
# PUBLIC API – UPCOMING STREAM (generator)
# =============================================================================

def fetch_upcoming_stream(
    sport_slug:         str,
    days:               int | None   = None,      # ignored for now, kept for compatibility
    max_matches:        int | None   = None,
    offset:             int          = 0,
    fetch_full_markets: bool         = True,
    sleep_between:      float        = 0.3,
    debug_ou:           bool         = False,
    **_
) -> Generator[dict, None, None]:
    """
    Yield one normalised match dict at a time.
    fetch_full_markets=True (default) fetches all markets via Playwright.
    offset: skip first N matches.
    """
    slug = sport_slug.lower().replace(" ", "-")
    sport_id = SP_SPORT_ID.get(slug)
    if not sport_id:
        print(f"[sp] unknown sport: {sport_slug!r}")
        return

    sport_url_slug = _SPORT_ID_TO_SLUG.get(sport_id, slug)
    upcoming_url = f"{_BASE}/en/sports-betting/{sport_url_slug}-{sport_id}/upcoming-games/?filterDay=-1"

    with sync_playwright() as pw:
        browser, context = _new_context(pw, headless=True)
        try:
            page = context.new_page()
            # Step 1: Collect all game items from all pages
            all_items = _collect_upcoming_game_ids(page, upcoming_url, sport_id, max_matches)
            page.close()
        finally:
            # We'll close the browser after the loop, but we need to keep the context alive
            # for the markets fetch. We'll reuse the same context by opening new pages.
            # However, the `with` block will close the browser. We need to restructure.
            # Instead, we fetch markets inside the same context but in a second phase.
            # To keep the generator working, we must stay inside the browser context.
            # So we'll fetch all markets before leaving the `with` block and then yield.
            # We'll collect all finished match dicts and then yield them.
            # But the original generator yields one at a time. To preserve that we could
            # use a list and yield after, but that's simpler and still compatible.
            pass

    # Because the generator must stay inside the playwright context, we'll restructure
    # to a single function that opens the context, collects all matches (with markets),
    # and then yields them. The caller expects a generator; we can make it a regular
    # function that returns a list and then wrap it? The original code used generators.
    # To avoid breaking the API, we'll keep the generator by yielding from inside the
    # context manager. That means we need to do the markets fetching inside the loop.

    # I'll rewrite the function entirely to keep the context open during the whole process.
    # This means we open one browser context per sport, collect all game IDs, then
    # for each game open a new page to fetch its markets, yield the match, and finally
    # close the browser when the generator is exhausted (or on error).

    # Here is the corrected version that stays inside the context:

def fetch_upcoming_stream(
    sport_slug:         str,
    days:               int | None   = None,
    max_matches:        int | None   = None,
    offset:             int          = 0,
    fetch_full_markets: bool         = True,
    sleep_between:      float        = 0.3,
    debug_ou:           bool         = False,
    **_
) -> Generator[dict, None, None]:
    slug = sport_slug.lower().replace(" ", "-")
    sport_id = SP_SPORT_ID.get(slug)
    if not sport_id:
        print(f"[sp] unknown sport: {sport_slug!r}")
        return

    sport_url_slug = _SPORT_ID_TO_SLUG.get(sport_id, slug)
    upcoming_url = f"{_BASE}/en/sports-betting/{sport_url_slug}-{sport_id}/upcoming-games/?filterDay=-1"

    with sync_playwright() as pw:
        browser, context = _new_context(pw, headless=True)
        try:
            # 1. Collect all game IDs (raw items)
            listing_page = context.new_page()
            try:
                all_items = _collect_upcoming_game_ids(listing_page, upcoming_url, sport_id, max_matches)
            finally:
                listing_page.close()

            # Apply offset and limit
            if offset:
                all_items = all_items[offset:]
            if max_matches:
                all_items = all_items[:max_matches]

            inline_count = 0
            yielded = 0
            for item in all_items:
                parsed = _parse_match_item(item)
                if not parsed:
                    continue
                game_id = parsed["sp_game_id"]
                raw_mkts = []
                if fetch_full_markets and game_id:
                    # Open a new page to fetch markets (avoids interfering with pagination)
                    market_page = context.new_page()
                    try:
                        raw_mkts = _fetch_markets_for_game(market_page, game_id, sport_id)
                        if not raw_mkts:
                            raw_mkts = parsed["_inline_mkts"]
                            inline_count += 1
                    finally:
                        market_page.close()
                    time.sleep(sleep_between)
                else:
                    raw_mkts = parsed["_inline_mkts"]

                markets = _parse_markets(
                    raw_mkts,
                    game_id=game_id if debug_ou else "",
                    sport_id=parsed.get("sp_sport_id", int(sport_id)),
                )
                match_dict = _build_match(parsed, markets, sport_slug)
                yield match_dict

                yielded += 1
                if max_matches and yielded >= max_matches:
                    break

                # Trigger analytics for near-term matches
                if _is_near_term(match_dict.get("start_time"), days=3):
                    scrape_sportpesa_match_analytics.apply_async(
                        args=[match_dict["sp_game_id"]],
                        kwargs={"unified_match_id": None},
                        queue="analytics",
                        countdown=random.uniform(5, 30),
                    )
            if inline_count:
                print(f"[sp:{sport_slug}] {inline_count} games used inline fallback markets")
        finally:
            browser.close()

def fetch_upcoming(
    sport_slug:         str,
    days:               int | None = None,
    max_matches:        int | None = None,
    offset:             int        = 0,
    fetch_full_markets: bool       = True,
    sleep_between:      float      = 0.3,
    debug_ou:           bool       = False,
    **_
) -> list[dict]:
    return list(fetch_upcoming_stream(
        sport_slug, days=days, max_matches=max_matches, offset=offset,
        fetch_full_markets=fetch_full_markets, sleep_between=sleep_between,
        debug_ou=debug_ou
    ))

# =============================================================================
# LIVE
# =============================================================================

def fetch_live_stream(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.3,
    debug_ou:           bool  = False,
    **_
) -> Generator[dict, None, None]:
    """
    Yield live matches with full markets (intercepted via Playwright).
    """
    slug = sport_slug.lower().replace(" ", "-")
    sport_id = SP_SPORT_ID.get(slug)
    if not sport_id:
        print(f"[sp] unknown sport: {sport_slug!r}")
        return
    live_url = f"{_BASE}/en/live/events?sportId={sport_id}"

    with sync_playwright() as pw:
        browser, context = _new_context(pw, headless=True)
        try:
            page = context.new_page()
            # Navigate to the live page and intercept the event list API
            data = _intercept_api(page, f"/api/live/sports/{sport_id}/events", lambda: page.goto(
                live_url, wait_until="domcontentloaded", timeout=30000
            ))
            event_items = _extract_items(data)
            # Fallback to generic live games endpoint if needed
            if not event_items:
                data2 = _intercept_api(page, "/api/live/games", lambda: page.goto(
                    f"{_BASE}/api/live/games?sportId={sport_id}", wait_until="domcontentloaded", timeout=30000
                ))
                event_items = _extract_items(data2)

            inline_count = 0
            for item in event_items:
                parsed = _parse_match_item(item)
                if not parsed:
                    continue
                game_id = parsed["sp_game_id"]
                raw_mkts = []
                if fetch_full_markets and game_id:
                    # Use page.evaluate to fetch markets from within the page context
                    try:
                        raw_mkts = page.evaluate("""
                            async (gameId) => {
                                const resp = await fetch('/api/games/markets?games=' + gameId + '&markets=all');
                                const json = await resp.json();
                                // The response may have the game ID as a key
                                if (json && typeof json === 'object' && !Array.isArray(json)) {
                                    return json[gameId] || json.markets || [];
                                }
                                return json;
                            }
                        """, game_id)
                        if isinstance(raw_mkts, dict):
                            raw_mkts = raw_mkts.get(game_id) or raw_mkts.get(str(game_id)) or raw_mkts.get("markets") or []
                    except Exception as e:
                        print(f"[pw] live markets fetch error {game_id}: {e}")
                    if not raw_mkts:
                        raw_mkts = parsed["_inline_mkts"]
                        inline_count += 1
                    time.sleep(sleep_between)
                else:
                    raw_mkts = parsed["_inline_mkts"]

                markets = _parse_markets(
                    raw_mkts,
                    game_id=game_id if debug_ou else "",
                    sport_id=parsed.get("sp_sport_id", int(sport_id)),
                )
                yield _build_match(parsed, markets, sport_slug, status="live")

            if inline_count:
                print(f"[sp:{sport_slug}:live] {inline_count} events used inline fallback")
        finally:
            browser.close()

def fetch_live(
    sport_slug:         str,
    fetch_full_markets: bool  = True,
    sleep_between:      float = 0.3,
    debug_ou:           bool  = False,
    **_
) -> list[dict]:
    return list(fetch_live_stream(
        sport_slug,
        fetch_full_markets=fetch_full_markets,
        sleep_between=sleep_between,
        debug_ou=debug_ou
    ))

__all__ = [
    "fetch_upcoming_stream",
    "fetch_live_stream",
    "fetch_upcoming",
    "fetch_live",
    "SP_SPORT_ID",
]