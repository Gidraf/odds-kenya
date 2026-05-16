"""
app/workers/sp_harvester.py – Playwright version
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

# Sport slug → SP sport_id (kept as before)
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

# SP sport_id → URL slug used on the SportPesa website
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

# Cookies taken from a working session – they prevent annoying pop-ups and bot checks
_COOKIES = [
    {"name": "visited", "value": "1", "domain": ".ke.sportpesa.com", "path": "/"},
    {"name": "settings", "value": "%7B%22betslip%22%3A%7B%22acceptOdds%22%3Atrue%2C%22amount%22%3Anull%2C%22direct%22%3Afalse%2C%22betSpinnerSkipAnimation%22%3Afalse%2C%22globalBetSpinnerEnabled%22%3Atrue%7D%2C%22markets_layout%22%3A%22multiple%22%2C%22single-wallet-first-phase%22%3A%221%22%7D", "domain": ".ke.sportpesa.com", "path": "/"},
    {"name": "_ga", "value": "GA1.1.1758714192.1765178328", "domain": ".ke.sportpesa.com", "path": "/"},
    {"name": "spkessid", "value": "25fc9dad193a25829b456512ce145639", "domain": ".ke.sportpesa.com", "path": "/"},
    {"name": "device_view", "value": "full", "domain": ".ke.sportpesa.com", "path": "/"},
    {"name": "locale", "value": "en", "domain": ".ke.sportpesa.com", "path": "/"},
    # You may need to refresh the ak_bmsc and bm_sv cookies occasionally.
    {"name": "ak_bmsc", "value": "E4E1D76684AC35FFA934A28F80ACEB58~000000000000000000000000000000~YAAQ5mrXFyW63B+eAQAA0auOMB/32AH8j0nc28EKKiXmSagzMJvziitKwGuq/lEEyrpg24LOzZ6z/f2hefNm6aTUpLOZLEbAZup3J/Q81mskWkmxoyEIX1HZ0RHI19Ognww7OfRTgJY7zuZsP8rYCKyJBHFoiWQIKFyHvjuawb7EhBeZbkHJr9rUxSRcrbV6cGLVeIzB6SIIqKACBU6ND4dHUr6SvgpOkum1RKVBEHHDLCH5jZTgcCzC30ALeMhmPh0qXioS9zPJU+WG1dFt20u8vJejB4QMk6uSl199+HfD+tx7sg4JipBWAy7x7dR9dZdXJ9727LqsfjZJOsGnqYPwuCbQNmswskvWiV4BLq+dVWGSlv9h9uRbRkslf4jrX2CmyvDJo5ldjrx9Ehy0ODk=", "domain": ".ke.sportpesa.com", "path": "/"},
    {"name": "bm_sv", "value": "1F2A08DE525489CF2A57728F46F01B6A~YAAQXqERAr7ubCKeAQAAqlDTMB9q6UpPaXmnX1Zt5rDym35YRsCZiqkEWtqiLpmF6iBpXtr9N7iKR+Zzf+pIuAmnyyku97fSGqCogCX6IzwuyPyZvn3WtPEH60hbc9oMXD6FrZ750BIqaPrA2/I08An8+nWooiyPIsc1u1OfioiTURxYhY4ArRoG7YD8+H8MB/TOkxFS9vNMdMsiaVhXk52Xrm94C6bZAUupXE5Ac8HjVaL5Pdz8KDpMbi9Wabimery4Z9KgEA==~1", "domain": ".ke.sportpesa.com", "path": "/"},
]

# =============================================================================
# PARSERS (unchanged from original – kept for reference)
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

    # Set a few harmless cookies before visiting the site to avoid pop-ups.
    # The rest will be set by the site itself.
    page.goto("https://www.ke.sportpesa.com", wait_until="domcontentloaded", timeout=30000)
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

    # Wait for the homepage to fully settle (bot detection scripts, etc.).
    # Adjust the timeout based on your network.
    page.wait_for_timeout(5000)

    # Now the context is fully “warmed up” — all required cookies are present.
    page.close()
    return browser, context


def _wait_for_api_response(page, url_substring: str, timeout: int = 30000):
    """Wait for a response whose URL contains the given substring and return its JSON body."""
    try:
        with page.expect_response(lambda resp: url_substring in resp.url and resp.status == 200, timeout=timeout) as resp_info:
            pass  # just waiting, the response will be captured
        response = resp_info.value
        return response.json()
    except PlaywrightTimeout:
        print(f"[pw] timeout waiting for {url_substring}")
        return None
    except Exception as e:
        print(f"[pw] error waiting for {url_substring}: {e}")
        return None

# =============================================================================
# UPCOMING – scrape all pages and then fetch markets
# =============================================================================

def _collect_upcoming_game_ids(page, sport_id: str, max_items: int | None) -> list[dict]:
    """Collect all raw game objects (from all pages) for a sport."""
    collected = []
    # We start with the initial page load; the API call with pag_min=1 is triggered automatically.
    # We'll intercept that response, then click through pages.
    current_page = 1
    while True:
        # Wait for the upcoming API response that loads this page.
        # The URL pattern is fixed; the pag_min parameter changes.
        # We rely on the page loading the first response, then we click "Next".
        # But we need to capture the data from each response. After page load or click,
        # a new request is made. We'll use a loop that waits for the next upcoming response.
        data = _wait_for_api_response(page, "/api/upcoming/games")
        if not data:
            break
        if isinstance(data, dict):
            for key in ("data", "games", "items", "results"):
                if isinstance(data.get(key), list):
                    data = data[key]
                    break
        if not isinstance(data, list):
            break
        for item in data:
            if max_items and len(collected) >= max_items:
                break
            gid = str(item.get("id") or "")
            if gid:
                collected.append(item)
        if max_items and len(collected) >= max_items:
            break
        # Check for next page button
        next_btn = page.query_selector('button.next-button:not([disabled])')
        if not next_btn:
            # Also check the pagination list for "Next" li
            next_li = page.query_selector('ul.event-list-pagination li[translate="next"]:not(.page-disabled)')
            if next_li:
                next_li.click()
            else:
                break
        else:
            next_btn.click()
        # Wait a moment for the next request to fire
        page.wait_for_timeout(1000)
        current_page += 1
    return collected

def _fetch_markets_for_game(page, game_id: str, sport_id: str) -> list[dict]:
    """Navigate to a game's markets page and intercept the markets API response."""
    url = f"{_BASE}/games/{game_id}/markets?sportId={sport_id}&section=upcoming-games&filterDay=-1"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception as e:
        print(f"[pw] goto markets page for {game_id}: {e}")
        return []
    # The markets API call is triggered automatically; wait for it
    markets_data = _wait_for_api_response(page, "/api/games/markets")
    if not markets_data:
        return []
    # Extract list of markets from the response
    if isinstance(markets_data, dict):
        # Try to locate the list inside the dict
        for key in ("data", "result", game_id, int(game_id) if game_id.isdigit() else None):
            if key and isinstance(markets_data.get(key), list):
                return markets_data[key]
        # fallback: if the dict contains a list somewhere
        for v in markets_data.values():
            if isinstance(v, list):
                return v
    elif isinstance(markets_data, list):
        return markets_data
    return []

# =============================================================================
# PUBLIC API – UPCOMING STREAM (synchronous generator)
# =============================================================================

def fetch_upcoming_stream(
    sport_slug:         str,
    days:               int | None   = None,      # ignored, kept for compatibility
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
    # Map slug to sport_id
    slug = sport_slug.lower().replace(" ", "-")
    sport_id = SP_SPORT_ID.get(slug)
    if not sport_id:
        print(f"[sp] unknown sport: {sport_slug!r}")
        return
    sport_url_slug = _SPORT_ID_TO_SLUG.get(sport_id, slug)
    upcoming_url = f"{_BASE}/en/sports-betting/{sport_url_slug}-{sport_id}/upcoming-games/?filterDay=-1"

    with sync_playwright() as pw:
        browser, context = _new_context(pw)
        page = context.new_page()
        try:
            page.goto(upcoming_url, wait_until="domcontentloaded", timeout=30000)
            # Collect all game items from all pages
            all_items = _collect_upcoming_game_ids(page, sport_id, max_matches)
        finally:
            browser.close()

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
        markets = {}
        if fetch_full_markets and game_id:
            # Need a fresh browser context for each market fetch to avoid referer issues?
            # We'll reuse a new browser inside the loop for simplicity.
            with sync_playwright() as pw:
                browser2, context2 = _new_context(pw)
                page2 = context2.new_page()
                try:
                    raw_mkts = _fetch_markets_for_game(page2, game_id, sport_id)
                    if not raw_mkts:
                        raw_mkts = parsed["_inline_mkts"]
                        inline_count += 1
                finally:
                    browser2.close()
            time.sleep(sleep_between)
        else:
            raw_mkts = parsed["_inline_mkts"]
        markets = _parse_markets(raw_mkts, game_id if debug_ou else "", int(sport_id))
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

def _collect_live_event_markets(page, sport_id: str, max_matches: int | None) -> list[dict]:
    """
    Intercept the live/event/markets API and parse out individual events with their markets.
    Returns a list of ready-to-yield match dicts (without market normalization yet).
    """
    matches = []
    while True:
        data = _wait_for_api_response(page, "/api/live/event/markets")
        if not data:
            break
        if isinstance(data, dict):
            # The live markets response usually has a "markets" key containing a dict keyed by eventId
            events_markets = data.get("markets") or data.get("data") or data
            if isinstance(events_markets, dict):
                for event_id, market_list in events_markets.items():
                    if not isinstance(market_list, list):
                        continue
                    # We need the event metadata; the response sometimes includes event info separately.
                    # In the provided curl, the request parameters already include event IDs; the response
                    # contains the markets keyed by eventId, but we lack home/away/start time.
                    # We must extract event details from the live listing page or from the API response itself.
                    # The live page also makes a separate call for event details? Let's handle common patterns.
                    # For simplicity, we'll parse what we have. The response often contains minimal info.
                    # A better approach: before intercepting markets, we can extract event data from the page's
                    # initial HTML or from another API call that lists events. We'll keep a placeholder.
                    # However, the original code used _fetch_live_list() and then parsed items.
                    # We can still do that: first intercept the live event list, then for each batch we get
                    # the markets. We'll adapt _fetch_live_list to use Playwright.
        # Since this function becomes complex, we'll use a different strategy:
        # We'll first get the list of live events and their details via the page's data,
        # then use the markets API to attach odds. The live page likely has embedded JSON.
        # To keep the code compact and functional, I'll implement a two-step:
        #   1. Navigate to live page, wait for an API call that returns event details (maybe /api/live/events?).
        #   2. Then wait for the markets call.
        # This matches the original logic where _fetch_live_list was called first.
        pass
    return matches

# Due to complexity, I'll rewrite live fetching using the original pattern:
# - Fetch the list of live events (navigate to live page, capture /api/live/sports/{sport_id}/events or similar)
# - For each event, get its markets (the page automatically loads markets for the first batch; we need to handle "Next" clicks that load more events and their markets).
# I'll provide a simplified but functional version that mimics the original behaviour.

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
        browser, context = _new_context(pw)
        page = context.new_page()
        # Step 1: load the live page and get event list from the API response
        page.goto(live_url, wait_until="domcontentloaded", timeout=30000)
        # The page immediately calls something like /api/live/sports/{id}/events
        events_data = _wait_for_api_response(page, f"/api/live/sports/{sport_id}/events")
        if not events_data:
            # try alternate endpoint
            events_data = _wait_for_api_response(page, "/api/live/games")
        # Extract event items
        event_items = []
        if isinstance(events_data, dict):
            for key in ("events", "data", "items"):
                if isinstance(events_data.get(key), list):
                    event_items = events_data[key]
                    break
        elif isinstance(events_data, list):
            event_items = events_data

        # For each event we need to get its markets. The page also calls the markets API for the first batch.
        # We can intercept that response and match it to the events.
        # However, it's easier: we already have the event IDs; we can navigate to each event's page or
        # directly call the markets API from the page context using fetch().
        # I'll choose to use page.evaluate() to fetch the markets for the event IDs we have.
        # The original live code did: for each raw event, call _fetch_live_event_details_markets or _fetch_markets.
        # We'll do a similar loop.
        inline_count = 0
        for item in event_items:
            parsed = _parse_match_item(item)
            if not parsed:
                continue
            game_id = parsed["sp_game_id"]
            raw_mkts = []
            if fetch_full_markets and game_id:
                # Use page.evaluate to fetch markets from within the page (keeps referer and cookies)
                try:
                    raw_mkts = page.evaluate("""
                        async (gameId) => {
                            const resp = await fetch('/api/games/markets?games=' + gameId + '&markets=all');
                            return await resp.json();
                        }
                    """, game_id)
                    # The response might be nested
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
            markets = _parse_markets(raw_mkts, game_id if debug_ou else "", int(sport_id))
            yield _build_match(parsed, markets, sport_slug, status="live")
        if inline_count:
            print(f"[sp:{sport_slug}:live] {inline_count} events used inline fallback")

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

# =============================================================================
# Additional utilities (kept for compatibility)
# =============================================================================

__all__ = [
    "fetch_upcoming_stream",
    "fetch_live_stream",
    "fetch_upcoming",
    "fetch_live",
    "SP_SPORT_ID",
]