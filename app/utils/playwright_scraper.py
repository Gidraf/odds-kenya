"""
playwright_scraper.py — Shared Sportradar data collector via Playwright.

Navigates statshub.sportradar.com match pages, intercepts every
fn.sportradar.com/gismo/* response, and returns a dict keyed by
queryUrl (e.g. "match_timeline/70807446" → {data dict}).

No tokens needed — the browser session handles auth automatically.
The same stealth setup as bp_raw_stream.py so bot detection stays
consistent across both code paths.
"""
import asyncio
import random
import logging
from playwright.async_api import async_playwright

log = logging.getLogger("playwright_scraper")

# ── Browser launch args (same as bp_raw_stream) ──────────────────────────────
_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-infobars",
    "--window-size=1280,800",
    "--disable-extensions",
    "--disable-plugins-discovery",
    "--disable-web-security",
    "--disable-features=IsolateOrigins,site-per-process",
]

# ── Stealth init script (same as bp_raw_stream) ───────────────────────────────
_STEALTH = """
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins',   { get: () => [1,2,3,4,5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
    window.chrome = { runtime:{}, loadTimes:function(){}, csi:function(){}, app:{} };
    const _oq = window.navigator.permissions.query;
    window.navigator.permissions.query = (p) =>
        p.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : _oq(p);
    Object.defineProperty(screen, 'availHeight', { get: () => 800  });
    Object.defineProperty(screen, 'availWidth',  { get: () => 1280 });
    const _gp = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {
        if (param === 37445) return 'Intel Inc.';
        if (param === 37446) return 'Intel Iris OpenGL Engine';
        return _gp.call(this, param);
    };
    delete window.__playwright;
    delete window.__pw_manual;
    delete window.playwright;
"""

# ── Tabs to visit for a match — each triggers a different set of API calls ────
# Statshub loads different gismo endpoints per tab:
#   /match/{id}             → match_info, match_timeline, match_squads, match_timelinedelta
#   /match/{id}/report      → match_timeline (richer), match_timelinedelta
#   /match/{id}/statistics  → stats_match_situation, stats_season_uniqueteamstats,
#                             stats_season_teamscoringconceding, stats_team_lastx
#   /match/{id}/head-to-head → stats_match_head2head, stats_team_versusrecent
#   /match/{id}/table        → season_dynamictable, stats_formtable,
#                              stats_season_tables, stats_season_topgoals
_TABS = [
    "",                 # overview
    "/report",
    "/statistics",
    "/head-to-head",
    "/table",
]


async def _collect_async(match_id: str, idle_timeout: int = 12000) -> dict:
    """
    Navigate all match page tabs and collect every intercepted gismo response.
    Returns {queryUrl: data_dict}.
    idle_timeout: ms to wait for network idle after each tab navigation.
    """
    collected: dict = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=_LAUNCH_ARGS)

        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/New_York",
            geolocation={"longitude": -73.935242, "latitude": 40.730610},
            permissions=["geolocation"],
            color_scheme="light",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "sec-ch-ua": '"Chromium";v="124","Google Chrome";v="124","Not-A.Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "Upgrade-Insecure-Requests": "1",
            },
        )

        await context.add_init_script(_STEALTH)
        page = await context.new_page()

        # ── Intercept every fn.sportradar.com gismo response ─────────────────
        async def handle_response(response):
            url = response.url
            if "fn.sportradar.com" not in url or "gismo" not in url:
                return
            if response.request.method == "OPTIONS":
                return
            try:
                raw = await response.json()
                docs = raw.get("doc", [])
                if not docs:
                    return
                doc = docs[0]
                # Use queryUrl from the doc when available — it's the canonical key
                query_url = doc.get("queryUrl")
                if not query_url:
                    # Fallback: extract the path after /gismo/ and strip token
                    parts = url.split("?")[0].split("/gismo/")
                    if len(parts) > 1:
                        query_url = parts[1].strip("/")
                if query_url:
                    data = doc.get("data") or {}
                    collected[query_url] = data
                    log.debug(f"intercepted → {query_url}")
            except Exception as e:
                log.debug(f"response parse failed ({url[:80]}): {e}")

        page.on("response", handle_response)

        # ── Navigate each tab ─────────────────────────────────────────────────
        base = f"https://statshub.sportradar.com/sportpesa/en/match/{match_id}"

        for tab in _TABS:
            target = f"{base}{tab}"
            try:
                await page.goto(
                    target,
                    wait_until="networkidle",
                    timeout=idle_timeout,
                )
                # Short human-like pause between tabs so requests settle
                await asyncio.sleep(random.uniform(1.0, 2.0))
            except Exception as e:
                # networkidle timeout is common on slower matches — data is still collected
                log.debug(f"goto {target}: {e}")
                await asyncio.sleep(1.0)

        await browser.close()

    log.info(f"match {match_id}: collected {len(collected)} endpoints → {list(collected.keys())}")
    return collected


def collect_match_data(match_id: str, timeout: int = 90) -> dict:
    """
    Synchronous wrapper around _collect_async.
    Blocks until all tabs have been navigated or timeout is reached.
    Returns {queryUrl: data_dict} or {} on failure.
    """
    try:
        # asyncio.run() creates a fresh event loop — safe in threads
        return asyncio.run(_collect_async(match_id))
    except Exception as e:
        log.error(f"collect_match_data({match_id}) failed: {e}")
        return {}


def get(collected: dict, *query_url_variants) -> dict:
    """
    Look up data from the collected dict, trying multiple queryUrl variants.
    Returns the first match found, or {}.

    Examples:
        get(c, "match_info_statshub/123", "match_info/123")
        get(c, "season_dynamictable/999", "stats_season_tables/999/1")
        get(c, "stats_team_lastx/456/20", "stats_team_lastx/456/10", "stats_team_lastx/456/5")
    """
    for key in query_url_variants:
        val = collected.get(key)
        if val:
            return val
    return {}