"""
playwright_scraper.py — Sportradar data collector via Playwright.

Phase 1: Navigate match page tabs → intercept all gismo API responses.
Phase 2: Extract the auth token from any intercepted URL → directly fetch
         any critical endpoints the page navigation didn't trigger.

No hardcoded tokens — they're harvested live from Phase 1 responses.
"""
import asyncio
import random
import logging
from playwright.async_api import async_playwright

log = logging.getLogger("playwright_scraper")

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

# Statshub tabs — each triggers a different subset of API calls
_TABS = [
    "",              # overview  → match_info, match_squads, match_timeline
    "/report",       # report    → match_timeline (full), timelinedelta
    "/statistics",   # stats     → stats_match_situation, stats_season_uniqueteamstats,
                     #             stats_season_teamscoringconceding, stats_team_lastx
    "/head-to-head", # h2h       → stats_match_head2head, stats_team_versusrecent
    "/table",        # table     → season_dynamictable, stats_formtable,
                     #             stats_season_tables, stats_season_topgoals
]

# API base URLs — sh hosts most stats, lmt hosts match events / h2h
_SH  = "https://sh.fn.sportradar.com/sportpesa/en/Etc:UTC/gismo"
_LMT = "https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo"
_SH_HEADERS  = {"origin": "https://statshub.sportradar.com",
                "referer": "https://statshub.sportradar.com/",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
_LMT_HEADERS = {"origin": "https://www.betika.com",
                "referer": "https://www.betika.com/",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


async def _collect_async(match_id: str, idle_timeout: int = 12000) -> dict:
    collected: dict = {}
    tokens: dict = {}   # {"sh": "exp=...", "lmt": "exp=..."}

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
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "sec-ch-ua": '"Chromium";v="124","Google Chrome";v="124","Not-A.Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "Upgrade-Insecure-Requests": "1",
            },
        )
        await context.add_init_script(_STEALTH)
        page = await context.new_page()

        # ── Phase 1 response handler ──────────────────────────────────────────
        async def handle_response(response):
            url = response.url
            if "fn.sportradar.com" not in url or "gismo" not in url:
                return
            if response.request.method == "OPTIONS":
                return
            # Harvest token from the URL on first sight of each domain
            if "?T=" in url:
                tok = url.split("?T=")[1].split("&")[0]
                if "sh.fn." in url and "sh" not in tokens:
                    tokens["sh"] = tok
                elif "lmt.fn." in url and "lmt" not in tokens:
                    tokens["lmt"] = tok
            try:
                raw  = await response.json()
                docs = raw.get("doc", [])
                if not docs:
                    return
                doc       = docs[0]
                query_url = doc.get("queryUrl")
                if not query_url:
                    parts = url.split("?")[0].split("/gismo/")
                    if len(parts) > 1:
                        query_url = parts[1].strip("/")
                if query_url:
                    collected[query_url] = doc.get("data") or {}
                    log.debug(f"intercepted → {query_url}")
            except Exception as e:
                log.debug(f"parse failed ({url[:80]}): {e}")

        page.on("response", handle_response)

        # ── Phase 1: navigate tabs ────────────────────────────────────────────
        base = f"https://statshub.sportradar.com/sportpesa/en/match/{match_id}"
        for tab in _TABS:
            target = f"{base}{tab}"
            try:
                await page.goto(target, wait_until="networkidle", timeout=idle_timeout)
                await asyncio.sleep(random.uniform(1.5, 2.5))
            except Exception as e:
                log.debug(f"goto {target}: {e}")
                await asyncio.sleep(1.5)

        log.info(f"Phase 1 done — {len(collected)} endpoints, tokens: {list(tokens.keys())}")

        # ── Phase 2: direct-fetch anything still missing ──────────────────────
        # Derive team/season IDs from whatever match_info we collected
        info  = (collected.get(f"match_info_statshub/{match_id}")
                 or collected.get(f"match_info/{match_id}") or {})
        md    = info.get("match", {})
        teams = md.get("teams", {})
        h_uid = str((teams.get("home") or {}).get("uid", ""))
        a_uid = str((teams.get("away") or {}).get("uid", ""))
        s_id  = str(md.get("_seasonid", ""))

        def _missing(*keys):
            """True if none of the given queryUrl keys are in collected."""
            return not any(k in collected for k in keys)

        # Build list of (path, base_url, token, headers) to fetch
        to_fetch = []

        sh  = tokens.get("sh")
        lmt = tokens.get("lmt") or tokens.get("sh")  # lmt sometimes shares sh token

        # H2H / managers — from versusrecent (lmt endpoint)
        if h_uid and a_uid and lmt and _missing(f"stats_team_versusrecent/{h_uid}/{a_uid}"):
            to_fetch.append((f"stats_team_versusrecent/{h_uid}/{a_uid}", _LMT, lmt, _LMT_HEADERS))

        # Upcoming fixtures for both teams (sh endpoint)
        if h_uid and sh and _missing(f"stats_team_fixtures/{h_uid}/10",
                                      f"stats_team_fixtures/{h_uid}/5"):
            to_fetch.append((f"stats_team_fixtures/{h_uid}/10", _SH, sh, _SH_HEADERS))

        if a_uid and sh and _missing(f"stats_team_fixtures/{a_uid}/10",
                                      f"stats_team_fixtures/{a_uid}/5"):
            to_fetch.append((f"stats_team_fixtures/{a_uid}/10", _SH, sh, _SH_HEADERS))

        # Season team stats aggregate (sh endpoint)
        if s_id and sh and _missing(f"stats_season_uniqueteamstats/{s_id}"):
            to_fetch.append((f"stats_season_uniqueteamstats/{s_id}", _SH, sh, _SH_HEADERS))

        # Top scorers per team if not already collected
        if s_id and h_uid and sh and _missing(f"stats_season_topgoals/{s_id}/{h_uid}"):
            to_fetch.append((f"stats_season_topgoals/{s_id}/{h_uid}", _LMT, lmt or sh, _LMT_HEADERS))
        if s_id and a_uid and sh and _missing(f"stats_season_topgoals/{s_id}/{a_uid}"):
            to_fetch.append((f"stats_season_topgoals/{s_id}/{a_uid}", _LMT, lmt or sh, _LMT_HEADERS))

        # Recent last-x if not loaded by statistics tab
        if h_uid and sh and _missing(
            f"stats_team_lastx/{h_uid}/20",
            f"stats_team_lastx/{h_uid}/10",
            f"stats_team_lastx/{h_uid}/5",
        ):
            to_fetch.append((f"stats_team_lastx/{h_uid}/10", _SH, sh, _SH_HEADERS))

        if a_uid and sh and _missing(
            f"stats_team_lastx/{a_uid}/20",
            f"stats_team_lastx/{a_uid}/10",
            f"stats_team_lastx/{a_uid}/5",
        ):
            to_fetch.append((f"stats_team_lastx/{a_uid}/10", _SH, sh, _SH_HEADERS))

        # Form table and season table if still missing
        if s_id and sh and _missing(f"stats_formtable/{s_id}"):
            to_fetch.append((f"stats_formtable/{s_id}", _SH, sh, _SH_HEADERS))
        if s_id and sh and _missing(f"season_dynamictable/{s_id}", f"stats_season_tables/{s_id}/1"):
            to_fetch.append((f"season_dynamictable/{s_id}", _SH, sh, _SH_HEADERS))

        # ── Fire all direct fetches in parallel ───────────────────────────────
        async def direct_fetch(path, base_url, token, headers):
            url = f"{base_url}/{path}?T={token}"
            try:
                resp = await context.request.get(url, headers=headers, timeout=10000)
                if not resp.ok:
                    log.debug(f"direct fetch {path}: HTTP {resp.status}")
                    return
                raw  = await resp.json()
                docs = raw.get("doc", [])
                if not docs:
                    return
                doc       = docs[0]
                query_url = doc.get("queryUrl") or path
                data      = doc.get("data") or {}
                if query_url and data:
                    collected[query_url] = data
                    log.info(f"Phase 2 ✓ {query_url}")
            except Exception as e:
                log.debug(f"direct fetch failed {path}: {e}")

        if to_fetch:
            log.info(f"Phase 2: fetching {len(to_fetch)} missing endpoints")
            await asyncio.gather(*[direct_fetch(p, b, t, h) for p, b, t, h in to_fetch])

        await browser.close()

    log.info(f"match {match_id}: total {len(collected)} endpoints collected")
    return collected


def collect_match_data(match_id: str) -> dict:
    """Synchronous wrapper. Returns {queryUrl: data_dict} or {} on failure."""
    try:
        return asyncio.run(_collect_async(match_id))
    except Exception as e:
        log.error(f"collect_match_data({match_id}) failed: {e}")
        return {}


def get(collected: dict, *query_url_variants) -> dict:
    """
    Try multiple queryUrl keys, return first match or {}.
    Handles variant naming (e.g. /5 vs /10 vs /20).
    """
    for key in query_url_variants:
        val = collected.get(key)
        if val:
            return val
    return {}