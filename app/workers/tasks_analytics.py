import asyncio
import base64
import json
import logging
import random
import time
from datetime import datetime

from celery import Task
from playwright.async_api import async_playwright

from app.workers.celery_tasks import celery, _redis
from app.extensions import db
from app.models.match_analytics import MatchAnalytics
from app.models.odds import UnifiedMatch

logger = logging.getLogger(__name__)

# Browser pool to avoid launching new browser per task
_browser_instance = None
_browser_lock = asyncio.Lock()

async def get_browser():
    global _browser_instance
    async with _browser_lock:
        if _browser_instance is None or not _browser_instance.is_connected():
            playwright = await async_playwright().start()
            _browser_instance = await playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                ]
            )
        return _browser_instance

async def scrape_sportradar_analytics(sp_match_id: str) -> dict:
    """Scrape stats, H2H, standings from Sportradar page."""
    result = {
        "sp_match_id": sp_match_id,
        "match_info": {},
        "statistics": {},
        "head_to_head": [],
        "standings": {},
        "team_form": {},
        "success": False,
    }

    try:
        browser = await get_browser()
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="en-US",
        )
        page = await context.new_page()

        # Navigate to main match page
        url = f"https://statshub.sportradar.com/sportpesa/en/match/{sp_match_id}"
        await page.goto(url, wait_until="networkidle")
        await asyncio.sleep(random.uniform(2, 4))

        # Extract basic match info
        match_info = await page.evaluate("""
            () => {
                const info = {};
                const venueEl = document.querySelector('[data-testid="venue"]');
                if (venueEl) info.venue = venueEl.innerText.trim();
                const refereeEl = document.querySelector('[data-testid="referee"]');
                if (refereeEl) info.referee = refereeEl.innerText.trim();
                const attendanceEl = document.querySelector('[data-testid="attendance"]');
                if (attendanceEl) info.attendance = attendanceEl.innerText.trim();
                return info;
            }
        """)
        result["match_info"] = match_info

        # Click on Statistics tab
        stats_tab = await page.query_selector('a[href*="/statistics"]')
        if stats_tab:
            await stats_tab.click()
            await asyncio.sleep(2)
            stats = await page.evaluate("""
                () => {
                    const stats = {};
                    document.querySelectorAll('[data-testid="statistics-row"]').forEach(row => {
                        const label = row.querySelector('[data-testid="stat-label"]')?.innerText.trim();
                        const home = row.querySelector('[data-testid="home-value"]')?.innerText.trim();
                        const away = row.querySelector('[data-testid="away-value"]')?.innerText.trim();
                        if (label) stats[label] = {home, away};
                    });
                    return stats;
                }
            """)
            result["statistics"] = stats

        # Click on H2H tab
        h2h_tab = await page.query_selector('a[href*="/h2h"]')
        if h2h_tab:
            await h2h_tab.click()
            await asyncio.sleep(2)
            h2h = await page.evaluate("""
                () => {
                    const matches = [];
                    document.querySelectorAll('[data-testid="h2h-match"]').forEach(el => {
                        const home = el.querySelector('[data-testid="home-team"]')?.innerText;
                        const away = el.querySelector('[data-testid="away-team"]')?.innerText;
                        const score = el.querySelector('[data-testid="score"]')?.innerText;
                        const date = el.querySelector('[data-testid="date"]')?.innerText;
                        if (home && away) matches.push({home, away, score, date});
                    });
                    return matches;
                }
            """)
            result["head_to_head"] = h2h

        # Click on Standings tab
        standings_tab = await page.query_selector('a[href*="/standings"]')
        if standings_tab:
            await standings_tab.click()
            await asyncio.sleep(2)
            standings = await page.evaluate("""
                () => {
                    const table = [];
                    document.querySelectorAll('[data-testid="standings-row"]').forEach(row => {
                        const pos = row.querySelector('[data-testid="position"]')?.innerText;
                        const team = row.querySelector('[data-testid="team"]')?.innerText;
                        const pts = row.querySelector('[data-testid="points"]')?.innerText;
                        if (team) table.push({pos, team, pts});
                    });
                    return table;
                }
            """)
            result["standings"] = standings

        await context.close()
        result["success"] = True
        return result

    except Exception as e:
        logger.error(f"Playwright scrape failed for sp_match_id={sp_match_id}: {e}")
        return result

@celery.task(
    name="tasks.analytics.scrape_sportpesa_match",
    bind=True,
    max_retries=2,
    default_retry_delay=60,
    soft_time_limit=120,
    time_limit=150,
    acks_late=True,
)
def scrape_sportpesa_match_analytics(self, sp_match_id: str, unified_match_id: int = None) -> dict:
    """Celery task to scrape and store analytics for a SportPesa match."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        data = loop.run_until_complete(scrape_sportradar_analytics(sp_match_id))
    except Exception as e:
        logger.error(f"Analytics task failed for {sp_match_id}: {e}")
        raise self.retry(exc=e)

    if not data.get("success"):
        return {"ok": False, "sp_match_id": sp_match_id}

    # Store in database
    try:
        if not unified_match_id:
            # Try to find UnifiedMatch by sp_match_id
            from app.models.bookmakers_model import BookmakerMatchLink
            link = BookmakerMatchLink.query.filter_by(
                external_match_id=sp_match_id
            ).first()
            if link:
                unified_match_id = link.match_id

        if unified_match_id:
            analytics = MatchAnalytics.query.filter_by(
                unified_match_id=unified_match_id
            ).first()
            if not analytics:
                analytics = MatchAnalytics(unified_match_id=unified_match_id)
                db.session.add(analytics)
            analytics.sp_match_id = sp_match_id
            analytics.betradar_id = data.get("betradar_id")  # may be extracted
            analytics.match_info = data.get("match_info")
            analytics.statistics = data.get("statistics")
            analytics.head_to_head = data.get("head_to_head")
            analytics.standings = data.get("standings")
            analytics.team_form = data.get("team_form")
            db.session.commit()
        else:
            # Cache in Redis for later linking
            r = _redis()
            r.setex(f"analytics:pending:{sp_match_id}", 86400, json.dumps(data))
            logger.info(f"Analytics for {sp_match_id} cached (no unified_match_id yet)")

    except Exception as e:
        logger.error(f"DB save failed for analytics {sp_match_id}: {e}")
        db.session.rollback()

    return {"ok": True, "sp_match_id": sp_match_id, "unified_match_id": unified_match_id}