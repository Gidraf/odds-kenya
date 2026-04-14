import json
import asyncio
import base64
import random
from queue import Queue
from threading import Thread
from flask import Blueprint, Response, stream_with_context
from playwright.async_api import async_playwright

bp_raw_stream = Blueprint("raw_stream", __name__, url_prefix="/api")

# Randomized human-like delay
async def human_delay(min_ms=800, max_ms=2500):
    await asyncio.sleep(random.uniform(min_ms, max_ms) / 1000)

def run_playwright_scraper(match_id, q):
    async def scrape():
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
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
            )

            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800},
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
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "Upgrade-Insecure-Requests": "1",
                }
            )

            # --- STEALTH SCRIPTS ---
            # Injected on every page before any script runs
            await context.add_init_script("""
                // Overwrite the `navigator.webdriver` property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });

                // Spoof plugins to look like a real browser
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });

                // Spoof language
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });

                // Remove `window.chrome` absence (headless has no chrome object)
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                };

                // Fix permission query spoofing
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) =>
                    parameters.name === 'notifications'
                        ? Promise.resolve({ state: Notification.permission })
                        : originalQuery(parameters);

                // Mask headless via screen properties
                Object.defineProperty(screen, 'availHeight', { get: () => 800 });
                Object.defineProperty(screen, 'availWidth', { get: () => 1280 });

                // Spoof WebGL vendor/renderer (fingerprinting vector)
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                    if (parameter === 37445) return 'Intel Inc.';
                    if (parameter === 37446) return 'Intel Iris OpenGL Engine';
                    return getParameter.call(this, parameter);
                };

                // Prevent automation detection via timing
                const originalDateNow = Date.now;
                Date.now = () => originalDateNow() + Math.floor(Math.random() * 10);
            """)

            page = await context.new_page()

            # Mask extra Playwright-specific properties
            await page.add_init_script("""
                delete window.__playwright;
                delete window.__pw_manual;
                delete window.playwright;
            """)

            is_scraping = True

            # --- SCREENSHOT STREAMING TASK ---
            async def capture_screenshots():
                while is_scraping:
                    try:
                        screenshot_bytes = await page.screenshot(type="jpeg", quality=30)
                        b64_img = base64.b64encode(screenshot_bytes).decode('utf-8')
                        q.put({"type": "screenshot", "data": b64_img})
                        await asyncio.sleep(0.5)
                    except Exception:
                        await asyncio.sleep(0.5)

            screenshot_task = asyncio.create_task(capture_screenshots())

            # --- JSON INTERCEPTION ---
            async def handle_response(response):
                if "fn.sportradar.com" in response.url and "gismo" in response.url:
                    if response.request.method != "OPTIONS":
                        try:
                            data = await response.json()
                            url_parts = response.url.split('?')[0].split('/')
                            endpoint_key = f"{url_parts[-2]}_{url_parts[-1]}"
                            q.put({
                                "type": "intercept",
                                "endpoint": endpoint_key,
                                "url": response.url,
                                "payload": data
                            })
                        except Exception:
                            pass

            page.on("response", handle_response)

            # --- HUMAN-LIKE MOUSE MOVEMENT ---
            async def move_mouse_randomly():
                for _ in range(random.randint(2, 5)):
                    x = random.randint(100, 1180)
                    y = random.randint(100, 700)
                    await page.mouse.move(x, y, steps=random.randint(5, 15))
                    await asyncio.sleep(random.uniform(0.1, 0.4))

            # --- NAVIGATION & CLICKING LOGIC ---
            try:
                base_url = f"https://statshub.sportradar.com/sportpesa/en/match/{match_id}"
                await page.goto(base_url, wait_until="networkidle")

                # Simulate human reading/looking at the page
                await move_mouse_randomly()
                await human_delay(2000, 4000)

                tabs = [
                    f"a[href='/sportpesa/en/match/{match_id}/report']",
                    f"a[href='/sportpesa/en/match/{match_id}/statistics']"
                ]

                for selector in tabs:
                    element = page.locator(selector).first
                    if await element.count() > 0:
                        # Scroll element into view naturally
                        await element.scroll_into_view_if_needed()
                        await human_delay(400, 900)

                        # Move mouse to element before clicking
                        box = await element.bounding_box()
                        if box:
                            await page.mouse.move(
                                box['x'] + box['width'] / 2 + random.uniform(-5, 5),
                                box['y'] + box['height'] / 2 + random.uniform(-5, 5),
                                steps=random.randint(8, 20)
                            )
                            await human_delay(100, 300)

                        await element.click()
                        await move_mouse_randomly()
                        await human_delay(2500, 4500)

            except Exception as e:
                q.put({"type": "error", "message": str(e)})

            is_scraping = False
            await screenshot_task
            await browser.close()
            q.put({"done": True})

    asyncio.run(scrape())


@bp_raw_stream.route("/odds/match/<match_id>/raw_stream")
def stream_raw_data(match_id):
    q = Queue()
    Thread(target=run_playwright_scraper, args=(match_id, q)).start()

    def generate():
        while True:
            item = q.get()
            if "done" in item:
                yield "event: done\ndata: {}\n\n"
                break
            if item.get("type") == "screenshot":
                yield f"event: screenshot\ndata: {json.dumps({'data': item['data']})}\n\n"
            elif item.get("type") == "intercept":
                yield f"event: intercept\ndata: {json.dumps(item)}\n\n"
            elif item.get("type") == "error":
                yield f"event: error\ndata: {json.dumps(item)}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )