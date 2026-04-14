import json
import asyncio
import base64
from queue import Queue
from threading import Thread
from flask import Blueprint, Response, stream_with_context
from playwright.async_api import async_playwright

bp_raw_stream = Blueprint("raw_stream", __name__, url_prefix="/api")

def run_playwright_scraper(match_id, q):
    async def scrape():
        async with async_playwright() as p:
            # Running headless, but we will "see" it via the stream
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1280, 'height': 800}, # Set a fixed viewport for consistent screenshots
                user_agent="Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
            )
            page = await context.new_page()

            # Flag to control the screenshot loop
            is_scraping = True

            # --- SCREENSHOT STREAMING TASK ---
            async def capture_screenshots():
                while is_scraping:
                    try:
                        # Use low-quality JPEG to prevent SSE payload limits and lag
                        screenshot_bytes = await page.screenshot(type="jpeg", quality=30)
                        b64_img = base64.b64encode(screenshot_bytes).decode('utf-8')
                        q.put({"type": "screenshot", "data": b64_img})
                        await asyncio.sleep(0.5) # Capture ~2 frames per second
                    except Exception:
                        await asyncio.sleep(0.5)
            
            # Start the screenshot loop in the background
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

            # --- NAVIGATION & CLICKING LOGIC ---
            try:
                base_url = f"https://statshub.sportradar.com/sportpesa/en/match/{match_id}"
                await page.goto(base_url, wait_until="networkidle")
                await page.wait_for_timeout(3000)

                tabs = [
                    f"a[href='/sportpesa/en/match/{match_id}/report']",
                    f"a[href='/sportpesa/en/match/{match_id}/statistics']"
                ]

                for selector in tabs:
                    element = page.locator(selector).first
                    if await element.count() > 0:
                        await element.click()
                        await page.wait_for_timeout(3000)
            except Exception as e:
                # If something fails, stream the error out so you can see it
                q.put({"type": "error", "message": str(e)})

            # Clean up
            is_scraping = False # Stop the screenshot loop
            await screenshot_task # Wait for it to finish its current frame
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
            
            # Route the data to the correct frontend event listener
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