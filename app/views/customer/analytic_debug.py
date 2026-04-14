import json
import asyncio
from queue import Queue
from threading import Thread
from flask import Blueprint, Response, stream_with_context
from playwright.async_api import async_playwright

bp_raw_stream = Blueprint("raw_stream", __name__, url_prefix="/api")

def run_playwright_scraper(match_id, q):
    """Runs Playwright in a new event loop inside a background thread."""
    async def scrape():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Mobile Safari/537.36"
            )
            page = await context.new_page()

            async def handle_response(response):
                if "fn.sportradar.com" in response.url and "gismo" in response.url:
                    if response.request.method != "OPTIONS":
                        try:
                            data = await response.json()
                            url_parts = response.url.split('?')[0].split('/')
                            endpoint_key = f"{url_parts[-2]}_{url_parts[-1]}"
                            
                            # Instantly stream the intercepted data to the frontend
                            q.put({
                                "endpoint": endpoint_key,
                                "url": response.url,
                                "payload": data
                            })
                        except Exception:
                            pass

            page.on("response", handle_response)

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

            await browser.close()
            # Send completion signal
            q.put({"done": True})

    # Start the async loop for this thread
    asyncio.run(scrape())

@bp_raw_stream.route("/odds/match/<match_id>/raw_stream")
def stream_raw_data(match_id):
    q = Queue()
    
    # Start Playwright in a background thread so it doesn't block Flask
    Thread(target=run_playwright_scraper, args=(match_id, q)).start()

    def generate():
        while True:
            item = q.get()
            if "done" in item:
                yield "event: done\ndata: {}\n\n"
                break
            
            # Yield the JSON payload as an SSE event named 'intercept'
            yield f"event: intercept\ndata: {json.dumps(item)}\n\n"

    return Response(
        stream_with_context(generate()), 
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )