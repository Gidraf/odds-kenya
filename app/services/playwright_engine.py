"""
Playwright Discovery Engine  v7  — Gemini Vision
==================================================
Every navigation decision is made by Gemini 2.5 Pro looking at the REAL
rendered screenshot — not HTML text. This mirrors how browser-use works.

FLOW:
  1. Load homepage → wait for full SPA render (networkidle + settle)
  2. Take viewport screenshot
  3. Phase 0: Gemini vision finds + dismisses overlays/banners/age-gates
  4. Take clean screenshot, drain homepage network captures
  5. Click loop:
       a. Take viewport screenshot
       b. Gemini vision plans next clicks (coords + selector fallback)
       c. Click: try pixel coords first, fallback to CSS/text selector
       d. Take screenshot after each successful click
       e. Drain + classify + save network captures
       f. On empty plan → scroll → retry → fallback href strategy
  6. WebSocket frame analysis
  7. Save endpoints, trigger harvest, emit screenshots to frontend

Screenshots:
  app/static/screenshots/bookmaker_<id>/
    01_homepage_before_<slug>.png
    02_homepage_after_<slug>.png
    03_click_<label>.png  ...

Each screenshot also emitted live via SocketIO 'screenshot' event.

Requirements:
  pip install google-genai playwright
"""

import json
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

from app.extensions import db
from app.models.bookmakers_model import Bookmaker, BookmakerEndpoint
from app.services.emmiter import emit_log
from app.services.ai_navigator import (
    _gemini_client,
    _ai_dismiss_overlays,
    _ai_plan_clicks,
    _ai_plan_clicks_fallback,
    _ai_classify_request,
    _ai_generate_parser,
    _ai_summarise_websocket,
    _ai_resolve_url_ids,
    CaptureBuffer, SessionState,
    OBSERVE_SECONDS, MAX_CLICKS,
    make_capture_handlers,
    build_url_pattern,
    is_noise_url,
    extract_bookmaker_name,
    _build_curl,
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

_SPA_SETTLE_SECS    = 2
_MAX_OVERLAY_ROUNDS = 2
_CLICK_SETTLE_SECS  = 0.6   # Wait after click before screenshot

_SCREENSHOT_ROOT = os.environ.get(
    "SCREENSHOT_DIR",
    os.path.join(os.path.dirname(__file__), "..", "static", "screenshots"),
)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _log(bk_id: int, level: str, msg: str, **extra):
    emit_log(bk_id, level, msg, **extra)


def _screenshot_dir(bookmaker_id: int) -> Path:
    d = Path(_SCREENSHOT_ROOT) / f"bookmaker_{bookmaker_id}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _slug(text: str, max_len: int = 40) -> str:
    return (re.sub(r"[^a-zA-Z0-9]+", "_", text.lower()).strip("_") or "action")[:max_len]


def _take_screenshot(page, bookmaker_id: int,
                     step: str, label: str) -> tuple[str | None, bytes | None]:
    """
    Take a viewport screenshot. Saves to disk and emits SocketIO event.
    Returns (web_path, raw_bytes). Both None on failure.
    raw_bytes are passed directly to Gemini vision for the next planning call.
    """
    try:
        folder   = _screenshot_dir(bookmaker_id)
        filename = f"{step}_{_slug(label)}.png"
        filepath = folder / filename

        # full_page=False → viewport only, so Gemini coords match click coords
        raw_bytes = page.screenshot(
            path=str(filepath), full_page=False, type="png", timeout=8000
        )
        # Playwright returns None for `path=` calls; read the file back
        if raw_bytes is None:
            raw_bytes = filepath.read_bytes()

        rel_path = f"/static/screenshots/bookmaker_{bookmaker_id}/{filename}"
        emit_log(
            bookmaker_id, "SCREENSHOT",
            f"  📸 {filename}",
            screenshot_path=rel_path, step=step, label=label,
        )
        return rel_path, raw_bytes

    except Exception as e:
        emit_log(bookmaker_id, "WARN",
                 f"  📸 Screenshot failed [{label}]: {str(e)[:60]}")
        return None, None


def _viewport_bytes(page) -> bytes | None:
    """Quick viewport screenshot returning only bytes (no disk save)."""
    try:
        return page.screenshot(full_page=False, type="png", timeout=6000)
    except Exception:
        return None


def _html_snapshot(page) -> tuple[str, str]:
    """
    Supplementary HTML snapshot — hoists nav/menu elements to the front so
    Gemini has accurate selector context even on hero-heavy pages.
    """
    try:
        title = page.title() or ""
    except Exception:
        title = ""

    try:
        html = page.evaluate("""
            () => {
                const clone = document.documentElement.cloneNode(true);
                clone.querySelectorAll(
                    'script, style, noscript, svg, iframe, img, ' +
                    'link[rel="stylesheet"], meta, head'
                ).forEach(e => e.remove());

                const priority = [];
                clone.querySelectorAll(
                    'nav, header, [class*="nav"], [class*="menu"], ' +
                    '[class*="sport"], [class*="sidebar"], [class*="tab"], ' +
                    '[role="navigation"], [role="menubar"]'
                ).forEach(el => {
                    const t = el.outerHTML;
                    if (t.length < 8000) priority.push(t);
                });

                const rest = clone.body ? clone.body.outerHTML : clone.outerHTML;
                return (priority.join('\\n') + '\\n' + rest).slice(0, 20000);
            }
        """)
    except Exception:
        try:
            html = page.content()[:20000]
        except Exception:
            html = ""

    return html, title


def _wait_for_render(page, bk_id: int):
    """Wait for SPA to hydrate: networkidle preferred, load+settle fallback."""
    try:
        page.wait_for_load_state("networkidle", timeout=10000)
        _log(bk_id, "INFO", "  ✅ Network idle")
    except Exception:
        try:
            page.wait_for_load_state("load", timeout=8000)
        except Exception:
            pass
        _log(bk_id, "INFO", "  ⏳ networkidle timeout — settled with load+wait")
    time.sleep(_SPA_SETTLE_SECS)


def _safe_click(page, plan, bk_id: int) -> bool:
    """
    Click using Gemini-supplied pixel coordinates first (most reliable),
    falling back to CSS selector, then text= locator.
    """
    label    = plan.label
    selector = plan.selector or ""
    x, y     = plan.x, plan.y

    # ── 1. Coordinate click (from Gemini vision) ──────────────────────────────
    if x is not None and y is not None:
        try:
            page.mouse.move(x, y)
            page.mouse.click(x, y)
            _log(bk_id, "INFO", f"  👆 Clicked @({x},{y}): {label}")
            return True
        except Exception as e:
            _log(bk_id, "WARN",
                 f"  ⚠️ Coord click ({x},{y}) failed, trying selector: {str(e)[:50]}")

    # ── 2. Selector / text= fallback ─────────────────────────────────────────
    if not selector:
        _log(bk_id, "WARN", f"  ✗ '{label}': no selector or coords")
        return False

    try:
        if selector.startswith("text="):
            loc = page.get_by_text(selector[5:], exact=False).first
        else:
            loc = page.locator(selector).first

        if loc.count() == 0:
            _log(bk_id, "WARN", f"  ✗ '{label}': selector not found ({selector})")
            return False

        loc.scroll_into_view_if_needed(timeout=3000)
        loc.click(timeout=5000)
        _log(bk_id, "INFO", f"  👆 Clicked (selector): {label}")
        return True

    except Exception as e:
        _log(bk_id, "WARN", f"  ✗ '{label}': {str(e)[:80]}")
        return False


def _dismiss_overlays(page, bk_id: int,
                       screenshot_bytes: bytes | None,
                       html: str, base_url: str) -> int:
    """Phase 0: Vision-based overlay dismissal. Returns # dismissed."""
    if _gemini_client is None:
        return 0

    dismissed = 0
    for attempt in range(_MAX_OVERLAY_ROUNDS):
        plans = _ai_dismiss_overlays(
            _gemini_client, screenshot_bytes, html, base_url
        )
        if not plans:
            break

        _log(bk_id, "INFO",
             f"  🪟 Overlay round {attempt+1}: {len(plans)} dismissal(s) planned")

        for plan in plans:
            if _safe_click(page, plan, bk_id):
                dismissed += 1
                time.sleep(0.8)

        if dismissed == 0:
            break

        # Refresh screenshot + HTML after dismissal
        screenshot_bytes = _viewport_bytes(page)
        html, _ = _html_snapshot(page)

    if dismissed:
        _log(bk_id, "INFO", f"  ✅ Dismissed {dismissed} overlay(s)")
    return dismissed


def _process_and_save(captured, bk_id: int, counter: list, _app):
    """Classify → generate parsers → upsert to DB."""
    if not captured or _gemini_client is None:
        return

    clean       = [r for r in captured if not is_noise_url(r.url, r.request_type)]
    noise_count = len(captured) - len(clean)

    if noise_count:
        _log(bk_id, "NET",
             f"  📦 {len(clean)} to classify ({noise_count} noise skipped)")
    else:
        _log(bk_id, "NET", f"  📦 Classifying {len(captured)} requests")

    client = _gemini_client

    for req in clean:
        is_sports, category, notes = _ai_classify_request(client, req)
        if not is_sports:
            continue

        _log(bk_id, "CAPTURE",
             f"  📡 [{category}] {req.url[:80]} ({req.resp_size}b)")

        if not req.url_pattern:
            req.url_pattern, req.id_schema = build_url_pattern(req.url)

        if req.id_schema and req.resp_sample:
            req.id_schema = _ai_resolve_url_ids(
                client, req.url, req.id_schema, req.resp_sample
            )
            if req.id_schema:
                _log(bk_id, "AI",
                     f"  🔑 ID schema: {list(req.id_schema.keys())}")

        parser_code = None
        if req.request_type == "json" and req.resp_sample:
            parser_code = _ai_generate_parser(
                client, req.url, req.resp_sample, category
            )
            _log(bk_id, "AI", f"  🤖 Parser generated: {req.url[:60]}")

        pattern        = req.url_pattern
        id_schema_json = json.dumps(req.id_schema) if req.id_schema else None

        with _app.app_context():
            try:
                existing = BookmakerEndpoint.query.filter_by(
                    bookmaker_id=bk_id, url_pattern=pattern[:499]
                ).first()

                if existing:
                    existing.headers_json  = req.req_headers
                    existing.parser_code   = parser_code or existing.parser_code
                    existing.endpoint_type = category
                    existing.curl_command  = req.curl
                    existing.is_active     = False
                    db.session.commit()
                    _log(bk_id, "SUCCESS",
                         f"  ♻️  Updated [{category}]: {pattern[:65]}")
                else:
                    ep = BookmakerEndpoint(
                        bookmaker_id=bk_id,
                        endpoint_type=category,
                        url_pattern=pattern[:499],
                        request_method=req.method,
                        headers_json=req.req_headers,
                        curl_command=req.curl,
                        parser_code=parser_code,
                        sample_response=(
                            req.resp_sample or id_schema_json or ""
                        )[:2000],
                        parser_test_passed=False,
                        is_active=False,
                    )
                    db.session.add(ep)
                    db.session.commit()
                    counter[0] += 1
                    _log(bk_id, "SUCCESS",
                         f"  ✅ #{counter[0]} [{category}]: {pattern[:65]}")

            except Exception as e:
                db.session.rollback()
                _log(bk_id, "ERROR", f"  DB error: {e}")


def _new_context(p, storage_state=None):
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--disable-notifications",
            "--disable-push-messaging-enforcement",
             "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-dev-shm-usage",   # ← add this for server environments
        f"--remote-debugging-port={5900}",
        "--remote-debugging-address=0.0.0.0",
        ],
    )
    ctx = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1920, "height": 1080},
        storage_state=storage_state,
        permissions=[],
    )
    ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        window.Notification = {
            permission: 'denied',
            requestPermission: async () => 'denied'
        };
    """)
    return browser, ctx


# ─────────────────────────────────────────────────────────────────────────────
# MAIN DISCOVERY ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def discover_bookmaker(bookmaker_id: int, domain: str):
    from flask import current_app
    _app = current_app._get_current_object()

    with _app.app_context():
        bookmaker = db.session.get(Bookmaker, bookmaker_id)
        if not bookmaker:
            return
        stored_name = bookmaker.name or ""
        if (not stored_name or stored_name.startswith("www.") or
                ("." in stored_name and " " not in stored_name)):
            bookmaker.name = extract_bookmaker_name(domain)
            db.session.commit()
            _log(bookmaker_id, "INFO", f"📛 Name: {bookmaker.name}")

    clean    = domain.strip().lower()
    base_url = clean if clean.startswith("http") else f"https://{clean}"

    if _gemini_client is None:
        _log(bookmaker_id, "ERROR",
             "Gemini client not available — set GEMINI_API_KEY and restart")
        return

    _log(bookmaker_id, "INFO", f"🚀 Discovery v7 (Vision): {base_url}")
    _log(bookmaker_id, "AI",
         "🤖 Gemini 2.5 Pro vision navigating autonomously. "
         f"Screenshots guide every click decision. {OBSERVE_SECONDS}s window.")

    counter          = [0]
    buffer           = CaptureBuffer()
    ws_frames        = {}
    ws_lock          = threading.Lock()
    seen_urls        = set()
    seen_lock        = threading.Lock()
    state            = SessionState(url=base_url)
    all_screenshots: list[dict] = []
    step_num         = [1]

    def _ss(page, prefix: str, label: str) -> bytes | None:
        """Screenshot → disk + SocketIO. Returns raw bytes for Gemini."""
        step = f"{step_num[0]:02d}_{prefix}"
        step_num[0] += 1
        path, raw = _take_screenshot(page, bookmaker_id, step, label)
        if path:
            all_screenshots.append({"step": step, "label": label, "path": path})
        return raw

    with sync_playwright() as p:
        browser, ctx = _new_context(p)

        action_label_ref = ["DISCOVERY:INIT"]
        on_response, on_websocket = make_capture_handlers(
            buffer, action_label_ref, ws_frames, ws_lock, seen_urls, seen_lock
        )
        ctx.on("response",  on_response)
        ctx.on("websocket", on_websocket)

        page = ctx.new_page()

        # ── Load homepage ─────────────────────────────────────────────────────
        _log(bookmaker_id, "INFO", "━━━ Loading homepage ━━━")
        try:
            page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
        except Exception as e:
            _log(bookmaker_id, "WARN", f"goto: {str(e)[:80]}")

        _wait_for_render(page, bookmaker_id)

        # ── Screenshot 1: raw homepage ────────────────────────────────────────
        ss_bytes = _ss(page, "homepage_before", "Homepage before overlays")
        html, title = _html_snapshot(page)

        # Capture brand colour
        try:
            bg = page.evaluate(
                "window.getComputedStyle(document.body).backgroundColor"
            )
            with _app.app_context():
                bm = db.session.get(Bookmaker, bookmaker_id)
                if bm:
                    bm.brand_color = bg
                    db.session.commit()
        except Exception:
            pass

        # ── Phase 0: Vision overlay dismissal ────────────────────────────────
        _log(bookmaker_id, "INFO", "━━━ Phase 0: Overlay dismissal ━━━")
        dismissed = _dismiss_overlays(
            page, bookmaker_id, ss_bytes, html, base_url
        )

        if dismissed:
            time.sleep(1)
            ss_bytes = _ss(page, "homepage_after", "Homepage after overlays")
            html, title = _html_snapshot(page)
        else:
            _log(bookmaker_id, "INFO", "  ℹ️  No overlays")

        # ── Drain homepage captures ───────────────────────────────────────────
        _log(bookmaker_id, "INFO", "⏱ Draining homepage captures...")
        initial = buffer.drain_after(OBSERVE_SECONDS)
        _process_and_save(initial, bookmaker_id, counter, _app)

        # ── AI Vision click loop ──────────────────────────────────────────────
        click_count      = 0
        click_history    = []
        visited_urls     = {base_url}
        empty_plan_count = 0

        while click_count < MAX_CLICKS:
            current_url = page.url

            # Fresh screenshot + HTML for this planning round
            ss_bytes    = _viewport_bytes(page)
            html, title = _html_snapshot(page)

            if not html and not ss_bytes:
                _log(bookmaker_id, "WARN", "Empty page — stopping")
                break

            _log(bookmaker_id, "INFO",
                 f"🤖 Gemini Vision planning [{click_count}/{MAX_CLICKS}] "
                 f"— {current_url[:70]}")

            click_plan = _ai_plan_clicks(
                _gemini_client, ss_bytes, html, current_url,
                click_history, title,
            )

            # ── Empty plan recovery ───────────────────────────────────────────
            if not click_plan:
                empty_plan_count += 1
                _log(bookmaker_id, "WARN",
                     f"  ⚠️  Empty plan (attempt {empty_plan_count}/2)")

                if empty_plan_count == 1:
                    _log(bookmaker_id, "INFO",
                         "  🔄 Scrolling to trigger lazy content...")
                    try:
                        page.evaluate(
                            "window.scrollTo(0, document.body.scrollHeight / 2)"
                        )
                        time.sleep(2)
                        page.evaluate("window.scrollTo(0, 0)")
                        time.sleep(1)
                    except Exception:
                        pass
                    ss_bytes    = _viewport_bytes(page)
                    html, title = _html_snapshot(page)
                    click_plan  = _ai_plan_clicks(
                        _gemini_client, ss_bytes, html, current_url,
                        click_history, title,
                    )

                if not click_plan:
                    _log(bookmaker_id, "INFO",
                         "  🔄 Fallback href strategy...")
                    click_plan = _ai_plan_clicks_fallback(
                        _gemini_client, ss_bytes, html, current_url
                    )

                if not click_plan:
                    _log(bookmaker_id, "INFO",
                         "  ⛔ No actions after retries — done")
                    break

                empty_plan_count = 0

            _log(bookmaker_id, "INFO",
                 f"  📋 {len(click_plan)} action(s) planned")
            any_clicked = False

            for plan in click_plan:
                if click_count >= MAX_CLICKS:
                    break
                if plan.label in click_history:
                    continue

                action_label_ref[0] = f"DISCOVER:{plan.label}"
                clicked = _safe_click(page, plan, bookmaker_id)
                click_count += 1
                click_history.append(plan.label)
                state.click_history.append(plan.label)

                if clicked:
                    any_clicked = True
                    time.sleep(_CLICK_SETTLE_SECS)

                    # Screenshot after click (used by Gemini next round)
                    _ss(page, "click", plan.label)

                    _log(bookmaker_id, "NET",
                         f"  ⏱ Observing {OBSERVE_SECONDS}s after: {plan.label}")
                    captured = buffer.drain_after(OBSERVE_SECONDS)
                    _process_and_save(captured, bookmaker_id, counter, _app)

                    new_url = page.url
                    if new_url != current_url and new_url not in visited_urls:
                        visited_urls.add(new_url)
                        _log(bookmaker_id, "INFO",
                             f"  🔀 Navigated: {new_url[:70]}")
                        _wait_for_render(page, bookmaker_id)
                        _ss(page, "nav", f"After nav {new_url[:40]}")
                        nav_captured = buffer.drain_after(OBSERVE_SECONDS)
                        _process_and_save(nav_captured, bookmaker_id, counter, _app)
                        break
                else:
                    buffer.drain_after(0.5)

            if not any_clicked:
                empty_plan_count += 1
                _log(bookmaker_id, "INFO",
                     f"  No clicks landed (consecutive: {empty_plan_count})")
                if empty_plan_count >= 3:
                    _log(bookmaker_id, "INFO",
                         "  3 consecutive empty rounds — done")
                    break

        # ── WebSocket analysis ─────────────────────────────────────────────────
        if ws_frames:
            _log(bookmaker_id, "AI",
                 f"🔌 Analysing {len(ws_frames)} WebSocket stream(s)...")
            for ws_url, frames in ws_frames.items():
                if frames:
                    analysis = _ai_summarise_websocket(
                        _gemini_client, frames, ws_url
                    )
                    _log(bookmaker_id, "CAPTURE",
                         f"  🔌 {ws_url[:60]}: "
                         f"{analysis.get('purpose','?')} | "
                         f"odds={analysis.get('odds_present')} | "
                         f"{analysis.get('update_frequency_ms')}ms")

        try:
            page.close()
            ctx.close()
            browser.close()
        except Exception:
            pass

    # ── Finalise ──────────────────────────────────────────────────────────────
    with _app.app_context():
        bm = db.session.get(Bookmaker, bookmaker_id)
        if bm:
            bm.needs_ui_intervention = False
            bm.last_discovery_at     = datetime.now(timezone.utc)
            homepage_ss = next(
                (s["path"] for s in all_screenshots if "homepage" in s["step"]),
                None,
            )
            if homepage_ss and hasattr(bm, "screenshot_url"):
                bm.screenshot_url = homepage_ss
            db.session.commit()

    _log(bookmaker_id, "SUCCESS",
         f"🏁 Discovery complete — {counter[0]} endpoints, "
         f"{len(all_screenshots)} screenshots",
         endpoints_discovered=counter[0],
         screenshot_count=len(all_screenshots))

    if counter[0] > 0:
        _log(bookmaker_id, "INFO",
             f"⚡ Triggering harvest for {counter[0]} endpoints...")
        try:
            from app.services.agents_tasks import harvest_data
            harvest_data.apply_async(queue="harvest")
        except Exception as he:
            _log(bookmaker_id, "WARN", f"Harvest trigger failed: {he}")

    from app.services.emmiter import get_emitter
    get_emitter().emit("discovery_complete", {
        "bookmaker_id":    bookmaker_id,
        "domain":          domain,
        "endpoints_found": counter[0],
        "ws_streams":      len(ws_frames),
        "actions_taken":   len(click_history),
        "screenshots":     all_screenshots,
        "msg": (
            f"Discovery complete. {counter[0]} endpoints, "
            f"{len(all_screenshots)} screenshots."
        ),
    }, namespace="/admin")