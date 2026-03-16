"""
Research Engine  v5  — Gemini Vision
======================================
All page-analysis decisions (what to click, overlays, login forms) use
Gemini 2.5 Pro vision on the real viewport screenshot.

PHASE 1 — UNAUTHENTICATED
  - Load page → networkidle wait
  - Screenshot → Gemini vision finds login form
  - Screenshot → Gemini vision dismisses overlays
  - Vision click loop with pixel-coord clicking
  - All captures classified + parsers generated
  - MD report → ResearchSession.report_md

PHASE 2 — LOGIN REQUEST

PHASE 3 — AUTHENTICATED
  - Same vision loop with auth cookies

PHASE 4 — REPORT + SocketIO notification

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

from playwright.sync_api import sync_playwright, BrowserContext, Page

from app.extensions import db
from app.models.bookmakers_model import Bookmaker, BookmakerEndpoint
from app.models.research_model import ResearchSession, ResearchFinding, ResearchEndpoint
from app.services.emmiter import emit_log
from app.services.ai_navigator import (
    _gemini_client,
    _ai_dismiss_overlays,
    _ai_plan_clicks,
    _ai_plan_clicks_fallback,
    _ai_find_login_form,
    _ai_classify_request,
    _ai_generate_parser,
    _ai_summarise_websocket,
    _ai_resolve_url_ids,
    CaptureBuffer, SessionState, CapturedRequest,
    OBSERVE_SECONDS, MAX_CLICKS,
    make_capture_handlers,
    build_markdown_report,
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
_CLICK_SETTLE_SECS  = 0.6

_SCREENSHOT_ROOT = os.environ.get(
    "SCREENSHOT_DIR",
    os.path.join(os.path.dirname(__file__), "..", "static", "screenshots"),
)


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def _log(session_id: int, bk_id: int, level: str, msg: str, **extra):
    emit_log(bk_id, level, f"[R#{session_id}] {msg}", **extra)


# ─────────────────────────────────────────────────────────────────────────────
# SCREENSHOT HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _screenshot_dir(bookmaker_id: int) -> Path:
    d = Path(_SCREENSHOT_ROOT) / f"bookmaker_{bookmaker_id}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _slug(text: str, max_len: int = 40) -> str:
    return (re.sub(r"[^a-zA-Z0-9]+", "_", text.lower()).strip("_") or "action")[:max_len]


def _take_screenshot(page: Page, session_id: int, bookmaker_id: int,
                     step: str, label: str) -> tuple[str | None, bytes | None]:
    """
    Viewport screenshot → disk + SocketIO event.
    Returns (web_path, raw_bytes). raw_bytes passed to Gemini vision.
    """
    try:
        folder   = _screenshot_dir(bookmaker_id)
        filename = f"{step}_{_slug(label)}.png"
        filepath = folder / filename

        raw_bytes = page.screenshot(
            path=str(filepath), full_page=False, type="png", timeout=8000
        )
        if raw_bytes is None:
            raw_bytes = filepath.read_bytes()

        rel_path = f"/static/screenshots/bookmaker_{bookmaker_id}/{filename}"
        emit_log(
            bookmaker_id, "SCREENSHOT",
            f"[R#{session_id}]   📸 {filename}",
            screenshot_path=rel_path, step=step, label=label,
        )
        return rel_path, raw_bytes

    except Exception as e:
        emit_log(bookmaker_id, "WARN",
                 f"[R#{session_id}]   📸 Screenshot failed [{label}]: {str(e)[:60]}")
        return None, None


def _viewport_bytes(page: Page) -> bytes | None:
    """Quick viewport screenshot returning only raw bytes for Gemini."""
    try:
        return page.screenshot(full_page=False, type="png", timeout=6000)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER FACTORY
# ─────────────────────────────────────────────────────────────────────────────

def _new_context(p, storage_state=None):
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--disable-notifications",
            "--disable-push-messaging-enforcement",
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
# PAGE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _html_snapshot(page: Page) -> tuple[str, str]:
    """Supplementary HTML — hoists nav/menu to front of character budget."""
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


def _wait_for_render(page: Page, session_id: int, bk_id: int):
    try:
        page.wait_for_load_state("networkidle", timeout=10000)
        _log(session_id, bk_id, "INFO", "  ✅ Network idle")
    except Exception:
        try:
            page.wait_for_load_state("load", timeout=8000)
        except Exception:
            pass
        _log(session_id, bk_id, "INFO", "  ⏳ load + settle")
    time.sleep(_SPA_SETTLE_SECS)


def _safe_click(page: Page, plan, session_id: int, bk_id: int) -> bool:
    """Try coordinate click first, then selector fallback."""
    label    = plan.label
    selector = plan.selector or ""
    x, y     = plan.x, plan.y

    # Coordinate click (Gemini vision coords)
    if x is not None and y is not None:
        try:
            page.mouse.move(x, y)
            page.mouse.click(x, y)
            _log(session_id, bk_id, "INFO",
                 f"  👆 Clicked @({x},{y}): {label}")
            return True
        except Exception as e:
            _log(session_id, bk_id, "WARN",
                 f"  ⚠️ Coord fail ({x},{y}), trying selector: {str(e)[:50]}")

    if not selector:
        _log(session_id, bk_id, "WARN", f"  ✗ '{label}': no selector or coords")
        return False

    try:
        if selector.startswith("text="):
            loc = page.get_by_text(selector[5:], exact=False).first
        else:
            loc = page.locator(selector).first

        if loc.count() == 0:
            _log(session_id, bk_id, "WARN",
                 f"  ✗ '{label}': not found ({selector})")
            return False

        loc.scroll_into_view_if_needed(timeout=3000)
        loc.click(timeout=5000)
        _log(session_id, bk_id, "INFO", f"  👆 Clicked (selector): {label}")
        return True

    except Exception as e:
        _log(session_id, bk_id, "WARN", f"  ✗ '{label}': {str(e)[:80]}")
        return False


def _dismiss_overlays(page: Page, session_id: int, bk_id: int,
                       screenshot_bytes: bytes | None,
                       html: str, base_url: str) -> int:
    """Vision-based overlay dismissal. Returns # dismissed."""
    if _gemini_client is None:
        return 0

    dismissed = 0
    for attempt in range(_MAX_OVERLAY_ROUNDS):
        plans = _ai_dismiss_overlays(
            _gemini_client, screenshot_bytes, html, base_url
        )
        if not plans:
            break
        _log(session_id, bk_id, "INFO",
             f"  🪟 Overlay round {attempt+1}: {len(plans)} planned")
        for plan in plans:
            if _safe_click(page, plan, session_id, bk_id):
                dismissed += 1
                time.sleep(0.8)
        if dismissed == 0:
            break
        screenshot_bytes = _viewport_bytes(page)
        html, _ = _html_snapshot(page)

    if dismissed:
        _log(session_id, bk_id, "INFO", f"  ✅ Dismissed {dismissed} overlay(s)")
    return dismissed


# ─────────────────────────────────────────────────────────────────────────────
# AI-DRIVEN EXPLORATION LOOP
# ─────────────────────────────────────────────────────────────────────────────

def _run_ai_exploration(
    ctx: BrowserContext,
    base_url: str,
    session_id: int,
    bk_id: int,
    is_authenticated: bool,
    state: SessionState,
    buffer: CaptureBuffer,
    ws_frames: dict,
    ws_lock: threading.Lock,
    all_screenshots: list,
    _app,
) -> None:
    if _gemini_client is None:
        _log(session_id, bk_id, "ERROR",
             "Gemini client not available — check GEMINI_API_KEY")
        return

    client = _gemini_client
    phase  = "AUTH" if is_authenticated else "UNAUTH"

    step_num = [1]

    def _ss(page: Page, prefix: str, label: str) -> bytes | None:
        step = f"{step_num[0]:02d}_{prefix}"
        step_num[0] += 1
        path, raw = _take_screenshot(page, session_id, bk_id, step, label)
        if path:
            all_screenshots.append({"step": step, "label": label, "path": path})
        return raw

    page = ctx.new_page()
    state.click_history.append(f"[{phase}] Navigate to: {base_url}")

    try:
        page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
    except Exception as e:
        _log(session_id, bk_id, "WARN", f"goto: {str(e)[:80]}")

    _wait_for_render(page, session_id, bk_id)

    # ── Screenshot 1: homepage before overlays ────────────────────────────────
    ss_bytes = _ss(page, f"{phase}_homepage_before", "Homepage before overlays")
    html, title = _html_snapshot(page)

    # ── Phase 0: Vision overlay dismissal ────────────────────────────────────
    _log(session_id, bk_id, "INFO", f"━━ [{phase}] Overlay dismissal ━━")
    dismissed = _dismiss_overlays(
        page, session_id, bk_id, ss_bytes, html, base_url
    )
    if dismissed:
        time.sleep(1)
        ss_bytes = _ss(
            page, f"{phase}_homepage_after", "Homepage after overlays"
        )
        html, title = _html_snapshot(page)
    else:
        _log(session_id, bk_id, "INFO", "  ℹ️  No overlays")

    # ── Drain homepage captures ───────────────────────────────────────────────
    _log(session_id, bk_id, "INFO",
         f"[{phase}] ⏱ Observing homepage {OBSERVE_SECONDS}s...")
    initial = buffer.drain_after(OBSERVE_SECONDS)
    _process_captured(
        initial, client, state, session_id, bk_id,
        f"{phase}:HOMEPAGE", _app,
    )

    # ── Vision click loop ─────────────────────────────────────────────────────
    visited_urls     = {base_url}
    empty_plan_count = 0

    while state.click_count < MAX_CLICKS:
        current_url = page.url
        ss_bytes    = _viewport_bytes(page)
        html, title = _html_snapshot(page)

        if not html and not ss_bytes:
            _log(session_id, bk_id, "WARN", "Empty page — stopping")
            break

        _log(session_id, bk_id, "INFO",
             f"🤖 [{phase}] Vision planning "
             f"[{state.click_count}/{MAX_CLICKS}] — {current_url[:70]}")

        click_plan = _ai_plan_clicks(
            client, ss_bytes, html, current_url,
            state.click_history, title,
        )

        # ── Empty plan recovery ───────────────────────────────────────────────
        if not click_plan:
            empty_plan_count += 1
            _log(session_id, bk_id, "WARN",
                 f"  ⚠️  Empty plan (attempt {empty_plan_count}/2)")

            if empty_plan_count == 1:
                _log(session_id, bk_id, "INFO", "  🔄 Scrolling for lazy content...")
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
                    client, ss_bytes, html, current_url,
                    state.click_history, title,
                )

            if not click_plan:
                _log(session_id, bk_id, "INFO", "  🔄 Fallback href strategy...")
                click_plan = _ai_plan_clicks_fallback(
                    client, ss_bytes, html, current_url
                )

            if not click_plan:
                _log(session_id, bk_id, "INFO",
                     f"  ⛔ No actions — [{phase}] exploration complete")
                break

            empty_plan_count = 0

        _log(session_id, bk_id, "INFO",
             f"  📋 {len(click_plan)} action(s) planned")
        any_clicked = False

        for plan in click_plan:
            if state.click_count >= MAX_CLICKS:
                break
            if plan.label in state.click_history:
                continue

            label = f"{phase}:{plan.label}"
            _log(session_id, bk_id, "INFO",
                 f"  [{state.click_count+1}] {plan.label} — {plan.reason[:60]}")

            clicked = _safe_click(page, plan, session_id, bk_id)
            state.click_count += 1
            state.click_history.append(plan.label)

            if clicked:
                any_clicked = True
                time.sleep(_CLICK_SETTLE_SECS)
                _ss(page, f"{phase}_click", plan.label)

                _log(session_id, bk_id, "NET",
                     f"  ⏱ Observing {OBSERVE_SECONDS}s after: {plan.label}")
                captured = buffer.drain_after(OBSERVE_SECONDS)
                _process_captured(
                    captured, client, state, session_id, bk_id, label, _app
                )

                new_url = page.url
                if new_url != current_url and new_url not in visited_urls:
                    visited_urls.add(new_url)
                    _log(session_id, bk_id, "INFO",
                         f"  🔀 Navigated: {new_url[:70]}")
                    state.click_history.append(f"Navigate to: {new_url}")
                    _wait_for_render(page, session_id, bk_id)
                    _ss(page, f"{phase}_nav", f"After nav {new_url[:40]}")
                    nav_captured = buffer.drain_after(OBSERVE_SECONDS)
                    _process_captured(
                        nav_captured, client, state, session_id, bk_id,
                        f"{phase}:NAV:{new_url[:50]}", _app,
                    )
                    break
            else:
                buffer.drain_after(0.5)

        if not any_clicked:
            empty_plan_count += 1
            _log(session_id, bk_id, "INFO",
                 f"  No clicks landed (consecutive: {empty_plan_count})")
            if empty_plan_count >= 3:
                _log(session_id, bk_id, "INFO",
                     f"  3 consecutive empty rounds — [{phase}] done")
                break

    sports_count = len([r for r in state.requests if r.is_sports])
    _log(session_id, bk_id, "SUCCESS",
         f"[{phase}] Done — {state.click_count} actions, "
         f"{sports_count} sports endpoints, "
         f"{len(all_screenshots)} screenshots")

    try:
        page.close()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# PROCESS CAPTURED REQUESTS
# ─────────────────────────────────────────────────────────────────────────────

def _process_captured(
    captured: list[CapturedRequest],
    client,
    state: SessionState,
    session_id: int,
    bk_id: int,
    action_label: str,
    _app,
):
    if not captured:
        return

    clean       = [r for r in captured if not is_noise_url(r.url, r.request_type)]
    noise_count = len(captured) - len(clean)

    if noise_count:
        _log(session_id, bk_id, "NET",
             f"  📦 {len(clean)} from '{action_label}' ({noise_count} noise)")
    else:
        _log(session_id, bk_id, "NET",
             f"  📦 {len(clean)} from '{action_label}'")

    for req in clean:
        is_sports, category, notes = _ai_classify_request(client, req)
        req.is_sports   = is_sports
        req.ai_category = category
        req.ai_notes    = notes

        if not req.url_pattern:
            req.url_pattern, req.id_schema = build_url_pattern(req.url)

        state.requests.append(req)

        if not is_sports:
            continue

        _log(session_id, bk_id, "CAPTURE",
             f"  📡 [{category}] {req.url[:80]} ({req.resp_size}b)")

        if req.id_schema and req.resp_sample:
            req.id_schema = _ai_resolve_url_ids(
                client, req.url, req.id_schema, req.resp_sample
            )
            if req.id_schema:
                _log(session_id, bk_id, "AI",
                     f"  🔑 ID schema: {list(req.id_schema.keys())}")

        parser_code = None
        if (req.request_type == "json" and req.resp_sample
                and category not in ("AUTH", "STATIC")):
            parser_code = _ai_generate_parser(
                client, req.url, req.resp_sample, category
            )
            _log(session_id, bk_id, "AI",
                 f"  🤖 Parser generated: {req.url[:60]}")

        with _app.app_context():
            _save_research_endpoint_db(req, session_id, bk_id,
                                        parser_code=parser_code)
            if parser_code:
                _promote_endpoint(req, bk_id, parser_code, _app)


def _save_research_endpoint_db(req: CapturedRequest, session_id: int,
                                 bk_id: int, parser_code: str | None = None):
    if is_noise_url(req.url, req.request_type):
        return
    try:
        pattern    = req.url_pattern or build_url_pattern(req.url)[0]
        notes_text = req.ai_notes or ""
        if req.id_schema:
            notes_text += f"\n\nID Schema: {json.dumps(req.id_schema)}"

        ep = ResearchEndpoint(
            session_id=session_id,
            url=req.url[:999],
            url_pattern=pattern[:999],
            method=req.method,
            status_code=req.status,
            content_type=req.request_type,
            endpoint_type=req.ai_category,
            is_authenticated=False,
            requires_auth=(req.status in (401, 403)),
            request_headers=req.req_headers,
            request_body=req.req_body,
            response_sample=(req.resp_sample or "")[:3000],
            curl_command=req.curl,
            notes=notes_text[:2000],
            parser_code=parser_code,
            parser_tested=False,
        )
        db.session.add(ep)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print(f"[DB] save_research_endpoint: {e}")


def _promote_endpoint(req: CapturedRequest, bk_id: int,
                       parser_code: str, _app):
    if is_noise_url(req.url, req.request_type):
        return
    pattern        = req.url_pattern or build_url_pattern(req.url)[0]
    id_schema_json = json.dumps(req.id_schema) if req.id_schema else None

    with _app.app_context():
        try:
            existing = BookmakerEndpoint.query.filter_by(
                bookmaker_id=bk_id, url_pattern=pattern[:499]
            ).first()
            if existing:
                existing.headers_json       = req.req_headers
                existing.parser_code        = parser_code
                existing.endpoint_type      = req.ai_category
                existing.curl_command       = req.curl
                existing.parser_test_passed = False
                existing.is_active          = False
                if id_schema_json and not existing.sample_response:
                    existing.sample_response = id_schema_json[:2000]
                db.session.commit()
            else:
                ep = BookmakerEndpoint(
                    bookmaker_id=bk_id,
                    endpoint_type=req.ai_category,
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
        except Exception as e:
            db.session.rollback()
            print(f"[DB] promote_endpoint: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# WEBSOCKET POST-PROCESSING
# ─────────────────────────────────────────────────────────────────────────────

def _process_websockets(ws_frames: dict, session_id: int,
                         bk_id: int, _app) -> list[dict]:
    if not ws_frames or _gemini_client is None:
        return []

    findings = []
    for ws_url, frames in ws_frames.items():
        if not frames:
            continue
        _log(session_id, bk_id, "INFO",
             f"🔌 Analysing WS: {ws_url[:80]} ({len(frames)} frames)")
        analysis        = _ai_summarise_websocket(_gemini_client, frames, ws_url)
        analysis["url"] = ws_url
        findings.append(analysis)

        with _app.app_context():
            try:
                f = ResearchFinding(
                    session_id=session_id,
                    category="WEBSOCKET",
                    title=f"WS: {ws_url[:80]}",
                    detail=json.dumps(analysis, indent=2),
                    code_snippet="\n".join(frames[:5]),
                    severity="CRITICAL",
                    phase="RESEARCH",
                )
                db.session.add(f)
                db.session.commit()
            except Exception as e:
                db.session.rollback()
                print(f"[DB] ws finding: {e}")

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# REPORT + SESSION CLOSE
# ─────────────────────────────────────────────────────────────────────────────

def _finalise_session(
    session_id: int, bk_id: int, bookmaker_name: str,
    state: SessionState, ws_findings: list[dict],
    base_url: str, all_screenshots: list, _app,
):
    sports_reqs = [r for r in state.requests if r.is_sports]

    parser_map = {}
    with _app.app_context():
        for ep in BookmakerEndpoint.query.filter_by(bookmaker_id=bk_id).all():
            if ep.parser_code:
                parser_map[ep.url_pattern] = ep.parser_code

    report_md = build_markdown_report(
        bookmaker_name=bookmaker_name,
        base_url=base_url,
        sports_requests=sports_reqs,
        all_requests=state.requests,
        ws_findings=ws_findings,
        click_history=state.click_history,
        parser_map=parser_map,
    )

    with _app.app_context():
        sess = db.session.get(ResearchSession, session_id)
        if sess:
            sess.report_md    = report_md
            sess.phase        = "COMPLETE"
            sess.completed_at = datetime.now(timezone.utc)
            db.session.commit()

    _log(session_id, bk_id, "SUCCESS",
         f"📄 Complete — {len(sports_reqs)} endpoints, "
         f"{len(ws_findings)} WS, {len(all_screenshots)} screenshots")

    from app.services.emmiter import get_emitter
    get_emitter().emit("research_complete", {
        "session_id":       session_id,
        "bookmaker_id":     bk_id,
        "bookmaker_name":   bookmaker_name,
        "domain":           base_url,
        "sports_endpoints": len(sports_reqs),
        "ws_streams":       len(ws_findings),
        "total_requests":   len(state.requests),
        "screenshots":      all_screenshots,
        "msg":              "Research complete. Full MD report ready.",
    }, namespace="/admin")


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 — UNAUTHENTICATED
# ─────────────────────────────────────────────────────────────────────────────

def run_unauthenticated_research(bookmaker_id: int, domain: str, session_id: int):
    from flask import current_app
    _app = current_app._get_current_object()

    with _app.app_context():
        bookmaker = db.session.get(Bookmaker, bookmaker_id)
        if not bookmaker:
            return
        stored_name = bookmaker.name or ""
        if (not stored_name or stored_name.startswith("www.") or
                ("." in stored_name and " " not in stored_name)):
            bookmaker_name = extract_bookmaker_name(domain)
            bookmaker.name = bookmaker_name
            db.session.commit()
        else:
            bookmaker_name = stored_name

    clean    = domain.strip().lower()
    base_url = clean if clean.startswith("http") else f"https://{clean}"

    _log(session_id, bookmaker_id, "INFO",
         f"🔬 PHASE 1 (Vision): {base_url}")
    _log(session_id, bookmaker_id, "AI",
         "🤖 Gemini 2.5 Pro Vision navigating. "
         f"{OBSERVE_SECONDS}s window. Screenshots guide every decision.")

    state          = SessionState(url=base_url)
    buffer         = CaptureBuffer()
    ws_frames      = {}
    ws_lock        = threading.Lock()
    seen_urls      = set()
    seen_lock      = threading.Lock()
    auth_info      = None
    all_screenshots: list = []

    with sync_playwright() as p:
        browser, ctx = _new_context(p)

        action_label_ref = ["UNAUTH:INIT"]
        on_response, on_websocket = make_capture_handlers(
            buffer, action_label_ref, ws_frames, ws_lock, seen_urls, seen_lock
        )
        ctx.on("response",  on_response)
        ctx.on("websocket", on_websocket)

        # ── Auth form discovery (Gemini vision on homepage) ───────────────────
        home = ctx.new_page()
        try:
            home.goto(base_url, wait_until="domcontentloaded", timeout=30000)
            _wait_for_render(home, session_id, bookmaker_id)
        except Exception as e:
            _log(session_id, bookmaker_id, "WARN", f"Homepage: {str(e)[:80]}")

        _log(session_id, bookmaker_id, "AI", "━━ Vision auth-form discovery ━━")

        if _gemini_client:
            ss_bytes  = _viewport_bytes(home)
            html, _   = _html_snapshot(home)
            auth_info = _ai_find_login_form(
                _gemini_client, ss_bytes, html, base_url
            )

        if auth_info:
            _log(session_id, bookmaker_id, "AI",
                 f"🔑 Login: fields={[f.get('name') for f in auth_info.get('fields', [])]} "
                 f"OTP={auth_info.get('otp_flow', {}).get('present')}")
            with _app.app_context():
                try:
                    db.session.add(ResearchFinding(
                        session_id=session_id,
                        category="AUTH_FLOW",
                        title=f"Login at {auth_info.get('login_url', base_url)}",
                        detail=json.dumps(auth_info, indent=2),
                        severity="CRITICAL",
                        phase="UNAUTH",
                    ))
                    db.session.commit()
                except Exception:
                    db.session.rollback()
        try:
            home.close()
        except Exception:
            pass

        # ── Main vision exploration ───────────────────────────────────────────
        _log(session_id, bookmaker_id, "AI",
             "━━ Starting Gemini Vision exploration ━━")

        _run_ai_exploration(
            ctx=ctx, base_url=base_url,
            session_id=session_id, bk_id=bookmaker_id,
            is_authenticated=False, state=state, buffer=buffer,
            ws_frames=ws_frames, ws_lock=ws_lock,
            all_screenshots=all_screenshots, _app=_app,
        )

        try:
            ctx.close()
            browser.close()
        except Exception:
            pass

    ws_findings = _process_websockets(ws_frames, session_id, bookmaker_id, _app)

    with _app.app_context():
        sess = db.session.get(ResearchSession, session_id)
        if sess and auth_info:
            sess.login_url    = auth_info.get("login_url")
            sess.otp_required = bool(
                auth_info.get("otp_flow", {}).get("present")
            )
            sess.phase = "LOGIN_PENDING"
            db.session.commit()

    sports_count = len([r for r in state.requests if r.is_sports])
    _log(session_id, bookmaker_id, "SUCCESS",
         f"Phase 1 complete — {sports_count} endpoints, "
         f"{len(ws_findings)} WS, {len(all_screenshots)} screenshots")

    if auth_info:
        otp_req = bool(auth_info.get("otp_flow", {}).get("present"))
        _log(session_id, bookmaker_id, "WARN",
             "🔑 Login found — requesting credentials from admin UI...")
        from app.services.emmiter import get_emitter
        get_emitter().emit("login_required", {
            "session_id":   session_id,
            "bookmaker_id": bookmaker_id,
            "domain":       domain,
            "login_url":    auth_info.get("login_url"),
            "fields":       auth_info.get("fields", []),
            "otp_required": otp_req,
            "phone_format": auth_info.get("phone_format"),
            "screenshots":  all_screenshots,
            "msg": (
                f"Login found for {domain}. "
                f"{'OTP required. ' if otp_req else ''}"
                "Enter credentials to continue."
            ),
        }, namespace="/admin")
    else:
        _finalise_session(
            session_id, bookmaker_id, bookmaker_name,
            state, ws_findings, base_url, all_screenshots, _app,
        )

    return auth_info


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 3 — AUTHENTICATED
# ─────────────────────────────────────────────────────────────────────────────

def run_authenticated_research(session_id: int, bookmaker_id: int, domain: str,
                                auth_info: dict, credentials: dict,
                                otp: str | None):
    from flask import current_app
    _app = current_app._get_current_object()

    with _app.app_context():
        bm             = db.session.get(Bookmaker, bookmaker_id)
        bookmaker_name = bm.name if bm else domain

    clean    = domain.strip().lower()
    base_url = clean if clean.startswith("http") else f"https://{clean}"

    _log(session_id, bookmaker_id, "INFO",
         "🔐 PHASE 3 (Vision): Authenticated research starting...")

    state          = SessionState(url=base_url)
    buffer         = CaptureBuffer()
    ws_frames      = {}
    ws_lock        = threading.Lock()
    seen_urls      = set()
    seen_lock      = threading.Lock()
    all_screenshots: list = []

    with sync_playwright() as p:
        browser, ctx = _new_context(p)

        action_label_ref = ["AUTH:INIT"]
        on_response, on_websocket = make_capture_handlers(
            buffer, action_label_ref, ws_frames, ws_lock, seen_urls, seen_lock
        )
        ctx.on("response",  on_response)
        ctx.on("websocket", on_websocket)

        login_ok = _perform_login(
            ctx, auth_info, credentials, otp,
            base_url, session_id, bookmaker_id, buffer, _app,
        )

        if login_ok:
            _log(session_id, bookmaker_id, "SUCCESS",
                 "✅ Login successful — authenticated exploration starting")
            with _app.app_context():
                sess = db.session.get(ResearchSession, session_id)
                if sess:
                    sess.login_success = True
                    sess.phase         = "AUTHENTICATED"
                    db.session.commit()
            _run_ai_exploration(
                ctx=ctx, base_url=base_url,
                session_id=session_id, bk_id=bookmaker_id,
                is_authenticated=True, state=state, buffer=buffer,
                ws_frames=ws_frames, ws_lock=ws_lock,
                all_screenshots=all_screenshots, _app=_app,
            )
        else:
            _log(session_id, bookmaker_id, "WARN", "⚠️ Login failed")
            with _app.app_context():
                sess = db.session.get(ResearchSession, session_id)
                if sess:
                    sess.login_success = False
                    sess.phase         = "LOGIN_FAILED"
                    db.session.commit()

        try:
            ctx.close()
            browser.close()
        except Exception:
            pass

    ws_findings = _process_websockets(ws_frames, session_id, bookmaker_id, _app)
    _finalise_session(
        session_id, bookmaker_id, bookmaker_name,
        state, ws_findings, base_url, all_screenshots, _app,
    )


# ─────────────────────────────────────────────────────────────────────────────
# LOGIN HELPER — Vision-assisted form fill
# ─────────────────────────────────────────────────────────────────────────────

def _perform_login(
    ctx: BrowserContext, auth_info: dict, credentials: dict,
    otp: str | None, base_url: str, session_id: int, bk_id: int,
    buffer: CaptureBuffer, _app,
) -> bool:
    login_url = auth_info.get("login_url", base_url.rstrip("/") + "/login")
    username  = credentials.get("username", "")
    password  = credentials.get("password", "")

    page = ctx.new_page()
    try:
        page.goto(login_url, wait_until="domcontentloaded", timeout=25000)
        _wait_for_render(page, session_id, bk_id)

        for field in auth_info.get("fields", []):
            fname = field.get("name", "")
            ftype = field.get("type", "")
            fsel  = field.get("selector", "")
            fx, fy = field.get("x"), field.get("y")
            val   = password if ftype == "password" else username
            if not val:
                continue

            # Try vision coords first
            if fx is not None and fy is not None:
                try:
                    page.mouse.click(fx, fy)
                    page.keyboard.type(val)
                    continue
                except Exception:
                    pass

            # Fallback: selector
            selectors = []
            if fsel:
                selectors.append(fsel)
            selectors += [
                f"[name='{fname}']", f"[id='{fname}']",
                f"input[type='{ftype}']",
            ]
            for sel in selectors:
                try:
                    loc = page.locator(sel).first
                    if loc.count() > 0:
                        loc.fill(val)
                        break
                except Exception:
                    continue

        # Submit — try vision coords then selectors
        sx, sy   = auth_info.get("submit_x"), auth_info.get("submit_y")
        submitted = False

        if sx is not None and sy is not None:
            try:
                page.mouse.click(sx, sy)
                submitted = True
            except Exception:
                pass

        if not submitted:
            submit_sel = auth_info.get("submit_selector", "")
            for sel in filter(None, [
                submit_sel,
                "button[type=submit]", "input[type=submit]",
                "text=Login", "text=Sign In", "text=Log In",
            ]):
                try:
                    loc = (
                        page.get_by_text(sel[5:], exact=False).first
                        if sel.startswith("text=")
                        else page.locator(sel).first
                    )
                    if loc.count() > 0:
                        loc.click(timeout=3000)
                        break
                except Exception:
                    continue

        time.sleep(4)
        buffer.drain()

        # OTP
        if otp:
            _log(session_id, bk_id, "INFO", f"📱 OTP: {otp}")
            for sel in [
                "input[type=number]", "input[name='otp']",
                "input[placeholder*='OTP']", "input[placeholder*='code']",
                ".otp-input", "[data-otp]",
            ]:
                try:
                    loc = page.locator(sel).first
                    if loc.count() > 0:
                        loc.fill(otp)
                        page.locator("button[type=submit]").first.click(timeout=3000)
                        time.sleep(3)
                        break
                except Exception:
                    continue

        current_url = page.url
        return (
            "/login"  not in current_url and
            "/signin" not in current_url and
            current_url != login_url
        )

    except Exception as e:
        _log(session_id, bk_id, "ERROR", f"Login error: {str(e)[:150]}")
        return False
    finally:
        try:
            page.close()
        except Exception:
            pass