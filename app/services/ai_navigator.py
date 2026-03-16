"""
AI Navigator  v5  — Gemini Vision + Native SDK
================================================
Every page-analysis decision (what to click, what to dismiss, where the login
form is) is made by sending the REAL SCREENSHOT to Gemini 2.5 Pro vision.
HTTP response classification, parser generation, and WebSocket analysis remain
text-only because they work on captured data, not rendered pixels.

Architecture
────────────
Page decisions  →  screenshot (PNG bytes) + supplementary HTML  →  Gemini vision
Response data   →  JSON text  →  Gemini text

Vision returns both pixel coordinates AND a CSS/text selector fallback.
Clicking strategy (in order):
  1. Pixel coordinate click  (most reliable — Gemini sees exactly what we see)
  2. CSS selector click       (fallback)
  3. text= locator click      (last resort)

Key exports
───────────
  Constants       : OBSERVE_SECONDS, MAX_CLICKS
  Client          : _gemini_client
  Data classes    : CapturedRequest, ClickPlan, SessionState, CaptureBuffer
  Page AI         : _ai_dismiss_overlays, _ai_plan_clicks,
                    _ai_plan_clicks_fallback, _ai_find_login_form
  Data AI         : _ai_classify_request, _ai_generate_parser,
                    _ai_summarise_websocket, _ai_resolve_url_ids,
                    _ai_repair_parser
  Utilities       : make_capture_handlers, build_url_pattern,
                    is_noise_url, extract_bookmaker_name,
                    build_markdown_report, instantiate_url_pattern,
                    _build_curl

Requirements:
  pip install google-genai playwright
"""

import base64
import json
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from google import genai
from google.genai import types

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

OBSERVE_SECONDS     = 5       # Seconds to watch network after each action
MAX_CLICKS          = 40      # Max elements to click per session
GEMINI_MODEL        = "gemini-2.5-pro"
VIEWPORT_W          = 1920
VIEWPORT_H          = 1080
HTML_CONTEXT_CHARS  = 12_000  # Supplementary HTML chars sent alongside screenshot


# ─────────────────────────────────────────────────────────────────────────────
# GEMINI CLIENT
# ─────────────────────────────────────────────────────────────────────────────

def _make_gemini_client() -> genai.Client | None:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("[AI_NAV] WARNING: GEMINI_API_KEY not set")
        return None
    try:
        return genai.Client(api_key=api_key)
    except Exception as e:
        print(f"[AI_NAV] Failed to create Gemini client: {e}")
        return None


_gemini_client: genai.Client | None = _make_gemini_client()


# ─────────────────────────────────────────────────────────────────────────────
# CORE GENERATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _generate(client: genai.Client, prompt: str,
               response_mime_type: str = "application/json",
               temperature: float = 0.0,
               max_output_tokens: int = 2000) -> str:
    """Text-only generation. Returns raw response text."""
    config = types.GenerateContentConfig(
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        response_mime_type=response_mime_type,
    )
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=config,
    )
    return response.text


def _generate_json(client: genai.Client, prompt: str,
                    temperature: float = 0.0,
                    max_output_tokens: int = 2000) -> dict | list:
    """Text-only generation, parses and returns JSON."""
    raw = _generate(client, prompt,
                    response_mime_type="application/json",
                    temperature=temperature,
                    max_output_tokens=max_output_tokens)
    clean = raw.strip()
    if clean.startswith("```"):
        clean = re.sub(r"^```(?:json)?\s*", "", clean)
        clean = re.sub(r"\s*```$", "", clean)
    return json.loads(clean)


def _generate_vision_json(client: genai.Client,
                           screenshot_bytes: bytes,
                           prompt: str,
                           temperature: float = 0.0,
                           max_output_tokens: int = 2500) -> dict | list:
    """
    Multimodal generation: sends PNG screenshot + text prompt to Gemini vision.
    Parses and returns JSON.
    Gemini 2.5 Pro can identify UI elements and return their pixel coordinates.
    """
    config = types.GenerateContentConfig(
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        response_mime_type="application/json",
    )
    contents = [
        types.Part.from_bytes(data=screenshot_bytes, mime_type="image/png"),
        types.Part.from_text(prompt),
    ]
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=contents,
        config=config,
    )
    raw = response.text.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def _generate_vision_text(client: genai.Client,
                           screenshot_bytes: bytes,
                           prompt: str,
                           temperature: float = 0.1,
                           max_output_tokens: int = 2000) -> str:
    """Multimodal generation returning plain text (for code generation)."""
    config = types.GenerateContentConfig(
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        response_mime_type="text/plain",
    )
    contents = [
        types.Part.from_bytes(data=screenshot_bytes, mime_type="image/png"),
        types.Part.from_text(prompt),
    ]
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=contents,
        config=config,
    )
    return response.text


# ─────────────────────────────────────────────────────────────────────────────
# NOISE FILTER
# ─────────────────────────────────────────────────────────────────────────────

_NOISE_DOMAINS = {
    "google-analytics.com", "googletagmanager.com", "analytics.google.com",
    "doubleclick.net", "googlesyndication.com", "googleadservices.com",
    "facebook.com", "connect.facebook.net", "fbcdn.net",
    "twitter.com", "t.co", "analytics.twitter.com",
    "hotjar.com", "mouseflow.com", "fullstory.com", "heap.io",
    "mixpanel.com", "segment.com", "segment.io",
    "amplitude.com", "braze.com", "intercom.io", "intercom.com",
    "crisp.chat", "tawk.to", "zendesk.com", "freshchat.com",
    "newrelic.com", "datadog-browser-agent.com", "sentry.io", "rollbar.com",
    "clarity.ms", "bing.com", "bat.bing.com",
    "cloudfront.net", "fastly.net", "akamaiedge.net", "akamai.net",
    "cloudflare.com", "cloudflareinsights.com",
    "fonts.googleapis.com", "fonts.gstatic.com",
    "use.typekit.net", "kit.fontawesome.com",
    "adsrvr.org", "adnxs.com", "rubiconproject.com", "pubmatic.com",
    "openx.net", "criteo.com", "outbrain.com", "taboola.com",
    "cookielaw.org", "cookiebot.com", "onetrust.com",
    "stripe.com", "braintreepayments.com", "paypal.com",
}

_NOISE_PATH_FRAGMENTS = {
    "/gtm.", "/gtag/", "/analytics", "/pixel", "/beacon",
    "/collect", "/track", "/event", "/ping",
    "/ad/", "/ads/", "/advertising/",
    "/consent", "/cookie", "/cdn-cgi/", "/__utm",
    "/recaptcha", "/captcha", "/font", "/fonts", "/favicon",
}

_NOISE_CONTENT_TYPES = {
    "text/css", "font/", "image/", "audio/",
    "application/font", "application/x-font",
}

_MIN_RESPONSE_BYTES = 50


def is_noise_url(url: str, content_type: str = "") -> bool:
    """Return True if this URL is definitely not sports data."""
    try:
        parsed         = urlparse(url.lower())
        hostname       = parsed.hostname or ""
        hostname_clean = hostname.removeprefix("www.")

        for nd in _NOISE_DOMAINS:
            if hostname_clean == nd or hostname_clean.endswith("." + nd):
                return True

        path = parsed.path
        for frag in _NOISE_PATH_FRAGMENTS:
            if frag in path:
                return True

        ct = content_type.lower()
        for nct in _NOISE_CONTENT_TYPES:
            if ct.startswith(nct):
                return True

        if re.search(
            r"\.(png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|eot|css|map|pdf)(\?|$)",
            url, re.I,
        ):
            return True

    except Exception:
        pass
    return False


# ─────────────────────────────────────────────────────────────────────────────
# BOOKMAKER NAME
# ─────────────────────────────────────────────────────────────────────────────

def extract_bookmaker_name(domain: str) -> str:
    try:
        raw = domain.strip().lower()
        if not raw.startswith("http"):
            raw = "https://" + raw
        parsed   = urlparse(raw)
        hostname = parsed.hostname or raw
        for prefix in ("www.", "m.", "mobile.", "app.", "sports.", "bet.", "en."):
            if hostname.startswith(prefix):
                hostname = hostname[len(prefix):]
        parts      = hostname.split(".")
        tld_parts  = {"com", "co", "uk", "ng", "ke", "za", "tz", "gh",
                      "net", "org", "io", "bet", "sport", "live", "app"}
        brand_parts = [p for p in parts if p not in tld_parts]
        name = brand_parts[0] if brand_parts else parts[0]
        return name.title()
    except Exception:
        return domain


# ─────────────────────────────────────────────────────────────────────────────
# URL PATTERN BUILDING
# ─────────────────────────────────────────────────────────────────────────────

_ID_PARAM_NAMES = {
    "gameId", "gameIds", "game_id", "game_ids", "games",
    "eventId", "eventIds", "event_id", "event_ids", "events",
    "matchId", "matchIds", "match_id", "match_ids", "matches",
    "fixtureId", "fixtureIds", "fixture_id", "fixture_ids", "fixtures",
    "sportId", "sport_id", "sportIds", "sport_ids",
    "competitionId", "competition_id", "competitionIds", "competition_ids",
    "leagueId", "league_id", "tournamentId", "tournament_id",
    "categoryId", "category_id", "regionId", "region_id",
    "marketId", "market_id",
}

_ID_PARAM_TO_SOURCE = {
    "gameId": "unifiedmatch.id", "gameIds": "unifiedmatch.id",
    "game_id": "unifiedmatch.id", "game_ids": "unifiedmatch.id",
    "games": "unifiedmatch.id",
    "eventId": "unifiedmatch.id", "eventIds": "unifiedmatch.id",
    "event_id": "unifiedmatch.id", "event_ids": "unifiedmatch.id",
    "events": "unifiedmatch.id",
    "matchId": "unifiedmatch.id", "matchIds": "unifiedmatch.id",
    "match_id": "unifiedmatch.id", "match_ids": "unifiedmatch.id",
    "matches": "unifiedmatch.id",
    "fixtureId": "unifiedmatch.id", "fixtureIds": "unifiedmatch.id",
    "fixture_id": "unifiedmatch.id", "fixture_ids": "unifiedmatch.id",
    "fixtures": "unifiedmatch.id",
    "sportId": "sport.id", "sport_id": "sport.id",
    "sportIds": "sport.id", "sport_ids": "sport.id",
    "competitionId": "competition.id", "competition_id": "competition.id",
    "competitionIds": "competition.id", "competition_ids": "competition.id",
    "leagueId": "competition.id", "league_id": "competition.id",
    "tournamentId": "competition.id", "tournament_id": "competition.id",
    "categoryId": "sport.id", "category_id": "sport.id",
    "regionId": "country.id", "region_id": "country.id",
    "marketId": "marketdefinition.id", "market_id": "marketdefinition.id",
}


def build_url_pattern(url: str) -> tuple[str, dict]:
    """
    Build a parameterised URL pattern from a real captured URL.
    Returns (pattern_string, id_schema).
    """
    try:
        parsed    = urlparse(url)
        qs        = parse_qs(parsed.query, keep_blank_values=True)
        id_schema = {}
        new_qs    = {}

        for key, values in qs.items():
            raw_val      = ",".join(values)
            matched_name = next(
                (n for n in _ID_PARAM_NAMES if key.lower() == n.lower()), None
            )
            if matched_name:
                source      = _ID_PARAM_TO_SOURCE.get(matched_name, "unifiedmatch.id")
                placeholder = "{" + source.split(".")[0].upper() + "_ID}"
                new_qs[key] = [placeholder]
                id_schema[key] = {
                    "source":         source,
                    "original_value": raw_val,
                    "placeholder":    placeholder,
                }
            else:
                new_qs[key] = ["{id}"] if re.match(
                    r"^\d+$", raw_val.replace(",", "")
                ) else values

        new_query = urlencode(new_qs, doseq=True)
        path = re.sub(
            r"/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            "/{uuid}", parsed.path,
        )
        path = re.sub(r"/\d{5,}", "/{id}", path)

        pattern = urlunparse((
            parsed.scheme, parsed.netloc,
            path, parsed.params, new_query, "",
        ))
        return pattern, id_schema

    except Exception:
        return url, {}


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CapturedRequest:
    url:          str
    method:       str
    request_type: str
    status:       int | None
    req_headers:  dict
    req_body:     str | None
    resp_sample:  str | None
    resp_size:    int
    curl:         str
    captured_at:  str
    action_label: str
    is_sports:    bool = False
    ai_category:  str  = ""
    ai_notes:     str  = ""
    url_pattern:  str  = ""
    id_schema:    dict = field(default_factory=dict)


@dataclass
class ClickPlan:
    """
    A single planned click action returned by Gemini vision.
    x, y are pixel coordinates from the viewport screenshot (1920×1080).
    selector is a CSS/text= fallback used when coordinate click fails.
    """
    selector: str
    label:    str
    priority: int
    reason:   str
    x:        int | None = None   # pixel x from Gemini vision
    y:        int | None = None   # pixel y from Gemini vision


@dataclass
class SessionState:
    url:           str
    requests:      list[CapturedRequest] = field(default_factory=list)
    click_history: list[str]             = field(default_factory=list)
    findings_md:   list[str]             = field(default_factory=list)
    click_count:   int                   = 0


# ─────────────────────────────────────────────────────────────────────────────
# CURL BUILDER
# ─────────────────────────────────────────────────────────────────────────────

_SKIP_H = frozenset({
    "content-length", "host", "connection",
    ":method", ":path", ":scheme", ":authority",
    "sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site",
})


def _build_curl(method: str, url: str, headers: dict,
                body: str | None = None) -> str:
    parts = [f"curl -X {method.upper()} '{url}'"]
    for k, v in headers.items():
        if k.lower() in _SKIP_H:
            continue
        parts.append(f"  -H '{k}: {v.replace(chr(39), chr(92)+chr(39))}'")
    if body:
        parts.append(f"  -d '{body.replace(chr(39), chr(92)+chr(39))}'")
    return " \\\n".join(parts)


# ─────────────────────────────────────────────────────────────────────────────
# REQUEST TYPE DETECTION
# ─────────────────────────────────────────────────────────────────────────────

def _detect_request_type(content_type: str, url: str) -> str:
    ct = content_type.lower()
    if "application/json" in ct or "json" in ct:
        return "json"
    if "text/html"         in ct: return "html"
    if "text/event-stream" in ct: return "sse"
    if ("application/octet-stream" in ct or
            "protobuf" in ct or "msgpack" in ct):
        return "buffer"
    if "multipart" in ct:
        return "multipart"
    if "json" in url.lower() and (
        "poll" in url.lower() or "subscribe" in url.lower()
    ):
        return "longpoll"
    return "other"


# ─────────────────────────────────────────────────────────────────────────────
# GEMINI VISION — PAGE ANALYSIS FUNCTIONS
# All functions that look at the rendered page use vision (screenshot → Gemini).
# ─────────────────────────────────────────────────────────────────────────────

def _ai_dismiss_overlays(client: genai.Client,
                          screenshot_bytes: bytes | None,
                          html_snapshot: str,
                          base_url: str) -> list[ClickPlan]:
    """
    Phase 0: Gemini vision identifies overlays, modals, cookie banners,
    age gates, or promotional popups blocking the main content and returns
    click actions to dismiss them.

    Uses screenshot as primary input; HTML included for selector accuracy.
    Returns [] if no overlays are detected.
    """
    prompt = f"""
You are analysing a screenshot of a sports betting website.

TASK: Identify any OVERLAYS, MODALS, or DIALOGS that are currently BLOCKING
the main content of the page.

Look for:
- Cookie consent banners  ("Accept all", "Accept cookies", "I agree")
- Age verification gates  ("I am 18+", "Enter", "Confirm age")
- Location / geo-block dialogs ("Continue anyway", "I understand")
- Welcome / promotional popups (close button "×", "Maybe later", "No thanks")
- Terms of service popups
- Push notification permission requests ("Not now", "Block")
- Newsletter signup modals

Viewport: {VIEWPORT_W} × {VIEWPORT_H} pixels
Current URL: {base_url}

Supplementary HTML (for accurate selectors):
{html_snapshot[:HTML_CONTEXT_CHARS]}

For each overlay dismissal action, return:
{{
  "x": pixel x of the dismiss button (integer, center of button),
  "y": pixel y of the dismiss button (integer, center of button),
  "selector": "CSS or text= selector as fallback",
  "label": "e.g. Accept cookies / Close popup / Confirm age 18+",
  "priority": 0,
  "reason": "This overlay blocks the main betting content"
}}

If NO overlays are visible, return an empty JSON array [].
Return at most 5 actions. Return ONLY a valid JSON array, no markdown.
""".strip()

    try:
        if screenshot_bytes:
            data = _generate_vision_json(
                client, screenshot_bytes, prompt,
                temperature=0.0, max_output_tokens=1000,
            )
        else:
            data = _generate_json(client, prompt,
                                   temperature=0.0, max_output_tokens=1000)

        items = data if isinstance(data, list) else []
        plans = []
        for item in items[:5]:
            if not isinstance(item, dict):
                continue
            plans.append(ClickPlan(
                selector=item.get("selector", ""),
                label=item.get("label", "dismiss overlay"),
                priority=0,
                reason=item.get("reason", "overlay dismissal"),
                x=item.get("x"),
                y=item.get("y"),
            ))
        return plans

    except Exception as e:
        print(f"[AI_NAV] overlay detection failed: {e}")
        return []


def _ai_plan_clicks(client: genai.Client,
                     screenshot_bytes: bytes | None,
                     html_snapshot: str,
                     base_url: str,
                     already_clicked: list[str],
                     page_title: str) -> list[ClickPlan]:
    """
    Gemini vision → prioritised click plan for sports data discovery.

    Sends the real viewport screenshot so Gemini sees exactly what the user
    would see — rendered JS components, lazy-loaded content, current state.
    HTML snapshot included as supplementary context for accurate selectors.

    Returns ClickPlan items with pixel coordinates AND CSS fallback selectors.
    """
    prompt = f"""
You are an autonomous web agent exploring a sports betting website.
Goal: discover ALL API endpoints delivering sports data (odds, markets,
fixtures, live scores, lineups) for an odds comparison platform.

Current URL: {base_url}
Page title: {page_title}
Viewport: {VIEWPORT_W} × {VIEWPORT_H} pixels
Already interacted with (DO NOT repeat): {json.dumps(already_clicked[-20:])}

Supplementary HTML (for accurate selectors):
{html_snapshot[:HTML_CONTEXT_CHARS]}

Look at the screenshot. Return a JSON array of click actions ordered by importance.

PRIORITY ORDER:
0 — Any remaining overlays, popups, or modals still blocking content
1 — Sport/league navigation tabs: Football, Basketball, Tennis, Live, Today,
    Tomorrow, In-Play, Prematch, Virtual, Highlights
2 — Individual match rows or event cards (triggers per-match odds API calls)
3 — Market type tabs: 1X2, Over/Under, Asian Handicap, BTTS, Correct Score,
    Both Teams to Score, Double Chance, Draw No Bet
4 — League/competition headers, accordion toggles, "Show all" links
5 — Load more / pagination / "View all markets" buttons

Each action:
{{
  "x": pixel x coordinate (integer, center of the element, 0-{VIEWPORT_W}),
  "y": pixel y coordinate (integer, center of the element, 0-{VIEWPORT_H}),
  "selector": "CSS selector or text=ButtonText as fallback",
  "label": "human-readable description",
  "priority": 0-5,
  "reason": "what API calls this will trigger"
}}

SKIP:
- Login / Register / Sign up / Deposit / Withdraw / Account / Profile
- Social media, Help, Support, Chat, Contact, Footer links
- Language selectors, currency selectors, app download prompts
- Advertisements, banners, affiliate links
- Anything already in the already-interacted list above

If the page appears empty, loading, or entirely blocked — return [].
Return at most 15 actions. Return ONLY a valid JSON array, no markdown.
""".strip()

    try:
        if screenshot_bytes:
            data = _generate_vision_json(
                client, screenshot_bytes, prompt,
                temperature=0.2, max_output_tokens=2500,
            )
        else:
            # Screenshot unavailable — fall back to HTML-only
            data = _generate_json(client, prompt,
                                   temperature=0.2, max_output_tokens=2500)

        items = data if isinstance(data, list) else data.get("actions",
                                                data.get("clicks", []))
        plans = []
        for item in items[:15]:
            if not isinstance(item, dict):
                continue
            # Accept item if it has coords OR a selector
            if not (item.get("selector") or
                    (item.get("x") is not None and item.get("y") is not None)):
                continue
            plans.append(ClickPlan(
                selector=item.get("selector", ""),
                label=item.get("label", item.get("selector", "unknown")),
                priority=int(item.get("priority", 5)),
                reason=item.get("reason", ""),
                x=item.get("x"),
                y=item.get("y"),
            ))
        plans.sort(key=lambda p: p.priority)
        return plans

    except Exception as e:
        print(f"[AI_NAV] click plan failed: {e}")
        return []


def _ai_plan_clicks_fallback(client: genai.Client,
                              screenshot_bytes: bytes | None,
                              html_snapshot: str,
                              base_url: str) -> list[ClickPlan]:
    """
    Fallback strategy when the main click plan returns empty.
    Gemini vision scans for any visible navigation links pointing to sports
    sections and returns direct coordinate / href actions.
    """
    prompt = f"""
A sports betting website's main navigation appears empty or not yet loaded.

Look carefully at the screenshot for ANY visible elements that could lead to
sports content — even partially loaded navigation, a hamburger menu icon,
a loading spinner that has finished, any text labels for sports.

Also scan the supplementary HTML for <a href> links pointing to sports sections.

URL: {base_url}
Viewport: {VIEWPORT_W} × {VIEWPORT_H} pixels

HTML:
{html_snapshot[:HTML_CONTEXT_CHARS]}

Return a JSON array of the best navigation actions available:
[
  {{
    "x": pixel x (integer),
    "y": pixel y (integer),
    "selector": "a[href*='football'] or similar",
    "label": "link text or description",
    "priority": 5,
    "reason": "fallback navigation to sports section"
  }}
]

Return at most 10 items. Return ONLY valid JSON array.
""".strip()

    try:
        if screenshot_bytes:
            data = _generate_vision_json(
                client, screenshot_bytes, prompt,
                temperature=0.0, max_output_tokens=1500,
            )
        else:
            data = _generate_json(client, prompt,
                                   temperature=0.0, max_output_tokens=1500)

        items = data if isinstance(data, list) else []
        plans = []
        for i, item in enumerate(items[:10]):
            if not isinstance(item, dict):
                continue
            plans.append(ClickPlan(
                selector=item.get("selector", ""),
                label=item.get("label", f"fallback {i}"),
                priority=5,
                reason=item.get("reason", "fallback href navigation"),
                x=item.get("x"),
                y=item.get("y"),
            ))
        return plans

    except Exception as e:
        print(f"[AI_NAV] fallback click plan failed: {e}")
        return []


def _ai_find_login_form(client: genai.Client,
                         screenshot_bytes: bytes | None,
                         html_snapshot: str,
                         base_url: str) -> dict | None:
    """
    Gemini vision identifies the login/sign-in form or link.
    Returns a dict with selectors and form metadata, or None.
    """
    prompt = f"""
You are analysing a sports betting website to find the login / sign-in form.

Viewport: {VIEWPORT_W} × {VIEWPORT_H} pixels
Base URL: {base_url}

Supplementary HTML:
{html_snapshot[:HTML_CONTEXT_CHARS]}

Look at the screenshot and the HTML. Find:
- The login button / link (if the form is on another page)
- OR the login form fields if present on this page

Return JSON:
{{
  "login_url": "full URL of login page or null",
  "login_link_selector": "CSS selector for the login button/link, or null",
  "login_link_x": pixel x of the login button (integer or null),
  "login_link_y": pixel y of the login button (integer or null),
  "form_present_on_this_page": true/false,
  "fields": [
    {{
      "name": "field name",
      "type": "email|tel|text|password",
      "selector": "CSS selector",
      "x": pixel x of field center,
      "y": pixel y of field center
    }}
  ],
  "submit_selector": "CSS selector for submit button",
  "submit_x": pixel x of submit button,
  "submit_y": pixel y of submit button,
  "otp_flow": {{"present": true/false, "trigger": "after_submit|on_page"}},
  "phone_format": "e.g. +254XXXXXXXXX or null"
}}

Return ONLY valid JSON, no markdown.
""".strip()

    try:
        if screenshot_bytes:
            data = _generate_vision_json(
                client, screenshot_bytes, prompt,
                temperature=0.0, max_output_tokens=800,
            )
        else:
            data = _generate_json(client, prompt,
                                   temperature=0.0, max_output_tokens=800)

        data.setdefault("login_url", base_url.rstrip("/") + "/login")
        return data

    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# GEMINI TEXT — HTTP RESPONSE ANALYSIS FUNCTIONS
# These work on captured HTTP data, not rendered pages — no vision needed.
# ─────────────────────────────────────────────────────────────────────────────

def _ai_classify_request(client: genai.Client,
                          captured: CapturedRequest) -> tuple[bool, str, str]:
    """
    Classify a captured HTTP request.
    Returns (is_sports_related, category, notes).

    Categories: MATCH_LIST | ODDS | LIVE_SCORES | LINEUPS | MARKETS |
                WEBSOCKET | LONGPOLL | AUTH | CONFIG | STATIC | OTHER
    """
    resp_preview = (captured.resp_sample or "")[:2000]
    prompt = f"""
You are analysing a captured HTTP request from a sports betting website.

URL: {captured.url}
Method: {captured.method}
Request type: {captured.request_type}
Response size: {captured.resp_size} bytes
Response sample:
{resp_preview}

Classify this request:
1. is_sports_related: true/false
   TRUE  for: match lists, odds, markets, fixtures, live scores, lineups,
              competitions, sports navigation data, event metadata.
   FALSE for: ads, analytics, tracking, fonts, images, cookie banners,
              payment processors, chat widgets, error logging, A/B testing,
              social media, CDN assets.

2. category: MATCH_LIST | ODDS | LIVE_SCORES | LINEUPS | MARKETS |
              WEBSOCKET | LONGPOLL | AUTH | CONFIG | STATIC | OTHER

3. notes: 1-2 sentences describing what this endpoint provides for an odds
           aggregator. Include polling interval if detectable.

Return JSON: {{"is_sports_related": bool, "category": str, "notes": str}}
Return ONLY valid JSON.
""".strip()

    try:
        data = _generate_json(client, prompt,
                               temperature=0.0, max_output_tokens=300)
        return (
            bool(data.get("is_sports_related", False)),
            data.get("category", "OTHER"),
            data.get("notes", ""),
        )
    except Exception:
        return False, "OTHER", ""


def _ai_resolve_url_ids(client: genai.Client, url: str, id_schema: dict,
                         resp_sample: str) -> dict:
    """Enrich id_schema with AI-detected ID type information."""
    if not id_schema:
        return id_schema

    prompt = f"""
A sports betting API endpoint was captured:
URL: {url}

URL ID parameters:
{json.dumps(id_schema, indent=2)}

Response sample:
{resp_sample[:2000]}

For each parameter add:
  "description": what these IDs represent
  "id_format": "numeric" | "uuid" | "alphanumeric" | "mixed"
  "multi_value": true/false
  "separator": "," | ";" | "|" | null
  "max_per_request": number or null

Return JSON with the same top-level keys as input, enriched. No markdown.
""".strip()

    try:
        enriched = _generate_json(client, prompt,
                                   temperature=0.0, max_output_tokens=800)
        for key in id_schema:
            if key in enriched and isinstance(enriched[key], dict):
                id_schema[key].update({
                    k: v for k, v in enriched[key].items()
                    if k not in id_schema[key]
                })
        return id_schema
    except Exception:
        return id_schema


def _ai_generate_parser(client: genai.Client, url: str,
                         resp_sample: str, category: str) -> str:
    """Generate a Python parse_data(raw) function for a sports endpoint."""
    prompt = f"""
Write a Python function `parse_data(raw_data)` to extract structured sports
data from this API response.

Endpoint URL: {url}
Category: {category}
Response sample:
{resp_sample[:4000]}

Return a list of dicts with keys (None if unavailable):
  sport, competition, home_team, away_team, start_time,
  market, selection, price, specifier

Rules:
- Handle both list and dict top-level responses
- Use try/except around every risky access
- Return [] on any error
- No imports needed (json is available in scope)

Return ONLY the Python function code, no markdown fences.
""".strip()

    try:
        raw = _generate(client, prompt,
                         response_mime_type="text/plain",
                         temperature=0.1,
                         max_output_tokens=1500)
        code = raw.strip()
        code = re.sub(r"^```(?:python)?\s*", "", code)
        code = re.sub(r"\s*```$", "", code)
        return code
    except Exception:
        return "def parse_data(raw_data):\n    return []"


def _ai_repair_parser(client: genai.Client, url: str, broken_code: str,
                       error_msg: str, resp_sample: str,
                       category: str) -> str:
    """
    When a parser crashes at harvest time, Gemini repairs it using the
    actual live response data and the error message.
    Returns fixed parser code.
    """
    prompt = f"""
A Python data-extraction function for a sports betting API endpoint is broken.

Endpoint URL: {url}
Category: {category}

BROKEN CODE:
```python
{broken_code[:2000]}
```

ERROR:
{error_msg[:500]}

LIVE RESPONSE SAMPLE (the real data the parser received):
{resp_sample[:3000]}

Write a corrected `parse_data(raw_data)` function that handles this response.
The function must return a list of dicts with keys:
  sport, competition, home_team, away_team, start_time,
  market, selection, price, specifier

All keys must be present (use None for missing values).
Return [] on any error. No imports needed.
Return ONLY the Python function code, no markdown fences.
""".strip()

    try:
        raw = _generate(client, prompt,
                         response_mime_type="text/plain",
                         temperature=0.1,
                         max_output_tokens=1500)
        code = raw.strip()
        code = re.sub(r"^```(?:python)?\s*", "", code)
        code = re.sub(r"\s*```$", "", code)
        return code
    except Exception:
        return broken_code  # Return original if repair also fails


def _ai_summarise_websocket(client: genai.Client,
                             frames: list[str], ws_url: str) -> dict:
    """Analyse WebSocket frames to understand the protocol and content."""
    prompt = f"""
Analyse these WebSocket frames from a sports betting site.

WebSocket URL: {ws_url}
Frames (first 20):
{json.dumps(frames[:20], indent=2)}

Return JSON:
{{
  "format": "json | binary | protobuf | custom_text",
  "purpose": "what data this WS stream carries",
  "message_types": ["list of message type names/codes seen"],
  "odds_present": true/false,
  "live_scores_present": true/false,
  "subscribe_message": "message to send to subscribe (if visible) or null",
  "update_frequency_ms": estimated milliseconds between updates or null
}}
Return ONLY valid JSON.
""".strip()

    try:
        return _generate_json(client, prompt,
                               temperature=0.0, max_output_tokens=600)
    except Exception:
        return {"format": "unknown", "purpose": "unknown"}


# ─────────────────────────────────────────────────────────────────────────────
# MARKDOWN REPORT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _md_request_block(req: CapturedRequest, index: int) -> str:
    lines = [
        f"### [{index}] {req.ai_category} — `{req.url[:100]}`",
        f"",
        f"**Triggered by:** `{req.action_label}`  ",
        f"**Type:** {req.request_type}  ",
        f"**Status:** {req.status}  ",
        f"**Size:** {req.resp_size} bytes  ",
        f"**Captured:** {req.captured_at}  ",
        f"",
        f"**Notes:** {req.ai_notes}",
        f"",
        f"**Pattern:** `{req.url_pattern}`  ",
    ]
    if req.id_schema:
        lines += [
            "", "**ID Schema:**",
            "```json", json.dumps(req.id_schema, indent=2), "```",
        ]
    lines += ["", "**cURL:**", "```bash", req.curl, "```", ""]
    if req.resp_sample:
        lines += [
            "**Response sample:**",
            "```", req.resp_sample[:800], "```", "",
        ]
    return "\n".join(lines)


def build_markdown_report(
    bookmaker_name: str,
    base_url: str,
    sports_requests: list[CapturedRequest],
    all_requests: list[CapturedRequest],
    ws_findings: list[dict],
    click_history: list[str],
    parser_map: dict[str, str],
) -> str:
    from collections import Counter
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines = [
        f"# 🔬 Research Report: {bookmaker_name}",
        f"",
        f"**URL:** {base_url}  ",
        f"**Generated:** {ts}  ",
        f"**Total captured:** {len(all_requests)}  ",
        f"**Sports endpoints:** {len(sports_requests)}  ",
        f"**WebSocket streams:** {len(ws_findings)}  ",
        f"**Actions taken:** {len(click_history)}  ",
        f"", f"---", f"", f"## 📊 Summary", f"",
    ]
    cats = Counter(r.ai_category for r in sports_requests)
    for cat, count in cats.most_common():
        lines.append(f"- **{cat}**: {count} endpoint(s)")
    lines.append("")

    if sports_requests:
        lines += [
            "---", "", "## 🏆 Sports Data Endpoints", "",
            f"Each observed for {OBSERVE_SECONDS}s after the triggering action.", "",
        ]
        for i, req in enumerate(sports_requests, 1):
            lines.append(_md_request_block(req, i))

    if ws_findings:
        lines += ["---", "", "## 🔌 WebSocket Streams", ""]
        for i, ws in enumerate(ws_findings, 1):
            lines += [
                f"### WS [{i}] `{ws.get('url', '')[:100]}`", "",
                f"**Format:** {ws.get('format')}  ",
                f"**Purpose:** {ws.get('purpose')}  ",
                f"**Odds present:** {ws.get('odds_present')}  ",
                f"**Update freq:** {ws.get('update_frequency_ms')} ms  ", "",
            ]

    lines += [
        "---", "", "## 🗺️ Exploration Path", "",
        *[f"{i}. {a}" for i, a in enumerate(click_history, 1)],
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# CAPTURE BUFFER
# ─────────────────────────────────────────────────────────────────────────────

class CaptureBuffer:
    """Thread-safe buffer collecting Playwright network events."""

    def __init__(self):
        self._lock    = threading.Lock()
        self._pending: list[CapturedRequest] = []

    def add(self, req: CapturedRequest):
        with self._lock:
            self._pending.append(req)

    def drain(self) -> list[CapturedRequest]:
        with self._lock:
            out = list(self._pending)
            self._pending.clear()
            return out

    def drain_after(self, seconds: float) -> list[CapturedRequest]:
        time.sleep(seconds)
        return self.drain()


# ─────────────────────────────────────────────────────────────────────────────
# RESPONSE INTERCEPTOR FACTORY
#
# Deduplication: exact URL at CAPTURE time.
# Pattern-level dedup happens at SAVE time (DB upsert by url_pattern).
# ─────────────────────────────────────────────────────────────────────────────

def make_capture_handlers(buffer: CaptureBuffer,
                           action_label_ref: list[str],
                           ws_frames: dict,
                           ws_lock: threading.Lock,
                           seen_urls: set,
                           seen_lock: threading.Lock):
    """
    Returns (on_response, on_websocket) Playwright event handlers.
    action_label_ref is a 1-element list mutated externally to tag captures.
    """

    def on_response(response):
        try:
            url = response.url
            ct  = response.headers.get("content-type", "")

            if is_noise_url(url, ct):
                return

            rtype = _detect_request_type(ct, url)

            try:
                raw_bytes = response.body()
                size      = len(raw_bytes)
                if size < _MIN_RESPONSE_BYTES:
                    return
            except Exception:
                return

            resp_sample = None
            try:
                if rtype in ("json", "html", "sse", "longpoll") or "text" in ct:
                    resp_sample = raw_bytes.decode("utf-8", errors="replace")[:3000]
                elif rtype == "buffer":
                    resp_sample = f"<binary {size}b hex={raw_bytes[:64].hex()}>"
            except Exception:
                pass

            try:
                req_obj  = response.request
                req_hdrs = dict(req_obj.headers)
                req_body = req_obj.post_data
                curl     = _build_curl(req_obj.method, url, req_hdrs, req_body)
                method   = req_obj.method
            except Exception:
                req_hdrs = {}
                req_body = None
                curl     = f"curl '{url}'"
                method   = "GET"

            # Exact-URL dedup
            with seen_lock:
                if url in seen_urls:
                    return
                seen_urls.add(url)

            url_pattern, id_schema = build_url_pattern(url)

            buffer.add(CapturedRequest(
                url=url,
                method=method,
                request_type=rtype,
                status=response.status,
                req_headers=req_hdrs,
                req_body=req_body,
                resp_sample=resp_sample,
                resp_size=size,
                curl=curl,
                captured_at=datetime.now(timezone.utc).isoformat(),
                action_label=action_label_ref[0],
                url_pattern=url_pattern,
                id_schema=id_schema,
            ))

        except Exception:
            pass

    def on_websocket(ws):
        ws_url = ws.url
        if is_noise_url(ws_url):
            return
        with ws_lock:
            ws_frames.setdefault(ws_url, [])

        def on_frame(payload):
            with ws_lock:
                frames = ws_frames[ws_url]
                if len(frames) < 50:
                    frames.append(str(payload)[:500])

        ws.on("framereceived", on_frame)
        ws.on("framesent",     lambda p: None)

    return on_response, on_websocket


# ─────────────────────────────────────────────────────────────────────────────
# HARVEST-TIME URL INSTANTIATION
# ─────────────────────────────────────────────────────────────────────────────

def instantiate_url_pattern(pattern: str, id_schema: dict,
                              db_session) -> list[str]:
    """
    Substitute real IDs from the DB into a stored URL pattern.
    Called by the harvest worker to build concrete request URLs.
    """
    if not id_schema or "{" not in pattern:
        return [pattern]

    try:
        from app.models.sports_model import Sport, Competition, Country
        from app.models.match_model import UnifiedMatch
        from app.models.market_model import MarketDefinition
    except ImportError:
        return [pattern]

    _source_to_model = {
        "unifiedmatch.id":     (UnifiedMatch, "id"),
        "sport.id":            (Sport, "id"),
        "competition.id":      (Competition, "id"),
        "country.id":          (Country, "id"),
        "marketdefinition.id": (MarketDefinition, "id"),
    }

    replacements = {}

    for param_key, schema in id_schema.items():
        source      = schema.get("source", "unifiedmatch.id")
        separator   = schema.get("separator", ",")
        max_per     = schema.get("max_per_request", 20) or 20
        placeholder = schema.get(
            "placeholder",
            "{" + source.split(".")[0].upper() + "_ID}",
        )

        if source not in _source_to_model:
            replacements[placeholder] = schema.get("original_value", "1")
            continue

        model, col = _source_to_model[source]
        try:
            ids = [
                str(row[0])
                for row in db_session.query(getattr(model, col))
                                     .order_by(getattr(model, col).desc())
                                     .limit(max_per)
                                     .all()
            ]
        except Exception:
            ids = [schema.get("original_value", "1")]

        if not ids:
            ids = [schema.get("original_value", "1")]

        replacements[placeholder] = (separator or ",").join(ids)

    url = pattern
    for placeholder, value in replacements.items():
        url = url.replace(placeholder, value)
    url = re.sub(r"\{id\}", "1", url)
    url = re.sub(r"\{uuid\}", "00000000-0000-0000-0000-000000000000", url)
    return [url]