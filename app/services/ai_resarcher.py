"""
AI Researcher
=============
All AI calls specific to the research pipeline — separate from the
parser-generation AI used in the harvest pipeline.
"""
import json
import os
from openai import OpenAI

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


def _call(system: str, user: str, model="gpt-4o", max_tokens=2000, json_mode=False) -> str:
    kwargs = {}
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": system},
                  {"role": "user",   "content": user}],
        max_tokens=max_tokens,
        temperature=0,
        **kwargs,
    )
    return resp.choices[0].message.content.strip()


# ─────────────────────────────────────────────────────────────────────────────
# JS ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

JS_ANALYSIS_PROMPT = """
You are a reverse-engineering expert analysing minified JavaScript from a betting website.

Find and document:
1. Encryption / signing of API requests (AES, RSA, HMAC, nonces, timestamps)
2. WebSocket protocol and message format (especially Aviator / crash games)
3. Auth token generation (JWT decoding, refresh logic, fingerprinting)
4. Anti-bot / anti-automation measures (canvas fingerprint, mouse tracking, reCAPTCHA v3)
5. Game engine logic (RNG seed, provably fair, multiplier calculation for Aviator)
6. Interesting function names, variable names, or comments in the JS

Return ONLY a JSON object:
{
  "encryption_methods": [{"name": "AES-256-CBC", "usage": "request body signing", "key_source": "hardcoded/dynamic", "code_hint": "relevant snippet"}],
  "websocket_protocol": {"url_pattern": "wss://...", "message_format": "JSON/binary", "example_message": "...", "auth_header": "..."},
  "auth_mechanism": {"type": "JWT/session/cookie", "token_field": "...", "refresh_endpoint": "...", "expiry_seconds": 3600},
  "anti_bot": ["reCAPTCHA v3", "canvas fingerprint"],
  "game_engine": {"type": "Aviator/crash/slots", "rng_method": "...", "provably_fair": true, "seed_source": "..."},
  "interesting_endpoints": [{"url_hint": "...", "purpose": "..."}],
  "notable_findings": ["finding 1", "finding 2"]
}
"""

def analyse_javascript(js_content: str, source_url: str) -> dict:
    try:
        result = _call(
            JS_ANALYSIS_PROMPT,
            f"Source: {source_url}\n\nJS (first 10000 chars):\n{js_content[:10000]}",
            model="gpt-4o",
            max_tokens=3000,
            json_mode=True,
        )
        return json.loads(result)
    except Exception as e:
        return {"error": str(e), "notable_findings": []}


# ─────────────────────────────────────────────────────────────────────────────
# AUTH FLOW ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

AUTH_FORM_PROMPT = """
You are analysing a login / registration form from a betting website.

Extract:
- The exact form fields (name, type, placeholder, required)
- The form action URL and method
- Any hidden fields or CSRF tokens
- Whether OTP / 2FA is present
- Phone number format hints (prefix, length)
- Password requirements shown

Return ONLY JSON:
{
  "login_url": "...",
  "form_method": "POST",
  "fields": [{"name": "phone", "type": "tel", "placeholder": "+254...", "required": true}],
  "hidden_fields": [{"name": "_token", "value": "..."}],
  "otp_flow": {"present": true, "trigger": "after_login/on_login", "length": 6, "delivery": "SMS/email"},
  "phone_format": {"prefix": "+254", "length": 9, "example": "+254712345678"},
  "password_rules": "min 6 chars",
  "captcha": "none/recaptcha/hcaptcha",
  "submit_endpoint": "/api/auth/login",
  "expected_response_fields": ["token", "user_id", "refresh_token"]
}
"""

def analyse_auth_form(page_html: str, page_url: str) -> dict:
    try:
        result = _call(
            AUTH_FORM_PROMPT,
            f"URL: {page_url}\n\nHTML:\n{page_html[:8000]}",
            model="gpt-4o",
            max_tokens=1000,
            json_mode=True,
        )
        return json.loads(result)
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# ERROR RESPONSE ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

ERROR_PROMPT = """
You are analysing error responses from a betting API to understand its structure.

Given an error JSON response from submitting incorrect data, extract:
- Field names in the request body
- Validation rules implied by error messages
- API response envelope structure
- Status code meanings

Return ONLY JSON:
{
  "request_fields": [{"name": "phone", "type": "string", "validation": "required, min 10 digits"}],
  "response_structure": {"success_key": "status", "data_key": "data", "error_key": "message"},
  "http_semantics": {"validation_error": 422, "auth_error": 401, "not_found": 404},
  "notes": "..."
}
"""

def analyse_error_response(error_json_str: str, endpoint_url: str, method: str) -> dict:
    try:
        result = _call(
            ERROR_PROMPT,
            f"Endpoint: {method} {endpoint_url}\nError response:\n{error_json_str[:3000]}",
            model="gpt-4o-mini",
            max_tokens=800,
            json_mode=True,
        )
        return json.loads(result)
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# WEBSOCKET PROTOCOL ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

WS_PROMPT = """
You are analysing WebSocket messages from a betting / casino game (often Aviator or crash games).

Given captured WebSocket frames, document:
- Message format (JSON / binary / protobuf)
- Game state update structure
- Multiplier / odds update format
- Auth handshake
- Heartbeat / ping-pong pattern

Return ONLY JSON:
{
  "format": "JSON",
  "auth_message": {"type": "auth", "token_field": "token"},
  "game_state_message": {"type": "game_state", "multiplier_field": "x", "status_field": "status"},
  "odds_update": {"event": "odds_change", "market_field": "...", "price_field": "..."},
  "heartbeat": {"client_sends": "ping", "server_responds": "pong", "interval_ms": 30000},
  "example_messages": ["..."],
  "integration_notes": "how to connect and receive live odds"
}
"""

def analyse_websocket_messages(messages: list[str], ws_url: str) -> dict:
    try:
        combined = "\n---\n".join(messages[:20])
        result = _call(
            WS_PROMPT,
            f"WebSocket URL: {ws_url}\n\nFrames:\n{combined[:6000]}",
            model="gpt-4o",
            max_tokens=1500,
            json_mode=True,
        )
        return json.loads(result)
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# REPORT GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

REPORT_PROMPT = """
You are a technical documentation writer for a sports betting data integration team.

Given structured research findings about a bookmaker's technical infrastructure,
write a comprehensive Markdown report that a developer can use to build an integration.

The report MUST include these sections:
# {bookmaker_name} — Technical Research Report

## Executive Summary
## Authentication & Login Flow
## API Endpoints Discovered
## WebSocket Connections
## JavaScript Analysis (Encryption, Anti-bot, Game Engines)
## Aviator / Crash Game Protocol (if found)
## Odds Data Structure
## Pagination & Rate Limiting
## Integration Recommendations
## Raw Endpoint Table

Use code blocks for JSON/curl examples. Be specific and technical.
"""

def generate_research_report(bookmaker_name: str, findings: dict) -> str:
    try:
        return _call(
            REPORT_PROMPT,
            f"Bookmaker: {bookmaker_name}\n\nFindings:\n{json.dumps(findings, indent=2)[:12000]}",
            model="gpt-4o",
            max_tokens=4000,
        )
    except Exception as e:
        return f"# Report Generation Failed\n\nError: {e}"