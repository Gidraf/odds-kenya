"""
Langfuse Prompt Service  —  app/services/langfuse_prompts.py
=============================================================
Central registry of every AI prompt used in the Research pipeline.

Each prompt is stored in the Langfuse dashboard under the name shown in
PROMPT_NAMES.  At runtime we fetch the current version, compile the
{{variable}} placeholders, and return a ready-to-send string.

Langfuse prompt names  (create these in your Langfuse project)
───────────────────────────────────────────────────────────────
  research/single-parser          → generate parse_data() for a single endpoint
  research/match-list-parser      → Stage 1 — generate parse_match_list()
  research/markets-parser         → Stage 2 — generate parse_markets()
  research/combine-parsers        → Stage 3 — merge into parse_data(ml, mk)

Environment variables required
───────────────────────────────
  LANGFUSE_PUBLIC_KEY
  LANGFUSE_SECRET_KEY
  LANGFUSE_HOST   (optional, default https://cloud.langfuse.com)

Usage
─────
  from app.services.langfuse_prompts import get_compiled_prompt, PROMPT_NAMES

  text = get_compiled_prompt(
      PROMPT_NAMES.SINGLE_PARSER,
      bookmaker="Betway",
      url="https://api.betway.com/events",
      request_type="MATCH_LIST",
      sport_hint="Sport: Football",
      sample=response_raw[:6_000],
      trunc_note="\\n[TRUNCATED]",
      unified_schema=json.dumps(UNIFIED_SCHEMA, indent=2),
  )
"""

from __future__ import annotations

import os
import json
import logging
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)

# ── Langfuse client ────────────────────────────────────────────────────────────

try:
    from langfuse import Langfuse

    _lf_client = Langfuse(
        public_key=os.environ["LANGFUSE_PUBLIC_KEY"],
        secret_key=os.environ["LANGFUSE_SECRET_KEY"],
        host=os.environ.get("LANGFUSE_HOST", "https://cloud.langfuse.com"),
    )
    _LANGFUSE_OK = True
    log.info("Langfuse prompt client initialised")
except Exception as exc:
    _lf_client  = None
    _LANGFUSE_OK = False
    log.warning("Langfuse not available (%s) — falling back to hardcoded prompts", exc)


# ─── Prompt name registry ─────────────────────────────────────────────────────

@dataclass(frozen=True)
class _PromptNames:
    SINGLE_PARSER:   str = "research/single-parser"
    ML_PARSER:       str = "research/match-list-parser"
    MK_PARSER:       str = "research/markets-parser"
    COMBINE_PARSERS: str = "research/combine-parsers"

PROMPT_NAMES = _PromptNames()


# ─── Fallback prompt templates (used when Langfuse is unavailable) ────────────
# These are the source-of-truth strings you should paste into the Langfuse UI.
# Variable placeholders use {{variable_name}} syntax.

_FALLBACK_PROMPTS: dict[str, str] = {

    # ── research/single-parser ────────────────────────────────────────────────
    # Variables: bookmaker, url, request_type, sport_hint, sample,
    #            trunc_note, unified_schema, parent_match_id_hint
    PROMPT_NAMES.SINGLE_PARSER: """You are building a Python parser for an odds aggregation platform.

Bookmaker  : {{bookmaker}}
Endpoint   : {{url}}
Type       : {{request_type}}
{{sport_hint}}

API RESPONSE:
{{sample}}{{trunc_note}}

UNIFIED OUTPUT SCHEMA — every dict in the returned list MUST have ALL keys:
{{unified_schema}}

RULES:
1. Write `def parse_data(raw_data):` — raw_data is the already-parsed Python object.
2. Return a list of dicts. Every dict MUST contain every key in the schema above.
3. parent_match_id MUST be the bookmaker's internal match/event ID (int or str). Never None. (Use {{parent_match_id_hint}} from the response).
4. home_team and away_team MUST be non-empty strings. Never None.
5. market and selection MUST be non-empty strings. Never None.
6. price MUST be a float > 1.0. Skip rows where price is missing or <= 1.0.
7. start_time must be ISO 8601 string or None.
8. specifier is None for standard markets (1X2), a string like "2.5" for O/U.
9. Wrap every nested access in try/except. Return [] on any top-level error.
10. No imports — json is in scope.
11. Extract EVERY market and selection from EVERY match, not just the first.

Return ONLY raw Python — no markdown fences, no explanation.""",

    # ── research/match-list-parser ────────────────────────────────────────────
    # Variables: bookmaker, sport_hint, ml_schema, key_contract, base_rules,
    #            parent_match_id_hint
    PROMPT_NAMES.ML_PARSER: """{{base_rules}}
{{key_contract}}
MATCH LIST SCHEMA (structure of the raw JSON you will receive):
{{ml_schema}}

Write ONE function — no other code:
def parse_match_list(raw_data):
    # raw_data is the already-parsed Python object (dict/list)
    # Return a flat list of dicts — every dict must have ALL required keys above
    # parent_match_id = the bookmaker's own match/event ID from this response. Use {{parent_match_id_hint}}.
    # Extract every market and selection from every match — not just the first""",

    # ── research/markets-parser ───────────────────────────────────────────────
    # Variables: bookmaker, sport_hint, ml_schema_preview, mk_schema,
    #            key_contract, base_rules, parent_match_id_hint
    PROMPT_NAMES.MK_PARSER: """{{base_rules}}
{{key_contract}}
For context, the match-list endpoint has this structure (to identify parent_match_id):
{{ml_schema_preview}}

MARKETS SCHEMA (structure of the markets raw JSON you will receive):
{{mk_schema}}

Write ONE function — no other code:
def parse_markets(raw_data):
    # raw_data is the already-parsed Python object (dict/list)
    # Return a flat list of dicts — every dict must have ALL required keys above
    # parent_match_id must match the same bookmaker match ID as parse_match_list. Use {{parent_match_id_hint}}.
    # Extract EVERY market and selection — not just the first""",

    # ── research/combine-parsers ──────────────────────────────────────────────
    # Variables: ml_code, mk_code, key_contract
    PROMPT_NAMES.COMBINE_PARSERS: """You are combining two Python parser functions into a single unified parser for a sports odds platform.

FUNCTION A — parse_match_list (extracts match metadata + odds from match-list endpoint):
{{ml_code}}

FUNCTION B — parse_markets (extracts odds from per-match markets endpoint):
{{mk_code}}

{{key_contract}}

Write ONE combined function with this exact signature:
def parse_data(match_list_raw, markets_raw):
    # match_list_raw — already-parsed Python object from the match-list endpoint
    # markets_raw    — already-parsed Python object from the markets endpoint
    # Call parse_match_list(match_list_raw) and parse_markets(markets_raw)
    # Merge results: for each row from parse_markets, find the matching row
    #   from parse_match_list by parent_match_id and fill in any missing fields
    #   (home_team, away_team, start_time, sport, competition)
    # Return a FLAT list of dicts — every dict must have ALL required keys above
    # The function must call parse_match_list and parse_markets internally
    # Wrap everything in try/except, return [] on any error

Return ONLY raw Python — the module must contain:
  1. parse_match_list  (unchanged from Function A)
  2. parse_markets     (unchanged from Function B)
  3. parse_data        (the new combined function)
No markdown, no explanation.""",
}


# ─── Public API ───────────────────────────────────────────────────────────────

def get_compiled_prompt(prompt_name: str, **variables: Any) -> str:
    """
    Fetch a prompt from Langfuse (or fall back to the hardcoded template),
    compile {{variable}} placeholders, and return the final string.

    Parameters
    ──────────
    prompt_name : one of PROMPT_NAMES.*
    **variables : keyword arguments matching the {{placeholder}} names

    Returns
    ───────
    Compiled prompt string ready to send to the LLM.

    Raises
    ──────
    KeyError  if prompt_name is not found in fallbacks and Langfuse is down.
    """
    raw_template: str | None = None

    # ── Try Langfuse first ────────────────────────────────────────────────
    if _LANGFUSE_OK and _lf_client:
        try:
            lf_prompt  = _lf_client.get_prompt(prompt_name)
            raw_template = lf_prompt.prompt           # text/plain prompts
            # For chat prompts use: lf_prompt.compile(**variables)
            log.debug("Langfuse prompt '%s' fetched (v%s)", prompt_name, lf_prompt.version)
        except Exception as exc:
            log.warning("Langfuse fetch failed for '%s': %s — using fallback", prompt_name, exc)

    # ── Fall back to hardcoded template ───────────────────────────────────
    if raw_template is None:
        raw_template = _FALLBACK_PROMPTS.get(prompt_name)
        if raw_template is None:
            raise KeyError(f"Unknown prompt name: {prompt_name!r}")

    # ── Compile {{variable}} placeholders ────────────────────────────────
    compiled = raw_template
    for key, value in variables.items():
        compiled = compiled.replace("{{" + key + "}}", str(value) if value is not None else "")

    return compiled.strip()


def list_prompt_names() -> list[str]:
    """Return all registered prompt names."""
    return [
        PROMPT_NAMES.SINGLE_PARSER,
        PROMPT_NAMES.ML_PARSER,
        PROMPT_NAMES.MK_PARSER,
        PROMPT_NAMES.COMBINE_PARSERS,
    ]


# ─── Langfuse dashboard setup guide ──────────────────────────────────────────
# Run this once to push the fallback prompts into Langfuse programmatically.

def push_prompts_to_langfuse() -> None:
    """
    One-time utility: create/update all prompts in the Langfuse project.
    Run from a Flask shell:  python -c "from app.services.langfuse_prompts import push_prompts_to_langfuse; push_prompts_to_langfuse()"
    """
    if not _LANGFUSE_OK or not _lf_client:
        raise RuntimeError("Langfuse not configured — set LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY")

    for name, template in _FALLBACK_PROMPTS.items():
        _lf_client.create_prompt(
            name=name,
            prompt=template,
            is_active=True,
        )
        print(f"✓ Pushed prompt: {name}")


# ─── Variable reference  (for documentation / dashboard setup) ───────────────
#
# research/single-parser
#   bookmaker            : str   — bookmaker name e.g. "Betway"
#   url                  : str   — endpoint URL
#   request_type         : str   — "MATCH_LIST" | "ODDS" | "MARKETS" | …
#   sport_hint           : str   — "Sport: Football" or ""
#   sample               : str   — first 6 000 chars of response_raw
#   trunc_note           : str   — "\n[TRUNCATED]" or ""
#   unified_schema       : str   — JSON.dumps(UNIFIED_SCHEMA)
#   parent_match_id_hint : str   — e.g. "raw_data['event_id']" or "item['id']"
#
# research/match-list-parser
#   bookmaker            : str
#   sport_hint           : str
#   ml_schema            : str   — _schema_sample(ml_raw)
#   key_contract         : str   — _KEY_CONTRACT constant
#   base_rules           : str   — _BASE_RULES constant
#   parent_match_id_hint : str   — field path chosen by operator in UI
#
# research/markets-parser
#   bookmaker            : str
#   sport_hint           : str
#   ml_schema_preview    : str   — ml_schema[:300]
#   mk_schema            : str   — _schema_sample(mk_raw)
#   key_contract         : str   — _KEY_CONTRACT constant
#   base_rules           : str   — _BASE_RULES constant
#   parent_match_id_hint : str   — same hint used for ML parser
#
# research/combine-parsers
#   ml_code              : str   — validated parse_match_list() source
#   mk_code              : str   — validated parse_markets() source
#   key_contract         : str   — _KEY_CONTRACT constant