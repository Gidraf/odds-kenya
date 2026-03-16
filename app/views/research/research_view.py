"""
Endpoint Research API  —  /research
=====================================
Step-based harvest workflow builder.  No AI parsers.

Instead of generating Python code, the operator configures a series of
"harvest steps" that describe *how the agent should navigate the API*:

  Step 1 — FETCH_LIST     fetch match list → extract array → record fields
  Step 2 — FETCH_PER_ITEM for each match, call {{match_id}}/odds → extract
  Step n — FETCH_PER_ITEM for each match, call {{match_id}}/lineup → extract
  …

Steps are stored in HarvestWorkflow.steps_json and executed at harvest time
by the agent (see harvest_tasks.py / workflow_runner.py).

Register:
    from app.api.research_api import bp_research
    app.register_blueprint(bp_research)

Routes
──────
GET  /research/meta
POST /research/probe                    raw HTTP probe (curl-like)
POST /research/extract-first-item      probe match-list → return first array item
POST /research/preview-step            probe a per-item URL (vars substituted)
POST /research/detect-fields           auto-detect field paths from JSON sample
POST /research/save-workflow           upsert full HarvestWorkflow
GET  /research/workflows               list workflows (filter by bookmaker)
GET  /research/workflows/<wid>         get single workflow + steps
DELETE /research/workflows/<wid>       delete workflow
PUT  /research/workflows/<wid>/step/<n> update one step in-place
POST /research/test-workflow           dry-run workflow against live endpoints
     → streams NDJSON progress lines
"""

from __future__ import annotations

import json
import re
import shlex
import time
import traceback
from datetime import datetime, timezone
from typing import Any

from flask import Blueprint, request, jsonify, Response, stream_with_context
from app.extensions import db
from app.models.bookmakers_model import Bookmaker
from app.models.harvest_workflow import HarvestWorkflow, HarvestWorkflowStep
from app.utils.probe_curl import _probe  # see model below
from . import bp_research

from curl_cffi import requests as cffi_requests
from curl_cffi.requests.exceptions import RequestException, Timeout


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

HTTP_METHODS   = ["GET", "POST", "PUT", "PATCH", "DELETE"]

# Step types the operator can configure
STEP_TYPES = [
    "FETCH_LIST",        # top-level array of matches
    "FETCH_PER_ITEM",    # one call per item from previous step (uses {{vars}})
    "FETCH_ONCE",        # single call, no looping (e.g. static competition list)
]

# Well-known field roles — used for canonical merging and downstream services
FIELD_ROLES = [
    # Match-level
    "match_id",          # bookmaker's internal ID (used in URL templates)
    "parent_match_id",   # betradar / cross-bookmaker canonical ID (optional)
    "home_team",
    "away_team",
    "start_time",
    "sport",
    "competition",
    # Market-level
    "market_name",
    "specifier",
    "selection_name",
    "selection_price",
    # Lineup / detail
    "home_lineup",
    "away_lineup",
    "home_form",
    "away_form",
    "match_status",
    "score_home",
    "score_away",
    # Meta / notification triggers
    "lineup_confirmed",  # bool — triggers lineup email when True
    "kickoff_in_mins",   # int  — used by notification scheduler
    # Generic / custom
    "custom",
]

# Regex that matches template variables like {{match_id}}
_VAR_RE = re.compile(r'\{\{(\w+)\}\}')


# ─────────────────────────────────────────────────────────────────────────────
# Low-level helpers
# ─────────────────────────────────────────────────────────────────────────────

def _try_json(text: str | None) -> Any:
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _resolve_path(obj: Any, path: str) -> Any:
    """
    Resolve a dot-path (possibly with [] array notation) against obj.

    "events"                → obj["events"]
    "teams.home.name"       → obj["teams"]["home"]["name"]
    "outcomes[0].name"      → obj["outcomes"][0]["name"]
    "outcomes[].name"       → [item["name"] for item in obj["outcomes"]]
    """
    if not path or obj is None:
        return obj

    def _walk(cur: Any, parts: list[str]) -> Any:
        if not parts:
            return cur
        tok = parts[0]
        rest = parts[1:]

        # Array wildcard: key[] or key[*]
        m = re.match(r'^(\w+)\[\]$', tok) or re.match(r'^(\w+)\[\*\]$', tok)
        if m:
            key = m.group(1)
            arr = cur.get(key, []) if isinstance(cur, dict) else []
            if not isinstance(arr, list):
                return []
            return [_walk(item, rest) for item in arr]

        # Specific index: key[0]
        m = re.match(r'^(\w+)\[(\d+)\]$', tok)
        if m:
            key, idx = m.group(1), int(m.group(2))
            arr = cur.get(key, []) if isinstance(cur, dict) else []
            if not isinstance(arr, list) or idx >= len(arr):
                return None
            return _walk(arr[idx], rest)

        # Plain key
        if isinstance(cur, dict):
            return _walk(cur.get(tok), rest)
        if isinstance(cur, list):
            return [_walk(item, rest) for item in cur if isinstance(item, dict)]
        return None

    parts = re.split(r'\.(?![^\[]*\])', path)  # split on dot not inside brackets
    return _walk(obj, parts)


def _first_array(obj: Any) -> tuple[str | None, list]:
    """Find the first top-level key whose value is a non-empty list of dicts."""
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        return None, obj
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return k, v
    return None, []


def _to_schema(obj: Any, prefix: str = "", depth: int = 0, max_depth: int = 6) -> list[str]:
    """Return dot-path → type lines (used as a schema summary for the UI)."""
    lines: list[str] = []
    if depth > max_depth:
        return lines
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else k
            lines += _to_schema(v, p, depth + 1, max_depth)
    elif isinstance(obj, list):
        if obj:
            lines += _to_schema(obj[0], f"{prefix}[]", depth + 1, max_depth)
        else:
            lines.append(f"{prefix}[] = []")
    else:
        t = type(obj).__name__ if obj is not None else "null"
        lines.append(f"{prefix} = {t}")
    return lines


def _guess_role(key: str, value: Any) -> str | None:
    """Heuristic: map a JSON key name to a FIELD_ROLE string."""
    k = key.lower()
    hints: list[tuple[re.Pattern, str]] = [
        (re.compile(r'^(event_id|match_id|game_id|fixture_id|id)$'), "match_id"),
        (re.compile(r'betradar|parent|canonical'), "parent_match_id"),
        (re.compile(r'home.*team|home.*name|home_side|^home$|team1'), "home_team"),
        (re.compile(r'away.*team|away.*name|away_side|^away$|team2|visitor'), "away_team"),
        (re.compile(r'kickoff|kick_off|start_time|start_date|scheduled|begins_at|^date$|^time$'), "start_time"),
        (re.compile(r'^sport'), "sport"),
        (re.compile(r'league|competition|tournament|cup'), "competition"),
        (re.compile(r'market.*name|market.*type|bet.*type|^market$'), "market_name"),
        (re.compile(r'specifier|handicap|spread|^line$'), "specifier"),
        (re.compile(r'selection.*name|outcome.*name|^selection$|^outcome$|runner_name'), "selection_name"),
        (re.compile(r'^(odds|price|decimal|coefficient|rate)$'), "selection_price"),
        (re.compile(r'lineup|squad|starting.*xi'), "home_lineup"),
        (re.compile(r'form|last_[0-9]'), "home_form"),
        (re.compile(r'status|state'), "match_status"),
        (re.compile(r'score.*home|home.*score'), "score_home"),
        (re.compile(r'score.*away|away.*score'), "score_away"),
        (re.compile(r'lineup.*confirm|confirmed'), "lineup_confirmed"),
    ]
    for pattern, role in hints:
        if pattern.search(k):
            return role
    return None


def _auto_detect_fields(obj: Any) -> list[dict]:
    """
    Walk a single JSON object (one array item) and return field suggestions:
    [{"path": "teams.home.name", "role": "home_team", "sample": "Arsenal"}, …]
    """
    results: list[dict] = []

    def walk(cur: Any, prefix: str, depth: int = 0):
        if depth > 5 or not isinstance(cur, dict):
            return
        for k, v in cur.items():
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                walk(v, path, depth + 1)
            elif isinstance(v, list) and v and isinstance(v[0], dict):
                pass  # skip nested arrays at auto-detect (handled as child steps)
            else:
                role = _guess_role(k, v)
                sample = str(v)[:60] if v is not None else None
                results.append({"path": path, "role": role, "sample": sample, "key": k})

    walk(obj if isinstance(obj, dict) else {}, "")
    return results


def _substitute_vars(url_template: str, vars_dict: dict) -> str:
    """Replace {{var}} placeholders in a URL template with actual values."""
    def replace(m: re.Match) -> str:
        key = m.group(1)
        return str(vars_dict.get(key, m.group(0)))
    return _VAR_RE.sub(replace, url_template)


def _extract_vars_from_template(template: str) -> list[str]:
    """Return list of variable names found in {{...}} placeholders."""
    return _VAR_RE.findall(template)


def _build_curl_repr(method: str, url: str, headers: dict, params: dict) -> str:
    param_str = "&".join(f"{k}={v}" for k, v in params.items()) if params else ""
    full_url  = url + ("?" + param_str if param_str else "")
    header_flags = " ".join(
        f"-H {shlex.quote(f'{k}: {v}')}" for k, v in headers.items()
    )
    return f"curl -X {method.upper()} {header_flags} {shlex.quote(full_url)}"


_STRIP_HEADERS = frozenset({
    ":method", ":path", ":scheme", ":authority",
    "content-length", "host", "connection",
    "sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site",
})


def _do_probe(
    url: str,
    method: str = "GET",
    headers: dict | None = None,
    params: dict | None = None,
    body: Any = None,
    impersonate: str = "chrome",
    timeout: int = 15,
) -> dict:
    """Execute an HTTP request and return a normalised result dict."""
    headers = {k: v for k, v in (headers or {}).items() if k.lower() not in _STRIP_HEADERS}
    t0 = time.monotonic()
    try:
        resp = cffi_requests.request(
            method=method.upper(), url=url,
            headers=headers, params=params or {},
            data=body if isinstance(body, str) else None,
            json=body if isinstance(body, dict) else None,
            timeout=timeout, allow_redirects=True, impersonate=impersonate,
        )
        latency_ms  = round((time.monotonic() - t0) * 1000)
        raw_text    = resp.text[:100_000]
        parsed_json = _try_json(raw_text)
        return {
            "ok":           resp.ok,
            "status":       resp.status_code,
            "response_raw": raw_text,
            "parsed":       parsed_json,
            "content_type": resp.headers.get("Content-Type", ""),
            "size_bytes":   len(resp.content),
            "latency_ms":   latency_ms,
            "error":        None,
        }
    except Timeout:
        return {"ok": False, "status": None, "response_raw": "", "parsed": None,
                "content_type": "", "size_bytes": 0, "latency_ms": 0,
                "error": f"Timeout after {timeout}s"}
    except RequestException as exc:
        return {"ok": False, "status": None, "response_raw": "", "parsed": None,
                "content_type": "", "size_bytes": 0, "latency_ms": 0,
                "error": str(exc)}
    except Exception as exc:
        return {"ok": False, "status": None, "response_raw": "", "parsed": None,
                "content_type": "", "size_bytes": 0, "latency_ms": 0,
                "error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
# Serialisers
# ─────────────────────────────────────────────────────────────────────────────

def _workflow_dict(wf: HarvestWorkflow, include_steps: bool = True) -> dict:
    d = {
        "id":              wf.id,
        "bookmaker_id":    wf.bookmaker_id,
        "bookmaker_name":  wf.bookmaker.name if wf.bookmaker else None,
        "name":            wf.name,
        "description":     wf.description,
        "is_active":       wf.is_active,
        "created_at":      wf.created_at.isoformat() if wf.created_at else None,
        "updated_at":      wf.updated_at.isoformat() if wf.updated_at else None,
    }
    if include_steps:
        d["steps"] = [_step_dict(s) for s in sorted(wf.steps, key=lambda s: s.position)]
    return d


def _step_dict(s: HarvestWorkflowStep) -> dict:
    return {
        "id":               s.id,
        "position":         s.position,
        "name":             s.name,
        "step_type":        s.step_type,
        "url_template":     s.url_template,
        "method":           s.method,
        "headers_json":     s.headers_json,
        "params_json":      s.params_json,
        "body_template":    s.body_template,
        "result_array_path":s.result_array_path,
        "fields_json":      s.fields_json,
        "depends_on_pos":   s.depends_on_pos,
        "enabled":          s.enabled,
        "notes":            s.notes,
        # parsed helpers
        "headers":          _try_json(s.headers_json) or {},
        "params":           _try_json(s.params_json) or {},
        "fields":           _try_json(s.fields_json) or [],
        "template_vars":    _extract_vars_from_template(s.url_template or ""),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Route 1 — META
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/meta", methods=["GET"])
def research_meta():
    bookmakers = Bookmaker.query.filter_by(is_active=True).order_by(Bookmaker.name).all()
    sports = []
    try:
        from app.models.competions_model import Sport
        sports = [{"id": s.id, "name": s.name} for s in Sport.query.filter_by(is_active=True).all()]
    except Exception:
        pass
    return jsonify({
        "bookmakers":  [{"id": b.id, "name": b.name or b.domain, "domain": b.domain} for b in bookmakers],
        "sports":      sports,
        "http_methods":HTTP_METHODS,
        "step_types":  STEP_TYPES,
        "field_roles": FIELD_ROLES,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Route 2 — PROBE  (raw HTTP, no parsing)
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/probe", methods=["POST"])
def probe():
    """
    Fire a raw HTTP request and return the response.
    Used for Step 1 (match list) and any subsequent step previews.
    """
    data = request.json or {}
    url  = (data.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "url is required"}), 400

    result = _probe(
        url         = url,
        method      = data.get("method", "GET"),
        headers     = data.get("headers") or {},
        params      = data.get("params")  or {},
        body        = data.get("body"),
        # impersonate = data.get("impersonate", "chrome"),
    )

    # Attach schema summary if we got JSON
    if result["parsed"] is not None:
        arr_key, items = _first_array(result["parsed"])
        result["array_key"]    = arr_key
        result["array_length"] = len(items)
        result["first_item"]   = items[0] if items else None
        result["schema_lines"] = _to_schema(items[0] if items else result["parsed"])
    else:
        result["array_key"]    = None
        result["array_length"] = 0
        result["first_item"]   = None
        result["schema_lines"] = []

    result["curl"] = _build_curl_repr(
        data.get("method", "GET"), url,
        data.get("headers") or {}, data.get("params") or {}
    )
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# Route 3 — EXTRACT FIRST ITEM
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/extract-first-item", methods=["POST"])
def extract_first_item():
    """
    Probe the match-list endpoint, locate the first item in the result array,
    auto-detect field paths, and return everything the UI needs to:

      a) show the first match as a reference object
      b) pre-fill field role suggestions
      c) let the operator build a per-item URL using {{field_name}} tokens

    Body:
      {url, method, headers, params, body, array_path?}

    Returns:
      {ok, first_item, detected_fields, schema_lines, array_key, array_length,
       suggested_id_field}
    """
    data       = request.json or {}
    url        = (data.get("url") or "").strip()
    array_path = (data.get("array_path") or "").strip()

    if not url:
        return jsonify({"ok": False, "error": "url is required"}), 400

    result = _do_probe(
        url     = url,
        method  = data.get("method", "GET"),
        headers = data.get("headers") or {},
        params  = data.get("params") or {},
        body    = data.get("body"),
    )

    if not result["ok"] or result["parsed"] is None:
        return jsonify({
            "ok":    False,
            "error": result["error"] or f"HTTP {result['status']} — not JSON",
            "status":result["status"],
        })

    # Locate the array
    parsed = result["parsed"]
    if array_path:
        items = _resolve_path(parsed, array_path)
        if not isinstance(items, list):
            items = [items] if items else []
        arr_key = array_path
    else:
        arr_key, items = _first_array(parsed)

    first = items[0] if items else (parsed if isinstance(parsed, dict) else {})

    detected = _auto_detect_fields(first)
    schema   = _to_schema(first)

    # Find the most likely match_id field to suggest for URL templates
    id_suggestion = next((f["path"] for f in detected if f["role"] == "match_id"), None)
    if not id_suggestion and detected:
        id_suggestion = detected[0]["path"]

    return jsonify({
        "ok":               True,
        "first_item":       first,
        "detected_fields":  detected,
        "schema_lines":     schema,
        "array_key":        arr_key,
        "array_length":     len(items),
        "suggested_id_field": id_suggestion,
        "status":           result["status"],
        "latency_ms":       result["latency_ms"],
        "size_bytes":       result["size_bytes"],
    })


# ─────────────────────────────────────────────────────────────────────────────
# Route 4 — PREVIEW STEP  (per-item URL with vars substituted from first item)
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/preview-step", methods=["POST"])
def preview_step():
    """
    Given a URL template + a reference match object, substitute template vars,
    fire the real HTTP request, and return the response + detected fields.

    This lets the operator paste e.g.:
        https://api.bookmaker.com/events/{{match_id}}/odds
    and immediately see what comes back for the first match.

    Body:
      {
        url_template:  "https://…/{{match_id}}/odds",
        method:        "GET",
        headers:       {...},
        params:        {...},
        reference_item: { "match_id": 12345, "home": "Arsenal", … },
        field_map:      [{"path": "id", "role": "match_id"}, …]
      }
    """
    data           = request.json or {}
    url_template   = (data.get("url_template") or "").strip()
    reference_item = data.get("reference_item") or {}
    field_map      = data.get("field_map") or []   # [{path, role}, …]

    if not url_template:
        return jsonify({"ok": False, "error": "url_template is required"}), 400

    # Build a vars dict from the reference item using the field_map
    vars_dict: dict[str, Any] = {}

    # First, add all top-level keys of the reference item directly (by role name)
    for entry in field_map:
        path = entry.get("path", "")
        role = entry.get("role", "")
        if role and path:
            val = _resolve_path(reference_item, path)
            if val is not None and not isinstance(val, (list, dict)):
                vars_dict[role] = val
                # Also map the last segment of the path (e.g. "id" from "event.id")
                vars_dict[path.split(".")[-1].rstrip("[]")] = val

    # Also expose every top-level scalar key directly
    for k, v in reference_item.items():
        if not isinstance(v, (list, dict)):
            vars_dict.setdefault(k, v)

    # Discover what vars the template needs
    needed_vars = _extract_vars_from_template(url_template)
    missing     = [v for v in needed_vars if v not in vars_dict]

    resolved_url = _substitute_vars(url_template, vars_dict)

    if "{{" in resolved_url:
        return jsonify({
            "ok":           False,
            "error":        f"Could not resolve template variables: {missing}",
            "resolved_url": resolved_url,
            "available_vars": list(vars_dict.keys()),
            "needed_vars":   needed_vars,
        })

    result = _do_probe(
        url     = resolved_url,
        method  = data.get("method", "GET"),
        headers = data.get("headers") or {},
        params  = data.get("params") or {},
        body    = data.get("body"),
    )

    if result["parsed"] is not None:
        arr_key, items = _first_array(result["parsed"])
        first          = items[0] if items else (result["parsed"] if isinstance(result["parsed"], dict) else {})
        detected       = _auto_detect_fields(first)
        result["array_key"]    = arr_key
        result["array_length"] = len(items)
        result["first_item"]   = first
        result["detected_fields"] = detected
        result["schema_lines"] = _to_schema(first)
    else:
        result["array_key"]    = None
        result["array_length"] = 0
        result["first_item"]   = None
        result["detected_fields"] = []
        result["schema_lines"] = []

    result["resolved_url"]  = resolved_url
    result["vars_used"]     = vars_dict
    result["needed_vars"]   = needed_vars
    result["missing_vars"]  = missing
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
# Route 5 — DETECT FIELDS  (from a raw JSON sample)
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/detect-fields", methods=["POST"])
def detect_fields():
    """
    Given a raw JSON string (one item or full response), return auto-detected
    field suggestions.  Used when the operator pastes a response directly
    rather than probing live.

    Body: {response_raw, array_path?}
    """
    data       = request.json or {}
    raw        = (data.get("response_raw") or "").strip()
    array_path = (data.get("array_path") or "").strip()

    if not raw:
        return jsonify({"ok": False, "error": "response_raw is required"}), 400

    parsed = _try_json(raw)
    if parsed is None:
        return jsonify({"ok": False, "error": "Not valid JSON"}), 400

    if array_path:
        items = _resolve_path(parsed, array_path)
        if isinstance(items, list):
            item = items[0] if items else parsed
        else:
            item = items or parsed
    else:
        _, items = _first_array(parsed)
        item = items[0] if items else (parsed if isinstance(parsed, dict) else {})

    detected = _auto_detect_fields(item)
    schema   = _to_schema(item)

    return jsonify({
        "ok":              True,
        "detected_fields": detected,
        "schema_lines":    schema,
        "first_item":      item,
        "array_length":    len(items) if isinstance(items, list) else 0,
    })


# ─────────────────────────────────────────────────────────────────────────────
# Route 6 — SAVE WORKFLOW
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/save-workflow", methods=["POST"])
def save_workflow():
    """
    Upsert a HarvestWorkflow + its steps.

    Body:
    {
      "id": null | int,              // null = create, int = update
      "bookmaker_id": 3,
      "name": "Betika — Match List + Odds",
      "description": "…",
      "is_active": true,
      "steps": [
        {
          "position":          1,
          "name":              "Fetch match list",
          "step_type":         "FETCH_LIST",
          "url_template":      "https://api.betika.com/v1/uo/matches",
          "method":            "GET",
          "headers":           {"Authorization": "Bearer …"},
          "params":            {"status": "upcoming"},
          "body_template":     null,
          "result_array_path": "data",
          "fields": [
            {"path": "id",           "role": "match_id",   "label": "Match ID"},
            {"path": "home_team",    "role": "home_team",  "label": "Home"},
            {"path": "away_team",    "role": "away_team",  "label": "Away"},
            {"path": "start_time",   "role": "start_time", "label": "Kick-off"},
            {"path": "sport.name",   "role": "sport",      "label": "Sport"},
            {"path": "league.name",  "role": "competition","label": "League"}
          ],
          "depends_on_pos": null,
          "enabled": true,
          "notes": ""
        },
        {
          "position":          2,
          "name":              "Fetch odds per match",
          "step_type":         "FETCH_PER_ITEM",
          "url_template":      "https://api.betika.com/v1/uo/matches/{{match_id}}/odds",
          "method":            "GET",
          "headers":           {},
          "params":            {},
          "body_template":     null,
          "result_array_path": "markets",
          "fields": [
            {"path": "name",             "role": "market_name",     "label": "Market"},
            {"path": "specifier",        "role": "specifier",       "label": "Specifier"},
            {"path": "outcomes[].name",  "role": "selection_name",  "label": "Selection"},
            {"path": "outcomes[].odds",  "role": "selection_price", "label": "Price"}
          ],
          "depends_on_pos": 1,
          "enabled": true,
          "notes": "Uses {{match_id}} from step 1"
        }
      ]
    }
    """
    data = request.json or {}
    bk_id = data.get("bookmaker_id")
    if not bk_id:
        return jsonify({"ok": False, "error": "bookmaker_id is required"}), 400

    steps_data = data.get("steps") or []
    if not steps_data:
        return jsonify({"ok": False, "error": "At least one step is required"}), 400

    # Validate step types
    invalid_types = [s.get("step_type") for s in steps_data if s.get("step_type") not in STEP_TYPES]
    if invalid_types:
        return jsonify({
            "ok": False,
            "error": f"Invalid step_type values: {invalid_types}. Must be one of {STEP_TYPES}",
        }), 400

    try:
        wf_id = data.get("id")

        if wf_id:
            wf = HarvestWorkflow.query.get(wf_id)
            if not wf:
                return jsonify({"ok": False, "error": f"Workflow #{wf_id} not found"}), 404
            # Clear existing steps before re-inserting
            for s in wf.steps:
                db.session.delete(s)
            db.session.flush()
        else:
            wf = HarvestWorkflow(bookmaker_id=int(bk_id))
            db.session.add(wf)

        wf.bookmaker_id = int(bk_id)
        wf.name         = (data.get("name") or "").strip() or f"Workflow #{bk_id}"
        wf.description  = data.get("description") or ""
        wf.is_active    = bool(data.get("is_active", True))
        wf.updated_at   = datetime.now(timezone.utc)
        if not wf.created_at:
            wf.created_at = datetime.now(timezone.utc)

        db.session.flush()  # get wf.id

        for step_d in steps_data:
            fields = step_d.get("fields") or []
            s = HarvestWorkflowStep(
                workflow_id       = wf.id,
                position          = int(step_d.get("position", 1)),
                name              = (step_d.get("name") or "").strip(),
                step_type         = step_d.get("step_type", "FETCH_LIST"),
                url_template      = (step_d.get("url_template") or "").strip(),
                method            = (step_d.get("method") or "GET").upper(),
                headers_json      = json.dumps(step_d.get("headers") or {}),
                params_json       = json.dumps(step_d.get("params") or {}),
                body_template     = step_d.get("body_template"),
                result_array_path = (step_d.get("result_array_path") or "").strip() or None,
                fields_json       = json.dumps(fields),
                depends_on_pos    = step_d.get("depends_on_pos"),
                enabled           = bool(step_d.get("enabled", True)),
                notes             = step_d.get("notes") or "",
            )
            db.session.add(s)

        db.session.commit()

        return jsonify({
            "ok":          True,
            "workflow_id": wf.id,
            "step_count":  len(steps_data),
            "msg":         f"Workflow '{wf.name}' saved with {len(steps_data)} step(s).",
        }), 201

    except Exception as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Route 7 — LIST WORKFLOWS
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/workflows", methods=["GET"])
def list_workflows():
    bk_id = request.args.get("bookmaker_id", type=int)
    q     = HarvestWorkflow.query
    if bk_id:
        q = q.filter_by(bookmaker_id=bk_id)
    workflows = q.order_by(HarvestWorkflow.bookmaker_id, HarvestWorkflow.name).all()
    return jsonify([_workflow_dict(wf, include_steps=False) for wf in workflows])


# ─────────────────────────────────────────────────────────────────────────────
# Route 8 — GET / DELETE single workflow
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/workflows/<int:wid>", methods=["GET"])
def get_workflow(wid: int):
    wf = HarvestWorkflow.query.get_or_404(wid)
    return jsonify(_workflow_dict(wf, include_steps=True))


@bp_research.route("/workflows/<int:wid>", methods=["DELETE"])
def delete_workflow(wid: int):
    wf = HarvestWorkflow.query.get_or_404(wid)
    db.session.delete(wf)
    db.session.commit()
    return jsonify({"ok": True, "deleted": wid})


# ─────────────────────────────────────────────────────────────────────────────
# Route 9 — UPDATE ONE STEP IN-PLACE
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/workflows/<int:wid>/step/<int:pos>", methods=["PUT"])
def update_step(wid: int, pos: int):
    """Patch a single step without replacing the whole workflow."""
    wf = HarvestWorkflow.query.get_or_404(wid)
    step = next((s for s in wf.steps if s.position == pos), None)
    if not step:
        return jsonify({"ok": False, "error": f"Step {pos} not found in workflow {wid}"}), 404

    d = request.json or {}
    if "name"              in d: step.name              = d["name"]
    if "step_type"         in d: step.step_type         = d["step_type"]
    if "url_template"      in d: step.url_template      = d["url_template"]
    if "method"            in d: step.method            = d["method"].upper()
    if "headers"           in d: step.headers_json      = json.dumps(d["headers"])
    if "params"            in d: step.params_json       = json.dumps(d["params"])
    if "body_template"     in d: step.body_template     = d["body_template"]
    if "result_array_path" in d: step.result_array_path = d["result_array_path"] or None
    if "fields"            in d: step.fields_json       = json.dumps(d["fields"])
    if "depends_on_pos"    in d: step.depends_on_pos    = d["depends_on_pos"]
    if "enabled"           in d: step.enabled           = bool(d["enabled"])
    if "notes"             in d: step.notes             = d["notes"]

    wf.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "step": _step_dict(step)})


# ─────────────────────────────────────────────────────────────────────────────
# Route 10 — TEST WORKFLOW  (dry-run, NDJSON stream)
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/test-workflow", methods=["POST"])
def test_workflow():
    """
    Execute a workflow's steps against live endpoints and stream progress.

    Stops after the first item in any FETCH_PER_ITEM step to avoid
    hammering the API.

    Body: {workflow_id} or inline {steps, bookmaker_id}

    Streams newline-delimited JSON events:
      {"type": "step_start",    "step": 1, "name": "…"}
      {"type": "step_done",     "step": 1, "items": 42, "fields_preview": […]}
      {"type": "merge_preview", "merged_row": {…}}
      {"type": "step_error",    "step": 1, "error": "…"}
      {"type": "done",          "total_rows_preview": 5}
    """
    data = request.json or {}
    wf_id = data.get("workflow_id")

    if wf_id:
        wf = HarvestWorkflow.query.get(wf_id)
        if not wf:
            return jsonify({"ok": False, "error": f"Workflow #{wf_id} not found"}), 404
        steps_data = [_step_dict(s) for s in sorted(wf.steps, key=lambda s: s.position)]
    else:
        steps_data = data.get("steps") or []

    if not steps_data:
        return jsonify({"ok": False, "error": "No steps provided"}), 400

    def _emit(obj: dict) -> bytes:
        return (json.dumps(obj, default=str) + "\n").encode()

    @stream_with_context
    def _generate():
        # context_stack[position] = list of first N items from that step
        context_stack: dict[int, list[dict]] = {}
        merged_rows:   list[dict] = []

        for step_d in sorted(steps_data, key=lambda s: s.get("position", 1)):
            pos       = step_d.get("position", 1)
            step_type = step_d.get("step_type", "FETCH_LIST")
            name      = step_d.get("name", f"Step {pos}")
            url_tmpl  = step_d.get("url_template") or step_d.get("url") or ""
            method    = step_d.get("method", "GET")
            headers   = step_d.get("headers") or (_try_json(step_d.get("headers_json")) or {})
            params    = step_d.get("params")  or (_try_json(step_d.get("params_json"))  or {})
            fields    = step_d.get("fields")  or (_try_json(step_d.get("fields_json"))  or [])
            arr_path  = step_d.get("result_array_path") or ""
            dep_pos   = step_d.get("depends_on_pos")

            if not step_d.get("enabled", True):
                yield _emit({"type": "step_skip", "step": pos, "name": name, "reason": "disabled"})
                continue

            yield _emit({"type": "step_start", "step": pos, "name": name, "step_type": step_type})

            # ── FETCH_LIST ──────────────────────────────────────────────
            if step_type == "FETCH_LIST":
                if "{{" in url_tmpl:
                    yield _emit({"type": "step_error", "step": pos, "name": name,
                                 "error": "FETCH_LIST URL should not contain template vars"})
                    continue

                result = _do_probe(url_tmpl, method, headers, params)
                if not result["ok"]:
                    yield _emit({"type": "step_error", "step": pos, "name": name,
                                 "error": result["error"] or f"HTTP {result['status']}"})
                    continue

                parsed = result["parsed"]
                if arr_path:
                    items = _resolve_path(parsed, arr_path)
                    if not isinstance(items, list):
                        items = [items] if items else []
                else:
                    _, items = _first_array(parsed or {})

                # Extract field values for first 3 items
                preview_rows = []
                for item in (items[:3] if items else []):
                    row: dict = {}
                    for f in fields:
                        val = _resolve_path(item, f["path"])
                        if val is not None and not isinstance(val, (list, dict)):
                            row[f.get("role") or f["path"]] = val
                    preview_rows.append(row)

                context_stack[pos] = items[:5]  # keep first 5 for downstream steps
                yield _emit({
                    "type":           "step_done",
                    "step":           pos,
                    "name":           name,
                    "total_items":    len(items),
                    "fields_preview": preview_rows[:2],
                    "latency_ms":     result["latency_ms"],
                })

            # ── FETCH_PER_ITEM ───────────────────────────────────────────
            elif step_type == "FETCH_PER_ITEM":
                parent_items = context_stack.get(dep_pos or pos - 1, [])
                if not parent_items:
                    yield _emit({"type": "step_error", "step": pos, "name": name,
                                 "error": f"No items from step {dep_pos or pos-1} to iterate"})
                    continue

                # Only test against FIRST parent item (dry-run)
                parent_item = parent_items[0]

                # Build vars dict from parent item using the parent step's field roles
                parent_step = next((s for s in steps_data if s.get("position") == (dep_pos or pos - 1)), None)
                parent_fields = parent_step.get("fields") or [] if parent_step else []
                vars_dict: dict = {}
                for f in parent_fields:
                    role = f.get("role") or ""
                    path = f.get("path") or ""
                    if role and path:
                        val = _resolve_path(parent_item, path)
                        if val is not None and not isinstance(val, (list, dict)):
                            vars_dict[role] = val
                            vars_dict[path.split(".")[-1].rstrip("[]")] = val
                # Also expose all top-level scalars
                for k, v in parent_item.items():
                    if not isinstance(v, (list, dict)):
                        vars_dict.setdefault(k, v)

                resolved_url = _substitute_vars(url_tmpl, vars_dict)
                if "{{" in resolved_url:
                    missing = _extract_vars_from_template(resolved_url)
                    yield _emit({"type": "step_error", "step": pos, "name": name,
                                 "error": f"Could not resolve template vars: {missing}",
                                 "available_vars": list(vars_dict.keys())})
                    continue

                result = _do_probe(resolved_url, method, headers, params)
                yield _emit({"type": "step_probe", "step": pos, "url": resolved_url,
                             "status": result["status"], "latency_ms": result["latency_ms"]})

                if not result["ok"]:
                    yield _emit({"type": "step_error", "step": pos, "name": name,
                                 "error": result["error"] or f"HTTP {result['status']}"})
                    continue

                parsed = result["parsed"]
                if arr_path:
                    items = _resolve_path(parsed, arr_path)
                    if not isinstance(items, list):
                        items = [items] if items else []
                else:
                    _, items = _first_array(parsed or {})

                # Build preview merged row
                merged: dict = {**{
                    f.get("role") or f["path"]: _resolve_path(parent_item, f["path"])
                    for f in parent_fields
                    if _resolve_path(parent_item, f.get("path", "")) not in (None, [], {})
                       and not isinstance(_resolve_path(parent_item, f.get("path", "")), (list, dict))
                }}
                for item in (items[:2] if items else []):
                    row: dict = dict(merged)
                    for f in fields:
                        val = _resolve_path(item, f["path"])
                        if val is not None and not isinstance(val, (list, dict)):
                            row[f.get("role") or f["path"]] = val
                    merged_rows.append(row)

                context_stack[pos] = items[:5]
                yield _emit({
                    "type":           "step_done",
                    "step":           pos,
                    "name":           name,
                    "total_items":    len(items),
                    "fields_preview": [merged_rows[-1]] if merged_rows else [],
                    "resolved_url":   resolved_url,
                    "latency_ms":     result["latency_ms"],
                })

            # ── FETCH_ONCE ───────────────────────────────────────────────
            elif step_type == "FETCH_ONCE":
                result = _do_probe(url_tmpl, method, headers, params)
                if not result["ok"]:
                    yield _emit({"type": "step_error", "step": pos, "name": name,
                                 "error": result["error"] or f"HTTP {result['status']}"})
                    continue

                parsed = result["parsed"]
                preview: dict = {}
                if isinstance(parsed, dict):
                    for f in fields:
                        val = _resolve_path(parsed, f["path"])
                        if val is not None and not isinstance(val, (list, dict)):
                            preview[f.get("role") or f["path"]] = val

                yield _emit({
                    "type":           "step_done",
                    "step":           pos,
                    "name":           name,
                    "fields_preview": [preview] if preview else [],
                    "latency_ms":     result["latency_ms"],
                })

        yield _emit({
            "type":                "done",
            "total_merged_rows":   len(merged_rows),
            "merged_rows_preview": merged_rows[:5],
        })

    return Response(_generate(), content_type="application/x-ndjson")


# ─────────────────────────────────────────────────────────────────────────────
# Route 11 — RESOLVE TEMPLATE (utility — no HTTP call)
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/resolve-template", methods=["POST"])
def resolve_template():
    """
    Substitute template vars in a URL string from a reference object.
    Used by the UI to show the resolved URL before making a live call.

    Body: {url_template, reference_item, field_map?}
    """
    data         = request.json or {}
    url_template = (data.get("url_template") or "").strip()
    ref          = data.get("reference_item") or {}
    field_map    = data.get("field_map") or []

    vars_dict: dict = {}
    for f in field_map:
        role = f.get("role") or ""
        path = f.get("path") or ""
        if role and path:
            val = _resolve_path(ref, path)
            if val is not None and not isinstance(val, (list, dict)):
                vars_dict[role] = val
                vars_dict[path.split(".")[-1].rstrip("[]")] = val
    for k, v in ref.items():
        if not isinstance(v, (list, dict)):
            vars_dict.setdefault(k, v)

    resolved   = _substitute_vars(url_template, vars_dict)
    still_tmpl = _extract_vars_from_template(resolved)

    return jsonify({
        "ok":            not still_tmpl,
        "resolved_url":  resolved,
        "missing_vars":  still_tmpl,
        "used_vars":     {k: v for k, v in vars_dict.items() if k in _VAR_RE.findall(url_template)},
    })