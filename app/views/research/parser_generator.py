"""
app/api/parser_generator.py
============================
Parser generation + multi-sport live iteration for HarvestWorkflow parsers.

Routes (register via workflow_api.py on bp_research):
  POST /research/workflows/<wid>/parser/generate
       Generate parse_data() from the bookmaker's onboarding session field maps.
       Returns: {ok, code, sport_name, steps_used, session_id}

  POST /research/workflows/<wid>/parser/iterate-sports
       Execute the parser against the LIVE API for every configured sport.
       For each sport: probe list → probe markets → merge → parse → coverage check.
       Body: {code, limit_per_sport?}  (default: 3 matches per sport)
       Returns: {ok, results: [{sport_name, bk_sport_id, status,
                                item_count, row_count, coverage_pct,
                                present_markets, missing_markets, error}]}

Usage in workflow_api.py:
    from .parser_generator import generate_parser_route, iterate_sports_route
    bp_research.add_url_rule('/workflows/<int:wid>/parser/generate',
                              view_func=generate_parser_route, methods=['POST'])
    bp_research.add_url_rule('/workflows/<int:wid>/parser/iterate-sports',
                              view_func=iterate_sports_route, methods=['POST'])
"""

from __future__ import annotations

import json
import re
import time
import traceback
from datetime import datetime, timezone
from typing import Any

from curl_cffi import requests as tls_requests
from flask import request, jsonify

from app.extensions import db
from app.models.harvest_workflow import HarvestWorkflow, HarvestWorkflowStep
from app.models.onboarding_model import BookmakerOnboardingSession
from app.models.odds_model import validate_parser_row, REQUIRED_PARSER_KEYS
from app.utils.mapping_seed import get_primary_slugs_for_sport, MARKETS_BY_SPORT

# ─────────────────────────────────────────────────────────────────────────────
# Browser headers (same as onboarding_api)
# ─────────────────────────────────────────────────────────────────────────────

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _probe(
    url: str,
    method: str = "GET",
    headers: dict | None = None,
    params: dict | None = None,
    body: Any = None,
    timeout: int = 15,
) -> dict:
    """Fire a TLS-impersonated HTTP request and return structured result."""
    final_headers = {**_BROWSER_HEADERS, **(headers or {})}
    t0 = time.perf_counter()
    try:
        resp = tls_requests.request(
            method=(method or "GET").upper(),
            url=url,
            headers=final_headers,
            params=params or {},
            json=body if isinstance(body, (dict, list)) else None,
            data=body if isinstance(body, str) else None,
            impersonate="chrome120",
            timeout=timeout,
        )
        latency_ms = int((time.perf_counter() - t0) * 1000)
        raw = resp.content
        text = raw.decode("utf-8", errors="replace")
        parsed: Any = None
        for candidate in (text.strip(), re.sub(r"^\s*\w+\s*\(", "", text).rstrip(");").strip()):
            try:
                parsed = json.loads(candidate)
                break
            except Exception:
                pass
        return {
            "ok": 200 <= resp.status_code < 300 and parsed is not None,
            "status": resp.status_code,
            "latency_ms": latency_ms,
            "parsed": parsed,
            "error": None if 200 <= resp.status_code < 300 else f"HTTP {resp.status_code}",
        }
    except Exception as exc:
        return {
            "ok": False, "status": None,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "parsed": None, "error": str(exc),
        }


def _get_nested(obj: Any, path: str) -> Any:
    """Resolve a dot-path in nested dict/list."""
    if not path or obj is None:
        return obj
    for part in path.split("."):
        if isinstance(obj, dict):
            obj = obj.get(part)
        elif isinstance(obj, list):
            obj = [item.get(part) if isinstance(item, dict) else None for item in obj]
        else:
            return None
        if obj is None:
            return None
    return obj


def _extract_items(parsed: Any, array_path: str | None) -> list:
    root = _get_nested(parsed, array_path) if array_path else parsed
    if isinstance(root, list):
        return root
    if isinstance(root, dict):
        for v in root.values():
            if isinstance(v, list) and v:
                return v
    return []


def _resolve_template(
    url_template: str,
    params: dict,
    placeholder_map: dict,
    item: dict,
    field_map: dict,
) -> tuple[str, dict]:
    """Replace {{var}} in URL and params using item + field_map."""
    def resolve(var: str) -> str:
        path = placeholder_map.get(var) or field_map.get(var)
        if path:
            v = _get_nested(item, path)
            if v is not None:
                return str(v)
        if var in item:
            return str(item[var])
        return f"<{var}>"

    resolved_url = re.sub(r"\{\{(\w+)\}\}", lambda m: resolve(m.group(1)), url_template)
    resolved_params = {
        k: re.sub(r"\{\{(\w+)\}\}", lambda m: resolve(m.group(1)), str(v))
        for k, v in params.items()
    }
    return resolved_url, resolved_params


def _safe_exec_parser(code: str, sample: Any) -> dict:
    """Execute parse_data() from user code and validate rows."""
    t0 = time.monotonic()
    try:
        compiled = compile(code, "<parser>", "exec")
    except SyntaxError as exc:
        return {"ok": False, "error": f"SyntaxError line {exc.lineno}: {exc.msg}", "rows": [], "row_count": 0, "valid_count": 0, "validation_errors": [], "elapsed_ms": 0}

    ns: dict = {"__builtins__": __builtins__}
    try:
        exec(compiled, ns)
    except Exception:
        return {"ok": False, "error": traceback.format_exc(), "rows": [], "row_count": 0, "valid_count": 0, "validation_errors": [], "elapsed_ms": 0}

    parse_fn = ns.get("parse_data")
    if not callable(parse_fn):
        return {"ok": False, "error": "Function 'parse_data' not found.", "rows": [], "row_count": 0, "valid_count": 0, "validation_errors": [], "elapsed_ms": 0}

    try:
        rows = parse_fn(sample)
        elapsed_ms = round((time.monotonic() - t0) * 1000)
    except Exception:
        return {"ok": False, "error": traceback.format_exc(), "rows": [], "row_count": 0, "valid_count": 0, "validation_errors": [], "elapsed_ms": round((time.monotonic() - t0) * 1000)}

    if not isinstance(rows, list):
        return {"ok": False, "error": f"parse_data() returned {type(rows).__name__}, expected list.", "rows": [], "row_count": 0, "valid_count": 0, "validation_errors": [], "elapsed_ms": elapsed_ms}

    validation_errors: list[str] = []
    valid_rows: list = []
    for i, row in enumerate(rows):
        try:
            validate_parser_row(row)
            valid_rows.append(row)
        except (ValueError, TypeError) as exc:
            validation_errors.append(f"Row {i}: {str(exc).split(chr(10))[0][:240]}")

    # JSON-safe rows (up to 200)
    safe_rows: list = []
    for row in rows[:200]:
        safe_row: dict = {}
        for k, v in row.items():
            try:
                json.dumps(v)
                safe_row[k] = v
            except Exception:
                safe_row[k] = str(v)
        safe_rows.append(safe_row)

    return {
        "ok": len(validation_errors) == 0 and len(rows) > 0,
        "rows": safe_rows, "row_count": len(rows), "valid_count": len(valid_rows),
        "validation_errors": validation_errors[:30], "error": None, "elapsed_ms": elapsed_ms,
    }


def _check_coverage(rows: list, sport_name: str | None) -> dict:
    """Check market coverage for a set of parsed rows against mapping_seed."""
    if not rows:
        return {"ok": False, "coverage_pct": 0.0, "present_markets": [], "missing_markets": [], "extra_markets": [], "expected_count": 0, "present_count": 0}

    # Build catalogue
    seen: set[str] = set()
    catalogue: dict[str, str] = {}  # slug → name
    for sport_key, entries in MARKETS_BY_SPORT.items():
        if sport_key is not None and sport_name and sport_key.lower() != sport_name.lower():
            if sport_key is not None:
                continue
        for name, slug, _ in entries:
            if slug not in seen:
                catalogue[slug] = name
                seen.add(slug)

    primary_slugs = set(get_primary_slugs_for_sport(sport_name))

    # Normalise
    def _norm(s: str) -> str:
        return " ".join(s.lower().split())

    catalogue_norm = {_norm(name): slug for slug, name in catalogue.items()}
    catalogue_norm.update({_norm(slug.replace("_", " ")): slug for slug, name in catalogue.items()})

    present_raw: set[str] = set()
    for row in rows:
        mkt = row.get("market") or row.get("market_name") or ""
        if mkt:
            present_raw.add(str(mkt).strip())

    matched_slugs: set[str] = set()
    for m in present_raw:
        slug = catalogue_norm.get(_norm(m))
        if slug:
            matched_slugs.add(slug)

    n_expected = len(catalogue)
    n_present  = len(matched_slugs)
    coverage   = (n_present / n_expected * 100) if n_expected else 100.0

    missing = [
        {"name": catalogue[slug], "slug": slug, "is_primary": slug in primary_slugs}
        for slug in catalogue if slug not in matched_slugs
    ]
    missing.sort(key=lambda m: (0 if m["is_primary"] else 1, m["name"]))

    extra = sorted(m for m in present_raw if catalogue_norm.get(_norm(m)) is None)

    return {
        "ok": coverage >= 50.0,
        "coverage_pct": round(coverage, 1),
        "present_markets": sorted(present_raw),
        "missing_markets": missing[:50],
        "extra_markets": extra[:20],
        "expected_count": n_expected,
        "present_count": n_present,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Parser code generator
# ─────────────────────────────────────────────────────────────────────────────

def _step2_key(step: HarvestWorkflowStep) -> str:
    """Derive the nested key name from step name (mirrors buildSampleFromSteps)."""
    return "_" + re.sub(r"[^a-z0-9]+", "_", step.name.lower()).strip("_") or f"step{step.position}"


def _field_path(field_map: dict | None, role: str, fallback: str) -> str:
    if not field_map:
        return fallback
    return field_map.get(role) or fallback


def generate_parser_code(
    bookmaker_name: str,
    workflow_name: str,
    sport_name: str | None,
    list_field_map: dict,
    list_array_path: str | None,
    markets_field_map: dict | None,
    markets_array_path: str | None,
    step2_key: str | None,
    placeholder_map: dict | None,
) -> str:
    """
    Generate a parse_data() function from onboarding field maps.

    The generated parser handles:
    - Single merged match item (multi-step: list item + nested markets)
    - Full list response with embedded markets (single-step)
    - Both dict and list root inputs
    - Per-market error isolation
    - All required keys (parent_match_id, home_team, away_team, start_time,
      sport, competition, market, selection, price, specifier)
    """
    fm  = list_field_map or {}
    mfm = markets_field_map or {}
    pm  = placeholder_map or {}

    match_id_p   = _field_path(fm,  "match_id",         _field_path(fm, "parent_match_id", "id"))
    home_p       = _field_path(fm,  "home_team",         "home_team")
    away_p       = _field_path(fm,  "away_team",         "away_team")
    start_p      = _field_path(fm,  "start_time",        "start_time")
    sport_p      = _field_path(fm,  "sport",             "sport")
    comp_p       = _field_path(fm,  "competition",       "competition")
    mkt_name_p   = _field_path(mfm, "market_name",       _field_path(mfm, "market", "name"))
    specifier_p  = _field_path(mfm, "specifier",         "")
    sel_name_p   = _field_path(mfm, "selection_name",    _field_path(mfm, "selection", "name"))
    price_p      = _field_path(mfm, "selection_price",   _field_path(mfm, "price", "odds"))

    # The markets array key inside the markets response
    markets_response_arr = markets_array_path or "markets"
    # The nested key in the merged item where step 2 data lives
    step2 = step2_key or "_markets"
    # The embedded markets path for single-step workflows
    embedded_mkts = markets_response_arr if not step2_key else "markets"
    # list array path for full-list input
    list_arr = list_array_path or ""

    # Build selection container path (e.g. mkt.get('selections') or mkt itself)
    # We detect common patterns
    sel_container = "outcomes" if "outcome" in sel_name_p.lower() or "outcome" in price_p.lower() else "selections"

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    code = f'''# Auto-generated parser — {bookmaker_name}: {workflow_name}
# Generated: {ts}
# Sport: {sport_name or "All sports"}
# ─────────────────────────────────────────────────────────────
# This parser handles BOTH the match list AND market data.
# raw_data is a SINGLE merged match item produced by the harvest runner:
#
#   {{
#       # Match fields from Step 1 (list endpoint)
#       "{match_id_p.split(".")[0]}": <match_id>,
#       "{home_p.split(".")[0]}": ...,
#       ...
#       # Market data from Step 2 (markets endpoint), nested under:
#       "{step2}": [
#           {{
#               "{mkt_name_p.split(".")[0]}": "1X2",
#               "{sel_container}": [
#                   {{"{sel_name_p.split(".")[-1]}": "Home", "{price_p.split(".")[-1]}": 2.10}},
#               ]
#           }},
#           ...
#       ]
#   }}
#
# The parser also handles full list responses (for single-step workflows
# where markets are embedded in the list item).
# ─────────────────────────────────────────────────────────────


def _get(obj, path, default=None):
    """Resolve dot-path in nested dict/list. Returns default on any failure."""
    if not path or obj is None:
        return default
    try:
        for part in path.split("."):
            if obj is None:
                return default
            if isinstance(obj, dict):
                obj = obj.get(part)
            elif isinstance(obj, list):
                obj = [item.get(part) if isinstance(item, dict) else None for item in obj]
            else:
                return default
    except Exception:
        return default
    return obj if obj is not None else default


def _to_float(val) -> float:
    """Safely convert to float."""
    try:
        return float(val or 0)
    except (TypeError, ValueError):
        return 0.0


def _parse_markets(item: dict, base: dict, rows: list) -> None:
    """
    Extract all market rows from a single match item.
    Looks for markets in:
      1. "{step2}" key (multi-step: markets from FETCH_PER_ITEM)
      2. "{embedded_mkts}" key  (single-step: embedded markets)
      3. Root of item if it looks like a market object
    """
    markets_raw = (
        _get(item, "{step2}")         # Multi-step nested key
        or _get(item, "{embedded_mkts}")    # Single-step embedded
        or []
    )

    # Some bookmakers return markets as a dict keyed by ID
    if isinstance(markets_raw, dict):
        markets_raw = list(markets_raw.values())
    if not isinstance(markets_raw, list):
        markets_raw = []

    for mkt in markets_raw:
        if not isinstance(mkt, dict):
            continue
        try:
            market_name = str(_get(mkt, "{mkt_name_p}") or "")
            if not market_name:
                continue

            # Specifier (handicap line, total goal line, etc.)
            specifier = {f'_get(mkt, "{specifier_p}")' if specifier_p else "None"}

            # Selections / outcomes
            selections = _get(mkt, "{sel_container}") or []
            if not isinstance(selections, list):
                # Some bookmakers embed selections at root
                selections = [mkt]

            for sel in selections:
                if not isinstance(sel, dict):
                    continue
                try:
                    price = _to_float(_get(sel, "{price_p}"))
                    if price <= 1.0:
                        continue
                    rows.append({{
                        **base,
                        "market":    market_name,
                        "selection": str(_get(sel, "{sel_name_p}") or ""),
                        "price":     price,
                        "specifier": str(specifier) if specifier is not None else None,
                    }})
                except Exception:
                    continue
        except Exception:
            continue


def parse_data(raw_data):
    """
    Unified parser: match list + per-match market data.

    Works for:
      - Single merged match item (FETCH_PER_ITEM pipeline)
      - Full list response with embedded markets
      - Live iteration across all configured sports

    Required keys per row:
      parent_match_id, home_team, away_team, start_time, sport, competition,
      market, selection, price (float > 1.0), specifier (str | None)
    """
    rows = []

    # ── Normalise input to a list of match items ──────────────────────────
    if isinstance(raw_data, list):
        items = raw_data
    elif isinstance(raw_data, dict):
        # Try the known list array path first
        list_data = {f'_get(raw_data, "{list_arr}")' if list_arr else "None"}
        if isinstance(list_data, list) and list_data:
            items = list_data
        else:
            # Auto-detect: look for any top-level list
            auto = next(
                (v for v in raw_data.values() if isinstance(v, list) and v
                 and isinstance(v[0], dict)),
                None,
            )
            items = auto if auto is not None else [raw_data]
    else:
        return rows

    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            match_id = _get(item, "{match_id_p}")
            if match_id is None:
                continue

            base = {{
                "parent_match_id": str(match_id),
                "home_team":       str(_get(item, "{home_p}") or ""),
                "away_team":       str(_get(item, "{away_p}") or ""),
                "start_time":      _get(item, "{start_p}"),
                "sport":           _get(item, "{sport_p}"),
                "competition":     _get(item, "{comp_p}"),
            }}

            _parse_markets(item, base, rows)

        except Exception:
            continue

    return rows
'''
    # Fix the specifier line — the f-string nesting was causing issues, let me clean it up
    if specifier_p:
        code = code.replace(
            f'_get(mkt, "{specifier_p}")',
            f'_get(mkt, "{specifier_p}")'
        )
        code = code.replace(
            f'{{f\'_get(mkt, "{specifier_p}")\'}}',
            f'_get(mkt, "{specifier_p}")'
        )
    else:
        code = code.replace(
            "{f'_get(mkt, \"\")' if specifier_p else \"None\"}",
            "None"
        ).replace(
            "{'_get(mkt, \"\")' if specifier_p else 'None'}",
            "None"
        )

    # Fix the list_data line
    if list_arr:
        code = code.replace(
            f'{{f\'_get(raw_data, "{list_arr}")\'}}',
            f'_get(raw_data, "{list_arr}")'
        ).replace(
            f"{{f'_get(raw_data, \"{list_arr}\")' if list_arr else \"None\"}}",
            f'_get(raw_data, "{list_arr}")'
        )
    else:
        code = code.replace(
            "{f'_get(raw_data, \"\")' if list_arr else \"None\"}",
            "None"
        )

    return code


def _build_parser_code(
    bookmaker_name: str,
    workflow_name: str,
    sport_name: str | None,
    list_field_map: dict,
    list_array_path: str | None,
    markets_field_map: dict | None,
    markets_array_path: str | None,
    step2_key_name: str | None,
    placeholder_map: dict | None,
) -> str:
    """Clean string-building approach for the parser (avoids f-string nesting issues)."""
    fm  = list_field_map or {}
    mfm = markets_field_map or {}

    match_id_p  = fm.get("match_id") or fm.get("parent_match_id") or "id"
    home_p      = fm.get("home_team") or "home_team"
    away_p      = fm.get("away_team") or "away_team"
    start_p     = fm.get("start_time") or "start_time"
    sport_p     = fm.get("sport") or "sport"
    comp_p      = fm.get("competition") or "competition"
    mkt_name_p  = mfm.get("market_name") or mfm.get("market") or "name"
    specifier_p = mfm.get("specifier") or ""
    sel_name_p  = mfm.get("selection_name") or mfm.get("selection") or "name"
    price_p     = mfm.get("selection_price") or mfm.get("price") or "odds"

    step2       = step2_key_name or "_markets"
    embedded    = markets_array_path or "markets"
    list_arr    = list_array_path or ""
    sel_cont    = "outcomes" if any(x in (sel_name_p + price_p).lower() for x in ("outcome",)) else "selections"
    ts          = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    specifier_line = f'_get(mkt, "{specifier_p}")' if specifier_p else "None"
    list_data_line = f'_get(raw_data, "{list_arr}")' if list_arr else "None"

    lines = [
        f'# Auto-generated parser — {bookmaker_name}: {workflow_name}',
        f'# Generated: {ts}',
        f'# Sport context: {sport_name or "All sports (generic)"}',
        f'# Paths: match_id={match_id_p}, home={home_p}, away={away_p}',
        f'#        markets_key={step2}, market_name={mkt_name_p}',
        f'#        selection={sel_name_p}, price={price_p}',
        '',
        '',
        'def _get(obj, path, default=None):',
        '    """Resolve dot-path in nested dict/list."""',
        '    if not path or obj is None:',
        '        return default',
        '    try:',
        '        for part in path.split("."):',
        '            if obj is None:',
        '                return default',
        '            if isinstance(obj, dict):',
        '                obj = obj.get(part)',
        '            elif isinstance(obj, list):',
        '                obj = [item.get(part) if isinstance(item, dict) else None for item in obj]',
        '            else:',
        '                return default',
        '    except Exception:',
        '        return default',
        '    return obj if obj is not None else default',
        '',
        '',
        'def _to_float(val) -> float:',
        '    try:',
        '        return float(val or 0)',
        '    except (TypeError, ValueError):',
        '        return 0.0',
        '',
        '',
        'def _parse_markets(item: dict, base: dict, rows: list) -> None:',
        '    """Extract all market rows from one match item."""',
        f'    markets_raw = _get(item, "{step2}") or _get(item, "{embedded}") or []',
        '    if isinstance(markets_raw, dict):',
        '        markets_raw = list(markets_raw.values())',
        '    if not isinstance(markets_raw, list):',
        '        markets_raw = []',
        '',
        '    for mkt in markets_raw:',
        '        if not isinstance(mkt, dict):',
        '            continue',
        '        try:',
        f'            market_name = str(_get(mkt, "{mkt_name_p}") or "")',
        '            if not market_name:',
        '                continue',
        f'            specifier = {specifier_line}',
        f'            selections = _get(mkt, "{sel_cont}") or []',
        '            if not isinstance(selections, list):',
        '                selections = [mkt]',
        '            for sel in selections:',
        '                if not isinstance(sel, dict):',
        '                    continue',
        '                try:',
        f'                    price = _to_float(_get(sel, "{price_p}"))',
        '                    if price <= 1.0:',
        '                        continue',
        '                    rows.append({',
        '                        **base,',
        '                        "market":    market_name,',
        f'                        "selection": str(_get(sel, "{sel_name_p}") or ""),',
        '                        "price":     price,',
        '                        "specifier": str(specifier) if specifier is not None else None,',
        '                    })',
        '                except Exception:',
        '                    continue',
        '        except Exception:',
        '            continue',
        '',
        '',
        'def parse_data(raw_data):',
        '    """',
        '    Unified parser: match list + per-match market data.',
        '    ',
        '    raw_data can be:',
        '      - A merged match item (FETCH_PER_ITEM pipeline)',
        '      - A full list response with embedded markets (FETCH_LIST pipeline)',
        '    ',
        '    Output rows must contain:',
        '      parent_match_id, home_team, away_team, start_time, sport,',
        '      competition, market, selection, price (float > 1.0), specifier',
        '    """',
        '    rows = []',
        '',
        '    # Normalise input to list of items',
        '    if isinstance(raw_data, list):',
        '        items = raw_data',
        '    elif isinstance(raw_data, dict):',
        f'        list_data = {list_data_line}',
        '        if isinstance(list_data, list) and list_data:',
        '            items = list_data',
        '        else:',
        '            # Auto-detect first top-level list of dicts',
        '            auto = next(',
        '                (v for v in raw_data.values()',
        '                 if isinstance(v, list) and v and isinstance(v[0], dict)),',
        '                None,',
        '            )',
        '            items = auto if auto is not None else [raw_data]',
        '    else:',
        '        return rows',
        '',
        '    for item in items:',
        '        if not isinstance(item, dict):',
        '            continue',
        '        try:',
        f'            match_id = _get(item, "{match_id_p}")',
        '            if match_id is None:',
        '                continue',
        '            base = {',
        '                "parent_match_id": str(match_id),',
        f'                "home_team":       str(_get(item, "{home_p}") or ""),',
        f'                "away_team":       str(_get(item, "{away_p}") or ""),',
        f'                "start_time":      _get(item, "{start_p}"),',
        f'                "sport":           _get(item, "{sport_p}"),',
        f'                "competition":     _get(item, "{comp_p}"),',
        '            }',
        '            _parse_markets(item, base, rows)',
        '        except Exception:',
        '            continue',
        '',
        '    return rows',
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Route: POST /research/workflows/<wid>/parser/generate
# ─────────────────────────────────────────────────────────────────────────────

def generate_parser_route(wid: int):
    """Generate parse_data() from the bookmaker's onboarding session field maps."""
    wf = HarvestWorkflow.query.get_or_404(wid)

    # Find the most recent onboarding session for this bookmaker
    session = (
        BookmakerOnboardingSession.query
        .filter_by(bookmaker_id=wf.bookmaker_id)
        .order_by(BookmakerOnboardingSession.updated_at.desc())
        .first()
    )

    # Get sport name from workflow
    sport_name: str | None = None
    if hasattr(wf, "sport") and wf.sport:
        sport_name = wf.sport.name if hasattr(wf.sport, "name") else str(wf.sport)

    # Determine step2 key from workflow steps
    sorted_steps = sorted(wf.steps, key=lambda s: s.position)
    step2 = sorted_steps[1] if len(sorted_steps) >= 2 else None
    step2_key_name = _step2_key(step2) if step2 else None

    if session:
        bk_name = wf.bookmaker.name if wf.bookmaker else "Bookmaker"

        # Determine which maps to use based on workflow name
        # Live workflows use live_list / live_markets field maps
        is_live = "live" in wf.name.lower()
        list_fm  = session.live_list_field_map  if is_live else session.list_field_map
        mkt_fm   = session.live_markets_field_map if is_live else session.markets_field_map
        list_arr = session.live_list_array_path  if is_live else session.list_array_path
        mkt_arr  = session.live_markets_array_path if is_live else session.markets_array_path
        ph_map   = session.live_markets_placeholder_map if is_live else session.markets_placeholder_map

        code = _build_parser_code(
            bookmaker_name=bk_name,
            workflow_name=wf.name,
            sport_name=sport_name,
            list_field_map=list_fm or {},
            list_array_path=list_arr,
            markets_field_map=mkt_fm,
            markets_array_path=mkt_arr,
            step2_key_name=step2_key_name,
            placeholder_map=ph_map,
        )
        return jsonify({
            "ok": True,
            "code": code,
            "sport_name": sport_name,
            "session_id": session.id,
            "steps_used": len(sorted_steps),
            "from_session": True,
        })

    # No session — generate a generic template from step field maps
    step1 = sorted_steps[0] if sorted_steps else None
    import json as _json

    def parse_fm(step: HarvestWorkflowStep | None) -> dict:
        if not step:
            return {}
        fields = _json.loads(step.fields_json) if step.fields_json else []
        return {f["role"]: f["path"] for f in fields if f.get("role") and f.get("path")}

    bk_name = wf.bookmaker.name if wf.bookmaker else "Bookmaker"
    code = _build_parser_code(
        bookmaker_name=bk_name,
        workflow_name=wf.name,
        sport_name=sport_name,
        list_field_map=parse_fm(step1),
        list_array_path=step1.result_array_path if step1 else None,
        markets_field_map=parse_fm(step2),
        markets_array_path=step2.result_array_path if step2 else None,
        step2_key_name=step2_key_name,
        placeholder_map=None,
    )
    return jsonify({
        "ok": True,
        "code": code,
        "sport_name": sport_name,
        "session_id": None,
        "steps_used": len(sorted_steps),
        "from_session": False,
        "warning": "No onboarding session found — generated from step field maps.",
    })


# ─────────────────────────────────────────────────────────────────────────────
# Route: POST /research/workflows/<wid>/parser/iterate-sports
# ─────────────────────────────────────────────────────────────────────────────

def iterate_sports_route(wid: int):
    """
    Execute the parser against the LIVE API for every configured sport.

    Flow per sport:
      1. Probe list endpoint with sport_id substituted
      2. Take first N match items
      3. For each match: probe markets endpoint with {{match_id}} resolved
      4. Merge: match_item + {step2_key: markets_response}
      5. Run parse_data(merged_item) on each
      6. Aggregate rows + check market coverage for that sport

    Body: {code, limit_per_sport?}
    """
    wf = HarvestWorkflow.query.get_or_404(wid)
    d  = request.json or {}

    code            = (d.get("code") or "").strip()
    limit_per_sport = int(d.get("limit_per_sport") or 3)
    limit_per_sport = max(1, min(10, limit_per_sport))

    if not code:
        return jsonify({"ok": False, "error": "code is required"}), 400

    # Get onboarding session
    session = (
        BookmakerOnboardingSession.query
        .filter_by(bookmaker_id=wf.bookmaker_id)
        .order_by(BookmakerOnboardingSession.updated_at.desc())
        .first()
    )
    if not session:
        return jsonify({"ok": False, "error": "No onboarding session found for this bookmaker"}), 404

    if not session.sport_mappings:
        return jsonify({"ok": False, "error": "No sport mappings configured in onboarding session"}), 400

    # Determine is_live from workflow name
    is_live = "live" in wf.name.lower()
    list_url     = session.live_list_url    if is_live else session.list_url
    list_method  = session.live_list_method if is_live else session.list_method or "GET"
    list_headers = session.live_list_headers if is_live else session.list_headers or {}
    list_params  = session.live_list_params  if is_live else session.list_params  or {}
    list_arr     = session.live_list_array_path  if is_live else session.list_array_path

    mkt_url_tmpl = session.live_markets_url_template    if is_live else session.markets_url_template
    mkt_method   = session.live_markets_method          if is_live else session.markets_method or "GET"
    mkt_headers  = session.live_markets_headers         if is_live else session.markets_headers or {}
    mkt_params   = session.live_markets_params          if is_live else session.markets_params  or {}
    mkt_arr      = session.live_markets_array_path      if is_live else session.markets_array_path
    ph_map       = session.live_markets_placeholder_map if is_live else session.markets_placeholder_map or {}
    list_fm      = session.live_list_field_map           if is_live else session.list_field_map or {}

    if not list_url:
        return jsonify({"ok": False, "error": "List endpoint URL not configured in session"}), 400

    # Step2 key for merging
    sorted_steps = sorted(wf.steps, key=lambda s: s.position)
    step2 = sorted_steps[1] if len(sorted_steps) >= 2 else None
    step2_key_name = _step2_key(step2) if step2 else "_markets"

    results = []

    for mapping in session.sport_mappings:
        bk_sport_id = str(mapping.get("bk_sport_id") or "")
        sport_name  = mapping.get("sport_name") or f"Sport {mapping.get('sport_id', '?')}"
        param_key   = mapping.get("param_key") or "sport_id"
        param_in    = mapping.get("param_in") or "query"

        sport_result: dict = {
            "bk_sport_id":     bk_sport_id,
            "sport_name":      sport_name,
            "canonical_sport": sport_name,
            "status":          "error",
            "item_count":      0,
            "row_count":       0,
            "coverage_pct":    0.0,
            "present_markets": [],
            "missing_markets": [],
            "error":           None,
            "match_samples":   [],
        }

        try:
            # ── 1. Probe list endpoint ──────────────────────────────────
            probe_url    = list_url
            probe_params = dict(list_params or {})

            if param_in == "query":
                probe_params[param_key] = bk_sport_id
            elif param_in == "path":
                probe_url = re.sub(r"\{\{" + param_key + r"\}\}", bk_sport_id, probe_url)

            list_result = _probe(
                url=probe_url, method=list_method,
                headers=list_headers, params=probe_params,
            )
            if not list_result["ok"]:
                sport_result["error"] = f"List probe failed: {list_result['error']}"
                results.append(sport_result)
                continue

            match_items = _extract_items(list_result["parsed"], list_arr)
            if not match_items:
                sport_result["error"] = "List returned no match items"
                results.append(sport_result)
                continue

            sport_result["item_count"] = len(match_items)

            # ── 2. Probe markets + parse each match ──────────────────────
            all_rows: list[dict] = []

            for i, match_item in enumerate(match_items[:limit_per_sport]):
                if not isinstance(match_item, dict):
                    continue

                merged = dict(match_item)  # copy list item

                if mkt_url_tmpl:
                    # Resolve {{match_id}} → actual value
                    resolved_url, resolved_params = _resolve_template(
                        mkt_url_tmpl, mkt_params or {}, ph_map, match_item, list_fm
                    )
                    mkt_result = _probe(
                        url=resolved_url, method=mkt_method,
                        headers=mkt_headers, params=resolved_params,
                    )
                    if mkt_result["ok"] and mkt_result["parsed"] is not None:
                        # Nest markets under step2 key
                        mkt_items = _extract_items(mkt_result["parsed"], mkt_arr)
                        if mkt_items:
                            merged[step2_key_name] = mkt_items
                        else:
                            merged[step2_key_name] = [mkt_result["parsed"]]

                # Run parser
                parse_result = _safe_exec_parser(code, merged)
                all_rows.extend(parse_result.get("rows") or [])

                if i == 0:
                    sport_result["match_samples"] = (parse_result.get("rows") or [])[:3]

            # ── 3. Coverage check ─────────────────────────────────────────
            coverage = _check_coverage(all_rows, sport_name)

            sport_result.update({
                "status":          "ok" if coverage["ok"] and all_rows else ("warn" if all_rows else "error"),
                "row_count":       len(all_rows),
                "coverage_pct":    coverage["coverage_pct"],
                "present_markets": coverage["present_markets"],
                "missing_markets": coverage["missing_markets"],
                "extra_markets":   coverage.get("extra_markets", []),
                "expected_count":  coverage["expected_count"],
                "present_count":   coverage["present_count"],
                "error":           None if all_rows else "Parser returned no rows",
            })

        except Exception as exc:
            sport_result["error"] = str(exc)

        results.append(sport_result)

    all_ok    = all(r["status"] == "ok" for r in results)
    any_ok    = any(r["status"] in ("ok", "warn") for r in results)
    total_rows = sum(r["row_count"] for r in results)

    return jsonify({
        "ok":        any_ok,
        "all_ok":    all_ok,
        "results":   results,
        "summary": {
            "sports_tested":  len(results),
            "sports_passed":  sum(1 for r in results if r["status"] == "ok"),
            "total_rows":     total_rows,
            "avg_coverage":   round(
                sum(r["coverage_pct"] for r in results) / len(results), 1
            ) if results else 0.0,
        },
    })