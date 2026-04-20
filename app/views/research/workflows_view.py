"""
app/api/workflow_api.py
=======================
Combined Workflow Tree + Parser API  —  single file, one blueprint.

All routes registered on  bp_research  (prefix: /research).

WORKFLOW TREE
─────────────
GET    /research/field-roles
GET    /research/workflows/tree
GET    /research/workflows/<wid>/full
PATCH  /research/workflows/<wid>
PUT    /research/workflows/<wid>/field-path
PUT    /research/workflows/<wid>/step/<pos>/full-update
POST   /research/workflows/<wid>/step
DELETE /research/workflows/<wid>/step/<pos>
POST   /research/workflows/<wid>/steps/reorder
POST   /research/workflows/<wid>/step/<pos>/add-field
DELETE /research/workflows/<wid>/step/<pos>/field/<idx>

MARKET CATALOGUE
─────────────────
GET  /research/markets
GET  /research/markets/sports
GET  /research/markets/<sport_name>

PARSER  (per-step — every step owns its own parse_data)
────────────────────────────────────────────────────────────
POST   /research/parser/test
         Stateless sandbox — no workflow or step required.

GET    /research/workflows/<wid>/step/<pos>/parser
         Return saved parser code + test status for this step.

POST   /research/workflows/<wid>/step/<pos>/parser/test
         Execute against a sample; auto-injects workflow sport.

POST   /research/workflows/<wid>/step/<pos>/parser/save
         Persist parser code + test status onto this step.
         Body: { code, test_passed?, sample_json?, run_test?,
                 sport?, workflow_type?, coverage_threshold? }

DELETE /research/workflows/<wid>/step/<pos>/parser
         Clear this step's parser and reset its test status.

PROBE
─────
POST   /research/probe
         Fire a real HTTP request on behalf of the UI.
         Accepts full header map (auth tokens, x-hd, etc.).
         Returns parsed JSON, schema sketch, latency, and curl string.
         Body: { url, method?, headers?, params?, body? }

Registration
────────────
In app/__init__.py register ONE entry:

    from app.api import workflow_api
    app.register_blueprint(bp_research, url_prefix="/research")
"""

from __future__ import annotations

import gzip
import json
import re
import time
import traceback
import zlib
from datetime import datetime, timezone
from typing import Any


import urllib.parse
from curl_cffi import requests as tls_requests

from flask import request, jsonify

from app.views.research.parser_generator import generate_parser_route, iterate_sports_route

# HTTP client — prefer httpx (async-capable), fall back to requests
try:
    import httpx as _http_lib
    _USE_HTTPX = True
except ImportError:
    import requests as _http_lib          # type: ignore
    _USE_HTTPX = False

from app.extensions import db
from app.models.bookmakers_model import Bookmaker
from app.models.harvest_workflow import HarvestWorkflow, HarvestWorkflowStep
from app.models.odds import validate_parser_row, REQUIRED_PARSER_KEYS
from app.utils.mapping_seed import (
    MARKETS_BY_SPORT,
    get_markets_for_sport,
    get_primary_slugs_for_sport,
)
from .workflows_validator import (
    enrich_test_result_with_coverage,
    get_expected_markets,
    get_primary_markets,
    MATCH_LIST_WORKFLOW_TYPES,
)
from . import bp_research


# ─────────────────────────────────────────────────────────────────────────────
# Field role catalogue  (returned by GET /research/field-roles)
# ─────────────────────────────────────────────────────────────────────────────

ROLE_CATALOGUE: list[dict] = [
    # ── Match identity ────────────────────────────────────────────────────────
    {"role": "match_id",         "group": "Match",   "label": "Match ID",
     "description": "Bookmaker's internal ID — used in {{match_id}} URL templates"},
    {"role": "parent_match_id",  "group": "Match",   "label": "Parent / Canonical ID",
     "description": "Betradar or cross-bookmaker canonical match ID"},
    {"role": "home_team",        "group": "Match",   "label": "Home Team",    "description": "Home team name"},
    {"role": "away_team",        "group": "Match",   "label": "Away Team",    "description": "Away team name"},
    {"role": "start_time",       "group": "Match",   "label": "Kick-off",     "description": "ISO 8601 or epoch start time"},
    {"role": "sport",            "group": "Match",   "label": "Sport",        "description": "Sport name string"},
    {"role": "competition",      "group": "Match",   "label": "Competition",  "description": "League / tournament name"},
    {"role": "kickoff_in_mins",  "group": "Match",   "label": "Kickoff In Mins",
     "description": "Minutes until kick-off — used by notification scheduler"},
    # ── Markets ───────────────────────────────────────────────────────────────
    {"role": "market_name",      "group": "Markets", "label": "Market Name",    "description": "e.g. '1X2', 'Over/Under'"},
    {"role": "specifier",        "group": "Markets", "label": "Specifier",      "description": "Line/handicap, e.g. '2.5'"},
    {"role": "selection_name",   "group": "Markets", "label": "Selection Name", "description": "e.g. 'Home', 'Over'"},
    {"role": "selection_price",  "group": "Markets", "label": "Price / Odds",   "description": "Decimal odds float"},
    # ── Live / detail ─────────────────────────────────────────────────────────
    {"role": "match_status",     "group": "Live",    "label": "Match Status",   "description": "e.g. 'upcoming', 'live', 'finished'"},
    {"role": "score_home",       "group": "Live",    "label": "Score Home",     "description": "Current home score"},
    {"role": "score_away",       "group": "Live",    "label": "Score Away",     "description": "Current away score"},
    # ── Lineup / intelligence ─────────────────────────────────────────────────
    {"role": "home_lineup",      "group": "Lineup",  "label": "Home Lineup",    "description": "Home starting XI (array or string)"},
    {"role": "away_lineup",      "group": "Lineup",  "label": "Away Lineup",    "description": "Away starting XI"},
    {"role": "lineup_confirmed", "group": "Lineup",  "label": "Lineup Confirmed",
     "description": "Boolean — triggers notification when True"},
    # ── Form ──────────────────────────────────────────────────────────────────
    {"role": "home_form",        "group": "Form",    "label": "Home Form",      "description": "Recent results string e.g. 'WWDLL'"},
    {"role": "away_form",        "group": "Form",    "label": "Away Form",      "description": "Recent results string"},
    # ── Generic ───────────────────────────────────────────────────────────────
    {"role": "custom",           "group": "Other",   "label": "Custom",
     "description": "Arbitrary field — stored in extra_json"},
]

_ROLE_SET = {r["role"] for r in ROLE_CATALOGUE}


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _json_safe(text: str | None, fallback: Any = None) -> Any:
    """Silently parse JSON text; return fallback on failure."""
    if not text:
        return fallback
    try:
        return json.loads(text)
    except Exception:
        return fallback


def _step_full(s: HarvestWorkflowStep) -> dict:
    """Serialise a step to the shape expected by the frontend."""
    tmpl = s.url_template or ""
    return {
        "id":                  s.id,
        "position":            s.position,
        "name":                s.name,
        "step_type":           s.step_type,
        "url_template":        tmpl,
        "method":              s.method,
        "headers":             _json_safe(s.headers_json, {}),
        "params":              _json_safe(s.params_json,  {}),
        "body_template":       s.body_template,
        "result_array_path":   s.result_array_path or "",
        "fields":              _json_safe(s.fields_json, []),
        "depends_on_pos":      s.depends_on_pos,
        # field_mappings_json may not exist on older schema — guard with getattr
        "field_mappings":      _json_safe(getattr(s, "field_mappings_json", None), {}),
        "enabled":             s.enabled,
        "notes":               s.notes or "",
        "template_vars":       re.findall(r'\{\{(\w+)\}\}', tmpl),
        # Per-step parser
        "parser_code":         s.parser_code or None,
        "parser_test_passed":  bool(s.parser_test_passed),
    }


def _wf_full(wf: HarvestWorkflow) -> dict:
    """
    Serialise a workflow to the shape expected by WorkflowExplorer.tsx.
    Parser state is now per-step; the sidebar badge derives from step data.
    """
    steps = sorted(wf.steps, key=lambda s: s.position)
    return {
        "id":            wf.id,
        "bookmaker_id":  wf.bookmaker_id,
        "bookmaker_name":wf.bookmaker.name if wf.bookmaker else None,
        "name":          wf.name,
        "description":   wf.description or "",
        "is_active":     wf.is_active,
        "created_at":    wf.created_at.isoformat() if wf.created_at else None,
        "updated_at":    wf.updated_at.isoformat() if wf.updated_at else None,
        "steps":         [_step_full(s) for s in steps],
    }


def _parse_coverage_params(d: dict) -> tuple[str | None, str, float]:
    """Extract (sport_name, workflow_type, threshold_pct) from request body."""
    sport         = (d.get("sport") or "").strip() or None
    workflow_type = (d.get("workflow_type") or "MARKETS_ONLY").strip().upper()
    threshold     = float(d.get("coverage_threshold") or 50.0)
    threshold     = max(0.0, min(100.0, threshold))
    return sport, workflow_type, threshold


def _resolve_sport_name(raw: str | None) -> str | None:
    """Case-insensitive match against known MARKETS_BY_SPORT keys."""
    if not raw:
        return None
    for key in MARKETS_BY_SPORT:
        if key and key.lower() == raw.strip().lower():
            return key
    return raw.strip()   # return as-is; validator will soft-pass unknowns


def _workflow_sport(wf: HarvestWorkflow) -> str | None:
    """Extract canonical sport name from workflow relationship."""
    if hasattr(wf, "sport") and wf.sport:
        return wf.sport.name if hasattr(wf.sport, "name") else str(wf.sport)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Parser sandbox
# ─────────────────────────────────────────────────────────────────────────────

def _safe_run_parser(code: str, sample: Any, max_rows: int = 200) -> dict:
    """
    Execute  parse_data(sample)  from user-supplied code in an isolated namespace.

    Security note: internal operator tool — exec() runs with full __builtins__
    so stdlib imports work.  Do NOT expose to untrusted public users.

    Returns:
    {
      ok, rows (up to max_rows), row_count, valid_count,
      validation_errors, error, elapsed_ms, warnings
    }
    """
    t0 = time.monotonic()

    # 1. Compile ────────────────────────────────────────────────────────────────
    try:
        compiled = compile(code, "<workflow_parser>", "exec")
    except SyntaxError as exc:
        return {
            "ok": False,
            "error": f"SyntaxError on line {exc.lineno}: {exc.msg}",
            "rows": [], "row_count": 0, "valid_count": 0,
            "validation_errors": [], "warnings": [], "elapsed_ms": 0,
        }

    # 2. Execute module level ───────────────────────────────────────────────────
    namespace: dict = {"__builtins__": __builtins__}
    try:
        exec(compiled, namespace)
    except Exception:
        return {
            "ok": False, "error": traceback.format_exc(),
            "rows": [], "row_count": 0, "valid_count": 0,
            "validation_errors": [], "warnings": [],
            "elapsed_ms": round((time.monotonic() - t0) * 1000),
        }

    # 3. Locate parse_data ──────────────────────────────────────────────────────
    parse_fn = namespace.get("parse_data")
    if not callable(parse_fn):
        return {
            "ok": False,
            "error": (
                "Function 'parse_data' not found.\n"
                "Define:  def parse_data(raw_data):\n"
                "             ...\n"
                "             return list_of_dicts"
            ),
            "rows": [], "row_count": 0, "valid_count": 0,
            "validation_errors": [], "warnings": [], "elapsed_ms": 0,
        }

    # 4. Call parse_data ────────────────────────────────────────────────────────
    try:
        rows       = parse_fn(sample)
        elapsed_ms = round((time.monotonic() - t0) * 1000)
    except Exception:
        elapsed_ms = round((time.monotonic() - t0) * 1000)
        return {
            "ok": False, "error": traceback.format_exc(),
            "rows": [], "row_count": 0, "valid_count": 0,
            "validation_errors": [], "warnings": [], "elapsed_ms": elapsed_ms,
        }

    if not isinstance(rows, list):
        return {
            "ok": False,
            "error": f"parse_data() returned {type(rows).__name__}, expected list.",
            "rows": [], "row_count": 0, "valid_count": 0,
            "validation_errors": [], "warnings": [], "elapsed_ms": elapsed_ms,
        }

    # 5. Row-level validation ───────────────────────────────────────────────────
    validation_errors: list[str] = []
    valid_rows: list[dict]       = []

    for i, row in enumerate(rows):
        try:
            validate_parser_row(row)
            valid_rows.append(row)
        except (ValueError, TypeError) as exc:
            msg = str(exc).split("\n")[0][:280]
            validation_errors.append(f"Row {i}: {msg}")

    # 6. JSON-safe serialisation ────────────────────────────────────────────────
    safe_rows: list[dict] = []
    for row in rows[:max_rows]:
        safe_row: dict = {}
        for k, v in row.items():
            try:
                json.dumps(v)
                safe_row[k] = v
            except (TypeError, ValueError):
                safe_row[k] = str(v)
        safe_rows.append(safe_row)

    return {
        "ok":                len(validation_errors) == 0 and len(rows) > 0,
        "rows":              safe_rows,
        "row_count":         len(rows),
        "valid_count":       len(valid_rows),
        "validation_errors": validation_errors[:30],
        "error":             None,
        "warnings":          [],
        "elapsed_ms":        elapsed_ms,
    }


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES — FIELD ROLES
# ═════════════════════════════════════════════════════════════════════════════

@bp_research.route("/field-roles", methods=["GET"])
def field_roles():
    """Return the canonical role catalogue used by the field-role picker."""
    return jsonify(ROLE_CATALOGUE)


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES — WORKFLOW TREE
# ═════════════════════════════════════════════════════════════════════════════

@bp_research.route("/workflows/tree", methods=["GET"])
def workflow_tree():
    """
    Returns all workflows grouped by bookmaker.

    QS params:
      bookmaker_id  — filter to one bookmaker
      active_only   — 1 | true  (default: false, show all)
    """
    bk_id       = request.args.get("bookmaker_id", type=int)
    active_only = request.args.get("active_only", "").lower() in ("1", "true", "yes")

    bk_query = Bookmaker.query.order_by(Bookmaker.name)
    if bk_id:
        bk_query = bk_query.filter_by(id=bk_id)
    bookmakers = bk_query.all()

    result = []
    for bk in bookmakers:
        wf_q = HarvestWorkflow.query.filter_by(bookmaker_id=bk.id)
        if active_only:
            wf_q = wf_q.filter_by(is_active=True)
        workflows = wf_q.order_by(HarvestWorkflow.name).all()

        result.append({
            "bookmaker_id":   bk.id,
            "bookmaker_name": bk.name,
            "domain":         bk.domain,
            "workflow_count": len(workflows),
            "workflows":      [_wf_full(wf) for wf in workflows],
        })

    return jsonify(result)


@bp_research.route("/workflows/<int:wid>/full", methods=["GET"])
def workflow_full(wid: int):
    """Single workflow with every step fully expanded."""
    wf = HarvestWorkflow.query.get_or_404(wid)
    return jsonify(_wf_full(wf))


@bp_research.route("/workflows/<int:wid>", methods=["PATCH"])
def patch_workflow(wid: int):
    """Update workflow-level metadata: name, description, is_active."""
    wf = HarvestWorkflow.query.get_or_404(wid)
    d  = request.json or {}
    if "name"        in d: wf.name        = (d["name"] or "").strip() or wf.name
    if "description" in d: wf.description = d["description"]
    if "is_active"   in d: wf.is_active   = bool(d["is_active"])
    wf.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "workflow": _wf_full(wf)})


@bp_research.route("/workflows/<int:wid>/field-path", methods=["PUT"])
def patch_field_path(wid: int):
    """
    Update one field descriptor inside a step's fields_json array.

    Body: { step_position, field_index, path?, role?, label?, store? }
    Returns the full updated step.
    """
    wf = HarvestWorkflow.query.get_or_404(wid)
    d  = request.json or {}

    pos       = d.get("step_position")
    field_idx = d.get("field_index")
    if pos is None or field_idx is None:
        return jsonify({"ok": False, "error": "step_position and field_index are required"}), 400

    step = next((s for s in wf.steps if s.position == pos), None)
    if not step:
        return jsonify({"ok": False, "error": f"Step {pos} not found"}), 404

    fields = _json_safe(step.fields_json, [])
    if field_idx >= len(fields):
        return jsonify({"ok": False, "error": f"field_index {field_idx} out of range (len={len(fields)})"}), 400

    if "path"  in d: fields[field_idx]["path"]  = d["path"]
    if "role"  in d: fields[field_idx]["role"]  = d["role"]
    if "label" in d: fields[field_idx]["label"] = d["label"]
    if "store" in d: fields[field_idx]["store"] = bool(d["store"])

    step.fields_json  = json.dumps(fields)
    wf.updated_at     = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "step": _step_full(step)})


@bp_research.route("/workflows/<int:wid>/step/<int:pos>/full-update", methods=["PUT"])
def full_update_step(wid: int, pos: int):
    """
    Full step update — replaces all mutable fields.
    Returns the entire updated workflow so the UI can re-render in one shot.
    """
    wf = HarvestWorkflow.query.get_or_404(wid)
    step = next((s for s in wf.steps if s.position == pos), None)
    if not step:
        return jsonify({"ok": False, "error": f"Step {pos} not found"}), 404

    d = request.json or {}
    if "name"              in d: step.name              = d["name"]
    if "step_type"         in d: step.step_type         = d["step_type"]
    if "url_template"      in d: step.url_template      = (d["url_template"] or "").strip()
    if "method"            in d: step.method            = d["method"].upper()
    if "headers"           in d: step.headers_json      = json.dumps(d["headers"] or {})
    if "params"            in d: step.params_json       = json.dumps(d["params"]  or {})
    if "body_template"     in d: step.body_template     = d["body_template"]
    if "result_array_path" in d: step.result_array_path = (d["result_array_path"] or "").strip() or None
    if "fields"            in d: step.fields_json       = json.dumps(d["fields"] or [])
    if "depends_on_pos"    in d: step.depends_on_pos    = d["depends_on_pos"]
    if "enabled"           in d: step.enabled           = bool(d["enabled"])
    if "notes"             in d: step.notes             = d["notes"]

    wf.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "workflow": _wf_full(wf)})


@bp_research.route("/workflows/<int:wid>/step", methods=["POST"])
def add_step(wid: int):
    """
    Insert a new step.  If after_position is given, insert after that position
    and re-number subsequent steps.  Otherwise append at the end.

    Body: { after_position?, step_type, name, url_template, depends_on_pos? }
    """
    wf = HarvestWorkflow.query.get_or_404(wid)
    d  = request.json or {}

    after = d.get("after_position")
    steps = sorted(wf.steps, key=lambda s: s.position)

    if after is not None:
        for s in steps:
            if s.position > after:
                s.position += 1
        new_pos = after + 1
    else:
        new_pos = (max((s.position for s in steps), default=0) + 1)

    new_step = HarvestWorkflowStep(
        workflow_id       = wf.id,
        position          = new_pos,
        name              = (d.get("name") or "").strip() or f"Step {new_pos}",
        step_type         = d.get("step_type", "FETCH_PER_ITEM"),
        url_template      = (d.get("url_template") or "").strip(),
        method            = (d.get("method") or "GET").upper(),
        headers_json      = json.dumps(d.get("headers") or {}),
        params_json       = json.dumps(d.get("params")  or {}),
        body_template     = d.get("body_template"),
        result_array_path = (d.get("result_array_path") or "").strip() or None,
        fields_json       = json.dumps(d.get("fields") or []),
        depends_on_pos    = d.get("depends_on_pos", after),
        enabled           = bool(d.get("enabled", True)),
        notes             = d.get("notes") or "",
    )
    db.session.add(new_step)
    wf.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "workflow": _wf_full(wf)}), 201


@bp_research.route("/workflows/<int:wid>/step/<int:pos>", methods=["DELETE"])
def delete_step(wid: int, pos: int):
    """Remove one step and re-number remaining steps."""
    wf = HarvestWorkflow.query.get_or_404(wid)
    step = next((s for s in wf.steps if s.position == pos), None)
    if not step:
        return jsonify({"ok": False, "error": f"Step {pos} not found"}), 404

    db.session.delete(step)
    db.session.flush()

    for s in sorted((x for x in wf.steps if x.id != step.id), key=lambda x: x.position):
        if s.position > pos:
            s.position -= 1

    wf.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "workflow": _wf_full(wf)})


@bp_research.route("/workflows/<int:wid>/steps/reorder", methods=["POST"])
def reorder_steps(wid: int):
    """
    Body: {"order": [2, 1, 3]}
    'order' is a list of current step positions in the desired new order.
    Steps are re-assigned positions 1, 2, 3 … in that sequence.
    depends_on_pos references are updated to follow the moves.
    """
    wf    = HarvestWorkflow.query.get_or_404(wid)
    d     = request.json or {}
    order = d.get("order") or []

    if not order:
        return jsonify({"ok": False, "error": "order is required"}), 400

    step_map = {s.position: s for s in wf.steps}
    missing  = [p for p in order if p not in step_map]
    if missing:
        return jsonify({"ok": False, "error": f"Positions not found: {missing}"}), 400

    pos_remap: dict[int, int] = {
        old_pos: new_pos
        for new_pos, old_pos in enumerate(order, start=1)
    }

    # Temp offset to avoid UNIQUE constraint violations during reassignment
    for old_pos, new_pos in pos_remap.items():
        step_map[old_pos].position = new_pos + 1000
    db.session.flush()

    for step in wf.steps:
        step.position -= 1000
        if step.depends_on_pos is not None:
            step.depends_on_pos = pos_remap.get(step.depends_on_pos, step.depends_on_pos)

    wf.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "workflow": _wf_full(wf)})


@bp_research.route("/workflows/<int:wid>/step/<int:pos>/add-field", methods=["POST"])
def add_field(wid: int, pos: int):
    """
    Append a field descriptor to the step's fields_json array.

    Body: { path, role?, label? }
    Returns the updated step.
    """
    wf   = HarvestWorkflow.query.get_or_404(wid)
    step = next((s for s in wf.steps if s.position == pos), None)
    if not step:
        return jsonify({"ok": False, "error": f"Step {pos} not found"}), 404

    d     = request.json or {}
    path  = (d.get("path")  or "").strip()
    role  = (d.get("role")  or "custom").strip()
    label = (d.get("label") or path or role).strip()

    if not path:
        return jsonify({"ok": False, "error": "path is required"}), 400

    fields = _json_safe(step.fields_json, [])
    fields.append({"path": path, "role": role, "label": label, "store": True})
    step.fields_json  = json.dumps(fields)
    wf.updated_at     = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "step": _step_full(step)})


@bp_research.route("/workflows/<int:wid>/step/<int:pos>/field/<int:idx>", methods=["DELETE"])
def delete_field(wid: int, pos: int, idx: int):
    """Remove one field from a step's fields_json by index."""
    wf   = HarvestWorkflow.query.get_or_404(wid)
    step = next((s for s in wf.steps if s.position == pos), None)
    if not step:
        return jsonify({"ok": False, "error": f"Step {pos} not found"}), 404

    fields = _json_safe(step.fields_json, [])
    if idx >= len(fields):
        return jsonify({"ok": False, "error": f"field index {idx} out of range"}), 400

    fields.pop(idx)
    step.fields_json  = json.dumps(fields)
    wf.updated_at     = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "step": _step_full(step)})


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES — MARKET CATALOGUE
# ═════════════════════════════════════════════════════════════════════════════

@bp_research.route("/markets", methods=["GET"])
def list_markets():
    """
    Return all market definitions, optionally filtered by ?sport=Football.

    Response:
    { ok, sport, total, primary_markets, markets: [{ name, slug, description, sport, is_primary }] }
    """
    sport_name = _resolve_sport_name(request.args.get("sport"))

    if sport_name and not any(k and k.lower() == sport_name.lower() for k in MARKETS_BY_SPORT):
        return jsonify({
            "ok":    False,
            "error": (
                f"Unknown sport '{sport_name}'. "
                f"Known sports: {sorted(k for k in MARKETS_BY_SPORT if k)}"
            ),
        }), 404

    markets       = get_markets_for_sport(sport_name)
    primary_defs  = get_primary_markets(sport_name)
    primary_slugs = {m.slug for m in primary_defs}

    for m in markets:
        m["is_primary"] = m["slug"] in primary_slugs

    return jsonify({
        "ok":              True,
        "sport":           sport_name,
        "total":           len(markets),
        "primary_markets": [m.to_dict() for m in primary_defs],
        "markets":         markets,
    })


@bp_research.route("/markets/sports", methods=["GET"])
def list_market_sports():
    """Return all sport names in the catalogue."""
    sports = sorted(k for k in MARKETS_BY_SPORT if k is not None)
    return jsonify({"ok": True, "sports": sports, "total": len(sports)})


@bp_research.route("/markets/<path:sport_name>", methods=["GET"])
def get_sport_markets(sport_name: str):
    """
    Full market list + primary markets + coverage reference for one sport.

    Adds coverage_info block:
    { match_list_requires, full_market_expected, threshold_pct }
    """
    canonical = _resolve_sport_name(sport_name)

    if not any(k and k.lower() == (canonical or "").lower() for k in MARKETS_BY_SPORT):
        return jsonify({
            "ok":    False,
            "error": (
                f"Sport '{sport_name}' not found in catalogue. "
                f"Known: {sorted(k for k in MARKETS_BY_SPORT if k)}"
            ),
        }), 404

    markets       = get_markets_for_sport(canonical)
    primary_defs  = get_primary_markets(canonical)
    primary_slugs = {m.slug for m in primary_defs}

    for m in markets:
        m["is_primary"] = m["slug"] in primary_slugs

    return jsonify({
        "ok":              True,
        "sport":           canonical,
        "total":           len(markets),
        "primary_markets": [m.to_dict() for m in primary_defs],
        "markets":         markets,
        "coverage_info": {
            "match_list_requires":  [m.to_dict() for m in primary_defs],
            "full_market_expected": len(markets),
            "threshold_pct":        50,
        },
    })


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES — PARSER
# ═════════════════════════════════════════════════════════════════════════════

@bp_research.route("/parser/test", methods=["POST"])
def test_parser():
    """
    Stateless parser test.

    Body:
    {
      code, sample_json|sample_raw,
      sport?,              # enables coverage check
      workflow_type?,      # default "MARKETS_ONLY"
      coverage_threshold?  # default 50
    }
    """
    d = request.json or {}

    code = (d.get("code") or "").strip()
    if not code:
        return jsonify({"ok": False, "error": "code is required"}), 400

    sample: Any = d.get("sample_json")
    if sample is None:
        raw = (d.get("sample_raw") or "").strip()
        if raw:
            try:
                sample = json.loads(raw)
            except json.JSONDecodeError as exc:
                return jsonify({"ok": False, "error": f"Invalid JSON: {exc}"}), 400
        else:
            sample = {}

    result = _safe_run_parser(code, sample)

    sport, workflow_type, threshold = _parse_coverage_params(d)
    if sport or workflow_type != "MARKETS_ONLY":
        enrich_test_result_with_coverage(result, sport, workflow_type, threshold)

    return jsonify(result)



# =============================================================================
# PARSER  —  step-scoped
# Each step stores its own parse_data() independently.
# =============================================================================

def _resolve_step(wf: HarvestWorkflow, pos: int):
    """Return the step at the given position or None."""
    return next((s for s in wf.steps if s.position == pos), None)


@bp_research.route("/workflows/<int:wid>/step/<int:pos>/parser", methods=["GET"])
def get_step_parser(wid: int, pos: int):
    """
    Return the parser code + test status for one step, plus the full
    market context derived from the workflow's sport.

    Response:
    {
      ok, workflow_id, step_id, step_pos, step_name,
      parser_code, parser_test_passed, sport,
      expected_markets, primary_markets, expected_market_count
    }
    """
    wf   = HarvestWorkflow.query.get_or_404(wid)
    step = _resolve_step(wf, pos)
    if not step:
        return jsonify({"ok": False, "error": f"Step {pos} not found"}), 404

    sport_name = _workflow_sport(wf)

    return jsonify({
        "ok":                    True,
        "workflow_id":           wf.id,
        "step_id":               step.id,
        "step_pos":              step.position,
        "step_name":             step.name,
        "parser_code":           step.parser_code or "",
        "parser_test_passed":    bool(step.parser_test_passed),
        "sport":                 sport_name,
        "expected_markets":      [m.to_dict() for m in get_expected_markets(sport_name)],
        "primary_markets":       [m.to_dict() for m in get_primary_markets(sport_name)],
        "expected_market_count": len(get_expected_markets(sport_name)),
    })


@bp_research.route("/workflows/<int:wid>/step/<int:pos>/parser/test", methods=["POST"])
def test_step_parser(wid: int, pos: int):
    """
    Execute this step's parse_data() against a sample and validate rows.
    Auto-injects the workflow's sport unless overridden in the request body.

    Body:
    {
      code,
      sample_json?        — already-parsed object
      sample_raw?         — JSON string (alternative)
      sport?              — overrides workflow sport (enables coverage)
      workflow_type?      — default "MARKETS_ONLY"
      coverage_threshold? — default 50
    }
    """
    wf   = HarvestWorkflow.query.get_or_404(wid)
    step = _resolve_step(wf, pos)
    if not step:
        return jsonify({"ok": False, "error": f"Step {pos} not found"}), 404

    d = request.json or {}

    # Inject workflow sport as default
    if not d.get("sport"):
        wf_sport = _workflow_sport(wf)
        if wf_sport:
            d = {**d, "sport": wf_sport}

    code = (d.get("code") or "").strip()
    if not code:
        return jsonify({"ok": False, "error": "code is required"}), 400

    sample: Any = d.get("sample_json")
    if sample is None:
        raw = (d.get("sample_raw") or "").strip()
        if raw:
            try:
                sample = json.loads(raw)
            except json.JSONDecodeError as exc:
                return jsonify({"ok": False, "error": f"Invalid JSON: {exc}"}), 400
        else:
            sample = {}

    result = _safe_run_parser(code, sample)

    sport, workflow_type, threshold = _parse_coverage_params(d)
    if sport or workflow_type != "MARKETS_ONLY":
        enrich_test_result_with_coverage(result, sport, workflow_type, threshold)

    return jsonify(result)


@bp_research.route("/workflows/<int:wid>/step/<int:pos>/parser/save", methods=["POST"])
def save_step_parser(wid: int, pos: int):
    """
    Persist parser code and test status onto a specific workflow step.

    If run_test=true the server re-executes the parser (including market
    coverage) before saving.  Returns 422 with test_result if it fails.

    Body:
    {
      code,
      test_passed?,        — client-reported result (used when run_test=false)
      sample_json?,        — required when run_test=true
      run_test?,
      sport?,              — overrides workflow sport for coverage check
      workflow_type?,
      coverage_threshold?
    }

    Response:
    {
      ok, workflow_id, step_id, step_pos, step_name,
      parser_test_passed, sport, msg
    }
    """
    wf   = HarvestWorkflow.query.get_or_404(wid)
    step = _resolve_step(wf, pos)
    if not step:
        return jsonify({"ok": False, "error": f"Step {pos} not found"}), 404

    d           = request.json or {}
    code        = (d.get("code") or "").strip()
    test_passed = bool(d.get("test_passed", False))

    # Sport resolution: body → workflow relationship
    sport_name = _resolve_sport_name(d.get("sport")) or _workflow_sport(wf)

    if d.get("run_test") and code:
        sample: Any   = d.get("sample_json") or {}
        workflow_type = (d.get("workflow_type") or "MARKETS_ONLY").strip().upper()
        threshold     = float(d.get("coverage_threshold") or 50.0)

        result = _safe_run_parser(code, sample)
        if sport_name or workflow_type != "MARKETS_ONLY":
            enrich_test_result_with_coverage(result, sport_name, workflow_type, threshold)

        test_passed = result.get("ok", False)
        if not test_passed:
            return jsonify({
                "ok":          False,
                "error":       "Parser did not pass validation — code NOT saved.",
                "test_result": result,
            }), 422

    step.parser_code        = code or None
    step.parser_test_passed = test_passed
    wf.updated_at           = datetime.now(timezone.utc)

    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({
        "ok":                 True,
        "workflow_id":        wf.id,
        "step_id":            step.id,
        "step_pos":           step.position,
        "step_name":          step.name,
        "parser_test_passed": bool(step.parser_test_passed),
        "sport":              sport_name,
        "msg":                f"Parser saved to step {step.position} (\'{step.name}\').",
    })


# ─────────────────────────────────────────────────────────────────────────────
# HTTP Probe  — POST /research/probe
# ─────────────────────────────────────────────────────────────────────────────

def _do_http(method: str, url: str, headers: dict, params: dict,
             body: str | None, timeout: int = 20) -> tuple[int, bytes, str]:
    """
    Fire a real HTTP request and return (status_code, raw_bytes, content_type).
    Works with both httpx and requests.
    """
    method  = (method or "GET").upper()
    kwargs: dict = dict(
        headers=headers,
        params=params,
        timeout=timeout,
    )
    if body:
        kwargs["content" if _USE_HTTPX else "data"] = body.encode()

    if _USE_HTTPX:
        with _http_lib.Client(follow_redirects=True) as client:
            resp = getattr(client, method.lower())(url, **kwargs)
        return resp.status_code, resp.content, resp.headers.get("content-type", "")
    else:
        resp = getattr(_http_lib, method.lower())(url, **kwargs)
        return resp.status_code, resp.content, resp.headers.get("content-type", "")


def _decompress(raw: bytes, content_type: str) -> bytes:
    """Try gzip / deflate / zlib decompression on non-text responses."""
    try:
        return gzip.decompress(raw)
    except Exception:
        pass
    try:
        return zlib.decompress(raw)
    except Exception:
        pass
    try:
        return zlib.decompress(raw, -zlib.MAX_WBITS)
    except Exception:
        pass
    return raw


def _parse_body(raw: bytes, content_type: str) -> tuple[Any, str]:
    """
    Attempt to decode raw bytes into a Python object.
    Returns (parsed_object_or_None, clean_text).
    """
    if not raw:
        return None, ""

    # Try decompression first (some bookmakers return gzipped even without header)
    if not raw[:1] in (b"{", b"[", b'"'):
        raw = _decompress(raw, content_type)

    try:
        text = raw.decode("utf-8", errors="replace")
    except Exception:
        text = ""

    # Strip JSONP wrapper  e.g. callback({...})
    text_stripped = re.sub(r"^\s*\w+\s*\(", "", text).rstrip(");").strip()

    for candidate in (text_stripped, text):
        try:
            parsed = json.loads(candidate)
            return parsed, candidate
        except Exception:
            pass

    return None, text


def _build_schema(obj: Any, depth: int = 0, max_depth: int = 3) -> list[str]:
    """Recursively sketch the JSON shape as dot-path lines."""
    lines: list[str] = []
    if depth >= max_depth:
        return lines
    if isinstance(obj, dict):
        for k, v in list(obj.items())[:30]:
            t = type(v).__name__
            lines.append(f"{'  ' * depth}{k}: {t}")
            lines.extend(_build_schema(v, depth + 1, max_depth))
    elif isinstance(obj, list) and obj:
        lines.append(f"{'  ' * depth}[{len(obj)} items]")
        lines.extend(_build_schema(obj[0], depth + 1, max_depth))
    return lines



# Assuming you have a blueprint defined. If not, replace @bp_research with @app

# ── Helper Functions ──────────────────────────────────────────────────────────

def _parse_body(raw_bytes: bytes, content_type: str) -> tuple[Any | None, str]:
    """Decodes bytes and attempts to parse JSON."""
    text = raw_bytes.decode("utf-8", errors="replace")
    parsed = None
    if "json" in content_type.lower() or text.strip().startswith(("{", "[")):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            pass
    return parsed, text

def _build_schema(obj: Any, prefix: str = "") -> list[str]:
    """Generates a dot-path schema sketch of the parsed JSON."""
    lines = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            lines.add(f"{prefix}{k}: {type(v).__name__}")
            if isinstance(v, (dict, list)):
                lines.update(_build_schema(v, f"{prefix}{k}."))
    elif isinstance(obj, list) and obj:
        lines.update(_build_schema(obj[0], f"{prefix}[0]."))
    return sorted(list(lines))

# ── Route ─────────────────────────────────────────────────────────────────────

@bp_research.route("/probe", methods=["POST"])
def probe_url():
    """
    POST /research/probe
    ════════════════════
    Fire an HTTP request on behalf of the frontend and return structured results.
    Mimics a modern Chrome browser perfectly (Headers + TLS fingerprinting) to 
    bypass bookmaker WAFs.
    """
    d       = request.json or {}
    url     = (d.get("url") or "").strip()
    method  = (d.get("method") or "GET").upper()
    headers = d.get("headers") or {}
    params  = d.get("params")  or {}
    body    = d.get("body")

    if not url:
        return jsonify({
            "ok": False, "error": "url is required", "status": None,
            "latency_ms": 0, "parsed": None, "response_raw": "",
            "first_item": None, "array_key": None, "array_length": 0,
            "schema_lines": [], "size_bytes": 0, "content_type": "",
            "curl": ""
        }), 400

    # 1. Base Browser Headers (Mimics a frontend XHR/Fetch request)
    # Using Chrome 120 headers to match the curl_cffi impersonate target
    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin", # Adjust to cross-site if needed
        "Connection": "keep-alive"
    }

    # Custom headers from frontend override the default browser headers
    # Case-insensitive merging could be added here if needed
    final_headers = {**browser_headers, **headers}

    # 2. Build equivalent curl string for debugging
    header_flags = " ".join(f"-H '{k}: {v}'" for k, v in final_headers.items())
    param_str    = urllib.parse.urlencode(params)
    curl_url     = f"{url}?{param_str}" if param_str else url
    
    body_flag = ""
    if body:
        body_str = json.dumps(body) if isinstance(body, (dict, list)) else str(body)
        body_flag = f" -d '{body_str}'"
        
    curl_cmd = f"curl -X {method} {header_flags}{body_flag} '{curl_url}'"
    print(curl_cmd)
    # 3. Fire the request with TLS impersonation
    t0 = time.perf_counter()
    try:
        # 'impersonate="chrome120"' forces the correct Ciphers, JA3, and HTTP2 frames
        resp = tls_requests.request(
            method=method,
            url=url,
            headers=final_headers,
            params=params,
            json=body if body and isinstance(body, (dict, list)) else None,
            data=body if body and isinstance(body, str) else None,
            impersonate="chrome120",
            timeout=15
        )
        print(resp.content)
        status_code  = resp.status_code
        raw_bytes    = resp.content
        content_type = resp.headers.get("Content-Type", "")

    except Exception as exc:
        print(f"Error during HTTP probe: {exc}")
        return jsonify({
            "ok": False, "status": None,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "content_type": "", "size_bytes": 0,
            "parsed": None, "response_raw": "",
            "first_item": None, "array_key": None, "array_length": 0,
            "schema_lines": [], "error": str(exc), "curl": curl_cmd,
        })

    latency_ms = int((time.perf_counter() - t0) * 1000)
    size_bytes = len(raw_bytes)
    
    # 4. Parse and inspect structure
    parsed, raw_text = _parse_body(raw_bytes, content_type)

    first_item:   Any        = None
    array_key:    str | None = None
    array_length: int        = 0

    if isinstance(parsed, list):
        array_length = len(parsed)
        first_item   = parsed[0] if parsed else None
    elif isinstance(parsed, dict):
        for k, v in parsed.items():
            if isinstance(v, list):
                array_key    = k
                array_length = len(v)
                first_item   = v[0] if v else None
                break

    schema_lines = _build_schema(parsed) if parsed is not None else []
    ok = 200 <= status_code < 300 and parsed is not None

    return jsonify({
        "ok":           ok,
        "status":       status_code,
        "latency_ms":   latency_ms,
        "content_type": content_type,
        "size_bytes":   size_bytes,
        "parsed":       parsed,
        "response":     parsed,          
        "response_raw": raw_text[:16_000],
        "first_item":   first_item,
        "array_key":    array_key,
        "array_length": array_length,
        "schema_lines": schema_lines,
        "error":        None if ok else f"HTTP {status_code}",
        "curl":         curl_cmd,
    })
# ─────────────────────────────────────────────────────────────────────────────

@bp_research.route("/workflows/<int:wid>/step/<int:pos>/parser", methods=["DELETE"])
def clear_step_parser(wid: int, pos: int):
    """Clear parser code and reset test status for one step."""
    wf   = HarvestWorkflow.query.get_or_404(wid)
    step = _resolve_step(wf, pos)
    if not step:
        return jsonify({"ok": False, "error": f"Step {pos} not found"}), 404

    step.parser_code        = None
    step.parser_test_passed = False
    wf.updated_at           = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({
        "ok":       True,
        "step_pos": step.position,
        "step_name":step.name,
        "msg":      f"Parser cleared from step {step.position} (\'{step.name}\').",
    })

# ── Workflow-level parser aliases (called by WorkflowParserPane) ──────────────
# These proxy to step 1's parser for backwards compatibility.

@bp_research.route("/workflows/<int:wid>/parser", methods=["GET"])
def get_workflow_parser(wid: int):
    wf   = HarvestWorkflow.query.get_or_404(wid)
    step = _resolve_step(wf, 1)
    if not step:
        return jsonify({"ok": False, "error": "No steps found"}), 404
    sport_name = _workflow_sport(wf)
    return jsonify({
        "ok":                    True,
        "parser_code":           step.parser_code or "",
        "parser_test_passed":    bool(step.parser_test_passed),
        "sport":                 sport_name,
        "expected_markets":      [m.to_dict() for m in get_expected_markets(sport_name)],
        "primary_markets":       [m.to_dict() for m in get_primary_markets(sport_name)],
        "expected_market_count": len(get_expected_markets(sport_name)),
    })


@bp_research.route("/workflows/<int:wid>/parser/test", methods=["POST"])
def test_workflow_parser(wid: int):
    wf   = HarvestWorkflow.query.get_or_404(wid)
    step = _resolve_step(wf, 1)
    if not step:
        return jsonify({"ok": False, "error": "No steps found"}), 404
    d = request.json or {}
    if not d.get("sport"):
        wf_sport = _workflow_sport(wf)
        if wf_sport:
            d = {**d, "sport": wf_sport}
    code = (d.get("code") or "").strip()
    if not code:
        return jsonify({"ok": False, "error": "code is required"}), 400
    sample = d.get("sample_json")
    if sample is None:
        raw = (d.get("sample_raw") or "").strip()
        sample = json.loads(raw) if raw else {}
    result = _safe_run_parser(code, sample)
    sport, workflow_type, threshold = _parse_coverage_params(d)
    if sport or workflow_type != "MARKETS_ONLY":
        enrich_test_result_with_coverage(result, sport, workflow_type, threshold)
    return jsonify(result)


@bp_research.route("/workflows/<int:wid>/parser/save", methods=["POST"])
def save_workflow_parser(wid: int):
    wf   = HarvestWorkflow.query.get_or_404(wid)
    step = _resolve_step(wf, 1)
    if not step:
        return jsonify({"ok": False, "error": "No steps found"}), 404
    d           = request.json or {}
    code        = (d.get("code") or "").strip()
    test_passed = bool(d.get("test_passed", False))
    step.parser_code        = code or None
    step.parser_test_passed = test_passed
    wf.updated_at           = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "msg": "Parser saved."})

# Import and register the new routes
# import app.api.workflow_api as _self  # noqa — just for docstring context
 
# ─ Route 1: generate parser code from onboarding field maps ─────────────────
# Place this block inside workflow_api.py after the existing parser routes:
 

# @bp_research.route("/workflows/<int:wid>/parser/generate", methods=["POST"])
# def generate_workflow_parser(wid: int):
#     return generate_parser_route(wid)
 
 
# @bp_research.route("/workflows/<int:wid>/parser/iterate-sports", methods=["POST"])
# def iterate_workflow_parser_sports(wid: int):
#     return iterate_sports_route(wid)

 
# ─────────────────────────────────────────────────────────────────────────────
# EXACT TEXT TO PASTE into workflow_api.py
# (add after the  clear_step_parser  route, around line 370)
# ─────────────────────────────────────────────────────────────────────────────
 
# ── Parser generation from onboarding session ─────────────────────────────────
 
 
 
@bp_research.route("/workflows/<int:wid>/parser/generate", methods=["POST"])
def generate_workflow_parser(wid: int):
    """
    Generate parse_data() code from the bookmaker\'s onboarding session field maps.
    Falls back to step field-role maps if no session exists.
 
    Returns: {ok, code, sport_name, session_id, steps_used, from_session, warning?}
    """
    return generate_parser_route(wid)
 
 
@bp_research.route("/workflows/<int:wid>/parser/iterate-sports", methods=["POST"])
def iterate_workflow_parser_sports(wid: int):
    """
    Execute the parser against the LIVE API for every configured sport variant.
 
    For each sport in the bookmaker\'s onboarding session sport_mappings:
      1. Probe the list endpoint with that sport\'s ID substituted
      2. Take first N matches (default 3)
      3. For each match: probe markets endpoint with {{match_id}} resolved
      4. Merge: list item + {step2_key: markets_data}
      5. Run parse_data(merged_item)
      6. Check market coverage against mapping_seed catalogue for that sport
 
    Body: {code, limit_per_sport?}
    Returns: {ok, all_ok, results: [...], summary: {sports_tested, sports_passed,
              total_rows, avg_coverage}}
    """
    return iterate_sports_route(wid)
