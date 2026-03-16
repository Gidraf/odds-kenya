"""
app/api/onboarding_api.py
=========================
Bookmaker onboarding wizard API.

Blueprint: bp_onboarding  (register with url_prefix="/onboarding")
"""

from __future__ import annotations

import json
import re
import urllib.parse
from datetime import datetime, timezone
from typing import Any

from flask import Blueprint, request, jsonify

from app.extensions import db
from app.models.bookmakers_model import Bookmaker, BookmakerEndpoint
from app.models.onboarding_model import BookmakerOnboardingSession
from app.models.harvest_workflow import HarvestWorkflow, HarvestWorkflowStep
from app.models.bookmakers_model import BookmakerEntityValue
from app.utils.probe_curl import _probe, _detect_placeholders

bp_onboarding = Blueprint("onboarding", __name__)


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

_AUTH_HEADER_PATTERNS = re.compile(
    r"(authorization|x-api-key|x-auth-token|bearer|token|secret|api[_-]?key)",
    re.I,
)

_VALID_HEADER_NAME_RE = re.compile(r'^[a-zA-Z0-9!#$%&\'*+\-.^_`|~]+$')


def _sanitize_headers(headers: dict | None) -> dict:
    if not headers:
        return {}
    clean = {}
    for k, v in headers.items():
        name  = str(k).strip()
        value = str(v).strip() if v is not None else ""
        if not name:
            continue
        if not _VALID_HEADER_NAME_RE.match(name):
            print(f"[probe] Skipping invalid header name: {name!r}")
            continue
        if not value:
            continue
        clean[name] = value
    return clean


def _get_nested(obj: Any, path: str) -> Any:
    if not path or obj is None:
        return obj
    for part in path.split("."):
        if isinstance(obj, dict):
            obj = obj.get(part)
        elif isinstance(obj, list):
            obj = [item.get(part) if isinstance(item, dict) else None for item in obj]
        else:
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


def _field_map_to_fields(field_map: dict | None) -> list[dict]:
    if not field_map:
        return []
    return [
        {"path": path, "role": role, "label": role.replace("_", " ").title(), "store": True}
        for role, path in field_map.items()
        if path
    ]


def _detect_auth_headers(headers: dict) -> list[dict]:
    out = []
    for k, v in (headers or {}).items():
        if _AUTH_HEADER_PATTERNS.search(k):
            out.append({"key": k, "value": v, "is_token": True})
        else:
            out.append({"key": k, "value": v, "is_token": False})
    return out


def _resolve_template(
    url_template: str,
    params: dict,
    placeholder_map: dict,
    item: dict,
    field_map: dict,
    body_template: str | None = None,
) -> tuple[str, dict, str | None]:
    """
    Replace {{var_name}} in url_template, params, and body_template using:
    1. placeholder_map: {var → list_field_path} → value from item
    2. field_map role match → value from item
    3. Direct key match on item
    """
    def resolve_var(var_name: str) -> str:
        path = placeholder_map.get(var_name)
        if path:
            v = _get_nested(item, path)
            if v is not None:
                return str(v)
        role_path = field_map.get(var_name)
        if role_path:
            v = _get_nested(item, role_path)
            if v is not None:
                return str(v)
        if var_name in item:
            return str(item[var_name])
        return f"<{var_name}>"

    resolved_url = re.sub(
        r"\{\{(\w+)\}\}",
        lambda m: resolve_var(m.group(1)),
        url_template,
    )
    resolved_params = {
        k: re.sub(r"\{\{(\w+)\}\}", lambda m: resolve_var(m.group(1)), str(v))
        for k, v in params.items()
    }
    resolved_body = None
    if body_template:
        resolved_body = re.sub(
            r"\{\{(\w+)\}\}",
            lambda m: resolve_var(m.group(1)),
            str(body_template),
        )
    return resolved_url, resolved_params, resolved_body


# ═════════════════════════════════════════════════════════════════════════════
# SESSION CRUD
# ═════════════════════════════════════════════════════════════════════════════

@bp_onboarding.route("/sessions", methods=["POST"])
def create_session():
    d     = request.json or {}
    bk_id = d.get("bookmaker_id")
    if not bk_id:
        return jsonify({"ok": False, "error": "bookmaker_id required"}), 400

    Bookmaker.query.get_or_404(bk_id)

    existing = BookmakerOnboardingSession.query.filter_by(
        bookmaker_id=bk_id, is_complete=False
    ).all()
    if len(existing) > 1:
        for s in existing[:-1]:
            db.session.delete(s)

    session = BookmakerOnboardingSession(bookmaker_id=bk_id)
    db.session.add(session)
    db.session.commit()
    return jsonify({"ok": True, "session": session.to_dict()}), 201


@bp_onboarding.route("/sessions/<int:sid>", methods=["GET"])
def get_session(sid: int):
    session = BookmakerOnboardingSession.query.get_or_404(sid)
    return jsonify({"ok": True, "session": session.to_dict()})


@bp_onboarding.route("/sessions/<int:sid>", methods=["PATCH"])
def patch_session(sid: int):
    session = BookmakerOnboardingSession.query.get_or_404(sid)
    d = request.json or {}

    FLAT_FIELDS = {
        "list_url", "list_url_raw", "list_method",
        "list_headers", "list_params", "list_body",
        "list_array_path", "list_field_map", "list_sport_id_path",
        "list_ok", "sport_mappings",
        "markets_url_template", "markets_url_raw", "markets_method",
        "markets_headers", "markets_params",
        "markets_body_template", "markets_array_path", "markets_field_map",
        "markets_placeholder_map", "markets_ok",
        "live_list_url", "live_list_url_raw", "live_list_method",
        "live_list_headers", "live_list_params",
        "live_list_body", "live_list_array_path", "live_list_field_map", "live_list_ok",
        "live_markets_url_template", "live_markets_url_raw", "live_markets_method",
        "live_markets_headers", "live_markets_params",
        "live_markets_body_template", "live_markets_array_path",
        "live_markets_field_map", "live_markets_placeholder_map", "live_markets_ok",
        "current_phase",
    }

    for field in FLAT_FIELDS:
        if field in d and hasattr(session, field):
            setattr(session, field, d[field])

    def _apply_sub(prefix: str, sub: dict):
        body_attr = f"{prefix}_body_template" if "markets" in prefix else f"{prefix}_body"
        url_attr  = f"{prefix}_url_template"  if "markets" in prefix else f"{prefix}_url"

        mapping = {
            "url":             url_attr,
            "url_raw":         f"{prefix}_url_raw",
            "url_template":    f"{prefix}_url_template",
            "method":          f"{prefix}_method",
            "headers":         f"{prefix}_headers",
            "params":          f"{prefix}_params",
            "body":            body_attr,
            "body_template":   f"{prefix}_body_template",
            "array_path":      f"{prefix}_array_path",
            "field_map":       f"{prefix}_field_map",
            "placeholder_map": f"{prefix}_placeholder_map",
            "sport_id_path":   f"{prefix}_sport_id_path",
        }
        for k, attr in mapping.items():
            if k in sub and hasattr(session, attr):
                setattr(session, attr, sub[k])

    for prefix in ("list", "markets", "live_list", "live_markets"):
        if prefix in d and isinstance(d[prefix], dict):
            _apply_sub(prefix, d[prefix])

    session.updated_at = datetime.now(timezone.utc)
    db.session.commit()
    return jsonify({"ok": True, "session": session.to_dict()})


@bp_onboarding.route("/sessions/<int:sid>", methods=["DELETE"])
def delete_session(sid: int):
    session = BookmakerOnboardingSession.query.get_or_404(sid)
    db.session.delete(session)
    db.session.commit()
    return jsonify({"ok": True, "msg": f"Session {sid} deleted."})


@bp_onboarding.route("/bookmakers/<int:bk_id>/session", methods=["GET"])
def get_bookmaker_session(bk_id: int):
    session = (
        BookmakerOnboardingSession.query
        .filter_by(bookmaker_id=bk_id, is_complete=False)
        .order_by(BookmakerOnboardingSession.updated_at.desc())
        .first()
    )
    if not session:
        return jsonify({"ok": False, "error": "No active session"}), 404
    return jsonify({"ok": True, "session": session.to_dict()})


# ═════════════════════════════════════════════════════════════════════════════
# PROBE
# ═════════════════════════════════════════════════════════════════════════════

@bp_onboarding.route("/sessions/<int:sid>/probe", methods=["POST"])
def probe(sid: int):
    session_obj = BookmakerOnboardingSession.query.get_or_404(sid)
    d           = request.json or {}
    url         = (d.get("url") or "").strip()
    method      = (d.get("method") or "GET").upper()
    phase       = (d.get("phase") or "").strip().lower()
    url_raw     = (d.get("url_raw") or "").strip()

    if not url:
        return jsonify({"ok": False, "error": "url is required"}), 400

    result = _probe(
        url     = url,
        method  = method,
        headers = d.get("headers") or {},
        params  = d.get("params") or {},
        body    = d.get("body"),
        url_raw = url_raw,
    )

    if phase and result["ok"] and result["parsed"] is not None:
        session = BookmakerOnboardingSession.query.get(sid)
        items   = _extract_items(result["parsed"], d.get("array_path"))
        sample  = json.dumps(items[:10])

        phase_map = {
            "list":         ("list_sample",        "list_ok",        "list_array_path",        "list_url_raw"),
            "markets":      ("markets_sample",      "markets_ok",     "markets_array_path",     "markets_url_raw"),
            "live_list":    ("live_list_sample",    "live_list_ok",   "live_list_array_path",   "live_list_url_raw"),
            "live_markets": ("live_markets_sample", "live_markets_ok","live_markets_array_path","live_markets_url_raw"),
        }
        if phase in phase_map:
            sample_attr, ok_attr, ap_attr, raw_attr = phase_map[phase]
            setattr(session, sample_attr, sample)
            setattr(session, ok_attr, True)
            if d.get("array_path") is not None:
                setattr(session, ap_attr, d["array_path"])
            if url_raw and hasattr(session, raw_attr):
                existing_raw = getattr(session, raw_attr, None)
                if not existing_raw:
                    setattr(session, raw_attr, url_raw)
            session.updated_at = datetime.now(timezone.utc)
            db.session.commit()

    result["headers_annotated"] = _detect_auth_headers(d.get("headers") or {})
    return jsonify(result)


# ═════════════════════════════════════════════════════════════════════════════
# DETECT PLACEHOLDERS
# ═════════════════════════════════════════════════════════════════════════════

@bp_onboarding.route("/sessions/<int:sid>/detect-placeholders", methods=["POST"])
def detect_placeholders_route(sid: int):
    session = BookmakerOnboardingSession.query.get_or_404(sid)
    d       = request.json or {}

    url    = (d.get("url") or "").strip()
    params = d.get("params") or {}

    if not url:
        return jsonify({"ok": False, "error": "url is required"}), 400

    url_raw = (
        (d.get("url_raw") or "").strip()
        or (getattr(session, "markets_url_raw",      None) or "").strip()
        or (getattr(session, "live_markets_url_raw", None) or "").strip()
    )

    list_sample: list = []
    raw_sample = session.list_sample or session.live_list_sample
    if raw_sample:
        try:
            list_sample = json.loads(raw_sample)
        except Exception:
            pass

    first_item = list_sample[0] if list_sample else {}

    result = _detect_placeholders(
        markets_url    = url,
        markets_params = params,
        list_item      = first_item,
        url_raw        = url_raw,
    )
    return jsonify({"ok": True, **result})


# ═════════════════════════════════════════════════════════════════════════════
# ITERATE SPORTS
# ═════════════════════════════════════════════════════════════════════════════

@bp_onboarding.route("/sessions/<int:sid>/iterate-sports", methods=["POST"])
def iterate_sports(sid: int):
    session = BookmakerOnboardingSession.query.get_or_404(sid)
    d       = request.json or {}
    limit   = int(d.get("limit") or 99)

    if not session.list_url:
        return jsonify({"ok": False, "error": "list_url not set"}), 400

    mappings = session.sport_mappings or []
    if not mappings:
        return jsonify({"ok": False, "error": "No sport_mappings configured"}), 400

    saved_raw        = (getattr(session, "list_url_raw", None) or "").strip()
    results          = []
    updated_mappings = []

    for mapping in mappings[:limit]:
        bk_sport_id = str(mapping.get("bk_sport_id") or "")
        param_key   = mapping.get("param_key") or ""
        param_in    = mapping.get("param_in") or "query"

        probe_url    = session.list_url
        probe_params = dict(session.list_params or {})

        if param_key and param_in == "query":
            probe_params[param_key] = bk_sport_id
        elif param_key and param_in == "path":
            probe_url = probe_url.replace(f"{{{{{param_key}}}}}", bk_sport_id)

        # _probe now completely handles resolving the params dict into the url_raw ordering.
        result = _probe(
            url     = probe_url,
            method  = session.list_method or "GET",
            headers = session.list_headers or {},
            params  = probe_params,
            body    = getattr(session, "list_body", None),
            url_raw = saved_raw, 
        )

        items  = _extract_items(result.get("parsed"), session.list_array_path)
        status = "ok" if result["ok"] else "error"

        updated_mappings.append({
            **mapping,
            "status":     status,
            "item_count": len(items),
            "sample":     items[:3] if items else [],
            "error":      result.get("error") if not result["ok"] else None,
        })
        results.append({
            "bk_sport_id": bk_sport_id,
            "sport_name":  mapping.get("sport_name"),
            "status":      status,
            "item_count":  len(items),
            "http_status": result.get("status"),
            "latency_ms":  result.get("latency_ms"),
            "error":       result.get("error"),
        })

    session.sport_mappings = updated_mappings + mappings[limit:]
    session.updated_at = datetime.now(timezone.utc)
    db.session.commit()

    all_ok = all(r["status"] == "ok" for r in results)
    return jsonify({"ok": all_ok, "results": results, "session": session.to_dict()})


# ═════════════════════════════════════════════════════════════════════════════
# ITERATE MARKETS
# ═════════════════════════════════════════════════════════════════════════════

@bp_onboarding.route("/sessions/<int:sid>/iterate-markets", methods=["POST"])
def iterate_markets(sid: int):
    session = BookmakerOnboardingSession.query.get_or_404(sid)
    d       = request.json or {}
    limit   = int(d.get("limit") or 5)
    phase   = (d.get("phase") or "markets").strip().lower()

    if phase == "live_markets":
        url_template    = session.live_markets_url_template
        url_raw_tmpl    = (getattr(session, "live_markets_url_raw", None) or "").strip()
        method          = session.live_markets_method or "GET"
        headers         = session.live_markets_headers or {}
        params          = session.live_markets_params or {}
        body_template   = getattr(session, "live_markets_body_template", None)
        placeholder_map = session.live_markets_placeholder_map or {}
        field_map       = session.live_list_field_map or {}
        raw_sample      = session.live_list_sample
        array_path      = session.live_markets_array_path
    else:
        url_template    = session.markets_url_template
        url_raw_tmpl    = (getattr(session, "markets_url_raw", None) or "").strip()
        method          = session.markets_method or "GET"
        headers         = session.markets_headers or {}
        params          = session.markets_params or {}
        body_template   = getattr(session, "markets_body_template", None)
        placeholder_map = session.markets_placeholder_map or {}
        field_map       = session.list_field_map or {}
        raw_sample      = session.list_sample
        array_path      = session.markets_array_path

    if not url_template:
        return jsonify({"ok": False, "error": f"{phase} url_template not set"}), 400

    items: list = []
    if raw_sample:
        try:
            items = json.loads(raw_sample)
        except Exception:
            pass

    if not items:
        return jsonify({"ok": False, "error": "No list sample available — probe the list first"}), 400

    results = []
    for item in items[:limit]:
        resolved_url, resolved_params, resolved_body = _resolve_template(
            url_template, params, placeholder_map, item, field_map, body_template
        )

        # Since resolved_params contains the final values (e.g. match_id=1234),
        # passing url_raw_tmpl to _probe will seamlessly inject them into the exact right place.
        result = _probe(
            url     = resolved_url,
            method  = method,
            headers = headers,
            params  = resolved_params,
            body    = resolved_body,
            url_raw = url_raw_tmpl, 
        )
        mkt_items = _extract_items(result.get("parsed"), array_path)

        match_id_path = field_map.get("match_id") or "id"
        match_id_val  = _get_nested(item, match_id_path)

        results.append({
            "match_id":     str(match_id_val) if match_id_val is not None else "?",
            "resolved_url": resolved_url,
            "status":       "ok" if result["ok"] else "error",
            "http_status":  result.get("status"),
            "latency_ms":   result.get("latency_ms"),
            "market_count": len(mkt_items),
            "sample":       mkt_items[:2],
            "error":        result.get("error"),
        })

    all_ok = all(r["status"] == "ok" for r in results)

    if all_ok and results:
        first_ok = next((r for r in results if r["status"] == "ok"), None)
        if first_ok and phase == "markets":
            session.markets_sample = json.dumps(first_ok["sample"])
            session.markets_ok     = True
        elif first_ok and phase == "live_markets":
            session.live_markets_sample = json.dumps(first_ok["sample"])
            session.live_markets_ok     = True
        session.updated_at = datetime.now(timezone.utc)
        db.session.commit()

    return jsonify({"ok": all_ok, "results": results, "session": session.to_dict()})


# ═════════════════════════════════════════════════════════════════════════════
# COMPLETE — save everything
# ═════════════════════════════════════════════════════════════════════════════

@bp_onboarding.route("/sessions/<int:sid>/complete", methods=["POST"])
def complete_session(sid: int):
    session = BookmakerOnboardingSession.query.get_or_404(sid)

    if not session.list_ok:
        return jsonify({"ok": False, "error": "List phase must pass before saving"}), 422

    workflow_ids: dict = {}
    endpoint_ids: list = []
    now = datetime.now(timezone.utc)

    try:
        # ── Upcoming workflow ─────────────────────────────────────────────────
        upcoming_wf = HarvestWorkflow(
            bookmaker_id=session.bookmaker_id,
            name="Upcoming Matches",
            description="Created by onboarding wizard",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        db.session.add(upcoming_wf)
        db.session.flush()

        list_step = HarvestWorkflowStep(
            workflow_id=upcoming_wf.id,
            position=1,
            name="Upcoming Match List",
            step_type="FETCH_LIST",
            url_template=session.list_url or "",
            url_raw=session.list_url_raw or "", # PASSED TO DB FOR CELERY
            method=(session.list_method or "GET").upper(),
            headers_json=json.dumps(session.list_headers or {}),
            params_json=json.dumps(session.list_params or {}),
            body_template=getattr(session, "list_body", None),
            result_array_path=session.list_array_path,
            fields_json=json.dumps(_field_map_to_fields(session.list_field_map)),
        )
        db.session.add(list_step)

        if session.markets_ok and session.markets_url_template:
            markets_step = HarvestWorkflowStep(
                workflow_id=upcoming_wf.id,
                position=2,
                name="Match Markets",
                step_type="FETCH_PER_ITEM",
                url_template=session.markets_url_template,
                url_raw=session.markets_url_raw or "", # PASSED TO DB FOR CELERY
                method=(session.markets_method or "GET").upper(),
                headers_json=json.dumps(session.markets_headers or {}),
                params_json=json.dumps(session.markets_params or {}),
                body_template=getattr(session, "markets_body_template", None),
                result_array_path=session.markets_array_path,
                fields_json=json.dumps(_field_map_to_fields(session.markets_field_map)),
                depends_on_pos=1,
            )
            if hasattr(markets_step, "field_mappings_json"):
                markets_step.field_mappings_json = json.dumps(
                    session.markets_placeholder_map or {}
                )
            db.session.add(markets_step)

        workflow_ids["upcoming"] = upcoming_wf.id

        # ── BookmakerEndpoint — list ──────────────────────────────────────────
        list_ep = BookmakerEndpoint(
            bookmaker_id=session.bookmaker_id,
            endpoint_type="MATCH_LIST",
            url_pattern=session.list_url or "",
            url_raw=session.list_url_raw or "", # PASSED TO DB FOR CELERY
            request_method=(session.list_method or "GET").upper(),
            headers_json=session.list_headers or {},
            sample_response=(session.list_sample or "")[:2000],
            is_active=True,
        )
        db.session.add(list_ep)
        db.session.flush()
        endpoint_ids.append(list_ep.id)

        if session.markets_ok and session.markets_url_template:
            mkt_ep = BookmakerEndpoint(
                bookmaker_id=session.bookmaker_id,
                endpoint_type="DEEP_MARKETS",
                url_pattern=session.markets_url_template,
                url_raw=session.markets_url_raw or "", # PASSED TO DB FOR CELERY
                request_method=(session.markets_method or "GET").upper(),
                headers_json=session.markets_headers or {},
                sample_response=(session.markets_sample or "")[:2000],
                is_active=True,
            )
            db.session.add(mkt_ep)
            db.session.flush()
            endpoint_ids.append(mkt_ep.id)

        # ── Live workflow (optional) ───────────────────────────────────────────
        if session.live_list_ok and session.live_list_url:
            live_wf = HarvestWorkflow(
                bookmaker_id=session.bookmaker_id,
                name="Live Matches",
                description="Created by onboarding wizard",
                is_active=True,
                created_at=now,
                updated_at=now,
            )
            db.session.add(live_wf)
            db.session.flush()

            live_list_step = HarvestWorkflowStep(
                workflow_id=live_wf.id,
                position=1,
                name="Live Match List",
                step_type="FETCH_LIST",
                url_template=session.live_list_url,
                url_raw=session.live_list_url_raw or "", # PASSED TO DB FOR CELERY
                method=(session.live_list_method or "GET").upper(),
                headers_json=json.dumps(session.live_list_headers or {}),
                params_json=json.dumps(session.live_list_params or {}),
                body_template=getattr(session, "live_list_body", None),
                result_array_path=session.live_list_array_path,
                fields_json=json.dumps(_field_map_to_fields(session.live_list_field_map)),
            )
            db.session.add(live_list_step)

            if session.live_markets_ok and session.live_markets_url_template:
                live_mkt_step = HarvestWorkflowStep(
                    workflow_id=live_wf.id,
                    position=2,
                    name="Live Match Markets",
                    step_type="FETCH_PER_ITEM",
                    url_template=session.live_markets_url_template,
                    url_raw=session.live_markets_url_raw or "", # PASSED TO DB FOR CELERY
                    method=(session.live_markets_method or "GET").upper(),
                    headers_json=json.dumps(session.live_markets_headers or {}),
                    params_json=json.dumps(session.live_markets_params or {}),
                    body_template=getattr(session, "live_markets_body_template", None),
                    result_array_path=session.live_markets_array_path,
                    fields_json=json.dumps(_field_map_to_fields(session.live_markets_field_map)),
                    depends_on_pos=1,
                )
                if hasattr(live_mkt_step, "field_mappings_json"):
                    live_mkt_step.field_mappings_json = json.dumps(
                        session.live_markets_placeholder_map or {}
                    )
                db.session.add(live_mkt_step)

            workflow_ids["live"] = live_wf.id

            live_list_ep = BookmakerEndpoint(
                bookmaker_id=session.bookmaker_id,
                endpoint_type="LIVE_ODDS",
                url_pattern=session.live_list_url,
                url_raw=session.live_list_url_raw or "", # PASSED TO DB FOR CELERY
                request_method=(session.live_list_method or "GET").upper(),
                headers_json=session.live_list_headers or {},
                sample_response=(session.live_list_sample or "")[:2000],
                is_active=True,
            )
            db.session.add(live_list_ep)
            db.session.flush()
            endpoint_ids.append(live_list_ep.id)

            if session.live_markets_ok and session.live_markets_url_template:
                live_mkt_ep = BookmakerEndpoint(
                    bookmaker_id=session.bookmaker_id,
                    endpoint_type="DEEP_MARKETS",
                    url_pattern=session.live_markets_url_template,
                    url_raw=session.live_markets_url_raw or "", # PASSED TO DB FOR CELERY
                    request_method=(session.live_markets_method or "GET").upper(),
                    headers_json=session.live_markets_headers or {},
                    sample_response=(session.live_markets_sample or "")[:2000],
                    is_active=True,
                )
                db.session.add(live_mkt_ep)
                db.session.flush()
                endpoint_ids.append(live_mkt_ep.id)

        # ── Sport entity values ───────────────────────────────────────────────
        for mapping in (session.sport_mappings or []):
            sport_id    = mapping.get("sport_id")
            bk_sport_id = mapping.get("bk_sport_id")
            if not sport_id or not bk_sport_id:
                continue
            existing = BookmakerEntityValue.query.filter_by(
                bookmaker_id=session.bookmaker_id,
                entity_type="sport",
                internal_id=int(sport_id),
            ).first()
            if existing:
                existing.external_id = str(bk_sport_id)
                existing.label       = mapping.get("sport_name")
            else:
                ev = BookmakerEntityValue(
                    bookmaker_id=session.bookmaker_id,
                    entity_type="sport",
                    internal_id=int(sport_id),
                    external_id=str(bk_sport_id),
                    label=mapping.get("sport_name"),
                )
                db.session.add(ev)

        bk = Bookmaker.query.get(session.bookmaker_id)
        if bk:
            bk.is_active = True

        session.is_complete   = True
        session.workflow_ids  = workflow_ids
        session.endpoint_ids  = endpoint_ids
        session.current_phase = "COMPLETE"
        session.updated_at    = now

        db.session.commit()

    except Exception as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({
        "ok":           True,
        "workflow_ids": workflow_ids,
        "endpoint_ids": endpoint_ids,
        "session":      session.to_dict(),
    })