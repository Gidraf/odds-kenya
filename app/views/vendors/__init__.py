"""
app/api/vendor_api.py
=====================
VendorTemplate API — onboard N bookmakers using a single shared parser.

Blueprint: bp_vendor  (register with url_prefix="/vendors")

Routes
──────
GET    /vendors                          List all vendor templates
POST   /vendors                          Create vendor template
GET    /vendors/<vid>                    Get vendor + all bookmaker configs
PATCH  /vendors/<vid>                    Update vendor (URL patterns, parser, etc.)
DELETE /vendors/<vid>                    Delete vendor

POST   /vendors/<vid>/bookmakers         Add bookmaker to vendor
PATCH  /vendors/<vid>/bookmakers/<bk_id> Update bookmaker config (params, overrides)
DELETE /vendors/<vid>/bookmakers/<bk_id> Remove bookmaker from vendor

POST   /vendors/<vid>/probe              Probe one bookmaker+sport combination
POST   /vendors/<vid>/detect-sports      Auto-detect sports from live API response
POST   /vendors/<vid>/test-parser        Test shared parser against live data

POST   /vendors/<vid>/generate-workflows Bulk create HarvestWorkflows for all bookmakers
POST   /vendors/<vid>/sync-workflows     Sync (update) existing workflows from vendor config

GET    /vendors/presets                  Built-in vendor presets (1xBet LiveFeed, etc.)

Registration (app/__init__.py):
    from app.api.vendor_api import bp_vendor
    app.register_blueprint(bp_vendor, url_prefix="/vendors")
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any

from flask import Blueprint, request, jsonify

from app.extensions import db
from app.models.bookmakers_model import Bookmaker
from app.models.harvest_workflow import HarvestWorkflow, HarvestWorkflowStep
from app.models.vendor_template import VendorTemplate, BookmakerVendorConfig
from app.utils.probe_curl import _probe

bp_vendor = Blueprint("vendor", __name__)


# ─────────────────────────────────────────────────────────────────────────────
# Built-in Vendor Presets
# ─────────────────────────────────────────────────────────────────────────────

# The 1xBet/BetWinner/Melbet LiveFeed format.
# All bookmakers using this vendor share the same URL structure and JSON schema.
# Only `base_url`, `partner`, and `country` differ per bookmaker.
# The `gr` parameter selects the sport/region group.
_1XBET_PARSER = '''
# ─── 1xBet / BetWinner / Melbet — LiveFeed Get1x2_VZip Parser ───────────────
# Handles: Football, Basketball, Volleyball, Table Tennis, Ice Hockey, and more.
# All bookmakers using this vendor format share this exact parser.

# Type code → (market_name, selection_name)
_T = {
    # Match Winner (G=1)
    1:   ("1x2",              "Home"),
    2:   ("1x2",              "Draw"),
    3:   ("1x2",              "Away"),
    # Asian / Main Lines (G=8)
    4:   ("Asian Handicap",   "Home"),
    5:   ("Asian Handicap",   "Away"),
    6:   ("Total",            "Over"),
    # Handicap (G=2)
    7:   ("Handicap",         "Home"),
    8:   ("Handicap",         "Away"),
    # Total (G=17)
    9:   ("Total",            "Over"),
    10:  ("Total",            "Under"),
    # 1st Half Total (G=15)
    11:  ("1st Half Total",   "Over"),
    12:  ("1st Half Total",   "Under"),
    # Corners (G=62)
    13:  ("Total Corners",    "Over"),
    14:  ("Total Corners",    "Under"),
    # 2nd Half Total (G=19)
    180: ("2nd Half Total",   "Over"),
    181: ("2nd Half Total",   "Under"),
    # Match Winner outright style (G=101)
    401: ("Match Winner",     "Home"),
    402: ("Match Winner",     "Away"),
}

# Sport indicator (SI field) → canonical sport name
_SPORT = {
    1: "Football", 2: "Ice Hockey", 3: "Basketball",
    4: "Tennis",   6: "Volleyball", 10: "Table Tennis",
    66: "Cricket",
}


def parse_data(raw):
    """
    Parse one response from /service-api/LiveFeed/Get1x2_VZip.
    raw may be the full response dict or the Value array directly.
    """
    items = raw if isinstance(raw, list) else raw.get("Value", [])
    rows = []

    for item in items:
        match_id  = str(item.get("I", ""))
        home      = item.get("O1") or item.get("O1E") or ""
        away      = item.get("O2") or item.get("O2E") or ""
        sport     = _SPORT.get(item.get("SI"), item.get("SN") or item.get("SE") or "Unknown")
        league    = item.get("L") or item.get("LE") or ""
        country   = item.get("CN", "")
        start_ts  = item.get("S", 0)

        sc      = item.get("SC", {})
        fs      = sc.get("FS", {})
        cps     = sc.get("CPS", "")
        status  = cps if cps else ("live" if sc.get("CP") else "upcoming")
        score_h = fs.get("S1")
        score_a = fs.get("S2")

        competition = f"{country} · {league}" if country and league else (league or country)

        for mkt in item.get("E", []):
            t    = mkt.get("T")
            odds = mkt.get("C")
            p    = mkt.get("P")           # handicap / line value

            if not t or not odds:
                continue
            try:
                odds_f = float(odds)
            except (TypeError, ValueError):
                continue
            if odds_f <= 1.0:
                continue
            if mkt.get("B"):              # suspended / blocked market
                continue

            market_name, selection = _T.get(t, (f"Type{t}", "Selection"))
            specifier = str(p) if p is not None else ""

            rows.append({
                "match_id":        match_id,
                "home_team":       home,
                "away_team":       away,
                "sport":           sport,
                "competition":     competition,
                "start_time":      start_ts,
                "match_status":    status,
                "score_home":      score_h,
                "score_away":      score_a,
                "market_name":     market_name,
                "specifier":       specifier,
                "selection_name":  selection,
                "selection_price": odds_f,
            })

    return rows
'''

VENDOR_PRESETS: list[dict] = [
    {
        "name":        "1xBet / BetWinner LiveFeed",
        "slug":        "1xbet-livefeed",
        "description": (
            "Shared vendor format used by 1xBet, BetWinner, Melbet, PariMatch, and others. "
            "Endpoint: /service-api/LiveFeed/Get1x2_VZip. "
            "Differentiators: base_url, partner, country."
        ),
        "list_url_template": (
            "{{base_url}}/service-api/LiveFeed/Get1x2_VZip"
            "?count={{count}}&lng={{lng}}&gr={{gr}}&mode=4"
            "&country={{country}}&partner={{partner}}"
            "&virtualSports=true&noFilterBlockEvent=true"
        ),
        "live_url_template": (
            "{{base_url}}/service-api/LiveFeed/Get1x2_VZip"
            "?count={{count}}&lng={{lng}}&gr={{gr}}&mode=4"
            "&country={{country}}&partner={{partner}}"
            "&virtualSports=true&noFilterBlockEvent=true"
        ),
        "markets_url_template": "",   # markets are embedded in the list response
        "common_params": {
            "count": "50", "lng": "en", "mode": "4",
            "virtualSports": "true", "noFilterBlockEvent": "true",
        },
        "bookmaker_param_keys": ["partner", "country"],
        # gr values that return each sport — discovered per bookmaker
        # These are sensible defaults; each bookmaker may use different gr values.
        "sport_params": {
            "Football":    "656",
            "Basketball":  "657",
            "Tennis":      "659",
            "Ice Hockey":  "660",
            "Volleyball":  "661",
            "Table Tennis":"662",
        },
        "sport_id_field": "SI",
        "parser_code": _1XBET_PARSER,
    },
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _j(text: str | None, fallback: Any = None) -> Any:
    if not text:
        return fallback
    try:
        return json.loads(text)
    except Exception:
        return fallback


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _get_nested(obj: Any, path: str) -> Any:
    if not path or obj is None:
        return obj
    for key in path.split("."):
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return None
    return obj


def _extract_items(parsed: Any, array_path: str | None) -> list:
    if array_path:
        root = _get_nested(parsed, array_path)
        if isinstance(root, list):
            return root
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for v in parsed.values():
            if isinstance(v, list) and v:
                return v
    return []


def _safe_run_parser(code: str, sample: Any, max_rows: int = 200) -> dict:
    """Execute parse_data(sample) from user-supplied code in an isolated namespace."""
    import traceback
    t0 = time.monotonic()
    try:
        compiled = compile(code, "<vendor_parser>", "exec")
    except SyntaxError as exc:
        return {"ok": False, "error": f"SyntaxError on line {exc.lineno}: {exc.msg}",
                "rows": [], "row_count": 0, "elapsed_ms": 0}

    ns: dict = {"__builtins__": __builtins__}
    try:
        exec(compiled, ns)
    except Exception:
        return {"ok": False, "error": traceback.format_exc(),
                "rows": [], "row_count": 0,
                "elapsed_ms": round((time.monotonic() - t0) * 1000)}

    fn = ns.get("parse_data")
    if not callable(fn):
        return {"ok": False, "error": "parse_data() function not found.",
                "rows": [], "row_count": 0, "elapsed_ms": 0}

    try:
        rows = fn(sample)
        elapsed = round((time.monotonic() - t0) * 1000)
    except Exception:
        return {"ok": False, "error": traceback.format_exc(),
                "rows": [], "row_count": 0,
                "elapsed_ms": round((time.monotonic() - t0) * 1000)}

    if not isinstance(rows, list):
        return {"ok": False, "error": f"parse_data() returned {type(rows).__name__}, expected list.",
                "rows": [], "row_count": 0, "elapsed_ms": elapsed}

    safe_rows = []
    for row in rows[:max_rows]:
        safe_row = {}
        for k, v in row.items():
            try:
                json.dumps(v)
                safe_row[k] = v
            except (TypeError, ValueError):
                safe_row[k] = str(v)
        safe_rows.append(safe_row)

    sports_seen = set(r.get("sport", "") for r in rows if r.get("sport"))
    markets_seen = set(r.get("market_name", "") for r in rows if r.get("market_name"))

    return {
        "ok":          len(rows) > 0,
        "rows":        safe_rows,
        "row_count":   len(rows),
        "sports_seen": sorted(sports_seen),
        "markets_seen": sorted(markets_seen),
        "error":       None,
        "elapsed_ms":  elapsed,
    }


def _build_workflow_for_config(
    vendor: VendorTemplate,
    cfg: BookmakerVendorConfig,
    workflow_type: str,        # "upcoming" | "live"
    sport: str | None = None,  # None = all sports in one call
) -> HarvestWorkflow:
    """
    Build a HarvestWorkflow + steps from vendor template + bookmaker config.
    """
    url_tmpl = (
        vendor.live_url_template if workflow_type == "live"
        else vendor.list_url_template
    ) or ""

    base_url, params = vendor.resolve_url(url_tmpl, sport, cfg)

    # Build a raw URL for param-order preservation (same pattern as onboarding)
    raw_url = url_tmpl.replace("{{base_url}}", cfg.base_url or "")

    bk_name = cfg.bookmaker.name if cfg.bookmaker else f"BK#{cfg.bookmaker_id}"
    name_parts = [bk_name, vendor.name, workflow_type.title()]
    if sport:
        name_parts.append(sport)

    wf = HarvestWorkflow(
        bookmaker_id=cfg.bookmaker_id,
        name=" — ".join(name_parts),
        description=f"Auto-generated from VendorTemplate #{vendor.id} ({vendor.slug})",
        is_active=True,
        created_at=_now(),
        updated_at=_now(),
    )
    db.session.add(wf)
    db.session.flush()

    list_step = HarvestWorkflowStep(
        workflow_id=wf.id,
        position=1,
        name=f"Fetch {workflow_type.title()} Matches",
        step_type="FETCH_LIST",
        url_template=base_url,
        url_raw=raw_url,
        method="GET",
        headers_json=json.dumps({}),
        params_json=json.dumps(params),
        result_array_path="Value",
        fields_json=json.dumps([
            {"path": "I",   "role": "match_id",  "label": "Match ID",   "store": True},
            {"path": "O1",  "role": "home_team",  "label": "Home Team",  "store": True},
            {"path": "O2",  "role": "away_team",  "label": "Away Team",  "store": True},
            {"path": "S",   "role": "start_time", "label": "Start Time", "store": True},
            {"path": "SN",  "role": "sport",      "label": "Sport",      "store": True},
            {"path": "L",   "role": "competition","label": "Competition", "store": True},
        ]),
        parser_code=vendor.parser_code or None,
        parser_test_passed=bool(vendor.parser_test_passed),
        enabled=True,
    )
    db.session.add(list_step)
    return wf


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES — Vendor Templates
# ═════════════════════════════════════════════════════════════════════════════

@bp_vendor.route("", methods=["GET"])
def list_vendors():
    """List all vendor templates."""
    vendors = VendorTemplate.query.order_by(VendorTemplate.name).all()
    return jsonify({"ok": True, "vendors": [v.to_dict(include_bookmakers=False) for v in vendors]})


@bp_vendor.route("", methods=["POST"])
def create_vendor():
    """
    Create a vendor template.
    Body: {name, slug, description?, list_url_template, live_url_template?,
           common_params?, bookmaker_param_keys?, sport_params?, parser_code?}
    """
    d = request.json or {}
    name = (d.get("name") or "").strip()
    slug = (d.get("slug") or "").strip().lower().replace(" ", "-")
    if not name or not slug:
        return jsonify({"ok": False, "error": "name and slug are required"}), 400
    if VendorTemplate.query.filter_by(slug=slug).first():
        return jsonify({"ok": False, "error": f"Slug '{slug}' already exists"}), 409

    v = VendorTemplate(
        name=name, slug=slug,
        description=d.get("description"),
        list_url_template=d.get("list_url_template"),
        live_url_template=d.get("live_url_template"),
        markets_url_template=d.get("markets_url_template"),
        common_params_json=json.dumps(d.get("common_params") or {}),
        bookmaker_param_keys_json=json.dumps(d.get("bookmaker_param_keys") or ["partner", "country"]),
        sport_params_json=json.dumps(d.get("sport_params") or {}),
        sport_id_field=d.get("sport_id_field", "SI"),
        parser_code=d.get("parser_code"),
        parser_test_passed=False,
    )
    db.session.add(v)
    db.session.commit()
    return jsonify({"ok": True, "vendor": v.to_dict()}), 201


@bp_vendor.route("/<int:vid>", methods=["GET"])
def get_vendor(vid: int):
    v = VendorTemplate.query.get_or_404(vid)
    return jsonify({"ok": True, "vendor": v.to_dict()})


@bp_vendor.route("/<int:vid>", methods=["PATCH"])
def patch_vendor(vid: int):
    """Update vendor fields."""
    v = VendorTemplate.query.get_or_404(vid)
    d = request.json or {}

    DIRECT = ["name", "slug", "description", "list_url_template",
              "live_url_template", "markets_url_template",
              "sport_id_field", "parser_code", "parser_test_passed"]
    JSON_FIELDS = {
        "common_params":        "common_params_json",
        "bookmaker_param_keys": "bookmaker_param_keys_json",
        "sport_params":         "sport_params_json",
    }

    for field in DIRECT:
        if field in d:
            setattr(v, field, d[field])
    for key, attr in JSON_FIELDS.items():
        if key in d:
            setattr(v, attr, json.dumps(d[key]))

    v.updated_at = _now()
    db.session.commit()
    return jsonify({"ok": True, "vendor": v.to_dict()})


@bp_vendor.route("/<int:vid>", methods=["DELETE"])
def delete_vendor(vid: int):
    v = VendorTemplate.query.get_or_404(vid)
    db.session.delete(v)
    db.session.commit()
    return jsonify({"ok": True, "msg": f"Vendor '{v.slug}' deleted."})


# ─── Presets ──────────────────────────────────────────────────────────────────

@bp_vendor.route("/presets", methods=["GET"])
def get_presets():
    """Return built-in vendor presets the user can import."""
    return jsonify({"ok": True, "presets": VENDOR_PRESETS})


@bp_vendor.route("/presets/import", methods=["POST"])
def import_preset():
    """
    Import a built-in preset as a VendorTemplate.
    Body: {slug}  — the preset slug to import
    """
    slug = (request.json or {}).get("slug", "")
    preset = next((p for p in VENDOR_PRESETS if p["slug"] == slug), None)
    if not preset:
        return jsonify({"ok": False, "error": f"Preset '{slug}' not found"}), 404
    if VendorTemplate.query.filter_by(slug=slug).first():
        return jsonify({"ok": False, "error": f"Vendor '{slug}' already imported"}), 409

    v = VendorTemplate(
        name=preset["name"],
        slug=preset["slug"],
        description=preset.get("description"),
        list_url_template=preset.get("list_url_template"),
        live_url_template=preset.get("live_url_template"),
        markets_url_template=preset.get("markets_url_template"),
        common_params_json=json.dumps(preset.get("common_params", {})),
        bookmaker_param_keys_json=json.dumps(preset.get("bookmaker_param_keys", [])),
        sport_params_json=json.dumps(preset.get("sport_params", {})),
        sport_id_field=preset.get("sport_id_field", "SI"),
        parser_code=preset.get("parser_code"),
        parser_test_passed=False,
    )
    db.session.add(v)
    db.session.commit()
    return jsonify({"ok": True, "vendor": v.to_dict()}), 201


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES — Bookmaker Configs
# ═════════════════════════════════════════════════════════════════════════════

@bp_vendor.route("/<int:vid>/bookmakers", methods=["POST"])
def add_bookmaker(vid: int):
    """
    Add a bookmaker to this vendor.
    Body: {bookmaker_id, base_url, params, sport_params_override?}
    """
    v = VendorTemplate.query.get_or_404(vid)
    d = request.json or {}
    bk_id = d.get("bookmaker_id")
    if not bk_id:
        return jsonify({"ok": False, "error": "bookmaker_id required"}), 400

    Bookmaker.query.get_or_404(bk_id)

    existing = BookmakerVendorConfig.query.filter_by(
        vendor_id=vid, bookmaker_id=bk_id
    ).first()
    if existing:
        return jsonify({"ok": False, "error": "Bookmaker already assigned to this vendor"}), 409

    cfg = BookmakerVendorConfig(
        vendor_id=vid,
        bookmaker_id=bk_id,
        base_url=(d.get("base_url") or "").rstrip("/"),
        params_json=json.dumps(d.get("params") or {}),
        sport_params_override_json=json.dumps(d.get("sport_params_override") or {}),
        is_active=True,
    )
    db.session.add(cfg)
    db.session.commit()
    return jsonify({"ok": True, "config": cfg.to_dict(), "vendor": v.to_dict()}), 201


@bp_vendor.route("/<int:vid>/bookmakers/<int:bk_id>", methods=["PATCH"])
def patch_bookmaker(vid: int, bk_id: int):
    """Update a bookmaker's vendor config."""
    cfg = BookmakerVendorConfig.query.filter_by(
        vendor_id=vid, bookmaker_id=bk_id
    ).first_or_404()
    d = request.json or {}

    if "base_url" in d:
        cfg.base_url = (d["base_url"] or "").rstrip("/")
    if "params" in d:
        cfg.params_json = json.dumps(d["params"] or {})
    if "sport_params_override" in d:
        cfg.sport_params_override_json = json.dumps(d["sport_params_override"] or {})
    if "is_active" in d:
        cfg.is_active = bool(d["is_active"])

    cfg.updated_at = _now()
    db.session.commit()
    return jsonify({"ok": True, "config": cfg.to_dict()})


@bp_vendor.route("/<int:vid>/bookmakers/<int:bk_id>", methods=["DELETE"])
def remove_bookmaker(vid: int, bk_id: int):
    """Remove a bookmaker from this vendor."""
    cfg = BookmakerVendorConfig.query.filter_by(
        vendor_id=vid, bookmaker_id=bk_id
    ).first_or_404()
    db.session.delete(cfg)
    db.session.commit()
    return jsonify({"ok": True})


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES — Probe & Sport Detection
# ═════════════════════════════════════════════════════════════════════════════

@bp_vendor.route("/<int:vid>/probe", methods=["POST"])
def probe_vendor(vid: int):
    """
    Fire a real HTTP request for one bookmaker + sport combination.

    Body: {bookmaker_id, sport?, gr_override?}
    Uses vendor URL template + bookmaker params to build the request.

    Returns: standard probe response + sports_detected (from SI field analysis).
    """
    v  = VendorTemplate.query.get_or_404(vid)
    d  = request.json or {}
    bk_id = d.get("bookmaker_id")
    sport = d.get("sport")   # canonical sport name

    if not bk_id:
        return jsonify({"ok": False, "error": "bookmaker_id required"}), 400

    cfg = BookmakerVendorConfig.query.filter_by(
        vendor_id=vid, bookmaker_id=bk_id
    ).first()

    # Build params — inline if config doesn't exist yet (for preview probing)
    inline_params = d.get("params") or {}
    inline_base   = (d.get("base_url") or "").rstrip("/")

    if cfg:
        base_url, params = v.resolve_url(
            v.list_url_template or "", sport, cfg
        )
    else:
        base_url = (v.list_url_template or "").replace(
            "{{base_url}}", inline_base
        )
        params = {**v.common_params, **inline_params}
        if sport and sport in v.sport_params:
            params["gr"] = v.sport_params[sport]

    # Allow gr_override for discovery probing
    if d.get("gr_override"):
        params["gr"] = str(d["gr_override"])

    raw_url = v.list_url_template or ""
    result  = _probe(
        url=base_url, method="GET",
        headers={}, params=params,
        url_raw=raw_url,
    )

    # Analyse response for sport detection
    sports_detected: dict = {}
    if result.get("ok") and result.get("parsed"):
        items = _extract_items(result["parsed"], "Value")
        si_field = v.sport_id_field or "SI"
        for item in items:
            si_val = item.get(si_field)
            sport_name = item.get("SN") or item.get("SE") or f"SI={si_val}"
            if si_val is not None:
                key = str(si_val)
                if key not in sports_detected:
                    sports_detected[key] = {"si": si_val, "name": sport_name, "count": 0}
                sports_detected[key]["count"] += 1

        # Update probe cache on cfg
        if cfg:
            probe_cache = cfg.last_probe
            sport_key   = sport or "all"
            probe_cache[sport_key] = {
                "ok":    True,
                "count": result.get("array_length", 0),
                "ts":    int(time.time()),
            }
            cfg.last_probe_json = json.dumps(probe_cache)
            cfg.updated_at = _now()
            db.session.commit()

    result["sports_detected"] = list(sports_detected.values())
    return jsonify(result)


@bp_vendor.route("/<int:vid>/detect-sports", methods=["POST"])
def detect_sports(vid: int):
    """
    Probe one bookmaker across all configured sport groups and detect
    which SI values appear in each response.

    Body: {bookmaker_id, gr_values?: [656, 657, 658, ...]}
    Returns: {ok, sport_map: {"656": {"si_values": [...], "sample_sports": [...]}}}
    """
    v  = VendorTemplate.query.get_or_404(vid)
    d  = request.json or {}
    bk_id = d.get("bookmaker_id")
    if not bk_id:
        return jsonify({"ok": False, "error": "bookmaker_id required"}), 400

    cfg = BookmakerVendorConfig.query.filter_by(
        vendor_id=vid, bookmaker_id=bk_id
    ).first_or_404()

    # GR values to probe — from request, vendor sport_params, or sensible range
    gr_values = d.get("gr_values") or list(v.sport_params.values())
    if not gr_values:
        return jsonify({"ok": False, "error": "No gr_values to probe. Provide gr_values or configure sport_params."}), 400

    si_field = v.sport_id_field or "SI"
    sport_map: dict = {}

    for gr in gr_values:
        base_url, params = v.resolve_url(v.list_url_template or "", None, cfg)
        params["gr"] = str(gr)
        result = _probe(url=base_url, method="GET", headers={}, params=params, url_raw="")

        si_counts: dict = {}
        if result.get("ok"):
            items = _extract_items(result.get("parsed"), "Value")
            for item in items:
                si = item.get(si_field)
                name = item.get("SN") or item.get("SE") or ""
                if si is not None:
                    key = str(si)
                    if key not in si_counts:
                        si_counts[key] = {"si": si, "name": name, "count": 0}
                    si_counts[key]["count"] += 1

        sport_map[str(gr)] = {
            "ok":           result.get("ok", False),
            "http_status":  result.get("status"),
            "item_count":   result.get("array_length", 0),
            "si_breakdown": list(si_counts.values()),
            "error":        result.get("error"),
        }

    return jsonify({"ok": True, "sport_map": sport_map})


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES — Parser
# ═════════════════════════════════════════════════════════════════════════════

@bp_vendor.route("/<int:vid>/test-parser", methods=["POST"])
def test_vendor_parser(vid: int):
    """
    Test the vendor's shared parser against live data or a provided sample.

    Body: {
      code?,            # use vendor's stored parser if omitted
      bookmaker_id?,    # probe live data if provided
      sport?,           # filter to specific sport
      sample_json?,     # use pre-fetched sample instead of probing
    }
    """
    v  = VendorTemplate.query.get_or_404(vid)
    d  = request.json or {}

    code   = (d.get("code") or v.parser_code or "").strip()
    sample = d.get("sample_json")

    if sample is None:
        # Try to probe live
        bk_id = d.get("bookmaker_id")
        if bk_id:
            cfg = BookmakerVendorConfig.query.filter_by(
                vendor_id=vid, bookmaker_id=bk_id
            ).first()
            if cfg:
                base_url, params = v.resolve_url(
                    v.list_url_template or "", d.get("sport"), cfg
                )
                result = _probe(url=base_url, method="GET", headers={}, params=params, url_raw="")
                if result.get("ok"):
                    sample = result["parsed"]
                    # Cache the sample
                    v.parser_sample_json = json.dumps(sample)
                    db.session.commit()

        if sample is None:
            raw = (d.get("sample_raw") or v.parser_sample_json or "").strip()
            if raw:
                try:
                    sample = json.loads(raw)
                except json.JSONDecodeError:
                    sample = {}
            else:
                sample = {}

    if not code:
        return jsonify({"ok": False, "error": "No parser code. Set vendor parser_code first."}), 400

    result = _safe_run_parser(code, sample)

    # On success, persist the test status
    if result.get("ok"):
        v.parser_code        = code
        v.parser_test_passed = True
        v.updated_at         = _now()
        db.session.commit()

    return jsonify(result)


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES — Bulk Workflow Generation
# ═════════════════════════════════════════════════════════════════════════════

@bp_vendor.route("/<int:vid>/generate-workflows", methods=["POST"])
def generate_workflows(vid: int):
    """
    Bulk-create HarvestWorkflows for all (or selected) bookmakers in this vendor.

    Body: {
      bookmaker_ids?: [1, 2, 3],  # omit for all active bookmakers
      include_live?: true,
      per_sport?: false,          # if true, create one workflow per sport
      sports?: ["Football"],      # only when per_sport=true
      overwrite?: false           # replace existing workflows
    }

    Returns: {ok, created, skipped, errors, summary}
    """
    v = VendorTemplate.query.get_or_404(vid)
    d = request.json or {}

    bk_ids       = d.get("bookmaker_ids")
    include_live = bool(d.get("include_live", True))
    per_sport    = bool(d.get("per_sport", False))
    overwrite    = bool(d.get("overwrite", False))
    sports_filter = d.get("sports") or list(v.sport_params.keys())

    configs = list(v.bookmaker_configs.filter_by(is_active=True))
    if bk_ids:
        configs = [c for c in configs if c.bookmaker_id in bk_ids]

    if not configs:
        return jsonify({"ok": False, "error": "No active bookmaker configs found."}), 400

    created: list = []
    skipped: list = []
    errors:  list = []

    for cfg in configs:
        bk_name = cfg.bookmaker.name if cfg.bookmaker else f"BK#{cfg.bookmaker_id}"
        try:
            # ── Upcoming workflow ─────────────────────────────────────────
            if cfg.upcoming_workflow_id and not overwrite:
                skipped.append({"bookmaker": bk_name, "type": "upcoming", "reason": "already exists"})
            else:
                if cfg.upcoming_workflow_id and overwrite:
                    old = HarvestWorkflow.query.get(cfg.upcoming_workflow_id)
                    if old:
                        db.session.delete(old)
                        db.session.flush()

                wf = _build_workflow_for_config(v, cfg, "upcoming")
                db.session.flush()
                cfg.upcoming_workflow_id = wf.id
                created.append({"bookmaker": bk_name, "type": "upcoming", "workflow_id": wf.id})

            # ── Live workflow ─────────────────────────────────────────────
            if include_live and v.live_url_template:
                if cfg.live_workflow_id and not overwrite:
                    skipped.append({"bookmaker": bk_name, "type": "live", "reason": "already exists"})
                else:
                    if cfg.live_workflow_id and overwrite:
                        old = HarvestWorkflow.query.get(cfg.live_workflow_id)
                        if old:
                            db.session.delete(old)
                            db.session.flush()

                    wf_live = _build_workflow_for_config(v, cfg, "live")
                    db.session.flush()
                    cfg.live_workflow_id = wf_live.id
                    created.append({"bookmaker": bk_name, "type": "live", "workflow_id": wf_live.id})

            cfg.updated_at = _now()

        except Exception as exc:
            db.session.rollback()
            errors.append({"bookmaker": bk_name, "error": str(exc)})
            continue

    try:
        v.updated_at = _now()
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({
        "ok":      len(errors) == 0,
        "created": created,
        "skipped": skipped,
        "errors":  errors,
        "summary": {
            "bookmakers_processed": len(configs),
            "workflows_created":    len(created),
            "workflows_skipped":    len(skipped),
            "errors":               len(errors),
        },
        "vendor": v.to_dict(),
    })


@bp_vendor.route("/<int:vid>/sync-workflows", methods=["POST"])
def sync_workflows(vid: int):
    """
    Sync (update parser + params) on all existing workflows generated from this vendor.
    Useful when you improve the vendor parser and want to propagate changes.

    Body: {bookmaker_ids?: [...], update_parser?: true, update_params?: true}
    """
    v = VendorTemplate.query.get_or_404(vid)
    d = request.json or {}
    bk_ids         = d.get("bookmaker_ids")
    update_parser  = bool(d.get("update_parser", True))
    update_params  = bool(d.get("update_params", False))

    configs = list(v.bookmaker_configs.filter_by(is_active=True))
    if bk_ids:
        configs = [c for c in configs if c.bookmaker_id in bk_ids]

    updated: list = []
    errors:  list = []

    for cfg in configs:
        bk_name = cfg.bookmaker.name if cfg.bookmaker else f"BK#{cfg.bookmaker_id}"
        for wf_id, label in [
            (cfg.upcoming_workflow_id, "upcoming"),
            (cfg.live_workflow_id,     "live"),
        ]:
            if not wf_id:
                continue
            wf = HarvestWorkflow.query.get(wf_id)
            if not wf:
                continue
            try:
                for step in wf.steps:
                    if update_parser and v.parser_code:
                        step.parser_code        = v.parser_code
                        step.parser_test_passed = v.parser_test_passed
                    if update_params:
                        _, params = v.resolve_url(
                            v.list_url_template or "", None, cfg
                        )
                        step.params_json = json.dumps(params)
                wf.updated_at = _now()
                updated.append({"bookmaker": bk_name, "type": label, "workflow_id": wf_id})
            except Exception as exc:
                errors.append({"bookmaker": bk_name, "type": label, "error": str(exc)})

    try:
        v.updated_at = _now()
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        return jsonify({"ok": False, "error": str(exc)}), 500

    return jsonify({
        "ok":     len(errors) == 0,
        "updated": updated,
        "errors":  errors,
    })