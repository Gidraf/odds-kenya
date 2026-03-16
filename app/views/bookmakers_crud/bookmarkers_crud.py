"""
app/api/bookmaker_config_api.py
================================
Full CRUD for bookmaker harvest configs.

Routes (all under /bookmakers):
    GET    /bookmakers/                     list all bookmakers
    POST   /bookmakers/                     create bookmaker
    GET    /bookmakers/<id>                 get single bookmaker
    PUT    /bookmakers/<id>                 update name/domain/vendor/active
    DELETE /bookmakers/<id>                 delete bookmaker

    GET    /bookmakers/<id>/config          get harvest config
    PUT    /bookmakers/<id>/config          save / replace harvest config
    DELETE /bookmakers/<id>/config          clear harvest config (set to {})
    POST   /bookmakers/<id>/config/validate validate JSON + test fetch (probe)
    GET    /bookmakers/<id>/config/history  last N saved configs (audit log in Redis)

    POST   /bookmakers/bulk-config          save configs for multiple bookmakers at once

Register in app factory:
    from app.api.bookmaker_config_api import bp as bookmaker_bp
    app.register_blueprint(bookmaker_bp)
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from . import bp_search as bp

from flask import Blueprint, jsonify, request, current_app

# bp = Blueprint("bookmakers", __name__, url_prefix="/api/admin/")


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Model helper ─────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _get_model():
    from app.models.bookmakers_model import Bookmaker
    return Bookmaker


def _serialize(bm) -> dict:
    """Full bookmaker dict including harvest_config."""
    config = getattr(bm, "harvest_config", None) or {}
    if isinstance(config, str):
        try: config = json.loads(config)
        except Exception: config = {}
    return {
        "id":             bm.id,
        "name":           bm.name,
        "domain":         bm.domain,
        "vendor_slug":    getattr(bm, "vendor_slug", "betb2b") or "betb2b",
        "is_active":      bm.is_active,
        "harvest_config": config,
        "brand_color":    getattr(bm, "brand_color", None),
        "created_at":     str(getattr(bm, "created_at", "") or ""),
        "updated_at":     str(getattr(bm, "updated_at", "") or ""),
    }


def _serialize_list(bm) -> dict:
    """Compact version for list endpoint — no full config."""
    config = getattr(bm, "harvest_config", None) or {}
    if isinstance(config, str):
        try: config = json.loads(config)
        except Exception: config = {}
    has_config = bool(config.get("params") or config.get("list_url"))
    partner    = (config.get("params") or {}).get("partner", "")
    return {
        "id":          bm.id,
        "name":        bm.name,
        "domain":      bm.domain,
        "vendor_slug": getattr(bm, "vendor_slug", "betb2b") or "betb2b",
        "is_active":   bm.is_active,
        "has_config":  has_config,
        "partner_id":  partner,
        "brand_color": getattr(bm, "brand_color", None),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Redis audit log helpers ──────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _redis():
    import redis as _r
    url  = current_app.config.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    return _r.Redis.from_url(f"{base}/2", decode_responses=True,
                             socket_timeout=3, socket_connect_timeout=3)


def _push_config_history(bm_id: int, config: dict, saved_by: str = "admin"):
    """Push a config snapshot to a Redis list (keeps last 10)."""
    try:
        r   = _redis()
        key = f"config_history:{bm_id}"
        entry = json.dumps({
            "config":   config,
            "saved_by": saved_by,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        })
        r.lpush(key, entry)
        r.ltrim(key, 0, 9)   # keep last 10
        r.expire(key, 86400 * 30)   # 30 days
    except Exception:
        pass   # don't fail the save if Redis is down


def _get_config_history(bm_id: int) -> list[dict]:
    try:
        r   = _redis()
        key = f"config_history:{bm_id}"
        raw = r.lrange(key, 0, 9)
        return [json.loads(e) for e in raw]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Bookmaker CRUD ───────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/admin/bookmakers", methods=["GET"])
def list_bookmakers():
    Bookmaker = _get_model()
    q = Bookmaker.query

    if s := request.args.get("q"):
        q = q.filter(Bookmaker.name.ilike(f"%{s}%"))
    if vendor := request.args.get("vendor"):
        q = q.filter(Bookmaker.vendor_slug == vendor)
    if (active := request.args.get("active")) is not None:
        q = q.filter(Bookmaker.is_active == (active.lower() == "true"))

    page     = request.args.get("page", 1, type=int)
    per_page = min(request.args.get("per_page", 50, type=int), 200)
    pag      = q.order_by(Bookmaker.name).paginate(page=page, per_page=per_page, error_out=False)

    return jsonify({
        "items":    [_serialize_list(b) for b in pag.items],
        "total":    pag.total,
        "page":     pag.page,
        "pages":    pag.pages,
        "per_page": per_page,
    })


@bp.route("/bookmakers/onboard", methods=["POST"])
def create_bookmaker():
    from app.extensions import db
    Bookmaker = _get_model()

    d = request.json or {}
    if not d.get("name") or not d.get("domain"):
        return jsonify({"error": "name and domain are required"}), 400

    if Bookmaker.query.filter(Bookmaker.domain.ilike(d["domain"].strip())).first():
        return jsonify({"error": f"Bookmaker with domain '{d['domain']}' already exists"}), 409

    bm = Bookmaker(
        name        = d["name"].strip(),
        domain      = d["domain"].strip().lower(),
        vendor_slug = d.get("vendor_slug", "betb2b"),
        is_active   = d.get("is_active", True),
    )
    if hasattr(bm, "brand_color") and "brand_color" in d:
        bm.brand_color = d["brand_color"]
    if hasattr(bm, "harvest_config"):
        bm.harvest_config = d.get("harvest_config") or {}

    db.session.add(bm)
    db.session.commit()
    return jsonify(_serialize(bm)), 201


@bp.route("/bookmakers/<int:bm_id>", methods=["GET"])
def get_bookmaker(bm_id: int):
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)
    return jsonify(_serialize(bm))


@bp.route("/bookmaker/<int:bm_id>", methods=["PUT"])
def update_bookmaker(bm_id: int):
    from app.extensions import db
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)
    d  = request.json or {}

    if "name"        in d: bm.name        = d["name"].strip()
    if "domain"      in d: bm.domain      = d["domain"].strip().lower()
    if "vendor_slug" in d: bm.vendor_slug = d["vendor_slug"]
    if "is_active"   in d: bm.is_active   = bool(d["is_active"])
    if "brand_color" in d and hasattr(bm, "brand_color"):
        bm.brand_color = d["brand_color"]

    # Allow updating config in the same call
    if "harvest_config" in d and hasattr(bm, "harvest_config"):
        old_config = getattr(bm, "harvest_config") or {}
        bm.harvest_config = d["harvest_config"]
        _push_config_history(bm_id, old_config)

    if "vendor_slug" in d and hasattr(bm, "vendor_slug"):
        bm.vendor_slug = d["vendor_slug"]

    db.session.commit()
    return jsonify(_serialize(bm))


@bp.route("/bookmakers/<int:bm_id>", methods=["DELETE"])
def delete_bookmaker(bm_id: int):
    from app.extensions import db
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)
    name = bm.name
    db.session.delete(bm)
    db.session.commit()
    return jsonify({"deleted": bm_id, "name": name})


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Config-specific endpoints ────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/bookmakers/<int:bm_id>/config", methods=["GET"])
def get_config(bm_id: int):
    """Return just the harvest_config + vendor_slug for this bookmaker."""
    Bookmaker = _get_model()
    bm     = Bookmaker.query.get_or_404(bm_id)
    config = getattr(bm, "harvest_config", None) or {}
    if isinstance(config, str):
        try: config = json.loads(config)
        except Exception: config = {}
    return jsonify({
        "bookmaker_id":   bm.id,
        "bookmaker_name": bm.name,
        "domain":         bm.domain,
        "vendor_slug":    getattr(bm, "vendor_slug", "betb2b") or "betb2b",
        "harvest_config": config,
        "has_config":     bool(config),
    })


@bp.route("/bookmakers/<int:bm_id>/config", methods=["PUT"])
def save_config(bm_id: int):
    """
    Save or replace the harvest_config for a bookmaker.

    Body:
        {
          "vendor_slug":    "betb2b",
          "harvest_config": { ... }
        }

    Pushes previous config to history before overwriting.
    """
    from app.extensions import db
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)
    d  = request.json or {}

    new_config  = d.get("harvest_config") or d.get("config") or {}
    new_vendor  = d.get("vendor_slug") or getattr(bm, "vendor_slug", "betb2b")

    # Validate it's a dict
    if not isinstance(new_config, dict):
        return jsonify({"error": "harvest_config must be a JSON object"}), 400

    # Light schema check for betb2b
    if new_vendor == "betb2b":
        params = new_config.get("params") or {}
        if not params.get("partner"):
            return jsonify({
                "error":   "BetB2B config requires params.partner",
                "hint":    "Add 'partner': '<id>' to the params block",
                "missing": "partner",
            }), 422

    # Archive old config
    old_config = getattr(bm, "harvest_config", None) or {}
    if isinstance(old_config, str):
        try: old_config = json.loads(old_config)
        except Exception: old_config = {}

    _push_config_history(bm_id, old_config, saved_by="api")

    # Save
    if hasattr(bm, "harvest_config"):
        bm.harvest_config = new_config
    if hasattr(bm, "vendor_slug"):
        bm.vendor_slug = new_vendor

    db.session.commit()

    return jsonify({
        "ok":             True,
        "bookmaker_id":   bm_id,
        "bookmaker_name": bm.name,
        "vendor_slug":    new_vendor,
        "harvest_config": new_config,
        "saved_at":       datetime.now(timezone.utc).isoformat(),
    })


@bp.route("/bookmakers/<int:bm_id>/config", methods=["DELETE"])
def clear_config(bm_id: int):
    """Clear the harvest_config (set to empty dict). Saves to history first."""
    from app.extensions import db
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)

    old_config = getattr(bm, "harvest_config", None) or {}
    _push_config_history(bm_id, old_config, saved_by="api:clear")

    if hasattr(bm, "harvest_config"):
        bm.harvest_config = {}
    db.session.commit()

    return jsonify({
        "ok":           True,
        "bookmaker_id": bm_id,
        "cleared_at":   datetime.now(timezone.utc).isoformat(),
    })


@bp.route("/bookmakers/<int:bm_id>/config/validate", methods=["POST"])
def validate_config(bm_id: int):
    """
    Validate a config JSON and optionally do a live probe against the API.

    Body:
        {
          "vendor_slug":    "betb2b",
          "harvest_config": { ... },
          "probe":          true        ← set false to skip live test
        }
    """
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)
    d  = request.json or {}

    config     = d.get("harvest_config") or d.get("config") or {}
    vendor     = d.get("vendor_slug", getattr(bm, "vendor_slug", "betb2b"))
    do_probe   = d.get("probe", True)

    errors   = []
    warnings = []

    # ── Schema checks ────────────────────────────────────────────────────────
    if not isinstance(config, dict):
        return jsonify({"valid": False, "errors": ["harvest_config must be an object"]}), 400

    if vendor == "betb2b":
        params  = config.get("params") or {}
        headers = config.get("headers") or {}

        if not params.get("partner"):
            errors.append("params.partner is required")
        if not params.get("lng"):
            warnings.append("params.lng missing — will default to 'en'")
        if not params.get("country"):
            warnings.append("params.country missing — will default to '87'")
        if "x-hd" in {k.lower() for k in headers}:
            warnings.append("headers.x-hd is set — this token expires per-request and will cause 406 errors")
        if not config.get("sport_mappings"):
            warnings.append("sport_mappings missing — fetcher will use sport ID 1 (football) as fallback")

    elif vendor == "sportpesa":
        if not config.get("list_url"):
            errors.append("list_url is required for sportpesa")
        if not config.get("markets_url"):
            warnings.append("markets_url missing — will default to /api/games/markets")

    elif vendor == "generic":
        if not config.get("list_url"):
            errors.append("list_url is required for generic vendor")

    if errors:
        return jsonify({"valid": False, "errors": errors, "warnings": warnings}), 422

    result = {
        "valid":    True,
        "errors":   errors,
        "warnings": warnings,
        "vendor":   vendor,
    }

    # ── Live probe (optional) ─────────────────────────────────────────────────
    if do_probe:
        t0 = time.perf_counter()
        try:
            from .bookmaker_fetcher import fetch_bookmaker
            bm_dict = {
                "id":          bm.id,
                "name":        bm.name,
                "domain":      bm.domain,
                "vendor_slug": vendor,
                "config":      config,
            }
            matches = fetch_bookmaker(bm_dict, sport_name="Football", mode="live", timeout=15)
            latency = int((time.perf_counter() - t0) * 1000)
            result.update({
                "probe_ok":      True,
                "probe_count":   len(matches),
                "probe_latency": latency,
                "probe_sample":  matches[:3],   # first 3 matches as preview
            })
        except Exception as exc:
            latency = int((time.perf_counter() - t0) * 1000)
            result.update({
                "probe_ok":      False,
                "probe_error":   str(exc),
                "probe_latency": latency,
            })

    return jsonify(result)


@bp.route("/bookmakers/<int:bm_id>/config/history", methods=["GET"])
def config_history(bm_id: int):
    """Return last 10 saved configs for this bookmaker (from Redis)."""
    Bookmaker = _get_model()
    Bookmaker.query.get_or_404(bm_id)   # 404 if bookmaker doesn't exist

    history = _get_config_history(bm_id)
    return jsonify({
        "bookmaker_id": bm_id,
        "history":      history,
        "count":        len(history),
    })


@bp.route("/bookmakers/<int:bm_id>/config/restore/<int:index>", methods=["POST"])
def restore_config(bm_id: int, index: int):
    """Restore config from history slot `index` (0 = most recent)."""
    from app.extensions import db
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)

    history = _get_config_history(bm_id)
    if not history or index >= len(history):
        return jsonify({"error": f"No history at index {index}"}), 404

    entry      = history[index]
    old_config = getattr(bm, "harvest_config", None) or {}
    _push_config_history(bm_id, old_config, saved_by=f"restore:idx{index}")

    if hasattr(bm, "harvest_config"):
        bm.harvest_config = entry["config"]
    db.session.commit()

    return jsonify({
        "ok":             True,
        "bookmaker_id":   bm_id,
        "restored_from":  entry.get("saved_at"),
        "harvest_config": entry["config"],
    })


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Bulk config save ─────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/bookmakers/bulk-config", methods=["POST"])
def bulk_save_configs():
    """
    Save configs for multiple bookmakers in one request.

    Body:
        {
          "bookmakers": [
            { "id": 1, "vendor_slug": "betb2b", "harvest_config": {...} },
            { "id": 2, "vendor_slug": "betb2b", "harvest_config": {...} },
            ...
          ]
        }
    """
    from app.extensions import db
    Bookmaker = _get_model()

    items = (request.json or {}).get("bookmakers", [])
    if not items:
        return jsonify({"error": "bookmakers array required"}), 400

    saved   = []
    failed  = []

    for item in items:
        bm_id = item.get("id")
        if not bm_id:
            failed.append({"item": item, "error": "id missing"})
            continue
        try:
            bm = Bookmaker.query.get(bm_id)
            if not bm:
                failed.append({"id": bm_id, "error": "not found"})
                continue

            new_config = item.get("harvest_config") or {}
            new_vendor = item.get("vendor_slug") or getattr(bm, "vendor_slug", "betb2b")

            old_config = getattr(bm, "harvest_config", None) or {}
            _push_config_history(bm_id, old_config, saved_by="bulk")

            if hasattr(bm, "harvest_config"):
                bm.harvest_config = new_config
            if hasattr(bm, "vendor_slug"):
                bm.vendor_slug = new_vendor

            saved.append({"id": bm_id, "name": bm.name})
        except Exception as exc:
            failed.append({"id": bm_id, "error": str(exc)})

    if saved:
        db.session.commit()

    return jsonify({
        "saved":  saved,
        "failed": failed,
        "total":  len(items),
    })


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Per-sport endpoint config ────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
#
# New config structure stored in harvest_config["sports"]:
#   {
#     "sports": {
#       "Football":    { "url": "...", "params": [["k","v"],...], "headers": {...} },
#       "Basketball":  { ... },
#     }
#   }
#
# The {{mode}} placeholder in params is replaced at fetch time with "4" (live)
# or "1" (upcoming), so one config covers both modes.
# ─────────────────────────────────────────────────────────────────────────────

_CANONICAL_SPORTS = [
    "Football", "Basketball", "Tennis", "Ice Hockey",
    "Volleyball", "Cricket", "Rugby", "Table Tennis",
    "Snooker", "American Football",
]

_DEFAULT_BK_SPORT_IDS = {
    "Football": "1", "Basketball": "3", "Tennis": "4",
    "Ice Hockey": "2", "Volleyball": "5", "Cricket": "21",
    "Rugby": "8", "Table Tennis": "19", "Snooker": "14",
    "American Football": "66",
}


def _build_sport_params(domain: str, partner: str, gr: str, sport_id: str) -> list[list[str]]:
    """Build the canonical ordered params list for a BetB2B bookmaker × sport."""
    params = [["count", "40"], ["lng", "en"]]
    if gr:
        params.append(["gr", gr])
    params += [
        ["mode",               "{{mode}}"],
        ["country",            "87"],
        ["partner",            partner],
        ["virtualSports",      "true"],
        ["noFilterBlockEvent", "true"],
        ["sports",             sport_id],
    ]
    return params


@bp.route("/bookmakers/<int:bm_id>/config/sports", methods=["GET"])
def get_sport_configs(bm_id: int):
    """
    GET /bookmakers/<id>/config/sports
    Return per-sport endpoint configs, or empty dict if not yet saved.
    """
    Bookmaker = _get_model()
    bm     = Bookmaker.query.get_or_404(bm_id)
    config = getattr(bm, "harvest_config", None) or {}
    if isinstance(config, str):
        try: config = json.loads(config)
        except Exception: config = {}

    sports_cfg = config.get("sports") or {}
    return jsonify({
        "bookmaker_id":   bm_id,
        "bookmaker_name": bm.name,
        "domain":         bm.domain,
        "vendor_slug":    getattr(bm, "vendor_slug", "betb2b") or "betb2b",
        "sports":         sports_cfg,
        "configured_sports": list(sports_cfg.keys()),
        "all_sports":        _CANONICAL_SPORTS,
    })


@bp.route("/bookmakers/<int:bm_id>/config/sports/<sport_name>", methods=["GET"])
def get_sport_config(bm_id: int, sport_name: str):
    """GET /bookmakers/<id>/config/sports/Football"""
    Bookmaker = _get_model()
    bm     = Bookmaker.query.get_or_404(bm_id)
    config = getattr(bm, "harvest_config", None) or {}
    if isinstance(config, str):
        try: config = json.loads(config)
        except Exception: config = {}
    sport_cfg = (config.get("sports") or {}).get(sport_name)
    if not sport_cfg:
        return jsonify({"error": f"No config for sport '{sport_name}'"}), 404
    return jsonify({"bookmaker_id": bm_id, "sport": sport_name, "config": sport_cfg})


@bp.route("/bookmakers/<int:bm_id>/config/sports/<sport_name>", methods=["PUT"])
def save_sport_config(bm_id: int, sport_name: str):
    """
    PUT /bookmakers/<id>/config/sports/Football
    Save or replace the endpoint config for one sport.

    Body:
        {
          "url":     "https://domain/service-api/LiveFeed/Get1x2_VZip",
          "params":  [["count","40"],["lng","en"],["mode","{{mode}}"],...],
          "headers": { "Accept": "..." }
        }
    """
    from app.extensions import db
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)
    d  = request.json or {}

    if not d.get("url") and not d.get("params"):
        return jsonify({"error": "url and params are required"}), 400

    config = getattr(bm, "harvest_config", None) or {}
    if isinstance(config, str):
        try: config = json.loads(config)
        except Exception: config = {}

    # Archive before mutating
    _push_config_history(bm_id, config, saved_by=f"sport:{sport_name}")

    sports_cfg = config.get("sports") or {}
    sports_cfg[sport_name] = {
        "url":     d.get("url", f"https://{bm.domain}/service-api/LiveFeed/Get1x2_VZip"),
        "params":  d.get("params", []),
        "headers": d.get("headers") or config.get("headers") or {},
    }
    config["sports"] = sports_cfg

    if hasattr(bm, "harvest_config"):
        bm.harvest_config = config
    db.session.commit()

    return jsonify({
        "ok":           True,
        "bookmaker_id": bm_id,
        "sport":        sport_name,
        "config":       sports_cfg[sport_name],
        "saved_at":     datetime.now(timezone.utc).isoformat(),
    })


@bp.route("/bookmakers/<int:bm_id>/config/sports/<sport_name>", methods=["DELETE"])
def delete_sport_config(bm_id: int, sport_name: str):
    """DELETE /bookmakers/<id>/config/sports/Football — remove one sport's config."""
    from app.extensions import db
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)

    config = getattr(bm, "harvest_config", None) or {}
    if isinstance(config, str):
        try: config = json.loads(config)
        except Exception: config = {}

    sports_cfg = config.get("sports") or {}
    if sport_name not in sports_cfg:
        return jsonify({"error": f"Sport '{sport_name}' not configured"}), 404

    _push_config_history(bm_id, config, saved_by=f"delete_sport:{sport_name}")
    del sports_cfg[sport_name]
    config["sports"] = sports_cfg
    if hasattr(bm, "harvest_config"):
        bm.harvest_config = config
    db.session.commit()

    return jsonify({"ok": True, "deleted_sport": sport_name, "remaining": list(sports_cfg.keys())})


@bp.route("/bookmakers/<int:bm_id>/config/sports/auto-fill", methods=["POST"])
def auto_fill_sports(bm_id: int):
    """
    POST /bookmakers/<id>/config/sports/auto-fill
    Auto-generate sport configs from existing params (partner, gr, headers).
    Uses default BetB2B sport IDs.

    Body:
        {
          "sports": ["Football","Basketball","Tennis"],  ← null = all canonical sports
          "partner": "61",   ← optional override (reads from existing config if omitted)
          "gr":      "656"
        }
    """
    from app.extensions import db
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)
    d  = request.json or {}

    config = getattr(bm, "harvest_config", None) or {}
    if isinstance(config, str):
        try: config = json.loads(config)
        except Exception: config = {}

    # Extract partner/gr from existing config or body
    params  = config.get("params") or {}
    partner = d.get("partner") or params.get("partner") or ""
    gr      = d.get("gr")      or params.get("gr")      or ""
    headers = config.get("headers") or {}

    if not partner:
        return jsonify({"error": "partner ID required — pass in body or save in config.params.partner first"}), 422

    domain  = bm.domain
    sports  = d.get("sports") or list(_DEFAULT_BK_SPORT_IDS.keys())

    _push_config_history(bm_id, config, saved_by="auto-fill")

    sports_cfg = config.get("sports") or {}
    filled = []

    for sport_name in sports:
        if sport_name not in _DEFAULT_BK_SPORT_IDS:
            continue
        sport_id = _DEFAULT_BK_SPORT_IDS[sport_name]
        sport_params = _build_sport_params(domain, partner, gr, sport_id)
        sports_cfg[sport_name] = {
            "url":     f"https://{domain}/service-api/LiveFeed/Get1x2_VZip",
            "params":  sport_params,
            "headers": headers,
        }
        filled.append(sport_name)

    config["sports"] = sports_cfg
    if hasattr(bm, "harvest_config"):
        bm.harvest_config = config
    db.session.commit()

    return jsonify({
        "ok":             True,
        "bookmaker_id":   bm_id,
        "filled_sports":  filled,
        "partner":        partner,
        "gr":             gr,
        "sports":         sports_cfg,
        "saved_at":       datetime.now(timezone.utc).isoformat(),
    })


@bp.route("/bookmakers/<int:bm_id>/config/sports/validate-all", methods=["POST"])
def validate_all_sports(bm_id: int):
    """
    POST /bookmakers/<id>/config/sports/validate-all
    Live-probe every configured sport and return per-sport results.
    """
    Bookmaker = _get_model()
    bm = Bookmaker.query.get_or_404(bm_id)

    config = getattr(bm, "harvest_config", None) or {}
    if isinstance(config, str):
        try: config = json.loads(config)
        except Exception: config = {}

    sports_cfg = config.get("sports") or {}
    if not sports_cfg:
        return jsonify({"error": "No sport configs found — run auto-fill first"}), 422

    bm_dict = {
        "id":          bm.id,
        "name":        bm.name,
        "domain":      bm.domain,
        "vendor_slug": getattr(bm, "vendor_slug", "betb2b") or "betb2b",
        "config":      config,
    }

    from app.workers.bookmaker_fetcher import fetch_bookmaker
    import time as _time
    results = {}

    for sport_name in sports_cfg:
        t0 = _time.perf_counter()
        try:
            matches  = fetch_bookmaker(bm_dict, sport_name=sport_name, mode="live", timeout=15)
            latency  = int((_time.perf_counter() - t0) * 1000)
            results[sport_name] = {
                "ok":         len(matches) > 0,
                "count":      len(matches),
                "latency_ms": latency,
                "sample":     [{"home": m["home_team"], "away": m["away_team"],
                                "markets": list(m.get("markets",{}).keys())} for m in matches[:2]],
            }
        except Exception as exc:
            latency = int((_time.perf_counter() - t0) * 1000)
            results[sport_name] = {"ok": False, "count": 0, "latency_ms": latency, "error": str(exc)}

    return jsonify({
        "bookmaker_id": bm_id,
        "bookmaker":    bm.name,
        "results":      results,
        "total_sports": len(sports_cfg),
        "ok_sports":    sum(1 for r in results.values() if r["ok"]),
    })