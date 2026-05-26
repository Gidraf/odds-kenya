"""
app/views/customer/download_routes_v2.py
────────────────────────────────────────
Flask Blueprint routes for the v2 per-group Word document download system.

Endpoints
─────────
    GET  /odds/download/v2/markets
    GET  /odds/download/v2/groups   ?sport=soccer&date=2025-05-25
    GET  /odds/download/v2/file     ?sport=soccer&date=…&group=evening&markets=…
    POST /odds/download/v2/batch    body JSON {sport, date, groups, markets}
"""

from __future__ import annotations

import io
import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, request, jsonify, make_response, send_file

bp_download_v2 = Blueprint("download_v2", __name__)

EAT         = timedelta(hours=3)
FREE_SPORTS = {"soccer", "football"}

MARKET_META = [
    {"id": "1x2",  "label": "Full-Time 1X2",        "aliases": ["1x2", "match_winner"]},
    {"id": "btts", "label": "Both Teams to Score",   "aliases": ["btts"]},
    {"id": "dc",   "label": "Double Chance",          "aliases": ["double_chance"]},
    {"id": "dnb",  "label": "Draw No Bet",            "aliases": ["dnb"]},
    {"id": "ht",   "label": "Half-Time Result",       "aliases": ["half_time"]},
    {"id": "ou15", "label": "Over/Under 1.5 Goals",  "aliases": ["over_under_goals_1_5"]},
    {"id": "ou25", "label": "Over/Under 2.5 Goals",  "aliases": ["over_under_goals_2_5"]},
    {"id": "ou35", "label": "Over/Under 3.5 Goals",  "aliases": ["over_under_goals_3_5"]},
]

_CORS_HEADERS = {
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Admin-Key, x-admin-key",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cors(response):
    """Attach CORS headers to any response object."""
    for k, v in _CORS_HEADERS.items():
        response.headers[k] = v
    return response


def _preflight():
    """Return a 200 OK response to an OPTIONS preflight request."""
    resp = make_response("", 200)
    return _cors(resp)


def _today_eat() -> str:
    return (datetime.now(timezone.utc) + EAT).strftime("%Y-%m-%d")


def _log(event: str, props: dict):
    try:
        from app.utils.decorators_ import log_event
        log_event(event, props)
    except Exception:
        pass


# ── Blueprint-level before_request: answer every OPTIONS instantly ────────────

@bp_download_v2.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        return _preflight()


# ── Blueprint-level after_request: stamp CORS headers on every real response ──

@bp_download_v2.after_request
def add_cors(response):
    return _cors(response)


# =============================================================================
# ROUTES
# =============================================================================

@bp_download_v2.route("/odds/download/v2/markets", methods=["GET", "OPTIONS"])
def get_market_options():
    return jsonify({"ok": True, "markets": MARKET_META})


@bp_download_v2.route("/odds/download/v2/groups", methods=["GET", "OPTIONS"])
def get_download_groups():
    """
    Return available time groups for a sport + date, with match counts.

    Query params:
        sport  – e.g. "soccer"  (default: "soccer")
        date   – EAT date "YYYY-MM-DD"  (default: today)
    """
    sport    = (request.args.get("sport") or "soccer").lower().strip()
    date_str = (request.args.get("date")  or _today_eat()).strip()

    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

    try:
        from app.views.customer.word_generator_v2 import get_available_groups
        groups = get_available_groups(sport, date_str)
    except Exception as exc:
        return jsonify({"error": str(exc), "groups": []}), 500

    _log("v2_groups_fetch", {"sport": sport, "date": date_str, "count": len(groups)})
    return jsonify({"ok": True, "sport": sport, "date": date_str, "groups": groups})


@bp_download_v2.route("/odds/download/v2/file", methods=["GET", "OPTIONS"])
def download_group_file():
    """
    Stream a Word document for one time group.

    Query params:
        sport    – e.g. "soccer"
        date     – "YYYY-MM-DD" (EAT, default today)
        group    – time group id e.g. "evening"
        markets  – comma-separated market ids (optional, omit = all)
        token    – JWT (optional; required for non-soccer sports)
    """
    t0       = time.perf_counter()
    sport    = (request.args.get("sport")   or "soccer").lower().strip()
    date_str = (request.args.get("date")    or _today_eat()).strip()
    group    = (request.args.get("group")   or "").strip()
    markets_raw = request.args.get("markets") or ""

    if not group:
        return jsonify({"error": "group parameter is required."}), 400

    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

    # Auth check (non-soccer only)
    user = None
    if sport not in FREE_SPORTS:
        try:
            from app.utils.customer_jwt_helpers import _current_user_from_header
            user = _current_user_from_header()
        except Exception:
            pass
        if not user:
            return jsonify({"error": "Authentication required for this sport."}), 401

    market_filter = [m.strip() for m in markets_raw.split(",") if m.strip()] or None

    _log("v2_report_download", {
        "sport": sport, "date": date_str, "group": group,
        "markets": market_filter, "user_id": getattr(user, "id", None),
    })

    try:
        from app.views.customer.word_generator_v2 import generate_group_document, TIME_GROUPS
        if group not in TIME_GROUPS:
            return jsonify({"error": f"Unknown group '{group}'.", "valid": list(TIME_GROUPS.keys())}), 400
        buf = generate_group_document(sport, group, date_str, market_filter)
    except Exception as exc:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"Document generation failed: {str(exc)}"}), 500

    from app.views.customer.word_generator_v2 import TIME_GROUPS as _TG
    group_label = _TG.get(group, ("matches", 0, 0))[0]
    safe_label  = "".join(
        c if c.isalnum() or c in " _-" else ""
        for c in group_label.encode("ascii", "ignore").decode()
    ).strip().replace(" ", "_")

    filename = f"OddsKenya_{sport.capitalize()}_{date_str}_{safe_label}_{time.strftime('%H%M%S')}.docx"

    response = make_response(send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        as_attachment=True,
        download_name=filename,
    ))
    response.headers["X-Latency-Ms"] = str(int((time.perf_counter() - t0) * 1000))
    response.headers["X-Group"]      = group
    response.headers["X-Sport"]      = sport
    response.headers["X-Date"]       = date_str
    # _cors() will be applied by after_request
    return response


@bp_download_v2.route("/odds/download/v2/batch", methods=["POST", "OPTIONS"])
def download_batch():
    """
    Generate and return a ZIP containing one .docx per requested group.

    Body JSON: {sport, date, groups: ["morning","evening"], markets: ["1x2"]}
    """
    import zipfile

    data          = request.get_json(silent=True) or {}
    sport         = (data.get("sport") or "soccer").lower().strip()
    date_str      = (data.get("date")  or _today_eat()).strip()
    groups        = data.get("groups") or []
    market_filter = data.get("markets") or None

    if not groups:
        return jsonify({"error": "Provide at least one group."}), 400

    if sport not in FREE_SPORTS:
        user = None
        try:
            from app.utils.customer_jwt_helpers import _current_user_from_header
            user = _current_user_from_header()
        except Exception:
            pass
        if not user:
            return jsonify({"error": "Authentication required."}), 401

    try:
        from app.views.customer.word_generator_v2 import generate_group_document, TIME_GROUPS
    except ImportError as exc:
        return jsonify({"error": str(exc)}), 500

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for gid in groups:
            if gid not in TIME_GROUPS:
                continue
            try:
                doc_buf = generate_group_document(sport, gid, date_str, market_filter)
                label   = "".join(
                    c if c.isalnum() or c in " _" else ""
                    for c in TIME_GROUPS[gid][0].encode("ascii", "ignore").decode()
                ).replace(" ", "_")
                zf.writestr(f"OddsKenya_{sport.capitalize()}_{date_str}_{label}.docx", doc_buf.read())
            except Exception:
                pass

    zip_buf.seek(0)
    return make_response(send_file(
        zip_buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"OddsKenya_{sport.capitalize()}_{date_str}_Booklets.zip",
    ))