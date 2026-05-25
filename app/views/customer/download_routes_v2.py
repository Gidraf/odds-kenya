"""
download_routes_v2.py
─────────────────────
Flask Blueprint routes for the v2 per-group Word document download system.

Register in your app factory:
    from .download_routes_v2 import bp_download_v2
    app.register_blueprint(bp_download_v2)

Endpoints
─────────
    GET  /odds/download/v2/groups
         ?sport=soccer&date=2025-05-25
         → JSON list of available time groups with match counts

    GET  /odds/download/v2/file
         ?sport=soccer&date=2025-05-25&group=evening
         &markets=1x2,btts,ou25          (optional; omit = all markets)
         &token=<jwt>                     (optional; required for non-soccer)
         → Stream .docx file download

    GET  /odds/download/v2/markets
         → JSON list of available market definitions (for the frontend)
"""

from __future__ import annotations

from flask import Blueprint, request, jsonify, make_response, send_file
from datetime import datetime, timezone, timedelta
import io
import time

bp_download_v2 = Blueprint("download_v2", __name__)

EAT = timedelta(hours=3)
FREE_SPORTS = {"soccer", "football"}   # no auth needed for these

MARKET_META = [
    {"id": "1x2",    "label": "Full-Time 1X2",          "aliases": ["1x2", "match_winner"]},
    {"id": "btts",   "label": "Both Teams to Score",    "aliases": ["btts"]},
    {"id": "dc",     "label": "Double Chance",           "aliases": ["double_chance"]},
    {"id": "dnb",    "label": "Draw No Bet",             "aliases": ["dnb"]},
    {"id": "ht",     "label": "Half-Time Result",        "aliases": ["half_time"]},
    {"id": "ou15",   "label": "Over/Under 1.5 Goals",   "aliases": ["over_under_goals_1_5", "ou_1_5"]},
    {"id": "ou25",   "label": "Over/Under 2.5 Goals",   "aliases": ["over_under_goals_2_5", "ou_2_5"]},
    {"id": "ou35",   "label": "Over/Under 3.5 Goals",   "aliases": ["over_under_goals_3_5", "ou_3_5"]},
]


def _today_eat() -> str:
    """Return today's EAT date as YYYY-MM-DD."""
    return (datetime.now(timezone.utc) + EAT).strftime("%Y-%m-%d")


def _log(event: str, props: dict):
    try:
        from app.utils.decorators_ import log_event
        log_event(event, props)
    except Exception:
        pass


def _require_auth_for_sport(sport: str):
    """
    Returns the current user if auth is needed and valid.
    Returns None if the sport is free.
    Raises a 401 response tuple if auth is required but missing/invalid.
    """
    if sport.lower() in FREE_SPORTS:
        return None
    try:
        from app.utils.customer_jwt_helpers import _current_user_from_header
        user = _current_user_from_header()
        if not user:
            return None, (jsonify({"error": "Authentication required for this sport.", "code": "AUTH_REQUIRED"}), 401)
        return user, None
    except Exception:
        return None, (jsonify({"error": "Authentication required.", "code": "AUTH_REQUIRED"}), 401)


# ─────────────────────────────────────────────────────────────────────────────
@bp_download_v2.route("/odds/download/v2/markets")
def get_market_options():
    """Return the list of available market filters for the frontend."""
    return jsonify({"ok": True, "markets": MARKET_META})


# ─────────────────────────────────────────────────────────────────────────────
@bp_download_v2.route("/odds/download/v2/groups")
def get_download_groups():
    """
    Return available time groups for a sport + date, with match counts.

    Query params:
        sport   – e.g. "soccer" (default: "soccer")
        date    – EAT date "YYYY-MM-DD" (default: today)
    """
    sport    = (request.args.get("sport") or "soccer").lower().strip()
    date_str = (request.args.get("date") or _today_eat()).strip()

    # Validate date format
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

    try:
        from word_generator_v2 import get_available_groups
        groups = get_available_groups(sport, date_str)
    except Exception as exc:
        return jsonify({"error": str(exc), "groups": []}), 500

    _log("v2_groups_fetch", {"sport": sport, "date": date_str, "count": len(groups)})
    return jsonify({
        "ok":    True,
        "sport": sport,
        "date":  date_str,
        "groups": groups,
    })


# ─────────────────────────────────────────────────────────────────────────────
@bp_download_v2.route("/odds/download/v2/file")
def download_group_file():
    """
    Stream a Word document for one time group.

    Query params:
        sport    – e.g. "soccer"
        date     – "YYYY-MM-DD" (EAT, default today)
        group    – time group id e.g. "evening"
        markets  – comma-separated market ids to include (optional)
        token    – JWT bearer token (optional; required for non-soccer)
    """
    t0       = time.perf_counter()
    sport    = (request.args.get("sport") or "soccer").lower().strip()
    date_str = (request.args.get("date") or _today_eat()).strip()
    group    = (request.args.get("group") or "").strip()
    markets_raw = request.args.get("markets") or ""

    if not group:
        return jsonify({"error": "group parameter is required."}), 400

    # Validate date
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD."}), 400

    # Auth check
    user = None
    if sport not in FREE_SPORTS:
        try:
            from app.utils.customer_jwt_helpers import _current_user_from_header
            user = _current_user_from_header()
        except Exception:
            pass
        if not user:
            return make_response("Authentication required for this sport.", 401)

    # Parse market filter
    market_filter = None
    if markets_raw:
        market_filter = [m.strip() for m in markets_raw.split(",") if m.strip()]

    _log("v2_report_download", {
        "sport": sport, "date": date_str, "group": group,
        "markets": market_filter, "user_id": getattr(user, "id", None),
    })

    try:
        from word_generator_v2 import generate_group_document, TIME_GROUPS
        if group not in TIME_GROUPS:
            return jsonify({"error": f"Unknown group '{group}'. Valid: {list(TIME_GROUPS.keys())}"}), 400

        buf = generate_group_document(sport, group, date_str, market_filter)
    except Exception as exc:
        import traceback; traceback.print_exc()
        return jsonify({"error": f"Document generation failed: {str(exc)}"}), 500

    group_label = TIME_GROUPS.get(group, ("matches", 0, 0))[0]
    # Sanitize label for filename
    safe_label  = group_label.encode("ascii", "ignore").decode().strip()
    safe_label  = "".join(c if c.isalnum() or c in " _-" else "" for c in safe_label).strip().replace(" ", "_")

    filename = (
        f"OddsKenya_{sport.capitalize()}_{date_str}_{safe_label}_"
        f"{time.strftime('%H%M%S')}.docx"
    )

    latency_ms = int((time.perf_counter() - t0) * 1000)

    response = make_response(send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        as_attachment=True,
        download_name=filename,
    ))
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["X-Latency-Ms"]  = str(latency_ms)
    response.headers["X-Group"]       = group
    response.headers["X-Sport"]       = sport
    response.headers["X-Date"]        = date_str
    return response


# ─────────────────────────────────────────────────────────────────────────────
@bp_download_v2.route("/odds/download/v2/batch", methods=["POST"])
def download_batch():
    """
    Generate and return a ZIP containing one .docx per requested group.

    Body JSON:
        {sport, date, groups: ["morning","evening"], markets: ["1x2","btts"]}
    """
    import zipfile

    data         = request.get_json(silent=True) or {}
    sport        = (data.get("sport") or "soccer").lower().strip()
    date_str     = (data.get("date") or _today_eat()).strip()
    groups       = data.get("groups") or []
    market_filter = data.get("markets") or None

    if not groups:
        return jsonify({"error": "Provide at least one group."}), 400

    if sport not in FREE_SPORTS:
        try:
            from app.utils.customer_jwt_helpers import _current_user_from_header
            user = _current_user_from_header()
        except Exception:
            user = None
        if not user:
            return make_response("Authentication required.", 401)

    try:
        from word_generator_v2 import generate_group_document, TIME_GROUPS
    except ImportError as exc:
        return jsonify({"error": str(exc)}), 500

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for gid in groups:
            if gid not in TIME_GROUPS:
                continue
            try:
                doc_buf = generate_group_document(sport, gid, date_str, market_filter)
                label   = TIME_GROUPS[gid][0].encode("ascii", "ignore").decode().strip()
                label   = "".join(c if c.isalnum() or c in " _" else "" for c in label).replace(" ", "_")
                fname   = f"OddsKenya_{sport.capitalize()}_{date_str}_{label}.docx"
                zf.writestr(fname, doc_buf.read())
            except Exception:
                pass

    zip_buf.seek(0)
    zip_filename = f"OddsKenya_{sport.capitalize()}_{date_str}_Booklets.zip"

    response = make_response(send_file(
        zip_buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=zip_filename,
    ))
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response