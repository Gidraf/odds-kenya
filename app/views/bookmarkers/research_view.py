"""
Research API Routes
====================
POST /api/admin/research/start          — start unauthenticated research
POST /api/admin/research/credentials    — supply login credentials (Phase 3)
POST /api/admin/research/otp            — supply OTP mid-session
GET  /api/admin/research/<id>           — session status + findings summary
GET  /api/admin/research/<id>/report    — full markdown report
GET  /api/admin/research/sessions       — list all sessions
"""
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required

from app.extensions import db
from app.models.bookmakers_model import Bookmaker, BookmakerEndpoint
from app.models.research_model import ResearchSession, ResearchFinding, ResearchEndpoint

research_bp = Blueprint("research", __name__, url_prefix="/api/admin/research")


# ── helpers ──────────────────────────────────────────────────────────────────

def _get_or_404(model, pk):
    obj = db.session.get(model, pk)
    if not obj:
        return None, jsonify({"error": "Not found"}), 404
    return obj, None, None


# ── routes ───────────────────────────────────────────────────────────────────

@research_bp.post("/start")
@jwt_required()
def start_research():
    """
    Kick off unauthenticated research for a bookmaker.
    Body: { "bookmaker_id": 1 } or { "domain": "sportpesa.co.ke" }
    """
    body        = request.get_json() or {}
    bookmaker_id = body.get("bookmaker_id")
    domain       = body.get("domain", "").strip()

    # Resolve bookmaker
    if bookmaker_id:
        bm = db.session.get(Bookmaker, bookmaker_id)
    elif domain:
        bm = Bookmaker.query.filter_by(domain=domain).first()
        if not bm:
            clean = domain.lstrip("https://").lstrip("http://").split("/")[0]
            bm = Bookmaker(domain=clean, name=clean, is_active=False)
            db.session.add(bm)
            db.session.commit()
    else:
        return jsonify({"error": "bookmaker_id or domain required"}), 400

    if not bm:
        return jsonify({"error": "Bookmaker not found"}), 404

    # Create session record
    session = ResearchSession(bookmaker_id=bm.id, phase="UNAUTHENTICATED")
    db.session.add(session)
    db.session.commit()

    # Dispatch Celery task
    from app.services.agents_tasks import research_unauthenticated
    research_unauthenticated.apply_async(
        args=[bm.id, bm.domain, session.id],
        queue="playwright",
    )

    return jsonify({
        "msg":        "Unauthenticated research started",
        "session_id": session.id,
        "bookmaker":  bm.domain,
    }), 202


@research_bp.post("/credentials")
@jwt_required()
def supply_credentials():
    """
    Admin supplies login credentials after Phase 1 emits login_required.
    Body: { "session_id": 1, "username": "+254...", "password": "xxx" }
    """
    body       = request.get_json() or {}
    session_id = body.get("session_id")
    username   = body.get("username", "").strip()
    password   = body.get("password", "").strip()

    if not all([session_id, username, password]):
        return jsonify({"error": "session_id, username, password required"}), 400

    sess = db.session.get(ResearchSession, session_id)
    if not sess:
        return jsonify({"error": "Session not found"}), 404

    bm = db.session.get(Bookmaker, sess.bookmaker_id)

    # Update session
    sess.username_used = username
    sess.phase         = "AUTHENTICATING"
    db.session.commit()

    # Retrieve auth_info from findings
    auth_finding = ResearchFinding.query.filter_by(
        session_id=session_id, category="AUTH_FLOW"
    ).order_by(ResearchFinding.id).first()

    auth_info = {}
    if auth_finding and auth_finding.detail:
        try:
            import json
            auth_info = json.loads(auth_finding.detail)
        except Exception:
            auth_info = {"login_url": sess.login_url}

    # Dispatch authenticated research task
    from app.services.agents_tasks import research_authenticated
    research_authenticated.apply_async(
        args=[session_id, sess.bookmaker_id, bm.domain,
              auth_info, {"username": username, "password": password}],
        queue="playwright",
    )

    return jsonify({
        "msg":        "Authenticated research dispatched",
        "session_id": session_id,
    }), 202


@research_bp.post("/otp")
@jwt_required()
def supply_otp():
    """
    Admin supplies OTP while authenticated research is mid-login.
    This emits to the Celery task via Redis SocketIO.
    Body: { "session_id": 1, "otp": "123456" }
    """
    body       = request.get_json() or {}
    session_id = body.get("session_id")
    otp        = body.get("otp", "").strip()

    if not session_id or not otp:
        return jsonify({"error": "session_id and otp required"}), 400

    # Re-emit OTP to the running task via SocketIO
    from app.extensions import socketio
    socketio.emit("otp_supplied", {
        "session_id": session_id,
        "otp":        otp,
    }, namespace="/admin")

    return jsonify({"msg": "OTP forwarded to agent"}), 200


@research_bp.get("/sessions")
@jwt_required()
def list_sessions():
    sessions = ResearchSession.query.order_by(ResearchSession.id.desc()).limit(50).all()
    return jsonify([{
        "id":           s.id,
        "bookmaker_id": s.bookmaker_id,
        "phase":        s.phase,
        "started_at":   s.started_at.isoformat() if s.started_at else None,
        "completed_at": s.completed_at.isoformat() if s.completed_at else None,
        "login_url":    s.login_url,
        "otp_required": s.otp_required,
        "login_success":s.login_success,
        "findings_count":  ResearchFinding.query.filter_by(session_id=s.id).count(),
        "endpoints_count": ResearchEndpoint.query.filter_by(session_id=s.id).count(),
    } for s in sessions])


@research_bp.get("/<int:session_id>")
@jwt_required()
def get_session(session_id: int):
    sess = db.session.get(ResearchSession, session_id)
    if not sess:
        return jsonify({"error": "Not found"}), 404

    findings  = ResearchFinding.query.filter_by(session_id=session_id).all()
    endpoints = ResearchEndpoint.query.filter_by(session_id=session_id).all()
    bk_eps    = BookmakerEndpoint.query.filter_by(bookmaker_id=sess.bookmaker_id).all()

    return jsonify({
        "session": {
            "id":           sess.id,
            "bookmaker_id": sess.bookmaker_id,
            "phase":        sess.phase,
            "otp_required": sess.otp_required,
            "login_success":sess.login_success,
            "started_at":   sess.started_at.isoformat() if sess.started_at else None,
            "completed_at": sess.completed_at.isoformat() if sess.completed_at else None,
        },
        "findings": [{
            "id":       f.id, "category": f.category, "title": f.title,
            "severity": f.severity, "phase": f.phase,
            "detail":   f.detail, "code": f.code_snippet,
            "discovered_at": f.discovered_at.isoformat() if f.discovered_at else None,
        } for f in findings],
        "research_endpoints": [{
            "url":          e.url, "method": e.method,
            "status":       e.status_code, "type": e.endpoint_type,
            "auth_required":e.requires_auth, "is_auth": e.is_authenticated,
            "curl":         e.curl_command, "notes": e.notes,
            "pagination":   e.pagination_info,
        } for e in endpoints],
        "production_endpoints": [{
            "type":         ep.endpoint_type, "url_pattern": ep.url_pattern,
            "method":       ep.request_method, "parser_ok": ep.parser_test_passed,
            "active":       ep.is_active,
        } for ep in bk_eps],
    })


@research_bp.get("/<int:session_id>/report")
@jwt_required()
def get_report(session_id: int):
    sess = db.session.get(ResearchSession, session_id)
    if not sess:
        return jsonify({"error": "Not found"}), 404
    if not sess.report_md:
        return jsonify({"error": "Report not yet generated", "phase": sess.phase}), 202

    return jsonify({
        "session_id": session_id,
        "phase":      sess.phase,
        "report_md":  sess.report_md,
    })