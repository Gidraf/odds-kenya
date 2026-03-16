"""
SocketIO Event Handlers — /admin namespace
==========================================
Handles:
- Agent status (research + harvest)
- Login credential collection (popup in UI)
- OTP collection mid-session
"""
import json
from datetime import datetime, timezone
from flask_socketio import emit
from app.extensions import socketio


@socketio.on("connect", namespace="/admin")
def admin_connect():
    print("🟢 Admin connected")
    emit("agent_status", {
        "level": "SUCCESS",
        "msg":   "Connected to OddsTerminal Agent Network",
        "ts":    datetime.now(timezone.utc).isoformat(),
    })


@socketio.on("disconnect", namespace="/admin")
def admin_disconnect():
    print("🔴 Admin disconnected")


# ── Credential response from UI ───────────────────────────────────────────────

@socketio.on("submit_credentials", namespace="/admin")
def handle_credentials(data: dict):
    """
    UI submits login credentials for a research session.
    We forward directly to the REST API endpoint via internal call.
    """
    session_id = data.get("session_id")
    username   = data.get("username", "")
    password   = data.get("password", "")

    if not all([session_id, username, password]):
        emit("agent_status", {
            "level": "ERROR",
            "msg":   "Missing session_id, username, or password",
            "ts":    datetime.now(timezone.utc).isoformat(),
        })
        return

    # Import here to avoid circular imports
    from app.extensions import db
    from app.models.research_model import ResearchSession, ResearchFinding
    from app.models.bookmakers_model import Bookmaker

    sess = db.session.get(ResearchSession, session_id)
    if not sess:
        emit("agent_status", {"level": "ERROR", "msg": "Session not found"})
        return

    bm = db.session.get(Bookmaker, sess.bookmaker_id)

    # Retrieve stored auth_info
    auth_finding = ResearchFinding.query.filter_by(
        session_id=session_id, category="AUTH_FLOW"
    ).order_by(ResearchFinding.id).first()

    auth_info = {"login_url": sess.login_url}
    if auth_finding and auth_finding.detail:
        try:
            auth_info = json.loads(auth_finding.detail)
        except Exception:
            pass

    sess.username_used = username
    sess.phase         = "AUTHENTICATING"
    db.session.commit()

    from app.services.agents_tasks import research_authenticated
    research_authenticated.apply_async(
        args=[session_id, sess.bookmaker_id, bm.domain,
              auth_info, {"username": username, "password": password}],
        queue="playwright",
    )

    emit("agent_status", {
        "level": "SUCCESS",
        "msg":   f"Credentials received — launching authenticated research for {bm.domain}",
        "ts":    datetime.now(timezone.utc).isoformat(),
    })


@socketio.on("submit_otp", namespace="/admin")
def handle_otp(data: dict):
    """UI submits OTP during authenticated research."""
    session_id = data.get("session_id")
    otp        = data.get("otp", "")

    if not session_id or not otp:
        emit("agent_status", {"level": "ERROR", "msg": "Missing session_id or otp"})
        return

    # Re-broadcast so the running Celery task (via Redis sub) can pick it up
    socketio.emit("otp_supplied", {
        "session_id": session_id,
        "otp":        otp,
    }, namespace="/admin")

    emit("agent_status", {
        "level": "INFO",
        "msg":   f"OTP forwarded to agent (session #{session_id})",
        "ts":    datetime.now(timezone.utc).isoformat(),
    })


@socketio.on("ui_command_response", namespace="/admin")
def handle_ui_response(data: dict):
    """Generic command response (OTP / captcha / 2FA)."""
    emit("agent_status", {
        "level": "INFO",
        "msg":   f"UI response received: {str(data)[:100]}",
        "ts":    datetime.now(timezone.utc).isoformat(),
    })