"""
Socket event handlers for the /admin namespace.
Must be imported in create_app() AFTER socketio.init_app().
"""
from app.extensions import socketio
from flask_socketio import emit, join_room


@socketio.on('connect', namespace='/admin')
def admin_connect():
    print("🟢 Admin client connected to /admin namespace")
    emit('agent_status', {
        'level': 'SUCCESS',
        'msg': 'Connected to Agent Control Network',
        'ts': __import__('datetime').datetime.utcnow().isoformat(),
    })


@socketio.on('disconnect', namespace='/admin')
def admin_disconnect():
    print("🔴 Admin client disconnected from /admin namespace")


@socketio.on('ui_command_response', namespace='/admin')
def handle_ui_response(data):
    """Admin sends OTP/captcha back to agent."""
    domain   = data.get('domain')
    otp_code = data.get('otp_code')
    print(f"[SOCKET] Received OTP {otp_code} for domain {domain}")
    emit('agent_status', {
        'level': 'INFO',
        'msg': f'OTP received for {domain}. Resuming agent...',
        'ts': __import__('datetime').datetime.utcnow().isoformat(),
    }, namespace='/admin')