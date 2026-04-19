# app/api/upcoming.py

from flask import request, Response, stream_with_context, Blueprint
from .decorators import tier_required
from . import bp_matches as bp_upcoming, _signed_response, _err
from app.workers.celery_tasks import _redis
import json
import time

# bp_upcoming = Blueprint("api_upcoming", __name__, url_prefix="/api/v1")

# -----------------------------------------------------------------------------
# REST Endpoint (Basic tier)
# -----------------------------------------------------------------------------
@bp_upcoming.route("/upcoming/matches", methods=["GET"])
@tier_required("basic")
def upcoming_matches():
    """
    List upcoming matches (cached). Basic tier can poll this.
    """
    sport = request.args.get("sport", "soccer")
    cache_key = f"unified:upcoming:{sport}"
    r = _redis()
    cached = r.get(cache_key)
    matches = json.loads(cached) if cached else []
    return _signed_response({"matches": matches})


# -----------------------------------------------------------------------------
# SSE Streaming Endpoint (Pro tier)
# -----------------------------------------------------------------------------
@bp_upcoming.route("/upcoming/stream", methods=["GET"])
@tier_required("pro")
def upcoming_stream():
    """
    SSE stream of upcoming match batch updates.
    Pro tier only – real‑time push of new harvests and enrichments.
    """
    sport = request.args.get("sport", "soccer")

    def generate():
        r = _redis()
        pubsub = r.pubsub(ignore_subscribe_messages=True)

        # Send cached snapshot immediately
        cache_key = f"unified:upcoming:{sport}"
        cached = r.get(cache_key)
        if cached:
            try:
                matches = json.loads(cached)
                yield f"event: batch\ndata: {json.dumps({'matches': matches, 'source': 'cache'})}\n\n"
            except Exception:
                pass

        channels = [
            f"odds:upcoming:{sport}",
            f"bus:live_updates:{sport}",
        ]
        pubsub.subscribe(*channels)

        yield f"event: connected\ndata: {json.dumps({'status': 'connected', 'channels': channels})}\n\n"

        last_keepalive = time.time()
        while True:
            msg = pubsub.get_message(timeout=1.0)
            if msg and msg["type"] == "message":
                try:
                    payload = json.loads(msg["data"])
                    channel = msg["channel"]
                    event_type = "batch" if "upcoming" in channel else "live_update"
                    yield f"event: {event_type}\ndata: {json.dumps(payload)}\n\n"
                except Exception:
                    pass

            if time.time() - last_keepalive > 15:
                yield ": keepalive\n\n"
                last_keepalive = time.time()

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )