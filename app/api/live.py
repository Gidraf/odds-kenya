"""
app/api/live.py
================
Provides paginated REST endpoints for initial data loads (Basic/Pro) and 
real-time SSE streams (Pro) for Live and Upcoming matches.

Features:
- Pagination for both Live and Upcoming match lists.
- Unified SSE Stream listening to Redis Pub/Sub channels.
- Real-time event notifications (goals, cards, status changes, arb/ev alerts).
"""

import json
import time
from flask import request, Response, stream_with_context, current_app
from . import bp_live, _signed_response, _err
from .decorators import tier_required

# Mappings to match the Celery worker sport IDs and slugs
_SPORT_SLUG_TO_ID = {
    "soccer": 1, "basketball": 2, "tennis": 3, "ice-hockey": 5,
    "volleyball": 6, "handball": 7, "cricket": 9, "table-tennis": 13,
}

# -----------------------------------------------------------------------------
# REST Endpoints: Paginated Lists (Basic & Pro)
# -----------------------------------------------------------------------------

@bp_live.route("/live/matches", methods=["GET"])
@tier_required(["basic","pro"])
def live_matches_list():
    """
    Paginated list of currently live matches.
    Basic tier clients use this to poll or load the initial view.
    """
    from app.workers.celery_tasks import _redis
    sport = request.args.get("sport", "soccer")
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 20))
    
    r = _redis()
    sport_id = _SPORT_SLUG_TO_ID.get(sport, 1)
    
    # Fetch from the snapshot created by tasks_live.py
    raw_snapshot = r.get(f"sp:live:snapshot:{sport_id}")
    matches = []
    if raw_snapshot:
        data = json.loads(raw_snapshot)
        matches = data if isinstance(data, list) else data.get("events", [])

    # Apply Pagination
    total = len(matches)
    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    paginated_matches = matches[start_idx:end_idx]

    # Optional: Enrich with latest state/markets if needed from individual hashes
    for match in paginated_matches:
        ev_id = match.get("id")
        if ev_id:
            live_state = r.get(f"sp:live:state:{ev_id}")
            if live_state:
                match["live_state"] = json.loads(live_state)

    return _signed_response({
        "mode": "live",
        "sport": sport,
        "page": page,
        "limit": limit,
        "total": total,
        "matches": paginated_matches
    })


@bp_live.route("/upcoming/matches", methods=["GET"])
@tier_required(["basic", "pro"])
def upcoming_matches_list():
    """
    Paginated list of upcoming matches.
    """
    from app.workers.celery_tasks import _redis
    sport = request.args.get("sport", "soccer")
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 20))
    
    r = _redis()
    
    # Fetch from the merged snapshot created by tasks_upcoming.py
    raw_snapshot = r.get(f"odds:upcoming:all:{sport}")
    matches = []
    if raw_snapshot:
        data = json.loads(raw_snapshot)
        matches = data.get("matches", [])

    # Apply Pagination
    total = len(matches)
    start_idx = (page - 1) * limit
    end_idx = start_idx + limit
    paginated_matches = matches[start_idx:end_idx]

    return _signed_response({
        "mode": "upcoming",
        "sport": sport,
        "page": page,
        "limit": limit,
        "total": total,
        "matches": paginated_matches
    })


# -----------------------------------------------------------------------------
# SSE Streaming Endpoint (Pro tier only)
# -----------------------------------------------------------------------------

@bp_live.route("/stream", methods=["GET"])
@tier_required(["basic","pro"])
def unified_stream():
    """
    Unified SSE stream for real-time push updates.
    Handles Live, Upcoming, Match Status changes, and Arb/EV alerts.
    
    Query Params:
    - mode: 'live', 'upcoming', or 'all' (default)
    - sport: e.g., 'soccer'
    - match_ids[]: Optional list of specific IDs to track (ties to pagination)
    """
    from app.workers.celery_tasks import _redis
    mode = request.args.get("mode", "all")
    sport = request.args.get("sport", "soccer")
    match_ids = request.args.getlist("match_ids[]")

    def generate():
        r = _redis()
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        channels = []

        # 1. Broad Channels (Global odds and arbitrage updates)
        channels.append(f"arb:updates:{sport}")
        channels.append(f"ev:updates:{sport}")
        channels.append("odds:updates") # Captures match status changes, finished, postponed

        # 2. Mode-Specific Channels
        sport_id = _SPORT_SLUG_TO_ID.get(sport, 1)
        
        if mode in ["live", "all"]:
            channels.append(f"sp:live:sport:{sport_id}")
            channels.append("sp:live:all")
            
        if mode in ["upcoming", "all"]:
            channels.append(f"odds:upcoming:{sport}")

        # 3. Specific Match Channels (If client is paginating and only wants updates for visible rows)
        for mid in match_ids:
            channels.append(f"sp:live:event:{mid}")
            channels.append(f"live:match:{mid}:all")

        # Subscribe to compiled list
        pubsub.subscribe(*channels)

        # Send initial connection success event
        init_payload = {
            "status": "connected", 
            "mode": mode, 
            "sport": sport, 
            "subscribed_channels": channels,
            "ts": time.time()
        }
        yield f"event: connected\ndata: {json.dumps(init_payload)}\n\n"

        last_keepalive = time.time()
        
        try:
            while True:
                # 1.0s timeout allows the loop to breathe and send keepalives
                msg = pubsub.get_message(timeout=1.0)
                
                if msg and msg["type"] == "message":
                    channel_name = msg["channel"]
                    try:
                        # Raw data from Redis
                        raw_data = msg["data"]
                        payload = json.loads(raw_data)
                        
                        # Determine event type based on payload or channel
                        event_type = payload.get("type") or payload.get("event") or "update"
                        
                        # Stream the event to the client
                        yield f"event: {event_type}\ndata: {raw_data}\n\n"
                    except json.JSONDecodeError:
                        current_app.logger.error(f"[SSE] Invalid JSON on channel {channel_name}")
                    except GeneratorExit:
                        # Client disconnected
                        break
                    except Exception as e:
                        current_app.logger.warning(f"[SSE] Stream error: {e}")

                # Heartbeat to keep the connection alive through load balancers (Nginx/AWS)
                if time.time() - last_keepalive > 15:
                    yield ": keepalive\n\n"
                    last_keepalive = time.time()
                    
        finally:
            # Clean up Redis connection when client disconnects
            pubsub.unsubscribe()
            pubsub.close()

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform", 
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        }
    )