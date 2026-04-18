# app/workers/live_broadcaster.py
import json
import logging
import os
import redis

# Set up the event logger
logger = logging.getLogger("kinetic_live_events")
logger.setLevel(logging.INFO)

# Connect to Redis DB 2 (The same one your SSE endpoint uses)
redis_url = os.getenv("REDIS_URL", "redis://localhost:6382/0")
base_url = redis_url.rsplit("/", 1)[0] if redis_url.count("/") >= 3 else redis_url
r = redis.from_url(f"{base_url}/2", decode_responses=True)

def get_internal_id(betradar_id: str):
    if not betradar_id: return None
    
    cache_key = f"map:br_to_db:{betradar_id}"
    cached = r.get(cache_key)
    if cached: return int(cached)
    
    # Fallback to DB if not in Redis
    try:
        from app.models.odds_model import UnifiedMatch
        from app.extensions import db
        um = db.session.query(UnifiedMatch.id).filter_by(parent_match_id=str(betradar_id)).first()
        if um:
            r.setex(cache_key, 3600, um[0])
            return um[0]
    except Exception:
        pass
    return None

def broadcast_event_state(betradar_id: str, bookie: str, old_state: dict, new_state: dict):
    """Detects game events (Goals, Phases), logs them, and pushes to frontend."""
    internal_id = get_internal_id(betradar_id)
    if not internal_id: return

    old_score = old_state.get("score", "").strip()
    new_score = new_state.get("score", "").strip()
    
    # 1. Log Goals
    if new_score and new_score != old_score and old_score != "":
        logger.info(f"⚽ GOAL! [{bookie.upper()}] Match {internal_id} | {old_score} ➔ {new_score}")
        
    # 2. Log Phase Changes (Kickoff, Halftime, Fulltime)
    old_phase = old_state.get("phase", "").lower()
    new_phase = new_state.get("phase", "").lower()
    if new_phase and new_phase != old_phase:
        if new_phase in ("halftime", "ht", "half-time"):
            logger.info(f"⏱️ HALF-TIME [{bookie.upper()}] Match {internal_id}")
        elif new_phase in ("started", "1h", "first half"):
            logger.info(f"🟢 KICKOFF [{bookie.upper()}] Match {internal_id}")
        elif new_phase in ("ended", "ft", "full-time"):
            logger.info(f"🏁 FULL-TIME [{bookie.upper()}] Match {internal_id}")
        else:
            logger.info(f"🔄 PHASE UPDATE [{bookie.upper()}] Match {internal_id} ➔ {new_phase.upper()}")

    # 3. Publish to Frontend SSE Channel
    payload = {
        "match_id": internal_id,
        "current_score": new_score,
        "match_time": new_state.get("match_time", ""),
        "event_status": new_state.get("phase", ""),
        "is_live": True
    }
    
    # Automatically split scores for the UI
    if "-" in new_score:
        parts = new_score.split("-")
        payload["score_home"], payload["score_away"] = parts[0].strip(), parts[1].strip()
    elif ":" in new_score:
        parts = new_score.split(":")
        payload["score_home"], payload["score_away"] = parts[0].strip(), parts[1].strip()

    r.publish(f"match:update:{internal_id}", json.dumps(payload))

def broadcast_market_odds(betradar_id: str, bookie: str, market_slug: str, outcomes: dict):
    """Pushes real-time odds changes directly to the UI."""
    internal_id = get_internal_id(betradar_id)
    if not internal_id: return
    
    payload = {
        "match_id": internal_id,
        "bookmakers": {
            bookie: {
                "markets": {
                    market_slug: outcomes  # e.g., {"1": 2.50, "X": 3.10, "2": 2.80}
                }
            }
        }
    }
    r.publish(f"match:update:{internal_id}", json.dumps(payload))