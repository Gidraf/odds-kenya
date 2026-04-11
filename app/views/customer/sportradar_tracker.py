from flask import Blueprint, request
import requests
import time
from app.utils.customer_jwt_helpers import _err, _signed_response

bp_tracker = Blueprint("tracker", __name__, url_prefix="/api")

def _parse_sportradar_timeline(doc_data: dict) -> dict:
    """Parses the raw Sportradar timeline JSON into clean UI props."""
    match = doc_data.get("match", {})
    events = doc_data.get("events", [])

    # Basic Match Info
    home_team = match.get("teams", {}).get("home", {}).get("name", "Home")
    away_team = match.get("teams", {}).get("away", {}).get("name", "Away")
    score_home = match.get("result", {}).get("home", 0)
    score_away = match.get("result", {}).get("away", 0)
    
    # Calculate current minute from the last event or match.timeinfo
    current_time = match.get("timeinfo", {}).get("played", "0")
    if not current_time and events:
        current_time = str(events[-1].get("time", 0))

    scorers = []
    stats_tally = {
        "corner": {"home": 0, "away": 0, "label": "Corners"},
        "shotontarget": {"home": 0, "away": 0, "label": "Shots on Target"},
        "shotofftarget": {"home": 0, "away": 0, "label": "Shots off Target"},
        "freekick": {"home": 0, "away": 0, "label": "Free Kicks"},
    }

    live_coords = {"x": 50, "y": 50, "action": "Waiting for data..."}

    for ev in events:
        team = ev.get("team") # "home" or "away"
        ev_type = ev.get("type")
        
        # 1. Capture Goalscorers
        if ev_type == "goal" and team:
            player_name = ev.get("player", {}).get("name", "Unknown")
            minute = str(ev.get("time", ""))
            scorers.append({"minute": minute, "player": player_name.split(",")[-1].strip(), "team": team})

        # 2. Tally Stats
        if ev_type in stats_tally and team in ["home", "away"]:
            stats_tally[ev_type][team] += 1

        # 3. Capture Latest Action & Coordinates
        if "X" in ev and "Y" in ev:
            # Sportradar X/Y are usually 0-100. Y=100 is bottom, X=100 is right.
            # If away team attacks, X might need flipping, but we'll pass raw for now.
            live_coords = {
                "x": float(ev["X"]),
                "y": float(ev["Y"]),
                "action": ev.get("name", "Action"),
                "team": team
            }

    # Format stats array for the frontend
    stats_array = []
    for key, data in stats_tally.items():
        total = max(data["home"] + data["away"], 1) # Prevent div by zero
        stats_array.append({
            "label": data["label"],
            "homeVal": data["home"],
            "awayVal": data["away"],
            "max": total
        })

    return {
        "homeTeam": home_team,
        "awayTeam": away_team,
        "scoreHome": str(score_home),
        "scoreAway": str(score_away),
        "time": str(current_time),
        "scorers": scorers,
        "stats": stats_array,
        "liveCoords": live_coords
    }

@bp_tracker.route("/odds/match/<betradar_id>/tracker", methods=["GET"])
def get_match_tracker(betradar_id: str):
    """Fetches the latest match timeline from Sportradar."""
    # NOTE: You will need to inject your dynamic Sportradar token (T=exp=...) here in production
    SPORTRADAR_TOKEN = request.args.get("token", "YOUR_DEFAULT_TOKEN_HERE")
    
    url = f"https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/match_timeline/{betradar_id}?T={SPORTRADAR_TOKEN}"
    
    headers = {
        "accept": "application/json",
        "origin": "https://www.ke.sportpesa.com",
        "referer": "https://www.ke.sportpesa.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=5)
        resp.raise_for_status()
        raw_data = resp.json()
        
        # Navigate the Sportradar JSON nesting
        doc = raw_data.get("doc", [{}])[0]
        data = doc.get("data", {})
        
        if not data.get("match"):
            return _err("No live match data found", 404)
            
        parsed_payload = _parse_sportradar_timeline(data)
        return _signed_response({"ok": True, "tracker": parsed_payload})
        
    except Exception as e:
        return _err(f"Tracker error: {str(e)}", 500)