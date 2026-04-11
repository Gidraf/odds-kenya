from flask import Blueprint, request, Response, stream_with_context
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from .utils import _sse

bp_deep_analytics = Blueprint("deep_analytics", __name__, url_prefix="/api")

# You should replace this with your dynamic token fetcher if you have one
DEFAULT_TOKEN = "exp=1776014087~acl=/*~data=eyJvIjoiaHR0cHM6Ly93d3cua2Uuc3BvcnRwZXNhLmNvbSIsImEiOiJmODYxN2E4OTZkMzU1MWJhNTBkNTFmMDE0OWQ0YjZkZCIsImFjdCI6Im9yaWdpbmNoZWNrIiwib3NyYyI6Im9yaWdpbiJ9~hmac=0c5778166001c92fb20fe250e531cbfcacdc6e557ef04ddfd4162720cbad72ce"

def _fetch_sr(endpoint: str, item_id: str, token: str):
    url = f"https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/{endpoint}/{item_id}?T={token}"
    headers = {
        "accept": "application/json",
        "origin": "https://www.ke.sportpesa.com",
        "referer": "https://www.ke.sportpesa.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code == 200:
            return res.json().get("doc", [{}])[0].get("data", {})
    except Exception as e:
        print(f"SR Fetch Error ({endpoint}):", e)
    return None

@bp_deep_analytics.route("/odds/match/<betradar_id>/deep_analytics/stream")
def stream_deep_analytics(betradar_id: str):
    token = request.args.get("token", DEFAULT_TOKEN)

    def generate():
        yield _sse("status", {"step": "Initializing", "message": "Connecting to Sportradar..."})

        # 1. Fetch Timeline (Provides initial data, plus Team UIDs and Season ID)
        timeline = _fetch_sr("match_timeline", betradar_id, token)
        if not timeline:
            yield _sse("error", {"message": "Could not load match metadata."})
            return

        match_info = timeline.get("match", {})
        home_uid = match_info.get("teams", {}).get("home", {}).get("uid")
        away_uid = match_info.get("teams", {}).get("away", {}).get("uid")
        season_id = match_info.get("_seasonid")
        
        # Stream basic metadata immediately
        yield _sse("meta", {
            "home_team": match_info.get("teams", {}).get("home", {}).get("name"),
            "away_team": match_info.get("teams", {}).get("away", {}).get("name"),
            "status": match_info.get("status", {}).get("name"),
            "match_time": match_info.get("timeinfo", {}).get("played"),
            "score": match_info.get("result", {})
        })

        # 2. Concurrently fetch Momentum, Table, Home Form, Away Form, and Lineups
        with ThreadPoolExecutor(max_workers=5) as pool:
            f_momentum = pool.submit(_fetch_sr, "stats_match_situation", betradar_id, token)
            f_home_form = pool.submit(_fetch_sr, "stats_team_lastx", home_uid, token) if home_uid else None
            f_away_form = pool.submit(_fetch_sr, "stats_team_lastx", away_uid, token) if away_uid else None
            f_table = pool.submit(_fetch_sr, "season_dynamictable", season_id, token) if season_id else None
            f_lineups = pool.submit(_fetch_sr, "match_lineups", betradar_id, token)
            
            # Yield Momentum
            momentum_data = f_momentum.result()
            if momentum_data:
                parsed_momentum = []
                for minute in momentum_data.get("data", []):
                    parsed_momentum.append({
                        "time": minute.get("time"),
                        "home_danger": minute.get("home", {}).get("dangerous", 0),
                        "away_danger": minute.get("away", {}).get("dangerous", 0)
                    })
                yield _sse("momentum", parsed_momentum)

            # Yield H2H Form
            if f_home_form and f_away_form:
                hf = f_home_form.result()
                af = f_away_form.result()
                
                def _parse_form(form_data):
                    if not form_data: return []
                    res = []
                    for m in form_data.get("matches", [])[:5]: # Get last 5
                        res.append("W" if m.get("result", {}).get("winner") == "home" else "L" if m.get("result", {}).get("winner") == "away" else "D")
                    return res
                
                yield _sse("form", {"home": _parse_form(hf), "away": _parse_form(af)})

            # Yield League Table (Standings)
            if f_table:
                table_data = f_table.result()
                if table_data:
                    rows = []
                    # Find the "Total" table type
                    tables = table_data.get("season", {}).get("tables", [])
                    for t in tables:
                        if t.get("name") == "Total" or len(t.get("tablerows", [])) > 0:
                            for tr in t.get("tablerows", []):
                                rows.append({
                                    "pos": tr.get("pos"),
                                    "team": tr.get("team", {}).get("name"),
                                    "played": tr.get("total"),
                                    "gd": tr.get("goalDiffTotal"),
                                    "pts": tr.get("pointsTotal"),
                                    "is_target": str(tr.get("team", {}).get("uid")) in [str(home_uid), str(away_uid)]
                                })
                            break
                    rows = sorted(rows, key=lambda x: x["pos"])
                    yield _sse("standings", rows)

            # Yield Lineups (If available)
            lineups_data = f_lineups.result()
            if lineups_data:
                yield _sse("lineups", lineups_data)
            else:
                yield _sse("lineups", {"status": "Not announced yet"})

        yield _sse("done", {"status": "complete"})

    return Response(stream_with_context(generate()), mimetype="text/event-stream")