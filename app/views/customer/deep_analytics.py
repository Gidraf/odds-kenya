from flask import Blueprint, request, Response, stream_with_context
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from .utils import _sse

bp_deep_analytics = Blueprint("deep_analytics", __name__, url_prefix="/api")

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
        pass
    return None

@bp_deep_analytics.route("/odds/match/<betradar_id>/deep_analytics/stream")
def stream_deep_analytics(betradar_id: str):
    token = request.args.get("token", DEFAULT_TOKEN)

    def generate():
        yield _sse("status", {"step": "Initializing", "message": "Connecting to Sportradar..."})

        # 1. Fetch Match Info (Crucial for Jerseys, Score, and Time)
        info = _fetch_sr("match_info", betradar_id, token)
        
        if not info:
            yield _sse("error", {"message": "Could not load match info."})
            return

        match_data = info.get("match", {})
        home_uid = match_data.get("teams", {}).get("home", {}).get("uid")
        away_uid = match_data.get("teams", {}).get("away", {}).get("uid")
        season_id = match_data.get("_seasonid")

        # Safely extract Jersey Colors
        jerseys = info.get("jerseys", {})
        home_color = f"#{jerseys.get('home', {}).get('player', {}).get('base', 'F06C6C')}"
        away_color = f"#{jerseys.get('away', {}).get('player', {}).get('base', '0DD8E8')}"

        # Determine Live Time
        match_time = match_data.get("timeinfo", {}).get("played")
        if not match_time: 
            match_time = match_data.get("p") # Sometimes stored as 'p' (e.g., "31")
        if not match_time:
            match_time = match_data.get("status", {}).get("shortName", "")

        # 2. Yield Metadata immediately to UI
        yield _sse("meta", {
            "home_team": match_data.get("teams", {}).get("home", {}).get("name", "Home"),
            "away_team": match_data.get("teams", {}).get("away", {}).get("name", "Away"),
            "status": match_data.get("status", {}).get("name", "Upcoming"),
            "match_time": str(match_time),
            "score_home": match_data.get("result", {}).get("home", 0),
            "score_away": match_data.get("result", {}).get("away", 0),
            "home_color": home_color,
            "away_color": away_color
        })

        # 3. Concurrently fetch Momentum, Table, Home Form, Away Form, and Lineups
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

            # Yield Form (H2H)
            if f_home_form and f_away_form:
                hf, af = f_home_form.result(), f_away_form.result()
                def _parse_form(form_data):
                    if not form_data: return []
                    return ["W" if m.get("result", {}).get("winner") == "home" else "L" if m.get("result", {}).get("winner") == "away" else "D" for m in form_data.get("matches", [])[:5]]
                yield _sse("form", {"home": _parse_form(hf), "away": _parse_form(af)})

            # Yield League Table
            if f_table:
                table_data = f_table.result()
                if table_data:
                    rows = []
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
                    yield _sse("standings", sorted(rows, key=lambda x: x["pos"]))

            # Yield Lineups (Safely handle the exception)
            lineups_data = f_lineups.result()
            if lineups_data and not lineups_data.get("message") == "Ups! Something went wrong":
                yield _sse("lineups", lineups_data)
            else:
                yield _sse("lineups", {"fallback": True}) # Tell UI to use default formation

        yield _sse("done", {"status": "complete"})

    return Response(stream_with_context(generate()), mimetype="text/event-stream")