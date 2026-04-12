from flask import Blueprint, request, Response, stream_with_context
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from .utils import _sse 

bp_deep_analytics = Blueprint("deep_analytics", __name__, url_prefix="/api")

LMT_TOKEN = "exp=1776025306~acl=/*~data=eyJvIjoiaHR0cHM6Ly93d3cuYmV0aWthLmNvbSIsImEiOiI2MDAwNmI1MjM0YzMxY2NmOGIxNGYxNmYyODczZWU3MSIsImFjdCI6Im9yaWdpbmNoZWNrIiwib3NyYyI6Im9yaWdpbiJ9~hmac=016ea9a66a30e7c493628bc5a2beb8e294aeefa76ea7582648f6e40904e395d4"
SH_TOKEN = "exp=1776064004~acl=/*~data=eyJvIjoiaHR0cHM6Ly9zdGF0c2h1Yi5zcG9ydHJhZGFyLmNvbSIsImEiOiJzcG9ydHBlc2EiLCJhY3QiOiJvcmlnaW5jaGVjayIsIm9zcmMiOiJob3N0aGVhZGVyIn0~hmac=1c7b2ef7f250e867db4f35699ca70d55884e705200df665ee15860e7eb4cddd6"

def _fetch_lmt(endpoint: str, item_id: str):
    """Hits Sportradar Live Match Tracker (Fast Realtime Data)"""
    url = f"https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/{endpoint}/{item_id}?T={LMT_TOKEN}"
    headers = {"origin": "https://www.betika.com", "referer": "https://www.betika.com/", "user-agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code == 200: return res.json().get("doc", [{}])[0].get("data", {})
    except: pass
    return None

def _fetch_sh(endpoint: str, item_id: str, extra=""):
    """Hits Sportradar StatsHub (Deep Historical Data)"""
    url = f"https://sh.fn.sportradar.com/sportpesa/en/Etc:UTC/gismo/{endpoint}/{item_id}{extra}?T={SH_TOKEN}"
    headers = {"origin": "https://statshub.sportradar.com", "referer": "https://statshub.sportradar.com/", "user-agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code == 200: return res.json().get("doc", [{}])[0].get("data", {})
    except: pass
    return None

@bp_deep_analytics.route("/odds/match/<betradar_id>/deep_analytics/stream")
def stream_deep_analytics(betradar_id: str):
    def generate():
        yield _sse("status", {"step": "Initializing", "message": "Connecting to Sportradar..."})

        # 1. Fetch Main Match Info
        info = _fetch_lmt("match_info", betradar_id) or _fetch_sh("match_info_statshub", betradar_id)
        
        if not info:
            yield _sse("error", {"message": "Could not load match info."})
            return

        match_data = info.get("match", {})
        home_uid = match_data.get("teams", {}).get("home", {}).get("uid")
        away_uid = match_data.get("teams", {}).get("away", {}).get("uid")
        season_id = match_data.get("_seasonid")

        jerseys = info.get("jerseys", {})
        home_color = f"#{jerseys.get('home', {}).get('player', {}).get('base', 'F06C6C')}"
        away_color = f"#{jerseys.get('away', {}).get('player', {}).get('base', '0DD8E8')}"

        match_time = match_data.get("timeinfo", {}).get("played")
        if not match_time: match_time = match_data.get("p")
        if not match_time: match_time = match_data.get("status", {}).get("shortName", "")

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

        # 2. Concurrently fetch deep stats
        with ThreadPoolExecutor(max_workers=5) as pool:
            f_squads   = pool.submit(_fetch_lmt, "match_squads", betradar_id)
            f_timeline = pool.submit(_fetch_lmt, "match_timelinedelta", betradar_id)
            f_h2h      = pool.submit(_fetch_sh, "stats_match_head2head", betradar_id)
            f_table    = pool.submit(_fetch_sh, "season_dynamictable", season_id) if season_id else None
            f_form     = pool.submit(_fetch_sh, "stats_formtable", season_id) if season_id else None

            # --- SQUADS (Safely handling Dicts AND Lists to prevent 500 error) ---
            squads_data = f_squads.result()
            if squads_data and "home" in squads_data:
                # Extract the lineup nodes
                h_node = squads_data.get("home", {}).get("startinglineup", squads_data.get("home", {}).get("players", []))
                a_node = squads_data.get("away", {}).get("startinglineup", squads_data.get("away", {}).get("players", []))
                
                # Check if it's a dict (has formation) or just a raw list of players
                h_form = h_node.get("formation", "") if isinstance(h_node, dict) else ""
                a_form = a_node.get("formation", "") if isinstance(a_node, dict) else ""
                
                h_players = h_node.get("players", []) if isinstance(h_node, dict) else (h_node if isinstance(h_node, list) else [])
                a_players = a_node.get("players", []) if isinstance(a_node, dict) else (a_node if isinstance(a_node, list) else [])

                yield _sse("lineups", {
                    "home": {
                        "formation": h_form,
                        "players": [{"name": p.get("playername", p.get("name", "")).split(",")[0].strip(), "num": p.get("shirtnumber", ""), "pos": p.get("matchpos", "M")} for p in h_players]
                    },
                    "away": {
                        "formation": a_form,
                        "players": [{"name": p.get("playername", p.get("name", "")).split(",")[0].strip(), "num": p.get("shirtnumber", ""), "pos": p.get("matchpos", "M")} for p in a_players]
                    }
                })
            else:
                yield _sse("lineups", {"fallback": True})

            # --- TIMELINE (EVENTS) ---
            timeline_data = f_timeline.result()
            if timeline_data:
                events = timeline_data.get("events", [])
                ignored = ["possession", "matchsituation", "ballcoordinates", "possible_event", "pitch coordinates"]
                comments = [{"time": ev.get("time", ""), "team": ev.get("team"), "type": ev.get("type"), "name": ev.get("name", "")} for ev in reversed(events) if ev.get("type") not in ignored]
                yield _sse("comments", comments[:20])

            # --- HEAD 2 HEAD ---
            h2h_data = f_h2h.result()
            if h2h_data:
                matches = h2h_data.get("matches", [])
                parsed_h2h = []
                for m in matches[:5]:
                    dt = m.get("_dt", {}).get("date", "")
                    ht = m.get("teams", {}).get("home", {}).get("name", "")
                    at = m.get("teams", {}).get("away", {}).get("name", "")
                    sh = m.get("result", {}).get("home", 0)
                    sa = m.get("result", {}).get("away", 0)
                    parsed_h2h.append({"date": dt, "home": ht, "away": at, "score_home": sh, "score_away": sa})
                yield _sse("h2h", parsed_h2h)

            # --- STANDINGS & FORM ---
            table_data = f_table.result() if f_table else None
            form_data = f_form.result() if f_form else None

            if table_data:
                rows = []
                tables = table_data.get("tables", []) or table_data.get("season", {}).get("tables", [])
                for t in tables:
                    if t.get("name") == "Total" or len(t.get("tablerows", [])) > 0:
                        for tr in t.get("tablerows", []):
                            is_home = str(tr.get("team", {}).get("uid")) == str(home_uid)
                            is_away = str(tr.get("team", {}).get("uid")) == str(away_uid)
                            rows.append({
                                "pos": tr.get("pos"),
                                "team": tr.get("team", {}).get("name"),
                                "played": tr.get("played", {}).get("total", tr.get("total", 0)),
                                "gd": tr.get("goaldifference", {}).get("total", tr.get("goalDiffTotal", 0)),
                                "pts": tr.get("points", {}).get("total", tr.get("pointsTotal", 0)),
                                "is_target": is_home or is_away
                            })
                        break
                yield _sse("standings", sorted(rows, key=lambda x: x["pos"]))

            if form_data:
                target_form = {"home": [], "away": []}
                for t in form_data.get("teams", []):
                    uid = str(t.get("team", {}).get("uid"))
                    form_list = [f.get("value") for f in t.get("form", {}).get("total", [])]
                    if uid == str(home_uid): target_form["home"] = form_list
                    if uid == str(away_uid): target_form["away"] = form_list
                yield _sse("form", target_form)

        yield _sse("done", {"status": "complete"})

    return Response(stream_with_context(generate()), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"
    })