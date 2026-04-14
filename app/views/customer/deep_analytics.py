import logging
from concurrent.futures import ThreadPoolExecutor
from flask import Blueprint, Response, stream_with_context
import requests
import json

bp_deep_analytics = Blueprint("deep_analytics", __name__, url_prefix="/api")

LMT_TOKEN = "exp=1776025306~acl=/*~data=...hmac=..." # Use your actual token
SH_TOKEN = "exp=1776064004~acl=/*~data=...hmac=..."   # Use your actual token

_HEADERS = {"origin": "https://www.betika.com", "referer": "https://www.betika.com/", "user-agent": "Mozilla/5.0"}

def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"

def _fetch_lmt(endpoint: str, item_id: str):
    url = f"https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/{endpoint}/{item_id}?T={LMT_TOKEN}"
    try:
        res = requests.get(url, headers=_HEADERS, timeout=5)
        if res.status_code == 200: return res.json().get("doc", [{}])[0].get("data", {})
    except: pass
    return None

def _fetch_sh(endpoint: str, item_id: str, extra=""):
    url = f"https://sh.fn.sportradar.com/sportpesa/en/Etc:UTC/gismo/{endpoint}/{item_id}{extra}?T={SH_TOKEN}"
    try:
        res = requests.get(url, headers=_HEADERS, timeout=5)
        if res.status_code == 200: return res.json().get("doc", [{}])[0].get("data", {})
    except: pass
    return None

@bp_deep_analytics.route("/odds/match/<betradar_id>/deep_analytics/stream")
def stream_deep_analytics(betradar_id: str):
    def generate():
        yield _sse("status", {"step": "Initializing", "message": "Connecting to Sportradar..."})

        # 1. Fetch Main Match Info & Delta Timeline
        info = _fetch_lmt("match_info", betradar_id) or _fetch_sh("match_info_statshub", betradar_id)
        timeline_data = _fetch_lmt("match_timelinedelta", betradar_id)

        if not info:
            yield _sse("error", {"message": "Could not load match info."})
            return

        match_data = info.get("match", {})
        home_uid = match_data.get("teams", {}).get("home", {}).get("uid")
        away_uid = match_data.get("teams", {}).get("away", {}).get("uid")
        season_id = match_data.get("_seasonid")

        # Fallback to timeline data if match_info is missing time
        if timeline_data:
            match_time = timeline_data.get("match", {}).get("timeinfo", {}).get("played")
            live_status = timeline_data.get("match", {}).get("status", {}).get("name")
        else:
            match_time = match_data.get("timeinfo", {}).get("played")
            live_status = match_data.get("status", {}).get("name", "Upcoming")

        yield _sse("meta", {
            "home_team": match_data.get("teams", {}).get("home", {}).get("name", "Home"),
            "away_team": match_data.get("teams", {}).get("away", {}).get("name", "Away"),
            "status": live_status,
            "match_time": str(match_time) if match_time else "0",
            "score_home": match_data.get("result", {}).get("home", 0),
            "score_away": match_data.get("result", {}).get("away", 0)
        })

        if timeline_data:
            events = timeline_data.get("events", [])
            ignored = ["possession", "matchsituation", "ballcoordinates"]
            comments = [{"time": ev.get("time", ""), "team": ev.get("team"), "type": ev.get("type"), "name": ev.get("name", "")} for ev in reversed(events) if ev.get("type") not in ignored]
            yield _sse("comments", comments[:20])

        # 2. Concurrently fetch deep stats
        with ThreadPoolExecutor(max_workers=10) as pool:
            f_table       = pool.submit(_fetch_sh, "stats_season_tables", str(season_id)) if season_id else None
            f_team_stats  = pool.submit(_fetch_sh, "stats_season_uniqueteamstats", str(season_id)) if season_id else None
            f_top_h       = pool.submit(_fetch_sh, "stats_season_topgoals", f"{season_id}/{home_uid}") if season_id and home_uid else None
            f_top_a       = pool.submit(_fetch_sh, "stats_season_topgoals", f"{season_id}/{away_uid}") if season_id and away_uid else None

            # Parse Table Standings
            table_data = f_table.result() if f_table else None
            if table_data:
                rows = []
                # Find the 'Total' table
                tables = table_data.get("tables", [])
                for t in tables:
                    for row in t.get("tablerows", []):
                        rows.append({
                            "pos": row.get("pos"),
                            "team": row.get("team", {}).get("name"),
                            "played": row.get("total"),
                            "gd": row.get("goalDiffTotal"),
                            "pts": row.get("pointsTotal"),
                            "is_target": str(row.get("team", {}).get("uid")) in [str(home_uid), str(away_uid)]
                        })
                    break # Take the first main table
                yield _sse("standings", sorted(rows, key=lambda x: x["pos"]))

            # Parse Top Scorers
            top_h_data = f_top_h.result() if f_top_h else {}
            top_a_data = f_top_a.result() if f_top_a else {}
            
            def extract_scorers(data):
                return [{"name": p.get("player", {}).get("name", "Unknown").split(",")[0], 
                         "goals": p.get("total", {}).get("goals", 0)} for p in data.get("players", [])[:3]]
            
            yield _sse("top_scorers", {
                "home": extract_scorers(top_h_data),
                "away": extract_scorers(top_a_data)
            })

            # Parse Unique Team Stats
            t_stats = f_team_stats.result() if f_team_stats else None
            if t_stats:
                h_stats = t_stats.get("stats", {}).get("uniqueteams", {}).get(str(home_uid), {})
                a_stats = t_stats.get("stats", {}).get("uniqueteams", {}).get(str(away_uid), {})
                
                yield _sse("team_stats", {
                    "home": {
                        "possession": h_stats.get("ball_possession", {}).get("average", 50),
                        "shots": h_stats.get("goal_attempts", {}).get("average", 0),
                        "corners": h_stats.get("corner_kicks", {}).get("average", 0),
                        "clean_sheets": h_stats.get("clean_sheet", {}).get("total", 0)
                    },
                    "away": {
                        "possession": a_stats.get("ball_possession", {}).get("average", 50),
                        "shots": a_stats.get("goal_attempts", {}).get("average", 0),
                        "corners": a_stats.get("corner_kicks", {}).get("average", 0),
                        "clean_sheets": a_stats.get("clean_sheet", {}).get("total", 0)
                    }
                })

        yield _sse("done", {"status": "complete"})

    return Response(stream_with_context(generate()), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"
    })