import logging
from concurrent.futures import ThreadPoolExecutor
from flask import Blueprint, Response, stream_with_context, request
import requests
import json

bp_deep_analytics = Blueprint("deep_analytics", __name__, url_prefix="/api")

LMT_TOKEN = "exp=1776025306~acl=/*~data=eyJvIjoiaHR0cHM6Ly93d3cuYmV0aWthLmNvbSIsImEiOiI2MDAwNmI1MjM0YzMxY2NmOGIxNGYxNmYyODczZWU3MSIsImFjdCI6Im9yaWdpbmNoZWNrIiwib3NyYyI6Im9yaWdpbiJ9~hmac=016ea9a66a30e7c493628bc5a2beb8e294aeefa76ea7582648f6e40904e395d4"
SH_TOKEN = "exp=1776064004~acl=/*~data=eyJvIjoiaHR0cHM6Ly9zdGF0c2h1Yi5zcG9ydHJhZGFyLmNvbSIsImEiOiJzcG9ydHBlc2EiLCJhY3QiOiJvcmlnaW5jaGVjayIsIm9zcmMiOiJob3N0aGVhZGVyIn0~hmac=1c7b2ef7f250e867db4f35699ca70d55884e705200df665ee15860e7eb4cddd6"

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

        # Extract New Meta Data
        stadium = info.get("stadium", {})
        distance = info.get("distance", "N/A")
        managers = info.get("manager", {})
        win_probs = match_data.get("best", {}).get("match_winner", {}) # Adjust based on actual API location for odds if available

        yield _sse("meta", {
            "home_team": match_data.get("teams", {}).get("home", {}).get("name", "Home"),
            "away_team": match_data.get("teams", {}).get("away", {}).get("name", "Away"),
            "status": match_data.get("status", {}).get("name", "Upcoming"),
            "match_time": str(match_time),
            "score_home": match_data.get("result", {}).get("home", 0),
            "score_away": match_data.get("result", {}).get("away", 0),
            "home_color": home_color,
            "away_color": away_color,
            "stadium": stadium.get("name", "Unknown Stadium"),
            "distance_km": distance,
            "home_manager": managers.get("home", {}).get("name", "TBA"),
            "away_manager": managers.get("away", {}).get("name", "TBA"),
            "competition": info.get("tournament", {}).get("name", ""),
            "stage": match_data.get("roundname", {}).get("name", "")
        })

        # 2. Concurrently fetch deep stats
        with ThreadPoolExecutor(max_workers=10) as pool:
            f_squads      = pool.submit(_fetch_lmt, "match_squads", betradar_id)
            f_timeline    = pool.submit(_fetch_lmt, "match_timelinedelta", betradar_id)
            f_h2h         = pool.submit(_fetch_sh, "stats_team_versusrecent", f"{home_uid}/{away_uid}") if home_uid and away_uid else None
            f_table       = pool.submit(_fetch_sh, "season_dynamictable", str(season_id)) if season_id else None
            f_form        = pool.submit(_fetch_sh, "stats_formtable", str(season_id)) if season_id else None
            f_home_recent = pool.submit(_fetch_sh, "stats_team_lastx", str(home_uid), "/10") if home_uid else None
            f_away_recent = pool.submit(_fetch_sh, "stats_team_lastx", str(away_uid), "/10") if away_uid else None
            f_home_next   = pool.submit(_fetch_sh, "stats_team_fixtures", str(home_uid), "/10") if home_uid else None
            f_away_next   = pool.submit(_fetch_sh, "stats_team_fixtures", str(away_uid), "/10") if away_uid else None
            f_top_h       = pool.submit(_fetch_sh, "stats_season_topgoals", f"{season_id}/{home_uid}") if season_id and home_uid else None
            f_top_a       = pool.submit(_fetch_sh, "stats_season_topgoals", f"{season_id}/{away_uid}") if season_id and away_uid else None

            # --- SQUADS & FORMATIONS ---
            squads_data = f_squads.result()
            if squads_data and "home" in squads_data:
                h_node = squads_data.get("home", {}).get("startinglineup", squads_data.get("home", {}).get("players", []))
                a_node = squads_data.get("away", {}).get("startinglineup", squads_data.get("away", {}).get("players", []))
                h_form = h_node.get("formation", "") if isinstance(h_node, dict) else ""
                a_form = a_node.get("formation", "") if isinstance(a_node, dict) else ""
                h_players = h_node.get("players", []) if isinstance(h_node, dict) else (h_node if isinstance(h_node, list) else [])
                a_players = a_node.get("players", []) if isinstance(a_node, dict) else (a_node if isinstance(a_node, list) else [])

                yield _sse("lineups", {
                    "home": {"formation": h_form, "players": [{"name": p.get("playername", p.get("name", "")).split(",")[0].strip(), "num": p.get("shirtnumber", ""), "pos": p.get("matchpos", "M")} for p in h_players]},
                    "away": {"formation": a_form, "players": [{"name": p.get("playername", p.get("name", "")).split(",")[0].strip(), "num": p.get("shirtnumber", ""), "pos": p.get("matchpos", "M")} for p in a_players]}
                })
            else:
                yield _sse("lineups", {"fallback": True})

            # --- TIMELINE ---
            timeline_data = f_timeline.result()
            if timeline_data:
                events = timeline_data.get("events", [])
                ignored = ["possession", "matchsituation", "ballcoordinates", "possible_event", "pitch coordinates"]
                comments = [{"time": ev.get("time", ""), "team": ev.get("team"), "type": ev.get("type"), "name": ev.get("name", "")} for ev in reversed(events) if ev.get("type") not in ignored]
                yield _sse("comments", comments[:20])

            # --- TOP SCORERS ---
            top_h_data = f_top_h.result() if f_top_h else {}
            top_a_data = f_top_a.result() if f_top_a else {}
            yield _sse("top_scorers", {
                "home": [{"name": p.get("player", {}).get("name", "Unknown"), "goals": p.get("total", {}).get("goals", 0)} for p in top_h_data.get("players", [])[:3]],
                "away": [{"name": p.get("player", {}).get("name", "Unknown"), "goals": p.get("total", {}).get("goals", 0)} for p in top_a_data.get("players", [])[:3]]
            })

            # --- HEAD 2 HEAD ---
            h2h_data = f_h2h.result() if f_h2h else None
            if h2h_data:
                parsed_h2h = []
                for m in h2h_data.get("matches", [])[:5]:
                    parsed_h2h.append({
                        "date": m.get("_dt", {}).get("date", ""),
                        "home": m.get("teams", {}).get("home", {}).get("name", ""),
                        "away": m.get("teams", {}).get("away", {}).get("name", ""),
                        "score_home": m.get("result", {}).get("home", 0),
                        "score_away": m.get("result", {}).get("away", 0)
                    })
                yield _sse("h2h", parsed_h2h)

            # --- RECENT & UPCOMING ---
            def _parse_recent(data):
                if not data: return []
                parsed = []
                for m in data.get("matches", [])[:5]:
                    parsed.append({
                        "date": m.get("_dt", {}).get("date", ""),
                        "time": m.get("_dt", {}).get("time", ""),
                        "home": m.get("teams", {}).get("home", {}).get("name", ""),
                        "away": m.get("teams", {}).get("away", {}).get("name", ""),
                        "score_home": m.get("result", {}).get("home", 0),
                        "score_away": m.get("result", {}).get("away", 0)
                    })
                return parsed

            yield _sse("recent", {
                "home": _parse_recent(f_home_recent.result() if f_home_recent else None),
                "away": _parse_recent(f_away_recent.result() if f_away_recent else None)
            })
            yield _sse("upcoming", {
                "home": _parse_recent(f_home_next.result() if f_home_next else None)[:3],
                "away": _parse_recent(f_away_next.result() if f_away_next else None)[:3]
            })

            # --- STANDINGS & FORM ---
            table_data = f_table.result() if f_table else None
            form_data = f_form.result() if f_form else None

            if table_data:
                rows = []
                tables = table_data.get("tables", []) or table_data.get("season", {}).get("tables", [])
                for t in tables:
                    if t.get("name") == "Total" or len(t.get("tablerows", [])) > 0:
                        for tr in t.get("tablerows", []):
                            rows.append({
                                "pos": tr.get("pos"),
                                "team": tr.get("team", {}).get("name"),
                                "played": tr.get("played", {}).get("total", tr.get("total", 0)),
                                "gd": tr.get("goaldifference", {}).get("total", tr.get("goalDiffTotal", 0)),
                                "pts": tr.get("points", {}).get("total", tr.get("pointsTotal", 0)),
                                "is_target": str(tr.get("team", {}).get("uid")) in [str(home_uid), str(away_uid)]
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