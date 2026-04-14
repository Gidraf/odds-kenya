"""
deep_analytics.py — Full Sportradar SSE Stream
Emits: status, meta, stadium, lineups, comments, h2h, form,
       standings, top_scorers, team_stats, recent, upcoming, done
"""
import os, json, logging
from concurrent.futures import ThreadPoolExecutor
from flask import Blueprint, Response, stream_with_context
import requests

log = logging.getLogger("deep_analytics")

LMT_TOKEN = os.environ.get("LMT_TOKEN",
    "exp=1776025306~acl=/*~data=eyJvIjoiaHR0cHM6Ly93d3cuYmV0aWthLmNvbSIsImEiOiI2MDAwNmI1MjM0YzMxY2NmOGIxNGYxNmYyODczZWU3MSIsImFjdCI6Im9yaWdpbmNoZWNrIiwib3NyYyI6Im9yaWdpbiJ9~hmac=016ea9a66a30e7c493628bc5a2beb8e294aeefa76ea7582648f6e40904e395d4")
SH_TOKEN = os.environ.get("SH_TOKEN",
    "exp=1776064004~acl=/*~data=eyJvIjoiaHR0cHM6Ly9zdGF0c2h1Yi5zcG9ydHJhZGFyLmNvbSIsImEiOiJzcG9ydHBlc2EiLCJhY3QiOiJvcmlnaW5jaGVjayIsIm9zcmMiOiJob3N0aGVhZGVyIn0~hmac=1c7b2ef7f250e867db4f35699ca70d55884e705200df665ee15860e7eb4cddd6")

_LMT_H = {"origin":"https://www.betika.com","referer":"https://www.betika.com/","user-agent":"Mozilla/5.0"}
_SH_H  = {"origin":"https://statshub.sportradar.com","referer":"https://statshub.sportradar.com/","user-agent":"Mozilla/5.0"}

bp_deep_analytics = Blueprint("deep_analytics", __name__, url_prefix="/api")

def _sse(event, data):
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"

def _lmt(endpoint, item_id, extra=""):
    url = f"https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/{endpoint}/{item_id}{extra}?T={LMT_TOKEN}"
    try:
        r = requests.get(url, headers=_LMT_H, timeout=8)
        if r.ok: return r.json().get("doc",[{}])[0].get("data",{}) or {}
    except Exception as e: log.warning(f"LMT {endpoint}/{item_id}: {e}")
    return {}

def _sh(endpoint, item_id, extra=""):
    url = f"https://sh.fn.sportradar.com/sportpesa/en/Etc:UTC/gismo/{endpoint}/{item_id}{extra}?T={SH_TOKEN}"
    try:
        r = requests.get(url, headers=_SH_H, timeout=8)
        if r.ok: return r.json().get("doc",[{}])[0].get("data",{}) or {}
    except Exception as e: log.warning(f"SH {endpoint}/{item_id}: {e}")
    return {}

def _clean(raw):
    if not raw: return ""
    if "," in raw:
        p = raw.split(",",1); return f"{p[1].strip()} {p[0].strip()}"
    return raw.strip()

def _parse_player(p):
    raw = p.get("playername", p.get("name",""))
    return {"name": raw.split(",")[0].strip() if "," in raw else raw.strip(),
            "num": p.get("shirtnumber",""), "pos": p.get("matchpos","M"), "id": p.get("_id")}

def _parse_recent(data):
    if not data: return []
    return [{"date": m.get("_dt",{}).get("date",""), "time": m.get("_dt",{}).get("time",""),
             "home": m.get("teams",{}).get("home",{}).get("name",""),
             "away": m.get("teams",{}).get("away",{}).get("name",""),
             "score_home": m.get("result",{}).get("home",0),
             "score_away": m.get("result",{}).get("away",0)}
            for m in (data.get("matches") or [])[:5]]

def _parse_upcoming(data):
    if not data: return []
    return [{"date": m.get("_dt",{}).get("date",""), "time": m.get("_dt",{}).get("time",""),
             "home": m.get("teams",{}).get("home",{}).get("name",""),
             "away": m.get("teams",{}).get("away",{}).get("name","")}
            for m in (data.get("matches") or [])[:3]]

def _detect_comp_type(info):
    t = info.get("tournament",{})
    name = t.get("name","").lower()
    if t.get("friendly"): return "friendly"
    if str(t.get("seasontype",""))=="26" or any(k in name for k in ["cup","champions","europa","coupe"]): return "cup"
    return "league"

def _format_stadium(s):
    if not s: return {}
    coords = s.get("googlecoords",""); lat=lng=None
    if coords:
        try: lat,lng=[float(x.strip()) for x in coords.split(",")]
        except: pass
    return {"id":s.get("_id",""),"name":s.get("name",""),"city":s.get("city",""),
            "country":s.get("country",""),"capacity":s.get("capacity",""),
            "built":s.get("constryear",""),"pitch":s.get("pitchsize",{}),
            "coordinates":{"lat":lat,"lng":lng} if lat else None}


@bp_deep_analytics.route("/odds/match/<betradar_id>/deep_analytics/stream")
def stream_deep_analytics(betradar_id):
    def generate():
        yield _sse("status",{"step":"init","message":"Connecting to Sportradar..."})

        # ── Core match info ────────────────────────────────────────────────────
        info = _lmt("match_info", betradar_id) or _sh("match_info_statshub", betradar_id)
        if not info:
            yield _sse("error",{"message":"Could not load match info."}); return

        md    = info.get("match",{})
        teams = md.get("teams",{})
        h_uid = str(teams.get("home",{}).get("uid",""))
        a_uid = str(teams.get("away",{}).get("uid",""))
        s_id  = str(md.get("_seasonid",""))

        jerseys    = info.get("jerseys",{})
        home_color = f"#{jerseys.get('home',{}).get('player',{}).get('base','ea0000')}"
        away_color = f"#{jerseys.get('away',{}).get('player',{}).get('base','0099ff')}"

        mt = (md.get("timeinfo") or {}).get("played") or md.get("p") or (md.get("status") or {}).get("shortName","")

        comp_type = _detect_comp_type(info)
        tourn     = info.get("tournament",{})
        mgr_raw   = info.get("manager",{})
        stadium   = _format_stadium(info.get("stadium",{}))

        yield _sse("meta",{
            "home_team":        teams.get("home",{}).get("name","Home"),
            "away_team":        teams.get("away",{}).get("name","Away"),
            "home_abbr":        teams.get("home",{}).get("abbr",""),
            "away_abbr":        teams.get("away",{}).get("abbr",""),
            "home_uid":         h_uid, "away_uid": a_uid, "season_id": s_id,
            "status":           (md.get("status") or {}).get("name","Upcoming"),
            "status_short":     (md.get("status") or {}).get("shortName","NS"),
            "match_time":       str(mt) if mt else "",
            "score_home":       (md.get("result") or {}).get("home"),
            "score_away":       (md.get("result") or {}).get("away"),
            "home_color":       home_color, "away_color": away_color,
            "competition":      tourn.get("name",""),
            "competition_type": comp_type,
            "is_league":        comp_type == "league",
            "is_cup":           comp_type == "cup",
            "round":            md.get("round"),
            "round_name":       str((md.get("roundname") or {}).get("name","")),
            "date":             (md.get("_dt") or {}).get("date",""),
            "time":             (md.get("_dt") or {}).get("time",""),
            "kickoff_uts":      (md.get("_dt") or {}).get("uts"),
            "venue":            stadium.get("name",""),
            "distance_km":      md.get("distance"),
            "home_manager":     _clean((mgr_raw.get("home") or {}).get("name","")),
            "away_manager":     _clean((mgr_raw.get("away") or {}).get("name","")),
            "referee":          _clean((info.get("referee") or {}).get("name","")),
            "season_name":      (info.get("season") or {}).get("name",""),
        })

        yield _sse("stadium", stadium)
        yield _sse("status",{"step":"fetching","message":"Loading match stats..."})

        # ── Parallel fetches ──────────────────────────────────────────────────
        with ThreadPoolExecutor(max_workers=14) as pool:
            f_squads      = pool.submit(_lmt,"match_squads",betradar_id)
            f_timeline    = pool.submit(_lmt,"match_timelinedelta",betradar_id)
            f_h2h_simple  = pool.submit(_sh, "stats_match_head2head",betradar_id)
            f_h2h_full    = pool.submit(_lmt,"stats_team_versusrecent",f"{h_uid}/{a_uid}") if h_uid and a_uid else None
            f_table       = pool.submit(_sh, "season_dynamictable",s_id) if s_id else None
            f_form        = pool.submit(_sh, "stats_formtable",s_id) if s_id else None
            f_team_stats  = pool.submit(_sh, "stats_season_uniqueteamstats",s_id) if s_id else None
            f_home_recent = pool.submit(_sh, "stats_team_lastx",h_uid,"/10") if h_uid else None
            f_away_recent = pool.submit(_sh, "stats_team_lastx",a_uid,"/10") if a_uid else None
            f_home_next   = pool.submit(_sh, "stats_team_fixtures",h_uid,"/10") if h_uid else None
            f_away_next   = pool.submit(_sh, "stats_team_fixtures",a_uid,"/10") if a_uid else None
            f_top_h       = pool.submit(_lmt,"stats_season_topgoals",f"{s_id}/{h_uid}") if s_id and h_uid else None
            f_top_a       = pool.submit(_lmt,"stats_season_topgoals",f"{s_id}/{a_uid}") if s_id and a_uid else None

            # SQUADS ─────────────────────────────────────────────────────────
            sq = f_squads.result()
            if sq and ("home" in sq or "away" in sq):
                def _squad(side):
                    node  = sq.get(side,{})
                    lu    = node.get("startinglineup") or node.get("players") or []
                    form  = lu.get("formation","") if isinstance(lu,dict) else ""
                    pls   = lu.get("players",[]) if isinstance(lu,dict) else (lu if isinstance(lu,list) else [])
                    coach = node.get("coach") or {}
                    return {"formation":form,"players":[_parse_player(p) for p in pls],
                            "coach":{"name":_clean(coach.get("name","")),"id":coach.get("_id")}}
                yield _sse("lineups",{"home":_squad("home"),"away":_squad("away")})
            else:
                yield _sse("lineups",{"fallback":True})

            # TIMELINE ───────────────────────────────────────────────────────
            tl = f_timeline.result()
            if tl:
                IGNORED = {"possession","matchsituation","ballcoordinates","possible_event","pitch coordinates"}
                events = [{"time":ev.get("time",""),"team":ev.get("team"),
                           "type":ev.get("type"),"name":ev.get("name","")}
                          for ev in reversed(tl.get("events",[]))
                          if ev.get("type") not in IGNORED]
                yield _sse("comments", events[:25])

            # H2H SIMPLE ─────────────────────────────────────────────────────
            h2h_s = f_h2h_simple.result()
            if h2h_s:
                yield _sse("h2h",[{
                    "date":(m.get("_dt") or {}).get("date",""),
                    "home":(m.get("teams") or {}).get("home",{}).get("name",""),
                    "away":(m.get("teams") or {}).get("away",{}).get("name",""),
                    "score_home":(m.get("result") or {}).get("home",0),
                    "score_away":(m.get("result") or {}).get("away",0)}
                    for m in (h2h_s.get("matches") or [])[:5]])

            # H2H FULL (versusrecent — 30 matches, goal timing) ───────────────
            h2h_f = f_h2h_full.result() if f_h2h_full else None
            if h2h_f:
                import re
                def _goal_mins(comment):
                    if not comment: return []
                    out=[]
                    for m in re.finditer(r'\((\d+)(?:\+(\d+))?\.\)',comment):
                        t=int(m.group(1))+(int(m.group(2)) if m.group(2) else 0)
                        if t<=130: out.append(t)
                    return out

                raw_m = h2h_f.get("matches",[])
                parsed=[]; hw=dr=aw=0; total_g=btts=ov25=0
                for m in raw_m[:30]:
                    res=m.get("result",{}); sh=res.get("home") or 0; sa=res.get("away") or 0
                    winner=res.get("winner")
                    h_uid_m=str((m.get("teams") or {}).get("home",{}).get("uid",""))
                    is_hh = h_uid_m == h_uid
                    if winner=="home":
                        if is_hh: hw+=1
                        else: aw+=1
                    elif winner=="away":
                        if is_hh: aw+=1
                        else: hw+=1
                    elif sh+sa>0: dr+=1
                    g=sh+sa; total_g+=g
                    if sh>0 and sa>0: btts+=1
                    if g>2: ov25+=1
                    teams_m=m.get("teams",{})
                    parsed.append({
                        "id":m.get("_id"),"date":(m.get("time") or {}).get("date",""),
                        "home":teams_m.get("home",{}).get("name",""),
                        "away":teams_m.get("away",{}).get("name",""),
                        "score_home":sh,"score_away":sa,"winner":winner,
                        "comment":m.get("comment",""),"attendance":m.get("attendance"),
                        "goal_minutes":_goal_mins(m.get("comment",""))})
                n=len(parsed)

                # Goal timing buckets
                all_mins=[mi for m in raw_m[:20] for mi in _goal_mins(m.get("comment",""))]
                b={"0-15":0,"16-30":0,"31-45":0,"46-60":0,"61-75":0,"76-90":0,"90+":0}
                for mi in all_mins:
                    if   mi<=15: b["0-15"]+=1
                    elif mi<=30: b["16-30"]+=1
                    elif mi<=45: b["31-45"]+=1
                    elif mi<=60: b["46-60"]+=1
                    elif mi<=75: b["61-75"]+=1
                    elif mi<=90: b["76-90"]+=1
                    else: b["90+"]+=1

                yield _sse("versus_history",{
                    "matches":parsed,
                    "summary":{"total":n,"home_wins":hw,"draws":dr,"away_wins":aw,
                               "avg_goals_pg":round(total_g/n,2) if n else 0,
                               "btts_pct":round(btts/n*100,1) if n else 0,
                               "over_2_5_pct":round(ov25/n*100,1) if n else 0},
                    "goal_timing":{"buckets":b,
                                   "most_dangerous":max(b,key=b.get) if all_mins else None,
                                   "avg_minute":round(sum(all_mins)/len(all_mins),1) if all_mins else None,
                                   "first_half_pct":round(sum(1 for m in all_mins if m<=45)/max(len(all_mins),1)*100,1)}
                })

                # Managers from versusrecent currentmanagers block
                cm = h2h_f.get("currentmanagers",{})
                def _mgr(uid):
                    lst=cm.get(uid) or cm.get(str(uid)) or []
                    if not lst: return {}
                    m=lst[0]
                    ms=m.get("membersince") or {}
                    return {"id":m.get("_id"),"name":_clean(m.get("name","")),"nationality":(m.get("nationality") or {}).get("name",""),
                            "membersince":ms.get("date","") if isinstance(ms,dict) else ""}
                yield _sse("managers",{"home":_mgr(h_uid),"away":_mgr(a_uid)})

                # Better stadium from next match block
                next_m = h2h_f.get("next",{})
                if next_m.get("stadium"):
                    yield _sse("stadium",_format_stadium(next_m["stadium"]))
                if next_m.get("matchdifficultyrating"):
                    yield _sse("difficulty",next_m["matchdifficultyrating"])

            # TOP SCORERS ─────────────────────────────────────────────────────
            def _scorers(data):
                if not data: return []
                out=[]
                for e in (data.get("players") or [])[:5]:
                    pl=e.get("player",{}); g=e.get("total",{}).get("goals",0)
                    if g: out.append({"id":pl.get("_id"),"name":_clean(pl.get("name","")),"goals":g,
                                      "matches":e.get("total",{}).get("matches",0),
                                      "nationality":(pl.get("nationality") or {}).get("name",""),
                                      "position":(pl.get("position") or {}).get("shortname",""),
                                      "jersey":pl.get("jerseynumber",""),
                                      "home_goals":(e.get("home") or {}).get("goals",0),
                                      "away_goals":(e.get("away") or {}).get("goals",0),
                                      "first_half":(e.get("firsthalf") or {}).get("goals",0),
                                      "second_half":(e.get("secondhalf") or {}).get("goals",0)})
                return out
            h_sc = f_top_h.result() if f_top_h else {}
            a_sc = f_top_a.result() if f_top_a else {}
            if h_sc or a_sc:
                yield _sse("top_scorers",{"home":_scorers(h_sc),"away":_scorers(a_sc)})

            # TEAM STATS ──────────────────────────────────────────────────────
            ts = f_team_stats.result() if f_team_stats else None
            if ts:
                def _ts(uid):
                    d=(ts.get("stats") or {}).get("uniqueteams",{}).get(str(uid),{})
                    return {"possession":(d.get("ball_possession") or {}).get("average",50),
                            "shots":     (d.get("goal_attempts") or {}).get("average",0),
                            "corners":   (d.get("corner_kicks") or {}).get("average",0),
                            "clean_sheets":(d.get("clean_sheet") or {}).get("total",0),
                            "goals_scored":(d.get("goals_scored") or {}).get("average",0),
                            "goals_conceded":(d.get("goals_conceded") or {}).get("average",0)}
                yield _sse("team_stats",{"home":_ts(h_uid),"away":_ts(a_uid)})

            # RECENT & UPCOMING ───────────────────────────────────────────────
            yield _sse("recent",{
                "home":_parse_recent(f_home_recent.result() if f_home_recent else None),
                "away":_parse_recent(f_away_recent.result() if f_away_recent else None)})
            yield _sse("upcoming",{
                "home":_parse_upcoming(f_home_next.result() if f_home_next else None),
                "away":_parse_upcoming(f_away_next.result() if f_away_next else None)})

            # STANDINGS ───────────────────────────────────────────────────────
            table = f_table.result() if f_table else None
            if table:
                rows=[]
                tables=(table.get("tables") or (table.get("season") or {}).get("tables") or [])
                for t in (tables or []):
                    if t.get("name")=="Total" or t.get("tablerows"):
                        for tr in t.get("tablerows",[]):
                            uid=str((tr.get("team") or {}).get("uid",""))
                            rows.append({
                                "pos":   tr.get("pos"),
                                "team":  (tr.get("team") or {}).get("name"),
                                "team_uid": uid,
                                "played":(tr.get("played") or {}).get("total",0),
                                "won":   (tr.get("won") or {}).get("total",0),
                                "drawn": (tr.get("drawn") or {}).get("total",0),
                                "lost":  (tr.get("lost") or {}).get("total",0),
                                "gf":    (tr.get("goalsfor") or {}).get("total",0),
                                "ga":    (tr.get("goalsagainst") or {}).get("total",0),
                                "gd":    (tr.get("goaldifference") or {}).get("total",0),
                                "pts":   (tr.get("points") or {}).get("total",0),
                                "is_target": uid in [h_uid,a_uid]})
                        break
                yield _sse("standings",sorted(rows,key=lambda x:x.get("pos") or 99))

            # FORM ────────────────────────────────────────────────────────────
            form_d = f_form.result() if f_form else None
            if form_d:
                fo={"home":[],"away":[]}
                for t in (form_d.get("teams") or []):
                    uid=str((t.get("team") or {}).get("uid",""))
                    fl=[f.get("value") for f in (t.get("form") or {}).get("total",[])]
                    if uid==h_uid: fo["home"]=fl
                    if uid==a_uid: fo["away"]=fl
                yield _sse("form",fo)

        yield _sse("done",{"status":"complete"})

    return Response(stream_with_context(generate()),mimetype="text/event-stream",
        headers={"Cache-Control":"no-cache","Connection":"keep-alive","X-Accel-Buffering":"no"})