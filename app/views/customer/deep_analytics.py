"""
deep_analytics.py — Full Sportradar SSE Stream via Playwright.

Data is collected by navigating statshub.sportradar.com with a real browser
(playwright_scraper.py), intercepting every gismo API response.
No tokens, no requests — the browser handles auth automatically.

SSE events emitted:
  status, meta, stadium, lineups, comments, shot_map, h2h, versus_history,
  goal_timing, managers, top_scorers, team_stats, recent, upcoming,
  standings, form, pressure, done
"""
import json
import logging
from threading import Thread, Event

from flask import Blueprint, Response, stream_with_context

from app.utils.playwright_scraper import collect_match_data, get as _get

log = logging.getLogger("deep_analytics")

bp_deep_analytics = Blueprint("deep_analytics", __name__, url_prefix="/api")


def _sse(event, data):
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _clean(raw):
    if not raw:
        return ""
    if "," in raw:
        p = raw.split(",", 1)
        return f"{p[1].strip()} {p[0].strip()}"
    return raw.strip()


def _parse_player(p):
    raw = p.get("playername", p.get("name", ""))
    return {
        "name": raw.split(",")[0].strip() if "," in raw else raw.strip(),
        "num":  p.get("shirtnumber", ""),
        "pos":  p.get("matchpos", "M"),
        "id":   p.get("_id"),
    }


def _format_stadium(s):
    if not s:
        return {}
    coords = s.get("googlecoords", "")
    lat = lng = None
    if coords:
        try:
            lat, lng = [float(x.strip()) for x in coords.split(",")]
        except Exception:
            pass
    return {
        "id":       s.get("_id", ""),
        "name":     s.get("name", ""),
        "city":     s.get("city", ""),
        "country":  s.get("country", ""),
        "capacity": s.get("capacity", ""),
        "built":    s.get("constryear", ""),
        "pitch":    s.get("pitchsize", {}),
        "coordinates": {"lat": lat, "lng": lng} if lat else None,
    }


def _detect_comp_type(info):
    t    = info.get("tournament", {})
    name = t.get("name", "").lower()
    if t.get("friendly"):
        return "friendly"
    if str(t.get("seasontype", "")) == "26" or any(
        k in name for k in ["cup", "champions", "europa", "coupe"]
    ):
        return "cup"
    return "league"


def _time_from(m):
    """Extract date/time from a match dict — handles _dt and time fields."""
    dt = m.get("time") or m.get("_dt") or {}
    if not isinstance(dt, dict):
        return "", ""
    return dt.get("date", ""), dt.get("time", "")


def _parse_recent(data):
    if not data:
        return []
    out = []
    for m in (data.get("matches") or [])[:5]:
        d, t = _time_from(m)
        out.append({
            "date":       d, "time": t,
            "home":       (m.get("teams", {}).get("home") or {}).get("name", ""),
            "away":       (m.get("teams", {}).get("away") or {}).get("name", ""),
            "score_home": (m.get("result") or {}).get("home", 0),
            "score_away": (m.get("result") or {}).get("away", 0),
        })
    return out


def _parse_upcoming(data):
    if not data:
        return []
    out = []
    for m in (data.get("matches") or [])[:3]:
        d, t = _time_from(m)
        out.append({
            "date": d, "time": t,
            "home": (m.get("teams", {}).get("home") or {}).get("name", ""),
            "away": (m.get("teams", {}).get("away") or {}).get("name", ""),
        })
    return out


@bp_deep_analytics.route("/odds/match/<betradar_id>/deep_analytics/stream")
def stream_deep_analytics(betradar_id):

    def generate():
        yield _sse("status", {"step": "init", "message": "Launching browser..."})

        # Run Playwright in a background thread so we can ping while waiting
        collected: dict = {}
        scrape_error: list = []
        done_event = Event()

        def scrape():
            try:
                result = collect_match_data(betradar_id)
                collected.update(result)
            except Exception as e:
                scrape_error.append(str(e))
                log.error(f"Scraper error for {betradar_id}: {e}")
            finally:
                done_event.set()

        Thread(target=scrape, daemon=True).start()

        # Yield status pings every 3 s while Playwright navigates tabs (~15-30 s)
        tab_labels = ["overview", "report", "statistics", "h2h", "table"]
        ping = 0
        while not done_event.wait(timeout=3):
            yield _sse("status", {
                "step":    "scraping",
                "message": f"Scraping {tab_labels[min(ping, len(tab_labels)-1)]} tab...",
            })
            ping += 1

        if scrape_error or not collected:
            yield _sse("error", {"message": scrape_error[0] if scrape_error else "Scraper returned no data"})
            return

        yield _sse("status", {"step": "parsing", "message": "Processing match data..."})

        # ── Core match info ───────────────────────────────────────────────────
        info = _get(
            collected,
            f"match_info_statshub/{betradar_id}",
            f"match_info/{betradar_id}",
        )
        if not info:
            yield _sse("error", {"message": "Could not find match info in scraped data."})
            return

        md     = info.get("match", {})
        teams  = md.get("teams", {})
        h_uid  = str((teams.get("home") or {}).get("uid", ""))
        a_uid  = str((teams.get("away") or {}).get("uid", ""))
        s_id   = str(md.get("_seasonid", ""))

        jerseys    = info.get("jerseys", {})
        home_color = f"#{(jerseys.get('home') or {}).get('player', {}).get('base', 'ea0000')}"
        away_color = f"#{(jerseys.get('away') or {}).get('player', {}).get('base', '0099ff')}"

        mt        = ((md.get("timeinfo") or {}).get("played") or md.get("p")
                     or (md.get("status") or {}).get("shortName", ""))
        comp_type = _detect_comp_type(info)
        tourn     = info.get("tournament", {})
        stadium   = _format_stadium(info.get("stadium", {}))
        mgr_raw   = info.get("manager", {})

        yield _sse("meta", {
            "home_team":        (teams.get("home") or {}).get("name", "Home"),
            "away_team":        (teams.get("away") or {}).get("name", "Away"),
            "home_abbr":        (teams.get("home") or {}).get("abbr", ""),
            "away_abbr":        (teams.get("away") or {}).get("abbr", ""),
            "home_uid":         h_uid,
            "away_uid":         a_uid,
            "season_id":        s_id,
            "status":           (md.get("status") or {}).get("name", "Upcoming"),
            "status_short":     (md.get("status") or {}).get("shortName", "NS"),
            "match_time":       str(mt) if mt else "",
            "score_home":       (md.get("result") or {}).get("home"),
            "score_away":       (md.get("result") or {}).get("away"),
            "home_color":       home_color,
            "away_color":       away_color,
            "competition":      tourn.get("name", ""),
            "competition_type": comp_type,
            "is_league":        comp_type == "league",
            "is_cup":           comp_type == "cup",
            "round":            md.get("round"),
            "round_name":       str((md.get("roundname") or {}).get("name", "")),
            "date":             (md.get("_dt") or {}).get("date", ""),
            "time":             (md.get("_dt") or {}).get("time", ""),
            "kickoff_uts":      (md.get("_dt") or {}).get("uts"),
            "venue":            stadium.get("name", ""),
            "home_manager":     _clean(((mgr_raw or {}).get("home") or {}).get("name", "")),
            "away_manager":     _clean(((mgr_raw or {}).get("away") or {}).get("name", "")),
            "referee":          _clean((info.get("referee") or {}).get("name", "")),
            "season_name":      (info.get("season") or {}).get("name", ""),
        })

        yield _sse("stadium", stadium)
        yield _sse("status", {"step": "emitting", "message": "Streaming analytics..."})

        # ── SQUADS ────────────────────────────────────────────────────────────
        sq = _get(collected, f"match_squads/{betradar_id}")
        if sq and ("home" in sq or "away" in sq):
            def _squad(side):
                node  = sq.get(side, {})
                lu    = node.get("startinglineup") or node.get("players") or []
                form  = lu.get("formation", "") if isinstance(lu, dict) else ""
                pls   = lu.get("players", []) if isinstance(lu, dict) else (lu if isinstance(lu, list) else [])
                coach = node.get("coach") or {}
                return {
                    "formation": form,
                    "players":   [_parse_player(p) for p in pls],
                    "coach":     {"name": _clean(coach.get("name", "")), "id": coach.get("_id")},
                }
            yield _sse("lineups", {"home": _squad("home"), "away": _squad("away")})
        else:
            yield _sse("lineups", {"fallback": True})

        # ── TIMELINE ──────────────────────────────────────────────────────────
        tl = _get(
            collected,
            f"match_timeline/{betradar_id}",
            f"match_timelinedelta/{betradar_id}",
        )
        if tl:
            IGNORED = {
                "possession", "matchsituation", "ballcoordinates", "possible_event",
                "players_warming_up", "players_on_pitch", "match_about_to_start",
                "gameon", "backfrominjury", "injurytimeshoot", "throwin", "goal_kick",
                "periodstart", "periodscore",
            }
            SIGNIFICANT = {
                "goal", "card", "corner", "freekick", "shotontarget",
                "shotofftarget", "shotblocked", "offside", "injury", "match_started",
            }
            events = []
            for ev in (tl.get("events") or []):
                t = ev.get("type")
                if t in IGNORED:
                    continue
                entry = {
                    "time":    ev.get("time"),
                    "seconds": ev.get("seconds"),
                    "team":    ev.get("team"),
                    "type":    t,
                    "name":    ev.get("name", ""),
                    "x":       ev.get("X"),
                    "y":       ev.get("Y"),
                }
                if t == "goal":
                    entry["owngoal"]  = ev.get("owngoal", False)
                    entry["penalty"]  = ev.get("penalty", False)
                    entry["result"]   = ev.get("result")
                if t == "card":
                    entry["card"] = ev.get("card")
                events.append(entry)

            significant = [e for e in events if e["type"] in SIGNIFICANT]
            yield _sse("comments", list(reversed(significant))[:30])

            all_coords = [e for e in events if e.get("x") is not None]
            if all_coords:
                yield _sse("shot_map", all_coords)

        # ── H2H SIMPLE ────────────────────────────────────────────────────────
        h2h_s       = _get(collected, f"stats_match_head2head/{betradar_id}")
        h2h_matches = h2h_s.get("matches") or []
        if h2h_matches:
            yield _sse("h2h", [{
                "date":       (m.get("_dt") or {}).get("date", ""),
                "home":       (m.get("teams") or {}).get("home", {}).get("name", ""),
                "away":       (m.get("teams") or {}).get("away", {}).get("name", ""),
                "score_home": (m.get("result") or {}).get("home", 0),
                "score_away": (m.get("result") or {}).get("away", 0),
            } for m in h2h_matches[:5]])
        else:
            yield _sse("h2h", [])

        # ── H2H FULL (versusrecent) ───────────────────────────────────────────
        h2h_f = _get(collected, f"stats_team_versusrecent/{h_uid}/{a_uid}") if h_uid and a_uid else {}
        if h2h_f:
            import re

            def _goal_mins(comment):
                if not comment:
                    return []
                out = []
                for m in re.finditer(r'\((\d+)(?:\+(\d+))?\.\)', comment):
                    t = int(m.group(1)) + (int(m.group(2)) if m.group(2) else 0)
                    if t <= 130:
                        out.append(t)
                return out

            raw_m  = h2h_f.get("matches", [])
            parsed = []
            hw = dr = aw = total_g = btts = ov25 = 0

            for m in raw_m[:30]:
                res     = m.get("result", {})
                sh      = res.get("home") or 0
                sa      = res.get("away") or 0
                winner  = res.get("winner")
                h_uid_m = str((m.get("teams") or {}).get("home", {}).get("uid", ""))
                is_hh   = h_uid_m == h_uid
                if winner == "home":
                    hw += 1 if is_hh else 0
                    aw += 0 if is_hh else 1
                elif winner == "away":
                    aw += 1 if is_hh else 0
                    hw += 0 if is_hh else 1
                elif sh + sa > 0:
                    dr += 1
                g = sh + sa
                total_g += g
                if sh > 0 and sa > 0:
                    btts += 1
                if g > 2:
                    ov25 += 1
                teams_m = m.get("teams", {})
                parsed.append({
                    "id":           m.get("_id"),
                    "date":         (m.get("time") or {}).get("date", ""),
                    "home":         teams_m.get("home", {}).get("name", ""),
                    "away":         teams_m.get("away", {}).get("name", ""),
                    "score_home":   sh,
                    "score_away":   sa,
                    "winner":       winner,
                    "comment":      m.get("comment", ""),
                    "attendance":   m.get("attendance"),
                    "goal_minutes": _goal_mins(m.get("comment", "")),
                })

            n = len(parsed)
            all_mins = [mi for m in raw_m[:20] for mi in _goal_mins(m.get("comment", ""))]
            b = {"0-15": 0, "16-30": 0, "31-45": 0, "46-60": 0, "61-75": 0, "76-90": 0, "90+": 0}
            for mi in all_mins:
                if   mi <= 15: b["0-15"]  += 1
                elif mi <= 30: b["16-30"] += 1
                elif mi <= 45: b["31-45"] += 1
                elif mi <= 60: b["46-60"] += 1
                elif mi <= 75: b["61-75"] += 1
                elif mi <= 90: b["76-90"] += 1
                else:          b["90+"]   += 1

            yield _sse("versus_history", {
                "matches": parsed,
                "summary": {
                    "total":        n,
                    "home_wins":    hw,
                    "draws":        dr,
                    "away_wins":    aw,
                    "avg_goals_pg": round(total_g / n, 2) if n else 0,
                    "btts_pct":     round(btts / n * 100, 1) if n else 0,
                    "over_2_5_pct": round(ov25 / n * 100, 1) if n else 0,
                },
                "goal_timing": {
                    "buckets":        b,
                    "most_dangerous": max(b, key=b.get) if all_mins else None,
                    "avg_minute":     round(sum(all_mins) / len(all_mins), 1) if all_mins else None,
                    "first_half_pct": round(
                        sum(1 for mi in all_mins if mi <= 45) / max(len(all_mins), 1) * 100, 1
                    ),
                },
            })

            cm = h2h_f.get("currentmanagers", {})
            def _mgr(uid):
                lst = cm.get(uid) or cm.get(str(uid)) or []
                if not lst:
                    return {}
                m  = lst[0]
                ms = m.get("membersince") or {}
                return {
                    "id":          m.get("_id"),
                    "name":        _clean(m.get("name", "")),
                    "nationality": (m.get("nationality") or {}).get("name", ""),
                    "membersince": ms.get("date", "") if isinstance(ms, dict) else "",
                }
            yield _sse("managers", {"home": _mgr(h_uid), "away": _mgr(a_uid)})

        # ── TOP SCORERS ───────────────────────────────────────────────────────
        def _scorers(data):
            if not data:
                return []
            out = []
            for e in (data.get("players") or [])[:5]:
                pl = e.get("player", {})
                g  = e.get("total", {}).get("goals", 0)
                if g:
                    out.append({
                        "id":          pl.get("_id"),
                        "name":        _clean(pl.get("name", "")),
                        "goals":       g,
                        "matches":     e.get("total", {}).get("matches", 0),
                        "nationality": (pl.get("nationality") or {}).get("name", ""),
                        "position":    (pl.get("position") or {}).get("shortname", ""),
                        "jersey":      pl.get("jerseynumber", ""),
                        "home_goals":  (e.get("home") or {}).get("goals", 0),
                        "away_goals":  (e.get("away") or {}).get("goals", 0),
                        "first_half":  (e.get("firsthalf") or {}).get("goals", 0),
                        "second_half": (e.get("secondhalf") or {}).get("goals", 0),
                    })
            return out

        h_sc = _get(collected, f"stats_season_topgoals/{s_id}/{h_uid}") if s_id and h_uid else {}
        a_sc = _get(collected, f"stats_season_topgoals/{s_id}/{a_uid}") if s_id and a_uid else {}
        if h_sc or a_sc:
            yield _sse("top_scorers", {"home": _scorers(h_sc), "away": _scorers(a_sc)})

        # ── TEAM STATS ────────────────────────────────────────────────────────
        ts = _get(collected, f"stats_season_uniqueteamstats/{s_id}") if s_id else {}
        if ts:
            def _ts(uid):
                d = (ts.get("stats") or {}).get("uniqueteams", {}).get(str(uid), {})
                return {
                    "possession":     (d.get("ball_possession") or {}).get("average", 50),
                    "shots":          (d.get("goal_attempts") or {}).get("average", 0),
                    "corners":        (d.get("corner_kicks") or {}).get("average", 0),
                    "clean_sheets":   (d.get("clean_sheet") or {}).get("total", 0),
                    "goals_scored":   (d.get("goals_scored") or {}).get("average", 0),
                    "goals_conceded": (d.get("goals_conceded") or {}).get("average", 0),
                }
            yield _sse("team_stats", {"home": _ts(h_uid), "away": _ts(a_uid)})

        # ── RECENT ────────────────────────────────────────────────────────────
        h_recent = _get(
            collected,
            f"stats_team_lastx/{h_uid}/20",
            f"stats_team_lastx/{h_uid}/10",
            f"stats_team_lastx/{h_uid}/5",
        ) if h_uid else {}
        a_recent = _get(
            collected,
            f"stats_team_lastx/{a_uid}/20",
            f"stats_team_lastx/{a_uid}/10",
            f"stats_team_lastx/{a_uid}/5",
        ) if a_uid else {}
        yield _sse("recent", {"home": _parse_recent(h_recent), "away": _parse_recent(a_recent)})

        # ── UPCOMING ──────────────────────────────────────────────────────────
        h_next = _get(
            collected,
            f"stats_team_fixtures/{h_uid}/10",
            f"stats_team_fixtures/{h_uid}/5",
        ) if h_uid else {}
        a_next = _get(
            collected,
            f"stats_team_fixtures/{a_uid}/10",
            f"stats_team_fixtures/{a_uid}/5",
        ) if a_uid else {}
        yield _sse("upcoming", {"home": _parse_upcoming(h_next), "away": _parse_upcoming(a_next)})

        # ── STANDINGS ─────────────────────────────────────────────────────────
        # statshub loads season_dynamictable; also try stats_season_tables as fallback.
        # Data layout differs slightly — handled below.
        raw_table = _get(
            collected,
            f"season_dynamictable/{s_id}",
            f"stats_season_tables/{s_id}/1",
        ) if s_id else {}

        if raw_table:
            # season_dynamictable: { "season": { "tables": [...] } }
            # stats_season_tables: { "tables": [...] }
            tables = (
                raw_table.get("tables")
                or (raw_table.get("season") or {}).get("tables")
                or []
            )
            rows = []
            for t in tables:
                for tr in t.get("tablerows", []):
                    uid   = str((tr.get("team") or {}).get("uid", ""))
                    promo = (tr.get("promotion") or {}).get("name", "")
                    rows.append({
                        "pos":       tr.get("pos"),
                        "team":      (tr.get("team") or {}).get("name"),
                        "team_uid":  uid,
                        "played":    tr.get("total", 0),
                        "won":       tr.get("winTotal", 0),
                        "drawn":     tr.get("drawTotal", 0),
                        "lost":      tr.get("lossTotal", 0),
                        "gf":        tr.get("goalsForTotal", 0),
                        "ga":        tr.get("goalsAgainstTotal", 0),
                        "gd":        tr.get("goalDiffTotal", 0),
                        "pts":       tr.get("pointsTotal", 0),
                        "promotion": promo,
                        "is_target": uid in [h_uid, a_uid],
                    })
                break
            if rows:
                yield _sse("standings", sorted(rows, key=lambda x: x.get("pos") or 99))

        # ── FORM ──────────────────────────────────────────────────────────────
        form_d = _get(collected, f"stats_formtable/{s_id}") if s_id else {}
        if form_d:
            fo = {"home": [], "away": []}
            for t in (form_d.get("teams") or []):
                uid = str((t.get("team") or {}).get("uid", ""))
                fl  = [f.get("value") for f in (t.get("form") or {}).get("total", [])]
                if uid == h_uid:
                    fo["home"] = fl
                if uid == a_uid:
                    fo["away"] = fl
            yield _sse("form", fo)

        # ── PRESSURE / MOMENTUM ───────────────────────────────────────────────
        situation_d = _get(collected, f"stats_match_situation/{betradar_id}")
        if situation_d:
            raw_data = situation_d.get("data", [])
            pressure = []
            for d in raw_data:
                home = d.get("home", {})
                away = d.get("away", {})
                hp   = home.get("dangerous", 0) * 2 + home.get("attack", 0)
                ap   = away.get("dangerous", 0) * 2 + away.get("attack", 0)
                if hp > 0 or ap > 0:
                    pressure.append({
                        "minute":     d.get("time", 0),
                        "injurytime": d.get("injurytime", 0),
                        "home":       hp,
                        "away":       ap,
                        "home_raw":   {"attack": home.get("attack", 0), "dangerous": home.get("dangerous", 0)},
                        "away_raw":   {"attack": away.get("attack", 0), "dangerous": away.get("dangerous", 0)},
                    })
            if pressure:
                yield _sse("pressure", pressure)

        yield _sse("done", {"status": "complete", "endpoints_collected": len(collected)})

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "Connection":        "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )