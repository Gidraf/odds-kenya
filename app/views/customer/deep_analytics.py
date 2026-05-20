"""
app/api/deep_analytics.py
==========================
Full Sportradar analytics via Playwright — Playwright-only version.
No Sportradar API key required.

Data source: statshub.sportradar.com scraped by playwright_scraper.py
Cache:       SQLite (kinetic_analytics.db)
             - Upcoming/live:  re-scrape after 5 minutes
             - Finished:       keep for 24 hours

SSE events emitted:
  status, meta, stadium, lineups, comments, shot_map, h2h,
  versus_history, goal_timing, managers, top_scorers, team_stats,
  recent, upcoming, standings, form, pressure, done

Endpoints:
  GET /api/odds/match/<betradar_id>/deep_analytics
      → Returns SQLite cached JSON instantly (404 if not cached)
  GET /api/odds/match/<betradar_id>/deep_analytics/stream
      → Checks cache first → instant if cached
        Falls through to Playwright if not cached (~15-30s)
  DELETE /api/odds/match/<betradar_id>/deep_analytics/cache
      → Force re-scrape by deleting cached entry
"""
import json
import os
import re
import sqlite3
import time as _time
import logging
from threading import Thread, Event

from flask import Blueprint, Response, jsonify, stream_with_context, request

from app.utils.playwright_scraper import collect_match_data, get as _get

log = logging.getLogger("deep_analytics")

bp_deep_analytics = Blueprint("deep_analytics", __name__, url_prefix="/api")

# ─────────────────────────────────────────────────────────────────────────────
# SQLITE CACHE
# ─────────────────────────────────────────────────────────────────────────────

_DB_PATH          = os.environ.get("ANALYTICS_DB", "kinetic_analytics.db")
_LIVE_TTL_MIN     = 5        # re-scrape live/upcoming after 5 min
_FINISHED_TTL_MIN = 24 * 60  # keep finished match data for 24 hours

_FINISHED_STATUSES = frozenset({
    "finished", "ft", "aet", "ap", "ended", "complete",
    "Finished", "FT", "AET", "AP",
})


def _db():
    conn = sqlite3.connect(_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS analytics_cache (
            match_id TEXT PRIMARY KEY,
            data     TEXT NOT NULL,
            created  REAL NOT NULL,
            status   TEXT
        )
    """)
    conn.commit()
    return conn


def _cache_get(match_id: str) -> dict | None:
    """
    Return cached analytics dict if still fresh, else None.
    TTL: 5 min for live/upcoming, 24h for finished.
    """
    try:
        conn = _db()
        row  = conn.execute(
            "SELECT data, created, status FROM analytics_cache WHERE match_id=?",
            (match_id,)
        ).fetchone()
        conn.close()

        if not row:
            return None

        data_json, created_ts, status = row
        age_min = (_time.time() - created_ts) / 60
        ttl     = _FINISHED_TTL_MIN if (status or "") in _FINISHED_STATUSES else _LIVE_TTL_MIN

        if age_min > ttl:
            log.info("Analytics cache STALE for %s (age=%.1f min, ttl=%d min)",
                     match_id, age_min, ttl)
            return None

        log.info("Analytics cache HIT for %s (age=%.1f min)", match_id, age_min)
        return json.loads(data_json)

    except Exception as exc:
        log.warning("Analytics cache get %s: %s", match_id, exc)
        return None


def _cache_put(match_id: str, data: dict, status: str = "") -> None:
    try:
        conn = _db()
        conn.execute(
            "INSERT OR REPLACE INTO analytics_cache VALUES (?,?,?,?)",
            (match_id, json.dumps(data, default=str), _time.time(), status)
        )
        conn.commit()
        conn.close()
        log.info("Analytics cached: %s status=%r", match_id, status)
    except Exception as exc:
        log.warning("Analytics cache put %s: %s", match_id, exc)


def _cache_delete(match_id: str) -> bool:
    try:
        conn = _db()
        conn.execute("DELETE FROM analytics_cache WHERE match_id=?", (match_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as exc:
        log.warning("Analytics cache delete %s: %s", match_id, exc)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# SSE HELPER
# ─────────────────────────────────────────────────────────────────────────────

def _sse(event: str, data) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


# ─────────────────────────────────────────────────────────────────────────────
# DATA PARSERS
# ─────────────────────────────────────────────────────────────────────────────

def _clean(raw: str) -> str:
    if not raw:
        return ""
    if "," in raw:
        p = raw.split(",", 1)
        return f"{p[1].strip()} {p[0].strip()}"
    return raw.strip()


def _parse_player(p: dict) -> dict:
    raw = p.get("playername", p.get("name", ""))
    return {
        "name": raw.split(",")[0].strip() if "," in raw else raw.strip(),
        "num":  p.get("shirtnumber", ""),
        "pos":  p.get("matchpos", "M"),
        "id":   p.get("_id"),
    }


def _format_stadium(s: dict) -> dict:
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
        "id":          s.get("_id", ""),
        "name":        s.get("name", ""),
        "city":        s.get("city", ""),
        "country":     s.get("country", ""),
        "capacity":    s.get("capacity", ""),
        "built":       s.get("constryear", ""),
        "pitch":       s.get("pitchsize", {}),
        "coordinates": {"lat": lat, "lng": lng} if lat else None,
    }


def _detect_comp_type(info: dict) -> str:
    t    = info.get("tournament", {})
    name = t.get("name", "").lower()
    if t.get("friendly"):
        return "friendly"
    if str(t.get("seasontype", "")) == "26" or any(
        k in name for k in ["cup", "champions", "europa", "coupe"]
    ):
        return "cup"
    return "league"


def _time_from(m: dict) -> tuple[str, str]:
    dt = m.get("time") or m.get("_dt") or {}
    if not isinstance(dt, dict):
        return "", ""
    return dt.get("date", ""), dt.get("time", "")


def _parse_recent(data: dict) -> list:
    if not data:
        return []
    out = []
    for m in (data.get("matches") or [])[:5]:
        d, t = _time_from(m)
        out.append({
            "date":       d,
            "time":       t,
            "home":       (m.get("teams", {}).get("home") or {}).get("name", ""),
            "away":       (m.get("teams", {}).get("away") or {}).get("name", ""),
            "score_home": (m.get("result") or {}).get("home", 0),
            "score_away": (m.get("result") or {}).get("away", 0),
        })
    return out


def _parse_upcoming(data: dict) -> list:
    if not data:
        return []
    out = []
    for m in (data.get("matches") or [])[:3]:
        d, t = _time_from(m)
        out.append({
            "date": d,
            "time": t,
            "home": (m.get("teams", {}).get("home") or {}).get("name", ""),
            "away": (m.get("teams", {}).get("away") or {}).get("name", ""),
        })
    return out


def _goal_mins_from_comment(comment: str) -> list[int]:
    if not comment:
        return []
    out = []
    for m in re.finditer(r'\((\d+)(?:\+(\d+))?\.\)', comment):
        t = int(m.group(1)) + (int(m.group(2)) if m.group(2) else 0)
        if t <= 130:
            out.append(t)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# STREAM BUILDER — generates all SSE events from a collected dict
# ─────────────────────────────────────────────────────────────────────────────

def _build_and_stream(betradar_id: str, collected: dict):
    """
    Generator that parses `collected` (output of playwright_scraper.collect_match_data)
    and yields SSE events one by one.

    Called from both:
      - the live stream (after Playwright finishes)
      - the cache-hit path (same events, just from stored data)
    """
    yield _sse("status", {"step": "parsing", "message": "Processing match data..."})

    # ── Core match info ───────────────────────────────────────────────────────
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

    mt        = (
        (md.get("timeinfo") or {}).get("played")
        or md.get("p")
        or (md.get("status") or {}).get("shortName", "")
    )
    comp_type = _detect_comp_type(info)
    tourn     = info.get("tournament", {})
    stadium   = _format_stadium(info.get("stadium", {}))
    mgr_raw   = info.get("manager", {})

    match_status = (md.get("status") or {}).get("name", "")

    yield _sse("meta", {
        "home_team":        (teams.get("home") or {}).get("name", "Home"),
        "away_team":        (teams.get("away") or {}).get("name", "Away"),
        "home_abbr":        (teams.get("home") or {}).get("abbr", ""),
        "away_abbr":        (teams.get("away") or {}).get("abbr", ""),
        "home_uid":         h_uid,
        "away_uid":         a_uid,
        "season_id":        s_id,
        "status":           match_status or "Upcoming",
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

    # ── SQUADS ────────────────────────────────────────────────────────────────
    sq = _get(collected, f"match_squads/{betradar_id}")
    if sq and ("home" in sq or "away" in sq):
        def _squad(side: str) -> dict:
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

    # ── TIMELINE ──────────────────────────────────────────────────────────────
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
            ev_type = ev.get("type")
            if ev_type in IGNORED:
                continue
            entry: dict = {
                "time":    ev.get("time"),
                "seconds": ev.get("seconds"),
                "team":    ev.get("team"),
                "type":    ev_type,
                "name":    ev.get("name", ""),
                "x":       ev.get("X"),
                "y":       ev.get("Y"),
            }
            if ev_type == "goal":
                entry["owngoal"] = ev.get("owngoal", False)
                entry["penalty"] = ev.get("penalty", False)
                entry["result"]  = ev.get("result")
            if ev_type == "card":
                entry["card"] = ev.get("card")
            events.append(entry)

        significant = [e for e in events if e["type"] in SIGNIFICANT]
        yield _sse("comments", list(reversed(significant))[:30])

        all_coords = [e for e in events if e.get("x") is not None]
        if all_coords:
            yield _sse("shot_map", all_coords)

    # ── H2H SIMPLE ────────────────────────────────────────────────────────────
    h2h_s       = _get(collected, f"stats_match_head2head/{betradar_id}") or {}
    h2h_matches = h2h_s.get("matches") or []
    if h2h_matches:
        yield _sse("h2h", [
            {
                "date":       (m.get("_dt") or {}).get("date", ""),
                "home":       (m.get("teams") or {}).get("home", {}).get("name", ""),
                "away":       (m.get("teams") or {}).get("away", {}).get("name", ""),
                "score_home": (m.get("result") or {}).get("home", 0),
                "score_away": (m.get("result") or {}).get("away", 0),
            }
            for m in h2h_matches[:5]
        ])
    else:
        yield _sse("h2h", [])

    # ── H2H FULL (versusrecent) ───────────────────────────────────────────────
    h2h_f = (_get(collected, f"stats_team_versusrecent/{h_uid}/{a_uid}") or {}) if h_uid and a_uid else {}
    if h2h_f:
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

            g        = sh + sa
            total_g += g
            if sh > 0 and sa > 0:  btts += 1
            if g > 2:              ov25 += 1

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
                "goal_minutes": _goal_mins_from_comment(m.get("comment", "")),
            })

        n        = len(parsed)
        all_mins = [mi for m in raw_m[:20] for mi in _goal_mins_from_comment(m.get("comment", ""))]
        buckets  = {"0-15": 0, "16-30": 0, "31-45": 0, "46-60": 0, "61-75": 0, "76-90": 0, "90+": 0}
        for mi in all_mins:
            if   mi <= 15: buckets["0-15"]  += 1
            elif mi <= 30: buckets["16-30"] += 1
            elif mi <= 45: buckets["31-45"] += 1
            elif mi <= 60: buckets["46-60"] += 1
            elif mi <= 75: buckets["61-75"] += 1
            elif mi <= 90: buckets["76-90"] += 1
            else:          buckets["90+"]   += 1

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
                "buckets":        buckets,
                "most_dangerous": max(buckets, key=buckets.get) if all_mins else None,
                "avg_minute":     round(sum(all_mins) / len(all_mins), 1) if all_mins else None,
                "first_half_pct": round(
                    sum(1 for mi in all_mins if mi <= 45) / max(len(all_mins), 1) * 100, 1
                ),
            },
        })

    # ── MANAGERS ──────────────────────────────────────────────────────────────
    cm = h2h_f.get("currentmanagers", {}) if h2h_f else {}

    def _mgr(uid: str, fallback_name: str = "") -> dict:
        lst = cm.get(uid) or cm.get(str(uid)) or []
        if lst:
            mgr_obj = lst[0]
            ms      = mgr_obj.get("membersince") or {}
            return {
                "id":          mgr_obj.get("_id"),
                "name":        _clean(mgr_obj.get("name", "")),
                "nationality": (mgr_obj.get("nationality") or {}).get("name", ""),
                "membersince": ms.get("date", "") if isinstance(ms, dict) else "",
            }
        return {"name": fallback_name} if fallback_name else {}

    h_mgr_name = _clean(((mgr_raw or {}).get("home") or {}).get("name", ""))
    a_mgr_name = _clean(((mgr_raw or {}).get("away") or {}).get("name", ""))
    h_mgr      = _mgr(h_uid, h_mgr_name)
    a_mgr      = _mgr(a_uid, a_mgr_name)
    if h_mgr or a_mgr:
        yield _sse("managers", {"home": h_mgr, "away": a_mgr})

    # ── TOP SCORERS ───────────────────────────────────────────────────────────
    def _scorers(data: dict) -> list:
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

    h_sc = (_get(collected, f"stats_season_topgoals/{s_id}/{h_uid}") or {}) if s_id and h_uid else {}
    a_sc = (_get(collected, f"stats_season_topgoals/{s_id}/{a_uid}") or {}) if s_id and a_uid else {}
    if h_sc or a_sc:
        yield _sse("top_scorers", {"home": _scorers(h_sc), "away": _scorers(a_sc)})

    # ── TEAM STATS ────────────────────────────────────────────────────────────
    ts = (_get(collected, f"stats_season_uniqueteamstats/{s_id}") or {}) if s_id else {}
    if ts:
        def _ts(uid: str) -> dict:
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

    # ── RECENT ────────────────────────────────────────────────────────────────
    h_recent = (
        _get(collected,
             f"stats_team_lastx/{h_uid}/20",
             f"stats_team_lastx/{h_uid}/10",
             f"stats_team_lastx/{h_uid}/5") or {}
    ) if h_uid else {}
    a_recent = (
        _get(collected,
             f"stats_team_lastx/{a_uid}/20",
             f"stats_team_lastx/{a_uid}/10",
             f"stats_team_lastx/{a_uid}/5") or {}
    ) if a_uid else {}
    yield _sse("recent", {
        "home": _parse_recent(h_recent),
        "away": _parse_recent(a_recent),
    })

    # ── UPCOMING ──────────────────────────────────────────────────────────────
    h_next = (
        _get(collected,
             f"stats_team_fixtures/{h_uid}/10",
             f"stats_team_fixtures/{h_uid}/5") or {}
    ) if h_uid else {}
    a_next = (
        _get(collected,
             f"stats_team_fixtures/{a_uid}/10",
             f"stats_team_fixtures/{a_uid}/5") or {}
    ) if a_uid else {}
    yield _sse("upcoming", {
        "home": _parse_upcoming(h_next),
        "away": _parse_upcoming(a_next),
    })

    # ── STANDINGS ─────────────────────────────────────────────────────────────
    raw_table = (
        _get(collected,
             f"season_dynamictable/{s_id}",
             f"stats_season_tables/{s_id}/1") or {}
    ) if s_id else {}

    if raw_table:
        tables = (
            raw_table.get("tables")
            or (raw_table.get("season") or {}).get("tables")
            or []
        )
        rows = []
        for t in tables:
            for tr in t.get("tablerows", []):
                uid  = str((tr.get("team") or {}).get("uid", ""))
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
            break  # only first table group
        if rows:
            yield _sse("standings", sorted(rows, key=lambda x: x.get("pos") or 99))

    # ── FORM ──────────────────────────────────────────────────────────────────
    form_d = (_get(collected, f"stats_formtable/{s_id}") or {}) if s_id else {}
    if form_d:
        fo: dict = {"home": [], "away": []}
        for t in (form_d.get("teams") or []):
            uid = str((t.get("team") or {}).get("uid", ""))
            fl  = [f.get("value") for f in (t.get("form") or {}).get("total", [])]
            if uid == h_uid: fo["home"] = fl
            if uid == a_uid: fo["away"] = fl
        yield _sse("form", fo)

    # ── PRESSURE / MOMENTUM ───────────────────────────────────────────────────
    situation_d = (_get(collected, f"stats_match_situation/{betradar_id}") or {})
    if situation_d:
        raw_data = situation_d.get("data", [])
        pressure = []
        for d in raw_data:
            home_d = d.get("home", {})
            away_d = d.get("away", {})
            hp     = home_d.get("dangerous", 0) * 2 + home_d.get("attack", 0)
            ap     = away_d.get("dangerous", 0) * 2 + away_d.get("attack", 0)
            if hp > 0 or ap > 0:
                pressure.append({
                    "minute":     d.get("time", 0),
                    "injurytime": d.get("injurytime", 0),
                    "home":       hp,
                    "away":       ap,
                    "home_raw":   {"attack": home_d.get("attack", 0), "dangerous": home_d.get("dangerous", 0)},
                    "away_raw":   {"attack": away_d.get("attack", 0), "dangerous": away_d.get("dangerous", 0)},
                })
        if pressure:
            yield _sse("pressure", pressure)

    yield _sse("done", {
        "status":               "complete",
        "endpoints_collected":  len(collected),
        "match_status":         match_status,
    })

    # Return match_status so the caller can cache with the right TTL
    return match_status


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@bp_deep_analytics.route("/odds/match/<betradar_id>/deep_analytics", methods=["GET"])
def get_analytics(betradar_id: str):
    """
    GET /api/odds/match/<betradar_id>/deep_analytics

    Returns cached analytics JSON instantly.
    Query params:
      force=true  — ignore cache, return 404 to force a fresh stream
    """
    force = request.args.get("force", "false").lower() == "true"
    if not force:
        cached = _cache_get(betradar_id)
        if cached:
            return jsonify({**cached, "cached": True, "betradar_id": betradar_id})
    return jsonify({
        "error":       "Not cached. Use the SSE stream endpoint to fetch fresh data.",
        "stream_url":  f"/api/odds/match/{betradar_id}/deep_analytics/stream",
        "betradar_id": betradar_id,
    }), 404


@bp_deep_analytics.route("/odds/match/<betradar_id>/deep_analytics/stream", methods=["GET"])
def stream_deep_analytics(betradar_id: str):
    """
    GET /api/odds/match/<betradar_id>/deep_analytics/stream

    Flow:
      1. Check SQLite cache → if fresh, stream all events instantly from cache
      2. If not cached (or force=true) → launch Playwright in background thread
         → yield status pings every 3s while Playwright navigates tabs
         → stream all events as they parse
         → persist result to SQLite cache
    Query params:
      force=true  — ignore cache, always re-scrape with Playwright
    """
    force = request.args.get("force", "false").lower() == "true"

    def generate():

        # ── Step 1: Try SQLite cache first ────────────────────────────────────
        if not force:
            cached = _cache_get(betradar_id)
            if cached:
                log.info("Analytics stream from CACHE: %s", betradar_id)
                yield _sse("status", {
                    "step":    "cache",
                    "message": "Serving from cache…",
                    "cached":  True,
                })
                # Stream all stored events
                yield from _build_and_stream(betradar_id, cached)
                return

        # ── Step 2: Playwright scrape ─────────────────────────────────────────
        log.info("Analytics Playwright scrape starting: %s", betradar_id)
        yield _sse("status", {"step": "init", "message": "Launching browser…"})

        collected:   dict  = {}
        scrape_error: list = []
        done_event         = Event()

        def scrape():
            try:
                result = collect_match_data(betradar_id)
                collected.update(result)
            except Exception as exc:
                scrape_error.append(str(exc))
                log.error("Playwright scrape error for %s: %s", betradar_id, exc)
            finally:
                done_event.set()

        Thread(target=scrape, daemon=True).start()

        # Yield progress pings every 3s while Playwright works (~15-30s total)
        tab_labels = ["overview", "report", "statistics", "h2h", "table"]
        ping = 0
        while not done_event.wait(timeout=3):
            yield _sse("status", {
                "step":    "scraping",
                "message": f"Scraping {tab_labels[min(ping, len(tab_labels) - 1)]} tab…",
                "ping":    ping,
            })
            ping += 1

        if scrape_error or not collected:
            err = scrape_error[0] if scrape_error else "Playwright returned no data"
            log.error("Playwright scrape failed for %s: %s", betradar_id, err)
            yield _sse("error", {"message": err})
            return

        # ── Step 3: Parse and stream ──────────────────────────────────────────
        match_status = ""
        try:
            # _build_and_stream is a generator; consume it and capture return value
            gen = _build_and_stream(betradar_id, collected)
            for event_chunk in gen:
                yield event_chunk
            # Python doesn't easily return from a generator via StopIteration value
            # so we re-read the status from collected directly
            md           = (_get(collected, f"match_info_statshub/{betradar_id}", f"match_info/{betradar_id}") or {}).get("match", {})
            match_status = (md.get("status") or {}).get("name", "")
        except Exception as exc:
            log.error("Stream build error for %s: %s", betradar_id, exc)
            yield _sse("error", {"message": f"Parse error: {exc}"})
            return

        # ── Step 4: Persist to SQLite ─────────────────────────────────────────
        try:
            _cache_put(betradar_id, dict(collected), status=match_status)
            log.info("Analytics persisted to SQLite: %s (status=%r)", betradar_id, match_status)
        except Exception as exc:
            log.warning("SQLite cache write failed for %s: %s", betradar_id, exc)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "Connection":        "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        },
    )


@bp_deep_analytics.route("/odds/match/<betradar_id>/deep_analytics/cache", methods=["DELETE"])
def invalidate_cache(betradar_id: str):
    """
    DELETE /api/odds/match/<betradar_id>/deep_analytics/cache
    Force re-scrape by deleting the cached entry.
    """
    ok = _cache_delete(betradar_id)
    return jsonify({
        "ok":          ok,
        "betradar_id": betradar_id,
        "message":     "Cache invalidated. Next request will re-scrape." if ok else "Nothing to delete.",
    })


@bp_deep_analytics.route("/odds/analytics/cache/status", methods=["GET"])
def cache_status():
    """
    GET /api/odds/analytics/cache/status
    Lists all cached match IDs with their age and status.
    """
    try:
        conn = _db()
        rows = conn.execute(
            "SELECT match_id, created, status FROM analytics_cache ORDER BY created DESC LIMIT 100"
        ).fetchall()
        conn.close()
        now = _time.time()
        return jsonify({
            "count": len(rows),
            "entries": [
                {
                    "match_id":   r[0],
                    "age_min":    round((now - r[1]) / 60, 1),
                    "status":     r[2],
                    "is_stale":   (now - r[1]) / 60 > (
                        _FINISHED_TTL_MIN if (r[2] or "") in _FINISHED_STATUSES
                        else _LIVE_TTL_MIN
                    ),
                }
                for r in rows
            ],
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500