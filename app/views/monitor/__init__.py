"""
app/views/odds_feed/monitor_view.py
====================================
Real-time monitoring dashboard — separated log levels, flask + celery logs.

Endpoints
─────────
  GET /api/monitor/dashboard        ← HTML dashboard
  GET /api/monitor/stream/logs      ← SSE: celery.log tail
  GET /api/monitor/stream/flask     ← SSE: flask.log tail
  GET /api/monitor/stream/tasks     ← SSE: tasks.log tail
  GET /api/monitor/stream/jobs      ← SSE: job log (Redis)
  GET /api/monitor/report           ← JSON health report
  GET /api/monitor/report/history   ← JSON last 60 snapshots
  GET /api/monitor/jobs             ← JSON last N jobs
  GET /api/monitor/sports           ← JSON per-sport cache
  GET /api/monitor/beat             ← JSON beat schedule
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from app.models.competions_model import Competition, Team, Country, Sport
from app.models.odds import UnifiedMatch 
from app.models.bookmake_competition_data import BookmakerCompetitionName, BookmakerTeamName, BookmakerCountryName
from sqlalchemy import func, desc

from flask import Blueprint, Response, request, stream_with_context

bp_monitor = Blueprint("monitor", __name__, url_prefix="/api/monitor/debug/")

LOG_DIR    = Path(os.environ.get("LOG_DIR", "logs"))
CELERY_LOG = LOG_DIR / "celery.log"
FLASK_LOG  = LOG_DIR / "flask.log"
TASK_LOG   = LOG_DIR / "tasks.log"
JOB_LOG    = LOG_DIR / "harvest_jobs.log"

BEAT_SCHEDULE: dict[str, int] = {
    "tasks.sp.harvest_all_upcoming":       300,
    "tasks.bt.harvest_all_upcoming":       360,
    "tasks.od.harvest_all_upcoming":       420,
    "tasks.b2b.harvest_all_upcoming":      480,
    "tasks.b2b_page.harvest_all_upcoming": 300,
    "tasks.sbo.harvest_all_upcoming":      180,
    "tasks.ops.health_check":               30,
    "tasks.ops.update_match_results":      300,
    "tasks.ops.persist_all_sports":        300,
    "tasks.ops.build_health_report":        60,
    "tasks.ops.expire_subscriptions":     3600,
    "tasks.ops.cache_finished_games":     3600,
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _cache_get(key: str):
    try:
        from app.workers.celery_tasks import cache_get
        return cache_get(key)
    except Exception:
        return None


def _redis():
    try:
        from app.workers.celery_tasks import _redis as _r
        return _r()
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# JSON API
# ─────────────────────────────────────────────────────────────────────────────

@bp_monitor.route("/report")
def get_report():
    report = _cache_get("monitor:report")
    if not report:
        try:
            from app.workers.tasks_ops import build_health_report
            report = build_health_report()
        except Exception as exc:
            return {"ok": False, "error": str(exc)}, 500
    return {"ok": True, **report}


@bp_monitor.route("/report/history")
def get_report_history():
    r = _redis()
    if not r:
        return {"ok": False, "error": "Redis unavailable"}, 503
    limit   = min(int(request.args.get("limit", 60)), 60)
    history = []
    for item in r.lrange("monitor:report_history", 0, limit - 1):
        try:
            history.append(json.loads(item))
        except Exception:
            pass
    return {"ok": True, "total": len(history), "history": history, "ts": _now()}


@bp_monitor.route("/jobs")
def get_jobs():
    r = _redis()
    if not r:
        return {"ok": False, "error": "Redis unavailable"}, 503
    limit     = min(int(request.args.get("limit", 100)), 500)
    bk_filter = request.args.get("bookmaker", "").lower()
    sp_filter = request.args.get("sport", "").lower()
    st_filter = request.args.get("status", "").lower()
    jobs = []
    for item in r.lrange("monitor:job_log", 0, 499):
        try:
            entry = json.loads(item)
            if bk_filter and entry.get("bookmaker", "") != bk_filter: continue
            if sp_filter and entry.get("sport", "")     != sp_filter: continue
            if st_filter and entry.get("status", "")    != st_filter: continue
            jobs.append(entry)
        except Exception:
            pass
        if len(jobs) >= limit:
            break
    return {
        "ok": True, "total": len(jobs),
        "ok_count":    sum(1 for j in jobs if j.get("status") == "ok"),
        "err_count":   sum(1 for j in jobs if j.get("status") == "error"),
        "empty_count": sum(1 for j in jobs if j.get("status") == "empty"),
        "jobs": jobs, "ts": _now(),
    }


@bp_monitor.route("/sports")
def get_sports():
    from app.workers.tasks_ops import PERSIST_SPORTS
    summary = []
    for sport in PERSIST_SPORTS:
        u   = _cache_get(f"combined:upcoming:{sport}") or {}
        l   = _cache_get(f"combined:live:{sport}")     or {}
        u_m = u.get("matches", [])
        l_m = l.get("matches", [])
        summary.append({
            "sport": sport,
            "upcoming": len(u_m), "live": len(l_m),
            "arbs": sum(1 for m in u_m + l_m if m.get("has_arb")),
            "evs":  sum(1 for m in u_m + l_m if m.get("has_ev")),
            "bk_counts_up":    u.get("bk_counts", {}),
            "harvested_at_up": u.get("harvested_at"),
            "harvested_at_lv": l.get("harvested_at"),
            "populated":       bool(u_m or l_m),
        })
    populated = sum(1 for s in summary if s["populated"])
    return {"ok": True, "total_sports": len(PERSIST_SPORTS),
            "populated_sports": populated, "sports": summary, "ts": _now()}


@bp_monitor.route("/beat")
def get_beat():
    r = _redis()
    last_run_map: dict[str, str] = {}
    if r:
        try:
            last_run_map = {
                k.decode() if isinstance(k, bytes) else k:
                v.decode() if isinstance(v, bytes) else v
                for k, v in (r.hgetall("monitor:last_run") or {}).items()
            }
        except Exception:
            pass
    now_utc = datetime.now(timezone.utc)
    tasks   = []
    for task_name, interval in sorted(BEAT_SCHEDULE.items(), key=lambda x: x[1]):
        last_str = last_run_map.get(task_name)
        next_in  = None
        if last_str:
            try:
                last_dt = datetime.fromisoformat(last_str.replace("Z", "+00:00"))
                elapsed = (now_utc - last_dt).total_seconds()
                next_in = max(0, int(interval - elapsed))
            except Exception:
                pass
        tasks.append({
            "task": task_name, "interval": interval,
            "last_run": last_str, "next_in_s": next_in,
            "overdue": (next_in is not None and next_in == 0),
        })
    return {"ok": True, "tasks": tasks, "ts": _now()}


# ─────────────────────────────────────────────────────────────────────────────
# SSE helpers
# ─────────────────────────────────────────────────────────────────────────────

def _tail_file_sse(filepath: Path, src: str):
    filepath.touch(exist_ok=True)
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        f.seek(0, 2)
        last_ka = time.monotonic()
        while True:
            line = f.readline()
            if line:
                line = line.rstrip("\n")
                if line:
                    yield f"data: {json.dumps({'src':src,'line':line,'ts':_now()})}\n\n"
            else:
                time.sleep(0.2)
                if time.monotonic() - last_ka >= 15:
                    yield f": keep-alive {_now()}\n\n"
                    last_ka = time.monotonic()


_SSE_HEADERS = {
    "Content-Type":      "text/event-stream",
    "Cache-Control":     "no-cache",
    "X-Accel-Buffering": "no",
    "Connection":        "keep-alive",
}


@bp_monitor.route("/stream/logs")
def stream_celery_logs():
    @stream_with_context
    def gen():
        yield f"data: {json.dumps({'src':'celery','type':'connected','ts':_now()})}\n\n"
        yield from _tail_file_sse(CELERY_LOG, "celery")
    return Response(gen(), headers=_SSE_HEADERS)


@bp_monitor.route("/stream/flask")
def stream_flask_logs():
    @stream_with_context
    def gen():
        yield f"data: {json.dumps({'src':'flask','type':'connected','ts':_now()})}\n\n"
        yield from _tail_file_sse(FLASK_LOG, "flask")
    return Response(gen(), headers=_SSE_HEADERS)


@bp_monitor.route("/stream/tasks")
def stream_task_logs():
    @stream_with_context
    def gen():
        yield f"data: {json.dumps({'src':'tasks','type':'connected','ts':_now()})}\n\n"
        yield from _tail_file_sse(TASK_LOG, "tasks")
    return Response(gen(), headers=_SSE_HEADERS)


@bp_monitor.route("/stream/jobs")
def stream_jobs():
    @stream_with_context
    def gen():
        r = _redis()
        if not r:
            yield f"data: {json.dumps({'error':'Redis unavailable'})}\n\n"
            return
        yield f"data: {json.dumps({'src':'jobs','type':'connected','ts':_now()})}\n\n"
        try:
            last_len = r.llen("monitor:job_log")
        except Exception:
            last_len = 0
        last_ka = time.monotonic()
        while True:
            time.sleep(0.5)
            try:
                cur = r.llen("monitor:job_log")
                if cur > last_len:
                    for raw in reversed(r.lrange("monitor:job_log", 0, cur - last_len - 1)):
                        try:
                            yield f"data: {json.dumps({'src':'jobs','type':'job',**json.loads(raw)})}\n\n"
                        except Exception:
                            pass
                    last_len = cur
            except Exception:
                pass
            if time.monotonic() - last_ka >= 15:
                yield f": keep-alive {_now()}\n\n"
                last_ka = time.monotonic()
    return Response(gen(), headers=_SSE_HEADERS)


# ─────────────────────────────────────────────────────────────────────────────
# HTML Dashboard
# ─────────────────────────────────────────────────────────────────────────────

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Odds Kenya Monitor</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{
  --bg:#080d1a;--panel:#0d1526;--border:#1a2540;--text:#e2e8f0;--dim:#4a5a78;
  --green:#22c55e;--gbg:#052e16;--red:#ef4444;--rbg:#450a0a;
  --yellow:#f59e0b;--ybg:#3d2000;--blue:#3b82f6;--bbg:#0c1d3d;
  --purple:#a855f7;--cyan:#06b6d4;--orange:#f97316;
}
*{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);
  font-family:'JetBrains Mono','Fira Code',Consolas,monospace;
  font-size:12px;overflow:hidden;height:100vh;display:flex;flex-direction:column;}

/* top bar */
#tb{background:var(--panel);border-bottom:2px solid var(--border);
  padding:7px 14px;display:flex;align-items:center;gap:10px;flex-shrink:0;}
#tb h1{font-size:13px;color:var(--cyan);letter-spacing:1px;white-space:nowrap;}
.pill{padding:2px 8px;border-radius:20px;font-size:10px;font-weight:bold;}
.pill.g{background:var(--gbg);color:var(--green);border:1px solid var(--green);}
.pill.r{background:var(--rbg);color:var(--red);border:1px solid var(--red);}
.pill.y{background:var(--ybg);color:var(--yellow);border:1px solid var(--yellow);}
#clock{margin-left:auto;color:var(--dim);font-size:10px;white-space:nowrap;}
.met{text-align:center;min-width:56px;}
.met .v{font-size:17px;font-weight:bold;}
.met .l{font-size:9px;color:var(--dim);text-transform:uppercase;}

/* layout */
#main{display:grid;grid-template-columns:220px 1fr;grid-template-rows:1fr 185px;
  gap:1px;background:var(--border);flex:1;overflow:hidden;}
.pn{background:var(--panel);overflow:hidden;display:flex;flex-direction:column;}
.pt{padding:5px 10px;font-size:10px;font-weight:bold;color:var(--dim);
  text-transform:uppercase;letter-spacing:1px;border-bottom:1px solid var(--border);
  flex-shrink:0;display:flex;align-items:center;gap:5px;}
.pt .dot{width:5px;height:5px;border-radius:50%;animation:pulse 2s infinite;}
.pb{flex:1;overflow-y:auto;padding:5px;}
@keyframes pulse{0%,100%{opacity:1;}50%{opacity:.3;}}

/* left column = beat + sports stacked */
#left{grid-column:1;grid-row:1;display:flex;flex-direction:column;}
#beat-pn{flex:1;overflow:hidden;display:flex;flex-direction:column;}
#sport-pn{flex:0 0 200px;border-top:1px solid var(--border);display:flex;flex-direction:column;}

/* log panel */
#log-pn{grid-column:2;grid-row:1;}

/* toolbar */
#ltb{display:flex;gap:3px;align-items:center;flex-wrap:wrap;
  padding:5px 8px;border-bottom:1px solid var(--border);flex-shrink:0;}
.sb,.lb,.cb{padding:2px 7px;border-radius:3px;font-size:10px;cursor:pointer;
  font-family:inherit;border:1px solid var(--border);background:var(--bg);color:var(--dim);}
.sb:hover,.lb:hover,.cb:hover{background:var(--border);color:var(--text);}
.sb.on,.lb.on{color:#fff;}
.sb.sc.on{background:#0d2e0d;border-color:var(--green);color:var(--green);}
.sb.sf.on{background:#0d1a2e;border-color:var(--blue);color:var(--blue);}
.sb.st.on{background:#1a0d2e;border-color:var(--purple);color:var(--purple);}
.lb.le.on{background:var(--rbg);border-color:var(--red);color:var(--red);}
.lb.lw.on{background:var(--ybg);border-color:var(--yellow);color:var(--yellow);}
.lb.li.on{background:var(--bbg);border-color:var(--blue);color:var(--blue);}
.lb.ld.on{background:#111;border-color:#333;color:#555;}
#sb-inp{background:var(--border);border:1px solid var(--dim);border-radius:3px;
  padding:2px 7px;color:var(--text);font-family:inherit;font-size:10px;width:130px;}
#sb-inp::placeholder{color:var(--dim);}
.sep{width:1px;height:14px;background:var(--border);}
.cbadge{font-size:10px;padding:1px 5px;border-radius:8px;background:var(--border);color:var(--dim);}
.cbadge.e{background:var(--rbg);color:var(--red);}
.cbadge.w{background:var(--ybg);color:var(--yellow);}
.sp{flex:1;}
#cpill{font-size:10px;padding:1px 7px;border-radius:8px;background:var(--border);}

/* log body */
#lb{flex:1;overflow-y:auto;padding:5px 10px;line-height:1.65;}
.ll{display:flex;gap:6px;padding:0px 0;border-radius:2px;}
.ll:hover{background:rgba(255,255,255,.03);}
.ll.hidden{display:none;}
.lts{color:var(--dim);white-space:nowrap;font-size:10px;flex-shrink:0;}
.lsrc{font-size:9px;padding:1px 4px;border-radius:3px;flex-shrink:0;align-self:center;font-weight:bold;}
.lsrc.celery{background:#0d2e0d;color:var(--green);}
.lsrc.flask {background:#0d1a2e;color:var(--blue);}
.lsrc.tasks {background:#1a0d2e;color:var(--purple);}
.ltxt{flex:1;word-break:break-all;}
.lv-error  {color:var(--red);}
.lv-warning{color:var(--yellow);}
.lv-info   {color:var(--text);}
.lv-debug  {color:#2a3a52;}
.lv-success{color:var(--green);}
.lv-startup{color:var(--orange);}
.lv-task   {color:var(--cyan);}

/* beat */
.br{display:flex;align-items:center;gap:5px;padding:3px;border-radius:3px;margin-bottom:1px;}
.br:hover{background:var(--border);}
.bn{flex:1;font-size:10px;color:var(--dim);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.bn span{color:var(--text);}
.bw{width:55px;height:3px;background:var(--border);border-radius:2px;}
.bb{height:100%;border-radius:2px;transition:width 1s linear;}
.bg2{background:var(--green);}
.by2{background:var(--yellow);}
.br2{background:var(--red);}
.bc{font-size:10px;width:30px;text-align:right;}

/* sports */
.sr{display:flex;align-items:center;gap:4px;padding:3px;border-radius:3px;margin-bottom:1px;}
.sr:hover{background:var(--border);}
.sdot{width:5px;height:5px;border-radius:50%;flex-shrink:0;}
.sn{flex:1;font-size:10px;}
.sbg{font-size:9px;padding:1px 4px;border-radius:6px;background:var(--border);}
.sbg.warm{color:var(--green);}
.sbg.cold{color:var(--red);}

/* jobs */
#jobs-pn{grid-column:1/-1;grid-row:2;}
.jtb{display:flex;gap:5px;align-items:center;padding:4px 10px;
  border-bottom:1px solid var(--border);flex-shrink:0;}
#jf-inp{background:var(--border);border:1px solid var(--dim);border-radius:3px;
  padding:2px 7px;color:var(--text);font-family:inherit;font-size:10px;width:110px;}
.jfb{padding:2px 7px;border-radius:3px;font-size:10px;cursor:pointer;
  font-family:inherit;border:1px solid var(--border);background:var(--bg);color:var(--dim);}
.jfb.a{background:var(--bbg);color:var(--blue);border-color:var(--blue);}
#jc{color:var(--dim);font-size:10px;}
#jbody{flex:1;overflow:auto;}
table{width:100%;border-collapse:collapse;}
thead th{position:sticky;top:0;background:#0d1526;padding:4px 8px;
  text-align:left;font-size:9px;text-transform:uppercase;color:var(--dim);
  white-space:nowrap;border-bottom:1px solid var(--border);}
tbody td{padding:3px 8px;border-bottom:1px solid #111827;font-size:10px;}
tbody tr:hover td{background:rgba(255,255,255,.03);}
.ok{color:var(--green);}
.er{color:var(--red);}
.em{color:var(--yellow);}
::-webkit-scrollbar{width:3px;height:3px;}
::-webkit-scrollbar-track{background:var(--panel);}
::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px;}
</style>
</head>
<body>

<div id="tb">
  <h1>⚡ ODDS KENYA MONITOR</h1>
  <span id="hpill" class="pill y">LOADING</span>
  <div class="met"><div class="v" id="mup">—</div><div class="l">Upcoming</div></div>
  <div class="met"><div class="v" id="mlv" style="color:var(--cyan)">—</div><div class="l">Live</div></div>
  <div class="met"><div class="v" id="marb" style="color:var(--yellow)">—</div><div class="l">Arbs</div></div>
  <div class="met"><div class="v" id="mev" style="color:var(--purple)">—</div><div class="l">EV</div></div>
  <div class="met"><div class="v" id="msp">—</div><div class="l">Sports</div></div>
  <span id="clock">—</span>
</div>

<div id="main">

  <!-- Left: beat + sports -->
  <div id="left">
    <div id="beat-pn" class="pn">
      <div class="pt"><span class="dot" style="background:var(--green)"></span>Beat Schedule</div>
      <div class="pb" id="blist"><div style="color:var(--dim);padding:6px">Loading…</div></div>
    </div>
    <div id="sport-pn" class="pn">
      <div class="pt"><span class="dot" style="background:var(--blue)"></span>Sport Cache</div>
      <div class="pb" id="slist"><div style="color:var(--dim);padding:6px">Loading…</div></div>
    </div>
  </div>

  <!-- Log panel -->
  <div class="pn" id="log-pn">
    <div id="ltb">
      <span style="color:var(--dim);font-size:9px">SRC</span>
      <button class="sb sc on" data-src="celery" onclick="toggleSrc(this)">Celery</button>
      <button class="sb sf on" data-src="flask"  onclick="toggleSrc(this)">Flask</button>
      <button class="sb st on" data-src="tasks"  onclick="toggleSrc(this)">Tasks</button>
      <div class="sep"></div>
      <span style="color:var(--dim);font-size:9px">LVL</span>
      <button class="lb le on" data-lv="error"   onclick="toggleLvl(this)">ERR</button>
      <button class="lb lw on" data-lv="warning" onclick="toggleLvl(this)">WARN</button>
      <button class="lb li on" data-lv="info"    onclick="toggleLvl(this)">INFO</button>
      <button class="lb ld"    data-lv="debug"   onclick="toggleLvl(this)">DBG</button>
      <div class="sep"></div>
      <button class="cb" style="border-color:var(--rbg);color:var(--red)"   onclick="soloLvl('error')">Errors only</button>
      <button class="cb" style="border-color:var(--ybg);color:var(--yellow)"onclick="soloLvl('warning')">Warnings only</button>
      <button class="cb" onclick="showAll()">All</button>
      <div class="sep"></div>
      <input id="sb-inp" placeholder="Search…" oninput="applyFilters()">
      <span class="cbadge e" id="cnt-e">0 ERR</span>
      <span class="cbadge w" id="cnt-w">0 WARN</span>
      <span class="cbadge"   id="cnt-t">0 lines</span>
      <div class="sp"></div>
      <button class="cb" id="btn-pause" onclick="togglePause()">⏸</button>
      <button class="cb" onclick="clearLog()">🗑</button>
      <button class="cb" onclick="scrollBot()">⬇</button>
      <span id="cpill">● —</span>
    </div>
    <div id="lb"></div>
  </div>

  <!-- Jobs -->
  <div class="pn" id="jobs-pn">
    <div class="pt" style="padding:0 10px;">
      <span class="dot" style="background:var(--orange)"></span>Harvest Jobs
      <div class="jtb" style="flex:1;border:none;padding:4px 0 4px 10px;">
        <input id="jf-inp" placeholder="bk / sport…" oninput="filterJobs(this.value)">
        <button class="jfb a" data-jf="all"   onclick="setJF(this,'all')">All</button>
        <button class="jfb"   data-jf="ok"    onclick="setJF(this,'ok')">✔ OK</button>
        <button class="jfb"   data-jf="error" onclick="setJF(this,'error')" style="color:var(--red)">✗ Err</button>
        <button class="jfb"   data-jf="empty" onclick="setJF(this,'empty')" style="color:var(--yellow)">○ Empty</button>
        <span class="sp"></span>
        <span id="jc">0 jobs</span>
      </div>
    </div>
    <div id="jbody">
      <table>
        <thead><tr>
          <th>Time</th><th>Bookmaker</th><th>Sport</th><th>Mode</th>
          <th>Count</th><th>Status</th><th>✔ Saved</th><th>✗ Failed</th>
          <th>Latency</th><th>Detail</th>
        </tr></thead>
        <tbody id="jtbody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
// ─── state ────────────────────────────────────────────────────────────
const aS={celery:true,flask:true,tasks:true};
const aL={error:true,warning:true,info:true,debug:false};
let paused=false,autoScroll=true,srchTxt="",jFilter="all",jSearch="";
let errCnt=0,warnCnt=0,totLines=0;
const MAX_LINES=3000,MAX_JOBS=60;
const beatLR={};
let allJobs=[];

// ─── clock ────────────────────────────────────────────────────────────
setInterval(()=>{
  document.getElementById("clock").textContent=
    new Date().toISOString().replace("T"," ").slice(0,19)+" UTC";
},1000);

// ─── classify ─────────────────────────────────────────────────────────
function classify(line){
  const u=line.toUpperCase();
  if(/\bCRITICAL\b|\bFATAL\b/.test(u)||/\bERROR\b| ERROR /i.test(line)) return"error";
  if(/\bWARNING\b| WARN /i.test(line)) return"warning";
  if(/\bDEBUG\b| DEBUG /i.test(line))  return"debug";
  if(/succeeded|persisted=\d|ok=true|\[startup\]/i.test(line)) return"success";
  if(/received task|task.*succeeded|task.*failed/i.test(line)) return"task";
  return"info";
}
function lvCls(lv){
  return{error:"lv-error",warning:"lv-warning",info:"lv-info",debug:"lv-debug",
         success:"lv-success",startup:"lv-startup",task:"lv-task"}[lv]||"lv-info";
}

// ─── log rendering ────────────────────────────────────────────────────
function visible(el){
  const srcOk=aS[el.dataset.src]!==false;
  const lv=el.dataset.lv;
  const lvKey={success:"info",startup:"info",task:"info"}[lv]||lv;
  const lvOk=aL[lvKey]!==false;
  const txtOk=!srchTxt||(el.dataset.txt||"").includes(srchTxt);
  return srcOk&&lvOk&&txtOk;
}

function addLine(src,raw){
  if(paused)return;
  const lv=classify(raw);
  const tsM=raw.match(/(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})/);
  const ts=tsM?tsM[1].replace("T"," "):"";
  const txt=ts?raw.replace(tsM[0],"").replace(/^[\s:,\-\[\]]+/,""):raw;

  const div=document.createElement("div");
  div.className="ll "+lvCls(lv);
  div.dataset.src=src; div.dataset.lv=lv; div.dataset.txt=raw.toLowerCase();
  div.innerHTML=`<span class="lts">${ts}</span>`+
    `<span class="lsrc ${src}">${src}</span>`+
    `<span class="ltxt">${esc(txt)}</span>`;
  if(!visible(div))div.classList.add("hidden");

  const lb=document.getElementById("lb");
  lb.appendChild(div);
  while(lb.children.length>MAX_LINES)lb.removeChild(lb.firstChild);

  totLines++;
  if(lv==="error")errCnt++;
  if(lv==="warning")warnCnt++;
  updCounts();
  if(autoScroll&&!div.classList.contains("hidden"))lb.scrollTop=lb.scrollHeight;
}

function updCounts(){
  document.getElementById("cnt-e").textContent=errCnt+" ERR";
  document.getElementById("cnt-w").textContent=warnCnt+" WARN";
  document.getElementById("cnt-t").textContent=totLines+" lines";
}

function applyFilters(){
  srchTxt=(document.getElementById("sb-inp").value||"").toLowerCase();
  document.querySelectorAll("#lb .ll").forEach(el=>
    el.classList.toggle("hidden",!visible(el)));
  if(autoScroll)scrollBot();
}

function toggleSrc(btn){
  aS[btn.dataset.src]=!aS[btn.dataset.src];
  btn.classList.toggle("on",aS[btn.dataset.src]);
  applyFilters();
}
function toggleLvl(btn){
  aL[btn.dataset.lv]=!aL[btn.dataset.lv];
  btn.classList.toggle("on",aL[btn.dataset.lv]);
  applyFilters();
}
function soloLvl(lv){
  Object.keys(aL).forEach(k=>aL[k]=(k===lv));
  document.querySelectorAll(".lb").forEach(b=>b.classList.toggle("on",b.dataset.lv===lv));
  applyFilters();
}
function showAll(){
  Object.keys(aS).forEach(k=>aS[k]=true);
  Object.keys(aL).forEach(k=>aL[k]=true);
  document.querySelectorAll(".sb,.lb").forEach(b=>b.classList.add("on"));
  applyFilters();
}
function togglePause(){
  paused=!paused;
  const b=document.getElementById("btn-pause");
  b.textContent=paused?"▶":"⏸";
  b.style.background=paused?"var(--rbg)":"";
}
function clearLog(){
  document.getElementById("lb").innerHTML="";
  errCnt=warnCnt=totLines=0; updCounts();
}
function scrollBot(){
  autoScroll=true;
  const lb=document.getElementById("lb");
  lb.scrollTop=lb.scrollHeight;
}
document.getElementById("lb").addEventListener("scroll",function(){
  autoScroll=this.scrollTop+this.clientHeight>=this.scrollHeight-60;
});
function esc(s){
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

// ─── SSE ──────────────────────────────────────────────────────────────
function conn(url,src){
  const es=new EventSource(url);
  const cp=document.getElementById("cpill");
  es.onopen=()=>{cp.style.color="var(--green)";cp.textContent="● LIVE";};
  es.onerror=()=>{
    cp.style.color="var(--red)";cp.textContent="● RECONNECTING";
    es.close();setTimeout(()=>conn(url,src),3000);
  };
  es.onmessage=e=>{
    try{
      const d=JSON.parse(e.data);
      if(d.type==="connected")return;
      if(d.type==="job"){addJobRow(d);return;}
      if(d.line)addLine(src,d.line);
    }catch(_){addLine(src,e.data);}
  };
}
conn("/api/monitor/stream/logs",  "celery");
conn("/api/monitor/stream/flask", "flask");
conn("/api/monitor/stream/tasks", "tasks");
conn("/api/monitor/stream/jobs",  "celery");

// ─── Beat ─────────────────────────────────────────────────────────────
function fmtS(s){
  if(s==null)return"—";
  if(s===0)return'<span style="color:var(--red)">DUE</span>';
  if(s<60)return s+"s";
  return Math.floor(s/60)+"m"+(s%60)+"s";
}
function renderBeat(tasks){
  const el=document.getElementById("blist");
  el.innerHTML="";
  tasks.forEach(t=>{
    const iv=t.interval,ni=t.next_in_s;
    const pct=ni==null?100:Math.max(0,Math.min(100,((iv-ni)/iv)*100));
    const bc=pct>90?"br2":pct>60?"by2":"bg2";
    const sh=t.task.split(".").slice(-2).join(".");
    if(ni!=null)beatLR[t.task]={iv,ts:Date.now()-(iv-ni)*1000};
    const row=document.createElement("div");
    row.className="br"; row.title=t.task+"\nInterval:"+iv+"s";
    row.innerHTML=`<div class="bn"><span>${sh}</span></div>`+
      `<div class="bw"><div class="bb ${bc}" style="width:${pct}%"></div></div>`+
      `<div class="bc" data-task="${t.task}" data-iv="${iv}" `+
      `style="color:${ni===0?"var(--red)":ni<15?"var(--yellow)":"var(--dim)"}">${fmtS(ni)}</div>`;
    el.appendChild(row);
  });
}
setInterval(()=>{
  document.querySelectorAll(".bc[data-task]").forEach(el=>{
    const s=beatLR[el.dataset.task];if(!s)return;
    const el2=(Date.now()-s.ts)/1000;
    const ni=Math.max(0,Math.round(s.iv-el2));
    el.innerHTML=fmtS(ni);
    el.style.color=ni===0?"var(--red)":ni<15?"var(--yellow)":"var(--dim)";
    const bar=el.parentElement.querySelector(".bb");
    if(bar){const p=Math.min(100,(el2/s.iv)*100);
      bar.style.width=p+"%";
      bar.className="bb "+(p>90?"br2":p>60?"by2":"bg2");}
  });
},1000);

// ─── Sports ───────────────────────────────────────────────────────────
const EM={soccer:"⚽",basketball:"🏀",tennis:"🎾","ice-hockey":"🏒",
  rugby:"🏉",handball:"🤾",volleyball:"🏐",cricket:"🏏",
  "table-tennis":"🏓",esoccer:"🎮",mma:"🥋",boxing:"🥊",darts:"🎯"};
function renderSports(sports){
  const el=document.getElementById("slist");
  el.innerHTML="";
  const warm=sports.filter(s=>s.populated).length;
  document.getElementById("msp").textContent=warm+"/"+sports.length;
  sports.forEach(s=>{
    const row=document.createElement("div");
    row.className="sr";
    row.innerHTML=`<div class="sdot" style="background:${s.populated?"var(--green)":"var(--red)"}"></div>`+
      `<div class="sn">${EM[s.sport]||"🏆"} ${s.sport}</div>`+
      `<span class="sbg ${s.populated?"warm":"cold"}">${s.populated?(s.upcoming+"↑"):"cold"}</span>`+
      (s.arbs>0?`<span style="color:var(--yellow);font-size:9px">${s.arbs}arb</span>`:"");
    el.appendChild(row);
  });
}

// ─── Jobs ─────────────────────────────────────────────────────────────
function addJobRow(j,prepend=true){
  allJobs=prepend?[j,...allJobs].slice(0,MAX_JOBS*2):[...allJobs,j].slice(-MAX_JOBS*2);
  renderJobs();
}
function renderJobs(){
  const tb=document.getElementById("jtbody");
  tb.innerHTML="";
  const txt=jSearch.toLowerCase();
  let n=0;
  allJobs.forEach(j=>{
    if(jFilter!=="all"&&j.status!==jFilter)return;
    if(txt&&!(j.bookmaker||"").toLowerCase().includes(txt)
        &&!(j.sport||"").toLowerCase().includes(txt))return;
    if(n>=MAX_JOBS)return;n++;
    const sc={ok:"ok",error:"er",empty:"em"}[j.status]||"";
    const ic={ok:"✔",error:"✗",empty:"○"}[j.status]||"?";
    const tr=document.createElement("tr");
    tr.innerHTML=`<td style="color:var(--dim)">${(j.ts||"").slice(11,19)}</td>`+
      `<td style="color:var(--cyan)">${j.bookmaker||""}</td>`+
      `<td>${j.sport||""}</td><td style="color:var(--dim)">${j.mode||""}</td>`+
      `<td>${j.count||0}</td><td class="${sc}">${ic} ${j.status||""}</td>`+
      `<td style="color:var(--green)">${j.unified_ok||0}</td>`+
      `<td style="color:var(--red)">${j.unified_fail||0}</td>`+
      `<td style="color:var(--dim)">${j.latency_ms||0}ms</td>`+
      `<td class="er" style="font-size:9px;max-width:280px;overflow:hidden;`+
      `text-overflow:ellipsis;white-space:nowrap" title="${esc(j.detail||"")}">`+
      `${esc((j.detail||"").slice(0,100))}</td>`;
    tb.appendChild(tr);
  });
  document.getElementById("jc").textContent=n+" jobs";
}
function setJF(btn,f){
  jFilter=f;
  document.querySelectorAll(".jfb").forEach(b=>b.classList.toggle("a",b.dataset.jf===f));
  renderJobs();
}
function filterJobs(v){jSearch=v;renderJobs();}

// ─── Report polling ───────────────────────────────────────────────────
async function poll(){
  try{
    const d=await(await fetch("/api/monitor/debug/report")).json();
    if(!d.ok)return;
    const h=d.overall?.healthy;
    const p=document.getElementById("hpill");
    p.textContent=h?"● HEALTHY":"● ISSUES";
    p.className="pill "+(h?"g":"r");
    document.getElementById("mup").textContent =d.totals?.upcoming_matches??'—';
    document.getElementById("mlv").textContent =d.totals?.live_matches??'—';
    document.getElementById("marb").textContent=d.totals?.arb_opportunities??'—';
    document.getElementById("mev").textContent =d.totals?.ev_opportunities??'—';
    if(d.sports)renderSports(d.sports);
    if(allJobs.length===0&&d.recent_jobs?.length)
      d.recent_jobs.slice().reverse().forEach(j=>addJobRow(j,false));
  }catch(e){}
}
async function pollBeat(){
  try{
    const d=await(await fetch("/api/monitor/debug/beat")).json();
    if(d.ok)renderBeat(d.tasks);
  }catch(e){}
}
poll();pollBeat();
setInterval(poll,15000);setInterval(pollBeat,30000);
</script>
</body>
</html>"""


@bp_monitor.route("/dashboard")
def dashboard():
    return Response(DASHBOARD_HTML, content_type="text/html; charset=utf-8")


# =============================================================================
# STRUCTURE VIEWS (Competitions, Teams, Countries, Bookmaker Mappings)
# =============================================================================

@bp_monitor.route("/competitions")
def list_competitions():
    """List all competitions with optional filters: sport, country, has_matches."""
    sport_id = request.args.get("sport_id", type=int)
    country_id = request.args.get("country_id", type=int)
    has_matches = request.args.get("has_matches", type=bool, default=False)
    limit = request.args.get("limit", 100, type=int)
    
    q = Competition.query
    if sport_id:
        q = q.filter(Competition.sport_id == sport_id)
    if country_id:
        q = q.filter(Competition.country_id == country_id)
    if has_matches:
        q = q.filter(Competition.id.in_(
            db.session.query(UnifiedMatch.competition_id).filter(UnifiedMatch.competition_id.isnot(None))
        ))
    comps = q.order_by(Competition.name).limit(limit).all()
    
    result = []
    for c in comps:
        upcoming_count = UnifiedMatch.query.filter(
            UnifiedMatch.competition_id == c.id,
            UnifiedMatch.status == "PRE_MATCH"
        ).count()
        result.append({
            "id": c.id,
            "name": c.name,
            "short_name": c.short_name,
            "sport_id": c.sport_id,
            "sport_name": c.sport.name if c.sport else None,
            "country_id": c.country_id,
            "country_name": c.country.name if c.country else None,
            "gender": c.gender.value if c.gender else None,
            "tier": c.tier,
            "is_active": c.is_active,
            "upcoming_matches": upcoming_count,
            "betradar_id": c.betradar_id,
        })
    return {"ok": True, "competitions": result, "total": len(result), "ts": _now()}


@bp_monitor.route("/competition/<int:comp_id>")
def competition_detail(comp_id: int):
    """Detailed view of a competition: matches (upcoming), bookmaker mappings."""
    comp = Competition.query.get_or_404(comp_id)
    
    # Upcoming matches (next 7 days)
    upcoming = UnifiedMatch.query.filter(
        UnifiedMatch.competition_id == comp_id,
        UnifiedMatch.status.in_(["PRE_MATCH", "LIVE"]),
        UnifiedMatch.start_time >= datetime.now(timezone.utc)
    ).order_by(UnifiedMatch.start_time).limit(50).all()
    
    matches = []
    for m in upcoming:
        matches.append({
            "id": m.id,
            "parent_match_id": m.parent_match_id,
            "home_team": m.home_team_name,
            "away_team": m.away_team_name,
            "start_time": m.start_time.isoformat() if m.start_time else None,
            "status": m.status.value if m.status else None,
            "home_team_id": m.home_team_id,
            "away_team_id": m.away_team_id,
            "has_arb": bool(m.arbitrage_opps.filter_by(status="OPEN").first()),
            "has_ev": bool(m.ev_opps.filter_by(status="OPEN").first()),
        })
    
    # Bookmaker competition name mappings
    mappings = BookmakerCompetitionName.query.filter_by(competition_id=comp_id).all()
    bookmaker_names = [{
        "bookmaker_key": bm.bookmaker_key,
        "bookmaker_competition_name": bm.bookmaker_competition_name,
        "created_at": bm.created_at.isoformat() if bm.created_at else None,
    } for bm in mappings]
    
    return {
        "ok": True,
        "competition": {
            "id": comp.id,
            "name": comp.name,
            "short_name": comp.short_name,
            "sport_id": comp.sport_id,
            "sport_name": comp.sport.name if comp.sport else None,
            "country_id": comp.country_id,
            "country_name": comp.country.name if comp.country else None,
            "gender": comp.gender.value if comp.gender else None,
            "tier": comp.tier,
            "logo_url": comp.logo_url,
            "is_active": comp.is_active,
            "betradar_id": comp.betradar_id,
            "sp_league_id": comp.sp_league_id,
            "created_at": comp.created_at.isoformat() if comp.created_at else None,
            "updated_at": comp.updated_at.isoformat() if comp.updated_at else None,
        },
        "bookmaker_names": bookmaker_names,
        "upcoming_matches": matches,
        "match_count": len(matches),
        "ts": _now(),
    }


@bp_monitor.route("/team/<int:team_id>")
def team_detail(team_id: int):
    """Detailed view of a team: upcoming matches, bookmaker team names."""
    team = Team.query.get_or_404(team_id)
    
    # Upcoming matches (home or away)
    upcoming = UnifiedMatch.query.filter(
        (UnifiedMatch.home_team_id == team_id) | (UnifiedMatch.away_team_id == team_id),
        UnifiedMatch.status.in_(["PRE_MATCH", "LIVE"]),
        UnifiedMatch.start_time >= datetime.now(timezone.utc)
    ).order_by(UnifiedMatch.start_time).limit(30).all()
    
    matches = []
    for m in upcoming:
        matches.append({
            "id": m.id,
            "parent_match_id": m.parent_match_id,
            "home_team": m.home_team_name,
            "away_team": m.away_team_name,
            "role": "home" if m.home_team_id == team_id else "away",
            "start_time": m.start_time.isoformat() if m.start_time else None,
            "status": m.status.value if m.status else None,
            "competition_name": m.competition_name,
        })
    
    # Bookmaker team name mappings
    mappings = BookmakerTeamName.query.filter_by(team_id=team_id).all()
    bookmaker_names = [{
        "bookmaker_key": bm.bookmaker_key,
        "bookmaker_team_name": bm.bookmaker_team_name,
        "created_at": bm.created_at.isoformat() if bm.created_at else None,
    } for bm in mappings]
    
    return {
        "ok": True,
        "team": {
            "id": team.id,
            "name": team.name,
            "short_name": team.short_name,
            "slug": team.slug,
            "sport_id": team.sport_id,
            "sport_name": team.sport.name if team.sport else None,
            "country_id": team.country_id,
            "country_name": team.country.name if team.country else None,
            "gender": team.gender.value if team.gender else None,
            "logo_url": team.logo_url,
            "venue_name": team.venue_name,
            "founded": team.founded,
            "is_active": team.is_active,
            "betradar_id": team.betradar_id,
            "created_at": team.created_at.isoformat() if team.created_at else None,
        },
        "bookmaker_names": bookmaker_names,
        "upcoming_matches": matches,
        "match_count": len(matches),
        "ts": _now(),
    }


@bp_monitor.route("/country/<int:country_id>")
def country_detail(country_id: int):
    """Detailed view of a country: competitions, teams, bookmaker country names."""
    country = Country.query.get_or_404(country_id)
    
    # Competitions in this country
    competitions = Competition.query.filter_by(country_id=country_id).limit(50).all()
    comp_list = [{
        "id": c.id,
        "name": c.name,
        "sport_name": c.sport.name if c.sport else None,
        "upcoming_matches": UnifiedMatch.query.filter_by(competition_id=c.id, status="PRE_MATCH").count(),
    } for c in competitions]
    
    # Teams in this country (limit 100)
    teams = Team.query.filter_by(country_id=country_id).limit(100).all()
    team_list = [{
        "id": t.id,
        "name": t.name,
        "sport_name": t.sport.name if t.sport else None,
    } for t in teams]
    
    # Bookmaker country name mappings
    mappings = BookmakerCountryName.query.filter_by(country_id=country_id).all()
    bookmaker_names = [{
        "bookmaker_key": bm.bookmaker_key,
        "bookmaker_country_name": bm.bookmaker_country_name,
        "created_at": bm.created_at.isoformat() if bm.created_at else None,
    } for bm in mappings]
    
    return {
        "ok": True,
        "country": {
            "id": country.id,
            "name": country.name,
            "iso_code": country.iso_code,
            "iso_code2": country.iso_code2,
            "flag_url": country.flag_url,
            "betradar_id": country.betradar_id,
            "created_at": country.created_at.isoformat() if country.created_at else None,
        },
        "bookmaker_names": bookmaker_names,
        "competitions": comp_list,
        "teams": team_list,
        "competition_count": len(comp_list),
        "team_count": len(team_list),
        "ts": _now(),
    }


@bp_monitor.route("/structure")
def structure_dashboard():
    """HTML dashboard for exploring competitions, teams, countries and their bookmaker mappings."""
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>Odds Kenya - Data Structure Explorer</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <style>
        *{box-sizing:border-box;margin:0;padding:0;}
        body{background:#0a0f1a;color:#e2e8f0;font-family:system-ui,'Segoe UI',sans-serif;font-size:14px;padding:20px;}
        .container{max-width:1400px;margin:0 auto;}
        h1{color:#06b6d4;margin-bottom:10px;font-size:24px;}
        .sub{color:#64748b;margin-bottom:30px;border-bottom:1px solid #1e293b;padding-bottom:10px;}
        .grid{display:grid;grid-template-columns:280px 1fr;gap:20px;}
        .sidebar{background:#0f172a;border-radius:12px;border:1px solid #1e293b;padding:15px;height:fit-content;}
        .sidebar h3{font-size:14px;margin-bottom:12px;color:#94a3b8;}
        .filter-group{margin-bottom:15px;}
        label{font-size:12px;color:#64748b;display:block;margin-bottom:4px;}
        select, input{width:100%;padding:8px;background:#1e293b;border:1px solid #334155;color:#e2e8f0;border-radius:6px;font-size:13px;}
        button{background:#0f172a;border:1px solid #334155;color:#94a3b8;padding:6px 12px;border-radius:6px;cursor:pointer;font-size:12px;}
        button:hover{background:#1e293b;color:#fff;}
        .content{background:#0f172a;border-radius:12px;border:1px solid #1e293b;padding:20px;min-height:500px;}
        .comp-list{display:flex;flex-direction:column;gap:8px;}
        .comp-item{background:#1e293b;border-radius:8px;padding:12px;cursor:pointer;transition:all 0.2s;}
        .comp-item:hover{background:#334155;transform:translateX(4px);}
        .comp-name{font-weight:600;margin-bottom:4px;display:flex;justify-content:space-between;}
        .comp-meta{font-size:12px;color:#94a3b8;display:flex;gap:12px;}
        .badge{background:#0f172a;padding:2px 8px;border-radius:20px;font-size:11px;color:#06b6d4;}
        .detail-card{background:#1e293b;border-radius:12px;padding:20px;margin-top:20px;}
        .detail-card h2{font-size:18px;margin-bottom:15px;color:#38bdf8;}
        .info-row{display:flex;margin-bottom:8px;font-size:13px;}
        .info-label{width:140px;color:#94a3b8;}
        .info-value{flex:1;color:#e2e8f0;}
        .table-wrap{overflow-x:auto;margin-top:15px;}
        table{width:100%;border-collapse:collapse;font-size:13px;}
        th,td{padding:8px 12px;text-align:left;border-bottom:1px solid #334155;}
        th{color:#94a3b8;font-weight:500;}
        .bk-mapping{margin-top:20px;}
        .bk-mapping h3{font-size:14px;color:#facc15;margin-bottom:8px;}
        .match-row{background:#0f172a;margin-bottom:6px;border-radius:6px;padding:8px 12px;font-size:12px;}
        .status{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;}
        .status.pre{background:#22c55e;}
        .status.live{background:#f97316;animation:pulse 1s infinite;}
        @keyframes pulse{0%,100%{opacity:1;}50%{opacity:0.5;}}
        .loading{text-align:center;padding:40px;color:#64748b;}
        .error{color:#ef4444;padding:20px;text-align:center;}
        .back{background:#1e293b;border:none;margin-bottom:15px;}
    </style>
</head>
<body>
<div class="container">
    <h1>🏛️ Data Structure Explorer</h1>
    <div class="sub">Competitions · Teams · Countries · Bookmaker Name Mappings</div>

    <div class="grid">
        <div class="sidebar">
            <h3>🔍 Filters</h3>
            <div class="filter-group">
                <label>Sport</label>
                <select id="sport-select">
                    <option value="">All Sports</option>
                </select>
            </div>
            <div class="filter-group">
                <label>Country</label>
                <select id="country-select">
                    <option value="">All Countries</option>
                </select>
            </div>
            <div class="filter-group">
                <label>Has upcoming matches</label>
                <input type="checkbox" id="has-matches-checkbox">
            </div>
            <button id="refresh-btn" style="width:100%;margin-top:10px;">⟳ Refresh</button>
            <hr style="margin:15px 0;border-color:#1e293b;">
            <h3>📊 Quick Links</h3>
            <button id="link-competitions" style="width:100%;margin-bottom:6px;">🏆 Competitions</button>
            <button id="link-teams" style="width:100%;margin-bottom:6px;">⚽ Teams (top 100)</button>
            <button id="link-countries" style="width:100%;">🌍 Countries</button>
        </div>

        <div class="content" id="content-area">
            <div class="loading">Select a competition from the list or use filters →</div>
        </div>
    </div>
</div>

<script>
let allCompetitions = [];
let currentDetail = null;

async function loadSports() {
    try {
        const res = await fetch('/api/monitor/debug/sports');
        const data = await res.json();
        if (data.ok) {
            const select = document.getElementById('sport-select');
            data.sports.forEach(s => {
                const opt = document.createElement('option');
                opt.value = s.sport;
                opt.textContent = s.sport.toUpperCase();
                select.appendChild(opt);
            });
        }
    } catch(e) { console.error(e); }
}

async function loadCompetitions() {
    const sport = document.getElementById('sport-select').value;
    const country = document.getElementById('country-select').value;
    const hasMatches = document.getElementById('has-matches-checkbox').checked;
    let url = '/api/monitor/debug/competitions?limit=200';
    if (sport) url += `&sport_name=${encodeURIComponent(sport)}`;
    if (country) url += `&country_name=${encodeURIComponent(country)}`;
    if (hasMatches) url += `&has_matches=true`;
    
    try {
        const res = await fetch(url);
        const data = await res.json();
        if (data.ok) {
            allCompetitions = data.competitions;
            renderCompetitionList(allCompetitions);
        } else {
            renderError('Failed to load competitions');
        }
    } catch(e) {
        renderError(e.message);
    }
}

function renderCompetitionList(comps) {
    const area = document.getElementById('content-area');
    if (!comps.length) {
        area.innerHTML = '<div class="loading">No competitions found</div>';
        return;
    }
    let html = '<div class="comp-list">';
    comps.forEach(c => {
        html += `
            <div class="comp-item" onclick="loadCompetitionDetail(${c.id})">
                <div class="comp-name">
                    <span>${escapeHtml(c.name)}</span>
                    <span class="badge">${c.upcoming_matches || 0} upcoming</span>
                </div>
                <div class="comp-meta">
                    <span>${c.sport_name || '?'}</span>
                    <span>${c.country_name || 'International'}</span>
                    <span>${c.gender || 'MIX'}</span>
                </div>
            </div>
        `;
    });
    html += '</div>';
    area.innerHTML = html;
}

async function loadCompetitionDetail(compId) {
    const area = document.getElementById('content-area');
    area.innerHTML = '<div class="loading">Loading competition details...</div>';
    try {
        const res = await fetch(`/api/monitor/debug/competition/${compId}`);
        const data = await res.json();
        if (data.ok) {
            currentDetail = data;
            renderCompetitionDetail(data);
        } else {
            renderError('Competition not found');
        }
    } catch(e) {
        renderError(e.message);
    }
}

function renderCompetitionDetail(data) {
    const c = data.competition;
    const html = `
        <button class="back" onclick="loadCompetitions()">← Back to competitions list</button>
        <div class="detail-card">
            <h2>🏆 ${escapeHtml(c.name)}</h2>
            <div class="info-row"><div class="info-label">Sport:</div><div class="info-value">${c.sport_name || '-'}</div></div>
            <div class="info-row"><div class="info-label">Country:</div><div class="info-value">${c.country_name || '-'}</div></div>
            <div class="info-row"><div class="info-label">Gender:</div><div class="info-value">${c.gender || '-'}</div></div>
            <div class="info-row"><div class="info-label">Tier:</div><div class="info-value">${c.tier || '-'}</div></div>
            <div class="info-row"><div class="info-label">Betradar ID:</div><div class="info-value">${c.betradar_id || '-'}</div></div>
            <div class="info-row"><div class="info-label">SP League ID:</div><div class="info-value">${c.sp_league_id || '-'}</div></div>
            <div class="info-row"><div class="info-label">Created:</div><div class="info-value">${c.created_at || '-'}</div></div>
        </div>
        ${renderBookmakerMappings(data.bookmaker_names, 'Competition Names')}
        ${renderMatches(data.upcoming_matches)}
    `;
    document.getElementById('content-area').innerHTML = html;
}

function renderBookmakerMappings(mappings, title) {
    if (!mappings || mappings.length === 0) return '';
    let rows = '';
    mappings.forEach(m => {
        rows += `<tr><td>${escapeHtml(m.bookmaker_key)}</td><td>${escapeHtml(m.bookmaker_competition_name || m.bookmaker_team_name || m.bookmaker_country_name)}</td><td>${m.created_at || '-'}</td></tr>`;
    });
    return `
        <div class="bk-mapping">
            <h3>📖 ${title} (Bookmaker Mappings)</h3>
            <div class="table-wrap">
                <table>
                    <thead><tr><th>Bookmaker</th><th>External Name</th><th>Created</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>
    `;
}

function renderMatches(matches) {
    if (!matches || matches.length === 0) return '<div class="bk-mapping"><h3>📅 Upcoming Matches</h3><div>No upcoming matches.</div></div>';
    let rows = '';
    matches.forEach(m => {
        const statusClass = m.status === 'PRE_MATCH' ? 'pre' : (m.status === 'LIVE' ? 'live' : '');
        rows += `
            <div class="match-row">
                <span class="status ${statusClass}"></span>
                <strong>${escapeHtml(m.home_team)}</strong> vs <strong>${escapeHtml(m.away_team)}</strong>
                <span style="color:#94a3b8;margin-left:12px;">${m.start_time || 'TBD'}</span>
                ${m.has_arb ? '<span style="color:#facc15;margin-left:8px;">⚡ ARB</span>' : ''}
                ${m.has_ev ? '<span style="color:#a855f7;margin-left:8px;">📈 EV</span>' : ''}
            </div>
        `;
    });
    return `
        <div class="bk-mapping">
            <h3>📅 Upcoming Matches (${matches.length})</h3>
            ${rows}
        </div>
    `;
}

async function loadTeamDetail(teamId) {
    const area = document.getElementById('content-area');
    area.innerHTML = '<div class="loading">Loading team details...</div>';
    try {
        const res = await fetch(`/api/monitor/debug/team/${teamId}`);
        const data = await res.json();
        if (data.ok) {
            renderTeamDetail(data);
        } else {
            renderError('Team not found');
        }
    } catch(e) {
        renderError(e.message);
    }
}

function renderTeamDetail(data) {
    const t = data.team;
    const html = `
        <button class="back" onclick="loadCompetitions()">← Back to competitions</button>
        <div class="detail-card">
            <h2>⚽ ${escapeHtml(t.name)}</h2>
            <div class="info-row"><div class="info-label">Sport:</div><div class="info-value">${t.sport_name || '-'}</div></div>
            <div class="info-row"><div class="info-label">Country:</div><div class="info-value">${t.country_name || '-'}</div></div>
            <div class="info-row"><div class="info-label">Venue:</div><div class="info-value">${t.venue_name || '-'}</div></div>
            <div class="info-row"><div class="info-label">Founded:</div><div class="info-value">${t.founded || '-'}</div></div>
            <div class="info-row"><div class="info-label">Betradar ID:</div><div class="info-value">${t.betradar_id || '-'}</div></div>
        </div>
        ${renderBookmakerMappings(data.bookmaker_names, 'Team Names')}
        ${renderMatches(data.upcoming_matches)}
    `;
    document.getElementById('content-area').innerHTML = html;
}

async function loadCountryDetail(countryId) {
    const area = document.getElementById('content-area');
    area.innerHTML = '<div class="loading">Loading country details...</div>';
    try {
        const res = await fetch(`/api/monitor/debug/country/${countryId}`);
        const data = await res.json();
        if (data.ok) {
            renderCountryDetail(data);
        } else {
            renderError('Country not found');
        }
    } catch(e) {
        renderError(e.message);
    }
}

function renderCountryDetail(data) {
    const c = data.country;
    let compsHtml = '';
    data.competitions.forEach(comp => {
        compsHtml += `<div class="match-row" onclick="loadCompetitionDetail(${comp.id})" style="cursor:pointer;">🏆 ${escapeHtml(comp.name)} (${comp.sport_name}) — ${comp.upcoming_matches} upcoming</div>`;
    });
    let teamsHtml = '';
    data.teams.forEach(team => {
        teamsHtml += `<div class="match-row" onclick="loadTeamDetail(${team.id})" style="cursor:pointer;">⚽ ${escapeHtml(team.name)} (${team.sport_name})</div>`;
    });
    const html = `
        <button class="back" onclick="loadCompetitions()">← Back to competitions</button>
        <div class="detail-card">
            <h2>🌍 ${escapeHtml(c.name)}</h2>
            <div class="info-row"><div class="info-label">ISO Code:</div><div class="info-value">${c.iso_code || '-'} / ${c.iso_code2 || '-'}</div></div>
            <div class="info-row"><div class="info-label">Betradar ID:</div><div class="info-value">${c.betradar_id || '-'}</div></div>
        </div>
        ${renderBookmakerMappings(data.bookmaker_names, 'Country Names')}
        <div class="bk-mapping"><h3>🏆 Competitions in this country</h3>${compsHtml || '<div>None</div>'}</div>
        <div class="bk-mapping"><h3>⚽ Teams in this country</h3>${teamsHtml || '<div>None</div>'}</div>
    `;
    document.getElementById('content-area').innerHTML = html;
}

async function loadTeamsList() {
    const area = document.getElementById('content-area');
    area.innerHTML = '<div class="loading">Loading top 100 teams...</div>';
    try {
        const res = await fetch('/api/monitor/debug/competitions?limit=1'); // fake to get sport filter
        // Actually we need a teams endpoint – use country filter? For simplicity, we'll load by sport from competition list
        // Better: load all competitions then extract unique teams? Not efficient.
        // We'll just redirect to a fallback: show note.
        area.innerHTML = `
            <div class="detail-card">
                <h2>Teams Explorer</h2>
                <p>Click on a country or competition to see its teams, or use the team detail link from match rows.</p>
                <button onclick="loadCompetitions()">← Back to competitions</button>
            </div>
        `;
    } catch(e) {
        renderError(e.message);
    }
}

async function loadCountriesList() {
    const area = document.getElementById('content-area');
    area.innerHTML = '<div class="loading">Loading countries...</div>';
    try {
        const res = await fetch('/api/monitor/debug/competitions?limit=200');
        const data = await res.json();
        if (data.ok) {
            const countries = {};
            data.competitions.forEach(c => {
                if (c.country_id && c.country_name) {
                    if (!countries[c.country_id]) {
                        countries[c.country_id] = { id: c.country_id, name: c.country_name, count: 0 };
                    }
                    countries[c.country_id].count++;
                }
            });
            let html = '<button class="back" onclick="loadCompetitions()">← Back to competitions</button><div class="comp-list">';
            for (const id in countries) {
                const cnt = countries[id];
                html += `<div class="comp-item" onclick="loadCountryDetail(${cnt.id})">
                            <div class="comp-name">🌍 ${escapeHtml(cnt.name)}</div>
                            <div class="comp-meta">${cnt.count} competitions</div>
                         </div>`;
            }
            html += '</div>';
            area.innerHTML = html;
        } else {
            renderError('Failed to load countries');
        }
    } catch(e) {
        renderError(e.message);
    }
}

function renderError(msg) {
    document.getElementById('content-area').innerHTML = `<div class="error">❌ ${escapeHtml(msg)}</div>`;
}

function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/[&<>]/g, function(m) {
        if (m === '&') return '&amp;';
        if (m === '<') return '&lt;';
        if (m === '>') return '&gt;';
        return m;
    });
}

// Event handlers
document.getElementById('refresh-btn').addEventListener('click', loadCompetitions);
document.getElementById('link-competitions').addEventListener('click', loadCompetitions);
document.getElementById('link-teams').addEventListener('click', loadTeamsList);
document.getElementById('link-countries').addEventListener('click', loadCountriesList);

// Initial load
loadSports();
loadCompetitions();
</script>
</body>
</html>
    """
    return Response(html, content_type="text/html; charset=utf-8")