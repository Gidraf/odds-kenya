"""
app/views/odds_feed/monitor_view.py
====================================
Real-time monitoring dashboard — logs, beat schedule, sport cache, job log.

Register in create_app():
    from app.views.odds_feed.monitor_view import bp_monitor
    app.register_blueprint(bp_monitor)

Endpoints
─────────
  GET /api/monitor/dashboard        ← Beautiful HTML dashboard (open in browser)
  GET /api/monitor/stream/logs      ← SSE: real-time celery.log tail
  GET /api/monitor/stream/jobs      ← SSE: real-time job log tail
  GET /api/monitor/report           ← JSON: latest health report
  GET /api/monitor/report/history   ← JSON: last 60 snapshots
  GET /api/monitor/jobs             ← JSON: last N job entries (filterable)
  GET /api/monitor/sports           ← JSON: per-sport cache summary
  GET /api/monitor/beat             ← JSON: beat schedule + next-run countdowns
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from flask import Blueprint, Response, request, stream_with_context

bp_monitor = Blueprint("monitor", __name__, url_prefix="/api/monitor")

LOG_DIR       = Path(os.environ.get("LOG_DIR", "logs"))
CELERY_LOG    = LOG_DIR / "celery.log"
TASK_LOG      = LOG_DIR / "tasks.log"
JOB_LOG       = LOG_DIR / "harvest_jobs.log"

# Beat schedule: {task_name: interval_seconds}
BEAT_SCHEDULE: dict[str, int] = {
    "tasks.sp.harvest_all_upcoming":      300,
    "tasks.bt.harvest_all_upcoming":      360,
    "tasks.od.harvest_all_upcoming":      420,
    "tasks.b2b.harvest_all_upcoming":     480,
    "tasks.b2b_page.harvest_all_upcoming": 300,
    "tasks.sbo.harvest_all_upcoming":     180,
    "tasks.sp.harvest_all_live":           60,
    "tasks.sp.poll_all_event_details":      5,
    "tasks.bt.harvest_all_live":           90,
    "tasks.od.harvest_all_live":           90,
    "tasks.b2b.harvest_all_live":         120,
    "tasks.b2b_page.harvest_all_live":     30,
    "tasks.sbo.harvest_all_live":          60,
    "tasks.ops.health_check":              30,
    "tasks.ops.update_match_results":     300,
    "tasks.ops.persist_all_sports":       300,
    "tasks.ops.build_health_report":       60,
    "tasks.ops.expire_subscriptions":    3600,
    "tasks.ops.cache_finished_games":    3600,
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
# JSON API endpoints
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
        u = _cache_get(f"combined:upcoming:{sport}") or {}
        l = _cache_get(f"combined:live:{sport}")     or {}
        u_m = u.get("matches", [])
        l_m = l.get("matches", [])
        summary.append({
            "sport": sport,
            "upcoming": len(u_m), "live": len(l_m),
            "arbs": sum(1 for m in u_m + l_m if m.get("has_arb")),
            "evs":  sum(1 for m in u_m + l_m if m.get("has_ev")),
            "bk_counts_up": u.get("bk_counts", {}),
            "harvested_at_up": u.get("harvested_at"),
            "harvested_at_lv": l.get("harvested_at"),
            "populated": bool(u_m or l_m),
        })
    populated = sum(1 for s in summary if s["populated"])
    return {"ok": True, "total_sports": len(PERSIST_SPORTS),
            "populated_sports": populated, "sports": summary, "ts": _now()}


@bp_monitor.route("/beat")
def get_beat():
    """Returns beat schedule with last-run time and next-run countdown."""
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
        last_dt  = None
        next_in  = None
        if last_str:
            try:
                last_dt = datetime.fromisoformat(last_str.replace("Z", "+00:00"))
                elapsed = (now_utc - last_dt).total_seconds()
                next_in = max(0, int(interval - elapsed))
            except Exception:
                pass
        tasks.append({
            "task":      task_name,
            "interval":  interval,
            "last_run":  last_str,
            "next_in_s": next_in,
            "overdue":   (next_in is not None and next_in == 0),
        })

    return {"ok": True, "tasks": tasks, "ts": _now()}


# ─────────────────────────────────────────────────────────────────────────────
# SSE: real-time log tail
# ─────────────────────────────────────────────────────────────────────────────

def _tail_file_sse(filepath: Path, event_type: str = "log"):
    """Generator: tails a file and yields SSE events for new lines."""
    filepath.touch(exist_ok=True)

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        # Seek to end — don't replay old history
        f.seek(0, 2)
        last_ka = time.monotonic()

        while True:
            line = f.readline()
            if line:
                line = line.rstrip("\n")
                if line:
                    yield f"data: {json.dumps({'type': event_type, 'line': line, 'ts': _now()})}\n\n"
            else:
                time.sleep(0.2)
                if time.monotonic() - last_ka >= 10:
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
    """SSE: real-time tail of logs/celery.log"""
    @stream_with_context
    def generate():
        yield f"data: {json.dumps({'type': 'connected', 'file': str(CELERY_LOG), 'ts': _now()})}\n\n"
        yield from _tail_file_sse(CELERY_LOG, "log")
    return Response(generate(), headers=_SSE_HEADERS)


@bp_monitor.route("/stream/tasks")
def stream_task_logs():
    """SSE: real-time tail of logs/tasks.log"""
    @stream_with_context
    def generate():
        yield f"data: {json.dumps({'type': 'connected', 'file': str(TASK_LOG), 'ts': _now()})}\n\n"
        yield from _tail_file_sse(TASK_LOG, "task")
    return Response(generate(), headers=_SSE_HEADERS)


@bp_monitor.route("/stream/jobs")
def stream_jobs():
    """SSE: pushes new job log entries from Redis in real-time."""
    @stream_with_context
    def generate():
        r = _redis()
        if not r:
            yield f"data: {json.dumps({'error': 'Redis unavailable'})}\n\n"
            return
        yield f"data: {json.dumps({'type': 'connected', 'ts': _now()})}\n\n"
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
                            yield f"data: {json.dumps({'type': 'job', **json.loads(raw)})}\n\n"
                        except Exception:
                            pass
                    last_len = cur
            except Exception:
                pass
            if time.monotonic() - last_ka >= 10:
                yield f": keep-alive {_now()}\n\n"
                last_ka = time.monotonic()
    return Response(generate(), headers=_SSE_HEADERS)


# ─────────────────────────────────────────────────────────────────────────────
# HTML Dashboard
# ─────────────────────────────────────────────────────────────────────────────

@bp_monitor.route("/dashboard")
def dashboard():
    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Odds Kenya — Dev Monitor</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  :root {
    --bg:      #0a0f1e;
    --panel:   #0f172a;
    --border:  #1e293b;
    --text:    #e2e8f0;
    --dim:     #64748b;
    --green:   #22c55e;
    --red:     #ef4444;
    --yellow:  #f59e0b;
    --blue:    #3b82f6;
    --purple:  #a855f7;
    --cyan:    #06b6d4;
    --orange:  #f97316;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg); color: var(--text);
    font-family: 'JetBrains Mono', 'Fira Code', 'Consolas', monospace;
    font-size: 13px; overflow: hidden; height: 100vh;
    display: flex; flex-direction: column;
  }

  /* ── Top bar ─────────────────────────────────────────────────────── */
  #topbar {
    background: var(--panel);
    border-bottom: 1px solid var(--border);
    padding: 10px 16px;
    display: flex; align-items: center; gap: 16px; flex-shrink: 0;
  }
  #topbar h1 { font-size: 15px; color: var(--cyan); letter-spacing: 1px; }
  .pill {
    padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: bold;
  }
  .pill.green  { background: #052e16; color: var(--green); border: 1px solid var(--green); }
  .pill.red    { background: #450a0a; color: var(--red);   border: 1px solid var(--red);   }
  .pill.yellow { background: #422006; color: var(--yellow);border: 1px solid var(--yellow);}
  #clock       { margin-left: auto; color: var(--dim); font-size: 12px; }
  .metric      { text-align: center; min-width: 80px; }
  .metric .val { font-size: 20px; font-weight: bold; }
  .metric .lbl { font-size: 10px; color: var(--dim); text-transform: uppercase; margin-top: 2px; }

  /* ── Main layout ─────────────────────────────────────────────────── */
  #main {
    display: grid;
    grid-template-columns: 260px 1fr 300px;
    grid-template-rows: 1fr 220px;
    gap: 1px; background: var(--border);
    flex: 1; overflow: hidden;
  }
  .panel {
    background: var(--panel); overflow: hidden;
    display: flex; flex-direction: column;
  }
  .panel-title {
    padding: 8px 12px; font-size: 11px; font-weight: bold;
    color: var(--dim); text-transform: uppercase; letter-spacing: 1px;
    border-bottom: 1px solid var(--border); flex-shrink: 0;
    display: flex; align-items: center; gap: 8px;
  }
  .panel-title .dot {
    width: 7px; height: 7px; border-radius: 50%;
    background: var(--green); animation: pulse 2s infinite;
  }
  .panel-body { flex: 1; overflow-y: auto; padding: 8px; }

  @keyframes pulse {
    0%, 100% { opacity: 1; } 50% { opacity: 0.3; }
  }

  /* ── Beat schedule ───────────────────────────────────────────────── */
  .beat-row {
    display: flex; align-items: center; gap: 8px;
    padding: 5px 4px; border-radius: 4px; margin-bottom: 2px;
  }
  .beat-row:hover { background: var(--border); }
  .beat-name  { flex: 1; font-size: 11px; color: var(--dim); overflow: hidden;
                text-overflow: ellipsis; white-space: nowrap; }
  .beat-name span { color: var(--text); }
  .beat-bar-wrap { width: 70px; height: 4px; background: var(--border); border-radius: 2px; }
  .beat-bar  { height: 100%; border-radius: 2px; transition: width 1s linear; }
  .beat-cntd { font-size: 11px; width: 36px; text-align: right; }
  .bar-green  { background: var(--green); }
  .bar-yellow { background: var(--yellow); }
  .bar-red    { background: var(--red); }

  /* ── Log panel ───────────────────────────────────────────────────── */
  #log-panel { grid-column: 2; grid-row: 1; }
  #log-tabs  {
    display: flex; gap: 1px; background: var(--border);
    flex-shrink: 0; border-bottom: 1px solid var(--border);
  }
  .tab-btn {
    padding: 7px 14px; font-size: 11px; cursor: pointer; font-family: inherit;
    background: var(--panel); color: var(--dim); border: none;
    border-bottom: 2px solid transparent;
  }
  .tab-btn.active { color: var(--cyan); border-bottom-color: var(--cyan); }
  #log-body {
    flex: 1; overflow-y: auto; padding: 8px 12px;
    font-size: 12px; line-height: 1.7;
  }
  .log-line { display: flex; gap: 8px; }
  .log-line:hover { background: rgba(255,255,255,0.03); border-radius: 3px; }
  .log-ts   { color: var(--dim); white-space: nowrap; flex-shrink: 0; }
  .log-text { flex: 1; word-break: break-all; }

  /* log level colours */
  .lvl-DEBUG    { color: #475569; }
  .lvl-INFO     { color: var(--text); }
  .lvl-WARNING  { color: var(--yellow); }
  .lvl-ERROR    { color: var(--red); }
  .lvl-CRITICAL { color: var(--red); font-weight: bold; }
  .lvl-SUCCESS  { color: var(--green); }
  .lvl-BEAT     { color: var(--purple); }
  .lvl-TASK     { color: var(--cyan); }
  .lvl-STARTUP  { color: var(--orange); }

  /* ── Sports panel ────────────────────────────────────────────────── */
  .sport-row {
    display: flex; align-items: center; gap: 6px;
    padding: 5px 4px; border-radius: 4px; margin-bottom: 2px;
  }
  .sport-row:hover { background: var(--border); }
  .sport-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
  .sport-name { flex: 1; font-size: 12px; }
  .sport-badge {
    font-size: 10px; padding: 1px 6px; border-radius: 10px;
    background: var(--border);
  }
  .sport-badge.warm { color: var(--green); }
  .sport-badge.cold { color: var(--red);   }

  /* ── Jobs panel (bottom) ─────────────────────────────────────────── */
  #jobs-panel { grid-column: 1 / -1; grid-row: 2; }
  #jobs-body  {
    flex: 1; overflow-x: auto; overflow-y: auto;
  }
  table  { width: 100%; border-collapse: collapse; }
  thead th {
    position: sticky; top: 0; background: var(--panel);
    padding: 6px 10px; text-align: left; font-size: 10px;
    text-transform: uppercase; color: var(--dim); white-space: nowrap;
    border-bottom: 1px solid var(--border);
  }
  tbody td { padding: 5px 10px; border-bottom: 1px solid #111827; font-size: 11px; }
  tbody tr:hover td { background: rgba(255,255,255,0.03); }
  .status-ok    { color: var(--green); }
  .status-error { color: var(--red);   }
  .status-empty { color: var(--yellow);}

  /* ── Scrollbar ───────────────────────────────────────────────────── */
  ::-webkit-scrollbar { width: 4px; height: 4px; }
  ::-webkit-scrollbar-track { background: var(--panel); }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }

  /* ── Controls ────────────────────────────────────────────────────── */
  #ctrl-bar {
    display: flex; gap: 8px; align-items: center;
    padding: 6px 12px; border-bottom: 1px solid var(--border); flex-shrink: 0;
  }
  #ctrl-bar button {
    padding: 3px 10px; border-radius: 4px; font-size: 11px; cursor: pointer;
    font-family: inherit; background: var(--border); color: var(--text); border: none;
  }
  #ctrl-bar button:hover { background: #334155; }
  #ctrl-bar button.active { background: var(--blue); color: #fff; }
  #search-box {
    background: var(--border); border: none; border-radius: 4px; padding: 3px 8px;
    color: var(--text); font-family: inherit; font-size: 11px; width: 160px;
  }
  .spacer { flex: 1; }
  #conn-status { font-size: 11px; }
</style>
</head>
<body>

<!-- ── Top bar ──────────────────────────────────────────────────────────── -->
<div id="topbar">
  <h1>⚡ ODDS KENYA — DEV MONITOR</h1>
  <span id="health-pill" class="pill yellow">LOADING…</span>

  <div class="metric"><div class="val" id="m-upcoming">—</div><div class="lbl">Upcoming</div></div>
  <div class="metric"><div class="val" id="m-live">—</div><div class="lbl">Live</div></div>
  <div class="metric" style="color:var(--yellow)"><div class="val" id="m-arbs">—</div><div class="lbl">Arbs</div></div>
  <div class="metric" style="color:var(--purple)"><div class="val" id="m-evs">—</div><div class="lbl">EV Bets</div></div>
  <div class="metric"><div class="val" id="m-sports">—/13</div><div class="lbl">Sports Warm</div></div>

  <span id="clock">—</span>
</div>

<!-- ── Main layout ──────────────────────────────────────────────────────── -->
<div id="main">

  <!-- Beat schedule -->
  <div class="panel" id="beat-panel">
    <div class="panel-title"><span class="dot"></span>Beat Schedule</div>
    <div class="panel-body" id="beat-list">
      <div style="color:var(--dim);padding:8px">Loading…</div>
    </div>
  </div>

  <!-- Log viewer -->
  <div class="panel" id="log-panel">
    <div id="log-tabs">
      <button class="tab-btn active" onclick="switchTab('celery')">celery.log</button>
      <button class="tab-btn"        onclick="switchTab('task')">tasks.log</button>
      <button class="tab-btn"        onclick="switchTab('jobs')">jobs.log</button>
    </div>
    <div id="ctrl-bar">
      <button id="btn-pause"  onclick="togglePause()">⏸ Pause</button>
      <button id="btn-clear"  onclick="clearLog()">🗑 Clear</button>
      <button id="btn-bottom" onclick="scrollBottom()">⬇ Bottom</button>
      <input id="search-box" placeholder="Filter logs…" oninput="filterLogs(this.value)">
      <span class="spacer"></span>
      <span id="conn-status" style="color:var(--dim)">●</span>
      <span id="line-count" style="color:var(--dim);font-size:11px">0 lines</span>
    </div>
    <div id="log-body"></div>
  </div>

  <!-- Sports cache -->
  <div class="panel" id="sports-panel">
    <div class="panel-title"><span class="dot" style="background:var(--blue)"></span>Sport Cache</div>
    <div class="panel-body" id="sports-list">
      <div style="color:var(--dim);padding:8px">Loading…</div>
    </div>
  </div>

  <!-- Jobs table -->
  <div class="panel" id="jobs-panel">
    <div class="panel-title">
      <span class="dot" style="background:var(--orange)"></span>
      Recent Jobs
      <span style="color:var(--dim);font-size:10px;margin-left:auto" id="jobs-count">0 jobs</span>
    </div>
    <div id="jobs-body">
      <table>
        <thead>
          <tr>
            <th>Time</th><th>Bookmaker</th><th>Sport</th><th>Mode</th>
            <th>Count</th><th>Status</th><th>✔ OK</th><th>✗ Fail</th>
            <th>Latency</th><th>Detail</th>
          </tr>
        </thead>
        <tbody id="jobs-tbody"></tbody>
      </table>
    </div>
  </div>

</div><!-- /main -->

<script>
// ── State ──────────────────────────────────────────────────────────────────
let paused      = false;
let autoScroll  = true;
let filterText  = "";
let activeTab   = "celery";
let lineCount   = 0;
const MAX_LINES = 2000;

// Stored lines per tab so we can filter without refetching
const logLines = { celery: [], task: [], jobs: [] };
const sseConns = {};

// Beat intervals & last-run state
const beatIntervals = {};  // task_name → interval_s
const beatLastRun   = {};  // task_name → last_run timestamp

// ── Clock ──────────────────────────────────────────────────────────────────
function tick() {
  document.getElementById("clock").textContent =
    new Date().toISOString().replace("T"," ").slice(0,19) + " UTC";
}
setInterval(tick, 1000); tick();

// ── Tab switching ──────────────────────────────────────────────────────────
function switchTab(tab) {
  activeTab = tab;
  document.querySelectorAll(".tab-btn").forEach((b,i) => {
    b.classList.toggle("active", ["celery","task","jobs"][i] === tab);
  });
  renderLog();
  connectLogStream(tab);
}

// ── Log rendering ──────────────────────────────────────────────────────────
function classifyLine(line) {
  if (line.includes("[startup]") || line.includes("worker ready"))   return "lvl-STARTUP";
  if (line.includes("[beat]")    || line.includes("Beat"))           return "lvl-BEAT";
  if (line.includes("task_start")|| line.includes("task_done") ||
      line.includes("Received"))                                      return "lvl-TASK";
  if (line.includes("SUCCESS")   || line.includes("✔"))             return "lvl-SUCCESS";
  if (/\bCRITICAL\b/.test(line))                                     return "lvl-CRITICAL";
  if (/\bERROR\b/.test(line)     || line.includes("error"))         return "lvl-ERROR";
  if (/\bWARNING\b/.test(line)   || line.includes("warning"))       return "lvl-WARNING";
  if (/\bDEBUG\b/.test(line))                                        return "lvl-DEBUG";
  return "lvl-INFO";
}

function makeLogLine(raw, tab) {
  // Try to parse JSON (task events)
  let display = raw;
  if (tab !== "celery" && raw.trim().startsWith("{")) {
    try {
      const obj = JSON.parse(raw);
      const ev  = obj.event || obj.type || "";
      const ts  = obj.ts    || "";
      const task = (obj.task || "").split(".").pop();
      if (ev === "task_start")
        display = `[${ts}] 🚀 START  ${task}`;
      else if (ev === "task_done")
        display = `[${ts}] ✔ DONE   ${task}  [${obj.state}]`;
      else if (ev === "task_failure")
        display = `[${ts}] ✗ FAIL   ${task}  ${obj.error}`;
      else if (ev === "task_retry")
        display = `[${ts}] ↺ RETRY  ${task}  ${obj.reason}`;
      else if (obj.bookmaker) {
        // job log entry
        const sc = {"ok":"✔","error":"✗","empty":"○"}[obj.status] || "?";
        display  = `[${ts}] ${sc} ${obj.bookmaker.toUpperCase()} / ${obj.sport} / ${obj.mode}  `
                 + `count=${obj.count} ok=${obj.unified_ok} fail=${obj.unified_fail} ${obj.latency_ms}ms`
                 + (obj.detail ? `  ERR: ${obj.detail}` : "");
      }
    } catch(e) {}
  }
  return display;
}

function addLine(line, tab) {
  if (logLines[tab].length >= MAX_LINES) logLines[tab].shift();
  logLines[tab].push(line);
  if (tab === activeTab && !paused) renderLastLine(line);
}

function renderLastLine(raw) {
  const display = makeLogLine(raw, activeTab);
  if (filterText && !display.toLowerCase().includes(filterText)) return;
  const el    = document.getElementById("log-body");
  const cls   = classifyLine(display);
  const ts    = display.match(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)?.[0] || "";
  const text  = ts ? display.replace(ts, "").trim() : display;
  const div   = document.createElement("div");
  div.className = `log-line ${cls}`;
  div.innerHTML = ts
    ? `<span class="log-ts">${ts}</span><span class="log-text">${escHtml(text)}</span>`
    : `<span class="log-text">${escHtml(display)}</span>`;
  el.appendChild(div);

  // Keep max lines in DOM
  while (el.children.length > MAX_LINES) el.removeChild(el.firstChild);

  lineCount++;
  document.getElementById("line-count").textContent = lineCount + " lines";
  if (autoScroll) el.scrollTop = el.scrollHeight;
}

function renderLog() {
  const el = document.getElementById("log-body");
  el.innerHTML = "";
  lineCount = 0;
  const lines = logLines[activeTab];
  const filt  = filterText;
  lines.forEach(raw => {
    const display = makeLogLine(raw, activeTab);
    if (filt && !display.toLowerCase().includes(filt)) return;
    const cls  = classifyLine(display);
    const ts   = display.match(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)?.[0] || "";
    const text = ts ? display.replace(ts, "").trim() : display;
    const div  = document.createElement("div");
    div.className = `log-line ${cls}`;
    div.innerHTML = ts
      ? `<span class="log-ts">${ts}</span><span class="log-text">${escHtml(text)}</span>`
      : `<span class="log-text">${escHtml(display)}</span>`;
    el.appendChild(div);
    lineCount++;
  });
  document.getElementById("line-count").textContent = lineCount + " lines";
  if (autoScroll) el.scrollTop = el.scrollHeight;
}

function escHtml(s) {
  return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}

// ── Controls ───────────────────────────────────────────────────────────────
function togglePause() {
  paused = !paused;
  document.getElementById("btn-pause").textContent = paused ? "▶ Resume" : "⏸ Pause";
  document.getElementById("btn-pause").classList.toggle("active", paused);
}
function clearLog() {
  logLines[activeTab] = [];
  document.getElementById("log-body").innerHTML = "";
  lineCount = 0;
  document.getElementById("line-count").textContent = "0 lines";
}
function scrollBottom() {
  autoScroll = true;
  const el = document.getElementById("log-body");
  el.scrollTop = el.scrollHeight;
}
function filterLogs(val) {
  filterText = val.toLowerCase();
  renderLog();
}

// Auto-scroll detection
document.getElementById("log-body").addEventListener("scroll", function() {
  const el = this;
  autoScroll = el.scrollTop + el.clientHeight >= el.scrollHeight - 40;
});

// ── SSE connections ────────────────────────────────────────────────────────
const TAB_STREAM = {
  celery: "/api/monitor/stream/logs",
  task:   "/api/monitor/stream/tasks",
  jobs:   "/api/monitor/stream/jobs",
};

function connectLogStream(tab) {
  if (sseConns[tab]) return;  // already connected

  const url = TAB_STREAM[tab];
  const es  = new EventSource(url);
  sseConns[tab] = es;

  const statusEl = document.getElementById("conn-status");

  es.onopen = () => {
    statusEl.style.color = "var(--green)";
    statusEl.textContent = "● LIVE";
  };
  es.onerror = () => {
    statusEl.style.color = "var(--red)";
    statusEl.textContent = "● DISCONNECTED";
    delete sseConns[tab];
    setTimeout(() => connectLogStream(tab), 3000);
  };
  es.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      if (data.type === "connected") return;
      const raw = data.line || (data.type === "job"
        ? JSON.stringify(data) : e.data);
      addLine(raw, tab);

      // If it's a job event, also update jobs table
      if (data.type === "job") addJobRow(data);
    } catch(_) {
      addLine(e.data, tab);
    }
  };
}

// Connect default tab on load
connectLogStream("celery");

// ── Beat schedule rendering ────────────────────────────────────────────────
function secondsToHuman(s) {
  if (s == null) return "—";
  if (s === 0)   return '<span style="color:var(--red)">NOW</span>';
  if (s < 60)    return `${s}s`;
  if (s < 3600)  return `${Math.floor(s/60)}m${s%60}s`;
  return `${Math.floor(s/3600)}h${Math.floor((s%3600)/60)}m`;
}

function renderBeat(tasks) {
  const list = document.getElementById("beat-list");
  list.innerHTML = "";
  tasks.forEach(t => {
    const interval = t.interval;
    const nextIn   = t.next_in_s;
    const pct = nextIn == null ? 100
      : Math.max(0, Math.min(100, ((interval - nextIn) / interval) * 100));
    const barClass = pct > 90 ? "bar-red" : pct > 60 ? "bar-yellow" : "bar-green";
    const shortName = t.task.split(".").slice(-2).join(".");

    const row = document.createElement("div");
    row.className = "beat-row";
    row.title = t.task + `\nInterval: ${interval}s\nLast run: ${t.last_run || "never"}`;
    row.innerHTML = `
      <div class="beat-name"><span>${shortName}</span></div>
      <div class="beat-bar-wrap">
        <div class="beat-bar ${barClass}" style="width:${pct}%"></div>
      </div>
      <div class="beat-cntd" style="color:${nextIn===0?'var(--red)':nextIn<10?'var(--yellow)':'var(--dim)'}"
        data-task="${t.task}" data-nextin="${nextIn ?? -1}" data-interval="${interval}">
        ${secondsToHuman(nextIn)}
      </div>`;
    list.appendChild(row);

    beatIntervals[t.task] = interval;
    if (nextIn != null) beatLastRun[t.task] = Date.now() - (interval - nextIn) * 1000;
  });
}

// Live countdown tick (updates every second without re-fetching)
setInterval(() => {
  document.querySelectorAll(".beat-cntd[data-nextin]").forEach(el => {
    const stored = parseInt(el.dataset.nextin);
    if (stored < 0) return;  // unknown last run
    const interval  = parseInt(el.dataset.interval);
    const taskName  = el.dataset.task;
    const lastRunMs = beatLastRun[taskName];
    if (!lastRunMs) return;
    const elapsedS = (Date.now() - lastRunMs) / 1000;
    const nextIn   = Math.max(0, Math.round(interval - elapsedS));
    el.dataset.nextin = nextIn;
    el.innerHTML = secondsToHuman(nextIn);
    el.style.color = nextIn === 0 ? "var(--red)" : nextIn < 10 ? "var(--yellow)" : "var(--dim)";

    // Update bar
    const pct = Math.max(0, Math.min(100, (elapsedS / interval) * 100));
    const bar = el.parentElement.querySelector(".beat-bar");
    if (bar) {
      bar.style.width = pct + "%";
      bar.className = "beat-bar " + (pct > 90 ? "bar-red" : pct > 60 ? "bar-yellow" : "bar-green");
    }
  });
}, 1000);

// ── Sports rendering ───────────────────────────────────────────────────────
const SPORT_EMOJI = {
  soccer:"⚽", basketball:"🏀", tennis:"🎾", "ice-hockey":"🏒",
  rugby:"🏉", handball:"🤾", volleyball:"🏐", cricket:"🏏",
  "table-tennis":"🏓", esoccer:"🎮", mma:"🥋", boxing:"🥊", darts:"🎯",
};

function renderSports(sports) {
  const list = document.getElementById("sports-list");
  list.innerHTML = "";
  sports.forEach(s => {
    const em  = SPORT_EMOJI[s.sport] || "🏆";
    const row = document.createElement("div");
    row.className = "sport-row";
    row.innerHTML = `
      <div class="sport-dot" style="background:${s.populated?'var(--green)':'var(--red)'}"></div>
      <div class="sport-name">${em} ${s.sport}</div>
      <span class="sport-badge ${s.populated?'warm':'cold'}">${s.populated?s.upcoming+'↑ '+s.live+'▶':'cold'}</span>
      ${s.arbs > 0 ? `<span style="color:var(--yellow);font-size:10px">${s.arbs}arb</span>` : ''}
    `;
    list.appendChild(row);
  });
  const warm = sports.filter(s=>s.populated).length;
  document.getElementById("m-sports").textContent = `${warm}/13`;
}

// ── Jobs table ─────────────────────────────────────────────────────────────
let jobCount = 0;
const MAX_JOBS = 50;

function addJobRow(j, prepend = true) {
  const tbody = document.getElementById("jobs-tbody");
  const sc    = {"ok":"status-ok","error":"status-error","empty":"status-empty"}[j.status] || "";
  const icon  = {"ok":"✔","error":"✗","empty":"○"}[j.status] || "?";
  const tr    = document.createElement("tr");
  tr.innerHTML = `
    <td style="color:var(--dim)">${(j.ts||"").slice(11,19)}</td>
    <td style="color:var(--cyan)">${j.bookmaker||""}</td>
    <td>${j.sport||""}</td>
    <td style="color:var(--dim)">${j.mode||""}</td>
    <td>${j.count||0}</td>
    <td class="${sc}">${icon} ${j.status||""}</td>
    <td style="color:var(--green)">${j.unified_ok||0}</td>
    <td style="color:var(--red)">${j.unified_fail||0}</td>
    <td style="color:var(--dim)">${j.latency_ms||0}ms</td>
    <td style="color:var(--red);font-size:10px">${j.detail||""}</td>
  `;
  if (prepend && tbody.firstChild) {
    tbody.insertBefore(tr, tbody.firstChild);
  } else {
    tbody.appendChild(tr);
  }
  jobCount++;
  while (tbody.children.length > MAX_JOBS) tbody.removeChild(tbody.lastChild);
  document.getElementById("jobs-count").textContent = `${jobCount} jobs`;
}

// ── Polling: report + beat + sports ───────────────────────────────────────
async function fetchReport() {
  try {
    const r = await fetch("/api/monitor/report");
    const d = await r.json();
    if (!d.ok) return;

    // Health pill
    const pill = document.getElementById("health-pill");
    const h    = d.overall?.healthy;
    pill.textContent = h ? "● HEALTHY" : "● ISSUES";
    pill.className   = "pill " + (h ? "green" : "red");

    // Metrics
    document.getElementById("m-upcoming").textContent = d.totals?.upcoming_matches ?? "—";
    document.getElementById("m-live").textContent     = d.totals?.live_matches     ?? "—";
    document.getElementById("m-arbs").textContent     = d.totals?.arb_opportunities ?? "—";
    document.getElementById("m-evs").textContent      = d.totals?.ev_opportunities  ?? "—";

    // Sports
    if (d.sports) renderSports(d.sports);

    // Recent jobs (initial load only)
    if (jobCount === 0 && d.recent_jobs) {
      [...d.recent_jobs].reverse().forEach(j => addJobRow(j, false));
    }
  } catch(e) { console.warn("report fetch:", e); }
}

async function fetchBeat() {
  try {
    const r = await fetch("/api/monitor/beat");
    const d = await r.json();
    if (d.ok) renderBeat(d.tasks);
  } catch(e) {}
}

// Initial + polling
fetchReport(); fetchBeat();
setInterval(fetchReport, 15000);
setInterval(fetchBeat,   30000);

// When a task_done event comes in via the log stream, refresh beat immediately
// (so the bar resets right away)
const origAddLine = addLine;
window._origAddLine = origAddLine;
</script>
</body>
</html>"""
    return Response(html, content_type="text/html")