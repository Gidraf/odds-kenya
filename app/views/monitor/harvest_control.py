"""
app/views/odds_feed/harvest_control.py
=======================================
Full lifecycle control for harvest tasks — trigger, pause, resume, stop,
and observe a running harvest in real-time via SSE.

Register in create_app():
    from app.views.odds_feed.harvest_control import bp_harvest_ctrl
    app.register_blueprint(bp_harvest_ctrl)

Endpoints
─────────
  POST /api/harvest/trigger
       Body: {bookmaker, sport, mode}
       → dispatches the harvest task, returns harvest_id

  POST /api/harvest/stop
       Body: {harvest_id}  OR  {task_id}
       → revokes the Celery task (terminates running worker)

  POST /api/harvest/pause
       Body: {harvest_id}
       → sets Redis flag; cooperative tasks check it and sleep

  POST /api/harvest/resume
       Body: {harvest_id}
       → clears pause flag; task continues

  GET  /api/harvest/active
       → list all currently tracked harvests

  GET  /api/harvest/status/<harvest_id>
       → full status + metrics for one harvest

  GET  /api/harvest/stream/<harvest_id>
       → SSE: real-time progress events for a single harvest

  DELETE /api/harvest/clear
       → remove all finished/stopped harvest records
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone

from flask import Blueprint, Response, request, stream_with_context

bp_harvest_ctrl = Blueprint("harvest_ctrl", __name__, url_prefix="/api/harvest")

# ── Bookmaker → task name map ─────────────────────────────────────────────────
UPCOMING_TASKS: dict[str, str] = {
    "sp":      "tasks.sp.harvest_all_upcoming",
    "bt":      "tasks.bt.harvest_all_upcoming",
    "od":      "tasks.od.harvest_all_upcoming",
    "b2b":     "tasks.b2b.harvest_all_upcoming",
    "b2b_page":"tasks.b2b_page.harvest_all_upcoming",
    "sbo":     "tasks.sbo.harvest_all_upcoming",
}
LIVE_TASKS: dict[str, str] = {
    "sp":      "tasks.sp.harvest_all_live",
    "bt":      "tasks.bt.harvest_all_live",
    "od":      "tasks.od.harvest_all_live",
    "b2b":     "tasks.b2b.harvest_all_live",
    "b2b_page":"tasks.b2b_page.harvest_all_live",
    "sbo":     "tasks.sbo.harvest_all_live",
}
QUEUE_MAP: dict[str, str] = {
    "sp": "harvest", "bt": "harvest", "od": "harvest",
    "b2b": "harvest", "b2b_page": "harvest", "sbo": "harvest",
}
ALL_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey", "rugby",
    "handball", "volleyball", "cricket", "table-tennis",
    "esoccer", "mma", "boxing", "darts",
]
ALL_BOOKMAKERS = ["sp", "bt", "od", "b2b", "b2b_page", "sbo"]

HARVEST_TTL = 3600   # seconds to keep harvest records in Redis


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _redis():
    from app.workers.celery_tasks import _redis as _r
    return _r()


def _celery():
    from app.workers.celery_tasks import celery
    return celery


# ── Redis helpers for harvest tracking ───────────────────────────────────────

def _hkey(harvest_id: str) -> str:
    return f"harvest:track:{harvest_id}"


def _save_harvest(harvest_id: str, data: dict) -> None:
    r = _redis()
    r.set(_hkey(harvest_id), json.dumps(data, default=str), ex=HARVEST_TTL)
    r.zadd("harvest:index", {harvest_id: time.time()})


def _load_harvest(harvest_id: str) -> dict | None:
    try:
        raw = _redis().get(_hkey(harvest_id))
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _update_harvest(harvest_id: str, **fields) -> dict | None:
    data = _load_harvest(harvest_id)
    if not data:
        return None
    data.update(fields)
    data["updated_at"] = _now()
    _save_harvest(harvest_id, data)
    return data


def _all_harvest_ids() -> list[str]:
    try:
        ids = _redis().zrevrange("harvest:index", 0, 99)
        return [i.decode() if isinstance(i, bytes) else i for i in ids]
    except Exception:
        return []


def _publish_progress(harvest_id: str, event: dict) -> None:
    """Publish a progress event to the harvest-specific Redis channel."""
    try:
        event["harvest_id"] = harvest_id
        event["ts"]         = _now()
        _redis().publish(f"harvest:progress:{harvest_id}",
                         json.dumps(event, default=str))
    except Exception:
        pass


# ── Pause flag helpers ────────────────────────────────────────────────────────

def _pause_key(harvest_id: str) -> str:
    return f"harvest:pause:{harvest_id}"


def is_paused(harvest_id: str) -> bool:
    try:
        return bool(_redis().exists(_pause_key(harvest_id)))
    except Exception:
        return False


def set_paused(harvest_id: str) -> None:
    _redis().set(_pause_key(harvest_id), "1", ex=HARVEST_TTL)


def set_resumed(harvest_id: str) -> None:
    _redis().delete(_pause_key(harvest_id))


# ── Cooperative pause helper (call from inside long harvest tasks) ────────────

def cooperative_pause_check(harvest_id: str, check_interval: float = 0.5) -> bool:
    """
    Call this inside a harvest loop.
    Sleeps while paused; returns True if the harvest was stopped/revoked.
    """
    while is_paused(harvest_id):
        time.sleep(check_interval)
        rec = _load_harvest(harvest_id)
        if rec and rec.get("status") in ("stopped", "revoked"):
            return True
    return False


# =============================================================================
# POST /api/harvest/trigger
# =============================================================================

@bp_harvest_ctrl.route("/trigger", methods=["POST"])
def trigger():
    """
    Dispatch a harvest task immediately.

    Body (JSON):
      bookmaker  — sp | bt | od | b2b | b2b_page | sbo  (required)
      sport      — soccer | basketball | …  (optional; all sports if omitted)
      mode       — upcoming | live           (default: upcoming)
    """
    body      = request.get_json(silent=True) or {}
    bookmaker = (body.get("bookmaker") or "").lower().strip()
    sport     = (body.get("sport")     or "all").lower().strip()
    mode      = (body.get("mode")      or "upcoming").lower().strip()

    if not bookmaker or bookmaker not in ALL_BOOKMAKERS:
        return {
            "ok":       False,
            "error":    f"bookmaker must be one of {ALL_BOOKMAKERS}",
        }, 400

    if mode not in ("upcoming", "live"):
        return {"ok": False, "error": "mode must be 'upcoming' or 'live'"}, 400

    task_map  = UPCOMING_TASKS if mode == "upcoming" else LIVE_TASKS
    task_name = task_map.get(bookmaker)
    if not task_name:
        return {"ok": False, "error": f"No task for {bookmaker}/{mode}"}, 400

    harvest_id = str(uuid.uuid4())[:8]

    # Dispatch the Celery task (existing tasks accept no per-sport args;
    # they iterate all sports internally — pass sport as metadata only)
    try:
        result = _celery().send_task(
            task_name,
            queue=QUEUE_MAP.get(bookmaker, "harvest"),
            kwargs={"_harvest_id": harvest_id} if False else {},  # future hook
        )
        task_id = result.id
    except Exception as exc:
        return {"ok": False, "error": str(exc)}, 500

    record = {
        "harvest_id":  harvest_id,
        "task_id":     task_id,
        "task_name":   task_name,
        "bookmaker":   bookmaker,
        "sport":       sport,
        "mode":        mode,
        "status":      "running",
        "started_at":  _now(),
        "updated_at":  _now(),
        "stopped_at":  None,
        "result":      None,
        "metrics": {
            "matches_fetched":  0,
            "matches_merged":   0,
            "arbs_found":       0,
            "evs_found":        0,
            "latency_ms":       0,
            "error_count":      0,
        },
        "events": [],
    }
    _save_harvest(harvest_id, record)

    # Publish start event
    _publish_progress(harvest_id, {
        "type":      "started",
        "bookmaker": bookmaker,
        "sport":     sport,
        "mode":      mode,
        "task_id":   task_id,
        "task_name": task_name,
    })

    return {
        "ok":         True,
        "harvest_id": harvest_id,
        "task_id":    task_id,
        "task_name":  task_name,
        "bookmaker":  bookmaker,
        "sport":      sport,
        "mode":       mode,
        "stream_url": f"/api/harvest/stream/{harvest_id}",
        "status_url": f"/api/harvest/status/{harvest_id}",
    }


# =============================================================================
# POST /api/harvest/stop
# =============================================================================

@bp_harvest_ctrl.route("/stop", methods=["POST"])
def stop():
    """
    Revoke + terminate a running harvest task.

    Body: {harvest_id}  or  {task_id}
    """
    body       = request.get_json(silent=True) or {}
    harvest_id = body.get("harvest_id")
    task_id    = body.get("task_id")

    if harvest_id:
        rec = _load_harvest(harvest_id)
        if not rec:
            return {"ok": False, "error": "harvest not found"}, 404
        task_id = rec.get("task_id")
    elif not task_id:
        return {"ok": False, "error": "provide harvest_id or task_id"}, 400

    try:
        _celery().control.revoke(task_id, terminate=True, signal="SIGTERM")
    except Exception as exc:
        return {"ok": False, "error": str(exc)}, 500

    if harvest_id:
        _update_harvest(harvest_id, status="stopped", stopped_at=_now())
        set_resumed(harvest_id)   # clear any pause flag
        _publish_progress(harvest_id, {"type": "stopped", "task_id": task_id})

    return {"ok": True, "task_id": task_id, "harvest_id": harvest_id, "status": "stopped"}


# =============================================================================
# POST /api/harvest/pause
# =============================================================================

@bp_harvest_ctrl.route("/pause", methods=["POST"])
def pause():
    """
    Set the pause flag for a harvest.
    Cooperative tasks that call cooperative_pause_check() will sleep.

    Body: {harvest_id}
    """
    body       = request.get_json(silent=True) or {}
    harvest_id = body.get("harvest_id")
    if not harvest_id:
        return {"ok": False, "error": "harvest_id required"}, 400

    rec = _load_harvest(harvest_id)
    if not rec:
        return {"ok": False, "error": "harvest not found"}, 404
    if rec.get("status") != "running":
        return {"ok": False, "error": f"harvest is {rec.get('status')} — can only pause running harvests"}, 400

    set_paused(harvest_id)
    _update_harvest(harvest_id, status="paused")
    _publish_progress(harvest_id, {"type": "paused"})

    return {"ok": True, "harvest_id": harvest_id, "status": "paused"}


# =============================================================================
# POST /api/harvest/resume
# =============================================================================

@bp_harvest_ctrl.route("/resume", methods=["POST"])
def resume():
    """
    Clear the pause flag for a harvest so it continues.

    Body: {harvest_id}
    """
    body       = request.get_json(silent=True) or {}
    harvest_id = body.get("harvest_id")
    if not harvest_id:
        return {"ok": False, "error": "harvest_id required"}, 400

    rec = _load_harvest(harvest_id)
    if not rec:
        return {"ok": False, "error": "harvest not found"}, 404

    set_resumed(harvest_id)
    _update_harvest(harvest_id, status="running")
    _publish_progress(harvest_id, {"type": "resumed"})

    return {"ok": True, "harvest_id": harvest_id, "status": "running"}


# =============================================================================
# GET /api/harvest/active
# =============================================================================

@bp_harvest_ctrl.route("/active")
def active():
    """List all tracked harvests (last 100, newest first)."""
    harvests = []
    for hid in _all_harvest_ids():
        rec = _load_harvest(hid)
        if rec:
            # Attach live cache count for context
            sport     = rec.get("sport", "soccer")
            mode      = rec.get("mode",  "upcoming")
            cache_key = f"combined:{mode}:{sport}"
            try:
                from app.workers.celery_tasks import cache_get
                cached = cache_get(cache_key)
                rec["cache_count"] = len(cached.get("matches", [])) if cached else 0
            except Exception:
                rec["cache_count"] = 0
            harvests.append(rec)

    running  = [h for h in harvests if h["status"] == "running"]
    paused   = [h for h in harvests if h["status"] == "paused"]
    finished = [h for h in harvests if h["status"] not in ("running", "paused")]

    return {
        "ok":       True,
        "total":    len(harvests),
        "running":  len(running),
        "paused":   len(paused),
        "finished": len(finished),
        "harvests": harvests,
    }


# =============================================================================
# GET /api/harvest/status/<harvest_id>
# =============================================================================

@bp_harvest_ctrl.route("/status/<harvest_id>")
def status(harvest_id: str):
    """Full status + metrics for a single harvest."""
    rec = _load_harvest(harvest_id)
    if not rec:
        return {"ok": False, "error": "harvest not found"}, 404

    # Enrich with live cache snapshot
    sport     = rec.get("sport", "soccer")
    mode      = rec.get("mode",  "upcoming")
    try:
        from app.workers.celery_tasks import cache_get
        cached      = cache_get(f"combined:{mode}:{sport}") or {}
        matches     = cached.get("matches", [])
        rec["cache_snapshot"] = {
            "count":         len(matches),
            "arbs":          sum(1 for m in matches if m.get("has_arb")),
            "evs":           sum(1 for m in matches if m.get("has_ev")),
            "harvested_at":  cached.get("harvested_at"),
            "bk_counts":     cached.get("bk_counts", {}),
        }
    except Exception:
        rec["cache_snapshot"] = {}

    # Check Celery task state
    task_id = rec.get("task_id")
    if task_id:
        try:
            result = _celery().AsyncResult(task_id)
            rec["celery_state"] = result.state
            if result.state == "SUCCESS" and rec["status"] == "running":
                _update_harvest(harvest_id, status="done",
                                stopped_at=_now(), result=str(result.result))
                rec["status"] = "done"
            elif result.state == "FAILURE" and rec["status"] == "running":
                _update_harvest(harvest_id, status="failed", stopped_at=_now())
                rec["status"] = "failed"
        except Exception:
            rec["celery_state"] = "unknown"

    return {"ok": True, **rec}


# =============================================================================
# DELETE /api/harvest/clear
# =============================================================================

@bp_harvest_ctrl.route("/clear", methods=["DELETE"])
def clear():
    """Remove all finished / stopped / failed harvest records."""
    cleared = 0
    for hid in _all_harvest_ids():
        rec = _load_harvest(hid)
        if rec and rec.get("status") not in ("running", "paused"):
            _redis().delete(_hkey(hid))
            _redis().zrem("harvest:index", hid)
            cleared += 1
    return {"ok": True, "cleared": cleared}


# =============================================================================
# GET /api/harvest/stream/<harvest_id>  — SSE
# =============================================================================

@bp_harvest_ctrl.route("/stream/<harvest_id>")
def stream(harvest_id: str):
    """
    SSE stream for a single harvest.

    Combines two sources:
      1. Redis pub/sub channel harvest:progress:{harvest_id}
         (receives events published by the task and control endpoints)
      2. Polling the combined cache key every 3 s to emit metrics diffs
         (works even if the task doesn't publish explicitly)
    """
    rec = _load_harvest(harvest_id)
    if not rec:
        return {"ok": False, "error": "harvest not found"}, 404

    @stream_with_context
    def generate():
        r          = _redis()
        sport      = rec.get("sport",     "soccer")
        mode       = rec.get("mode",      "upcoming")
        bookmaker  = rec.get("bookmaker", "sp")
        channel    = f"harvest:progress:{harvest_id}"

        # Send current status immediately
        snapshot = dict(_load_harvest(harvest_id) or rec)
        snapshot["type"] = "snapshot"
        yield _sse(snapshot)

        # Subscribe to progress channel
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(channel)

        prev_count  = 0
        prev_arbs   = 0
        prev_evs    = 0
        last_poll   = 0.0
        last_ka     = time.monotonic()
        POLL_SEC    = 3.0
        KA_SEC      = 15.0

        try:
            while True:
                # ── Pub/sub messages ──────────────────────────────────────────
                msg = pubsub.get_message(timeout=0.3)
                if msg and msg.get("type") == "message":
                    try:
                        yield _sse(json.loads(msg["data"]))
                    except Exception:
                        pass

                # ── Poll cache every POLL_SEC ─────────────────────────────────
                now = time.monotonic()
                if now - last_poll >= POLL_SEC:
                    last_poll = now
                    try:
                        from app.workers.celery_tasks import cache_get
                        cached  = cache_get(f"combined:{mode}:{sport}") or {}
                        matches = cached.get("matches", [])
                        count   = len(matches)
                        arbs    = sum(1 for m in matches if m.get("has_arb"))
                        evs     = sum(1 for m in matches if m.get("has_ev"))

                        # Emit metrics update (even if no change — heartbeat)
                        yield _sse({
                            "type":           "metrics",
                            "harvest_id":     harvest_id,
                            "bookmaker":      bookmaker,
                            "sport":          sport,
                            "mode":           mode,
                            "matches_total":  count,
                            "matches_new":    max(0, count - prev_count),
                            "arbs":           arbs,
                            "arbs_new":       max(0, arbs - prev_arbs),
                            "evs":            evs,
                            "evs_new":        max(0, evs - prev_evs),
                            "harvested_at":   cached.get("harvested_at"),
                            "bk_counts":      cached.get("bk_counts", {}),
                        })
                        prev_count = count
                        prev_arbs  = arbs
                        prev_evs   = evs
                    except Exception:
                        pass

                    # Check if task completed
                    current_rec = _load_harvest(harvest_id)
                    if current_rec and current_rec.get("status") in ("done", "failed", "stopped"):
                        yield _sse({"type": "finished",
                                    "status": current_rec["status"],
                                    "harvest_id": harvest_id})
                        break

                # ── Keep-alive ────────────────────────────────────────────────
                if now - last_ka >= KA_SEC:
                    yield f": keep-alive {_now()}\n\n"
                    last_ka = now

        except GeneratorExit:
            pass
        finally:
            try:
                pubsub.unsubscribe(channel)
                pubsub.close()
            except Exception:
                pass

    return Response(generate(), headers={
        "Content-Type":      "text/event-stream",
        "Cache-Control":     "no-cache",
        "X-Accel-Buffering": "no",
        "Connection":        "keep-alive",
    })


def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"


# =============================================================================
# GET /api/harvest/ui   — Standalone control panel HTML
# =============================================================================

@bp_harvest_ctrl.route("/ui")
def ui():
    """
    GET /api/harvest/ui
    Standalone harvest control panel.
    Also embedded in the main monitor dashboard.
    """
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Harvest Control</title>
<style>
  :root {
    --bg:#0a0f1e; --panel:#0f172a; --border:#1e293b; --text:#e2e8f0;
    --dim:#64748b; --green:#22c55e; --red:#ef4444; --yellow:#f59e0b;
    --blue:#3b82f6; --cyan:#06b6d4; --orange:#f97316; --purple:#a855f7;
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { background:var(--bg); color:var(--text);
         font-family:'JetBrains Mono','Fira Code',monospace; font-size:13px;
         padding:20px; }
  h2   { color:var(--cyan); margin-bottom:12px; font-size:14px; letter-spacing:1px; }
  h3   { color:var(--dim);  margin:16px 0 8px;  font-size:12px; text-transform:uppercase; }

  /* ── Trigger form ────────────────────────────────────────────── */
  .trigger-form {
    background:var(--panel); border:1px solid var(--border);
    border-radius:8px; padding:16px; margin-bottom:20px;
  }
  .form-row { display:flex; gap:10px; flex-wrap:wrap; align-items:flex-end; }
  .field    { display:flex; flex-direction:column; gap:4px; }
  .field label { font-size:11px; color:var(--dim); text-transform:uppercase; }
  select, input {
    background:var(--border); border:1px solid #334155; border-radius:4px;
    color:var(--text); padding:6px 10px; font-family:inherit; font-size:12px;
  }
  select:focus, input:focus { outline:none; border-color:var(--blue); }

  /* ── Buttons ─────────────────────────────────────────────────── */
  .btn {
    padding:7px 14px; border-radius:5px; font-size:12px; font-weight:bold;
    cursor:pointer; font-family:inherit; border:none; transition:opacity .15s;
  }
  .btn:hover    { opacity:.85; }
  .btn:disabled { opacity:.4; cursor:default; }
  .btn-green  { background:var(--green);  color:#000; }
  .btn-red    { background:var(--red);    color:#fff; }
  .btn-yellow { background:var(--yellow); color:#000; }
  .btn-blue   { background:var(--blue);   color:#fff; }
  .btn-gray   { background:#334155;       color:var(--text); }

  /* ── Active harvests ──────────────────────────────────────────── */
  #active-list { display:flex; flex-direction:column; gap:10px; }

  .harvest-card {
    background:var(--panel); border:1px solid var(--border);
    border-radius:8px; padding:12px; position:relative;
  }
  .harvest-card.running { border-left:3px solid var(--green);  }
  .harvest-card.paused  { border-left:3px solid var(--yellow); }
  .harvest-card.done    { border-left:3px solid var(--blue);   }
  .harvest-card.failed  { border-left:3px solid var(--red);    }
  .harvest-card.stopped { border-left:3px solid var(--dim);    }

  .card-header {
    display:flex; align-items:center; gap:10px; margin-bottom:10px;
  }
  .card-title  { font-weight:bold; font-size:13px; flex:1; }
  .card-status {
    font-size:11px; padding:2px 8px; border-radius:10px; font-weight:bold;
  }
  .st-running { background:#052e16; color:var(--green); }
  .st-paused  { background:#422006; color:var(--yellow); }
  .st-done    { background:#172554; color:var(--blue); }
  .st-failed  { background:#450a0a; color:var(--red); }
  .st-stopped { background:#1c1917; color:var(--dim); }

  .card-meta { color:var(--dim); font-size:11px; margin-bottom:8px; }

  /* Metrics bar */
  .metrics { display:flex; gap:16px; margin-bottom:10px; flex-wrap:wrap; }
  .metric  { text-align:center; }
  .metric .val { font-size:20px; font-weight:bold; }
  .metric .lbl { font-size:10px; color:var(--dim); }
  .metric.new  .val { color:var(--green); }
  .metric.arb  .val { color:var(--yellow); }
  .metric.ev   .val { color:var(--purple); }

  /* BK breakdown */
  .bk-row { display:flex; gap:8px; margin-bottom:8px; flex-wrap:wrap; }
  .bk-pill {
    font-size:11px; padding:2px 8px; background:var(--border);
    border-radius:10px; color:var(--dim);
  }
  .bk-pill.active { color:var(--cyan); }

  /* Log feed per card */
  .card-log {
    background:#060c1a; border-radius:4px; padding:8px;
    font-size:11px; height:80px; overflow-y:auto;
    border:1px solid var(--border);
  }
  .card-log .entry { color:var(--dim); line-height:1.6; }
  .card-log .entry.ok    { color:var(--green);  }
  .card-log .entry.warn  { color:var(--yellow); }
  .card-log .entry.err   { color:var(--red);    }
  .card-log .entry.info  { color:var(--cyan);   }

  .card-btns { display:flex; gap:8px; margin-top:10px; }

  /* ── History table ────────────────────────────────────────────── */
  table { width:100%; border-collapse:collapse; background:var(--panel);
          border-radius:6px; overflow:hidden; }
  th    { padding:7px 10px; text-align:left; font-size:10px; color:var(--dim);
          text-transform:uppercase; border-bottom:1px solid var(--border); }
  td    { padding:6px 10px; border-bottom:1px solid #111827; font-size:11px; }
  tr:hover td { background:rgba(255,255,255,.03); }

  ::-webkit-scrollbar { width:3px; }
  ::-webkit-scrollbar-thumb { background:var(--border); }
</style>
</head>
<body>

<h2>⚡ Harvest Control Panel</h2>

<!-- ── Trigger form ──────────────────────────────────────────────────── -->
<div class="trigger-form">
  <h3>Trigger Harvest</h3>
  <div class="form-row">
    <div class="field">
      <label>Bookmaker</label>
      <select id="f-bookmaker">
        <option value="sp">SportPesa (SP)</option>
        <option value="bt">Betika (BT)</option>
        <option value="od">OdiBets (OD)</option>
        <option value="b2b">B2B Direct</option>
        <option value="b2b_page">B2B Page</option>
        <option value="sbo">SBO</option>
      </select>
    </div>
    <div class="field">
      <label>Sport</label>
      <select id="f-sport">
        <option value="all">All Sports</option>
        <option value="soccer">⚽ Soccer</option>
        <option value="basketball">🏀 Basketball</option>
        <option value="tennis">🎾 Tennis</option>
        <option value="ice-hockey">🏒 Ice Hockey</option>
        <option value="rugby">🏉 Rugby</option>
        <option value="handball">🤾 Handball</option>
        <option value="volleyball">🏐 Volleyball</option>
        <option value="cricket">🏏 Cricket</option>
        <option value="table-tennis">🏓 Table Tennis</option>
        <option value="esoccer">🎮 eSoccer</option>
        <option value="mma">🥋 MMA</option>
        <option value="boxing">🥊 Boxing</option>
        <option value="darts">🎯 Darts</option>
      </select>
    </div>
    <div class="field">
      <label>Mode</label>
      <select id="f-mode">
        <option value="upcoming">Upcoming</option>
        <option value="live">Live</option>
      </select>
    </div>
    <div class="field" style="justify-content:flex-end">
      <button class="btn btn-green" onclick="triggerHarvest()">▶ Trigger</button>
    </div>
    <div class="field" style="justify-content:flex-end">
      <button class="btn btn-gray"  onclick="triggerAll()">⚡ Trigger All</button>
    </div>
  </div>
  <div id="trigger-msg" style="margin-top:10px;font-size:12px;color:var(--dim)"></div>
</div>

<!-- ── Active harvests ───────────────────────────────────────────────── -->
<div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">
  <h3 style="margin:0">Active Harvests</h3>
  <button class="btn btn-gray" style="font-size:11px;padding:3px 10px"
    onclick="loadActive()">↺ Refresh</button>
  <button class="btn btn-gray" style="font-size:11px;padding:3px 10px"
    onclick="clearFinished()">🗑 Clear Finished</button>
  <span id="active-count" style="color:var(--dim);font-size:11px"></span>
</div>
<div id="active-list"></div>

<h3 style="margin-top:20px">All Harvests</h3>
<table>
  <thead>
    <tr>
      <th>ID</th><th>Bookmaker</th><th>Sport</th><th>Mode</th>
      <th>Status</th><th>Matches</th><th>Arbs</th><th>EVs</th>
      <th>Started</th><th>Actions</th>
    </tr>
  </thead>
  <tbody id="history-tbody"></tbody>
</table>

<script>
const sseConnections = {};  // harvest_id → EventSource

// ── Trigger ──────────────────────────────────────────────────────────────────
async function triggerHarvest(bk, sport, mode) {
  const bookmaker = bk    || document.getElementById("f-bookmaker").value;
  const sp        = sport || document.getElementById("f-sport").value;
  const md        = mode  || document.getElementById("f-mode").value;
  const msg       = document.getElementById("trigger-msg");
  msg.textContent = "Dispatching…";
  try {
    const r = await fetch("/api/harvest/trigger", {
      method:"POST",
      headers:{"Content-Type":"application/json"},
      body: JSON.stringify({bookmaker, sport: sp, mode: md}),
    });
    const d = await r.json();
    if (d.ok) {
      msg.style.color = "var(--green)";
      msg.textContent = `✔ Dispatched — harvest_id: ${d.harvest_id}  task: ${d.task_name}`;
      setTimeout(loadActive, 800);
      connectStream(d.harvest_id);
    } else {
      msg.style.color = "var(--red)";
      msg.textContent = `✗ Error: ${d.error}`;
    }
  } catch(e) {
    msg.style.color = "var(--red)";
    msg.textContent = `✗ ${e}`;
  }
}

async function triggerAll() {
  const bookmakers = ["sp","bt","od"];
  const mode = document.getElementById("f-mode").value;
  document.getElementById("trigger-msg").textContent = `Triggering ${bookmakers.length} bookmakers…`;
  for (const bk of bookmakers) {
    await triggerHarvest(bk, "all", mode);
    await new Promise(r => setTimeout(r, 400));
  }
  document.getElementById("trigger-msg").textContent =
    `✔ Triggered ${bookmakers.length} bookmakers`;
}

// ── Control actions ───────────────────────────────────────────────────────────
async function stopHarvest(harvest_id) {
  const r = await fetch("/api/harvest/stop", {
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body: JSON.stringify({harvest_id}),
  });
  const d = await r.json();
  if (d.ok) {
    logToCard(harvest_id, "Stopped", "err");
    setTimeout(loadActive, 500);
  }
}

async function pauseHarvest(harvest_id) {
  await fetch("/api/harvest/pause", {
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body: JSON.stringify({harvest_id}),
  });
  logToCard(harvest_id, "Paused", "warn");
  setTimeout(loadActive, 500);
}

async function resumeHarvest(harvest_id) {
  await fetch("/api/harvest/resume", {
    method:"POST",
    headers:{"Content-Type":"application/json"},
    body: JSON.stringify({harvest_id}),
  });
  logToCard(harvest_id, "Resumed", "ok");
  setTimeout(loadActive, 500);
}

async function clearFinished() {
  const r = await fetch("/api/harvest/clear", {method:"DELETE"});
  const d = await r.json();
  loadActive();
}

// ── SSE stream per harvest ────────────────────────────────────────────────────
function connectStream(harvest_id) {
  if (sseConnections[harvest_id]) return;

  const es = new EventSource(`/api/harvest/stream/${harvest_id}`);
  sseConnections[harvest_id] = es;

  es.onmessage = (e) => {
    try {
      const data = JSON.parse(e.data);
      handleStreamEvent(harvest_id, data);
    } catch(_) {}
  };
  es.onerror = () => {
    delete sseConnections[harvest_id];
  };
}

function handleStreamEvent(harvest_id, data) {
  const type = data.type;

  if (type === "snapshot") {
    renderCard(data);
    return;
  }

  if (type === "metrics") {
    updateCardMetrics(harvest_id, data);
    const msg = `matches=${data.matches_total} (+${data.matches_new}) arbs=${data.arbs} evs=${data.evs}`;
    logToCard(harvest_id, msg, data.matches_new > 0 ? "ok" : "info");
    return;
  }

  if (type === "started")  logToCard(harvest_id, `Started — ${data.task_name}`, "ok");
  if (type === "stopped")  logToCard(harvest_id, "Stopped by user", "err");
  if (type === "paused")   logToCard(harvest_id, "Paused", "warn");
  if (type === "resumed")  logToCard(harvest_id, "Resumed", "ok");
  if (type === "finished") {
    logToCard(harvest_id, `Finished — status: ${data.status}`,
              data.status === "done" ? "ok" : "err");
    const es = sseConnections[harvest_id];
    if (es) { es.close(); delete sseConnections[harvest_id]; }
    setTimeout(loadActive, 1000);
  }
}

// ── Card rendering ────────────────────────────────────────────────────────────
const cardCache = {};  // harvest_id → DOM element

function renderCard(rec) {
  const existing = document.getElementById(`card-${rec.harvest_id}`);
  if (existing) { updateCardMetrics(rec.harvest_id, rec.metrics || {}); return; }

  const status = rec.status || "running";
  const stCls  = `st-${status}`;
  const stIcon = {running:"▶",paused:"⏸",done:"✔",failed:"✗",stopped:"■"}[status] || "?";
  const bkLabel= {sp:"SportPesa",bt:"Betika",od:"OdiBets",
                  b2b:"B2B",b2b_page:"B2BPage",sbo:"SBO"}[rec.bookmaker] || rec.bookmaker;

  const div = document.createElement("div");
  div.className    = `harvest-card ${status}`;
  div.id           = `card-${rec.harvest_id}`;
  div.innerHTML = `
    <div class="card-header">
      <div class="card-title">${bkLabel} / ${rec.sport} / ${rec.mode}</div>
      <span class="card-status ${stCls}">${stIcon} ${status.toUpperCase()}</span>
    </div>
    <div class="card-meta">
      ID: <b>${rec.harvest_id}</b> &nbsp;|&nbsp;
      Task: <code style="color:var(--dim);font-size:10px">${rec.task_id || "—"}</code>
      &nbsp;|&nbsp; Started: ${(rec.started_at||"").slice(11,19)}
    </div>
    <div class="metrics">
      <div class="metric new">
        <div class="val" id="m-${rec.harvest_id}-total">0</div>
        <div class="lbl">Matches</div>
      </div>
      <div class="metric">
        <div class="val" id="m-${rec.harvest_id}-new" style="color:var(--cyan)">+0</div>
        <div class="lbl">New</div>
      </div>
      <div class="metric arb">
        <div class="val" id="m-${rec.harvest_id}-arbs">0</div>
        <div class="lbl">Arbs</div>
      </div>
      <div class="metric ev">
        <div class="val" id="m-${rec.harvest_id}-evs">0</div>
        <div class="lbl">EVs</div>
      </div>
    </div>
    <div class="card-log" id="log-${rec.harvest_id}"></div>
    <div class="card-btns" id="btns-${rec.harvest_id}">
      ${_cardBtns(rec.harvest_id, status)}
    </div>`;
  document.getElementById("active-list").prepend(div);
  cardCache[rec.harvest_id] = div;
  connectStream(rec.harvest_id);
}

function _cardBtns(hid, status) {
  if (status === "running") return `
    <button class="btn btn-yellow" onclick="pauseHarvest('${hid}')">⏸ Pause</button>
    <button class="btn btn-red"    onclick="stopHarvest('${hid}')">■ Stop</button>`;
  if (status === "paused") return `
    <button class="btn btn-green"  onclick="resumeHarvest('${hid}')">▶ Resume</button>
    <button class="btn btn-red"    onclick="stopHarvest('${hid}')">■ Stop</button>`;
  return `<span style="color:var(--dim);font-size:11px">${status}</span>`;
}

function updateCardMetrics(harvest_id, data) {
  const set = (id, val) => {
    const el = document.getElementById(`m-${harvest_id}-${id}`);
    if (el) el.textContent = val;
  };
  set("total", data.matches_total ?? data.matches_fetched ?? 0);
  set("new",  "+" + (data.matches_new ?? 0));
  set("arbs", data.arbs ?? data.arbs_found ?? 0);
  set("evs",  data.evs  ?? data.evs_found  ?? 0);
}

function logToCard(harvest_id, msg, cls = "") {
  const el = document.getElementById(`log-${harvest_id}`);
  if (!el) return;
  const ts  = new Date().toISOString().slice(11,19);
  const div = document.createElement("div");
  div.className = `entry ${cls}`;
  div.textContent = `[${ts}] ${msg}`;
  el.appendChild(div);
  while (el.children.length > 30) el.removeChild(el.firstChild);
  el.scrollTop = el.scrollHeight;
}

// ── Load active harvests ──────────────────────────────────────────────────────
async function loadActive() {
  try {
    const r = await fetch("/api/harvest/active");
    const d = await r.json();
    if (!d.ok) return;

    document.getElementById("active-count").textContent =
      `${d.running} running  ${d.paused} paused  ${d.finished} finished`;

    // Render active/paused cards
    d.harvests
      .filter(h => ["running","paused"].includes(h.status))
      .forEach(h => renderCard(h));

    // Update history table
    const tbody = document.getElementById("history-tbody");
    tbody.innerHTML = "";
    d.harvests.forEach(h => {
      const sc = {running:"var(--green)",paused:"var(--yellow)",
                  done:"var(--blue)",failed:"var(--red)",stopped:"var(--dim)"}[h.status]||"";
      const snap = h.cache_snapshot || {};
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td style="font-size:10px;color:var(--dim)">${h.harvest_id}</td>
        <td style="color:var(--cyan)">${h.bookmaker}</td>
        <td>${h.sport}</td>
        <td style="color:var(--dim)">${h.mode}</td>
        <td style="color:${sc};font-weight:bold">${h.status}</td>
        <td>${snap.count??0}</td>
        <td style="color:var(--yellow)">${snap.arbs??0}</td>
        <td style="color:var(--purple)">${snap.evs??0}</td>
        <td style="color:var(--dim);font-size:10px">${(h.started_at||"").slice(11,19)}</td>
        <td>
          ${h.status==="running"
            ? `<button class="btn btn-yellow" style="padding:2px 8px;font-size:10px"
                 onclick="pauseHarvest('${h.harvest_id}')">⏸</button>
               <button class="btn btn-red" style="padding:2px 8px;font-size:10px"
                 onclick="stopHarvest('${h.harvest_id}')">■</button>`
            : h.status==="paused"
              ? `<button class="btn btn-green" style="padding:2px 8px;font-size:10px"
                   onclick="resumeHarvest('${h.harvest_id}')">▶</button>`
              : "—"}
          <a href="/api/harvest/stream/${h.harvest_id}"
             style="color:var(--blue);font-size:10px;margin-left:4px" target="_blank">SSE</a>
        </td>`;
      tbody.appendChild(tr);
    });
  } catch(e) { console.warn(e); }
}

// Poll active harvests every 5 s
loadActive();
setInterval(loadActive, 5000);
</script>
</body>
</html>"""
    return Response(html, content_type="text/html")