from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, request, Response, stream_with_context, render_template_string

log = logging.getLogger("kinetic.activity")

bp_activity = Blueprint("activity", __name__, url_prefix="/api/activity")

IS_DEV = os.environ.get("FLASK_ENV", "production") != "production"


def _hash_ip(ip: str) -> str:
    if not ip:
        return ""
    return hashlib.sha256(ip.encode()).hexdigest()[:16]


def _redis():
    try:
        from app.workers.match_window_service import _redis as _r
        return _r()
    except Exception:
        return None


def _get_model():
    from app.models.tracking_model import UserActivityLog
    return UserActivityLog, UserActivityLog.__tablename__


# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD TEMPLATE — polished, lively, responsive
# ══════════════════════════════════════════════════════════════════════════════
DASHBOARD_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kinetic Activity Dashboard</title>
  <script src="https://unpkg.com/htmx.org@1.9.10"></script>
  <script src="https://unpkg.com/htmx.org/dist/ext/sse.js"></script>
  <style>
    :root {
      --bg: #0b1020;
      --bg-soft: rgba(14, 20, 39, 0.78);
      --panel: rgba(17, 24, 39, 0.82);
      --panel-strong: rgba(24, 31, 50, 0.94);
      --border: rgba(148, 163, 184, 0.18);
      --text: #e5eefc;
      --muted: #94a3b8;
      --accent: #7c3aed;
      --accent-2: #22c55e;
      --accent-3: #38bdf8;
      --danger: #ef4444;
      --warning: #f59e0b;
      --shadow: 0 20px 60px rgba(0, 0, 0, 0.35);
      --radius: 22px;
    }

    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(124, 58, 237, 0.30), transparent 28%),
        radial-gradient(circle at top right, rgba(56, 189, 248, 0.20), transparent 22%),
        radial-gradient(circle at bottom left, rgba(34, 197, 94, 0.12), transparent 24%),
        linear-gradient(180deg, #080b14 0%, #0b1020 100%);
    }

    .shell {
      max-width: 1440px;
      margin: 0 auto;
      padding: 24px;
    }

    .hero {
      position: relative;
      overflow: hidden;
      background: linear-gradient(135deg, rgba(124, 58, 237, 0.22), rgba(56, 189, 248, 0.12));
      border: 1px solid var(--border);
      border-radius: 28px;
      padding: 28px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }

    .hero::after {
      content: "";
      position: absolute;
      inset: -2px;
      background: radial-gradient(circle at 20% 20%, rgba(255,255,255,0.12), transparent 18%),
                  radial-gradient(circle at 80% 0%, rgba(255,255,255,0.08), transparent 14%);
      pointer-events: none;
    }

    .hero-top {
      display: flex;
      justify-content: space-between;
      gap: 20px;
      align-items: flex-start;
      position: relative;
      z-index: 1;
      flex-wrap: wrap;
    }

    .brand {
      display: flex;
      gap: 14px;
      align-items: center;
    }

    .brand-mark {
      width: 56px;
      height: 56px;
      border-radius: 18px;
      display: grid;
      place-items: center;
      font-size: 26px;
      background: linear-gradient(145deg, rgba(124,58,237,0.95), rgba(56,189,248,0.82));
      box-shadow: 0 12px 30px rgba(124,58,237,0.35);
    }

    .eyebrow {
      display: inline-flex;
      gap: 8px;
      align-items: center;
      font-size: 12px;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: #c4b5fd;
      font-weight: 700;
      margin-bottom: 10px;
    }

    h1 {
      margin: 0;
      font-size: clamp(28px, 4vw, 48px);
      line-height: 1.02;
      letter-spacing: -0.04em;
    }

    .subcopy {
      margin: 12px 0 0;
      color: rgba(226, 232, 240, 0.82);
      max-width: 72ch;
      line-height: 1.6;
    }

    .hero-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      justify-content: flex-end;
      position: relative;
      z-index: 1;
    }

    .pill {
      display: inline-flex;
      gap: 8px;
      align-items: center;
      padding: 10px 14px;
      border: 1px solid var(--border);
      background: rgba(255,255,255,0.06);
      border-radius: 999px;
      color: var(--text);
      text-decoration: none;
      font-size: 14px;
      transition: transform 0.18s ease, background 0.18s ease;
    }

    .pill:hover { transform: translateY(-1px); background: rgba(255,255,255,0.10); }

    .grid {
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 20px;
      margin-top: 20px;
    }

    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
      overflow: hidden;
    }

    .card-header {
      padding: 20px 20px 0;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
    }

    .card-title {
      margin: 0;
      font-size: 18px;
      letter-spacing: -0.02em;
    }

    .card-subtitle {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 14px;
    }

    .card-body { padding: 20px; }

    .stats {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 14px;
    }

    .stat {
      border-radius: 18px;
      padding: 16px;
      background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.02));
      border: 1px solid rgba(255,255,255,0.08);
      min-height: 112px;
      position: relative;
      overflow: hidden;
    }

    .stat::after {
      content: "";
      position: absolute;
      inset: auto -22px -30px auto;
      width: 110px;
      height: 110px;
      border-radius: 50%;
      background: radial-gradient(circle, rgba(255,255,255,0.16), transparent 68%);
      pointer-events: none;
    }

    .label {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 10px;
    }

    .value {
      font-size: 32px;
      font-weight: 800;
      letter-spacing: -0.05em;
      line-height: 1;
    }

    .delta {
      margin-top: 10px;
      font-size: 13px;
      color: #cbd5e1;
    }

    .delta strong { color: #fff; }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid rgba(255,255,255,0.10);
      background: rgba(255,255,255,0.05);
      color: #dbeafe;
    }

    .badge.ok { color: #bbf7d0; background: rgba(34,197,94,0.12); }
    .badge.live { color: #bae6fd; background: rgba(56,189,248,0.12); }
    .badge.warn { color: #fde68a; background: rgba(245,158,11,0.14); }

    .chips {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 16px;
    }

    .chip {
      padding: 8px 10px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid rgba(255,255,255,0.08);
      background: rgba(255,255,255,0.04);
      color: #cbd5e1;
    }

    .spark-wrap {
      display: grid;
      grid-template-columns: 1fr;
      gap: 16px;
    }

    .sparkline {
      display: grid;
      grid-template-columns: repeat(24, minmax(0, 1fr));
      gap: 6px;
      align-items: end;
      height: 180px;
      padding-top: 12px;
    }

    .bar-col {
      height: 100%;
      display: flex;
      flex-direction: column;
      justify-content: end;
      gap: 8px;
      align-items: center;
    }

    .bar {
      width: 100%;
      min-height: 8px;
      border-radius: 12px 12px 6px 6px;
      background: linear-gradient(180deg, rgba(56,189,248,0.95), rgba(124,58,237,0.95));
      box-shadow: 0 10px 20px rgba(56,189,248,0.16);
      transition: transform 0.18s ease;
    }

    .bar:hover { transform: translateY(-2px); }

    .bar-label {
      writing-mode: vertical-rl;
      transform: rotate(180deg);
      font-size: 10px;
      color: var(--muted);
      height: 52px;
      white-space: nowrap;
      opacity: 0.75;
    }

    .layout-2 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 20px;
      margin-top: 20px;
    }

    .table {
      width: 100%;
      border-collapse: collapse;
    }

    .table th, .table td {
      padding: 12px 0;
      border-bottom: 1px solid rgba(255,255,255,0.08);
      text-align: left;
      vertical-align: top;
      font-size: 14px;
    }

    .table th { color: var(--muted); font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; }
    .table td { color: #e2e8f0; }

    .table tr:last-child td { border-bottom: 0; }

    .list {
      display: grid;
      gap: 10px;
    }

    .list-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(255,255,255,0.04);
      border: 1px solid rgba(255,255,255,0.06);
    }

    .list-item strong { display: block; }
    .list-item span { color: var(--muted); font-size: 12px; }

    .feed {
      display: grid;
      gap: 10px;
    }

    .feed-item {
      display: flex;
      gap: 12px;
      align-items: flex-start;
      padding: 14px;
      border-radius: 18px;
      background: rgba(255,255,255,0.04);
      border: 1px solid rgba(255,255,255,0.06);
    }

    .dot {
      width: 12px;
      height: 12px;
      border-radius: 999px;
      margin-top: 4px;
      background: linear-gradient(180deg, #22c55e, #38bdf8);
      box-shadow: 0 0 0 6px rgba(56,189,248,0.09);
      flex: 0 0 auto;
    }

    .feed-main { flex: 1; min-width: 0; }
    .feed-title { margin: 0; font-weight: 700; }
    .feed-meta { margin: 6px 0 0; color: var(--muted); font-size: 12px; line-height: 1.5; }
    .feed-pill { align-self: center; }

    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
    }

    .button {
      appearance: none;
      border: 0;
      cursor: pointer;
      border-radius: 14px;
      padding: 11px 14px;
      color: #fff;
      font-weight: 700;
      background: linear-gradient(135deg, rgba(124,58,237,1), rgba(56,189,248,1));
      box-shadow: 0 12px 24px rgba(124,58,237,0.25);
      transition: transform 0.18s ease, filter 0.18s ease;
    }

    .button:hover { transform: translateY(-1px); filter: brightness(1.05); }

    .button.secondary {
      background: rgba(255,255,255,0.07);
      border: 1px solid rgba(255,255,255,0.10);
      box-shadow: none;
    }

    .empty, .loading, .error-box {
      padding: 18px;
      border-radius: 16px;
      border: 1px dashed rgba(255,255,255,0.16);
      color: var(--muted);
      background: rgba(255,255,255,0.03);
    }

    .live-grid {
      display: grid;
      gap: 14px;
    }

    .kpi-mini {
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 12px;
      margin-top: 14px;
    }

    .mini-box {
      border-radius: 16px;
      padding: 14px;
      background: rgba(255,255,255,0.04);
      border: 1px solid rgba(255,255,255,0.08);
    }

    .mini-box .label { margin-bottom: 8px; }
    .mini-box .value { font-size: 24px; }

    .footer-note {
      margin-top: 18px;
      color: var(--muted);
      font-size: 12px;
      text-align: center;
    }

    @media (max-width: 1100px) {
      .grid, .layout-2 { grid-template-columns: 1fr; }
      .stats { grid-template-columns: 1fr; }
      .sparkline { grid-template-columns: repeat(12, minmax(0, 1fr)); height: 220px; }
    }

    @media (max-width: 640px) {
      .shell { padding: 14px; }
      .hero, .card { border-radius: 20px; }
      .card-header, .card-body { padding-left: 16px; padding-right: 16px; }
      .hero { padding: 20px; }
      .sparkline { grid-template-columns: repeat(8, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="hero-top">
        <div class="brand">
          <div class="brand-mark">⚡</div>
          <div>
            <div class="eyebrow">Realtime activity console</div>
            <h1>Kinetic Activity Dashboard</h1>
            <p class="subcopy">
              A live view of what users are doing right now — sessions, traffic spikes, top actions, sports interest, and page engagement.
            </p>
          </div>
        </div>

        <div class="hero-actions">
          <span class="badge live">● Live stream ready</span>
          <a class="pill" href="/api/activity/summary?hours={{ period_hours }}" target="_blank">Open JSON summary</a>
          <button class="button secondary" hx-get="/api/activity/summary?hours={{ period_hours }}" hx-target="#summary-panel" hx-swap="outerHTML">Refresh summary</button>
          <button class="button" hx-delete="/api/activity/clear" hx-confirm="Delete ALL activity logs?">Clear logs</button>
        </div>
      </div>
    </section>

    <div class="grid">
      <section class="card" id="summary-panel">
        <div class="card-header">
          <div>
            <h2 class="card-title">Overview</h2>
            <p class="card-subtitle">Last {{ period_hours }} hours • Generated from the tracking table</p>
          </div>
          <div class="toolbar">
            <span class="badge ok">{{ total_sessions }} sessions</span>
            <span class="badge">{{ total_events }} events</span>
          </div>
        </div>

        <div class="card-body">
          <div class="stats">
            <div class="stat">
              <div class="label">Total events</div>
              <div class="value">{{ total_events }}</div>
              <div class="delta">All logged interactions in the selected window.</div>
            </div>
            <div class="stat">
              <div class="label">Unique sessions</div>
              <div class="value">{{ total_sessions }}</div>
              <div class="delta">Distinct browsing or app sessions seen.</div>
            </div>
            <div class="stat">
              <div class="label">Peak hour</div>
              <div class="value">{{ peak_hour }}</div>
              <div class="delta">Busiest hour from the hourly sparkline.</div>
            </div>
          </div>

          <div class="chips">
            <span class="chip">Period: {{ period_hours }}h</span>
            <span class="chip">Events tracked: navigation, engagement, conversion</span>
            <span class="chip">Data freshness: live / cached</span>
            <span class="chip">Top insight: {{ top_insight }}</span>
          </div>

          <div class="spark-wrap">
            <div style="display:flex;justify-content:space-between;align-items:flex-end;gap:12px;flex-wrap:wrap;margin-top:18px;">
              <div>
                <h3 class="card-title" style="margin-bottom:4px;">Hourly activity</h3>
                <div class="card-subtitle">A compact pulse of traffic over the past 24 hours</div>
              </div>
              <span class="badge warn">Hover bars for details</span>
            </div>

            <div class="sparkline" aria-label="Hourly activity sparkline">
              {% for point in sparkline %}
              <div class="bar-col" title="{{ point.hour }} • {{ point.count }} events">
                <div class="bar" style="height: {{ point.count * 3 + 8 }}px;"></div>
                <div class="bar-label">{{ point.hour_label }}</div>
              </div>
              {% endfor %}
            </div>
          </div>
        </div>
      </section>

      <section class="card">
        <div class="card-header">
          <div>
            <h2 class="card-title">Live snapshot</h2>
            <p class="card-subtitle">Pulled from the summary cache every few seconds</p>
          </div>
          <span class="badge live">SSE stream</span>
        </div>
        <div class="card-body live-grid" hx-ext="sse" sse-connect="/api/activity/stream">
          <div class="loading" sse-swap="message" hx-swap="innerHTML">
            Waiting for live data…
          </div>

          <div class="kpi-mini">
            <div class="mini-box">
              <div class="label">Active now</div>
              <div class="value">{{ active_now }}</div>
            </div>
            <div class="mini-box">
              <div class="label">Cached</div>
              <div class="value">{{ "Yes" if cached else "No" }}</div>
            </div>
          </div>

          <div class="footer-note">This panel feels alive once the SSE endpoint is feeding new summaries.</div>
        </div>
      </section>
    </div>

    <div class="layout-2">
      <section class="card">
        <div class="card-header">
          <div>
            <h2 class="card-title">Top event types</h2>
            <p class="card-subtitle">The big picture across navigation, search, engagement, and conversion</p>
          </div>
        </div>
        <div class="card-body">
          {% if top_event_types %}
          <table class="table">
            <thead>
              <tr>
                <th>Type</th>
                <th>Count</th>
                <th>Unique sessions</th>
              </tr>
            </thead>
            <tbody>
              {% for row in top_event_types %}
              <tr>
                <td><span class="badge">{{ row.type }}</span></td>
                <td>{{ row.count }}</td>
                <td>{{ row.unique_sessions }}</td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
          {% else %}
          <div class="empty">No event types yet for this time window.</div>
          {% endif %}
        </div>
      </section>

      <section class="card">
        <div class="card-header">
          <div>
            <h2 class="card-title">Top pages</h2>
            <p class="card-subtitle">Pages or resources users keep returning to</p>
          </div>
        </div>
        <div class="card-body">
          {% if top_pages %}
          <div class="list">
            {% for row in top_pages %}
            <div class="list-item">
              <div>
                <strong>{{ row.page }}</strong>
                <span>{{ row.unique_sessions }} unique sessions</span>
              </div>
              <div class="badge ok">{{ row.views }} views</div>
            </div>
            {% endfor %}
          </div>
          {% else %}
          <div class="empty">No page views captured yet.</div>
          {% endif %}
        </div>
      </section>
    </div>

    <div class="layout-2">
      <section class="card">
        <div class="card-header">
          <div>
            <h2 class="card-title">Top sports</h2>
            <p class="card-subtitle">Which sports are drawing the most attention</p>
          </div>
        </div>
        <div class="card-body">
          {% if top_sports %}
          <div class="list">
            {% for row in top_sports %}
            <div class="list-item">
              <div>
                <strong>{{ row.sport }}</strong>
                <span>Audience interest in the selected window</span>
              </div>
              <div class="badge live">{{ row.count }}</div>
            </div>
            {% endfor %}
          </div>
          {% else %}
          <div class="empty">No sport metadata captured yet.</div>
          {% endif %}
        </div>
      </section>

      <section class="card">
        <div class="card-header">
          <div>
            <h2 class="card-title">Recent live note</h2>
            <p class="card-subtitle">A more human-readable stream card</p>
          </div>
        </div>
        <div class="card-body" hx-ext="sse" sse-connect="/api/activity/stream">
          <div class="feed" sse-swap="message" hx-swap="innerHTML">
            <div class="empty">Live updates will appear here as soon as new summary payloads are emitted.</div>
          </div>
        </div>
      </section>
    </div>

    <div class="footer-note">
      Built for fast glanceability, live behavior monitoring, and cleaner operational debugging.
    </div>
  </div>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD ROUTE
# ══════════════════════════════════════════════════════════════════════════════
@bp_activity.route("/dashboard", methods=["GET"])
def activity_dashboard():
    hours = min(max(1, int(request.args.get("hours", 24))), 168)

    try:
        _, table_name = _get_model()
        from app.extensions import db
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

        totals = db.session.execute(db.text(f"""
            SELECT COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name} WHERE occurred_at >= :since
        """), {"since": since}).fetchone()

        sparkline_rows = db.session.execute(db.text(f"""
            SELECT date_trunc('hour', occurred_at) AS hour_bucket, COUNT(*) AS count
            FROM {table_name}
            WHERE occurred_at >= NOW() - INTERVAL '24 hours'
            GROUP BY 1
            ORDER BY 1 ASC
        """)).fetchall()

        top_event_types = db.session.execute(db.text(f"""
            SELECT event_type, COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name}
            WHERE occurred_at >= :since
            GROUP BY event_type
            ORDER BY 2 DESC
            LIMIT 6
        """), {"since": since}).fetchall()

        top_pages = db.session.execute(db.text(f"""
            SELECT resource, COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name}
            WHERE event_name = 'page_view'
              AND occurred_at >= :since
              AND resource IS NOT NULL AND resource != ''
            GROUP BY resource
            ORDER BY 2 DESC
            LIMIT 6
        """), {"since": since}).fetchall()

        top_sports = db.session.execute(db.text(f"""
            SELECT meta_data->>'sport', COUNT(*)
            FROM {table_name}
            WHERE occurred_at >= :since
              AND meta_data->>'sport' IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 6
        """), {"since": since}).fetchall()

        peak_hour = "—"
        if sparkline_rows:
            peak = max(sparkline_rows, key=lambda r: int(r[1]))
            peak_hour = peak[0].strftime("%H:00") if peak[0] else "—"

        top_insight = "No dominant pattern yet"
        if top_event_types:
            top_insight = f"{top_event_types[0][0]} leads activity"

        summary_data = {
            "total_events": int(totals[0]) if totals else 0,
            "total_sessions": int(totals[1]) if totals else 0,
            "active_now": 0,
            "cached": False,
            "peak_hour": peak_hour,
            "top_insight": top_insight,
            "sparkline": [
                {
                    "hour": r[0].strftime("%Y-%m-%dT%H:00Z") if r[0] else "",
                    "hour_label": r[0].strftime("%H") if r[0] else "",
                    "count": int(r[1]),
                }
                for r in sparkline_rows
            ],
            "top_event_types": [
                {"type": r[0] or "unknown", "count": int(r[1]), "unique_sessions": int(r[2])}
                for r in top_event_types
            ],
            "top_pages": [
                {"page": r[0], "views": int(r[1]), "unique_sessions": int(r[2])}
                for r in top_pages
            ],
            "top_sports": [
                {"sport": r[0], "count": int(r[1])}
                for r in top_sports if r[0]
            ],
            "period_hours": hours,
        }
    except Exception as e:
        log.error("Dashboard initial load error: %s", e)
        summary_data = {
            "total_events": 0,
            "total_sessions": 0,
            "active_now": 0,
            "cached": False,
            "peak_hour": "—",
            "top_insight": "Unavailable",
            "sparkline": [],
            "top_event_types": [],
            "top_pages": [],
            "top_sports": [],
            "period_hours": hours,
        }

    return render_template_string(DASHBOARD_TEMPLATE, **summary_data)


# ══════════════════════════════════════════════════════════════════════════════
# DEBUG
# ══════════════════════════════════════════════════════════════════════════════
@bp_activity.route("/debug", methods=["GET"])
def activity_debug():
    info: dict = {}
    try:
        Model, table_name = _get_model()
        info["table_name"] = table_name
        info["model_columns"] = [c.name for c in Model.__table__.columns]
    except Exception as e:
        return jsonify({"model_error": str(e)}), 500

    try:
        from app.extensions import db
        info["row_count"] = db.session.execute(db.text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
    except Exception as e:
        info["count_error"] = str(e)

    try:
        from app.extensions import db
        cols = db.session.execute(db.text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = :t ORDER BY ordinal_position
        """), {"t": table_name}).fetchall()
        info["column_types"] = {r[0]: {"type": r[1], "nullable": r[2]} for r in cols}
    except Exception as e:
        info["column_types_error"] = str(e)

    try:
        from app.extensions import db
        rows = db.session.execute(db.text(f"""
            SELECT event_name, event_type, session_id, occurred_at
            FROM {table_name} ORDER BY occurred_at DESC LIMIT 3
        """)).fetchall()
        info["last_rows"] = [
            {"event": r[0], "type": r[1], "session": r[2], "at": str(r[3])}
            for r in rows
        ]
    except Exception as e:
        info["rows_error"] = str(e)

    try:
        from app.extensions import db
        test = Model(
            user_id=None,
            session_id="debug_test",
            event_name="debug_ping",
            event_type="debug",
            resource="/debug",
            properties={"test": True},
            meta_data={},
            ip_address="debug",
            user_agent="debug",
            occurred_at=datetime.now(timezone.utc),
        )
        db.session.add(test)
        db.session.commit()
        info["write_test"] = "OK"
    except Exception as e:
        info["write_test"] = f"FAILED: {e}"
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass

    return jsonify(info)


# ══════════════════════════════════════════════════════════════════════════════
# LOG
# ══════════════════════════════════════════════════════════════════════════════
_EVENT_TYPE_MAP: dict[str, str] = {
    "page_view": "navigation",
    "tab_switch": "navigation",
    "sport_switch": "navigation",
    "match_click": "engagement",
    "market_view": "engagement",
    "arb_view": "engagement",
    "result_view": "engagement",
    "live_view": "engagement",
    "match_watch_add": "engagement",
    "search": "search",
    "filter_change": "filter",
    "load_more": "pagination",
    "signup_click": "conversion",
    "upgrade_view": "conversion",
}


def _event_type(event_name: str) -> str:
    return _EVENT_TYPE_MAP.get(event_name, "custom")


@bp_activity.route("/log", methods=["POST"])
def log_activity():
    body = request.get_json(silent=True) or {}
    events = body.get("events", [])

    if not events or not isinstance(events, list):
        return jsonify({"ok": True, "written": 0, "reason": "empty"})

    raw_ip = (
        request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.remote_addr or ""
    )
    ip_hash = _hash_ip(raw_ip)
    ua = request.headers.get("User-Agent", "")[:256]

    utm = {
        "utm_source": request.args.get("utm_source") or body.get("utm_source") or "",
        "utm_medium": request.args.get("utm_medium") or body.get("utm_medium") or "",
        "utm_campaign": request.args.get("utm_campaign") or body.get("utm_campaign") or "",
        "utm_content": request.args.get("utm_content") or body.get("utm_content") or "",
        "utm_term": request.args.get("utm_term") or body.get("utm_term") or "",
    }

    written = 0
    error = None

    try:
        Model, _ = _get_model()
        from app.extensions import db

        rows = []
        for ev in events[:50]:
            if not isinstance(ev, dict):
                continue

            event_name = str(ev.get("event") or "unknown")[:64]
            props = dict(ev.get("properties") or {})
            resource = str(ev.get("url") or props.get("page") or "")[:512]
            session_id = str(ev.get("session_id") or "")[:128]

            for key in ("user_id", "email", "phone", "name", "ip"):
                props.pop(key, None)

            meta = {"page": props.pop("page", None), "sport": props.pop("sport", None)}
            meta = {k: v for k, v in meta.items() if v is not None}

            try:
                ts = datetime.fromtimestamp(int(ev.get("ts", 0)) / 1000, tz=timezone.utc)
            except Exception:
                ts = datetime.now(timezone.utc)

            rows.append(Model(
                user_id=None,
                session_id=session_id,
                event_name=event_name,
                event_type=_event_type(event_name),
                resource=resource,
                properties=props,
                meta_data=meta,
                ip_address=ip_hash,
                user_agent=ua,
                occurred_at=ts,
                **{k: v[:128] for k, v in utm.items() if v},
            ))

        db.session.bulk_save_objects(rows)
        db.session.commit()
        written = len(rows)

    except Exception as e:
        error = str(e)
        log.error("Activity log write failed: %s", e)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass

    if error and IS_DEV:
        return jsonify({"ok": False, "written": 0, "error": error}), 500

    return jsonify({"ok": True, "written": written})


# ══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
@bp_activity.route("/summary", methods=["GET"])
def activity_summary():
    hours = min(max(1, int(request.args.get("hours", 24))), 168)
    force = request.args.get("force", "false").lower() == "true"

    try:
        _, table_name = _get_model()
    except Exception as e:
        return jsonify({"error": f"Model import failed: {e}"}), 500

    cache_key = f"kinetic:activity:summary:{hours}h"
    r = _redis()
    if r and not force:
        try:
            cached = r.get(cache_key)
            if cached:
                data = json.loads(cached)
                data["cached"] = True
                return jsonify(data)
        except Exception:
            pass

    try:
        from app.extensions import db
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

        totals = db.session.execute(db.text(f"""
            SELECT COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name} WHERE occurred_at >= :since
        """), {"since": since}).fetchone()

        active_now = db.session.execute(db.text(f"""
            SELECT COUNT(DISTINCT session_id)
            FROM {table_name}
            WHERE occurred_at >= NOW() - INTERVAL '15 minutes'
        """)).scalar() or 0

        top_events = db.session.execute(db.text(f"""
            SELECT event_name, COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name} WHERE occurred_at >= :since
            GROUP BY event_name ORDER BY 2 DESC LIMIT 30
        """), {"since": since}).fetchall()

        top_event_types = db.session.execute(db.text(f"""
            SELECT event_type, COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name} WHERE occurred_at >= :since
            GROUP BY event_type ORDER BY 2 DESC
        """), {"since": since}).fetchall()

        top_sports = db.session.execute(db.text(f"""
            SELECT meta_data->>'sport', COUNT(*)
            FROM {table_name}
            WHERE occurred_at >= :since
              AND meta_data->>'sport' IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """), {"since": since}).fetchall()

        top_matches = db.session.execute(db.text(f"""
            SELECT
                properties->>'home_team',
                properties->>'away_team',
                meta_data->>'sport',
                COUNT(*)
            FROM {table_name}
            WHERE event_name = 'match_click'
              AND occurred_at >= :since
              AND properties->>'home_team' IS NOT NULL
            GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 10
        """), {"since": since}).fetchall()

        top_pages = db.session.execute(db.text(f"""
            SELECT resource, COUNT(*), COUNT(DISTINCT session_id)
            FROM {table_name}
            WHERE event_name = 'page_view'
              AND occurred_at >= :since
              AND resource IS NOT NULL AND resource != ''
            GROUP BY resource ORDER BY 2 DESC LIMIT 10
        """), {"since": since}).fetchall()

        sparkline = db.session.execute(db.text(f"""
            SELECT date_trunc('hour', occurred_at), COUNT(*)
            FROM {table_name}
            WHERE occurred_at >= NOW() - INTERVAL '24 hours'
            GROUP BY 1 ORDER BY 1 ASC
        """)).fetchall()

        result = {
            "period": f"{hours}h",
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "cached": False,
            "table": table_name,
            "summary": {
                "total_events": int(totals[0]) if totals else 0,
                "total_sessions": int(totals[1]) if totals else 0,
                "active_now": int(active_now),
            },
            "top_events": [
                {"event": r[0], "count": int(r[1]), "unique_sessions": int(r[2])}
                for r in top_events
            ],
            "top_event_types": [
                {"type": r[0], "count": int(r[1]), "unique_sessions": int(r[2])}
                for r in top_event_types
            ],
            "top_sports": [
                {"sport": r[0], "count": int(r[1])}
                for r in top_sports if r[0]
            ],
            "top_matches": [
                {"match": f"{r[0]} v {r[1]}", "sport": r[2] or "", "clicks": int(r[3])}
                for r in top_matches if r[0] and r[1]
            ],
            "top_pages": [
                {"page": r[0], "views": int(r[1]), "unique_sessions": int(r[2])}
                for r in top_pages if r[0]
            ],
            "hourly_sparkline": [
                {"hour": r[0].strftime("%Y-%m-%dT%H:00Z") if r[0] else "", "count": int(r[1])}
                for r in sparkline
            ],
        }

        if r:
            try:
                r.setex(cache_key, 60, json.dumps(result))
            except Exception:
                pass

        return jsonify(result)
    except Exception as e:
        log.error("activity_summary error: %s", e)
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# STREAM
# ══════════════════════════════════════════════════════════════════════════════
@bp_activity.route("/stream", methods=["GET"])
def activity_stream():
    def generate():
        while True:
            try:
                r = _redis()
                if r:
                    cached = r.get("kinetic:activity:summary:24h")
                    if cached:
                        data_str = cached.decode("utf-8") if isinstance(cached, bytes) else cached
                        yield f"data: {data_str}\n\n"
                    else:
                        yield ": ping\n\n"
                else:
                    yield ": ping\n\n"
            except Exception as e:
                log.error("SSE error: %s", e)
            time.sleep(5)

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


# ══════════════════════════════════════════════════════════════════════════════
# DELETE / CLEAR
# ══════════════════════════════════════════════════════════════════════════════
@bp_activity.route("/clear", methods=["DELETE"])
def activity_clear():
    try:
        from app.extensions import db
        _, table_name = _get_model()
        deleted = db.session.execute(db.text(f"DELETE FROM {table_name}")).rowcount
        db.session.commit()
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as e:
        log.error("Failed to clear activity logs: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp_activity.route("/<int:record_id>", methods=["DELETE"])
def activity_delete(record_id):
    try:
        from app.extensions import db
        _, table_name = _get_model()
        deleted = db.session.execute(
            db.text(f"DELETE FROM {table_name} WHERE id = :id"),
            {"id": record_id},
        ).rowcount
        db.session.commit()
        return jsonify({"ok": True, "deleted": deleted})
    except Exception as e:
        log.error("Failed to delete activity log %s: %s", record_id, e)
        return jsonify({"ok": False, "error": str(e)}), 500
