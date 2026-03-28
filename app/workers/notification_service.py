"""
app/workers/notification_service.py
=====================================
NotificationService — dispatches arb / EV alerts to eligible users.

Called from:
    celery_tasks.dispatch_notifications(match_id, event_type)

Delivery channels (in priority order):
    1. Email   → send_async_email Celery task  (always attempted if email_enabled)
    2. WhatsApp → send_message Celery task      (only if wa_number set on user)

Templates are rendered with Jinja2 from the templates/notifications/ directory.
Falls back to a plain-text template if HTML file not found.

User eligibility (checked in dispatch_notifications before calling here):
    - Customer.is_active == True
    - NotificationPref.email_enabled == True
    - arb profit_pct >= pref.arb_min_profit  (or ev_pct >= pref.ev_min_edge)
    - sports_filter passes (if set)

Rate limiting:
    Redis key:  notif_throttle:{user_id}:{match_id}
    TTL:        THROTTLE_TTL_S (default 30 min) — one alert per match per user per window
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.models.customer import Customer
    from app.models.odds_model import ArbitrageOpportunity, EVOpportunity

log = logging.getLogger(__name__)

THROTTLE_TTL_S = int(os.getenv("NOTIF_THROTTLE_S", "1800"))   # 30 min default
APP_NAME       = os.getenv("APP_NAME", "OddsKenya")
APP_URL        = os.getenv("APP_URL",  "https://oddskenya.com")


# ─────────────────────────────────────────────────────────────────────────────
# Redis helper (shared with celery_tasks)
# ─────────────────────────────────────────────────────────────────────────────

def _redis():
    import redis as _r
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return _r.from_url(url, decode_responses=True,
                       socket_timeout=3, socket_connect_timeout=3)


def _throttle_key(user_id: int, match_id: int) -> str:
    return f"notif_throttle:{user_id}:{match_id}"


def _is_throttled(user_id: int, match_id: int) -> bool:
    try:
        return bool(_redis().get(_throttle_key(user_id, match_id)))
    except Exception:
        return False   # fail open — send if Redis unavailable


def _mark_throttled(user_id: int, match_id: int) -> None:
    try:
        _redis().setex(_throttle_key(user_id, match_id), THROTTLE_TTL_S, "1")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Template helpers
# ─────────────────────────────────────────────────────────────────────────────

def _render_email(
    match_label:  str,
    arbs:         list,
    evs:          list,
    event_type:   str,
    user_name:    str,
) -> tuple[str, str]:
    """
    Returns (subject, html_body).
    Tries Jinja2 template first, falls back to inline HTML.
    """
    subject = _subject(match_label, arbs, evs, event_type)
    html    = _inline_html(match_label, arbs, evs, event_type, user_name)

    try:
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        tpl_dir = os.path.join(os.path.dirname(__file__), "..", "templates", "notifications")
        env     = Environment(
            loader=FileSystemLoader(tpl_dir),
            autoescape=select_autoescape(["html"]),
        )
        tpl_name = "arb_alert.html" if arbs else "ev_alert.html"
        tpl      = env.get_template(tpl_name)
        html     = tpl.render(
            match_label=match_label, arbs=arbs, evs=evs,
            event_type=event_type, user_name=user_name,
            app_name=APP_NAME, app_url=APP_URL,
            now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        )
    except Exception:
        pass   # keep inline fallback

    return subject, html


def _subject(match_label: str, arbs: list, evs: list, event_type: str) -> str:
    if arbs and evs:
        best_arb = max(arbs, key=lambda a: a.profit_pct or 0)
        return (f"[{APP_NAME}] Arb + EV: {match_label} — "
                f"{best_arb.profit_pct:.1f}% profit")
    if arbs:
        best_arb = max(arbs, key=lambda a: a.profit_pct or 0)
        return (f"[{APP_NAME}] Arb opportunity: {match_label} — "
                f"{best_arb.profit_pct:.1f}% profit")
    if evs:
        best_ev = max(evs, key=lambda e: e.ev_pct or 0)
        return (f"[{APP_NAME}] Value bet: {match_label} — "
                f"{best_ev.ev_pct:.1f}% EV @ {best_ev.bookmaker}")
    return f"[{APP_NAME}] Odds alert: {match_label}"


def _inline_html(
    match_label: str,
    arbs:        list,
    evs:         list,
    event_type:  str,
    user_name:   str,
) -> str:
    """Minimal inline HTML email — no external dependencies."""
    rows = []

    for arb in arbs:
        legs_text = " | ".join(
            f"{l['selection']} @ {l['price']:.2f} ({l['bookmaker']})"
            for l in (arb.legs_json or [])
        )
        rows.append(f"""
        <tr>
          <td style="padding:8px;border-bottom:1px solid #1e3050;">
            <strong style="color:#4ade80">ARB</strong>
            {arb.market} — {arb.specifier or ""}
          </td>
          <td style="padding:8px;border-bottom:1px solid #1e3050;color:#4ade80;font-weight:bold;">
            +{arb.profit_pct:.2f}%
          </td>
          <td style="padding:8px;border-bottom:1px solid #1e3050;font-size:12px;color:rgba(255,255,255,0.6);">
            {legs_text}
          </td>
        </tr>""")

    for ev in evs:
        rows.append(f"""
        <tr>
          <td style="padding:8px;border-bottom:1px solid #1e3050;">
            <strong style="color:#fbbf24">EV</strong>
            {ev.market} — {ev.selection} @ {ev.bookmaker}
          </td>
          <td style="padding:8px;border-bottom:1px solid #1e3050;color:#fbbf24;font-weight:bold;">
            +{ev.ev_pct:.2f}% EV
          </td>
          <td style="padding:8px;border-bottom:1px solid #1e3050;font-size:12px;color:rgba(255,255,255,0.6);">
            Kelly: {ev.kelly_fraction:.1%} | Fair prob: {ev.fair_prob:.1%}
          </td>
        </tr>""")

    table = "\n".join(rows) if rows else "<tr><td>No details available.</td></tr>"
    ts    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>{APP_NAME} Alert</title></head>
<body style="background:#060e1a;color:#fff;font-family:monospace;margin:0;padding:20px;">
  <div style="max-width:600px;margin:0 auto;">

    <div style="border-left:3px solid #1FB954;padding:0 0 0 16px;margin-bottom:20px;">
      <div style="font-size:11px;color:#1FB954;letter-spacing:2px;margin-bottom:4px;">{APP_NAME.upper()}</div>
      <div style="font-size:18px;font-weight:bold;">{match_label}</div>
      <div style="font-size:11px;color:rgba(255,255,255,0.4);margin-top:4px;">{ts}</div>
    </div>

    <table width="100%" cellspacing="0" cellpadding="0"
           style="border:1px solid #1e3050;border-radius:4px;font-size:13px;">
      <thead>
        <tr style="background:#0a1628;">
          <th style="padding:8px;text-align:left;color:rgba(255,255,255,0.5);font-size:10px;letter-spacing:1.5px;">TYPE / MARKET</th>
          <th style="padding:8px;text-align:left;color:rgba(255,255,255,0.5);font-size:10px;letter-spacing:1.5px;">EDGE</th>
          <th style="padding:8px;text-align:left;color:rgba(255,255,255,0.5);font-size:10px;letter-spacing:1.5px;">DETAILS</th>
        </tr>
      </thead>
      <tbody>{table}</tbody>
    </table>

    <div style="margin-top:20px;font-size:11px;color:rgba(255,255,255,0.3);border-top:1px solid #1e3050;padding-top:12px;">
      Sent to {user_name} · <a href="{APP_URL}/odds" style="color:#1FB954;">View on {APP_NAME}</a>
      · <a href="{APP_URL}/settings/notifications" style="color:rgba(255,255,255,0.3);">Manage alerts</a>
    </div>
  </div>
</body>
</html>"""


def _whatsapp_text(match_label: str, arbs: list, evs: list) -> str:
    """Plain-text WhatsApp message."""
    lines = [f"🎯 *{APP_NAME}* — {match_label}\n"]

    for arb in arbs[:3]:   # cap at 3 to keep message short
        legs = " | ".join(
            f"{l['selection']} {l['price']:.2f} ({l['bookmaker']})"
            for l in (arb.legs_json or [])
        )
        lines.append(f"✅ *ARB +{arb.profit_pct:.2f}%* — {arb.market}\n   {legs}")

    for ev in evs[:3]:
        lines.append(
            f"📈 *EV +{ev.ev_pct:.2f}%* — {ev.market} {ev.selection} "
            f"@ {ev.offered_price:.2f} ({ev.bookmaker})"
        )

    lines.append(f"\n🔗 {APP_URL}/odds")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Main service class
# ─────────────────────────────────────────────────────────────────────────────

class NotificationService:
    """
    Stateless service — all methods are classmethods.

    Usage:
        NotificationService.send_alert(
            user=user,
            match_label="Arsenal v Chelsea",
            arbs=[arb1, arb2],
            evs=[ev1],
            event_type="arb",
        )
    """

    @classmethod
    def send_alert(
        cls,
        user:        "Customer",
        match_label: str,
        arbs:        list["ArbitrageOpportunity"],
        evs:         list["EVOpportunity"],
        event_type:  str = "arb",
    ) -> bool:
        """
        Dispatch email + optional WhatsApp for one user.
        Returns True if at least one channel was dispatched.

        Rate-limited: one alert per user per match per THROTTLE_TTL_S window.
        Match identity is derived from the first arb/ev match_id.
        """
        if not arbs and not evs:
            return False

        match_id = (arbs[0].match_id if arbs else evs[0].match_id)

        # ── Rate limiting ────────────────────────────────────────────────────
        if _is_throttled(user.id, match_id):
            log.debug("throttled: user=%d match=%d", user.id, match_id)
            return False

        sent = False

        # ── Email ────────────────────────────────────────────────────────────
        if getattr(user, "email", None):
            try:
                subject, html = _render_email(
                    match_label=match_label,
                    arbs=arbs, evs=evs,
                    event_type=event_type,
                    user_name=getattr(user, "name", None) or user.email.split("@")[0],
                )
                from app.workers.celery_tasks import send_async_email
                send_async_email.apply_async(
                    args=[subject, [user.email], html, "html"],
                    queue="notify",
                )
                sent = True
                log.info("email queued: user=%d match=%d subject=%r", user.id, match_id, subject)
            except Exception as exc:
                log.error("email dispatch error: user=%d: %s", user.id, exc)

        # ── WhatsApp ─────────────────────────────────────────────────────────
        wa_number = getattr(user, "whatsapp_number", None) or getattr(user, "phone", None)
        if wa_number:
            try:
                text = _whatsapp_text(match_label, arbs, evs)
                from app.workers.celery_tasks import send_message
                send_message.apply_async(
                    args=[text, wa_number],
                    queue="notify",
                )
                sent = True
                log.info("whatsapp queued: user=%d match=%d", user.id, match_id)
            except Exception as exc:
                log.error("whatsapp dispatch error: user=%d: %s", user.id, exc)

        if sent:
            _mark_throttled(user.id, match_id)

        return sent

    @classmethod
    def send_bulk(
        cls,
        users:       list["Customer"],
        match_label: str,
        arbs:        list["ArbitrageOpportunity"],
        evs:         list["EVOpportunity"],
        event_type:  str = "arb",
    ) -> int:
        """
        Send to a list of users. Returns count of users notified.
        Each user is independently throttled.
        """
        sent = 0
        for user in users:
            try:
                if cls.send_alert(user, match_label, arbs, evs, event_type):
                    sent += 1
            except Exception as exc:
                log.error("send_bulk: user=%d: %s", user.id, exc)
        return sent

    @classmethod
    def send_system(
        cls,
        subject:     str,
        message:     str,
        recipients:  list[str] | None = None,
    ) -> None:
        """
        Send a plain system notification (e.g. worker health, harvest failure).
        recipients defaults to ADMIN_EMAIL from env.
        """
        if not recipients:
            admin = os.getenv("ADMIN_EMAIL")
            if not admin:
                return
            recipients = [admin]
        try:
            from app.workers.celery_tasks import send_async_email
            html = f"""<div style="font-family:monospace;background:#060e1a;color:#fff;padding:16px;">
<strong style="color:#1FB954">[{APP_NAME}] {subject}</strong><br><br>
<pre style="color:rgba(255,255,255,0.7)">{message}</pre>
<small style="color:rgba(255,255,255,0.3)">{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</small>
</div>"""
            send_async_email.apply_async(
                args=[f"[{APP_NAME}] {subject}", recipients, html, "html"],
                queue="notify",
            )
        except Exception as exc:
            log.error("send_system: %s", exc)