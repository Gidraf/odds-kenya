"""
app/workers/match_lifecycle.py
================================
Unified Match Lifecycle Manager for Kinetic Odds Engine.

Architecture
─────────────────────────────────────────────────────────────────────────────
  ┌──────────────────────────────────────────────────────────────────────┐
  │  MatchLifecycleManager (singleton background thread)                 │
  │    ├─ save_match()        — persist watcher to Redis                 │
  │    ├─ _monitor_loop()     — every 30s: check start/suspend/finish    │
  │    ├─ _check_started()    — compare now() vs start_time             │
  │    ├─ _check_suspended()  — inspect live Redis channels              │
  │    └─ _transition(match, new_state) → triggers notifications         │
  ├──────────────────────────────────────────────────────────────────────┤
  │  LiveMatchBridge (background thread)                                  │
  │    • Subscribes to ALL live channels: SP, BT, OD                     │
  │    • Normalises payloads → unified schema                             │
  │    • Republishes to:                                                  │
  │        bus:live_updates:{sport}   ← SSE stream listens here          │
  │        live:match:{join_key}:all  ← per-match detail SSE             │
  │        arb:updates:{sport}        ← arb recalculation trigger        │
  │    • Calls MatchLifecycleManager.on_live_event()                     │
  ├──────────────────────────────────────────────────────────────────────┤
  │  SofaScoreBridge  (placeholder)                                       │
  │    • fetch_live_data(match_id)  → score, incidents, timeline         │
  │    • Called by MatchLifecycleManager after match starts              │
  │    • Publishes to unified live channel                               │
  ├──────────────────────────────────────────────────────────────────────┤
  │  NotificationDispatcher                                               │
  │    • email   — via SMTP / Sendgrid                                   │
  │    • SMS     — via Africa's Talking                                   │
  │    • Webhook — HTTP POST to subscriber URL                           │
  │    • WebSocket — Redis pub/sub (consumed by SSE gateway)             │
  │    • Push    — Firebase FCM (placeholder)                            │
  └──────────────────────────────────────────────────────────────────────┘

Redis key patterns
──────────────────
  kinetic:watch:{join_key}              → SavedMatch JSON (TTL 48h)
  kinetic:watch:index                   → sorted set: join_key → start_ts
  kinetic:watch:user:{user_id}          → set of join_keys
  kinetic:live:match:{join_key}         → latest live state JSON (TTL 90s)
  kinetic:notif:log:{join_key}:{event}  → deduplicate notifications (TTL 1h)

Live channels consumed
──────────────────────
  SP  :  live:match:{betradar_id}:all   (market_update)
         live:all                       (event_update)
  BT  :  bt:live:{sport_id}:updates    (batch_update)
  OD  :  od:live:{sport_id}:updates    (batch_update)

Unified channels produced
─────────────────────────
  bus:live_updates:{sport}             → SSE stream (live tab)
  live:match:{join_key}:all           → MatchDetail SSE
  arb:updates:{sport}                  → arb re-detect trigger
  kinetic:notifications:{user_id}      → per-user WebSocket feed
"""
from __future__ import annotations

import email.mime.multipart
import email.mime.text
import hashlib
import json
import logging
import os
import smtplib
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from urllib.parse import quote as _urlquote

import httpx

log = logging.getLogger(__name__)


# =============================================================================
# CONFIG  (all from env — no hardcoded secrets)
# =============================================================================

def _env(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


SMTP_HOST     = _env("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(_env("SMTP_PORT", "587"))
SMTP_USER     = _env("SMTP_USER")
SMTP_PASS     = _env("SMTP_PASS")
SMTP_FROM     = _env("SMTP_FROM", SMTP_USER)

AT_API_KEY    = _env("AT_API_KEY")           # Africa's Talking API key
AT_SENDER_ID  = _env("AT_SENDER_ID", "KINETIC")
AT_API_URL    = "https://api.africastalking.com/version1/messaging"

SOFASCORE_API = _env("SOFASCORE_API_BASE", "https://api.sofascore.com/api/v1")
SOFASCORE_KEY = _env("SOFASCORE_API_KEY")    # set when you have a key

MONITOR_INTERVAL  = int(_env("LIFECYCLE_INTERVAL_S",  "30"))
NOTIFY_PRE_MIN    = int(_env("LIFECYCLE_PRE_NOTIFY_M", "15"))  # minutes before KO
SOFASCORE_REFRESH = int(_env("SOFASCORE_REFRESH_S",   "30"))

# Redis
WATCH_KEY      = "kinetic:watch:{join_key}"
WATCH_INDEX    = "kinetic:watch:index"
WATCH_USER_KEY = "kinetic:watch:user:{user_id}"
LIVE_STATE_KEY = "kinetic:live:match:{join_key}"
NOTIF_LOG_KEY  = "kinetic:notif:log:{join_key}:{event}"

# Unified publish channels
BUS_LIVE_CHAN  = "bus:live_updates:{sport}"
BUS_MATCH_CHAN = "live:match:{join_key}:all"
BUS_ARB_CHAN   = "arb:updates:{sport}"
BUS_USER_CHAN  = "kinetic:notifications:{user_id}"

# Live source channels to consume
SP_ALL_CHAN     = "live:all"
SP_MATCH_CHAN   = "live:match:*:all"
BT_SPORT_CHAN   = "bt:live:*:updates"
OD_SPORT_CHAN   = "od:live:*:updates"


# =============================================================================
# DATA MODELS
# =============================================================================

MatchState  = Literal["pending", "pre_notify", "live", "suspended", "finished", "cancelled"]
NotiChannel = Literal["email", "sms", "webhook", "websocket", "pubsub"]


@dataclass
class WatchPrefs:
    """Per-user notification preferences for a watched match."""
    user_id:      str
    email:        str         = ""
    phone:        str         = ""           # E.164 e.g. +254712345678
    webhook_url:  str         = ""
    channels:     list[str]   = field(default_factory=lambda: ["websocket", "pubsub"])
    # Which events trigger a notification
    notify_on:    list[str]   = field(default_factory=lambda: [
        "pre_start", "started", "suspended", "resumed",
        "goal", "red_card", "finished", "arb_found",
    ])


@dataclass
class SavedMatch:
    """Persisted match watcher record."""
    join_key:      str
    match_id:      str
    home_team:     str
    away_team:     str
    sport:         str
    competition:   str
    start_time:    str          # ISO-8601 UTC
    state:         MatchState  = "pending"
    betradar_id:   str         = ""
    sofascore_id:  str         = ""          # set when resolved
    score_home:    str | None  = None
    score_away:    str | None  = None
    match_time:    str | None  = None
    is_suspended:  bool        = False
    watchers:      list[WatchPrefs] = field(default_factory=list)
    created_at:    str         = field(default_factory=lambda: _now_iso())
    updated_at:    str         = field(default_factory=lambda: _now_iso())
    # Odds snapshot at save time
    best_arb_pct:  float       = 0.0
    has_arb:       bool        = False

    def to_dict(self) -> dict:
        d = asdict(self)
        d["watchers"] = [asdict(w) for w in self.watchers]
        return d

    @staticmethod
    def from_dict(d: dict) -> "SavedMatch":
        watchers = [WatchPrefs(**w) for w in d.pop("watchers", [])]
        return SavedMatch(**d, watchers=watchers)

    @property
    def start_dt(self) -> datetime:
        s = self.start_time
        if not s.endswith("Z") and "+" not in s:
            s += "Z"
        return datetime.fromisoformat(s.replace("Z", "+00:00"))

    @property
    def minutes_to_start(self) -> float:
        return (self.start_dt - _now_utc()).total_seconds() / 60


@dataclass
class LiveEvent:
    """Normalised live event from any BK source."""
    source:       str           # "sp" | "bt" | "od"
    join_key:     str
    match_id:     str
    sport:        str
    home_team:    str
    away_team:    str
    match_time:   str | None    = None
    score_home:   str | None    = None
    score_away:   str | None    = None
    is_suspended: bool          = False
    markets:      dict          = field(default_factory=dict)   # {slug: {outcome: odd}}
    event_type:   str           = "market_update"               # market_update | score_update | status_update
    ts:           float         = field(default_factory=time.time)


@dataclass
class Notification:
    """Notification to be dispatched."""
    match:      SavedMatch
    watcher:    WatchPrefs
    event_type: str           # e.g. "started", "goal", "arb_found"
    title:      str
    body:       str
    data:       dict          = field(default_factory=dict)
    ts:         float         = field(default_factory=time.time)


# =============================================================================
# UTILITIES
# =============================================================================

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _now_iso() -> str:
    return _now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")

def _hash(obj: Any) -> str:
    return hashlib.md5(json.dumps(obj, sort_keys=True).encode()).hexdigest()[:12]

def _build_redis_url() -> str:
    full = os.getenv("REDIS_URL", "").strip()
    if full: return full
    host = os.getenv("REDIS_HOST", "localhost")
    port = os.getenv("REDIS_PORT", "6379")
    auth = os.getenv("REDIS_AUTH", os.getenv("REDIS_PASSWORD", ""))
    db   = os.getenv("REDIS_DB", "0")
    if auth: return f"redis://:{_urlquote(auth, safe='')}@{host}:{port}/{db}"
    return f"redis://{host}:{port}/{db}"

_redis_client: Any = None
def _redis() -> Any:
    global _redis_client
    if _redis_client is None:
        import redis
        _redis_client = redis.from_url(
            _build_redis_url(), decode_responses=True,
            socket_connect_timeout=3, socket_timeout=5,
        )
    return _redis_client


# =============================================================================
# NOTIFICATION DISPATCHER
# =============================================================================

class NotificationDispatcher:
    """
    Sends notifications via email, SMS, webhook, WebSocket (pubsub), push.
    Each channel is independent — failure in one does not block others.
    """

    def dispatch(self, notif: Notification) -> None:
        """Dispatch a notification to all requested channels for this watcher."""
        w  = notif.watcher
        r  = _redis()

        # Deduplicate — don't fire same event twice within 1 hour
        dedup_key = NOTIF_LOG_KEY.format(join_key=notif.match.join_key, event=notif.event_type)
        dedup_field = f"{w.user_id}:{notif.event_type}"
        if r.hget(dedup_key, dedup_field):
            return
        r.hset(dedup_key, dedup_field, _now_iso())
        r.expire(dedup_key, 3600)

        for channel in w.channels:
            try:
                if   channel == "email"     and w.email:       self._send_email(notif)
                elif channel == "sms"       and w.phone:       self._send_sms(notif)
                elif channel == "webhook"   and w.webhook_url: self._send_webhook(notif)
                elif channel in ("websocket", "pubsub"):       self._send_pubsub(notif)
            except Exception as exc:
                log.warning("Notification channel %s failed: %s", channel, exc)

    # ── Email ─────────────────────────────────────────────────────────────────

    def _send_email(self, notif: Notification) -> None:
        if not SMTP_USER or not SMTP_PASS:
            log.debug("Email not configured (SMTP_USER/SMTP_PASS missing)")
            return

        msg = email.mime.multipart.MIMEMultipart("alternative")
        msg["Subject"] = f"[Kinetic] {notif.title}"
        msg["From"]    = SMTP_FROM
        msg["To"]      = notif.watcher.email

        m   = notif.match
        txt = (
            f"{notif.title}\n\n"
            f"{m.home_team} vs {m.away_team}\n"
            f"{m.competition} · {m.sport.upper()}\n"
            f"Start: {m.start_time}\n\n"
            f"{notif.body}\n\n"
            f"-- Kinetic Odds Engine"
        )
        html = f"""
        <html><body style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:16px">
          <div style="background:#070C18;border-radius:10px;padding:20px;color:#E8EDF8">
            <div style="color:#CCFF00;font-weight:900;font-size:18px;margin-bottom:8px">⚡ Kinetic</div>
            <h2 style="margin:0 0 4px;font-size:16px">{notif.title}</h2>
            <div style="color:#9AA3B0;font-size:13px;margin-bottom:12px">
              {m.home_team} vs {m.away_team}<br>
              {m.competition} · {m.start_time[:10]}
            </div>
            <div style="background:#0A1020;border-radius:8px;padding:14px;font-size:14px">
              {notif.body}
            </div>
            {"<div style='margin-top:10px;color:#F5A523;font-weight:700'>⚡ ARB: +" + str(m.best_arb_pct) + "%</div>" if notif.event_type == "arb_found" else ""}
          </div>
        </body></html>"""

        msg.attach(email.mime.text.MIMEText(txt,  "plain"))
        msg.attach(email.mime.text.MIMEText(html, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_FROM, notif.watcher.email, msg.as_string())

        log.info("Email sent to %s: %s", notif.watcher.email, notif.title)

    # ── SMS via Africa's Talking ───────────────────────────────────────────────

    def _send_sms(self, notif: Notification) -> None:
        if not AT_API_KEY:
            log.debug("SMS not configured (AT_API_KEY missing)")
            return

        m   = notif.match
        body = (
            f"[Kinetic] {notif.title}\n"
            f"{m.home_team} v {m.away_team} · {notif.body[:80]}"
        )

        resp = httpx.post(
            AT_API_URL,
            data={
                "username":    "kinetic",
                "to":          notif.watcher.phone,
                "message":     body,
                "from":        AT_SENDER_ID,
            },
            headers={
                "apiKey":      AT_API_KEY,
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept":      "application/json",
            },
            timeout=8.0,
        )
        if resp.is_success:
            log.info("SMS sent to %s: %s", notif.watcher.phone, notif.title)
        else:
            log.warning("SMS failed %s: %s", resp.status_code, resp.text[:200])

    # ── Webhook ───────────────────────────────────────────────────────────────

    def _send_webhook(self, notif: Notification) -> None:
        payload = {
            "event":    notif.event_type,
            "title":    notif.title,
            "body":     notif.body,
            "match":    {
                "join_key":   notif.match.join_key,
                "home_team":  notif.match.home_team,
                "away_team":  notif.match.away_team,
                "sport":      notif.match.sport,
                "state":      notif.match.state,
                "score_home": notif.match.score_home,
                "score_away": notif.match.score_away,
                "match_time": notif.match.match_time,
                "has_arb":    notif.match.has_arb,
                "best_arb_pct": notif.match.best_arb_pct,
            },
            "data": notif.data,
            "ts":   notif.ts,
        }
        resp = httpx.post(
            notif.watcher.webhook_url,
            json=payload,
            timeout=8.0,
            headers={"X-Kinetic-Event": notif.event_type},
        )
        if resp.is_success:
            log.info("Webhook delivered: %s", notif.watcher.webhook_url)
        else:
            log.warning("Webhook failed %s: %d", notif.watcher.webhook_url, resp.status_code)

    # ── WebSocket / pub-sub (Redis → SSE gateway) ─────────────────────────────

    def _send_pubsub(self, notif: Notification) -> None:
        r = _redis()
        payload = json.dumps({
            "type":       "notification",
            "event":      notif.event_type,
            "title":      notif.title,
            "body":       notif.body,
            "match": {
                "join_key":     notif.match.join_key,
                "home_team":    notif.match.home_team,
                "away_team":    notif.match.away_team,
                "sport":        notif.match.sport,
                "state":        notif.match.state,
                "score_home":   notif.match.score_home,
                "score_away":   notif.match.score_away,
                "match_time":   notif.match.match_time,
                "is_suspended": notif.match.is_suspended,
                "has_arb":      notif.match.has_arb,
                "best_arb_pct": notif.match.best_arb_pct,
            },
            "data": notif.data,
            "ts":   notif.ts,
        })
        # Per-user channel (SSE /api/notifications/stream picks this up)
        r.publish(BUS_USER_CHAN.format(user_id=notif.watcher.user_id), payload)
        # Also push to match-level channel so MatchDetail SSE gets it
        r.publish(BUS_MATCH_CHAN.format(join_key=notif.match.join_key), payload)
        log.debug("Pubsub notification → user=%s event=%s", notif.watcher.user_id, notif.event_type)


# =============================================================================
# SOFASCORE BRIDGE  (placeholder — wire in your key when ready)
# =============================================================================

class SofaScoreBridge:
    """
    Fetches live match data from SofaScore (or any external API).

    Replace the _fetch_* methods with your actual API calls.
    The public interface (update_match, get_live_state) stays the same.

    SofaScore free endpoints (no key needed, rate-limited):
      GET https://api.sofascore.com/api/v1/event/{event_id}/incidents
      GET https://api.sofascore.com/api/v1/event/{event_id}/statistics
    """

    _headers = {
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 10; K) "
            "AppleWebKit/537.36 Chrome/124.0.0.0 Mobile Safari/537.36"
        ),
        "Accept": "application/json",
        "Cache-Control": "no-cache",
    }

    def resolve_event_id(self, home: str, away: str, sport: str, date: str) -> str | None:
        """
        Search SofaScore for the event ID given team names + date.
        Returns the SofaScore event ID string, or None if not found.

        TODO: implement actual search when you have API access.
        """
        # Placeholder — return None until wired
        log.debug("SofaScore resolve: %s vs %s on %s", home, away, date)
        return None

    def fetch_live_state(self, sofascore_id: str) -> dict | None:
        """
        Fetch current score + match time from SofaScore.
        Returns unified dict or None.
        """
        if not sofascore_id:
            return None
        try:
            resp = httpx.get(
                f"{SOFASCORE_API}/event/{sofascore_id}",
                headers=self._headers, timeout=6.0,
            )
            if not resp.is_success:
                return None
            raw = resp.json().get("event") or {}
            scores = raw.get("homeScore") or {}
            return {
                "score_home":  str(scores.get("current", "")),
                "score_away":  str((raw.get("awayScore") or {}).get("current", "")),
                "match_time":  str(raw.get("time", {}).get("played", "") or ""),
                "status":      raw.get("status", {}).get("description", ""),
                "is_live":     raw.get("status", {}).get("type") == "inprogress",
                "is_finished": raw.get("status", {}).get("type") == "finished",
            }
        except Exception as exc:
            log.debug("SofaScore fetch error: %s", exc)
            return None

    def fetch_incidents(self, sofascore_id: str) -> list[dict]:
        """
        Fetch match incidents (goals, cards, substitutions).
        Returns list of incident dicts.
        """
        if not sofascore_id:
            return []
        try:
            resp = httpx.get(
                f"{SOFASCORE_API}/event/{sofascore_id}/incidents",
                headers=self._headers, timeout=6.0,
            )
            if not resp.is_success:
                return []
            return resp.json().get("incidents") or []
        except Exception:
            return []

    def publish_live_state(self, match: "SavedMatch", state: dict) -> None:
        """Push SofaScore data to unified Redis channels."""
        if not state:
            return
        r = _redis()
        payload = json.dumps({
            "source":       "sofascore",
            "join_key":     match.join_key,
            "match_id":     match.match_id,
            "home_team":    match.home_team,
            "away_team":    match.away_team,
            "score_home":   state.get("score_home"),
            "score_away":   state.get("score_away"),
            "match_time":   state.get("match_time"),
            "is_live":      state.get("is_live"),
            "is_finished":  state.get("is_finished"),
            "ts":           time.time(),
        })
        r.set(LIVE_STATE_KEY.format(join_key=match.join_key), payload, ex=90)
        r.publish(BUS_LIVE_CHAN.format(sport=match.sport), payload)
        r.publish(BUS_MATCH_CHAN.format(join_key=match.join_key), payload)


_sofascore = SofaScoreBridge()


# =============================================================================
# LIVE MATCH BRIDGE — unifies SP + BT + OD live channels
# =============================================================================

class LiveMatchBridge:
    """
    Subscribes to all live Redis pub/sub channels from SP, BT, OD harvesters.
    Normalises every payload → LiveEvent → republishes to unified channels.

    SP  publishes: live:all  (event_update + market_update)
                   live:match:{betradar_id}:all
    BT  publishes: bt:live:{sport_id}:updates  (batch_update)
    OD  publishes: od:live:{sport_id}:updates  (batch_update)

    This bridge republishes to:
      bus:live_updates:{sport}   → SSE live tab
      live:match:{jk}:all        → MatchDetail SSE
      arb:updates:{sport}        → arb re-detection
    """

    # SP sport IDs → slug
    _SP_SLUG = {
        1:"soccer",2:"basketball",4:"tennis",5:"handball",
        8:"rugby",9:"cricket",10:"volleyball",13:"table-tennis",
    }
    # BT sport IDs → slug
    _BT_SLUG = {
        1:"soccer",2:"basketball",3:"tennis",4:"ice-hockey",
        5:"volleyball",6:"cricket",7:"rugby",8:"handball",
        9:"table-tennis",10:"baseball",11:"american-football",
        15:"mma",16:"boxing",17:"darts",
    }
    # OD sport IDs → slug
    _OD_SLUG = {
        1:"soccer",2:"basketball",3:"tennis",4:"cricket",
        5:"rugby",6:"ice-hockey",7:"volleyball",8:"handball",
        9:"table-tennis",10:"baseball",11:"american-football",
        15:"mma",16:"boxing",17:"darts",1001:"esoccer",
    }

    def __init__(self, lifecycle: "MatchLifecycleManager") -> None:
        self._lc       = lifecycle
        self._running  = False
        self._thread:  threading.Thread | None = None

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(
            target=self._listen, daemon=True, name="live-bridge"
        )
        self._thread.start()
        log.info("LiveMatchBridge started")

    def stop(self) -> None:
        self._running = False

    # ── Internal ──────────────────────────────────────────────────────────────

    def _listen(self) -> None:
        while self._running:
            try:
                r      = _redis()
                pubsub = r.pubsub(ignore_subscribe_messages=True)
                # Subscribe to pattern channels for BT + OD
                pubsub.psubscribe("bt:live:*:updates", "od:live:*:updates")
                # Subscribe to SP specific channels
                pubsub.subscribe("live:all")

                log.info("LiveMatchBridge: subscribed to SP/BT/OD channels")
                while self._running:
                    msg = pubsub.get_message(timeout=2.0)
                    if not msg:
                        continue
                    try:
                        channel = (msg.get("channel") or b"").decode() if isinstance(msg.get("channel"), bytes) else (msg.get("channel") or "")
                        pattern = (msg.get("pattern") or b"").decode() if isinstance(msg.get("pattern"), bytes) else (msg.get("pattern") or "")
                        raw_data = msg.get("data") or ""
                        if not raw_data or raw_data == 1:
                            continue
                        payload = json.loads(raw_data)

                        if channel == "live:all":
                            self._handle_sp(channel, payload)
                        elif "bt:live:" in channel:
                            self._handle_bt(channel, payload)
                        elif "od:live:" in channel:
                            self._handle_od(channel, payload)

                    except Exception as exc:
                        log.debug("Bridge message error: %s", exc)
            except Exception as exc:
                log.error("LiveMatchBridge reconnect: %s", exc)
                time.sleep(3)

    def _publish_unified(self, ev: LiveEvent) -> None:
        """Push normalised LiveEvent to all unified channels."""
        r = _redis()
        payload = json.dumps({
            "source":       ev.source,
            "join_key":     ev.join_key,
            "match_id":     ev.match_id,
            "home_team":    ev.home_team,
            "away_team":    ev.away_team,
            "sport":        ev.sport,
            "match_time":   ev.match_time,
            "score_home":   ev.score_home,
            "score_away":   ev.score_away,
            "is_suspended": ev.is_suspended,
            "markets":      ev.markets,
            "event_type":   ev.event_type,
            "ts":           ev.ts,
        })
        pipe = r.pipeline()
        # SSE live tab
        pipe.publish(BUS_LIVE_CHAN.format(sport=ev.sport), payload)
        # Per-match SSE (MatchDetail)
        if ev.join_key:
            pipe.publish(BUS_MATCH_CHAN.format(join_key=ev.join_key), payload)
        # Arb re-detection trigger (only for market updates)
        if ev.event_type == "market_update" and ev.markets:
            pipe.publish(BUS_ARB_CHAN.format(sport=ev.sport), payload)
        # Cache latest live state
        pipe.set(LIVE_STATE_KEY.format(join_key=ev.join_key), payload, ex=90)
        pipe.execute()

        # Notify lifecycle manager
        self._lc.on_live_event(ev)

    # ── SP normaliser ──────────────────────────────────────────────────────────

    def _handle_sp(self, channel: str, data: dict) -> None:
        """SP publishes both market_update and event_update payloads."""
        parent_match_id = str(data.get("parent_match_id") or data.get("betradar_id") or "")
        sport_id        = int(data.get("sportId") or 1)
        sport           = self._SP_SLUG.get(sport_id, "soccer")

        # Market update
        bk_data = data.get("bookmakers") or {}
        mkts: dict = {}
        for _bk, bd in bk_data.items():
            for slug, outcomes in (bd.get("markets") or {}).items():
                mkts[slug] = {k: float(v.get("price", v) if isinstance(v, dict) else v) for k, v in outcomes.items()}

        # Score / time update
        score_home = str(data.get("score_home") or "") or None
        score_away = str(data.get("score_away") or "") or None
        match_time = str(data.get("match_time") or "") or None

        ev = LiveEvent(
            source       = "sp",
            join_key     = parent_match_id,
            match_id     = parent_match_id,
            sport        = sport,
            home_team    = data.get("home_team") or "",
            away_team    = data.get("away_team") or "",
            match_time   = match_time,
            score_home   = score_home,
            score_away   = score_away,
            is_suspended = False,
            markets      = mkts,
            event_type   = "market_update" if mkts else "score_update",
        )
        if ev.join_key:
            self._publish_unified(ev)

    # ── BT normaliser ──────────────────────────────────────────────────────────

    def _handle_bt(self, channel: str, data: dict) -> None:
        """
        BT: batch_update payload has `events` list, each event has:
          match_id, home_team, away_team, market_slug, outcome_key, odd,
          match_time, score_home, score_away
        """
        sport_id   = int(channel.split(":")[2]) if ":" in channel else 1
        sport      = self._BT_SLUG.get(sport_id, "soccer")
        events     = data.get("events") or []

        # Group by match_id so we emit one LiveEvent per match
        by_mid: dict[str, dict] = {}
        for ev_raw in events:
            mid = str(ev_raw.get("match_id") or "")
            if not mid:
                continue
            entry = by_mid.setdefault(mid, {
                "home_team":    ev_raw.get("home_team", ""),
                "away_team":    ev_raw.get("away_team", ""),
                "match_time":   ev_raw.get("match_time"),
                "score_home":   str(ev_raw.get("score_home") or ""),
                "score_away":   str(ev_raw.get("score_away") or ""),
                "is_suspended": False,
                "markets":      {},
            })
            slug     = ev_raw.get("market_slug") or ""
            outcome  = ev_raw.get("outcome_key") or ""
            odd      = float(ev_raw.get("odd") or 0)
            if slug and outcome and odd > 1.0:
                entry["markets"].setdefault(slug, {})[outcome] = odd

        for mid, entry in by_mid.items():
            ev = LiveEvent(
                source       = "bt",
                join_key     = mid,    # BT uses betradar-compatible IDs
                match_id     = mid,
                sport        = sport,
                home_team    = entry["home_team"],
                away_team    = entry["away_team"],
                match_time   = entry.get("match_time"),
                score_home   = entry.get("score_home") or None,
                score_away   = entry.get("score_away") or None,
                is_suspended = entry.get("is_suspended", False),
                markets      = entry["markets"],
                event_type   = "market_update" if entry["markets"] else "score_update",
            )
            self._publish_unified(ev)

    # ── OD normaliser ──────────────────────────────────────────────────────────

    def _handle_od(self, channel: str, data: dict) -> None:
        """
        OD: batch_update payload has `events` list, each event has:
          match_id, home_team, away_team, market_slug, outcome_key, odd,
          match_time, score_home, score_away
        """
        sport_id = int(channel.split(":")[2]) if ":" in channel else 1
        sport    = self._OD_SLUG.get(sport_id, "soccer")
        events   = data.get("events") or []

        by_mid: dict[str, dict] = {}
        for ev_raw in events:
            mid = str(ev_raw.get("match_id") or "")
            if not mid:
                continue
            entry = by_mid.setdefault(mid, {
                "home_team":    ev_raw.get("home_team", ""),
                "away_team":    ev_raw.get("away_team", ""),
                "match_time":   ev_raw.get("match_time"),
                "score_home":   str(ev_raw.get("score_home") or ""),
                "score_away":   str(ev_raw.get("score_away") or ""),
                "is_suspended": False,
                "markets":      {},
            })
            slug    = ev_raw.get("market_slug") or ""
            outcome = ev_raw.get("outcome_key") or ""
            odd     = float(ev_raw.get("odd") or 0)
            if slug and outcome and odd > 1.0:
                entry["markets"].setdefault(slug, {})[outcome] = odd

        for mid, entry in by_mid.items():
            ev = LiveEvent(
                source       = "od",
                join_key     = mid,
                match_id     = mid,
                sport        = sport,
                home_team    = entry["home_team"],
                away_team    = entry["away_team"],
                match_time   = entry.get("match_time"),
                score_home   = entry.get("score_home") or None,
                score_away   = entry.get("score_away") or None,
                is_suspended = entry.get("is_suspended", False),
                markets      = entry["markets"],
                event_type   = "market_update" if entry["markets"] else "score_update",
            )
            self._publish_unified(ev)


# =============================================================================
# MATCH LIFECYCLE MANAGER
# =============================================================================

class MatchLifecycleManager:
    """
    Singleton that:
      1. Saves match watchers to Redis
      2. Monitors match state transitions every MONITOR_INTERVAL seconds
      3. Fires notifications on state changes
      4. Starts SofaScore polling once a match goes live
      5. Exposes on_live_event() hook called by LiveMatchBridge
    """

    def __init__(self) -> None:
        self._dispatcher = NotificationDispatcher()
        self._bridge     = LiveMatchBridge(self)
        self._sofascore  = _sofascore
        self._running    = False
        self._thread:    threading.Thread | None = None
        self._ss_threads: dict[str, threading.Thread] = {}  # join_key → sofascore poller thread
        self._lock       = threading.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    def save_match(
        self,
        match_data:  dict,
        user_prefs:  WatchPrefs | None = None,
    ) -> SavedMatch:
        """
        Persist a match watcher.

        match_data keys (same shape as Match from odds_stream):
          join_key, match_id, home_team, away_team, sport,
          competition, start_time, betradar_id, has_arb, best_arb_pct
        """
        r   = _redis()
        jk  = str(match_data.get("join_key") or match_data.get("match_id") or "")
        key = WATCH_KEY.format(join_key=jk)

        # Reuse existing watcher if present
        existing_raw = r.get(key)
        if existing_raw:
            saved = SavedMatch.from_dict(json.loads(existing_raw))
        else:
            saved = SavedMatch(
                join_key    = jk,
                match_id    = str(match_data.get("match_id") or jk),
                home_team   = match_data.get("home_team") or "",
                away_team   = match_data.get("away_team") or "",
                sport       = match_data.get("sport") or "soccer",
                competition = match_data.get("competition") or "",
                start_time  = match_data.get("start_time") or _now_iso(),
                betradar_id = str(match_data.get("betradar_id") or ""),
                has_arb     = bool(match_data.get("has_arb")),
                best_arb_pct = float(match_data.get("best_arb_pct") or 0),
            )

        if user_prefs and not any(w.user_id == user_prefs.user_id for w in saved.watchers):
            saved.watchers.append(user_prefs)

        saved.updated_at = _now_iso()
        self._persist(saved)
        log.info("Match saved: %s vs %s (join_key=%s)", saved.home_team, saved.away_team, jk)
        return saved

    def remove_watcher(self, join_key: str, user_id: str) -> None:
        r   = _redis()
        key = WATCH_KEY.format(join_key=join_key)
        raw = r.get(key)
        if not raw:
            return
        saved = SavedMatch.from_dict(json.loads(raw))
        saved.watchers = [w for w in saved.watchers if w.user_id != user_id]
        if saved.watchers:
            self._persist(saved)
        else:
            r.delete(key)
            r.zrem(WATCH_INDEX, join_key)

    def get_watch(self, join_key: str) -> SavedMatch | None:
        raw = _redis().get(WATCH_KEY.format(join_key=join_key))
        return SavedMatch.from_dict(json.loads(raw)) if raw else None

    def list_watches(self, user_id: str | None = None) -> list[SavedMatch]:
        r = _redis()
        if user_id:
            jks = r.smembers(WATCH_USER_KEY.format(user_id=user_id))
        else:
            jks = [jk for jk, _ in r.zscan_iter(WATCH_INDEX)]
        result = []
        for jk in jks:
            raw = r.get(WATCH_KEY.format(join_key=jk))
            if raw:
                try:
                    result.append(SavedMatch.from_dict(json.loads(raw)))
                except Exception:
                    pass
        return result

    # ── Called by LiveMatchBridge ─────────────────────────────────────────────

    def on_live_event(self, ev: LiveEvent) -> None:
        """Called on every normalised live event from any BK source."""
        saved = self.get_watch(ev.join_key)
        if not saved:
            return

        changed = False

        # Score change
        if ev.score_home is not None and ev.score_home != saved.score_home:
            old_home = saved.score_home; old_away = saved.score_away
            saved.score_home = ev.score_home
            saved.score_away = ev.score_away
            changed = True
            # Check if a goal was scored
            if self._goal_scored(old_home, old_away, ev.score_home, ev.score_away, saved.sport):
                self._notify_all(saved, "goal", "⚽ Goal!", f"Score: {ev.score_home}–{ev.score_away}", data={"score_home": ev.score_home, "score_away": ev.score_away})

        # Match time
        if ev.match_time and ev.match_time != saved.match_time:
            saved.match_time = ev.match_time
            changed = True

        # Suspension detection
        if ev.is_suspended and not saved.is_suspended:
            saved.is_suspended = True
            changed = True
            self._transition(saved, "suspended")
        elif not ev.is_suspended and saved.is_suspended and saved.state == "suspended":
            saved.is_suspended = False
            changed = True
            self._transition(saved, "live")

        # Match started transition
        if saved.state == "pending" and ev.match_time:
            self._transition(saved, "live")
            changed = True

        if changed:
            self._persist(saved)

    # ── Background monitor ────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(
            target=self._monitor_loop, daemon=True, name="lifecycle-monitor"
        )
        self._thread.start()
        self._bridge.start()
        log.info("MatchLifecycleManager started")

    def stop(self) -> None:
        self._running = False
        self._bridge.stop()

    def _monitor_loop(self) -> None:
        """Runs every MONITOR_INTERVAL seconds to check all watched matches."""
        while self._running:
            tick = time.time()
            try:
                self._run_checks()
            except Exception as exc:
                log.error("Lifecycle monitor error: %s", exc)
            elapsed = time.time() - tick
            time.sleep(max(1.0, MONITOR_INTERVAL - elapsed))

    def _run_checks(self) -> None:
        r   = _redis()
        now = _now_utc()

        # Iterate all active watches (newest start time first)
        watches = r.zrangebyscore(WATCH_INDEX, now.timestamp() - 7200, "+inf")

        for jk in watches:
            try:
                raw = r.get(WATCH_KEY.format(join_key=jk))
                if not raw:
                    continue
                saved = SavedMatch.from_dict(json.loads(raw))
                self._check_match(saved)
            except Exception as exc:
                log.debug("Lifecycle check error jk=%s: %s", jk, exc)

    def _check_match(self, saved: SavedMatch) -> None:
        """Run all state checks for one match."""
        mins_to_start = saved.minutes_to_start

        if saved.state == "pending":
            # Pre-start notification
            if 0 < mins_to_start <= NOTIFY_PRE_MIN:
                self._transition(saved, "pre_notify")

            # Kick-off passed — mark live and start SofaScore polling
            if mins_to_start < -2:
                self._transition(saved, "live")

        elif saved.state in ("pre_notify", "live"):
            # Start SofaScore poller if not already running
            self._ensure_sofascore_poller(saved)

            # Check for finish (match time > 90 min or start + 3 hours ago)
            if mins_to_start < -180:
                self._transition(saved, "finished")

    def _ensure_sofascore_poller(self, saved: SavedMatch) -> None:
        jk = saved.join_key
        with self._lock:
            if jk in self._ss_threads and self._ss_threads[jk].is_alive():
                return
            # Resolve SofaScore ID if missing
            if not saved.sofascore_id:
                date_str = saved.start_time[:10]
                ss_id    = self._sofascore.resolve_event_id(
                    saved.home_team, saved.away_team, saved.sport, date_str
                )
                if ss_id:
                    saved.sofascore_id = ss_id
                    self._persist(saved)

            if not saved.sofascore_id:
                return  # SofaScore not wired yet

            t = threading.Thread(
                target=self._sofascore_poll_loop,
                args=(saved.join_key,),
                daemon=True,
                name=f"ss-{jk[:8]}",
            )
            self._ss_threads[jk] = t
            t.start()
            log.info("SofaScore poller started for %s", jk)

    def _sofascore_poll_loop(self, join_key: str) -> None:
        """Poll SofaScore every SOFASCORE_REFRESH seconds until match is finished."""
        while self._running:
            saved = self.get_watch(join_key)
            if not saved or saved.state == "finished":
                break
            if saved.sofascore_id:
                state = self._sofascore.fetch_live_state(saved.sofascore_id)
                if state:
                    self._sofascore.publish_live_state(saved, state)
                    if state.get("is_finished"):
                        self._transition(saved, "finished")
                        break
            time.sleep(SOFASCORE_REFRESH)

    # ── State transitions ──────────────────────────────────────────────────────

    def _transition(self, saved: SavedMatch, new_state: MatchState) -> None:
        if saved.state == new_state:
            return
        old_state = saved.state
        saved.state = new_state
        saved.updated_at = _now_iso()
        self._persist(saved)

        log.info(
            "Match state %s→%s: %s vs %s",
            old_state, new_state, saved.home_team, saved.away_team,
        )

        # Publish state change to all SSE channels
        r = _redis()
        payload = json.dumps({
            "type":      "state_change",
            "join_key":  saved.join_key,
            "old_state": old_state,
            "new_state": new_state,
            "home_team": saved.home_team,
            "away_team": saved.away_team,
            "sport":     saved.sport,
            "ts":        time.time(),
        })
        r.publish(BUS_LIVE_CHAN.format(sport=saved.sport), payload)
        r.publish(BUS_MATCH_CHAN.format(join_key=saved.join_key), payload)

        # Fire notifications
        notif_map: dict[str, tuple[str, str]] = {
            "pre_notify": ("⏰ Match Starting Soon", f"Kicks off in ~{NOTIFY_PRE_MIN} min"),
            "live":       ("🔴 Match is LIVE", f"{saved.home_team} vs {saved.away_team} has started"),
            "suspended":  ("⏸ Market Suspended", "Betting markets have been suspended"),
            "live_after_suspend": ("▶ Markets Resumed", "Betting markets are open again"),
            "finished":   ("✅ Match Finished", f"Final score: {saved.score_home or '?'}–{saved.score_away or '?'}"),
        }
        key = "live_after_suspend" if new_state == "live" and old_state == "suspended" else new_state
        if key in notif_map:
            title, body = notif_map[key]
            event_type  = key
            self._notify_all(saved, event_type, title, body)

    def _notify_all(
        self,
        saved: SavedMatch,
        event_type: str,
        title: str,
        body: str,
        data: dict | None = None,
    ) -> None:
        for w in saved.watchers:
            if event_type not in w.notify_on:
                continue
            notif = Notification(
                match=saved, watcher=w,
                event_type=event_type,
                title=title, body=body,
                data=data or {},
            )
            try:
                self._dispatcher.dispatch(notif)
            except Exception as exc:
                log.warning("Dispatch error for user %s: %s", w.user_id, exc)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _persist(self, saved: SavedMatch) -> None:
        r   = _redis()
        key = WATCH_KEY.format(join_key=saved.join_key)
        r.set(key, json.dumps(saved.to_dict()), ex=172800)  # 48h TTL
        # Add to sorted index by start time
        try:
            ts = saved.start_dt.timestamp()
        except Exception:
            ts = time.time()
        r.zadd(WATCH_INDEX, {saved.join_key: ts})
        # Per-user index
        for w in saved.watchers:
            r.sadd(WATCH_USER_KEY.format(user_id=w.user_id), saved.join_key)

    @staticmethod
    def _goal_scored(
        old_home: str | None, old_away: str | None,
        new_home: str | None, new_away: str | None,
        sport: str,
    ) -> bool:
        """Returns True if any team scored (soccer/basketball/etc.)."""
        if sport not in ("soccer", "esoccer", "futsal"):
            return False
        try:
            oh = int(old_home or 0); oa = int(old_away or 0)
            nh = int(new_home or 0); na = int(new_away or 0)
            return nh > oh or na > oa
        except (TypeError, ValueError):
            return False


# =============================================================================
# SINGLETON + FLASK BLUEPRINT (notification stream)
# =============================================================================

_manager: MatchLifecycleManager | None = None


def get_lifecycle_manager() -> MatchLifecycleManager:
    global _manager
    if _manager is None:
        _manager = MatchLifecycleManager()
    return _manager


def start_lifecycle_manager() -> MatchLifecycleManager:
    """Call once at app startup (e.g. in create_app or celery worker init)."""
    mgr = get_lifecycle_manager()
    mgr.start()
    return mgr


# Flask blueprint — notification SSE + watch management API
try:
    from flask import Blueprint, Response, g, jsonify, request, stream_with_context

    bp_lifecycle = Blueprint("lifecycle", __name__, url_prefix="/api/matches")

    def _auth_user_lc():
        """Minimal auth — reuse odds_stream pattern."""
        try:
            from app.utils.customer_jwt_helpers import _decode_token
            from app.models.customer import Customer
            auth  = request.headers.get("Authorization", "")
            token = auth[7:] if auth.startswith("Bearer ") else request.args.get("token", "")
            if token:
                payload = _decode_token(token)
                return Customer.query.get(int(payload["sub"]))
        except Exception:
            pass
        return None

    @bp_lifecycle.route("/watch", methods=["POST"])
    def watch_match():
        """Save a match watcher. Body: {match: {...}, prefs: {...}}"""
        user = _auth_user_lc()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401

        body = request.get_json(silent=True) or {}
        match_data = body.get("match") or {}
        prefs_data = body.get("prefs") or {}

        prefs = WatchPrefs(
            user_id     = str(user.id),
            email       = prefs_data.get("email") or getattr(user, "email", ""),
            phone       = prefs_data.get("phone") or getattr(user, "phone", ""),
            webhook_url = prefs_data.get("webhook_url", ""),
            channels    = prefs_data.get("channels") or ["websocket", "pubsub"],
            notify_on   = prefs_data.get("notify_on") or [
                "pre_start", "started", "suspended", "resumed",
                "goal", "finished", "arb_found",
            ],
        )

        mgr   = get_lifecycle_manager()
        saved = mgr.save_match(match_data, prefs)
        return jsonify({"ok": True, "watch": saved.to_dict()}), 201

    @bp_lifecycle.route("/watch/<join_key>", methods=["DELETE"])
    def unwatch_match(join_key: str):
        user = _auth_user_lc()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        get_lifecycle_manager().remove_watcher(join_key, str(user.id))
        return jsonify({"ok": True})

    @bp_lifecycle.route("/watch", methods=["GET"])
    def list_watches():
        user = _auth_user_lc()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        watches = get_lifecycle_manager().list_watches(user_id=str(user.id))
        return jsonify({"watches": [w.to_dict() for w in watches]})

    @bp_lifecycle.route("/watch/<join_key>", methods=["GET"])
    def get_watch(join_key: str):
        saved = get_lifecycle_manager().get_watch(join_key)
        if not saved:
            return jsonify({"error": "Not found"}), 404
        return jsonify({"watch": saved.to_dict()})

    @bp_lifecycle.route("/notifications/stream", methods=["GET"])
    def notification_stream():
        """
        Per-user SSE notification stream.
        Frontend: const es = new EventSource('/api/matches/notifications/stream?token=...')
        """
        user = _auth_user_lc()
        if not user:
            def _deny():
                yield f"event: error\ndata: {json.dumps({'error': 'Unauthorized'})}\n\n"
            return Response(stream_with_context(_deny()), mimetype="text/event-stream", status=200)

        user_id = str(user.id)

        def generate():
            r      = _redis()
            pubsub = r.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(BUS_USER_CHAN.format(user_id=user_id))
            last_ka = time.time()
            yield f"event: connected\ndata: {json.dumps({'user_id': user_id})}\n\n"
            try:
                while True:
                    msg = pubsub.get_message(timeout=1.0)
                    if msg and msg.get("type") == "message":
                        try:
                            data = json.loads(msg["data"])
                            yield f"event: notification\ndata: {json.dumps(data)}\n\n"
                        except Exception:
                            pass
                    if time.time() - last_ka > 20:
                        yield ": keepalive\n\n"
                        last_ka = time.time()
            finally:
                pubsub.unsubscribe()
                pubsub.close()

        return Response(
            stream_with_context(generate()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control":    "no-cache",
                "X-Accel-Buffering": "no",
                "Connection":       "keep-alive",
                "Access-Control-Allow-Origin":  "*",
                "Access-Control-Allow-Headers": "Authorization,Content-Type",
            },
        )

    @bp_lifecycle.route("/live/<join_key>", methods=["GET"])
    def live_state(join_key: str):
        """Latest cached live state for a specific match."""
        raw = _redis().get(LIVE_STATE_KEY.format(join_key=join_key))
        if not raw:
            return jsonify({"error": "No live data yet"}), 404
        return jsonify(json.loads(raw))

    @bp_lifecycle.route("/sofascore/link", methods=["POST"])
    def link_sofascore():
        """
        Manually link a SofaScore ID to a watched match.
        Body: {join_key: "...", sofascore_id: "12345678"}
        """
        user = _auth_user_lc()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        body = request.get_json(silent=True) or {}
        jk   = body.get("join_key")
        ss_id = body.get("sofascore_id")
        if not jk or not ss_id:
            return jsonify({"error": "join_key and sofascore_id required"}), 400
        mgr   = get_lifecycle_manager()
        saved = mgr.get_watch(jk)
        if not saved:
            return jsonify({"error": "Watch not found"}), 404
        saved.sofascore_id = str(ss_id)
        mgr._persist(saved)
        return jsonify({"ok": True, "sofascore_id": ss_id})

    @bp_lifecycle.route("/sofascore/resolve", methods=["POST"])
    def resolve_sofascore():
        """
        Try to auto-resolve SofaScore ID.
        Body: {join_key: "..."} or {home_team, away_team, sport, date}
        """
        user = _auth_user_lc()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        body = request.get_json(silent=True) or {}
        jk   = body.get("join_key")
        home = body.get("home_team") or ""
        away = body.get("away_team") or ""
        sport = body.get("sport") or "soccer"
        date  = body.get("date") or _now_iso()[:10]
        if jk:
            saved = get_lifecycle_manager().get_watch(jk)
            if saved:
                home, away, sport = saved.home_team, saved.away_team, saved.sport
                date = saved.start_time[:10]
        ss_id = _sofascore.resolve_event_id(home, away, sport, date)
        return jsonify({"sofascore_id": ss_id, "resolved": ss_id is not None})

except ImportError:
    bp_lifecycle = None  # Flask not available (e.g. in Celery worker)