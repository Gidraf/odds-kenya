"""
app/services/playwright_onboarding_manager.py
===============================================
Human-in-the-loop Playwright onboarding — polling edition.

Changes from socket.io version
────────────────────────────────
• NO socket.io — backend POSTs webhook events to itself via HTTP
  (POST /playwright/internal/webhook  with {event, session_id, ...payload})
• Frontend polls GET /playwright/sessions/<id>/state every 2 s
• Playwright injects a floating HUD overlay into the browser so the user
  can see which phase is active and toggle phase selection directly
• All captured requests stored server-side; frontend fetches via REST
• partner_id / country / lang auto-saved to session on every capture
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

try:
    from playwright.async_api import async_playwright, Response as PwResponse
    _PW_OK = True
except ImportError:
    _PW_OK = False

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

_PHASE_URL_HINTS = {
    "list": [
        r"(events|matches|fixtures|prematch|upcoming|schedule|odds)",
        r"(get1x2|livefeed|getgame|sportevents)",
        r"(mode=4|type=prematch|filter=upcoming)",
    ],
    "live_list": [
        r"(live|inplay|in.play|inprogress)",
        r"(mode=1|mode=2|type=live|status=live)",
        r"(getlive|livegames|liveevents)",
    ],
    "markets": [
        r"(markets|odds|prices|selections|outcomes)",
        r"(match_id|event_id|fixture_id|game_id|eventid)",
        r"(deepmarket|getgamezip|getodds|getmarket)",
    ],
    "live_markets": [
        r"(live.*market|market.*live|liveodds|live.*odds)",
        r"(live.*event.*odds|livetrade)",
    ],
}

_SKIP_URL_PATTERNS = re.compile(
    r"\.(png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|css|js\.map)"
    r"|analytics|tracking|beacon|pixel|sentry|hotjar|gtag"
    r"|fonts\.google|cdnjs|jsdelivr",
    re.I,
)

_PHASE_FIELD_HINTS = {
    "list":         {"I", "O1", "O2", "S", "SN", "L", "home", "away", "id", "teams", "homeTeam", "awayTeam", "startTime", "kickOff", "matchId"},
    "live_list":    {"I", "O1", "O2", "SC", "CP", "CPS", "live", "score", "currentScore", "minute", "period"},
    "markets":      {"E", "AE", "markets", "outcomes", "selections", "odds", "price", "coefficient", "C", "T", "G"},
    "live_markets": {"E", "AE", "markets", "outcomes", "selections", "liveOdds", "coefficient", "C", "T"},
}

PHASES_ORDERED = ["list", "markets", "live_list", "live_markets"]

PHASE_GUIDE = {
    "list":         {"name": "Upcoming Match List",   "message": "Navigate to upcoming sports events with many match listings.", "icon": "📋"},
    "markets":      {"name": "Single Match Markets",  "message": "Click on any single match to open its full odds/markets page.", "icon": "📊"},
    "live_list":    {"name": "Live Matches List",     "message": "Navigate to the LIVE or In-Play section.", "icon": "🔴"},
    "live_markets": {"name": "Live Match Markets",    "message": "Click on any live match to see its live odds.", "icon": "⚡"},
}

# ─────────────────────────────────────────────────────────────────────────────
# Browser HUD overlay injected into the Playwright page
# ─────────────────────────────────────────────────────────────────────────────

_HUD_SCRIPT = """
(function() {
  if (document.getElementById('__pw_hud')) return;

  const PHASES = ['list','markets','live_list','live_markets'];
  const LABELS = {list:'📋 Upcoming List', markets:'📊 Match Markets', live_list:'🔴 Live List', live_markets:'⚡ Live Markets'};
  const ACCENT = {list:'#c6f135', markets:'#38bdf8', live_list:'#f472b6', live_markets:'#fb923c'};

  let currentPhase = '__CURRENT_PHASE__';
  let phaseDone    = __PHASE_DONE_JSON__;

  const hud = document.createElement('div');
  hud.id = '__pw_hud';
  hud.style.cssText = `
    position: fixed; top: 12px; right: 12px; z-index: 2147483647;
    background: rgba(10,10,10,.96); border: 1px solid rgba(198,241,53,.35);
    font-family: 'IBM Plex Mono', monospace; font-size: 10px; color: #eee;
    width: 230px; border-radius: 2px; box-shadow: 0 4px 32px rgba(0,0,0,.8);
    user-select: none;
  `;

  hud.innerHTML = `
    <div style="padding:8px 10px 6px;border-bottom:1px solid rgba(255,255,255,.08);
         color:#c6f135;font-size:8px;letter-spacing:2px;display:flex;justify-content:space-between;align-items:center">
      PLAYWRIGHT CAPTURE
      <span id="__pw_count" style="color:rgba(255,255,255,.4);font-size:7px">0 captured</span>
    </div>
    <div style="padding:6px 4px">
      ${PHASES.map(p => `
        <div id="__pw_ph_${p}" onclick="__pw_setPhase('${p}')" style="
          padding:6px 8px; cursor:pointer; border-left:3px solid transparent;
          margin:2px 0; transition:all .15s; border-radius:1px;
        ">
          <div style="display:flex;justify-content:space-between;align-items:center">
            <span style="color:inherit">${LABELS[p]}</span>
            <span id="__pw_ph_${p}_cnt" style="font-size:7px;color:rgba(255,255,255,.35)">0</span>
          </div>
        </div>
      `).join('')}
    </div>
    <div style="padding:6px 10px 8px;border-top:1px solid rgba(255,255,255,.06);font-size:7px;color:rgba(255,255,255,.3);line-height:1.5" id="__pw_msg">
      Navigate to the page for the active phase
    </div>
  `;
  document.body.appendChild(hud);

  function render() {
    PHASES.forEach(p => {
      const el  = document.getElementById('__pw_ph_' + p);
      if (!el) return;
      const done = phaseDone[p];
      const active = p === currentPhase;
      el.style.borderLeftColor = done ? ACCENT[p] : active ? 'var(--cyan,#06b6d4)' : 'transparent';
      el.style.background      = active ? 'rgba(255,255,255,.04)' : 'transparent';
      el.style.color           = done ? ACCENT[p] : active ? '#fff' : 'rgba(255,255,255,.45)';
    });
    const guide = {
      list:'Navigate to upcoming sports events',
      markets:'Click a single match to open its markets',
      live_list:'Go to the Live / In-Play section',
      live_markets:'Click a live match for its live odds'
    };
    const msgEl = document.getElementById('__pw_msg');
    if (msgEl) msgEl.textContent = guide[currentPhase] || '';
  }
  render();

  window.__pw_setPhase = function(phase) {
    currentPhase = phase;
    render();
    fetch('/__pw_phase', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({phase, session_id:'__SESSION_ID__'})
    }).catch(()=>{});
  };

  window.__pw_updateCounts = function(counts, done) {
    phaseDone = done || {};
    let total = 0;
    Object.keys(counts).forEach(p => {
      const el = document.getElementById('__pw_ph_' + p + '_cnt');
      if (el) el.textContent = counts[p] || 0;
      total += (counts[p] || 0);
    });
    const ce = document.getElementById('__pw_count');
    if (ce) ce.textContent = total + ' captured';
    render();
  };
})();
"""

# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CapturedReq:
    phase:        str
    url:          str
    method:       str
    status:       int
    body_raw:     str
    body_size:    int
    req_headers:  dict  = field(default_factory=dict)
    resp_headers: dict  = field(default_factory=dict)
    params:       dict  = field(default_factory=dict)
    score:        float = 0.0
    ts:           float = field(default_factory=time.time)

    @property
    def body_preview(self) -> str:
        return self.body_raw[:800]

    @property
    def parsed_body(self) -> Any:
        try:
            return json.loads(self.body_raw)
        except Exception:
            return None

    def to_wire(self) -> dict:
        return {
            "phase":        self.phase,
            "url":          self.url,
            "method":       self.method,
            "status":       self.status,
            "body_size":    self.body_size,
            "body_preview": self.body_preview,
            "params":       self.params,
            "score":        round(self.score, 2),
            "ts":           self.ts,
            "params_extracted": extract_partner_config(self.url),
        }


@dataclass
class PhaseResult:
    phase:           str
    chosen_url:      str  = ""
    url_template:    str  = ""
    method:          str  = "GET"
    headers:         dict = field(default_factory=dict)
    params:          dict = field(default_factory=dict)
    placeholder_map: dict = field(default_factory=dict)
    sample_items:    list = field(default_factory=list)
    partner_config:  dict = field(default_factory=dict)
    array_path:      str  = ""
    confirmed:       bool = False
    skipped:         bool = False


@dataclass
class LogEntry:
    level:      str
    msg:        str
    ts:         float = field(default_factory=time.time)
    session_id: str   = ""


@dataclass
class PlaywrightSession:
    session_id:     str
    domain:         str
    bookmaker_id:   int
    country_code:   str              = "KE"
    current_phase:  str              = "list"
    phases:         dict             = field(default_factory=dict)
    captures:       dict             = field(default_factory=lambda: defaultdict(list))
    logs:           list             = field(default_factory=list)
    is_active:      bool             = True
    partner_config: dict             = field(default_factory=dict)
    vendor_slug:    str | None       = None
    started_at:     float            = field(default_factory=time.time)
    page_url:       str              = ""
    page_title:     str              = ""
    status:         str              = "launching"  # launching|active|complete|error
    error:          str              = ""


# ─────────────────────────────────────────────────────────────────────────────
# Scoring helpers
# ─────────────────────────────────────────────────────────────────────────────

def _score_request(cap: CapturedReq, phase: str) -> float:
    score = 0.0
    url_lower = cap.url.lower()
    for pattern in _PHASE_URL_HINTS.get(phase, []):
        if re.search(pattern, url_lower, re.I):
            score += 20
    body = cap.parsed_body
    if body:
        items = body if isinstance(body, list) else body.get("Value", body.get("data", body.get("events", [])))
        if isinstance(items, list) and items:
            first = items[0] if isinstance(items[0], dict) else {}
            hints = _PHASE_FIELD_HINTS.get(phase, set())
            score += min(40, sum(1 for k in first if k in hints) * 10)
            if phase in ("list", "live_list"):
                score += min(20, len(items) * 0.4)
            if phase in ("markets", "live_markets") and isinstance(first.get("E"), list):
                score += 25
    elif cap.body_size > 0:
        score -= 10
    score += 10 if cap.body_size > 10_000 else (-20 if cap.body_size < 200 else 0)
    score += 5 if cap.status == 200 else (-50 if cap.status >= 400 else 0)
    return max(0.0, score)


def _detect_array_path(parsed: Any) -> str:
    if isinstance(parsed, list):
        return ""
    if isinstance(parsed, dict):
        best_key, best_len = "", 0
        for k, v in parsed.items():
            if isinstance(v, list) and len(v) > best_len:
                best_len = len(v); best_key = k
        return best_key
    return ""


def _extract_items(parsed: Any, array_path: str = "") -> list:
    if array_path:
        for key in array_path.split("."):
            parsed = parsed.get(key) if isinstance(parsed, dict) else None
            if parsed is None:
                return []
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        for v in parsed.values():
            if isinstance(v, list) and len(v) > 0:
                return v
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Partner config + placeholder extraction
# ─────────────────────────────────────────────────────────────────────────────

def extract_partner_config(url: str) -> dict:
    config: dict = {}
    try:
        parsed = urllib.parse.urlparse(url)
        config["base_url"] = f"{parsed.scheme}://{parsed.netloc}"
        qs = dict(urllib.parse.parse_qsl(parsed.query))
        for key in ("partner", "country", "lang", "lng", "mode", "count",
                    "partner_id", "affiliate", "pid", "source", "currency"):
            if key in qs:
                config[key] = qs[key]
    except Exception:
        pass
    return config


_ROLE_HINTS = [
    (re.compile(r"^[Ii]$|match.?id|event.?id|game.?id|fixture.?id", re.I), "match_id"),
    (re.compile(r"sport.?id|^SI$|sport$",                                   re.I), "sport_id"),
    (re.compile(r"league|competition|champ|tour",                            re.I), "league_id"),
    (re.compile(r"partner|affiliate|pid",                                    re.I), "partner_id"),
    (re.compile(r"country|region|loc",                                       re.I), "country"),
    (re.compile(r"lang|language|lng",                                        re.I), "lang"),
]

def _flatten_item(obj: Any, prefix: str = "", depth: int = 0) -> list[tuple[str, str]]:
    if depth > 5 or obj is None: return []
    if isinstance(obj, (str, int, float, bool)) and prefix: return [(prefix, str(obj))]
    if isinstance(obj, list):
        out = []
        for i, v in enumerate(obj[:3]):
            out.extend(_flatten_item(v, f"{prefix}[{i}]", depth + 1))
        return out
    if isinstance(obj, dict):
        out = []
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (str, int, float, bool)): out.append((path, str(v)))
            else: out.extend(_flatten_item(v, path, depth + 1))
        return out
    return []

def _var_name(field_path: str) -> str:
    leaf = field_path.split(".")[-1].split("[")[0]
    for pattern, role in _ROLE_HINTS:
        if pattern.search(leaf): return role
    clean = re.sub(r"([A-Z])", r"_\1", leaf).lower().strip("_")
    return re.sub(r"[^a-z0-9_]", "_", clean).strip("_") or "value"

def detect_placeholders_from_capture(market_cap: CapturedReq, list_items: list) -> dict:
    if not list_items:
        return {"url_template": market_cap.url, "params": market_cap.params, "placeholder_map": {}}
    first = list_items[0] if isinstance(list_items[0], dict) else {}
    flat  = _flatten_item(first)
    qidx     = market_cap.url.find("?")
    base_url = market_cap.url[:qidx] if qidx >= 0 else market_cap.url
    qs       = market_cap.url[qidx + 1:] if qidx >= 0 else ""
    qs_parts = []
    for part in qs.split("&"):
        if not part: continue
        eq = part.find("=")
        if eq >= 0:
            k = urllib.parse.unquote_plus(part[:eq])
            v = urllib.parse.unquote_plus(part[eq + 1:])
            qs_parts.append([k, v, part[:eq]])
        else:
            qs_parts.append([urllib.parse.unquote_plus(part), "", part])
    detected, placeholder_map, used_vars = {}, {}, set()
    for field_path, raw_value in flat:
        value = raw_value.strip()
        if len(value) < 3 or value.lower() in ("true","false","null","none","undefined"):
            continue
        var = _var_name(field_path)
        base_v = var; n = 2
        while var in used_vars: var = f"{base_v}_{n}"; n += 1
        ph = f"{{{{{var}}}}}"
        found = False
        if value in base_url:
            base_url = base_url.replace(value, ph, 1)
            detected[var] = {"field_path": field_path, "value": value}
            placeholder_map[var] = field_path
            used_vars.add(var); found = True
        if not found:
            for part in qs_parts:
                if str(part[1]).strip() == value:
                    part[1] = ph
                    detected[var] = {"field_path": field_path, "value": value}
                    placeholder_map[var] = field_path
                    used_vars.add(var); found = True; break
    rebuilt = [f"{p[0]}={urllib.parse.quote(str(p[1]), safe='{}')}" for p in qs_parts]
    url_template = f"{base_url}?{'&'.join(rebuilt)}" if rebuilt else base_url
    return {"url_template": url_template, "params": {p[0]: p[1] for p in qs_parts}, "placeholder_map": placeholder_map, "detected": detected}


def detect_vendor(caps: dict) -> str | None:
    for phase_caps in caps.values():
        for cap in phase_caps:
            body = cap.parsed_body
            if not body: continue
            items = body if isinstance(body, list) else body.get("Value", [])
            if isinstance(items, list) and items and isinstance(items[0], dict):
                first = items[0]
                if all(k in first for k in ("I", "O1", "O2", "SI")): return "1xbet-livefeed"
                if "homeTeamName" in first and "markets" in first:    return "betway-generic"
                if "homeTeam" in first and "eventId" in first:        return "sportingbet-generic"
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Session store
# ─────────────────────────────────────────────────────────────────────────────

_SESSIONS: dict[str, PlaywrightSession] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Manager
# ─────────────────────────────────────────────────────────────────────────────

class PlaywrightOnboardingManager:
    """
    All real-time events are now stored on the session object.
    The frontend polls /playwright/sessions/<id>/state every 2 s.
    Playwright posts webhooks to itself via _post_webhook() which simply
    writes to the in-process session (no HTTP round-trip needed).
    """

    def _log(self, sid: str, level: str, msg: str):
        session = _SESSIONS.get(sid)
        if session:
            entry = LogEntry(level=level, msg=msg, session_id=sid)
            session.logs.append(entry)
            # keep last 200
            if len(session.logs) > 200:
                session.logs = session.logs[-200:]

    def _post_webhook(self, sid: str, event: str, payload: dict):
        """Write event directly to session state (same-process webhook)."""
        session = _SESSIONS.get(sid)
        if not session:
            return
        if event == "browser_ready":
            session.status      = "active"
            session.page_url    = payload.get("url", "")
            session.page_title  = payload.get("title", "")
            self._log(sid, "SUCCESS", f"Browser ready: {payload.get('title')}")
        elif event == "page_navigated":
            session.page_url   = payload.get("url", "")
            session.page_title = payload.get("title", "")
        elif event == "session_complete":
            session.status      = "complete"
            session.vendor_slug = payload.get("vendor_slug")
            self._log(sid, "SUCCESS", f"Session complete — vendor: {session.vendor_slug or 'unknown'}")
        elif event == "session_error":
            session.status = "error"
            session.error  = payload.get("error", "Unknown error")
            self._log(sid, "ERROR", session.error)

    # ── Public API ─────────────────────────────────────────────────────────────

    def start_session(self, session_id: str, domain: str, bookmaker_id: int,
                      country_code: str = "KE") -> dict:
        if not _PW_OK:
            return {"ok": False, "error": "playwright not installed — run: pip install playwright && playwright install chromium"}
        if session_id in _SESSIONS:
            return {"ok": False, "error": "Session already active"}
        session = PlaywrightSession(
            session_id=session_id, domain=domain,
            bookmaker_id=bookmaker_id, country_code=country_code,
        )
        _SESSIONS[session_id] = session
        threading.Thread(target=self._run_thread, args=(session_id,),
                         daemon=True, name=f"pw-{session_id[:8]}").start()
        return {"ok": True, "session_id": session_id}

    def get_state(self, session_id: str) -> dict | None:
        session = _SESSIONS.get(session_id)
        if not session:
            return None
        phase_done = {p: bool(session.phases.get(p) and session.phases[p].confirmed)
                      for p in PHASES_ORDERED}
        cap_counts = {phase: len(caps) for phase, caps in session.captures.items()}
        return {
            "session_id":     session.session_id,
            "domain":         session.domain,
            "bookmaker_id":   session.bookmaker_id,
            "status":         session.status,
            "current_phase":  session.current_phase,
            "page_url":       session.page_url,
            "page_title":     session.page_title,
            "partner_config": session.partner_config,
            "vendor_slug":    session.vendor_slug,
            "error":          session.error,
            "phase_done":     phase_done,
            "capture_counts": cap_counts,
            "phases": {
                phase: {
                    "confirmed":     r.confirmed,
                    "skipped":       r.skipped,
                    "url_template":  r.url_template,
                    "placeholder_map": r.placeholder_map,
                    "array_path":    r.array_path,
                    "sample_count":  len(r.sample_items),
                }
                for phase, r in session.phases.items()
            },
            "logs": [
                {"level": l.level, "msg": l.msg, "ts": l.ts}
                for l in session.logs[-60:]
            ],
        }

    def get_candidates(self, session_id: str, phase: str) -> list[dict]:
        session = _SESSIONS.get(session_id)
        if not session:
            return []
        caps = list(session.captures.get(phase, []))
        for cap in caps:
            cap.score = _score_request(cap, phase)
        caps.sort(key=lambda c: c.score, reverse=True)
        return [c.to_wire() for c in caps[:30]]

    def confirm_phase(self, session_id: str, phase: str, chosen_url: str | None = None) -> dict:
        session = _SESSIONS.get(session_id)
        if not session:
            return {"ok": False, "error": "session not found"}
        caps = list(session.captures.get(phase, []))
        if not chosen_url and caps:
            for c in caps: c.score = _score_request(c, phase)
            caps.sort(key=lambda c: c.score, reverse=True)
            chosen_url = caps[0].url
        cap = next((c for c in caps if c.url == chosen_url), caps[0] if caps else None)
        if not cap:
            return {"ok": False, "error": "no captures for this phase"}

        extraction = self._extract_phase(session, phase, cap)
        result = PhaseResult(
            phase=phase, chosen_url=chosen_url or cap.url,
            url_template=extraction.get("url_template", cap.url),
            method=cap.method,
            headers={k: v for k, v in cap.req_headers.items()
                     if k.lower() not in ("host", "content-length", ":method", ":path")},
            params=extraction.get("params", cap.params),
            placeholder_map=extraction.get("placeholder_map", {}),
            sample_items=extraction.get("sample_items", []),
            partner_config=extraction.get("partner_config", {}),
            array_path=extraction.get("array_path", ""),
            confirmed=True,
        )
        session.phases[phase] = result
        session.partner_config.update(result.partner_config)
        # Remove base_url from partner_config display (keep it internal)
        session.partner_config.pop("base_url", None)

        self._log(session_id, "SUCCESS", f"✓ Phase '{phase}' confirmed — {len(result.sample_items)} items")

        # Advance
        idx = PHASES_ORDERED.index(phase) if phase in PHASES_ORDERED else -1
        if idx >= 0 and idx + 1 < len(PHASES_ORDERED):
            session.current_phase = PHASES_ORDERED[idx + 1]
        else:
            self._finalise(session_id)

        return {"ok": True, "extraction": extraction}

    def skip_phase(self, session_id: str, phase: str):
        session = _SESSIONS.get(session_id)
        if not session: return
        session.phases[phase] = PhaseResult(phase=phase, skipped=True)
        self._log(session_id, "INFO", f"Phase '{phase}' skipped")
        idx = PHASES_ORDERED.index(phase) if phase in PHASES_ORDERED else -1
        if idx >= 0 and idx + 1 < len(PHASES_ORDERED):
            session.current_phase = PHASES_ORDERED[idx + 1]
        else:
            self._finalise(session_id)

    def set_phase(self, session_id: str, phase: str):
        """Called when user clicks a phase in the browser HUD."""
        session = _SESSIONS.get(session_id)
        if session and phase in PHASES_ORDERED:
            session.current_phase = phase
            self._log(session_id, "INFO", f"Phase switched to '{phase}' via browser HUD")

    def recapture_phase(self, session_id: str, phase: str):
        session = _SESSIONS.get(session_id)
        if session:
            session.captures[phase] = []
            self._log(session_id, "INFO", f"Captures cleared for phase '{phase}'")

    def set_partner_config(self, session_id: str, config: dict):
        session = _SESSIONS.get(session_id)
        if session:
            session.partner_config.update(config)
            self._log(session_id, "INFO", f"Partner config updated: {list(config.keys())}")

    def stop_session(self, session_id: str):
        session = _SESSIONS.get(session_id)
        if session:
            session.is_active = False
        _SESSIONS.pop(session_id, None)

    # ── Extraction ─────────────────────────────────────────────────────────────

    def _extract_phase(self, session: PlaywrightSession, phase: str, cap: CapturedReq) -> dict:
        body       = cap.parsed_body
        array_path = _detect_array_path(body) if body else ""
        items      = _extract_items(body, array_path) if body else []
        partner    = extract_partner_config(cap.url)
        result     = {
            "url_template":    cap.url,
            "params":          cap.params,
            "placeholder_map": {},
            "array_path":      array_path,
            "sample_items":    items[:5],
            "partner_config":  partner,
        }
        if phase in ("markets", "live_markets"):
            list_phase = "live_list" if phase == "live_markets" else "list"
            list_items: list = []
            for lc in session.captures.get(list_phase, []):
                lb = lc.parsed_body
                if lb:
                    li = _extract_items(lb, _detect_array_path(lb))
                    if li: list_items = li; break
            if list_items:
                result.update(detect_placeholders_from_capture(cap, list_items))
            else:
                # Fallback: template from QS keys
                qidx = cap.url.find("?")
                base = cap.url[:qidx] if qidx >= 0 else cap.url
                qs   = cap.url[qidx + 1:] if qidx >= 0 else ""
                parts = []
                for part in qs.split("&"):
                    if not part: continue
                    eq = part.find("=")
                    parts.append(f"{part[:eq]}={{{{{urllib.parse.unquote_plus(part[:eq])}}}}}" if eq >= 0 else part)
                result["url_template"] = f"{base}?{'&'.join(parts)}" if parts else base
        return result

    # ── Playwright async thread ───────────────────────────────────────────────

    def _run_thread(self, session_id: str):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self._async_session(session_id))
        except Exception as e:
            self._post_webhook(session_id, "session_error", {"error": str(e)})
        finally:
            loop.close()

    async def _async_session(self, session_id: str):
        session = _SESSIONS.get(session_id)
        if not session: return

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled",
                      "--no-sandbox", "--window-size=1440,900", "--lang=en-US"],
            )
            context = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                user_agent=("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/131.0.0.0 Safari/537.36"),
                locale="en-US",
                timezone_id="Africa/Nairobi" if session.country_code == "KE" else "UTC",
                ignore_https_errors=True,
            )
            page = await context.new_page()

            # ── Passive response listener ──────────────────────────────────
            async def handle_response(response: PwResponse):
                try:
                    url_str = response.url
                    if _SKIP_URL_PATTERNS.search(url_str): return
                    ct = (response.headers.get("content-type") or "").lower()
                    if "json" not in ct:
                        looks_like_api = bool(re.search(
                            r"(api|service|feed|odds|events|matches|livefeed|getgame)",
                            url_str, re.I))
                        if not looks_like_api: return
                    try:
                        body_bytes = await response.body()
                    except Exception:
                        return
                    if len(body_bytes) < 40: return
                    body_text = body_bytes.decode("utf-8", errors="replace")
                    stripped  = body_text.strip()
                    if not (stripped.startswith("{") or stripped.startswith("[")): return

                    try:
                        parsed_url = urllib.parse.urlparse(url_str)
                        params = dict(urllib.parse.parse_qsl(parsed_url.query))
                    except Exception:
                        params = {}

                    best_phase = self._classify_url(url_str, body_text, session.current_phase)

                    cap = CapturedReq(
                        phase=best_phase, url=url_str,
                        method=response.request.method, status=response.status,
                        body_raw=body_text, body_size=len(body_text),
                        req_headers=dict(response.request.headers),
                        resp_headers=dict(response.headers),
                        params=params,
                    )
                    cap.score = _score_request(cap, best_phase)

                    existing = session.captures[best_phase]
                    if any(c.url == url_str for c in existing):
                        for c in existing:
                            if c.url == url_str and cap.score > c.score:
                                c.score = cap.score
                        return

                    existing.append(cap)

                    # Auto-save partner config
                    pc = extract_partner_config(url_str)
                    for k, v in pc.items():
                        if k != "base_url":
                            session.partner_config[k] = v

                    self._log(session_id, "CAPTURE",
                              f"[{best_phase.upper()}] {response.request.method} {url_str[:80]} "
                              f"({len(body_text)//1024}KB score:{round(cap.score)})")

                    # Update HUD in browser
                    await self._update_hud(page, session)

                except Exception as e:
                    self._log(session_id, "DEBUG", f"Response handler: {e}")

            page.on("response", handle_response)

            # ── Navigation tracking ────────────────────────────────────────
            async def on_nav(url_str: str):
                if not url_str.startswith("data:"):
                    try:
                        title = await page.title()
                    except Exception:
                        title = ""
                    self._post_webhook(session_id, "page_navigated",
                                       {"url": url_str, "title": title})
                    # Re-inject HUD after navigation (page may have replaced DOM)
                    await asyncio.sleep(1.5)
                    await self._inject_hud(page, session)

            page.on("url", on_nav)

            # ── Initial navigation ─────────────────────────────────────────
            try:
                await page.goto(f"https://{session.domain}",
                                wait_until="domcontentloaded", timeout=30_000)
            except Exception as e:
                self._post_webhook(session_id, "session_error",
                                   {"error": f"Could not load {session.domain}: {e}"})
                await browser.close()
                return

            title = await page.title()
            self._post_webhook(session_id, "browser_ready",
                               {"url": page.url, "title": title})

            await self._inject_hud(page, session)

            # ── Keep alive ─────────────────────────────────────────────────
            while session.is_active:
                await asyncio.sleep(0.5)
                try:
                    await page.evaluate("1+1")
                except Exception:
                    break  # user closed the browser

            await browser.close()
            if session.is_active:
                self._finalise(session_id)

    async def _inject_hud(self, page, session: PlaywrightSession):
        """Inject the floating overlay HUD into the current page."""
        try:
            phase_done = {p: bool(session.phases.get(p) and session.phases[p].confirmed)
                         for p in PHASES_ORDERED}
            script = (_HUD_SCRIPT
                      .replace("'__CURRENT_PHASE__'", f"'{session.current_phase}'")
                      .replace("__PHASE_DONE_JSON__", json.dumps(phase_done))
                      .replace("'__SESSION_ID__'", f"'{session.session_id}'"))
            await page.evaluate(script)
        except Exception as e:
            self._log(session.session_id, "DEBUG", f"HUD inject: {e}")

    async def _update_hud(self, page, session: PlaywrightSession):
        """Update counts and phase state in an already-injected HUD."""
        try:
            counts = {phase: len(caps) for phase, caps in session.captures.items()}
            phase_done = {p: bool(session.phases.get(p) and session.phases[p].confirmed)
                         for p in PHASES_ORDERED}
            await page.evaluate(
                f"if(window.__pw_updateCounts) __pw_updateCounts({json.dumps(counts)}, {json.dumps(phase_done)})"
            )
        except Exception:
            pass  # page navigating, ignore

    def _classify_url(self, url: str, body_text: str, current_phase: str) -> str:
        scores = {}
        for phase in PHASES_ORDERED:
            fake = CapturedReq(phase=phase, url=url, method="GET", status=200,
                               body_raw=body_text, body_size=len(body_text))
            scores[phase] = _score_request(fake, phase)
        best = max(scores, key=lambda p: scores[p])
        return best if scores[best] >= 10 else current_phase

    def _finalise(self, session_id: str):
        session = _SESSIONS.get(session_id)
        if not session: return
        session.vendor_slug = detect_vendor(session.captures)
        self._post_webhook(session_id, "session_complete",
                           {"vendor_slug": session.vendor_slug,
                            "partner_config": session.partner_config})
        try:
            from flask import current_app
            with current_app.app_context():
                self._persist(session)
        except RuntimeError:
            pass
        session.is_active = False

    def _persist(self, session: PlaywrightSession):
        """Save captured endpoints to DB."""
        try:
            from app.models.bookmakers_model import BookmakerEndpoint, Bookmaker
            from app.models.onboarding_model import BookmakerOnboardingSession
            from app.extensions import db
        except ImportError:
            return

        phase_to_ep_type = {"list": "MATCH_LIST", "live_list": "LIVE_ODDS",
                             "markets": "DEEP_MARKETS", "live_markets": "DEEP_MARKETS"}

        bk = Bookmaker.query.get(session.bookmaker_id)
        if bk and not bk.domain: bk.domain = session.domain

        onb = (BookmakerOnboardingSession.query
               .filter_by(bookmaker_id=session.bookmaker_id, is_complete=False).first()
               or BookmakerOnboardingSession(bookmaker_id=session.bookmaker_id))
        if not onb.id: db.session.add(onb)

        for phase, result in session.phases.items():
            if result.skipped or not result.confirmed: continue
            ep_type = phase_to_ep_type.get(phase, "OTHER")
            ep = (BookmakerEndpoint.query
                  .filter_by(bookmaker_id=session.bookmaker_id, endpoint_type=ep_type).first()
                  or BookmakerEndpoint(bookmaker_id=session.bookmaker_id, endpoint_type=ep_type, is_active=True))
            ep.url_pattern    = result.url_template
            ep.request_method = result.method
            ep.headers_json   = result.headers
            ep.url_params     = result.params
            ep.sample_response = json.dumps(result.sample_items[:3])[:2000]
            if not ep.id: db.session.add(ep)

            samples_json = json.dumps(result.sample_items[:10])
            setattr_map = {
                "list":         [("list_url", result.url_template), ("list_method", result.method),
                                 ("list_headers", result.headers), ("list_params", result.params),
                                 ("list_array_path", result.array_path), ("list_sample", samples_json), ("list_ok", True)],
                "markets":      [("markets_url_template", result.url_template), ("markets_method", result.method),
                                 ("markets_headers", result.headers), ("markets_params", result.params),
                                 ("markets_array_path", result.array_path), ("markets_placeholder_map", result.placeholder_map),
                                 ("markets_sample", samples_json), ("markets_ok", True)],
                "live_list":    [("live_list_url", result.url_template), ("live_list_method", result.method),
                                 ("live_list_headers", result.headers), ("live_list_params", result.params),
                                 ("live_list_array_path", result.array_path), ("live_list_sample", samples_json), ("live_list_ok", True)],
                "live_markets": [("live_markets_url_template", result.url_template), ("live_markets_method", result.method),
                                 ("live_markets_headers", result.headers), ("live_markets_params", result.params),
                                 ("live_markets_array_path", result.array_path), ("live_markets_placeholder_map", result.placeholder_map),
                                 ("live_markets_sample", samples_json), ("live_markets_ok", True)],
            }
            for attr, val in setattr_map.get(phase, []):
                if hasattr(onb, attr): setattr(onb, attr, val)

        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(f"[Playwright] Persist error: {e}")

        if session.vendor_slug:
            try:
                from app.models.vendor_template import VendorTemplate, BookmakerVendorConfig
                vendor = VendorTemplate.query.filter_by(slug=session.vendor_slug).first()
                if vendor:
                    cfg = BookmakerVendorConfig.query.filter_by(
                        vendor_id=vendor.id, bookmaker_id=session.bookmaker_id).first()
                    if not cfg:
                        cfg = BookmakerVendorConfig(
                            vendor_id=vendor.id, bookmaker_id=session.bookmaker_id,
                            base_url=f"https://{session.domain}",
                            params_json=json.dumps(session.partner_config), is_active=True,
                        )
                        db.session.add(cfg)
                        db.session.commit()
                    try:
                        from app.tasks.odds_harvest_tasks import run_onboarding_tests
                        run_onboarding_tests.delay(vendor.id, session.bookmaker_id)
                    except Exception:
                        pass
            except Exception as e:
                print(f"[Playwright] Vendor config error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Flask Blueprint  (pure REST — no socket.io)
# ─────────────────────────────────────────────────────────────────────────────

from flask import Blueprint, request as flask_request, jsonify

bp_playwright = Blueprint("playwright_onboarding", __name__)
_manager: PlaywrightOnboardingManager | None = None


def init_playwright_manager():
    global _manager
    _manager = PlaywrightOnboardingManager()


def _mgr() -> PlaywrightOnboardingManager:
    if _manager is None:
        raise RuntimeError("Call init_playwright_manager() at app startup")
    return _manager


@bp_playwright.route("/sessions", methods=["POST"])
def start_session():
    d = flask_request.json or {}
    domain       = (d.get("domain") or "").strip().lstrip("https://").lstrip("http://").rstrip("/")
    bookmaker_id = d.get("bookmaker_id")
    country_code = d.get("country_code", "KE")
    if not domain or not bookmaker_id:
        return jsonify({"ok": False, "error": "domain and bookmaker_id required"}), 400
    session_id = f"pw-{bookmaker_id}-{int(time.time())}"
    result = _mgr().start_session(session_id, domain, int(bookmaker_id), country_code)
    return jsonify(result), (201 if result["ok"] else 400)


@bp_playwright.route("/sessions/<sid>/state", methods=["GET"])
def get_state(sid: str):
    state = _mgr().get_state(sid)
    if not state:
        return jsonify({"ok": False, "error": "Session not found"}), 404
    return jsonify({"ok": True, **state})


@bp_playwright.route("/sessions/<sid>/candidates", methods=["GET"])
def get_candidates(sid: str):
    phase = flask_request.args.get("phase", "list")
    return jsonify({"ok": True, "phase": phase,
                    "candidates": _mgr().get_candidates(sid, phase)})


@bp_playwright.route("/sessions/<sid>/confirm", methods=["POST"])
def confirm_phase(sid: str):
    d = flask_request.json or {}
    return jsonify(_mgr().confirm_phase(sid, d.get("phase", ""), d.get("chosen_url")))


@bp_playwright.route("/sessions/<sid>/skip", methods=["POST"])
def skip_phase(sid: str):
    d = flask_request.json or {}
    _mgr().skip_phase(sid, d.get("phase", ""))
    return jsonify({"ok": True})


@bp_playwright.route("/sessions/<sid>/set_phase", methods=["POST"])
def set_phase(sid: str):
    """Called by the browser HUD overlay when user switches phase."""
    d = flask_request.json or {}
    _mgr().set_phase(sid, d.get("phase", ""))
    return jsonify({"ok": True})


@bp_playwright.route("/sessions/<sid>/recapture", methods=["POST"])
def recapture_phase(sid: str):
    d = flask_request.json or {}
    _mgr().recapture_phase(sid, d.get("phase", ""))
    return jsonify({"ok": True})


@bp_playwright.route("/sessions/<sid>/partner", methods=["POST"])
def set_partner(sid: str):
    d = flask_request.json or {}
    _mgr().set_partner_config(sid, d.get("config", {}))
    return jsonify({"ok": True})


@bp_playwright.route("/sessions/<sid>", methods=["DELETE"])
def stop_session(sid: str):
    _mgr().stop_session(sid)
    return jsonify({"ok": True})


COUNTRIES = [
    {"code":"KE","name":"Kenya",        "flag":"🇰🇪","currency":"KES","region":"East Africa",    "timezone":"Africa/Nairobi"},
    {"code":"UG","name":"Uganda",       "flag":"🇺🇬","currency":"UGX","region":"East Africa",    "timezone":"Africa/Kampala"},
    {"code":"TZ","name":"Tanzania",     "flag":"🇹🇿","currency":"TZS","region":"East Africa",    "timezone":"Africa/Dar_es_Salaam"},
    {"code":"RW","name":"Rwanda",       "flag":"🇷🇼","currency":"RWF","region":"East Africa",    "timezone":"Africa/Kigali"},
    {"code":"ET","name":"Ethiopia",     "flag":"🇪🇹","currency":"ETB","region":"East Africa",    "timezone":"Africa/Addis_Ababa"},
    {"code":"NG","name":"Nigeria",      "flag":"🇳🇬","currency":"NGN","region":"West Africa",    "timezone":"Africa/Lagos"},
    {"code":"GH","name":"Ghana",        "flag":"🇬🇭","currency":"GHS","region":"West Africa",    "timezone":"Africa/Accra"},
    {"code":"ZA","name":"South Africa", "flag":"🇿🇦","currency":"ZAR","region":"Southern Africa","timezone":"Africa/Johannesburg"},
    {"code":"ZM","name":"Zambia",       "flag":"🇿🇲","currency":"ZMW","region":"Southern Africa","timezone":"Africa/Lusaka"},
    {"code":"ZW","name":"Zimbabwe",     "flag":"🇿🇼","currency":"ZWL","region":"Southern Africa","timezone":"Africa/Harare"},
    {"code":"GB","name":"United Kingdom","flag":"🇬🇧","currency":"GBP","region":"Europe",        "timezone":"Europe/London"},
    {"code":"DE","name":"Germany",      "flag":"🇩🇪","currency":"EUR","region":"Europe",        "timezone":"Europe/Berlin"},
    {"code":"MT","name":"Malta",        "flag":"🇲🇹","currency":"EUR","region":"Europe",        "timezone":"Europe/Malta"},
    {"code":"CY","name":"Cyprus",       "flag":"🇨🇾","currency":"EUR","region":"Europe",        "timezone":"Asia/Nicosia"},
]

@bp_playwright.route("/countries", methods=["GET"])
def list_countries():
    return jsonify({"ok": True, "countries": COUNTRIES})