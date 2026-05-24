"""
odds_narration.py — AI VOICE-OVER for Odds Video Studio
────────────────────────────────────────────────────────────────────────────
Generates a short, timed narration script for an odds/arbitrage video and
synthesizes it to speech. It is a SIBLING of commentary.py and reuses that
module's voice catalogue, so the studio's voice picker and the commentary
broadcast share exactly the same voices.

TEXT  : OpenAI gpt-4o-mini  (OPENAI_API_KEY)
VOICE : edge-tts            (FREE Microsoft neural voices — no API key)

Register it on the SAME Flask app as bp_commentary, e.g.:

    from app.views.customer.odds_narration import bp_odds_narration
    app.register_blueprint(bp_odds_narration)

ENDPOINTS
  GET  /api/odds-video/voices       -> voice catalogue (shared with commentary)
  POST /api/odds-video/narration    -> {segments:[{scene,start,dur,text,url}], ...}
  GET  /api/odds-video/audio/<file> -> serve a cached narration mp3
"""

import asyncio
import hashlib
import json
import logging
import os
import time

from flask import Blueprint, request, jsonify, send_file, abort

log = logging.getLogger("odds_narration")
bp_odds_narration = Blueprint("odds_narration", __name__, url_prefix="/api/odds-video")

# ── OpenAI (script writer) ──────────────────────────────────────────────────
try:
    from openai import OpenAI
    _openai = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))
    OPENAI_OK = bool(os.environ.get("OPENAI_API_KEY"))
except Exception as e:                                  # noqa
    _openai, OPENAI_OK = None, False
    log.warning(f"OpenAI unavailable: {e}")

# ── edge-tts (speech) ───────────────────────────────────────────────────────
try:
    import edge_tts
    EDGE_OK = True
except Exception as e:                                  # noqa
    EDGE_OK = False
    log.warning(f"edge-tts unavailable: {e}")

# ── voice catalogue — reuse commentary.py so both share one voice list ──────
try:
    from app.views.customer.gemini_comentary import (
        EDGE_TTS_VOICES, DEFAULT_MALE, DEFAULT_FEMALE,
    )
except Exception:                                       # standalone fallback
    EDGE_TTS_VOICES = [
        {"id": "en-US-GuyNeural",      "name": "Guy",      "gender": "male",   "locale": "en-US", "tag": "news"},
        {"id": "en-US-AriaNeural",     "name": "Aria",     "gender": "female", "locale": "en-US", "tag": "expressive"},
        {"id": "en-GB-RyanNeural",     "name": "Ryan",     "gender": "male",   "locale": "en-GB", "tag": "authoritative"},
        {"id": "en-GB-SoniaNeural",    "name": "Sonia",    "gender": "female", "locale": "en-GB", "tag": "warm"},
        {"id": "en-KE-ChilembaNeural", "name": "Chilemba", "gender": "male",   "locale": "en-KE", "tag": "natural"},
        {"id": "en-KE-AsiliaNeural",   "name": "Asilia",   "gender": "female", "locale": "en-KE", "tag": "natural"},
    ]
    DEFAULT_MALE, DEFAULT_FEMALE = "en-US-GuyNeural", "en-US-AriaNeural"

# ── cache dir ───────────────────────────────────────────────────────────────
_CACHE = os.path.join(os.environ.get("AUDIO_CACHE_DIR", "/tmp/kinetic_audio"), "narration")
os.makedirs(_CACHE, exist_ok=True)

WORDS_PER_SEC = 2.6      # spoken pace used to size each scene's line
SCENES = ["intro", "match", "odds", "arb", "outro"]

NARRATION_STYLES = {
    "hype":    "loud, fast, high-energy sports-betting promo",
    "analyst": "calm, sharp, data-driven betting analyst",
    "street":  "casual, confident, street-smart Kenyan punter",
}


# ── add permissive CORS only when the app hasn't already set it ─────────────
@bp_odds_narration.after_request
def _cors(resp):
    if "Access-Control-Allow-Origin" not in resp.headers:
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    return resp


# ── scene timing — mirrors getSceneBoundaries() in OddsVideoComponents.tsx ──
def _boundaries(num_arbs: int):
    if num_arbs <= 1:
        return [0, 0.18, 0.36, 0.62, 0.82, 1.0]
    intro_w, match_w, odds_w, outro_w = 14, 14, 20, 14
    arb_w = 24 + (num_arbs - 1) * 16
    tot = intro_w + match_w + odds_w + arb_w + outro_w
    return [
        0, intro_w / tot, (intro_w + match_w) / tot,
        (intro_w + match_w + odds_w) / tot,
        (intro_w + match_w + odds_w + arb_w) / tot, 1.0,
    ]


# ── script generation ───────────────────────────────────────────────────────
def _fallback_script(home, away, comp, arb, budgets):
    """Used when OpenAI is unavailable — keeps the feature working offline."""
    return {
        "intro": "Stop scrolling. There is free money on the table tonight.",
        "match": f"{home} take on {away} in the {comp}.",
        "odds":  "Here are the best prices across every bookmaker, side by side.",
        "arb":   f"A guaranteed arbitrage edge of {arb} percent — you profit whoever wins.",
        "outro": ("Follow for daily edges. Strictly for adults eighteen and over. "
                  "Gamble responsibly."),
    }


def _script(match: dict, durations: dict, style: str) -> dict:
    home = match.get("home_team", "Home")
    away = match.get("away_team", "Away")
    comp = match.get("competition", "the match")
    arb = match.get("best_arb_pct", 0)
    mw = (match.get("best", {}) or {}).get("match_winner", {}) or {}
    o1 = (mw.get("1", {}) or {}).get("odd")
    ox = (mw.get("X", {}) or {}).get("odd")
    o2 = (mw.get("2", {}) or {}).get("odd")
    bks = match.get("bookmakers", []) or []
    budgets = {s: max(6, round(WORDS_PER_SEC * durations[s])) for s in SCENES}

    if not OPENAI_OK:
        return _fallback_script(home, away, comp, arb, budgets)

    tone = NARRATION_STYLES.get(style, NARRATION_STYLES["hype"])
    facts = (f"Match: {home} vs {away} ({comp}). "
             f"Best odds — Home {o1}, Draw {ox}, Away {o2}. "
             f"Arbitrage edge: {arb} percent. "
             f"Bookmakers: {', '.join(str(b) for b in bks[:6]) or 'multiple'}.")
    sys = ("You write the voice-over for a short sports-betting highlight video. "
           f"Tone: {tone}. One narrator, spoken aloud. Short, punchy sentences. "
           "No emojis, no stage directions, no speaker labels — only the words to "
           "be spoken. The outro MUST contain a brief responsible-gambling line and "
           "state it is for adults eighteen and over.")
    usr = (f"{facts}\n\nWrite ONE narration line per scene, each within ~15% of its "
           f"word budget so it fits the scene length:\n"
           f"- intro ({budgets['intro']} words): hook the viewer, tease the edge.\n"
           f"- match ({budgets['match']} words): name the teams and the fixture.\n"
           f"- odds ({budgets['odds']} words): call out the best odds and bookmakers.\n"
           f"- arb ({budgets['arb']} words): reveal the guaranteed arbitrage profit.\n"
           f"- outro ({budgets['outro']} words): call to action + 18+ responsible-gambling note.\n\n"
           'Return ONLY JSON: {"intro":"","match":"","odds":"","arb":"","outro":""}')
    try:
        r = _openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": sys},
                      {"role": "user", "content": usr}],
            temperature=0.8, max_tokens=600,
            response_format={"type": "json_object"},
        )
        data = json.loads(r.choices[0].message.content)
        out = {s: str(data.get(s, "")).strip() for s in SCENES}
        if all(out.values()):
            return out
    except Exception as e:
        log.warning(f"script generation failed: {e}")
    return _fallback_script(home, away, comp, arb, budgets)


# ── text-to-speech ──────────────────────────────────────────────────────────
def _rate_for(text: str, target: float) -> str:
    """Pick an edge-tts speaking rate so the line roughly fits its scene."""
    words = max(1, len(text.split()))
    est = words / WORDS_PER_SEC
    pct = int(round((est / max(0.8, target) - 1) * 100))
    pct = max(-10, min(45, pct))
    return f"+{pct}%" if pct >= 0 else f"{pct}%"


async def _synth(text: str, voice: str, rate: str, path: str):
    await edge_tts.Communicate(text, voice, rate=rate).save(path)


def _tts(text: str, voice: str, target: float, path: str) -> bool:
    if not EDGE_OK or not text:
        return False
    try:
        asyncio.run(_synth(text, voice, _rate_for(text, target), path))
        return os.path.getsize(path) > 200
    except Exception as e:
        log.warning(f"tts failed ({voice}): {e}")
        try:                                            # retry at neutral rate
            asyncio.run(_synth(text, voice, "+0%", path))
            return os.path.getsize(path) > 200
        except Exception:
            return False


# ═══════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════
@bp_odds_narration.route("/voices", methods=["GET"])
def voices():
    """Voice catalogue — same list the commentary broadcast uses."""
    return jsonify({
        "voices": EDGE_TTS_VOICES,
        "default_male": DEFAULT_MALE,
        "default_female": DEFAULT_FEMALE,
        "tts_available": EDGE_OK,
        "script_ai": OPENAI_OK,
    })


@bp_odds_narration.route("/narration", methods=["POST", "OPTIONS"])
def narration():
    """Generate a timed voice-over for the odds video."""
    if request.method == "OPTIONS":
        return ("", 204)
    if not EDGE_OK:
        return jsonify(error="edge-tts is not installed on the server"), 503

    body = request.get_json(silent=True) or {}
    match = body.get("match", {}) or {}
    try:
        dur = float(body.get("duration", 18) or 18)
    except (TypeError, ValueError):
        dur = 18.0
    dur = max(8.0, min(60.0, dur))
    voice = body.get("voice", DEFAULT_MALE)
    style = body.get("style", "hype")

    # Per-scene start times + lengths, identical to the canvas timeline.
    num_arbs = max(1, len(match.get("arb_opportunities", []) or []))
    b = _boundaries(num_arbs)
    starts = {SCENES[i]: b[i] * dur for i in range(5)}
    durations = {SCENES[i]: (b[i + 1] - b[i]) * dur for i in range(5)}

    # Cache key — same match + duration + voice + style reuses cached audio.
    key = hashlib.sha1(json.dumps({
        "h": match.get("home_team"), "a": match.get("away_team"),
        "c": match.get("competition"), "p": match.get("best_arb_pct"),
        "d": round(dur, 1), "v": voice, "s": style,
    }, sort_keys=True, default=str).encode()).hexdigest()[:16]

    t0 = time.time()
    script = _script(match, durations, style)
    base = request.host_url.rstrip("/")

    segments = []
    for s in SCENES:
        text = script.get(s, "")
        fname = f"{key}_{s}.mp3"
        fpath = os.path.join(_CACHE, fname)
        if not (os.path.exists(fpath) and os.path.getsize(fpath) > 200):
            _tts(text, voice, durations[s], fpath)
        if os.path.exists(fpath) and os.path.getsize(fpath) > 200:
            segments.append({
                "scene": s,
                "text": text,
                "start": round(starts[s], 3),
                "dur": round(durations[s], 3),
                "url": f"{base}/api/odds-video/audio/{fname}",
            })

    log.info(f"narration {match.get('home_team')} vs {match.get('away_team')} "
             f"— {len(segments)} clips in {time.time() - t0:.1f}s")
    return jsonify({
        "voice": voice, "style": style, "total_duration": dur,
        "segments": segments,
    })


@bp_odds_narration.route("/audio/<path:fname>", methods=["GET"])
def audio(fname: str):
    """Serve a cached narration mp3."""
    safe = os.path.basename(fname)                      # block path traversal
    path = os.path.join(_CACHE, safe)
    if not os.path.exists(path):
        abort(404)
    return send_file(path, mimetype="audio/mpeg", conditional=True)