"""
commentary.py — AI Sports Commentary Engine
────────────────────────────────────────────────────────────────────────────
PRIMARY TTS: edge-tts (FREE — Microsoft Edge neural voices, no API key)
FALLBACK TTS: OpenAI TTS (set TTS_PROVIDER=openai in env)
MinIO bucket "sport" for MP3 caching
DuckDuckGo for free live data enrichment
Full conversation continuity across all scenes
Sportradar: H2H goal timing, top scorers, managers, stadium, distance

ENDPOINTS
─────────
GET  /api/voices                                  list all TTS voices
GET  /api/odds/match/<id>/commentary              get/generate full commentary JSON
GET  /api/odds/match/<id>/commentary/stream       SSE progress stream
POST /api/odds/match/<id>/scene/<sid>/audio       generate audio for custom text+voice
POST /api/odds/match/<id>/scene/<sid>/regenerate  delete cache + regenerate
POST /api/odds/match/<id>/player/<idx>/audio      generate player spotlight audio
DELETE /api/odds/match/<id>/commentary/delete     delete all cached audio for match
GET  /api/audio/<path>                            proxy audio from MinIO
GET  /api/audio/status                            health check
"""
import asyncio, base64, hashlib, io, json, logging, os, re, time, threading
from concurrent.futures import ThreadPoolExecutor
from flask import Blueprint, request, jsonify, Response, stream_with_context
import requests
from openai import OpenAI

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("commentary.log", encoding="utf-8")],
)
log = logging.getLogger("commentary")

bp_commentary = Blueprint("commentary", __name__, url_prefix="/api")
_openai = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))
TTS_PROVIDER = os.environ.get("TTS_PROVIDER", "edge").lower()

# ── MinIO ─────────────────────────────────────────────────────────────────────
MINIO_OK = False; _minio = None; BUCKET = "sport"
try:
    from minio import Minio
    import urllib3
    _minio = Minio(
        os.environ.get("STORAGE_ENDPOINT", "localhost:9000"),
        access_key=os.environ.get("STORAGE_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("STORAGE_SECRET_KEY", "minioadmin"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=3, read=10)),
    )
    if not _minio.bucket_exists(BUCKET): _minio.make_bucket(BUCKET)
    MINIO_OK = True; log.info("MinIO connected ✓")
except Exception as e: log.warning(f"MinIO unavailable: {e}")

# ── DuckDuckGo ────────────────────────────────────────────────────────────────
SEARCH_OK = False
try:
    from duckduckgo_search import DDGS
    SEARCH_OK = True; log.info("DuckDuckGo ready ✓")
except ImportError: log.warning("duckduckgo_search not installed")

# ── Playwright scraper (shared collector — no hardcoded tokens) ───────────────
try:
    from app.utils.playwright_scraper import collect_match_data as _playwright_collect, get as _pget
    PLAYWRIGHT_OK = True
    log.info("playwright_scraper loaded ✓")
except ImportError as e:
    PLAYWRIGHT_OK = False
    log.warning(f"playwright_scraper not available: {e}")
    def _playwright_collect(mid): return {}
    def _pget(c, *keys): return {}


# ═══════════════════════════════════════════════════════════════════════════════
# VOICE CATALOGUE — all English edge-tts voices
# ═══════════════════════════════════════════════════════════════════════════════
EDGE_TTS_VOICES = [
    # British English — best for football commentary
    {"id":"en-GB-RyanNeural",    "name":"Ryan",       "gender":"male",   "locale":"en-GB","tag":"authoritative"},
    {"id":"en-GB-ThomasNeural",  "name":"Thomas",     "gender":"male",   "locale":"en-GB","tag":"calm"},
    {"id":"en-GB-OliverNeural",  "name":"Oliver",     "gender":"male",   "locale":"en-GB","tag":"natural"},
    {"id":"en-GB-ElliotNeural",  "name":"Elliot",     "gender":"male",   "locale":"en-GB","tag":"expressive"},
    {"id":"en-GB-SoniaNeural",   "name":"Sonia",      "gender":"female", "locale":"en-GB","tag":"warm"},
    {"id":"en-GB-LibbyNeural",   "name":"Libby",      "gender":"female", "locale":"en-GB","tag":"friendly"},
    {"id":"en-GB-MaisieNeural",  "name":"Maisie",     "gender":"female", "locale":"en-GB","tag":"expressive"},
    {"id":"en-GB-HollieNeural",  "name":"Hollie",     "gender":"female", "locale":"en-GB","tag":"natural"},
    {"id":"en-GB-BellaNeural",   "name":"Bella",      "gender":"female", "locale":"en-GB","tag":"expressive"},
    {"id":"en-GB-AbbiNeural",    "name":"Abbi",       "gender":"female", "locale":"en-GB","tag":"natural"},
    # American English
    {"id":"en-US-GuyNeural",         "name":"Guy",         "gender":"male",   "locale":"en-US","tag":"news"},
    {"id":"en-US-ChristopherNeural", "name":"Christopher", "gender":"male",   "locale":"en-US","tag":"authoritative"},
    {"id":"en-US-EricNeural",        "name":"Eric",        "gender":"male",   "locale":"en-US","tag":"casual"},
    {"id":"en-US-SteffanNeural",     "name":"Steffan",     "gender":"male",   "locale":"en-US","tag":"news"},
    {"id":"en-US-AndrewNeural",      "name":"Andrew",      "gender":"male",   "locale":"en-US","tag":"warm"},
    {"id":"en-US-BrianNeural",       "name":"Brian",       "gender":"male",   "locale":"en-US","tag":"casual"},
    {"id":"en-US-AriaNeural",        "name":"Aria",        "gender":"female", "locale":"en-US","tag":"expressive"},
    {"id":"en-US-JennyNeural",       "name":"Jenny",       "gender":"female", "locale":"en-US","tag":"friendly"},
    {"id":"en-US-MonicaNeural",      "name":"Monica",      "gender":"female", "locale":"en-US","tag":"calm"},
    {"id":"en-US-MichelleNeural",    "name":"Michelle",    "gender":"female", "locale":"en-US","tag":"warm"},
    {"id":"en-US-ElizabethNeural",   "name":"Elizabeth",   "gender":"female", "locale":"en-US","tag":"natural"},
    # Australian
    {"id":"en-AU-WilliamNeural", "name":"William", "gender":"male",   "locale":"en-AU","tag":"natural"},
    {"id":"en-AU-DarrenNeural",  "name":"Darren",  "gender":"male",   "locale":"en-AU","tag":"natural"},
    {"id":"en-AU-NatashaNeural", "name":"Natasha", "gender":"female", "locale":"en-AU","tag":"natural"},
    {"id":"en-AU-AnnetteNeural", "name":"Annette", "gender":"female", "locale":"en-AU","tag":"natural"},
    # African English
    {"id":"en-KE-ChilembaNeural","name":"Chilemba","gender":"male",   "locale":"en-KE","tag":"natural"},
    {"id":"en-KE-AsiliaNeural",  "name":"Asilia",  "gender":"female", "locale":"en-KE","tag":"natural"},
    {"id":"en-NG-AbeoNeural",    "name":"Abeo",    "gender":"male",   "locale":"en-NG","tag":"natural"},
    {"id":"en-NG-EzinneNeural",  "name":"Ezinne",  "gender":"female", "locale":"en-NG","tag":"natural"},
    {"id":"en-ZA-LukeNeural",    "name":"Luke",    "gender":"male",   "locale":"en-ZA","tag":"natural"},
    {"id":"en-ZA-LeahNeural",    "name":"Leah",    "gender":"female", "locale":"en-ZA","tag":"natural"},
    # Indian English
    {"id":"en-IN-PrabhatNeural", "name":"Prabhat", "gender":"male",   "locale":"en-IN","tag":"natural"},
    {"id":"en-IN-NeerjaNeural",  "name":"Neerja",  "gender":"female", "locale":"en-IN","tag":"natural"},
    # OpenAI voices
    {"id":"onyx",  "name":"Alex (OpenAI)", "gender":"male",   "locale":"en-US","tag":"openai-deep", "provider":"openai"},
    {"id":"nova",  "name":"Sarah (OpenAI)","gender":"female", "locale":"en-US","tag":"openai-warm", "provider":"openai"},
    {"id":"echo",  "name":"Echo (OpenAI)", "gender":"male",   "locale":"en-US","tag":"openai",      "provider":"openai"},
    {"id":"fable", "name":"Fable (OpenAI)","gender":"male",   "locale":"en-GB","tag":"openai",      "provider":"openai"},
    {"id":"alloy", "name":"Alloy (OpenAI)","gender":"neutral","locale":"en-US","tag":"openai",      "provider":"openai"},
]

DEFAULT_MALE   = "en-GB-RyanNeural"
DEFAULT_FEMALE = "en-GB-SoniaNeural"

# Scene voice assignments: (lead_gender, response_gender)
SCENE_LEADS = {
    "intro":     ("alex",  "sarah"),
    "lineups":   ("alex",  "sarah"),
    "h2h":       ("sarah", "alex"),
    "form":      ("alex",  "sarah"),
    "standings": ("sarah", "alex"),
    "scorers":   ("alex",  "sarah"),
    "managers":  ("alex",  "sarah"),
    "upcoming":  ("alex",  "sarah"),
    "closing":   ("sarah", None),
}


# ═══════════════════════════════════════════════════════════════════════════════
# TTS ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

async def _edge_async(text: str, voice: str) -> bytes:
    import edge_tts
    c = edge_tts.Communicate(text, voice)
    chunks = []
    async for chunk in c.stream():
        if chunk["type"] == "audio":
            chunks.append(chunk["data"])
    return b"".join(chunks)

def _tts_edge(text: str, voice: str) -> bytes | None:
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        data = loop.run_until_complete(_edge_async(text, voice))
        loop.close()
        log.info(f"edge-tts OK voice={voice} bytes={len(data)}")
        return data
    except Exception as e:
        log.error(f"edge-tts FAIL voice={voice}: {e}")
        return None

def _tts_openai(text: str, voice: str) -> bytes | None:
    try:
        r = _openai.audio.speech.create(model="tts-1", voice=voice, input=text, response_format="mp3")
        log.info(f"OpenAI TTS OK voice={voice}")
        return r.content
    except Exception as e:
        log.error(f"OpenAI TTS FAIL voice={voice}: {e}")
        return None

def _generate_mp3(text: str, voice: str) -> bytes | None:
    if not text or text.startswith("[Error"): return None
    is_openai = voice in ("onyx","nova","echo","alloy","fable","shimmer")
    if TTS_PROVIDER == "openai" or is_openai:
        data = _tts_openai(text, voice)
        if data: return data
        log.warning("OpenAI TTS failed, falling back to edge-tts")
        voice = DEFAULT_MALE
    return _tts_edge(text, voice)


# ── MinIO helpers ─────────────────────────────────────────────────────────────
def _mget(key: str) -> bytes | None:
    if not MINIO_OK: return None
    try:
        r = _minio.get_object(BUCKET, key); data = r.read(); r.close(); r.release_conn()
        log.debug(f"MinIO HIT: {key}"); return data
    except Exception as e:
        if hasattr(e, "code") and e.code == "NoSuchKey": return None
        log.warning(f"MinIO get {key}: {e}"); return None

def _mput(key: str, data: bytes) -> bool:
    if not MINIO_OK or not data: return False
    try:
        _minio.put_object(BUCKET, key, io.BytesIO(data), len(data), content_type="audio/mpeg")
        log.info(f"MinIO PUT: {key} ({len(data)} bytes)"); return True
    except Exception as e:
        log.error(f"MinIO put {key}: {e}"); return False

def _mdel(key: str) -> bool:
    if not MINIO_OK: return False
    try: _minio.remove_object(BUCKET, key); log.info(f"MinIO DEL: {key}"); return True
    except Exception as e: log.warning(f"MinIO del {key}: {e}"); return False

def _tts_cached(text: str, voice: str, cache_key: str) -> str | None:
    if not text or text.startswith("[Error"): return None
    path = f"commentary/{cache_key}.mp3"
    cached = _mget(path)
    if cached: log.info(f"TTS CACHE HIT: {path}"); return base64.b64encode(cached).decode()
    mp3 = _generate_mp3(text, voice)
    if not mp3: return None
    _mput(path, mp3)
    return base64.b64encode(mp3).decode()

def _audio_url(ckey: str) -> str:
    return f"/api/audio/commentary/{ckey}.mp3"


# ═══════════════════════════════════════════════════════════════════════════════
# SPORTRADAR HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _clean(raw: str) -> str:
    if not raw: return ""
    if "," in raw:
        p = raw.split(",", 1); return f"{p[1].strip()} {p[0].strip()}"
    return raw.strip()

def _gpms(comment: str) -> list:
    if not comment: return []
    out = []
    for m in re.finditer(r'\((\d+)(?:\+(\d+))?\.\)', comment):
        t = int(m.group(1)) + (int(m.group(2)) if m.group(2) else 0)
        if t <= 130: out.append(t)
    return out

def _top_sc(data: dict) -> list:
    out = []
    for e in (data.get("players") or [])[:5]:
        pl = e.get("player",{}); g = e.get("total",{}).get("goals",0)
        if g: out.append({"name": _clean(pl.get("name","")), "goals": g, "matches": e.get("total",{}).get("matches",0)})
    return out


# ── Web search ────────────────────────────────────────────────────────────────
def _search(q: str, n: int = 3) -> str:
    if not SEARCH_OK: return ""
    try:
        with DDGS() as d: results = list(d.text(q, max_results=n, timelimit="m")) or list(d.text(q, max_results=n))
        return "\n".join(f"• {r.get('title','')}: {r.get('body','')[:200]}" for r in results)
    except Exception as e: log.warning(f"Search '{q}': {e}"); return ""


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

BROADCAST_SYSTEM = """You are scripting a live TV sports broadcast.
ALEX: Lead male commentator — authoritative, dramatic, passionate, encyclopaedic.
SARAH: Female analyst — warm, statistically sharp, great storyteller.
Rules: natural broadcast dialogue, references previous scenes, no stage directions, pure speech."""

def _chat(prompt: str, history: list = None, system: str = None) -> str:
    msgs = []
    if system: msgs.append({"role":"system","content":system})
    if history: msgs.extend(history[-6:])
    msgs.append({"role":"user","content":prompt})
    try:
        r = _openai.chat.completions.create(model="gpt-4o-mini", messages=msgs, temperature=0.75, max_tokens=450)
        return r.choices[0].message.content.strip()
    except Exception as e:
        log.error(f"Chat: {e}"); return f"[Error: {e}]"

def _history(scenes: list) -> list:
    h = []
    for sc in scenes[-3:]:
        if sc.get("text"): h.append({"role":"assistant","content":f"[{sc['id'].upper()}] ALEX: {sc['text']}"})
        if sc.get("text_b"): h.append({"role":"assistant","content":f"[{sc['id'].upper()}] SARAH: {sc['text_b']}"})
    return h

def _enrich_player(name: str, team: str) -> dict:
    web  = _search(f"{name} {team} 2025 football stats goals")
    news = _search(f"{name} transfer 2025")
    prompt = f"""Stats for {name} ({team}). Web: {web} News: {news}
JSON only: {{"full_name":"","nationality":"","age":0,"appearances_this_season":0,"goals_this_season":0,
"assists_this_season":0,"career_goals":0,"market_value_eur":"","transfer_news":"","key_strength":"","fun_fact":"","club_history":""}}"""
    try:
        raw = re.sub(r"```(?:json)?","",_chat(prompt,system="Sports analyst. JSON only.")).strip(" `")
        return json.loads(raw)
    except: return {"full_name":name,"nationality":"Unknown","key_strength":"Skilled"}

def _enrich_manager(name: str, team: str) -> dict:
    web = _search(f"{name} manager {team} 2025 tactics formation")
    prompt = f"""Manager profile {name} ({team}). Web: {web}
JSON only: {{"full_name":"","nationality":"","age":0,"current_team":"{team}","tenure_years":0.0,
"win_rate_pct":0,"trophies":[],"preferred_formation":"","tactical_style":"","career_highlights":"","pre_match_quote":""}}"""
    try:
        raw = re.sub(r"```(?:json)?","",_chat(prompt,system="Sports analyst. JSON only.")).strip(" `")
        return json.loads(raw)
    except: return {"full_name":name,"current_team":team,"pre_match_quote":"We'll give everything tonight."}


# ── Scene generators ──────────────────────────────────────────────────────────
def _s_intro(ctx, hist):
    h,a = ctx["home"],ctx["away"]
    comp = f"{ctx.get('competition','')} {ctx.get('stage','')}".strip()
    cup  = "It's a cup tie — winner takes all!" if ctx.get("competition_type")=="cup" else ""
    alex = _chat(f"""ALEX opens the show. {h} vs {a} | {comp} | {ctx.get('venue','')} | {ctx.get('date','')}. {cup}
~35 words. Start "LADIES AND GENTLEMEN". End "...what a night this promises to be!" """, hist, BROADCAST_SYSTEM)
    sarah = _chat(f"""SARAH responds — ONE sharp insight about {h} vs {a} ~20 words. Continues: "{alex[:80]}..." """,
                  hist+[{"role":"assistant","content":f"ALEX: {alex}"}], BROADCAST_SYSTEM)
    return alex, sarah

def _s_h2h(ctx, hist):
    h,a = ctx["home"],ctx["away"]
    hw,dr,aw = ctx.get("home_wins",0),ctx.get("draws",0),ctx.get("away_wins",0)
    snip = " | ".join(f"{m['home']} {m['score_home']}-{m['score_away']} {m['away']} ({m.get('date','')})"
                      for m in (ctx.get("h2h_matches") or [])[:3]) or "No recent meetings"
    gt = ctx.get("goal_timing",{})
    tn = f"Most goals in {gt.get('most_dangerous_period','')} period." if gt.get("most_dangerous_period") else ""
    sarah = _chat(f"""SARAH presents H2H — picking up from intro.
{h} wins:{hw}, Draws:{dr}, {a} wins:{aw}. Recent: {snip}. {tn}
~40 words. Pure broadcast speech.""", hist, BROADCAST_SYSTEM)
    alex = _chat(f"""ALEX reacts ~20 words. What does history say for TONIGHT?
Continues: "{sarah[:80]}..." """, hist+[{"role":"assistant","content":f"SARAH: {sarah}"}], BROADCAST_SYSTEM)
    return sarah, alex

def _s_form(ctx, hist):
    h,a = ctx["home"],ctx["away"]
    hf=" ".join(ctx.get("home_form") or []) or "unknown"
    af=" ".join(ctx.get("away_form") or []) or "unknown"
    hs=ctx.get("home_top_scorer",""); as_=ctx.get("away_top_scorer","")
    sn=(f" {h}'s {hs} is red-hot." if hs else "")+(f" {a}'s {as_} leads their attack." if as_ else "")
    alex = _chat(f"""ALEX breaks down form — continuing naturally.
{h} last 5: {hf} ({ctx.get('home_gpg',0):.1f} goals/game) | {a} last 5: {af} ({ctx.get('away_gpg',0):.1f}). {sn}
~40 words. Build narrative tension.""", hist, BROADCAST_SYSTEM)
    sarah = _chat(f"""SARAH adds analysis ~25 words. Which momentum wins tonight?
Natural response: "{alex[:80]}..." """, hist+[{"role":"assistant","content":f"ALEX: {alex}"}], BROADCAST_SYSTEM)
    return alex, sarah

def _s_stage(ctx, hist):
    h,a = ctx["home"],ctx["away"]
    dist = ctx.get("distance_km"); dn = f"Only {dist}km separates these cities." if dist else ""
    cup  = "One team goes out — the stakes are everything." if ctx.get("competition_type")=="cup" else ""
    sarah = _chat(f"""SARAH sets the scene — continuing the broadcast.
{h} vs {a} in {ctx.get('competition','')} {ctx.get('stage','')}. {dn} {cup}
~40 words, passionate. Reference earlier conversation.""", hist, BROADCAST_SYSTEM)
    alex = _chat(f"""ALEX adds dramatic weight ~20 words. What does victory mean for each club?
Continues: "{sarah[:80]}..." """, hist+[{"role":"assistant","content":f"SARAH: {sarah}"}], BROADCAST_SYSTEM)
    return sarah, alex

def _s_scorers(ctx, hist):
    h,a = ctx["home"],ctx["away"]
    def _sc_str(sc, team):
        if not sc: return f"{team}: scorer data unavailable."
        return ", ".join(f"{s['name']} ({s['goals']} goals)" for s in sc[:3])
    alex = _chat(f"""ALEX spotlights the top scorers.
{h}: {_sc_str(ctx.get('home_top_scorers',[]),h)}
{a}: {_sc_str(ctx.get('away_top_scorers',[]),a)}
~40 words. Who is the biggest danger tonight?""", hist, BROADCAST_SYSTEM)
    sarah = _chat(f"""SARAH adds ONE tactical note about the goal threat ~20 words.
Continues: "{alex[:80]}..." """, hist+[{"role":"assistant","content":f"ALEX: {alex}"}], BROADCAST_SYSTEM)
    return alex, sarah

def _s_managers(ctx, hist):
    hm=ctx.get("home_manager",{}); am=ctx.get("away_manager",{})
    h,a = ctx["home"],ctx["away"]
    alex = _chat(f"""ALEX spotlights the managers — flowing from previous topics.
{hm.get('full_name',h+' coach')}: {hm.get('tactical_style','pressing')}, {hm.get('win_rate_pct','?')}% win rate, {hm.get('preferred_formation','?')}.
{am.get('full_name',a+' coach')}: {am.get('tactical_style','counter')}, {am.get('win_rate_pct','?')}% win rate, {am.get('preferred_formation','?')}.
~40 words. Tactical chess-match angle.""", hist, BROADCAST_SYSTEM)
    sarah = _chat(f"""SARAH names ONE decisive tactical edge tonight ~25 words.
Continues: "{alex[:80]}..." """, hist+[{"role":"assistant","content":f"ALEX: {alex}"}], BROADCAST_SYSTEM)
    return alex, sarah

def _s_player(player: dict, enriched: dict, is_home: bool) -> str:
    speaker = "ALEX" if is_home else "SARAH"
    name = player.get("name","")
    return _chat(f"""{speaker} spotlights {name} ({player.get('pos','')}, {enriched.get('nationality','?')}).
Season: {enriched.get('appearances_this_season','?')} apps, {enriched.get('goals_this_season','?')} goals.
Strength: {enriched.get('key_strength','technically gifted')}. {('Fun fact: '+enriched.get('fun_fact','')) if enriched.get('fun_fact') else ''}
~25 words, energetic spotlight.""", system=BROADCAST_SYSTEM)

def _s_lineups(ctx, hist):
    """Announces both starting XIs, reading player names."""
    h, a = ctx["home"], ctx["away"]
    h_names = ctx.get("home_players_list", "")
    a_names = ctx.get("away_players_list", "")
    h_form  = ctx.get("home_formation", "unknown formation")
    a_form  = ctx.get("away_formation", "unknown formation")

    if not h_names and not a_names:
        alex = _chat(f"""ALEX announces lineups for {h} vs {a}.
Lineups have not been officially confirmed yet — ~30 words, dramatic anticipation.""",
                     hist, BROADCAST_SYSTEM)
        sarah = _chat(f"""SARAH responds with tactical curiosity ~20 words.
Continues: "{alex[:80]}..." """,
                      hist+[{"role":"assistant","content":f"ALEX: {alex}"}], BROADCAST_SYSTEM)
        return alex, sarah

    alex = _chat(f"""ALEX announces the confirmed starting XIs.
{h} line up in a {h_form}: {h_names or 'squad to be confirmed'}.
{a} line up in a {a_form}: {a_names or 'squad to be confirmed'}.
~45 words. Read key player names naturally, commentator broadcast style.""",
                 hist, BROADCAST_SYSTEM)
    sarah = _chat(f"""SARAH highlights ONE key tactical match-up based on these lineups ~25 words.
Continues: "{alex[:80]}..." """,
                  hist+[{"role":"assistant","content":f"ALEX: {alex}"}], BROADCAST_SYSTEM)
    return alex, sarah


def _s_standings(ctx, hist):
    """Discusses current league/group stage standings for both teams."""
    h, a = ctx["home"], ctx["away"]
    h_pos  = ctx.get("home_standing")
    a_pos  = ctx.get("away_standing")
    comp   = ctx.get("competition", "the competition")
    is_cup = ctx.get("competition_type") == "cup"

    if is_cup or (not h_pos and not a_pos):
        # Cup / knockout — discuss what's at stake
        sarah = _chat(f"""SARAH discusses what is at stake in this {comp} {ctx.get('stage','match')}.
{h} vs {a} — knockout, one chance, everything on the line. ~35 words.""",
                      hist, BROADCAST_SYSTEM)
        alex = _chat(f"""ALEX adds the pressure angle ~20 words. Continues: "{sarah[:80]}..." """,
                     hist+[{"role":"assistant","content":f"SARAH: {sarah}"}], BROADCAST_SYSTEM)
        return sarah, alex

    h_str = f"{h} sit {_ordinal(h_pos['pos'])} in {comp} with {h_pos['pts']} points from {h_pos['played']} games" if h_pos else f"{h}'s league position"
    a_str = f"{a} are {_ordinal(a_pos['pos'])} with {a_pos['pts']} points" if a_pos else f"{a}'s position"
    sarah = _chat(f"""SARAH breaks down where both teams stand.
{h_str}. {a_str}. ~40 words. What does this result mean for their season?""",
                  hist, BROADCAST_SYSTEM)
    alex = _chat(f"""ALEX adds one pressure point ~20 words. Continues: "{sarah[:80]}..." """,
                 hist+[{"role":"assistant","content":f"SARAH: {sarah}"}], BROADCAST_SYSTEM)
    return sarah, alex


def _s_upcoming(ctx, hist):
    """Previews the next fixtures for both teams after this match."""
    h, a = ctx["home"], ctx["away"]
    h_fix = ctx.get("home_upcoming", [])
    a_fix = ctx.get("away_upcoming", [])

    def _fix_str(fixtures, team):
        if not fixtures:
            return f"{team}'s schedule to be confirmed"
        top = fixtures[:3]
        return " | ".join(f"vs {f['away'] if f.get('home','').lower().startswith(team[:4].lower()) else f['home']} ({f.get('date','')[:5]})" for f in top)

    alex = _chat(f"""ALEX looks ahead at what awaits both sides after this match.
{h} upcoming: {_fix_str(h_fix, h)}.
{a} upcoming: {_fix_str(a_fix, a)}.
~40 words. Context of fixture congestion / big games ahead.""",
                 hist, BROADCAST_SYSTEM)
    sarah = _chat(f"""SARAH adds one strategic implication ~20 words. Continues: "{alex[:80]}..." """,
                  hist+[{"role":"assistant","content":f"ALEX: {alex}"}], BROADCAST_SYSTEM)
    return alex, sarah


def _ordinal(n: int) -> str:
    if not n: return "?"
    s = ["th","st","nd","rd"]; v = n % 100
    return f"{n}{s[(v-20)%10] if (v-20)%10 < 4 else s[v] if v < 4 else s[0]}"


def _s_closing(ctx) -> str:
    h,a = ctx["home"],ctx["away"]
    return _chat(f"""SARAH closes the pre-match show — the natural finale.
{h} vs {a} — build excitement for kickoff (~20 words).
Then EXACTLY: "Before we kick off — a reminder: this is strictly for adults aged 18 and over. Gambling can become addictive. Please know your limits."
Then: "For live win probabilities tap the Insights tab in your app."
Then: "Enjoy the match!" — warm, sincere.""", system=BROADCAST_SYSTEM)


# ═══════════════════════════════════════════════════════════════════════════════
# MATCH CONTEXT FETCHER
# ═══════════════════════════════════════════════════════════════════════════════

def _fetch_ctx(betradar_id: str, hints: dict = None) -> dict:
    """
    Collect match context via Playwright scraper.
    `hints` is a dict of pre-known values from the frontend query params
    (home, away, competition, home_players, away_players, etc.) —
    used as fallbacks when the scraper hasn't collected a given endpoint yet.
    """
    hints = hints or {}
    log.info(f"[{betradar_id}] Playwright collecting match data for commentary...")
    raw = _playwright_collect(betradar_id)
    log.info(f"[{betradar_id}] Playwright done — {len(raw)} endpoints collected")

    # Core match info
    info = _pget(raw, f"match_info_statshub/{betradar_id}", f"match_info/{betradar_id}")
    md   = info.get("match", {}) if info else {}
    teams = md.get("teams", {})
    h_uid = str((teams.get("home") or {}).get("uid", ""))
    a_uid = str((teams.get("away") or {}).get("uid", ""))
    s_id  = str(md.get("_seasonid", ""))
    jerseys   = info.get("jerseys", {}) if info else {}
    home_color = f"#{(jerseys.get('home') or {}).get('player', {}).get('base', 'ea0000')}"
    away_color = f"#{(jerseys.get('away') or {}).get('player', {}).get('base', 'ffffff')}"

    # Use hints as fallbacks for team names / competition when info is empty
    home  = (teams.get("home") or {}).get("name") or hints.get("home") or "Home"
    away  = (teams.get("away") or {}).get("name") or hints.get("away") or "Away"
    tourn = info.get("tournament", {}) if info else {}
    comp  = tourn.get("name", "") or hints.get("competition", "")
    is_cup = str(tourn.get("seasontype",""))=="26" or any(k in comp.lower() for k in ["cup","champions","europa"])
    comp_type = "cup" if is_cup else ("friendly" if tourn.get("friendly") else "league")

    # Pull collected endpoints
    sq       = _pget(raw, f"match_squads/{betradar_id}")
    h2h_raw  = _pget(raw, f"stats_match_head2head/{betradar_id}")
    versus   = _pget(raw, f"stats_team_versusrecent/{h_uid}/{a_uid}") if h_uid and a_uid else {}
    form_raw = _pget(raw, f"stats_formtable/{s_id}") if s_id else {}
    hr       = _pget(raw, f"stats_team_lastx/{h_uid}/20", f"stats_team_lastx/{h_uid}/10", f"stats_team_lastx/{h_uid}/5") if h_uid else {}
    ar       = _pget(raw, f"stats_team_lastx/{a_uid}/20", f"stats_team_lastx/{a_uid}/10", f"stats_team_lastx/{a_uid}/5") if a_uid else {}
    h_sc_raw = _pget(raw, f"stats_season_topgoals/{s_id}/{h_uid}") if s_id and h_uid else {}
    a_sc_raw = _pget(raw, f"stats_season_topgoals/{s_id}/{a_uid}") if s_id and a_uid else {}

    # Standings — find both teams' rows
    table_raw = _pget(raw, f"season_dynamictable/{s_id}", f"stats_season_tables/{s_id}/1") if s_id else {}
    h_standing = a_standing = None
    for t in (table_raw.get("tables") or (table_raw.get("season") or {}).get("tables") or []):
        for row in t.get("tablerows", []):
            uid = str((row.get("team") or {}).get("uid", ""))
            entry = {"pos": row.get("pos"), "pts": row.get("pointsTotal", 0), "played": row.get("total", 0), "gd": row.get("goalDiffTotal", 0)}
            if uid == h_uid: h_standing = entry
            if uid == a_uid: a_standing = entry
        break

    # Upcoming fixtures
    h_upcoming_raw = _pget(raw, f"stats_team_fixtures/{h_uid}/10", f"stats_team_fixtures/{h_uid}/5") if h_uid else {}
    a_upcoming_raw = _pget(raw, f"stats_team_fixtures/{a_uid}/10", f"stats_team_fixtures/{a_uid}/5") if a_uid else {}
    def _parse_upcoming(data):
        return [{"home": (m.get("teams",{}).get("home") or {}).get("name",""), "away": (m.get("teams",{}).get("away") or {}).get("name",""), "date": (m.get("time") or m.get("_dt") or {}).get("date","")} for m in (data.get("matches") or [])[:3]]

    # H2H
    h2h_list=[]; hw=dr=aw=0
    for m in (h2h_raw.get("matches") or [])[:10]:
        sh=m.get("result",{}).get("home",0); sa=m.get("result",{}).get("away",0)
        h2h_list.append({"date":(m.get("_dt") or {}).get("date",""),
            "home":(m.get("teams") or {}).get("home",{}).get("name",""),
            "away":(m.get("teams") or {}).get("away",{}).get("name",""),
            "score_home":sh,"score_away":sa})
        if sh>sa: hw+=1
        elif sh==sa: dr+=1
        else: aw+=1

    # Goal timing from versusrecent
    all_mins=[]
    for m in (versus.get("matches") or [])[:20]:
        all_mins.extend(_gpms(m.get("comment","")))
    most_danger=None
    if all_mins:
        b={"0-15":0,"16-30":0,"31-45":0,"46-60":0,"61-75":0,"76-90":0,"90+":0}
        for mi in all_mins:
            if   mi<=15: b["0-15"]+=1
            elif mi<=30: b["16-30"]+=1
            elif mi<=45: b["31-45"]+=1
            elif mi<=60: b["46-60"]+=1
            elif mi<=75: b["61-75"]+=1
            elif mi<=90: b["76-90"]+=1
            else: b["90+"]+=1
        most_danger=max(b,key=b.get)

    # Form
    hf=[]; af=[]
    for t in (form_raw.get("teams") or []):
        uid=str((t.get("team") or {}).get("uid",""))
        fl=[f.get("value") for f in (t.get("form") or {}).get("total",[])][:5]
        if uid==h_uid: hf=fl
        if uid==a_uid: af=fl

    def _gpg(r):
        ms=(r.get("matches") or [])[:5]
        return round(sum((m.get("result",{}).get("home",0) or 0)+(m.get("result",{}).get("away",0) or 0) for m in ms)/max(len(ms),1),1)

    # Squads
    def _pp(node):
        if isinstance(node,dict): return node.get("players") or []
        return node if isinstance(node,list) else []
    h_sq=sq.get("home",{}); a_sq=sq.get("away",{})
    h_pl=_pp(h_sq.get("startinglineup",h_sq.get("players",[])))
    a_pl=_pp(a_sq.get("startinglineup",a_sq.get("players",[])))
    h_mgr_n=_clean((h_sq.get("coach") or {}).get("name","")) or _clean(((info or {}).get("manager") or {}).get("home",{}).get("name","")) or f"{home} Manager"
    a_mgr_n=_clean((a_sq.get("coach") or {}).get("name","")) or _clean(((info or {}).get("manager") or {}).get("away",{}).get("name","")) or f"{away} Manager"

    h_scorers=_top_sc(h_sc_raw); a_scorers=_top_sc(a_sc_raw)

    return {
        "home":home,"away":away,"home_color":home_color,"away_color":away_color,
        "venue":(md.get("venue") or {}).get("name","TBA"),
        "date":(md.get("_dt") or {}).get("date","TBA"),
        "home_uid":h_uid,"away_uid":a_uid,"season_id":s_id,
        "competition":tourn.get("name",""),"competition_type":comp_type,
        "stage":str(md.get("round","")),
        "distance_km":md.get("distance"),
        "h2h_matches":h2h_list,"home_wins":hw,"draws":dr,"away_wins":aw,
        "home_form":hf,"away_form":af,"home_gpg":_gpg(hr),"away_gpg":_gpg(ar),
        "goal_timing":{"avg_minute":round(sum(all_mins)/len(all_mins),1) if all_mins else None,
                       "most_dangerous_period":most_danger,
                       "first_half_pct":round(sum(1 for m in all_mins if m<=45)/max(len(all_mins),1)*100,1)},
        "home_top_scorers":h_scorers,"away_top_scorers":a_scorers,
        "home_top_scorer":h_scorers[0]["name"] if h_scorers else "",
        "away_top_scorer":a_scorers[0]["name"] if a_scorers else "",
        "h_pl":h_pl,"a_pl":a_pl,"h_mgr_name":h_mgr_n,"a_mgr_name":a_mgr_n,
        # New fields for new scenes
        "home_formation": (h_sq.get("formation") or ""),
        "away_formation": (a_sq.get("formation") or ""),
        "home_players_list": hints.get("home_players") or ", ".join((p.get("playername", p.get("name","")).split(",")[0].strip() for p in h_pl[:11] if p.get("playername") or p.get("name"))),
        "away_players_list": hints.get("away_players") or ", ".join((p.get("playername", p.get("name","")).split(",")[0].strip() for p in a_pl[:11] if p.get("playername") or p.get("name"))),
        "home_standing": h_standing,
        "away_standing": a_standing,
        "home_upcoming": _parse_upcoming(h_upcoming_raw),
        "away_upcoming": _parse_upcoming(a_upcoming_raw),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# CORE BUILD FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════

def _build(betradar_id: str, ctx: dict,
           voice_alex: str = DEFAULT_MALE, voice_sarah: str = DEFAULT_FEMALE,
           tts_on: bool = True, progress_cb=None, max_pl: int = 3) -> dict:

    home=ctx["home"]; away=ctx["away"]
    def _p(step, msg):
        log.info(f"[{betradar_id}] {step}: {msg}")
        if progress_cb: progress_cb(step, msg)

    _p("enrichment","Enriching managers + players…")
    pl_raw = ctx["h_pl"][:max_pl] + ctx["a_pl"][:max(1,max_pl-1)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_hm = pool.submit(_enrich_manager, ctx["h_mgr_name"], home)
        f_am = pool.submit(_enrich_manager, ctx["a_mgr_name"], away)
        pf   = {pool.submit(_enrich_player,
                             p.get("playername",p.get("name","")).split(",")[0].strip(),
                             home if p in ctx["h_pl"] else away): i
                for i, p in enumerate(pl_raw)}

    mgr_h=f_hm.result(); mgr_a=f_am.result()
    ctx["home_manager"]=mgr_h; ctx["away_manager"]=mgr_a
    enriched=[{}]*len(pl_raw)
    for fut,idx in pf.items():
        try: enriched[idx]=fut.result()
        except Exception as e: log.error(f"Pl enrichment[{idx}]: {e}")

    players_base=[]
    for i,p in enumerate(pl_raw):
        raw=p.get("playername",p.get("name",""))
        nm=raw.split(",")[0].strip() if "," in raw else raw.strip()
        players_base.append({"name":nm,"number":p.get("shirtnumber",""),
            "pos":p.get("matchpos","M"),"team":home if p in ctx["h_pl"] else away,
            "color":ctx["home_color"] if p in ctx["h_pl"] else ctx["away_color"]})

    _p("dialogue","Writing scenes…")
    scenes_text: list[dict] = []

    def _make(sid, dur, fn):
        hist=_history(scenes_text); ta,tb=fn(ctx,hist)
        s={"id":sid,"duration":dur,"timing":0,"text":ta,"text_b":tb,
           "audio_a":None,"audio_b":None,"audio_url_a":None,"audio_url_b":None}
        scenes_text.append(s); _p("dialogue",f"Scene '{sid}' written"); return s

    scene_defs=[
        ("intro",     8,  _s_intro),
        ("lineups",  11,  _s_lineups),
        ("h2h",      10,  _s_h2h),
        ("form",      9,  _s_form),
        ("scorers",   9,  _s_scorers),
        ("standings", 9,  _s_standings),
        ("managers",  9,  _s_managers),
        ("upcoming",  7,  _s_upcoming),
    ]
    scenes=[_make(sid,dur,fn) for sid,dur,fn in scene_defs]

    pl_texts=[]
    for i,(p,e) in enumerate(zip(players_base[:6],enriched[:6])):
        is_home = p["team"]==home
        voice_lead = voice_alex if is_home else voice_sarah
        txt=_s_player(p,e,is_home)
        pl_texts.append((txt,voice_lead,p,e))

    pl_dur=max(len(pl_raw)*4+10,35)
    players_scene={"id":"players","duration":int(pl_dur),"timing":0,
        "text":"","text_b":"","audio_a":None,"audio_b":None,
        "players":[{**p,**e,"commentary":txt,"voice":v,"audio_b64":None,"audio_url":None}
                   for txt,v,p,e in pl_texts]+
                  [{**p,**(enriched[i] if i<len(enriched) else {}),"commentary":"",
                    "voice":voice_alex,"audio_b64":None,"audio_url":None}
                   for i,p in enumerate(players_base[len(pl_texts):])]}
    scenes.append(players_scene)

    closing=_s_closing(ctx)
    scenes.append({"id":"closing","duration":7,"timing":0,"text":closing,"text_b":"",
                   "audio_a":None,"audio_b":None,"audio_url_a":None,"audio_url_b":None})

    t=0
    for sc in scenes: sc["timing"]=t; t+=sc["duration"]

    if tts_on:
        _p("tts","Generating audio…")
        mk = betradar_id
        for sc in scenes:
            if sc["id"]=="players":
                for j,p in enumerate(sc.get("players",[])):
                    if not p.get("commentary"): continue
                    ckey=f"{mk}/player_{j}_{p.get('name','x').replace(' ','_')[:18]}"
                    b64=_tts_cached(p["commentary"],p.get("voice",voice_alex),ckey)
                    p["audio_b64"]=b64
                    if b64: p["audio_url"]=_audio_url(ckey)
                _p("tts","Players done")
            else:
                la,lb=SCENE_LEADS.get(sc["id"],("alex","sarah"))
                va=voice_alex if la=="alex" else voice_sarah
                vb=(voice_sarah if lb=="sarah" else voice_alex) if lb else None
                if sc.get("text"):
                    ca=f"{mk}/scene_{sc['id']}_a"
                    b=_tts_cached(sc["text"],va,ca)
                    sc["audio_a"]=b; sc["audio_url_a"]=_audio_url(ca) if b else None
                if sc.get("text_b") and vb:
                    cb=f"{mk}/scene_{sc['id']}_b"
                    b=_tts_cached(sc["text_b"],vb,cb)
                    sc["audio_b"]=b; sc["audio_url_b"]=_audio_url(cb) if b else None
                _p("tts",f"Scene '{sc['id']}' audio done")

    gw="Gambling is strictly for persons aged 18 and over. Please gamble responsibly. Help: gamblingtherapy.org"
    gw_ck=f"{betradar_id}/gambling_warning"
    gw_b=_tts_cached(gw,voice_sarah,gw_ck) if tts_on else None

    return {
        "match":{"home":home,"away":away,"home_color":ctx["home_color"],"away_color":ctx["away_color"],
                 "venue":ctx["venue"],"date":ctx["date"],"competition":ctx.get("competition",""),
                 "competition_type":ctx.get("competition_type","league"),"distance_km":ctx.get("distance_km"),
                 "h2h":{"total":len(ctx["h2h_matches"]),"home_wins":ctx["home_wins"],
                        "draws":ctx["draws"],"away_wins":ctx["away_wins"],"matches":ctx["h2h_matches"][:5]},
                 "form":{"home":ctx["home_form"],"away":ctx["away_form"]},
                 "goal_timing":ctx.get("goal_timing",{}),
                 "top_scorers":{"home":ctx.get("home_top_scorers",[]),"away":ctx.get("away_top_scorers",[])}},
        "home_manager":mgr_h,"away_manager":mgr_a,
        "enriched_players":players_scene.get("players",[]),
        "scenes":scenes,"total_duration":t,
        "voice_alex":voice_alex,"voice_sarah":voice_sarah,
        "gambling_warning":{"text":gw,"audio_b64":gw_b,"audio_url":_audio_url(gw_ck) if gw_b else None},
    }


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

@bp_commentary.route("/voices")
def list_voices():
    prov = request.args.get("provider","all")
    voices = EDGE_TTS_VOICES
    if prov=="edge": voices=[v for v in voices if v.get("provider")!="openai"]
    elif prov=="openai": voices=[v for v in voices if v.get("provider")=="openai"]
    return jsonify({"voices":voices,"default_male":DEFAULT_MALE,"default_female":DEFAULT_FEMALE})


@bp_commentary.route("/odds/match/<betradar_id>/commentary", methods=["GET"])
def get_commentary(betradar_id: str):
    t0 = time.time()
    va     = request.args.get("voice_alex",   DEFAULT_MALE)
    vs     = request.args.get("voice_sarah",  DEFAULT_FEMALE)
    tts_on = request.args.get("tts", "true").lower() != "false"

    # Hints passed from the CinematicMatchPlayer frontend (real team/player data)
    hints = {
        "home":         request.args.get("home", ""),
        "away":         request.args.get("away", ""),
        "competition":  request.args.get("competition", ""),
        "stage":        request.args.get("stage", ""),
        "venue":        request.args.get("venue", ""),
        "home_players": request.args.get("home_players", ""),
        "away_players": request.args.get("away_players", ""),
    }

    log.info(f"Commentary: {betradar_id} va={va} vs={vs} hints_home={hints['home']} hints_away={hints['away']}")
    ctx    = _fetch_ctx(betradar_id, hints=hints)
    max_pl = int(os.environ.get("MAX_PLAYERS", "3"))
    result = _build(betradar_id, ctx, va, vs, tts_on, max_pl=max_pl)
    log.info(f"Commentary done {time.time()-t0:.1f}s for {ctx['home']} vs {ctx['away']}")
    return jsonify(result)


@bp_commentary.route("/odds/match/<betradar_id>/commentary/stream")
def stream_commentary(betradar_id: str):
    """SSE stream — emits progress events during generation."""
    va = request.args.get("voice_alex",  DEFAULT_MALE)
    vs = request.args.get("voice_sarah", DEFAULT_FEMALE)
    tts_on = request.args.get("tts","true").lower()!="false"

    def gen():
        def _sse(event, data): return f"event: {event}\ndata: {json.dumps(data,default=str)}\n\n"
        progress_q = []
        def _cb(step, msg): progress_q.append((step, msg))

        yield _sse("status",{"step":"init","message":"Fetching match data…"})
        try:
            ctx = _fetch_ctx(betradar_id)
            yield _sse("match_info",{"home":ctx["home"],"away":ctx["away"],"competition":ctx.get("competition","")})

            result_holder={}; error_holder={}
            def _run():
                try:
                    result_holder["data"]=_build(betradar_id,ctx,va,vs,tts_on,progress_cb=_cb,
                                                  max_pl=int(os.environ.get("MAX_PLAYERS","3")))
                except Exception as e: error_holder["err"]=str(e)

            t=threading.Thread(target=_run,daemon=True); t.start()
            import time as _t
            while t.is_alive():
                while progress_q: step,msg=progress_q.pop(0); yield _sse("status",{"step":step,"message":msg})
                _t.sleep(0.3)
            while progress_q: step,msg=progress_q.pop(0); yield _sse("status",{"step":step,"message":msg})

            if error_holder.get("err"):
                yield _sse("error",{"message":error_holder["err"]}); return

            data=result_holder.get("data",{})
            for sc in data.get("scenes",[]):
                if sc["id"]=="players": continue
                yield _sse("scene",{"id":sc["id"],"text":sc.get("text",""),"text_b":sc.get("text_b",""),
                    "audio_url_a":sc.get("audio_url_a"),"audio_url_b":sc.get("audio_url_b"),
                    "has_audio":bool(sc.get("audio_a") or sc.get("audio_url_a"))})
            yield _sse("done",{"total_duration":data.get("total_duration",0),"voice_alex":va,"voice_sarah":vs})
        except Exception as e:
            log.error(f"Stream commentary: {e}"); yield _sse("error",{"message":str(e)})

    return Response(stream_with_context(gen()),mimetype="text/event-stream",
        headers={"Cache-Control":"no-cache","Connection":"keep-alive","X-Accel-Buffering":"no"})


@bp_commentary.route("/odds/match/<betradar_id>/scene/<scene_id>/audio", methods=["POST"])
def scene_audio(betradar_id: str, scene_id: str):
    """Generate/regenerate audio for one scene. Body: text_a, text_b, voice_a, voice_b, force."""
    body=request.json or {}
    va=body.get("voice_a",DEFAULT_MALE); vb=body.get("voice_b",DEFAULT_FEMALE)
    force=body.get("force",False); out={}
    if body.get("text_a"):
        ck=f"{betradar_id}/scene_{scene_id}_a"
        if force: _mdel(f"commentary/{ck}.mp3")
        b=_tts_cached(body["text_a"],va,ck)
        out["audio_a"]=b; out["audio_url_a"]=_audio_url(ck) if b else None
    if body.get("text_b"):
        ck=f"{betradar_id}/scene_{scene_id}_b"
        if force: _mdel(f"commentary/{ck}.mp3")
        b=_tts_cached(body["text_b"],vb,ck)
        out["audio_b"]=b; out["audio_url_b"]=_audio_url(ck) if b else None
    return jsonify({"status":"ok","scene":scene_id,**out})


@bp_commentary.route("/odds/match/<betradar_id>/scene/<scene_id>/regenerate", methods=["POST"])
def regen_scene(betradar_id: str, scene_id: str):
    """Delete cached audio for scene + regenerate with new text/voice."""
    body=request.json or {}
    va=body.get("voice_a",DEFAULT_MALE); vb=body.get("voice_b",DEFAULT_FEMALE)
    _mdel(f"commentary/{betradar_id}/scene_{scene_id}_a.mp3")
    _mdel(f"commentary/{betradar_id}/scene_{scene_id}_b.mp3")
    out={}
    if body.get("text_a"):
        ck=f"{betradar_id}/scene_{scene_id}_a"
        b=_tts_cached(body["text_a"],va,ck)
        out["audio_a"]=b; out["audio_url_a"]=_audio_url(ck) if b else None
    if body.get("text_b"):
        ck=f"{betradar_id}/scene_{scene_id}_b"
        b=_tts_cached(body["text_b"],vb,ck)
        out["audio_b"]=b; out["audio_url_b"]=_audio_url(ck) if b else None
    return jsonify({"status":"ok","scene":scene_id,**out})


@bp_commentary.route("/odds/match/<betradar_id>/player/<int:player_idx>/audio", methods=["POST"])
def player_audio(betradar_id: str, player_idx: int):
    """Generate audio for player spotlight. Body: text, voice, name."""
    body=request.json or {}; text=body.get("text",""); voice=body.get("voice",DEFAULT_MALE)
    name=body.get("name",f"player_{player_idx}").replace(" ","_")[:18]
    if not text: return jsonify({"error":"text required"}),400
    ck=f"{betradar_id}/player_{player_idx}_{name}"
    _mdel(f"commentary/{ck}.mp3")
    b=_tts_cached(text,voice,ck)
    return jsonify({"status":"ok","audio_b64":b,"audio_url":_audio_url(ck) if b else None})


@bp_commentary.route("/audio/<path:key>")
def serve_audio(key: str):
    data=_mget(f"commentary/{key}")
    if not data: return jsonify({"error":"Not found","key":key}),404
    return Response(data,mimetype="audio/mpeg",headers={
        "Content-Length":str(len(data)),"Cache-Control":"public, max-age=3600","Accept-Ranges":"bytes"})


@bp_commentary.route("/audio/status")
def audio_status():
    return jsonify({"minio_ok":MINIO_OK,"search_ok":SEARCH_OK,"tts_provider":TTS_PROVIDER,
        "openai_key":bool(os.environ.get("OPENAI_API_KEY")),"bucket":BUCKET if MINIO_OK else None,
        "default_male":DEFAULT_MALE,"default_female":DEFAULT_FEMALE,
        "total_voices":len(EDGE_TTS_VOICES)})


@bp_commentary.route("/odds/match/<betradar_id>/commentary/delete", methods=["DELETE"])
def delete_commentary(betradar_id: str):
    if not MINIO_OK: return jsonify({"error":"MinIO not available"}),503
    prefix=f"commentary/{betradar_id}/"; deleted=0
    try:
        from minio.deleteobjects import DeleteObject
        objs=list(_minio.list_objects(BUCKET,prefix=prefix,recursive=True))
        if objs:
            errs=list(_minio.remove_objects(BUCKET,[DeleteObject(o.object_name) for o in objs]))
            deleted=len(objs)-len(errs)
    except Exception as e: return jsonify({"error":str(e)}),500
    return jsonify({"status":"ok","deleted":deleted,"prefix":prefix})