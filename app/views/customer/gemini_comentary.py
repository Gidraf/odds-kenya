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
        os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=3, read=10)),
    )
    if not _minio.bucket_exists(BUCKET): _minio.make_bucket(BUCKET)
    MINIO_OK = True; log.info("MinIO connected ✓")
except Exception as e: log.warning(f"MinIO unavailable: {e}")

# ── MinIO ─────────────────────────────────────────────────────────────────────
# MinIO removed — commentary uses SQLite for caching

# ── Local filesystem audio cache — always available, primary fallback when MinIO is down
_LOCAL_CACHE: str = os.path.join(
    os.environ.get("AUDIO_CACHE_DIR", "/tmp/kinetic_audio"), "commentary"
)
os.makedirs(_LOCAL_CACHE, exist_ok=True)
log.info(f"Local audio cache: {_LOCAL_CACHE}")


def _local_path(minio_key: str) -> str:
    """Map a MinIO key to its local mirror path, e.g.
    'commentary/12345/scene_intro_a.mp3' -> '/tmp/kinetic_audio/commentary/12345/scene_intro_a.mp3'
    """
    rel = minio_key[len("commentary/"):] if minio_key.startswith("commentary/") else minio_key
    full = os.path.join(_LOCAL_CACHE, rel)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    return full


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

# Best sports voices:
# en-US-GuyNeural   → supports "sports-commentary-excited" SSML style — top pick for Alex
# en-US-AriaNeural  → supports "excited" style — top pick for Sarah
# en-GB-RyanNeural  → no style support but sounds authoritative at +40% rate
DEFAULT_MALE   = "en-US-GuyNeural"
DEFAULT_FEMALE = "en-US-AriaNeural"

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
# SQLITE CACHE — persist commentary to avoid regenerating for same match
# ═══════════════════════════════════════════════════════════════════════════════
import sqlite3, time as _time

_DB_PATH = os.environ.get("COMMENTARY_DB", "kinetic_commentary.db")

def _db():
    conn = sqlite3.connect(_DB_PATH)
    conn.execute("""CREATE TABLE IF NOT EXISTS commentary_cache (
        match_id  TEXT PRIMARY KEY,
        data      TEXT NOT NULL,
        created   REAL NOT NULL,
        home      TEXT,
        away      TEXT
    )""")
    conn.commit()
    return conn

def _cache_get(match_id: str) -> dict | None:
    try:
        conn = _db()
        row = conn.execute("SELECT data, created FROM commentary_cache WHERE match_id=?", (match_id,)).fetchone()
        conn.close()
        if not row: return None
        age_h = (_time.time() - row[1]) / 3600
        if age_h > 24: return None   # expire after 24 h
        return json.loads(row[0])
    except Exception as e:
        log.warning(f"Cache get {match_id}: {e}")
        return None

def _cache_put(match_id: str, data: dict, home="", away=""):
    try:
        conn = _db()
        conn.execute(
            "INSERT OR REPLACE INTO commentary_cache VALUES (?,?,?,?,?)",
            (match_id, json.dumps(data, default=str), _time.time(), home, away)
        )
        conn.commit(); conn.close()
        log.info(f"Commentary cached: {match_id}")
    except Exception as e:
        log.warning(f"Cache put {match_id}: {e}")

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

BROADCAST_SYSTEM = """You are scripting a LIVE, HIGH-ENERGY televised sports broadcast.
The delivery will be TEXT-TO-SPEECH at fast rate — write for SPOKEN performance, not reading.

ALEX: Lead commentator. Booming voice. Dramatic. Short punchy sentences. Stadium roar energy.
SARAH: Analyst. Sharp. Witty. Challenges with data. Warm but electric.

STRICT RULES:
1. NO name prefix ever — never write "ALEX:" or "SARAH:" anywhere.
2. NEVER repeat what the other speaker said. Each line = fresh information or angle.
3. SHORT sentences. Punchy. Like this. No sub-clauses. Spoken rhythm only.
4. Maximum 35 words per turn. Quality over length — TTS sounds better short.
5. Use exclamations naturally. Rhetorical questions. Build urgency.
6. Real football language: "clinical finish", "high press", "battle in the middle third".
7. No stage directions, no bullet points, pure spoken words only."""

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

def _sp(text: str) -> str:
    """Strip any speaker prefix (ALEX:, SARAH:, Alex:, Sarah:) the model may have added."""
    return re.sub(r"^(ALEX|SARAH|Alex|Sarah)\s*:\s*", "", text.strip())


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
# Rules enforced here:
# - Every _chat return is wrapped in _sp() to strip any "ALEX:"/"SARAH:" prefixes
# - The second speaker always gets a DIFFERENT angle prompt — never "Continues: ..."
# - Web search used to verify factual claims before writing them into scripts

def _s_intro(ctx, hist):
    h, a = ctx["home"], ctx["away"]
    comp  = f"{ctx.get('competition', '')} {ctx.get('stage', '')}".strip()
    venue = ctx.get("venue", "")
    date  = ctx.get("date", "")
    cup   = "Winner takes all — this is knockout football." if ctx.get("competition_type") == "cup" else ""
    # Web search for any breaking team news
    news  = _search(f"{h} vs {a} {comp} preview team news 2025") if SEARCH_OK else ""

    alex = _sp(_chat(
        f"""Open the broadcast. {h} vs {a} | {comp} | {venue} | {date}. {cup}
Latest context: {news[:200] if news else 'N/A'}
Start: "Ladies and gentlemen..." End with genuine excitement. ~35 words.""",
        hist, BROADCAST_SYSTEM))

    sarah = _sp(_chat(
        f"""Alex just opened the show for {h} vs {a}. Your turn.
Give ONE stat or narrative angle Alex has NOT mentioned. ~20 words.
Do NOT repeat anything Alex said.""",
        hist + [{"role": "assistant", "content": alex}], BROADCAST_SYSTEM))
    return alex, sarah


def _s_h2h(ctx, hist):
    h, a    = ctx["home"], ctx["away"]
    hw, dr, aw = ctx.get("home_wins", 0), ctx.get("draws", 0), ctx.get("away_wins", 0)
    matches = ctx.get("h2h_matches") or []
    snip    = " | ".join(
        f"{m['home']} {m['score_home']}-{m['score_away']} {m['away']} ({m.get('date', '')})"
        for m in matches[:4]) or "No recent meetings recorded"
    gt  = ctx.get("goal_timing", {})
    dn  = f"Most goals historically come in the {gt.get('most_dangerous_period', '')} period." if gt.get("most_dangerous_period") else ""
    # Verify H2H facts with a search
    web = _search(f"{h} vs {a} head to head record all time") if SEARCH_OK else ""

    sarah = _sp(_chat(
        f"""Present the head-to-head record.
Record: {h} {hw} wins, {dr} draws, {a} {aw} wins.
Last meetings: {snip}.
{dn}
Extra context from web: {web[:250] if web else 'N/A'}
~40 words. Mention one surprising detail from the history.""",
        hist, BROADCAST_SYSTEM))

    alex = _sp(_chat(
        f"""Sarah just presented the H2H record ({h} {hw}W / {dr}D / {a} {aw}W).
Add a DIFFERENT angle: what psychological edge or momentum factor does tonight carry? ~20 words.
Do NOT recap the numbers Sarah already gave.""",
        hist + [{"role": "assistant", "content": sarah}], BROADCAST_SYSTEM))
    return sarah, alex


def _s_form(ctx, hist):
    h, a  = ctx["home"], ctx["away"]
    hf    = " ".join(ctx.get("home_form") or []) or "form unknown"
    af    = " ".join(ctx.get("away_form") or []) or "form unknown"
    hgpg  = ctx.get("home_gpg", 0)
    agpg  = ctx.get("away_gpg", 0)
    h_top = ctx.get("home_top_scorer", "")
    a_top = ctx.get("away_top_scorer", "")
    # Verify form with web search
    web   = _search(f"{h} {a} recent results form 2025") if SEARCH_OK else ""

    alex = _sp(_chat(
        f"""{h} last 5 results: {hf} ({hgpg:.1f} goals/game).
{a} last 5 results: {af} ({agpg:.1f} goals/game).
{h_top and f"{h}'s top scorer is {h_top}." or ""}
Web context: {web[:200] if web else "N/A"}
~40 words. Tell the STORY of both teams' form — build tension toward tonight.""",
        hist, BROADCAST_SYSTEM))

    # Sarah gets the OPPOSITE angle — the underdog's chance or the danger in the stats
    underdog = a if "L" * 3 in af else h if "L" * 3 in hf else ""
    sarah = _sp(_chat(
        f"""Alex described the form stats. Your angle: {f"How can {underdog} overcome their slump?" if underdog else "What tactical shift could change the narrative tonight?"}
~25 words. Bring a FRESH insight — not the win/loss numbers Alex already covered.""",
        hist + [{"role": "assistant", "content": alex}], BROADCAST_SYSTEM))
    return alex, sarah


def _s_scorers(ctx, hist):
    h, a = ctx["home"], ctx["away"]
    def _sc_str(sc, team):
        if not sc: return f"no scorer data for {team}"
        return ", ".join(f"{s['name']} ({s['goals']} goals in {s.get('matches', '?')} games)" for s in sc[:3])
    h_str = _sc_str(ctx.get("home_top_scorers", []), h)
    a_str = _sc_str(ctx.get("away_top_scorers", []), a)
    # Search for tonight's goal-scorer tips
    web   = _search(f"{h} {a} predicted goalscorer anytime goal tip tonight") if SEARCH_OK else ""

    alex = _sp(_chat(
        f"""Name the biggest goal threats.
{h}: {h_str}.
{a}: {a_str}.
Web tips: {web[:200] if web else "N/A"}
~40 words. Pick ONE player from each side and explain WHY they're dangerous tonight.""",
        hist, BROADCAST_SYSTEM))

    # Sarah brings the defender's perspective or an underrated threat
    sarah = _sp(_chat(
        f"""Alex highlighted the obvious threats. Bring the counter-argument:
Which defender could shut them down, OR which under-the-radar player could be the surprise scorer? ~25 words.
Do NOT name the same players Alex mentioned.""",
        hist + [{"role": "assistant", "content": alex}], BROADCAST_SYSTEM))
    return alex, sarah


def _s_managers(ctx, hist):
    hm = ctx.get("home_manager", {})
    am = ctx.get("away_manager", {})
    h, a = ctx["home"], ctx["away"]
    hm_name = hm.get("full_name", f"{h} coach")
    am_name = am.get("full_name", f"{a} coach")
    # Search for pre-match manager quotes
    web = _search(f'{hm_name} pre-match press conference {h} vs {a} 2025') if SEARCH_OK else ""

    alex = _sp(_chat(
        f"""Set up the tactical battle between the managers.
{hm_name} ({h}): {hm.get('tactical_style', 'organized')} system, {hm.get('preferred_formation', 'flexible')}, {hm.get('win_rate_pct', '?')}% win rate.
{am_name} ({a}): {am.get('tactical_style', 'counter-attack')}, {am.get('preferred_formation', 'flexible')}, {am.get('win_rate_pct', '?')}% win rate.
Pre-match context: {web[:200] if web else "N/A"}
~40 words. Frame this as a chess match — who has the edge in the dugout?""",
        hist, BROADCAST_SYSTEM))

    # Sarah brings a personal story about one of the managers
    sarah = _sp(_chat(
        f"""Alex described both managers' stats. Add a HUMAN angle:
What personal milestone, rivalry history, or tactical gamble could define {hm_name} or {am_name} tonight? ~25 words.
No repeated stats.""",
        hist + [{"role": "assistant", "content": alex}], BROADCAST_SYSTEM))
    return alex, sarah


def _s_player(player: dict, enriched: dict, is_home: bool) -> str:
    name = player.get("name", "")
    pos  = player.get("pos", "")
    nat  = enriched.get("nationality", "")
    web  = _search(f"{name} football 2025 form goals stats") if SEARCH_OK else ""
    return _sp(_chat(
        f"""Spotlight {name} ({pos}{', ' + nat if nat else ''}).
Season: {enriched.get('appearances_this_season', '?')} appearances, {enriched.get('goals_this_season', '?')} goals.
Key strength: {enriched.get('key_strength', 'quality in tight spaces')}.
{('Fun fact: ' + enriched.get('fun_fact', '')) if enriched.get('fun_fact') else ''}
Recent web: {web[:150] if web else 'N/A'}
~25 words. One vivid sentence that makes the listener excited to watch this player.""",
        system=BROADCAST_SYSTEM))


def _s_lineups(ctx, hist):
    h, a      = ctx["home"], ctx["away"]
    h_names   = ctx.get("home_players_list", "")
    a_names   = ctx.get("away_players_list", "")
    h_form    = ctx.get("home_formation", "")
    a_form    = ctx.get("away_formation", "")

    if not h_names and not a_names:
        alex  = _sp(_chat(
            f"""The starting lineups for {h} vs {a} have not yet been confirmed.
Build dramatic anticipation — what selection decisions could swing the game? ~30 words.""",
            hist, BROADCAST_SYSTEM))
        sarah = _sp(_chat(
            f"""Alex noted the lineups aren't out yet for {h} vs {a}.
Give a DIFFERENT tactical curiosity — a specific position battle or injury doubt to watch. ~20 words.""",
            hist + [{"role": "assistant", "content": alex}], BROADCAST_SYSTEM))
        return alex, sarah

    alex = _sp(_chat(
        f"""Announce both confirmed starting XIs.
{h}{(' (' + h_form + ')') if h_form else ''}: {h_names}.
{a}{(' (' + a_form + ')') if a_form else ''}: {a_names}.
~45 words. Read the starting players naturally, highlight 2-3 names that stand out tonight.""",
        hist, BROADCAST_SYSTEM))

    sarah = _sp(_chat(
        f"""Alex read out the lineups. Your job: identify ONE specific positional match-up
(e.g., a striker vs. a centre-back, a winger vs. a full-back) that will decide the game. ~25 words.
Do NOT repeat player names Alex already mentioned unless comparing match-ups.""",
        hist + [{"role": "assistant", "content": alex}], BROADCAST_SYSTEM))
    return alex, sarah


def _s_standings(ctx, hist):
    h, a   = ctx["home"], ctx["away"]
    h_pos  = ctx.get("home_standing")
    a_pos  = ctx.get("away_standing")
    comp   = ctx.get("competition", "the competition")
    is_cup = ctx.get("competition_type") == "cup"

    if is_cup or (not h_pos and not a_pos):
        sarah = _sp(_chat(
            f"""Describe what is at stake in this {comp} {ctx.get('stage', 'match')}.
{h} vs {a} — one shot, everything on the line. ~35 words. Stakes, not statistics.""",
            hist, BROADCAST_SYSTEM))
        alex = _sp(_chat(
            f"""Sarah set up the stakes. Add the EMOTIONAL weight — what does winning or losing mean for each club's season? ~20 words.
No repeated facts.""",
            hist + [{"role": "assistant", "content": sarah}], BROADCAST_SYSTEM))
        return sarah, alex

    h_str = f"{h} are {_ordinal(h_pos['pos'])} with {h_pos['pts']} pts from {h_pos['played']} games" if h_pos else f"{h}"
    a_str = f"{a} are {_ordinal(a_pos['pos'])} with {a_pos['pts']} pts" if a_pos else f"{a}"
    web   = _search(f"{h} {a} {comp} table standings 2025") if SEARCH_OK else ""

    sarah = _sp(_chat(
        f"""{h_str}. {a_str}.
Web context: {web[:200] if web else 'N/A'}
~40 words. Explain what tonight's result means in the context of the title race or relegation battle.""",
        hist, BROADCAST_SYSTEM))

    alex = _sp(_chat(
        f"""Sarah covered the points tally. Add the TACTICAL pressure angle: which team HAS to win today and why that changes how they'll play? ~20 words.
New information only — not the positions Sarah already gave.""",
        hist + [{"role": "assistant", "content": sarah}], BROADCAST_SYSTEM))
    return sarah, alex


def _s_upcoming(ctx, hist):
    h, a  = ctx["home"], ctx["away"]
    h_fix = ctx.get("home_upcoming", [])
    a_fix = ctx.get("away_upcoming", [])

    def _fix_str(fixtures, team):
        if not fixtures: return f"{team}'s schedule TBC"
        return " | ".join(
            f"vs {f['away'] if (f.get('home','') or '').lower().startswith(team[:4].lower()) else f['home']} ({(f.get('date','') or '')[:5]})"
            for f in fixtures[:3])

    h_str = _fix_str(h_fix, h)
    a_str = _fix_str(a_fix, a)

    alex = _sp(_chat(
        f"""Look beyond tonight's match — what comes next?
{h} fixtures: {h_str}.
{a} fixtures: {a_str}.
~40 words. Focus on ONE team whose upcoming schedule is particularly brutal or favourable.""",
        hist, BROADCAST_SYSTEM))

    sarah = _sp(_chat(
        f"""Alex previewed the upcoming fixtures. Add a different dimension:
How does today's result affect squad rotation, injury risk, or psychological momentum going into those games? ~25 words.
New angle only.""",
        hist + [{"role": "assistant", "content": alex}], BROADCAST_SYSTEM))
    return alex, sarah


def _ordinal(n: int) -> str:
    if not n: return "?"
    s = ["th","st","nd","rd"]; v = n % 100
    return f"{n}{s[(v-20)%10] if (v-20)%10 < 4 else s[v] if v < 4 else s[0]}"


def _s_closing(ctx) -> str:
    h, a = ctx["home"], ctx["away"]
    return _sp(_chat(
        f"""Close the pre-match broadcast. Build genuine excitement for {h} vs {a} in ~20 words.
Then say EXACTLY this — word for word:
"Before we kick off — a reminder: this is strictly for adults aged 18 and over. Gambling can become addictive. Please know your limits."
Then: "For live win probabilities, tap the Insights tab in your app."
Then: "Enjoy the match!" 
Warm and sincere. Output only the spoken words.""",
        system=BROADCAST_SYSTEM))


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

def _build(betradar_id: str, ctx: dict, progress_cb=None, max_pl: int = 3) -> dict:

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
        s={"id":sid,"duration":dur,"timing":0,"text":ta,"text_b":tb}
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
                   })

    t=0
    for sc in scenes: sc["timing"]=t; t+=sc["duration"]


    gw="Gambling is strictly for persons aged 18 and over. Please gamble responsibly."

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

        "gambling_warning":{"text":gw},
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

    # Allow per-request rate override — useful for regeneration with different speed
    rate_override = request.args.get("rate", "")
    if rate_override:
        import commentary as _self
        _self._SPORTS_RATE = rate_override   # module-level override for this request
        log.info(f"Rate override: {rate_override}")

    log.info(f"Commentary: {betradar_id} va={va} vs={vs} hints_home={hints['home']} hints_away={hints['away']}")
    ctx    = _fetch_ctx(betradar_id, hints=hints)
    max_pl = int(os.environ.get("MAX_PLAYERS", "3"))
    result = _build(betradar_id, ctx, max_pl=max_pl)
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
                    result_holder["data"]=_build(betradar_id,ctx,progress_cb=_cb,max_pl=int(os.environ.get("MAX_PLAYERS","3")))
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