"""
OpenAI Commentary Blueprint — Production Edition
• MinIO bucket "sport" for audio caching (skip TTS if already generated)
• DuckDuckGo search for live player/team enrichment (free, no paid API)
• Conversation continuity — each scene continues from the previous
• Comprehensive logging for all errors
• Scene regeneration endpoint
• HTML Audio-compatible MP3 base64 + signed MinIO URLs
"""
import logging, os, json, base64, hashlib, re, time
from concurrent.futures import ThreadPoolExecutor
from flask import Blueprint, request, jsonify, Response, stream_with_context
import requests
from openai import OpenAI

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("commentary.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("commentary")

# ── OpenAI ────────────────────────────────────────────────────────────────────
_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))

# ── MinIO / S3-compatible storage ─────────────────────────────────────────────
try:
    from minio import Minio
    from minio.error import S3Error
    import urllib3
    _minio = Minio(
        os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
        http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=3, read=10)),
    )
    BUCKET = "sport"
    # Ensure bucket exists
    if not _minio.bucket_exists(BUCKET):
        _minio.make_bucket(BUCKET)
        log.info(f"Created MinIO bucket '{BUCKET}'")
    MINIO_OK = True
    log.info("MinIO connected ✓")
except Exception as e:
    log.warning(f"MinIO unavailable ({e}) — audio will not be cached")
    MINIO_OK = False
    _minio = None

# ── DuckDuckGo web search (free) ──────────────────────────────────────────────
try:
    from duckduckgo_search import DDGS
    SEARCH_OK = True
    log.info("DuckDuckGo search available ✓")
except ImportError:
    SEARCH_OK = False
    log.warning("duckduckgo_search not installed — using model knowledge only")

def _web_search(query: str, max_results: int = 3) -> str:
    """Free web search via DuckDuckGo — no API key needed."""
    if not SEARCH_OK:
        return ""
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=max_results, timelimit="m"))
        if not results:
            results = list(DDGS().text(query, max_results=max_results))
        snippets = [f"• {r.get('title','')}: {r.get('body','')[:180]}" for r in results]
        log.debug(f"Search '{query}': {len(snippets)} results")
        return "\n".join(snippets)
    except Exception as e:
        log.warning(f"Web search failed for '{query}': {e}")
        return ""

# ── MinIO helpers ─────────────────────────────────────────────────────────────
def _minio_key(match_id: str, name: str) -> str:
    return f"commentary/{match_id}/{name}"

def _minio_get(key: str) -> bytes | None:
    """Return object bytes if it exists in MinIO, else None."""
    if not MINIO_OK:
        return None
    try:
        resp = _minio.get_object(BUCKET, key)
        data = resp.read()
        resp.close(); resp.release_conn()
        log.debug(f"MinIO HIT: {key} ({len(data)} bytes)")
        return data
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        log.error(f"MinIO get error {key}: {e}")
        return None
    except Exception as e:
        log.error(f"MinIO unexpected error {key}: {e}")
        return None

def _minio_put(key: str, data: bytes, content_type: str = "audio/mpeg") -> bool:
    """Upload bytes to MinIO. Returns True on success."""
    if not MINIO_OK or not data:
        return False
    try:
        import io
        _minio.put_object(BUCKET, key, io.BytesIO(data), len(data), content_type=content_type)
        log.info(f"MinIO PUT: {key} ({len(data)} bytes)")
        return True
    except Exception as e:
        log.error(f"MinIO put error {key}: {e}")
        return False

def _minio_delete(key: str) -> bool:
    if not MINIO_OK:
        return False
    try:
        _minio.remove_object(BUCKET, key)
        log.info(f"MinIO DEL: {key}")
        return True
    except Exception as e:
        log.warning(f"MinIO delete error {key}: {e}")
        return False

def _minio_url(key: str) -> str | None:
    """Return presigned GET URL valid for 1 hour."""
    if not MINIO_OK:
        return None
    try:
        from datetime import timedelta
        url = _minio.presigned_get_object(BUCKET, key, expires=timedelta(hours=1))
        return url
    except Exception as e:
        log.error(f"MinIO presign error {key}: {e}")
        return None

# ── Blueprint ─────────────────────────────────────────────────────────────────
bp_commentary = Blueprint("commentary", __name__, url_prefix="/api")

# ── Sportradar ────────────────────────────────────────────────────────────────
LMT_TOKEN = os.environ.get("LMT_TOKEN",
    "exp=1776025306~acl=/*~data=eyJvIjoiaHR0cHM6Ly93d3cuYmV0aWthLmNvbSIsImEiOiI2MDAwNmI1MjM0YzMxY2NmOGIxNGYxNmYyODczZWU3MSIsImFjdCI6Im9yaWdpbmNoZWNrIiwib3NyYyI6Im9yaWdpbiJ9~hmac=016ea9a66a30e7c493628bc5a2beb8e294aeefa76ea7582648f6e40904e395d4")
SH_TOKEN = os.environ.get("SH_TOKEN",
    "exp=1776064004~acl=/*~data=eyJvIjoiaHR0cHM6Ly9zdGF0c2h1Yi5zcG9ydHJhZGFyLmNvbSIsImEiOiJzcG9ydHBlc2EiLCJhY3QiOiJvcmlnaW5jaGVjayIsIm9zcmMiOiJob3N0aGVhZGVyIn0~hmac=1c7b2ef7f250e867db4f35699ca70d55884e705200df665ee15860e7eb4cddd6")
_LMT_H = {"origin":"https://www.betika.com","referer":"https://www.betika.com/","user-agent":"Mozilla/5.0"}
_SH_H  = {"origin":"https://statshub.sportradar.com","referer":"https://statshub.sportradar.com/","user-agent":"Mozilla/5.0"}

def _lmt(ep, mid):
    try:
        r = requests.get(f"https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/{ep}/{mid}?T={LMT_TOKEN}", headers=_LMT_H, timeout=6)
        if r.ok: return r.json().get("doc",[{}])[0].get("data",{})
    except Exception as e: log.warning(f"LMT {ep}/{mid}: {e}")
    return {}

def _sh(ep, mid, extra=""):
    try:
        r = requests.get(f"https://sh.fn.sportradar.com/sportpesa/en/Etc:UTC/gismo/{ep}/{mid}{extra}?T={SH_TOKEN}", headers=_SH_H, timeout=6)
        if r.ok: return r.json().get("doc",[{}])[0].get("data",{})
    except Exception as e: log.warning(f"SH {ep}/{mid}: {e}")
    return {}

# ── Voice constants ───────────────────────────────────────────────────────────
VOICE_ALEX  = "onyx"    # Deep male
VOICE_SARAH = "nova"    # Warm female
SCENE_LEAD  = {
    "intro":   (VOICE_ALEX,  VOICE_SARAH),
    "h2h":     (VOICE_SARAH, VOICE_ALEX),
    "form":    (VOICE_ALEX,  VOICE_SARAH),
    "bracket": (VOICE_SARAH, VOICE_ALEX),
    "managers":(VOICE_ALEX,  VOICE_SARAH),
    "closing": (VOICE_SARAH, None),
}

# ── OpenAI TTS with MinIO caching ────────────────────────────────────────────
def _tts_cached(text: str, voice: str, cache_key: str) -> str | None:
    """Generate TTS MP3, cache in MinIO, return base64. Returns None on failure."""
    if not text or text.startswith("[Error"): 
        return None
    
    minio_path = f"commentary/{cache_key}.mp3"
    
    # 1. Check cache
    cached = _minio_get(minio_path)
    if cached:
        log.info(f"TTS cache HIT: {minio_path}")
        return base64.b64encode(cached).decode()
    
    # 2. Generate
    log.info(f"TTS generate: voice={voice} key={minio_path} text_len={len(text)}")
    try:
        resp = _client.audio.speech.create(model="tts-1", voice=voice, input=text, response_format="mp3")
        mp3_bytes = resp.content
    except Exception as e:
        log.error(f"TTS generation failed (voice={voice}): {e}")
        return None
    
    # 3. Cache
    _minio_put(minio_path, mp3_bytes)
    return base64.b64encode(mp3_bytes).decode()

# ── OpenAI text with conversation history ────────────────────────────────────
def _chat(user_msg: str, history: list = None, system: str = None) -> str:
    """Call gpt-4o-mini with optional conversation history for continuity."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    if history:
        messages.extend(history[-6:])  # last 3 exchanges
    messages.append({"role": "user", "content": user_msg})
    
    try:
        resp = _client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.72,
            max_tokens=400,
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        log.error(f"Chat API error: {e}")
        return f"[Error: {e}]"

# ── Player enrichment with web search ────────────────────────────────────────
def _enrich_player(name: str, team: str) -> dict:
    """Enrich player data using model knowledge + DuckDuckGo search."""
    # Search for current stats
    search_info = _web_search(f"{name} {team} 2024 2025 goals appearances stats football")
    transfer_info = _web_search(f"{name} transfer news 2025")
    
    prompt = f"""Football analyst. Provide current stats for {name} ({team}).
Web search results:
{search_info}
Transfer news:
{transfer_info}

Return ONLY valid JSON (no markdown) with exact keys:
{{"full_name": "", "nationality": "", "age": 0,
  "appearances_this_season": 0, "goals_this_season": 0, "assists_this_season": 0,
  "career_goals": 0, "market_value_eur": "", "transfer_news": "",
  "key_strength": "", "fun_fact": "", "club_history": ""}}
Use search data where available. Use model knowledge for the rest."""
    
    try:
        raw = _chat(prompt, system="You are a precise football data analyst. Output only valid JSON.")
        # Strip any markdown
        raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`")
        return json.loads(raw)
    except Exception as e:
        log.warning(f"Player enrichment parse error for {name}: {e}")
        return {"full_name": name, "nationality": "Unknown", "transfer_news": "No recent news.", "key_strength": "Skilled"}

def _enrich_manager(name: str, team: str) -> dict:
    """Manager enrichment with web search."""
    search_info = _web_search(f"{name} manager {team} tactics 2025 formation win rate")
    
    prompt = f"""Manager profile for {name} ({team}).
Web search: {search_info}
Return ONLY valid JSON:
{{"full_name": "", "nationality": "", "age": 0, "current_team": "{team}",
  "tenure_years": 0.0, "win_rate_pct": 0, "trophies": [],
  "preferred_formation": "", "tactical_style": "",
  "career_highlights": "", "pre_match_quote": ""}}"""
    try:
        raw = _chat(prompt, system="Football analyst. JSON only.")
        raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`")
        return json.loads(raw)
    except Exception as e:
        log.warning(f"Manager enrichment error for {name}: {e}")
        return {"full_name": name, "current_team": team, "pre_match_quote": "We'll give everything tonight."}

# ── Conversation system — scenes reference previous dialogue ──────────────────
BROADCAST_SYSTEM = """You are scripting a live TV football broadcast.
ALEX: Lead male commentator — authoritative, dramatic, passionate.
SARAH: Female analyst — warm, insightful, sharp statistics.
Write NATURAL broadcast dialogue. Reference previous exchanges. 
No stage directions. No hashtags. Just pure speech."""

def _build_history(prev_scenes: list) -> list:
    """Convert previous scene texts to conversation history messages."""
    history = []
    for sc in prev_scenes[-3:]:  # last 3 scenes for context
        if sc.get("text"):
            history.append({"role":"assistant","content":f"[{sc['id'].upper()}] ALEX: {sc['text']}"})
        if sc.get("text_b"):
            history.append({"role":"assistant","content":f"[{sc['id'].upper()}] SARAH: {sc['text_b']}"})
    return history

def _gen_intro(ctx: dict, history: list) -> tuple:
    h, a = ctx["home"], ctx["away"]
    comp = f"{ctx.get('competition','')} {ctx.get('stage','')}".strip()
    venue, date = ctx.get("venue",""), ctx.get("date","")
    
    alex = _chat(
        f"""ALEX opens the pre-match show. Match: {h} vs {a} | {comp} | {venue} | {date}
~35 words. Start with "LADIES AND GENTLEMEN" — electrifying. End: "...what a night this promises to be!" """,
        history, BROADCAST_SYSTEM
    )
    sarah = _chat(
        f"""SARAH responds to Alex's opening. Add ONE sharp insight about {h} vs {a} tonight — ~20 words.
Continue naturally as if you just heard Alex say: "{alex[:80]}..." """,
        history + [{"role":"assistant","content":f"ALEX: {alex}"}], BROADCAST_SYSTEM
    )
    return alex, sarah

def _gen_h2h(ctx: dict, history: list) -> tuple:
    h, a = ctx["home"], ctx["away"]
    hw, dr, aw = ctx.get("home_wins",0), ctx.get("draws",0), ctx.get("away_wins",0)
    recent = ctx.get("h2h_matches",[])[:2]
    snip = " | ".join(f"{m['home']} {m['score_home']}-{m['score_away']} {m['away']} ({m.get('date','')})" for m in recent) or "No recent meetings"
    
    sarah = _chat(
        f"""SARAH presents H2H stats — picking up from the intro.
{h} wins: {hw}, Draws: {dr}, {a} wins: {aw}. Recent: {snip}
~40 words. Reference Alex's intro context. Pure broadcast speech.""",
        history, BROADCAST_SYSTEM
    )
    alex = _chat(
        f"""ALEX reacts to Sarah's H2H analysis — ~20 words. What does this history mean for TONIGHT?
Continuing naturally from: "{sarah[:80]}..." """,
        history + [{"role":"assistant","content":f"SARAH: {sarah}"}], BROADCAST_SYSTEM
    )
    return sarah, alex

def _gen_form(ctx: dict, history: list) -> tuple:
    h, a = ctx["home"], ctx["away"]
    hf = " ".join(ctx.get("home_form",[])) or "unknown"
    af = " ".join(ctx.get("away_form",[])) or "unknown"
    hg, ag = ctx.get("home_gpg",0), ctx.get("away_gpg",0)
    
    alex = _chat(
        f"""ALEX breaks down recent form — continuing the broadcast naturally.
{h} last 5: {hf} ({hg:.1f} goals/game) | {a} last 5: {af} ({ag:.1f} goals/game).
~40 words. Build narrative tension. Reference previous conversation.""",
        history, BROADCAST_SYSTEM
    )
    sarah = _chat(
        f"""SARAH adds analysis to Alex's form breakdown — ~25 words.
Which side's momentum is more dangerous tonight? Natural response to: "{alex[:80]}..." """,
        history + [{"role":"assistant","content":f"ALEX: {alex}"}], BROADCAST_SYSTEM
    )
    return alex, sarah

def _gen_stage(ctx: dict, history: list) -> tuple:
    h, a = ctx["home"], ctx["away"]
    comp = ctx.get("competition","this competition"); stage = ctx.get("stage","")
    
    sarah = _chat(
        f"""SARAH sets the competitive stage — continuing the broadcast.
{h} vs {a} in {comp} {stage}. What's at stake? ~40 words, passionate. 
Reference earlier discussion naturally.""",
        history, BROADCAST_SYSTEM
    )
    alex = _chat(
        f"""ALEX adds dramatic weight to Sarah's stage-setting — ~20 words.
What would victory mean for each club? Continuing from: "{sarah[:80]}..." """,
        history + [{"role":"assistant","content":f"SARAH: {sarah}"}], BROADCAST_SYSTEM
    )
    return sarah, alex

def _gen_managers(ctx: dict, history: list) -> tuple:
    hm, am = ctx.get("home_manager",{}), ctx.get("away_manager",{})
    h, a = ctx["home"], ctx["away"]
    
    alex = _chat(
        f"""ALEX spotlights the managers — flowing naturally from previous topics.
{hm.get('full_name',h+' coach')}: {hm.get('tactical_style','pressing')}, {hm.get('win_rate_pct','?')}% win rate, formation {hm.get('preferred_formation','?')}.
{am.get('full_name',a+' coach')}: {am.get('tactical_style','counter')}, {am.get('win_rate_pct','?')}% win rate, formation {am.get('preferred_formation','?')}.
~40 words. Tactical chess match angle.""",
        history, BROADCAST_SYSTEM
    )
    sarah = _chat(
        f"""SARAH identifies ONE decisive tactical edge in tonight's manager battle — ~25 words.
Natural response to Alex's manager spotlight: "{alex[:80]}..." """,
        history + [{"role":"assistant","content":f"ALEX: {alex}"}], BROADCAST_SYSTEM
    )
    return alex, sarah

def _gen_player(player: dict, enriched: dict, voice_lead: str) -> str:
    name = player.get("name","")
    pos, nat = player.get("pos",""), enriched.get("nationality","?")
    apps, gls = enriched.get("appearances_this_season","?"), enriched.get("goals_this_season","?")
    strength = enriched.get("key_strength","technically gifted")
    fun_fact = enriched.get("fun_fact","")
    
    speaker = "ALEX" if voice_lead == VOICE_ALEX else "SARAH"
    txt = _chat(
        f"""{speaker} spotlights {name} ({pos}, {nat}). Season: {apps} apps, {gls} goals. 
Key strength: {strength}. {('Fun fact: '+fun_fact) if fun_fact else ''}
~25 words, energetic spotlight commentary.""",
        system=BROADCAST_SYSTEM
    )
    return txt

def _gen_closing(ctx: dict) -> str:
    h, a = ctx["home"], ctx["away"]
    sarah = _chat(
        f"""SARAH closes the pre-match show — the natural finale of the whole broadcast.
{h} vs {a} — build the anticipation for kickoff (~20 words).
Then EXACTLY say: "Before we kick off — a quick reminder: this broadcast is for responsible adults aged 18 and over. Gambling can become addictive. Please know your limits and play responsibly."
Then: "For live win probabilities and deeper match insights, tap the Insights tab in your app."
Then: "Enjoy the match!" — warm, sincere.""",
        system=BROADCAST_SYSTEM
    )
    return sarah

# ── Main commentary endpoint ──────────────────────────────────────────────────
@bp_commentary.route("/odds/match/<betradar_id>/commentary", methods=["GET"])
def get_commentary(betradar_id: str):
    t0 = time.time()
    sport  = request.args.get("sport","soccer")
    force  = request.args.get("force","false").lower() == "true"  # bypass cache
    log.info(f"Commentary request: {betradar_id} sport={sport} force={force}")

    # ── 1. Match data ──────────────────────────────────────────────────────────
    info = _lmt("match_info", betradar_id) or _sh("match_info_statshub", betradar_id)
    md   = (info.get("match") if info else None) or {}
    teams = md.get("teams",{})
    home  = teams.get("home",{}).get("name","Home Team")
    away  = teams.get("away",{}).get("name","Away Team")
    h_uid = teams.get("home",{}).get("uid")
    a_uid = teams.get("away",{}).get("uid")
    s_id  = md.get("_seasonid")
    jerseys    = info.get("jerseys",{}) if info else {}
    home_color = f"#{jerseys.get('home',{}).get('player',{}).get('base','e63030')}"
    away_color = f"#{jerseys.get('away',{}).get('player',{}).get('base','c8a200')}"
    venue = md.get("venue",{}).get("name","TBA")
    dt    = md.get("_dt",{}).get("date","TBA")
    log.info(f"Match: {home} vs {away} | {venue} | {dt}")

    # ── 2. Parallel Sportradar fetches ─────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=8) as pool:
        f_sq  = pool.submit(_lmt, "match_squads", betradar_id)
        f_h2h = pool.submit(_sh, "stats_match_head2head", betradar_id)
        f_frm = pool.submit(_sh, "stats_formtable", str(s_id)) if s_id else None
        f_hr  = pool.submit(_sh, "stats_team_lastx", str(h_uid), "/10") if h_uid else None
        f_ar  = pool.submit(_sh, "stats_team_lastx", str(a_uid), "/10") if a_uid else None
        sq       = f_sq.result()
        h2h_raw  = f_h2h.result()
        form_raw = f_frm.result() if f_frm else {}
        hr = f_hr.result() if f_hr else {}
        ar = f_ar.result() if f_ar else {}

    # ── 3. Parse H2H ──────────────────────────────────────────────────────────
    h2h_list=[]; hw=dr=aw=0
    for m in (h2h_raw.get("matches") or [])[:10]:
        sh=m.get("result",{}).get("home",0); sa=m.get("result",{}).get("away",0)
        h2h_list.append({"date":m.get("_dt",{}).get("date",""),
            "home":m.get("teams",{}).get("home",{}).get("name",""),
            "away":m.get("teams",{}).get("away",{}).get("name",""),
            "score_home":sh,"score_away":sa})
        if sh>sa: hw+=1
        elif sh==sa: dr+=1
        else: aw+=1

    # ── 4. Parse form ─────────────────────────────────────────────────────────
    hf=[]; af=[]
    for t in (form_raw.get("teams") or []):
        uid=str(t.get("team",{}).get("uid",""))
        fl=[f.get("value","?") for f in (t.get("form",{}).get("total") or [])][:5]
        if uid==str(h_uid): hf=fl
        if uid==str(a_uid): af=fl

    # ── 5. Parse players ──────────────────────────────────────────────────────
    def _pp(node):
        if isinstance(node,dict): return node.get("players") or []
        return node if isinstance(node,list) else []
    h_sq = sq.get("home",{}) if sq else {}
    a_sq = sq.get("away",{}) if sq else {}
    h_pl = _pp(h_sq.get("startinglineup", h_sq.get("players",[])))
    a_pl = _pp(a_sq.get("startinglineup", a_sq.get("players",[])))
    h_mgr = h_sq.get("coach",{}).get("name") or f"{home} Manager"
    a_mgr = a_sq.get("coach",{}).get("name") or f"{away} Manager"

    all_pl = h_pl[:6] + a_pl[:5]
    players_base = [{
        "name": p.get("playername",p.get("name","")).split(",")[0].strip(),
        "number": p.get("shirtnumber",""), "pos": p.get("matchpos","M"),
        "team": home if p in h_pl else away,
        "color": home_color if p in h_pl else away_color,
    } for p in all_pl]

    def _gpg(r):
        ms=(r.get("matches") or [])[:5]
        return round(sum(m.get("result",{}).get("home",0)+m.get("result",{}).get("away",0) for m in ms)/max(len(ms),1),1)

    ctx = {
        "home":home,"away":away,
        "home_city":teams.get("home",{}).get("countryCode",""),
        "away_city":teams.get("away",{}).get("countryCode",""),
        "competition":"","stage":"","venue":venue,"date":dt,
        "h2h_matches":h2h_list,"total_meetings":len(h2h_list),
        "home_wins":hw,"draws":dr,"away_wins":aw,
        "home_form":hf,"away_form":af,
        "home_gpg":_gpg(hr),"away_gpg":_gpg(ar),
    }

    # ── 6. Parallel AI enrichment ─────────────────────────────────────────────
    max_pl = int(os.environ.get("MAX_PLAYERS","3"))
    log.info(f"Enriching {min(max_pl, len(players_base))} players + 2 managers")
    
    with ThreadPoolExecutor(max_workers=6) as pool:
        fmh = pool.submit(_enrich_manager, h_mgr, home)
        fma = pool.submit(_enrich_manager, a_mgr, away)
        pf  = {pool.submit(_enrich_player, p["name"], p["team"]): i
               for i,p in enumerate(players_base[:max_pl])}
    
    mgr_h = fmh.result(); mgr_a = fma.result()
    ctx["home_manager"] = mgr_h; ctx["away_manager"] = mgr_a
    
    enriched = [{}]*len(players_base)
    for fut,i in pf.items():
        try: enriched[i] = fut.result()
        except Exception as e: log.error(f"Player {i} enrichment: {e}"); enriched[i] = {}

    # ── 7. Generate scene dialogues with conversation continuity ──────────────
    log.info("Generating scene dialogues with conversation continuity...")
    scenes_text: list[dict] = []
    
    # Generate all 5 main scenes sequentially so each references the previous
    def _make_scene(sid, dur, fn):
        hist = _build_history(scenes_text)
        ta, tb = fn(ctx, hist)
        s = {"id":sid,"duration":dur,"timing":0,"text":ta,"text_b":tb,"audio_a":None,"audio_b":None,"audio_url_a":None,"audio_url_b":None}
        scenes_text.append(s)
        return s

    scene_defs = [
        ("intro",    6,  _gen_intro),
        ("h2h",      7,  _gen_h2h),
        ("form",     6,  _gen_form),
        ("bracket",  7,  _gen_stage),
        ("managers", 6,  _gen_managers),
    ]
    scenes = [_make_scene(sid, dur, fn) for sid,dur,fn in scene_defs]
    
    # Player commentary
    pl_texts = []
    for i,(p,e) in enumerate(zip(players_base[:6], enriched[:6])):
        v = VOICE_ALEX if p["team"]==home else VOICE_SARAH
        hist = _build_history(scenes_text)
        pl_texts.append((_gen_player(p,e,v), v, p, e))

    pl_dur = max(len(players_base[:11])*3.5+10, 35)
    players_scene = {
        "id":"players","duration":int(pl_dur),"timing":0,
        "text":"","text_b":"","audio_a":None,"audio_b":None,
        "players": [{**p,**e,"commentary":txt,"voice":v,"audio_b64":None,"audio_url":None}
                    for txt,v,p,e in pl_texts] +
                   [{**p,**(enriched[i] if i<len(enriched) else {}),"commentary":"","voice":VOICE_ALEX,"audio_b64":None,"audio_url":None}
                    for i,p in enumerate(players_base[len(pl_texts):])]
    }
    scenes.append(players_scene)

    closing_text = _gen_closing(ctx)
    scenes.append({"id":"closing","duration":5,"timing":0,"text":closing_text,"text_b":"",
                   "audio_a":None,"audio_b":None,"audio_url_a":None,"audio_url_b":None})

    # Compute timings
    t_cursor = 0
    for sc in scenes:
        sc["timing"] = t_cursor
        t_cursor += sc["duration"]

    # ── 8. TTS with MinIO caching ─────────────────────────────────────────────
    tts_enabled = os.environ.get("TTS","true").lower() != "false"
    log.info(f"TTS generation: enabled={tts_enabled}")
    
    def _safe_tts(text, voice, cache_key):
        if not tts_enabled: return None
        if not text or text.startswith("[Error"): return None
        return _tts_cached(text, voice, cache_key)

    match_hash = hashlib.md5(f"{betradar_id}".encode()).hexdigest()[:8]
    
    for sc in scenes:
        if sc["id"] == "players":
            for j,p in enumerate(sc.get("players",[])):
                if not p.get("commentary"): continue
                ckey = f"{betradar_id}/player_{j}_{p.get('name','x').replace(' ','_')[:20]}"
                b64 = _safe_tts(p["commentary"], p.get("voice",VOICE_ALEX), ckey)
                p["audio_b64"] = b64
                if b64: p["audio_url"] = f"/api/audio/commentary/{ckey}.mp3"
        else:
            va, vb = SCENE_LEAD.get(sc["id"],(VOICE_ALEX,None))
            if sc.get("text"):
                ckey_a = f"{betradar_id}/scene_{sc['id']}_{va}"
                b64_a  = _safe_tts(sc["text"], va, ckey_a)
                sc["audio_a"] = b64_a
                if b64_a: sc["audio_url_a"] = f"/api/audio/commentary/{ckey_a}.mp3"
            if sc.get("text_b") and vb:
                ckey_b = f"{betradar_id}/scene_{sc['id']}_{vb}"
                b64_b  = _safe_tts(sc["text_b"], vb, ckey_b)
                sc["audio_b"] = b64_b
                if b64_b: sc["audio_url_b"] = f"/api/audio/commentary/{ckey_b}.mp3"

    gw_text = ("Gambling is only for persons aged 18 and over. Please gamble responsibly. "
               "Set deposit limits and seek help at gamblingtherapy.org if needed.")
    gw_ckey = f"{betradar_id}/gambling_warning"
    gw_audio = _safe_tts(gw_text, VOICE_SARAH, gw_ckey)
    gw_url   = f"/api/audio/commentary/{gw_ckey}.mp3" if gw_audio else None

    log.info(f"Commentary done in {time.time()-t0:.1f}s for {home} vs {away}")
    
    return jsonify({
        "match": {"home":home,"away":away,"home_color":home_color,"away_color":away_color,
                  "venue":venue,"date":dt,
                  "h2h":{"total":len(h2h_list),"home_wins":hw,"draws":dr,"away_wins":aw,"matches":h2h_list[:5]},
                  "form":{"home":hf,"away":af}},
        "home_manager":mgr_h, "away_manager":mgr_a,
        "enriched_players":players_scene.get("players",[]),
        "scenes":scenes,
        "total_duration":t_cursor,
        "gambling_warning":{"text":gw_text,"audio_b64":gw_audio,"audio_url":gw_url}
    })


# ── Audio proxy endpoint (serves files from MinIO) ────────────────────────────
@bp_commentary.route("/audio/<path:key>")
def get_audio(key: str):
    """Stream an audio file from MinIO. Used as audio_url for scenes."""
    full_key = f"commentary/{key}"
    log.info(f"Audio request: {full_key}")
    
    data = _minio_get(full_key)
    if not data:
        return jsonify({"error": "Audio not found", "key": full_key}), 404
    
    return Response(
        data,
        mimetype="audio/mpeg",
        headers={
            "Content-Length": str(len(data)),
            "Content-Disposition": f'inline; filename="{key.split("/")[-1]}"',
            "Cache-Control": "public, max-age=3600",
            "Accept-Ranges": "bytes",
        }
    )


# ── Scene regeneration endpoint ───────────────────────────────────────────────
@bp_commentary.route("/odds/match/<betradar_id>/scene/<scene_id>/regenerate", methods=["POST"])
def regenerate_scene(betradar_id: str, scene_id: str):
    """Delete cached audio for a scene and regenerate it with an optional custom prompt."""
    body = request.json or {}
    custom_text_a = body.get("text_a")
    custom_text_b = body.get("text_b")
    voice_a = body.get("voice_a", VOICE_ALEX)
    voice_b = body.get("voice_b", VOICE_SARAH)
    
    log.info(f"Regenerate scene {scene_id} for match {betradar_id}")
    
    # Delete old cache
    for v in [voice_a, voice_b]:
        _minio_delete(f"commentary/{betradar_id}/scene_{scene_id}_{v}")
    
    results = {}
    if custom_text_a:
        ckey = f"{betradar_id}/scene_{scene_id}_{voice_a}"
        b64  = _tts_cached(custom_text_a, voice_a, ckey)
        results["audio_a"] = b64
        results["audio_url_a"] = f"/api/audio/commentary/{ckey}.mp3" if b64 else None
    if custom_text_b:
        ckey = f"{betradar_id}/scene_{scene_id}_{voice_b}"
        b64  = _tts_cached(custom_text_b, voice_b, ckey)
        results["audio_b"] = b64
        results["audio_url_b"] = f"/api/audio/commentary/{ckey}.mp3" if b64 else None
    
    return jsonify({"status":"ok","scene":scene_id,**results})


# ── MinIO status endpoint ─────────────────────────────────────────────────────
@bp_commentary.route("/audio/status")
def audio_status():
    return jsonify({
        "minio_ok": MINIO_OK,
        "search_ok": SEARCH_OK,
        "bucket": BUCKET if MINIO_OK else None,
        "tts_enabled": os.environ.get("TTS","true").lower() != "false",
    })