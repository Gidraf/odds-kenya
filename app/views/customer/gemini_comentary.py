"""
Gemini Commentary + Edge-TTS Blueprint
Route: /api/odds/match/<betradar_id>/commentary
Returns JSON with per-scene commentary text + base64 MP3 audio + player/manager enrichment.
"""
from flask import Blueprint, request, jsonify
import requests, json, base64, os, re, time

# 🟢 CRITICAL IMPORT FIX: as_completed is explicitly imported here
from concurrent.futures import ThreadPoolExecutor, as_completed 
import threading as _threading
import tempfile
import asyncio
import edge_tts

# ── Gemini SDK (For Text Generation Only) ─────────────────────────────────────
try:
    from google import genai as _genai
    from google.genai import types as _gtypes
    _genai_client = _genai.Client(api_key=os.environ.get("GEMINI_API_KEY", ""))
    GENAI_AVAILABLE = True
except Exception as e:
    print(f"[Gemini] SDK not available: {e}")
    GENAI_AVAILABLE = False

# ── Blueprint ─────────────────────────────────────────────────────────────────
bp_commentary = Blueprint("commentary", __name__, url_prefix="/api")

# ── Sportradar tokens (reuse from deep_analytics) ────────────────────────────
LMT_TOKEN = os.environ.get("LMT_TOKEN",
    "exp=1776025306~acl=/*~data=eyJvIjoiaHR0cHM6Ly93d3cuYmV0aWthLmNvbSIsImEiOiI2MDAwNmI1MjM0YzMxY2NmOGIxNGYxNmYyODczZWU3MSIsImFjdCI6Im9yaWdpbmNoZWNrIiwib3NyYyI6Im9yaWdpbiJ9~hmac=016ea9a66a30e7c493628bc5a2beb8e294aeefa76ea7582648f6e40904e395d4")
SH_TOKEN  = os.environ.get("SH_TOKEN",
    "exp=1776064004~acl=/*~data=eyJvIjoiaHR0cHM6Ly9zdGF0c2h1Yi5zcG9ydHJhZGFyLmNvbSIsImEiOiJzcG9ydHBlc2EiLCJhY3QiOiJvcmlnaW5jaGVjayIsIm9zcmMiOiJob3N0aGVhZGVyIn0~hmac=1c7b2ef7f250e867db4f35699ca70d55884e705200df665ee15860e7eb4cddd6")

_LMT_HDRS = {"origin":"https://www.betika.com","referer":"https://www.betika.com/","user-agent":"Mozilla/5.0"}
_SH_HDRS  = {"origin":"https://statshub.sportradar.com","referer":"https://statshub.sportradar.com/","user-agent":"Mozilla/5.0"}

def _lmt(endpoint, item_id):
    try:
        r = requests.get(f"https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/{endpoint}/{item_id}?T={LMT_TOKEN}", headers=_LMT_HDRS, timeout=6)
        if r.ok: return r.json().get("doc",[{}])[0].get("data",{})
    except: pass
    return {}

def _sh(endpoint, item_id, extra=""):
    try:
        r = requests.get(f"https://sh.fn.sportradar.com/sportpesa/en/Etc:UTC/gismo/{endpoint}/{item_id}{extra}?T={SH_TOKEN}", headers=_SH_HDRS, timeout=6)
        if r.ok: return r.json().get("doc",[{}])[0].get("data",{})
    except: pass
    return {}

# ── Gemini Text Helpers ───────────────────────────────────────────────────────
_TEXT_MODELS = [
    "gemini-2.5-flash",
    "gemini-2.5-flash-preview-05-20",
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-flash-latest",
]

class _RateLimiter:
    """Token-bucket: max `rate` calls per `period` seconds."""
    def __init__(self, rate=4, period=62.0):
        self._rate, self._period = rate, period
        self._lock  = _threading.Lock()
        self._calls = []

    def acquire(self):
        while True:
            with self._lock:
                now = time.time()
                self._calls = [t for t in self._calls if now - t < self._period]
                if len(self._calls) < self._rate:
                    self._calls.append(now)
                    return
                wait = self._period - (now - self._calls[0]) + 0.5
            print(f"[RateLimit] free-tier 4 RPM cap — sleeping {wait:.1f}s")
            time.sleep(max(wait, 1.0))

_rl = _RateLimiter(rate=4, period=62.0)

def _parse_retry_delay(err_str, default=17.0):
    m = re.search(r"retry in ([0-9.]+)s", err_str, re.I)
    if m: return float(m.group(1)) + 1.0
    m = re.search(r"retryDelay.*?([0-9.]+)s", err_str, re.I)
    if m: return float(m.group(1)) + 1.0
    return default

def _gemini_text(prompt, model=None, max_retries=4):
    if not GENAI_AVAILABLE:
        return "[AI commentary unavailable – GEMINI_API_KEY not set]"
    candidates = ([model] + _TEXT_MODELS) if model else _TEXT_MODELS
    last_err = ""
    for m in candidates:
        if not m:
            continue
        for attempt in range(max_retries):
            try:
                _rl.acquire()
                resp = _genai_client.models.generate_content(model=m, contents=prompt)
                return resp.text.strip()
            except Exception as e:
                last_err = str(e)
                if "429" in last_err or "RESOURCE_EXHAUSTED" in last_err:
                    wait = _parse_retry_delay(last_err)
                    print(f"[Gemini] 429 on {m}, waiting {wait:.1f}s (attempt {attempt+1}/{max_retries})")
                    time.sleep(wait)
                elif "404" in last_err or "NOT_FOUND" in last_err:
                    break   
                else:
                    return f"[Error: {last_err}]"
    return f"[Error: {last_err}]"

# ── 🟢 Free Edge-TTS Audio Generation ─────────────────────────────────────────
def _free_commentator_tts(text, voice="en-GB-RyanNeural"):
    """Generates free TTS using Microsoft Edge's Neural voices."""
    if not text or text.startswith("[Error"):
        return None
        
    try:
        # Speed up the rate slightly for that fast-paced sports energy
        communicate = edge_tts.Communicate(text, voice, rate="+15%", pitch="+5Hz")
        
        # Save to a temporary file
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as fp:
            temp_path = fp.name
            
        # edge-tts is async. We run it in a new event loop to ensure thread safety in Flask/ThreadPool
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(communicate.save(temp_path))
        loop.close()
        
        # Read back as base64 for your frontend
        with open(temp_path, "rb") as audio_file:
            encoded_b64 = base64.b64encode(audio_file.read()).decode()
            
        os.remove(temp_path)
        return encoded_b64
        
    except Exception as e:
        print(f"[Edge TTS] Fatal error: {e}")
        return None

# ── Data Enrichment Helpers ───────────────────────────────────────────────────
def _search_player_info(player_name: str, team: str) -> dict:
    prompt = f"""You are a football data analyst. Search for current information about {player_name} who plays for {team}.
Return ONLY valid JSON (no markdown, no backticks) with these exact keys:
{{
  "full_name": "...",
  "nationality": "...",
  "age": 0,
  "appearances_this_season": 0,
  "goals_this_season": 0,
  "assists_this_season": 0,
  "career_goals": 0,
  "market_value_eur": "...",
  "transfer_news": "...",
  "key_strength": "...",
  "fun_fact": "..."
}}
If data is unavailable, use null for numbers and "Unknown" for strings."""

    raw = _gemini_text(prompt)
    try:
        cleaned = re.sub(r"```json|```", "", raw).strip()
        return json.loads(cleaned)
    except:
        return {"full_name": player_name, "nationality": "Unknown", "transfer_news": "No recent transfer news."}

def _search_manager_info(manager_name: str, team: str) -> dict:
    prompt = f"""Football analyst report for manager {manager_name} of {team}.
Return ONLY valid JSON (no markdown):
{{
  "full_name": "...",
  "nationality": "...",
  "age": 0,
  "current_team": "{team}",
  "tenure_years": 0.0,
  "win_rate_pct": 0,
  "trophies": ["..."],
  "preferred_formation": "...",
  "tactical_style": "...",
  "career_highlights": "...",
  "pre_match_quote": "..."
}}"""
    raw = _gemini_text(prompt)
    try:
        cleaned = re.sub(r"```json|```", "", raw).strip()
        return json.loads(cleaned)
    except:
        return {"full_name": manager_name, "current_team": team, "pre_match_quote": "We are ready for this challenge."}

# ── Scene commentary generators ───────────────────────────────────────────────
GAMBLING_REMINDER = " Remember: only those aged 18 and over may bet. Please gamble responsibly."

def _commentary_intro(ctx: dict) -> str:
    return _gemini_text(f"""You are the lead UEFA Champions League TV commentator with an intense, dramatic voice.
Generate a punchy 12-second spoken intro (about 40 words) for this match:
- Home: {ctx['home']} ({ctx['home_city']})
- Away: {ctx['away']} ({ctx['away_city']})
- Competition: {ctx['competition']}, Stage: {ctx['stage']}
- Venue: {ctx['venue']}, Date: {ctx['date']}
Open with "LADIES AND GENTLEMEN..." — be electrifying. No hashtags, no formatting.
End with: "...tonight, football history awaits!" """)

def _commentary_h2h(ctx: dict) -> str:
    recent = ctx.get("h2h_matches", [])[:3]
    snippets = " | ".join(f"{m.get('home','?')} {m.get('score_home',0)}-{m.get('score_away',0)} {m.get('away','?')} ({m.get('date','')})" for m in recent)
    return _gemini_text(f"""Football commentator voice. 18-second spoken analysis (about 55 words).
Head-to-head stats: {ctx['home']} vs {ctx['away']}.
Total meetings: {ctx.get('total_meetings','N/A')}.
{ctx['home']} wins: {ctx.get('home_wins',0)}, Draws: {ctx.get('draws',0)}, {ctx['away']} wins: {ctx.get('away_wins',0)}.
Recent: {snippets}
Deliver this like it's the biggest match of the decade. Pure commentary, no punctuation marks.""")

def _commentary_form(ctx: dict) -> str:
    hf = " ".join(ctx.get("home_form",[]))
    af = " ".join(ctx.get("away_form",[]))
    return _gemini_text(f"""TV football commentator, 15-second form guide (about 45 words).
{ctx['home']} last 5 matches: {hf}. Goals/game: {ctx.get('home_gpg',2.0):.1f}. Clean sheets: {ctx.get('home_cs',0)}.
{ctx['away']} last 5 matches: {af}. Goals/game: {ctx.get('away_gpg',2.0):.1f}. Clean sheets: {ctx.get('away_cs',0)}.
Contrast the two sides' momentum. Pure speech, no markdown.""")

def _commentary_bracket(ctx: dict) -> str:
    return _gemini_text(f"""UEFA Champions League knock-out bracket commentator speech (about 40 words, 13 seconds).
{ctx['home']} reached the semi-finals beating their previous opponents.
{ctx['away']} also fought their way through. Tonight they COLLIDE.
Build epic tension. Pure speech.""")

def _commentary_managers(ctx: dict) -> str:
    hm = ctx.get("home_manager", {})
    am = ctx.get("away_manager", {})
    return _gemini_text(f"""Football commentator spotlight on managers (about 55 words, 18 seconds).
HOME bench: {hm.get('full_name', ctx['home']+' Manager')} — {hm.get('tactical_style','attacking')} tactician, {hm.get('win_rate_pct','N/A')}% win rate, trophies: {', '.join(hm.get('trophies',['N/A'])[:2])}.
AWAY bench: {am.get('full_name', ctx['away']+' Manager')} — {am.get('tactical_style','counter-attack')} expert, {am.get('win_rate_pct','N/A')}% win rate.
The tactical CHESS match begins! No markdown.""")

def _commentary_player(player: dict, enriched: dict) -> str:
    return _gemini_text(f"""Football commentator player spotlight (about 30 words, 10 seconds).
Player: {player.get('name','Unknown')}, {player.get('pos','?')}, #{player.get('number',0)}, {enriched.get('nationality','?')}.
This season: {enriched.get('appearances_this_season','?')} apps, {enriched.get('goals_this_season','?')} goals.
{enriched.get('key_strength','Technically gifted')}. Pure speech, dramatic energy.""")

def _commentary_closing(ctx: dict) -> str:
    return _gemini_text(f"""30-word closing commentary: {ctx['home']} vs {ctx['away']} — "KICK OFF IS MOMENTS AWAY..."
Then say: This broadcast is brought to you responsibly. Gambling is strictly for adults aged 18 and over. Please play responsibly and know your limits.
Pure speech.""")

# ── Main endpoint ─────────────────────────────────────────────────────────────
@bp_commentary.route("/odds/match/<betradar_id>/commentary", methods=["GET"])
def get_commentary(betradar_id: str):
    """
    Returns full cinematic commentary JSON.
    """
    sport = request.args.get("sport", "soccer")

    # ── 1. Fetch core match data ──────────────────────────────────────────────
    info = _lmt("match_info", betradar_id) or _sh("match_info_statshub", betradar_id)
    match_data = (info.get("match") if info else None) or {}
    teams = match_data.get("teams", {})
    home_team = teams.get("home", {}).get("name", "Home")
    away_team = teams.get("away", {}).get("name", "Away")
    home_uid  = teams.get("home", {}).get("uid")
    away_uid  = teams.get("away", {}).get("uid")
    season_id = match_data.get("_seasonid")

    # Jerseys
    jerseys    = info.get("jerseys", {}) if info else {}
    home_color = f"#{jerseys.get('home',{}).get('player',{}).get('base','e63030')}"
    away_color = f"#{jerseys.get('away',{}).get('player',{}).get('base','c8a200')}"

    # ── 2. Parallel data fetches ─────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=8) as pool:
        f_squads   = pool.submit(_lmt, "match_squads",           betradar_id)
        f_h2h      = pool.submit(_sh,  "stats_match_head2head",  betradar_id)
        f_form     = pool.submit(_sh,  "stats_formtable", str(season_id)) if season_id else None
        f_hr       = pool.submit(_sh,  "stats_team_lastx",       str(home_uid), "/10") if home_uid else None
        f_ar       = pool.submit(_sh,  "stats_team_lastx",       str(away_uid), "/10") if away_uid else None

        squads_raw = f_squads.result()
        h2h_raw    = f_h2h.result()
        form_raw   = f_form.result() if f_form else {}
        hr_raw     = f_hr.result() if f_hr else {}
        ar_raw     = f_ar.result() if f_ar else {}

    # ── 3. Parse H2H ─────────────────────────────────────────────────────────
    h2h_matches = []
    home_wins = draws = away_wins = 0
    for m in (h2h_raw.get("matches") or [])[:10]:
        sh = m.get("result",{}).get("home",0)
        sa = m.get("result",{}).get("away",0)
        h2h_matches.append({
            "date":  m.get("_dt",{}).get("date",""),
            "home":  m.get("teams",{}).get("home",{}).get("name",""),
            "away":  m.get("teams",{}).get("away",{}).get("name",""),
            "score_home": sh, "score_away": sa
        })
        if sh > sa: home_wins += 1
        elif sh == sa: draws += 1
        else: away_wins += 1

    # ── 4. Parse Form ─────────────────────────────────────────────────────────
    home_form, away_form = [], []
    for t in (form_raw.get("teams") or []):
        uid = str(t.get("team",{}).get("uid",""))
        f_list = [f.get("value","?") for f in (t.get("form",{}).get("total") or [])][:5]
        if uid == str(home_uid): home_form = f_list
        if uid == str(away_uid): away_form = f_list

    # ── 5. Parse Players ──────────────────────────────────────────────────────
    def _parse_players(node):
        if isinstance(node, dict): return node.get("players") or []
        if isinstance(node, list): return node
        return []

    home_sq = squads_raw.get("home", {}) if squads_raw else {}
    away_sq = squads_raw.get("away", {}) if squads_raw else {}
    h_lineup = home_sq.get("startinglineup", home_sq.get("players", []))
    a_lineup = away_sq.get("startinglineup", away_sq.get("players", []))
    home_players_raw = _parse_players(h_lineup)
    away_players_raw = _parse_players(a_lineup)
    all_players_raw  = home_players_raw[:6] + away_players_raw[:5]  # top 11

    players_base = [{
        "name":   p.get("playername", p.get("name","")).split(",")[0].strip(),
        "number": p.get("shirtnumber", ""),
        "pos":    p.get("matchpos", "M"),
        "team":   home_team if p in home_players_raw else away_team,
        "color":  home_color if p in home_players_raw else away_color,
    } for p in all_players_raw]

    # ── 6. Manager names (from squad metadata) ───────────────────────────────
    home_mgr_name = home_sq.get("coach", {}).get("name") or f"{home_team} Manager"
    away_mgr_name = away_sq.get("coach", {}).get("name") or f"{away_team} Manager"

    # ── 7. Build context ──────────────────────────────────────────────────────
    venue = match_data.get("venue", {}).get("name", "TBA")
    dt    = match_data.get("_dt", {}).get("date", "TBA")

    def _gpg(recent_raw):
        ms = (recent_raw.get("matches") or [])[:5]
        if not ms: return 2.0
        total = sum(m.get("result",{}).get("home",0) + m.get("result",{}).get("away",0) for m in ms)
        return round(total / len(ms), 1)

    ctx = {
        "home": home_team, "away": away_team,
        "home_city": teams.get("home",{}).get("countryCode",""),
        "away_city": teams.get("away",{}).get("countryCode",""),
        "competition": "UEFA Champions League", "stage": "Quarter Final",
        "venue": venue, "date": dt,
        "h2h_matches": h2h_matches, "total_meetings": len(h2h_matches),
        "home_wins": home_wins, "draws": draws, "away_wins": away_wins,
        "home_form": home_form, "away_form": away_form,
        "home_gpg": _gpg(hr_raw), "away_gpg": _gpg(ar_raw),
        "home_cs": sum(1 for m in (hr_raw.get("matches") or [])[:5]
                       if m.get("result",{}).get(("home" if str(m.get("teams",{}).get("home",{}).get("uid",""))
                                                  == str(home_uid) else "away"),99) == 0),
        "away_cs": 0,  
    }

    # ── 8. Parallel Gemini Tasks for Script Writing ───────────────────────────
    with ThreadPoolExecutor(max_workers=2) as pool:  
        f_intro    = pool.submit(_commentary_intro,    ctx)
        f_h2h_c    = pool.submit(_commentary_h2h,      ctx)
        f_form_c   = pool.submit(_commentary_form,     ctx)
        f_bracket  = pool.submit(_commentary_bracket,  ctx)
        f_mgr_home = pool.submit(_search_manager_info, home_mgr_name, home_team)
        f_mgr_away = pool.submit(_search_manager_info, away_mgr_name, away_team)
        f_closing  = pool.submit(_commentary_closing,  ctx)

        player_futures = {
            pool.submit(_search_player_info, p["name"], p["team"]): i
            for i, p in enumerate(players_base[:int(os.environ.get('MAX_PLAYERS','3'))])
        }

    mgr_home_data = f_mgr_home.result()
    mgr_away_data = f_mgr_away.result()
    ctx["home_manager"] = mgr_home_data
    ctx["away_manager"] = mgr_away_data

    mgr_text = _commentary_managers(ctx)

    enriched_players = [{}] * len(players_base)
    for fut, idx in player_futures.items():
        try: enriched_players[idx] = fut.result()
        except: enriched_players[idx] = {}

    player_commentaries = []
    for i, (p, e) in enumerate(zip(players_base[:6], enriched_players[:6])):
        txt = _commentary_player(p, e)
        player_commentaries.append(txt)

    # ── 9. Assemble scene list ────────────────────────────────────────────────
    SCENE_DURATIONS = [6, 7, 6, 7, 6] 
    player_scene_dur = max(len(players_base[:11]) * 3.5 + 10, 35)
    SCENE_DURATIONS.append(int(player_scene_dur))
    closing_dur = 5

    timing = 0
    scene_texts = [
        f_intro.result(), f_h2h_c.result(), f_form_c.result(),
        f_bracket.result(), mgr_text, None
    ]
    scene_ids = ["intro","h2h","form","bracket","managers","players"]
    scenes = []
    for i, (sid, dur) in enumerate(zip(scene_ids, SCENE_DURATIONS)):
        scenes.append({
            "id": sid, "duration": dur, "timing": timing,
            "text": scene_texts[i] or ""
        })
        timing += dur

    players_enriched_full = []
    for p, e, ct in zip(players_base, enriched_players, player_commentaries + [""]*20):
        players_enriched_full.append({**p, **e, "commentary": ct})

    scenes[-1]["players"] = players_enriched_full

    closing_text = f_closing.result()
    scenes.append({"id":"closing","duration":closing_dur,"timing":timing,"text":closing_text})

    gw_text = ("Important: Gambling is only permitted for persons aged 18 and over. "
               "Please gamble responsibly. Set limits and stick to them. "
               "If gambling affects your life, seek help at gamblingtherapy.org.")

    # ── 10. Generate Audio using Edge-TTS in Parallel ─────────────────────────
    tts_enabled = os.environ.get("EDGE_TTS", "true").lower() != "false"
    DEFAULT_VOICE = "en-GB-RyanNeural"  
    WARNING_VOICE = "en-US-GuyNeural"   

    def _safe_tts(text, voice=DEFAULT_VOICE):
        if not tts_enabled or not text or text.startswith("[Error"):
            return None
        return _free_commentator_tts(text, voice=voice)

    with ThreadPoolExecutor(max_workers=5) as tts_pool:
        # Submit scene text
        sc_futs = {tts_pool.submit(_safe_tts, sc.get("text", "")): sc for sc in scenes if sc.get("text")}
        # Submit player text
        pl_futs = {tts_pool.submit(_safe_tts, pc): i for i, pc in enumerate(player_commentaries)}
        # Submit warning text
        gw_fut = tts_pool.submit(_safe_tts, gw_text, WARNING_VOICE)

        # 🟢 This is the line that caused the error! `as_completed` is now safely imported!
        for fut in as_completed(sc_futs):
            sc_futs[fut]["audio_b64"] = fut.result()
            
        player_audios = {}
        for fut in as_completed(pl_futs):
            player_audios[pl_futs[fut]] = fut.result()
            
        gw_audio = gw_fut.result()

    if scenes[-2]["id"] == "players":
        for i, p in enumerate(scenes[-2].get("players",[])):
            p["audio_b64"] = player_audios.get(i)

    # ── 11. Return JSON Payload ───────────────────────────────────────────────
    return jsonify({
        "match": {
            "home": home_team, "away": away_team,
            "home_color": home_color, "away_color": away_color,
            "venue": venue, "date": dt,
            "h2h": {
                "total": len(h2h_matches),
                "home_wins": home_wins, "draws": draws, "away_wins": away_wins,
                "matches": h2h_matches[:5]
            },
            "form": {"home": home_form, "away": away_form},
        },
        "home_manager":  mgr_home_data,
        "away_manager":  mgr_away_data,
        "enriched_players": players_enriched_full,
        "scenes": scenes,
        "total_duration": timing + closing_dur,
        "gambling_warning": {
            "text": gw_text,
            "audio_b64": gw_audio
        }
    })