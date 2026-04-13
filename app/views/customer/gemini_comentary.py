"""
OpenAI Commentary Blueprint — Two-Voice Edition
ALEX (male / onyx) + SARAH (female / nova) alternate per scene.
Gambling warning ONLY at the closing scene with CTA to Insights tab.
Audio is returned as two separate b64 tracks per scene (audio_a, audio_b)
so the frontend can play them sequentially without overlap.
"""
from flask import Blueprint, request, jsonify
import requests, json, base64, os, re
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))
bp_commentary = Blueprint("commentary", __name__, url_prefix="/api")

# ── Sportradar tokens ─────────────────────────────────────────────────────────
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
    except: pass
    return {}

def _sh(ep, mid, extra=""):
    try:
        r = requests.get(f"https://sh.fn.sportradar.com/sportpesa/en/Etc:UTC/gismo/{ep}/{mid}{extra}?T={SH_TOKEN}", headers=_SH_H, timeout=6)
        if r.ok: return r.json().get("doc",[{}])[0].get("data",{})
    except: pass
    return {}

# ── Voice constants ───────────────────────────────────────────────────────────
VOICE_ALEX  = "onyx"    # Deep male TV presenter
VOICE_SARAH = "nova"    # Warm female analyst
# Per-scene voice assignment: which commentator leads each scene
SCENE_LEAD = {
    "intro":    (VOICE_ALEX,  VOICE_SARAH),
    "h2h":      (VOICE_SARAH, VOICE_ALEX),
    "form":     (VOICE_ALEX,  VOICE_SARAH),
    "bracket":  (VOICE_SARAH, VOICE_ALEX),
    "managers": (VOICE_ALEX,  VOICE_SARAH),
    "players":  None,   # handled per-player
    "closing":  (VOICE_SARAH, None),
}

# ── OpenAI text helper ────────────────────────────────────────────────────────
def _text(prompt, require_json=False):
    try:
        kwargs = {"model":"gpt-4o-mini","messages":[{"role":"user","content":prompt}],"temperature":0.7}
        if require_json: kwargs["response_format"] = {"type":"json_object"}
        return _client.chat.completions.create(**kwargs).choices[0].message.content.strip()
    except Exception as e:
        print(f"[text] {e}"); return f"[Error: {e}]"

# ── OpenAI TTS helper — returns base64 MP3 ───────────────────────────────────
def _tts(text, voice=VOICE_ALEX):
    if not text or text.startswith("[Error"): return None
    try:
        r = _client.audio.speech.create(model="tts-1", voice=voice, input=text)
        return base64.b64encode(r.content).decode()
    except Exception as e:
        print(f"[tts/{voice}] {e}"); return None

# ── Enrichment helpers ────────────────────────────────────────────────────────
def _player_info(name, team):
    raw = _text(f"""Football data analyst. Output current stats for {name} of {team}.
Return ONLY valid JSON: {{"full_name":"","nationality":"","age":0,
"appearances_this_season":0,"goals_this_season":0,"assists_this_season":0,
"career_goals":0,"market_value_eur":"","transfer_news":"","key_strength":"","fun_fact":""}}""", require_json=True)
    try: return json.loads(raw)
    except: return {"full_name":name,"nationality":"Unknown","transfer_news":"No recent news."}

def _manager_info(name, team):
    raw = _text(f"""Manager profile for {name} of {team}.
Return ONLY valid JSON: {{"full_name":"","nationality":"","age":0,"current_team":"{team}",
"tenure_years":0.0,"win_rate_pct":0,"trophies":[],"preferred_formation":"",
"tactical_style":"","career_highlights":"","pre_match_quote":""}}""", require_json=True)
    try: return json.loads(raw)
    except: return {"full_name":name,"current_team":team,"pre_match_quote":"We're ready."}

# ── Scene dialogue generators — returns (alex_text, sarah_text) ───────────────

def _scene_intro(ctx):
    home, away = ctx["home"], ctx["away"]
    comp = ctx.get("competition",""); stage = ctx.get("stage","")
    venue = ctx.get("venue",""); date = ctx.get("date","")
    
    alex = _text(f"""You are ALEX, lead TV football commentator. Open tonight's broadcast.
Match: {home} vs {away} | {comp} {stage} | {venue} | {date}
Deliver ~30 words of electrifying opening. Start with "LADIES AND GENTLEMEN".
End with "...what a night this promises to be!" — pure speech, no notes, no stage directions.""")
    
    sarah = _text(f"""You are SARAH, football analyst co-presenter. Respond to Alex's intro.
Match: {home} vs {away} — add ONE punchy insight about tonight's match-up (~20 words).
Pure speech only, natural and excited, as if responding on air.""")
    return alex, sarah

def _scene_h2h(ctx):
    home, away = ctx["home"], ctx["away"]
    hw, dr, aw = ctx.get("home_wins",0), ctx.get("draws",0), ctx.get("away_wins",0)
    recent = ctx.get("h2h_matches",[])[:2]
    snip = " | ".join(f"{m['home']} {m['score_home']}-{m['score_away']} {m['away']}" for m in recent)
    
    sarah = _text(f"""You are SARAH, football analyst. Present H2H stats — {home} vs {away}.
{home} wins: {hw}, Draws: {dr}, {away} wins: {aw}. Recent: {snip}
~35 words, pure analysis, exciting delivery. Pure speech only.""")
    
    alex = _text(f"""You are ALEX, football commentator. React to the H2H stats Sarah just shared.
Add context about what this rivalry means tonight — ~20 words. Pure speech.""")
    return sarah, alex

def _scene_form(ctx):
    home, away = ctx["home"], ctx["away"]
    hf = " ".join(ctx.get("home_form",[])); af = " ".join(ctx.get("away_form",[]))
    
    alex = _text(f"""You are ALEX, commentator. Describe recent form.
{home} last 5: {hf} | {away} last 5: {af}
~35 words, describe momentum shift. Pure speech only.""")
    
    sarah = _text(f"""You are SARAH, analyst. Follow up on Alex's form summary.
Which team's form worries you most going into tonight? ~20 words. Pure speech.""")
    return alex, sarah

def _scene_stage(ctx):
    home, away = ctx["home"], ctx["away"]
    comp = ctx.get("competition",""); stage = ctx.get("stage","")
    
    sarah = _text(f"""You are SARAH, football analyst. Set the scene for {comp} {stage}.
{home} vs {away} — describe the journey to this point with passion. ~35 words. Pure speech.""")
    
    alex = _text(f"""You are ALEX, commentator. Add drama to Sarah's stage-setting.
What's riding on tonight's result? ~20 words of pure tension. Pure speech.""")
    return sarah, alex

def _scene_managers(ctx):
    hm = ctx.get("home_manager",{}); am = ctx.get("away_manager",{})
    home, away = ctx["home"], ctx["away"]
    
    alex = _text(f"""You are ALEX, commentator. Spotlight the managers.
{hm.get('full_name',home+' manager')}: {hm.get('tactical_style','pressing')} — {hm.get('win_rate_pct','N/A')}% win rate.
{am.get('full_name',away+' manager')}: {am.get('tactical_style','counter')} — {am.get('win_rate_pct','N/A')}% win rate.
~35 words, tactical chess match angle. Pure speech.""")
    
    sarah = _text(f"""You are SARAH, analyst. One key tactical battle between tonight's managers — ~20 words. Pure speech.""")
    return alex, sarah

def _scene_player(player, enriched, voice_lead):
    name = player.get("name","")
    pos  = player.get("pos","")
    nat  = enriched.get("nationality","?")
    apps = enriched.get("appearances_this_season","?")
    gls  = enriched.get("goals_this_season","?")
    str_ = enriched.get("key_strength","technically gifted")
    
    lead_gender = "ALEX" if voice_lead == VOICE_ALEX else "SARAH"
    resp_gender = "SARAH" if voice_lead == VOICE_ALEX else "ALEX"
    
    lead = _text(f"""You are {lead_gender}, football commentator. Player spotlight: {name}, {pos}, {nat}.
Season: {apps} appearances, {gls} goals. {str_}. ~25 words, punchy energy. Pure speech.""")
    
    return lead  # single voice for player spotlight (too short for back-and-forth)

def _scene_closing(ctx, win_prob_home=None, win_prob_draw=None, win_prob_away=None):
    home, away = ctx["home"], ctx["away"]
    
    sarah = _text(f"""You are SARAH, football presenter. Close the pre-match show.
{home} vs {away} — build excitement for kickoff (~20 words). 
Then say EXACTLY: "Before we go — a quick reminder. Tonight's broadcast is for responsible adults aged 18 and over. Gambling can be addictive. Please set your limits and play responsibly." 
Then add: "Tap the Insights tab in your app to see the latest win probabilities and data for this match — it could give you the edge."
Then: "Enjoy the game!" Pure speech, warm but serious on the warning.""")
    return sarah

# ── Main endpoint ─────────────────────────────────────────────────────────────
@bp_commentary.route("/odds/match/<betradar_id>/commentary", methods=["GET"])
def get_commentary(betradar_id):
    sport = request.args.get("sport","soccer")

    # 1. Core match data
    info = _lmt("match_info", betradar_id) or _sh("match_info_statshub", betradar_id)
    md   = (info.get("match") if info else None) or {}
    teams = md.get("teams",{})
    home  = teams.get("home",{}).get("name","Home")
    away  = teams.get("away",{}).get("name","Away")
    h_uid = teams.get("home",{}).get("uid")
    a_uid = teams.get("away",{}).get("uid")
    s_id  = md.get("_seasonid")
    jerseys    = info.get("jerseys",{}) if info else {}
    home_color = f"#{jerseys.get('home',{}).get('player',{}).get('base','e63030')}"
    away_color = f"#{jerseys.get('away',{}).get('player',{}).get('base','c8a200')}"

    # 2. Parallel Sportradar fetches
    with ThreadPoolExecutor(max_workers=8) as pool:
        f_sq  = pool.submit(_lmt,"match_squads",betradar_id)
        f_h2h = pool.submit(_sh,"stats_match_head2head",betradar_id)
        f_frm = pool.submit(_sh,"stats_formtable",str(s_id)) if s_id else None
        f_hr  = pool.submit(_sh,"stats_team_lastx",str(h_uid),"/10") if h_uid else None
        f_ar  = pool.submit(_sh,"stats_team_lastx",str(a_uid),"/10") if a_uid else None
        sq=f_sq.result(); h2h_raw=f_h2h.result()
        form_raw=f_frm.result() if f_frm else {}
        hr=f_hr.result() if f_hr else {}; ar=f_ar.result() if f_ar else {}

    # 3. Parse H2H
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

    # 4. Parse form
    hf=[]; af=[]
    for t in (form_raw.get("teams") or []):
        uid=str(t.get("team",{}).get("uid",""))
        fl=[f.get("value","?") for f in (t.get("form",{}).get("total") or [])][:5]
        if uid==str(h_uid): hf=fl
        if uid==str(a_uid): af=fl

    # 5. Parse players
    def _pp(node):
        if isinstance(node,dict): return node.get("players") or []
        return node if isinstance(node,list) else []
    h_sq=sq.get("home",{}) if sq else {}; a_sq=sq.get("away",{}) if sq else {}
    h_lin=h_sq.get("startinglineup",h_sq.get("players",[]))
    a_lin=a_sq.get("startinglineup",a_sq.get("players",[]))
    h_pl=_pp(h_lin); a_pl=_pp(a_lin)
    all_pl=h_pl[:6]+a_pl[:5]
    players_base=[{
        "name":p.get("playername",p.get("name","")).split(",")[0].strip(),
        "number":p.get("shirtnumber",""),"pos":p.get("matchpos","M"),
        "team":home if p in h_pl else away,
        "color":home_color if p in h_pl else away_color,
    } for p in all_pl]

    h_mgr = h_sq.get("coach",{}).get("name") or f"{home} Manager"
    a_mgr = a_sq.get("coach",{}).get("name") or f"{away} Manager"
    venue = md.get("venue",{}).get("name","TBA")
    dt    = md.get("_dt",{}).get("date","TBA")

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

    # 6. Parallel AI tasks (text generation)
    max_pl = int(os.environ.get("MAX_PLAYERS","3"))
    with ThreadPoolExecutor(max_workers=8) as pool:
        fi = pool.submit(_scene_intro, ctx)
        fh = pool.submit(_scene_h2h, ctx)
        ff = pool.submit(_scene_form, ctx)
        fb = pool.submit(_scene_stage, ctx)
        fmh= pool.submit(_manager_info, h_mgr, home)
        fma= pool.submit(_manager_info, a_mgr, away)
        fcl= pool.submit(_scene_closing, ctx)
        pf = {pool.submit(_player_info, p["name"],p["team"]): i for i,p in enumerate(players_base[:max_pl])}

    mgr_h=fmh.result(); mgr_a=fma.result()
    ctx["home_manager"]=mgr_h; ctx["away_manager"]=mgr_a
    mgr_texts = _scene_managers(ctx)

    enriched=[{}]*len(players_base)
    for fut,i in pf.items():
        try: enriched[i]=fut.result()
        except: enriched[i]={}

    # Player commentary (single voice alternating by team)
    pl_texts=[]
    for i,(p,e) in enumerate(zip(players_base[:6],enriched[:6])):
        v = VOICE_ALEX if p["team"]==home else VOICE_SARAH
        pl_texts.append((p,e,v))

    # 7. Scene list assembly
    DURS=[6,7,6,7,6]
    pl_dur=max(len(players_base[:11])*3.5+10,35)
    DURS.append(int(pl_dur)); DURS.append(5)

    ia,ib = fi.result(); ha,hb = fh.result()
    fa2,fb2 = ff.result(); ba,bb = fb.result()
    ma,mb = mgr_texts
    closing = fcl.result()

    scene_specs=[
        ("intro",    DURS[0], ia, ib, VOICE_ALEX,  VOICE_SARAH),
        ("h2h",      DURS[1], ha, hb, VOICE_SARAH, VOICE_ALEX),
        ("form",     DURS[2], fa2,fb2,VOICE_ALEX,  VOICE_SARAH),
        ("bracket",  DURS[3], ba, bb, VOICE_SARAH, VOICE_ALEX),
        ("managers", DURS[4], ma, mb, VOICE_ALEX,  VOICE_SARAH),
    ]
    scenes=[]
    timing=0
    for sid,dur,ta,tb,va,vb in scene_specs:
        scenes.append({"id":sid,"duration":dur,"timing":timing,
            "text":ta,"text_b":tb,"audio_a":None,"audio_b":None})
        timing+=dur

    # Players scene
    pl_enriched=[{**p,**e,"commentary":_scene_player(p,e,v),"voice":v}
                 for p,e,v in pl_texts] + [{**p,**({} if i>=len(enriched) else enriched[i]),"commentary":"","voice":VOICE_ALEX}
                 for i,p in enumerate(players_base[len(pl_texts):])]
    scenes.append({"id":"players","duration":DURS[5],"timing":timing,
        "text":"","text_b":"","audio_a":None,"audio_b":None,
        "players":pl_enriched})
    timing+=DURS[5]

    # Closing
    scenes.append({"id":"closing","duration":DURS[6],"timing":timing,
        "text":closing,"text_b":"","audio_a":None,"audio_b":None})

    gw_text=("Important: Gambling is strictly for persons aged 18 and over. "
             "Please gamble responsibly and know your limits. Help: gamblingtherapy.org")

    # 8. TTS — sequential pairs (no overlap)
    tts_enabled = os.environ.get("TTS","true").lower()!="false"

    def _safe_tts(text, voice):
        return _tts(text, voice) if tts_enabled and text and not text.startswith("[Error") else None

    for sc in scenes:
        if sc["id"]=="players":
            for p in sc.get("players",[]):
                p["audio_b64"]=_safe_tts(p.get("commentary",""), p.get("voice",VOICE_ALEX))
        else:
            sc["audio_a"]=_safe_tts(sc.get("text",""), SCENE_LEAD.get(sc["id"],(VOICE_ALEX,None))[0] or VOICE_ALEX)
            if sc.get("text_b"):
                vb_pair=SCENE_LEAD.get(sc["id"],(None,VOICE_SARAH))
                sc["audio_b"]=_safe_tts(sc["text_b"], vb_pair[1] or VOICE_SARAH)

    gw_audio=_safe_tts(gw_text, VOICE_SARAH)

    return jsonify({
        "match":{"home":home,"away":away,"home_color":home_color,"away_color":away_color,
                 "venue":venue,"date":dt,
                 "h2h":{"total":len(h2h_list),"home_wins":hw,"draws":dr,"away_wins":aw,"matches":h2h_list[:5]},
                 "form":{"home":hf,"away":af}},
        "home_manager":mgr_h,"away_manager":mgr_a,
        "enriched_players":pl_enriched,"scenes":scenes,
        "total_duration":timing+DURS[6],
        "gambling_warning":{"text":gw_text,"audio_b64":gw_audio}
    })