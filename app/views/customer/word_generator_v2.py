"""
word_generator_v2.py
────────────────────
v2 per-time-group Word document generator.

Public API
──────────
    TIME_GROUPS     – dict[group_id, (label, start_hour_EAT, end_hour_EAT)]
    get_available_groups(sport, date_str) -> list[dict]
    generate_group_document(sport, group_id, date_str, market_filter) -> BytesIO
"""

from __future__ import annotations

import io
import time
import json as _json
from datetime import datetime, timezone, timedelta
from typing import Optional

# ── EAT offset ───────────────────────────────────────────────────────────────
EAT = timedelta(hours=3)

# ── TIME GROUPS ───────────────────────────────────────────────────────────────
# group_id -> (display_label, start_hour_EAT_inclusive, end_hour_EAT_exclusive)
TIME_GROUPS: dict[str, tuple[str, int, int]] = {
    "late_night":    ("🌙 Late Night (00–06)",      0,  6),
    "early_morning": ("🌅 Early Morning (06–10)",   6, 10),
    "morning":       ("☀️ Morning (10–14)",          10, 14),
    "afternoon":     ("🌤 Afternoon (14–18)",        14, 18),
    "evening":       ("🌆 Evening (18–21)",          18, 21),
    "night":         ("🌃 Night (21–24)",            21, 24),
}

# ── MARKETS ───────────────────────────────────────────────────────────────────
# Maps short market-filter id  →  (section_label, backend_keys, outcome_labels)
_MARKET_SECTIONS: list[tuple[str, list[str], list[str], list[str]]] = [
    # (filter_id,  section_title,                 backend_keys,                   outcomes)
    ("1x2",  "🏆 FULL-TIME 1X2",          ["1x2", "match_winner", "moneyline"],  ["1", "X", "2"]),
    ("ht",   "⏱ HALF-TIME RESULT",        ["half_time"],                          ["1", "X", "2"]),
    ("btts", "⚽ BOTH TEAMS TO SCORE",    ["btts"],                               ["Yes", "No"]),
    ("dc",   "🔄 DOUBLE CHANCE",          ["double_chance"],                      ["1X", "12", "X2"]),
    ("dnb",  "🎯 DRAW NO BET",            ["dnb"],                                ["1", "2"]),
    ("ou15", "📊 OVER / UNDER 1.5 GOALS", ["over_under_goals_1_5", "over_under_1_5"], ["Over", "Under"]),
    ("ou25", "📊 OVER / UNDER 2.5 GOALS", ["over_under_goals_2_5", "over_under_2_5"], ["Over", "Under"]),
    ("ou35", "📊 OVER / UNDER 3.5 GOALS", ["over_under_goals_3_5", "over_under_3_5"], ["Over", "Under"]),
    ("ou45", "📊 OVER / UNDER 4.5 GOALS", ["over_under_goals_4_5", "over_under_4_5"], ["Over", "Under"]),
]

_OUTCOME_ALIASES: dict[str, list[str]] = {
    "1":    ["1", "home", "home_win", "win"],
    "X":    ["x", "draw", "tie"],
    "2":    ["2", "away", "away_win", "loss"],
    "Yes":  ["yes", "btts_yes", "both_score"],
    "No":   ["no",  "btts_no"],
    "1X":   ["1x", "home_or_draw"],
    "12":   ["12", "home_or_away"],
    "X2":   ["x2", "draw_or_away"],
    "Over": ["over", "o"],
    "Under":["under", "u"],
}


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).strip().replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _eat_hour(dt_utc: datetime) -> int:
    return (dt_utc + EAT).hour


def _match_dt(m: dict) -> datetime:
    dt = _parse_dt(m.get("start_time", ""))
    return dt if dt else _now_utc()


def _eat_time(dt_utc: datetime) -> str:
    try:
        return (dt_utc + EAT).strftime("%H:%M")
    except Exception:
        return ""


def _eat_date(dt_utc: datetime) -> str:
    try:
        return (dt_utc + EAT).strftime("%d %b")
    except Exception:
        return ""


def _get_redis():
    try:
        from app.workers.celery_tasks import _redis as _get_r
        return _get_r()
    except Exception:
        return None


def _sp_match_count(r, sport_slug: str) -> int:
    if not r:
        return 0
    for key in (f"odds:sp:upcoming:{sport_slug}", f"sp:upcoming:{sport_slug}"):
        try:
            raw = r.get(key)
            if not raw:
                continue
            obj = _json.loads(raw)
            if isinstance(obj, list):
                return len([m for m in obj if isinstance(m, dict)])
            if isinstance(obj, dict):
                ms = obj.get("matches") or obj.get("data") or []
                return len([m for m in ms if isinstance(m, dict)])
        except Exception:
            pass
    return 0


def _load_matches(sport: str) -> tuple[list[dict], bool]:
    """
    Load and enrich matches from the unified stream + SP Redis cache.
    Returns (matches_list, sp_available).
    """
    sp_available = False
    matches_raw: list[dict] = []
    _now_ts = time.time()

    # 1. Try unified patched stream
    try:
        from app.api.odds_stream import _get_unified_patched
        raw_up = _get_unified_patched("upcoming", sport, force_refresh=False)
        raw_lv = _get_unified_patched("live",     sport, force_refresh=False)
        seen   = set()
        live_jks: set[str] = set()
        for m in raw_lv:
            if not isinstance(m, dict):
                continue
            jk = m.get("join_key") or m.get("parent_match_id")
            if jk:
                live_jks.add(jk)
        for m in raw_lv:
            if not isinstance(m, dict):
                continue
            jk = m.get("join_key") or m.get("parent_match_id")
            if jk and jk not in seen:
                seen.add(jk)
                m.setdefault("_live", True)
                if m.get("arb_opportunities") and not m.get("arbitrage"):
                    m["arbitrage"] = m["arb_opportunities"]
                    m["has_arb"] = True
                matches_raw.append(m)
        for m in raw_up:
            if not isinstance(m, dict):
                continue
            jk = m.get("join_key") or m.get("parent_match_id")
            if jk in live_jks or (jk and jk in seen):
                continue
            st = m.get("start_time", "")
            if st:
                try:
                    st_dt = datetime.fromisoformat(st.replace("Z", "+00:00"))
                    if st_dt.tzinfo is None:
                        st_dt = st_dt.replace(tzinfo=timezone.utc)
                    if st_dt.timestamp() < _now_ts - 90:
                        continue
                except Exception:
                    pass
            if jk:
                seen.add(jk)
            if m.get("arb_opportunities") and not m.get("arbitrage"):
                m["arbitrage"] = m["arb_opportunities"]
                m["has_arb"] = True
            matches_raw.append(m)
    except Exception:
        pass

    # 2. SP enrichment
    r = _get_redis()
    sp_raw: list[dict] = []
    for key in (f"odds:sp:upcoming:{sport}", f"sp:upcoming:{sport}"):
        try:
            raw = r.get(key) if r else None
            if not raw:
                continue
            obj = _json.loads(raw)
            if isinstance(obj, list):
                sp_raw = [m for m in obj if isinstance(m, dict)]
            elif isinstance(obj, dict):
                ms = obj.get("matches") or obj.get("data") or []
                sp_raw = [m for m in ms if isinstance(m, dict)]
            if sp_raw:
                sp_available = True
            break
        except Exception:
            pass

    if not sp_available:
        # Trigger async SP harvest (non-blocking)
        try:
            from app.workers.tasks_upcoming import sp_harvest_sport
            sp_harvest_sport.delay(sport)
        except Exception:
            pass

    if sp_raw:
        try:
            from app.api.odds_stream import _normalise_markets, _get_price
            _sp_by_br: dict = {}
            _sp_by_name: dict = {}
            for sp_m in sp_raw:
                br = str(sp_m.get("betradar_id") or "").strip()
                if br:
                    _sp_by_br[br] = sp_m
                h = (sp_m.get("home_team") or "").lower().strip()[:10]
                a = (sp_m.get("away_team") or "").lower().strip()[:10]
                if h and a:
                    _sp_by_name[f"{h}|{a}"] = sp_m

            for m in matches_raw:
                br = str(m.get("betradar_id") or m.get("join_key", "").replace("br_", "") or "").strip()
                sp_m = _sp_by_br.get(br)
                if not sp_m:
                    h = (m.get("home_team") or "").lower().strip()[:10]
                    a = (m.get("away_team") or "").lower().strip()[:10]
                    sp_m = _sp_by_name.get(f"{h}|{a}") if (h and a) else None
                if sp_m is None:
                    continue
                if not m.get("sms_id") and sp_m.get("sms_id"):
                    m["sms_id"] = sp_m["sms_id"]
                if not m.get("sp_game_id") and sp_m.get("sp_game_id"):
                    m["sp_game_id"] = sp_m["sp_game_id"]
                sp_mkts = sp_m.get("markets") or {}
                if sp_mkts and m.get("best") is not None:
                    try:
                        norm = _normalise_markets(sp_mkts)
                        for mkt, outs in norm.items():
                            if not isinstance(outs, dict):
                                continue
                            m["best"].setdefault(mkt, {})
                            for out, p in outs.items():
                                price = _get_price(p)
                                if price > 1.0:
                                    existing = m["best"][mkt].get(out)
                                    if not existing or price > existing.get("odd", 0):
                                        m["best"][mkt][out] = {"odd": price, "bk": "sp"}
                    except Exception:
                        pass
        except Exception:
            pass

    # 3. Fallback to DB
    if not matches_raw:
        try:
            from app.api.odds_stream import _load_db_matches
            db_up, _, _ = _load_db_matches(sport, mode="upcoming", page=1, per_page=500)
            db_lv, _, _ = _load_db_matches(sport, mode="live",     page=1, per_page=100)
            matches_raw.extend(db_up + db_lv)
        except Exception:
            pass

    matches_raw.sort(key=lambda x: x.get("start_time") or "")
    return matches_raw, sp_available


def _filter_group(matches: list[dict], group_id: str,
                  date_str: Optional[str] = None) -> list[dict]:
    """Return only matches that fall in the given time group on date_str."""
    if group_id not in TIME_GROUPS:
        return []
    _, g_start, g_end = TIME_GROUPS[group_id]

    result = []
    for m in matches:
        dt = _match_dt(m)
        eat_dt = dt + EAT
        h = eat_dt.hour
        if not (g_start <= h < g_end):
            continue
        if date_str:
            if eat_dt.strftime("%Y-%m-%d") != date_str:
                continue
        result.append(m)
    return result


def _get_odd(val) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, dict):
        for f in ("price", "odd", "odds", "value"):
            if val.get(f):
                try:
                    return float(val[f])
                except Exception:
                    pass
    try:
        return float(val)
    except Exception:
        return None


def _best_for_out(m: dict, mkt_keys: list[str],
                  out_canon: str) -> tuple[Optional[float], str]:
    aliases = [a.lower() for a in _OUTCOME_ALIASES.get(out_canon, [out_canon.lower()])]
    best_odd: Optional[float] = None
    best_bk = ""
    for mkt_key in mkt_keys:
        mkt_data = (m.get("best") or {}).get(mkt_key) or {}
        for k, v in mkt_data.items():
            if str(k).lower() in aliases:
                fv = _get_odd(v.get("odd") if isinstance(v, dict) else v)
                bk = v.get("bk", "") if isinstance(v, dict) else ""
                if fv and (best_odd is None or fv > best_odd):
                    best_odd = fv
                    best_bk = bk
    return best_odd, best_bk


def _get_ids(m: dict) -> dict[str, str]:
    ids: dict[str, str] = {}
    for slug in ("sp", "bt", "od"):
        val = None
        if slug == "sp":
            val = m.get("sms_id") or m.get("sp_game_id")
        if not val:
            val = (m.get("bk_ids") or {}).get(slug)
        if val and str(val).isdigit() and len(str(val)) <= 8:
            ids[slug] = str(val)
    return ids


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def get_available_groups(sport: str, date_str: Optional[str] = None) -> list[dict]:
    """
    Return a list of time-group dicts with match counts for the given sport/date.
    Only groups that have ≥1 match are included.

    Each entry:
        { id, label, start_hour, end_hour, match_count }
    """
    if date_str is None:
        date_str = (datetime.now(timezone.utc) + EAT).strftime("%Y-%m-%d")

    matches, sp_available = _load_matches(sport)
    result = []
    for gid, (label, g_start, g_end) in TIME_GROUPS.items():
        grp_matches = _filter_group(matches, gid, date_str)
        if grp_matches:
            result.append({
                "id":          gid,
                "label":       label,
                "start_hour":  g_start,
                "end_hour":    g_end,
                "match_count": len(grp_matches),
                "sp_available": sp_available,
            })
    return result


def generate_group_document(
    sport: str,
    group_id: str,
    date_str: Optional[str] = None,
    market_filter: Optional[list[str]] = None,
) -> io.BytesIO:
    """
    Generate a compact landscape A4 Word document for a single time group.

    Parameters
    ──────────
    sport          – e.g. "soccer"
    group_id       – key from TIME_GROUPS
    date_str       – EAT date "YYYY-MM-DD" (default: today)
    market_filter  – list of short market IDs to include; None = all

    Returns
    ───────
    BytesIO containing the .docx file content.
    """
    from docx import Document
    from docx.shared import Pt, RGBColor, Cm
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.enum.section import WD_ORIENT
    from docx.oxml import OxmlElement, parse_xml
    from docx.oxml.ns import nsdecls, qn

    if date_str is None:
        date_str = (datetime.now(timezone.utc) + EAT).strftime("%Y-%m-%d")

    if group_id not in TIME_GROUPS:
        raise ValueError(f"Unknown group_id '{group_id}'. Valid: {list(TIME_GROUPS.keys())}")

    group_label, g_start, g_end = TIME_GROUPS[group_id]

    # ── Style constants ────────────────────────────────────────────────────────
    FF       = "Arial"
    RGB_W    = RGBColor(0xFF, 0xFF, 0xFF)
    RGB_TXT  = RGBColor(0x1E, 0x29, 0x3B)
    RGB_MUT  = RGBColor(0x64, 0x74, 0x8B)
    RGB_GRN  = RGBColor(0x15, 0x80, 0x3D)
    RGB_SKY  = RGBColor(0x7D, 0xD3, 0xFC)
    RGB_SP   = RGBColor(0x25, 0x63, 0xEB)
    RGB_BT   = RGBColor(0x16, 0xA3, 0x4A)
    RGB_OD   = RGBColor(0xD9, 0x77, 0x06)

    HEX_PRI    = "0F172A"
    HEX_ALT    = "F8FAFC"
    HEX_BEST   = "DCFCE7"
    HEX_BORDER = "CBD5E1"

    USABLE_W = Cm(27.9)

    # ── XML helpers ────────────────────────────────────────────────────────────
    def _shd(cell, hex_color: str):
        cell._tc.get_or_add_tcPr().append(
            parse_xml(f'<w:shd {nsdecls("w")} w:fill="{hex_color}"/>'))

    def _margins(cell, top=50, bottom=50, left=70, right=70):
        tcPr = cell._tc.get_or_add_tcPr()
        tcMar = OxmlElement("w:tcMar")
        for nm, v in [("top", top), ("bottom", bottom), ("left", left), ("right", right)]:
            n = OxmlElement(f"w:{nm}")
            n.set(qn("w:w"), str(v)); n.set(qn("w:type"), "dxa")
            tcMar.append(n)
        tcPr.append(tcMar)

    def _borders(cell, color=HEX_BORDER, sz="2"):
        tcPr = cell._tc.get_or_add_tcPr()
        tcB = OxmlElement("w:tcBorders")
        for nm in ("top", "left", "bottom", "right"):
            b = OxmlElement(f"w:{nm}")
            b.set(qn("w:val"), "single"); b.set(qn("w:sz"), sz)
            b.set(qn("w:space"), "0");    b.set(qn("w:color"), color)
            tcB.append(b)
        tcPr.append(tcB)

    def _no_borders(table):
        tblPr = table._tbl.tblPr
        tcB = OxmlElement("w:tblBorders")
        for nm in ("top", "left", "bottom", "right", "insideH", "insideV"):
            b = OxmlElement(f"w:{nm}"); b.set(qn("w:val"), "none")
            tcB.append(b)
        tblPr.append(tcB)

    def _ct(cell, text: str, bold=False, color=None, size=None, align=None):
        if len(cell.paragraphs) == 1 and not cell.paragraphs[0].text:
            p = cell.paragraphs[0]
        else:
            p = cell.add_paragraph()
        p.alignment = align or WD_ALIGN_PARAGRAPH.LEFT
        p.paragraph_format.space_after  = Pt(0)
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.line_spacing = 1.0
        r = p.add_run(text)
        r.bold = bold; r.font.name = FF
        if color: r.font.color.rgb = color
        if size:  r.font.size = size
        return p

    # ── Load + filter matches ──────────────────────────────────────────────────
    all_matches, sp_available = _load_matches(sport)
    matches = _filter_group(all_matches, group_id, date_str)
    matches.sort(key=lambda x: x.get("start_time") or "")

    # ── Document setup ─────────────────────────────────────────────────────────
    doc = Document()
    for sec in doc.sections:
        sec.orientation   = WD_ORIENT.LANDSCAPE
        sec.page_width    = Cm(29.7)
        sec.page_height   = Cm(21.0)
        sec.top_margin    = Cm(0.85)
        sec.bottom_margin = Cm(0.75)
        sec.left_margin   = Cm(0.9)
        sec.right_margin  = Cm(0.9)

    doc.styles["Normal"].font.name      = FF
    doc.styles["Normal"].font.size      = Pt(7.5)
    doc.styles["Normal"].font.color.rgb = RGB_TXT

    # ── Title bar ─────────────────────────────────────────────────────────────
    sport_emojis = {
        "soccer": "⚽", "basketball": "🏀", "tennis": "🎾",
        "ice-hockey": "🏒", "volleyball": "🏐", "cricket": "🏏",
        "rugby": "🏉", "table-tennis": "🏓", "handball": "🤾",
        "baseball": "⚾", "mma": "🥊", "boxing": "🥊",
        "darts": "🎯", "american-football": "🏈", "esoccer": "⚽",
    }
    s_emoji = sport_emojis.get(sport.lower(), "🏆")
    eat_now = datetime.now(timezone.utc) + EAT

    tb = doc.add_table(rows=1, cols=2)
    _no_borders(tb); tb.autofit = False
    tb.columns[0].width = Cm(19.0)
    tb.columns[1].width = Cm(8.9)
    tl, tr = tb.rows[0].cells[0], tb.rows[0].cells[1]
    _shd(tl, HEX_PRI); _shd(tr, HEX_PRI)
    _margins(tl, top=130, bottom=130, left=200, right=100)
    _margins(tr, top=130, bottom=130, left=100, right=200)

    p_t = tl.paragraphs[0]; p_t.paragraph_format.space_after = Pt(3); p_t.paragraph_format.line_spacing = 1.0
    r1 = p_t.add_run(f"{s_emoji} {sport.upper()}  "); r1.bold = True; r1.font.size = Pt(14); r1.font.color.rgb = RGB_W; r1.font.name = FF
    r2 = p_t.add_run("ODDS BOOKLET"); r2.bold = True; r2.font.size = Pt(14); r2.font.color.rgb = RGB_SKY; r2.font.name = FF
    p_s = tl.add_paragraph(); p_s.paragraph_format.space_after = Pt(0); p_s.paragraph_format.line_spacing = 1.0
    rs = p_s.add_run(f"🗓 {eat_now.strftime('%A, %d %B %Y')}   |   {group_label}   |   📋 {len(matches)} match{'es' if len(matches) != 1 else ''}")
    rs.font.size = Pt(7.5); rs.font.color.rgb = RGB_MUT; rs.font.name = FF

    p_l = tr.paragraphs[0]; p_l.alignment = WD_ALIGN_PARAGRAPH.RIGHT; p_l.paragraph_format.space_after = Pt(4); p_l.paragraph_format.line_spacing = 1.0
    for badge, bclr, lbl, sep in [("● SP", RGB_SP, " SportPesa", "   "),
                                    ("● BT", RGB_BT, " Betika", "   "),
                                    ("● OD", RGB_OD, " OdiBets", "")]:
        rb = p_l.add_run(badge); rb.bold = True; rb.font.color.rgb = bclr; rb.font.size = Pt(6.5); rb.font.name = FF
        rl = p_l.add_run(lbl + sep); rl.font.size = Pt(6.5); rl.font.color.rgb = RGB_W; rl.font.name = FF

    p_l2 = tr.add_paragraph(); p_l2.alignment = WD_ALIGN_PARAGRAPH.RIGHT; p_l2.paragraph_format.space_after = Pt(0); p_l2.paragraph_format.line_spacing = 1.0
    rl2 = p_l2.add_run("🟢 Green cell = best odd   |   IDs = bookmaker game codes")
    rl2.font.size = Pt(5.5); rl2.font.color.rgb = RGB_MUT; rl2.font.name = FF

    if not sp_available:
        p_sp = doc.add_paragraph(); p_sp.paragraph_format.space_after = Pt(2); p_sp.paragraph_format.space_before = Pt(2)
        r_sp = p_sp.add_run("⚠️  SportPesa data is currently unavailable — fetching in background. Please try again in 3–5 minutes.")
        r_sp.font.size = Pt(6.5); r_sp.font.color.rgb = RGBColor(0xFB, 0xBF, 0x24); r_sp.font.name = FF

    # Accent stripe
    p_acc = doc.add_paragraph(); p_acc.paragraph_format.space_after = Pt(6); p_acc.paragraph_format.space_before = Pt(0)
    r_acc = p_acc.add_run("▬" * 260)
    r_acc.font.size = Pt(1.5); r_acc.font.color.rgb = RGBColor(0x38, 0xBD, 0xF8); r_acc.font.name = FF

    if not matches:
        pn = doc.add_paragraph(); pn.alignment = WD_ALIGN_PARAGRAPH.CENTER
        pn.add_run(f"No matches found for {group_label} on {date_str}.").italic = True
        buf = io.BytesIO(); doc.save(buf); buf.seek(0)
        return buf

    # ── Decide which sections to render ───────────────────────────────────────
    # market_filter is a list of short IDs like ["1x2", "btts"] or None = all
    sections = [
        (fid, title, bk_keys, outcomes)
        for fid, title, bk_keys, outcomes in _MARKET_SECTIONS
        if (market_filter is None) or (fid in market_filter)
    ]

    # Also discover any dynamic O/U lines actually in the data
    if market_filter is None or any(f.startswith("ou") for f in (market_filter or [])):
        seen_ou: set = set()
        for m in matches:
            for k in (m.get("best") or {}):
                if k.startswith(("over_under_goals_", "over_under_")):
                    raw = k.replace("over_under_goals_", "").replace("over_under_", "")
                    static = {s[2:].replace("_", ".") for s in ["ou15", "ou25", "ou35", "ou45"]}
                    if raw.replace("_", ".") not in static and raw not in seen_ou:
                        seen_ou.add(raw)
        for raw in sorted(seen_ou):
            line = raw.replace("_", ".")
            fid = f"ou_{raw}"
            bk_keys = [f"over_under_goals_{raw}", f"over_under_{raw}"]
            if market_filter is None or fid in market_filter:
                sections.append((fid, f"📊 OVER / UNDER {line} GOALS", bk_keys, ["Over", "Under"]))

    # Remove duplicates (keep first)
    seen_fids: set = set()
    unique_sections = []
    for s in sections:
        if s[0] not in seen_fids:
            seen_fids.add(s[0])
            unique_sections.append(s)
    sections = unique_sections

    # Global numbering
    match_global_idx = {id(m): i + 1 for i, m in enumerate(matches)}

    # Column width helper
    def _col_widths(n_out: int) -> list[float]:
        num_w   = 0.55
        match_w = 5.5
        ko_w    = 1.4
        out_w   = 1.85 if n_out == 2 else 1.65
        bk_w    = 1.8
        ids_w   = max(27.9 - num_w - match_w - ko_w - out_w * n_out - bk_w, 2.0)
        return [num_w, match_w, ko_w] + [out_w] * n_out + [bk_w, ids_w]

    def _section_header(title_txt: str):
        sp_b = doc.add_paragraph(); sp_b.paragraph_format.space_after = Pt(0); sp_b.paragraph_format.space_before = Pt(10)
        ht = doc.add_table(rows=1, cols=1); ht.autofit = False; _no_borders(ht)
        ht.columns[0].width = USABLE_W
        hc = ht.rows[0].cells[0]; _shd(hc, HEX_PRI); _margins(hc, top=120, bottom=120, left=180, right=180)
        p_h = hc.paragraphs[0]; p_h.paragraph_format.line_spacing = 1.0
        rh = p_h.add_run(title_txt); rh.bold = True; rh.font.size = Pt(8.5); rh.font.color.rgb = RGB_W; rh.font.name = FF
        pa = doc.add_paragraph(); pa.paragraph_format.space_after = Pt(0); pa.paragraph_format.space_before = Pt(0)
        ra = pa.add_run("▬" * 260); ra.font.size = Pt(1.5); ra.font.color.rgb = RGBColor(0x38, 0xBD, 0xF8); ra.font.name = FF

    def _table_header(outcomes: list[str], col_ws: list[float]):
        n = len(col_ws)
        t = doc.add_table(rows=1, cols=n); t.autofit = False; _no_borders(t)
        for ci, w in enumerate(col_ws): t.columns[ci].width = Cm(w)
        hdr = t.rows[0]
        labels = ["#", "Home vs Away", "KO"] + outcomes + ["Best BK", "Game IDs"]
        aligns = [WD_ALIGN_PARAGRAPH.CENTER, WD_ALIGN_PARAGRAPH.LEFT, WD_ALIGN_PARAGRAPH.CENTER] \
                 + [WD_ALIGN_PARAGRAPH.CENTER] * (len(outcomes) + 1) \
                 + [WD_ALIGN_PARAGRAPH.LEFT]
        for ci, (lbl, aln) in enumerate(zip(labels, aligns)):
            c = hdr.cells[ci]; _shd(c, "1E293B"); _margins(c, top=75, bottom=75, left=55, right=55)
            _ct(c, lbl, bold=True, color=RGB_SKY, size=Pt(7), align=aln)
        return t

    # ── Render each market section ─────────────────────────────────────────────
    for _fid, sec_title, mkt_keys, outcomes in sections:
        sec_matches = [m for m in matches if any((m.get("best") or {}).get(k) for k in mkt_keys)]
        if not sec_matches:
            continue

        _section_header(sec_title)
        col_ws = _col_widths(len(outcomes))
        curr_tbl = _table_header(outcomes, col_ws)
        row_idx = 0

        for m in sec_matches:
            h_team = (m.get("home_team") or "Home")[:22]
            a_team = (m.get("away_team") or "Away")[:22]
            m_dt = _match_dt(m)
            ko_str = _eat_time(m_dt)
            ids = _get_ids(m)
            ids_str = "  ".join(f"{s.upper()}#{v}" for s, v in ids.items())

            best_outs = [_best_for_out(m, mkt_keys, out) for out in outcomes]
            max_odd = max((o for o, _ in best_outs if o), default=None)
            bk_set: list[str] = []
            bk_seen: set = set()
            for _, bk in best_outs:
                if bk and bk.upper() not in bk_seen:
                    bk_seen.add(bk.upper()); bk_set.append(bk.upper())
            bk_summary = " / ".join(bk_set) or "—"

            dr = curr_tbl.add_row()
            bg_hex = HEX_ALT if row_idx % 2 == 1 else "FFFFFF"
            for ci2 in range(len(col_ws)):
                c2 = dr.cells[ci2]; _shd(c2, bg_hex)
                _margins(c2, top=40, bottom=40, left=55, right=55)
                _borders(c2, HEX_BORDER, "2")

            col_i = 0
            gn = match_global_idx.get(id(m), 0)
            _ct(dr.cells[col_i], str(gn), color=RGB_MUT, size=Pt(6.5), align=WD_ALIGN_PARAGRAPH.CENTER); col_i += 1

            mc = dr.cells[col_i]; col_i += 1
            pm = mc.paragraphs[0]; pm.paragraph_format.line_spacing = 1.0
            rh2 = pm.add_run(h_team); rh2.bold = True; rh2.font.size = Pt(7.5); rh2.font.color.rgb = RGB_TXT; rh2.font.name = FF
            rv2 = pm.add_run("  v  "); rv2.font.size = Pt(6.5); rv2.font.color.rgb = RGB_MUT; rv2.font.name = FF
            ra2 = pm.add_run(a_team); ra2.font.size = Pt(7.5); ra2.font.color.rgb = RGB_TXT; ra2.font.name = FF

            _ct(dr.cells[col_i], ko_str, bold=True, color=RGB_TXT, size=Pt(7.5), align=WD_ALIGN_PARAGRAPH.CENTER); col_i += 1

            for b_odd, b_bk in best_outs:
                oc = dr.cells[col_i]; col_i += 1
                if b_odd and b_odd > 1.0:
                    is_max = (b_odd == max_odd)
                    if is_max: _shd(oc, HEX_BEST)
                    po = oc.paragraphs[0]; po.alignment = WD_ALIGN_PARAGRAPH.CENTER; po.paragraph_format.line_spacing = 1.0
                    rv3 = po.add_run(f"{b_odd:.2f}"); rv3.bold = is_max; rv3.font.size = Pt(7.5)
                    rv3.font.color.rgb = RGB_GRN if is_max else RGB_TXT; rv3.font.name = FF
                    if b_bk:
                        bk_clr = RGB_SP if b_bk.lower() == "sp" else (RGB_BT if b_bk.lower() == "bt" else RGB_OD)
                        rb5 = po.add_run(f"\n{b_bk.upper()}"); rb5.font.size = Pt(5.5); rb5.font.color.rgb = bk_clr; rb5.font.name = FF
                else:
                    _ct(oc, "—", color=RGB_MUT, size=Pt(6.5), align=WD_ALIGN_PARAGRAPH.CENTER)

            bk_clr_main = RGB_SP if "SP" in bk_set else (RGB_BT if "BT" in bk_set else RGB_OD)
            _ct(dr.cells[col_i], bk_summary, bold=True, color=bk_clr_main, size=Pt(6.5), align=WD_ALIGN_PARAGRAPH.CENTER); col_i += 1
            _ct(dr.cells[col_i], ids_str, color=RGB_MUT, size=Pt(6.5)); col_i += 1
            row_idx += 1

        doc.add_paragraph().paragraph_format.space_after = Pt(6)

    # ── Footer ─────────────────────────────────────────────────────────────────
    pf = doc.add_paragraph(); pf.alignment = WD_ALIGN_PARAGRAPH.CENTER; pf.paragraph_format.space_before = Pt(14)
    rf = pf.add_run(
        f"📊 OddsKenya  |  {group_label}  |  {date_str}  |  "
        f"Generated {eat_now.strftime('%H:%M')} EAT  |  Verify odds before placing bets."
    )
    rf.font.size = Pt(6.5); rf.italic = True; rf.font.color.rgb = RGB_MUT; rf.font.name = FF

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf


# ─────────────────────────────────────────────────────────────────────────────
# MinIO helpers — pre-generate and cache per group per sport
# ─────────────────────────────────────────────────────────────────────────────

def save_group_to_minio(sport: str, group_id: str,
                         date_str: str, buf: io.BytesIO) -> bool:
    """
    Save a group document to MinIO.
    Key: odds-reports/v2/{sport}/{date}/{group_id}.docx
    Returns True on success.
    """
    try:
        from app.views.customer.routes_api import _get_minio_client
        client, bucket = _get_minio_client()
        if not client:
            return False
        from minio.error import S3Error
        object_name = f"odds-reports/v2/{sport}/{date_str}/{group_id}.docx"
        data = buf.read()
        import io as _io
        client.put_object(
            bucket, object_name,
            _io.BytesIO(data), len(data),
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        buf.seek(0)
        return True
    except Exception:
        return False


def load_group_from_minio(sport: str, group_id: str, date_str: str) -> Optional[io.BytesIO]:
    """
    Load a previously cached group document from MinIO.
    Returns BytesIO on hit, None on miss.
    """
    try:
        from app.views.customer.routes_api import _get_minio_client
        client, bucket = _get_minio_client()
        if not client:
            return None
        object_name = f"odds-reports/v2/{sport}/{date_str}/{group_id}.docx"
        response = client.get_object(bucket, object_name)
        data = response.read()
        response.close(); response.release_conn()
        return io.BytesIO(data)
    except Exception:
        return None
