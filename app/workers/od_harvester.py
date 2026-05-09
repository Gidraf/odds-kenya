"""
app/workers/od_harvester.py
============================
OdiBets upcoming + live harvester.
Uses unified OdiBets mappers (odibet_mappers.py) for market canonicalisation.
- 1X2 market is always captured (sub_type_id=1).
- All other markets are mapped via get_od_market_info.
- Concurrent sub‑type fetching merges all markets without overwriting.
"""


import re
import logging
from typing import Any, Dict, Optional, Tuple

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as _date, timedelta
from typing import Any, Generator

import httpx

from app.utils.mapping.odibets import get_od_market_info
from app.workers.mappers.shared import normalize_outcome   # <-- NEW

logger = logging.getLogger(__name__)

log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# SPORT SLUG MAP
# ══════════════════════════════════════════════════════════════════════════════

_OD_ID_TO_SLUG: dict[int, dict[str, str]] = {
    # ── Core soccer markets seen in production ────────────────────────────────
    1:   {"soccer": "1x2",               "basketball": "1x2"},
    2:   {"soccer": "double_chance"},
    3:   {"soccer": "draw_no_bet"},
    4:   {"soccer": "btts"},
    5:   {"soccer": "first_team_to_score"},
    6:   {"soccer": "draw_no_bet"},
    7:   {"soccer": "correct_score"},
    8:   {"soccer": "half_time_result"},
    9:   {"soccer": "half_time_result"},
    10:  {"soccer": "double_chance"},          # outcomes: 12, 1X, X2
    11:  {"soccer": "draw_no_bet"},            # outcomes: 1, 2  (no draw)
    12:  {"soccer": "draw_no_bet"},            # outcomes: 2, X
    13:  {"soccer": "draw_no_bet"},            # outcomes: 1, X
    14:  {"soccer": "european_handicap"},      # big handicap
    15:  {"soccer": "winning_margin"},         # 1_by_1, 1_by_2, X, 2_by_1...
    16:  {"soccer": "draw_no_bet"},            # 1, 2 only (asian-style no draw)
    17:  {"soccer": "btts"},
    18:  {"soccer": "first_half_btts"},
    19:  {"soccer": "over_under_goals"},       # line in specifier
    20:  {"soccer": "over_under_goals"},
    21:  {"soccer": "exact_goals"},            # 0,1,2,3,4,5,6+
    22:  {"soccer": "half_time_full_time"},
    23:  {"soccer": "exact_goals"},            # team-specific 0,1,2,3+
    24:  {"soccer": "exact_goals"},            # team-specific 0,1,2,3+
    25:  {"soccer": "over_under_goals"},
    26:  {"soccer": "odd_even_goals"},
    27:  {"soccer": "odd_even_goals"},
    28:  {"soccer": "odd_even_goals"},
    29:  {"soccer": "btts"},                   # yes/no (same as gg/ng)
    30:  {"soccer": "which_team_to_score"},    # both_teams/none/only_1/only_2
    31:  {"soccer": "team_to_score"},          # yes/no
    32:  {"soccer": "team_to_score"},
    33:  {"soccer": "team_to_score"},
    34:  {"soccer": "team_to_score"},
    35:  {"soccer": "1x2_btts"},              # 1_yes/1_no/x_yes/x_no/2_yes/2_no
    36:  {"soccer": "over_under_btts_2_5"},   # over_2.5_yes/no
    37:  {"soccer": "first_goalscorer"},
    38:  {"soccer": "anytime_goalscorer"},
    39:  {"soccer": "last_goalscorer"},
    40:  {"soccer": "first_half_correct_score"},
    41:  {"soccer": "correct_score"},         # full match scorelines
    42:  {"soccer": "double_chance"},
    43:  {"soccer": "draw_no_bet"},
    44:  {"soccer": "over_under_goals"},
    45:  {"soccer": "correct_score"},
    46:  {"soccer": "ht_correct_score_combo"}, # ht_score_ft_score combos
    47:  {"soccer": "half_time_full_time"},    # 1/1, x/x, 2/2 format
    48:  {"soccer": "team_to_score"},
    49:  {"soccer": "team_to_score"},
    50:  {"soccer": "btts"},
    51:  {"soccer": "team_to_score"},
    52:  {"soccer": "highest_scoring_half"},   # 1st_half/2nd_half/equal
    53:  {"soccer": "highest_scoring_half"},
    54:  {"soccer": "highest_scoring_half"},
    55:  {"soccer": "btts_both_halves"},       # no/no, yes/no, yes/yes, no/yes
    56:  {"soccer": "team_clean_sheet"},
    57:  {"soccer": "team_clean_sheet"},
    58:  {"soccer": "team_to_win_both_halves"},
    59:  {"soccer": "team_to_score_in_both_halves"},
    60:  {"soccer": "second_half_1x2"},
    61:  {"soccer": "first_half_1x2"},
    62:  {"soccer": "draw_no_bet"},
    63:  {"soccer": "first_half_double_chance"},
    64:  {"soccer": "first_half_draw_no_bet"},
    65:  {"soccer": "european_handicap"},
    66:  {"soccer": "asian_handicap"},
    67:  {"soccer": "asian_handicap"},
    68:  {"soccer": "asian_handicap"},
    69:  {"soccer": "over_under_goals"},
    70:  {"soccer": "over_under_goals"},
    71:  {"soccer": "exact_goals"},            # 0,1,2,3+,2+
    72:  {"soccer": "exact_goals"},
    73:  {"soccer": "exact_goals"},
    74:  {"soccer": "odd_even_goals"},
    75:  {"soccer": "team_to_score"},
    76:  {"soccer": "btts"},
    77:  {"soccer": "btts"},
    78:  {"soccer": "first_half_1x2_btts"},
    79:  {"soccer": "result_and_over_under_1_5"},
    80:  {"soccer": "result_and_over_under"},
    81:  {"soccer": "first_half_correct_score"},
    82:  {"soccer": "first_half_correct_score"},
    83:  {"soccer": "second_half_1x2"},
    84:  {"soccer": "second_half_1x2"},
    85:  {"soccer": "second_half_double_chance"},
    86:  {"soccer": "second_half_draw_no_bet"},
    87:  {"soccer": "european_handicap"},
    88:  {"soccer": "asian_handicap"},
    89:  {"soccer": "asian_handicap"},
    90:  {"soccer": "over_under_goals"},
    91:  {"soccer": "over_under_goals"},
    92:  {"soccer": "over_under_goals"},
    93:  {"soccer": "exact_goals"},            # 0, 1, 2+
    94:  {"soccer": "odd_even_goals"},
    95:  {"soccer": "team_to_score"},
    96:  {"soccer": "team_to_score"},
    97:  {"soccer": "team_to_score"},
    98:  {"soccer": "first_half_correct_score"},
    99:  {"soccer": "first_half_correct_score"},
    100: {"soccer": "over_under_goals"},
    101: {"soccer": "over_under_goals"},
    102: {"soccer": "over_under_goals"},
    103: {"soccer": "over_under_goals"},
    104: {"soccer": "over_under_goals"},
    105: {"soccer": "over_under_goals"},
    106: {"soccer": "over_under_goals"},
    107: {"soccer": "over_under_goals"},
    108: {"soccer": "asian_handicap"},
    109: {"soccer": "asian_handicap"},
    110: {"soccer": "asian_handicap"},
    111: {"soccer": "correct_score"},
    112: {"soccer": "half_time_result"},
    113: {"soccer": "first_half_over_under"},
    114: {"soccer": "first_half_over_under"},
    115: {"soccer": "first_half_asian_handicap"},
    116: {"soccer": "first_half_asian_handicap"},
    117: {"soccer": "team_total_goals_home"},
    118: {"soccer": "team_total_goals_away"},
    119: {"soccer": "result_and_btts"},
    120: {"soccer": "result_and_over_under"},
    121: {"soccer": "double_chance_and_btts"},
    122: {"soccer": "draw_no_bet"},
    123: {"soccer": "european_handicap"},
    124: {"soccer": "european_handicap"},
    125: {"soccer": "multigoals"},
    126: {"soccer": "exact_goals"},
    127: {"soccer": "winning_margin"},
    128: {"soccer": "half_time_full_time"},
    129: {"soccer": "correct_score"},
    130: {"soccer": "over_under_goals"},
    131: {"soccer": "over_under_goals"},
    132: {"soccer": "over_under_goals"},
    133: {"soccer": "over_under_goals"},
    134: {"soccer": "over_under_goals"},
    135: {"soccer": "over_under_goals"},
    136: {"soccer": "over_under_goals"},
    137: {"soccer": "over_under_goals"},
    138: {"soccer": "over_under_goals"},
    139: {"soccer": "over_under_goals"},
    140: {"soccer": "exact_goals"},
    141: {"soccer": "btts"},
    142: {"soccer": "first_half_btts"},
    143: {"soccer": "result_and_btts"},
    144: {"soccer": "result_and_over_under"},
    145: {"soccer": "double_chance_and_over_under"},
    146: {"soccer": "double_chance_and_btts"},
    147: {"soccer": "odd_even_goals"},
    148: {"soccer": "anytime_goalscorer"},
    149: {"soccer": "anytime_goalscorer", "basketball": "match_winner"},
    150: {"soccer": "first_goalscorer",   "basketball": "over_under"},
    151: {"soccer": "last_goalscorer"},
    152: {"soccer": "asian_handicap"},
    153: {"soccer": "asian_handicap"},
    154: {"soccer": "european_handicap"},
    155: {"soccer": "first_half_1x2",     "basketball": "point_spread"},
    156: {"soccer": "first_half_double_chance", "basketball": "asian_handicap"},
    157: {"soccer": "first_half_over_under",    "basketball": "total_points"},
    158: {"soccer": "first_half_btts"},
    159: {"soccer": "first_half_correct_score"},
    160: {"soccer": "second_half_1x2",    "basketball": "quarter_winner"},
    161: {"soccer": "second_half_over_under", "basketball": "over_under"},
    162: {"soccer": "correct_score"},
    163: {"soccer": "exact_goals"},
    164: {"soccer": "winning_margin"},
    165: {"soccer": "multigoals"},
    170: {"soccer": "anytime_goalscorer"},
    180: {"soccer": "first_goalscorer"},
    184: {"soccer": "over_under_goals"},
    190: {"soccer": "btts_and_over_under"},
    200: {"soccer": "result_and_over_under"},
    210: {"soccer": "draw_no_bet"},
    220: {"soccer": "correct_score"},
    # ── Combo markets (540-553) ───────────────────────────────────────────────
    540: {"soccer": "double_chance_btts"},
    541: {"soccer": "double_chance_btts"},
    542: {"soccer": "double_chance_btts"},
    543: {"soccer": "1x2_btts"},
    544: {"soccer": "result_and_over_under_1_5"},
    545: {"soccer": "double_chance_over_under_2_5"},
    546: {"soccer": "double_chance_btts"},
    547: {"soccer": "double_chance_over_under"},
    548: {"soccer": "multigoals"},
    549: {"soccer": "multigoals"},
    550: {"soccer": "multigoals"},
    551: {"soccer": "multigoals"},
    552: {"soccer": "multigoals"},
    553: {"soccer": "multigoals"},
    # ── HT/FT + Over/Under combos ─────────────────────────────────────────────
    818: {"soccer": "ht_ft_over_under"},
    819: {"soccer": "ht_ft_over_under"},
    820: {"soccer": "ht_ft_exact_goals"},
    856: {"soccer": "team_to_score"},
    858: {"soccer": "team_to_score"},
    859: {"soccer": "team_to_score"},
    861: {"soccer": "team_to_score"},
    879: {"soccer": "team_to_score"},
    880: {"soccer": "team_to_score"},
    881: {"soccer": "team_to_score"},
    # ── Basketball ─────────────────────────────────────────────────────────────
    300: {"basketball": "match_winner"},
    301: {"basketball": "1x2"},
    302: {"basketball": "draw_no_bet"},
    303: {"soccer": "match_winner",   "basketball": "match_winner"},
    304: {"soccer": "match_winner",   "basketball": "total_points"},
    305: {"basketball": "asian_handicap"},
    306: {"basketball": "total_points"},
    307: {"basketball": "first_half_1x2"},
    308: {"basketball": "quarter_winner"},
    309: {"basketball": "quarter_total"},
    310: {"basketball": "moneyline"},
    311: {"basketball": "asian_handicap"},
    312: {"basketball": "total_points"},
    313: {"basketball": "team_total"},
    314: {"basketball": "quarter_asian_handicap"},
    315: {"basketball": "1x2"},
    316: {"basketball": "over_under"},
    317: {"basketball": "first_half_asian_handicap"},
    318: {"basketball": "first_half_total"},
}
 
 
def lookup_by_id(sid: int, sport: str) -> Optional[str]:
    """Look up canonical slug by integer sub_type_id."""
    entry = _OD_ID_TO_SLUG.get(sid)
    if not entry:
        return None
    return entry.get(sport) or list(entry.values())[0]
 
 
# ─── PATCHED _make_unique_slug ────────────────────────────────────────────────
 
def make_unique_slug(sport: str, raw_slug: str, specifiers: str, sid: str = "") -> str:
    """
    Resolve an OdiBets market to a canonical slug.
 
    Resolution order:
      1. Integer sub_type_id → _OD_ID_TO_SLUG (fastest, most reliable)
      2. Name-based mapper via get_od_market_info (for named markets)
      3. Heuristic from raw slug text
      4. Fallback: sanitised raw_slug
 
    Args:
        sport:      canonical sport slug e.g. "soccer"
        raw_slug:   odds_type name from API e.g. "Double Chance" or "soccer_unknown_10"
        specifiers: specifier string e.g. "total=2.5"
        sid:        sub_type_id string e.g. "10"
    """
    # ── 1. Integer ID lookup (covers ~90% of unknown markets) ─────────────────
    if sid and sid.isdigit():
        base = lookup_by_id(int(sid), sport)
        if base:
            line = _extract_line(specifiers)
            if line and "over_under" in base:
                return f"{base}_{line.replace('.', '_')}"
            return base
 
    # ── 2. Name-based mapper ───────────────────────────────────────────────────
    if raw_slug and not raw_slug.startswith(f"{sport}_unknown_"):
        try:
            from app.utils.mapping.odibets import get_od_market_info
            info = get_od_market_info(sport, raw_slug)
            if info:
                canon_slug, spec_dict = info
                spec_str = ",".join(f"{k}={v}" for k, v in sorted(spec_dict.items()))
                return f"{canon_slug}|{spec_str}" if spec_str else canon_slug
        except Exception as exc:
            log.debug("get_od_market_info error sport=%s slug=%s: %s", sport, raw_slug, exc)
 
        # ── 3. Quick heuristic on name ─────────────────────────────────────────
        heuristic = _name_heuristic(raw_slug, sport)
        if heuristic:
            return heuristic
 
    # ── 4. Sanitise fallback ───────────────────────────────────────────────────
    clean = raw_slug.lower().replace(" ", "_").replace("-", "_").replace("/", "_")
    clean = re.sub(r"[^a-z0-9_]", "", clean).strip("_")
    return clean or f"{sport}_unknown_{sid or 'x'}"
 
 
def _extract_line(spec_str: str) -> str:
    if not spec_str:
        return ""
    m = re.search(r"total=([\d.]+)", spec_str)
    if m: return m.group(1)
    m = re.search(r"([\d]+\.[\d]+)", spec_str)
    if m: return m.group(1)
    return ""
 
 
def _name_heuristic(name: str, sport: str) -> Optional[str]:
    """Map raw API market names to canonical slugs without a full mapper."""
    n = name.lower().strip().replace(" ", "_").replace("/", "_")
    MAP = {
        "1x2":             "1x2",
        "double_chance":   "double_chance",
        "draw_no_bet":     "draw_no_bet",
        "both_teams_to_score": "btts",
        "btts":            "btts",
        "gg_ng":           "btts",
        "gg/ng":           "btts",
        "over_under":      "over_under_goals",
        "total_goals":     "over_under_goals",
        "correct_score":   "correct_score",
        "half_time_result": "half_time_result",
        "ht_ft":           "half_time_full_time",
        "half_time_full_time": "half_time_full_time",
        "exact_goals":     "exact_goals",
        "odd_even":        "odd_even_goals",
        "asian_handicap":  "asian_handicap",
        "european_handicap": "european_handicap",
        "first_goalscorer": "first_goalscorer",
        "anytime_goalscorer": "anytime_goalscorer",
        "highest_scoring_half": "highest_scoring_half",
        "winning_margin":  "winning_margin",
        "multigoals":      "multigoals",
        "1st_half_1x2":    "first_half_1x2",
        "first_half_1x2":  "first_half_1x2",
        "1st_half_btts":   "first_half_btts",
        "1st_half_over_under": "first_half_over_under",
        "result_both_teams_to_score": "result_and_btts",
        "result_total_goals": "result_and_over_under",
        "double_chance_total": "double_chance_and_over_under",
        "double_chance_btts": "double_chance_and_btts",
        "clean_sheet":     "team_clean_sheet",
        "to_score":        "team_to_score",
        "which_team_to_score": "which_team_to_score",
    }
    return MAP.get(n)
 
 
# ─── Unified normalize_outcome ────────────────────────────────────────────────
# Uses shared.py's normalize_outcome as the single source of truth.
# Both bt_harvester and od_harvester should import from here or from shared.py.
 
def normalize_outcome_unified(market_slug: str, raw_key: str, display: str = "") -> str:
    """
    Single normalize_outcome for all harvesters.
    Prefers shared.py's implementation (used by bt_harvester).
    Falls back to inline logic if shared.py unavailable.
    """
    try:
        from app.workers.mappers.shared import normalize_outcome as _shared_norm
        return _shared_norm(market_slug, raw_key, display)
    except ImportError:
        pass
 
    # Inline fallback (mirrors shared.py logic)
    key = raw_key.strip()
    kl  = key.lower()
    _MAP = {
        "1": "1", "home": "1", "x": "X", "draw": "X", "2": "2", "away": "2",
        "yes": "Yes", "no": "No", "over": "Over", "under": "Under",
        "ov": "Over", "un": "Under", "odd": "Odd", "even": "Even",
        "1x": "1X", "x2": "X2", "12": "12",
        "home_or_draw": "1X", "draw_or_away": "X2", "home_or_away": "12",
    }
    if kl in _MAP: return _MAP[kl]
    if re.match(r"^\d+:\d+$", key): return key
    if re.match(r"^\d+\+?$", key): return key
    if re.match(r"^[12xX]/[12xX]$", key): return key
    if "_" in kl and re.match(r"^[a-z][a-z_\-]{2,}$", kl):
        NON_PLAYER = {"no_goal","none","own_goal","home_win","away_win",
                      "both_teams","only_1","only_2","no_goalscorer"}
        if kl not in NON_PLAYER:
            parts = key.split("_")
            if all(p.isalpha() for p in parts) and len(parts) >= 2:
                return " ".join(p.capitalize() for p in parts)
    return re.sub(r"[^a-zA-Z0-9_:+./\-]+", "_", key).strip("_") or key

OD_SPORT_IDS: dict[str, str] = {
    "soccer":            "soccer",
    "basketball":        "basketball",
    "tennis":            "tennis",
    "ice-hockey":        "ice-hockey",
    "rugby":             "rugby",
    "handball":          "handball",
    "table-tennis":      "table-tennis",
    "cricket":           "cricket",
    "volleyball":        "volleyball",
    "baseball":          "baseball",
    "american-football": "american-football",
    "mma":               "mma",
    "boxing":            "boxing",
    "darts":             "darts",
    "esoccer":           "esoccer",
}

_SLUG_FALLBACKS: dict[str, list[str]] = {
    "american-football": ["americanfootball", "american_football", "nfl", "football", "11"],
    "mma":               ["mixed-martial-arts", "mixedmartialarts", "ufc", "117"],
    "table-tennis":      ["tabletennis", "table_tennis", "20"],
    "ice-hockey":        ["icehockey", "ice_hockey", "hockey", "4"],
    "darts":             ["22"],
    "boxing":            ["10"],
    "handball":          ["6"],
    "volleyball":        ["23"],
    "baseball":          ["3"],
    "rugby":             ["12"],
    "cricket":           ["21"],
}

_NUMERIC_TO_SLUG: dict[str, str] = {
    "1": "soccer",  "2": "basketball", "5": "tennis",  "4": "ice-hockey",
    "12": "rugby",  "6": "handball",   "20": "table-tennis", "21": "cricket",
    "23": "volleyball", "3": "baseball", "11": "american-football",
    "117": "mma",   "10": "boxing",    "22": "darts", "137": "esoccer",
}

def slug_to_od_sport_id(slug: str) -> str:
    return OD_SPORT_IDS.get(slug, slug)

def _resolve_sport(raw: Any, fallback: str) -> str:
    if raw is None:
        return fallback
    s = str(raw).lower().strip()
    if s in OD_SPORT_IDS:
        return s
    return _NUMERIC_TO_SLUG.get(s, fallback)


# ══════════════════════════════════════════════════════════════════════════════
# API + HTTP
# ══════════════════════════════════════════════════════════════════════════════

SBOOK_V1 = "https://api.odi.site/sportsbook/v1"

HEADERS: dict[str, str] = {
    "accept":             "application/json, text/plain, */*",
    "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "authorization":      "Bearer",
    "content-type":       "application/json",
    "origin":             "https://odibets.com",
    "referer":            "https://odibets.com/",
    "user-agent": (
        "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36"
    ),
    "sec-ch-ua":          '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile":   "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "cross-site",
}

_POOL_LIMITS = httpx.Limits(max_connections=150, max_keepalive_connections=60, keepalive_expiry=30.0)
_shared_client: httpx.Client | None = None
_client_lock = threading.Lock()

def _get_client() -> httpx.Client:
    global _shared_client
    if _shared_client is None:
        with _client_lock:
            if _shared_client is None:
                _shared_client = httpx.Client(headers=HEADERS, timeout=20.0, limits=_POOL_LIMITS)
    return _shared_client

_REQUEST_SEMAPHORE = threading.Semaphore(60)

def configure_concurrency(max_concurrent: int = 60) -> None:
    global _REQUEST_SEMAPHORE
    _REQUEST_SEMAPHORE = threading.Semaphore(max_concurrent)


def _get(url: str, params: dict | None = None, timeout: float = 20.0) -> dict | list | None:
    client = _get_client()
    for attempt in range(3):
        with _REQUEST_SEMAPHORE:
            try:
                r = client.get(url, params=params, timeout=timeout)
                if r.status_code in (429, 503):
                    wait = float(r.headers.get("Retry-After", 2 ** (attempt + 1)))
                    logger.warning("OD rate-limited (%s) – waiting %.1fs", r.status_code, wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                logger.warning("OD HTTP %s %s (attempt %d)", e.response.status_code, url, attempt + 1)
            except httpx.TimeoutException:
                logger.warning("OD timeout %s (attempt %d)", url, attempt + 1)
            except Exception as e:
                logger.warning("OD error %s (attempt %d): %s", url, attempt + 1, e)
        if attempt < 2:
            time.sleep(0.5 * (attempt + 1))
    return None


# ══════════════════════════════════════════════════════════════════════════════
# RESPONSE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _inner(resp: dict | list) -> dict:
    return resp.get("data") if isinstance(resp, dict) and isinstance(resp.get("data"), dict) else {}

def _unwrap(resp: dict | list) -> tuple[list[dict], dict]:
    d = _inner(resp)
    matches = d.get("matches") or []
    return (matches if isinstance(matches, list) else []), (d.get("meta") or {})

def _competitions_from_resp(resp: dict | list) -> list[dict]:
    return _inner(resp).get("competitions") or []

def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v or default)
    except (TypeError, ValueError):
        return default

def _unwrap_live(resp: dict | list) -> list[dict]:
    if isinstance(resp, list): return resp
    d = _inner(resp)
    for k in ("matches", "events", "live", "results"):
        if isinstance(d.get(k), list) and d[k]: return d[k]
    if isinstance(resp, dict):
        for k in ("matches", "events", "live", "results", "data"):
            if isinstance(resp.get(k), list) and resp[k]: return resp[k]
    return []


# ══════════════════════════════════════════════════════════════════════════════
# BASE PARAMS BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _base(sport_id: str, day: str = "", competition_id: str = "", page: int = 1) -> dict:
    return {
        "resource":       "sport",
        "sport_id":       sport_id,
        "sportsbook":     "sportsbook",
        "ua":             HEADERS["user-agent"],
        "day":            day,
        "competition_id": competition_id,
        "sub_type_id":    "",
        "hour":           "",
        "day_tmp":        "",
        "country_id":     "",
        "sort_by":        "",
        "filter":         "",
        "cs":             "",
        "hs":             "",
        "page":           page,
        "per_page":       200,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SLUG PROBE
# ══════════════════════════════════════════════════════════════════════════════

_probed: dict[str, str] = {}

def _probe(slug: str) -> str:
    if slug in _probed:
        return _probed[slug]

    primary    = OD_SPORT_IDS.get(slug, slug)
    candidates = [primary] + _SLUG_FALLBACKS.get(slug, [])
    today      = _date.today().isoformat()

    for candidate in candidates:
        data = _get(SBOOK_V1, params=_base(candidate, day=today))
        if data:
            matches, meta = _unwrap(data)
            if matches or _safe_int(meta.get("total")) > 0:
                _cache_probe(slug, candidate)
                return candidate

    for candidate in candidates:
        data = _get(SBOOK_V1, params=_base(candidate))
        if data:
            matches, meta = _unwrap(data)
            comps = _competitions_from_resp(data)
            if matches or comps or _safe_int(meta.get("total")) > 0:
                _cache_probe(slug, candidate)
                return candidate

    logger.warning("OD probe: no working slug for '%s' (tried: %s)", slug, candidates)
    _probed[slug] = primary
    return primary

def _cache_probe(slug: str, working: str) -> None:
    _probed[slug] = working
    if working != OD_SPORT_IDS.get(slug, slug):
        logger.info("OD probe: '%s' → resolved to '%s'", slug, working)
        OD_SPORT_IDS[slug] = working


# ══════════════════════════════════════════════════════════════════════════════
# MARKET PARSING – using the unified OdiBets mapper and shared outcome normaliser
# ══════════════════════════════════════════════════════════════════════════════

def _parse_specifiers(s: str) -> dict:
    if not s: return {}
    out = {}
    for part in str(s).split("|"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out

def _outcomes(mkt: dict) -> list[tuple[dict, str]]:
    ms = str(mkt.get("specifiers") or "").strip()
    results: list[tuple[dict, str]] = []
    for o in (mkt.get("outcomes") or []):
        if isinstance(o, dict):
            results.append((o, ms or str(o.get("specifiers") or "").strip()))
    if results: return results
    for line in (mkt.get("lines") or []):
        if not isinstance(line, dict): continue
        ls = str(line.get("specifiers") or "").strip()
        for o in (line.get("outcomes") or []):
            if isinstance(o, dict):
                results.append((o, ls or str(o.get("specifiers") or "").strip()))
    if results: return results
    for o in (mkt.get("odds") or []):
        if isinstance(o, dict):
            results.append((o, ms or str(o.get("special_bet_value") or o.get("specifiers") or "").strip()))
    return results

def _translate(ds: str, home: str, away: str) -> str:
    """Kept only for the 1X2 special case."""
    ds = ds.lower().strip()
    if not ds or ds in ("1", "x", "2", "yes", "no", "over", "under", "odd", "even", "none"):
        return ds
    for src, tgt in sorted([
        (home.lower().replace(" ", "_"), "1"), (away.lower().replace(" ", "_"), "2"),
        (home.lower(), "1"), (away.lower(), "2"), ("draw", "X"),
    ], key=lambda x: -len(x[0])):
        if len(src) > 2 and src in ds:
            ds = ds.replace(src, tgt)
    return ds

def _make_unique_slug(sport: str, raw_slug: str, specifiers: str, sid: str = "") -> str:
        # from app.workers.od_harvester_patch import make_unique_slug as _make
        return make_unique_slug(sport, raw_slug, specifiers, sid=sid)


def _parse_markets(raw: list[dict], sport: str, home: str, away: str) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    for mkt in raw:
        if not isinstance(mkt, dict): continue
        if str(mkt.get("status") or "") == "0": continue

        sid  = str(mkt.get("sub_type_id") or mkt.get("type_id") or "")
        name = str(mkt.get("odd_type") or mkt.get("name") or mkt.get("type_name") or "")
        mkt_spec = str(mkt.get("specifiers") or "")

        # --- SPECIAL CASE: 1X2 market (sub_type_id = "1") ---
        if sid == "1":
            slug = "1x2" if sport == "soccer" else f"{sport}_1x2"
            for o, _ in _outcomes(mkt):
                if str(o.get("active") or "") in ("0", "false"): continue
                if str(o.get("status") or "") == "0": continue
                try:
                    val = float(o.get("odd_value") or 0)
                except (TypeError, ValueError):
                    continue
                if val <= 1.0: continue
                outcome_key = str(o.get("outcome_key") or "X").upper()
                if outcome_key not in ("1", "X", "2"):
                    display = str(o.get("outcome_name") or outcome_key)
                    outcome_key = _translate(display, home, away)
                result.setdefault(slug, {})[outcome_key] = val
            continue

        # --- ALL OTHER MARKETS: use the OdiBets mapper and shared outcome normaliser ---
        # raw_slug = name
        raw_slug = name or ""
        slug = _make_unique_slug(sport, raw_slug, mkt_spec, sid=sid)
        # if not raw_slug:
        #     raw_slug = f"{sport}_unknown_{sid}"
        # slug = _make_unique_slug(sport, raw_slug, mkt_spec)

        for o, outcome_spec in _outcomes(mkt):
            if str(o.get("active") or "") in ("0", "false"): continue
            if str(o.get("status") or "") == "0": continue
            try:
                val = float(o.get("odd_value") or 0)
            except (TypeError, ValueError):
                continue
            if val <= 1.0: continue

            display = str(o.get("outcome_key") or o.get("odd_key") or o.get("outcome_name") or o.get("odd_def") or "")
            # Use shared outcome normaliser with the market slug as context
            key = normalize_outcome(slug, display)
            result.setdefault(slug, {})[key] = val

    return result


# ══════════════════════════════════════════════════════════════════════════════
# MARKET ENRICHMENT
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_sub_type(sid: str, params: dict, sport: str, home: str, away: str) -> dict:
    data = _get(SBOOK_V1, params={**params, "sub_type_id": sid})
    if not data: return {}
    d = _inner(data) or (data if isinstance(data, dict) else {})
    return _parse_markets(d.get("markets") or [], sport, home, away)


def fetch_full_markets_for_match(
    event_id: str | int,
    sport_slug: str = "soccer",
    sub_type_workers: int = 10,
) -> dict[str, dict[str, float]]:
    all_markets: dict[str, dict[str, float]] = {}
    base = {
        "resource": "sportevent", "id": str(event_id),
        "category_id": "", "sub_type_id": "", "builder": 0,
        "sportsbook": "sportsbook", "ua": HEADERS["user-agent"],
    }
    data = _get(SBOOK_V1, params=base)
    if not data or not isinstance(data, dict): return {}
    d     = _inner(data) or data
    info  = d.get("info") or {}
    home  = str(info.get("home_team") or "")
    away  = str(info.get("away_team") or "")
    sport = _resolve_sport(info.get("s_binomen") or info.get("sport_id"), sport_slug)

    for slug, outcomes in _parse_markets(d.get("markets") or [], sport, home, away).items():
        all_markets.setdefault(slug, {}).update(outcomes)

    sub_ids = {str(m["sub_type_id"]) for m in (d.get("markets_list") or []) if m.get("sub_type_id")}
    if not sub_ids:
        return all_markets

    with ThreadPoolExecutor(max_workers=min(sub_type_workers, len(sub_ids))) as pool:
        futures = {pool.submit(_fetch_sub_type, sid, base, sport, home, away): sid for sid in sub_ids}
        for f in as_completed(futures):
            try:
                for slug, outcomes in f.result().items():
                    all_markets.setdefault(slug, {}).update(outcomes)
            except Exception as e:
                logger.warning("OD sub_type %s error: %s", futures[f], e)
    return all_markets


# ══════════════════════════════════════════════════════════════════════════════
# MATCH NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

def _normalise(raw: dict, sport_slug: str, is_live: bool = False) -> dict | None:
    try:
        mid = str(raw.get("parent_match_id") or raw.get("game_id") or raw.get("id") or raw.get("match_id") or "")
        if not mid: return None
        home  = str(raw.get("home_team") or raw.get("home") or "Home")
        away  = str(raw.get("away_team") or raw.get("away") or "Away")
        sport = _resolve_sport(raw.get("s_binomen") or raw.get("sport_id") or raw.get("sport"), sport_slug)
        score = str(raw.get("current_score") or raw.get("result") or "")
        parts = score.split(":") if ":" in score else score.split("-") if "-" in score else []
        bet   = str(raw.get("bet_status") or raw.get("b_status") or "")

        mkts = raw.get("markets") or raw.get("odds") or []
        markets = (_parse_markets(mkts, sport, home, away) if isinstance(mkts, list)
                   else mkts if isinstance(mkts, dict) else {})

        expected_1x2 = "1x2" if sport == "soccer" else f"{sport}_1x2"
        if expected_1x2 not in markets:
            try:
                ho = float(raw.get("home_odd") or raw.get("h_odd") or 0)
                no = float(raw.get("draw_odd") or raw.get("d_odd") or raw.get("neutral_odd") or 0)
                ao = float(raw.get("away_odd") or raw.get("a_odd") or 0)
                if ho > 1 or no > 1 or ao > 1:
                    markets[expected_1x2] = {k: v for k, v in [("1", ho), ("X", no), ("2", ao)] if v > 1}
            except (TypeError, ValueError):
                pass

        return {
            "od_match_id":   mid,
            "od_event_id":   str(raw.get("game_id") or mid),
            "od_parent_id":  mid,
            "sp_game_id":    None,
            "betradar_id":   str(raw.get("betradar_id") or raw.get("sr_id") or mid) or None,
            "home_team":     home,
            "away_team":     away,
            "competition":   str(raw.get("competition_name") or raw.get("competition") or raw.get("league") or ""),
            "category":      str(raw.get("category_name") or raw.get("category") or raw.get("country_name") or ""),
            "sport":         sport,
            "od_sport_id":   sport,
            "start_time":    str(raw.get("start_time") or raw.get("event_date") or raw.get("date") or ""),
            "source":        "odibets",
            "is_live":       is_live,
            "is_suspended":  bet in ("STOPPED", "BET_STOP", "SUSPENDED"),
            "match_time":    str(raw.get("match_time") or raw.get("game_time") or raw.get("periodic_time") or ""),
            "event_status":  str(raw.get("event_status") or raw.get("status_desc") or raw.get("status") or ""),
            "bet_status":    bet,
            "current_score": score,
            "score_home":    parts[0].strip() if len(parts) >= 2 else None,
            "score_away":    parts[1].strip() if len(parts) >= 2 else None,
            "markets":       markets,
            "market_count":  len(markets),
        }
    except Exception as e:
        logger.debug("OD normalise error: %s | %s", e, str(raw)[:200])
        return None


# ══════════════════════════════════════════════════════════════════════════════
# PER-DAY FETCHER
# ══════════════════════════════════════════════════════════════════════════════

class _DayCollector:
    def __init__(self):
        self._seen: set[str] = set()
        self._matches: list[dict] = []
        self._lock = threading.Lock()

    def add(self, batch: list[dict]) -> int:
        added = 0
        with self._lock:
            for m in batch:
                uid = str(m.get("parent_match_id") or m.get("game_id") or "")
                if uid and uid not in self._seen:
                    self._seen.add(uid)
                    self._matches.append(m)
                    added += 1
        return added

    @property
    def matches(self) -> list[dict]:
        return self._matches


def _paginate_day(sport_id: str, day: str) -> tuple[list[dict], list[dict]]:
    all_matches: list[dict] = []
    competitions: list[dict] = []
    page = 1
    MAX_PAGES = 200

    while page <= MAX_PAGES:
        data = _get(SBOOK_V1, params=_base(sport_id, day=day, page=page))
        if not data: break
        matches, meta = _unwrap(data)
        if not matches: break
        all_matches.extend(matches)
        if page == 1:
            competitions = _competitions_from_resp(data)
        actual_pp = _safe_int(meta.get("per_page"), len(matches))
        if actual_pp <= 0: actual_pp = len(matches)
        if len(matches) < actual_pp: break
        total = _safe_int(meta.get("total"))
        if total > 0 and page * actual_pp >= total: break
        page += 1
    return all_matches, competitions


def _fetch_competition_page(sport_id: str, day: str, comp_id: str) -> list[dict]:
    all_matches: list[dict] = []
    page = 1
    while page <= 50:
        data = _get(SBOOK_V1, params=_base(sport_id, day=day, competition_id=comp_id, page=page))
        if not data: break
        matches, meta = _unwrap(data)
        if not matches: break
        all_matches.extend(matches)
        actual_pp = _safe_int(meta.get("per_page"), len(matches))
        if actual_pp <= 0: actual_pp = len(matches)
        if len(matches) < actual_pp: break
        page += 1
    return all_matches


def _fetch_day_complete(sport_id: str, day: str, concurrent_comps: int = 15) -> list[dict]:
    collector = _DayCollector()
    day_matches, competitions = _paginate_day(sport_id, day)
    collector.add(day_matches)
    comp_ids = [str(c["competition_id"]) for c in competitions if c.get("competition_id")]
    if comp_ids:
        with ThreadPoolExecutor(max_workers=min(concurrent_comps, len(comp_ids))) as pool:
            futures = {pool.submit(_fetch_competition_page, sport_id, day, cid): cid for cid in comp_ids}
            for f in as_completed(futures):
                try:
                    collector.add(f.result())
                except Exception as e:
                    logger.warning("OD comp fetch error %s %s %s: %s", sport_id, day, futures[f], e)
    return collector.matches


# ══════════════════════════════════════════════════════════════════════════════
# UPCOMING MATCHES
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_matches(
    sport_slug: str = "soccer",
    days: int = 30,
    offset: int = 0,
    max_matches: int | None = None,
    fetch_full_markets: bool = True,
    max_workers: int = 12,
    concurrent_days: int = 8,
    concurrent_comps: int = 15,
    **kwargs,
) -> list[dict]:
    api_id = _probe(sport_slug)

    if sport_slug == "esoccer":
        data = _get(SBOOK_V1, params=_base(api_id))
        if not data: return []
        raw, _ = _unwrap(data)
        return _finalise(raw, sport_slug, fetch_full_markets, max_workers, offset, max_matches)

    today = _date.today()
    day_strings = [(today + timedelta(days=i)).isoformat() for i in range(days)]

    all_raw: list[dict] = []
    global_seen: set[str] = set()

    def _collect(batch: list[dict]) -> None:
        for m in batch:
            uid = str(m.get("parent_match_id") or m.get("game_id") or "")
            if uid and uid not in global_seen:
                global_seen.add(uid)
                all_raw.append(m)

    with ThreadPoolExecutor(max_workers=min(concurrent_days, len(day_strings))) as executor:
        futures = {executor.submit(_fetch_day_complete, api_id, ds, concurrent_comps): ds for ds in day_strings}
        for f in as_completed(futures):
            try:
                _collect(f.result())
            except Exception as e:
                logger.error("OD day error %s %s: %s", sport_slug, futures[f], e)

    logger.info("OD upcoming %s (%d days): %d raw matches", sport_slug, days, len(all_raw))
    return _finalise(all_raw, sport_slug, fetch_full_markets, max_workers, offset, max_matches)


def _finalise(
    raw: list[dict],
    sport_slug: str,
    fetch_full_markets: bool,
    max_workers: int,
    offset: int,
    max_matches: int | None,
) -> list[dict]:
    normalised: list[dict] = []
    seen: set[str] = set()
    for r in raw:
        m = _normalise(r, sport_slug)
        if not m: continue
        if m["od_match_id"] not in seen:
            seen.add(m["od_match_id"])
            normalised.append(m)

    if fetch_full_markets and normalised:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            normalised = list(pool.map(_enrich, normalised))

    if offset:      normalised = normalised[offset:]
    if max_matches: normalised = normalised[:max_matches]
    return normalised


def _enrich(match: dict) -> dict:
    br = match.get("betradar_id")
    if not br: return match
    full = fetch_full_markets_for_match(br, match.get("sport", "soccer"))
    if full:
        match["markets"].update(full)
        match["market_count"] = len(match["markets"])
    return match


# ══════════════════════════════════════════════════════════════════════════════
# LIVE MATCHES
# ══════════════════════════════════════════════════════════════════════════════

def fetch_live_matches(sport_slug: str | None = None) -> list[dict]:
    params: dict[str, Any] = {
        "resource": "live", "sportsbook": "sportsbook",
        "ua": HEADERS["user-agent"], "sub_type_id": "1", "sport_id": "",
    }
    if sport_slug:
        params["sport_id"] = slug_to_od_sport_id(sport_slug)
    data = _get(SBOOK_V1, params=params, timeout=10.0)
    if not data: return []
    matches: list[dict] = []
    for raw in _unwrap_live(data):
        if not isinstance(raw, dict): continue
        sl = _resolve_sport(raw.get("s_binomen") or raw.get("sport_id"), sport_slug or "soccer")
        m = _normalise(raw, sl, is_live=True)
        if m: matches.append(m)
    logger.info("OD live: %d matches (sport=%s)", len(matches), sport_slug or "all")
    return matches


# ══════════════════════════════════════════════════════════════════════════════
# STREAMING + ALIASES
# ══════════════════════════════════════════════════════════════════════════════

def fetch_upcoming_stream(sport_slug: str = "soccer", days: int = 30,
                          max_matches: int | None = None,
                          fetch_full_markets: bool = True, **kwargs) -> Generator[dict, None, None]:
    yield from fetch_upcoming_matches(sport_slug=sport_slug, days=days,
                                      fetch_full_markets=fetch_full_markets,
                                      max_matches=max_matches)

def fetch_live_stream(sport_slug: str, fetch_full_markets: bool = True,
                      **kwargs) -> Generator[dict, None, None]:
    for m in fetch_live_matches(sport_slug):
        if fetch_full_markets and m.get("betradar_id"):
            full = fetch_full_markets_for_match(m["betradar_id"], m.get("sport", "soccer"))
            if full:
                m["markets"].update(full)
                m["market_count"] = len(m["markets"])
        yield m

def fetch_upcoming(sport_slug: str = "soccer", days: int = 30,
                   fetch_full_markets: bool = True, **kwargs) -> list[dict]:
    return fetch_upcoming_matches(sport_slug, days=days,
                                  fetch_full_markets=fetch_full_markets, **kwargs)

def fetch_live(sport_slug: str | None = None, **kwargs) -> list[dict]:
    return fetch_live_matches(sport_slug)


# ══════════════════════════════════════════════════════════════════════════════
# PLUGIN & COMPATIBILITY LAYER
# ══════════════════════════════════════════════════════════════════════════════

class OdiBetsLivePoller:
    def __init__(self, redis_client: Any, interval: float = 2.0):
        self.redis = redis_client
        self.interval = interval
        self._running = False
    def start(self): self._running = True
    def stop(self): self._running = False
    @property
    def alive(self) -> bool: return False

_live_poller = None
def get_live_poller(): return _live_poller
def init_live_poller(redis_client: Any, interval: float = 2.0):
    global _live_poller
    if _live_poller is None:
        _live_poller = OdiBetsLivePoller(redis_client, interval)
        _live_poller.start()
    return _live_poller
def get_cached_upcoming(r: Any, s: str) -> list[dict] | None: return None
def cache_upcoming(r: Any, s: str, m: list[dict], ttl: int = 300) -> None: pass
def get_cached_live(r: Any, i: Any) -> list[dict] | None: return None

class OdiBetsHarvesterPlugin:
    bookie_id = "odibets"
    bookie_name = "OdiBets"
    sport_slugs = list(OD_SPORT_IDS.keys())
    def fetch_upcoming(self, sport_slug: str, days: int = 30, **kwargs) -> list[dict]:
        return fetch_upcoming_matches(sport_slug, days=days, **kwargs)
    def fetch_live(self, sport_slug: str | None = None) -> list[dict]:
        return fetch_live_matches(sport_slug)

__all__ = [
    "fetch_upcoming_matches", "fetch_live_matches",
    "fetch_upcoming_stream", "fetch_live_stream",
    "fetch_full_markets_for_match", "fetch_upcoming", "fetch_live",
    "OdiBetsHarvesterPlugin", "OD_SPORT_IDS",
    "slug_to_od_sport_id", "configure_concurrency",
]