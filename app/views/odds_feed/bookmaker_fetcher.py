"""
app/workers/bookmaker_fetcher.py
=================================
Fetches live and upcoming odds from any bookmaker endpoint using curl_cffi
TLS impersonation.  Handles:

  • BetB2B family  (1xBet, 22Bet, Betwinner, Melbet, Megapari, Helabet, Paripesa)
  • Sportpesa      (REST JSON)
  • Generic JSON   (fallback)

Unified ParsedMatch shape:
    {
        "match_id":    str,
        "home_team":   str,
        "away_team":   str,
        "sport":       str,
        "competition": str,
        "start_time":  str | None,   # ISO-8601
        "status":      "upcoming" | "live",
        "score_home":  int | None,
        "score_away":  int | None,
        "markets": {
            "1X2":           { "Home": float, "Draw": float, "Away": float },
            "Double Chance": { "1X": float, "12": float, "2X": float },
            "Total_2.5":     { "Over": float, "Under": float },
            "BTTS":          { "Yes": float, "No": float },
            "Handicap_-1.5": { "1": float, "2": float },
            "Corners_0.5":   { "Over": float, "Under": float },
            "1H Total_1.5":  { "Over": float, "Under": float },
            "GG/NG":         { "Yes": float, "No": float },
            ... (all markets returned by the API)
        }
    }

Merged match shape (output of merge_bookmaker_results):
    {
        "home_team": str, "away_team": str, ...,
        "bookmakers": {
            "1xBet":   { "match_id": "123", "markets": { "1X2": { "Home": 1.85 } } },
            "Helabet": { "match_id": "123", "markets": { "1X2": { "Home": 1.85 } } },
        },
        "markets": {
            "1X2": { "Home": { "odds": 1.85, "bookmaker": "Paripesa" } }  # best odds
        }
    }
"""

from __future__ import annotations

import gzip
import json
import re
import time
import urllib.parse
import zlib
from typing import Any

try:
    from curl_cffi import requests as _tls
    _CURL_OK = True
except ImportError:
    import requests as _tls  # type: ignore
    _CURL_OK = False


# ─── TLS targets ─────────────────────────────────────────────────────────────
_TARGETS = ["chrome131", "chrome124", "chrome120", "chrome116", "chrome110", "chrome99"]

_STRIP = frozenset({"if-modified-since", "if-none-match", "if-match", "if-unmodified-since"})

_DEFAULTS = {
    "user-agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "accept":          "application/json, text/plain, */*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "connection":      "keep-alive",
}


# =============================================================================
# Low-level HTTP + decompression
# =============================================================================

def _decompress(raw: bytes, encoding: str = "") -> bytes:
    enc = encoding.lower()
    if "gzip" in enc:
        try: return gzip.decompress(raw)
        except Exception: pass
    if "deflate" in enc:
        for wb in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
            try: return zlib.decompress(raw, wb)
            except Exception: pass
    if len(raw) >= 2 and raw[0] == 0x78:
        for wb in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
            try: return zlib.decompress(raw, wb)
            except Exception: pass
    if len(raw) >= 2 and raw[0] == 0x1F and raw[1] == 0x8B:
        try: return gzip.decompress(raw)
        except Exception: pass
    return raw


def _build_url(base: str, ordered_params: list[tuple[str, str]]) -> str:
    if not ordered_params:
        return base
    qs = "&".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in ordered_params)
    return f"{base}?{qs}"


def _fetch(url: str, headers: dict, params: dict | list | None = None, timeout: int = 15) -> dict | list | None:
    merged = {k.lower(): v for k, v in _DEFAULTS.items()}
    for k, v in headers.items():
        kl = k.strip().lower()
        if kl not in _STRIP:
            merged[kl] = v
    merged["accept-encoding"] = "gzip, deflate, br"

    if isinstance(params, list) and params:
        final_url = _build_url(url.split("?")[0], params)
    elif isinstance(params, dict) and params:
        qs = "&".join(f"{k}={urllib.parse.quote(str(v), safe='')}" for k, v in params.items())
        final_url = f"{url.split('?')[0]}?{qs}"
    else:
        final_url = url

    print(f"[fetcher] GET {final_url}")

    last_exc = None
    targets  = _TARGETS if _CURL_OK else [None]

    for target in targets:
        try:
            kwargs: dict[str, Any] = dict(method="GET", url=final_url, headers=merged, timeout=timeout)
            if target and _CURL_OK:
                kwargs["impersonate"] = target

            resp = _tls.request(**kwargs)

            if resp.status_code == 304:
                return None
            if resp.status_code == 406 and len(resp.content) == 0:
                print(f"[fetcher] 406+0b from {final_url} — auth token expired")
                return None
            # Accept 200, 201, 203 (Non-Authoritative — used by Paripesa CDN)
            if resp.status_code not in (200, 201, 203):
                if resp.status_code in (403, 406) and target != targets[-1]:
                    print(f"[fetcher] {resp.status_code} with {target} — trying next fingerprint…")
                    continue
                print(f"[fetcher] HTTP {resp.status_code} from {final_url}")
                return None

            raw  = _decompress(resp.content, resp.headers.get("Content-Encoding", ""))
            text = raw.decode("utf-8", errors="replace")

            text_stripped = re.sub(r"^\s*[\w$]+\s*\(", "", text).rstrip(");").strip()
            for candidate in (text.strip(), text_stripped):
                try: return json.loads(candidate)
                except Exception: pass

            print(f"[fetcher] JSON parse failed. First 200: {text[:200]!r}")
            return None

        except Exception as exc:
            last_exc = exc
            if any(x in str(exc).lower() for x in ("ssl", "tls", "handshake", "connect")):
                print(f"[fetcher] {target} TLS error — trying next…")
                continue
            break

    if last_exc:
        print(f"[fetcher] Error fetching {final_url}: {last_exc}")
    return None


# =============================================================================
# BetB2B sport ID maps
# =============================================================================

_SPORT_MAP = {
    1: "Football",         2: "Ice Hockey",       3: "Basketball",
    4: "Tennis",           5: "Volleyball",        7: "Rugby",
    8: "Handball",         9: "Boxing",            10: "Table Tennis",
    11: "Chess",           12: "Billiards",        13: "American Football",
    14: "Futsal",          15: "Bandy",            17: "Water Polo",
    18: "Motorsport",      20: "TV Games",         21: "Darts",
    24: "Skiing",          26: "Formula 1",        28: "Australian Rules",
    30: "Snooker",         31: "Motorbikes",       36: "Bicycle Racing",
    40: "Esports",         41: "Golf",             44: "Horse Racing",
    46: "Curling",         48: "Lacrosse",         49: "Netball",
    56: "Martial Arts",    66: "Cricket",          67: "Floorball",
    68: "Greyhound Racing",80: "Gaelic Football",  82: "Lottery",
    83: "Softball",        87: "Special Bets",     92: "Trotting",
    126: "Hurling",        189: "UFC",
}

_B2B_SPORT_NAME_MAP: dict[str, int] = {
    "football":          1,  "soccer":            1,
    "ice hockey":        2,  "hockey":            2,
    "basketball":        3,  "tennis":            4,
    "baseball":          5,  "volleyball":        6,
    "rugby":             7,  "handball":          8,
    "boxing":            9,  "table tennis":      10,
    "chess":             11, "billiards":         12,
    "american football": 13, "futsal":            14,
    "bandy":             15, "water polo":        17,
    "motorsport":        18, "tv games":          20,
    "darts":             21, "skiing":            24,
    "formula 1":         26, "australian rules":  28,
    "snooker":           30, "motorbikes":        31,
    "bicycle racing":    36, "esports":           40,
    "golf":              41, "horse racing":      44,
    "curling":           46, "lacrosse":          48,
    "netball":           49, "martial arts":      56,
    "cricket":           66, "floorball":         67,
    "greyhound racing":  68, "gaelic football":   80,
    "lottery":           82, "softball":          83,
    "special bets":      87, "trotting":          92,
    "hurling":           126, "ufc":              189,
}


def _b2b_sport_id(sport_name: str) -> int | None:
    return _B2B_SPORT_NAME_MAP.get(sport_name.strip().lower())


# Confirmed BetB2B domain credentials — per-domain param overrides.
#
# Each entry has two sub-dicts: "live" and "upcoming".
# Any key present here overrides the standard template value in fetch_betb2b.
# Standard template (matches 1xBet reference URL):
#   live:     sports count=1000 lng=en [gr] mode=4 country=87 partner
#             getEmpty=true virtualSports=true noFilterBlockEvent=true
#   upcoming: sports count=1000 lng=en mode=4 country=87 partner
#             getEmpty=true virtualSports=true
#
_B2B_DOMAIN_CREDS: dict[str, dict] = {
    "1xbet.co.ke": {
        "live":     {"partner": "61",  "gr": "656"},
        "upcoming": {"partner": "61"},
    },
    "22bet.co.ke": {
        # Live:     count=50, lng=en_GB, gr=515, partner=151, no virtualSports, no noFilterBlockEvent
        # Upcoming: partner=515, no gr
        "live":     {"partner": "151", "gr": "515", "lng": "en_GB",
                     "count": "50", "virtualSports": None, "noFilterBlockEvent": None},
        "upcoming": {"partner": "515"},
    },
    "betwinner.co.ke": {
        "live":     {"partner": "3",   "gr": "656"},
        "upcoming": {"partner": "3"},
    },
    "melbet.co.ke": {
        "live":     {"partner": "4",   "gr": "656"},
        "upcoming": {"partner": "4"},
    },
    "megapari.com": {
        "live":     {"partner": "6",   "gr": "656"},
        "upcoming": {"partner": "6"},
    },
    "helabetke.com": {
        "live":     {"partner": "237"},   # no gr
        "upcoming": {"partner": "237"},
    },
    "paripesa.cool": {
        "live":     {"partner": "188", "gr": "764"},
        "upcoming": {"partner": "188"},
    },
}

# Per-domain sport ID cache — populated on first fetch per domain
_domain_sport_cache: dict[str, dict[str, int]] = {}


def fetch_betb2b_sport_ids(domain: str, headers: dict, params: dict, timeout: int = 10) -> dict[str, int]:
    """
    Fetch the live sport list for a BetB2B bookmaker and return name->id map.
    Cached per domain. Falls back to _B2B_SPORT_NAME_MAP if request fails.
    """
    if domain in _domain_sport_cache:
        return _domain_sport_cache[domain]

    partner = params.get("partner", "61")
    lng     = params.get("lng",     "en")
    country = params.get("country", "87")

    url = f"https://{domain}/service-api/LineFeed/GetSportsTreeMD_VZip"
    ordered_params: list[tuple[str, str]] = [
        ("CID",     "1"),
        ("lT",      "1"),
        ("count",   "100"),
        ("lng",     lng),
        ("partner", partner),
        ("country", country),
    ]

    raw = _fetch(url, headers, ordered_params, timeout)

    sport_map: dict[str, int] = {}
    if isinstance(raw, dict) and raw.get("Success") and raw.get("Value"):
        for item in raw["Value"]:
            name = (item.get("N") or "").strip().lower()
            sid  = item.get("I")
            c    = item.get("C", 0)
            if name and sid:
                if name not in sport_map or c > sport_map.get(f"_c_{name}", 0):
                    sport_map[name]         = int(sid)
                    sport_map[f"_c_{name}"] = c
        sport_map = {k: v for k, v in sport_map.items() if not k.startswith("_c_")}
        print(f"[fetcher] {domain}: fetched {len(sport_map)} sports from API")
    else:
        print(f"[fetcher] {domain}: sports API unavailable — using built-in map")

    merged = dict(_B2B_SPORT_NAME_MAP)
    merged.update(sport_map)
    _domain_sport_cache[domain] = merged
    return merged


def resolve_betb2b_sport_id(
    domain: str,
    sport_name: str,
    headers: dict,
    params: dict,
    sport_mappings: list | None = None,
) -> int | None:
    """
    3-tier sport ID resolution:
    1. Saved sport_mappings in bookmaker config (explicit override)
    2. Per-domain API cache  (GetSportsTreeMD_VZip, fetched once per domain)
    3. Built-in _B2B_SPORT_NAME_MAP
    Returns None if unknown — caller will fetch all sports.
    """
    key = sport_name.strip().lower()

    if sport_mappings:
        matched = next(
            (m for m in sport_mappings if m.get("sport_name", "").lower() == key),
            None
        )
        if matched:
            return int(matched["bk_sport_id"])

    domain_map = fetch_betb2b_sport_ids(domain, headers, params)
    sid = domain_map.get(key)
    if sid:
        return int(sid)

    print(f"[fetcher] Unknown sport '{sport_name}' on {domain} — will fetch all")
    return None


# =============================================================================
# BetB2B market / outcome maps
# =============================================================================

# ── Market group ID → (display_name, uses_P_for_line_key) ───────────────────
# uses_P=True  → key = "{name}_{P}"   e.g. "Total_2.5", "Handicap_-1.5"
# uses_P=False → key = "{name}"       e.g. "1X2", "BTTS"
_B2B_MARKET_GROUPS: dict[int, tuple[str, bool]] = {
    1:    ("1X2",               False),
    2:    ("Handicap",          True),   # Asian handicap
    3:    ("Correct Score",     False),
    4:    ("HT Correct Score",  False),
    5:    ("HT/FT",             False),
    6:    ("First Scorer",      False),
    7:    ("Last Scorer",       False),
    8:    ("Double Chance",     False),
    9:    ("Draw No Bet",       False),
    10:   ("Eur Handicap",      True),
    11:   ("Win to Nil",        False),
    12:   ("Clean Sheet",       False),
    13:   ("GG/NG",             False),  # Both Teams Score
    14:   ("Next Goal",         False),
    15:   ("1H Total",          True),   # 1st half total
    16:   ("1H Total",          True),
    17:   ("Total",             True),   # Goals O/U — primary
    18:   ("Result+Total",      False),
    19:   ("BTTS",              False),
    20:   ("Exact Goals",       False),
    21:   ("Corners",           True),
    22:   ("Asian Corners",     True),
    23:   ("Bookings",          True),
    24:   ("1H Result",         False),
    25:   ("2H Result",         False),
    26:   ("2H Total",          True),
    27:   ("Cards Total",       True),
    28:   ("1H Double Chance",  False),
    30:   ("Win Both Halves",   False),
    31:   ("Score Both Halves", False),
    32:   ("Team Total",        True),
    37:   ("1H Handicap",       True),
    38:   ("2H Handicap",       True),
    62:   ("Corners",           True),   # Corners total (football)
    63:   ("Handicap",          True),
    64:   ("Corners Handicap",  True),
    99:   ("Total",             True),   # Sport-specific total
    100:  ("Handicap",          True),
    107:  ("Handicap",          True),
    2854: ("Handicap",          True),
}

# Backward compat alias
_B2B_MARKETS = {k: v[0] for k, v in _B2B_MARKET_GROUPS.items()}

# ── Outcome type T → display label ───────────────────────────────────────────
_B2B_OUTCOME_LABELS: dict[int, str] = {
    # 1X2 / Result
    1:    "Home",   2:    "Draw",   3:    "Away",
    # Double Chance
    4:    "1X",     5:    "12",     6:    "2X",
    # Handicap sides
    7:    "1",      8:    "2",
    # Total Over/Under (standard G=17)
    9:    "Over",   10:   "Under",
    # Total Over/Under (alternate slots G=15,62,99)
    11:   "Over",   12:   "Under",
    13:   "Over",   14:   "Under",
    # HT Result (G=24)
    15:   "Home",   16:   "Draw",   17:   "Away",
    # 2H Result (G=25)
    18:   "Home",   19:   "Draw",   20:   "Away",
    # GG/NG (G=13)
    21:   "Yes",    22:   "No",
    # Draw No Bet (G=9)
    23:   "Home",   24:   "Away",
    # Clean Sheet / Win to Nil (G=11,12)
    50:   "Yes",    51:   "No",
    60:   "Yes",    61:   "No",
    # BTTS (G=19)
    180:  "Yes",    181:  "No",
    # Special T IDs seen in LineFeed / GetGameZip samples
    388:  "Home",   389:  "Draw",   390:  "Away",    # exact goals variants
    731:  "T731",                                      # correct score lines
    818:  "Home",   819:  "Draw",   820:  "Away",
    821:  "1X",     822:  "2X",
    3786: "T3786",
    3827: "Over",   3828: "Under",
    3829: "1",      3830: "2",
    7778: "Over",   7779: "Under",  # bookings total
    7780: "Over",   7781: "Under",  # cards total
    8617: "T8617",  8618: "T8618",  # correct score
    11273: "T11273", 11274: "T11274",
    15770: "Home",  15771: "Draw",  15772: "Away",
    15773: "T15773", 15775: "T15775",
    16258: "Home",  16259: "Away",
    16260: "Draw",  16261: "T16261", 16262: "T16262",
    16263: "T16263", 16264: "T16264", 16265: "T16265",
    16266: "T16266", 16267: "T16267", 16268: "T16268",
    16269: "T16269",
}

# Legacy alias (used by per-sport config walker)
_B2B_OUTCOMES = {
    1:   ("1X2",          "Home"),  2:   ("1X2",   "Draw"),  3:   ("1X2",   "Away"),
    4:   ("Double Chance","1X"),    5:   ("Double Chance","12"), 6:   ("Double Chance","2X"),
    7:   ("Total",        "Over"),  8:   ("Total",  "Under"),
    9:   ("Total",        "Over"),  10:  ("Total",  "Under"),
    180: ("BTTS",         "Yes"),   181: ("BTTS",   "No"),
}


# =============================================================================
# BetB2B flat-event parser  (used by Get1x2_VZip  E[] / AE[].ME[])
# =============================================================================

def _build_market_key(g_id: int | None, t_id: int | None, p_val: Any) -> tuple[str, str]:
    """
    Returns (market_key, outcome_label) for a single odds event.
    Used by both _parse_betb2b_item and _parse_gamezi_ge.
    """
    # ── Market key ────────────────────────────────────────────────────────────
    if g_id in _B2B_MARKET_GROUPS:
        mkt_name, uses_p = _B2B_MARKET_GROUPS[g_id]
        if uses_p and p_val is not None:
            try:
                pf = float(p_val)
                p_str = str(int(pf)) if pf == int(pf) else str(pf)
            except Exception:
                p_str = str(p_val)
            mkt_key = f"{mkt_name}_{p_str}"
        else:
            mkt_key = mkt_name
    else:
        mkt_key = f"G{g_id}_{p_val}" if p_val is not None else f"G{g_id}"

    # ── Outcome label ─────────────────────────────────────────────────────────
    outcome = _B2B_OUTCOME_LABELS.get(t_id, f"T{t_id}")  # type: ignore[arg-type]

    return mkt_key, outcome


def _parse_betb2b_item(item: dict, status: str) -> dict | None:
    """
    Parse one match item from Get1x2_VZip Value[].

    Market key rules:
      • Fixed-outcome markets  → key = market_name          e.g. "1X2", "BTTS"
      • Line markets           → key = market_name + "_" + P  e.g. "Total_2.5"
      • Unknown groups         → key = "G{id}" or "G{id}_{P}"

    All markets from E[] and AE[].ME[] are parsed — not just 1X2/Total/BTTS.
    Matches with empty E[] (no odds opened) are returned with markets={} and
    are filtered out by fetch_betb2b after parsing.
    """
    home = (item.get("O1") or item.get("O1N") or item.get("HN") or item.get("Team1") or
            item.get("home") or item.get("HomeTeam") or "")
    away = (item.get("O2") or item.get("O2N") or item.get("AN") or item.get("Team2") or
            item.get("away") or item.get("AwayTeam") or "")

    if not home or not away:
        return None

    # Collect all events from E[] and AE[].ME[]
    events: list[dict] = list(item.get("E") or [])
    for ae in (item.get("AE") or []):
        if isinstance(ae, dict):
            events.extend(ae.get("ME") or ae.get("E") or [])

    markets: dict[str, dict[str, float]] = {}

    for e in events:
        if not isinstance(e, dict):
            continue
        g_id  = e.get("G")
        t_id  = e.get("T")
        p_val = e.get("P")

        raw_odds = e.get("C") or e.get("CV") or e.get("Cf") or e.get("v") or 0
        try:
            odds_val = float(raw_odds)
        except (TypeError, ValueError):
            odds_val = 0.0
        if odds_val <= 1.0:
            continue

        mkt_key, outcome = _build_market_key(g_id, t_id, p_val)
        markets.setdefault(mkt_key, {})[outcome] = odds_val

    # Meta fields
    sport_name_raw = item.get("SN") or item.get("SE") or ""
    sport_id_int   = item.get("SI") or item.get("SportId") or 1
    champ_name     = (item.get("L") or item.get("LE") or item.get("ChampName")
                      or item.get("League") or item.get("CI") or "")
    match_id       = str(item.get("I") or item.get("Id") or item.get("ID") or "")
    start_ts       = item.get("S") or item.get("StartTime") or item.get("ST")

    start_iso = None
    if start_ts:
        try:
            import datetime
            start_iso = datetime.datetime.utcfromtimestamp(int(start_ts)).isoformat() + "Z"
        except Exception:
            pass

    score_home = score_away = None
    if status == "live":
        sc = item.get("SC") or {}
        if isinstance(sc, dict):
            fs = sc.get("FS") or sc.get("fs") or {}
            score_home = fs.get("S1") or fs.get("s1")
            score_away = fs.get("S2") or fs.get("s2")

    if sport_name_raw:
        sport_name_out = str(sport_name_raw)
    else:
        try:
            sport_int = int(sport_id_int)
        except (TypeError, ValueError):
            sport_int = 1
        sport_name_out = _SPORT_MAP.get(sport_int, f"Sport_{sport_id_int}")

    return {
        "match_id":    match_id,
        "home_team":   str(home),
        "away_team":   str(away),
        "sport":       sport_name_out,
        "competition": str(champ_name),
        "start_time":  start_iso,
        "status":      status,
        "score_home":  score_home,
        "score_away":  score_away,
        "markets":     markets,
    }


# =============================================================================
# BetB2B GE[] parser  (used by GetGameZip — full match market book)
# =============================================================================

def _parse_gamezi_ge(ge_list: list) -> dict[str, dict[str, float]]:
    """
    Parse GE[] from GetGameZip response.

    GetGameZip.GE structure (different from Get1x2_VZip.E):
        GE[n] = {
            "G":  group_id,
            "GS": group_sub_id,
            "E":  [                     ← list of "columns"
                [                       ← column 0 = one side (Home / Over / Team1)
                    { "C": odds, "T": outcome_type, "P": line, "CE": 1 },  ← main line
                    { "C": odds, "T": outcome_type, "P": line },
                    ...
                ],
                [                       ← column 1 = other side (Away / Under / Team2)
                    ...
                ],
                ...
            ]
        }

    CE=1 marks the "main" displayed line for line markets.
    All lines are returned — the UI decides which to highlight.
    """
    markets: dict[str, dict[str, float]] = {}

    for ge in (ge_list or []):
        if not isinstance(ge, dict):
            continue
        g_id = ge.get("G")
        cols = ge.get("E") or []

        for col in cols:
            if not isinstance(col, list):
                continue
            for ev in col:
                if not isinstance(ev, dict):
                    continue
                t_id     = ev.get("T")
                p_val    = ev.get("P")
                raw_odds = ev.get("C") or ev.get("CV") or 0
                try:
                    odds_val = float(raw_odds)
                except (TypeError, ValueError):
                    odds_val = 0.0
                if odds_val <= 1.0:
                    continue

                mkt_key, outcome = _build_market_key(g_id, t_id, p_val)
                markets.setdefault(mkt_key, {})[outcome] = odds_val

    return markets


# =============================================================================
# BetB2B fetcher  (Get1x2_VZip — match list with basic markets)
# =============================================================================

def fetch_betb2b(
    domain: str,
    headers: dict,
    params: dict,
    sport_id: str | int = 1,
    mode: str = "live",
    timeout: int = 15,
) -> list[dict]:
    """
    Fetch from BetB2B family (1xBet, Helabet, Paripesa, etc.) using Get1x2_VZip.

    Exact URL structure — param order is fixed, only domain and partner change:

    Live (LiveFeed):
        https://{domain}/service-api/LiveFeed/Get1x2_VZip
            ?sports={id}&count=1000&lng=en&gr={gr}&mode=4
            &country=87&partner={partner}
            &getEmpty=true&virtualSports=true&noFilterBlockEvent=true

    Upcoming (LineFeed):
        https://{domain}/service-api/LineFeed/Get1x2_VZip
            ?sports={id}&count=1000&lng=en&mode=4
            &country=87&partner={partner}
            &getEmpty=true&virtualSports=true

    Notes:
      - gr is only present on LiveFeed (omitted for LineFeed)
      - count=1000 covers a full sport in one call — no pagination needed
      - mode=4 always fixed
      - sport_id filters server-side (1=Football, 3=Basketball, etc.)
    """
    lng     = params.get("lng",     "en")
    gr      = params.get("gr",      "")
    country = params.get("country", "87")
    partner = params.get("partner", "61")
    status  = "live" if mode == "live" else "upcoming"

    # ── Apply per-domain overrides from _B2B_DOMAIN_CREDS ─────────────────────
    # Start with values from params dict (harvest_config), then overlay
    # per-domain overrides. Value=None means "omit this param entirely".
    domain_key   = domain.lower().lstrip("www.")
    domain_creds = _B2B_DOMAIN_CREDS.get(domain_key, {})
    mode_key     = "live" if mode == "live" else "upcoming"
    overrides    = domain_creds.get(mode_key, {})

    def _p(key: str, default: str) -> str | None:
        """Return override value, params-dict value, or default. None = omit."""
        if key in overrides:
            return overrides[key]   # None means omit
        return params.get(key, default)

    p_lng     = _p("lng",     "en")
    p_gr      = _p("gr",      gr)     # gr from params dict or override
    p_country = _p("country", "87")
    p_partner = _p("partner", partner)
    p_count   = _p("count",   "1000")
    p_vs      = _p("virtualSports",       "true")   # None = omit
    p_nfbe    = _p("noFilterBlockEvent",  "true")   # None = omit (live only)

    if mode == "live":
        base_url = f"https://{domain}/service-api/LiveFeed/Get1x2_VZip"
        # Fixed param order: sports count lng [gr] mode country partner getEmpty [virtualSports] [noFilterBlockEvent]
        ordered_params: list[tuple[str, str]] = [
            ("sports",  str(sport_id)),
            ("count",   p_count),
            ("lng",     p_lng),
        ]
        if p_gr:
            ordered_params.append(("gr", p_gr))
        ordered_params += [
            ("mode",      "4"),
            ("country",   p_country),
            ("partner",   p_partner),
            ("getEmpty",  "true"),
        ]
        if p_vs is not None:
            ordered_params.append(("virtualSports", p_vs))
        if p_nfbe is not None:
            ordered_params.append(("noFilterBlockEvent", p_nfbe))
    else:
        base_url = f"https://{domain}/service-api/LineFeed/Get1x2_VZip"
        # Fixed param order: sports count lng mode country partner getEmpty [virtualSports]
        # Note: no gr for LineFeed
        ordered_params = [
            ("sports",  str(sport_id)),
            ("count",   p_count),
            ("lng",     p_lng),
            ("mode",    "4"),
            ("country", p_country),
            ("partner", p_partner),
            ("getEmpty","true"),
        ]
        if p_vs is not None:
            ordered_params.append(("virtualSports", p_vs))

    try:
        raw = _fetch(base_url, headers, ordered_params, timeout)
    except Exception as exc:
        print(f"[fetcher] {domain} -> fetch error: {exc}")
        return []

    if not isinstance(raw, dict):
        print(f"[fetcher] {domain} -> non-dict response, skipping")
        return []
    if not raw.get("Success"):
        print(f"[fetcher] {domain} -> Success=False (ErrorCode={raw.get('ErrorCode')}), skipping")
        return []

    value = raw.get("Value") or []
    print(f"[fetcher] {domain} -> Value[] len={len(value)} mode={mode} sport_id={sport_id}")

    # ── Flatten nested tree (LineFeed = championship tree, LiveFeed = flat) ──
    # LineFeed: Value[] = [{CI, CN, ..., GE:[{O1,O2,E...},...]}]  ← leagues
    # LiveFeed: Value[] = [{O1, O2, E...}, ...]                   ← flat matches
    flat_items: list[dict] = []

    def _walk_value(node: dict, depth: int = 0) -> None:
        """Descend until we find items that have team names (O1/O2 = a match)."""
        has_teams = (node.get("O1") or node.get("O1N") or node.get("HN") or
                     node.get("O2") or node.get("O2N") or node.get("AN"))
        if has_teams:
            flat_items.append(node)
            return
        if depth >= 4:
            return
        for key, child_val in node.items():
            if key in ("E", "AE"):           # always odds arrays — never descend
                continue
            if isinstance(child_val, list) and child_val:
                first = child_val[0]
                if not isinstance(first, dict):
                    continue
                # Skip pure odds-event arrays (have G/T/C but no team names)
                if "C" in first and "T" in first and "G" in first:
                    continue
                for child in child_val:
                    if isinstance(child, dict):
                        _walk_value(child, depth + 1)

    for top_item in value:
        if isinstance(top_item, dict):
            _walk_value(top_item)

    if len(flat_items) != len(value):
        print(f"[fetcher] {domain} -> tree walk: {len(value)} top-level "
              f"→ {len(flat_items)} match items")

    results:         list[dict] = []
    no_odds:         int = 0
    parse_fail:      int = 0
    sport_counts:    dict[str, int] = {}
    no_odds_samples: list[dict] = []

    for item in flat_items:
        if not isinstance(item, dict):
            continue
        parsed = _parse_betb2b_item(item, status)
        if not parsed:
            parse_fail += 1
            continue

        item_sport = (parsed.get("sport") or "unknown").strip()
        sport_counts[item_sport] = sport_counts.get(item_sport, 0) + 1

        if not parsed.get("markets"):
            no_odds += 1
            if len(no_odds_samples) < 3:
                no_odds_samples.append({
                    "match":    f"{parsed['home_team']} v {parsed['away_team']}",
                    "sport":    item_sport,
                    "E_len":    len(item.get("E") or []),
                    "AE_len":   len(item.get("AE") or []),
                    "raw_keys": list(item.keys())[:8],
                })
            continue

        results.append(parsed)

    # ── Log summary ───────────────────────────────────────────────────────────
    top_sports = sorted(sport_counts.items(), key=lambda x: -x[1])[:6]
    sports_str = ", ".join(f"{s}:{n}" for s, n in top_sports)
    print(
        f"[fetcher] {domain} -> {len(results)} matches with odds"
        + (f", {no_odds} no-odds skipped" if no_odds else "")
        + (f", {parse_fail} parse-fail" if parse_fail else "")
        + f"  |  sports: {sports_str}"
    )

    if no_odds_samples:
        print(f"[fetcher:debug] {domain} no-odds samples:")
        for s in no_odds_samples:
            print(f"[fetcher:debug]   {s['match']} [{s['sport']}] "
                  f"E={s['E_len']} AE={s['AE_len']} keys={s['raw_keys']}")

    return results


def fetch_betb2b_markets(
    domain: str,
    headers: dict,
    params: dict,
    match_id: str,
    feed: str = "LineFeed",
    timeout: int = 15,
) -> dict:
    """
    Fetch full market odds for a single BetB2B match via GetGameZip.

    GetGameZip returns the complete market book (250+ events / 20+ market groups)
    for one match, grouped into GE[] by market group (G=1 1X2, G=17 Total, etc.).

    URL:
      https://{domain}/service-api/{LineFeed|LiveFeed}/GetGameZip
        ?id={match_id}&lng=en&isSubGames=true&GroupEvents=true
        &countevents=250&grMode=4&partner={partner}
        &topGroups=&country=87&marketType=1&isNewBuilder=true

    Returns:
    {
        "match_id":     str,
        "home_team":    str,
        "away_team":    str,
        "competition":  str,
        "start_time":   int | None,       # unix timestamp
        "win_probs":    { "P1": float, "PX": float, "P2": float } | None,
        "markets":      { market_key: { outcome: odds } },
        "market_count": int,
    }
    """
    url = f"https://{domain}/service-api/{feed}/GetGameZip"
    ordered_params: list[tuple[str, str]] = [
        ("id",           str(match_id)),
        ("lng",          params.get("lng", "en")),
        ("isSubGames",   "true"),
        ("GroupEvents",  "true"),
        ("countevents",  "250"),
        ("grMode",       "4"),
        ("partner",      params.get("partner", "61")),
        ("topGroups",    ""),
        ("country",      params.get("country", "87")),
        ("marketType",   "1"),
        ("isNewBuilder", "true"),
    ]

    raw = _fetch(url, headers, ordered_params, timeout)

    # Try LiveFeed if LineFeed returns nothing (live matches only on LiveFeed)
    if (not isinstance(raw, dict) or not raw.get("Success")) and feed == "LineFeed":
        url2 = f"https://{domain}/service-api/LiveFeed/GetGameZip"
        raw  = _fetch(url2, headers, ordered_params, timeout)

    if not isinstance(raw, dict) or not raw.get("Success"):
        return {}

    item = raw.get("Value") or {}
    if not isinstance(item, dict):
        return {}

    # Parse GE[] (primary path for GetGameZip)
    markets = _parse_gamezi_ge(item.get("GE") or [])

    # Fallback: parse legacy flat E[] if GE absent (older BetB2B versions)
    if not markets and item.get("E"):
        fallback = _parse_betb2b_item(item, "upcoming") or {}
        markets = fallback.get("markets", {})

    win_probs = None
    if isinstance(item.get("WP"), dict):
        wp = item["WP"]
        win_probs = {
            "P1": wp.get("P1"),
            "PX": wp.get("PX"),
            "P2": wp.get("P2"),
        }

    return {
        "match_id":     str(item.get("I") or match_id),
        "home_team":    str(item.get("O1") or ""),
        "away_team":    str(item.get("O2") or ""),
        "competition":  str(item.get("L") or item.get("LE") or ""),
        "start_time":   item.get("S"),
        "win_probs":    win_probs,
        "markets":      markets,
        "market_count": len(markets),
    }


# =============================================================================
# Sportpesa fetcher
# =============================================================================

_SP_MARKET_MAP = {
    10: "1X2",
    46: "Double Chance",
    52: "Total",
    43: "BTTS",
}
_SP_OUTCOME_MAP = {
    "1X2":          {"1": "Home",  "X": "Draw",  "2":  "Away"},
    "Double Chance":{"1X": "1X",   "12": "12",   "X2": "2X"},
    "Total":        {"OV": "Over", "UN": "Under"},
    "BTTS":         {"Yes": "Yes", "No": "No"},
}


def _parse_sportpesa_response(raw: dict) -> list[dict]:
    results = []
    for game_id, markets_list in raw.items():
        home = away = ""
        markets: dict[str, dict] = {}

        for m in (markets_list or []):
            m_id        = m.get("id")
            market_name = _SP_MARKET_MAP.get(m_id)
            if not market_name:
                continue

            if m_id == 10 and not home:
                sels = m.get("selections", [])
                if len(sels) >= 3:
                    home = sels[0].get("name", "")
                    away = sels[2].get("name", "")

            spec = m.get("specValue", 0)
            key  = f"Total_{spec}" if market_name == "Total" and spec else market_name
            outcome_map = _SP_OUTCOME_MAP.get(market_name, {})

            for sel in m.get("selections", []):
                short     = sel.get("shortName") or sel.get("name", "")
                canonical = outcome_map.get(short, short)
                odds_val  = float(sel.get("odds", 0))
                if odds_val > 1.0:
                    markets.setdefault(key, {})[canonical] = odds_val

        if home or away:
            results.append({
                "match_id":    str(game_id),
                "home_team":   home,
                "away_team":   away,
                "sport":       "Football",
                "competition": "",
                "start_time":  None,
                "status":      "upcoming",
                "score_home":  None,
                "score_away":  None,
                "markets":     markets,
            })
    return results


def fetch_sportpesa(
    domain: str,
    headers: dict,
    params: dict,
    game_ids: list[str] | None = None,
    page: int = 1,
    page_size: int = 50,
    timeout: int = 15,
) -> list[dict]:
    """
    Fetch from Sportpesa.
    Step 1: GET list_url → game ID list
    Step 2: GET markets_url?games=<ids>&markets=10,46,52,43
    """
    if not game_ids:
        list_url = params.get("list_url") or f"https://{domain}/api/games"
        raw = _fetch(list_url, headers, None, timeout)
        if not raw:
            return []
        if isinstance(raw, list):
            game_ids = [str(g.get("id") or g.get("gameId", "")) for g in raw if g]
        elif isinstance(raw, dict):
            if "games" in raw:
                game_ids = [str(g.get("id", g)) for g in raw["games"]]
            else:
                game_ids = [k for k in raw.keys() if k.isdigit()][:200]

    if not game_ids:
        return []

    start_idx = (page - 1) * page_size
    paged_ids = game_ids[start_idx: start_idx + page_size]
    if not paged_ids:
        return []

    markets_url = params.get("markets_url") or f"https://{domain}/api/games/markets"
    market_ids  = params.get("markets", "10,46,52,43")

    ordered_params: list[tuple[str, str]] = [
        ("games",   ",".join(str(i) for i in paged_ids)),
        ("markets", market_ids),
    ]

    raw = _fetch(markets_url, headers, ordered_params, timeout)
    if not isinstance(raw, dict):
        return []
    return _parse_sportpesa_response(raw)


# =============================================================================
# Generic JSON fetcher
# =============================================================================

def fetch_generic(
    url: str,
    headers: dict,
    params: dict,
    field_map: dict,
    array_path: str | None = None,
    timeout: int = 15,
) -> list[dict]:
    """
    Generic fetcher for any bookmaker with a flat JSON list endpoint.
    field_map example:
        {
          "match_id":    "id",
          "home_team":   "home",
          "away_team":   "away",
          "start_time":  "startTime",
          "sport":       "sport.name",
          "competition": "league.name",
        }
    """
    raw = _fetch(url, headers, params, timeout)
    if raw is None:
        return []

    def get_path(obj: Any, path: str) -> Any:
        for key in path.split("."):
            if not isinstance(obj, dict): return None
            obj = obj.get(key)
        return obj

    items: list = []
    if isinstance(raw, list):
        items = raw
    elif array_path:
        items = get_path(raw, array_path) or []
    else:
        if isinstance(raw, dict):
            for v in raw.values():
                if isinstance(v, list): items = v; break

    results = []
    for item in items:
        results.append({
            "match_id":    str(get_path(item, field_map.get("match_id",    "id"))    or ""),
            "home_team":   str(get_path(item, field_map.get("home_team",   "home"))  or ""),
            "away_team":   str(get_path(item, field_map.get("away_team",   "away"))  or ""),
            "sport":       str(get_path(item, field_map.get("sport",       "sport")) or "Football"),
            "competition": str(get_path(item, field_map.get("competition", "league"))or ""),
            "start_time":  get_path(item, field_map.get("start_time", "startTime")),
            "status":      "upcoming",
            "score_home":  None,
            "score_away":  None,
            "markets":     {},
        })
    return results


# =============================================================================
# Mode placeholder resolver  (for per-sport saved configs)
# =============================================================================

def _resolve_mode_param(params: list[tuple[str, str]], mode: str) -> list[tuple[str, str]]:
    """Replace {{mode}} placeholder: live -> '4', upcoming -> '1'."""
    mv = "4" if mode == "live" else "1"
    return [(k, mv if v == "{{mode}}" else v) for k, v in params]


# =============================================================================
# Top-level dispatcher
# =============================================================================

def fetch_bookmaker(
    bookmaker: dict,
    sport_name: str = "Football",
    mode: str = "live",
    page: int = 1,
    page_size: int = 40,
    timeout: int = 20,
) -> list[dict]:
    """
    Route a fetch to the correct vendor parser.

    Config resolution priority for BetB2B:
    1. Per-sport saved config  bookmaker["config"]["sports"][sport_name]
    2. Hardcoded domain creds  _B2B_DOMAIN_CREDS
    3. Saved config.params.partner

    Any bookmaker that fails returns [] — never blocks concurrent siblings.
    """
    vendor  = (bookmaker.get("vendor_slug") or "generic").lower()
    domain  = bookmaker.get("domain", "")
    config  = bookmaker.get("config") or {}
    matches: list[dict] = []

    # ── Per-sport saved config path ───────────────────────────────────────────
    sports_cfg = config.get("sports") or {}

    if sports_cfg and sport_name in sports_cfg:
        sport_entry = sports_cfg[sport_name]
        url         = sport_entry.get("url") or f"https://{domain}/service-api/LiveFeed/Get1x2_VZip"
        raw_params  = sport_entry.get("params") or []
        headers     = sport_entry.get("headers") or config.get("headers") or {}
        status      = "live" if mode == "live" else "upcoming"

        ordered: list[tuple[str, str]] = [
            (str(p[0]), str(p[1])) for p in raw_params if len(p) == 2
        ]
        ordered = _resolve_mode_param(ordered, mode)

        skip = (page - 1) * page_size
        if skip > 0:
            ordered.append(("skip", str(skip)))

        try:
            raw = _fetch(url, headers, ordered, timeout)
        except Exception as exc:
            print(f"[fetcher] {domain} per-sport fetch error: {exc}")
            return []

        if isinstance(raw, dict) and raw.get("Success"):
            def _walk_ps(node: dict) -> None:
                if node.get("O1") or node.get("O2") or node.get("HN") or node.get("Team1"):
                    parsed = _parse_betb2b_item(node, status)
                    if parsed and parsed.get("markets"):
                        matches.append(parsed)
                    return
                for val in node.values():
                    if isinstance(val, list):
                        for child in val:
                            if isinstance(child, dict) and child:
                                _walk_ps(child)
            for sport_node in raw.get("Value", []):
                if isinstance(sport_node, dict):
                    _walk_ps(sport_node)

    # ── Legacy BetB2B path ────────────────────────────────────────────────────
    elif vendor == "betb2b":
        headers = config.get("headers") or {}
        params  = dict(config.get("params") or {})

        partner = params.get("partner") or ""
        if not partner:
            creds = _B2B_DOMAIN_CREDS.get(domain.lower().lstrip("www."))
            if creds:
                # For legacy path: pull partner/gr from the "upcoming" entry
                up = creds.get("upcoming", {})
                lv = creds.get("live", {})
                partner = up.get("partner") or lv.get("partner", "")
                gr_val  = lv.get("gr", "")
                if partner:
                    params["partner"] = partner
                if gr_val and not params.get("gr"):
                    params["gr"] = gr_val
                print(f"[fetcher] {domain} — using hardcoded creds partner={partner} gr={gr_val or '(none)'}") 
            else:
                print(f"[fetcher] {domain} -> WARNING: no partner ID — skipping")
                return []

        # Resolve BetB2B sport ID from sport name
        sid = _b2b_sport_id(sport_name) if sport_name.upper() != "ALL" else 1
        if sid is None:
            print(f"[fetcher] {domain} — unknown sport '{sport_name}', defaulting to Football (1)")
            sid = 1
        print(f"[fetcher] {domain} — '{sport_name}' → sport_id={sid} mode={mode}")

        matches = fetch_betb2b(
            domain, headers, params,
            sport_id=sid,
            mode=mode,
            timeout=timeout,
        )

    # ── Sportpesa ─────────────────────────────────────────────────────────────
    elif vendor == "sportpesa":
        headers = config.get("headers") or {}
        params  = config.get("params")  or {}
        try:
            matches = fetch_sportpesa(
                domain, headers, params,
                page=page, page_size=page_size, timeout=timeout,
            )
        except Exception as exc:
            print(f"[fetcher] {domain} sportpesa error: {exc}")

    # ── Generic ───────────────────────────────────────────────────────────────
    elif vendor == "generic":
        headers = config.get("headers") or {}
        params  = config.get("params")  or {}
        url     = config.get("list_url", f"https://{domain}/api/events")
        try:
            matches = fetch_generic(
                url, headers, params,
                config.get("field_map", {}),
                config.get("array_path"), timeout,
            )
        except Exception as exc:
            print(f"[fetcher] {domain} generic error: {exc}")

    for m in matches:
        m["bookmaker_id"]     = bookmaker.get("id")
        m["bookmaker_name"]   = bookmaker.get("name") or domain
        m["bookmaker_domain"] = domain

    return matches


# =============================================================================
# Multi-bookmaker concurrent fetch
# =============================================================================

def fetch_all_bookmakers(
    bookmakers: list[dict],
    sport_name: str = "Football",
    mode: str = "live",
    page: int = 1,
    page_size: int = 200,
    timeout: int = 20,
    max_workers: int = 8,
) -> dict:
    """
    Fetch from ALL bookmakers concurrently for one sport+mode.
    Bookmakers that fail are recorded in per_bookmaker[name].error
    but never block concurrent siblings.

    Returns:
    {
        "sport":         str,
        "mode":          str,
        "total":         int,
        "matches":       [UnifiedMatch, ...],
        "per_bookmaker": {
            "1xBet":   { "ok": True,  "count": 46, "latency_ms": 1120, "error": None },
            "Helabet": { "ok": False, "count": 0,  "latency_ms": 200,  "error": "..." },
        },
        "errors": [{ "bookmaker": str, "error": str }, ...]
    }
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    per_bk_stats: dict[str, dict] = {}
    all_results:  list[list[dict]] = []
    errors:       list[dict] = []

    def _fetch_one(bm: dict) -> tuple[str, list[dict], int, str | None]:
        name = bm.get("name") or bm.get("domain", "?")
        t0   = time.perf_counter()
        try:
            result  = fetch_bookmaker(bm, sport_name=sport_name, mode=mode,
                                      page=page, page_size=page_size, timeout=timeout)
            latency = int((time.perf_counter() - t0) * 1000)
            return name, result, latency, None
        except Exception as exc:
            latency = int((time.perf_counter() - t0) * 1000)
            print(f"[fetcher] {name} -> exception: {exc}")
            return name, [], latency, str(exc)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_one, bm): bm for bm in bookmakers}
        for fut in as_completed(futures):
            name, match_list, latency, err = fut.result()
            per_bk_stats[name] = {
                "ok":         err is None and len(match_list) > 0,
                "api_ok":     err is None,   # True = API responded (even if 0 matches)
                "count":      len(match_list),
                "latency_ms": latency,
                "error":      err,
            }
            if match_list:
                all_results.append(match_list)
            if err:
                errors.append({"bookmaker": name, "error": err})

    merged = merge_bookmaker_results(all_results) if all_results else []

    return {
        "sport":         sport_name,
        "mode":          mode,
        "total":         len(merged),
        "matches":       merged,
        "per_bookmaker": per_bk_stats,
        "errors":        errors,
    }


def fetch_all_sports(
    bookmakers: list[dict],
    sports: list[str] | None = None,
    mode: str = "live",
    page_size: int = 200,
    timeout: int = 20,
    max_workers: int = 8,
) -> dict:
    """
    Fetch all sports from all bookmakers concurrently.

    Returns:
    {
        "Football":   { ...fetch_all_bookmakers result... },
        "Basketball": { ... },
        ...
    }
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if not sports:
        sports = ["Football", "Basketball", "Tennis", "Ice Hockey", "Volleyball", "Cricket"]

    results: dict[str, dict] = {}

    def _fetch_sport(sport: str) -> tuple[str, dict]:
        return sport, fetch_all_bookmakers(
            bookmakers, sport_name=sport, mode=mode,
            page_size=page_size, timeout=timeout, max_workers=max_workers,
        )

    with ThreadPoolExecutor(max_workers=min(len(sports), 6)) as pool:
        futures = {pool.submit(_fetch_sport, sp): sp for sp in sports}
        for fut in as_completed(futures):
            sport, data = fut.result()
            results[sport] = data

    return results


# =============================================================================
# Fuzzy team name matching + merge
# =============================================================================

def _normalize(name: str) -> str:
    return re.sub(
        r"\b(fc|sc|ac|cf|sf|afc|utd|united|city|club|de|the)\b",
        "", name.lower()
    ).strip()


def match_similarity(a: str, b: str) -> float:
    import difflib
    return difflib.SequenceMatcher(None, _normalize(a), _normalize(b)).ratio()


def merge_bookmaker_results(all_results: list[list[dict]], threshold: float = 0.72) -> list[dict]:
    """
    Merge results from multiple bookmakers into unified matches.

    Groups matches by:
      1. Exact normalised "home|away" key   (O(1) fast path)
      2. BetB2B match_id "__id__{id}"       (catches same match across white-labels)
      3. Fuzzy team-name similarity ≥ 0.72  (catches "Arsenal" vs "Arsenal FC")

    Output shape per match:
    {
        "home_team":   str,
        "away_team":   str,
        "sport":       str,
        "competition": str,
        "start_time":  str | None,
        "status":      "live" | "upcoming",
        "score_home":  int | None,
        "score_away":  int | None,

        # Per-bookmaker raw odds (floats) — used by MatchDetailView comparison
        "bookmakers": {
            "1xBet":   { "match_id": "123", "markets": { "1X2": { "Home": 1.85 } } },
            "Helabet": { "match_id": "123", "markets": { "1X2": { "Home": 1.85 } } },
        },

        # Best-odds summary (highest odds per outcome + source bookmaker)
        "markets": {
            "1X2": { "Home": { "odds": 1.85, "bookmaker": "Paripesa" }, ... }
        }
    }
    """
    merged: list[dict] = []
    # O(1) lookup indexes
    _name_idx: dict[str, int] = {}   # normalized "home|away" → merged index
    _id_idx:   dict[str, int] = {}   # "__id__{match_id}"    → merged index

    def _norm_key(home: str, away: str) -> str:
        return f"{_normalize(home)}|{_normalize(away)}"

    for bk_matches in all_results:
        for match in bk_matches:
            home    = match["home_team"]
            away    = match["away_team"]
            bk_name = match["bookmaker_name"]
            mid     = match.get("match_id", "")

            # ── Fast path 1: normalized team name key ─────────────────────────
            norm_key   = _norm_key(home, away)
            target_idx = _name_idx.get(norm_key, -1)

            # ── Fast path 2: BetB2B match_id (white-labels share IDs) ─────────
            if target_idx < 0 and mid:
                target_idx = _id_idx.get(f"__id__{mid}", -1)

            # ── Slow path: fuzzy scan ─────────────────────────────────────────
            if target_idx < 0:
                best_score = 0.0
                for i, existing in enumerate(merged):
                    s = (match_similarity(home, existing["home_team"]) +
                         match_similarity(away, existing["away_team"])) / 2
                    if s > best_score:
                        best_score = s
                        target_idx = i
                if best_score < threshold:
                    target_idx = -1

            # ── Merge into existing entry ─────────────────────────────────────
            if target_idx >= 0:
                existing = merged[target_idx]

                # Register this bookmaker's raw odds
                existing["bookmakers"][bk_name] = {
                    "match_id": mid,
                    "markets":  match["markets"],
                }

                # Update best-odds summary
                for mkt_key, outcomes in match["markets"].items():
                    existing["markets"].setdefault(mkt_key, {})
                    for outcome, odds in outcomes.items():
                        cur = existing["markets"][mkt_key].get(outcome, {})
                        if not cur or odds > cur.get("odds", 0):
                            existing["markets"][mkt_key][outcome] = {
                                "odds":      odds,
                                "bookmaker": bk_name,
                            }

                # Update live score if this entry has it
                if match.get("score_home") is not None:
                    existing["score_home"] = match["score_home"]
                    existing["score_away"] = match["score_away"]

            # ── Create new entry ──────────────────────────────────────────────
            else:
                best_mkts: dict[str, dict[str, dict]] = {}
                for mkt_key, outcomes in match["markets"].items():
                    best_mkts[mkt_key] = {
                        o: {"odds": v, "bookmaker": bk_name}
                        for o, v in outcomes.items()
                    }
                entry = {
                    "home_team":   home,
                    "away_team":   away,
                    "sport":       match["sport"],
                    "competition": match["competition"],
                    "start_time":  match["start_time"],
                    "status":      match["status"],
                    "score_home":  match["score_home"],
                    "score_away":  match["score_away"],
                    "markets":     best_mkts,
                    "bookmakers": {
                        bk_name: {
                            "match_id": mid,
                            "markets":  match["markets"],
                        }
                    },
                }
                new_idx = len(merged)
                merged.append(entry)

                # Register both indexes for this new entry
                _name_idx[norm_key] = new_idx
                if mid:
                    _id_idx[f"__id__{mid}"] = new_idx

    return merged