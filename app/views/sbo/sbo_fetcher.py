"""
app/workers/sbo_fetcher.py
===========================
Standalone odds fetcher for the three Kenyan bookmakers that share Betradar
match IDs:

    • Sportpesa  (betradarId field)
    • Betika     (parent_match_id field)
    • Odibets    (id field == betradar ID)

This module is completely self-contained — it has no dependency on
bookmaker_fetcher.py, the BetB2B engine, or Celery.  Import it directly:

    from app.workers.sbo_fetcher import OddsAggregator, SPORT_CONFIG

Architecture
------------
1. Sportpesa is the PRIMARY source: definitive match list with betradar IDs,
   team names, start times and competitions.
2. Betika and Odibets are SECONDARY: queried per-match using the betradar ID
   as the join key.
3. All markets from all three are normalised into a single unified schema,
   merged into per-match documents, and checked for arbitrage.

Unified market schema
---------------------
{
    "1x2":              {"1": [...odds], "X": [...odds], "2": [...odds]},
    "double_chance":    {"1X": [...], "X2": [...], "12": [...]},
    "btts":             {"yes": [...], "no": [...]},
    "over_under_2.5":   {"over": [...], "under": [...]},
    "ht_1x2":           {"1": [...], "X": [...], "2": [...]},
    "handicap_0:1":     {"1": [...], "X": [...], "2": [...]},
    "correct_score":    {"1:0": [...], ...},
    "1st_goalscorer":   {"player_name": [...], ...},
    ...
}

Each odds entry: {"bookie": str, "odd": float}

Entry points
------------
    OddsAggregator(sport_config).run(max_matches=30)
        → list of unified match dicts with best_odds + arbitrage

    run_sport_aggregation(sport_config, **kwargs) → dict
        → single-sport result with stats

    run_all_sports(sports=None, max_workers=4) → dict
        → {sport_slug: result} for all configured sports

Classes
-------
    MarketNormalizer   — name/specifier/sub_type_id → canonical keys
    SportpesaFetcher   — fetch_matches() + fetch_markets() + parse_markets()
    BetikaFetcher      — fetch_matches() + fetch_markets() + parse_markets()
    OdibetsFetcher     — fetch_markets() → (raw_markets, meta)
    MarketMerger       — apply() merges raw list into unified dict; best_odds()
    ArbCalculator      — check() → list of arb opportunities with stake splits
    OddsAggregator     — run() ties it all together
"""

from __future__ import annotations

from datetime import timezone
import re
import time
import traceback
from typing import Any

import requests

# ─── Shared headers ──────────────────────────────────────────────────────────

_HEADERS_MOBILE = {
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Mobile) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
}
_HEADERS_SP = {
    **_HEADERS_MOBILE,
    "x-app-timezone": "Africa/Nairobi",
    "Origin": "https://www.ke.sportpesa.com",
    "Referer": "https://www.ke.sportpesa.com/",
}
_HEADERS_BT = {
    **_HEADERS_MOBILE,
    "Origin": "https://www.betika.com",
    "Referer": "https://www.betika.com/",
}
_HEADERS_OD = {
    **_HEADERS_MOBILE,
    "Origin": "https://odibets.com",
    "Referer": "https://odibets.com/",
}

# ─── Sport config — maps sport slug to each bookmaker's sport ID ─────────────

SPORT_CONFIG: list[dict] = [
    {"sport": "soccer",          "sportpesa_id": "1",   "betika_id": 14,  "odibets_id": "soccer"},
    {"sport": "basketball",      "sportpesa_id": "2",   "betika_id": 30,  "odibets_id": "basketball"},
    {"sport": "tennis",          "sportpesa_id": "5",   "betika_id": 28,  "odibets_id": "tennis"},
    {"sport": "ice-hockey",      "sportpesa_id": "4",   "betika_id": 29,  "odibets_id": "ice-hockey"},
    {"sport": "volleyball",      "sportpesa_id": "23",  "betika_id": 35,  "odibets_id": "volleyball"},
    {"sport": "cricket",         "sportpesa_id": "21",  "betika_id": 37,  "odibets_id": "cricket"},
    {"sport": "rugby",           "sportpesa_id": "12",  "betika_id": 41,  "odibets_id": "rugby"},
    {"sport": "boxing",          "sportpesa_id": "10",  "betika_id": 39,  "odibets_id": "boxing"},
    {"sport": "handball",        "sportpesa_id": "6",   "betika_id": 33,  "odibets_id": "handball"},
    {"sport": "mma",             "sportpesa_id": "117", "betika_id": 36,  "odibets_id": "mma"},
    {"sport": "table-tennis",    "sportpesa_id": "16",  "betika_id": 45,  "odibets_id": "table-tennis"},
    {"sport": "esoccer",         "sportpesa_id": "126", "betika_id": 105, "odibets_id": "esoccer"},
    {"sport": "snooker",         "sportpesa_id": None,  "betika_id": 34,  "odibets_id": "snooker"},
    {"sport": "darts",           "sportpesa_id": None,  "betika_id": 44,  "odibets_id": "darts"},
    {"sport": "american-football","sportpesa_id": None, "betika_id": 56,  "odibets_id": "american-football"},
]


# =============================================================================
# Market normaliser
# =============================================================================

class MarketNormalizer:
    """
    Converts raw market name + specifier + outcome key from any bookmaker
    into a canonical (market_key, outcome_key) pair.

    market_key examples:
        "1x2"                "double_chance"      "btts"
        "dnb"                "over_under_2.5"     "ht_over_under_0.5"
        "ht_1x2"             "handicap_0:1"       "ht_handicap_0:1"
        "winning_margin"     "correct_score"      "ht_correct_score"
        "1st_goalscorer"     "last_goalscorer"    "anytime_goalscorer"
        "ht_ftresult"        "corner_1x2"         "total_corners_9.5"
        "booking_1x2"        "total_bookings_2.5" "1st_corner"
        "1st_booking"        "red_card"           "brentford_clean_sheet"
        ...

    outcome_key examples:
        1x2          → "1"  "X"  "2"
        over_under   → "over"  "under"
        btts / dnb   → "yes"  "no" / "1"  "2"
        double_chance→ "1X"  "X2"  "12"
        scorers      → player name as-is (lowercased, comma-normalised)
    """

    # ── Sportpesa / Betika sub_type_id → canonical market name ───────────────
    _SUBTYPE_MAP: dict[str, str] = {
        # Soccer
        "1":   "1x2",
        "10":  "double_chance",
        "11":  "dnb",
        "14":  "handicap",
        "15":  "winning_margin",
        "18":  "over_under",
        "19":  "home_over_under",
        "20":  "away_over_under",
        "21":  "exact_goals",
        "23":  "home_exact_goals",
        "24":  "away_exact_goals",
        "29":  "btts",
        "31":  "home_clean_sheet",
        "32":  "away_clean_sheet",
        "33":  "home_win_to_nil",
        "34":  "away_win_to_nil",
        "35":  "1x2_and_btts",
        "36":  "over_under_and_btts",
        "37":  "1x2_and_over_under",
        "38":  "1st_goalscorer",
        "39":  "last_goalscorer",
        "40":  "anytime_goalscorer",
        "45":  "correct_score",
        "47":  "ht_ft_result",
        "48":  "home_win_both_halves",
        "49":  "away_win_both_halves",
        "50":  "home_win_either_half",
        "51":  "away_win_either_half",
        "55":  "ht_and_ft_btts",
        "56":  "home_score_both_halves",
        "57":  "away_score_both_halves",
        "58":  "both_halves_over_1.5",
        "59":  "both_halves_under_1.5",
        "60":  "ht_1x2",
        "63":  "ht_double_chance",
        "65":  "ht_handicap",
        "68":  "ht_over_under",
        "69":  "ht_home_over_under",
        "70":  "ht_away_over_under",
        "71":  "ht_exact_goals",
        "75":  "ht_btts",
        "78":  "ht_1x2_and_btts",
        "79":  "ht_1x2_and_over_under",
        "81":  "ht_correct_score",
        "105": "10min_1x2",
        "136": "booking_1x2",
        "137": "1st_booking",
        "139": "total_bookings",
        "146": "red_card",
        "149": "ht_booking_1x2",
        "152": "ht_total_bookings",
        "162": "corner_1x2",
        "163": "1st_corner",
        "166": "total_corners",
        "169": "corner_range",
        "173": "ht_corner_1x2",
        "174": "ht_1st_corner",
        "177": "ht_total_corners",
        "182": "ht_corner_range",
        "184": "1st_goal_and_1x2",
        "542": "ht_double_chance_and_btts",
        "543": "2h_1x2_and_btts",
        "544": "2h_1x2_and_over_under",
        "546": "double_chance_and_btts",
        "547": "double_chance_and_over_under",
        "548": "multigoals",
        "549": "home_multigoals",
        "550": "away_multigoals",
        "552": "ht_multigoals",
        "818": "ht_ft_and_over_under",
        # Basketball
        "382": "2way_ot",
        "51_bkt": "handicap_ot",
        "52_bkt": "total_points_ot",
        "42":  "ht_1x2_bkt",
        "53":  "ht_handicap_bkt",
        "54":  "ht_total_bkt",
        "45_bkt": "odd_even_ot",
        "224": "highest_scoring_quarter",
        "222": "winning_margin_bkt",
    }

    # ── Outcome key normalisers per market type ───────────────────────────────
    @staticmethod
    def _norm_val(v: str) -> str:
        """Clean a value string."""
        return str(v).strip()

    @staticmethod
    def normalize_market(name: str, specifier: Any = None, sub_type_id: str | None = None) -> str:
        """
        Return a canonical market key from market name / sub_type_id / specifier.
        """
        n = str(name).lower().strip()

        # ── Sub-type ID lookup (fastest path) ────────────────────────────────
        if sub_type_id is not None:
            sid = str(sub_type_id)
            mapped = MarketNormalizer._SUBTYPE_MAP.get(sid)
            if mapped:
                # Append line specifier for markets that use it
                if mapped in ("over_under", "ht_over_under", "home_over_under",
                              "away_over_under", "ht_home_over_under", "ht_away_over_under",
                              "total_corners", "ht_total_corners", "total_bookings",
                              "ht_total_bookings", "total_points_ot", "ht_total_bkt",
                              "handicap", "ht_handicap", "handicap_ot", "ht_handicap_bkt"):
                    line = MarketNormalizer._extract_line(specifier, n)
                    return f"{mapped}_{line}" if line else mapped
                return mapped

        # ── Name-based matching ───────────────────────────────────────────────
        if n in ("1x2", "3 way", "match winner", "fulltime", "winner", "1x2 / winner"):
            return "1x2"
        if n in ("double chance", "dc"):
            return "double_chance"
        if n in ("both teams to score", "gg/ng", "btts",
                 "both teams to score (gg/ng)", "both teams to score (gg\\/ng)"):
            return "btts"
        if n in ("draw no bet", "dnb", "who will win? (if draw, money back)"):
            return "dnb"
        if "halftime" in n and "fulltime" in n:
            return "ht_ft_result"
        if "ht" in n and "ft" in n and "result" in n:
            return "ht_ft_result"
        if "correct score" in n and "1st" not in n and "ht" not in n:
            return "correct_score"
        if ("correct score" in n or "halftime" in n) and ("1st" in n or "ht" in n):
            return "ht_correct_score"
        if "1st half" in n and "1x2" in n:
            return "ht_1x2"
        if "1st half" in n and "double chance" in n:
            return "ht_double_chance"
        if "1st half" in n and "both teams" in n:
            return "ht_btts"
        if "1st half" in n and "total" in n and "booking" in n:
            return "ht_total_bookings"
        if "1st half" in n and "total corner" in n:
            return "ht_total_corners"
        if "1st half" in n and "corner 1x2" in n:
            return "ht_corner_1x2"
        if "1st half" in n and "1st corner" in n:
            return "ht_1st_corner"
        if "1st half" in n and "handicap" in n:
            return "ht_handicap"
        if "1st half" in n and "exact goals" in n:
            return "ht_exact_goals"
        if "winning margin" in n:
            return "winning_margin"
        if "1st goalscorer" in n or "first goal" in n:
            return "1st_goalscorer"
        if "last goalscorer" in n:
            return "last_goalscorer"
        if "anytime goalscorer" in n:
            return "anytime_goalscorer"
        if "exact goals" in n and "1st" not in n and "ht" not in n:
            return "exact_goals"
        if "booking 1x2" in n:
            return "booking_1x2"
        if "1st booking" in n:
            return "1st_booking"
        if "total bookings" in n and "1st" not in n:
            return "total_bookings"
        if "red card" in n:
            return "red_card"
        if "corner 1x2" in n and "1st" not in n:
            return "corner_1x2"
        if "1st corner" in n:
            return "1st_corner"
        if "total corners" in n and "1st" not in n:
            return "total_corners"
        if "corner range" in n:
            return "corner_range"
        if "clean sheet" in n:
            team = "home" if ("home" in n or (specifier and "home" in str(specifier).lower())) else "away"
            return f"{team}_clean_sheet"
        if "win to nil" in n:
            team = "home" if "home" in n else "away"
            return f"{team}_win_to_nil"
        if "score in both halves" in n:
            team = "home" if "home" in n else "away"
            return f"{team}_score_both_halves"
        if "win both halves" in n:
            team = "home" if "home" in n else "away"
            return f"{team}_win_both_halves"
        if "win either half" in n:
            team = "home" if "home" in n else "away"
            return f"{team}_win_either_half"
        if ("over" in n and "under" in n) or n in ("total", "over/under"):
            line = MarketNormalizer._extract_line(specifier, n)
            base = "ht_over_under" if ("1st half" in n or "halftime" in n) else "over_under"
            return f"{base}_{line}" if line else base
        if "handicap" in n and "ht" not in n and "1st" not in n:
            line = MarketNormalizer._extract_line(specifier, n)
            return f"handicap_{line}" if line else "handicap"
        if "multigoal" in n:
            return "multigoals"
        if "1x2 & btts" in n or "1x2 and btts" in n:
            return "1x2_and_btts"
        if "1x2 & total" in n or "1x2 and total" in n:
            period = "ht_" if "1st half" in n else ("2h_" if "2nd half" in n else "")
            return f"{period}1x2_and_over_under"
        if "double chance & btts" in n:
            return "double_chance_and_btts"
        if "double chance & total" in n:
            return "double_chance_and_over_under"
        if "over/under & btts" in n or "total & btts" in n:
            line = MarketNormalizer._extract_line(specifier, n)
            return f"over_under_and_btts_{line}" if line else "over_under_and_btts"
        if "2 way" in n or "2way" in n:
            suffix = "ot" if "ot" in n else ""
            return f"2way_{suffix}".rstrip("_")
        if "total points" in n:
            line = MarketNormalizer._extract_line(specifier, n)
            period = "ht_" if "first half" in n else ""
            return f"{period}total_points_{line}" if line else f"{period}total_points"
        if "odd/even" in n:
            return "odd_even"
        if "highest scoring quarter" in n:
            return "highest_scoring_quarter"
        # Generic fallback
        slug = re.sub(r"[^a-z0-9]+", "_", n).strip("_")[:40]
        return slug or "unknown"

    @staticmethod
    def _extract_line(specifier: Any, name_fallback: str = "") -> str:
        """Extract the numeric line from a specifier or market name."""
        if specifier is not None:
            if isinstance(specifier, (int, float)):
                v = specifier
            elif isinstance(specifier, dict):
                v = (specifier.get("total") or specifier.get("hcp")
                     or specifier.get("points") or specifier.get("goals"))
                if v is None:
                    # e.g. {"hcp": "0:1"}
                    vals = list(specifier.values())
                    v = vals[0] if vals else None
            elif isinstance(specifier, str):
                m = re.search(r"(?:total|hcp|points|goals|from)\s*=?\s*(-?[\d.]+(?::-?[\d.]+)?)", specifier)
                v = m.group(1) if m else specifier
            else:
                v = None

            if v is not None:
                try:
                    f = float(v)
                    return str(int(f)) if f == int(f) else str(f)
                except (TypeError, ValueError):
                    return re.sub(r"\s+", "", str(v))  # e.g. "0:1"

        # Fall back to extracting from market name
        m = re.search(r"(-?\d+\.?\d*)", name_fallback)
        return m.group(1) if m else ""

    @staticmethod
    def normalize_outcome(market_key: str, raw_key: str, display: str = "") -> str:
        """
        Return a canonical outcome key.
        """
        k  = str(raw_key).strip().lower()
        d  = str(display).strip().lower()

        if "1x2" in market_key and "&" not in market_key:
            if k in ("1", "home", "{$competitor1}") or d == "1": return "1"
            if k in ("x", "draw", "draw")                      : return "X"
            if k in ("2", "away", "{$competitor2}") or d == "2": return "2"

        if market_key in ("double_chance", "ht_double_chance"):
            if "1" in k and "x" in k: return "1X"
            if "x" in k and "2" in k: return "X2"
            if "1" in k and "2" in k: return "12"

        if market_key in ("btts", "ht_btts", "red_card",
                          "home_clean_sheet", "away_clean_sheet",
                          "home_win_to_nil", "away_win_to_nil",
                          "home_win_both_halves", "away_win_both_halves",
                          "home_win_either_half", "away_win_either_half",
                          "home_score_both_halves", "away_score_both_halves",
                          "both_halves_over_1.5", "both_halves_under_1.5"):
            if k in ("yes", "gg"): return "yes"
            if k in ("no",  "ng"): return "no"

        if market_key in ("dnb",):
            if k in ("1", "home"): return "1"
            if k in ("2", "away"): return "2"

        if "over_under" in market_key or "total" in market_key:
            if "ov" in k or "over" in d:  return "over"
            if "un" in k or "under" in d: return "under"

        if "handicap" in market_key:
            if k in ("1", "home") or d in ("1", "home"): return "1"
            if k in ("x", "draw")                      : return "X"
            if k in ("2", "away") or d in ("2", "away"): return "2"
            # specifier-based e.g. "Brentford (0:1)" → keep as-is
            return re.sub(r"\s+", "_", k)[:30]

        if "goalscorer" in market_key:
            # Normalise player names: "Schade, Kevin" → "schade_kevin"
            return re.sub(r"[^a-z0-9]+", "_", k).strip("_")

        if market_key == "correct_score" or market_key == "ht_correct_score":
            # "1:0", "2:1" etc — pass through
            m = re.search(r"(\d+:\d+|other)", k)
            return m.group(1) if m else k[:10]

        if market_key == "ht_ft_result":
            # "1/1", "x/2" etc
            m = re.search(r"([12x])[/\\]([12x])", k)
            if m: return f"{m.group(1).upper()}/{m.group(2).upper()}"
            return k[:5].upper()

        if market_key == "winning_margin":
            return re.sub(r"\s+", "_", d or k)[:20]

        if market_key == "multigoals":
            return re.sub(r"\s+", "_", d or k)[:10]

        if "1x2_and_btts" in market_key:
            # e.g. "1 & YES" → "1_yes"
            parts = re.findall(r"([12x]|yes|no)", k)
            return "_".join(parts) if parts else k[:10]

        if "1x2_and_over_under" in market_key:
            parts = re.findall(r"([12x]|over|under)", k)
            return "_".join(parts) if parts else k[:15]

        if "double_chance_and" in market_key:
            parts = re.findall(r"(1x|x2|12|yes|no|over|under)", k)
            return "_".join(parts) if parts else k[:15]

        if market_key == "highest_scoring_quarter":
            return re.sub(r"\s+", "_", d or k).lower()[:10]

        if market_key == "odd_even":
            if "odd" in k: return "odd"
            if "even" in k: return "even"

        if market_key == "2way_ot":
            if k in ("1", "home"): return "1"
            if k in ("2", "away"): return "2"

        if "corner" in market_key and "1x2" in market_key:
            if k == "1": return "1"
            if k in ("x", "draw"): return "X"
            if k == "2": return "2"

        if "1st_corner" in market_key or "1st_booking" in market_key or "1st_goal" in market_key:
            if k == "none": return "none"
            if k in ("1", "home"): return "1"
            if k in ("2", "away"): return "2"

        # Generic fallback — use display name cleaned, or raw key
        return re.sub(r"[^a-z0-9_/:]", "_", d or k).strip("_")[:30] or k[:15]


# =============================================================================
# Sportpesa fetcher
# =============================================================================

class SportpesaFetcher:
    """
    Fetches upcoming matches and their full market data from Sportpesa Kenya.

    Step 1: GET  /api/upcoming/games  → list of matches with betradarId + internal game_id
    Step 2: GET  /api/games/markets?games={game_id}&markets=all → full markets per match
    """
    BASE = "https://www.ke.sportpesa.com"

    @staticmethod
    def fetch_matches(sport_id: str, days: int = 5, pages: int = 5,
                      page_size: int = 20) -> list[dict]:
        """
        Returns a list of raw match dicts. Each dict has:
            betradar_id, game_id, home, away, start_time, competition, sport, markets[]
        """
        import datetime
        results: list[dict] = []
        seen_ids: set[str] = set()

        for day_offset in range(days):
            base_dt  =  datetime.datetime.now(timezone.utc) + datetime.timedelta(hours=3)  # EAT
            day_start = base_dt + datetime.timedelta(days=day_offset)
            day_end   = day_start + datetime.timedelta(days=1)
            ts_start  = int(day_start.replace(hour=0, minute=0, second=0).timestamp())
            ts_end    = int(day_end.replace(hour=0, minute=0, second=0).timestamp())

            for page in range(pages):
                url = (
                    f"{SportpesaFetcher.BASE}/api/upcoming/games"
                    f"?type=prematch&sportId={sport_id}"
                    f"&section=upcoming&markets_layout=multiple"
                    f"&o=leagues&pag_count={page_size}&pag_min={page * page_size}"
                    f"&from={ts_start}&to={ts_end}"
                )
                try:
                    resp = requests.get(url, headers=_HEADERS_SP, timeout=15)
                    if not resp.ok:
                        break
                    matches = resp.json()
                    if not matches or not isinstance(matches, list):
                        break

                    for m in matches:
                        betradar_id = str(m.get("betradarId") or "")
                        game_id     = str(m.get("id") or "")
                        if not betradar_id or betradar_id == "None":
                            continue
                        if game_id in seen_ids:
                            continue
                        seen_ids.add(game_id)

                        home = (m.get("competitors") or [{}])[0].get("name", "")
                        away = (m.get("competitors") or [{}, {}])[1].get("name", "")

                        # league / competition / sport may be dicts {id, name} — extract .name
                        def _str_field(v):
                            if isinstance(v, dict):
                                return str(v.get("name") or v.get("title") or "")
                            return str(v) if v else ""

                        results.append({
                            "betradar_id":    betradar_id,
                            "game_id":        game_id,
                            "home_team":      home,
                            "away_team":      away,
                            "start_time":     m.get("date"),
                            "competition":    _str_field(m.get("league") or m.get("competition")),
                            "sport":          _str_field(m.get("sport")),
                            "sp_markets_raw": m.get("markets") or [],  # inline markets
                        })

                except Exception as exc:
                    print(f"[sportpesa] list error day={day_offset} page={page}: {exc}")
                    break

        return results

    @staticmethod
    def fetch_markets(game_id: str) -> list[dict]:
        """Fetch full market book for a single Sportpesa game_id."""
        url = (
            f"{SportpesaFetcher.BASE}/api/games/markets"
            f"?games={game_id}&markets=all"
        )
        try:
            resp = requests.get(url, headers=_HEADERS_SP, timeout=15)
            if not resp.ok:
                return []
            raw = resp.json()
            # Response is {game_id: [market, ...]}
            return raw.get(str(game_id), [])
        except Exception as exc:
            print(f"[sportpesa] markets error game_id={game_id}: {exc}")
            return []

    @staticmethod
    def parse_markets(markets_list: list[dict], bookie_name: str = "Sportpesa") -> list[dict]:
        """
        Convert Sportpesa market list → normalised raw_markets format.

        Output format: [
          {
            "name": str,
            "sub_type_id": str,
            "specifier": Any,
            "outcomes": [{"key": str, "display": str, "value": float}]
          }
        ]
        """
        raw_markets: list[dict] = []
        # Group entries with the same (id, specValue) — they are one market
        seen: dict[tuple, dict] = {}

        for mkt in (markets_list or []):
            mid     = str(mkt.get("id", ""))
            spec    = mkt.get("specValue")
            key     = (mid, spec)
            name    = mkt.get("name", "")

            outcomes: list[dict] = []
            for sel in (mkt.get("selections") or []):
                try:
                    val = float(sel.get("odds", 0))
                except (TypeError, ValueError):
                    val = 0.0
                if val <= 1.0:
                    continue
                outcomes.append({
                    "key":     sel.get("shortName") or sel.get("name", ""),
                    "display": sel.get("name", ""),
                    "value":   val,
                })

            if not outcomes:
                continue

            if key not in seen:
                seen[key] = {"name": name, "sub_type_id": mid,
                             "specifier": spec, "outcomes": outcomes}
            else:
                # Merge outcomes (different spec lines)
                seen[key]["outcomes"].extend(outcomes)

        return list(seen.values())


# =============================================================================
# Betika fetcher
# =============================================================================

class BetikaFetcher:
    """
    Fetches upcoming matches and full market data from Betika Kenya.

    Step 1: GET  /v1/uo/matches?sport_id=...  → match list with parent_match_id (betradar)
    Step 2: GET  /v1/uo/match?parent_match_id=...  → full markets for one match
    """
    BASE = "https://api.betika.com"

    @staticmethod
    def fetch_matches(sport_id: int, per_page: int = 50,
                      max_pages: int = 5) -> list[dict]:
        """Returns list of match dicts with betradar_id, team names, etc."""
        results: list[dict] = []
        seen_ids: set[str] = set()

        for page in range(1, max_pages + 1):
            url = (
                f"{BetikaFetcher.BASE}/v1/uo/matches"
                f"?sport_id={sport_id}&per_page={per_page}&page={page}&tab=all"
            )
            try:
                resp = requests.get(url, headers=_HEADERS_BT, timeout=15)
                if not resp.ok:
                    break
                payload = resp.json()
                data    = payload.get("data") or []
                if not data:
                    break

                for m in data:
                    betradar_id = str(m.get("parent_match_id") or "")
                    if not betradar_id:
                        continue
                    if betradar_id in seen_ids:
                        continue
                    seen_ids.add(betradar_id)

                    # Basic 1X2 odds are included inline
                    inline_odds: list[dict] = []
                    for mkt in (m.get("odds") or []):
                        for o in (mkt.get("odds") or []):
                            try: val = float(o.get("odd_value", 0))
                            except: val = 0.0
                            if val <= 1.0: continue
                            inline_odds.append({
                                "key":     o.get("odd_key", ""),
                                "display": o.get("display", ""),
                                "value":   val,
                            })

                    results.append({
                        "betradar_id": betradar_id,
                        "home_team":   m.get("home_team", ""),
                        "away_team":   m.get("away_team", ""),
                        "start_time":  m.get("start_time"),
                        "competition": m.get("competition_name", ""),
                        "category":    m.get("category", ""),
                        "sport_id":    sport_id,
                        "bt_inline_markets": m.get("odds") or [],  # basic 1X2
                        "bt_game_id":  m.get("game_id", ""),
                        "bt_match_id": m.get("match_id", ""),
                    })

                meta = payload.get("meta") or {}
                if page * per_page >= int(meta.get("total", 0)):
                    break

            except Exception as exc:
                print(f"[betika] list error sport={sport_id} page={page}: {exc}")
                break

        return results

    @staticmethod
    def fetch_markets(parent_match_id: str) -> list[dict]:
        """Fetch full markets for one match by betradar parent_match_id."""
        url = f"{BetikaFetcher.BASE}/v1/uo/match?parent_match_id={parent_match_id}"
        try:
            resp = requests.get(url, headers=_HEADERS_BT, timeout=15)
            if not resp.ok:
                return []
            payload = resp.json()
            # Response can be {"data": [...]} or {"data": null}
            data = payload.get("data") or []
            if not isinstance(data, list):
                return []
            return data
        except Exception as exc:
            print(f"[betika] markets error id={parent_match_id}: {exc}")
            return []

    @staticmethod
    def parse_markets(raw_data: list[dict]) -> list[dict]:
        """
        Convert Betika /uo/match response → normalised raw_markets format.
        """
        raw_markets: list[dict] = []

        for mkt in (raw_data or []):
            name      = mkt.get("name", "")
            sub_id    = str(mkt.get("sub_type_id", ""))
            odds_list = mkt.get("odds") or []

            # Extract specifier from first outcome
            spec = None
            if odds_list:
                first_pbv = odds_list[0].get("parsed_special_bet_value") or {}
                if "total" in first_pbv:
                    spec = first_pbv["total"]
                elif "hcp" in first_pbv:
                    spec = first_pbv["hcp"]
                elif first_pbv and list(first_pbv.keys()) != [""]:
                    spec = first_pbv

            outcomes: list[dict] = []
            for o in odds_list:
                try: val = float(o.get("odd_value", 0))
                except: val = 0.0
                if val <= 1.0:
                    continue
                # For markets with multiple specifiers (e.g. total=0.5, total=1.5)
                # each outcome has its own specifier
                o_spec = None
                o_pbv  = o.get("parsed_special_bet_value") or {}
                if o_pbv and list(o_pbv.keys()) != [""]:
                    o_spec = o_pbv.get("total") or o_pbv.get("hcp") or o_pbv

                outcomes.append({
                    "key":     o.get("odd_key", ""),
                    "display": o.get("display", ""),
                    "value":   val,
                    "spec":    o_spec,  # per-outcome specifier (Total market has these)
                })

            if not outcomes:
                continue

            # For markets where each outcome has its own spec (TOTAL), split them
            if any(o.get("spec") for o in outcomes):
                # Group by spec — spec can be a dict (unhashable), so we use a
                # stable string key for grouping and store the original spec value
                def _spec_key(v: Any) -> str:
                    """Convert any spec value to a hashable string key."""
                    if v is None:
                        return "__none__"
                    if isinstance(v, (int, float, str)):
                        return str(v)
                    if isinstance(v, dict):
                        # e.g. {"total": 2.5} → "total=2.5"
                        return "&".join(f"{k}={v2}" for k, v2 in sorted(v.items()))
                    return str(v)

                by_spec: dict[str, list] = {}       # str key → [outcomes]
                spec_val: dict[str, Any] = {}        # str key → original spec value

                for o in outcomes:
                    raw_s = o.get("spec") or spec
                    sk    = _spec_key(raw_s)
                    by_spec.setdefault(sk, []).append(o)
                    spec_val[sk] = raw_s              # keep first seen original value

                for sk, outs in by_spec.items():
                    raw_markets.append({
                        "name":        name,
                        "sub_type_id": sub_id,
                        "specifier":   spec_val[sk],  # original value (may be dict)
                        "outcomes":    outs,
                    })
            else:
                raw_markets.append({
                    "name":        name,
                    "sub_type_id": sub_id,
                    "specifier":   spec,
                    "outcomes":    outcomes,
                })

        return raw_markets


# =============================================================================
# Odibets fetcher
# =============================================================================

class OdibetsFetcher:
    """
    Fetches from Odibets using the betradar match ID directly.

    GET https://api.odi.site/sportsbook/v1
        ?id={betradar_id}&category_id=&sub_type_id=&sportsbook=sportsbook
    """
    BASE = "https://api.odi.site"

    @staticmethod
    def fetch_markets(parent_match_id: str) -> tuple[list[dict], dict]:
        """
        Returns (raw_markets_list, meta_dict).
        meta_dict may contain home_team / away_team if available.
        """
        url = (
            f"{OdibetsFetcher.BASE}/sportsbook/v1"
            f"?id={parent_match_id}&category_id=&sub_type_id=&sportsbook=sportsbook"
        )
        try:
            resp = requests.get(url, headers=_HEADERS_OD, timeout=15)
            if not resp.ok:
                return [], {}
            payload = resp.json()
            data = payload.get("data")
            if not data or not isinstance(data, dict):
                return [], {}

            raw_markets: list[dict] = []
            meta_map = {m["sub_type_id"]: m for m in (data.get("markets_list") or [])}
            markets  = data.get("markets") or []

            for m in markets:
                sub_id   = str(m.get("sub_type_id", ""))
                meta     = meta_map.get(m.get("sub_type_id"), {})
                mkt_name = meta.get("odd_type") or m.get("market_name") or "Unknown"

                spec = meta.get("specifiers")
                if not spec and m.get("outcomes"):
                    spec = m["outcomes"][0].get("specifiers")

                outcomes: list[dict] = []
                for o in (m.get("outcomes") or []):
                    try: val = float(o.get("odd_value", 0))
                    except: val = 0.0
                    if val <= 1.0:
                        continue
                    outcomes.append({
                        "key":     o.get("outcome_key", ""),
                        "display": o.get("outcome_name", ""),
                        "value":   val,
                    })

                if not outcomes:
                    continue

                raw_markets.append({
                    "name":        mkt_name,
                    "sub_type_id": sub_id,
                    "specifier":   spec,
                    "outcomes":    outcomes,
                })

            meta_out = {}
            if "team_home" in data:
                meta_out["home_team"] = data["team_home"]
            if "team_away" in data:
                meta_out["away_team"] = data["team_away"]

            return raw_markets, meta_out

        except Exception as exc:
            print(f"[odibets] error id={parent_match_id}: {exc}")
            return [], {}


# =============================================================================
# Unified market merger
# =============================================================================

class MarketMerger:
    """
    Merges raw_markets lists from multiple bookmakers into a single
    unified_market_data dict.

    unified_market_data shape:
    {
        "1x2": {
            "1": [{"bookie": "Sportpesa", "odd": 1.61}, ...],
            "X": [...],
            "2": [...],
        },
        "over_under_2.5": {
            "over":  [...],
            "under": [...],
        },
        ...
    }
    """

    @staticmethod
    def apply(current: dict, raw_markets: list[dict], bookie_name: str) -> dict:
        """
        Add odds from raw_markets into current dict, removing old entries from
        this bookie_name first.
        """
        data = {k: {ok: list(ov) for ok, ov in outs.items()}
                for k, outs in current.items()}

        for raw in raw_markets:
            mkt_key = MarketNormalizer.normalize_market(
                raw.get("name", ""),
                raw.get("specifier"),
                raw.get("sub_type_id"),
            )
            if mkt_key not in data:
                data[mkt_key] = {}

            for outcome in raw.get("outcomes") or []:
                out_key = MarketNormalizer.normalize_outcome(
                    mkt_key,
                    outcome.get("key", ""),
                    outcome.get("display", ""),
                )
                if out_key not in data[mkt_key]:
                    data[mkt_key][out_key] = []

                # Remove stale odds from this bookie
                data[mkt_key][out_key] = [
                    x for x in data[mkt_key][out_key]
                    if x.get("bookie") != bookie_name
                ]

                try:
                    val = float(outcome["value"])
                    if val > 1.0:
                        data[mkt_key][out_key].append({
                            "bookie": bookie_name,
                            "odd":    val,
                        })
                except (TypeError, ValueError, KeyError):
                    pass

        return data

    @staticmethod
    def best_odds(unified: dict) -> dict:
        """
        Return a summary dict showing the best available odd per outcome.
        {market: {outcome: {"odd": float, "bookie": str}}}
        """
        best: dict = {}
        for mkt, outcomes in unified.items():
            best[mkt] = {}
            for out, entries in outcomes.items():
                valid = [e for e in entries if e.get("odd", 0) > 1.0]
                if not valid:
                    continue
                top = max(valid, key=lambda x: x["odd"])
                best[mkt][out] = {"odd": top["odd"], "bookie": top["bookie"]}
        return best


# =============================================================================
# Arbitrage calculator
# =============================================================================

class ArbCalculator:
    """
    Checks for arbitrage in a unified market dict.
    Returns None if no arb exists, or:
    {
        "market": str,
        "profit_pct": float,
        "implied_prob": float,
        "bets": [{"outcome": str, "bookie": str, "odd": float, "stake_pct": float}]
    }
    """
    _REQUIRED: dict[str, list[str]] = {
        "1x2":          ["1", "X", "2"],
        "double_chance":["1X", "X2", "12"],
        "btts":         ["yes", "no"],
        "dnb":          ["1", "2"],
        "2way_ot":      ["1", "2"],
    }

    @staticmethod
    def check(unified: dict) -> list[dict]:
        opportunities: list[dict] = []
        best = MarketMerger.best_odds(unified)

        for mkt, outcomes in best.items():
            # Determine required outcomes
            required: list[str] | None = None
            for pattern, reqs in ArbCalculator._REQUIRED.items():
                if mkt == pattern or mkt.startswith(pattern + "_"):
                    # Check that it only has exactly these outcomes
                    required = reqs
                    break
            if required is None:
                # For over/under markets
                if "over_under" in mkt or "total" in mkt:
                    required = ["over", "under"]
                elif "handicap" in mkt and "1x2" not in mkt:
                    required = ["1", "2"]  # Asian handicap
                else:
                    continue

            best_per_out: dict[str, dict] = {}
            for req in required:
                if req not in outcomes:
                    break
                best_per_out[req] = outcomes[req]
            else:
                total_prob = sum(1.0 / e["odd"] for e in best_per_out.values())
                if 0 < total_prob < 1.0:
                    bets = [
                        {
                            "outcome":   out,
                            "bookie":    entry["bookie"],
                            "odd":       entry["odd"],
                            "stake_pct": round(((1.0 / entry["odd"]) / total_prob) * 100, 2),
                        }
                        for out, entry in best_per_out.items()
                    ]
                    opportunities.append({
                        "market":       mkt,
                        "profit_pct":   round((1.0 - total_prob) * 100, 2),
                        "implied_prob": round(total_prob * 100, 2),
                        "bets":         bets,
                    })

        return sorted(opportunities, key=lambda x: -x["profit_pct"])


# =============================================================================
# Main aggregation engine (no Celery dependency — can be called from tasks)
# =============================================================================

class OddsAggregator:
    """
    Full pipeline: fetch → parse → merge → arbitrage check.

    Usage:
        agg = OddsAggregator(sport_config)
        results = agg.run()
        # results: list of unified match dicts

    Each unified match dict:
    {
        "betradar_id": str,
        "home_team":   str,
        "away_team":   str,
        "start_time":  str,
        "competition": str,
        "sport":       str,

        # Per-bookmaker raw odds
        "bookmakers": {
            "Sportpesa": {"raw_markets": [...], "fetched_at": ...},
            "Betika":    {"raw_markets": [...], "fetched_at": ...},
            "Odibets":   {"raw_markets": [...], "fetched_at": ...},
        },

        # Unified merged market data (list of odds per outcome)
        "unified_markets": { "1x2": {"1": [...], "X": [...], "2": [...]}, ... },

        # Best odds summary
        "best_odds": { "1x2": {"1": {"odd": 1.73, "bookie": "Betika"}, ...}, ... },

        # Arbitrage opportunities found
        "arbitrage": [{"market": ..., "profit_pct": ..., "bets": [...]}, ...],
    }
    """

    def __init__(self, sport_config: dict,
                 fetch_full_sp_markets: bool = True,
                 fetch_full_bt_markets: bool = True,
                 fetch_od_markets:      bool = True):
        self.sport_config         = sport_config
        self.fetch_full_sp        = fetch_full_sp_markets
        self.fetch_full_bt        = fetch_full_bt_markets
        self.fetch_od             = fetch_od_markets

    def run(self, max_matches: int | None = None) -> list[dict]:
        sport    = self.sport_config.get("sport", "soccer")
        sp_id    = self.sport_config.get("sportpesa_id")
        bt_id    = self.sport_config.get("betika_id")

        print(f"[aggregator] ── {sport.upper()} ─────────────────────────────")

        # ── 1. Fetch Sportpesa match list ─────────────────────────────────────
        sp_matches: list[dict] = []
        if sp_id:
            print(f"[aggregator] Sportpesa fetch sport_id={sp_id}…")
            sp_matches = SportpesaFetcher.fetch_matches(sp_id)
            print(f"[aggregator] Sportpesa: {len(sp_matches)} matches")
        else:
            print(f"[aggregator] Sportpesa: no sport_id for {sport}")

        # ── 2. Fetch Betika match list ────────────────────────────────────────
        bt_by_brid: dict[str, dict] = {}   # betradar_id → betika match dict
        if bt_id:
            print(f"[aggregator] Betika fetch sport_id={bt_id}…")
            bt_list = BetikaFetcher.fetch_matches(bt_id)
            bt_by_brid = {m["betradar_id"]: m for m in bt_list if m.get("betradar_id")}
            print(f"[aggregator] Betika: {len(bt_by_brid)} matches")
        else:
            print(f"[aggregator] Betika: no sport_id for {sport}")

        # ── 3. Build unified match index from both sources ────────────────────
        all_brids: list[str] = []
        match_info: dict[str, dict] = {}

        # Primary: Sportpesa
        for m in sp_matches:
            brid = m["betradar_id"]
            if brid not in match_info:
                all_brids.append(brid)
            match_info[brid] = {
                "betradar_id": brid,
                "home_team":   m["home_team"],
                "away_team":   m["away_team"],
                "start_time":  m.get("start_time"),
                "competition": m.get("competition", ""),
                "sport":       m.get("sport", sport),
                "sp_game_id":  m.get("game_id", ""),
            }

        # Add Betika-only matches
        for brid, m in bt_by_brid.items():
            if brid not in match_info:
                all_brids.append(brid)
                match_info[brid] = {
                    "betradar_id": brid,
                    "home_team":   m.get("home_team", ""),
                    "away_team":   m.get("away_team", ""),
                    "start_time":  m.get("start_time"),
                    "competition": m.get("competition", ""),
                    "sport":       sport,
                    "sp_game_id":  "",
                }

        if max_matches:
            all_brids = all_brids[:max_matches]

        print(f"[aggregator] Total unique matches: {len(all_brids)}")

        # ── 4. Per-match: fetch full markets + merge ──────────────────────────
        unified_list: list[dict] = []

        for i, brid in enumerate(all_brids):
            info = match_info[brid]
            unified_mkts: dict = {}
            bk_data: dict = {}

            # ── Sportpesa ─────────────────────────────────────────────────────
            sp_game_id = info.get("sp_game_id", "")
            if sp_game_id:
                # Use inline markets first (already fetched from list)
                sp_inline = [m for m in sp_matches if m.get("game_id") == sp_game_id]
                sp_raw_mkts = []
                if sp_inline:
                    sp_raw_mkts = SportpesaFetcher.parse_markets(
                        sp_inline[0].get("sp_markets_raw", []))

                if self.fetch_full_sp:
                    full_mkts = SportpesaFetcher.fetch_markets(sp_game_id)
                    if full_mkts:
                        sp_raw_mkts = SportpesaFetcher.parse_markets(full_mkts)

                if sp_raw_mkts:
                    unified_mkts = MarketMerger.apply(unified_mkts, sp_raw_mkts, "Sportpesa")
                    bk_data["Sportpesa"] = {
                        "raw_markets": sp_raw_mkts,
                        "fetched_at":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    }

            # ── Betika ────────────────────────────────────────────────────────
            bt_match = bt_by_brid.get(brid)
            if bt_match:
                bt_raw_mkts: list[dict] = []

                if self.fetch_full_bt:
                    full = BetikaFetcher.fetch_markets(brid)
                    if full:
                        bt_raw_mkts = BetikaFetcher.parse_markets(full)
                    else:
                        # Fall back to inline
                        bt_raw_mkts = BetikaFetcher.parse_markets(
                            bt_match.get("bt_inline_markets", []))
                else:
                    bt_raw_mkts = BetikaFetcher.parse_markets(
                        bt_match.get("bt_inline_markets", []))

                if bt_raw_mkts:
                    unified_mkts = MarketMerger.apply(unified_mkts, bt_raw_mkts, "Betika")
                    bk_data["Betika"] = {
                        "raw_markets": bt_raw_mkts,
                        "fetched_at":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    }

            # ── Odibets ───────────────────────────────────────────────────────
            if self.fetch_od:
                od_raw, od_meta = OdibetsFetcher.fetch_markets(brid)
                if od_raw:
                    unified_mkts = MarketMerger.apply(unified_mkts, od_raw, "Odibets")
                    bk_data["Odibets"] = {
                        "raw_markets": od_raw,
                        "fetched_at":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    }

            # ── Build output ──────────────────────────────────────────────────
            best  = MarketMerger.best_odds(unified_mkts)
            arbs  = ArbCalculator.check(unified_mkts)

            entry = {
                **info,
                "bookmakers":     bk_data,
                "unified_markets": unified_mkts,
                "best_odds":       best,
                "arbitrage":       arbs,
                "market_count":    len(unified_mkts),
                "bookie_count":    len(bk_data),
            }
            unified_list.append(entry)

            if arbs:
                best_arb = arbs[0]
                print(
                    f"[aggregator] ARB {info['home_team'][:15]} v {info['away_team'][:15]}"
                    f" → {best_arb['market']} +{best_arb['profit_pct']:.1f}%"
                )

            if (i + 1) % 20 == 0:
                print(f"[aggregator] Progress: {i+1}/{len(all_brids)}")

        print(f"[aggregator] {sport}: {len(unified_list)} matches processed, "
              f"{sum(1 for m in unified_list if m['arbitrage'])} with arb")
        return unified_list


# =============================================================================
# Celery task wrappers
# =============================================================================

def run_sport_aggregation(sport_config: dict, **kwargs) -> dict:
    """
    Top-level entry point for Celery tasks.
    Returns the aggregated result dict.
    """
    try:
        agg = OddsAggregator(sport_config, **kwargs)
        matches = agg.run()
        return {
            "sport":       sport_config.get("sport"),
            "match_count": len(matches),
            "arb_count":   sum(1 for m in matches if m["arbitrage"]),
            "matches":     matches,
        }
    except Exception as exc:
        print(f"[aggregator] FATAL {sport_config.get('sport')}: {exc}")
        traceback.print_exc()
        return {"sport": sport_config.get("sport"), "error": str(exc), "matches": []}


def run_all_sports(sports: list[str] | None = None,
                   max_workers: int = 4, **kwargs) -> dict:
    """
    Run aggregation for all (or selected) sports concurrently.
    Returns {sport_name: aggregation_result}
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    configs = [c for c in SPORT_CONFIG
               if (sports is None or c["sport"] in sports)]

    results: dict[str, dict] = {}

    def _run(cfg: dict) -> tuple[str, dict]:
        return cfg["sport"], run_sport_aggregation(cfg, **kwargs)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_run, cfg): cfg for cfg in configs}
        for fut in as_completed(futures):
            sport, data = fut.result()
            results[sport] = data

    total_matches = sum(d.get("match_count", 0) for d in results.values())
    total_arbs    = sum(d.get("arb_count", 0)   for d in results.values())
    print(f"[aggregator] ALL SPORTS: {total_matches} matches, {total_arbs} arb opportunities")
    return results


# =============================================================================
# Standalone test runner
# =============================================================================

if __name__ == "__main__":
    import json

    soccer_cfg = next(c for c in SPORT_CONFIG if c["sport"] == "soccer")
    result = run_sport_aggregation(
        soccer_cfg,
        fetch_full_sp_markets=True,
        fetch_full_bt_markets=True,
        fetch_od_markets=True,
    )

    print(f"\nTotal matches: {result['match_count']}")
    print(f"Arb opportunities: {result['arb_count']}")

    # Show first 3 matches
    for m in result["matches"][:3]:
        print(f"\n{m['home_team']} v {m['away_team']} [{m['competition']}]")
        print(f"  Bookmakers: {list(m['bookmakers'].keys())}")
        print(f"  Markets: {m['market_count']}")
        if m["arbitrage"]:
            for arb in m["arbitrage"][:2]:
                print(f"  🔥 ARB {arb['market']}: +{arb['profit_pct']:.2f}%")
                for bet in arb["bets"]:
                    print(f"       {bet['bookie']:10} {bet['outcome']:5} @{bet['odd']:.2f}  stake {bet['stake_pct']:.1f}%")

        # Sample best odds
        for mkt in ["1x2", "btts", "over_under_2.5"]:
            if mkt in m["best_odds"]:
                print(f"  {mkt}: {m['best_odds'][mkt]}")