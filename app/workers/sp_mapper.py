"""
app/workers/sp_mapper.py
=========================
Sportpesa market-ID → canonical slug mapping for EVERY sport using class-based routing.

FIX: eFootball (sport_id 126) now uses dedicated mapper, so Over/Under markets appear.
"""

from __future__ import annotations
import re
from typing import Any

# ══════════════════════════════════════════════════════════════════════════════
# 1. SPORT-SPECIFIC MAPPERS
# ══════════════════════════════════════════════════════════════════════════════

class BaseMapper:
    @staticmethod
    def format_line(spec_value: float) -> str:
        if spec_value == 0:
            return "0_0"
        val_str = f"{spec_value:g}".replace(".", "_")
        return val_str.replace("-", "minus_") if spec_value < 0 else val_str


class SportpesaFootballMapper(BaseMapper):
    STATIC_MARKETS = {
        10: "1x2", 1: "1x2", 381: "1x2", 46: "double_chance", 43: "btts", 29: "btts",
        47: "draw_no_bet", 42: "first_half_1x2", 60: "first_half_1x2", 328: "first_half_btts",
        203: "first_half_correct_score", 207: "highest_scoring_half", 44: "ht_ft",
        332: "correct_score", 258: "exact_goals", 45: "odd_even", 202: "goal_groups",
        41: "first_team_to_score", 386: "btts_and_result", 162: "total_corners", 136: "total_bookings",
    }

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        # Over/Under – includes ID 52 (soccer) and 56 (eFootball fallback)
        if sp_id in [52, 18, 56]:
            return f"over_under_goals_{line}"
        if sp_id in [54, 15, 68]:
            return f"first_half_over_under_{line}"
        if sp_id == 353:
            return f"home_goals_{line}"
        if sp_id == 352:
            return f"away_goals_{line}"
        if sp_id == 51:
            return f"asian_handicap_{line}"
        if sp_id == 53:
            return f"first_half_asian_handicap_{line}"
        if sp_id == 55:
            prefix = "plus" if spec_value > 0 else "minus"
            return f"european_handicap_{prefix}_{abs(int(spec_value))}"
        if sp_id == 208:
            return f"result_and_over_under_{line}"
        if sp_id == 166:
            return f"total_corners_{line}"
        if sp_id == 139:
            return f"total_bookings_{line}"
        return None


class SportpesaEFootballMapper(BaseMapper):
    """Dedicated mapper for eFootball (virtual football)."""
    STATIC_MARKETS = {
        381: "efootball_1x2",
        46:  "efootball_double_chance",
    }

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]

        line = cls.format_line(spec_value)

        if sp_id == 56:   # Total Goals Over/Under
            return f"over_under_efootball_goals_{line}"
        if sp_id == 51:   # Asian Handicap
            if spec_value == 0:
                return "efootball_ah_0_0"
            prefix = "plus" if spec_value > 0 else "minus"
            val = str(abs(spec_value)).replace(".", "_")
            return f"efootball_ah_{prefix}_{val}"
        # Fallback to football mapper for other IDs (e.g., 1x2)
        return SportpesaFootballMapper.get_market_slug(sp_id, spec_value)


class SportpesaBasketballMapper(BaseMapper):
    STATIC_MARKETS = {
        382: "match_winner", 42: "first_half_winner", 45: "odd_even",
        224: "highest_scoring_quarter", 222: "winning_margin",
        10: "1x2", 1: "1x2", 381: "1x2", 47: "draw_no_bet", 44: "ht_ft"
    }

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)

        if sp_id == 51:
            return f"asian_handicap_{line}"
        if sp_id == 52:
            return f"over_under_goals_{line}"
        if sp_id == 53:
            return f"first_half_asian_handicap_{line}"
        if sp_id == 54:
            return f"first_half_over_under_{line}"
        if sp_id == 55:
            return f"european_handicap_{line}"
        if sp_id == 353:
            return f"home_total_points_{line}"
        if sp_id == 352:
            return f"away_total_points_{line}"
        if sp_id == 362:
            return f"q1_total_{line}"
        if sp_id == 363:
            return f"q2_total_{line}"
        if sp_id == 364:
            return f"q3_total_{line}"
        if sp_id == 365:
            return f"q4_total_{line}"
        if sp_id == 366:
            return f"q1_spread_{line}"
        if sp_id == 367:
            return f"q2_spread_{line}"
        if sp_id == 368:
            return f"q3_spread_{line}"
        if sp_id == 369:
            return f"q4_spread_{line}"
        return None


class SportpesaTennisMapper(BaseMapper):
    STATIC_MARKETS = {
        382: "match_winner", 204: "first_set_winner", 231: "second_set_winner",
        233: "set_betting", 433: "first_set_match_winner", 45: "odd_even"
    }

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        if sp_id in [51, 439, 339]:
            return f"asian_handicap_{line}"
        if sp_id in [226, 340]:
            return f"over_under_goals_{line}"
        if sp_id == 353:
            return f"player1_games_{line}"
        if sp_id == 352:
            return f"player2_games_{line}"
        return None


class SportpesaUSSportsMapper(BaseMapper):
    STATIC_MARKETS = {382: "match_winner", 381: "1x2", 1: "1x2", 10: "1x2", 45: "odd_even"}

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        if sp_id in [51, 228]:
            return f"asian_handicap_{line}"
        if sp_id in [52, 229]:
            return f"over_under_goals_{line}"
        if sp_id == 353:
            return f"home_runs_{line}"
        if sp_id == 352:
            return f"away_runs_{line}"
        return None


class SportpesaGenericMapper(BaseMapper):
    STATIC_MARKETS = {
        10: "1x2", 1: "1x2", 381: "1x2", 382: "match_winner", 20: "match_winner",
        42: "first_half_1x2", 46: "double_chance", 47: "draw_no_bet", 44: "ht_ft",
        45: "odd_even", 43: "btts", 378: "match_winner_ot", 204: "first_set_winner",
        233: "set_betting", 210: "first_period_winner", 379: "winning_margin",
        207: "highest_scoring_half", 227: "highest_scoring_period"
    }

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        if sp_id in [51, 53, 439, 339]:
            return f"asian_handicap_{line}"
        if sp_id in [52, 60, 226, 54, 377, 212, 229, 340, 56]:
            return f"over_under_goals_{line}"
        if sp_id == 55:
            prefix = "plus" if spec_value > 0 else "minus"
            return f"european_handicap_{prefix}_{abs(int(spec_value))}"
        if sp_id == 353:
            return f"home_points_{line}"
        if sp_id == 352:
            return f"away_points_{line}"
        if sp_id == 208:
            return f"result_and_over_under_{line}"
        return None


class SportpesaCombatMapper(BaseMapper):
    STATIC_MARKETS = {382: "match_winner", 20: "match_winner", 10: "1x2"}

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        if sp_id == 51:
            return f"round_betting_{line}"
        if sp_id == 52:
            return f"total_rounds_{line}"
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 2. MASTER DISPATCHER (FIXED: eFootball routed correctly)
# ══════════════════════════════════════════════════════════════════════════════

class MasterSportpesaMapper:
    SPORT_ID_MAP = {
        1: "Football", 2: "Basketball", 3: "Baseball", 4: "Ice Hockey",
        5: "Tennis", 6: "Handball", 10: "Boxing", 12: "Rugby",
        15: "American Football", 16: "Table Tennis", 21: "Cricket",
        23: "Volleyball", 49: "Darts", 117: "MMA", 126: "eFootball"
    }

    @classmethod
    def get_slug(cls, sport_id: int, market_id: int, spec_value: float) -> str | None:
        spec_val = spec_value if spec_value is not None else 0
        sport_name = cls.SPORT_ID_MAP.get(sport_id)

        if sport_name == "eFootball":
            slug = SportpesaEFootballMapper.get_market_slug(market_id, spec_val)
        elif sport_name == "Football":
            slug = SportpesaFootballMapper.get_market_slug(market_id, spec_val)
        elif sport_name == "Basketball":
            slug = SportpesaBasketballMapper.get_market_slug(market_id, spec_val)
        elif sport_name in ["Baseball", "American Football"]:
            slug = SportpesaUSSportsMapper.get_market_slug(market_id, spec_val)
        elif sport_name in ["Tennis", "Table Tennis"]:
            slug = SportpesaTennisMapper.get_market_slug(market_id, spec_val)
        elif sport_name in ["Boxing", "MMA"]:
            slug = SportpesaCombatMapper.get_market_slug(market_id, spec_val)
        else:
            slug = SportpesaGenericMapper.get_market_slug(market_id, spec_val)

        return slug if slug else f"market_{market_id}"


# ══════════════════════════════════════════════════════════════════════════════
# 3. PUBLIC API UTILITIES (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def normalize_sp_market(mkt_id: int, spec_val: Any = None, sport_id: int = 1) -> str:
    try:
        val = float(spec_val) if spec_val is not None else 0
    except (ValueError, TypeError):
        val = 0
    return MasterSportpesaMapper.get_slug(sport_id, mkt_id, val)


MARKET_DISPLAY_NAMES: dict[str, str] = {
    "match_winner": "Match Winner", "1x2": "1X2", "asian_handicap": "Asian Handicap",
    "over_under_goals": "Goals O/U", "double_chance": "Double Chance", "btts": "BTTS",
    "european_handicap": "European Handicap", "odd_even": "Odd/Even", "correct_score": "Correct Score",
    "ht_ft": "HT/FT", "winning_margin": "Winning Margin", "first_half_1x2": "1st Half 1X2",
}

def get_market_display_name(slug: str) -> str:
    if slug in MARKET_DISPLAY_NAMES:
        return MARKET_DISPLAY_NAMES[slug]
    return slug.replace("_", " ").title()

def get_outcome_display(slug: str, outcome_key: str) -> str:
    if "over_under" in slug or "total" in slug:
        if outcome_key == "over" or outcome_key.upper().startswith("OV"):
            return "Over"
        if outcome_key == "under" or outcome_key.upper().startswith("UN"):
            return "Under"
    if "asian_handicap" in slug or "spread" in slug:
        if outcome_key in ("1", "home"):
            return "Home"
        if outcome_key in ("2", "away"):
            return "Away"
    return outcome_key.upper()

def extract_base_and_line(slug: str) -> tuple[str, str]:
    m = re.match(r"^(.+?)_(-?\d+(?:\.\d+)?)$", slug)
    if m:
        return m.group(1), m.group(2)
    return slug, ""

_OU_SLUGS: frozenset[str] = frozenset({
    "over_under_goals", "total_points", "total_runs", "total_games", "total_legs",
    "total_sets", "total_rounds", "total_corners", "total_bookings", "home_goals", "away_goals",
    "home_points", "away_points", "home_total_points", "away_total_points",
    "home_runs", "away_runs", "home_runs_total", "away_runs_total", "player1_games", "player2_games",
    "first_half_over_under", "first_half_total", "q1_total", "q2_total",
    "q3_total", "q4_total", "first_set_total_games", "over_under_goals_ot", "first_period_total"
})

_HC_SLUGS: frozenset[str] = frozenset({
    "asian_handicap", "first_half_asian_handicap", "point_spread", "puck_line", "run_line",
    "set_handicap", "game_handicap", "first_set_game_handicap", "leg_handicap", "run_handicap",
    "q1_spread", "q2_spread", "q3_spread", "q4_spread", "first_half_spread",
})

_EUR_HC_SLUGS: frozenset[str] = frozenset({"european_handicap"})

OUTCOME_DISPLAY: dict[str, dict[str, str]] = {
    "1x2": {"1": "Home", "X": "Draw", "2": "Away"},
    "match_winner": {"1": "Home", "2": "Away"},
    "double_chance": {"1X": "Home or Draw", "X2": "Draw or Away", "12": "Home or Away"},
    "draw_no_bet": {"1": "Home", "2": "Away"},
    "btts": {"yes": "Yes (GG)", "no": "No (NG)"},
    "odd_even": {"odd": "Odd", "even": "Even"},
    "first_half_1x2": {"1": "Home", "X": "Draw", "2": "Away"},
    "ht_ft": {"1/1": "Home/Home", "1/X": "Home/Draw", "1/2": "Home/Away", "X/1": "Draw/Home", "X/X": "Draw/Draw", "X/2": "Draw/Away", "2/1": "Away/Home", "2/X": "Away/Draw", "2/2": "Away/Away"},
}

SPORT_PRIMARY_MARKETS: dict[int, list[str]] = {
    1:   ["1x2", "over_under_goals", "btts", "double_chance", "asian_handicap", "first_half_1x2", "correct_score", "ht_ft"],
    126: ["efootball_1x2", "over_under_efootball_goals", "efootball_double_chance", "efootball_ah"],
    2:   ["match_winner", "asian_handicap", "over_under_goals", "first_half_winner", "highest_scoring_quarter"],
    5:   ["match_winner", "first_set_winner", "over_under_goals", "set_betting", "asian_handicap"],
    4:   ["1x2", "match_winner_ot", "over_under_goals", "asian_handicap", "btts"],
    12:  ["1x2", "match_winner", "asian_handicap", "over_under_goals", "highest_scoring_half", "ht_ft"],
    23:  ["match_winner", "asian_handicap", "over_under_goals", "set_betting"],
    21:  ["match_winner", "1x2", "over_under_goals", "asian_handicap"],
    6:   ["1x2", "match_winner", "over_under_goals", "asian_handicap", "btts", "double_chance"],
    16:  ["match_winner", "asian_handicap", "over_under_goals"],
    117: ["match_winner", "total_rounds", "round_betting"],
    10:  ["match_winner", "total_rounds", "round_betting"],
    49:  ["match_winner", "over_under_goals", "odd_even", "asian_handicap"],
    15:  ["match_winner", "asian_handicap", "over_under_goals", "odd_even"],
    3:   ["match_winner", "asian_handicap", "over_under_goals"],
}

SPORT_META: dict[int, dict] = {
    1:   {"name": "Football",       "emoji": "⚽", "slugs": ["soccer", "football"]},
    126: {"name": "eFootball",      "emoji": "🎮", "slugs": ["esoccer", "efootball"]},
    2:   {"name": "Basketball",     "emoji": "🏀", "slugs": ["basketball"]},
    5:   {"name": "Tennis",         "emoji": "🎾", "slugs": ["tennis"]},
    4:   {"name": "Ice Hockey",     "emoji": "🏒", "slugs": ["ice-hockey"]},
    12:  {"name": "Rugby Union",    "emoji": "🏉", "slugs": ["rugby"]},
    23:  {"name": "Volleyball",     "emoji": "🏐", "slugs": ["volleyball"]},
    21:  {"name": "Cricket",        "emoji": "🏏", "slugs": ["cricket"]},
    6:   {"name": "Handball",       "emoji": "🤾", "slugs": ["handball"]},
    16:  {"name": "Table Tennis",   "emoji": "🏓", "slugs": ["table-tennis"]},
    117: {"name": "MMA",            "emoji": "🥋", "slugs": ["mma", "ufc"]},
    10:  {"name": "Boxing",         "emoji": "🥊", "slugs": ["boxing"]},
    49:  {"name": "Darts",          "emoji": "🎯", "slugs": ["darts"]},
    15:  {"name": "Am. Football",   "emoji": "🏈", "slugs": ["american-football"]},
    3:   {"name": "Baseball",       "emoji": "⚾", "slugs": ["baseball"]},
}

def list_all_slugs(sport_id: int = 1) -> list[str]:
    return get_sport_primary_markets(sport_id)

def get_sport_primary_markets(sport_id: int) -> list[str]:
    return SPORT_PRIMARY_MARKETS.get(sport_id, ["match_winner"])

def get_sport_meta(sport_id: int) -> dict:
    return SPORT_META.get(sport_id, {"name": f"Sport {sport_id}", "emoji": "🏆", "slugs": []})

# ══════════════════════════════════════════════════════════════════════════════
# 4. LEGACY COMPATIBILITY API
# ══════════════════════════════════════════════════════════════════════════════

def get_sport_table(sport_id: int) -> dict[int, tuple[str, bool]]:
    """Legacy compatibility mapping to prevent import errors in other modules."""
    return {
        382: ("match_winner", False), 51: ("asian_handicap", True),
        52: ("over_under_goals", True), 45: ("odd_even", False),
    }