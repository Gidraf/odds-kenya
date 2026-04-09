# """
# app/workers/sp_mapper.py
# =========================
# Sportpesa market-ID → canonical slug mapping for EVERY sport using class-based routing.

# Public API
# ──────────
#   normalize_sp_market(mkt_id, spec_val, sport_id)   → canonical slug
#   list_all_slugs(sport_id)                          → sorted list of base slugs
#   extract_base_and_line(slug)                       → (base_slug, line_str)
#   get_market_display_name(slug)                     → "Goals O/U 2.5"
#   get_outcome_display(slug, outcome_key)            → "Over 2.5" / "Home +1.5"

# Changes in v2
# ──────────────
#   • Implemented MasterSportpesaMapper to route market IDs by sport to prevent collisions.
#   • extract_base_and_line() handles:
#       - integer lines: "european_handicap_2" → ("european_handicap", "2")
#       - negative lines: "asian_handicap_-1.25" → ("asian_handicap", "-1.25")
#       - decimal lines: "over_under_goals_2.5" → ("over_under_goals", "2.5")
# """

# from __future__ import annotations
# import re
# from typing import Any


# # ══════════════════════════════════════════════════════════════════════════════
# # 1. SPORT-SPECIFIC MAPPERS
# # ══════════════════════════════════════════════════════════════════════════════

# class BaseMapper:
#     """Helper methods shared across all mappers."""
#     @staticmethod
#     def format_line(spec_value: float) -> str:
#         """Returns exact line string (e.g., '2.5', '-1.25', '2') to match V2 regex."""
#         if spec_value == 0:
#             return "0"
#         return f"{spec_value:g}"


# class SportpesaFootballMapper(BaseMapper):
#     STATIC_MARKETS = {
#         10:  "1x2", 1: "1x2", 381: "1x2",
#         46:  "double_chance",
#         43:  "btts", 29: "btts",
#         47:  "draw_no_bet",
#         42:  "first_half_1x2", 60: "first_half_1x2",
#         328: "first_half_btts",
#         203: "first_half_correct_score",
#         207: "highest_scoring_half",
#         44:  "ht_ft",
#         332: "correct_score",
#         258: "exact_goals",
#         45:  "odd_even",
#         202: "goal_groups",
#         41:  "first_team_to_score",
#         386: "btts_and_result",
#         162: "total_corners",
#         136: "total_bookings",
#     }

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS:
#             return cls.STATIC_MARKETS[sp_id]

#         line = cls.format_line(spec_value)
#         if sp_id in [52, 18, 56]: return f"over_under_goals_{line}"
#         if sp_id in [54, 15, 68]: return f"first_half_over_under_{line}"
#         if sp_id == 353: return f"home_goals_{line}"
#         if sp_id == 352: return f"away_goals_{line}"
#         if sp_id == 51:  return f"asian_handicap_{line}"
#         if sp_id == 53:  return f"first_half_asian_handicap_{line}"
#         if sp_id == 55:  return f"european_handicap_{line}"
#         if sp_id == 208: return f"result_and_over_under_{line}"
#         if sp_id == 166: return f"total_corners_{line}"
#         if sp_id == 139: return f"total_bookings_{line}"
#         return None


# class SportpesaEFootballMapper(BaseMapper):
#     STATIC_MARKETS = {381: "1x2", 1: "1x2", 10: "1x2", 46: "double_chance", 47: "draw_no_bet", 43: "btts", 45: "odd_even", 258: "exact_goals", 202: "goal_groups"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id in [56, 52]: return f"over_under_goals_{line}"
#         if sp_id == 51: return f"asian_handicap_{line}"
#         if sp_id == 208: return f"result_and_over_under_{line}"
#         return None


# class SportpesaBasketballMapper(BaseMapper):
#     STATIC_MARKETS = {382: "match_winner", 42: "first_half_winner", 45: "odd_even", 224: "highest_scoring_quarter", 222: "winning_margin"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id == 51: return f"point_spread_{line}"
#         if sp_id == 52: return f"total_points_{line}"
#         if sp_id == 53: return f"first_half_spread_{line}"
#         if sp_id == 54: return f"first_half_total_{line}"
#         if sp_id == 353: return f"home_total_points_{line}"
#         if sp_id == 352: return f"away_total_points_{line}"
#         if sp_id == 362: return f"q1_total_{line}"
#         if sp_id == 363: return f"q2_total_{line}"
#         if sp_id == 364: return f"q3_total_{line}"
#         if sp_id == 365: return f"q4_total_{line}"
#         if sp_id == 366: return f"q1_spread_{line}"
#         if sp_id == 367: return f"q2_spread_{line}"
#         if sp_id == 368: return f"q3_spread_{line}"
#         if sp_id == 369: return f"q4_spread_{line}"
#         return None


# class SportpesaTennisMapper(BaseMapper):
#     STATIC_MARKETS = {382: "match_winner", 204: "first_set_winner", 231: "second_set_winner", 233: "set_betting", 433: "first_set_match_winner", 45: "odd_even_games"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id == 51: return f"game_handicap_{line}"
#         if sp_id == 439: return f"set_handicap_{line}"
#         if sp_id == 339: return f"first_set_game_handicap_{line}"
#         if sp_id == 226: return f"total_games_{line}"
#         if sp_id == 340: return f"first_set_total_games_{line}"
#         if sp_id == 353: return f"player1_games_{line}"
#         if sp_id == 352: return f"player2_games_{line}"
#         return None


# class SportpesaIceHockeyMapper(BaseMapper):
#     STATIC_MARKETS = {10: "1x2", 1: "1x2", 382: "match_winner", 378: "match_winner_ot", 210: "first_period_winner", 46: "double_chance", 45: "odd_even", 43: "btts", 2: "correct_score", 227: "highest_scoring_period"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id == 51: return f"puck_line_{line}"
#         if sp_id in [52, 60]: return f"over_under_goals_{line}"
#         if sp_id == 377: return f"over_under_goals_ot_{line}"
#         if sp_id == 212: return f"first_period_total_{line}"
#         if sp_id == 55:  return f"european_handicap_{line}"
#         if sp_id == 353: return f"home_goals_{line}"
#         if sp_id == 352: return f"away_goals_{line}"
#         if sp_id == 208: return f"result_and_over_under_{line}"
#         return None


# class SportpesaRugbyMapper(BaseMapper):
#     STATIC_MARKETS = {10: "1x2", 1: "1x2", 382: "match_winner", 42: "first_half_1x2", 45: "odd_even", 46: "double_chance", 44: "ht_ft", 207: "highest_scoring_half", 379: "winning_margin"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id == 51: return f"asian_handicap_{line}"
#         if sp_id == 53: return f"first_half_asian_handicap_{line}"
#         if sp_id in [60, 52]: return f"total_points_{line}"
#         if sp_id == 353: return f"home_points_{line}"
#         if sp_id == 352: return f"away_points_{line}"
#         return None


# class SportpesaHandballMapper(BaseMapper):
#     STATIC_MARKETS = {1: "1x2", 10: "1x2", 382: "match_winner", 45: "odd_even", 46: "double_chance", 47: "draw_no_bet", 43: "btts", 42: "first_half_1x2", 207: "highest_scoring_half"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id == 52: return f"over_under_goals_{line}"
#         if sp_id == 54: return f"first_half_over_under_{line}"
#         if sp_id == 51: return f"asian_handicap_{line}"
#         if sp_id == 353: return f"home_goals_{line}"
#         if sp_id == 352: return f"away_goals_{line}"
#         if sp_id == 208: return f"result_and_over_under_{line}"
#         return None


# class SportpesaVolleyballMapper(BaseMapper):
#     STATIC_MARKETS = {382: "match_winner", 20: "match_winner", 204: "first_set_winner", 233: "set_betting", 45: "odd_even"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id == 51: return f"set_handicap_{line}"
#         if sp_id == 226: return f"total_sets_{line}"
#         if sp_id == 353: return f"home_points_{line}"
#         if sp_id == 352: return f"away_points_{line}"
#         return None


# class SportpesaCricketMapper(BaseMapper):
#     STATIC_MARKETS = {382: "match_winner", 1: "1x2"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id in [52, 229]: return f"total_runs_{line}"
#         if sp_id == 51: return f"run_handicap_{line}"
#         if sp_id == 353: return f"home_runs_{line}"
#         if sp_id == 352: return f"away_runs_{line}"
#         return None


# class SportpesaBaseballMapper(BaseMapper):
#     STATIC_MARKETS = {382: "match_winner", 381: "1x2", 45: "odd_even"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id in [51, 228]: return f"run_line_{line}"
#         if sp_id in [52, 229]: return f"total_runs_{line}"
#         if sp_id == 353: return f"home_runs_total_{line}"
#         if sp_id == 352: return f"away_runs_total_{line}"
#         return None


# class SportpesaCombatMapper(BaseMapper):
#     STATIC_MARKETS = {382: "match_winner", 20: "match_winner"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id == 51: return f"round_betting_{line}"
#         if sp_id == 52: return f"total_rounds_{line}"
#         return None


# class SportpesaAmericanFootballMapper(BaseMapper):
#     STATIC_MARKETS = {382: "match_winner", 45: "odd_even"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id in [51, 228]: return f"point_spread_{line}"
#         if sp_id in [52, 229]: return f"total_points_{line}"
#         if sp_id == 353: return f"home_total_points_{line}"
#         if sp_id == 352: return f"away_total_points_{line}"
#         return None


# class SportpesaDartsMapper(BaseMapper):
#     STATIC_MARKETS = {382: "match_winner", 45: "odd_even"}

#     @classmethod
#     def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
#         if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
#         line = cls.format_line(spec_value)
#         if sp_id == 226: return f"total_legs_{line}"
#         if sp_id == 51: return f"leg_handicap_{line}"
#         return None


# # ══════════════════════════════════════════════════════════════════════════════
# # 2. THE MASTER ROUTER
# # ══════════════════════════════════════════════════════════════════════════════

# class MasterSportpesaMapper:
#     """Routes the market to the correct sport-specific mapper."""
    
#     SPORT_ID_MAP = {
#         1: "Football", 2: "Basketball", 3: "Baseball", 4: "Ice Hockey", 
#         5: "Tennis", 6: "Handball", 10: "Boxing", 12: "Rugby", 
#         15: "American Football", 16: "Table Tennis", 21: "Cricket", 
#         23: "Volleyball", 49: "Darts", 117: "MMA", 126: "eFootball"
#     }

#     @classmethod
#     def get_slug(cls, sport_id: int, market_id: int, spec_value: float) -> str | None:
#         # Default spec_value if missing
#         spec_val = spec_value if spec_value is not None else 0
#         sport_name = cls.SPORT_ID_MAP.get(sport_id)

#         slug = None
#         if sport_name == "Football":
#             slug = SportpesaFootballMapper.get_market_slug(market_id, spec_val)
#         elif sport_name == "Basketball":
#             slug = SportpesaBasketballMapper.get_market_slug(market_id, spec_val)
#         elif sport_name == "Baseball":
#             slug = SportpesaBaseballMapper.get_market_slug(market_id, spec_val)
#         elif sport_name == "Ice Hockey":
#             slug = SportpesaIceHockeyMapper.get_market_slug(market_id, spec_val)
#         elif sport_name in ["Tennis", "Table Tennis"]:
#             slug = SportpesaTennisMapper.get_market_slug(market_id, spec_val)
#         elif sport_name == "Handball":
#             slug = SportpesaHandballMapper.get_market_slug(market_id, spec_val)
#         elif sport_name in ["Boxing", "MMA"]:
#             slug = SportpesaCombatMapper.get_market_slug(market_id, spec_val)
#         elif sport_name == "Rugby":
#             slug = SportpesaRugbyMapper.get_market_slug(market_id, spec_val)
#         elif sport_name == "American Football":
#             slug = SportpesaAmericanFootballMapper.get_market_slug(market_id, spec_val)
#         elif sport_name == "Cricket":
#             slug = SportpesaCricketMapper.get_market_slug(market_id, spec_val)
#         elif sport_name == "Volleyball":
#             slug = SportpesaVolleyballMapper.get_market_slug(market_id, spec_val)
#         elif sport_name == "Darts":
#             slug = SportpesaDartsMapper.get_market_slug(market_id, spec_val)
#         elif sport_name == "eFootball":
#             slug = SportpesaEFootballMapper.get_market_slug(market_id, spec_val)

#         return slug if slug else f"market_{market_id}"


# # ══════════════════════════════════════════════════════════════════════════════
# # 3. METADATA & CONSTANTS
# # ══════════════════════════════════════════════════════════════════════════════

# MARKET_DISPLAY_NAMES: dict[str, str] = {
#     "match_winner": "Match Winner", "1x2": "1X2", "asian_handicap": "Asian Handicap",
#     "european_handicap": "European Handicap", "over_under": "Over/Under", "odd_even": "Odd/Even",
#     "correct_score": "Correct Score", "ht_ft": "HT/FT", "winning_margin": "Winning Margin",
#     "over_under_goals": "Goals O/U", "double_chance": "Double Chance", "draw_no_bet": "Draw No Bet",
#     "btts": "Both Teams to Score", "btts_and_result": "BTTS + Result", "result_and_over_under": "Result + O/U",
#     "first_team_to_score": "First Team to Score", "exact_goals": "Exact Goals", "goal_groups": "Goal Groups",
#     "highest_scoring_half": "Half with Most Goals", "first_half_1x2": "1st Half 1X2",
#     "first_half_over_under": "1st Half O/U", "first_half_btts": "1st Half BTTS",
#     "first_half_correct_score": "1st Half Correct Score", "first_half_asian_handicap": "1st Half Handicap",
#     "home_goals": "Home Goals O/U", "away_goals": "Away Goals O/U", "total_corners": "Total Corners",
#     "total_bookings": "Total Bookings", "point_spread": "Point Spread", "total_points": "Total Points",
#     "home_total_points": "Home Points O/U", "away_total_points": "Away Points O/U",
#     "first_half_winner": "1st Half Winner", "first_half_spread": "1st Half Spread",
#     "first_half_total": "1st Half Total", "highest_scoring_quarter": "Highest Scoring Quarter",
#     "q1_total": "Q1 Total", "q2_total": "Q2 Total", "q3_total": "Q3 Total", "q4_total": "Q4 Total",
#     "q1_spread": "Q1 Spread", "q2_spread": "Q2 Spread", "q3_spread": "Q3 Spread", "q4_spread": "Q4 Spread",
#     "first_set_winner": "1st Set Winner", "second_set_winner": "2nd Set Winner",
#     "game_handicap": "Game Handicap", "total_games": "Total Games", "set_betting": "Set Betting",
#     "set_handicap": "Set Handicap", "odd_even_games": "Odd/Even Games", "first_set_game_handicap": "1st Set Game HC",
#     "first_set_total_games": "1st Set Total Games", "first_set_match_winner": "1st Set / Match Winner",
#     "player1_games": "Player 1 Games O/U", "player2_games": "Player 2 Games O/U", "puck_line": "Puck Line",
#     "first_period_winner": "1st Period Winner", "match_winner_ot": "Winner (OT incl.)",
#     "home_points": "Home Points O/U", "away_points": "Away Points O/U", "total_sets": "Total Sets",
#     "total_runs": "Total Runs", "home_runs": "Home Runs O/U", "away_runs": "Away Runs O/U",
#     "run_handicap": "Run Handicap", "round_betting": "Round Betting", "total_rounds": "Total Rounds",
#     "total_legs": "Total Legs", "leg_handicap": "Leg Handicap", "run_line": "Run Line",
#     "home_runs_total": "Home Runs O/U", "away_runs_total": "Away Runs O/U",
#     "home_total": "Home Total O/U", "away_total": "Away Total O/U",
# }

# _OU_SLUGS: frozenset[str] = frozenset({
#     "over_under_goals", "total_points", "total_runs", "total_games", "total_legs",
#     "total_sets", "total_rounds", "total_corners", "total_bookings", "home_goals", "away_goals",
#     "home_points", "away_points", "home_total_points", "away_total_points",
#     "home_runs", "away_runs", "home_runs_total", "away_runs_total", "player1_games", "player2_games",
#     "first_half_over_under", "first_half_total", "q1_total", "q2_total",
#     "q3_total", "q4_total", "first_set_total_games", "over_under_goals_ot", "first_period_total"
# })

# _HC_SLUGS: frozenset[str] = frozenset({
#     "asian_handicap", "first_half_asian_handicap", "point_spread", "puck_line", "run_line",
#     "set_handicap", "game_handicap", "first_set_game_handicap", "leg_handicap", "run_handicap",
#     "q1_spread", "q2_spread", "q3_spread", "q4_spread", "first_half_spread",
# })

# _EUR_HC_SLUGS: frozenset[str] = frozenset({"european_handicap"})

# OUTCOME_DISPLAY: dict[str, dict[str, str]] = {
#     "1x2": {"1": "Home", "X": "Draw", "2": "Away"},
#     "match_winner": {"1": "Home", "2": "Away"},
#     "double_chance": {"1X": "Home or Draw", "X2": "Draw or Away", "12": "Home or Away"},
#     "draw_no_bet": {"1": "Home", "2": "Away"},
#     "btts": {"yes": "Yes (GG)", "no": "No (NG)"},
#     "odd_even": {"odd": "Odd", "even": "Even"},
#     "first_half_1x2": {"1": "Home", "X": "Draw", "2": "Away"},
#     "ht_ft": {"1/1": "Home/Home", "1/X": "Home/Draw", "1/2": "Home/Away", "X/1": "Draw/Home", "X/X": "Draw/Draw", "X/2": "Draw/Away", "2/1": "Away/Home", "2/X": "Away/Draw", "2/2": "Away/Away"},
# }

# SPORT_PRIMARY_MARKETS: dict[int, list[str]] = {
#     1:   ["1x2", "over_under_goals", "btts", "double_chance", "asian_handicap", "first_half_1x2", "correct_score", "ht_ft"],
#     126: ["1x2", "over_under_goals", "btts", "double_chance", "asian_handicap"],
#     2:   ["match_winner", "point_spread", "total_points", "first_half_winner", "highest_scoring_quarter"],
#     5:   ["match_winner", "first_set_winner", "total_games", "set_betting", "game_handicap"],
#     4:   ["1x2", "match_winner", "over_under_goals", "puck_line", "btts"],
#     12:  ["1x2", "match_winner", "asian_handicap", "total_points", "highest_scoring_half", "ht_ft"],
#     23:  ["match_winner", "set_handicap", "total_sets", "set_betting"],
#     21:  ["match_winner", "1x2", "total_runs", "run_handicap"],
#     6:   ["1x2", "match_winner", "over_under_goals", "asian_handicap", "btts", "double_chance"],
#     16:  ["match_winner", "game_handicap", "total_games"],
#     117: ["match_winner", "total_rounds", "round_betting"],
#     10:  ["match_winner", "total_rounds", "round_betting"],
#     49:  ["match_winner", "total_legs", "odd_even", "leg_handicap"],
#     15:  ["match_winner", "point_spread", "total_points", "odd_even"],
#     3:   ["match_winner", "run_line", "total_runs"],
# }

# SPORT_META: dict[int, dict] = {
#     1:   {"name": "Football",       "emoji": "⚽", "slugs": ["soccer", "football"]},
#     126: {"name": "eFootball",      "emoji": "🎮", "slugs": ["esoccer", "efootball"]},
#     2:   {"name": "Basketball",     "emoji": "🏀", "slugs": ["basketball"]},
#     5:   {"name": "Tennis",         "emoji": "🎾", "slugs": ["tennis"]},
#     4:   {"name": "Ice Hockey",     "emoji": "🏒", "slugs": ["ice-hockey"]},
#     12:  {"name": "Rugby Union",    "emoji": "🏉", "slugs": ["rugby"]},
#     23:  {"name": "Volleyball",     "emoji": "🏐", "slugs": ["volleyball"]},
#     21:  {"name": "Cricket",        "emoji": "🏏", "slugs": ["cricket"]},
#     6:   {"name": "Handball",       "emoji": "🤾", "slugs": ["handball"]},
#     16:  {"name": "Table Tennis",   "emoji": "🏓", "slugs": ["table-tennis"]},
#     117: {"name": "MMA",            "emoji": "🥋", "slugs": ["mma", "ufc"]},
#     10:  {"name": "Boxing",         "emoji": "🥊", "slugs": ["boxing"]},
#     49:  {"name": "Darts",          "emoji": "🎯", "slugs": ["darts"]},
#     15:  {"name": "Am. Football",   "emoji": "🏈", "slugs": ["american-football"]},
#     3:   {"name": "Baseball",       "emoji": "⚾", "slugs": ["baseball"]},
# }


# # ══════════════════════════════════════════════════════════════════════════════
# # 4. PUBLIC API UTILITIES
# # ══════════════════════════════════════════════════════════════════════════════

# _LINE_SUFFIX_RE = re.compile(r"^(.+?)_(-?\d+(?:\.\d+)?)$")

# def normalize_sp_market(mkt_id: int, spec_val: Any = None, sport_id: int = 1) -> str:
#     """Public method used by sp_harvester.py"""
#     try:
#         val = float(spec_val) if spec_val is not None else 0
#     except (ValueError, TypeError):
#         val = 0
#     return MasterSportpesaMapper.get_slug(sport_id, mkt_id, val)

# def extract_base_and_line(slug: str) -> tuple[str, str]:
#     m = _LINE_SUFFIX_RE.match(slug)
#     if m:
#         candidate_base, line = m.group(1), m.group(2)
#         if candidate_base in MARKET_DISPLAY_NAMES:
#             return candidate_base, line
#     return slug, ""

# def get_market_display_name(slug: str) -> str:
#     if slug in MARKET_DISPLAY_NAMES: return MARKET_DISPLAY_NAMES[slug]
#     base, line = extract_base_and_line(slug)
#     base_name = MARKET_DISPLAY_NAMES.get(base)
#     if base_name: return f"{base_name} {line}" if line else base_name
#     return slug.replace("_", " ").title()

# def get_outcome_display(slug: str, outcome_key: str) -> str:
#     base, line = extract_base_and_line(slug)

#     if line and base in _OU_SLUGS:
#         if outcome_key == "over" or outcome_key.upper().startswith("OV"): return f"Over {line}"
#         if outcome_key == "under" or outcome_key.upper().startswith("UN"): return f"Under {line}"

#     if line and base in _HC_SLUGS:
#         try:
#             f = float(line)
#             if outcome_key in ("1", "home"): return f"Home {'+' if f >= 0 else ''}{line}"
#             if outcome_key in ("2", "away"):
#                 mirror = -f
#                 mirror_str = str(int(mirror)) if mirror == int(mirror) else str(mirror)
#                 return f"Away {'+' if mirror >= 0 else ''}{mirror_str}"
#         except (ValueError, TypeError): pass

#     if line and base in _EUR_HC_SLUGS:
#         try:
#             f = float(line)
#             if outcome_key == "1": return f"1 ({'+' if f >= 0 else ''}{line})"
#             if outcome_key == "X": return f"X ({'+' if f >= 0 else ''}{line})"
#             if outcome_key == "2":
#                 mirror = -f
#                 mirror_str = str(int(mirror)) if mirror == int(mirror) else str(mirror)
#                 return f"2 ({'+' if mirror >= 0 else ''}{mirror_str})"
#         except (ValueError, TypeError): pass

#     out_map = OUTCOME_DISPLAY.get(base) or OUTCOME_DISPLAY.get(slug)
#     if out_map and outcome_key in out_map: return out_map[outcome_key]
#     return outcome_key.upper()

# def list_all_slugs(sport_id: int = 1) -> list[str]:
#     return get_sport_primary_markets(sport_id)

# def get_sport_primary_markets(sport_id: int) -> list[str]:
#     return SPORT_PRIMARY_MARKETS.get(sport_id, ["match_winner"])

# def get_sport_meta(sport_id: int) -> dict:
#     return SPORT_META.get(sport_id, {"name": f"Sport {sport_id}", "emoji": "🏆", "slugs": []})


# # ══════════════════════════════════════════════════════════════════════════════
# # 5. LEGACY COMPATIBILITY API (get_sport_table)
# # ══════════════════════════════════════════════════════════════════════════════

# _GENERIC: dict[int, tuple[str, bool]] = {
#     382: ("match_winner",  False),
#     51:  ("asian_handicap", True),
#     52:  ("over_under",    True),
#     45:  ("odd_even",      False),
#     353: ("home_total",    True),
#     352: ("away_total",    True),
#     226: ("total_games",   True),
#     233: ("set_betting",   False),
# }

# _SPORT_TABLES: dict[int, dict[int, tuple[str, bool]]] = {
#     1: { # Football
#         1: ("1x2", False), 10: ("1x2", False), 381: ("1x2", False), 46: ("double_chance", False),
#         47: ("draw_no_bet", False), 43: ("btts", False), 29: ("btts", False), 386: ("btts_and_result", False),
#         52: ("over_under_goals", True), 18: ("over_under_goals", True), 56: ("over_under_goals", True),
#         353: ("home_goals", True), 352: ("away_goals", True), 208: ("result_and_over_under", True),
#         258: ("exact_goals", False), 202: ("goal_groups", False), 332: ("correct_score", False),
#         51: ("asian_handicap", True), 53: ("first_half_asian_handicap", True), 55: ("european_handicap", True),
#         45: ("odd_even", False), 41: ("first_team_to_score", False), 207: ("highest_scoring_half", False),
#         42: ("first_half_1x2", False), 60: ("first_half_1x2", False), 15: ("first_half_over_under", True),
#         54: ("first_half_over_under", True), 68: ("first_half_over_under", True), 44: ("ht_ft", False),
#         328: ("first_half_btts", False), 203: ("first_half_correct_score", False), 162: ("total_corners", False),
#         166: ("total_corners", True), 136: ("total_bookings", False), 139: ("total_bookings", True),
#     },
#     2: { # Basketball
#         382: ("match_winner", False), 51: ("point_spread", True), 52: ("total_points", True),
#         353: ("home_total_points", True), 352: ("away_total_points", True), 45: ("odd_even", False),
#         222: ("winning_margin", False), 42: ("first_half_winner", False), 53: ("first_half_spread", True),
#         54: ("first_half_total", True), 224: ("highest_scoring_quarter", False), 362: ("q1_total", True),
#         363: ("q2_total", True), 364: ("q3_total", True), 365: ("q4_total", True), 366: ("q1_spread", True),
#         367: ("q2_spread", True), 368: ("q3_spread", True), 369: ("q4_spread", True),
#     },
#     3: { # Baseball
#         382: ("match_winner", False), 51: ("run_line", True), 52: ("total_runs", True),
#         45: ("odd_even", False), 353: ("home_runs_total", True), 352: ("away_runs_total", True),
#     },
#     4: { # Ice Hockey
#         1: ("1x2", False), 10: ("1x2", False), 382: ("match_winner", False), 52: ("over_under_goals", True),
#         51: ("puck_line", True), 45: ("odd_even", False), 46: ("double_chance", False),
#         353: ("home_goals", True), 352: ("away_goals", True), 208: ("result_and_over_under", True),
#         43: ("btts", False), 210: ("first_period_winner", False), 378: ("match_winner_ot", False),
#     },
#     5: { # Tennis
#         382: ("match_winner", False), 204: ("first_set_winner", False), 231: ("second_set_winner", False),
#         51: ("game_handicap", True), 226: ("total_games", True), 233: ("set_betting", False),
#         439: ("set_handicap", True), 45: ("odd_even_games", False), 339: ("first_set_game_handicap", True),
#         340: ("first_set_total_games", True), 433: ("first_set_match_winner", False),
#         353: ("player1_games", True), 352: ("player2_games", True),
#     },
#     6: { # Handball
#         1: ("1x2", False), 10: ("1x2", False), 382: ("match_winner", False), 52: ("over_under_goals", True),
#         51: ("asian_handicap", True), 45: ("odd_even", False), 46: ("double_chance", False),
#         47: ("draw_no_bet", False), 353: ("home_goals", True), 352: ("away_goals", True),
#         208: ("result_and_over_under", True), 43: ("btts", False), 42: ("first_half_1x2", False),
#         207: ("highest_scoring_half", False),
#     },
#     10: { # Boxing
#         382: ("match_winner", False), 51: ("round_betting", True), 52: ("total_rounds", True),
#     },
#     12: { # Rugby
#         10: ("1x2", False), 1: ("1x2", False), 382: ("match_winner", False), 46: ("double_chance", False),
#         42: ("first_half_1x2", False), 51: ("asian_handicap", True), 53: ("first_half_asian_handicap", True),
#         60: ("total_points", True), 52: ("total_points", True), 353: ("home_points", True),
#         352: ("away_points", True), 45: ("odd_even", False), 379: ("winning_margin", False),
#         207: ("highest_scoring_half", False), 44: ("ht_ft", False),
#     },
#     15: { # American Football
#         382: ("match_winner", False), 51: ("point_spread", True), 52: ("total_points", True),
#         45: ("odd_even", False), 353: ("home_total_points", True), 352: ("away_total_points", True),
#     },
#     16: { # Table Tennis
#         382: ("match_winner", False), 51: ("game_handicap", True), 226: ("total_games", True),
#         45: ("odd_even", False), 233: ("set_betting", False), 340: ("first_set_total_games", True),
#     },
#     21: { # Cricket
#         382: ("match_winner", False), 1: ("1x2", False), 51: ("run_handicap", True),
#         52: ("total_runs", True), 353: ("home_runs", True), 352: ("away_runs", True),
#     },
#     23: { # Volleyball
#         382: ("match_winner", False), 204: ("first_set_winner", False), 20: ("match_winner", False),
#         51: ("set_handicap", True), 226: ("total_sets", True), 233: ("set_betting", False),
#         45: ("odd_even", False), 353: ("home_points", True), 352: ("away_points", True),
#     },
#     49: { # Darts
#         382: ("match_winner", False), 226: ("total_legs", True), 45: ("odd_even", False),
#         51: ("leg_handicap", True),
#     },
#     117: { # MMA
#         382: ("match_winner", False), 20: ("match_winner", False), 51: ("round_betting", True),
#         52: ("total_rounds", True),
#     },
#     126: { # eFootball / eSoccer
#         381: ("1x2", False), 1: ("1x2", False), 10: ("1x2", False), 56: ("over_under_goals", True),
#         52: ("over_under_goals", True), 46: ("double_chance", False), 47: ("draw_no_bet", False),
#         43: ("btts", False), 51: ("asian_handicap", True), 45: ("odd_even", False),
#         208: ("result_and_over_under", True), 258: ("exact_goals", False), 202: ("goal_groups", False),
#     },
# }

# def get_sport_table(sport_id: int) -> dict[int, tuple[str, bool]]:
#     """Return the full market-ID lookup table for a sport (with _GENERIC fallbacks)."""
#     base   = _SPORT_TABLES.get(sport_id, {})
#     merged = dict(_GENERIC)
#     merged.update(base)
#     return merged

"""
app/workers/sp_mapper.py
=========================
Sportpesa market-ID → canonical slug mapping for EVERY sport using class-based routing.

Public API
──────────
  normalize_sp_market(mkt_id, spec_val, sport_id)   → canonical slug
  list_all_slugs(sport_id)                          → sorted list of base slugs
  extract_base_and_line(slug)                       → (base_slug, line_str)
  get_market_display_name(slug)                     → "Goals O/U 2.5"
  get_outcome_display(slug, outcome_key)            → "Over 2.5" / "Home +1.5"

Changes in v2
──────────────
  • Implemented MasterSportpesaMapper to route market IDs by sport to prevent collisions.
  • extract_base_and_line() handles:
      - integer lines: "european_handicap_2" → ("european_handicap", "2")
      - negative lines: "asian_handicap_-1.25" → ("asian_handicap", "-1.25")
      - decimal lines: "over_under_goals_2.5" → ("over_under_goals", "2.5")
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
        if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        if sp_id in [52, 18, 56]: return f"over_under_goals_{line}"
        if sp_id in [54, 15, 68]: return f"first_half_over_under_{line}"
        if sp_id == 353: return f"home_goals_{line}"
        if sp_id == 352: return f"away_goals_{line}"
        if sp_id == 51:  return f"asian_handicap_{line}"
        if sp_id == 53:  return f"first_half_asian_handicap_{line}"
        if sp_id == 55:  
            prefix = "plus" if spec_value > 0 else "minus"
            return f"european_handicap_{prefix}_{abs(int(spec_value))}"
        if sp_id == 208: return f"result_and_over_under_{line}"
        if sp_id == 166: return f"total_corners_{line}"
        if sp_id == 139: return f"total_bookings_{line}"
        return None

class SportpesaBasketballMapper(BaseMapper):
    STATIC_MARKETS = {
        382: "match_winner", 42: "first_half_winner", 45: "odd_even", 
        224: "highest_scoring_quarter", 222: "winning_margin",
        10: "1x2", 1: "1x2", 381: "1x2", 47: "draw_no_bet", 44: "ht_ft"
    }

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        
        if sp_id == 51: return f"asian_handicap_{line}"
        if sp_id == 52: return f"over_under_goals_{line}"
        if sp_id == 53: return f"first_half_asian_handicap_{line}"
        if sp_id == 54: return f"first_half_over_under_{line}"
        if sp_id == 55: return f"european_handicap_{line}"
        if sp_id == 353: return f"home_total_points_{line}"
        if sp_id == 352: return f"away_total_points_{line}"
        if sp_id == 362: return f"q1_total_{line}"
        if sp_id == 363: return f"q2_total_{line}"
        if sp_id == 364: return f"q3_total_{line}"
        if sp_id == 365: return f"q4_total_{line}"
        if sp_id == 366: return f"q1_spread_{line}"
        if sp_id == 367: return f"q2_spread_{line}"
        if sp_id == 368: return f"q3_spread_{line}"
        if sp_id == 369: return f"q4_spread_{line}"
        return None

class SportpesaTennisMapper(BaseMapper):
    STATIC_MARKETS = {
        382: "match_winner", 204: "first_set_winner", 231: "second_set_winner", 
        233: "set_betting", 433: "first_set_match_winner", 45: "odd_even"
    }

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        if sp_id in [51, 439, 339]: return f"asian_handicap_{line}"
        if sp_id in [226, 340]: return f"over_under_goals_{line}"
        if sp_id == 353: return f"player1_games_{line}"
        if sp_id == 352: return f"player2_games_{line}"
        return None

class SportpesaUSSportsMapper(BaseMapper):
    STATIC_MARKETS = {382: "match_winner", 381: "1x2", 1: "1x2", 10: "1x2", 45: "odd_even"}

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        if sp_id in [51, 228]: return f"asian_handicap_{line}"
        if sp_id in [52, 229]: return f"over_under_goals_{line}"
        if sp_id == 353: return f"home_runs_{line}"
        if sp_id == 352: return f"away_runs_{line}"
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
        if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        if sp_id in [51, 53, 439, 339]: return f"asian_handicap_{line}"
        if sp_id in [52, 60, 226, 54, 377, 212, 229, 340, 56]: return f"over_under_goals_{line}"
        if sp_id == 55:  
            prefix = "plus" if spec_value > 0 else "minus"
            return f"european_handicap_{prefix}_{abs(int(spec_value))}"
        if sp_id == 353: return f"home_points_{line}"
        if sp_id == 352: return f"away_points_{line}"
        if sp_id == 208: return f"result_and_over_under_{line}"
        return None

class SportpesaCombatMapper(BaseMapper):
    STATIC_MARKETS = {382: "match_winner", 20: "match_winner", 10: "1x2"}

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS: return cls.STATIC_MARKETS[sp_id]
        line = cls.format_line(spec_value)
        if sp_id == 51: return f"round_betting_{line}"
        if sp_id == 52: return f"total_rounds_{line}"
        return None

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

        if sport_name in ["Football", "eFootball"]:
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
# 2. PUBLIC API UTILITIES
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
    if slug in MARKET_DISPLAY_NAMES: return MARKET_DISPLAY_NAMES[slug]
    return slug.replace("_", " ").title()

def get_outcome_display(slug: str, outcome_key: str) -> str:
    if "over_under" in slug or "total" in slug:
        if outcome_key == "over" or outcome_key.upper().startswith("OV"): return "Over"
        if outcome_key == "under" or outcome_key.upper().startswith("UN"): return "Under"
    if "asian_handicap" in slug or "spread" in slug:
        if outcome_key in ("1", "home"): return "Home"
        if outcome_key in ("2", "away"): return "Away"
    return outcome_key.upper()

def extract_base_and_line(slug: str) -> tuple[str, str]:
    m = re.match(r"^(.+?)_(-?\d+(?:\.\d+)?)$", slug)
    if m: return m.group(1), m.group(2)
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
    126: ["1x2", "over_under_goals", "btts", "double_chance", "asian_handicap"],
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
# 3. LEGACY COMPATIBILITY API
# ══════════════════════════════════════════════════════════════════════════════

def get_sport_table(sport_id: int) -> dict[int, tuple[str, bool]]:
    """Legacy compatibility mapping to prevent import errors in other modules."""
    return {
        382: ("match_winner", False), 51: ("asian_handicap", True),
        52: ("over_under_goals", True), 45: ("odd_even", False),
    }