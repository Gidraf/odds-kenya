"""
app/workers/mappers/betika.py
==============================
Betika market normalisation (Dynamic Multi-Sport).

Uses object-oriented mapper classes to handle sport-specific sub_type_ids
and dynamic specifiers for Betika's unified API.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# SPORT ID MAPS (Betika Specific)
# ══════════════════════════════════════════════════════════════════════════════

BT_SPORT_IDS: dict[str, str] = {
    "soccer":            "14",
    "basketball":        "15",
    "tennis":            "28",
    "cricket":           "17",
    "cricket_srl":       "37",
    "rugby":             "41",
    "ice-hockey":        "29",
    "volleyball":        "35",
    "handball":          "33",
    "table-tennis":      "22",
    "baseball":          "23",
    "american-football": "24",
    "mma":               "36",
    "boxing":            "39",
    "darts":             "27",
    "esoccer":           "105",
}

BT_SPORT_SLUGS: dict[str, str] = {v: k for k, v in BT_SPORT_IDS.items()}

def slug_to_bt_sport_id(slug: str) -> str:
    return BT_SPORT_IDS.get(slug, "14")

def bt_sport_to_slug(sport_id: str | int) -> str:
    # Map back to canonical base (e.g. cricket_srl -> cricket)
    slug = BT_SPORT_SLUGS.get(str(sport_id), "soccer")
    return "cricket" if slug == "cricket_srl" else slug


# ══════════════════════════════════════════════════════════════════════════════
# BASE MAPPER
# ══════════════════════════════════════════════════════════════════════════════

class BaseBetikaMapper:
    """Base class handling common string formatting and specifier logic for Betika."""
    MARKET_MAP: dict[str, str] = {}
    PREFIX: str = "unknown"

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '-1.5' -> 'minus_1_5')."""
        if not raw_line:
            return ""

        # Handle complex variants (e.g., 'sr:winning_margin_no_draw:11+', 'sr:correct_score:max:7')
        if ":" in str(raw_line):
            if any(key in str(raw_line) for key in ["exact", "winning_margin", "max", "point_range"]):
                raw_line = str(raw_line).split(":")[-1].replace("+", "")

        try:
            val = float(raw_line)
            if val == 0:
                return "0_0"
            val_str = f"{val:g}".replace(".", "_")
            return val_str.replace("-", "minus_") if val < 0 else val_str
        except ValueError:
            return str(raw_line).replace(":", "_").replace("-", "_")

    @classmethod
    def get_market_slug(cls, sub_type_id: str, parsed_specifiers: dict, fallback_name: str = "") -> str:
        base_slug = cls.MARKET_MAP.get(str(sub_type_id))

        # Fallback if ID is unknown
        if not base_slug:
            clean_name = fallback_name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
            base_slug = f"{cls.PREFIX}_{clean_name}" if cls.PREFIX else clean_name

        if not parsed_specifiers:
            return base_slug

        # Extract the line value (Total, Handicap, Variant, etc.)
        raw_line = (
            parsed_specifiers.get("total") or 
            parsed_specifiers.get("hcp") or 
            parsed_specifiers.get("variant") or 
            parsed_specifiers.get("goalnr") or
            parsed_specifiers.get("pointnr") or
            parsed_specifiers.get("points") or
            parsed_specifiers.get("freethrownr") or
            parsed_specifiers.get("inningnr") or
            parsed_specifiers.get("cornernr")
        )

        # Handle Volleyball/Tennis Set Modifiers
        if "setnr" in parsed_specifiers and not raw_line:
            if "set" not in base_slug:
                 return f"set_{parsed_specifiers['setnr']}_{base_slug}"

        # Handle Basketball Quarters
        if "quarternr" in parsed_specifiers and not raw_line:
            if "quarter" not in base_slug:
                raw_line = parsed_specifiers["quarternr"]

        line_str = cls.format_line(str(raw_line)) if raw_line else ""
        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """Cleans Betika's verbose display strings into standard outcome keys."""
        d = display.upper().strip()

        # Strip out Line annotations in parenthesis e.g., "1 (-1.5)" -> "1"
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # Recursively handle Combo Markets e.g., "1/X & OVER 1.5"
        if "&" in d:
            left = BaseBetikaMapper.normalize_outcome(d.split("&")[0])
            right = BaseBetikaMapper.normalize_outcome(d.split("&")[1])
            return f"{left}_{right}"

        # Standard outcome mappings
        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
            "NONE": "none", "NO GOAL": "none",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
            "EVEN": "even", "ODD": "odd",
            "1ST QUARTER": "1st_quarter",
            "2ND QUARTER": "2nd_quarter",
            "3RD QUARTER": "3rd_quarter",
            "4TH QUARTER": "4th_quarter",
            "1ST PERIOD": "1st_period",
            "2ND PERIOD": "2nd_period",
            "3RD PERIOD": "3rd_period",
            "EQUAL": "equal",
        }
        if d in mapping: 
            return mapping[d]

        if d.startswith("OVER"): return "over"
        if d.startswith("UNDER"): return "under"
        
        # Handle Specific Set Betting / Scores (e.g. "2:0", "1:2")
        if ":" in d and len(d) <= 5: return d.strip()
        
        # Handle HT/FT slashes
        if "/" in d and len(d) == 3: return d 

        # Fallback (e.g., "1 BY 1-5")
        return d.lower().replace(" ", "_").replace("-", "_")


# ══════════════════════════════════════════════════════════════════════════════
# SPORT-SPECIFIC MAPPERS
# ══════════════════════════════════════════════════════════════════════════════

class BetikaSoccerMapper(BaseBetikaMapper):
    PREFIX = "soccer"
    MARKET_MAP = {
        "1":   "1x2",
        "8":   "first_team_to_score",
        "10":  "double_chance",
        "11":  "draw_no_bet",
        "14":  "european_handicap",
        "15":  "winning_margin",
        "16":  "asian_handicap",
        "18":  "over_under_goals",
        "19":  "home_over_under",
        "20":  "away_over_under",
        "21":  "exact_goals",
        "29":  "btts",
        "35":  "1x2_btts",
        "36":  "over_under_btts",
        "37":  "1x2_over_under",
        "38":  "first_goalscorer",
        "39":  "last_goalscorer",
        "40":  "anytime_goalscorer",
        "45":  "correct_score",
        "47":  "ht_ft",
        "60":  "first_half_1x2",
        "63":  "first_half_double_chance",
        "65":  "first_half_european_handicap",
        "66":  "first_half_asian_handicap",
        "68":  "first_half_over_under_goals",
        "69":  "first_half_home_over_under",
        "70":  "first_half_away_over_under",
        "71":  "first_half_exact_goals",
        "75":  "first_half_btts",
        "136": "booking_1x2",
        "137": "first_booking",
        "139": "total_bookings",
        "146": "red_card",
        "149": "first_half_booking_1x2",
        "162": "corner_1x2",
        "163": "first_corner",
        "166": "total_corners",
        "169": "corner_range",
        "173": "first_half_corner_1x2",
        "177": "first_half_total_corners",
        "542": "first_half_double_chance_btts",
        "543": "second_half_1x2_btts",
        "544": "second_half_1x2_over_under",
        "546": "double_chance_btts",
        "547": "double_chance_over_under",
        "548": "multigoals",
        "818": "ht_ft_over_under",
    }


class BetikaEFootballMapper(BaseBetikaMapper):
    PREFIX = "efootball"
    MARKET_MAP = {
        "1":   "efootball_1x2",
        "8":   "efootball_first_team_to_score",
        "10":  "efootball_double_chance",
        "11":  "efootball_draw_no_bet",
        "14":  "efootball_european_handicap",
        "16":  "efootball_asian_handicap",
        "18":  "over_under_efootball_goals",
        "21":  "efootball_exact_goals",
        "29":  "efootball_btts",
        "35":  "efootball_1x2_btts",
        "36":  "efootball_over_under_btts",
        "45":  "efootball_correct_score",
        "47":  "efootball_ht_ft",
        "60":  "first_half_efootball_1x2",
        "68":  "first_half_over_under_efootball_goals",
        "75":  "first_half_efootball_btts",
        "105": "efootball_10_minutes_1x2",
        "548": "efootball_multigoals",
    }


class BetikaBaseballMapper(BaseBetikaMapper):
    PREFIX = "baseball"
    MARKET_MAP = {
        "1":   "baseball_1x2",
        "251": "baseball_moneyline",
        "256": "baseball_spread",
        "258": "over_under_baseball_runs",
        "260": "baseball_home_team_total",
        "261": "baseball_away_team_total",
        "264": "baseball_odd_even",
        "274": "baseball_f5_1x2",
        "275": "baseball_f5_spread",
        "276": "over_under_baseball_f5_runs",
        "287": "baseball_1st_inning_1x2",
        "288": "over_under_baseball_1st_inning_runs"
    }


class BetikaBasketballMapper(BaseBetikaMapper):
    PREFIX = "basketball"
    MARKET_MAP = {
        "1":   "basketball_1x2",
        "10":  "basketball_double_chance",
        "11":  "basketball_draw_no_bet",
        "18":  "over_under_basketball_points",
        "47":  "basketball_ht_ft",
        "60":  "first_half_basketball_1x2",
        "66":  "first_half_basketball_spread",
        "68":  "first_half_over_under_basketball_points",
        "69":  "first_half_basketball_home_team_total",
        "70":  "first_half_basketball_away_team_total",
        "219": "basketball_moneyline",
        "225": "over_under_basketball_points_incl_ot",
        "227": "basketball_home_team_total_incl_ot",
        "228": "basketball_away_team_total_incl_ot",
        "230": "basketball_race_to_points",
        "234": "basketball_highest_scoring_quarter",
        "236": "first_quarter_over_under_basketball_points",
        "290": "basketball_winning_margin_incl_ot",
        "292": "basketball_moneyline_and_total",
        "301": "first_quarter_winning_margin",
        "548": "basketball_multigoals",
        "756": "first_quarter_basketball_home_team_total",
        "757": "first_quarter_basketball_away_team_total",
        "960": "first_quarter_last_point",
        "961": "basketball_first_free_throw_scored",
        "962": "basketball_home_max_consecutive_points",
        "963": "basketball_away_max_consecutive_points",
        "964": "basketball_any_team_max_consecutive_points",
        "965": "basketball_home_to_lead_by_points",
        "966": "basketball_away_to_lead_by_points",
        "967": "basketball_any_team_to_lead_by_points",
    }


class BetikaBoxingMapper(BaseBetikaMapper):
    PREFIX = "boxing"
    MARKET_MAP = {
        "186": "boxing_moneyline",
        "1":   "boxing_1x2",
    }


class BetikaMMAMapper(BaseBetikaMapper):
    PREFIX = "mma"
    MARKET_MAP = {
        "186": "mma_winner",
        "1":   "mma_1x2",
        "18":  "over_under_mma_rounds",
    }


class BetikaCricketMapper(BaseBetikaMapper):
    PREFIX = "cricket"
    MARKET_MAP = {
        "340": "cricket_winner_incl_super_over",
        "342": "cricket_will_there_be_a_tie",
        "875": "cricket_home_total_at_1st_dismissal",
        "876": "cricket_away_total_at_1st_dismissal",
    }


class BetikaHandballMapper(BaseBetikaMapper):
    PREFIX = "handball"
    MARKET_MAP = {
        "1":   "handball_1x2",
        "10":  "handball_double_chance",
        "11":  "handball_draw_no_bet",
        "15":  "handball_winning_margin",
        "16":  "handball_spread",
        "18":  "over_under_handball_goals",
        "19":  "handball_home_team_total",
        "20":  "handball_away_team_total",
        "37":  "handball_1x2_over_under",
        "47":  "handball_ht_ft",
        "60":  "first_half_handball_1x2",
        "63":  "first_half_handball_double_chance",
        "66":  "first_half_handball_spread",
        "68":  "first_half_over_under_handball_goals",
    }


class BetikaIceHockeyMapper(BaseBetikaMapper):
    PREFIX = "hockey"
    MARKET_MAP = {
        "1":   "hockey_1x2",
        "8":   "hockey_first_goal",
        "10":  "hockey_double_chance",
        "11":  "hockey_draw_no_bet",
        "14":  "hockey_european_handicap",
        "15":  "hockey_winning_margin",
        "16":  "hockey_asian_handicap",
        "18":  "over_under_hockey_goals",
        "19":  "hockey_home_team_total",
        "20":  "hockey_away_team_total",
        "29":  "hockey_btts",
        "37":  "hockey_1x2_over_under",
        "47":  "hockey_ht_ft",
        "60":  "hockey_p1_1x2",
        "68":  "hockey_p1_over_under_goals",
        "199": "hockey_correct_score",
        "406": "hockey_moneyline",
        "432": "hockey_highest_scoring_period",
        "447": "hockey_p1_home_team_total",
        "448": "hockey_p1_away_team_total",
    }


class BetikaRugbyMapper(BaseBetikaMapper):
    PREFIX = "rugby"
    MARKET_MAP = {
        "1":   "rugby_1x2",
        "10":  "rugby_double_chance",
        "15":  "rugby_winning_margin",
        "16":  "rugby_spread",
        "18":  "over_under_rugby_pts",
        "47":  "rugby_ht_ft",
        "60":  "rugby_first_half_1x2",
        "66":  "rugby_first_half_spread",
        "264": "rugby_odd_even_pts",
        "432": "rugby_highest_scoring_half",
    }


class BetikaTennisMapper(BaseBetikaMapper):
    PREFIX = "tennis"
    MARKET_MAP = {
        "186": "tennis_match_winner",
        "187": "tennis_game_handicap",
        "188": "tennis_set_handicap",
        "189": "over_under_tennis_games",
        "190": "p1_over_under_games",
        "191": "p2_over_under_games",
        "192": "tennis_s1_game_handicap",
        "193": "over_under_s1_games",
        "204": "first_set_winner",
        "231": "second_set_winner",
        "264": "tennis_odd_even_games",
        "265": "tennis_odd_even_s1_games",
        "433": "tennis_s1_and_match_winner",
    }


class BetikaVolleyballMapper(BaseBetikaMapper):
    PREFIX = "volleyball"
    MARKET_MAP = {
        "186": "volleyball_match_winner",
        "188": "volleyball_set_handicap",
        "189": "over_under_volleyball_total_points",
        "202": "first_set_winner",
        "237": "volleyball_point_handicap",
        "309": "first_set_point_handicap",
        "310": "first_set_total_points",
    }


# =============================================================================
# ROUTING FUNCTIONS
# =============================================================================

_MAPPERS = {
    "soccer": BetikaSoccerMapper,
    "esoccer": BetikaEFootballMapper,
    "baseball": BetikaBaseballMapper,
    "basketball": BetikaBasketballMapper,
    "boxing": BetikaBoxingMapper,
    "mma": BetikaMMAMapper,
    "cricket": BetikaCricketMapper,
    "handball": BetikaHandballMapper,
    "ice-hockey": BetikaIceHockeyMapper,
    "rugby": BetikaRugbyMapper,
    "tennis": BetikaTennisMapper,
    "volleyball": BetikaVolleyballMapper,
}

def get_market_slug(sport_slug: str, sub_type_id: str | int, parsed_specifiers: dict, fallback_name: str = "") -> str:
    """
    Routes the payload to the correct sport class to generate a canonical slug.
    """
    mapper = _MAPPERS.get(sport_slug.lower(), BetikaSoccerMapper)
    return mapper.get_market_slug(str(sub_type_id), parsed_specifiers, fallback_name)

def normalize_outcome(sport_slug: str, display: str) -> str:
    """
    Routes the payload to the correct sport class to clean the outcome string.
    """
    mapper = _MAPPERS.get(sport_slug.lower(), BetikaSoccerMapper)
    return mapper.normalize_outcome(display)