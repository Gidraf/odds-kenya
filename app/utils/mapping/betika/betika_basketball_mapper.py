import logging

logger = logging.getLogger(__name__)

class BetikaBasketballMapper:
    """Maps Betika Basketball JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":   "basketball_1x2",                           # 1X2 (Regular Time)
        "10":  "basketball_double_chance",                 # Double Chance
        "11":  "basketball_draw_no_bet",                   # Who Will Win? (If Draw, Money Back)
        "47":  "basketball_ht_ft",                         # Halftime/Fulltime
        "60":  "first_half_basketball_1x2",                # 1st Half - 1X2
        "219": "basketball_moneyline",                     # Winner (Incl. Overtime)
        "234": "basketball_highest_scoring_quarter",       # Highest Scoring Quarter
        "960": "first_quarter_last_point",                 # 1st Quarter - Last Point
        "961": "basketball_first_free_throw_scored",       # 1st Free Throw Scored

        # Dynamic Markets (Require line/specifier appended)
        "18":  "over_under_basketball_points",             # Total (Regular Time)
        "66":  "first_half_basketball_spread",             # 1st Half - Asian Handicap
        "68":  "first_half_over_under_basketball_points",  # 1st Half - Total
        "69":  "first_half_basketball_home_team_total",    # 1st Half - Home Total
        "70":  "first_half_basketball_away_team_total",    # 1st Half - Away Total
        "225": "over_under_basketball_points_incl_ot",     # Total (Incl. Overtime)
        "227": "basketball_home_team_total_incl_ot",       # Home Total (Incl. Overtime)
        "228": "basketball_away_team_total_incl_ot",       # Away Total (Incl. Overtime)
        "230": "basketball_race_to_points",                # Race To X Points (Incl. Overtime)
        "236": "first_quarter_over_under_basketball_points",# 1st Quarter - Total
        "290": "basketball_winning_margin_incl_ot",        # Winning Margin (Incl. Overtime)
        "292": "basketball_moneyline_and_total",           # Winner & Total (Incl. Overtime)
        "301": "first_quarter_winning_margin",             # 1st Quarter - Winning Margin
        "548": "basketball_multigoals",                    # Multigoals
        "756": "first_quarter_basketball_home_team_total", # 1st Quarter - Home Total
        "757": "first_quarter_basketball_away_team_total", # 1st Quarter - Away Total
        "962": "basketball_home_max_consecutive_points",   # Home Total Maximum Consecutive Points
        "963": "basketball_away_max_consecutive_points",   # Away Total Maximum Consecutive Points
        "964": "basketball_any_team_max_consecutive_points",# Any Team Total Maximum Consecutive Points
        "965": "basketball_home_to_lead_by_points",        # Home To Lead By X
        "966": "basketball_away_to_lead_by_points",        # Away To Lead By X
        "967": "basketball_any_team_to_lead_by_points",    # Any Team To Lead By X
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '-1.5' -> 'minus_1_5')."""
        if not raw_line:
            return ""

        # Handle complex variants like 'sr:winning_margin_no_draw:11+' -> '11'
        if ":" in raw_line:
            raw_line = raw_line.split(":")[-1].replace("+", "")

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
            base_slug = f"basketball_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Extract the line value (Basketball has a lot of unique specifier keys)
        raw_line = (
            parsed_specifiers.get("total") or 
            parsed_specifiers.get("hcp") or 
            parsed_specifiers.get("pointnr") or 
            parsed_specifiers.get("points") or 
            parsed_specifiers.get("freethrownr") or
            parsed_specifiers.get("variant")
        )

        # Prevent appending quarter numbers if the base slug already identifies the quarter
        if "quarternr" in parsed_specifiers and not raw_line:
            if "quarter" not in base_slug:
                raw_line = parsed_specifiers["quarternr"]

        line_str = cls.format_line(str(raw_line)) if raw_line else ""

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Cleans Betika's verbose display strings into standard outcome keys.
        Examples: 
          '2 (+1.5)' -> '2'
          '2 & UNDER 247.5' -> '2_under'
          '1 BY 1-5' -> '1_by_1_5'
        """
        d = display.upper().strip()

        # 1. Strip out Handicap/Line annotations in parenthesis e.g., "1 (-1.5)" -> "1"
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # 2. Recursively handle Combo Markets (e.g., "2 & UNDER 247.5")
        if "&" in d:
            left = BetikaBasketballMapper.normalize_outcome(d.split("&")[0])
            right = BetikaBasketballMapper.normalize_outcome(d.split("&")[1])
            return f"{left}_{right}"

        # 3. Standard outcome mappings
        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
            "1ST QUARTER": "1st_quarter",
            "2ND QUARTER": "2nd_quarter",
            "3RD QUARTER": "3rd_quarter",
            "4TH QUARTER": "4th_quarter",
            "EQUAL": "equal",
        }
        if d in mapping:
            return mapping[d]

        # 4. Handle Over/Under
        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        # 5. Handle HT/FT slashes (e.g., "1/1", "X/2")
        if "/" in d and len(d) == 3:
            return d 

        # 6. Fallback (e.g., "1 BY 1-5" or "1-2")
        return d.lower().replace(" ", "_").replace("-", "_")