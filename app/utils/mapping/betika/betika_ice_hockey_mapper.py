import logging

logger = logging.getLogger(__name__)

class BetikaIceHockeyMapper:
    """Maps Betika Ice Hockey JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":   "hockey_1x2",                     # 1X2 (Regulation Time)
        "8":   "hockey_first_goal",              # 1st Goal
        "10":  "hockey_double_chance",           # Double Chance
        "11":  "hockey_draw_no_bet",             # Who Will Win? (If Draw, Money Back)
        "29":  "hockey_btts",                    # Both Teams To Score
        "47":  "hockey_ht_ft",                   # Halftime/Fulltime
        "60":  "hockey_p1_1x2",                  # 1st Half / 1st Period 1X2
        "199": "hockey_correct_score",           # Correct Score
        "406": "hockey_moneyline",               # Winner (Incl. Overtime and Penalties)
        "432": "hockey_highest_scoring_period",  # Highest Scoring Period

        # Dynamic Markets (Require line/specifier appended)
        "14":  "hockey_european_handicap",       # Handicap (1X2)
        "15":  "hockey_winning_margin",          # Winning Margin
        "16":  "hockey_asian_handicap",          # Asian Handicap
        "18":  "over_under_hockey_goals",        # Total Goals (Regulation)
        "19":  "hockey_home_team_total",         # Home Team Total
        "20":  "hockey_away_team_total",         # Away Team Total
        "37":  "hockey_1x2_over_under",          # 1X2 & Total
        "68":  "hockey_p1_over_under_goals",     # 1st Half / 1st Period Total
        "447": "hockey_p1_home_team_total",      # 1st Period Home Total
        "448": "hockey_p1_away_team_total",      # 1st Period Away Total
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '-1.5' -> 'minus_1_5')."""
        if not raw_line:
            return ""

        # Handle complex variants like 'sr:correct_score:max:7' -> '7'
        if ":" in raw_line and ("variant" in raw_line or "max" in raw_line):
            raw_line = raw_line.split(":")[-1].replace("+", "")

        try:
            val = float(raw_line)
            if val == 0:
                return "0_0"
            val_str = f"{val:g}".replace(".", "_")
            return val_str.replace("-", "minus_") if val < 0 else val_str
        except ValueError:
            # Fallback for strings like "0:1" (Euro Handicap)
            return str(raw_line).replace(":", "_").replace("-", "_")

    @classmethod
    def get_market_slug(cls, sub_type_id: str, parsed_specifiers: dict, fallback_name: str = "") -> str:
        base_slug = cls.MARKET_MAP.get(str(sub_type_id))

        # Fallback if ID is unknown
        if not base_slug:
            clean_name = fallback_name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
            base_slug = f"hockey_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Extract the line value (Total, Handicap, Variant)
        raw_line = (
            parsed_specifiers.get("total") or 
            parsed_specifiers.get("hcp") or 
            parsed_specifiers.get("variant")
        )

        line_str = cls.format_line(str(raw_line)) if raw_line else ""

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Cleans Betika's verbose display strings into standard outcome keys.
        Examples: 
          '1 (-1.5)' -> '1'
          '2 & UNDER 5.5' -> '2_under'
          '1ST PERIOD' -> '1st_period'
        """
        d = display.upper().strip()

        # 1. Strip out Line annotations in parenthesis e.g., "1 (0:1)" -> "1"
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # 2. Recursively handle Combo Markets (e.g., "2 & UNDER 5.5")
        if "&" in d:
            left = BetikaIceHockeyMapper.normalize_outcome(d.split("&")[0])
            right = BetikaIceHockeyMapper.normalize_outcome(d.split("&")[1])
            return f"{left}_{right}"

        # 3. Standard outcome mappings
        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
            "NONE": "none", "NO GOAL": "none",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
            "1ST PERIOD": "1st_period",
            "2ND PERIOD": "2nd_period",
            "3RD PERIOD": "3rd_period",
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

        # 6. Fallback (e.g., "3:2", "1 BY 1-5")
        return d.lower().replace(" ", "_").replace("-", "_")