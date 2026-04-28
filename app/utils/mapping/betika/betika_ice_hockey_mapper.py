# Inside app/workers/mappers/betika.py

import logging

logger = logging.getLogger(__name__)

class BetikaIceHockeyMapper:
    """Maps Betika Ice Hockey JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":    "hockey_3way",                     # 1X2 (Regulation Time)
        "8":    "hockey_first_goal",               # First goal (which team or none)
        "10":   "double_chance",                   # Double Chance (universal slug)
        "11":   "draw_no_bet",                     # Draw No Bet
        "186":  "hockey_moneyline",                # Winner (2-way, incl OT & penalties)
        "199":  "hockey_correct_score",            # Correct score (full match)
        "264":  "hockey_odd_even",                 # Odd/Even total goals (incl OT)
        "342":  "hockey_overtime",                 # Will there be overtime
        "432":  "hockey_highest_scoring_period",   # Highest scoring period
        "60":   "hockey_p1_winner",                # 1st period 1X2
        "75":   "hockey_p1_btts",                  # 1st period both teams to score (if exists)
        "29":   "hockey_btts",                     # Both teams to score (full match)
        "47":   "hockey_ht_ft",                    # Halftime/Fulltime (period/full game)

        # Dynamic Markets (Require line/specifier appended)
        "14":   "european_handicap",               # European handicap (3-way, specifiers like 0:1)
        "15":   "hockey_winning_margin",           # Winning margin (banded)
        "16":   "asian_handicap",                  # Asian handicap (2-way)
        "18":   "hockey_total_goals",              # Total goals (full match)
        "19":   "hockey_home_team_total",          # Home team total goals
        "20":   "hockey_away_team_total",          # Away team total goals
        "37":   "hockey_result_and_total",         # 1X2 + total goals combo
        "68":   "hockey_p1_total",                 # 1st period total goals
        "69":   "hockey_p1_home_total",            # 1st period home team total
        "70":   "hockey_p1_away_total",            # 1st period away team total
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats specifiers into canonical lines (e.g., '6.5' -> '6_5', '0:1' -> '0_1')."""
        if not raw_line:
            return ""

        # Handle European handicap specifier like "0:1" -> "0_1"
        if ":" in raw_line:
            return raw_line.replace(":", "_")

        # Handle winning margin band already as string like "3" or "11+"
        if "by" in raw_line:
            return raw_line.replace(" ", "_").replace("+", "")

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

        if not base_slug:
            clean_name = fallback_name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
            base_slug = f"hockey_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Determine the key line value
        raw_line = (
            parsed_specifiers.get("total") or
            parsed_specifiers.get("hcp") or
            parsed_specifiers.get("variant")
        )

        # For European handicap, the specifier often is like "variant": "sr:handicap:0:1"
        if sub_type_id == "14" and not raw_line:
            variant = parsed_specifiers.get("variant", "")
            if "handicap:" in variant:
                parts = variant.split(":")
                if len(parts) >= 4:
                    raw_line = f"{parts[2]}:{parts[3]}"

        line_str = cls.format_line(str(raw_line)) if raw_line else ""

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Cleans Betika's verbose display strings into standard outcome keys.
        Examples:
          '1' -> '1', 'X' -> 'X', '2' -> '2'
          '1 (-1.5)' -> '1'
          'OVER 6.5' -> 'over'
          '1 BY 1-5' -> '1_by_1_5'
          '1st period' -> '1st_period'
        """
        d = display.upper().strip()

        # Strip handicap/parenthesis
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # Combo markets like "X & UNDER 5.5"
        if "&" in d:
            left = BetikaIceHockeyMapper.normalize_outcome(d.split("&")[0])
            right = BetikaIceHockeyMapper.normalize_outcome(d.split("&")[1])
            return f"{left}_{right}"

        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no", "NONE": "none",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
            "1ST PERIOD": "1st_period",
            "2ND PERIOD": "2nd_period",
            "3RD PERIOD": "3rd_period",
            "EQUAL": "equal",
            "ODD": "odd", "EVEN": "even",
        }
        if d in mapping:
            return mapping[d]

        # Over/Under
        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        # Winning margin: e.g., "1 BY 1-5" -> "1_by_1_5"
        if " BY " in display:
            parts = display.split(" BY ")
            if len(parts) == 2:
                winner = "1" if parts[0].strip() == "1" else "2"
                margin = parts[1].strip().replace("-", "_").replace("+", "")
                return f"{winner}_by_{margin}"

        # Correct score: leave as is (e.g., "3:2")
        if ":" in d and not d.startswith("OVER"):
            return d

        return d.lower().replace(" ", "_").replace("-", "_")