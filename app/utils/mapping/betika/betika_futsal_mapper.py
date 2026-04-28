# Inside app/workers/mappers/betika.py

import logging

logger = logging.getLogger(__name__)

class BetikaFutsalMapper:
    """Maps Betika Futsal JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":   "futsal_1x2",                # 1X2 (3-way)
        "10":  "futsal_double_chance",      # Double Chance
        "11":  "futsal_draw_no_bet",        # Draw No Bet
        "47":  "futsal_ht_ft",              # Halftime/Fulltime
        "60":  "first_half_futsal_1x2",     # 1st half 1X2
        "264": "futsal_odd_even",           # Odd/Even total goals

        # Dynamic Markets (Require line/specifier appended)
        "15":  "futsal_winning_margin",     # Winning margin (banded)
        "16":  "futsal_handicap",           # Asian handicap / spread
        "18":  "over_under_futsal_goals",   # Total goals (over/under)
        "19":  "futsal_home_team_total",    # Home team total goals
        "20":  "futsal_away_team_total",    # Away team total goals
        "37":  "futsal_1x2_over_under",     # 1X2 + total goals combo
        "66":  "first_half_futsal_handicap",# 1st half handicap
        "68":  "first_half_over_under_futsal_goals", # 1st half total goals
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats specifiers into canonical lines (e.g., '6.5' -> '6_5')."""
        if not raw_line:
            return ""
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
            base_slug = f"futsal_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Extract the line value (total goals, handicap, etc.)
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
          '1' -> '1', '2' -> '2', 'X' -> 'X'
          'OVER 6.5' -> 'over'
        """
        d = display.upper().strip()

        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
        }
        if d in mapping:
            return mapping[d]

        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        if "/" in d and len(d) == 3:
            return d

        return d.lower().replace(" ", "_").replace("-", "_")