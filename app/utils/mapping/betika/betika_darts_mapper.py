# Inside app/workers/mappers/betika.py

import logging

logger = logging.getLogger(__name__)

class BetikaDartsMapper:
    """Maps Betika Darts JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "186": "darts_winner",        # Winner (2-Way Moneyline)
        "1":   "darts_1x2",           # 1X2 (3-Way)
        "10":  "darts_double_chance", # Double Chance
        "11":  "darts_draw_no_bet",   # Draw No Bet

        # Dynamic Markets (Require line/specifier appended)
        "18":  "over_under_darts_total_legs",     # Total legs over/under
        "19":  "darts_home_player_total_legs",    # Home player leg total
        "20":  "darts_away_player_total_legs",    # Away player leg total
        "188": "darts_correct_score",             # Correct score (sets/legs)
        "189": "darts_set_handicap",              # Set handicap
        "192": "darts_leg_handicap",              # Leg handicap
        "190": "darts_player_180s_over_under",    # Player 180s over/under
        "191": "darts_total_180s",                # Total 180s in match
        "193": "darts_first_180",                 # First player to hit 180
        "194": "darts_highest_checkout",          # Highest checkout
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats specifiers into canonical lines (e.g., '5.5' -> '5_5')."""
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
            base_slug = f"darts_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Extract the line value (total legs, handicap, etc.)
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
          '1' -> '1'
          '2' -> '2'
          'OVER 5.5' -> 'over'
        """
        d = display.upper().strip()

        # Strip out line annotations in parenthesis
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
        }
        if d in mapping:
            return mapping[d]

        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        return d.lower().replace(" ", "_").replace("-", "_")