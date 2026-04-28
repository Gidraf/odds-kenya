# Inside app/workers/mappers/betika.py

import logging

logger = logging.getLogger(__name__)

class BetikaCricketMapper:
    """Maps Betika Cricket JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":   "cricket_1x2",                     # 3-Way Match Result
        "10":  "cricket_double_chance",           # Double Chance
        "11":  "cricket_draw_no_bet",             # Draw No Bet
        "340": "cricket_winner_incl_super_over",  # Winner (incl. Super Over)
        "342": "cricket_will_there_be_a_tie",     # Will There Be A Tie
        "382": "cricket_moneyline",               # Standard 2-Way Winner

        # Dynamic Markets (Require line/specifier appended)
        "875": "cricket_home_total_at_1st_dismissal",  # Home team runs at 1st dismissal
        "876": "cricket_away_total_at_1st_dismissal",  # Away team runs at 1st dismissal
        "18":  "over_under_cricket_runs",              # Total runs (match)
        "19":  "cricket_home_team_total",              # Home team total runs (innings)
        "20":  "cricket_away_team_total",              # Away team total runs (innings)
        "229": "over_under_cricket_runs",              # Alternative total runs ID
        "228": "cricket_ah_run_line",                  # Asian handicap / run line
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '23.5' -> '23_5')."""
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
            base_slug = f"cricket_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Extract the line value (total runs or handicap)
        raw_line = (
            parsed_specifiers.get("total") or
            parsed_specifiers.get("hcp") or
            parsed_specifiers.get("variant")
        )

        line_str = cls.format_line(str(raw_line)) if raw_line else ""

        # Special handling for team 1st dismissal totals
        if sub_type_id in ("875", "876"):
            # The specifier usually contains the line (e.g., 23.5)
            return f"{base_slug}_{line_str}" if line_str else base_slug

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Cleans Betika's verbose display strings into standard outcome keys.
        Examples:
          '1' -> '1'
          '2' -> '2'
          'OVER 23.5' -> 'over'
        """
        d = display.upper().strip()

        # Strip out line annotations in parenthesis e.g., "OVER (23.5)" -> "OVER"
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