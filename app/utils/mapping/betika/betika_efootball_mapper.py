# Inside app/workers/mappers/betika.py

import logging

logger = logging.getLogger(__name__)

class BetikaEFootballMapper:
    """Maps Betika eFootball (eSoccer) JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":   "efootball_1x2",          # 1X2 (3-way)
        "8":   "efootball_first_team_to_score",  # First team to score
        "10":  "efootball_double_chance",        # Double chance
        "11":  "efootball_draw_no_bet",          # Draw no bet
        "14":  "efootball_european_handicap",    # European handicap (3-way)
        "16":  "efootball_asian_handicap",       # Asian handicap (2-way)
        "21":  "efootball_exact_goals",          # Exact goals (0,1,2,3+)
        "29":  "efootball_btts",                 # Both teams to score
        "35":  "efootball_1x2_btts",             # 1X2 + BTTS
        "36":  "efootball_over_under_btts",      # BTTS + total goals
        "45":  "efootball_correct_score",        # Correct score
        "47":  "efootball_ht_ft",                # Half time / full time
        "60":  "first_half_efootball_1x2",       # 1st half 1X2
        "75":  "first_half_efootball_btts",      # 1st half BTTS
        "105": "efootball_10_minutes_1x2",       # 10 minutes 1X2 (e-sports)
        "548": "efootball_multigoals",           # Multigoals (range betting)

        # Dynamic Markets (Require line/specifier appended)
        "18":  "over_under_efootball_goals",     # Total goals (over/under)
        "68":  "first_half_over_under_efootball_goals",  # 1st half total goals
        "19":  "efootball_home_team_total",      # Home team total goals
        "20":  "efootball_away_team_total",      # Away team total goals
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats specifiers into canonical lines (e.g., '2.5' -> '2_5')."""
        if not raw_line:
            return ""

        # Handle exact goal variants like 'sr:exact_goals:3+'
        if ":" in raw_line and "exact" in raw_line:
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
            base_slug = f"efootball_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Extract the line value (total goals, handicap, etc.)
        raw_line = (
            parsed_specifiers.get("total") or
            parsed_specifiers.get("hcp") or
            parsed_specifiers.get("variant") or
            parsed_specifiers.get("goalnr")
        )

        line_str = cls.format_line(str(raw_line)) if raw_line else ""

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Cleans Betika's verbose display strings into standard outcome keys.
        Examples:
          '1' -> '1', '2' -> '2', 'X' -> 'X'
          'OVER 2.5' -> 'over'
          '1 & OVER 2.5' -> '1_over'
        """
        d = display.upper().strip()

        # Strip out line annotations in parenthesis
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # Handle combo markets (e.g., "1 & OVER 2.5")
        if "&" in d:
            left = BetikaEFootballMapper.normalize_outcome(d.split("&")[0])
            right = BetikaEFootballMapper.normalize_outcome(d.split("&")[1])
            return f"{left}_{right}"

        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
            "NONE": "none",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
        }
        if d in mapping:
            return mapping[d]

        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        # Handle HT/FT slashes (e.g., "1/1", "X/2")
        if "/" in d and len(d) == 3:
            return d

        return d.lower().replace(" ", "_").replace("-", "_")