# Inside app/workers/mappers/betika.py

import logging

logger = logging.getLogger(__name__)

class BetikaHandballMapper:
    """Maps Betika Handball JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":    "handball_1x2",                     # 1X2
        "10":   "handball_double_chance",           # Double Chance
        "11":   "handball_draw_no_bet",             # Draw No Bet
        "15":   "handball_winning_margin",          # Winning Margin (banded)
        "47":   "handball_ht_ft",                   # Halftime/Fulltime
        "60":   "first_half_handball_1x2",          # 1st Half 1X2
        "63":   "first_half_handball_double_chance",# 1st Half Double Chance
        "71":   "first_half_handball_odd_even",     # 1st Half Odd/Even
        "264":  "handball_odd_even",                # Full match Odd/Even
        "432":  "handball_highest_scoring_half",    # Highest Scoring Half

        # Dynamic Markets (Require line/specifier appended)
        "16":   "handball_spread",                  # Asian Handicap (spread)
        "18":   "over_under_handball_goals",        # Total goals (full match)
        "19":   "handball_home_team_total",         # Home team total goals
        "20":   "handball_away_team_total",         # Away team total goals
        "37":   "handball_1x2_over_under",          # 1X2 & Total combo
        "66":   "first_half_handball_spread",       # 1st Half spread
        "68":   "first_half_over_under_handball_goals", # 1st Half total goals
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats specifiers into canonical lines (e.g., '60.5' -> '60_5')."""
        if not raw_line:
            return ""

        # For winning margin bands already in outcome keys, don't modify further
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
            base_slug = f"handball_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Extract line value – total goals, handicap, or winning margin band
        raw_line = (
            parsed_specifiers.get("total") or
            parsed_specifiers.get("hcp") or
            parsed_specifiers.get("variant")
        )

        # For winning margin (sub_type_id=15), the band often comes from variant like "winning_margin:11"
        if sub_type_id == "15" and not raw_line:
            variant = parsed_specifiers.get("variant", "")
            if "winning_margin:" in variant:
                raw_line = variant.split(":")[-1].replace("+", "")

        line_str = cls.format_line(str(raw_line)) if raw_line else ""

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Cleans Betika's verbose display strings into standard outcome keys.
        Examples:
          '1' -> '1', 'X' -> 'X', '2' -> '2'
          'OVER 59.5' -> 'over'
          '2 BY 1-5' -> '2_by_1_5'
          '1st half' -> '1st_half'
        """
        d = display.upper().strip()

        # Strip out any handicap/parenthesis
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # Combo markets (e.g., "X & OVER 60.5")
        if "&" in d:
            left = BetikaHandballMapper.normalize_outcome(d.split("&")[0])
            right = BetikaHandballMapper.normalize_outcome(d.split("&")[1])
            return f"{left}_{right}"

        # Standard mappings
        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
            "1ST HALF": "1st_half",
            "2ND HALF": "2nd_half",
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

        # HT/FT slashes like "1/1", "X/2", "2/1"
        if "/" in d and len(d) in (3, 5):
            return d

        # Winning margin: e.g., "2 BY 1-5" -> "2_by_1_5"
        if " BY " in display:
            parts = display.split(" BY ")
            if len(parts) == 2:
                winner = "1" if parts[0].strip() == "1" else "2"
                margin = parts[1].strip().replace("-", "_").replace("+", "")
                return f"{winner}_by_{margin}"

        return d.lower().replace(" ", "_").replace("-", "_")