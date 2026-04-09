import logging

logger = logging.getLogger(__name__)

class BetikaHandballMapper:
    """Maps Betika Handball JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":   "handball_1x2",                     # 1X2
        "10":  "handball_double_chance",           # Double Chance
        "11":  "handball_draw_no_bet",             # Who Will Win? (If Draw, Money Back)
        "47":  "handball_ht_ft",                   # Halftime/Fulltime
        "60":  "first_half_handball_1x2",          # 1st Half - 1X2
        "63":  "first_half_handball_double_chance",# 1st Half - Double Chance

        # Dynamic Markets (Require line/specifier appended)
        "15":  "handball_winning_margin",               # Winning Margin
        "16":  "handball_spread",                       # Asian Handicap
        "18":  "over_under_handball_goals",             # Total
        "19":  "handball_home_team_total",              # Home Team Total
        "20":  "handball_away_team_total",              # Away Team Total
        "37":  "handball_1x2_over_under",               # 1X2 & Total
        "66":  "first_half_handball_spread",            # 1st Half - Asian Handicap
        "68":  "first_half_over_under_handball_goals",  # 1st Half - Total
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '-1.5' -> 'minus_1_5')."""
        if not raw_line:
            return ""

        # Handle complex variants like 'sr:winning_margin:11+' -> '11'
        if ":" in raw_line and "winning_margin" in raw_line:
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
            base_slug = f"handball_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Extract the line value (Total, Handicap, or Variant)
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
          'X & OVER 63.5' -> 'X_over'
          '1 BY 1-5' -> '1_by_1_5'
        """
        d = display.upper().strip()

        # 1. Strip out Handicap/Line annotations in parenthesis e.g., "1 (-0.5)" -> "1"
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # 2. Recursively handle Combo Markets (e.g., "X & OVER 63.5")
        if "&" in d:
            left = BetikaHandballMapper.normalize_outcome(d.split("&")[0])
            right = BetikaHandballMapper.normalize_outcome(d.split("&")[1])
            return f"{left}_{right}"

        # 3. Standard outcome mappings
        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
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

        # 6. Fallback (e.g., "1 BY 1-5")
        return d.lower().replace(" ", "_").replace("-", "_")