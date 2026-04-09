import logging

logger = logging.getLogger(__name__)

class BetikaRugbyMapper:
    """Maps Betika Rugby JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":   "rugby_1x2",                  # 1X2
        "10":  "rugby_double_chance",        # Double Chance
        "47":  "rugby_ht_ft",                # Halftime/Fulltime
        "60":  "rugby_first_half_1x2",       # 1st Half - 1X2
        "264": "rugby_odd_even_pts",         # Odd/Even Points (Based on typical Betika ID)
        "432": "rugby_highest_scoring_half", # Highest Scoring Half (Based on typical Betika ID)

        # Dynamic Markets (Require line/specifier appended)
        "15":  "rugby_winning_margin",       # Winning Margin
        "16":  "rugby_spread",               # Asian Handicap (Spread)
        "18":  "over_under_rugby_pts",       # Total Points
        "66":  "rugby_first_half_spread",    # 1st Half - Asian Handicap
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '56.5' -> '56_5')."""
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
            base_slug = f"rugby_{clean_name}"

        # If there are no dynamic specifiers, return the base slug
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
          '1 (+1.5)' -> '1'
          'OVER 56.5' -> 'over'
        """
        d = display.upper().strip()

        # 1. Strip out Line annotations in parenthesis e.g., "1 (+1.5)" -> "1"
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # 2. Standard outcome mappings
        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
        }
        if d in mapping:
            return mapping[d]

        # 3. Handle Over/Under
        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        # 4. Handle HT/FT slashes (e.g., "1/1", "X/2")
        if "/" in d and len(d) == 3:
            return d 

        # 5. Fallback
        return d.lower().replace(" ", "_").replace("-", "_")