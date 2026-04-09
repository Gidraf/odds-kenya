import logging

logger = logging.getLogger(__name__)

class BetikaVolleyballMapper:
    """Maps Betika Volleyball JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "186": "volleyball_match_winner",   # Winner (2-Way)
        "202": "first_set_winner",          # 1st Set - Winner (Betika often sends setnr=1 anyway)

        # Dynamic Markets (Require line/specifier appended)
        "237": "volleyball_point_handicap", # Point Handicap
        "309": "first_set_point_handicap",  # 1st Set - Point Handicap
        "310": "first_set_total_points",    # 1st Set - Total Points
        
        # Placeholders for future standard volleyball markets
        "189": "over_under_volleyball_total_points", # Total Points (Match)
        "188": "volleyball_set_handicap",            # Set Handicap
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '-10.5' -> 'minus_10_5')."""
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
            base_slug = f"volleyball_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Extract the line value (Usually 'hcp' or 'total' in Volleyball)
        raw_line = (
            parsed_specifiers.get("total") or 
            parsed_specifiers.get("hcp") or 
            parsed_specifiers.get("variant")
        )

        # Handle Set Modifiers if they appear dynamically on unknown markets
        if "setnr" in parsed_specifiers and not raw_line:
            if "set" not in base_slug:
                 return f"set_{parsed_specifiers['setnr']}_{base_slug}"

        line_str = cls.format_line(str(raw_line)) if raw_line else ""

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Cleans Betika's verbose display strings into standard outcome keys.
        Examples: 
          '2 (-10.5)' -> '2'
          'OVER 44.5' -> 'over'
        """
        d = display.upper().strip()

        # 1. Strip out Handicap/Line annotations in parenthesis e.g., "2 (-10.5)" -> "2"
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # 2. Standard outcome mappings
        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
        }
        if d in mapping:
            return mapping[d]

        # 3. Handle Over/Under
        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        # 4. Fallback
        return d.lower().replace(" ", "_").replace("-", "_")