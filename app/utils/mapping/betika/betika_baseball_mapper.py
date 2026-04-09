import logging

logger = logging.getLogger(__name__)

class BetikaBaseballMapper:
    """Maps Betika Baseball JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":   "baseball_1x2",             # 1X2
        "251": "baseball_moneyline",       # Winner (Incl. Extra Innings)
        "264": "baseball_odd_even",        # Odd/Even (Incl. Extra Innings)
        "274": "baseball_f5_1x2",          # Innings 1 to 5 - 1X2
        "287": "baseball_1st_inning_1x2",  # 1st Inning - 1X2

        # Dynamic Markets (Require line/specifier appended)
        "256": "baseball_spread",                    # Handicap (Incl. Extra Innings)
        "258": "over_under_baseball_runs",           # Total (Incl. Extra Innings)
        "260": "baseball_home_team_total",           # Team 1 Total
        "261": "baseball_away_team_total",           # Team 2 Total
        "275": "baseball_f5_spread",                 # Innings 1 to 5 - Handicap
        "276": "over_under_baseball_f5_runs",        # Innings 1 to 5 - Total
        "288": "over_under_baseball_1st_inning_runs" # 1st Inning - Total
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '-1.5' -> 'minus_1_5')."""
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
            base_slug = f"baseball_{clean_name}"

        # If it's a known static market and doesn't require a line, return it immediately
        # (Though we still check for specifiers just in case)
        if not parsed_specifiers:
            return base_slug

        # Extract the line value for dynamic markets
        raw_line = parsed_specifiers.get("total") or parsed_specifiers.get("hcp") or parsed_specifiers.get("inningnr")
        
        # Prevent appending '_1' for 1st inning static markers if already defined as 1st inning
        if "1st_inning" in base_slug and "total" not in parsed_specifiers:
             return base_slug

        line_str = cls.format_line(str(raw_line)) if raw_line else ""

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Cleans Betika's verbose display strings into standard outcome keys.
        Examples: 
          '1 (-1.5)' -> '1'
          'OVER 5.5' -> 'over'
        """
        d = display.upper().strip()

        # 1. Strip out Handicap/Line annotations in parenthesis e.g., "1 (-1.5)" -> "1"
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # 2. Standard outcome mappings
        mapping = {
            "1": "1", "X": "X", "2": "2",
            "EVEN": "even", "ODD": "odd",
        }
        if d in mapping:
            return mapping[d]

        # 3. Handle Over/Under
        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        # 4. Fallback
        return d.lower()