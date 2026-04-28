# Inside app/workers/mappers/betika.py or a new file betika_aussie_rules.py

import logging

logger = logging.getLogger(__name__)

class BetikaAussieRulesMapper:
    """Maps Betika Aussie Rules JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "1":    "aussie_rules_1x2",          # 1X2 (3-way)
        "10":   "aussie_rules_double_chance",# Double Chance
        "186":  "aussie_rules_winner",       # Winner (2-way moneyline)
        "60":   "aussie_rules_first_half_1x2", # 1st Half 1X2
        "264":  "aussie_rules_odd_even",     # Odd/Even total points

        # Dynamic Markets (Require line/specifier appended)
        "18":   "over_under_aussie_rules_points",   # Total points
        "19":   "aussie_rules_home_team_total",     # Home team total points
        "20":   "aussie_rules_away_team_total",     # Away team total points
        "68":   "first_half_over_under_aussie_rules_points", # 1st Half total points
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '180.5' -> '180_5')."""
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
            base_slug = f"aussie_rules_{clean_name}"

        if not parsed_specifiers:
            return base_slug

        # Extract the line value (total points, handicap not common in Aussie rules)
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
          'OVER 180.5' -> 'over'
        """
        d = display.upper().strip()

        # 1. Strip out Handicap/Line annotations in parenthesis
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

        # 4. Fallback
        return d.lower().replace(" ", "_").replace("-", "_")