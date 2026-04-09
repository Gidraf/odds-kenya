import logging

logger = logging.getLogger(__name__)

class BetikaBoxingMapper:
    """Maps Betika Boxing JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "186": "boxing_moneyline",  # Winner (2-Way)
        "1":   "boxing_1x2",        # Fallback if they ever use a 3-Way (1X2)
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '5.5' -> '5_5')."""
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

        # Fallback if ID is unknown (prepends "boxing_" to keep it organized)
        if not base_slug:
            clean_name = fallback_name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
            base_slug = f"boxing_{clean_name}"

        # If there are no dynamic specifiers, return the base slug
        if not parsed_specifiers:
            return base_slug

        # Extract the line value for dynamic markets (e.g., Total Rounds)
        raw_line = parsed_specifiers.get("total") or parsed_specifiers.get("hcp") or parsed_specifiers.get("variant")

        line_str = cls.format_line(str(raw_line)) if raw_line else ""

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Cleans Betika's verbose display strings into standard outcome keys.
        """
        d = display.upper().strip()

        # 1. Strip out Line annotations in parenthesis e.g., "OVER (5.5)" -> "OVER"
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
        return d.lower()