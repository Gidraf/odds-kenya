# Inside app/workers/mappers/betika.py

import re
import logging

logger = logging.getLogger(__name__)

class BetikaSnookerMapper:
    """Maps Betika Snooker JSON to internal canonical slugs."""

    # Mapping for known sub_type_id (if present in raw data)
    MARKET_MAP = {
        "1":    "snooker_1x2",                   # 1X2 (3-way)
        "186":  "snooker_winner",                # Winner (2-way)
        "204":  "first_frame_winner",            # 1st frame winner
        "188":  "snooker_frame_handicap",        # Frame handicap (asian)
        "189":  "total_frames",                  # Total frames over/under
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats numeric lines (e.g., '22.5' -> '22_5', '-1.5' -> 'minus_1_5')."""
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
    def get_market_slug(cls, sub_type_id: str, parsed_specifiers: dict, fallback_name: str = "", raw_market_slug: str = "") -> str:
        """
        Attempt to map using sub_type_id first; if not available, fallback to slug pattern matching.
        raw_market_slug is the market slug from the JSON (e.g., 'soccer_winner').
        """
        # If we have a known sub_type_id, use it
        if sub_type_id and str(sub_type_id) in cls.MARKET_MAP:
            base_slug = cls.MARKET_MAP[str(sub_type_id)]
        else:
            # Fallback: pattern match based on the raw market slug (since Betika uses 'soccer_*' for snooker)
            base_slug = None
            slug_lower = raw_market_slug.lower()

            if slug_lower == "soccer_winner":
                base_slug = "snooker_winner"
            elif slug_lower == "snooker_1x2":
                base_slug = "snooker_1x2"
            elif "frame_handicap" in slug_lower:
                base_slug = "snooker_frame_handicap"
            elif "total_frames" in slug_lower:
                base_slug = "total_frames"
            elif "1st_frame" in slug_lower and "winner" in slug_lower:
                base_slug = "first_frame_winner"
            else:
                # Ultimate fallback: use cleaned fallback name
                clean_name = fallback_name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
                if clean_name:
                    base_slug = f"snooker_{clean_name}"
                else:
                    base_slug = f"snooker_{slug_lower}"

        if not parsed_specifiers:
            return base_slug

        # Extract line value (handicap or total)
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
          '1' -> '1', '2' -> '2'
          'OVER 22.5' -> 'over'
        """
        d = display.upper().strip()

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