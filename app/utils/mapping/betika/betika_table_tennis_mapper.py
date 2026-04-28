# Inside app/workers/mappers/betika.py

import logging
import re

logger = logging.getLogger(__name__)

class BetikaTableTennisMapper:
    """Maps Betika Table Tennis JSON (often using 'soccer_' prefix) to canonical slugs."""

    # Known sub_type_id mapping (if available in raw data, otherwise fallback to pattern)
    MARKET_MAP = {
        "1":    "tt_1x2",                     # 1X2
        "186":  "tt_winner",                  # Winner (2-way)
        "202":  "tt_first_game_winner",       # First game winner
        "204":  "tt_first_game_winner",       # Alias
        "188":  "tt_set_betting",             # Correct score (best of 5/7)
        "189":  "tt_total_points",            # Total points over/under
        "190":  "tt_point_handicap",          # Point handicap
        "191":  "tt_exact_games",             # Exact number of games (e.g., 3,4,5)
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Format numeric line (e.g., '2.5' -> '2_5', '-1.5' -> 'minus_1_5')."""
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
        Map from raw market slug or sub_type_id to canonical slug.
        Since Betika often uses 'soccer_*' for table tennis, we pattern-match.
        """
        slug_lower = raw_market_slug.lower() if raw_market_slug else ""

        # First try sub_type_id mapping
        base_slug = cls.MARKET_MAP.get(str(sub_type_id)) if sub_type_id else None

        # If not found, guess from raw slug
        if not base_slug:
            if "winner" in slug_lower and "1x2" not in slug_lower:
                base_slug = "tt_winner"
            elif "correct_score" in slug_lower or "bestof" in slug_lower:
                base_slug = "tt_set_betting"
            elif "point_handicap" in slug_lower:
                base_slug = "tt_point_handicap"
            elif "total_points" in slug_lower:
                if "1st_game" in slug_lower:
                    base_slug = "tt_first_game_total_points"
                else:
                    base_slug = "tt_total_points"
            elif "exact_games" in slug_lower:
                base_slug = "tt_exact_games"
            elif "1st_game" in slug_lower and "winner" in slug_lower:
                base_slug = "tt_first_game_winner"
            elif "1x2" in slug_lower:
                base_slug = "tt_1x2"
            else:
                # Fallback: clean the raw slug (remove 'soccer_' prefix)
                cleaned = re.sub(r"^soccer_", "", slug_lower)
                cleaned = re.sub(r"^table-tennis_", "", cleaned)
                if cleaned:
                    base_slug = f"tt_{cleaned}"
                else:
                    base_slug = fallback_name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
                    if base_slug:
                        base_slug = f"tt_{base_slug}"
                    else:
                        base_slug = "tt_unknown"

        # Append line specifier if present
        if parsed_specifiers:
            raw_line = (
                parsed_specifiers.get("total") or
                parsed_specifiers.get("hcp") or
                parsed_specifiers.get("variant") or
                parsed_specifiers.get("goalnr")
            )
            line_str = cls.format_line(str(raw_line)) if raw_line else ""
            return f"{base_slug}_{line_str}" if line_str else base_slug

        # For markets where the line is already embedded in the slug (e.g., "soccer_total_points_79_5"),
        # we still need to extract and append because the base slug is generic.
        # However, if raw_market_slug contains numbers like _79_5, we should append them.
        if not parsed_specifiers and raw_market_slug:
            # Try to extract trailing numeric line (e.g., "_79_5")
            match = re.search(r"_(\d+_\d+)$", raw_market_slug)
            if match:
                line_str = match.group(1)
                # But avoid double appending if base_slug already ends with that line (unlikely)
                if not base_slug.endswith(f"_{line_str}"):
                    return f"{base_slug}_{line_str}"
        return base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Convert Betika outcome display strings to standard keys.
        Examples:
          '1' -> '1', '2' -> '2'
          'OVER 79.5' -> 'over'
          '2:3' -> '2:3'
        """
        d = display.upper().strip()
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        mapping = {
            "1": "1", "2": "2",
            "YES": "yes", "NO": "no",
        }
        if d in mapping:
            return mapping[d]

        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        # Score like "3:0", "2:3" – keep as is
        if re.match(r"^\d+:\d+$", d):
            return d

        # Exact games count like "3", "4", "5" (from soccer_exact_games_5)
        if d.isdigit():
            return d

        return d.lower().replace(" ", "_").replace("-", "_")