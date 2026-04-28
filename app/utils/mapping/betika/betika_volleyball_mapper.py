# Inside app/workers/mappers/betika.py

import logging
import re

logger = logging.getLogger(__name__)

class BetikaVolleyballMapper:
    """Maps Betika Volleyball JSON to internal canonical slugs."""

    # Known sub_type_id mapping (if available in raw data)
    MARKET_MAP = {
        # Static markets
        "1":    "volleyball_1x2",                # 1X2 (3-way)
        "186":  "volleyball_match_winner",       # 2-way winner
        "202":  "first_set_winner",              # First set winner
        "204":  "first_set_winner",              # Alias
        "264":  "volleyball_odd_even",           # Odd/even total points
        "432":  "volleyball_highest_scoring_set", # Highest scoring set (if present)
        # Dynamic markets
        "188":  "volleyball_set_betting",        # Correct score (3:0, 3:1, etc.)
        "189":  "volleyball_total_points",       # Total match points
        "190":  "volleyball_point_handicap",     # Point handicap (match)
        "191":  "volleyball_set_handicap",       # Set handicap (asian)
        "192":  "first_set_point_handicap",      # 1st set point handicap
        "193":  "first_set_total_points",        # 1st set total points
        "194":  "volleyball_total_sets",         # Total sets over/under
        "195":  "volleyball_exact_sets",         # Exact number of sets (3,4,5)
        "196":  "volleyball_team_to_win_a_set",  # Named team to win at least one set
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Format numeric line (e.g., '10.5' -> '10_5', '-1.5' -> 'minus_1_5')."""
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
        Map to canonical slug using sub_type_id when possible, otherwise by pattern.
        """
        sid = str(sub_type_id) if sub_type_id else None
        base_slug = cls.MARKET_MAP.get(sid) if sid else None

        # Fallback: pattern matching from raw slug
        if not base_slug and raw_market_slug:
            slug_lower = raw_market_slug.lower()
            if "match_winner" in slug_lower:
                base_slug = "volleyball_match_winner"
            elif "1x2" in slug_lower:
                base_slug = "volleyball_1x2"
            elif "set_handicap" in slug_lower:
                base_slug = "volleyball_set_handicap"
            elif "point_handicap" in slug_lower:
                if "first_set" in slug_lower:
                    base_slug = "first_set_point_handicap"
                else:
                    base_slug = "volleyball_point_handicap"
            elif "total_points" in slug_lower:
                if "first_set" in slug_lower:
                    base_slug = "first_set_total_points"
                else:
                    base_slug = "volleyball_total_points"
            elif "total_sets" in slug_lower:
                base_slug = "volleyball_total_sets"
            elif "exact_sets" in slug_lower:
                base_slug = "volleyball_exact_sets"
            elif "correct_score" in slug_lower or "bestof" in slug_lower:
                base_slug = "volleyball_set_betting"
            elif "first_set_winner" in slug_lower or "1st_set_winner" in slug_lower:
                base_slug = "first_set_winner"
            elif "odd_even" in slug_lower or "1st_set___odd_even" in slug_lower:
                base_slug = "volleyball_odd_even"
            elif "to_win_a_set" in slug_lower:
                base_slug = "volleyball_team_to_win_a_set"
            else:
                # Fallback: clean the slug by removing prefixes
                cleaned = re.sub(r"^volleyball_", "", slug_lower)
                cleaned = re.sub(r"^first_set_", "", cleaned)
                if cleaned:
                    base_slug = f"volleyball_{cleaned}"
                else:
                    base_slug = fallback_name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
                    if not base_slug:
                        base_slug = f"volleyball_{slug_lower}"

        # Append line specifier from parsed_specifiers or from raw slug
        line_str = ""
        if parsed_specifiers:
            raw_line = (
                parsed_specifiers.get("total") or
                parsed_specifiers.get("hcp") or
                parsed_specifiers.get("variant")
            )
            if raw_line:
                line_str = cls.format_line(str(raw_line))
        else:
            # If no parsed_specifiers, try to extract line from raw slug (e.g., "set_handicap_1_5")
            match = re.search(r"_(\d+_\d+)$", raw_market_slug) if raw_market_slug else None
            if match:
                line_str = match.group(1)

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Convert Betika outcome display strings to standard keys.
        Examples:
          '1' -> '1', '2' -> '2'
          'OVER 181.5' -> 'over'
          '3:0' -> '3:0'
          'YES' -> 'yes', 'NO' -> 'no'
        """
        d = display.upper().strip()
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        mapping = {
            "1": "1", "2": "2",
            "YES": "yes", "NO": "no",
            "EVEN": "even", "ODD": "odd",
        }
        if d in mapping:
            return mapping[d]

        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        # Set scores like "3:0", "2:3"
        if re.match(r"^\d+:\d+$", d):
            return d

        # Exact sets count (3,4,5)
        if d.isdigit():
            return d

        return d.lower().replace(" ", "_").replace("-", "_")