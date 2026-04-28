# Inside app/workers/mappers/betika.py

import logging
import re

logger = logging.getLogger(__name__)

class BetikaSoccerMapper:
    """Maps Betika Soccer JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Core result markets
        "1":    "1x2",                           # 1X2
        "8":    "first_team_to_score",           # First team to score
        "10":   "double_chance",                 # Double chance
        "11":   "draw_no_bet",                   # Draw no bet
        "14":   "european_handicap",             # European handicap (3‑way)
        "15":   "winning_margin",                # Winning margin band
        "16":   "asian_handicap",                # Asian handicap (2‑way)
        "18":   "over_under",                    # Total goals
        "19":   "home_over_under",               # Home team total
        "20":   "away_over_under",               # Away team total
        "21":   "exact_goals",                   # Exact goals (0,1,2,3,4,5,6+)
        "29":   "btts",                          # Both teams to score
        "35":   "1x2_btts",                     # 1X2 + BTTS
        "36":   "over_under_btts",              # BTTS + total goals
        "37":   "1x2_over_under",               # 1X2 + total goals
        "38":   "first_goalscorer",             # First goalscorer
        "39":   "last_goalscorer",              # Last goalscorer
        "40":   "anytime_goalscorer",           # Anytime goalscorer
        "45":   "correct_score",                # Correct score
        "47":   "ht_ft",                        # Half time / full time
        "60":   "first_half_1x2",               # First half 1X2
        "63":   "first_half_double_chance",     # First half double chance
        "65":   "first_half_european_handicap", # First half European handicap
        "66":   "first_half_asian_handicap",    # First half Asian handicap
        "68":   "first_half_over_under",        # First half total goals
        "69":   "first_half_home_over_under",   # First half home total
        "70":   "first_half_away_over_under",   # First half away total
        "71":   "first_half_exact_goals",       # First half exact goals
        "75":   "first_half_btts",              # First half BTTS
        "105":  "10_minutes_1x2",               # 10 minutes 1X2
        "136":  "booking_1x2",                  # Booking 1X2
        "137":  "first_booking",                # First booking
        "139":  "total_bookings",               # Total bookings
        "146":  "red_card",                     # Red card
        "149":  "first_half_booking_1x2",       # First half booking 1X2
        "162":  "corner_1x2",                   # Corner 1X2
        "163":  "first_corner",                 # First corner
        "166":  "total_corners",                # Total corners
        "169":  "corner_range",                 # Corner range (e.g., 0-8, 9-11, 12+)
        "173":  "first_half_corner_1x2",        # First half corner 1X2
        "177":  "first_half_total_corners",     # First half total corners
        "542":  "first_half_double_chance_btts",# First half double chance + BTTS
        "543":  "second_half_1x2_btts",         # Second half 1X2 + BTTS
        "544":  "second_half_1x2_over_under",   # Second half 1X2 + total
        "546":  "double_chance_btts",           # Double chance + BTTS
        "547":  "double_chance_over_under",     # Double chance + total
        "548":  "multigoals",                   # Multigoals (goal range betting)
        "818":  "ht_ft_over_under",             # HT/FT + total goals
        # Additional from your JSON
        "548":  "multigoals",                   # Already present
        "549":  "team_multigoals",              # Team‑specific multigoals
        "550":  "both_halves_over_under",       # Both halves over/under 1.5
        "551":  "both_halves_btts",             # Both halves both teams to score
        "552":  "team_score_both_halves",       # Named team scores in both halves
        "553":  "win_both_halves",              # Team wins both halves
        "554":  "highest_scoring_half",         # Highest scoring half
        "555":  "team_highest_scoring_half",    # Team‑specific highest scoring half
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Format numeric specifier to slug fragment (e.g., '2.5' -> '2_5', '-1.5' -> 'minus_1_5')."""
        if not raw_line:
            return ""
        # Handle European handicap lines like '0:1' -> '0_1'
        if ":" in raw_line:
            return raw_line.replace(":", "_")
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
        Build canonical slug using sub_type_id mapping, then add line specifier if any.
        If sub_type_id is missing, fallback to raw_market_slug pattern.
        """
        sid = str(sub_type_id) if sub_type_id else None
        base_slug = cls.MARKET_MAP.get(sid) if sid else None

        # Fallback: guess from raw market slug (e.g., "soccer_atalanta_bc_exact_goals_3")
        if not base_slug and raw_market_slug:
            slug_lower = raw_market_slug.lower()
            # Team exact goals: soccer_home_exact_goals -> home_exact_goals
            if "exact_goals" in slug_lower:
                if "home" in slug_lower or "cagliari" in slug_lower:
                    base_slug = "home_exact_goals"
                elif "away" in slug_lower or "atalanta" in slug_lower:
                    base_slug = "away_exact_goals"
                else:
                    base_slug = "exact_goals"
            elif "odd_even" in slug_lower:
                if "home" in slug_lower or "cagliari" in slug_lower:
                    base_slug = "home_odd_even"
                elif "away" in slug_lower or "atalanta" in slug_lower:
                    base_slug = "away_odd_even"
                else:
                    base_slug = "odd_even"
            elif "clean_sheet" in slug_lower:
                base_slug = "clean_sheet"
            elif "win_to_nil" in slug_lower:
                base_slug = "win_to_nil"
            elif "multigoals" in slug_lower:
                if "home" in slug_lower or "cagliari" in slug_lower:
                    base_slug = "home_multigoals"
                elif "away" in slug_lower or "atalanta" in slug_lower:
                    base_slug = "away_multigoals"
                else:
                    base_slug = "multigoals"
            elif "corner_range" in slug_lower:
                base_slug = "corner_range"
            elif "first_half" in slug_lower and "corner_range" in slug_lower:
                base_slug = "first_half_corner_range"
            elif "first_half" in slug_lower and "1st_corner" in slug_lower:
                base_slug = "first_half_first_corner"
            elif "1st_goal" in slug_lower and "1x2" in slug_lower:
                base_slug = "first_goal_and_1x2"
            else:
                # Ultimate fallback: clean the raw slug by removing team names etc.
                clean = re.sub(r"(soccer_|cagliari|atalanta_bc|_bc|_no_bet)", "", slug_lower)
                base_slug = clean.strip("_")

        # If still no base_slug, use fallback_name
        if not base_slug:
            base_slug = fallback_name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
            if base_slug:
                base_slug = f"bf_{base_slug}"  # prefix to avoid generic
            else:
                base_slug = f"unknown_{sid or raw_market_slug}"

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

        return base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """
        Convert Betika's outcome display into standard outcome keys.
        Handles simple outcomes, combos, over/under, player names, etc.
        """
        d = display.upper().strip()
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # Combo markets like "2 & OVER 2.5"
        if " & " in d:
            parts = d.split(" & ")
            left = BetikaSoccerMapper.normalize_outcome(parts[0])
            right = BetikaSoccerMapper.normalize_outcome(parts[1])
            return f"{left}_{right}"

        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no", "NONE": "none",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
            "1ST HALF": "1st_half",
            "2ND HALF": "2nd_half",
            "EQUAL": "equal",
            "ODD": "odd", "EVEN": "even",
            "ONLY 1": "only_1", "ONLY 2": "only_2", "BOTH TEAMS": "both_teams",
        }
        if d in mapping:
            return mapping[d]

        # Over/Under
        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        # Correct score like "2:1" stays as is
        if re.match(r"^\d+:\d+$", d):
            return d

        # HT/FT like "1/1", "X/2", etc.
        if "/" in d and len(d) in (3, 5):
            return d.lower()

        # Winning margin outcomes like "1 BY 1" -> "1_by_1"
        if " BY " in display:
            parts = display.split(" BY ")
            if len(parts) == 2:
                winner = "1" if parts[0].strip() == "1" else "2"
                margin = parts[1].strip().replace("-", "_").replace("+", "")
                return f"{winner}_by_{margin}"

        # Player names (first/last/anytime goalscorer)
        return d.lower().replace(" ", "_").replace(",_", "_")