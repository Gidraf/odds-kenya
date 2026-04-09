import logging

logger = logging.getLogger(__name__)

class BetikaTennisMapper:
    """Maps Betika Tennis JSON to internal canonical slugs."""

    MARKET_MAP = {
        # Static Markets (No lines/specifiers)
        "186": "tennis_match_winner",        # Winner (2-Way)
        "204": "first_set_winner",           # First Set Winner
        "231": "second_set_winner",          # Second Set Winner
        "264": "tennis_odd_even_games",      # Odd/Even Games
        "265": "tennis_odd_even_s1_games",   # Odd/Even Games (1st Set)
        "433": "tennis_s1_and_match_winner", # 1st Set/Match Winner
        
        # Dynamic Markets (Require line/specifier appended)
        "187": "tennis_game_handicap",       # Game Handicap (Match)
        "188": "tennis_set_handicap",        # Set Handicap
        "189": "over_under_tennis_games",    # Total Games (Match)
        "190": "p1_over_under_games",        # Player 1 Total Games
        "191": "p2_over_under_games",        # Player 2 Total Games
        "192": "tennis_s1_game_handicap",    # 1st Set Game Handicap
        "193": "over_under_s1_games",        # 1st Set Total Games
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '-1.5' -> 'minus_1_5')."""
        if not raw_line:
            return ""

        # Catch specific string-based variants if Betika passes them (e.g. correct score)
        if ":" in raw_line and "variant" in raw_line:
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
            base_slug = f"tennis_{clean_name}"

        # Static Markets return immediately
        if not parsed_specifiers:
            return base_slug

        # Extract the primary line value (Total Games or Handicap)
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
          '1 (+0.5)' -> '1'
          'OVER 22.5' -> 'over'
        """
        d = display.upper().strip()

        # 1. Strip out Line annotations in parenthesis e.g., "1 (+0.5)" -> "1"
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()

        # 2. Standard outcome mappings
        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
            "EVEN": "even", "ODD": "odd"
        }
        if d in mapping:
            return mapping[d]

        # 3. Handle Over/Under
        if d.startswith("OVER"):
            return "over"
        if d.startswith("UNDER"):
            return "under"

        # 4. Handle Specific Set Betting / Scores (e.g. "2:0", "1:2")
        if ":" in d:
            return d.strip()

        # 5. Fallback
        return d.lower().replace(" ", "_").replace("-", "_")