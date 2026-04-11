class BetikaSoccerMapper:
    """Maps Betika Soccer JSON to internal canonical slugs."""

    MARKET_MAP = {
        "1":   "match_winner",
        "8":   "first_team_to_score",
        "10":  "double_chance",
        "11":  "draw_no_bet",
        "14":  "european_handicap",
        "15":  "winning_margin",
        "16":  "asian_handicap",
        "18":  "over_under",
        "19":  "home_over_under",
        "20":  "away_over_under",
        "21":  "exact_goals",
        "29":  "btts",
        "35":  "1x2_btts",
        "36":  "over_under_btts",
        "37":  "1x2_over_under",
        "38":  "first_goalscorer",
        "39":  "last_goalscorer",
        "40":  "anytime_goalscorer",
        "45":  "correct_score",
        "47":  "ht_ft",
        "60":  "first_half_1x2",
        "63":  "first_half_double_chance",
        "65":  "first_half_european_handicap",
        "66":  "first_half_asian_handicap",
        "68":  "first_half_over_under",
        "69":  "first_half_home_over_under",
        "70":  "first_half_away_over_under",
        "71":  "first_half_exact_goals",
        "75":  "first_half_btts",
        "105": "10_minutes_1x2",
        "136": "booking_1x2",
        "137": "first_booking",
        "139": "total_bookings",
        "146": "red_card",
        "149": "first_half_booking_1x2",
        "162": "corner_1x2",
        "163": "first_corner",
        "166": "total_corners",
        "169": "corner_range",
        "173": "first_half_corner_1x2",
        "177": "first_half_total_corners",
        "542": "first_half_double_chance_btts",
        "543": "second_half_1x2_btts",
        "544": "second_half_1x2_over_under",
        "546": "double_chance_btts",
        "547": "double_chance_over_under",
        "548": "multigoals",
        "818": "ht_ft_over_under",
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines (e.g., '-1.5' -> 'minus_1_5')."""
        if not raw_line:
            return ""
            
        # Handle exact goal variants like 'sr:exact_goals:3+'
        if ":" in raw_line and "exact" in raw_line:
            raw_line = raw_line.split(":")[-1].replace("+", "")

        try:
            # Handle standard numeric lines (totals, asian handicaps)
            val = float(raw_line)
            if val == 0:
                return "0_0"
            val_str = f"{val:g}".replace(".", "_")
            return val_str.replace("-", "minus_") if val < 0 else val_str
        except ValueError:
            # Fallback for strings like "0:1" (Euro Handicap) or raw ranges
            return str(raw_line).replace(":", "_").replace("-", "_")

    @classmethod
    def get_market_slug(cls, sub_type_id: str, parsed_specifiers: dict, fallback_name: str = "") -> str:
        base_slug = cls.MARKET_MAP.get(str(sub_type_id))
        
        # Fallback if ID is unknown
        if not base_slug:
            base_slug = fallback_name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")

        if not parsed_specifiers:
            return base_slug

        raw_line = (
            parsed_specifiers.get("total") or 
            parsed_specifiers.get("hcp") or 
            parsed_specifiers.get("variant") or 
            parsed_specifiers.get("goalnr")
        )

        line_str = cls.format_line(str(raw_line)) if raw_line else ""

        return f"{base_slug}_{line_str}" if line_str else base_slug

    @staticmethod
    def normalize_outcome(display: str) -> str:
        """Cleans Betika's verbose display strings into standard outcome keys."""
        d = display.upper().strip()
        
        if "(" in d and d.endswith(")"):
            d = d.split("(")[0].strip()
            
        if "&" in d:
            left = BetikaSoccerMapper.normalize_outcome(d.split("&")[0])
            right = BetikaSoccerMapper.normalize_outcome(d.split("&")[1])
            return f"{left}_{right}"
            
        mapping = {
            "1": "1", "X": "X", "2": "2",
            "YES": "yes", "NO": "no",
            "NONE": "none", "NO GOAL": "none",
            "1/X": "1X", "X/2": "X2", "1/2": "12",
        }
        if d in mapping: return mapping[d]
        if d.startswith("OVER"): return "over"
        if d.startswith("UNDER"): return "under"
        if "/" in d and len(d) == 3: return d 
        return d.lower()