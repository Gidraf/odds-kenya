class BetikaEFootballMapper:
    """Maps Betika E-Soccer JSON to internal canonical slugs."""

    # Same Betika IDs, mapped to efootball canonicals
    MARKET_MAP = {
        "1":   "efootball_1x2",
        "8":   "efootball_first_team_to_score",
        "10":  "efootball_double_chance",
        "11":  "efootball_draw_no_bet",
        "14":  "efootball_european_handicap",
        "16":  "efootball_asian_handicap",
        "18":  "over_under_efootball_goals",
        "21":  "efootball_exact_goals",
        "29":  "efootball_btts",
        "35":  "efootball_1x2_btts",
        "36":  "efootball_over_under_btts",
        "45":  "efootball_correct_score",
        "47":  "efootball_ht_ft",
        "60":  "first_half_efootball_1x2",
        "68":  "first_half_over_under_efootball_goals",
        "75":  "first_half_efootball_btts",
        "105": "efootball_10_minutes_1x2",
        "548": "efootball_multigoals",
    }

    @staticmethod
    def format_line(raw_line: str) -> str:
        """Formats string specifiers into canonical lines."""
        if not raw_line:
            return ""
            
        if ":" in raw_line and "exact" in raw_line:
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
        
        if not base_slug:
            # Prepend efootball_ to unknown fallbacks for consistency
            clean_name = fallback_name.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
            base_slug = f"efootball_{clean_name}"

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
            left = BetikaEFootballMapper.normalize_outcome(d.split("&")[0])
            right = BetikaEFootballMapper.normalize_outcome(d.split("&")[1])
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