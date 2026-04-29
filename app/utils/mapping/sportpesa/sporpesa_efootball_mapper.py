class SportpesaEFootballMapper:
    """Maps SportPesa eFootball JSON to internal slugs."""
    
    STATIC_MARKETS = {
        1:   "efootball_1x2",           # 1X2 (3‑way)
        10:  "efootball_double_chance", # Double Chance
        11:  "efootball_draw_no_bet",   # Draw No Bet
        29:  "efootball_btts",          # Both Teams To Score
        47:  "efootball_ht_ft",         # Half Time / Full Time
        60:  "first_half_efootball_1x2",# 1st Half 1X2
    }

    @staticmethod
    def format_line(spec_value: float) -> str:
        if spec_value == 0:
            return "0_0"
        val_str = f"{spec_value:g}".replace(".", "_")
        return val_str.replace("-", "minus_") if spec_value < 0 else val_str

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]

        line_str = cls.format_line(spec_value)

        # --- OVER / UNDER GOALS ---
        if sp_id == 18:
            return f"over_under_efootball_goals_{line_str}"

        # --- ASIAN HANDICAP ---
        if sp_id == 16:
            # spec_value can be positive (e.g., +0.5 => "plus_0_5") or negative
            # The JSON shows "efootball_ah_plus_0_5" which uses "plus" prefix.
            # We'll replicate that.
            if spec_value > 0:
                return f"efootball_ah_plus_{line_str}"
            elif spec_value < 0:
                # Negative handicap: e.g., -0.5 -> "minus_0_5"
                return f"efootball_ah_minus_{line_str[6:]}"  # remove "minus_" prefix from line_str
                # Simpler: return f"efootball_ah_{line_str}"
            else:
                return f"efootball_ah_{line_str}"

        # --- TEAM TOTALS (if needed) ---
        if sp_id == 19:
            return f"efootball_home_team_total_{line_str}"
        if sp_id == 20:
            return f"efootball_away_team_total_{line_str}"

        return None