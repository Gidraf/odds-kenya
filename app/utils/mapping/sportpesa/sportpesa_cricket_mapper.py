class SportpesaCricketMapper:
    """Maps SportPesa Cricket JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "match_winner",                # Money Line (2‑way)
        340: "cricket_winner_incl_super_over",  # Winner incl. Super Over
        342: "cricket_will_there_be_a_tie",    # Will There Be A Tie
        341: "cricket_1x2",                 # 1X2 (3‑way, for Test matches)
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

        # --- TOTAL MATCH RUNS (Over/Under) ---
        if sp_id == 18 or sp_id == 229:
            return f"total_runs_match_{line_str}"

        # --- TEAM TOTAL RUNS (Innings) ---
        if sp_id == 19:
            return f"home_team_total_runs_{line_str}"
        if sp_id == 20:
            return f"away_team_total_runs_{line_str}"

        # --- TOP BATSMAN / TOP BOWLER ---
        if sp_id == 15:
            return "top_batsman"
        if sp_id == 16:
            return "top_bowler"

        # --- METHOD OF DISMISSAL ---
        if sp_id == 21:
            return "method_of_dismissal"

        return None