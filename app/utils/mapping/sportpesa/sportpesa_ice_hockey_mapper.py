class SportpesaIceHockeyMapper:
    """Maps SportPesa Ice Hockey JSON to internal slugs."""
    
    STATIC_MARKETS = {
        1:   "1x2",                       # Regulation result (3‑way)
        186: "match_winner_ot",           # Moneyline (2‑way, includes OT/shootout)
        199: "correct_score",             # Correct final score (market_2)
        60:  "first_period_winner",       # 1st period 1X2
        432: "highest_scoring_period",    # Which period has most goals
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
        if sp_id == 229 or sp_id == 18:
            return f"over_under_goals_{line_str}"

        return None