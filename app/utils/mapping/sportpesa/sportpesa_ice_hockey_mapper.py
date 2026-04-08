class SportpesaIceHockeyMapper:
    """Maps SportPesa Ice Hockey JSON to internal slugs."""
    
    STATIC_MARKETS = {
        10:  "hockey_1x2",                  # 3 Way - Full Time (Regulation)
        378: "hockey_moneyline",            # 2 Way Winner OT-incl.
        2:   "hockey_correct_score",        # Correct Score - Full Time
        210: "hockey_p1_winner",            # 1st period Winner
        227: "hockey_highest_scoring_period"# Highest scoring period
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

        # --- TOTALS (OVER/UNDER) ---
        if sp_id == 60:
            return f"over_under_hockey_goals_{line_str}" # Total Goals (Reg)
        elif sp_id == 377:
            return f"over_under_hockey_goals_ot_{line_str}" # Total Goals (OT incl)
        elif sp_id == 212:
            return f"hockey_p1_over_under_goals_{line_str}" # 1st Period Totals

        # --- HANDICAPS ---
        elif sp_id == 55:
            # Euro handicaps are integers (e.g., -2, 1)
            prefix = "plus" if spec_value > 0 else "minus"
            val = int(abs(spec_value))
            return f"hockey_european_handicap_{prefix}_{val}"

        return None