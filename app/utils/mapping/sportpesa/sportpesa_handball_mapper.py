class SportpesaHandballMapper:
    """Maps SportPesa Handball JSON to internal slugs."""
    
    STATIC_MARKETS = {
        1:   "1x2",                       # Full match 3‑way
        60:  "first_half_1x2",            # 1st half 3‑way
        432: "highest_scoring_half",      # Which half has more goals
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

        # --- OVER / UNDER GOALS (full match & half) ---
        # The same ID (18 or 229) is used for all totals, differentiated by line value
        if sp_id == 18 or sp_id == 229:
            return f"over_under_goals_{line_str}"

        return None