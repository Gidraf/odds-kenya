class SportpesaHandballMapper:
    """Maps SportPesa Handball JSON to internal slugs."""
    
    STATIC_MARKETS = {
        10: "handball_1x2", # 3 Way Winner
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
        if sp_id == 52:
            return f"over_under_handball_goals_{line_str}" # Full Time Totals
        elif sp_id == 54:
            return f"first_half_over_under_handball_goals_{line_str}" # 1st Half Totals

        return None