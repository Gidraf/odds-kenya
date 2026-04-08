class SportpesaAmericanFootballMapper:
    """Maps SportPesa American Football (NFL) JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "nfl_moneyline", # Money Line
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
        if sp_id == 229:
            return f"over_under_nfl_pts_{line_str}"
            
        # --- HANDICAPS (Point Spreads) ---
        # Note: Added 228 here proactively since it's the standard SportPesa US sports spread ID
        elif sp_id == 228:
            return f"nfl_spread_{line_str}"

        return None