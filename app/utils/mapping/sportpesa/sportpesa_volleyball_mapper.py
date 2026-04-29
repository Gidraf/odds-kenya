class SportpesaVolleyballMapper:
    """Maps SportPesa Volleyball JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "match_winner",                # Moneyline (2‑way)
        337: "volleyball_set_betting",      # Correct set score (3:0, 3:1, etc.)
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

        # --- TOTAL POINTS (Over/Under) ---
        if sp_id == 229 or sp_id == 18:
            return f"over_under_goals_{line_str}"

        return None