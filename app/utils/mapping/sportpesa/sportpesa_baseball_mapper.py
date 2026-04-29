class SportpesaBaseballMapper:
    """Maps SportPesa Baseball JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "match_winner",      # Money Line (2‑way)
        381: "1x2",               # 1X2 (3‑Way Moneyline)
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

        # --- HANDICAPS (Run Lines) ---
        if sp_id == 228:
            return f"asian_handicap_{line_str}"

        # --- TOTALS (OVER/UNDER) ---
        elif sp_id == 229:
            return f"over_under_goals_{line_str}"

        return None