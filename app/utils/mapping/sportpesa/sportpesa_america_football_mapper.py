class SportpesaAmericanFootballMapper:
    """Maps SportPesa American Football JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "match_winner",                # Moneyline (2‑way)
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

        # --- POINT SPREAD (Handicap) ---
        if sp_id == 228:
            return f"nfl_point_spread_{line_str}"

        # --- TOTAL POINTS (Over/Under) ---
        if sp_id == 229:
            return f"nfl_total_points_{line_str}"

        return None