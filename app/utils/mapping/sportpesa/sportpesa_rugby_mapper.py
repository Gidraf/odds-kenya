class SportpesaRugbyMapper:
    """Maps SportPesa Rugby Union JSON to internal slugs."""
    
    STATIC_MARKETS = {
        1:   "rugby_result",                # Full match 3‑way
        60:  "first_half_result",           # First half 3‑way
        10:  "double_chance",               # Double chance
        264: "odd_even",                    # Odd/even total points
        432: "highest_scoring_half",        # Highest scoring half
        47:  "rugby_ht_ft",                 # Half time / Full time
        15:  "winning_margin",              # Winning margin (banded)
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

        # --- ASIAN HANDICAP (point spread) ---
        if sp_id == 16:
            return f"asian_handicap_{line_str}"

        # --- TOTAL POINTS (Over/Under) ---
        if sp_id == 18 or sp_id == 229:
            return f"rugby_total_points_{line_str}"

        return None