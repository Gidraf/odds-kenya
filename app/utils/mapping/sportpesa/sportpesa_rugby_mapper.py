class SportpesaRugbyMapper:
    """Maps SportPesa Rugby JSON to internal slugs."""
    
    STATIC_MARKETS = {
        10:  "rugby_1x2",                  # 3 Way - Full Time
        42:  "rugby_first_half_1x2",       # 3 Way - First Half
        45:  "rugby_odd_even_pts",         # Odd/Even Point - Full Time
        46:  "rugby_double_chance",        # Double Chance - Full Time
        44:  "rugby_ht_ft",                # Half Time/Full Time
        207: "rugby_highest_scoring_half", # Highest scoring half
        379: "rugby_winning_margin",       # Winning margin
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

        # --- HANDICAPS / SPREADS ---
        if sp_id == 51:
            return f"rugby_spread_{line_str}" # Handicap - Full Time
        elif sp_id == 53:
            return f"rugby_first_half_spread_{line_str}" # Handicap - First Half

        # --- TOTALS (OVER/UNDER) ---
        elif sp_id == 60:
            return f"over_under_rugby_pts_{line_str}" # Total points - Full Time

        return None