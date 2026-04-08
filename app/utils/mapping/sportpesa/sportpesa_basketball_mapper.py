class SportpesaBasketballMapper:
    """Maps SportPesa Basketball JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "basketball_moneyline",       # 2 Way - OT incl.
        42:  "basketball_first_half_1x2",  # 3 Way - First Half
        45:  "basketball_odd_even_pts",    # Odd/Even Points
        224: "highest_scoring_quarter",    # Highest Scoring Quarter
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

        # --- SPREADS / HANDICAPS ---
        if sp_id == 51:
            return f"basketball_spread_{line_str}" # Full Time Spread
        elif sp_id == 53:
            return f"basketball_first_half_spread_{line_str}" # 1st Half Spread
        elif sp_id == 366:
            return f"basketball_q1_spread_{line_str}" # 1st Quarter
        elif sp_id == 367:
            return f"basketball_q2_spread_{line_str}" # 2nd Quarter
        elif sp_id == 368:
            return f"basketball_q3_spread_{line_str}" # 3rd Quarter
        elif sp_id == 369:
            return f"basketball_q4_spread_{line_str}" # 4th Quarter

        # --- TOTALS (OVER/UNDER) ---
        elif sp_id == 52:
            return f"over_under_pts_{line_str}" # Full Time Totals
        elif sp_id == 54:
            return f"first_half_over_under_pts_{line_str}" # 1st Half Totals
        elif sp_id == 362:
            return f"q1_over_under_pts_{line_str}" # 1st Quarter Totals
        elif sp_id == 363:
            return f"q2_over_under_pts_{line_str}" # 2nd Quarter Totals
        elif sp_id == 364:
            return f"q3_over_under_pts_{line_str}" # 3rd Quarter Totals
        elif sp_id == 365:
            return f"q4_over_under_pts_{line_str}" # 4th Quarter Totals

        # --- TEAM TOTALS ---
        elif sp_id == 353:
            return f"home_team_over_under_pts_{line_str}"
        elif sp_id == 352:
            return f"away_team_over_under_pts_{line_str}"

        return None