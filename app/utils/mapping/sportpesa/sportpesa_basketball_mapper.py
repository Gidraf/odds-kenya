class SportpesaBasketballMapper:
    """Maps SportPesa Basketball JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "match_winner",                # Money Line (2‑way)
        381: "1x2",                         # 1X2 (3‑way, includes draw)
        60:  "first_half_winner",           # 1st half 1X2 (3‑way)
        264: "odd_even",                    # Odd/Even total points
        432: "highest_scoring_quarter",     # Highest scoring quarter
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

        # --- FULL MATCH HANDICAP (Point Spread) ---
        if sp_id == 228:
            return f"asian_handicap_{line_str}"

        # --- FULL MATCH TOTAL POINTS ---
        elif sp_id == 229:
            return f"over_under_goals_{line_str}"

        # --- FIRST HALF HANDICAP ---
        elif sp_id == 66:
            return f"first_half_asian_handicap_{line_str}"

        # --- FIRST HALF TOTAL POINTS ---
        elif sp_id == 68:
            return f"first_half_over_under_{line_str}"

        # --- HOME TEAM TOTAL POINTS ---
        elif sp_id == 19:
            return f"home_total_points_{line_str}"

        # --- AWAY TEAM TOTAL POINTS ---
        elif sp_id == 20:
            return f"away_total_points_{line_str}"

        # --- 1ST QUARTER TOTAL POINTS ---
        elif sp_id == 236:
            return f"q1_total_{line_str}"

        # --- 1ST QUARTER SPREAD ---
        elif sp_id == 230:   # typical ID for quarter spread
            return f"q1_spread_{line_str}"

        # --- 2ND QUARTER SPREAD ---
        elif sp_id == 231:
            return f"q2_spread_{line_str}"

        # --- 3RD QUARTER SPREAD ---
        elif sp_id == 232:
            return f"q3_spread_{line_str}"

        # --- 4TH QUARTER SPREAD ---
        elif sp_id == 233:
            return f"q4_spread_{line_str}"

        # --- WINNING MARGIN (banded) ---
        elif sp_id == 15:
            # spec_value may encode the band, but we return static slug
            return "winning_margin"

        # Fallback – unknown market ID
        return None