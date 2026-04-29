class SportpesaTennisMapper:
    """Maps SportPesa Tennis JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "match_winner",                # Moneyline (2‑way)
        204: "first_set_winner",            # Winner of 1st set
        231: "second_set_winner",           # Winner of 2nd set
        188: "set_betting",                 # Correct set score (e.g., 2:0, 2:1)
        264: "odd_even",                    # Odd/even total games
        433: "first_set_match_winner",      # 1st set winner + match winner combo
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

        # --- GAME HANDICAP (Asian Handicap on games) ---
        if sp_id == 228:
            return f"asian_handicap_{line_str}"

        # --- TOTAL GAMES (Over/Under) ---
        if sp_id == 229:
            return f"over_under_goals_{line_str}"

        # --- PLAYER 1 TOTAL GAMES (Over/Under) ---
        if sp_id == 19:
            return f"player1_games_{line_str}"

        # --- PLAYER 2 TOTAL GAMES (Over/Under) ---
        if sp_id == 20:
            return f"player2_games_{line_str}"

        return None