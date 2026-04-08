class SportpesaTennisMapper:
    """Maps SportPesa Tennis JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "tennis_match_winner",       # 2 Way - Who will win?
        204: "first_set_winner",          # First Set Winner
        231: "second_set_winner",         # 2nd Set Winner
        233: "tennis_correct_score_sets", # Correct Score (per set)
        433: "tennis_s1_and_match_winner",# 1st Set/Match Winner
        45:  "tennis_odd_even_games",     # Odd/Even Number of Games
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

        # --- HANDICAPS ---
        if sp_id == 51:
            return f"tennis_game_handicap_{line_str}" # Full Match Game Handicap
        elif sp_id == 439:
            return f"tennis_set_handicap_{line_str}" # Set Handicap (-1.5 / +1.5)
        elif sp_id == 339:
            return f"tennis_s1_game_handicap_{line_str}" # 1st Set Game Handicap

        # --- TOTAL GAMES (OVER/UNDER) ---
        elif sp_id == 226:
            return f"over_under_tennis_games_{line_str}" # Full Match Games
        elif sp_id == 340:
            return f"over_under_s1_games_{line_str}" # 1st Set Total Games

        # --- PLAYER TOTAL GAMES ---
        elif sp_id == 353:
            return f"p1_over_under_games_{line_str}" # Player 1 Total Games
        elif sp_id == 352:
            return f"p2_over_under_games_{line_str}" # Player 2 Total Games

        return None