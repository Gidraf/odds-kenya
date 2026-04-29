class SportpesaSoccerMapper:
    """Maps SportPesa Football/Soccer JSON to internal slugs."""
    
    STATIC_MARKETS = {
        1:   "1x2",                       # Full time 3‑way
        10:  "double_chance",             # Double chance
        11:  "draw_no_bet",               # Draw no bet
        8:   "first_team_to_score",       # First team to score
        29:  "btts",                      # Both teams to score
        35:  "btts_and_result",           # BTTS + match result
        45:  "correct_score",             # Full time correct score
        47:  "ht_ft",                     # Half time / Full time
        60:  "first_half_1x2",            # First half 3‑way
        75:  "first_half_btts",           # First half BTTS
        71:  "first_half_correct_score",  # First half correct score
        21:  "exact_goals",               # Exact number of goals (0,1,2,3,…)
        264: "odd_even",                  # Odd/even total goals
        432: "highest_scoring_half",      # Highest scoring half
        105: "goal_groups",               # Goal range groups (0-1, 2-3, 4-5, 6+)
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

        # --- FULL MATCH TOTAL GOALS (Over/Under) ---
        if sp_id == 18:
            return f"over_under_goals_{line_str}"

        # --- HOME TEAM TOTAL GOALS ---
        if sp_id == 19:
            return f"home_goals_{line_str}"

        # --- AWAY TEAM TOTAL GOALS ---
        if sp_id == 20:
            return f"away_goals_{line_str}"

        # --- ASIAN HANDICAP (2‑way) ---
        if sp_id == 16:
            return f"asian_handicap_{line_str}"

        # --- RESULT + TOTAL GOALS (1X2 + Over/Under) ---
        if sp_id == 37:
            return f"result_and_over_under_{line_str}"

        # --- FIRST HALF TOTAL GOALS (Over/Under) ---
        if sp_id == 68:
            return f"first_half_over_under_{line_str}"

        # --- FIRST HALF ASIAN HANDICAP ---
        if sp_id == 66:
            return f"first_half_asian_handicap_{line_str}"

        # --- EUROPEAN HANDICAP (3‑way) ---
        if sp_id == 14:
            # JSON shows: european_handicap_minus_3, european_handicap_plus_1
            if spec_value < 0:
                return f"european_handicap_minus_{abs(int(spec_value))}"
            else:
                return f"european_handicap_plus_{int(spec_value)}"

        return None