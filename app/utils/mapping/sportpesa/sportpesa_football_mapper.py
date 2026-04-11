class SportpesaFootballMapper:
    """
    Maps SportPesa's raw JSON market IDs and specValues to your internal database slugs.
    """
    
    # 1. STATIC MARKETS (No specValue required)
    STATIC_MARKETS = {
        10:  "match_winner",             # 3 Way
        46:  "double_chance",            # Double Chance
        43:  "btts",                     # Both Teams To Score (Base Market)
        47:  "draw_no_bet",              # Draw No Bet
        42:  "first_half_1x2",           # 3 Way - First Half
        328: "first_half_btts",          # BTTS - First Half
        203: "first_half_correct_score", # Correct Score - First Half
        207: "highest_scoring_half",     # Half with most goals
        44:  "ht_ft",                    # Half Time/Full Time
        332: "correct_score",            # Correct Score
        258: "exact_goals",              # Total Goals Exactly
        45:  "odd_even",                 # Odd/Even
        202: "goal_groups",              # Number of Goals in Groups
    }

    @staticmethod
    def format_line(spec_value: float) -> str:
        """Helper to convert 2.5 to '2_5' or -0.25 to 'minus_0_25'."""
        if spec_value == 0:
            return "0_0"
        
        val_str = f"{spec_value:g}".replace(".", "_")
        if spec_value < 0:
            return val_str.replace("-", "minus_")
        return val_str

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        """
        Returns the base internal market slug for a given SportPesa market.
        """
        
        # 1. Static Markets (Simple Lookup)
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]

        # ---------------------------------------------------------
        # 2. DYNAMIC MARKETS (Requires parsing the specValue)
        # ---------------------------------------------------------
        line_str = cls.format_line(spec_value)

        # Total Goals Over/Under - Full Time
        if sp_id == 52:
            return f"over_under_{line_str}"
            
        # Total Goals Over/Under - Half Time
        elif sp_id == 54:
            return f"first_half_over_under_{line_str}"

        # Total Goals Home Team (id: 353)
        elif sp_id == 353:
            return f"home_over_under_{line_str}"

        # Total Goals Away Team (id: 352)
        elif sp_id == 352:
            return f"away_over_under_{line_str}"

        # Asian Handicap - Full Time (id: 51)
        elif sp_id == 51:
            return f"asian_handicap_{line_str}"

        # Asian Handicap - Half Time (id: 53)
        elif sp_id == 53:
            return f"first_half_asian_handicap_{line_str}"

        # Euro Handicap (id: 55)
        elif sp_id == 55:
            prefix = "plus" if spec_value > 0 else "minus"
            val = int(abs(spec_value))
            return f"european_handicap_{prefix}_{val}"

        # Result + Over/Under Combo (id: 208)
        elif sp_id == 208:
            return f"1x2_over_under_{line_str}"
            
        # Result + BTTS Combo (id: 386)
        elif sp_id == 386:
            return "1x2_btts"

        # Unknown Market
        return f"unknown_{sp_id}"

    @classmethod
    def get_exhaustive_selection_slug(cls, sp_id: int, spec_value: float, selection_name: str) -> str | None:
        """
        Use this IF you created separate distinct markets for EVERY single selection 
        (e.g., separating "Over 2.5" and "Under 2.5" into distinct database rows).
        """
        line_str = cls.format_line(spec_value)
        name_upper = selection_name.upper()

        # O/U Full Time mapping
        if sp_id == 52:
            if "OVER" in name_upper:
                return f"over_{line_str}_goals"
            elif "UNDER" in name_upper:
                return f"under_{line_str}_goals"

        # BTTS Mapping
        if sp_id == 43:
            if "YES" in name_upper:
                return "btts_yes"
            elif "NO" in name_upper:
                return "btts_no"

        # Goals Bands (id: 202)
        if sp_id == 202:
            if "0 OR 1" in name_upper or "0-1" in name_upper:
                return "goals_band_0_1"
            if "2 OR 3" in name_upper or "2-3" in name_upper:
                return "goals_band_2_3"
            if "4 OR 5" in name_upper or "4-5" in name_upper:
                return "goals_band_4_5"
            if "6 OR MORE" in name_upper or "6+" in name_upper:
                return "goals_band_6_plus"

        # Fallback to standard base market
        return cls.get_market_slug(sp_id, spec_value)