import logging

logger = logging.getLogger(__name__)

class SportpesaCricketMapper:
    """Maps SportPesa Cricket JSON to internal canonical slugs."""
    
    # SportPesa (Sportradar) static market IDs for Cricket
    STATIC_MARKETS = {
        1:   "cricket_1x2",                    # 3-Way Match Result (Usually for Test Matches)
        10:  "cricket_double_chance",          # Double Chance
        11:  "cricket_draw_no_bet",            # Draw No Bet
        340: "cricket_winner_incl_super_over", # Winner (Incl. Super Over)
        342: "cricket_will_there_be_a_tie",    # Will There Be A Tie
        382: "cricket_moneyline",              # Standard 2-Way Winner
    }

    @staticmethod
    def format_line(spec_value: float) -> str:
        """Formats float specifiers into canonical lines (e.g., 22.5 -> '22_5')."""
        if spec_value == 0:
            return "0_0"
        val_str = f"{spec_value:g}".replace(".", "_")
        return val_str.replace("-", "minus_") if spec_value < 0 else val_str

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float = 0.0) -> str | None:
        """
        Routes the SportPesa market ID and its specifier to your standard internal slug.
        """
        # 1. Check if it's a known static market (no line appended)
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]

        # 2. Format the line for dynamic markets
        line_str = cls.format_line(spec_value)

        # 3. Dynamic Markets Routing
        
        # Total Match Runs (Over/Under) - SP usually uses 18, 52, or 229
        if sp_id in [18, 52, 229]:
            return f"over_under_cricket_runs_{line_str}"
            
        # Asian Handicap / Run Line
        elif sp_id in [16, 228]:
            if spec_value == 0:
                return "cricket_ah_0_0"
            prefix = "plus" if spec_value > 0 else "minus"
            val = str(abs(spec_value)).replace(".", "_")
            return f"cricket_ah_{prefix}_{val}"
            
        # Team Total Runs (Over/Under)
        elif sp_id == 19:
            return f"cricket_home_total_runs_{line_str}"
        elif sp_id == 20:
            return f"cricket_away_total_runs_{line_str}"

        # 1st Innings - Total at 1st Dismissal (Matches Betika's ID schema)
        elif sp_id == 875:
            return f"cricket_home_total_at_1st_dismissal_{line_str}"
        elif sp_id == 876:
            return f"cricket_away_total_at_1st_dismissal_{line_str}"

        # Unmapped/Unknown Market
        logger.debug(f"Unmapped SportPesa Cricket Market ID: {sp_id} with line {spec_value}")
        return None