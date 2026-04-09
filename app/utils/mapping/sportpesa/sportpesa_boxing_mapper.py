import logging

logger = logging.getLogger(__name__)

class SportpesaBoxingMapper:
    """Maps SportPesa Boxing JSON to internal canonical slugs."""
    
    STATIC_MARKETS = {
        382: "boxing_moneyline", # Who will win? (Usually 2-Way)
        381: "boxing_1x2",       # Fallback just in case they use 3-Way (1X2)
    }

    @staticmethod
    def format_line(spec_value: float) -> str:
        """Formats string specifiers into canonical lines (e.g., 5.5 -> '5_5')."""
        if spec_value == 0:
            return "0_0"
        val_str = f"{spec_value:g}".replace(".", "_")
        return val_str.replace("-", "minus_") if spec_value < 0 else val_str

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        # Handle Static Markets
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]
        
        # Prepare line formatting for dynamic markets (Over/Under Rounds)
        line_str = cls.format_line(spec_value)
        
        # --- DYNAMIC MARKETS (Placeholders for when you get the JSON) ---
        # Example: Total Rounds (Over/Under)
        # if sp_id == ???: 
        #     return f"over_under_boxing_rounds_{line_str}"
            
        return None