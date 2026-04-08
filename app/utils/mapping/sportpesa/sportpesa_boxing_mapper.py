class SportpesaBoxingMapper:
    """Maps SportPesa Boxing JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "boxing_winner", # Who will win?
    }

    @classmethod
    def get_market_slug(cls, sp_id: int, spec_value: float) -> str | None:
        if sp_id in cls.STATIC_MARKETS:
            return cls.STATIC_MARKETS[sp_id]
        
        # Room for Over/Under Rounds if provided later
        return None