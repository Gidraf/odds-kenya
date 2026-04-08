class SportpesaEFootballMapper:
    """Maps SportPesa eFootball (Virtuals) JSON to internal slugs."""
    
    STATIC_MARKETS = {
        381: "efootball_1x2",           # 3 Way Match Result
        46:  "efootball_double_chance", # Double Chance
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

        # Total Goals Over/Under (id: 56)
        if sp_id == 56:
            return f"over_under_efootball_goals_{line_str}"
            
        # Asian Handicap (id: 51)
        elif sp_id == 51:
            if spec_value == 0:
                return "efootball_ah_0_0"
            prefix = "plus" if spec_value > 0 else "minus"
            val = str(abs(spec_value)).replace(".", "_")
            return f"efootball_ah_{prefix}_{val}"

        return None