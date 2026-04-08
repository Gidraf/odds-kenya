class SportpesaCricketMapper:
    """Maps SportPesa Cricket JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "cricket_winner", # 2 Way - incl.Super Over
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

        # In case they add runs totals later (usually ID 52 or 229 on SportPesa)
        line_str = cls.format_line(spec_value)
        if sp_id in [52, 229]:
            return f"over_under_cricket_runs_{line_str}"

        return None