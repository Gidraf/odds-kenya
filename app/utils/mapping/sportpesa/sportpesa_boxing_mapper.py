class SportpesaBoxingMapper:
    """Maps SportPesa Boxing JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "match_winner",        # Money Line (2‑way winner)
        381: "1x2",                 # 1X2 (3‑way, rarely used in boxing)
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

        # --- TOTAL ROUNDS (Over/Under) ---
        if sp_id == 18:   # typical ID for over/under rounds
            return f"over_under_rounds_{line_str}"

        # --- METHOD OF VICTORY (KO, Decision, etc.) ---
        if sp_id == 15:   # typical ID for method of victory (banded)
            return "method_of_victory"

        # --- ROUND BETTING (exact round + winner) ---
        if sp_id == 188:  # typical ID for round betting
            return "round_betting"

        return None