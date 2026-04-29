class SportpesaMMAMapper:
    """Maps SportPesa MMA JSON to internal slugs."""
    
    STATIC_MARKETS = {
        382: "match_winner",                # Moneyline (2‑way winner)
        1:   "mma_1x2",                     # 1X2 (3‑way, rare)
        15:  "method_of_victory",           # Method of victory (KO, SUB, DEC)
        188: "round_betting",               # Exact round + winner
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
        if sp_id == 18 or sp_id == 229:
            return f"over_under_rounds_{line_str}"

        return None