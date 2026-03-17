import datetime
from enum import Enum


class SubscriptionTier(str, Enum):
    FREE    = "free"
    BASIC   = "basic"      # KES 150/day
    PRO     = "pro"        # KES 3 000/month
    PREMIUM = "premium"    # KES 5 000/month
 
 
class SubscriptionStatus(str, Enum):
    TRIAL    = "trial"
    ACTIVE   = "active"
    EXPIRED  = "expired"
    CANCELLED= "cancelled"
    PENDING  = "pending"   # awaiting first payment
 
 
class BankrollStrategy(str, Enum):
    KELLY      = "kelly"       # Kelly Criterion
    FLAT       = "flat"        # Fixed stake per bet
    MARTINGALE = "martingale"  # Double on loss (capped)
    FIBONACCI  = "fibonacci"   # Fibonacci sequence on loss
 
 
# ─── Pricing config ──────────────────────────────────────────────────────────
 
TIER_PRICES: dict[str, dict] = {
    "basic":   {"amount_kes": 150,  "period_days": 1,  "trial_days": 3},
    "pro":     {"amount_kes": 3000, "period_days": 30, "trial_days": 3},
    "premium": {"amount_kes": 5000, "period_days": 30, "trial_days": 3},
}
 
TIER_LIMITS: dict[str, dict] = {
    "free": {
        "max_matches":       100,
        "days_ahead":        0,          # today only
        "can_see_finished":  False,
        "notifications":     False,
        "bankroll":          False,
        "api_access":        False,
        "export":            False,
        "arb_calculator":    True,
        "odds_calculator":   True,
    },
    "basic": {
        "max_matches":       None,       # unlimited
        "days_ahead":        0,
        "can_see_finished":  True,
        "notifications":     False,
        "bankroll":          False,
        "api_access":        False,
        "export":            False,
        "arb_calculator":    True,
        "odds_calculator":   True,
    },
    "pro": {
        "max_matches":       None,
        "days_ahead":        30,
        "can_see_finished":  True,
        "notifications":     True,
        "bankroll":          False,
        "api_access":        False,
        "export":            False,
        "arb_calculator":    True,
        "odds_calculator":   True,
    },
    "premium": {
        "max_matches":       None,
        "days_ahead":        30,
        "can_see_finished":  True,
        "notifications":     True,
        "bankroll":          True,
        "api_access":        True,
        "export":            True,       # PDF / Word
        "arb_calculator":    True,
        "odds_calculator":   True,
    },
}


def _is_upcoming(match: dict, now: datetime) -> bool:
    st = match.get("start_time")
    if not st:
        return True
    try:
        if isinstance(st, str):
            dt = datetime.fromisoformat(st.replace("Z", "+00:00"))
        else:
            return True
        return dt > now
    except Exception:
        return True