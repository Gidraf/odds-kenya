"""
app/workers/arb_engine.py
==========================
Correct arbitrage calculation for 2-way and 3-way markets.

Key principle:
  For a true arb, you place stakes on ALL outcomes of ONE market
  across different bookmakers. The sum of (1/odd) for each outcome
  must be < 1.0. The profit is (1/sum - 1) × 100%.

  Stake per leg = (1/odd) / sum_of_inverses × total_stake

  This guarantees the SAME return regardless of which outcome wins:
    return = stake_i × odd_i = total_stake / sum_of_inverses   ∀ i

2-way markets:  Over/Under, BTTS, Odd/Even, DNB, Asian lines
3-way markets:  1X2, Match Winner, Moneyline, HT/FT (9-way treated as 3×3)

Usage:
    from app.workers.arb_engine import detect_all_arbs, calculate_stakes

    arbs = detect_all_arbs(best_odds_map)
    for arb in arbs:
        stakes = calculate_stakes(arb, total_stake=10000)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations
from typing import Any


# ── Market classification ──────────────────────────────────────────────────────

# Markets that are inherently 3-outcome: use ALL THREE legs together
_THREE_WAY = frozenset({
    "1x2", "match_winner", "moneyline", "3way",
    "first_half_1x2", "second_half_1x2", "half_time",
    "ht_1x2", "first_goal", "last_goal",
})

# Markets that are inherently 2-outcome
_TWO_WAY = frozenset({
    "btts", "both_teams_to_score", "odd_even",
    "draw_no_bet", "dnb", "asian_handicap",
})

# Over/Under prefix
_OU_PREFIX = "over_under_"

# Double chance — these are synthetic 2-leg combinations, NOT separate arb legs
# They must NOT be combined with each other or with 1X2 outcomes
_DOUBLE_CHANCE = frozenset({"1X", "X2", "12"})


def _market_type(slug: str) -> str:
    """Return '2way', '3way', or 'unknown'."""
    s = slug.lower()
    if s in _THREE_WAY:
        return "3way"
    if s in _TWO_WAY:
        return "2way"
    if s.startswith(_OU_PREFIX) and "asian" not in s:
        return "2way"
    return "unknown"


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class ArbLeg:
    outcome:   str
    bk:        str
    odd:       float
    stake_pct: float    # percentage of total stake to place on this leg
    stake_kes: float    # actual stake in KES for a given total
    return_kes: float   # guaranteed return from this leg

    def to_dict(self) -> dict:
        return {
            "outcome":    self.outcome,
            "bk":         self.bk,
            "odd":        round(self.odd, 3),
            "stake_pct":  round(self.stake_pct, 4),
            "stake_kes":  round(self.stake_kes, 2),
            "return_kes": round(self.return_kes, 2),
        }


@dataclass
class ArbOpportunity:
    market:       str
    n_legs:       int
    arb_sum:      float          # sum of 1/odd — must be < 1.0
    profit_pct:   float          # (1/arb_sum - 1) * 100
    guaranteed_return: float     # for a 1000 KES total stake
    legs:         list[ArbLeg]
    bks_used:     list[str]

    def to_dict(self) -> dict:
        return {
            "market":            self.market,
            "n_legs":            self.n_legs,
            "arb_sum":           round(self.arb_sum, 6),
            "profit_pct":        round(self.profit_pct, 3),
            "guaranteed_return": round(self.guaranteed_return, 2),
            "legs":              [l.to_dict() for l in self.legs],
            "bks_used":          self.bks_used,
            "explanation": (
                f"Place {len(self.legs)} bets covering ALL outcomes of '{self.market}'. "
                f"No matter which outcome wins, you profit {round(self.profit_pct, 2)}% "
                f"on your total stake."
            ),
        }


# ── Stake calculator ──────────────────────────────────────────────────────────

def calculate_stakes(arb: ArbOpportunity, total_stake: float = 10000.0) -> list[ArbLeg]:
    """
    Given an arb opportunity, calculate the exact KES amount to place
    on each leg so that the return is identical regardless of outcome.

    Stake_i = total_stake × (1/odd_i) / arb_sum
    Return   = total_stake / arb_sum   (same for all outcomes)
    """
    guaranteed = total_stake / arb.arb_sum
    updated = []
    for leg in arb.legs:
        stake = total_stake * (1.0 / leg.odd) / arb.arb_sum
        updated.append(ArbLeg(
            outcome    = leg.outcome,
            bk         = leg.bk,
            odd        = leg.odd,
            stake_pct  = leg.stake_pct,
            stake_kes  = round(stake, 2),
            return_kes = round(stake * leg.odd, 2),
        ))
    return updated


# ── Core detector ─────────────────────────────────────────────────────────────

def detect_all_arbs(
    best: dict[str, dict[str, dict]],
    min_profit_pct: float = 0.1,
) -> list[ArbOpportunity]:
    """
    Detect arbitrage across all markets in the best-odds map.

    `best` format (as built by odds_stream._build_best or combined_merger.compute_best):
        {
          "1x2":           {"1": {"odd": 2.1, "bk": "sp"}, "X": {"odd": 3.4, "bk": "bt"}, "2": {"odd": 4.0, "bk": "od"}},
          "over_under_2.5": {"Over": {"odd": 1.9, "bk": "sp"}, "Under": {"odd": 2.1, "bk": "bt"}},
          ...
        }

    Returns a list of ArbOpportunity sorted by profit_pct descending.
    """
    results: list[ArbOpportunity] = []

    for market_slug, outcomes in best.items():
        if not outcomes:
            continue

        # Filter out double-chance synthetic outcomes from arb legs
        clean = {
            out: data for out, data in outcomes.items()
            if out not in _DOUBLE_CHANCE
            and isinstance(data, dict)
            and data.get("odd", 0) > 1.0
        }

        if len(clean) < 2:
            continue

        mtype = _market_type(market_slug)

        if mtype == "3way":
            # CORRECT: use all three legs together
            arb = _check_legs(market_slug, clean, min_profit_pct, require_n=3)
            if arb:
                results.append(arb)
            # Also check if any 2-leg sub-combo is arb (rare but possible)
            # e.g. 1 vs X2 across books — but only if we have exactly 2 of the 3
            if len(clean) == 2:
                arb2 = _check_legs(market_slug, clean, min_profit_pct, require_n=2)
                if arb2:
                    results.append(arb2)

        elif mtype == "2way":
            # Exactly 2 outcomes (Over/Under, BTTS etc)
            arb = _check_legs(market_slug, clean, min_profit_pct, require_n=2)
            if arb:
                results.append(arb)

        else:
            # Unknown market type: try 2-leg combos only
            # (avoid false 3-way arbs on player props etc.)
            keys = list(clean.keys())
            for combo in combinations(keys, 2):
                subset = {k: clean[k] for k in combo}
                arb = _check_legs(market_slug, subset, min_profit_pct, require_n=2)
                if arb:
                    results.append(arb)

    results.sort(key=lambda a: -a.profit_pct)
    return results


def _check_legs(
    market_slug: str,
    outcomes:    dict[str, dict],
    min_profit:  float,
    require_n:   int,
) -> ArbOpportunity | None:
    """
    Check if the given outcomes form a valid arb.
    require_n: expected number of legs (2 or 3).
    """
    if len(outcomes) < require_n:
        return None

    # Use only the first require_n outcomes if more are present
    use = dict(list(outcomes.items())[:require_n])

    # Validate all odds
    legs_raw = []
    for out, data in use.items():
        odd = float(data.get("odd", 0) if isinstance(data, dict) else data)
        bk  = data.get("bk", "") if isinstance(data, dict) else ""
        if odd <= 1.0 or not bk:
            return None
        legs_raw.append((out, bk, odd))

    # Must use at least 2 different bookmakers for a real arb
    bks = {bk for _, bk, _ in legs_raw}
    if len(bks) < 2:
        return None

    arb_sum = sum(1.0 / odd for _, _, odd in legs_raw)
    if arb_sum >= 1.0:
        return None

    profit_pct = (1.0 / arb_sum - 1.0) * 100.0
    if profit_pct < min_profit:
        return None

    # Build legs with correct stake allocation
    guaranteed = 1000.0 / arb_sum   # guaranteed return for 1000 KES total
    legs = []
    for out, bk, odd in legs_raw:
        stake_pct = (1.0 / odd) / arb_sum * 100.0
        stake_kes = 1000.0 * (1.0 / odd) / arb_sum
        legs.append(ArbLeg(
            outcome    = out,
            bk         = bk,
            odd        = odd,
            stake_pct  = round(stake_pct, 4),
            stake_kes  = round(stake_kes, 2),
            return_kes = round(stake_kes * odd, 2),
        ))

    return ArbOpportunity(
        market            = market_slug,
        n_legs            = len(legs),
        arb_sum           = round(arb_sum, 6),
        profit_pct        = round(profit_pct, 4),
        guaranteed_return = round(guaranteed, 2),
        legs              = legs,
        bks_used          = sorted(bks),
    )


# ── Replacement for odds_stream._detect_arb ───────────────────────────────────

def detect_arb_for_stream(best: dict) -> tuple[bool, float, list]:
    """
    Drop-in replacement for _detect_arb() in odds_stream.py.

    Returns (has_arb, best_profit_pct, arb_list_as_dicts)
    Compatible with the existing SSE payload shape.
    """
    arbs = detect_all_arbs(best, min_profit_pct=0.1)
    if not arbs:
        return False, 0.0, []

    arb_dicts = []
    for a in arbs:
        arb_dicts.append({
            "market":      a.market,
            "profit_pct":  a.profit_pct,
            "n_legs":      a.n_legs,
            "arb_sum":     a.arb_sum,
            "legs": [
                {
                    "outcome":   l.outcome,
                    "odd":       l.odd,
                    "bk":        l.bk,
                    "stake_pct": l.stake_pct,
                }
                for l in a.legs
            ],
            "bks_used":    a.bks_used,
            "explanation": a.to_dict()["explanation"],
            # Stake breakdown for 1000 KES
            "breakdown_1000": [l.to_dict() for l in a.legs],
        })

    return True, arbs[0].profit_pct, arb_dicts


# ── Replacement for combined_merger.compute_arb ───────────────────────────────

def compute_arb_combined(
    best: dict,
    min_profit_pct: float = 0.05,
) -> list:
    """
    Drop-in replacement for compute_arb() in combined_merger.py.
    Returns ArbResult-compatible dicts.
    """
    from app.workers.combined_merger import ArbResult, ArbLeg as CMArbLeg

    arbs = detect_all_arbs(best, min_profit_pct=min_profit_pct)
    results = []
    for a in arbs:
        legs = [
            CMArbLeg(
                outcome   = l.outcome,
                bk        = l.bk,
                odd       = l.odd,
                stake_pct = l.stake_pct,
            )
            for l in a.legs
        ]
        results.append(ArbResult(
            market_slug = a.market,
            profit_pct  = a.profit_pct,
            arb_sum     = a.arb_sum,
            legs        = legs,
        ))
    return results


# ── Verification helper (use in flask shell to test) ──────────────────────────

def verify_arb(arb: ArbOpportunity, total_stake: float = 10000.0) -> dict:
    """
    Verify that an arb opportunity truly guarantees profit.
    Returns a breakdown showing the return for each winning outcome.
    """
    stakes = calculate_stakes(arb, total_stake)
    total_placed = sum(l.stake_kes for l in stakes)
    returns = {l.outcome: round(l.stake_kes * l.odd, 2) for l in stakes}
    profits = {out: round(ret - total_placed, 2) for out, ret in returns.items()}

    return {
        "market":       arb.market,
        "total_placed": round(total_placed, 2),
        "profit_pct":   arb.profit_pct,
        "by_outcome": {
            out: {
                "stake_kes":   next(l.stake_kes for l in stakes if l.outcome == out),
                "return_kes":  returns[out],
                "profit_kes":  profits[out],
                "guaranteed":  profits[out] > 0,
            }
            for out in returns
        },
        "all_profitable": all(p > 0 for p in profits.values()),
    }