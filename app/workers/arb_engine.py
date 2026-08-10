"""
app/workers/arb_engine.py
==========================
Correct arbitrage detection + bookmaker-pair grouping/sorting.

3-way markets (1X2):  ALL three legs covered simultaneously
2-way markets (O/U):  Both legs covered
Guaranteed no-loss stake allocation for every opportunity.

New in this version:
  - ArbOpportunity.bk_pair_key / bk_pair_label
  - sort_arbs_by_bk_pair(arb_list) → {pair_key: [arbs]}
  - arb_pair_label(key)            → "SportPesa / Betika"
  - arb_summary(grouped)           → summary dict for API response
  - verify_arb(arb, total_stake)   → proves guaranteed profit
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# ── Market classification ──────────────────────────────────────────────────────

_THREE_WAY = frozenset({
    "1x2", "match_winner", "moneyline", "3way",
    "first_half_1x2", "second_half_1x2", "half_time", "ht_1x2",
})

_TWO_WAY = frozenset({
    "btts", "both_teams_to_score", "odd_even",
    "draw_no_bet", "dnb",
})

_OU_PREFIX     = "over_under_"
_DOUBLE_CHANCE = frozenset({"1X", "X2", "12"})

_AMBIGUOUS_OUTCOMES = frozenset({
    "", "none", "no goal", "no_goal", "null", "unknown", "other", "othr",
})

_EXCLUDED_MARKET_TOKENS = (
    "correct_score",
    "scorecast",
    "goalscorer",
    "winning_margin",
    "multigoals",
    "exact_goals",
    "result_and_",
    "double_chance_and_",
)

_BK_LABELS: dict[str, str] = {
    "sp": "SportPesa", "bt": "Betika", "od": "OdiBets",
    "1xbet": "1xBet", "22bet": "22Bet", "betwinner": "Betwinner",
    "melbet": "Melbet", "megapari": "Megapari",
    "helabet": "Helabet", "paripesa": "Paripesa",
}

# Sort order for pair keys — local BKs first, then B2B
_BK_ORDER = {"sp": 0, "bt": 1, "od": 2}


def _market_type(slug: str) -> str:
    s = slug.lower()
    if s in _THREE_WAY:                               return "3way"
    if s in _TWO_WAY:                                 return "2way"
    if _is_over_under_market(s) and "asian" not in s: return "2way"
    return "unknown"


def _is_over_under_market(slug: str) -> bool:
    s = slug.lower()
    return (
        s.startswith(_OU_PREFIX)
        or "over_under" in s
        or s.startswith("total_goals")
        or s.startswith("total_points")
        or s.startswith("total_runs")
    )


def _split_market_spec(market_slug: str) -> tuple[str, str]:
    """
    Market keys can carry a spec suffix from flatten/merge, e.g.:
      over_under_goals__spec__2_5
    Returns (base_slug, spec_text) -> ('over_under_goals', '2.5').
    """
    raw = str(market_slug or "")
    marker = "__spec__"
    if marker not in raw:
        return raw, ""
    base, suffix = raw.split(marker, 1)
    spec = suffix.replace("m", "-").replace("p", "+").replace("_", ".")
    return base, spec


def _market_display_label(market_slug: str) -> str:
    base, spec = _split_market_spec(market_slug)
    if spec:
        return f"{base} ({spec})"
    return base


def _canon_outcome_key(value: str) -> str:
    return str(value or "").strip().lower()


def _is_ambiguous_outcome(value: str) -> bool:
    return _canon_outcome_key(value) in _AMBIGUOUS_OUTCOMES


def _market_allowed(slug: str) -> bool:
    s = slug.lower()
    if any(tok in s for tok in _EXCLUDED_MARKET_TOKENS):
        return False
    return _market_type(s) != "unknown"


def _valid_outcomes_for_market(market_slug: str, outcomes: dict[str, dict]) -> dict[str, dict]:
    """
    Keep only canonical outcomes required for low-ambiguity arbitrage markets.
    """
    slug = market_slug.lower()
    mtype = _market_type(slug)
    if mtype == "unknown":
        return {}

    if mtype == "3way":
        wanted = {"1", "x", "2"}
    elif _is_over_under_market(slug):
        wanted = {"over", "under"}
    elif slug in {"btts", "both_teams_to_score"}:
        wanted = {"yes", "no"}
    elif slug == "odd_even":
        wanted = {"odd", "even"}
    elif slug in {"draw_no_bet", "dnb"}:
        wanted = {"1", "2"}
    else:
        wanted = set()

    if not wanted:
        return {}

    filtered: dict[str, dict] = {}
    for out, data in outcomes.items():
        if _canon_outcome_key(out) in wanted:
            filtered[out] = data
    return filtered


def bk_label(slug: str) -> str:
    return _BK_LABELS.get(slug, slug.upper())


def arb_pair_label(pair_key: str) -> str:
    """'sp_bt' → 'SportPesa / Betika', 'sp_bt_od' → 'SportPesa / Betika / OdiBets'"""
    return " / ".join(bk_label(p) for p in pair_key.split("_"))


def _make_pair_key(bks: set[str]) -> str:
    return "_".join(sorted(bks, key=lambda b: (_BK_ORDER.get(b, 99), b)))


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ArbLeg:
    outcome:    str
    bk:         str
    odd:        float
    stake_pct:  float
    stake_kes:  float
    return_kes: float

    def to_dict(self) -> dict:
        return {
            "outcome":    self.outcome,
            "bk":         self.bk,
            "bk_label":   bk_label(self.bk),
            "odd":        round(self.odd, 3),
            "stake_pct":  round(self.stake_pct, 4),
            "stake_kes":  round(self.stake_kes, 2),
            "return_kes": round(self.return_kes, 2),
        }


@dataclass
class ArbOpportunity:
    market:            str
    market_base:       str
    market_spec:       str
    market_display:    str
    n_legs:            int
    arb_sum:           float
    profit_pct:        float
    guaranteed_return: float   # for 1000 KES total stake
    legs:              list[ArbLeg]
    bks_used:          list[str]
    bk_pair_key:       str
    bk_pair_label:     str

    def to_dict(self) -> dict:
        return {
            "market":            self.market,
            "market_slug":       self.market,
            "market_label":      self.market_display,
            "market_base":       self.market_base,
            "market_spec":       self.market_spec,
            "market_display":    self.market_display,
            "n_legs":            self.n_legs,
            "arb_sum":           round(self.arb_sum, 6),
            "profit_pct":        round(self.profit_pct, 4),
            "guaranteed_return": round(self.guaranteed_return, 2),
            "legs":              [l.to_dict() for l in self.legs],
            "bks_used":          self.bks_used,
            "bk_pair_key":       self.bk_pair_key,
            "bk_pair_label":     self.bk_pair_label,
            # backward-compatible fields for existing SSE consumers
            "combo":             " + ".join(l.outcome for l in self.legs),
            "n_bks":             len(self.bks_used),
            "breakdown_1000":    [l.to_dict() for l in self.legs],
            "explanation": (
                f"Place {self.n_legs} bets on ALL outcomes of '{self.market_display}' "
                f"using {self.bk_pair_label}. "
                f"Profit: {round(self.profit_pct, 2)}% guaranteed."
            ),
        }


# ── Core detector ─────────────────────────────────────────────────────────────

def detect_all_arbs(
    best: dict[str, dict[str, dict]],
    min_profit_pct: float = 0.1,
) -> list[ArbOpportunity]:
    """
    Detect all genuine arbitrage opportunities across all markets.

    `best` format:
        {"1x2": {"1": {"odd": 2.1, "bk": "sp"}, "X": {...}, "2": {...}}, ...}
    """
    results: list[ArbOpportunity] = []

    for market_slug, outcomes in best.items():
        if not outcomes or not _market_allowed(market_slug):
            continue

        # Keep only deterministic outcomes and skip ambiguous labels.
        clean = {
            out: data for out, data in outcomes.items()
            if out not in _DOUBLE_CHANCE
            and isinstance(data, dict)
            and not _is_ambiguous_outcome(out)
            and data.get("odd", 0) > 1.0
        }
        clean = _valid_outcomes_for_market(market_slug, clean)

        if len(clean) < 2:
            continue

        mtype = _market_type(market_slug)

        if mtype == "3way":
            # Strict rule: 1X2/3-way arbs are valid only when ALL three
            # outcomes are present (1, X, 2). Never compute 2-leg pseudo-arbs.
            arb = _check_legs(market_slug, clean, min_profit_pct, require_n=3)
            if arb:
                results.append(arb)
        elif mtype == "2way":
            arb = _check_legs(market_slug, clean, min_profit_pct, require_n=2)
            if arb:
                results.append(arb)
        else:
            continue

    results.sort(key=lambda a: -a.profit_pct)
    return results


def _check_legs(
    market_slug: str,
    outcomes:    dict[str, dict],
    min_profit:  float,
    require_n:   int,
) -> ArbOpportunity | None:
    if len(outcomes) < require_n:
        return None

    use = dict(list(outcomes.items())[:require_n])

    legs_raw = []
    for out, data in use.items():
        odd = float(data.get("odd", 0) if isinstance(data, dict) else data)
        bk  = data.get("bk", "") if isinstance(data, dict) else ""
        if odd <= 1.0 or not bk:
            return None
        legs_raw.append((out, bk, odd))

    bks = {bk for _, bk, _ in legs_raw}
    if len(bks) < 2:
        return None

    arb_sum = sum(1.0 / odd for _, _, odd in legs_raw)
    if arb_sum >= 1.0:
        return None

    profit_pct = (1.0 / arb_sum - 1.0) * 100.0
    if profit_pct < min_profit:
        return None

    guaranteed = 1000.0 / arb_sum
    pair_key   = _make_pair_key(bks)
    market_base, market_spec = _split_market_spec(market_slug)
    market_display = _market_display_label(market_slug)

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
        market_base       = market_base,
        market_spec       = market_spec,
        market_display    = market_display,
        n_legs            = len(legs),
        arb_sum           = round(arb_sum, 6),
        profit_pct        = round(profit_pct, 4),
        guaranteed_return = round(guaranteed, 2),
        legs              = legs,
        bks_used          = sorted(bks),
        bk_pair_key       = pair_key,
        bk_pair_label     = arb_pair_label(pair_key),
    )


# ── Bookmaker-pair grouping ───────────────────────────────────────────────────

_PAIR_PRIORITY = {"sp_bt": 0, "sp_od": 1, "bt_od": 2, "sp_bt_od": 3}


def sort_arbs_by_bk_pair(
    arb_opps: list[dict | ArbOpportunity],
) -> dict[str, list[dict]]:
    """
    Group arb opportunities by bookmaker pair, sorted by profit within each group.

    Returns:
      {
        "sp_bt":    [{"market":..., "profit_pct":..., "bk_pair_label": "SportPesa / Betika", ...}],
        "sp_od":    [...],
        "bt_od":    [...],
        "sp_bt_od": [...],   # all 3 local BKs
        "1xbet_sp": [...],   # B2B combinations
      }
    """
    groups: dict[str, list] = {}

    for arb in (arb_opps or []):
        if isinstance(arb, ArbOpportunity):
            pair_key = arb.bk_pair_key
            arb_dict = arb.to_dict()
        else:
            bks_used = set(arb.get("bks_used") or [
                leg["bk"] for leg in arb.get("legs", [])
            ])
            pair_key = _make_pair_key(bks_used)
            arb_dict = {
                **arb,
                "bk_pair_key":   pair_key,
                "bk_pair_label": arb_pair_label(pair_key),
            }
        groups.setdefault(pair_key, []).append(arb_dict)

    for key in groups:
        groups[key].sort(key=lambda a: -(a.get("profit_pct", 0) or 0))

    return dict(sorted(
        groups.items(),
        key=lambda kv: (_PAIR_PRIORITY.get(kv[0], 99), kv[0])
    ))


def arb_summary(arb_by_pair: dict[str, list]) -> dict:
    """Build a concise summary for the API response."""
    total = sum(len(v) for v in arb_by_pair.values())
    best  = max(
        (a.get("profit_pct", 0) or 0 for v in arb_by_pair.values() for a in v),
        default=0.0,
    )
    return {
        "total_arbs": total,
        "best_pct":   round(best, 3),
        "by_pair": {
            pair: {
                "count":    len(arbs),
                "best_pct": round(max((a.get("profit_pct", 0) or 0) for a in arbs), 3),
                "label":    arb_pair_label(pair),
            }
            for pair, arbs in arb_by_pair.items()
        },
    }


# ── Drop-in replacements for existing code ────────────────────────────────────

def detect_arb_for_stream(best: dict) -> tuple[bool, float, list]:
    """Drop-in for odds_stream._detect_arb."""
    arbs = detect_all_arbs(best, min_profit_pct=0.1)
    if not arbs:
        return False, 0.0, []
    return True, arbs[0].profit_pct, [a.to_dict() for a in arbs]


def compute_arb_combined(best: dict, min_profit_pct: float = 0.05) -> list:
    """Drop-in for combined_merger.compute_arb."""
    try:
        from app.workers.combined_merger import ArbResult, ArbLeg as CMArbLeg
        arbs = detect_all_arbs(best, min_profit_pct=min_profit_pct)
        return [
            ArbResult(
                market_slug = a.market,
                profit_pct  = a.profit_pct,
                arb_sum     = a.arb_sum,
                legs        = [CMArbLeg(outcome=l.outcome, bk=l.bk, odd=l.odd, stake_pct=l.stake_pct) for l in a.legs],
            )
            for a in arbs
        ]
    except ImportError:
        return []


# ── Verification ──────────────────────────────────────────────────────────────

def verify_arb(arb: ArbOpportunity, total_stake: float = 10000.0) -> dict:
    """Prove that every outcome returns a profit. Use in flask shell to test."""
    guaranteed    = total_stake / arb.arb_sum
    total_placed  = 0.0
    rows          = []
    for leg in arb.legs:
        stake = total_stake * (1.0 / leg.odd) / arb.arb_sum
        total_placed += stake
        rows.append({
            "outcome":    leg.outcome,
            "bk":         bk_label(leg.bk),
            "odd":        leg.odd,
            "stake_kes":  round(stake, 2),
            "return_kes": round(stake * leg.odd, 2),
            "profit_kes": round(stake * leg.odd - total_placed, 2),
        })
    return {
        "market":            arb.market,
        "pair":              arb.bk_pair_label,
        "profit_pct":        arb.profit_pct,
        "total_placed":      round(total_placed, 2),
        "guaranteed_return": round(guaranteed, 2),
        "guaranteed_profit": round(guaranteed - total_placed, 2),
        "legs":              rows,
        "all_profitable":    all(r["profit_kes"] > 0 for r in rows),
    }