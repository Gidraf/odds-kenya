"""
PATCH for app/models/odds_model.py — OpportunityDetector v2
==============================================================
Fixes two bugs:
  1. AttributeError: 'float' object has no attribute 'get'
     markets_json can be {mkt: {outcome: float}} not always {mkt: {spec: {outcome: {best_price:...}}}}

  2. IntegrityError: bookmaker_id=0 violates FK constraint on ev_opportunities
     When markets_json has flat Shape B (no per-bookmaker breakdown), we cannot
     attribute an EV to a specific bookmaker, so we skip those rows entirely.
     EV detection only runs when we have Shape A (enriched, with bookmakers:{id: price}).

Replace the OpportunityDetector class in app/models/odds_model.py with this version.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc)


# ─── Shape normalisation helpers ─────────────────────────────────────────────

def _price_from_sel(d: Any) -> float:
    """
    Extract best price from a selection value regardless of storage shape.
      • bare float/int              →  float
      • {"best_price": 1.85, ...}   →  1.85   (Shape A enriched)
      • {"price": 1.85}             →  1.85
      • {"odd": 1.85}               →  1.85
      • anything else               →  0.0
    """
    if isinstance(d, (int, float)):
        fv = float(d)
        return fv if fv > 1.0 else 0.0
    if isinstance(d, dict):
        for key in ("best_price", "price", "odd", "odds"):
            v = d.get(key)
            if v is not None:
                try:
                    fv = float(v)
                    if fv > 1.0:
                        return fv
                except (TypeError, ValueError):
                    pass
    return 0.0


def _bk_prices_from_sel(d: Any) -> dict[str, float]:
    """
    Extract {bookmaker_id_str: price} from Shape A enriched selection.
    Returns {} for Shape B (flat float) — caller must skip EV for those.
    """
    if not isinstance(d, dict):
        return {}
    bks = d.get("bookmakers")
    if not isinstance(bks, dict) or not bks:
        return {}
    result: dict[str, float] = {}
    for bk_id, price in bks.items():
        try:
            bk_int = int(bk_id)
            if bk_int <= 0:          # ← FK fix: skip bookmaker_id = 0 or negative
                continue
            fv = float(price)
            if fv > 1.0:
                result[str(bk_int)] = fv
        except (TypeError, ValueError):
            pass
    return result


def _unwrap_specs(specs: dict) -> list[tuple[str, dict]]:
    """
    Normalise the two possible specs structures to a list of (spec_key, selections).

    Shape A: {"null": {"1": {"best_price":1.85,...}, "X":..., "2":...}}
    Shape B: {"1": 1.85, "X": 3.20, "2": 4.50}   ← flat

    Returns list of (spec_key, {sel: sel_data}) pairs.
    """
    if not specs:
        return []
    first_val = next(iter(specs.values()), None)
    # Shape A: first value is a dict whose values are NOT bare numbers
    if isinstance(first_val, dict) and not any(
        isinstance(v, (int, float)) for v in first_val.values()
    ):
        return list(specs.items())
    # Shape B: wrap in a fake specifier
    return [("null", specs)]


# ─── Main detector ────────────────────────────────────────────────────────────

class OpportunityDetector:
    """
    Stateless detector. Returns lists of kwargs dicts for Arb / EV inserts.
    Called after every odds update.

    Handles markets_json in both enriched Shape A and flat Shape B.
    EV detection is **skipped** when no per-bookmaker breakdown exists, which
    avoids the FK violation caused by inserting bookmaker_id=0.
    """

    def __init__(
        self,
        min_profit_pct: float = 0.5,
        min_ev_pct:     float = 3.0,
    ) -> None:
        self.min_profit_pct = min_profit_pct
        self.min_ev_pct     = min_ev_pct

    # ── Arbitrage ─────────────────────────────────────────────────────────────

    def find_arbs(self, match) -> list[dict]:
        results = []
        markets = match.markets_json or {}

        for market_name, specs in markets.items():
            if not isinstance(specs, dict):
                continue
            for spec_key, selections in _unwrap_specs(specs):
                if not isinstance(selections, dict) or len(selections) < 2:
                    continue

                legs: list[dict] = []
                for sel_name, sel_data in selections.items():
                    best_p = _price_from_sel(sel_data)
                    if best_p <= 1.0:
                        continue
                    best_bk = sel_data.get("best_bookmaker_id") if isinstance(sel_data, dict) else None
                    legs.append({
                        "selection":    sel_name,
                        "bookmaker_id": best_bk,
                        "bookmaker":    str(best_bk) if best_bk else "unknown",
                        "price":        best_p,
                    })

                if len(legs) < 2:
                    continue

                arb_sum = sum(1.0 / leg["price"] for leg in legs)
                if arb_sum >= 1.0:
                    continue

                profit_pct = (1.0 / arb_sum - 1.0) * 100
                if profit_pct < self.min_profit_pct:
                    continue

                for leg in legs:
                    leg["stake_pct"] = round((1.0 / leg["price"]) / arb_sum * 100, 2)

                specifier = None if spec_key == "null" else spec_key
                results.append(dict(
                    match_id          = match.id,
                    home_team         = match.home_team_name,
                    away_team         = match.away_team_name,
                    sport             = match.sport_name,
                    competition       = match.competition_name,
                    match_start       = match.start_time,
                    market            = market_name,
                    specifier         = specifier,
                    profit_pct        = round(profit_pct, 4),
                    peak_profit_pct   = round(profit_pct, 4),
                    arb_sum           = round(arb_sum, 6),
                    legs_json         = legs,
                    stake_100_returns = round(100.0 / arb_sum, 2),
                    bookmaker_ids     = sorted({
                        l["bookmaker_id"] for l in legs if l["bookmaker_id"] is not None
                    }),
                    status            = "OPEN",
                    open_at           = _utcnow_naive(),
                ))

        return results

    # ── Expected Value ────────────────────────────────────────────────────────

    def find_ev(self, match) -> list[dict]:
        """
        Only detects EV when we have a proper per-bookmaker breakdown (Shape A).
        Skips Shape B flat markets entirely to avoid bookmaker_id=0 FK violations.
        """
        results = []
        markets = match.markets_json or {}

        for market_name, specs in markets.items():
            if not isinstance(specs, dict):
                continue
            for spec_key, selections in _unwrap_specs(specs):
                if not isinstance(selections, dict) or len(selections) < 2:
                    continue

                # ── Build best prices ─────────────────────────────────────────
                best_prices: dict[str, float] = {}
                for sel, d in selections.items():
                    p = _price_from_sel(d)
                    if p > 1.0:
                        best_prices[sel] = p

                if len(best_prices) < 2:
                    continue

                # ── Check if we have per-BK data (Shape A check) ──────────────
                # If NONE of the selections have a bookmakers dict, skip this market.
                # Without per-BK data we cannot attribute EV to a real bookmaker_id.
                any_bk_data = any(
                    bool(_bk_prices_from_sel(d))
                    for d in selections.values()
                    if isinstance(d, dict)
                )
                if not any_bk_data:
                    continue   # ← FK fix: skip Shape B markets for EV

                # ── Fair probabilities (over-round removed) ───────────────────
                inv_sum = sum(1.0 / p for p in best_prices.values())
                if inv_sum <= 0:
                    continue
                fair_probs = {sel: (1.0 / p) / inv_sum for sel, p in best_prices.items()}
                specifier  = None if spec_key == "null" else spec_key

                for sel_name, sel_data in selections.items():
                    fair_p = fair_probs.get(sel_name)
                    if not fair_p:
                        continue

                    bk_prices = _bk_prices_from_sel(sel_data)
                    if not bk_prices:
                        continue   # ← FK fix: no valid bookmakers for this selection

                    consensus_p = best_prices.get(sel_name, 0)

                    for bk_id_str, bk_price in bk_prices.items():
                        # _bk_prices_from_sel already filters bk_id <= 0
                        bk_id  = int(bk_id_str)
                        ev_pct = (bk_price * fair_p - 1.0) * 100

                        if ev_pct < self.min_ev_pct:
                            continue

                        b     = bk_price - 1
                        kelly = max(0.0, (b * fair_p - (1 - fair_p)) / b) if b > 0 else 0.0

                        results.append(dict(
                            match_id                = match.id,
                            home_team               = match.home_team_name,
                            away_team               = match.away_team_name,
                            sport                   = match.sport_name,
                            competition             = match.competition_name,
                            match_start             = match.start_time,
                            market                  = market_name,
                            specifier               = specifier,
                            selection               = sel_name,
                            bookmaker_id            = bk_id,          # always > 0 now
                            bookmaker               = str(bk_id),
                            offered_price           = bk_price,
                            consensus_price         = round(consensus_p, 4),
                            fair_prob               = round(fair_p, 6),
                            ev_pct                  = round(ev_pct, 4),
                            peak_ev_pct             = round(ev_pct, 4),
                            bookmakers_in_consensus = len(bk_prices),
                            kelly_fraction          = round(kelly, 4),
                            half_kelly              = round(kelly / 2, 4),
                            status                  = "OPEN",
                            open_at                 = _utcnow_naive(),
                        ))

        return results