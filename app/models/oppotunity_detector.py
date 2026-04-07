"""
PATCH for app/models/odds_model.py
====================================
Replace the entire OpportunityDetector class with this version.

ROOT CAUSE of  AttributeError: 'float' object has no attribute 'get'
────────────────────────────────────────────────────────────────────
markets_json is stored in two different shapes depending on the harvest path:

  Shape A (enriched via upsert_bookmaker_price):
    {"1x2": {"null": {"1": {"best_price": 1.85, "best_bookmaker_id": 3,
                             "bookmakers": {"3": 1.85, "7": 1.90}, ...}}}}

  Shape B (written directly by some harvesters as a flat dict):
    {"1x2": {"1": 1.85, "X": 3.20, "2": 4.50}}
    or
    {"1x2": {"null": {"1": 1.85}}}   ← specifier key present but value is float

Both shapes must be handled by find_arbs() and find_ev().
The helper _price_from_sel() normalises either shape to a bare float.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc)


def _price_from_sel(d: Any) -> float:
    """
    Extract a numeric price from a selection value regardless of storage shape.

    Handles:
      • bare float/int                  →  1.85
      • {"best_price": 1.85, ...}       →  1.85  (Shape A enriched)
      • {"price": 1.85}                 →  1.85
      • {"odd": 1.85}                   →  1.85
      • anything else                   →  0.0   (treated as unavailable)
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
    Extract a {bookmaker_id_str: price} map from a selection value.

    Shape A: {"best_price": 1.85, "bookmakers": {"3": 1.85, "7": 1.90}}
    Shape B: bare float — we can't recover per-BK breakdown, return empty.
    """
    if isinstance(d, dict):
        bks = d.get("bookmakers")
        if isinstance(bks, dict):
            result: dict[str, float] = {}
            for bk_id, price in bks.items():
                try:
                    fv = float(price)
                    if fv > 1.0:
                        result[str(bk_id)] = fv
                except (TypeError, ValueError):
                    pass
            return result
    return {}


class OpportunityDetector:
    """
    Stateless detector — called after every odds update.
    Returns lists of dicts ready to insert as Arb / EV rows.

    Handles markets_json in both the enriched Shape A and the flat Shape B.
    """

    def __init__(
        self,
        min_profit_pct: float = 0.5,
        min_ev_pct:     float = 3.0,
    ) -> None:
        self.min_profit_pct = min_profit_pct
        self.min_ev_pct     = min_ev_pct

    # ── Arbitrage detection ───────────────────────────────────────────────────

    def find_arbs(self, match) -> list[dict]:
        """
        Scan match.markets_json for arbitrage opportunities.
        Returns list of kwargs dicts for ArbitrageOpportunity(**kwargs).
        """
        results = []
        markets = match.markets_json or {}

        for market_name, specs in markets.items():
            if not isinstance(specs, dict):
                continue

            # Detect whether specs is {spec_key: {sel: val}} or {sel: val}
            # by checking if the first value is itself a dict whose values
            # look like prices or selection dicts.
            first_val = next(iter(specs.values()), None)
            if isinstance(first_val, dict) and not any(
                isinstance(v, (int, float)) for v in first_val.values()
            ):
                # Shape A: {"null": {"1": {...}, "X": {...}}}
                spec_items = specs.items()
            else:
                # Shape B: {"1": 1.85, "X": 3.20} — wrap in a fake spec
                spec_items = [("null", specs)]

            for spec_key, selections in spec_items:
                if not isinstance(selections, dict):
                    continue
                if len(selections) < 2:
                    continue

                legs: list[dict] = []
                for sel_name, sel_data in selections.items():
                    best_p = _price_from_sel(sel_data)
                    if best_p <= 1.0:
                        continue
                    # best_bookmaker_id — Shape A only; fall back to None
                    if isinstance(sel_data, dict):
                        best_bk = sel_data.get("best_bookmaker_id")
                    else:
                        best_bk = None
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
                    leg["stake_pct"] = round(
                        (1.0 / leg["price"]) / arb_sum * 100, 2
                    )

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
                    bookmaker_ids     = sorted({l["bookmaker_id"] for l in legs if l["bookmaker_id"] is not None}),
                    status            = "OPEN",
                    open_at           = _utcnow_naive(),
                ))

        return results

    # ── EV detection ──────────────────────────────────────────────────────────

    def find_ev(self, match) -> list[dict]:
        """
        Scan match.markets_json for positive-EV selections.
        Returns list of kwargs dicts for EVOpportunity(**kwargs).
        """
        results = []
        markets = match.markets_json or {}

        for market_name, specs in markets.items():
            if not isinstance(specs, dict):
                continue

            # Same shape detection as find_arbs
            first_val = next(iter(specs.values()), None)
            if isinstance(first_val, dict) and not any(
                isinstance(v, (int, float)) for v in first_val.values()
            ):
                spec_items = specs.items()
            else:
                spec_items = [("null", specs)]

            for spec_key, selections in spec_items:
                if not isinstance(selections, dict):
                    continue
                if len(selections) < 2:
                    continue

                # Build best price per selection (handles both shapes)
                best_prices: dict[str, float] = {}
                for sel, d in selections.items():
                    p = _price_from_sel(d)
                    if p > 1.0:
                        best_prices[sel] = p

                if len(best_prices) < 2:
                    continue

                # Remove over-round → fair probabilities
                inv_sum = sum(1.0 / p for p in best_prices.values())
                if inv_sum <= 0:
                    continue
                fair_probs = {sel: (1.0 / p) / inv_sum for sel, p in best_prices.items()}
                specifier  = None if spec_key == "null" else spec_key

                # Check each bookmaker's per-selection price
                for sel_name, sel_data in selections.items():
                    fair_p = fair_probs.get(sel_name)
                    if not fair_p:
                        continue

                    consensus_p = best_prices.get(sel_name, 0)
                    bk_prices   = _bk_prices_from_sel(sel_data)

                    # If no per-BK breakdown (Shape B), use the bare price
                    # and attribute it to bookmaker_id = None
                    if not bk_prices:
                        flat_p = _price_from_sel(sel_data)
                        if flat_p > 1.0:
                            bk_prices = {"0": flat_p}

                    for bk_id_str, bk_price in bk_prices.items():
                        ev_pct = (bk_price * fair_p - 1.0) * 100
                        if ev_pct < self.min_ev_pct:
                            continue

                        bk_id = int(bk_id_str) if bk_id_str.isdigit() else 0
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
                            bookmaker_id            = bk_id,
                            bookmaker               = str(bk_id) if bk_id else "unknown",
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