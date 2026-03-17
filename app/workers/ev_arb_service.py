"""
app/workers/ev_arb_service.py
===============================
Expected Value (EV) calculator, arbitrage detector, and sharp-money signal
engine.  Reads from unified_markets (already merged by OddsAggregator / harvest
pipeline) and writes to:

    ArbitrageOpportunity + ArbitrageLeg + ArbitrageHistory
    EVOpportunity
    SharpMoneySignal

All calculations use the CANONICAL unified market dict that MarketMerger.apply()
produces.  This service is called from Celery tasks AFTER each harvest cycle
completes.

EV Formula
----------
    sharp_prob   = (1 / sharp_odds) / sum(1/odds for all outcomes)  # remove vig
    EV           = (sharp_prob × decimal_odds) - 1
    edge_pct     = EV × 100

Arbitrage
----------
    implied_total = sum(1 / best_odd for each required outcome)
    profit_pct    = (1 - implied_total) × 100

Sharp-money / Steam-move detection
------------------------------------
A steam move is when odds drop ≥ STEAM_DROP_PCT within STEAM_WINDOW_SECONDS.
We compare the current price in unified_markets against BookmakerOddsHistory.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from typing import Any

# ─── Config ───────────────────────────────────────────────────────────────────

# Bookmaker IDs considered "sharp" for EV calculation.
# These are typically Pinnacle-equivalent markets.  Adjust per your DB.
SHARP_BOOKMAKER_NAMES = {"Betika", "Sportpesa", "1xBet"}

# Steam move thresholds
STEAM_DROP_PCT      = 5.0    # % drop in odds that triggers a steam signal
STEAM_WINDOW_SECS   = 300    # 5-minute window for steam detection

# Arbitrage: minimum profit to record
MIN_ARB_PROFIT_PCT  = 0.5

# EV: minimum edge to record
MIN_EV_EDGE_PCT     = 2.0

# Required outcomes per market type for arb calculation
ARB_REQUIRED: dict[str, list[str]] = {
    "1x2":           ["1", "X", "2"],
    "double_chance": ["1X", "X2", "12"],
    "btts":          ["yes", "no"],
    "dnb":           ["1", "2"],
    "2way_ot":       ["1", "2"],
}


# =============================================================================
# EV Calculator
# =============================================================================

class EVCalculator:
    """
    Computes +EV opportunities from a unified market dict.

    Input:  unified_markets (from OddsAggregator) — dict of
            {market_key: {outcome: [{bookie, odd}, ...]}}

    Output: list of EV opportunity dicts ready for DB insert.
    """

    @staticmethod
    def _remove_vig(odds_list: list[float]) -> list[float]:
        """Return fair (no-vig) probabilities for a set of odds."""
        raw_probs = [1.0 / o for o in odds_list if o > 1.0]
        total     = sum(raw_probs)
        if total <= 0:
            return []
        return [p / total for p in raw_probs]

    @staticmethod
    def _sharp_prob(outcome: str, market_outcomes: dict[str, list[dict]],
                    sharp_bookie_names: set[str]) -> float | None:
        """
        Compute the sharp consensus probability for one outcome.
        Uses only odds from sharp bookmakers to remove vig.
        """
        # Collect all outcomes' sharp odds to remove vig properly
        all_fair_probs: dict[str, float] = {}

        # Build {outcome: best_sharp_odd}
        sharp_odds: dict[str, float] = {}
        for out, entries in market_outcomes.items():
            sharp = [e for e in entries if e.get("bookie") in sharp_bookie_names]
            if sharp:
                sharp_odds[out] = max(e["odd"] for e in sharp)

        if len(sharp_odds) < 2:
            return None

        vals  = list(sharp_odds.values())
        outs  = list(sharp_odds.keys())
        fair  = EVCalculator._remove_vig(vals)
        probs = dict(zip(outs, fair))
        return probs.get(outcome)

    @staticmethod
    def compute(unified_markets: dict,
                sharp_bookmaker_names: set[str] | None = None) -> list[dict]:
        """
        Returns list of EV dicts:
        {
            market_key, outcome, bookmaker, odds,
            fair_value_price, no_vig_probability, edge_pct,
            sharp_bookmaker_names_used
        }
        """
        if sharp_bookmaker_names is None:
            sharp_bookmaker_names = SHARP_BOOKMAKER_NAMES

        results: list[dict] = []

        for mkt_key, outcome_map in unified_markets.items():
            for outcome, entries in outcome_map.items():
                if not entries:
                    continue

                # Get sharp consensus probability for this outcome
                fair_prob = EVCalculator._sharp_prob(
                    outcome, outcome_map, sharp_bookmaker_names
                )
                if fair_prob is None or fair_prob <= 0:
                    continue

                fair_price = round(1.0 / fair_prob, 4)

                # Check every bookmaker's odds for +EV
                for entry in entries:
                    bookie = entry.get("bookie", "")
                    odds   = entry.get("odd", 0.0)
                    if odds <= 1.0:
                        continue

                    # EV = (prob × decimal_odds) - 1
                    ev        = (fair_prob * odds) - 1.0
                    edge_pct  = round(ev * 100, 3)

                    if edge_pct >= MIN_EV_EDGE_PCT:
                        results.append({
                            "market_key":              mkt_key,
                            "outcome":                 outcome,
                            "bookmaker":               bookie,
                            "odds":                    odds,
                            "fair_value_price":        fair_price,
                            "no_vig_probability":      round(fair_prob, 6),
                            "edge_pct":                edge_pct,
                            "sharp_bookmaker_names":   list(sharp_bookmaker_names),
                        })

        results.sort(key=lambda x: -x["edge_pct"])
        return results


# =============================================================================
# Arbitrage Detector
# =============================================================================

class ArbDetector:
    """
    Detects arbitrage across a unified market dict.
    Identical logic to ArbCalculator in sbo_fetcher but works on the
    broader unified schema that includes BetB2B bookmakers too.
    """

    @staticmethod
    def compute(unified_markets: dict) -> list[dict]:
        """
        Returns list of arb dicts:
        {
            market_key, profit_pct, implied_prob,
            bets: [{outcome, bookie, odd, stake_pct}]
        }
        """
        results: list[dict] = []

        for mkt_key, outcome_map in unified_markets.items():
            required = ArbDetector._required_outcomes(mkt_key)
            if required is None:
                continue

            # Best odd per required outcome
            best: dict[str, dict] = {}
            for req in required:
                entries = outcome_map.get(req) or outcome_map.get(req.lower()) or []
                valid   = [e for e in entries if e.get("odd", 0) > 1.0]
                if not valid:
                    break
                best[req] = max(valid, key=lambda e: e["odd"])
            else:
                if len(best) < len(required):
                    continue

                total_prob = sum(1.0 / b["odd"] for b in best.values())
                if 0 < total_prob < 1.0:
                    profit_pct = round((1.0 - total_prob) * 100, 3)
                    if profit_pct < MIN_ARB_PROFIT_PCT:
                        continue
                    bets = [
                        {
                            "outcome":   out,
                            "bookie":    b["bookie"],
                            "odd":       b["odd"],
                            "stake_pct": round(((1.0 / b["odd"]) / total_prob) * 100, 2),
                        }
                        for out, b in best.items()
                    ]
                    results.append({
                        "market_key":   mkt_key,
                        "profit_pct":   profit_pct,
                        "implied_prob": round(total_prob * 100, 3),
                        "bets":         bets,
                    })

        results.sort(key=lambda x: -x["profit_pct"])
        return results

    @staticmethod
    def _required_outcomes(mkt_key: str) -> list[str] | None:
        for pattern, reqs in ARB_REQUIRED.items():
            if mkt_key == pattern or mkt_key.startswith(f"{pattern}_"):
                return reqs
        if "over_under" in mkt_key or "total_" in mkt_key:
            return ["over", "under"]
        if "handicap" in mkt_key and "1x2" not in mkt_key:
            return ["1", "2"]
        return None

    @staticmethod
    def calc_stakes(arb: dict, bankroll: float) -> dict:
        """
        Given total bankroll to allocate, compute KES stake per leg.
        Returns arb dict with stake_kes added to each bet.
        """
        for bet in arb.get("bets", []):
            bet["stake_kes"] = round(bankroll * bet["stake_pct"] / 100, 2)
        return arb


# =============================================================================
# Sharp Money / Steam Move Detector
# =============================================================================

class SteamMoveDetector:
    """
    Detects rapid odds drops (steam moves) by comparing current prices
    against recent BookmakerOddsHistory records.

    A steam move signals sharp (professional) money hitting a line.
    """

    @staticmethod
    def detect(match_id: int, unified_markets: dict) -> list[dict]:
        """
        Compare current best_odds against history and return steam signals.
        """
        from app.models.odds_model import BookmakerOddsHistory

        cutoff   = datetime.now(timezone.utc) - timedelta(seconds=STEAM_WINDOW_SECS)
        signals: list[dict] = []

        # Pull recent history for this match
        history_rows = (
            BookmakerOddsHistory.query
            .filter(
                BookmakerOddsHistory.match_id == match_id,
                BookmakerOddsHistory.recorded_at >= cutoff,
            )
            .all()
        )

        # Build: (market, selection, bookmaker_id) → oldest price in window
        old_prices: dict[tuple, float] = {}
        bk_map: dict[tuple, int] = {}
        for h in history_rows:
            key = (h.market, h.selection, h.bookmaker_id)
            if key not in old_prices:
                old_prices[key] = h.new_price
                bk_map[key]     = h.bookmaker_id

        # Compare current odds against oldest price in window
        for mkt_key, outcome_map in unified_markets.items():
            for outcome, entries in outcome_map.items():
                for entry in entries:
                    bookie     = entry.get("bookie", "")
                    curr_odds  = entry.get("odd", 0.0)
                    if curr_odds <= 1.0:
                        continue

                    for (mkt, sel, bk_id), old_odds in old_prices.items():
                        if mkt != mkt_key or sel != outcome:
                            continue
                        if old_odds <= 1.0:
                            continue

                        # Steam = odds dropped significantly
                        drop_pct = ((old_odds - curr_odds) / old_odds) * 100
                        if drop_pct >= STEAM_DROP_PCT:
                            signals.append({
                                "match_id":      match_id,
                                "market_key":    mkt_key,
                                "bookmaker_id":  bk_id,
                                "bookmaker":     bookie,
                                "signal_type":   "STEAM_MOVE",
                                "selection":     outcome,
                                "old_price":     old_odds,
                                "new_price":     curr_odds,
                                "drop_pct":      round(drop_pct, 2),
                            })

                        # Reverse line movement = public money on one side but line moves other way
                        elif drop_pct <= -STEAM_DROP_PCT:
                            signals.append({
                                "match_id":      match_id,
                                "market_key":    mkt_key,
                                "bookmaker_id":  bk_id,
                                "bookmaker":     bookie,
                                "signal_type":   "REVERSE_LINE_MOVEMENT",
                                "selection":     outcome,
                                "old_price":     old_odds,
                                "new_price":     curr_odds,
                                "drift_pct":     round(-drop_pct, 2),
                            })

        return signals


# =============================================================================
# DB Persistence Service
# =============================================================================

class EVArbPersistenceService:
    """
    Writes EV / Arb / Sharp-money results to the DB.
    Called once per harvest cycle (after odds have been updated).
    """

    @staticmethod
    def process_match(match_id: int, unified_markets: dict,
                      bookmaker_id_map: dict[str, int] | None = None) -> dict:
        """
        Full pipeline for one match:
          1. Detect arbitrage → upsert ArbitrageOpportunity
          2. Detect +EV       → upsert EVOpportunity
          3. Detect steam     → insert SharpMoneySignal

        bookmaker_id_map: {"Betika": 3, "Sportpesa": 7, ...}
        """
        from app.extensions import db
        from app.models.odds_model import (
            ArbitrageOpportunity, ArbitrageLeg, ArbitrageHistory,
            EVOpportunity, SharpMoneySignal, MarketDefinition,
        )

        stats = {"arb_new": 0, "arb_updated": 0, "ev": 0, "steam": 0}
        now   = datetime.now(timezone.utc)

        # ── 1. Arbitrage ──────────────────────────────────────────────────────
        arbs = ArbDetector.compute(unified_markets)
        for arb in arbs:
            mkt_def = MarketDefinition.get_or_create(arb["market_key"])

            # Check for existing active arb on this (match, market)
            existing = ArbitrageOpportunity.query.filter_by(
                match_id       = match_id,
                market_def_id  = mkt_def.id,
                is_active      = True,
            ).first()

            if existing:
                # Update history + max profit
                if arb["profit_pct"] > existing.max_profit_percentage:
                    existing.max_profit_percentage = arb["profit_pct"]
                h = ArbitrageHistory(
                    arbitrage_id       = existing.id,
                    profit_percentage  = arb["profit_pct"],
                )
                db.session.add(h)
                stats["arb_updated"] += 1
            else:
                opp = ArbitrageOpportunity(
                    match_id                  = match_id,
                    market_def_id             = mkt_def.id,
                    initial_profit_percentage = arb["profit_pct"],
                    max_profit_percentage     = arb["profit_pct"],
                    is_active                 = True,
                )
                db.session.add(opp)
                db.session.flush()

                # Add legs
                for bet in arb.get("bets", []):
                    bk_id = (bookmaker_id_map or {}).get(bet["bookie"])
                    leg   = ArbitrageLeg(
                        arbitrage_id       = opp.id,
                        odds_id            = None,     # resolve later if odds FK available
                        selection_name     = bet["outcome"],
                        price_at_discovery = bet["odd"],
                        bookmaker_name     = bet["bookie"],
                        bookmaker_id       = bk_id,
                        stake_pct          = bet["stake_pct"],
                    )
                    db.session.add(leg)

                h = ArbitrageHistory(
                    arbitrage_id      = opp.id,
                    profit_percentage = arb["profit_pct"],
                )
                db.session.add(h)
                stats["arb_new"] += 1

        # Mark arbs no longer present as collapsed
        active_arbs = ArbitrageOpportunity.query.filter_by(
            match_id=match_id, is_active=True
        ).all()
        current_mkt_keys = {a["market_key"] for a in arbs}
        for active in active_arbs:
            mkt_name = active.market_definition.name if active.market_definition else ""
            if mkt_name not in current_mkt_keys:
                active.is_active     = False
                active.collapsed_at  = now

        # ── 2. EV Opportunities ───────────────────────────────────────────────
        ev_opps = EVCalculator.compute(unified_markets)
        for ev in ev_opps:
            # Deactivate stale EV records for this (match, market, outcome)
            mkt_def = MarketDefinition.get_or_create(ev["market_key"])
            EVOpportunity.query.filter_by(
                match_id      = match_id,
                market_def_id = mkt_def.id,
                selection     = ev["outcome"],
                is_active     = True,
            ).update({"is_active": False})

            bk_id = (bookmaker_id_map or {}).get(ev["bookmaker"])
            new_ev = EVOpportunity(
                match_id              = match_id,
                market_def_id         = mkt_def.id,
                bookmaker_id          = bk_id,
                bookmaker_name        = ev["bookmaker"],
                selection             = ev["outcome"],
                odds                  = ev["odds"],
                fair_value_price      = ev["fair_value_price"],
                no_vig_probability    = ev["no_vig_probability"],
                edge_pct              = ev["edge_pct"],
                sharp_bookmaker_names = ev["sharp_bookmaker_names"],
                is_active             = True,
            )
            db.session.add(new_ev)
            stats["ev"] += 1

        # ── 3. Steam Moves ────────────────────────────────────────────────────
        steam_signals = SteamMoveDetector.detect(match_id, unified_markets)
        for sig in steam_signals:
            bk_id = (bookmaker_id_map or {}).get(sig["bookmaker"]) or sig.get("bookmaker_id")
            mkt_def = MarketDefinition.get_or_create(sig["market_key"])
            sm = SharpMoneySignal(
                match_id       = match_id,
                market_def_id  = mkt_def.id,
                bookmaker_id   = bk_id,
                signal_type    = sig["signal_type"],
                selection_name = sig["selection"],
                old_price      = sig["old_price"],
                new_price      = sig["new_price"],
            )
            db.session.add(sm)
            stats["steam"] += 1

        db.session.commit()
        return stats

    @staticmethod
    def collapse_stale_arbs(before: datetime | None = None) -> int:
        """Mark arb opportunities as collapsed if the match has started."""
        from app.extensions import db
        from app.models.odds_model import ArbitrageOpportunity, UnifiedMatch

        if before is None:
            before = datetime.now(timezone.utc)

        # Join to unified_matches and find matches that have started
        stale = (
            db.session.query(ArbitrageOpportunity)
            .join(UnifiedMatch, UnifiedMatch.id == ArbitrageOpportunity.match_id)
            .filter(
                ArbitrageOpportunity.is_active == True,
                UnifiedMatch.start_time <= before,
            )
            .all()
        )
        count = 0
        for arb in stale:
            arb.is_active    = False
            arb.collapsed_at = before
            count += 1
        if count:
            db.session.commit()
        return count


# =============================================================================
# Utility: odds calculator for all users
# =============================================================================

def calculate_arbitrage(bets: list[dict]) -> dict:
    """
    Frontend-facing arbitrage calculator.

    Input: [{"odd": 2.10, "label": "Home"}, {"odd": 3.50, "label": "Draw"}, ...]
    Output: {
        "is_arb": bool,
        "profit_pct": float,
        "implied_prob": float,
        "stakes": [{"label": "Home", "odd": 2.10, "stake_pct": ..., "stake_for_100": ...}]
    }
    """
    valid = [b for b in bets if b.get("odd", 0) > 1.0]
    if not valid:
        return {"is_arb": False, "error": "No valid odds provided"}

    total_prob = sum(1.0 / b["odd"] for b in valid)
    is_arb     = total_prob < 1.0
    profit_pct = round((1.0 - total_prob) * 100, 3) if is_arb else 0.0

    stakes = [
        {
            "label":        b.get("label", f"Bet {i+1}"),
            "odd":          b["odd"],
            "stake_pct":    round(((1.0 / b["odd"]) / total_prob) * 100, 2),
            "stake_for_100": round(((1.0 / b["odd"]) / total_prob) * 100, 2),  # for £100 bankroll
            "profit_on_100": round(100 * (1 - total_prob), 2) if is_arb else 0,
        }
        for i, b in enumerate(valid)
    ]

    return {
        "is_arb":       is_arb,
        "profit_pct":   profit_pct,
        "implied_prob": round(total_prob * 100, 3),
        "stakes":       stakes,
    }


def calculate_ev(odds: float, fair_probability: float) -> dict:
    """
    EV calculator for any single bet.
    odds: decimal odds
    fair_probability: 0.0-1.0 (your estimated true probability)
    """
    if odds <= 1.0 or not (0 < fair_probability < 1):
        return {"ev": 0.0, "edge_pct": 0.0, "is_positive": False}

    ev       = (fair_probability * odds) - 1.0
    edge_pct = round(ev * 100, 3)

    # Kelly fraction
    kelly_full  = (odds * fair_probability - 1) / (odds - 1)
    kelly_half  = kelly_full / 2   # safer half-Kelly

    return {
        "ev":            round(ev, 5),
        "edge_pct":      edge_pct,
        "is_positive":   ev > 0,
        "kelly_full":    round(max(kelly_full, 0), 4),
        "kelly_half":    round(max(kelly_half, 0), 4),
        "fair_odds":     round(1.0 / fair_probability, 4),
    }