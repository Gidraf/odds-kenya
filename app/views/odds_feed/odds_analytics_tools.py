"""
app/views/analytics_module.py  (v2 — fixed for odds_model v3 + combined merger)
================================================================================

Fixes from v1
─────────────
• ArbitrageOpportunity: uses actual columns (profit_pct, status, open_at, legs_json)
  instead of non-existent (is_active, max_profit_percentage, started_at, legs relation)
• EVOpportunity: uses actual columns (ev_pct, offered_price, fair_prob, status)
  instead of non-existent (edge_pct, is_active, odds, fair_value_price)
• Sharp money: reads from Redis combined cache (no SharpMoneySignal model needed)
• Arb calculator: uses combined_merger.compute_arb engine
• EV calculator: uses combined_merger.compute_ev formulas
• datetime import fixed (was broken: datetime.now(datetime.timezone.utc))

Endpoint map
────────────
  GET  /api/customer/analytics/arbitrage     — DB arb history + live from Redis
  GET  /api/customer/analytics/ev            — DB EV history + live from Redis
  GET  /api/customer/analytics/steam         — live steam moves from Redis cache
  GET  /api/customer/analytics/match/<id>    — full match detail (odds, events, stats)
  GET  /api/customer/analytics/team/<id>/form — team recent form
  GET  /api/customer/analytics/h2h           — head to head
  POST /api/customer/tools/arb-calculator    — manual arb calculator
  POST /api/customer/tools/ev-calculator     — manual EV calculator
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

from flask import g, request

from app.utils.customer_jwt_helpers import _err, _signed_response
from app.utils.decorators_ import log_event, require_auth, require_tier

from . import bp_odds as bp_customer


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _cache_get(key: str):
    try:
        from app.workers.celery_tasks import cache_get
        return cache_get(key)
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════════════════
# ARBITRAGE — DB history + live from Redis
# ═════════════════════════════════════════════════════════════════════════════

@bp_customer.route("/analytics/arbitrage")
@require_auth
def get_arbitrage():
    """
    Active + recent arbitrage opportunities.

    All tiers see basic arb data. Pro/Premium see full leg breakdown + staking.

    Sources:
      1. Redis combined cache (live/upcoming) — real-time arb detection
      2. PostgreSQL arbitrage_opportunities — historical arb windows

    Query params:
      sport       filter by sport slug
      mode        "live" | "upcoming" | "all" (default: all)
      min_profit  minimum profit % (default: 0.5)
      source      "live" | "db" | "all" (default: all)
      days        history lookback days for DB (default: 7)
      limit       max results (default: 50, max: 100)
    """
    from app.models.odds import (
        ArbitrageOpportunity, OpportunityStatus, OddsQueryHelper,
    )

    tier       = g.user.tier
    sport      = request.args.get("sport")
    mode       = request.args.get("mode", "all")
    source     = request.args.get("source", "all")
    min_profit = float(request.args.get("min_profit", 0.5))
    days       = min(int(request.args.get("days", 7)), 90)
    limit      = min(int(request.args.get("limit", 50)), 100)

    if tier not in ("pro", "premium"):
        limit = min(limit, 20)

    results: list[dict] = []

    # ── Source 1: Redis combined cache (real-time) ────────────────────────
    if source in ("live", "all"):
        sports_to_check = [sport] if sport else [
            "soccer", "basketball", "tennis", "ice-hockey",
            "volleyball", "cricket", "rugby",
        ]
        for s in sports_to_check:
            for m_mode in (["live", "upcoming"] if mode == "all" else [mode]):
                cached = _cache_get(f"combined:{m_mode}:{s}")
                if not cached:
                    continue
                for m in cached.get("matches", []):
                    for arb in (m.get("arbs") or []):
                        profit = arb.get("profit_pct", 0)
                        if profit < min_profit:
                            continue
                        entry = {
                            "source":       "live",
                            "mode":         m_mode,
                            "join_key":     m.get("join_key"),
                            "home_team":    m.get("home_team"),
                            "away_team":    m.get("away_team"),
                            "competition":  m.get("competition"),
                            "start_time":   m.get("start_time"),
                            "is_live":      m.get("is_live"),
                            "market":       arb.get("market_slug"),
                            "profit_pct":   profit,
                            "arb_sum":      arb.get("arb_sum"),
                        }
                        if tier in ("pro", "premium"):
                            entry["legs"]          = arb.get("legs", [])
                            entry["breakdown_1000"] = arb.get("breakdown_1000", [])
                        results.append(entry)

    # ── Source 2: PostgreSQL (historical) ─────────────────────────────────
    if source in ("db", "all"):
        try:
            q = OddsQueryHelper.arb_history(
                sport=sport, min_profit=min_profit, days=days,
            )
            db_arbs = q.order_by(
                ArbitrageOpportunity.profit_pct.desc()
            ).limit(limit).all()

            for arb in db_arbs:
                entry = {
                    "source":          "db",
                    "id":              arb.id,
                    "match_id":        arb.match_id,
                    "home_team":       arb.home_team,
                    "away_team":       arb.away_team,
                    "sport":           arb.sport,
                    "competition":     arb.competition,
                    "match_start":     arb.match_start.isoformat() if arb.match_start else None,
                    "market":          arb.market,
                    "profit_pct":      arb.profit_pct,
                    "peak_profit_pct": arb.peak_profit_pct,
                    "arb_sum":         arb.arb_sum,
                    "status":          arb.status.value if arb.status else None,
                    "open_at":         arb.open_at.isoformat() if arb.open_at else None,
                    "closed_at":       arb.closed_at.isoformat() if arb.closed_at else None,
                    "duration_s":      arb.duration_s,
                }
                if tier in ("pro", "premium"):
                    entry["legs"]             = arb.legs_json or []
                    entry["stake_100_returns"] = arb.stake_100_returns
                    entry["bookmaker_ids"]    = arb.bookmaker_ids or []
                results.append(entry)
        except Exception as exc:
            print("DB query failed for arbitrage history:", exc)
            pass  # DB might be unavailable, live data still works

    # Sort by profit descending
    results.sort(key=lambda x: -(x.get("profit_pct") or 0))
    results = results[:limit]

    log_event("arbitrage_view", {"count": len(results), "tier": tier, "sport": sport})
    return _signed_response({
        "ok":       True,
        "total":    len(results),
        "sport":    sport,
        "mode":     mode,
        "arbitrage": results,
    })


# ═════════════════════════════════════════════════════════════════════════════
# EXPECTED VALUE — DB history + live from Redis
# ═════════════════════════════════════════════════════════════════════════════

@bp_customer.route("/analytics/ev")
@require_tier("pro", "premium")
def get_ev():
    """
    Positive EV opportunities — pro + premium only.

    Query params:
      sport, mode, min_ev, source, days, limit
    """
    from app.models.odds import EVOpportunity, OddsQueryHelper

    sport   = request.args.get("sport")
    mode    = request.args.get("mode", "all")
    source  = request.args.get("source", "all")
    min_ev  = float(request.args.get("min_ev", 2.0))
    days    = min(int(request.args.get("days", 7)), 90)
    limit   = min(int(request.args.get("limit", 50)), 100)

    results: list[dict] = []

    # ── Redis live ────────────────────────────────────────────────────────
    if source in ("live", "all"):
        sports_to_check = [sport] if sport else [
            "soccer", "basketball", "tennis", "ice-hockey",
        ]
        for s in sports_to_check:
            for m_mode in (["live", "upcoming"] if mode == "all" else [mode]):
                cached = _cache_get(f"combined:{m_mode}:{s}")
                if not cached:
                    continue
                for m in cached.get("matches", []):
                    for ev in (m.get("evs") or []):
                        if (ev.get("ev_pct") or 0) < min_ev:
                            continue
                        results.append({
                            "source":      "live",
                            "mode":        m_mode,
                            "join_key":    m.get("join_key"),
                            "home_team":   m.get("home_team"),
                            "away_team":   m.get("away_team"),
                            "competition": m.get("competition"),
                            "start_time":  m.get("start_time"),
                            "is_live":     m.get("is_live"),
                            "market":      ev.get("market_slug"),
                            "outcome":     ev.get("outcome"),
                            "bk":          ev.get("bk"),
                            "odd":         ev.get("odd"),
                            "fair_prob":   ev.get("fair_prob"),
                            "ev_pct":      ev.get("ev_pct"),
                            "kelly":       ev.get("kelly"),
                            "half_kelly":  ev.get("half_kelly"),
                        })

    # ── PostgreSQL historical ─────────────────────────────────────────────
    if source in ("db", "all"):
        try:
            q = OddsQueryHelper.ev_history(
                sport=sport, min_ev=min_ev, days=days,
            )
            db_evs = q.order_by(EVOpportunity.ev_pct.desc()).limit(limit).all()

            for ev in db_evs:
                results.append({
                    "source":          "db",
                    "id":              ev.id,
                    "match_id":        ev.match_id,
                    "home_team":       ev.home_team,
                    "away_team":       ev.away_team,
                    "sport":           ev.sport,
                    "competition":     ev.competition,
                    "match_start":     ev.match_start.isoformat() if ev.match_start else None,
                    "market":          ev.market,
                    "selection":       ev.selection,
                    "bookmaker_id":    ev.bookmaker_id,
                    "bookmaker":       ev.bookmaker,
                    "offered_price":   ev.offered_price,
                    "consensus_price": ev.consensus_price,
                    "fair_prob":       ev.fair_prob,
                    "ev_pct":          ev.ev_pct,
                    "peak_ev_pct":     ev.peak_ev_pct,
                    "kelly_fraction":  ev.kelly_fraction,
                    "half_kelly":      ev.half_kelly,
                    "status":          ev.status.value if ev.status else None,
                    "clv_pct":         ev.clv_pct,
                    "won":             ev.won,
                    "settlement_pct":  ev.settlement_pct,
                })
        except Exception:
            pass

    results.sort(key=lambda x: -(x.get("ev_pct") or 0))
    results = results[:limit]

    log_event("ev_view", {"tier": g.user.tier, "count": len(results)})
    return _signed_response({
        "ok":               True,
        "total":            len(results),
        "ev_opportunities": results,
    })


# ═════════════════════════════════════════════════════════════════════════════
# STEAM / SHARP MONEY — live from Redis only
# ═════════════════════════════════════════════════════════════════════════════

@bp_customer.route("/analytics/steam")
@require_tier("pro", "premium")
def get_steam():
    """
    Sharp money signals — real-time from combined merger cache.
    No DB table needed — steam moves are ephemeral (detected per-cycle).
    """
    sport = request.args.get("sport")
    limit = min(int(request.args.get("limit", 30)), 100)

    results: list[dict] = []
    sports_to_check = [sport] if sport else [
        "soccer", "basketball", "tennis", "ice-hockey",
    ]
    for s in sports_to_check:
        for mode in ("live", "upcoming"):
            cached = _cache_get(f"combined:{mode}:{s}")
            if not cached:
                continue
            for m in cached.get("matches", []):
                for sharp in (m.get("sharp") or []):
                    if sharp.get("direction") != "steam_down":
                        continue
                    results.append({
                        "join_key":    m.get("join_key"),
                        "home_team":   m.get("home_team"),
                        "away_team":   m.get("away_team"),
                        "competition": m.get("competition"),
                        "start_time":  m.get("start_time"),
                        "is_live":     m.get("is_live"),
                        "market":      sharp.get("market_slug"),
                        "outcome":     sharp.get("outcome"),
                        "bk":          sharp.get("bk"),
                        "direction":   sharp.get("direction"),
                        "delta":       sharp.get("delta"),
                        "from_odd":    sharp.get("from_odd"),
                        "to_odd":      sharp.get("to_odd"),
                    })

    results.sort(key=lambda x: -(x.get("delta") or 0))
    results = results[:limit]

    return _signed_response({"ok": True, "total": len(results), "signals": results})


# ═════════════════════════════════════════════════════════════════════════════
# MATCH DETAIL — full match with odds, events, stats, lineups
# ═════════════════════════════════════════════════════════════════════════════

@bp_customer.route("/analytics/match/<int:match_id>")
@require_auth
def get_match_detail(match_id: int):
    """Full match detail from PostgreSQL."""
    from app.utils.entity_resolver import MatchQueryService

    detail = MatchQueryService.get_match_detail(match_id)
    if not detail:
        return _err("Match not found", 404)

    # Restrict odds detail for free tier
    if g.user.tier not in ("pro", "premium"):
        detail.pop("bookmaker_odds", None)

    return _signed_response({"ok": True, "match": detail})


# ═════════════════════════════════════════════════════════════════════════════
# TEAM FORM
# ═════════════════════════════════════════════════════════════════════════════

@bp_customer.route("/analytics/team/<int:team_id>/form")
@require_auth
def get_team_form(team_id: int):
    """Last N results for a team."""
    from app.utils.entity_resolver import MatchQueryService

    limit = min(int(request.args.get("limit", 10)), 30)
    form  = MatchQueryService.get_team_form(team_id, limit=limit)

    # Compute summary
    wins   = sum(1 for m in form if m.get("outcome") == "W")
    draws  = sum(1 for m in form if m.get("outcome") == "D")
    losses = sum(1 for m in form if m.get("outcome") == "L")
    form_str = "".join(m.get("outcome", "?") for m in form[:5])

    return _signed_response({
        "ok":       True,
        "team_id":  team_id,
        "total":    len(form),
        "wins":     wins,
        "draws":    draws,
        "losses":   losses,
        "form":     form_str,
        "matches":  form,
    })


# ═════════════════════════════════════════════════════════════════════════════
# HEAD TO HEAD
# ═════════════════════════════════════════════════════════════════════════════

@bp_customer.route("/analytics/h2h")
@require_auth
def get_h2h():
    """Head-to-head between two teams."""
    from app.utils.entity_resolver import MatchQueryService

    team_a = request.args.get("team_a", type=int)
    team_b = request.args.get("team_b", type=int)
    if not team_a or not team_b:
        return _err("Provide team_a and team_b (integer IDs)")

    limit   = min(int(request.args.get("limit", 10)), 30)
    matches = MatchQueryService.get_h2h(team_a, team_b, limit=limit)

    # Summary stats
    a_wins = sum(1 for m in matches
                 if (m.get("home_team_id") == team_a and m.get("result") == "1") or
                    (m.get("away_team_id") == team_a and m.get("result") == "2"))
    b_wins = sum(1 for m in matches
                 if (m.get("home_team_id") == team_b and m.get("result") == "1") or
                    (m.get("away_team_id") == team_b and m.get("result") == "2"))
    draws = sum(1 for m in matches if m.get("result") == "X")

    return _signed_response({
        "ok":       True,
        "team_a":   team_a,
        "team_b":   team_b,
        "total":    len(matches),
        "a_wins":   a_wins,
        "b_wins":   b_wins,
        "draws":    draws,
        "matches":  matches,
    })


# ═════════════════════════════════════════════════════════════════════════════
# TOOLS — Arb Calculator (uses combined_merger engine)
# ═════════════════════════════════════════════════════════════════════════════

@bp_customer.route("/tools/arb-calculator", methods=["POST"])
def arb_calculator():
    """
    Manual arbitrage calculator.

    POST body:
    {
      "bets": [
        {"odd": 2.10, "label": "Home", "bookmaker": "SportPesa"},
        {"odd": 3.40, "label": "Draw", "bookmaker": "Betika"},
        {"odd": 3.50, "label": "Away", "bookmaker": "OdiBets"}
      ],
      "total_stake": 1000
    }

    Returns:
    {
      "is_arb": true,
      "profit_pct": 2.34,
      "arb_sum": 0.977,
      "total_stake": 1000,
      "guaranteed_return": 1023.40,
      "legs": [
        {"label": "Home", "odd": 2.10, "stake": 465.12, "return": 976.75},
        ...
      ]
    }
    """
    data = request.get_json(force=True) or {}
    bets = data.get("bets", [])
    total_stake = float(data.get("total_stake", 1000))

    if len(bets) < 2:
        return _err("Provide at least 2 bets: [{odd: float, label: str}]")

    # Validate odds
    odds_list = []
    for b in bets:
        try:
            odd = float(b.get("odd", 0))
            if odd <= 1.0:
                return _err(f"All odds must be > 1.0, got {odd}")
            odds_list.append(odd)
        except (TypeError, ValueError):
            return _err(f"Invalid odd value: {b.get('odd')}")

    # Compute arb
    arb_sum = sum(1.0 / o for o in odds_list)
    is_arb  = arb_sum < 1.0
    profit_pct = (1.0 / arb_sum - 1.0) * 100 if is_arb else -(arb_sum - 1.0) * 100
    overround_pct = (arb_sum - 1.0) * 100

    # Optimal stake distribution
    legs = []
    for i, b in enumerate(bets):
        odd = odds_list[i]
        stake_pct = (1.0 / odd / arb_sum) * 100
        stake_amt = round(total_stake * stake_pct / 100, 2)
        ret       = round(stake_amt * odd, 2)
        legs.append({
            "label":     b.get("label", f"Outcome {i+1}"),
            "bookmaker": b.get("bookmaker", ""),
            "odd":       odd,
            "stake_pct": round(stake_pct, 2),
            "stake":     stake_amt,
            "return":    ret,
        })

    guaranteed_return = round(total_stake / arb_sum, 2) if is_arb else None
    guaranteed_profit = round(guaranteed_return - total_stake, 2) if guaranteed_return else None

    result = {
        "is_arb":            is_arb,
        "arb_sum":           round(arb_sum, 6),
        "profit_pct":        round(profit_pct, 4),
        "overround_pct":     round(overround_pct, 4),
        "total_stake":       total_stake,
        "guaranteed_return": guaranteed_return,
        "guaranteed_profit": guaranteed_profit,
        "legs":              legs,
    }

    log_event("arb_calculator", {"bets": len(bets), "is_arb": is_arb})
    return _signed_response({"ok": True, **result})


# ═════════════════════════════════════════════════════════════════════════════
# TOOLS — EV Calculator
# ═════════════════════════════════════════════════════════════════════════════

@bp_customer.route("/tools/ev-calculator", methods=["POST"])
def ev_calculator():
    """
    Manual EV calculator.

    POST body:
    {
      "odds": 2.10,
      "fair_probability": 0.52,   // 52% chance of winning
      "stake": 100
    }

    OR with all outcomes for automatic fair probability:
    {
      "odds": 2.10,
      "all_odds": [2.10, 3.40, 3.50],   // 1X2 market
      "outcome_index": 0,                 // which outcome is ours
      "stake": 100
    }
    """
    data = request.get_json(force=True) or {}
    odds  = float(data.get("odds", 0))
    stake = float(data.get("stake", 100))

    if odds <= 1.0:
        return _err("odds must be > 1.0")

    # Determine fair probability
    if "fair_probability" in data:
        fair_prob = float(data["fair_probability"])
        if not (0 < fair_prob < 1):
            return _err("fair_probability must be between 0 and 1")
    elif "all_odds" in data:
        all_odds = [float(o) for o in data["all_odds"]]
        idx = int(data.get("outcome_index", 0))
        if any(o <= 1.0 for o in all_odds):
            return _err("All odds must be > 1.0")
        if idx >= len(all_odds):
            return _err("outcome_index out of range")
        # Remove overround to get fair probabilities
        inv_sum   = sum(1.0 / o for o in all_odds)
        fair_prob = (1.0 / all_odds[idx]) / inv_sum
    else:
        return _err("Provide either fair_probability or all_odds + outcome_index")

    # EV calculation
    ev_pct = (odds * fair_prob - 1.0) * 100
    implied_prob = 1.0 / odds * 100

    # Kelly criterion
    b = odds - 1.0
    kelly = max(0.0, (b * fair_prob - (1.0 - fair_prob)) / b) if b > 0 else 0.0
    half_kelly = kelly / 2

    kelly_stake      = round(stake * kelly, 2)
    half_kelly_stake = round(stake * half_kelly, 2)

    # Expected return on stake
    ev_per_bet = round(stake * (odds * fair_prob - 1.0), 2)

    result = {
        "odds":             odds,
        "fair_probability": round(fair_prob, 6),
        "implied_prob_pct": round(implied_prob, 2),
        "ev_pct":           round(ev_pct, 4),
        "is_positive_ev":   ev_pct > 0,
        "ev_per_bet":       ev_per_bet,
        "kelly_fraction":   round(kelly, 4),
        "kelly_pct":        round(kelly * 100, 2),
        "half_kelly_pct":   round(half_kelly * 100, 2),
        "kelly_stake":      kelly_stake,
        "half_kelly_stake": half_kelly_stake,
        "stake":            stake,
    }

    log_event("ev_calculator")
    return _signed_response({"ok": True, **result})