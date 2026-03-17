import datetime

from flask import g, request
from pytz import timezone

from app.utils.customer_jwt_helpers import _err, _signed_response
from app.utils.decorators_ import log_event, require_auth, require_tier

from . import bp_odds as bp_customer

@bp_customer.route("/analytics/arbitrage")
@require_auth
def get_arbitrage():
    """Active arbitrage opportunities — all tiers see basic arb, pro+ see full detail."""
    from app.models.odds_model import ArbitrageOpportunity, UnifiedMatch
    tier    = g.user.tier
    min_pct = float(request.args.get("min_profit", 0.5))
    sport   = request.args.get("sport")
    limit   = 50 if tier in ("pro", "premium") else 20
 
    query = ArbitrageOpportunity.query.filter_by(is_active=True)
    if sport:
        query = query.join(UnifiedMatch).filter(
            UnifiedMatch.sport_name.ilike(f"%{sport}%")
        )
    arbs = query.order_by(ArbitrageOpportunity.max_profit_percentage.desc()).limit(limit).all()
 
    results = []
    for arb in arbs:
        match = UnifiedMatch.query.get(arb.match_id)
        entry = {
            "id":              arb.id,
            "profit_pct":      arb.max_profit_percentage,
            "started_at":      arb.started_at.isoformat() if arb.started_at else None,
            "match":           match.to_dict() if match else None,
            "market":          arb.market_definition.name if arb.market_definition else None,
        }
        if tier in ("pro", "premium"):
            entry["legs"] = [l.to_dict() for l in arb.legs]
        results.append(entry)
 
    log_event("arbitrage_view", {"count": len(results), "tier": tier})
    return _signed_response({"ok": True, "total": len(results), "arbitrage": results})
 
 
@bp_customer.route("/analytics/ev")
@require_tier("pro", "premium")
def get_ev():
    """Expected Value opportunities — pro + premium only."""
    from app.models.odds_model import EVOpportunity, UnifiedMatch
 
    min_edge = float(request.args.get("min_edge", 2.0))
    evs = EVOpportunity.query.filter(
        EVOpportunity.is_active  == True,
        EVOpportunity.edge_pct   >= min_edge,
    ).order_by(EVOpportunity.edge_pct.desc()).limit(50).all()
 
    results = []
    for ev in evs:
        match = UnifiedMatch.query.get(ev.match_id) if ev.match_id else None
        results.append({
            "id":               ev.id,
            "match":            match.to_dict() if match else None,
            "market":           ev.market_definition.name if ev.market_definition else None,
            "bookmaker":        ev.bookmaker_name,
            "selection":        ev.selection,
            "odds":             ev.odds,
            "fair_value":       ev.fair_value_price,
            "no_vig_prob":      ev.no_vig_probability,
            "edge_pct":         ev.edge_pct,
            "discovered_at":    ev.discovered_at.isoformat() if ev.discovered_at else None,
        })
 
    log_event("ev_view", {"tier": g.user.tier})
    return _signed_response({"ok": True, "total": len(results), "ev_opportunities": results})
 
 
@bp_customer.route("/analytics/steam")
@require_tier("pro", "premium")
def get_steam():
    """Sharp money signals — pro + premium only."""
    from app.models.odds_model import SharpMoneySignal, UnifiedMatch
 
    since   = datetime.now(timezone.utc) - datetime.timedelta(hours=6)
    signals = SharpMoneySignal.query.filter(
        SharpMoneySignal.triggered_at >= since,
    ).order_by(SharpMoneySignal.triggered_at.desc()).limit(50).all()
 
    results = []
    for sig in signals:
        match = UnifiedMatch.query.get(sig.match_id)
        results.append({
            "id":           sig.id,
            "match":        match.to_dict() if match else None,
            "market":       sig.market_definition.name if sig.market_definition else None,
            "signal_type":  sig.signal_type,
            "selection":    sig.selection_name,
            "old_price":    sig.old_price,
            "new_price":    sig.new_price,
            "triggered_at": sig.triggered_at.isoformat() if sig.triggered_at else None,
        })
 
    return _signed_response({"ok": True, "signals": results})
 
 
# =============================================================================
# Tools (available to all users)
# =============================================================================
 
@bp_customer.route("/tools/arb-calculator", methods=["POST"])
def arb_calculator():
    from app.workers.ev_arb_service import calculate_arbitrage
    data = request.get_json(force=True) or {}
    bets = data.get("bets", [])
    if not bets:
        return _err("Provide bets: [{odd: float, label: str}]")
 
    result = calculate_arbitrage(bets)
    log_event("arb_calculator", {"bets": len(bets), "is_arb": result.get("is_arb")})
    return _signed_response({"ok": True, **result})
 
 
@bp_customer.route("/tools/ev-calculator", methods=["POST"])
def ev_calculator():
    from app.workers.ev_arb_service import calculate_ev
    data = request.get_json(force=True) or {}
    odds    = float(data.get("odds", 0))
    prob    = float(data.get("fair_probability", 0))
    if odds <= 1.0:
        return _err("odds must be > 1.0")
    if not (0 < prob < 1):
        return _err("fair_probability must be between 0 and 1")
 
    result = calculate_ev(odds, prob)
    log_event("ev_calculator")
    return _signed_response({"ok": True, **result})
 