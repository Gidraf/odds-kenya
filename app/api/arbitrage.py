from flask import request
from . import bp_arbitrage, _signed_response
from .decorators import tier_required
from app.models.odds_model import ArbitrageOpportunity, OpportunityStatus
from datetime import datetime, timedelta

@bp_arbitrage.route("/arbitrage", methods=["GET"])
@tier_required("pro")
def list_arbitrage():
    """List current arbitrage opportunities."""
    sport = request.args.get("sport")
    min_profit = request.args.get("min_profit", 0.5, type=float)

    query = ArbitrageOpportunity.query.filter(
        ArbitrageOpportunity.status == OpportunityStatus.OPEN,
        ArbitrageOpportunity.profit_pct >= min_profit
    )
    if sport:
        query = query.filter(ArbitrageOpportunity.sport == sport)

    # Only show recent (last hour)
    since = datetime.utcnow() - timedelta(hours=1)
    query = query.filter(ArbitrageOpportunity.open_at >= since)

    arbs = query.order_by(ArbitrageOpportunity.profit_pct.desc()).limit(50).all()
    return _signed_response({
        "arbitrage_opportunities": [a.to_dict() for a in arbs]
    })