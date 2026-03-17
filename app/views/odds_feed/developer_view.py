from flask import request, g
from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
from app.utils.decorators_ import log_event, require_tier
from . import bp_odds as bp_customer

@bp_customer.route("/v1/odds/<sport_slug>")
def api_v1_odds(sport_slug: str):
    """Encrypted developer API — API key auth, rate limited."""
    user = _current_user_from_header()
    if not user:
        return _err("API key required. Obtain from /api/developer/keys", 401)
    if user.tier != "premium":
        return _err("Developer API requires a Premium subscription", 403)
 
    from app.workers.celery_tasks import cache_get
    data = cache_get(f"sbo:upcoming:{sport_slug.lower()}")
    log_event("api_call", {"sport": sport_slug, "endpoint": "odds"})
    return _signed_response({
        "ok":      True,
        "sport":   sport_slug,
        "matches": (data or {}).get("matches", []),
    }, encrypt_for=user)
 
 
@bp_customer.route("/v1/arbitrage")
def api_v1_arbitrage():
    user = _current_user_from_header()
    if not user or user.tier != "premium":
        return _err("Developer API requires Premium", 403)
 
    from app.models.odds_model import ArbitrageOpportunity, UnifiedMatch
    arbs = ArbitrageOpportunity.query.filter_by(is_active=True) \
             .order_by(ArbitrageOpportunity.max_profit_percentage.desc()).limit(50).all()
 
    log_event("api_call", {"endpoint": "arbitrage"})
    return _signed_response({
        "ok":   True,
        "arbs": [{
            "match_id":    a.match_id,
            "market":      a.market_definition.name if a.market_definition else None,
            "profit_pct":  a.max_profit_percentage,
            "legs":        [l.to_dict() for l in a.legs],
        } for a in arbs],
    }, encrypt_for=user)
 
 
@bp_customer.route("/developer/keys", methods=["GET", "POST", "DELETE"])
@require_tier("premium")
def developer_keys():
    from app.models.api_key import ApiKey
    from app.extensions import db
    user = g.user
 
    if request.method == "GET":
        keys = ApiKey.query.filter_by(user_id=user.id, is_active=True).all()
        return _signed_response({"ok": True, "keys": [k.to_dict() for k in keys]})
 
    if request.method == "POST":
        data  = request.get_json(force=True) or {}
        count = ApiKey.query.filter_by(user_id=user.id, is_active=True).count()
        if count >= 5:
            return _err("Maximum 5 API keys allowed")
        ak = ApiKey(user_id=user.id, label=data.get("label", "My Key"))
        db.session.add(ak)
        db.session.commit()
        log_event("api_key_created")
        return _signed_response({"ok": True, "key": ak.to_dict()}, 201)
 
    # DELETE
    data   = request.get_json(force=True) or {}
    key_id = data.get("id")
    ak     = ApiKey.query.filter_by(id=key_id, user_id=user.id).first()
    if not ak:
        return _err("Key not found", 404)
    ak.is_active = False
    db.session.commit()
    return _signed_response({"ok": True, "message": "Key revoked."})
 
 
@bp_customer.route("/developer/register-public-key", methods=["POST"])
@require_tier("premium")
def register_public_key():
    """Register your RSA public key for encrypted API responses."""
    data   = request.get_json(force=True) or {}
    pem    = data.get("public_key_pem", "")
 
    if "BEGIN PUBLIC KEY" not in pem and "BEGIN RSA PUBLIC KEY" not in pem:
        return _err("Invalid PEM public key")
 
    from app.extensions import db
    g.user.public_key_pem = pem
    db.session.commit()
    return _signed_response({"ok": True, "message": "Public key registered. Add ?encrypt=1 to API requests."})