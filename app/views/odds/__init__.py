from flask import Blueprint

from app.models.bookmakers_model import Bookmaker, BookmakerEndpoint


# ── Optionally import Odds if you have it ─────────────────────────────────────
try:
    from app.models.odds import Odds
    _HAS_ODDS = True
except ImportError:
    _HAS_ODDS = False

def _endpoint_dict(ep: BookmakerEndpoint) -> dict:
    """
    Shape expected by EndpointPanel in the React component.
    Maps BookmakerEndpoint fields → component interface.
    """
    headers = ep.headers_json or {}
    if isinstance(headers, str):
        import json as _json
        try:
            headers = _json.loads(headers)
        except Exception:
            headers = {}

    return {
        "id":              ep.id,
        "type":            ep.endpoint_type,          # component uses `type`
        "url_pattern":     ep.url_pattern,
        "method":          ep.request_method or "GET",
        "is_active":       ep.is_active,
        "parser_tested":   ep.parser_test_passed,     # component uses `parser_tested`
        "curl_command":    ep.curl_command or "",
        "parser_code":     ep.parser_code or "",
        "sample_response": ep.sample_response or "",
        "pagination_info": ep.pagination_info,
        "headers":         headers,
    }


def _bookmaker_dict(bm: Bookmaker, include_endpoints: bool = True) -> dict:
    """
    Shape expected by BookmakerRow in the React component.
    Adds computed stats: endpoints_total, endpoints_active, odds_count.
    """
    eps = list(bm.endpoints)  # eager-load from dynamic relationship

    endpoints_total  = len(eps)
    endpoints_active = sum(1 for e in eps if e.is_active)

    # Odds count — zero if Odds model not available
    odds_count = 0
    if _HAS_ODDS:
        try:
            odds_count = db.session.query(
                func.count(Odds.id)
            ).filter_by(bookmaker_id=bm.id).scalar() or 0
        except Exception:
            odds_count = 0

    result = {
        "id":                    bm.id,
        "name":                  bm.name or bm.domain,
        "domain":                bm.domain,
        "is_active":             bm.is_active,
        "brand_color":           bm.brand_color,
        "needs_ui_intervention": bm.needs_ui_intervention,
        "last_discovery_at":     (
            bm.last_discovery_at.isoformat()
            if bm.last_discovery_at else None
        ),
        "endpoints_total":       endpoints_total,
        "endpoints_active":      endpoints_active,
        "odds_count":            odds_count,
    }

    if include_endpoints:
        result["endpoints"] = [_endpoint_dict(e) for e in eps]

    return result



odds_bp = Blueprint("odds", __name__, url_prefix="/api/odds")