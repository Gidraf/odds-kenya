from flask import request
from . import bp_analytics, _signed_response, _err
from .decorators import tier_required
from app.models.match_analytics import MatchAnalytics
from app.models.odds_model import UnifiedMatch

@bp_analytics.route("/matches/<int:match_id>/analytics", methods=["GET"])
@tier_required("pro")
def get_match_analytics(match_id):
    """Get analytics for a match. Triggers scrape if not available."""
    match = UnifiedMatch.query.get_or_404(match_id)
    analytics = MatchAnalytics.query.filter_by(unified_match_id=match_id).first()

    if not analytics:
        from app.workers.tasks_analytics import scrape_sportpesa_match_analytics
        # Queue scraping task
        sp_match_id = None
        from app.models.bookmakers_model import BookmakerMatchLink
        link = BookmakerMatchLink.query.filter_by(match_id=match_id).first()
        if link:
            sp_match_id = link.external_match_id
        if sp_match_id:
            scrape_sportpesa_match_analytics.apply_async(
                args=[sp_match_id], kwargs={"unified_match_id": match_id}
            )
        return _signed_response({
            "status": "pending",
            "message": "Analytics are being fetched. Please try again in a few seconds."
        }, 202)

    return _signed_response(analytics.to_dict())