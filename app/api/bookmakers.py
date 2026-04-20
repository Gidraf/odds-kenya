from flask import request
from . import bp_bookmakers, _signed_response
from .decorators import tier_required
from app.models.bookmakers_model import Bookmaker
from app.models.odds import BookmakerMatchOdds

@bp_bookmakers.route("/bookmakers", methods=["GET"])
@tier_required("basic")
def list_bookmakers():
    """List all active bookmakers."""
    from app.workers.celery_tasks import cache_get
    bms = Bookmaker.query.filter_by(is_active=True).all()
    return _signed_response({
        "bookmakers": [{"id": b.id, "name": b.name, "slug": b.slug} for b in bms]
    })

@bp_bookmakers.route("/bookmakers/<slug>/matches", methods=["GET"])
@tier_required("basic")
def bookmaker_matches(slug):
    """Get matches with odds from a specific bookmaker."""
    from app.workers.celery_tasks import cache_get
    bookmaker = Bookmaker.query.filter_by(slug=slug).first_or_404()
    sport = request.args.get("sport", "soccer")

    # Fetch from Redis cache (fast)
    cache_key = f"{slug}:upcoming:{sport}"
    cached = cache_get(cache_key)
    if cached:
        return _signed_response({"matches": cached.get("matches", [])})

    return _signed_response({"matches": []})