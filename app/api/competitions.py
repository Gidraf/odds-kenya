from flask import request
from . import bp_competitions, _signed_response
from .decorators import tier_required
from app.models.competions_model import Competition, Country, Sport

@bp_competitions.route("/competitions", methods=["GET"])
def list_competitions():
    """List competitions, optionally filtered by sport/country."""
    sport_id = request.args.get("sport_id", type=int)
    country_id = request.args.get("country_id", type=int)

    query = Competition.query
    if sport_id:
        query = query.filter_by(sport_id=sport_id)
    if country_id:
        query = query.filter_by(country_id=country_id)

    comps = query.all()
    return _signed_response({
        "competitions": [{"id": c.id, "name": c.name, "sport_id": c.sport_id, "country_id": c.country_id} for c in comps]
    })

@bp_competitions.route("/countries", methods=["GET"])
def list_countries():
    """List all countries."""
    countries = Country.query.all()
    return _signed_response({
        "countries": [{"id": c.id, "name": c.name} for c in countries]
    })

@bp_competitions.route("/sports", methods=["GET"])
def list_sports():
    """List all sports."""
    sports = Sport.query.all()
    return _signed_response({
        "sports": [{"id": s.id, "name": s.name, "slug": s.slug} for s in sports]
    })