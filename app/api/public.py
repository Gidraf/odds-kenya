from flask import request
from . import bp_public, _signed_response, _err
from app.models.odds_model import UnifiedMatch, MatchStatus
from app.models.competions_model import Competition, Country
from datetime import datetime, timedelta

@bp_public.route("/demo/matches", methods=["GET"])
def demo_matches():
    """Return up to 2 soccer matches starting today (no auth)."""
    today = datetime.now().date()
    matches = UnifiedMatch.query.filter(
        UnifiedMatch.sport_name == "Soccer",
        UnifiedMatch.start_time >= today,
        UnifiedMatch.start_time < today + timedelta(days=1),
        UnifiedMatch.status == MatchStatus.PRE_MATCH
    ).limit(2).all()

    return _signed_response({
        "matches": [m.to_dict() for m in matches]
    })

@bp_public.route("/demo/competitions", methods=["GET"])
def demo_competitions():
    """List all competitions (names only)."""
    comps = Competition.query.with_entities(Competition.id, Competition.name).all()
    return _signed_response({
        "competitions": [{"id": c.id, "name": c.name} for c in comps]
    })

@bp_public.route("/demo/countries", methods=["GET"])
def demo_countries():
    """List all countries."""
    countries = Country.query.with_entities(Country.id, Country.name).all()
    return _signed_response({
        "countries": [{"id": c.id, "name": c.name} for c in countries]
    })