"""
app/views/data_api.py
======================
REST API for canonical entity data — YOUR data, YOUR IDs.

This is the "Oddspedia-class" API layer that serves structured data
from PostgreSQL, not proxied from bookmaker APIs.

Endpoint map
────────────
  GET /api/data/sports                          — all sports
  GET /api/data/sports/<slug>/competitions       — competitions for a sport
  GET /api/data/competitions/<id>                — competition detail + current season
  GET /api/data/competitions/<id>/standings       — league table
  GET /api/data/competitions/<id>/matches         — matches for a competition
  GET /api/data/teams/<id>                       — team detail
  GET /api/data/teams/<id>/form                  — team recent form
  GET /api/data/teams/<id>/squad                 — current squad
  GET /api/data/matches/<id>                     — full match detail
  GET /api/data/matches/<id>/odds                — per-bookmaker odds comparison
  GET /api/data/matches/<id>/events              — match events timeline
  GET /api/data/matches/<id>/stats               — match stats
  GET /api/data/matches/<id>/lineups             — lineups
  GET /api/data/h2h?team_a=X&team_b=Y           — head to head
  GET /api/data/search?q=arsenal                 — search teams/competitions
"""

from __future__ import annotations

import time
from flask import Blueprint, request

bp_data = Blueprint("data_api", __name__, url_prefix="/api/data")


def _err(msg: str, code: int = 400):
    return {"ok": False, "error": msg}, code


def _ok(data: dict) -> dict:
    return {"ok": True, **data}


# ═════════════════════════════════════════════════════════════════════════════
# SPORTS
# ═════════════════════════════════════════════════════════════════════════════

@bp_data.route("/sports")
def list_sports():
    from app.models.competions_model import Sport
    sports = Sport.query.filter_by(is_active=True).order_by(Sport.name).all()
    return _ok({"sports": [s.to_dict() for s in sports]})


@bp_data.route("/sports/<slug>/competitions")
def sport_competitions(slug: str):
    from app.models.competions_model import Sport, Competition

    sport = Sport.query.filter_by(slug=slug).first()
    if not sport:
        return _err(f"Sport '{slug}' not found", 404)

    country_id = request.args.get("country_id", type=int)
    q = Competition.query.filter_by(sport_id=sport.id, is_active=True)
    if country_id:
        q = q.filter_by(country_id=country_id)

    comps = q.order_by(Competition.tier, Competition.name).all()
    return _ok({
        "sport": sport.to_dict(),
        "total": len(comps),
        "competitions": [c.to_dict() for c in comps],
    })


# ═════════════════════════════════════════════════════════════════════════════
# COMPETITIONS
# ═════════════════════════════════════════════════════════════════════════════

@bp_data.route("/competitions/<int:comp_id>")
def competition_detail(comp_id: int):
    from app.models.competions_model import Competition, Season

    comp = Competition.query.get(comp_id)
    if not comp:
        return _err("Competition not found", 404)

    current_season = Season.query.filter_by(
        competition_id=comp_id, is_current=True
    ).first()

    d = comp.to_dict()
    d["sport"] = comp.sport.to_dict() if comp.sport else None
    d["current_season"] = current_season.to_dict() if current_season else None
    return _ok({"competition": d})


@bp_data.route("/competitions/<int:comp_id>/standings")
def competition_standings(comp_id: int):
    from app.models.competions_model import Season
    from app.models.match import SeasonStanding

    season_id = request.args.get("season_id", type=int)
    if season_id:
        season = Season.query.get(season_id)
    else:
        season = Season.query.filter_by(
            competition_id=comp_id, is_current=True
        ).first()

    if not season:
        return _err("No current season found", 404)

    standings = SeasonStanding.query.filter_by(
        season_id=season.id
    ).order_by(SeasonStanding.position).all()

    return _ok({
        "season": season.to_dict(),
        "standings": [s.to_dict() for s in standings],
    })


@bp_data.route("/competitions/<int:comp_id>/matches")
def competition_matches(comp_id: int):
    from app.utils.entity_resolver import MatchQueryService

    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(int(request.args.get("per_page", 25)), 100)
    status   = request.args.get("status")

    result = MatchQueryService.get_competition_matches(
        comp_id, status=status, page=page, per_page=per_page,
    )
    return _ok(result)


# ═════════════════════════════════════════════════════════════════════════════
# TEAMS
# ═════════════════════════════════════════════════════════════════════════════

@bp_data.route("/teams/<int:team_id>")
def team_detail(team_id: int):
    from app.models.competions_model import Team

    team = Team.query.get(team_id)
    if not team:
        return _err("Team not found", 404)

    d = team.to_dict(include_country=True)
    d["sport"] = team.sport.to_dict() if team.sport else None
    return _ok({"team": d})


@bp_data.route("/teams/<int:team_id>/form")
def team_form(team_id: int):
    from app.utils.entity_resolver import MatchQueryService

    limit = min(int(request.args.get("limit", 10)), 30)
    form  = MatchQueryService.get_team_form(team_id, limit=limit)

    wins   = sum(1 for m in form if m.get("outcome") == "W")
    draws  = sum(1 for m in form if m.get("outcome") == "D")
    losses = sum(1 for m in form if m.get("outcome") == "L")
    form_str = "".join(m.get("outcome", "?") for m in form[:5])

    return _ok({
        "team_id": team_id,
        "total":   len(form),
        "wins": wins, "draws": draws, "losses": losses,
        "form": form_str,
        "matches": form,
    })


@bp_data.route("/teams/<int:team_id>/squad")
def team_squad(team_id: int):
    from app.models.competions_model import TeamPlayer

    active_only = request.args.get("active", "1") in ("1", "true")
    q = TeamPlayer.query.filter_by(team_id=team_id)
    if active_only:
        q = q.filter_by(is_active=True)

    players = q.all()
    return _ok({
        "team_id": team_id,
        "total":   len(players),
        "players": [{
            **tp.to_dict(),
            "player": tp.player.to_dict() if tp.player else None,
        } for tp in players],
    })


# ═════════════════════════════════════════════════════════════════════════════
# MATCHES
# ═════════════════════════════════════════════════════════════════════════════

@bp_data.route("/matches/<int:match_id>")
def match_detail(match_id: int):
    from app.utils.entity_resolver import MatchQueryService

    detail = MatchQueryService.get_match_detail(match_id)
    if not detail:
        return _err("Match not found", 404)
    return _ok({"match": detail})


@bp_data.route("/matches/<int:match_id>/odds")
def match_odds(match_id: int):
    """Per-bookmaker odds comparison for a match."""
    from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds
    from app.models.bookmakers_model import BookmakerMatchLink

    um = UnifiedMatch.query.get(match_id)
    if not um:
        return _err("Match not found", 404)

    bmo_list = BookmakerMatchOdds.query.filter_by(match_id=match_id).all()
    links    = BookmakerMatchLink.query.filter_by(match_id=match_id).all()
    link_map = {l.bookmaker_id: l.to_dict() for l in links}

    bookmakers = []
    for bmo in bmo_list:
        entry = bmo.to_dict()
        entry["link"] = link_map.get(bmo.bookmaker_id)
        bookmakers.append(entry)

    return _ok({
        "match_id":   match_id,
        "home_team":  um.home_team_name,
        "away_team":  um.away_team_name,
        "aggregated": um.markets_json or {},
        "bookmakers": bookmakers,
    })


@bp_data.route("/matches/<int:match_id>/events")
def match_events(match_id: int):
    from app.models.match import MatchEvent
    events = MatchEvent.query.filter_by(match_id=match_id).order_by(
        MatchEvent.minute, MatchEvent.id
    ).all()
    return _ok({
        "match_id": match_id,
        "total":    len(events),
        "events":   [e.to_dict() for e in events],
    })


@bp_data.route("/matches/<int:match_id>/stats")
def match_stats(match_id: int):
    from app.models.match import MatchStats
    stats = MatchStats.query.filter_by(match_id=match_id).all()
    return _ok({
        "match_id": match_id,
        "stats":    {s.team_side: s.to_dict() for s in stats},
    })


@bp_data.route("/matches/<int:match_id>/lineups")
def match_lineups(match_id: int):
    from app.models.match import MatchLineup
    lineups = MatchLineup.query.filter_by(match_id=match_id).order_by(
        MatchLineup.team_side, MatchLineup.lineup_type, MatchLineup.jersey_number
    ).all()
    return _ok({
        "match_id": match_id,
        "home": [l.to_dict() for l in lineups if l.team_side == "home"],
        "away": [l.to_dict() for l in lineups if l.team_side == "away"],
    })


# ═════════════════════════════════════════════════════════════════════════════
# HEAD TO HEAD
# ═════════════════════════════════════════════════════════════════════════════

@bp_data.route("/h2h")
def h2h():
    from app.utils.entity_resolver import MatchQueryService

    team_a = request.args.get("team_a", type=int)
    team_b = request.args.get("team_b", type=int)
    if not team_a or not team_b:
        return _err("Provide team_a and team_b query params")

    limit   = min(int(request.args.get("limit", 10)), 30)
    matches = MatchQueryService.get_h2h(team_a, team_b, limit=limit)

    return _ok({
        "team_a":  team_a,
        "team_b":  team_b,
        "total":   len(matches),
        "matches": matches,
    })


# ═════════════════════════════════════════════════════════════════════════════
# SEARCH
# ═════════════════════════════════════════════════════════════════════════════

@bp_data.route("/search")
def search():
    """Search teams and competitions by name."""
    from app.models.competions_model import Team, Competition

    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return _err("Query must be at least 2 characters")

    limit = min(int(request.args.get("limit", 20)), 50)
    pattern = f"%{q}%"

    teams = Team.query.filter(
        Team.name.ilike(pattern)
    ).limit(limit).all()

    comps = Competition.query.filter(
        Competition.name.ilike(pattern)
    ).limit(limit).all()

    return _ok({
        "query": q,
        "teams": [t.to_dict() for t in teams],
        "competitions": [c.to_dict() for c in comps],
    })


# ═════════════════════════════════════════════════════════════════════════════
# ODDS HISTORY (for line movement charts)
# ═════════════════════════════════════════════════════════════════════════════

@bp_data.route("/matches/<int:match_id>/odds-history")
def odds_history(match_id: int):
    """Price movement timeline for a specific selection."""
    from app.models.odds_model import OddsQueryHelper

    market      = request.args.get("market", "1x2")
    selection   = request.args.get("selection", "1")
    bookmaker_id = request.args.get("bookmaker_id", type=int)

    history = OddsQueryHelper.odds_movement(
        match_id=match_id,
        market=market,
        selection=selection,
        bookmaker_id=bookmaker_id,
    ).limit(500).all()

    return _ok({
        "match_id":  match_id,
        "market":    market,
        "selection": selection,
        "total":     len(history),
        "history":   [h.to_dict() for h in history],
    })