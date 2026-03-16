"""
Sports Reference Data CRUD API  — fixed
=========================================
Blueprint prefix: /admin/sports-data

Import fix: BookmakerEntityValue is now in its own model file.
Add this to your app/__init__.py blueprint registration:

    from app.api.sports_crud import bp as sports_data_bp
    app.register_blueprint(sports_data_bp)

And in your models/__init__.py (or wherever you import models for Alembic):

    from app.models.bookmaker_entity_value import BookmakerEntityValue
"""

from flask import Blueprint, request, jsonify
from app.extensions import db
from app.models.competions_model import Country, Sport, Competition, Team
from app.models.bookmakers_model import Bookmaker
from app.models.bookmakers_model import BookmakerEntityValue   # ← separate file
from . import bookmarker as bp

ENTITY_TYPES = ("country", "sport", "competition", "team")


# ─────────────────────────────────────────────────────────────────────────────
# SERIALISERS
# ─────────────────────────────────────────────────────────────────────────────

def _country_dict(c: Country) -> dict:
    return {"id": c.id, "name": c.name, "iso_code": c.iso_code}


def _sport_dict(s: Sport) -> dict:
    return {"id": s.id, "name": s.name, "is_active": s.is_active}


def _competition_dict(c: Competition) -> dict:
    return {
        "id":           c.id,
        "name":         c.name,
        "sport_id":     c.sport_id,
        "sport_name":   c.sport.name if c.sport else "",
        "country_id":   c.country_id,
        "country_name": c.country.name if c.country else "",
        "gender":       c.gender,
    }


def _team_dict(t: Team) -> dict:
    return {
        "id":         t.id,
        "name":       t.name,
        "sport_id":   t.sport_id,
        "sport_name": t.sport.name if t.sport else "",
        "gender":     t.gender,
    }


def _bv_dict(v: BookmakerEntityValue) -> dict:
    return {
        "id":             v.id,
        "bookmaker_id":   v.bookmaker_id,
        "bookmaker_name": v.bookmaker.name if v.bookmaker else "",
        "entity_type":    v.entity_type,
        "internal_id":    v.internal_id,
        "external_id":    v.external_id,
        "label":          v.label,
        "extra_json":     v.extra_json,
    }


def _paginate(query, default_per_page: int = 50):
    page     = request.args.get("page", 1, type=int)
    per_page = min(request.args.get("per_page", default_per_page, type=int), 200)
    pag      = query.paginate(page=page, per_page=per_page, error_out=False)
    return pag


# ─────────────────────────────────────────────────────────────────────────────
# COUNTRIES
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/countries/", methods=["GET"])
def list_countries():
    q = Country.query
    if s := request.args.get("q"):
        q = q.filter(Country.name.ilike(f"%{s}%"))
    if iso := request.args.get("iso"):
        q = q.filter(Country.iso_code.ilike(iso))
    pag = _paginate(q.order_by(Country.name))
    return jsonify({
        "items": [_country_dict(c) for c in pag.items],
        "total": pag.total, "page": pag.page, "pages": pag.pages,
    })


@bp.route("/countries/", methods=["POST"])
def create_country():
    d = request.json or {}
    if not d.get("name"):
        return jsonify({"error": "name required"}), 400
    if Country.query.filter(Country.name.ilike(d["name"])).first():
        return jsonify({"error": "Country already exists"}), 409
    c = Country(
        name=d["name"].strip(),
        iso_code=(d.get("iso_code") or "").upper()[:3] or None,
    )
    db.session.add(c)
    db.session.commit()
    return jsonify(_country_dict(c)), 201


@bp.route("/countries/<int:cid>", methods=["GET"])
def get_country(cid):
    return jsonify(_country_dict(Country.query.get_or_404(cid)))


@bp.route("/countries/<int:cid>", methods=["PUT"])
def update_country(cid):
    c = Country.query.get_or_404(cid)
    d = request.json or {}
    if "name"     in d: c.name     = d["name"].strip()
    if "iso_code" in d: c.iso_code = (d["iso_code"] or "").upper()[:3] or None
    db.session.commit()
    return jsonify(_country_dict(c))


@bp.route("/countries/<int:cid>", methods=["DELETE"])
def delete_country(cid):
    c = Country.query.get_or_404(cid)
    db.session.delete(c)
    db.session.commit()
    return jsonify({"deleted": cid})


# ─────────────────────────────────────────────────────────────────────────────
# SPORTS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/sports/", methods=["GET"])
def list_sports():
    q = Sport.query
    if s := request.args.get("q"):
        q = q.filter(Sport.name.ilike(f"%{s}%"))
    if (active := request.args.get("active")) is not None:
        q = q.filter(Sport.is_active == (active.lower() == "true"))
    pag = _paginate(q.order_by(Sport.name))
    return jsonify({
        "items": [_sport_dict(s) for s in pag.items],
        "total": pag.total, "page": pag.page, "pages": pag.pages,
    })


@bp.route("/sports/", methods=["POST"])
def create_sport():
    d = request.json or {}
    if not d.get("name"):
        return jsonify({"error": "name required"}), 400
    if Sport.query.filter(Sport.name.ilike(d["name"])).first():
        return jsonify({"error": "Sport already exists"}), 409
    s = Sport(name=d["name"].strip(), is_active=d.get("is_active", True))
    db.session.add(s)
    db.session.commit()
    return jsonify(_sport_dict(s)), 201


@bp.route("/sports/<int:sid>", methods=["GET"])
def get_sport(sid):
    return jsonify(_sport_dict(Sport.query.get_or_404(sid)))


@bp.route("/sports/<int:sid>", methods=["PUT"])
def update_sport(sid):
    s = Sport.query.get_or_404(sid)
    d = request.json or {}
    if "name"      in d: s.name      = d["name"].strip()
    if "is_active" in d: s.is_active = bool(d["is_active"])
    db.session.commit()
    return jsonify(_sport_dict(s))


@bp.route("/sports/<int:sid>", methods=["DELETE"])
def delete_sport(sid):
    s = Sport.query.get_or_404(sid)
    db.session.delete(s)
    db.session.commit()
    return jsonify({"deleted": sid})


# ─────────────────────────────────────────────────────────────────────────────
# COMPETITIONS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/competitions/", methods=["GET"])
def list_competitions():
    q = Competition.query
    if s := request.args.get("q"):
        q = q.filter(Competition.name.ilike(f"%{s}%"))
    if sport_id := request.args.get("sport_id", type=int):
        q = q.filter_by(sport_id=sport_id)
    if country_id := request.args.get("country_id", type=int):
        q = q.filter_by(country_id=country_id)
    if gender := request.args.get("gender"):
        q = q.filter_by(gender=gender)
    pag = _paginate(q.order_by(Competition.name))
    return jsonify({
        "items": [_competition_dict(c) for c in pag.items],
        "total": pag.total, "page": pag.page, "pages": pag.pages,
    })


@bp.route("/competitions/", methods=["POST"])
def create_competition():
    d = request.json or {}
    if not d.get("name") or not d.get("sport_id"):
        return jsonify({"error": "name and sport_id required"}), 400
    c = Competition(
        name=d["name"].strip(),
        sport_id=int(d["sport_id"]),
        country_id=d.get("country_id") or None,
        gender=d.get("gender", "M"),
    )
    db.session.add(c)
    db.session.commit()
    return jsonify(_competition_dict(c)), 201


@bp.route("/competitions/<int:cid>", methods=["GET"])
def get_competition(cid):
    return jsonify(_competition_dict(Competition.query.get_or_404(cid)))


@bp.route("/competitions/<int:cid>", methods=["PUT"])
def update_competition(cid):
    c = Competition.query.get_or_404(cid)
    d = request.json or {}
    if "name"       in d: c.name       = d["name"].strip()
    if "sport_id"   in d: c.sport_id   = int(d["sport_id"])
    if "country_id" in d: c.country_id = d["country_id"] or None
    if "gender"     in d: c.gender     = d["gender"]
    db.session.commit()
    return jsonify(_competition_dict(c))


@bp.route("/competitions/<int:cid>", methods=["DELETE"])
def delete_competition(cid):
    c = Competition.query.get_or_404(cid)
    db.session.delete(c)
    db.session.commit()
    return jsonify({"deleted": cid})


@bp.route("/competitions/by-sport/<int:sport_id>", methods=["GET"])
def competitions_by_sport(sport_id: int):
    comps = (Competition.query
             .filter_by(sport_id=sport_id)
             .order_by(Competition.name)
             .limit(500).all())
    return jsonify([_competition_dict(c) for c in comps])


# ─────────────────────────────────────────────────────────────────────────────
# TEAMS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/teams/", methods=["GET"])
def list_teams():
    q = Team.query
    if s := request.args.get("q"):
        q = q.filter(Team.name.ilike(f"%{s}%"))
    if sport_id := request.args.get("sport_id", type=int):
        q = q.filter_by(sport_id=sport_id)
    if gender := request.args.get("gender"):
        q = q.filter_by(gender=gender)
    pag = _paginate(q.order_by(Team.name))
    return jsonify({
        "items": [_team_dict(t) for t in pag.items],
        "total": pag.total, "page": pag.page, "pages": pag.pages,
    })


@bp.route("/teams/", methods=["POST"])
def create_team():
    d = request.json or {}
    if not d.get("name") or not d.get("sport_id"):
        return jsonify({"error": "name and sport_id required"}), 400
    t = Team(
        name=d["name"].strip(),
        sport_id=int(d["sport_id"]),
        gender=d.get("gender", "M"),
    )
    db.session.add(t)
    db.session.commit()
    return jsonify(_team_dict(t)), 201


@bp.route("/teams/<int:tid>", methods=["GET"])
def get_team(tid):
    return jsonify(_team_dict(Team.query.get_or_404(tid)))


@bp.route("/teams/<int:tid>", methods=["PUT"])
def update_team(tid):
    t = Team.query.get_or_404(tid)
    d = request.json or {}
    if "name"     in d: t.name     = d["name"].strip()
    if "sport_id" in d: t.sport_id = int(d["sport_id"])
    if "gender"   in d: t.gender   = d["gender"]
    db.session.commit()
    return jsonify(_team_dict(t))


@bp.route("/teams/<int:tid>", methods=["DELETE"])
def delete_team(tid):
    t = Team.query.get_or_404(tid)
    db.session.delete(t)
    db.session.commit()
    return jsonify({"deleted": tid})


@bp.route("/teams/by-sport/<int:sport_id>", methods=["GET"])
def teams_by_sport(sport_id: int):
    teams = (Team.query
             .filter_by(sport_id=sport_id)
             .order_by(Team.name)
             .limit(500).all())
    return jsonify([_team_dict(t) for t in teams])


# ─────────────────────────────────────────────────────────────────────────────
# BOOKMAKER ENTITY VALUES
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/bookmaker-values/", methods=["GET"])
def list_bookmaker_values():
    q = BookmakerEntityValue.query
    if bk_id := request.args.get("bookmaker_id", type=int):
        q = q.filter_by(bookmaker_id=bk_id)
    if etype := request.args.get("entity_type"):
        q = q.filter_by(entity_type=etype)
    if internal_id := request.args.get("internal_id", type=int):
        q = q.filter_by(internal_id=internal_id)
    pag = _paginate(q.order_by(
        BookmakerEntityValue.entity_type,
        BookmakerEntityValue.internal_id,
    ))
    return jsonify({
        "items": [_bv_dict(v) for v in pag.items],
        "total": pag.total, "page": pag.page, "pages": pag.pages,
    })


@bp.route("/bookmaker-values/", methods=["POST"])
def create_bookmaker_value():
    d = request.json or {}
    required = ["bookmaker_id", "entity_type", "internal_id", "external_id"]
    if missing := [f for f in required if d.get(f) is None]:
        return jsonify({"error": f"Missing: {missing}"}), 400
    if d["entity_type"] not in ENTITY_TYPES:
        return jsonify({"error": f"entity_type must be one of {ENTITY_TYPES}"}), 400

    # Upsert by (bookmaker_id, entity_type, internal_id)
    existing = BookmakerEntityValue.query.filter_by(
        bookmaker_id=int(d["bookmaker_id"]),
        entity_type=d["entity_type"],
        internal_id=int(d["internal_id"]),
    ).first()

    if existing:
        existing.external_id = str(d["external_id"])
        if "label"      in d: existing.label      = d["label"]
        if "extra_json" in d: existing.extra_json  = d["extra_json"]
        db.session.commit()
        return jsonify(_bv_dict(existing))

    v = BookmakerEntityValue(
        bookmaker_id=int(d["bookmaker_id"]),
        entity_type=d["entity_type"],
        internal_id=int(d["internal_id"]),
        external_id=str(d["external_id"]),
        label=d.get("label"),
        extra_json=d.get("extra_json"),
    )
    db.session.add(v)
    db.session.commit()
    return jsonify(_bv_dict(v)), 201


@bp.route("/bookmaker-values/<int:vid>", methods=["PUT"])
def update_bookmaker_value(vid):
    v = BookmakerEntityValue.query.get_or_404(vid)
    d = request.json or {}
    if "external_id" in d: v.external_id = str(d["external_id"])
    if "label"       in d: v.label       = d["label"]
    if "extra_json"  in d: v.extra_json  = d["extra_json"]
    db.session.commit()
    return jsonify(_bv_dict(v))


@bp.route("/bookmaker-values/<int:vid>", methods=["DELETE"])
def delete_bookmaker_value(vid):
    v = BookmakerEntityValue.query.get_or_404(vid)
    db.session.delete(v)
    db.session.commit()
    return jsonify({"deleted": vid})


# ─────────────────────────────────────────────────────────────────────────────
# META — lightweight lists for dropdowns
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/meta/", methods=["GET"])
def get_meta():
    sports     = Sport.query.filter_by(is_active=True).order_by(Sport.name).all()
    countries  = Country.query.order_by(Country.name).all()
    bookmakers = Bookmaker.query.order_by(Bookmaker.name).all()
    return jsonify({
        "sports":     [_sport_dict(s)   for s in sports],
        "countries":  [_country_dict(c) for c in countries],
        "bookmakers": [{"id": b.id, "name": b.name, "domain": b.domain}
                       for b in bookmakers],
    })