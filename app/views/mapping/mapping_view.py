"""
app/api/mapping_api.py
======================
Mapping & onboarding API — markets, aliases, endpoint maps, curl fetch,
deterministic parser generation.

Blueprint prefix: /admin/mapping

Routes
──────
  # Markets
  GET    /markets/                   list + search
  POST   /markets/                   create
  GET    /markets/<id>               detail with aliases
  PUT    /markets/<id>               update
  DELETE /markets/<id>               delete

  # Market aliases
  GET    /markets/<id>/aliases       list aliases for one market
  POST   /markets/<id>/aliases       add alias
  DELETE /market-aliases/<id>        remove alias

  # Entity aliases (team / competition / sport)
  GET    /entity-aliases/            list (filter: bookmaker_id, entity_type)
  POST   /entity-aliases/            create
  DELETE /entity-aliases/<type>/<id> delete  (type = team|competition|sport)

  # Endpoint maps
  GET    /endpoint-maps/             list (filter: bookmaker_id)
  POST   /endpoint-maps/             create / upsert
  GET    /endpoint-maps/<id>         detail
  PUT    /endpoint-maps/<id>         update
  DELETE /endpoint-maps/<id>         delete

  # Primary bookmaker
  POST   /endpoint-maps/<id>/set-primary  set this bookmaker as primary

  # Fetch + parse (the live curl tool)
  POST   /fetch-sample              execute curl template, return raw JSON + preview rows
  POST   /test-paths                test accessor paths against stored sample_response

  # Parser generation (no AI)
  GET    /endpoint-maps/<id>/parser  return generated parse_data() code
  POST   /endpoint-maps/<id>/ingest-matches
                                     fetch live data, extract teams/competitions, return
                                     list for user to confirm before saving

  # Meta
  GET    /meta/                      bookmakers + sports for dropdowns
"""

from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime, timezone

from flask import Blueprint, request, jsonify

from app.extensions import db
from app.models.mapping_models import (
    Market, MarketAlias,
    TeamAlias, CompetitionAlias, SportAlias,
    BookmakerEndpointMap,
)
from app.models.bookmakers_model import Bookmaker
from app.models.competions_model import Sport, Competition, Team, Country
from . import bp  # ← blueprint instance


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _paginate(query, default: int = 50):
    page     = request.args.get("page", 1, type=int)
    per_page = min(request.args.get("per_page", default, type=int), 200)
    pag      = query.paginate(page=page, per_page=per_page, error_out=False)
    return {
        "items": pag.items,
        "total": pag.total,
        "page":  pag.page,
        "pages": pag.pages,
    }


def _run_curl(curl_template: str, timeout: int = 15) -> tuple[str | None, str | None]:
    """
    Execute a curl command (with -s -L flags injected) and return
    (response_text, error_message).
    Strips -o and --output flags to prevent writing files.
    """
    cmd = curl_template.strip()
    # safety: remove -o / --output flags
    cmd = re.sub(r"-o\s+\S+", "", cmd)
    cmd = re.sub(r"--output\s+\S+", "", cmd)
    # inject silent + follow-redirects
    cmd = cmd.replace("curl ", "curl -s -L --max-time 15 ", 1)
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout
        )
        if result.returncode != 0:
            return None, result.stderr or "Non-zero exit"
        return result.stdout, None
    except subprocess.TimeoutExpired:
        return None, "Request timed out"
    except Exception as exc:
        return None, str(exc)


def _resolve(obj, path: str):
    """Resolve dot-path against dict/list. Same logic as BookmakerEndpointMap.resolve."""
    return BookmakerEndpointMap.resolve(obj, path)


def _preview_rows(bem: BookmakerEndpointMap, raw: dict | list, limit: int = 20) -> list[dict]:
    """Extract up to `limit` flattened rows using the stored accessor paths."""
    rows = []
    for item in bem.extract_matches(raw):
        meta = bem.extract_match_meta(item)
        rows.extend(bem.extract_rows(item, meta))
        if len(rows) >= limit:
            break
    return rows[:limit]


# ---------------------------------------------------------------------------
# META
# ---------------------------------------------------------------------------

@bp.get("/meta/")
def meta():
    bookmakers = Bookmaker.query.filter_by(is_active=True).order_by(Bookmaker.name).all()
    sports     = Sport.query.order_by(Sport.name).all()
    return jsonify({
        "bookmakers": [{"id": b.id, "name": b.name or b.domain} for b in bookmakers],
        "sports":     [{"id": s.id, "name": s.name} for s in sports],
    })


# ---------------------------------------------------------------------------
# MARKETS
# ---------------------------------------------------------------------------

@bp.get("/markets/")
def list_markets():
    q       = request.args.get("q", "")
    sport_id= request.args.get("sport_id", type=int)
    query   = Market.query.order_by(Market.name)
    if q:
        query = query.filter(Market.name.ilike(f"%{q}%"))
    if sport_id:
        query = query.filter(
            db.or_(Market.sport_id == sport_id, Market.sport_id.is_(None))
        )
    result = _paginate(query)
    return jsonify({
        **result,
        "items": [m.to_dict() for m in result["items"]],
    })


@bp.post("/markets/")
def create_market():
    body = request.get_json() or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    slug = (body.get("slug") or re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_"))
    if Market.query.filter_by(name=name).first():
        return jsonify({"error": "Market name already exists"}), 409
    m = Market(
        name=name,
        slug=slug,
        description=body.get("description"),
        sport_id=body.get("sport_id") or None,
        is_active=body.get("is_active", True),
    )
    db.session.add(m)
    db.session.commit()
    return jsonify(m.to_dict()), 201


@bp.get("/markets/<int:mid>")
def get_market(mid: int):
    m = db.session.get(Market, mid)
    if not m:
        return jsonify({"error": "Not found"}), 404
    return jsonify(m.to_dict(with_aliases=True))


@bp.put("/markets/<int:mid>")
def update_market(mid: int):
    m = db.session.get(Market, mid)
    if not m:
        return jsonify({"error": "Not found"}), 404
    body = request.get_json() or {}
    for field in ("name", "slug", "description", "is_active", "sport_id"):
        if field in body:
            setattr(m, field, body[field])
    db.session.commit()
    return jsonify(m.to_dict())


@bp.delete("/markets/<int:mid>")
def delete_market(mid: int):
    m = db.session.get(Market, mid)
    if not m:
        return jsonify({"error": "Not found"}), 404
    db.session.delete(m)
    db.session.commit()
    return jsonify({"deleted": mid})


# ---------------------------------------------------------------------------
# MARKET ALIASES
# ---------------------------------------------------------------------------

@bp.get("/markets/<int:mid>/aliases")
def list_market_aliases(mid: int):
    aliases = MarketAlias.query.filter_by(market_id=mid).all()
    return jsonify([a.to_dict() for a in aliases])


@bp.post("/markets/<int:mid>/aliases")
def add_market_alias(mid: int):
    m = db.session.get(Market, mid)
    if not m:
        return jsonify({"error": "Market not found"}), 404
    body = request.get_json() or {}
    bk_id      = body.get("bookmaker_id")
    alias_name = (body.get("alias_name") or "").strip()
    if not bk_id or not alias_name:
        return jsonify({"error": "bookmaker_id and alias_name required"}), 400

    existing = MarketAlias.query.filter_by(
        bookmaker_id=bk_id, alias_name=alias_name
    ).first()
    if existing:
        return jsonify({"error": "Alias already exists for this bookmaker"}), 409

    a = MarketAlias(market_id=mid, bookmaker_id=bk_id, alias_name=alias_name,
                    notes=body.get("notes"))
    db.session.add(a)
    db.session.commit()
    return jsonify(a.to_dict()), 201


@bp.delete("/market-aliases/<int:aid>")
def delete_market_alias(aid: int):
    a = db.session.get(MarketAlias, aid)
    if not a:
        return jsonify({"error": "Not found"}), 404
    db.session.delete(a)
    db.session.commit()
    return jsonify({"deleted": aid})


# ---------------------------------------------------------------------------
# ENTITY ALIASES  (team | competition | sport)
# ---------------------------------------------------------------------------

_ALIAS_MODELS = {
    "team":        TeamAlias,
    "competition": CompetitionAlias,
    "sport":       SportAlias,
}

_ENTITY_ID_FIELD = {
    "team":        "team_id",
    "competition": "competition_id",
    "sport":       "sport_id",
}


@bp.get("/entity-aliases/")
def list_entity_aliases():
    entity_type  = request.args.get("entity_type", "team")
    bookmaker_id = request.args.get("bookmaker_id", type=int)
    q            = request.args.get("q", "")

    model = _ALIAS_MODELS.get(entity_type)
    if not model:
        return jsonify({"error": f"Unknown entity_type: {entity_type}"}), 400

    query = model.query
    if bookmaker_id:
        query = query.filter_by(bookmaker_id=bookmaker_id)
    if q:
        query = query.filter(model.alias_name.ilike(f"%{q}%"))

    result = _paginate(query)
    return jsonify({
        **result,
        "items": [a.to_dict() for a in result["items"]],
    })


@bp.post("/entity-aliases/")
def create_entity_alias():
    body        = request.get_json() or {}
    entity_type = body.get("entity_type", "").strip()
    entity_id   = body.get("entity_id")
    bk_id       = body.get("bookmaker_id")
    alias_name  = (body.get("alias_name") or "").strip()

    model = _ALIAS_MODELS.get(entity_type)
    if not model:
        return jsonify({"error": f"Unknown entity_type: {entity_type}"}), 400
    if not all([entity_id, bk_id, alias_name]):
        return jsonify({"error": "entity_id, bookmaker_id, alias_name required"}), 400

    id_field = _ENTITY_ID_FIELD[entity_type]
    existing = model.query.filter_by(bookmaker_id=bk_id, alias_name=alias_name).first()
    if existing:
        return jsonify({"error": "Alias already exists for this bookmaker"}), 409

    a = model(**{id_field: entity_id, "bookmaker_id": bk_id, "alias_name": alias_name})
    db.session.add(a)
    db.session.commit()
    return jsonify(a.to_dict()), 201


@bp.delete("/entity-aliases/<string:entity_type>/<int:aid>")
def delete_entity_alias(entity_type: str, aid: int):
    model = _ALIAS_MODELS.get(entity_type)
    if not model:
        return jsonify({"error": f"Unknown entity_type: {entity_type}"}), 400
    a = db.session.get(model, aid)
    if not a:
        return jsonify({"error": "Not found"}), 404
    db.session.delete(a)
    db.session.commit()
    return jsonify({"deleted": aid})


# ---------------------------------------------------------------------------
# ENDPOINT MAPS
# ---------------------------------------------------------------------------

@bp.get("/endpoint-maps/")
def list_endpoint_maps():
    bk_id = request.args.get("bookmaker_id", type=int)
    query = BookmakerEndpointMap.query.order_by(
        BookmakerEndpointMap.bookmaker_id,
        BookmakerEndpointMap.endpoint_type
    )
    if bk_id:
        query = query.filter_by(bookmaker_id=bk_id)
    result = _paginate(query, default=100)
    # Strip sample_response from list view (can be large)
    items = []
    for em in result["items"]:
        d = em.to_dict()
        d.pop("sample_response", None)
        items.append(d)
    return jsonify({**result, "items": items})


@bp.post("/endpoint-maps/")
def create_endpoint_map():
    body = request.get_json() or {}
    bk_id = body.get("bookmaker_id")
    etype = (body.get("endpoint_type") or "").upper().strip()
    if not bk_id or not etype:
        return jsonify({"error": "bookmaker_id and endpoint_type required"}), 400

    # Upsert: one row per (bookmaker × endpoint_type)
    em = BookmakerEndpointMap.query.filter_by(
        bookmaker_id=bk_id, endpoint_type=etype
    ).first()
    if not em:
        em = BookmakerEndpointMap(bookmaker_id=bk_id, endpoint_type=etype)
        db.session.add(em)

    _apply_endpoint_map_fields(em, body)
    db.session.commit()
    return jsonify(em.to_dict()), 201


@bp.get("/endpoint-maps/<int:eid>")
def get_endpoint_map(eid: int):
    em = db.session.get(BookmakerEndpointMap, eid)
    if not em:
        return jsonify({"error": "Not found"}), 404
    return jsonify(em.to_dict())


@bp.put("/endpoint-maps/<int:eid>")
def update_endpoint_map(eid: int):
    em = db.session.get(BookmakerEndpointMap, eid)
    if not em:
        return jsonify({"error": "Not found"}), 404
    body = request.get_json() or {}
    _apply_endpoint_map_fields(em, body)
    db.session.commit()
    return jsonify(em.to_dict())


@bp.delete("/endpoint-maps/<int:eid>")
def delete_endpoint_map(eid: int):
    em = db.session.get(BookmakerEndpointMap, eid)
    if not em:
        return jsonify({"error": "Not found"}), 404
    db.session.delete(em)
    db.session.commit()
    return jsonify({"deleted": eid})


@bp.post("/endpoint-maps/<int:eid>/set-primary")
def set_primary_bookmaker(eid: int):
    """Mark this bookmaker as primary for canonical naming. Clears all others."""
    em = db.session.get(BookmakerEndpointMap, eid)
    if not em:
        return jsonify({"error": "Not found"}), 404
    # Clear all primaries
    BookmakerEndpointMap.query.update({"is_primary_bookmaker": False})
    em.is_primary_bookmaker = True
    db.session.commit()
    return jsonify({"ok": True, "primary_bookmaker_id": em.bookmaker_id})


def _apply_endpoint_map_fields(em: BookmakerEndpointMap, body: dict) -> None:
    fields = [
        "url", "method", "headers_json", "params_json", "body_template",
        "curl_template", "sample_response",
        "match_list_array_path", "match_id_path", "home_team_path",
        "away_team_path", "start_time_path", "sport_path", "competition_path",
        "markets_array_path", "market_name_path", "specifier_path",
        "selections_array_path", "selection_name_path", "selection_price_path",
        "is_primary_bookmaker", "is_active",
    ]
    for f in fields:
        if f in body:
            setattr(em, f, body[f])


# ---------------------------------------------------------------------------
# PARSER GENERATION (no AI)
# ---------------------------------------------------------------------------

import shlex
from curl_cffi import requests

def chi_curl(curl_cmd: str, impersonate="chrome110") -> tuple[str | None, str | None]:
    """
    Parses a shell curl string and executes it using curl_cffi 
    to mimic a real browser's TLS/JA3 fingerprints.
    
    Returns: (response_text, error_message)
    """
    tokens = shlex.split(curl_cmd)
    
    url = None
    headers = {}
    method = "GET"
    data = None

    # Basic CLI parser to translate the curl string
    it = iter(tokens)
    for token in it:
        if token.startswith("http"):
            url = token
        elif token in ("-H", "--header"):
            header_val = next(it, None)
            if header_val and ":" in header_val:
                k, v = header_val.split(":", 1)
                headers[k.strip()] = v.strip()
        elif token in ("-d", "--data", "--data-raw", "--data-binary", "--data-urlencode"):
            # If data is provided, curl defaults to POST unless specified otherwise
            method = "POST" if method == "GET" else method
            data = next(it, None)
            if data and isinstance(data, str):
                data = data.encode('utf-8')
        elif token in ("-X", "--request"):
            method = next(it, "GET").upper()

    if not url:
        return None, "Invalid curl template: No URL found."

    try:
        # requests.request from curl_cffi handles the browser impersonation natively
        res = requests.request(
            method=method,
            url=url,
            headers=headers,
            data=data,
            impersonate=impersonate,
            timeout=30
        )
        return res.text, None
        
    except requests.RequestsError as e:
        return None, f"Network or Impersonation error: {str(e)}"
    except Exception as e:
        return None, f"Unexpected chi_curl error: {str(e)}"

@bp.get("/endpoint-maps/<int:eid>/parser")
def generate_parser(eid: int):
    """Return the deterministic parse_data() code for this endpoint map."""
    em = db.session.get(BookmakerEndpointMap, eid)
    if not em:
        return jsonify({"error": "Not found"}), 404
    code = em.build_parser_code()
    return jsonify({"parser_code": code, "endpoint_map_id": eid})


# ---------------------------------------------------------------------------
# FETCH SAMPLE  (execute curl → store + preview)
# ---------------------------------------------------------------------------

@bp.post("/fetch-sample")
def fetch_sample():
    """
    Execute the curl_template for an endpoint map, store the response as
    sample_response, run the accessor paths, return rows preview.

    Body: { "endpoint_map_id": 1 }
          OR { "curl_template": "curl ...", "endpoint_map_id": 1 } to override curl
    """
    body = request.get_json() or {}
    eid  = body.get("endpoint_map_id")
    if not eid:
        return jsonify({"error": "endpoint_map_id required"}), 400

    em = db.session.get(BookmakerEndpointMap, eid)
    if not em:
        return jsonify({"error": "Endpoint map not found"}), 404

    curl = body.get("curl_template") or em.curl_template
    if not curl:
        return jsonify({"error": "No curl_template configured"}), 400

    # Call chi_curl and mimic a modern Chrome browser
    raw_text, err = chi_curl(curl, impersonate="chrome110")
    if err:
        return jsonify({"ok": False, "error": err}), 502

    # Try to parse JSON
    try:
        raw_json = json.loads(raw_text)
    except Exception:
        return jsonify({
            "ok": False,
            "error": "Response is not valid JSON",
            "raw_preview": raw_text[:500],
        }), 422

    # Store sample
    em.sample_response = raw_text[:50_000]  # cap at 50 KB
    db.session.commit()

    # Preview rows
    rows = _preview_rows(em, raw_json, limit=30)

    # Extract unique values for entity discovery
    home_teams   = list({r["home_team"]   for r in rows if r.get("home_team")})
    away_teams   = list({r["away_team"]   for r in rows if r.get("away_team")})
    competitions = list({r["competition"] for r in rows if r.get("competition")})
    sports       = list({r["sport"]       for r in rows if r.get("sport")})
    markets      = list({r["market"]      for r in rows if r.get("market")})

    return jsonify({
        "ok":           True,
        "rows_count":   len(rows),
        "rows_preview": rows[:10],
        "discovered": {
            "teams":        sorted(set(home_teams + away_teams)),
            "competitions": sorted(competitions),
            "sports":       sorted(sports),
            "markets":      sorted(markets),
        },
    })

# ---------------------------------------------------------------------------
# TEST PATHS  (against stored sample_response)
# ---------------------------------------------------------------------------

@bp.post("/test-paths")
def test_paths():
    """
    Test accessor paths against the stored sample_response without re-fetching.

    Body: {
      "endpoint_map_id": 1,
      "paths": {
        "match_list_array_path": "events",
        "match_id_path": "id",
        ...
      }
    }
    Returns preview rows + per-path sample values.
    """
    body = request.get_json() or {}
    eid  = body.get("endpoint_map_id")
    if not eid:
        return jsonify({"error": "endpoint_map_id required"}), 400

    em = db.session.get(BookmakerEndpointMap, eid)
    if not em:
        return jsonify({"error": "Not found"}), 404
    if not em.sample_response:
        return jsonify({"error": "No sample_response stored yet — run fetch-sample first"}), 400

    try:
        raw = json.loads(em.sample_response)
    except Exception:
        return jsonify({"error": "Stored sample_response is not valid JSON"}), 422

    # Apply override paths from body
    paths = body.get("paths", {})
    tmp = BookmakerEndpointMap()  # ephemeral object — not saved
    for field in (
        "match_list_array_path", "match_id_path", "home_team_path",
        "away_team_path", "start_time_path", "sport_path", "competition_path",
        "markets_array_path", "market_name_path", "specifier_path",
        "selections_array_path", "selection_name_path", "selection_price_path",
    ):
        setattr(tmp, field, paths.get(field, getattr(em, field)))

    # Per-path sample values (first match item only)
    matches = tmp.extract_matches(raw)
    first   = matches[0] if matches else {}

    path_samples = {}
    for field in (
        "match_id_path", "home_team_path", "away_team_path",
        "start_time_path", "sport_path", "competition_path",
    ):
        path = getattr(tmp, field) or ""
        path_samples[field] = BookmakerEndpointMap.resolve(first, path) if path else None

    # Markets sample from first match
    mkts_sample = None
    if tmp.markets_array_path and first:
        mkts = BookmakerEndpointMap.resolve(first, tmp.markets_array_path)
        if isinstance(mkts, list) and mkts:
            m0 = mkts[0]
            mkts_sample = {
                "market_name_path":   BookmakerEndpointMap.resolve(m0, tmp.market_name_path or "") if tmp.market_name_path else None,
                "specifier_path":     BookmakerEndpointMap.resolve(m0, tmp.specifier_path   or "") if tmp.specifier_path   else None,
            }
            sels = BookmakerEndpointMap.resolve(m0, tmp.selections_array_path or "") if tmp.selections_array_path else []
            if isinstance(sels, list) and sels:
                s0 = sels[0]
                mkts_sample["selection_name_path"]  = BookmakerEndpointMap.resolve(s0, tmp.selection_name_path  or "") if tmp.selection_name_path  else None
                mkts_sample["selection_price_path"] = BookmakerEndpointMap.resolve(s0, tmp.selection_price_path or "") if tmp.selection_price_path else None

    rows = _preview_rows(tmp, raw, limit=20)

    return jsonify({
        "ok":            True,
        "matches_found": len(matches),
        "rows_count":    len(rows),
        "rows_preview":  rows[:10],
        "path_samples":  path_samples,
        "markets_sample": mkts_sample,
    })


# ---------------------------------------------------------------------------
# INGEST MATCHES  (auto-discover teams + competitions from live data)
# ---------------------------------------------------------------------------

@bp.post("/endpoint-maps/<int:eid>/ingest-matches")
def ingest_matches(eid: int):
    """
    Fetch live data, extract all teams + competitions, cross-reference with
    existing DB records, return a diff for the user to confirm before saving.

    Body: {} (no body needed — uses stored curl_template)

    Response: {
      "ok": true,
      "new_teams": [...],
      "existing_teams": [...],
      "new_competitions": [...],
      "existing_competitions": [...],
      "new_sports": [...],
    }
    """
    em = db.session.get(BookmakerEndpointMap, eid)
    if not em:
        return jsonify({"error": "Not found"}), 404

    # Use stored sample if available, else fetch live
    raw_text = em.sample_response
    if not raw_text:
        curl = em.curl_template
        if not curl:
            return jsonify({"error": "No curl_template configured"}), 400
        raw_text, err = _run_curl(curl)
        if err:
            return jsonify({"ok": False, "error": err}), 502

    try:
        raw = json.loads(raw_text)
    except Exception:
        return jsonify({"error": "Could not parse JSON"}), 422

    # Extract unique entity strings from the raw data
    discovered_teams: set[tuple[str, str]] = set()  # (name, sport_name)
    discovered_comps: set[tuple[str, str]] = set()  # (name, sport_name)
    discovered_sports: set[str] = set()

    for item in em.extract_matches(raw):
        meta = em.extract_match_meta(item)
        sport = str(meta.get("sport") or "")
        comp  = str(meta.get("competition") or "")
        home  = str(meta.get("home_team") or "")
        away  = str(meta.get("away_team") or "")
        if sport:
            discovered_sports.add(sport)
        if comp and sport:
            discovered_comps.add((comp, sport))
        if home and sport:
            discovered_teams.add((home, sport))
        if away and sport:
            discovered_teams.add((away, sport))

    # Cross-reference with DB
    def _team_exists(name, sport_name):
        s = Sport.query.filter(Sport.name.ilike(sport_name)).first()
        if not s:
            return False
        alias = TeamAlias.query.filter(
            TeamAlias.bookmaker_id == em.bookmaker_id,
            TeamAlias.alias_name.ilike(name)
        ).first()
        if alias:
            return True
        return Team.query.filter(Team.sport_id == s.id, Team.name.ilike(name)).first() is not None

    def _comp_exists(name, sport_name):
        s = Sport.query.filter(Sport.name.ilike(sport_name)).first()
        if not s:
            return False
        alias = CompetitionAlias.query.filter(
            CompetitionAlias.bookmaker_id == em.bookmaker_id,
            CompetitionAlias.alias_name.ilike(name)
        ).first()
        if alias:
            return True
        return Competition.query.filter(
            Competition.sport_id == s.id, Competition.name.ilike(name)
        ).first() is not None

    def _sport_exists(name):
        return Sport.query.filter(Sport.name.ilike(name)).first() is not None

    new_teams     = [{"name": n, "sport": s} for n, s in sorted(discovered_teams)  if not _team_exists(n, s)]
    existing_teams= [{"name": n, "sport": s} for n, s in sorted(discovered_teams)  if     _team_exists(n, s)]
    new_comps     = [{"name": n, "sport": s} for n, s in sorted(discovered_comps)  if not _comp_exists(n, s)]
    existing_comps= [{"name": n, "sport": s} for n, s in sorted(discovered_comps)  if     _comp_exists(n, s)]
    new_sports    = [{"name": s} for s in sorted(discovered_sports) if not _sport_exists(s)]

    return jsonify({
        "ok":                  True,
        "new_teams":           new_teams,
        "existing_teams":      existing_teams,
        "new_competitions":    new_comps,
        "existing_competitions": existing_comps,
        "new_sports":          new_sports,
        "totals": {
            "teams":        len(discovered_teams),
            "competitions": len(discovered_comps),
            "sports":       len(discovered_sports),
        },
    })


@bp.post("/endpoint-maps/<int:eid>/confirm-ingest")
def confirm_ingest(eid: int):
    """
    User confirms which new entities to create.

    Body: {
      "create_sports":       [{"name": "Football"}],
      "create_teams":        [{"name": "Man Utd", "sport": "Football", "gender": "M"}],
      "create_competitions": [{"name": "Premier League", "sport": "Football", "gender": "M"}],
      "create_aliases":      true  // create bookmaker aliases for ALL created + existing entities
    }
    """
    em = db.session.get(BookmakerEndpointMap, eid)
    if not em:
        return jsonify({"error": "Not found"}), 404

    body = request.get_json() or {}
    create_aliases = body.get("create_aliases", True)
    created = {"sports": [], "teams": [], "competitions": []}

    # Sports
    for sp_data in (body.get("create_sports") or []):
        name = sp_data.get("name", "").strip()
        if not name:
            continue
        s = Sport.query.filter(Sport.name.ilike(name)).first()
        if not s:
            s = Sport(name=name, is_active=True)
            db.session.add(s)
            db.session.flush()
            created["sports"].append(name)
        if create_aliases:
            _upsert_sport_alias(s.id, em.bookmaker_id, name)

    # Teams
    for t_data in (body.get("create_teams") or []):
        name     = t_data.get("name", "").strip()
        sp_name  = t_data.get("sport", "").strip()
        gender   = t_data.get("gender", "M")
        if not name or not sp_name:
            continue
        sport = Sport.query.filter(Sport.name.ilike(sp_name)).first()
        if not sport:
            continue
        t = Team.query.filter_by(sport_id=sport.id).filter(Team.name.ilike(name)).first()
        if not t:
            t = Team(name=name, sport_id=sport.id, gender=gender)
            db.session.add(t)
            db.session.flush()
            created["teams"].append(name)
        if create_aliases:
            _upsert_team_alias(t.id, em.bookmaker_id, name)

    # Competitions
    for c_data in (body.get("create_competitions") or []):
        name    = c_data.get("name", "").strip()
        sp_name = c_data.get("sport", "").strip()
        gender  = c_data.get("gender", "M")
        if not name or not sp_name:
            continue
        sport = Sport.query.filter(Sport.name.ilike(sp_name)).first()
        if not sport:
            continue
        c = Competition.query.filter_by(sport_id=sport.id).filter(Competition.name.ilike(name)).first()
        if not c:
            c = Competition(name=name, sport_id=sport.id, gender=gender)
            db.session.add(c)
            db.session.flush()
            created["competitions"].append(name)
        if create_aliases:
            _upsert_comp_alias(c.id, em.bookmaker_id, name)

    db.session.commit()
    return jsonify({"ok": True, "created": created})


# ---------------------------------------------------------------------------
# MARKETS INGEST  (extract market names from live data)
# ---------------------------------------------------------------------------

@bp.post("/endpoint-maps/<int:eid>/ingest-markets")
def ingest_markets(eid: int):
    """
    Extract all market names from stored sample_response.
    Returns which are already mapped to canonical Markets and which are new.
    """
    em = db.session.get(BookmakerEndpointMap, eid)
    if not em:
        return jsonify({"error": "Not found"}), 404
    if not em.sample_response:
        return jsonify({"error": "No sample_response — run fetch-sample first"}), 400

    try:
        raw = json.loads(em.sample_response)
    except Exception:
        return jsonify({"error": "Invalid JSON in sample_response"}), 422

    discovered: set[str] = set()
    for item in em.extract_matches(raw):
        for mkt in em.extract_markets(item):
            name = em.resolve(mkt, em.market_name_path or "")
            if name:
                discovered.add(str(name))

    def _find_mapping(alias_name: str):
        alias = MarketAlias.query.filter_by(
            bookmaker_id=em.bookmaker_id, alias_name=alias_name
        ).first()
        if alias:
            return {"market_id": alias.market_id, "market_name": alias.market.name if alias.market else None}
        return None

    result = []
    for name in sorted(discovered):
        mapping = _find_mapping(name)
        result.append({
            "alias_name":  name,
            "mapped":      mapping is not None,
            "market_id":   mapping["market_id"]   if mapping else None,
            "market_name": mapping["market_name"] if mapping else None,
        })

    return jsonify({"ok": True, "markets": result, "total": len(result)})


# ---------------------------------------------------------------------------
# BULK MARKET ALIAS SAVE
# ---------------------------------------------------------------------------

@bp.post("/markets/bulk-alias")
def bulk_market_alias():
    """
    Save multiple market aliases at once (from the ingest-markets UI).

    Body: {
      "bookmaker_id": 1,
      "mappings": [
        {"alias_name": "Match Winner", "market_id": 3},
        {"alias_name": "1x2",          "market_id": 3},
      ]
    }
    """
    body = request.get_json() or {}
    bk_id    = body.get("bookmaker_id")
    mappings = body.get("mappings", [])
    if not bk_id:
        return jsonify({"error": "bookmaker_id required"}), 400

    saved = 0
    for m in mappings:
        alias_name = (m.get("alias_name") or "").strip()
        market_id  = m.get("market_id")
        if not alias_name or not market_id:
            continue
        existing = MarketAlias.query.filter_by(
            bookmaker_id=bk_id, alias_name=alias_name
        ).first()
        if existing:
            existing.market_id = market_id
        else:
            db.session.add(MarketAlias(
                market_id=market_id, bookmaker_id=bk_id, alias_name=alias_name
            ))
        saved += 1

    db.session.commit()
    return jsonify({"ok": True, "saved": saved})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _upsert_sport_alias(sport_id: int, bk_id: int, name: str) -> None:
    if not SportAlias.query.filter_by(bookmaker_id=bk_id, alias_name=name).first():
        db.session.add(SportAlias(sport_id=sport_id, bookmaker_id=bk_id, alias_name=name))


def _upsert_team_alias(team_id: int, bk_id: int, name: str) -> None:
    if not TeamAlias.query.filter_by(bookmaker_id=bk_id, alias_name=name).first():
        db.session.add(TeamAlias(team_id=team_id, bookmaker_id=bk_id, alias_name=name))


def _upsert_comp_alias(comp_id: int, bk_id: int, name: str) -> None:
    if not CompetitionAlias.query.filter_by(bookmaker_id=bk_id, alias_name=name).first():
        db.session.add(CompetitionAlias(competition_id=comp_id, bookmaker_id=bk_id, alias_name=name))