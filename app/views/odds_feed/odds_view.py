"""
ADD THESE ROUTES to app/views/odds_feed/odds_view.py
=====================================================
Direct PostgreSQL endpoints — work regardless of Redis cache state.
Query unified_matches + bookmaker_match_odds grouped by parent_match_id.

New endpoints added:
  GET /api/odds/unified/<sport_slug>
      All unified matches for a sport, each row showing odds from every
      bookmaker that has data for that match.

  GET /api/odds/unified/match/<match_id>
      Full detail for one unified match — all bookmakers, all markets,
      odds history summary.

  GET /api/odds/unified/search
      Search by team name, competition, or betradar_id across all sports.

  GET /api/odds/unified/stats
      Aggregate stats: total matches, coverage per bookmaker, arb count.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta

from flask import Blueprint, request

# ── paste these route functions into the existing bp_odds Blueprint ───────────
# from app.views.odds_feed.odds_view import bp_odds 

bp_odds = Blueprint("odds-main", __name__, url_prefix="/api/feed")


@bp_odds.route("/unified/<sport_slug>")
def unified_matches(sport_slug: str):
    """
    GET /api/odds/unified/soccer
    Queries PostgreSQL directly — no Redis dependency.

    Returns unified matches grouped by parent_match_id, each match
    showing odds from every bookmaker that has recorded data.

    Query params:
      page        default 1
      per_page    default 25 (max 100)
      sort        start_time | home_team | competition  (default: start_time)
      order       asc | desc  (default: asc)
      comp        filter by competition name (partial match)
      team        filter by home or away team (partial match)
      status      PRE_MATCH | IN_PLAY | FINISHED | all  (default: all)
      has_arb     1 → only matches with arbitrage opportunities
      bk          sp,bt,od → only matches where ALL listed bookmakers have odds
      date        YYYY-MM-DD → filter by start date
    """
    t0 = time.perf_counter()

    page     = max(1,   int(request.args.get("page",     1)))
    per_page = min(100, int(request.args.get("per_page", 25)))
    sort     = request.args.get("sort",   "start_time")
    order    = request.args.get("order",  "asc").lower()
    comp_flt = (request.args.get("comp",  "") or "").strip().lower()
    team_flt = (request.args.get("team",  "") or "").strip().lower()
    status_flt = (request.args.get("status", "all") or "all").upper()
    has_arb  = request.args.get("has_arb", "") in ("1", "true")
    bk_flt   = [b.strip() for b in (request.args.get("bk", "") or "").split(",") if b.strip()]
    date_flt = (request.args.get("date", "") or "").strip()

    try:
        from app.extensions import db
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds,
            ArbitrageOpportunity,
        )
        from app.models.bookmakers_model import Bookmaker, BookmakerMatchLink
        from sqlalchemy import or_, and_, func

        # ── Base query ────────────────────────────────────────────────────────
        q = UnifiedMatch.query

        # Sport filter (sport_name column)
        if sport_slug and sport_slug != "all":
            q = q.filter(UnifiedMatch.sport_name.ilike(f"%{sport_slug}%"))

        # Status filter
        if status_flt != "ALL":
            q = q.filter(UnifiedMatch.status == status_flt)

        # Competition filter
        if comp_flt:
            q = q.filter(UnifiedMatch.competition_name.ilike(f"%{comp_flt}%"))

        # Team filter
        if team_flt:
            q = q.filter(or_(
                UnifiedMatch.home_team_name.ilike(f"%{team_flt}%"),
                UnifiedMatch.away_team_name.ilike(f"%{team_flt}%"),
            ))

        # Date filter
        if date_flt:
            try:
                day = datetime.strptime(date_flt, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                q   = q.filter(
                    UnifiedMatch.start_time >= day,
                    UnifiedMatch.start_time <  day + timedelta(days=1),
                )
            except ValueError:
                pass

        # Sort
        sort_col = {
            "start_time":  UnifiedMatch.start_time,
            "home_team":   UnifiedMatch.home_team_name,
            "competition": UnifiedMatch.competition_name,
        }.get(sort, UnifiedMatch.start_time)
        q = q.order_by(sort_col.desc() if order == "desc" else sort_col.asc())

        total   = q.count()
        matches = q.offset((page - 1) * per_page).limit(per_page).all()

        if not matches:
            return {
                "ok": True, "sport": sport_slug,
                "total": 0, "page": page, "per_page": per_page, "pages": 0,
                "matches": [],
                "latency_ms": int((time.perf_counter() - t0) * 1000),
            }

        match_ids = [m.id for m in matches]

        # ── Load all bookmaker odds for these matches in one query ────────────
        bmo_rows = (
            BookmakerMatchOdds.query
            .filter(BookmakerMatchOdds.match_id.in_(match_ids))
            .all()
        )

        # ── Load bookmaker names ──────────────────────────────────────────────
        bk_ids   = {bmo.bookmaker_id for bmo in bmo_rows}
        bk_objs  = Bookmaker.query.filter(Bookmaker.id.in_(bk_ids)).all() if bk_ids else []
        bk_map   = {b.id: b for b in bk_objs}

        # ── Load bookmaker match links (deep-link URLs) ───────────────────────
        links_rows = (
            BookmakerMatchLink.query
            .filter(BookmakerMatchLink.match_id.in_(match_ids))
            .all()
        )
        links_by_match: dict[int, dict] = {}
        for lnk in links_rows:
            links_by_match.setdefault(lnk.match_id, {})[lnk.bookmaker_id] = lnk.to_dict()

        # ── Load open arb opportunities for these matches ─────────────────────
        try:
            arb_match_ids = {
                row.match_id
                for row in (
                    ArbitrageOpportunity.query
                    .filter(
                        ArbitrageOpportunity.match_id.in_(match_ids),
                        ArbitrageOpportunity.status == "OPEN",
                    )
                    .with_entities(ArbitrageOpportunity.match_id)
                    .all()
                )
            }
        except Exception:
            arb_match_ids = set()

        # ── Group BMO rows by match ───────────────────────────────────────────
        bmo_by_match: dict[int, list] = {}
        for bmo in bmo_rows:
            bmo_by_match.setdefault(bmo.match_id, []).append(bmo)

        # ── Build response rows ───────────────────────────────────────────────
        BK_SLUG_MAP = {
            "sportpesa": "sp", "betika": "bt", "odibets": "od",
            "sp": "sp", "bt": "bt", "od": "od",
        }

        result_rows = []
        for um in matches:
            bmos      = bmo_by_match.get(um.id, [])
            bookmakers: dict[str, dict] = {}

            for bmo in bmos:
                bk_obj  = bk_map.get(bmo.bookmaker_id)
                bk_name = (bk_obj.name if bk_obj else str(bmo.bookmaker_id)).lower()
                bk_slug = BK_SLUG_MAP.get(bk_name, bk_name)
                bk_label = bk_obj.name if bk_obj else bk_slug.upper()

                # markets_json on BMO is {market_slug: {outcome: odd}}
                markets = bmo.markets_json or {}

                bookmakers[bk_slug] = {
                    "bookmaker_id":  bmo.bookmaker_id,
                    "bookmaker":     bk_label,
                    "slug":          bk_slug,
                    "markets":       markets,
                    "market_count":  len(markets),
                    "link":          links_by_match.get(um.id, {}).get(bmo.bookmaker_id),
                    "updated_at":    bmo.updated_at.isoformat() if getattr(bmo, "updated_at", None) else None,
                }

            # Apply bk filter — skip match if not all requested bks present
            if bk_flt and not all(b in bookmakers for b in bk_flt):
                total -= 1
                continue

            # Build best-odds map: {market: {outcome: {odd, bk}}}
            best: dict[str, dict[str, dict]] = {}
            for bk_slug, bk_data in bookmakers.items():
                for mkt, outcomes in (bk_data.get("markets") or {}).items():
                    best.setdefault(mkt, {})
                    for out, odd in (outcomes or {}).items():
                        try:
                            fv = float(odd)
                        except (TypeError, ValueError):
                            continue
                        if fv <= 1.0:
                            continue
                        if out not in best[mkt] or fv > best[mkt][out]["odd"]:
                            best[mkt][out] = {"odd": fv, "bk": bk_slug}

            # Detect arbitrage from best odds
            arb_markets: list[dict] = []
            for mkt, outcomes in best.items():
                if len(outcomes) < 2:
                    continue
                arb_sum = sum(1.0 / v["odd"] for v in outcomes.values())
                if arb_sum < 1.0:
                    profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
                    arb_markets.append({
                        "market":     mkt,
                        "profit_pct": profit_pct,
                        "arb_sum":    round(arb_sum, 6),
                        "legs": [
                            {"outcome": out, "bk": v["bk"], "odd": v["odd"]}
                            for out, v in outcomes.items()
                        ],
                    })
            arb_markets.sort(key=lambda x: -x["profit_pct"])

            has_arb_flag  = bool(arb_markets) or um.id in arb_match_ids

            # Skip if has_arb filter is active and no arb found
            if has_arb and not has_arb_flag:
                total -= 1
                continue

            result_rows.append({
                # ── Identity ──────────────────────────────────────────────────
                "match_id":       um.id,
                "parent_match_id": um.parent_match_id,
                "betradar_id":    um.parent_match_id,   # alias — same field
                # ── Teams ─────────────────────────────────────────────────────
                "home_team":      um.home_team_name,
                "away_team":      um.away_team_name,
                "competition":    um.competition_name,
                "sport":          um.sport_name,
                # ── Timing ────────────────────────────────────────────────────
                "start_time":     um.start_time.isoformat() if um.start_time else None,
                "status":         getattr(um, "status", "PRE_MATCH"),
                # ── Coverage ──────────────────────────────────────────────────
                "bookmaker_count": len(bookmakers),
                "bookmakers":     bookmakers,
                "markets_json":   um.markets_json or {},   # aggregated best odds
                "best":           best,
                # ── Opportunities ─────────────────────────────────────────────
                "has_arb":        has_arb_flag,
                "arb_markets":    arb_markets,
                "best_arb_pct":   arb_markets[0]["profit_pct"] if arb_markets else 0.0,
            })

        pages = max(1, (total + per_page - 1) // per_page)

        return {
            "ok":         True,
            "sport":      sport_slug,
            "total":      total,
            "page":       page,
            "per_page":   per_page,
            "pages":      pages,
            "arb_count":  sum(1 for r in result_rows if r["has_arb"]),
            "bk_filter":  bk_flt or None,
            "matches":    result_rows,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "source":     "postgresql",
        }

    except Exception as exc:
        import traceback
        return {"ok": False, "error": str(exc),
                "trace": traceback.format_exc()}, 500


@bp_odds.route("/unified/match/<int:match_id>")
def unified_match_detail(match_id: int):
    """
    GET /api/odds/unified/match/42
    Full detail for one unified match — all bookmakers, all markets,
    recent odds movement.
    """
    t0 = time.perf_counter()
    try:
        from app.extensions import db
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
            ArbitrageOpportunity, EVOpportunity,
        )
        from app.models.bookmakers_model import Bookmaker, BookmakerMatchLink

        um = UnifiedMatch.query.get(match_id)
        if not um:
            return {"ok": False, "error": "Match not found"}, 404

        # Bookmaker odds
        bmo_list = BookmakerMatchOdds.query.filter_by(match_id=match_id).all()
        bk_ids   = {bmo.bookmaker_id for bmo in bmo_list}
        bk_map   = {b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(bk_ids)).all()} if bk_ids else {}

        BK_SLUG_MAP = {"sportpesa":"sp","betika":"bt","odibets":"od","sp":"sp","bt":"bt","od":"od"}

        bookmakers = {}
        for bmo in bmo_list:
            bk_obj   = bk_map.get(bmo.bookmaker_id)
            bk_name  = (bk_obj.name if bk_obj else str(bmo.bookmaker_id)).lower()
            bk_slug  = BK_SLUG_MAP.get(bk_name, bk_name)
            bookmakers[bk_slug] = {
                "bookmaker_id": bmo.bookmaker_id,
                "bookmaker":    bk_obj.name if bk_obj else bk_slug.upper(),
                "markets":      bmo.markets_json or {},
                "market_count": len(bmo.markets_json or {}),
                "updated_at":   bmo.updated_at.isoformat() if getattr(bmo,"updated_at",None) else None,
            }

        # Links
        links = {
            lnk.bookmaker_id: lnk.to_dict()
            for lnk in BookmakerMatchLink.query.filter_by(match_id=match_id).all()
        }
        for bk_slug, bk_data in bookmakers.items():
            bk_data["link"] = links.get(bk_data["bookmaker_id"])

        # Recent odds history (last 50 changes)
        history = (
            BookmakerOddsHistory.query
            .filter_by(match_id=match_id)
            .order_by(BookmakerOddsHistory.recorded_at.desc())
            .limit(50).all()
        )
        history_rows = []
        for h in history:
            bk_obj = bk_map.get(h.bookmaker_id)
            history_rows.append({
                "bookmaker":   bk_obj.name if bk_obj else str(h.bookmaker_id),
                "market":      h.market,
                "selection":   h.selection,
                "old_price":   h.old_price,
                "new_price":   h.new_price,
                "price_delta": h.price_delta,
                "recorded_at": h.recorded_at.isoformat() if h.recorded_at else None,
            })

        # Open arbs / EVs
        arbs = ArbitrageOpportunity.query.filter_by(
            match_id=match_id, status="OPEN").all()
        evs  = EVOpportunity.query.filter_by(
            match_id=match_id, status="OPEN").all()

        return {
            "ok":           True,
            "match_id":     um.id,
            "parent_match_id": um.parent_match_id,
            "betradar_id":  um.parent_match_id,
            "home_team":    um.home_team_name,
            "away_team":    um.away_team_name,
            "competition":  um.competition_name,
            "sport":        um.sport_name,
            "start_time":   um.start_time.isoformat() if um.start_time else None,
            "status":       getattr(um, "status", "PRE_MATCH"),
            "bookmaker_count": len(bookmakers),
            "bookmakers":   bookmakers,
            "aggregated_markets": um.markets_json or {},
            "arbs":         [a.to_dict() for a in arbs],
            "evs":          [e.to_dict() for e in evs],
            "odds_history": history_rows,
            "latency_ms":   int((time.perf_counter() - t0) * 1000),
            "source":       "postgresql",
        }

    except Exception as exc:
        return {"ok": False, "error": str(exc)}, 500


@bp_odds.route("/unified/search")
def unified_search():
    """
    GET /api/odds/unified/search?q=arsenal
    Search by team name, competition, or betradar_id across all sports.
    """
    t0 = time.perf_counter()
    q_str = (request.args.get("q") or "").strip()
    if len(q_str) < 2:
        return {"ok": False, "error": "query must be at least 2 characters"}, 400

    limit = min(int(request.args.get("limit", 20)), 50)

    try:
        from app.extensions import db
        from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds
        from sqlalchemy import or_, func

        matches = (
            UnifiedMatch.query
            .filter(or_(
                UnifiedMatch.home_team_name.ilike(f"%{q_str}%"),
                UnifiedMatch.away_team_name.ilike(f"%{q_str}%"),
                UnifiedMatch.competition_name.ilike(f"%{q_str}%"),
                UnifiedMatch.parent_match_id.ilike(f"%{q_str}%"),
            ))
            .order_by(UnifiedMatch.start_time.desc())
            .limit(limit).all()
        )

        match_ids = [m.id for m in matches]
        bk_counts = {}
        if match_ids:
            rows = (
                db.session.query(
                    BookmakerMatchOdds.match_id,
                    func.count(BookmakerMatchOdds.bookmaker_id).label("cnt"),
                )
                .filter(BookmakerMatchOdds.match_id.in_(match_ids))
                .group_by(BookmakerMatchOdds.match_id)
                .all()
            )
            bk_counts = {r.match_id: r.cnt for r in rows}

        results = []
        for um in matches:
            results.append({
                "match_id":        um.id,
                "parent_match_id": um.parent_match_id,
                "home_team":       um.home_team_name,
                "away_team":       um.away_team_name,
                "competition":     um.competition_name,
                "sport":           um.sport_name,
                "start_time":      um.start_time.isoformat() if um.start_time else None,
                "status":          getattr(um, "status", "PRE_MATCH"),
                "bookmaker_count": bk_counts.get(um.id, 0),
                "detail_url":      f"/api/odds/unified/match/{um.id}",
            })

        return {
            "ok":         True,
            "query":      q_str,
            "total":      len(results),
            "results":    results,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "source":     "postgresql",
        }

    except Exception as exc:
        return {"ok": False, "error": str(exc)}, 500


@bp_odds.route("/unified/stats")
def unified_stats():
    """
    GET /api/odds/unified/stats
    Aggregate stats: total matches, per-bookmaker coverage, arb count.
    """
    t0 = time.perf_counter()
    try:
        from app.extensions import db
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, ArbitrageOpportunity,
        )
        from app.models.bookmakers_model import Bookmaker
        from sqlalchemy import func

        total_matches = UnifiedMatch.query.count()

        # Per-status breakdown
        status_counts = dict(
            db.session.query(
                UnifiedMatch.status,
                func.count(UnifiedMatch.id),
            ).group_by(UnifiedMatch.status).all()
        ) if hasattr(UnifiedMatch, "status") else {}

        # Per-sport breakdown
        sport_counts = dict(
            db.session.query(
                UnifiedMatch.sport_name,
                func.count(UnifiedMatch.id),
            ).group_by(UnifiedMatch.sport_name)
            .order_by(func.count(UnifiedMatch.id).desc())
            .all()
        )

        # Per-bookmaker coverage
        bk_coverage_rows = (
            db.session.query(
                BookmakerMatchOdds.bookmaker_id,
                func.count(BookmakerMatchOdds.match_id).label("match_count"),
            )
            .group_by(BookmakerMatchOdds.bookmaker_id)
            .all()
        )
        bk_objs = {b.id: b for b in Bookmaker.query.all()}
        bk_coverage = []
        for row in bk_coverage_rows:
            bk  = bk_objs.get(row.bookmaker_id)
            pct = round(row.match_count / total_matches * 100, 1) if total_matches else 0
            bk_coverage.append({
                "bookmaker_id": row.bookmaker_id,
                "bookmaker":    bk.name if bk else str(row.bookmaker_id),
                "match_count":  row.match_count,
                "coverage_pct": pct,
            })
        bk_coverage.sort(key=lambda x: -x["match_count"])

        # Matches covered by 2+ bookmakers
        multi_bk = (
            db.session.query(BookmakerMatchOdds.match_id)
            .group_by(BookmakerMatchOdds.match_id)
            .having(func.count(BookmakerMatchOdds.bookmaker_id) >= 2)
            .count()
        )

        # Matches covered by all 3 main bookmakers
        all_three = (
            db.session.query(BookmakerMatchOdds.match_id)
            .group_by(BookmakerMatchOdds.match_id)
            .having(func.count(BookmakerMatchOdds.bookmaker_id) >= 3)
            .count()
        )

        # Open arbs
        open_arbs = ArbitrageOpportunity.query.filter_by(status="OPEN").count()

        return {
            "ok":             True,
            "total_matches":  total_matches,
            "multi_bk_matches": multi_bk,
            "all_three_bk_matches": all_three,
            "open_arbs":      open_arbs,
            "by_status":      status_counts,
            "by_sport":       sport_counts,
            "bookmaker_coverage": bk_coverage,
            "latency_ms":     int((time.perf_counter() - t0) * 1000),
            "source":         "postgresql",
        }

    except Exception as exc:
        return {"ok": False, "error": str(exc)}, 500