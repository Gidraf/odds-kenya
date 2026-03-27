"""
app/api/odds_api.py
====================
Public read API for saved odds.

Blueprint: bp_odds  (register with url_prefix="/odds")

Routes
──────
GET  /odds/sports                    — Sports with match counts
GET  /odds/matches                   — Matches (paginated, filterable by sport/status)
GET  /odds/matches/<match_id>        — Single match: all bookmakers + all markets
GET  /odds/live                      — Live matches only
GET  /odds/arbitrage                 — Current arbitrage opportunities
GET  /odds/arbitrage/history         — Historical arb log (last 24h)
GET  /odds/market/<market_name>      — All matches offering a specific market
GET  /odds/bookmaker/<bk_id>/odds    — All odds from one bookmaker
GET  /odds/price-history/<match_id>  — Price movement for a match
GET  /odds/stats                     — System-level stats snapshot

Registration:
    from app.api.odds_api import bp_odds
    app.register_blueprint(bp_odds, url_prefix="/odds")
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from flask import Blueprint, request, jsonify
from sqlalchemy import func, and_, or_, desc

from app.extensions import db
from app.models.odds_model import (
    UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
)
from app.models.bookmakers_model import Bookmaker

bp_odds = Blueprint("odds-unified", __name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _paginate_args() -> tuple[int, int]:
    page     = request.args.get("page", 1, type=int)
    per_page = min(request.args.get("per_page", 50, type=int), 200)
    return page, per_page


def _match_summary(um: UnifiedMatch) -> dict:
    """Lightweight match card for list views."""
    markets = um.markets_json or {}
    mkt_count = sum(
        len(sels)
        for specs in markets.values()
        for sels in specs.values()
    )
    bookmaker_ids = set()
    for specs in markets.values():
        for sels in specs.values():
            for sel_data in sels.values():
                bookmaker_ids.update(sel_data.get("bookmakers", {}).keys())

    return {
        "id":               um.id,
        "parent_match_id":  um.parent_match_id,
        "home_team":        um.home_team_name,
        "away_team":        um.away_team_name,
        "sport":            um.sport_name,
        "competition":      um.competition_name,
        "start_time":       um.start_time.isoformat() if um.start_time else None,
        "status":           um.status,
        "market_count":     mkt_count,
        "bookmaker_count":  len(bookmaker_ids),
        "updated_at":       um.updated_at.isoformat() if um.updated_at else None,
    }


def _match_full(um: UnifiedMatch) -> dict:
    """Full match with all markets + all bookmaker prices."""
    markets_flat: list[dict] = []
    markets_raw   = um.markets_json or {}

    for market_name, specs in markets_raw.items():
        for specifier, selections in specs.items():
            spec_display = None if specifier == "null" else specifier
            for sel_name, sel_data in selections.items():
                markets_flat.append({
                    "market_name":        market_name,
                    "specifier":          spec_display,
                    "selection_name":     sel_name,
                    "best_price":         sel_data.get("best_price"),
                    "best_bookmaker_id":  sel_data.get("best_bookmaker_id"),
                    "is_active":          sel_data.get("is_active", True),
                    "updated_at":         sel_data.get("updated_at"),
                    "bookmakers":         {
                        bk_id: price
                        for bk_id, price in sel_data.get("bookmakers", {}).items()
                    },
                })

    # Per-bookmaker views
    bmo_list = list(um.bookmaker_odds)
    bookmaker_views: list[dict] = []
    for bmo in bmo_list:
        bk = Bookmaker.query.get(bmo.bookmaker_id)
        bookmaker_views.append({
            "bookmaker_id":   bmo.bookmaker_id,
            "bookmaker_name": bk.name if bk else f"BK#{bmo.bookmaker_id}",
            "markets":        bmo.markets_json or {},
            "is_active":      bmo.is_active,
            "updated_at":     bmo.updated_at.isoformat() if bmo.updated_at else None,
        })

    return {
        "id":               um.id,
        "parent_match_id":  um.parent_match_id,
        "home_team":        um.home_team_name,
        "away_team":        um.away_team_name,
        "sport":            um.sport_name,
        "competition":      um.competition_name,
        "start_time":       um.start_time.isoformat() if um.start_time else None,
        "status":           um.status,
        "markets_flat":     markets_flat,
        "bookmakers":       bookmaker_views,
        "updated_at":       um.updated_at.isoformat() if um.updated_at else None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@bp_odds.route("/sports", methods=["GET"])
def list_sports():
    """
    GET /odds/sports
    Returns sports with match counts and active bookmaker counts.
    """
    rows = (
        db.session.query(
            UnifiedMatch.sport_name,
            func.count(UnifiedMatch.id).label("match_count"),
        )
        .filter(UnifiedMatch.sport_name.isnot(None))
        .group_by(UnifiedMatch.sport_name)
        .order_by(desc("match_count"))
        .all()
    )
    return jsonify({
        "ok":     True,
        "sports": [
            {"name": r.sport_name, "match_count": r.match_count}
            for r in rows
        ],
    })


@bp_odds.route("/matches", methods=["GET"])
def list_matches():
    """
    GET /odds/matches
    Query params:
      sport     — filter by sport name
      status    — PRE_MATCH | LIVE | FINISHED
      q         — search home/away team
      page, per_page
    Returns paginated match summaries.
    """
    page, per_page = _paginate_args()

    q = UnifiedMatch.query

    if sport := request.args.get("sport"):
        q = q.filter(UnifiedMatch.sport_name.ilike(f"%{sport}%"))
    if status := request.args.get("status"):
        q = q.filter_by(status=status.upper())
    if search := request.args.get("q"):
        like = f"%{search}%"
        q = q.filter(or_(
            UnifiedMatch.home_team_name.ilike(like),
            UnifiedMatch.away_team_name.ilike(like),
            UnifiedMatch.competition_name.ilike(like),
        ))

    # Default ordering: live first, then upcoming sorted by start_time
    q = q.order_by(
        (UnifiedMatch.status == "LIVE").desc(),
        UnifiedMatch.start_time.asc().nullslast(),
        UnifiedMatch.updated_at.desc(),
    )

    pag = q.paginate(page=page, per_page=per_page, error_out=False)

    return jsonify({
        "ok":       True,
        "total":    pag.total,
        "page":     pag.page,
        "pages":    pag.pages,
        "matches":  [_match_summary(um) for um in pag.items],
    })


@bp_odds.route("/matches/<int:match_id>", methods=["GET"])
def get_match(match_id: int):
    """GET /odds/matches/<id>  — Full match with all markets and all bookmakers."""
    um = UnifiedMatch.query.get_or_404(match_id)
    return jsonify({"ok": True, "match": _match_full(um)})


@bp_odds.route("/live", methods=["GET"])
def live_matches():
    """
    GET /odds/live
    Live matches only, ordered by last update.
    Lightweight — designed for polling every 10s.
    """
    page, per_page = _paginate_args()
    sport = request.args.get("sport")

    q = UnifiedMatch.query.filter_by(status="LIVE")
    if sport:
        q = q.filter(UnifiedMatch.sport_name.ilike(f"%{sport}%"))

    q = q.order_by(UnifiedMatch.updated_at.desc())
    pag = q.paginate(page=page, per_page=per_page, error_out=False)

    return jsonify({
        "ok":         True,
        "total":      pag.total,
        "matches":    [_match_summary(um) for um in pag.items],
        "fetched_at": int(time.time()),
    })


@bp_odds.route("/arbitrage", methods=["GET"])
def arbitrage():
    """
    GET /odds/arbitrage
    Query params:
      sport      — filter by sport
      min_profit — minimum profit % (default 0.3)
      vendor_id  — limit to one vendor's bookmakers
    Returns cached arbitrage opportunities (updated every 90s by Celery).
    """
    sport      = request.args.get("sport")
    min_profit = request.args.get("min_profit", 0.3, type=float)
    vendor_id  = request.args.get("vendor_id", type=int)

    try:
        from app.tasks.odds_harvest_tasks import get_cached_arbitrage
        arbs = get_cached_arbitrage(vendor_id=vendor_id, sport=sport)
    except Exception:
        arbs = []

    # Apply min_profit filter
    arbs = [a for a in arbs if a["profit_pct"] >= min_profit]

    # Group by sport
    by_sport: dict = defaultdict(list)
    for arb in arbs:
        by_sport[arb["sport"]].append(arb)

    return jsonify({
        "ok":       True,
        "count":    len(arbs),
        "by_sport": dict(by_sport),
        "arbs":     arbs,
        "cached_at": arbs[0].get("found_at") if arbs else None,
    })


@bp_odds.route("/arbitrage/history", methods=["GET"])
def arbitrage_history():
    """
    GET /odds/arbitrage/history
    Last 24h price-history records that represent potential arb situations.
    Note: This reads BookmakerOddsHistory — not the arb cache.
    Provides historical evidence of arb opportunities that existed.
    """
    since = _now_utc() - timedelta(hours=24)
    sport = request.args.get("sport")

    # Find matches updated by multiple bookmakers in the last 24h
    subq = (
        db.session.query(
            BookmakerOddsHistory.match_id,
            BookmakerOddsHistory.market,
            BookmakerOddsHistory.specifier,
            BookmakerOddsHistory.selection,
            func.count(func.distinct(BookmakerOddsHistory.bookmaker_id)).label("bk_count"),
            func.max(BookmakerOddsHistory.new_price).label("max_price"),
            func.min(BookmakerOddsHistory.new_price).label("min_price"),
        )
        .filter(BookmakerOddsHistory.recorded_at >= since)
        .group_by(
            BookmakerOddsHistory.match_id,
            BookmakerOddsHistory.market,
            BookmakerOddsHistory.specifier,
            BookmakerOddsHistory.selection,
        )
        .having(func.count(func.distinct(BookmakerOddsHistory.bookmaker_id)) >= 2)
        .subquery()
    )

    # Only show selections where max/min price diverges enough
    threshold = 0.05  # 5% price gap = potential arb signal
    results = []
    for row in db.session.query(subq).all():
        if row.min_price and row.max_price:
            implied_margin = (1 / row.max_price) + (0.95 / row.min_price)  # simplified
            if implied_margin < 1.0:
                match = UnifiedMatch.query.get(row.match_id)
                if match and (not sport or (match.sport_name or "").lower() == sport.lower()):
                    results.append({
                        "match_id":    row.match_id,
                        "home_team":   match.home_team_name,
                        "away_team":   match.away_team_name,
                        "sport":       match.sport_name,
                        "market":      row.market,
                        "specifier":   row.specifier,
                        "selection":   row.selection,
                        "best_price":  row.max_price,
                        "worst_price": row.min_price,
                        "bk_count":    row.bk_count,
                        "est_margin":  round(implied_margin, 5),
                    })

    return jsonify({"ok": True, "count": len(results), "results": results})


@bp_odds.route("/market/<market_name>", methods=["GET"])
def market_view(market_name: str):
    """
    GET /odds/market/<market_name>
    All matches currently offering this market, with best prices.
    Useful for comparing e.g. all "1x2" or all "Total" lines.
    """
    sport  = request.args.get("sport")
    page, per_page = _paginate_args()

    # Filter UnifiedMatch where the market exists in markets_json
    # SQLAlchemy JSON contains query (PostgreSQL style)
    q = UnifiedMatch.query.filter(
        UnifiedMatch.markets_json.has_key(market_name)  # type: ignore
    )
    if sport:
        q = q.filter(UnifiedMatch.sport_name.ilike(f"%{sport}%"))

    q = q.order_by(UnifiedMatch.updated_at.desc())
    pag = q.paginate(page=page, per_page=per_page, error_out=False)

    items = []
    for um in pag.items:
        mkt_data = (um.markets_json or {}).get(market_name, {})
        items.append({
            **_match_summary(um),
            "market_data": mkt_data,
        })

    return jsonify({
        "ok":          True,
        "market_name": market_name,
        "total":       pag.total,
        "matches":     items,
    })


@bp_odds.route("/bookmaker/<int:bk_id>/odds", methods=["GET"])
def bookmaker_odds(bk_id: int):
    """
    GET /odds/bookmaker/<id>/odds
    All current odds from one bookmaker, paginated.
    """
    Bookmaker.query.get_or_404(bk_id)
    page, per_page = _paginate_args()
    sport  = request.args.get("sport")
    status = request.args.get("status")

    q = (
        db.session.query(BookmakerMatchOdds, UnifiedMatch)
        .join(UnifiedMatch, BookmakerMatchOdds.match_id == UnifiedMatch.id)
        .filter(BookmakerMatchOdds.bookmaker_id == bk_id)
        .filter(BookmakerMatchOdds.is_active == True)
    )
    if sport:
        q = q.filter(UnifiedMatch.sport_name.ilike(f"%{sport}%"))
    if status:
        q = q.filter(UnifiedMatch.status == status.upper())

    q = q.order_by(UnifiedMatch.updated_at.desc())
    total = q.count()
    rows  = q.offset((page - 1) * per_page).limit(per_page).all()

    bk = Bookmaker.query.get(bk_id)
    return jsonify({
        "ok":            True,
        "bookmaker_id":  bk_id,
        "bookmaker_name": bk.name if bk else f"BK#{bk_id}",
        "total":         total,
        "page":          page,
        "matches": [
            {
                **_match_summary(um),
                "markets": bmo.markets_json or {},
            }
            for bmo, um in rows
        ],
    })


@bp_odds.route("/price-history/<int:match_id>", methods=["GET"])
def price_history(match_id: int):
    """
    GET /odds/price-history/<match_id>
    Full price movement log for one match across all bookmakers.
    Query params:
      market    — filter to one market name
      hours     — look-back window (default 24)
    """
    um = UnifiedMatch.query.get_or_404(match_id)

    market   = request.args.get("market")
    hours    = request.args.get("hours", 24, type=int)
    since    = _now_utc() - timedelta(hours=hours)

    q = BookmakerOddsHistory.query.filter(
        BookmakerOddsHistory.match_id == match_id,
        BookmakerOddsHistory.recorded_at >= since,
    )
    if market:
        q = q.filter(BookmakerOddsHistory.market == market)

    q = q.order_by(BookmakerOddsHistory.recorded_at.asc())

    history = q.all()
    bk_names = {
        bk.id: bk.name
        for bk in Bookmaker.query.filter(
            Bookmaker.id.in_([h.bookmaker_id for h in history])
        ).all()
    }

    # Group by market → selection for timeline view
    timelines: dict = defaultdict(lambda: defaultdict(list))
    for h in history:
        mk_key  = f"{h.market}|{h.specifier}" if h.specifier else h.market
        sel_key = h.selection
        timelines[mk_key][sel_key].append({
            "bookmaker_id":   h.bookmaker_id,
            "bookmaker_name": bk_names.get(h.bookmaker_id, f"BK#{h.bookmaker_id}"),
            "old_price":      h.old_price,
            "new_price":      h.new_price,
            "price_delta":    h.price_delta,
            "recorded_at":    h.recorded_at.isoformat(),
        })

    return jsonify({
        "ok":        True,
        "match_id":  match_id,
        "home_team": um.home_team_name,
        "away_team": um.away_team_name,
        "sport":     um.sport_name,
        "hours":     hours,
        "timelines": {
            mk: dict(sels)
            for mk, sels in timelines.items()
        },
        "total_events": len(history),
    })


@bp_odds.route("/stats", methods=["GET"])
def system_stats():
    """
    GET /odds/stats
    System-wide snapshot — match counts, bookmaker activity, top arbs.
    Designed for a dashboard header widget.
    """
    now  = _now_utc()
    hour = now - timedelta(hours=1)

    total_matches  = UnifiedMatch.query.count()
    live_matches   = UnifiedMatch.query.filter_by(status="LIVE").count()
    recent_changes = BookmakerOddsHistory.query.filter(
        BookmakerOddsHistory.recorded_at >= hour
    ).count()

    sport_breakdown = [
        {"sport": r[0], "count": r[1]}
        for r in db.session.query(
            UnifiedMatch.sport_name,
            func.count(UnifiedMatch.id)
        ).group_by(UnifiedMatch.sport_name).order_by(desc(func.count(UnifiedMatch.id))).limit(10).all()
        if r[0]
    ]

    try:
        from app.tasks.odds_harvest_tasks import get_cached_arbitrage
        arbs = get_cached_arbitrage()
    except Exception:
        arbs = []

    return jsonify({
        "ok":              True,
        "total_matches":   total_matches,
        "live_matches":    live_matches,
        "price_changes_1h": recent_changes,
        "arb_count":       len(arbs),
        "best_arb_pct":    arbs[0]["profit_pct"] if arbs else 0,
        "sport_breakdown": sport_breakdown,
        "updated_at":      now.isoformat(),
    })


@bp_odds.route("/grouped/<sport>", methods=["GET"])
def odds_grouped_by_sport(sport: str):
    """
    GET /odds/grouped/<sport>
    All matches for a sport with full market data — optimised for the
    odds comparison table where you see all bookmakers side-by-side.
    """
    page, per_page = _paginate_args()
    status = request.args.get("status", "PRE_MATCH")

    q = (
        UnifiedMatch.query
        .filter(UnifiedMatch.sport_name.ilike(f"%{sport}%"))
        .filter_by(status=status.upper())
        .order_by(UnifiedMatch.start_time.asc().nullslast())
    )

    pag = q.paginate(page=page, per_page=per_page, error_out=False)

    return jsonify({
        "ok":     True,
        "sport":  sport,
        "status": status,
        "total":  pag.total,
        "page":   page,
        "pages":  pag.pages,
        "matches": [_match_full(um) for um in pag.items],
    })