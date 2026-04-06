"""
app/views/odds_feed/match_refresh.py
=====================================
Real-time per-match data refresh.

POST /api/odds/match/<parent_match_id>/refresh

Fetches live data directly from SP + BT + OD in parallel using
the same normalisation pipeline as sp_cross_bk_enrich, but
synchronously — results are returned immediately and also persisted.

Query params:
  ?sources=sp,bt,od   (default: all three)
  ?save=1             (default: 1 — save to DB)

Response includes the fully updated markets_by_bk, best odds,
arb detection, and a per-source fetch report.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from flask import Blueprint, request

from app.utils.customer_jwt_helpers import _err, _signed_response
from app.utils.decorators_ import log_event
from app.views.odds_feed.customer_odds_view import _normalise_sport_slug

bp_match_refresh = Blueprint("match-refresh", __name__, url_prefix="/api")

_REFRESH_TIMEOUT = 18   # seconds per bookmaker fetch
_MAX_WORKERS     = 3


# ── Per-source fetchers ────────────────────────────────────────────────────────

def _fetch_sp(betradar_id: str, sp_game_id: str | None, sport_slug: str) -> tuple[str, dict, str]:
    """
    Fetch fresh SP markets for this match.
    SP uses its own game_id, but we look it up from the betradar_id mapping.
    Falls back to fetching by betradar_id via the search endpoint.
    """
    try:
        from app.workers.sp_harvester import fetch_match_markets
        markets = fetch_match_markets(sp_game_id or betradar_id, sport_slug)
        if not markets:
            return "sp", {}, "no_markets"
        return "sp", markets, "ok"
    except Exception as exc:
        return "sp", {}, str(exc)[:120]


def _fetch_bt(betradar_id: str, sport_slug: str) -> tuple[str, dict, str]:
    """BT parent_match_id == betradar_id."""
    try:
        from app.workers.bt_harvester import get_full_markets, slug_to_bt_sport_id
        bt_sport_id = slug_to_bt_sport_id(sport_slug)
        markets = get_full_markets(betradar_id, bt_sport_id)
        if not markets:
            return "bt", {}, "no_markets"
        return "bt", markets, "ok"
    except Exception as exc:
        return "bt", {}, str(exc)[:120]


def _fetch_od(betradar_id: str, sport_slug: str) -> tuple[str, dict, str]:
    """OD event id == betradar_id."""
    try:
        from app.workers.od_harvester import fetch_event_detail, slug_to_od_sport_id
        od_sport_id = slug_to_od_sport_id(sport_slug)
        markets, _meta = fetch_event_detail(betradar_id, od_sport_id)
        if not markets:
            return "od", {}, "no_markets"
        return "od", markets, "ok"
    except Exception as exc:
        return "od", {}, str(exc)[:120]


# ── Normalise + save ───────────────────────────────────────────────────────────

def _normalise_markets(raw: dict, bk_slug: str) -> dict:
    """
    Ensure markets are in the flat canonical shape:
      {market_slug: {outcome_key: price_float}}
    """
    if not raw:
        return {}
    clean: dict = {}
    for mkt, outcomes in raw.items():
        if not isinstance(outcomes, dict):
            continue
        clean_outs: dict = {}
        for out, val in outcomes.items():
            if isinstance(val, (int, float)):
                fv = float(val)
            elif isinstance(val, dict):
                fv = float(
                    val.get("price") or val.get("odd") or val.get("odds") or 0
                )
            elif isinstance(val, str):
                try:
                    fv = float(val)
                except ValueError:
                    continue
            else:
                continue
            if fv > 1.0:
                clean_outs[out] = fv
        if clean_outs:
            clean[mkt] = clean_outs
    return clean


def _save_bk_markets(
    match_id: int,
    bookmaker_id: int,
    markets: dict,
    betradar_id: str,
) -> int:
    """
    Upsert BookmakerMatchOdds and record history for changed prices.
    Returns number of selections updated.
    """
    from app.extensions import db
    from app.models.odds_model import BookmakerMatchOdds, BookmakerOddsHistory, UnifiedMatch
    from datetime import datetime

    bmo = BookmakerMatchOdds.query.filter_by(
        match_id=match_id, bookmaker_id=bookmaker_id,
    ).with_for_update().first()

    if not bmo:
        bmo = BookmakerMatchOdds(match_id=match_id, bookmaker_id=bookmaker_id)
        db.session.add(bmo)
        db.session.flush()

    um = UnifiedMatch.query.get(match_id)
    history_batch: list[dict] = []
    updates = 0

    for mkt_slug, outcomes in markets.items():
        for outcome, price in outcomes.items():
            try:
                price_changed, old_price = bmo.upsert_selection(
                    market=mkt_slug, specifier=None,
                    selection=outcome, price=float(price),
                )
                if um:
                    um.upsert_bookmaker_price(
                        market=mkt_slug, specifier=None,
                        selection=outcome, price=float(price),
                        bookmaker_id=bookmaker_id,
                    )
                if price_changed:
                    history_batch.append({
                        "bmo_id":       bmo.id,
                        "bookmaker_id": bookmaker_id,
                        "match_id":     match_id,
                        "market":       mkt_slug,
                        "specifier":    None,
                        "selection":    outcome,
                        "old_price":    old_price,
                        "new_price":    float(price),
                        "price_delta":  round(float(price) - old_price, 4) if old_price else None,
                        "recorded_at":  datetime.utcnow(),
                    })
                updates += 1
            except Exception:
                pass

    if history_batch:
        BookmakerOddsHistory.bulk_append(history_batch)

    return updates


def _detect_arbs(markets_by_bk: dict) -> list[dict]:
    """Quick in-memory arb detection across bookmakers."""
    # Build best-per-outcome across all BKs
    best: dict[str, dict[str, tuple[float, str]]] = {}
    for bk_slug, mkts in markets_by_bk.items():
        for mkt, outcomes in (mkts or {}).items():
            best.setdefault(mkt, {})
            for out, price in outcomes.items():
                try:
                    fv = float(price)
                except (TypeError, ValueError):
                    continue
                if fv > 1.0:
                    existing = best[mkt].get(out)
                    if not existing or fv > existing[0]:
                        best[mkt][out] = (fv, bk_slug)

    arbs: list[dict] = []
    for mkt, outcomes in best.items():
        if len(outcomes) < 2:
            continue
        arb_sum = sum(1.0 / v[0] for v in outcomes.values())
        if arb_sum < 1.0:
            profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
            legs = [
                {
                    "outcome":   out,
                    "bk":        v[1],
                    "odd":       v[0],
                    "stake_pct": round((1.0 / v[0] / arb_sum) * 100, 3),
                    "stake_kes": round(1000 * (1.0 / v[0] / arb_sum), 2),
                }
                for out, v in outcomes.items()
            ]
            arbs.append({
                "market":     mkt,
                "profit_pct": profit_pct,
                "arb_sum":    round(arb_sum, 6),
                "legs":       legs,
            })

    return sorted(arbs, key=lambda x: -x["profit_pct"])


# ══════════════════════════════════════════════════════════════════════════════
# ENDPOINT
# ══════════════════════════════════════════════════════════════════════════════

@bp_match_refresh.route("/odds/match/<parent_match_id>/refresh", methods=["POST"])
def refresh_match(parent_match_id: str):
    """
    POST /api/odds/match/<parent_match_id>/refresh

    Fetches live markets from SP + BT + OD in parallel, normalises,
    optionally persists, and returns the full updated match payload.

    Query params:
      sources=sp,bt,od   (comma-separated, default all)
      save=1             (1=persist to DB, 0=return only, default 1)

    Returns:
      {
        ok, match_id, parent_match_id, home_team, away_team,
        markets_by_bk, best, arb_markets, has_arb, best_arb_pct,
        fetch_report: { sp: {status, market_count, latency_ms}, ... },
        changes: { sp: N, bt: N, od: N },
        latency_ms, server_time
      }
    """
    t0 = time.perf_counter()

    # ── Parse params ──────────────────────────────────────────────────────────
    raw_sources = (request.args.get("sources") or "sp,bt,od").strip()
    sources     = {s.strip() for s in raw_sources.split(",") if s.strip()}
    do_save     = request.args.get("save", "1") not in ("0", "false")

    # ── Resolve match ─────────────────────────────────────────────────────────
    try:
        from app.models.odds_model import UnifiedMatch
        from app.models.bookmakers_model import Bookmaker
        um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    except Exception as exc:
        return _err(f"DB error: {exc}", 500)

    if not um:
        return _err("Match not found", 404)

    betradar_id = um.parent_match_id or ""
    if not betradar_id:
        return _err("Match has no betradar_id — cannot refresh", 422)

    # Infer sport slug
   
    sport_slug = _normalise_sport_slug(um.sport_name or "soccer")

    # Resolve bookmaker IDs once
    bk_id_map: dict[str, int] = {}
    try:
        for bk in Bookmaker.query.filter(
            Bookmaker.name.in_(["SportPesa", "Betika", "OdiBets"])
        ).all():
            slug_map = {"sportpesa": "sp", "betika": "bt", "odibets": "od"}
            slug = slug_map.get(bk.name.lower(), bk.name[:2].lower())
            bk_id_map[slug] = bk.id
    except Exception:
        pass

    # SP game id (may be stored in a link table)
    sp_game_id: str | None = None
    try:
        from app.models.bookmakers_model import BookmakerMatchLink
        sp_bk_id = bk_id_map.get("sp")
        if sp_bk_id:
            lnk = BookmakerMatchLink.query.filter_by(
                match_id=um.id, bookmaker_id=sp_bk_id
            ).first()
            if lnk:
                sp_game_id = lnk.external_match_id
    except Exception:
        pass

    log_event("match_refresh", {
        "match_id": parent_match_id, "sources": list(sources), "save": do_save,
    })

    # ── Parallel fetch ─────────────────────────────────────────────────────────
    fetch_jobs: dict[str, any] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        if "sp" in sources:
            fetch_jobs["sp"] = pool.submit(_fetch_sp, betradar_id, sp_game_id, sport_slug)
        if "bt" in sources:
            fetch_jobs["bt"] = pool.submit(_fetch_bt, betradar_id, sport_slug)
        if "od" in sources:
            fetch_jobs["od"] = pool.submit(_fetch_od, betradar_id, sport_slug)

    raw_results: dict[str, tuple[str, dict, str]] = {}
    for slug, fut in fetch_jobs.items():
        try:
            raw_results[slug] = fut.result(timeout=_REFRESH_TIMEOUT)
        except Exception as exc:
            raw_results[slug] = (slug, {}, str(exc)[:120])

    # ── Normalise ──────────────────────────────────────────────────────────────
    markets_by_bk: dict[str, dict] = {}
    fetch_report:  dict[str, dict] = {}
    t_fetch        = time.perf_counter()

    for slug, (_, raw_mkts, status) in raw_results.items():
        clean = _normalise_markets(raw_mkts, slug)
        markets_by_bk[slug] = clean
        fetch_report[slug] = {
            "status":       status,
            "market_count": len(clean),
        }

    # ── Save to DB ─────────────────────────────────────────────────────────────
    changes: dict[str, int] = {}
    if do_save and any(m for m in markets_by_bk.values()):
        try:
            from app.extensions import db
            for slug, clean in markets_by_bk.items():
                if not clean:
                    continue
                bk_id = bk_id_map.get(slug)
                if not bk_id:
                    continue
                n = _save_bk_markets(um.id, bk_id, clean, betradar_id)
                changes[slug] = n
            db.session.commit()
        except Exception as exc:
            try:
                from app.extensions import db
                db.session.rollback()
            except Exception:
                pass
            fetch_report["_save_error"] = str(exc)[:200]

    # ── Build best + arb ───────────────────────────────────────────────────────
    best: dict[str, dict] = {}
    for slug, mkts in markets_by_bk.items():
        for mkt, outcomes in mkts.items():
            best.setdefault(mkt, {})
            for out, price in outcomes.items():
                try:
                    fv = float(price)
                except (TypeError, ValueError):
                    continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]):
                    best[mkt][out] = {"odd": fv, "bk": slug}

    arb_markets = _detect_arbs(markets_by_bk)
    has_arb     = bool(arb_markets)

    # Also include any existing DB markets not refreshed in this call
    try:
        from app.models.odds_model import BookmakerMatchOdds
        from app.views.odds_feed.customer_odds_view import _flatten_db_markets, _bk_slug
        from app.models.bookmakers_model import Bookmaker as Bk2
        existing_bmos = BookmakerMatchOdds.query.filter_by(match_id=um.id).all()
        all_bk = {b.id: b for b in Bk2.query.all()}
        for bmo in existing_bmos:
            bk_obj = all_bk.get(bmo.bookmaker_id)
            if not bk_obj:
                continue
            slug = _bk_slug(bk_obj.name.lower())
            if slug not in markets_by_bk or not markets_by_bk[slug]:
                flat = _flatten_db_markets(bmo.markets_json or {})
                if flat:
                    markets_by_bk[slug] = flat
    except Exception:
        pass

    now = datetime.now(timezone.utc)
    latency_ms = int((time.perf_counter() - t0) * 1000)

    return _signed_response({
        "ok":              True,
        "match_id":        um.id,
        "parent_match_id": parent_match_id,
        "betradar_id":     betradar_id,
        "home_team":       um.home_team_name,
        "away_team":       um.away_team_name,
        "competition":     um.competition_name,
        "sport":           sport_slug,
        "start_time":      um.start_time.isoformat() if um.start_time else None,
        "status":          getattr(um, "status", "PRE_MATCH"),
        "markets_by_bk":   markets_by_bk,
        "market_slugs":    sorted(best.keys()),
        "market_count":    len(best),
        "best":            best,
        "arb_markets":     arb_markets,
        "has_arb":         has_arb,
        "best_arb_pct":    arb_markets[0]["profit_pct"] if arb_markets else 0.0,
        "fetch_report":    fetch_report,
        "sources_tried":   list(sources),
        "sources_ok":      [s for s, r in fetch_report.items() if r.get("status") == "ok"],
        "changes":         changes,
        "saved":           do_save and bool(changes),
        "latency_ms":      latency_ms,
        "server_time":     now.isoformat(),
    })