"""
app/api/match_refresh_api.py
==============================
GET /api/odds/match/<join_key>/refresh
  — Fetches a single match live from SP + BT + OD simultaneously,
    merges into a unified payload, and returns it.
  — Does NOT read or write Redis or DB.
  — Uses the join_key (br_<betradar_id> or bk_<match_id>) to
    identify the match and fetch from each bookmaker.

Returns the same shape as the SSE "full" batch match objects
so the frontend can drop it straight into the match list.
"""
from __future__ import annotations

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, jsonify, request

log = logging.getLogger(__name__)

bp_refresh = Blueprint("match_refresh", __name__, url_prefix="/api/odds/match")


# ── Auth helper ───────────────────────────────────────────────────────────────

def _auth_user():
    from app.utils.customer_jwt_helpers import _decode_token
    from app.models.customer import Customer
    auth  = request.headers.get("Authorization", "")
    token = auth[7:] if auth.startswith("Bearer ") else request.args.get("token", "")
    if not token:
        return None
    try:
        payload = _decode_token(token)
        return Customer.query.get(int(payload["sub"]))
    except Exception:
        return None


# ── Bookmaker fetchers (no cache, direct API calls) ───────────────────────────

def _fetch_sp(betradar_id: str, sport_slug: str) -> dict:
    """Fetch full markets for one match from SportPesa directly."""
    t0 = time.perf_counter()
    try:
        from app.workers.sp_harvester import _fetch_markets, _parse_markets, SP_SPORT_ID
        sport_id_str = SP_SPORT_ID.get(sport_slug, "1")
        sport_id_int = int(sport_id_str)
        raw_mkts = _fetch_markets(betradar_id, "all", max_tries=2)
        markets  = _parse_markets(raw_mkts, sport_id=sport_id_int)
        return {
            "bk":        "sp",
            "markets":   markets,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "ok":        bool(markets),
        }
    except Exception as exc:
        log.warning("[refresh:sp] %s: %s", betradar_id, exc)
        return {"bk": "sp", "markets": {}, "latency_ms": int((time.perf_counter() - t0) * 1000), "ok": False}


def _fetch_bt(betradar_id: str, sport_slug: str) -> dict:
    """Fetch full markets for one match from Betika directly."""
    t0 = time.perf_counter()
    try:
        from app.workers.bt_harvester import get_full_markets
        markets = get_full_markets(betradar_id, sport_slug)
        return {
            "bk":        "bt",
            "markets":   markets,
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "ok":        bool(markets),
        }
    except Exception as exc:
        log.warning("[refresh:bt] %s: %s", betradar_id, exc)
        return {"bk": "bt", "markets": {}, "latency_ms": int((time.perf_counter() - t0) * 1000), "ok": False}


def _fetch_od(betradar_id: str, sport_slug: str) -> dict:
    """Fetch full markets for one match from OdiBets directly."""
    t0 = time.perf_counter()
    try:
        from app.workers.od_harvester import fetch_event_detail, slug_to_od_sport_id
        od_sport_id = slug_to_od_sport_id(sport_slug)
        markets, _meta = fetch_event_detail(betradar_id, od_sport_id)
        return {
            "bk":        "od",
            "markets":   markets or {},
            "latency_ms": int((time.perf_counter() - t0) * 1000),
            "ok":        bool(markets),
        }
    except Exception as exc:
        log.warning("[refresh:od] %s: %s", betradar_id, exc)
        return {"bk": "od", "markets": {}, "latency_ms": int((time.perf_counter() - t0) * 1000), "ok": False}


# ── Market merger ─────────────────────────────────────────────────────────────

def _build_unified(betradar_id: str, sport_slug: str, bk_results: list[dict]) -> dict:
    """
    Merge per-BK market results into the unified match shape.
    Applies arb detection from arb_engine.
    """
    from app.api.odds_stream import _normalise_markets, _build_best, _norm_outcome

    bookmakers: dict = {}
    all_markets: dict = {}

    for res in bk_results:
        bk   = res["bk"]
        mkts = _normalise_markets(res["markets"])
        if not mkts:
            continue
        bookmakers[bk] = {
            "bookmaker": bk.upper(),
            "slug":      bk,
            "markets":   mkts,
        }
        for mkt_slug, outcomes in mkts.items():
            all_markets.setdefault(mkt_slug, {})
            for out, price in outcomes.items():
                existing = all_markets[mkt_slug].get(out, 0)
                p = float(price) if not isinstance(price, dict) else float(price.get("odd", price.get("price", 0)))
                if p > existing:
                    all_markets[mkt_slug][out] = p

    best = _build_best(bookmakers)

    # Arb detection
    try:
        from app.workers.arb_engine import detect_arb_for_stream, detect_all_arbs
        has_arb, best_arb_pct, arb_opps = detect_arb_for_stream(best)
        # Also sort arbs by bookmaker pair
        sorted_arbs = sort_arbs_by_bk_pair(arb_opps)
    except Exception:
        has_arb = False; best_arb_pct = 0.0; arb_opps = []; sorted_arbs = {}

    latencies = {r["bk"]: r["latency_ms"] for r in bk_results}

    return {
        "join_key":          f"br_{betradar_id}",
        "parent_match_id":   betradar_id,
        "betradar_id":       betradar_id,
        "sport":             sport_slug,
        "bookmakers":        bookmakers,
        "bk_count":          len(bookmakers),
        "markets":           all_markets,
        "market_count":      len(all_markets),
        "market_slugs":      sorted(all_markets.keys()),
        "best":              best,
        "has_arb":           has_arb,
        "best_arb_pct":      best_arb_pct,
        "arb_opportunities": arb_opps,
        "arb_by_bk_pair":    sorted_arbs,
        "has_ev":            False,
        "is_live":           False,
        "refreshed_at":      __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "latency_ms":        latencies,
        "source":            "live_refresh",
    }


# ── Arb sort by bookmaker pair ────────────────────────────────────────────────

def sort_arbs_by_bk_pair(arb_opps: list[dict]) -> dict[str, list[dict]]:
    """
    Group and sort arb opportunities by the bookmaker pair involved.

    Returns:
    {
        "sp_bt":  [ {...arb...}, ... ],   # SportPesa vs Betika
        "sp_od":  [ ... ],               # SportPesa vs OdiBets
        "bt_od":  [ ... ],               # Betika vs OdiBets
        "sp_bt_od": [ ... ],             # 3-way (all three)
        "other":  [ ... ],               # B2B combinations
    }
    Sorted by profit_pct descending within each group.
    """
    groups: dict[str, list] = {
        "sp_bt":    [],
        "sp_od":    [],
        "bt_od":    [],
        "sp_bt_od": [],
        "other":    [],
    }

    for arb in (arb_opps or []):
        bks_used = set(arb.get("bks_used") or [leg["bk"] for leg in arb.get("legs", [])])

        if bks_used == {"sp", "bt", "od"} or (len(bks_used) == 3 and bks_used >= {"sp", "bt", "od"}):
            groups["sp_bt_od"].append(arb)
        elif "sp" in bks_used and "bt" in bks_used:
            groups["sp_bt"].append(arb)
        elif "sp" in bks_used and "od" in bks_used:
            groups["sp_od"].append(arb)
        elif "bt" in bks_used and "od" in bks_used:
            groups["bt_od"].append(arb)
        else:
            groups["other"].append(arb)

    # Sort each group by profit descending
    for key in groups:
        groups[key].sort(key=lambda a: -(a.get("profit_pct", 0) or 0))

    # Remove empty groups
    return {k: v for k, v in groups.items() if v}


# ── Main endpoint ─────────────────────────────────────────────────────────────

@bp_refresh.route("/<join_key>/refresh", methods=["GET"])
def refresh_match(join_key: str):
    """
    Fetch a single match fresh from all bookmakers. No Redis/DB access.

    Query params:
      sport=soccer        (required — needed to pick correct API endpoints)
      bks=sp,bt,od        (optional — which bookmakers to query, default all three)
    """
    user = _auth_user()
    if not user:
        return jsonify({"error": "Authentication required"}), 401

    sport_slug = request.args.get("sport", "soccer").lower()
    bks_param  = request.args.get("bks", "sp,bt,od")
    requested_bks = {b.strip() for b in bks_param.split(",") if b.strip()}

    # Extract betradar_id from join_key
    betradar_id = ""
    if join_key.startswith("br_"):
        betradar_id = join_key[3:]
    elif join_key.startswith("bt_p_"):
        betradar_id = join_key[5:]
    elif join_key.startswith("od_p_"):
        betradar_id = join_key[5:]
    else:
        betradar_id = join_key  # try as-is

    if not betradar_id:
        return jsonify({"error": "Cannot extract betradar_id from join_key"}), 400

    t0 = time.perf_counter()

    # Build fetcher map
    fetcher_map = {
        "sp": lambda: _fetch_sp(betradar_id, sport_slug),
        "bt": lambda: _fetch_bt(betradar_id, sport_slug),
        "od": lambda: _fetch_od(betradar_id, sport_slug),
    }

    active_fetchers = {bk: fn for bk, fn in fetcher_map.items() if bk in requested_bks}

    bk_results = []
    with ThreadPoolExecutor(max_workers=len(active_fetchers)) as pool:
        futures = {pool.submit(fn): bk for bk, fn in active_fetchers.items()}
        for fut in as_completed(futures):
            try:
                bk_results.append(fut.result())
            except Exception as exc:
                bk = futures[fut]
                log.warning("[refresh] %s %s: %s", bk, betradar_id, exc)
                bk_results.append({"bk": bk, "markets": {}, "latency_ms": 0, "ok": False})

    unified = _build_unified(betradar_id, sport_slug, bk_results)
    unified["total_latency_ms"] = int((time.perf_counter() - t0) * 1000)

    bks_ok    = [r["bk"] for r in bk_results if r["ok"]]
    bks_fail  = [r["bk"] for r in bk_results if not r["ok"]]

    return jsonify({
        "ok":          True,
        "join_key":    join_key,
        "match":       unified,
        "bks_fetched": bks_ok,
        "bks_failed":  bks_fail,
        "arb_summary": {
            "has_arb":      unified["has_arb"],
            "best_pct":     unified["best_arb_pct"],
            "count":        len(unified["arb_opportunities"]),
            "by_pair":      {pair: len(arbs) for pair, arbs in unified["arb_by_bk_pair"].items()},
        },
        "total_latency_ms": unified["total_latency_ms"],
    })


# ── Arb-only endpoint (lighter, just returns sorted arbs for a known match) ───

@bp_refresh.route("/<join_key>/arb", methods=["GET"])
def refresh_arb(join_key: str):
    """
    Like /refresh but only returns arb opportunities.
    Faster — skips heavy market normalisation for non-arb markets.
    """
    user = _auth_user()
    if not user:
        return jsonify({"error": "Authentication required"}), 401

    sport_slug  = request.args.get("sport", "soccer").lower()
    result      = refresh_match.__wrapped__(join_key) if hasattr(refresh_match, "__wrapped__") else None

    # Re-run the full refresh and extract arbs
    betradar_id = join_key.replace("br_", "").replace("bt_p_", "").replace("od_p_", "")
    t0 = time.perf_counter()

    bk_results = []
    with ThreadPoolExecutor(max_workers=3) as pool:
        futs = {
            pool.submit(_fetch_sp, betradar_id, sport_slug): "sp",
            pool.submit(_fetch_bt, betradar_id, sport_slug): "bt",
            pool.submit(_fetch_od, betradar_id, sport_slug): "od",
        }
        for fut in as_completed(futs):
            try:
                bk_results.append(fut.result())
            except Exception:
                bk_results.append({"bk": futs[fut], "markets": {}, "latency_ms": 0, "ok": False})

    unified = _build_unified(betradar_id, sport_slug, bk_results)

    return jsonify({
        "ok":              True,
        "join_key":        join_key,
        "has_arb":         unified["has_arb"],
        "best_arb_pct":    unified["best_arb_pct"],
        "arb_count":       len(unified["arb_opportunities"]),
        "arb_by_bk_pair":  unified["arb_by_bk_pair"],
        "arb_opportunities": unified["arb_opportunities"],
        "bk_count":        unified["bk_count"],
        "total_latency_ms": int((time.perf_counter() - t0) * 1000),
    })