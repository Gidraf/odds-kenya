"""
app/api/sbo_api.py
===================
Flask blueprint for the Sportpesa / Betika / Odibets (SBO) unified odds engine.
Completely separate from the BetB2B odds_feed_api.py.

All imports come from .sbo_fetcher — no dependency on
bookmaker_fetcher.py or any BetB2B code.

Public routes:
    GET /sbo/sports                      list all supported sports + match counts
    GET /sbo/sport/<slug>                upcoming matches merged from SP+BT+OD
    GET /sbo/arbitrage/<slug>            arb-only matches sorted by profit %
    GET /sbo/match/<betradar_id>         full market detail for one match

Admin routes:
    GET  /sbo/admin/sports               same as public /sports (alias)
    POST /sbo/admin/probe                live probe one sport (no cache write)
    POST /sbo/admin/match                full market detail for admin MatchDetailView
    GET  /sbo/admin/health               fetcher connectivity check for all 3 bookmakers

Register:
    from app.api.sbo_api import bp_sbo
    app.register_blueprint(bp_sbo, url_prefix="/api")
"""

from __future__ import annotations

import time

from flask import Blueprint, jsonify, request

bp_sbo = Blueprint("sbo", __name__, url_prefix="/api/sbo")


# =============================================================================
# Sport config helpers
# =============================================================================

def _sport_config(sport_slug: str) -> dict | None:
    """
    Resolve a sport slug to a SPORT_CONFIG entry.
    Accepts: 'soccer', 'football', 'Football', 'basketball', 'ice-hockey', etc.
    """
    from .sbo_fetcher import SPORT_CONFIG
    slug = sport_slug.lower().strip().replace(" ", "-")
    for cfg in SPORT_CONFIG:
        if cfg["sport"] == slug:
            return cfg
    _aliases = {
        "football":           "soccer",
        "soccer":             "soccer",
        "basketball":         "basketball",
        "tennis":             "tennis",
        "ice-hockey":         "ice-hockey",
        "hockey":             "ice-hockey",
        "volleyball":         "volleyball",
        "cricket":            "cricket",
        "rugby":              "rugby",
        "boxing":             "boxing",
        "handball":           "handball",
        "mma":                "mma",
        "table-tennis":       "table-tennis",
        "ping-pong":          "table-tennis",
        "esoccer":            "esoccer",
        "virtual-soccer":     "esoccer",
        "snooker":            "snooker",
        "darts":              "darts",
        "american-football":  "american-football",
        "nfl":                "american-football",
    }
    resolved = _aliases.get(slug)
    if resolved:
        for cfg in SPORT_CONFIG:
            if cfg["sport"] == resolved:
                return cfg
    return None


def _slim_match(m: dict) -> dict:
    """
    Strip the raw_markets bulk from each bookmaker entry for public API responses.
    Keeps best_odds, arbitrage, and per-bookmaker market_count + fetched_at.
    """
    return {
        "betradar_id":  m.get("betradar_id", ""),
        "home_team":    m.get("home_team",   ""),
        "away_team":    m.get("away_team",   ""),
        "competition":  m.get("competition", ""),
        "sport":        m.get("sport",       ""),
        "start_time":   m.get("start_time"),
        "bookie_count": m.get("bookie_count", 0),
        "market_count": m.get("market_count", 0),
        "best_odds":    m.get("best_odds") or {},
        "arbitrage":    m.get("arbitrage") or [],
        "bookmakers": {
            bk: {
                "fetched_at":   v.get("fetched_at"),
                "market_count": len(v.get("raw_markets", [])),
            }
            for bk, v in (m.get("bookmakers") or {}).items()
        },
    }


# =============================================================================
# Public routes
# =============================================================================

@bp_sbo.route("/sports")
def list_sports():
    """
    List all sports supported by the SBO engine with bookmaker ID mappings.

    Response:
    {
        "sports": [
            {
                "sport":        "soccer",
                "display":      "Football",
                "sportpesa_id": "1",
                "betika_id":    14,
                "odibets_id":   "soccer",
                "bookmakers":   ["Sportpesa", "Betika", "Odibets"]
            },
            ...
        ],
        "total": 15
    }
    """
    from .sbo_fetcher import SPORT_CONFIG
    _display = {
        "soccer": "Football", "basketball": "Basketball", "tennis": "Tennis",
        "ice-hockey": "Ice Hockey", "volleyball": "Volleyball", "cricket": "Cricket",
        "rugby": "Rugby", "boxing": "Boxing", "handball": "Handball",
        "mma": "MMA / UFC", "table-tennis": "Table Tennis", "esoccer": "E-Soccer",
        "snooker": "Snooker", "darts": "Darts", "american-football": "American Football",
    }
    sports = []
    for cfg in SPORT_CONFIG:
        slug       = cfg["sport"]
        bookmakers = []
        if cfg.get("sportpesa_id"): bookmakers.append("Sportpesa")
        if cfg.get("betika_id"):    bookmakers.append("Betika")
        if cfg.get("odibets_id"):   bookmakers.append("Odibets")
        sports.append({
            "sport":        slug,
            "display":      _display.get(slug, slug.replace("-", " ").title()),
            "sportpesa_id": cfg.get("sportpesa_id"),
            "betika_id":    cfg.get("betika_id"),
            "odibets_id":   cfg.get("odibets_id"),
            "bookmakers":   bookmakers,
        })
    return jsonify({"sports": sports, "total": len(sports)})


@bp_sbo.route("/sport/<sport_slug>")
def get_sport(sport_slug: str):
    """
    Fetch and merge upcoming odds for a sport from Sportpesa + Betika + Odibets.

    Query params:
        max=<int>        max matches to process (default 30, max 100)
        market=<key>     filter to matches with this market key  e.g. "1x2"
        arb_only=1       only return matches with arbitrage opportunities
        page=<int>        default 1
        per_page=<int>   default 20, max 100

    Response:
    {
        "sport":      "soccer",
        "display":    "Football",
        "total":      int,
        "arb_count":  int,
        "latency_ms": int,
        "page":       int,
        "per_page":   int,
        "pages":      int,
        "bookmakers": ["Sportpesa", "Betika", "Odibets"],
        "meta": {
            "sportpesa_count": int,
            "betika_count":    int,
            "odibets_count":   int,
        },
        "matches": [
            {
                "betradar_id":  str,
                "home_team":    str,
                "away_team":    str,
                "competition":  str,
                "sport":        str,
                "start_time":   str | null,
                "bookie_count": int,
                "market_count": int,
                "best_odds": {
                    "1x2":            {"1":{"odd":1.73,"bookie":"Betika"},
                                       "X":{"odd":3.50,"bookie":"Sportpesa"},
                                       "2":{"odd":5.20,"bookie":"Odibets"}},
                    "over_under_2.5": {"over":{"odd":1.80,"bookie":"Odibets"},
                                       "under":{"odd":2.05,"bookie":"Betika"}},
                    "btts":           {"yes":{...}, "no":{...}},
                    ...
                },
                "bookmakers": {
                    "Sportpesa": {"fetched_at":"2026-03-16T10:00:00Z","market_count":12},
                    "Betika":    {"fetched_at":"2026-03-16T10:00:01Z","market_count":18},
                    "Odibets":   {"fetched_at":"2026-03-16T10:00:01Z","market_count":9},
                },
                "arbitrage": [
                    {
                        "market":       "1x2",
                        "profit_pct":   1.23,
                        "implied_prob": 98.77,
                        "bets": [
                            {"outcome":"1","bookie":"Sportpesa","odd":1.73,"stake_pct":57.80},
                            {"outcome":"X","bookie":"Betika",   "odd":4.10,"stake_pct":24.39},
                            {"outcome":"2","bookie":"Odibets",  "odd":5.20,"stake_pct":19.23}
                        ]
                    }
                ]
            }
        ]
    }
    """
    cfg = _sport_config(sport_slug)
    if not cfg:
        return jsonify({"error": f"Unknown sport '{sport_slug}'. See /api/sbo/sports"}), 404

    max_m    = min(int(request.args.get("max",      30)),  100)
    market   = request.args.get("market")
    arb_only = request.args.get("arb_only", "0") in ("1", "true", "yes")
    page     = max(1, int(request.args.get("page",     1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)

    from .sbo_fetcher import OddsAggregator
    t0      = time.perf_counter()
    matches = OddsAggregator(
        cfg,
        fetch_full_sp_markets=True,
        fetch_full_bt_markets=True,
        fetch_od_markets=True,
    ).run(max_matches=max_m)
    latency = int((time.perf_counter() - t0) * 1000)

    if market:   matches = [m for m in matches if market in (m.get("best_odds") or {})]
    if arb_only: matches = [m for m in matches if m.get("arbitrage")]

    arb_count = sum(1 for m in matches if m.get("arbitrage"))
    sp_count  = sum(1 for m in matches if "Sportpesa" in m.get("bookmakers", {}))
    bt_count  = sum(1 for m in matches if "Betika"    in m.get("bookmakers", {}))
    od_count  = sum(1 for m in matches if "Odibets"   in m.get("bookmakers", {}))

    total = len(matches)
    start = (page - 1) * per_page

    _display = {
        "soccer": "Football", "basketball": "Basketball", "tennis": "Tennis",
        "ice-hockey": "Ice Hockey", "volleyball": "Volleyball", "cricket": "Cricket",
        "rugby": "Rugby", "boxing": "Boxing", "handball": "Handball",
        "mma": "MMA / UFC", "table-tennis": "Table Tennis", "esoccer": "E-Soccer",
        "snooker": "Snooker", "darts": "Darts", "american-football": "American Football",
    }

    return jsonify({
        "sport":      cfg["sport"],
        "display":    _display.get(cfg["sport"], cfg["sport"]),
        "total":      total,
        "arb_count":  arb_count,
        "latency_ms": latency,
        "page":       page,
        "per_page":   per_page,
        "pages":      max(1, (total + per_page - 1) // per_page),
        "bookmakers": ["Sportpesa", "Betika", "Odibets"],
        "meta": {
            "sportpesa_count": sp_count,
            "betika_count":    bt_count,
            "odibets_count":   od_count,
        },
        "matches": [_slim_match(m) for m in matches[start:start + per_page]],
    })


@bp_sbo.route("/arbitrage/<sport_slug>")
def get_arbitrage(sport_slug: str):
    """
    Return only matches with arbitrage opportunities for a sport,
    sorted descending by best profit %.

    Query params: max=<int> (default 50, max 100)

    Response:
    {
        "sport":      str,
        "arb_count":  int,
        "total":      int,
        "latency_ms": int,
        "matches": [
            {
                "betradar_id": str,
                "home_team":   str,
                "away_team":   str,
                "competition": str,
                "start_time":  str | null,
                "bookmakers":  ["Sportpesa", "Betika"],
                "best_profit": float,           ← best arb profit %
                "best_odds":   { ... },
                "arbitrage":   [ { market, profit_pct, implied_prob, bets:[...] } ]
            }
        ]
    }
    """
    cfg = _sport_config(sport_slug)
    if not cfg:
        return jsonify({"error": f"Unknown sport '{sport_slug}'"}), 404

    max_m = min(int(request.args.get("max", 50)), 100)

    from .sbo_fetcher import OddsAggregator
    t0      = time.perf_counter()
    matches = OddsAggregator(
        cfg,
        fetch_full_sp_markets=True,
        fetch_full_bt_markets=True,
        fetch_od_markets=True,
    ).run(max_matches=max_m)
    latency = int((time.perf_counter() - t0) * 1000)

    arbs = sorted(
        [m for m in matches if m.get("arbitrage")],
        key=lambda m: max((a["profit_pct"] for a in m["arbitrage"]), default=0),
        reverse=True,
    )

    return jsonify({
        "sport":      cfg["sport"],
        "arb_count":  len(arbs),
        "total":      len(matches),
        "latency_ms": latency,
        "matches": [
            {
                "betradar_id": m.get("betradar_id", ""),
                "home_team":   m.get("home_team",   ""),
                "away_team":   m.get("away_team",   ""),
                "competition": m.get("competition", ""),
                "start_time":  m.get("start_time"),
                "bookmakers":  list((m.get("bookmakers") or {}).keys()),
                "best_profit": round(max((a["profit_pct"] for a in m["arbitrage"]), default=0), 2),
                "best_odds":   m.get("best_odds") or {},
                "arbitrage":   m.get("arbitrage") or [],
            }
            for m in arbs
        ],
    })


@bp_sbo.route("/match/<betradar_id>")
def get_match(betradar_id: str):
    """
    Full market detail for one match from all 3 bookmakers using its betradar ID.
    Fetches live from SP + BT + OD in parallel.

    Query params:
        sport=<slug>   sport hint for Sportpesa ID lookup (default "soccer")

    Response:
    {
        "ok":              true,
        "betradar_id":     str,
        "home_team":       str,
        "away_team":       str,
        "competition":     str,
        "start_time":      str,
        "latency_ms":      int,
        "bookie_count":    int,
        "market_count":    int,
        "unified_markets": { market_key: { outcome: [{bookie, odd}] } },
        "best_odds":       { market_key: { outcome: {odd, bookie} } },
        "arbitrage":       [ {market, profit_pct, implied_prob, bets:[...]} ],
        "bookmakers": {
            "Sportpesa": { "raw_markets":[...], "market_count":int, "fetched_at":str },
            "Betika":    { ... },
            "Odibets":   { "raw_markets":[...], "market_count":int, "fetched_at":str },
        }
    }
    """
    sport_slug  = request.args.get("sport", "soccer")
    cfg         = _sport_config(sport_slug)
    sp_id       = cfg.get("sportpesa_id") if cfg else None

    from .sbo_fetcher import (
        SportpesaFetcher, BetikaFetcher, OdibetsFetcher,
        MarketMerger, ArbCalculator,
    )

    t0      = time.perf_counter()
    unified: dict = {}
    bk_data: dict = {}
    meta: dict    = {"betradar_id": betradar_id}

    # ── Sportpesa ─────────────────────────────────────────────────────────────
    if sp_id:
        try:
            sp_matches = SportpesaFetcher.fetch_matches(sp_id, days=3, pages=3)
            sp_m = next((m for m in sp_matches if m["betradar_id"] == betradar_id), None)
            if sp_m:
                meta.update({
                    "home_team":   sp_m["home_team"],
                    "away_team":   sp_m["away_team"],
                    "competition": sp_m.get("competition", ""),
                    "start_time":  sp_m.get("start_time"),
                })
                sp_raw = SportpesaFetcher.fetch_markets(sp_m["game_id"])
                if sp_raw:
                    parsed  = SportpesaFetcher.parse_markets(sp_raw)
                    unified = MarketMerger.apply(unified, parsed, "Sportpesa")
                    bk_data["Sportpesa"] = {
                        "raw_markets":  parsed,
                        "market_count": len(parsed),
                        "fetched_at":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    }
        except Exception as exc:
            bk_data["Sportpesa"] = {"error": str(exc)}

    # ── Betika ────────────────────────────────────────────────────────────────
    try:
        bt_full = BetikaFetcher.fetch_markets(betradar_id)
        if bt_full:
            parsed  = BetikaFetcher.parse_markets(bt_full)
            unified = MarketMerger.apply(unified, parsed, "Betika")
            bk_data["Betika"] = {
                "raw_markets":  parsed,
                "market_count": len(parsed),
                "fetched_at":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
    except Exception as exc:
        bk_data["Betika"] = {"error": str(exc)}

    # ── Odibets ───────────────────────────────────────────────────────────────
    try:
        od_raw, od_meta = OdibetsFetcher.fetch_markets(betradar_id)
        if od_raw:
            unified = MarketMerger.apply(unified, od_raw, "Odibets")
            bk_data["Odibets"] = {
                "raw_markets":  od_raw,
                "market_count": len(od_raw),
                "fetched_at":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        if od_meta and not meta.get("home_team"):
            meta.update(od_meta)
    except Exception as exc:
        bk_data["Odibets"] = {"error": str(exc)}

    best    = MarketMerger.best_odds(unified)
    arbs    = ArbCalculator.check(unified)
    latency = int((time.perf_counter() - t0) * 1000)

    return jsonify({
        "ok":              True,
        "latency_ms":      latency,
        **meta,
        "bookie_count":    len([b for b in bk_data if "error" not in bk_data.get(b, {})]),
        "market_count":    len(unified),
        "unified_markets": unified,
        "best_odds":       best,
        "arbitrage":       arbs,
        "bookmakers":      bk_data,
    })


# =============================================================================
# Admin routes
# =============================================================================

@bp_sbo.route("/admin/sports")
def admin_list_sports():
    """Admin alias for /sbo/sports — identical response."""
    return list_sports()


@bp_sbo.route("/admin/probe", methods=["POST"])
def admin_probe():
    """
    Live probe — fetch Sportpesa + Betika + Odibets for one sport.
    Returns full match data (raw_markets included) for the admin monitor.
    Does NOT write to any cache.

    POST body:
    {
        "sport":        "soccer",   ← sport slug
        "max":          10,         ← max matches (default 10, max 50)
        "sp_only":      false,      ← only Sportpesa
        "bt_only":      false,      ← only Betika
        "od_only":      false,      ← only Odibets
        "full_markets": true        ← fetch full market book (slower but richer)
    }

    Response:
    {
        "ok":         true,
        "sport":      str,
        "total":      int,
        "arb_count":  int,
        "latency_ms": int,
        "bookmakers": ["Sportpesa", "Betika", "Odibets"],
        "per_bookmaker": {
            "Sportpesa": {"ok": true,  "count": 25, "latency_ms": 840, "error": null},
            "Betika":    {"ok": true,  "count": 24, "latency_ms": 1120,"error": null},
            "Odibets":   {"ok": false, "count": 0,  "latency_ms": 200, "error": "..."},
        },
        "matches": [ ... full match dicts with raw_markets ... ]
    }
    """
    d          = request.get_json(force=True) or {}
    sport_slug = d.get("sport", "soccer")
    max_m      = min(int(d.get("max", 10)), 50)
    sp_only    = bool(d.get("sp_only",  False))
    bt_only    = bool(d.get("bt_only",  False))
    od_only    = bool(d.get("od_only",  False))
    full_mkts  = bool(d.get("full_markets", True))

    cfg = _sport_config(sport_slug)
    if not cfg:
        return jsonify({"error": f"Unknown sport '{sport_slug}'"}), 404

    from .sbo_fetcher import OddsAggregator
    t0  = time.perf_counter()
    agg = OddsAggregator(
        cfg,
        fetch_full_sp_markets=(not bt_only and not od_only) and full_mkts,
        fetch_full_bt_markets=(not sp_only and not od_only) and full_mkts,
        fetch_od_markets=(not sp_only and not bt_only),
    )
    matches = agg.run(max_matches=max_m)
    latency = int((time.perf_counter() - t0) * 1000)

    # Build per-bookmaker stats
    per_bk: dict = {}
    for bk in ("Sportpesa", "Betika", "Odibets"):
        bk_matches = [m for m in matches if bk in m.get("bookmakers", {})]
        errors     = [m["bookmakers"][bk].get("error")
                      for m in bk_matches if isinstance(m.get("bookmakers", {}).get(bk), dict)
                      and m["bookmakers"][bk].get("error")]
        per_bk[bk] = {
            "ok":         len(bk_matches) > 0,
            "count":      len(bk_matches),
            "latency_ms": None,
            "error":      errors[0] if errors else None,
        }

    arb_count = sum(1 for m in matches if m.get("arbitrage"))
    bk_used   = (["Sportpesa"] if sp_only else
                 ["Betika"]    if bt_only else
                 ["Odibets"]   if od_only else
                 ["Sportpesa", "Betika", "Odibets"])

    return jsonify({
        "ok":           True,
        "sport":        cfg["sport"],
        "total":        len(matches),
        "arb_count":    arb_count,
        "latency_ms":   latency,
        "bookmakers":   bk_used,
        "per_bookmaker": per_bk,
        "matches":      matches,    # full data for admin detail view
    })


@bp_sbo.route("/admin/match", methods=["POST"])
def admin_match():
    """
    Full market detail for one match — identical to GET /sbo/match/<id>
    but accepts POST body for convenience from the admin UI.

    POST body: { "betradar_id": str, "sport": str }
    """
    d           = request.get_json(force=True) or {}
    betradar_id = str(d.get("betradar_id") or "")
    sport_slug  = d.get("sport", "soccer")
    if not betradar_id:
        return jsonify({"error": "betradar_id required"}), 400
    # Delegate to GET handler by calling the function directly
    with request.environ["werkzeug.request"].environ.copy() as _env:
        pass
    # Call directly
    return get_match(betradar_id)


@bp_sbo.route("/admin/health")
def admin_health():
    """
    Quick connectivity check — pings each bookmaker with a minimal request
    (1 match, no full market fetch) and reports latency + status.

    Response:
    {
        "ok": true,
        "checked_at": "2026-03-16T10:00:00Z",
        "bookmakers": {
            "Sportpesa": {"ok": true,  "matches": 25, "latency_ms": 840},
            "Betika":    {"ok": true,  "matches": 24, "latency_ms": 1120},
            "Odibets":   {"ok": false, "matches": 0,  "latency_ms": 200, "error": "..."},
        }
    }
    """
    from .sbo_fetcher import (
        SPORT_CONFIG, SportpesaFetcher, BetikaFetcher, OdibetsFetcher,
        MarketMerger,
    )

    # Use soccer as the health-check sport
    cfg = next((c for c in SPORT_CONFIG if c["sport"] == "soccer"), SPORT_CONFIG[0])
    results: dict = {}

    # Sportpesa
    try:
        t0 = time.perf_counter()
        sp_matches = SportpesaFetcher.fetch_matches(cfg["sportpesa_id"], days=1, pages=1)
        results["Sportpesa"] = {
            "ok":         len(sp_matches) > 0,
            "matches":    len(sp_matches),
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        }
    except Exception as exc:
        results["Sportpesa"] = {"ok": False, "matches": 0, "latency_ms": None, "error": str(exc)}

    # Betika
    try:
        t0 = time.perf_counter()
        bt_matches = BetikaFetcher.fetch_matches(cfg["betika_id"], per_page=10, max_pages=1)
        results["Betika"] = {
            "ok":         len(bt_matches) > 0,
            "matches":    len(bt_matches),
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        }
    except Exception as exc:
        results["Betika"] = {"ok": False, "matches": 0, "latency_ms": None, "error": str(exc)}

    # Odibets (use first Betika betradar_id if available)
    od_ok = False
    try:
        t0 = time.perf_counter()
        bt_list    = BetikaFetcher.fetch_matches(cfg["betika_id"], per_page=5, max_pages=1)
        test_brid  = bt_list[0]["betradar_id"] if bt_list else None
        if test_brid:
            od_raw, _ = OdibetsFetcher.fetch_markets(test_brid)
            od_ok     = bool(od_raw)
            results["Odibets"] = {
                "ok":         od_ok,
                "matches":    1 if od_ok else 0,
                "latency_ms": int((time.perf_counter() - t0) * 1000),
            }
        else:
            results["Odibets"] = {"ok": False, "matches": 0, "latency_ms": None,
                                   "error": "No betradar_id available for test"}
    except Exception as exc:
        results["Odibets"] = {"ok": False, "matches": 0, "latency_ms": None, "error": str(exc)}

    all_ok = all(v.get("ok") for v in results.values())
    return jsonify({
        "ok":          all_ok,
        "checked_at":  time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "bookmakers":  results,
    })