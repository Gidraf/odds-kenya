"""
app/tasks/odds_harvest_tasks.py
================================
Celery tasks for the vendor-based odds harvesting pipeline.

Task graph
──────────
  harvest_all_vendors          (Beat: every 90s)
    └─ harvest_vendor.si(vid)  (one per active vendor)
         └─ for each bookmaker config → probe API → parse → upsert
         └─ detect_arbitrage.si(vid, sport)

  harvest_single_bookmaker     (on-demand from dashboard)
  run_onboarding_tests         (auto-triggered after onboarding completes)

Registration (app/__init__.py):
    from app.tasks.odds_harvest_tasks import register_beat
    register_beat(celery)
"""
from __future__ import annotations

import json
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from celery import shared_task, chord, group
from celery.utils.log import get_task_logger

from app.extensions import db, celery, socketio
from app.models.vendor_template import VendorTemplate, BookmakerVendorConfig
from app.models.bookmakers_model import Bookmaker
from app.models.odds_model import (
    UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
    validate_parser_row,
)
from app.utils.probe_curl import _probe

log = get_task_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Beat Schedule
# ─────────────────────────────────────────────────────────────────────────────

def register_beat(celery_app):
    celery_app.conf.beat_schedule = {
        "harvest-all-vendors": {
            "task":     "odds.harvest_all_vendors",
            "schedule": 90,          # every 90 seconds
            "options":  {"expires": 80},
        },
    }
    celery_app.conf.timezone = "UTC"


# ─────────────────────────────────────────────────────────────────────────────
# Parser (inline — matches vendor_1xbet_parser.py)
# ─────────────────────────────────────────────────────────────────────────────

MARKET_MAP: dict[int, tuple[str, str]] = {
    1: ("1x2", "Home"), 2: ("1x2", "Draw"), 3: ("1x2", "Away"),
    4: ("Asian Handicap", "Home"), 5: ("Asian Handicap", "Away"),
    6: ("Total", "Over"), 7: ("Handicap", "Home"), 8: ("Handicap", "Away"),
    9: ("Total", "Over"), 10: ("Total", "Under"),
    11: ("1st Half Total", "Over"), 12: ("1st Half Total", "Under"),
    13: ("Total Corners", "Over"), 14: ("Total Corners", "Under"),
    180: ("2nd Half Total", "Over"), 181: ("2nd Half Total", "Under"),
    401: ("Match Winner", "Home"), 402: ("Match Winner", "Away"),
    3653: ("Double Chance", "Home/Draw"), 3654: ("Double Chance", "Draw/Away"),
    3655: ("Double Chance", "Home/Away"),
}

SPORT_MAP: dict[int, str] = {
    1: "Football", 2: "Ice Hockey", 3: "Basketball", 4: "Tennis",
    6: "Volleyball", 10: "Table Tennis", 66: "Cricket",
    7: "Baseball", 8: "Rugby", 23: "Handball", 20: "MMA", 19: "Boxing",
}

SPECIFIER_SENSITIVE = {
    "Total", "Handicap", "Asian Handicap",
    "1st Half Total", "2nd Half Total", "Total Corners"
}


def _parse_vendor_response(raw: Any, bookmaker_id: int, bookmaker_name: str) -> list[dict]:
    """Parse 1xBet-format response into flat OddsRow dicts."""
    items = raw if isinstance(raw, list) else raw.get("Value", [])
    rows = []
    now = int(time.time())

    for item in items:
        match_id   = str(item.get("I", ""))
        home       = item.get("O1") or item.get("O1E") or ""
        away       = item.get("O2") or item.get("O2E") or ""
        si_val     = item.get("SI")
        sport      = SPORT_MAP.get(si_val, item.get("SN") or "Unknown")
        league     = item.get("L") or item.get("LE") or ""
        country    = item.get("CN", "")
        start_ts   = item.get("S", 0)
        competition = f"{country} · {league}" if country and league else (league or country)

        sc      = item.get("SC") or {}
        fs      = sc.get("FS") or {}
        cps     = sc.get("CPS") or ""
        status  = cps if cps else ("live" if sc.get("CP") else "upcoming")
        score_h = fs.get("S1")
        score_a = fs.get("S2")

        for mkt in item.get("E") or []:
            t, odds, p = mkt.get("T"), mkt.get("C"), mkt.get("P")
            if not t or not odds or mkt.get("B"):
                continue
            try:
                price = float(odds)
            except (TypeError, ValueError):
                continue
            if price <= 1.01:
                continue

            entry = MARKET_MAP.get(t)
            market_name = entry[0] if entry else f"Market_{t}"
            selection   = entry[1] if entry else "Selection"

            rows.append({
                "match_id":        match_id,
                "home_team":       home,
                "away_team":       away,
                "sport":           sport,
                "competition":     competition,
                "country":         country,
                "start_time":      start_ts,
                "match_status":    status,
                "score_home":      score_h,
                "score_away":      score_a,
                "market_name":     market_name,
                "specifier":       str(p) if p is not None else "",
                "selection_name":  selection,
                "selection_price": price,
                "bookmaker_id":    bookmaker_id,
                "bookmaker_name":  bookmaker_name,
                "fetched_at":      now,
            })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Classification
# ─────────────────────────────────────────────────────────────────────────────

def classify_odds(rows: list[dict]) -> dict:
    """
    Group flat rows into:
    { sport: { match_key: { meta, books: { bk_id: { market_key: { sel: price } } } } } }

    match_key = "{home}|{away}|{start_bucket}"  (cross-bookmaker canonical)
    """
    bundle: dict = defaultdict(lambda: defaultdict(lambda: {"meta": {}, "books": defaultdict(lambda: defaultdict(dict))}))

    for row in rows:
        sport   = row["sport"]
        home    = row["home_team"].lower().strip()
        away    = row["away_team"].lower().strip()
        bucket  = (row["start_time"] // 300) * 300
        mkey    = f"{home}|{away}|{bucket}"

        entry = bundle[sport][mkey]
        if not entry["meta"]:
            entry["meta"] = {
                "home_team":    row["home_team"],
                "away_team":    row["away_team"],
                "start_time":   row["start_time"],
                "competition":  row["competition"],
                "match_status": row["match_status"],
                "score_home":   row["score_home"],
                "score_away":   row["score_away"],
                "bookmaker_ids": [],
            }

        bk_id = row["bookmaker_id"]
        if bk_id not in entry["meta"]["bookmaker_ids"]:
            entry["meta"]["bookmaker_ids"].append(bk_id)

        mk = (f"{row['market_name']}|{row['specifier']}"
              if row["specifier"] and row["market_name"] in SPECIFIER_SENSITIVE
              else row["market_name"])
        entry["books"][bk_id][mk][row["selection_name"]] = row["selection_price"]

    # Convert defaultdicts
    def _d(x):
        if isinstance(x, defaultdict):
            return {k: _d(v) for k, v in x.items()}
        return x

    return _d(bundle)


# ─────────────────────────────────────────────────────────────────────────────
# Arbitrage Engine
# ─────────────────────────────────────────────────────────────────────────────

def find_arbitrage(
    bundle: dict,
    bk_names: dict[int, str] | None = None,
    min_profit_pct: float = 0.3,
) -> list[dict]:
    """
    Scan bundle for guaranteed-profit cross-bookmaker opportunities.
    Returns list sorted by profit_pct descending.
    """
    bk_names = bk_names or {}
    arbs: list[dict] = []

    for sport, matches in bundle.items():
        for mkey, mdata in matches.items():
            meta  = mdata.get("meta", {})
            books = mdata.get("books", {})

            if len(books) < 2:
                continue

            # Collect all market keys across all bookmakers
            all_mks: set[str] = set()
            for bk_markets in books.values():
                all_mks.update(bk_markets.keys())

            for mk in all_mks:
                mname, spec = (mk.split("|", 1) + [""])[:2] if "|" in mk else (mk, "")

                # Collect all selections across all bks for this market
                all_sels: set[str] = set()
                for bk_id, bk_markets in books.items():
                    all_sels.update(bk_markets.get(mk, {}).keys())

                if len(all_sels) < 2:
                    continue

                # Best price per selection across all bks
                best: dict[str, tuple[float, int]] = {}
                for sel in all_sels:
                    for bk_id, bk_markets in books.items():
                        price = bk_markets.get(mk, {}).get(sel)
                        if price and price > 1.0:
                            if sel not in best or price > best[sel][0]:
                                best[sel] = (price, bk_id)

                if len(best) < len(all_sels):
                    continue  # incomplete coverage

                margin = sum(1.0 / p for p, _ in best.values())
                if margin >= 1.0:
                    continue

                profit_pct = round((1.0 / margin - 1.0) * 100, 3)
                if profit_pct < min_profit_pct:
                    continue

                legs = []
                for sel, (price, bk_id) in best.items():
                    stake_pct = round((1.0 / price / margin) * 100, 2)
                    legs.append({
                        "selection":      sel,
                        "price":          price,
                        "bookmaker_id":   bk_id,
                        "bookmaker_name": bk_names.get(bk_id, f"BK#{bk_id}"),
                        "stake_pct":      stake_pct,
                    })

                arbs.append({
                    "sport":       sport,
                    "home_team":   meta.get("home_team", ""),
                    "away_team":   meta.get("away_team", ""),
                    "start_time":  meta.get("start_time"),
                    "competition": meta.get("competition", ""),
                    "market_name": mname,
                    "specifier":   spec,
                    "margin":      round(margin, 5),
                    "profit_pct":  profit_pct,
                    "legs":        legs,
                    "found_at":    int(time.time()),
                })

    return sorted(arbs, key=lambda x: x["profit_pct"], reverse=True)


# ─────────────────────────────────────────────────────────────────────────────
# DB Upsert
# ─────────────────────────────────────────────────────────────────────────────

def _upsert_rows(rows: list[dict], bookmaker_id: int) -> dict:
    """
    Upsert flat OddsRows into UnifiedMatch + BookmakerMatchOdds + history.
    Returns stats dict.
    """
    stats = defaultdict(int)

    for row in rows:
        try:
            # Validate minimal contract
            required = {"home_team", "away_team", "market_name", "selection_name", "selection_price"}
            if not required.issubset(row.keys()):
                stats["skipped"] += 1
                continue

            price = float(row["selection_price"])
            if price <= 1.0:
                stats["skipped"] += 1
                continue

            # Build cross-bookmaker match key
            home   = (row["home_team"] or "").strip().lower()
            away   = (row["away_team"] or "").strip().lower()
            bucket = (int(row.get("start_time") or 0) // 300) * 300
            canon_id = f"{home}|{away}|{bucket}"

            # UnifiedMatch upsert
            um = UnifiedMatch.query.filter_by(parent_match_id=canon_id).first()
            if not um:
                from datetime import datetime
                start_dt = (
                    datetime.fromtimestamp(int(row["start_time"]), tz=timezone.utc)
                    if row.get("start_time") else None
                )
                um = UnifiedMatch(
                    parent_match_id  = canon_id,
                    home_team_name   = row["home_team"],
                    away_team_name   = row["away_team"],
                    sport_name       = row.get("sport", "Unknown"),
                    competition_name = row.get("competition", ""),
                    start_time       = start_dt,
                    status           = "LIVE" if row.get("match_status") == "live" else "PRE_MATCH",
                    markets_json     = {},
                )
                db.session.add(um)
                db.session.flush()
                stats["matches_created"] += 1
            else:
                stats["matches_updated"] += 1

            # BookmakerMatchOdds upsert
            bmo = BookmakerMatchOdds.query.filter_by(
                match_id=um.id, bookmaker_id=bookmaker_id
            ).first()
            if not bmo:
                bmo = BookmakerMatchOdds(
                    match_id=um.id, bookmaker_id=bookmaker_id,
                    markets_json={},
                )
                db.session.add(bmo)
                db.session.flush()

            market    = row["market_name"]
            specifier = row.get("specifier") or None
            selection = row["selection_name"]

            changed, old_price = bmo.upsert_selection(market, specifier, selection, price)
            um.upsert_bookmaker_price(market, specifier, selection, price, bookmaker_id)

            if changed:
                if old_price is None:
                    stats["odds_new"] += 1
                else:
                    stats["odds_changed"] += 1
                    delta = price - old_price
                    hist = BookmakerOddsHistory(
                        bmo_id=bmo.id, bookmaker_id=bookmaker_id,
                        match_id=um.id, market=market,
                        specifier=str(specifier) if specifier else None,
                        selection=selection, old_price=old_price,
                        new_price=price, price_delta=delta,
                    )
                    db.session.add(hist)
            else:
                stats["odds_unchanged"] += 1

        except Exception as e:
            stats["errors"] += 1
            log.warning(f"Row upsert error: {e}")
            continue

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        log.error(f"Commit failed: {e}")
        stats["commit_error"] = 1

    return dict(stats)


def _emit(channel: str, data: dict):
    """Emit to Socket.IO if available."""
    try:
        socketio.emit(channel, data, namespace="/odds")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Celery Tasks
# ─────────────────────────────────────────────────────────────────────────────

@shared_task(name="odds.harvest_all_vendors", bind=True, max_retries=2)
def harvest_all_vendors(self):
    """Scheduled every 90s — fan-out to per-vendor tasks."""
    vendors = VendorTemplate.query.filter(
        VendorTemplate.list_url_template.isnot(None)
    ).all()

    if not vendors:
        return {"vendors": 0}

    job = group(harvest_vendor.si(v.id) for v in vendors)
    job.apply_async()
    return {"vendors": len(vendors), "triggered_at": int(time.time())}


@shared_task(name="odds.harvest_vendor", bind=True, max_retries=3)
def harvest_vendor(self, vendor_id: int):
    """Harvest all bookmakers for one vendor, then detect arbitrage."""
    vendor = VendorTemplate.query.get(vendor_id)
    if not vendor:
        return {"error": f"Vendor {vendor_id} not found"}

    configs = list(vendor.bookmaker_configs.filter_by(is_active=True))
    if not configs:
        return {"vendor": vendor.slug, "bookmakers": 0}

    all_rows: list[dict]  = []
    bk_names: dict[int, str] = {}
    stats_per_bk: list[dict] = []

    for cfg in configs:
        bk_name = cfg.bookmaker.name if cfg.bookmaker else f"BK#{cfg.bookmaker_id}"
        bk_names[cfg.bookmaker_id] = bk_name

        try:
            base_url, params = vendor.resolve_url(
                vendor.list_url_template or "", None, cfg
            )
            result = _probe(
                url=base_url, method="GET",
                headers={}, params=params,
                url_raw=vendor.list_url_template or "",
            )

            if not result.get("ok"):
                log.warning(f"[{vendor.slug}] {bk_name} probe failed: {result.get('error')}")
                _emit("harvest_update", {
                    "vendor_id": vendor_id,
                    "bookmaker": bk_name,
                    "status": "error",
                    "error": result.get("error"),
                })
                stats_per_bk.append({"bookmaker": bk_name, "status": "probe_error"})
                continue

            raw = result["parsed"]
            rows = _parse_vendor_response(raw, cfg.bookmaker_id, bk_name)

            if not rows:
                log.warning(f"[{vendor.slug}] {bk_name}: parser returned 0 rows")
                stats_per_bk.append({"bookmaker": bk_name, "status": "empty", "rows": 0})
                continue

            all_rows.extend(rows)
            upsert_stats = _upsert_rows(rows, cfg.bookmaker_id)

            _emit("harvest_update", {
                "vendor_id":    vendor_id,
                "bookmaker":    bk_name,
                "bookmaker_id": cfg.bookmaker_id,
                "status":       "ok",
                "row_count":    len(rows),
                "stats":        upsert_stats,
            })

            stats_per_bk.append({
                "bookmaker": bk_name,
                "status":    "ok",
                "rows":      len(rows),
                **upsert_stats,
            })

        except Exception as exc:
            log.error(f"[{vendor.slug}] {bk_name} error: {exc}\n{traceback.format_exc()}")
            stats_per_bk.append({"bookmaker": bk_name, "status": "exception", "error": str(exc)})

    # ── Classify + Arbitrage ──────────────────────────────────────────────────
    arbs: list[dict] = []
    bundle: dict = {}

    if all_rows and len(bk_names) >= 2:
        bundle = classify_odds(all_rows)
        arbs   = find_arbitrage(bundle, bk_names, min_profit_pct=0.3)

        if arbs:
            # Cache arbs in Redis/memory for fast API reads
            _cache_arbitrage(vendor_id, arbs)
            _emit("arbitrage_found", {
                "vendor_id":  vendor_id,
                "count":      len(arbs),
                "best_profit": arbs[0]["profit_pct"],
                "arbs":       arbs[:20],  # top 20 to socket
            })

    total_rows = len(all_rows)
    log.info(
        f"[{vendor.slug}] Harvest complete — {total_rows} rows, "
        f"{len(arbs)} arbs from {len(configs)} bookmakers"
    )

    return {
        "vendor":      vendor.slug,
        "bookmakers":  len(configs),
        "total_rows":  total_rows,
        "arb_count":   len(arbs),
        "stats":       stats_per_bk,
    }


@shared_task(name="odds.harvest_single_bookmaker", bind=True)
def harvest_single_bookmaker(self, vendor_id: int, bookmaker_id: int):
    """On-demand harvest for one bookmaker — called from dashboard."""
    vendor = VendorTemplate.query.get(vendor_id)
    cfg    = BookmakerVendorConfig.query.filter_by(
        vendor_id=vendor_id, bookmaker_id=bookmaker_id
    ).first()

    if not vendor or not cfg:
        return {"error": "Vendor or bookmaker config not found"}

    bk_name = cfg.bookmaker.name if cfg.bookmaker else f"BK#{bookmaker_id}"
    base_url, params = vendor.resolve_url(vendor.list_url_template or "", None, cfg)

    result = _probe(
        url=base_url, method="GET", headers={}, params=params,
        url_raw=vendor.list_url_template or "",
    )

    if not result.get("ok"):
        return {"status": "error", "error": result.get("error")}

    rows  = _parse_vendor_response(result["parsed"], bookmaker_id, bk_name)
    stats = _upsert_rows(rows, bookmaker_id)

    _emit("harvest_update", {
        "bookmaker": bk_name, "bookmaker_id": bookmaker_id,
        "status": "ok", "row_count": len(rows), "stats": stats,
    })

    return {"bookmaker": bk_name, "rows": len(rows), **stats}


@shared_task(name="odds.run_onboarding_tests", bind=True)
def run_onboarding_tests(self, vendor_id: int, bookmaker_id: int):
    """
    Auto-triggered when a bookmaker completes onboarding.
    Probes every sport in the vendor config and validates the parser.
    Emits progress via Socket.IO for the human-in-the-loop UI.
    """
    vendor = VendorTemplate.query.get(vendor_id)
    cfg    = BookmakerVendorConfig.query.filter_by(
        vendor_id=vendor_id, bookmaker_id=bookmaker_id
    ).first()

    if not vendor or not cfg:
        return {"error": "Vendor or config not found"}

    bk_name      = cfg.bookmaker.name if cfg.bookmaker else f"BK#{bookmaker_id}"
    sport_params = cfg.resolved_sport_params()
    results: list[dict] = []
    all_passed = True

    _emit("onboarding_test_start", {
        "bookmaker": bk_name,
        "vendor":    vendor.slug,
        "sports":    list(sport_params.keys()),
    })

    for sport_name, gr_value in sport_params.items():
        try:
            base_url, params = vendor.resolve_url(
                vendor.list_url_template or "", sport_name, cfg
            )
            params["gr"] = str(gr_value)

            result = _probe(
                url=base_url, method="GET", headers={}, params=params,
                url_raw=vendor.list_url_template or "",
            )

            if not result.get("ok"):
                results.append({
                    "sport": sport_name, "gr": gr_value,
                    "passed": False, "error": result.get("error"),
                    "row_count": 0,
                })
                all_passed = False
                _emit("onboarding_test_sport", {
                    "bookmaker": bk_name, "sport": sport_name,
                    "passed": False, "error": result.get("error"),
                })
                continue

            rows = _parse_vendor_response(
                result["parsed"], bookmaker_id, bk_name
            )

            # Filter to this sport
            sport_rows = [r for r in rows if r.get("sport") == sport_name]

            # Validate parser contract on first 3 rows
            validation_errors = []
            for row in sport_rows[:3]:
                # Convert to minimal contract keys
                test_row = {
                    "parent_match_id": row.get("match_id", "test"),
                    "home_team":       row.get("home_team", ""),
                    "away_team":       row.get("away_team", ""),
                    "start_time":      str(row.get("start_time", "")),
                    "sport":           row.get("sport"),
                    "competition":     row.get("competition"),
                    "market":          row.get("market_name"),
                    "selection":       row.get("selection_name"),
                    "price":           row.get("selection_price"),
                    "specifier":       row.get("specifier") or None,
                }
                try:
                    validate_parser_row(test_row)
                except ValueError as e:
                    validation_errors.append(str(e)[:120])

            passed = len(rows) > 0 and len(validation_errors) == 0
            all_passed = all_passed and passed

            results.append({
                "sport":             sport_name,
                "gr":                gr_value,
                "passed":            passed,
                "row_count":         len(sport_rows),
                "total_rows":        len(rows),
                "sample_match":      (sport_rows[0] if sport_rows else rows[0]) if rows else None,
                "validation_errors": validation_errors,
                "latency_ms":        result.get("latency_ms"),
            })

            _emit("onboarding_test_sport", {
                "bookmaker": bk_name, "sport": sport_name,
                "passed": passed, "row_count": len(sport_rows),
                "validation_errors": validation_errors,
            })

            # If all sports passed so far, run a partial harvest
            if passed:
                _upsert_rows(sport_rows[:50], bookmaker_id)

        except Exception as exc:
            results.append({
                "sport": sport_name, "gr": gr_value,
                "passed": False, "error": str(exc),
            })
            all_passed = False

    # Update vendor config with test results
    try:
        probe_cache = dict(cfg.last_probe)
        probe_cache["onboarding_test"] = {
            "all_passed": all_passed,
            "ts":         int(time.time()),
            "results":    results,
        }
        cfg.last_probe_json = json.dumps(probe_cache)
        db.session.commit()
    except Exception:
        pass

    _emit("onboarding_test_complete", {
        "bookmaker":  bk_name,
        "all_passed": all_passed,
        "results":    results,
    })

    return {
        "bookmaker":  bk_name,
        "all_passed": all_passed,
        "results":    results,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Arbitrage Cache (Redis-backed)
# ─────────────────────────────────────────────────────────────────────────────

_ARB_CACHE: dict = {}   # fallback in-process cache when Redis unavailable


def _cache_arbitrage(vendor_id: int, arbs: list[dict]):
    """Store arbitrage opportunities keyed by vendor and sport."""
    by_sport: dict = defaultdict(list)
    for arb in arbs:
        by_sport[arb["sport"]].append(arb)

    try:
        from app.extensions import redis_client
        key = f"arb:v{vendor_id}"
        redis_client.setex(key, 120, json.dumps(arbs))
        for sport, sarbs in by_sport.items():
            redis_client.setex(f"arb:v{vendor_id}:s{sport}", 120, json.dumps(sarbs))
    except Exception:
        _ARB_CACHE[f"v{vendor_id}"] = {"arbs": arbs, "by_sport": dict(by_sport), "ts": int(time.time())}


def get_cached_arbitrage(vendor_id: int | None = None, sport: str | None = None) -> list[dict]:
    """Read arbitrage from cache — used by odds API."""
    try:
        from app.extensions import redis_client
        if vendor_id and sport:
            data = redis_client.get(f"arb:v{vendor_id}:s{sport}")
        elif vendor_id:
            data = redis_client.get(f"arb:v{vendor_id}")
        else:
            # Merge all vendors
            keys = redis_client.keys("arb:v*")
            all_arbs = []
            for k in keys:
                if b":" not in k[5:]:  # top-level vendor keys only
                    raw = redis_client.get(k)
                    if raw:
                        all_arbs.extend(json.loads(raw))
            return sorted(all_arbs, key=lambda x: x["profit_pct"], reverse=True)

        return json.loads(data) if data else []
    except Exception:
        # Fallback to in-process cache
        if vendor_id:
            cached = _ARB_CACHE.get(f"v{vendor_id}")
            if cached:
                if sport:
                    return cached["by_sport"].get(sport, [])
                return cached["arbs"]
        return []