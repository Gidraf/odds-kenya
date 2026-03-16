"""
app/api/odds_feed_api.py
=========================
Public odds API + admin monitor routes.

Public routes (BetB2B family):
    GET /odds/sport/<sport_name>          upcoming odds, all bookmakers merged
    GET /odds/live/<sport_name>           live odds, all bookmakers merged
    GET /odds/sports                      list sports with cached data
    GET /odds/bookmakers                  list bookmakers with cache stats

Public routes (Sportpesa / Betika / Odibets — unified via betradar ID):
    GET /odds/sbo/sports                  list supported sports
    GET /odds/sbo/sport/<sport>           upcoming odds merged from SP + Betika + Odibets
    GET /odds/sbo/arbitrage/<sport>       only matches with arbitrage opportunities

Admin routes (BetB2B):
    GET  /odds/admin/monitor              worker health, beat status, per-task table
    GET  /odds/admin/cache-keys           all Redis keys with TTL + match counts
    POST /odds/admin/probe                live fetch for 1 bookmaker (no cache write)
    POST /odds/admin/probe-all            live fetch ALL bookmakers concurrently
    POST /odds/admin/probe-all-sports     live fetch all bookmakers × all sports
    POST /odds/admin/trigger-harvest      kick off Celery harvest now
    GET  /odds/admin/task-result/<id>     Celery task status
    POST /odds/admin/debug-probe          deep diagnostic: raw Value[] breakdown
    POST /odds/admin/gamezi               GetGameZip full market fetch for one match

Admin routes (Sportpesa / Betika / Odibets):
    GET  /odds/admin/sbo-sports           list supported sports
    POST /odds/admin/sbo-probe            live fetch SP+Betika+Odibets for one sport
    POST /odds/admin/sbo-match            full market detail for one betradar match

Register:
    from app.api.odds_feed_api import bp_odds
    app.register_blueprint(bp_odds)
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

bp_odds = Blueprint("odds", __name__, url_prefix="/api/odds")


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Redis helpers ────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _redis():
    import redis
    from flask import current_app
    url  = current_app.config.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    return redis.Redis.from_url(
        f"{base}/2",
        decode_responses=False,
        socket_timeout=5, socket_connect_timeout=5,
    )


def _cache_get(key: str):
    try:
        raw = _redis().get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _cache_ttl(key: str) -> int:
    try: return _redis().ttl(key)
    except Exception: return -1


def _cache_keys(pattern: str) -> list[str]:
    try:
        return [k.decode() if isinstance(k, bytes) else k
                for k in _redis().keys(pattern)]
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# ─── DB helpers ───────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _load_active_bookmakers() -> list[dict]:
    try:
        from app.models.bookmakers_model import Bookmaker
        bms = Bookmaker.query.filter_by(is_active=True).all()
        result = []
        for bm in bms:
            cfg = getattr(bm, "harvest_config", None) or {}
            if isinstance(cfg, str):
                try: cfg = json.loads(cfg)
                except Exception: cfg = {}
            result.append({
                "id":          bm.id,
                "name":        bm.name or bm.domain,
                "domain":      bm.domain,
                "vendor_slug": getattr(bm, "vendor_slug", "betb2b") or "betb2b",
                "config":      cfg,
            })
        return result
    except Exception:
        return []


def _load_bookmaker(bk_id: int) -> dict | None:
    try:
        from app.models.bookmakers_model import Bookmaker
        bm = Bookmaker.query.get(bk_id)
        if not bm: return None
        cfg = getattr(bm, "harvest_config", None) or {}
        if isinstance(cfg, str):
            try: cfg = json.loads(cfg)
            except Exception: cfg = {}
        return {
            "id":          bm.id,
            "name":        bm.name or bm.domain,
            "domain":      bm.domain,
            "vendor_slug": getattr(bm, "vendor_slug", "betb2b") or "betb2b",
            "config":      cfg,
        }
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Cache merge ──────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _merge_cached(mode: str, sport: str) -> list[dict]:
    from .bookmaker_fetcher import merge_bookmaker_results
    sport_key = sport.lower().replace(" ", "_")
    keys      = _cache_keys(f"odds:{mode}:{sport_key}:*")
    all_bk: list[list[dict]] = []
    for key in keys:
        data = _cache_get(key)
        if data and data.get("matches"):
            all_bk.append(data["matches"])
    return merge_bookmaker_results(all_bk) if all_bk else []


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Public routes — BetB2B ───────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/sport/<sport_name>")
def get_upcoming(sport_name: str):
    market   = request.args.get("market")
    page     = max(1, request.args.get("page", 1, type=int))
    per_page = min(request.args.get("per_page", 50, type=int), 200)
    matches  = _merge_cached("upcoming", sport_name)
    if market: matches = [m for m in matches if market in m.get("markets", {})]
    total = len(matches); start = (page - 1) * per_page
    return jsonify({"sport": sport_name, "mode": "upcoming", "total": total,
                    "page": page, "per_page": per_page,
                    "pages": max(1, (total + per_page - 1) // per_page),
                    "matches": matches[start:start + per_page]})


@bp_odds.route("/live/<sport_name>")
def get_live(sport_name: str):
    market   = request.args.get("market")
    page     = max(1, request.args.get("page", 1, type=int))
    per_page = min(request.args.get("per_page", 50, type=int), 200)
    matches  = _merge_cached("live", sport_name)
    if market: matches = [m for m in matches if market in m.get("markets", {})]
    total = len(matches); start = (page - 1) * per_page
    return jsonify({"sport": sport_name, "mode": "live", "total": total,
                    "page": page, "per_page": per_page,
                    "pages": max(1, (total + per_page - 1) // per_page),
                    "matches": matches[start:start + per_page]})


@bp_odds.route("/sports")
def list_sports():
    keys = _cache_keys("odds:upcoming:*:*") + _cache_keys("odds:live:*:*")
    sports: set[str] = set()
    for k in keys:
        parts = k.split(":")
        if len(parts) >= 3: sports.add(parts[2].replace("_", " ").title())
    return jsonify({"sports": sorted(sports)})


@bp_odds.route("/bookmakers")
def list_active_bookmakers_cached():
    keys = _cache_keys("odds:upcoming:*:*") + _cache_keys("odds:live:*:*")
    bk_map: dict = {}
    for k in keys:
        data = _cache_get(k)
        if not data: continue
        bid = str(data.get("bookmaker_id", ""))
        if bid not in bk_map:
            bk_map[bid] = {
                "id":           data.get("bookmaker_id"),
                "name":         data.get("bookmaker_name") or bid,
                "sports":       [],
                "match_count":  0,
                "last_harvest": None,
            }
        bk_map[bid]["sports"].append(data.get("sport"))
        bk_map[bid]["match_count"]  += data.get("match_count", 0)
        bk_map[bid]["last_harvest"]  = data.get("harvested_at")
    return jsonify({"bookmakers": list(bk_map.values())})


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Admin monitor ────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/admin/monitor")
def monitor():
    try:
        heartbeat = _cache_get("worker_heartbeat") or {}
        beat_up   = _cache_get("task_status:beat_upcoming") or {}
        beat_live = _cache_get("task_status:beat_live") or {}

        task_keys = [k for k in _cache_keys("task_status:*") if "beat_" not in k]
        tasks = []
        for k in task_keys:
            d = _cache_get(k)
            if d: d["_key"] = k.replace("task_status:", ""); tasks.append(d)

        up_keys   = _cache_keys("odds:upcoming:*:*")
        live_keys = _cache_keys("odds:live:*:*")
        total_up   = sum((_cache_get(k) or {}).get("match_count", 0) for k in up_keys)
        total_live = sum((_cache_get(k) or {}).get("match_count", 0) for k in live_keys)

        worker_ok = False
        if hb_at := heartbeat.get("checked_at"):
            try:
                dt = datetime.fromisoformat(hb_at.replace("Z", "+00:00"))
                worker_ok = (datetime.now(timezone.utc) - dt).total_seconds() < 90
            except Exception: pass

        return jsonify({
            "worker_alive":     worker_ok,
            "heartbeat":        heartbeat,
            "beat_upcoming":    beat_up,
            "beat_live":        beat_live,
            "cached_upcoming":  len(up_keys),
            "cached_live":      len(live_keys),
            "total_matches":    total_up + total_live,
            "upcoming_matches": total_up,
            "live_matches":     total_live,
            "tasks": sorted(tasks, key=lambda t: t.get("updated_at", ""), reverse=True),
        })
    except Exception as exc:
        return jsonify({"error": str(exc), "worker_alive": False}), 500


@bp_odds.route("/admin/cache-keys")
def cache_keys_view():
    pattern = request.args.get("pattern", "odds:*")
    keys    = _cache_keys(pattern)
    rows    = []
    for k in sorted(keys):
        data = _cache_get(k)
        rows.append({
            "key":          k,
            "ttl":          _cache_ttl(k),
            "bookmaker":    (data or {}).get("bookmaker_name"),
            "sport":        (data or {}).get("sport"),
            "mode":         (data or {}).get("mode"),
            "match_count":  (data or {}).get("match_count", 0),
            "latency_ms":   (data or {}).get("latency_ms"),
            "harvested_at": (data or {}).get("harvested_at"),
        })
    return jsonify({"keys": rows, "total": len(rows)})


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Admin probe — single bookmaker ──────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/admin/probe", methods=["POST"])
def admin_probe():
    """
    Live fetch one bookmaker × one sport. Does NOT write to cache.
    Body: { bookmaker_id, sport, mode, page, page_size }
    """
    d     = request.json or {}
    bk_id = d.get("bookmaker_id")
    sport = d.get("sport", "Football")
    mode  = d.get("mode",  "live")
    if not bk_id: return jsonify({"ok": False, "error": "bookmaker_id required"}), 400

    bm = _load_bookmaker(int(bk_id))
    if not bm: return jsonify({"ok": False, "error": f"Bookmaker {bk_id} not found"}), 404

    from .bookmaker_fetcher import fetch_bookmaker
    t0 = time.perf_counter()
    try:
        matches = fetch_bookmaker(bm, sport_name=sport, mode=mode,
                                  page=d.get("page", 1), page_size=d.get("page_size", 40),
                                  timeout=25)
        latency = int((time.perf_counter() - t0) * 1000)
        return jsonify({"ok": True, "bookmaker": bm["name"], "sport": sport, "mode": mode,
                        "count": len(matches), "latency_ms": latency, "matches": matches[:10]})
    except Exception as exc:
        latency = int((time.perf_counter() - t0) * 1000)
        return jsonify({"ok": False, "error": str(exc), "latency_ms": latency}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Admin probe — all bookmakers concurrently ────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/admin/probe-all", methods=["POST"])
def admin_probe_all():
    """
    Live fetch ALL active bookmakers concurrently for one sport+mode.
    Returns merged odds with per-bookmaker stats.

    Body:
        {
          "sport":         "Football",
          "mode":          "live",
          "page_size":     40,
          "bookmaker_ids": [1,2,3]   ← optional, defaults to all active
        }

    Response:
        {
          "ok":          true,
          "sport":       "Football",
          "mode":        "live",
          "total":       46,
          "latency_ms":  1250,
          "matches":     [ ... ],
          "per_bookmaker": {
            "1xBet":   { "ok":true,  "count":46, "latency_ms":1120, "error":null },
            "22Bet":   { "ok":true,  "count":43, "latency_ms":980,  "error":null },
            "Helabet": { "ok":false, "count":0,  "latency_ms":200,  "error":"..." }
          },
          "errors": []
        }
    """
    d         = request.json or {}
    sport     = d.get("sport",     "Football")
    mode      = d.get("mode",      "live")
    page_size = d.get("page_size", 40)
    bk_ids    = d.get("bookmaker_ids")

    all_bks = _load_active_bookmakers()
    if not all_bks: return jsonify({"ok": False, "error": "No active bookmakers in DB"}), 404
    if bk_ids: all_bks = [b for b in all_bks if b["id"] in bk_ids]

    from .bookmaker_fetcher import fetch_all_bookmakers
    t0     = time.perf_counter()
    result = fetch_all_bookmakers(all_bks, sport_name=sport, mode=mode,
                                   page_size=page_size, timeout=20, max_workers=8)
    result["latency_ms"] = int((time.perf_counter() - t0) * 1000)
    result["ok"] = True
    return jsonify(result)


@bp_odds.route("/admin/probe-all-sports", methods=["POST"])
def admin_probe_all_sports():
    """
    Live fetch all bookmakers × all requested sports concurrently.

    Body:
        { "sports": ["Football","Basketball"], "mode": "live", "page_size": 40 }

    Response: { "Football": {probe-all result}, "Basketball": {...} }
    """
    d         = request.json or {}
    sports    = d.get("sports")
    mode      = d.get("mode",      "live")
    page_size = d.get("page_size", 40)

    all_bks = _load_active_bookmakers()
    if not all_bks: return jsonify({"ok": False, "error": "No active bookmakers in DB"}), 404

    from .bookmaker_fetcher import fetch_all_sports
    t0     = time.perf_counter()
    result = fetch_all_sports(all_bks, sports=sports, mode=mode,
                               page_size=page_size, timeout=20, max_workers=8)
    return jsonify({"ok": True, "latency_ms": int((time.perf_counter() - t0) * 1000),
                    "sports": result})


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Trigger harvest ──────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/admin/trigger-harvest", methods=["POST"])
def trigger_harvest():
    d    = request.json or {}
    mode = d.get("mode", "upcoming")
    try:
        from .celery_tasks import harvest_all_upcoming, harvest_all_live
        task = (harvest_all_live if mode == "live" else harvest_all_upcoming).apply_async()
        return jsonify({"ok": True, "task_id": task.id, "mode": mode})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@bp_odds.route("/admin/task-result/<task_id>")
def task_result(task_id: str):
    try:
        from .celery_tasks import celery
        r = celery.AsyncResult(task_id)
        return jsonify({"task_id": task_id, "state": r.state,
                        "result": r.result if r.ready() else None})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Admin: deep debug probe ─────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/admin/debug-probe", methods=["POST"])
def admin_debug_probe():
    """
    Deep diagnostic probe — returns full breakdown of what the API returns
    and where matches are being filtered out.

    POST: { "bookmaker_id": int, "sport": str, "mode": str }
    """
    from .bookmaker_fetcher import (
        _fetch, _parse_betb2b_item, _B2B_DOMAIN_CREDS,
        _B2B_MARKET_GROUPS, _B2B_OUTCOME_LABELS,
    )

    body  = request.get_json(force=True) or {}
    bk_id = body.get("bookmaker_id")
    sport = body.get("sport", "Football")
    mode  = body.get("mode",  "upcoming")

    if not bk_id:
        return jsonify({"error": "bookmaker_id required"}), 400

    bm = _load_bookmaker(int(bk_id))
    if not bm:
        return jsonify({"error": f"Bookmaker {bk_id} not found"}), 404

    domain  = bm["domain"]
    config  = bm.get("config") or {}
    headers = config.get("headers") or {}
    params  = dict(config.get("params") or {})

    # Inject hardcoded creds if missing
    if not params.get("partner"):
        creds = _B2B_DOMAIN_CREDS.get(domain.lower().lstrip("www."))
        if creds:
            params["partner"] = creds[0]
            if creds[1] and not params.get("gr"):
                params["gr"] = creds[1]

    if not params.get("partner"):
        return jsonify({"error": f"No partner ID for {domain}"}), 400

    lng     = params.get("lng", "en")
    gr      = params.get("gr", "")
    country = params.get("country", "87")
    partner = params.get("partner", "61")

    if mode == "live":
        base_url = f"https://{domain}/service-api/LiveFeed/Get1x2_VZip"
        ordered  = [("count", "200"), ("lng", lng)]
        if gr: ordered.append(("gr", gr))
        ordered += [("country", country), ("partner", partner), ("getEmpty", "true"),
                    ("virtualSports", "true"), ("noFilterBlockEvent", "true")]
    else:
        base_url = f"https://{domain}/service-api/LineFeed/Get1x2_VZip"
        ordered  = [("count", "200"), ("lng", lng), ("country", country),
                    ("partner", partner), ("getEmpty", "true"), ("virtualSports", "true")]
        if gr: ordered.append(("gr", gr))

    t0  = time.perf_counter()
    raw = _fetch(base_url, headers, ordered, 20)
    latency = int((time.perf_counter() - t0) * 1000)

    if raw is None:
        return jsonify({"ok": False, "domain": domain, "mode": mode, "latency_ms": latency,
                        "error": "No response — HTTP error, timeout or JSON parse failure"})
    if not isinstance(raw, dict):
        return jsonify({"ok": False, "domain": domain, "mode": mode, "latency_ms": latency,
                        "error": f"Response is {type(raw).__name__}, not dict"})
    if not raw.get("Success"):
        return jsonify({"ok": False, "domain": domain, "mode": mode, "latency_ms": latency,
                        "error": f"Success=False, ErrorCode={raw.get('ErrorCode')}",
                        "raw_keys": list(raw.keys())})

    value       = raw.get("Value") or []
    total_items = len(value)

    sport_counts:  dict[str, int]  = {}
    no_odds_items: list[dict]      = []
    good_items:    list[dict]      = []
    parse_errors:  list[str]       = []
    field_samples: dict[str, list] = {}

    for i, item in enumerate(value[:500]):
        if not isinstance(item, dict):
            parse_errors.append(f"item[{i}] is {type(item).__name__}"); continue
        try:
            parsed = _parse_betb2b_item(item, "live" if mode == "live" else "upcoming")
        except Exception as exc:
            parse_errors.append(f"item[{i}] parse exception: {exc}"); continue
        if not parsed:
            parse_errors.append(
                f"item[{i}] returned None — O1={item.get('O1')!r} O2={item.get('O2')!r}"
            ); continue

        sp = (parsed.get("sport") or "unknown").strip()
        sport_counts[sp] = sport_counts.get(sp, 0) + 1

        e_arr  = item.get("E") or []
        ae_arr = item.get("AE") or []
        for ev in (e_arr[:5] if isinstance(e_arr, list) else []):
            if isinstance(ev, dict):
                k = f"G{ev.get('G')}_T{ev.get('T')}"
                if k not in field_samples:
                    field_samples[k] = [ev.get("C"), ev.get("P")]

        summary = {
            "match":    f"{parsed['home_team']} v {parsed['away_team']}",
            "sport":    sp,
            "comp":     parsed.get("competition", ""),
            "start":    parsed.get("start_time", ""),
            "E_count":  len(e_arr),
            "AE_count": len(ae_arr),
            "markets":  list(parsed.get("markets", {}).keys()),
            "match_id": parsed.get("match_id", ""),
        }

        if not parsed.get("markets"):
            no_odds_items.append(summary)
        elif sp.lower() == sport.lower():
            good_items.append(summary)

    return jsonify({
        "ok": True, "domain": domain, "mode": mode,
        "sport_filter": sport, "latency_ms": latency,
        "summary": {
            "total_items":        total_items,
            "parse_errors":       len(parse_errors),
            "no_odds_count":      len(no_odds_items),
            f"{sport}_with_odds": len(good_items),
            f"{sport}_total":     sport_counts.get(sport, 0),
        },
        "sport_breakdown":        dict(sorted(sport_counts.items(), key=lambda x: -x[1])),
        "good_matches_sample":    good_items[:5],
        "no_odds_matches_sample": no_odds_items[:10],
        "parse_errors":           parse_errors[:10],
        "event_field_samples":    field_samples,
        "first_item_keys":        list(value[0].keys()) if value and isinstance(value[0], dict) else [],
        "first_item_preview": {
            k: value[0].get(k)
            for k in ["O1", "O2", "SE", "SN", "SI", "I", "L", "S", "E", "AE"]
            if value and isinstance(value[0], dict) and k in value[0]
        } if value else {},
    })


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Admin: GetGameZip full market fetch ─────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/admin/gamezi", methods=["POST"])
def admin_gamezi():
    """
    Fetch full match markets via GetGameZip for a single match + bookmaker.
    Called by MatchDetailView on click (and every 4s auto-refresh).

    POST body: { "bookmaker_id": int, "match_id": str }

    Returns GameZipResult shape:
    {
        "match_id":     str,
        "home_team":    str,
        "away_team":    str,
        "competition":  str,
        "win_probs":    { "P1": float, "PX": float, "P2": float } | null,
        "markets":      { market_key: { outcome: odds } },
        "market_count": int,
        "latency_ms":   int,
    }
    """
    from .bookmaker_fetcher import fetch_betb2b_markets, _B2B_DOMAIN_CREDS

    body     = request.get_json(force=True) or {}
    bk_id    = body.get("bookmaker_id")
    match_id = str(body.get("match_id") or "")

    if not bk_id or not match_id:
        return jsonify({"error": "bookmaker_id and match_id required"}), 400

    bm = _load_bookmaker(int(bk_id))
    if not bm:
        return jsonify({"error": "bookmaker not found"}), 404

    cfg     = bm.get("config") or {}
    headers = cfg.get("headers") or {}
    params  = dict(cfg.get("params") or {})
    domain  = bm.get("domain", "")

    # Inject hardcoded creds if missing
    if not params.get("partner"):
        creds = _B2B_DOMAIN_CREDS.get(domain.lower().lstrip("www."))
        if creds:
            params["partner"] = creds[0]
            if creds[1] and not params.get("gr"):
                params["gr"] = creds[1]

    if not params.get("partner"):
        return jsonify({"error": f"No partner ID for {domain}"}), 400

    t0     = time.perf_counter()
    result = fetch_betb2b_markets(domain, headers, params, match_id)
    latency = int((time.perf_counter() - t0) * 1000)

    if not result:
        return jsonify({"error": f"No data returned from {domain} for match {match_id}"}), 502

    result["latency_ms"] = latency
    return jsonify(result)


# ═══════════════════════════════════════════════════════════════════════════════
# ─── SBO helper: sport slug → SPORT_CONFIG entry ────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _sbo_sport_config(sport_slug: str) -> dict | None:
    """
    Find the SPORT_CONFIG entry matching a sport slug or common alias.
    Examples: 'soccer', 'football', 'Soccer', 'basketball', 'ice-hockey'
    """
    from .process_odds_data import SPORT_CONFIG
    slug = sport_slug.lower().strip().replace(" ", "-")
    for cfg in SPORT_CONFIG:
        if cfg["sport"] == slug:
            return cfg
    _aliases = {
        "football": "soccer",   "soccer": "soccer",
        "basketball": "basketball", "tennis": "tennis",
        "ice-hockey": "ice-hockey", "hockey": "ice-hockey",
        "volleyball": "volleyball", "cricket": "cricket",
        "rugby": "rugby",           "boxing": "boxing",
        "handball": "handball",     "mma": "mma",
        "table-tennis": "table-tennis", "esoccer": "esoccer",
        "snooker": "snooker",       "darts": "darts",
        "american-football": "american-football",
    }
    resolved = _aliases.get(slug)
    if resolved:
        for cfg in SPORT_CONFIG:
            if cfg["sport"] == resolved:
                return cfg
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Public routes — Sportpesa / Betika / Odibets (SBO) ──────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/sbo/sports")
def sbo_list_sports():
    """List all sports supported by the Sportpesa/Betika/Odibets engine."""
    from .process_odds_data import SPORT_CONFIG
    return jsonify({
        "sports": [
            {
                "sport":        c["sport"],
                "sportpesa_id": c.get("sportpesa_id"),
                "betika_id":    c.get("betika_id"),
                "odibets_id":   c.get("odibets_id"),
            }
            for c in SPORT_CONFIG
        ],
        "total": len(SPORT_CONFIG),
    })


@bp_odds.route("/sbo/sport/<sport_slug>")
def sbo_get_upcoming(sport_slug: str):
    """
    Fetch and merge upcoming odds from Sportpesa + Betika + Odibets.

    Query params:
        market=<key>     filter to only matches containing this market  e.g. "1x2"
        max=<int>        max matches to process (default 30, max 100)
        arb_only=1       only return matches with arbitrage opportunities
        page=<int>       page number (default 1)
        per_page=<int>   results per page (default 20, max 100)

    Response:
    {
        "sport":      str,
        "total":      int,
        "arb_count":  int,
        "latency_ms": int,
        "page":       int,
        "per_page":   int,
        "pages":      int,
        "bookmakers": ["Sportpesa", "Betika", "Odibets"],
        "meta": { "sportpesa_count": int, "betika_count": int, "odibets_count": int },
        "matches": [
            {
                "betradar_id":  str,
                "home_team":    str,
                "away_team":    str,
                "competition":  str,
                "sport":        str,
                "start_time":   str,
                "bookie_count": int,       ← how many bookmakers have this match
                "market_count": int,       ← number of distinct markets available

                # Best odds per outcome across all 3 bookmakers
                "best_odds": {
                    "1x2":            {"1":{"odd":1.73,"bookie":"Betika"},
                                       "X":{"odd":3.50,"bookie":"Sportpesa"},
                                       "2":{"odd":5.20,"bookie":"Odibets"}},
                    "over_under_2.5": {"over":{"odd":1.80,"bookie":"Odibets"},
                                       "under":{"odd":2.05,"bookie":"Betika"}},
                    "btts":           {"yes":{...}, "no":{...}},
                    ...
                },

                # Per-bookmaker summary (market count + timestamp)
                "bookmakers": {
                    "Sportpesa": {"fetched_at": "2026-03-16T10:00:00Z", "market_count": 12},
                    "Betika":    {"fetched_at": "2026-03-16T10:00:01Z", "market_count": 18},
                    "Odibets":   {"fetched_at": "2026-03-16T10:00:01Z", "market_count": 9},
                },

                # Arbitrage opportunities (empty list if none)
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
    cfg = _sbo_sport_config(sport_slug)
    if not cfg:
        return jsonify({"error": f"Unknown sport '{sport_slug}'. "
                                 f"See /api/odds/sbo/sports"}), 404

    market   = request.args.get("market")
    max_m    = min(int(request.args.get("max",      30)),  100)
    arb_only = request.args.get("arb_only", "0") in ("1", "true", "yes")
    page     = max(1, int(request.args.get("page",     1)))
    per_page = min(int(request.args.get("per_page", 20)), 100)

    from .process_odds_data import OddsAggregator
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
    total     = len(matches)
    start     = (page - 1) * per_page

    def _slim(m: dict) -> dict:
        """Trim response — keep best_odds + summary, drop raw_markets bulk."""
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
            "bookmakers":   {
                bk: {
                    "fetched_at":   v.get("fetched_at"),
                    "market_count": len(v.get("raw_markets", [])),
                }
                for bk, v in (m.get("bookmakers") or {}).items()
            },
            "arbitrage": m.get("arbitrage") or [],
        }

    return jsonify({
        "sport":      cfg["sport"],
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
        "matches": [_slim(m) for m in matches[start:start + per_page]],
    })


@bp_odds.route("/sbo/arbitrage/<sport_slug>")
def sbo_arbitrage(sport_slug: str):
    """
    Return only matches with arbitrage opportunities, sorted by best profit %.
    Shortcut for /sbo/sport/<slug>?arb_only=1

    Query params: max=<int> (default 50, max 100)
    """
    cfg = _sbo_sport_config(sport_slug)
    if not cfg:
        return jsonify({"error": f"Unknown sport '{sport_slug}'"}), 404

    max_m = min(int(request.args.get("max", 50)), 100)

    from .process_odds_data import OddsAggregator
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
                "best_odds":   m.get("best_odds") or {},
                "arbitrage":   m.get("arbitrage") or [],
            }
            for m in arbs
        ],
    })


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Admin routes — Sportpesa / Betika / Odibets ─────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/admin/sbo-sports")
def admin_sbo_sports():
    """List all sports supported by the SBO engine."""
    from .process_odds_data import SPORT_CONFIG
    return jsonify({
        "sports": [
            {
                "sport":        c["sport"],
                "sportpesa_id": c.get("sportpesa_id"),
                "betika_id":    c.get("betika_id"),
                "odibets_id":   c.get("odibets_id"),
            }
            for c in SPORT_CONFIG
        ]
    })


@bp_odds.route("/admin/sbo-probe", methods=["POST"])
def admin_sbo_probe():
    """
    Admin live probe — fetch Sportpesa + Betika + Odibets for one sport.
    Returns full unified match data (no cache write) for the admin monitor.

    POST body:
    {
        "sport":        "soccer",   ← sport slug (see /admin/sbo-sports)
        "max":          10,         ← max matches to process (default 10, max 50)
        "sp_only":      false,      ← only fetch Sportpesa
        "bt_only":      false,      ← only fetch Betika
        "full_markets": true        ← fetch full market book per match (slower)
    }

    Response: same shape as /sbo/sport/<slug> but full raw_markets included
    (no _slim() applied — designed for the admin detail view).
    """
    d          = request.get_json(force=True) or {}
    sport_slug = d.get("sport", "soccer")
    max_m      = min(int(d.get("max", 10)), 50)
    sp_only    = bool(d.get("sp_only",  False))
    bt_only    = bool(d.get("bt_only",  False))
    full_mkts  = bool(d.get("full_markets", True))

    cfg = _sbo_sport_config(sport_slug)
    if not cfg:
        return jsonify({"error": f"Unknown sport '{sport_slug}'"}), 404

    from .process_odds_data import OddsAggregator
    t0  = time.perf_counter()
    agg = OddsAggregator(
        cfg,
        fetch_full_sp_markets=(not bt_only) and full_mkts,
        fetch_full_bt_markets=(not sp_only) and full_mkts,
        fetch_od_markets=not sp_only and not bt_only,
    )
    matches = agg.run(max_matches=max_m)
    latency = int((time.perf_counter() - t0) * 1000)

    arb_count = sum(1 for m in matches if m.get("arbitrage"))
    bk_used   = (["Sportpesa"] if sp_only else
                 ["Betika"]    if bt_only else
                 ["Sportpesa", "Betika", "Odibets"])

    return jsonify({
        "ok":         True,
        "sport":      cfg["sport"],
        "total":      len(matches),
        "arb_count":  arb_count,
        "latency_ms": latency,
        "bookmakers": bk_used,
        "matches":    matches,          # full data for admin detail view
    })


@bp_odds.route("/admin/sbo-match", methods=["POST"])
def admin_sbo_match():
    """
    Fetch full market detail for ONE match from all 3 bookmakers using its betradar ID.
    Designed for MatchDetailView and the side-by-side arbitrage display.

    POST body: { "betradar_id": "68096896", "sport": "soccer" }

    Response:
    {
        "ok":              true,
        "latency_ms":      int,
        "betradar_id":     str,
        "home_team":       str,
        "away_team":       str,
        "competition":     str,
        "start_time":      str,
        "bookie_count":    int,
        "market_count":    int,

        # Full merged market list — all odds per outcome per bookmaker
        "unified_markets": {
            "1x2": {
                "1": [{"bookie":"Sportpesa","odd":1.61},
                      {"bookie":"Betika",   "odd":1.60},
                      {"bookie":"Odibets",  "odd":1.65}],
                "X": [...],
                "2": [...]
            },
            "over_under_2.5": { "over":[...], "under":[...] },
            ...
        },

        # Best odds summary (highest per outcome + which bookmaker)
        "best_odds": {
            "1x2": {
                "1": {"odd":1.65, "bookie":"Odibets"},
                "X": {"odd":3.60, "bookie":"Betika"},
                "2": {"odd":5.50, "bookie":"Sportpesa"}
            },
            ...
        },

        # Arbitrage opportunities detected
        "arbitrage": [
            {
                "market":       "1x2",
                "profit_pct":   1.23,
                "implied_prob": 98.77,
                "bets": [
                    {"outcome":"1","bookie":"Odibets",  "odd":1.65,"stake_pct":60.6},
                    {"outcome":"X","bookie":"Betika",   "odd":3.60,"stake_pct":27.8},
                    {"outcome":"2","bookie":"Sportpesa","odd":5.50,"stake_pct":18.2}
                ]
            }
        ],

        # Per-bookmaker raw market data
        "bookmakers": {
            "Sportpesa": {
                "raw_markets":  [ {name, sub_type_id, specifier, outcomes:[...]} ],
                "market_count": int,
                "fetched_at":   str,
            },
            "Betika":  { ... },
            "Odibets": { ... },
        }
    }
    """
    d           = request.get_json(force=True) or {}
    betradar_id = str(d.get("betradar_id") or "")
    sport_slug  = d.get("sport", "soccer")

    if not betradar_id:
        return jsonify({"error": "betradar_id required"}), 400

    from .process_odds_data import (
        SportpesaFetcher, BetikaFetcher, OdibetsFetcher,
        MarketMerger, ArbCalculator,
    )

    cfg   = _sbo_sport_config(sport_slug)
    sp_id = cfg.get("sportpesa_id") if cfg else None

    t0      = time.perf_counter()
    unified: dict = {}
    bk_data: dict = {}
    meta: dict    = {"betradar_id": betradar_id}

    # ── Sportpesa ─────────────────────────────────────────────────────────────
    # Need SP game_id for this match — search the upcoming list by betradar_id
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