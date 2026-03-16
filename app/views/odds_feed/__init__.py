"""
app/api/odds_feed_api.py
=========================
Public odds API + admin monitor routes.

Public routes:
    GET /odds/sport/<sport_name>          upcoming odds, all bookmakers merged
    GET /odds/live/<sport_name>           live odds, all bookmakers merged
    GET /odds/sports                      list sports with cached data
    GET /odds/bookmakers                  list bookmakers with cache stats

Admin routes:
    GET  /odds/admin/monitor              worker health, beat status, per-task table
    GET  /odds/admin/cache-keys           all Redis keys with TTL + match counts
    POST /odds/admin/probe                live fetch for 1 bookmaker (no cache write)
    POST /odds/admin/probe-all            live fetch ALL bookmakers concurrently
    POST /odds/admin/probe-all-sports     live fetch all bookmakers × all sports
    POST /odds/admin/trigger-harvest      kick off Celery harvest now

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
# ─── Public routes ────────────────────────────────────════════════════════════
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
                    "page": page, "per_page": per_page, "pages": max(1,(total+per_page-1)//per_page),
                    "matches": matches[start:start+per_page]})


@bp_odds.route("/live/<sport_name>")
def get_live(sport_name: str):
    market   = request.args.get("market")
    page     = max(1, request.args.get("page", 1, type=int))
    per_page = min(request.args.get("per_page", 50, type=int), 200)
    matches  = _merge_cached("live", sport_name)
    if market: matches = [m for m in matches if market in m.get("markets", {})]
    total = len(matches); start = (page - 1) * per_page
    return jsonify({"sport": sport_name, "mode": "live", "total": total,
                    "page": page, "per_page": per_page, "pages": max(1,(total+per_page-1)//per_page),
                    "matches": matches[start:start+per_page]})


@bp_odds.route("/sports")
def list_sports():
    keys = _cache_keys("odds:upcoming:*:*") + _cache_keys("odds:live:*:*")
    sports: set[str] = set()
    for k in keys:
        parts = k.split(":")
        if len(parts) >= 3: sports.add(parts[2].replace("_"," ").title())
    return jsonify({"sports": sorted(sports)})


@bp_odds.route("/bookmakers")
def list_active_bookmakers_cached():
    keys = _cache_keys("odds:upcoming:*:*") + _cache_keys("odds:live:*:*")
    bk_map: dict = {}
    for k in keys:
        data = _cache_get(k)
        if not data: continue
        bid = str(data.get("bookmaker_id",""))
        if bid not in bk_map:
            bk_map[bid] = {"id":data.get("bookmaker_id"),"name":data.get("bookmaker_name") or bid,"sports":[],"match_count":0,"last_harvest":None}
        bk_map[bid]["sports"].append(data.get("sport"))
        bk_map[bid]["match_count"] += data.get("match_count",0)
        bk_map[bid]["last_harvest"] = data.get("harvested_at")
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
            if d: d["_key"] = k.replace("task_status:",""); tasks.append(d)

        up_keys   = _cache_keys("odds:upcoming:*:*")
        live_keys = _cache_keys("odds:live:*:*")
        total_up   = sum((_cache_get(k) or {}).get("match_count",0) for k in up_keys)
        total_live = sum((_cache_get(k) or {}).get("match_count",0) for k in live_keys)

        worker_ok = False
        if hb_at := heartbeat.get("checked_at"):
            try:
                dt = datetime.fromisoformat(hb_at.replace("Z","+00:00"))
                worker_ok = (datetime.now(timezone.utc)-dt).total_seconds() < 90
            except Exception: pass

        return jsonify({
            "worker_alive": worker_ok, "heartbeat": heartbeat,
            "beat_upcoming": beat_up, "beat_live": beat_live,
            "cached_upcoming": len(up_keys), "cached_live": len(live_keys),
            "total_matches": total_up+total_live,
            "upcoming_matches": total_up, "live_matches": total_live,
            "tasks": sorted(tasks, key=lambda t: t.get("updated_at",""), reverse=True),
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
            "key": k, "ttl": _cache_ttl(k),
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
                                  page=d.get("page",1), page_size=d.get("page_size",40), timeout=25)
        latency = int((time.perf_counter()-t0)*1000)
        return jsonify({"ok":True,"bookmaker":bm["name"],"sport":sport,"mode":mode,
                        "count":len(matches),"latency_ms":latency,"matches":matches[:10]})
    except Exception as exc:
        latency = int((time.perf_counter()-t0)*1000)
        return jsonify({"ok":False,"error":str(exc),"latency_ms":latency}), 500


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
          "latency_ms":  1250,          ← wall-clock for all concurrent fetches
          "matches":     [ ... ],       ← merged, best odds per outcome flagged
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
    if not all_bks: return jsonify({"ok":False,"error":"No active bookmakers in DB"}), 404
    if bk_ids: all_bks = [b for b in all_bks if b["id"] in bk_ids]

    from .bookmaker_fetcher import fetch_all_bookmakers
    t0     = time.perf_counter()
    result = fetch_all_bookmakers(all_bks, sport_name=sport, mode=mode,
                                   page_size=page_size, timeout=20, max_workers=8)
    result["latency_ms"] = int((time.perf_counter()-t0)*1000)
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
    if not all_bks: return jsonify({"ok":False,"error":"No active bookmakers in DB"}), 404

    from .bookmaker_fetcher import fetch_all_sports
    t0     = time.perf_counter()
    result = fetch_all_sports(all_bks, sports=sports, mode=mode,
                               page_size=page_size, timeout=20, max_workers=8)
    return jsonify({"ok":True,"latency_ms":int((time.perf_counter()-t0)*1000),"sports":result})


# ═══════════════════════════════════════════════════════════════════════════════
# ─── Trigger harvest ──────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

@bp_odds.route("/admin/trigger-harvest", methods=["POST"])
def trigger_harvest():
    d    = request.json or {}
    mode = d.get("mode", "upcoming")
    try:
        from .celery_tasks import harvest_all_upcoming, harvest_all_live
        task = (harvest_all_live if mode=="live" else harvest_all_upcoming).apply_async()
        return jsonify({"ok":True,"task_id":task.id,"mode":mode})
    except Exception as exc:
        return jsonify({"ok":False,"error":str(exc)}), 500


@bp_odds.route("/admin/task-result/<task_id>")
def task_result(task_id: str):
    try:
        from .celery_tasks import celery
        r = celery.AsyncResult(task_id)
        return jsonify({"task_id":task_id,"state":r.state,"result":r.result if r.ready() else None})
    except Exception as exc:
        return jsonify({"error":str(exc)}), 500

# ─── Admin: deep debug probe ──────────────────────────────────────────────────

# ─── Admin: deep debug probe ──────────────────────────────────────────────────

@bp_odds.route('/admin/debug-probe', methods=['POST'])
def admin_debug_probe():
    """
    Deep diagnostic probe — returns full breakdown of what the API returns
    and where matches are being filtered out.

    POST: { "bookmaker_id": int, "sport": str, "mode": str }
    """
    import io, sys, traceback as tb
    from .bookmaker_fetcher import (
        _fetch, _parse_betb2b_item, _B2B_DOMAIN_CREDS,
        _B2B_MARKET_GROUPS, _B2B_OUTCOME_LABELS
    )

    body   = request.get_json(force=True) or {}
    bk_id  = body.get("bookmaker_id")
    sport  = body.get("sport", "Football")
    mode   = body.get("mode", "upcoming")

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

    # Build URL
    import urllib.parse as _up
    lng     = params.get("lng", "en")
    gr      = params.get("gr", "")
    country = params.get("country", "87")
    partner = params.get("partner", "61")

    if mode == "live":
        base_url = f"https://{domain}/service-api/LiveFeed/Get1x2_VZip"
        ordered  = [("count","200"),("lng",lng)]
        if gr: ordered.append(("gr",gr))
        ordered += [("country",country),("partner",partner),("getEmpty","true"),
                    ("virtualSports","true"),("noFilterBlockEvent","true")]
    else:
        base_url = f"https://{domain}/service-api/LineFeed/Get1x2_VZip"
        ordered  = [("count","200"),("lng",lng),("country",country),
                    ("partner",partner),("getEmpty","true"),("virtualSports","true")]
        if gr: ordered.append(("gr",gr))

    # Fire the request
    t0  = time.perf_counter()
    raw = _fetch(base_url, headers, ordered, 20)
    latency = int((time.perf_counter() - t0) * 1000)

    if raw is None:
        return jsonify({
            "ok": False, "domain": domain, "mode": mode,
            "error": "No response — HTTP error, timeout or JSON parse failure",
            "latency_ms": latency,
        })

    if not isinstance(raw, dict):
        return jsonify({
            "ok": False, "domain": domain, "mode": mode,
            "error": f"Response is {type(raw).__name__}, not dict",
            "latency_ms": latency,
        })

    if not raw.get("Success"):
        return jsonify({
            "ok": False, "domain": domain, "mode": mode,
            "error": f"Success=False, ErrorCode={raw.get('ErrorCode')}",
            "raw_keys": list(raw.keys()),
            "latency_ms": latency,
        })

    value = raw.get("Value") or []
    total_items = len(value)

    # Analyse every item
    sport_counts:  dict[str, int]    = {}
    no_odds_items: list[dict]        = []
    good_items:    list[dict]        = []
    parse_errors:  list[str]         = []
    field_samples: dict[str, list]   = {}   # E[] field keys seen

    for i, item in enumerate(value[:500]):   # cap at 500 to avoid huge responses
        if not isinstance(item, dict):
            parse_errors.append(f"item[{i}] is {type(item).__name__}")
            continue

        try:
            parsed = _parse_betb2b_item(item, "live" if mode=="live" else "upcoming")
        except Exception as exc:
            parse_errors.append(f"item[{i}] parse exception: {exc}")
            continue

        if not parsed:
            parse_errors.append(
                f"item[{i}] returned None — O1={item.get('O1')!r} O2={item.get('O2')!r}"
            )
            continue

        sp = (parsed.get("sport") or "unknown").strip()
        sport_counts[sp] = sport_counts.get(sp, 0) + 1

        has_markets = bool(parsed.get("markets"))
        e_arr       = item.get("E") or []
        ae_arr      = item.get("AE") or []

        # Collect E[] field key samples
        for ev in (e_arr[:5] if isinstance(e_arr, list) else []):
            if isinstance(ev, dict):
                k = f"G{ev.get('G')}_T{ev.get('T')}"
                if k not in field_samples:
                    field_samples[k] = [ev.get("C"), ev.get("P")]

        summary = {
            "match":      f"{parsed['home_team']} v {parsed['away_team']}",
            "sport":      sp,
            "comp":       parsed.get("competition",""),
            "start":      parsed.get("start_time",""),
            "E_count":    len(e_arr),
            "AE_count":   len(ae_arr),
            "markets":    list(parsed.get("markets",{}).keys()),
            "match_id":   parsed.get("match_id",""),
        }

        if not has_markets:
            no_odds_items.append(summary)
        elif sp.lower() == sport.lower():
            good_items.append(summary)

    # Top 10 sports
    top_sports = sorted(sport_counts.items(), key=lambda x: -x[1])

    return jsonify({
        "ok":          True,
        "domain":      domain,
        "mode":        mode,
        "sport_filter": sport,
        "latency_ms":  latency,

        "summary": {
            "total_items":         total_items,
            "parse_errors":        len(parse_errors),
            "total_with_markets":  sum(1 for k,v in sport_counts.items()
                                      if v > 0) ,  # approximate
            "no_odds_count":       len(no_odds_items),
            f"{sport}_with_odds":  len(good_items),
            f"{sport}_total":      sport_counts.get(sport, 0),
        },

        "sport_breakdown": dict(top_sports),

        # Samples
        "good_matches_sample":    good_items[:5],
        "no_odds_matches_sample": no_odds_items[:10],
        "parse_errors":           parse_errors[:10],
        "event_field_samples":    field_samples,

        # Raw first item for inspection
        "first_item_keys":        list(value[0].keys()) if value and isinstance(value[0], dict) else [],
        "first_item_preview": {
            k: value[0].get(k)
            for k in ["O1","O2","SE","SN","SI","I","L","S","E","AE"]
            if value and isinstance(value[0], dict) and k in value[0]
        } if value else {},
    })


# ─── Admin: GetGameZip full market fetch ──────────────────────────────────────

@bp_odds.route('/admin/gamezi', methods=['POST'])
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
    }
    """
    from app.models.bookmaker_models import Bookmaker
    from .bookmaker_fetcher import fetch_betb2b_markets

    body        = request.get_json(force=True) or {}
    bk_id       = body.get("bookmaker_id")
    match_id    = str(body.get("match_id") or "")

    if not bk_id or not match_id:
        return jsonify({"error": "bookmaker_id and match_id required"}), 400

    bm = Bookmaker.query.get(bk_id)
    if not bm:
        return jsonify({"error": "bookmaker not found"}), 404

    cfg     = bm.harvest_config or {}
    headers = cfg.get("headers") or {}
    params  = dict(cfg.get("params") or {})
    domain  = bm.domain or ""

    # Inject hardcoded creds if missing
    from .bookmaker_fetcher import _B2B_DOMAIN_CREDS
    if not params.get("partner"):
        creds = _B2B_DOMAIN_CREDS.get(domain.lower().lstrip("www."))
        if creds:
            params["partner"] = creds[0]
            if creds[1] and not params.get("gr"):
                params["gr"] = creds[1]

    if not params.get("partner"):
        return jsonify({"error": f"No partner ID for {domain}"}), 400

    import time
    t0 = time.perf_counter()
    result = fetch_betb2b_markets(domain, headers, params, match_id)
    latency = int((time.perf_counter() - t0) * 1000)

    if not result:
        return jsonify({"error": f"No data returned from {domain} for match {match_id}"}), 502

    result["latency_ms"] = latency
    return jsonify(result)