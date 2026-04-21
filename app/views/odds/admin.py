"""
app/api/admin.py
=================
Admin endpoints for triggering harvests and diagnosing BK connectivity.
No auth — add @require_admin when you're ready for production.

GET  /admin/harvest/run?bk=bt&sport=soccer   Force-run one BK harvest right now
GET  /admin/harvest/run/all                   Force-run all BKs for all sports
GET  /admin/harvest/status                    Show Redis state per BK per sport
GET  /admin/test/fetch?bk=bt&sport=soccer     Test if BK API is reachable + sample
GET  /admin/redis/keys?sport=soccer           All Redis keys for a sport
"""
from __future__ import annotations

import json
import time
import traceback
from datetime import datetime, timezone

from flask import request, Blueprint

bp_admin = Blueprint("admin-debug", __name__, url_prefix="/admin")

_LOCAL_BKS = ["sp", "bt", "od"]
_B2B_BKS   = ["1xbet", "22bet", "betwinner", "melbet", "megapari", "helabet", "paripesa"]
_ALL_BKS   = _LOCAL_BKS + _B2B_BKS

_ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby", "ice-hockey",
    "volleyball", "handball", "table-tennis", "baseball", "mma",
    "boxing", "darts", "american-football", "esoccer",
]

_LOCAL_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
]


def _get_redis():
    from app.workers.celery_tasks import _redis
    return _redis()


def _ok(data: dict):
    from flask import jsonify
    r = jsonify(data)
    r.headers["Access-Control-Allow-Origin"] = "*"
    return r


# ══════════════════════════════════════════════════════════════════════════════
# HARVEST STATUS — what's in Redis right now
# ══════════════════════════════════════════════════════════════════════════════

@bp_admin.route("/harvest/status", methods=["GET"])
def harvest_status():
    """
    Show Redis state for every BK × sport combo.
    Tells you exactly which harvesters have run and when.
    """
    r       = _get_redis()
    sport   = request.args.get("sport", "soccer")
    results = {}

    # Key patterns each harvester writes to
    key_patterns = {
        "sp":  [f"sp:upcoming:{sport}", f"odds:sp:upcoming:{sport}"],
        "bt":  [f"bt:upcoming:{sport}", f"odds:bt:upcoming:{sport}"],
        "od":  [f"od:upcoming:{sport}", f"odds:od:upcoming:{sport}"],
        "b2b": [f"b2b:upcoming:{sport}", f"odds:b2b:upcoming:{sport}"],
    }
    for bk in _B2B_BKS:
        key_patterns[bk] = [
            f"odds:b2b:{bk}:upcoming:{sport}",
            f"odds:{bk}:upcoming:{sport}",
        ]
    key_patterns["unified"] = [
        f"odds:unified:upcoming:{sport}",
        f"odds:upcoming:all:{sport}",
    ]

    for bk, keys in key_patterns.items():
        bk_result = {"keys": {}, "match_count": 0, "has_data": False}
        for key in keys:
            raw = r.get(key)
            if raw:
                count = 0
                harvested_at = None
                try:
                    d  = json.loads(raw)
                    ms = d if isinstance(d, list) else d.get("matches", [])
                    count        = len(ms)
                    harvested_at = (d if isinstance(d, dict) else {}).get("harvested_at")
                except Exception:
                    pass
                bk_result["keys"][key] = {
                    "ttl":          r.ttl(key),
                    "match_count":  count,
                    "bytes":        len(raw),
                    "harvested_at": harvested_at,
                }
                bk_result["match_count"] = max(bk_result["match_count"], count)
                bk_result["has_data"]    = True
            else:
                bk_result["keys"][key] = None

        results[bk] = bk_result

    total_matches = max(
        results.get("unified", {}).get("match_count", 0),
        results.get("sp", {}).get("match_count", 0),
    )

    return _ok({
        "sport":         sport,
        "bks":           results,
        "total_matches": total_matches,
        "ts":            time.time(),
        "diagnosis": {
            "sp_ok":      results["sp"]["has_data"],
            "bt_ok":      results["bt"]["has_data"],
            "od_ok":      results["od"]["has_data"],
            "b2b_ok":     results["b2b"]["has_data"],
            "unified_ok": results["unified"]["has_data"],
            "advice": (
                "All BKs have data ✓" if all([
                    results["sp"]["has_data"], results["bt"]["has_data"],
                    results["od"]["has_data"],
                ]) else
                "Only SP has data → BT/OD harvest not running or API failing. "
                "Use /admin/harvest/run?bk=bt&sport=soccer to trigger manually, "
                "then /admin/test/fetch?bk=bt&sport=soccer to test the API directly."
            ),
        },
    })


# ══════════════════════════════════════════════════════════════════════════════
# HARVEST TRIGGER — run right now, synchronously, return result
# ══════════════════════════════════════════════════════════════════════════════

@bp_admin.route("/harvest/run", methods=["GET", "POST"])
def harvest_run():
    """
    Force-run one BK harvest NOW and return the result.
    This calls the fetch function DIRECTLY — no Celery, no queue.

    GET /admin/harvest/run?bk=bt&sport=soccer
    GET /admin/harvest/run?bk=od&sport=soccer
    GET /admin/harvest/run?bk=sp&sport=soccer
    GET /admin/harvest/run?bk=1xbet&sport=soccer
    """
    bk    = request.args.get("bk", "bt").lower()
    sport = request.args.get("sport", "soccer")
    t0    = time.perf_counter()

    try:
        if bk == "sp":
            result = _run_sp(sport)
        elif bk == "bt":
            result = _run_bt(sport)
        elif bk == "od":
            result = _run_od(sport)
        elif bk in _B2B_BKS:
            result = _run_b2b_single(bk, sport)
        elif bk == "b2b":
            result = _run_b2b_all(sport)
        else:
            return _ok({"ok": False, "error": f"Unknown bk: {bk}. Choose from: {_ALL_BKS}"})

        latency = int((time.perf_counter() - t0) * 1000)
        return _ok({**result, "latency_ms": latency, "ts": time.time()})

    except Exception as e:
        return _ok({
            "ok": False, "bk": bk, "sport": sport,
            "error": str(e),
            "traceback": traceback.format_exc()[-2000:],
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })


@bp_admin.route("/harvest/run/all", methods=["GET", "POST"])
def harvest_run_all():
    """
    Dispatch all harvesters via Celery (async).
    GET /admin/harvest/run/all?sport=soccer  (one sport)
    GET /admin/harvest/run/all               (all local sports)
    """
    sport  = request.args.get("sport", "")
    sports = [sport] if sport else _LOCAL_SPORTS
    dispatched = []

    for s in sports:
        for task_name in [
            "tasks.sp.harvest_sport",
            "tasks.bt.harvest_sport",
            "tasks.od.harvest_sport",
            "tasks.b2b.harvest_sport",
        ]:
            try:
                from app.workers.celery_tasks import celery
                celery.send_task(task_name, args=[s], queue="harvest", countdown=0)
                dispatched.append(f"{task_name}({s})")
            except Exception as e:
                dispatched.append(f"FAILED {task_name}({s}): {e}")

    return _ok({
        "ok": True,
        "dispatched": dispatched,
        "count": len(dispatched),
        "note": "Tasks sent to Celery queue. Check /admin/harvest/status in ~60s for results.",
        "ts": time.time(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# TEST FETCH — directly call BK API and show raw result
# ══════════════════════════════════════════════════════════════════════════════

@bp_admin.route("/test/fetch", methods=["GET"])
def test_fetch():
    """
    Test BK API connectivity directly.
    Returns first 3 matches from the API so you can see if data is coming through.

    GET /admin/test/fetch?bk=bt&sport=soccer
    """
    bk    = request.args.get("bk", "bt").lower()
    sport = request.args.get("sport", "soccer")
    t0    = time.perf_counter()

    try:
        if bk == "bt":
            from app.workers.bt_harvester import fetch_upcoming_matches
            matches = fetch_upcoming_matches(sport, max_pages=1, fetch_full=False)
            sample  = matches[:3]
            return _ok({
                "ok": True, "bk": bk, "sport": sport,
                "match_count": len(matches),
                "sample_matches": [
                    {"home": m.get("home_team"), "away": m.get("away_team"),
                     "markets": list(m.get("markets", {}).keys())[:5]}
                    for m in sample
                ],
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "diagnosis": "BT API is working ✓" if matches else "BT API returned 0 matches — check API connectivity",
            })

        elif bk == "od":
            from app.workers.od_harvester import fetch_upcoming_matches
            matches = fetch_upcoming_matches(sport, day="")
            sample  = matches[:3]
            return _ok({
                "ok": True, "bk": bk, "sport": sport,
                "match_count": len(matches),
                "sample_matches": [
                    {"home": m.get("home_team"), "away": m.get("away_team"),
                     "markets": list(m.get("markets", {}).keys())[:5]}
                    for m in sample
                ],
                "latency_ms": int((time.perf_counter() - t0) * 1000),
                "diagnosis": "OD API is working ✓" if matches else "OD API returned 0 matches — check API connectivity",
            })

        elif bk == "sp":
            from app.workers.sp_harvester import fetch_upcoming_stream
            matches = []
            for m in fetch_upcoming_stream(sport, max_matches=3, fetch_full_markets=False, sleep_between=0):
                matches.append(m)
            return _ok({
                "ok": True, "bk": bk, "sport": sport,
                "match_count": len(matches),
                "sample_matches": [
                    {"home": m.get("home_team"), "away": m.get("away_team"),
                     "betradar_id": m.get("betradar_id"),
                     "markets": list(m.get("markets", {}).keys())[:5]}
                    for m in matches
                ],
                "latency_ms": int((time.perf_counter() - t0) * 1000),
            })

        elif bk in _B2B_BKS:
            from app.workers.b2b_harvester import B2B_BOOKMAKERS, fetch_single_bk
            bk_cfg = next((b for b in B2B_BOOKMAKERS if b["slug"] == bk), None)
            if not bk_cfg:
                return _ok({"ok": False, "error": f"BK config not found for {bk}"})
            matches = fetch_single_bk(bk_cfg, sport, "upcoming", page=1, page_size=10)
            return _ok({
                "ok": True, "bk": bk, "sport": sport,
                "match_count": len(matches),
                "sample_matches": [
                    {"home": m.get("home_team"), "away": m.get("away_team"),
                     "markets": list(m.get("markets", {}).keys())[:5]}
                    for m in matches[:3]
                ],
                "latency_ms": int((time.perf_counter() - t0) * 1000),
            })

        else:
            return _ok({"ok": False, "error": f"Unknown bk: {bk}"})

    except Exception as e:
        return _ok({
            "ok": False, "bk": bk, "sport": sport,
            "error": str(e),
            "traceback": traceback.format_exc()[-2000:],
            "latency_ms": int((time.perf_counter() - t0) * 1000),
        })


# ══════════════════════════════════════════════════════════════════════════════
# REDIS KEY DUMP
# ══════════════════════════════════════════════════════════════════════════════

@bp_admin.route("/redis/keys", methods=["GET"])
def redis_keys():
    """Show every odds-related Redis key, match count, TTL."""
    sport = request.args.get("sport", "soccer")
    r     = _get_redis()
    keys  = {}

    patterns = [
        f"*:{sport}*", f"odds:*:{sport}", f"odds:*:*:{sport}",
        f"odds:unified:*:{sport}",
    ]
    for pat in patterns:
        cur = 0
        while True:
            cur, found = r.scan(cur, match=pat, count=100)
            for k in found:
                ks  = k.decode() if isinstance(k, bytes) else k
                raw = r.get(k)
                count = 0
                if raw:
                    try:
                        d = json.loads(raw)
                        ms = d if isinstance(d, list) else d.get("matches", [])
                        count = len(ms)
                    except Exception:
                        pass
                keys[ks] = {"ttl": r.ttl(k), "match_count": count, "bytes": len(raw or b"")}
            if cur == 0:
                break

    return _ok({
        "sport":      sport,
        "keys_found": len(keys),
        "keys":       dict(sorted(keys.items())),
        "ts":         time.time(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# DIRECT FETCH HELPERS  (synchronous, bypass Celery)
# ══════════════════════════════════════════════════════════════════════════════

def _run_sp(sport: str) -> dict:
    from app.workers.sp_harvester import fetch_upcoming_stream
    from app.workers.celery_tasks import cache_set, _now_iso
    from app.workers.redis_bus import publish_snapshot

    matches = []
    for m in fetch_upcoming_stream(sport, max_matches=500, fetch_full_markets=True, sleep_between=0.1):
        matches.append(m)

    if not matches:
        return {"ok": False, "bk": "sp", "sport": sport, "reason": "SP returned 0 matches"}

    cache_set(f"sp:upcoming:{sport}", {
        "source": "sportpesa", "sport": sport, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(), "matches": matches,
    }, ttl=3600)
    publish_snapshot("sp", "upcoming", sport, matches)
    return {"ok": True, "bk": "sp", "sport": sport, "count": len(matches)}


def _run_bt(sport: str) -> dict:
    from app.workers.bt_harvester import fetch_upcoming_matches
    from app.workers.celery_tasks import cache_set, _now_iso
    from app.workers.redis_bus import publish_snapshot

    matches = fetch_upcoming_matches(sport, max_pages=10, fetch_full=False)

    if not matches:
        return {
            "ok": False, "bk": "bt", "sport": sport,
            "reason": "Betika API returned 0 matches",
            "hint": "Check /admin/test/fetch?bk=bt&sport=soccer to test API directly",
        }

    cache_set(f"bt:upcoming:{sport}", {
        "source": "betika", "sport": sport, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(), "matches": matches,
    }, ttl=3600)
    publish_snapshot("bt", "upcoming", sport, matches)
    return {"ok": True, "bk": "bt", "sport": sport, "count": len(matches)}


def _run_od(sport: str) -> dict:
    from app.workers.od_harvester import fetch_upcoming_matches
    from app.workers.celery_tasks import cache_set, _now_iso
    from app.workers.redis_bus import publish_snapshot
    from datetime import date

    today   = date.today().isoformat()
    matches = fetch_upcoming_matches(sport, day=today)

    if not matches:
        return {
            "ok": False, "bk": "od", "sport": sport,
            "reason": "OdiBets API returned 0 matches for today",
            "hint": "Check /admin/test/fetch?bk=od&sport=soccer to test API directly",
        }

    cache_set(f"od:upcoming:{sport}", {
        "source": "odibets", "sport": sport, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(), "matches": matches,
    }, ttl=3600)
    publish_snapshot("od", "upcoming", sport, matches)
    return {"ok": True, "bk": "od", "sport": sport, "count": len(matches)}


def _run_b2b_single(bk_slug: str, sport: str) -> dict:
    from app.workers.b2b_harvester import B2B_BOOKMAKERS, fetch_single_bk
    from app.workers.celery_tasks import cache_set, _now_iso
    from app.workers.redis_bus import publish_snapshot

    bk_cfg = next((b for b in B2B_BOOKMAKERS if b["slug"] == bk_slug), None)
    if not bk_cfg:
        return {"ok": False, "error": f"No config for {bk_slug}"}

    matches = fetch_single_bk(bk_cfg, sport, "upcoming")
    if not matches:
        return {"ok": False, "bk": bk_slug, "sport": sport, "reason": "API returned 0 matches"}

    publish_snapshot(bk_slug, "upcoming", sport, matches)
    return {"ok": True, "bk": bk_slug, "sport": sport, "count": len(matches)}


def _run_b2b_all(sport: str) -> dict:
    from app.workers.b2b_harvester import harvest_b2b_sport
    from app.workers.celery_tasks import cache_set, _now_iso
    from app.workers.redis_bus import publish_snapshot

    matches = harvest_b2b_sport(sport, "upcoming")
    if not matches:
        return {"ok": False, "bk": "b2b", "sport": sport, "reason": "All B2B BKs returned 0 matches"}

    cache_set(f"b2b:upcoming:{sport}", {
        "source": "b2b", "sport": sport, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(), "matches": matches,
    }, ttl=3600)
    publish_snapshot("b2b", "upcoming", sport, matches)
    return {"ok": True, "bk": "b2b", "sport": sport, "count": len(matches)}