"""
app/workers/tasks_live.py
==========================
All LIVE harvest tasks.

These run on the `live` Celery queue at high frequency.

Beat schedule entries:
  sender.add_periodic_task(60.0,   sp_harvest_all_live.s(),      name="sp-live-60s")
  sender.add_periodic_task(90.0,   bt_harvest_all_live.s(),      name="bt-live-90s")
  sender.add_periodic_task(90.0,   od_harvest_all_live.s(),      name="od-live-90s")
  sender.add_periodic_task(120.0,  b2b_harvest_all_live.s(),     name="b2b-live-2min")
  sender.add_periodic_task(30.0,   b2b_page_harvest_all_live.s(), name="b2b-page-live-30s")
  sender.add_periodic_task(60.0,   sbo_harvest_all_live.s(),     name="sbo-live-60s")

NOTE: Sportpesa's TRUE real-time feed lives in sp_live_harvester.py
(WebSocket thread + 1-second Redis republisher). The tasks here are
the fallback HTTP snapshots that keep the cache warm even if the WS
thread is temporarily disconnected.
"""

from __future__ import annotations

import time

from celery import group
from celery.utils.log import get_task_logger

from app.workers.celery_app import (
    celery, cache_set, _now_iso, _upsert_and_chain, _publish,
)

logger = get_task_logger(__name__)

WS_CHANNEL = "odds:updates"

_LIVE_SPORTS     = ["soccer", "basketball", "tennis"]
_B2B_LIVE_SPORTS = ["Football", "Basketball", "Ice Hockey", "Tennis"]
_SBO_LIVE_SPORTS = ["soccer", "basketball", "tennis"]


def _emit(source: str, sport: str, count: int, latency: int) -> None:
    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": source, "sport": sport,
        "mode": "live", "count": count, "latency_ms": latency,
        "ts": _now_iso(),
    })


# =============================================================================
# SPORTPESA LIVE  (HTTP snapshot fallback — WS is primary)
# =============================================================================

@celery.task(
    name="tasks.sp.harvest_sport_live", bind=True,
    max_retries=2, default_retry_delay=10,
    soft_time_limit=120, time_limit=150, acks_late=True,
)
def sp_harvest_sport_live(self, sport_slug: str) -> dict:
    """
    HTTP snapshot of live SP matches.  Runs every 60 s as a fallback.
    The WebSocket harvester (sp_live_harvester.py) provides the true
    sub-second feed; this task keeps the cache warm if WS disconnects.
    """
    t0 = time.perf_counter()
    try:
        from app.workers.sp_live_harvester import fetch_live
        matches = fetch_live(sport_slug, fetch_full_markets=True)
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"sp:live:{sport_slug}", {
        "source": "sportpesa", "sport": sport_slug, "mode": "live",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=90)
    _upsert_and_chain(matches, "Sportpesa")
    _emit("sportpesa", sport_slug, len(matches), latency)
    logger.info("[sp:live] %s → %d matches %dms", sport_slug, len(matches), latency)
    return {"ok": True, "source": "sportpesa", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.sp.harvest_all_live", soft_time_limit=30, time_limit=60)
def sp_harvest_all_live() -> dict:
    sigs = [sp_harvest_sport_live.s(s) for s in _LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}


# =============================================================================
# BETIKA LIVE
# =============================================================================

@celery.task(
    name="tasks.bt.harvest_sport_live", bind=True,
    max_retries=2, default_retry_delay=15,
    soft_time_limit=120, time_limit=150, acks_late=True,
)
def bt_harvest_sport_live(self, sport_slug: str) -> dict:
    t0 = time.perf_counter()
    try:
        from app.workers.bt_harvester import fetch_live
        matches = fetch_live(sport_slug, fetch_full_markets=True)
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"bt:live:{sport_slug}", {
        "source": "betika", "sport": sport_slug, "mode": "live",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=90)
    _upsert_and_chain(matches, "Betika")
    _emit("betika", sport_slug, len(matches), latency)
    logger.info("[bt:live] %s → %d matches %dms", sport_slug, len(matches), latency)
    return {"ok": True, "source": "betika", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.bt.harvest_all_live", soft_time_limit=30, time_limit=60)
def bt_harvest_all_live() -> dict:
    sigs = [bt_harvest_sport_live.s(s) for s in _LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}


# =============================================================================
# ODIBETS LIVE
# =============================================================================

@celery.task(
    name="tasks.od.harvest_sport_live", bind=True,
    max_retries=1, default_retry_delay=20,
    soft_time_limit=150, time_limit=180, acks_late=True,
)
def od_harvest_sport_live(self, sport_slug: str) -> dict:
    t0 = time.perf_counter()
    try:
        from app.workers.od_harvester import fetch_live
        matches = fetch_live(sport_slug, fetch_full_markets=True)
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"od:live:{sport_slug}", {
        "source": "odibets", "sport": sport_slug, "mode": "live",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=90)
    _upsert_and_chain(matches, "Odibets")
    _emit("odibets", sport_slug, len(matches), latency)
    return {"ok": True, "source": "odibets", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.od.harvest_all_live", soft_time_limit=30, time_limit=60)
def od_harvest_all_live() -> dict:
    sigs = [od_harvest_sport_live.s(s) for s in _LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}


# =============================================================================
# B2B DIRECT LIVE
# =============================================================================

@celery.task(
    name="tasks.b2b.harvest_sport_live", bind=True,
    max_retries=2, default_retry_delay=20,
    soft_time_limit=180, time_limit=210, acks_late=True,
)
def b2b_harvest_sport_live(self, sport_slug: str) -> dict:
    t0 = time.perf_counter()
    try:
        from app.workers.b2b_harvester import fetch_b2b_sport
        matches = fetch_b2b_sport(sport_slug, mode="live")
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"b2b:live:{sport_slug}", {
        "source": "b2b", "sport": sport_slug, "mode": "live",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=60)
    for m in matches:
        for bk_name, bk_data in (m.get("bookmakers") or {}).items():
            bk_match = dict(m)
            bk_match["markets"] = bk_data.get("markets") or {}
            _upsert_and_chain([bk_match], bk_name)
    _emit("b2b", sport_slug, len(matches), latency)
    return {"ok": True, "source": "b2b", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.b2b.harvest_all_live", soft_time_limit=30, time_limit=60)
def b2b_harvest_all_live() -> dict:
    sigs = [b2b_harvest_sport_live.s(s) for s in _LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}


# =============================================================================
# B2B PAGE FAN-OUT LIVE (legacy)
# =============================================================================

@celery.task(
    name="tasks.b2b_page.harvest_page_live", bind=True,
    max_retries=2, default_retry_delay=10,
    soft_time_limit=45, time_limit=60, acks_late=True,
)
def b2b_page_harvest_page_live(self, bookmaker: dict, sport: str) -> dict:
    from app.workers.celery_app import _upsert_unified_match
    bk_name = bookmaker.get("name") or bookmaker.get("domain", "?")
    bk_id   = bookmaker.get("id")
    t0      = time.perf_counter()
    try:
        from app.workers.bookmaker_fetcher import fetch_bookmaker
        matches = fetch_bookmaker(bookmaker, sport_name=sport, mode="live",
                                  page=1, page_size=50, timeout=20)
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    ck = f"odds:live:{sport.lower().replace(' ','_')}:{bk_id}"
    cache_set(ck, {
        "bookmaker_id": bk_id, "bookmaker_name": bk_name,
        "sport": sport, "mode": "live",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=60)
    for m in matches:
        _upsert_unified_match(m, bk_id, bk_name)
    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": "b2b",
        "bookmaker": bk_name, "sport": sport, "mode": "live",
        "count": len(matches), "ts": _now_iso(),
    })
    return {"ok": True, "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.b2b_page.harvest_all_live", soft_time_limit=30, time_limit=60)
def b2b_page_harvest_all_live() -> dict:
    import json as _json
    from app.workers.celery_app import _redis
    raw = _redis().get("cache:bookmakers:active")
    bookmakers = _json.loads(raw) if raw else []
    sigs = [
        b2b_page_harvest_page_live.s(bm, sport)
        for bm in bookmakers
        for sport in _B2B_LIVE_SPORTS
    ]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}


# =============================================================================
# SBO LIVE
# =============================================================================

@celery.task(
    name="tasks.sbo.harvest_sport_live", bind=True,
    max_retries=1, default_retry_delay=20,
    soft_time_limit=120, time_limit=150, acks_late=True,
)
def sbo_harvest_sport_live(self, sport_slug: str) -> dict:
    t0 = time.perf_counter()
    try:
        from app.views.sbo.sbo_fetcher import OddsAggregator, SPORT_CONFIG
        cfg = next((c for c in SPORT_CONFIG if c["sport"] == sport_slug), None)
        if not cfg:
            return {"ok": False, "error": f"Unknown sport: {sport_slug}"}
        agg     = OddsAggregator(cfg, fetch_full_sp_markets=True, fetch_full_bt_markets=True)
        matches = agg.run(max_matches=30, mode="live")
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"sbo:live:{sport_slug}", {
        "sport": sport_slug, "match_count": len(matches),
        "harvested_at": _now_iso(), "latency_ms": latency, "matches": matches,
    }, ttl=60)
    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": "sbo",
        "sport": sport_slug, "mode": "live", "count": len(matches), "ts": _now_iso(),
    })
    return {"ok": True, "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.sbo.harvest_all_live", soft_time_limit=30, time_limit=60)
def sbo_harvest_all_live() -> dict:
    sigs = [sbo_harvest_sport_live.s(s) for s in _SBO_LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}