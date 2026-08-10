"""
app/workers/tasks_ops.py
=========================
KEY CHANGES vs previous version:
  1. _beat_bt_od_all — new beat task (every 10 min) that runs BT+OD harvest
     for ALL sports independently of SP.  This is the root fix for
     "soccer always available, other sports not" — previously BT/OD only
     ran as a side-effect of SP harvest succeeding.  Now they run on their
     own schedule so basketball, tennis, cricket etc. are always populated.

  2. All other logic unchanged from the previous version.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from celery.signals import worker_ready
from celery.utils.log import get_task_logger

from app.workers.celery_tasks import celery, _redis as _get_redis, _now_iso, _publish

logger = get_task_logger(__name__)
log    = logging.getLogger(__name__)

_ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby",
    "ice-hockey", "volleyball", "handball", "table-tennis",
    "baseball", "mma", "boxing", "darts", "american-football", "esoccer",
]

_ALL_BKS   = ["sp", "bt", "od", "1xbet", "22bet", "betwinner", "melbet",
               "megapari", "helabet", "paripesa"]
_LOCAL_BKS = ["sp", "bt", "od"]
_B2B_BKS   = ["1xbet", "22bet", "betwinner", "melbet", "megapari", "helabet", "paripesa"]

TIER_CONFIG = {
    "free":    {"sports": ["soccer"],  "days_ahead": 1,  "markets_api": False},
    "basic":   {"sports": _ALL_SPORTS, "days_ahead": 1,  "markets_api": False},
    "pro":     {"sports": _ALL_SPORTS, "days_ahead": 7,  "markets_api": "local"},
    "premium": {"sports": _ALL_SPORTS, "days_ahead": 30, "markets_api": "all"},
    "admin":   {"sports": _ALL_SPORTS, "days_ahead": 90, "markets_api": "all"},
}

_ARB_MARKETS_2 = {"btts", "odd_even", "both_teams_to_score"}
_ARB_MARKETS_3 = {"match_winner", "1x2", "moneyline", "3way"}
_OU_PREFIX     = "over_under_"

TTL_BK_SNAP = 86400
TTL_UNIFIED = 86400


# =============================================================================
# PERSISTENT CACHE HELPERS
# =============================================================================

def _smart_set(r, key: str, value: Any, ttl: int = TTL_UNIFIED) -> None:
    try:
        from app.workers.persistent_cache import smart_set
        smart_set(r, key, value, ttl=ttl)
        return
    except ImportError:
        pass
    except Exception as exc:
        log.debug("_smart_set (persistent_cache) error %s: %s", key, exc)
    try:
        payload = json.dumps(value, default=str) if not isinstance(value, (str, bytes)) else value
        r.set(key, payload, ex=ttl)
    except Exception as exc:
        log.debug("_smart_set fallback error %s: %s", key, exc)


def _refresh_all_ttls(r) -> int:
    try:
        from app.workers.persistent_cache import refresh_all_ttls
        return refresh_all_ttls(r)
    except ImportError:
        pass
    count = 0
    try:
        for key in r.scan_iter("odds:*"):
            ttl = r.ttl(key)
            if 0 < ttl < 3600:
                r.expire(key, TTL_UNIFIED)
                count += 1
    except Exception as exc:
        log.debug("_refresh_all_ttls fallback error: %s", exc)
    return count


def _startup_hydrate(r) -> int:
    try:
        from app.workers.persistent_cache import startup_hydrate
        return startup_hydrate(r)
    except ImportError:
        log.debug("persistent_cache not available — skipping startup hydration")
        return 0
    except Exception as exc:
        log.warning("startup_hydrate error: %s", exc)
        return 0


def _backup_redis_to_snapshot(r) -> dict:
    try:
        from app.workers.persistent_cache import backup_redis_to_snapshot
        return backup_redis_to_snapshot(r)
    except ImportError:
        return {"skipped": True, "reason": "persistent_cache not available"}
    except Exception as exc:
        log.warning("backup_redis_to_snapshot error: %s", exc)
        return {"error": str(exc)}


# =============================================================================
# EV / ARB COMPUTATION
# =============================================================================

@celery.task(
    name="tasks.ops.compute_ev_arb",
    bind=True, max_retries=2, default_retry_delay=5,
    soft_time_limit=3000, time_limit=4500, acks_late=True,
)
def compute_ev_arb(self, match_id) -> dict:
    r          = _get_redis()
    match_data = _load_match_for_ev(r, match_id)
    if not match_data:
        return {"ok": False, "reason": "not_found", "match_id": match_id}

    sport    = (match_data.get("sport") or "soccer").lower()
    home     = match_data.get("home_team", "?")
    away     = match_data.get("away_team", "?")
    join_key = str(match_data.get("join_key") or match_data.get("parent_match_id") or match_id)

    best: dict[str, dict[str, dict]] = {}

    for bk_slug, bk_data in (match_data.get("bookmakers") or {}).items():
        mkts = (bk_data.get("markets") if isinstance(bk_data, dict) else None) or {}
        for mkt, outcomes in mkts.items():
            if not isinstance(outcomes, dict):
                continue
            best.setdefault(mkt, {})
            for out, p in outcomes.items():
                price = _xp(p)
                if price > 1.0:
                    existing = best[mkt].get(out)
                    if not existing or price > existing["odd"]:
                        best[mkt][out] = {"odd": price, "bk": bk_slug}

    for mkt, outcomes in (match_data.get("markets") or {}).items():
        if not isinstance(outcomes, dict):
            continue
        best.setdefault(mkt, {})
        for out, p in outcomes.items():
            price = _xp_db(p)
            bk    = (p.get("bk_slug") if isinstance(p, dict) else None) or "sp"
            if price > 1.0:
                existing = best[mkt].get(out)
                if not existing or price > existing["odd"]:
                    best[mkt][out] = {"odd": price, "bk": bk}

    if not best:
        return {"ok": False, "reason": "no_markets", "match_id": match_id}

    arbs: list[dict] = []
    evs:  list[dict] = []

    try:
        from app.workers.arb_engine import detect_arb_for_stream
        has_arb, best_arb, arbs = detect_arb_for_stream(best)
    except Exception:
        has_arb, best_arb, arbs = False, 0.0, []

    for mkt, ob in best.items():
        keys = list(ob.keys())
        n    = len(keys)
        if mkt in _ARB_MARKETS_2 or (mkt.startswith(_OU_PREFIX) and "asian" not in mkt):
            exp = 2
        elif mkt in _ARB_MARKETS_3:
            exp = 3
        else:
            exp = n

        if n < max(exp, 2):
            continue

        use     = keys[:exp]
        sum_inv = 0.0
        valid   = True
        for k in use:
            odd = ob[k]["odd"]
            if odd <= 1.0:
                valid = False; break
            sum_inv += 1.0 / odd

        if not valid or sum_inv <= 0:
            continue

        for k in use:
            odd = ob[k]["odd"]
            if odd > 1.0:
                fair_p = (1.0 / odd) / sum_inv
                ev_pct = round((odd * fair_p - 1) * 100, 2)
                if ev_pct > 3.0:
                    evs.append({
                        "market": mkt, "outcome": k, "odd": odd,
                        "bk": ob[k]["bk"], "ev_pct": ev_pct,
                    })

    has_ev   = bool(evs)
    best_ev  = max((e["ev_pct"]     for e in evs),  default=0.0)

    result = {
        "join_key": join_key, "match_id": match_id,
        "home_team": home, "away_team": away, "sport": sport,
        "has_arb": has_arb, "best_arb_pct": best_arb, "arb_count": len(arbs),
        "arb_opportunities": arbs,
        "has_ev": has_ev, "best_ev_pct": best_ev,
        "ev_opportunities": evs,
        "bk_count": len(match_data.get("bookmakers") or {}),
        "market_count": len(best), "computed_at": _now_iso(),
    }

    if has_arb:
        _publish(f"arb:updates:{sport}", {"event": "arb_updated", "type": "arb_updated", **result})
    if has_ev:
        _publish(f"ev:updates:{sport}",  {"event": "ev_updated",  "type": "ev_updated",  **result})

    _patch_unified(r, sport, join_key, {
        "has_arb": has_arb, "best_arb_pct": best_arb,
        "has_ev": has_ev, "best_ev_pct": best_ev,
        "arb_opportunities": arbs[:3], "ev_opportunities": evs[:3],
    })

    _persist_ev_arb(match_id, result)
    logger.info("[ev_arb] %s v %s arb=%s %.2f%% ev=%s bks=%d",
                home, away, has_arb, best_arb, has_ev, result["bk_count"])
    return {"ok": True, **result}


@celery.task(
    name="tasks.ops.generate_custom_report",
    bind=True, max_retries=1,
    soft_time_limit=300, time_limit=450, acks_late=True,
)
def generate_custom_report(
    self, sport: str, arb_only: bool,
    start_time_str: str = None, end_time_str: str = None,
    preset: str = "all",
) -> dict:
    import base64
    from app.views.customer.routes_api import _generate_word_document, _serve_minio_report
    r = _get_redis()
    task_id = self.request.id
    try:
        f_stream     = _serve_minio_report(sport, arb_only, preset=preset)
        sp_available = True
        sp_flag      = r.get(f"odds_report:sp_available:{sport}:{preset}")
        if sp_flag is not None:
            sp_available = sp_flag in (b"1", "1")
        if f_stream is None:
            f_stream, sp_available = _generate_word_document(
                sport, arb_only,
                start_time_str=start_time_str,
                end_time_str=end_time_str,
            )
        data_bytes = f_stream.getvalue()
        encoded    = base64.b64encode(data_bytes).decode("utf-8")
        r.set(f"custom_report:{task_id}", encoded, ex=300)
        r.set(f"custom_report:sp_available:{task_id}", "1" if sp_available else "0", ex=300)
        return {"status": "SUCCESS", "task_id": task_id, "sp_available": sp_available}
    except Exception as exc:
        logger.error("[generate_custom_report] %s", exc, exc_info=True)
        return {"status": "FAILED", "error": str(exc)}


# =============================================================================
# PER-BK PUBLISH
# =============================================================================

@celery.task(name="tasks.ops.publish_bk_snapshot", soft_time_limit=2000, time_limit=3000)
def publish_bk_snapshot(bk_slug: str, mode: str, sport: str, matches: list[dict]) -> dict:
    r = _get_redis()
    _smart_set(r, f"odds:{bk_slug}:{mode}:{sport}", {
        "bk": bk_slug, "mode": mode, "sport": sport,
        "matches": matches, "ts": time.time(),
    }, ttl=TTL_BK_SNAP)
    _merge_bk_into_unified(r, bk_slug, mode, sport, matches)
    _publish(f"odds:{bk_slug}:{mode}:{sport}:ready", {
        "event": "snapshot_ready", "bk": bk_slug, "mode": mode,
        "sport": sport, "count": len(matches), "ts": time.time(),
    })
    return {"ok": True, "bk": bk_slug, "count": len(matches)}


def _merge_bk_into_unified(r, bk_slug: str, mode: str, sport: str,
                            new_matches: list[dict]) -> None:
    key = f"odds:unified:{mode}:{sport}"
    try:
        raw      = r.get(key)
        existing = []
        if raw:
            d        = json.loads(raw)
            existing = d if isinstance(d, list) else d.get("matches", [])

        idx: dict[str, int] = {}
        for i, m in enumerate(existing):
            jk = str(m.get("join_key") or m.get("parent_match_id") or m.get("betradar_id") or "")
            nk = _name_key(m)
            if jk: idx[jk] = i
            idx.setdefault(nk, i)

        for nm in new_matches:
            jk  = str(nm.get("join_key") or nm.get("parent_match_id") or nm.get("betradar_id") or "")
            nk  = _name_key(nm)
            pos = idx.get(jk) if jk else None
            if pos is None:
                pos = idx.get(nk)
            mkts = nm.get("markets") or {}
            if pos is not None:
                em = existing[pos]
                em.setdefault("bookmakers", {})[bk_slug] = {
                    "match_id": nm.get("match_id") or nm.get("external_id") or "",
                    "markets": mkts,
                }
                for bk, bd in (nm.get("bookmakers") or {}).items():
                    em["bookmakers"].setdefault(bk, bd)
                for fld in ("score_home", "score_away", "is_live", "match_time"):
                    if nm.get(fld) is not None:
                        em[fld] = nm[fld]
            else:
                nr = dict(nm)
                nr.setdefault("bookmakers", {})[bk_slug] = {
                    "match_id": nm.get("match_id") or "",
                    "markets": mkts,
                }
                p2 = len(existing); existing.append(nr)
                if jk: idx[jk] = p2
                idx[nk] = p2

        _smart_set(r, key, {
            "mode": mode, "sport": sport, "source": "unified",
            "matches": existing, "updated_at": time.time(),
        }, ttl=TTL_UNIFIED)

        _publish(f"odds:all:{mode}:{sport}:updates", {
            "event": "snapshot_ready", "bk": bk_slug, "mode": mode,
            "sport": sport, "count": len(existing), "ts": time.time(),
        })
    except Exception as exc:
        logger.warning("[merge_bk] %s %s %s: %s", bk_slug, mode, sport, exc)


# =============================================================================
# BEAT SCHEDULE
# =============================================================================

def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(300.0,  _beat_harvest_all_paged.s(), name="harvest:all_paged 5min")
    sender.add_periodic_task(90.0,   _beat_b2b_live.s(),          name="b2b:live 90s")
    sender.add_periodic_task(600.0,  _beat_bt_od_all.s(),         name="bt_od:all_sports 10min")  # NEW
    sender.add_periodic_task(600.0,  _beat_alignment.s(),         name="align:full 10min")
    sender.add_periodic_task(1800.0, _beat_prune.s(),             name="prune:redis 30min")
    sender.add_periodic_task(900.0,  _beat_db_backup.s(),         name="db:backup 15min")
    sender.add_periodic_task(600.0,  _beat_v2_groups.s(),         name="v2:group_cache 10min")
    logger.info("[tasks_ops] beat schedule registered")


celery.on_after_configure.connect(setup_periodic_tasks)


@celery.task(name="tasks.ops.beat.harvest_all_paged", soft_time_limit=3000, time_limit=6000)
def _beat_harvest_all_paged():
    try:
        from app.workers.tasks_harvest_pages import harvest_all_paged
        harvest_all_paged.apply_async(queue="harvest")
    except ImportError as exc:
        log.warning("[beat:harvest] import failed: %s", exc)
    return {"ok": True}


@celery.task(name="tasks.ops.beat.b2b_live", soft_time_limit=3000, time_limit=6000)
def _beat_b2b_live():
    try:
        from app.workers.tasks_harvest_b2b import b2b_harvest_all_live
        b2b_harvest_all_live.apply_async(queue="harvest")
    except ImportError as exc:
        log.warning("[beat:b2b_live] import failed: %s", exc)
    return {"ok": True}


@celery.task(name="tasks.ops.beat.bt_od_all", soft_time_limit=3000, time_limit=6000)
def _beat_bt_od_all():
    """
    NEW — Independent BT+OD harvest for ALL sports, every 10 minutes.

    Why this matters:
    - Previously BT/OD only ran as a side-effect of `sp_harvest_sport` succeeding.
    - SP only covers a subset of sports reliably (heavy soccer bias).
    - This beat task ensures basketball, tennis, cricket, handball, darts etc.
      always have fresh BT+OD data in Redis + DB regardless of SP status.
    """
    try:
        from app.workers.tasks_upcoming import bt_od_harvest_all_upcoming
        bt_od_harvest_all_upcoming.apply_async(queue="harvest")
        log.info("[beat:bt_od_all] dispatched bt_od harvest for all sports")
    except ImportError as exc:
        log.warning("[beat:bt_od_all] import failed: %s", exc)
    return {"ok": True}


@celery.task(name="tasks.ops.beat.alignment", soft_time_limit=3000, time_limit=6000)
def _beat_alignment():
    try:
        from celery import group as cg
        from app.workers.tasks_align import align_sport
        cg([align_sport.s(s, 500) for s in _ALL_SPORTS]).apply_async(queue="results")
    except ImportError as exc:
        log.warning("[beat:alignment] import failed: %s", exc)
    return {"ok": True}


@celery.task(name="tasks.ops.beat.prune", soft_time_limit=120, time_limit=180)
def _beat_prune():
    r = _get_redis()
    n = _refresh_all_ttls(r)
    logger.info("[beat:prune] refreshed %d Redis key TTLs", n)
    return {"ok": True, "refreshed": n}


@celery.task(name="tasks.ops.beat.db_backup", soft_time_limit=120, time_limit=180)
def _beat_db_backup():
    r      = _get_redis()
    result = _backup_redis_to_snapshot(r)
    logger.info("[beat:db_backup] %s", result)
    return {"ok": True, **result}


@celery.task(name="tasks.ops.beat.hydrate_redis", soft_time_limit=60, time_limit=90)
def _beat_hydrate_redis():
    r = _get_redis()
    n = _startup_hydrate(r)
    log.info("[beat:hydrate] hydrated %d keys", n)
    return {"ok": True, "hydrated": n}


@celery.task(name="tasks.ops.beat.v2_groups", soft_time_limit=300, time_limit=420)
def _beat_v2_groups():
    """Pre-warm DB-based v2 group list for today + tomorrow."""
    from datetime import datetime, timezone, timedelta
    try:
        from app.views.customer.word_generator_v2 import get_available_groups
    except ImportError as exc:
        log.warning("[beat:v2_groups] word_generator_v2 not found: %s", exc)
        return {"ok": False, "error": str(exc)}

    EAT     = timedelta(hours=3)
    now_eat = datetime.now(timezone.utc) + EAT
    dates   = [
        now_eat.strftime("%Y-%m-%d"),
        (now_eat + timedelta(days=1)).strftime("%Y-%m-%d"),
    ]
    # Priority sports — others load on demand from DB
    priority_sports = ["soccer", "basketball", "tennis", "cricket"]
    total = 0
    for sport in priority_sports:
        for date_str in dates:
            try:
                groups = get_available_groups(sport, date_str)
                total += len(groups)
                log.info("[beat:v2_groups] %s %s → %d groups", sport, date_str, len(groups))
            except Exception as exc:
                log.warning("[beat:v2_groups] %s %s failed: %s", sport, date_str, exc)

    return {"ok": True, "groups_warmed": total}


# =============================================================================
# WORKER STARTUP
# =============================================================================

@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    _app = None

    # Step 1: App context
    try:
        import os
        os.environ.setdefault("ENABLE_HARVESTER", "0")
        from app import create_app as _create_app
        _app = _create_app()
        log.info("[startup] app context created")
    except Exception as exc:
        log.warning("[startup] could not create app context: %s", exc)

    # Step 2: Hydrate Redis from DB
    try:
        r = _get_redis()
        if _app is not None:
            with _app.app_context():
                n = _startup_hydrate(r)
        else:
            n = _startup_hydrate(r)
        log.info("[startup] hydrated %d Redis keys from DB", n)
    except Exception as exc:
        log.warning("[startup] hydration failed: %s", exc)

    # Step 3: Harvest dispatch
    try:
        _dispatch_startup_harvests()
    except Exception as exc:
        log.warning("[startup] harvest dispatch failed: %s", exc)

    # Step 4: Mozzart WebSocket live harvester
    try:
        from app.workers.mz_live_harvester import get_mozzart_live_harvester
        mz_h = get_mozzart_live_harvester()
        mz_h.start()
        log.info("[startup] Mozzart WebSocket harvester started")
    except Exception as exc:
        log.warning("[startup] Mozzart live harvester failed: %s", exc)

    # Step 5: SP WebSocket harvester
    try:
        from app.workers.sp_live_harvester import start_harvester_thread as _start_sp
        _start_sp()
        log.info("[startup] SP WebSocket harvester started")
    except Exception as exc:
        log.warning("[startup] SP live harvester failed: %s", exc)

    # Step 6: LiveFeedBridge
    try:
        from app.workers.live_feed_bridge import start_live_bridge
        if _app is not None:
            with _app.app_context():
                start_live_bridge()
        else:
            start_live_bridge()
        log.info("[startup] LiveFeedBridge started")
    except ImportError:
        try:
            from app.workers.match_lifecycle import start_lifecycle_manager
            if _app is not None:
                with _app.app_context():
                    start_lifecycle_manager()
            else:
                start_lifecycle_manager()
            log.info("[startup] MatchLifecycleManager started (fallback)")
        except Exception as exc2:
            log.warning("[startup] no lifecycle manager started: %s", exc2)
    except Exception as exc:
        log.warning("[startup] LiveFeedBridge failed: %s", exc)


def _dispatch_startup_harvests() -> None:
    dispatched = []
    try:
        from app.workers.tasks_harvest_pages import (
            sp_harvest_all_paged, bt_harvest_all_paged,
        )
        from app.workers.celery_tasks import harvest_mozzart_upcoming_task
        sp_harvest_all_paged.apply_async(queue="harvest", countdown=5)
        bt_harvest_all_paged.apply_async(queue="harvest", countdown=15)
        harvest_mozzart_upcoming_task.apply_async(queue="harvest", countdown=25)
        dispatched.extend(["sp", "bt", "mz"])
    except ImportError as exc:
        log.warning("[startup] harvest_pages import failed: %s", exc)

    try:
        from app.workers.tasks_harvest_b2b import b2b_harvest_all_paged, b2b_harvest_all_live
        b2b_harvest_all_paged.apply_async(queue="harvest", countdown=35)
        b2b_harvest_all_live.apply_async(queue="harvest",  countdown=60)
        dispatched.append("b2b")
    except ImportError as exc:
        log.warning("[startup] b2b harvest import failed: %s", exc)

    log.info("[startup] dispatched harvests: %s", dispatched)


# =============================================================================
# LEGACY TASK ALIASES
# =============================================================================

@celery.task(name="tasks.ops.update_match_results",   soft_time_limit=6000,  time_limit=9000)
def update_match_results():
    return {"ok": True}

@celery.task(name="tasks.ops.dispatch_notifications", soft_time_limit=3000,  time_limit=6000)
def dispatch_notifications(**kwargs):
    return {"ok": True}

@celery.task(name="tasks.ops.publish_ws_event",       soft_time_limit=1000,  time_limit=1500)
def publish_ws_event(channel: str, data: dict):
    _publish(channel, data)
    return {"ok": True}

@celery.task(name="tasks.ops.health_check",           soft_time_limit=1000,  time_limit=1500)
def healthcheck():
    r  = _get_redis(); ok = False
    try: r.ping(); ok = True
    except Exception: pass
    return {"ok": True, "redis": ok, "ts": time.time()}

@celery.task(name="tasks.ops.expire_subscriptions",   soft_time_limit=3000,  time_limit=6000)
def expire_subscriptions():
    return {"ok": True}

@celery.task(name="tasks.ops.cache_finished_games",   soft_time_limit=6000,  time_limit=9000)
def cache_finished_games():
    return {"ok": True}

@celery.task(name="tasks.ops.send_async_email",       soft_time_limit=3000,  time_limit=6000)
def send_async_email(**kwargs):
    return {"ok": True}

@celery.task(name="tasks.ops.send_message",           soft_time_limit=3000,  time_limit=6000)
def send_message(**kwargs):
    return {"ok": True}

@celery.task(
    name="tasks.ops.persist_combined_batch",
    bind=True, max_retries=3, default_retry_delay=10,
    soft_time_limit=12000, time_limit=15000, acks_late=True,
)
def persist_combined_batch(self, match_dicts: list, sport_slug: str = "soccer",
                            mode: str = "upcoming") -> dict:
    try:
        from app.workers.persist_hook import persist_from_serialized
        return persist_from_serialized(match_dicts, sport_slug=sport_slug, mode=mode)
    except Exception as exc:
        log.error("persist_combined_batch failed [%s]: %s", sport_slug, exc)
        raise self.retry(exc=exc)

@celery.task(name="tasks.ops.persist_all_sports",     soft_time_limit=12000, time_limit=15000)
def persist_all_sports(**kwargs):
    return {"ok": True}

@celery.task(name="tasks.ops.build_health_report",    soft_time_limit=3000,  time_limit=6000)
def build_health_report():
    return {"ok": True}


@celery.task(name="tasks.ops.save_match_result",
             bind=True, max_retries=3, default_retry_delay=10)
def save_match_result(self, join_key: str, result: dict):
    """Save a finished match result to DB. Fired by LiveFeedBridge / broadcaster."""
    try:
        from app.api.live_results_api import _save_result_now
        ok = _save_result_now(
            join_key,
            result.get("score_home"),
            result.get("score_away"),
            source=result.get("source", "celery"),
        )
        if not ok:
            raise Exception(f"save_result_now returned False for {join_key}")
    except Exception as exc:
        raise self.retry(exc=exc)


@celery.task(name="tasks.ops.update_match_state",
             bind=True, max_retries=2, default_retry_delay=5)
def update_match_state(self, join_key: str, new_state: str, meta: dict):
    try:
        from app.models.odds import UnifiedMatch
        from app.extensions import db
        um = None
        if join_key.startswith("br_"):
            um = UnifiedMatch.query.filter_by(parent_match_id=join_key[3:]).first()
        elif join_key.startswith("db_"):
            um = UnifiedMatch.query.get(int(join_key[3:]))
        else:
            um = UnifiedMatch.query.filter_by(parent_match_id=join_key).first()
        if um:
            um.status = new_state
            db.session.commit()
    except Exception as exc:
        raise self.retry(exc=exc)


# =============================================================================
# WORD REPORT PRE-GENERATION
# =============================================================================

_REPORT_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby",
    "ice-hockey", "volleyball", "handball", "table-tennis",
    "baseball", "mma", "boxing", "darts", "american-football", "esoccer",
]


@celery.task(
    name="tasks.ops.pre_generate_word_reports",
    soft_time_limit=540, time_limit=600, acks_late=True,
)
def pre_generate_word_reports() -> dict:
    import json as _json
    from app.views.customer.routes_api import (
        _generate_word_document, _save_minio_report, _get_minio_client,
    )
    from app.workers.celery_tasks import _redis as _get_redis

    successes = failures = skipped = sp_fetched = 0
    _r = _get_redis()
    _get_minio_client()

    def _sp_match_count(sport_slug: str) -> int:
        for key in (f"odds:sp:upcoming:{sport_slug}", f"sp:upcoming:{sport_slug}"):
            try:
                raw = _r.get(key)
                if not raw: continue
                obj = _json.loads(raw)
                if isinstance(obj, list): return len([m for m in obj if isinstance(m, dict)])
                if isinstance(obj, dict):
                    ms = obj.get("matches") or obj.get("data") or []
                    if isinstance(ms, list): return len([m for m in ms if isinstance(m, dict)])
            except Exception: pass
        return 0

    def _ensure_sp_data(sport_slug: str) -> bool:
        if _sp_match_count(sport_slug) > 0:
            return True
        try:
            from app.workers.tasks_upcoming import sp_harvest_sport
            sp_harvest_sport.apply(args=[sport_slug])
        except Exception as exc:
            log.warning("[word_reports] SP harvest for %s failed: %s", sport_slug, exc)
        for _ in range(18):
            time.sleep(5)
            if _sp_match_count(sport_slug) > 0:
                return True
        return False

    for sport in _REPORT_SPORTS:
        had_sp = _ensure_sp_data(sport)
        if not had_sp:
            log.warning("[word_reports] Skipping %s — no SP data", sport)
            skipped += 2; continue
        else:
            sp_fetched += 1
        for arb_only in (False, True):
            try:
                buf   = _generate_word_document(sport, arb_only)
                saved = _save_minio_report(sport, arb_only, buf)
                if saved:
                    _r.setex(
                        f"odds_report:ready:{sport}:{'arb' if arb_only else 'full'}",
                        600, _json.dumps({"sport": sport, "arb_only": arb_only, "ts": time.time()}),
                    )
                    successes += 1
                else:
                    skipped += 1
            except Exception as exc:
                failures += 1
                log.warning("[word_reports] ✗ %s arb=%s: %s", sport, arb_only, exc)

    return {"ok": True, "sports": len(_REPORT_SPORTS), "success": successes,
            "failures": failures, "skipped": skipped, "sp_fetched": sp_fetched}


@celery.task(
    name="tasks.ops.pre_generate_preset_reports",
    bind=True, max_retries=1,
    soft_time_limit=900, time_limit=1200, acks_late=True,
)
def pre_generate_preset_reports(self, preset: str) -> dict:
    import json as _json
    from datetime import datetime as _dt, timezone as _tz, timedelta
    from app.views.customer.routes_api import _generate_word_document, _save_minio_report
    from app.workers.celery_tasks import _redis as _get_redis

    now = _dt.now(_tz.utc)
    start_dt = end_dt = None
    if preset == "today":
        start_dt = now
        eat_now  = now + timedelta(hours=3)
        end_dt   = eat_now.replace(hour=23, minute=59, second=59, microsecond=999999) - timedelta(hours=3)
    elif preset == "tomorrow":
        eat_now             = now + timedelta(hours=3)
        eat_tomorrow_start  = (eat_now + timedelta(days=1)).replace(hour=0,  minute=0,  second=0,  microsecond=0)
        eat_tomorrow_end    = (eat_now + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
        start_dt = eat_tomorrow_start - timedelta(hours=3)
        end_dt   = eat_tomorrow_end   - timedelta(hours=3)
    elif preset == "week":
        start_dt = now; end_dt = now + timedelta(days=7)
    elif preset == "month":
        start_dt = now; end_dt = now + timedelta(days=30)
    else:
        preset = "all"

    s_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ") if start_dt else None
    e_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ") if end_dt else None

    successes = failures = skipped = sp_fetched = 0
    _r = _get_redis()

    for sport in _REPORT_SPORTS:
        for arb_only in (False, True):
            try:
                buf, sp_avail = _generate_word_document(sport, arb_only, start_time_str=s_str, end_time_str=e_str)
                saved = _save_minio_report(sport, arb_only, buf, preset=preset)
                if saved:
                    _r.set(f"odds_report:sp_available:{sport}:{preset}", "1" if sp_avail else "0", ex=86400)
                    _r.setex(
                        f"odds_report:ready:{sport}:{preset}:{'arb' if arb_only else 'full'}",
                        3600,
                        _json.dumps({"sport": sport, "arb_only": arb_only, "preset": preset, "ts": time.time()}),
                    )
                    successes += 1
                else:
                    skipped += 1
            except Exception as exc:
                failures += 1
                log.warning("[word_reports_%s] ✗ %s arb=%s: %s", preset, sport, arb_only, exc)

    return {"ok": True, "preset": preset, "sports": len(_REPORT_SPORTS),
            "success": successes, "failures": failures, "skipped": skipped}


@celery.task(name="tasks.ops.monitor_tailscale_proxies", bind=True, max_retries=1)
def monitor_tailscale_proxies(self) -> dict:
    import json as _json, os
    from app.workers.celery_tasks import _redis as _get_redis
    from app.workers.email_jobs import send_async_email

    r = _get_redis()
    if not r:
        return {"ok": False, "error": "Redis unavailable"}

    raw = r.get("tailscale:active_proxies")
    active_count = 0
    if raw:
        try:
            proxies = _json.loads(raw)
            if isinstance(proxies, list):
                active_count = len(proxies)
        except Exception:
            pass

    if active_count == 0 and not r.get("tailscale:alert_sent_lock"):
        r.setex("tailscale:alert_sent_lock", 3600, "1")
        try:
            send_async_email.apply_async(args=[
                "⚠️ ALERT: OddsKenya Tailscale Proxies Connection Down!",
                ["orenjagidraf@gmail.com"],
                "<p>No active Tailscale SOCKS5 proxies detected.</p>",
                "html", [],
                os.environ.get("ADMIN_EMAIL"),
                os.environ.get("ADMIN_EMAIL_PASSWORD"),
            ])
        except Exception as e:
            log.error("[tailscale_monitor] email failed: %s", e)
        return {"ok": True, "alert_sent": True, "active_count": 0}

    return {"ok": True, "alert_sent": False, "active_count": active_count}


# =============================================================================
# HELPERS
# =============================================================================

def _xp(p) -> float:
    if isinstance(p, (int, float)): return float(p)
    if isinstance(p, dict):
        return float(p.get("odd") or p.get("odds") or p.get("price") or p.get("value") or 0)
    return 0.0

def _xp_db(p) -> float:
    if isinstance(p, (int, float)): return float(p)
    if isinstance(p, dict):
        return float(p.get("best_price") or p.get("odd") or p.get("odds") or p.get("price") or 0)
    return 0.0

def _name_key(m: dict) -> str:
    h = (m.get("home_team") or m.get("home_team_name") or "")[:8].lower()
    a = (m.get("away_team") or m.get("away_team_name") or "")[:8].lower()
    return f"{h}|{a}"

def _load_match_for_ev(r, match_id) -> dict | None:
    for mode in ("upcoming", "live"):
        for sport in _ALL_SPORTS:
            raw = r.get(f"odds:unified:{mode}:{sport}")
            if not raw: continue
            try:
                d  = json.loads(raw)
                ms = d if isinstance(d, list) else d.get("matches", [])
                for m in ms:
                    mid = m.get("match_id") or m.get("parent_match_id") or m.get("join_key")
                    if str(mid) == str(match_id):
                        return m
            except Exception:
                pass
    try:
        from app.models.odds import UnifiedMatch
        um = UnifiedMatch.query.get(match_id)
        if um:
            return {
                "match_id":   um.id,
                "join_key":   um.parent_match_id,
                "home_team":  um.home_team_name,
                "away_team":  um.away_team_name,
                "sport":      (um.sport_name or "soccer").lower(),
                "markets":    getattr(um, "markets", {}),
                "bookmakers": getattr(um, "bookmaker_odds", {}),
            }
    except Exception:
        pass
    return None

def _patch_unified(r, sport: str, join_key: str, patch: dict) -> None:
    for mode in ("upcoming", "live"):
        key = f"odds:unified:{mode}:{sport}"
        raw = r.get(key)
        if not raw: continue
        try:
            d  = json.loads(raw)
            ms = d if isinstance(d, list) else d.get("matches", [])
            for m in ms:
                jk = str(m.get("join_key") or m.get("parent_match_id") or "")
                if jk == str(join_key):
                    m.update(patch); break
            payload = ms if isinstance(d, list) else {**d, "matches": ms}
            _smart_set(r, key, payload, ttl=TTL_UNIFIED)
        except Exception:
            pass

def _persist_ev_arb(match_id, result: dict) -> None:
    try:
        from app.extensions import db
        from sqlalchemy import text
        with db.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO match_ev_arb
                    (match_id, has_arb, best_arb_pct, has_ev, best_ev_pct,
                     arb_count, ev_count, computed_at)
                VALUES (:mid, :arb, :ap, :ev, :ep, :ac, :ec, NOW())
                ON CONFLICT (match_id) DO UPDATE SET
                    has_arb=EXCLUDED.has_arb, best_arb_pct=EXCLUDED.best_arb_pct,
                    has_ev=EXCLUDED.has_ev, best_ev_pct=EXCLUDED.best_ev_pct,
                    arb_count=EXCLUDED.arb_count, ev_count=EXCLUDED.ev_count,
                    computed_at=EXCLUDED.computed_at
            """), {
                "mid": match_id, "arb": result["has_arb"], "ap": result["best_arb_pct"],
                "ev": result["has_ev"], "ep": result["best_ev_pct"],
                "ac": result["arb_count"], "ec": len(result.get("ev_opportunities", [])),
            })
            conn.commit()
    except Exception as exc:
        log.debug("[persist_ev_arb] skipped: %s", exc)