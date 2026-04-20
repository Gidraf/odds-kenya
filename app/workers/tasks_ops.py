"""
app/workers/tasks_ops.py
=========================
Core ops tasks: EV/arb computation, beat schedule, startup dispatcher.

IMPORTANT: compute_ev_arb MUST live here — imported by celery_tasks._upsert_and_chain
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

_ALL_BKS   = ["sp", "bt", "od", "1xbet", "22bet", "betwinner", "melbet", "megapari", "helabet", "paripesa"]
_LOCAL_BKS = ["sp", "bt", "od"]
_B2B_BKS   = ["1xbet", "22bet", "betwinner", "melbet", "megapari", "helabet", "paripesa"]

# Tier access rules — used by live.py and upcoming stream
TIER_CONFIG = {
    "free":    {"sports": ["soccer"],  "days_ahead": 1,  "markets_api": False},
    "basic":   {"sports": _ALL_SPORTS, "days_ahead": 1,  "markets_api": False},
    "pro":     {"sports": _ALL_SPORTS, "days_ahead": 7,  "markets_api": "local"},
    "premium": {"sports": _ALL_SPORTS, "days_ahead": 30, "markets_api": "all"},
    "admin":   {"sports": _ALL_SPORTS, "days_ahead": 90, "markets_api": "all"},
}

_ARB_MARKETS_2  = {"btts", "odd_even", "both_teams_to_score"}
_ARB_MARKETS_3  = {"match_winner", "1x2", "moneyline", "3way"}
_OU_PREFIX      = "over_under_"


# ══════════════════════════════════════════════════════════════════════════════
# EV / ARB COMPUTATION  (called by _upsert_and_chain in celery_tasks.py)
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.ops.compute_ev_arb",
    bind=True, max_retries=2, default_retry_delay=5,
    soft_time_limit=30, time_limit=45, acks_late=True,
)
def compute_ev_arb(self, match_id) -> dict:
    """
    Compute EV / arbitrage across ALL 10 bookmakers for one match.
    - Reads from Redis unified snapshot (real-time first, DB fallback)
    - Publishes arb:updates and ev:updates channels
    - Patches arb/ev flags back onto the Redis unified record
    """
    r          = _get_redis()
    match_data = _load_match_for_ev(r, match_id)
    if not match_data:
        return {"ok": False, "reason": "not_found", "match_id": match_id}

    sport    = (match_data.get("sport") or "soccer").lower()
    home     = match_data.get("home_team", "?")
    away     = match_data.get("away_team", "?")
    join_key = str(match_data.get("join_key") or match_data.get("parent_match_id") or match_id)

    # ── Build best-odds matrix ────────────────────────────────────────────
    best: dict[str, dict[str, dict]] = {}   # market → outcome → {odd, bk}

    # From bookmakers dict (Redis unified / B2B format)
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

    # From top-level markets (SP/BT/OD DB API format)
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

    # ── Detect arb / EV ───────────────────────────────────────────────────
    arbs: list[dict] = []
    evs:  list[dict] = []

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

        use = keys[:exp]
        sum_inv = 0.0
        valid   = True
        for k in use:
            odd = ob[k]["odd"]
            if odd <= 1.0:
                valid = False
                break
            sum_inv += 1.0 / odd

        if not valid or sum_inv <= 0:
            continue

        if sum_inv < 1.0:
            profit = round((1.0 - sum_inv) * 100, 3)
            legs   = [{
                "outcome": k, "odd": ob[k]["odd"], "bk": ob[k]["bk"],
                "stake": round((1.0 / ob[k]["odd"]) / sum_inv, 4),
            } for k in use]
            arbs.append({
                "market": mkt, "profit_pct": profit,
                "sum_inv": round(sum_inv, 6), "legs": legs,
                "bks_used": list({l["bk"] for l in legs}),
            })

        # EV: reward long odds vs market consensus
        for k in use:
            odd = ob[k]["odd"]
            if odd > 1.0:
                fair_p   = (1.0 / odd) / sum_inv    # normalised implied prob
                ev_pct   = round((odd * fair_p - 1) * 100, 2)
                if ev_pct > 3.0:
                    evs.append({
                        "market": mkt, "outcome": k,
                        "odd": odd, "bk": ob[k]["bk"], "ev_pct": ev_pct,
                    })

    has_arb  = bool(arbs)
    has_ev   = bool(evs)
    best_arb = max((a["profit_pct"] for a in arbs), default=0.0)
    best_ev  = max((e["ev_pct"]     for e in evs),  default=0.0)

    result = {
        "join_key":          join_key,
        "match_id":          match_id,
        "home_team":         home,
        "away_team":         away,
        "sport":             sport,
        "has_arb":           has_arb,
        "best_arb_pct":      best_arb,
        "arb_count":         len(arbs),
        "arb_opportunities": arbs,
        "has_ev":            has_ev,
        "best_ev_pct":       best_ev,
        "ev_opportunities":  evs,
        "bk_count":          len(match_data.get("bookmakers") or {}),
        "market_count":      len(best),
        "computed_at":       _now_iso(),
    }

    # ── Publish ───────────────────────────────────────────────────────────
    if has_arb:
        _publish(f"arb:updates:{sport}", {"event": "arb_updated", "type": "arb_updated", **result})
    if has_ev:
        _publish(f"ev:updates:{sport}",  {"event": "ev_updated",  "type": "ev_updated",  **result})

    # ── Patch unified Redis record ────────────────────────────────────────
    _patch_unified(r, sport, join_key, {
        "has_arb":           has_arb,
        "best_arb_pct":      best_arb,
        "has_ev":            has_ev,
        "best_ev_pct":       best_ev,
        "arb_opportunities": arbs[:3],
        "ev_opportunities":  evs[:3],
    })

    _persist_ev_arb(match_id, result)
    logger.info("[ev_arb] %s v %s arb=%s %.2f%% ev=%s bks=%d",
                home, away, has_arb, best_arb, has_ev, result["bk_count"])
    return {"ok": True, **result}


# ══════════════════════════════════════════════════════════════════════════════
# PER-BK INDEPENDENT PUBLISH
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(name="tasks.ops.publish_bk_snapshot", soft_time_limit=20, time_limit=30)
def publish_bk_snapshot(bk_slug: str, mode: str, sport: str, matches: list[dict]) -> dict:
    """
    Each BK calls this after harvest. Saves to its own key, then merges
    into the unified snapshot. SP/BT/OD run independently.
    """
    r = _get_redis()
    r.setex(f"odds:{bk_slug}:{mode}:{sport}", 7200, json.dumps({
        "bk": bk_slug, "mode": mode, "sport": sport,
        "matches": matches, "ts": time.time(),
    }, default=str))
    _merge_bk_into_unified(r, bk_slug, mode, sport, matches)
    _publish(f"odds:{bk_slug}:{mode}:{sport}:ready", {
        "event": "snapshot_ready", "bk": bk_slug, "mode": mode,
        "sport": sport, "count": len(matches), "ts": time.time(),
    })
    return {"ok": True, "bk": bk_slug, "count": len(matches)}


def _merge_bk_into_unified(r, bk_slug: str, mode: str, sport: str,
                            new_matches: list[dict]) -> None:
    """
    Incremental merge: add/update this BK's matches into the unified snapshot.
    Keeps all other BKs' data intact.
    """
    key = f"odds:unified:{mode}:{sport}"
    try:
        raw      = r.get(key)
        existing = []
        if raw:
            d        = json.loads(raw)
            existing = d if isinstance(d, list) else d.get("matches", [])

        # Build index
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
                if not em.get("bookmakers"):
                    em["bookmakers"] = {}
                em["bookmakers"][bk_slug] = {
                    "match_id": nm.get("match_id") or nm.get("external_id") or "",
                    "markets":  mkts,
                }
                for bk, bd in (nm.get("bookmakers") or {}).items():
                    em["bookmakers"].setdefault(bk, bd)
                for fld in ("score_home", "score_away", "is_live", "match_time"):
                    if nm.get(fld) is not None:
                        em[fld] = nm[fld]
            else:
                nr = dict(nm)
                if not nr.get("bookmakers"):
                    nr["bookmakers"] = {}
                if mkts:
                    nr["bookmakers"][bk_slug] = {
                        "match_id": nm.get("match_id") or "",
                        "markets":  mkts,
                    }
                p2 = len(existing)
                existing.append(nr)
                if jk: idx[jk] = p2
                idx[nk] = p2

        r.setex(key, 7200, json.dumps({
            "mode": mode, "sport": sport, "source": "unified",
            "matches": existing, "updated_at": time.time(),
        }, default=str))
        _publish(f"odds:all:{mode}:{sport}:updates", {
            "event": "snapshot_ready", "bk": bk_slug,
            "mode": mode, "sport": sport, "count": len(existing), "ts": time.time(),
        })
    except Exception as exc:
        logger.warning("[merge_bk] %s %s %s: %s", bk_slug, mode, sport, exc)


# ══════════════════════════════════════════════════════════════════════════════
# BEAT SCHEDULE + STARTUP
# ══════════════════════════════════════════════════════════════════════════════

def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(300.0,  _beat_harvest_all_paged.s(),    name="harvest:all_paged 5min")
    sender.add_periodic_task(90.0,   _beat_b2b_live.s(),             name="b2b:live 90s")
    sender.add_periodic_task(600.0,  _beat_alignment.s(),            name="align:full 10min")
    sender.add_periodic_task(1800.0, _beat_prune.s(),                name="prune:redis 30min")
    logger.info("[tasks_ops] beat schedule registered")


celery.on_after_configure.connect(setup_periodic_tasks)


@celery.task(name="tasks.ops.beat.harvest_all_paged",  soft_time_limit=30, time_limit=60)
def _beat_harvest_all_paged():
    from app.workers.tasks_harvest_pages import harvest_all_paged
    harvest_all_paged.apply_async(queue="harvest")
    return {"ok": True}


@celery.task(name="tasks.ops.beat.b2b_live",           soft_time_limit=30, time_limit=60)
def _beat_b2b_live():
    from app.workers.tasks_harvest_b2b import b2b_harvest_all_live
    b2b_harvest_all_live.apply_async(queue="harvest")
    return {"ok": True}


@celery.task(name="tasks.ops.beat.alignment",          soft_time_limit=30, time_limit=60)
def _beat_alignment():
    from celery import group as cg
    from app.workers.tasks_align import align_sport
    cg([align_sport.s(s, 500) for s in _ALL_SPORTS]).apply_async(queue="results")
    return {"ok": True}


@celery.task(name="tasks.ops.beat.prune",              soft_time_limit=60, time_limit=90)
def _beat_prune():
    r = _get_redis()
    n = 0
    for pat in ["odds:b2b:*", "odds:sp:*", "odds:bt:*", "odds:od:*", "odds:unified:*"]:
        c = 0
        while True:
            c, keys = r.scan(c, match=pat, count=200)
            for k in keys:
                if r.ttl(k) == -1:
                    r.expire(k, 7200)
                    n += 1
            if c == 0:
                break
    return {"ok": True, "pruned": n}


# ── Legacy task name aliases (TASK_ROUTES in celery_tasks.py) ────────────────

@celery.task(name="tasks.ops.update_match_results",    soft_time_limit=60,  time_limit=90)
def update_match_results():
    return {"ok": True}


@celery.task(name="tasks.ops.dispatch_notifications",  soft_time_limit=30,  time_limit=60)
def dispatch_notifications(**kwargs):
    return {"ok": True}


@celery.task(name="tasks.ops.publish_ws_event",        soft_time_limit=10,  time_limit=15)
def publish_ws_event(channel: str, data: dict):
    _publish(channel, data)
    return {"ok": True}


@celery.task(name="tasks.ops.health_check",            soft_time_limit=10,  time_limit=15)
def healthcheck():
    r = _get_redis()
    ok = False
    try:
        r.ping(); ok = True
    except Exception:
        pass
    return {"ok": True, "redis": ok, "ts": time.time()}


@celery.task(name="tasks.ops.expire_subscriptions",    soft_time_limit=30,  time_limit=60)
def expire_subscriptions():
    return {"ok": True}


@celery.task(name="tasks.ops.cache_finished_games",    soft_time_limit=60,  time_limit=90)
def cache_finished_games():
    return {"ok": True}


@celery.task(name="tasks.ops.send_async_email",        soft_time_limit=30,  time_limit=60)
def send_async_email(**kwargs):
    return {"ok": True}


@celery.task(name="tasks.ops.send_message",            soft_time_limit=30,  time_limit=60)
def send_message(**kwargs):
    return {"ok": True}


@celery.task(name="tasks.ops.persist_combined_batch",  soft_time_limit=120, time_limit=150)
def persist_combined_batch(**kwargs):
    return {"ok": True}


@celery.task(name="tasks.ops.persist_all_sports",      soft_time_limit=120, time_limit=150)
def persist_all_sports(**kwargs):
    return {"ok": True}


@celery.task(name="tasks.ops.build_health_report",     soft_time_limit=30,  time_limit=60)
def build_health_report():
    return {"ok": True}


@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    try:
        _dispatch_startup_harvests()
    except Exception as exc:
        log.warning("[tasks_ops] startup dispatch failed: %s", exc)


def _dispatch_startup_harvests():
    from app.workers.tasks_harvest_pages import (
        sp_harvest_all_paged, bt_harvest_all_paged, od_harvest_all_paged,
    )
    from app.workers.tasks_harvest_b2b import b2b_harvest_all_paged, b2b_harvest_all_live
    log.info("[tasks_ops] dispatching startup harvests...")
    sp_harvest_all_paged.apply_async(queue="harvest", countdown=5)
    bt_harvest_all_paged.apply_async(queue="harvest", countdown=15)
    od_harvest_all_paged.apply_async(queue="harvest", countdown=25)
    b2b_harvest_all_paged.apply_async(queue="harvest", countdown=35)
    b2b_harvest_all_live.apply_async(queue="harvest", countdown=60)
    log.info("[tasks_ops] startup dispatched (SP+BT+OD+B2Bx7 upcoming + B2B live)")


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _xp(p) -> float:
    """Extract decimal price from any format."""
    if isinstance(p, (int, float)): return float(p)
    if isinstance(p, dict):
        return float(p.get("odd") or p.get("odds") or p.get("price") or p.get("value") or 0)
    return 0.0


def _xp_db(p) -> float:
    """Extract best_price from DB API format."""
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
                d = json.loads(raw)
                ms = d if isinstance(d, list) else d.get("matches", [])
                for m in ms:
                    mid = m.get("match_id") or m.get("parent_match_id") or m.get("join_key")
                    if str(mid) == str(match_id):
                        return m
            except Exception:
                pass
    # DB fallback
    try:
        from app.models.odds import UnifiedMatch
        um = UnifiedMatch.query.get(match_id)
        if um:
            return {
                "match_id":  um.id, "join_key":   um.parent_match_id,
                "home_team": um.home_team_name, "away_team":  um.away_team_name,
                "sport":     (um.sport_name or "soccer").lower(),
                "markets":   getattr(um, "markets", {}),
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
                    m.update(patch)
                    break
            payload = json.dumps(ms if isinstance(d, list) else {**d, "matches": ms}, default=str)
            r.setex(key, 7200, payload)
        except Exception:
            pass


def _persist_ev_arb(match_id, result: dict) -> None:
    try:
        from app.extensions import db
        from sqlalchemy import text
        with db.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO match_ev_arb
                    (match_id,has_arb,best_arb_pct,has_ev,best_ev_pct,arb_count,ev_count,computed_at)
                VALUES (:mid,:arb,:ap,:ev,:ep,:ac,:ec,NOW())
                ON CONFLICT (match_id) DO UPDATE SET
                    has_arb=EXCLUDED.has_arb, best_arb_pct=EXCLUDED.best_arb_pct,
                    has_ev=EXCLUDED.has_ev,   best_ev_pct=EXCLUDED.best_ev_pct,
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