"""
app/workers/tasks_upcoming.py
==============================
SP is the sole source of truth.  BT and OD are enriched directly using
the betradar_id that SP stamps on every match — no list fetching required.

Flow
─────
  sp_harvest_sport(sport_slug)
    → fetches SP matches, persists them
    → dispatches sp_cross_bk_enrich  (10 s countdown)
    → dispatches sp_enrich_analytics (60 s countdown)
    → dispatches tasks.bt_od.harvest_sport (30 s countdown)
         ↳ reads SP cache (now warm), fetches BT+OD by team name,
           persists only matches in 2+ bookmakers

  sp_cross_bk_enrich(sport_slug)
    → reads SP cache
    → for each match with betradar_id (parallel, 8 workers):
        BT: GET api.betika.com/v1/uo/match?parent_match_id={betradar_id}
        OD: GET api.odi.site/sportsbook/v1?resource=sportevent&id={betradar_id}
    → NO list fetch, NO lookup tables

  sp_enrich_analytics(sport_slug)
    → reads SP cache, fetches Sportradar stats per betradar_id

BT / OD standalone tasks are kept for manual triggering only.
They are NOT in the beat schedule and NOT called at startup.
"""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timezone, timedelta
from calendar import monthrange

from celery import group
from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded

from app.workers.celery_tasks import (
    celery, cache_set, cache_get, _now_iso, _publish,
    _upsert_and_chain, _extract_betradar_id, _normalise_sport_name,
    _get_or_create_bookmaker,
)

logger = get_task_logger(__name__)

_LOCAL_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
]

_ALL_SPORTS: list[str] = [
    "soccer",
    "basketball",
    "tennis",
    "cricket",
    "rugby",
    "ice-hockey",
    "volleyball",
    "handball",
    "table-tennis",
    "baseball",
    "mma",
    "boxing",
    "darts",
    "american-football",
    "esoccer",
]

_B2B_HARVEST_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
    "darts", "handball",
]
_B2B_SPORTS = [
    "Football", "Basketball", "Tennis", "Ice Hockey",
    "Volleyball", "Cricket", "Rugby", "Table Tennis",
]
_SBO_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "boxing",
    "handball", "mma", "table-tennis",
]

PAGE_SIZE         = 15
MAX_PAGES         = 6
WS_CHANNEL        = "odds:updates"
ARB_CHANNEL       = "arb:updates"
EV_CHANNEL        = "ev:updates"
SP_MAX_MATCHES    = 3_000
BT_MAX_MATCHES    = 3_000   # manual only
_CROSS_BK_WORKERS = 8
_ANALYTICS_TTL    = 86_400
_NEAR_TERM_DAYS   = 7

_BK_NAMES: dict[str, str] = {
    "sp":  "SportPesa", "bt": "Betika",
    "od":  "OdiBets",   "b2b": "1xBet", "sbo": "SBO",
}
_SPORT_SLUG_TO_DB: dict[str, str] = {
    "soccer":        "Soccer",       "football":    "Soccer",
    "basketball":    "Basketball",   "tennis":      "Tennis",
    "ice-hockey":    "Ice Hockey",   "volleyball":  "Volleyball",
    "cricket":       "Cricket",      "rugby":       "Rugby",
    "table-tennis":  "Table Tennis", "handball":    "Handball",
    "mma":           "MMA",          "boxing":      "Boxing",
    "darts":         "Darts",        "esoccer":     "eSoccer",
}


# =============================================================================
# HELPERS
# =============================================================================

def _emit(source: str, sport: str, count: int, latency: int) -> None:
    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": source,
        "sport": sport, "mode": "upcoming",
        "count": count, "latency_ms": latency, "ts": _now_iso(),
    })


def _schedule_alignment(sport_slug: str, countdown: int = 60) -> None:
    try:
        from app.workers.tasks_market_align import align_sport_markets
        align_sport_markets.apply_async(
            args=[sport_slug, 100], queue="results", countdown=countdown,
        )
    except Exception as exc:
        logger.warning("[harvest] alignment schedule failed %s: %s", sport_slug, exc)


def _fuzzy_find_match(home: str, away: str, start_time_raw) -> str | None:
    if not home or not away:
        return None
    try:
        from app.models.odds_model import UnifiedMatch
        from sqlalchemy import func
        start_dt = None
        if start_time_raw:
            try:
                if isinstance(start_time_raw, str):
                    start_dt = datetime.fromisoformat(start_time_raw.replace("Z", "+00:00"))
                elif isinstance(start_time_raw, (int, float)):
                    start_dt = datetime.fromtimestamp(float(start_time_raw), tz=timezone.utc)
                elif isinstance(start_time_raw, datetime):
                    start_dt = start_time_raw
            except Exception:
                pass
        q = UnifiedMatch.query.filter(
            func.lower(UnifiedMatch.home_team_name) == home.lower().strip(),
            func.lower(UnifiedMatch.away_team_name) == away.lower().strip(),
        )
        if start_dt:
            w = timedelta(minutes=90)
            q = q.filter(
                UnifiedMatch.start_time >= start_dt - w,
                UnifiedMatch.start_time <= start_dt + w,
            )
        um = q.first()
        return um.parent_match_id if um else None
    except Exception as exc:
        logger.debug("[fuzzy_find] %s vs %s: %s", home, away, exc)
        return None


def _resolve_match_id(m: dict, bk_slug: str) -> tuple[str | None, str | None]:
    betradar_id = _extract_betradar_id(m)
    if not betradar_id:
        home  = str(m.get("home_team") or m.get("home_team_name") or "").strip()
        away  = str(m.get("away_team") or m.get("away_team_name") or "").strip()
        start = m.get("start_time") or ""
        found = _fuzzy_find_match(home, away, start)
        if found:
            betradar_id = found
            
    # CRITICAL FIX: Prioritize specific native parent IDs for accurate API querying later
    bk_ext_id = str(
        m.get("sp_game_id")  or 
        m.get("bt_parent_id") or m.get("bt_match_id") or
        m.get("od_parent_id") or m.get("od_event_id") or m.get("od_match_id") or 
        m.get("match_id")    or m.get("event_id")    or ""
    ).strip() or None
    
    return betradar_id, bk_ext_id

def _persist_bk_matches(matches: list[dict], bk_slug: str, sport_slug: str) -> None:
    if not matches:
        return
    bk_id = _get_or_create_bookmaker(_BK_NAMES.get(bk_slug, bk_slug.upper()))
    if not bk_id:
        logger.warning("[persist_bk] could not resolve %s — skipping", bk_slug)
        return
    canonical_sport = _SPORT_SLUG_TO_DB.get(sport_slug, sport_slug)
    serialized: list[dict] = []
    for m in matches:
        betradar_id, bk_ext_id = _resolve_match_id(m, bk_slug)
        if betradar_id:
            join_key = f"br_{betradar_id}"
        elif bk_ext_id:
            join_key = f"{bk_slug}_{bk_ext_id}"
        else:
            continue
        markets = m.get("markets") or {}
        if not markets:
            continue
        serialized.append({
            "join_key":       join_key,
            "home_team":      m.get("home_team")   or m.get("home_team_name")   or "",
            "away_team":      m.get("away_team")   or m.get("away_team_name")   or "",
            "competition":    m.get("competition") or m.get("competition_name") or "",
            "start_time":     m.get("start_time")  or "",
            "is_live":        False,
            "betradar_id":    betradar_id,
            "sport":          canonical_sport,
            "bk_ids":         {bk_slug: bk_ext_id or join_key},
            "markets":        markets,
            "bookmaker_slug": bk_slug,
        })
    if not serialized:
        return
    dispatched = 0
    for i in range(0, len(serialized), 500):
        chunk = serialized[i: i + 500]
        try:
            celery.send_task(
                "tasks.ops.persist_combined_batch",
                args=[chunk, sport_slug, "upcoming"],
                queue="results", countdown=3,
            )
            dispatched += len(chunk)
        except Exception as exc:
            logger.warning("[persist_bk] dispatch failed %s/%s: %s", bk_slug, sport_slug, exc)
    logger.info("[persist_bk] %s/%s: dispatched %d/%d",
                bk_slug, sport_slug, dispatched, len(serialized))


def _persist_b2b_matches(matches: list[dict], sport_slug: str) -> None:
    if not matches:
        return
    bk_batches: dict[str, list[dict]] = {}
    for m in matches:
        for bk_name, bk_data in (m.get("bookmakers") or {}).items():
            bk_mkts = (bk_data or {}).get("markets") or {}
            if not bk_mkts:
                continue
            flat = {**m, "markets": bk_mkts, "betradar_id": m.get("betradar_id") or ""}
            flat.pop("bookmakers", None)
            slug = {v: k for k, v in _BK_NAMES.items()}.get(bk_name, bk_name[:4].lower())
            bk_batches.setdefault(slug, []).append(flat)
    for slug, batch in bk_batches.items():
        _upsert_and_chain(batch, _BK_NAMES.get(slug, slug.upper()))
        _persist_bk_matches(batch, slug, sport_slug)


def _is_near_term(start_time_str: str, days: int = _NEAR_TERM_DAYS) -> bool:
    if not start_time_str:
        return False
    try:
        st  = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return timedelta(0) <= (st - now) <= timedelta(days=days)
    except Exception:
        return False


# =============================================================================
# REGISTRY PIPELINE
# =============================================================================

@celery.task(
    name="harvest.bookmaker_sport",
    bind=True, max_retries=2, default_retry_delay=60,
    soft_time_limit=300, time_limit=360, acks_late=True,
)
def harvest_bookmaker_sport(self, bookmaker_slug: str, sport_slug: str) -> dict:
    from app.workers.harvest_registry import get_bookmaker
    t0 = time.perf_counter()
    bk = get_bookmaker(bookmaker_slug)
    if not bk or not bk["enabled"] or sport_slug not in bk["sports"]:
        return {"ok": True, "skipped": True}
    try:
        matches: list[dict] = bk["fetch_fn"](sport_slug)
    except Exception as exc:
        raise self.retry(exc=exc)
    latency_ms = int((time.perf_counter() - t0) * 1000)
    cache_set(f"odds:upcoming:{bookmaker_slug}:{sport_slug}", {
        "bookmaker": bookmaker_slug, "sport": sport_slug,
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency_ms, "matches": matches,
    }, ttl=bk["redis_ttl"])
    _upsert_and_chain(matches, bk["label"])
    _persist_bk_matches(matches, bookmaker_slug, sport_slug)
    _publish("odds:harvest:done", {
        "type": "harvest_done", "bookmaker": bookmaker_slug,
        "sport": sport_slug, "count": len(matches), "ts": _now_iso(),
    })
    _schedule_alignment(sport_slug, countdown=60)
    return {"ok": True, "bookmaker": bookmaker_slug, "sport": sport_slug,
            "count": len(matches), "latency_ms": latency_ms}


@celery.task(name="harvest.all_upcoming", soft_time_limit=60, time_limit=120)
def harvest_all_registry_upcoming() -> dict:
    from app.workers.harvest_registry import ENABLED_BOOKMAKERS
    sigs = [
        harvest_bookmaker_sport.s(bk["slug"], sport)
        for bk in ENABLED_BOOKMAKERS for sport in bk["sports"]
    ]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


@celery.task(name="harvest.merge_broadcast", soft_time_limit=30, time_limit=60)
def merge_and_broadcast(sport_slug: str) -> dict:
    from app.workers.harvest_registry import ENABLED_BOOKMAKERS
    bk_slugs = [bk["slug"] for bk in ENABLED_BOOKMAKERS if sport_slug in bk["sports"]]
    merged: dict[str, dict] = {}
    seen_bk: list[str]      = []
    for slug in bk_slugs:
        cached = cache_get(f"odds:upcoming:{slug}:{sport_slug}")
        if not cached or not cached.get("matches"):
            continue
        seen_bk.append(slug)
        for match in cached["matches"]:
            br_id = _extract_betradar_id(match)
            key   = br_id or f"{match.get('home_team','')}|{match.get('away_team','')}"
            if not key:
                continue
            if key not in merged:
                merged[key] = {
                    "betradar_id": br_id,
                    "home_team":   match.get("home_team", ""),
                    "away_team":   match.get("away_team", ""),
                    "competition": match.get("competition", ""),
                    "start_time":  match.get("start_time"),
                    "sport":       sport_slug,
                    "event_ids":   {},
                    "markets":     {},
                }
            entry = merged[key]
            entry["event_ids"][slug] = str(match.get("sp_game_id") or match.get("event_id") or "")
            for mkt_slug, outcomes in (match.get("markets") or {}).items():
                entry["markets"].setdefault(mkt_slug, {})
                for out_key, odd_val in outcomes.items():
                    try:
                        fv = float(odd_val)
                    except (TypeError, ValueError):
                        continue
                    if fv <= 1.0:
                        continue
                    entry["markets"][mkt_slug].setdefault(out_key, {})
                    entry["markets"][mkt_slug][out_key][slug] = fv
    merged_list = list(merged.values())
    for match in merged_list:
        match["best_odds"] = {}
        for mkt_slug, outcomes in match["markets"].items():
            for out_key, bk_odds in outcomes.items():
                if bk_odds:
                    best_bk = max(bk_odds, key=lambda b: bk_odds[b])
                    match["best_odds"][f"{mkt_slug}____{out_key}"] = {
                        "bookmaker": best_bk, "odd": bk_odds[best_bk],
                    }
    cache_set(f"odds:upcoming:all:{sport_slug}", {
        "sport": sport_slug, "bookmakers": seen_bk,
        "match_count": len(merged_list), "harvested_at": _now_iso(),
        "matches": merged_list,
    }, ttl=14_400)
    _publish(f"odds:upcoming:{sport_slug}", {
        "type": "odds_updated", "sport": sport_slug,
        "bookmakers": seen_bk, "count": len(merged_list), "ts": _now_iso(),
    })
    compute_value_bets.apply_async(args=[sport_slug], queue="ev_arb")
    return {"ok": True, "sport": sport_slug, "events": len(merged_list)}


@celery.task(name="harvest.value_bets", soft_time_limit=60, time_limit=90)
def compute_value_bets(sport_slug: str) -> dict:
    from app.extensions import db
    from app.models.odds_model import ArbitrageOpportunity, OpportunityStatus
    
    SAFE_ARB_MARKETS = {
        "match_winner", "1x2", "moneyline", "btts", "both_teams_score",
        "double_chance", "odd_even"
    }

    raw = cache_get(f"odds:upcoming:all:{sport_slug}")
    if not raw:
        return {"ok": True, "found": 0}
    matches  = raw.get("matches") or []
    arb_rows = 0
    now      = datetime.now(timezone.utc)
    try:
        for match in matches:
            for mkt_slug, outcomes in (match.get("markets") or {}).items():
                
                # 1. Filter safe markets only
                is_safe = (
                    mkt_slug in SAFE_ARB_MARKETS or
                    "over_under" in mkt_slug or
                    "asian_handicap" in mkt_slug
                )
                if not is_safe:
                    continue

                if len(outcomes) < 2:
                    continue

                # 2. Strict outcome count verification
                if mkt_slug in ("match_winner", "1x2") and len(outcomes) != 3:
                    continue
                if ("over_under" in mkt_slug or mkt_slug in ("btts", "both_teams_score", "odd_even")) and len(outcomes) != 2:
                    continue

                best_prices: dict[str, float] = {}
                leg_details: dict[str, tuple] = {}
                is_complete = True

                for out_key, bk_odds in outcomes.items():
                    if not bk_odds:
                        is_complete = False
                        continue
                    best_bk  = max(bk_odds, key=lambda b: float(bk_odds[b]))
                    best_odd = float(bk_odds[best_bk])
                    if best_odd > 1.0:
                        best_prices[out_key] = best_odd
                        leg_details[out_key] = (best_bk, best_odd)
                    else:
                        is_complete = False

                # 3. Final validation: Must have all legs, must span multiple bookmakers
                unique_bks = {b for b, o in leg_details.values()}
                if not is_complete or len(best_prices) < 2 or len(unique_bks) < 2:
                    continue

                arb_sum = sum(1.0 / p for p in best_prices.values())
                if arb_sum >= 1.0:
                    continue

                profit_pct = (1.0 / arb_sum - 1.0) * 100
                if profit_pct < 0.5:
                    continue

                legs = [{
                    "selection": sel, "bookmaker": leg_details[sel][0],
                    "price": leg_details[sel][1],
                    "stake_pct": round((1.0 / leg_details[sel][1]) / arb_sum * 100, 2),
                } for sel in best_prices]

                start_dt = None
                if match.get("start_time"):
                    try:
                        start_dt = datetime.fromisoformat(
                            str(match["start_time"]).replace("Z", "+00:00"))
                    except Exception:
                        pass

                db.session.add(ArbitrageOpportunity(
                    home_team=match.get("home_team", ""),
                    away_team=match.get("away_team", ""),
                    sport=sport_slug, competition=match.get("competition", ""),
                    match_start=start_dt, market=mkt_slug,
                    profit_pct=round(profit_pct, 4),
                    peak_profit_pct=round(profit_pct, 4),
                    arb_sum=round(arb_sum, 6), legs_json=legs,
                    stake_100_returns=round(100 / arb_sum, 2),
                    bookmaker_ids=sorted({l["bookmaker"] for l in legs}),
                    status=OpportunityStatus.OPEN, open_at=now,
                ))
                arb_rows += 1
        db.session.commit()
    except Exception as exc:
        logger.error("[value_bets] %s: %s", sport_slug, exc)
        try:
            db.session.rollback()
        except Exception:
            pass
    return {"ok": True, "sport": sport_slug, "arbs": arb_rows}


@celery.task(name="harvest.cleanup", soft_time_limit=60, time_limit=90)
def cleanup_old_snapshots(days_keep: int = 7) -> dict:
    from app.extensions import db
    from app.models.odds_model import ArbitrageOpportunity, EVOpportunity
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_keep)
    n_a = ArbitrageOpportunity.query.filter(ArbitrageOpportunity.open_at < cutoff).delete()
    n_e = EVOpportunity.query.filter(EVOpportunity.open_at < cutoff).delete()
    db.session.commit()
    return {"ok": True, "arbs_deleted": n_a, "evs_deleted": n_e}


# =============================================================================
# SPORTPESA — source of truth
# =============================================================================

@celery.task(
    name="tasks.sp.harvest_sport", bind=True,
    max_retries=2, default_retry_delay=20,
    soft_time_limit=3600, time_limit=3660, acks_late=True,
)
def sp_harvest_sport(self, sport_slug: str, max_matches: int = SP_MAX_MATCHES) -> dict:
    t0      = time.perf_counter()
    matches = []
    try:
        from app.workers.sp_harvester import fetch_upcoming_stream
        logger.info("[sp] %s → fetching up to %d matches", sport_slug, max_matches)
        for match in fetch_upcoming_stream(
            sport_slug, fetch_full_markets=True,
            max_matches=max_matches, days=30, sleep_between=0.1,
        ):
            matches.append(match)
            if len(matches) % 500 == 0:
                logger.info("[sp] %s → %d/%d collected", sport_slug, len(matches), max_matches)
    except SoftTimeLimitExceeded:
        logger.warning("[sp] soft timeout %s — saving %d partial", sport_slug, len(matches))
    except Exception as exc:
        if matches:
            logger.warning("[sp] %s error: %s — saving partial", sport_slug, exc)
        else:
            raise self.retry(exc=exc)

    if not matches:
        try:
            celery.send_task(
                "tasks.bt_od.harvest_sport",
                args=[sport_slug],
                queue="harvest",
                countdown=5,
            )
        except Exception as exc:
            logger.warning("[sp] bt_od dispatch (no-sp) failed %s: %s", sport_slug, exc)
        return {"ok": False, "reason": "No SP matches fetched"}

    latency     = int((time.perf_counter() - t0) * 1000)
    avg_markets = int(sum(m.get("market_count", 0) for m in matches) / max(len(matches), 1))
    br_count    = sum(1 for m in matches if m.get("betradar_id"))

    logger.info("[sp] %s → %d matches (%d with betradar_id), avg %d mkts, %dms",
                sport_slug, len(matches), br_count, avg_markets, latency)

    cache_set(f"sp:upcoming:{sport_slug}", {
        "source":       "sportpesa",
        "sport":        sport_slug,
        "mode":         "upcoming",
        "match_count":  len(matches),
        "harvested_at": _now_iso(),
        "latency_ms":   latency,
        "matches":      matches,
        "avg_markets":  avg_markets,
        "br_count":     br_count,
    }, ttl=3600)

    from app.workers.redis_bus import publish_snapshot as _bus_publish
    _bus_publish("sp", "upcoming", sport_slug, matches, meta={
        "source":      "sportpesa",
        "avg_markets": avg_markets,
        "br_count":    br_count,
    })

    from app.workers.redis_bus import publish_snapshot
    publish_snapshot("sp", "upcoming", sport_slug, matches, meta={
        "source": "sportpesa", "avg_markets": avg_markets, "br_count": br_count,
    })

    _upsert_and_chain(matches, "SportPesa")
    _persist_bk_matches(matches, "sp", sport_slug)
    _emit("sportpesa", sport_slug, len(matches), latency)

    if br_count > 0:
        sp_cross_bk_enrich.apply_async(
            args=[sport_slug], queue="harvest", countdown=10,
        )
        logger.info("[sp] %s → cross-BK enrich dispatched (%d betradar_ids)",
                    sport_slug, br_count)

    sp_enrich_analytics.apply_async(
        args=[sport_slug], queue="harvest", countdown=60,
    )

    _schedule_alignment(sport_slug, countdown=60)

    try:
        celery.send_task(
            "tasks.bt_od.harvest_sport",
            args=[sport_slug],
            queue="harvest",
            countdown=30,
        )
        logger.info("[sp] %s → bt_od harvest dispatched", sport_slug)
    except Exception as exc:
        logger.warning("[sp] bt_od dispatch failed %s: %s", sport_slug, exc)

    return {
        "ok":           True,
        "source":       "sportpesa",
        "sport":        sport_slug,
        "count":        len(matches),
        "br_count":     br_count,
        "avg_markets":  avg_markets,
        "latency_ms":   latency,
    }


@celery.task(name="tasks.sp.harvest_all_upcoming", soft_time_limit=300, time_limit=600)
def sp_harvest_all_upcoming() -> dict:
    sigs = [sp_harvest_sport.s(s, SP_MAX_MATCHES) for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


# =============================================================================
# CROSS-BK ENRICHMENT  (fast path — betradar_id driven)
# =============================================================================

def _fetch_bt_by_betradar_id(
    sp_match: dict, sport_slug: str,
) -> tuple[dict, dict[str, dict[str, float]]]:
    from app.workers.bt_harvester import get_full_markets
    markets = get_full_markets(sp_match["betradar_id"], sport_slug)
    return sp_match, markets


def _fetch_od_by_betradar_id(
    sp_match: dict, od_sport_id: int,
) -> tuple[dict, dict[str, dict[str, float]]]:
    from app.workers.od_harvester import fetch_event_detail
    markets, _meta = fetch_event_detail(sp_match["betradar_id"], od_sport_id)
    return sp_match, markets


@celery.task(
    name="tasks.sp.cross_bk_enrich", bind=True,
    max_retries=1, default_retry_delay=120,
    soft_time_limit=3600, time_limit=3660, acks_late=True,
)
def sp_cross_bk_enrich(self, sport_slug: str) -> dict:
    from app.workers.od_harvester import slug_to_od_sport_id

    cached = cache_get(f"sp:upcoming:{sport_slug}")
    if not cached:
        logger.warning("[cross_bk] %s: no SP cache", sport_slug)
        return {"ok": False, "reason": "no_sp_cache"}

    sp_matches = cached.get("matches") or []
    with_br    = [m for m in sp_matches if m.get("betradar_id")]
    if not with_br:
        return {"ok": True, "bt_enriched": 0, "od_enriched": 0}

    od_sport_id = slug_to_od_sport_id(sport_slug)

    logger.info("[cross_bk] %s: direct-fetching BT+OD for %d ids (%d workers)",
                sport_slug, len(with_br), _CROSS_BK_WORKERS)

    bt_batch:    list[dict] = []
    od_batch:    list[dict] = []
    bt_enriched = od_enriched = bt_errors = od_errors = 0

    try:
        with ThreadPoolExecutor(max_workers=_CROSS_BK_WORKERS) as pool:
            bt_futs = {
                pool.submit(_fetch_bt_by_betradar_id, m, sport_slug): m
                for m in with_br
            }
            for fut in as_completed(bt_futs):
                try:
                    sp_m, bt_markets = fut.result()
                    if bt_markets:
                        bt_batch.append({
                            **sp_m,
                            "markets":      bt_markets,
                            "market_count": len(bt_markets),
                            "bt_parent_id": sp_m["betradar_id"],
                        })
                        bt_enriched += 1
                except SoftTimeLimitExceeded:
                    raise
                except Exception as exc:
                    bt_errors += 1
                    logger.debug("[cross_bk] BT %s: %s",
                                 bt_futs[fut].get("betradar_id"), exc)
    except SoftTimeLimitExceeded:
        logger.warning("[cross_bk] BT soft timeout %s (%d done)", sport_slug, bt_enriched)
        raise

    if bt_batch:
        _upsert_and_chain(bt_batch, "Betika")
        _persist_bk_matches(bt_batch, "bt", sport_slug)

    logger.info("[cross_bk] %s BT: enriched=%d errors=%d",
                sport_slug, bt_enriched, bt_errors)

    try:
        with ThreadPoolExecutor(max_workers=_CROSS_BK_WORKERS) as pool:
            od_futs = {
                pool.submit(_fetch_od_by_betradar_id, m, od_sport_id): m
                for m in with_br
            }
            for fut in as_completed(od_futs):
                try:
                    sp_m, od_markets = fut.result()
                    if od_markets:
                        od_batch.append({
                            **sp_m,
                            "markets":      od_markets,
                            "market_count": len(od_markets),
                            "od_event_id":  sp_m["betradar_id"],
                        })
                        od_enriched += 1
                except SoftTimeLimitExceeded:
                    raise
                except Exception as exc:
                    od_errors += 1
                    logger.debug("[cross_bk] OD %s: %s",
                                 od_futs[fut].get("betradar_id"), exc)
    except SoftTimeLimitExceeded:
        logger.warning("[cross_bk] OD soft timeout %s (%d done)", sport_slug, od_enriched)
        raise

    if od_batch:
        _upsert_and_chain(od_batch, "OdiBets")
        _persist_bk_matches(od_batch, "od", sport_slug)

    logger.info("[cross_bk] %s OD: enriched=%d errors=%d",
                sport_slug, od_enriched, od_errors)
    _schedule_alignment(sport_slug, countdown=30)

    return {
        "ok":          True,
        "sport":       sport_slug,
        "total":       len(with_br),
        "bt_enriched": bt_enriched,
        "bt_errors":   bt_errors,
        "od_enriched": od_enriched,
        "od_errors":   od_errors,
    }


# =============================================================================
# SPORTRADAR ANALYTICS
# =============================================================================

@celery.task(
    name="tasks.sp.enrich_analytics", bind=True,
    max_retries=1, default_retry_delay=300,
    soft_time_limit=3600, time_limit=3660, acks_late=True,
)
def sp_enrich_analytics(self, sport_slug: str) -> dict:
    from app.workers.sr_analytics import get_match_details, get_match_analytics
    cached = cache_get(f"sp:upcoming:{sport_slug}")
    if not cached:
        return {"ok": False, "reason": "no_sp_cache"}
    with_br = [m for m in (cached.get("matches") or []) if m.get("betradar_id")]
    if not with_br:
        return {"ok": True, "fetched": 0}

    fetched = near_term = errors = 0
    for m in with_br:
        br_id = m["betradar_id"]
        try:
            existing = cache_get(f"sr:analytics:{br_id}")
            if existing and existing.get("available"):
                fetched += 1
                continue
            if _is_near_term(m.get("start_time", "")):
                bundle    = get_match_analytics(br_id, fetch_season=True)
                near_term += 1
            else:
                details = get_match_details(br_id)
                bundle  = {"sr_match_id": br_id, "available": bool(details),
                           "match": details or {}}
            cache_set(f"sr:analytics:{br_id}", bundle, ttl=_ANALYTICS_TTL)
            fetched += 1
            time.sleep(0.2)
        except SoftTimeLimitExceeded:
            logger.warning("[analytics] soft timeout %s after %d/%d",
                           sport_slug, fetched, len(with_br))
            break
        except Exception as exc:
            errors += 1
            logger.debug("[analytics] %s: %s", br_id, exc)
    return {"ok": True, "sport": sport_slug, "fetched": fetched,
            "near_term": near_term, "errors": errors}


@celery.task(name="tasks.sp.get_match_analytics", soft_time_limit=30, time_limit=40)
def sp_get_match_analytics(betradar_id: str, force_refresh: bool = False) -> dict:
    from app.workers.sr_analytics import get_match_analytics
    if not force_refresh:
        existing = cache_get(f"sr:analytics:{betradar_id}")
        if existing and existing.get("available"):
            return {"ok": True, "cached": True, "betradar_id": betradar_id}
    bundle = get_match_analytics(betradar_id, fetch_season=True)
    cache_set(f"sr:analytics:{betradar_id}", bundle, ttl=_ANALYTICS_TTL)
    return {"ok": True, "cached": False, "betradar_id": betradar_id,
            "available": bundle.get("available", False)}


# =============================================================================
# BETIKA — manual trigger only (NOT in beat / startup)
# =============================================================================

@celery.task(
    name="tasks.bt.harvest_sport", bind=True,
    max_retries=2, default_retry_delay=20,
    soft_time_limit=600, time_limit=660, acks_late=True,
)
def bt_harvest_sport(self, sport_slug: str, max_matches: int = BT_MAX_MATCHES) -> dict:
    t0      = time.perf_counter()
    matches = []
    try:
        from app.workers.bt_harvester import fetch_upcoming_matches
        matches = fetch_upcoming_matches(
            sport_slug, max_pages=(max_matches // 50) + 10, fetch_full=False,
        )
        if len(matches) > max_matches:
            matches = matches[:max_matches]
    except SoftTimeLimitExceeded:
        logger.warning("[bt] soft timeout %s — saving %d partial", sport_slug, len(matches))
    except Exception as exc:
        if matches:
            logger.warning("[bt] %s error: %s — saving partial", sport_slug, exc)
        else:
            raise self.retry(exc=exc)
    if not matches:
        return {"ok": False, "reason": "No matches fetched"}
    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"bt:upcoming:{sport_slug}", {
        "source": "betika", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches, "enriched": False,
    }, ttl=3600)
    from app.workers.redis_bus import publish_snapshot as _bus_publish
    _bus_publish("bt", "upcoming", sport_slug, matches, meta={
        "source": "betika",
    })
    _upsert_and_chain(matches, "Betika")
    _persist_bk_matches(matches, "bt", sport_slug)
    _emit("betika", sport_slug, len(matches), latency)
    bt_enrich_sport.apply_async(
        args=[sport_slug, matches], queue="harvest", countdown=120,
    )
    return {"ok": True, "source": "betika", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(
    name="tasks.bt.enrich_sport", bind=True,
    max_retries=1, default_retry_delay=300,
    soft_time_limit=3600, time_limit=3660, acks_late=True,
)
def bt_enrich_sport(self, sport_slug: str, matches: list[dict]) -> dict:
    if not matches:
        return {"ok": True, "enriched": 0}
    t0 = time.perf_counter()
    try:
        from app.workers.bt_harvester import enrich_matches_with_full_markets
        enriched = enrich_matches_with_full_markets(matches, max_workers=12)
    except SoftTimeLimitExceeded:
        return {"ok": True, "enriched": 0, "reason": "timeout"}
    except Exception as exc:
        return {"ok": False, "reason": str(exc)}
    latency     = int((time.perf_counter() - t0) * 1000)
    avg_markets = int(sum(m.get("market_count", 0) for m in enriched) / max(len(enriched), 1))
    cache_set(f"bt:upcoming:{sport_slug}", {
        "source": "betika", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(enriched), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": enriched,
        "avg_markets": avg_markets, "enriched": True,
    }, ttl=3600)
    _upsert_and_chain(enriched, "Betika")
    _persist_bk_matches(enriched, "bt", sport_slug)
    _schedule_alignment(sport_slug, countdown=60)
    return {"ok": True, "source": "betika", "sport": sport_slug,
            "count": len(enriched), "avg_markets": avg_markets, "latency_ms": latency}


@celery.task(name="tasks.bt.harvest_all_upcoming", soft_time_limit=300, time_limit=600)
def bt_harvest_all_upcoming() -> dict:
    sigs = [bt_harvest_sport.s(s, BT_MAX_MATCHES) for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


# =============================================================================
# ODIBETS — manual trigger only (NOT in beat / startup)
# =============================================================================

@celery.task(
    name="tasks.od.harvest_sport", bind=True,
    max_retries=1, default_retry_delay=30,
    soft_time_limit=1800, time_limit=1860, acks_late=True,
)
def od_harvest_sport(self, sport_slug: str, max_matches=None) -> dict:
    t0      = time.perf_counter()
    matches = []
    try:
        from app.workers.od_harvester import fetch_upcoming_matches
        today     = date.today()
        _, last_d = monthrange(today.year, today.month)
        month_days = [date(today.year, today.month, d).isoformat()
                      for d in range(1, last_d + 1)]
        seen_ids: set[str]   = set()
        all_matches: list[dict] = []
        for day_str in month_days:
            try:
                for m in fetch_upcoming_matches(sport_slug=sport_slug, day=day_str):
                    mid = m.get("od_match_id") or m.get("od_event_id")
                    if mid:
                        if mid not in seen_ids:
                            seen_ids.add(mid)
                            all_matches.append(m)
                    else:
                        all_matches.append(m)
            except Exception as day_exc:
                logger.warning("[od] %s day=%s: %s", sport_slug, day_str, day_exc)
        matches = all_matches
    except SoftTimeLimitExceeded:
        logger.warning("[od] soft timeout %s — saving %d partial", sport_slug, len(matches))
    except Exception as exc:
        raise self.retry(exc=exc)
    if not matches:
        return {"ok": False, "reason": "No matches fetched"}
    latency     = int((time.perf_counter() - t0) * 1000)
    avg_markets = int(sum(m.get("market_count", 0) for m in matches) / max(len(matches), 1))
    cache_set(f"od:upcoming:{sport_slug}", {
        "source": "odibets", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches, "avg_markets": avg_markets,
    }, ttl=3600)
    from app.workers.redis_bus import publish_snapshot as _bus_publish
    _bus_publish("od", "upcoming", sport_slug, matches, meta={
        "source": "odibets", "avg_markets": avg_markets,
    })
    _upsert_and_chain(matches, "OdiBets")
    _persist_bk_matches(matches, "od", sport_slug)
    _emit("odibets", sport_slug, len(matches), latency)
    _schedule_alignment(sport_slug, countdown=60)
    return {"ok": True, "source": "odibets", "sport": sport_slug,
            "count": len(matches), "avg_markets": avg_markets, "latency_ms": latency}


@celery.task(name="tasks.od.harvest_all_upcoming", soft_time_limit=300, time_limit=600)
def od_harvest_all_upcoming() -> dict:
    sigs = [od_harvest_sport.s(s) for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


# =============================================================================
# B2B DIRECT  (scheduled, unchanged)
# =============================================================================

@celery.task(
    name="tasks.b2b.harvest_sport", bind=True,
    max_retries=2, default_retry_delay=30,
    soft_time_limit=300, time_limit=360, acks_late=True,
)
def b2b_harvest_sport(self, sport_slug: str) -> dict:
    t0 = time.perf_counter()
    try:
        from app.workers.b2b_harvester import fetch_b2b_sport
        matches = fetch_b2b_sport(sport_slug, mode="upcoming")
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"b2b:upcoming:{sport_slug}", {
        "source": "b2b", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=300)
    _persist_b2b_matches(matches, sport_slug)
    _emit("b2b", sport_slug, len(matches), latency)
    _schedule_alignment(sport_slug, countdown=60)
    return {"ok": True, "source": "b2b", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.b2b.harvest_all_upcoming", soft_time_limit=300, time_limit=600)
def b2b_harvest_all_upcoming() -> dict:
    sigs = [b2b_harvest_sport.s(s) for s in _B2B_HARVEST_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


@celery.task(
    name="tasks.b2b_page.harvest_page", bind=True,
    max_retries=2, default_retry_delay=15,
    soft_time_limit=45, time_limit=60, acks_late=True,
)
def b2b_page_harvest_page(self, bookmaker: dict, sport: str, page: int) -> dict:
    from app.workers.celery_tasks import _upsert_unified_match
    bk_name = bookmaker.get("name") or bookmaker.get("domain", "?")
    bk_id   = bookmaker.get("id")
    t0      = time.perf_counter()
    try:
        from app.views.odds_feed.bookmaker_fetcher import fetch_bookmaker
        matches = fetch_bookmaker(bookmaker, sport_name=sport, mode="upcoming",
                                  page=page, page_size=PAGE_SIZE, timeout=20)
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    ck = f"odds:upcoming:{sport.lower().replace(' ', '_')}:{bk_id}:p{page}"
    cache_set(ck, {
        "bookmaker_id": bk_id, "bookmaker_name": bk_name,
        "sport": sport, "mode": "upcoming", "page": page,
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=360)
    for m in matches:
        _upsert_unified_match(m, bk_id, bk_name)
    sport_slug = sport.lower().replace(" ", "-")
    bk_slug    = {v: k for k, v in _BK_NAMES.items()}.get(bk_name, bk_name[:4].lower())
    _persist_bk_matches(matches, bk_slug, sport_slug)
    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": "b2b",
        "bookmaker": bk_name, "sport": sport, "mode": "upcoming",
        "page": page, "count": len(matches), "ts": _now_iso(),
    })
    return {"ok": True, "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.b2b_page.harvest_all_upcoming", soft_time_limit=60, time_limit=120)
def b2b_page_harvest_all_upcoming() -> dict:
    from app.workers.celery_tasks import _redis
    raw        = _redis().get("cache:bookmakers:active")
    bookmakers = json.loads(raw) if raw else []
    if not bookmakers:
        return {"dispatched": 0}
    sigs = [
        b2b_page_harvest_page.s(bm, sport, page)
        for bm in bookmakers
        for sport in _B2B_SPORTS
        for page in range(1, MAX_PAGES + 1)
    ]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


# =============================================================================
# SBO  (scheduled, unchanged)
# =============================================================================

@celery.task(
    name="tasks.sbo.harvest_sport", bind=True,
    max_retries=1, default_retry_delay=30,
    soft_time_limit=120, time_limit=150, acks_late=True,
)
def sbo_harvest_sport(self, sport_slug: str, max_matches: int = 90) -> dict:
    t0 = time.perf_counter()
    try:
        from app.views.sbo.sbo_fetcher import OddsAggregator, SPORT_CONFIG
        cfg = next((c for c in SPORT_CONFIG if c["sport"] == sport_slug), None)
        if not cfg:
            return {"ok": False, "error": f"Unknown sport: {sport_slug}"}
        agg = OddsAggregator(cfg, fetch_full_sp_markets=True,
                             fetch_full_bt_markets=True, fetch_od_markets=True)
        matches = agg.run(max_matches=max_matches)
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"sbo:upcoming:{sport_slug}", {
        "sport": sport_slug, "match_count": len(matches),
        "harvested_at": _now_iso(), "latency_ms": latency, "matches": matches,
    }, ttl=180)
    if matches and matches[0].get("bookmakers"):
        _persist_b2b_matches(matches, sport_slug)
    else:
        _upsert_and_chain(matches, "SBO")
        _persist_bk_matches(matches, "sbo", sport_slug)
    arb_count = sum(1 for m in matches if m.get("arbitrage"))
    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": "sbo",
        "sport": sport_slug, "count": len(matches),
        "arb_count": arb_count, "ts": _now_iso(),
    })
    if arb_count:
        _publish(ARB_CHANNEL, {
            "event": "arb_found", "sport": sport_slug,
            "arb_count": arb_count, "ts": _now_iso(),
        })
    _schedule_alignment(sport_slug, countdown=60)
    return {"ok": True, "count": len(matches),
            "arb_count": arb_count, "latency_ms": latency}


@celery.task(name="tasks.sbo.harvest_all_upcoming", soft_time_limit=60, time_limit=120)
def sbo_harvest_all_upcoming() -> dict:
    sigs = [sbo_harvest_sport.s(s, 90) for s in _SBO_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}