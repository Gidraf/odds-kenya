"""
app/workers/tasks_upcoming.py  — PATCHED
=========================================
Key change vs previous version:
  _persist_bk_matches() now calls _resolve_betradar_id() before building
  the serialized payload.  When BT/OD have no betradar_id it looks up the
  existing unified_match by home+away+start_time so BT odds attach to the
  same DB row as SP instead of creating an orphan row.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta

from celery import group
from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded

from app.workers.celery_tasks import (
    celery, cache_set, cache_get, _now_iso, _publish,
    _upsert_and_chain, _fuzzy_find_match, _normalise_sport_name,
)

logger = get_task_logger(__name__)

# ── Sport lists ───────────────────────────────────────────────────────────────
_LOCAL_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
]
_B2B_HARVEST_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
    "darts", "handball",
]
_B2B_SPORTS = ["Football", "Basketball", "Tennis", "Ice Hockey",
               "Volleyball", "Cricket", "Rugby", "Table Tennis"]
_SBO_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "boxing",
    "handball", "mma", "table-tennis",
]

PAGE_SIZE = 15
MAX_PAGES = 6
WS_CHANNEL  = "odds:updates"
ARB_CHANNEL = "arb:updates"
EV_CHANNEL  = "ev:updates"

_BK_NAMES = {
    "sp":  "SportPesa",
    "bt":  "Betika",
    "od":  "OdiBets",
    "b2b": "1xBet",
    "sbo": "SBO",
}

# Sport slug → canonical DB name (used to normalise before persisting)
_SPORT_SLUG_TO_DB: dict[str, str] = {
    "soccer":       "Soccer",
    "football":     "Soccer",
    "basketball":   "Basketball",
    "tennis":       "Tennis",
    "ice-hockey":   "Ice Hockey",
    "volleyball":   "Volleyball",
    "cricket":      "Cricket",
    "rugby":        "Rugby",
    "table-tennis": "Table Tennis",
    "handball":     "Handball",
    "mma":          "MMA",
    "boxing":       "Boxing",
    "darts":        "Darts",
    "esoccer":      "eSoccer",
}


def _emit(source: str, sport: str, count: int, latency: int) -> None:
    _publish(WS_CHANNEL, {
        "event":      "odds_updated",
        "source":     source,
        "sport":      sport,
        "mode":       "upcoming",
        "count":      count,
        "latency_ms": latency,
        "ts":         _now_iso(),
    })


def _ensure_bookmaker(bk_slug: str) -> int | None:
    """Get or create a bookmaker row. Returns bookmaker.id or None."""
    canonical_name = _BK_NAMES.get(bk_slug, bk_slug.upper())
    try:
        from app.models.bookmakers_model import Bookmaker
        from app.extensions import db
        bm = Bookmaker.query.filter(
            Bookmaker.name.ilike(f"%{canonical_name}%")
        ).first()
        if bm:
            return bm.id
        bm = Bookmaker(
            name=canonical_name,
            domain=f"{bk_slug}.co.ke",
            is_active=True,
        )
        db.session.add(bm)
        db.session.commit()
        logger.info("[ensure_bookmaker] created: %s (id=%d)", canonical_name, bm.id)
        return bm.id
    except Exception as exc:
        logger.warning("[ensure_bookmaker] %s: %s", bk_slug, exc)
        return None


def _resolve_betradar_id(
    m: dict,
    bk_slug: str,
) -> tuple[str | None, str | None]:
    """
    Return (betradar_id, bk_ext_id) for a single match dict.

    For SP the betradar_id is usually set directly.
    For BT/OD it is None, so we fall back to a fuzzy DB lookup by
    home+away+start_time.  If the match isn't in the DB yet (SP hasn't
    harvested it) we return None so the caller creates a new row under
    the bk-specific ID — it will be merged when SP harvests later.
    """
    # 1. Try explicit betradar_id first
    betradar_id = str(
        m.get("betradar_id") or m.get("betradarId") or m.get("sr_id") or ""
    ).strip()
    if betradar_id and betradar_id not in ("0", "None"):
        pass  # valid, keep it
    else:
        betradar_id = None

    # 2. BK-specific external ID
    bk_ext_id = str(
        m.get("sp_game_id") or
        m.get("bt_match_id") or m.get("bt_parent_id") or
        m.get("od_match_id") or m.get("od_parent_id") or
        m.get("match_id") or ""
    ).strip() or None

    # 3. If no betradar_id, try fuzzy lookup to find the canonical match
    if not betradar_id:
        home  = str(m.get("home_team") or m.get("home_team_name") or "").strip()
        away  = str(m.get("away_team") or m.get("away_team_name") or "").strip()
        start = m.get("start_time") or ""
        found = _fuzzy_find_match(home, away, start)
        if found:
            betradar_id = found
            logger.debug(
                "[resolve_betradar] fuzzy matched %s vs %s → %s",
                home, away, found,
            )

    return betradar_id, bk_ext_id


def _persist_bk_matches(
    matches:    list[dict],
    bk_slug:    str,
    sport_slug: str,
) -> None:
    """
    Persist individual-bookmaker matches to PostgreSQL.

    Called by sp/bt/od harvest tasks after writing to Redis.
    Works independently of the combined stream so each bookmaker's
    data lands in the DB on every harvest cycle.

    For BT/OD where betradar_id is None, _resolve_betradar_id() does a
    fuzzy home+away+start_time lookup to find the existing SP row so odds
    are added to the same unified_match instead of creating a duplicate.
    """
    if not matches:
        return

    bk_id = _ensure_bookmaker(bk_slug)
    if not bk_id:
        logger.warning(
            "[persist_bk] could not resolve bookmaker %s — skipping DB persist",
            bk_slug,
        )
        return

    # Canonical sport name for the DB (Soccer not soccer)
    canonical_sport = _SPORT_SLUG_TO_DB.get(sport_slug, sport_slug)

    serialized: list[dict] = []
    for m in matches:
        betradar_id, bk_ext_id = _resolve_betradar_id(m, bk_slug)

        # Build join_key: prefer betradar, then bk-specific
        if betradar_id:
            join_key = f"br_{betradar_id}"
        elif bk_ext_id:
            join_key = f"{bk_slug}_{bk_ext_id}"
        else:
            continue  # nothing to key on — skip

        markets = m.get("markets") or {}
        if not markets:
            continue

        serialized.append({
            "join_key":    join_key,
            "home_team":   m.get("home_team")        or m.get("home_team_name") or "",
            "away_team":   m.get("away_team")         or m.get("away_team_name") or "",
            "competition": m.get("competition")       or m.get("competition_name") or "",
            "start_time":  m.get("start_time")        or "",
            "is_live":     False,
            "betradar_id": betradar_id,
            "sport":       canonical_sport,
            "bk_ids":      {bk_slug: bk_ext_id or join_key},
            "markets":     {bk_slug: markets},
            # Pass through raw BT/OD IDs so _upsert_unified_match can use them
            "bt_match_id":  m.get("bt_match_id")  or "",
            "bt_parent_id": m.get("bt_parent_id") or "",
            "od_match_id":  m.get("od_match_id")  or "",
            "od_parent_id": m.get("od_parent_id") or "",
        })

    if not serialized:
        logger.debug("[persist_bk] %s/%s: no serializable matches", bk_slug, sport_slug)
        return

    try:
        celery.send_task(
            "tasks.ops.persist_combined_batch",
            args=[serialized, sport_slug, "upcoming"],
            queue="results",
            countdown=3,
        )
        logger.info(
            "[persist_bk] %s/%s: dispatched %d matches to DB",
            bk_slug, sport_slug, len(serialized),
        )
    except Exception as exc:
        logger.warning(
            "[persist_bk] dispatch failed for %s/%s: %s",
            bk_slug, sport_slug, exc,
        )


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
    _publish("odds:harvest:done", {
        "type": "harvest_done", "bookmaker": bookmaker_slug,
        "sport": sport_slug, "count": len(matches), "ts": _now_iso(),
    })
    logger.info("[registry] %s/%s → %d matches %dms",
                bookmaker_slug, sport_slug, len(matches), latency_ms)
    return {"ok": True, "bookmaker": bookmaker_slug, "sport": sport_slug,
            "count": len(matches), "latency_ms": latency_ms}


@celery.task(name="harvest.all_upcoming", soft_time_limit=30, time_limit=60)
def harvest_all_registry_upcoming() -> dict:
    from app.workers.harvest_registry import ENABLED_BOOKMAKERS
    sigs = [
        harvest_bookmaker_sport.s(bk["slug"], sport)
        for bk in ENABLED_BOOKMAKERS
        for sport in bk["sports"]
    ]
    group(sigs).apply_async(queue="harvest")
    logger.info("[registry] dispatched %d tasks", len(sigs))
    return {"dispatched": len(sigs)}


@celery.task(name="harvest.merge_broadcast", soft_time_limit=30, time_limit=60)
def merge_and_broadcast(sport_slug: str) -> dict:
    from app.workers.harvest_registry import ENABLED_BOOKMAKERS
    bk_slugs = [bk["slug"] for bk in ENABLED_BOOKMAKERS if sport_slug in bk["sports"]]
    merged: dict[str, dict] = {}
    seen_bk: list[str] = []
    for slug in bk_slugs:
        cached = cache_get(f"odds:upcoming:{slug}:{sport_slug}")
        if not cached or not cached.get("matches"):
            continue
        seen_bk.append(slug)
        for match in cached["matches"]:
            br_id = str(match.get("betradar_id") or "")
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
                    if not odd_val or float(odd_val) <= 1.0:
                        continue
                    entry["markets"][mkt_slug].setdefault(out_key, {})
                    entry["markets"][mkt_slug][out_key][slug] = float(odd_val)

    merged_list = list(merged.values())
    for match in merged_list:
        match["best_odds"] = {}
        for mkt_slug, outcomes in match["markets"].items():
            for out_key, bk_odds in outcomes.items():
                if bk_odds:
                    best_bk = max(bk_odds, key=lambda b: bk_odds[b])
                    match["best_odds"][f"{mkt_slug}__{out_key}"] = {
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
    logger.info("[merge] %s: %d events from %d bookmakers",
                sport_slug, len(merged_list), len(seen_bk))
    return {"ok": True, "sport": sport_slug, "events": len(merged_list)}


@celery.task(name="harvest.value_bets", soft_time_limit=60, time_limit=90)
def compute_value_bets(sport_slug: str) -> dict:
    import os
    from app.extensions import db
    from app.models.odds_model import (
        ArbitrageOpportunity, EVOpportunity, OpportunityStatus,
    )
    raw = cache_get(f"odds:upcoming:all:{sport_slug}")
    if not raw:
        return {"ok": True, "found": 0}
    matches  = raw.get("matches") or []
    arb_rows = 0
    now      = datetime.now(timezone.utc)
    try:
        for match in matches:
            for mkt_slug, outcomes in (match.get("markets") or {}).items():
                best_prices: dict[str, float] = {}
                leg_details: dict[str, tuple] = {}
                for out_key, bk_odds in outcomes.items():
                    if not bk_odds:
                        continue
                    best_bk  = max(bk_odds, key=lambda b: float(bk_odds[b]))
                    best_odd = float(bk_odds[best_bk])
                    if best_odd > 1.0:
                        best_prices[out_key] = best_odd
                        leg_details[out_key] = (best_bk, best_odd)
                if len(best_prices) >= 2:
                    arb_sum = sum(1.0 / p for p in best_prices.values())
                    if arb_sum < 1.0:
                        profit_pct = (1.0 / arb_sum - 1.0) * 100
                        if profit_pct >= 0.5:
                            legs = [{
                                "selection": sel, "bookmaker": leg_details[sel][0],
                                "price": leg_details[sel][1],
                                "stake_pct": round(
                                    (1.0 / leg_details[sel][1]) / arb_sum * 100, 2
                                ),
                            } for sel in best_prices]
                            start_dt = None
                            if match.get("start_time"):
                                try:
                                    start_dt = datetime.fromisoformat(
                                        str(match["start_time"]).replace("Z", "+00:00")
                                    )
                                except Exception:
                                    pass
                            db.session.add(ArbitrageOpportunity(
                                home_team=match.get("home_team", ""),
                                away_team=match.get("away_team", ""),
                                sport=sport_slug,
                                competition=match.get("competition", ""),
                                match_start=start_dt,
                                market=mkt_slug,
                                profit_pct=round(profit_pct, 4),
                                peak_profit_pct=round(profit_pct, 4),
                                arb_sum=round(arb_sum, 6),
                                legs_json=legs,
                                stake_100_returns=round(100 / arb_sum, 2),
                                bookmaker_ids=sorted({l["bookmaker"] for l in legs}),
                                status=OpportunityStatus.OPEN,
                                open_at=now,
                            ))
                            arb_rows += 1
        db.session.commit()
    except Exception as exc:
        logger.error("[value_bets] %s: %s", sport_slug, exc)
        try:
            db.session.rollback()
        except Exception:
            pass
    logger.info("[value_bets] %s: %d arbs", sport_slug, arb_rows)
    return {"ok": True, "sport": sport_slug, "arbs": arb_rows, "evs": 0}


@celery.task(name="harvest.cleanup", soft_time_limit=60, time_limit=90)
def cleanup_old_snapshots(days_keep: int = 7) -> dict:
    from app.extensions import db
    from app.models.odds_model import ArbitrageOpportunity, EVOpportunity
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_keep)
    n_a = ArbitrageOpportunity.query.filter(ArbitrageOpportunity.open_at < cutoff).delete()
    n_e = EVOpportunity.query.filter(EVOpportunity.open_at < cutoff).delete()
    db.session.commit()
    logger.info("[cleanup] %d arbs  %d evs deleted", n_a, n_e)
    return {"ok": True, "arbs_deleted": n_a, "evs_deleted": n_e}


# =============================================================================
# SPORTPESA UPCOMING
# =============================================================================

@celery.task(
    name="tasks.sp.harvest_sport", bind=True,
    max_retries=2, default_retry_delay=20,
    soft_time_limit=180, time_limit=210, acks_late=True,
)
def sp_harvest_sport(self, sport_slug: str, max_matches=None) -> dict:
    t0 = time.perf_counter()
    matches = []
    try:
        from app.workers.sp_harvester import fetch_upcoming_stream
        for match in fetch_upcoming_stream(
            sport_slug, fetch_full_markets=True,
            max_matches=max_matches, sleep_between=0.1,
        ):
            matches.append(match)
    except SoftTimeLimitExceeded:
        logger.warning(
            "[sp:upcoming] Soft timeout for %s — saving %d partial matches",
            sport_slug, len(matches),
        )
    except Exception as exc:
        raise self.retry(exc=exc)

    if not matches:
        return {"ok": False, "reason": "No matches fetched"}

    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"sp:upcoming:{sport_slug}", {
        "source": "sportpesa", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=300)
    _upsert_and_chain(matches, "SportPesa")
    _persist_bk_matches(matches, "sp", sport_slug)
    _emit("sportpesa", sport_slug, len(matches), latency)
    logger.info("[sp:upcoming] %s → %d matches %dms", sport_slug, len(matches), latency)
    return {"ok": True, "source": "sportpesa", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.sp.harvest_all_upcoming", soft_time_limit=600, time_limit=630)
def sp_harvest_all_upcoming() -> dict:
    sigs = [sp_harvest_sport.s(s) for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


# =============================================================================
# BETIKA UPCOMING
# =============================================================================

@celery.task(
    name="tasks.bt.harvest_sport", bind=True,
    max_retries=2, default_retry_delay=20,
    soft_time_limit=180, time_limit=210, acks_late=True,
)
def bt_harvest_sport(self, sport_slug: str, max_matches=None) -> dict:
    t0 = time.perf_counter()
    matches = []
    try:
        from app.workers.bt_harvester import fetch_upcoming_matches
        matches = fetch_upcoming_matches(sport_slug, max_pages=8, fetch_full=False)
    except SoftTimeLimitExceeded:
        logger.warning(
            "[bt:upcoming] Soft timeout for %s — saving %d partial matches",
            sport_slug, len(matches),
        )
    except Exception as exc:
        raise self.retry(exc=exc)

    if not matches:
        return {"ok": False, "reason": "No matches fetched"}

    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"bt:upcoming:{sport_slug}", {
        "source": "betika", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=300)
    _upsert_and_chain(matches, "Betika")
    _persist_bk_matches(matches, "bt", sport_slug)
    _emit("betika", sport_slug, len(matches), latency)
    logger.info("[bt:upcoming] %s → %d matches %dms", sport_slug, len(matches), latency)
    return {"ok": True, "source": "betika", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.bt.harvest_all_upcoming", soft_time_limit=60, time_limit=90)
def bt_harvest_all_upcoming() -> dict:
    sigs = [bt_harvest_sport.s(s) for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


# =============================================================================
# ODIBETS UPCOMING
# =============================================================================

@celery.task(
    name="tasks.od.harvest_sport", bind=True,
    max_retries=1, default_retry_delay=30,
    soft_time_limit=300, time_limit=330, acks_late=True,
)
def od_harvest_sport(self, sport_slug: str, max_matches=None) -> dict:
    t0 = time.perf_counter()
    matches = []
    try:
        from app.workers.od_harvester import fetch_upcoming_matches
        matches = fetch_upcoming_matches(sport_slug=sport_slug)
    except SoftTimeLimitExceeded:
        logger.warning(
            "[od:upcoming] Soft timeout for %s — saving %d partial matches",
            sport_slug, len(matches),
        )
    except Exception as exc:
        raise self.retry(exc=exc)

    if not matches:
        return {"ok": False, "reason": "No matches fetched"}

    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"od:upcoming:{sport_slug}", {
        "source": "odibets", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=300)
    _upsert_and_chain(matches, "OdiBets")
    _persist_bk_matches(matches, "od", sport_slug)
    _emit("odibets", sport_slug, len(matches), latency)
    logger.info("[od:upcoming] %s → %d matches %dms", sport_slug, len(matches), latency)
    return {"ok": True, "source": "odibets", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.od.harvest_all_upcoming", soft_time_limit=60, time_limit=90)
def od_harvest_all_upcoming() -> dict:
    sigs = [od_harvest_sport.s(s) for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


# =============================================================================
# B2B DIRECT UPCOMING
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
    for m in matches:
        for bk_name, bk_data in (m.get("bookmakers") or {}).items():
            bk_match = dict(m)
            bk_match["markets"] = bk_data.get("markets") or {}
            _upsert_and_chain([bk_match], bk_name)
    _emit("b2b", sport_slug, len(matches), latency)
    return {"ok": True, "source": "b2b", "sport": sport_slug,
            "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.b2b.harvest_all_upcoming", soft_time_limit=30, time_limit=60)
def b2b_harvest_all_upcoming() -> dict:
    sigs = [b2b_harvest_sport.s(s) for s in _B2B_HARVEST_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


# =============================================================================
# B2B PAGE FAN-OUT (legacy)
# =============================================================================

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
        matches = fetch_bookmaker(
            bookmaker, sport_name=sport, mode="upcoming",
            page=page, page_size=PAGE_SIZE, timeout=20,
        )
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    ck = f"odds:upcoming:{sport.lower().replace(' ','_')}:{bk_id}:p{page}"
    cache_set(ck, {
        "bookmaker_id": bk_id, "bookmaker_name": bk_name,
        "sport": sport, "mode": "upcoming", "page": page,
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=360)
    for m in matches:
        _upsert_unified_match(m, bk_id, bk_name)
    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": "b2b",
        "bookmaker": bk_name, "sport": sport, "mode": "upcoming",
        "page": page, "count": len(matches), "ts": _now_iso(),
    })
    return {"ok": True, "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.b2b_page.harvest_all_upcoming", soft_time_limit=30, time_limit=60)
def b2b_page_harvest_all_upcoming() -> dict:
    from app.workers.celery_tasks import _redis
    raw = _redis().get("cache:bookmakers:active")
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
# SBO UPCOMING
# =============================================================================

@celery.task(
    name="tasks.sbo.harvest_sport", bind=True,
    max_retries=1, default_retry_delay=30,
    soft_time_limit=120, time_limit=150, acks_late=True,
)
def sbo_harvest_sport(self, sport_slug: str, max_matches: int = 90) -> dict:
    from app.workers.celery_tasks import _upsert_unified_match
    t0 = time.perf_counter()
    try:
        from app.views.sbo.sbo_fetcher import OddsAggregator, SPORT_CONFIG
        cfg = next((c for c in SPORT_CONFIG if c["sport"] == sport_slug), None)
        if not cfg:
            return {"ok": False, "error": f"Unknown sport: {sport_slug}"}
        agg = OddsAggregator(
            cfg,
            fetch_full_sp_markets=True,
            fetch_full_bt_markets=True,
            fetch_od_markets=True,
        )
        matches = agg.run(max_matches=max_matches)
    except Exception as exc:
        raise self.retry(exc=exc)
    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"sbo:upcoming:{sport_slug}", {
        "sport": sport_slug, "match_count": len(matches),
        "harvested_at": _now_iso(), "latency_ms": latency, "matches": matches,
    }, ttl=180)
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
    return {"ok": True, "count": len(matches),
            "arb_count": arb_count, "latency_ms": latency}


@celery.task(name="tasks.sbo.harvest_all_upcoming", soft_time_limit=30, time_limit=60)
def sbo_harvest_all_upcoming() -> dict:
    sigs = [sbo_harvest_sport.s(s, 90) for s in _SBO_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}