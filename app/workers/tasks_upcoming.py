"""
app/workers/tasks_upcoming.py
==============================
Harvest targets:
  SP  — fetch until 15,000 matches total (full markets, 30-day lookahead)
  BT  — fetch until 15,000 matches total (parallel full-market enrichment)
  OD  — fetch entire current calendar month day-by-day

After each harvest completes, _schedule_alignment(sport_slug) is called
with a 60-second countdown so the persist pipeline finishes writing BMO
rows before the alignment job reads and merges them.

Market resolution priority:
  1. betradar_id / bt_parent_id / od_parent_id (explicit ID)
  2. fuzzy home+away+start_time DB lookup (fallback)
  3. bk-specific external ID as join key (last resort)
"""

from __future__ import annotations

import json
import time
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
_B2B_SPORTS = [
    "Football", "Basketball", "Tennis", "Ice Hockey",
    "Volleyball", "Cricket", "Rugby", "Table Tennis",
]
_SBO_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "boxing",
    "handball", "mma", "table-tennis",
]

PAGE_SIZE      = 15
MAX_PAGES      = 6
WS_CHANNEL     = "odds:updates"
ARB_CHANNEL    = "arb:updates"
EV_CHANNEL     = "ev:updates"
SP_MAX_MATCHES = 15_000
BT_MAX_MATCHES = 15_000

_BK_NAMES: dict[str, str] = {
    "sp":  "SportPesa",
    "bt":  "Betika",
    "od":  "OdiBets",
    "b2b": "1xBet",
    "sbo": "SBO",
}

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


# =============================================================================
# HELPERS
# =============================================================================

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


def _schedule_alignment(sport_slug: str, countdown: int = 60) -> None:
    """
    Schedule market alignment for a sport after harvest finishes.
    countdown=60 gives the persist pipeline time to write all BMO rows
    before the alignment job reads and merges them.
    """
    try:
        from app.workers.tasks_market_align import align_sport_markets
        align_sport_markets.apply_async(
            args=[sport_slug, 100],
            queue="results",
            countdown=countdown,
        )
        logger.info(
            "[harvest] alignment scheduled for %s in %ds", sport_slug, countdown,
        )
    except Exception as exc:
        logger.warning(
            "[harvest] failed to schedule alignment for %s: %s", sport_slug, exc,
        )


def _fuzzy_find_match(home: str, away: str, start_time_raw) -> str | None:
    """
    Find an existing unified_match by home + away + start_time (±90 min).
    Returns parent_match_id if found, else None.
    Only called when no explicit betradar ID is available.
    """
    if not home or not away:
        return None
    try:
        from app.models.odds_model import UnifiedMatch
        from sqlalchemy import func

        start_dt = None
        if start_time_raw:
            try:
                if isinstance(start_time_raw, str):
                    start_dt = datetime.fromisoformat(
                        start_time_raw.replace("Z", "+00:00")
                    )
                elif isinstance(start_time_raw, (int, float)):
                    start_dt = datetime.utcfromtimestamp(float(start_time_raw))
                elif isinstance(start_time_raw, datetime):
                    start_dt = start_time_raw
            except Exception:
                pass

        q = UnifiedMatch.query.filter(
            func.lower(UnifiedMatch.home_team_name) == home.lower().strip(),
            func.lower(UnifiedMatch.away_team_name) == away.lower().strip(),
        )
        if start_dt:
            window = timedelta(minutes=90)
            q = q.filter(
                UnifiedMatch.start_time >= start_dt - window,
                UnifiedMatch.start_time <= start_dt + window,
            )
        um = q.first()
        return um.parent_match_id if um else None
    except Exception as exc:
        logger.debug("[fuzzy_find] %s vs %s: %s", home, away, exc)
        return None


def _resolve_match_id(m: dict, bk_slug: str) -> tuple[str | None, str | None]:
    """
    Return (betradar_id, bk_ext_id) for a single raw match dict.

    Resolution order:
      1. _extract_betradar_id() — checks betradar_id, betradarId, sr_id,
                                   bt_parent_id, od_parent_id
      2. Fuzzy DB lookup by home + away + start_time — only when step 1 fails
      3. bk_ext_id — bookmaker's own match ID (fallback join key)
    """
    betradar_id = _extract_betradar_id(m)

    if not betradar_id:
        home  = str(m.get("home_team") or m.get("home_team_name") or "").strip()
        away  = str(m.get("away_team") or m.get("away_team_name") or "").strip()
        start = m.get("start_time") or ""
        found = _fuzzy_find_match(home, away, start)
        if found:
            betradar_id = found
            logger.debug(
                "[resolve_match_id] fuzzy matched %s vs %s → %s",
                home, away, betradar_id,
            )

    bk_ext_id = str(
        m.get("sp_game_id")   or
        m.get("bt_match_id")  or
        m.get("od_match_id")  or
        m.get("match_id")     or
        m.get("event_id")     or
        ""
    ).strip() or None

    return betradar_id, bk_ext_id


def _persist_bk_matches(
    matches:    list[dict],
    bk_slug:    str,
    sport_slug: str,
) -> None:
    """
    Serialise raw harvested matches and dispatch Celery persist task(s).
    Splits large batches into chunks of 500 to avoid oversized messages
    and DB transaction timeouts.

    FIX (Bug 1): markets are stored FLAT as {mkt_key: {outcome: price}} —
    NOT wrapped under the bookmaker slug.  The previous shape
    {"markets": {bk_slug: markets}} caused _upsert_unified_match to see
    mkt_key=bk_slug → outcome=market_name → odds_data=dict, which resolves
    to price=0 and silently drops every BT/OD/SBO selection.
    """
    if not matches:
        return

    bk_id = _get_or_create_bookmaker(_BK_NAMES.get(bk_slug, bk_slug.upper()))
    if not bk_id:
        logger.warning(
            "[persist_bk] could not resolve bookmaker %s — skipping", bk_slug,
        )
        return

    canonical_sport = _SPORT_SLUG_TO_DB.get(sport_slug, sport_slug)

    br_id_count = sum(1 for m in matches if _extract_betradar_id(m))
    logger.info(
        "[persist_bk] %s/%s: %d matches total, %d with betradar_id, %d need fuzzy",
        bk_slug, sport_slug, len(matches), br_id_count, len(matches) - br_id_count,
    )

    serialized: list[dict] = []
    for m in matches:
        betradar_id, bk_ext_id = _resolve_match_id(m, bk_slug)

        if betradar_id:
            join_key = f"br_{betradar_id}"
        elif bk_ext_id:
            join_key = f"{bk_slug}_{bk_ext_id}"
        else:
            logger.debug(
                "[persist_bk] %s: no key for %s vs %s — skipping",
                bk_slug,
                m.get("home_team", "?"),
                m.get("away_team", "?"),
            )
            continue

        markets = m.get("markets") or {}
        if not markets:
            continue

        serialized.append({
            "join_key":       join_key,
            "home_team":      m.get("home_team")    or m.get("home_team_name")    or "",
            "away_team":      m.get("away_team")    or m.get("away_team_name")    or "",
            "competition":    m.get("competition")  or m.get("competition_name")  or "",
            "start_time":     m.get("start_time")   or "",
            "is_live":        False,
            "betradar_id":    betradar_id,
            "sport":          canonical_sport,
            "bk_ids":         {bk_slug: bk_ext_id or join_key},
            # FIX: store markets FLAT, not wrapped under bk_slug.
            # Wrong:  "markets": {bk_slug: markets}
            # Correct: "markets": markets  (i.e. {mkt_key: {outcome: price}})
            "markets":        markets,
            "bookmaker_slug": bk_slug,   # kept for traceability / debugging
        })

    if not serialized:
        logger.debug(
            "[persist_bk] %s/%s: no serializable matches", bk_slug, sport_slug,
        )
        return

    # Chunk into 500s to avoid oversized Celery messages / DB timeouts
    chunk_size  = 500
    chunks      = [serialized[i: i + chunk_size]
                   for i in range(0, len(serialized), chunk_size)]
    dispatched  = 0
    for chunk in chunks:
        try:
            celery.send_task(
                "tasks.ops.persist_combined_batch",
                args=[chunk, sport_slug, "upcoming"],
                queue="results",
                countdown=3,
            )
            dispatched += len(chunk)
        except Exception as exc:
            logger.warning(
                "[persist_bk] dispatch failed chunk for %s/%s: %s",
                bk_slug, sport_slug, exc,
            )

    logger.info(
        "[persist_bk] %s/%s: dispatched %d/%d in %d chunks",
        bk_slug, sport_slug, dispatched, len(serialized), len(chunks),
    )


def _persist_b2b_matches(matches: list[dict], sport_slug: str) -> None:
    """Fan out one _persist_bk_matches call per sub-bookmaker in a B2B batch."""
    if not matches:
        return

    bk_batches: dict[str, list[dict]] = {}
    for m in matches:
        for bk_name, bk_data in (m.get("bookmakers") or {}).items():
            bk_mkts = (bk_data or {}).get("markets") or {}
            if not bk_mkts:
                continue
            flat = {
                **m,
                "markets":     bk_mkts,
                "betradar_id": m.get("betradar_id") or "",
            }
            flat.pop("bookmakers", None)
            slug = {v: k for k, v in _BK_NAMES.items()}.get(bk_name, bk_name[:4].lower())
            bk_batches.setdefault(slug, []).append(flat)

    for slug, batch in bk_batches.items():
        _upsert_and_chain(batch, _BK_NAMES.get(slug, slug.upper()))
        _persist_bk_matches(batch, slug, sport_slug)


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
        "bookmaker":   bookmaker_slug, "sport": sport_slug,
        "match_count": len(matches),   "harvested_at": _now_iso(),
        "latency_ms":  latency_ms,     "matches": matches,
    }, ttl=bk["redis_ttl"])
    _upsert_and_chain(matches, bk["label"])
    _persist_bk_matches(matches, bookmaker_slug, sport_slug)
    _publish("odds:harvest:done", {
        "type": "harvest_done", "bookmaker": bookmaker_slug,
        "sport": sport_slug, "count": len(matches), "ts": _now_iso(),
    })
    _schedule_alignment(sport_slug, countdown=60)
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
    merged:  dict[str, dict] = {}
    seen_bk: list[str]       = []

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
            entry["event_ids"][slug] = str(
                match.get("sp_game_id") or match.get("event_id") or ""
            )
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
                    match["best_odds"][f"{mkt_slug}__{out_key}"] = {
                        "bookmaker": best_bk, "odd": bk_odds[best_bk],
                    }

    cache_set(f"odds:upcoming:all:{sport_slug}", {
        "sport":        sport_slug,
        "bookmakers":   seen_bk,
        "match_count":  len(merged_list),
        "harvested_at": _now_iso(),
        "matches":      merged_list,
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
    from app.extensions import db
    from app.models.odds_model import ArbitrageOpportunity, OpportunityStatus

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

                if len(best_prices) < 2:
                    continue
                arb_sum = sum(1.0 / p for p in best_prices.values())
                if arb_sum >= 1.0:
                    continue
                profit_pct = (1.0 / arb_sum - 1.0) * 100
                if profit_pct < 0.5:
                    continue

                legs = [{
                    "selection": sel,
                    "bookmaker": leg_details[sel][0],
                    "price":     leg_details[sel][1],
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

    logger.info("[value_bets] %s: %d arbs found", sport_slug, arb_rows)
    return {"ok": True, "sport": sport_slug, "arbs": arb_rows, "evs": 0}


@celery.task(name="harvest.cleanup", soft_time_limit=60, time_limit=90)
def cleanup_old_snapshots(days_keep: int = 7) -> dict:
    from app.extensions import db
    from app.models.odds_model import ArbitrageOpportunity, EVOpportunity
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_keep)
    n_a    = ArbitrageOpportunity.query.filter(
        ArbitrageOpportunity.open_at < cutoff
    ).delete()
    n_e    = EVOpportunity.query.filter(
        EVOpportunity.open_at < cutoff
    ).delete()
    db.session.commit()
    logger.info("[cleanup] %d arbs  %d evs deleted", n_a, n_e)
    return {"ok": True, "arbs_deleted": n_a, "evs_deleted": n_e}


# =============================================================================
# SPORTPESA UPCOMING  (up to 15,000 matches, full markets, 30-day lookahead)
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

        logger.info(
            "[sp:upcoming] %s → fetching up to %d matches with full markets",
            sport_slug, max_matches,
        )
        for match in fetch_upcoming_stream(
            sport_slug,
            fetch_full_markets=True,
            max_matches=max_matches,
            days=30,
            sleep_between=0.1,
        ):
            matches.append(match)
            if len(matches) % 1000 == 0:
                logger.info(
                    "[sp:upcoming] %s → %d/%d collected",
                    sport_slug, len(matches), max_matches,
                )
    except SoftTimeLimitExceeded:
        logger.warning("[sp:upcoming] Soft timeout %s — saving %d partial",
                       sport_slug, len(matches))
    except Exception as exc:
        raise self.retry(exc=exc)

    if not matches:
        return {"ok": False, "reason": "No matches fetched"}

    latency     = int((time.perf_counter() - t0) * 1000)
    avg_markets = int(sum(m.get("market_count", 0) for m in matches) / max(len(matches), 1))

    logger.info("[sp:upcoming] %s → %d matches, avg %d markets/match, %dms",
                sport_slug, len(matches), avg_markets, latency)

    cache_set(f"sp:upcoming:{sport_slug}", {
        "source": "sportpesa", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches, "avg_markets": avg_markets,
    }, ttl=3600)

    _upsert_and_chain(matches, "SportPesa")
    _persist_bk_matches(matches, "sp", sport_slug)
    _emit("sportpesa", sport_slug, len(matches), latency)
    _schedule_alignment(sport_slug, countdown=60)

    return {
        "ok": True, "source": "sportpesa", "sport": sport_slug,
        "count": len(matches), "avg_markets": avg_markets, "latency_ms": latency,
    }


@celery.task(name="tasks.sp.harvest_all_upcoming",
             soft_time_limit=120, time_limit=150)
def sp_harvest_all_upcoming() -> dict:
    sigs = [sp_harvest_sport.s(s, SP_MAX_MATCHES) for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    logger.info("[sp] dispatched %d upcoming tasks (max %d each)", len(sigs), SP_MAX_MATCHES)
    return {"dispatched": len(sigs), "max_per_sport": SP_MAX_MATCHES}


# =============================================================================
# BETIKA UPCOMING  (up to 15,000 matches, parallel full-market enrichment)
# =============================================================================

@celery.task(
    name="tasks.bt.harvest_sport", bind=True,
    max_retries=2, default_retry_delay=20,
    soft_time_limit=3600, time_limit=3660, acks_late=True,
)
def bt_harvest_sport(self, sport_slug: str, max_matches: int = BT_MAX_MATCHES) -> dict:
    t0      = time.perf_counter()
    matches = []
    try:
        from app.workers.bt_harvester import (
            fetch_upcoming_matches,
            enrich_matches_with_full_markets,
        )

        max_pages = (max_matches // 50) + 10

        logger.info(
            "[bt:upcoming] %s → grid fetch, target %d matches (max_pages=%d)",
            sport_slug, max_matches, max_pages,
        )

        matches = fetch_upcoming_matches(
            sport_slug,
            max_pages=max_pages,
            fetch_full=False,
        )
        if len(matches) > max_matches:
            matches = matches[:max_matches]

        if not matches:
            return {"ok": False, "reason": "No matches fetched"}

        logger.info(
            "[bt:upcoming] %s → %d matches, starting full-market enrichment (12 workers)",
            sport_slug, len(matches),
        )

        matches = enrich_matches_with_full_markets(matches, max_workers=12)

        avg_markets = int(sum(m.get("market_count", 0) for m in matches) / max(len(matches), 1))
        logger.info("[bt:upcoming] %s → enrichment done, avg %d markets/match",
                    sport_slug, avg_markets)

    except SoftTimeLimitExceeded:
        logger.warning("[bt:upcoming] Soft timeout %s — saving %d partial",
                       sport_slug, len(matches))
    except Exception as exc:
        raise self.retry(exc=exc)

    if not matches:
        return {"ok": False, "reason": "No matches fetched"}

    latency     = int((time.perf_counter() - t0) * 1000)
    avg_markets = int(sum(m.get("market_count", 0) for m in matches) / max(len(matches), 1))

    cache_set(f"bt:upcoming:{sport_slug}", {
        "source": "betika", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches, "avg_markets": avg_markets,
    }, ttl=3600)

    _upsert_and_chain(matches, "Betika")
    _persist_bk_matches(matches, "bt", sport_slug)
    _emit("betika", sport_slug, len(matches), latency)
    _schedule_alignment(sport_slug, countdown=60)

    logger.info("[bt:upcoming] %s → %d matches %dms (avg %d markets/match)",
                sport_slug, len(matches), latency, avg_markets)
    return {
        "ok": True, "source": "betika", "sport": sport_slug,
        "count": len(matches), "avg_markets": avg_markets, "latency_ms": latency,
    }


@celery.task(name="tasks.bt.harvest_all_upcoming",
             soft_time_limit=120, time_limit=150)
def bt_harvest_all_upcoming() -> dict:
    sigs = [bt_harvest_sport.s(s, BT_MAX_MATCHES) for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    logger.info("[bt] dispatched %d upcoming tasks (max %d each)", len(sigs), BT_MAX_MATCHES)
    return {"dispatched": len(sigs), "max_per_sport": BT_MAX_MATCHES}


# =============================================================================
# ODIBETS UPCOMING  (full current calendar month, deduplicated)
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
        month_days = [
            date(today.year, today.month, d).isoformat()
            for d in range(1, last_d + 1)
        ]

        logger.info(
            "[od:upcoming] %s → fetching %d days (full month %04d-%02d)",
            sport_slug, len(month_days), today.year, today.month,
        )

        seen_ids:    set[str]   = set()
        all_matches: list[dict] = []

        for day_str in month_days:
            try:
                day_matches = fetch_upcoming_matches(
                    sport_slug=sport_slug,
                    day=day_str,
                )
                new_count = 0
                for m in day_matches:
                    mid = m.get("od_match_id") or m.get("od_event_id")
                    if mid:
                        if mid not in seen_ids:
                            seen_ids.add(mid)
                            all_matches.append(m)
                            new_count += 1
                    else:
                        all_matches.append(m)
                        new_count += 1

                if day_matches:
                    logger.info(
                        "[od:upcoming] %s day=%s → %d fetched, %d new (total=%d)",
                        sport_slug, day_str, len(day_matches), new_count, len(all_matches),
                    )
            except Exception as day_exc:
                logger.warning("[od:upcoming] %s day=%s error: %s",
                               sport_slug, day_str, day_exc)

        matches = all_matches

    except SoftTimeLimitExceeded:
        logger.warning("[od:upcoming] Soft timeout %s — saving %d partial",
                       sport_slug, len(matches))
    except Exception as exc:
        raise self.retry(exc=exc)

    if not matches:
        return {"ok": False, "reason": "No matches fetched for month"}

    latency     = int((time.perf_counter() - t0) * 1000)
    avg_markets = int(sum(m.get("market_count", 0) for m in matches) / max(len(matches), 1))

    logger.info("[od:upcoming] %s → %d matches for full month, avg %d markets/match, %dms",
                sport_slug, len(matches), avg_markets, latency)

    cache_set(f"od:upcoming:{sport_slug}", {
        "source": "odibets", "sport": sport_slug, "mode": "upcoming",
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches, "avg_markets": avg_markets,
        "month": f"{date.today().year}-{date.today().month:02d}",
    }, ttl=3600)

    _upsert_and_chain(matches, "OdiBets")
    _persist_bk_matches(matches, "od", sport_slug)
    _emit("odibets", sport_slug, len(matches), latency)
    _schedule_alignment(sport_slug, countdown=60)

    return {
        "ok": True, "source": "odibets", "sport": sport_slug,
        "count": len(matches), "avg_markets": avg_markets, "latency_ms": latency,
    }


@celery.task(name="tasks.od.harvest_all_upcoming",
             soft_time_limit=120, time_limit=150)
def od_harvest_all_upcoming() -> dict:
    sigs = [od_harvest_sport.s(s) for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    logger.info("[od] dispatched %d upcoming tasks (full month)", len(sigs))
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

    _persist_b2b_matches(matches, sport_slug)
    _emit("b2b", sport_slug, len(matches), latency)
    _schedule_alignment(sport_slug, countdown=60)

    return {
        "ok": True, "source": "b2b", "sport": sport_slug,
        "count": len(matches), "latency_ms": latency,
    }


@celery.task(name="tasks.b2b.harvest_all_upcoming",
             soft_time_limit=30, time_limit=60)
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
    slug_map   = {v: k for k, v in _BK_NAMES.items()}
    bk_slug    = slug_map.get(bk_name, bk_name[:4].lower())
    _persist_bk_matches(matches, bk_slug, sport_slug)

    _publish(WS_CHANNEL, {
        "event": "odds_updated", "source": "b2b",
        "bookmaker": bk_name, "sport": sport, "mode": "upcoming",
        "page": page, "count": len(matches), "ts": _now_iso(),
    })
    return {"ok": True, "count": len(matches), "latency_ms": latency}


@celery.task(name="tasks.b2b_page.harvest_all_upcoming",
             soft_time_limit=30, time_limit=60)
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
# SBO UPCOMING
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

    return {
        "ok": True, "count": len(matches),
        "arb_count": arb_count, "latency_ms": latency,
    }


@celery.task(name="tasks.sbo.harvest_all_upcoming",
             soft_time_limit=30, time_limit=60)
def sbo_harvest_all_upcoming() -> dict:
    sigs = [sbo_harvest_sport.s(s, 90) for s in _SBO_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}