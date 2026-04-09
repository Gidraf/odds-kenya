"""
app/workers/tasks_market_align.py
===================================
Market Alignment Service
========================
Runs after each bookmaker harvest and reconciles ALL bookmaker markets
into the unified_match.markets_json so every endpoint sees the full picture.

Problem it solves
-----------------
After harvesting:
  BT  → BookmakerMatchOdds row: 24 markets
  SP  → BookmakerMatchOdds row: 16 markets
  OD  → BookmakerMatchOdds row: 10 markets

But unified_match.markets_json may only contain the LAST writer's markets
or a partial merge. This service:

  1. Finds every unified_match that has ≥2 BMO rows (multi-bookmaker matches)
  2. Merges ALL markets from ALL bookmakers into one canonical dict
  3. Picks the best (highest) price per outcome across all bookmakers
  4. Writes back the merged result to unified_match.markets_json
  5. Re-runs arb + EV detection on the merged markets
  6. Publishes a WebSocket event so SSE clients get the update

Scheduling
----------
Called automatically at the end of each bookmaker harvest via:
  align_sport_markets.apply_async(args=[sport_slug], queue="results", countdown=60)

Also runs on a beat schedule (every 15 min) as a safety net:
  sender.add_periodic_task(900.0, align_all_sports.s(), name="market-align-15min")

Can be triggered manually:
  celery -A app.workers.celery_tasks call tasks.align.sport --args='["soccer"]'
  celery -A app.workers.celery_tasks call tasks.align.all
"""

from __future__ import annotations

import random
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from celery.utils.log import get_task_logger

from app.workers.celery_tasks import celery, cache_set, cache_get, _now_iso, _publish

logger = get_task_logger(__name__)

WS_CHANNEL = "odds:updates"

_LOCAL_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
    "handball", "mma", "boxing", "darts", "esoccer",
]

# Only align matches whose start_time is within this window
# (no point re-aligning matches from 6 months ago)
_ALIGN_WINDOW_DAYS = 60


# =============================================================================
# CORE MARKET MERGING
# =============================================================================

def _extract_price(odd_data: Any) -> float:
    """Safely extract a float price from any odds format."""
    if isinstance(odd_data, (int, float)):
        return float(odd_data)
    if isinstance(odd_data, dict):
        for key in ("price", "odd", "value", "odds"):
            v = odd_data.get(key)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
    try:
        return float(odd_data)
    except (TypeError, ValueError):
        return 0.0


def _merge_bookmaker_markets(
    bmo_rows: list,
    bk_map:   dict[int, Any],
) -> tuple[dict[str, dict], dict[str, dict]]:
    """
    Merge all BookmakerMatchOdds rows into:
      - markets_by_bk: {bk_slug: {market_slug: {outcome: odd_data}}}
      - best_markets:  {market_slug: {outcome: {price, bookmaker_id, bk_slug}}}

    For each outcome across all bookmakers, picks the highest price.
    Returns (markets_by_bk, best_markets).
    """
    _BK_SLUG_MAP = {
        "sportpesa": "sp", "sp": "sp",
        "betika":    "bt", "bt": "bt",
        "odibets":   "od", "od": "od",
        "1xbet":    "b2b", "b2b": "b2b",
        "sbo":      "sbo",
    }

    markets_by_bk: dict[str, dict] = {}
    best_markets:  dict[str, dict] = {}

    for bmo in bmo_rows:
        bk_obj  = bk_map.get(bmo.bookmaker_id)
        bk_name = (bk_obj.name if bk_obj else str(bmo.bookmaker_id)).lower()
        slug    = _BK_SLUG_MAP.get(bk_name, bk_name[:3])

        raw_mkts = bmo.markets_json or {}
        if not raw_mkts:
            continue

        # Flatten nested market format → {market_slug: {outcome: price}}
        flat: dict[str, dict] = {}
        for mkt_slug, spec_dict in raw_mkts.items():
            if not isinstance(spec_dict, dict):
                flat[mkt_slug] = spec_dict
                continue
            outcomes: dict = {}
            for spec_val, inner in spec_dict.items():
                if isinstance(inner, dict):
                    for out_key, out_val in inner.items():
                        outcomes[out_key] = out_val
                else:
                    outcomes[spec_val] = inner
            if outcomes:
                flat[mkt_slug] = outcomes

        markets_by_bk[slug] = flat

        # Build best-price dict
        for mkt_slug, outcomes in flat.items():
            if mkt_slug not in best_markets:
                best_markets[mkt_slug] = {}
            for outcome, odd_data in outcomes.items():
                price = _extract_price(odd_data)
                if price <= 1.0:
                    continue
                existing = best_markets[mkt_slug].get(outcome)
                if existing is None or price > existing.get("price", 0.0):
                    best_markets[mkt_slug][outcome] = {
                        "best_price":        price,
                        "best_bookmaker_id": bmo.bookmaker_id,
                        "bk_slug":           slug,
                        "is_active":         True,
                        "updated_at":        _now_iso(),
                        "bookmakers": {
                            str(bmo.bookmaker_id): price,
                        },
                    }
                elif price > 1.0:
                    # Also record this bookmaker's price even if not best
                    existing["bookmakers"][str(bmo.bookmaker_id)] = price

    return markets_by_bk, best_markets


def _detect_arbs(
    best_markets: dict[str, dict],
    match_id:     int,
    home_team:    str,
    away_team:    str,
    sport:        str,
) -> list[dict]:
    """
    Detect arbitrage opportunities across all bookmakers.
    Returns list of arb dicts ready to insert into ArbitrageOpportunity.
    """
    arbs = []
    for mkt_slug, outcomes in best_markets.items():
        if len(outcomes) < 2:
            continue
        # Need at least 2 different bookmakers for a cross-bk arb
        all_bk_ids = set()
        prices     = {}
        for outcome, data in outcomes.items():
            price = data.get("best_price", 0.0)
            if price > 1.0:
                prices[outcome] = price
                all_bk_ids.add(data.get("best_bookmaker_id"))

        if len(prices) < 2 or len(all_bk_ids) < 2:
            continue

        arb_sum = sum(1.0 / p for p in prices.values())
        if arb_sum >= 1.0:
            continue

        profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
        if profit_pct < 0.5:
            continue

        legs = []
        for outcome, data in outcomes.items():
            price = data.get("best_price", 0.0)
            if price <= 1.0:
                continue
            stake_frac = (1.0 / price) / arb_sum
            legs.append({
                "selection":   outcome,
                "bookmaker":   data.get("bk_slug", ""),
                "bookmaker_id": data.get("best_bookmaker_id"),
                "price":       price,
                "stake_pct":   round(stake_frac * 100, 2),
                "stake_kes":   round(stake_frac * 1000, 2),
                "return_kes":  round(stake_frac * 1000 * price, 2),
            })

        arbs.append({
            "match_id":        match_id,
            "home_team":       home_team,
            "away_team":       away_team,
            "sport":           sport,
            "market":          mkt_slug,
            "profit_pct":      profit_pct,
            "arb_sum":         round(arb_sum, 6),
            "legs":            legs,
            "stake_100_returns": round(100 / arb_sum, 2),
            "bookmaker_ids":   sorted(str(l["bookmaker"]) for l in legs),
        })

    arbs.sort(key=lambda x: -x["profit_pct"])
    return arbs


def _align_single_match(um, bmo_rows: list, bk_map: dict) -> dict:
    """
    Align one unified_match:
      1. Merge all BMO markets → best_markets
      2. Write merged markets_json back to unified_match
      3. Detect arbs on merged markets
      4. Return stats dict

    Returns {"match_id", "market_count", "bk_count", "arbs_found", "updated"}
    """
    from app.extensions import db
    from app.models.odds_model import (
        ArbitrageOpportunity, OpportunityStatus,
    )

    if not bmo_rows:
        return {"match_id": um.id, "market_count": 0, "bk_count": 0,
                "arbs_found": 0, "updated": False}

    markets_by_bk, best_markets = _merge_bookmaker_markets(bmo_rows, bk_map)

    if not best_markets:
        return {"match_id": um.id, "market_count": 0, "bk_count": len(bmo_rows),
                "arbs_found": 0, "updated": False}

    # Write merged markets back to unified_match.markets_json
    # Format: {market_slug: {outcome: {best_price, best_bookmaker_id, bookmakers:{id: price}}}}
    try:
        um.markets_json = best_markets
        db.session.add(um)
    except Exception as exc:
        logger.warning("[align] match %d markets_json write failed: %s", um.id, exc)

    # Detect arbs on merged cross-bookmaker markets
    arb_dicts = _detect_arbs(
        best_markets, um.id,
        um.home_team_name or "", um.away_team_name or "",
        um.sport_name or "",
    )

    arbs_written = 0
    if arb_dicts:
        try:
            # Close existing open arbs for this match
            ArbitrageOpportunity.query.filter_by(
                match_id=um.id, status=OpportunityStatus.OPEN
            ).update({
                "status":    OpportunityStatus.CLOSED,
                "closed_at": datetime.utcnow(),
            })
            # Insert fresh arbs
            now = datetime.now(timezone.utc)
            for arb in arb_dicts:
                db.session.add(ArbitrageOpportunity(
                    match_id=um.id,
                    home_team=arb["home_team"],
                    away_team=arb["away_team"],
                    sport=arb["sport"],
                    competition=um.competition_name or "",
                    match_start=um.start_time,
                    market=arb["market"],
                    profit_pct=arb["profit_pct"],
                    peak_profit_pct=arb["profit_pct"],
                    arb_sum=arb["arb_sum"],
                    legs_json=arb["legs"],
                    stake_100_returns=arb["stake_100_returns"],
                    bookmaker_ids=arb["bookmaker_ids"],
                    status=OpportunityStatus.OPEN,
                    open_at=now,
                ))
            arbs_written = len(arb_dicts)
        except Exception as exc:
            logger.warning("[align] match %d arb write failed: %s", um.id, exc)

    return {
        "match_id":     um.id,
        "market_count": len(best_markets),
        "bk_count":     len(markets_by_bk),
        "arbs_found":   arbs_written,
        "updated":      True,
    }


# =============================================================================
# CELERY TASKS
# =============================================================================

@celery.task(
    name="tasks.align.sport",
    bind=True,
    max_retries=3,          # Increased retries to handle deadlocks gracefully
    default_retry_delay=10, 
    soft_time_limit=600,    # 10 min per sport
    time_limit=660,
    acks_late=True,
)
def align_sport_markets(self, sport_slug: str, batch_size: int = 100) -> dict:
    """
    Align all markets for a sport:
      - Finds all unified_matches for this sport with ≥1 BMO row
      - Merges markets from all bookmakers into best_markets
      - Writes unified markets_json back to each match
      - Re-detects arb opportunities on merged markets
      - Commits in batches of `batch_size` to avoid long transactions

    Called automatically after each harvest with countdown=60s.
    Also on beat schedule every 15 min via align_all_sports.
    """
    from app.extensions import db
    from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds
    from app.models.bookmakers_model import Bookmaker
    from sqlalchemy import or_

    t0 = time.perf_counter()

    # Map sport slug → DB sport name
    _SLUG_TO_DB = {
        "soccer": "Soccer", "football": "Soccer",
        "basketball": "Basketball", "tennis": "Tennis",
        "ice-hockey": "Ice Hockey", "volleyball": "Volleyball",
        "cricket": "Cricket", "rugby": "Rugby",
        "table-tennis": "Table Tennis", "handball": "Handball",
        "mma": "MMA", "boxing": "Boxing", "darts": "Darts",
        "esoccer": "eSoccer", "baseball": "Baseball",
        "american-football": "American Football",
    }
    db_sport_names = []
    if sport_slug == "soccer":
        db_sport_names = ["Soccer", "Football"]
    else:
        name = _SLUG_TO_DB.get(sport_slug)
        if name:
            db_sport_names = [name]
        else:
            db_sport_names = [sport_slug.replace("-", " ").title()]

    # Window: only align matches starting within the alignment window
    now        = datetime.now(timezone.utc)
    win_start  = now - timedelta(hours=3)            # include recently started
    win_end    = now + timedelta(days=_ALIGN_WINDOW_DAYS)

    try:
        # Load all bookmaker objects for name→slug lookup
        all_bk_objs = Bookmaker.query.all()
        bk_map: dict[int, Any] = {b.id: b for b in all_bk_objs}

        # Query matches for this sport in the time window
        if len(db_sport_names) == 1:
            base_q = UnifiedMatch.query.filter(
                UnifiedMatch.sport_name == db_sport_names[0],
            )
        else:
            base_q = UnifiedMatch.query.filter(
                or_(*[UnifiedMatch.sport_name == n for n in db_sport_names])
            )

        base_q = base_q.filter(
            or_(
                UnifiedMatch.start_time.is_(None),
                UnifiedMatch.start_time >= win_start,
                UnifiedMatch.start_time <= win_end,
            )
        )

        total_matches = base_q.count()
        logger.info(
            "[align:%s] found %d matches to align", sport_slug, total_matches,
        )

        if total_matches == 0:
            return {"ok": True, "sport": sport_slug, "aligned": 0,
                    "skipped": 0, "arbs_found": 0, "latency_ms": 0}

        # Pre-load ALL BMO rows for this sport in one query to avoid N+1
        # Get all match IDs first
        all_match_ids = [
            row.id for row in base_q.with_entities(UnifiedMatch.id).all()
        ]

        # Load all BMO rows for these matches in one shot
        all_bmo_rows = BookmakerMatchOdds.query.filter(
            BookmakerMatchOdds.match_id.in_(all_match_ids)
        ).all()

        # Group by match_id
        bmo_by_match: dict[int, list] = {}
        for bmo in all_bmo_rows:
            bmo_by_match.setdefault(bmo.match_id, []).append(bmo)

        # Process in batches
        aligned   = 0
        skipped   = 0
        arbs_total = 0
        errors    = 0

        offset = 0
        while offset < total_matches:
            batch_ums = base_q.offset(offset).limit(batch_size).all()
            if not batch_ums:
                break
                
            # CRITICAL DEADLOCK FIX: Sort the UnifiedMatches in the batch by ID BEFORE processing.
            # This ensures that if two workers grab overlapping batches, they will attempt 
            # to acquire row locks in the exact same order, preventing deadlocks.
            batch_ums = sorted(batch_ums, key=lambda m: m.id)

            batch_stats = []
            for um in batch_ums:
                bmo_rows = bmo_by_match.get(um.id, [])

                # Skip matches with only 1 bookmaker — nothing to align
                if len(bmo_rows) < 1:
                    skipped += 1
                    continue

                try:
                    stat = _align_single_match(um, bmo_rows, bk_map)
                    batch_stats.append(stat)
                    if stat["updated"]:
                        aligned   += 1
                        arbs_total += stat["arbs_found"]
                    else:
                        skipped += 1
                except Exception as exc:
                    logger.warning("[align:%s] match %d error: %s",
                                   sport_slug, um.id, exc)
                    errors += 1

            # Commit this batch
            try:
                db.session.commit()
                logger.debug(
                    "[align:%s] committed batch offset=%d (%d aligned, %d arbs)",
                    sport_slug, offset,
                    sum(1 for s in batch_stats if s.get("updated")),
                    sum(s.get("arbs_found", 0) for s in batch_stats),
                )
            except Exception as exc:
                logger.error("[align:%s] batch commit error at offset=%d: %s",
                             sport_slug, offset, exc)
                # Must cleanly rollback the entire batch transaction before retrying
                try:
                    db.session.rollback()
                except Exception:
                    pass
                errors += len(batch_ums)

            offset += batch_size

    except Exception as exc:
        logger.error("[align:%s] fatal error: %s", sport_slug, exc)
        try:
            db.session.rollback()
        except Exception:
            pass
        # Add Jitter to the retry to ensure workers don't immediately collide again
        raise self.retry(exc=exc, countdown=random.uniform(5.0, 20.0))

    latency_ms = int((time.perf_counter() - t0) * 1000)

    result = {
        "ok":          True,
        "sport":       sport_slug,
        "total":       total_matches,
        "aligned":     aligned,
        "skipped":     skipped,
        "errors":      errors,
        "arbs_found":  arbs_total,
        "latency_ms":  latency_ms,
        "ts":          _now_iso(),
    }

    logger.info(
        "[align:%s] done — aligned=%d skipped=%d errors=%d arbs=%d (%dms)",
        sport_slug, aligned, skipped, errors, arbs_total, latency_ms,
    )

    # Publish update event so SSE clients refresh
    if aligned > 0:
        _publish(WS_CHANNEL, {
            "event":      "markets_aligned",
            "sport":      sport_slug,
            "aligned":    aligned,
            "arbs_found": arbs_total,
            "ts":         _now_iso(),
        })

    # Cache alignment stats for the health dashboard
    cache_set(f"align:stats:{sport_slug}", result, ttl=1800)

    return result


@celery.task(
    name="tasks.align.all",
    soft_time_limit=120,
    time_limit=150,
)
def align_all_sports() -> dict:
    """
    Fan-out: trigger align_sport_markets for every sport.
    Called on beat schedule every 15 min and after bulk harvests.
    """
    from celery import group as celery_group

    sigs = [
        align_sport_markets.s(sport, 100)
        for sport in _LOCAL_SPORTS
    ]
    celery_group(sigs).apply_async(queue="results")

    logger.info("[align:all] dispatched %d sport alignment tasks", len(sigs))
    return {"dispatched": len(sigs), "sports": _LOCAL_SPORTS}


@celery.task(
    name="tasks.align.match",
    bind=True,
    max_retries=3,          # Increased retries
    default_retry_delay=5,
    soft_time_limit=30,
    time_limit=40,
    acks_late=True,
)
def align_single_match_task(self, match_id: int) -> dict:
    """
    Align one specific match by DB ID.
    Called immediately after a new BMO row is written for a match,
    ensuring the unified_match.markets_json is always up to date.

    Usage:
      align_single_match_task.apply_async(args=[match_id], queue="results")
    """
    from app.extensions import db
    from app.models.odds_model import UnifiedMatch, BookmakerMatchOdds
    from app.models.bookmakers_model import Bookmaker

    try:
        um = UnifiedMatch.query.get(match_id)
        if not um:
            return {"ok": False, "error": f"Match {match_id} not found"}

        bmo_rows = BookmakerMatchOdds.query.filter_by(match_id=match_id).all()
        if not bmo_rows:
            return {"ok": True, "match_id": match_id, "skipped": True,
                    "reason": "No BMO rows"}

        bk_ids   = {bmo.bookmaker_id for bmo in bmo_rows}
        bk_map   = {b.id: b for b in Bookmaker.query.filter(
            Bookmaker.id.in_(bk_ids)
        ).all()}

        stat = _align_single_match(um, bmo_rows, bk_map)
        db.session.commit()

        logger.info(
            "[align:match] %d aligned — %d markets from %d bookmakers, %d arbs",
            match_id, stat["market_count"], stat["bk_count"], stat["arbs_found"],
        )
        return {"ok": True, **stat}

    except Exception as exc:
        logger.error("[align:match] %d error: %s", match_id, exc)
        try:
            db.session.rollback()
        except Exception:
            pass
        # Jitter the retry to prevent immediate secondary collisions
        raise self.retry(exc=exc, countdown=random.uniform(2.0, 8.0))


@celery.task(
    name="tasks.align.status",
    soft_time_limit=20,
    time_limit=30,
)
def alignment_status() -> dict:
    """
    Return the last alignment stats for all sports.
    Useful for the health dashboard.

    Call: celery -A app.workers.celery_tasks call tasks.align.status
    """
    stats = {}
    for sport in _LOCAL_SPORTS:
        cached = cache_get(f"align:stats:{sport}")
        stats[sport] = cached or {"aligned": 0, "arbs_found": 0, "ts": None}
    return {"ok": True, "sports": stats, "ts": _now_iso()}


# =============================================================================
# HOOK — call this at the end of each bookmaker harvest task
# =============================================================================

def schedule_alignment(sport_slug: str, countdown: int = 60) -> None:
    """
    Schedule market alignment for a sport after harvest completes.
    countdown=60 gives the persist pipeline time to finish writing BMO rows
    before the alignment job reads them.

    Add this call at the end of sp_harvest_sport, bt_harvest_sport,
    od_harvest_sport in tasks_upcoming.py:

        from app.workers.tasks_market_align import schedule_alignment
        schedule_alignment(sport_slug, countdown=60)
    """
    try:
        # Add Jitter to the scheduling delay to prevent multiple bookmakers 
        # from finishing at the exact same time and triggering parallel alignments 
        # that collide in the database.
        jittered_delay = countdown + random.uniform(5.0, 30.0)
        
        align_sport_markets.apply_async(
            args=[sport_slug, 100],
            queue="results",
            countdown=jittered_delay,
        )
        logger.info(
            "[align] scheduled alignment for %s in %ds", sport_slug, int(jittered_delay),
        )
    except Exception as exc:
        logger.warning("[align] failed to schedule alignment for %s: %s",
                       sport_slug, exc)