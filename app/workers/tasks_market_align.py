"""
app/workers/tasks_market_align.py
===================================
Market Alignment Service
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

_ALIGN_WINDOW_DAYS = 60

# ══════════════════════════════════════════════════════════════════════════════
# CORE MARKET MERGING & ARB DETECTION
# ══════════════════════════════════════════════════════════════════════════════

SAFE_ARB_MARKETS = {
    "match_winner", "1x2", "moneyline", "btts", "both_teams_score",
    "double_chance", "odd_even"
}

def _extract_price(odd_data: Any) -> float:
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


def _merge_bookmaker_markets(bmo_rows: list, bk_map: dict[int, Any]) -> tuple[dict[str, dict], dict[str, dict]]:
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
                    existing["bookmakers"][str(bmo.bookmaker_id)] = price

    return markets_by_bk, best_markets


def _detect_arbs(best_markets: dict[str, dict], match_id: int, home_team: str, away_team: str, sport: str) -> list[dict]:
    arbs = []
    for mkt_slug, outcomes in best_markets.items():
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

        all_bk_ids = set()
        prices     = {}
        is_complete = True

        for outcome, data in outcomes.items():
            price = data.get("best_price", 0.0)
            if price > 1.0:
                prices[outcome] = price
                all_bk_ids.add(data.get("best_bookmaker_id"))
            else:
                is_complete = False

        # 3. Final validation: Must have all legs, must span multiple bookmakers
        if not is_complete or len(prices) < 2 or len(all_bk_ids) < 2:
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
    from app.extensions import db
    from app.models.odds import ArbitrageOpportunity, OpportunityStatus
    from sqlalchemy.orm.attributes import flag_modified

    if not bmo_rows:
        return {"match_id": um.id, "market_count": 0, "bk_count": 0, "arbs_found": 0, "updated": False}

    markets_by_bk, best_markets = _merge_bookmaker_markets(bmo_rows, bk_map)

    if not best_markets:
        return {"match_id": um.id, "market_count": 0, "bk_count": len(bmo_rows), "arbs_found": 0, "updated": False}

    try:
        um.markets_json = best_markets
        flag_modified(um, "markets_json")  # CRITICAL: Forces SQLAlchemy to recognize JSON mutation
    except Exception as exc:
        logger.warning("[align] match %d markets_json write failed: %s", um.id, exc)

    arb_dicts = _detect_arbs(
        best_markets, um.id,
        um.home_team_name or "", um.away_team_name or "",
        um.sport_name or "",
    )

    arbs_written = 0
    if arb_dicts:
        try:
            ArbitrageOpportunity.query.filter_by(match_id=um.id, status=OpportunityStatus.OPEN).update({
                "status":    OpportunityStatus.CLOSED,
                "closed_at": datetime.utcnow(),
            })
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
    max_retries=3,          
    default_retry_delay=10, 
    soft_time_limit=600,   
    time_limit=660,
    acks_late=True,
)
def align_sport_markets(self, sport_slug: str, batch_size: int = 100) -> dict:
    from app.extensions import db
    from app.models.odds import UnifiedMatch, BookmakerMatchOdds
    from app.models.bookmakers_model import Bookmaker
    from sqlalchemy import or_
    from sqlalchemy.orm.exc import StaleDataError

    t0 = time.perf_counter()

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
        db_sport_names = [name] if name else [sport_slug.replace("-", " ").title()]

    now        = datetime.now(timezone.utc)
    win_start  = now - timedelta(hours=3)            
    win_end    = now + timedelta(days=_ALIGN_WINDOW_DAYS)

    try:
        all_bk_objs = Bookmaker.query.all()
        bk_map: dict[int, Any] = {b.id: b for b in all_bk_objs}

        if len(db_sport_names) == 1:
            base_q = UnifiedMatch.query.filter(UnifiedMatch.sport_name == db_sport_names[0])
        else:
            base_q = UnifiedMatch.query.filter(or_(*[UnifiedMatch.sport_name == n for n in db_sport_names]))

        base_q = base_q.filter(
            or_(
                UnifiedMatch.start_time.is_(None),
                UnifiedMatch.start_time >= win_start,
                UnifiedMatch.start_time <= win_end,
            )
        )

        # 1. Fetch ONLY the IDs first (prevents massive memory buffering and staleness)
        all_match_ids = [row.id for row in base_q.with_entities(UnifiedMatch.id).all()]
        total_matches = len(all_match_ids)
        
        logger.info("[align:%s] found %d matches to align", sport_slug, total_matches)

        if total_matches == 0:
            return {"ok": True, "sport": sport_slug, "aligned": 0, "skipped": 0, "arbs_found": 0, "latency_ms": 0}

        aligned = skipped = arbs_total = errors = 0
        
        # 2. Process in fresh, isolated chunks
        for i in range(0, total_matches, batch_size):
            chunk_ids = all_match_ids[i:i+batch_size]
            
            # FRESH QUERY: get current state of these matches right before processing
            batch_ums = UnifiedMatch.query.filter(UnifiedMatch.id.in_(chunk_ids)).all()
            if not batch_ums:
                continue
                
            # DEADLOCK PREVENTION: Always sort batch modifications by Primary Key
            batch_ums = sorted(batch_ums, key=lambda m: m.id)
            current_um_ids = [um.id for um in batch_ums]

            # FETCH BMOs specifically for this chunk
            bmo_rows_chunk = BookmakerMatchOdds.query.filter(BookmakerMatchOdds.match_id.in_(current_um_ids)).all()
            bmo_by_match: dict[int, list] = {}
            for bmo in bmo_rows_chunk:
                bmo_by_match.setdefault(bmo.match_id, []).append(bmo)

            batch_stats = []
            for um in batch_ums:
                bmo_rows = bmo_by_match.get(um.id, [])

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
                    logger.warning("[align:%s] match %d error: %s", sport_slug, um.id, exc)
                    errors += 1

            # 3. Handle concurrent mutations safely
            try:
                db.session.commit()
            except StaleDataError as exc:
                logger.warning("[align:%s] StaleDataError at chunk %d (concurrent background update): %s", sport_slug, i, exc)
                db.session.rollback()
                errors += len(batch_ums)
            except Exception as exc:
                logger.error("[align:%s] batch commit error at chunk %d: %s", sport_slug, i, exc)
                db.session.rollback()
                errors += len(batch_ums)

    except Exception as exc:
        logger.error("[align:%s] fatal error: %s", sport_slug, exc)
        try:
            db.session.rollback()
        except Exception:
            pass
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

    if aligned > 0:
        _publish(WS_CHANNEL, {
            "event":      "markets_aligned",
            "sport":      sport_slug,
            "aligned":    aligned,
            "arbs_found": arbs_total,
            "ts":         _now_iso(),
        })

    cache_set(f"align:stats:{sport_slug}", result, ttl=1800)
    return result


@celery.task(name="tasks.align.all", soft_time_limit=120, time_limit=150)
def align_all_sports() -> dict:
    from celery import group as celery_group
    sigs = [align_sport_markets.s(sport, 100) for sport in _LOCAL_SPORTS]
    celery_group(sigs).apply_async(queue="results")
    return {"dispatched": len(sigs), "sports": _LOCAL_SPORTS}


@celery.task(
    name="tasks.align.match",
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    soft_time_limit=30,
    time_limit=40,
    acks_late=True,
)
def align_single_match_task(self, match_id: int) -> dict:
    from app.extensions import db
    from app.models.odds import UnifiedMatch, BookmakerMatchOdds
    from app.models.bookmakers_model import Bookmaker

    try:
        um = UnifiedMatch.query.get(match_id)
        if not um:
            return {"ok": False, "error": f"Match {match_id} not found"}

        bmo_rows = BookmakerMatchOdds.query.filter_by(match_id=match_id).all()
        if not bmo_rows:
            return {"ok": True, "match_id": match_id, "skipped": True, "reason": "No BMO rows"}

        bk_ids   = {bmo.bookmaker_id for bmo in bmo_rows}
        bk_map   = {b.id: b for b in Bookmaker.query.filter(Bookmaker.id.in_(bk_ids)).all()}

        stat = _align_single_match(um, bmo_rows, bk_map)
        db.session.commit()

        return {"ok": True, **stat}

    except Exception as exc:
        logger.error("[align:match] %d error: %s", match_id, exc)
        try:
            db.session.rollback()
        except Exception:
            pass
        raise self.retry(exc=exc, countdown=random.uniform(2.0, 8.0))


@celery.task(name="tasks.align.status", soft_time_limit=20, time_limit=30)
def alignment_status() -> dict:
    stats = {}
    for sport in _LOCAL_SPORTS:
        cached = cache_get(f"align:stats:{sport}")
        stats[sport] = cached or {"aligned": 0, "arbs_found": 0, "ts": None}
    return {"ok": True, "sports": stats, "ts": _now_iso()}


def schedule_alignment(sport_slug: str, countdown: int = 60) -> None:
    try:
        jittered_delay = countdown + random.uniform(5.0, 30.0)
        align_sport_markets.apply_async(
            args=[sport_slug, 100],
            queue="results",
            countdown=jittered_delay,
        )
    except Exception as exc:
        logger.warning("[align] failed to schedule alignment for %s: %s", sport_slug, exc)