# app/workers/celery_tasks.py
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta

from celery.signals import worker_ready

from app.workers.celery_app import celery_app as celery

_log = logging.getLogger(__name__)

# =============================================================================
# REDIS HELPERS
# =============================================================================
def _redis(db: int = 2):
    import redis as _r
    import os

    # Use REDIS_URL from environment, fallback to default
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    # If the URL already contains a database number (e.g., /0), remove it
    # so we can append our own /{db} cleanly.
    if url.count("/") >= 3:
        # Example: redis://:pass@host:6379/0  →  redis://:pass@host:6379
        base = url.rsplit("/", 1)[0]
    else:
        base = url
    return _r.Redis.from_url(f"{base}/{db}", decode_responses=False, socket_timeout=5)

def cache_set(key: str, data, ttl: int = 600) -> bool:
    try:
        _redis().set(key, json.dumps(data, default=str), ex=ttl)
        return True
    except Exception:
        return False

def cache_get(key: str):
    try:
        raw = _redis().get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None

def cache_delete(key: str) -> bool:
    try:
        _redis().delete(key)
        return True
    except Exception:
        return False

def cache_keys(pattern: str) -> list[str]:
    try:
        return [k.decode() if isinstance(k, bytes) else k for k in _redis().keys(pattern)]
    except Exception:
        return []

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _publish(channel: str, data: dict) -> None:
    try:
        _redis().publish(channel, json.dumps(data, default=str))
    except Exception:
        pass

# =============================================================================
# SPORT NORMALISATION
# =============================================================================
_SPORT_NORM: dict[str, str] = {
    "football": "Soccer", "Football": "Soccer", "soccer": "Soccer", "Soccer": "Soccer",
    "basketball": "Basketball", "Basketball": "Basketball",
    "tennis": "Tennis", "Tennis": "Tennis",
    "ice-hockey": "Ice Hockey", "ice hockey": "Ice Hockey", "Ice Hockey": "Ice Hockey",
    "volleyball": "Volleyball", "Volleyball": "Volleyball",
    "cricket": "Cricket", "Cricket": "Cricket",
    "rugby": "Rugby", "Rugby": "Rugby",
    "table-tennis": "Table Tennis", "table tennis": "Table Tennis", "Table Tennis": "Table Tennis",
    "handball": "Handball", "Handball": "Handball",
    "mma": "MMA", "MMA": "MMA",
    "boxing": "Boxing", "Boxing": "Boxing",
    "darts": "Darts", "Darts": "Darts",
    "esoccer": "eSoccer", "eSoccer": "eSoccer", "efootball": "eSoccer", "eFootball": "eSoccer",
    "american-football": "American Football", "American Football": "American Football",
    "baseball": "Baseball", "Baseball": "Baseball",
}

def _normalise_sport_name(raw: str) -> str:
    if not raw:
        return raw
    return _SPORT_NORM.get(raw, _SPORT_NORM.get(raw.lower(), raw))

# =============================================================================
# BOOKMAKER HELPERS
# =============================================================================
_BK_NAME_TO_SLUG: dict[str, str] = {
    "sportpesa": "sp", "betika": "bt", "odibets": "od",
    "1xbet": "1xbet", "22bet": "22bet", "betwinner": "betwinner",
    "melbet": "melbet", "megapari": "megapari", "helabet": "helabet",
    "paripesa": "paripesa", "sbo": "sbo",
}
_BK_SLUG_TO_NAME: dict[str, str] = {
    "sp": "SportPesa", "bt": "Betika", "od": "OdiBets",
    "1xbet": "1xBet", "22bet": "22Bet", "betwinner": "Betwinner",
    "melbet": "Melbet", "megapari": "Megapari", "helabet": "Helabet",
    "paripesa": "Paripesa", "sbo": "SBO",
}

def _get_or_create_bookmaker(bk_name: str) -> int | None:
    try:
        from app.models.bookmakers_model import Bookmaker
        from app.extensions import db
        canonical = _BK_SLUG_TO_NAME.get(bk_name.lower(), bk_name)
        bm = Bookmaker.query.filter(Bookmaker.name.ilike(f"%{canonical}%")).first()
        if bm:
            return bm.id
        if canonical != bk_name:
            bm = Bookmaker.query.filter(Bookmaker.name.ilike(f"%{bk_name}%")).first()
            if bm:
                return bm.id
        slug   = _BK_NAME_TO_SLUG.get(canonical.lower(), bk_name.lower()[:4])
        domain = f"{slug}.co.ke"
        bm = Bookmaker(name=canonical, domain=domain, is_active=True)
        db.session.add(bm)
        db.session.commit()
        _log.info("[bookmaker] auto-created: %s (id=%d)", canonical, bm.id)
        return bm.id
    except Exception as exc:
        _log.warning("[bookmaker] create failed for %s: %s", bk_name, exc)
        return None

def _extract_betradar_id(m: dict) -> str:
    raw = (
        m.get("betradar_id")  or m.get("betradarId")   or
        m.get("sr_id")        or m.get("bt_parent_id") or m.get("od_parent_id") or ""
    )
    val = str(raw).strip()
    return "" if val in ("0", "None", "null", "") else val

def _parse_start_time(raw) -> datetime | None:
    if not raw:
        return None
    try:
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, str):
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(float(raw))
    except Exception:
        pass
    return None

def _fuzzy_find_match(home: str, away: str, start_time_raw) -> str | None:
    if not home or not away:
        return None
    try:
        from app.models.odds import UnifiedMatch
        from sqlalchemy import func
        start_dt = _parse_start_time(start_time_raw)
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
        _log.debug("[fuzzy_find] error: %s", exc)
        return None

# =============================================================================
# UPSERT PIPELINE
# =============================================================================
def _to_upsert_shape(m: dict) -> dict:
    betradar_id  = _extract_betradar_id(m)
    join_key_raw = str(m.get("join_key") or "")
    join_key_id  = ""
    for prefix in ("br_", "bt_", "od_", "fuzzy_"):
        if join_key_raw.startswith(prefix):
            join_key_id = join_key_raw[len(prefix):]
            break
    return {
        "match_id": (
            betradar_id or m.get("bt_match_id") or m.get("od_match_id") or
            m.get("match_id") or m.get("event_id") or join_key_id or join_key_raw or ""
        ),
        "betradar_id": betradar_id,
        "home_team":   m.get("home_team",   ""),
        "away_team":   m.get("away_team",   ""),
        "sport":       m.get("sport",       ""),
        "competition": m.get("competition", ""),
        "start_time":  m.get("start_time"),
        "markets":     m.get("markets") or m.get("best_odds") or {},
        "bookmakers":  m.get("bookmakers") or {},
    }

def _upsert_unified_match(match_data: dict, bookmaker_id, bookmaker_name: str):
    try:
        from app.extensions import db
        from app.models.odds import (UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory)

        parent_id = str(match_data.get("match_id") or "").strip()
        if not parent_id:
            return None

        sport      = _normalise_sport_name(str(match_data.get("sport") or ""))
        comp       = str(match_data.get("competition") or "")
        home       = str(match_data.get("home_team")   or "").strip()
        away       = str(match_data.get("away_team")   or "").strip()
        start_time = _parse_start_time(match_data.get("start_time"))

        um = UnifiedMatch.query.filter_by(parent_match_id=parent_id).with_for_update().first()
        if not um:
            um = UnifiedMatch(
                parent_match_id=parent_id, home_team_name=home, away_team_name=away,
                sport_name=sport, competition_name=comp, start_time=start_time,
            )
            db.session.add(um)
            db.session.flush()
        else:
            if home  and home  != um.home_team_name:  um.home_team_name   = home
            if away  and away  != um.away_team_name:  um.away_team_name   = away
            if sport:                                   um.sport_name       = sport
            if comp  and not um.competition_name:       um.competition_name = comp
            if start_time and not um.start_time:        um.start_time       = start_time

        if bookmaker_id is None:
            db.session.commit()
            return um.id

        bmo = BookmakerMatchOdds.query.filter_by(
            match_id=um.id, bookmaker_id=bookmaker_id
        ).with_for_update().first()
        if not bmo:
            bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bookmaker_id)
            db.session.add(bmo)
            db.session.flush()

        history_batch = []
        for mkt_key, outcomes in (match_data.get("markets") or {}).items():
            if isinstance(outcomes, (int, float)):
                outcomes = {"value": float(outcomes)}
            if not isinstance(outcomes, dict):
                continue
            for outcome, odds_data in outcomes.items():
                try:
                    if isinstance(odds_data, (int, float)):
                        price = float(odds_data)
                    elif isinstance(odds_data, dict):
                        price = float(
                            odds_data.get("best_price") or odds_data.get("odds") or
                            odds_data.get("odd") or odds_data.get("price") or 0
                        )
                    else:
                        continue
                except (TypeError, ValueError):
                    continue

                if price <= 1.0:
                    continue

                price_changed, old_price = bmo.upsert_selection(
                    market=mkt_key, specifier=None, selection=outcome, price=price,
                )
                um.upsert_bookmaker_price(
                    market=mkt_key, specifier=None, selection=outcome,
                    price=price, bookmaker_id=bookmaker_id,
                )
                if price_changed:
                    history_batch.append({
                        "bmo_id": bmo.id, "bookmaker_id": bookmaker_id,
                        "match_id": um.id, "market": mkt_key, "specifier": None,
                        "selection": outcome, "old_price": old_price, "new_price": price,
                        "price_delta": round(price - old_price, 4) if old_price else None,
                        "recorded_at": datetime.now(timezone.utc),
                    })

        if history_batch:
            BookmakerOddsHistory.bulk_append(history_batch)

        db.session.commit()
        return um.id

    except Exception as exc:
        _log.error("[upsert_unified_match] %s: %s", bookmaker_name, exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        return None

# app/workers/celery_tasks.py

def _upsert_and_chain(matches: list[dict], bk_name: str) -> None:
    """
    Upsert all matches first, collect their DB IDs, then dispatch
    compute_ev_arb in small batches with staggered countdowns so the
    queue never gets a 500-task spike in one second.
    """
    from app.workers.tasks_ops import compute_ev_arb

    BATCH_SIZE  = 25    # tasks per batch
    BATCH_DELAY = 5     # extra seconds between each batch

    try:
        bk_id = _get_or_create_bookmaker(bk_name)

        # ── 1. Upsert everything, collect IDs ──────────────────────────────
        match_ids: list[int] = []
        for m in matches:
            mid = _upsert_unified_match(_to_upsert_shape(m), bk_id, bk_name)
            if mid:
                match_ids.append(mid)

        if not match_ids:
            return

        # ── 2. Dispatch in batches, each batch delayed a bit more ──────────
        for batch_num, i in enumerate(range(0, len(match_ids), BATCH_SIZE)):
            batch     = match_ids[i : i + BATCH_SIZE]
            countdown = 10 + (batch_num * BATCH_DELAY)   # 10s, 15s, 20s …

            for mid in batch:
                compute_ev_arb.apply_async(
                    args=[mid],
                    queue="ev_arb",
                    countdown=countdown,
                )

        _log.info(
            "[upsert_and_chain] %s: %d matches upserted, %d ev_arb tasks "
            "dispatched in %d batches (batch_size=%d, delay=%ds)",
            bk_name,
            len(match_ids),
            len(match_ids),
            -(-len(match_ids) // BATCH_SIZE),   # ceiling division
            BATCH_SIZE,
            BATCH_DELAY,
        )

    except Exception as exc:
        _log.error("[upsert_and_chain] %s: %s", bk_name, exc)
    from app.workers.tasks_ops import compute_ev_arb
    try:
        bk_id = _get_or_create_bookmaker(bk_name)
        for m in matches:
            mid = _upsert_unified_match(_to_upsert_shape(m), bk_id, bk_name)
            if mid:
                compute_ev_arb.apply_async(args=[mid], queue="ev_arb", countdown=1)
    except Exception as exc:
        _log.error("[upsert_and_chain] %s: %s", bk_name, exc)
