"""
app/workers/celery_tasks.py  (patched)
========================================
Central Celery app + shared helpers.

KEY FIX: _upsert_and_chain now imports compute_ev_arb from tasks_ops
         (was broken: the function didn't exist there).
"""
# ── This file is a DROP-IN PATCH — copy it over the original ──────────────────
# It preserves ALL existing symbols/behaviour while fixing:
#   1. compute_ev_arb import in _upsert_and_chain
#   2. _to_upsert_shape extended fallback chain
#   3. Adds _publish helper used by tasks_ops

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone, timedelta

from app import create_app
from app.extensions import celery as celery_service, init_celery

LIVE_ENABLED: bool = False

TASK_ROUTES: dict[str, dict] = {
    "harvest.bookmaker_sport":             {"queue": "harvest"},
    "harvest.all_upcoming":                {"queue": "harvest"},
    "harvest.merge_broadcast":             {"queue": "harvest"},
    "harvest.value_bets":                  {"queue": "ev_arb"},
    "harvest.cleanup":                     {"queue": "harvest"},
    "tasks.sp.harvest_sport":              {"queue": "harvest"},
    "tasks.sp.harvest_all_upcoming":       {"queue": "harvest"},
    "tasks.sp.harvest_all_live":           {"queue": "live"},
    "tasks.sp.cross_bk_enrich":           {"queue": "harvest"},
    "tasks.sp.enrich_analytics":          {"queue": "harvest"},
    "tasks.sp.get_match_analytics":       {"queue": "harvest"},
    "tasks.sp.harvest_page":              {"queue": "harvest"},
    "tasks.sp.merge_pages":               {"queue": "results"},
    "tasks.sp.harvest_sport_paged":       {"queue": "harvest"},
    "tasks.sp.harvest_all_paged":         {"queue": "harvest"},
    "tasks.bt_od.harvest_sport":          {"queue": "harvest"},
    "tasks.bt_od.harvest_all":            {"queue": "harvest"},
    "tasks.bt.harvest_sport":              {"queue": "harvest"},
    "tasks.bt.harvest_page":              {"queue": "harvest"},
    "tasks.bt.merge_pages":               {"queue": "results"},
    "tasks.bt.harvest_sport_paged":       {"queue": "harvest"},
    "tasks.bt.harvest_all_paged":         {"queue": "harvest"},
    "tasks.bt.enrich_sport":              {"queue": "harvest"},
    "tasks.bt.harvest_all_upcoming":       {"queue": "harvest"},
    "tasks.bt.harvest_all_live":           {"queue": "live"},
    "tasks.od.harvest_sport":              {"queue": "harvest"},
    "tasks.od.harvest_date_chunk":        {"queue": "harvest"},
    "tasks.od.merge_pages":               {"queue": "results"},
    "tasks.od.harvest_sport_paged":       {"queue": "harvest"},
    "tasks.od.harvest_all_paged":         {"queue": "harvest"},
    "tasks.od.harvest_all_upcoming":       {"queue": "harvest"},
    "tasks.od.harvest_all_live":           {"queue": "live"},
    "tasks.b2b.harvest_sport":             {"queue": "harvest"},
    "tasks.b2b.harvest_all_upcoming":      {"queue": "harvest"},
    "tasks.b2b.harvest_all_live":          {"queue": "live"},
    "tasks.b2b_page.harvest_page":         {"queue": "harvest"},
    "tasks.b2b_page.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.b2b_page.harvest_all_live":     {"queue": "live"},
    "tasks.b2b.harvest_bk_sport":         {"queue": "harvest"},
    "tasks.b2b.merge_sport":              {"queue": "results"},
    "tasks.b2b.harvest_all_paged":        {"queue": "harvest"},
    "tasks.b2b.harvest_all_live_paged":   {"queue": "live"},
    "tasks.sbo.harvest_sport":             {"queue": "harvest"},
    "tasks.sbo.harvest_all_upcoming":      {"queue": "harvest"},
    "tasks.sbo.harvest_all_live":          {"queue": "live"},
    "tasks.sp.poll_all_event_details":     {"queue": "live"},
    "tasks.ops.compute_ev_arb":            {"queue": "ev_arb"},
    "tasks.ops.update_match_results":      {"queue": "results"},
    "tasks.ops.dispatch_notifications":    {"queue": "notify"},
    "tasks.ops.publish_ws_event":          {"queue": "notify"},
    "tasks.ops.health_check":              {"queue": "default"},
    "tasks.ops.healthcheck":               {"queue": "default"},
    "tasks.ops.expire_subscriptions":      {"queue": "default"},
    "tasks.ops.cache_finished_games":      {"queue": "results"},
    "tasks.ops.send_async_email":          {"queue": "notify"},
    "tasks.ops.send_message":              {"queue": "notify"},
    "tasks.ops.persist_combined_batch":    {"queue": "results"},
    "tasks.ops.persist_all_sports":        {"queue": "results"},
    "tasks.ops.build_health_report":       {"queue": "default"},
    "tasks.ops.publish_bk_snapshot":       {"queue": "harvest"},
    "tasks.align.sport":                   {"queue": "results"},
    "tasks.align.all_sports":             {"queue": "results"},
    "tasks.harvest.all_paged":            {"queue": "harvest"},
    "tasks.ops.beat.harvest_all_paged":   {"queue": "harvest"},
    "tasks.ops.beat.b2b_live":            {"queue": "harvest"},
    "tasks.ops.beat.alignment":           {"queue": "results"},
    "tasks.ops.beat.prune":               {"queue": "default"},
}

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


def make_celery(app=None):
    if app is None:
        app = create_app()
    init_celery(app)
    celery_service.conf.update(
        task_acks_late             = True,
        worker_prefetch_multiplier = 1,
        task_reject_on_worker_lost = True,
        task_default_queue         = "default",
        worker_max_tasks_per_child = 1000,
        task_serializer            = "json",
        result_serializer          = "json",
        accept_content             = ["json"],
        task_routes                = TASK_ROUTES,
        include=[
            "app.workers.tasks_ops",
            "app.workers.tasks_upcoming",
            "app.workers.tasks_live",
            "app.workers.tasks_market_align",
            "app.workers.tasks_bt_od",
            "app.workers.tasks_harvest_pages",
            "app.workers.tasks_harvest_b2b",
            "app.workers.tasks_align",
        ],
    )
    return celery_service


flask_app = create_app()
celery    = make_celery(flask_app)

_log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# REDIS HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _redis(db: int = 2):
    import redis as _r
    url  = celery.conf.broker_url or "redis://localhost:6379/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    return _r.Redis.from_url(
        f"{base}/{db}",
        decode_responses=False,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=True,
    )


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


# ══════════════════════════════════════════════════════════════════════════════
# SPORT NAME NORMALISATION
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# BOOKMAKER AUTO-CREATION
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# BETRADAR ID EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

def _extract_betradar_id(m: dict) -> str:
    raw = (
        m.get("betradar_id")  or m.get("betradarId")   or
        m.get("sr_id")        or m.get("bt_parent_id") or m.get("od_parent_id") or ""
    )
    val = str(raw).strip()
    return "" if val in ("0", "None", "null", "") else val


# ══════════════════════════════════════════════════════════════════════════════
# FUZZY MATCH LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════════════════════
# UPSERT PIPELINE  — FIXED _upsert_and_chain import
# ══════════════════════════════════════════════════════════════════════════════

def _upsert_and_chain(matches: list[dict], bk_name: str) -> None:
    """
    Write odds to DB and chain EV/arb computation.
    FIX: imports compute_ev_arb from tasks_ops (not a circular import because
    tasks_ops imports celery from celery_tasks, not _upsert_and_chain).
    """
    # Lazy import to avoid circular at module load time
    from app.workers.tasks_ops import compute_ev_arb  # ← FIXED

    try:
        bk_id = _get_or_create_bookmaker(bk_name)
        for m in matches:
            mid = _upsert_unified_match(_to_upsert_shape(m), bk_id, bk_name)
            if mid:
                compute_ev_arb.apply_async(
                    args=[mid], queue="ev_arb", countdown=1
                )
    except Exception as exc:
        _log.error("[upsert_and_chain] %s: %s", bk_name, exc)


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


def _parse_start_time(raw) -> "datetime | None":
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