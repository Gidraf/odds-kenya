"""
app/workers/celery_tasks.py  — v5 (unified + registry harvest)
===============================================================
Architecture
────────────
Two harvest pipelines run side by side:

  ┌─ REGISTRY (harvest_registry.py) ───────────────────────────────────────────┐
  │  harvest_bookmaker_sport(slug, sport)  → odds:upcoming:{bk}:{sport}         │
  │  harvest_all_upcoming()                → fan-out all enabled bk × sport     │
  │  merge_and_broadcast(sport)            → odds:upcoming:all:{sport}           │
  │  compute_value_bets(sport)             → ArbitrageOpportunity / EVOpportunity│
  └─────────────────────────────────────────────────────────────────────────────┘
  ┌─ LOCAL (Sportpesa / Betika / Odibets) ──────────────────────────────────────┐
  │  harvest_sp_sport      → sp:{mode}:{sport}                                  │
  │  harvest_bt_sport      → bt:{mode}:{sport}                                  │
  │  harvest_od_for_sport  → od:{mode}:{sport}                                  │
  └─────────────────────────────────────────────────────────────────────────────┘
  ┌─ B2B DIRECT (b2b_harvester merged) ────────────────────────────────────────┐
  │  harvest_b2b_sport  → b2b:{mode}:{sport}                                    │
  └─────────────────────────────────────────────────────────────────────────────┘
  ┌─ B2B PAGE FAN-OUT (bookmaker_fetcher, legacy) ──────────────────────────────┐
  │  harvest_b2b_page  → odds:{mode}:{sport}:{bkid}:p{n}                        │
  └─────────────────────────────────────────────────────────────────────────────┘
  ┌─ SBO aggregator ────────────────────────────────────────────────────────────┐
  │  harvest_sbo_sport  → sbo:upcoming:{sport}                                  │
  └─────────────────────────────────────────────────────────────────────────────┘

After every leaf harvest:
  compute_ev_arb_for_match (uses OpportunityDetector from odds_model.py)
    → dispatch_notifications
    → publish_ws_event

Beat schedule (all registered in setup_periodic_tasks):
  ── Registry pipeline ────────────────────────────────
  registry-upcoming-4h      harvest_all_registry_upcoming    4h
  registry-cleanup-daily    cleanup_old_snapshots            24h
  ── Local bookmakers ─────────────────────────────────
  sp-upcoming-5min          harvest_all_sp_upcoming         300s
  sp-live-60s               harvest_all_sp_live              60s
  bt-upcoming-6min          harvest_all_bt_upcoming         360s
  bt-live-90s               harvest_all_bt_live              90s
  od-upcoming-7min          harvest_all_od_upcoming         420s
  od-live-90s               harvest_all_od_live              90s
  ── B2B direct ───────────────────────────────────────
  b2b-upcoming-8min         harvest_all_b2b_upcoming        480s
  b2b-live-2min             harvest_all_b2b_live            120s
  ── B2B page fan-out (legacy) ────────────────────────
  b2b-page-5min             harvest_all_upcoming(B2B)       300s
  b2b-page-live-30s         harvest_all_live(B2B)            30s
  ── SBO ──────────────────────────────────────────────
  sbo-upcoming-3min         harvest_all_sbo_upcoming        180s
  sbo-live-60s              harvest_all_sbo_live             60s
  ── Ops ──────────────────────────────────────────────
  results-5min              update_match_results            300s
  cache-finished-h          cache_finished_games           3600s
  health-30s                health_check                     30s
  expire-subs-h             expire_subscriptions           3600s
"""

from __future__ import annotations

import base64
import json
import mimetypes
import os
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import requests
from celery import group, chord, chain
from celery.utils.log import get_task_logger
from flask_mail import Mail, Message

from app import create_app
from app.extensions import celery as celery_service, init_celery

logger = get_task_logger(__name__)

# ── Sport lists ───────────────────────────────────────────────────────────────
_LOCAL_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
]
_LIVE_SPORTS = ["soccer", "basketball", "tennis"]

_B2B_HARVEST_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
    "darts", "handball",
]

_B2B_SPORTS      = ["Football", "Basketball", "Tennis", "Ice Hockey",
                    "Volleyball", "Cricket", "Rugby", "Table Tennis"]
_B2B_LIVE_SPORTS = ["Football", "Basketball", "Ice Hockey", "Tennis"]

_SBO_SPORTS      = ["soccer", "basketball", "tennis", "ice-hockey",
                    "volleyball", "cricket", "rugby", "boxing",
                    "handball", "mma", "table-tennis"]
_SBO_LIVE_SPORTS = ["soccer", "basketball", "tennis"]

PAGE_SIZE   = 15
MAX_PAGES   = 6


# =============================================================================
# Bootstrap — FlaskTask MUST be set before any @celery.task decorator
# =============================================================================

def make_celery(app):
    """Bind global Celery instance to Flask app and configure queues."""
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
        task_routes = {
            # ── Registry pipeline ─────────────────────────────────────────────
            "harvest.bookmaker_sport":       {"queue": "harvest"},
            "harvest.all_upcoming":          {"queue": "harvest"},
            "harvest.merge_broadcast":       {"queue": "harvest"},
            "harvest.value_bets":            {"queue": "harvest"},
            "harvest.cleanup":               {"queue": "harvest"},
            # ── Local ─────────────────────────────────────────────────────────
            "app.workers.celery_tasks.harvest_sp_sport":         {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_sp_upcoming":  {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_sp_live":      {"queue": "live"},
            "app.workers.celery_tasks.harvest_bt_sport":         {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_bt_upcoming":  {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_bt_live":      {"queue": "live"},
            "app.workers.celery_tasks.harvest_od_for_sport":     {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_od_upcoming":  {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_od_live":      {"queue": "live"},
            # ── B2B direct ────────────────────────────────────────────────────
            "app.workers.celery_tasks.harvest_b2b_sport":        {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_b2b_upcoming": {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_b2b_live":     {"queue": "live"},
            # ── B2B page fan-out ──────────────────────────────────────────────
            "app.workers.celery_tasks.harvest_b2b_page":         {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_upcoming":     {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_live":         {"queue": "live"},
            # ── SBO ───────────────────────────────────────────────────────────
            "app.workers.celery_tasks.harvest_sbo_sport":        {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_sbo_upcoming": {"queue": "harvest"},
            "app.workers.celery_tasks.harvest_all_sbo_live":     {"queue": "live"},
            # ── Compute / notify / ops ────────────────────────────────────────
            "app.workers.celery_tasks.compute_ev_arb_for_match": {"queue": "ev_arb"},
            "app.workers.celery_tasks.update_match_results":     {"queue": "results"},
            "app.workers.celery_tasks.dispatch_notifications":   {"queue": "notify"},
            "app.workers.celery_tasks.publish_ws_event":         {"queue": "notify"},
            "app.workers.celery_tasks.health_check":             {"queue": "default"},
            "app.workers.celery_tasks.expire_subscriptions":     {"queue": "default"},
            "app.workers.celery_tasks.cache_finished_games":     {"queue": "results"},
            "app.workers.celery_tasks.send_async_email":         {"queue": "notify"},
            "app.workers.celery_tasks.send_message":             {"queue": "notify"},
        },
    )
    return celery_service


flask_app = create_app()
celery    = make_celery(flask_app)


# ── Beat schedule ─────────────────────────────────────────────────────────────
@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # ── Registry pipeline (new unified harvest) ───────────────────────────────
    sender.add_periodic_task(14400.0, harvest_all_registry_upcoming.s(),
                             name="registry-upcoming-4h")
    sender.add_periodic_task(86400.0, cleanup_old_snapshots_task.s(),
                             name="registry-cleanup-daily")
    # ── Local bookmakers ──────────────────────────────────────────────────────
    sender.add_periodic_task(300.0, harvest_all_sp_upcoming.s(),  name="sp-upcoming-5min")
    sender.add_periodic_task( 60.0, harvest_all_sp_live.s(),      name="sp-live-60s")
    sender.add_periodic_task(360.0, harvest_all_bt_upcoming.s(),  name="bt-upcoming-6min")
    sender.add_periodic_task( 90.0, harvest_all_bt_live.s(),      name="bt-live-90s")
    sender.add_periodic_task(420.0, harvest_all_od_upcoming.s(),  name="od-upcoming-7min")
    sender.add_periodic_task( 90.0, harvest_all_od_live.s(),      name="od-live-90s")
    # ── B2B direct ────────────────────────────────────────────────────────────
    sender.add_periodic_task(480.0, harvest_all_b2b_upcoming.s(), name="b2b-upcoming-8min")
    sender.add_periodic_task(120.0, harvest_all_b2b_live.s(),     name="b2b-live-2min")
    # ── B2B page fan-out (legacy) ─────────────────────────────────────────────
    sender.add_periodic_task(300.0, harvest_all_upcoming.s("upcoming"), name="b2b-page-5min")
    sender.add_periodic_task( 30.0, harvest_all_live.s(),               name="b2b-page-live-30s")
    # ── SBO ───────────────────────────────────────────────────────────────────
    sender.add_periodic_task(180.0, harvest_all_sbo_upcoming.s(), name="sbo-upcoming-3min")
    sender.add_periodic_task( 60.0, harvest_all_sbo_live.s(),     name="sbo-live-60s")
    # ── Ops ───────────────────────────────────────────────────────────────────
    sender.add_periodic_task(300.0,  update_match_results.s(),    name="results-5min")
    sender.add_periodic_task(3600.0, cache_finished_games.s(),    name="cache-finished-hourly")
    sender.add_periodic_task( 30.0,  health_check.s(),            name="health-30s")
    sender.add_periodic_task(3600.0, expire_subscriptions.s(),    name="expire-subs-hourly")

    logger.info("[beat] All periodic tasks registered")


# =============================================================================
# Redis helpers
# =============================================================================

def _redis():
    import redis as _r
    url  = celery.conf.broker_url or "redis://localhost:6379/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    return _r.Redis.from_url(
        f"{base}/2",
        decode_responses=False, socket_timeout=5,
        socket_connect_timeout=5, retry_on_timeout=True,
    )


def cache_set(key: str, data, ttl: int = 600) -> bool:
    try:
        _redis().set(key, json.dumps(data, default=str), ex=ttl)
        return True
    except Exception as e:
        logger.warning(f"[cache:set] {key}: {e}")
        return False


def cache_get(key: str):
    try:
        raw = _redis().get(key)
        return json.loads(raw) if raw else None
    except Exception as e:
        logger.warning(f"[cache:get] {key}: {e}")
        return None


def cache_delete(key: str) -> bool:
    try:
        _redis().delete(key)
        return True
    except Exception as e:
        logger.warning(f"[cache:del] {key}: {e}")
        return False


def cache_keys(pattern: str) -> list[str]:
    try:
        return [k.decode() if isinstance(k, bytes) else k
                for k in _redis().keys(pattern)]
    except Exception as e:
        logger.warning(f"[cache:keys] {pattern}: {e}")
        return []


def task_status_set(name: str, status: dict):
    cache_set(f"task_status:{name}", {
        **status, "updated_at": datetime.now(timezone.utc).isoformat(),
    }, ttl=300)


# =============================================================================
# Redis PubSub / Topics Emission
# =============================================================================

@celery.task(name="app.workers.celery_tasks.publish_ws_event",
             soft_time_limit=5, time_limit=10)
def publish_ws_event(channel: str, data: dict) -> bool:
    try:
        _redis().publish(channel, json.dumps(data, default=str))
        return True
    except Exception as e:
        logger.warning(f"[ws:publish] {channel}: {e}")
        return False


def _publish(channel: str, data: dict):
    publish_ws_event.apply_async(args=[channel, data], queue="notify")


def _emit(source: str, sport: str, mode: str, matches: list[dict], latency: int) -> None:
    """
    Topic-based emitter that publishes events dynamically depending on the bookmaker, sport,
    and individual markets present in the batch.
    """
    ts = _now_iso()
    
    # 1. Base Bookmaker & Sport Topic
    # Example: odds:sportpesa:soccer:upcoming
    base_topic = f"odds:{source}:{sport}:{mode}"
    _publish(base_topic, {
        "event": "odds_updated",
        "source": source,
        "sport": sport,
        "mode": mode,
        "count": len(matches),
        "latency_ms": latency,
        "ts": ts,
    })

    # 2. Extract which markets were updated and emit per-market topics
    # Example: odds:sportpesa:soccer:upcoming:1x2
    market_counts = {}
    for m in matches:
        markets = m.get("markets") or m.get("best_odds") or {}
        for mkt in markets.keys():
            market_counts[mkt] = market_counts.get(mkt, 0) + 1

    for mkt, count in market_counts.items():
        mkt_topic = f"odds:{source}:{sport}:{mode}:{mkt}"
        _publish(mkt_topic, {
            "event": "odds_market_updated",
            "source": source,
            "sport": sport,
            "mode": mode,
            "market": mkt,
            "count": count,
            "ts": ts
        })


# =============================================================================
# DB helpers
# =============================================================================

def _load_bookmakers() -> list[dict]:
    try:
        from app.models.bookmakers_model import Bookmaker
        bms    = Bookmaker.query.filter_by(is_active=True).all()
        result = []
        for bm in bms:
            vendor = getattr(bm, "vendor_slug", "betb2b") or "betb2b"
            if vendor in ("sbo", "sportpesa", "betika", "odibets"):
                continue
            cfg     = getattr(bm, "harvest_config", None) or {}
            if isinstance(cfg, str):
                try:    cfg = json.loads(cfg)
                except: cfg = {}
            params  = cfg.get("params", {})
            partner = params.get("partner", "")
            if not partner:
                from app.workers.bookmaker_fetcher import _B2B_DOMAIN_CREDS
                domain = (bm.domain or "").lower().lstrip("www.")
                creds  = _B2B_DOMAIN_CREDS.get(domain)
                if creds:
                    partner, gr = creds
                    cfg.setdefault("params", {})
                    cfg["params"]["partner"] = partner
                    if gr: cfg["params"]["gr"] = gr
                else:
                    continue
            result.append({"id": bm.id, "name": bm.name or bm.domain,
                           "domain": bm.domain, "vendor_slug": vendor, "config": cfg})
        return result
    except Exception as exc:
        logger.error(f"[db] load_bookmakers: {exc}")
        return []


def _bookmaker_id_map() -> dict[str, int]:
    try:
        from app.models.bookmakers_model import Bookmaker
        return {bm.name: bm.id for bm in Bookmaker.query.all()}
    except Exception:
        return {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _upsert_unified_match(match_data: dict, bookmaker_id: int | None,
                           bookmaker_name: str) -> int | None:
    """
    Upsert match + per-bookmaker odds into UnifiedMatch / BookmakerMatchOdds.
    Uses SELECT FOR UPDATE to prevent concurrent write races.
    Appends to BookmakerOddsHistory on every price change.
    """
    try:
        from app.extensions import db
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
        )

        parent_id = str(
            match_data.get("match_id") or match_data.get("betradar_id") or
            match_data.get("b2b_match_id") or ""
        )
        if not parent_id:
            return None

        home  = str(match_data.get("home_team")   or "")
        away  = str(match_data.get("away_team")   or "")
        sport = str(match_data.get("sport")       or "")
        comp  = str(match_data.get("competition") or "")

        start_time = None
        if st := match_data.get("start_time"):
            try:
                if isinstance(st, str):
                    start_time = datetime.fromisoformat(st.replace("Z", "+00:00"))
                elif isinstance(st, (int, float)):
                    start_time = datetime.utcfromtimestamp(float(st))
            except Exception:
                pass

        # ── SELECT FOR UPDATE — prevents concurrent duplicate inserts ─────────
        um = (UnifiedMatch.query
              .filter_by(parent_match_id=parent_id)
              .with_for_update()
              .first())
        if not um:
            um = UnifiedMatch(
                parent_match_id=parent_id, home_team_name=home,
                away_team_name=away, sport_name=sport,
                competition_name=comp, start_time=start_time,
            )
            db.session.add(um)
            db.session.flush()
        else:
            if home  and home  != um.home_team_name:  um.home_team_name   = home
            if away  and away  != um.away_team_name:  um.away_team_name   = away
            if sport and not um.sport_name:            um.sport_name       = sport
            if comp  and not um.competition_name:      um.competition_name = comp
            if start_time and not um.start_time:       um.start_time       = start_time

        if bookmaker_id:
            bmo = (BookmakerMatchOdds.query
                   .filter_by(match_id=um.id, bookmaker_id=bookmaker_id)
                   .with_for_update()
                   .first())
            if not bmo:
                bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bookmaker_id)
                db.session.add(bmo)
                db.session.flush()

            history_batch: list[dict] = []
            markets = match_data.get("markets") or match_data.get("best_odds") or {}

            for mkt_key, outcomes in markets.items():
                for outcome, odds_data in outcomes.items():
                    if isinstance(odds_data, (int, float)):
                        price = float(odds_data)
                    elif isinstance(odds_data, dict):
                        price = float(odds_data.get("odds") or odds_data.get("odd") or 0)
                    else:
                        continue
                    if price <= 1.0:
                        continue

                    price_changed, old_price = bmo.upsert_selection(
                        market=mkt_key, specifier=None,
                        selection=outcome, price=price,
                    )
                    um.upsert_bookmaker_price(
                        market=mkt_key, specifier=None, selection=outcome,
                        price=price, bookmaker_id=bookmaker_id,
                    )

                    if price_changed:
                        history_batch.append({
                            "bmo_id":      bmo.id,
                            "bookmaker_id": bookmaker_id,
                            "match_id":     um.id,
                            "market":       mkt_key,
                            "specifier":    None,
                            "selection":    outcome,
                            "old_price":    old_price,
                            "new_price":    price,
                            "price_delta":  round(price - old_price, 4) if old_price else None,
                            "recorded_at":  datetime.utcnow(),
                        })

            if history_batch:
                BookmakerOddsHistory.bulk_append(history_batch)

        db.session.commit()
        return um.id

    except Exception as exc:
        logger.error(f"[upsert] {match_data.get('match_id')}: {exc}")
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        return None


def _upsert_sbo_match(sbo_match: dict, bk_name_to_id: dict[str, int]) -> int | None:
    try:
        from app.extensions import db
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
        )

        parent_id = str(sbo_match.get("betradar_id") or "")
        if not parent_id:
            return None

        home  = str(sbo_match.get("home_team")   or "")
        away  = str(sbo_match.get("away_team")   or "")
        sport = str(sbo_match.get("sport")       or "")
        comp  = str(sbo_match.get("competition") or "")

        start_time = None
        if st := sbo_match.get("start_time"):
            try:
                if isinstance(st, str):
                    start_time = datetime.fromisoformat(st.replace("Z", "+00:00"))
                elif isinstance(st, (int, float)):
                    start_time = datetime.utcfromtimestamp(float(st))
            except Exception:
                pass

        um = (UnifiedMatch.query
              .filter_by(parent_match_id=parent_id)
              .with_for_update()
              .first())
        if not um:
            um = UnifiedMatch(
                parent_match_id=parent_id, home_team_name=home,
                away_team_name=away, sport_name=sport,
                competition_name=comp, start_time=start_time,
            )
            db.session.add(um)
            db.session.flush()
        else:
            if home  and home  != um.home_team_name:  um.home_team_name   = home
            if away  and away  != um.away_team_name:  um.away_team_name   = away
            if sport and not um.sport_name:            um.sport_name       = sport
            if comp  and not um.competition_name:      um.competition_name = comp
            if start_time and not um.start_time:       um.start_time       = start_time

        unified = sbo_match.get("unified_markets") or {}
        history_batch: list[dict] = []

        for mkt_key, outcome_map in unified.items():
            for outcome, entries in outcome_map.items():
                for entry in entries:
                    bookie = entry.get("bookie", "")
                    price  = float(entry.get("odd", 0))
                    bk_id  = bk_name_to_id.get(bookie)
                    if not bk_id or price <= 1.0:
                        continue

                    bmo = (BookmakerMatchOdds.query
                           .filter_by(match_id=um.id, bookmaker_id=bk_id)
                           .with_for_update()
                           .first())
                    if not bmo:
                        bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bk_id)
                        db.session.add(bmo)
                        db.session.flush()

                    price_changed, old_price = bmo.upsert_selection(
                        market=mkt_key, specifier=None,
                        selection=outcome, price=price,
                    )
                    um.upsert_bookmaker_price(
                        market=mkt_key, specifier=None, selection=outcome,
                        price=price, bookmaker_id=bk_id,
                    )
                    if price_changed:
                        history_batch.append({
                            "bmo_id": bmo.id, "bookmaker_id": bk_id,
                            "match_id": um.id, "market": mkt_key,
                            "specifier": None, "selection": outcome,
                            "old_price": old_price, "new_price": price,
                            "price_delta": round(price - old_price, 4) if old_price else None,
                            "recorded_at": datetime.utcnow(),
                        })

        if history_batch:
            BookmakerOddsHistory.bulk_append(history_batch)

        db.session.commit()
        return um.id

    except Exception as exc:
        logger.error(f"[upsert_sbo] {sbo_match.get('betradar_id')}: {exc}")
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        return None


# =============================================================================
# Shared harvest helpers
# =============================================================================

def _to_upsert_shape(m: dict) -> dict:
    return {
        "match_id":    (m.get("betradar_id") or m.get("b2b_match_id") or
                        m.get("sp_game_id")  or m.get("od_match_id")  or
                        m.get("bt_match_id") or ""),
        "betradar_id": m.get("betradar_id") or "",
        "home_team":   m.get("home_team", ""),
        "away_team":   m.get("away_team", ""),
        "sport":       m.get("sport", ""),
        "competition": m.get("competition", ""),
        "start_time":  m.get("start_time"),
        "markets":     m.get("markets") or m.get("best_odds") or {},
        "bookmakers":  m.get("bookmakers") or {},
    }


def _upsert_and_chain(matches: list[dict], bk_name: str) -> None:
    try:
        from app.models.bookmakers_model import Bookmaker
        bm    = Bookmaker.query.filter(Bookmaker.name.ilike(f"%{bk_name}%")).first()
        bk_id = bm.id if bm else None
        for m in matches:
            mid = _upsert_unified_match(_to_upsert_shape(m), bk_id, bk_name)
            if mid:
                chain(
                    compute_ev_arb_for_match.si(mid),
                    dispatch_notifications.si(mid, "arb"),
                ).apply_async(queue="ev_arb", countdown=1)
    except Exception as exc:
        logger.error(f"[harvest] upsert {bk_name}: {exc}")


# =============================================================================
# ════════════════════════════════════════════════════════════════════════════
# REGISTRY PIPELINE  (new unified harvest — harvest_registry.py)
# ════════════════════════════════════════════════════════════════════════════
# =============================================================================

@celery.task(
    name="harvest.bookmaker_sport",
    bind=True, max_retries=2, default_retry_delay=60,
    soft_time_limit=300, time_limit=360, acks_late=True,
    queue="harvest",
)
def harvest_bookmaker_sport_task(self, bookmaker_slug: str, sport_slug: str) -> dict:
    from app.workers.harvest_registry import get_bookmaker
    from app.models.odds_model import BookmakerOddsHistory

    bk = get_bookmaker(bookmaker_slug)
    if not bk or not bk["enabled"]:
        return {"ok": False, "skipped": True}
    if sport_slug not in bk["sports"]:
        return {"ok": True, "skipped": True}

    t0 = time.perf_counter()

    try:
        matches: list[dict] = bk["fetch_fn"](sport_slug)
    except Exception as exc:
        raise self.retry(exc=exc)

    latency_ms = int((time.perf_counter() - t0) * 1000)

    # Write to Redis
    cache_set(
        f"odds:upcoming:{bookmaker_slug}:{sport_slug}",
        {
            "bookmaker": bookmaker_slug, "sport": sport_slug,
            "match_count": len(matches), "harvested_at": _now_iso(),
            "latency_ms": latency_ms, "matches": matches,
        },
        ttl=bk["redis_ttl"],
    )

    # Write to Postgres via shared upsert
    _upsert_and_chain(matches, bk["label"])

    # Delegate market-specific routing dynamically
    _emit(bookmaker_slug, sport_slug, "upcoming", matches, latency_ms)

    logger.info("[registry] %s/%s → %d matches %dms",
                bookmaker_slug, sport_slug, len(matches), latency_ms)
    return {"ok": True, "bookmaker": bookmaker_slug, "sport": sport_slug,
            "count": len(matches), "latency_ms": latency_ms}


@celery.task(
    name="harvest.all_upcoming",
    soft_time_limit=30, time_limit=60, queue="harvest",
)
def harvest_all_registry_upcoming() -> dict:
    from app.workers.harvest_registry import ENABLED_BOOKMAKERS

    sigs = [
        harvest_bookmaker_sport_task.s(bk["slug"], sport)
        for bk in ENABLED_BOOKMAKERS
        for sport in bk["sports"]
    ]
    group(sigs).apply_async(queue="harvest")
    logger.info("[registry] dispatched %d harvest tasks", len(sigs))
    return {"dispatched": len(sigs)}


@celery.task(
    name="harvest.merge_broadcast",
    soft_time_limit=30, time_limit=60, queue="harvest",
)
def merge_and_broadcast_task(sport_slug: str) -> dict:
    from app.workers.harvest_registry import ENABLED_BOOKMAKERS

    bk_slugs = [bk["slug"] for bk in ENABLED_BOOKMAKERS
                if sport_slug in bk["sports"]]
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
                    "betradar_id":  br_id,
                    "home_team":    match.get("home_team", ""),
                    "away_team":    match.get("away_team", ""),
                    "competition":  match.get("competition", ""),
                    "start_time":   match.get("start_time"),
                    "sport":        sport_slug,
                    "event_ids":    {},
                    "markets":      {},
                }

            entry = merged[key]
            entry["event_ids"][slug] = str(
                match.get("sp_game_id") or match.get("event_id") or ""
            )

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

    # Emit overarching 'merged' state to dynamic specific topic
    _publish(f"odds:merged:{sport_slug}:upcoming", {
        "type": "odds_merged", "sport": sport_slug,
        "bookmakers": seen_bk, "count": len(merged_list), "ts": _now_iso(),
    })

    # Fire value bet detection
    compute_value_bets_task.apply_async(args=[sport_slug], queue="ev_arb")

    logger.info("[merge] %s: %d events from %d bookmakers",
                sport_slug, len(merged_list), len(seen_bk))
    return {"ok": True, "sport": sport_slug, "events": len(merged_list),
            "bookmakers": seen_bk}


@celery.task(
    name="harvest.value_bets",
    soft_time_limit=60, time_limit=90, queue="ev_arb",
)
def compute_value_bets_task(sport_slug: str) -> dict:
    import os
    from decimal import Decimal
    from app.extensions import db
    from app.models.odds_model import ArbitrageOpportunity, EVOpportunity, OpportunityStatus

    threshold = float(os.getenv("VALUE_BET_THRESHOLD", "5.0"))
    raw = cache_get(f"odds:upcoming:all:{sport_slug}")
    if not raw:
        return {"ok": True, "found": 0}

    matches  = raw.get("matches") or []
    arb_rows = 0
    ev_rows  = 0
    now      = datetime.utcnow()

    try:
        for match in matches:
            for mkt_slug, outcomes in (match.get("markets") or {}).items():
                # ── Arbitrage ─────────────────────────────────────────────────
                best_prices = {}
                leg_details = {}
                for out_key, bk_odds in outcomes.items():
                    if not bk_odds:
                        continue
                    best_bk  = max(bk_odds, key=lambda b: float(bk_odds[b]))
                    best_odd = float(bk_odds[best_bk])
                    if best_odd > 1.0:
                        best_prices[out_key]  = best_odd
                        leg_details[out_key]  = (best_bk, best_odd)

                if len(best_prices) >= 2:
                    arb_sum = sum(1.0 / p for p in best_prices.values())
                    if arb_sum < 1.0:
                        profit_pct = (1.0 / arb_sum - 1.0) * 100
                        if profit_pct >= 0.5:
                            legs = [
                                {
                                    "selection":  sel,
                                    "bookmaker":  leg_details[sel][0],
                                    "price":      leg_details[sel][1],
                                    "stake_pct":  round((1.0 / leg_details[sel][1]) / arb_sum * 100, 2),
                                }
                                for sel in best_prices
                            ]
                            start_dt = None
                            if match.get("start_time"):
                                try:
                                    start_dt = datetime.fromisoformat(
                                        str(match["start_time"]).replace("Z", "+00:00"))
                                except Exception:
                                    pass
                            db.session.add(ArbitrageOpportunity(
                                home_team        = match.get("home_team", ""),
                                away_team        = match.get("away_team", ""),
                                sport            = sport_slug,
                                competition      = match.get("competition", ""),
                                match_start      = start_dt,
                                market           = mkt_slug,
                                profit_pct       = round(profit_pct, 4),
                                peak_profit_pct  = round(profit_pct, 4),
                                arb_sum          = round(arb_sum, 6),
                                legs_json        = legs,
                                stake_100_returns = round(100 / arb_sum, 2),
                                bookmaker_ids    = sorted({l["bookmaker"] for l in legs}),
                                status           = OpportunityStatus.OPEN,
                                open_at          = now,
                            ))
                            arb_rows += 1
                            
                            # Market specific Arb Topic -> arb:{sport}:{market}
                            _publish(f"arb:{sport_slug}:{mkt_slug}", {
                                "event": "arb_found", "sport": sport_slug,
                                "match": f"{match.get('home_team')} v {match.get('away_team')}",
                                "market": mkt_slug, "profit_pct": round(profit_pct, 2),
                                "legs": legs, "ts": _now_iso(),
                            })

                # ── EV ────────────────────────────────────────────────────────
                if len(best_prices) >= 2:
                    inv_sum = sum(1.0 / p for p in best_prices.values())
                    if inv_sum <= 0:
                        continue
                    fair_probs = {sel: (1.0 / p) / inv_sum
                                  for sel, p in best_prices.items()}
                    for out_key, bk_odds in outcomes.items():
                        fair_p = fair_probs.get(out_key)
                        if not fair_p:
                            continue
                        for bk_name, bk_price in (bk_odds or {}).items():
                            bk_price = float(bk_price)
                            if bk_price <= 1.0:
                                continue
                            ev_pct = (bk_price * fair_p - 1.0) * 100
                            if ev_pct < threshold:
                                continue
                            b = bk_price - 1
                            kelly = max(0.0, (b * fair_p - (1 - fair_p)) / b) if b > 0 else 0.0
                            db.session.add(EVOpportunity(
                                home_team               = match.get("home_team", ""),
                                away_team               = match.get("away_team", ""),
                                sport                   = sport_slug,
                                competition             = match.get("competition", ""),
                                market                  = mkt_slug,
                                selection               = out_key,
                                bookmaker               = bk_name,
                                offered_price           = bk_price,
                                consensus_price         = round(best_prices.get(out_key, 0), 4),
                                fair_prob               = round(fair_p, 6),
                                ev_pct                  = round(ev_pct, 4),
                                peak_ev_pct             = round(ev_pct, 4),
                                bookmakers_in_consensus = len(bk_odds),
                                kelly_fraction          = round(kelly, 4),
                                half_kelly              = round(kelly / 2, 4),
                                status                  = OpportunityStatus.OPEN,
                                open_at                 = now,
                            ))
                            ev_rows += 1
                            
                            # Market specific EV Topic -> ev:{sport}:{market}
                            _publish(f"ev:{sport_slug}:{mkt_slug}", {
                                "event": "ev_found", "sport": sport_slug,
                                "match": f"{match.get('home_team')} v {match.get('away_team')}",
                                "market": mkt_slug, "selection": out_key, "bookmaker": bk_name,
                                "ev_pct": round(ev_pct, 2), "ts": _now_iso(),
                            })

        db.session.commit()

    except Exception as exc:
        logger.error("[value_bets] %s: %s", sport_slug, exc)
        try:
            db.session.rollback()
        except Exception:
            pass

    logger.info("[value_bets] %s: %d arbs  %d EVs", sport_slug, arb_rows, ev_rows)
    return {"ok": True, "sport": sport_slug, "arbs": arb_rows, "evs": ev_rows}


@celery.task(
    name="harvest.cleanup",
    soft_time_limit=60, time_limit=90, queue="harvest",
)
def cleanup_old_snapshots_task(days_keep: int = 7) -> dict:
    from app.extensions import db
    from app.models.odds_model import ArbitrageOpportunity, EVOpportunity

    cutoff  = datetime.utcnow() - timedelta(days=days_keep)
    n_arbs  = ArbitrageOpportunity.query.filter(
        ArbitrageOpportunity.open_at < cutoff).delete()
    n_evs   = EVOpportunity.query.filter(
        EVOpportunity.open_at < cutoff).delete()
    db.session.commit()

    logger.info("[cleanup] %d arbs  %d evs deleted (cutoff=%s)", n_arbs, n_evs, cutoff.date())
    return {"ok": True, "arbs_deleted": n_arbs, "evs_deleted": n_evs}


# =============================================================================
# SPORTPESA harvest tasks
# =============================================================================

@celery.task(
    bind=True, name="app.workers.celery_tasks.harvest_sp_sport",
    max_retries=2, default_retry_delay=20,
    soft_time_limit=180, time_limit=210, acks_late=True,
)
def harvest_sp_sport(self, sport_slug: str, mode: str = "upcoming",
                     max_matches: int | None = None) -> dict:
    t0 = time.perf_counter()
    try:
        from app.workers.sp_harvester import fetch_upcoming, fetch_live
        matches = (fetch_live(sport_slug, fetch_full_markets=True)
                   if mode == "live"
                   else fetch_upcoming(sport_slug, fetch_full_markets=True,
                                       max_matches=max_matches))
    except Exception as exc:
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"sp:{mode}:{sport_slug}", {
        "source": "sportpesa", "sport": sport_slug, "mode": mode,
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=60 if mode == "live" else 300)
    _upsert_and_chain(matches, "Sportpesa")
    _emit("sportpesa", sport_slug, mode, matches, latency)
    return {"ok": True, "source": "sportpesa", "sport": sport_slug,
            "mode": mode, "count": len(matches), "latency_ms": latency}


@celery.task(name="app.workers.celery_tasks.harvest_all_sp_upcoming",
             soft_time_limit=30, time_limit=60)
def harvest_all_sp_upcoming() -> dict:
    sigs = [harvest_sp_sport.s(s, "upcoming") for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


@celery.task(name="app.workers.celery_tasks.harvest_all_sp_live",
             soft_time_limit=30, time_limit=60)
def harvest_all_sp_live() -> dict:
    sigs = [harvest_sp_sport.s(s, "live") for s in _LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}


# =============================================================================
# BETIKA harvest tasks
# =============================================================================

@celery.task(
    bind=True, name="app.workers.celery_tasks.harvest_bt_sport",
    max_retries=2, default_retry_delay=20,
    soft_time_limit=180, time_limit=210, acks_late=True,
)
def harvest_bt_sport(self, sport_slug: str, mode: str = "upcoming",
                     max_matches: int | None = None) -> dict:
    t0 = time.perf_counter()
    try:
        from app.workers.bt_harvester import fetch_upcoming, fetch_live
        matches = (fetch_live(sport_slug, fetch_full_markets=True)
                   if mode == "live"
                   else fetch_upcoming(sport_slug, fetch_full_markets=True,
                                       max_matches=max_matches))
    except Exception as exc:
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"bt:{mode}:{sport_slug}", {
        "source": "betika", "sport": sport_slug, "mode": mode,
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=60 if mode == "live" else 300)
    _upsert_and_chain(matches, "Betika")
    _emit("betika", sport_slug, mode, matches, latency)
    return {"ok": True, "source": "betika", "sport": sport_slug,
            "mode": mode, "count": len(matches), "latency_ms": latency}


@celery.task(name="app.workers.celery_tasks.harvest_all_bt_upcoming",
             soft_time_limit=30, time_limit=60)
def harvest_all_bt_upcoming() -> dict:
    sigs = [harvest_bt_sport.s(s, "upcoming") for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


@celery.task(name="app.workers.celery_tasks.harvest_all_bt_live",
             soft_time_limit=30, time_limit=60)
def harvest_all_bt_live() -> dict:
    sigs = [harvest_bt_sport.s(s, "live") for s in _LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}


# =============================================================================
# ODIBETS harvest tasks
# =============================================================================

@celery.task(
    bind=True, name="app.workers.celery_tasks.harvest_od_for_sport",
    max_retries=1, default_retry_delay=30,
    soft_time_limit=300, time_limit=330, acks_late=True,
)
def harvest_od_for_sport(self, sport_slug: str, mode: str = "upcoming",
                          max_matches: int | None = None) -> dict:
    t0 = time.perf_counter()
    try:
        from app.workers.od_harvester import fetch_upcoming, fetch_live
        matches = (fetch_live(sport_slug, fetch_full_markets=True)
                   if mode == "live"
                   else fetch_upcoming(sport_slug, fetch_full_markets=True,
                                       fetch_extended=True, max_matches=max_matches))
    except Exception as exc:
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"od:{mode}:{sport_slug}", {
        "source": "odibets", "sport": sport_slug, "mode": mode,
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=60 if mode == "live" else 300)
    _upsert_and_chain(matches, "Odibets")
    _emit("odibets", sport_slug, mode, matches, latency)
    return {"ok": True, "source": "odibets", "sport": sport_slug,
            "mode": mode, "count": len(matches), "latency_ms": latency}


@celery.task(name="app.workers.celery_tasks.harvest_all_od_upcoming",
             soft_time_limit=30, time_limit=60)
def harvest_all_od_upcoming() -> dict:
    sigs = [harvest_od_for_sport.s(s, "upcoming") for s in _LOCAL_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


@celery.task(name="app.workers.celery_tasks.harvest_all_od_live",
             soft_time_limit=30, time_limit=60)
def harvest_all_od_live() -> dict:
    sigs = [harvest_od_for_sport.s(s, "live") for s in _LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}


# =============================================================================
# B2B DIRECT harvest tasks
# =============================================================================

@celery.task(
    bind=True, name="app.workers.celery_tasks.harvest_b2b_sport",
    max_retries=2, default_retry_delay=30,
    soft_time_limit=300, time_limit=360, acks_late=True,
)
def harvest_b2b_sport(self, sport_slug: str, mode: str = "upcoming") -> dict:
    t0 = time.perf_counter()
    try:
        from app.workers.b2b_harvester import fetch_b2b_sport
        matches = fetch_b2b_sport(sport_slug, mode=mode)
    except Exception as exc:
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"b2b:{mode}:{sport_slug}", {
        "source": "b2b", "sport": sport_slug, "mode": mode,
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=60 if mode == "live" else 300)

    for m in matches:
        for bk_name, bk_data in (m.get("bookmakers") or {}).items():
            bk_match = dict(m)
            bk_match["markets"] = bk_data.get("markets") or {}
            _upsert_and_chain([bk_match], bk_name)

    _emit("b2b", sport_slug, mode, matches, latency)
    return {"ok": True, "source": "b2b", "sport": sport_slug,
            "mode": mode, "count": len(matches), "latency_ms": latency}


@celery.task(name="app.workers.celery_tasks.harvest_all_b2b_upcoming",
             soft_time_limit=30, time_limit=60)
def harvest_all_b2b_upcoming() -> dict:
    sigs = [harvest_b2b_sport.s(s, "upcoming") for s in _B2B_HARVEST_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


@celery.task(name="app.workers.celery_tasks.harvest_all_b2b_live",
             soft_time_limit=30, time_limit=60)
def harvest_all_b2b_live() -> dict:
    sigs = [harvest_b2b_sport.s(s, "live") for s in _LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}


# =============================================================================
# B2B PAGE FAN-OUT (legacy)
# =============================================================================

@celery.task(
    bind=True, name="app.workers.celery_tasks.harvest_b2b_page",
    max_retries=2, default_retry_delay=15,
    soft_time_limit=45, time_limit=60, acks_late=True,
)
def harvest_b2b_page(self, bookmaker: dict, sport: str, mode: str, page: int) -> dict:
    t0      = time.perf_counter()
    bk_name = bookmaker.get("name") or bookmaker.get("domain", "?")
    bk_id   = bookmaker.get("id")
    try:
        from app.workers.bookmaker_fetcher import fetch_bookmaker
        matches = fetch_bookmaker(bookmaker, sport_name=sport, mode=mode,
                                  page=page, page_size=PAGE_SIZE, timeout=20)
    except Exception as exc:
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)
    cache_key = f"odds:{mode}:{sport.lower().replace(' ','_')}:{bk_id}:p{page}"
    cache_set(cache_key, {
        "bookmaker_id": bk_id, "bookmaker_name": bk_name,
        "sport": sport, "mode": mode, "page": page,
        "match_count": len(matches), "harvested_at": _now_iso(),
        "latency_ms": latency, "matches": matches,
    }, ttl=60 if mode == "live" else 360)

    match_ids: list[int] = []
    for m in matches:
        mid = _upsert_unified_match(m, bk_id, bk_name)
        if mid:
            match_ids.append(mid)
            chain(
                compute_ev_arb_for_match.si(mid),
                dispatch_notifications.si(mid, "arb"),
            ).apply_async(queue="ev_arb", countdown=1)

    bk_slug = bk_name.lower().replace(' ', '_')
    _emit(bk_slug, sport.lower().replace(' ', '_'), mode, matches, latency)
    return {"ok": True, "count": len(matches), "latency_ms": latency, "db_ids": match_ids}


@celery.task(name="app.workers.celery_tasks.harvest_all_upcoming",
             soft_time_limit=30, time_limit=60)
def harvest_all_upcoming(mode: str = "upcoming") -> dict:
    bookmakers = _load_bookmakers()
    if not bookmakers:
        task_status_set("beat_upcoming", {"state": "error", "error": "No bookmakers"})
        return {"dispatched": 0}
    sigs = [harvest_b2b_page.s(bm, sport, mode, page)
            for bm in bookmakers
            for sport in _B2B_SPORTS
            for page in range(1, MAX_PAGES + 1)]
    group(sigs).apply_async(queue="harvest")
    task_status_set("beat_upcoming", {"state": "ok", "dispatched": len(sigs),
                                       "bookmakers": len(bookmakers)})
    return {"dispatched": len(sigs)}


@celery.task(name="app.workers.celery_tasks.harvest_all_live",
             soft_time_limit=30, time_limit=60)
def harvest_all_live() -> dict:
    bookmakers = _load_bookmakers()
    sigs = [harvest_b2b_page.s(bm, sport, "live", 1)
            for bm in bookmakers for sport in _B2B_LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    task_status_set("beat_live", {"state": "ok", "dispatched": len(sigs)})
    return {"dispatched": len(sigs)}


# =============================================================================
# SBO harvest
# =============================================================================

@celery.task(
    bind=True, name="app.workers.celery_tasks.harvest_sbo_sport",
    max_retries=1, default_retry_delay=30,
    soft_time_limit=120, time_limit=150, acks_late=True,
)
def harvest_sbo_sport(self, sport_slug: str, max_matches: int = 90) -> dict:
    t0 = time.perf_counter()
    try:
        from app.views.sbo.sbo_fetcher import OddsAggregator, SPORT_CONFIG
        cfg = next((c for c in SPORT_CONFIG if c["sport"] == sport_slug), None)
        if not cfg:
            return {"ok": False, "error": f"Unknown sport: {sport_slug}"}
        agg     = OddsAggregator(cfg, fetch_full_sp_markets=True,
                                 fetch_full_bt_markets=True, fetch_od_markets=True)
        matches = agg.run(max_matches=max_matches)
    except Exception as exc:
        raise self.retry(exc=exc)

    latency = int((time.perf_counter() - t0) * 1000)
    cache_set(f"sbo:upcoming:{sport_slug}", {
        "sport": sport_slug, "match_count": len(matches),
        "harvested_at": _now_iso(), "latency_ms": latency, "matches": matches,
    }, ttl=180)

    bk_name_to_id = _bookmaker_id_map()
    match_ids: list[int] = []
    for m in matches:
        mid = _upsert_sbo_match(m, bk_name_to_id)
        if mid:
            match_ids.append(mid)
            chain(
                compute_ev_arb_for_match.si(mid),
                dispatch_notifications.si(mid, "arb"),
            ).apply_async(queue="ev_arb", countdown=2)

    arb_count = sum(1 for m in matches if m.get("arbitrage"))
    _emit("sbo", sport_slug, "upcoming", matches, latency)
    
    if arb_count:
        # Generic fallback topic for sbo arbs since `compute_ev_arb_for_match` will do the detailed routing
        _publish(f"arb:sbo:{sport_slug}", {"event": "arb_found", "sport": sport_slug,
                                "arb_count": arb_count, "ts": _now_iso()})
        
    return {"ok": True, "count": len(matches), "arb_count": arb_count,
            "latency_ms": latency}


@celery.task(name="app.workers.celery_tasks.harvest_all_sbo_upcoming",
             soft_time_limit=30, time_limit=60)
def harvest_all_sbo_upcoming() -> dict:
    sigs = [harvest_sbo_sport.s(sport, 90) for sport in _SBO_SPORTS]
    group(sigs).apply_async(queue="harvest")
    return {"dispatched": len(sigs)}


@celery.task(name="app.workers.celery_tasks.harvest_all_sbo_live",
             soft_time_limit=30, time_limit=60)
def harvest_all_sbo_live() -> dict:
    sigs = [harvest_sbo_sport.s(sport, 30) for sport in _SBO_LIVE_SPORTS]
    group(sigs).apply_async(queue="live")
    return {"dispatched": len(sigs)}


# =============================================================================
# EV / Arbitrage computation (per-match, from unified markets_json)
# =============================================================================

@celery.task(
    bind=True, name="app.workers.celery_tasks.compute_ev_arb_for_match",
    max_retries=1, soft_time_limit=20, time_limit=30, acks_late=True,
)
def compute_ev_arb_for_match(self, match_id: int) -> dict:
    """
    Uses OpportunityDetector from odds_model.py to scan markets_json
    and write ArbitrageOpportunity / EVOpportunity rows.
    """
    try:
        from app.models.odds_model import UnifiedMatch, OpportunityDetector
        from app.models.odds_model import ArbitrageOpportunity, EVOpportunity
        from app.extensions import db

        um = UnifiedMatch.query.get(match_id)
        if not um or not um.markets_json:
            return {"ok": False, "reason": "no markets"}

        detector = OpportunityDetector(min_profit_pct=0.5, min_ev_pct=3.0)
        arbs     = detector.find_arbs(um)
        evs      = detector.find_ev(um)

        for kwargs in arbs:
            db.session.add(ArbitrageOpportunity(**kwargs))
        for kwargs in evs:
            db.session.add(EVOpportunity(**kwargs))

        db.session.commit()

        # Route dynamically calculated Arbitrages per market
        if arbs:
            arbs_by_mkt = {}
            for a in arbs:
                m = a.get("market") or "unknown"
                arbs_by_mkt.setdefault(m, []).append(a)
            for m, mkt_arbs in arbs_by_mkt.items():
                _publish(f"arb:{um.sport_name}:{m}", {
                    "event": "arb_updated", "match_id": match_id,
                    "match": f"{um.home_team_name} v {um.away_team_name}",
                    "sport": um.sport_name, "market": m,
                    "arbs": len(mkt_arbs), "ts": _now_iso(),
                })

        # Route dynamically calculated EV per market
        if evs:
            evs_by_mkt = {}
            for e in evs:
                m = e.get("market") or "unknown"
                evs_by_mkt.setdefault(m, []).append(e)
            for m, mkt_evs in evs_by_mkt.items():
                _publish(f"ev:{um.sport_name}:{m}", {
                    "event": "ev_updated", "match_id": match_id,
                    "match": f"{um.home_team_name} v {um.away_team_name}",
                    "sport": um.sport_name, "market": m,
                    "evs": len(mkt_evs), "ts": _now_iso(),
                })

        return {"ok": True, "match_id": match_id,
                "arbs": len(arbs), "evs": len(evs)}

    except Exception as exc:
        logger.error(f"[ev_arb] match {match_id}: {exc}")
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        raise self.retry(exc=exc)


# =============================================================================
# Match results — every 5 min
# =============================================================================

@celery.task(name="app.workers.celery_tasks.update_match_results",
             soft_time_limit=120, time_limit=150)
def update_match_results() -> dict:
    try:
        from app.extensions import db
        from app.models.odds_model import UnifiedMatch

        now     = datetime.now(timezone.utc)
        updated = 0

        for um in UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now,
            UnifiedMatch.start_time >= now - timedelta(hours=3),
            UnifiedMatch.status.in_(["PRE_MATCH", "IN_PLAY"]),
        ).all():
            if um.status == "PRE_MATCH":
                um.status = "IN_PLAY"
                updated  += 1

        for um in UnifiedMatch.query.filter(
            UnifiedMatch.start_time <= now - timedelta(hours=2, minutes=30),
            UnifiedMatch.status == "IN_PLAY",
        ).all():
            um.status = "FINISHED"
            updated  += 1
            _settle_bankroll_bets(um.id)
            date_str = (um.start_time.strftime("%Y-%m-%d")
                        if um.start_time else now.strftime("%Y-%m-%d"))
            ck       = f"results:finished:{date_str}"
            cached   = cache_get(ck) or []
            cached.append(um.to_dict())
            cache_set(ck, cached, ttl=30 * 86400)

        if updated:
            db.session.commit()
            _publish("match:results", {"event": "results_updated",
                                   "updated": updated, "ts": _now_iso()})
        return {"updated": updated}

    except Exception as exc:
        logger.error(f"[results] {exc}")
        return {"error": str(exc)}


def _settle_bankroll_bets(match_id: int) -> None:
    try:
        from app.models.bank_roll import BankrollBet
        for bet in BankrollBet.query.filter_by(match_id=match_id, status="pending").all():
            bet.status = "manual_check"
    except Exception:
        pass


# =============================================================================
# Cache finished games — hourly
# =============================================================================

@celery.task(name="app.workers.celery_tasks.cache_finished_games",
             soft_time_limit=120, time_limit=150)
def cache_finished_games() -> dict:
    try:
        from app.models.odds_model import UnifiedMatch
        now    = datetime.now(timezone.utc)
        cached = 0
        for day_offset in range(0, 30):
            day_start = (now - timedelta(days=day_offset)).replace(
                hour=0, minute=0, second=0, microsecond=0)
            day_end   = day_start + timedelta(days=1)
            date_str  = day_start.strftime("%Y-%m-%d")
            ck        = f"results:finished:{date_str}"
            if cache_get(ck):
                continue
            matches = UnifiedMatch.query.filter(
                UnifiedMatch.status == "FINISHED",
                UnifiedMatch.start_time >= day_start,
                UnifiedMatch.start_time <  day_end,
            ).all()
            if matches:
                cache_set(ck, [m.to_dict() for m in matches], ttl=30 * 86400)
                cached += 1
        return {"cached_days": cached}
    except Exception as exc:
        logger.error(f"[cache:finished] {exc}")
        return {"error": str(exc)}


# =============================================================================
# Notifications
# =============================================================================

@celery.task(
    bind=True, name="app.workers.celery_tasks.dispatch_notifications",
    max_retries=2, soft_time_limit=30, time_limit=45, acks_late=True,
)
def dispatch_notifications(self, match_id: int, event_type: str) -> dict:
    try:
        from app.models.odds_model import UnifiedMatch, ArbitrageOpportunity, EVOpportunity
        from app.models.notifications import NotificationPref
        from app.models.customer import Customer
        from app.workers.notification_service import NotificationService

        um = UnifiedMatch.query.get(match_id)
        if not um:
            return {"ok": False}
        match_label = f"{um.home_team_name} v {um.away_team_name}"
        arbs = ArbitrageOpportunity.query.filter_by(match_id=match_id).filter(
            ArbitrageOpportunity.status == "OPEN").all()
        evs  = EVOpportunity.query.filter_by(match_id=match_id).filter(
            EVOpportunity.status == "OPEN").all()
        if not arbs and not evs:
            return {"ok": True, "sent": 0}
        prefs = (NotificationPref.query.join(Customer).filter(Customer.is_active == True).all())
        sent = 0
        for pref in prefs:
            user = pref.user
            if not pref.email_enabled:
                continue
            qa = [a for a in arbs if (a.profit_pct or 0) >= (pref.arb_min_profit or 0)]
            qe = [e for e in evs  if (e.ev_pct or 0)    >= (pref.ev_min_edge  or 0)]
            if not qa and not qe:
                continue
            try:
                NotificationService.send_alert(user=user, match_label=match_label,
                                               arbs=qa, evs=qe, event_type=event_type)
                sent += 1
            except Exception as e:
                logger.warning(f"[notify] user {user.id}: {e}")
        return {"ok": True, "sent": sent}
    except Exception as exc:
        raise self.retry(exc=exc)


# =============================================================================
# Subscription expiry
# =============================================================================

@celery.task(name="app.workers.celery_tasks.expire_subscriptions",
             soft_time_limit=30, time_limit=45)
def expire_subscriptions() -> dict:
    try:
        from app.extensions import db
        from app.models.subscriptions import Subscription, SubscriptionStatus
        now = datetime.now(timezone.utc)
        expired = 0
        for sub in Subscription.query.filter(
            Subscription.status   == SubscriptionStatus.TRIAL.value,
            Subscription.is_trial == True,
            Subscription.trial_ends <= now,
        ).all():
            if _attempt_charge(sub):
                sub.activate()
            else:
                sub.status   = SubscriptionStatus.EXPIRED.value
                sub.is_trial = False
            expired += 1
        for sub in Subscription.query.filter(
            Subscription.status     == SubscriptionStatus.ACTIVE.value,
            Subscription.auto_renew == True,
            Subscription.period_end <= now,
        ).all():
            if _attempt_charge(sub):
                sub.activate()
            else:
                sub.status = SubscriptionStatus.EXPIRED.value
            expired += 1
        if expired:
            db.session.commit()
        return {"processed": expired}
    except Exception as exc:
        logger.error(f"[subs:expire] {exc}")
        return {"error": str(exc)}


def _attempt_charge(sub) -> bool:
    return False   # stub — integrate M-Pesa / Stripe here


def _notify_billing(user_id: int, event_type: str, message: str) -> None:
    try:
        from app.extensions import db
        from app.models.notifications import Notification
        from app.models.customer import Customer
        db.session.add(Notification(
            user_id=user_id, type="billing",
            event_type=event_type, title="Billing Alert", message=message,
        ))
        db.session.flush()
        user = Customer.query.get(user_id)
        if user and user.email:
            send_async_email.apply_async(
                args=[f"OddsKenya — {message[:60]}", [user.email],
                      f"<p>{message}</p>", "html"],
                queue="notify")
    except Exception as e:
        logger.warning(f"[billing:notify] user {user_id}: {e}")


# =============================================================================
# Health check
# =============================================================================

@celery.task(name="app.workers.celery_tasks.health_check",
             soft_time_limit=10, time_limit=15)
def health_check() -> dict:
    ts = _now_iso()
    cache_set("worker_heartbeat", {"alive": True, "checked_at": ts, "pid": os.getpid()}, ttl=120)
    return {"ok": True, "ts": ts}


# =============================================================================
# On-demand probe (admin)
# =============================================================================

@celery.task(name="app.workers.celery_tasks.probe_bookmaker_now",
             soft_time_limit=30, time_limit=45)
def probe_bookmaker_now(bookmaker: dict, sport: str, mode: str = "live") -> dict:
    from app.views.odds_feed.bookmaker_fetcher import fetch_bookmaker
    t0 = time.perf_counter()
    try:
        matches = fetch_bookmaker(bookmaker, sport_name=sport, mode=mode, timeout=20)
        latency = int((time.perf_counter() - t0) * 1000)
        return {"ok": True, "count": len(matches), "latency_ms": latency,
                "matches": matches[:10], "bookmaker": bookmaker.get("name")}
    except Exception as exc:
        return {"ok": False, "error": str(exc),
                "latency_ms": int((time.perf_counter() - t0) * 1000)}


# =============================================================================
# WhatsApp + Email senders
# =============================================================================

whatsapp_bot = os.environ.get("WA_BOT", "")
message_url  = f"{whatsapp_bot}/api/v1/send-message" if whatsapp_bot else ""


@celery.task(name="app.workers.celery_tasks.send_message",
             soft_time_limit=30, time_limit=45)
def send_message(msg: str, whatsapp_number: str):
    if not message_url:
        return {"error": "WA_BOT not configured"}
    r = requests.post(message_url, json={"message": msg, "number": whatsapp_number}, timeout=15)
    return r.text


@celery_service.task(
    name="app.workers.celery_tasks.send_async_email",
    bind=True, max_retries=3, default_retry_delay=30,
    soft_time_limit=60, time_limit=90,
)
def send_async_email(self, subject, recipients, body, body_type="plain",
                     attachments=None, username=None, password=None):
    try:
        app = create_app()
        with app.app_context():
            mail = Mail(app)
            msg  = Message(subject=subject, recipients=recipients,
                           sender=os.environ.get("ADMIN_EMAIL"))
            if body_type == "html": msg.html = body
            else:                   msg.body = body
            for att in (attachments or []):
                fname = att.get("filename", "file")
                mime  = att.get("mimetype", "application/octet-stream")
                content = att.get("content")
                if content:
                    try: msg.attach(fname, mime, base64.b64decode(content))
                    except Exception as e: logger.warning(f"[email] attachment: {e}")
            mail.send(msg)
    except Exception as exc:
        raise self.retry(exc=exc)


def create_message_with_attachment(sender, to, subject,
                                   html_message=None, text_message=None, files=None):
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication
    message = MIMEMultipart()
    message["to"] = to; message["from"] = sender; message["subject"] = subject
    body = MIMEText(html_message or text_message or "",
                    "html" if html_message else "plain")
    message.attach(body)
    for f in (files or []):
        resp = requests.get(f["url"], stream=True, timeout=15)
        if resp.status_code != 200: continue
        ct, enc = mimetypes.guess_type(f["url"])
        if not ct or enc: ct = "application/octet-stream"
        _, sub = ct.split("/", 1)
        att = MIMEApplication(resp.content, _subtype=sub)
        att.add_header("Content-Disposition", "attachment", filename=f["name"])
        message.attach(att)
    return {"raw": base64.urlsafe_b64encode(message.as_bytes()).decode()}