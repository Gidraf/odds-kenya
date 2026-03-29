"""
app/workers/celery_app.py
=========================
Single source of truth for the Celery instance.

Every task module does:
    from app.workers.celery_app import celery

This module MUST be imported before any @celery.task decorator fires, so
keep it side-effect-free (no DB calls, no harvester imports).

Queue layout
────────────
  default   – health, expire, misc
  harvest   – slow upcoming scrapes  (concurrency 4)
  live      – fast live scrapes      (concurrency 8)
  ev_arb    – EV / arbitrage compute (concurrency 4)
  results   – result updates         (concurrency 2)
  notify    – email / WS publish     (concurrency 4)
"""

from __future__ import annotations

import json
import os

from app import create_app
from app.extensions import celery as celery_service, init_celery


# ── Task route table (centralised here so all modules share it) ────────────
TASK_ROUTES: dict[str, dict] = {
    # ── Registry ─────────────────────────────────────────────────────────────
    "harvest.bookmaker_sport":       {"queue": "harvest"},
    "harvest.all_upcoming":          {"queue": "harvest"},
    "harvest.merge_broadcast":       {"queue": "harvest"},
    "harvest.value_bets":            {"queue": "ev_arb"},
    "harvest.cleanup":               {"queue": "harvest"},
    # ── SP ───────────────────────────────────────────────────────────────────
    "tasks.sp.harvest_sport":        {"queue": "harvest"},
    "tasks.sp.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.sp.harvest_all_live":     {"queue": "live"},
    # ── BT ───────────────────────────────────────────────────────────────────
    "tasks.bt.harvest_sport":        {"queue": "harvest"},
    "tasks.bt.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.bt.harvest_all_live":     {"queue": "live"},
    # ── OD ───────────────────────────────────────────────────────────────────
    "tasks.od.harvest_sport":        {"queue": "harvest"},
    "tasks.od.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.od.harvest_all_live":     {"queue": "live"},
    # ── B2B direct ───────────────────────────────────────────────────────────
    "tasks.b2b.harvest_sport":        {"queue": "harvest"},
    "tasks.b2b.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.b2b.harvest_all_live":     {"queue": "live"},
    # ── B2B page fan-out (legacy) ─────────────────────────────────────────────
    "tasks.b2b_page.harvest_page":        {"queue": "harvest"},
    "tasks.b2b_page.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.b2b_page.harvest_all_live":     {"queue": "live"},
    # ── SBO ──────────────────────────────────────────────────────────────────
    "tasks.sbo.harvest_sport":        {"queue": "harvest"},
    "tasks.sbo.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.sbo.harvest_all_live":     {"queue": "live"},
    # ── Compute / notify / ops ────────────────────────────────────────────────
    "tasks.ops.compute_ev_arb":          {"queue": "ev_arb"},
    "tasks.ops.update_match_results":    {"queue": "results"},
    "tasks.ops.dispatch_notifications":  {"queue": "notify"},
    "tasks.ops.publish_ws_event":        {"queue": "notify"},
    "tasks.ops.health_check":            {"queue": "default"},
    "tasks.ops.expire_subscriptions":    {"queue": "default"},
    "tasks.ops.cache_finished_games":    {"queue": "results"},
    "tasks.ops.send_async_email":        {"queue": "notify"},
    "tasks.ops.send_message":            {"queue": "notify"},
}


def make_celery(app=None):
    """Bind Celery to Flask app context and configure queues."""
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
    )
    return celery_service


flask_app = create_app()
celery    = make_celery(flask_app)


# ── Redis helpers (shared across all task modules) ─────────────────────────

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
        return [k.decode() if isinstance(k, bytes) else k
                for k in _redis().keys(pattern)]
    except Exception:
        return []


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _publish(channel: str, data: dict):
    """Fire-and-forget Redis pub to a channel (bypasses Celery queue)."""
    try:
        _redis().publish(channel, json.dumps(data, default=str))
    except Exception:
        pass


def _upsert_and_chain(matches: list[dict], bk_name: str) -> None:
    from app.workers.tasks_ops import compute_ev_arb
    from celery import chain as cchain
    try:
        from app.models.bookmakers_model import Bookmaker
        bm    = Bookmaker.query.filter(Bookmaker.name.ilike(f"%{bk_name}%")).first()
        bk_id = bm.id if bm else None
        for m in matches:
            mid = _upsert_unified_match(_to_upsert_shape(m), bk_id, bk_name)
            if mid:
                cchain(
                    compute_ev_arb.si(mid),
                ).apply_async(queue="ev_arb", countdown=1)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error(f"[upsert] {bk_name}: {exc}")


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


def _upsert_unified_match(match_data: dict, bookmaker_id, bookmaker_name: str):
    from datetime import datetime
    try:
        from app.extensions import db
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
        )
        parent_id = str(
            match_data.get("match_id") or match_data.get("betradar_id") or ""
        )
        if not parent_id:
            return None

        home  = str(match_data.get("home_team") or "")
        away  = str(match_data.get("away_team") or "")
        sport = str(match_data.get("sport")     or "")
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

        um = (UnifiedMatch.query
              .filter_by(parent_match_id=parent_id)
              .with_for_update().first())
        if not um:
            um = UnifiedMatch(
                parent_match_id=parent_id, home_team_name=home,
                away_team_name=away, sport_name=sport,
                competition_name=comp, start_time=start_time,
            )
            db.session.add(um); db.session.flush()
        else:
            if home  and home  != um.home_team_name: um.home_team_name   = home
            if away  and away  != um.away_team_name: um.away_team_name   = away
            if sport and not um.sport_name:           um.sport_name       = sport
            if comp  and not um.competition_name:     um.competition_name = comp
            if start_time and not um.start_time:      um.start_time       = start_time

        if bookmaker_id:
            bmo = (BookmakerMatchOdds.query
                   .filter_by(match_id=um.id, bookmaker_id=bookmaker_id)
                   .with_for_update().first())
            if not bmo:
                bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bookmaker_id)
                db.session.add(bmo); db.session.flush()

            history_batch: list[dict] = []
            markets = match_data.get("markets") or {}
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
                        market=mkt_key, specifier=None, selection=outcome, price=price,
                    )
                    um.upsert_bookmaker_price(
                        market=mkt_key, specifier=None, selection=outcome,
                        price=price, bookmaker_id=bookmaker_id,
                    )
                    if price_changed:
                        history_batch.append({
                            "bmo_id": bmo.id, "bookmaker_id": bookmaker_id,
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
        import logging
        logging.getLogger(__name__).error(f"[upsert_match] {exc}")
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        return None