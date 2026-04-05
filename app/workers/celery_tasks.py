"""
app/workers/celery_tasks.py
============================
Central Celery application + shared helpers used by every task module.

Bookmaker ID resolution priority (used in _extract_betradar_id):
  SP  → m["betradar_id"]   — explicit sportradar ID
  BT  → m["bt_parent_id"]  — Betika stores sportradar ID in parent_match_id
  OD  → m["od_parent_id"]  — OdiBets stores sportradar ID in parent_match_id

When none of those fields are present, _fuzzy_find_match() does a
home + away + start_time ±90 min DB lookup so BT/OD odds still attach
to the correct unified_match row.

_get_or_create_bookmaker() auto-creates Bookmaker rows on a fresh DB
so bookmaker_match_odds are ALWAYS written regardless of seed data.
"""

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
    "tasks.bt.harvest_sport":              {"queue": "harvest"},
    "tasks.bt.harvest_all_upcoming":       {"queue": "harvest"},
    "tasks.bt.harvest_all_live":           {"queue": "live"},
    "tasks.od.harvest_sport":              {"queue": "harvest"},
    "tasks.od.harvest_all_upcoming":       {"queue": "harvest"},
    "tasks.od.harvest_all_live":           {"queue": "live"},
    "tasks.b2b.harvest_sport":             {"queue": "harvest"},
    "tasks.b2b.harvest_all_upcoming":      {"queue": "harvest"},
    "tasks.b2b.harvest_all_live":          {"queue": "live"},
    "tasks.b2b_page.harvest_page":         {"queue": "harvest"},
    "tasks.b2b_page.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.b2b_page.harvest_all_live":     {"queue": "live"},
    "tasks.sbo.harvest_sport":             {"queue": "harvest"},
    "tasks.sbo.harvest_all_upcoming":      {"queue": "harvest"},
    "tasks.sbo.harvest_all_live":          {"queue": "live"},
    "tasks.sp.poll_all_event_details":     {"queue": "live"},
    "tasks.ops.compute_ev_arb":            {"queue": "ev_arb"},
    "tasks.ops.update_match_results":      {"queue": "results"},
    "tasks.ops.dispatch_notifications":    {"queue": "notify"},
    "tasks.ops.publish_ws_event":          {"queue": "notify"},
    "tasks.ops.health_check":              {"queue": "default"},
    "tasks.ops.expire_subscriptions":      {"queue": "default"},
    "tasks.ops.cache_finished_games":      {"queue": "results"},
    "tasks.ops.send_async_email":          {"queue": "notify"},
    "tasks.ops.send_message":              {"queue": "notify"},
    "tasks.ops.persist_combined_batch":    {"queue": "results"},
    "tasks.ops.persist_all_sports":        {"queue": "results"},
    "tasks.ops.build_health_report":       {"queue": "default"},
}

_BK_NAME_TO_SLUG: dict[str, str] = {
    "sportpesa": "sp",
    "betika":    "bt",
    "odibets":   "od",
    "1xbet":     "b2b",
    "sbo":       "sbo",
}

_BK_SLUG_TO_NAME: dict[str, str] = {
    "sp":  "SportPesa",
    "bt":  "Betika",
    "od":  "OdiBets",
    "b2b": "1xBet",
    "sbo": "SBO",
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
        return [
            k.decode() if isinstance(k, bytes) else k
            for k in _redis().keys(pattern)
        ]
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
    "football":     "Soccer",   "Football":     "Soccer",
    "soccer":       "Soccer",   "Soccer":       "Soccer",
    "basketball":   "Basketball", "Basketball": "Basketball",
    "tennis":       "Tennis",   "Tennis":       "Tennis",
    "ice-hockey":   "Ice Hockey", "ice hockey": "Ice Hockey",
    "Ice Hockey":   "Ice Hockey", "Ice hockey": "Ice Hockey",
    "volleyball":   "Volleyball", "Volleyball": "Volleyball",
    "cricket":      "Cricket",  "Cricket":      "Cricket",
    "rugby":        "Rugby",    "Rugby":        "Rugby",
    "table-tennis": "Table Tennis", "table tennis": "Table Tennis",
    "Table Tennis": "Table Tennis",
    "handball":     "Handball", "Handball":     "Handball",
    "mma":          "MMA",      "MMA":          "MMA",
    "boxing":       "Boxing",   "Boxing":       "Boxing",
    "darts":        "Darts",    "Darts":        "Darts",
    "esoccer":      "eSoccer",  "eSoccer":      "eSoccer",
    "efootball":    "eSoccer",  "eFootball":    "eSoccer",
}


def _normalise_sport_name(raw: str) -> str:
    if not raw:
        return raw
    return _SPORT_NORM.get(raw, _SPORT_NORM.get(raw.lower(), raw))


# ══════════════════════════════════════════════════════════════════════════════
# BOOKMAKER AUTO-CREATION
# ══════════════════════════════════════════════════════════════════════════════

def _get_or_create_bookmaker(bk_name: str) -> int | None:
    """
    Find the Bookmaker row by name (case-insensitive).
    Auto-creates it if missing so BookmakerMatchOdds rows are always written
    even on a fresh DB with no seed data.

    Accepts both full names ("SportPesa", "Betika") and slugs ("sp", "bt").
    """
    try:
        from app.models.bookmakers_model import Bookmaker
        from app.extensions import db

        # Resolve slug → full name if needed
        canonical = _BK_SLUG_TO_NAME.get(bk_name.lower(), bk_name)

        bm = Bookmaker.query.filter(
            Bookmaker.name.ilike(f"%{canonical}%")
        ).first()
        if bm:
            return bm.id

        # Also try partial match on original name
        if canonical != bk_name:
            bm = Bookmaker.query.filter(
                Bookmaker.name.ilike(f"%{bk_name}%")
            ).first()
            if bm:
                return bm.id

        slug   = _BK_NAME_TO_SLUG.get(canonical.lower(),
                 _BK_NAME_TO_SLUG.get(bk_name.lower(),
                 bk_name.lower()[:4]))
        domain = f"{slug}.co.ke"
        bm = Bookmaker(name=canonical, domain=domain, is_active=True)
        db.session.add(bm)
        db.session.commit()
        _log.info("[bookmaker] auto-created: %s (id=%d slug=%s)", canonical, bm.id, slug)
        return bm.id

    except Exception as exc:
        _log.warning("[bookmaker] create failed for %s: %s", bk_name, exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# BETRADAR ID EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

def _extract_betradar_id(m: dict) -> str:
    """
    Extract the canonical betradar/sportradar ID from a raw match dict,
    regardless of which bookmaker produced it.

    Resolution order:
      1. m["betradar_id"]   — SP and some others set this explicitly
      2. m["betradarId"]    — alternate camelCase spelling
      3. m["sr_id"]         — SportRadar ID used by some aggregators
      4. m["bt_parent_id"]  — Betika: parent_match_id IS the sportradar ID
      5. m["od_parent_id"]  — OdiBets: parent_match_id IS the sportradar ID

    Returns "" (empty string) when nothing is found or the value is
    a sentinel like "0" / "None" / "null".
    """
    raw = (
        m.get("betradar_id")  or
        m.get("betradarId")   or
        m.get("sr_id")        or
        m.get("bt_parent_id") or   # Betika parent_match_id == sportradar ID
        m.get("od_parent_id") or   # OdiBets parent_match_id == sportradar ID
        ""
    )
    val = str(raw).strip()
    return "" if val in ("0", "None", "null", "") else val


# ══════════════════════════════════════════════════════════════════════════════
# FUZZY MATCH LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

def _fuzzy_find_match(
    home:           str,
    away:           str,
    start_time_raw, # str | int | float | datetime | None
) -> str | None:
    """
    Find an existing unified_match by home + away + start_time (±90 min window).
    Returns the parent_match_id (sportradar ID string) if found, else None.

    ONLY called when _extract_betradar_id() returns empty — i.e. when a
    bookmaker match has no ID field at all.  This lets BT/OD odds attach
    to the existing SP row for the same real-world event.

    Performance: indexed on home_team_name + away_team_name; start_time
    window keeps the result set tiny.
    """
    if not home or not away:
        return None
    try:
        from app.models.odds_model import UnifiedMatch
        from sqlalchemy import func

        start_dt: datetime | None = None
        if start_time_raw:
            try:
                if isinstance(start_time_raw, datetime):
                    start_dt = start_time_raw
                elif isinstance(start_time_raw, str):
                    start_dt = datetime.fromisoformat(
                        start_time_raw.replace("Z", "+00:00")
                    )
                elif isinstance(start_time_raw, (int, float)):
                    start_dt = datetime.utcfromtimestamp(float(start_time_raw))
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
        if um:
            _log.debug(
                "[fuzzy_find] matched %s vs %s → %s",
                home, away, um.parent_match_id,
            )
            return um.parent_match_id
        return None

    except Exception as exc:
        _log.debug("[fuzzy_find] error for %s vs %s: %s", home, away, exc)
        return None


# ══════════════════════════════════════════════════════════════════════════════
# UPSERT PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def _upsert_and_chain(matches: list[dict], bk_name: str) -> None:
    """
    Write odds to DB via _upsert_unified_match and chain EV/arb computation.

    Called immediately after each harvest so the DB is updated without
    waiting for the next persist_combined_batch cycle.
    """
    from app.workers.tasks_ops import compute_ev_arb
    from celery import chain as cchain

    try:
        bk_id = _get_or_create_bookmaker(bk_name)
        for m in matches:
            mid = _upsert_unified_match(_to_upsert_shape(m), bk_id, bk_name)
            if mid:
                cchain(compute_ev_arb.si(mid)).apply_async(
                    queue="ev_arb", countdown=1
                )
    except Exception as exc:
        _log.error("[upsert_and_chain] %s: %s", bk_name, exc)


def _to_upsert_shape(m: dict) -> dict:
    """
    Normalise a raw harvested match dict into the shape expected by
    _upsert_unified_match.

    Resolution chain for match_id (parent_match_id in DB):
      1. betradar_id via _extract_betradar_id (handles bt_parent_id, od_parent_id)
      2. bt_match_id  (Betika's own game ID — creates a BT-specific row)
      3. od_match_id  (OdiBets' own game ID — creates an OD-specific row)
      4. "" → skipped in _upsert_unified_match
    """
    betradar_id = _extract_betradar_id(m)
    return {
        "match_id":    betradar_id or m.get("bt_match_id") or m.get("od_match_id") or "",
        "betradar_id": betradar_id,
        "home_team":   m.get("home_team",   ""),
        "away_team":   m.get("away_team",   ""),
        "sport":       m.get("sport",       ""),
        "competition": m.get("competition", ""),
        "start_time":  m.get("start_time"),
        "markets":     m.get("markets") or m.get("best_odds") or {},
        "bookmakers":  m.get("bookmakers") or {},
    }


def _upsert_unified_match(
    match_data:     dict,
    bookmaker_id:   int | None,
    bookmaker_name: str,
) -> int | None:
    """
    Upsert a unified_match row and write BookmakerMatchOdds + history.

    Returns unified_match.id on success, None on failure.
    """
    try:
        from app.extensions import db
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
        )

        parent_id = str(match_data.get("match_id") or "").strip()
        if not parent_id:
            _log.debug(
                "[upsert] no parent_id for %s vs %s — skipping",
                match_data.get("home_team", "?"),
                match_data.get("away_team", "?"),
            )
            return None

        sport      = _normalise_sport_name(str(match_data.get("sport") or ""))
        comp       = str(match_data.get("competition") or "")
        home       = str(match_data.get("home_team")   or "").strip()
        away       = str(match_data.get("away_team")   or "").strip()
        start_time = _parse_start_time(match_data.get("start_time"))

        um = UnifiedMatch.query.filter_by(
            parent_match_id=parent_id
        ).with_for_update().first()

        if not um:
            um = UnifiedMatch(
                parent_match_id=parent_id,
                home_team_name=home,
                away_team_name=away,
                sport_name=sport,
                competition_name=comp,
                start_time=start_time,
            )
            db.session.add(um)
            db.session.flush()
            _log.debug(
                "[upsert] created %s: %s vs %s (%s)",
                parent_id, home, away, bookmaker_name,
            )
        else:
            if home       and home  != um.home_team_name:  um.home_team_name   = home
            if away       and away  != um.away_team_name:  um.away_team_name   = away
            if sport:                                        um.sport_name       = sport
            if comp       and not um.competition_name:      um.competition_name = comp
            if start_time and not um.start_time:            um.start_time       = start_time

        if bookmaker_id is None:
            _log.warning(
                "[upsert] bookmaker_id is None for %s — odds NOT written for %s vs %s",
                bookmaker_name,
                match_data.get("home_team", "?"),
                match_data.get("away_team", "?"),
            )
            db.session.commit()
            return um.id

        bmo = BookmakerMatchOdds.query.filter_by(
            match_id=um.id, bookmaker_id=bookmaker_id
        ).with_for_update().first()
        if not bmo:
            bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bookmaker_id)
            db.session.add(bmo)
            db.session.flush()

        history_batch: list[dict] = []
        for mkt_key, outcomes in (match_data.get("markets") or {}).items():
            for outcome, odds_data in outcomes.items():
                try:
                    if isinstance(odds_data, (int, float)):
                        price = float(odds_data)
                    elif isinstance(odds_data, dict):
                        price = float(
                            odds_data.get("odds") or
                            odds_data.get("odd")  or
                            odds_data.get("price") or 0
                        )
                    else:
                        continue
                except (TypeError, ValueError):
                    continue

                if price <= 1.0:
                    continue

                price_changed, old_price = bmo.upsert_selection(
                    market=mkt_key, specifier=None,
                    selection=outcome, price=price,
                )
                um.upsert_bookmaker_price(
                    market=mkt_key, specifier=None,
                    selection=outcome, price=price,
                    bookmaker_id=bookmaker_id,
                )
                if price_changed:
                    history_batch.append({
                        "bmo_id":       bmo.id,
                        "bookmaker_id": bookmaker_id,
                        "match_id":     um.id,
                        "market":       mkt_key,
                        "specifier":    None,
                        "selection":    outcome,
                        "old_price":    old_price,
                        "new_price":    price,
                        "price_delta":  round(price - old_price, 4) if old_price else None,
                        "recorded_at":  datetime.now(timezone.utc),
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


def _parse_start_time(raw) -> datetime | None:
    """Parse start_time from string, int (unix), or datetime. Returns None on failure."""
    if not raw:
        return None
    try:
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, str):
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if isinstance(raw, (int, float)):
            return datetime.utcfromtimestamp(float(raw))
    except Exception:
        pass
    return None