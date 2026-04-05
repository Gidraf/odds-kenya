"""
app/workers/celery_app.py  — PATCHED
=====================================
Key changes vs previous version:
  1. _normalise_sport_name()  — Football → Soccer on every write
  2. _fuzzy_find_match()      — NEW: finds existing unified_match by
                                home+away+start_time when betradar_id is absent.
                                This is what makes BT odds attach to the same
                                row as SP instead of creating an orphan row.
  3. _upsert_unified_match()  — uses _fuzzy_find_match() as fallback when
                                parent_id would otherwise be a bt/od-specific ID.
"""

from __future__ import annotations

from datetime import timezone, timedelta, datetime
import json
import os

from app import create_app
from app.extensions import celery as celery_service, init_celery

LIVE_ENABLED: bool = False

TASK_ROUTES: dict[str, dict] = {
    "harvest.bookmaker_sport":        {"queue": "harvest"},
    "harvest.all_upcoming":           {"queue": "harvest"},
    "harvest.merge_broadcast":        {"queue": "harvest"},
    "harvest.value_bets":             {"queue": "ev_arb"},
    "harvest.cleanup":                {"queue": "harvest"},
    "tasks.sp.harvest_sport":         {"queue": "harvest"},
    "tasks.sp.harvest_all_upcoming":  {"queue": "harvest"},
    "tasks.sp.harvest_all_live":      {"queue": "live"},
    "tasks.bt.harvest_sport":         {"queue": "harvest"},
    "tasks.bt.harvest_all_upcoming":  {"queue": "harvest"},
    "tasks.bt.harvest_all_live":      {"queue": "live"},
    "tasks.od.harvest_sport":         {"queue": "harvest"},
    "tasks.od.harvest_all_upcoming":  {"queue": "harvest"},
    "tasks.od.harvest_all_live":      {"queue": "live"},
    "tasks.b2b.harvest_sport":        {"queue": "harvest"},
    "tasks.b2b.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.b2b.harvest_all_live":     {"queue": "live"},
    "tasks.b2b_page.harvest_page":         {"queue": "harvest"},
    "tasks.b2b_page.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.b2b_page.harvest_all_live":     {"queue": "live"},
    "tasks.sbo.harvest_sport":        {"queue": "harvest"},
    "tasks.sbo.harvest_all_upcoming": {"queue": "harvest"},
    "tasks.sbo.harvest_all_live":     {"queue": "live"},
    "tasks.sp.poll_all_event_details": {"queue": "live"},
    "tasks.ops.compute_ev_arb":         {"queue": "ev_arb"},
    "tasks.ops.update_match_results":   {"queue": "results"},
    "tasks.ops.dispatch_notifications": {"queue": "notify"},
    "tasks.ops.publish_ws_event":       {"queue": "notify"},
    "tasks.ops.health_check":           {"queue": "default"},
    "tasks.ops.expire_subscriptions":   {"queue": "default"},
    "tasks.ops.cache_finished_games":   {"queue": "results"},
    "tasks.ops.send_async_email":       {"queue": "notify"},
    "tasks.ops.send_message":           {"queue": "notify"},
    "tasks.ops.persist_combined_batch": {"queue": "results"},
    "tasks.ops.persist_all_sports":     {"queue": "results"},
    "tasks.ops.build_health_report":    {"queue": "default"},
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


# ── Redis helpers ─────────────────────────────────────────────────────────────

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


def _publish(channel: str, data: dict):
    try:
        _redis().publish(channel, json.dumps(data, default=str))
    except Exception:
        pass


# ── Sport name normalisation ──────────────────────────────────────────────────
_SPORT_NORM: dict[str, str] = {
    "football":     "Soccer",
    "Football":     "Soccer",
    "soccer":       "Soccer",
    "Soccer":       "Soccer",
    "basketball":   "Basketball",
    "Basketball":   "Basketball",
    "tennis":       "Tennis",
    "Tennis":       "Tennis",
    "ice-hockey":   "Ice Hockey",
    "ice hockey":   "Ice Hockey",
    "Ice Hockey":   "Ice Hockey",
    "Ice hockey":   "Ice Hockey",
    "volleyball":   "Volleyball",
    "Volleyball":   "Volleyball",
    "cricket":      "Cricket",
    "Cricket":      "Cricket",
    "rugby":        "Rugby",
    "Rugby":        "Rugby",
    "table-tennis": "Table Tennis",
    "table tennis": "Table Tennis",
    "Table Tennis": "Table Tennis",
    "handball":     "Handball",
    "Handball":     "Handball",
    "mma":          "MMA",
    "MMA":          "MMA",
    "boxing":       "Boxing",
    "Boxing":       "Boxing",
    "darts":        "Darts",
    "Darts":        "Darts",
    "esoccer":      "eSoccer",
    "eSoccer":      "eSoccer",
    "efootball":    "eSoccer",
    "eFootball":    "eSoccer",
}


def _normalise_sport_name(raw: str) -> str:
    """'Football' → 'Soccer', 'ice-hockey' → 'Ice Hockey', etc."""
    if not raw:
        return raw
    return _SPORT_NORM.get(raw, _SPORT_NORM.get(raw.lower(), raw))


# ── Fuzzy match lookup ────────────────────────────────────────────────────────

def _fuzzy_find_match(home: str, away: str, start_time_raw) -> str | None:
    """
    Find an existing unified_match row by home_team + away_team + start_time
    when we don't have a betradar_id (e.g. Betika matches).

    Returns the existing parent_match_id if found, None otherwise.

    Match window: ±90 minutes around start_time so minor time differences
    between bookmakers don't prevent linking.
    """
    if not home or not away:
        return None
    try:
        from app.models.odds_model import UnifiedMatch
        from sqlalchemy import func

        # Parse start_time
        start_dt: datetime | None = None
        if start_time_raw:
            if isinstance(start_time_raw, datetime):
                start_dt = start_time_raw
            elif isinstance(start_time_raw, str):
                start_dt = datetime.fromisoformat(
                    start_time_raw.replace("Z", "+00:00")
                )
            elif isinstance(start_time_raw, (int, float)):
                start_dt = datetime.utcfromtimestamp(float(start_time_raw))

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
            return um.parent_match_id

    except Exception as exc:
        import logging
        logging.getLogger(__name__).debug("[fuzzy_find] %s vs %s: %s", home, away, exc)

    return None


# ── IDs that come from a specific bookmaker (not canonical) ──────────────────
# If parent_id looks like one of these, we should fuzzy-search rather than
# create a new row, because these IDs don't match across bookmakers.
def _is_bk_specific_id(match_id: str) -> bool:
    """Return True if the ID is a bookmaker-internal ID (not a betradar ID)."""
    # Betradar IDs are typically 8-digit numbers starting from ~6000000+
    # BT/OD internal IDs tend to be shorter or follow different patterns.
    # We detect by checking if the ID was built from bt_match_id or od_match_id
    # by looking at its range — but that's fragile.
    # Instead, callers pass context; this is a safety-net check.
    return False  # conservative: let callers decide


# ─────────────────────────────────────────────────────────────────────────────

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
                cchain(compute_ev_arb.si(mid)).apply_async(queue="ev_arb", countdown=1)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error(f"[upsert] {bk_name}: {exc}")


def _to_upsert_shape(m: dict) -> dict:
    return {
        "match_id":    (
            m.get("betradar_id") or m.get("b2b_match_id") or
            m.get("sp_game_id")  or m.get("od_match_id")  or
            m.get("bt_match_id") or ""
        ),
        "betradar_id": m.get("betradar_id") or "",
        "home_team":   m.get("home_team", ""),
        "away_team":   m.get("away_team", ""),
        "sport":       m.get("sport", ""),
        "competition": m.get("competition", ""),
        "start_time":  m.get("start_time"),
        "markets":     m.get("markets") or m.get("best_odds") or {},
        "bookmakers":  m.get("bookmakers") or {},
        # Pass through BT/OD-specific IDs so the upsert can fuzzy-match
        "bt_match_id": m.get("bt_match_id") or "",
        "bt_parent_id": m.get("bt_parent_id") or "",
        "od_match_id": m.get("od_match_id") or "",
        "od_parent_id": m.get("od_parent_id") or "",
    }


def _upsert_unified_match(match_data: dict, bookmaker_id, bookmaker_name: str):
    import logging as _log
    _logger = _log.getLogger(__name__)

    try:
        from app.extensions import db
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
        )

        # ── Resolve parent_match_id ───────────────────────────────────────────
        # Priority: betradar_id > fuzzy match > bt/od specific id
        betradar_id = str(match_data.get("betradar_id") or "").strip()
        home        = str(match_data.get("home_team")   or "").strip()
        away        = str(match_data.get("away_team")   or "").strip()
        start_time_raw = match_data.get("start_time")

        # IDs from bookmaker-specific fields (not canonical)
        bt_match_id  = str(match_data.get("bt_match_id")  or "").strip()
        bt_parent_id = str(match_data.get("bt_parent_id") or "").strip()
        od_match_id  = str(match_data.get("od_match_id")  or "").strip()
        od_parent_id = str(match_data.get("od_parent_id") or "").strip()

        # Also accept the legacy "match_id" field
        legacy_id = str(match_data.get("match_id") or "").strip()

        if betradar_id and betradar_id not in ("0", "None", ""):
            # Best case: we have a canonical ID → use it directly
            parent_id = betradar_id
        else:
            # No betradar_id — try to find an existing row by team names
            parent_id = _fuzzy_find_match(home, away, start_time_raw)

            if not parent_id:
                # Still no match — use whatever ID is available as a fallback
                # to at least create a row (it may be merged later when SP
                # harvests the same match with a betradar_id)
                parent_id = (
                    bt_parent_id or bt_match_id or
                    od_parent_id or od_match_id or
                    legacy_id or ""
                )

        if not parent_id:
            _logger.debug(
                "[upsert] no parent_id for %s vs %s — skipping", home, away
            )
            return None

        # ── Normalise sport ───────────────────────────────────────────────────
        sport = _normalise_sport_name(str(match_data.get("sport") or ""))
        comp  = str(match_data.get("competition") or "")

        # ── Parse start_time ──────────────────────────────────────────────────
        start_time = None
        if start_time_raw:
            try:
                if isinstance(start_time_raw, str):
                    start_time = datetime.fromisoformat(
                        start_time_raw.replace("Z", "+00:00")
                    )
                elif isinstance(start_time_raw, (int, float)):
                    start_time = datetime.utcfromtimestamp(float(start_time_raw))
                elif isinstance(start_time_raw, datetime):
                    start_time = start_time_raw
            except Exception:
                pass

        # ── Find or create UnifiedMatch ───────────────────────────────────────
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
            _logger.debug(
                "[upsert] created match %s: %s vs %s (%s)",
                parent_id, home, away, bookmaker_name,
            )
        else:
            if home  and home  != um.home_team_name: um.home_team_name   = home
            if away  and away  != um.away_team_name: um.away_team_name   = away
            if sport:                                 um.sport_name       = sport
            if comp  and not um.competition_name:     um.competition_name = comp
            if start_time and not um.start_time:      um.start_time       = start_time

        # ── Upsert BookmakerMatchOdds ─────────────────────────────────────────
        if bookmaker_id:
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
                    if isinstance(odds_data, (int, float)):
                        price = float(odds_data)
                    elif isinstance(odds_data, dict):
                        price = float(
                            odds_data.get("odds") or odds_data.get("odd") or 0
                        )
                    else:
                        continue
                    if price <= 1.0:
                        continue
                    price_changed, old_price = bmo.upsert_selection(
                        market=mkt_key, specifier=None, selection=outcome, price=price
                    )
                    um.upsert_bookmaker_price(
                        market=mkt_key, specifier=None, selection=outcome,
                        price=price, bookmaker_id=bookmaker_id,
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
        import logging
        logging.getLogger(__name__).error(f"[upsert_match] {exc}")
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        return None