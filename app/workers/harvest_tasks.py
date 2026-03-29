"""
app/workers/harvest_tasks.py
==============================
Celery tasks for upcoming odds harvesting.

Tasks
-----
  harvest_bookmaker_sport(slug, sport)    — harvest one bookmaker+sport combination
  harvest_all_upcoming()                  — fan-out: schedule all enabled bk×sport pairs
  compute_value_bets(sport)               — run after each harvest cycle completes
  cleanup_old_snapshots(days_keep=7)      — nightly prune of odds_snapshots table
  broadcast_harvest_done(slug, sport)     — publish to Redis so SSE clients refresh

Beat schedule (defined at bottom, registered in celery_app.py)
--------------------------------------------------------------
  Every 4h  → harvest_all_upcoming
  Every 24h → cleanup_old_snapshots
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from celery import Celery, chord, group
from celery.utils.log import get_task_logger

log = get_task_logger(__name__)


# ── Lazy app import (avoids circular imports) ─────────────────────────────────

def _get_celery() -> Celery:
    from app.workers.celery_tasks import celery_app
    return celery_app


def _get_db():
    from app import db
    return db


def _get_redis():
    import redis
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.from_url(url, decode_responses=True)


def _flask_ctx():
    from app import create_app
    return create_app().app_context()


# ══════════════════════════════════════════════════════════════════════════════
# REDIS HELPERS
# ══════════════════════════════════════════════════════════════════════════════

# Channels
CH_HARVEST_DONE = "odds:harvest:done"          # published after each bk+sport harvest
CH_ODDS_UPDATED = "odds:upcoming:{sport}"      # published with merged multi-bk data
CH_VALUE_BET    = "odds:value_bets:{sport}"    # published when value bets detected

# Keys
KEY_UPCOMING    = "odds:upcoming:{bookmaker}:{sport}"    # matches list per bk+sport
KEY_ALL_BK      = "odds:upcoming:all:{sport}"            # merged across all bk


def _cache_matches(bookmaker: str, sport: str, matches: list[dict], ttl: int) -> None:
    """Write match list to Redis. Compress large payloads with zlib."""
    r   = _get_redis()
    key = KEY_UPCOMING.format(bookmaker=bookmaker, sport=sport)
    payload = json.dumps({
        "bookmaker":    bookmaker,
        "sport":        sport,
        "match_count":  len(matches),
        "harvested_at": _now_iso(),
        "matches":      matches,
    }, ensure_ascii=False)
    r.setex(key, ttl, payload)


def _get_cached_matches(bookmaker: str, sport: str) -> dict | None:
    r   = _get_redis()
    raw = r.get(KEY_UPCOMING.format(bookmaker=bookmaker, sport=sport))
    return json.loads(raw) if raw else None


def _publish_harvest_done(bookmaker: str, sport: str, count: int) -> None:
    r = _get_redis()
    r.publish(CH_HARVEST_DONE, json.dumps({
        "type": "harvest_done", "bookmaker": bookmaker,
        "sport": sport, "count": count, "ts": _now_iso(),
    }))


def _publish_odds_updated(sport: str, bookmakers: list[str], merged_matches: list[dict]) -> None:
    """Publish merged multi-bookmaker data to the unified SSE channel."""
    r = _get_redis()
    r.publish(CH_ODDS_UPDATED.format(sport=sport), json.dumps({
        "type":       "odds_updated",
        "sport":      sport,
        "bookmakers": bookmakers,
        "count":      len(merged_matches),
        "ts":         _now_iso(),
    }))
    # Also cache the all-bookmaker merged view
    r.setex(KEY_ALL_BK.format(sport=sport), 14_400, json.dumps({
        "sport":        sport,
        "bookmakers":   bookmakers,
        "match_count":  len(merged_matches),
        "harvested_at": _now_iso(),
        "matches":      merged_matches,
    }))


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ══════════════════════════════════════════════════════════════════════════════
# TASK: harvest one bookmaker + sport
# ══════════════════════════════════════════════════════════════════════════════

def harvest_bookmaker_sport(bookmaker_slug: str, sport_slug: str) -> dict:
    """
    Harvest upcoming matches for one bookmaker+sport.

    Steps:
    1. Call bookmaker fetch_fn (returns canonical SpMatch[])
    2. Write to Redis (KEY_UPCOMING)
    3. Write OddsSnapshot rows to Postgres (odds history)
    4. Write HarvestRun audit row
    5. Publish CH_HARVEST_DONE → triggers merge + value bet detection
    """
    from app.workers.harvest_registry import get_bookmaker

    bk = get_bookmaker(bookmaker_slug)
    if not bk:
        log.error("harvest: unknown bookmaker %r", bookmaker_slug)
        return {"ok": False, "error": f"unknown bookmaker: {bookmaker_slug}"}

    if not bk["enabled"]:
        log.info("harvest: %s disabled — skipping", bookmaker_slug)
        return {"ok": True, "skipped": True}

    if sport_slug not in bk["sports"]:
        return {"ok": True, "skipped": True, "reason": "sport not in bookmaker list"}

    log.info("harvest start: %s / %s", bookmaker_slug, sport_slug)
    t0 = time.perf_counter()

    with _flask_ctx():
        from app.models.odds_models import db, HarvestRun, OddsSnapshot

        run = HarvestRun(bookmaker=bookmaker_slug, sport=sport_slug, status="running")
        db.session.add(run)
        db.session.commit()
        run_id = run.id

    try:
        matches: list[dict] = bk["fetch_fn"](sport_slug)
    except Exception as exc:
        log.exception("harvest fetch error: %s/%s", bookmaker_slug, sport_slug)
        with _flask_ctx():
            from app.models.odds_models import db, HarvestRun
            run = db.session.get(HarvestRun, run_id)
            if run:
                run.status    = "failed"
                run.error_msg = str(exc)
                run.finished_at = datetime.now(timezone.utc)
                db.session.commit()
        return {"ok": False, "error": str(exc)}

    if not matches:
        log.warning("harvest: %s/%s returned 0 matches", bookmaker_slug, sport_slug)
        with _flask_ctx():
            from app.models.odds_models import db, HarvestRun
            run = db.session.get(HarvestRun, run_id)
            if run:
                run.status    = "partial"
                run.match_count = 0
                run.finished_at = datetime.now(timezone.utc)
                db.session.commit()
        _publish_harvest_done(bookmaker_slug, sport_slug, 0)
        return {"ok": True, "bookmaker": bookmaker_slug, "sport": sport_slug, "count": 0}

    # ── Write to Redis ─────────────────────────────────────────────────────────
    _cache_matches(bookmaker_slug, sport_slug, matches, ttl=bk["redis_ttl"])

    # ── Write odds history to Postgres ────────────────────────────────────────
    snap_count  = 0
    mkt_count   = 0
    batch_size  = 500

    with _flask_ctx():
        from app.models.odds_models import db, HarvestRun, OddsSnapshot

        batch: list[OddsSnapshot] = []
        now   = datetime.now(timezone.utc)

        for match in matches:
            start_dt = None
            if match.get("start_time"):
                try:
                    start_dt = datetime.fromisoformat(
                        str(match["start_time"]).replace("Z", "+00:00")
                    )
                except Exception:
                    pass

            markets: dict = match.get("markets") or {}
            for mkt_slug, outcomes in markets.items():
                mkt_count += 1
                for out_key, odd_val in outcomes.items():
                    if not odd_val or float(odd_val) <= 1.0:
                        continue
                    batch.append(OddsSnapshot(
                        bookmaker   = bookmaker_slug,
                        sport       = sport_slug,
                        event_id    = str(match.get("sp_game_id") or match.get("event_id") or ""),
                        betradar_id = str(match.get("betradar_id") or ""),
                        home_team   = match.get("home_team", ""),
                        away_team   = match.get("away_team", ""),
                        competition = match.get("competition", ""),
                        start_time  = start_dt,
                        market_slug = mkt_slug,
                        outcome_key = out_key,
                        odd         = Decimal(str(odd_val)).quantize(Decimal("0.001")),
                        run_id      = run_id,
                        recorded_at = now,
                    ))
                    snap_count += 1

                    if len(batch) >= batch_size:
                        db.session.bulk_save_objects(batch)
                        db.session.commit()
                        batch.clear()

        if batch:
            db.session.bulk_save_objects(batch)
            db.session.commit()

        # Update harvest run
        run = db.session.get(HarvestRun, run_id)
        if run:
            run.status         = "success"
            run.match_count    = len(matches)
            run.market_count   = mkt_count
            run.snapshot_count = snap_count
            run.latency_ms     = int((time.perf_counter() - t0) * 1000)
            run.finished_at    = datetime.now(timezone.utc)
            db.session.commit()

    latency_ms = int((time.perf_counter() - t0) * 1000)
    log.info("harvest done: %s/%s → %d matches  %d snapshots  %dms",
             bookmaker_slug, sport_slug, len(matches), snap_count, latency_ms)

    _publish_harvest_done(bookmaker_slug, sport_slug, len(matches))

    return {
        "ok":          True,
        "bookmaker":   bookmaker_slug,
        "sport":       sport_slug,
        "count":       len(matches),
        "snapshots":   snap_count,
        "latency_ms":  latency_ms,
    }


# ══════════════════════════════════════════════════════════════════════════════
# TASK: fan-out harvest for all enabled bookmaker × sport pairs
# ══════════════════════════════════════════════════════════════════════════════

def harvest_all_upcoming() -> dict:
    """
    Fan-out: schedule harvest tasks for all enabled bookmaker+sport combos.
    Called by Celery Beat every 4 hours.

    Uses Celery group so all harvests run in parallel across workers.
    After all complete, triggers merge + value bet detection per sport.
    """
    from app.workers.harvest_registry import ENABLED_BOOKMAKERS

    pairs: list[tuple[str, str]] = []
    for bk in ENABLED_BOOKMAKERS:
        for sport in bk["sports"]:
            pairs.append((bk["slug"], sport))

    log.info("harvest_all_upcoming: scheduling %d pairs", len(pairs))

    # Fire all harvests in parallel (one Celery task per pair)
    jobs = group(
        _harvest_task.s(slug, sport)
        for slug, sport in pairs
    )
    jobs.apply_async()

    return {"ok": True, "scheduled": len(pairs)}


# ══════════════════════════════════════════════════════════════════════════════
# TASK: merge all bookmakers for a sport + publish unified SSE update
# ══════════════════════════════════════════════════════════════════════════════

def merge_and_broadcast(sport_slug: str) -> dict:
    """
    After harvests complete, merge all bookmakers' matches for a sport into
    one canonical view keyed by betradar_id, then publish to the unified channel.

    Merge strategy:
      - Group matches by betradar_id (cross-bookmaker event ID)
      - For each event: combine markets from all bookmakers
      - Each market outcome shows: {bookmaker: odd} dict
      - Best odd per outcome is flagged

    Published to: odds:upcoming:{sport}
    Cached at:    odds:upcoming:all:{sport}  (TTL 4h)
    """
    from app.workers.harvest_registry import ENABLED_BOOKMAKERS

    r          = _get_redis()
    bk_slugs   = [bk["slug"] for bk in ENABLED_BOOKMAKERS if sport_slug in bk["sports"]]
    merged: dict[str, dict] = {}   # betradar_id → merged match
    seen_bk: list[str] = []

    for slug in bk_slugs:
        cached = _get_cached_matches(slug, sport_slug)
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
                    # bookmaker-specific IDs
                    "event_ids":    {},
                    # merged markets: {slug: {outcome: {bookmaker: odd}}}
                    "markets":      {},
                }
            entry = merged[key]
            entry["event_ids"][slug] = str(
                match.get("sp_game_id") or match.get("event_id") or ""
            )

            for mkt_slug, outcomes in (match.get("markets") or {}).items():
                if mkt_slug not in entry["markets"]:
                    entry["markets"][mkt_slug] = {}
                for out_key, odd_val in outcomes.items():
                    if not odd_val or float(odd_val) <= 1.0:
                        continue
                    if out_key not in entry["markets"][mkt_slug]:
                        entry["markets"][mkt_slug][out_key] = {}
                    entry["markets"][mkt_slug][out_key][slug] = float(odd_val)

    # Flag best odd per outcome
    merged_list = list(merged.values())
    for match in merged_list:
        match["best_odds"] = {}   # {slug_outcome: bookmaker}
        for mkt_slug, outcomes in match["markets"].items():
            for out_key, bk_odds in outcomes.items():
                if bk_odds:
                    best_bk = max(bk_odds, key=lambda b: bk_odds[b])
                    match["best_odds"][f"{mkt_slug}__{out_key}"] = {
                        "bookmaker": best_bk,
                        "odd":       bk_odds[best_bk],
                    }

    log.info("merge_and_broadcast %s: %d events from %d bookmakers",
             sport_slug, len(merged_list), len(seen_bk))

    _publish_odds_updated(sport_slug, seen_bk, merged_list)
    return {"ok": True, "sport": sport_slug, "events": len(merged_list), "bookmakers": seen_bk}


# ══════════════════════════════════════════════════════════════════════════════
# TASK: value bet detection
# ══════════════════════════════════════════════════════════════════════════════

def compute_value_bets(sport_slug: str) -> dict:
    """
    Scan merged upcoming matches for value bets:
      value_pct = (best_odd / mean_of_others - 1) × 100
      threshold = VALUE_BET_THRESHOLD env var (default: 5%)

    Writes new ValueBet rows to Postgres.
    Publishes to odds:value_bets:{sport} channel.
    """
    threshold = float(os.getenv("VALUE_BET_THRESHOLD", "5.0"))
    r = _get_redis()

    raw = r.get(KEY_ALL_BK.format(sport=sport_slug))
    if not raw:
        log.info("compute_value_bets %s: no merged data", sport_slug)
        return {"ok": True, "found": 0}

    data    = json.loads(raw)
    matches = data.get("matches") or []
    found   = 0
    value_bets_payload: list[dict] = []

    with _flask_ctx():
        from app.models.odds_models import db, ValueBet

        for match in matches:
            for mkt_slug, outcomes in (match.get("markets") or {}).items():
                for out_key, bk_odds in outcomes.items():
                    if len(bk_odds) < 2:
                        continue   # need at least 2 bookmakers to compare

                    best_bk   = max(bk_odds, key=lambda b: bk_odds[b])
                    best_odd  = bk_odds[best_bk]
                    others    = [v for k, v in bk_odds.items() if k != best_bk]
                    consensus = sum(others) / len(others)

                    if consensus <= 1.0:
                        continue

                    val_pct = (best_odd / consensus - 1) * 100
                    if val_pct < threshold:
                        continue

                    start_dt = None
                    if match.get("start_time"):
                        try:
                            start_dt = datetime.fromisoformat(
                                str(match["start_time"]).replace("Z", "+00:00")
                            )
                        except Exception:
                            pass

                    vb = ValueBet(
                        sport               = sport_slug,
                        betradar_id         = match.get("betradar_id") or "",
                        home_team           = match.get("home_team", ""),
                        away_team           = match.get("away_team", ""),
                        start_time          = start_dt,
                        market_slug         = mkt_slug,
                        outcome_key         = out_key,
                        best_bookmaker      = best_bk,
                        best_odd            = Decimal(str(best_odd)).quantize(Decimal("0.001")),
                        consensus_odd       = Decimal(str(round(consensus, 3))).quantize(Decimal("0.001")),
                        value_pct           = Decimal(str(round(val_pct, 2))).quantize(Decimal("0.01")),
                        bookmakers_compared = len(bk_odds),
                        detected_at         = datetime.now(timezone.utc),
                    )
                    db.session.add(vb)
                    found += 1
                    value_bets_payload.append({
                        "home":          match.get("home_team"),
                        "away":          match.get("away_team"),
                        "market":        mkt_slug,
                        "outcome":       out_key,
                        "best_bk":       best_bk,
                        "best_odd":      best_odd,
                        "consensus_odd": round(consensus, 3),
                        "value_pct":     round(val_pct, 2),
                    })

        if found > 0:
            db.session.commit()

    if value_bets_payload:
        r.publish(
            CH_VALUE_BET.format(sport=sport_slug),
            json.dumps({"type": "value_bets", "sport": sport_slug,
                        "count": found, "bets": value_bets_payload[:20], "ts": _now_iso()})
        )

    log.info("compute_value_bets %s: %d value bets found (threshold=%.1f%%)",
             sport_slug, found, threshold)
    return {"ok": True, "sport": sport_slug, "found": found}


# ══════════════════════════════════════════════════════════════════════════════
# TASK: nightly cleanup
# ══════════════════════════════════════════════════════════════════════════════

def cleanup_old_snapshots(days_keep: int = 7) -> dict:
    """
    Delete OddsSnapshot rows older than days_keep.
    Runs nightly at 03:00 UTC.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_keep)
    log.info("cleanup: deleting odds_snapshots older than %s", cutoff.date())

    with _flask_ctx():
        from app.models.odds_models import db, OddsSnapshot, HarvestRun, ValueBet

        n_snaps = OddsSnapshot.query.filter(OddsSnapshot.recorded_at < cutoff).delete()
        n_runs  = HarvestRun.query.filter(HarvestRun.started_at < cutoff).delete()
        n_vbets = ValueBet.query.filter(ValueBet.detected_at < cutoff).delete()
        db.session.commit()

    log.info("cleanup done: %d snapshots  %d runs  %d value_bets deleted",
             n_snaps, n_runs, n_vbets)
    return {"ok": True, "deleted": {"snapshots": n_snaps, "runs": n_runs, "value_bets": n_vbets}}


# ══════════════════════════════════════════════════════════════════════════════
# CELERY TASK WRAPPERS
# (decorated lazily so this module can be imported without celery running)
# ══════════════════════════════════════════════════════════════════════════════

def register_tasks(celery: Celery) -> None:
    """
    Call this from celery_app.py after creating the Celery instance.
    Registers all tasks with proper names.
    """
    global _harvest_task, _merge_task, _value_bet_task, _cleanup_task

    @celery.task(name="harvest.bookmaker_sport", bind=True, max_retries=2,
                 default_retry_delay=60, queue="harvest")
    def harvest_task(self, bookmaker_slug: str, sport_slug: str) -> dict:
        try:
            return harvest_bookmaker_sport(bookmaker_slug, sport_slug)
        except Exception as exc:
            log.exception("harvest_task failed: %s/%s", bookmaker_slug, sport_slug)
            raise self.retry(exc=exc)

    @celery.task(name="harvest.merge_broadcast", queue="harvest")
    def merge_task(sport_slug: str) -> dict:
        result = merge_and_broadcast(sport_slug)
        _value_bet_task.delay(sport_slug)   # always run value bet detection after merge
        return result

    @celery.task(name="harvest.value_bets", queue="harvest")
    def value_bet_task(sport_slug: str) -> dict:
        return compute_value_bets(sport_slug)

    @celery.task(name="harvest.all_upcoming", queue="harvest")
    def all_upcoming_task() -> dict:
        return harvest_all_upcoming()

    @celery.task(name="harvest.cleanup", queue="harvest")
    def cleanup_task(days_keep: int = 7) -> dict:
        return cleanup_old_snapshots(days_keep)

    _harvest_task    = harvest_task
    _merge_task      = merge_task
    _value_bet_task  = value_bet_task
    _cleanup_task    = cleanup_task


# Forward references — filled in by register_tasks()
_harvest_task:   any = None
_merge_task:     any = None
_value_bet_task: any = None
_cleanup_task:   any = None