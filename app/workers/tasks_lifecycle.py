"""
app/workers/tasks_lifecycle.py
================================
Celery tasks for the match lifecycle + window service.

Defined here (not in live_results_api.py) so Celery's auto-discovery
via the `include` list in celery_app.py can find them without importing
Flask blueprint machinery.

Tasks
─────
  tasks.lifecycle.update_match_state   — write state transition to DB
  tasks.lifecycle.save_match_result    — write final result to DB
  tasks.lifecycle.flush_live_markets   — batch-write live market changes
  tasks.lifecycle.notify_event         — fan-out notifications
  tasks.lifecycle.window_scan          — beat-driven DB scan (fallback)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from app.workers.celery_tasks import celery

log = logging.getLogger("kinetic.tasks_lifecycle")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ══════════════════════════════════════════════════════════════════════════════
# STATE TRANSITIONS
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.lifecycle.update_match_state",
    bind=True, max_retries=3, default_retry_delay=5,
    acks_late=True,
)
def update_match_state(self, join_key: str, new_state: str, meta: dict):
    """Write a state transition to UnifiedMatch.status in PostgreSQL."""
    try:
        from app.models.odds import UnifiedMatch
        from app.extensions import db

        um = db.session.execute(
            db.select(UnifiedMatch).where(
                UnifiedMatch.parent_match_id == join_key
            )
        ).scalar_one_or_none()

        if not um:
            log.warning("update_match_state: no match for join_key=%s", join_key)
            return {"ok": False, "reason": "not_found", "join_key": join_key}

        um.status = new_state

        if new_state == "live":
            live_since_raw = meta.get("live_since", _now_iso())
            try:
                um.live_since = datetime.fromisoformat(
                    live_since_raw.replace("Z", "+00:00")
                )
            except Exception:
                um.live_since = datetime.now(timezone.utc)

        elif new_state == "finished":
            um.finished_at = datetime.now(timezone.utc)

        db.session.commit()
        log.info("Match state → %s: %s", new_state, join_key)
        return {"ok": True, "join_key": join_key, "state": new_state}

    except Exception as exc:
        log.error("update_match_state error: %s", exc)
        raise self.retry(exc=exc)


# ══════════════════════════════════════════════════════════════════════════════
# RESULTS
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.lifecycle.save_match_result",
    bind=True, max_retries=3, default_retry_delay=10,
    acks_late=True,
)
def save_match_result(self, join_key: str, result: dict):
    """Write final score to UnifiedMatch."""
    try:
        from app.models.odds import UnifiedMatch
        from app.extensions import db

        um = db.session.execute(
            db.select(UnifiedMatch).where(
                UnifiedMatch.parent_match_id == join_key
            )
        ).scalar_one_or_none()

        if not um:
            log.warning("save_match_result: no match for join_key=%s", join_key)
            return {"ok": False, "reason": "not_found"}

        um.status            = "finished"
        um.final_score_home  = str(result.get("score_home", ""))
        um.final_score_away  = str(result.get("score_away", ""))
        um.result_source     = result.get("source", "lifecycle")
        um.finished_at       = datetime.now(timezone.utc)
        db.session.commit()

        log.info("Result saved %s: %s-%s",
                 join_key, result.get("score_home"), result.get("score_away"))
        return {"ok": True, "join_key": join_key}

    except Exception as exc:
        log.error("save_match_result error: %s", exc)
        raise self.retry(exc=exc)


# ══════════════════════════════════════════════════════════════════════════════
# LIVE MARKET FLUSH
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.lifecycle.flush_live_markets",
    bind=True, max_retries=2, default_retry_delay=3,
    acks_late=True,
    soft_time_limit=3600, time_limit=6000,
)
def flush_live_markets(self, join_key: str, writes: list[dict]):
    """
    Batch-write live market changes to BookmakerMatchOdds.

    `writes` is a list of:
      {"bk": "sp", "slug": "1x2", "outcome": "1", "odd": 2.15, "ts": "..."}
    """
    if not writes:
        return {"ok": True, "written": 0}

    try:
        from app.models.odds import UnifiedMatch, BookmakerMatchOdds
        from app.extensions import db

        um = db.session.execute(
            db.select(UnifiedMatch).where(
                UnifiedMatch.parent_match_id == join_key
            )
        ).scalar_one_or_none()

        if not um:
            return {"ok": False, "reason": "not_found", "join_key": join_key}

        written = 0
        # Group by BK to minimise queries
        by_bk: dict[str, list] = {}
        for w in writes:
            by_bk.setdefault(w["bk"], []).append(w)

        for bk_slug, bk_writes in by_bk.items():
            try:
                from app.models.bookmakers_model import Bookmaker
                bm = Bookmaker.query.filter_by(slug=bk_slug).first()
                if not bm:
                    continue

                bmo = BookmakerMatchOdds.query.filter_by(
                    match_id=um.id, bookmaker_id=bm.id
                ).with_for_update(skip_locked=True).first()

                if not bmo:
                    bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bm.id)
                    db.session.add(bmo)
                    db.session.flush()

                for w in bk_writes:
                    try:
                        bmo.upsert_selection(
                            market=w["slug"],
                            specifier=None,
                            selection=w["outcome"],
                            price=float(w["odd"]),
                        )
                        written += 1
                    except Exception as inner:
                        log.debug("upsert_selection error: %s", inner)

            except Exception as bk_exc:
                log.warning("flush BK %s error: %s", bk_slug, bk_exc)
                continue

        db.session.commit()
        return {"ok": True, "join_key": join_key, "written": written}

    except Exception as exc:
        log.error("flush_live_markets error: %s", exc)
        raise self.retry(exc=exc)


# ══════════════════════════════════════════════════════════════════════════════
# NOTIFICATIONS
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.lifecycle.notify_event",
    bind=True, max_retries=2, default_retry_delay=5,
    acks_late=True,
)
def notify_lifecycle_event(self, event_type: str, join_key: str, payload: dict):
    """
    Fan-out lifecycle notifications to all watchers of a match.
    Dispatches email / SMS / webhook / pubsub per watcher preferences.
    """
    try:
        from app.workers.match_lifecycle import (
            get_lifecycle_manager, Notification, NotificationDispatcher,
        )

        mgr   = get_lifecycle_manager()
        saved = mgr.get_watch(join_key)
        if not saved:
            return {"ok": True, "reason": "no_watchers", "join_key": join_key}

        dispatcher = NotificationDispatcher()

        _MESSAGES = {
            "state_change":   lambda p: (
                f"Match {p.get('new_state', '').upper()}",
                f"{p.get('home_team','')} vs {p.get('away_team','')}",
            ),
            "result":         lambda p: (
                "Match Finished",
                f"Final: {(p.get('result') or {}).get('score_home','?')}"
                f"–{(p.get('result') or {}).get('score_away','?')}",
            ),
            "kickoff_delay":  lambda p: (
                "Kickoff Delayed",
                f"Delayed by {p.get('delay_minutes', 0):.0f} min",
            ),
            "goal":           lambda p: (
                "⚽ Goal!",
                f"Score: {p.get('score_home','?')}–{p.get('score_away','?')}",
            ),
            "arb_found":      lambda p: (
                f"⚡ Arb +{p.get('profit_pct', 0):.2f}%",
                f"Market: {p.get('market', '')}",
            ),
        }

        builder = _MESSAGES.get(event_type)
        if builder:
            title, body = builder(payload)
        else:
            title, body = event_type.replace("_", " ").title(), str(payload)[:120]

        sent = 0
        for watcher in saved.watchers:
            if event_type not in watcher.notify_on:
                continue
            try:
                notif = Notification(
                    match=saved, watcher=watcher,
                    event_type=event_type,
                    title=title, body=body,
                    data=payload,
                )
                dispatcher.dispatch(notif)
                sent += 1
            except Exception as w_exc:
                log.warning("notify user %s error: %s", watcher.user_id, w_exc)

        return {"ok": True, "join_key": join_key, "sent": sent}

    except Exception as exc:
        log.error("notify_lifecycle_event error: %s", exc)
        raise self.retry(exc=exc)


# ══════════════════════════════════════════════════════════════════════════════
# BEAT FALLBACK — window scan
# ══════════════════════════════════════════════════════════════════════════════

@celery.task(
    name="tasks.lifecycle.window_scan",
    soft_time_limit=6000, time_limit=9000,
)
def window_scan_beat():
    """
    Beat-driven fallback scan. Runs every 60s.

    Normally the MatchWindowService runs this in its own thread, but if
    the leader process restarts mid-cycle this task bridges the gap.
    Scans DB for matches in the 3h window and ensures they're in Redis.
    """
    try:
        from app.workers.match_window_service import get_window_service
        svc = get_window_service()
        # Only do the cheap Redis sync, not the full service start
        svc._scan_window()
        svc._check_kickoff_times()
        return {"ok": True}
    except Exception as exc:
        log.warning("window_scan_beat error: %s", exc)
        return {"ok": False, "error": str(exc)}