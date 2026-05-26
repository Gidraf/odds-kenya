"""
app/workers/live_broadcaster.py
================================
Detects game events (Goals, Phase changes) from live harvester state diffs,
logs them, publishes to the frontend SSE channel, and — critically —
saves full-time results to the DB so the Results tab works for all sports.
"""

from __future__ import annotations

import json
import logging
import os

import redis as _redis_lib

log = logging.getLogger("kinetic_live_events")
log.setLevel(logging.INFO)

# ── Redis connection (DB 2 — same as SSE endpoint) ────────────────────────────
_redis_url  = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_base_url   = _redis_url.rsplit("/", 1)[0] if _redis_url.count("/") >= 3 else _redis_url
r = _redis_lib.from_url(f"{_base_url}/2", decode_responses=True)


# ── Internal match-id cache ────────────────────────────────────────────────────

def get_internal_id(betradar_id: str) -> int | None:
    """Resolve betradar_id → internal DB UnifiedMatch.id (with Redis cache)."""
    if not betradar_id:
        return None
    cache_key = f"map:br_to_db:{betradar_id}"
    cached    = r.get(cache_key)
    if cached:
        return int(cached)
    try:
        from app.models.odds import UnifiedMatch
        from app.extensions import db
        um = db.session.query(UnifiedMatch.id).filter_by(
            parent_match_id=str(betradar_id)
        ).first()
        if um:
            r.setex(cache_key, 3600, um[0])
            return um[0]
    except Exception as exc:
        log.debug("get_internal_id error %s: %s", betradar_id, exc)
    return None


# ── Result saving ─────────────────────────────────────────────────────────────

def save_result_to_db(
    betradar_id: str,
    score_home:  str | None,
    score_away:  str | None,
    source:      str = "live_broadcaster",
) -> bool:
    """
    Persist a full-time result to UnifiedMatch.
    Called when any live harvester detects FT.
    Returns True on success.
    """
    if not betradar_id:
        return False

    # Avoid duplicate saves in a short window
    lock_key = f"ft_saved:{betradar_id}"
    if r.get(lock_key):
        return False

    try:
        from app.models.odds import UnifiedMatch
        from app.extensions import db
        from datetime import datetime, timezone

        um = UnifiedMatch.query.filter_by(
            parent_match_id=str(betradar_id)
        ).first()

        if not um:
            log.warning("[result_save] UnifiedMatch not found for br_id=%s", betradar_id)
            return False

        # Parse integer scores
        sh: int | None = None
        sa: int | None = None
        try:
            sh = int(score_home) if score_home is not None else None
        except (TypeError, ValueError):
            pass
        try:
            sa = int(score_away) if score_away is not None else None
        except (TypeError, ValueError):
            pass

        um.status           = "finished"
        um.final_score_home = sh
        um.final_score_away = sa
        um.result_source    = source
        um.finished_at      = datetime.now(timezone.utc)
        db.session.commit()

        # Rate-limit lock — 30 min so we don't save it twice from two harvesters
        r.setex(lock_key, 1800, "1")

        log.info(
            "[result_save] ✅ Saved: %s  %s %s-%s %s (source=%s)",
            betradar_id,
            um.home_team_name or "?",
            sh, sa,
            um.away_team_name or "?",
            source,
        )

        # Also dispatch Celery task for any post-processing (notifications etc.)
        try:
            from app.workers.celery_tasks import celery
            celery.send_task(
                "tasks.ops.save_match_result",
                args=[f"br_{betradar_id}", {
                    "score_home": sh,
                    "score_away": sa,
                    "source":     source,
                }],
                queue="results",
            )
        except Exception:
            pass

        return True

    except Exception as exc:
        log.error("[result_save] DB error for br_id=%s: %s", betradar_id, exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        return False


def _parse_score(score_str: str) -> tuple[str | None, str | None]:
    """Parse 'home-away' or 'home:away' into (home, away)."""
    s = (score_str or "").strip()
    for sep in ("-", ":"):
        if sep in s:
            parts = s.split(sep, 1)
            return parts[0].strip(), parts[1].strip()
    return None, None


# ── Main broadcaster ──────────────────────────────────────────────────────────

def broadcast_event_state(
    betradar_id: str,
    bookie:      str,
    old_state:   dict,
    new_state:   dict,
) -> None:
    """
    Detects game events (Goals, Phase changes), logs them,
    saves FT results to DB, and pushes to the frontend SSE channel.
    """
    internal_id = get_internal_id(betradar_id)
    if not internal_id:
        return

    old_score  = (old_state.get("score") or "").strip()
    new_score  = (new_state.get("score") or "").strip()
    old_phase  = (old_state.get("phase") or "").lower()
    new_phase  = (new_state.get("phase") or "").lower()
    match_time = new_state.get("match_time", "")

    # ── 1. Goal detection ────────────────────────────────────────────────────
    if new_score and new_score != old_score and old_score:
        log.info(
            "⚽ GOAL! [%s] Match %s | %s ➔ %s",
            bookie.upper(), internal_id, old_score, new_score,
        )

    # ── 2. Phase change ──────────────────────────────────────────────────────
    if new_phase and new_phase != old_phase:
        _FT_PHASES = {"ended", "ft", "full-time", "fulltime", "finished",
                      "complete", "completed", "over", "result"}
        _HT_PHASES = {"halftime", "ht", "half-time", "half_time"}
        _KO_PHASES = {"started", "1h", "first half", "first_half", "in_play",
                      "live", "inplay"}

        if new_phase in _KO_PHASES:
            log.info("🟢 KICKOFF [%s] Match %s", bookie.upper(), internal_id)

        elif new_phase in _HT_PHASES:
            log.info("⏱️ HALF-TIME [%s] Match %s", bookie.upper(), internal_id)

        elif new_phase in _FT_PHASES:
            log.info(
                "🏁 FULL-TIME [%s] Match %s | Final: %s",
                bookie.upper(), internal_id, new_score or "?"
            )
            # ── SAVE RESULT TO DB ────────────────────────────────────────────
            sh, sa = _parse_score(new_score)
            save_result_to_db(
                betradar_id  = betradar_id,
                score_home   = sh,
                score_away   = sa,
                source       = f"{bookie}_live",
            )
        else:
            log.info(
                "🔄 PHASE [%s] Match %s ➔ %s",
                bookie.upper(), internal_id, new_phase.upper(),
            )

    # ── 3. Publish to frontend SSE channel ───────────────────────────────────
    sh_new, sa_new = _parse_score(new_score)
    payload = {
        "match_id":     internal_id,
        "betradar_id":  betradar_id,
        "current_score": new_score,
        "score_home":   sh_new,
        "score_away":   sa_new,
        "match_time":   match_time,
        "event_status": new_state.get("phase", ""),
        "is_live":      True,
        "bookie":       bookie,
    }
    r.publish(f"match:update:{internal_id}", json.dumps(payload))
    # Also publish to sport-level channel used by SSE streams
    r.publish(f"bus:live_updates:soccer", json.dumps({**payload, "join_key": f"br_{betradar_id}"}))


def broadcast_market_odds(
    betradar_id: str,
    bookie:      str,
    market_slug: str,
    outcomes:    dict,
) -> None:
    """Pushes real-time odds changes directly to the UI."""
    internal_id = get_internal_id(betradar_id)
    if not internal_id:
        return

    payload = {
        "match_id":  internal_id,
        "bookmakers": {
            bookie: {
                "markets": {
                    market_slug: outcomes
                }
            }
        },
    }
    r.publish(f"match:update:{internal_id}", json.dumps(payload))