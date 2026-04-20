"""
app/workers/tasks_align.py
===========================
Cross-bookmaker fuzzy match alignment.

Uses FuzzyMatcher.bulk_align() to pair each BK's matches against the
SP (SportPesa) anchor list, then merges bookmaker odds into a single
unified record per real-world match.

Key tasks:
  align_sport(sport_slug, limit)   — align one sport
  align_all_sports()               — dispatch for every sport
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

from celery.utils.log import get_task_logger

from app.workers.celery_tasks import celery, _redis as _get_redis
from app.workers.fuzzy_matcher import bulk_align, match_dict_to_candidate

logger = get_task_logger(__name__)

_ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby",
    "ice-hockey", "volleyball", "handball", "table-tennis",
    "baseball", "mma", "boxing", "darts", "american-football", "esoccer",
]

_BK_KEYS = {
    "sp":        "odds:sp:upcoming:{sport}",
    "bt":        "odds:bt:upcoming:{sport}",
    "od":        "odds:od:upcoming:{sport}",
    "1xbet":     "odds:b2b:1xbet:upcoming:{sport}",
    "22bet":     "odds:b2b:22bet:upcoming:{sport}",
    "betwinner": "odds:b2b:betwinner:upcoming:{sport}",
    "melbet":    "odds:b2b:melbet:upcoming:{sport}",
    "megapari":  "odds:b2b:megapari:upcoming:{sport}",
    "helabet":   "odds:b2b:helabet:upcoming:{sport}",
    "paripesa":  "odds:b2b:paripesa:upcoming:{sport}",
}

_UNIFIED_KEY   = "odds:unified:upcoming:{sport}"
_ALIGNMENT_TTL = 7200


# ─────────────────────────────────────────────────────────────────────────────

@celery.task(
    name="tasks.align.sport",
    bind=True, max_retries=2, default_retry_delay=30,
    soft_time_limit=180, time_limit=210, acks_late=True,
)
def align_sport(self, sport_slug: str, limit: int = 500) -> dict:
    """
    Cross-BK alignment for one sport using SP as anchor.
    """
    t0 = time.perf_counter()
    r  = _get_redis()

    # ── 1. Load all BK snapshots ─────────────────────────────────────────
    bk_matches: dict[str, list[dict]] = {}
    for bk, key_tpl in _BK_KEYS.items():
        raw = r.get(key_tpl.format(sport=sport_slug))
        if not raw:
            continue
        try:
            data = json.loads(raw)
            ms   = data if isinstance(data, list) else data.get("matches", [])
            bk_matches[bk] = ms[:limit]
        except Exception as exc:
            logger.warning("[align:%s] parse error %s: %s", sport_slug, bk, exc)

    if not bk_matches:
        return {"ok": False, "reason": "no_data", "sport": sport_slug}

    # ── 2. Choose anchor BK (prefer SP for betradar IDs) ─────────────────
    anchor_bk = "sp" if "sp" in bk_matches else max(
        bk_matches, key=lambda k: len(bk_matches[k])
    )
    anchor_raw = bk_matches[anchor_bk]

    # Convert to the "existing_matches" format expected by bulk_align
    anchor_existing = []
    anchor_by_id: dict[Any, dict] = {}
    for idx, m in enumerate(anchor_raw):
        uid  = m.get("match_id") or m.get("sp_game_id") or m.get("external_id") or idx
        rec  = {
            "id":               uid,
            "betradar_id":      m.get("betradar_id") or "",
            "home_team_name":   m.get("home_team") or m.get("home_team_name", ""),
            "away_team_name":   m.get("away_team") or m.get("away_team_name", ""),
            "start_time":       m.get("start_time"),
            "competition_name": m.get("competition"),
            "external_ids":     {anchor_bk: str(uid)},
        }
        anchor_existing.append(rec)
        anchor_by_id[uid] = _make_unified(m, anchor_bk, sport_slug)

    unified_index: dict[Any, dict] = dict(anchor_by_id)
    total_aligned = 0
    alignment_map: dict[str, list[str]] = {}

    # ── 3. Align each other BK ───────────────────────────────────────────
    for bk, raw_list in bk_matches.items():
        if bk == anchor_bk:
            continue
        candidates = []
        for m in raw_list:
            try:
                candidates.append(match_dict_to_candidate(m, bk_slug=bk))
            except Exception:
                pass
        if not candidates:
            continue
        try:
            updates, creates = bulk_align(candidates, anchor_existing, sport_slug)
        except Exception as exc:
            logger.warning("[align:%s] bulk_align %s: %s", sport_slug, bk, exc)
            continue

        for result in updates:
            uid  = result.unified_match_id
            cand = result.candidate
            if uid not in unified_index:
                continue
            rec       = unified_index[uid]
            bk_mkts   = cand.raw.get("markets") or {}
            rec["bookmakers"][bk] = {
                "match_id":   cand.external_id,
                "markets":    bk_mkts,
                "confidence": result.confidence,
            }
            if bk not in rec.get("aligned_bks", []):
                rec.setdefault("aligned_bks", []).append(bk)
            alignment_map.setdefault(str(uid), []).append(f"{bk}:{cand.external_id}")
            total_aligned += 1

        for result in creates:
            cand = result.candidate
            key  = f"_new_{bk}_{cand.external_id}"
            unified_index[key] = _make_unified(cand.raw, bk, sport_slug)

    unified = list(unified_index.values())

    # ── 4. Write to Redis ─────────────────────────────────────────────────
    try:
        r.setex(
            _UNIFIED_KEY.format(sport=sport_slug),
            _ALIGNMENT_TTL,
            json.dumps({
                "sport":         sport_slug,
                "mode":          "upcoming",
                "source":        "alignment",
                "matches":       unified,
                "aligned_at":    time.time(),
                "anchor_bk":     anchor_bk,
                "bks":           list(bk_matches.keys()),
                "total_aligned": total_aligned,
            }, default=str),
        )
        r.publish(
            f"odds:all:upcoming:{sport_slug}:updates",
            json.dumps({
                "event": "snapshot_ready", "bk": "aligned",
                "sport": sport_slug, "count": len(unified), "ts": time.time(),
            }),
        )
    except Exception as exc:
        logger.error("[align:%s] redis write failed: %s", sport_slug, exc)

    _persist_alignment_map(sport_slug, alignment_map)

    latency = int((time.perf_counter() - t0) * 1000)
    logger.info(
        "[align:%s] %d BKs → %d unified, %d cross-aligned (%dms)",
        sport_slug, len(bk_matches), len(unified), total_aligned, latency,
    )
    return {
        "ok": True, "sport": sport_slug,
        "unified": len(unified), "total_aligned": total_aligned,
        "anchor_bk": anchor_bk, "latency_ms": latency,
    }


@celery.task(name="tasks.align.all_sports", soft_time_limit=60, time_limit=90)
def align_all_sports() -> dict:
    from celery import group as cgroup
    sigs = [align_sport.s(s, 500) for s in _ALL_SPORTS]
    cgroup(sigs).apply_async(queue="results")
    return {"dispatched": len(_ALL_SPORTS)}


# ─────────────────────────────────────────────────────────────────────────────

def _make_unified(m: dict, bk: str, sport: str) -> dict:
    bk_mkts  = m.get("markets") or {}
    bookmakers: dict = {}
    if bk_mkts:
        bookmakers[bk] = {
            "match_id": m.get("match_id") or m.get("sp_game_id") or m.get("external_id") or "",
            "markets":  bk_mkts,
        }
    if m.get("bookmakers"):
        bookmakers.update(m["bookmakers"])

    br_id    = m.get("betradar_id")
    join_key = str(br_id or m.get("match_id") or m.get("sp_game_id") or m.get("external_id") or id(m))
    return {
        "match_id":        m.get("match_id") or m.get("sp_game_id") or m.get("external_id"),
        "parent_match_id": join_key,
        "join_key":        join_key,
        "betradar_id":     br_id,
        "home_team":       m.get("home_team") or m.get("home_team_name", ""),
        "away_team":       m.get("away_team") or m.get("away_team_name", ""),
        "competition":     m.get("competition") or m.get("competition_name"),
        "start_time":      m.get("start_time"),
        "sport":           sport,
        "is_live":         m.get("is_live", False),
        "bookmakers":      bookmakers,
        "aligned_bks":     [bk],
        "market_slugs":    list(bk_mkts.keys()),
        "has_arb": False, "has_ev": False, "has_sharp": False,
        "best_arb_pct": 0.0, "best_ev_pct": 0.0,
    }


def _persist_alignment_map(sport_slug: str, alignment_map: dict[str, list[str]]) -> None:
    if not alignment_map:
        return
    try:
        from app.models.db import get_db_session
        from sqlalchemy import text
        with get_db_session() as session:
            for group_id, refs in alignment_map.items():
                session.execute(
                    text("""
                        INSERT INTO match_alignments (sport_slug, group_id, bk_refs, aligned_at)
                        VALUES (:sport, :gid, :refs, NOW())
                        ON CONFLICT (sport_slug, group_id)
                        DO UPDATE SET bk_refs=EXCLUDED.bk_refs, aligned_at=EXCLUDED.aligned_at
                    """),
                    {"sport": sport_slug, "gid": str(group_id), "refs": json.dumps(refs)},
                )
    except Exception as exc:
        logger.debug("[align:persist] DB write skipped: %s", exc)