"""
app/workers/tasks_bt_od.py
===========================
Standalone Betika + OdiBets harvest pipeline.

Architecture
────────────
  1. Fetch ALL sports from BT independently (uses parent_match_id as join key)
  2. Fetch ALL sports from OD independently (uses od_match_id as join key)
  3. Match events across bookmakers by: normalised_home | normalised_away | start_hour
  4. ONLY persist matches that appear in 2+ bookmakers
  5. Only store markets from bookmakers that actually have that match
  6. SP data is merged in if it's already in cache (bonus, not required)

Match Key Logic
───────────────
  key = first6(home_norm) | first6(away_norm) | start_time[:13]
  e.g. "irland|german|2026-04-10T14"

This avoids dependency on betradar_id alignment between BT and OD.
"""
from __future__ import annotations

import re
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as _date

from celery import group
from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded

from app.workers.celery_tasks import (
    celery, cache_set, cache_get, _now_iso, _publish,
    _get_or_create_bookmaker,
)

logger = get_task_logger(__name__)

# ── Sports to harvest ─────────────────────────────────────────────────────────
ALL_SPORT_SLUGS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "volleyball", "cricket", "rugby", "table-tennis",
    "handball", "mma", "boxing", "darts",
]

# ── Betika sport_id map (from betika_harvester.py) ───────────────────────────
BT_SLUG_TO_SPORT_ID: dict[str, int] = {
    "soccer":            14,
    "basketball":        30,
    "tennis":            28,
    "ice-hockey":        29,
    "rugby":             41,
    "handball":          33,
    "volleyball":        35,
    "cricket":           37,
    "table-tennis":      45,
    "mma":               106,
    "boxing":            106,
    "darts":             49,
    "esoccer":           105,
}

# ── OdiBets sport slugs (from od_harvester.py OD_SPORT_IDS) ─────────────────
OD_SPORT_SLUGS = [
    "soccer", "basketball", "tennis", "cricket",
    "rugby", "ice-hockey", "volleyball", "handball",
    "table-tennis", "mma", "boxing", "darts",
]

_BK_NAMES = {"bt": "Betika", "od": "OdiBets", "sp": "SportPesa"}
WS_CHANNEL = "odds:updates"


# =============================================================================
# MATCH KEY — team-name based, bookmaker-agnostic
# =============================================================================

def _norm(s: str) -> str:
    """Normalise a team name to 6 alphanum chars for fuzzy key."""
    return re.sub(r'[^a-z0-9]', '', s.lower())[:6]


def _match_key(home: str, away: str, start_time: str) -> str:
    """
    Bookmaker-agnostic match key.
    Uses first 6 chars of each team + start hour so minor name variations
    (e.g. "Man Utd" vs "Manchester United") still match.
    """
    return f"{_norm(home)}|{_norm(away)}|{str(start_time)[:13]}"


# =============================================================================
# FETCH HELPERS
# =============================================================================

def _fetch_bt_sport(sport_slug: str) -> list[dict]:
    """Fetch one sport from Betika upcoming."""
    try:
        from app.workers.bt_harvester import fetch_upcoming_matches, slug_to_bt_sport_id
        bt_id = slug_to_bt_sport_id(sport_slug)
        matches = fetch_upcoming_matches(sport_slug=sport_slug, max_pages=10)
        logger.info("[bt_od] BT %s: %d matches", sport_slug, len(matches))
        return matches
    except Exception as exc:
        logger.warning("[bt_od] BT %s fetch error: %s", sport_slug, exc)
        return []


def _fetch_od_sport(sport_slug: str) -> list[dict]:
    """Fetch one sport from OdiBets upcoming (all days of current month)."""
    try:
        from app.workers.od_harvester import fetch_upcoming_matches
        today = _date.today()
        # Fetch today + next 7 days
        from datetime import timedelta
        all_matches: list[dict] = []
        seen: set[str] = set()
        for offset in range(8):
            day_str = (_date.today() + timedelta(days=offset)).isoformat()
            try:
                for m in fetch_upcoming_matches(sport_slug=sport_slug, day=day_str):
                    mid = str(m.get("od_match_id") or m.get("od_event_id") or "")
                    if mid and mid not in seen:
                        seen.add(mid)
                        all_matches.append(m)
                    elif not mid:
                        all_matches.append(m)
            except Exception:
                pass
        logger.info("[bt_od] OD %s: %d matches", sport_slug, len(all_matches))
        return all_matches
    except Exception as exc:
        logger.warning("[bt_od] OD %s fetch error: %s", sport_slug, exc)
        return []


def _fetch_sp_cache(sport_slug: str) -> list[dict]:
    """Read SP cache if available — bonus data, never blocks."""
    try:
        cached = cache_get(f"sp:upcoming:{sport_slug}")
        return (cached or {}).get("matches") or []
    except Exception:
        return []


# =============================================================================
# CORE MATCHING LOGIC
# =============================================================================

def _index_by_key(matches: list[dict], bk_slug: str) -> dict[str, dict]:
    """Build {match_key: match_dict} index for one bookmaker's matches."""
    index: dict[str, dict] = {}
    for m in matches:
        home  = str(m.get("home_team") or m.get("home_team_name") or "")
        away  = str(m.get("away_team") or m.get("away_team_name") or "")
        start = str(m.get("start_time") or "")
        key   = _match_key(home, away, start)
        if not key.startswith("||"):  # skip if both teams empty
            index[key] = m
    return index


def _build_multi_bk_matches(
    bt_matches:  list[dict],
    od_matches:  list[dict],
    sp_matches:  list[dict],
    sport_slug:  str,
) -> list[dict]:
    """
    Merge matches from all bookmakers.
    Only return matches that appear in 2+ bookmakers.
    Each returned match contains ONLY the markets from bookmakers that have it.
    """
    bt_idx = _index_by_key(bt_matches, "bt")
    od_idx = _index_by_key(od_matches, "od")
    sp_idx = _index_by_key(sp_matches, "sp")

    all_keys = set(bt_idx) | set(od_idx) | set(sp_idx)
    results: list[dict] = []

    for key in all_keys:
        sources: dict[str, dict] = {}
        if key in bt_idx:
            sources["bt"] = bt_idx[key]
        if key in od_idx:
            sources["od"] = od_idx[key]
        if key in sp_idx:
            sources["sp"] = sp_idx[key]

        # Only include matches present in 2+ bookmakers
        if len(sources) < 2:
            continue

        # Pick the best representative match for metadata
        # Priority: SP > BT > OD (SP has best betradar_id coverage)
        rep = sources.get("sp") or sources.get("bt") or sources.get("od")

        home        = str(rep.get("home_team") or rep.get("home_team_name") or "")
        away        = str(rep.get("away_team") or rep.get("away_team_name") or "")
        start_time  = str(rep.get("start_time") or "")
        competition = str(rep.get("competition") or rep.get("competition_name") or "")
        betradar_id = str(
            rep.get("betradar_id") or
            (sources.get("sp") or {}).get("betradar_id") or
            (sources.get("bt") or {}).get("bt_parent_id") or
            ""
        )

        # Build per-bookmaker market data
        bookmakers: dict[str, dict] = {}
        markets_by_bk: dict[str, dict] = {}

        for bk_slug, match in sources.items():
            raw_mkts = match.get("markets") or {}

            # Normalise markets format — bt_harvester uses list, od/sp use dict
            if isinstance(raw_mkts, list):
                mkt_dict: dict[str, dict] = {}
                for mkt in raw_mkts:
                    slug = mkt.get("slug") or mkt.get("base_slug") or ""
                    if not slug:
                        continue
                    outcomes = {}
                    for o in (mkt.get("outcomes") or []):
                        if o.get("active") and o.get("odds", 0) > 1.0:
                            outcomes[o["key"]] = o["odds"]
                    if outcomes:
                        mkt_dict[slug] = outcomes
                raw_mkts = mkt_dict

            if not raw_mkts:
                continue

            bookmakers[bk_slug] = {
                "bookmaker": _BK_NAMES.get(bk_slug, bk_slug.upper()),
                "slug":      bk_slug,
                "markets":   raw_mkts,
                "market_count": len(raw_mkts),
            }
            markets_by_bk[bk_slug] = raw_mkts

        if len(bookmakers) < 2:
            continue

        # Build join_key — prefer betradar_id for SP matches
        if betradar_id:
            join_key = f"br_{betradar_id}"
        else:
            # Use BT parent_match_id if available
            bt_pid = str((sources.get("bt") or {}).get("bt_parent_id") or
                         (sources.get("bt") or {}).get("bt_match_id") or "")
            join_key = f"bt_{bt_pid}" if bt_pid else f"fuzzy_{_norm(home)}_{_norm(away)}"

        results.append({
            "join_key":       join_key,
            "betradar_id":    betradar_id or None,
            "home_team":      home,
            "away_team":      away,
            "competition":    competition,
            "sport":          sport_slug,
            "start_time":     start_time,
            "bookie_count":   len(bookmakers),
            "bookmakers":     bookmakers,
            "markets_by_bk":  markets_by_bk,
            "bk_slugs":       sorted(bookmakers.keys()),
            # persist helpers: dynamically lookup the right ID, fallback to shared betradar_id
            "bk_ids": {
                slug: str(
                    m.get(f"{slug}_match_id") or 
                    m.get(f"{slug}_game_id") or 
                    m.get(f"{slug}_event_id") or 
                    m.get(f"{slug}_parent_id") or 
                    m.get("match_id") or 
                    m.get("event_id") or 
                    betradar_id or 
                    ""
                )
                for slug, m in sources.items()
            },
        })

    logger.info(
        "[bt_od] %s: %d bt + %d od + %d sp → %d multi-bookie matches",
        sport_slug, len(bt_matches), len(od_matches), len(sp_matches), len(results),
    )
    return results


# =============================================================================
# PERSIST
# =============================================================================

def _persist_multi_bk(matches: list[dict], sport_slug: str) -> int:
    """
    Persist matched multi-bookmaker events via the same pipeline as persist_hook.py:
      1. Flatten to one record per (match × bookmaker)
      2. Sort records by join_key  ← prevents PostgreSQL deadlocks (same as persist_hook._sort_batch)
      3. Dispatch to tasks.ops.persist_combined_batch with random jitter  ← prevents thundering herd
      4. persist_from_serialized → EntityResolver.persist_batch does the actual DB write

    Format matches what _persist_bk_matches in tasks_upcoming.py sends,
    so EntityResolver already knows how to handle it.
    """
    if not matches:
        return 0

    _SPORT_SLUG_TO_DB: dict[str, str] = {
        "soccer": "Soccer", "basketball": "Basketball", "tennis": "Tennis",
        "ice-hockey": "Ice Hockey", "volleyball": "Volleyball", "cricket": "Cricket",
        "rugby": "Rugby", "table-tennis": "Table Tennis", "handball": "Handball",
        "mma": "MMA", "boxing": "Boxing", "darts": "Darts",
    }
    canonical_sport = _SPORT_SLUG_TO_DB.get(sport_slug, sport_slug)

    # ── Step 1: Flatten — one record per (match × bookmaker) ─────────────────
    serialized: list[dict] = []
    for m in matches:
        for bk_slug, mkts in (m.get("markets_by_bk") or {}).items():
            if not mkts:
                continue
            serialized.append({
                # join_key drives unified_match lookup in EntityResolver
                "join_key":       m["join_key"],
                "home_team":      m["home_team"],
                "away_team":      m["away_team"],
                "competition":    m["competition"],
                "start_time":     m["start_time"],
                "is_live":        False,
                "betradar_id":    m.get("betradar_id"),
                "sport":          canonical_sport,
                # bk_ids lets EntityResolver write the correct BookmakerMatchLink row
                "bk_ids":         {bk_slug: m["bk_ids"].get(bk_slug, "")},
                "markets":        mkts,
                "bookmaker_slug": bk_slug,
            })

    if not serialized:
        return 0

    # ── Step 2: Sort by join_key — same as persist_hook._sort_batch ──────────
    # Prevents PostgreSQL DeadlockDetected when multiple workers update the
    # same unified_match rows in different orders simultaneously.
    serialized.sort(key=lambda r: str(r.get("join_key") or r.get("betradar_id") or ""))

    # ── Step 3: Chunk + dispatch with jitter ─────────────────────────────────
    # Random countdown (2–6 s) mirrors persist_hook.persist_merged_async's
    # thundering-herd prevention when many sports finish at the same time.
    import random
    dispatched = 0
    for i in range(0, len(serialized), 500):
        chunk = serialized[i: i + 500]
        try:
            celery.send_task(
                "tasks.ops.persist_combined_batch",
                args=[chunk, sport_slug, "upcoming"],
                queue="results",
                countdown=random.uniform(2.0, 6.0),  # jitter — matches persist_hook
            )
            dispatched += len(chunk)
        except Exception as exc:
            logger.warning("[bt_od] persist dispatch failed: %s", exc)

    return dispatched


# =============================================================================
# CELERY TASKS
# =============================================================================

@celery.task(
    name="tasks.bt_od.harvest_sport",
    bind=True,
    max_retries=2,
    default_retry_delay=30,
    soft_time_limit=6000,
    time_limit=6600,
    acks_late=True,
)
def bt_od_harvest_sport(self, sport_slug: str) -> dict:
    """
    Fetch BT + OD for one sport concurrently, match events,
    persist only those appearing in 2+ bookmakers.
    """
    t0 = time.perf_counter()

    try:
        # Fetch BT and OD in parallel, SP from cache
        with ThreadPoolExecutor(max_workers=2) as pool:
            bt_fut = pool.submit(_fetch_bt_sport, sport_slug)
            od_fut = pool.submit(_fetch_od_sport, sport_slug)
            bt_matches = bt_fut.result()
            od_matches = od_fut.result()

        sp_matches = _fetch_sp_cache(sport_slug)

    except SoftTimeLimitExceeded as e:
        logger.error(f"Failed to load bookmakers: {e}")
        raise
    except Exception as exc:
        logger.error(f"Failed to load bookmakers: {e}")
        raise self.retry(exc=exc)

    # Match across bookmakers
    multi = _build_multi_bk_matches(bt_matches, od_matches, sp_matches, sport_slug)

    if not multi:
        logger.info("[bt_od] %s: no multi-bookie matches found", sport_slug)
        return {"ok": True, "sport": sport_slug, "matched": 0}

    # Cache for API layer
    cache_set(f"bt_od:upcoming:{sport_slug}", {
        "sport":       sport_slug,
        "match_count": len(multi),
        "bk_counts":   {"bt": len(bt_matches), "od": len(od_matches), "sp": len(sp_matches)},
        "harvested_at": _now_iso(),
        "matches":     multi,
    }, ttl=3600)

    # Persist
    dispatched = _persist_multi_bk(multi, sport_slug)

    latency = int((time.perf_counter() - t0) * 1000)
    _publish(WS_CHANNEL, {
        "event":   "odds_updated",
        "source":  "bt_od",
        "sport":   sport_slug,
        "count":   len(multi),
        "latency_ms": latency,
        "ts":      _now_iso(),
    })

    logger.info(
        "[bt_od] %s: matched=%d dispatched=%d latency=%dms",
        sport_slug, len(multi), dispatched, latency,
    )
    return {
        "ok":        True,
        "sport":     sport_slug,
        "matched":   len(multi),
        "bt_raw":    len(bt_matches),
        "od_raw":    len(od_matches),
        "sp_cache":  len(sp_matches),
        "dispatched": dispatched,
        "latency_ms": latency,
    }


@celery.task(
    name="tasks.bt_od.harvest_all",
    soft_time_limit=6000,
    time_limit=180,
)
def bt_od_harvest_all() -> dict:
    """Dispatch bt_od_harvest_sport for every sport in parallel."""
    sigs = [bt_od_harvest_sport.s(slug) for slug in ALL_SPORT_SLUGS]
    group(sigs).apply_async(queue="harvest")
    logger.info("[bt_od] dispatched %d sport tasks", len(sigs))
    return {"dispatched": len(sigs), "sports": ALL_SPORT_SLUGS}


# =============================================================================
# BEAT SCHEDULE ENTRY  (add to celery_app.py)
# =============================================================================
#
# "bt-od-harvest-all": {
#     "task":     "tasks.bt_od.harvest_all",
#     "schedule": crontab(minute="*/45"),   # every 45 min
#     "options":  {"queue": "harvest"},
# },