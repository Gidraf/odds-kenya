"""
app/utils/persist_hook.py
=============================
Integration hooks that connect the existing combined_merger SSE streams
to the PostgreSQL persistence layer.

Call these after every successful merge cycle to save data to DB
while keeping the SSE stream fast (Redis cache is written first,
DB persistence happens async or in background).
"""

from __future__ import annotations

import logging
import random
from typing import Any

logger = logging.getLogger(__name__)


def _sort_batch(match_dicts: list[dict | Any]) -> list[dict | Any]:
    """
    Sorts a batch of matches by a deterministic key before DB operations.
    CRITICAL FIX: By ensuring all workers attempt to lock/update database rows 
    in the exact same order, we prevent PostgreSQL deadlocks.
    """
    def sort_key(m):
        if isinstance(m, dict):
            # Look for the universal betradar_id first, then platform-specific IDs, then fallback to string
            return str(
                m.get("betradar_id") or 
                m.get("parent_match_id") or 
                m.get("bt_parent_id") or 
                m.get("od_parent_id") or 
                m.get("home_team") or 
                ""
            )
        else:
            # If it's an object (e.g., CombinedMatch), use getattr
            return str(
                getattr(m, "betradar_id", None) or 
                getattr(m, "parent_match_id", None) or 
                getattr(m, "home_team", "")
            )

    return sorted(match_dicts, key=sort_key)


def persist_merged_sync(
    combined_matches: list,
    sport_slug: str = "soccer",
) -> dict:
    """
    Synchronous persist — resolves entities and writes all matches to PostgreSQL.
    """
    try:
        from app.utils.entity_resolver import EntityResolver
        
        # Sort to prevent deadlocks if multiple sync calls happen concurrently
        sorted_matches = _sort_batch(combined_matches)
        
        resolver = EntityResolver()
        stats = resolver.persist_batch(sorted_matches, commit=True)
        logger.info(
            "persist_merged_sync [%s]: persisted=%d failed=%d",
            sport_slug, stats["persisted"], stats["failed"],
        )
        return stats
    except Exception as exc:
        logger.error("persist_merged_sync error: %s", exc)
        return {"persisted": 0, "failed": len(combined_matches), "error": str(exc)}


def persist_merged_async(
    combined_matches: list,
    sport_slug: str = "soccer",
    mode: str = "upcoming",
) -> None:
    """
    Fire-and-forget: serializes CombinedMatch list and dispatches
    a Celery task to persist to DB in background.
    """
    try:
        # Serialize to dicts for Celery JSON transport
        serialized = [cm.to_dict() for cm in combined_matches]

        from app.workers.celery_tasks import celery
        
        # Add Jitter: Randomize the countdown between 2 and 6 seconds.
        # This prevents the "thundering herd" problem where multiple sports 
        # finish merging at the same time and hit the DB simultaneously.
        delay = random.uniform(2.0, 6.0)
        
        celery.send_task(
            "tasks.ops.persist_combined_batch",
            args=[serialized, sport_slug, mode],
            queue="results",
            countdown=delay,
        )
    except Exception as exc:
        logger.warning("persist_merged_async dispatch failed: %s", exc)


def persist_from_serialized(
    match_dicts: list[dict],
    sport_slug: str = "soccer",
    mode: str = "upcoming",
) -> dict:
    """
    Called by Celery task — takes serialized match dicts (from to_dict())
    and persists them to DB.
    """
    from app.utils.entity_resolver import EntityResolver

    # Ensure the sport_slug makes it into the dictionary so the resolver can find it
    for md in match_dicts:
        if "sport_slug" not in md and "sport" not in md:
            md["sport_slug"] = sport_slug

    # Sort the raw dictionaries to prevent DeadlockDetected errors during bulk UPSERTs
    sorted_match_dicts = _sort_batch(match_dicts)

    resolver = EntityResolver()
    
    # Pass the sorted, raw dictionaries directly into the resolver
    try:
        stats = resolver.persist_batch(sorted_match_dicts, commit=True)
        
        logger.info(
            "persist_from_serialized [%s/%s]: persisted=%d failed=%d",
            sport_slug, mode, stats["persisted"], stats["failed"],
        )
        return stats
        
    except Exception as e:
        # If we still hit a StaleDataError, log it cleanly so Celery can retry
        logger.error(f"❌ DB Commit Error in persist_from_serialized [{sport_slug}]: {str(e)}")
        raise e  # Reraise so Celery's @task(autoretry_for=(Exception,)) can catch it