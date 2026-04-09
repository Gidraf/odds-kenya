"""
app/utils/persist_hook.py
=============================
Integration hooks that connect the existing combined_merger SSE streams
to the PostgreSQL persistence layer.

Call these after every successful merge cycle to save data to DB
while keeping the SSE stream fast (Redis cache is written first,
DB persistence happens async or in background).

Usage in combined_module.py:
────────────────────────────
    from app.utils.persist_hook import persist_merged_async

    # After merger produces combined list:
    combined = merger.merge(sp, bt, od, is_live=False)

    # Fire-and-forget DB persistence (Celery task)
    persist_merged_async(combined, sport_slug="soccer", mode="upcoming")

    # Or synchronous (blocks — use only if latency is acceptable):
    from app.utils.persist_hook import persist_merged_sync
    stats = persist_merged_sync(combined, sport_slug="soccer")
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def persist_merged_sync(
    combined_matches: list,
    sport_slug: str = "soccer",
) -> dict:
    """
    Synchronous persist — resolves entities and writes all matches to PostgreSQL.
    Returns {"persisted": N, "failed": N}.

    Use sparingly — this blocks the request thread.
    Best for Celery tasks or background workers.
    """
    try:
        from app.services.entity_resolver import EntityResolver
        
        resolver = EntityResolver()
        stats = resolver.persist_batch(combined_matches, commit=True)
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

    The SSE stream is NOT blocked — Redis cache was already written.
    """
    try:
        # Serialize to dicts for Celery JSON transport
        serialized = [cm.to_dict() for cm in combined_matches]

        from app.workers.celery_tasks import celery
        celery.send_task(
            "tasks.ops.persist_combined_batch",
            args=[serialized, sport_slug, mode],
            queue="results",
            countdown=2,  # slight delay to let Redis cache settle
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
    
    Thanks to the patched EntityResolver, we can pass the raw JSON dictionaries 
    directly into the resolver without needing to rebuild proxy objects!
    """
    from app.services.entity_resolver import EntityResolver

    # Ensure the sport_slug makes it into the dictionary so the resolver can find it
    for md in match_dicts:
        if "sport_slug" not in md and "sport" not in md:
            md["sport_slug"] = sport_slug

    resolver = EntityResolver()
    
    # We pass the raw dictionaries directly!
    stats = resolver.persist_batch(match_dicts, commit=True)
    
    logger.info(
        "persist_from_serialized [%s/%s]: persisted=%d failed=%d",
        sport_slug, mode, stats["persisted"], stats["failed"],
    )
    return stats