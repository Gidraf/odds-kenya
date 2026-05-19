"""
app/workers/persistent_cache.py
================================
Three-layer data persistence — works with the EXISTING DB schema
(UnifiedMatch, BookmakerMatchOdds) already populated by
persist_hook.py → entity_resolver.py. No new migration required
for core functionality; odds_snapshot is a bonus speed layer.

Layer 1 — Redis (hot, extended TTL)
    smart_set() writes only when data changed (MD5 hash) and
    always refreshes TTL, so short-lived writes from
    tasks_upcoming._write_bk_keys() and odds_stream._get_unified_patched()
    don't expire the key — the prune task keeps it alive at 24h.

Layer 2 — odds_snapshot (key-value backup, optional)
    Stores exact Redis payloads. Created by run_migration() at startup.
    Written by _beat_db_backup every 15 min.
    Fastest cold-start restore.

Layer 3 — UnifiedMatch / BookmakerMatchOdds reconstruction
    Fallback when odds_snapshot is empty (first deploy / table gap).
    Reads from the tables that persist_hook already populates.

How it fits with the existing files (which are NOT modified):
─────────────────────────────────────────────────────────────
• tasks_upcoming._write_bk_keys()  → cache_set(..., ttl=3600)
  We don't touch it. The prune task refreshes TTL to 86400 every 30 min.

• odds_stream._get_unified_patched() → r.setex(unified_key, 3600, ...)
  Same — prune task keeps the key alive at 86400.

• persist_hook.persist_from_serialized() → EntityResolver.persist_batch()
  → UnifiedMatch, BookmakerMatchOdds already populated.
  hydrate_from_unified_match() reads them back on cold start.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any

log = logging.getLogger(__name__)

# ── TTL constants ─────────────────────────────────────────────────────────────

TTL_UNIFIED  = 86400   # 24h — unified + per-BK snapshot keys
TTL_BK_SNAP  = 86400   # 24h — individual BK snapshot keys
TTL_HASH     = 86400   # 24h — change-detection hash keys
TTL_LOW_WARN = 3600    # refresh a key when its TTL drops below this (1h)

# ── Sport slug ↔ display name (mirrors entity_resolver.SPORT_SLUG_MAP) ───────

_SPORT_SLUG_MAP: dict[str, str] = {
    "soccer": "Soccer", "football": "Soccer", "esoccer": "eSoccer",
    "basketball": "Basketball", "tennis": "Tennis",
    "ice-hockey": "Ice Hockey", "volleyball": "Volleyball",
    "cricket": "Cricket", "rugby": "Rugby", "table-tennis": "Table Tennis",
    "boxing": "Boxing", "handball": "Handball", "mma": "MMA",
    "darts": "Darts", "american-football": "American Football", "baseball": "Baseball",
}

_BK_SLUG_TO_NAME: dict[str, str] = {
    "sp": "SportPesa", "bt": "Betika", "od": "OdiBets",
    "1xbet": "1xBet", "22bet": "22Bet", "betwinner": "Betwinner",
    "melbet": "Melbet", "megapari": "Megapari", "helabet": "Helabet",
    "paripesa": "Paripesa",
}


# =============================================================================
# CHANGE DETECTION HELPERS
# =============================================================================

def _hash_key(redis_key: str) -> str:
    return f"chk:{redis_key}"


def _content_hash(data: Any) -> str:
    return hashlib.md5(
        json.dumps(data, sort_keys=True, default=str).encode()
    ).hexdigest()


def _count(data: Any) -> int:
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        ms = data.get("matches")
        return len(ms) if isinstance(ms, list) else len(data)
    return 0


# =============================================================================
# SMART SET
# =============================================================================

def smart_set(r, key: str, data: Any, ttl: int = TTL_UNIFIED, force: bool = False) -> bool:
    """
    Write data to Redis only if it has changed since last write.
    Always resets TTL to `ttl` seconds even when data is unchanged.

    Returns True if a write happened, False if skipped (no change).
    """
    new_hash = _content_hash(data)
    hk = _hash_key(key)
    try:
        old_hash = r.get(hk)
        if old_hash:
            if isinstance(old_hash, bytes):
                old_hash = old_hash.decode()
            if old_hash == new_hash and not force:
                pipe = r.pipeline()
                pipe.expire(key, ttl)
                pipe.expire(hk, TTL_HASH)
                pipe.execute()
                return False
        pipe = r.pipeline()
        pipe.set(key, json.dumps(data, default=str), ex=ttl)
        pipe.set(hk, new_hash, ex=TTL_HASH)
        pipe.execute()
        return True
    except Exception as exc:
        log.warning("[smart_set] %s: %s", key, exc)
        try:
            r.set(key, json.dumps(data, default=str), ex=ttl)
        except Exception:
            pass
        return True


# =============================================================================
# BULK TTL REFRESH  (used by _beat_prune in tasks_ops.py)
# =============================================================================

def refresh_all_ttls(r) -> int:
    """
    Scan all odds:* and {bk}:upcoming:* keys.
    For any key whose TTL is missing or below TTL_LOW_WARN, reset it to
    the target long TTL so data never silently disappears.

    This compensates for the hard-coded setex(3600) in:
      • tasks_upcoming._write_bk_keys() via cache_set
      • odds_stream._get_unified_patched()
    """
    patterns = [
        # Keys written by tasks_ops._merge_bk_into_unified / publish_bk_snapshot
        ("odds:unified:*",   TTL_UNIFIED),
        ("odds:b2b:*",       TTL_BK_SNAP),
        ("odds:sp:*",        TTL_BK_SNAP),
        ("odds:bt:*",        TTL_BK_SNAP),
        ("odds:od:*",        TTL_BK_SNAP),
        ("odds:1xbet:*",     TTL_BK_SNAP),
        ("odds:22bet:*",     TTL_BK_SNAP),
        ("odds:betwinner:*", TTL_BK_SNAP),
        ("odds:melbet:*",    TTL_BK_SNAP),
        ("odds:megapari:*",  TTL_BK_SNAP),
        ("odds:helabet:*",   TTL_BK_SNAP),
        ("odds:paripesa:*",  TTL_BK_SNAP),
        # Keys written by tasks_upcoming._write_bk_keys via cache_set
        ("sp:upcoming:*",    TTL_BK_SNAP),
        ("bt:upcoming:*",    TTL_BK_SNAP),
        ("od:upcoming:*",    TTL_BK_SNAP),
        ("b2b:upcoming:*",   TTL_BK_SNAP),
        # Analytics / EV-arb
        ("sr:analytics:*",   43200),
        ("chk:*",            TTL_HASH),
    ]
    refreshed = 0
    for pat, desired_ttl in patterns:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match=pat, count=300)
            for k in keys:
                try:
                    current = r.ttl(k)
                    if current == -1 or (0 < current < TTL_LOW_WARN):
                        r.expire(k, desired_ttl)
                        refreshed += 1
                except Exception:
                    pass
            if cursor == 0:
                break
    return refreshed


# =============================================================================
# COLD-START HYDRATION — PRIMARY: UnifiedMatch reconstruction
# =============================================================================

def hydrate_from_unified_match(r) -> int:
    """
    Reconstruct Redis keys from the UnifiedMatch + BookmakerMatchOdds
    tables that persist_hook.py already writes to.

    Only writes to Redis keys that don't already exist — never overwrites
    fresh data that harvesters just populated.

    Returns the number of Redis keys written.
    """
    n = 0
    try:
        from datetime import datetime, timezone, timedelta
        from app.extensions import db
        from app.models.odds import UnifiedMatch, BookmakerMatchOdds
        from app.models.bookmakers_model import Bookmaker

        now           = datetime.now(timezone.utc)
        cutoff_past   = now - timedelta(hours=3)
        cutoff_future = now + timedelta(days=30)

        rows = db.session.execute(
            db.select(UnifiedMatch).where(
                UnifiedMatch.start_time >= cutoff_past,
                UnifiedMatch.start_time <= cutoff_future,
            ).order_by(UnifiedMatch.start_time)
        ).scalars().all()

        if not rows:
            log.info("[hydrate] No UnifiedMatch rows in the next 30 days")
            return 0

        # Build bookmaker id → slug lookup
        bk_id_to_slug: dict[int, str] = {}
        try:
            bks = db.session.execute(db.select(Bookmaker)).scalars().all()
            for bk in bks:
                slug = getattr(bk, "slug", None) or bk.name.lower().replace(" ", "")[:8]
                bk_id_to_slug[bk.id] = slug
        except Exception as exc:
            log.warning("[hydrate] bookmaker lookup: %s", exc)

        # Group match dicts by sport slug
        by_sport: dict[str, list[dict]] = {}

        for um in rows:
            sport_slug = _reverse_sport_slug(um.sport_name or "soccer")
            bk_markets: dict[str, dict] = {}

            # Per-bookmaker odds from BookmakerMatchOdds rows
            try:
                bmos = BookmakerMatchOdds.query.filter_by(match_id=um.id).all()
                for bmo in bmos:
                    bk_slug = bk_id_to_slug.get(bmo.bookmaker_id, f"bk{bmo.bookmaker_id}")
                    mkt_data = _extract_bmo_markets(bmo)
                    if mkt_data:
                        bk_markets[bk_slug] = {
                            "bookmaker": _BK_SLUG_TO_NAME.get(bk_slug, bk_slug.upper()),
                            "slug":      bk_slug,
                            "markets":   mkt_data,
                        }
            except Exception:
                pass

            # Top-level markets from bookmaker_prices JSON column if available
            top_markets: dict = {}
            bp = getattr(um, "bookmaker_prices", None)
            if bp:
                if isinstance(bp, str):
                    try:
                        bp = json.loads(bp)
                    except Exception:
                        bp = {}
                if isinstance(bp, dict):
                    top_markets = bp

            match_dict = {
                "match_id":        str(um.parent_match_id or um.id),
                "parent_match_id": str(um.parent_match_id or um.id),
                "join_key":        str(um.parent_match_id or um.id),
                "betradar_id":     str(um.parent_match_id or ""),
                "home_team":       um.home_team_name or "",
                "away_team":       um.away_team_name or "",
                "competition":     um.competition_name or "",
                "sport":           sport_slug,
                "start_time":      um.start_time.isoformat() if um.start_time else "",
                "status":          getattr(um, "status", "PRE_MATCH") or "PRE_MATCH",
                "is_live":         False,
                "markets":         top_markets,
                "bookmakers":      bk_markets,
                "market_count":    len(top_markets),
                "bk_count":        len(bk_markets),
            }
            by_sport.setdefault(sport_slug, []).append(match_dict)

        # Write Redis keys per sport
        for sport_slug, matches in by_sport.items():
            if not matches:
                continue

            # Unified key — the main key _get_unified_patched reads
            unified_key = f"odds:unified:upcoming:{sport_slug}"
            if not r.exists(unified_key):
                smart_set(r, unified_key, {
                    "mode":        "upcoming",
                    "sport":       sport_slug,
                    "source":      "db_hydration",
                    "match_count": len(matches),
                    "matches":     matches,
                    "updated_at":  time.time(),
                }, ttl=TTL_UNIFIED)
                n += 1

            # Per-BK keys — what _merge_bks reads when unified cache misses
            bk_groups: dict[str, list[dict]] = {}
            for m in matches:
                for bk_slug in (m.get("bookmakers") or {}).keys():
                    bk_groups.setdefault(bk_slug, []).append(m)

            for bk_slug, bk_matches in bk_groups.items():
                bk_payload = {
                    "bk":          bk_slug,
                    "source":      _BK_SLUG_TO_NAME.get(bk_slug, bk_slug),
                    "sport":       sport_slug,
                    "mode":        "upcoming",
                    "match_count": len(bk_matches),
                    "matches":     bk_matches,
                    "updated_at":  time.time(),
                }
                for key_tmpl in [
                    f"{bk_slug}:upcoming:{sport_slug}",
                    f"odds:{bk_slug}:upcoming:{sport_slug}",
                ]:
                    if not r.exists(key_tmpl):
                        smart_set(r, key_tmpl, bk_payload, ttl=TTL_BK_SNAP)
                        n += 1

        log.info("[hydrate] wrote %d Redis keys from %d DB rows across %d sports",
                 n, len(rows), len(by_sport))

    except Exception as exc:
        log.warning("[hydrate_from_unified_match] %s", exc)

    return n


def _reverse_sport_slug(sport_name: str) -> str:
    """'Ice Hockey' → 'ice-hockey', 'Soccer' → 'soccer'"""
    _rev = {v.lower(): k for k, v in _SPORT_SLUG_MAP.items()}
    return _rev.get(sport_name.lower(), sport_name.lower().replace(" ", "-"))


def _extract_bmo_markets(bmo) -> dict:
    """
    Pull market/odds data from a BookmakerMatchOdds ORM object.
    Tries several common column/attribute shapes. Adapt if your model
    stores selections differently.
    """
    # Try JSON blob columns first
    for attr in ("odds_data", "markets", "selections", "data", "best_odds"):
        raw = getattr(bmo, attr, None)
        if raw:
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:
                    continue
            if isinstance(raw, dict):
                return raw

    # Fall back to iterating individual OddsSelection child rows
    markets: dict = {}
    for sel in (getattr(bmo, "odds_selections", None) or []):
        mkt   = getattr(sel, "market", "") or ""
        out   = getattr(sel, "selection", "") or ""
        price = getattr(sel, "price", 0) or 0
        try:
            price = float(price)
        except Exception:
            continue
        if mkt and out and price > 1.0:
            markets.setdefault(mkt, {})[out] = price

    return markets


# =============================================================================
# ODDS SNAPSHOT — fast key-value backup layer (optional)
# =============================================================================

MIGRATION_SQL = """
CREATE TABLE IF NOT EXISTS odds_snapshot (
    id           SERIAL PRIMARY KEY,
    redis_key    TEXT NOT NULL UNIQUE,
    payload      TEXT NOT NULL,
    content_hash TEXT NOT NULL DEFAULT '',
    record_count INTEGER DEFAULT 0,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_odds_snapshot_key     ON odds_snapshot (redis_key);
CREATE INDEX IF NOT EXISTS idx_odds_snapshot_updated ON odds_snapshot (updated_at DESC);
"""


def run_migration() -> bool:
    """Create odds_snapshot table if it doesn't exist. Safe to call every startup."""
    try:
        from app.extensions import db
        from sqlalchemy import text
        with db.engine.connect() as conn:
            conn.execute(text(MIGRATION_SQL))
            conn.commit()
        return True
    except Exception as exc:
        log.debug("[migration] odds_snapshot: %s", exc)
        return False


def snapshot_to_db(key: str, data: Any) -> bool:
    """Save a Redis key's payload to odds_snapshot. Only writes if content changed."""
    try:
        from app.extensions import db
        from sqlalchemy import text
        payload = json.dumps(data, default=str)
        chash   = _content_hash(data)
        with db.engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO odds_snapshot
                    (redis_key, payload, content_hash, record_count, updated_at)
                VALUES (:k, :p, :h, :c, NOW())
                ON CONFLICT (redis_key) DO UPDATE SET
                    payload      = EXCLUDED.payload,
                    content_hash = EXCLUDED.content_hash,
                    record_count = EXCLUDED.record_count,
                    updated_at   = NOW()
                WHERE odds_snapshot.content_hash != EXCLUDED.content_hash
            """), {"k": key, "p": payload, "h": chash, "c": _count(data)})
            conn.commit()
        return True
    except Exception as exc:
        log.debug("[snapshot_to_db] %s: %s", key, exc)
        return False


def hydrate_from_snapshot(r) -> int:
    """Restore Redis keys from odds_snapshot. Skips keys already in Redis."""
    n = 0
    try:
        from app.extensions import db
        from sqlalchemy import text
        with db.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT redis_key, payload FROM odds_snapshot ORDER BY updated_at DESC")
            ).fetchall()
        for redis_key, payload in rows:
            try:
                if r.exists(redis_key):
                    if r.ttl(redis_key) < TTL_LOW_WARN:
                        r.expire(redis_key, TTL_UNIFIED)
                    continue
                r.set(redis_key, payload, ex=TTL_UNIFIED)
                n += 1
            except Exception:
                pass
        log.info("[hydrate_snapshot] restored %d/%d keys", n, len(rows))
    except Exception as exc:
        log.debug("[hydrate_snapshot] %s", exc)
    return n


def backup_redis_to_snapshot(r) -> dict:
    """Snapshot all unified + BK-level Redis keys to odds_snapshot."""
    patterns = [
        "odds:unified:*",
        "odds:sp:*", "odds:bt:*", "odds:od:*",
        "odds:1xbet:*", "odds:22bet:*", "odds:betwinner:*",
        "odds:melbet:*", "odds:megapari:*", "odds:helabet:*", "odds:paripesa:*",
    ]
    written = skipped = errors = 0
    for pat in patterns:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match=pat, count=200)
            for k in keys:
                rk = k.decode() if isinstance(k, bytes) else k
                if rk.startswith("chk:"):
                    continue
                try:
                    raw = r.get(rk)
                    if not raw:
                        continue
                    ok = snapshot_to_db(rk, json.loads(raw))
                    written += ok
                    skipped += (not ok)
                except Exception as exc:
                    log.debug("[backup] %s: %s", rk, exc)
                    errors += 1
            if cursor == 0:
                break
    log.info("[backup_redis] written=%d skipped=%d errors=%d", written, skipped, errors)
    return {"written": written, "skipped": skipped, "errors": errors}


# =============================================================================
# COMBINED STARTUP HYDRATION
# =============================================================================

def startup_hydrate(r) -> int:
    """
    Main entry point called once at worker startup (on_worker_ready).

    Order:
    1. run_migration() — ensure odds_snapshot table exists (no-op if present)
    2. hydrate_from_snapshot() — fast path: exact payloads from backup table
    3. hydrate_from_unified_match() — fallback: reconstruct from existing DB

    Returns total keys written to Redis.
    """
    run_migration()
    n = hydrate_from_snapshot(r)
    if n == 0:
        n = hydrate_from_unified_match(r)
    return n