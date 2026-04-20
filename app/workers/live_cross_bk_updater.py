"""
app/workers/live_cross_bk_updater.py
=======================================
Cross-bookmaker live enrichment engine triggered by SportPesa WebSocket.

Architecture
─────────────────────────────────────────────────────────────────────────────
  SP WS fires MARKET_UPDATE / EVENT_UPDATE
    → LiveCrossBkUpdater.trigger(betradar_id, sp_payload, sport_id, kind)
        ├─ Immediately: enrich(retry=0)
        │     • Fetch BT + OD markets in parallel (betradar_id as key)
        │     • Merge with SP payload already received
        │     • Diff vs Redis snapshot
        │     • Update BookmakerMatchOdds in DB
        │     • Write LiveRawSnapshot row (for future analytics)
        │     • Publish to all relevant Redis pub/sub channels
        │     • Always emit notification (SP confirmed a real change)
        │
        └─ Retries 1-5  (each 1 s apart, via threading.Timer)
              • Re-fetch BT + OD
              • Only publish / notify if the data ACTUALLY changed
                vs what is currently in the Redis snapshot
              • After 5 retries the enrichment cycle ends

Channel topology (all keyed by INTERNAL match_id where possible)
─────────────────────────────────────────────────────────────────
  live:sports                         — sport-level counts changed
  live:matches:{sport_slug}           — full match list for sport refreshed
  live:match:{match_id}:markets       — odds change for one match
  live:match:{match_id}:events        — score / phase / status change
  live:match:{match_id}:all           — merged feed (markets + events)
  live:all                            — global broadcast (every change)

Fallback when internal match_id is not yet in Redis:
  live:br:{betradar_id}:markets / events / all
  (frontend should subscribe to both until matched)

DB tables written
─────────────────
  BookmakerMatchOdds     — updated per-BK per-match
  BookmakerOddsHistory   — one row per price tick
  LiveRawSnapshot        — every enrichment result stored for analytics
"""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("live_cross_bk")

# ─── Constants ────────────────────────────────────────────────────────────────

MAX_RETRIES        = 5
RETRY_INTERVAL_S   = 1.0   # seconds between retries
FETCH_TIMEOUT_S    = 8.0
WORKER_THREADS     = 12

# Redis TTLs
_TTL_ID_MAP        = 3600   # betradar_id → match_id cache
_TTL_SNAP          = 7200   # odds snapshot
_TTL_MATCH_LIST    = 30     # live match list cache

# Pub/sub channels
CH_SPORTS           = "live:sports"
CH_MATCHES          = "live:matches:{sport_slug}"
CH_MATCH_MARKETS    = "live:match:{mid}:markets"
CH_MATCH_EVENTS     = "live:match:{mid}:events"
CH_MATCH_ALL        = "live:match:{mid}:all"
CH_ALL              = "live:all"
# Fallback channels keyed by betradar_id (used before internal id is resolved)
CH_BR_MARKETS       = "live:br:{br_id}:markets"
CH_BR_EVENTS        = "live:br:{br_id}:events"
CH_BR_ALL           = "live:br:{br_id}:all"

# ─── Redis helper ─────────────────────────────────────────────────────────────

_redis_client = None


def _r():
    global _redis_client
    if _redis_client is None:
        import os, redis
        url = os.getenv("REDIS_URL", "redis://localhost:6382/0")
        _redis_client = redis.from_url(url, decode_responses=True)
    return _redis_client


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── Internal match-id resolution ────────────────────────────────────────────

def _resolve_match_id(betradar_id: str) -> int | None:
    """
    betradar_id (= parent_match_id) → internal UnifiedMatch.id
    Result is cached in Redis for _TTL_ID_MAP seconds.
    """
    if not betradar_id:
        return None
    key = f"live:br2mid:{betradar_id}"
    cached = _r().get(key)
    if cached:
        try:
            return int(cached)
        except ValueError:
            pass
    try:
        from app.models.odds import UnifiedMatch
        um = UnifiedMatch.query.filter_by(parent_match_id=betradar_id).first()
        if um:
            _r().setex(key, _TTL_ID_MAP, str(um.id))
            return um.id
    except Exception as exc:
        logger.debug("resolve_match_id(%s): %s", betradar_id, exc)
    return None


def _resolve_sport_slug(betradar_id: str, sport_id: int | None) -> str:
    """Resolve sport slug from cached event or DB."""
    key = f"sp:live:event_slug:{betradar_id}"
    cached = _r().get(key)
    if cached:
        return cached
    if sport_id:
        from app.workers.sp_live_harvester import SPORT_SLUG_MAP
        slug = SPORT_SLUG_MAP.get(sport_id, "soccer")
        if slug:
            return slug
    try:
        from app.models.odds import UnifiedMatch
        from app.views.odds_feed.customer_odds_view import _normalise_sport_slug
        um = UnifiedMatch.query.filter_by(parent_match_id=betradar_id).first()
        if um and um.sport_name:
            return _normalise_sport_slug(um.sport_name)
    except Exception:
        pass
    return "soccer"


# ─── Bookmaker fetchers ───────────────────────────────────────────────────────

def _fetch_bt(betradar_id: str, sport_slug: str) -> tuple[dict, str]:
    """Fetch BT live markets. Returns (markets_dict, status)."""
    try:
        from app.workers.bt_harvester import get_full_markets, slug_to_bt_sport_id
        bt_sport_id = slug_to_bt_sport_id(sport_slug)
        mkts = get_full_markets(betradar_id, bt_sport_id)
        return (mkts or {}, "ok" if mkts else "no_markets")
    except Exception as exc:
        logger.debug("_fetch_bt(%s): %s", betradar_id, exc)
        return ({}, str(exc)[:80])


def _fetch_od(betradar_id: str, sport_slug: str) -> tuple[dict, str]:
    """Fetch OD live markets. Returns (markets_dict, status)."""
    try:
        from app.workers.od_harvester import fetch_event_detail, slug_to_od_sport_id
        od_sport_id = slug_to_od_sport_id(sport_slug)
        mkts, _meta = fetch_event_detail(betradar_id, od_sport_id)
        return (mkts or {}, "ok" if mkts else "no_markets")
    except Exception as exc:
        logger.debug("_fetch_od(%s): %s", betradar_id, exc)
        return ({}, str(exc)[:80])


# ─── Odds snapshot diff ───────────────────────────────────────────────────────

def _snap_key(mid_or_br: str, bk_slug: str) -> str:
    return f"live:snap:{mid_or_br}:{bk_slug}"


def _load_snap(key: str) -> dict:
    raw = _r().get(key)
    return json.loads(raw) if raw else {}


def _save_snap(key: str, markets: dict) -> None:
    _r().setex(key, _TTL_SNAP, json.dumps(markets, ensure_ascii=False))


def _diff_markets(old: dict, new: dict) -> dict:
    """Return only the markets/outcomes that changed price."""
    changed: dict = {}
    for mkt, outcomes in new.items():
        if not isinstance(outcomes, dict):
            continue
        for out, val in outcomes.items():
            try:
                new_price = float(val)
            except (TypeError, ValueError):
                continue
            old_price = None
            old_mkt = old.get(mkt, {})
            if isinstance(old_mkt, dict):
                old_raw = old_mkt.get(out)
                if old_raw is not None:
                    try:
                        old_price = float(old_raw)
                    except (TypeError, ValueError):
                        pass
            if old_price is None or abs(new_price - old_price) > 0.001:
                changed.setdefault(mkt, {})[out] = {
                    "new": new_price,
                    "old": old_price,
                    "delta": round(new_price - (old_price or 0), 4),
                }
    return changed


# ─── DB persistence ───────────────────────────────────────────────────────────

def _normalise_flat(markets: dict) -> dict:
    """Ensure {slug: {outcome: float}} — drop anything that isn't a valid price."""
    clean: dict = {}
    for mkt, outcomes in (markets or {}).items():
        if not isinstance(outcomes, dict):
            continue
        c: dict = {}
        for out, val in outcomes.items():
            if isinstance(val, dict):
                val = val.get("price") or val.get("odd") or val.get("odds") or 0
            try:
                fv = float(val)
            except (TypeError, ValueError):
                continue
            if fv > 1.0:
                c[out] = fv
        if c:
            clean[mkt] = c
    return clean


def _upsert_bk_odds(match_id: int, bookmaker_id: int, markets: dict) -> int:
    """
    Upsert BookmakerMatchOdds for one bookmaker. Returns number of selections written.
    Also writes BookmakerOddsHistory ticks for any price changes.
    """
    if not markets or not match_id or not bookmaker_id:
        return 0
    try:
        from app.extensions import db
        from app.models.odds import BookmakerMatchOdds, BookmakerOddsHistory
        bmo = (BookmakerMatchOdds.query
               .filter_by(match_id=match_id, bookmaker_id=bookmaker_id)
               .with_for_update(skip_locked=True)
               .first())
        if not bmo:
            bmo = BookmakerMatchOdds(match_id=match_id, bookmaker_id=bookmaker_id)
            db.session.add(bmo)
            db.session.flush()
        history: list[dict] = []
        count = 0
        for mkt_slug, outcomes in markets.items():
            for outcome, price in outcomes.items():
                try:
                    changed, old_price = bmo.upsert_selection(
                        market=mkt_slug, specifier=None,
                        selection=outcome, price=float(price),
                    )
                    count += 1
                    if changed:
                        history.append({
                            "bmo_id":       bmo.id,
                            "bookmaker_id": bookmaker_id,
                            "match_id":     match_id,
                            "market":       mkt_slug,
                            "specifier":    None,
                            "selection":    outcome,
                            "old_price":    old_price,
                            "new_price":    float(price),
                            "price_delta":  round(float(price) - (old_price or 0), 4),
                            "recorded_at":  datetime.utcnow(),
                        })
                except Exception:
                    pass
        if history:
            try:
                BookmakerOddsHistory.bulk_append(history)
            except Exception:
                pass
        return count
    except Exception as exc:
        logger.warning("_upsert_bk_odds match=%s bk=%s: %s", match_id, bookmaker_id, exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass
        return 0


def _get_bk_id_map() -> dict[str, int]:
    """Return {slug: bookmaker_id} from DB, cached in Redis 5 min."""
    key = "live:bk_id_map"
    cached = _r().get(key)
    if cached:
        try:
            return json.loads(cached)
        except Exception:
            pass
    try:
        from app.models.bookmakers_model import Bookmaker
        slug_map = {"sportpesa": "sp", "betika": "bt", "odibets": "od"}
        result: dict[str, int] = {}
        for bk in Bookmaker.query.all():
            slug = slug_map.get(bk.name.lower(), bk.name[:2].lower())
            result[slug] = bk.id
        _r().setex(key, 300, json.dumps(result))
        return result
    except Exception:
        return {}


# ─── LiveRawSnapshot persistence ─────────────────────────────────────────────

def _save_raw_snapshot(
    match_id: int | None,
    betradar_id: str,
    bk_slug: str,
    markets: dict,
    trigger: str,
    sport_slug: str,
    event_data: dict | None = None,
) -> None:
    """
    Persist one enrichment result to LiveRawSnapshot for future analytics.
    Silently swallowed if table doesn't exist yet (run migrations first).
    """
    if not markets and not event_data:
        return
    try:
        from app.extensions import db
        from app.models.live_snapshot_model import LiveRawSnapshot
        snap = LiveRawSnapshot(
            match_id     = match_id,
            betradar_id  = betradar_id,
            bk_slug      = bk_slug,
            sport_slug   = sport_slug,
            trigger      = trigger,
            markets_json = markets or {},
            event_json   = event_data or {},
            recorded_at  = datetime.utcnow(),
        )
        db.session.add(snap)
        db.session.commit()
    except Exception as exc:
        logger.debug("_save_raw_snapshot: %s", exc)
        try:
            from app.extensions import db
            db.session.rollback()
        except Exception:
            pass


# ─── Redis publish ────────────────────────────────────────────────────────────

def _publish_all(
    match_id: int | None,
    betradar_id: str,
    sport_slug: str,
    kind: str,           # "market_update" | "event_update"
    payload: dict,
) -> None:
    """
    Publish payload to all relevant channels.
    Uses internal match_id channels as primary, betradar fallback as secondary.
    """
    r = _r()
    msg = json.dumps(payload, ensure_ascii=False, default=str)
    channels: list[str] = [CH_ALL]

    if kind == "market_update":
        channels.append(CH_MATCHES.format(sport_slug=sport_slug))
        if match_id:
            channels += [
                CH_MATCH_MARKETS.format(mid=match_id),
                CH_MATCH_ALL.format(mid=match_id),
            ]
        channels += [
            CH_BR_MARKETS.format(br_id=betradar_id),
            CH_BR_ALL.format(br_id=betradar_id),
        ]
    elif kind == "event_update":
        channels.append(CH_MATCHES.format(sport_slug=sport_slug))
        if match_id:
            channels += [
                CH_MATCH_EVENTS.format(mid=match_id),
                CH_MATCH_ALL.format(mid=match_id),
            ]
        channels += [
            CH_BR_EVENTS.format(br_id=betradar_id),
            CH_BR_ALL.format(br_id=betradar_id),
        ]

    pipe = r.pipeline()
    for ch in channels:
        pipe.publish(ch, msg)
    pipe.execute()


def _publish_sports_update(sport_slug: str) -> None:
    """Publish lightweight sport-count refresh event."""
    _r().publish(CH_SPORTS, json.dumps({
        "type": "sport_update",
        "sport_slug": sport_slug,
        "ts": _now_iso(),
    }))


def _publish_match_list_refresh(match_id: int | None, betradar_id: str, sport_slug: str) -> None:
    """Publish a lightweight refresh hint for the match list."""
    _r().publish(CH_MATCHES.format(sport_slug=sport_slug), json.dumps({
        "type":       "match_refresh",
        "match_id":   match_id,
        "betradar_id": betradar_id,
        "sport_slug": sport_slug,
        "ts":         _now_iso(),
    }))


# ─── Build combined payload ────────────────────────────────────────────────────

def _build_market_payload(
    match_id: int | None,
    betradar_id: str,
    sport_slug: str,
    bk_slug: str,
    markets: dict,
    changed: dict,
    retry_num: int,
    sp_payload: dict,
) -> dict:
    return {
        "type":          "market_update",
        "source":        "cross_bk_enrich",
        "bk":            bk_slug,
        "match_id":      match_id,
        "betradar_id":   betradar_id,
        "sport_slug":    sport_slug,
        "retry":         retry_num,
        "markets":       markets,
        "changed":       changed,
        "changed_count": sum(len(v) for v in changed.values()),
        "sp_market_slug": sp_payload.get("market_slug"),
        "sp_market_type": sp_payload.get("market_type"),
        "sp_event_id":   sp_payload.get("event_id"),
        "ts":            _now_iso(),
    }


def _build_event_payload(
    match_id: int | None,
    betradar_id: str,
    sport_slug: str,
    sp_payload: dict,
    retry_num: int,
) -> dict:
    return {
        "type":          "event_update",
        "source":        "cross_bk_enrich",
        "match_id":      match_id,
        "betradar_id":   betradar_id,
        "sport_slug":    sport_slug,
        "retry":         retry_num,
        "phase":         sp_payload.get("phase"),
        "score_home":    sp_payload.get("score_home"),
        "score_away":    sp_payload.get("score_away"),
        "is_paused":     sp_payload.get("is_paused"),
        "clock_running": sp_payload.get("clock_running"),
        "remaining_ms":  sp_payload.get("remaining_ms"),
        "state":         sp_payload.get("state", {}),
        "ts":            _now_iso(),
    }


# ─── Core enrichment logic ────────────────────────────────────────────────────

def _do_enrich(
    betradar_id: str,
    sp_payload:  dict,
    sport_id:    int | None,
    retry_num:   int,
    kind:        str,    # "market_update" | "event_update"
) -> bool:
    """
    Run one enrichment cycle. Returns True if anything changed / was published.
    """
    sport_slug = _resolve_sport_slug(betradar_id, sport_id)
    match_id   = _resolve_match_id(betradar_id)
    bk_id_map  = _get_bk_id_map()
    published  = False

    # ── 1. Always re-publish the SP payload itself (normalised) ──────────────
    if kind == "market_update" and sp_payload.get("normalised_selections"):
        # Build a markets dict from the SP normalised selections
        slug = sp_payload.get("market_slug", "unknown")
        sp_markets: dict[str, dict] = {
            slug: {
                s["outcome_key"]: float(s["odds"])
                for s in sp_payload.get("normalised_selections", [])
                if s.get("outcome_key") and s.get("odds")
            }
        }
        sp_markets = _normalise_flat(sp_markets)

        # Diff and save SP snapshot
        sp_snap_key = _snap_key(match_id or betradar_id, "sp")
        old_sp = _load_snap(sp_snap_key)
        changed_sp = _diff_markets(old_sp, sp_markets)
        if changed_sp or retry_num == 0:
            _save_snap(sp_snap_key, {**old_sp, **sp_markets})
            if match_id and bk_id_map.get("sp"):
                _upsert_bk_odds(match_id, bk_id_map["sp"], sp_markets)
            _save_raw_snapshot(match_id, betradar_id, "sp", sp_markets,
                               f"sp_ws_r{retry_num}", sport_slug)
            sp_pub = _build_market_payload(
                match_id, betradar_id, sport_slug, "sp",
                sp_markets, changed_sp, retry_num, sp_payload,
            )
            _publish_all(match_id, betradar_id, sport_slug, "market_update", sp_pub)
            published = True

    elif kind == "event_update":
        ev_pub = _build_event_payload(match_id, betradar_id, sport_slug, sp_payload, retry_num)
        _publish_all(match_id, betradar_id, sport_slug, "event_update", ev_pub)
        _save_raw_snapshot(match_id, betradar_id, "sp", {}, f"sp_ws_r{retry_num}",
                           sport_slug, event_data=sp_payload)
        _publish_sports_update(sport_slug)
        published = True

    # ── 2. Parallel fetch BT + OD ─────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_bt = pool.submit(_fetch_bt, betradar_id, sport_slug)
        fut_od = pool.submit(_fetch_od, betradar_id, sport_slug)
        bt_raw, bt_status = fut_bt.result(timeout=FETCH_TIMEOUT_S)
        od_raw, od_status = fut_od.result(timeout=FETCH_TIMEOUT_S)

    for bk_slug, raw_mkts, status in [("bt", bt_raw, bt_status), ("od", od_raw, od_status)]:
        if not raw_mkts:
            logger.debug("enrich r%d %s %s: %s", retry_num, betradar_id, bk_slug, status)
            continue

        markets = _normalise_flat(raw_mkts)
        snap_key = _snap_key(match_id or betradar_id, bk_slug)
        old = _load_snap(snap_key)
        changed = _diff_markets(old, markets)

        # For retry_1+, only proceed if data actually changed
        if retry_num > 0 and not changed:
            continue

        _save_snap(snap_key, {**old, **markets})

        # DB write
        if match_id and bk_id_map.get(bk_slug):
            _upsert_bk_odds(match_id, bk_id_map[bk_slug], markets)
            try:
                from app.extensions import db
                db.session.commit()
            except Exception as exc:
                logger.warning("db commit %s r%d: %s", bk_slug, retry_num, exc)
                try:
                    from app.extensions import db
                    db.session.rollback()
                except Exception:
                    pass

        # Raw snapshot for analytics
        _save_raw_snapshot(match_id, betradar_id, bk_slug, markets,
                           f"cross_bk_r{retry_num}", sport_slug)

        # Publish
        pub = _build_market_payload(
            match_id, betradar_id, sport_slug, bk_slug,
            markets, changed, retry_num, sp_payload,
        )
        _publish_all(match_id, betradar_id, sport_slug, "market_update", pub)
        published = True

    if published:
        _publish_match_list_refresh(match_id, betradar_id, sport_slug)

    return published


# ─── Singleton updater ────────────────────────────────────────────────────────

class LiveCrossBkUpdater:
    """
    Singleton that receives SP WS trigger events and orchestrates
    cross-BK enrichment with retries.
    """

    def __init__(self) -> None:
        self._pool   = ThreadPoolExecutor(max_workers=WORKER_THREADS,
                                          thread_name_prefix="live-enrich")
        self._timers: dict[str, list[threading.Timer]] = {}
        self._lock   = threading.Lock()

    def trigger(
        self,
        betradar_id: str,
        sp_payload:  dict,
        sport_id:    int | None,
        kind:        str = "market_update",   # "market_update" | "event_update"
    ) -> None:
        """
        Called from the SP WS message handler (non-blocking).
        Dispatches immediate enrich + schedules 5 retries.
        """
        if not betradar_id:
            return

        # Cancel any outstanding retries for this match (debounce rapid WS events)
        self._cancel_retries(betradar_id)

        # Immediate enrichment in thread pool
        self._pool.submit(self._safe_enrich, betradar_id, sp_payload, sport_id, 0, kind)

        # Schedule retries
        timers: list[threading.Timer] = []
        for i in range(1, MAX_RETRIES + 1):
            t = threading.Timer(
                i * RETRY_INTERVAL_S,
                self._safe_enrich,
                args=[betradar_id, sp_payload, sport_id, i, kind],
            )
            t.daemon = True
            t.start()
            timers.append(t)

        with self._lock:
            self._timers[betradar_id] = timers

    def _safe_enrich(
        self,
        betradar_id: str,
        sp_payload:  dict,
        sport_id:    int | None,
        retry_num:   int,
        kind:        str,
    ) -> None:
        try:
            _do_enrich(betradar_id, sp_payload, sport_id, retry_num, kind)
        except Exception as exc:
            logger.error("enrich r%d %s: %s", retry_num, betradar_id, exc, exc_info=True)

    def _cancel_retries(self, betradar_id: str) -> None:
        with self._lock:
            for t in self._timers.pop(betradar_id, []):
                t.cancel()

    def shutdown(self) -> None:
        with self._lock:
            for timers in self._timers.values():
                for t in timers:
                    t.cancel()
            self._timers.clear()
        self._pool.shutdown(wait=False)


# ─── Singleton instance ───────────────────────────────────────────────────────

_updater: LiveCrossBkUpdater | None = None


def get_updater() -> LiveCrossBkUpdater:
    global _updater
    if _updater is None:
        _updater = LiveCrossBkUpdater()
        logger.info("LiveCrossBkUpdater initialised (workers=%d retries=%d)",
                    WORKER_THREADS, MAX_RETRIES)
    return _updater