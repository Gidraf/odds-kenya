"""
bk_streams.py — Per-bookmaker SSE stream endpoints.

Architecture
────────────
Frontend opens THREE independent EventSource connections simultaneously:
  /api/odds/stream/live/<sport>?bk=sp   ← SportPesa
  /api/odds/stream/live/<sport>?bk=bt   ← Betika
  /api/odds/stream/live/<sport>?bk=od   ← OdiBets

Each stream is fully independent:
  • No blocking on other bookmakers
  • SP appears in < 1 second (raw events, no market fetches)
  • BT and OD appear as their single HTTP call resolves (~2–5 s)
  • SP market updates trickle in as per-event futures resolve
  • Frontend mergeMatchLists() reconciles all three by parent_match_id + team names

SSE events emitted by every stream:
  meta       {"bk": "sp"|"bt"|"od", "sport": ..., "mode": ...}
  batch      {"bk": ..., "matches": [one match], "offset": n}
  live_update{"bk": ..., "parent_match_id": ..., "bookmakers": {...}, "markets_by_bk": {...}}
  list_done  {"bk": ..., "total": n}
  done       {"bk": ..., "status": "finished"}
  error      {"bk": ..., "error": "..."}

Upcoming mode:
  SP and BT both have true generators (yield one match at a time).
  OD upcoming is a blocking list call — it yields matches in one chunk
  after the API responds, but the UI already has SP+BT painted by then.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, Response, request, stream_with_context

from .utils import _now_utc, _normalise_sport_slug, _sse, _keepalive
from .routes_debug import _unify_match_payload, _merge_best, _normalize_market_slug

bp_bk_streams = Blueprint("bk_streams", __name__, url_prefix="/api/odds/stream")


# ── helpers ───────────────────────────────────────────────────────────────────

def _sport_id_for(sport_slug: str) -> int:
    try:
        from app.workers.sp_live_harvester import SPORT_SLUG_MAP
        return {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)
    except Exception:
        return 1


def _norm_key(h: str, a: str) -> str:
    """Short team-name key used for cross-bookmaker deduplication."""
    return f"{h[:8].lower().strip()}|{a[:8].lower().strip()}"


# ══════════════════════════════════════════════════════════════════════════════
# SP LIVE STREAM
# ══════════════════════════════════════════════════════════════════════════════

def _gen_sp_live(sport_slug: str):
    yield _sse("meta", {"bk": "sp", "sport": sport_slug, "mode": "live",
                        "now": _now_utc().isoformat()})
    try:
        from app.workers.sp_live_harvester import (
            fetch_live_events,
            fetch_live_markets,
            live_market_slug,
            normalize_live_outcome,
            SPORT_SLUG_MAP,
        )

        sport_id   = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)
        raw_events = fetch_live_events(sport_id, limit=100)
        if not raw_events:
            yield _sse("list_done", {"bk": "sp", "total": 0})
            yield _sse("done",      {"bk": "sp", "status": "no_events"})
            return

        # Build betradar → sp_event_id map for Phase 2
        betradar_map: dict[int, str] = {}
        count = 0

        # ── Phase 1: yield raw events immediately (no market fetches) ─────────
        for ev in raw_events:
            count += 1
            state  = ev.get("state") or {}
            score  = state.get("matchScore") or {}
            comps  = ev.get("competitors") or [{}, {}]
            br_id  = str(ev.get("externalId") or "")
            ev_id  = ev.get("id")

            if br_id and br_id not in ("0", "None"):
                betradar_map[ev_id] = br_id

            sp_raw = {
                "betradar_id":   br_id if br_id not in ("0","None","") else None,
                "home_team":     comps[0].get("name","Home") if comps else "Home",
                "away_team":     comps[1].get("name","Away") if len(comps)>1 else "Away",
                "competition":   (ev.get("tournament") or {}).get("name",""),
                "sport":         sport_slug,
                "start_time":    ev.get("kickoffTimeUTC",""),
                "match_time":    str(state.get("matchTime","")),
                "score_home":    str(score.get("home","")) or None,
                "score_away":    str(score.get("away","")) or None,
                "event_status":  state.get("currentEventPhase",""),
                "markets":       [],
            }
            card = _unify_match_payload(sp_raw, count, "live", "sp", "SPORTPESA")
            card["is_live"]    = True
            card["match_time"] = sp_raw["match_time"]
            card["score_home"] = sp_raw["score_home"]
            card["score_away"] = sp_raw["score_away"]
            card["bk"]         = "sp"

            yield _sse("batch", {"bk": "sp", "matches": [card],
                                 "offset": count - 1, "of": len(raw_events)})
            yield _keepalive()

        yield _sse("list_done", {"bk": "sp", "total": count})

        # ── Phase 2: fetch markets per event concurrently ─────────────────────
        _LIVE_MKT_TYPES = [194, 105, 138]   # 1x2, O/U, BTTS

        def _fetch_ev_mkts(ev_id: int) -> tuple[int, list]:
            collected = []
            for mtype in _LIVE_MKT_TYPES:
                try:
                    res = fetch_live_markets([ev_id], sport_id, mtype)
                    for m in res:
                        inner = m.get("markets") or []
                        if inner:
                            collected.extend(inner)
                except Exception:
                    pass
            return ev_id, collected

        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = {pool.submit(_fetch_ev_mkts, ev["id"]): ev["id"]
                    for ev in raw_events if ev.get("id") in betradar_map}

            for fut in as_completed(futs):
                ev_id = futs[fut]
                br_id = betradar_map.get(ev_id, "")
                if not br_id:
                    continue
                try:
                    _, raw_mkts = fut.result(timeout=8)
                except Exception:
                    continue
                if not raw_mkts:
                    continue

                bk_markets: dict = {}
                for mkt in raw_mkts:
                    if not isinstance(mkt, dict): continue
                    mkt_type = mkt.get("id") or mkt.get("typeId")
                    handicap = mkt.get("specValue") or mkt.get("handicap")
                    if not mkt_type: continue
                    slug = live_market_slug(int(mkt_type), handicap, sport_id)
                    for idx, sel in enumerate(mkt.get("selections") or []):
                        if not isinstance(sel, dict): continue
                        try: odd = float(sel.get("odds") or 0)
                        except: odd = 0.0
                        if odd <= 1.0: continue
                        ok = normalize_live_outcome(slug, sel.get("name",""), idx,
                                                    mkt.get("selections") or [])
                        bk_markets.setdefault(slug, {})[ok] = {"price": odd}

                if bk_markets:
                    yield _sse("live_update", {
                        "bk":              "sp",
                        "parent_match_id": br_id,
                        "home_team":       "",
                        "bookmakers":      {"sp": {"bookmaker":"SPORTPESA","slug":"sp",
                                                   "markets": bk_markets}},
                        "markets_by_bk":   {"sp": bk_markets},
                    })
                    yield _keepalive()

        yield _sse("done", {"bk": "sp", "status": "finished"})

    except Exception as exc:
        import traceback
        yield _sse("error", {"bk": "sp", "error": str(exc),
                              "traceback": traceback.format_exc()})


# ══════════════════════════════════════════════════════════════════════════════
# BT LIVE STREAM
# ══════════════════════════════════════════════════════════════════════════════

def _gen_bt_live(sport_slug: str):
    yield _sse("meta", {"bk": "bt", "sport": sport_slug, "mode": "live",
                        "now": _now_utc().isoformat()})
    try:
        from app.workers.bt_harvester import fetch_live_matches, slug_to_bt_sport_id

        matches = fetch_live_matches(slug_to_bt_sport_id(sport_slug)) or []
        count   = 0

        for m in matches:
            if not isinstance(m, dict): continue
            count += 1
            card  = _unify_match_payload(m, count, "live", "bt", "BETIKA")
            card["is_live"]    = True
            card["bk"]         = "bt"
            card["match_time"] = str(m.get("match_time") or "")
            card["score_home"] = m.get("score_home")
            card["score_away"] = m.get("score_away")
            yield _sse("batch", {"bk": "bt", "matches": [card],
                                 "offset": count - 1, "of": len(matches)})
            yield _keepalive()

        yield _sse("list_done", {"bk": "bt", "total": count})
        yield _sse("done",      {"bk": "bt", "status": "finished"})

    except Exception as exc:
        import traceback
        yield _sse("error", {"bk": "bt", "error": str(exc),
                              "traceback": traceback.format_exc()})


# ══════════════════════════════════════════════════════════════════════════════
# OD LIVE STREAM
# ══════════════════════════════════════════════════════════════════════════════

def _gen_od_live(sport_slug: str):
    """
    OD live: one HTTP call returns all live matches, then we stream them
    one card at a time so the frontend renders progressively.
    The single blocking call typically takes 2-4s — SP is already painted by then.
    """
    yield _sse("meta", {"bk": "od", "sport": sport_slug, "mode": "live",
                        "now": _now_utc().isoformat()})
    # Emit a status ping immediately so the frontend knows OD is loading
    yield _sse("status", {"bk": "od", "message": "Fetching OdiBets live data..."})
    try:
        from app.workers.od_harvester import fetch_live_matches

        matches = fetch_live_matches(sport_slug) or []
        count   = 0

        for m in matches:
            if not isinstance(m, dict): continue
            count += 1
            card  = _unify_match_payload(m, count, "live", "od", "ODIBETS")
            card["is_live"]    = True
            card["bk"]         = "od"
            card["match_time"] = str(m.get("match_time") or "")
            card["score_home"] = m.get("score_home")
            card["score_away"] = m.get("score_away")
            yield _sse("batch", {"bk": "od", "matches": [card],
                                 "offset": count - 1, "of": len(matches)})
            yield _keepalive()

        yield _sse("list_done", {"bk": "od", "total": count})
        yield _sse("done",      {"bk": "od", "status": "finished"})

    except Exception as exc:
        import traceback
        yield _sse("error", {"bk": "od", "error": str(exc),
                              "traceback": traceback.format_exc()})


# ══════════════════════════════════════════════════════════════════════════════
# SP UPCOMING STREAM
# ══════════════════════════════════════════════════════════════════════════════

def _gen_sp_upcoming(sport_slug: str, max_m: int = 50):
    yield _sse("meta", {"bk": "sp", "sport": sport_slug, "mode": "upcoming",
                        "now": _now_utc().isoformat()})
    try:
        from app.workers.sp_harvester import fetch_upcoming_stream

        count = 0
        for sp_match in fetch_upcoming_stream(sport_slug, max_matches=max_m,
                                              fetch_full_markets=True):
            if not isinstance(sp_match, dict): continue
            count += 1
            card = _unify_match_payload(sp_match, count, "upcoming", "sp", "SPORTPESA")
            card["bk"] = "sp"
            yield _sse("batch", {"bk": "sp", "matches": [card], "offset": count - 1})
            yield _keepalive()

        yield _sse("list_done", {"bk": "sp", "total": count})
        yield _sse("done",      {"bk": "sp", "status": "finished"})

    except Exception as exc:
        import traceback
        yield _sse("error", {"bk": "sp", "error": str(exc),
                              "traceback": traceback.format_exc()})


# ══════════════════════════════════════════════════════════════════════════════
# BT UPCOMING STREAM
# ══════════════════════════════════════════════════════════════════════════════

def _gen_bt_upcoming(sport_slug: str, max_m: int = 50):
    yield _sse("meta", {"bk": "bt", "sport": sport_slug, "mode": "upcoming",
                        "now": _now_utc().isoformat()})
    try:
        from app.workers.bt_harvester import fetch_upcoming_stream

        count = 0
        for m in fetch_upcoming_stream(sport_slug, max_matches=max_m,
                                       fetch_full_markets=True):
            if not isinstance(m, dict): continue
            count += 1
            card = _unify_match_payload(m, count, "upcoming", "bt", "BETIKA")
            card["bk"] = "bt"
            yield _sse("batch", {"bk": "bt", "matches": [card], "offset": count - 1})
            yield _keepalive()

        yield _sse("list_done", {"bk": "bt", "total": count})
        yield _sse("done",      {"bk": "bt", "status": "finished"})

    except Exception as exc:
        import traceback
        yield _sse("error", {"bk": "bt", "error": str(exc),
                              "traceback": traceback.format_exc()})


# ══════════════════════════════════════════════════════════════════════════════
# OD UPCOMING STREAM
# ══════════════════════════════════════════════════════════════════════════════

def _gen_od_upcoming(sport_slug: str, max_m: int = 50):
    """
    OD upcoming streams in two ways:
    - Today's matches: one fast API call, yields all at once
    - Tomorrow's matches: second call, yields when ready
    This gives the frontend OD data within ~3s while SP/BT stream in parallel.
    """
    yield _sse("meta", {"bk": "od", "sport": sport_slug, "mode": "upcoming",
                        "now": _now_utc().isoformat()})
    try:
        from app.workers.od_harvester import fetch_upcoming_matches
        from datetime import date, timedelta

        count = 0
        # Fetch today + tomorrow in separate calls so we can yield sooner
        for day_offset in range(3):
            day = (date.today() + timedelta(days=day_offset)).isoformat()
            try:
                matches = fetch_upcoming_matches(sport_slug, day=day,
                                                 max_matches=max_m) or []
            except Exception:
                continue

            for m in matches:
                if not isinstance(m, dict): continue
                if max_m and count >= max_m: break
                count += 1
                card = _unify_match_payload(m, count, "upcoming", "od", "ODIBETS")
                card["bk"] = "od"
                yield _sse("batch", {"bk": "od", "matches": [card],
                                     "offset": count - 1})
                yield _keepalive()

            if max_m and count >= max_m:
                break

        yield _sse("list_done", {"bk": "od", "total": count})
        yield _sse("done",      {"bk": "od", "status": "finished"})

    except Exception as exc:
        import traceback
        yield _sse("error", {"bk": "od", "error": str(exc),
                              "traceback": traceback.format_exc()})


# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════

_SSE_HEADERS = {
    "Content-Type":  "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection":    "keep-alive",
    "X-Accel-Buffering": "no",
}

_GENERATORS = {
    ("live",     "sp"): _gen_sp_live,
    ("live",     "bt"): _gen_bt_live,
    ("live",     "od"): _gen_od_live,
    ("upcoming", "sp"): _gen_sp_upcoming,
    ("upcoming", "bt"): _gen_bt_upcoming,
    ("upcoming", "od"): _gen_od_upcoming,
}


@bp_bk_streams.route("/live/<sport_slug>")
@bp_bk_streams.route("/upcoming/<sport_slug>")
def bk_stream(sport_slug: str):
    """
    Single endpoint, bookmaker selected via ?bk=sp|bt|od.

    Usage:
      GET /api/odds/stream/live/soccer?bk=sp
      GET /api/odds/stream/live/soccer?bk=bt
      GET /api/odds/stream/live/soccer?bk=od
      GET /api/odds/stream/upcoming/soccer?bk=sp
      ... etc.

    Frontend opens three EventSources in parallel and reconciles.
    """
    bk      = request.args.get("bk", "sp").lower().strip()
    max_m   = int(request.args.get("max", 50))
    mode    = "live" if "live" in request.path else "upcoming"
    gen_fn  = _GENERATORS.get((mode, bk))

    if not gen_fn:
        def _err():
            yield _sse("error", {"error": f"Unknown bk={bk!r}. Use sp, bt, or od."})
        return Response(stream_with_context(_err()), headers=_SSE_HEADERS)

    def _gen():
        yield from gen_fn(sport_slug, max_m) if mode == "upcoming" else gen_fn(sport_slug)

    return Response(stream_with_context(_gen()), headers=_SSE_HEADERS)