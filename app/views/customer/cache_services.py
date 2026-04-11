from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from . import config
from .utils import _now_utc, _normalise_sport_slug, _effective_status, _bk_slug, _sse, _keepalive

def _read_cache_sources(mode, sport_slug):
    from app.workers.celery_tasks import cache_get, cache_keys
    canonical = _normalise_sport_slug(sport_slug)
    slugs_to_try = list({sport_slug.lower().replace(" ", "_").replace("-", "_"), canonical.replace("-", "_")})

    def _fetch_prefix(prefix, slug):
        c = cache_get(f"bt_od:upcoming:{slug}") if prefix == "bt_od" else cache_get(f"{prefix}:{mode}:{slug}")
        if c and c.get("matches"):
            for m in c["matches"]: m.setdefault("_cache_source", prefix)
            return list(c["matches"])
        return []

    raw = []
    work = [(p, s) for p in config._CACHE_PREFIXES for s in slugs_to_try]
    with ThreadPoolExecutor(max_workers=min(16, len(work) or 1)) as pool:
        for fut in as_completed({pool.submit(_fetch_prefix, p, s): (p, s) for p, s in work}):
            try: raw.extend(fut.result())
            except Exception: pass

    extra_keys = []
    for s in slugs_to_try: extra_keys += cache_keys(f"odds:{mode}:{s}:*") + cache_keys(f"odds:{mode}:{s}")
    if extra_keys:
        with ThreadPoolExecutor(max_workers=min(8, len(extra_keys))) as pool:
            for fut in as_completed({pool.submit(lambda k: list((cache_get(k) or {}).get("matches") or []), k): k for k in extra_keys}):
                try: raw.extend(fut.result())
                except Exception: pass
    return raw

def _deduplicate(matches):
    seen = set(); result = []
    for m in matches:
        key = f"{m.get('home_team','').lower().strip()}|{m.get('away_team','').lower().strip()}|{str(m.get('start_time',''))[:13]}"
        if key not in seen: seen.add(key); result.append(m)
    return result

def _normalise_cache_match(m: dict, mode: str = "upcoming") -> dict | None:
    home = str(m.get("home_team") or m.get("home_team_name") or "")
    away = str(m.get("away_team") or m.get("away_team_name") or "")
    start_dt = None
    raw_st = m.get("start_time")
    if raw_st:
        try: start_dt = datetime.fromisoformat(str(raw_st).replace("Z", "+00:00"))
        except Exception: pass
        
    db_status = m.get("status") or "PRE_MATCH"
    status_out = _effective_status(db_status, start_dt)

    if mode == "upcoming":
        if (db_status or "").upper() in config._EXCLUDE_FROM_UPCOMING: return None
        if not start_dt: return None
        st_utc = start_dt if start_dt.tzinfo else start_dt.replace(tzinfo=timezone.utc)
        if st_utc <= _now_utc(): return None
        if status_out != "PRE_MATCH": return None
            
    if mode == "live" and status_out in config._TERMINAL_STATUSES: return None
    if mode == "live" and status_out != "IN_PLAY": return None
    if mode == "finished" and status_out not in config._TERMINAL_STATUSES: return None

    live_flag = status_out == "IN_PLAY"
    minutes_elapsed = int((_now_utc() - (start_dt if start_dt.tzinfo else start_dt.replace(tzinfo=timezone.utc))).total_seconds() / 60) if start_dt and live_flag else None

    markets_by_bk = {}
    src = m.get("_cache_source", "")
    raw_mkts = m.get("markets") or m.get("markets_by_bk") or {} 

    if src == "bt_od":
        markets_by_bk = {_bk_slug(bk): v for bk, v in raw_mkts.items() if isinstance(v, dict)}
    elif src and isinstance(raw_mkts, dict):
        first_val = next(iter(raw_mkts.values()), None)
        if isinstance(first_val, dict):
            inner = next(iter(first_val.values()), None)
            markets_by_bk = ({src: raw_mkts} if isinstance(inner, (int, float)) else {_bk_slug(bk): v for bk, v in raw_mkts.items()})
        else: markets_by_bk[src] = raw_mkts
    elif isinstance(raw_mkts, dict): markets_by_bk = raw_mkts

    if mode in ("upcoming", "live") and len(markets_by_bk) < config.MIN_BOOKMAKERS: return None

    best = {}
    for sl, bk_mkts in markets_by_bk.items():
        for mkt, outcomes in (bk_mkts or {}).items():
            best.setdefault(mkt, {})
            for out, odd in (outcomes or {}).items():
                try: fv = float(odd)
                except Exception: continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]): best[mkt][out] = {"odd": fv, "bk": sl}

    slugs = sorted(best.keys())
    br_id = str(m.get("betradar_id") or m.get("parent_match_id") or "")

    return {
        "match_id": None, "parent_match_id": br_id, "betradar_id": br_id, "join_key": (f"br_{br_id}" if br_id else f"fuzzy_{home.lower()[:8]}_{away.lower()[:8]}"),
        "home_team": home, "away_team": away, "competition": str(m.get("competition") or m.get("competition_name") or ""),
        "sport": _normalise_sport_slug(str(m.get("sport") or "")), "start_time": raw_st, "status": status_out, "is_live": live_flag, "minutes_elapsed": minutes_elapsed,
        "bk_count": len(markets_by_bk), "bookie_count": len(markets_by_bk), "bookmaker_count": len(markets_by_bk), "market_count": len(slugs), "market_slugs": slugs,
        "bookmakers": {sl: {"bookmaker": sl.upper(), "slug": sl, "markets": mkts, "market_count": len(mkts), "link": None} for sl, mkts in markets_by_bk.items()},
        "markets_by_bk": markets_by_bk, "markets": markets_by_bk, "best": best,
        "best_odds": {mkt: {out: {"odd": v["odd"], "bookie": v["bk"]} for out, v in outs.items()} for mkt, outs in best.items()},
        "has_arb": False, "arb_markets": [], "arbs": [], "best_arb_pct": 0.0, "has_ev":  False, "evs": [], "has_sharp": False, "sharp": [], "best_ev_pct": 0.0,
        "bk_ids": {sl: sl for sl in markets_by_bk}, "has_analytics": False, "analytics": {"available": False}, "source": "cache",
    }

def _stream_from_cache(mode, sport_slug, batch_size=config._STREAM_BATCH):
    raw = _read_cache_sources(mode, sport_slug)
    matches = [x for m in _deduplicate(raw) if (x := _normalise_cache_match(m, mode)) is not None]
    total = len(matches)
    yield _sse("meta", {"total": total, "sport": _normalise_sport_slug(sport_slug), "mode": mode, "source": "cache", "now": _now_utc().isoformat()})
    for i in range(0, total, batch_size):
        yield _sse("batch", {"matches": matches[i: i + batch_size], "batch": i // batch_size + 1, "of": max(1, (total + batch_size - 1) // batch_size), "offset": i})
        yield _keepalive()
    yield _sse("done", {"total_sent": total, "source": "cache"})