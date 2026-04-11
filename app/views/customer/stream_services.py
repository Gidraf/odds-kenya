import time
import json
from . import config
from .utils import _now_utc, _normalise_sport_slug, _is_upcoming_safe, _is_available, _sse, _keepalive
from .db_services import _build_base_query, _fetch_batch_data, _fetch_analytics_map
from .formatters import _build_match_dict

def _stream_matches(sport_slug, mode="upcoming", comp_filter="", team_filter="", has_arb=False, sort="start_time", date_str="", from_dt="", to_dt="", batch_size=config._STREAM_BATCH, include_analytics=False, listen_live=True):
    t0 = time.perf_counter()
    match_ids_for_redis = set()
    
    try:
        q     = _build_base_query(sport_slug, mode, comp_filter, team_filter, date_str, from_dt, to_dt, sort)
        total = q.count()
    except Exception as exc:
        yield _sse("error", {"error": str(exc)})
        return

    total_batches = max(1, (total + batch_size - 1) // batch_size)
    yield _sse("meta", {
        "total": total, "sport": _normalise_sport_slug(sport_slug), "mode": mode,
        "batch_size": batch_size, "total_batches": total_batches, "now": _now_utc().isoformat(),
        "live_window_minutes": int(config._LIVE_WINDOW.total_seconds() / 60), "analytics_included": include_analytics,
    })

    if total == 0:
        yield _sse("done", {"total_sent": 0, "latency_ms": 0})
        return

    offset = 0; total_sent = 0; batch_num = 0

    while offset < total:
        try:
            um_list = q.offset(offset).limit(batch_size).all()
            if not um_list: break

            match_ids = [um.id for um in um_list]
            bmo_rows, bk_objs, links_by_match, arb_set = _fetch_batch_data(match_ids)
            analytics_map = _fetch_analytics_map(um_list, include_analytics)

            bmo_by_match = {}
            for bmo in bmo_rows: bmo_by_match.setdefault(bmo.match_id, []).append(bmo)

            batch_matches = []
            for um in um_list:
                db_st = getattr(um, "status", None)
                if mode == "upcoming" and not _is_upcoming_safe(db_st, um.start_time): continue
                elif mode != "upcoming" and not _is_available(db_st, um.start_time): continue
                
                d = _build_match_dict(um, bmo_by_match.get(um.id, []), bk_objs, links_by_match, arb_set, sport_slug, analytics_map=analytics_map)
                if d["bk_count"] < config.MIN_BOOKMAKERS: continue
                if has_arb and not d["has_arb"]: continue
                    
                batch_matches.append(d)
                match_ids_for_redis.add(um.id) 

            batch_num  += 1
            total_sent += len(batch_matches)
            yield _sse("batch", {"matches": batch_matches, "batch": batch_num, "of": total_batches, "offset": offset})
            yield _keepalive()
            offset += batch_size

        except Exception as exc:
            yield _sse("error", {"error": str(exc), "offset": offset})
            break

    yield _sse("list_done", {"total_sent": total_sent, "batches": batch_num, "latency_ms": int((time.perf_counter() - t0) * 1000)})

    if not listen_live or not match_ids_for_redis:
        yield _sse("done", {"status": "finished"})
        return
        
    import redis as _rl
    from app.workers.celery_tasks import celery as _celery
    url  = _celery.conf.broker_url or "redis://localhost:6379/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    r    = _rl.Redis.from_url(f"{base}/2", decode_responses=True)
    ps   = r.pubsub()

    channels = [f"match:update:{mid}" for mid in match_ids_for_redis]
    ps.subscribe(*channels)

    last_hb = _now_utc()
    try:
        for msg in ps.listen():
            now_tz = _now_utc()
            if msg["type"] == "message":
                try: yield _sse("live_update", json.loads(msg["data"]))
                except json.JSONDecodeError: yield _sse("live_update", {"raw_data": msg["data"]})
            if (now_tz - last_hb).seconds >= 20:
                yield _keepalive()
                last_hb = now_tz
    except (GeneratorExit, Exception):
        ps.unsubscribe(*channels)

def _sse_stream(channel):
    import redis as _rl
    from app.workers.celery_tasks import celery as _celery
    url  = _celery.conf.broker_url or "redis://localhost:6379/0"
    base = url.rsplit("/", 1)[0] if url.count("/") >= 3 else url
    r    = _rl.Redis.from_url(f"{base}/2", decode_responses=True)
    ps   = r.pubsub()
    ps.subscribe(channel)
    last_hb = _now_utc()
    try:
        for msg in ps.listen():
            now = _now_utc()
            if msg["type"] == "message": yield f"data: {msg['data']}\n\n"
            if (now - last_hb).seconds >= 20:
                yield ": ping\n\n"
                last_hb = now
    except (GeneratorExit, Exception): ps.unsubscribe(channel)