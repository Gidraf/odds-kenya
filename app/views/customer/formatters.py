from datetime import timezone
from .utils import _now_utc, _effective_status, _normalise_sport_slug, _flatten_db_markets, _bk_slug
from . import config

def _build_analytics_summary(analytics: dict) -> dict:
    if not analytics or not analytics.get("available"): return {"available": False}
    match = analytics.get("match") or {}
    teams = match.get("teams") or {}
    tournament = match.get("tournament") or {}
    t_info = tournament.get("tennisinfo") or {}
    tennis_info = {"surface": (match.get("ground") or {}).get("name"), "tournament_level": tournament.get("tournamentlevelname"), "prize_amount": (t_info.get("prize") or {}).get("amount"), "prize_currency": (t_info.get("prize") or {}).get("currency"), "gender": t_info.get("gender"), "event_type": t_info.get("type")} if t_info else None
    
    return {
        "available": True, "sr_match_id": analytics.get("sr_match_id"), "tournament": tournament.get("name"),
        "tournament_level": tournament.get("tournamentlevelname"), "season": (match.get("season") or {}).get("name"),
        "season_year": (match.get("season") or {}).get("year"), "stadium": (match.get("stadium") or {}).get("name"),
        "stadium_city": (match.get("stadium") or {}).get("city"), "stadium_country": (match.get("stadium") or {}).get("country"),
        "coverage_flags": [k for k, v in (match.get("statscoverage") or {}).items() if v is True],
        "home_uid": teams.get("home", {}).get("uid"), "away_uid": teams.get("away", {}).get("uid"),
        "home_next_count": len((analytics.get("home_next") or {}).get("matches") or []),
        "away_next_count": len((analytics.get("away_next") or {}).get("matches") or []),
        "tennis": tennis_info,
    }

def _build_analytics_full(analytics: dict) -> dict:
    if not analytics or not analytics.get("available"): return {"available": False}
    match = analytics.get("match") or {}
    t_info = (match.get("tournament") or {}).get("tennisinfo") or {}

    def _fmt_match(m: dict) -> dict:
        t = m.get("time") or {}; res = m.get("result") or {}
        return {"sr_match_id": m.get("_id"), "home": (m.get("teams") or {}).get("home", {}).get("name"), "away": (m.get("teams") or {}).get("away", {}).get("name"), "date": t.get("date"), "time": t.get("time"), "tz": t.get("tz"), "uts": t.get("uts"), "round": (m.get("roundname") or {}).get("name"), "surface": (m.get("ground") or {}).get("name"), "result": {"home": res.get("home"), "away": res.get("away"), "winner": res.get("winner")} if any(v is not None for v in [res.get("home"), res.get("away")]) else None, "status": m.get("status"), "canceled": m.get("canceled"), "postponed": m.get("postponed")}

    def _fmt_team(t: dict) -> dict: return {"uid": t.get("uid"), "name": t.get("name"), "abbr": t.get("abbr"), "country": (t.get("cc") or {}).get("name"), "cc_a2": (t.get("cc") or {}).get("a2")}

    return {
        "available": True, "sr_match_id": analytics.get("sr_match_id"),
        "tournament": {"name": (match.get("tournament") or {}).get("name"), "level": (match.get("tournament") or {}).get("tournamentlevelname"), "gender": t_info.get("gender"), "type": t_info.get("type"), "prize": {"amount": (t_info.get("prize") or {}).get("amount"), "currency": (t_info.get("prize") or {}).get("currency")} if t_info else None, "surface": ((match.get("tournament") or {}).get("ground") or {}).get("name"), "city": t_info.get("city"), "country": t_info.get("country")},
        "season": {"id": (match.get("season") or {}).get("_id"), "name": (match.get("season") or {}).get("name"), "year": (match.get("season") or {}).get("year")},
        "venue": {"name": (match.get("stadium") or {}).get("name"), "city": (match.get("stadium") or {}).get("city"), "country": (match.get("stadium") or {}).get("country"), "capacity": (match.get("stadium") or {}).get("capacity")},
        "surface": (match.get("ground") or {}).get("name"),
        "home_team": _fmt_team((match.get("teams") or {}).get("home") or {}), "away_team": _fmt_team((match.get("teams") or {}).get("away") or {}),
        "home_next": {"team": _fmt_team((analytics.get("home_next") or {}).get("team") or {}), "matches": [_fmt_match(m) for m in ((analytics.get("home_next") or {}).get("matches") or [])]},
        "away_next": {"team": _fmt_team((analytics.get("away_next") or {}).get("team") or {}), "matches": [_fmt_match(m) for m in ((analytics.get("away_next") or {}).get("matches") or [])]},
    }

def _build_match_dict(um, bmos, bk_objs, links_by_match, arb_set, sport_slug, analytics_map=None) -> dict:
    bookmakers = {}; markets_by_bk = {}

    for bmo in bmos:
        bk_obj = bk_objs.get(bmo.bookmaker_id)
        slug = _bk_slug((bk_obj.name if bk_obj else str(bmo.bookmaker_id)).lower())
        mkt_data = _flatten_db_markets(bmo.markets_json or {})
        if not mkt_data: continue
        
        if slug in bookmakers:
            bookmakers[slug]["markets"].update(mkt_data)
            bookmakers[slug]["market_count"] = len(bookmakers[slug]["markets"])
            markets_by_bk[slug].update(mkt_data)
        else:
            bookmakers[slug] = {"bookmaker_id": bmo.bookmaker_id, "bookmaker": bk_obj.name if bk_obj else slug.upper(), "slug": slug, "markets": mkt_data, "market_count": len(mkt_data), "link": links_by_match.get(um.id, {}).get(bmo.bookmaker_id)}
            markets_by_bk[slug] = mkt_data

    best = {}
    for sl, bk_mkts in markets_by_bk.items():
        for mkt, outcomes in bk_mkts.items():
            best.setdefault(mkt, {})
            for out, odd_data in (outcomes or {}).items():
                try: fv = float(odd_data.get("price") or odd_data.get("odd") or 0) if isinstance(odd_data, dict) else float(odd_data)
                except Exception: continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]): best[mkt][out] = {"odd": fv, "bk": sl}

    best_odds = {mkt: {out: {"odd": v["odd"], "bookie": v["bk"]} for out, v in outs.items()} for mkt, outs in best.items()}
    arb_markets = []
    if len(bookmakers) >= 2:
        for mkt, outcomes in best.items():
            if len(outcomes) < 2: continue
            arb_sum = sum(1.0 / v["odd"] for v in outcomes.values())
            if arb_sum < 1.0:
                profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
                legs = [{"outcome": o, "bk": v["bk"], "odd": v["odd"]} for o, v in outcomes.items()]
                breakdown = [{**leg, "stake_pct": round((1.0 / leg["odd"] / arb_sum) * 100, 3), "stake_kes": round(1000 * (1.0 / leg["odd"] / arb_sum), 2), "return_kes": round(1000 * (1.0 / leg["odd"] / arb_sum) * leg["odd"], 2)} for leg in legs]
                arb_markets.append({"market": mkt, "market_slug": mkt, "profit_pct": profit_pct, "arb_sum": round(arb_sum, 6), "legs": legs, "breakdown_1000": breakdown})
        arb_markets.sort(key=lambda x: -x["profit_pct"])

    status_out = _effective_status(getattr(um, "status", None), um.start_time)
    minutes_elapsed = int((_now_utc() - (um.start_time if um.start_time.tzinfo else um.start_time.replace(tzinfo=timezone.utc))).total_seconds() / 60) if um.start_time and status_out == "IN_PLAY" else None
    br_id = um.parent_match_id or ""

    analytics_summary = {"available": False}
    if analytics_map and br_id and br_id in analytics_map:
        analytics_summary = _build_analytics_summary(analytics_map[br_id])

    return {
        "match_id": um.id, "parent_match_id": br_id, "betradar_id": br_id, "join_key": f"br_{br_id}" if br_id else f"db_{um.id}",
        "home_team": um.home_team_name or "", "away_team": um.away_team_name or "", "competition": um.competition_name or "",
        "sport": _normalise_sport_slug(um.sport_name or sport_slug), "start_time": um.start_time.isoformat() if um.start_time else None,
        "status": status_out, "is_live": status_out == "IN_PLAY", "minutes_elapsed": minutes_elapsed,
        "bk_count": len(bookmakers), "bookie_count": len(bookmakers), "bookmaker_count": len(bookmakers),
        "market_count": len(best), "market_slugs": sorted(best.keys()),
        "bookmakers": bookmakers, "markets_by_bk": markets_by_bk, "markets": markets_by_bk,
        "best": best, "best_odds": best_odds, "aggregated": _flatten_db_markets(um.markets_json or {}),
        "has_arb": bool(arb_markets) or um.id in arb_set, "arb_markets": arb_markets, "arbs": arb_markets, "best_arb_pct": arb_markets[0]["profit_pct"] if arb_markets else 0.0,
        "has_ev": False, "evs": [], "has_sharp": False, "sharp": [], "best_ev_pct": 0.0,
        "bk_ids": {sl: str(d["bookmaker_id"]) for sl, d in bookmakers.items()},
        "has_analytics": analytics_summary.get("available", False), "analytics": analytics_summary, "source": "postgresql",
    }

def _build_envelope(matches, sport, mode, tier, page, per_page, truncated, latency_ms, total=None, pages=None, extra=None):
    bk_names = set()
    for m in matches: bk_names.update((m.get("bookmakers") or {}).keys())
    env = {
        "ok": True, "sport": _normalise_sport_slug(sport), "mode": mode, "tier": tier,
        "total": total if total is not None else len(matches), "page": page, "per_page": per_page, "pages": pages if pages is not None else max(1, ((total if total is not None else len(matches)) + per_page - 1) // per_page),
        "truncated": truncated, "latency_ms": latency_ms, "arb_count": sum(1 for m in matches if m.get("has_arb")),
        "live_count": sum(1 for m in matches if m.get("is_live")), "bookie_count": len(bk_names), "bookmakers": sorted(bk_names),
        "matches": matches, "source": "postgresql", "server_time": _now_utc().isoformat(), "min_bookmakers": config.MIN_BOOKMAKERS,
    }
    if truncated: env["upgrade_message"] = "Upgrade your plan to see all matches."
    if extra: env.update(extra)
    return env