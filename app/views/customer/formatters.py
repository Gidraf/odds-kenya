from .utils import _now_utc, _effective_status, _normalise_sport_slug, _flatten_db_markets, _bk_slug
from . import config
from datetime import timezone

def _build_analytics_summary(analytics: dict) -> dict:
    if not analytics or not analytics.get("available"):
        return {"available": False}
    match      = analytics.get("match") or {}
    teams      = match.get("teams") or {}
    home_t     = teams.get("home") or {}
    away_t     = teams.get("away") or {}
    tournament = match.get("tournament") or {}
    season     = match.get("season") or {}
    stadium    = match.get("stadium") or {}
    coverage   = match.get("statscoverage") or {}
    home_next  = analytics.get("home_next") or {}
    away_next  = analytics.get("away_next") or {}

    tennis_info: dict = {}
    t_info = tournament.get("tennisinfo") or {}
    if t_info:
        prize = t_info.get("prize") or {}
        tennis_info = {
            "surface":         (match.get("ground") or {}).get("name"),
            "tournament_level": tournament.get("tournamentlevelname"),
            "prize_amount":  prize.get("amount"),
            "prize_currency": prize.get("currency"),
            "gender":         t_info.get("gender"),
            "event_type":    t_info.get("type"),
        }
    active_coverage = [k for k, v in coverage.items() if v is True]

    return {
        "available":           True,
        "sr_match_id":         analytics.get("sr_match_id"),
        "tournament":          tournament.get("name"),
        "tournament_level":    tournament.get("tournamentlevelname"),
        "season":              season.get("name"),
        "season_year":         season.get("year"),
        "stadium":             stadium.get("name"),
        "stadium_city":        stadium.get("city"),
        "stadium_country":     stadium.get("country"),
        "coverage_flags":      active_coverage,
        "home_uid":            home_t.get("uid"),
        "away_uid":            away_t.get("uid"),
        "home_next_count":     len(home_next.get("matches") or []),
        "away_next_count":     len(away_next.get("matches") or []),
        "tennis":              tennis_info if tennis_info else None,
    }

def _build_analytics_full(analytics: dict) -> dict:
    if not analytics or not analytics.get("available"): return {"available": False}
    match      = analytics.get("match") or {}
    home_next  = analytics.get("home_next") or {}
    away_next  = analytics.get("away_next") or {}
    sf         = analytics.get("season_fixtures") or {}
    teams      = match.get("teams") or {}
    tournament = match.get("tournament") or {}
    season     = match.get("season") or {}
    stadium    = match.get("stadium") or {}
    coverage   = match.get("statscoverage") or {}
    t_info     = tournament.get("tennisinfo") or {}

    def _fmt_match(m: dict) -> dict:
        home = (m.get("teams") or {}).get("home") or {}
        away = (m.get("teams") or {}).get("away") or {}
        t    = m.get("time") or {}
        rn   = m.get("roundname") or {}
        g    = m.get("ground") or {}
        res  = m.get("result") or {}
        return {
            "sr_match_id": m.get("_id"), "home": home.get("name"), "away": away.get("name"),
            "date": t.get("date"), "time": t.get("time"), "tz": t.get("tz"), "uts": t.get("uts"),
            "round": rn.get("name"), "surface": g.get("name"),
            "result": {"home": res.get("home"), "away": res.get("away"), "winner": res.get("winner")} if any(v is not None for v in [res.get("home"), res.get("away")]) else None,
            "status": m.get("status"), "canceled": m.get("canceled"), "postponed": m.get("postponed"),
            "walkover": m.get("walkover"), "retired": m.get("retired"), "bestof": m.get("bestof"),
        }

    def _fmt_team(t: dict) -> dict:
        cc = t.get("cc") or {}
        return {"uid": t.get("uid"), "name": t.get("name"), "abbr": t.get("abbr"), "country": cc.get("name"), "cc_a2": cc.get("a2")}

    prize_info = None
    if t_info:
        prize = t_info.get("prize") or {}
        prize_info = {"amount": prize.get("amount"), "currency": prize.get("currency")}

    return {
        "available":       True, "sr_match_id": analytics.get("sr_match_id"),
        "tournament": {
            "name": tournament.get("name"), "level": tournament.get("tournamentlevelname"),
            "gender": t_info.get("gender"), "type": t_info.get("type"), "prize": prize_info,
            "surface": (tournament.get("ground") or {}).get("name"), "start": t_info.get("start"),
            "end": t_info.get("end"), "qualification": t_info.get("qualification"),
            "city": t_info.get("city"), "country": t_info.get("country"),
        },
        "season": {"id": season.get("_id"), "name": season.get("name"), "year": season.get("year"), "start": (season.get("start") or {}).get("date"), "end": (season.get("end") or {}).get("date")},
        "venue": {"name": stadium.get("name"), "city": stadium.get("city"), "country": stadium.get("country"), "capacity": stadium.get("capacity")},
        "surface": (match.get("ground") or {}).get("name"),
        "home_team": _fmt_team(teams.get("home") or {}), "away_team": _fmt_team(teams.get("away") or {}),
        "coverage": {k: v for k, v in coverage.items() if isinstance(v, bool)},
        "home_next": {"team": _fmt_team(home_next.get("team") or {}), "matches": [_fmt_match(m) for m in (home_next.get("matches") or [])]},
        "away_next": {"team": _fmt_team(away_next.get("team") or {}), "matches": [_fmt_match(m) for m in (away_next.get("matches") or [])]},
        "season_fixtures": {"total": len(sf.get("matches") or []), "matches": [_fmt_match(m) for m in (sf.get("matches") or [])], "cups": list((sf.get("cups") or {}).keys())} if sf else None,
    }

def _build_match_dict(um, bmos, bk_objs, links_by_match, arb_set, sport_slug, analytics_map=None) -> dict:
    bookmakers:    dict[str, dict] = {}
    markets_by_bk: dict[str, dict] = {}

    for bmo in bmos:
        bk_obj   = bk_objs.get(bmo.bookmaker_id)
        bk_name  = (bk_obj.name if bk_obj else str(bmo.bookmaker_id)).lower()
        slug     = _bk_slug(bk_name)
        label    = bk_obj.name if bk_obj else slug.upper()
        mkt_data = _flatten_db_markets(bmo.markets_json or {})
        if not mkt_data: continue
        
        if slug in bookmakers:
            bookmakers[slug]["markets"].update(mkt_data)
            bookmakers[slug]["market_count"] = len(bookmakers[slug]["markets"])
            markets_by_bk[slug].update(mkt_data)
        else:
            bookmakers[slug] = {
                "bookmaker_id": bmo.bookmaker_id, "bookmaker": label, "slug": slug,
                "markets": mkt_data, "market_count": len(mkt_data),
                "link": links_by_match.get(um.id, {}).get(bmo.bookmaker_id),
            }
            markets_by_bk[slug] = mkt_data

    best: dict[str, dict] = {}
    for sl, bk_mkts in markets_by_bk.items():
        for mkt, outcomes in bk_mkts.items():
            best.setdefault(mkt, {})
            for out, odd_data in (outcomes or {}).items():
                try: fv = float(odd_data.get("price") or odd_data.get("odd") or 0) if isinstance(odd_data, dict) else float(odd_data)
                except (TypeError, ValueError): continue
                if fv <= 1.0: continue
                if out not in best[mkt] or fv > best[mkt][out]["odd"]:
                    best[mkt][out] = {"odd": fv, "bk": sl}

    best_odds = {mkt: {out: {"odd": v["odd"], "bookie": v["bk"]} for out, v in outs.items()} for mkt, outs in best.items()}
    
    arb_markets = []
    if len(bookmakers) >= 2:
        for mkt, outcomes in best.items():
            if len(outcomes) < 2: continue
            arb_sum = sum(1.0 / v["odd"] for v in outcomes.values())
            if arb_sum < 1.0:
                profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
                legs = [{"outcome": o, "bk": v["bk"], "odd": v["odd"]} for o, v in outcomes.items()]
                breakdown = [{
                    **leg, "stake_pct": round((1.0 / leg["odd"] / arb_sum) * 100, 3),
                    "stake_kes": round(1000 * (1.0 / leg["odd"] / arb_sum), 2),
                    "return_kes": round(1000 * (1.0 / leg["odd"] / arb_sum) * leg["odd"], 2),
                } for leg in legs]
                arb_markets.append({
                    "market": mkt, "market_slug": mkt, "profit_pct": profit_pct, "arb_sum": round(arb_sum, 6),
                    "legs": legs, "breakdown_1000": breakdown,
                })
        arb_markets.sort(key=lambda x: -x["profit_pct"])

    db_status       = getattr(um, "status", None)
    status_out      = _effective_status(db_status, um.start_time)
    live_flag       = status_out == "IN_PLAY"
    minutes_elapsed = None
    if um.start_time and live_flag:
        st = um.start_time if um.start_time.tzinfo else um.start_time.replace(tzinfo=timezone.utc)
        minutes_elapsed = int((_now_utc() - st).total_seconds() / 60)

    has_arb_flag     = bool(arb_markets) or um.id in arb_set
    all_market_slugs = sorted(best.keys())
    sport_out        = _normalise_sport_slug(um.sport_name or sport_slug)
    br_id            = um.parent_match_id or ""

    analytics_summary = {"available": False}
    if analytics_map is not None and br_id:
        bundle = analytics_map.get(br_id) or {}
        if bundle: analytics_summary = _build_analytics_summary(bundle)

    return {
        "match_id":         um.id,
        "parent_match_id":  br_id, "betradar_id": br_id, "join_key": f"br_{br_id}" if br_id else f"db_{um.id}",
        "home_team":        um.home_team_name  or "", "away_team": um.away_team_name  or "",
        "competition":      um.competition_name or "", "sport": sport_out,
        "start_time":       um.start_time.isoformat() if um.start_time else None,
        "status":           status_out, "is_live": live_flag, "minutes_elapsed": minutes_elapsed,
        "bk_count":         len(bookmakers), "bookie_count": len(bookmakers), "bookmaker_count": len(bookmakers),
        "market_count":     len(all_market_slugs), "market_slugs": all_market_slugs,
        "bookmakers":       bookmakers, "markets_by_bk": markets_by_bk, "markets": markets_by_bk,
        "best":             best, "best_odds": best_odds, "aggregated": _flatten_db_markets(um.markets_json or {}),
        "has_arb":          has_arb_flag, "arb_markets": arb_markets, "arbs": arb_markets,
        "best_arb_pct":     arb_markets[0]["profit_pct"] if arb_markets else 0.0,
        "has_ev": False, "evs": [], "has_sharp": False, "sharp": [], "best_ev_pct": 0.0,
        "bk_ids": {sl: str(d["bookmaker_id"]) for sl, d in bookmakers.items()},
        "has_analytics":    analytics_summary.get("available", False), "analytics": analytics_summary,
        "source":           "postgresql",
    }

def _build_envelope(matches, sport, mode, tier, page, per_page, truncated, latency_ms, total=None, pages=None, extra=None):
    arb_count  = sum(1 for m in matches if m.get("has_arb"))
    live_count = sum(1 for m in matches if m.get("is_live"))
    bk_names   = set()
    for m in matches: bk_names.update((m.get("bookmakers") or {}).keys())
    
    _total = total if total is not None else len(matches)
    _pages = pages if pages is not None else max(1, (_total + per_page - 1) // per_page)
    
    env = {
        "ok": True, "sport": _normalise_sport_slug(sport), "mode": mode, "tier": tier,
        "total": _total, "page": page, "per_page": per_page, "pages": _pages,
        "truncated": truncated, "latency_ms": latency_ms,
        "arb_count": arb_count, "live_count": live_count,
        "bookie_count": len(bk_names), "bookmakers": sorted(bk_names),
        "matches": matches, "source": "postgresql",
        "server_time": _now_utc().isoformat(), "min_bookmakers": config.MIN_BOOKMAKERS,
    }
    if truncated: env["upgrade_message"] = "Upgrade your plan to see all matches."
    if extra: env.update(extra)
    return env