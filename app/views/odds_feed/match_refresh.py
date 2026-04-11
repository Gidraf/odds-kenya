from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from flask import Blueprint, request

from app.utils.customer_jwt_helpers import _err, _signed_response
from app.utils.decorators_ import log_event
from app.views.odds_feed.customer_odds_view import _normalise_sport_slug
import re

bp_match_refresh = Blueprint("match-refresh", __name__, url_prefix="/api")

_REFRESH_TIMEOUT = 18   # seconds per bookmaker fetch
_MAX_WORKERS     = 3

SAFE_ARB_MARKETS = {
    "match_winner", "1x2", "moneyline", "btts", "both_teams_score",
    "double_chance", "odd_even"
}

def _resolve_native_ids_from_cache(sport_slug: str, home_team: str, away_team: str) -> dict[str, str]:
    from app.workers.celery_tasks import cache_get
    native_ids = {}
    
    h_target = (home_team or "").lower().strip()[:6]
    a_target = (away_team or "").lower().strip()[:6]
    
    for bk in ["sp", "bt", "od"]:
        cached = cache_get(f"{bk}:upcoming:{sport_slug}")
        if not cached or not cached.get("matches"):
            continue
            
        for m in cached["matches"]:
            h = str(m.get("home_team") or m.get("home_team_name") or "").lower()
            a = str(m.get("away_team") or m.get("away_team_name") or "").lower()
            
            if (h_target in h or h in h_target) and (a_target in a or a in a_target):
                if bk == "sp":
                    native_ids["sp"] = str(m.get("sp_game_id") or m.get("id") or "")
                elif bk == "bt":
                    native_ids["bt"] = str(m.get("bt_parent_id") or m.get("parent_match_id") or m.get("bt_match_id") or "")
                elif bk == "od":
                    native_ids["od"] = str(m.get("od_event_id") or m.get("od_match_id") or "")
                break
                
    return native_ids


def _fetch_sp_live(betradar_id: str, sport_slug: str) -> tuple[str, dict, str]:
    """Dedicated Live Fetcher for SP"""
    try:
        from app.workers.sp_live_harvester import fetch_event_details, fetch_live_events, SPORT_SLUG_MAP
        sport_id = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)
        
        # Need to find SP's internal live ID using the Betradar ID
        raw_events = fetch_live_events(sport_id, limit=200)
        sp_event_id = None
        for ev in raw_events:
            if str(ev.get("externalId")) == str(betradar_id):
                sp_event_id = ev.get("id")
                break
                
        if not sp_event_id:
            return "sp", {}, "no_live_match_found"
            
        details = fetch_event_details(sp_event_id)
        if not details or not details.get("markets"):
            return "sp", {}, "no_markets"
            
        # Format to match normalizer expectations
        mock_match = {"markets": details.get("markets")}
        return "sp", mock_match, "ok"
    except Exception as exc:
        return "sp", {}, str(exc)[:120]


def _fetch_sp(betradar_id: str, sp_game_id: str | None, sport_slug: str) -> tuple[str, dict, str]:
    try:
        from app.workers.sp_harvester_ import fetch_match_markets
        target_id = sp_game_id or betradar_id
        markets = fetch_match_markets(target_id, sport_slug)
        if not markets:
            return "sp", {}, "no_markets"
        return "sp", markets, "ok"
    except Exception as exc:
        return "sp", {}, str(exc)[:120]


def _fetch_bt_live(betradar_id: str, sport_slug: str) -> tuple[str, dict, str]:
    """Dedicated Live Fetcher for BT"""
    try:
        from app.workers.bt_harvester import get_live_match_markets
        markets, _ = get_live_match_markets(betradar_id, sport_slug)
        if not markets:
            return "bt", {}, "no_markets"
        return "bt", {"markets": markets}, "ok"
    except Exception as exc:
        return "bt", {}, str(exc)[:120]


def _fetch_bt(betradar_id: str, bt_game_id: str | None, sport_slug: str) -> tuple[str, dict, str]:
    try:
        from app.workers.bt_harvester import get_full_markets
        target_id = bt_game_id or betradar_id
        markets = get_full_markets(target_id, sport_slug=sport_slug)
        if not markets and target_id != betradar_id:
            markets = get_full_markets(betradar_id, sport_slug=sport_slug)
        if not markets:
            return "bt", {}, "no_markets"
        return "bt", {"markets": markets}, "ok"
    except Exception as exc:
        return "bt", {}, str(exc)[:120]


def _fetch_od_live(betradar_id: str, sport_slug: str) -> tuple[str, dict, str]:
    """Dedicated Live Fetcher for OD"""
    try:
        from app.workers.od_harvester import fetch_event_detail, slug_to_od_sport_id
        od_sport_id = slug_to_od_sport_id(sport_slug)
        markets, _meta = fetch_event_detail(betradar_id, od_sport_id)
        if not markets:
            return "od", {}, "no_markets"
        return "od", {"markets": markets}, "ok"
    except Exception as exc:
        return "od", {}, str(exc)[:120]


def _fetch_od(betradar_id: str, od_game_id: str | None, sport_slug: str) -> tuple[str, dict, str]:
    try:
        from app.workers.od_harvester import fetch_event_detail, slug_to_od_sport_id
        od_sport_id = slug_to_od_sport_id(sport_slug)
        target_id = od_game_id or betradar_id
        markets, _meta = fetch_event_detail(target_id, od_sport_id)
        if not markets:
            return "od", {}, "no_markets"
        return "od", {"markets": markets}, "ok"
    except Exception as exc:
        return "od", {}, str(exc)[:120]


def _normalise_markets(raw_match: dict, bk_slug: str) -> dict:
    """Uses the aggressive Canonical Parser logic from routes_debug"""
    if not raw_match or not raw_match.get("markets"): return {}
    
    raw_list = raw_match.get("markets", {})
    items = raw_list.items() if isinstance(raw_list, dict) else []
    clean: dict = {}

    if bk_slug == "sp" and isinstance(raw_list, list):
        for mkt_obj in raw_list:
            m_id = mkt_obj.get("id")
            spec = mkt_obj.get("specValue") or mkt_obj.get("specialValue") or ""
            name = mkt_obj.get("name", "").lower()
            
            m = name
            if m_id in (52, 105): m = f"over_under_{str(spec).replace('.', '_')}"
            elif m_id == 54: m = f"first_half_over_under_{str(spec).replace('.', '_')}"
            elif m_id in (51, 184): m = f"asian_handicap_{str(spec).replace('.', '_').replace('-', 'minus_')}"
            elif m_id == 53: m = f"first_half_asian_handicap_{str(spec).replace('.', '_').replace('-', 'minus_')}"
            elif m_id in (55, 151): m = f"european_handicap_{str(spec).replace('-', 'minus_').replace('+', '')}"
            elif m_id in (208, 196): m = f"result_and_over_under_{str(spec).replace('.','_')}"
            elif m_id in (10, 194): m = "match_winner"
            elif m_id in (43, 138): m = "btts"
            elif m_id in (46, 147): m = "double_chance"
            elif m_id in (47, 166): m = "draw_no_bet"
            
            m = re.sub(r"^(soccer|volleyball|tennis|basketball)_+", "", m)
            m = m.replace(" ", "_").replace("-", "_").replace("___", "_")
            m = re.sub(r"_+", "_", m).strip("_")

            clean_outs = {}
            for sel in mkt_obj.get("selections", []):
                ok = sel.get("shortName") or sel.get("name")
                ok = ok.replace("UN", "Under").replace("OV", "Over").replace("Yes", "Yes").replace("No", "No")
                price = float(sel.get("odds", 0))
                if price > 1.0: clean_outs[ok] = {"odd": price}
            if clean_outs: clean[m] = clean_outs

    else:
        for mkt_slug, outcomes in items:
            m = mkt_slug.lower()
            m = re.sub(r"^(soccer|basketball|tennis|ice_hockey|volleyball|cricket|rugby|table_tennis|handball|mma|boxing|darts|esoccer|baseball|american_football)_+", "", m)
            m = m.replace("1st_half___", "first_half_").replace("1st_half_", "first_half_")
            m = m.replace("2nd_half___", "second_half_").replace("2nd_half_", "second_half_")
            m = m.replace("___", "_").replace("_&_", "_and_").replace(" ", "_")
            m = m.replace("both_teams_to_score", "btts").replace("over_under_goals", "over_under").replace("total_goals", "over_under").replace("total_points", "over_under")
            
            if m == "1x2" or m == "moneyline": m = "match_winner"
            if m.endswith("_1x2"): m = m.replace("_1x2", "_match_winner")
            if m.startswith("1x2_"): m = m.replace("1x2_", "match_winner_")
            if m == "match_winner_btts": m = "btts_and_result"
            if m.startswith("match_winner_over_under") or m.startswith("match_winner_and_over_under"): m = m.replace("match_winner_over_under", "result_and_over_under").replace("match_winner_and_over_under", "result_and_over_under")
            
            m = re.sub(r"exact_goals_\d+$", "exact_goals", m)
            m = m.replace(".", "_")
            if "handicap_-" in m: m = m.replace("handicap_-", "handicap_minus_")
            elif "handicap_" in m: m = m.replace("handicap_+", "handicap_")
            m = m.replace("_0_0", "_0")
            m = re.sub(r"_+", "_", m).strip("_")

            clean_outs = {}
            for out_key, price in outcomes.items():
                p = float(price) if not isinstance(price, dict) else float(price.get("price", 0))
                if p > 1.0:
                    ok = str(out_key).lower()
                    if "draw" in ok and "or" not in ok and "&" not in ok: out_key = "X"
                    
                    ok_lower = str(out_key).lower()
                    if "double_chance" in m:
                        out_key = ok_lower.replace("or", "").replace("/", "").replace("draw", "x").replace(" ", "").upper()
                        if out_key == "X1": out_key = "1X"
                        elif out_key == "2X": out_key = "X2"
                        elif out_key == "21": out_key = "12"
                    elif "btts_and" in m or "result_and" in m:
                        out_key = ok_lower.replace(" & ", " ").replace("and", "").replace("yes", "Yes").replace("no", "No").replace("draw", "X")
                        out_key = out_key.replace("1", "1 ").replace("x", "X ").replace("2", "2 ").replace("  ", " ").strip()
                    else:
                        if ok_lower == "yes": out_key = "Yes"
                        elif ok_lower == "no": out_key = "No"
                        elif ok_lower == "over": out_key = "Over"
                        elif ok_lower == "under": out_key = "Under"
                        elif ok_lower == "odd": out_key = "Odd"
                        elif ok_lower == "even": out_key = "Even"
                        if "exact_goals" in m: out_key = out_key.replace(" OR MORE", "+").replace(" or more", "+").replace(" and over", "+")
                        if "correct_score" in m: out_key = out_key.replace("-", ":")

                    clean_outs[out_key] = {"odd": p}
            if clean_outs: clean[m] = clean_outs

    return clean


def _save_bk_markets(match_id: int, bookmaker_id: int, markets: dict, betradar_id: str) -> int:
    from app.extensions import db
    from app.models.odds_model import BookmakerMatchOdds, BookmakerOddsHistory, UnifiedMatch
    from datetime import datetime
    bmo = BookmakerMatchOdds.query.filter_by(match_id=match_id, bookmaker_id=bookmaker_id).with_for_update().first()
    if not bmo:
        bmo = BookmakerMatchOdds(match_id=match_id, bookmaker_id=bookmaker_id)
        db.session.add(bmo)
        db.session.flush()

    um = UnifiedMatch.query.get(match_id)
    history_batch: list[dict] = []
    updates = 0
    for mkt_slug, outcomes in markets.items():
        for outcome, odd_data in outcomes.items():
            price = float(odd_data.get("odd", 0)) if isinstance(odd_data, dict) else float(odd_data)
            if price <= 1.0: continue
            try:
                price_changed, old_price = bmo.upsert_selection(market=mkt_slug, specifier=None, selection=outcome, price=price)
                if um: um.upsert_bookmaker_price(market=mkt_slug, specifier=None, selection=outcome, price=price, bookmaker_id=bookmaker_id)
                if price_changed:
                    history_batch.append({
                        "bmo_id": bmo.id, "bookmaker_id": bookmaker_id, "match_id": match_id, "market": mkt_slug,
                        "specifier": None, "selection": outcome, "old_price": old_price, "new_price": price,
                        "price_delta": round(price - old_price, 4) if old_price else None, "recorded_at": datetime.utcnow(),
                    })
                updates += 1
            except Exception: pass
    if history_batch: BookmakerOddsHistory.bulk_append(history_batch)
    return updates


def _detect_arbs(markets_by_bk: dict) -> list[dict]:
    best: dict[str, dict[str, tuple[float, str]]] = {}
    for bk_slug, mkts in markets_by_bk.items():
        for mkt, outcomes in (mkts or {}).items():
            best.setdefault(mkt, {})
            for out, odd_data in outcomes.items():
                try: fv = float(odd_data.get("odd") or odd_data.get("price") or odd_data) if isinstance(odd_data, dict) else float(odd_data)
                except (TypeError, ValueError, AttributeError): continue
                if fv > 1.0:
                    existing = best[mkt].get(out)
                    if not existing or fv > existing[0]: best[mkt][out] = (fv, bk_slug)

    arbs: list[dict] = []
    for mkt, outcomes in best.items():
        is_safe = (mkt in SAFE_ARB_MARKETS or "over_under" in mkt or "asian_handicap" in mkt)
        if not is_safe or len(outcomes) < 2: continue
        if mkt in ("match_winner", "1x2") and len(outcomes) != 3: continue
        if ("over_under" in mkt or mkt in ("btts", "both_teams_score", "odd_even")) and len(outcomes) != 2: continue
        
        bk_ids = {v[1] for v in outcomes.values()}
        if len(bk_ids) < 2: continue
        arb_sum = sum(1.0 / v[0] for v in outcomes.values())
        if arb_sum >= 1.0 or arb_sum == 0: continue

        profit_pct = round((1.0 / arb_sum - 1.0) * 100, 4)
        if profit_pct < 0.5: continue
            
        legs = [{"outcome": out, "bk": v[1], "odd": v[0], "stake_pct": round((1.0 / v[0] / arb_sum) * 100, 3), "stake_kes": round(1000 * (1.0 / v[0] / arb_sum), 2)} for out, v in outcomes.items()]
        arbs.append({"market": mkt, "profit_pct": profit_pct, "arb_sum": round(arb_sum, 6), "legs": legs})
    return sorted(arbs, key=lambda x: -x["profit_pct"])


@bp_match_refresh.route("/odds/match/<parent_match_id>/refresh", methods=["POST"])
def refresh_match(parent_match_id: str):
    t0 = time.perf_counter()
    raw_sources = (request.args.get("sources") or "sp,bt,od").strip()
    sources     = {s.strip() for s in raw_sources.split(",") if s.strip()}
    do_save     = request.args.get("save", "1") not in ("0", "false")

    try:
        from app.models.odds_model import UnifiedMatch
        from app.models.bookmakers_model import Bookmaker
        um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    except Exception as exc:
        return _err(f"DB error: {exc}", 500)

    if not um: return _err("Match not found", 404)
    betradar_id = um.parent_match_id or ""
    if not betradar_id: return _err("Match has no betradar_id — cannot refresh", 422)

    sport_slug = _normalise_sport_slug(um.sport_name or "soccer")
    is_live = getattr(um, "status", "PRE_MATCH") == "IN_PLAY"

    bk_id_map: dict[str, int] = {}
    try:
        for bk in Bookmaker.query.filter(Bookmaker.name.in_(["SportPesa", "Betika", "OdiBets"])).all():
            slug = {"sportpesa": "sp", "betika": "bt", "odibets": "od"}.get(bk.name.lower(), bk.name[:2].lower())
            bk_id_map[slug] = bk.id
    except Exception: pass

    sp_game_id, bt_game_id, od_game_id = None, None, None
    if not is_live:
        try:
            from app.models.bookmakers_model import BookmakerMatchLink
            links = BookmakerMatchLink.query.filter_by(match_id=um.id).all()
            for lnk in links:
                if lnk.bookmaker_id == bk_id_map.get("sp"): sp_game_id = lnk.external_match_id
                elif lnk.bookmaker_id == bk_id_map.get("bt"): bt_game_id = lnk.external_match_id
                elif lnk.bookmaker_id == bk_id_map.get("od"): od_game_id = lnk.external_match_id
        except Exception: pass

        native_ids = _resolve_native_ids_from_cache(sport_slug, um.home_team_name, um.away_team_name)
        sp_game_id = native_ids.get("sp") or sp_game_id
        bt_game_id = native_ids.get("bt") or bt_game_id
        od_game_id = native_ids.get("od") or od_game_id

    log_event("match_refresh", {"match_id": parent_match_id, "sources": list(sources), "is_live": is_live})

    fetch_jobs: dict[str, any] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        if is_live:
            if "sp" in sources: fetch_jobs["sp"] = pool.submit(_fetch_sp_live, betradar_id, sport_slug)
            if "bt" in sources: fetch_jobs["bt"] = pool.submit(_fetch_bt_live, betradar_id, sport_slug)
            if "od" in sources: fetch_jobs["od"] = pool.submit(_fetch_od_live, betradar_id, sport_slug)
        else:
            if "sp" in sources: fetch_jobs["sp"] = pool.submit(_fetch_sp, betradar_id, sp_game_id, sport_slug)
            if "bt" in sources: fetch_jobs["bt"] = pool.submit(_fetch_bt, betradar_id, bt_game_id, sport_slug)
            if "od" in sources: fetch_jobs["od"] = pool.submit(_fetch_od, betradar_id, od_game_id, sport_slug)

    raw_results: dict[str, tuple[str, dict, str]] = {}
    for slug, fut in fetch_jobs.items():
        try: raw_results[slug] = fut.result(timeout=_REFRESH_TIMEOUT)
        except Exception as exc: raw_results[slug] = (slug, {}, str(exc)[:120])

    markets_by_bk: dict[str, dict] = {}
    fetch_report:  dict[str, dict] = {}
    
    # Process and fully normalize all markets
    for slug, (_, raw_mkts, status) in raw_results.items():
        clean = _normalise_markets(raw_mkts, slug)
        markets_by_bk[slug] = clean
        fetch_report[slug] = {"status": status, "market_count": len(clean)}

    # Fallback to DB if live fetch fails for any reason
    try:
        from app.models.odds_model import BookmakerMatchOdds
        from app.views.odds_feed.customer_odds_view import _flatten_db_markets, _bk_slug
        from app.models.bookmakers_model import Bookmaker as Bk2
        existing_bmos = BookmakerMatchOdds.query.filter_by(match_id=um.id).all()
        all_bk = {b.id: b for b in Bk2.query.all()}
        for bmo in existing_bmos:
            bk_obj = all_bk.get(bmo.bookmaker_id)
            if not bk_obj: continue
            slug = _bk_slug(bk_obj.name.lower())
            if slug not in markets_by_bk or not markets_by_bk[slug]:
                flat = _flatten_db_markets(bmo.markets_json or {})
                if flat: markets_by_bk[slug] = flat
    except Exception: pass

    changes: dict[str, int] = {}
    if do_save and any(m for m in markets_by_bk.values()):
        try:
            from app.extensions import db
            for slug, clean in markets_by_bk.items():
                if not clean: continue
                bk_id = bk_id_map.get(slug)
                if not bk_id: continue
                n = _save_bk_markets(um.id, bk_id, clean, betradar_id)
                changes[slug] = n
            db.session.commit()
        except Exception as exc:
            try:
                from app.extensions import db
                db.session.rollback()
            except Exception: pass
            fetch_report["_save_error"] = str(exc)[:200]

    best: dict[str, dict] = {}
    for slug, mkts in markets_by_bk.items():
        for mkt, outcomes in mkts.items():
            best.setdefault(mkt, {})
            for out, odd_data in outcomes.items():
                try: fv = float(odd_data.get("odd") or odd_data.get("price") or odd_data) if isinstance(odd_data, dict) else float(odd_data)
                except (TypeError, ValueError, AttributeError): continue
                if fv > 1.0 and (out not in best[mkt] or fv > best[mkt][out]["odd"]):
                    best[mkt][out] = {"odd": fv, "bk": slug}

    arb_markets = _detect_arbs(markets_by_bk)

    return _signed_response({
        "ok": True, "match_id": um.id, "parent_match_id": parent_match_id, "betradar_id": betradar_id,
        "home_team": um.home_team_name, "away_team": um.away_team_name, "competition": um.competition_name,
        "sport": sport_slug, "start_time": um.start_time.isoformat() if um.start_time else None,
        "status": getattr(um, "status", "PRE_MATCH"), "markets_by_bk": markets_by_bk,
        "market_slugs": sorted(best.keys()), "market_count": len(best), "best": best,
        "arb_markets": arb_markets, "has_arb": bool(arb_markets),
        "best_arb_pct": arb_markets[0]["profit_pct"] if arb_markets else 0.0,
        "fetch_report": fetch_report, "sources_tried": list(sources),
        "sources_ok": [s for s, r in fetch_report.items() if r.get("status") == "ok"],
        "changes": changes, "saved": do_save and bool(changes),
        "latency_ms": int((time.perf_counter() - t0) * 1000), "server_time": datetime.now(timezone.utc).isoformat(),
    })