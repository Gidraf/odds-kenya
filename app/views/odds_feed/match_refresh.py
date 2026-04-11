from flask import request, Response, stream_with_context
from ..customer import bp_odds_customer
from ..customer import config
from app.utils.decorators_ import log_event
from ..customer.utils import _now_utc, _normalise_sport_slug, _sse, _keepalive

import re
import json
import time
import os

# ══════════════════════════════════════════════════════════════════════════════
# UNIVERSAL MATCH NORMALIZER (BULLETPROOF CANONICAL PARSER)
# ══════════════════════════════════════════════════════════════════════════════
def _unify_match_payload(raw_match: dict, count: int, mode: str, bk_slug: str, bk_name: str) -> dict:
    if not raw_match: return {}

    st = str(raw_match.get("start_time") or "")
    if st and not st.endswith("Z") and "+" not in st:
        st = st.replace(" ", "T")
        if len(st) == 19: st += ".000Z"

    h_val = raw_match.get("home_team") or raw_match.get("home_team_name")
    a_val = raw_match.get("away_team") or raw_match.get("away_team_name")
    comps = raw_match.get("competitors")
    if isinstance(comps, list):
        if not h_val and len(comps) > 0 and isinstance(comps[0], dict): h_val = comps[0].get("name")
        if not a_val and len(comps) > 1 and isinstance(comps[1], dict): a_val = comps[1].get("name")

    h_team  = str(h_val or "Home")
    a_team  = str(a_val or "Away")
    h_clean = re.sub(r'[^a-z0-9]', '_', h_team.lower()).strip('_')
    a_clean = re.sub(r'[^a-z0-9]', '_', a_team.lower()).strip('_')

    sport_val = raw_match.get("sport") or raw_match.get("sport_name") or "soccer"
    if isinstance(sport_val, dict): sport_val = sport_val.get("name", "soccer")
    sport_slug_out = _normalise_sport_slug(str(sport_val))

    best_mock  = {}
    bk_markets = {}

    raw_list = raw_match.get("markets") or []
    standardized_markets: dict = {}

    if isinstance(raw_list, list):
        for mkt_obj in raw_list:
            if not isinstance(mkt_obj, dict): continue
            m_id = mkt_obj.get("id")
            spec = str(mkt_obj.get("specValue") or mkt_obj.get("specialValue") or "")
            if   spec.endswith(".00"): spec = spec[:-3]
            elif spec.endswith(".0"):  spec = spec[:-2]

            name = str(mkt_obj.get("name") or "").lower()
            m    = name

            if   m_id in (52,  105): m = f"over_under_{spec}"
            elif m_id == 54:         m = f"first_half_over_under_{spec}"
            elif m_id in (51,  184): m = f"asian_handicap_{spec}"
            elif m_id == 53:         m = f"first_half_asian_handicap_{spec}"
            elif m_id in (55,  151): m = f"european_handicap_{spec}"
            elif m_id in (208, 196): m = f"result_and_over_under_{spec}"
            elif m_id in (386, 124): m = "btts_and_result"
            elif m_id in (10,  194): m = "match_winner"
            elif m_id in (43,  138): m = "btts"
            elif m_id in (46,  147): m = "double_chance"
            elif m_id in (47,  166): m = "draw_no_bet"

            standardized_markets[m] = {}
            for sel in mkt_obj.get("selections", []):
                if not isinstance(sel, dict): continue
                ok = str(sel.get("shortName") or sel.get("name") or "")
                try:    price = float(sel.get("odds", 0))
                except: price = 0.0
                if price > 1.0:
                    standardized_markets[m][ok] = price
    else:
        # dict shape  { market_slug: { outcome: price|{price:..} } }
        for mkt_slug, outcomes in raw_list.items():
            m = str(mkt_slug or "")
            standardized_markets[m] = {}
            if not isinstance(outcomes, dict): continue
            for out_key, price in outcomes.items():
                try:    p = float(price) if not isinstance(price, dict) else float(price.get("price", 0))
                except: p = 0.0
                if p > 1.0:
                    standardized_markets[m][str(out_key)] = p

    for mkt_slug, outcomes in standardized_markets.items():
        if not outcomes: continue

        m = str(mkt_slug).lower()
        m = re.sub(
            r"^(soccer|basketball|tennis|ice_hockey|volleyball|cricket|rugby|"
            r"table_tennis|handball|mma|boxing|darts|esoccer|baseball|american_football)_+",
            "", m
        )
        m = m.replace("1st_half___", "first_half_").replace("1st_half_", "first_half_")
        m = m.replace("2nd_half___", "second_half_").replace("2nd_half_", "second_half_")
        if h_clean and len(h_clean) > 3 and h_clean in m: m = m.replace(h_clean, "home")
        if a_clean and len(a_clean) > 3 and a_clean in m: m = m.replace(a_clean, "away")
        m = (m.replace("___", "_").replace("_&_", "_and_").replace(" ", "_")
              .replace("both_teams_to_score", "btts")
              .replace("over_under_goals", "over_under")
              .replace("total_goals", "over_under")
              .replace("total_points", "over_under"))

        if m in ("1x2", "moneyline", "3_way"):           m = "match_winner"
        if m.endswith("_1x2"):                            m = m.replace("_1x2", "_match_winner")
        if m.startswith("1x2_"):                          m = m.replace("1x2_", "match_winner_")
        if m == "match_winner_btts":                      m = "btts_and_result"
        if m.startswith("match_winner_over_under") or m.startswith("match_winner_and_over_under"):
            m = m.replace("match_winner_over_under", "result_and_over_under") \
                 .replace("match_winner_and_over_under", "result_and_over_under")
        if m == "draw_no_bet_full_time":                  m = "draw_no_bet"

        m = re.sub(r"exact_goals_\d+$", "exact_goals", m)
        m = m.replace(".", "_")
        if "handicap_-" in m: m = m.replace("handicap_-", "handicap_minus_")
        else:                  m = m.replace("handicap_+", "handicap_")
        m = re.sub(r"_0_0$", "_0", m)
        m = re.sub(r"_+", "_", m).strip("_")

        bk_markets[m] = {}
        best_mock[m]   = {}

        for ok_raw, p in outcomes.items():
            ok_raw   = str(ok_raw)
            ok_lower = ok_raw.lower()

            ok = "X" if ("draw" in ok_lower and "or" not in ok_lower and "&" not in ok_lower) else ok_raw
            if h_clean and h_clean in ok_lower.replace(" ", "_") and "or" not in ok_lower and "&" not in ok_lower: ok = "1"
            if a_clean and a_clean in ok_lower.replace(" ", "_") and "or" not in ok_lower and "&" not in ok_lower: ok = "2"
            ok_lower = ok.lower()

            if "double_chance" in m:
                ok = (ok_lower.replace("or","").replace("/","").replace("draw","x")
                              .replace(h_clean,"1").replace(a_clean,"2")
                              .replace("home","1").replace("away","2").replace(" ","").upper())
                if ok == "X1": ok = "1X"
                elif ok == "2X": ok = "X2"
                elif ok == "21": ok = "12"

            elif "btts_and" in m or "result_and" in m:
                suffix = ("Yes"   if "yes"   in ok_lower else
                          "No"    if "no"    in ok_lower else
                          "Over"  if "over"  in ok_lower else
                          "Under" if "under" in ok_lower else "")
                prefix = ("X" if ("draw" in ok_lower or "x" in ok_lower.split()) else
                           "1" if (h_clean in ok_lower.replace(" ","_") or "home" in ok_lower or "1" in ok_lower.split()) else
                           "2" if (a_clean in ok_lower.replace(" ","_") or "away" in ok_lower or "2" in ok_lower.split()) else "")
                if prefix and suffix:
                    ok = f"{prefix} {suffix}"
                else:
                    ok = (ok_lower.replace(" & "," ").replace("and","")
                                  .replace("yes","Yes").replace("no","No")
                                  .replace("draw","X").replace("home","1").replace("away","2")
                                  .replace(h_clean,"1").replace(a_clean,"2"))
                    ok = ok.replace("1","1 ").replace("x","X ").replace("2","2 ").replace("  "," ").strip().title().replace(" X"," X")

            else:
                if ok_lower in ("yes","y"):                        ok = "Yes"
                elif ok_lower in ("no","n"):                       ok = "No"
                elif "over"  in ok_lower or ok_lower == "ov":     ok = "Over"
                elif "under" in ok_lower or ok_lower == "un":     ok = "Under"
                elif ok_lower == "odd":                            ok = "Odd"
                elif ok_lower == "even":                           ok = "Even"
                elif ok_lower == "home":                           ok = "1"
                elif ok_lower == "away":                           ok = "2"
                elif ok_lower == "1x":                             ok = "1X"
                elif ok_lower == "x2":                             ok = "X2"
                elif ok_lower == "12":                             ok = "12"

                if "over_under" in m and ok not in ("Over", "Under"):
                    if ok_lower.startswith("o"):   ok = "Over"
                    elif ok_lower.startswith("u"): ok = "Under"

                if "exact_goals"  in m: ok = ok.replace(" OR MORE","+").replace(" or more","+").replace(" and over","+")
                if "correct_score" in m: ok = ok.replace("-",":")

            best_mock[m][ok]  = {"odd": p, "bk": bk_slug}
            bk_markets[m][ok] = {"price": p}

    actual_id = str(raw_match.get(f"{bk_slug}_match_id") or raw_match.get("match_id") or count)
    b_id      = str(raw_match.get("betradar_id") or raw_match.get(f"{bk_slug}_parent_id") or "")

    return {
        "match_id":        actual_id,
        "join_key":        f"br_{b_id}" if b_id and b_id != "None" else f"{bk_slug}_{actual_id}",
        "parent_match_id": b_id if b_id and b_id != "None" else None,
        "home_team":       h_team,
        "away_team":       a_team,
        "competition":     str(raw_match.get("competition_name") or raw_match.get("competition", "")),
        "sport":           sport_slug_out,
        "start_time":      st,
        "status":          "IN_PLAY" if mode == "live" else "PRE_MATCH",
        "is_live":         mode == "live",
        "bk_count":        1,
        "market_count":    len(bk_markets),
        "market_slugs":    list(bk_markets.keys()),
        "bookmakers": {
            bk_slug: {
                "bookmaker":    bk_name,
                "slug":         bk_slug,
                "markets":      bk_markets,
                "market_count": len(bk_markets),
            }
        },
        "markets_by_bk":   {bk_slug: bk_markets},
        "best":            best_mock,
        "best_odds":       best_mock,
        "has_arb":  False,
        "has_ev":   False,
        "has_sharp": False,
    }


# ══════════════════════════════════════════════════════════════════════════════
# HELPER  –  rebuild best/bk_count/market_slugs from markets_by_bk
# ══════════════════════════════════════════════════════════════════════════════
def _rebuild_best(payload: dict) -> None:
    best: dict = {}
    for bk_slug, mkts in payload.get("markets_by_bk", {}).items():
        for mkt_slug, outcomes in mkts.items():
            if mkt_slug not in best:
                best[mkt_slug] = {}
            for out_key, price_dict in outcomes.items():
                p = price_dict.get("price", 0)
                if p > 1.0:
                    cur = best[mkt_slug].get(out_key)
                    if cur is None or p > cur["odd"]:
                        best[mkt_slug][out_key] = {"odd": p, "bk": bk_slug}
    payload["best"]         = best
    payload["best_odds"]    = best
    payload["market_slugs"] = list(best.keys())
    payload["market_count"] = len(best)
    payload["bk_count"]     = len(payload.get("bookmakers", {}))


# ══════════════════════════════════════════════════════════════════════════════
# ROUTE 1  –  UNIFIED SSE STREAM
#   GET /odds/debug/unified/stream/<mode>/<sport_slug>
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/debug/unified/stream/<mode>/<sport_slug>")
def unified_stream(mode: str, sport_slug: str):
    """
    Server-Sent Events stream merging SportPesa + Betika + OdiBets into one
    canonical payload per match.

    Path params
    ───────────
    mode        "live" | "upcoming"
    sport_slug  e.g. "soccer", "basketball"

    Query params
    ────────────
    full=1    fetch full markets from each bookmaker (default true)
    max=N     cap on upcoming matches (default 50)
    """
    fetch_full = request.args.get("full", "true").lower() in ("1", "true")
    max_m      = int(request.args.get("max", 50))
    log_event("unified_stream", {"sport": sport_slug, "mode": mode})

    def _gen():
        yield _sse("meta", {
            "source": "unified_direct",
            "sport":  sport_slug,
            "mode":   mode,
            "now":    _now_utc().isoformat(),
            "bks":    ["sp", "bt", "od"],
        })

        try:
            count = 0

            # ── LIVE ──────────────────────────────────────────────────────────
            if mode == "live":
                from app.workers.sp_live_harvester import fetch_live_stream, SPORT_SLUG_MAP
                from app.workers.bt_harvester       import fetch_live_matches as bt_fetch_live, slug_to_bt_sport_id
                from app.workers.od_harvester       import fetch_live_matches as od_fetch_live

                sport_id    = {v: k for k, v in SPORT_SLUG_MAP.items()}.get(sport_slug, 1)
                bt_data     = bt_fetch_live(slug_to_bt_sport_id(sport_slug)) or []
                od_data     = od_fetch_live(sport_slug) or []
                bt_map      = {str(m.get("betradar_id") or m.get("bt_parent_id") or ""): m for m in bt_data}
                od_map      = {str(m.get("betradar_id") or m.get("od_parent_id") or ""): m for m in od_data}

                seen_br_ids:        set  = set()
                sp_event_map:       dict = {}
                active_live_br_ids: list = []

                # Phase 1 – SP + enrich
                for sp_match in fetch_live_stream(sport_slug, fetch_full_markets=True):
                    count     += 1
                    br_id      = str(sp_match.get("betradar_id") or "")
                    sp_ev_id   = str(sp_match.get("sp_match_id") or sp_match.get("match_id") or "")
                    if br_id and br_id not in ("0", "None"):
                        seen_br_ids.add(br_id)
                        active_live_br_ids.append(br_id)
                        sp_event_map[br_id] = sp_ev_id

                    payload = _unify_match_payload(sp_match, count, "live", "sp", "SPORTPESA")
                    payload["is_live"] = True

                    bt_m = bt_map.get(br_id)
                    if bt_m and bt_m.get("markets"):
                        c = _unify_match_payload(bt_m, count, "live", "bt", "BETIKA")
                        payload["bookmakers"]["bt"]    = c["bookmakers"]["bt"]
                        payload["markets_by_bk"]["bt"] = c["markets_by_bk"]["bt"]

                    od_m = od_map.get(br_id)
                    if od_m and od_m.get("markets"):
                        c = _unify_match_payload(od_m, count, "live", "od", "ODIBETS")
                        payload["bookmakers"]["od"]    = c["bookmakers"]["od"]
                        payload["markets_by_bk"]["od"] = c["markets_by_bk"]["od"]

                    _rebuild_best(payload)
                    yield _sse("batch", {"matches": [payload], "batch": count, "of": "unknown", "offset": count - 1})
                    yield _keepalive()

                # Phase 2 – BT-only
                for br_id, bt_m in bt_map.items():
                    if br_id in seen_br_ids or not br_id or br_id == "None": continue
                    count += 1
                    seen_br_ids.add(br_id)
                    active_live_br_ids.append(br_id)

                    payload = _unify_match_payload(bt_m, count, "live", "bt", "BETIKA")
                    payload["is_live"] = True

                    od_m = od_map.get(br_id)
                    if od_m and od_m.get("markets"):
                        c = _unify_match_payload(od_m, count, "live", "od", "ODIBETS")
                        payload["bookmakers"]["od"]    = c["bookmakers"]["od"]
                        payload["markets_by_bk"]["od"] = c["markets_by_bk"]["od"]

                    _rebuild_best(payload)
                    yield _sse("batch", {"matches": [payload], "batch": count, "of": "unknown", "offset": count - 1})
                    yield _keepalive()

                # Phase 3 – OD-only
                for br_id, od_m in od_map.items():
                    if br_id in seen_br_ids or not br_id or br_id == "None": continue
                    count += 1
                    seen_br_ids.add(br_id)
                    active_live_br_ids.append(br_id)

                    payload = _unify_match_payload(od_m, count, "live", "od", "ODIBETS")
                    payload["is_live"] = True
                    _rebuild_best(payload)
                    yield _sse("batch", {"matches": [payload], "batch": count, "of": "unknown", "offset": count - 1})
                    yield _keepalive()

                yield _sse("list_done", {"total_sent": count})

                # Phase 4 – Real-time pub/sub
                import redis as _redis
                r_conn = _redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
                pubsub = r_conn.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(f"sp:live:sport:{sport_id}")
                last_poll = time.time()

                while True:
                    msg = pubsub.get_message(timeout=0.5)
                    if msg and msg["type"] == "message":
                        try:
                            data     = json.loads(msg["data"])
                            ev_id    = str(data.get("event_id") or "")
                            br_id    = next((k for k, v in sp_event_map.items() if v == ev_id), ev_id)
                            msg_type = data.get("type")

                            if msg_type == "event_update":
                                state = data.get("state", {})
                                yield _sse("live_update", {
                                    "parent_match_id": br_id,
                                    "match_time":  state.get("matchTime") or data.get("match_time", ""),
                                    "score_home":  data.get("score_home"),
                                    "score_away":  data.get("score_away"),
                                    "status":      data.get("phase") or state.get("currentEventPhase", ""),
                                    "is_live":     True,
                                })

                            elif msg_type == "market_update":
                                sels = data.get("normalised_selections", [])
                                slug = data.get("market_slug")
                                if sels and slug:
                                    outs = {s["outcome_key"]: {"price": float(s["odds"])}
                                            for s in sels if float(s.get("odds", 0)) > 1}
                                    if outs:
                                        yield _sse("live_update", {
                                            "parent_match_id": br_id,
                                            "bookmakers":    {"sp": {"slug": "sp", "markets": {slug: outs}}},
                                            "markets_by_bk": {"sp": {slug: outs}},
                                        })
                        except Exception:
                            pass

                    # Re-poll BT + OD every 10 s
                    if time.time() - last_poll > 10.0:
                        last_poll = time.time()
                        try:
                            bt_data = bt_fetch_live(slug_to_bt_sport_id(sport_slug)) or []
                            od_data = od_fetch_live(sport_slug) or []
                            bt_map  = {str(m.get("betradar_id") or ""): m for m in bt_data}
                            od_map  = {str(m.get("betradar_id") or ""): m for m in od_data}

                            for br_id in active_live_br_ids:
                                up: dict = {"parent_match_id": br_id, "bookmakers": {}, "markets_by_bk": {}}
                                bt_m = bt_map.get(br_id)
                                if bt_m and bt_m.get("markets"):
                                    c = _unify_match_payload(bt_m, 0, "live", "bt", "BETIKA")
                                    up["bookmakers"]["bt"]    = c["bookmakers"]["bt"]
                                    up["markets_by_bk"]["bt"] = c["markets_by_bk"]["bt"]
                                od_m = od_map.get(br_id)
                                if od_m and od_m.get("markets"):
                                    c = _unify_match_payload(od_m, 0, "live", "od", "ODIBETS")
                                    up["bookmakers"]["od"]    = c["bookmakers"]["od"]
                                    up["markets_by_bk"]["od"] = c["markets_by_bk"]["od"]
                                if up["bookmakers"]:
                                    yield _sse("live_update", up)
                        except Exception:
                            pass

                    yield _keepalive()

            # ── UPCOMING ──────────────────────────────────────────────────────
            else:
                from app.workers.sp_harvester import fetch_upcoming_stream
                from app.workers.bt_harvester import get_full_markets
                from app.workers.od_harvester import fetch_event_detail, slug_to_od_sport_id

                od_sport_id = slug_to_od_sport_id(sport_slug)

                for sp_match in fetch_upcoming_stream(sport_slug, max_matches=max_m, fetch_full_markets=fetch_full):
                    count += 1
                    br_id      = str(sp_match.get("betradar_id") or "")
                    bt_markets = {}
                    od_markets = {}

                    if br_id and br_id != "None":
                        try:    bt_markets = get_full_markets(br_id, sport_slug)
                        except: pass
                        try:    od_markets, _ = fetch_event_detail(br_id, od_sport_id)
                        except: pass

                    sp_clean = _unify_match_payload(sp_match, count, "upcoming", "sp", "SPORTPESA")

                    if bt_markets:
                        raw = {"markets": bt_markets, "betradar_id": br_id,
                               "home_team": sp_clean["home_team"], "away_team": sp_clean["away_team"]}
                        c = _unify_match_payload(raw, count, "upcoming", "bt", "BETIKA")
                        sp_clean["bookmakers"]["bt"]    = c["bookmakers"]["bt"]
                        sp_clean["markets_by_bk"]["bt"] = c["markets_by_bk"]["bt"]

                    if od_markets:
                        raw = {"markets": od_markets, "betradar_id": br_id,
                               "home_team": sp_clean["home_team"], "away_team": sp_clean["away_team"]}
                        c = _unify_match_payload(raw, count, "upcoming", "od", "ODIBETS")
                        sp_clean["bookmakers"]["od"]    = c["bookmakers"]["od"]
                        sp_clean["markets_by_bk"]["od"] = c["markets_by_bk"]["od"]

                    _rebuild_best(sp_clean)
                    yield _sse("batch", {"matches": [sp_clean], "batch": count, "of": "unknown", "offset": count - 1})
                    yield _keepalive()

                yield _sse("list_done", {"total_sent": count})
                yield _sse("done", {"status": "finished", "total_sent": count})

        except Exception as exc:
            yield _sse("error", {"error": str(exc)})

    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)