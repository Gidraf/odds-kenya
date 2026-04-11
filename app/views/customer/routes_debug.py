from flask import request, Response, stream_with_context
from .blueprint import bp_odds_customer
from . import config
from .utils import _now_utc, _normalise_sport_slug, _sse, _keepalive
from app.utils.decorators_ import log_event
import re

# ══════════════════════════════════════════════════════════════════════════════
# UNIVERSAL MATCH NORMALIZER
# Guarantees Betika, SportPesa and OdiBets JSON payloads are 100% identical
# ══════════════════════════════════════════════════════════════════════════════
def _unify_match_payload(raw_match: dict, count: int, mode: str, bk_slug: str, bk_name: str) -> dict:

    # 1. Normalize Time
    st = raw_match.get("start_time") or ""
    if st and not st.endswith("Z") and "+" not in st:
        st = st.replace(" ", "T")
        if len(st) == 19: st += ".000Z"

    # 2. Clean Team Names for regex matching
    h_clean = re.sub(r'[^a-z0-9]', '_', raw_match.get("home_team", "").lower()).strip('_')
    a_clean = re.sub(r'[^a-z0-9]', '_', raw_match.get("away_team", "").lower()).strip('_')

    best_mock = {}
    bk_markets = {}

    for mkt, outs in raw_match.get("markets", {}).items():
        m = mkt.lower()

        # 3. Strip Betika's sports prefixes
        m = re.sub(
            r"^(soccer|basketball|tennis|ice_hockey|volleyball|cricket|rugby|"
            r"table_tennis|handball|mma|boxing|darts|esoccer|baseball|american_football)_+",
            "", m
        )

        # 4. Fix Half Time labels
        m = m.replace("1st_half___", "first_half_").replace("1st_half_", "first_half_")
        m = m.replace("2nd_half___", "second_half_").replace("2nd_half_", "second_half_")
        m = m.replace("1st_2nd_half_", "both_halves_")

        # 5. Strip Team Names (Betika injects team names into the market slug)
        if h_clean and len(h_clean) > 3 and h_clean in m: m = m.replace(h_clean, "home")
        if a_clean and len(a_clean) > 3 and a_clean in m: m = m.replace(a_clean, "away")

        # 6. Standardize core names
        m = m.replace("___", "_").replace("_&_", "_and_").replace(" ", "_")
        m = m.replace("both_teams_to_score", "btts")
        m = m.replace("over_under_goals", "over_under").replace("total_goals", "over_under")

        # Normalize 1x2 to match_winner
        if m == "1x2" or m == "moneyline": m = "match_winner"
        if m.endswith("_1x2"): m = m.replace("_1x2", "_match_winner")
        if m.startswith("1x2_"): m = m.replace("1x2_", "match_winner_")

        # Normalize BTTS & Result Combos
        if m == "match_winner_btts": m = "btts_and_result"
        if m.startswith("match_winner_over_under"):
            m = m.replace("match_winner_over_under", "result_and_over_under")

        # Normalize Exact Goals (Betika appends the maximum goal limit)
        m = re.sub(r"exact_goals_\d+$", "exact_goals", m)

        # Clean up any leftover double underscores
        m = re.sub(r"_+", "_", m).strip("_")

        best_mock[m] = {}
        bk_markets[m] = {}

        for out_key, price in outs.items():
            p = float(price) if not isinstance(price, dict) else float(price.get("price", 0))
            if p > 1.0:
                ok = out_key.replace("draw", "X").replace("Draw", "X")

                if "exact_goals" in m:
                    ok = ok.replace(" OR MORE", "+").replace(" or more", "+")
                if "correct_score" in m:
                    ok = ok.replace("-", ":")

                ok_lower = ok.lower()
                if ok_lower == "yes":   ok = "Yes"
                elif ok_lower == "no":  ok = "No"
                elif ok_lower == "over":  ok = "Over"
                elif ok_lower == "under": ok = "Under"
                elif ok_lower == "odd":   ok = "Odd"
                elif ok_lower == "even":  ok = "Even"

                best_mock[m][ok]  = {"odd": p, "bk": bk_slug}
                bk_markets[m][ok] = {"price": p}

    actual_id = (
        raw_match.get(f"{bk_slug}_match_id") or
        raw_match.get(f"{bk_slug}_game_id")  or
        raw_match.get("match_id") or count
    )
    b_id = raw_match.get("betradar_id") or raw_match.get(f"{bk_slug}_parent_id")

    return {
        "match_id":       int(actual_id),
        "join_key":       f"br_{b_id}" if b_id else f"{bk_slug}_{actual_id}",
        "parent_match_id": b_id,
        "home_team":      raw_match.get("home_team", ""),
        "away_team":      raw_match.get("away_team", ""),
        "competition":    raw_match.get("competition_name") or raw_match.get("competition", ""),
        "sport":          _normalise_sport_slug(raw_match.get("sport", "soccer")),
        "start_time":     st,
        "status":         "IN_PLAY" if mode == "live" else "PRE_MATCH",
        "is_live":        mode == "live",
        "bk_count":       1,
        "market_count":   len(bk_markets),
        "market_slugs":   list(bk_markets.keys()),
        "bookmakers": {
            bk_slug: {
                "bookmaker":    bk_name,
                "slug":         bk_slug,
                "markets":      bk_markets,
                "market_count": len(bk_markets),
            }
        },
        "markets_by_bk": {bk_slug: bk_markets},
        "best":           best_mock,
        "best_odds":      best_mock,
        "has_arb": False, "has_ev": False, "has_sharp": False,
    }


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _merge_bk_into_unified(unified: dict, bk_slug: str, bk_name: str, bk_markets: dict) -> None:
    """
    Safely merge a single bookmaker's canonical market dict into the
    already-built unified payload, updating best-odds as we go.
    """
    bk_markets_with_price: dict = {}
    for mkt_slug, outcomes in bk_markets.items():
        bk_markets_with_price[mkt_slug] = {}
        for out_key, val in outcomes.items():
            # od_harvester returns plain floats; bt_harvester returns floats too
            p = float(val) if isinstance(val, (int, float)) else float(val.get("price", 0))
            if p > 1.0:
                bk_markets_with_price[mkt_slug][out_key] = {"price": p}

    # Register bookmaker
    unified["bookmakers"][bk_slug] = {
        "bookmaker":    bk_name,
        "slug":         bk_slug,
        "markets":      bk_markets_with_price,
        "market_count": len(bk_markets_with_price),
    }
    unified["markets_by_bk"][bk_slug] = bk_markets_with_price

    # Update best-odds
    for mkt_slug, outcomes in bk_markets_with_price.items():
        if mkt_slug not in unified["best"]:
            unified["best"][mkt_slug] = {}
        for out_key, price_dict in outcomes.items():
            p = price_dict["price"]
            existing = unified["best"][mkt_slug].get(out_key)
            if existing is None or p > existing["odd"]:
                unified["best"][mkt_slug][out_key] = {"odd": p, "bk": bk_slug}


# ══════════════════════════════════════════════════════════════════════════════
# UNIFIED DIRECT STREAM  (SP + BT + OD)
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/debug/unified/stream/<mode>/<sport_slug>")
def debug_stream_unified(mode: str, sport_slug: str):
    fetch_full = request.args.get("full", "true").lower() in ("1", "true")
    max_m      = int(request.args.get("max", 20))
    log_event("debug_unified_stream", {"sport": sport_slug, "mode": mode})

    def _gen():
        from app.workers.sp_harvester  import fetch_upcoming_stream, fetch_live_stream
        from app.workers.bt_harvester  import get_full_markets  as bt_get_full_markets
        from app.workers.od_harvester  import (
            fetch_event_detail  as od_fetch_event_detail,
            slug_to_od_sport_id as od_slug_to_sport_id,
        )

        yield _sse("meta", {
            "source": "unified_direct",
            "sport":  sport_slug,
            "mode":   mode,
            "now":    _now_utc().isoformat(),
            "bookmakers": ["sp", "bt", "od"],
        })

        try:
            stream = (
                fetch_live_stream(sport_slug, fetch_full_markets=fetch_full)
                if mode == "live"
                else fetch_upcoming_stream(sport_slug, max_matches=max_m, fetch_full_markets=fetch_full)
            )
            count = 0

            for sp_match in stream:
                count += 1
                betradar_id  = sp_match.get("betradar_id")
                od_sport_id  = od_slug_to_sport_id(sport_slug)

                # ── 1. Normalise the SportPesa base payload ───────────────
                sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")

                # ── 2. Enrich with Betika ─────────────────────────────────
                if betradar_id:
                    try:
                        bt_markets = bt_get_full_markets(betradar_id, sport_slug)
                        if bt_markets:
                            _merge_bk_into_unified(sp_clean, "bt", "BETIKA", bt_markets)
                    except Exception:
                        pass

                # ── 3. Enrich with OdiBets ────────────────────────────────
                if betradar_id:
                    try:
                        od_markets, od_meta = od_fetch_event_detail(
                            event_id=betradar_id,
                            od_sport_id=od_sport_id,
                        )
                        if od_markets:
                            _merge_bk_into_unified(sp_clean, "od", "ODIBETS", od_markets)

                            # Back-fill missing team names / competition from OD meta
                            if od_meta:
                                if not sp_clean.get("home_team") and od_meta.get("home_team"):
                                    sp_clean["home_team"] = od_meta["home_team"]
                                if not sp_clean.get("away_team") and od_meta.get("away_team"):
                                    sp_clean["away_team"] = od_meta["away_team"]
                                if not sp_clean.get("competition") and od_meta.get("competition"):
                                    sp_clean["competition"] = od_meta["competition"]
                    except Exception:
                        pass

                # ── 4. Finalise counts / slugs ────────────────────────────
                sp_clean["bk_count"]     = len(sp_clean["bookmakers"])
                sp_clean["market_slugs"] = list(sp_clean["best"].keys())
                sp_clean["market_count"] = len(sp_clean["best"])
                sp_clean["best_odds"]    = sp_clean["best"]

                yield _sse("batch", {
                    "matches": [sp_clean],
                    "batch":   count,
                    "of":      "unknown",
                    "offset":  count - 1,
                })
                yield _keepalive()

            yield _sse("list_done", {"total_sent": count})
            yield _sse("done",      {"status": "finished", "total_sent": count})

        except Exception as exc:
            yield _sse("error", {"error": str(exc)})

    return Response(stream_with_context(_gen()), headers=config._SSE_HEADERS)