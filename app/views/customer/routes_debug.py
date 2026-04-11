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

        # 3. Strip Betika's / OdiBets' sports prefixes
        m = re.sub(
            r"^(soccer|basketball|tennis|ice_hockey|volleyball|cricket|rugby|"
            r"table_tennis|handball|mma|boxing|darts|esoccer|baseball|american_football)_+",
            "", m
        )

        # 4. Fix Half Time labels
        m = m.replace("1st_half___", "first_half_").replace("1st_half_", "first_half_")
        m = m.replace("2nd_half___", "second_half_").replace("2nd_half_", "second_half_")
        m = m.replace("1st_2nd_half_", "both_halves_")

        # 5. Strip Team Names (Betika / OdiBets inject team names into the market slug)
        if h_clean and len(h_clean) > 3 and h_clean in m: m = m.replace(h_clean, "home")
        if a_clean and len(a_clean) > 3 and a_clean in m: m = m.replace(a_clean, "away")

        # 6. Standardize core names
        m = m.replace("___", "_").replace("_&_", "_and_").replace(" ", "_")
        m = m.replace("both_teams_to_score", "btts")
        m = m.replace("over_under_goals", "over_under").replace("total_goals", "over_under")

        # Normalize 1x2 → match_winner
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
                # Standardize Outcomes
                ok = out_key.replace("draw", "X").replace("Draw", "X")

                # Standardize Exact goals (6+ vs 6 OR MORE)
                if "exact_goals" in m:
                    ok = ok.replace(" OR MORE", "+").replace(" or more", "+")

                # Standardize Correct score keys (e.g. "0-0" -> "0:0")
                if "correct_score" in m:
                    ok = ok.replace("-", ":")

                ok_lower = ok.lower()
                if ok_lower == "yes":   ok = "Yes"
                elif ok_lower == "no":  ok = "No"
                elif ok_lower == "over": ok = "Over"
                elif ok_lower == "under": ok = "Under"
                elif ok_lower == "odd":  ok = "Odd"
                elif ok_lower == "even": ok = "Even"

                best_mock[m][ok] = {"odd": p, "bk": bk_slug}
                bk_markets[m][ok] = {"price": p}

    actual_id = (
        raw_match.get(f"{bk_slug}_match_id")
        or raw_match.get(f"{bk_slug}_game_id")
        or raw_match.get("match_id")
        or count
    )
    b_id = raw_match.get("betradar_id") or raw_match.get(f"{bk_slug}_parent_id")

    return {
        "match_id":        int(actual_id),
        "join_key":        f"br_{b_id}" if b_id else f"{bk_slug}_{actual_id}",
        "parent_match_id": b_id,
        "home_team":       raw_match.get("home_team", ""),
        "away_team":       raw_match.get("away_team", ""),
        "competition":     raw_match.get("competition_name") or raw_match.get("competition", ""),
        "sport":           _normalise_sport_slug(raw_match.get("sport", "soccer")),
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
# HELPER: merge a secondary bookmaker's clean payload into the primary one
# ══════════════════════════════════════════════════════════════════════════════
def _merge_bk_into_primary(primary: dict, secondary_clean: dict, bk_slug: str) -> None:
    """
    Merge `secondary_clean` (output of _unify_match_payload) into `primary`
    in-place, updating bookmakers, markets_by_bk, best and bk_count.
    """
    primary["bookmakers"][bk_slug]    = secondary_clean["bookmakers"][bk_slug]
    primary["markets_by_bk"][bk_slug] = secondary_clean["markets_by_bk"][bk_slug]
    primary["bk_count"]               = len(primary["bookmakers"])

    for mkt_slug, outcomes in secondary_clean["markets_by_bk"][bk_slug].items():
        if mkt_slug not in primary["best"]:
            primary["best"][mkt_slug] = {}
        for out_key, price_dict in outcomes.items():
            p = price_dict["price"]
            current_best = primary["best"][mkt_slug].get(out_key)
            if current_best is None or p > current_best["odd"]:
                primary["best"][mkt_slug][out_key] = {"odd": p, "bk": bk_slug}


# ══════════════════════════════════════════════════════════════════════════════
# UNIFIED DIRECT STREAM  –  SportPesa + Betika + OdiBets
# ══════════════════════════════════════════════════════════════════════════════
@bp_odds_customer.route("/odds/debug/unified/stream/<mode>/<sport_slug>")
def debug_stream_unified(mode: str, sport_slug: str):
    fetch_full = request.args.get("full", "true").lower() in ("1", "true")
    max_m      = int(request.args.get("max", 20))
    log_event("debug_unified_stream", {"sport": sport_slug, "mode": mode})

    def _gen():
        from app.workers.sp_harvester import fetch_upcoming_stream, fetch_live_stream
        from app.workers.bt_harvester  import get_full_markets        as bt_get_full_markets
        from app.workers.od_harvester  import fetch_event_detail      as od_fetch_event_detail

        yield _sse("meta", {
            "source": "unified_direct",
            "sport":  sport_slug,
            "mode":   mode,
            "now":    _now_utc().isoformat(),
            "bks":    ["sp", "bt", "od"],
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
                betradar_id = sp_match.get("betradar_id")

                # ── Betika ────────────────────────────────────────────────────
                bt_markets: dict = {}
                if betradar_id:
                    try:
                        bt_markets = bt_get_full_markets(betradar_id, sport_slug)
                    except Exception:
                        pass

                # ── OdiBets ───────────────────────────────────────────────────
                od_markets: dict = {}
                od_meta:    dict = {}
                if betradar_id:
                    try:
                        od_markets, od_meta = od_fetch_event_detail(
                            betradar_id,
                            od_sport_id=1,          # sp_harvester normalises sport; 1 = soccer default
                        )
                    except Exception:
                        pass

                # ── Build the base payload from SportPesa ─────────────────────
                sp_clean = _unify_match_payload(sp_match, count, mode, "sp", "SPORTPESA")

                # ── Merge Betika ──────────────────────────────────────────────
                if bt_markets:
                    bt_raw   = {"markets": bt_markets}
                    bt_clean = _unify_match_payload(bt_raw, count, mode, "bt", "BETIKA")
                    _merge_bk_into_primary(sp_clean, bt_clean, "bt")

                # ── Merge OdiBets ─────────────────────────────────────────────
                # od_markets from fetch_event_detail is already a canonical
                # {slug: {outcome: price}} dict produced by the betika mapper,
                # so we wrap it the same way we do for Betika.
                if od_markets:
                    # od_harvester already normalises via get_market_slug /
                    # normalize_outcome, but we still run it through
                    # _unify_match_payload to apply the same final-pass
                    # normalisation (strip sports prefix, fix half-time labels,
                    # etc.) and to get the standard bookmakers/markets_by_bk shape.
                    od_home = od_meta.get("home_team") or sp_match.get("home_team", "")
                    od_away = od_meta.get("away_team") or sp_match.get("away_team", "")
                    od_raw  = {
                        "markets":   od_markets,
                        "home_team": od_home,
                        "away_team": od_away,
                    }
                    od_clean = _unify_match_payload(od_raw, count, mode, "od", "ODIBETS")
                    _merge_bk_into_primary(sp_clean, od_clean, "od")

                # ── Finalise slugs / counts ───────────────────────────────────
                sp_clean["market_slugs"] = list(sp_clean["best"].keys())
                sp_clean["market_count"] = len(sp_clean["best"])

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