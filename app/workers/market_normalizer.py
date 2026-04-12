from functools import lru_cache

from app.views.customer.utils import _normalise_sport_slug


@lru_cache(maxsize=4096)
def _normalize_market_slug(raw_slug: str, h_clean: str, a_clean: str) -> str:
    m = raw_slug.lower()
    m = _RE_SPORT_PREFIX.sub("", m)
    m = m.replace("1st_half___", "first_half_").replace("1st_half_", "first_half_")
    m = m.replace("2nd_half___", "second_half_").replace("2nd_half_", "second_half_")
    if h_clean and len(h_clean) > 3 and h_clean in m:
        m = m.replace(h_clean, "home")
    if a_clean and len(a_clean) > 3 and a_clean in m:
        m = m.replace(a_clean, "away")
    m = m.replace("___", "_").replace("_&_", "_and_").replace(" ", "_")
    m = (m
         .replace("both_teams_to_score", "btts")
         .replace("over_under_goals", "over_under")
         .replace("total_goals", "over_under")
         .replace("total_points", "over_under"))

    if m in ("1x2", "moneyline", "3_way", "match_winner"):
        m = "match_winner"
    elif m in ("btts", "both_teams_to_score", "gg_ng", "both_teams_to_score__gg_ng_"):
        m = "btts"
    if m.endswith("_1x2"):
        m = m.replace("_1x2", "_match_winner")
    if m.startswith("1x2_"):
        m = m.replace("1x2_", "match_winner_")
    if m == "match_winner_btts":
        m = "btts_and_result"
    if m.startswith("match_winner_over_under") or m.startswith("match_winner_and_over_under"):
        m = (m
             .replace("match_winner_over_under", "result_and_over_under")
             .replace("match_winner_and_over_under", "result_and_over_under"))
    if m == "draw_no_bet_full_time":
        m = "draw_no_bet"

    m = _RE_EXACT_GOALS.sub("exact_goals", m)
    m = m.replace(".", "_")
    if "handicap_-" in m:
        m = m.replace("handicap_-", "handicap_minus_")
    elif "handicap_" in m:
        m = m.replace("handicap_+", "handicap_")
    m = _RE_TRAILING_ZERO.sub("_0", m)
    m = _RE_MULTI_UNDERSCORE.sub("_", m).strip("_")
    return m



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

    h_team, a_team = str(h_val or "Home"), str(a_val or "Away")
    h_clean = _RE_NON_ALPHANUM.sub('_', h_team.lower()).strip('_')
    a_clean = _RE_NON_ALPHANUM.sub('_', a_team.lower()).strip('_')

    sport_val = raw_match.get("sport") or raw_match.get("sport_name") or "soccer"
    if isinstance(sport_val, dict): sport_val = sport_val.get("name", "soccer")
    sport_slug = _normalise_sport_slug(str(sport_val))

    best_mock, bk_markets, standardized_markets = {}, {}, {}
    raw_list = raw_match.get("markets", {})
    if not raw_list: raw_list = []

    if isinstance(raw_list, list):
        for mkt_obj in raw_list:
            if not isinstance(mkt_obj, dict): continue
            m_id = mkt_obj.get("id")
            spec = str(mkt_obj.get("specValue") or mkt_obj.get("specialValue") or "")
            if spec.endswith(".00"): spec = spec[:-3]
            elif spec.endswith(".0"): spec = spec[:-2]

            name = str(mkt_obj.get("name") or "").lower()
            m = name

            if m_id in (52, 105):        m = f"over_under_{spec}"
            elif m_id == 54:             m = f"first_half_over_under_{spec}"
            elif m_id in (51, 184):      m = f"asian_handicap_{spec}"
            elif m_id == 53:             m = f"first_half_asian_handicap_{spec}"
            elif m_id in (55, 151):      m = f"european_handicap_{spec}"
            elif m_id in (208, 196):     m = f"result_and_over_under_{spec}"
            elif m_id in (386, 124):     m = "btts_and_result"
            elif m_id in (10, 194):      m = "match_winner"
            elif m_id in (43, 138):      m = "btts"
            elif m_id in (46, 147):      m = "double_chance"
            elif m_id in (47, 166):      m = "draw_no_bet"

            standardized_markets[m] = {}
            for sel in mkt_obj.get("selections", []):
                if not isinstance(sel, dict): continue
                ok = str(sel.get("shortName") or sel.get("name") or "")
                try: price = float(sel.get("odds", 0))
                except: price = 0.0
                if price > 1.0: standardized_markets[m][ok] = price
    else:
        for mkt_slug, outcomes in raw_list.items():
            m = str(mkt_slug or "")
            standardized_markets[m] = {}
            if not isinstance(outcomes, dict): continue
            for out_key, price in outcomes.items():
                try: p = float(price) if not isinstance(price, dict) else float(price.get("price", 0))
                except: p = 0.0
                if p > 1.0: standardized_markets[m][str(out_key)] = p

    for mkt_slug, outcomes in standardized_markets.items():
        if not outcomes: continue

        m = _normalize_market_slug(str(mkt_slug), h_clean, a_clean)
        bk_markets[m] = {}
        best_mock[m] = {}

        for ok_raw, p in outcomes.items():
            ok_raw, ok_lower = str(ok_raw), str(ok_raw).lower()
            ok = "X" if ("draw" in ok_lower and "or" not in ok_lower and "&" not in ok_lower) else ok_raw
            if h_clean and h_clean in ok_lower.replace(" ", "_") and "or" not in ok_lower and "&" not in ok_lower: ok = "1"
            if a_clean and a_clean in ok_lower.replace(" ", "_") and "or" not in ok_lower and "&" not in ok_lower: ok = "2"

            ok_lower = ok.lower()

            if "double_chance" in m:
                ok = (ok_lower.replace("or", "").replace("/", "").replace("draw", "x").replace(h_clean, "1").replace(a_clean, "2").replace("home", "1").replace("away", "2").replace(" ", "").upper())
                if ok == "X1": ok = "1X"
                elif ok == "2X": ok = "X2"
                elif ok == "21": ok = "12"

            elif "btts_and" in m or "result_and" in m:
                suffix = "Yes" if "yes" in ok_lower else "No" if "no" in ok_lower else "Over" if "over" in ok_lower else "Under" if "under" in ok_lower else ""
                prefix = "X" if "draw" in ok_lower or "x" in ok_lower.split() else "1" if h_clean in ok_lower.replace(" ", "_") or "home" in ok_lower or "1" in ok_lower.split() else "2" if a_clean in ok_lower.replace(" ", "_") or "away" in ok_lower or "2" in ok_lower.split() else ""
                
                if prefix and suffix: ok = f"{prefix} {suffix}"
                else:
                    ok = ok_lower.replace(" & ", " ").replace("and", "").replace("yes", "Yes").replace("no", "No").replace("draw", "X").replace("home", "1").replace("away", "2").replace(h_clean, "1").replace(a_clean, "2")
                    ok = ok.replace("1", "1 ").replace("x", "X ").replace("2", "2 ").replace("  ", " ").strip().title().replace(" X", " X")
            else:
                if ok_lower in ("yes", "y"): ok = "Yes"
                elif ok_lower in ("no", "n"): ok = "No"
                elif "over" in ok_lower or ok_lower == "ov": ok = "Over"
                elif "under" in ok_lower or ok_lower == "un": ok = "Under"
                elif ok_lower == "odd": ok = "Odd"
                elif ok_lower == "even": ok = "Even"
                elif ok_lower == "home": ok = "1"
                elif ok_lower == "away": ok = "2"
                elif ok_lower == "1x": ok = "1X"
                elif ok_lower == "x2": ok = "X2"
                elif ok_lower == "12": ok = "12"

                if "over_under" in m and ok not in ("Over", "Under"):
                    if ok_lower.startswith("o"): ok = "Over"
                    elif ok_lower.startswith("u"): ok = "Under"

                if "exact_goals" in m: ok = ok.replace(" OR MORE", "+").replace(" or more", "+").replace(" and over", "+")
                if "correct_score" in m: ok = ok.replace("-", ":")

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
        "sport":           sport_slug,
        "start_time":      st,
        "status":          "IN_PLAY" if mode == "live" else "PRE_MATCH",
        "is_live":         mode == "live",
        "bk_count":        1,
        "market_count":    len(bk_markets),
        "market_slugs":    list(bk_markets.keys()),
        "bookmakers": { bk_slug: { "bookmaker": bk_name, "slug": bk_slug, "markets": bk_markets, "market_count": len(bk_markets) } },
        "markets_by_bk": {bk_slug: bk_markets},
        "best":            best_mock,
        "best_odds":       best_mock,
        "has_arb":  False, "has_ev": False, "has_sharp": False,
    }
