#!/usr/bin/env python3
"""
test_od_mappers.py — quick sanity check for all OdiBets mapper classes.

Run from project root:
    python test_od_mappers.py

Checks:
  • No NameError / ImportError in any mapper file
  • Known markets resolve to expected canonical slugs
  • transform_outcome() works for standard keys
"""
import sys, traceback

TESTS = [
    # (sport, market_slug, expected_canonical, raw_outcome, expected_outcome)
    ("soccer",       "1x2",                           "1x2",                    "1",       "1"),
    ("soccer",       "double_chance",                  "double_chance",           "1_or_x",  "1X"),
    ("soccer",       "over_under_goals_2_5",           "over_under_goals",        "over",    "Over"),
    ("soccer",       "over_under_goals_1_5",           "over_under_goals",        "under",   "Under"),
    ("soccer",       "btts",                           "btts",                    "yes",     "Yes"),
    ("soccer",       "asian_handicap_minus_1_5",       "asian_handicap",          "1",       "1"),
    ("soccer",       "european_handicap_0_1",          "european_handicap",       "X",       "X"),
    ("soccer",       "first_half_1x2",                 "first_half_1x2",          "1",       "1"),
    ("soccer",       "draw_no_bet",                    "draw_no_bet",             "1",       "1"),
    ("soccer",       "ht_ft",                          "ht_ft",                   "1/2",     "1/2"),
    ("soccer",       "exact_goals_6",                  "exact_goals",             "0",       "0"),
    ("soccer",       "1x2_btts",                       "result_and_btts",         "1_yes",   "1_yes"),
    ("soccer",       "1x2_over_under_2_5",             "result_and_total",        "1_over",  "1_over"),
    ("basketball",   "basketball_1x2",                 "1x2",                     "1",       "1"),
    ("basketball",   "basketball_moneyline",           "basketball_moneyline",    "1",       "1"),
    ("basketball",   "over_under_basketball_points_215_5", "total_points",        "over",    "Over"),
    ("basketball",   "basketball__minus_10_5",         "point_spread",            "1",       "1"),
    ("basketball",   "basketball__1",                  "quarter_winner",          "1",       "1"),
    ("tennis",       "tennis_match_winner",            "tennis_match_winner",     "1",       "1"),
    ("tennis",       "over_under_tennis_games_22_5",   "total_games",             "over",    "Over"),
    ("tennis",       "tennis_set_betting_2_0",         "set_betting",             "2:0",     "2:0"),
    ("tennis",       "tennis_game_handicap_minus_1_5", "games_handicap",          "1",       "1"),
    ("ice-hockey",   "ice-hockey_1x2",                 "1x2",                     "1",       "1"),
    ("ice-hockey",   "over_under_hockey_goals_5_5",    "total_goals",             "over",    "Over"),
    ("ice-hockey",   "hockey_asian_handicap_minus_1_5","asian_handicap",          "2",       "2"),
    ("volleyball",   "volleyball_match_winner",        "volleyball_winner",       "1",       "1"),
    ("volleyball",   "volleyball_total_points_178_5",  "volleyball_total_points", "over",    "Over"),
    ("cricket",      "cricket_winner_incl_super_over", "cricket_match_winner",    "1",       "1"),
    ("cricket",      "over_under_cricket_runs_180_5",  "total_runs_match",        "over",    "Over"),
    ("rugby",        "rugby_1x2",                     "rugby_result",            "1",       "1"),
    ("rugby",        "rugby_spread_minus_10_5",        "point_spread",            "1",       "1"),
    ("baseball",     "over_under_baseball_runs_7_5",   "total_runs",              "over",    "Over"),
    ("baseball",     "baseball_moneyline",             "baseball_moneyline",      "1",       "1"),
    ("esoccer",      "esoccer_1x2",                    "1x2",                     "1",       "1"),
    ("esoccer",      "over_under_efootball_goals_1_5", "over_under_goals",        "over",    "Over"),
    ("table-tennis", "soccer_winner",                  "tt_winner",               "1",       "1"),
    ("table-tennis", "soccer_total_points_77_5",       "tt_total_points",         "over",    "Over"),
    ("handball",     "handball_1x2",                   "1x2",                     "1",       "1"),
    ("handball",     "over_under_handball_goals_55_5", "total_goals",             "over",    "Over"),
]

def run_tests():
    from app.utils.mapping.odibets import resolve_od_market  # noqa

    passed = failed = 0
    for sport, slug, exp_can, raw_out, exp_out in TESTS:
        try:
            can_slug, outcomes = resolve_od_market(sport, slug, {raw_out: 1.85})
            ok_slug = exp_can in can_slug or can_slug.startswith(exp_can.split("_")[0])
            ok_out  = exp_out in outcomes

            if ok_slug and ok_out:
                print(f"  ✅ [{sport}] {slug}")
                passed += 1
            else:
                print(f"  ❌ [{sport}] {slug}")
                if not ok_slug:
                    print(f"       slug:    got={can_slug!r}  want={exp_can!r}")
                if not ok_out:
                    print(f"       outcome: got={list(outcomes.keys())}  want={exp_out!r}")
                failed += 1
        except Exception as exc:
            print(f"  💥 [{sport}] {slug} → {exc}")
            traceback.print_exc()
            failed += 1

    print(f"\n{'─'*50}")
    print(f"  {passed} passed / {failed} failed  ({passed+failed} total)")
    return failed == 0


def test_class_names():
    """Verify no NameError from wrong class names in dispatcher functions."""
    errors = []
    imports = [
        ("app.utils.mapping.odibets",             "OdibetsBaseballMapper"),
        ("app.utils.mapping.odibets.odibets_basketball_mapper",  "OdibetsBasketballMapper"),
        ("app.utils.mapping.odibets.odibets_boxing_mapper",      "OdibetsBoxingMapper"),
        ("app.utils.mapping.odibets.odibets_cricket_mapper",     "OdibetsCricketMapper"),
        ("app.utils.mapping.odibets.odibets_esoccer_mapper",     "OdibetsEsoccerMapper"),
        ("app.utils.mapping.odibets.odibets_handball_mapper",    "OdibetsHandballMapper"),
        ("app.utils.mapping.odibets.odibets_ice_hockey_mapper",  "OdibetsIceHockeyMapper"),
        ("app.utils.mapping.odibets.odibets_rugby_mapper",       "OdibetsRugbyMapper"),
        ("app.utils.mapping.odibets.odibets_soccer_mapper",      "OdibetsSoccerMapper"),
        ("app.utils.mapping.odibets.odibets_table_tennis_mapper","OdibetsTableTennisMapper"),
        ("app.utils.mapping.odibets.odibets_tennis_mapper",      "OdibetsTennisMapper"),
        ("app.utils.mapping.odibets.odibets_volleyball_mapper",  "OdibetsVolleyballMapper"),
    ]
    for module_path, class_name in imports:
        try:
            import importlib
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            print(f"  ✅ {class_name}")
        except (ImportError, AttributeError) as exc:
            print(f"  ❌ {class_name}: {exc}")
            errors.append((module_path, class_name, exc))
    return len(errors) == 0


if __name__ == "__main__":
    print("\nChecking class names…")
    class_ok = test_class_names()

    print("\nRunning market resolution tests…")
    tests_ok = run_tests()

    sys.exit(0 if (class_ok and tests_ok) else 1)