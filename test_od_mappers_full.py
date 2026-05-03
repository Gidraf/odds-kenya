#!/usr/bin/env python3
"""
test_od_mappers_full.py
========================
Exhaustive test suite for all OdiBets market mappers.
Generates tests for every static market + all regex patterns
with representative line/handicap values.

Run:
    python test_od_mappers_full.py
    python test_od_mappers_full.py -v      # verbose: show all results
    python test_od_mappers_full.py -f      # only show failures
"""
from __future__ import annotations
import sys
import importlib
from typing import NamedTuple, Optional


class Case(NamedTuple):
    sport:         str
    market_slug:   str
    expect_slug:   str          # expected canonical slug (substring match)
    raw_outcome:   str
    expect_outcome: str         # expected canonical outcome key
    desc:          str = ""


# ══════════════════════════════════════════════════════════════════════════════
# SOCCER  (OdibetsSoccerMapper)
# ══════════════════════════════════════════════════════════════════════════════
SOCCER: list[Case] = [
    # Static
    Case("soccer","1x2",                        "1x2",                 "1","1","1x2 home"),
    Case("soccer","1x2",                        "1x2",                 "X","X","1x2 draw"),
    Case("soccer","1x2",                        "1x2",                 "2","2","1x2 away"),
    Case("soccer","soccer_1x2",                 "1x2",                 "1","1","soccer_1x2"),
    Case("soccer","double_chance",              "double_chance",       "1_or_x","1X","dc 1X"),
    Case("soccer","double_chance",              "double_chance",       "x_or_2","X2","dc X2"),
    Case("soccer","double_chance",              "double_chance",       "1_or_2","12","dc 12"),
    Case("soccer","draw_no_bet",                "draw_no_bet",         "1","1","dnb home"),
    Case("soccer","draw_no_bet",                "draw_no_bet",         "2","2","dnb away"),
    Case("soccer","btts",                       "btts",                "yes","Yes","btts yes"),
    Case("soccer","btts",                       "btts",                "no","No","btts no"),
    Case("soccer","first_half_btts",            "btts",                "yes","Yes","1H btts"),
    Case("soccer","ht_ft",                      "ht_ft",               "1/1","1/1","htft 1/1"),
    Case("soccer","ht_ft",                      "ht_ft",               "X/2","X/2","htft X/2"),
    Case("soccer","first_half_1x2",             "first_half_1x2",      "1","1","1H 1x2 home"),
    Case("soccer","first_half_1x2",             "first_half_1x2",      "X","X","1H 1x2 draw"),
    Case("soccer","first_half_double_chance",   "double_chance",       "1_or_x","1X","1H dc"),
    Case("soccer","first_team_to_score_1",      "first_team_to_score", "1","1","fts home"),
    Case("soccer","first_team_to_score_1",      "first_team_to_score", "2","2","fts away"),
    Case("soccer","anytime_goalscorer",         "anytime_goalscorer",  "messi","messi","anytime scorer"),
    Case("soccer","first_goalscorer_1",         "first_goalscorer",    "ronaldo","ronaldo","first scorer"),
    Case("soccer","last_goalscorer",            "last_goalscorer",     "salah","salah","last scorer"),
    Case("soccer","multigoals",                 "multigoals",          "1_2","1_2","multigoals"),
    Case("soccer","1x2_btts",                   "result_and_btts",     "1_yes","1_yes","1x2+btts"),
    Case("soccer","double_chance_btts",         "double_chance_and_btts","1_or_x_yes","1_or_x_yes","dc+btts"),
    Case("soccer","first_half_double_chance_btts","double_chance_and_btts","1_or_x_yes","1_or_x_yes","1H dc+btts"),
    Case("soccer","second_half_1x2_btts",       "result_and_btts",     "1_yes","1_yes","2H 1x2+btts"),
    # O/U goals — multiple lines
    *[Case("soccer", f"over_under_goals_{line.replace('.','_')}", "over_under_goals", "over","Over", f"O/U {line}")
      for line in ["0.5","1.5","2.5","3.5","4.5","5.5","6.5","7.5"]],
    *[Case("soccer", f"over_under_goals_{line.replace('.','_')}", "over_under_goals", "under","Under", f"U {line}")
      for line in ["0.5","1.5","2.5","3.5","4.5"]],
    # First half O/U
    *[Case("soccer", f"first_half_over_under_goals_{line.replace('.','_')}", "over_under_goals", "over","Over", f"1H O/U {line}")
      for line in ["0.5","1.5","2.5","3.5"]],
    # BTTS + total combos
    *[Case("soccer", f"over_under_btts_{line.replace('.','_')}", "btts_and_total", "yes_over","yes_over", f"btts+ou {line}")
      for line in ["1.5","2.5","3.5"]],
    # European handicap
    *[Case("soccer", f"european_handicap_{h.replace('-','').replace('.','_')}", "european_handicap", "1","1", f"EH {h}")
      for h in ["0_1","1_0","0_2","2_0","1_1"]],
    # Asian handicap positive
    *[Case("soccer", f"asian_handicap_{h.replace('.','_')}", "asian_handicap", "1","1", f"AH +{h}")
      for h in ["0.5","1.0","1.5","2.0","2.5"]],
    # Asian handicap negative
    *[Case("soccer", f"asian_handicap_minus_{h.replace('.','_')}", "asian_handicap", "2","2", f"AH -{h}")
      for h in ["0.5","1.0","1.5","2.0","2.5","3.0"]],
    # First half asian handicap
    *[Case("soccer", f"first_half_asian_handicap_{h.replace('.','_')}", "asian_handicap", "1","1", f"1H AH +{h}")
      for h in ["0.5","1.0","1.5"]],
    *[Case("soccer", f"first_half_asian_handicap_minus_{h.replace('.','_')}", "asian_handicap", "2","2", f"1H AH -{h}")
      for h in ["0.5","1.0","1.5"]],
    # Home/Away team totals
    *[Case("soccer", f"home_over_under_{h.replace('.','_')}", "team_total_goals", "over","Over", f"home total {h}")
      for h in ["0.5","1.5","2.5","3.5"]],
    *[Case("soccer", f"away_over_under_{h.replace('.','_')}", "team_total_goals", "under","Under", f"away total {h}")
      for h in ["0.5","1.5","2.5","3.5"]],
    # First half team totals
    *[Case("soccer", f"first_half_home_over_under_{h.replace('.','_')}", "team_total_goals", "over","Over", f"1H home total {h}")
      for h in ["0.5","1.5"]],
    *[Case("soccer", f"first_half_away_over_under_{h.replace('.','_')}", "team_total_goals", "under","Under", f"1H away total {h}")
      for h in ["0.5","1.5"]],
    # Winning margin bands
    *[Case("soccer", f"winning_margin_{n}", "winning_margin", "1_by_1","1_by_1", f"margin band {n}")
      for n in ["3","5","7","10"]],
    # Exact goals
    *[Case("soccer", f"exact_goals_{n}", "exact_goals", str(n-1),str(n-1), f"exact goals max={n}")
      for n in [4,5,6,7]],
    # First half exact goals
    *[Case("soccer", f"first_half_exact_goals_{n}", "exact_goals", "0","0", f"1H exact goals max={n}")
      for n in [3,4]],
    # Result + Over/Under combos
    *[Case("soccer", f"1x2_over_under_{l.replace('.','_')}", "result_and_total", "1_over","1_over", f"1x2+ou {l}")
      for l in ["1.5","2.5","3.5"]],
    *[Case("soccer", f"double_chance_over_under_{l.replace('.','_')}", "double_chance_and_total", "1_or_x_over","1_or_x_over", f"dc+ou {l}")
      for l in ["1.5","2.5"]],
    *[Case("soccer", f"second_half_1x2_over_under_{l.replace('.','_')}", "result_and_total", "1_over","1_over", f"2H 1x2+ou {l}")
      for l in ["0.5","1.5"]],
    # First half European handicap
    *[Case("soccer", f"first_half_european_handicap_{h}", "european_handicap", "1","1", f"1H EH {h}")
      for h in ["0_1","1_0","0_2"]],
]

# ══════════════════════════════════════════════════════════════════════════════
# BASKETBALL  (OdibetsBasketballMapper)
# ══════════════════════════════════════════════════════════════════════════════
BASKETBALL: list[Case] = [
    Case("basketball","basketball_1x2",              "1x2",                 "1","1","bball 1x2 home"),
    Case("basketball","basketball_1x2",              "1x2",                 "X","X","bball 1x2 draw"),
    Case("basketball","basketball_moneyline",         "basketball_moneyline","1","1","moneyline"),
    Case("basketball","basketball_draw_no_bet",       "draw_no_bet",         "1","1","dnb"),
    Case("basketball","basketball_ht_ft",             "ht_ft",               "1/1","1/1","ht/ft"),
    Case("basketball","first_half_basketball_1x2",    "first_half_1x2",      "2","2","1H 1x2"),
    Case("basketball","first_quarter_winning_margin", "quarter_winning_margin","1_by_5+","1_by_5+","Q1 margin"),
    # O/U total points — many lines
    *[Case("basketball", f"over_under_basketball_points_{l.replace('.','_')}", "total_points","over","Over",f"total pts {l}")
      for l in ["190.5","200.5","210.5","215.5","220.5","225.5","230.5","240.5","250.5"]],
    *[Case("basketball", f"over_under_basketball_points_{l.replace('.','_')}", "total_points","under","Under",f"total pts U{l}")
      for l in ["190.5","210.5","230.5"]],
    # Incl OT
    *[Case("basketball", f"over_under_basketball_points_incl_ot_{l.replace('.','_')}", "total_points","over","Over",f"total incl OT {l}")
      for l in ["210.5","215.5","225.5"]],
    # Point spreads (negative)
    *[Case("basketball", f"basketball__minus_{h.replace('.','_')}", "point_spread","1","1",f"spread -{h}")
      for h in ["0.5","1.5","2.5","3.5","4.5","5.5","6.5","7.5","8.5","9.5","10.5","12.5","14.5"]],
    # Point spreads (positive)
    *[Case("basketball", f"basketball__plus_{h.replace('.','_')}", "point_spread","2","2",f"spread +{h}")
      for h in ["0.5","1.5","2.5","3.5","5.5"]],
    # First half spread
    *[Case("basketball", f"first_half_basketball_spread_minus_{h.replace('.','_')}", "point_spread","1","1",f"1H spread -{h}")
      for h in ["1.5","2.5","3.5","4.5","5.5","6.5","7.5"]],
    # Quarter winner
    *[Case("basketball", f"basketball__{q}", "quarter_winner",str(q),str(q),f"Q{q} winner")
      for q in [1,2,3,4]],
    # Team totals incl OT
    *[Case("basketball", f"basketball_home_team_total_incl_ot_{l.replace('.','_')}", "team_total_points","over","Over",f"home total {l}")
      for l in ["105.5","110.5","115.5","120.5"]],
    *[Case("basketball", f"basketball_away_team_total_incl_ot_{l.replace('.','_')}", "team_total_points","under","Under",f"away total {l}")
      for l in ["105.5","110.5","115.5"]],
    # Moneyline + total combo
    *[Case("basketball", f"basketball_moneyline_and_total_{l.replace('.','_')}", "moneyline_and_total","1_over","1_over",f"ml+total {l}")
      for l in ["210.5","215.5","220.5","225.5"]],
]

# ══════════════════════════════════════════════════════════════════════════════
# TENNIS  (OdibetsTennisMapper)
# ══════════════════════════════════════════════════════════════════════════════
TENNIS: list[Case] = [
    Case("tennis","tennis_match_winner",         "tennis_match_winner",  "1","1","winner home"),
    Case("tennis","tennis_match_winner",         "tennis_match_winner",  "2","2","winner away"),
    Case("tennis","first_set_winner",            "first_set_winner",     "1","1","1st set winner home"),
    Case("tennis","first_set_winner",            "first_set_winner",     "2","2","1st set winner away"),
    Case("tennis","first_set_match_winner",      "first_set_match_winner","11","11","1st set+match"),
    Case("tennis","tennis_odd_even",             "odd_even",             "odd","odd","odd/even"),
    # Set betting
    *[Case("tennis", f"tennis_set_betting_{h}_{a}", "set_betting", f"{h}:{a}",f"{h}:{a}", f"set score {h}:{a}")
      for h,a in [("2","0"),("2","1"),("0","2"),("1","2")]],
    # Total games O/U — many lines
    *[Case("tennis", f"over_under_tennis_games_{l.replace('.','_')}", "total_games","over","Over",f"games O/U {l}")
      for l in ["18.5","19.5","20.5","21.5","22.5","23.5","24.5","25.5","26.5","27.5","28.5","30.5","32.5","34.5","36.5","38.5"]],
    *[Case("tennis", f"over_under_tennis_games_{l.replace('.','_')}", "total_games","under","Under",f"games U {l}")
      for l in ["18.5","22.5","26.5","30.5","34.5"]],
    # Games handicap negative
    *[Case("tennis", f"tennis_game_handicap_minus_{h.replace('.','_')}", "games_handicap","1","1",f"games hcp -{h}")
      for h in ["0.5","1.5","2.5","3.5","4.5","5.5","6.5"]],
    # Games handicap positive
    *[Case("tennis", f"tennis_game_handicap_{h.replace('.','_')}", "games_handicap","2","2",f"games hcp +{h}")
      for h in ["0.5","1.5","2.5","3.5","4.5"]],
    # Correct score (sets)
    *[Case("tennis", f"tennis_correct_score_{h}_{a}", "correct_score_sets",f"{h}_{a}",f"{h}_{a}",f"cs sets {h}:{a}")
      for h,a in [("2","1"),("2","0"),("0","2"),("1","2")]],
]

# ══════════════════════════════════════════════════════════════════════════════
# ICE HOCKEY  (OdibetsIceHockeyMapper)
# ══════════════════════════════════════════════════════════════════════════════
ICE_HOCKEY: list[Case] = [
    Case("ice-hockey","ice-hockey_1x2",          "1x2",              "1","1","1x2 home"),
    Case("ice-hockey","ice-hockey_1x2",          "1x2",              "X","X","1x2 draw"),
    Case("ice-hockey","hockey_double_chance",     "double_chance",    "1_or_x","1X","dc"),
    Case("ice-hockey","hockey_btts",              "btts",             "yes","Yes","btts"),
    Case("ice-hockey","hockey_draw_no_bet",       "draw_no_bet",      "1","1","dnb"),
    Case("ice-hockey","hockey_moneyline",         "hockey_moneyline", "1","1","moneyline"),
    Case("ice-hockey","hockey_first_goal_1",      "first_goal",       "1","1","first goal"),
    Case("ice-hockey","hockey_highest_scoring_period","highest_scoring_period","1st_period","1st_period","high scoring period"),
    # O/U goals many lines
    *[Case("ice-hockey", f"over_under_hockey_goals_{l.replace('.','_')}", "total_goals","over","Over",f"goals O/U {l}")
      for l in ["3.5","4.5","5.5","5.0","6.5","7.5","8.5","9.5"]],
    *[Case("ice-hockey", f"over_under_hockey_goals_{l.replace('.','_')}", "total_goals","under","Under",f"goals U{l}")
      for l in ["3.5","4.5","5.5","6.5"]],
    # European handicap
    *[Case("ice-hockey", f"hockey_european_handicap_{h}", "european_handicap","1","1",f"EH {h}")
      for h in ["0_1","1_0","0_2","2_0","1_1"]],
    # Asian handicap
    *[Case("ice-hockey", f"hockey_asian_handicap_{h.replace('.','_')}", "asian_handicap","1","1",f"AH +{h}")
      for h in ["0.5","1.0","1.5","2.0","2.5"]],
    *[Case("ice-hockey", f"hockey_asian_handicap_minus_{h.replace('.','_')}", "asian_handicap","2","2",f"AH -{h}")
      for h in ["0.5","1.0","1.5","2.0","2.5","3.0"]],
    # Fallback spread
    *[Case("ice-hockey", f"hockey__minus_{h.replace('.','_')}", "asian_handicap","1","1",f"spread -{h}")
      for h in ["0.5","1.5","2.5"]],
    # Team totals
    *[Case("ice-hockey", f"hockey_home_team_total_{l.replace('.','_')}", "team_total_goals","over","Over",f"home total {l}")
      for l in ["1.5","2.5","3.5","4.5"]],
    *[Case("ice-hockey", f"hockey_away_team_total_{l.replace('.','_')}", "team_total_goals","under","Under",f"away total {l}")
      for l in ["1.5","2.5","3.5"]],
    # Winning margin
    *[Case("ice-hockey", f"hockey_winning_margin_{n}", "winning_margin","1_by_1","1_by_1",f"margin {n}")
      for n in ["3","5","7"]],
    # 1x2 + O/U combo
    *[Case("ice-hockey", f"hockey_1x2_over_under_{l.replace('.','_')}", "result_and_total","1_over","1_over",f"1x2+ou {l}")
      for l in ["4.5","5.5","6.5"]],
]

# ══════════════════════════════════════════════════════════════════════════════
# VOLLEYBALL  (OdibetsVolleyballMapper)
# ══════════════════════════════════════════════════════════════════════════════
VOLLEYBALL: list[Case] = [
    Case("volleyball","volleyball_match_winner",  "volleyball_winner","1","1","winner"),
    Case("volleyball","first_set_winner",         "first_set_winner", "1","1","1st set winner"),
    Case("volleyball","volleyball__sr_correct_score_bestof_5","volleyball_set_betting","3:0","3:0","set betting"),
    Case("volleyball","volleyball_",              "volleyball_5th_set","yes","yes","5th set"),
    Case("volleyball","set_1_volleyball_",        "volleyball_set_odd_even","odd","odd","set 1 odd/even"),
    # Total points many lines
    *[Case("volleyball", f"volleyball_total_points_{l.replace('.','_')}", "volleyball_total_points","over","Over",f"total pts {l}")
      for l in ["155.5","160.5","165.5","170.5","175.5","178.5","180.5","185.5","190.5","195.5","200.5"]],
    *[Case("volleyball", f"volleyball_total_points_{l.replace('.','_')}", "volleyball_total_points","under","Under",f"total pts U{l}")
      for l in ["155.5","170.5","185.5"]],
    # Generic slug total points
    *[Case("volleyball", f"volleyball__{l.replace('.','_')}", "volleyball_total_points","over","Over",f"generic total {l}")
      for l in ["178.5","184.5","190.5"]],
    # Point handicap negative
    *[Case("volleyball", f"volleyball_point_handicap_minus_{h.replace('.','_')}", "volleyball_point_handicap","1","1",f"hcp -{h}")
      for h in ["1.5","2.5","3.5","4.5","5.5","6.5","7.5","8.5"]],
    # Point handicap positive
    *[Case("volleyball", f"volleyball_point_handicap_{h.replace('.','_')}", "volleyball_point_handicap","2","2",f"hcp +{h}")
      for h in ["1.5","2.5","3.5","4.5","5.5"]],
    # First set total
    *[Case("volleyball", f"first_set_total_points_{l.replace('.','_')}", "volleyball_set_total_points","over","Over",f"set1 total {l}")
      for l in ["40.5","42.5","44.5","45.5","47.5","50.5"]],
    # First set handicap
    *[Case("volleyball", f"first_set_point_handicap_minus_{h.replace('.','_')}", "volleyball_set_point_handicap","1","1",f"set1 hcp -{h}")
      for h in ["1.5","2.5","3.5","4.5"]],
    *[Case("volleyball", f"first_set_point_handicap_{h.replace('.','_')}", "volleyball_set_point_handicap","2","2",f"set1 hcp +{h}")
      for h in ["1.5","2.5","3.5"]],
]

# ══════════════════════════════════════════════════════════════════════════════
# CRICKET  (OdibetsCricketMapper)
# ══════════════════════════════════════════════════════════════════════════════
CRICKET: list[Case] = [
    Case("cricket","cricket_winner_incl_super_over","cricket_match_winner","1","1","winner incl SO"),
    Case("cricket","cricket_winner",                "cricket_match_winner","2","2","winner no SO"),
    Case("cricket","cricket_top_batsman",           "top_batsman",          "kohli","kohli","top batsman"),
    Case("cricket","cricket_top_batter",            "top_batsman",          "root","root","top batter alias"),
    Case("cricket","cricket_top_bowler",            "top_bowler",           "bumrah","bumrah","top bowler"),
    Case("cricket","cricket_man_of_match",          "man_of_match_cricket", "stokes","stokes","mom"),
    Case("cricket","cricket_method_of_dismissal",   "method_of_dismissal",  "caught","caught","dismissal"),
    Case("cricket","cricket_first_innings_lead",    "first_innings_lead",   "1","1","1st inns lead"),
    Case("cricket","cricket_toss_winner",           "toss_winner",          "1","1","toss"),
    Case("cricket","cricket_fifty_scored",          "fifty_scored",         "yes","yes","fifty"),
    Case("cricket","cricket_century_scored",        "century_scored",       "yes","yes","century"),
    # Total runs match
    *[Case("cricket", f"over_under_cricket_runs_{l.replace('.','_')}", "total_runs_match","over","Over",f"runs O/U {l}")
      for l in ["150.5","160.5","170.5","180.5","190.5","200.5","210.5","220.5","240.5","260.5","300.5","320.5"]],
    # Innings total runs
    *[Case("cricket", f"over_under_cricket_innings_{inn}_runs_{l.replace('.','_')}", "total_runs_innings","over","Over",f"inns {inn} runs {l}")
      for inn in ["1","2"] for l in ["150.5","180.5","200.5"]],
    # Player runs
    *[Case("cricket", f"cricket_player_runs_{l.replace('.','_')}", "player_runs","over","Over",f"player runs {l}")
      for l in ["25.5","30.5","40.5","50.5","60.5","75.5","100.5"]],
    # Player wickets
    *[Case("cricket", f"cricket_player_wickets_{l.replace('.','_')}", "player_wickets","over","Over",f"player wkts {l}")
      for l in ["1.5","2.5","3.5","4.5"]],
    # Fall of next wicket
    *[Case("cricket", f"cricket_fall_next_wicket_{l.replace('.','_')}", "fall_next_wicket","1_10","1_10",f"fall wkt {l}")
      for l in ["50_5","100_5","150_5"]],
    # Powerplay runs
    *[Case("cricket", f"cricket_powerplay_runs_{l.replace('.','_')}", "powerplay_runs","over","Over",f"powerplay {l}")
      for l in ["40.5","45.5","50.5","55.5","60.5"]],
]

# ══════════════════════════════════════════════════════════════════════════════
# RUGBY  (OdibetsRugbyMapper)
# ══════════════════════════════════════════════════════════════════════════════
RUGBY: list[Case] = [
    Case("rugby","rugby_1x2",            "rugby_result",     "1","1","1x2 home"),
    Case("rugby","rugby_1x2",            "rugby_result",     "X","X","1x2 draw"),
    Case("rugby","rugby_double_chance",  "double_chance",    "1_or_x","1X","dc"),
    Case("rugby","rugby_draw_no_bet",    "draw_no_bet",      "1","1","dnb"),
    Case("rugby","rugby_ht_ft",          "ht_ft",            "1/2","1/2","ht/ft"),
    Case("rugby","rugby_first_half_1x2", "first_half_result","1","1","1H 1x2"),
    # Winning margin bands
    *[Case("rugby", f"rugby_winning_margin_{n}", "winning_margin","1_by_1","1_by_1",f"margin {n}")
      for n in ["7","10","12","14","15","20","21"]],
    *[Case("rugby", f"rugby__{n}", "winning_margin","1_by_1","1_by_1",f"generic margin {n}")
      for n in ["7","12","15","20"]],
    # Full match spread negative
    *[Case("rugby", f"rugby_spread_minus_{h.replace('.','_')}", "point_spread","1","1",f"spread -{h}")
      for h in ["0.5","1.5","2.5","3.5","4.5","5.5","6.5","7.5","8.5","9.5","10.5","12.5","14.5","17.5","22.5"]],
    # Full match spread positive
    *[Case("rugby", f"rugby_spread_{h.replace('.','_')}", "point_spread","2","2",f"spread +{h}")
      for h in ["0.5","1.5","2.5","3.5","4.5","5.5"]],
    # First half spread
    *[Case("rugby", f"rugby_first_half_spread_minus_{h.replace('.','_')}", "point_spread","1","1",f"1H spread -{h}")
      for h in ["0.5","1.5","2.5","3.5","4.5","5.5","6.5","7.5"]],
    *[Case("rugby", f"rugby_first_half_spread_{h.replace('.','_')}", "point_spread","2","2",f"1H spread +{h}")
      for h in ["0.5","1.5","2.5","3.5"]],
]

# ══════════════════════════════════════════════════════════════════════════════
# BASEBALL  (OdibetsBaseballMapper)
# ══════════════════════════════════════════════════════════════════════════════
BASEBALL: list[Case] = [
    Case("baseball","baseball_1x2",        "1x2",               "1","1","1x2 home"),
    Case("baseball","baseball_moneyline",  "baseball_moneyline","1","1","moneyline"),
    Case("baseball","baseball_odd_even",   "odd_even",          "odd","odd","odd/even"),
    Case("baseball","baseball_f5_1x2",     "f5_winner",         "1","1","F5 winner"),
    # Full game O/U runs
    *[Case("baseball", f"over_under_baseball_runs_{l.replace('.','_')}", "total_runs","over","Over",f"runs O/U {l}")
      for l in ["6.5","7.0","7.5","8.0","8.5","9.0","9.5","10.0","10.5","11.5"]],
    # Full game spread
    *[Case("baseball", f"baseball_spread_{h.replace('.','_')}", "run_line","1","1",f"spread {h}")
      for h in ["0.5","1.0","1.5"]],
    # F5 O/U
    *[Case("baseball", f"over_under_baseball_f5_runs_{l.replace('.','_')}", "total_runs","over","Over",f"F5 runs O/U {l}")
      for l in ["3.5","4.0","4.5","5.0","5.5"]],
    # F5 spread
    *[Case("baseball", f"baseball_f5_spread_{h.replace('.','_')}", "run_line","1","1",f"F5 spread {h}")
      for h in ["0.5","1.0","1.5"]],
    # Home team total
    *[Case("baseball", f"baseball_home_team_total_{l.replace('.','_')}", "team_total_runs","over","Over",f"home total {l}")
      for l in ["3.5","4.0","4.5","5.0","5.5"]],
    # Away team total
    *[Case("baseball", f"baseball_away_team_total_{l.replace('.','_')}", "team_total_runs","under","Under",f"away total {l}")
      for l in ["3.5","4.5","5.5"]],
    # 1st inning 1X2
    *[Case("baseball", f"baseball_{n}st_inning_1x2_1" if n==1 else f"baseball_{n}nd_inning_1x2_1" if n==2 else f"baseball_{n}rd_inning_1x2_1" if n==3 else f"baseball_{n}th_inning_1x2_1",
           "first_inning_score" if n==1 else "inning_winner","1","1",f"inning {n} 1x2")
      for n in [1]],
]

# ══════════════════════════════════════════════════════════════════════════════
# ESOCCER  (OdibetsEsoccerMapper)
# ══════════════════════════════════════════════════════════════════════════════
ESOCCER: list[Case] = [
    Case("esoccer","esoccer_1x2",                "1x2",           "1","1","1x2 home"),
    Case("esoccer","esoccer_1x2",                "1x2",           "X","X","1x2 draw"),
    Case("esoccer","efootball_double_chance",     "double_chance", "1_or_x","1X","dc"),
    Case("esoccer","efootball_draw_no_bet",       "draw_no_bet",   "1","1","dnb"),
    # O/U goals
    *[Case("esoccer", f"over_under_efootball_goals_{l.replace('.','_')}", "over_under_goals","over","Over",f"goals O/U {l}")
      for l in ["0.5","1.5","2.5","3.5","4.5","5.5"]],
    # Asian handicap positive
    *[Case("esoccer", f"efootball_asian_handicap_{h.replace('.','_')}", "asian_handicap","1","1",f"AH +{h}")
      for h in ["0.5","1.0","1.5","2.0"]],
    # Asian handicap negative
    *[Case("esoccer", f"efootball_asian_handicap_minus_{h.replace('.','_')}", "asian_handicap","2","2",f"AH -{h}")
      for h in ["0.5","1.0","1.5","2.0"]],
]

# ══════════════════════════════════════════════════════════════════════════════
# TABLE TENNIS  (OdibetsTableTennisMapper)
# ══════════════════════════════════════════════════════════════════════════════
TABLE_TENNIS: list[Case] = [
    Case("table-tennis","soccer_winner",     "tt_winner",         "1","1","winner home"),
    Case("table-tennis","soccer_winner",     "tt_winner",         "2","2","winner away"),
    Case("table-tennis","soccer_",           "tt_winner",         "1","1","winner fallback"),
    # Total points
    *[Case("table-tennis", f"soccer_total_points_{l.replace('.','_')}", "tt_total_points","over","Over",f"total pts {l}")
      for l in ["55.5","60.5","65.5","70.5","75.5","77.5","80.5","85.5","90.5","95.5"]],
    # Generic slug total points
    *[Case("table-tennis", f"soccer__{l.replace('.','_')}", "tt_total_points","over","Over",f"generic total {l}")
      for l in ["55_5","65_5","77_5","85_5"]],
    # Handicap negative
    *[Case("table-tennis", f"soccer_point_handicap_minus_{h.replace('.','_')}", "point_handicap","1","1",f"hcp -{h}")
      for h in ["0.5","1.5","2.5","3.5","4.5","5.5","6.5"]],
    # Handicap positive
    *[Case("table-tennis", f"soccer_point_handicap_{h.replace('.','_')}", "point_handicap","2","2",f"hcp +{h}")
      for h in ["0.5","1.5","2.5","3.5"]],
    # Generic minus
    *[Case("table-tennis", f"soccer__minus_{h.replace('.','_')}", "point_handicap","1","1",f"generic hcp -{h}")
      for h in ["0.5","1.5","2.5","3.5","4.5"]],
]

# ══════════════════════════════════════════════════════════════════════════════
# HANDBALL  (OdibetsHandballMapper)
# ══════════════════════════════════════════════════════════════════════════════
HANDBALL: list[Case] = [
    Case("handball","handball_1x2",   "1x2",        "1","1","1x2 home"),
    Case("handball","handball_1x2",   "1x2",        "X","X","1x2 draw"),
    Case("handball","handball_1x2",   "1x2",        "2","2","1x2 away"),
    # Full match O/U
    *[Case("handball", f"over_under_handball_goals_{l.replace('.','_')}", "total_goals","over","Over",f"goals O/U {l}")
      for l in ["44.5","46.5","48.5","50.5","52.5","54.5","55.5","57.5","60.5","62.5"]],
    *[Case("handball", f"over_under_handball_goals_{l.replace('.','_')}", "total_goals","under","Under",f"goals U{l}")
      for l in ["44.5","50.5","55.5","60.5"]],
    # First half O/U
    *[Case("handball", f"first_half_over_under_handball_goals_{l.replace('.','_')}", "total_goals","over","Over",f"1H goals O/U {l}")
      for l in ["22.5","24.5","26.5","28.5","30.5"]],
    *[Case("handball", f"first_half_over_under_handball_goals_{l.replace('.','_')}", "total_goals","under","Under",f"1H goals U{l}")
      for l in ["22.5","26.5","30.5"]],
]

# ══════════════════════════════════════════════════════════════════════════════
# BOXING / MMA  (OdibetsBoxingMapper)
# ══════════════════════════════════════════════════════════════════════════════
BOXING: list[Case] = [
    Case("boxing","boxing_1x2",                          "boxing_winner",     "1","1","winner 3way"),
    Case("boxing","boxing_moneyline",                    "boxing_winner",     "1","1","moneyline"),
    Case("boxing","boxing_",                             "fight_distance",    "yes","yes","fight distance"),
    Case("boxing","boxing__sr_winning_method_ko_decision","method_of_victory","1_by_ko","home_ko","method KO"),
    Case("boxing","boxing__sr_winning_method_ko_decision","method_of_victory","2_by_decision","away_decision","method decision"),
    Case("boxing","boxing__sr_winning_method_ko_decision","method_of_victory","X","draw","method draw"),
    # Winner + exact round
    *[Case("boxing", f"boxing__sr_winner_and_rounds_{r}", "round_betting","1_1","home_round_1",f"winner rounds {r}")
      for r in ["10","12"]],
    # Winner + round range
    *[Case("boxing", f"boxing__sr_winner_and_round_range_{r}", "round_group","1_decision","home_decision",f"round range {r}")
      for r in ["10","12"]],
]

# ══════════════════════════════════════════════════════════════════════════════
# ALL CASES
# ══════════════════════════════════════════════════════════════════════════════
ALL_CASES: list[Case] = (
    SOCCER + BASKETBALL + TENNIS + ICE_HOCKEY + VOLLEYBALL +
    CRICKET + RUGBY + BASEBALL + ESOCCER + TABLE_TENNIS + HANDBALL + BOXING
)


# ══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ══════════════════════════════════════════════════════════════════════════════
def run(verbose: bool = False, failures_only: bool = False) -> bool:
    try:
        from app.workers.mappers import resolve_od_market
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("   Run from your project root with venv activated.")
        return False

    passed = failed = 0
    by_sport: dict[str, tuple[int,int]] = {}

    for c in ALL_CASES:
        sport_p, sport_f = by_sport.get(c.sport, (0, 0))
        try:
            can_slug, outcomes = resolve_od_market(c.sport, c.market_slug, {c.raw_outcome: 1.85})
            ok_slug    = c.expect_slug in can_slug or can_slug.startswith(c.expect_slug.split("_")[0])
            ok_outcome = c.expect_outcome in outcomes

            if ok_slug and ok_outcome:
                passed += 1
                by_sport[c.sport] = (sport_p + 1, sport_f)
                if verbose and not failures_only:
                    print(f"  ✅ [{c.sport:14}] {c.market_slug}  →  {can_slug}  |  {c.expect_outcome!r} ∈ {list(outcomes.keys())[:3]}")
            else:
                failed += 1
                by_sport[c.sport] = (sport_p, sport_f + 1)
                print(f"  ❌ [{c.sport:14}] {c.market_slug}  ({c.desc})")
                if not ok_slug:
                    print(f"        slug:    got={can_slug!r}  want contains={c.expect_slug!r}")
                if not ok_outcome:
                    print(f"        outcome: got={list(outcomes.keys())}  want={c.expect_outcome!r}")
        except Exception as exc:
            import traceback
            failed += 1
            by_sport[c.sport] = (sport_p, sport_f + 1)
            print(f"  💥 [{c.sport:14}] {c.market_slug}: {exc}")
            if verbose:
                traceback.print_exc()

    print(f"\n{'─'*60}")
    print(f"  Sport breakdown:")
    for sport, (p, f) in sorted(by_sport.items()):
        bar = "✅" * min(p, 20) + ("❌" if f else "")
        print(f"    {sport:18} {p:3} passed  {f:2} failed")
    print(f"{'─'*60}")
    print(f"  TOTAL: {passed} passed / {failed} failed / {passed+failed} tests")
    if failed == 0:
        print("  🎉 All tests passed!")
    return failed == 0


if __name__ == "__main__":
    verbose       = "-v" in sys.argv
    failures_only = "-f" in sys.argv
    ok = run(verbose=verbose, failures_only=failures_only)
    sys.exit(0 if ok else 1)