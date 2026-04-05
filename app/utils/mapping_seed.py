"""
app/seeds/market_seeds.py
=========================
Comprehensive market definitions organised by sport.
Run with: python -m flask seed-markets   (or call seed_all_markets() directly)

Usage
─────
    from app.seeds.market_seeds import seed_all_markets
    seed_all_markets()          # idempotent — skips existing names

Or as a Flask CLI command (register in app/__init__.py):
    @app.cli.command("seed-markets")
    def seed_markets_cmd():
        from app.seeds.market_seeds import seed_all_markets
        seed_all_markets()
        print("Done.")
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# MARKET DATA
# Each entry: (name, slug, description)
# sport_key = None  → applies to ALL sports
# sport_key values must match Sport.name values in the DB.
# ---------------------------------------------------------------------------

MARKETS_BY_SPORT: dict[str | None, list[tuple[str, str, str]]] = {

    # ── Universal (all sports) ───────────────────────────────────────────────
    None: [
        ("Match Winner",         "match_winner",        "Outright winner of the match (2-way)"),
        ("Draw No Bet",          "draw_no_bet",         "Bet on either team; stake refunded on draw"),
        ("Double Chance",        "double_chance",       "Home/Draw, Home/Away, or Draw/Away"),
        ("Half Time Result",     "half_time_result",    "Result at half-time only"),
        ("To Qualify",           "to_qualify",          "Which team progresses in a cup/knockout tie"),
    ],

    # ── Football (Soccer) ────────────────────────────────────────────────────
    "Football": [
        # Core
        ("1X2",                         "1x2",                          "Home / Draw / Away — 3-way match result"),
        ("Asian Handicap",              "asian_handicap",               "Handicap spread market, half-goal lines"),
        ("European Handicap",           "european_handicap",            "3-way handicap with draw option"),
        ("Over/Under Goals",            "over_under_goals",             "Total goals over or under a line (e.g. 2.5)"),
        ("Both Teams To Score",         "btts",                         "Both teams score at least one goal"),
        ("Both Teams To Score & Result","btts_and_result",              "BTTS combined with match result"),
        ("Correct Score",               "correct_score",                "Exact final score"),
        ("Half Time / Full Time",       "ht_ft",                        "Correct result at HT and FT"),
        ("Anytime Goalscorer",          "anytime_goalscorer",           "Player scores at any point in the match"),
        ("First Goalscorer",            "first_goalscorer",             "Player to score the first goal"),
        ("Last Goalscorer",             "last_goalscorer",              "Player to score the last goal"),
        ("Player To Score 2+",          "player_score_2plus",           "Player scores a brace or more"),
        ("Player To Score Hat-Trick",   "player_hattrick",              "Player scores 3+ goals"),
        ("Total Goals - Home Team",     "total_goals_home",             "Goals scored by home team over/under"),
        ("Total Goals - Away Team",     "total_goals_away",             "Goals scored by away team over/under"),
        ("First Half Over/Under",       "first_half_over_under",        "Total goals in first half over/under"),
        ("Second Half Over/Under",      "second_half_over_under",       "Total goals in second half over/under"),
        ("First Half 1X2",              "first_half_1x2",               "Result at half-time (3-way)"),
        ("Second Half Result",          "second_half_result",           "Result in the second half only"),
        ("Total Corners",               "total_corners",                "Total corners over/under a line"),
        ("Asian Corners",               "asian_corners",                "Corner handicap spread"),
        ("First Corner",                "first_corner",                 "Which team wins the first corner"),
        ("Last Corner",                 "last_corner",                  "Which team wins the last corner"),
        ("Total Corners - Home",        "total_corners_home",           "Corners taken by home team"),
        ("Total Corners - Away",        "total_corners_away",           "Corners taken by away team"),
        ("Total Bookings",              "total_bookings",               "Total yellow/red cards over/under"),
        ("Player To Be Booked",         "player_booked",                "Named player receives a card"),
        ("First Booking",               "first_booking",                "Which team's player is booked first"),
        ("Total Fouls",                 "total_fouls",                  "Total fouls committed over/under"),
        ("Offsides Over/Under",         "offsides_over_under",          "Total offsides in the match"),
        ("Clean Sheet - Home",          "clean_sheet_home",             "Home team keeps a clean sheet"),
        ("Clean Sheet - Away",          "clean_sheet_away",             "Away team keeps a clean sheet"),
        ("Win To Nil - Home",           "win_to_nil_home",              "Home team wins without conceding"),
        ("Win To Nil - Away",           "win_to_nil_away",              "Away team wins without conceding"),
        ("Score In Both Halves",        "score_both_halves",            "A team scores in both halves"),
        ("To Score In Both Halves",     "team_score_both_halves",       "Named team scores in both halves"),
        ("Match Result & Over/Under",   "result_and_over_under",        "Combined: result + total goals line"),
        ("First Team To Score",         "first_team_to_score",          "Which team opens the scoring"),
        ("Last Team To Score",          "last_team_to_score",           "Which team scores last"),
        ("Goal In Both Halves",         "goal_both_halves",             "At least one goal in each half"),
        ("Exact Goals",                 "exact_goals",                  "Exact number of goals in the match"),
        ("Highest Scoring Half",        "highest_scoring_half",         "Which half has more goals"),
        ("Number Of Goals",             "number_of_goals",              "Exact goal band e.g. 0-1, 2-3, 4+"),
        ("Scorecast",                   "scorecast",                    "First goalscorer + correct score combo"),
        ("Wincast",                     "wincast",                      "Anytime goalscorer + team to win combo"),
        ("Penalty Awarded",             "penalty_awarded",              "Will a penalty be awarded in the match"),
        ("Penalty Scored",              "penalty_scored",               "Will a penalty be scored"),
        ("Own Goal",                    "own_goal",                     "Will an own goal be scored"),
        ("Next Goal",                   "next_goal",                    "Which team scores the next goal"),
        ("Next Corner",                 "next_corner",                  "Which team wins the next corner"),
        ("Draw In Both Halves",         "draw_both_halves",             "Both halves end as draws"),
        ("Player Assists",              "player_assists",               "Named player to register an assist"),
        ("Player Shots On Target",      "player_shots_on_target",       "Player shots on target over/under"),
        ("Player Cards",                "player_cards",                 "Player total cards over/under"),
    ],

    # ── Basketball ───────────────────────────────────────────────────────────
    "Basketball": [
        ("Moneyline",                   "basketball_moneyline",         "Outright winner (no draw) — 2-way"),
        ("Point Spread",                "point_spread",                 "Handicap spread — points"),
        ("Total Points",                "total_points",                 "Total points over/under a line"),
        ("1st Quarter Winner",          "q1_winner",                    "Winner of the first quarter"),
        ("1st Half Winner",             "basketball_1h_winner",         "Winner at half-time"),
        ("Race To Points",              "race_to_points",               "First team to reach a points total"),
        ("Total Rebounds",              "total_rebounds",               "Combined team rebounds over/under"),
        ("Total Assists",               "total_assists_basketball",     "Combined team assists over/under"),
        ("Player Points",               "player_points",                "Named player points over/under"),
        ("Player Rebounds",             "player_rebounds",              "Named player rebounds over/under"),
        ("Player Assists (Basketball)", "player_assists_basketball",    "Named player assists over/under"),
        ("Player 3-Pointers Made",      "player_3pointers",             "Named player 3-pointers over/under"),
        ("Highest Scoring Quarter",     "highest_scoring_quarter",      "Which quarter has most points"),
        ("Halftime / Fulltime",         "basketball_ht_ft",             "Leader at HT and final result"),
        ("Will There Be Overtime",      "overtime",                     "Does the match go to overtime"),
        ("Margin Of Victory",           "margin_of_victory",            "Winning margin band"),
        ("Team Total Points",           "team_total_points",            "Named team's total points over/under"),
        ("Quarter Total Points",        "quarter_total_points",         "Total points in a specific quarter"),
        ("Winning Margin",              "winning_margin_basketball",    "Exact margin band for the winner"),
    ],

    # ── Tennis ───────────────────────────────────────────────────────────────
    "Tennis": [
        ("Match Winner",                "tennis_match_winner",          "Outright winner of the match"),
        ("Set Betting",                 "set_betting",                  "Correct set score e.g. 2-0, 2-1"),
        ("Total Sets",                  "total_sets",                   "Total sets played over/under"),
        ("Total Games",                 "total_games",                  "Total games over/under"),
        ("Games Handicap",              "games_handicap",               "Handicap on games"),
        ("Set Winner",                  "set_winner",                   "Winner of a specific set"),
        ("Set Total Games",             "set_total_games",              "Games in a specific set over/under"),
        ("First Set Winner",            "first_set_winner",             "Who wins the first set"),
        ("Player To Win A Set",         "player_win_a_set",             "Named player wins at least one set"),
        ("Correct Score (Sets)",        "correct_score_sets",           "Exact set score of the match"),
        ("Tiebreak In Match",           "tiebreak_in_match",            "Will any tiebreak be played"),
        ("Double Fault",                "double_fault",                 "Named player hits a double fault"),
        ("Ace Count",                   "ace_count",                    "Total aces over/under"),
        ("Break Of Serve",              "break_of_serve",               "Will there be a break in a set"),
        ("Game Handicap",               "game_handicap_tennis",         "Point spread on total games"),
        ("Next Game Winner",            "next_game_winner",             "Who wins the current/next game"),
        ("Will Match Go To Final Set",  "final_set",                    "Does the match reach the final set"),
    ],

    # ── Cricket ──────────────────────────────────────────────────────────────
    "Cricket": [
        ("Match Winner",                "cricket_match_winner",         "Outright winner including tie/no result"),
        ("Series Winner",               "series_winner",                "Who wins the series"),
        ("Top Batsman",                 "top_batsman",                  "Highest run scorer in the match/innings"),
        ("Top Bowler",                  "top_bowler",                   "Most wickets taken"),
        ("Man Of The Match",            "man_of_match_cricket",         "Player of the match award"),
        ("Total Runs - Match",          "total_runs_match",             "Total runs in the full match over/under"),
        ("Total Runs - Innings",        "total_runs_innings",           "Total runs in a specific innings"),
        ("Total Runs - Over",           "total_runs_over",              "Runs in a specific over over/under"),
        ("Player Runs",                 "player_runs",                  "Named batsman runs over/under"),
        ("Player Wickets",              "player_wickets",               "Named bowler wickets over/under"),
        ("Method Of Dismissal",         "method_of_dismissal",          "How next wicket falls"),
        ("Fall Of Next Wicket",         "fall_next_wicket",             "Score at which next wicket falls"),
        ("Highest Opening Partnership", "highest_opening_partnership",  "Opening partnership runs over/under"),
        ("Next Dismissal",              "next_dismissal",               "Type of next dismissal"),
        ("Toss Winner",                 "toss_winner",                  "Who wins the coin toss"),
        ("1st Innings Lead",            "first_innings_lead",           "Who leads after first innings"),
        ("Innings Runs - 6 Overs",      "powerplay_runs",               "Runs in powerplay overs"),
        ("Fifty Scored",                "fifty_scored",                 "Will a batsman score 50+"),
        ("Century Scored",              "century_scored",               "Will a batsman score 100+"),
    ],

    # ── American Football ────────────────────────────────────────────────────
    "American Football": [
        ("Moneyline",                   "nfl_moneyline",                "Outright winner (2-way)"),
        ("Point Spread",                "nfl_point_spread",             "Handicap spread — points"),
        ("Total Points",                "nfl_total_points",             "Total points over/under"),
        ("1st Half Spread",             "nfl_1h_spread",                "First-half point spread"),
        ("1st Half Total",              "nfl_1h_total",                 "First-half total points"),
        ("Quarter Spread",              "nfl_quarter_spread",           "Point spread for a specific quarter"),
        ("Quarter Total",               "nfl_quarter_total",            "Total for a specific quarter"),
        ("Player Passing Yards",        "player_passing_yards",         "QB passing yards over/under"),
        ("Player Rushing Yards",        "player_rushing_yards",         "RB rushing yards over/under"),
        ("Player Receiving Yards",      "player_receiving_yards",       "WR receiving yards over/under"),
        ("Player Touchdowns",           "player_touchdowns",            "Player total TDs over/under"),
        ("Anytime Touchdown Scorer",    "nfl_anytime_td",               "Player scores a touchdown"),
        ("First Touchdown Scorer",      "nfl_first_td",                 "First player to score a TD"),
        ("To Win The Toss",             "nfl_toss",                     "Coin toss winner"),
        ("Will There Be Overtime",      "nfl_overtime",                 "Does the game go to OT"),
        ("Exact Score",                 "nfl_exact_score",              "Correct final score"),
        ("Winning Margin",              "nfl_winning_margin",           "Margin band for the winner"),
        ("Half Time / Full Time",       "nfl_ht_ft",                    "Leader at HT and final result"),
        ("Total Field Goals",           "total_field_goals",            "Total field goals over/under"),
        ("Race To Points",              "nfl_race_to_points",           "First team to reach a points total"),
    ],

    # ── Baseball ─────────────────────────────────────────────────────────────
    "Baseball": [
        ("Moneyline",                   "baseball_moneyline",           "Outright winner (2-way)"),
        ("Run Line",                    "run_line",                     "Handicap spread — runs (usually ±1.5)"),
        ("Total Runs",                  "total_runs",                   "Total runs over/under"),
        ("1st 5 Innings Winner",        "f5_winner",                    "Leader after 5 innings"),
        ("1st 5 Innings Total",         "f5_total",                     "Total runs in first 5 innings"),
        ("1st Inning Score",            "first_inning_score",           "Will a run be scored in the 1st inning"),
        ("Player Home Runs",            "player_home_runs",             "Named player HRs over/under"),
        ("Player Hits",                 "player_hits",                  "Named player hits over/under"),
        ("Player Strikeouts",           "player_strikeouts",            "Pitcher strikeouts over/under"),
        ("Player Total Bases",          "player_total_bases",           "Named player total bases over/under"),
        ("Team Total Runs",             "team_total_runs",              "Named team runs over/under"),
        ("Winning Margin",              "baseball_winning_margin",      "Exact run margin band"),
        ("Extra Innings",               "extra_innings",                "Does the game go to extra innings"),
        ("Series Winner",               "baseball_series_winner",       "Who wins the series"),
    ],

    # ── Hockey (Ice) ─────────────────────────────────────────────────────────
    "Ice Hockey": [
        ("Moneyline",                   "hockey_moneyline",             "2-way winner (regulation + OT + SO)"),
        ("3-Way Result",                "hockey_3way",                  "Home / Draw / Away in regulation"),
        ("Puck Line",                   "puck_line",                    "Handicap spread — goals (usually ±1.5)"),
        ("Total Goals",                 "hockey_total_goals",           "Total goals over/under"),
        ("1st Period Winner",           "hockey_p1_winner",             "Winner of 1st period"),
        ("1st Period Total",            "hockey_p1_total",              "Goals in 1st period over/under"),
        ("Player To Score",             "hockey_player_score",          "Named player scores a goal"),
        ("Player Points",               "hockey_player_points",         "Named player goals+assists over/under"),
        ("Player Shots On Goal",        "hockey_shots_on_goal",         "Named player shots on goal over/under"),
        ("Total Shots On Goal",         "hockey_total_shots",           "Combined shots on goal over/under"),
        ("Will There Be Overtime",      "hockey_overtime",              "Does the game go to OT/SO"),
        ("Game Decided In Overtime",    "hockey_ot_decision",           "Winner decided in OT"),
        ("Penalty Minutes",             "penalty_minutes",              "Total penalty minutes over/under"),
        ("Total Power Plays",           "total_power_plays",            "Combined power plays over/under"),
        ("Clean Sheet",                 "hockey_clean_sheet",           "A team concedes no goals"),
        ("Correct Score",               "hockey_correct_score",         "Exact final score"),
        ("Winning Margin",              "hockey_winning_margin",        "Margin band for the winner"),
        ("Period Handicap",             "period_handicap",              "Handicap for a specific period"),
    ],

    # ── Rugby (Union & League) ────────────────────────────────────────────────
    "Rugby Union": [
        ("Match Result",                "rugby_result",                 "Home / Draw / Away"),
        ("Asian Handicap",              "rugby_handicap",               "Points spread"),
        ("Total Points",                "rugby_total_points",           "Total points over/under"),
        ("First Try Scorer",            "first_try_scorer",             "Player to score the first try"),
        ("Anytime Try Scorer",          "anytime_try_scorer",           "Player to score at any point"),
        ("Half Time Result",            "rugby_ht_result",              "Result at half-time"),
        ("Half Time / Full Time",       "rugby_ht_ft",                  "HT and FT result combo"),
        ("Total Tries",                 "total_tries",                  "Total tries over/under"),
        ("Winning Margin",              "rugby_winning_margin",         "Winning margin band"),
        ("Correct Score",               "rugby_correct_score",          "Exact final score"),
        ("First Scoring Play",          "first_scoring_play",           "Try, penalty, drop goal"),
        ("Team To Score First",         "rugby_first_team_score",       "Which team opens the scoring"),
        ("Draw No Bet",                 "rugby_draw_no_bet",            "2-way — stake back on draw"),
        ("Player To Be First Try",      "player_first_try",             "Named player gets the first try"),
        ("Total Conversions",           "total_conversions",            "Total conversions over/under"),
        ("Total Penalties",             "total_penalties_rugby",        "Penalty kicks over/under"),
    ],

    "Rugby League": [
        ("Match Result",                "rleague_result",               "Home / Draw / Away"),
        ("Asian Handicap",              "rleague_handicap",             "Points spread"),
        ("Total Points",                "rleague_total_points",         "Total points over/under"),
        ("First Try Scorer",            "rleague_first_try",            "Player to score the first try"),
        ("Anytime Try Scorer",          "rleague_anytime_try",          "Player to score at any point"),
        ("Total Tries",                 "rleague_total_tries",          "Total tries over/under"),
        ("Winning Margin",              "rleague_winning_margin",       "Winning margin band"),
        ("Half Time Result",            "rleague_ht_result",            "Result at half-time"),
    ],

    # ── Boxing & MMA ─────────────────────────────────────────────────────────
    "Boxing": [
        ("Fight Winner",                "boxing_winner",                "Outright fight winner"),
        ("Method Of Victory",           "method_of_victory",            "KO/TKO, Decision, DQ, Draw"),
        ("Round Betting",               "round_betting",                "Exact round + winner"),
        ("Fight To Go The Distance",    "fight_distance",               "Does the fight go all rounds"),
        ("Over/Under Rounds",           "over_under_rounds",            "Total rounds over/under"),
        ("Round Group Betting",         "round_group",                  "Fighter wins in rounds 1-3, 4-6, etc"),
        ("Knockdown Scored",            "knockdown_scored",             "Will a knockdown be scored"),
        ("Draw Or Technical Draw",      "boxing_draw",                  "Fight ends in a draw"),
        ("Winner & Method",             "winner_and_method",            "Combined winner + method"),
    ],

    "MMA": [
        ("Fight Winner",                "mma_winner",                   "Outright fight winner"),
        ("Method Of Victory",           "mma_method",                   "KO/TKO, Submission, Decision, DQ"),
        ("Round Betting",               "mma_round_betting",            "Exact round + winner"),
        ("Fight To Go The Distance",    "mma_distance",                 "Does the fight go all rounds"),
        ("Over/Under Rounds",           "mma_over_under_rounds",        "Total rounds over/under"),
        ("Round Group Betting",         "mma_round_group",              "Fighter wins in rounds 1-2, 3+"),
        ("Winning Method & Round",      "mma_method_round",             "Combined method + round"),
        ("Performance Bonus",           "mma_bonus",                    "Which fighter earns a bonus"),
    ],

    # ── Volleyball ───────────────────────────────────────────────────────────
    "Volleyball": [
        ("Match Winner",                "volleyball_winner",            "Outright match winner"),
        ("Set Betting",                 "volleyball_set_betting",       "Correct set score e.g. 3-0, 3-2"),
        ("Total Sets",                  "volleyball_total_sets",        "Total sets played over/under"),
        ("Set Winner",                  "volleyball_set_winner",        "Winner of a specific set"),
        ("Set Total Points",            "volleyball_set_points",        "Points in a set over/under"),
        ("Total Points",                "volleyball_total_points",      "Total points in the match"),
        ("Handicap Sets",               "volleyball_handicap",          "Set handicap"),
        ("Will There Be A 5th Set",     "volleyball_5th_set",           "Does the match go to a deciding set"),
        ("Team To Win A Set",           "volleyball_team_win_set",      "Named team wins at least one set"),
    ],

    # ── Darts ────────────────────────────────────────────────────────────────
    "Darts": [
        ("Match Winner",                "darts_winner",                 "Outright match winner"),
        ("Correct Score (Sets/Legs)",   "darts_correct_score",          "Exact sets/legs score"),
        ("Highest Checkout",            "highest_checkout",             "Which player hits the highest checkout"),
        ("Total 180s",                  "total_180s",                   "Total 180s scored over/under"),
        ("Player 180s",                 "player_180s",                  "Named player 180s over/under"),
        ("First To Score 180",          "first_180",                    "First player to hit a 180"),
        ("Leg Winner",                  "leg_winner",                   "Winner of a specific leg"),
        ("Set Handicap",                "darts_set_handicap",           "Set handicap"),
        ("Leg Handicap",                "darts_leg_handicap",           "Leg handicap"),
        ("Will Match Go To Final Set",  "darts_final_set",              "Does the match reach the deciding set"),
    ],

    # ── Snooker ──────────────────────────────────────────────────────────────
    "Snooker": [
        ("Match Winner",                "snooker_winner",               "Outright match winner"),
        ("Correct Score",               "snooker_correct_score",        "Exact frame score"),
        ("Frame Handicap",              "frame_handicap",               "Frame spread"),
        ("Total Frames",                "total_frames",                 "Total frames over/under"),
        ("Frame Winner",                "frame_winner",                 "Winner of a specific frame"),
        ("Century Scored",              "snooker_century",              "Will a century break be made"),
        ("Highest Break",               "highest_break",                "Which player makes the highest break"),
        ("Player To Make First Century","first_century",                "First player to make a 100+ break"),
        ("147 Made",                    "maximum_break",                "Will a maximum 147 be made"),
        ("Will Match Go To Final Frame","snooker_final_frame",          "Does match reach the deciding frame"),
    ],

    # ── Table Tennis ─────────────────────────────────────────────────────────
    "Table Tennis": [
        ("Match Winner",                "tt_winner",                    "Outright match winner"),
        ("Set Betting",                 "tt_set_betting",               "Correct set score"),
        ("Total Sets",                  "tt_total_sets",                "Total sets over/under"),
        ("Set Handicap",                "tt_set_handicap",              "Set spread"),
        ("Total Points",                "tt_total_points",              "Total match points over/under"),
        ("Set Winner",                  "tt_set_winner",                "Winner of a specific set"),
        ("Will There Be A Deciding Set","tt_final_set",                 "Does the match go to a deciding set"),
    ],

    # ── Golf ─────────────────────────────────────────────────────────────────
    "Golf": [
        ("Tournament Winner",           "golf_tournament_winner",       "Outright tournament winner"),
        ("Each Way Winner",             "golf_each_way",                "Winner + placed (top positions)"),
        ("Top 5 / Top 10 Finish",       "golf_top_finish",              "Player finishes in top positions"),
        ("Matchup Betting",             "golf_matchup",                 "Head-to-head player vs player"),
        ("Round Leader",                "golf_round_leader",            "Who leads after a specific round"),
        ("Round Score",                 "golf_round_score",             "Player's round score over/under"),
        ("Hole In One",                 "hole_in_one",                  "Will a hole in one be made"),
        ("Make / Miss The Cut",         "golf_cut",                     "Named player makes the cut"),
        ("Winning Score",               "golf_winning_score",           "Final winning score over/under"),
        ("Nationality Of Winner",       "golf_nationality",             "Nationality of tournament winner"),
        ("First Round Leader",          "golf_frl",                     "Leader after round 1"),
        ("Group Betting",               "golf_group_betting",           "Best score within a group of players"),
    ],

    # ── Motorsport ───────────────────────────────────────────────────────────
    "Motorsport": [
        ("Race Winner",                 "race_winner",                  "Outright race winner"),
        ("Podium Finish",               "podium_finish",                "Driver finishes in top 3"),
        ("Points Finish",               "points_finish",                "Driver finishes in points"),
        ("Qualifying Winner",           "qualifying_winner",            "Pole position"),
        ("Fastest Lap",                 "fastest_lap",                  "Driver sets the fastest lap"),
        ("Head To Head",                "motorsport_h2h",               "Driver vs driver matchup"),
        ("Winning Constructor",         "winning_constructor",          "Constructor/team wins the race"),
        ("Safety Car Deployed",         "safety_car",                   "Will a safety car appear"),
        ("Retirement",                  "retirement",                   "Named driver fails to finish"),
        ("Championship Winner",         "championship_winner",          "Season championship outright"),
        ("Points Scored",               "driver_points_scored",         "Named driver points over/under"),
    ],

    # ── Horse Racing ─────────────────────────────────────────────────────────
    "Horse Racing": [
        ("Win",                         "horse_win",                    "Horse wins the race"),
        ("Each Way",                    "horse_each_way",               "Win or place"),
        ("Place",                       "horse_place",                  "Horse places (e.g. top 3)"),
        ("Forecast",                    "horse_forecast",               "Correct 1st and 2nd"),
        ("Reverse Forecast",            "horse_reverse_forecast",       "1st and 2nd in either order"),
        ("Combination Forecast",        "horse_combo_forecast",         "Multiple horses, correct 1st/2nd"),
        ("Tricast",                     "horse_tricast",                "Correct 1st, 2nd, 3rd"),
        ("Winning Distance",            "horse_winning_distance",       "Margin of victory"),
        ("Winning Favourite",           "horse_favourite_wins",         "Favourite wins the race"),
        ("Jockey To Win",               "jockey_win",                   "Named jockey wins the race"),
        ("Trainer To Win",              "trainer_win",                  "Named trainer has the winner"),
        ("Number Of Runners",           "num_runners",                  "How many horses complete the race"),
    ],

    # ── Esports ──────────────────────────────────────────────────────────────
    "Esports": [
        ("Match Winner",                "esports_winner",               "Outright match winner"),
        ("Map Winner",                  "map_winner",                   "Winner of a specific map/game"),
        ("Map Handicap",                "map_handicap",                 "Map spread"),
        ("Total Maps",                  "total_maps",                   "Total maps played over/under"),
        ("First Blood",                 "first_blood",                  "Team/player gets first kill"),
        ("First Tower",                 "first_tower",                  "First tower destroyed (MOBA)"),
        ("First Dragon",                "first_dragon",                 "First dragon secured (LoL)"),
        ("First Baron",                 "first_baron",                  "First baron secured (LoL)"),
        ("Total Kills",                 "total_kills",                  "Total kills over/under"),
        ("Round Handicap",              "round_handicap",               "Round spread"),
        ("Total Rounds",                "total_rounds",                 "Total rounds over/under"),
        ("Pistol Round Winner",         "pistol_round",                 "Winner of pistol round (CS)"),
        ("Knife Round Winner",          "knife_round",                  "Knife round winner (CS)"),
        ("Ace Scored",                  "ace_scored",                   "Will an ace be scored (CS)"),
    ],
}


# ── Primary result market per sport (used for match-list 3-way check) ────────
# Maps sport_key → canonical primary market slug(s) this bookmaker must supply.
# Football is the only sport with a true 3-way result; others accept their
# primary result market (Moneyline, Match Winner, etc.)
PRIMARY_RESULT_MARKET_SLUGS: dict[str | None, list[str]] = {
    None:               ["match_winner"],           # universal fallback
    "Football":         ["1x2"],                    # the ONLY true 3-way
    "Basketball":       ["basketball_moneyline"],
    "Tennis":           ["tennis_match_winner"],
    "Cricket":          ["cricket_match_winner"],
    "American Football":["nfl_moneyline"],
    "Baseball":         ["baseball_moneyline"],
    "Ice Hockey":       ["hockey_3way", "hockey_moneyline"],
    "Rugby Union":      ["rugby_result"],
    "Rugby League":     ["rleague_result"],
    "Boxing":           ["boxing_winner"],
    "MMA":              ["mma_winner"],
    "Volleyball":       ["volleyball_winner"],
    "Darts":            ["darts_winner"],
    "Snooker":          ["snooker_winner"],
    "Table Tennis":     ["tt_winner"],
    "Golf":             ["golf_tournament_winner"],
    "Motorsport":       ["race_winner"],
    "Horse Racing":     ["horse_win"],
    "Esports":          ["esports_winner"],
}


# ---------------------------------------------------------------------------
# HELPER — used by API without a DB
# ---------------------------------------------------------------------------

def get_markets_for_sport(sport_name: str | None = None) -> list[dict]:
    """
    Return all market defs for a given sport (including universal None markets).
    Does not require DB context — works directly from the dict above.
    Used by the /research/markets endpoint.
    """
    results: list[dict] = []
    for sport_key, entries in MARKETS_BY_SPORT.items():
        if sport_name is None:
            # Return everything
            pass
        elif sport_key is not None and sport_key.lower() != sport_name.lower():
            continue

        is_primary = False
        primary_slugs: list[str] = (
            PRIMARY_RESULT_MARKET_SLUGS.get(sport_key)
            or PRIMARY_RESULT_MARKET_SLUGS.get(None)
            or []
        )

        for name, slug, description in entries:
            results.append({
                "name":        name,
                "slug":        slug,
                "description": description,
                "sport":       sport_key,
                "is_primary":  slug in primary_slugs,
            })

    return results


def get_primary_slugs_for_sport(sport_name: str | None) -> list[str]:
    """Return the expected primary/result market slug(s) for the sport."""
    if sport_name is None:
        return PRIMARY_RESULT_MARKET_SLUGS.get(None, ["match_winner"])
    # Case-insensitive match
    for k, v in PRIMARY_RESULT_MARKET_SLUGS.items():
        if k and k.lower() == sport_name.lower():
            return v
    return PRIMARY_RESULT_MARKET_SLUGS.get(None, ["match_winner"])


# ---------------------------------------------------------------------------
# SEED FUNCTION
# ---------------------------------------------------------------------------

def seed_all_markets(sport_filter: list[str] | None = None) -> dict[str, int]:
    """
    Idempotent seed — only creates markets that don't yet exist by name.

    Args:
        sport_filter: if provided, only seeds markets for those sport names
                      (plus universal None markets).
    Returns:
        {"created": N, "skipped": N}
    """
    from app.extensions import db
    from app.models.mapping_models import Market
    from app.models.competions_model import Sport

    sport_map: dict[str, int] = {s.name: s.id for s in Sport.query.all()}

    created = 0
    skipped = 0

    for sport_key, entries in MARKETS_BY_SPORT.items():
        if sport_filter and sport_key is not None and sport_key not in sport_filter:
            continue

        sport_id = sport_map.get(sport_key) if sport_key else None

        for (name, slug, description) in entries:
            if Market.query.filter_by(name=name).first():
                skipped += 1
                continue

            base_slug = slug
            n = 1
            while Market.query.filter_by(slug=slug).first():
                slug = f"{base_slug}_{n}"
                n += 1

            m = Market(
                name=name,
                slug=slug,
                description=description,
                sport_id=sport_id,
                is_active=True,
            )
            db.session.add(m)
            created += 1

    db.session.commit()
    return {"created": created, "skipped": skipped}

# ---------------------------------------------------------------------------
# BOOKMAKER SEED DATA
# ---------------------------------------------------------------------------

BOOKMAKERS_DATA: list[dict] = [
    # ── Local Kenyan bookmakers (direct harvesters) ───────────────────────────
    {
        "name":               "SportPesa",
        "slug":               "sp",
        "domain":             "ke.sportpesa.com",
        "brand_color":        "#008000",
        "currency":           "KES",
        "min_bet_amount":     49,
        "max_bet_amount":     None,
        "tax_percent":        7.5,
        "mpesa_paybill":      "880100",
        "mpesa_paybill_label":"SportPesa Deposit",
        "airtel_paybill":     "880100",
        "airtel_paybill_label":"SportPesa Airtel",
        "vendor_slug":        "sp",
        "betb2b_partner":     None,
        "betb2b_gr":          None,
        "betb2b_endpoint":    None,
        "is_active":          True,
    },
    {
        "name":               "Betika",
        "slug":               "bt",
        "domain":             "betika.com",
        "brand_color":        "#E31E2D",
        "currency":           "KES",
        "min_bet_amount":     1,
        "max_bet_amount":     None,
        "tax_percent":        7.5,
        "mpesa_paybill":      "29071922",
        "mpesa_paybill_label":"Betika Deposit",
        "airtel_paybill":     "29071922",
        "airtel_paybill_label":"Betika Airtel",
        "vendor_slug":        "bt",
        "betb2b_partner":     None,
        "betb2b_gr":          None,
        "betb2b_endpoint":    None,
        "is_active":          True,
    },
    {
        "name":               "OdiBets",
        "slug":               "od",
        "domain":             "odibets.com",
        "brand_color":        "#FF6600",
        "currency":           "KES",
        "min_bet_amount":     1,
        "max_bet_amount":     None,
        "tax_percent":        7.5,
        "mpesa_paybill":      "558844",
        "mpesa_paybill_label":"OdiBets Deposit",
        "airtel_paybill":     "558844",
        "airtel_paybill_label":"OdiBets Airtel",
        "vendor_slug":        "od",
        "betb2b_partner":     None,
        "betb2b_gr":          None,
        "betb2b_endpoint":    None,
        "is_active":          True,
    },
    # ── B2B / International bookmakers ────────────────────────────────────────
    {
        "name":               "1xBet",
        "slug":               "1xbet",
        "domain":             "1xbet.co.ke",
        "brand_color":        "#1F8AEB",
        "currency":           "KES",
        "min_bet_amount":     10,
        "max_bet_amount":     None,
        "tax_percent":        20,
        "mpesa_paybill":      "290435",
        "mpesa_paybill_label":"1xBet Deposit",
        "airtel_paybill":     "290435",
        "airtel_paybill_label":"1xBet Airtel",
        "mpesa_sms":          "20019",
        "airtel_sms":         None,
        "vendor_slug":        "betb2b",
        "betb2b_partner":     61,
        "betb2b_gr":          656,
        "betb2b_endpoint":    "LiveFeed",
        "is_active":          True,
    },
    {
        "name":               "22Bet",
        "slug":               "22bet",
        "domain":             "22bet.co.ke",
        "brand_color":        "#0B2133",
        "currency":           "KES",
        "min_bet_amount":     10,
        "max_bet_amount":     None,
        "tax_percent":        20,
        "mpesa_paybill":      "290557",
        "mpesa_paybill_label":"22Bet Deposit",
        "airtel_paybill":     "290557",
        "airtel_paybill_label":"22Bet Airtel",
        "mpesa_sms":          "22228",
        "airtel_sms":         None,
        "vendor_slug":        "betb2b",
        "betb2b_partner":     2,
        "betb2b_gr":          656,
        "betb2b_endpoint":    "LiveFeed",
        "is_active":          True,
    },
    {
        "name":               "Betwinner",
        "slug":               "betwinner",
        "domain":             "betwinner.co.ke",
        "brand_color":        "#FF6600",
        "currency":           "KES",
        "min_bet_amount":     10,
        "max_bet_amount":     None,
        "tax_percent":        20,
        "mpesa_paybill":      None,
        "mpesa_paybill_label":None,
        "airtel_paybill":     None,
        "airtel_paybill_label":None,
        "mpesa_sms":          None,
        "airtel_sms":         None,
        "vendor_slug":        "betb2b",
        "betb2b_partner":     3,
        "betb2b_gr":          656,
        "betb2b_endpoint":    "LiveFeed",
        "is_active":          True,
    },
    {
        "name":               "Melbet",
        "slug":               "melbet",
        "domain":             "melbet.co.ke",
        "brand_color":        "#FF0000",
        "currency":           "KES",
        "min_bet_amount":     10,
        "max_bet_amount":     None,
        "tax_percent":        20,
        "mpesa_paybill":      None,
        "mpesa_paybill_label":None,
        "airtel_paybill":     None,
        "airtel_paybill_label":None,
        "mpesa_sms":          None,
        "airtel_sms":         None,
        "vendor_slug":        "betb2b",
        "betb2b_partner":     4,
        "betb2b_gr":          656,
        "betb2b_endpoint":    "LiveFeed",
        "is_active":          True,
    },
    {
        "name":               "Megapari",
        "slug":               "megapari",
        "domain":             "megapari.com",
        "brand_color":        "#7B2FBE",
        "currency":           "KES",
        "min_bet_amount":     10,
        "max_bet_amount":     None,
        "tax_percent":        20,
        "mpesa_paybill":      None,
        "mpesa_paybill_label":None,
        "airtel_paybill":     None,
        "airtel_paybill_label":None,
        "mpesa_sms":          None,
        "airtel_sms":         None,
        "vendor_slug":        "betb2b",
        "betb2b_partner":     6,
        "betb2b_gr":          656,
        "betb2b_endpoint":    "LiveFeed",
        "is_active":          True,
    },
    {
        "name":               "Helabet",
        "slug":               "helabet",
        "domain":             "helabetke.com",
        "brand_color":        "#9C27B0",
        "currency":           "KES",
        "min_bet_amount":     10,
        "max_bet_amount":     None,
        "tax_percent":        20,
        "mpesa_paybill":      "290700",
        "mpesa_paybill_label":"Helabet Deposit",
        "airtel_paybill":     "290700",
        "airtel_paybill_label":"Helabet Airtel",
        "mpesa_sms":          "29070",
        "airtel_sms":         None,
        "vendor_slug":        "betb2b",
        "betb2b_partner":     237,
        "betb2b_gr":          None,
        "betb2b_endpoint":    "LineFeed",
        "is_active":          True,
    },
    {
        "name":               "Paripesa",
        "slug":               "paripesa",
        "domain":             "paripesa.cool",
        "brand_color":        "#FF6B35",
        "currency":           "KES",
        "min_bet_amount":     10,
        "max_bet_amount":     None,
        "tax_percent":        20,
        "mpesa_paybill":      None,
        "mpesa_paybill_label":None,
        "airtel_paybill":     None,
        "airtel_paybill_label":None,
        "mpesa_sms":          None,
        "airtel_sms":         None,
        "vendor_slug":        "betb2b",
        "betb2b_partner":     188,
        "betb2b_gr":          764,
        "betb2b_endpoint":    "LiveFeed",
        "is_active":          True,
    },
]


def seed_bookmakers() -> dict[str, int]:
    """
    Idempotent bookmaker seed.
    Matches on `name` — updates existing rows, creates missing ones.
    Returns {"created": N, "updated": N, "skipped": N}.
    """
    from app.extensions import db
    from app.models.bookmakers_model import Bookmaker

    created = updated = skipped = 0

    for data in BOOKMAKERS_DATA:
        bm = Bookmaker.query.filter(
            db.func.lower(Bookmaker.name) == data["name"].lower()
        ).first()

        if bm:
            # Update any fields that may have changed
            changed = False
            for field in (
                "slug", "domain", "brand_color", "currency",
                "min_bet_amount", "max_bet_amount", "tax_percent",
                "mpesa_paybill", "mpesa_paybill_label",
                "airtel_paybill", "airtel_paybill_label",
                "vendor_slug", "betb2b_partner", "betb2b_gr",
                "betb2b_endpoint", "is_active",
            ):
                val = data.get(field)
                if val is not None and getattr(bm, field, None) != val:
                    setattr(bm, field, val)
                    changed = True
            if changed:
                updated += 1
            else:
                skipped += 1
        else:
            bm = Bookmaker(**{
                k: v for k, v in data.items()
                if hasattr(Bookmaker, k)
            })
            db.session.add(bm)
            created += 1

    db.session.commit()
    print(f"[seed_bookmakers] created={created} updated={updated} skipped={skipped}")
    return {"created": created, "updated": updated, "skipped": skipped}


def seed_all(sport_filter: list[str] | None = None) -> dict:
    """Run all seeds in order: bookmakers → markets."""
    bk_stats  = seed_bookmakers()
    mkt_stats = seed_all_markets(sport_filter=sport_filter)
    return {"bookmakers": bk_stats, "markets": mkt_stats}