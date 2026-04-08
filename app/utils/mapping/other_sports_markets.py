VOLLEYBALL_MARKETS = [
    ("Match Winner",                "volleyball_winner",            "Outright match winner"),
    ("Set Betting",                 "volleyball_set_betting",       "Correct set score e.g. 3-0, 3-2"),
    ("Total Sets",                  "volleyball_total_sets",        "Total sets played over/under"),
    ("Set Winner",                  "volleyball_set_winner",        "Winner of a specific set"),
    ("Set Total Points",            "volleyball_set_points",        "Points in a set over/under"),
    ("Total Points",                "volleyball_total_points",      "Total points in the match"),
    ("Handicap Sets",               "volleyball_handicap",          "Set handicap"),
    ("Will There Be A 5th Set",     "volleyball_5th_set",           "Does the match go to a deciding set"),
    ("Team To Win A Set",           "volleyball_team_win_set",      "Named team wins at least one set"),
]

DARTS_MARKETS = [
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
]

SNOOKER_MARKETS = [
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
]

TABLE_TENNIS_MARKETS = [
    ("Match Winner",                "tt_winner",                    "Outright match winner"),
    ("Set Betting",                 "tt_set_betting",               "Correct set score"),
    ("Total Sets",                  "tt_total_sets",                "Total sets over/under"),
    ("Set Handicap",                "tt_set_handicap",              "Set spread"),
    ("Total Points",                "tt_total_points",              "Total match points over/under"),
    ("Set Winner",                  "tt_set_winner",                "Winner of a specific set"),
    ("Will There Be A Deciding Set","tt_final_set",                 "Does the match go to a deciding set"),
]

GOLF_MARKETS = [
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
]

MOTORSPORT_MARKETS = [
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
]

HORSE_RACING_MARKETS = [
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
]

ESPORTS_MARKETS = [
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
]