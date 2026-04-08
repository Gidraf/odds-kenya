FOOTBALL_MARKETS = [
    # Match Result
    ("1X2", "1x2", "Home / Draw / Away — 3-way match result"),
    
    # Exhaustive Over/Under Goals
    ("Over 0.5 Goals",  "over_0_5_goals",  "1 or more goals scored in the match"),
    ("Under 0.5 Goals", "under_0_5_goals", "0 goals scored in the match"),
    ("Over 1.5 Goals",  "over_1_5_goals",  "2 or more goals scored in the match"),
    ("Under 1.5 Goals", "under_1_5_goals", "1 or fewer goals scored in the match"),
    ("Over 2.5 Goals",  "over_2_5_goals",  "3 or more goals scored in the match"),
    ("Under 2.5 Goals", "under_2_5_goals", "2 or fewer goals scored in the match"),
    ("Over 3.5 Goals",  "over_3_5_goals",  "4 or more goals scored in the match"),
    ("Under 3.5 Goals", "under_3_5_goals", "3 or fewer goals scored in the match"),
    ("Over 4.5 Goals",  "over_4_5_goals",  "5 or more goals scored in the match"),
    ("Under 4.5 Goals", "under_4_5_goals", "4 or fewer goals scored in the match"),
    ("Over 5.5 Goals",  "over_5_5_goals",  "6 or more goals scored in the match"),
    ("Under 5.5 Goals", "under_5_5_goals", "5 or fewer goals scored in the match"),

    # Exhaustive Exact Goals
    ("Exact Goals: 0", "exact_goals_0", "Exactly 0 goals scored"),
    ("Exact Goals: 1", "exact_goals_1", "Exactly 1 goal scored"),
    ("Exact Goals: 2", "exact_goals_2", "Exactly 2 goals scored"),
    ("Exact Goals: 3", "exact_goals_3", "Exactly 3 goals scored"),
    ("Exact Goals: 4", "exact_goals_4", "Exactly 4 goals scored"),
    ("Exact Goals: 5", "exact_goals_5", "Exactly 5 goals scored"),
    ("Exact Goals: 6+", "exact_goals_6_plus", "6 or more goals scored"),

    # Exhaustive Asian Handicaps (Home/Away are usually odds within these markets)
    ("Asian Handicap -2.5",  "ah_minus_2_5",  "Team starts with a -2.5 goal deficit"),
    ("Asian Handicap -2.0",  "ah_minus_2_0",  "Team starts with a -2.0 goal deficit"),
    ("Asian Handicap -1.75", "ah_minus_1_75", "Team starts with a -1.75 goal deficit"),
    ("Asian Handicap -1.5",  "ah_minus_1_5",  "Team starts with a -1.5 goal deficit"),
    ("Asian Handicap -1.25", "ah_minus_1_25", "Team starts with a -1.25 goal deficit"),
    ("Asian Handicap -1.0",  "ah_minus_1_0",  "Team starts with a -1.0 goal deficit"),
    ("Asian Handicap -0.75", "ah_minus_0_75", "Team starts with a -0.75 goal deficit"),
    ("Asian Handicap -0.5",  "ah_minus_0_5",  "Team starts with a -0.5 goal deficit"),
    ("Asian Handicap -0.25", "ah_minus_0_25", "Team starts with a -0.25 goal deficit"),
    ("Asian Handicap 0.0",   "ah_0_0",        "Draw No Bet equivalent (Level)"),
    ("Asian Handicap +0.25", "ah_plus_0_25",  "Team starts with a +0.25 goal advantage"),
    ("Asian Handicap +0.5",  "ah_plus_0_5",   "Team starts with a +0.5 goal advantage"),
    ("Asian Handicap +0.75", "ah_plus_0_75",  "Team starts with a +0.75 goal advantage"),
    ("Asian Handicap +1.0",  "ah_plus_1_0",   "Team starts with a +1.0 goal advantage"),
    ("Asian Handicap +1.5",  "ah_plus_1_5",   "Team starts with a +1.5 goal advantage"),
    ("Asian Handicap +2.5",  "ah_plus_2_5",   "Team starts with a +2.5 goal advantage"),

    # Common Correct Scores (Hardcoded)
    ("Correct Score: 1-0", "cs_1_0", "Home team wins 1-0"),
    ("Correct Score: 2-0", "cs_2_0", "Home team wins 2-0"),
    ("Correct Score: 2-1", "cs_2_1", "Home team wins 2-1"),
    ("Correct Score: 3-0", "cs_3_0", "Home team wins 3-0"),
    ("Correct Score: 3-1", "cs_3_1", "Home team wins 3-1"),
    ("Correct Score: 0-0", "cs_0_0", "Match ends 0-0"),
    ("Correct Score: 1-1", "cs_1_1", "Match ends 1-1"),
    ("Correct Score: 2-2", "cs_2_2", "Match ends 2-2"),
    ("Correct Score: 0-1", "cs_0_1", "Away team wins 1-0"),
    ("Correct Score: 0-2", "cs_0_2", "Away team wins 2-0"),
    ("Correct Score: 1-2", "cs_1_2", "Away team wins 2-1"),
    ("Correct Score: Other", "cs_other", "Any other unlisted scoreline"),

    # Goal Bands
    ("Goals Band: 0-1", "goals_band_0_1", "Total goals is 0 or 1"),
    ("Goals Band: 2-3", "goals_band_2_3", "Total goals is 2 or 3"),
    ("Goals Band: 4-5", "goals_band_4_5", "Total goals is 4 or 5"),
    ("Goals Band: 6+",  "goals_band_6_plus", "Total goals is 6 or more"),
]