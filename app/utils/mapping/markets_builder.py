from __future__ import annotations

# ==============================================================================
# 1. GENERATOR UTILITIES (THE LOOPS)
# ==============================================================================

def generate_over_under(name_prefix: str, slug_prefix: str, start: float, end: float, step: float = 0.5) -> list[tuple[str, str, str]]:
    """Generates exhaustive Over/Under markets."""
    markets = []
    current = int(start * 10)
    stop = int(end * 10)
    step_int = int(step * 10)
    
    for val_int in range(current, stop + step_int, step_int):
        val = val_int / 10.0
        display_val = f"{val:g}" 
        slug_val = str(val).replace(".", "_")
        
        markets.append((
            f"Over {display_val} {name_prefix}",
            f"over_{slug_val}_{slug_prefix}",
            f"Total {name_prefix.lower()} is more than {display_val}"
        ))
        markets.append((
            f"Under {display_val} {name_prefix}",
            f"under_{slug_val}_{slug_prefix}",
            f"Total {name_prefix.lower()} is less than {display_val}"
        ))
    return markets

def generate_spreads(sport: str, start: float, end: float, step: float = 0.5) -> list[tuple[str, str, str]]:
    """Generates exhaustive Point/Run/Puck Spreads (Handicaps)."""
    markets = []
    current = int(start * 10)
    stop = int(end * 10)
    step_int = int(step * 10)
    
    for val_int in range(current, stop + step_int, step_int):
        val = val_int / 10.0
        if val == 0:
            continue
            
        sign = "+" if val > 0 else ""
        display_val = f"{sign}{val:g}"
        slug_val = str(val).replace(".", "_").replace("-", "minus_").replace("+", "plus_")
        
        markets.append((
            f"Home Spread {display_val}",
            f"home_spread_{slug_val}_{sport}",
            f"Home team handicap {display_val}"
        ))
        markets.append((
            f"Away Spread {display_val}",
            f"away_spread_{slug_val}_{sport}",
            f"Away team handicap {display_val}"
        ))
    return markets

def generate_asian_handicaps(start: float, end: float) -> list[tuple[str, str, str]]:
    """Generates Quarter-line Asian Handicaps (0.25, 0.75, etc)."""
    markets = []
    current = int(start * 100)
    stop = int(end * 100)
    
    for val_int in range(current, stop + 25, 25):
        val = val_int / 100.0
        if val == 0:
            markets.append(("Asian Handicap 0.0", "ah_0_0", "Draw No Bet equivalent"))
            continue
            
        sign = "+" if val > 0 else ""
        display_val = f"{sign}{val:g}"
        slug_val = str(val).replace(".", "_").replace("-", "minus_")
        
        markets.append((
            f"Asian Handicap {display_val}",
            f"ah_{slug_val}",
            f"Asian Handicap line {display_val}"
        ))
    return markets

def generate_winning_margins(sport: str, max_margin: int, step: int = 5) -> list[tuple[str, str, str]]:
    """Generates winning margin bands."""
    markets = []
    for i in range(1, max_margin, step):
        end = i + step - 1
        markets.append((f"Home Win by {i}-{end}", f"home_win_{i}_{end}_{sport}", f"Home wins by {i} to {end} margin"))
        markets.append((f"Away Win by {i}-{end}", f"away_win_{i}_{end}_{sport}", f"Away wins by {i} to {end} margin"))
    
    markets.append((f"Home Win by {max_margin}+", f"home_win_{max_margin}_plus_{sport}", f"Home wins by {max_margin} or more"))
    markets.append((f"Away Win by {max_margin}+", f"away_win_{max_margin}_plus_{sport}", f"Away wins by {max_margin} or more"))
    return markets

def generate_combat_rounds(sport: str, max_rounds: int) -> list[tuple[str, str, str]]:
    """Generates exact round betting for MMA/Boxing."""
    markets = []
    for r in range(1, max_rounds + 1):
        markets.append((f"Fighter 1 Wins in Round {r}", f"f1_win_r{r}_{sport}", f"Fighter 1 wins exactly in Round {r}"))
        markets.append((f"Fighter 2 Wins in Round {r}", f"f2_win_r{r}_{sport}", f"Fighter 2 wins exactly in Round {r}"))
    
    markets.append(("Fighter 1 Wins by Decision", f"f1_win_dec_{sport}", "Fighter 1 wins by judges decision"))
    markets.append(("Fighter 2 Wins by Decision", f"f2_win_dec_{sport}", "Fighter 2 wins by judges decision"))
    return markets


# ==============================================================================
# 2. BUILDING THE EXHAUSTIVE SPORT DICTIONARIES
# ==============================================================================

# -- UNIVERSAL (All Sports Fallbacks) --
UNIVERSAL_MARKETS = [
    ("Match Winner",         "match_winner",        "Outright winner of the match (2-way)"),
    ("Draw No Bet",          "draw_no_bet",         "Bet on either team; stake refunded on draw"),
    ("Double Chance",        "double_chance",       "Home/Draw, Home/Away, or Draw/Away"),
    ("Half Time Result",     "half_time_result",    "Result at half-time only"),
    ("To Qualify",           "to_qualify",          "Which team progresses in a cup/knockout tie"),
]

# -- FOOTBALL (SOCCER) --
FOOTBALL_MARKETS = [
    ("1X2", "1x2", "Home / Draw / Away — 3-way match result"),
    ("Both Teams To Score - Yes", "btts_yes", "Both teams score"),
    ("Both Teams To Score - No", "btts_no", "At least one team fails to score"),
    ("Anytime Goalscorer", "anytime_goalscorer", "Player scores at any point in the match"),
    ("First Goalscorer", "first_goalscorer", "Player to score the first goal"),
    ("Correct Score: 0-0", "cs_0_0", "Match ends 0-0"),
    ("Correct Score: 1-0", "cs_1_0", "Home wins 1-0"),
    ("Correct Score: 0-1", "cs_0_1", "Away wins 1-0"),
    ("Correct Score: 1-1", "cs_1_1", "Match ends 1-1"),
    ("Correct Score: 2-1", "cs_2_1", "Home wins 2-1"),
    ("Correct Score: 1-2", "cs_1_2", "Away wins 2-1"),
    ("Correct Score: Other", "cs_other", "Any other unlisted scoreline"),
]
FOOTBALL_MARKETS += generate_over_under("Goals", "goals", 0.5, 9.5, 1.0) 
FOOTBALL_MARKETS += generate_over_under("Corners", "corners", 4.5, 18.5, 1.0)
FOOTBALL_MARKETS += generate_over_under("Cards", "cards", 1.5, 9.5, 1.0)
FOOTBALL_MARKETS += generate_asian_handicaps(-4.0, 4.0)

# -- BASKETBALL --
BASKETBALL_MARKETS = [
    ("Moneyline", "basketball_moneyline", "Outright winner (no draw)"),
    ("Will There Be Overtime", "basketball_ot_yes", "Match goes to OT"),
    ("First to 10 Points", "race_to_10_pts", "First team to reach 10 points"),
    ("First to 20 Points", "race_to_20_pts", "First team to reach 20 points"),
]
BASKETBALL_MARKETS += generate_over_under("Points", "pts", 130.5, 260.5, 1.0)
BASKETBALL_MARKETS += generate_spreads("basketball", -30.5, 30.5, 0.5)
BASKETBALL_MARKETS += generate_winning_margins("basketball", 26, 5)

# -- AMERICAN FOOTBALL (NFL) --
NFL_MARKETS = [
    ("Moneyline", "nfl_moneyline", "Outright winner"),
    ("Anytime Touchdown Scorer", "nfl_anytime_td", "Player scores a touchdown"),
    ("First Touchdown Scorer", "nfl_first_td", "First player to score a TD"),
]
NFL_MARKETS += generate_over_under("Points", "nfl_pts", 30.5, 75.5, 0.5)
NFL_MARKETS += generate_spreads("nfl", -24.5, 24.5, 0.5)
NFL_MARKETS += generate_winning_margins("nfl", 21, 7)

# -- TENNIS --
TENNIS_MARKETS = [
    ("Match Winner", "tennis_match_winner", "Outright winner"),
    ("Correct Score 2-0", "tennis_cs_2_0", "Win 2 sets to 0"),
    ("Correct Score 2-1", "tennis_cs_2_1", "Win 2 sets to 1"),
]
TENNIS_MARKETS += generate_over_under("Games", "tennis_games", 16.5, 45.5, 1.0)
TENNIS_MARKETS += generate_spreads("tennis_games", -8.5, 8.5, 0.5)

# -- BASEBALL --
BASEBALL_MARKETS = [
    ("Moneyline", "baseball_moneyline", "Outright winner"),
    ("Will there be Extra Innings", "baseball_extra_innings", "Game goes to 10th inning"),
]
BASEBALL_MARKETS += generate_over_under("Runs", "baseball_runs", 4.5, 15.5, 0.5)
BASEBALL_MARKETS += generate_spreads("baseball", -4.5, 4.5, 0.5)

# -- ICE HOCKEY --
HOCKEY_MARKETS = [
    ("Moneyline (Inc. OT/SO)", "hockey_moneyline", "Outright winner including overtime"),
    ("60 Min 3-Way", "hockey_1x2", "Home / Draw / Away at end of regulation"),
]
HOCKEY_MARKETS += generate_over_under("Goals", "hockey_goals", 3.5, 9.5, 0.5)
HOCKEY_MARKETS += generate_spreads("hockey", -3.5, 3.5, 0.5)

# -- CRICKET --
CRICKET_MARKETS = [
    ("Match Winner", "cricket_winner", "Outright match winner"),
    ("Toss Winner", "cricket_toss", "Who wins the coin toss"),
    ("Top Batsman", "top_batsman", "Highest run scorer"),
    ("Top Bowler", "top_bowler", "Most wickets taken"),
]
CRICKET_MARKETS += generate_over_under("Match Runs", "cricket_runs", 100.5, 400.5, 5.0) 

# -- RUGBY (UNION & LEAGUE) --
RUGBY_MARKETS = [
    ("Match Winner", "rugby_winner", "Outright winner"),
    ("First Try Scorer", "rugby_first_try", "Player to score the first try"),
]
RUGBY_MARKETS += generate_over_under("Points", "rugby_pts", 20.5, 80.5, 1.0)
RUGBY_MARKETS += generate_spreads("rugby", -35.5, 35.5, 0.5)

# -- COMBAT SPORTS (BOXING & MMA) --
BOXING_MARKETS = [
    ("Fight Winner", "boxing_winner", "Outright fight winner"),
    ("Draw Or Technical Draw", "boxing_draw", "Fight ends in a draw"),
]
BOXING_MARKETS += generate_over_under("Rounds", "boxing_rounds", 4.5, 11.5, 1.0)
BOXING_MARKETS += generate_combat_rounds("boxing", 12)

MMA_MARKETS = [
    ("Fight Winner", "mma_winner", "Outright fight winner"),
    ("Method of Victory: KO/TKO", "mma_method_ko", "Fight ends via Knockout or TKO"),
    ("Method of Victory: Submission", "mma_method_sub", "Fight ends via Submission"),
    ("Fight to go the distance - Yes", "mma_distance_yes", "Fight lasts all scheduled rounds"),
    ("Fight to go the distance - No", "mma_distance_no", "Fight finishes before final bell"),
]
MMA_MARKETS += generate_over_under("Rounds", "mma_rounds", 0.5, 4.5, 1.0)
MMA_MARKETS += generate_combat_rounds("mma", 5)

# -- VIRTUAL SPORTS (eFootball / eBasketball) --
EFOOTBALL_MARKETS = [
    ("1X2", "efootball_1x2", "Home / Draw / Away — 3-way match result"),
    ("Both Teams To Score - Yes", "efootball_btts_yes", "Both teams score"),
    ("Both Teams To Score - No", "efootball_btts_no", "At least one team fails to score"),
]
EFOOTBALL_MARKETS += generate_over_under("Goals", "efootball_goals", 0.5, 12.5, 1.0) 
EFOOTBALL_MARKETS += generate_asian_handicaps(-5.0, 5.0)

EBASKETBALL_MARKETS = [
    ("Moneyline", "ebasketball_moneyline", "Outright winner (no draw)"),
]
EBASKETBALL_MARKETS += generate_over_under("Points", "ebasketball_pts", 40.5, 130.5, 1.0)
EBASKETBALL_MARKETS += generate_spreads("ebasketball", -25.5, 25.5, 0.5)

# -- ESPORTS (eGames) --
EGAMES_MARKETS = [
    ("Match Winner", "egames_winner", "Outright match winner"),
    ("Map 1 Winner", "egames_map1_winner", "Winner of first map"),
    ("Map 2 Winner", "egames_map2_winner", "Winner of second map"),
    ("First Blood", "egames_first_blood", "Team to get the first kill"),
    ("First Tower/Turret", "egames_first_tower", "Team to destroy the first tower"),
    ("First Dragon/Roshan", "egames_first_dragon", "Team to slay the first major objective"),
    ("First to 10 Kills", "egames_race_10_kills", "First team to reach 10 overall kills"),
    ("Pistol Round Winner (Map 1)", "egames_pistol_m1", "Winner of round 1 on map 1"),
]
EGAMES_MARKETS += generate_over_under("Maps", "egames_maps", 1.5, 4.5, 1.0) 
EGAMES_MARKETS += generate_spreads("egames_maps", -2.5, 2.5, 1.0) 
EGAMES_MARKETS += generate_over_under("Total Kills", "egames_kills", 15.5, 65.5, 1.0)
EGAMES_MARKETS += generate_over_under("Total Rounds", "egames_rounds", 18.5, 30.5, 1.0)

# -- RACKET/TABLE/PUB SPORTS --
OTHER_SPORTS_MARKETS = [
    ("Match Winner", "other_winner", "Outright match winner"),
]
OTHER_SPORTS_MARKETS += generate_over_under("Sets", "sets", 2.5, 6.5, 1.0)
OTHER_SPORTS_MARKETS += generate_over_under("Total Points/Frames", "total_pts", 10.5, 200.5, 1.0)
OTHER_SPORTS_MARKETS += generate_spreads("sets", -3.5, 3.5, 1.5)


# ==============================================================================
# 3. MASTER EXPORT
# ==============================================================================

ALL_MARKETS_BY_SPORT = {
    None: UNIVERSAL_MARKETS,  # Universal fallback markets
    "Football": FOOTBALL_MARKETS,
    "Basketball": BASKETBALL_MARKETS,
    "American Football": NFL_MARKETS,
    "Tennis": TENNIS_MARKETS,
    "Baseball": BASEBALL_MARKETS,
    "Ice Hockey": HOCKEY_MARKETS,
    "Cricket": CRICKET_MARKETS,
    "Rugby": RUGBY_MARKETS,
    "Boxing": BOXING_MARKETS,
    "MMA": MMA_MARKETS,
    "eFootball": EFOOTBALL_MARKETS,
    "eBasketball": EBASKETBALL_MARKETS,
    "Esports": EGAMES_MARKETS,
    "Volleyball": OTHER_SPORTS_MARKETS,
    "Table Tennis": OTHER_SPORTS_MARKETS,
    "Darts": OTHER_SPORTS_MARKETS,
    "Snooker": OTHER_SPORTS_MARKETS,
}

def get_all_generated_markets() -> dict[str | None, list[tuple[str, str, str]]]:
    """
    Returns the massive dictionary of all generated markets.
    Can be imported directly into seed files.
    """
    return ALL_MARKETS_BY_SPORT