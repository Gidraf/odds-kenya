# """
# app/workers/mappers/odibet_mappers.py
# ======================================
# Unified OdiBets market mapper for all sports.
# Outputs canonical slugs and specifiers compatible with MARKET_SEED.
# """

# from __future__ import annotations

# import re
# from typing import Dict, Optional, Tuple


# # ----------------------------------------------------------------------
# # BASEBALL
# # ----------------------------------------------------------------------
# class OdibetBaseballMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         if market_slug == "baseball_1x2":
#             return ("1x2", {"period": "full"})
#         if market_slug == "baseball_moneyline":
#             return ("baseball_moneyline", {"period": "full"})
#         if market_slug == "baseball_odd_even":
#             return ("odd_even", {"period": "full"})

#         spread_match = re.match(r"baseball_spread_minus_([\d_]+)$", market_slug)
#         if spread_match:
#             hcp = -float(spread_match.group(1).replace("_", "."))
#             return ("run_line", {"handicap": str(hcp)})

#         total_match = re.match(r"over_under_baseball_runs_([\d_]+)$", market_slug)
#         if total_match:
#             line = float(total_match.group(1).replace("_", "."))
#             return ("total_runs", {"line": str(line), "period": "full"})

#         home_total = re.match(r"baseball_home_team_total_([\d_]+)$", market_slug)
#         if home_total:
#             line = float(home_total.group(1).replace("_", "."))
#             return ("team_total_runs", {"team": "home", "line": str(line)})
#         away_total = re.match(r"baseball_away_team_total_([\d_]+)$", market_slug)
#         if away_total:
#             line = float(away_total.group(1).replace("_", "."))
#             return ("team_total_runs", {"team": "away", "line": str(line)})

#         if market_slug == "baseball_f5_1x2":
#             return ("f5_winner", {"period": "f5"})
#         f5_spread = re.match(r"baseball_f5_spread_minus_([\d_]+)$", market_slug)
#         if f5_spread:
#             hcp = -float(f5_spread.group(1).replace("_", "."))
#             return ("run_line", {"handicap": str(hcp), "period": "f5"})
#         f5_total = re.match(r"over_under_baseball_f5_runs_([\d_]+)$", market_slug)
#         if f5_total:
#             line = float(f5_total.group(1).replace("_", "."))
#             return ("total_runs", {"line": str(line), "period": "f5"})

#         inning_1x2 = re.match(r"baseball_(\d+)(?:st|nd|rd|th)_inning_1x2_\d+$", market_slug)
#         if inning_1x2:
#             return ("inning_winner", {"inning": inning_1x2.group(1)})
#         inning_total = re.match(r"over_under_baseball_(\d+)(?:st|nd|rd|th)_inning_runs_([\d_]+)$", market_slug)
#         if inning_total:
#             line = float(inning_total.group(2).replace("_", "."))
#             return ("inning_total_runs", {"inning": inning_total.group(1), "line": str(line)})

#         return None


# # ----------------------------------------------------------------------
# # BASKETBALL
# # ----------------------------------------------------------------------
# class OdibetBasketballMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         static = {
#             "basketball_1x2": ("1x2", {"period": "full"}),
#             "basketball_moneyline": ("basketball_moneyline", {"period": "full"}),
#             "basketball_draw_no_bet": ("draw_no_bet", {"period": "full"}),
#             "basketball_ht_ft": ("basketball_ht_ft", {"period": "full"}),
#             "first_half_basketball_1x2": ("first_half_1x2", {"period": "first_half"}),
#             "first_quarter_winning_margin": ("quarter_winner", {"quarter": "1"}),  # banded, but good enough
#         }
#         if market_slug in static:
#             return static[market_slug]

#         total_match = re.match(r"over_under_basketball_points(?:_incl_ot)?_([\d_]+)$", market_slug)
#         if total_match:
#             line = float(total_match.group(1).replace("_", "."))
#             return ("total_points", {"line": str(line), "period": "full"})

#         spread_match = re.match(r"basketball__minus_([\d_]+)$", market_slug)
#         if spread_match:
#             hcp = -float(spread_match.group(1).replace("_", "."))
#             return ("point_spread", {"handicap": str(hcp)})

#         fh_spread = re.match(r"first_half_basketball_spread_minus_([\d_]+)$", market_slug)
#         if fh_spread:
#             hcp = -float(fh_spread.group(1).replace("_", "."))
#             return ("point_spread", {"handicap": str(hcp), "period": "first_half"})

#         quarter_match = re.match(r"basketball__([1-4])$", market_slug)
#         if quarter_match:
#             return ("quarter_winner", {"quarter": quarter_match.group(1)})

#         home_total = re.match(r"basketball_home_team_total_incl_ot_([\d_]+)$", market_slug)
#         if home_total:
#             line = float(home_total.group(1).replace("_", "."))
#             return ("team_total_points", {"team": "home", "line": str(line)})
#         away_total = re.match(r"basketball_away_team_total_incl_ot_([\d_]+)$", market_slug)
#         if away_total:
#             line = float(away_total.group(1).replace("_", "."))
#             return ("team_total_points", {"team": "away", "line": str(line)})

#         combo = re.match(r"basketball_moneyline_and_total_([\d_]+)$", market_slug)
#         if combo:
#             line = float(combo.group(1).replace("_", "."))
#             return ("moneyline_and_total", {"line": str(line)})

#         if re.match(r"basketball__\d+_\d+$", market_slug):
#             return ("total_points_range", {})

#         return None


# # ----------------------------------------------------------------------
# # BOXING
# # ----------------------------------------------------------------------
# class OdibetBoxingMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         static = {
#             "boxing_1x2": ("boxing_winner", {"period": "full"}),
#             "boxing_moneyline": ("boxing_winner", {"period": "full", "two_way": "true"}),
#             "boxing_": ("fight_distance", {}),
#             "boxing__sr_winning_method_ko_decision": ("method_of_victory", {}),
#         }
#         if market_slug in static:
#             return static[market_slug]

#         exact_round = re.match(r"boxing__sr_winner_and_rounds_(\d+)$", market_slug)
#         if exact_round:
#             return ("round_betting", {"total_rounds": exact_round.group(1)})

#         range_round = re.match(r"boxing__sr_winner_and_round_range_(\d+)$", market_slug)
#         if range_round:
#             return ("round_group", {"total_rounds": range_round.group(1)})

#         return None


# # ----------------------------------------------------------------------
# # CRICKET
# # ----------------------------------------------------------------------
# class OdibetCricketMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         if market_slug == "cricket_winner_incl_super_over":
#             return ("cricket_match_winner", {"include_super_over": "true"})
#         if market_slug == "cricket_winner":
#             return ("cricket_match_winner", {})

#         total = re.match(r"over_under_cricket_runs_([\d_]+)$", market_slug)
#         if total:
#             line = float(total.group(1).replace("_", "."))
#             return ("total_runs_match", {"line": str(line)})

#         innings = re.match(r"over_under_cricket_innings_(\d+)_runs_([\d_]+)$", market_slug)
#         if innings:
#             line = float(innings.group(2).replace("_", "."))
#             return ("total_runs_innings", {"innings": innings.group(1), "line": str(line)})

#         player_runs = re.match(r"cricket_player_runs_([\d_]+)$", market_slug)
#         if player_runs:
#             line = float(player_runs.group(1).replace("_", "."))
#             return ("player_runs", {"line": str(line)})

#         player_wkts = re.match(r"cricket_player_wickets_([\d_]+)$", market_slug)
#         if player_wkts:
#             line = float(player_wkts.group(1).replace("_", "."))
#             return ("player_wickets", {"line": str(line)})

#         simple = {
#             "cricket_top_batsman": ("top_batsman", {}),
#             "cricket_top_bowler": ("top_bowler", {}),
#             "cricket_man_of_match": ("man_of_match_cricket", {}),
#             "cricket_method_of_dismissal": ("method_of_dismissal", {}),
#             "cricket_first_innings_lead": ("first_innings_lead", {}),
#             "cricket_toss_winner": ("toss_winner", {}),
#             "cricket_fifty_scored": ("fifty_scored", {}),
#             "cricket_century_scored": ("century_scored", {}),
#         }
#         return simple.get(market_slug, None)


# # ----------------------------------------------------------------------
# # DARTS
# # ----------------------------------------------------------------------
# class OdibetDartsMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         static = {
#             "darts_winner": ("darts_winner", {}),
#             "darts_match_winner": ("darts_winner", {}),
#             "soccer_winner": ("darts_winner", {}),
#             "soccer_": ("darts_winner", {}),
#             "darts_first_180": ("first_180", {}),
#             "darts_highest_checkout": ("highest_checkout", {}),
#             "darts_final_set": ("darts_final_set", {}),
#         }
#         if market_slug in static:
#             return static[market_slug]

#         total_180s = re.match(r"over_under_darts_180s_([\d_]+)$", market_slug)
#         if total_180s:
#             line = float(total_180s.group(1).replace("_", "."))
#             return ("total_180s", {"line": str(line)})

#         player_180s = re.match(r"darts_player_180s_([\d_]+)$", market_slug)
#         if player_180s:
#             line = float(player_180s.group(1).replace("_", "."))
#             return ("player_180s", {"line": str(line)})

#         correct = re.match(r"darts_correct_score_([\d_]+)$", market_slug)
#         if correct:
#             return ("darts_correct_score", {"score": correct.group(1).replace("_", "-")})

#         set_hcp = re.match(r"darts_set_handicap_([\d_]+)$", market_slug)
#         if set_hcp:
#             return ("darts_set_handicap", {"handicap": set_hcp.group(1).replace("_", ".")})

#         leg_hcp = re.match(r"darts_leg_handicap_([\d_]+)$", market_slug)
#         if leg_hcp:
#             return ("darts_leg_handicap", {"handicap": leg_hcp.group(1).replace("_", ".")})

#         total_legs = re.match(r"over_under_darts_total_legs_([\d_]+)$", market_slug)
#         if total_legs:
#             line = float(total_legs.group(1).replace("_", "."))
#             return ("total_legs", {"line": str(line)})

#         set_winner = re.match(r"darts_set_(\d+)_winner$", market_slug)
#         if set_winner:
#             return ("set_winner", {"set": set_winner.group(1)})

#         leg_winner = re.match(r"darts_leg_(\d+)_winner$", market_slug)
#         if leg_winner:
#             return ("leg_winner", {"leg": leg_winner.group(1)})

#         return None


# # ----------------------------------------------------------------------
# # ESOCCER – reuse soccer mapper (handled later)
# # ----------------------------------------------------------------------

# # ----------------------------------------------------------------------
# # HANDBALL
# # ----------------------------------------------------------------------
# class OdibetHandballMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         if market_slug == "handball_1x2":
#             return ("1x2", {"period": "match"})

#         total = re.match(r"over_under_handball_goals_([\d_]+)$", market_slug)
#         if total:
#             line = float(total.group(1).replace("_", "."))
#             return ("total_goals", {"line": str(line), "period": "match"})

#         fh_total = re.match(r"first_half_over_under_handball_goals_([\d_]+)$", market_slug)
#         if fh_total:
#             line = float(fh_total.group(1).replace("_", "."))
#             return ("total_goals", {"line": str(line), "period": "first_half"})

#         return None


# # ----------------------------------------------------------------------
# # ICE HOCKEY
# # ----------------------------------------------------------------------
# class OdibetIceHockeyMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         static = {
#             "ice-hockey_1x2": ("hockey_3way", {"period": "match"}),
#             "hockey_moneyline": ("hockey_moneyline", {"period": "match"}),
#             "hockey_double_chance": ("double_chance", {"period": "match"}),
#             "hockey_btts": ("btts", {"period": "match"}),
#             "hockey_draw_no_bet": ("draw_no_bet", {"period": "match"}),
#             "hockey_first_goal_1": ("first_goal", {}),
#             "hockey_highest_scoring_period": ("highest_scoring_period", {}),
#         }
#         if market_slug in static:
#             return static[market_slug]

#         euro = re.match(r"hockey_european_handicap_([\d_]+)$", market_slug)
#         if euro:
#             return ("european_handicap", {"handicap": euro.group(1).replace("_", ".")})

#         asian = re.match(r"hockey_asian_handicap_([\d_]+)$", market_slug)
#         if asian:
#             hcp = float(asian.group(1).replace("_", "."))
#             return ("asian_handicap", {"handicap": str(hcp)})
#         asian_minus = re.match(r"hockey_asian_handicap_minus_([\d_]+)$", market_slug)
#         if asian_minus:
#             hcp = -float(asian_minus.group(1).replace("_", "."))
#             return ("asian_handicap", {"handicap": str(hcp)})

#         ou = re.match(r"over_under_hockey_goals_([\d_]+)$", market_slug)
#         if ou:
#             line = float(ou.group(1).replace("_", "."))
#             return ("hockey_total_goals", {"line": str(line), "period": "match"})

#         home_total = re.match(r"hockey_home_team_total_([\d_]+)$", market_slug)
#         if home_total:
#             line = float(home_total.group(1).replace("_", "."))
#             return ("team_total_goals", {"team": "home", "line": str(line)})
#         away_total = re.match(r"hockey_away_team_total_([\d_]+)$", market_slug)
#         if away_total:
#             line = float(away_total.group(1).replace("_", "."))
#             return ("team_total_goals", {"team": "away", "line": str(line)})

#         win_margin = re.match(r"hockey_winning_margin_(\d+)$", market_slug)
#         if win_margin:
#             return ("hockey_winning_margin", {"band": win_margin.group(1)})

#         correct = re.match(r"hockey_correct_score_\d+$", market_slug)
#         if correct:
#             return ("hockey_correct_score", {})

#         combo = re.match(r"hockey_1x2_over_under_([\d_]+)$", market_slug)
#         if combo:
#             line = float(combo.group(1).replace("_", "."))
#             return ("result_and_total", {"line": str(line)})

#         return None


# # ----------------------------------------------------------------------
# # RUGBY (Union & League)
# # ----------------------------------------------------------------------
# class OdibetRugbyMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         # Detect sport code from match's sport name later; here we map generic slugs
#         static = {
#             "rugby_1x2": ("rugby_result", {"period": "match"}),
#             "rugby_double_chance": ("double_chance", {"period": "match"}),
#             "rugby_draw_no_bet": ("draw_no_bet", {"period": "match"}),
#             "rugby_ht_ft": ("rugby_ht_ft", {"period": "match"}),
#             "rugby_first_half_1x2": ("half_time_result", {"period": "first_half"}),
#         }
#         if market_slug in static:
#             return static[market_slug]

#         win_margin = re.match(r"rugby(?:_winning_margin)?_(\d+)$", market_slug)
#         if win_margin:
#             return ("rugby_winning_margin", {"band": win_margin.group(1)})

#         spread = re.match(r"rugby_spread_minus_([\d_]+)$", market_slug)
#         if spread:
#             hcp = -float(spread.group(1).replace("_", "."))
#             return ("asian_handicap", {"handicap": str(hcp)})

#         fh_spread = re.match(r"rugby_first_half_spread_minus_([\d_]+)$", market_slug)
#         if fh_spread:
#             hcp = -float(fh_spread.group(1).replace("_", "."))
#             return ("asian_handicap", {"handicap": str(hcp), "period": "first_half"})
#         fh_spread_plain = re.match(r"rugby_first_half_spread_([\d_]+)$", market_slug)
#         if fh_spread_plain:
#             hcp = float(fh_spread_plain.group(1).replace("_", "."))
#             return ("asian_handicap", {"handicap": str(hcp), "period": "first_half"})

#         return None


# # ----------------------------------------------------------------------
# # SOCCER (most comprehensive)
# # ----------------------------------------------------------------------
# class OdibetSoccerMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         static = {
#             "1x2": ("1x2", {"period": "match"}),
#             "soccer_1x2": ("1x2", {"period": "match"}),
#             "double_chance": ("double_chance", {"period": "match"}),
#             "draw_no_bet": ("draw_no_bet", {"period": "match"}),
#             "btts": ("btts", {"period": "match"}),
#             "first_half_btts": ("btts", {"period": "first_half"}),
#             "ht_ft": ("ht_ft", {"period": "match"}),
#             "first_half_1x2": ("first_half_1x2", {"period": "first_half"}),
#             "first_half_double_chance": ("double_chance", {"period": "first_half"}),
#             "first_team_to_score_1": ("first_team_to_score", {}),
#             "multigoals": ("multigoals", {}),
#             "exact_goals_6": ("exact_goals", {}),
#             "anytime_goalscorer": ("anytime_goalscorer", {}),
#             "first_goalscorer_1": ("first_goalscorer", {}),
#             "last_goalscorer": ("last_goalscorer", {}),
#             "1x2_btts": ("btts_and_result", {}),
#         }
#         if market_slug in static:
#             return static[market_slug]

#         ou = re.match(r"over_under_goals_([\d_]+)$", market_slug)
#         if ou:
#             line = float(ou.group(1).replace("_", "."))
#             return ("over_under_goals", {"line": str(line), "period": "match"})

#         fh_ou = re.match(r"first_half_over_under_goals_([\d_]+)$", market_slug)
#         if fh_ou:
#             line = float(fh_ou.group(1).replace("_", "."))
#             return ("over_under_goals", {"line": str(line), "period": "first_half"})

#         euro = re.match(r"european_handicap_([\d_]+)$", market_slug)
#         if euro:
#             return ("european_handicap", {"handicap": euro.group(1).replace("_", ".")})

#         asian = re.match(r"asian_handicap_([\d_]+)$", market_slug)
#         if asian:
#             hcp = float(asian.group(1).replace("_", "."))
#             return ("asian_handicap", {"handicap": str(hcp)})
#         asian_minus = re.match(r"asian_handicap_minus_([\d_]+)$", market_slug)
#         if asian_minus:
#             hcp = -float(asian_minus.group(1).replace("_", "."))
#             return ("asian_handicap", {"handicap": str(hcp)})

#         fh_asian = re.match(r"first_half_asian_handicap_minus_([\d_]+)$", market_slug)
#         if fh_asian:
#             hcp = -float(fh_asian.group(1).replace("_", "."))
#             return ("asian_handicap", {"handicap": str(hcp), "period": "first_half"})

#         home_total = re.match(r"home_over_under_([\d_]+)$", market_slug)
#         if home_total:
#             line = float(home_total.group(1).replace("_", "."))
#             return ("total_goals_home", {"line": str(line)})
#         away_total = re.match(r"away_over_under_([\d_]+)$", market_slug)
#         if away_total:
#             line = float(away_total.group(1).replace("_", "."))
#             return ("total_goals_away", {"line": str(line)})

#         win_margin = re.match(r"winning_margin_(\d+)$", market_slug)
#         if win_margin:
#             return ("winning_margin", {"band_max": win_margin.group(1)})

#         combo_1x2_ou = re.match(r"1x2_over_under_([\d_]+)$", market_slug)
#         if combo_1x2_ou:
#             line = float(combo_1x2_ou.group(1).replace("_", "."))
#             return ("result_and_over_under", {"line": str(line)})

#         dc_ou = re.match(r"double_chance_over_under_([\d_]+)$", market_slug)
#         if dc_ou:
#             line = float(dc_ou.group(1).replace("_", "."))
#             return ("double_chance_and_total", {"line": str(line)})

#         btts_ou = re.match(r"over_under_btts_([\d_]+)$", market_slug)
#         if btts_ou:
#             line = float(btts_ou.group(1).replace("_", "."))
#             return ("btts_and_total", {"line": str(line)})

#         htft_ou = re.match(r"ht_ft_over_under_([\d_]+)$", market_slug)
#         if htft_ou:
#             line = float(htft_ou.group(1).replace("_", "."))
#             return ("ht_ft_and_total", {"line": str(line)})

#         if market_slug == "double_chance_btts":
#             return ("double_chance_and_btts", {})

#         if market_slug == "first_half_double_chance_btts":
#             return ("double_chance_and_btts", {"period": "first_half"})

#         if re.match(r"soccer__\d+", market_slug):
#             return ("goal_time_band", {})

#         return None

# class OdibetTableTennisMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         # Direct mapping for the messy "soccer_*" slugs used by OdiBets for table tennis
#         if market_slug in ("soccer_winner", "soccer_", "soccer_winner"):
#             return ("tt_winner", {"period": "match"})

#         # Total points (over/under)
#         total_match = re.match(r"soccer_total_points_([\d_]+)$", market_slug)
#         if total_match:
#             line = float(total_match.group(1).replace("_", "."))
#             return ("tt_total_points", {"line": str(line)})

#         # Point handicap (positive or negative)
#         # Note: raw slug may contain a tab character "\t" – we clean it
#         clean_slug = market_slug.replace("\t", "")
#         handicap_match = re.match(r"soccer_point_handicap_minus_([\d_]+)$", clean_slug)
#         if handicap_match:
#             hcp = -float(handicap_match.group(1).replace("_", "."))
#             return ("point_handicap", {"handicap": str(hcp)})
#         handicap_pos = re.match(r"soccer_point_handicap_([\d_]+)$", clean_slug)
#         if handicap_pos:
#             hcp = float(handicap_pos.group(1).replace("_", "."))
#             return ("point_handicap", {"handicap": str(hcp)})

#         # Generic "soccer__minus_X_X" also point handicap
#         generic_minus = re.match(r"soccer__minus_([\d_]+)$", market_slug)
#         if generic_minus:
#             hcp = -float(generic_minus.group(1).replace("_", "."))
#             return ("point_handicap", {"handicap": str(hcp)})

#         generic_plain = re.match(r"soccer__(\d+_\d+)$", market_slug)
#         if generic_plain:
#             # Could be total points again (e.g., soccer__77_5)
#             # Check if it matches total pattern
#             parts = generic_plain.group(1).split("_")
#             if len(parts) == 2 and parts[1] in ("5", "0"):
#                 line = float(generic_plain.group(1).replace("_", "."))
#                 return ("tt_total_points", {"line": str(line)})

#         return None


# # ----------------------------------------------------------------------
# # TENNIS
# # ----------------------------------------------------------------------
# class OdibetTennisMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         if market_slug == "tennis_match_winner":
#             return ("tennis_match_winner", {"period": "match"})
#         # Additional tennis markets can be added here if encountered
#         return None


# # ----------------------------------------------------------------------
# # VOLLEYBALL
# # ----------------------------------------------------------------------
# class OdibetVolleyballMapper:
#     @classmethod
#     def get_market_info(cls, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#         # Match winner
#         if market_slug == "volleyball_match_winner":
#             return ("volleyball_winner", {"period": "match"})

#         # Total points (full match)
#         total_match = re.match(r"volleyball_total_points_([\d_]+)$", market_slug)
#         if total_match:
#             line = float(total_match.group(1).replace("_", "."))
#             return ("volleyball_total_points", {"line": str(line), "period": "match"})

#         # Point handicap (full match)
#         hcp_match = re.match(r"volleyball_point_handicap_([\d_]+)$", market_slug)
#         if hcp_match:
#             hcp = float(hcp_match.group(1).replace("_", "."))
#             return ("volleyball_point_handicap", {"handicap": str(hcp), "period": "match"})

#         # Correct set score (best of 5)
#         if market_slug == "volleyball__sr_correct_score_bestof_5":
#             return ("volleyball_set_betting", {"max_sets": "5"})

#         # First set winner
#         if market_slug == "first_set_winner":
#             return ("volleyball_set_winner", {"set": "1"})

#         # Total points for a specific set (e.g., volleyball__179_5, first_set_total_points_45_5)
#         # pattern: volleyball__{line}_5
#         set_total = re.match(r"volleyball__(\d+_\d+)$", market_slug)
#         if set_total:
#             line = float(set_total.group(1).replace("_", "."))
#             return ("volleyball_total_points", {"line": str(line), "period": "match"})  # actually full match, not set

#         # First set total points
#         first_set_total = re.match(r"first_set_total_points_([\d_]+)$", market_slug)
#         if first_set_total:
#             line = float(first_set_total.group(1).replace("_", "."))
#             return ("volleyball_set_total_points", {"set": "1", "line": str(line)})

#         # First set point handicap
#         fh_hcp_minus = re.match(r"first_set_point_handicap_minus_([\d_]+)$", market_slug)
#         if fh_hcp_minus:
#             hcp = -float(fh_hcp_minus.group(1).replace("_", "."))
#             return ("volleyball_set_point_handicap", {"set": "1", "handicap": str(hcp)})
#         fh_hcp_pos = re.match(r"first_set_point_handicap_([\d_]+)$", market_slug)
#         if fh_hcp_pos:
#             hcp = float(fh_hcp_pos.group(1).replace("_", "."))
#             return ("volleyball_set_point_handicap", {"set": "1", "handicap": str(hcp)})

#         # Set 1 odd/even
#         if market_slug == "set_1_volleyball_":
#             return ("volleyball_set_odd_even", {"set": "1"})

#         # Other generic volleyball__<line>_5 (already handled above)
#         # The catch-all "volleyball_" – skip (ambiguous)
#         return None


# # ----------------------------------------------------------------------
# # DISPATCHER
# # ----------------------------------------------------------------------
# def get_od_market_info(sport: str, market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#     """
#     Main entry point: sport = "soccer", "baseball", "basketball", etc.
#     """
#     sport_lower = sport.lower()
#     if sport_lower == "baseball":
#         return OdibetBaseballMapper.get_market_info(market_slug)
#     if sport_lower == "basketball":
#         return OdibetBasketballMapper.get_market_info(market_slug)
#     if sport_lower == "boxing":
#         return OdibetBoxingMapper.get_market_info(market_slug)
#     if sport_lower == "cricket":
#         return OdibetCricketMapper.get_market_info(market_slug)
#     if sport_lower == "darts":
#         return OdibetDartsMapper.get_market_info(market_slug)
#     if sport_lower in ("esoccer", "efootball"):
#         return OdibetSoccerMapper.get_market_info(market_slug)  # reuse soccer mapper
#     if sport_lower == "handball":
#         return OdibetHandballMapper.get_market_info(market_slug)
#     if sport_lower in ("ice-hockey", "ice_hockey", "hockey"):
#         return OdibetIceHockeyMapper.get_market_info(market_slug)
#     if sport_lower in ("rugby", "rugby union", "rugby league"):
#         return OdibetRugbyMapper.get_market_info(market_slug)
#     if sport_lower == "soccer":
#         return OdibetSoccerMapper.get_market_info(market_slug)
#     if sport_lower in ("table-tennis", "table tennis"):
#         return OdibetTableTennisMapper.get_market_info(market_slug)
#     if sport_lower == "tennis":
#         return OdibetTennisMapper.get_market_info(market_slug)
#     if sport_lower == "volleyball":
#         return OdibetVolleyballMapper.get_market_info(market_slug)
#     return None