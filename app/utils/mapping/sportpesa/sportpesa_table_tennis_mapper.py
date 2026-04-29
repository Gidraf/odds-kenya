# """
# app/workers/mappers/odibet_table_tennis.py
# ===========================================
# OdiBets Table Tennis market mapper.
# Converts OdiBets-specific market slugs (as produced by od_harvester.py)
# into canonical market slugs + specifiers for internal use.
# """

# from __future__ import annotations

# import re
# from typing import Dict, Optional, Tuple


# class OdibetTableTennisMapper:
#     """Maps OdiBets Table Tennis JSON market slugs to canonical slugs + specifiers."""

#     # Direct mapping for simple markets (no specifiers)
#     STATIC_MARKETS: Dict[str, Tuple[str, Dict[str, str]]] = {
#         "soccer_winner":      ("tt_winner", {"period": "match"}),
#         "soccer_":            ("tt_winner", {"period": "match"}),   # fallback for ambiguous slug
#     }

#     @staticmethod
#     def format_line(value: float) -> str:
#         """Convert a numeric line into a URL-safe slug fragment."""
#         if value == 0:
#             return "0_0"
#         val_str = f"{value:g}".replace(".", "_")
#         return val_str.replace("-", "minus_") if value < 0 else val_str

#     @classmethod
#     def get_market_info(
#         cls, market_slug: str
#     ) -> Optional[Tuple[str, Dict[str, str]]]:
#         """
#         Parse an OdiBets market slug and return (canonical_slug, specifiers).

#         Examples:
#             "soccer_winner"                        → ("tt_winner", {"period": "match"})
#             "soccer_total_points_77_5"             → ("tt_total_points", {"line": "77.5"})
#             "soccer_point_handicap_minus_0_5"      → ("point_handicap", {"handicap": "-0.5"})
#             "soccer__minus_1_5"                    → ("point_handicap", {"handicap": "-1.5"})
#             "soccer__77_5"                         → ("tt_total_points", {"line": "77.5"})
#         """
#         # ----- Static mappings -----
#         if market_slug in cls.STATIC_MARKETS:
#             return cls.STATIC_MARKETS[market_slug]

#         # ----- Total points (over/under) -----
#         # Patterns: soccer_total_points_77_5  and  soccer__77_5
#         total_match = re.match(r"soccer_total_points_([\d_]+)$", market_slug)
#         if not total_match:
#             total_match = re.match(r"soccer__(\d+_\d+)$", market_slug)
#         if total_match:
#             line = float(total_match.group(1).replace("_", "."))
#             return ("tt_total_points", {"line": str(line)})

#         # ----- Point handicap (positive or negative) -----
#         # Patterns: soccer_point_handicap_minus_0_5  or  soccer_point_handicap_0_5
#         # Also soccer__minus_1_5, soccer__minus_0_5
#         handicap_match = re.match(r"soccer_point_handicap_minus_([\d_]+)$", market_slug)
#         if handicap_match:
#             hcp = -float(handicap_match.group(1).replace("_", "."))
#             return ("point_handicap", {"handicap": str(hcp)})
#         handicap_pos = re.match(r"soccer_point_handicap_([\d_]+)$", market_slug)
#         if handicap_pos:
#             hcp = float(handicap_pos.group(1).replace("_", "."))
#             return ("point_handicap", {"handicap": str(hcp)})

#         # Soccer__minus_X_X
#         generic_minus = re.match(r"soccer__minus_([\d_]+)$", market_slug)
#         if generic_minus:
#             hcp = -float(generic_minus.group(1).replace("_", "."))
#             return ("point_handicap", {"handicap": str(hcp)})

#         # ----- Unknown market -----
#         return None

#     @classmethod
#     def get_canonical_slug(cls, market_slug: str) -> Optional[str]:
#         info = cls.get_market_info(market_slug)
#         return info[0] if info else None

#     @classmethod
#     def transform_outcome(cls, market_slug: str, outcome_key: str) -> str:
#         """
#         Convert OdiBets outcome keys to canonical outcome names.
#         """
#         # For winner markets (home/away)
#         if market_slug in ("soccer_winner", "soccer_"):
#             mapping = {"1": "home", "2": "away"}
#             return mapping.get(outcome_key, outcome_key)

#         # For handicap markets: outcome keys "1" or "2"
#         if market_slug.startswith(("soccer_point_handicap", "soccer__minus")):
#             return "home" if outcome_key == "1" else "away" if outcome_key == "2" else outcome_key

#         # For over/under: outcome keys "over" or "under"
#         if market_slug.startswith(("soccer_total_points", "soccer__")):
#             return outcome_key

#         return outcome_key


# def get_od_table_tennis_market_info(market_slug: str) -> Optional[Tuple[str, Dict[str, str]]]:
#     return OdibetTableTennisMapper.get_market_info(market_slug)