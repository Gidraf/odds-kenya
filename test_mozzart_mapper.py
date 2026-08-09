"""
Test Mozzart Market Mapper & ID handling.
"""
import unittest
from app.workers.mappers.mozzart import MozzartMapper, _MZ_GAME_TO_SLUG, MZ_SPORT_SLUGS
from app.workers.mappers.shared import normalize_line, normalize_outcome

class TestMozzartMapper(unittest.TestCase):

    def test_canonical_slug_mapping(self):
        # 1x2
        self.assertEqual(MozzartMapper.get_canonical_slug(1), "1x2")
        # Over Under Goals with line
        self.assertEqual(MozzartMapper.get_canonical_slug(2, special_value="2.5", special_type="MARGIN"), "over_under_goals_2.5")
        # Asian Handicap Basketball
        self.assertEqual(MozzartMapper.get_canonical_slug(111, special_value="-3.5", special_type="HANDICAP"), "asian_handicap_-3.5")
        # Tennis Total Games
        self.assertEqual(MozzartMapper.get_canonical_slug(331, special_value="21.5", special_type="MARGIN"), "total_games_21.5")
        # Table Tennis Set Handicap
        self.assertEqual(MozzartMapper.get_canonical_slug(840, special_value="-1.5", special_type="HANDICAP"), "first_set_handicap_-1.5")

    def test_outcome_key_normalization(self):
        self.assertEqual(MozzartMapper.normalize_outcome_key("1x2", "1"), "1")
        self.assertEqual(MozzartMapper.normalize_outcome_key("1x2", "X"), "X")
        self.assertEqual(MozzartMapper.normalize_outcome_key("1x2", "2"), "2")
        self.assertEqual(MozzartMapper.normalize_outcome_key("over_under_goals_2.5", "over"), "over")
        self.assertEqual(MozzartMapper.normalize_outcome_key("over_under_goals_2.5", "under"), "under")
        self.assertEqual(MozzartMapper.normalize_outcome_key("btts", "yes"), "yes")
        self.assertEqual(MozzartMapper.normalize_outcome_key("btts", "no"), "no")

    def test_all_53_game_ids_defined(self):
        self.assertGreaterEqual(len(_MZ_GAME_TO_SLUG), 35)
        self.assertIn(1, _MZ_GAME_TO_SLUG)
        self.assertIn(109, _MZ_GAME_TO_SLUG)
        self.assertIn(331, _MZ_GAME_TO_SLUG)
        self.assertIn(831, _MZ_GAME_TO_SLUG)

    def test_sport_slugs(self):
        self.assertEqual(MZ_SPORT_SLUGS[1], "soccer")
        self.assertEqual(MZ_SPORT_SLUGS[2], "basketball")
        self.assertEqual(MZ_SPORT_SLUGS[5], "tennis")
        self.assertEqual(MZ_SPORT_SLUGS[20], "table-tennis")

if __name__ == "__main__":
    unittest.main()
