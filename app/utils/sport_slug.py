"""
app/utils/sport_slug.py
=======================
Single source of truth for sport slug normalisation.
Import this everywhere instead of inline dicts.
"""

SLUG_CANONICAL: dict[str, str] = {
    "soccer":            "football",
    "football":          "football",
    "esoccer":           "esoccer",
    "efootball":         "esoccer",
    "e-football":        "esoccer",
    "virtual-football":  "esoccer",
    "basketball":        "basketball",
    "tennis":            "tennis",
    "ice-hockey":        "ice-hockey",
    "icehockey":         "ice-hockey",
    "ice_hockey":        "ice-hockey",
    "volleyball":        "volleyball",
    "cricket":           "cricket",
    "rugby":             "rugby",
    "rugby-league":      "rugby",
    "rugby-union":       "rugby",
    "rugby_league":      "rugby",
    "rugby_union":       "rugby",
    "table-tennis":      "table-tennis",
    "tabletennis":       "table-tennis",
    "table_tennis":      "table-tennis",
    "boxing":            "boxing",
    "handball":          "handball",
    "mma":               "mma",
    "ufc":               "mma",
    "darts":             "darts",
    "american-football": "american-football",
    "americanfootball":  "american-football",
    "american_football": "american-football",
    "nfl":               "american-football",
    "baseball":          "baseball",
}

SLUG_TO_NAME: dict[str, str] = {
    "football":          "Football",
    "esoccer":           "eSoccer",
    "basketball":        "Basketball",
    "tennis":            "Tennis",
    "ice-hockey":        "Ice Hockey",
    "volleyball":        "Volleyball",
    "cricket":           "Cricket",
    "rugby":             "Rugby",
    "table-tennis":      "Table Tennis",
    "boxing":            "Boxing",
    "handball":          "Handball",
    "mma":               "MMA",
    "darts":             "Darts",
    "american-football": "American Football",
    "baseball":          "Baseball",
}

# All DB values that mean football — used in ilike OR queries
FOOTBALL_ALIASES = ("football", "soccer")


def canonical_slug(raw: str) -> str:
    """
    Normalise any sport slug/name to canonical form.
    'soccer' -> 'football', 'Soccer' -> 'football', etc.
    Returns the input lowercased if no mapping found.
    """
    if not raw:
        return raw
    key = raw.lower().strip().replace(" ", "-").replace("_", "-")
    return SLUG_CANONICAL.get(key, key)


def display_name(raw: str) -> str:
    """Return the human-readable display name for a sport slug."""
    slug = canonical_slug(raw)
    return SLUG_TO_NAME.get(slug, slug.replace("-", " ").title())


def is_football(raw: str) -> bool:
    """Return True for any football/soccer variant."""
    return canonical_slug(raw) == "football"