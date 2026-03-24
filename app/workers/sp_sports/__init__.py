"""
app/workers/sp_sports/__init__.py
==================================
Exports every SportConfig and a registry dict keyed by sport slug.

Usage
─────
  from app.workers.sp_sports import SPORT_CONFIGS, get_config

  cfg = get_config("tennis")          # → SportConfig(sport_id=5, …)
  cfg = get_config("soccer")          # → SportConfig(sport_id=1, …)
"""

from app.workers.sp_harvester_base import SportConfig

# ══════════════════════════════════════════════════════════════════════════════
# SPORT CONFIGS
# ══════════════════════════════════════════════════════════════════════════════

FOOTBALL = SportConfig(
    slugs        = ("soccer", "football"),
    sport_id     = 1,
    days_default = 3,
    max_default  = 150,
    is_esoccer   = False,
    market_ids   = (
        "10,1,"
        "46,47,"
        "43,29,386,"
        "52,18,"
        "353,352,"
        "208,"
        "258,202,"
        "332,"
        "51,53,"
        "55,"
        "45,"
        "41,"
        "207,"
        "42,60,"
        "15,54,68,"
        "44,"
        "328,"
        "203,"
        "162,166,"
        "136,139"
    ).replace("\n", "").replace(" ", ""),
)

ESOCCER = SportConfig(
    slugs        = ("esoccer", "efootball", "e-football", "virtual-football"),
    sport_id     = 126,
    days_default = 1,
    max_default  = 60,
    is_esoccer   = True,
    market_ids   = (
        "381,1,10,"
        "56,52,"
        "46,47,"
        "43,"
        "51,"
        "45,"
        "208,258,202"
    ).replace("\n", "").replace(" ", ""),
)

BASKETBALL = SportConfig(
    slugs        = ("basketball",),
    sport_id     = 2,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "382,"
        "51,"
        "52,"
        "353,"
        "352,"
        "45,"
        "222,"
        "42,"
        "53,"
        "54,"
        "224,"
        "362,"
        "363,"
        "364,"
        "365,"
        "366,"
        "367,"
        "368,"
        "369"
    ).replace("\n", "").replace(" ", ""),
)

TENNIS = SportConfig(
    slugs        = ("tennis",),
    sport_id     = 5,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "382,"
        "204,231,"
        "51,"
        "226,"
        "233,"
        "439,"
        "45,"
        "339,340,"
        "433,"
        "353,352"
    ).replace("\n", "").replace(" ", ""),
)

ICE_HOCKEY = SportConfig(
    slugs        = ("ice-hockey", "icehockey"),
    sport_id     = 4,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "1,10,"
        "382,"
        "52,"
        "51,"
        "45,"
        "46,"
        "353,352,"
        "208,"
        "43,"
        "210,378"
    ).replace("\n", "").replace(" ", ""),
)

VOLLEYBALL = SportConfig(
    slugs        = ("volleyball",),
    sport_id     = 23,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "382,"
        "204,"
        "20,"
        "51,"
        "226,"
        "233,"
        "45,"
        "353,352"
    ).replace("\n", "").replace(" ", ""),
)

CRICKET = SportConfig(
    slugs        = ("cricket",),
    sport_id     = 21,
    days_default = 5,
    max_default  = 50,
    market_ids   = (
        "382,"
        "1,"
        "51,"
        "52,"
        "353,352"
    ).replace("\n", "").replace(" ", ""),
)

RUGBY = SportConfig(
    slugs        = ("rugby", "rugby-league", "rugby-union"),
    sport_id     = 12,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "10,1,"
        "382,"
        "46,"
        "42,"
        "51,"
        "53,"
        "60,52,"
        "353,352,"
        "45,"
        "379,"
        "207,"
        "44"
    ).replace("\n", "").replace(" ", ""),
)

HANDBALL = SportConfig(
    slugs        = ("handball",),
    sport_id     = 6,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "1,10,"
        "382,"
        "52,"
        "51,"
        "45,"
        "46,47,"
        "353,352,"
        "208,"
        "43,"
        "42,"
        "207"
    ).replace("\n", "").replace(" ", ""),
)

TABLE_TENNIS = SportConfig(
    slugs        = ("table-tennis", "tabletennis"),
    sport_id     = 16,
    days_default = 3,
    max_default  = 150,
    market_ids   = (
        "382,"
        "51,"
        "226,"
        "45,"
        "233,"
        "340"
    ).replace("\n", "").replace(" ", ""),
)

MMA = SportConfig(
    slugs        = ("mma", "ufc"),
    sport_id     = 117,
    days_default = 7,
    max_default  = 30,
    market_ids   = (
        "382,"
        "20,"
        "51,"
        "52"
    ).replace("\n", "").replace(" ", ""),
)

BOXING = SportConfig(
    slugs        = ("boxing",),
    sport_id     = 10,
    days_default = 7,
    max_default  = 30,
    market_ids   = (
        "382,"
        "51,"
        "52"
    ).replace("\n", "").replace(" ", ""),
)

DARTS = SportConfig(
    slugs        = ("darts",),
    sport_id     = 49,
    days_default = 3,
    max_default  = 100,
    market_ids   = (
        "382,"
        "226,"
        "45,"
        "51"
    ).replace("\n", "").replace(" ", ""),
)

AMERICAN_FOOTBALL = SportConfig(
    slugs        = ("american-football", "americanfootball", "nfl"),
    sport_id     = 15,
    days_default = 7,
    max_default  = 50,
    market_ids   = (
        "382,"
        "51,"
        "52,"
        "45,"
        "353,"
        "352"
    ).replace("\n", "").replace(" ", ""),
)

BASEBALL = SportConfig(
    slugs        = ("baseball",),
    sport_id     = 3,
    days_default = 3,
    max_default  = 100,
    market_ids   = (
        "382,"
        "51,"
        "52,"
        "45,"
        "353,"
        "352"
    ).replace("\n", "").replace(" ", ""),
)


# ══════════════════════════════════════════════════════════════════════════════
# REGISTRY  — slug → config
# ══════════════════════════════════════════════════════════════════════════════

_ALL_CONFIGS = [
    FOOTBALL, ESOCCER, BASKETBALL, TENNIS, ICE_HOCKEY,
    VOLLEYBALL, CRICKET, RUGBY, HANDBALL, TABLE_TENNIS,
    MMA, BOXING, DARTS, AMERICAN_FOOTBALL, BASEBALL,
]

SPORT_CONFIGS: dict[str, SportConfig] = {}
for _cfg in _ALL_CONFIGS:
    for _slug in _cfg.slugs:
        SPORT_CONFIGS[_slug.lower()] = _cfg

# sport_id → config (first slug wins for each sport_id)
SPORT_ID_CONFIGS: dict[int, SportConfig] = {}
for _cfg in _ALL_CONFIGS:
    if _cfg.sport_id not in SPORT_ID_CONFIGS:
        SPORT_ID_CONFIGS[_cfg.sport_id] = _cfg


def get_config(sport_slug: str) -> SportConfig | None:
    """Return SportConfig for a given slug, case-insensitive."""
    return SPORT_CONFIGS.get(sport_slug.lower().replace(" ", "-"))


def get_config_by_id(sport_id: int) -> SportConfig | None:
    """Return SportConfig for a numeric sport ID."""
    return SPORT_ID_CONFIGS.get(sport_id)


__all__ = [
    "FOOTBALL", "ESOCCER", "BASKETBALL", "TENNIS", "ICE_HOCKEY",
    "VOLLEYBALL", "CRICKET", "RUGBY", "HANDBALL", "TABLE_TENNIS",
    "MMA", "BOXING", "DARTS", "AMERICAN_FOOTBALL", "BASEBALL",
    "SPORT_CONFIGS", "SPORT_ID_CONFIGS",
    "get_config", "get_config_by_id",
]