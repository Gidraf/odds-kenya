import enum
from datetime import datetime, timezone

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _utcnow_naive() -> datetime:
    """Some DB drivers prefer naive UTC; use consistently."""
    return datetime.now(timezone.utc)

class MatchStatus(str, enum.Enum):
    PRE_MATCH  = "PRE_MATCH"
    LIVE       = "LIVE"
    FINISHED   = "FINISHED"
    CANCELLED  = "CANCELLED"
    POSTPONED  = "POSTPONED"
    SUSPENDED  = "SUSPENDED"

class OpportunityStatus(str, enum.Enum):
    OPEN   = "OPEN"    # still available right now
    CLOSED = "CLOSED"  # price moved, no longer profitable
    EXPIRED = "EXPIRED" # match kicked off before anyone acted

# ─────────────────────────────────────────────────────────────────────────────
# PARSER ROW CONTRACT
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_PARSER_KEYS = frozenset({
    "parent_match_id",
    "home_team",
    "away_team",
    "start_time",
    "sport",
    "competition",
    "market",
    "selection",
    "price",
    "specifier",
})

NULLABLE_KEYS = frozenset({"specifier", "start_time", "sport", "competition"})

def validate_parser_row(row: dict) -> None:
    if not isinstance(row, dict):
        raise ValueError(
            f"Parser must return a list of dicts, got a row of type {type(row).__name__!r}."
        )
    missing = REQUIRED_PARSER_KEYS - row.keys()
    if missing:
        raise ValueError(
            f"Parser row missing required keys: {sorted(missing)}.\n"
            f"Row keys received:  {sorted(row.keys())}\n"
            f"All required keys:  {sorted(REQUIRED_PARSER_KEYS)}"
        )
    for key in REQUIRED_PARSER_KEYS - NULLABLE_KEYS:
        if row.get(key) is None:
            raise ValueError(
                f"Parser row has None for required non-nullable key {key!r}.\n"
                f"Row: {row}"
            )
    raw_price = row.get("price")
    try:
        p = float(raw_price)
    except (TypeError, ValueError):
        raise ValueError(
            f"Parser row price={raw_price!r} is not a valid float.\n"
            f"Row: {row}"
        )
    if p <= 1.0:
        raise ValueError(
            f"Parser row price={p} is <= 1.0 (invalid decimal odds).\n"
            f"Row: {row}"
        )