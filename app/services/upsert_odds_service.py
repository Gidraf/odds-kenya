"""
Odds Upsert Service  —  app/services/odds_upsert.py
=====================================================
The single function agents call after running a parser.

Flow
────
  parser rows (flat list)
       │
       ▼
  _upsert_odds_rows(rows, bookmaker_id)
       │
       ├─► UnifiedMatch          (one row per parent_match_id — aggregated view)
       │       └─ markets_json   (best price + all bookmakers keyed by ID)
       │
       ├─► BookmakerMatchOdds    (one row per match × bookmaker — independent)
       │       └─ markets_json   (only this bookmaker's prices)
       │
       └─► BookmakerOddsHistory  (append-only — only when price changed)

Independent update path (WebSocket / SSE odds change)
──────────────────────────────────────────────────────
  update_single_price(
      parent_match_id, bookmaker_id,
      market, specifier, selection, new_price
  )
  → updates BookmakerMatchOdds + UnifiedMatch in one commit
  → appends BookmakerOddsHistory row

Both functions are transaction-safe and can be called concurrently for
different bookmakers on the same match without data loss.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from app.extensions import db
from app.models.odds_model import (
    UnifiedMatch,
    BookmakerMatchOdds,
    BookmakerOddsHistory,
    validate_parser_row,
    REQUIRED_PARSER_KEYS,
)

log = logging.getLogger(__name__)


# ─── Main agent entry point ───────────────────────────────────────────────────

def upsert_odds_rows(
    rows: list[dict],
    bookmaker_id: int,
    *,
    commit: bool = True,
) -> dict[str, int]:
    """
    Persist a list of parser output rows for one bookmaker.

    Each row MUST satisfy the parser row contract (validate_parser_row).
    Rows that fail validation are skipped and counted in `skipped`.

    Parameters
    ──────────
    rows         : flat list of dicts from parse_data() / parse_match_list()
    bookmaker_id : Bookmakers.id — who these prices belong to
    commit       : set False when caller manages the transaction

    Returns
    ───────
    {
        "matches_created":  int,   # new UnifiedMatch rows
        "matches_updated":  int,   # existing UnifiedMatch rows touched
        "odds_new":         int,   # brand-new (selection first seen)
        "odds_changed":     int,   # price moved
        "odds_unchanged":   int,   # price same — no history written
        "skipped":          int,   # rows that failed validation
    }
    """
    stats = dict(
        matches_created=0, matches_updated=0,
        odds_new=0, odds_changed=0, odds_unchanged=0, skipped=0,
    )

    # Group rows by parent_match_id so we make one DB look-up per match
    by_match: dict[str, list[dict]] = {}
    for row in rows:
        try:
            validate_parser_row(row)
        except ValueError as exc:
            log.debug("Skipping invalid row: %s", exc)
            stats["skipped"] += 1
            continue
        mid = str(row["parent_match_id"])
        by_match.setdefault(mid, []).append(row)

    for parent_match_id, match_rows in by_match.items():
        # Use first row for match-level metadata (they all share the same match)
        meta = match_rows[0]

        # ── 1. Get or create UnifiedMatch ─────────────────────────────────
        unified = UnifiedMatch.query.filter_by(
            parent_match_id=parent_match_id
        ).first()

        if unified is None:
            unified = UnifiedMatch(
                parent_match_id  = parent_match_id,
                home_team_name   = meta.get("home_team"),
                away_team_name   = meta.get("away_team"),
                sport_name       = meta.get("sport"),
                competition_name = meta.get("competition"),
                start_time       = _parse_dt(meta.get("start_time")),
                markets_json     = {},
            )
            db.session.add(unified)
            db.session.flush()   # get unified.id before BMO FK
            stats["matches_created"] += 1
        else:
            # Refresh denormalised fields if parser provides them
            if meta.get("home_team"):
                unified.home_team_name = meta["home_team"]
            if meta.get("away_team"):
                unified.away_team_name = meta["away_team"]
            if meta.get("sport"):
                unified.sport_name = meta["sport"]
            if meta.get("competition"):
                unified.competition_name = meta["competition"]
            if meta.get("start_time"):
                unified.start_time = _parse_dt(meta["start_time"])
            stats["matches_updated"] += 1

        # ── 2. Get or create BookmakerMatchOdds ───────────────────────────
        bmo = BookmakerMatchOdds.query.filter_by(
            match_id    = unified.id,
            bookmaker_id = bookmaker_id,
        ).first()

        if bmo is None:
            bmo = BookmakerMatchOdds(
                match_id     = unified.id,
                bookmaker_id = bookmaker_id,
                markets_json = {},
                is_active    = True,
            )
            db.session.add(bmo)
            db.session.flush()

        # ── 3. Upsert each selection ──────────────────────────────────────
        for row in match_rows:
            market    = row["market"]
            selection = row["selection"]
            price     = float(row["price"])
            specifier = row.get("specifier")

            changed, old_price = bmo.upsert_selection(market, specifier, selection, price)

            # Mirror into the unified aggregated view
            unified.upsert_bookmaker_price(market, specifier, selection, price, bookmaker_id)

            if not changed:
                stats["odds_unchanged"] += 1
                continue

            # ── 4. Append history row on first-seen or price change ───────
            db.session.add(BookmakerOddsHistory(
                bmo_id       = bmo.id,
                bookmaker_id = bookmaker_id,
                match_id     = unified.id,
                market       = market,
                specifier    = str(specifier) if specifier is not None else None,
                selection    = selection,
                old_price    = old_price,
                new_price    = price,
                price_delta  = round(price - old_price, 6) if old_price is not None else None,
            ))

            if old_price is None:
                stats["odds_new"] += 1
            else:
                stats["odds_changed"] += 1

    if commit:
        db.session.commit()

    return stats


# ─── Independent single-price update (WebSocket / SSE) ───────────────────────

def update_single_price(
    parent_match_id: str | int,
    bookmaker_id:    int,
    market:          str,
    specifier:       str | None,
    selection:       str,
    new_price:       float,
    *,
    commit: bool = True,
) -> bool:
    """
    Update ONE selection price independently — called from a WebSocket listener
    or any real-time feed without re-parsing the whole match.

    Returns True if the price changed (or was new), False if identical.

    This is deliberately minimal: no match metadata update, no batch grouping.
    Use upsert_odds_rows() for full parser output.
    """
    mid = str(parent_match_id)

    unified = UnifiedMatch.query.filter_by(parent_match_id=mid).first()
    if unified is None:
        log.warning(
            "update_single_price: parent_match_id=%s not found — run full upsert first", mid
        )
        return False

    bmo = BookmakerMatchOdds.query.filter_by(
        match_id=unified.id, bookmaker_id=bookmaker_id
    ).first()
    if bmo is None:
        log.warning(
            "update_single_price: no BMO for match=%s bookmaker=%s", mid, bookmaker_id
        )
        return False

    changed, old_price = bmo.upsert_selection(market, specifier, selection, new_price)

    if not changed:
        return False

    # Update aggregated view
    unified.upsert_bookmaker_price(market, specifier, selection, new_price, bookmaker_id)

    # History
    db.session.add(BookmakerOddsHistory(
        bmo_id       = bmo.id,
        bookmaker_id = bookmaker_id,
        match_id     = unified.id,
        market       = market,
        specifier    = str(specifier) if specifier is not None else None,
        selection    = selection,
        old_price    = old_price,
        new_price    = new_price,
        price_delta  = round(new_price - old_price, 6) if old_price is not None else None,
    ))

    if commit:
        db.session.commit()

    return True


# ─── Retrieval helper (used by API read layer) ────────────────────────────────

def get_match_odds_grouped(parent_match_id: str | int) -> dict | None:
    """
    Return a fully-grouped odds payload for one match.

    Shape:
    {
      "parent_match_id": "12345",
      "home_team":  "Arsenal",
      "away_team":  "Chelsea",
      "start_time": "2025-06-01T15:00:00+00:00",
      "sport":      "Football",
      "competition":"Premier League",
      "markets": {
        "1X2": {
          "null": {
            "Home": {
              "best_price":        1.95,
              "best_bookmaker_id": 3,
              "bookmakers": {"3": 1.95, "7": 1.90},
              "updated_at": "2025-06-01T14:58:00+00:00"
            },
            "Draw": { ... },
            "Away": { ... }
          }
        },
        "Over/Under": {
          "2.5": { "Over": { ... }, "Under": { ... } }
        }
      }
    }
    """
    unified = UnifiedMatch.query.filter_by(
        parent_match_id=str(parent_match_id)
    ).first()

    if unified is None:
        return None

    return unified.to_dict(include_bookmaker_odds=False)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        from dateutil import parser as _dp
        return _dp.parse(str(value))
    except Exception:
        return None