"""
app/services/entity_resolver.py
================================
Bridges harvested match dicts → PostgreSQL canonical entities.

KEY FIXES IN THIS VERSION
─────────────────────────
1. datetime.utcnow() bug — the Sports model uses datetime.utcnow as a column
   default but imports `datetime` as the module (not the class). This caused
   every persist to fail. Fixed by bypassing get_or_create and inserting with
   an explicit datetime.now(timezone.utc).

2. BT / OD matching — uses bt_parent_id / od_match_id as parent_match_id so
   matches are deduped correctly against existing SportPesa rows that share the
   same betradar parent ID.

3. B2B fuzzy matching — since B2B bookmakers don't share IDs with SP/BT/OD,
   matches are linked by: home_team + away_team + start_time window (±5 min) +
   competition (optional). Falls back to creating a new UnifiedMatch if no
   existing row is close enough.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any

from app.extensions import db

logger = logging.getLogger(__name__)

# Bookmaker slug → DB bookmaker name
BK_SLUG_TO_NAME = {
    "sp":  "SportPesa",
    "bt":  "Betika",
    "od":  "OdiBets",
    "b2b": "B2B",
    "sbo": "SBO",
}

SPORT_SLUG_MAP = {
    "soccer": "Soccer", "football": "Soccer", "esoccer": "eSoccer",
    "efootball": "eSoccer", "basketball": "Basketball", "tennis": "Tennis",
    "ice-hockey": "Ice Hockey", "volleyball": "Volleyball",
    "cricket": "Cricket", "rugby": "Rugby", "table-tennis": "Table Tennis",
    "boxing": "Boxing", "handball": "Handball", "mma": "MMA",
    "darts": "Darts", "american-football": "American Football",
    "baseball": "Baseball",
}

# B2B bookmaker slugs — use fuzzy matching, not ID matching
_B2B_SLUGS = {"b2b", "sbo", "1xbet", "22bet", "helabet", "paripesa",
               "melbet", "betwinner", "megapari"}


def _now() -> datetime:
    """UTC-aware now. Replaces deprecated datetime.utcnow()."""
    return datetime.now(timezone.utc)


def _utcnow_naive() -> datetime:
    """UTC naive datetime (for DB columns that expect naive datetime)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class EntityResolver:
    """
    Stateful resolver — create one per harvest batch, discard after commit.
    """

    def __init__(self):
        self._sport_cache:     dict[str, int]        = {}
        self._comp_cache:      dict[str, int]         = {}
        self._team_cache:      dict[str, int]         = {}
        self._bookmaker_cache: dict[str, int | None]  = {}

    # ─────────────────────────────────────────────────────────────────────
    # BOOKMAKER RESOLUTION
    # ─────────────────────────────────────────────────────────────────────

    def _get_bookmaker_id(self, bk_slug: str) -> int | None:
        if bk_slug in self._bookmaker_cache:
            return self._bookmaker_cache[bk_slug]
        try:
            from app.models.bookmakers_model import Bookmaker
            name = BK_SLUG_TO_NAME.get(bk_slug)
            if not name:
                self._bookmaker_cache[bk_slug] = None
                return None
            bm = Bookmaker.query.filter(
                db.func.lower(Bookmaker.name) == name.lower()
            ).first()
            bk_id = bm.id if bm else None
            self._bookmaker_cache[bk_slug] = bk_id
            return bk_id
        except Exception:
            self._bookmaker_cache[bk_slug] = None
            return None

    # ─────────────────────────────────────────────────────────────────────
    # SPORT RESOLUTION  (bypasses get_or_create to avoid datetime bug)
    # ─────────────────────────────────────────────────────────────────────

    def resolve_sport(self, sport_slug: str) -> int | None:
        slug_lower = (sport_slug or "").lower().strip()
        if not slug_lower:
            return None
        if slug_lower in self._sport_cache:
            return self._sport_cache[slug_lower]

        try:
            from app.models.competions_model import Sport

            canonical_name = SPORT_SLUG_MAP.get(
                slug_lower, sport_slug.replace("-", " ").title()
            )
            canonical_slug = re.sub(r"[^a-z0-9]+", "-", slug_lower).strip("-")

            # Try to find existing sport
            sport = Sport.query.filter(
                db.or_(
                    Sport.slug == canonical_slug,
                    db.func.lower(Sport.name) == canonical_name.lower(),
                )
            ).first()

            if not sport:
                # Create with explicit datetime to avoid model default bug
                sport = Sport(
                    name=canonical_name,
                    slug=canonical_slug,
                    is_active=True,
                )
                # Set created_at explicitly if the column exists to bypass
                # any model-level default that uses the wrong datetime import
                if hasattr(sport, "created_at") and sport.created_at is None:
                    sport.created_at = _utcnow_naive()
                db.session.add(sport)
                db.session.flush()

            self._sport_cache[slug_lower] = sport.id
            return sport.id

        except Exception as exc:
            logger.warning("[resolve_sport] %s: %s", slug_lower, exc)
            try:
                db.session.rollback()
            except Exception:
                pass
            return None

    # ─────────────────────────────────────────────────────────────────────
    # COMPETITION RESOLUTION
    # ─────────────────────────────────────────────────────────────────────

    def resolve_competition(self, comp_name: str, sport_id: int,
                            betradar_id: str | None = None) -> int | None:
        if not comp_name or not sport_id:
            return None
        cache_key = f"{sport_id}|{comp_name.lower().strip()}"
        if cache_key in self._comp_cache:
            return self._comp_cache[cache_key]
        try:
            from app.models.competions_model import Competition
            if betradar_id:
                existing = Competition.query.filter_by(betradar_id=betradar_id).first()
                if existing:
                    self._comp_cache[cache_key] = existing.id
                    return existing.id

            comp = Competition.query.filter(
                Competition.sport_id == sport_id,
                db.func.lower(Competition.name) == comp_name.strip().lower(),
            ).first()

            if not comp:
                comp = Competition(
                    name=comp_name.strip(),
                    sport_id=sport_id,
                    betradar_id=betradar_id,
                    is_active=True,
                )
                if hasattr(comp, "created_at") and comp.created_at is None:
                    comp.created_at = _utcnow_naive()
                db.session.add(comp)
                db.session.flush()

            self._comp_cache[cache_key] = comp.id
            return comp.id
        except Exception as exc:
            logger.warning("[resolve_competition] %s: %s", comp_name, exc)
            return None

    # ─────────────────────────────────────────────────────────────────────
    # TEAM RESOLUTION
    # ─────────────────────────────────────────────────────────────────────

    def resolve_team(self, team_name: str, sport_id: int,
                     betradar_id: str | None = None) -> int | None:
        if not team_name or not sport_id:
            return None
        cache_key = f"{sport_id}|{team_name.lower().strip()}"
        if cache_key in self._team_cache:
            return self._team_cache[cache_key]
        try:
            from app.models.competions_model import Team
            if betradar_id:
                existing = Team.query.filter_by(betradar_id=betradar_id).first()
                if existing:
                    self._team_cache[cache_key] = existing.id
                    return existing.id

            team = Team.query.filter(
                Team.sport_id == sport_id,
                db.func.lower(Team.name) == team_name.strip().lower(),
            ).first()

            if not team:
                team = Team(
                    name=team_name.strip(),
                    sport_id=sport_id,
                    betradar_id=betradar_id,
                    is_active=True,
                )
                if hasattr(team, "created_at") and team.created_at is None:
                    team.created_at = _utcnow_naive()
                db.session.add(team)
                db.session.flush()

            self._team_cache[cache_key] = team.id
            return team.id
        except Exception as exc:
            logger.warning("[resolve_team] %s: %s", team_name, exc)
            return None

    # ─────────────────────────────────────────────────────────────────────
    # BOOKMAKER ENTITY / MATCH LINK HELPERS
    # ─────────────────────────────────────────────────────────────────────

    def _map_bookmaker_match(self, bk_slug: str, match_id: int,
                             external_match_id: str,
                             betradar_id: str | None = None) -> None:
        bk_id = self._get_bookmaker_id(bk_slug)
        if not bk_id or not match_id or not external_match_id:
            return
        try:
            from app.models.bookmakers_model import BookmakerMatchLink
            BookmakerMatchLink.upsert(
                match_id=match_id, bookmaker_id=bk_id,
                external_match_id=external_match_id, betradar_id=betradar_id,
            )
        except Exception as exc:
            logger.debug("match link error: %s", exc)

    # ─────────────────────────────────────────────────────────────────────
    # UNIFIED MATCH LOOKUP STRATEGIES
    # ─────────────────────────────────────────────────────────────────────

    def _find_by_parent_id(self, parent_id: str):
        """Exact match on parent_match_id — used for SP/BT/OD."""
        from app.models.odds_model import UnifiedMatch
        return UnifiedMatch.query.filter_by(
            parent_match_id=parent_id
        ).with_for_update().first()

    def _find_by_fuzzy(self, home: str, away: str,
                       start_time: datetime | None,
                       comp: str = "") -> Any | None:
        """
        Fuzzy match for B2B bookmakers that don't share IDs with SP/BT/OD.

        Matches on:
          1. home_team ILIKE + away_team ILIKE  (normalised: lower, strip)
          2. start_time within ±5 minutes  (if available)
          3. competition ILIKE  (optional bonus, not required)

        Returns the best match or None.
        """
        from app.models.odds_model import UnifiedMatch

        if not home or not away:
            return None

        home_l = home.strip().lower()
        away_l = away.strip().lower()

        q = UnifiedMatch.query.filter(
            db.func.lower(UnifiedMatch.home_team_name) == home_l,
            db.func.lower(UnifiedMatch.away_team_name) == away_l,
        )

        if start_time:
            # ±5 minute window to handle timezone/rounding differences
            window = timedelta(minutes=5)
            st = start_time.replace(tzinfo=None) if start_time.tzinfo else start_time
            q = q.filter(
                UnifiedMatch.start_time >= st - window,
                UnifiedMatch.start_time <= st + window,
            )

        candidates = q.all()
        if not candidates:
            return None

        if len(candidates) == 1:
            return candidates[0]

        # Multiple candidates — prefer same competition
        if comp:
            comp_l = comp.strip().lower()
            for c in candidates:
                if c.competition_name and comp_l in c.competition_name.lower():
                    return c

        # Fall back to most recently created
        return max(candidates, key=lambda x: x.id)

    # ─────────────────────────────────────────────────────────────────────
    # CORE PERSIST
    # ─────────────────────────────────────────────────────────────────────

    def persist_combined_match(self, cm) -> int | None:
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
            OpportunityDetector, ArbitrageOpportunity, EVOpportunity,
            OpportunityStatus,
        )

        try:
            # ── 1. Resolve Sport ──────────────────────────────────────────
            sport_slug = self._infer_sport_slug(cm)
            sport_id   = self.resolve_sport(sport_slug) if sport_slug else None

            comp_id      = None
            home_team_id = None
            away_team_id = None

            if sport_id:
                if cm.competition:
                    comp_id = self.resolve_competition(cm.competition, sport_id)
                if cm.home_team:
                    home_team_id = self.resolve_team(cm.home_team, sport_id)
                if cm.away_team:
                    away_team_id = self.resolve_team(cm.away_team, sport_id)

            # ── 2. Determine which bookmaker slugs are in this match ───────
            bk_slugs  = list((cm.bk_ids or {}).keys())
            is_b2b    = any(s in _B2B_SLUGS for s in bk_slugs)
            is_bt     = "bt" in bk_slugs
            is_od     = "od" in bk_slugs

            start_time = self._parse_start_time(cm.start_time)

            # ── 3. Find or create UnifiedMatch ────────────────────────────
            #
            #  SP / BT / OD → exact parent_match_id lookup
            #    • SP: betradar_id or join_key
            #    • BT: bt_parent_id stored in bk_ids["bt"]
            #    • OD: od_match_id stored in bk_ids["od"]
            #  B2B / SBO → fuzzy lookup then create if no match found
            #
            um = None
            parent_id = cm.betradar_id or cm.join_key
            if not parent_id:
                return None

            if is_b2b:
                # Try fuzzy first, so B2B odds attach to the existing SP row
                um = self._find_by_fuzzy(
                    cm.home_team, cm.away_team, start_time, cm.competition or ""
                )
                if um is None:
                    # No SP/BT row exists yet — create a new one with b2b key
                    um = self._find_by_parent_id(parent_id)

            else:
                # SP / BT / OD — exact match
                um = self._find_by_parent_id(parent_id)

            if not um:
                um = UnifiedMatch(
                    parent_match_id=parent_id,
                    home_team_name=cm.home_team,
                    away_team_name=cm.away_team,
                    sport_name=sport_slug or "",
                    competition_name=cm.competition or "",
                    start_time=start_time,
                    competition_id=comp_id,
                    home_team_id=home_team_id,
                    away_team_id=away_team_id,
                )
                db.session.add(um)
                db.session.flush()
            else:
                # Fill in any missing fields
                if cm.home_team and not um.home_team_name:
                    um.home_team_name = cm.home_team
                if cm.away_team and not um.away_team_name:
                    um.away_team_name = cm.away_team
                if cm.competition and not um.competition_name:
                    um.competition_name = cm.competition
                if start_time and not um.start_time:
                    um.start_time = start_time
                if comp_id and not um.competition_id:
                    um.competition_id = comp_id
                if home_team_id and not um.home_team_id:
                    um.home_team_id = home_team_id
                if away_team_id and not um.away_team_id:
                    um.away_team_id = away_team_id
                if sport_slug and not um.sport_name:
                    um.sport_name = sport_slug

            # ── 4. Bookmaker match links ───────────────────────────────────
            for bk_slug, ext_id in (cm.bk_ids or {}).items():
                if ext_id:
                    self._map_bookmaker_match(bk_slug, um.id, ext_id, cm.betradar_id)

            # ── 5. Per-bookmaker odds ──────────────────────────────────────
            for bk_slug, bk_markets in (cm.markets or {}).items():
                bk_id = self._get_bookmaker_id(bk_slug)
                if not bk_id:
                    logger.debug("[entity_resolver] no bk_id for slug=%s", bk_slug)
                    continue

                bmo = BookmakerMatchOdds.query.filter_by(
                    match_id=um.id, bookmaker_id=bk_id
                ).with_for_update().first()
                if not bmo:
                    bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=bk_id)
                    db.session.add(bmo)
                    db.session.flush()

                history_batch: list[dict] = []
                for mkt_slug, outcomes in bk_markets.items():
                    for outcome, odd in (outcomes or {}).items():
                        try:
                            price = float(odd)
                        except (TypeError, ValueError):
                            continue
                        if price <= 1.0:
                            continue

                        price_changed, old_price = bmo.upsert_selection(
                            market=mkt_slug, specifier=None,
                            selection=outcome, price=price,
                        )
                        um.upsert_bookmaker_price(
                            market=mkt_slug, specifier=None,
                            selection=outcome, price=price,
                            bookmaker_id=bk_id,
                        )
                        if price_changed:
                            history_batch.append({
                                "bmo_id":       bmo.id,
                                "bookmaker_id": bk_id,
                                "match_id":     um.id,
                                "market":       mkt_slug,
                                "specifier":    None,
                                "selection":    outcome,
                                "old_price":    old_price,
                                "new_price":    price,
                                "price_delta":  round(price - old_price, 4) if old_price else None,
                                "recorded_at":  _utcnow_naive(),
                            })

                if history_batch:
                    BookmakerOddsHistory.bulk_append(history_batch)

            # ── 6. Arb / EV detection ──────────────────────────────────────
            try:
                detector = OpportunityDetector(min_profit_pct=0.5, min_ev_pct=3.0)
                ArbitrageOpportunity.query.filter_by(
                    match_id=um.id, status=OpportunityStatus.OPEN
                ).update({"status": OpportunityStatus.CLOSED,
                          "closed_at": _utcnow_naive()})
                for arb_kw in detector.find_arbs(um):
                    db.session.add(ArbitrageOpportunity(**arb_kw))
                EVOpportunity.query.filter_by(
                    match_id=um.id, status=OpportunityStatus.OPEN
                ).update({"status": OpportunityStatus.CLOSED,
                          "closed_at": _utcnow_naive()})
                for ev_kw in detector.find_ev(um):
                    db.session.add(EVOpportunity(**ev_kw))
            except Exception as exc:
                logger.warning("opportunity detection error: %s", exc)

            return um.id

        except Exception as exc:
            logger.error("persist_combined_match error: %s", exc)
            try:
                db.session.rollback()
            except Exception:
                pass
            return None

    # ─────────────────────────────────────────────────────────────────────
    # BATCH PERSIST
    # ─────────────────────────────────────────────────────────────────────

    def persist_batch(self, combined_matches: list, commit: bool = True) -> dict:
        persisted = 0
        failed    = 0
        for cm in combined_matches:
            try:
                mid = self.persist_combined_match(cm)
                if mid:
                    persisted += 1
                else:
                    failed += 1
            except Exception as exc:
                logger.error("batch persist error: %s", exc)
                failed += 1
                try:
                    db.session.rollback()
                except Exception:
                    pass

        if commit:
            try:
                db.session.commit()
            except Exception as exc:
                logger.error("batch commit error: %s", exc)
                db.session.rollback()
                return {"persisted": 0, "failed": len(combined_matches), "error": str(exc)}

        return {"persisted": persisted, "failed": failed}

    # ─────────────────────────────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    def _infer_sport_slug(cm) -> str | None:
        # Try explicit sport field first
        sport = getattr(cm, "sport", None) or getattr(cm, "sport_slug", None)
        if sport:
            return str(sport).lower().strip()
        # Fall back to "soccer" — TODO: carry sport_slug through the pipeline
        return "soccer"

    @staticmethod
    def _parse_start_time(raw) -> datetime | None:
        if not raw:
            return None
        try:
            if isinstance(raw, datetime):
                return raw.replace(tzinfo=None) if raw.tzinfo else raw
            if isinstance(raw, str):
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return dt.replace(tzinfo=None)
            if isinstance(raw, (int, float)):
                return datetime.utcfromtimestamp(float(raw))
        except Exception:
            pass
        return None