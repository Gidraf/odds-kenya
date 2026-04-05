"""
app/services/entity_resolver.py
================================
Bridges the in-memory CombinedMatch merger → PostgreSQL canonical entities.

Responsibilities
────────────────
  1. Resolve or create canonical Sport / Competition / Team from harvested names.
  2. Map each bookmaker's external IDs to our canonical IDs.
  3. Persist UnifiedMatch with proper FK links (not just denormalized strings).
  4. Store per-bookmaker match links and odds.
  5. Trigger arb/EV detection and persist opportunities to DB.
  6. Cache resolved entities in Redis for fast repeated lookups.

SportPesa names are treated as canonical (primary). When a BT/OD name differs,
we still create the entity using the SP name if available, and store the BT/OD
name as an alias in BookmakerEntityMap.

Usage
─────
    from app.services.entity_resolver import EntityResolver

    resolver = EntityResolver()

    # After combined_merger produces CombinedMatch list:
    for cm in combined_matches:
        resolver.persist_combined_match(cm)
    db.session.commit()

    # Or bulk:
    resolver.persist_batch(combined_matches)
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from app.extensions import db

logger = logging.getLogger(__name__)

# Bookmaker slug → DB bookmaker name (for Bookmaker.query lookups)
BK_SLUG_TO_NAME = {
    "sp": "SportPesa",
    "bt": "Betika",
    "od": "OdiBets",
}

# Sport slug → canonical sport name
SPORT_SLUG_MAP = {
    "soccer": "Soccer", "football": "Soccer", "esoccer": "eSoccer",
    "efootball": "eSoccer", "basketball": "Basketball", "tennis": "Tennis",
    "ice-hockey": "Ice Hockey", "volleyball": "Volleyball",
    "cricket": "Cricket", "rugby": "Rugby", "table-tennis": "Table Tennis",
    "boxing": "Boxing", "handball": "Handball", "mma": "MMA",
    "darts": "Darts", "american-football": "American Football",
    "baseball": "Baseball",
}


class EntityResolver:
    """
    Stateful resolver that caches lookups within a session.
    Create one per harvest cycle, discard after commit.
    """

    def __init__(self):
        # In-memory caches to avoid repeated DB hits within a single batch
        self._sport_cache: dict[str, int] = {}           # slug → sport.id
        self._comp_cache: dict[str, int] = {}             # "sport_id|comp_name" → comp.id
        self._team_cache: dict[str, int] = {}             # "sport_id|team_name" → team.id
        self._bookmaker_cache: dict[str, int | None] = {} # bk_slug → bookmaker.id

    # ─────────────────────────────────────────────────────────────────────
    # BOOKMAKER RESOLUTION
    # ─────────────────────────────────────────────────────────────────────

    def _get_bookmaker_id(self, bk_slug: str) -> int | None:
        if bk_slug in self._bookmaker_cache:
            return self._bookmaker_cache[bk_slug]
        try:
            from app.models.bookmakers_model import Bookmaker
            name = BK_SLUG_TO_NAME.get(bk_slug)
            if name:
                bm = Bookmaker.query.filter(
                    db.func.lower(Bookmaker.name) == name.lower()
                ).first()
            else:
                # FIX: Fallback for slugs not in BK_SLUG_TO_NAME (e.g., 'b2b', '1xbet')
                bm = Bookmaker.query.filter_by(slug=bk_slug).first()
                
            bk_id = bm.id if bm else None
            self._bookmaker_cache[bk_slug] = bk_id
            return bk_id
        except Exception:
            self._bookmaker_cache[bk_slug] = None
            return None

    # ─────────────────────────────────────────────────────────────────────
    # SPORT RESOLUTION
    # ─────────────────────────────────────────────────────────────────────

    def resolve_sport(self, sport_slug: str) -> int | None:
        """Get or create canonical Sport from slug. Returns sport.id."""
        slug_lower = (sport_slug or "").lower().strip()
        if not slug_lower:
            return None

        if slug_lower in self._sport_cache:
            return self._sport_cache[slug_lower]

        from app.models.competions_model import Sport

        canonical_name = SPORT_SLUG_MAP.get(slug_lower, sport_slug.replace("-", " ").title())
        canonical_slug = re.sub(r"[^a-z0-9]+", "-", slug_lower).strip("-")

        sport = Sport.get_or_create(name=canonical_name, slug=canonical_slug)
        self._sport_cache[slug_lower] = sport.id
        return sport.id

    # ─────────────────────────────────────────────────────────────────────
    # COMPETITION RESOLUTION
    # ─────────────────────────────────────────────────────────────────────

    def resolve_competition(self, comp_name: str, sport_id: int,
                            betradar_id: str | None = None) -> int | None:
        """Get or create canonical Competition. Returns competition.id."""
        if not comp_name or not sport_id:
            return None

        cache_key = f"{sport_id}|{comp_name.lower().strip()}"
        if cache_key in self._comp_cache:
            return self._comp_cache[cache_key]

        from app.models.competions_model import Competition

        # Try betradar_id first (most reliable)
        if betradar_id:
            existing = Competition.query.filter_by(betradar_id=betradar_id).first()
            if existing:
                self._comp_cache[cache_key] = existing.id
                return existing.id

        comp = Competition.get_or_create(
            name=comp_name.strip(),
            sport_id=sport_id,
            betradar_id=betradar_id,
        )
        self._comp_cache[cache_key] = comp.id
        return comp.id

    # ─────────────────────────────────────────────────────────────────────
    # TEAM RESOLUTION
    # ─────────────────────────────────────────────────────────────────────

    def resolve_team(self, team_name: str, sport_id: int,
                     betradar_id: str | None = None) -> int | None:
        """Get or create canonical Team. Returns team.id."""
        if not team_name or not sport_id:
            return None

        cache_key = f"{sport_id}|{team_name.lower().strip()}"
        if cache_key in self._team_cache:
            return self._team_cache[cache_key]

        from app.models.competions_model import Team

        if betradar_id:
            existing = Team.query.filter_by(betradar_id=betradar_id).first()
            if existing:
                self._team_cache[cache_key] = existing.id
                return existing.id

        team = Team.get_or_create(
            name=team_name.strip(),
            sport_id=sport_id,
            betradar_id=betradar_id,
        )
        self._team_cache[cache_key] = team.id
        return team.id

    # ─────────────────────────────────────────────────────────────────────
    # BOOKMAKER ENTITY MAPPING
    # ─────────────────────────────────────────────────────────────────────

    def _map_bookmaker_entity(
        self,
        bk_slug: str,
        entity_type: str,
        canonical_id: int,
        external_id: str,
        external_name: str | None = None,
        betradar_id: str | None = None,
    ) -> None:
        """Record that bookmaker X calls our entity Y by external_id Z."""
        bk_id = self._get_bookmaker_id(bk_slug)
        if not bk_id or not canonical_id or not external_id:
            return
        try:
            from app.models.bookmakers_model import BookmakerEntityMap
            BookmakerEntityMap.upsert(
                bookmaker_id=bk_id,
                entity_type=entity_type,
                canonical_id=canonical_id,
                external_id=external_id,
                external_name=external_name,
                betradar_id=betradar_id,
            )
        except Exception as exc:
            logger.debug("entity map error: %s", exc)

    def _map_bookmaker_match(
        self,
        bk_slug: str,
        match_id: int,
        external_match_id: str,
        betradar_id: str | None = None,
    ) -> None:
        """Record that bookmaker X calls our match Y by external_match_id Z."""
        bk_id = self._get_bookmaker_id(bk_slug)
        if not bk_id or not match_id or not external_match_id:
            return
        try:
            from app.models.bookmakers_model import BookmakerMatchLink
            BookmakerMatchLink.upsert(
                match_id=match_id,
                bookmaker_id=bk_id,
                external_match_id=external_match_id,
                betradar_id=betradar_id,
            )
        except Exception as exc:
            logger.debug("match link error: %s", exc)

    # ─────────────────────────────────────────────────────────────────────
    # UNIFIED MATCH PERSISTENCE
    # ─────────────────────────────────────────────────────────────────────

    def persist_combined_match(self, cm) -> int | None:
        """
        Take a CombinedMatch (from combined_merger) and persist everything to DB.

        Returns the UnifiedMatch.id or None on failure.

        Steps:
          1. Resolve Sport → Competition → home Team / away Team
          2. Upsert UnifiedMatch with FKs
          3. Store bookmaker match links (bk_ids)
          4. Store per-bookmaker odds
          5. Detect and store arb / EV opportunities
        """
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
            OpportunityDetector, ArbitrageOpportunity, EVOpportunity,
            OpportunityStatus,
        )

        try:
            # FIX: Create a savepoint for this specific match to isolate failures
            with db.session.begin_nested():
                # ── 1. Resolve entities ────────────────────────────────────────
                sport_slug = self._infer_sport_slug(cm)
                sport_id   = self.resolve_sport(sport_slug) if sport_slug else None

                comp_id = None
                if sport_id and cm.competition:
                    comp_id = self.resolve_competition(cm.competition, sport_id)

                home_team_id = None
                away_team_id = None
                if sport_id:
                    if cm.home_team:
                        home_team_id = self.resolve_team(cm.home_team, sport_id)
                    if cm.away_team:
                        away_team_id = self.resolve_team(cm.away_team, sport_id)

                # ── 2. Upsert UnifiedMatch ─────────────────────────────────────
                parent_id = cm.betradar_id or cm.join_key
                if not parent_id:
                    return None

                um = UnifiedMatch.query.filter_by(
                    parent_match_id=parent_id
                ).with_for_update().first()

                start_time = self._parse_start_time(cm.start_time)

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
                    # Update denorms if better data available
                    if cm.home_team and not um.home_team_name:
                        um.home_team_name = cm.home_team
                    if cm.away_team and not um.away_team_name:
                        um.away_team_name = cm.away_team
                    if cm.competition and not um.competition_name:
                        um.competition_name = cm.competition
                    if start_time and not um.start_time:
                        um.start_time = start_time
                    # Set FKs if not yet set
                    if comp_id and not um.competition_id:
                        um.competition_id = comp_id
                    if home_team_id and not um.home_team_id:
                        um.home_team_id = home_team_id
                    if away_team_id and not um.away_team_id:
                        um.away_team_id = away_team_id
                    if sport_slug and not um.sport_name:
                        um.sport_name = sport_slug

                # ── 3. Bookmaker match links ───────────────────────────────────
                for bk_slug, ext_id in (cm.bk_ids or {}).items():
                    if ext_id:
                        self._map_bookmaker_match(
                            bk_slug, um.id, ext_id, cm.betradar_id
                        )

                # ── 4. Per-bookmaker odds ──────────────────────────────────────
                for bk_slug, bk_markets in (cm.markets or {}).items():
                    bk_id = self._get_bookmaker_id(bk_slug)
                    if not bk_id:
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
                        for outcome, odd in outcomes.items():
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
                                    "bmo_id": bmo.id,
                                    "bookmaker_id": bk_id,
                                    "match_id": um.id,
                                    "market": mkt_slug,
                                    "specifier": None,
                                    "selection": outcome,
                                    "old_price": old_price,
                                    "new_price": price,
                                    "price_delta": round(price - old_price, 4) if old_price else None,
                                    "recorded_at": datetime.utcnow(),
                                })

                    if history_batch:
                        BookmakerOddsHistory.bulk_append(history_batch)

                # ── 5. Detect arb / EV opportunities ──────────────────────────
                try:
                    detector = OpportunityDetector(min_profit_pct=0.5, min_ev_pct=3.0)

                    # Close stale open arbs for this match first
                    ArbitrageOpportunity.query.filter_by(
                        match_id=um.id, status=OpportunityStatus.OPEN
                    ).update({"status": OpportunityStatus.CLOSED,
                              "closed_at": datetime.utcnow()})

                    for arb_kw in detector.find_arbs(um):
                        db.session.add(ArbitrageOpportunity(**arb_kw))

                    EVOpportunity.query.filter_by(
                        match_id=um.id, status=OpportunityStatus.OPEN
                    ).update({"status": OpportunityStatus.CLOSED,
                              "closed_at": datetime.utcnow()})

                    for ev_kw in detector.find_ev(um):
                        db.session.add(EVOpportunity(**ev_kw))
                except Exception as exc:
                    logger.warning("opportunity detection error: %s", exc)

                return um.id

        except Exception as exc:
            logger.error("persist_combined_match error: %s", exc)
            # FIX: Clear caches to prevent reusing IDs that were just rolled back in the main session
            self._comp_cache.clear()
            self._team_cache.clear()
            self._sport_cache.clear()
            self._bookmaker_cache.clear()
            return None

    # ─────────────────────────────────────────────────────────────────────
    # BATCH PERSISTENCE
    # ─────────────────────────────────────────────────────────────────────

    def persist_batch(self, combined_matches: list, commit: bool = True) -> dict:
        """
        Persist a list of CombinedMatch objects to PostgreSQL.
        Returns summary stats.
        """
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
                # db.session.rollback() is no longer needed here for individual items 
                # because `db.session.begin_nested()` handles individual rollbacks cleanly.

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
        """
        Try to extract a sport slug from the CombinedMatch.
        The merger doesn't always carry sport info, so we fall back to
        competition name heuristics.
        """
        # Check if markets have sport-specific slugs
        # For now, default to soccer if unknown
        # TODO: The combined stream should carry sport_slug
        return "soccer"

    @staticmethod
    def _parse_start_time(raw) -> datetime | None:
        if not raw:
            return None
        try:
            if isinstance(raw, datetime):
                return raw
            if isinstance(raw, str):
                return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if isinstance(raw, (int, float)):
                return datetime.utcfromtimestamp(float(raw))
        except Exception:
            pass
        return None


# ═════════════════════════════════════════════════════════════════════════════
# QUERY SERVICE  (read path — used by API endpoints)
# ═════════════════════════════════════════════════════════════════════════════

class MatchQueryService:
    """
    High-level query methods for API endpoints.
    All return dicts ready for JSON serialization.
    Uses Redis cache where indicated.
    """

    @staticmethod
    def get_match_detail(match_id: int) -> dict | None:
        """Full match detail with odds, events, stats, lineups."""
        from app.models.odds_model import UnifiedMatch
        from app.models.match import (
            MatchEvent, MatchStats, MatchLineup
        )
        from app.models.bookmakers_model import (
             BookmakerMatchLink
        )

        um = UnifiedMatch.query.get(match_id)
        if not um:
            return None

        d = um.to_dict(include_bookmaker_odds=True)

        # Add events
        events = MatchEvent.query.filter_by(match_id=match_id).order_by(
            MatchEvent.minute, MatchEvent.id
        ).all()
        d["events"] = [e.to_dict() for e in events]

        # Add stats
        stats = MatchStats.query.filter_by(match_id=match_id).all()
        d["stats"] = {s.team_side: s.to_dict() for s in stats}

        # Add lineups
        lineups = MatchLineup.query.filter_by(match_id=match_id).order_by(
            MatchLineup.team_side, MatchLineup.lineup_type, MatchLineup.jersey_number
        ).all()
        d["lineups"] = {
            "home": [l.to_dict() for l in lineups if l.team_side == "home"],
            "away": [l.to_dict() for l in lineups if l.team_side == "away"],
        }

        # Add bookmaker links
        links = BookmakerMatchLink.query.filter_by(match_id=match_id).all()
        d["bookmaker_links"] = [l.to_dict() for l in links]

        return d

    @staticmethod
    def get_team_form(team_id: int, limit: int = 10) -> list[dict]:
        """Last N matches for a team with results."""
        from app.models.odds_model import UnifiedMatch, MatchStatus

        matches = UnifiedMatch.query.filter(
            db.or_(
                UnifiedMatch.home_team_id == team_id,
                UnifiedMatch.away_team_id == team_id,
            ),
            UnifiedMatch.status == MatchStatus.FINISHED,
        ).order_by(UnifiedMatch.start_time.desc()).limit(limit).all()

        results = []
        for m in matches:
            is_home = m.home_team_id == team_id
            if m.score_home is not None and m.score_away is not None:
                if is_home:
                    if m.score_home > m.score_away: outcome = "W"
                    elif m.score_home == m.score_away: outcome = "D"
                    else: outcome = "L"
                    goals_for, goals_against = m.score_home, m.score_away
                else:
                    if m.score_away > m.score_home: outcome = "W"
                    elif m.score_away == m.score_home: outcome = "D"
                    else: outcome = "L"
                    goals_for, goals_against = m.score_away, m.score_home
            else:
                outcome = None
                goals_for = goals_against = None

            results.append({
                "match_id":      m.id,
                "home_team":     m.home_team_name,
                "away_team":     m.away_team_name,
                "competition":   m.competition_name,
                "start_time":    m.start_time.isoformat() if m.start_time else None,
                "score_home":    m.score_home,
                "score_away":    m.score_away,
                "is_home":       is_home,
                "outcome":       outcome,
                "goals_for":     goals_for,
                "goals_against": goals_against,
            })

        return results

    @staticmethod
    def get_h2h(team_a_id: int, team_b_id: int, limit: int = 10) -> list[dict]:
        """Head-to-head history between two teams."""
        from app.models.odds_model import UnifiedMatch, MatchStatus

        matches = UnifiedMatch.query.filter(
            UnifiedMatch.status == MatchStatus.FINISHED,
            db.or_(
                db.and_(
                    UnifiedMatch.home_team_id == team_a_id,
                    UnifiedMatch.away_team_id == team_b_id,
                ),
                db.and_(
                    UnifiedMatch.home_team_id == team_b_id,
                    UnifiedMatch.away_team_id == team_a_id,
                ),
            ),
        ).order_by(UnifiedMatch.start_time.desc()).limit(limit).all()

        return [m.to_dict() for m in matches]

    @staticmethod
    def get_competition_matches(
        competition_id: int,
        status: str | None = None,
        page: int = 1,
        per_page: int = 25,
    ) -> dict:
        """Matches for a competition with pagination."""
        from app.models.odds_model import UnifiedMatch, MatchStatus

        q = UnifiedMatch.query.filter_by(competition_id=competition_id)

        if status:
            try:
                q = q.filter(UnifiedMatch.status == MatchStatus(status))
            except ValueError:
                pass

        total = q.count()
        matches = q.order_by(UnifiedMatch.start_time.desc()).offset(
            (page - 1) * per_page
        ).limit(per_page).all()

        return {
            "total": total,
            "page": page,
            "per_page": per_page,
            "pages": max(1, (total + per_page - 1) // per_page),
            "matches": [m.to_dict() for m in matches],
        }