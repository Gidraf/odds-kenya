"""
app/services/entity_resolver.py
==========================================
Key change: 
- Added comprehensive traceback logging to `persist_combined_match` to catch silent DB fails.
- cm is passed as a dictionary via Celery. Uses `_val()` helper to safely extract properties.
- _infer_sport_slug() normalises via _NAME_TO_SLUG map.
"""

from __future__ import annotations

import logging
import re
import traceback
from datetime import datetime, timezone

from app.extensions import db
from app.models.oppotunity_detector import OpportunityDetector

logger = logging.getLogger(__name__)

BK_SLUG_TO_NAME = {"sp": "SportPesa", "bt": "Betika", "od": "OdiBets"}

SPORT_SLUG_MAP = {
    "soccer": "Soccer", "football": "Soccer", "Football": "Soccer",
    "esoccer": "eSoccer", "efootball": "eSoccer",
    "basketball": "Basketball", "Basketball": "Basketball",
    "tennis": "Tennis", "Tennis": "Tennis",
    "ice-hockey": "Ice Hockey", "ice hockey": "Ice Hockey", "Ice Hockey": "Ice Hockey",
    "volleyball": "Volleyball", "Volleyball": "Volleyball",
    "cricket": "Cricket", "Cricket": "Cricket",
    "rugby": "Rugby", "Rugby": "Rugby",
    "table-tennis": "Table Tennis", "table tennis": "Table Tennis", "Table Tennis": "Table Tennis",
    "boxing": "Boxing", "Boxing": "Boxing",
    "handball": "Handball", "Handball": "Handball",
    "mma": "MMA", "MMA": "MMA",
    "darts": "Darts", "Darts": "Darts",
    "american-football": "American Football",
    "baseball": "Baseball",
}

_NAME_TO_SLUG: dict[str, str] = {
    "Soccer": "soccer", "Football": "soccer", "football": "soccer", "soccer": "soccer",
    "Basketball": "basketball", "basketball": "basketball",
    "Tennis": "tennis", "tennis": "tennis",
    "Ice Hockey": "ice-hockey", "ice-hockey": "ice-hockey", "ice hockey": "ice-hockey",
    "Volleyball": "volleyball", "volleyball": "volleyball",
    "Cricket": "cricket", "cricket": "cricket",
    "Rugby": "rugby", "rugby": "rugby",
    "Table Tennis": "table-tennis", "table-tennis": "table-tennis", "table tennis": "table-tennis",
    "Handball": "handball", "handball": "handball",
    "MMA": "mma", "mma": "mma",
    "Boxing": "boxing", "boxing": "boxing",
    "Darts": "darts", "darts": "darts",
    "eSoccer": "esoccer", "eFootball": "esoccer", "esoccer": "esoccer",
}


class EntityResolver:
    def __init__(self):
        self._sport_cache:     dict[str, int]        = {}
        self._comp_cache:      dict[str, int]        = {}
        self._team_cache:      dict[str, int]        = {}
        self._bookmaker_cache: dict[str, int | None] = {}

    @staticmethod
    def _val(obj, key: str, default=None):
        """Safely extract values whether obj is a dictionary (Celery) or Class Object."""
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

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
                bm = Bookmaker.query.filter_by(slug=bk_slug).first()
            bk_id = bm.id if bm else None
            self._bookmaker_cache[bk_slug] = bk_id
            return bk_id
        except Exception:
            self._bookmaker_cache[bk_slug] = None
            return None

    def resolve_sport(self, sport_slug: str) -> int | None:
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

    def resolve_competition(self, comp_name: str, sport_id: int,
                            betradar_id: str | None = None) -> int | None:
        if not comp_name or not sport_id:
            return None
        cache_key = f"{sport_id}|{comp_name.lower().strip()}"
        if cache_key in self._comp_cache:
            return self._comp_cache[cache_key]
        from app.models.competions_model import Competition
        if betradar_id:
            existing = Competition.query.filter_by(betradar_id=betradar_id).first()
            if existing:
                self._comp_cache[cache_key] = existing.id
                return existing.id
        comp = Competition.get_or_create(
            name=comp_name.strip(), sport_id=sport_id, betradar_id=betradar_id,
        )
        self._comp_cache[cache_key] = comp.id
        return comp.id

    def resolve_team(self, team_name: str, sport_id: int,
                     betradar_id: str | None = None) -> int | None:
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
            name=team_name.strip(), sport_id=sport_id, betradar_id=betradar_id,
        )
        self._team_cache[cache_key] = team.id
        return team.id

    def _map_bookmaker_entity(self, bk_slug, entity_type, canonical_id,
                               external_id, external_name=None, betradar_id=None):
        bk_id = self._get_bookmaker_id(bk_slug)
        if not bk_id or not canonical_id or not external_id:
            return
        try:
            from app.models.bookmakers_model import BookmakerEntityMap
            BookmakerEntityMap.upsert(
                bookmaker_id=bk_id, entity_type=entity_type,
                canonical_id=canonical_id, external_id=external_id,
                external_name=external_name, betradar_id=betradar_id,
            )
        except Exception as exc:
            logger.debug("entity map error: %s", exc)

    def _map_bookmaker_match(self, bk_slug, match_id, external_match_id, betradar_id=None):
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

    def persist_combined_match(self, cm) -> int | None:
        from app.models.odds_model import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory,
            ArbitrageOpportunity, EVOpportunity,
            OpportunityStatus,
        )
        
        cm_competition = self._val(cm, "competition")
        cm_home_team   = self._val(cm, "home_team")
        cm_away_team   = self._val(cm, "away_team")
        cm_betradar_id = self._val(cm, "betradar_id")
        cm_join_key    = self._val(cm, "join_key")
        cm_start_time  = self._val(cm, "start_time")
        cm_bk_ids      = self._val(cm, "bk_ids") or {}
        cm_markets     = self._val(cm, "markets") or {}

        try:
            with db.session.begin_nested():
                sport_slug = self._infer_sport_slug(cm)
                sport_id   = self.resolve_sport(sport_slug) if sport_slug else None
                comp_id    = None
                
                if sport_id and cm_competition:
                    comp_id = self.resolve_competition(cm_competition, sport_id)
                home_team_id = away_team_id = None
                if sport_id:
                    if cm_home_team:
                        home_team_id = self.resolve_team(cm_home_team, sport_id)
                    if cm_away_team:
                        away_team_id = self.resolve_team(cm_away_team, sport_id)

                parent_id = cm_betradar_id or cm_join_key
                if not parent_id:
                    return None

                um = UnifiedMatch.query.filter_by(
                    parent_match_id=parent_id
                ).with_for_update().first()
                start_time   = self._parse_start_time(cm_start_time)
                sport_name_db = SPORT_SLUG_MAP.get(sport_slug or "", sport_slug or "")

                if not um:
                    um = UnifiedMatch(
                        parent_match_id=parent_id,
                        home_team_name=cm_home_team,
                        away_team_name=cm_away_team,
                        sport_name=sport_name_db,
                        competition_name=cm_competition or "",
                        start_time=start_time,
                        competition_id=comp_id,
                        home_team_id=home_team_id,
                        away_team_id=away_team_id,
                    )
                    db.session.add(um)
                    db.session.flush()
                else:
                    if cm_home_team and not um.home_team_name:
                        um.home_team_name = cm_home_team
                    if cm_away_team and not um.away_team_name:
                        um.away_team_name = cm_away_team
                    if cm_competition and not um.competition_name:
                        um.competition_name = cm_competition
                    if start_time and not um.start_time:
                        um.start_time = start_time
                    if comp_id and not um.competition_id:
                        um.competition_id = comp_id
                    if home_team_id and not um.home_team_id:
                        um.home_team_id = home_team_id
                    if away_team_id and not um.away_team_id:
                        um.away_team_id = away_team_id
                    if sport_name_db:
                        um.sport_name = sport_name_db

                for bk_slug, ext_id in cm_bk_ids.items():
                    if ext_id:
                        self._map_bookmaker_match(bk_slug, um.id, ext_id, cm_betradar_id)

                for bk_slug, bk_markets in cm_markets.items():
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
                                selection=outcome, price=price, bookmaker_id=bk_id,
                            )
                            if price_changed:
                                history_batch.append({
                                    "bmo_id": bmo.id, "bookmaker_id": bk_id,
                                    "match_id": um.id, "market": mkt_slug,
                                    "specifier": None, "selection": outcome,
                                    "old_price": old_price, "new_price": price,
                                    "price_delta": round(price - old_price, 4) if old_price else None,
                                    "recorded_at": datetime.utcnow(),
                                })
                                
                    if history_batch:
                        BookmakerOddsHistory.bulk_append(history_batch)

                try:
                    detector = OpportunityDetector(min_profit_pct=0.5, min_ev_pct=3.0)
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
            # 🔴 MASSIVE TRACEBACK ADDED HERE 🔴
            logger.error(
                "\n" + "="*50 + "\n"
                "❌ CRITICAL ERROR IN PERSIST_COMBINED_MATCH ❌\n"
                f"Error: {str(exc)}\n"
                f"Match: {cm_home_team} vs {cm_away_team}\n"
                f"Join Key: {cm_join_key} | Betradar ID: {cm_betradar_id}\n"
                f"Traceback:\n{traceback.format_exc()}\n"
                + "="*50 + "\n"
            )
            self._comp_cache.clear()
            self._team_cache.clear()
            self._sport_cache.clear()
            self._bookmaker_cache.clear()
            return None

    def persist_batch(self, combined_matches: list, commit: bool = True) -> dict:
        persisted = failed = 0
        for cm in combined_matches:
            try:
                mid = self.persist_combined_match(cm)
                if mid:
                    persisted += 1
                else:
                    failed += 1
            except Exception as exc:
                logger.error("batch persist error: %s\n%s", exc, traceback.format_exc())
                failed += 1
        if commit:
            try:
                db.session.commit()
            except Exception as exc:
                logger.error("batch commit error: %s\n%s", exc, traceback.format_exc())
                db.session.rollback()
                return {"persisted": 0, "failed": len(combined_matches), "error": str(exc)}
        return {"persisted": persisted, "failed": failed}

    @classmethod
    def _infer_sport_slug(cls, cm) -> str | None:
        """Read sport from CombinedMatch safely, normalise, fall back to heuristics."""
        raw = (
            cls._val(cm, "sport") or
            cls._val(cm, "sport_slug") or
            cls._val(cm, "sport_name") or
            ""
        )
        if raw:
            slug = _NAME_TO_SLUG.get(raw) or _NAME_TO_SLUG.get(raw.lower())
            return slug or raw.lower().replace(" ", "-")
            
        # Competition-name heuristics
        comp = (cls._val(cm, "competition") or "").lower()
        if any(w in comp for w in ("league", "cup", " fc", "fc ", "united", "premier",
                                   "championship", "serie", "bundesliga", "laliga")):
            return "soccer"
        if "basket" in comp:
            return "basketball"
        if "tennis" in comp or "atp" in comp or "wta" in comp:
            return "tennis"
        if "hockey" in comp:
            return "ice-hockey"
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


class MatchQueryService:
    @staticmethod
    def get_match_detail(match_id: int) -> dict | None:
        from app.models.odds_model import UnifiedMatch
        from app.models.match import MatchEvent, MatchStats, MatchLineup
        from app.models.bookmakers_model import BookmakerMatchLink
        um = UnifiedMatch.query.get(match_id)
        if not um:
            return None
        d = um.to_dict(include_bookmaker_odds=True)
        events = MatchEvent.query.filter_by(match_id=match_id).order_by(
            MatchEvent.minute, MatchEvent.id).all()
        d["events"] = [e.to_dict() for e in events]
        stats = MatchStats.query.filter_by(match_id=match_id).all()
        d["stats"] = {s.team_side: s.to_dict() for s in stats}
        lineups = MatchLineup.query.filter_by(match_id=match_id).order_by(
            MatchLineup.team_side, MatchLineup.lineup_type, MatchLineup.jersey_number).all()
        d["lineups"] = {
            "home": [l.to_dict() for l in lineups if l.team_side == "home"],
            "away": [l.to_dict() for l in lineups if l.team_side == "away"],
        }
        links = BookmakerMatchLink.query.filter_by(match_id=match_id).all()
        d["bookmaker_links"] = [l.to_dict() for l in links]
        return d

    @staticmethod
    def get_team_form(team_id: int, limit: int = 10) -> list[dict]:
        from app.models.odds_model import UnifiedMatch, MatchStatus
        matches = UnifiedMatch.query.filter(
            db.or_(UnifiedMatch.home_team_id == team_id,
                   UnifiedMatch.away_team_id == team_id),
            UnifiedMatch.status == MatchStatus.FINISHED,
        ).order_by(UnifiedMatch.start_time.desc()).limit(limit).all()
        results = []
        for m in matches:
            is_home = m.home_team_id == team_id
            if m.score_home is not None and m.score_away is not None:
                if is_home:
                    outcome = "W" if m.score_home > m.score_away else ("D" if m.score_home == m.score_away else "L")
                    gf, ga = m.score_home, m.score_away
                else:
                    outcome = "W" if m.score_away > m.score_home else ("D" if m.score_away == m.score_home else "L")
                    gf, ga = m.score_away, m.score_home
            else:
                outcome = gf = ga = None
            results.append({
                "match_id": m.id, "home_team": m.home_team_name,
                "away_team": m.away_team_name, "competition": m.competition_name,
                "start_time": m.start_time.isoformat() if m.start_time else None,
                "score_home": m.score_home, "score_away": m.score_away,
                "is_home": is_home, "outcome": outcome,
                "goals_for": gf, "goals_against": ga,
            })
        return results

    @staticmethod
    def get_h2h(team_a_id: int, team_b_id: int, limit: int = 10) -> list[dict]:
        from app.models.odds_model import UnifiedMatch, MatchStatus
        matches = UnifiedMatch.query.filter(
            UnifiedMatch.status == MatchStatus.FINISHED,
            db.or_(
                db.and_(UnifiedMatch.home_team_id == team_a_id,
                        UnifiedMatch.away_team_id == team_b_id),
                db.and_(UnifiedMatch.home_team_id == team_b_id,
                        UnifiedMatch.away_team_id == team_a_id),
            ),
        ).order_by(UnifiedMatch.start_time.desc()).limit(limit).all()
        return [m.to_dict() for m in matches]

    @staticmethod
    def get_competition_matches(competition_id: int, status: str | None = None,
                                page: int = 1, per_page: int = 25) -> dict:
        from app.models.odds_model import UnifiedMatch, MatchStatus
        q = UnifiedMatch.query.filter_by(competition_id=competition_id)
        if status:
            try:
                q = q.filter(UnifiedMatch.status == MatchStatus(status))
            except ValueError:
                pass
        total = q.count()
        matches = q.order_by(UnifiedMatch.start_time.desc()).offset(
            (page - 1) * per_page).limit(per_page).all()
        return {
            "total": total, "page": page, "per_page": per_page,
            "pages": max(1, (total + per_page - 1) // per_page),
            "matches": [m.to_dict() for m in matches],
        }