"""
app/services/entity_resolver.py
==========================================
- Persists unified matches and resolves sports, competitions, teams, countries.
- Now **stores every bookmaker‑specific team name** in BookmakerTeamName.
- resolve_team accepts optional bookmaker_key; when given, it uses and
  updates the alias table.
- All other existing functionality remains unchanged.
"""

from __future__ import annotations

import difflib
import logging
import re
import traceback
from datetime import datetime, timezone

from app.extensions import db
from app.models.bookmake_competition_data import (
    BookmakerTeamName,
    BookmakerCompetitionName,
    BookmakerCountryName,
)

logger = logging.getLogger(__name__)

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


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _best_match(name: str, candidates: list[str], threshold: float = 0.85) -> tuple[str | None, float]:
    if not candidates:
        return None, 0.0
    best = max(candidates, key=lambda x: _similarity(name, x))
    score = _similarity(name, best)
    return (best, score) if score >= threshold else (None, score)


class EntityResolver:
    def __init__(self):
        self._sport_cache:     dict[str, int]        = {}
        self._comp_cache:      dict[str, int]        = {}
        self._team_cache:      dict[str, int]        = {}
        self._country_cache:   dict[str, int]        = {}
        self._sp_bookmaker_id: int | None            = None

    @staticmethod
    def _val(obj, key: str, default=None):
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    def _get_sp_bookmaker_id(self) -> int | None:
        if self._sp_bookmaker_id is not None:
            return self._sp_bookmaker_id
        try:
            from app.models.bookmakers_model import Bookmaker
            bm = Bookmaker.query.filter(db.func.lower(Bookmaker.name) == "sportpesa").first()
            if not bm:
                bm = Bookmaker.query.filter_by(slug="sp").first()
            self._sp_bookmaker_id = bm.id if bm else None
            return self._sp_bookmaker_id
        except Exception:
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

    def resolve_competition(
        self, comp_name: str, sport_id: int,
        country_id: int | None = None,
        betradar_id: str | None = None
    ) -> int | None:
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
            name=comp_name.strip(), sport_id=sport_id,
            country_id=country_id, betradar_id=betradar_id
        )
        self._comp_cache[cache_key] = comp.id
        return comp.id

    # ── NEW: bookmaker‑aware team resolution ────────────────────────────────
    def resolve_team(
        self,
        team_name: str,
        sport_id: int,
        bookmaker_key: str | None = None,   # new optional, backward compatible
        country_id: int | None = None,
        betradar_id: str | None = None,
    ) -> int | None:
        """
        Resolve a team name as seen on a specific bookmaker.
        If bookmaker_key is given, we first check (and later create)
        a BookmakerTeamName mapping, making future matches exact.
        """
        if not team_name or not sport_id:
            return None

        team_name_clean = team_name.strip()
        cache_key = f"{sport_id}|{team_name_clean.lower()}"
        if cache_key in self._team_cache:
            return self._team_cache[cache_key]

        from app.models.competions_model import Team

        # 1. If we know the bookmaker, try the alias table
        if bookmaker_key:
            existing_alias = BookmakerTeamName.query.filter_by(
                bookmaker_key=bookmaker_key,
                bookmaker_team_name=team_name_clean,
            ).first()
            if existing_alias:
                self._team_cache[cache_key] = existing_alias.team_id
                return existing_alias.team_id

        # 2. Betradar ID lookup
        if betradar_id:
            team = Team.query.filter_by(betradar_id=betradar_id).first()
            if team and team.sport_id == sport_id:
                self._store_team_alias(bookmaker_key, team_name_clean, team.id)
                self._team_cache[cache_key] = team.id
                return team.id

        # 3. Exact canonical name match
        canonical = Team.query.filter(
            db.func.lower(Team.name) == team_name_clean.lower(),
            Team.sport_id == sport_id,
        ).first()
        if canonical:
            self._store_team_alias(bookmaker_key, team_name_clean, canonical.id)
            self._team_cache[cache_key] = canonical.id
            return canonical.id

        # 4. Fuzzy match against all active teams in the sport
        all_teams = Team.query.filter(Team.sport_id == sport_id).all()
        if all_teams:
            candidates = [t.name for t in all_teams]
            best_name, score = _best_match(team_name_clean, candidates, threshold=0.85)
            if best_name and score >= 0.85:
                best_team = next(t for t in all_teams if t.name == best_name)
                self._store_team_alias(bookmaker_key, team_name_clean, best_team.id)
                self._team_cache[cache_key] = best_team.id
                return best_team.id

        # 5. Create new canonical team
        new_team = Team.get_or_create(
            name=team_name_clean, sport_id=sport_id,
            country_id=country_id, betradar_id=betradar_id,
        )
        self._store_team_alias(bookmaker_key, team_name_clean, new_team.id)
        self._team_cache[cache_key] = new_team.id
        return new_team.id

    def _store_team_alias(self, bookmaker_key: str | None, bookmaker_team_name: str, team_id: int) -> None:
        """Create a BookmakerTeamName mapping if bookmaker_key provided."""
        if not bookmaker_key:
            return
        try:
            BookmakerTeamName.get_or_create(bookmaker_key, bookmaker_team_name, team_id)
        except Exception:
            logger.exception(
                "Failed to store team alias: %s / %s", bookmaker_key, bookmaker_team_name
            )

    # ── Country resolution (unchanged) ─────────────────────────────────────
    def resolve_country(self, country_name: str) -> int | None:
        if not country_name:
            return None
        country_name_clean = country_name.strip()
        if country_name_clean in self._country_cache:
            return self._country_cache[country_name_clean]

        from app.models.competions_model import Country

        # 1. Try mapping table for bookmaker "sp"
        mapping = BookmakerCountryName.query.filter_by(
            bookmaker_key="sp",
            bookmaker_country_name=country_name_clean
        ).first()
        if mapping and mapping.country_id:
            self._country_cache[country_name_clean] = mapping.country_id
            return mapping.country_id

        # 2. Exact match
        exact = Country.query.filter(db.func.lower(Country.name) == country_name_clean.lower()).first()
        if exact:
            BookmakerCountryName.get_or_create("sp", country_name_clean, exact.id)
            db.session.commit()
            self._country_cache[country_name_clean] = exact.id
            return exact.id

        # 3. Fuzzy match
        all_countries = Country.query.all()
        candidates = [(c.id, c.name) for c in all_countries]
        if candidates:
            best_name, best_score = _best_match(country_name_clean, [name for _, name in candidates])
            if best_name and best_score >= 0.85:
                best_id = next(cid for cid, name in candidates if name == best_name)
                BookmakerCountryName.get_or_create("sp", country_name_clean, best_id)
                db.session.commit()
                self._country_cache[country_name_clean] = best_id
                return best_id

        # 4. Create new country
        country = Country.get_or_create(name=country_name_clean)
        BookmakerCountryName.get_or_create("sp", country_name_clean, country.id)
        db.session.commit()
        self._country_cache[country_name_clean] = country.id
        return country.id

    def _extract_country_from_competition(self, comp_name: str) -> str:
        if not comp_name:
            return ""
        parts = re.split(r'[:|-]', comp_name, maxsplit=1)
        if len(parts) > 1:
            return parts[0].strip()
        return ""

    def _map_sp_match(self, match_id, sp_game_id, betradar_id=None):
        sp_id = self._get_sp_bookmaker_id()
        if not sp_id or not match_id or not sp_game_id:
            logger.warning(
                "⚠️ [Linker Warning] Skipping SP link. Missing data -> "
                f"DB sp_id: {sp_id}, DB match_id: {match_id}, "
                f"Received sp_game_id: '{sp_game_id}'"
            )
            return
        try:
            from app.models.bookmakers_model import BookmakerMatchLink
            BookmakerMatchLink.upsert(
                match_id=match_id, bookmaker_id=sp_id,
                external_match_id=sp_game_id, betradar_id=betradar_id,
            )
        except Exception as exc:
            logger.error(
                f"❌ [Linker Error] DB failure while upserting link for SportPesa "
                f"(Match: {match_id}, ExtID: {sp_game_id}). "
                f"Error: {str(exc)}",
                exc_info=True
            )

    def persist_sp_match(self, cm) -> int | None:
        from app.models.odds import (
            UnifiedMatch, BookmakerMatchOdds, BookmakerOddsHistory
        )
        cm_competition = self._val(cm, "competition")
        cm_home_team   = self._val(cm, "home_team")
        cm_away_team   = self._val(cm, "away_team")
        cm_betradar_id = self._val(cm, "betradar_id")
        cm_sp_game_id  = self._val(cm, "sp_game_id")
        cm_start_time  = self._val(cm, "start_time")
        cm_markets     = self._val(cm, "markets") or {}
        cm_country     = self._val(cm, "country") or self._val(cm, "country_name") or ""
        if not cm_country and cm_competition:
            cm_country = self._extract_country_from_competition(cm_competition)
        parent_id = cm_betradar_id or f"sp_{cm_sp_game_id}"
        try:
            with db.session.begin_nested():
                sport_slug = self._infer_sport_slug(cm)
                sport_id   = self.resolve_sport(sport_slug) if sport_slug else None
                country_id = self.resolve_country(cm_country) if cm_country else None
                comp_id    = None
                if sport_id and cm_competition:
                    comp_id = self.resolve_competition(cm_competition, sport_id, country_id=country_id)
                home_team_id = away_team_id = None
                if sport_id:
                    if cm_home_team:
                        # Pass bookmaker_key="sp" to store the alias
                        home_team_id = self.resolve_team(
                            cm_home_team, sport_id, bookmaker_key="sp",
                            country_id=country_id, betradar_id=cm_betradar_id
                        )
                    if cm_away_team:
                        away_team_id = self.resolve_team(
                            cm_away_team, sport_id, bookmaker_key="sp",
                            country_id=country_id, betradar_id=cm_betradar_id
                        )
                if not parent_id:
                    return None
                um = UnifiedMatch.query.filter_by(parent_match_id=parent_id).with_for_update().first()
                start_time = self._parse_start_time(cm_start_time)
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
                        country_name=cm_country,
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
                    if cm_country and not um.country_name:
                        um.country_name = cm_country
                if cm_sp_game_id:
                    self._map_sp_match(um.id, cm_sp_game_id, cm_betradar_id)
                sp_id = self._get_sp_bookmaker_id()
                if sp_id and cm_markets:
                    bmo = BookmakerMatchOdds.query.filter_by(
                        match_id=um.id, bookmaker_id=sp_id
                    ).with_for_update().first()
                    if not bmo:
                        bmo = BookmakerMatchOdds(match_id=um.id, bookmaker_id=sp_id)
                        db.session.add(bmo)
                        db.session.flush()
                    history_batch: list[dict] = []
                    for mkt_slug, outcomes in cm_markets.items():
                        for outcome, odd in outcomes.items():
                            try:
                                price = float(odd)
                            except (TypeError, ValueError):
                                continue
                            if price <= 1.0:
                                continue
                            price_changed, old_price = bmo.upsert_selection(
                                market=mkt_slug, specifier=None, selection=outcome, price=price,
                            )
                            um.upsert_bookmaker_price(
                                market=mkt_slug, specifier=None,
                                selection=outcome, price=price, bookmaker_id=sp_id,
                            )
                            if price_changed:
                                history_batch.append({
                                    "bmo_id": bmo.id, "bookmaker_id": sp_id,
                                    "match_id": um.id, "market": mkt_slug,
                                    "specifier": None, "selection": outcome,
                                    "old_price": old_price, "new_price": price,
                                    "price_delta": round(price - old_price, 4) if old_price else None,
                                    "recorded_at": datetime.utcnow(),
                                })
                    if history_batch:
                        BookmakerOddsHistory.bulk_append(history_batch)
                return um.id
        except Exception as exc:
            logger.error(
                "\n" + "="*50 + "\n"
                "❌ CRITICAL ERROR IN PERSIST_SP_MATCH ❌\n"
                f"Error: {str(exc)}\n"
                f"Match: {cm_home_team} vs {cm_away_team}\n"
                f"SP Game ID: {cm_sp_game_id} | Betradar ID: {cm_betradar_id}\n"
                f"Country: {cm_country}\n"
                f"Traceback:\n{traceback.format_exc()}\n"
                + "="*50 + "\n"
            )
            self._comp_cache.clear()
            self._team_cache.clear()
            self._sport_cache.clear()
            self._country_cache.clear()
            return None

    def persist_batch(self, sp_matches: list, commit: bool = True) -> dict:
        persisted = failed = 0
        for m in sp_matches:
            try:
                mid = self.persist_sp_match(m)
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
                return {"persisted": 0, "failed": len(sp_matches), "error": str(exc)}
        return {"persisted": persisted, "failed": failed}

    @classmethod
    def _infer_sport_slug(cls, cm) -> str | None:
        raw = cls._val(cm, "sport") or cls._val(cm, "sport_slug") or cls._val(cm, "sport_name") or ""
        if raw:
            slug = _NAME_TO_SLUG.get(raw) or _NAME_TO_SLUG.get(raw.lower())
            return slug or raw.lower().replace(" ", "-")
        comp = (cls._val(cm, "competition") or "").lower()
        if any(w in comp for w in ("league", "cup", " fc", "fc ", "united", "premier", "championship", "serie", "bundesliga", "laliga")):
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
                return datetime.fromtimestamp(float(raw))
        except Exception:
            pass
        return None