"""
app/services/entity_resolver.py
==========================================
Key change: 
- Stripped all multi-bookmaker logic. 
- Strictly persists ONLY SportPesa (sp) markets and links.
- Removed Arbitrage and EV logic (since a single bookmaker cannot have cross-bookmaker arbitrage).
- cm is passed as a dictionary via Celery. Uses `_val()` helper to safely extract properties.
- _infer_sport_slug() normalises via _NAME_TO_SLUG map.
- Added country resolution with BookmakerCountryName mapping and fuzzy matching.
"""

from __future__ import annotations

import difflib
import logging
import re
import traceback
from datetime import datetime, timezone

from app.extensions import db

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
    """Return similarity ratio between two strings (0-1)."""
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _best_match(name: str, candidates: list[str], threshold: float = 0.85) -> tuple[str | None, float]:
    """Find best candidate with similarity >= threshold."""
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
        if isinstance(obj, dict): return obj.get(key, default)
        return getattr(obj, key, default)

    def _get_sp_bookmaker_id(self) -> int | None:
        """Fetch and cache ONLY the SportPesa bookmaker ID."""
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
        if not slug_lower: return None
        if slug_lower in self._sport_cache: return self._sport_cache[slug_lower]
        from app.models.competions_model import Sport
        canonical_name = SPORT_SLUG_MAP.get(slug_lower, sport_slug.replace("-", " ").title())
        canonical_slug = re.sub(r"[^a-z0-9]+", "-", slug_lower).strip("-")
        sport = Sport.get_or_create(name=canonical_name, slug=canonical_slug)
        self._sport_cache[slug_lower] = sport.id
        return sport.id

    def resolve_competition(self, comp_name: str, sport_id: int, country_id: int | None = None, betradar_id: str | None = None) -> int | None:
        if not comp_name or not sport_id: return None
        cache_key = f"{sport_id}|{comp_name.lower().strip()}"
        if cache_key in self._comp_cache: return self._comp_cache[cache_key]
        from app.models.competions_model import Competition
        if betradar_id:
            existing = Competition.query.filter_by(betradar_id=betradar_id).first()
            if existing:
                self._comp_cache[cache_key] = existing.id
                return existing.id
        # Pass country_id to Competition.get_or_create if we have it
        comp = Competition.get_or_create(name=comp_name.strip(), sport_id=sport_id, country_id=country_id, betradar_id=betradar_id)
        self._comp_cache[cache_key] = comp.id
        return comp.id

    def resolve_team(self, team_name: str, sport_id: int, country_id: int | None = None, betradar_id: str | None = None) -> int | None:
        if not team_name or not sport_id: return None
        cache_key = f"{sport_id}|{team_name.lower().strip()}"
        if cache_key in self._team_cache: return self._team_cache[cache_key]
        from app.models.competions_model import Team
        if betradar_id:
            existing = Team.query.filter_by(betradar_id=betradar_id).first()
            if existing:
                self._team_cache[cache_key] = existing.id
                return existing.id
        team = Team.get_or_create(name=team_name.strip(), sport_id=sport_id, country_id=country_id, betradar_id=betradar_id)
        self._team_cache[cache_key] = team.id
        return team.id

    def resolve_country(self, country_name: str) -> int | None:
        """Resolve country using BookmakerCountryName mapping, fuzzy search, and fallback creation."""
        if not country_name:
            return None
        country_name_clean = country_name.strip()
        if country_name_clean in self._country_cache:
            return self._country_cache[country_name_clean]

        from app.models.competions_model import Country
        from app.models.bookmake_competition_data import BookmakerCountryName

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
            # Create mapping for future use
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
        """Extract country from competition name like 'England: Premier League'."""
        if not comp_name:
            return ""
        # Split on colon or dash
        parts = re.split(r'[:|-]', comp_name, maxsplit=1)
        if len(parts) > 1:
            return parts[0].strip()
        return ""

    def _map_sp_match(self, match_id, sp_game_id, betradar_id=None):
        sp_id = self._get_sp_bookmaker_id()
        
        if not sp_id or not match_id or not sp_game_id: 
            logger.warning(
                f"⚠️ [Linker Warning] Skipping SP link. Missing data -> "
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
        """
        Persists ONLY SportPesa data. The expected dictionary `cm` comes directly
        from the `sp_harvester.py` yield output.
        """
        from app.models.odds_model import (
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

        # If country not explicitly provided, try to extract from competition name
        if not cm_country and cm_competition:
            cm_country = self._extract_country_from_competition(cm_competition)

        # The fallback parent ID is the SP Game ID if Betradar is missing
        parent_id = cm_betradar_id or f"sp_{cm_sp_game_id}"

        try:
            with db.session.begin_nested():
                sport_slug = self._infer_sport_slug(cm)
                sport_id   = self.resolve_sport(sport_slug) if sport_slug else None
                
                # Resolve country
                country_id = self.resolve_country(cm_country) if cm_country else None
                comp_id    = None
                
                if sport_id and cm_competition:
                    comp_id = self.resolve_competition(cm_competition, sport_id, country_id=country_id)
                home_team_id = away_team_id = None
                if sport_id:
                    if cm_home_team:
                        home_team_id = self.resolve_team(cm_home_team, sport_id, country_id=country_id)
                    if cm_away_team:
                        away_team_id = self.resolve_team(cm_away_team, sport_id, country_id=country_id)

                if not parent_id: return None

                um = UnifiedMatch.query.filter_by(parent_match_id=parent_id).with_for_update().first()
                start_time = self._parse_start_time(cm_start_time)
                sport_name_db = SPORT_SLUG_MAP.get(sport_slug or "", sport_slug or "")

                # --- 1. UPSERT UNIFIED MATCH ---
                if not um:
                    um = UnifiedMatch(
                        parent_match_id=parent_id, home_team_name=cm_home_team,
                        away_team_name=cm_away_team, sport_name=sport_name_db,
                        competition_name=cm_competition or "", start_time=start_time,
                        competition_id=comp_id, home_team_id=home_team_id, away_team_id=away_team_id,
                        country_id=country_id, country_name=cm_country,
                    )
                    db.session.add(um)
                    db.session.flush()
                else:
                    if cm_home_team and not um.home_team_name: um.home_team_name = cm_home_team
                    if cm_away_team and not um.away_team_name: um.away_team_name = cm_away_team
                    if cm_competition and not um.competition_name: um.competition_name = cm_competition
                    if start_time and not um.start_time: um.start_time = start_time
                    if comp_id and not um.competition_id: um.competition_id = comp_id
                    if home_team_id and not um.home_team_id: um.home_team_id = home_team_id
                    if away_team_id and not um.away_team_id: um.away_team_id = away_team_id
                    if sport_name_db: um.sport_name = sport_name_db
                    if country_id and not um.country_id: um.country_id = country_id
                    if cm_country and not um.country_name: um.country_name = cm_country

                # --- 2. MAP SPORTPESA EXTERNAL ID ---
                if cm_sp_game_id: 
                    self._map_sp_match(um.id, cm_sp_game_id, cm_betradar_id)

                # --- 3. PERSIST SPORTPESA MARKETS ONLY ---
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
                            try: price = float(odd)
                            except (TypeError, ValueError): continue
                            if price <= 1.0: continue
                                
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
                                
                    if history_batch: BookmakerOddsHistory.bulk_append(history_batch)

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
                if mid: persisted += 1
                else: failed += 1
            except Exception as exc:
                logger.error("batch persist error: %s\n%s", exc, traceback.format_exc())
                failed += 1
        if commit:
            try: db.session.commit()
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
        if any(w in comp for w in ("league", "cup", " fc", "fc ", "united", "premier", "championship", "serie", "bundesliga", "laliga")): return "soccer"
        if "basket" in comp: return "basketball"
        if "tennis" in comp or "atp" in comp or "wta" in comp: return "tennis"
        if "hockey" in comp: return "ice-hockey"
        return "soccer"

    @staticmethod
    def _parse_start_time(raw) -> datetime | None:
        if not raw: return None
        try:
            if isinstance(raw, datetime): return raw
            if isinstance(raw, str): return datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if isinstance(raw, (int, float)): return datetime.utcfromtimestamp(float(raw))
        except Exception: pass
        return None