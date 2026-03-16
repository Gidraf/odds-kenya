"""
app/models/mapping_models.py
============================
Canonical mapping layer — allows deterministic (no-AI) parser generation
from stored JSON accessor paths and entity alias tables.

Tables
──────
  markets               — canonical internal market definitions
  market_aliases        — per-bookmaker name aliases for a market
  team_aliases          — per-bookmaker name aliases for a team
  competition_aliases   — per-bookmaker name aliases for a competition
  sport_aliases         — per-bookmaker name aliases for a sport
  bookmaker_endpoint_maps — per (bookmaker x endpoint_type) JSON structure map

JSON accessor path notation
───────────────────────────
Dot-notation, [] marks an array level.
  ""              - root IS the array
  "events"        - raw_data["events"]
  "events[].id"   - raw_data["events"][i]["id"]
  "a.b.c"         - raw_data["a"]["b"]["c"]

Migration
─────────
  flask db migrate -m "mapping_models"
  flask db upgrade

Register in app/models/__init__.py:
  from app.models.mapping_models import (
      Market, MarketAlias, TeamAlias, CompetitionAlias, SportAlias,
      BookmakerEndpointMap,
  )
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any

from app.extensions import db


# ---------------------------------------------------------------------------
# MARKET  (canonical)
# ---------------------------------------------------------------------------

class Market(db.Model):
    __tablename__ = "markets"

    id          = db.Column(db.Integer, primary_key=True)
    name        = db.Column(db.String(120), unique=True, nullable=False, index=True)
    slug        = db.Column(db.String(120), unique=True, nullable=False, index=True)
    description = db.Column(db.String(300), nullable=True)
    is_active   = db.Column(db.Boolean, default=True, nullable=False)
    sport_id    = db.Column(db.Integer, db.ForeignKey("sports.id"), nullable=True)
    created_at  = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    sport   = db.relationship("Sport", backref="markets", lazy="joined")
    aliases = db.relationship(
        "MarketAlias", back_populates="market",
        cascade="all, delete-orphan", lazy="dynamic"
    )

    def to_dict(self, with_aliases: bool = False) -> dict:
        d = {
            "id":          self.id,
            "name":        self.name,
            "slug":        self.slug,
            "description": self.description,
            "is_active":   self.is_active,
            "sport_id":    self.sport_id,
            "sport_name":  self.sport.name if self.sport else None,
        }
        if with_aliases:
            d["aliases"] = [a.to_dict() for a in self.aliases]
        return d

    @classmethod
    def get_or_create(cls, name: str, slug: str | None = None) -> "Market":
        import re
        obj = cls.query.filter_by(name=name).first()
        if not obj:
            auto_slug = slug or re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
            obj = cls(name=name, slug=auto_slug)
            db.session.add(obj)
            db.session.flush()
        return obj


# ---------------------------------------------------------------------------
# ALIAS TABLES
# ---------------------------------------------------------------------------

class MarketAlias(db.Model):
    __tablename__ = "market_aliases"
    __table_args__ = (
        db.UniqueConstraint("bookmaker_id", "alias_name", name="uq_market_alias_bk"),
    )

    id           = db.Column(db.Integer, primary_key=True)
    market_id    = db.Column(db.Integer, db.ForeignKey("markets.id",    ondelete="CASCADE"), nullable=False, index=True)
    bookmaker_id = db.Column(db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"), nullable=False, index=True)
    alias_name   = db.Column(db.String(200), nullable=False)
    notes        = db.Column(db.String(300), nullable=True)

    market    = db.relationship("Market",    back_populates="aliases")
    bookmaker = db.relationship("Bookmaker", lazy="joined")

    def to_dict(self) -> dict:
        return {
            "id":             self.id,
            "market_id":      self.market_id,
            "market_name":    self.market.name if self.market else None,
            "bookmaker_id":   self.bookmaker_id,
            "bookmaker_name": self.bookmaker.name if self.bookmaker else None,
            "alias_name":     self.alias_name,
            "notes":          self.notes,
        }


class TeamAlias(db.Model):
    __tablename__ = "team_aliases"
    __table_args__ = (
        db.UniqueConstraint("bookmaker_id", "alias_name", name="uq_team_alias_bk"),
    )

    id           = db.Column(db.Integer, primary_key=True)
    team_id      = db.Column(db.Integer, db.ForeignKey("teams.id",      ondelete="CASCADE"), nullable=False, index=True)
    bookmaker_id = db.Column(db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"), nullable=False, index=True)
    alias_name   = db.Column(db.String(200), nullable=False)

    team      = db.relationship("Team",      backref=db.backref("aliases", lazy="dynamic"), lazy="joined")
    bookmaker = db.relationship("Bookmaker", lazy="joined")

    def to_dict(self) -> dict:
        return {
            "id":             self.id,
            "team_id":        self.team_id,
            "team_name":      self.team.name if self.team else None,
            "bookmaker_id":   self.bookmaker_id,
            "bookmaker_name": self.bookmaker.name if self.bookmaker else None,
            "alias_name":     self.alias_name,
        }


class CompetitionAlias(db.Model):
    __tablename__ = "competition_aliases"
    __table_args__ = (
        db.UniqueConstraint("bookmaker_id", "alias_name", name="uq_comp_alias_bk"),
    )

    id             = db.Column(db.Integer, primary_key=True)
    competition_id = db.Column(db.Integer, db.ForeignKey("competitions.id", ondelete="CASCADE"), nullable=False, index=True)
    bookmaker_id   = db.Column(db.Integer, db.ForeignKey("bookmakers.id",   ondelete="CASCADE"), nullable=False, index=True)
    alias_name     = db.Column(db.String(200), nullable=False)

    competition = db.relationship("Competition", backref=db.backref("aliases", lazy="dynamic"), lazy="joined")
    bookmaker   = db.relationship("Bookmaker",   lazy="joined")

    def to_dict(self) -> dict:
        return {
            "id":               self.id,
            "competition_id":   self.competition_id,
            "competition_name": self.competition.name if self.competition else None,
            "bookmaker_id":     self.bookmaker_id,
            "bookmaker_name":   self.bookmaker.name if self.bookmaker else None,
            "alias_name":       self.alias_name,
        }


class SportAlias(db.Model):
    __tablename__ = "sport_aliases"
    __table_args__ = (
        db.UniqueConstraint("bookmaker_id", "alias_name", name="uq_sport_alias_bk"),
    )

    id           = db.Column(db.Integer, primary_key=True)
    sport_id     = db.Column(db.Integer, db.ForeignKey("sports.id",    ondelete="CASCADE"), nullable=False, index=True)
    bookmaker_id = db.Column(db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"), nullable=False, index=True)
    alias_name   = db.Column(db.String(200), nullable=False)

    sport     = db.relationship("Sport",     backref=db.backref("aliases", lazy="dynamic"), lazy="joined")
    bookmaker = db.relationship("Bookmaker", lazy="joined")

    def to_dict(self) -> dict:
        return {
            "id":             self.id,
            "sport_id":       self.sport_id,
            "sport_name":     self.sport.name if self.sport else None,
            "bookmaker_id":   self.bookmaker_id,
            "bookmaker_name": self.bookmaker.name if self.bookmaker else None,
            "alias_name":     self.alias_name,
        }


# ---------------------------------------------------------------------------
# BOOKMAKER ENDPOINT MAP
# ---------------------------------------------------------------------------

class BookmakerEndpointMap(db.Model):
    """
    Per-bookmaker, per-endpoint-type JSON structure map.

    Stores how to navigate the bookmaker's API response to extract matches,
    markets and selections — enabling deterministic (no-AI) parser generation.

    is_primary_bookmaker:
      When True, this bookmaker's team/competition/sport names become the
      canonical display names when auto-creating entities during harvest.
      Only one bookmaker should have this flag set system-wide.
    """
    __tablename__ = "bookmaker_endpoint_maps"
    __table_args__ = (
        db.UniqueConstraint("bookmaker_id", "endpoint_type", name="uq_bem_bk_type"),
    )

    id           = db.Column(db.Integer, primary_key=True)
    bookmaker_id = db.Column(
        db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

    # MATCH_LIST | MARKETS | COMBINED
    endpoint_type = db.Column(db.String(30), nullable=False)

    # HTTP config
    url           = db.Column(db.String(500), nullable=True)
    method        = db.Column(db.String(10),  default="GET")
    headers_json  = db.Column(db.Text, default="{}")
    params_json   = db.Column(db.Text, default="{}")
    body_template = db.Column(db.Text, nullable=True)
    curl_template = db.Column(db.Text, nullable=True)

    # Stored sample response for path re-mapping
    sample_response = db.Column(db.Text, nullable=True)

    # Match-list accessor paths
    match_list_array_path = db.Column(db.String(300), nullable=True)
    match_id_path         = db.Column(db.String(300), nullable=True)
    home_team_path        = db.Column(db.String(300), nullable=True)
    away_team_path        = db.Column(db.String(300), nullable=True)
    start_time_path       = db.Column(db.String(300), nullable=True)
    sport_path            = db.Column(db.String(300), nullable=True)
    competition_path      = db.Column(db.String(300), nullable=True)

    # Market accessor paths
    markets_array_path    = db.Column(db.String(300), nullable=True)
    market_name_path      = db.Column(db.String(300), nullable=True)
    specifier_path        = db.Column(db.String(300), nullable=True)
    selections_array_path = db.Column(db.String(300), nullable=True)
    selection_name_path   = db.Column(db.String(300), nullable=True)
    selection_price_path  = db.Column(db.String(300), nullable=True)

    is_primary_bookmaker = db.Column(db.Boolean, default=False, nullable=False)
    is_active  = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(
        db.DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    bookmaker = db.relationship("Bookmaker", lazy="joined")

    def to_dict(self) -> dict:
        return {
            "id":             self.id,
            "bookmaker_id":   self.bookmaker_id,
            "bookmaker_name": self.bookmaker.name if self.bookmaker else None,
            "endpoint_type":  self.endpoint_type,
            "url":            self.url,
            "method":         self.method,
            "headers_json":   self.headers_json,
            "params_json":    self.params_json,
            "body_template":  self.body_template,
            "curl_template":  self.curl_template,
            "sample_response": self.sample_response,
            "match_list_array_path": self.match_list_array_path,
            "match_id_path":         self.match_id_path,
            "home_team_path":        self.home_team_path,
            "away_team_path":        self.away_team_path,
            "start_time_path":       self.start_time_path,
            "sport_path":            self.sport_path,
            "competition_path":      self.competition_path,
            "markets_array_path":    self.markets_array_path,
            "market_name_path":      self.market_name_path,
            "specifier_path":        self.specifier_path,
            "selections_array_path": self.selections_array_path,
            "selection_name_path":   self.selection_name_path,
            "selection_price_path":  self.selection_price_path,
            "is_primary_bookmaker":  self.is_primary_bookmaker,
            "is_active":             self.is_active,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

    @staticmethod
    def resolve(obj: Any, path: str) -> Any:
        """Resolve a dot-path against a Python dict/list."""
        if not path or path.strip() == "":
            return obj
        try:
            parts = path.replace("[]", ".[*]").split(".")
            current: Any = obj
            i = 0
            while i < len(parts):
                seg = parts[i]
                if seg == "[*]":
                    if not isinstance(current, list):
                        return None
                    ns = parts[i + 1] if i + 1 < len(parts) else None
                    if ns is None:
                        return current
                    current = [
                        item.get(ns) if isinstance(item, dict) else None
                        for item in current
                    ]
                    i += 2
                    continue
                if isinstance(current, dict):
                    current = current.get(seg)
                elif isinstance(current, list):
                    current = [
                        item.get(seg) if isinstance(item, dict) else None
                        for item in current
                    ]
                else:
                    return None
                i += 1
            return current
        except Exception:
            return None

    def extract_matches(self, raw: dict | list) -> list[dict]:
        path = self.match_list_array_path or ""
        items = self.resolve(raw, path) if path else raw
        if isinstance(items, dict):
            items = [items]
        return items if isinstance(items, list) else []

    def extract_match_meta(self, item: dict) -> dict:
        return {
            "match_id":    self.resolve(item, self.match_id_path or ""),
            "home_team":   self.resolve(item, self.home_team_path or ""),
            "away_team":   self.resolve(item, self.away_team_path or ""),
            "start_time":  self.resolve(item, self.start_time_path or ""),
            "sport":       self.resolve(item, self.sport_path or ""),
            "competition": self.resolve(item, self.competition_path or ""),
        }

    def extract_rows(self, item: dict, match_meta: dict) -> list[dict]:
        rows = []
        for mkt in (self.resolve(item, self.markets_array_path or "") or []):
            if not isinstance(mkt, dict):
                continue
            market_name = self.resolve(mkt, self.market_name_path or "")
            specifier   = self.resolve(mkt, self.specifier_path or "") if self.specifier_path else None
            sels        = self.resolve(mkt, self.selections_array_path or "") or []
            if not isinstance(sels, list):
                continue
            for sel in sels:
                name  = self.resolve(sel, self.selection_name_path or "")
                price = self.resolve(sel, self.selection_price_path or "")
                try:
                    price = float(price)
                except (TypeError, ValueError):
                    continue
                if price <= 1.0:
                    continue
                rows.append({
                    **match_meta,
                    "parent_match_id": match_meta.get("match_id"),
                    "market":    market_name,
                    "specifier": str(specifier) if specifier is not None else None,
                    "selection": name,
                    "price":     price,
                })
        return rows

    def build_parser_code(self) -> str:
        """Generate deterministic parse_data() from stored paths. No AI needed."""
        bk_name = self.bookmaker.name if self.bookmaker else f"bookmaker_{self.bookmaker_id}"
        arr  = self.match_list_array_path or ""
        mid  = self.match_id_path         or "id"
        home = self.home_team_path        or "home_team"
        away = self.away_team_path        or "away_team"
        st   = self.start_time_path       or "start_time"
        sp   = self.sport_path            or "sport"
        comp = self.competition_path      or "competition"
        mkts = self.markets_array_path    or "markets"
        mkn  = self.market_name_path      or "name"
        spec = self.specifier_path        or ""
        sels = self.selections_array_path or "outcomes"
        seln = self.selection_name_path   or "name"
        selp = self.selection_price_path  or "odds"

        return f'''# Auto-generated parser — {bk_name} ({self.endpoint_type})
# Regenerate: BookmakerEndpointMap.build_parser_code()

def _get(obj, path):
    if not path:
        return obj
    try:
        parts = path.replace("[]", ".[*]").split(".")
        cur = obj
        i = 0
        while i < len(parts):
            seg = parts[i]
            if seg == "[*]":
                if not isinstance(cur, list):
                    return None
                ns = parts[i + 1] if i + 1 < len(parts) else None
                if ns is None:
                    return cur
                cur = [x.get(ns) if isinstance(x, dict) else None for x in cur]
                i += 2
                continue
            if isinstance(cur, dict):
                cur = cur.get(seg)
            elif isinstance(cur, list):
                cur = [x.get(seg) if isinstance(x, dict) else None for x in cur]
            else:
                return None
            i += 1
        return cur
    except Exception:
        return None


def parse_data(raw_data):
    try:
        items = _get(raw_data, {arr!r}) if {arr!r} else raw_data
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            return []
        rows = []
        for item in items:
            try:
                mid_val = _get(item, {mid!r})
                if mid_val is None:
                    continue
                meta = {{
                    "parent_match_id": mid_val,
                    "home_team":       _get(item, {home!r}),
                    "away_team":       _get(item, {away!r}),
                    "start_time":      _get(item, {st!r}),
                    "sport":           _get(item, {sp!r}),
                    "competition":     _get(item, {comp!r}),
                }}
                for mkt in (_get(item, {mkts!r}) or []):
                    try:
                        mkt_name = _get(mkt, {mkn!r})
                        specifier = _get(mkt, {spec!r}) if {spec!r} else None
                        for sel in (_get(mkt, {sels!r}) or []):
                            try:
                                price = float(_get(sel, {selp!r}) or 0)
                                if price <= 1.0:
                                    continue
                                rows.append({{**meta, "market": mkt_name,
                                    "specifier": str(specifier) if specifier is not None else None,
                                    "selection": _get(sel, {seln!r}), "price": price}})
                            except Exception:
                                pass
                    except Exception:
                        pass
            except Exception:
                pass
        return rows
    except Exception:
        return []
'''