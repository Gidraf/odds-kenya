"""
app/models/endpoint_config_model.py
====================================
EndpointConfig SQLAlchemy model and _build_fetch_url helper.

Extracted from app/api/endpoints_manual.py so that Celery tasks (and any
other non-API code) can import from the models layer without touching the
Flask blueprint layer.

Usage
─────
# In models/__init__.py
from app.models.endpoint_config_model import EndpointConfig

# In Celery tasks / anywhere else
from app.models.endpoint_config_model import EndpointConfig, _build_fetch_url

# The API blueprint still works — it imports from here too:
# app/api/endpoints_manual.py:
#   from app.models.endpoint_config_model import EndpointConfig, _build_fetch_url

Migration
─────────
If you already have an endpoint_config table from the old inline model, just
run:  flask db migrate && flask db upgrade
No schema change — the table definition is identical.
"""

from __future__ import annotations

import json
import urllib.parse
from datetime import datetime, timezone

from sqlalchemy import text

from app.extensions import db


# ─────────────────────────────────────────────────────────────────────────────
# PARAM SOURCE REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

PARAM_SOURCES: dict[str, dict] = {
    "sport": {
        "label":       "Sport",
        "table":       "sports",
        "columns":     ["id", "name", "external_id", "slug"],
        "filter_cols": ["id", "name", "is_active"],
        "description": "Sports (Football, Basketball, Tennis…)",
    },
    "competition": {
        "label":       "Competition / League",
        "table":       "competitions",
        "columns":     ["id", "name", "external_id", "sport_id"],
        "filter_cols": ["id", "sport_id", "name", "country_id"],
        "description": "Competitions and leagues",
    },
    "unifiedmatch": {
        "label":       "Match / Event",
        "table":       "unified_matches",
        "columns":     ["id", "parent_match_id", "home_team_id", "away_team_id", "start_time"],
        "filter_cols": ["status", "sport_name", "competition_id", "start_time"],
        "description": "Matches — use fetch_mode=today for today's fixtures",
    },
    "bookmaker": {
        "label":       "Bookmaker",
        "table":       "bookmakers",
        "columns":     ["id", "name", "domain"],
        "filter_cols": ["id", "name", "is_active"],
        "description": "Bookmakers registered in the system",
    },
    "team": {
        "label":       "Team",
        "table":       "teams",
        "columns":     ["id", "name", "external_id", "sport_id"],
        "filter_cols": ["id", "sport_id", "name"],
        "description": "Teams",
    },
    "marketdefinition": {
        "label":       "Market Definition",
        "table":       "market_definitions",
        "columns":     ["id", "name"],
        "filter_cols": ["id", "name"],
        "description": "Market types (1X2, Over/Under…)",
    },
}

FETCH_MODES: list[dict] = [
    {"value": "all",    "label": "All rows",        "description": "Return all rows up to max_values"},
    {"value": "today",  "label": "Today only",       "description": "Filter by today's date (start_time)"},
    {"value": "filter", "label": "Filter by value",  "description": "WHERE filter_column = filter_value"},
    {"value": "static", "label": "Static value",     "description": "Use filter_value literally, no DB lookup"},
]

ENDPOINT_TYPES: list[str] = [
    "ODDS", "MATCH_LIST", "MARKETS", "LIVE_SCORES",
    "LINEUPS", "CONFIG", "OTHER",
]


# ─────────────────────────────────────────────────────────────────────────────
# MODEL
# ─────────────────────────────────────────────────────────────────────────────

class EndpointConfig(db.Model):
    """
    Human-defined endpoint configuration.

    Stores everything needed to fetch + parse data from one bookmaker API
    endpoint: URL pattern, HTTP method, authentication headers, static and
    dynamic query params, optional JSON body template, and the generated parser.

    Dynamic params are resolved at harvest time by querying the DB — e.g.
    fetching all active sport IDs to build ?sport_ids=1,2,3.
    """
    __tablename__ = "endpoint_configs"

    id                    = db.Column(db.Integer, primary_key=True)
    bookmaker_id          = db.Column(
        db.Integer, db.ForeignKey("bookmakers.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    name                  = db.Column(db.String(200), nullable=False)
    endpoint_type         = db.Column(db.String(50),  default="ODDS",  nullable=False)
    base_url              = db.Column(db.Text,         nullable=False)
    method                = db.Column(db.String(10),   default="GET",  nullable=False)

    # Stored as JSON text — use the *_parsed properties to read them
    headers_json          = db.Column(db.Text, default="{}")
    static_params_json    = db.Column(db.Text, default="{}")
    dynamic_params_json   = db.Column(db.Text, default="[]")
    path_params_json      = db.Column(db.Text, default="[]")

    body_template         = db.Column(db.Text, nullable=True,
                                      comment="JSON body template for POST/PUT requests")
    parser_code           = db.Column(db.Text, nullable=True,
                                      comment="parse_data(raw_data) -> list[dict]")
    parser_test_passed    = db.Column(db.Boolean, default=False)

    is_active             = db.Column(db.Boolean, default=False, nullable=False, index=True)
    poll_interval_seconds = db.Column(db.Integer, default=60)

    last_harvested_at   = db.Column(db.DateTime(timezone=True), nullable=True)
    last_harvest_status = db.Column(db.String(20), nullable=True)   # OK | ERROR | EMPTY | …
    last_harvest_rows   = db.Column(db.Integer, default=0)

    created_at = db.Column(
        db.DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    updated_at = db.Column(
        db.DateTime(timezone=True),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=True,
    )

    # ── Relationships ────────────────────────────────────────────────────────
    bookmaker = db.relationship("Bookmaker", lazy="joined", foreign_keys=[bookmaker_id])

    # ── Parsed accessors ─────────────────────────────────────────────────────

    @property
    def headers_parsed(self) -> dict:
        try:
            return json.loads(self.headers_json or "{}")
        except Exception:
            return {}

    @property
    def static_params_parsed(self) -> dict:
        try:
            return json.loads(self.static_params_json or "{}")
        except Exception:
            return {}

    @property
    def dynamic_params_parsed(self) -> list:
        try:
            return json.loads(self.dynamic_params_json or "[]")
        except Exception:
            return []

    @property
    def path_params_parsed(self) -> list:
        try:
            return json.loads(self.path_params_json or "[]")
        except Exception:
            return []

    # ── Serialisation ────────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "id":                    self.id,
            "bookmaker_id":          self.bookmaker_id,
            "bookmaker_name":        self.bookmaker.name if self.bookmaker else "",
            "name":                  self.name,
            "endpoint_type":         self.endpoint_type,
            "base_url":              self.base_url,
            "method":                self.method,
            "headers":               self.headers_parsed,
            "static_params":         self.static_params_parsed,
            "dynamic_params":        self.dynamic_params_parsed,
            "path_params":           self.path_params_parsed,
            "body_template":         self.body_template,
            "parser_code":           self.parser_code,
            "parser_test_passed":    self.parser_test_passed,
            "is_active":             self.is_active,
            "poll_interval_seconds": self.poll_interval_seconds,
            "last_harvested_at":     self.last_harvested_at.isoformat()
                                     if self.last_harvested_at else None,
            "last_harvest_status":   self.last_harvest_status,
            "last_harvest_rows":     self.last_harvest_rows,
            "created_at":            self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self) -> str:
        return f"<EndpointConfig #{self.id} {self.name!r} [{self.endpoint_type}]>"


# ─────────────────────────────────────────────────────────────────────────────
# URL BUILDER  (no Flask context needed — pure SQLAlchemy + urllib)
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_dynamic_params(dynamic_params: list, static_params: dict) -> dict:
    """
    Build a concrete query-param dict by resolving dynamic params from the DB
    and merging them with static_params.

    Each dynamic param spec:
    {
      "param_key":     "sport_id",
      "source_table":  "sport",          # key into PARAM_SOURCES
      "source_column": "id",
      "filter_column": null,
      "filter_value":  null,
      "multi_value":   true,             # join multiple values with separator?
      "separator":     ",",
      "max_values":    20,
      "fetch_mode":    "all"             # all | today | filter | static
    }
    """
    result = dict(static_params or {})

    for dp in (dynamic_params or []):
        key       = dp.get("param_key", "").strip()
        mode      = dp.get("fetch_mode", "all")
        separator = dp.get("separator", ",")
        max_vals  = int(dp.get("max_values") or 20)
        source    = dp.get("source_table", "")
        col       = dp.get("source_column", "id")
        filt_col  = dp.get("filter_column")
        filt_val  = dp.get("filter_value")

        if not key:
            continue

        # Static value — no DB hit
        if mode == "static":
            result[key] = str(filt_val or "")
            continue

        src_info = PARAM_SOURCES.get(source)
        if not src_info:
            continue

        table = src_info["table"]

        try:
            if mode == "today":
                sql  = text(
                    f"SELECT {col} FROM {table} "
                    "WHERE DATE(start_time) = CURRENT_DATE "
                    f"ORDER BY {col} LIMIT :lim"
                )
                rows = db.session.execute(sql, {"lim": max_vals}).fetchall()

            elif mode == "filter" and filt_col and filt_val is not None:
                sql  = text(
                    f"SELECT {col} FROM {table} "
                    f"WHERE {filt_col} = :val "
                    f"ORDER BY {col} LIMIT :lim"
                )
                rows = db.session.execute(sql, {"val": filt_val, "lim": max_vals}).fetchall()

            else:  # all
                sql  = text(
                    f"SELECT {col} FROM {table} "
                    f"ORDER BY {col} LIMIT :lim"
                )
                rows = db.session.execute(sql, {"lim": max_vals}).fetchall()

            ids = [str(r[0]) for r in rows if r[0] is not None]
            if ids:
                if dp.get("multi_value"):
                    result[key] = separator.join(ids)
                else:
                    result[key] = ids[0]

        except Exception as e:
            print(f"[EndpointConfig] dynamic param error ({key}): {e}")

    return result


def _resolve_path_params(url: str, path_params: list) -> str:
    """
    Replace {placeholder} tokens in the URL path using DB lookups or static
    values.

    Each path param spec:
    {
      "placeholder":   "{league_id}",
      "source_table":  "competition",
      "source_column": "external_id",
      "filter_column": "sport_id",
      "filter_value":  "1",
      "fetch_mode":    "filter"   # or "static"
    }
    """
    for pp in (path_params or []):
        placeholder = pp.get("placeholder", "")
        if not placeholder or placeholder not in url:
            continue

        mode     = pp.get("fetch_mode", "all")
        source   = pp.get("source_table", "")
        col      = pp.get("source_column", "id")
        filt_col = pp.get("filter_column")
        filt_val = pp.get("filter_value")

        if mode == "static":
            url = url.replace(placeholder, str(filt_val or ""))
            continue

        src_info = PARAM_SOURCES.get(source)
        if not src_info:
            continue
        table = src_info["table"]

        try:
            if mode == "filter" and filt_col and filt_val is not None:
                sql = text(
                    f"SELECT {col} FROM {table} "
                    f"WHERE {filt_col} = :val LIMIT 1"
                )
                row = db.session.execute(sql, {"val": filt_val}).fetchone()
            else:
                sql = text(f"SELECT {col} FROM {table} ORDER BY {col} LIMIT 1")
                row = db.session.execute(sql).fetchone()

            if row:
                url = url.replace(placeholder, str(row[0]))
        except Exception as e:
            print(f"[EndpointConfig] path param error ({placeholder}): {e}")

    return url


def _build_fetch_url(config: dict) -> str:
    """
    Build a fully-resolved, ready-to-fetch URL from an endpoint config dict.

    Accepts either a raw dict (e.g. from EndpointConfig.to_dict()) or the
    internal config dict used by the harvest task.  Both shapes are supported.

    Steps:
      1. Resolve {placeholder} tokens in the URL path via _resolve_path_params.
      2. Resolve dynamic params from the DB via _resolve_dynamic_params.
      3. Merge static params.
      4. Append the query string.
    """
    base_url = (
        config.get("base_url")
        or config.get("url", "")
    ).strip()

    path_params    = config.get("path_params",    [])
    static_params  = config.get("static_params",  {})
    dynamic_params = config.get("dynamic_params", [])

    url    = _resolve_path_params(base_url, path_params)
    params = _resolve_dynamic_params(dynamic_params, static_params)

    if params:
        url = url + "?" + urllib.parse.urlencode(params)

    return url