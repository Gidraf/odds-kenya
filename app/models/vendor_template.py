"""
app/models/vendor_template.py
==============================
VendorTemplate  — a shared API format used by multiple bookmakers.
BookmakerVendorConfig — per-bookmaker parameters for a vendor.

Example: 1xBet, BetWinner, Melbet, PariMatch all use the same
"LiveFeed/Get1x2_VZip" endpoint; only `base_url` and `partner` differ.

One VendorTemplate  →  N BookmakerVendorConfig  →  N Bookmakers
One parser, one URL pattern, unlimited bookmakers.

Registration (app/__init__.py or extensions):
    from app.models.vendor_template import VendorTemplate, BookmakerVendorConfig  # noqa
    # Then run: flask db migrate && flask db upgrade
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.extensions import db


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _j(text: str | None, fallback: Any = None) -> Any:
    if not text:
        return fallback
    try:
        return json.loads(text)
    except Exception:
        return fallback


# ─────────────────────────────────────────────────────────────────────────────
# VendorTemplate
# ─────────────────────────────────────────────────────────────────────────────

class VendorTemplate(db.Model):
    """
    Represents a shared API format/vendor used by multiple bookmakers.

    URL templates use double-brace placeholders:
      {{base_url}}    — bookmaker's root domain
      {{partner}}     — bookmaker's partner ID
      {{country}}     — country/region code
      {{gr}}          — sport group ID (from sport_params)
      {{match_id}}    — event ID (for markets endpoint)
      {{count}}       — page size

    Example list URL:
      {{base_url}}/service-api/LiveFeed/Get1x2_VZip?
        count={{count}}&lng=en&gr={{gr}}&mode=4
        &country={{country}}&partner={{partner}}
        &virtualSports=true&noFilterBlockEvent=true
    """
    __tablename__ = "vendor_templates"

    id          = db.Column(db.Integer, primary_key=True)
    name        = db.Column(db.String(200), nullable=False)
    slug        = db.Column(db.String(100), unique=True, nullable=False)  # "1xbet-livefeed"
    description = db.Column(db.Text)

    # ── URL patterns ──────────────────────────────────────────────────────────
    list_url_template    = db.Column(db.Text)   # upcoming matches
    live_url_template    = db.Column(db.Text)   # live matches (often same endpoint, mode differs)
    markets_url_template = db.Column(db.Text)   # per-match odds (if separate endpoint)

    # ── Param schema ──────────────────────────────────────────────────────────
    # Params shared by all bookmakers — e.g. {"lng": "en", "mode": "4", "count": "50"}
    common_params_json       = db.Column(db.Text, default="{}")

    # Which keys are bookmaker-specific (used to render the assignment form)
    # e.g. ["partner", "country"]
    bookmaker_param_keys_json = db.Column(db.Text, default='["partner", "country"]')

    # ── Sport / group mapping ─────────────────────────────────────────────────
    # Maps our canonical sport names → vendor-specific group/sport IDs
    # e.g. {"Football": "1", "Basketball": "3", "Volleyball": "6"}
    # These become the {{gr}} / {{sport_id}} value when building requests.
    # Bookmakers can override per-sport in their own config.
    sport_params_json = db.Column(db.Text, default="{}")

    # SI field name in the response that identifies the sport (for auto-detect)
    # e.g. "SI"  →  1=Football, 3=Basketball …
    sport_id_field = db.Column(db.String(50), default="SI")

    # ── Parser ────────────────────────────────────────────────────────────────
    parser_code        = db.Column(db.Text)
    parser_test_passed = db.Column(db.Boolean, default=False)
    parser_sample_json = db.Column(db.Text)   # cached live sample for re-testing

    # ── Meta ──────────────────────────────────────────────────────────────────
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # ── Relationships ─────────────────────────────────────────────────────────
    bookmaker_configs = db.relationship(
        "BookmakerVendorConfig",
        back_populates="vendor",
        cascade="all, delete-orphan",
        lazy="dynamic",
    )

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def common_params(self) -> dict:
        return _j(self.common_params_json, {})

    @property
    def sport_params(self) -> dict:
        return _j(self.sport_params_json, {})

    @property
    def bookmaker_param_keys(self) -> list:
        return _j(self.bookmaker_param_keys_json, ["partner", "country"])

    def resolve_url(self, url_template: str, sport: str | None = None,
                    bk_config: "BookmakerVendorConfig | None" = None) -> tuple[str, dict]:
        """
        Resolve a URL template for a specific sport + bookmaker combination.
        Returns (base_url_without_qs, merged_params).
        """
        # Merged params: common → bookmaker-specific → sport override
        params: dict = {**self.common_params}
        if bk_config:
            params.update(bk_config.params)

        # Sport group ID
        sport_params = {**self.sport_params}
        if bk_config:
            sport_params.update(bk_config.sport_params_override)
        if sport and sport in sport_params:
            params["gr"] = sport_params[sport]

        # Resolve {{base_url}} in URL
        base_url = url_template
        if bk_config and bk_config.base_url:
            base_url = base_url.replace("{{base_url}}", bk_config.base_url.rstrip("/"))

        # Split off any query string embedded in the template
        if "?" in base_url:
            base_url, qs = base_url.split("?", 1)
            for part in qs.split("&"):
                if "=" in part:
                    k, v = part.split("=", 1)
                    params.setdefault(k.strip(), v.strip())

        # Resolve remaining {{placeholders}} in base_url from params
        import re
        base_url = re.sub(
            r"\{\{(\w+)\}\}",
            lambda m: str(params.pop(m.group(1), f"{{{{{m.group(1)}}}}}")),
            base_url,
        )
        return base_url, params

    def to_dict(self, include_bookmakers: bool = True) -> dict:
        configs = list(self.bookmaker_configs) if include_bookmakers else []
        return {
            "id":                     self.id,
            "name":                   self.name,
            "slug":                   self.slug,
            "description":            self.description or "",
            "list_url_template":      self.list_url_template or "",
            "live_url_template":      self.live_url_template or "",
            "markets_url_template":   self.markets_url_template or "",
            "common_params":          self.common_params,
            "bookmaker_param_keys":   self.bookmaker_param_keys,
            "sport_params":           self.sport_params,
            "sport_id_field":         self.sport_id_field or "SI",
            "parser_code":            self.parser_code or "",
            "parser_test_passed":     bool(self.parser_test_passed),
            "bookmaker_count":        len(configs),
            "bookmakers":             [c.to_dict() for c in configs],
            "created_at":             self.created_at.isoformat() if self.created_at else None,
            "updated_at":             self.updated_at.isoformat() if self.updated_at else None,
        }


# ─────────────────────────────────────────────────────────────────────────────
# BookmakerVendorConfig
# ─────────────────────────────────────────────────────────────────────────────

class BookmakerVendorConfig(db.Model):
    """
    Per-bookmaker overrides for a VendorTemplate.

    Stores only the values that DIFFER between bookmakers using the same vendor:
      base_url   — "https://1xbet.co.ke"
      params     — {"partner": "61", "country": "87"}
      sport_params_override — {"Football": "656"}  (if gr differs from vendor default)
    """
    __tablename__ = "bookmaker_vendor_configs"

    id           = db.Column(db.Integer, primary_key=True)
    bookmaker_id = db.Column(db.Integer, db.ForeignKey("bookmakers.id"), nullable=False)
    vendor_id    = db.Column(db.Integer, db.ForeignKey("vendor_templates.id"), nullable=False)

    base_url     = db.Column(db.String(500))        # root domain, no trailing slash
    params_json  = db.Column(db.Text, default="{}")  # {"partner": "61", "country": "87"}

    # Per-sport group ID overrides (when a bookmaker uses different gr values)
    sport_params_override_json = db.Column(db.Text, default="{}")

    is_active = db.Column(db.Boolean, default=True)

    # IDs of generated HarvestWorkflows for this bookmaker+vendor combo
    upcoming_workflow_id = db.Column(db.Integer, db.ForeignKey("harvest_workflow.id"), nullable=True)
    live_workflow_id     = db.Column(db.Integer, db.ForeignKey("harvest_workflow.id"), nullable=True)

    # Last probe status per sport: {"Football": {"ok": True, "count": 42, "ts": 1234}}
    last_probe_json = db.Column(db.Text, default="{}")

    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    # ── Relationships ─────────────────────────────────────────────────────────
    vendor    = db.relationship("VendorTemplate", back_populates="bookmaker_configs")
    bookmaker = db.relationship("Bookmaker", foreign_keys=[bookmaker_id])

    upcoming_workflow = db.relationship(
        "HarvestWorkflow", foreign_keys=[upcoming_workflow_id], uselist=False
    )
    live_workflow = db.relationship(
        "HarvestWorkflow", foreign_keys=[live_workflow_id], uselist=False
    )

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def params(self) -> dict:
        return _j(self.params_json, {})

    @property
    def sport_params_override(self) -> dict:
        return _j(self.sport_params_override_json, {})

    @property
    def last_probe(self) -> dict:
        return _j(self.last_probe_json, {})

    def resolved_params(self) -> dict:
        """Vendor common params merged with this bookmaker's overrides."""
        return {**self.vendor.common_params, **self.params}

    def resolved_sport_params(self) -> dict:
        """Vendor sport params merged with bookmaker overrides."""
        return {**self.vendor.sport_params, **self.sport_params_override}

    def to_dict(self) -> dict:
        return {
            "id":                    self.id,
            "bookmaker_id":          self.bookmaker_id,
            "bookmaker_name":        self.bookmaker.name if self.bookmaker else None,
            "bookmaker_domain":      getattr(self.bookmaker, "domain", None),
            "vendor_id":             self.vendor_id,
            "base_url":              self.base_url or "",
            "params":                self.params,
            "sport_params_override": self.sport_params_override,
            "is_active":             self.is_active,
            "upcoming_workflow_id":  self.upcoming_workflow_id,
            "live_workflow_id":      self.live_workflow_id,
            "last_probe":            self.last_probe,
        }