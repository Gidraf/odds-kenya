"""
app/api/workflow_market_validator.py
=====================================
Market coverage validation for harvest workflow parser output.

Rules
─────
Match-list workflows  (workflow_type in MATCH_LIST_WORKFLOW_TYPES)
  • Only the sport's PRIMARY result market is required (e.g. "1X2" for Football).
  • Football specifically requires the 3-way 1X2 — no Moneyline substitute.
  • Return an error if that primary market is absent.
  • All other markets are ignored — they are not expected on a match-list endpoint.

Full-market workflows
  • Compare unique market names in the parsed rows against the full catalogue
    for the sport (sport-specific + universal markets).
  • coverage_pct = (markets_present / markets_expected) × 100
  • coverage_pct < threshold (default 50 %)  →  ok=False, error listing every
    missing market name + slug + description.
  • coverage_pct ≥ threshold                 →  ok=True,  missing listed as warnings.

Public API
──────────
  validate_market_coverage(rows, sport, workflow_type, threshold_pct) -> CoverageResult
  get_expected_markets(sport_name)    -> list[MarketDef]
  get_primary_markets(sport_name)     -> list[MarketDef]
  enrich_test_result_with_coverage(test_result, sport, workflow_type) -> dict
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import NamedTuple

from app.utils.mapping_seed import (
    MARKETS_BY_SPORT,
    get_primary_slugs_for_sport,
)

# ── Workflow type classification ──────────────────────────────────────────────
MATCH_LIST_WORKFLOW_TYPES: frozenset[str] = frozenset({
    "MATCH_LIST",
    "LIVE_MATCHES",
    "FIXTURE_LIST",
    "LIST",
})


# ── MarketDef ─────────────────────────────────────────────────────────────────

class MarketDef(NamedTuple):
    name:        str
    slug:        str
    description: str
    sport:       str | None    # None = universal

    def to_dict(self) -> dict:
        return {
            "name":        self.name,
            "slug":        self.slug,
            "description": self.description,
            "sport":       self.sport,
        }


# ── CoverageResult ────────────────────────────────────────────────────────────

@dataclass
class CoverageResult:
    ok:              bool
    workflow_type:   str
    sport:           str | None
    coverage_pct:    float
    present_markets: list[str]       # market names found in rows (deduplicated)
    missing_markets: list[MarketDef] # expected but absent
    extra_markets:   list[str]       # present but not in catalogue
    expected_count:  int
    present_count:   int
    error:           str | None
    warnings:        list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "ok":              self.ok,
            "workflow_type":   self.workflow_type,
            "sport":           self.sport,
            "coverage_pct":    round(self.coverage_pct, 1),
            "present_markets": self.present_markets,
            "missing_markets": [m.to_dict() for m in self.missing_markets],
            "extra_markets":   self.extra_markets,
            "expected_count":  self.expected_count,
            "present_count":   self.present_count,
            "error":           self.error,
            "warnings":        self.warnings,
        }


# ── Internal helpers ──────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Lower-case, strip, collapse whitespace."""
    return " ".join(s.lower().split())


def _build_catalogue(sport_name: str | None) -> list[MarketDef]:
    """
    Collect all MarketDef objects relevant to this sport:
      - universal (None) entries always included
      - sport-specific entries included when sport_name matches
    """
    seen_slugs: set[str] = set()
    results: list[MarketDef] = []

    for sport_key, entries in MARKETS_BY_SPORT.items():
        include = (
            sport_key is None                                          # universal
            or (sport_name and sport_key.lower() == sport_name.lower())
        )
        if not include:
            continue
        for name, slug, description in entries:
            if slug not in seen_slugs:
                results.append(MarketDef(name=name, slug=slug,
                                         description=description, sport=sport_key))
                seen_slugs.add(slug)

    return results


def _extract_present_markets(rows: list[dict]) -> set[str]:
    """Return the set of unique, stripped market strings from parsed rows."""
    present: set[str] = set()
    for row in rows:
        mkt = row.get("market") or row.get("market_name") or ""
        if mkt:
            present.add(str(mkt).strip())
    return present


# ── Public: get_expected_markets ─────────────────────────────────────────────

def get_expected_markets(sport_name: str | None) -> list[MarketDef]:
    """All markets (universal + sport-specific) expected for a given sport."""
    return _build_catalogue(sport_name)


def get_primary_markets(sport_name: str | None) -> list[MarketDef]:
    """The primary/result market(s) required for a match-list workflow."""
    primary_slugs = set(get_primary_slugs_for_sport(sport_name))
    catalogue     = _build_catalogue(sport_name)
    universal     = _build_catalogue(None)

    all_defs = {m.slug: m for m in (universal + catalogue)}
    result   = [all_defs[slug] for slug in primary_slugs if slug in all_defs]

    if not result:
        result = [MarketDef(
            name=f"Primary Result Market ({sport_name or 'universal'})",
            slug=next(iter(primary_slugs), "match_winner"),
            description="Primary match result market for this sport",
            sport=sport_name,
        )]
    return result


# ── Main validator ────────────────────────────────────────────────────────────

def validate_market_coverage(
    rows:          list[dict],
    sport_name:    str | None,
    workflow_type: str   = "MARKETS_ONLY",
    threshold_pct: float = 50.0,
) -> CoverageResult:
    """
    Validate parsed rows against the expected market catalogue.

    Parameters
    ──────────
    rows          : list of dicts returned by parse_data()
    sport_name    : e.g. "Football", "Basketball", None
    workflow_type : string from MATCH_LIST_WORKFLOW_TYPES or a full-market type
    threshold_pct : minimum % of markets required (default 50)
    """
    is_match_list = workflow_type in MATCH_LIST_WORKFLOW_TYPES
    present_raw   = _extract_present_markets(rows)
    present_norms = {_norm(m): m for m in present_raw}

    # ─────────────────────────────────────────────────────────────────────────
    # BRANCH A — Match-list: only the primary result market is required
    # ─────────────────────────────────────────────────────────────────────────
    if is_match_list:
        primary_defs  = get_primary_markets(sport_name)
        primary_norms = (
            {_norm(m.name) for m in primary_defs}
            | {_norm(m.slug.replace("_", " ")) for m in primary_defs}
        )

        has_primary = bool(present_norms.keys() & primary_norms)

        if not present_raw:
            return CoverageResult(
                ok=False,
                workflow_type=workflow_type,
                sport=sport_name,
                coverage_pct=0.0,
                present_markets=[],
                missing_markets=primary_defs,
                extra_markets=[],
                expected_count=len(primary_defs),
                present_count=0,
                error=(
                    "Parser returned no rows. "
                    "Match-list workflows must emit at least the primary result market: "
                    f"[{', '.join(m.name for m in primary_defs)}]."
                ),
            )

        if not has_primary:
            primary_names = ", ".join(m.name for m in primary_defs)
            return CoverageResult(
                ok=False,
                workflow_type=workflow_type,
                sport=sport_name,
                coverage_pct=0.0,
                present_markets=sorted(present_raw),
                missing_markets=primary_defs,
                extra_markets=[],
                expected_count=len(primary_defs),
                present_count=0,
                error=(
                    f"Match-list workflow is missing the required primary result market "
                    f"for {sport_name or 'this sport'}. "
                    f"Expected: [{primary_names}]. "
                    f"Markets found in rows: {sorted(present_raw)}. "
                    f"Ensure parse_data() emits rows with market='{primary_defs[0].name}'."
                ),
            )

        warnings = []
        if len(present_raw) > 1:
            warnings.append(
                f"Match-list mode: only primary result market is validated. "
                f"{len(present_raw) - 1} extra market(s) found but not checked."
            )

        return CoverageResult(
            ok=True,
            workflow_type=workflow_type,
            sport=sport_name,
            coverage_pct=100.0,
            present_markets=sorted(present_raw),
            missing_markets=[],
            extra_markets=[],
            expected_count=len(primary_defs),
            present_count=len(primary_defs),
            error=None,
            warnings=warnings,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # BRANCH B — Full-market workflow: validate against full catalogue
    # ─────────────────────────────────────────────────────────────────────────
    catalogue = _build_catalogue(sport_name)

    if not catalogue:
        return CoverageResult(
            ok=True,
            workflow_type=workflow_type,
            sport=sport_name,
            coverage_pct=100.0,
            present_markets=sorted(present_raw),
            missing_markets=[],
            extra_markets=[],
            expected_count=0,
            present_count=len(present_raw),
            error=None,
            warnings=[f"No market catalogue found for sport '{sport_name}'. Coverage check skipped."],
        )

    # Normalised lookup: both market name and slug normalised → MarketDef
    catalogue_norm: dict[str, MarketDef] = {}
    for m in catalogue:
        catalogue_norm[_norm(m.name)]                   = m
        catalogue_norm[_norm(m.slug.replace("_", " "))] = m

    # Which catalogue slugs are represented in present rows
    matched_slugs: set[str] = set()
    for p_norm in present_norms:
        if p_norm in catalogue_norm:
            matched_slugs.add(catalogue_norm[p_norm].slug)

    # Deduplicated catalogue by slug
    unique_catalogue: dict[str, MarketDef] = {m.slug: m for m in catalogue}
    n_expected = len(unique_catalogue)
    n_present  = len(matched_slugs)

    # Missing: expected but not in rows — sort primary markets to the top
    primary_slugs_set = set(get_primary_slugs_for_sport(sport_name))
    missing: list[MarketDef] = sorted(
        [m for slug, m in unique_catalogue.items() if slug not in matched_slugs],
        key=lambda m: (0 if m.slug in primary_slugs_set else 1, m.name),
    )

    # Extra: in rows but not in catalogue
    extra: list[str] = sorted(
        orig for norm, orig in present_norms.items()
        if norm not in catalogue_norm
    )

    coverage  = (n_present / n_expected * 100) if n_expected else 100.0
    ok        = coverage >= threshold_pct
    warnings: list[str] = []
    error:    str | None = None

    if ok and missing:
        warnings.append(
            f"{len(missing)} catalogue market(s) not found in rows "
            f"({coverage:.0f}% coverage — above {threshold_pct:.0f}% threshold). "
            f"Missing: {[m.name for m in missing[:10]]}"
            + (" …" if len(missing) > 10 else "")
        )

    if not ok:
        # Comprehensive error: list ALL missing markets with slugs
        missing_list = [
            f"{m.name} (slug: {m.slug})"
            for m in missing
        ]
        error = (
            f"Market coverage too low: {coverage:.1f}% "
            f"({n_present}/{n_expected} expected markets present, "
            f"threshold is {threshold_pct:.0f}%). "
            f"Missing {len(missing)} market(s): {missing_list}"
        )

    return CoverageResult(
        ok=ok,
        workflow_type=workflow_type,
        sport=sport_name,
        coverage_pct=coverage,
        present_markets=sorted(present_raw),
        missing_markets=missing,
        extra_markets=extra,
        expected_count=n_expected,
        present_count=n_present,
        error=error,
        warnings=warnings,
    )


# ── Convenience: attach coverage to a test-result dict ───────────────────────

def enrich_test_result_with_coverage(
    test_result:   dict,
    sport_name:    str | None,
    workflow_type: str   = "MARKETS_ONLY",
    threshold_pct: float = 50.0,
) -> dict:
    """
    Attach a market_coverage block to a parser test-result dict.

    Side effects:
    - Sets test_result["market_coverage"] to the CoverageResult dict.
    - If coverage fails, flips test_result["ok"] to False and prepends
      the coverage error to test_result["validation_errors"].
    - If coverage passes with warnings, appends them to test_result["warnings"].

    Returns the mutated dict.
    """
    rows = test_result.get("rows") or []
    if not rows:
        test_result["market_coverage"] = None
        return test_result

    coverage = validate_market_coverage(
        rows=rows,
        sport_name=sport_name,
        workflow_type=workflow_type,
        threshold_pct=threshold_pct,
    )
    test_result["market_coverage"] = coverage.to_dict()

    if not coverage.ok:
        test_result["ok"] = False
        errs = list(test_result.get("validation_errors") or [])
        errs.insert(0, f"[MARKET COVERAGE] {coverage.error}")
        test_result["validation_errors"] = errs

    elif coverage.warnings:
        warns = list(test_result.get("warnings") or [])
        warns.extend(coverage.warnings)
        test_result["warnings"] = warns

    return test_result