"""
app/workers/fuzzy_matcher.py
=============================
Cross-bookmaker match alignment engine.

Aligns matches across all bookmakers (SP, BT, OD, 1xBet, 22Bet, Betwinner,
Melbet, Megapari, Helabet, Paripesa) using:
  1. BetradarID     → exact, highest confidence
  2. External ID    → per-BK exact match
  3. Fuzzy name     → rapidfuzz token_sort_ratio ≥ 85 on home+away
  4. Kick-off time  → within ±15 min window
  5. Competition    → optional fuzzy boost

Scoring rubric (0-100 confidence):
  betradar_exact   = 100
  name_score ≥ 92  + time ≤ 5m   = 98
  name_score ≥ 85  + time ≤ 15m  = 80-90
  name_score ≥ 75  + time ≤ 30m  = 60 (warn only)

Architecture:
  ┌─────────────────────────────────────────────┐
  │  FuzzyMatcher.align(source, candidates)     │
  │    → returns AlignResult(match, confidence) │
  └─────────────────────────────────────────────┘
      ↑ called by tasks_align.py after each harvest
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

# rapidfuzz is much faster than fuzzywuzzy
try:
    from rapidfuzz import fuzz, process as rfprocess
    _HAS_RAPIDFUZZ = True
except ImportError:
    try:
        from fuzzywuzzy import fuzz  # type: ignore
        _HAS_RAPIDFUZZ = False
    except ImportError:
        fuzz = None  # type: ignore
        _HAS_RAPIDFUZZ = False

import logging
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

EXACT_BETRADAR_SCORE    = 100
HIGH_CONFIDENCE_SCORE   = 95
MEDIUM_CONFIDENCE_SCORE = 80
LOW_CONFIDENCE_SCORE    = 60
MIN_ACCEPT_SCORE        = 72   # below this → no match

MAX_TIME_DELTA_MIN      = 15   # minutes window for same match
BOOST_TIME_DELTA_MIN    = 5    # tight window → +5 confidence
NAME_THRESHOLD          = 82   # minimum fuzz ratio for names

# Team name normalisation tokens to strip/replace
_STRIP_TOKENS = re.compile(
    r'\b(fc|sc|ac|cf|rc|rcd|afc|bfc|cd|sd|ud|ca|ad|ce|ee|'
    r'united|city|town|rovers|wanderers|athletic|atletico|'
    r'hotspur|county|borough|villa|albion|wednesday|'
    r'real|sporting|club|de|la|los|las|el|the|'
    r'u21|u23|u20|u18|u19|reserves|youth|b|ii|2)\b',
    re.IGNORECASE,
)
_MULTI_SPACE = re.compile(r'\s+')
_NON_ALPHA   = re.compile(r'[^a-z0-9\s]')


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class MatchCandidate:
    """Minimal match descriptor used for alignment."""
    bk_slug:        str           # "sp", "bt", "od", "1xbet", etc.
    external_id:    str           # BK-internal match ID
    home_team:      str
    away_team:      str
    start_time:     Optional[datetime]
    competition:    Optional[str] = None
    betradar_id:    Optional[str] = None
    country:        Optional[str] = None
    raw:            dict          = field(default_factory=dict)  # full original dict


@dataclass
class AlignResult:
    """Result of aligning a candidate to a unified match."""
    unified_match_id: Optional[int]   # None = no match found → create new
    confidence:       int              # 0-100
    method:           str              # how we matched
    candidate:        MatchCandidate


# ─── Normalisation ────────────────────────────────────────────────────────────

def _norm_name(name: str) -> str:
    """Normalise team name for fuzzy comparison."""
    if not name:
        return ""
    s = name.lower().strip()
    s = _NON_ALPHA.sub(' ', s)
    s = _STRIP_TOKENS.sub(' ', s)
    s = _MULTI_SPACE.sub(' ', s).strip()
    return s


def _parse_dt(val) -> Optional[datetime]:
    """Parse various datetime formats into a timezone-aware datetime."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, (int, float)):
        # Unix timestamp
        try:
            return datetime.fromtimestamp(val, tz=timezone.utc)
        except (ValueError, OSError):
            return None
    if isinstance(val, str):
        val = val.strip()
        # .NET /Date(ms)/ format
        m = re.match(r'/Date\((\d+)\)/', val)
        if m:
            return datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc)
        for fmt in (
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                dt = datetime.strptime(val, fmt)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _time_delta_minutes(a: Optional[datetime], b: Optional[datetime]) -> Optional[float]:
    if a is None or b is None:
        return None
    delta = abs((a - b).total_seconds()) / 60
    return delta


# ─── Core Scoring ─────────────────────────────────────────────────────────────

def _name_score(c: MatchCandidate, home: str, away: str) -> int:
    """
    Compute fuzzy name similarity score (0–100).
    Uses token_sort_ratio to handle word-order differences.
    """
    if fuzz is None:
        # Fallback: exact lowercase match
        h_match = int(_norm_name(c.home_team) == _norm_name(home)) * 100
        a_match = int(_norm_name(c.away_team) == _norm_name(away)) * 100
        return (h_match + a_match) // 2

    nh, na = _norm_name(c.home_team), _norm_name(c.away_team)
    rh, ra = _norm_name(home), _norm_name(away)

    if not nh or not na or not rh or not ra:
        return 0

    h_score = fuzz.token_sort_ratio(nh, rh)
    a_score = fuzz.token_sort_ratio(na, ra)
    base    = int((h_score + a_score) / 2)

    # Bonus: also try swapped (in case home/away are flipped)
    h_score2 = fuzz.token_sort_ratio(nh, ra)
    a_score2 = fuzz.token_sort_ratio(na, rh)
    swapped  = int((h_score2 + a_score2) / 2)

    # If swapped is better, deduct a penalty (different team order = different fixture)
    if swapped > base:
        return max(base, swapped - 15)

    return base


def _comp_score(c: MatchCandidate, comp: Optional[str]) -> int:
    """Fuzzy competition name match (0–100)."""
    if not c.competition or not comp or fuzz is None:
        return 50  # neutral when unknown
    return fuzz.token_sort_ratio(
        _norm_name(c.competition),
        _norm_name(comp),
    )


def score_pair(
    candidate: MatchCandidate,
    home_team: str,
    away_team: str,
    start_time: Optional[datetime] = None,
    competition: Optional[str] = None,
    betradar_id: Optional[str] = None,
) -> int:
    """
    Score how likely `candidate` is the same match as the reference.
    Returns confidence 0-100.
    """
    # ── Level 0: BetradarID exact ────────────────────────────────────────────
    if betradar_id and candidate.betradar_id:
        if betradar_id == candidate.betradar_id:
            return EXACT_BETRADAR_SCORE
        else:
            return 0  # Different BR ID → definitely different game

    # ── Level 1: Name similarity ─────────────────────────────────────────────
    ns = _name_score(candidate, home_team, away_team)
    if ns < NAME_THRESHOLD:
        return 0

    # Start with name score as base
    score = ns

    # ── Level 2: Time proximity ──────────────────────────────────────────────
    dt_delta = _time_delta_minutes(candidate.start_time, start_time)
    if dt_delta is not None:
        if dt_delta > MAX_TIME_DELTA_MIN:
            return 0  # too far apart in time
        elif dt_delta <= BOOST_TIME_DELTA_MIN:
            score = min(100, score + 5)
        # else: within window, no penalty
    else:
        # Unknown time → slight penalty
        score = max(0, score - 5)

    # ── Level 3: Competition boost ───────────────────────────────────────────
    if competition and candidate.competition:
        cs = _comp_score(candidate, competition)
        if cs >= 85:
            score = min(100, score + 3)
        elif cs < 50:
            score = max(0, score - 5)

    return int(score)


# ─── FuzzyMatcher Class ───────────────────────────────────────────────────────

class FuzzyMatcher:
    """
    Aligns incoming match candidates against existing unified matches.

    Usage:
        matcher = FuzzyMatcher(existing_matches)
        result  = matcher.align(candidate)
        if result.unified_match_id:
            # merge into existing
        else:
            # create new unified match
    """

    def __init__(self, existing: list[dict]):
        """
        existing: list of UnifiedMatch.to_dict() or similar dicts with keys:
          id, betradar_id, home_team_name, away_team_name, start_time,
          competition_name, sport_name, external_ids
        """
        self._index: list[dict] = existing

        # Build betradar lookup for O(1) BR-ID matches
        self._br_index: dict[str, dict] = {}
        for m in existing:
            br = m.get("betradar_id")
            if br:
                self._br_index[str(br)] = m

        # Build external_id lookups per BK
        self._ext_index: dict[str, dict[str, dict]] = {}
        for m in existing:
            ext = m.get("external_ids") or {}
            for bk, eid in ext.items():
                if bk not in self._ext_index:
                    self._ext_index[bk] = {}
                self._ext_index[bk][str(eid)] = m

    def align(self, candidate: MatchCandidate) -> AlignResult:
        """Find the best existing match for this candidate."""

        # ── Fast path: BetradarID ────────────────────────────────────────────
        if candidate.betradar_id:
            existing = self._br_index.get(str(candidate.betradar_id))
            if existing:
                return AlignResult(
                    unified_match_id=existing["id"],
                    confidence=EXACT_BETRADAR_SCORE,
                    method="betradar_id",
                    candidate=candidate,
                )

        # ── Fast path: known external_id for this BK ────────────────────────
        bk_ext = self._ext_index.get(candidate.bk_slug, {})
        if candidate.external_id and candidate.external_id in bk_ext:
            existing = bk_ext[candidate.external_id]
            return AlignResult(
                unified_match_id=existing["id"],
                confidence=HIGH_CONFIDENCE_SCORE,
                method="external_id",
                candidate=candidate,
            )

        # ── Fuzzy search ─────────────────────────────────────────────────────
        cand_dt = _parse_dt(candidate.start_time)
        best_score = 0
        best_match: Optional[dict] = None

        for m in self._index:
            s = score_pair(
                candidate=candidate,
                home_team=m.get("home_team_name", ""),
                away_team=m.get("away_team_name", ""),
                start_time=_parse_dt(m.get("start_time")),
                competition=m.get("competition_name"),
                betradar_id=m.get("betradar_id"),
            )
            if s > best_score:
                best_score = s
                best_match = m

        if best_match and best_score >= MIN_ACCEPT_SCORE:
            method = "fuzzy_high" if best_score >= HIGH_CONFIDENCE_SCORE else "fuzzy_medium"
            return AlignResult(
                unified_match_id=best_match["id"],
                confidence=best_score,
                method=method,
                candidate=candidate,
            )

        # ── No match found → caller should create new unified match ──────────
        return AlignResult(
            unified_match_id=None,
            confidence=0,
            method="no_match",
            candidate=candidate,
        )

    def update_index(self, new_match: dict) -> None:
        """Add a newly created unified match to local index."""
        self._index.append(new_match)
        br = new_match.get("betradar_id")
        if br:
            self._br_index[str(br)] = new_match
        ext = new_match.get("external_ids") or {}
        for bk, eid in ext.items():
            if bk not in self._ext_index:
                self._ext_index[bk] = {}
            self._ext_index[bk][str(eid)] = new_match


# ─── Bulk align helper (used by tasks_align.py) ───────────────────────────────

def bulk_align(
    candidates: list[MatchCandidate],
    existing_matches: list[dict],
    sport_slug: str,
) -> tuple[list[AlignResult], list[AlignResult]]:
    """
    Align a batch of candidates against existing unified matches.

    Returns:
        (updates, creates)
        updates: candidates matched to existing unified matches
        creates: candidates with no match → new unified matches needed
    """
    matcher = FuzzyMatcher(existing_matches)
    updates: list[AlignResult] = []
    creates: list[AlignResult] = []

    for cand in candidates:
        result = matcher.align(cand)
        if result.unified_match_id is not None:
            updates.append(result)
        else:
            creates.append(result)
            # add a placeholder so subsequent candidates from the same batch
            # don't create duplicates
            placeholder = {
                "id": f"__pending__{id(cand)}",  # temp ID
                "betradar_id": cand.betradar_id or "",
                "home_team_name": cand.home_team,
                "away_team_name": cand.away_team,
                "start_time": cand.start_time,
                "competition_name": cand.competition,
                "external_ids": {cand.bk_slug: cand.external_id},
            }
            matcher.update_index(placeholder)

    logger.info(
        "[fuzzy:%s] %d candidates → %d updates, %d creates",
        sport_slug, len(candidates), len(updates), len(creates),
    )
    return updates, creates


# ─── Candidate builder helpers ────────────────────────────────────────────────

def match_dict_to_candidate(m: dict, bk_slug: str) -> MatchCandidate:
    """Convert a normalised harvester match dict into a MatchCandidate."""
    ext_id = (
        m.get(f"{bk_slug}_match_id") or
        m.get("b2b_match_id") or
        m.get("bt_match_id") or
        m.get("od_match_id") or
        m.get("sp_game_id") or
        m.get("external_id") or
        str(m.get("id", ""))
    )
    return MatchCandidate(
        bk_slug=bk_slug,
        external_id=str(ext_id),
        home_team=m.get("home_team") or m.get("home_team_name", ""),
        away_team=m.get("away_team") or m.get("away_team_name", ""),
        start_time=_parse_dt(m.get("start_time")),
        competition=m.get("competition") or m.get("competition_name"),
        betradar_id=m.get("betradar_id") or m.get("betradar_event_id"),
        country=m.get("country"),
        raw=m,
    )