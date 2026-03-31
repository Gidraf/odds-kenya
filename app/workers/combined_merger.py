"""
app/workers/combined_merger.py
===============================
Multi-bookmaker data merger + opportunity detector.

Merges SP / BT / OD matches into a single canonical CombinedMatch shape,
grouped by:
  1. betradar_id           (most reliable cross-book key)
  2. bt_parent_id / od_parent_id  (Sportradar-derived parent IDs)
  3. fuzzy  home+away+date  (last resort)

Then computes per-merged-row:
  • best odds per outcome per market
  • arbitrage (arb_sum < 1.0)
  • expected value (EV > threshold vs consensus fair price)
  • overround per bookmaker per market
  • sharp-money signals (steam moves: odds shortening below consensus)

Public API
----------
  merge_upcoming(sport_slug, sp_matches, bt_matches, od_matches) → list[CombinedMatch]
  merge_live(sport_slug, sp_matches, bt_matches, od_matches)     → list[CombinedMatch]
  compute_opportunities(combined: list[CombinedMatch], min_ev=3.0) → OpportunityReport
"""

from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# TYPES
# ─────────────────────────────────────────────────────────────────────────────

BK = str   # "sp" | "bt" | "od"
BOOKMAKERS: list[BK] = ["sp", "bt", "od"]

BK_LABELS = {"sp": "SportPesa", "bt": "Betika", "od": "OdiBets"}
BK_SHORT  = {"sp": "SP", "bt": "BT", "od": "OD"}


@dataclass
class BkOdds:
    bk:  BK
    odd: float
    match_id: str = ""


@dataclass
class OutcomeBest:
    best_odd: float
    best_bk:  BK
    all:      list[BkOdds] = field(default_factory=list)

    @property
    def spread_pct(self) -> float:
        """% spread between best and worst odds for this outcome."""
        if len(self.all) < 2:
            return 0.0
        odds = [x.odd for x in self.all]
        return round((max(odds) / min(odds) - 1) * 100, 2)


@dataclass
class ArbLeg:
    outcome:   str
    bk:        BK
    odd:       float
    stake_pct: float   # % of total stake to place here


@dataclass
class ArbResult:
    market_slug: str
    profit_pct:  float
    arb_sum:     float
    legs:        list[ArbLeg]

    def stake_breakdown(self, total: float = 1000.0) -> list[dict]:
        return [
            {
                "outcome":   leg.outcome,
                "bk":        leg.bk,
                "odd":       leg.odd,
                "stake_pct": leg.stake_pct,
                "stake_kes": round(total * leg.stake_pct / 100, 2),
                "return_kes": round(total * leg.stake_pct / 100 * leg.odd, 2),
            }
            for leg in self.legs
        ]


@dataclass
class EVResult:
    market_slug: str
    outcome:     str
    bk:          BK
    odd:         float
    fair_prob:   float
    ev_pct:      float
    kelly:       float      # full Kelly fraction
    half_kelly:  float


@dataclass
class SharpMove:
    market_slug: str
    outcome:     str
    bk:          BK
    direction:   str        # "steam_down" | "drift_up"
    delta:       float
    from_odd:    float
    to_odd:      float


@dataclass
class CombinedMatch:
    join_key:    str
    home_team:   str
    away_team:   str
    competition: str
    start_time:  str
    is_live:     bool

    # Per-bk match IDs
    bk_ids:           dict[BK, str]          = field(default_factory=dict)
    betradar_id:      str | None             = None

    # Live state
    match_time:  str | None = None
    score_home:  str | None = None
    score_away:  str | None = None

    # Markets: {bk → {slug → {outcome → odd}}}
    markets:     dict[BK, dict[str, dict[str, float]]] = field(default_factory=dict)

    # Derived: {slug → {outcome → OutcomeBest}}
    best:        dict[str, dict[str, OutcomeBest]] = field(default_factory=dict)

    # Opportunities
    arbs:        list[ArbResult]  = field(default_factory=list)
    evs:         list[EVResult]   = field(default_factory=list)
    sharp:       list[SharpMove]  = field(default_factory=list)

    # Metadata
    market_slugs:    list[str]  = field(default_factory=list)
    bk_count:        int        = 0
    has_arb:         bool       = False
    has_ev:          bool       = False
    has_sharp:       bool       = False
    best_arb_pct:    float      = 0.0
    best_ev_pct:     float      = 0.0

    def to_dict(self) -> dict:
        """Serialise to JSON-safe dict for SSE / REST responses."""
        return {
            "join_key":    self.join_key,
            "home_team":   self.home_team,
            "away_team":   self.away_team,
            "competition": self.competition,
            "start_time":  self.start_time,
            "is_live":     self.is_live,
            "match_time":  self.match_time,
            "score_home":  self.score_home,
            "score_away":  self.score_away,
            "betradar_id": self.betradar_id,
            "bk_ids":      self.bk_ids,
            "bk_count":    self.bk_count,
            "market_slugs": self.market_slugs,
            "markets":     self.markets,
            "best": {
                slug: {
                    out: {
                        "best_odd":    b.best_odd,
                        "best_bk":     b.best_bk,
                        "spread_pct":  b.spread_pct,
                        "all": [{"bk": x.bk, "odd": x.odd} for x in b.all],
                    }
                    for out, b in outcomes.items()
                }
                for slug, outcomes in self.best.items()
            },
            "arbs": [
                {
                    "market_slug": a.market_slug,
                    "profit_pct":  a.profit_pct,
                    "arb_sum":     a.arb_sum,
                    "legs": [
                        {"outcome": l.outcome, "bk": l.bk,
                         "odd": l.odd, "stake_pct": l.stake_pct}
                        for l in a.legs
                    ],
                    "breakdown_1000": a.stake_breakdown(1000),
                }
                for a in self.arbs
            ],
            "evs": [
                {
                    "market_slug": e.market_slug,
                    "outcome":     e.outcome,
                    "bk":          e.bk,
                    "odd":         e.odd,
                    "fair_prob":   round(e.fair_prob * 100, 2),
                    "ev_pct":      round(e.ev_pct, 2),
                    "kelly":       round(e.kelly * 100, 2),
                    "half_kelly":  round(e.half_kelly * 100, 2),
                }
                for e in self.evs
            ],
            "sharp": [
                {
                    "market_slug": s.market_slug,
                    "outcome":     s.outcome,
                    "bk":          s.bk,
                    "direction":   s.direction,
                    "delta":       s.delta,
                    "from_odd":    s.from_odd,
                    "to_odd":      s.to_odd,
                }
                for s in self.sharp
            ],
            "has_arb":      self.has_arb,
            "has_ev":       self.has_ev,
            "has_sharp":    self.has_sharp,
            "best_arb_pct": self.best_arb_pct,
            "best_ev_pct":  self.best_ev_pct,
        }


@dataclass
class OpportunityReport:
    total_matches:    int
    arb_count:        int
    ev_count:         int
    sharp_count:      int
    best_arb:         ArbResult | None
    best_ev:          EVResult | None
    total_arb_pct:    float   = 0.0   # sum of all profits
    computed_at:      str     = ""


# ─────────────────────────────────────────────────────────────────────────────
# JOIN KEY COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────

def _norm_team(name: str) -> str:
    """Normalise team name for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", name.lower().replace(" ", ""))


def make_join_key(raw: dict, bk: BK) -> str:
    """
    Derive a stable join key for a raw match dict.

    Priority:
      1. betradar_id  (cross-book Sportradar ID — most reliable)
      2. bt_parent_id / od_parent_id  (both come from Sportradar SRUID)
      3. fuzzy home+away+date
    """
    br = raw.get("betradar_id")
    if br and str(br).strip() not in ("", "None", "null", "0"):
        return f"br_{br}"

    if bk == "bt":
        pid = raw.get("bt_parent_id") or raw.get("parent_match_id")
        if pid:
            return f"bt_p_{pid}"
    if bk == "od":
        pid = raw.get("od_parent_id") or raw.get("parent_match_id")
        if pid:
            return f"od_p_{pid}"

    h = _norm_team(raw.get("home_team", ""))
    a = _norm_team(raw.get("away_team", ""))
    d = (raw.get("start_time") or "")[:10]
    return f"fuzzy_{h}_vs_{a}_{d}"


def get_bk_match_id(raw: dict, bk: BK) -> str:
    if bk == "sp":
        return str(raw.get("sp_game_id") or raw.get("game_id") or "")
    if bk == "bt":
        return str(raw.get("bt_match_id") or raw.get("match_id") or "")
    return str(raw.get("od_match_id") or raw.get("od_event_id") or raw.get("event_id") or "")


# ─────────────────────────────────────────────────────────────────────────────
# ODDS ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def compute_best(markets: dict[BK, dict[str, dict[str, float]]]) -> dict[str, dict[str, OutcomeBest]]:
    """For every market slug and outcome, find the best (highest) odds across bookmakers."""
    all_slugs: set[str] = set()
    for bk_mkts in markets.values():
        all_slugs.update(bk_mkts.keys())

    result: dict[str, dict[str, OutcomeBest]] = {}
    for slug in all_slugs:
        outcomes_best: dict[str, OutcomeBest] = {}
        all_outcomes: set[str] = set()
        for bk in BOOKMAKERS:
            all_outcomes.update((markets.get(bk) or {}).get(slug, {}).keys())

        for outcome in all_outcomes:
            bk_odds: list[BkOdds] = []
            for bk in BOOKMAKERS:
                odd = (markets.get(bk) or {}).get(slug, {}).get(outcome)
                if odd and odd > 1.0:
                    bk_odds.append(BkOdds(bk=bk, odd=odd))
            if not bk_odds:
                continue
            best = max(bk_odds, key=lambda x: x.odd)
            outcomes_best[outcome] = OutcomeBest(
                best_odd=best.odd, best_bk=best.bk, all=bk_odds
            )

        if len(outcomes_best) >= 2:
            result[slug] = outcomes_best

    return result


def compute_arb(
    best: dict[str, dict[str, OutcomeBest]],
    min_profit_pct: float = 0.05,
) -> list[ArbResult]:
    """Detect arbitrage: arb_sum < 1.0 across the best odds for each outcome."""
    results: list[ArbResult] = []
    for slug, outcomes in best.items():
        if len(outcomes) < 2:
            continue
        arb_sum = sum(1.0 / b.best_odd for b in outcomes.values())
        if arb_sum >= 1.0:
            continue
        profit_pct = (1.0 / arb_sum - 1.0) * 100
        if profit_pct < min_profit_pct:
            continue
        legs = [
            ArbLeg(
                outcome=out,
                bk=b.best_bk,
                odd=b.best_odd,
                stake_pct=round((1.0 / b.best_odd / arb_sum) * 100, 3),
            )
            for out, b in outcomes.items()
        ]
        results.append(ArbResult(
            market_slug=slug,
            profit_pct=round(profit_pct, 4),
            arb_sum=round(arb_sum, 6),
            legs=legs,
        ))
    return sorted(results, key=lambda a: -a.profit_pct)


def compute_ev(
    best: dict[str, dict[str, OutcomeBest]],
    markets: dict[BK, dict[str, dict[str, float]]],
    min_ev_pct: float = 3.0,
) -> list[EVResult]:
    """
    For each bookmaker and outcome, compare their price to the consensus fair price.
    Fair price = normalised implied prob of the BEST odds available.
    EV% = (bk_odd × fair_prob - 1) × 100
    """
    results: list[EVResult] = []
    for slug, outcomes in best.items():
        if len(outcomes) < 2:
            continue
        arb_sum = sum(1.0 / b.best_odd for b in outcomes.values())
        if arb_sum <= 0:
            continue

        # Normalised fair probabilities from best odds
        fair_probs = {out: (1.0 / b.best_odd) / arb_sum for out, b in outcomes.items()}

        for bk in BOOKMAKERS:
            bk_mkt = (markets.get(bk) or {}).get(slug, {})
            for out, odd in bk_mkt.items():
                if not odd or odd <= 1.0:
                    continue
                fair_p = fair_probs.get(out)
                if not fair_p:
                    continue
                ev_pct = (odd * fair_p - 1.0) * 100
                if ev_pct < min_ev_pct:
                    continue
                b_val = odd - 1.0
                kelly = max(0.0, (b_val * fair_p - (1.0 - fair_p)) / b_val) if b_val > 0 else 0.0
                results.append(EVResult(
                    market_slug=slug,
                    outcome=out,
                    bk=bk,
                    odd=round(odd, 3),
                    fair_prob=round(fair_p, 6),
                    ev_pct=round(ev_pct, 3),
                    kelly=round(kelly, 4),
                    half_kelly=round(kelly / 2, 4),
                ))

    return sorted(results, key=lambda e: -e.ev_pct)


def compute_sharp(
    best: dict[str, dict[str, OutcomeBest]],
    prev_odds_map: dict[str, float],   # key: f"{join_key}_{slug}_{out}_{bk}"
    join_key: str,
    min_delta: float = 0.02,
    steam_threshold: float = 0.98,   # odds < 98% of consensus = steam
) -> list[SharpMove]:
    """
    Detect sharp-money signals:
      steam_down — a book's odds for a selection shorten significantly below consensus
      drift_up   — odds drift upwards (money going elsewhere)
    """
    moves: list[SharpMove] = []
    for slug, outcomes in best.items():
        for out, best_data in outcomes.items():
            for bk_odds in best_data.all:
                pk = f"{join_key}_{slug}_{out}_{bk_odds.bk}"
                prev = prev_odds_map.get(pk)
                if prev is None or abs(bk_odds.odd - prev) < min_delta:
                    continue
                if bk_odds.odd < prev and bk_odds.odd < best_data.best_odd * steam_threshold:
                    moves.append(SharpMove(
                        market_slug=slug, outcome=out, bk=bk_odds.bk,
                        direction="steam_down",
                        delta=round(prev - bk_odds.odd, 3),
                        from_odd=prev, to_odd=bk_odds.odd,
                    ))
                elif bk_odds.odd > prev and bk_odds.odd > best_data.best_odd * (2.0 - steam_threshold):
                    moves.append(SharpMove(
                        market_slug=slug, outcome=out, bk=bk_odds.bk,
                        direction="drift_up",
                        delta=round(bk_odds.odd - prev, 3),
                        from_odd=prev, to_odd=bk_odds.odd,
                    ))
    return moves


# ─────────────────────────────────────────────────────────────────────────────
# VALUE BET DETECTOR  (price vs market consensus)
# ─────────────────────────────────────────────────────────────────────────────

def detect_value_bets(
    combined: list[CombinedMatch],
    min_ev_pct: float = 2.0,
) -> list[dict]:
    """
    Return a flat list of all EV+ bets across all matches, sorted by EV%.
    Useful for dashboard "top value bets" section.
    """
    value_bets: list[dict] = []
    for m in combined:
        for ev in m.evs:
            if ev.ev_pct >= min_ev_pct:
                value_bets.append({
                    "join_key":    m.join_key,
                    "home_team":   m.home_team,
                    "away_team":   m.away_team,
                    "competition": m.competition,
                    "start_time":  m.start_time,
                    "is_live":     m.is_live,
                    "market_slug": ev.market_slug,
                    "outcome":     ev.outcome,
                    "bk":          ev.bk,
                    "odd":         ev.odd,
                    "fair_prob":   round(ev.fair_prob * 100, 2),
                    "ev_pct":      ev.ev_pct,
                    "kelly":       round(ev.kelly * 100, 2),
                    "half_kelly":  round(ev.half_kelly * 100, 2),
                })
    return sorted(value_bets, key=lambda x: -x["ev_pct"])


# ─────────────────────────────────────────────────────────────────────────────
# MERGE ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class MultiBookMerger:
    """
    Stateful merger that tracks previous odds to enable sharp-money detection.
    Create once per stream session and call merge() on each data refresh.
    """

    def __init__(
        self,
        min_arb_profit: float = 0.05,
        min_ev_pct:     float = 3.0,
        sharp_min_delta: float = 0.02,
    ):
        self.min_arb_profit  = min_arb_profit
        self.min_ev_pct      = min_ev_pct
        self.sharp_min_delta = sharp_min_delta
        self._prev_odds: dict[str, float] = {}   # key → previous odd value

    def merge(
        self,
        sp_matches: list[dict],
        bt_matches: list[dict],
        od_matches: list[dict],
        is_live:    bool = False,
    ) -> list[CombinedMatch]:
        """
        Merge three lists of raw match dicts into a deduplicated list of CombinedMatch.
        Updates internal prev_odds state for sharp detection across calls.
        """
        # Phase 1: group by join key
        rows: dict[str, CombinedMatch] = {}

        for bk, raw_list in [("sp", sp_matches), ("bt", bt_matches), ("od", od_matches)]:
            for raw in (raw_list or []):
                jk = make_join_key(raw, bk)
                if jk not in rows:
                    rows[jk] = CombinedMatch(
                        join_key=jk,
                        home_team=raw.get("home_team", ""),
                        away_team=raw.get("away_team", ""),
                        competition=raw.get("competition", ""),
                        start_time=raw.get("start_time", ""),
                        is_live=is_live or bool(raw.get("is_live")),
                        betradar_id=str(raw.get("betradar_id") or ""),
                    )
                row = rows[jk]
                # Update live state from any bk that has it
                if raw.get("match_time"):
                    row.match_time = raw["match_time"]
                if raw.get("score_home") is not None:
                    row.score_home = str(raw["score_home"])
                if raw.get("score_away") is not None:
                    row.score_away = str(raw["score_away"])
                if not row.betradar_id and raw.get("betradar_id"):
                    row.betradar_id = str(raw["betradar_id"])
                if not row.competition and raw.get("competition"):
                    row.competition = raw["competition"]

                mid = get_bk_match_id(raw, bk)
                if mid:
                    row.bk_ids[bk] = mid

                # Merge markets
                row.markets.setdefault(bk, {})
                for slug, outcomes in (raw.get("markets") or {}).items():
                    row.markets[bk].setdefault(slug, {})
                    for out, odd in outcomes.items():
                        try:
                            fv = float(odd)
                        except (TypeError, ValueError):
                            continue
                        if fv > 1.0:
                            row.markets[bk][slug][out] = round(fv, 3)

        # Phase 2: compute derived fields for each row
        result: list[CombinedMatch] = []
        new_odds: dict[str, float] = {}

        for jk, row in rows.items():
            row.bk_count = len(row.bk_ids)

            # Track current odds for sharp detection
            for bk in BOOKMAKERS:
                for slug, outcomes in (row.markets.get(bk) or {}).items():
                    for out, odd in outcomes.items():
                        pk = f"{jk}_{slug}_{out}_{bk}"
                        new_odds[pk] = odd

            row.best   = compute_best(row.markets)
            row.arbs   = compute_arb(row.best, self.min_arb_profit)
            row.evs    = compute_ev(row.best, row.markets, self.min_ev_pct)
            row.sharp  = compute_sharp(row.best, self._prev_odds, jk, self.sharp_min_delta)

            row.market_slugs = sorted(row.best.keys())
            row.has_arb      = bool(row.arbs)
            row.has_ev       = bool(row.evs)
            row.has_sharp    = any(s.direction == "steam_down" for s in row.sharp)
            row.best_arb_pct = row.arbs[0].profit_pct  if row.arbs else 0.0
            row.best_ev_pct  = row.evs[0].ev_pct       if row.evs  else 0.0

            result.append(row)

        # Update previous odds state for next call (sharp detection)
        self._prev_odds.update(new_odds)

        return result

    def reset(self) -> None:
        """Reset sharp-detection state (call when switching sport or mode)."""
        self._prev_odds.clear()


# ─────────────────────────────────────────────────────────────────────────────
# OPPORTUNITY REPORT
# ─────────────────────────────────────────────────────────────────────────────

def compute_opportunities(
    combined: list[CombinedMatch],
    min_ev: float = 3.0,
) -> OpportunityReport:
    arbs_flat = [a for m in combined for a in m.arbs]
    evs_flat  = [e for m in combined for e in m.evs if e.ev_pct >= min_ev]
    sharp_flat = [s for m in combined for s in m.sharp if s.direction == "steam_down"]

    return OpportunityReport(
        total_matches=len(combined),
        arb_count=len(arbs_flat),
        ev_count=len(evs_flat),
        sharp_count=len(sharp_flat),
        best_arb=arbs_flat[0] if arbs_flat else None,
        best_ev=evs_flat[0]   if evs_flat  else None,
        total_arb_pct=round(sum(a.profit_pct for a in arbs_flat), 3),
        computed_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# CONVENIENCE FUNCTIONS  (used directly from Flask views)
# ─────────────────────────────────────────────────────────────────────────────

# Module-level singleton mergers (one per mode)
_upcoming_merger = MultiBookMerger(min_arb_profit=0.05, min_ev_pct=2.5)
_live_merger     = MultiBookMerger(min_arb_profit=0.01, min_ev_pct=2.0, sharp_min_delta=0.015)


def merge_upcoming(
    sp_matches: list[dict],
    bt_matches: list[dict],
    od_matches: list[dict],
) -> list[CombinedMatch]:
    return _upcoming_merger.merge(sp_matches, bt_matches, od_matches, is_live=False)


def merge_live(
    sp_matches: list[dict],
    bt_matches: list[dict],
    od_matches: list[dict],
) -> list[CombinedMatch]:
    return _live_merger.merge(sp_matches, bt_matches, od_matches, is_live=True)


def reset_mergers() -> None:
    """Call when switching sport to clear sharp-detection history."""
    _upcoming_merger.reset()
    _live_merger.reset()