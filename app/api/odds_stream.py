"""
app/api/odds_stream.py
======================
Unified SSE stream + REST endpoints for ALL 10 bookmakers.

Patches applied vs original
-----------------------------
1. _build_best() normalises outcome keys before comparing:
   yes/YES/Yes → "Yes", over/OV/ov → "Over", home → "1" etc.
   Duplicates (same normalised key, different BKs) merge by keeping higher odd.
   No more "Yes" + "yes" double rows.

2. _detect_arb() skips combos with duplicate normalised outcomes.

3. _normalise_markets() applied to all BK market data on ingest.
"""
from __future__ import annotations

import json
import re
import time
import logging
from functools import wraps
from itertools import combinations

from flask import Blueprint, Response, request, stream_with_context, g

log = logging.getLogger(__name__)

bp_stream  = Blueprint("odds_stream",        __name__, url_prefix="/api")
bp_monitor = Blueprint("odds_monitor_main",  __name__, url_prefix="/api/monitor")

_TIER_RANK = {"free": 0, "basic": 1, "pro": 2, "premium": 3, "admin": 4}
_LOCAL_BKS = {"sp", "bt", "od"}
_KEEPALIVE = 20
_CACHE_TTL = 300

_THREE_WAY_MARKETS = frozenset({
    "match_winner", "1x2", "moneyline", "first_half_1x2",
    "second_half_1x2", "draw_no_bet",
})

_HTFT_MARKETS = frozenset({"ht_ft", "half_time_full_time", "htft"})

ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby", "ice-hockey",
    "volleyball", "handball", "table-tennis", "baseball", "mma", "boxing",
    "darts", "american-football", "esoccer",
]

_BK_KEY_FORMATS: list[tuple[str, list[str]]] = [
    ("sp",        ["odds:sp:upcoming:{sport}",        "sp:upcoming:{sport}"]),
    ("bt",        ["odds:bt:upcoming:{sport}",        "bt:upcoming:{sport}"]),
    ("od",        ["odds:od:upcoming:{sport}",        "od:upcoming:{sport}"]),
    ("b2b",       ["odds:b2b:upcoming:{sport}",       "b2b:upcoming:{sport}"]),
    ("1xbet",     ["odds:1xbet:upcoming:{sport}",     "odds:b2b:1xbet:upcoming:{sport}",     "1xbet:upcoming:{sport}"]),
    ("22bet",     ["odds:22bet:upcoming:{sport}",     "odds:b2b:22bet:upcoming:{sport}",     "22bet:upcoming:{sport}"]),
    ("betwinner", ["odds:betwinner:upcoming:{sport}", "odds:b2b:betwinner:upcoming:{sport}", "betwinner:upcoming:{sport}"]),
    ("melbet",    ["odds:melbet:upcoming:{sport}",    "odds:b2b:melbet:upcoming:{sport}",    "melbet:upcoming:{sport}"]),
    ("megapari",  ["odds:megapari:upcoming:{sport}",  "odds:b2b:megapari:upcoming:{sport}",  "megapari:upcoming:{sport}"]),
    ("helabet",   ["odds:helabet:upcoming:{sport}",   "odds:b2b:helabet:upcoming:{sport}",   "helabet:upcoming:{sport}"]),
    ("paripesa",  ["odds:paripesa:upcoming:{sport}",  "odds:b2b:paripesa:upcoming:{sport}",  "paripesa:upcoming:{sport}"]),
]

_BK_KEY_FORMATS_LIVE: list[tuple[str, list[str]]] = [
    ("sp", ["odds:sp:live:{sport}", "sp:live:{sport}"]),
    ("bt", ["odds:bt:live:{sport}", "bt:live:{sport}"]),
    ("od", ["odds:od:live:{sport}", "od:live:{sport}"]),
]

# ─── Outcome normalisation tables ────────────────────────────────────────────

_NON_PLAYER = frozenset({
    "no_goal", "no_goalscorer", "none", "own_goal",
    "home_win", "away_win", "home_or_draw", "draw_or_away", "home_or_away",
    "first_half", "second_half", "full_time", "both_teams",
    "only_1", "only_2",
})

_DC_MAP = {
    "1x": "1X", "x2": "X2",
    "1_or_x": "1X", "x_or_2": "X2", "1_or_2": "12",
    "home_or_draw": "1X", "draw_or_away": "X2", "home_or_away": "12",
}

_SIMPLE_MAP = {
    "1": "1",   "home": "1",   "w1": "1",   "home_win": "1",
    "x": "X",   "draw": "X",   "tie": "X",
    "2": "2",   "away": "2",   "w2": "2",   "away_win": "2",
    "over": "Over",   "under": "Under",
    "ov":   "Over",   "un":    "Under",
    "yes":  "Yes",    "no":    "No",
    "odd":  "Odd",    "even":  "Even",
    "othr": "Other",  "other": "Other",
    "none": "None",
}

_HTFT_CONCAT = {
    "11": "1/1", "1x": "1/X", "12": "1/2",
    "x1": "X/1", "xx": "X/X", "x2": "X/2",
    "21": "2/1", "2x": "2/X", "22": "2/2",
}


def _norm_outcome(key: str, market: str = "") -> str:
    """Normalise a raw outcome key to a canonical string."""
    k  = key.strip()
    kl = k.lower()
    if kl in _DC_MAP:       return _DC_MAP[kl]
    if kl in _SIMPLE_MAP:   return _SIMPLE_MAP[kl]
    if re.match(r"^\d+:\d+$", k):  return k          # score
    if re.match(r"^\d+\+?$",  k):  return k          # exact count
    if re.match(r"^[12xX]/[12xX]$", k): return k     # HT/FT slash
    if kl in _HTFT_CONCAT and market in _HTFT_MARKETS:
        return _HTFT_CONCAT[kl]
    if "_" in kl and re.match(r"^[a-z][a-z_\-]{2,}$", kl) and kl not in _NON_PLAYER:
        parts = k.replace("-", " ").split("_")
        if len(parts) >= 2 and all(p.isalpha() for p in parts):
            return " ".join(p.capitalize() for p in parts)
    return k


def _get_price(p) -> float:
    if isinstance(p, (int, float)):     return float(p)
    if isinstance(p, dict):
        for fld in ("price", "odd", "odds", "best_price", "value"):
            if p.get(fld):
                try: return float(p[fld])
                except: pass
    try:    return float(p or 0)
    except: return 0.0


def _normalise_markets(markets: dict) -> dict:
    """Normalise all outcome keys in a markets dict, merging by highest odd."""
    if not markets or not isinstance(markets, dict): return markets or {}
    result = {}
    for mkt, outcomes in markets.items():
        if not isinstance(outcomes, dict): result[mkt] = outcomes; continue
        norm: dict = {}
        for raw_k, val in outcomes.items():
            can_k = _norm_outcome(str(raw_k), mkt)
            price = _get_price(val)
            if can_k not in norm:
                norm[can_k] = val
            elif price > _get_price(norm[can_k]):
                norm[can_k] = val
        result[mkt] = norm
    return result


# =============================================================================
# AUTH
# =============================================================================

def _auth_user():
    """
    Authenticate the request. Stores the resolved tier in Flask g.jwt_tier
    so _tier_rank() can read it without a DB round-trip.

    Tier priority: JWT claim > DB subscription_tier > DB tier > "basic"
    """
    from app.utils.customer_jwt_helpers import _decode_token
    from app.models.customer import Customer
    auth  = request.headers.get("Authorization", "")
    token = auth[7:] if auth.startswith("Bearer ") else None
    if not token:
        token = request.args.get("token", "").strip() or None
    if token:
        try:
            payload  = _decode_token(token)
            user     = Customer.query.get(int(payload["sub"]))
            if not user:
                return None
            jwt_tier = str(payload.get("tier") or "").strip()
            db_tier  = _get_user_tier(user)
            # JWT claim wins if it's a recognised tier
            g.jwt_tier = jwt_tier if jwt_tier in _TIER_RANK else db_tier
            log.debug("Auth uid=%s jwt_tier=%r db_tier=%r → g.jwt_tier=%r",
                      user.id, jwt_tier, db_tier, g.jwt_tier)
            return user
        except Exception as exc:
            log.warning("Token decode failed: %s", exc)
            return None
    api_key = request.headers.get("X-Api-Key", "").strip()
    if api_key:
        try:
            from app.models.api_key import ApiKey
            from app.models.customer import Customer as C
            ak   = ApiKey.query.filter_by(key=api_key, is_active=True).first()
            if not ak:
                return None
            user = C.query.get(ak.user_id)
            if not (user and user.is_active):
                return None
            g.jwt_tier = _get_user_tier(user)
            return user
        except:
            pass
    return None


def _get_user_tier(user) -> str:
    """Read tier from any of the field names the Customer model might use."""
    if not user: return "free"
    return (
        getattr(user, "subscription_tier", None) or
        getattr(user, "tier", None)              or
        getattr(user, "plan", None)              or
        "free"
    )

def _tier_rank(user) -> int:
    if not user: return -1
    # g.jwt_tier is set by _auth_user() from the JWT claim — most authoritative
    try:
        from flask import g as _g
        jwt_tier = getattr(_g, "jwt_tier", None)
        if jwt_tier and jwt_tier in _TIER_RANK:
            return _TIER_RANK[jwt_tier]
    except RuntimeError:
        pass  # outside request context (tests, CLI)
    return _TIER_RANK.get(_get_user_tier(user), 1)


def require_tier(min_tier: str):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            from app.api import _err
            user = _auth_user()
            if not user:              return _err("Authentication required", 401)
            if _tier_rank(user) < _TIER_RANK.get(min_tier, 99):
                return _err(f"{min_tier.title()} tier or higher required", 403)
            g.user = user
            return fn(*args, **kwargs)
        return wrapper
    return decorator


# =============================================================================
# REDIS
# =============================================================================

def _r():
    from app.workers.celery_tasks import _redis
    return _redis()


# =============================================================================
# DATA LAYER
# =============================================================================

def _read_key(r, patterns, sport):
    from app.workers.bandwidth_optimizer import redis_get_decompressed
    best = None
    for pat in patterns:
        key = pat.format(sport=sport)
        try:
            # Tries gz:key first, falls back to plain key
            data = redis_get_decompressed(r, key)
            if not data:
                continue
            matches = data.get("matches", []) if isinstance(data, dict) else data
            if matches and (best is None or len(matches) > len(best)):
                best = matches
        except: continue
    return best

def _get_unified_patched(mode: str, sport: str, force_refresh: bool = False) -> list[dict]:
    """
    Drop-in replacement for _get_unified in odds_stream.py.
    
    Upcoming: unchanged (pure Redis harvest keys).
    Live:     harvest keys + window service enrichment (score, phase, delay).
    """
    import json, time
    from app.workers.bandwidth_optimizer import redis_get_decompressed
 
    r = _r()
    unified_key = f"odds:unified:{mode}:{sport}"
 
    if not force_refresh:
        # For live, shorter cache (5s) since we want near-real-time
        ttl = 5 if mode == "live" else _CACHE_TTL
        try:
            raw = r.get(unified_key)
            if raw:
                data = json.loads(raw)
                age  = time.time() - float(data.get("updated_at", 0))
                if age < ttl:
                    matches = data.get("matches", [])
                    # Still enrich live matches even from cache (score changes every ~30s)
                    if mode == "live":
                        matches = _enrich_with_window_state(matches, r)
                    return matches
        except Exception:
            pass
 
    bk_formats = _BK_KEY_FORMATS_LIVE if mode == "live" else _BK_KEY_FORMATS
    merged = _merge_bks(r, sport, bk_formats)
 
    # For live mode: also pull from window service live set
    # (catches matches that window service knows are live but BK snapshots haven't updated)
    if mode == "live":
        merged = _enrich_with_window_state(merged, r)
        _inject_window_only_live(merged, r, sport)
 
    if merged:
        try:
            r.setex(unified_key, 3600, json.dumps({
                "mode": mode, "sport": sport,
                "match_count": len(merged), "updated_at": time.time(),
                "matches": merged,
            }, default=str))
        except Exception:
            pass
 
    return merged

def _merge_bks(r, sport: str, bk_formats: list[tuple[str, list[str]]]) -> list[dict]:
    result: list[dict] = []
    by_jk:  dict[str, int] = {}
    by_name: dict[str, int] = {}

    def jk(m: dict) -> str:
        # betradar_id is the most reliable cross-BK identifier
        return str(
            m.get("betradar_id") or
            m.get("join_key") or
            m.get("parent_match_id") or
            m.get("match_id") or ""
        )

    def nk(m: dict) -> str:
        h = (m.get("home_team") or m.get("home_team_name") or "").lower().strip()
        a = (m.get("away_team") or m.get("away_team_name") or "").lower().strip()
        # Use first word (most stable across BKs) truncated to 10 chars
        def first_word(t: str) -> str:
            t = t.replace(".", "").replace("-", " ").replace("_", " ")
            parts = t.split()
            return parts[0][:10] if parts else t[:10]
        hc = first_word(h); ac = first_word(a)
        return f"{hc}|{ac}" if hc and ac else ""

    for bk_slug, patterns in bk_formats:
        matches = _read_key(r, patterns, sport)
        if not matches: continue
        for m in matches:
            key_jk = jk(m);  key_nk = nk(m)
            pos = by_jk.get(key_jk) if key_jk else None
            if pos is None and key_nk: pos = by_name.get(key_nk)

            # Resolve markets — prefer bookmakers[bk].markets, fall back to top-level
            bk_bd = m.get("bookmakers", {}).get(bk_slug, {})
            mkts  = _normalise_markets(bk_bd.get("markets") or m.get("markets") or {})

            if pos is not None:
                ex = result[pos]
                ex.setdefault("bookmakers", {})[bk_slug] = {
                    "bookmaker": bk_slug.upper(), "slug": bk_slug, "markets": mkts,
                }
                # Also absorb any extra BKs already present on this match
                for xbk, xbd in (m.get("bookmakers") or {}).items():
                    if xbk == bk_slug: continue
                    xm = _normalise_markets(xbd.get("markets") or {})
                    if xm:
                        ex["bookmakers"].setdefault(xbk, {
                            "bookmaker": xbk.upper(), "slug": xbk, "markets": {},
                        })["markets"].update(xm)
                ex["bk_count"] = len(ex["bookmakers"])
                if not ex.get("competition") and m.get("competition"):
                    ex["competition"] = m["competition"]
            else:
                bks_seed: dict = {
                    bk_slug: {"bookmaker": bk_slug.upper(), "slug": bk_slug, "markets": mkts}
                }
                for xbk, xbd in (m.get("bookmakers") or {}).items():
                    if xbk == bk_slug: continue
                    xm = _normalise_markets(xbd.get("markets") or {})
                    if xm:
                        bks_seed[xbk] = {"bookmaker": xbk.upper(), "slug": xbk, "markets": xm}

                entry: dict = {
                    "match_id":          m.get("match_id") or key_jk,
                    "join_key":          key_jk,
                    "parent_match_id":   m.get("parent_match_id") or m.get("betradar_id") or key_jk,
                    "betradar_id":       m.get("betradar_id") or "",
                    "home_team":         m.get("home_team")  or m.get("home_team_name")  or "",
                    "away_team":         m.get("away_team")  or m.get("away_team_name")  or "",
                    "competition":       m.get("competition") or m.get("competition_name") or "",
                    "sport":             m.get("sport") or sport,
                    "start_time":        m.get("start_time") or "",
                    "status":            m.get("status") or "PRE_MATCH",
                    "is_live":           m.get("is_live", False),
                    "has_arb":           False,
                    "has_ev":            False,
                    "best_arb_pct":      0,
                    "arb_opportunities": [],
                    "market_slugs":      list(mkts.keys()),
                    "bookmakers":        bks_seed,
                    "bk_count":          len(bks_seed),
                }
                pos = len(result);  result.append(entry)
                if key_jk: by_jk[key_jk]   = pos
                if key_nk: by_name[key_nk] = pos

    for m in result:
        m["best"]              = _build_best(m["bookmakers"])
        has_arb, pct, arbs     = _detect_arb(m["best"])
        m["has_arb"]           = has_arb
        m["best_arb_pct"]      = pct
        m["arb_opportunities"] = arbs
        m["market_slugs"]      = list(m["best"].keys())

    return result


def _build_best(bookmakers: dict) -> dict:
    """
    Build best-price-per-outcome map across all bookmakers.
    Outcome keys are normalised so Yes/yes, OV/Over, home/1 all merge.
    """
    best: dict = {}
    for bk_slug, bd in bookmakers.items():
        for mkt, outcomes in (bd.get("markets") or {}).items():
            if not isinstance(outcomes, dict): continue
            best.setdefault(mkt, {})
            for raw_k, p in outcomes.items():
                can_k = _norm_outcome(str(raw_k), mkt)
                price = _get_price(p)
                if price <= 1.0: continue
                existing = best[mkt].get(can_k)
                if not existing or price > existing.get("odd", 0):
                    best[mkt][can_k] = {"odd": price, "bk": bk_slug}
    return best


def _detect_arb(best: dict) -> tuple[bool, float, list]:
    """
    Find ALL arb opportunities. Skips combos where legs have the same
    normalised outcome (prevents Yes+yes false-arb from stale cache data).
    """
    arbs: list[dict] = []
    seen: set[tuple] = set()

    for mkt, ob in best.items():
        keys = list(ob.keys())
        if len(keys) < 2: continue

        combos: list[tuple[str, ...]] = []
        if mkt in _THREE_WAY_MARKETS and len(keys) >= 3:
            trio = tuple(keys[:3])
            combos.append(trio)
            combos.extend(combinations(trio, 2))
        else:
            combos.extend(combinations(keys[:4], 2))

        for use in combos:
            # Guard: all outcomes must be distinct after normalisation
            norm_set = {_norm_outcome(k, mkt) for k in use}
            if len(norm_set) < len(use): continue

            bks = {ob[k]["bk"] for k in use if ob[k].get("bk")}
            if len(bks) < 2: continue

            odds = [ob[k]["odd"] for k in use]
            if any(o <= 1.0 for o in odds): continue

            inv = sum(1.0 / o for o in odds)
            if not (0 < inv < 1.0): continue

            pct = round((1.0 / inv - 1.0) * 100, 3)
            ck  = (mkt,) + tuple(sorted(use))
            if ck in seen: continue
            seen.add(ck)

            arbs.append({
                "market":     mkt,
                "combo":      " + ".join(use),
                "profit_pct": pct,
                "legs": [{"outcome": k, "odd": ob[k]["odd"], "bk": ob[k]["bk"],
                           "stake_pct": round((1.0 / ob[k]["odd"]) / inv, 4)} for k in use],
                "n_bks": len(bks),
            })

    if not arbs: return False, 0.0, []
    arbs.sort(key=lambda a: -a["profit_pct"])
    return True, arbs[0]["profit_pct"], arbs


def _slim(m: dict) -> dict:
    best = m.get("best", {})
    return {
        "match_id":          m["match_id"],
        "join_key":          m["join_key"],
        "parent_match_id":   m.get("parent_match_id", m["join_key"]),
        "home_team":         m["home_team"],
        "away_team":         m["away_team"],
        "competition":       m["competition"],
        "start_time":        m["start_time"],
        "is_live":           m["is_live"],
        "has_arb":           m["has_arb"],
        "best_arb_pct":      m["best_arb_pct"],
        "bk_count":          m["bk_count"],
        "market_slugs":      m.get("market_slugs", []),
        "bookmakers": {
            k: {"bookmaker": v["bookmaker"], "slug": v["slug"], "markets": {}}
            for k, v in (m.get("bookmakers") or {}).items()
        },
        "best": {
            "1x2":          best.get("1x2", {}),
            "match_winner": best.get("match_winner", {}),
            "moneyline":    best.get("moneyline", {}),
        },
        "arb_opportunities": m.get("arb_opportunities", []),
    }


def _filter_tier(matches: list[dict], tier: str) -> list[dict]:
    if tier in ("pro", "premium", "admin"): return matches
    out = []
    for m in matches:
        mc = {**m}
        bks = mc.get("bookmakers") or {}
        mc["bookmakers"] = {k: v for k, v in bks.items() if k in _LOCAL_BKS}
        mc["bk_count"]   = len(mc["bookmakers"])
        mc["best"]       = _build_best(mc["bookmakers"])
        has_arb, pct, arbs = _detect_arb(mc["best"])
        mc["has_arb"] = has_arb; mc["best_arb_pct"] = pct; mc["arb_opportunities"] = arbs
        out.append(mc)
    return out

def _enrich_with_window_state(matches: list[dict], r) -> list[dict]:
    """
    For live matches, overlay real-time state from MatchWindowService.
    This is a non-blocking read — if window keys don't exist, original data passes through.
    
    Adds/updates per match:
      - score_home, score_away, match_time (from kinetic:match:{jk}:score)
      - phase, live_since (from kinetic:match:{jk}:state)
      - has_delay, delay_minutes (from kinetic:match:{jk}:delay)
      - bk_consensus (from kinetic:match:{jk}:bk_live)
      - markets delta enrichment (from kinetic:match:{jk}:markets — latest per BK)
    """
    if not matches:
        return matches
 
    pipe = r.pipeline()
    for m in matches:
        jk = str(m.get("join_key") or m.get("betradar_id") or "")
        if not jk:
            continue
        pipe.hgetall(f"kinetic:match:{jk}:score")
        pipe.hgetall(f"kinetic:match:{jk}:state")
        pipe.hgetall(f"kinetic:match:{jk}:delay")
        pipe.hgetall(f"kinetic:match:{jk}:bk_live")
 
    try:
        results = pipe.execute()
    except Exception:
        return matches
 
    enriched = []
    for i, m in enumerate(matches):
        jk = str(m.get("join_key") or m.get("betradar_id") or "")
        if not jk:
            enriched.append(m)
            continue
 
        base = i * 4
        score  = results[base]     if base     < len(results) else {}
        state  = results[base + 1] if base + 1 < len(results) else {}
        delay  = results[base + 2] if base + 2 < len(results) else {}
        bk_lv  = results[base + 3] if base + 3 < len(results) else {}
 
        mc = {**m}
 
        if score:
            if score.get("home") is not None:
                mc["score_home"] = score["home"]
            if score.get("away") is not None:
                mc["score_away"] = score["away"]
            if score.get("time"):
                mc["match_time"] = score["time"]
 
        if state.get("phase"):
            mc["phase"]      = state["phase"]
            mc["live_since"] = state.get("live_since", "")
            mc["is_live"]    = state["phase"] == "live"
 
        if delay:
            mc["has_delay"]      = True
            mc["delay_minutes"]  = round(float(delay.get("delay_s", 0)) / 60, 1)
 
        if bk_lv:
            mc["bk_consensus"] = {bk: (v == "1") for bk, v in bk_lv.items()}
 
        enriched.append(mc)
 
    return enriched



def _inject_window_only_live(existing: list[dict], r, sport: str) -> None:
    """
    Check if MatchWindowService has live matches that aren't in BK snapshots yet.
    Adds them to `existing` in place. This handles the transition gap where
    the window service declares a match live before the next harvest cycle.
    """
    try:
        live_jks = r.smembers("kinetic:window:live") or set()
        existing_jks = {
            str(m.get("join_key") or m.get("betradar_id") or "")
            for m in existing
        }
 
        for jk in live_jks:
            if jk in existing_jks:
                continue
            meta  = r.hgetall(f"kinetic:match:{jk}:meta") or {}
            if meta.get("sport", "") != sport:
                continue
            score = r.hgetall(f"kinetic:match:{jk}:score") or {}
            state = r.hgetall(f"kinetic:match:{jk}:state") or {}
 
            existing.append({
                "match_id":        jk,
                "join_key":        jk,
                "parent_match_id": jk,
                "betradar_id":     jk,
                "home_team":       meta.get("home_team", ""),
                "away_team":       meta.get("away_team", ""),
                "competition":     meta.get("competition", ""),
                "sport":           sport,
                "start_time":      meta.get("start_time", ""),
                "status":          "live",
                "is_live":         True,
                "phase":           "live",
                "score_home":      score.get("home"),
                "score_away":      score.get("away"),
                "match_time":      score.get("time"),
                "live_since":      state.get("live_since", ""),
                "has_arb":         False,
                "best_arb_pct":    0,
                "arb_opportunities": [],
                "market_slugs":    [],
                "bookmakers":      {},
                "bk_count":        0,
                "best":            {},
                "source":          "window_service",
            })
    except Exception:
        pass
# =============================================================================
# SSE
# =============================================================================

def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"

def _make_generator(mode: str, sport: str, user, live_tier: bool, tier: str = "free"):
    # tier is now passed in — no longer read from Flask g here.
    # This makes the function testable and safe for anonymous callers.
 
    def generate():
        try:
            r = _r()
        except RuntimeError as exc:
            yield _sse("error", {"error": str(exc), "code": 503})
            return
 
        matches = _filter_tier(_get_unified_patched(mode, sport), tier)
 
        yield _sse("batch", {
            "matches": [_slim(m) for m in matches],
            "source":  "slim",
            "sport":   sport,
            "mode":    mode,
            "count":   len(matches),
            "tier":    tier,
        })
        yield _sse("batch", {
            "matches": matches,
            "source":  "full",
            "sport":   sport,
            "mode":    mode,
            "count":   len(matches),
            "tier":    tier,
        })
        yield _sse("connected", {
            "status":    "connected",
            "sport":     sport,
            "mode":      mode,
            "tier":      tier,
            "live_push": live_tier,
            "count":     len(matches),
        })
 
        if not live_tier:
            yield ": keepalive\n\n"
            return
 
        pubsub   = r.pubsub(ignore_subscribe_messages=True)
        channels = [
            f"odds:all:{mode}:{sport}:updates",
            f"arb:updates:{sport}",
            f"ev:updates:{sport}",
        ]
        if mode == "live":
            channels.append(f"bus:live_updates:{sport}")
        pubsub.subscribe(*channels)
        last_ka = time.time()
 
        try:
            while True:
                msg = pubsub.get_message(timeout=1.0)
                if msg and msg.get("type") == "message":
                    try:
                        payload = json.loads(msg["data"])
                        ch = msg.get("channel") or b""
                        if isinstance(ch, bytes):
                            ch = ch.decode()
                        if   "arb:"         in ch: yield _sse("arb_update",  payload)
                        elif "ev:"          in ch: yield _sse("ev_update",   payload)
                        elif "live_updates" in ch: yield _sse("live_update", payload)
                        else:
                            fresh = _filter_tier(
                                _get_unified_patched(mode, sport, force_refresh=True), tier
                            )
                            yield _sse("batch", {
                                "matches": fresh, "source": "live",
                                "sport":   sport, "mode":   mode,
                                "count":   len(fresh),
                            })
                    except Exception as exc:
                        log.debug("[stream] pubsub error: %s", exc)
 
                if time.time() - last_ka > _KEEPALIVE:
                    yield ": keepalive\n\n"
                    last_ka = time.time()
        finally:
            try:
                pubsub.unsubscribe(*channels)
                pubsub.close()
            except Exception:
                pass
 
    return generate

# =============================================================================
# PUBLISH HELPERS
# =============================================================================

def publish_harvest_done(bk_slug: str, sport: str, count: int):
    try:
        r = _r()
        r.delete(f"odds:unified:upcoming:{sport}")
        r.publish(f"odds:all:upcoming:{sport}:updates",
                  json.dumps({"event": "harvest_done", "bk": bk_slug,
                              "sport": sport, "count": count, "ts": time.time()}))
    except Exception as exc:
        log.warning("publish_harvest_done failed: %s", exc)


def publish_live_update(sport: str, match_id: str, join_key: str,
                        score_home=None, score_away=None,
                        match_time=None, is_live=None,
                        bookmakers: dict | None = None):
    try:
        r = _r()
        p: dict = {"match_id": match_id, "join_key": join_key}
        if score_home  is not None: p["score_home"]  = score_home
        if score_away  is not None: p["score_away"]  = score_away
        if match_time  is not None: p["match_time"]  = match_time
        if is_live     is not None: p["is_live"]     = is_live
        if bookmakers:              p["bookmakers"]  = bookmakers
        r.publish(f"bus:live_updates:{sport}", json.dumps(p))
    except Exception as exc:
        log.warning("publish_live_update failed: %s", exc)


def publish_arb_update(sport: str, join_key: str, match_id: str,
                       has_arb: bool, best_arb_pct: float, arb_opportunities: list):
    try:
        r = _r()
        r.publish(f"arb:updates:{sport}", json.dumps({
            "join_key": join_key, "match_id": match_id,
            "has_arb": has_arb, "best_arb_pct": best_arb_pct,
            "arb_opportunities": arb_opportunities, "ts": time.time(),
        }))
    except Exception as exc:
        log.warning("publish_arb_update failed: %s", exc)


# =============================================================================
# ROUTES
# =============================================================================

@bp_stream.route("/odds/stream/<mode>/<sport>", methods=["GET"])
def stream_odds(mode: str, sport: str):
    from app.api import _err
 
    if mode not in ("upcoming", "live"):
        return _err("mode must be 'upcoming' or 'live'", 400)
 
    # ── Auth — optional. Anonymous users get local BKs only. ─────────────────
    user = _auth_user()   # returns None for anonymous — that's fine now
 
    if user:
        # Logged-in: use JWT claim tier (most authoritative) or DB tier
        tier = getattr(g, "jwt_tier", None) or _get_user_tier(user)
    else:
        # Anonymous: free tier — _filter_tier will restrict to sp/bt/od
        tier = "free"
 
    # Only pro+ users get the live pubsub push loop
    live_tier = _TIER_RANK.get(tier, 0) >= _TIER_RANK["pro"]
 
    return Response(
        stream_with_context(
            _make_generator(mode, sport, user, live_tier, tier)()
        ),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":                "no-cache",
            "X-Accel-Buffering":            "no",
            "Connection":                   "keep-alive",
            "Access-Control-Allow-Origin":  "*",
            "Access-Control-Allow-Headers": "Authorization,Content-Type",
        },
    )

@bp_stream.route("/odds/snapshot/<mode>/<sport>", methods=["GET"])
@require_tier("free")               # ← was "basic"
def snapshot_odds(mode: str, sport: str):
    from app.api import _signed_response
    tier    = getattr(g, "jwt_tier", None) or _get_user_tier(g.user)
    matches = _filter_tier(_get_unified_patched(mode, sport), tier)
    return _signed_response({
        "matches": matches,
        "sport":   sport,
        "mode":    mode,
        "count":   len(matches),
    })

@bp_stream.route("/odds/page/<mode>/<sport>", methods=["GET"])
@require_tier("free")               # ← was "basic"
def paged_odds(mode: str, sport: str):
    from app.api import _signed_response
    tier     = getattr(g, "jwt_tier", None) or _get_user_tier(g.user)
    page     = max(1,   request.args.get("page",      1,   type=int))
    per_page = min(200, request.args.get("per_page", 100,  type=int))
    sort_by  = request.args.get("sort", "start_time")
    all_m    = _filter_tier(_get_unified_patched(mode, sport), tier)
    all_m.sort(
        key=lambda m: -(m.get("best_arb_pct") or 0)
        if sort_by == "arb"
        else (m.get("start_time") or "")
    )
    total  = len(all_m)
    offset = (page - 1) * per_page
    return _signed_response({
        "matches":  all_m[offset: offset + per_page],
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    -(-total // per_page),
        "has_more": (offset + per_page) < total,
        "sport":    sport,
        "mode":     mode,
    })
 
# =============================================================================
# MONITOR
# =============================================================================

@bp_monitor.route("/competitions", methods=["GET"])
def monitor_competitions():
    from app.api import _signed_response
    sport   = request.args.get("sport", "soccer")
    mode    = request.args.get("mode",  "upcoming")
    matches = _get_unified_patched(mode, sport)
    comps   = sorted({str(m.get("competition_name") or m.get("competition") or "").strip()
                      for m in matches if (m.get("competition_name") or m.get("competition"))} - {""})
    return _signed_response({"competitions": comps, "sport": sport, "mode": mode})


@bp_monitor.route("/stats", methods=["GET"])
def monitor_stats():
    from app.api import _signed_response
    r = _r(); stats: dict = {}
    for sport in ALL_SPORTS:
        for mode in ("upcoming", "live"):
            raw = r.get(f"odds:unified:{mode}:{sport}")
            if not raw: continue
            try:
                data    = json.loads(raw)
                matches = data.get("matches", []) if isinstance(data, dict) else data
                bk_seen: set[str] = set()
                for m in matches: bk_seen.update((m.get("bookmakers") or {}).keys())
                stats.setdefault(sport, {})[mode] = {
                    "count": len(matches), "bks": sorted(bk_seen), "bk_count": len(bk_seen),
                }
            except: pass
    return _signed_response({"stats": stats})


@bp_monitor.route("/redis-keys", methods=["GET"])
def monitor_redis_keys():
    from app.api import _signed_response
    sport = request.args.get("sport", "soccer"); r = _r()
    found: dict[str, int] = {}; missing: list[str] = []
    for _, patterns in _BK_KEY_FORMATS:
        for pat in patterns:
            key = pat.format(sport=sport)
            try:
                raw = r.get(key)
                if raw:
                    data = json.loads(raw)
                    found[key] = len(data.get("matches", data) if isinstance(data, dict) else data)
                else: missing.append(key)
            except Exception as exc: missing.append(f"{key} (err: {exc})")
    for k in [f"odds:unified:upcoming:{sport}", f"odds:unified:live:{sport}"]:
        try:
            raw = r.get(k)
            if raw: found[k] = len(json.loads(raw).get("matches", []))
            else: missing.append(k)
        except: missing.append(k)
    return _signed_response({"sport": sport, "found": found, "missing": missing,
                              "summary": f"{len(found)} keys, {sum(found.values())} matches"})


@bp_monitor.route("/warm-cache", methods=["GET", "POST"])
def warm_cache():
    from app.api import _signed_response
    t0 = time.time(); results = {}
    for sport in ALL_SPORTS:
        try:    results[sport] = len(_get_unified_patched("upcoming", sport, force_refresh=True))
        except Exception as e: results[sport] = f"error: {e}"
    return _signed_response({"warmed": results, "elapsed_s": round(time.time() - t0, 2)})


# =============================================================================
# LIFECYCLE
# =============================================================================

def _register_lifecycle(app) -> None:
    try:
        from app.workers.match_lifecycle import start_lifecycle_manager
        with app.app_context(): start_lifecycle_manager()
        log.info("MatchLifecycleManager started via _register_lifecycle()")
    except Exception as exc:
        log.warning("Could not start lifecycle manager: %s", exc)


@bp_stream.route("/odds/watch", methods=["POST"])
def watch_match_inline():
    from app.api import _err
    user = _auth_user()
    if not user: return _err("Authentication required", 401)
    try: from app.workers.match_lifecycle import WatchPrefs, get_lifecycle_manager
    except ImportError: return _err("Lifecycle module not available", 503)
    body = __import__("flask").request.get_json(silent=True) or {}
    prefs_data = body.get("prefs") or {}
    prefs = WatchPrefs(
        user_id=str(user.id),
        email=prefs_data.get("email") or getattr(user, "email", ""),
        phone=prefs_data.get("phone") or getattr(user, "phone", ""),
        webhook_url=prefs_data.get("webhook_url", ""),
        channels=prefs_data.get("channels") or ["websocket", "pubsub"],
        notify_on=prefs_data.get("notify_on") or [
            "pre_start", "started", "suspended", "goal", "finished", "arb_found",
        ],
    )
    mgr = get_lifecycle_manager(); saved = mgr.save_match(body.get("match") or {}, prefs)
    from app.api import _signed_response
    return _signed_response({"ok": True, "watch": saved.to_dict()}), 201