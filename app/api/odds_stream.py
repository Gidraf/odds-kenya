"""
app/api/odds_stream.py  v2
===========================
AUTH: Disabled — all endpoints open, tier = pro.

Fixes in this version:
  1. _read_key()        — reads paginated Redis LIST keys + plain string keys
  2. _get_unified_patched() — skips empty cache (stops cache poisoning)
  3. _detect_arb()      — try/except fallback if arb_engine missing
  4. _merge_bks()       — arb detection wrapped per-match so one crash
                          doesn't wipe all 2000 matches
  5. SSE full batch     — strips raw per-BK markets (can be 50KB per match)
                          so total payload stays under 500KB
  6. /odds/match/<id>/markets — on-demand full markets for MatchDetail
  7. /monitor/debug-stream/<sport> — diagnostic endpoint
  8. Token errors       — debug level only (no log spam)
  9. sport_unavailable  — SSE event with reason when sport has no data
"""
from __future__ import annotations

import json
import re
import time
import logging
from functools import wraps

from flask import Blueprint, Response, request, stream_with_context, g

log = logging.getLogger(__name__)

bp_stream  = Blueprint("odds_stream",       __name__, url_prefix="/api")
bp_monitor = Blueprint("odds_monitor_main", __name__, url_prefix="/api/monitor")

_TIER_RANK = {"free": 0, "basic": 1, "pro": 2, "premium": 3, "admin": 4}
_LOCAL_BKS = {"sp", "bt", "od"}
_KEEPALIVE  = 20
_CACHE_TTL  = 300

_HTFT_MARKETS = frozenset({"ht_ft", "half_time_full_time", "htft"})

ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby", "ice-hockey",
    "volleyball", "handball", "table-tennis", "baseball", "mma", "boxing",
    "darts", "american-football", "esoccer",
]

_BK_KEY_FORMATS: list[tuple[str, list[str]]] = [
    ("sp", [
        "odds:sp:upcoming:{sport}",
        "sp:upcoming:{sport}",
    ]),
    ("bt", [
        "odds:bt:upcoming:{sport}",
        "bt:upcoming:{sport}",
    ]),
    ("od", [
        "odds:od:upcoming:{sport}",
        "od:upcoming:{sport}",
    ]),
    ("1xbet", [
        "odds:1xbet:upcoming:{sport}",
        "odds:b2b:1xbet:upcoming:{sport}",
        "1xbet:upcoming:{sport}",
    ]),
    ("22bet", [
        "odds:22bet:upcoming:{sport}",
        "odds:b2b:22bet:upcoming:{sport}",
        "22bet:upcoming:{sport}",
    ]),
    ("betwinner", [
        "odds:betwinner:upcoming:{sport}",
        "odds:b2b:betwinner:upcoming:{sport}",
        "betwinner:upcoming:{sport}",
    ]),
    ("melbet", [
        "odds:melbet:upcoming:{sport}",
        "odds:b2b:melbet:upcoming:{sport}",
        "melbet:upcoming:{sport}",
    ]),
    ("megapari", [
        "odds:megapari:upcoming:{sport}",
        "odds:b2b:megapari:upcoming:{sport}",
        "megapari:upcoming:{sport}",
    ]),
    ("helabet", [
        "odds:helabet:upcoming:{sport}",
        "odds:b2b:helabet:upcoming:{sport}",
        "helabet:upcoming:{sport}",
    ]),
    ("paripesa", [
        "odds:paripesa:upcoming:{sport}",
        "odds:b2b:paripesa:upcoming:{sport}",
        "paripesa:upcoming:{sport}",
    ]),
]

_BK_KEY_FORMATS_LIVE: list[tuple[str, list[str]]] = [
    ("sp", ["odds:sp:live:{sport}", "sp:live:{sport}"]),
    ("bt", ["odds:bt:live:{sport}", "bt:live:{sport}"]),
    ("od", ["odds:od:live:{sport}", "od:live:{sport}"]),
]

# ── Outcome normalisation ──────────────────────────────────────────────────────

_NON_PLAYER = frozenset({
    "no_goal", "no_goalscorer", "none", "own_goal",
    "home_win", "away_win", "home_or_draw", "draw_or_away", "home_or_away",
    "first_half", "second_half", "full_time", "both_teams", "only_1", "only_2",
})
_DC_MAP = {
    "1x": "1X", "x2": "X2",
    "1_or_x": "1X", "x_or_2": "X2", "1_or_2": "12",
    "home_or_draw": "1X", "draw_or_away": "X2", "home_or_away": "12",
}
_SIMPLE_MAP = {
    "1": "1",  "home": "1",  "w1": "1",  "home_win": "1",
    "x": "X",  "draw": "X",  "tie": "X",
    "2": "2",  "away": "2",  "w2": "2",  "away_win": "2",
    "over": "Over",  "under": "Under",
    "ov":   "Over",  "un":    "Under",
    "yes":  "Yes",   "no":    "No",
    "odd":  "Odd",   "even":  "Even",
    "othr": "Other", "other": "Other",
    "none": "None",
}
_HTFT_CONCAT = {
    "11": "1/1", "1x": "1/X", "12": "1/2",
    "x1": "X/1", "xx": "X/X", "x2": "X/2",
    "21": "2/1", "2x": "2/X", "22": "2/2",
}


def _norm_outcome(key: str, market: str = "") -> str:
    k  = key.strip()
    kl = k.lower()
    if kl in _DC_MAP:     return _DC_MAP[kl]
    if kl in _SIMPLE_MAP: return _SIMPLE_MAP[kl]
    if re.match(r"^\d+:\d+$",       k): return k
    if re.match(r"^\d+\+?$",        k): return k
    if re.match(r"^[12xX]/[12xX]$", k): return k
    if kl in _HTFT_CONCAT and market in _HTFT_MARKETS:
        return _HTFT_CONCAT[kl]
    if "_" in kl and re.match(r"^[a-z][a-z_\-]{2,}$", kl) and kl not in _NON_PLAYER:
        parts = k.replace("-", " ").split("_")
        if len(parts) >= 2 and all(p.isalpha() for p in parts):
            return " ".join(p.capitalize() for p in parts)
    return k


def _get_price(p) -> float:
    if isinstance(p, (int, float)): return float(p)
    if isinstance(p, dict):
        for fld in ("price", "odd", "odds", "best_price", "value"):
            if p.get(fld):
                try: return float(p[fld])
                except: pass
    try:    return float(p or 0)
    except: return 0.0


def _normalise_markets(markets: dict) -> dict:
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
# AUTH — DISABLED
# =============================================================================

def _auth_user():
    try:
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
                if not user: return None
                jwt_tier = str(payload.get("tier") or "").strip()
                db_tier  = _get_user_tier(user)
                g.jwt_tier = jwt_tier if jwt_tier in _TIER_RANK else db_tier
                return user
            except Exception as exc:
                log.debug("Token decode (open access): %s", exc)
                return None
    except Exception:
        pass
    return None


def _get_user_tier(user) -> str:
    if not user: return "pro"
    return (
        getattr(user, "subscription_tier", None) or
        getattr(user, "tier", None)              or
        getattr(user, "plan", None)              or
        "pro"
    )


def _tier_rank(user) -> int:
    return _TIER_RANK["pro"]


def require_tier(min_tier: str):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            g.user = None
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
# KEY READERS  (paginated LIST + plain string)
# =============================================================================

def _page_num(key) -> int:
    k = key.decode() if isinstance(key, bytes) else key
    try:    return int(k.rsplit(":", 1)[-1])
    except: return 0


def _decode_key(key) -> str:
    return key.decode("utf-8") if isinstance(key, bytes) else key


def _decode_type(t) -> str:
    return t.decode("utf-8") if isinstance(t, bytes) else (t or "")


def _read_list_key(r, key: str) -> list:
    """Read a Redis LIST key — each element is a JSON match or list of matches."""
    matches = []
    try:
        items = r.lrange(key, 0, -1)
        for raw in items:
            if not raw: continue
            try:
                obj = json.loads(raw)
                if isinstance(obj, list):
                    matches.extend(obj)
                elif isinstance(obj, dict) and (obj.get("home_team") or obj.get("match_id")):
                    matches.append(obj)
            except Exception:
                pass
    except Exception as exc:
        log.debug("_read_list_key %s: %s", key, exc)
    return matches


def _read_string_key(r, key: str) -> list:
    """Read a Redis STRING key — JSON list or dict with 'matches' field."""
    try:
        raw = r.get(key)
        if not raw: return []
        obj = json.loads(raw)
        if isinstance(obj, list):
            return [m for m in obj if isinstance(m, dict)]
        if isinstance(obj, dict):
            ms = obj.get("matches") or obj.get("data") or []
            if isinstance(ms, list):
                return [m for m in ms if isinstance(m, dict) and
                        (m.get("home_team") or m.get("match_id") or m.get("home_team_name"))]
        return []
    except Exception as exc:
        log.debug("_read_string_key %s: %s", key, exc)
        return []


def _read_key(r, patterns: list[str], sport: str) -> list | None:
    """
    Try each pattern in order:
      1. Compressed key  (bandwidth_optimizer gz: prefix)
      2. Plain string key (JSON dict/list)
      3. Key as Redis LIST type
      4. Paginated LIST keys  (base_key:page:1, :page:2 …)

    Returns the largest non-empty list found, or None.
    """
    best: list | None = None

    for pat in patterns:
        base_key = pat.format(sport=sport)
        matches:  list = []

        # 1. Compressed key
        try:
            from app.workers.bandwidth_optimizer import redis_get_decompressed
            data = redis_get_decompressed(r, base_key)
            if data:
                if isinstance(data, list):
                    matches = [m for m in data if isinstance(m, dict)]
                elif isinstance(data, dict):
                    raw_m = data.get("matches") or data.get("data") or []
                    matches = [m for m in raw_m if isinstance(m, dict) and
                               (m.get("home_team") or m.get("match_id"))]
        except Exception:
            pass

        # 2. Plain string key
        if not matches:
            matches = _read_string_key(r, base_key)

        # 3. Plain key as LIST type
        if not matches:
            try:
                kt = _decode_type(r.type(base_key))
                if kt == "list":
                    matches = _read_list_key(r, base_key)
            except Exception:
                pass

        # 4. Paginated page keys
        if not matches:
            page_keys = []   # declared before try so UnboundLocalError can't occur
            try:
                page_keys = r.keys(f"{base_key}:page:*")
            except Exception:
                page_keys = []

            if page_keys:
                paged: list = []
                for pk in sorted(page_keys, key=_page_num):
                    pk_str = _decode_key(pk)
                    try:
                        kt = _decode_type(r.type(pk_str))
                    except Exception:
                        kt = ""
                    if kt == "list":
                        paged.extend(_read_list_key(r, pk_str))
                    else:
                        paged.extend(_read_string_key(r, pk_str))
                matches = paged

        if matches and (best is None or len(matches) > len(best)):
            best = matches

    return best


# =============================================================================
# UNIFIED DATA LAYER
# =============================================================================

_UNIFIED_CACHE_TTL   = 86400   # 24h — survives overnight without harvesters
_STALE_FALLBACK_TTL  = 86400   # also 24h — always serve stale if BK keys expire
_LIVE_CACHE_TTL      = 30      # live mode: re-read every 30s (scores change)


def _get_unified_patched(mode: str, sport: str, force_refresh: bool = False) -> list[dict]:
    r           = _r()
    unified_key = f"odds:unified:{mode}:{sport}"

    # Read cached data upfront — used for both fresh serve and stale fallback
    cached_matches: list = []
    cached_age: float    = 9999.0
    try:
        raw = r.get(unified_key)
        if raw:
            data           = json.loads(raw)
            cached_age     = time.time() - float(data.get("updated_at", 0))
            cached_matches = data.get("matches", [])
    except Exception:
        pass

    # ── Serve from cache if fresh and non-empty ───────────────────────────────
    if not force_refresh and cached_matches:
        ttl = _LIVE_CACHE_TTL if mode == "live" else _CACHE_TTL
        if cached_age < ttl:
            return cached_matches

    # ── Try to rebuild from BK Redis keys ─────────────────────────────────────
    bk_formats = _BK_KEY_FORMATS_LIVE if mode == "live" else _BK_KEY_FORMATS
    merged     = _merge_bks(r, sport, bk_formats, is_live_mode=(mode == "live"))

    if merged:
        try:
            r.setex(unified_key, _UNIFIED_CACHE_TTL, json.dumps({
                "mode":        mode,
                "sport":       sport,
                "match_count": len(merged),
                "updated_at":  time.time(),
                "matches":     merged,
            }, default=str))
            log.info("[unified] cached %d %s/%s matches", len(merged), mode, sport)
        except Exception as exc:
            log.warning("[unified] cache write failed %s/%s: %s", mode, sport, exc)
        return merged

    # ── BK keys expired — serve stale unified cache ───────────────────────────
    if cached_matches and cached_age < _STALE_FALLBACK_TTL:
        log.warning(
            "[unified] BK keys empty for %s/%s — serving stale cache "
            "(age=%.0fs, %d matches)",
            mode, sport, cached_age, len(cached_matches)
        )
        return cached_matches

    log.warning("[unified] 0 matches and no stale cache for %s/%s", mode, sport)
    return []


def _merge_bks(r, sport: str, bk_formats: list[tuple[str, list[str]]],
               is_live_mode: bool = False) -> list[dict]:
    result:  list[dict] = []
    by_jk:   dict[str, int] = {}
    by_name: dict[str, int] = {}

    def jk(m: dict) -> str:
        return str(
            m.get("betradar_id") or m.get("join_key") or
            m.get("parent_match_id") or m.get("match_id") or ""
        )

    def nk(m: dict) -> str:
        h = (m.get("home_team") or m.get("home_team_name") or "").lower().strip()
        a = (m.get("away_team") or m.get("away_team_name") or "").lower().strip()
        def fw(t: str) -> str:
            t = t.replace(".", "").replace("-", " ").replace("_", " ")
            p = t.split()
            return p[0][:10] if p else t[:10]
        hc = fw(h); ac = fw(a)
        return f"{hc}|{ac}" if hc and ac else ""

    for bk_slug, patterns in bk_formats:
        raw_matches = _read_key(r, patterns, sport)
        if not raw_matches:
            log.debug("[merge_bks] %s/%s: no data", bk_slug, sport)
            continue

        log.debug("[merge_bks] %s/%s: %d raw matches", bk_slug, sport, len(raw_matches))

        for m in raw_matches:
            if not isinstance(m, dict):
                continue
            key_jk = jk(m); key_nk = nk(m)
            pos = by_jk.get(key_jk) if key_jk else None
            if pos is None and key_nk:
                pos = by_name.get(key_nk)

            bk_bd = m.get("bookmakers", {}).get(bk_slug, {})
            mkts  = _normalise_markets(bk_bd.get("markets") or m.get("markets") or {})

            # Extract bookmaker-specific external match ID / Game ID (the "king" SMS ID)
            ext_id = str(
                m.get("sms_id") or m.get("sp_game_id") or m.get("sp_api_id") or
                m.get("bt_parent_id") or m.get("bt_match_id") or m.get("bt_game_id") or
                m.get("od_parent_id") or m.get("od_event_id") or m.get("od_match_id") or m.get("od_game_id") or
                m.get("match_id") or m.get("game_id") or m.get("event_id") or ""
            ).strip()

            bk_ids_seed = {}
            for k, v in (m.get("bk_ids") or {}).items():
                if v and str(v).lower() != "none":
                    bk_ids_seed[k] = str(v)
            if ext_id and ext_id.lower() != "none":
                bk_ids_seed[bk_slug] = ext_id

            if pos is not None:
                ex = result[pos]
                ex.setdefault("bookmakers", {})[bk_slug] = {
                    "bookmaker": bk_slug.upper(), "slug": bk_slug, "markets": mkts,
                }
                # Merge bk_ids dictionary
                ex_ids = ex.setdefault("bk_ids", {})
                for k, v in bk_ids_seed.items():
                    if v and str(v).lower() != "none":
                        ex_ids[k] = v

                # Expose root-level Game/SMS ID keys for seamless backward compatibility with UI templates
                if ex_ids.get("sp"):
                    ex["sp_game_id"] = str(ex_ids["sp"])
                    ex["sms_id"] = str(ex_ids["sp"])
                if ex_ids.get("bt"):
                    ex["bt_game_id"] = str(ex_ids["bt"])
                if ex_ids.get("od"):
                    ex["od_game_id"] = str(ex_ids["od"])

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
                # In live mode, trust the live harvester's score/time fields
                if is_live_mode:
                    for fld in ("score_home", "score_away", "match_time"):
                        if m.get(fld) is not None:
                            ex[fld] = m[fld]
            else:
                bks_seed: dict = {
                    bk_slug: {"bookmaker": bk_slug.upper(), "slug": bk_slug, "markets": mkts}
                }
                for xbk, xbd in (m.get("bookmakers") or {}).items():
                    if xbk == bk_slug: continue
                    xm = _normalise_markets(xbd.get("markets") or {})
                    if xm:
                        bks_seed[xbk] = {"bookmaker": xbk.upper(), "slug": xbk, "markets": xm}

                # ── is_live fix ────────────────────────────────────────────────
                # Upcoming harvesters sometimes mark in-progress matches as
                # is_live=True. Only trust this flag when we're actually reading
                # from the live harvest keys (is_live_mode=True).
                # For upcoming mode, always False — the live tab handles live.
                is_live_val = bool(m.get("is_live", False)) if is_live_mode else False

                # Expose root-level Game/SMS ID keys for seamless backward compatibility with UI templates
                sp_game_id_val = bk_ids_seed.get("sp")
                bt_game_id_val = bk_ids_seed.get("bt")
                od_game_id_val = bk_ids_seed.get("od")

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
                    "is_live":           is_live_val,
                    "score_home":        m.get("score_home") if is_live_mode else None,
                    "score_away":        m.get("score_away") if is_live_mode else None,
                    "match_time":        m.get("match_time") if is_live_mode else None,
                    "has_arb":           False,
                    "has_ev":            False,
                    "best_arb_pct":      0,
                    "arb_opportunities": [],
                    "market_slugs":      list(mkts.keys()),
                    "bookmakers":        bks_seed,
                    "bk_count":          len(bks_seed),
                    "bk_ids":            {k: v for k, v in bk_ids_seed.items() if v and str(v).lower() != "none"},
                    "sp_game_id":        str(sp_game_id_val) if sp_game_id_val else "",
                    "bt_game_id":        str(bt_game_id_val) if bt_game_id_val else "",
                    "od_game_id":        str(od_game_id_val) if od_game_id_val else "",
                    "sms_id":            str(sp_game_id_val) if sp_game_id_val else "",
                }
                pos = len(result); result.append(entry)
                if key_jk: by_jk[key_jk]   = pos
                if key_nk: by_name[key_nk] = pos

    # Build best odds + arb detection — wrapped per match so one failure
    # never wipes the entire result list
    for m in result:
        try:
            m["best"] = _build_best(m["bookmakers"])
        except Exception as exc:
            log.debug("[merge_bks] _build_best failed %s: %s", m.get("join_key"), exc)
            m["best"] = {}

        try:
            has_arb, pct, arbs = _detect_arb(m["best"])
            m["has_arb"]           = has_arb
            m["best_arb_pct"]      = pct
            m["arb_opportunities"] = arbs
        except Exception as exc:
            log.debug("[merge_bks] arb failed %s: %s", m.get("join_key"), exc)
            m["has_arb"]           = False
            m["best_arb_pct"]      = 0
            m["arb_opportunities"] = []

        m["market_slugs"] = list(m["best"].keys())

    log.info("[merge_bks] sport=%s merged=%d matches", sport, len(result))
    return result


def _build_best(bookmakers: dict) -> dict:
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
    """Detect arb — tries arb_engine first, falls back to inline 2-way scan."""
    try:
        from app.workers.arb_engine import detect_arb_for_stream
        return detect_arb_for_stream(best)
    except (ImportError, AttributeError):
        pass
    except Exception as exc:
        log.debug("[detect_arb] arb_engine error: %s", exc)

    # Inline fallback: simple 2-way scan
    for mkt, ob in best.items():
        if not isinstance(ob, dict) or len(ob) < 2:
            continue
        keys = [k for k, v in ob.items() if isinstance(v, dict) and v.get("odd", 0) > 1]
        if len(keys) < 2:
            continue
        odds    = [ob[k]["odd"] for k in keys[:3]]
        sum_inv = sum(1 / o for o in odds)
        if 0 < sum_inv < 1.0:
            pct  = round((1 - sum_inv) * 100, 3)
            legs = [{"outcome": k, "odd": ob[k]["odd"], "bk": ob[k].get("bk")}
                    for k in keys[:3]]
            return True, pct, [{"market": mkt, "profit_pct": pct, "legs": legs}]
    return False, 0.0, []


# =============================================================================
# SLIM / STRIP / FILTER
# =============================================================================

def _slim(m: dict) -> dict:
    """Minimal payload for fast initial render — no raw markets."""
    best = m.get("best") or {}
    return {
        "match_id":          m.get("match_id"),
        "join_key":          m.get("join_key"),
        "parent_match_id":   m.get("parent_match_id") or m.get("join_key"),
        "home_team":         m.get("home_team"),
        "away_team":         m.get("away_team"),
        "competition":       m.get("competition"),
        "start_time":        m.get("start_time"),
        "is_live":           m.get("is_live", False),
        "has_arb":           m.get("has_arb", False),
        "best_arb_pct":      m.get("best_arb_pct", 0.0),
        "bk_count":          m.get("bk_count", 0),
        "market_slugs":      m.get("market_slugs", []),
        "bookmakers": {
            k: {"bookmaker": v.get("bookmaker", k.upper()), "slug": v.get("slug", k), "markets": {}}
            for k, v in (m.get("bookmakers") or {}).items() if isinstance(v, dict)
        },
        "best": {
            "1x2":          best.get("1x2", {}),
            "match_winner": best.get("match_winner", {}),
            "moneyline":    best.get("moneyline", {}),
        },
        "arb_opportunities": m.get("arb_opportunities", []),
    }


def _strip_markets(m: dict) -> dict:
    """
    Full match data but with raw per-BK markets removed.
    Keeps `best` odds (already computed). Prevents huge SSE payloads.
    Raw markets are fetched on-demand via /api/odds/match/<id>/markets.
    """
    out = {**m}
    if "bookmakers" in out:
        out["bookmakers"] = {
            k: {
                "bookmaker": v.get("bookmaker", k.upper()),
                "slug":      v.get("slug", k),
                "markets":   {},      # stripped — fetch on-demand
            }
            for k, v in out["bookmakers"].items()
            if isinstance(v, dict)
        }
    return out


def _filter_tier(matches: list[dict], tier: str) -> list[dict]:
    # AUTH DISABLED — all matches returned as-is
    return matches


def _sport_unavailable_payload(sport: str) -> dict | None:
    try:
        from app.workers.bk_sport_config import (
            SP_SPORTS, BT_SPORTS, OD_SPORTS, B2B_SPORTS, bks_for_sport
        )
        covering = bks_for_sport(sport)
        local    = set(SP_SPORTS) | set(BT_SPORTS) | set(OD_SPORTS)
        b2b_set  = set(B2B_SPORTS)
        if sport not in b2b_set and sport not in local:
            return {"sport": sport, "reason": "not_covered",
                    "message": f"'{sport}' is not covered by any bookmaker.",
                    "covering_bks": [], "upgrade_required": False}
        if sport not in local and sport in b2b_set:
            return {"sport": sport, "reason": "b2b_only_no_data",
                    "message": f"{sport.title()} data comes from international BKs. Harvesters may still be running.",
                    "covering_bks": covering, "upgrade_required": False}
        return {"sport": sport, "reason": "no_data",
                "message": f"No {sport} matches in Redis yet. Harvesters running — try again in a few minutes.",
                "covering_bks": covering, "upgrade_required": False}
    except ImportError:
        return {"sport": sport, "reason": "no_data",
                "message": f"No {sport} matches available yet."}


# _enrich_with_window_state and _inject_window_only_live removed.
# Window service is disabled — LiveFeedBridge handles live state via
# SP WebSocket (writes to kinetic:match:{jk}:score directly).


# =============================================================================
# SSE
# =============================================================================

def _sse(event: str, data) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _make_generator(mode: str, sport: str, user, live_tier: bool, tier: str = "pro"):

    def generate():
        try:
            r = _r()
        except RuntimeError as exc:
            yield _sse("error", {"error": str(exc), "code": 503})
            return

        all_matches = _get_unified_patched(mode, sport)
        matches     = _filter_tier(all_matches, tier)

        if not matches:
            payload = _sport_unavailable_payload(sport)
            if payload:
                yield _sse("sport_unavailable", payload)

        # ── Slim batch — fast initial render (small payload) ──────────────
        yield _sse("batch", {
            "matches": [_slim(m) for m in matches],
            "source":  "slim",
            "sport":   sport,
            "mode":    mode,
            "count":   len(matches),
            "tier":    tier,
        })

        # ── Full batch — best odds + arb, NO raw per-BK markets ───────────
        # Raw markets are stripped here because 655 matches × 50KB/match = 33MB.
        # Nginx will buffer/drop that. MatchDetail fetches full markets
        # on-demand via GET /api/odds/match/<join_key>/markets
        yield _sse("batch", {
            "matches": [_strip_markets(m) for m in matches],
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

        # ── Pub/sub push loop ─────────────────────────────────────────────
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
                        ch = _decode_key(msg.get("channel") or b"")
                        if   "arb:"         in ch: yield _sse("arb_update",  payload)
                        elif "ev:"          in ch: yield _sse("ev_update",   payload)
                        elif "live_updates" in ch: yield _sse("live_update", payload)
                        else:
                            fresh = _filter_tier(
                                _get_unified_patched(mode, sport, force_refresh=True), tier
                            )
                            yield _sse("batch", {
                                "matches": [_strip_markets(m) for m in fresh],
                                "source":  "live",
                                "sport":   sport,
                                "mode":    mode,
                                "count":   len(fresh),
                            })
                    except Exception as exc:
                        log.debug("[stream] pubsub error: %s", exc)
                if time.time() - last_ka > _KEEPALIVE:
                    yield ": keepalive\n\n"
                    last_ka = time.time()
        finally:
            try: pubsub.unsubscribe(*channels); pubsub.close()
            except Exception: pass

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
                        score_home=None, score_away=None, match_time=None,
                        is_live=None, bookmakers: dict | None = None):
    try:
        r = _r()
        p: dict = {"match_id": match_id, "join_key": join_key}
        if score_home is not None: p["score_home"] = score_home
        if score_away is not None: p["score_away"] = score_away
        if match_time is not None: p["match_time"] = match_time
        if is_live    is not None: p["is_live"]    = is_live
        if bookmakers:             p["bookmakers"] = bookmakers
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
    return Response(
        stream_with_context(_make_generator(mode, sport, None, True, "pro")()),
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
def snapshot_odds(mode: str, sport: str):
    from app.api import _signed_response
    matches = _get_unified_patched(mode, sport)
    return _signed_response({"matches": matches, "sport": sport,
                              "mode": mode, "count": len(matches), "tier": "pro"})


@bp_stream.route("/odds/page/<mode>/<sport>", methods=["GET"])
def paged_odds(mode: str, sport: str):
    from app.api import _signed_response
    page     = max(1,   request.args.get("page",     1,  type=int))
    per_page = min(200, request.args.get("per_page", 10, type=int))
    sort_by  = request.args.get("sort", "start_time")
    all_m    = _get_unified_patched(mode, sport)
    if sort_by == "arb":
        all_m.sort(key=lambda m: -(m.get("best_arb_pct") or 0))
    else:
        all_m.sort(key=lambda m: m.get("start_time") or "")
    total  = len(all_m)
    offset = (page - 1) * per_page
    # Strip raw markets from paged response too — MatchDetail fetches on demand
    page_m = [_strip_markets(m) for m in all_m[offset: offset + per_page]]
    return _signed_response({
        "matches":  page_m,
        "total":    total, "page": page, "per_page": per_page,
        "pages":    -(-total // per_page),
        "has_more": (offset + per_page) < total,
        "sport":    sport, "mode": mode, "tier": "pro",
    })


@bp_stream.route("/odds/match/<join_key>/markets", methods=["GET"])
def match_markets(join_key: str):
    """
    GET /api/odds/match/<join_key>/markets?sport=soccer
    Returns full per-BK markets for ONE match.
    Called by MatchDetail when the user opens a match card.
    NOT included in bulk SSE/paged payloads (too large).
    """
    from app.api import _signed_response
    sport = request.args.get("sport", "soccer")
    r     = _r()

    # Check unified cache first (fast path)
    for mode in ("upcoming", "live"):
        raw = r.get(f"odds:unified:{mode}:{sport}")
        if not raw: continue
        try:
            data = json.loads(raw)
            ms   = data.get("matches", []) if isinstance(data, dict) else data
            for m in ms:
                jk = str(m.get("join_key") or m.get("parent_match_id") or m.get("betradar_id") or "")
                if jk == join_key:
                    return _signed_response({
                        "join_key":     join_key,
                        "bookmakers":   m.get("bookmakers", {}),
                        "best":         m.get("best", {}),
                        "market_slugs": m.get("market_slugs", []),
                        "source":       "cache",
                    })
        except Exception:
            pass

    # Not in cache — rebuild and find
    all_matches = _get_unified_patched("upcoming", sport)
    for m in all_matches:
        jk = str(m.get("join_key") or m.get("parent_match_id") or "")
        if jk == join_key:
            return _signed_response({
                "join_key":     join_key,
                "bookmakers":   m.get("bookmakers", {}),
                "best":         m.get("best", {}),
                "market_slugs": m.get("market_slugs", []),
                "source":       "live_read",
            })

    return _signed_response({"join_key": join_key, "bookmakers": {}, "best": {},
                              "market_slugs": [], "source": "not_found"}), 404


# =============================================================================
# MONITOR
# =============================================================================

@bp_monitor.route("/competitions", methods=["GET"])
def monitor_competitions():
    from app.api import _signed_response
    sport   = request.args.get("sport", "soccer")
    mode    = request.args.get("mode",  "upcoming")
    matches = _get_unified_patched(mode, sport)
    comps   = sorted({
        str(m.get("competition_name") or m.get("competition") or "").strip()
        for m in matches if (m.get("competition_name") or m.get("competition"))
    } - {""})
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
    sport = request.args.get("sport", "soccer")
    r     = _r()
    found:   dict[str, int] = {}
    missing: list[str]      = []
    for _, patterns in _BK_KEY_FORMATS:
        for pat in patterns:
            base = pat.format(sport=sport)
            try:
                raw = r.get(base)
                if raw:
                    data = json.loads(raw)
                    found[base] = len(data.get("matches", data) if isinstance(data, dict) else data)
                else:
                    pkeys = r.keys(f"{base}:page:*")
                    if pkeys:
                        total = 0
                        for pk in pkeys:
                            pk_str = _decode_key(pk)
                            kt = _decode_type(r.type(pk))
                            if kt == "list":
                                total += r.llen(pk)
                            else:
                                v = r.get(pk)
                                if v:
                                    d = json.loads(v)
                                    total += len(d) if isinstance(d, list) else len(d.get("matches", []))
                        found[f"{base}:page:* ({len(pkeys)} pages)"] = total
                    else:
                        missing.append(base)
            except Exception as exc:
                missing.append(f"{base} (err: {exc})")
    for k in [f"odds:unified:upcoming:{sport}", f"odds:unified:live:{sport}"]:
        try:
            raw = r.get(k)
            if raw: found[k] = len(json.loads(raw).get("matches", []))
            else:   missing.append(k)
        except: missing.append(k)
    return _signed_response({"sport": sport, "found": found, "missing": missing,
                              "summary": f"{len(found)} keys, {sum(found.values())} matches"})


@bp_monitor.route("/bk-data", methods=["GET"])
def monitor_bk_data():
    from app.api import _signed_response
    sport  = request.args.get("sport", "soccer")
    r      = _r()
    result = {}
    for bk_slug, patterns in _BK_KEY_FORMATS:
        matches = _read_key(r, patterns, sport)
        result[bk_slug] = {"count": len(matches) if matches else 0, "has_data": bool(matches)}
    return _signed_response({"sport": sport, "bookmakers": result})


@bp_monitor.route("/debug-stream/<sport>", methods=["GET"])
def debug_stream(sport: str):
    """
    GET /api/monitor/debug-stream/soccer
    Shows exactly what the SSE stream would send — useful for diagnosing
    why matches aren't reaching the UI.
    """
    from app.api import _signed_response
    import traceback as _tb
    r      = _r()
    errors = []

    # Step 1: raw BK key counts
    bk_counts = {}
    for bk_slug, patterns in _BK_KEY_FORMATS:
        try:
            ms = _read_key(r, patterns, sport)
            bk_counts[bk_slug] = len(ms) if ms else 0
        except Exception as exc:
            bk_counts[bk_slug] = f"ERROR: {exc}"
            errors.append(f"{bk_slug}: {exc}")

    # Step 2: _merge_bks
    merged_count = 0
    merge_error  = None
    sample       = []
    payload_kb   = 0
    try:
        merged       = _merge_bks(r, sport, _BK_KEY_FORMATS)
        merged_count = len(merged)
        sample       = [{"home": m.get("home_team"), "away": m.get("away_team"),
                         "bks": list((m.get("bookmakers") or {}).keys()),
                         "markets": len(m.get("best") or {})}
                        for m in merged[:5]]
        # Measure payload size
        slim_payload = json.dumps([_slim(m) for m in merged], default=str)
        full_payload = json.dumps([_strip_markets(m) for m in merged], default=str)
        payload_kb   = {
            "slim_kb": round(len(slim_payload) / 1024, 1),
            "full_stripped_kb": round(len(full_payload) / 1024, 1),
        }
    except Exception as exc:
        merge_error = str(exc)
        errors.append(_tb.format_exc())

    # Step 3: unified cache state
    unified_count = 0
    unified_age   = None
    unified_empty = False
    raw = r.get(f"odds:unified:upcoming:{sport}")
    if raw:
        try:
            d             = json.loads(raw)
            unified_count = len(d.get("matches", []))
            unified_age   = round(time.time() - float(d.get("updated_at", 0)), 1)
            unified_empty = unified_count == 0
        except Exception:
            pass

    return _signed_response({
        "sport":          sport,
        "bk_raw_counts":  bk_counts,
        "merge_result":   merged_count,
        "merge_error":    merge_error,
        "sample_matches": sample,
        "payload_size":   payload_kb,
        "unified_cache":  {
            "count":     unified_count,
            "age_s":     unified_age,
            "is_empty":  unified_empty,
            "poisoned":  unified_empty and unified_age is not None and unified_age < 300,
        },
        "errors": errors,
    })


@bp_monitor.route("/warm-cache", methods=["GET", "POST"])
def warm_cache():
    from app.api import _signed_response
    t0 = time.time(); results = {}
    for sport in ALL_SPORTS:
        try:    results[sport] = len(_get_unified_patched("upcoming", sport, force_refresh=True))
        except Exception as e: results[sport] = f"error: {e}"
    return _signed_response({"warmed": results, "elapsed_s": round(time.time() - t0, 2)})


# =============================================================================
# LIFECYCLE STUB
# =============================================================================

def _register_lifecycle(app) -> None:
    try:
        from app.workers.live_feed_bridge import start_live_bridge
        with app.app_context(): start_live_bridge()
        log.info("LiveFeedBridge started via _register_lifecycle()")
    except ImportError:
        try:
            from app.workers.match_lifecycle import start_lifecycle_manager
            with app.app_context(): start_lifecycle_manager()
            log.info("MatchLifecycleManager started via _register_lifecycle()")
        except Exception as exc:
            log.warning("No lifecycle manager started: %s", exc)


@bp_stream.route("/odds/watch", methods=["POST"])
def watch_match_inline():
    from app.api import _err, _signed_response
    try:
        from app.workers.match_lifecycle import WatchPrefs, get_lifecycle_manager
    except ImportError:
        return _err("Lifecycle module not available", 503)
    body       = request.get_json(silent=True) or {}
    prefs_data = body.get("prefs") or {}
    prefs = WatchPrefs(
        user_id     = prefs_data.get("user_id", "anonymous"),
        email       = prefs_data.get("email", ""),
        phone       = prefs_data.get("phone", ""),
        webhook_url = prefs_data.get("webhook_url", ""),
        channels    = prefs_data.get("channels") or ["websocket", "pubsub"],
        notify_on   = prefs_data.get("notify_on") or [
            "pre_start", "started", "suspended", "goal", "finished", "arb_found",
        ],
    )
    mgr   = get_lifecycle_manager()
    saved = mgr.save_match(body.get("match") or {}, prefs)
    return _signed_response({"ok": True, "watch": saved.to_dict()}), 201

@bp_stream.route("/odds/download/word", methods=["GET"])
def download_odds_word():
    from app.views.customer.routes_api import download_odds_word as _download_odds_word
    return _download_odds_word()