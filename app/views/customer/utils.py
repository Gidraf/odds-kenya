import json
import re
from datetime import datetime, timezone
from . import config

def _now_utc() -> datetime: return datetime.now(timezone.utc)

def _effective_status(db_status: str | None, start_time: datetime | None) -> str:
    if db_status in config._TERMINAL_STATUSES: return db_status
    if start_time is None: return db_status or "PRE_MATCH"
    st = start_time if start_time.tzinfo else start_time.replace(tzinfo=timezone.utc)
    now = _now_utc()
    if st > now: return "PRE_MATCH"
    if now - st <= config._LIVE_WINDOW: return "IN_PLAY"
    return "FINISHED"

def _is_live(db_status, start_time) -> bool: return _effective_status(db_status, start_time) == "IN_PLAY"
def _is_upcoming(db_status, start_time) -> bool: return _effective_status(db_status, start_time) == "PRE_MATCH"
def _is_available(db_status, start_time) -> bool: return _effective_status(db_status, start_time) in ("PRE_MATCH", "IN_PLAY")

def _is_upcoming_safe(db_status, start_time) -> bool:
    if (db_status or "").upper() in config._EXCLUDE_FROM_UPCOMING: return False
    if not start_time: return False
    st = start_time if start_time.tzinfo else start_time.replace(tzinfo=timezone.utc)
    if st <= _now_utc(): return False
    return _effective_status(db_status, start_time) == "PRE_MATCH"

def _bk_slug(name: str) -> str: return config._BK_SLUG.get(name.lower(), name.lower()[:4])
def _normalise_sport_slug(raw: str) -> str:
    if not raw: return raw
    return config._CANONICAL_SLUG.get(raw, raw.lower().replace(" ", "-"))


_LINE_SPEC_RE = re.compile(r"[+-]?\d+(?:[.,]\d+)?")


def _spec_suffix(spec_val: object) -> str:
    s = str(spec_val or "").strip()
    if not s:
        return ""
    if _LINE_SPEC_RE.fullmatch(s):
        return s.replace(",", ".")
    return ""


def _market_with_spec(market_slug: str, spec_val: object) -> str:
    suffix = _spec_suffix(spec_val)
    if not suffix:
        return market_slug
    key_suffix = suffix.replace(".", "_").replace("-", "m").replace("+", "p")
    return f"{market_slug}__spec__{key_suffix}"

def _flatten_db_markets(raw_markets: dict) -> dict:
    flat: dict = {}
    for mkt_slug, spec_dict in (raw_markets or {}).items():
        if not isinstance(spec_dict, dict):
            flat[mkt_slug] = spec_dict
            continue

        for spec_val, inner in spec_dict.items():
            if isinstance(inner, dict):
                market_key = _market_with_spec(mkt_slug, spec_val)
                outcomes = flat.setdefault(market_key, {})
                for out_key, out_val in inner.items():
                    outcomes[out_key] = out_val
            else:
                outcomes = flat.setdefault(mkt_slug, {})
                outcomes[spec_val] = inner
    return flat

def _sse(event: str, data: dict) -> str: return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"
def _keepalive() -> str: return ": ping\n\n"

def _apply_tier_limits(matches, user):
    if config.FREE_ACCESS: return matches, False
    limits = (user.limits if user else None) or {"max_matches": config.FREE_MATCH_LIMIT}
    max_m  = limits.get("max_matches") or config.FREE_MATCH_LIMIT
    if max_m and len(matches) > max_m: return matches[:max_m], True
    return matches, False

def _get_country_weight(sport_slug: str, category_name: str) -> int:
    if not category_name: return 99
    return config._POPULARITY_WEIGHTS.get(sport_slug, {}).get(category_name.lower(), 99)