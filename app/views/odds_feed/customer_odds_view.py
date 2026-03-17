from app.utils.customer_jwt_helpers import _current_user_from_header, _err, _signed_response
from app.utils.decorators_ import log_event, require_tier
from app.utils.fetcher_utils import _is_upcoming

from . import bp_odds as bp_customer
from flask import request, g, Response
from datetime import datetime, timezone, timedelta

FREE_MATCH_LIMIT = 100
 
 
def _filter_by_tier(matches: list[dict], user: "Customer | None") -> tuple[list[dict], bool]:
    """
    Return (filtered_matches, truncated).
    Free users: 100 matches, today only.
    Basic:      all today.
    Pro+:       all available.
    """
    tier   = user.tier if user else "free"
    limits = user.limits if user else {"max_matches": FREE_MATCH_LIMIT, "days_ahead": 0}
 
    # Day filter
    days_ahead = limits.get("days_ahead", 0)
    if days_ahead == 0:
        cutoff = datetime.now(timezone.utc) + timedelta(days=1)
        matches = [m for m in matches
                   if not m.get("start_time") or
                   datetime.fromisoformat(str(m["start_time"]).replace("Z", "+00:00")) <= cutoff]
 
    # Count limit
    max_m = limits.get("max_matches") or FREE_MATCH_LIMIT
    if max_m and len(matches) > max_m:
        return matches[:max_m], True
 
    return matches, False
 
 
@bp_customer.route("/odds/sports")
def list_sports():
    """All supported sports (B2B + SBO combined)."""
    from app.workers.celery_tasks import cache_keys
    b2b_keys = cache_keys("odds:upcoming:*:*:p1")
    sbo_keys = cache_keys("sbo:upcoming:*")
    sports: set[str] = set()
    for k in b2b_keys:
        parts = k.split(":")
        if len(parts) >= 3:
            sports.add(parts[2].replace("_", " ").title())
    for k in sbo_keys:
        parts = k.split(":")
        if len(parts) >= 3:
            sports.add(parts[2].replace("-", " ").title())
    return _signed_response({"ok": True, "sports": sorted(sports)})
 
 
@bp_customer.route("/odds/upcoming/<sport_slug>")
def get_upcoming(sport_slug: str):
    """
    Upcoming matches merged from all bookmakers for a sport.
    Free: max 100, today only.
    Basic+: all matches today.
    Pro+: future month.
    """
    user    = _current_user_from_header()
    tier    = user.tier if user else "free"
    page    = int(request.args.get("page", 1))
    per_p   = min(int(request.args.get("per_page", 20)), 100)
    market  = request.args.get("market")
 
    log_event("odds_view", {"sport": sport_slug, "tier": tier})
 
    # ── Try SBO cache first ────────────────────────────────────────────────
    from app.workers.celery_tasks import cache_get, cache_keys
    from .bookmaker_fetcher import merge_bookmaker_results
 
    sbo_data = cache_get(f"sbo:upcoming:{sport_slug.lower()}")
    b2b_data = []
 
    # Pull B2B pages from cache
    bk_keys = cache_keys(f"odds:upcoming:{sport_slug.lower().replace(' ','_')}:*")
    for k in bk_keys:
        d = cache_get(k)
        if d and d.get("matches"):
            b2b_data.append(d["matches"])
 
    matches: list[dict] = []
    if sbo_data:
        matches.extend(sbo_data.get("matches", []))
    if b2b_data:
        merged = merge_bookmaker_results(b2b_data)
        matches.extend(merged)
 
    # Remove duplicates by team names (SBO + B2B may overlap)
    seen: set[str] = set()
    unique: list[dict] = []
    for m in matches:
        key = f"{(m.get('home_team') or '').lower()}|{(m.get('away_team') or '').lower()}"
        if key not in seen:
            seen.add(key)
            unique.append(m)
    matches = unique
 
    # Filter upcoming only (not started)
    now     = datetime.now(timezone.utc)
    matches = [m for m in matches if _is_upcoming(m, now)]
 
    if market:
        matches = [m for m in matches
                   if market in (m.get("markets") or m.get("best_odds") or {})]
 
    # Tier gate
    matches, truncated = _filter_by_tier(matches, user)
 
    total  = len(matches)
    start  = (page - 1) * per_p
    paged  = matches[start:start + per_p]
 
    resp_data = {
        "ok":        True,
        "sport":     sport_slug,
        "tier":      tier,
        "total":     total,
        "page":      page,
        "per_page":  per_p,
        "pages":     max(1, (total + per_p - 1) // per_p),
        "truncated": truncated,
        "matches":   paged,
    }
    if truncated:
        resp_data["upgrade_message"] = (
            "Showing first 100 matches. Sign up for Basic or higher to see all."
        )
 
    return _signed_response(resp_data, encrypt_for=user)
 
 
@bp_customer.route("/odds/live/<sport_slug>")
def get_live(sport_slug: str):
    """Live matches — all tiers can see live but limited for free."""
    user = _current_user_from_header()
    from app.workers.celery_tasks import cache_get, cache_keys
    from .bookmaker_fetcher import merge_bookmaker_results
 
    bk_keys = cache_keys(f"odds:live:{sport_slug.lower().replace(' ','_')}:*")
    b2b_data = [cache_get(k).get("matches", []) for k in bk_keys
                if cache_get(k) and cache_get(k).get("matches")]
 
    matches = merge_bookmaker_results(b2b_data) if b2b_data else []
    matches, truncated = _filter_by_tier(matches, user)
 
    return _signed_response({
        "ok":        True,
        "sport":     sport_slug,
        "mode":      "live",
        "total":     len(matches),
        "truncated": truncated,
        "matches":   matches,
    }, encrypt_for=user)
 
 
@bp_customer.route("/odds/match/<parent_match_id>")
def get_match(parent_match_id: str):
    """Full market detail for one match — all tiers."""
    user = _current_user_from_header()
    from app.models.odds_model import UnifiedMatch
 
    um = UnifiedMatch.query.filter_by(parent_match_id=parent_match_id).first()
    if not um:
        return _err("Match not found", 404)
 
    log_event("match_view", {"match_id": parent_match_id})
    return _signed_response({"ok": True, "match": um.to_dict()}, encrypt_for=user)
 
 
@bp_customer.route("/odds/finished")
@require_tier("basic", "pro", "premium")
def get_finished():
    """Finished games — paginated, today only by default."""
    date_str = request.args.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    return _get_finished_by_date(date_str)
 
 
@bp_customer.route("/odds/finished/<date_str>")
@require_tier("basic", "pro", "premium")
def get_finished_by_date(date_str: str):
    return _get_finished_by_date(date_str)
 
 
def _get_finished_by_date(date_str: str) -> Response:
    from app.workers.celery_tasks import cache_get
    from app.models.odds_model import UnifiedMatch
 
    # Try Redis cache first
    cached = cache_get(f"results:finished:{date_str}")
    if cached:
        log_event("finished_games_view", {"date": date_str, "source": "cache"})
        return _signed_response({
            "ok": True, "date": date_str, "source": "cache",
            "total": len(cached), "matches": cached,
        })
 
    # Fallback to DB
    try:
        day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        day_end   = day_start + timedelta(days=1)
        matches   = UnifiedMatch.query.filter(
            UnifiedMatch.status    == "FINISHED",
            UnifiedMatch.start_time >= day_start,
            UnifiedMatch.start_time <  day_end,
        ).all()
        data = [m.to_dict() for m in matches]
        log_event("finished_games_view", {"date": date_str, "source": "db"})
        return _signed_response({
            "ok": True, "date": date_str, "source": "db",
            "total": len(data), "matches": data,
        })
    except ValueError:
        return _err("Invalid date format. Use YYYY-MM-DD.")
 
 