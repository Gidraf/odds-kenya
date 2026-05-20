#!/usr/bin/env python3
"""
scripts/restore_redis.py
=========================
Loads all upcoming matches from the DB into Redis so the frontend
shows data immediately without waiting for a harvest cycle.

Run inside the web container:
    docker exec odds-kenya-web-1 python3 scripts/restore_redis.py
"""
import json
import sys
import time
import os

sys.path.insert(0, "/app")
os.environ.setdefault("FLASK_APP", "run.py")

from app import create_app

app = create_app()

SPORT_SLUG_MAP = {
    "soccer": "soccer", "football": "soccer",
    "esoccer": "esoccer", "efootball": "esoccer",
    "basketball": "basketball",
    "tennis": "tennis",
    "ice hockey": "ice-hockey", "ice-hockey": "ice-hockey",
    "volleyball": "volleyball",
    "cricket": "cricket",
    "rugby": "rugby",
    "table tennis": "table-tennis", "table-tennis": "table-tennis",
    "boxing": "boxing",
    "handball": "handball",
    "mma": "mma",
    "darts": "darts",
    "american football": "american-football", "american-football": "american-football",
    "baseball": "baseball",
}

with app.app_context():
    from app.workers.celery_tasks import _redis
    from app.models.odds import UnifiedMatch
    from app.extensions import db
    from datetime import datetime, timezone, timedelta

    r = _redis()
    now = datetime.now(timezone.utc)

    print(f"[restore] Querying DB for upcoming matches...")
    rows = db.session.execute(
        db.select(UnifiedMatch).where(
            UnifiedMatch.start_time >= now - timedelta(hours=3),
            UnifiedMatch.start_time <= now + timedelta(days=30),
        ).order_by(UnifiedMatch.start_time)
    ).scalars().all()

    print(f"[restore] Found {len(rows)} matches in DB")
    if not rows:
        print("[restore] No matches found — check that harvests have run before")
        sys.exit(1)

    by_sport: dict[str, list] = {}
    for um in rows:
        raw_sport = (um.sport_name or "soccer").lower().strip()
        sport = SPORT_SLUG_MAP.get(raw_sport, raw_sport.replace(" ", "-"))

        match_dict = {
            "match_id":        str(um.parent_match_id or um.id),
            "parent_match_id": str(um.parent_match_id or um.id),
            "join_key":        str(um.parent_match_id or um.id),
            "betradar_id":     str(um.parent_match_id or ""),
            "home_team":       um.home_team_name or "",
            "away_team":       um.away_team_name or "",
            "competition":     um.competition_name or "",
            "sport":           sport,
            "start_time":      um.start_time.isoformat() if um.start_time else "",
            "status":          getattr(um, "status", "PRE_MATCH") or "PRE_MATCH",
            "is_live":         False,
            "has_arb":         False,
            "has_ev":          False,
            "best_arb_pct":    0,
            "bk_count":        0,
            "market_count":    0,
            "arb_opportunities": [],
            "bookmakers":      {},
            "markets":         {},
            "best":            {},
            "market_slugs":    [],
        }
        by_sport.setdefault(sport, []).append(match_dict)

    written = 0
    for sport, matches in sorted(by_sport.items()):
        key = f"odds:unified:upcoming:{sport}"
        payload = json.dumps({
            "mode":        "upcoming",
            "sport":       sport,
            "source":      "db_restore",
            "match_count": len(matches),
            "matches":     matches,
            "updated_at":  time.time(),
        })
        r.set(key, payload, ex=86400)
        written += 1
        print(f"  ✓ {sport:20} {len(matches):4d} matches → {key}")

    print(f"\n[restore] Done — wrote {written} sport keys covering {len(rows)} matches")
    print("[restore] Frontend should show data within 5 seconds")