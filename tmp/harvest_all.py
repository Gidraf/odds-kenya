#!/usr/bin/env python3
"""
harvest_all.py — fetch SP + BT + OD for all sports and write to Redis
Run: docker exec -w /app odds-kenya-web-1 python3 /tmp/harvest_all.py

Options:
  --sports soccer basketball tennis ...  (default: soccer only)
  --all                                  (all sports)
  --debug                                (show join key comparison)
"""
import sys, json, os, time, argparse
sys.path.insert(0, '/app')

import redis as _redis

REDIS_URL = os.getenv("REDIS_URL", "redis://:Winners1127@redis6382:6382/0")
r = _redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
r.ping()
print(f"✅ Redis OK  ({REDIS_URL.split('@')[-1]})\n")

TTL = 7200  # 2 hours

ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby",
    "ice-hockey", "volleyball", "handball", "table-tennis",
    "mma", "boxing", "darts", "esoccer",
]

ap = argparse.ArgumentParser()
ap.add_argument("--sports",  nargs="+", default=["soccer"])
ap.add_argument("--all",     action="store_true")
ap.add_argument("--debug",   action="store_true")
ap.add_argument("--no-full", action="store_true", help="Skip full market fetch (faster)")
args = ap.parse_args()

sports     = ALL_SPORTS if args.all else args.sports
fetch_full = not args.no_full


def save(bk: str, sport: str, matches: list):
    """Write matches to every key format _merge_bks checks."""
    data = json.dumps(matches, default=str)
    for key in [
        f"{bk}:upcoming:{sport}",
        f"odds:{bk}:upcoming:{sport}",
    ]:
        r.setex(key, TTL, data)
    print(f"    💾 {len(matches):4}  →  {bk}:upcoming:{sport}")


def fetch_sp(sport: str) -> list:
    from app.workers.sp_harvester import fetch_upcoming
    return fetch_upcoming(
        sport_slug=sport, days=7,
        max_matches=500,
        fetch_full_markets=fetch_full,
        sleep_between=0.1,
    )


def fetch_bt(sport: str) -> list:
    from app.workers.bt_harvester import fetch_upcoming_matches
    return fetch_upcoming_matches(
        sport_slug=sport, days=30,
        fetch_full=fetch_full,
        max_workers=4,
    )


def fetch_od(sport: str) -> list:
    from app.workers.od_harvester import fetch_upcoming_matches
    return fetch_upcoming_matches(
        sport_slug=sport, days=30,
        fetch_full_markets=fetch_full,
        max_workers=4,
    )


def debug_join_keys(sp_matches, bt_matches, od_matches):
    """Show join key format for each BK to understand merge failures."""
    print("\n  🔍 JOIN KEY COMPARISON")
    print("  " + "─"*70)
    for label, matches in [("SP", sp_matches), ("BT", bt_matches), ("OD", od_matches)]:
        if not matches:
            continue
        m = matches[0]
        print(f"  {label}:")
        print(f"    home_team      = {m.get('home_team')!r}")
        print(f"    away_team      = {m.get('away_team')!r}")
        print(f"    join_key       = {m.get('join_key')!r}")
        print(f"    parent_match_id= {m.get('parent_match_id')!r}")
        print(f"    betradar_id    = {m.get('betradar_id')!r}")
        print(f"    match_id       = {m.get('match_id')!r}")
        print()


def rebuild_unified(sport: str) -> list:
    r.delete(f"odds:unified:upcoming:{sport}")
    r.delete(f"odds:unified:live:{sport}")
    from app.api.odds_stream import _get_unified
    return _get_unified("upcoming", sport, force_refresh=True)


# ─── Main loop ────────────────────────────────────────────────────────────────
total = {"sp": 0, "bt": 0, "od": 0}

for sport in sports:
    print(f"\n{'═'*55}")
    print(f"  {sport.upper()}")
    print(f"{'═'*55}")

    sp_matches = bt_matches = od_matches = []

    # SP
    print("  SP…")
    try:
        sp_matches = fetch_sp(sport)
        if sp_matches:
            save("sp", sport, sp_matches)
            total["sp"] += len(sp_matches)
        else:
            print(f"    ⚠️  SP {sport}: 0 matches")
    except Exception as e:
        print(f"    ❌ SP {sport}: {e}")

    # BT
    print("  BT…")
    try:
        bt_matches = fetch_bt(sport)
        if bt_matches:
            save("bt", sport, bt_matches)
            total["bt"] += len(bt_matches)
        else:
            print(f"    ⚠️  BT {sport}: 0 matches")
    except Exception as e:
        print(f"    ❌ BT {sport}: {e}")

    # OD
    print("  OD…")
    try:
        od_matches = fetch_od(sport)
        if od_matches:
            save("od", sport, od_matches)
            total["od"] += len(od_matches)
        else:
            print(f"    ⚠️  OD {sport}: 0 matches")
    except Exception as e:
        print(f"    ❌ OD {sport}: {e}")

    # Debug join keys
    if args.debug:
        debug_join_keys(sp_matches, bt_matches, od_matches)

    # Rebuild unified
    print("  🔄 Rebuilding unified cache…")
    try:
        unified = rebuild_unified(sport)
        bk_cover: dict[str, int] = {}
        for m in unified:
            for bk in m.get("bookmakers", {}):
                bk_cover[bk] = bk_cover.get(bk, 0) + 1
        multi_bk = sum(1 for m in unified if len(m.get("bookmakers", {})) > 1)
        print(f"    ✅ {len(unified)} unified matches  |  multi-BK: {multi_bk}")
        print(f"    BK coverage: {dict(sorted(bk_cover.items()))}")
        if unified and args.debug:
            for m in unified[:3]:
                bks = sorted(m.get("bookmakers", {}).keys())
                print(f"      {m['home_team'][:18]:18} vs {m['away_team'][:18]:18}  {bks}")
    except Exception as e:
        print(f"    ❌ unified {sport}: {e}")

# ─── Summary ──────────────────────────────────────────────────────────────────
print(f"\n{'═'*55}")
print("📊 HARVEST COMPLETE")
print(f"{'═'*55}")
for bk, n in total.items():
    print(f"  {bk.upper()}  {n:5} matches total")

print(f"\n📦 REDIS KEYS")
for sport in sports:
    for bk in ["sp", "bt", "od"]:
        raw = r.get(f"{bk}:upcoming:{sport}")
        n = len(json.loads(raw)) if raw else 0
        icon = "✅" if n > 0 else "❌"
        print(f"  {icon} {bk}:upcoming:{sport:20}  {n}")
    unified_raw = r.get(f"odds:unified:upcoming:{sport}")
    if unified_raw:
        data = json.loads(unified_raw)
        matches = data.get("matches", data) if isinstance(data, dict) else data
        bks_seen: set = set()
        for m in matches:
            bks_seen.update(m.get("bookmakers", {}).keys())
        print(f"  🔀 unified:upcoming:{sport:20}  {len(matches)}  BKs={sorted(bks_seen)}")
    else:
        print(f"  ❌ unified:upcoming:{sport}  MISSING")

print(f"\n✅ Done!")
print(f"   Test: curl http://localhost:5050/api/odds/snapshot/upcoming/soccer?token=TOKEN")