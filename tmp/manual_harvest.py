#!/usr/bin/env python3
"""
manual_harvest.py — run inside the web container
=================================================
    docker cp manual_harvest.py odds-kenya-web-1:/tmp/
    docker exec odds-kenya-web-1 python3 /tmp/manual_harvest.py

Fetches SP + BT + OD for all sports and writes to the Redis keys
that odds_stream.py _merge_bks() expects:
    sp:upcoming:{sport}
    bt:upcoming:{sport}
    od:upcoming:{sport}

Then rebuilds the unified cache for each sport.
"""
import json, os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed

import redis

# ── Redis connection ──────────────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://:Winners1127@redis6382:6382/0")
r = redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
try:
    r.ping()
    print(f"✅ Redis connected ({REDIS_URL.split('@')[-1]})")
except Exception as e:
    print(f"❌ Redis failed: {e}"); sys.exit(1)

SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby",
    "ice-hockey", "volleyball", "handball", "table-tennis",
    "mma", "boxing", "darts", "esoccer",
]

TTL = 3600  # 1 hour


def write_key(bk: str, sport: str, matches: list) -> None:
    """Write matches to the key that _merge_bks reads."""
    key = f"{bk}:upcoming:{sport}"
    r.setex(key, TTL, json.dumps(matches, default=str))
    # Also write the odds: prefixed variant
    r.setex(f"odds:{bk}:upcoming:{sport}", TTL, json.dumps(matches, default=str))
    print(f"    📝 {key}  ({len(matches)} matches)")


def harvest_sp(sport: str) -> int:
    try:
        from app.workers.sp_harvester import fetch_upcoming
        matches = fetch_upcoming(
            sport_slug=sport, days=7,
            max_matches=500, fetch_full_markets=True,
            sleep_between=0.1,
        )
        if matches:
            write_key("sp", sport, matches)
        return len(matches)
    except Exception as e:
        print(f"    ❌ SP {sport}: {e}")
        return 0


def harvest_bt(sport: str) -> int:
    try:
        from app.workers.bt_harvester import fetch_upcoming_matches
        matches = fetch_upcoming_matches(
            sport_slug=sport, days=30,
            fetch_full=True, max_workers=4,
        )
        if matches:
            write_key("bt", sport, matches)
        return len(matches)
    except Exception as e:
        print(f"    ❌ BT {sport}: {e}")
        return 0


def harvest_od(sport: str) -> int:
    try:
        from app.workers.od_harvester import fetch_upcoming_matches
        matches = fetch_upcoming_matches(
            sport_slug=sport, days=30,
            fetch_full_markets=True, max_workers=4,
        )
        if matches:
            write_key("od", sport, matches)
        return len(matches)
    except Exception as e:
        print(f"    ❌ OD {sport}: {e}")
        return 0


def rebuild_unified(sport: str) -> int:
    """Delete cached unified key so odds_stream rebuilds it fresh."""
    r.delete(f"odds:unified:upcoming:{sport}")
    try:
        from app.api.odds_stream import _get_unified
        matches = _get_unified("upcoming", sport, force_refresh=True)
        print(f"    🔄 unified {sport}: {len(matches)} matches")
        return len(matches)
    except Exception as e:
        print(f"    ❌ unified {sport}: {e}")
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
import argparse
p = argparse.ArgumentParser()
p.add_argument("--sports",  nargs="+", default=["soccer"], help="Sports to harvest")
p.add_argument("--all",     action="store_true",            help="Harvest all sports")
p.add_argument("--bk",      nargs="+", default=["sp","bt","od"], help="Bookmakers")
p.add_argument("--workers", type=int,  default=3,           help="Parallel sports")
args = p.parse_args()

sports = SPORTS if args.all else args.sports

print(f"\n🚀 Harvesting {len(sports)} sports × {args.bk} bookmakers\n")

totals = {"sp": 0, "bt": 0, "od": 0}

for sport in sports:
    print(f"\n⚽ {sport.upper()}")
    if "sp" in args.bk:
        print(f"  SP…")
        totals["sp"] += harvest_sp(sport)
    if "bt" in args.bk:
        print(f"  BT…")
        totals["bt"] += harvest_bt(sport)
    if "od" in args.bk:
        print(f"  OD…")
        totals["od"] += harvest_od(sport)

    # Rebuild unified after all BKs done for this sport
    print(f"  Rebuilding unified cache…")
    rebuild_unified(sport)

print(f"\n{'='*50}")
print("📊 HARVEST SUMMARY")
print(f"{'='*50}")
for bk, count in totals.items():
    print(f"  {bk.upper():8} {count:5} matches")

# Show what's in Redis now
print(f"\n📦 REDIS KEY COUNTS")
for sport in sports:
    for bk in ["sp", "bt", "od"]:
        raw = r.get(f"{bk}:upcoming:{sport}")
        if raw:
            try:
                n = len(json.loads(raw))
                print(f"  {bk}:upcoming:{sport:20} {n} matches")
            except:
                pass
    raw = r.get(f"odds:unified:upcoming:{sport}")
    if raw:
        try:
            data = json.loads(raw)
            n = len(data.get("matches", []))
            bks = set()
            for m in data.get("matches", []):
                bks.update(m.get("bookmakers", {}).keys())
            print(f"  unified:upcoming:{sport:20} {n} matches  BKs: {sorted(bks)}")
        except:
            pass

print(f"\n✅ Done. Test with:")
print(f"   curl http://localhost:5000/api/odds/snapshot/upcoming/soccer?token=YOUR_TOKEN")