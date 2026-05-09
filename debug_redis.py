#!/usr/bin/env python3
"""
debug_redis.py  —  run inside the web container
================================================
    docker exec odds-kenya-web-1 python3 debug_redis.py

Shows:
  1. All Redis keys + match counts
  2. First match from the most populated key (full JSON)
  3. All market slugs for that match
  4. BTTS outcome keys (to check normalisation)
"""
import json, os, sys

# ── Connect to Redis ──────────────────────────────────────────────────────────
import redis as _redis

REDIS_URL = os.getenv("REDIS_URL", "")
if not REDIS_URL:
    host = os.getenv("REDIS_HOST", "redis6382")
    port = os.getenv("REDIS_PORT", "6382")
    pwd  = os.getenv("REDIS_PAxxSSWORD", "Winners1127")
    REDIS_URL = f"redis://:{pwd}@{host}:{pxort}/0"

print(f"\n🔗 Connecting: {REDIS_URL[:50]}…")
try:
    r = _redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=3)
    r.ping()
    print("✅ Redis connected\n")
except Exception as e:
    print(f"❌ Redis failed: {e}")
    sys.exit(1)

# ── Show all keys ─────────────────────────────────────────────────────────────
print("=" * 60)
print("ALL REDIS KEYS")
print("=" * 60)

all_keys = r.keys("*")
if not all_keys:
    print("❌ Redis is EMPTY — no data harvested yet")
    print("\nTo harvest data run:")
    print("  docker exec odds-kenya-web-1 flask push-sp-to-redis --sport soccer")
    sys.exit(0)

# Count matches per key
key_counts: list[tuple[int, str]] = []
for key in all_keys:
    try:
        raw  = r.get(key)
        if not raw: continue
        data = json.loads(raw)
        if isinstance(data, dict):   count = len(data.get("matches", [data]))
        elif isinstance(data, list): count = len(data)
        else:                        count = 1
        key_counts.append((count, key))
    except:
        key_counts.append((0, key))

key_counts.sort(reverse=True)
for count, key in key_counts[:30]:
    bar = "▓" * min(count // 10, 30)
    print(f"  {count:5}  {key}  {bar}")

if len(key_counts) > 30:
    print(f"  … and {len(key_counts) - 30} more keys")

# ── Pick the most populated key ───────────────────────────────────────────────
if not key_counts or key_counts[0][0] == 0:
    print("\n❌ All keys are empty")
    sys.exit(0)

best_count, best_key = key_counts[0]
print(f"\n\n{'=' * 60}")
print(f"BEST KEY:  {best_key}  ({best_count} matches)")
print("=" * 60)

raw  = r.get(best_key)
data = json.loads(raw)
matches = data.get("matches", data) if isinstance(data, dict) else data
if not matches:
    print("❌ No matches in this key")
    sys.exit(0)

# ── Show first match summary ──────────────────────────────────────────────────
m = matches[0]
print(f"\n  Match:       {m.get('home_team')} vs {m.get('away_team')}")
print(f"  Competition: {m.get('competition')}")
print(f"  Start time:  {m.get('start_time')}")
print(f"  Join key:    {m.get('join_key')}")
print(f"  BK count:    {m.get('bk_count', len(m.get('bookmakers', {})))}")
print(f"  Has arb:     {m.get('has_arb')}  ({m.get('best_arb_pct', 0):.2f}%)")

# ── Show bookmakers ───────────────────────────────────────────────────────────
bks = m.get("bookmakers") or {}
print(f"\n  Bookmakers ({len(bks)}):")
for bk, bd in sorted(bks.items()):
    mkt_count = len((bd.get("markets") or {}))
    print(f"    {bk.upper():12} → {mkt_count} markets")

# ── Show all market slugs ─────────────────────────────────────────────────────
best = m.get("best") or {}
slugs = sorted(best.keys())
print(f"\n  Market slugs in best ({len(slugs)}):")
unknown = [s for s in slugs if "unknown" in s]
known   = [s for s in slugs if "unknown" not in s]
for s in known:
    ob = best[s]
    outcomes = list(ob.keys())
    print(f"    ✅ {s:45} → {outcomes}")
if unknown:
    print(f"\n  ❌ UNRESOLVED UNKNOWN MARKETS ({len(unknown)}):")
    for s in unknown[:20]:
        ob = best[s]
        outcomes = list(ob.keys())
        print(f"    ❓ {s:45} → {outcomes}")
    if len(unknown) > 20:
        print(f"    … and {len(unknown)-20} more")

# ── BTTS normalisation check ──────────────────────────────────────────────────
print(f"\n{'=' * 60}")
print("BTTS NORMALISATION CHECK")
print("=" * 60)
btts = best.get("btts") or best.get("gg/ng") or {}
if btts:
    print(f"  BTTS outcomes: {list(btts.keys())}")
    if len(btts) <= 2:
        print("  ✅ Correctly merged (2 outcomes)")
    else:
        print(f"  ❌ NOT MERGED — {len(btts)} outcomes (should be 2)")
        print("     → odds_stream.py normalisation not applied yet")
else:
    print("  ℹ  No BTTS market in this match")

# ── Show raw bookmaker BTTS to compare ───────────────────────────────────────
print("\n  Per-BK BTTS raw outcome keys:")
for bk, bd in sorted(bks.items()):
    bk_btts = (bd.get("markets") or {}).get("btts") or {}
    if bk_btts:
        print(f"    {bk.upper():8} → {list(bk_btts.keys())}")

# ── Show HT/FT outcomes ───────────────────────────────────────────────────────
htft = best.get("ht_ft") or best.get("half_time_full_time") or {}
if htft:
    print(f"\n  HT/FT outcomes ({len(htft)}): {list(htft.keys())}")

# ── Dump full match JSON to file ──────────────────────────────────────────────
out_file = "/tmp/debug_match.json"
with open(out_file, "w") as f:
    json.dump(m, f, indent=2, default=str)
print(f"\n{'=' * 60}")
print(f"Full match JSON saved to: {out_file}")
print(f"  docker cp odds-kenya-web-1:{out_file} ./debug_match.json")
print("=" * 60)

# ── Quick harvest trigger if empty ───────────────────────────────────────────
unified_keys = [k for _, k in key_counts if "unified" in k]
sp_keys      = [k for _, k in key_counts if k.startswith("sp:") or "sp:upcoming" in k]
bt_keys      = [k for _, k in key_counts if k.startswith("bt:") or "bt:upcoming" in k]
od_keys      = [k for _, k in key_counts if k.startswith("od:") or "od:upcoming" in k]

print(f"\n📊 Summary:")
print(f"   Unified keys:  {len(unified_keys)}")
print(f"   SP keys:       {len(sp_keys)}")
print(f"   BT keys:       {len(bt_keys)}")
print(f"   OD keys:       {len(od_keys)}")

if not unified_keys:
    print("\n⚠️  No unified cache — run:")
    print("   curl -X POST http://localhost:5000/api/monitor/warm-cache")
if not sp_keys:
    print("\n⚠️  No SP data — harvest with:")
    print("   docker exec odds-kenya-web-1 flask push-sp-to-redis --sport soccer")
if not bt_keys:
    print("\n⚠️  No BT data — harvest with:")
    print("   docker exec odds-kenya-web-1 flask push-bt-to-redis --sport soccer")
if not od_keys:
    print("\n⚠️  No OD data — harvest with:")
    print("   docker exec odds-kenya-web-1 flask push-od-to-redis --sport soccer")