#!/usr/bin/env python3
"""
scripts/flush_unified_cache.py
================================
Flush all odds:unified:* keys from Redis so the API rebuilds them fresh
from the per-BK snapshot keys.

Run this on your server inside the container:
  docker compose exec web python scripts/flush_unified_cache.py

Or directly:
  python scripts/flush_unified_cache.py
"""
import os
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

sports = [
    "soccer", "basketball", "tennis", "cricket", "rugby",
    "ice-hockey", "volleyball", "handball", "table-tennis",
    "baseball", "mma", "boxing", "darts", "american-football", "esoccer",
]
modes = ["upcoming", "live"]

deleted = 0
for mode in modes:
    for sport in sports:
        key = f"odds:unified:{mode}:{sport}"
        if r.delete(key):
            print(f"  ✓ deleted {key}")
            deleted += 1
        else:
            print(f"  - skip   {key} (not found)")

# Also flush any wildcard keys
extra_keys = r.keys("odds:unified:*")
for k in extra_keys:
    r.delete(k)
    print(f"  ✓ deleted extra: {k}")
    deleted += 1

print(f"\nDone. {deleted} unified cache keys flushed.")
print("The API will rebuild them fresh from per-BK snapshot keys on the next request.")
