#!/usr/bin/env python3
"""
scripts/test_deployment.py
===========================
Automated post-deployment health checker.

Waits up to 10 minutes for the API and Celery worker to become healthy,
then runs a full suite of checks against the live service.

Usage:
    python scripts/test_deployment.py
    python scripts/test_deployment.py --base-url http://localhost:5000
    python scripts/test_deployment.py --base-url http://localhost:5000 --skip-wait
    python scripts/test_deployment.py --base-url https://staging.myapp.com --timeout 300
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime

import requests

# ── Colour helpers ────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):     print(f"  {GREEN}✔{RESET}  {msg}")
def fail(msg):   print(f"  {RED}✗{RESET}  {msg}")
def warn(msg):   print(f"  {YELLOW}⚠{RESET}  {msg}")
def info(msg):   print(f"  {CYAN}→{RESET}  {msg}")
def header(msg): print(f"\n{BOLD}{msg}{RESET}")
def sep():       print(f"  {'─' * 60}")


# ── Result tracker ────────────────────────────────────────────────────────────
class Results:
    def __init__(self):
        self.passed:   list[str] = []
        self.failed:   list[str] = []
        self.warnings: list[str] = []

    def record(self, name: str, success: bool, detail: str = "") -> bool:
        label = name + (f"  [{detail}]" if detail else "")
        if success:
            ok(label);   self.passed.append(name)
        else:
            fail(label); self.failed.append(name)
        return success

    def warn(self, name: str, detail: str = ""):
        warn(name + (f"  [{detail}]" if detail else ""))
        self.warnings.append(name)

    def summary(self) -> bool:
        header("━━  SUMMARY  ━━")
        sep()
        print(f"  Passed   : {GREEN}{len(self.passed)}{RESET}")
        print(f"  Failed   : {RED}{len(self.failed)}{RESET}")
        print(f"  Warnings : {YELLOW}{len(self.warnings)}{RESET}")
        sep()
        if self.failed:
            print(f"\n  {RED}Failed checks:{RESET}")
            for f in self.failed:
                print(f"    • {f}")
        return len(self.failed) == 0


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def _get(base_url: str, path: str, params: dict | None = None, timeout: int = 15):
    try:
        r = requests.get(f"{base_url}{path}", params=params, timeout=timeout)
        try:    return r.status_code, r.json()
        except: return r.status_code, {}
    except Exception as exc:
        return 0, {"error": str(exc)}


def _post(base_url: str, path: str, timeout: int = 10):
    try:
        r = requests.post(f"{base_url}{path}", timeout=timeout)
        try:    return r.status_code, r.json()
        except: return r.status_code, {}
    except Exception as exc:
        return 0, {"error": str(exc)}


ALL_SPORTS = [
    "soccer", "basketball", "tennis", "ice-hockey",
    "rugby", "handball", "volleyball", "cricket",
    "table-tennis", "esoccer", "mma", "boxing", "darts",
]


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 — Wait for API
# ─────────────────────────────────────────────────────────────────────────────

def wait_for_api(base_url: str, timeout: int = 600, interval: int = 10) -> bool:
    header("Phase 1 — Waiting for API to become reachable")
    deadline = time.monotonic() + timeout
    attempt  = 0

    while time.monotonic() < deadline:
        attempt  += 1
        remaining = int(deadline - time.monotonic())
        for endpoint in ("/api/combined/health", "/api/combined/status"):
            try:
                r = requests.get(f"{base_url}{endpoint}", timeout=5)
                if r.status_code < 500:
                    ok(f"API reachable at {endpoint}  [{r.status_code}]  (attempt {attempt})")
                    return True
            except requests.exceptions.ConnectionError:
                pass
            except Exception:
                pass

        info(f"Not ready — retrying in {interval}s  ({remaining}s remaining)")
        time.sleep(interval)

    fail(f"API did not become reachable within {timeout}s")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 2 — Wait for Celery worker heartbeat
# ─────────────────────────────────────────────────────────────────────────────

def wait_for_celery(base_url: str, timeout: int = 600, interval: int = 15) -> bool:
    header("Phase 2 — Waiting for Celery worker heartbeat")
    deadline = time.monotonic() + timeout
    attempt  = 0

    while time.monotonic() < deadline:
        attempt  += 1
        remaining = int(deadline - time.monotonic())
        code, data = _get(base_url, "/api/combined/health")
        if code == 200:
            if data.get("worker_alive"):
                ok(f"Worker alive  (attempt {attempt})"
                   f"  checked_at={data.get('checked_at', '?')}")
                return True
            redis_ok = data.get("redis", False)
            info(f"API up | Redis={'✔' if redis_ok else '✗'} | worker not ready yet"
                 f" — retrying in {interval}s  ({remaining}s remaining)")
        else:
            info(f"Health returned {code} — retrying in {interval}s  ({remaining}s remaining)")
        time.sleep(interval)

    fail(f"Celery heartbeat not detected within {timeout}s")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 3 — API endpoint checks
# ─────────────────────────────────────────────────────────────────────────────

def check_api_endpoints(base_url: str, results: Results) -> None:
    header("Phase 3 — API endpoint checks")

    # Health
    code, data = _get(base_url, "/api/combined/health")
    results.record(
        "GET /api/combined/health",
        code == 200 and data.get("ok"),
        f"HTTP {code} | redis={data.get('redis')} worker={data.get('worker_alive')}",
    )

    # Status (all 13 sports)
    code, data = _get(base_url, "/api/combined/status")
    results.record(
        "GET /api/combined/status",
        code == 200 and data.get("ok"),
        f"HTTP {code} | populated={data.get('populated_sports','?')}/{data.get('total_sports','?')}",
    )

    # Upcoming soccer
    code, data = _get(base_url, "/api/combined/upcoming/soccer")
    match_count = len(data.get("matches", []))
    results.record(
        "GET /api/combined/upcoming/soccer",
        code == 200 and data.get("ok"),
        f"HTTP {code} | {match_count} matches",
    )
    if code == 200 and match_count == 0:
        results.warn("upcoming/soccer match count", "0 — workers may still be seeding")

    # Live soccer
    code, data = _get(base_url, "/api/combined/live/soccer")
    results.record(
        "GET /api/combined/live/soccer",
        code == 200 and data.get("ok"),
        f"HTTP {code} | {len(data.get('matches', []))} matches",
    )

    # Opportunities soccer
    code, data = _get(base_url, "/api/combined/opportunities/soccer")
    results.record(
        "GET /api/combined/opportunities/soccer",
        code == 200 and data.get("ok"),
        f"HTTP {code} | arbs={data.get('arb_count',0)} evs={data.get('ev_count',0)}",
    )

    # Data API sports list
    code, data = _get(base_url, "/api/data/sports")
    results.record(
        "GET /api/data/sports",
        code == 200 and data.get("ok"),
        f"HTTP {code} | {len(data.get('sports', []))} sports in DB",
    )

    # Per-sport cache check
    header("Phase 3b — Per-sport cache check")
    _, status_data = _get(base_url, "/api/combined/status")
    sport_map = {s["sport"]: s for s in status_data.get("sports", [])}

    for sport in ALL_SPORTS:
        s = sport_map.get(sport, {})
        if s.get("cache_populated"):
            results.record(f"Cache: {sport}", True,
                           f"upcoming={s.get('upcoming_count',0)}"
                           f" live={s.get('live_count',0)}")
        else:
            results.warn(f"Cache: {sport}", "not populated yet")


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 4 — Celery task checks
# ─────────────────────────────────────────────────────────────────────────────

def check_celery_tasks(base_url: str, results: Results) -> None:
    header("Phase 4 — Celery task smoke tests")

    # Worker heartbeat freshness
    code, data = _get(base_url, "/api/combined/health")
    if code == 200:
        results.record("Worker alive",
                       bool(data.get("worker_alive")),
                       f"checked_at={data.get('checked_at','?')}")
        age = data.get("heartbeat_age_s")
        if age is not None:
            results.record("Heartbeat freshness (<90s)",
                           age < 90, f"{age}s ago")
        results.record("Redis reachable",
                       bool(data.get("redis")))
    else:
        results.record("GET /api/combined/health", False, f"HTTP {code}")

    # Trigger persist_all_sports
    code, data = _post(base_url, "/api/combined/ops/trigger/persist_all_sports")
    results.record(
        "Trigger persist_all_sports",
        code == 200 and data.get("ok"),
        f"HTTP {code} | task_id={data.get('task_id','?')}",
    )

    # Trigger health_check
    code, data = _post(base_url, "/api/combined/ops/trigger/health_check")
    results.record(
        "Trigger health_check task",
        code == 200 and data.get("ok"),
        f"HTTP {code}",
    )

    # Flower queue depths (optional)
    header("Phase 4b — Flower queue depths (optional)")
    flower_base = base_url.replace(":5000", ":5555")
    for queue in ("default", "harvest", "live", "ev_arb", "results", "notify"):
        try:
            r = requests.get(f"{flower_base}/api/queues/{queue}", timeout=3)
            if r.status_code == 200:
                ok(f"  queue '{queue}': {r.json().get('messages','?')} pending")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 5 — Persist hook round-trip
# ─────────────────────────────────────────────────────────────────────────────

def check_persist_hook(base_url: str, results: Results) -> None:
    header("Phase 5 — Persist hook round-trip")
    info("Waiting 8s for persist_all_sports task to complete …")
    time.sleep(8)

    code, data = _get(base_url, "/api/data/sports")
    sport_count = len(data.get("sports", []))
    results.record(
        "Sports persisted to PostgreSQL",
        code == 200 and sport_count > 0,
        f"{sport_count} sport(s) in DB",
    )

    code, data = _get(base_url, "/api/data/competitions/1/matches", params={"per_page": 5})
    if code == 200:
        results.record(
            "Matches persisted to PostgreSQL",
            data.get("total", 0) > 0,
            f"{data.get('total', 0)} match(es) in competition 1",
        )
    else:
        results.warn("Competition matches", f"HTTP {code} — DB may still be seeding")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deployment health checker")
    parser.add_argument("--base-url",  default="http://localhost:5000")
    parser.add_argument("--timeout",   type=int, default=600)
    parser.add_argument("--skip-wait", action="store_true",
                        help="Skip boot-wait phases (use if services are already up)")
    args = parser.parse_args()

    print(f"\n{BOLD}{'━' * 62}{RESET}")
    print(f"{BOLD}  Deployment Health Checker{RESET}")
    print(f"  Base URL : {args.base_url}")
    print(f"  Timeout  : {args.timeout}s  ({args.timeout // 60} min)")
    print(f"  Started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{BOLD}{'━' * 62}{RESET}")

    results = Results()

    if not args.skip_wait:
        if not wait_for_api(args.base_url, timeout=args.timeout):
            results.record("API reachable", False, "timed out")
            results.summary()
            sys.exit(1)
        wait_for_celery(args.base_url, timeout=args.timeout)
    else:
        info("--skip-wait passed — jumping straight to checks")

    check_api_endpoints(args.base_url, results)
    check_celery_tasks(args.base_url, results)
    check_persist_hook(args.base_url, results)

    passed = results.summary()
    print(f"\n  Finished : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()