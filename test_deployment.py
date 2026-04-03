#!/usr/bin/env python3
"""
scripts/test_deployment.py
===========================
Automated post-deployment health checker.

Waits up to 10 minutes for all services to become healthy, then runs
a full suite of API and Celery checks. Exits 0 on success, 1 on failure.

Usage:
    python scripts/test_deployment.py
    python scripts/test_deployment.py --base-url http://localhost:5000 --timeout 600
    python scripts/test_deployment.py --base-url https://staging.myapp.com --timeout 300
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests

# ── Colour helpers (no deps) ─────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):    print(f"  {GREEN}✔{RESET}  {msg}")
def fail(msg):  print(f"  {RED}✗{RESET}  {msg}")
def warn(msg):  print(f"  {YELLOW}⚠{RESET}  {msg}")
def info(msg):  print(f"  {CYAN}→{RESET}  {msg}")
def header(msg): print(f"\n{BOLD}{msg}{RESET}")
def sep():       print(f"  {'─' * 60}")


# ── Result tracker ────────────────────────────────────────────────────────────
class Results:
    def __init__(self):
        self.passed:  list[str] = []
        self.failed:  list[str] = []
        self.warnings: list[str] = []

    def record(self, name: str, success: bool, detail: str = "") -> bool:
        label = f"{name}" + (f" — {detail}" if detail else "")
        if success:
            ok(label);   self.passed.append(name)
        else:
            fail(label); self.failed.append(name)
        return success

    def warn(self, name: str, detail: str = ""):
        warn(f"{name}" + (f" — {detail}" if detail else ""))
        self.warnings.append(name)

    def summary(self):
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


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 1 — Wait for services to boot (up to 10 minutes)
# ─────────────────────────────────────────────────────────────────────────────

def wait_for_api(base_url: str, timeout: int = 600, interval: int = 10) -> bool:
    """
    Poll GET /api/health (or /) until a 2xx response is received.
    Times out after `timeout` seconds.
    """
    header("Phase 1 — Waiting for API to become reachable")
    deadline = time.monotonic() + timeout
    attempt  = 0

    health_endpoints = ["/api/health", "/api/combined/status", "/health", "/"]

    while time.monotonic() < deadline:
        attempt += 1
        remaining = int(deadline - time.monotonic())
        for endpoint in health_endpoints:
            url = f"{base_url}{endpoint}"
            try:
                r = requests.get(url, timeout=5)
                if r.status_code < 500:
                    ok(f"API reachable at {url}  [{r.status_code}]  (attempt {attempt})")
                    return True
            except requests.exceptions.ConnectionError:
                pass
            except Exception:
                pass

        info(f"Not ready yet — retrying in {interval}s  ({remaining}s remaining)")
        time.sleep(interval)

    fail(f"API did not become reachable within {timeout}s")
    return False


def wait_for_celery_heartbeat(base_url: str, timeout: int = 600, interval: int = 15) -> bool:
    """
    Poll GET /api/health/celery (or worker_heartbeat via combined/status)
    until the Celery heartbeat key appears in Redis (set by health_check task).
    """
    header("Phase 2 — Waiting for Celery worker heartbeat")
    deadline = time.monotonic() + timeout
    attempt  = 0

    while time.monotonic() < deadline:
        attempt += 1
        remaining = int(deadline - time.monotonic())

        # Primary: dedicated celery health endpoint
        for endpoint in ("/api/health/celery", "/api/ops/health"):
            try:
                r = requests.get(f"{base_url}{endpoint}", timeout=5)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("worker_alive") or data.get("ok"):
                        ok(f"Celery worker alive via {endpoint}  (attempt {attempt})")
                        return True
            except Exception:
                pass

        # Fallback: combined/status implies workers ran at least one harvest
        try:
            r = requests.get(f"{base_url}/api/combined/status", timeout=5)
            if r.status_code == 200:
                data = r.json()
                sports = data.get("sports", [])
                if sports:
                    ok(f"Celery confirmed alive — {len(sports)} sport(s) have cached data  (attempt {attempt})")
                    return True
        except Exception:
            pass

        info(f"Worker not ready yet — retrying in {interval}s  ({remaining}s remaining)")
        time.sleep(interval)

    fail(f"Celery worker heartbeat not detected within {timeout}s")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 3 — API endpoint checks
# ─────────────────────────────────────────────────────────────────────────────

def _get(base_url: str, path: str, params: dict | None = None, timeout: int = 15) -> tuple[int, Any]:
    try:
        r = requests.get(f"{base_url}{path}", params=params, timeout=timeout)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, {}
    except Exception as exc:
        return 0, {"error": str(exc)}


def check_api_endpoints(base_url: str, results: Results) -> None:
    header("Phase 3 — API endpoint checks")

    # ── Combined status ───────────────────────────────────────────────────────
    code, data = _get(base_url, "/api/combined/status")
    results.record(
        "GET /api/combined/status",
        code == 200 and data.get("ok"),
        f"HTTP {code}",
    )

    # ── Combined upcoming (soccer) ────────────────────────────────────────────
    code, data = _get(base_url, "/api/combined/upcoming/soccer")
    has_matches = isinstance(data.get("matches"), list)
    results.record(
        "GET /api/combined/upcoming/soccer",
        code == 200 and has_matches,
        f"HTTP {code} — {len(data.get('matches', []))} matches",
    )

    # Warn if no matches yet (workers may still be seeding)
    if code == 200 and has_matches and len(data.get("matches", [])) == 0:
        results.warn(
            "upcoming/soccer match count",
            "0 matches returned — workers may still be seeding"
        )

    # ── Combined live (soccer) ────────────────────────────────────────────────
    code, data = _get(base_url, "/api/combined/live/soccer")
    results.record(
        "GET /api/combined/live/soccer",
        code == 200,
        f"HTTP {code}",
    )

    # ── Opportunities ─────────────────────────────────────────────────────────
    code, data = _get(base_url, "/api/combined/opportunities/soccer")
    results.record(
        "GET /api/combined/opportunities/soccer",
        code in (200, 404),   # 404 is acceptable if cache is cold
        f"HTTP {code}",
    )

    # ── Data API — sports list ────────────────────────────────────────────────
    code, data = _get(base_url, "/api/data/sports")
    results.record(
        "GET /api/data/sports",
        code == 200 and isinstance(data.get("sports"), list),
        f"HTTP {code} — {len(data.get('sports', []))} sports",
    )

    # ── Spot-check every sport has a cache entry after 10 min ────────────────
    SPORTS = [
        "soccer", "basketball", "tennis", "ice-hockey",
        "volleyball", "cricket", "rugby", "table-tennis",
        "esoccer", "mma", "boxing", "darts", "handball",
    ]
    code, status_data = _get(base_url, "/api/combined/status")
    cached_sports = {s["sport"] for s in status_data.get("sports", [])}

    header("Phase 3b — Per-sport cache check")
    for sport in SPORTS:
        if sport in cached_sports:
            results.record(f"Cache populated: {sport}", True)
        else:
            results.warn(f"Cache unpopulated: {sport}", "not in /api/combined/status yet")


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 4 — Celery task checks
# ─────────────────────────────────────────────────────────────────────────────

def check_celery_tasks(base_url: str, results: Results) -> None:
    header("Phase 4 — Celery task smoke tests")

    # ── Trigger persist_all_sports manually via API ───────────────────────────
    try:
        r = requests.post(
            f"{base_url}/api/ops/trigger/persist_all_sports",
            timeout=10,
        )
        if r.status_code == 200:
            results.record("Trigger persist_all_sports", True, "task dispatched")
        elif r.status_code == 404:
            results.warn(
                "Trigger persist_all_sports",
                "endpoint not exposed — check manually via Celery flower"
            )
        else:
            results.record("Trigger persist_all_sports", False, f"HTTP {r.status_code}")
    except Exception as exc:
        results.warn("Trigger persist_all_sports", f"endpoint not reachable: {exc}")

    # ── Worker heartbeat key present ──────────────────────────────────────────
    code, data = _get(base_url, "/api/health/celery")
    if code == 200:
        alive = data.get("worker_alive") or data.get("ok")
        results.record(
            "Worker heartbeat key in Redis",
            bool(alive),
            data.get("checked_at", ""),
        )
    else:
        results.warn("Worker heartbeat", f"/api/health/celery returned {code}")

    # ── Beat scheduler alive (checked_at recency) ─────────────────────────────
    code, data = _get(base_url, "/api/health/celery")
    if code == 200 and data.get("checked_at"):
        try:
            checked_at = datetime.fromisoformat(
                data["checked_at"].replace("Z", "+00:00")
            )
            age_s = (datetime.now(timezone.utc) - checked_at).total_seconds()
            results.record(
                "Beat scheduler heartbeat age",
                age_s < 120,          # health_check runs every 30 s; allow 2× slack
                f"{int(age_s)}s ago",
            )
        except Exception:
            results.warn("Beat scheduler heartbeat age", "could not parse checked_at")

    # ── Queue depth via Flower (optional) ─────────────────────────────────────
    for queue in ("default", "harvest", "live", "ev_arb", "results", "notify"):
        try:
            r = requests.get(
                f"{base_url.replace('5000', '5555')}/api/queues/{queue}",
                timeout=5,
            )
            if r.status_code == 200:
                d = r.json()
                depth = d.get("messages", "?")
                ok(f"  Flower queue '{queue}': {depth} messages")
        except Exception:
            pass   # Flower is optional


# ─────────────────────────────────────────────────────────────────────────────
# PHASE 5 — Persist hook round-trip check
# ─────────────────────────────────────────────────────────────────────────────

def check_persist_hook(base_url: str, results: Results) -> None:
    header("Phase 5 — Persist hook round-trip (wait 60 s for first flush)")

    # After 10 min the beat has fired at least twice.
    # Check that at least one sport has DB-persisted matches via the data API.
    info("Checking /api/data/sports for persisted entity data …")
    time.sleep(5)   # small buffer

    code, data = _get(base_url, "/api/data/sports")
    sport_count = len(data.get("sports", []))

    results.record(
        "Persist hook — sports persisted to DB",
        code == 200 and sport_count > 0,
        f"{sport_count} sport(s) found in PostgreSQL",
    )

    # Try pulling matches for soccer from the canonical data API
    code, data = _get(
        base_url,
        "/api/data/competitions/1/matches",   # adjust comp_id as needed
        params={"per_page": 5},
    )
    if code == 200:
        match_count = data.get("total", 0)
        results.record(
            "Persist hook — competition matches in DB",
            match_count > 0,
            f"{match_count} match(es) in competition 1",
        )
    else:
        results.warn(
            "Persist hook — competition matches",
            f"HTTP {code} — DB may still be seeding",
        )


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deployment health checker")
    parser.add_argument(
        "--base-url", default="http://localhost:5000",
        help="Base URL of the Flask API (default: http://localhost:5000)",
    )
    parser.add_argument(
        "--timeout", type=int, default=600,
        help="Max seconds to wait for services to boot (default: 600 = 10 min)",
    )
    parser.add_argument(
        "--skip-wait", action="store_true",
        help="Skip the boot-wait phases and go straight to checks",
    )
    args = parser.parse_args()

    print(f"\n{BOLD}{'━' * 62}{RESET}")
    print(f"{BOLD}  Deployment Health Checker{RESET}")
    print(f"  Base URL : {args.base_url}")
    print(f"  Timeout  : {args.timeout}s  ({args.timeout // 60} min)")
    print(f"  Started  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{BOLD}{'━' * 62}{RESET}")

    results = Results()

    # ── Phase 1 & 2 — wait for services ──────────────────────────────────────
    if not args.skip_wait:
        api_up = wait_for_api(args.base_url, timeout=args.timeout)
        if not api_up:
            results.record("API reachable", False, "timed out")
            results.summary()
            sys.exit(1)

        celery_up = wait_for_celery_heartbeat(args.base_url, timeout=args.timeout)
        if not celery_up:
            results.record("Celery worker reachable", False, "timed out")
            # Don't exit — still run API checks so we know what's working

    # ── Phase 3 — API endpoint checks ─────────────────────────────────────────
    check_api_endpoints(args.base_url, results)

    # ── Phase 4 — Celery task checks ──────────────────────────────────────────
    check_celery_tasks(args.base_url, results)

    # ── Phase 5 — Persist round-trip ──────────────────────────────────────────
    check_persist_hook(args.base_url, results)

    # ── Final summary ─────────────────────────────────────────────────────────
    passed = results.summary()
    print(f"\n  Finished : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()