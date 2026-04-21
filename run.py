from app import create_app

flask_app = create_app()

from app.extensions import db, init_celery, socketio
from app.models.user_admin import User
from werkzeug.security import generate_password_hash

# Expose celery_app at module level so Celery CLI can find it:
# celery -A run.celery_app worker ...
init_celery(flask_app)
celery_app = flask_app.celery


@flask_app.cli.command("seed-markets")
def seed_markets_cmd():
        from app.utils.mapping_seed import seed_all_markets
        seed_all_markets()
        print("Done.")

@flask_app.cli.command("create-admin")
def create_admin():
    email    = input("Enter admin email: ")
    password = input("Enter admin password: ")
    if User.query.filter_by(email=email).first():
        print("User already exists!")
        return
    db.session.add(User(email=email, password_hash=generate_password_hash(password)))
    db.session.commit()
    print(f"✅ Admin '{email}' created.")


@flask_app.cli.command("run-harvest")
def run_harvest():
    from app.services.agents_tasks import harvest_data
    harvest_data()

@flask_app.cli.command("seed-bookmakers")
def seed_bookmakers_cmd():
    from app.utils.mapping_seed import seed_bookmakers
    result = seed_bookmakers()
    print(f"Done: {result}")

@flask_app.cli.command("seed-all")
def seed_all_cmd():
    from app.utils.mapping_seed import seed_all
    result = seed_all()
    print(f"Done: {result}")


@flask_app.cli.command("test-sp-harvester")
def test_sp_harvester():
    """
    Test sp_harvester across all defined sports.
    For each sport slug, fetch a few upcoming matches and report:
      - Number of matches returned
      - How many used the full market fetch vs inline fallback
      - Whether Over/Under markets are present (if expected)
      - Any API errors or timeouts
    """
    from app.workers.sp_harvester import fetch_upcoming, SP_SPORT_ID

    # Deduplicate by sport_id (some slugs map to the same id, e.g. soccer/football)
    # We'll test each distinct slug to ensure mapping works, but avoid testing same sport twice.
    seen_ids = set()
    distinct_sports = []
    for slug, sid in SP_SPORT_ID.items():
        if sid not in seen_ids:
            seen_ids.add(sid)
            distinct_sports.append((slug, sid))

    print(f"Testing {len(distinct_sports)} distinct sports\n{'-'*60}")

    results = {}
    for slug, sid in distinct_sports:
        print(f"\n▶ Sport: {slug} (ID {sid})")
        try:
            matches = fetch_upcoming(
                sport_slug=slug,
                days=7,                # look 2 days ahead
                max_matches=10,        # limit to 10 matches per sport
                fetch_full_markets=True,
                sleep_between=0.2,     # be gentle with API
                debug_ou=True,         # prints O/U debug info
            )
        except Exception as e:
            print(f"  ❌ ERROR: {e}")
            results[slug] = {"error": str(e), "matches": 0}
            continue

        if not matches:
            print("  ⚠️ No matches found")
            results[slug] = {"matches": 0, "inline_fallback": 0, "ou_present": False}
            continue

        # Analyse the returned matches
        total = len(matches)
        inline_used = sum(1 for m in matches if m.get("market_count", 0) == 0)
        # Check if any match contains an over/under market (key contains 'total' or 'over_under')
        ou_present = any(
            any("total" in k or "over_under" in k for k in m.get("markets", {}).keys())
            for m in matches
        )

        print(f"  ✅ {total} matches fetched")
        print(f"     Full markets: {total - inline_used} | Inline fallback: {inline_used}")
        print(f"     Over/Under markets present: {'✓' if ou_present else '✗'}")

        results[slug] = {
            "matches": total,
            "inline_fallback": inline_used,
            "ou_present": ou_present,
        }

    # Final summary
    print("\n" + "="*60)
    print("SUMMARY")
    for slug, data in results.items():
        if "error" in data:
            print(f"  {slug:15} → ERROR: {data['error']}")
        else:
            print(f"  {slug:15} → {data['matches']:2} matches, "
                  f"fallback: {data['inline_fallback']:2}, "
                  f"O/U: {'yes' if data['ou_present'] else 'no'}")


# Add this to your run.py file

@flask_app.cli.command("test-harvesters")
def test_harvesters():
    """
    Test both Betika and OdiBets harvesters across all sports.
    Reports match counts, market completeness, and any errors.
    """
    import time
    from datetime import datetime
    
    # Test configuration
    TEST_SPORTS = ["soccer", "basketball", "tennis", "cricket", "rugby", "ice-hockey"]
    MAX_MATCHES_PER_SPORT = 5  # Small sample for quick testing
    DAYS_AHEAD = 3
    
    print("\n" + "="*80)
    print(f"🧪 HARVESTER TEST SUITE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    results = {
        "betika": {"passed": 0, "failed": 0, "sports": {}},
        "odibets": {"passed": 0, "failed": 0, "sports": {}}
    }
    
    # ============================================================
    # TEST BETIKA HARVESTER
    # ============================================================
    print("\n📊 TESTING BETIKA HARVESTER")
    print("-" * 60)
    
    try:
        from app.workers.bt_harvester import (
            fetch_upcoming_matches,
            fetch_live_matches,
            get_full_markets,
            BetikaHarvesterPlugin
        )
        
        for sport in TEST_SPORTS:
            print(f"\n  🏆 Sport: {sport}")
            
            # Test upcoming matches
            try:
                start = time.time()
                upcoming = fetch_upcoming_matches(
                    sport_slug=sport,
                    max_pages=2,  # Only 2 pages for quick test
                    fetch_full=False,  # Don't fetch full markets yet
                    period_id=9
                )
                elapsed = time.time() - start
                
                if upcoming:
                    print(f"    ✅ Upcoming: {len(upcoming)} matches ({elapsed:.2f}s)")
                    
                    # Test full market enrichment on first match
                    if len(upcoming) > 0 and upcoming[0].get("bt_parent_id"):
                        try:
                            full_markets = get_full_markets(
                                upcoming[0]["bt_parent_id"], 
                                sport
                            )
                            market_count = len(full_markets)
                            print(f"    ✅ Full markets: {market_count} markets for first match")
                            
                            # Check for key market types
                            has_1x2 = any("1x2" in k or "match_winner" in k for k in full_markets.keys())
                            has_ou = any("over_under" in k for k in full_markets.keys())
                            has_hc = any("handicap" in k or "spread" in k for k in full_markets.keys())
                            
                            print(f"       - 1X2: {'✓' if has_1x2 else '✗'}")
                            print(f"       - Over/Under: {'✓' if has_ou else '✗'}")
                            print(f"       - Handicap: {'✓' if has_hc else '✗'}")
                            
                        except Exception as e:
                            print(f"    ⚠️ Full markets failed: {str(e)[:50]}")
                    
                    results["betika"]["sports"][sport] = {
                        "upcoming_count": len(upcoming),
                        "status": "passed"
                    }
                    results["betika"]["passed"] += 1
                else:
                    print(f"    ⚠️ Upcoming: No matches found")
                    results["betika"]["sports"][sport] = {
                        "upcoming_count": 0,
                        "status": "warning"
                    }
                    
            except Exception as e:
                print(f"    ❌ Upcoming failed: {str(e)}")
                results["betika"]["sports"][sport] = {
                    "error": str(e),
                    "status": "failed"
                }
                results["betika"]["failed"] += 1
            
            # Test live matches (quick check)
            try:
                live = fetch_live_matches(slug_to_bt_sport_id(sport) if sport else None)
                if live:
                    print(f"    🟢 Live: {len(live)} matches found")
                else:
                    print(f"    🟡 Live: No live matches")
            except Exception as e:
                print(f"    ⚠️ Live check failed: {str(e)[:50]}")
            
            time.sleep(0.5)  # Small delay between sports
            
    except ImportError as e:
        print(f"❌ Failed to import Betika harvester: {e}")
    except Exception as e:
        print(f"❌ Betika test error: {e}")
    
    # ============================================================
    # TEST ODIBETS HARVESTER
    # ============================================================
    print("\n\n📊 TESTING ODIBETS HARVESTER")
    print("-" * 60)
    
    try:
        from app.workers.od_harvester import (
            fetch_upcoming_matches,
            fetch_live_matches,
            fetch_event_detail,
            OdiBetsHarvesterPlugin
        )
        
        for sport in TEST_SPORTS:
            print(f"\n  🏆 Sport: {sport}")
            
            # Test upcoming matches
            try:
                start = time.time()
                upcoming = fetch_upcoming_matches(
                    sport_slug=sport,
                    fetch_full_markets=False,
                    max_matches=MAX_MATCHES_PER_SPORT
                )
                elapsed = time.time() - start
                
                if upcoming:
                    print(f"    ✅ Upcoming: {len(upcoming)} matches ({elapsed:.2f}s)")
                    
                    # Check markets in first match
                    if len(upcoming) > 0:
                        first_match = upcoming[0]
                        market_count = first_match.get("market_count", 0)
                        markets = first_match.get("markets", {})
                        
                        print(f"    ✅ Markets: {market_count} markets in first match")
                        
                        # Check for key market types
                        has_1x2 = any("1x2" in k for k in markets.keys())
                        has_ou = any("over_under" in k for k in markets.keys())
                        has_hc = any("handicap" in k or "spread" in k for k in markets.keys())
                        
                        print(f"       - 1X2: {'✓' if has_1x2 else '✗'}")
                        print(f"       - Over/Under: {'✓' if has_ou else '✗'}")
                        print(f"       - Handicap: {'✓' if has_hc else '✗'}")
                        
                        # Test event detail fetch if we have an event ID
                        event_id = first_match.get("od_match_id") or first_match.get("betradar_id")
                        if event_id:
                            try:
                                detail_markets, meta = fetch_event_detail(event_id)
                                print(f"    ✅ Event detail: {len(detail_markets)} additional markets")
                            except Exception as e:
                                print(f"    ⚠️ Event detail failed: {str(e)[:50]}")
                    
                    results["odibets"]["sports"][sport] = {
                        "upcoming_count": len(upcoming),
                        "status": "passed"
                    }
                    results["odibets"]["passed"] += 1
                else:
                    print(f"    ⚠️ Upcoming: No matches found")
                    results["odibets"]["sports"][sport] = {
                        "upcoming_count": 0,
                        "status": "warning"
                    }
                    
            except Exception as e:
                print(f"    ❌ Upcoming failed: {str(e)}")
                results["odibets"]["sports"][sport] = {
                    "error": str(e),
                    "status": "failed"
                }
                results["odibets"]["failed"] += 1
            
            # Test live matches
            try:
                live = fetch_live_matches(sport)
                if live:
                    print(f"    🟢 Live: {len(live)} matches found")
                    # Check if live matches have markets
                    if len(live) > 0:
                        live_market_count = live[0].get("market_count", 0)
                        print(f"       - First live match: {live_market_count} markets")
                else:
                    print(f"    🟡 Live: No live matches")
            except Exception as e:
                print(f"    ⚠️ Live check failed: {str(e)[:50]}")
            
            time.sleep(0.5)  # Small delay between sports
            
    except ImportError as e:
        print(f"❌ Failed to import OdiBets harvester: {e}")
    except Exception as e:
        print(f"❌ OdiBets test error: {e}")
    
    # ============================================================
    # SUMMARY
    # ============================================================
    print("\n\n" + "="*80)
    print("📈 TEST SUMMARY")
    print("="*80)
    
    # Betika Summary
    print(f"\n🏢 BETIKA")
    print(f"   ✅ Passed: {results['betika']['passed']}/{len(TEST_SPORTS)} sports")
    print(f"   ❌ Failed: {results['betika']['failed']}/{len(TEST_SPORTS)} sports")
    
    if results['betika']['sports']:
        print(f"\n   Details:")
        for sport, data in results['betika']['sports'].items():
            if data.get('status') == 'passed':
                print(f"     ✓ {sport}: {data.get('upcoming_count', 0)} matches")
            elif data.get('status') == 'warning':
                print(f"     ⚠ {sport}: No matches (may be normal)")
            else:
                print(f"     ✗ {sport}: {data.get('error', 'Unknown error')[:60]}")
    
    # OdiBets Summary
    print(f"\n🏢 ODIBETS")
    print(f"   ✅ Passed: {results['odibets']['passed']}/{len(TEST_SPORTS)} sports")
    print(f"   ❌ Failed: {results['odibets']['failed']}/{len(TEST_SPORTS)} sports")
    
    if results['odibets']['sports']:
        print(f"\n   Details:")
        for sport, data in results['odibets']['sports'].items():
            if data.get('status') == 'passed':
                print(f"     ✓ {sport}: {data.get('upcoming_count', 0)} matches")
            elif data.get('status') == 'warning':
                print(f"     ⚠ {sport}: No matches (may be normal)")
            else:
                print(f"     ✗ {sport}: {data.get('error', 'Unknown error')[:60]}")
    
    # Overall verdict
    print("\n" + "="*80)
    total_passed = results['betika']['passed'] + results['odibets']['passed']
    total_tests = len(TEST_SPORTS) * 2
    
    if total_passed == total_tests:
        print("🎉 ALL TESTS PASSED! Both harvesters are working correctly.")
    elif total_passed >= total_tests * 0.7:
        print("⚠️ MOST TESTS PASSED - Some sports may have no matches or minor issues.")
    else:
        print("❌ MULTIPLE FAILURES - Check network connectivity and API endpoints.")
    
    print("="*80)
    
    return results


@flask_app.cli.command("test-betika")
def test_betika():
    """Quick test for Betika harvester only."""
    from app.workers.bt_harvester import fetch_upcoming_matches, get_full_markets
    
    sport = "soccer"
    print(f"\n🧪 Testing Betika harvester for {sport}...")
    
    # Test upcoming
    upcoming = fetch_upcoming_matches(sport_slug=sport, max_pages=1, fetch_full=False)
    print(f"✅ Upcoming matches: {len(upcoming)}")
    
    if upcoming:
        match = upcoming[0]
        print(f"   Sample: {match.get('home_team')} vs {match.get('away_team')}")
        print(f"   Match ID: {match.get('bt_match_id')}")
        print(f"   Parent ID: {match.get('bt_parent_id')}")
        print(f"   Betradar ID: {match.get('betradar_id')}")
        
        # Test full markets
        if match.get('bt_parent_id'):
            full = get_full_markets(match['bt_parent_id'], sport)
            print(f"✅ Full markets: {len(full)}")
            market_types = list(full.keys())[:5]
            print(f"   Sample markets: {', '.join(market_types)}")
    
    # Test live
    from app.workers.bt_harvester import fetch_live_matches
    live = fetch_live_matches()
    print(f"🟢 Live matches: {len(live)}")
    
    print("\n✅ Betika test complete!")


@flask_app.cli.command("test-odibets")
def test_odibets():
    """Quick test for OdiBets harvester only."""
    from app.workers.od_harvester import fetch_upcoming_matches, fetch_live_matches, fetch_event_detail
    
    sport = "soccer"
    print(f"\n🧪 Testing OdiBets harvester for {sport}...")
    
    # Test upcoming
    upcoming = fetch_upcoming_matches(sport_slug=sport, max_matches=3)
    print(f"✅ Upcoming matches: {len(upcoming)}")
    
    if upcoming:
        match = upcoming[0]
        print(f"   Sample: {match.get('home_team')} vs {match.get('away_team')}")
        print(f"   Match ID: {match.get('od_match_id')}")
        print(f"   Betradar ID: {match.get('betradar_id')}")
        print(f"   Markets: {match.get('market_count')}")
        
        # Test event detail
        if match.get('betradar_id'):
            detail_markets, meta = fetch_event_detail(match['betradar_id'])
            print(f"✅ Event detail markets: {len(detail_markets)}")
    
    # Test live
    live = fetch_live_matches(sport)
    print(f"🟢 Live matches: {len(live)}")
    
    if live:
        print(f"   First live match markets: {live[0].get('market_count')}")
    
    print("\n✅ OdiBets test complete!")


# Helper function needed for Betika test
def slug_to_bt_sport_id(slug: str) -> int:
    """Convert sport slug to Betika sport ID."""
    bt_sport_map = {
        "soccer": 1,
        "basketball": 2,
        "tennis": 3,
        "cricket": 4,
        "rugby": 5,
        "ice-hockey": 6,
        "volleyball": 7,
        "handball": 8,
        "table-tennis": 9,
        "baseball": 10,
        "american-football": 11,
        "mma": 15,
        "boxing": 16,
        "darts": 17,
        "esoccer": 1001,
    }
    return bt_sport_map.get(slug, 1)

@flask_app.cli.command("test-bt-od-all")
def test_bt_od_all():
    """
    Test Betika and OdiBets harvesters for every sport they support.
    Uses 30-day default for upcoming matches.
    """
    import time
    from datetime import datetime

    def fmt(slug: str) -> str:
        return slug.replace("-", " ").title()

    print("\n" + "=" * 80)
    print(f"🧪 BETIKA + ODIBETS – ALL SPORTS TEST (30 days upcoming)")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # ------------------------------------------------------------
    # 1. BETIKA
    # ------------------------------------------------------------
    print("\n📊 BETIKA HARVESTER")
    print("-" * 60)

    try:
        from app.workers.bt_harvester import (
            fetch_upcoming_matches,
            fetch_live_matches,
            get_full_markets,
            CANONICAL_SPORT_IDS,
            slug_to_bt_sport_id,
        )
    except ImportError as e:
        print(f"❌ Cannot import Betika harvester: {e}")
        return

    betika_sports = list(CANONICAL_SPORT_IDS.keys())
    betika_results = {}

    for sport in betika_sports:
        print(f"\n  🏆 {fmt(sport)}")
        res = {"upcoming": 0, "live": 0, "markets": 0, "has_1x2": False, "has_ou": False, "has_hc": False, "status": "unknown", "sample": ""}

        # ---- Upcoming (30 days, fetch_full=False for speed) ----
        try:
            start = time.time()
            # Use days=30 (harvester default) and fetch_full=False
            upcoming = fetch_upcoming_matches(
                sport_slug=sport,
                days=30,
                fetch_full=False,   # only basic markets for speed
                max_pages=20,
            )
            elapsed = time.time() - start

            if upcoming:
                res["upcoming"] = len(upcoming)
                # Take first match as sample
                first = upcoming[0]
                res["sample"] = f"{first.get('home_team', '?')} vs {first.get('away_team', '?')}"
                print(f"    ✅ Upcoming: {len(upcoming)} matches ({elapsed:.2f}s) - sample: {res['sample']}")

                # For market stats, fetch full markets for first match only (optional)
                pid = first.get("bt_parent_id")
                if pid:
                    try:
                        full = get_full_markets(pid, sport)
                        res["markets"] = len(full)
                        res["has_1x2"] = any("1x2" in k or "match_winner" in k for k in full)
                        res["has_ou"] = any("over_under" in k for k in full)
                        res["has_hc"] = any("handicap" in k or "spread" in k for k in full)
                    except Exception as e:
                        print(f"    ⚠️ Full markets error: {str(e)[:60]}")
                res["status"] = "passed"
            else:
                print(f"    ⚠️ No upcoming matches")
                res["status"] = "warning"

        except Exception as e:
            print(f"    ❌ Upcoming error: {str(e)[:80]}")
            res["status"] = "failed"

        # ---- Live (filter by sport) ----
        try:
            bt_id = slug_to_bt_sport_id(sport)
            live = fetch_live_matches(bt_id)
            res["live"] = len(live)
            print(f"    {'🟢' if live else '🟡'} Live: {len(live)} matches")
        except Exception as e:
            print(f"    ⚠️ Live check failed: {str(e)[:50]}")

        betika_results[sport] = res
        time.sleep(0.3)

    # Betika summary table
    print("\n  " + "-" * 50)
    print("  BETIKA – SUMMARY")
    print(f"{'Sport':<20} {'Up':<6} {'Live':<6} {'Mkts':<6} {'1X2':<4} {'O/U':<4} {'HC':<4} {'Status':<8}")
    print("  " + "-" * 70)
    for s, r in betika_results.items():
        print(f"  {fmt(s):<18} {r['upcoming']:<6} {r['live']:<6} {r['markets']:<6} "
              f"{'✓' if r['has_1x2'] else '✗':<4} {'✓' if r['has_ou'] else '✗':<4} {'✓' if r['has_hc'] else '✗':<4} {r['status']:<8}")

    # ------------------------------------------------------------
    # 2. ODIBETS (similar changes)
    # ------------------------------------------------------------
    print("\n\n📊 ODIBETS HARVESTER")
    print("-" * 60)

    try:
        from app.workers.od_harvester import (
            fetch_upcoming_matches,
            fetch_live_matches,
            fetch_event_detail,
            OD_SPORT_IDS,
        )
    except ImportError as e:
        print(f"❌ Cannot import OdiBets harvester: {e}")
        return

    odibets_sports = list(OD_SPORT_IDS.keys())
    odibets_results = {}

    for sport in odibets_sports:
        print(f"\n  🏆 {fmt(sport)}")
        res = {"upcoming": 0, "live": 0, "markets": 0, "has_1x2": False, "has_ou": False, "has_hc": False, "status": "unknown", "sample": ""}

        # ---- Upcoming (30 days, fetch_full=False) ----
        try:
            start = time.time()
            upcoming = fetch_upcoming_matches(
                sport_slug=sport,
                days=30,
                fetch_full_markets=False,   # speed
            )
            elapsed = time.time() - start

            if upcoming:
                res["upcoming"] = len(upcoming)
                first = upcoming[0]
                res["sample"] = f"{first.get('home_team', '?')} vs {first.get('away_team', '?')}"
                print(f"    ✅ Upcoming: {len(upcoming)} matches ({elapsed:.2f}s) - sample: {res['sample']}")

                # Inline markets from first match
                markets = first.get("markets", {})
                res["markets"] = len(markets)
                res["has_1x2"] = any("1x2" in k for k in markets)
                res["has_ou"] = any("over_under" in k for k in markets)
                res["has_hc"] = any("handicap" in k or "spread" in k for k in markets)

                # Optionally fetch full event detail for first match
                br_id = first.get("betradar_id")
                if br_id:
                    try:
                        detail, _ = fetch_event_detail(br_id, OD_SPORT_IDS.get(sport, 1))
                        if detail:
                            print(f"    🔄 Event detail added {len(detail)} markets")
                    except Exception as e:
                        print(f"    ⚠️ Event detail error: {str(e)[:50]}")

                res["status"] = "passed"
            else:
                print(f"    ⚠️ No upcoming matches")
                res["status"] = "warning"

        except Exception as e:
            print(f"    ❌ Upcoming error: {str(e)[:80]}")
            res["status"] = "failed"

        # ---- Live ----
        try:
            live = fetch_live_matches(sport)
            res["live"] = len(live)
            print(f"    {'🟢' if live else '🟡'} Live: {len(live)} matches")
        except Exception as e:
            print(f"    ⚠️ Live check failed: {str(e)[:50]}")

        odibets_results[sport] = res
        time.sleep(0.3)

    # OdiBets summary table
    print("\n  " + "-" * 50)
    print("  ODIBETS – SUMMARY")
    print(f"{'Sport':<20} {'Up':<6} {'Live':<6} {'Mkts':<6} {'1X2':<4} {'O/U':<4} {'HC':<4} {'Status':<8}")
    print("  " + "-" * 70)
    for s, r in odibets_results.items():
        print(f"  {fmt(s):<18} {r['upcoming']:<6} {r['live']:<6} {r['markets']:<6} "
              f"{'✓' if r['has_1x2'] else '✗':<4} {'✓' if r['has_ou'] else '✗':<4} {'✓' if r['has_hc'] else '✗':<4} {r['status']:<8}")

    print("\n" + "=" * 80)
    print("✅ Test completed.")
    
if __name__ == "__main__":
    socketio.run(flask_app, debug=True, host="0.0.0.0", port=5500, use_reloader=False, log_output=True)