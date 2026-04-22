from flask import jsonify, request

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
                    fetch_full_markets=True,
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

# @flask_app.cli.command("test-all")
# def test_all():
#     """
#     Test Betika, OdiBets, and Sportpesa harvesters for all sports.
#     Uses 30 days upcoming, full markets for ALL matches (fetch_full_markets=True).
#     Prints only the final summary tables.
#     """
#     import time
#     from datetime import datetime

#     def fmt(slug: str) -> str:
#         return slug.replace("-", " ").title()

#     print("\n" + "=" * 80)
#     print(f"🧪 ALL BOOKMAKERS TEST (30 days upcoming, full markets enabled)")
#     print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#     print("=" * 80)

#     # ============================================================
#     # 1. BETIKA
#     # ============================================================
#     try:
#         from app.workers.bt_harvester import (
#             fetch_upcoming_matches,
#             fetch_live_matches,
#             get_full_markets,
#             CANONICAL_SPORT_IDS,
#             slug_to_bt_sport_id,
#         )
#     except ImportError as e:
#         print(f"❌ Cannot import Betika harvester: {e}")
#         return

#     betika_sports = list(CANONICAL_SPORT_IDS.keys())
#     betika_results = {}

#     for sport in betika_sports:
#         res = {"upcoming": 0, "live": 0, "markets": 0, "has_1x2": False, "has_ou": False, "has_hc": False, "status": "unknown"}
#         try:
#             # Fetch all upcoming matches for 30 days, fetch_full=True (gets all markets for every match)
#             upcoming = fetch_upcoming_matches(
#                 sport_slug=sport,
#                 days=30,
#                 fetch_full=True,        # ← full markets for all matches
#                 max_pages=30
#             )
#             if upcoming:
#                 res["upcoming"] = len(upcoming)
#                 # Use first match's markets (already have full markets)
#                 first = upcoming[0]
#                 markets = first.get("markets", {})
#                 res["markets"] = len(markets)
#                 res["has_1x2"] = any("1x2" in k or "match_winner" in k for k in markets)
#                 res["has_ou"] = any("over_under" in k for k in markets)
#                 res["has_hc"] = any("handicap" in k or "spread" in k for k in markets)
#                 res["status"] = "passed"
#             else:
#                 res["status"] = "warning"
#         except Exception:
#             res["status"] = "failed"

#         try:
#             bt_id = slug_to_bt_sport_id(sport)
#             live = fetch_live_matches(bt_id)
#             res["live"] = len(live)
#         except Exception:
#             pass

#         betika_results[sport] = res
#         time.sleep(0.3)

#     # ============================================================
#     # 2. ODIBETS (full markets for all matches)
#     # ============================================================
#     try:
#         from app.workers.od_harvester import (
#             fetch_upcoming_matches,
#             fetch_live_matches,
#             fetch_full_markets_for_match,
#             OD_SPORT_IDS,
#         )
#     except ImportError as e:
#         print(f"❌ Cannot import OdiBets harvester: {e}")
#         return

#     odibets_sports = list(OD_SPORT_IDS.keys())
#     odibets_results = {}

#     for sport in odibets_sports:
#         res = {"upcoming": 0, "live": 0, "markets": 0, "has_1x2": False, "has_ou": False, "has_hc": False, "status": "unknown"}
#         try:
#             # Fetch all upcoming matches for 30 days, fetch_full_markets=True (gets all markets for every match via smart fetcher)
#             upcoming = fetch_upcoming_matches(
#                 sport_slug=sport,
#                 days=30,
#                 fetch_full_markets=True,   # ← full markets for all matches
#             )
#             if upcoming:
#                 res["upcoming"] = len(upcoming)
#                 # Use first match's markets (already have full markets)
#                 first = upcoming[0]
#                 markets = first.get("markets", {})
#                 res["markets"] = len(markets)
#                 res["has_1x2"] = any("1x2" in k for k in markets)
#                 res["has_ou"] = any("over_under" in k for k in markets)
#                 res["has_hc"] = any("handicap" in k or "spread" in k for k in markets)
#                 res["status"] = "passed"
#             else:
#                 res["status"] = "warning"
#         except Exception:
#             res["status"] = "failed"

#         try:
#             live = fetch_live_matches(sport)
#             res["live"] = len(live)
#         except Exception:
#             pass

#         odibets_results[sport] = res
#         time.sleep(0.3)

#     # ============================================================
#     # 3. SPORTPESA (full markets for all matches)
#     # ============================================================
#     try:
#         from app.workers.sp_harvester import fetch_upcoming, SP_SPORT_ID
#     except ImportError as e:
#         print(f"❌ Cannot import Sportpesa harvester: {e}")
#         return

#     seen_ids = set()
#     sp_sports = []
#     for slug, sid in SP_SPORT_ID.items():
#         if sid not in seen_ids:
#             seen_ids.add(sid)
#             sp_sports.append((slug, sid))

#     sp_results = {}

#     for slug, sid in sp_sports:
#         res = {"upcoming": 0, "live": 0, "markets": 0, "has_1x2": False, "has_ou": False, "has_hc": False, "status": "unknown"}
#         try:
#             # Fetch all upcoming matches for 30 days, fetch_full_markets=True (gets all markets for every match)
#             matches = fetch_upcoming(
#                 sport_slug=slug,
#                 days=30,
#                 max_matches=None,          # no limit
#                 fetch_full_markets=True,   # ← full markets for all matches
#                 sleep_between=0.2,
#                 debug_ou=False,
#             )
#             if matches:
#                 res["upcoming"] = len(matches)
#                 # Use first match's markets (already have full markets)
#                 first = matches[0]
#                 markets = first.get("markets", {})
#                 res["markets"] = len(markets)
#                 res["has_1x2"] = any("1x2" in k or "match_winner" in k for k in markets)
#                 res["has_ou"] = any("over_under" in k for k in markets)
#                 res["has_hc"] = any("handicap" in k or "spread" in k for k in markets)
#                 res["status"] = "passed"
#             else:
#                 res["status"] = "warning"
#         except Exception as e:
#             print(f"Error for {slug}: {e}")
#             res["status"] = "failed"

#         res["live"] = 0  # Sportpesa live not tested here for simplicity
#         sp_results[slug] = res
#         time.sleep(0.3)

#     # ============================================================
#     # PRINT FINAL TABLES
#     # ============================================================
#     print("\n📊 BETIKA HARVESTER")
#     print("-" * 60)
#     print(f"{'Sport':<20} {'Up':<6} {'Live':<6} {'Mkts':<6} {'1X2':<4} {'O/U':<4} {'HC':<4} {'Status':<8}")
#     print("-" * 70)
#     for s, r in betika_results.items():
#         print(f"  {fmt(s):<18} {r['upcoming']:<6} {r['live']:<6} {r['markets']:<6} "
#               f"{'✓' if r['has_1x2'] else '✗':<4} {'✓' if r['has_ou'] else '✗':<4} {'✓' if r['has_hc'] else '✗':<4} {r['status']:<8}")

#     print("\n📊 ODIBETS HARVESTER")
#     print("-" * 60)
#     print(f"{'Sport':<20} {'Up':<6} {'Live':<6} {'Mkts':<6} {'1X2':<4} {'O/U':<4} {'HC':<4} {'Status':<8}")
#     print("-" * 70)
#     for s, r in odibets_results.items():
#         print(f"  {fmt(s):<18} {r['upcoming']:<6} {r['live']:<6} {r['markets']:<6} "
#               f"{'✓' if r['has_1x2'] else '✗':<4} {'✓' if r['has_ou'] else '✗':<4} {'✓' if r['has_hc'] else '✗':<4} {r['status']:<8}")

#     print("\n📊 SPORTPESA HARVESTER")
#     print("-" * 60)
#     print(f"{'Sport':<20} {'Up':<6} {'Live':<6} {'Mkts':<6} {'1X2':<4} {'O/U':<4} {'HC':<4} {'Status':<8}")
#     print("-" * 70)
#     for s, r in sp_results.items():
#         print(f"  {fmt(s):<18} {r['upcoming']:<6} {r['live']:<6} {r['markets']:<6} "
#               f"{'✓' if r['has_1x2'] else '✗':<4} {'✓' if r['has_ou'] else '✗':<4} {'✓' if r['has_hc'] else '✗':<4} {r['status']:<8}")

#     print("\n" + "=" * 80)
#     print("✅ Test completed.")

@flask_app.cli.command("run-all-harvesters")
def run_all_harvesters():
    """
    Run ALL 10 bookmakers for ALL sports (30 days upcoming) and persist to DB + Redis.
    Runs sports in parallel for faster execution.
    """
    import json
    import time
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Lazy imports to avoid circular deps
    from app.workers.bt_harvester import fetch_upcoming_matches, CANONICAL_SPORT_IDS
    from app.workers.od_harvester import fetch_upcoming_matches as od_fetch_upcoming, OD_SPORT_IDS
    from app.workers.sp_harvester import fetch_upcoming as sp_fetch_upcoming, SP_SPORT_ID
    from app.workers.b2b_harvester import fetch_all_b2b_sport, merge_b2b_by_match, B2B_SUPPORTED_SPORTS
    from app.workers.persist_hook import persist_merged_sync
    from app.workers.redis_bus import publish_snapshot, _r
    from app.workers.fuzzy_matcher import match_dict_to_candidate, bulk_align

    # Build unique list of sports from all bookmakers
    all_sports = set()
    all_sports.update(CANONICAL_SPORT_IDS.keys())
    all_sports.update(OD_SPORT_IDS.keys())
    all_sports.update([s for s, _ in SP_SPORT_ID.items()])
    all_sports.update(B2B_SUPPORTED_SPORTS)
    ALL_SPORTS = sorted(list(all_sports))  # deterministic order
    DAYS = 30
    MAX_WORKERS_SPORTS = 8  # parallel sports

    print(f"\n🚀 Starting ALL BOOKMAKERS HARVESTER ({len(ALL_SPORTS)} sports, {DAYS} days)")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    results = {}

    def process_sport(sport_slug):
        print(f"🏆 Processing {sport_slug}...")
        start = time.time()
        sport_result = {
            "sport": sport_slug,
            "upcoming": {},
            "merged_upcoming": None,
            "error": None
        }

        try:
            # 1. Betika
            bt_matches = fetch_upcoming_matches(sport_slug=sport_slug, days=DAYS, fetch_full=False, max_pages=30)
            sport_result["upcoming"]["betika"] = len(bt_matches) if bt_matches else 0

            # 2. OdiBets
            od_matches = od_fetch_upcoming(sport_slug=sport_slug, days=DAYS, fetch_full_markets=False)
            sport_result["upcoming"]["odibets"] = len(od_matches) if od_matches else 0

            # 3. Sportpesa (may raise if sport not supported)
            try:
                sp_matches = sp_fetch_upcoming(sport_slug=sport_slug, days=DAYS, max_matches=None, fetch_full_markets=False)
                sport_result["upcoming"]["sportpesa"] = len(sp_matches) if sp_matches else 0
            except Exception as e:
                sport_result["upcoming"]["sportpesa"] = 0

            # 4. B2B (7 bookmakers) – fetch all, then merge
            b2b_per_bk = fetch_all_b2b_sport(sport_slug, mode="upcoming", max_workers=7)
            b2b_merged = merge_b2b_by_match(b2b_per_bk, sport_slug)
            sport_result["upcoming"]["b2b_raw"] = {bk: len(matches) for bk, matches in b2b_per_bk.items()}
            sport_result["merged_upcoming"] = b2b_merged

            # Collect all BK matches (including non-B2B) for final unified merge
            all_bk_matches = {
                "betika": bt_matches,
                "odibets": od_matches,
                "sportpesa": sp_matches if sp_matches else [],
                **b2b_per_bk
            }
            sport_result["all_bk_matches"] = all_bk_matches

            # --- Cross-BK fuzzy merge using SP as anchor (if available) ---
            anchor_bk = "sportpesa" if sport_result["upcoming"].get("sportpesa", 0) > 0 else "betika"
            anchor_matches = all_bk_matches.get(anchor_bk, [])
            if anchor_matches:
                # Convert anchor to unified format
                anchor_unified = []
                for m in anchor_matches:
                    anchor_unified.append({
                        "id": m.get("sp_game_id") or m.get("bt_match_id") or m.get("od_match_id") or m.get("b2b_match_id"),
                        "betradar_id": m.get("betradar_id") or "",
                        "home_team_name": m.get("home_team", ""),
                        "away_team_name": m.get("away_team", ""),
                        "start_time": m.get("start_time"),
                        "competition_name": m.get("competition") or m.get("competition_name"),
                        "external_ids": {anchor_bk: m.get("sp_game_id") or m.get("bt_match_id") or m.get("od_match_id")}
                    })
                # Align other BKs
                unified_map = {u["id"]: u for u in anchor_unified}
                for bk, bk_matches_list in all_bk_matches.items():
                    if bk == anchor_bk or not bk_matches_list:
                        continue
                    candidates = [match_dict_to_candidate(m, bk_slug=bk) for m in bk_matches_list]
                    updates, _ = bulk_align(candidates, anchor_unified, sport_slug)
                    for upd in updates:
                        uid = upd.unified_match_id
                        if uid not in unified_map:
                            continue
                        rec = unified_map[uid]
                        rec.setdefault("bookmakers", {})[bk] = {
                            "match_id": upd.candidate.external_id,
                            "markets": upd.candidate.raw.get("markets", {})
                        }
                sport_result["unified_matches"] = list(unified_map.values())
            else:
                sport_result["unified_matches"] = b2b_merged  # fallback

            # --- Persist to DB ---
            if sport_result["unified_matches"]:
                stats = persist_merged_sync(sport_result["unified_matches"], sport_slug)
                sport_result["db_persisted"] = stats.get("persisted", 0)
            else:
                sport_result["db_persisted"] = 0

            # --- Publish to Redis ---
            try:
                r = _r()
                # Store unified snapshot
                unified_key = f"odds:unified:upcoming:{sport_slug}"
                r.setex(unified_key, 7200, json.dumps({
                    "sport": sport_slug,
                    "mode": "upcoming",
                    "matches": sport_result["unified_matches"],
                    "harvested_at": datetime.now().isoformat()
                }, default=str))
                # Also publish per-BK snapshots
                for bk, matches in all_bk_matches.items():
                    if matches:
                        publish_snapshot(bk, "upcoming", sport_slug, matches, ttl=3600)
                sport_result["redis_published"] = True
            except Exception as e:
                sport_result["redis_published"] = False
                print(f"   ⚠️ Redis publish error: {e}")

            sport_result["elapsed_ms"] = int((time.time() - start) * 1000)
            print(f"   ✅ Done: unified={len(sport_result.get('unified_matches', []))} db={sport_result.get('db_persisted',0)} redis={sport_result.get('redis_published',False)} in {sport_result['elapsed_ms']}ms")

        except Exception as e:
            sport_result["error"] = str(e)
            print(f"   ❌ FAILED: {e}")

        return sport_slug, sport_result

    # Run sports in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_SPORTS) as executor:
        futures = {executor.submit(process_sport, sport): sport for sport in ALL_SPORTS}
        for future in as_completed(futures):
            sport, res = future.result()
            results[sport] = res

    # Generate HTML report
    from datetime import datetime as dt
    timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
    html_path = f"all_bookmakers_report_{timestamp}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(f"""<html>
<head><title>All Bookmakers Report - {dt.now()}</title>
<style>
body{{font-family: Arial, sans-serif; margin:20px;}}
table{{border-collapse:collapse; margin-bottom:20px; width:100%;}}
th,td{{border:1px solid #ccc; padding:8px; text-align:left;}}
th{{background:#f0f0f0;}}
.good{{color:green;}} .bad{{color:red;}}
</style>
</head>
<body>
<h1>All Bookmakers Harvester Report</h1>
<p>Run at: {dt.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
<h2>Summary per Sport</h2>
<table>
<tr><th>Sport</th><th>Betika</th><th>OdiBets</th><th>Sportpesa</th><th>B2B (merged)</th><th>Unified</th><th>DB</th><th>Redis</th><th>Time (ms)</th></tr>
""")
        for sport, res in results.items():
            err = res.get("error")
            if err:
                f.write(f"<tr><td>{sport}</td><td colspan='7' class='bad'>❌ {err}</td></tr>")
            else:
                betika = res.get("upcoming", {}).get("betika", 0)
                odibets = res.get("upcoming", {}).get("odibets", 0)
                sportpesa = res.get("upcoming", {}).get("sportpesa", 0)
                b2b_merged = len(res.get("merged_upcoming", []))
                unified = len(res.get("unified_matches", []))
                db = res.get("db_persisted", 0)
                redis = "✅" if res.get("redis_published") else "❌"
                elapsed = res.get("elapsed_ms", 0)
                f.write(f"<tr><td>{sport}</td><td>{betika}</td><td>{odibets}</td><td>{sportpesa}</td><td>{b2b_merged}</td><td>{unified}</td><td>{db}</td><td>{redis}</td><td>{elapsed}</td></tr>")
        f.write("</table>")

        # Sample unified matches
        f.write("<h2>Sample Unified Matches</h2>")
        for sport, res in results.items():
            unified_matches = res.get("unified_matches", [])
            if unified_matches:
                sample = unified_matches[0]
                f.write(f"<h3>{sport}</h3>")
                f.write(f"<p><b>{sample.get('home_team_name')} vs {sample.get('away_team_name')}</b> – {sample.get('competition_name')}<br>")
                f.write(f"Betradar ID: {sample.get('betradar_id')}<br>")
                f.write("Bookmakers with odds:</p><ul>")
                for bk, bk_data in sample.get("bookmakers", {}).items():
                    markets_count = len(bk_data.get("markets", {}))
                    f.write(f"<li>{bk}: {markets_count} markets</li>")
                f.write("</ul><hr>")
        f.write("</body></html>")

    print(f"\n📄 HTML report saved to {html_path}")
    total_unified = sum(len(res.get("unified_matches", [])) for res in results.values())
    total_db = sum(res.get("db_persisted", 0) for res in results.values())
    print(f"\n🎉 Done! Total unified matches: {total_unified}")
    print(f"   Persisted to DB: {total_db}")
    print(f"   Redis published: {sum(1 for r in results.values() if r.get('redis_published'))}/{len(results)} sports")
    print(f"   Report: {html_path}")


@flask_app.route("/api/v1/dev/unified/matches", methods=["GET"])
def get_unified_matches():
    """Get unified matches from PostgreSQL (latest persisted)."""
    from app.models.odds import UnifiedMatch
    from app.views.odds_feed.customer_odds_view import _normalise_sport_slug

    sport = request.args.get("sport")
    limit = int(request.args.get("limit", 100))
    offset = int(request.args.get("offset", 0))

    query = UnifiedMatch.query.filter_by(match_status="upcoming")
    if sport:
        sport_slug = _normalise_sport_slug(sport)
        query = query.filter_by(sport_slug=sport_slug)

    total = query.count()
    matches = query.order_by(UnifiedMatch.start_time.desc()).offset(offset).limit(limit).all()

    return jsonify({
        "success": True,
        "total": total,
        "returned": len(matches),
        "offset": offset,
        "limit": limit,
        "matches": [m.to_dict() for m in matches]  # ensure to_dict exists
    })

if __name__ == "__main__":
    socketio.run(flask_app, debug=True, host="0.0.0.0", port=5500, use_reloader=False, log_output=True)