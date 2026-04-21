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
                days=2,                # look 2 days ahead
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

if __name__ == "__main__":
    socketio.run(flask_app, debug=True, host="0.0.0.0", port=5500, use_reloader=False, log_output=True)