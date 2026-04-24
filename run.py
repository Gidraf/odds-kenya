import click
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

def generate_html_with_json(all_data, bookmaker_name, mode, days, max_matches, expected_markets, bk, timestamp):
    import json
    from datetime import datetime

    # Convert data to JSON string for embedding
    data_json = json.dumps(all_data, default=str, indent=2)

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Normalized {bookmaker_name} {mode}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        .header {{ background: #2c3e50; color: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
        .controls {{ background: #ecf0f1; padding: 10px; border-radius: 8px; margin-bottom: 20px; }}
        button {{ background: #3498db; color: white; border: none; padding: 8px 15px; border-radius: 4px; cursor: pointer; margin-right: 10px; }}
        button:hover {{ background: #2980b9; }}
        .sport-section {{ margin-bottom: 30px; border: 1px solid #ccc; padding: 10px; border-radius: 5px; background: white; }}
        .match {{ border-bottom: 1px solid #eee; margin-bottom: 15px; padding-bottom: 10px; }}
        .market {{ margin: 5px 0; border-left: 3px solid #aaa; padding-left: 10px; }}
        .slug {{ font-weight: bold; display: inline-block; width: 250px; }}
        .outcome {{ display: inline-block; margin-right: 20px; background: #f5f5f5; padding: 2px 6px; border-radius: 3px; }}
        .collapsible {{ background-color: #f9f9f9; cursor: pointer; padding: 10px; width: 100%; border: none; text-align: left; outline: none; font-size: 1.1em; }}
        .active, .collapsible:hover {{ background-color: #e9e9e9; }}
        .content {{ padding: 0 18px; display: none; overflow-x: auto; background-color: #f1f1f1; }}
        .modal {{
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            overflow: auto;
            background-color: rgba(0,0,0,0.4);
        }}
        .modal-content {{
            background-color: #fefefe;
            margin: 5% auto;
            padding: 20px;
            border: 1px solid #888;
            width: 80%;
            max-height: 80%;
            overflow-y: auto;
            border-radius: 8px;
        }}
        .close {{
            color: #aaa;
            float: right;
            font-size: 28px;
            font-weight: bold;
            cursor: pointer;
        }}
        .close:hover {{ color: black; }}
        pre {{
            background: #2d2d2d;
            color: #f8f8f2;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
            font-size: 12px;
        }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>Normalized Markets – {bookmaker_name} ({mode})</h1>
        <p>Generated: {datetime.now()}</p>
        <p>Days: {days} | Max matches per sport: {max_matches}</p>
    </div>
    <div class="controls">
        <button onclick="showJsonModal()">📋 View JSON Data</button>
        <button onclick="downloadJson()">💾 Download JSON</button>
        <button onclick="copyJson()">📄 Copy JSON to Clipboard</button>
    </div>
"""

    for sport_slug, matches in all_data.items():
        if not matches:
            continue
        html += f"""
    <div class="sport-section">
        <button class="collapsible" onclick="toggleCollapsible(this)">{sport_slug.upper()} – {len(matches)} matches</button>
        <div class="content">
"""
        for idx, m in enumerate(matches):
            html += f"""
            <div class="match">
                <h3>{idx+1}. {m.get('home_team')} vs {m.get('away_team')}</h3>
                <p><strong>Competition:</strong> {m.get('competition')}<br>
                <strong>Start:</strong> {m.get('start_time')}<br>
                <strong>Betradar ID:</strong> {m.get('betradar_id') or 'N/A'}</p>
                <h4>Markets (normalized)</h4>
"""
            actual_markets = m.get('markets', {})
            if actual_markets:
                for slug, outcomes in actual_markets.items():
                    html += f'                <div class="market"><span class="slug">{slug}</span> '
                    for out, odds in outcomes.items():
                        html += f'<span class="outcome">{out}: {odds:.2f}</span> '
                    html += '</div>\n'
            else:
                html += '                <p><em>No markets returned by API.</em></p>\n'
            if bk == "sp" and sport_slug in expected_markets:
                expected = expected_markets.get(sport_slug, [])
                missing = [e for e in expected if e not in actual_markets]
                if missing:
                    html += f'                <div style="margin-top:10px; color:#888;"><em>Missing expected markets: {", ".join(missing)}</em></div>\n'
            html += '            </div>\n'
        html += '        </div>\n    </div>\n'

    # Add modal for JSON viewer
    html += f"""
</div>

<div id="jsonModal" class="modal">
    <div class="modal-content">
        <span class="close" onclick="closeModal()">&times;</span>
        <h2>JSON Data</h2>
        <pre id="jsonDisplay"></pre>
    </div>
</div>

<script>
    const jsonData = {data_json};

    function toggleCollapsible(el) {{
        el.classList.toggle("active");
        var content = el.nextElementSibling;
        content.style.display = content.style.display === "block" ? "none" : "block";
    }}

    function showJsonModal() {{
        document.getElementById("jsonDisplay").textContent = JSON.stringify(jsonData, null, 2);
        document.getElementById("jsonModal").style.display = "block";
    }}

    function closeModal() {{
        document.getElementById("jsonModal").style.display = "none";
    }}

    function downloadJson() {{
        const dataStr = JSON.stringify(jsonData, null, 2);
        const blob = new Blob([dataStr], {{type: "application/json"}});
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "normalized_{bk}_{mode}_{timestamp}.json";
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }}

    function copyJson() {{
        const dataStr = JSON.stringify(jsonData, null, 2);
        navigator.clipboard.writeText(dataStr).then(() => {{
            alert("JSON copied to clipboard!");
        }}).catch(err => {{
            alert("Failed to copy: " + err);
        }});
    }}

    window.onclick = function(event) {{
        if (event.target == document.getElementById("jsonModal")) {{
            closeModal();
        }}
    }}
</script>
</body>
</html>"""
    return html


@flask_app.cli.command("show-normalized")
@click.option("--bk", default="sp", help="Bookmaker: sp, bt, od")
@click.option("--mode", default="upcoming", help="upcoming or live")
@click.option("--sport", default=None, help="Limit to one sport slug")
@click.option("--days", default=30, help="Days ahead for upcoming")
@click.option("--max-matches", default=10, help="Max matches to display per sport")
@click.option("--format", default="html", type=click.Choice(["html", "json"]), help="Output format")
@click.option("--output", default=None, help="Output file path (optional)")
def show_normalized(bk, mode, sport, days, max_matches, format, output):
    """
    Show normalized matches for a single bookmaker (sp, bt, od).
    Markets are already normalized by the respective harvester.
    Output as HTML (default) or JSON.
    """
    import json
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Helper to get the correct fetch function and expected markets (for sp)
    expected_markets = {}

    if bk == "sp":
        from app.workers.sp_harvester import fetch_upcoming as fetch_func
        from app.workers.sp_mapper import SPORT_PRIMARY_MARKETS
        bookmaker_name = "Sportpesa"
        expected_markets = SPORT_PRIMARY_MARKETS
    elif bk == "bt":
        from app.workers.bt_harvester import fetch_upcoming_matches as fetch_func
        bookmaker_name = "Betika"
    elif bk == "od":
        from app.workers.od_harvester import fetch_upcoming as fetch_func
        bookmaker_name = "OdiBets"
    else:
        print(f"Unknown bookmaker: {bk}")
        return

    # Determine sports list
    if sport:
        sports = [sport]
    else:
        if bk == "sp":
            from app.workers.sp_harvester import SP_SPORT_ID
            sports = list(SP_SPORT_ID.keys())
        elif bk == "bt":
            from app.workers.bt_harvester import CANONICAL_SPORT_IDS
            sports = list(CANONICAL_SPORT_IDS.keys())
        else:
            from app.workers.od_harvester import OD_SPORT_IDS
            sports = list(OD_SPORT_IDS.keys())

    print(f"\n📊 Fetching {mode} data for {bookmaker_name} ({len(sports)} sports, days={days})")
    print(f"   {datetime.now()}\n")

    all_data = {}

    def fetch_sport(sport_slug):
        try:
            if mode == "upcoming":
                matches = fetch_func(sport_slug=sport_slug, days=days, max_matches=None, fetch_full_markets=True)
            else:
                matches = []  # live not fully implemented
            return sport_slug, matches[:max_matches] if max_matches else matches
        except Exception as e:
            print(f"  {sport_slug} error: {e}")
            return sport_slug, []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_sport, s): s for s in sports}
        for future in as_completed(futures):
            s, m = future.result()
            all_data[s] = m
            print(f"  {s}: {len(m)} matches")

    # Debug: print raw normalized market slugs for first match of each sport (console only)
    print("\n🔍 Raw normalized markets for first match of each sport (if any):")
    for sport_slug, matches in all_data.items():
        if matches:
            first = matches[0]
            print(f"\n  {sport_slug.upper()} – {first.get('home_team')} vs {first.get('away_team')}")
            print(f"    Betradar ID: {first.get('betradar_id')}")
            if first.get("markets"):
                for slug, outcomes in first.get("markets", {}).items():
                    print(f"      {slug}: {', '.join(outcomes.keys())}")
            else:
                print("      No markets")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not output:
        if format == "json":
            output = f"normalized_{bk}_{mode}_{timestamp}.json"
        else:
            output = f"normalized_{bk}_{mode}_{timestamp}.html"
    else:
        if format == "json" and not output.endswith(".json"):
            output += ".json"
        elif format == "html" and not output.endswith(".html"):
            output += ".html"

    if format == "json":
        # Save only JSON
        output_data = {
            "bookmaker": bookmaker_name,
            "mode": mode,
            "days": days,
            "max_matches": max_matches,
            "generated_at": datetime.now().isoformat(),
            "sports": all_data
        }
        with open(output, "w", encoding="utf-8") as f:
            json.dump(output_data, f, default=str, indent=2)
        print(f"\n💾 JSON saved to: {output}")
    else:
        # Generate HTML with embedded JSON viewer
        data_json = json.dumps(all_data, default=str, indent=2)

        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Normalized {bookmaker_name} {mode}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        .header {{ background: #2c3e50; color: white; padding: 15px; border-radius: 8px; margin-bottom: 20px; }}
        .controls {{ background: #ecf0f1; padding: 10px; border-radius: 8px; margin-bottom: 20px; }}
        button {{ background: #3498db; color: white; border: none; padding: 8px 15px; border-radius: 4px; cursor: pointer; margin-right: 10px; }}
        button:hover {{ background: #2980b9; }}
        .sport-section {{ margin-bottom: 30px; border: 1px solid #ccc; padding: 10px; border-radius: 5px; background: white; }}
        .match {{ border-bottom: 1px solid #eee; margin-bottom: 15px; padding-bottom: 10px; }}
        .market {{ margin: 5px 0; border-left: 3px solid #aaa; padding-left: 10px; }}
        .slug {{ font-weight: bold; display: inline-block; width: 250px; }}
        .outcome {{ display: inline-block; margin-right: 20px; background: #f5f5f5; padding: 2px 6px; border-radius: 3px; }}
        .collapsible {{ background-color: #f9f9f9; cursor: pointer; padding: 10px; width: 100%; border: none; text-align: left; outline: none; font-size: 1.1em; }}
        .active, .collapsible:hover {{ background-color: #e9e9e9; }}
        .content {{ padding: 0 18px; display: none; overflow-x: auto; background-color: #f1f1f1; }}
        .modal {{
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            overflow: auto;
            background-color: rgba(0,0,0,0.5);
        }}
        .modal-content {{
            background-color: #fefefe;
            margin: 5% auto;
            padding: 20px;
            border: 1px solid #888;
            width: 80%;
            max-height: 80%;
            overflow-y: auto;
            border-radius: 8px;
        }}
        .close {{
            color: #aaa;
            float: right;
            font-size: 28px;
            font-weight: bold;
            cursor: pointer;
        }}
        .close:hover {{ color: black; }}
        pre {{
            background: #2d2d2d;
            color: #f8f8f2;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
            font-size: 12px;
        }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>Normalized Markets – {bookmaker_name} ({mode})</h1>
        <p>Generated: {datetime.now()}</p>
        <p>Days: {days} | Max matches per sport: {max_matches}</p>
    </div>
    <div class="controls">
        <button onclick="showJsonModal()">📋 View JSON Data</button>
        <button onclick="downloadJson()">💾 Download JSON</button>
        <button onclick="copyJson()">📄 Copy JSON to Clipboard</button>
    </div>
"""

        for sport_slug, matches in all_data.items():
            if not matches:
                continue
            html_content += f"""
    <div class="sport-section">
        <button class="collapsible" onclick="toggleCollapsible(this)">{sport_slug.upper()} – {len(matches)} matches</button>
        <div class="content">
"""
            for idx, m in enumerate(matches):
                html_content += f"""
            <div class="match">
                <h3>{idx+1}. {m.get('home_team')} vs {m.get('away_team')}</h3>
                <p><strong>Competition:</strong> {m.get('competition')}<br>
                <strong>Start:</strong> {m.get('start_time')}<br>
                <strong>Betradar ID:</strong> {m.get('betradar_id') or 'N/A'}</p>
                <h4>Markets (normalized)</h4>
"""
                actual_markets = m.get('markets', {})
                if actual_markets:
                    for slug, outcomes in actual_markets.items():
                        html_content += f'                <div class="market"><span class="slug">{slug}</span> '
                        for out, odds in outcomes.items():
                            html_content += f'<span class="outcome">{out}: {odds:.2f}</span> '
                        html_content += '</div>\n'
                else:
                    html_content += '                <p><em>No markets returned by API.</em></p>\n'
                if bk == "sp" and sport_slug in expected_markets:
                    expected = expected_markets.get(sport_slug, [])
                    missing = [e for e in expected if e not in actual_markets]
                    if missing:
                        html_content += f'                <div style="margin-top:10px; color:#888;"><em>Missing expected markets: {", ".join(missing)}</em></div>\n'
                html_content += '            </div>\n'
            html_content += '        </div>\n    </div>\n'

        html_content += f"""
</div>

<div id="jsonModal" class="modal">
    <div class="modal-content">
        <span class="close" onclick="closeModal()">&times;</span>
        <h2>JSON Data</h2>
        <pre id="jsonDisplay"></pre>
    </div>
</div>

<script>
    const jsonData = {data_json};

    function toggleCollapsible(el) {{
        el.classList.toggle("active");
        var content = el.nextElementSibling;
        content.style.display = content.style.display === "block" ? "none" : "block";
    }}

    function showJsonModal() {{
        document.getElementById("jsonDisplay").textContent = JSON.stringify(jsonData, null, 2);
        document.getElementById("jsonModal").style.display = "block";
    }}

    function closeModal() {{
        document.getElementById("jsonModal").style.display = "none";
    }}

    function downloadJson() {{
        const dataStr = JSON.stringify(jsonData, null, 2);
        const blob = new Blob([dataStr], {{type: "application/json"}});
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "normalized_{bk}_{mode}_{timestamp}.json";
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }}

    function copyJson() {{
        const dataStr = JSON.stringify(jsonData, null, 2);
        navigator.clipboard.writeText(dataStr).then(() => {{
            alert("JSON copied to clipboard!");
        }}).catch(err => {{
            alert("Failed to copy: " + err);
        }});
    }}

    window.onclick = function(event) {{
        if (event.target == document.getElementById("jsonModal")) {{
            closeModal();
        }}
    }}
</script>
</body>
</html>"""

        with open(output, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"\n📄 HTML report saved: {output}")

        # Also save a standalone JSON file
        json_file = output.replace(".html", ".json")
        output_data = {
            "bookmaker": bookmaker_name,
            "mode": mode,
            "days": days,
            "max_matches": max_matches,
            "generated_at": datetime.now().isoformat(),
            "sports": all_data
        }
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, default=str, indent=2)
        print(f"💾 JSON companion saved: {json_file}")



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

    # Lazy imports to avoid circular dependencies
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
            except Exception:
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
                "sportpesa": sp_matches if 'sp_matches' in locals() else [],
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

            # --- Persist to DB (WITH APP CONTEXT) ---
            with flask_app.app_context():
                if sport_result["unified_matches"]:
                    stats = persist_merged_sync(sport_result["unified_matches"], sport_slug)
                    sport_result["db_persisted"] = stats.get("persisted", 0)
                else:
                    sport_result["db_persisted"] = 0

            # --- Publish to Redis (no app context needed) ---
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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = f"all_bookmakers_report_{timestamp}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(f"""<html>
<head><title>All Bookmakers Report - {datetime.now()}</title>
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
<p>Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
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

@flask_app.cli.command("debug-markets-per-bookmaker")
def debug_markets_per_bookmaker():
    """
    For each sport, fetch one match from each bookmaker (Betika, OdiBets, Sportpesa, B2B)
    and display all markets they return (without unification).
    Saves HTML and JSON reports.
    """
    import json
    from datetime import datetime
    from app.workers.bt_harvester import fetch_upcoming_matches, get_full_markets, CANONICAL_SPORT_IDS
    from app.workers.od_harvester import fetch_upcoming_matches as od_fetch, fetch_full_markets_for_match, OD_SPORT_IDS
    from app.workers.sp_harvester import fetch_upcoming as sp_fetch, SP_SPORT_ID
    from app.workers.b2b_harvester import fetch_single_bk, B2B_BOOKMAKERS, B2B_SUPPORTED_SPORTS

    # Combine all sports from all sources
    all_sports = set()
    all_sports.update(CANONICAL_SPORT_IDS.keys())
    all_sports.update(OD_SPORT_IDS.keys())
    all_sports.update([s for s, _ in SP_SPORT_ID.items()])
    all_sports.update(B2B_SUPPORTED_SPORTS)
    all_sports = sorted(list(all_sports))

    report = []
    json_data = {}

    for sport in all_sports:
        print(f"Processing {sport}...")
        sport_data = {"sport": sport, "bookmakers": {}}

        # Betika
        try:
            bt_matches = fetch_upcoming_matches(sport_slug=sport, days=3, fetch_full=False, max_pages=1)
            if bt_matches:
                first = bt_matches[0]
                pid = first.get("bt_parent_id")
                if pid:
                    full_markets = get_full_markets(pid, sport)
                    sport_data["bookmakers"]["betika"] = {
                        "match": f"{first.get('home_team')} vs {first.get('away_team')}",
                        "start_time": first.get("start_time"),
                        "market_count": len(full_markets),
                        "markets": full_markets  # dict of {slug: {outcome: price}}
                    }
        except Exception as e:
            sport_data["bookmakers"]["betika"] = {"error": str(e)}

        # OdiBets
        try:
            od_matches = od_fetch(sport_slug=sport, days=3, fetch_full_markets=False, max_matches=1)
            if od_matches:
                first = od_matches[0]
                br_id = first.get("betradar_id")
                if br_id:
                    full_markets = fetch_full_markets_for_match(br_id, OD_SPORT_IDS.get(sport, 1))
                    sport_data["bookmakers"]["odibets"] = {
                        "match": f"{first.get('home_team')} vs {first.get('away_team')}",
                        "start_time": first.get("start_time"),
                        "market_count": len(full_markets),
                        "markets": full_markets
                    }
        except Exception as e:
            sport_data["bookmakers"]["odibets"] = {"error": str(e)}

        # Sportpesa
        try:
            sp_matches = sp_fetch(sport_slug=sport, days=3, max_matches=1, fetch_full_markets=True)
            if sp_matches:
                first = sp_matches[0]
                sport_data["bookmakers"]["sportpesa"] = {
                    "match": f"{first.get('home_team')} vs {first.get('away_team')}",
                    "start_time": first.get("start_time"),
                    "market_count": first.get("market_count", 0),
                    "markets": first.get("markets", {})
                }
        except Exception as e:
            sport_data["bookmakers"]["sportpesa"] = {"error": str(e)}

        # B2B bookmakers
        for bk in B2B_BOOKMAKERS:
            slug = bk["slug"]
            try:
                # Fetch a small number of matches (page 1, limit 1)
                raw = fetch_single_bk(bk, sport, mode="upcoming", page=1, page_size=1)
                if raw:
                    first = raw[0]
                    sport_data["bookmakers"][slug] = {
                        "match": f"{first.get('home_team')} vs {first.get('away_team')}",
                        "start_time": first.get("start_time"),
                        "market_count": first.get("market_count", 0),
                        "markets": first.get("markets", {})
                    }
            except Exception as e:
                sport_data["bookmakers"][slug] = {"error": str(e)}

        json_data[sport] = sport_data

        # Build HTML for this sport
        html = f"<h2>{sport}</h2>"
        for bk, data in sport_data["bookmakers"].items():
            if "error" in data:
                html += f"<h3>{bk}</h3><p>❌ {data['error']}</p>"
                continue
            html += f"<h3>{bk}</h3>"
            html += f"<p><b>Match:</b> {data.get('match', 'N/A')}<br>"
            html += f"<b>Start:</b> {data.get('start_time', 'N/A')}<br>"
            html += f"<b>Total markets:</b> {data.get('market_count', 0)}</p>"
            markets = data.get("markets", {})
            if markets:
                html += "<ul>"
                for mkt_slug, outcomes in markets.items():
                    outcome_count = len(outcomes)
                    html += f"<li><b>{mkt_slug}</b>: {outcome_count} outcomes</li>"
                html += "</ul>"
            else:
                html += "<p>No markets available.</p>"
        report.append(html)

    # Write HTML file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = f"markets_per_bookmaker_{timestamp}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(f"""<html>
<head><title>Markets per Bookmaker - {timestamp}</title>
<style>
body{{font-family: Arial; margin:20px;}}
h2{{background:#f0f0f0; padding:10px;}}
h3{{margin-top:20px; color:#0066cc;}}
li{{margin:5px 0;}}
</style>
</head>
<body>
<h1>Markets Returned by Each Bookmaker (per sport, first match)</h1>
<p>Generated: {datetime.now()}</p>
""")
        for html_block in report:
            f.write(html_block)
        f.write("</body></html>")

    # Write JSON file
    json_path = f"markets_per_bookmaker_{timestamp}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, default=str)

    print(f"\n✅ HTML report: {html_path}")
    print(f"✅ JSON data:   {json_path}")


@flask_app.cli.command("debug-unify")
@click.option("--sport", default="soccer", help="Sport slug to debug")
@click.option("--days", default=7, help="Days ahead to fetch")
def debug_unify(sport, days):
    """
    Debug cross-bookmaker unification for a specific sport.
    Shows raw markets per match per bookmaker, then attempts unification.
    Outputs JSON + HTML with detailed matching results.
    """
    import json
    import time
    from datetime import datetime
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor

    from app.workers.bt_harvester import fetch_upcoming_matches, CANONICAL_SPORT_IDS
    from app.workers.od_harvester import fetch_upcoming_matches as od_fetch_upcoming, OD_SPORT_IDS
    from app.workers.sp_harvester import fetch_upcoming as sp_fetch_upcoming, SP_SPORT_ID
    from app.workers.b2b_harvester import fetch_all_b2b_sport, B2B_BOOKMAKERS
    from app.workers.fuzzy_matcher import match_dict_to_candidate, bulk_align

    print(f"\n🔍 Debugging unification for: {sport} (next {days} days)")
    print(f"   {datetime.now()}\n")

    # Fetch all bookmakers
    bookmaker_data = {}

    # 1. Betika
    try:
        bt = fetch_upcoming_matches(sport_slug=sport, days=days, fetch_full=False, max_pages=30)
        bookmaker_data["betika"] = bt
        print(f"✅ Betika: {len(bt)} matches")
    except Exception as e:
        print(f"❌ Betika: {e}")
        bookmaker_data["betika"] = []

    # 2. OdiBets
    try:
        od = od_fetch_upcoming(sport_slug=sport, days=days, fetch_full_markets=False)
        bookmaker_data["odibets"] = od
        print(f"✅ OdiBets: {len(od)} matches")
    except Exception as e:
        print(f"❌ OdiBets: {e}")
        bookmaker_data["odibets"] = []

    # 3. Sportpesa
    try:
        sp = sp_fetch_upcoming(sport_slug=sport, days=days, max_matches=None, fetch_full_markets=False)
        bookmaker_data["sportpesa"] = sp
        print(f"✅ Sportpesa: {len(sp)} matches")
    except Exception as e:
        print(f"❌ Sportpesa: {e}")
        bookmaker_data["sportpesa"] = []

    # 4. B2B (7 bookmakers)
    try:
        b2b_per_bk = fetch_all_b2b_sport(sport, mode="upcoming", max_workers=7)
        for bk, matches in b2b_per_bk.items():
            bookmaker_data[bk] = matches
            print(f"✅ {bk}: {len(matches)} matches")
    except Exception as e:
        print(f"❌ B2B: {e}")

    # Determine anchor bookmaker: the one with the MOST matches
    anchor_bk = max(bookmaker_data.items(), key=lambda x: len(x[1]))[0]
    anchor_matches = bookmaker_data.get(anchor_bk, [])
    if not anchor_matches:
        print("\n⚠️ No anchor matches found – unification impossible.")
        return

    print(f"\n📌 Using anchor: {anchor_bk} ({len(anchor_matches)} matches)")

    # Convert anchor to unified format
    anchor_list = []
    for m in anchor_matches:
        anchor_list.append({
            "id": m.get("sp_game_id") or m.get("bt_match_id") or m.get("od_match_id") or m.get("b2b_match_id"),
            "betradar_id": m.get("betradar_id") or "",
            "home_team_name": m.get("home_team", ""),
            "away_team_name": m.get("away_team", ""),
            "start_time": m.get("start_time"),
            "competition_name": m.get("competition") or m.get("competition_name"),
            "external_ids": {anchor_bk: m.get("sp_game_id") or m.get("bt_match_id") or m.get("od_match_id")}
        })

    # Align other bookmakers
    unified_map = {u["id"]: u for u in anchor_list}
    alignment_log = []  # store which matches were aligned

    for bk, matches in bookmaker_data.items():
        if bk == anchor_bk or not matches:
            continue
        candidates = [match_dict_to_candidate(m, bk_slug=bk) for m in matches]
        updates, creates = bulk_align(candidates, anchor_list, sport)
        for upd in updates:
            uid = upd.unified_match_id
            if uid in unified_map:
                unified_map[uid].setdefault("bookmakers", {})[bk] = {
                    "match_id": upd.candidate.external_id,
                    "markets": upd.candidate.raw.get("markets", {})
                }
                alignment_log.append({
                    "bk": bk,
                    "match": f"{upd.candidate.raw.get('home_team')} vs {upd.candidate.raw.get('away_team')}",
                    "start_time": upd.candidate.raw.get('start_time'),
                    "matched_to": uid,
                    "confidence": upd.confidence
                })
        for create in creates:
            # New match not in anchor – add to unified
            cand = create.candidate
            new_id = f"new_{bk}_{cand.external_id}"
            unified_map[new_id] = {
                "id": new_id,
                "betradar_id": "",
                "home_team_name": cand.raw.get("home_team", ""),
                "away_team_name": cand.raw.get("away_team", ""),
                "start_time": cand.raw.get("start_time"),
                "competition_name": cand.raw.get("competition") or cand.raw.get("competition_name"),
                "external_ids": {bk: cand.external_id},
                "bookmakers": {bk: {"match_id": cand.external_id, "markets": cand.raw.get("markets", {})}}
            }
            alignment_log.append({
                "bk": bk,
                "match": f"{cand.raw.get('home_team')} vs {cand.raw.get('away_team')}",
                "start_time": cand.raw.get('start_time'),
                "matched_to": "NEW (not in anchor)",
                "confidence": create.confidence
            })

    unified_matches = list(unified_map.values())

    # Count how many bookmakers contributed to each unified match
    for um in unified_matches:
        um["bookmaker_count"] = len(um.get("bookmakers", {})) + (1 if anchor_bk in um.get("external_ids", {}) else 0)

    # Identify failures: matches that are only from one bookmaker
    orphans = [um for um in unified_matches if um["bookmaker_count"] == 1]

    # Prepare output data
    output = {
        "sport": sport,
        "days": days,
        "anchor_bk": anchor_bk,
        "bookmaker_counts": {bk: len(matches) for bk, matches in bookmaker_data.items()},
        "unified_total": len(unified_matches),
        "orphan_count": len(orphans),
        "alignment_log": alignment_log,
        "unified_matches": unified_matches,
        "raw_bookmaker_matches": bookmaker_data,  # full raw data
    }

    # Save JSON
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = f"unify_debug_{sport}_{timestamp}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, default=str, indent=2)
    print(f"\n💾 JSON saved: {json_path}")

    # Generate HTML report
    html_path = f"unify_debug_{sport}_{timestamp}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(f"""<html>
<head><title>Unification Debug – {sport}</title>
<style>
body{{font-family: Arial, sans-serif; margin:20px;}}
table{{border-collapse:collapse; margin-bottom:20px; width:100%;}}
th,td{{border:1px solid #ccc; padding:8px; text-align:left;}}
th{{background:#f0f0f0;}}
.collapsible{{background-color:#f9f9f9; cursor:pointer; padding:10px; width:100%; border:none; text-align:left; outline:none;}}
.active, .collapsible:hover{{background-color:#e9e9e9;}}
.content{{padding:0 18px; display:none; overflow:hidden; background-color:#f1f1f1;}}
.bad{{color:red;}} .good{{color:green;}}
</style>
<script>
function toggleCollapsible(el) {{
    el.classList.toggle("active");
    var content = el.nextElementSibling;
    if (content.style.display === "block") content.style.display = "none";
    else content.style.display = "block";
}}
</script>
</head>
<body>
<h1>🔗 Unification Debug – {sport.upper()}</h1>
<p>Generated: {datetime.now()}</p>
<p>Anchor bookmaker: {anchor_bk} (most matches: {len(anchor_matches)})</p>
<h2>Summary</h2>
<ul>
<li>Total raw matches across all bookmakers: {sum(output['bookmaker_counts'].values())}</li>
<li>Unified matches: {len(unified_matches)}</li>
<li>Orphan matches (only one bookmaker): {len(orphans)}</li>
</ul>

<h2>Raw matches per bookmaker</h2>
<table>
   <tr><th>Bookmaker</th><th>Match count</th></tr>
""")
        for bk, cnt in output['bookmaker_counts'].items():
            f.write(f"<tr><td>{bk}</td><td>{cnt}</td></tr>")
        f.write("</table>")

        f.write("<h2>Alignment log</h2><table><tr><th>Bookmaker</th><th>Match</th><th>Time</th><th>Matched to</th><th>Confidence</th></tr>")
        for log in alignment_log:
            match_time = log.get('start_time', '') or ''
            # Format time for display (first 16 chars: YYYY-MM-DD HH:MM)
            time_display = match_time[:16] if match_time else '?'
            f.write(f"<tr><td>{log['bk']}</td><td>{log['match']}</td><td>{time_display}</td><td>{log['matched_to']}</td><td>{log['confidence']:.2f}</td></tr>")
        f.write("</table>")

        f.write("<h2>Unified matches (collapsible)</h2>")
        for idx, um in enumerate(unified_matches[:50]):  # limit for readability
            # Format start time nicely
            start_time = um.get("start_time", "")
            time_display = start_time[:16] if start_time else "TBD"
            f.write(f'<button class="collapsible" onclick="toggleCollapsible(this)">#{idx+1}: {um.get("home_team_name")} vs {um.get("away_team_name")} – {um["bookmaker_count"]} bookmakers – {time_display}</button>')
            f.write('<div class="content">')
            f.write(f"<p><b>Start time:</b> {um.get('start_time')}<br>")
            f.write(f"<b>Betradar ID:</b> {um.get('betradar_id')}<br>")
            f.write(f"<b>Competition:</b> {um.get('competition_name')}</p>")
            f.write("<h4>Bookmakers & markets</h4><ul>")
            for bk, bk_data in um.get("bookmakers", {}).items():
                mkts = list(bk_data.get("markets", {}).keys())
                f.write(f"<li><b>{bk}</b>: {len(mkts)} markets – {', '.join(mkts[:10])}{'...' if len(mkts)>10 else ''}</li>")
            # Also show anchor bookmaker if present
            if anchor_bk in um.get("external_ids", {}):
                anchor_match = next((m for m in bookmaker_data.get(anchor_bk, []) if m.get("sp_game_id") == um["external_ids"][anchor_bk]), None)
                if anchor_match:
                    mkts = list(anchor_match.get("markets", {}).keys())
                    f.write(f"<li><b>{anchor_bk} (anchor)</b>: {len(mkts)} markets – {', '.join(mkts[:10])}{'...' if len(mkts)>10 else ''}</li>")
            f.write("</ul>")
            f.write('</div>')
        f.write("</body></html>")

    print(f"📄 HTML report saved: {html_path}")

    print(f"\n🎯 Orphan matches (not unified with any other bookmaker): {len(orphans)}")
    if orphans:
        print("  First 5 orphans:")
        for o in orphans[:5]:
            start_time = o.get("start_time", "")
            time_display = start_time[:16] if start_time else "TBD"
            print(f"    - {o.get('home_team_name')} vs {o.get('away_team_name')} ({time_display}) – only from {list(o.get('external_ids', {}).keys())[0]}")


@flask_app.cli.command("full-report")
@click.option("--days", default=30, help="Days ahead to fetch")
@click.option("--max-matches", default=10, help="Max unified matches to display per sport")
def full_report(days, max_matches):
    """
    Generate a complete odds comparison report for ALL sports (30 days).
    Outputs HTML and JSON files.
    """
    import json
    import time
    from datetime import datetime
    from collections import defaultdict
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from app.workers.bt_harvester import fetch_upcoming_matches, CANONICAL_SPORT_IDS
    from app.workers.od_harvester import fetch_upcoming_matches as od_fetch_upcoming, OD_SPORT_IDS
    from app.workers.sp_harvester import fetch_upcoming as sp_fetch_upcoming, SP_SPORT_ID
    from app.workers.b2b_harvester import fetch_all_b2b_sport, B2B_BOOKMAKERS, B2B_SUPPORTED_SPORTS
    from app.workers.fuzzy_matcher import match_dict_to_candidate, bulk_align

    # Build list of all sports
    all_sports = set()
    all_sports.update(CANONICAL_SPORT_IDS.keys())
    all_sports.update(OD_SPORT_IDS.keys())
    all_sports.update([s for s, _ in SP_SPORT_ID.items()])
    all_sports.update(B2B_SUPPORTED_SPORTS)
    ALL_SPORTS = sorted(list(all_sports))

    print(f"\n📊 Generating full odds comparison report for ALL {len(ALL_SPORTS)} sports")
    print(f"   Days ahead: {days} | Display limit: {max_matches} matches per sport")
    print(f"   Started at: {datetime.now()}\n")

    def process_sport(sport_slug):
        print(f"🏆 Processing {sport_slug}...")
        start = time.time()
        sport_data = {
            "sport": sport_slug,
            "bookmaker_counts": {},
            "unified_matches": [],
            "errors": []
        }

        # Fetch from each bookmaker
        try:
            bt = fetch_upcoming_matches(sport_slug=sport_slug, days=days, fetch_full=False, max_pages=30)
            sport_data["bookmaker_counts"]["betika"] = len(bt)
        except Exception as e:
            sport_data["errors"].append(f"betika: {e}")
            bt = []

        try:
            od = od_fetch_upcoming(sport_slug=sport_slug, days=days, fetch_full_markets=False)
            sport_data["bookmaker_counts"]["odibets"] = len(od)
        except Exception as e:
            sport_data["errors"].append(f"odibets: {e}")
            od = []

        try:
            sp = sp_fetch_upcoming(sport_slug=sport_slug, days=days, max_matches=None, fetch_full_markets=False)
            sport_data["bookmaker_counts"]["sportpesa"] = len(sp)
        except Exception as e:
            sport_data["errors"].append(f"sportpesa: {e}")
            sp = []

        try:
            b2b_per_bk = fetch_all_b2b_sport(sport_slug, mode="upcoming", max_workers=7)
            for bk, matches in b2b_per_bk.items():
                sport_data["bookmaker_counts"][bk] = len(matches)
        except Exception as e:
            sport_data["errors"].append(f"b2b: {e}")
            b2b_per_bk = {}

        # Combine all matches
        all_bk_matches = {
            "betika": bt,
            "odibets": od,
            "sportpesa": sp,
            **b2b_per_bk
        }

        # Choose anchor (bookmaker with most matches)
        anchor_bk = max(all_bk_matches.items(), key=lambda x: len(x[1]))[0]
        anchor_matches = all_bk_matches[anchor_bk]

        if not anchor_matches:
            sport_data["unified_matches"] = []
            sport_data["anchor_bk"] = anchor_bk
            return sport_slug, sport_data

        # Build anchor list for fuzzy matcher
        anchor_list = []
        for m in anchor_matches:
            anchor_list.append({
                "id": m.get("sp_game_id") or m.get("bt_match_id") or m.get("od_match_id") or m.get("b2b_match_id"),
                "betradar_id": m.get("betradar_id") or "",
                "home_team_name": m.get("home_team", ""),
                "away_team_name": m.get("away_team", ""),
                "start_time": m.get("start_time"),
                "competition_name": m.get("competition") or m.get("competition_name"),
                "external_ids": {anchor_bk: m.get("sp_game_id") or m.get("bt_match_id") or m.get("od_match_id")}
            })

        # Align other bookmakers
        unified_map = {u["id"]: u for u in anchor_list}
        for bk, matches in all_bk_matches.items():
            if bk == anchor_bk or not matches:
                continue
            candidates = [match_dict_to_candidate(m, bk_slug=bk) for m in matches]
            updates, creates = bulk_align(candidates, anchor_list, sport_slug)
            for upd in updates:
                uid = upd.unified_match_id
                if uid in unified_map:
                    unified_map[uid].setdefault("bookmakers", {})[bk] = {
                        "match_id": upd.candidate.external_id,
                        "markets": upd.candidate.raw.get("markets", {})
                    }
            for create in creates:
                cand = create.candidate
                new_id = f"new_{bk}_{cand.external_id}"
                unified_map[new_id] = {
                    "id": new_id,
                    "betradar_id": "",
                    "home_team_name": cand.raw.get("home_team", ""),
                    "away_team_name": cand.raw.get("away_team", ""),
                    "start_time": cand.raw.get("start_time"),
                    "competition_name": cand.raw.get("competition") or cand.raw.get("competition_name"),
                    "external_ids": {bk: cand.external_id},
                    "bookmakers": {bk: {"match_id": cand.external_id, "markets": cand.raw.get("markets", {})}}
                }

        unified_matches = list(unified_map.values())
        # Add anchor bookmaker markets
        for um in unified_matches:
            if anchor_bk in um.get("external_ids", {}):
                anchor_id = um["external_ids"][anchor_bk]
                anchor_match = next((m for m in anchor_matches if m.get("sp_game_id") == anchor_id or m.get("bt_match_id") == anchor_id or m.get("od_match_id") == anchor_id or m.get("b2b_match_id") == anchor_id), None)
                if anchor_match:
                    um.setdefault("bookmakers", {})[anchor_bk] = {
                        "match_id": anchor_id,
                        "markets": anchor_match.get("markets", {})
                    }

        sport_data["anchor_bk"] = anchor_bk
        sport_data["unified_matches"] = unified_matches
        sport_data["elapsed_ms"] = int((time.time() - start) * 1000)
        print(f"   ✅ Unified: {len(unified_matches)} matches in {sport_data['elapsed_ms']}ms")
        return sport_slug, sport_data

    # Process all sports in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(process_sport, s): s for s in ALL_SPORTS}
        for future in as_completed(futures):
            sport, data = future.result()
            results[sport] = data

    # Generate HTML report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = f"odds_comparison_{timestamp}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(f"""<html>
<head><meta charset="UTF-8"><title>Full Odds Comparison Report – {datetime.now()}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 20px; }}
table {{ border-collapse: collapse; margin-bottom: 20px; width: 100%; }}
th, td {{ border: 1px solid #ccc; padding: 8px; text-align: left; vertical-align: top; }}
th {{ background: #f0f0f0; }}
.collapsible {{
    background-color: #f9f9f9; cursor: pointer; padding: 10px; width: 100%;
    border: none; text-align: left; outline: none; font-size: 1.2em; margin-top: 10px;
}}
.active, .collapsible:hover {{ background-color: #e9e9e9; }}
.content {{ padding: 0 18px; display: none; overflow-x: auto; background-color: #f1f1f1; }}
.match-container {{ margin-bottom: 30px; border-bottom: 1px solid #ccc; }}
</style>
<script>
function toggleCollapsible(el) {{
    el.classList.toggle("active");
    var content = el.nextElementSibling;
    content.style.display = content.style.display === "block" ? "none" : "block";
}}
</script>
</head>
<body>
<h1>📊 Full Cross‑Bookmaker Odds Comparison Report</h1>
<p>Generated: {datetime.now()}</p>
<p>Days ahead: {days} | Display limit per sport: {max_matches} matches</p>

<h2>Summary per Sport</h2>
<table>
    <tr><th>Sport</th><th>Betika</th><th>OdiBets</th><th>Sportpesa</th><th>B2B (merged)</th><th>Unified</th><th>Anchor</th><th>Time (ms)</th></tr>
""")
        for sport, data in results.items():
            bt = data["bookmaker_counts"].get("betika", 0)
            od = data["bookmaker_counts"].get("odibets", 0)
            sp = data["bookmaker_counts"].get("sportpesa", 0)
            b2b_total = sum(v for k,v in data["bookmaker_counts"].items() if k not in ("betika","odibets","sportpesa"))
            unified = len(data["unified_matches"])
            anchor = data.get("anchor_bk", "?")
            elapsed = data.get("elapsed_ms", 0)
            f.write(f"<tr><td>{sport}</td><td>{bt}</td><td>{od}</td><td>{sp}</td><td>{b2b_total}</td><td>{unified}</td><td>{anchor}</td><td>{elapsed}</td></tr>")
        f.write("</table>")

        # Detailed matches per sport
        for sport, data in results.items():
            unified = data["unified_matches"]
            if not unified:
                continue
            f.write(f'<button class="collapsible" onclick="toggleCollapsible(this)">{sport.upper()} – {len(unified)} unified matches (showing first {min(max_matches, len(unified))})</button>')
            f.write('<div class="content">')
            f.write(f'<p><strong>Anchor bookmaker:</strong> {data.get("anchor_bk")}</p>')
            for idx, um in enumerate(unified[:max_matches]):
                f.write('<div class="match-container">')
                f.write(f'<h3>{idx+1}. {um.get("home_team_name")} vs {um.get("away_team_name")}</h3>')
                f.write(f'<p><strong>Competition:</strong> {um.get("competition_name")}<br>')
                f.write(f'<strong>Start time:</strong> {um.get("start_time")}<br>')
                f.write(f'<strong>Betradar ID:</strong> {um.get("betradar_id") or "N/A"}</p>')

                # Collect all markets across bookmakers
                all_markets = set()
                for bk, bk_data in um.get("bookmakers", {}).items():
                    all_markets.update(bk_data.get("markets", {}).keys())
                all_markets = sorted(all_markets)

                if not all_markets:
                    f.write("<p><em>No markets available for this match.</em></p>")
                else:
                    bookmakers = list(um["bookmakers"].keys())
                    f.write(f'<table><tr><th>Market / Outcome</th>')
                    for bk in bookmakers:
                        f.write(f'<th>{bk}</th>')
                    f.write('</tr>')
                    for mkt in all_markets:
                        outcomes = set()
                        for bk in bookmakers:
                            outcomes.update(um["bookmakers"][bk].get("markets", {}).get(mkt, {}).keys())
                        outcomes = sorted(outcomes)
                        # Market name row
                        f.write(f'<tr><td colspan="{len(bookmakers)+1}"><strong>{mkt}</strong></td></tr>')
                        for out in outcomes:
                            f.write(f'<tr><td style="padding-left:20px;">{out}</td>')
                            for bk in bookmakers:
                                odds = um["bookmakers"][bk].get("markets", {}).get(mkt, {}).get(out)
                                if odds:
                                    f.write(f'<td>{odds:.2f}</td>')
                                else:
                                    f.write('<td>—</td>')
                            f.write('</tr>')
                    f.write('</table>')
                f.write('</div>')
            f.write('</div>')
        f.write("</body></html>")

    print(f"\n📄 HTML report saved to {html_path}")

    # Save raw JSON
    json_path = f"odds_comparison_{timestamp}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, default=str, indent=2)
    print(f"💾 JSON data saved to {json_path}")
    print("\n✅ Report generation complete.")

@flask_app.cli.command("raw-odds-report")
@click.option("--sport", default="soccer", help="Sport slug")
@click.option("--days", default=7, help="Days ahead to fetch")
def raw_odds_report(sport, days):
    """
    Find one match that exists in Sportpesa, Betika, and OdiBets (by betradar_id)
    and show raw markets from each bookmaker.
    """
    import json
    from datetime import datetime
    from app.workers.bt_harvester import fetch_upcoming_matches, CANONICAL_SPORT_IDS
    from app.workers.od_harvester import fetch_upcoming_matches as od_fetch_upcoming, OD_SPORT_IDS
    from app.workers.sp_harvester import fetch_upcoming as sp_fetch_upcoming, SP_SPORT_ID

    print(f"\n🔍 Fetching matches for {sport} ({days} days)...")
    
    # Fetch from each bookmaker
    bt = fetch_upcoming_matches(sport_slug=sport, days=days, fetch_full=True, max_pages=10)
    od = od_fetch_upcoming(sport_slug=sport, days=days, fetch_full_markets=True)
    sp = sp_fetch_upcoming(sport_slug=sport, days=days, max_matches=None, fetch_full_markets=True)
    
    print(f"Betika: {len(bt)} matches")
    print(f"OdiBets: {len(od)} matches")
    print(f"Sportpesa: {len(sp)} matches")
    
    # Build maps by betradar_id
    bt_by_br = {m.get("betradar_id"): m for m in bt if m.get("betradar_id")}
    od_by_br = {m.get("betradar_id"): m for m in od if m.get("betradar_id")}
    sp_by_br = {m.get("betradar_id"): m for m in sp if m.get("betradar_id")}
    
    # Find common betradar_id
    common = set(bt_by_br.keys()) & set(od_by_br.keys()) & set(sp_by_br.keys())
    if not common:
        print("No match found with betradar_id across all three bookmakers.")
        return
    
    # Pick first common match
    br_id = next(iter(common))
    bt_match = bt_by_br[br_id]
    od_match = od_by_br[br_id]
    sp_match = sp_by_br[br_id]
    
    print(f"\n✅ Found common match: {bt_match.get('home_team')} vs {bt_match.get('away_team')} (betradar_id: {br_id})")
    
    # Helper to format raw markets
    def format_raw_markets(match, bookie_name):
        markets = match.get("markets", {})
        if not markets:
            return "<p>No markets</p>"
        html = f"<h4>{bookie_name} – {len(markets)} markets</h4><ul>"
        for mkt_slug, outcomes in markets.items():
            html += f"<li><strong>{mkt_slug}</strong>: "
            odds_str = ", ".join([f"{k}={v:.2f}" for k, v in outcomes.items()])
            html += odds_str + "</li>"
        html += "</ul>"
        return html
    
    # Generate HTML
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = f"raw_odds_comparison_{sport}_{timestamp}.html"
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(f"""<html>
<head><meta charset="UTF-8"><title>Raw Markets Comparison – {sport}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 20px; }}
.match-info {{ background: #f0f0f0; padding: 10px; margin-bottom: 20px; }}
.bookmaker-section {{ border: 1px solid #ccc; margin: 10px 0; padding: 10px; }}
</style>
</head>
<body>
<h1>Raw Markets Comparison – {sport.upper()}</h1>
<div class="match-info">
<h2>{bt_match.get('home_team')} vs {bt_match.get('away_team')}</h2>
<p><strong>Competition:</strong> {bt_match.get('competition')}<br>
<strong>Start time:</strong> {bt_match.get('start_time')}<br>
<strong>Betradar ID:</strong> {br_id}</p>
</div>
<div class="bookmaker-section">
{format_raw_markets(sp_match, "Sportpesa (raw, no normalization)")}
</div>
<div class="bookmaker-section">
{format_raw_markets(bt_match, "Betika (raw, no normalization)")}
</div>
<div class="bookmaker-section">
{format_raw_markets(od_match, "OdiBets (raw, no normalization)")}
</div>
</body></html>""")
    
    print(f"\n📄 Report saved to {html_path}")
    print(f"   Open in browser to see raw market slugs and odds from all three bookmakers for the same match.")

if __name__ == "__main__":
    socketio.run(flask_app, debug=True, host="0.0.0.0", port=5500, use_reloader=False, log_output=True)