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


@flask_app.cli.command("fast-sp-harvest")
@click.option("--days", default=7, help="Days ahead")
@click.option("--batch-size", default=500, help="Matches per batch")
@click.option("--max-workers", default=8, help="Max parallel batches across sports")
@click.option("--full-markets/--no-full-markets", default=True, help="Fetch full markets (slower)")
def fast_sp_harvest(days, batch_size, max_workers, full_markets):
    """
    Harvest Sportpesa matches for ALL sports in parallel batches.
    Splits sports with many matches into 4 parallel batches using offset.
    """
    import time
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from app.workers.sp_harvester import fetch_upcoming, SP_SPORT_ID

    # Deduplicate sports by id
    seen_ids = set()
    sports = []
    for slug, sid in SP_SPORT_ID.items():
        if sid not in seen_ids:
            seen_ids.add(sid)
            sports.append(slug)

    print(f"\n🚀 FAST SPORTPESA HARVEST – {len(sports)} sports, {days} days")
    print(f"   Batch size: {batch_size}, Parallel workers: {max_workers}, Full markets: {full_markets}")
    print(f"   Started: {datetime.now()}\n")

    final_results = {}

    def fetch_sport(sport_slug):
        print(f"🔍 {sport_slug} – fetching...")
        t0 = time.perf_counter()
        batches = []
        with ThreadPoolExecutor(max_workers=4) as batch_executor:
            futures = []
            for i in range(4):
                offset = i * batch_size
                fut = batch_executor.submit(
                    fetch_upcoming,
                    sport_slug=sport_slug,
                    days=days,
                    max_matches=batch_size,
                    offset=offset,
                    fetch_full_markets=full_markets,
                    sleep_between=0.1
                )
                futures.append((i, offset, fut))
            for i, offset, fut in futures:
                try:
                    matches = fut.result()
                    batches.append(matches)
                    print(f"  {sport_slug} batch{i+1} (offset={offset}): {len(matches)} matches")
                    if len(matches) < batch_size:
                        break
                except Exception as e:
                    print(f"  {sport_slug} batch{i+1} error: {e}")
        # Merge batches (deduplicate by sp_game_id)
        all_matches = []
        seen = set()
        for batch in batches:
            for m in batch:
                mid = m.get("sp_game_id") or m.get("betradar_id")
                if mid and mid not in seen:
                    seen.add(mid)
                    all_matches.append(m)
                elif not mid:
                    all_matches.append(m)
        elapsed = time.perf_counter() - t0
        print(f"✅ {sport_slug}: {len(all_matches)} matches in {elapsed:.2f}s")
        return sport_slug, all_matches

    # Run all sports in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_sport, sport): sport for sport in sports}
        for future in as_completed(futures):
            sport, matches = future.result()
            final_results[sport] = matches

    # Summary table
    print("\n" + "="*80)
    print("📊 SPORTPESA HARVEST SUMMARY")
    print("-"*80)
    print(f"{'Sport':<20} {'Matches':<10}")
    print("-"*40)
    total_matches = 0
    for sport in sorted(final_results.keys()):
        cnt = len(final_results[sport])
        total_matches += cnt
        print(f"{sport:<20} {cnt:<10}")
    print("-"*40)
    print(f"{'TOTAL':<20} {total_matches:<10}")
    print("="*80)

    # Save to JSON
    import json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = f"sp_fast_harvest_{timestamp}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(final_results, f, default=str, indent=2)
    print(f"\n💾 Full data saved to {out_file}")


@flask_app.cli.command("fast-od-harvest")
@click.option("--days", default=7, help="Days ahead")
@click.option("--batch-size", default=500, help="Matches per batch")
@click.option("--max-workers", default=8, help="Max parallel batches across sports")
@click.option("--full-markets/--no-full-markets", default=True, help="Fetch full markets (slower)")
def fast_od_harvest(days, batch_size, max_workers, full_markets):
    """
    Harvest OdiBets matches for ALL sports in parallel batches.
    Sports run in parallel, each sport fetches batches sequentially using offset.
    """
    import time
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from app.workers.od_harvester import fetch_upcoming_matches, OD_SPORT_IDS

    sports = list(OD_SPORT_IDS.keys())
    print(f"\n🚀 FAST ODIBETS HARVEST – {len(sports)} sports, {days} days")
    print(f"   Batch size: {batch_size}, Parallel workers: {max_workers}, Full markets: {full_markets}")
    print(f"   Started: {datetime.now()}\n")

    final_results = {}

    def fetch_sport(sport_slug):
        print(f"🔍 {sport_slug} – fetching...")
        t0 = time.perf_counter()
        batches = []
        offset = 0
        batch_num = 1
        while True:
            try:
                matches = fetch_upcoming_matches(
                    sport_slug=sport_slug,
                    days=days,
                    offset=offset,
                    max_matches=batch_size,
                    fetch_full_markets=full_markets,
                    max_workers=4
                )
                if not matches:
                    break
                batches.append(matches)
                print(f"  {sport_slug} batch{batch_num} (offset={offset}): {len(matches)} matches")
                if len(matches) < batch_size:
                    break
                offset += batch_size
                batch_num += 1
            except Exception as e:
                print(f"  {sport_slug} batch{batch_num} error: {e}")
                break
        # Merge batches (deduplicate by od_match_id)
        all_matches = []
        seen = set()
        for batch in batches:
            for m in batch:
                mid = m.get("od_match_id") or m.get("betradar_id")
                if mid and mid not in seen:
                    seen.add(mid)
                    all_matches.append(m)
                elif not mid:
                    all_matches.append(m)
        elapsed = time.perf_counter() - t0
        print(f"✅ {sport_slug}: {len(all_matches)} matches in {elapsed:.2f}s")
        return sport_slug, all_matches

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_sport, sport): sport for sport in sports}
        for future in as_completed(futures):
            sport, matches = future.result()
            final_results[sport] = matches

    # Summary table
    print("\n" + "="*80)
    print("📊 ODIBETS HARVEST SUMMARY")
    print("-"*80)
    print(f"{'Sport':<20} {'Matches':<10}")
    print("-"*40)
    total_matches = 0
    for sport in sorted(final_results.keys()):
        cnt = len(final_results[sport])
        total_matches += cnt
        print(f"{sport:<20} {cnt:<10}")
    print("-"*40)
    print(f"{'TOTAL':<20} {total_matches:<10}")
    print("="*80)

    import json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = f"od_fast_harvest_{timestamp}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(final_results, f, default=str, indent=2)
    print(f"\n💾 Full data saved to {out_file}")


@flask_app.cli.command("dump-od-raw")
@click.option("--sport", default=None, help="Sport slug (e.g., soccer, basketball, tennis). Omit for all sports.")
@click.option("--days", default=1, help="Days ahead to fetch")
@click.option("--max-matches", default=5, help="Max matches per sport")
@click.option("--output-dir", default="od_raw_dumps", help="Directory to save JSON files")
def dump_od_raw(sport, days, max_matches, output_dir):
    """
    Dump raw OdiBets API responses for upcoming matches (before normalization).
    Useful for creating custom mappers.
    """
    import os
    import json
    import httpx
    from datetime import datetime, date as _date
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from app.workers.od_harvester import slug_to_od_sport_id, _get, SBOOK_ODI, SBOOK_V1, _unwrap_upcoming_response

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Determine sports list
    if sport:
        sports = [sport]
    else:
        from app.workers.od_harvester import OD_SPORT_IDS
        sports = list(OD_SPORT_IDS.keys())

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Headers for detail call (same as in od_harvester)
    HEADERS = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "authorization": "Bearer",
        "content-type": "application/json",
        "origin": "https://odibets.com",
        "referer": "https://odibets.com/",
        "user-agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36",
    }

    def dump_sport(sport_slug):
        od_id = slug_to_od_sport_id(sport_slug)
        day_str = _date.today().isoformat()
        params = {
            "resource": "sportevents",
            "platform": "mobile",
            "mode": 1,
            "sport_id": od_id,
            "sub_type_id": "",
            "day": day_str,
        }
        data = _get(SBOOK_ODI, params=params, _throttle=True)
        if not data:
            print(f"⚠️ No data for {sport_slug}")
            return

        raw_events = _unwrap_upcoming_response(data, od_id)
        if not raw_events:
            print(f"⚠️ No events for {sport_slug}")
            return

        # Take only first max_matches
        raw_events = raw_events[:max_matches]

        # Fetch full markets for each match (by calling the match detail endpoint)
        for ev in raw_events:
            parent_id = ev.get("parent_match_id") or ev.get("game_id")
            if parent_id:
                detail_params = {
                    "resource": "sportevent",
                    "id": str(parent_id),
                    "category_id": "",
                    "sub_type_id": "",
                    "builder": 0,
                    "sportsbook": "sportsbook",
                    "ua": HEADERS["user-agent"],
                }
                try:
                    r = httpx.get(SBOOK_V1, params=detail_params, headers=HEADERS, timeout=15.0)
                    if r.status_code == 200:
                        ev["_full_markets_raw"] = r.json()
                    else:
                        ev["_full_markets_raw"] = f"HTTP {r.status_code}"
                except Exception as e:
                    ev["_full_markets_raw"] = f"Error: {e}"

        out_file = os.path.join(output_dir, f"od_raw_{sport_slug}_{timestamp}.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(raw_events, f, default=str, indent=2)
        print(f"✅ {sport_slug}: saved {len(raw_events)} raw matches to {out_file}")

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(dump_sport, s): s for s in sports}
        for future in as_completed(futures):
            s = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ {s} error: {e}")

    print(f"\n📁 Raw data saved to directory: {output_dir}")

@flask_app.cli.command("fetch-od-complete")
@click.option("--max-days", default=60, help="Maximum days ahead to search")
@click.option("--output-dir", default="od_complete_dumps", help="Directory to save JSON files")
def fetch_od_complete(max_days, output_dir):
    """
    Fetch OdiBets upcoming matches for EVERY sport in OD_SPORT_IDS.
    Searches day by day (starting today) up to `max_days` until matches are found.
    For each sport, collects all matches across all days (up to max_days) and saves a JSON.
    """
    import os
    import json
    import time
    from datetime import datetime, date as _date, timedelta
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from app.workers.od_harvester import (
        slug_to_od_sport_id, _get, SBOOK_ODI, _unwrap_upcoming_response,
        fetch_full_markets_for_match, OD_SPORT_IDS
    )

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = {}
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "authorization": "Bearer",
        "content-type": "application/json",
        "origin": "https://odibets.com",
        "referer": "https://odibets.com/",
        "user-agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36",
    }

    def fetch_sport_complete(sport_slug, od_sport_id):
        print(f"🔍 {sport_slug} (id={od_sport_id}) – searching up to {max_days} days...")
        all_matches = []
        start_date = _date.today()
        day_count = 0
        for offset in range(max_days):
            day = start_date + timedelta(days=offset)
            day_str = day.isoformat()
            params = {
                "resource": "sportevents",
                "platform": "mobile",
                "mode": 1,
                "sport_id": od_sport_id,
                "sub_type_id": "",
                "day": day_str,
            }
            data = _get(SBOOK_ODI, params=params, _throttle=True)
            if not data:
                continue
            raw_events = _unwrap_upcoming_response(data, od_sport_id)
            if not raw_events:
                continue
            # For each match, fetch full markets
            for ev in raw_events[:5]:  # limit to 5 per day for speed (change to None for all)
                parent_id = ev.get("parent_match_id") or ev.get("game_id")
                if parent_id:
                    detail_params = {
                        "resource": "sportevent",
                        "id": str(parent_id),
                        "category_id": "",
                        "sub_type_id": "",
                        "builder": 0,
                        "sportsbook": "sportsbook",
                        "ua": headers["user-agent"],
                    }
                    # Use local _get to avoid throttling delays (we can call directly)
                    # We'll use the same _get function but we need to import it
                    from app.workers.od_harvester import _get as od_get
                    detail_data = od_get(SBOOK_V1, params=detail_params, _throttle=True)
                    ev["_full_markets_raw"] = detail_data
                all_matches.append(ev)
            day_count += 1
            if all_matches:
                print(f"  ✅ {sport_slug} – found {len(raw_events)} matches on {day_str} (total so far: {len(all_matches)})")
                # We continue searching further days to collect more matches
                # (but stop at max_days)
        if not all_matches:
            print(f"  ⚠️ {sport_slug} – no matches found in {max_days} days")
        return all_matches

    # Run for all sports in OD_SPORT_IDS
    for sport_slug, od_id in OD_SPORT_IDS.items():
        matches = fetch_sport_complete(sport_slug, od_id)
        results[sport_slug] = matches
        out_file = os.path.join(output_dir, f"od_complete_{sport_slug}_{timestamp}.json")
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(matches, f, default=str, indent=2)
        print(f"💾 Saved {len(matches)} matches to {out_file}")

    # Summary table
    print("\n" + "="*80)
    print("📊 ODIBETS COMPLETE HARVEST SUMMARY")
    print("-"*80)
    print(f"{'Sport':<20} {'Matches found':<15}")
    print("-"*40)
    total_matches = 0
    for sport, matches in sorted(results.items()):
        cnt = len(matches)
        total_matches += cnt
        print(f"{sport:<20} {cnt:<15}")
    print("-"*40)
    print(f"{'TOTAL':<20} {total_matches:<15}")
    print("="*80)

if __name__ == "__main__":
    socketio.run(flask_app, debug=True, host="0.0.0.0", port=5500, use_reloader=False, log_output=True)