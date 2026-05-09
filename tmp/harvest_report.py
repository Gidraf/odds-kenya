#!/usr/bin/env python3
"""
harvest_report.py
==================
1. Harvests SP + BT + OD for all sports
2. Writes matches to correct Redis keys
3. Rebuilds unified cache
4. Tracks which markets failed to map (still soccer_unknown_N)
5. Saves JSON report + HTML visualization

Run:
    docker exec -w /app odds-kenya-web-1 python3 /tmp/harvest_report.py
    docker exec -w /app odds-kenya-web-1 python3 /tmp/harvest_report.py --sports soccer basketball
    docker exec -w /app odds-kenya-web-1 python3 /tmp/harvest_report.py --all --no-full
"""
import sys, json, os, time, re, argparse, collections
from datetime import datetime
sys.path.insert(0, '/app')

import redis as _redis

# ── Connect ───────────────────────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://:Winners1127@redis6382:6382/0")
r = _redis.from_url(REDIS_URL, decode_responses=True, socket_connect_timeout=5)
r.ping()
print(f"✅ Redis OK\n")

TTL = 7200

ALL_SPORTS = [
    "soccer", "basketball", "tennis", "cricket", "rugby",
    "ice-hockey", "volleyball", "handball", "table-tennis",
    "mma", "boxing", "darts", "esoccer", "american-football",
]

ap = argparse.ArgumentParser()
ap.add_argument("--sports",  nargs="+", default=["soccer"])
ap.add_argument("--all",     action="store_true")
ap.add_argument("--no-full", action="store_true", help="Skip full market fetch (faster)")
ap.add_argument("--out",     default="/tmp/harvest_report", help="Output path (no extension)")
args = ap.parse_args()

sports     = ALL_SPORTS if args.all else args.sports
fetch_full = not args.no_full

# ── Report data structures ────────────────────────────────────────────────────
report = {
    "generated_at":  datetime.now().isoformat(),
    "sports":        sports,
    "fetch_full":    fetch_full,
    "by_sport":      {},
    "unknown_markets": {},     # bk → sport → {slug: count}
    "known_markets":   {},     # bk → sport → {slug: count}
    "merge_stats":     {},     # sport → {total, multi_bk, single_bk, no_match}
}

def save_bk(bk: str, sport: str, matches: list):
    data = json.dumps(matches, default=str)
    r.setex(f"{bk}:upcoming:{sport}",      TTL, data)
    r.setex(f"odds:{bk}:upcoming:{sport}", TTL, data)

def analyse_markets(bk: str, sport: str, matches: list):
    """Categorise markets per match as known or unknown."""
    unknown_counter = collections.Counter()
    known_counter   = collections.Counter()
    for m in matches:
        mkts = m.get("markets") or {}
        # Also look inside bookmakers[bk]
        for bk2, bd in (m.get("bookmakers") or {}).items():
            mkts.update(bd.get("markets") or {})
        for slug in mkts:
            if re.search(r"_unknown_\d+$", slug):
                unknown_counter[slug] += 1
            else:
                known_counter[slug] += 1
    report["unknown_markets"].setdefault(bk, {}).setdefault(sport, {})\
        .update(dict(unknown_counter))
    report["known_markets"].setdefault(bk, {}).setdefault(sport, {})\
        .update(dict(known_counter))

def harvest_one(bk: str, sport: str) -> list:
    try:
        if bk == "sp":
            from app.workers.sp_harvester import fetch_upcoming
            return fetch_upcoming(sport_slug=sport, days=7, max_matches=500,
                                  fetch_full_markets=fetch_full, sleep_between=0.1)
        elif bk == "bt":
            from app.workers.bt_harvester import fetch_upcoming_matches
            return fetch_upcoming_matches(sport_slug=sport, days=30,
                                          fetch_full=fetch_full, max_workers=4)
        elif bk == "od":
            from app.workers.od_harvester import fetch_upcoming_matches
            return fetch_upcoming_matches(sport_slug=sport, days=30,
                                          fetch_full_markets=fetch_full, max_workers=4)
    except Exception as e:
        print(f"    ❌ {bk.upper()} {sport}: {e}")
        return []
    return []

def rebuild_unified(sport: str) -> list:
    r.delete(f"odds:unified:upcoming:{sport}")
    try:
        from app.api.odds_stream import _get_unified
        return _get_unified("upcoming", sport, force_refresh=True)
    except Exception as e:
        print(f"    ❌ unified {sport}: {e}")
        return []

# ══════════════════════════════════════════════════════════════════════════════
# HARVEST ALL SPORTS
# ══════════════════════════════════════════════════════════════════════════════
for sport in sports:
    print(f"\n{'═'*58}")
    print(f"  {sport.upper()}")
    print(f"{'═'*58}")

    sport_data: dict = {"fetch_counts": {}, "unified": {}, "market_analysis": {}}

    for bk in ["sp", "bt", "od"]:
        print(f"  {bk.upper()}…", end=" ", flush=True)
        t0 = time.time()
        matches = harvest_one(bk, sport)
        elapsed = round(time.time() - t0, 1)
        if matches:
            save_bk(bk, sport, matches)
            analyse_markets(bk, sport, matches)
            sport_data["fetch_counts"][bk] = len(matches)
            print(f"{len(matches)} matches  ({elapsed}s)")
        else:
            sport_data["fetch_counts"][bk] = 0
            print(f"0 matches  ({elapsed}s)")

    # Rebuild unified
    print(f"  🔄 Merging…", end=" ", flush=True)
    unified = rebuild_unified(sport)

    bk_cover: dict[str, int] = {}
    multi_bk = single_bk = 0
    for m in unified:
        bks = set(m.get("bookmakers", {}).keys())
        for bk in bks:
            bk_cover[bk] = bk_cover.get(bk, 0) + 1
        if len(bks) > 1:
            multi_bk += 1
        else:
            single_bk += 1

    sport_data["unified"] = {
        "total":     len(unified),
        "multi_bk":  multi_bk,
        "single_bk": single_bk,
        "bk_cover":  dict(sorted(bk_cover.items())),
    }
    report["merge_stats"][sport] = sport_data["unified"]
    report["by_sport"][sport]    = sport_data

    print(f"{len(unified)} unified  |  multi-BK: {multi_bk}/{len(unified)}")
    print(f"    BK coverage: {dict(sorted(bk_cover.items()))}")

# ══════════════════════════════════════════════════════════════════════════════
# COMPILE UNKNOWN MARKET SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n\n{'═'*58}")
print("📊 UNKNOWN MARKET SUMMARY")
print(f"{'═'*58}")

all_unknown: dict[str, dict] = {}  # slug → {bk, sport, count}
for bk, sport_map in report["unknown_markets"].items():
    for sport, slugs in sport_map.items():
        for slug, count in slugs.items():
            if slug not in all_unknown:
                all_unknown[slug] = {"bk": bk, "sport": sport, "count": 0,
                                     "occurrences": []}
            all_unknown[slug]["count"] += count
            all_unknown[slug]["occurrences"].append({"bk": bk, "sport": sport, "count": count})

# Sort by count desc
sorted_unknown = sorted(all_unknown.items(), key=lambda x: -x[1]["count"])
print(f"\n  Top unknown markets ({len(sorted_unknown)} total):")
for slug, info in sorted_unknown[:30]:
    sid = re.search(r"_(\d+)$", slug)
    sid = sid.group(1) if sid else "?"
    print(f"  ID {sid:>4}  {slug:45}  count={info['count']:4}  bk={info['bk']}  sport={info['sport']}")

# ══════════════════════════════════════════════════════════════════════════════
# SAVE JSON REPORT
# ══════════════════════════════════════════════════════════════════════════════
report["unknown_summary"]  = dict(sorted_unknown[:100])
report["total_unknown"]    = len(all_unknown)
report["total_known"]      = sum(
    len(slugs) for sport_map in report["known_markets"].values()
    for slugs in sport_map.values()
)

json_path = f"{args.out}.json"
with open(json_path, "w") as f:
    json.dump(report, f, indent=2, default=str)
print(f"\n💾 JSON saved: {json_path}")

# ══════════════════════════════════════════════════════════════════════════════
# HTML REPORT
# ══════════════════════════════════════════════════════════════════════════════
def pct(a, b): return f"{round(a/b*100)}%" if b > 0 else "0%"
def badge(n, good=1): return f'<span class="badge {"green" if n>=good else "red"}">{n}</span>'

bk_colors = {"sp": "#22C55E", "bt": "#EF4444", "od": "#EAB308"}

# Build sport table rows
sport_rows = ""
for sport in sports:
    sd = report["by_sport"].get(sport, {})
    fc = sd.get("fetch_counts", {})
    u  = sd.get("unified", {})
    sp_n = fc.get("sp", 0); bt_n = fc.get("bt", 0); od_n = fc.get("od", 0)
    total = u.get("total", 0); multi = u.get("multi_bk", 0)
    coverage = pct(multi, total) if total > 0 else "—"
    bk_cov = u.get("bk_cover", {})
    bk_pills = " ".join(
        f'<span class="bkpill" style="background:{bk_colors.get(bk,"#555")}">'
        f'{bk.upper()} {n}</span>'
        for bk, n in bk_cov.items()
    )
    sp_cls = "green" if sp_n>0 else "red"
    bt_cls = "green" if bt_n>0 else "red"
    od_cls = "green" if od_n>0 else "red"
    merge_cls = "green" if multi > 0 else ("yellow" if total > 0 else "red")
    sport_rows += f"""
    <tr>
      <td><strong>{sport}</strong></td>
      <td class="{sp_cls}">{sp_n:,}</td>
      <td class="{bt_cls}">{bt_n:,}</td>
      <td class="{od_cls}">{od_n:,}</td>
      <td class="{merge_cls}">{total:,}</td>
      <td class="{merge_cls}">{multi:,} ({coverage})</td>
      <td>{bk_pills}</td>
    </tr>"""

# Build unknown markets table
unknown_rows = ""
for slug, info in sorted_unknown[:100]:
    sid = re.search(r"_(\d+)$", slug); sid = sid.group(1) if sid else "?"
    occ = ", ".join(f"{o['bk'].upper()}/{o['sport']}×{o['count']}" for o in info["occurrences"][:5])
    unknown_rows += f"""
    <tr>
      <td><code>{sid}</code></td>
      <td><code>{slug}</code></td>
      <td>{info['count']}</td>
      <td>{occ}</td>
    </tr>"""

# Build known markets by BK
known_tabs = ""
for bk in ["sp", "bt", "od"]:
    rows = ""
    all_slugs: dict[str, int] = {}
    for sport_map in report["known_markets"].get(bk, {}).values():
        for slug, count in sport_map.items():
            all_slugs[slug] = all_slugs.get(slug, 0) + count
    for slug, count in sorted(all_slugs.items(), key=lambda x: -x[1])[:80]:
        rows += f"<tr><td><code>{slug}</code></td><td>{count}</td></tr>"
    color = bk_colors.get(bk, "#888")
    known_tabs += f"""
    <div class="tab-pane" id="known-{bk}">
      <h3 style="color:{color}">{bk.upper()} — {len(all_slugs)} unique known markets</h3>
      <table><thead><tr><th>Market Slug</th><th>Occurrences</th></tr></thead>
      <tbody>{rows}</tbody></table>
    </div>"""

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Kinetic Harvest Report — {datetime.now().strftime("%Y-%m-%d %H:%M")}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Inter',system-ui,sans-serif;background:#05080F;color:#E8EDF8;padding:20px}}
h1{{color:#CCFF00;font-size:22px;margin-bottom:4px}}
h2{{color:#CCFF00;font-size:16px;margin:24px 0 10px;border-bottom:1px solid #1e293b;padding-bottom:6px}}
h3{{font-size:14px;margin-bottom:10px}}
.meta{{font-size:11px;color:#6B7A8E;margin-bottom:24px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:24px}}
.card{{background:#080E1C;border:1px solid #1e293b;border-radius:10px;padding:16px}}
.card .val{{font-size:28px;font-weight:900;color:#CCFF00;margin:4px 0}}
.card .lbl{{font-size:10px;color:#6B7A8E;text-transform:uppercase;letter-spacing:.5px}}
table{{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:20px}}
th{{background:#0A1020;color:#6B7A8E;padding:8px 10px;text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.5px}}
td{{padding:7px 10px;border-bottom:1px solid #0f172a}}
tr:hover{{background:rgba(255,255,255,0.02)}}
.green{{color:#3EE08A}}
.red{{color:#F06C6C}}
.yellow{{color:#F5A523}}
.badge{{display:inline-block;padding:2px 8px;border-radius:20px;font-size:10px;font-weight:700}}
.badge.green{{background:rgba(62,224,138,.15);color:#3EE08A}}
.badge.red{{background:rgba(240,108,108,.15);color:#F06C6C}}
code{{background:#0A1020;border:1px solid #1e293b;border-radius:4px;padding:1px 6px;font-size:11px;color:#0DD8E8}}
.bkpill{{display:inline-block;padding:2px 7px;border-radius:10px;font-size:9px;font-weight:700;color:#000;margin:1px}}
.tabs{{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap}}
.tab-btn{{padding:6px 14px;border:1px solid #1e293b;border-radius:7px;background:#080E1C;color:#6B7A8E;cursor:pointer;font-size:11px;font-weight:700}}
.tab-btn.active{{background:#CCFF0015;border-color:#CCFF0040;color:#CCFF00}}
.tab-pane{{display:none}}
.tab-pane.active{{display:block}}
.progress{{height:8px;background:#0A1020;border-radius:4px;overflow:hidden;margin:4px 0}}
.progress-bar{{height:100%;background:linear-gradient(90deg,#CCFF00,#0DD8E8);border-radius:4px;transition:width .3s}}
</style>
</head>
<body>
<h1>⚡ Kinetic Harvest Report</h1>
<div class="meta">Generated: {datetime.now().strftime("%A, %d %B %Y at %H:%M:%S")} &nbsp;|&nbsp;
Sports: {', '.join(sports)} &nbsp;|&nbsp; Full markets: {'yes' if fetch_full else 'no (--no-full)'}</div>

<div class="grid">
  <div class="card">
    <div class="lbl">Total Unified</div>
    <div class="val">{sum(u.get("total",0) for u in report["merge_stats"].values()):,}</div>
    <div class="lbl">matches across all sports</div>
  </div>
  <div class="card">
    <div class="lbl">Multi-BK Matches</div>
    <div class="val" style="color:#3EE08A">{sum(u.get("multi_bk",0) for u in report["merge_stats"].values()):,}</div>
    <div class="lbl">merged from 2+ bookmakers</div>
  </div>
  <div class="card">
    <div class="lbl">Unknown Markets</div>
    <div class="val" style="color:#F06C6C">{report["total_unknown"]}</div>
    <div class="lbl">need ID mapping</div>
  </div>
  <div class="card">
    <div class="lbl">Known Markets</div>
    <div class="val">{report["total_known"]}</div>
    <div class="lbl">successfully mapped</div>
  </div>
  <div class="card">
    <div class="lbl">SP Matches</div>
    <div class="val" style="color:#22C55E">{sum(sd.get("fetch_counts",{}).get("sp",0) for sd in report["by_sport"].values()):,}</div>
  </div>
  <div class="card">
    <div class="lbl">BT Matches</div>
    <div class="val" style="color:#EF4444">{sum(sd.get("fetch_counts",{}).get("bt",0) for sd in report["by_sport"].values()):,}</div>
  </div>
  <div class="card">
    <div class="lbl">OD Matches</div>
    <div class="val" style="color:#EAB308">{sum(sd.get("fetch_counts",{}).get("od",0) for sd in report["by_sport"].values()):,}</div>
  </div>
</div>

<h2>📊 Harvest by Sport</h2>
<table>
<thead><tr>
  <th>Sport</th>
  <th style="color:#22C55E">SP</th>
  <th style="color:#EF4444">BT</th>
  <th style="color:#EAB308">OD</th>
  <th>Unified</th>
  <th>Multi-BK</th>
  <th>BK Coverage</th>
</tr></thead>
<tbody>{sport_rows}</tbody>
</table>

<h2>❌ Unknown Markets (need ID mapping)</h2>
<p style="font-size:11px;color:#6B7A8E;margin-bottom:10px">
  These market IDs were not resolved to canonical slugs.
  Add them to <code>canonical_mapper.py → _KNOWN_IDS</code> or <code>odds_normalizer.py → OD_MARKET_ID_MAP</code>.
</p>
<table>
<thead><tr><th>ID</th><th>Slug</th><th>Total Occurrences</th><th>Where seen</th></tr></thead>
<tbody>{unknown_rows if unknown_rows else "<tr><td colspan='4' class='green'>🎉 No unknown markets!</td></tr>"}</tbody>
</table>

<h2>✅ Known Markets by Bookmaker</h2>
<div class="tabs">
  <button class="tab-btn active" onclick="showTab('sp')">🟢 SportPesa</button>
  <button class="tab-btn" onclick="showTab('bt')">🔴 Betika</button>
  <button class="tab-btn" onclick="showTab('od')">🟡 OdiBets</button>
</div>
<div id="known-sp" class="tab-pane active">{known_tabs.split('id="known-sp"')[1].split('</div>')[0] if 'known-sp' in known_tabs else ""}</div>
<div id="known-bt" class="tab-pane">{known_tabs.split('id="known-bt"')[1].split('</div>')[0] if 'known-bt' in known_tabs else ""}</div>
<div id="known-od" class="tab-pane">{known_tabs.split('id="known-od"')[1].split('</div>')[0] if 'known-od' in known_tabs else ""}</div>

<script>
function showTab(bk) {{
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('known-' + bk).classList.add('active');
  event.target.classList.add('active');
}}
</script>
</body></html>"""

html_path = f"{args.out}.html"
with open(html_path, "w") as f:
    f.write(html)
print(f"📄 HTML saved: {html_path}")
print(f"\n   Copy to host:")
print(f"   docker cp odds-kenya-web-1:{json_path} ./harvest_report.json")
print(f"   docker cp odds-kenya-web-1:{html_path} ./harvest_report.html")