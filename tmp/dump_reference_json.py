#!/usr/bin/env python3
"""
dump_reference_json.py
========================
Dumps reference data for DeepSeek mapper creation:
  1. One unified SP+BT match per sport (the "correct" target)
  2. One raw OD match per sport (what OD actually returns before mapping)
  3. HTML report with DeepSeek prompt instructions

Run:
    docker exec -w /app odds-kenya-web-1 python3 /tmp/dump_reference_json.py
    docker cp odds-kenya-web-1:/tmp/kinetic_reference/ ./kinetic_reference/
"""
import sys, json, os, re
sys.path.insert(0, '/app')
import redis as _redis

REDIS_URL = os.getenv("REDIS_URL", "redis://:Winners1127@redis6382:6382/0")
r = _redis.from_url(REDIS_URL, decode_responses=True)
r.ping()

OUT_DIR = "/tmp/kinetic_reference"
os.makedirs(OUT_DIR, exist_ok=True)

SPORTS = ["soccer", "basketball", "tennis", "cricket", "rugby",
          "ice-hockey", "volleyball", "handball", "table-tennis",
          "mma", "boxing", "darts", "esoccer"]

# ─── Helper ───────────────────────────────────────────────────────────────────
def load(key):
    raw = r.get(key)
    if not raw: return []
    data = json.loads(raw)
    return data.get("matches", data) if isinstance(data, dict) else data

def first_with_markets(matches, min_markets=5):
    for m in matches:
        mkts = m.get("markets") or {}
        if len(mkts) >= min_markets:
            return m
    return matches[0] if matches else None

# ─── 1. Unified SP+BT reference (built manually) ─────────────────────────────
print("\n📦 Building unified SP+BT reference matches...")

unified_refs = {}
for sport in SPORTS:
    sp = load(f"sp:upcoming:{sport}")
    bt = load(f"bt:upcoming:{sport}")
    if not sp and not bt:
        print(f"  ⚠️  {sport}: no data")
        continue

    # Find a match that exists in both BKs by betradar_id
    sp_by_br = {str(m.get("betradar_id") or ""): m for m in sp if m.get("betradar_id")}
    bt_by_br = {str(m.get("betradar_id") or ""): m for m in bt if m.get("betradar_id")}
    common = set(sp_by_br.keys()) & set(bt_by_br.keys()) - {""}

    if common:
        br_id = next(iter(common))
        sp_m = sp_by_br[br_id]
        bt_m = bt_by_br[br_id]
        unified = {
            "home_team":   sp_m.get("home_team"),
            "away_team":   sp_m.get("away_team"),
            "competition": sp_m.get("competition"),
            "betradar_id": br_id,
            "sport":       sport,
            "bookmakers": {
                "sp": {
                    "bookmaker": "SportPesa",
                    "markets":   sp_m.get("markets", {})
                },
                "bt": {
                    "bookmaker": "Betika",
                    "markets":   bt_m.get("markets", {})
                }
            },
            "_note": "Both BKs matched via betradar_id. Markets show canonical slugs."
        }
    elif sp:
        sp_m = first_with_markets(sp)
        unified = {
            "home_team":   sp_m.get("home_team"),
            "away_team":   sp_m.get("away_team"),
            "competition": sp_m.get("competition"),
            "betradar_id": sp_m.get("betradar_id"),
            "sport":       sport,
            "bookmakers": {"sp": {"bookmaker": "SportPesa", "markets": sp_m.get("markets", {})}},
            "_note": "SP only — no BT match found with same betradar_id"
        }
    else:
        bt_m = first_with_markets(bt)
        unified = {
            "home_team":   bt_m.get("home_team"),
            "away_team":   bt_m.get("away_team"),
            "competition": bt_m.get("competition"),
            "betradar_id": bt_m.get("betradar_id"),
            "sport":       sport,
            "bookmakers": {"bt": {"bookmaker": "Betika", "markets": bt_m.get("markets", {})}},
            "_note": "BT only"
        }

    # Build best odds across BKs
    best = {}
    for bk, bd in unified["bookmakers"].items():
        for mkt, outs in (bd.get("markets") or {}).items():
            if not isinstance(outs, dict): continue
            best.setdefault(mkt, {})
            for out, val in outs.items():
                price = float(val) if isinstance(val, (int, float)) else 0
                if price > best[mkt].get(out, {}).get("odd", 0):
                    best[mkt][out] = {"odd": price, "bk": bk}
    unified["best"] = best
    unified["market_count"] = len(best)
    unified_refs[sport] = unified

    path = f"{OUT_DIR}/unified_sp_bt_{sport}.json"
    with open(path, "w") as f:
        json.dump(unified, f, indent=2, default=str)
    bks = list(unified["bookmakers"].keys())
    print(f"  ✅ {sport:15} {len(best):3} markets  BKs={bks}  → {path}")

# ─── 2. Raw OD matches per sport ─────────────────────────────────────────────
print("\n🟡 Dumping raw OD matches per sport...")

od_refs = {}
unknown_ids = {}  # sport → set of unknown IDs

for sport in SPORTS:
    od = load(f"od:upcoming:{sport}")
    if not od:
        print(f"  ⚠️  {sport}: no OD data")
        continue

    # Pick a match with the most markets
    m = max(od, key=lambda x: len(x.get("markets", {})), default=None)
    if not m:
        continue

    # Separate known vs unknown markets
    markets = m.get("markets", {})
    known_mkts   = {k: v for k, v in markets.items() if not re.search(r"_unknown_\d+$", k)}
    unknown_mkts = {k: v for k, v in markets.items() if re.search(r"_unknown_\d+$", k)}

    # Extract unknown IDs for this sport
    sport_unknown_ids = []
    for slug in unknown_mkts:
        m2 = re.search(r"_unknown_(\d+)$", slug)
        if m2:
            sport_unknown_ids.append(int(m2.group(1)))
    unknown_ids[sport] = sorted(set(sport_unknown_ids))

    ref = {
        "home_team":       m.get("home_team"),
        "away_team":       m.get("away_team"),
        "competition":     m.get("competition"),
        "betradar_id":     m.get("betradar_id"),
        "sport":           sport,
        "total_markets":   len(markets),
        "known_markets":   known_mkts,
        "unknown_markets": unknown_mkts,
        "unknown_ids":     unknown_ids[sport],
        "_note": (
            f"{len(known_mkts)} markets resolved, {len(unknown_mkts)} unknown. "
            f"Unknown IDs need adding to odibets_football_mapper.py get_market_info()"
        )
    }
    od_refs[sport] = ref

    path = f"{OUT_DIR}/od_raw_{sport}.json"
    with open(path, "w") as f:
        json.dump(ref, f, indent=2, default=str)
    print(f"  ✅ {sport:15} {len(known_mkts):3} known + {len(unknown_mkts):3} unknown  → {path}")

# ─── 3. Master unknown ID list across all sports ──────────────────────────────
all_unknown = {}
for sport, ids in unknown_ids.items():
    for sid in ids:
        all_unknown.setdefault(sid, []).append(sport)

master = {
    "total_unknown_ids": len(all_unknown),
    "by_id": {
        str(sid): {"sports": sports, "count": len(sports)}
        for sid, sports in sorted(all_unknown.items())
    }
}
with open(f"{OUT_DIR}/unknown_ids_master.json", "w") as f:
    json.dump(master, f, indent=2)
print(f"\n  📋 Master unknown ID list: {len(all_unknown)} unique IDs")

# ─── 4. HTML report with DeepSeek instructions ───────────────────────────────
print("\n📄 Generating HTML report...")

sport_rows = ""
for sport in SPORTS:
    u = unified_refs.get(sport, {})
    od = od_refs.get(sport, {})
    bks = list(u.get("bookmakers", {}).keys())
    bk_str = "+".join(b.upper() for b in bks) or "—"
    u_mkts = u.get("market_count", 0)
    od_known = len(od.get("known_markets", {}))
    od_unk = len(od.get("unknown_markets", {}))
    unk_ids = od.get("unknown_ids", [])
    unk_preview = ", ".join(str(i) for i in unk_ids[:15])
    if len(unk_ids) > 15: unk_preview += f"... +{len(unk_ids)-15} more"
    u_cls = "green" if u_mkts > 0 else "red"
    od_cls = "green" if od_known > 0 else "red"
    unk_cls = "red" if od_unk > 0 else "green"
    sport_rows += f"""<tr>
      <td><strong>{sport}</strong></td>
      <td class="{u_cls}">{u_mkts}</td>
      <td>{bk_str}</td>
      <td class="{od_cls}">{od_known}</td>
      <td class="{unk_cls}">{od_unk}</td>
      <td style="font-size:10px;color:#6B7A8E">{unk_preview}</td>
    </tr>"""

# Soccer unknown ID table
soccer_od = od_refs.get("soccer", {})
soccer_unk = soccer_od.get("unknown_markets", {})
soccer_unk_rows = ""
for slug, outs in sorted(soccer_unk.items()):
    sid = re.search(r"_unknown_(\d+)$", slug)
    sid = sid.group(1) if sid else "?"
    sample_outs = list(outs.keys())[:5] if isinstance(outs, dict) else []
    soccer_unk_rows += f"""<tr>
      <td><code>{sid}</code></td>
      <td><code>{slug}</code></td>
      <td>{", ".join(f"<code>{o}</code>" for o in sample_outs)}</td>
      <td><input style="width:250px;background:#0A1020;border:1px solid #1e293b;color:#CCFF00;padding:3px 6px;border-radius:4px" placeholder="e.g. draw_no_bet / btts / over_under_goals" id="id_{sid}"/></td>
    </tr>"""

deepseek_prompt = """You are a sports betting market mapper expert.

I have OdiBets soccer markets identified by sub_type_id (integer).
I need you to map each sub_type_id to a canonical market slug.

Here are the unknown IDs with sample outcome keys that hint at what the market is:
{MARKET_TABLE}

Rules for canonical slugs:
- 1x2 = 3-way match result (outcomes: 1, X, 2)
- draw_no_bet = 2-way (outcomes: 1, 2)
- double_chance = (outcomes: 1X, X2, 12)
- btts = Both Teams to Score (outcomes: Yes, No)
- over_under_goals = Over/Under (outcomes: Over, Under) — add line e.g. over_under_goals_2_5
- correct_score = (outcomes: 0:0, 1:0, etc.)
- half_time_full_time = HT/FT (outcomes: 1/1, 1/X, X/2 etc.)
- first_half_1x2, first_half_btts, first_half_over_under_*
- asian_handicap_*, european_handicap_*
- exact_goals, odd_even, winning_margin, multigoals

Return ONLY a Python dict like:
{
  10: "double_chance",
  11: "draw_no_bet",
  ...
}
No explanation, just the dict."""

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Kinetic — OD Market Mapping Reference</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Inter',system-ui,sans-serif;background:#05080F;color:#E8EDF8;padding:20px}}
h1{{color:#CCFF00;font-size:20px;margin-bottom:4px}}
h2{{color:#CCFF00;font-size:15px;margin:20px 0 8px;border-bottom:1px solid #1e293b;padding-bottom:5px}}
h3{{font-size:13px;color:#EAB308;margin:14px 0 6px}}
.meta{{font-size:11px;color:#6B7A8E;margin-bottom:20px}}
table{{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:16px}}
th{{background:#0A1020;color:#6B7A8E;padding:7px 10px;text-align:left;font-size:10px;text-transform:uppercase}}
td{{padding:6px 10px;border-bottom:1px solid #0f172a}}
tr:hover{{background:rgba(255,255,255,.02)}}
.green{{color:#3EE08A}} .red{{color:#F06C6C}} .yellow{{color:#EAB308}}
code{{background:#0A1020;border:1px solid #1e293b;border-radius:4px;padding:1px 6px;font-size:11px;color:#0DD8E8}}
pre{{background:#080E1C;border:1px solid #1e293b;border-radius:8px;padding:14px;font-size:11px;overflow-x:auto;line-height:1.5;white-space:pre-wrap}}
.card{{background:#080E1C;border:1px solid #1e293b;border-radius:10px;padding:16px;margin-bottom:16px}}
.step{{display:flex;gap:12px;margin-bottom:12px}}
.step-num{{min-width:28px;height:28px;border-radius:50%;background:#CCFF0020;border:1px solid #CCFF0040;color:#CCFF00;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:13px}}
.step-body{{flex:1}}
.badge{{display:inline-block;padding:2px 8px;border-radius:20px;font-size:10px;font-weight:700}}
.badge.green{{background:rgba(62,224,138,.15);color:#3EE08A}}
.badge.red{{background:rgba(240,108,108,.15);color:#F06C6C}}
.file-link{{color:#0DD8E8;font-size:11px}}
</style>
</head>
<body>
<h1>⚡ Kinetic — OdiBets Market Mapping Reference</h1>
<div class="meta">Use this to instruct DeepSeek to create market mappers for each sport.</div>

<h2>📊 Status by Sport</h2>
<table>
<thead><tr>
  <th>Sport</th><th>Unified Markets (SP+BT)</th><th>BKs</th>
  <th style="color:#EAB308">OD Known</th><th style="color:#F06C6C">OD Unknown</th><th>Unknown IDs</th>
</tr></thead>
<tbody>{sport_rows}</tbody>
</table>

<h2>🔧 How to Fix: Step by Step</h2>
<div class="card">
  <div class="step"><div class="step-num">1</div><div class="step-body">
    <strong>Give DeepSeek the prompt below</strong> (one per sport)<br>
    <span style="color:#6B7A8E;font-size:11px">Include the unknown IDs + sample outcomes from the od_raw_SPORT.json files</span>
  </div></div>
  <div class="step"><div class="step-num">2</div><div class="step-body">
    <strong>DeepSeek returns a Python dict</strong>: <code>{{10: "double_chance", 11: "draw_no_bet", ...}}</code>
  </div></div>
  <div class="step"><div class="step-num">3</div><div class="step-body">
    <strong>Add the dict to <code>canonical_mapper.py → _KNOWN_IDS</code></strong><br>
    This is the simplest place — already has the lookup logic.
  </div></div>
  <div class="step"><div class="step-num">4</div><div class="step-body">
    <strong>Re-harvest OD</strong>: <code>python3 /tmp/harvest_report.py --sports soccer --debug</code>
  </div></div>
  <div class="step"><div class="step-num">5</div><div class="step-body">
    <strong>Verify</strong>: unknown markets should drop from 207 → 0 for soccer
  </div></div>
</div>

<h2>📋 DeepSeek Prompt Template</h2>
<div class="card">
<pre>{deepseek_prompt}</pre>
<p style="font-size:11px;color:#6B7A8E;margin-top:8px">
  Replace {{MARKET_TABLE}} with the IDs + outcomes from the od_raw_SPORT.json file.
  Run once per sport (soccer, basketball, tennis, etc.)
</p>
</div>

<h2>📂 Reference Files Generated</h2>
<div class="card">
<table>
<thead><tr><th>File</th><th>Description</th></tr></thead>
<tbody>
  <tr><td><code>unified_sp_bt_SPORT.json</code></td><td>Target — what the unified match should look like (SP+BT markets)</td></tr>
  <tr><td><code>od_raw_SPORT.json</code></td><td>Raw OD match — known + unknown markets per sport</td></tr>
  <tr><td><code>unknown_ids_master.json</code></td><td>All unknown IDs across all sports</td></tr>
</tbody>
</table>
<p style="font-size:11px;color:#6B7A8E;margin-top:8px">
  Copy from container: <code>docker cp odds-kenya-web-1:/tmp/kinetic_reference/ ./kinetic_reference/</code>
</p>
</div>

<h2>❓ Soccer Unknown Markets — Fill in canonical slugs</h2>
<p style="font-size:11px;color:#6B7A8E;margin-bottom:8px">
  You can fill this in manually or use DeepSeek. Sample outcomes help identify the market type.
</p>
<table>
<thead><tr><th>ID</th><th>Current Slug</th><th>Sample Outcomes</th><th>Canonical Slug (fill in)</th></tr></thead>
<tbody>{soccer_unk_rows}</tbody>
</table>

<h2>🔌 Where to Put Changes After DeepSeek</h2>
<div class="card">
<h3>Option A — Simplest: <code>app/workers/canonical_mapper.py</code></h3>
<pre># In _KNOWN_IDS dict, add/update:
_KNOWN_IDS = {{
    10:  {{"soccer": "double_chance"}},
    11:  {{"soccer": "draw_no_bet"}},
    12:  {{"soccer": "draw_no_bet"}},
    # ... rest of DeepSeek output
}}</pre>
<p style="font-size:11px;color:#6B7A8E">Already handles all sports. Fastest to deploy. No harvester restart needed — just re-harvest OD.</p>

<h3 style="margin-top:14px">Option B — Proper: <code>app/utils/mapping/odibets/odibets_football_mapper.py</code></h3>
<pre># Add to get_market_info() — a sub_type_id int → slug dict at the top:
_ID_MAP = {{
    10: "double_chance",
    11: "draw_no_bet",
    # ... DeepSeek output
}}

@classmethod
def get_market_info(cls, market_slug):
    # NEW: try sub_type_id int lookup first
    try:
        sid = int(market_slug.split("_unknown_")[-1])
        if sid in _ID_MAP:
            return (_ID_MAP[sid], {{}})
    except (ValueError, IndexError):
        pass
    # ... rest of existing logic</pre>
<p style="font-size:11px;color:#6B7A8E">More maintainable per-sport. The mapper file already exists — just add _ID_MAP and the lookup.</p>

<h3 style="margin-top:14px">Recommended: Do Option A first (immediate fix), then migrate to Option B</h3>
</div>

</body></html>"""

html_path = f"{OUT_DIR}/mapping_reference.html"
with open(html_path, "w") as f:
    f.write(html)

print(f"\n{'='*55}")
print(f"✅ All files saved to {OUT_DIR}/")
print(f"\nFiles generated:")
for fn in sorted(os.listdir(OUT_DIR)):
    size = os.path.getsize(f"{OUT_DIR}/{fn}")
    print(f"  {fn:45} {size//1024:4}KB")
print(f"\nCopy to host:")
print(f"  docker cp odds-kenya-web-1:{OUT_DIR}/ ./kinetic_reference/")