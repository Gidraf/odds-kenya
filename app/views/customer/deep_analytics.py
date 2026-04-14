import logging
import traceback
import re
import asyncio
import nest_asyncio
from concurrent.futures import ThreadPoolExecutor
from flask import Blueprint, Response, stream_with_context
import requests
import json
from playwright.async_api import async_playwright

# Required to run async Playwright inside a synchronous Flask route
nest_asyncio.apply()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bp_deep_analytics = Blueprint("deep_analytics", __name__, url_prefix="/api")

_HEADERS = {
    "origin": "https://www.betika.com", 
    "referer": "https://www.betika.com/", 
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Global token cache
ACTIVE_TOKENS = {
    "LMT_TOKEN": "",
    "SH_TOKEN": ""
}

def _get_fresh_tokens():
    """Scrapes Betika's widget loader to find the active Sportradar HMAC tokens."""
    try:
        url = "https://widgets.sir.sportradar.com/60006b5234c31ccf8b14f16f2873ee71/widgetloader"
        res = requests.get(url, headers=_HEADERS, timeout=5)
        
        if res.status_code == 200:
            tokens = re.findall(r'(exp=\d+~acl=/\*~data=[a-zA-Z0-9_]+~hmac=[a-f0-9]+)', res.text)
            if tokens:
                ACTIVE_TOKENS["LMT_TOKEN"] = tokens[0]
                ACTIVE_TOKENS["SH_TOKEN"] = tokens[0]
                logger.info(f"Successfully scraped fresh Sportradar Tokens.")
                return True
        else:
            logger.warning(f"Failed to fetch widget loader. HTTP {res.status_code}")
    except Exception as e:
        logger.error(f"Exception while scraping tokens: {e}")
    return False

def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"

def _fetch_lmt(endpoint: str, item_id: str):
    if not ACTIVE_TOKENS["LMT_TOKEN"]:
        _get_fresh_tokens()

    url = f"https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/{endpoint}/{item_id}?T={ACTIVE_TOKENS['LMT_TOKEN']}"
    try:
        logger.info(f"[LMT] Fetching: {endpoint} for {item_id}")
        res = requests.get(url, headers=_HEADERS, timeout=5)
        if res.status_code == 200: 
            return res.json().get("doc", [{}])[0].get("data", {})
        elif res.status_code == 403:
            logger.warning(f"[LMT] 403 Forbidden. Token likely expired. Refreshing...")
            if _get_fresh_tokens():
                url = f"https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/{endpoint}/{item_id}?T={ACTIVE_TOKENS['LMT_TOKEN']}"
                retry_res = requests.get(url, headers=_HEADERS, timeout=5)
                if retry_res.status_code == 200:
                    return retry_res.json().get("doc", [{}])[0].get("data", {})
        else:
            logger.warning(f"[LMT] Error {res.status_code} for {endpoint}/{item_id}")
    except Exception as e:
        logger.error(f"[LMT] Exception for {endpoint}/{item_id}: {e}")
    return {}

def _fetch_sh(endpoint: str, item_id: str, extra=""):
    if not ACTIVE_TOKENS["SH_TOKEN"]:
        _get_fresh_tokens()

    url = f"https://sh.fn.sportradar.com/sportpesa/en/Etc:UTC/gismo/{endpoint}/{item_id}{extra}?T={ACTIVE_TOKENS['SH_TOKEN']}"
    try:
        logger.info(f"[SH] Fetching: {endpoint} for {item_id}")
        res = requests.get(url, headers=_HEADERS, timeout=5)
        if res.status_code == 200: 
            return res.json().get("doc", [{}])[0].get("data", {})
        elif res.status_code == 403:
            logger.warning(f"[SH] 403 Forbidden. Token likely expired. Refreshing...")
            if _get_fresh_tokens():
                url = f"https://sh.fn.sportradar.com/sportpesa/en/Etc:UTC/gismo/{endpoint}/{item_id}{extra}?T={ACTIVE_TOKENS['SH_TOKEN']}"
                retry_res = requests.get(url, headers=_HEADERS, timeout=5)
                if retry_res.status_code == 200:
                    return retry_res.json().get("doc", [{}])[0].get("data", {})
        else:
            logger.warning(f"[SH] Error {res.status_code} for {endpoint}/{item_id}")
    except Exception as e:
        logger.error(f"[SH] Exception for {endpoint}/{item_id}: {e}")
    return {}


# --- THE PLAYWRIGHT FALLBACK ---
# --- THE PLAYWRIGHT FALLBACK (NOW WITH EXTREME LOGGING) ---
async def _scrape_betika_fallback(match_id: str):
    """
    Spins up a headless browser to intercept network calls natively if Sportradar endpoints fail.
    """
    url = f"https://www.betika.com/en-ke/m/{match_id}"
    logger.info(f"[Scraper] Launching Playwright fallback for {match_id}...")
    
    captured_data = {"match_data": None}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        async def handle_response(response):
            if "sportradar.com" in response.url and response.status == 200:
                if response.request.method == "OPTIONS": return
                try:
                    # 1. Grab the raw JSON
                    json_payload = await response.json()
                    
                    if "match_info" in response.url or "match_timelinedelta" in response.url:
                        logger.info(f"[Scraper] Intercepted 200 OK from: {response.url.split('?')[0]}")
                        
                        # 2. Log a snippet of the raw payload so we can see the actual schema
                        payload_str = json.dumps(json_payload)
                        logger.info(f"[Scraper] RAW PAYLOAD SNIPPET: {payload_str[:500]}...")

                        # 3. Attempt extraction with detailed error checks
                        doc_list = json_payload.get("doc", [])
                        if not doc_list:
                            logger.warning("[Scraper] Payload missing 'doc' array!")
                            return
                            
                        data = doc_list[0].get("data", {})
                        if not data:
                            logger.warning("[Scraper] Payload missing 'data' object inside 'doc[0]'!")
                            return

                        if "match" in data:
                            if not captured_data["match_data"]:
                                captured_data["match_data"] = data
                                logger.info(f"[Scraper] [+] SUCCESS: Captured Match Info via Playwright")
                        else:
                            logger.warning("[Scraper] 'match' key is missing from the 'data' object! Schema mismatch.")
                            
                except Exception as e:
                    logger.error(f"[Scraper] Crash while parsing {response.url}: {e}")
                    logger.error(traceback.format_exc())

        page.on("response", handle_response)

        try:
            logger.info(f"[Scraper] Navigating to {url}")
            await page.goto(url, wait_until="networkidle", timeout=15000)
            await asyncio.sleep(3) # Extra wait for React/AJAX to fire
        except Exception as e:
            logger.warning(f"[Scraper] Timeout or navigation warning: {e}")

        await browser.close()
        
    return captured_data

@bp_deep_analytics.route("/odds/match/<betradar_id>/deep_analytics/stream")
def stream_deep_analytics(betradar_id: str):
    def generate():
        yield _sse("status", {"step": "Initializing", "message": "Connecting to Sportradar..."})

        logger.info(f"--- Starting stream for Match ID: {betradar_id} ---")

        # 1. Standard Fast Fetching
        timeline_data = _fetch_lmt("match_timelinedelta", betradar_id) or {}
        info_data = _fetch_lmt("match_info", betradar_id) or {}
        sh_info_data = _fetch_sh("match_info_statshub", betradar_id) or {}
        
        match_data = timeline_data.get("match") or info_data.get("match") or sh_info_data.get("match") or {}

        # 2. Playwright Fallback trigger
        if not match_data:
            logger.warning(f"Standard endpoints failed for {betradar_id}. Engaging Playwright fallback...")
            yield _sse("status", {"step": "Fallback", "message": "Bypassing restrictions..."})
            
            # Run the async scraper synchronously inside the Flask thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            fallback_data = loop.run_until_complete(_scrape_betika_fallback(betradar_id))
            
            scraped_match = fallback_data.get("match_data", {})
            if scraped_match and "match" in scraped_match:
                match_data = scraped_match["match"]
                # We also want to assign this to timeline_data so the events parser below works
                timeline_data = scraped_match 
                info_data = scraped_match
            else:
                logger.error(f"FATAL: Playwright fallback also failed to find match data for {betradar_id}.")
                yield _sse("error", {"message": f"Could not load match data for {betradar_id}."})
                return

        # Extract strict UIDs and Team Names
        teams = match_data.get("teams", {})
        home_uid = teams.get("home", {}).get("uid")
        away_uid = teams.get("away", {}).get("uid")
        season_id = match_data.get("_seasonid")

        match_time = match_data.get("timeinfo", {}).get("played", "0")
        live_status = match_data.get("status", {}).get("name", "Upcoming")

        stadium = info_data.get("stadium") or sh_info_data.get("stadium") or {}
        distance = info_data.get("distance") or sh_info_data.get("distance") or "N/A"
        managers = info_data.get("manager") or sh_info_data.get("manager") or {}

        yield _sse("meta", {
            "home_team": teams.get("home", {}).get("name", "Home"),
            "away_team": teams.get("away", {}).get("name", "Away"),
            "status": live_status,
            "match_time": str(match_time) if match_time else "0",
            "score_home": match_data.get("result", {}).get("home", 0),
            "score_away": match_data.get("result", {}).get("away", 0),
            "stadium": stadium.get("name", "Unknown Stadium"),
            "distance_km": distance,
            "home_manager": managers.get("home", {}).get("name", "TBA"),
            "away_manager": managers.get("away", {}).get("name", "TBA")
        })

        if timeline_data and "events" in timeline_data:
            events = timeline_data.get("events", [])
            ignored = ["possession", "matchsituation", "ballcoordinates", "possible_event", "pitch coordinates"]
            comments = [{"time": ev.get("time", ""), "team": ev.get("team"), "type": ev.get("type"), "name": ev.get("name", "")} for ev in reversed(events) if ev.get("type") not in ignored]
            yield _sse("comments", comments[:20])
        else:
            yield _sse("comments", []) 

        # 3. Concurrently fetch deep stats (Only if we have the IDs)
        if home_uid and away_uid:
            with ThreadPoolExecutor(max_workers=10) as pool:
                f_squads      = pool.submit(_fetch_lmt, "match_squads", betradar_id)
                f_h2h         = pool.submit(_fetch_sh, "stats_team_versusrecent", f"{home_uid}/{away_uid}")
                f_home_recent = pool.submit(_fetch_sh, "stats_team_lastx", str(home_uid), "/10")
                f_away_recent = pool.submit(_fetch_sh, "stats_team_lastx", str(away_uid), "/10")
                f_home_next   = pool.submit(_fetch_sh, "stats_team_fixtures", str(home_uid), "/10")
                f_away_next   = pool.submit(_fetch_sh, "stats_team_fixtures", str(away_uid), "/10")
                f_form        = pool.submit(_fetch_sh, "stats_formtable", str(season_id)) if season_id else None
                f_table       = pool.submit(_fetch_sh, "stats_season_tables", str(season_id)) if season_id else None
                f_team_stats  = pool.submit(_fetch_sh, "stats_season_uniqueteamstats", str(season_id)) if season_id else None
                f_top_h       = pool.submit(_fetch_sh, "stats_season_topgoals", f"{season_id}/{home_uid}") if season_id else None
                f_top_a       = pool.submit(_fetch_sh, "stats_season_topgoals", f"{season_id}/{away_uid}") if season_id else None

                # --- SQUADS ---
                squads_data = f_squads.result()
                if squads_data and "home" in squads_data:
                    h_node = squads_data.get("home", {}).get("startinglineup", squads_data.get("home", {}).get("players", []))
                    a_node = squads_data.get("away", {}).get("startinglineup", squads_data.get("away", {}).get("players", []))
                    h_form = h_node.get("formation", "") if isinstance(h_node, dict) else ""
                    a_form = a_node.get("formation", "") if isinstance(a_node, dict) else ""
                    h_players = h_node.get("players", []) if isinstance(h_node, dict) else (h_node if isinstance(h_node, list) else [])
                    a_players = a_node.get("players", []) if isinstance(a_node, dict) else (a_node if isinstance(a_node, list) else [])

                    yield _sse("lineups", {
                        "home": {"formation": h_form, "players": [{"name": p.get("playername", p.get("name", "")).split(",")[0].strip(), "num": p.get("shirtnumber", ""), "pos": p.get("matchpos", "M")} for p in h_players]},
                        "away": {"formation": a_form, "players": [{"name": p.get("playername", p.get("name", "")).split(",")[0].strip(), "num": p.get("shirtnumber", ""), "pos": p.get("matchpos", "M")} for p in a_players]}
                    })
                else:
                    yield _sse("lineups", {"fallback": True})

                # --- H2H ---
                h2h_data = f_h2h.result() if f_h2h else None
                if h2h_data:
                    parsed_h2h = []
                    for m in h2h_data.get("matches", [])[:5]:
                        parsed_h2h.append({
                            "date": m.get("_dt", {}).get("date", ""),
                            "home": m.get("teams", {}).get("home", {}).get("name", ""),
                            "away": m.get("teams", {}).get("away", {}).get("name", ""),
                            "score_home": m.get("result", {}).get("home", 0),
                            "score_away": m.get("result", {}).get("away", 0)
                        })
                    yield _sse("h2h", parsed_h2h)
                else:
                    yield _sse("h2h", [])

                # --- RECENT / UPCOMING ---
                def _parse_recent(data):
                    if not data: return []
                    return [{
                        "date": m.get("_dt", {}).get("date", ""),
                        "time": m.get("_dt", {}).get("time", ""),
                        "home": m.get("teams", {}).get("home", {}).get("name", ""),
                        "away": m.get("teams", {}).get("away", {}).get("name", ""),
                        "score_home": m.get("result", {}).get("home", 0),
                        "score_away": m.get("result", {}).get("away", 0)
                    } for m in data.get("matches", [])[:5]]

                yield _sse("recent", {
                    "home": _parse_recent(f_home_recent.result() if f_home_recent else None),
                    "away": _parse_recent(f_away_recent.result() if f_away_recent else None)
                })
                yield _sse("upcoming", {
                    "home": _parse_recent(f_home_next.result() if f_home_next else None)[:3],
                    "away": _parse_recent(f_away_next.result() if f_away_next else None)[:3]
                })

                # --- FORM ---
                form_data = f_form.result() if f_form else None
                if form_data:
                    target_form = {"home": [], "away": []}
                    for t in form_data.get("teams", []):
                        uid = str(t.get("team", {}).get("uid"))
                        form_list = [f.get("value") for f in t.get("form", {}).get("total", [])]
                        if uid == str(home_uid): target_form["home"] = form_list
                        if uid == str(away_uid): target_form["away"] = form_list
                    yield _sse("form", target_form)

                # --- STANDINGS ---
                table_data = f_table.result() if f_table else None
                if table_data:
                    rows = []
                    tables = table_data.get("tables", [])
                    for t in tables:
                        for row in t.get("tablerows", []):
                            rows.append({
                                "pos": row.get("pos"),
                                "team": row.get("team", {}).get("name"),
                                "played": row.get("total"),
                                "gd": row.get("goalDiffTotal"),
                                "pts": row.get("pointsTotal"),
                                "is_target": str(row.get("team", {}).get("uid")) in [str(home_uid), str(away_uid)]
                            })
                        break 
                    yield _sse("standings", sorted(rows, key=lambda x: x["pos"]))

                # --- TOP SCORERS ---
                top_h_data = f_top_h.result() if f_top_h else {}
                top_a_data = f_top_a.result() if f_top_a else {}
                def extract_scorers(data):
                    return [{"name": p.get("player", {}).get("name", "Unknown").split(",")[0], 
                             "goals": p.get("total", {}).get("goals", 0)} for p in data.get("players", [])[:3]]
                yield _sse("top_scorers", {
                    "home": extract_scorers(top_h_data),
                    "away": extract_scorers(top_a_data)
                })

                # --- TEAM STATS ---
                t_stats = f_team_stats.result() if f_team_stats else None
                if t_stats:
                    h_stats = t_stats.get("stats", {}).get("uniqueteams", {}).get(str(home_uid), {})
                    a_stats = t_stats.get("stats", {}).get("uniqueteams", {}).get(str(away_uid), {})
                    yield _sse("team_stats", {
                        "home": {
                            "possession": h_stats.get("ball_possession", {}).get("average", 50),
                            "shots": h_stats.get("goal_attempts", {}).get("average", 0),
                            "corners": h_stats.get("corner_kicks", {}).get("average", 0),
                            "clean_sheets": h_stats.get("clean_sheet", {}).get("total", 0)
                        },
                        "away": {
                            "possession": a_stats.get("ball_possession", {}).get("average", 50),
                            "shots": a_stats.get("goal_attempts", {}).get("average", 0),
                            "corners": a_stats.get("corner_kicks", {}).get("average", 0),
                            "clean_sheets": a_stats.get("clean_sheet", {}).get("total", 0)
                        }
                    })

        logger.info(f"--- Stream complete for Match ID: {betradar_id} ---")
        yield _sse("done", {"status": "complete"})

    return Response(stream_with_context(generate()), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"
    })