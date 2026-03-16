import json
import requests
import difflib
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------------------------
BETB2B_DOMAINS = [
    {"name": "1xBet", "domain": "1xbet.co.ke"},
    {"name": "22Bet", "domain": "22bet.co.ke"},
    {"name": "Betwinner", "domain": "betwinner.ke"},
    {"name": "Melbet", "domain": "melbet.ke"},
    {"name": "Megapari", "domain": "megapari.com"}
]

# Using the exact Sportpesa URL you provided to fetch their markets
SPORTPESA_URL = "https://www.ke.sportpesa.com/api/games/markets?games=8709667,8710035,8534289,8534295,8550659,8534497,8534297,8546813,8546793,8550677,8550653,8550649,8546767,8545413,8546775,8550651,8545443,8550761,8550645,8545483,8550643,8698357,8407743,8697823,8723255,8547115,8547109,8408099,8697481,8698325,8696645,8547119,8697531,8700431,8697857,8723845&markets=10,46,52-2.5,43"

# ---------------------------------------------------------------------------
# 2. UNIFIED MARKET MAPPINGS
# ---------------------------------------------------------------------------
# BetB2B Mapping
B2B_MARKET_GROUPS = {1: "1X2", 8: "Double Chance", 17: "Total", 19: "Both Teams To Score"}
B2B_OUTCOMES = {
    1: "Home Win", 2: "Draw", 3: "Away Win",
    4: "1X", 5: "12", 6: "2X",
    7: "Over", 8: "Under",
    180: "YES", 181: "NO"
}

# Sportpesa Mapping
SP_MARKET_GROUPS = {10: "1X2", 46: "Double Chance", 52: "Total", 43: "Both Teams To Score"}
SP_OUTCOMES = {
    "1X2": {"1": "Home Win", "X": "Draw", "2": "Away Win"},
    "Double Chance": {"1X": "1X", "X2": "2X", "12": "12"}, # Normalized X2 to 2X for consistency
    "Total": {"OV": "Over", "UN": "Under"},
    "Both Teams To Score": {"Yes": "YES", "No": "NO"}
}


class MasterOddsAggregator:
    def __init__(self):
        self.unified_matches = {}

    # --- 1. FUZZY MATCHING ENGINE ---
    def generate_key(self, home, away):
        """Creates a standardized string to compare cross-platform matches."""
        home_clean = str(home).lower().replace("fc", "").strip()
        away_clean = str(away).lower().replace("fc", "").strip()
        return f"{home_clean} | {away_clean}"

    def find_or_create_match(self, home, away, sport, start_time=None):
        """Finds an existing match via fuzzy matching, or creates a new one."""
        target_key = self.generate_key(home, away)
        
        best_match_key = None
        highest_ratio = 0.0

        for existing_key in self.unified_matches.keys():
            ratio = difflib.SequenceMatcher(None, target_key, existing_key).ratio()
            if ratio > highest_ratio:
                highest_ratio = ratio
                best_match_key = existing_key

        # If similarity is above 65%, we assume it's the exact same match
        if highest_ratio > 0.65:
            return best_match_key
            
        # Otherwise, create a new match entry
        self.unified_matches[target_key] = {
            "teams": f"{home} vs {away}",
            "sport": sport,
            "bookmaker_event_ids": {},
            "odds": {}
        }
        return target_key

    def set_odds(self, match_key, market, line, outcome, bookie, odds_val):
        """Safely injects odds deep into the nested dictionary."""
        odds_dict = self.unified_matches[match_key]["odds"]
        if market not in odds_dict: odds_dict[market] = {}
        if line not in odds_dict[market]: odds_dict[market][line] = {}
        if outcome not in odds_dict[market][line]: odds_dict[market][line][outcome] = {}
        
        odds_dict[market][line][outcome][bookie] = odds_val

    # --- 2. DATA FETCHERS ---
    def fetch_betb2b(self, bookie):
        domain = bookie["domain"]
        name = bookie["name"]
        url = f"https://{domain}/service-api/LiveFeed/Get1x2_VZip?sports=1&count=50&lng=en&mode=4&country=87&virtualSports=true"
        try:
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if res.status_code == 200 and res.json().get("Success"):
                return "BetB2B", name, res.json().get("Value", [])
        except Exception:
            pass
        return "BetB2B", name, []

    def fetch_sportpesa(self):
        try:
            res = requests.get(SPORTPESA_URL, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}, timeout=10)
            if res.status_code == 200:
                return "Sportpesa", "Sportpesa", res.json()
        except Exception:
            pass
        return "Sportpesa", "Sportpesa", {}

    # --- 3. DATA PROCESSORS ---
    def process_betb2b_data(self, bookie_name, matches):
        for item in matches:
            home, away = item.get("O1"), item.get("O2")
            match_id, sms_id = item.get("I"), item.get("N")
            if not home or not away or not match_id: continue

            match_key = self.find_or_create_match(home, away, item.get("SN", "Football"))
            
            # Map the SMS / Game ID
            self.unified_matches[match_key]["bookmaker_event_ids"][bookie_name] = {
                "game_id": match_id,
                "sms_id": sms_id
            }

            # Parse the E and AE arrays for odds
            events = item.get("E", [])
            for ae in item.get("AE", []): events.extend(ae.get("ME", []))

            for e in events:
                g_id, t_id = e.get("G"), e.get("T")
                if g_id not in B2B_MARKET_GROUPS: continue
                
                market = B2B_MARKET_GROUPS[g_id]
                outcome = B2B_OUTCOMES.get(t_id, f"Type_{t_id}")
                odds_val = float(e.get("C") or e.get("CV", 0))
                line = str(e.get("P", "Main Line"))

                self.set_odds(match_key, market, line, outcome, bookie_name, odds_val)

    def process_sportpesa_data(self, raw_data):
        # Note: Sportpesa's market endpoint doesn't return team names directly in this specific JSON.
        # It assumes you already mapped the ID to the name from their /events endpoint.
        # For this script to work fully autonomously, we extract names from the 1X2 market strings if available.
        
        for sp_match_id, markets in raw_data.items():
            home_team, away_team = "Team A", "Team B"
            
            # Try to extract team names from the 1X2 market selections
            for m in markets:
                if m.get("id") == 10 and len(m.get("selections", [])) >= 3:
                    home_team = m["selections"][0]["name"]
                    away_team = m["selections"][2]["name"]
                    break
            
            match_key = self.find_or_create_match(home_team, away_team, "Football")

            # Map Sportpesa ID
            self.unified_matches[match_key]["bookmaker_event_ids"]["Sportpesa"] = {
                "game_id": sp_match_id,
                "sms_id": sp_match_id # Sportpesa often uses game ID for SMS
            }

            for m in markets:
                m_id = m.get("id")
                if m_id not in SP_MARKET_GROUPS: continue
                
                market_name = SP_MARKET_GROUPS[m_id]
                raw_spec = m.get("specValue", 0)
                line = str(raw_spec) if raw_spec != 0 else "Main Line"

                for sel in m.get("selections", []):
                    raw_outcome = sel.get("shortName") or sel.get("name")
                    outcome = SP_OUTCOMES[market_name].get(raw_outcome, raw_outcome)
                    odds_val = float(sel.get("odds", 0))
                    
                    self.set_odds(match_key, market_name, line, outcome, "Sportpesa", odds_val)

    # --- 4. EXECUTION CONTROLLER ---
    def build_feed(self):
        print("Starting massive concurrent data fetch...\n")
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Dispatch BetB2B
            for b in BETB2B_DOMAINS:
                futures.append(executor.submit(self.fetch_betb2b, b))
            # Dispatch Sportpesa
            futures.append(executor.submit(self.fetch_sportpesa))

            # Process responses
            for future in as_completed(futures):
                arch_type, bookie_name, payload = future.result()
                
                if arch_type == "BetB2B":
                    print(f"[+] Retrieved {len(payload)} matches from {bookie_name}")
                    self.process_betb2b_data(bookie_name, payload)
                elif arch_type == "Sportpesa":
                    print(f"[+] Retrieved {len(payload.keys())} matches from {bookie_name}")
                    self.process_sportpesa_data(payload)

        # 5. Calculate Absolute Best Odds
        for match in self.unified_matches.values():
            for m_group, lines in match.get("odds", {}).items():
                for line, outcomes in lines.items():
                    for outcome, bookies in outcomes.items():
                        if bookies:
                            best_bookie = max(bookies, key=bookies.get)
                            bookies["__BEST_ODDS__"] = {
                                "bookmaker": best_bookie,
                                "odds": bookies[best_bookie]
                            }

        return list(self.unified_matches.values())

# --- RUN SCRIPT ---
if __name__ == "__main__":
    master_aggregator = MasterOddsAggregator()
    results = master_aggregator.build_feed()
    
    print(f"\n--- Successfully built Master Feed with {len(results)} Unique Matches ---")
    
    # Let's print a match that hopefully contains both BetB2B and Sportpesa to see the merge
    if results:
        print(json.dumps(results[0], indent=2))