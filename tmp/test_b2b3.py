from app.workers.b2b_harvester import _curl, B2B_BOOKMAKERS
bk = B2B_BOOKMAKERS[-1] # paripesa

url = f"https://{bk['domain']}/service-api/LineFeed/GetSportsShortZip?lng=en&country=87&partner={bk['partner_id']}&virtualSports=true&groupChamps=true"
raw = _curl(url, f"https://{bk['domain']}/en/line")
total_games = 0
for sport in raw.get("Value", []):
    for country in sport.get("L", []):
        for sc in country.get("SC", []):
            for game in sc.get("G", []):
                total_games += 1
                if total_games == 1:
                    print("Sample game:", game.get("O1E"), "vs", game.get("O2E"), "- markets:", len(game.get("E", [])))
print(f"Total games in tree: {total_games}")
