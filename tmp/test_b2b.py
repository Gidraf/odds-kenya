from app.workers.b2b_harvester import _curl, B2B_BOOKMAKERS
bk = B2B_BOOKMAKERS[-1] # paripesa

# 1. Get championships for sport 2 (ice hockey)
champs_url = f"https://{bk['domain']}/service-api/LineFeed/GetChampsZip?sport=2&partner={bk['partner_id']}&lng=en"
print("Champs URL:", champs_url)
raw_champs = _curl(champs_url, f"https://{bk['domain']}/en/line")
champs = raw_champs.get("Value") or []
print(f"Found {len(champs)} championships")

if champs:
    champ_id = champs[0].get("LI")
    print(f"Fetching games for champ {champ_id}...")
    games_url = f"https://{bk['domain']}/service-api/LineFeed/GetChampZip?sport=2&champ={champ_id}&partner={bk['partner_id']}&lng=en"
    raw_games = _curl(games_url, f"https://{bk['domain']}/en/line")
    games = raw_games.get("Value") or []
    print(f"Found {len(games)} games in champ {champ_id}")
