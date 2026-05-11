from app.workers.b2b_harvester import _curl, B2B_BOOKMAKERS
bk = B2B_BOOKMAKERS[-1] # paripesa

url = f"https://{bk['domain']}/service-api/LineFeed/GetChampsZip?sport=2&partner={bk['partner_id']}&lng=en"
print("Champs URL:", url)
raw = _curl(url, f"https://{bk['domain']}/en/line")
print("ErrorCode:", raw.get("ErrorCode") if raw else "None")
val = raw.get("Value") if raw else None
print("Value length:", len(val) if val else 0)
