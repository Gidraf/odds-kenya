from app.workers.b2b_harvester import _curl, B2B_BOOKMAKERS, _BASE_HEADERS
bk = B2B_BOOKMAKERS[-1] # paripesa

_BASE_HEADERS["accept"] = "application/json" # OVERRIDE

url = f"https://{bk['domain']}/service-api/LineFeed/GetChampsZip?sport=1&partner={bk['partner_id']}&lng=en"
raw = _curl(url, f"https://{bk['domain']}/en/line")
print("Value length:", len(raw.get("Value") or []) if raw else "None")

