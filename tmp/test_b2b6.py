from app.workers.b2b_harvester import _curl, B2B_BOOKMAKERS, _BASE_HEADERS
bk = B2B_BOOKMAKERS[-1] # paripesa

print("Current headers:", _BASE_HEADERS)

# Minimal headers that we know work!
_BASE_HEADERS.clear()
_BASE_HEADERS["accept"] = "application/json"
_BASE_HEADERS["user-agent"] = "Mozilla/5.0"

url = f"https://{bk['domain']}/service-api/LineFeed/GetChampsZip?sport=1&partner={bk['partner_id']}&lng=en"
raw = _curl(url, f"https://{bk['domain']}/en/line")
print("ErrorCode:", raw.get("ErrorCode") if raw else "None")
print("Value length:", len(raw.get("Value") or []) if raw else 0)

