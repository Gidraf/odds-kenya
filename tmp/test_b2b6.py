import httpx
url = "https://1xbet.co.ke/service-api/LineFeed/GetSportsShortZip?sports=1&partner=61&lng=en"
headers = {"accept": "application/json, text/plain, */*", "User-Agent": "curl/8.4.0"}
resp = httpx.get(url, headers=headers)
print("Status:", resp.status_code)
print(resp.text[:500])
