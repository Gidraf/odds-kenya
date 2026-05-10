import httpx
url = "https://1xbet.co.ke/service-api/LineFeed/GetSportsShortZip?sports=1&lng=en&gr=656&partner=61"
headers = {
    "accept": "application/json, text/plain, */*",
    "x-app-n": "__BETTING_APP__",
    "x-requested-with": "XMLHttpRequest",
    "user-agent": "Mozilla/5.0"
}
resp = httpx.get(url, headers=headers)
print("Status:", resp.status_code)
print(resp.text[:500])
