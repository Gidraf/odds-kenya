from curl_cffi import requests
url = "https://1xbet.co.ke/service-api/LineFeed/GetSportsShortZip?sports=1&partner=61&lng=en"
headers = {"accept": "application/json, text/plain, */*", "User-Agent": "Mozilla/5.0"}
resp = requests.get(url, headers=headers, impersonate="chrome110")
print("Status:", resp.status_code)
print(resp.text[:500])
