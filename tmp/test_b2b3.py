import httpx
url = "https://1xbet.co.ke/service-api/LineFeed/GetChampZip?sport=1&champ=110163,225733&partner=61&lng=en"
headers = {"accept": "application/json, text/plain, */*"}
resp = httpx.get(url, headers=headers)
print("Status:", resp.status_code)
print(resp.text[:500])
