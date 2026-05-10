import httpx
url = "https://1xbet.co.ke/service-api/LineFeed/GetGameZip?sports=1&champs=110163,225733&partner=61&tf=1200&tz=0&lng=en&GroupEvents=true&countryId=0&getEmpty=true&hot=false&grMode=2"
headers = {"accept": "application/json, text/plain, */*"}
resp = httpx.get(url, headers=headers)
print("Status:", resp.status_code)
print(resp.text[:500])
