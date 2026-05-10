import subprocess
import json

url = "https://1xbet.co.ke/service-api/LineFeed/GetSportsShortZip?sports=1&partner=61&lng=en"
res = subprocess.run(["curl", "-s", "-H", "accept: application/json", url], capture_output=True, text=True)
print("Return code:", res.returncode)
print(res.stdout[:500])
