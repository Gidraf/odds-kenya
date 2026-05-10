import subprocess
url = "https://1xbet.co.ke/service-api/LineFeed/GetChampsZip?sport=1&partner=61&lng=en"
res = subprocess.run(["curl", "-v", "-s", "-m", "15", "-H", "accept: application/json", url], capture_output=True, text=True)
print("Code:", res.returncode)
print("Stderr:", res.stderr)
