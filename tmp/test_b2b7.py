from app.workers.b2b_harvester import _curl, B2B_BOOKMAKERS, _champs_url, _referer
bk = B2B_BOOKMAKERS[-1] # paripesa

url = _champs_url(bk, 1, "upcoming")
print("URL:", url)

import subprocess, json
from app.workers.b2b_harvester import _BASE_HEADERS
cmd = ["curl", "-s", "-g", "-m10"]
for k, v in _BASE_HEADERS.items():
    cmd += ["-H", f"{k}: {v}"]
cmd += ["-H", f"referer: {_referer(bk)}", "--", url]

res = subprocess.run(cmd, capture_output=True, text=True)
print("STDOUT:", res.stdout[:500])
print("STDERR:", res.stderr)

