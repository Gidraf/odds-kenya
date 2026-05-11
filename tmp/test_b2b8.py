from app.workers.b2b_harvester import _curl, B2B_BOOKMAKERS, _champs_url
import subprocess
bk = B2B_BOOKMAKERS[-1] # paripesa

url = _champs_url(bk, 1, "upcoming")
from app.workers.b2b_harvester import _BASE_HEADERS

cmd = ["curl", "-v", "-s", "-g", "-m10"]
for k, v in _BASE_HEADERS.items():
    cmd += ["-H", f"{k}: {v}"]
cmd += ["--", url]
print("CMD:", " ".join(cmd))

res = subprocess.run(cmd, capture_output=True, text=True)
print("STDOUT:", res.stdout[:500])
print("STDERR:", res.stderr)

