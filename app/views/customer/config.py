from datetime import timedelta

FREE_ACCESS = True
_ENDPOINT_ACCESS = {
    "get_upcoming": "free", "get_live": "free", "get_results": "free", 
    "get_results_by_date": "free", "get_match": "free", "search_matches": "free",
    "list_sports": "free", "list_bookmakers": "free", "list_markets": "free", 
    "harvest_status": "free", "get_match_analytics": "free",
}

FREE_MATCH_LIMIT = 1000
_WS_CHANNEL      = "odds:updates"
_ARB_CHANNEL     = "arb:updates"
_EV_CHANNEL      = "ev:updates"
_CACHE_PREFIXES  = ["sbo", "sp", "bt", "od", "b2b", "bt_od"]
_STREAM_BATCH    = 20
MIN_BOOKMAKERS   = 2
_LIVE_WINDOW     = timedelta(hours=2, minutes=30)

_TERMINAL_STATUSES = frozenset({"FINISHED", "CANCELLED", "POSTPONED", "SUSPENDED"})
_EXCLUDE_FROM_UPCOMING = frozenset({"FINISHED", "CANCELLED", "POSTPONED", "SUSPENDED", "IN_PLAY", "LIVE", "INPLAY", "IN PLAY"})

_BK_SLUG = {"sportpesa": "sp", "betika": "bt", "odibets": "od", "sp": "sp", "bt": "bt", "od": "od", "sbo": "sbo", "b2b": "b2b"}

_SPORT_ALIASES = {
    "soccer": ["Soccer", "Football"], "football": ["Soccer", "Football"],
    "basketball": ["Basketball"], "tennis": ["Tennis"], "ice-hockey": ["Ice Hockey"],
    "volleyball": ["Volleyball"], "cricket": ["Cricket"], "rugby": ["Rugby"],
    "table-tennis": ["Table Tennis"], "handball": ["Handball"], "mma": ["MMA"],
    "boxing": ["Boxing"], "darts": ["Darts"], "esoccer": ["eSoccer", "eFootball"],
    "baseball": ["Baseball"], "american-football": ["American Football"],
}

_CANONICAL_SLUG = {
    "Football": "soccer", "football": "soccer", "Soccer": "soccer", "soccer": "soccer",
    "Ice Hockey": "ice-hockey", "ice hockey": "ice-hockey", "ice-hockey": "ice-hockey",
    "Table Tennis": "table-tennis","table tennis": "table-tennis","table-tennis":"table-tennis",
    "Basketball": "basketball", "Tennis": "tennis", "Cricket": "cricket", 
    "Volleyball": "volleyball", "Rugby": "rugby", "Handball": "handball",
    "MMA": "mma", "Boxing": "boxing", "Darts": "darts", 
    "eSoccer": "esoccer", "eFootball": "esoccer", "Baseball": "baseball", "baseball": "baseball",
    "American Football": "american-football", "american football": "american-football", "american-football": "american-football",
}

_SSE_HEADERS = {
    "Content-Type": "text/event-stream", "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no", "Access-Control-Allow-Origin": "*", "Connection": "keep-alive",
}

_POPULARITY_WEIGHTS = {
    "soccer": {"england": 1, "spain": 2, "germany": 3, "italy": 4, "france": 5, "brazil": 6, "argentina": 7, "netherlands": 8, "portugal": 9},
    "basketball": {"usa": 1, "spain": 2, "greece": 3, "turkey": 4, "italy": 5},
    "cricket": {"india": 1, "australia": 2, "england": 3, "pakistan": 4, "south africa": 5, "new zealand": 6},
    "tennis": {"atp": 1, "wta": 2, "challenger": 3, "itf": 4}
}

"""
config.py — central configuration for the Odds Video Studio encoder.

Every value can be overridden with an environment variable, so the same code
runs locally and in production without edits. In development, values are read
from a `.env` file (see `.env.example`).
"""

import os
from pathlib import Path

try:
    # Optional: only used in development. Safe to skip in production.
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ── Paths ───────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent

# Where uploaded .webm files and encoded .mp4 files live.
STORAGE_DIR = Path(os.getenv("STORAGE_DIR", BASE_DIR / "storage"))
UPLOAD_DIR = STORAGE_DIR / "uploads"
OUTPUT_DIR = STORAGE_DIR / "outputs"

for _d in (UPLOAD_DIR, OUTPUT_DIR):
    _d.mkdir(parents=True, exist_ok=True)


# ── Redis / Celery ──────────────────────────────────────────────────────
# Redis is used as both the Celery broker and result backend.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", REDIS_URL)
CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", REDIS_URL)


# ── Upload limits / validation ──────────────────────────────────────────
# Max upload size in bytes. A 4K WebM clip is usually well under this.
MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH", 300 * 1024 * 1024))  # 300 MB
ALLOWED_EXTENSIONS = {".webm", ".mp4", ".mov", ".mkv"}


# ── CORS ────────────────────────────────────────────────────────────────
# Comma-separated list of origins allowed to call the API.
# Use "*" only for local testing — set the real frontend origin in production.
CORS_ORIGINS = [
    o.strip() for o in os.getenv(
        "CORS_ORIGINS",
        "http://localhost:5173,http://localhost:3000,https://kinetic.gidraf.dev",
    ).split(",") if o.strip()
]


# ── ffmpeg ──────────────────────────────────────────────────────────────
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "ffmpeg")
FFPROBE_BIN = os.getenv("FFPROBE_BIN", "ffprobe")

# H.264 quality. CRF 18-23 is the useful range — lower = better/larger.
H264_CRF = os.getenv("H264_CRF", "20")
# Encoding speed/efficiency tradeoff: ultrafast … slow. "medium" is a good default.
H264_PRESET = os.getenv("H264_PRESET", "medium")
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "192k")

# Encoded files older than this (seconds) are deleted by the cleanup task.
RESULT_TTL_SECONDS = int(os.getenv("RESULT_TTL_SECONDS", 6 * 60 * 60))  # 6 hours