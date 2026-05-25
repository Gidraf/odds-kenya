import os
import random
import json
from app.workers.celery_tasks import _redis as _get_redis

def get_active_proxy() -> str:
    """
    Retrieve an active SOCKS5 proxy URL from the Tailscale pool in Redis.
    If multiple active proxies are present, it selects one randomly
    to achieve concurrent harvesting rotation.
    Falls back to ALL_PROXY/HTTP_PROXY env variables if pool is empty or unavailable.
    """
    try:
        r = _get_redis()
        if r:
            raw = r.get("tailscale:active_proxies")
            if raw:
                proxies = json.loads(raw)
                if proxies and isinstance(proxies, list):
                    # Rotate randomly to load-balance concurrently across devices
                    return random.choice(proxies)
    except Exception:
        pass
    
    # Fallback to standard environment proxy
    return os.environ.get("ALL_PROXY") or os.environ.get("HTTP_PROXY") or ""
