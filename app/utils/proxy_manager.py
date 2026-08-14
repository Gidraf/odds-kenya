import os
import random
import json
import socket
import subprocess
import re
import logging

logger = logging.getLogger(__name__)

PROXY_PORT = 1080

def _is_socks5_alive(ip: str, port: int = PROXY_PORT, timeout: float = 1.0) -> bool:
    """Test if a SOCKS5 proxy port is open and responding."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((ip, port))
        sock.sendall(bytes([0x05, 0x01, 0x00]))
        resp = sock.recv(2)
        sock.close()
        return len(resp) == 2 and resp[0] == 0x05 and resp[1] == 0x00
    except Exception:
        return False

def discover_tailscale_proxies() -> list[str]:
    """Dynamically discover active Tailscale SOCKS5 proxies by querying tailscale status."""
    active = []
    try:
        out = subprocess.check_output(["tailscale", "status"], text=True, timeout=3)
        for line in out.splitlines():
            line_str = line.strip()
            # Exclude empty lines and offline nodes
            if not line_str or "offline" in line_str.lower():
                continue
            tokens = line_str.split()
            if not tokens:
                continue
            ip = tokens[0]
            if re.match(r"^100\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip):
                if _is_socks5_alive(ip):
                    active.append(f"socks5h://{ip}:{PROXY_PORT}")
    except Exception as exc:
        logger.debug("Tailscale CLI discovery unavailable: %s", exc)
    return active

def get_active_proxy() -> str:
    """
    Retrieve an active SOCKS5 proxy URL automatically.
    1. Checks Redis pool ('tailscale:active_proxies').
    2. If Redis pool is empty/unavailable, attempts dynamic Tailscale status discovery.
    3. Falls back to ALL_PROXY / HTTP_PROXY env vars.
    """
    # 1. Try Redis pool
    try:
        from app.workers.celery_tasks import _redis as _get_redis
        r = _get_redis()
        if r:
            raw = r.get("tailscale:active_proxies")
            if raw:
                proxies = json.loads(raw)
                if proxies and isinstance(proxies, list):
                    valid = [p for p in proxies if isinstance(p, str) and p.strip()]
                    if valid:
                        return random.choice(valid)
    except Exception as exc:
        logger.debug("Redis proxy lookup failed: %s", exc)

    # 2. Try dynamic CLI discovery
    try:
        discovered = discover_tailscale_proxies()
        if discovered:
            return random.choice(discovered)
    except Exception:
        pass

    # 3. Fallback to standard environment proxy
    return os.environ.get("ALL_PROXY") or os.environ.get("HTTP_PROXY") or ""
