#!/usr/bin/env python3
"""
scripts/update_proxies.py
=========================
Host-side automation script to check Tailscale status, verify which devices
have an active SOCKS5 proxy listening on port 1080, and synchronize the list
in Redis for container-native harvesters.

Setup:
  1. Make executable: chmod +x scripts/update_proxies.py
  2. Run via crontab every minute:
     * * * * * cd /home/appuser/sports/odds-kenya && ./scripts/update_proxies.py >> /var/log/tailscale_proxies.log 2>&1
"""

import os
import subprocess
import socket
import json
import re

# We test SOCKS5 on port 1080
PROXY_PORT = 1080
TIMEOUT_S = 2

def load_redis_password():
    # Look for .env in the root directory relative to this script
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(base_dir, ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                clean = line.strip()
                if clean.startswith("REDIS_PASSWORD="):
                    # Extract and strip quotes
                    val = clean.split("=", 1)[1].strip()
                    if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                        val = val[1:-1]
                    return val
    return None

def is_socks5_alive(ip, port=PROXY_PORT, timeout=TIMEOUT_S):
    """Perform a simple SOCKS5 handshake or TCP socket check to verify port is open."""
    try:
        # 1. Open socket connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((ip, port))
        
        # 2. Optional: simple SOCKS5 handshake (no auth)
        # Send greeting: [SOCKS_VERSION (5), NUM_METHODS (1), NO_AUTH_METHOD (0)]
        sock.sendall(bytes([0x05, 0x01, 0x00]))
        resp = sock.recv(2)
        
        sock.close()
        # SOCKS5 returns version 5 (0x05) and accepted method (0x00)
        if len(resp) == 2 and resp[0] == 0x05 and resp[1] == 0x00:
            return True
    except Exception:
        pass
    return False

def get_tailscale_ips():
    try:
        # Run tailscale status
        output = subprocess.check_output(["tailscale", "status"], text=True)
        ips = []
        for line in output.splitlines():
            tokens = line.strip().split()
            if not tokens:
                continue
            ip = tokens[0]
            # Match standard IPv4 addresses in Tailscale subnet (100.64.0.0/10)
            if re.match(r"^100\.\d{1,3}\.\d{1,3}\.\d{1,3}$", ip):
                ips.append(ip)
        return ips
    except Exception as e:
        print(f"Error running 'tailscale status': {e}")
        return []

def main():
    print(f"--- Running Tailscale Proxy Update: {subprocess.check_output(['date']).decode().strip()} ---")
    
    ips = get_tailscale_ips()
    if not ips:
        print("No Tailscale devices discovered.")
        
    active_proxies = []
    for ip in ips:
        print(f"🔍 Testing SOCKS5 proxy on {ip}:{PROXY_PORT}...")
        if is_socks5_alive(ip):
            print(f"   ✅ {ip} is active SOCKS5 proxy!")
            # Use 'socks5h' scheme to resolve DNS on the exit node proxy host
            active_proxies.append(f"socks5h://{ip}:{PROXY_PORT}")
        else:
            print(f"   ❌ {ip} proxy is down/unreachable.")

    # Write to Redis
    redis_pwd = load_redis_password()
    try:
        import redis
        # Connect to local Redis exposed on port 6382
        r = redis.Redis(host="127.0.0.1", port=6382, password=redis_pwd, socket_timeout=5)
        
        # Save JSON array
        r.set("tailscale:active_proxies", json.dumps(active_proxies), ex=86400)
        r.set("tailscale:last_update", int(subprocess.check_output(["date", "+%s"]).strip().decode()), ex=86400)
        
        print(f"SUCCESS: Synchronized {len(active_proxies)} active proxy/proxies in Redis: {active_proxies}")
    except Exception as e:
        print(f"ERROR connecting to Redis: {e}")

if __name__ == "__main__":
    main()
