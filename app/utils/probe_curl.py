"""
probe_utils.py
==============
Refactored to construct URLs using an optimized O(N) dictionary lookup loop
based on a strict parameter order list, preserving the original curl fingerprint.
"""

import gzip
import json
import re
import time
import urllib.parse
import zlib
from typing import Any

from curl_cffi import requests as tls_requests

try:
    import brotli as _brotli_mod
    _BROTLI_AVAILABLE = True
except ImportError:
    _BROTLI_AVAILABLE = False


# Chrome impersonation targets -- ordered newest-first
_CHROME_TARGETS = [
    "chrome131", "chrome124", "chrome123", "chrome120", "chrome119",
    "chrome116", "chrome110", "chrome107", "chrome104",
    "chrome101", "chrome100", "chrome99",
]

_BROWSER_HEADERS: dict[str, str] = {
    "user-agent":         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "accept":             "application/json, text/plain, */*",
    "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "sec-ch-ua":          '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "connection":         "keep-alive",
}

_CACHE_HEADERS_TO_STRIP = frozenset({
    "if-modified-since", "if-none-match", "if-match",
    "if-unmodified-since", "if-range",
})


# ── URL Reconstruction Helpers (Optimized O(N) Dictionary Match) ─────────────

def _extract_order_from_raw(url_raw: str) -> tuple[str, list[str]]:
    """
    Extracts the base URL and the exact order of parameter keys from a raw URL.
    Returns: (base_url, param_order_list)
    """
    if "?" not in url_raw:
        return url_raw, []
    
    base_url, qs = url_raw.split("?", 1)
    param_order = []
    
    for part in qs.split("&"):
        if not part:
            continue
        key = urllib.parse.unquote_plus(part.split("=")[0])
        if key not in param_order:  # guard against duplicates breaking iteration
            param_order.append(key)
            
    return base_url, param_order


def _reconstruct_url(base_url: str, params: dict, param_order: list[str]) -> str:
    """
    Perfectly reconstructs the URL by iterating through the ordered keys 
    and accessing the values via dictionary lookup.
    """
    if not base_url:
        return ""
        
    query_parts = []
    seen_keys = set()
    
    # 1. Map values strictly matching the original key order
    for key in param_order:
        if key in params:
            val = params.get(key)
            ek = urllib.parse.quote(str(key), safe="")
            ev = urllib.parse.quote(str(val), safe="{}") # keep {{ }} for placeholders
            query_parts.append(f"{ek}={ev}" if val != "" else ek)
            seen_keys.add(key)
            
    # 2. Append any extra keys injected dynamically (e.g. sport_id added later)
    for key, val in params.items():
        if key not in seen_keys:
            ek = urllib.parse.quote(str(key), safe="")
            ev = urllib.parse.quote(str(val), safe="{}")
            query_parts.append(f"{ek}={ev}" if val != "" else ek)
            
    qs = "&".join(query_parts)
    return f"{base_url}?{qs}" if qs else base_url


# ── Header merge ─────────────────────────────────────────────────────────────

def _merge_headers(browser: dict, user: dict) -> dict:
    merged: dict[str, str] = {k.lower(): v for k, v in browser.items()}
    for k, v in (user or {}).items():
        key = k.strip().lower()
        if key:
            merged[key] = v
    return merged


# ── Curl string ──────────────────────────────────────────────────────────────

def _build_curl(method: str, url: str, params: dict,
                headers: dict, cookies: str | None, body: Any,
                url_raw: str = "") -> str:
    """Build a curl command string using the optimized URL reconstructor."""
    print(f"[probe] This url raw {url_raw} with params {params} and headers {headers}")
    parts: list[str] = ["curl"]
    if method.upper() != "GET":
        parts.append(f"-X {method.upper()}")

    if url_raw:
        # ── Fast Dictionary Lookup Path ──
        base_url, param_order = _extract_order_from_raw(url_raw)
        full_url = _reconstruct_url(base_url, params, param_order)
    else:
        # ── Legacy Path ──
        if params:
            qs = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
            full_url = f"{url}?{qs}"
        else:
            full_url = url

    parts.append(f"'{full_url}'")
    for k, v in headers.items():
        safe_v = v.replace("'", "'\\''")
        parts.append(f"-H '{k}: {safe_v}'")
    if cookies:
        parts.append(f"-b '{cookies.replace(chr(39), chr(39)+chr(92)+chr(39)+chr(39))}'")
    if body is not None:
        body_str = json.dumps(body) if isinstance(body, (dict, list)) else str(body)
        parts.append(f"--data-raw '{body_str.replace(chr(39), chr(39)+chr(92)+chr(39)+chr(39))}'")
    return " \\\n  ".join(parts)


# ── Decompression ─────────────────────────────────────────────────────────────

def _decompress(raw: bytes, content_encoding: str) -> tuple[bytes, str]:
    enc = content_encoding.lower()
    if "br" in enc and _BROTLI_AVAILABLE:
        try: return _brotli_mod.decompress(raw), "brotli"
        except Exception: pass
    if "gzip" in enc:
        try: return gzip.decompress(raw), "gzip"
        except Exception: pass
    if "deflate" in enc:
        for wb in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
            try: return zlib.decompress(raw, wb), "deflate"
            except Exception: pass
    if len(raw) >= 2 and raw[0] == 0x78:
        for wb in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
            try: return zlib.decompress(raw, wb), "zlib-magic"
            except Exception: pass
    if len(raw) >= 2 and raw[0] == 0x1F and raw[1] == 0x8B:
        try: return gzip.decompress(raw), "gzip-magic"
        except Exception: pass
    return raw, "none"


# ── Main probe ────────────────────────────────────────────────────────────────

def _probe(
    url:     str,
    method:  str         = "GET",
    headers: dict | None = None,
    params:  dict | None = None,
    body:    Any         = None,
    timeout: int         = 15,
    url_raw: str         = "",
) -> dict:
    """Fire an HTTP request utilizing O(N) URL parameter reconstruction."""
    method    = (method or "GET").upper()
    params    = {str(k): str(v) for k, v in (params or {}).items() if str(k).strip()}
    body_data = body

    cleaned_headers = {
        k: v for k, v in (headers or {}).items()
        if k.strip().lower() not in _CACHE_HEADERS_TO_STRIP
    }

    final_h = _merge_headers(_BROWSER_HEADERS, cleaned_headers)
    user_sent_ae = any(k.lower() == "accept-encoding" for k in cleaned_headers)
    if not user_sent_ae:
        final_h["accept-encoding"] = "gzip, deflate, br"

    cookie_val: str | None = None
    display_h: dict[str, str] = {}
    for k, v in final_h.items():
        if k.lower() == "cookie":
            cookie_val = v
        elif k.lower() == "accept-encoding" and not user_sent_ae:
            pass
        else:
            display_h[k] = v

    curl_cmd = _build_curl(method, url, params, display_h, cookie_val, body_data, url_raw=url_raw)
    print(f"Executing: {url_raw}")

    # Determine what to actually send to curl_cffi:
    if url_raw:
        base_url, param_order = _extract_order_from_raw(url_raw)
        request_url = _reconstruct_url(base_url, params, param_order)
        request_params = {}   # already embedded in request_url
    else:
        request_url    = url
        request_params = params

    resp        = None
    latency_ms  = 0
    used_target = _CHROME_TARGETS[0]
    last_exc    = None

    for target in _CHROME_TARGETS:
        t0 = time.perf_counter()
        try:
            resp = tls_requests.request(
                method=method, url=request_url,
                headers=final_h, params=request_params,
                json=body_data if body_data and isinstance(body_data, (dict, list)) else None,
                data=body_data if body_data and isinstance(body_data, str) else None,
                impersonate=target, timeout=timeout,
            )
            latency_ms  = int((time.perf_counter() - t0) * 1000)
            used_target = target
            if resp.status_code not in (403, 406):
                break
            print(f"[probe] {resp.status_code} with {target} — trying next fingerprint…")
        except Exception as exc:
            latency_ms = int((time.perf_counter() - t0) * 1000)
            last_exc   = exc
            err_str    = str(exc).lower()
            if any(x in err_str for x in ("ssl", "tls", "handshake", "connect", "unknown")):
                print(f"[probe] {target} TLS error: {exc} — trying next…")
                continue
            break

    if resp is None:
        err = str(last_exc) if last_exc else "All impersonation targets failed"
        print(f"[probe] Fatal error {method} {url}: {err}")
        return _error_response(curl_cmd, err, latency_ms)

    raw_bytes        = resp.content
    size_bytes       = len(raw_bytes)
    content_type     = resp.headers.get("Content-Type", "")
    content_encoding = resp.headers.get("Content-Encoding", "")

    decompressed, decomp_method = _decompress(raw_bytes, content_encoding)

    if resp.status_code == 304:
        return {
            "ok": False, "status": 304, "latency_ms": latency_ms,
            "content_type": content_type, "size_bytes": 0,
            "parsed": None, "response": None, "response_raw": "",
            "first_item": None, "array_key": None, "array_length": 0,
            "error": "HTTP 304 Not Modified — reload the session and re-probe.",
            "curl": curl_cmd,
        }

    if decomp_method != "none":
        print(f"[probe] {resp.status_code} {method} {url} "
              f"— {decomp_method} {size_bytes}>{len(decompressed)}b [{used_target}]")
    else:
        print(f"[probe] {resp.status_code} {method} {url} — {size_bytes}b [{used_target}]")

    raw_text = decompressed.decode("utf-8", errors="replace")

    parsed: Any = None
    jsonp_stripped = re.sub(r"^\s*[\w$]+\s*\(", "", raw_text).rstrip(");").strip()
    for candidate in (raw_text.strip(), jsonp_stripped):
        try:
            parsed = json.loads(candidate)
            break
        except Exception:
            pass

    if parsed is None and raw_text.strip():
        print(f"[probe] JSON parse failed. First 300 chars: {raw_text[:300]!r}")

    first_item:   Any        = None
    array_key:    str | None = None
    array_length: int        = 0

    if isinstance(parsed, list):
        array_length = len(parsed)
        first_item   = parsed[0] if parsed else None
    elif isinstance(parsed, dict):
        for k, v in parsed.items():
            if isinstance(v, list):
                array_key    = k
                array_length = len(v)
                first_item   = v[0] if v else None
                break

    ok = 200 <= resp.status_code < 300 and parsed is not None

    return {
        "ok": ok, "status": resp.status_code, "latency_ms": latency_ms,
        "content_type": content_type, "size_bytes": size_bytes,
        "parsed": parsed, "response": parsed,
        "response_raw": raw_text[:16_000],
        "first_item": first_item, "array_key": array_key, "array_length": array_length,
        "error": None if ok else f"HTTP {resp.status_code}",
        "curl": curl_cmd,
    }


def _error_response(curl_cmd: str, error: str, latency_ms: int) -> dict:
    return {
        "ok": False, "status": None, "latency_ms": latency_ms,
        "content_type": "", "size_bytes": 0,
        "parsed": None, "response": None, "response_raw": "",
        "first_item": None, "array_key": None, "array_length": 0,
        "error": error, "curl": curl_cmd,
    }


# =============================================================================
# Placeholder detection
# =============================================================================

_ROLE_HINTS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^(match_?id|event_?id|game_?id|fixture_?id|^Id$)", re.I), "match_id"),
    (re.compile(r"^(sport_?id|^sport$|SportId)",                     re.I), "sport_id"),
    (re.compile(r"^(league_?id|competition_?id|champ_?id|ChampId)",  re.I), "league_id"),
    (re.compile(r"^(team_?id|home_?id|T1Id|away_?id|T2Id)",         re.I), "team_id"),
    (re.compile(r"^(country_?id|^country$|CountryId)",               re.I), "country_id"),
    (re.compile(r"^(partner_?id|^partner$)",                         re.I), "partner_id"),
]


def _flatten_item(obj: Any, prefix: str = "", depth: int = 0) -> list[tuple[str, str]]:
    if depth > 5 or obj is None:
        return []
    if isinstance(obj, (str, int, float, bool)) and prefix:
        return [(prefix, str(obj))]
    if isinstance(obj, list):
        out = []
        for i, v in enumerate(obj[:5]):
            out.extend(_flatten_item(v, f"{prefix}[{i}]", depth + 1))
        return out
    if isinstance(obj, dict):
        out = []
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (str, int, float, bool)):
                out.append((path, str(v)))
            else:
                out.extend(_flatten_item(v, path, depth + 1))
        return out
    return []


def _field_to_var_name(field_path: str) -> str:
    leaf = field_path.split(".")[-1].split("[")[0]
    for pattern, role in _ROLE_HINTS:
        if pattern.search(leaf):
            return role
    clean = re.sub(r"([A-Z])", r"_\1", leaf).lower().strip("_")
    clean = re.sub(r"[^a-z0-9_]", "_", clean).strip("_")
    return clean or "value"


def _detect_placeholders(
    markets_url:    str,
    markets_params: dict,
    list_item:      dict,
    min_value_len:  int = 4,
    url_raw:        str = "",
) -> dict:
    """
    Compare values from a list item against a markets URL and its params.
    Uses O(N) URL reconstruction based on exact param order.
    """

    # ── 1. Establish strict param order ───────────────────────────────────────
    if url_raw:
        base_url, param_order = _extract_order_from_raw(url_raw)
    else:
        # Fallback for sessions saved before url_raw was added
        base_url = markets_url.split("?")[0]
        qs = markets_url.split("?")[1] if "?" in markets_url else ""
        param_order = [urllib.parse.unquote_plus(p.split("=")[0]) for p in qs.split("&") if p]
        
    # Build complete dict merging url params + specific markets_params
    complete_params = {}
    for k in param_order:
        complete_params[k] = markets_params.get(k, "")
    for k, v in markets_params.items():
        if k not in complete_params:
            complete_params[k] = v

    # ── 2. Scan list item fields ───────────────────────────────────────────────
    flat = _flatten_item(list_item)
    detected:        list[dict]     = []
    placeholder_map: dict[str, str] = {}
    used_vars:       set[str]       = set()

    for field_path, raw_value in flat:
        value = raw_value.strip()
        if len(value) < min_value_len:
            continue
        if value.lower() in ("true", "false", "null", "none", "undefined"):
            continue

        var_name = _field_to_var_name(field_path)
        base_var = var_name
        n = 2
        while var_name in used_vars:
            var_name = f"{base_var}_{n}"
            n += 1

        placeholder = f"{{{{{var_name}}}}}"
        found = False

        # Check URL path
        if value in base_url:
            base_url = base_url.replace(value, placeholder, 1)
            detected.append({"var": var_name, "field_path": field_path,
                             "value": value, "location": "url-path"})
            placeholder_map[var_name] = field_path
            used_vars.add(var_name)
            found = True

        # Check query param dictionary
        if not found:
            for key, pval in complete_params.items():
                if str(pval).strip() == value:
                    complete_params[key] = placeholder
                    detected.append({"var": var_name, "field_path": field_path,
                                    "value": value, "location": f"param:{key}"})
                    placeholder_map[var_name] = field_path
                    used_vars.add(var_name)
                    found = True
                    break

    # ── 3. Rebuild suggested URL utilizing optimized reconstruction ───────────
    suggested_url = _reconstruct_url(base_url, complete_params, param_order)

    return {
        "suggested_url":    suggested_url,
        "suggested_params": complete_params,
        "detected":         detected,
        "placeholder_map":  placeholder_map,
    }