

from datetime import datetime, timedelta
import hashlib
import hmac
import json
import time

from flask import Response, request
from pytz import timezone

from app.utils.security import _SIGNING_SECRET, _get_private_key, _get_public_key


def _issue_token(user_id: int, token_type: str = "access", extra: dict | None = None) -> str:
    import jwt as _jwt
    now     = datetime.now(timezone.utc)
    expires = now + (timedelta(hours=24) if token_type == "access" else timedelta(days=30))
    payload = {
        "sub":  str(user_id),
        "type": token_type,
        "iat":  int(now.timestamp()),
        "exp":  int(expires.timestamp()),
        **(extra or {}),
    }
    return _jwt.encode(payload, _get_private_key(), algorithm="RS256")
 
 
def _decode_token(token: str) -> dict:
    import jwt as _jwt
    pub = _get_public_key()
    if not pub:
        raise ValueError("No public key configured")
    return _jwt.decode(token, pub, algorithms=["RS256"],
                       options={"verify_exp": True})
 
 
def _current_user_from_header() -> "Customer | None":
    from app.models.customer import Customer
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
    elif request.headers.get("X-Api-Key"):
        return _current_user_from_api_key(request.headers.get("X-Api-Key"))
    else:
        return None
    try:
        payload = _decode_token(token)
        if payload.get("type") not in ("access", "api"):
            return None
        return Customer.query.get(int(payload["sub"]))
    except Exception:
        return None
 
 
def _current_user_from_api_key(api_key: str) -> "Customer | None":
    from app.models.api_key import ApiKey
    from app.models.customer import Customer
    from app.extensions import db
    ak = ApiKey.query.filter_by(key=api_key, is_active=True).first()
    if not ak:
        return None
    allowed, remaining = ak.check_rate_limit()
    if not allowed:
        return None
    db.session.commit()
    user = Customer.query.get(ak.user_id)
    if user and user.tier == "premium":
        return user
    return None
 
 
# =============================================================================
# Response helpers
# =============================================================================
 
def _signed_response(data: dict, status: int = 200,
                     encrypt_for: "Customer | None" = None) -> Response:
    """
    Serialise data to JSON, add HMAC signature header, optionally RSA-encrypt.
    """
    body = json.dumps(data, default=str)
 
    # HMAC-SHA256 signature of the raw JSON body
    sig = hmac.new(
        _SIGNING_SECRET.encode(),
        body.encode(),
        hashlib.sha256,
    ).hexdigest()
 
    # Optional RSA payload encryption (premium developers)
    if encrypt_for and encrypt_for.public_key_pem and request.args.get("encrypt") == "1":
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.primitives import hashes, serialization
        pub_key = serialization.load_pem_public_key(
            encrypt_for.public_key_pem.encode()
        )
        # Encrypt with user's public key (they decrypt with their private key)
        encrypted = pub_key.encrypt(
            body.encode()[:190],   # RSA 2048 can encrypt ~190 bytes; chunk if needed
            padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None),
        )
        import base64
        body   = json.dumps({"encrypted": base64.b64encode(encrypted).decode(), "chunked": False})
        status = 200
 
    resp = Response(body, status=status, mimetype="application/json")
    resp.headers["X-Signature"]     = sig
    resp.headers["X-Timestamp"]     = str(int(time.time()))
    resp.headers["X-API-Version"]   = "1.0"
    return resp
 
 
def _err(msg: str, status: int = 400) -> Response:
    return _signed_response({"error": msg, "ok": False}, status)