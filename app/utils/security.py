import os


def _get_private_key():
    """Load RS256 private key from env var or file."""
    pem = os.environ.get("JWT_PRIVATE_KEY_PEM")
    if pem:
        return pem.replace("\\n", "\n")
    path = os.environ.get("JWT_PRIVATE_KEY_PATH", "keys/private.pem")
    try:
        with open(path) as f:
            return f.read()
    except FileNotFoundError:
        # Dev fallback — generate ephemeral key
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        return key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ).decode()
 
 
def _get_public_key():
    pem = os.environ.get("JWT_PUBLIC_KEY_PEM")
    if pem:
        return pem.replace("\\n", "\n")
    path = os.environ.get("JWT_PUBLIC_KEY_PATH", "keys/public.pem")
    try:
        with open(path) as f:
            return f.read()
    except FileNotFoundError:
        return None
 
 
_SIGNING_SECRET = os.environ.get("RESPONSE_SIGNING_SECRET", os.urandom(32).hex())