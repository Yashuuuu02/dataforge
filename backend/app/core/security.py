"""Security utilities for encryption."""

import base64
import os
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


def _get_fernet() -> Fernet:
    """Get Fernet instance derived from ENCRYPTION_KEY env var."""
    # Fallback key for dev if not set — IN PRODUCTION ALWAYS SET ENCRYPTION_KEY
    key_material = os.environ.get("ENCRYPTION_KEY", "default_insecure_dev_key_dataforge_2024")
    
    # Derive a 32-byte url-safe base64 key using PBKDF2
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b"dataforge_salt_fixed", # Fixed salt so keys remain stable across restarts
        iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(key_material.encode()))
    return Fernet(key)


def encrypt_key(plain_text: str) -> str:
    """Encrypt a string (like an API key)."""
    if not plain_text:
        return ""
    f = _get_fernet()
    return f.encrypt(plain_text.encode()).decode()


def decrypt_key(cipher_text: str) -> str:
    """Decrypt a string."""
    if not cipher_text:
        return ""
    try:
        f = _get_fernet()
        return f.decrypt(cipher_text.encode()).decode()
    except Exception:
        # If decryption fails (e.g. key changed), return empty to avoid crashes
        return ""
