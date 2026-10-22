import secrets
import threading

import bcrypt

# bcrypt hashes the first 72 bytes. bcrypt 4 truncated longer input and bcrypt 5
# raises ValueError, so truncate here to keep existing hashes valid.
BCRYPT_MAX_PASSWORD_BYTES = 72

def generate_salt() -> str:
    return bcrypt.gensalt(14)


def create_bcrypt_hash(password: str, salt: str) -> str:
    password_bytes = password.encode()[:BCRYPT_MAX_PASSWORD_BYTES]
    password_hash_bytes = bcrypt.hashpw(password_bytes, salt)
    password_hash_str = password_hash_bytes.decode()
    return password_hash_str


def verify_password(password: str, hash_from_database: str) -> bool:
    password_bytes = password.encode()[:BCRYPT_MAX_PASSWORD_BYTES]
    hash_bytes = hash_from_database.encode()

    # this will automatically retrieve the salt from the hash,
    # then combine it with the password (parameter 1)
    # and then hash that, and compare it to the user's hash
    does_match = bcrypt.checkpw(password_bytes, hash_bytes)

    return does_match


_unmatchable = None
_unmatchable_lock = threading.Lock()


def unmatchable_hash() -> str:
    """
    A hash no password matches, made at the cost of real ones: checked when no user has the
    email, a refusal takes as long as for a wrong password, so timing does not tell which
    emails have accounts.
    """
    global _unmatchable
    with _unmatchable_lock:
        if _unmatchable is None:
            _unmatchable = create_bcrypt_hash(secrets.token_hex(32), generate_salt())
        return _unmatchable
