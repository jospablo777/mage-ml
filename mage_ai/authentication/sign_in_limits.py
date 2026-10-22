"""
Failed sign-ins per account. Without a limit, a password could be guessed as fast as parallel
requests went: after MAGE_SIGN_IN_MAX_FAILURES failed attempts (10) within
MAGE_SIGN_IN_WINDOW_SECONDS (900) the account's sign-ins are refused until the oldest of them
leaves the window. A successful sign-in clears the account's failures. Kept in the server
process.
"""
import os
import threading
import time
from collections import defaultdict, deque
from typing import Deque, Dict

from mage_ai.api.errors import ApiError


def _setting(name: str, default: int) -> int:
    try:
        return max(int(os.getenv(name) or default), 1)
    except ValueError:
        return default


MAX_FAILURES = _setting('MAGE_SIGN_IN_MAX_FAILURES', 10)
WINDOW_SECONDS = _setting('MAGE_SIGN_IN_WINDOW_SECONDS', 900)

_lock = threading.Lock()
_failures: Dict[str, Deque[float]] = defaultdict(deque)


def _key(account: str) -> str:
    return (account or '').strip().lower()


def _recent(key: str, now: float) -> Deque[float]:
    times = _failures[key]
    while times and now - times[0] >= WINDOW_SECONDS:
        times.popleft()
    if not times:
        _failures.pop(key, None)
    return times


def check(account: str) -> None:
    """Refuses a sign-in to an account with too many recent failures."""
    now = time.monotonic()
    with _lock:
        times = _recent(_key(account), now)
        if len(times) < MAX_FAILURES:
            return
        wait = int(WINDOW_SECONDS - (now - times[0])) + 1
    raise ApiError(dict(
        code=429,
        message=f'Too many failed sign-ins for this account; try again in {wait} s.',
        type='too_many_requests',
    ))


def failed(account: str) -> None:
    now = time.monotonic()
    with _lock:
        _recent(_key(account), now)
        _failures[_key(account)].append(now)


def succeeded(account: str) -> None:
    with _lock:
        _failures.pop(_key(account), None)


def reset() -> None:
    """For tests."""
    with _lock:
        _failures.clear()
