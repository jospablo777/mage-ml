import logging
import os
import threading
import time
import uuid

from mage_ai.services.redis.redis import init_redis_client
from mage_ai.settings import REDIS_URL

logger = logging.getLogger(__name__)


def _int_from_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except ValueError:
        return default


REDIS_LOCK_DEFAULT_TIMEOUT = _int_from_env('REDIS_LOCK_DEFAULT_TIMEOUT', 30)
REDIS_RECONNECT_INTERVAL = _int_from_env('REDIS_LOCK_RECONNECT_INTERVAL', 10)

# Deletes the key only when the caller still holds it. A holder whose lock expired
# would otherwise delete the key of the next holder.
RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class DistributedLock:
    """
    Coordinates schedulers through Redis.

    Without REDIS_URL there is no coordination and every lock is granted, which is
    only correct when a single scheduler runs. With REDIS_URL set, locks are denied
    while Redis is unreachable so that several schedulers cannot act at the same time.
    """

    def __init__(
        self,
        lock_key_prefix='LOCK_KEY',
        lock_timeout=REDIS_LOCK_DEFAULT_TIMEOUT,
        redis_url=None,
        reconnect_interval=REDIS_RECONNECT_INTERVAL,
    ):
        self.lock_key_prefix = lock_key_prefix
        self.lock_timeout = lock_timeout
        self.reconnect_interval = reconnect_interval
        self.redis_url = REDIS_URL if redis_url is None else redis_url
        self.redis_client = init_redis_client(self.redis_url) if self.redis_url else None
        self.__last_connection_attempt = time.monotonic()
        self.__tokens = {}
        self.__tokens_lock = threading.Lock()

    @property
    def coordination_required(self) -> bool:
        return bool(self.redis_url)

    def __lock_key(self, key) -> str:
        return f'{self.lock_key_prefix}_{key}'

    def __client(self):
        if self.redis_client is not None:
            return self.redis_client

        elapsed = time.monotonic() - self.__last_connection_attempt
        if elapsed < self.reconnect_interval:
            return None

        self.__last_connection_attempt = time.monotonic()
        self.redis_client = init_redis_client(self.redis_url)
        if self.redis_client is None:
            logger.warning(
                'Redis at %s is unreachable, locks are denied until it responds.',
                self.redis_url,
            )

        return self.redis_client

    def __drop_client(self, error: Exception) -> None:
        logger.warning('Redis lock operation failed: %s', error)
        self.redis_client = None
        self.__last_connection_attempt = time.monotonic()

    def try_acquire_lock(self, key, timeout: int = None) -> bool:
        if not self.coordination_required:
            return True

        client = self.__client()
        if client is None:
            return False

        token = uuid.uuid4().hex
        try:
            acquired = client.set(
                self.__lock_key(key),
                token,
                nx=True,
                ex=timeout or self.lock_timeout,
            )
        except Exception as error:
            self.__drop_client(error)
            return False

        if acquired is not True:
            return False

        with self.__tokens_lock:
            self.__tokens[key] = token

        return True

    def release_lock(self, key) -> None:
        if not self.coordination_required:
            return

        with self.__tokens_lock:
            token = self.__tokens.pop(key, None)

        if token is None:
            return

        client = self.__client()
        if client is None:
            return

        try:
            client.eval(RELEASE_SCRIPT, 1, self.__lock_key(key), token)
        except Exception as error:
            self.__drop_client(error)
