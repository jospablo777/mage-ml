import logging

import redis

logger = logging.getLogger(__name__)


def init_redis_client(redis_url):
    if not redis_url:
        return None
    try:
        redis_client = redis.Redis.from_url(url=redis_url, decode_responses=True)
        redis_client.ping()
    except Exception as error:
        # Callers retry, so the traceback would repeat for as long as Redis is down.
        logger.warning('Could not connect to Redis: %s', error)
        logger.debug('Redis connection error', exc_info=True)
        redis_client = None
    return redis_client
