import hashlib
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


def redis_namespace() -> str:
    """
    A prefix for the Redis keys of one Mage deployment, from its metadata database URL.
    Job and lock keys were named by ids alone, such as block_run_1, so deployments that
    share a Redis server, or a deployment whose metadata database was recreated, took
    each other's keys and skipped jobs.
    """
    from mage_ai.orchestration.db import db_connection_url

    return hashlib.sha256(str(db_connection_url or '').encode()).hexdigest()[:12]
