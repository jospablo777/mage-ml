"""
Lock behavior against a running Redis.

The unit tests use a fake client, so they cover the branches and not the Redis
semantics the lock depends on: SET with NX and EX, and the Lua script that
releases a key only for the holder. Set MAGE_TEST_REDIS_URL to run these.
"""
import os
import time
import unittest

from mage_ai.orchestration.utils.distributed_lock import DistributedLock

REDIS_URL = os.getenv('MAGE_TEST_REDIS_URL')


@unittest.skipUnless(REDIS_URL, 'MAGE_TEST_REDIS_URL is not set')
class DistributedLockIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.prefix = 'TEST_LOCK_%s' % os.getpid()
        self.lock = self.build_lock()
        self.other = self.build_lock()
        self.addCleanup(self.clear_keys)

    def build_lock(self, **kwargs):
        return DistributedLock(lock_key_prefix=self.prefix, redis_url=REDIS_URL, **kwargs)

    def clear_keys(self):
        client = self.lock.redis_client
        if client:
            keys = list(client.scan_iter('%s_*' % self.prefix))
            if keys:
                client.delete(*keys)

    def test_connects(self):
        self.assertIsNotNone(self.lock.redis_client)

    def test_only_one_holder_at_a_time(self):
        self.assertTrue(self.lock.try_acquire_lock('schedule_1'))
        self.assertFalse(self.other.try_acquire_lock('schedule_1'))

        self.lock.release_lock('schedule_1')

        self.assertTrue(self.other.try_acquire_lock('schedule_1'))

    def test_release_runs_the_lua_script(self):
        key = '%s_schedule_2' % self.prefix
        self.assertTrue(self.lock.try_acquire_lock('schedule_2'))
        self.assertIsNotNone(self.lock.redis_client.get(key))

        self.lock.release_lock('schedule_2')

        self.assertIsNone(self.lock.redis_client.get(key))

    def test_release_leaves_the_key_of_the_next_holder(self):
        key = '%s_schedule_3' % self.prefix
        self.assertTrue(self.lock.try_acquire_lock('schedule_3', timeout=1))
        time.sleep(1.5)
        self.assertTrue(self.other.try_acquire_lock('schedule_3'))

        # The first holder finishes after its key expired.
        self.lock.release_lock('schedule_3')

        self.assertIsNotNone(self.lock.redis_client.get(key))
        self.assertFalse(self.lock.try_acquire_lock('schedule_3'))

    def test_key_expires(self):
        key = '%s_schedule_4' % self.prefix
        self.assertTrue(self.lock.try_acquire_lock('schedule_4', timeout=1))

        self.assertGreater(self.lock.redis_client.ttl(key), 0)
        time.sleep(1.5)

        self.assertIsNone(self.lock.redis_client.get(key))
        self.assertTrue(self.other.try_acquire_lock('schedule_4'))

    def test_lock_is_denied_after_redis_goes_away(self):
        lock = self.build_lock(reconnect_interval=3600)
        lock.redis_url = 'redis://127.0.0.1:1/0'
        lock.redis_client = None

        self.assertFalse(lock.try_acquire_lock('schedule_5'))


if __name__ == '__main__':
    unittest.main()
