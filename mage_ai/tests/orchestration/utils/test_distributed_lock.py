import unittest
from unittest.mock import patch

from mage_ai.orchestration.utils.distributed_lock import DistributedLock

REDIS_URL = 'redis://localhost:6379/0'


class FakeRedisClient:
    def __init__(self):
        self.values = {}
        self.set_error = None
        self.set_calls = 0

    def set(self, key, value, nx=False, ex=None):
        self.set_calls += 1
        if self.set_error:
            raise self.set_error
        if nx and key in self.values:
            return None
        self.values[key] = value
        return True

    def eval(self, script, numkeys, key, value):
        if self.values.get(key) == value:
            del self.values[key]
            return 1
        return 0


class DistributedLockTest(unittest.TestCase):
    def build_lock(self, client, **kwargs):
        with patch(
            'mage_ai.orchestration.utils.distributed_lock.init_redis_client',
            return_value=client,
        ):
            return DistributedLock(redis_url=REDIS_URL, **kwargs)

    def test_lock_granted_without_redis_url(self):
        lock = DistributedLock(redis_url='')

        self.assertTrue(lock.try_acquire_lock('pipeline_schedule_1'))
        self.assertTrue(lock.try_acquire_lock('pipeline_schedule_1'))
        lock.release_lock('pipeline_schedule_1')

    def test_lock_denied_when_redis_is_unreachable(self):
        lock = self.build_lock(None)

        self.assertFalse(lock.try_acquire_lock('pipeline_schedule_1'))

    def test_lock_denied_while_held(self):
        client = FakeRedisClient()
        lock = self.build_lock(client)
        other = self.build_lock(client)

        self.assertTrue(lock.try_acquire_lock('pipeline_schedule_1'))
        self.assertFalse(other.try_acquire_lock('pipeline_schedule_1'))

        lock.release_lock('pipeline_schedule_1')
        self.assertTrue(other.try_acquire_lock('pipeline_schedule_1'))

    def test_release_keeps_the_lock_of_the_next_holder(self):
        client = FakeRedisClient()
        lock = self.build_lock(client)
        other = self.build_lock(client)
        key = 'pipeline_schedule_1'

        self.assertTrue(lock.try_acquire_lock(key))
        # The key expired in Redis while the first holder was still running.
        client.values.clear()
        self.assertTrue(other.try_acquire_lock(key))

        lock.release_lock(key)

        self.assertIn(f'LOCK_KEY_{key}', client.values)
        self.assertFalse(lock.try_acquire_lock(key))

    def test_reconnects_after_the_interval(self):
        client = FakeRedisClient()
        lock = self.build_lock(None, reconnect_interval=0)

        with patch(
            'mage_ai.orchestration.utils.distributed_lock.init_redis_client',
            return_value=client,
        ) as init_redis_client:
            self.assertTrue(lock.try_acquire_lock('pipeline_schedule_1'))
            init_redis_client.assert_called_once_with(REDIS_URL)

    def test_does_not_reconnect_before_the_interval(self):
        lock = self.build_lock(None, reconnect_interval=3600)

        with patch(
            'mage_ai.orchestration.utils.distributed_lock.init_redis_client',
        ) as init_redis_client:
            self.assertFalse(lock.try_acquire_lock('pipeline_schedule_1'))
            init_redis_client.assert_not_called()

    def test_failed_command_drops_the_client(self):
        client = FakeRedisClient()
        client.set_error = ConnectionError('connection reset')
        lock = self.build_lock(client, reconnect_interval=3600)

        self.assertFalse(lock.try_acquire_lock('pipeline_schedule_1'))
        self.assertIsNone(lock.redis_client)

        # The dropped client is not reused before the reconnect interval elapses.
        self.assertFalse(lock.try_acquire_lock('pipeline_schedule_1'))
        self.assertEqual(client.set_calls, 1)


if __name__ == '__main__':
    unittest.main()
