import unittest
from unittest.mock import patch

from mage_ai.orchestration.utils.distributed_lock import DistributedLock
from mage_ai.services.redis.redis import redis_namespace

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

    def eval(self, script, numkeys, key, value, *args):
        if self.values.get(key) != value:
            return 0
        if 'expire' in script:
            self.renewals = getattr(self, 'renewals', 0) + 1
            self.expiry = args[0]
            return 1
        del self.values[key]
        return 1


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

        self.assertIn(f'LOCK_KEY_{redis_namespace()}_{key}', client.values)
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

    def test_keys_are_namespaced_by_the_metadata_database(self):
        """Deployments that share a Redis server took each other's locks."""
        client = FakeRedisClient()
        lock = self.build_lock(client)
        with patch('mage_ai.orchestration.db.db_connection_url', 'postgresql://a/one'):
            self.assertTrue(lock.try_acquire_lock('pipeline_schedule_1'))
        with patch('mage_ai.orchestration.db.db_connection_url', 'postgresql://a/two'):
            self.assertTrue(lock.try_acquire_lock('pipeline_schedule_1'))
        self.assertEqual(len(client.values), 2)

    def test_a_held_lock_is_renewed_while_the_work_runs_and_released_after(self):
        import time

        client = FakeRedisClient()
        lock = self.build_lock(client)
        other = self.build_lock(client)

        with lock.held('pipeline_run_1', timeout=1) as acquired:
            self.assertTrue(acquired)
            time.sleep(1.2)
            # Another replica is kept out past the first expiry.
            self.assertFalse(other.try_acquire_lock('pipeline_run_1'))

        self.assertGreaterEqual(client.renewals, 2)
        self.assertEqual(client.expiry, 1)
        self.assertTrue(other.try_acquire_lock('pipeline_run_1'))

    def test_a_held_lock_that_is_taken_or_lost_says_so(self):
        import time

        client = FakeRedisClient()
        lock = self.build_lock(client)
        other = self.build_lock(client)
        self.assertTrue(other.try_acquire_lock('pipeline_run_2'))
        with lock.held('pipeline_run_2', timeout=1) as acquired:
            self.assertFalse(acquired)
        other.release_lock('pipeline_run_2')

        with self.assertLogs('mage_ai.orchestration.utils.distributed_lock', 'WARNING') as logs:
            with lock.held('pipeline_run_2', timeout=1) as acquired:
                self.assertTrue(acquired)
                client.values.clear()  # expired while the holder stalled
                time.sleep(0.5)
        self.assertIn('was lost while held', '\n'.join(logs.output))

    def test_a_held_lock_without_redis_is_granted(self):
        lock = DistributedLock(redis_url='')
        with lock.held('pipeline_run_3') as acquired:
            self.assertTrue(acquired)


if __name__ == '__main__':
    unittest.main()
