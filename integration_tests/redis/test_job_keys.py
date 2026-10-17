"""
Job keys left by a replica that is gone are swept, against a running Redis (upstream
#6102). Set MAGE_TEST_REDIS_URL to run these.
"""
import os
import unittest
import uuid

from mage_ai.orchestration.queue.process_queue import sweep_dead_job_keys
from mage_ai.services.redis.redis import init_redis_client, redis_namespace

REDIS_URL = os.getenv('MAGE_TEST_REDIS_URL')


@unittest.skipUnless(REDIS_URL, 'MAGE_TEST_REDIS_URL is not set')
class JobKeySweepTest(unittest.TestCase):
    def setUp(self):
        self.client = init_redis_client(REDIS_URL)
        self.run_id = uuid.uuid4().hex[:8]
        self.keys = []
        self.addCleanup(lambda: self.keys and self.client.delete(*self.keys))

    def key(self, name, value, **kwargs):
        self.client.set(name, value, **kwargs)
        self.keys.append(name)
        return name

    def test_only_the_keys_of_gone_replicas_are_deleted(self):
        me, alive, gone = (f'HOST_{n}_{self.run_id}' for n in ('me', 'alive', 'gone'))
        self.key(alive, '1', ex=30)
        ns = redis_namespace()
        mine = self.key(f'{ns}:job_mine_{self.run_id}', me)
        running = self.key(f'{ns}:job_alive_{self.run_id}', alive)
        orphan = self.key(f'{ns}:job_gone_{self.run_id}', gone)
        kill = self.key(f'{ns}:kill_job_x_{self.run_id}', '1', ex=300)

        sweep_dead_job_keys(self.client, me)

        self.assertEqual(
            [bool(self.client.exists(k)) for k in (mine, running, orphan, kill)],
            [True, True, False, True],
        )
