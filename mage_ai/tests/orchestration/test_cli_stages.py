"""
`mage run` with block fusion: when stages start, how many run at once, what happens after
one fails, and the execution partition each block gets (I-110 to I-114).
"""
import asyncio
import threading
import time
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from mage_ai.orchestration import fusion


class FakePipeline:
    """Stages of single blocks; `upstream` maps a block to the blocks it waits for."""

    def __init__(self, upstream, concurrency_config=None):
        self.upstream = upstream
        self.concurrency_config = concurrency_config or {}

    def get_block(self, uuid):
        return SimpleNamespace(
            uuid=uuid,
            upstream_blocks=[SimpleNamespace(uuid=u) for u in self.upstream[uuid]],
        )


class Recorder:
    def __init__(self, seconds=None, fail=()):
        self.seconds = seconds or {}
        self.fail = set(fail)
        self.lock = threading.Lock()
        self.events = []
        self.running = 0
        self.peak = 0
        self.partitions = []

    def __call__(self, pipeline, stage, global_vars, run_tests, update_status, partition=None,
                 tracking=None, summary=None):
        with self.lock:
            self.events.append(('start', stage[0]))
            self.running += 1
            self.peak = max(self.peak, self.running)
            self.partitions.append(partition)
        time.sleep(self.seconds.get(stage[0], 0.01))
        with self.lock:
            self.running -= 1
            self.events.append(('end', stage[0]))
        if stage[0] in self.fail:
            raise ValueError(f'{stage[0]} failed')


def run(pipeline, recorder, **kwargs):
    stages = [[uuid] for uuid in pipeline.upstream]
    with patch.object(fusion, 'fusion_plan', return_value=stages), \
            patch.object(fusion, 'cli_tracking', return_value=None), \
            patch.object(fusion, '_run_stage_in_process', recorder):
        asyncio.run(fusion.execute_in_stages(pipeline, **kwargs))


class CliStagesTest(TestCase):
    def test_a_stage_starts_when_its_upstream_ends_not_after_the_slowest_sibling(self):
        # Stages ran in waves: c waited for a, which it does not depend on (I-114).
        pipeline = FakePipeline({'a': [], 'b': [], 'c': ['b']})
        recorder = Recorder(seconds={'a': 0.5})
        run(pipeline, recorder)
        events = recorder.events
        self.assertLess(events.index(('start', 'c')), events.index(('end', 'a')))

    def test_the_block_run_limit_and_max_parallel_bound_running_stages(self):
        # concurrency_config was ignored: a limit of 1 ran three stages at once (I-110).
        upstream = {'a': [], 'b': [], 'c': []}
        recorder = Recorder()
        run(FakePipeline(upstream, dict(block_run_limit=1)), recorder)
        self.assertEqual(recorder.peak, 1)
        recorder = Recorder(seconds={'a': 0.2, 'b': 0.2, 'c': 0.2})
        run(FakePipeline(upstream), recorder, max_parallel=2)
        self.assertEqual(recorder.peak, 2)
        recorder = Recorder(seconds={'a': 0.2, 'b': 0.2, 'c': 0.2})
        run(FakePipeline(upstream), recorder)
        self.assertEqual(recorder.peak, 3)

    def test_after_a_failure_no_stage_starts_and_running_ones_finish_first(self):
        # A sibling kept running after the failure was reported (I-112).
        pipeline = FakePipeline({'a': [], 'b': [], 'c': ['a'], 'd': ['b']})
        recorder = Recorder(seconds={'a': 0.3, 'b': 0.05}, fail={'b'})
        with self.assertRaises(ValueError) as caught:
            run(pipeline, recorder)
        self.assertIn(('end', 'a'), recorder.events)
        self.assertNotIn(('start', 'c'), recorder.events)
        self.assertNotIn(('start', 'd'), recorder.events)
        self.assertIn('Not started after the failure: c, d', '\n'.join(caught.exception.__notes__))

    def test_every_failure_is_reported(self):
        recorder = Recorder(seconds={'a': 0.1, 'b': 0.1}, fail={'a', 'b'})
        with self.assertRaises(ValueError) as caught:
            run(FakePipeline({'a': [], 'b': []}), recorder)
        self.assertIn('failed too', '\n'.join(caught.exception.__notes__))

    def test_the_execution_partition_reaches_every_stage(self):
        # The partition given to mage run was dropped (I-111).
        recorder = Recorder()
        run(FakePipeline({'a': [], 'b': ['a']}), recorder, execution_partition='run_7')
        self.assertEqual(recorder.partitions, ['run_7', 'run_7'])

    def test_no_new_stage_starts_beside_another_while_memory_is_over_the_limit(self):
        # The memory guard of scheduled runs was missing from mage run (I-113).
        recorder = Recorder(seconds={'a': 0.1, 'b': 0.1, 'c': 0.1})
        with patch.object(fusion, 'over_memory_limit', return_value=True):
            run(FakePipeline({'a': [], 'b': [], 'c': []}), recorder)
        self.assertEqual(recorder.peak, 1)
        self.assertEqual(len([e for e in recorder.events if e[0] == 'end']), 3)

    def test_stage_limit_ignores_values_that_are_not_positive_integers(self):
        self.assertIsNone(fusion.stage_limit(FakePipeline({}, dict(block_run_limit=0))))
        self.assertIsNone(fusion.stage_limit(FakePipeline({}, dict(block_run_limit='x'))))
        self.assertEqual(fusion.stage_limit(FakePipeline({}, dict(block_run_limit=2)), 5), 5)
