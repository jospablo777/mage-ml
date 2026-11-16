"""
Block run attempts: a worker's claim of a block run increments its attempt, and the
worker's status writes apply only while the block run is RUNNING in that attempt. A
worker the scheduler gave up on (crash reset, timeout, cancel, manual retry) can no
longer overwrite what happened since.
"""
from unittest.mock import patch

from mage_ai.orchestration import attempt_fence, fusion
from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
from mage_ai.orchestration.pipeline_scheduler_original import run_block
from mage_ai.shared.retry import retry
from mage_ai.tests.base_test import AsyncDBTestCase, DBTestCase
from mage_ai.tests.factory import (
    create_pipeline_run_with_schedule,
    create_pipeline_with_blocks,
)


class BlockRunFencingTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.pipeline = create_pipeline_with_blocks(
            self.faker.unique.name(), self.repo_path,
        )
        self.pipeline_run = create_pipeline_run_with_schedule(
            pipeline_uuid=self.pipeline.uuid,
            status=PipelineRun.PipelineRunStatus.RUNNING,
        )
        self.pipeline_run.create_block_runs()
        self.block_run = BlockRun.get(
            pipeline_run_id=self.pipeline_run.id, block_uuid='block1',
        )

    def claim(self):
        return fusion.claim_block_run(self.block_run.id, self.pipeline_run.id)

    def worker(self, attempt: int) -> PipelineScheduler:
        scheduler = PipelineScheduler(self.pipeline_run)
        scheduler.attempts[self.block_run.id] = attempt
        return scheduler

    def current(self) -> BlockRun:
        self.block_run.refresh()
        return self.block_run

    def test_each_claim_increments_the_attempt_and_only_one_worker_wins(self):
        first = self.claim()
        self.assertEqual(first, 1)
        self.assertIsNone(self.claim(), 'A RUNNING block run cannot be claimed again.')
        self.block_run.update(status=BlockRun.BlockRunStatus.INITIAL)
        self.assertEqual(self.claim(), 2)
        self.assertEqual(self.current().status, BlockRun.BlockRunStatus.RUNNING)

    def test_a_worker_whose_block_run_was_reset_and_claimed_again_records_nothing(self):
        old = self.worker(self.claim())
        # The scheduler found the worker lost and a new worker claimed the block run.
        self.block_run.update(status=BlockRun.BlockRunStatus.INITIAL)
        new_attempt = self.claim()

        old.on_block_complete_without_schedule('block1', metrics=dict(rows=1))
        old.on_block_failure('block1', error=dict(error='late', message='late'))

        current = self.current()
        self.assertEqual(current.status, BlockRun.BlockRunStatus.RUNNING)
        self.assertEqual(current.attempt, new_attempt)
        self.assertNotIn('error', current.metrics or {})

        self.worker(new_attempt).on_block_complete_without_schedule('block1')
        self.assertEqual(self.current().status, BlockRun.BlockRunStatus.COMPLETED)

    def test_late_results_after_a_timeout_or_a_cancel_are_discarded(self):
        for final in (BlockRun.BlockRunStatus.FAILED, BlockRun.BlockRunStatus.CANCELLED):
            with self.subTest(final=final):
                self.block_run.update(status=BlockRun.BlockRunStatus.QUEUED, metrics=None)
                worker = self.worker(self.claim())
                self.block_run.update(status=final)

                worker.on_block_complete_without_schedule('block1')

                self.assertEqual(self.current().status, final)

    def test_a_queued_job_does_not_start_a_block_run_that_was_cancelled(self):
        self.block_run.update(status=BlockRun.BlockRunStatus.CANCELLED)
        with patch(
            'mage_ai.orchestration.pipeline_scheduler_original.ExecutorFactory'
            '.get_block_executor',
        ) as executor:
            result = run_block(
                self.pipeline_run.id, self.block_run.id, {}, {}, claim=True,
            )
        self.assertEqual(result, {})
        executor.assert_not_called()
        self.assertEqual(self.current().status, BlockRun.BlockRunStatus.CANCELLED)

    def test_a_superseded_stage_neither_completes_nor_claims_the_next_block_run(self):
        attempt = self.claim()
        next_run = BlockRun.get(pipeline_run_id=self.pipeline_run.id, block_uuid='block2')
        self.block_run.update(status=BlockRun.BlockRunStatus.INITIAL)
        self.claim()

        recorded, next_attempt = fusion.complete_and_claim(
            self.block_run.id, next_run.id, self.pipeline_run.id, attempt=attempt,
        )

        self.assertFalse(recorded)
        self.assertIsNone(next_attempt)
        self.assertEqual(self.current().status, BlockRun.BlockRunStatus.RUNNING)
        next_run.refresh()
        self.assertEqual(next_run.status, BlockRun.BlockRunStatus.INITIAL)

        recorded, next_attempt = fusion.complete_and_claim(
            self.block_run.id, next_run.id, self.pipeline_run.id,
            attempt=self.current().attempt,
        )
        self.assertTrue(recorded)
        self.assertEqual(next_attempt, 1)


class RetryStopsRunningJobsTest(AsyncDBTestCase):
    async def test_retrying_a_running_block_run_stops_its_job_and_resets_it(self):
        from mage_ai.api.operations.base import BaseOperation
        from mage_ai.api.operations.constants import OperationType
        from mage_ai.tests.factory import create_user

        pipeline = create_pipeline_with_blocks(self.faker.unique.name(), self.repo_path)
        pipeline_run = create_pipeline_run_with_schedule(
            pipeline_uuid=pipeline.uuid, status=PipelineRun.PipelineRunStatus.FAILED,
        )
        pipeline_run.create_block_runs()
        running = BlockRun.get(pipeline_run_id=pipeline_run.id, block_uuid='block1')
        fusion.claim_block_run(running.id, pipeline_run.id) or running.update(
            status=BlockRun.BlockRunStatus.RUNNING,
        )

        with patch('mage_ai.orchestration.job_manager.get_job_manager') as manager:
            response = await BaseOperation(
                action=OperationType.UPDATE,
                pk=pipeline_run.id,
                payload=dict(pipeline_run=dict(pipeline_run_action='retry_blocks')),
                resource='pipeline_runs',
                user=create_user(_owner=True),
            ).execute()

        self.assertIsNone(response.get('error'), response)
        manager.return_value.kill_block_run_job.assert_any_call(running.id)
        running.refresh()
        self.assertEqual(running.status, BlockRun.BlockRunStatus.INITIAL)


class StaleOutputsTest(DBTestCase):
    """I-277: a worker superseded while its block ran does not write the block's outputs."""

    def setUp(self):
        super().setUp()
        self.pipeline = create_pipeline_with_blocks(
            self.faker.unique.name(), self.repo_path,
        )
        self.pipeline_run = create_pipeline_run_with_schedule(
            pipeline_uuid=self.pipeline.uuid,
            status=PipelineRun.PipelineRunStatus.RUNNING,
        )
        self.pipeline_run.create_block_runs()
        self.block_run = BlockRun.get(
            pipeline_run_id=self.pipeline_run.id, block_uuid='block1',
        )

    def write_block(self, body: str) -> None:
        block = self.pipeline.get_block('block1')
        with open(block.file_path, 'w') as file:
            file.write(
                'from mage_ai.orchestration import fusion\n'
                'from mage_ai.orchestration.db.models.schedules import BlockRun\n\n\n'
                '@data_loader\n'
                'def load(*args, **kwargs):\n'
                f'{body}'
                "    return {'value': [1, 2, 3]}\n"
            )

    def outputs(self):
        return self.pipeline.get_block('block1').output_variables(
            execution_partition=self.pipeline_run.execution_partition,
        )

    def test_the_current_attempt_writes_its_outputs(self):
        self.write_block('')

        run_block(self.pipeline_run.id, self.block_run.id, {}, {}, claim=True)

        self.block_run.refresh()
        self.assertEqual(self.block_run.status, BlockRun.BlockRunStatus.COMPLETED)
        self.assertEqual(self.outputs(), ['output_0'])

    def test_a_worker_reset_and_claimed_again_while_running_writes_nothing(self):
        # While the block runs, the scheduler gives up on this worker and another claims
        # the block run.
        self.write_block(
            f'    BlockRun.get_by_id({self.block_run.id}).update(status=BlockRun.BlockRunStatus.INITIAL)\n'  # noqa: E501
            f'    fusion.claim_block_run({self.block_run.id}, {self.pipeline_run.id})\n'
        )

        with patch('mage_ai.data_preparation.executors.block_executor.BlockExecutor'
                   '.execute_callback') as callbacks:
            with self.assertRaises(attempt_fence.SupersededAttempt):
                run_block(self.pipeline_run.id, self.block_run.id, {}, {}, claim=True)

        self.block_run.refresh()
        self.assertEqual(self.block_run.status, BlockRun.BlockRunStatus.RUNNING)
        self.assertEqual(self.block_run.attempt, 2)
        self.assertNotIn('error', self.block_run.metrics or {})
        self.assertEqual(self.outputs(), [])
        callbacks.assert_not_called()

    def test_superseded_writes_are_not_retried(self):
        calls = []

        @retry(retries=3, delay=0)
        def write():
            calls.append(1)
            raise attempt_fence.SupersededAttempt('superseded')

        with self.assertRaises(attempt_fence.SupersededAttempt):
            write()
        self.assertEqual(len(calls), 1)

    def test_outside_a_claimed_attempt_writes_are_not_checked(self):
        with patch.object(attempt_fence, 'current_attempt') as current:
            attempt_fence.check()
            with attempt_fence.holding(self.block_run.id, None):
                attempt_fence.check()
        current.assert_not_called()
