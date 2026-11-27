from datetime import datetime, timedelta, timezone

from mage_ai.data_preparation.logging.logger_manager_factory import LoggerManagerFactory
from mage_ai.orchestration import incidents
from mage_ai.orchestration.db.models.schedules import BlockRun, PipelineRun
from mage_ai.shared.utils import clean_name
from mage_ai.tests.base_test import DBTestCase
from mage_ai.tests.factory import (
    create_pipeline_run_with_schedule,
    create_pipeline_with_blocks,
)

START = datetime(2026, 11, 20, 8, 0, tzinfo=timezone.utc)
TRACEBACK = '''Traceback (most recent call last):
  File "transformers/block2.py", line 4, in transform
    return [row['amount'] for row in rows]
            ~~~^^^^^^^^^^
KeyError: 'amount'
Block block2 (transformers/block2.py) failed.
'''


class IncidentSummaryTest(DBTestCase):
    """
    A failed run's incident panel: each failed block's error, the end of its log, its time
    against its usual, its upstream blocks' states, and the ways to recover.
    """

    def setUp(self):
        super().setUp()
        self.pipeline = create_pipeline_with_blocks(self._testMethodName, self.repo_path)
        self.addCleanup(self.pipeline.delete)

    def run_with(self, status, blocks):
        run = create_pipeline_run_with_schedule(
            pipeline_uuid=self.pipeline.uuid, status=status, create_block_runs=False,
        )
        for block_uuid, state, seconds, metrics in blocks:
            BlockRun.create(
                pipeline_run_id=run.id, block_uuid=block_uuid, status=state,
                started_at=START if seconds is not None else None,
                completed_at=START + timedelta(seconds=seconds) if seconds is not None else None,
                metrics=metrics,
            )
        run.refresh()
        return run

    def write_log(self, run, block_uuid, text):
        manager = LoggerManagerFactory.get_logger_manager(
            pipeline_uuid=self.pipeline.uuid, block_uuid=clean_name(block_uuid),
            partition=run.execution_partition, repo_config=self.pipeline.repo_config,
        )
        with open(manager.get_log_filepath(create_dir=True), 'w') as file:
            file.write(text)

    def test_a_failed_block_with_its_error_log_and_usual_time(self):
        Status = BlockRun.BlockRunStatus
        for seconds in (10, 20, 30):
            self.run_with(PipelineRun.PipelineRunStatus.COMPLETED, [
                ('block1', Status.COMPLETED, 1, None),
                ('block2', Status.COMPLETED, seconds, None),
            ])
        # A failed run of block2 does not count toward its usual time.
        self.run_with(PipelineRun.PipelineRunStatus.FAILED, [
            ('block2', Status.FAILED, 500, None),
        ])
        run = self.run_with(PipelineRun.PipelineRunStatus.FAILED, [
            ('block1', Status.COMPLETED, 1, None),
            ('block2', Status.FAILED, 95, dict(error=dict(
                error="KeyError('amount')", message=TRACEBACK,
            ))),
            ('block3', Status.CONDITION_FAILED, None, None),
            ('block4', Status.CANCELLED, None, None),
        ])
        self.write_log(run, 'block2', ''.join(f'line {i}\n' for i in range(100)) +
                       "Traceback (most recent call last):\nKeyError: 'amount'\n")

        incident = incidents.summary(run, self.pipeline, log_lines=5)

        [failed] = incident['blocks']
        self.assertEqual(failed['block_uuid'], 'block2')
        self.assertEqual(failed['error_line'], "KeyError: 'amount'")
        self.assertEqual(failed['seconds'], 95)
        self.assertEqual(failed['usual'], dict(seconds=20, runs=3))
        self.assertEqual(failed['upstream'], dict(block1='completed'))
        self.assertTrue(failed['log']['content'].endswith("KeyError: 'amount'\n"))
        self.assertEqual(failed['log']['content'].count('\n'), 5)
        self.assertEqual(failed['log']['total_lines'], 102)
        # A failed run cancels the blocks it had not run; a skipped block is not among them.
        self.assertEqual(incident['not_completed'], [dict(block_uuid='block4', status='cancelled')])
        self.assertEqual(
            [(r['action'], r.get('block_uuid'), r.get('blocks')) for r in incident['recovery']],
            [('retry_incomplete', None, ['block2', 'block4']), ('retry_from', 'block2', None)],
        )

    def test_a_failed_block_ends_at_its_last_update(self):
        # MageML sets no completed_at on a failed block run; its failure is its last update.
        run = self.run_with(PipelineRun.PipelineRunStatus.FAILED, [
            ('block1', BlockRun.BlockRunStatus.FAILED, None, None),
        ])
        block_run = run.block_runs[0]
        block_run.update(started_at=START, updated_at=START + timedelta(seconds=42))
        run.refresh()
        [block] = incidents.summary(run, self.pipeline)['blocks']
        self.assertEqual(block['seconds'], 42)

    def test_a_run_that_did_not_fail_has_no_incident_and_a_missing_log_is_none(self):
        Status = BlockRun.BlockRunStatus
        completed = self.run_with(PipelineRun.PipelineRunStatus.COMPLETED, [
            ('block1', Status.COMPLETED, 1, None),
        ])
        self.assertIsNone(incidents.summary(completed, self.pipeline))

        failed = self.run_with(PipelineRun.PipelineRunStatus.FAILED, [
            ('block1', Status.FAILED, None, None),
        ])
        # Never started: no time.
        BlockRun.query.filter(BlockRun.pipeline_run_id == failed.id).update(
            dict(updated_at=None), synchronize_session=False,
        )
        [block] = incidents.summary(failed, self.pipeline)['blocks']
        self.assertEqual(
            (block['error_line'], block['seconds'], block['usual'], block['log'],
             block['upstream']),
            # The completed run before gives block1 its usual time.
            (None, None, dict(seconds=1, runs=1), None, {}),
        )


    def test_failed_monitors_with_their_failed_checks(self):
        Status = BlockRun.BlockRunStatus
        monitors = [
            dict(monitor='orders_quality', result='fail', checks=[
                dict(check='not_null', column='amount', result='fail', detail='3 nulls'),
                dict(check='row_count', result='pass'),
            ]),
            dict(monitor='freshness', result='warn', checks=[]),
            dict(monitor='broken', result='error', error='no column id', checks=[]),
        ]
        run = self.run_with(PipelineRun.PipelineRunStatus.FAILED, [
            ('block1', Status.FAILED, 1, dict(monitors=monitors)),
        ])
        self.assertEqual(incidents.summary(run, self.pipeline)['monitors'], [
            dict(block_uuid='block1', monitor='orders_quality', result='fail', error=None,
                 checks=[dict(check='not_null', column='amount', detail='3 nulls')]),
            dict(block_uuid='block1', monitor='broken', result='error', error='no column id',
                 checks=[]),
        ])

    def test_a_partitions_run_shows_the_same_partition_upstream(self):
        RunStatus = PipelineRun.PipelineRunStatus
        self.pipeline.partitions = dict(
            key='day', daily=dict(start='2026-11-01'), upstream=['orders_raw', 'stock_raw'],
        )

        upstream = create_pipeline_with_blocks('orders_raw', self.repo_path)
        self.addCleanup(upstream.delete)

        def upstream_run(name, partition, status):
            return PipelineRun.create(
                pipeline_uuid=name, partition=partition, status=status,
                execution_date=START, variables={},
            )

        done = upstream_run('orders_raw', '2026-11-20', RunStatus.COMPLETED)
        retried = upstream_run('orders_raw', '2026-11-20', RunStatus.FAILED)
        upstream_run('orders_raw', '2026-11-21', RunStatus.COMPLETED)
        run = self.run_with(RunStatus.FAILED, [
            ('block1', BlockRun.BlockRunStatus.FAILED, 1, None),
        ])
        run.update(partition='2026-11-20')

        self.assertEqual(incidents.summary(run, self.pipeline)['upstream_partitions'], [
            dict(pipeline_uuid='orders_raw', partition='2026-11-20', status='failed',
                 run_id=retried.id, completed_run_id=done.id),
            dict(pipeline_uuid='stock_raw', partition='2026-11-20', status=None, run_id=None,
                 completed_run_id=None),
        ])
        # An unpartitioned run has none.
        run.update(partition=None)
        self.assertEqual(incidents.summary(run, self.pipeline)['upstream_partitions'], [])


class ReadableTest(DBTestCase):
    def test_json_log_lines_read_as_time_level_and_message(self):
        line = '2026-11-27T09:23:55 {"level": "INFO", "message": "scoring rows", "x": 1}'
        self.assertEqual(
            incidents.readable(f'{line}\nplain text\n2026 {{not json}}\n'),
            '2026-11-27T09:23:55 INFO scoring rows\nplain text\n2026 {not json}\n',
        )

    def test_the_error_line_of_a_traceback_is_its_exception(self):
        self.assertEqual(incidents.error_line(TRACEBACK), "KeyError: 'amount'")
        self.assertEqual(incidents.error_line('Timed out\nafter 30 s'), 'Timed out')
        self.assertIsNone(incidents.error_line(None))


class RecentIncidentsTest(DBTestCase):
    """The pipelines failing lately, still failing first, each with its latest failure."""

    def test_still_failing_pipelines_come_before_recovered_ones(self):
        from mage_ai.orchestration.db.models.schedules import PipelineRun as Run

        Status = BlockRun.BlockRunStatus
        recovered = create_pipeline_with_blocks('recent_recovered', self.repo_path)
        failing = create_pipeline_with_blocks('recent_failing', self.repo_path)
        self.addCleanup(recovered.delete)
        self.addCleanup(failing.delete)

        def run(pipeline, status, blocks=()):
            created = create_pipeline_run_with_schedule(
                pipeline_uuid=pipeline.uuid, status=status, create_block_runs=False)
            for block_uuid, state, metrics in blocks:
                BlockRun.create(pipeline_run_id=created.id, block_uuid=block_uuid,
                                status=state, metrics=metrics)
            return created

        run(recovered, Run.PipelineRunStatus.FAILED, [('block1', Status.FAILED, None)])
        fixed = run(recovered, Run.PipelineRunStatus.COMPLETED)
        run(failing, Run.PipelineRunStatus.FAILED, [('block2', Status.FAILED, None)])
        latest = run(failing, Run.PipelineRunStatus.FAILED, [
            ('block1', Status.COMPLETED, None),
            ('block2', Status.FAILED, dict(error=dict(message=TRACEBACK))),
        ])

        rows = [r for r in incidents.recent(days=7)
                if r['pipeline_uuid'] in (recovered.uuid, failing.uuid)]

        self.assertEqual([r['pipeline_uuid'] for r in rows], [failing.uuid, recovered.uuid])
        self.assertEqual(
            (rows[0]['failed_runs'], rows[0]['latest_run_id'], rows[0]['recovered_by']),
            (2, latest.id, None),
        )
        self.assertEqual(rows[0]['blocks'],
                         [dict(block_uuid='block2', error_line="KeyError: 'amount'")])
        self.assertEqual(rows[1]['recovered_by'], fixed.id)

    def test_the_api_lists_them_for_a_number_of_days(self):
        import asyncio

        from mage_ai.api.errors import ApiError
        from mage_ai.api.resources.IncidentResource import IncidentResource

        result = asyncio.run(IncidentResource.collection(dict(days=['3']), {}, None))
        self.assertIsInstance(list(result), list)
        with self.assertRaises(ApiError):
            asyncio.run(IncidentResource.collection(dict(days=['400']), {}, None))
