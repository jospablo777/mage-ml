"""
The state of a pipeline's partitions (orchestration/partition_state.py): missing, queued,
running, failed, done and stale, by code or by an upstream pipeline, and the actions on them.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration import partition_state
from mage_ai.orchestration.db.models.schedules import PipelineRun
from mage_ai.orchestration.partition_runs import run_partition, run_selected
from mage_ai.tests.base_test import DBTestCase

Status = PipelineRun.PipelineRunStatus
NOW = datetime(2026, 11, 7, 12, tzinfo=timezone.utc)


class PartitionStateTest(DBTestCase):
    def setUp(self):
        super().setUp()
        partition_state._codes.clear()
        code = patch('mage_ai.orchestration.run_records.code_digest', return_value='code-2')
        code.start()
        self.addCleanup(code.stop)

    def pipeline(self, name=None, upstream=()):
        name = name or self._testMethodName
        pipeline = Pipeline.create(name, repo_path=self.repo_path)
        self.addCleanup(pipeline.delete)
        declaration = dict(key='day', daily=dict(start='2026-11-01'))
        if upstream:
            declaration['upstream'] = list(upstream)
        asyncio.run(pipeline.update(dict(partitions=declaration)))
        return Pipeline.get(pipeline.uuid, repo_path=self.repo_path)

    def finish(self, run, status=Status.COMPLETED, code='code-2', at=None):
        run.update(status=status, completed_at=at or datetime.now(timezone.utc),
                   metrics=dict(run_record=dict(code=code)))

    def by_partition(self, pipeline):
        found = partition_state.states(pipeline, NOW)
        return {row['partition']: row for row in found['partitions']}, found['counts']

    def test_each_state_from_the_runs(self):
        pipeline = self.pipeline()
        self.finish(run_partition(pipeline, '2026-11-01')[0])
        self.finish(run_partition(pipeline, '2026-11-02')[0], code='code-1')
        self.finish(run_partition(pipeline, '2026-11-03')[0], status=Status.FAILED)
        running, _ = run_partition(pipeline, '2026-11-04')
        running.update(status=Status.RUNNING)
        run_partition(pipeline, '2026-11-05')

        rows, counts = self.by_partition(pipeline)

        self.assertEqual({v: r['state'] for v, r in rows.items()}, {
            '2026-11-01': 'done', '2026-11-02': 'stale', '2026-11-03': 'failed',
            '2026-11-04': 'running', '2026-11-05': 'queued', '2026-11-06': 'missing',
        })
        self.assertEqual(rows['2026-11-02']['stale_because'],
                         [dict(kind='code', ran_with='code-1', now='code-2')])
        self.assertEqual(counts, dict(missing=1, queued=1, running=1, failed=1, stale=1, done=1))

    def test_the_latest_run_decides(self):
        pipeline = self.pipeline()
        self.finish(run_partition(pipeline, '2026-11-01')[0], status=Status.FAILED)
        self.finish(run_partition(pipeline, '2026-11-01')[0])
        rows, _ = self.by_partition(pipeline)
        self.assertEqual(rows['2026-11-01']['state'], 'done')

    def test_accepted_code_is_done_without_a_run(self):
        pipeline = self.pipeline()
        self.finish(run_partition(pipeline, '2026-11-02')[0], code='code-1')
        partition_state.accept_code(pipeline, ['2026-11-02'])
        rows, _ = self.by_partition(pipeline)
        self.assertEqual(rows['2026-11-02']['state'], 'done')

    def test_an_upstream_partition_completed_later_makes_it_stale(self):
        raw = self.pipeline(name=f'{self._testMethodName}_raw')
        pipeline = self.pipeline(upstream=[raw.uuid])
        earlier = datetime(2026, 11, 3, tzinfo=timezone.utc)
        self.finish(run_partition(pipeline, '2026-11-01')[0], at=earlier)
        self.finish(run_partition(pipeline, '2026-11-02')[0], at=earlier)
        # The raw data of Nov 1 was loaded again after the pipeline used it.
        self.finish(run_partition(raw, '2026-11-01')[0], at=earlier + timedelta(days=1))
        self.finish(run_partition(raw, '2026-11-02')[0], at=earlier - timedelta(days=1))

        rows, _ = self.by_partition(pipeline)

        self.assertEqual(rows['2026-11-01']['state'], 'stale')
        self.assertEqual(rows['2026-11-01']['stale_because'][0]['pipeline'], raw.uuid)
        self.assertEqual(rows['2026-11-02']['state'], 'done')

    def test_running_the_stale_and_the_missing(self):
        pipeline = self.pipeline()
        self.finish(run_partition(pipeline, '2026-11-01')[0], code='code-1')
        self.finish(run_partition(pipeline, '2026-11-02')[0])

        stale = run_selected(pipeline, 'stale', NOW)
        missing = run_selected(pipeline, 'missing', NOW)

        self.assertEqual([r['partition'] for r in stale['created']], ['2026-11-01'])
        self.assertEqual([r['partition'] for r in missing['created']],
                         ['2026-11-03', '2026-11-04', '2026-11-05', '2026-11-06'])
        # Asked again, they have active runs already.
        again = run_selected(pipeline, ['2026-11-03'], NOW)
        self.assertEqual((again['created'], len(again['active'])), ([], 1))

    def test_the_declaration_is_checked_with_its_upstream_pipelines(self):
        from mage_ai.orchestration import compilation

        raw = self.pipeline(name=f'{self._testMethodName}_raw')
        self.assertEqual(partition_state.check(self.pipeline(upstream=[raw.uuid])), [])
        missing = self.pipeline(name=f'{self._testMethodName}_b', upstream=['nowhere'])
        self.assertEqual(partition_state.check(missing),
                         ['partitions: upstream pipeline nowhere does not exist.'])
        other = Pipeline.create(f'{self._testMethodName}_other', repo_path=self.repo_path)
        self.addCleanup(other.delete)
        asyncio.run(other.update(dict(partitions=dict(key='region', values=['eu']))))
        mismatched = self.pipeline(name=f'{self._testMethodName}_c', upstream=[other.uuid])
        [problem] = partition_state.check(mismatched)
        self.assertIn(f'upstream pipeline {other.uuid} has other partitions', problem)
        compilation._compilations.clear()
        self.assertEqual(compilation.summary_for(mismatched)['partitions']['problems'],
                         [problem])

