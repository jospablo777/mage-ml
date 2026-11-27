"""
Datasets (ADR-0046): block outputs a project names in datasets.yaml, their status read from
recorded runs and the stored output's footer, freshness kept apart from checks, and the
datasets each one reads or feeds.
"""
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mage_ai.data_preparation import datasets
from mage_ai.data_preparation.models.block import Block
from mage_ai.data_preparation.models.pipeline import Pipeline
from mage_ai.orchestration.fusion_verify import run_in_process
from mage_ai.orchestration.triggers.api import trigger_pipeline
from mage_ai.tests.base_test import DBTestCase

LOAD = '''
import pandas as pd


@data_loader
def load(*args, **kwargs):
    return pd.DataFrame({
        'day': pd.to_datetime(['2026-11-20', '2026-11-21', '2026-11-22'], utc=True),
        'sales': [10, 20, 30],
    })
'''

TOTAL = '''
@transformer
def total(frame, *args, **kwargs):
    return frame.groupby('day', as_index=False)['sales'].sum()
'''


class DatasetsTest(DBTestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(lambda: Path(self.repo_path, datasets.FILE).unlink(missing_ok=True))
        self.pipeline = Pipeline.create(self.faker.unique.name(), repo_path=self.repo_path)
        uuid = self.pipeline.uuid
        load = Block.create(f'{uuid}_load', 'data_loader', self.repo_path, pipeline=self.pipeline)
        Path(load.file_path).write_text(textwrap.dedent(LOAD))
        total = Block.create(f'{uuid}_total', 'transformer', self.repo_path,
                             pipeline=self.pipeline, upstream_block_uuids=[load.uuid])
        Path(total.file_path).write_text(textwrap.dedent(TOTAL))
        self.load_uuid, self.total_uuid = load.uuid, total.uuid
        self.pipeline = Pipeline.get(uuid, repo_path=self.repo_path)

    def run_once(self):
        run = run_in_process(trigger_pipeline(self.pipeline.uuid, variables={}),
                             log=lambda _line: None)
        self.assertEqual(run.status, 'completed')
        return run

    def register(self, **entry):
        return datasets.register(self.repo_path, dict(
            pipeline=self.pipeline.uuid, **entry,
        ))

    def test_a_registered_output_reports_its_rows_columns_and_data_through(self):
        raw = self.register(id='raw_sales', block=self.load_uuid, owner='ana@example.com')
        daily = self.register(
            id='daily_sales', name='Daily sales', block=self.total_uuid,
            time_column='day', max_age='1d', description='Sales per day.',
        )
        self.assertIn('id: daily_sales', Path(self.repo_path, datasets.FILE).read_text())
        before = datasets.status(self.repo_path, daily)
        self.assertEqual((before['published_at'], before['checks']['state']), (None, 'unknown'))
        self.assertEqual(before['freshness']['reason'], 'The output has no day values.')

        run = self.run_once()
        found = datasets.status(self.repo_path, daily)
        self.assertEqual(found['pipeline_run_id'], run.id)
        self.assertEqual(found['rows'], 3)
        self.assertEqual([c['name'] for c in found['columns']], ['day', 'sales'])
        self.assertTrue(found['data_through'].startswith('2026-11-22'))
        # The data ends 2026-11-22: stale against a day, however recently it was published.
        self.assertEqual(found['freshness']['state'], 'stale')
        self.assertEqual(found['freshness']['basis'], 'data')
        self.assertEqual(found['checks'], dict(state='passing', failing=[]))

        deps = datasets.dependencies(self.repo_path, datasets.load(self.repo_path), daily)
        self.assertEqual(deps['upstream'], [dict(id='raw_sales', via='block')])
        self.assertEqual(datasets.dependencies(
            self.repo_path, datasets.load(self.repo_path), raw,
        )['downstream'], [dict(id='daily_sales', via='block')])
        self.assertEqual([d['id'] for d in datasets.search(datasets.load(self.repo_path),
                                                            'ana sales')], ['raw_sales'])

    def test_freshness_by_publication_and_its_unknown_states(self):
        now = datetime(2026, 11, 27, tzinfo=timezone.utc)
        published = now - timedelta(hours=2)
        entry = dict(max_age='6h')
        self.assertEqual(datasets.freshness(entry, published, None, now)['state'], 'fresh')
        self.assertEqual(datasets.freshness(dict(max_age='1h'), published, None, now)['state'],
                         'stale')
        self.assertEqual(datasets.freshness(dict(), published, None, now)['reason'],
                         'No max_age is set.')
        future = datasets.freshness(entry, now + timedelta(hours=1), None, now)
        self.assertEqual(future['reason'], 'Its time is in the future.')

    def test_bad_registrations_are_refused(self):
        for entry, message in [
            (dict(id='Bad Id', block=self.load_uuid), 'lowercase'),
            (dict(id='x', block='nope'), 'has no block nope'),
            (dict(id='x', block=self.load_uuid, max_age='soon'), 'max_age'),
            (dict(id='x', block=self.load_uuid, colour='red'), 'Unknown dataset fields'),
        ]:
            with self.assertRaisesRegex(datasets.DatasetError, message):
                self.register(**entry)
        self.register(id='first', block=self.load_uuid)
        with self.assertRaisesRegex(datasets.DatasetError, 'is the dataset first already'):
            self.register(id='second', block=self.load_uuid)
        datasets.unregister(self.repo_path, 'first')
        self.assertEqual(datasets.load(self.repo_path), [])
